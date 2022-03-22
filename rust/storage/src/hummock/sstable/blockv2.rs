use std::cmp::Ordering;
use std::io::{Read, Write};
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lz4::Decoder;

use super::utils::{
    bytes_diff, var_u32_len, xxhash64_verify, BufExt, BufMutExt, CompressionAlgorithm,
};
use crate::hummock::sstable::utils::xxhash64_checksum;
use crate::hummock::{HummockError, HummockResult};

const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;
const DEFAULT_RESTART_INTERVAL: usize = 16;
const DEFAULT_ENTRY_SIZE: usize = 16;

pub struct BlockV2 {
    /// Uncompressed entries data.
    data: Bytes,
    /// Restart points.
    restart_points: Vec<u32>,
}

impl BlockV2 {
    pub fn decode(buf: Bytes) -> HummockResult<Self> {
        // Verify checksum.
        let xxhash64_checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], xxhash64_checksum)?;

        // Decompress.
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        let buf = match compression {
            CompressionAlgorithm::None => buf.slice(..buf.len() - 9),
            CompressionAlgorithm::Lz4 => {
                let mut decoder = Decoder::new(buf.reader())
                    .map_err(HummockError::decode_error)
                    .unwrap();
                let mut decoded = Vec::with_capacity(DEFAULT_BLOCK_SIZE);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)
                    .unwrap();
                Bytes::from(decoded)
            }
        };

        // Decode restart points.
        let n_restarts = (&buf[buf.len() - 4..]).get_u32_le();
        let data_len = buf.len() - 4 - n_restarts as usize * 4;
        let mut restart_points = Vec::with_capacity(n_restarts as usize);
        let mut restart_points_buf = &buf[data_len..buf.len() - 4];
        for _ in 0..n_restarts {
            restart_points.push(restart_points_buf.get_u32_le());
        }

        Ok(BlockV2 {
            data: buf.slice(..data_len),
            restart_points,
        })
    }

    /// Entries data len.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        assert!(!self.data.is_empty());
        self.data.len()
    }

    /// Get restart point by index.
    pub fn restart_point(&self, index: usize) -> u32 {
        self.restart_points[index]
    }

    /// Get restart point len.
    pub fn restart_point_len(&self) -> usize {
        self.restart_points.len()
    }

    /// Search the index of the restart point that the given `offset` belongs to.
    pub fn search_restart_point(&self, offset: usize) -> usize {
        self.restart_points
            .binary_search(&(offset as u32))
            .unwrap_or_else(|x| x.saturating_sub(1))
    }

    pub fn search_restart_point_by<F>(&self, f: F) -> usize
    where
        F: FnMut(&u32) -> Ordering,
    {
        self.restart_points
            .binary_search_by(f)
            .unwrap_or_else(|x| x.saturating_sub(1))
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    #[cfg(test)]
    pub fn raw(&self) -> Bytes {
        self.data.clone()
    }
}

/// [`KeyPrefix`] contains info for prefix compression.
#[derive(Debug)]
pub struct KeyPrefix {
    overlap: usize,
    diff: usize,
    value: usize,
    /// Used for calculating range, won't be encoded.
    offset: usize,
}

impl KeyPrefix {
    pub fn encode(&self, mut buf: &mut impl BufMut) {
        buf.put_var_u32(self.overlap as u32);
        buf.put_var_u32(self.diff as u32);
        buf.put_var_u32(self.value as u32);
    }

    pub fn decode(mut buf: &mut impl Buf, offset: usize) -> Self {
        let overlap = buf.get_var_u32() as usize;
        let diff = buf.get_var_u32() as usize;
        let value = buf.get_var_u32() as usize;
        Self {
            overlap,
            diff,
            value,
            offset,
        }
    }

    /// Encoded length.
    fn len(&self) -> usize {
        var_u32_len(self.overlap as u32)
            + var_u32_len(self.diff as u32)
            + var_u32_len(self.value as u32)
    }

    /// Get overlap len.
    pub fn overlap_len(&self) -> usize {
        self.overlap
    }

    /// Get diff key range.
    pub fn diff_key_range(&self) -> Range<usize> {
        self.offset + self.len()..self.offset + self.len() + self.diff
    }

    /// Get value range.
    pub fn value_range(&self) -> Range<usize> {
        self.offset + self.len() + self.diff..self.offset + self.len() + self.diff + self.value
    }

    /// Get entry len.
    pub fn entry_len(&self) -> usize {
        self.len() + self.diff + self.value
    }
}

pub struct BlockV2BuilderOptions {
    /// Reserved bytes size when creating buffer to avoid frequent allocating.
    pub capacity: usize,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    /// Restart point interval.
    pub restart_interval: usize,
}

impl Default for BlockV2BuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_BLOCK_SIZE,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        }
    }
}

/// [`BlockV2Writer`] encode and append block to a buffer.
pub struct BlockV2Builder {
    /// Write buffer.
    buf: BytesMut,
    /// Entry interval between restart points.
    restart_count: usize,
    /// Restart points.
    restart_points: Vec<u32>,
    /// Last key.
    last_key: Bytes,
    /// Count of entries in current block.
    entry_count: usize,
    /// Compression algorithm.
    compression_algorithm: CompressionAlgorithm,
}

impl BlockV2Builder {
    pub fn new(options: BlockV2BuilderOptions) -> Self {
        Self {
            buf: BytesMut::with_capacity(options.capacity),
            restart_count: options.restart_interval,
            restart_points: Vec::with_capacity(
                options.capacity / DEFAULT_ENTRY_SIZE / options.restart_interval + 1,
            ),
            last_key: Bytes::default(),
            entry_count: 0,
            compression_algorithm: options.compression_algorithm,
        }
    }

    /// Append a kv pair to the block.
    ///
    /// NOTE: Key must be added in ASCEND order.
    ///
    /// # Format
    ///
    /// ```plain
    /// entry (kv pair): | overlap len (2B) | diff len (2B) | value len(4B) | diff key | value |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if key is not added in ASCEND order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.entry_count > 0 {
            // TODO: Remove me.
            // if self.last_key >= key {
            //     println!(
            //         "last key: {:?}\n     key: {:?}",
            //         Bytes::from(self.last_key.clone()),
            //         Bytes::copy_from_slice(key)
            //     );
            // }
            assert!(self.last_key < key);
        }
        // Update restart point if needed and calculate diff key.
        let diff_key = if self.entry_count % self.restart_count == 0 {
            self.restart_points.push(self.buf.len() as u32);
            self.last_key = key.to_vec().into();
            key
        } else {
            bytes_diff(&self.last_key, key)
        };

        let prefix = KeyPrefix {
            overlap: key.len() - diff_key.len(),
            diff: diff_key.len(),
            value: value.len(),
            offset: self.buf.len(),
        };

        prefix.encode(&mut self.buf);
        self.buf.put_slice(diff_key);
        self.buf.put_slice(value);

        self.last_key = key.to_vec().into();
        self.entry_count += 1;
    }

    /// Finish building block.
    ///
    /// # Format
    ///
    /// ```plain
    /// compressed: | entries | restart point 0 (4B) | ... | restart point N-1 (4B) | N (4B) |
    /// uncompressed: | compression method (1B) | crc32sum (4B) |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if there is compression error.
    pub fn build(mut self) -> Bytes {
        assert!(self.entry_count > 0);
        for restart_point in &self.restart_points {
            self.buf.put_u32_le(*restart_point);
        }
        self.buf.put_u32_le(self.restart_points.len() as u32);
        let mut buf = match self.compression_algorithm {
            CompressionAlgorithm::None => self.buf,
            CompressionAlgorithm::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(BytesMut::with_capacity(self.buf.len()).writer())
                    .map_err(HummockError::encode_error)
                    .unwrap();
                encoder
                    .write(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let (writer, result) = encoder.finish();
                result.map_err(HummockError::encode_error).unwrap();
                writer.into_inner()
            }
        };
        self.compression_algorithm.encode(&mut buf);
        let checksum = xxhash64_checksum(&buf);
        buf.put_u64_le(checksum);
        buf.freeze()
    }

    /// Approximate block len (uncompressed).
    pub fn approximate_len(&self) -> usize {
        self.buf.len() + 4 * self.restart_points.len() + 4 + 1 + 4
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::sstable::utils::full_key;
    use crate::hummock::BlockIteratorV2;

    #[test]
    fn test_block_enc_dec() {
        let options = BlockV2BuilderOptions::default();
        let mut builder = BlockV2Builder::new(options);
        builder.add(&full_key(b"k1", 1), b"v01");
        builder.add(&full_key(b"k2", 2), b"v02");
        builder.add(&full_key(b"k3", 3), b"v03");
        builder.add(&full_key(b"k4", 4), b"v04");
        let buf = builder.build();
        let block = Arc::new(BlockV2::decode(buf).unwrap());
        let mut bi = BlockIteratorV2::new(block);

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k2", 2)[..], bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v04", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    #[test]
    fn test_compressed_block_enc_dec() {
        let options = BlockV2BuilderOptions {
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };
        let mut builder = BlockV2Builder::new(options);
        builder.add(&full_key(b"k1", 1), b"v01");
        builder.add(&full_key(b"k2", 2), b"v02");
        builder.add(&full_key(b"k3", 3), b"v03");
        builder.add(&full_key(b"k4", 4), b"v04");
        let buf = builder.build();
        let block = Arc::new(BlockV2::decode(buf).unwrap());
        let mut bi = BlockIteratorV2::new(block);

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k2", 2)[..], bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v04", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }
}
