// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::io::{Read, Write};
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::VersionedComparator;
use {lz4, zstd};

use super::utils::{bytes_diff, xxhash64_verify, CompressionAlgorithm};
use crate::hummock::sstable::utils::xxhash64_checksum;
use crate::hummock::{HummockError, HummockResult};

pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;
pub const DEFAULT_RESTART_INTERVAL: usize = 16;
pub const DEFAULT_ENTRY_SIZE: usize = 16;

pub struct Block {
    /// Uncompressed entries data.
    data: Bytes,
    /// Restart points.
    restart_points: Vec<u32>,
}

impl Block {
    pub fn decode(buf: Bytes) -> HummockResult<Self> {
        // Verify checksum.
        let xxhash64_checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], xxhash64_checksum)?;

        // Decompress.
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        let compressed_data = &buf[..buf.len() - 9];
        let buf = match compression {
            CompressionAlgorithm::None => buf.slice(..buf.len() - 9),
            CompressionAlgorithm::Lz4 => {
                let mut decoder = lz4::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(DEFAULT_BLOCK_SIZE);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                Bytes::from(decoded)
            }
            CompressionAlgorithm::Zstd => {
                let mut decoder = zstd::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(DEFAULT_BLOCK_SIZE);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
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

        Ok(Block {
            data: buf.slice(..data_len),
            restart_points,
        })
    }

    /// Entries data len.
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        assert!(!self.data.is_empty());
        self.data.len()
    }

    /// Gets restart point by index.
    pub fn restart_point(&self, index: usize) -> u32 {
        self.restart_points[index]
    }

    /// Gets restart point len.
    pub fn restart_point_len(&self) -> usize {
        self.restart_points.len()
    }

    /// Searches the index of the restart point that the given `offset` belongs to.
    pub fn search_restart_point(&self, offset: usize) -> usize {
        // Find the largest restart point that equals or less than the given offset.
        self.restart_points
            .partition_point(|&position| position <= offset as u32)
            .saturating_sub(1) // Prevent from underflowing when given is smaller than ther first.
    }

    /// Searches the index of the restart point by partition point.
    pub fn search_restart_partition_point<P>(&self, pred: P) -> usize
    where
        P: FnMut(&u32) -> bool,
    {
        self.restart_points.partition_point(pred)
    }

    pub fn data(&self) -> &Bytes {
        &self.data
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
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16(self.overlap as u16);
        buf.put_u16(self.diff as u16);
        buf.put_u32(self.value as u32);
    }

    pub fn decode(buf: &mut impl Buf, offset: usize) -> Self {
        let overlap = buf.get_u16() as usize;
        let diff = buf.get_u16() as usize;
        let value = buf.get_u32() as usize;
        Self {
            overlap,
            diff,
            value,
            offset,
        }
    }

    /// Encoded length.
    fn len(&self) -> usize {
        2 + 2 + 4
    }

    /// Gets overlap len.
    pub fn overlap_len(&self) -> usize {
        self.overlap
    }

    /// Gets diff key range.
    pub fn diff_key_range(&self) -> Range<usize> {
        self.offset + self.len()..self.offset + self.len() + self.diff
    }

    /// Gets value range.
    pub fn value_range(&self) -> Range<usize> {
        self.offset + self.len() + self.diff..self.offset + self.len() + self.diff + self.value
    }

    /// Gets entry len.
    pub fn entry_len(&self) -> usize {
        self.len() + self.diff + self.value
    }
}

pub struct BlockBuilderOptions {
    /// Reserved bytes size when creating buffer to avoid frequent allocating.
    pub capacity: usize,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    /// Restart point interval.
    pub restart_interval: usize,
}

impl Default for BlockBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_BLOCK_SIZE,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        }
    }
}

/// [`BlockBuilder`] encodes and appends block to a buffer.
pub struct BlockBuilder {
    /// Write buffer.
    buf: BytesMut,
    /// Entry interval between restart points.
    restart_count: usize,
    /// Restart points.
    restart_points: Vec<u32>,
    /// Last key.
    last_key: Vec<u8>,
    /// Count of entries in current block.
    entry_count: usize,
    /// Compression algorithm.
    compression_algorithm: CompressionAlgorithm,
}

impl BlockBuilder {
    pub fn new(options: BlockBuilderOptions) -> Self {
        Self {
            // add more space to avoid re-allocate space.
            buf: BytesMut::with_capacity(options.capacity + 256),
            restart_count: options.restart_interval,
            restart_points: Vec::with_capacity(
                options.capacity / DEFAULT_ENTRY_SIZE / options.restart_interval + 1,
            ),
            last_key: vec![],
            entry_count: 0,
            compression_algorithm: options.compression_algorithm,
        }
    }

    /// Appends a kv pair to the block.
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
            debug_assert!(!key.is_empty());
            debug_assert_eq!(
                VersionedComparator::compare_key(&self.last_key[..], key),
                Ordering::Less
            );
        }
        // Update restart point if needed and calculate diff key.
        let diff_key = if self.entry_count % self.restart_count == 0 {
            self.restart_points.push(self.buf.len() as u32);
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

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
    }

    pub fn get_last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.restart_points.clear();
        self.last_key.clear();
        self.entry_count = 0;
    }

    /// Finishes building block.
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
    pub fn build(&mut self) -> &[u8] {
        assert!(self.entry_count > 0);
        for restart_point in &self.restart_points {
            self.buf.put_u32_le(*restart_point);
        }
        self.buf.put_u32_le(self.restart_points.len() as u32);
        match self.compression_algorithm {
            CompressionAlgorithm::None => (),
            CompressionAlgorithm::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(BytesMut::with_capacity(self.buf.len()).writer())
                    .map_err(HummockError::encode_error)
                    .unwrap();
                encoder
                    .write_all(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let (writer, result) = encoder.finish();
                result.map_err(HummockError::encode_error).unwrap();
                self.buf = writer.into_inner();
            }
            CompressionAlgorithm::Zstd => {
                let mut encoder =
                    zstd::Encoder::new(BytesMut::with_capacity(self.buf.len()).writer(), 4)
                        .map_err(HummockError::encode_error)
                        .unwrap();
                encoder
                    .write_all(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let writer = encoder
                    .finish()
                    .map_err(HummockError::encode_error)
                    .unwrap();
                self.buf = writer.into_inner();
            }
        };
        self.compression_algorithm.encode(&mut self.buf);
        let checksum = xxhash64_checksum(&self.buf);
        self.buf.put_u64_le(checksum);
        self.buf.as_ref()
    }

    /// Approximate block len (uncompressed).
    pub fn approximate_len(&self) -> usize {
        // block + restart_points + restart_points.len + compression_algorithm + checksum
        self.buf.len() + 4 * self.restart_points.len() + 4 + 1 + 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::{BlockHolder, BlockIterator};

    #[test]
    fn test_block_enc_dec() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k1", 1), b"v01");
        builder.add(&full_key(b"k2", 2), b"v02");
        builder.add(&full_key(b"k3", 3), b"v03");
        builder.add(&full_key(b"k4", 4), b"v04");
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(Bytes::from(buf)).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

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
        inner_test_compressed(CompressionAlgorithm::Lz4);
        inner_test_compressed(CompressionAlgorithm::Zstd);
    }

    fn inner_test_compressed(algo: CompressionAlgorithm) {
        let options = BlockBuilderOptions {
            compression_algorithm: algo,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k1", 1), b"v01");
        builder.add(&full_key(b"k2", 2), b"v02");
        builder.add(&full_key(b"k3", 3), b"v03");
        builder.add(&full_key(b"k4", 4), b"v04");
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(Bytes::from(buf)).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

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

    pub fn full_key(user_key: &[u8], epoch: u64) -> Bytes {
        let mut buf = BytesMut::with_capacity(user_key.len() + 8);
        buf.put_slice(user_key);
        buf.put_u64(!epoch);
        buf.freeze()
    }
}
