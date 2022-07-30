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

pub struct AlignedAllocator<const ALIGN: usize>;

extern crate alloc;
use alloc::alloc::{Allocator, Global};

use super::utils;

unsafe impl<const ALIGN: usize> Allocator for AlignedAllocator<ALIGN> {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        let layout = std::alloc::Layout::from_size_align(
            layout.size(),
            utils::align_up(ALIGN, layout.align()),
        )
        .unwrap();
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        let layout = std::alloc::Layout::from_size_align(
            layout.size(),
            utils::align_up(ALIGN, layout.align()),
        )
        .unwrap();
        Global.deallocate(ptr, layout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer() {
        const ALIGN: usize = 512;
        let allocator = AlignedAllocator::<ALIGN>;

        let mut buf: Vec<u8, _> = Vec::with_capacity_in(ALIGN * 8, &allocator);
        utils::assert_aligned(ALIGN, buf.as_ptr().addr());

        buf.extend_from_slice(&[b'x'; ALIGN * 8]);
        utils::assert_aligned(ALIGN, buf.as_ptr().addr());
        assert_eq!(buf, [b'x'; ALIGN * 8]);

        buf.extend_from_slice(&[b'x'; ALIGN * 8]);
        utils::assert_aligned(ALIGN, buf.as_ptr().addr());
        assert_eq!(buf, [b'x'; ALIGN * 16])
    }
}
