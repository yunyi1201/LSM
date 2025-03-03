// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

///----------------------------------Block encoding format--------------------------------------------------------------------------------
/// --------------------------------------------------------------------------------------------------------------------------------------
/// |                                     Entry                                        |             Entry Offsets     |  Offset numbers |
/// --------------------------------------------------------------------------------------------------------------------------------------
/// | key_overlap_len (u16) | rest_key_len (u16) | key | value_len (u16) | value | ... |  offset (u16) | offset (u16)  |       len       |
/// --------------------------------------------------------------------------------------------------------------------------------------

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    loop {
        if i >= first_key.key_len() || i >= key.key_len() {
            break;
        }
        if first_key.key_ref()[i] != key.key_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::default(),
        }
    }
    fn estimated_size(&self) -> usize {
        2 /*number of elements*/ + self.data.len() + self.offsets.len() * 2
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.estimated_size() + key.raw_len() + value.len() + 6 /*key_len + value_len + offset*/ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        let offset = self.data.len() as u16;
        self.offsets.push(offset);

        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // put key_overlap_len compare the first key.
        self.data.put_u16(overlap as u16);
        // put the remaining_key_len for the key.
        self.data.put_u16((key.key_len() - overlap) as u16);
        // put the slice that the key remain.
        self.data.put(&key.key_ref()[overlap..]);
        // put the timestamp.
        self.data.put_u64(key.ts());
        // put the value
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("Block shouldn't empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
