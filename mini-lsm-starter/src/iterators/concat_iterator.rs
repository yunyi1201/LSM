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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn seek_to(&mut self, idx: usize) -> Result<()> {
        if idx >= self.sstables.len() {
            self.current = None;
            self.next_sst_idx = self.sstables.len();
            return Ok(());
        }
        *self = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                self.sstables[idx].clone(),
            )?),
            next_sst_idx: idx + 1,
            sstables: self.sstables.clone(),
        };
        Ok(())
    }

    pub fn seek_to_key_inner(&mut self, key: KeySlice) -> Result<()> {
        let idx = self
            .sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        self.seek_to(idx)?;

        if let Some(ref mut curr) = self.current {
            curr.seek_to_key(key)?;
        }

        if !self.is_valid() {
            self.seek_to(idx + 1)?;
        }

        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        iter.seek_to(0)?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        iter.seek_to_key_inner(key)?;

        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        if let Some(ref mut curr) = self.current {
            curr.next()?;
            if curr.is_valid() {
                return Ok(());
            }
        }

        self.seek_to(self.next_sst_idx)?;

        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(ref curr) = self.current {
            curr.key()
        } else {
            panic!("invalid iterator");
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(ref curr) = self.current {
            curr.value()
        } else {
            panic!("invalid iterator");
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
