use std::{
    ops::{Deref, DerefMut},
    ptr,
};

use libduckdb_sys::{duckdb_data_chunk, duckdb_destroy_data_chunk};

use super::result::DuckResult;
use crate::{error::Result, ffi};

pub struct DataChunk(
    pub(crate) duckdb_data_chunk,
    pub(crate) u64, // current row index in chunk
);

impl DataChunk {
    #[inline]
    pub unsafe fn new(data_chunk: ffi::duckdb_data_chunk) -> Result<DataChunk> {
        if data_chunk.is_null() {
            return Err(crate::error::Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("data chunk is null".to_owned()),
            ));
        }
        Ok(DataChunk(data_chunk, 0))
    }
    #[inline]
    pub unsafe fn from_result(result: &DuckResult) -> Option<Result<DataChunk>> {
        let data_chunk = ffi::duckdb_fetch_chunk(**result);
        if data_chunk.is_null() {
            return None;
        }
        let res = DataChunk::new(data_chunk);
        Some(res)
    }

    #[allow(unused)]
    #[inline]
    pub fn current_row(&self) -> u64 {
        self.1
    }
    #[inline]
    pub unsafe fn row_count(&self) -> u64 {
        ffi::duckdb_data_chunk_get_size(self.0)
    }

    #[inline]
    pub unsafe fn next_row(&mut self) -> Option<u64> {
        if self.row_count() < 1 {
            return None;
        }
        if self.1 >= self.row_count() {
            // Reset the row index and fetch the next chunk
            self.1 = 0;
            // SAFETY: `self.0` is a valid, non-null duckdb_data_chunk; after destroy
            // we null it so this path is never re-entered.
            unsafe { duckdb_destroy_data_chunk(&mut (self.0)) };
            self.0 = ptr::null_mut();
            return None;
        }
        self.1 += 1;
        Some(self.1 - 1)
    }
}

impl Deref for DataChunk {
    type Target = duckdb_data_chunk;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataChunk {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for DataChunk {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is a valid non-null `duckdb_data_chunk`. The null guard
            // ensures this path runs at most once.
            unsafe { duckdb_destroy_data_chunk(&mut (self.0)) };
        }
    }
}
