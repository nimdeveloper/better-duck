use std::ptr;

use libduckdb_sys::{duckdb_data_chunk, duckdb_destroy_data_chunk};

use super::result::RawResult;
use crate::{error::Result, ffi};

pub struct RawDataChunk(
    pub(crate) duckdb_data_chunk,
    pub(crate) u64, // current row index in chunk
);

impl RawDataChunk {
    #[inline]
    pub fn raw(&mut self) -> *mut duckdb_data_chunk {
        &mut self.0
    }
    #[inline]
    pub unsafe fn new(data_chunk: ffi::duckdb_data_chunk) -> Result<RawDataChunk> {
        if data_chunk.is_null() {
            return Err(crate::error::Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("data chunk is null".to_owned()),
            ));
        }
        Ok(RawDataChunk(data_chunk, 0))
    }
    #[inline]
    pub unsafe fn from_result(result: &RawResult) -> Option<Result<RawDataChunk>> {
        let data_chunk = ffi::duckdb_fetch_chunk(result.res);
        if data_chunk.is_null() {
            return None;
        }
        let res = RawDataChunk::new(data_chunk);
        Some(res)
    }

    #[allow(unused)]
    #[inline]
    pub fn current_row(&self) -> u64 {
        return self.1;
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
            unsafe { duckdb_destroy_data_chunk(&mut (self.0)) };
            self.0 = ptr::null_mut();
            return None;
        }
        self.1 += 1;
        Some(self.1 - 1)
    }
}
impl Drop for RawDataChunk {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { duckdb_destroy_data_chunk(&mut (self.0)) };
        }
    }
}
