use std::{
    ops::{Deref, DerefMut},
    ptr,
};

use crate::ffi::{duckdb_data_chunk, duckdb_data_chunk_reset, duckdb_destroy_data_chunk};

use super::result::DuckResult;
use crate::{error::Result, ffi};

/// An owned DuckDB `duckdb_data_chunk` — a batch of column vectors — that is
/// destroyed on drop.
///
/// Obtain one from a query result with [`DataChunk::from_result`], then append it
/// elsewhere with [`Appender::append_chunk`](crate::Appender::append_chunk).
pub struct DataChunk(
    pub(crate) duckdb_data_chunk,
    pub(crate) u64, // current row index in chunk
);

impl DataChunk {
    /// Takes ownership of a raw `duckdb_data_chunk` handle.
    ///
    /// # Errors
    ///
    /// Returns an error if `data_chunk` is null.
    #[inline]
    pub fn new(data_chunk: ffi::duckdb_data_chunk) -> Result<DataChunk> {
        if data_chunk.is_null() {
            return Err(crate::error::Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("data chunk is null".to_owned()),
            ));
        }
        Ok(DataChunk(data_chunk, 0))
    }
    /// Fetches the next chunk of a query result, or `None` once the result is
    /// exhausted.
    #[inline]
    pub fn from_result(result: &DuckResult) -> Option<Result<DataChunk>> {
        // SAFETY: `result` is a valid duckdb_result; the returned chunk (if non-null)
        // is owned by us and must be destroyed via `duckdb_destroy_data_chunk`.
        let data_chunk = unsafe { ffi::duckdb_fetch_chunk(**result) };
        if data_chunk.is_null() {
            return None;
        }
        // SAFETY: `data_chunk` is non-null and freshly obtained from `duckdb_fetch_chunk`.
        let res = DataChunk::new(data_chunk);
        Some(res)
    }

    /// The current row cursor used by [`next_row`](DataChunk::next_row).
    #[allow(unused)]
    #[inline]
    pub fn current_row(&self) -> u64 {
        self.1
    }
    /// The number of rows in the chunk.
    #[inline]
    pub fn row_count(&self) -> u64 {
        // SAFETY: `self.0` is a valid duckdb_data_chunk (enforced by the caller).
        unsafe { ffi::duckdb_data_chunk_get_size(self.0) }
    }

    /// Resets the chunk to empty (row count 0), keeping its allocated capacity so it
    /// can be refilled and re-appended.
    #[inline]
    pub fn reset(&mut self) {
        // SAFETY: `self.0` is a valid, non-null duckdb_data_chunk owned by `self`.
        unsafe { duckdb_data_chunk_reset(self.0) };
        self.1 = 0;
    }

    /// Advances the row cursor, returning the next row index, or `None` at the end
    /// (destroying the underlying chunk).
    #[inline]
    pub fn next_row(&mut self) -> Option<u64> {
        // SAFETY: `self.0` is a valid duckdb_data_chunk (enforced by the caller).
        if self.row_count() < 1 {
            return None;
        }
        // SAFETY: same as above.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogicalType;

    #[test]
    fn reset_clears_the_row_count() {
        // Build a one-column INTEGER chunk and give it three rows.
        let int_ty = LogicalType::of::<i32>().unwrap();
        let mut raw_types = [int_ty.as_raw()];
        // SAFETY: `raw_types` holds one valid logical type handle for the call; DuckDB
        // copies it. The returned chunk is wrapped in RAII (destroyed once on drop).
        let raw = unsafe { ffi::duckdb_create_data_chunk(raw_types.as_mut_ptr(), 1) };
        drop(int_ty);
        let mut chunk = DataChunk::new(raw).unwrap();
        // SAFETY: `chunk` is valid; sizing to 3 rows is within the default capacity.
        unsafe { ffi::duckdb_data_chunk_set_size(*chunk, 3) };
        assert_eq!(chunk.row_count(), 3);

        chunk.reset();
        assert_eq!(chunk.row_count(), 0, "reset must clear the chunk to empty");
    }
}
