//! An owned-or-borrowed DuckDB data chunk.

use crate::{
    error::{Error, Result},
    ffi::{
        duckdb_create_data_chunk, duckdb_data_chunk, duckdb_data_chunk_get_column_count,
        duckdb_data_chunk_get_size, duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size,
        duckdb_destroy_data_chunk, duckdb_vector_size,
    },
};

use super::{
    logical_type::LogicalType,
    vector::{VectorMut, VectorRef},
};

/// A DuckDB data chunk: a batch of rows laid out column-wise, one [`VectorRef`]/
/// [`VectorMut`] per column.
///
/// Either owned by this handle (allocated via [`DataChunkHandle::new`] and
/// destroyed on drop) or borrowed from a DuckDB callback frame (via a crate-
/// internal constructor), in which case DuckDB owns the chunk itself and it
/// must not be destroyed out from under it.
///
/// This is a separate type from the crate's internal, read-only
/// `raw::data_chunk::DataChunk` — that type's `Drop` unconditionally destroys
/// the chunk and its `next_row` destroys-and-nulls on exhaustion, both of which
/// would double-free a chunk DuckDB owns.
pub struct DataChunkHandle {
    ptr: duckdb_data_chunk,
    owned: bool,
}

impl DataChunkHandle {
    /// Allocates a new data chunk with the given column types.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB fails to allocate the chunk.
    pub fn new(types: &[LogicalType]) -> Result<Self> {
        let mut raw_types: Vec<_> = types.iter().map(LogicalType::as_raw).collect();
        // SAFETY: `raw_types` is a valid, non-dangling array of `raw_types.len()`
        // logical type handles, each owned (and kept alive) by `types` for the
        // duration of this call; `duckdb_create_data_chunk` copies them.
        let ptr =
            unsafe { duckdb_create_data_chunk(raw_types.as_mut_ptr(), raw_types.len() as u64) };
        if ptr.is_null() {
            return Err(Error::ConversionError(
                crate::error::DuckDBConversionError::ConversionError(
                    "duckdb_create_data_chunk returned null".to_owned(),
                ),
            ));
        }
        Ok(Self { ptr, owned: true })
    }

    /// Wraps a `duckdb_data_chunk` owned by a DuckDB callback frame, without
    /// taking ownership of it.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null `duckdb_data_chunk` that stays allocated
    /// and is not mutated by other code for the full lifetime of the returned
    /// handle and any vectors derived from it.
    pub(crate) unsafe fn borrowed(ptr: duckdb_data_chunk) -> Self {
        Self { ptr, owned: false }
    }

    /// Returns a read-only view of column `idx`.
    ///
    /// # Errors
    ///
    /// Returns an error if `idx` is out of range.
    pub fn vector(
        &self,
        idx: usize,
    ) -> Result<VectorRef<'_>> {
        self.check_col(idx)?;
        // SAFETY: `self.ptr` is valid; `idx` was checked above. The returned
        // vector pointer is valid for as long as `self.ptr` is (per the C API
        // docs) and does not need separate destruction.
        let vec_ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        // SAFETY: `vec_ptr` is valid for the lifetime of `&self`.
        Ok(unsafe { VectorRef::new(vec_ptr) })
    }

    /// Returns an exclusive, writable view of column `idx`.
    ///
    /// # Errors
    ///
    /// Returns an error if `idx` is out of range.
    pub fn vector_mut(
        &mut self,
        idx: usize,
    ) -> Result<VectorMut<'_>> {
        self.check_col(idx)?;
        // SAFETY: `self.ptr` is valid; `idx` was checked above.
        let vec_ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        // SAFETY: `vec_ptr` is valid for the lifetime of `&mut self`, and the
        // `&mut self` borrow ensures no other view of this chunk's vectors is
        // live at the same time.
        Ok(unsafe { VectorMut::new(vec_ptr) })
    }

    /// Returns a writable view of every column, in column order.
    ///
    /// The views are disjoint by construction (distinct column indices), so
    /// they may be held simultaneously despite each being individually
    /// exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error if a column vector cannot be obtained.
    pub fn vectors_mut(&mut self) -> Result<Vec<VectorMut<'_>>> {
        let n = self.num_columns();
        (0..n)
            .map(|idx| {
                // SAFETY: `self.ptr` is valid; `idx < n` is in range.
                let vec_ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
                // SAFETY: each `vec_ptr` refers to a distinct column, so these
                // views never alias each other even though several are held at
                // once; all are bounded by the `&mut self` borrow.
                Ok(unsafe { VectorMut::new(vec_ptr) })
            })
            .collect()
    }

    /// The number of rows currently in this chunk.
    pub fn len(&self) -> usize {
        // SAFETY: `self.ptr` is a valid data chunk.
        unsafe { duckdb_data_chunk_get_size(self.ptr) as usize }
    }

    /// Returns `true` if this chunk has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of columns in this chunk.
    pub fn num_columns(&self) -> usize {
        // SAFETY: `self.ptr` is a valid data chunk.
        unsafe { duckdb_data_chunk_get_column_count(self.ptr) as usize }
    }

    /// Sets the number of valid rows in this chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if `n` exceeds [`DataChunkHandle::capacity`].
    pub fn set_len(
        &mut self,
        n: usize,
    ) -> Result<()> {
        if n > self.capacity() {
            return Err(Error::ConversionError(
                crate::error::DuckDBConversionError::ConversionError(format!(
                    "row count {n} exceeds chunk capacity {}",
                    self.capacity()
                )),
            ));
        }
        // SAFETY: `self.ptr` is a valid data chunk; `n` was just bounds-checked
        // against its capacity.
        unsafe { duckdb_data_chunk_set_size(self.ptr, n as u64) };
        Ok(())
    }

    /// The maximum number of rows this chunk can hold — DuckDB's build-time
    /// vector size, not a fixed constant. Never hardcode a row-count limit;
    /// always read it from here.
    pub fn capacity(&self) -> usize {
        // SAFETY: always safe to call; takes no arguments and reads no memory
        // beyond DuckDB's own build-time configuration.
        unsafe { duckdb_vector_size() as usize }
    }

    fn check_col(
        &self,
        idx: usize,
    ) -> Result<()> {
        if idx >= self.num_columns() {
            return Err(Error::InvalidColumnIndex(idx));
        }
        Ok(())
    }
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        if self.owned && !self.ptr.is_null() {
            // SAFETY: `self.owned` guarantees this handle allocated `self.ptr`
            // via `duckdb_create_data_chunk` and no one else destroys it;
            // `self.ptr` is non-null (checked above) and destroyed exactly once.
            unsafe { duckdb_destroy_data_chunk(&mut self.ptr) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_chunk_reports_column_count_and_capacity() {
        let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<String>().unwrap()];
        let chunk = DataChunkHandle::new(&types).unwrap();
        assert_eq!(chunk.num_columns(), 2);
        assert!(chunk.capacity() > 0);
        assert_eq!(chunk.len(), 0);
    }

    #[test]
    fn set_len_within_capacity_succeeds() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        chunk.set_len(3).unwrap();
        assert_eq!(chunk.len(), 3);
    }

    #[test]
    fn set_len_beyond_capacity_errors() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        let too_many = chunk.capacity() + 1;
        assert!(chunk.set_len(too_many).is_err());
    }

    #[test]
    fn vector_out_of_range_errors() {
        let types = [LogicalType::of::<i32>().unwrap()];
        let chunk = DataChunkHandle::new(&types).unwrap();
        assert!(chunk.vector(1).is_err());
    }

    #[test]
    fn vectors_mut_covers_every_column() {
        let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<i64>().unwrap()];
        let mut chunk = DataChunkHandle::new(&types).unwrap();
        let views = chunk.vectors_mut().unwrap();
        assert_eq!(views.len(), 2);
    }
}
