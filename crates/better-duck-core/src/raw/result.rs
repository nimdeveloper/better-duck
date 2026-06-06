use std::{
    cell::OnceCell,
    ffi::CStr,
    ops::{Deref, DerefMut},
};

use crate::ffi::{duckdb_column_count, duckdb_column_name, duckdb_destroy_result, DUCKDB_TYPE};

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi,
    raw::row::DuckRow,
};

use super::data_chunk::DataChunk;

// TODO: Implement rows cache by using Box<[DuckValue]> or Vec<DuckValue> to store rows
// TODO: Implement exists method

/// Represents the result of a DuckDB query, providing row-by-row iteration over
/// the returned data.
///
/// `DuckResult` owns the underlying `duckdb_result` and destroys it in [`Drop`].
/// It implements [`Iterator`] yielding `Result<DuckRow>`.
///
/// # Safety
///
/// This struct interacts directly with the DuckDB C API. The underlying
/// `duckdb_result` must be a fully initialized result from a successful query.
pub struct DuckResult {
    res: ffi::duckdb_result,
    chunk: Option<DataChunk>,
    /// Owned column names, populated once on construction.
    column_names: OnceCell<Box<[Box<str>]>>,
    column_types: Box<[DUCKDB_TYPE]>,
    /// Number of columns in the result.
    pub col_count: u64,
}

impl DuckResult {
    /// Creates a new `DuckResult` from an owned `duckdb_result`.
    ///
    /// Immediately resolves column names and types. Panics if the result is in
    /// an invalid state (should not happen for a result from a successful query).
    pub fn new(mut result: ffi::duckdb_result) -> DuckResult {
        let mut res = DuckResult {
            // SAFETY: `result` is a valid, fully initialized `duckdb_result` that was
            // returned by `duckdb_query` or `duckdb_execute_prepared` and is now moved
            // (heap-allocated by the caller). `duckdb_column_count` reads from this struct.
            col_count: unsafe { duckdb_column_count(&mut result) },
            res: result,
            chunk: None,
            column_names: OnceCell::new(),
            column_types: Box::new([]),
        };
        res.resolve_columns_name().expect("failed to resolve column names");
        res.resolve_columns_types().expect("failed to resolve column types");
        res
    }

    #[inline]
    // SAFETY: caller must ensure `col_index` is within [0, col_count).
    fn get_col_type(
        &mut self,
        col_index: u64,
    ) -> DUCKDB_TYPE {
        // SAFETY: `self.res` is valid; `col_index` is within bounds (enforced by caller).
        unsafe { ffi::duckdb_column_type(&mut self.res, col_index) }
    }

    #[inline]
    fn resolve_columns_types(&mut self) -> Result<()> {
        // TODO: guard the uninit slice on early return (consider `scopeguard`)
        let mut col_types = Box::<[DUCKDB_TYPE]>::new_uninit_slice(self.col_count as usize);

        for each in 0..self.col_count {
            // SAFETY: `each` is within [0, col_count), satisfying the invariant of
            // `get_col_type`.
            let temp_col_type = self.get_col_type(each);
            // SAFETY: `col_types[each]` is within the allocation; we write an initialized
            // `DUCKDB_TYPE` value.
            unsafe {
                col_types[each as usize].as_mut_ptr().write(temp_col_type);
            }
        }
        // SAFETY: every element in `col_types` has been initialized above.
        self.column_types = unsafe { col_types.assume_init() };
        Ok(())
    }

    #[inline]
    fn resolve_columns_name(&mut self) -> Result<()> {
        let names = (0..self.col_count)
            .map(|i| {
                // SAFETY: `i` is within [0, col_count). `duckdb_column_name` returns a
                // pointer into result-owned memory valid for the lifetime of `self.res`.
                // We copy the bytes immediately so the raw pointer does not escape.
                let raw = unsafe { duckdb_column_name(&mut self.res, i) };
                if raw.is_null() {
                    return Err(Error::InvalidColumnIndex(i as usize));
                }
                // SAFETY: DuckDB guarantees null-terminated valid UTF-8 for column names.
                unsafe { CStr::from_ptr(raw) }
                    .to_str()
                    .map(|s| s.to_string().into_boxed_str())
                    .map_err(|e| {
                        Error::ConversionError(DuckDBConversionError::ConversionError(
                            e.to_string(),
                        ))
                    })
            })
            .collect::<Result<Vec<Box<str>>>>()?;

        self.column_names
            .set(names.into_boxed_slice())
            .map_err(|_| Error::UNKNOWN("column names already set".into()))
    }

    /// Advances the internal cursor to the next row.
    ///
    /// Returns `Some(())` if a row is available, or `None` if all rows have been
    /// consumed.
    fn advance(&mut self) -> Option<()> {
        loop {
            if self.chunk.is_none() {
                // SAFETY: `self.res` is a valid duckdb_result. `DataChunk::from_result`
                // calls `duckdb_fetch_chunk` which returns null when exhausted.
                let next_chunk = DataChunk::from_result(self);
                match next_chunk {
                    None => return None,
                    Some(Err(_)) => {
                        self.chunk = None;
                        return None;
                    },
                    Some(Ok(chunk)) => {
                        self.chunk = Some(chunk);
                    },
                }
            }
            let the_chunk = self.chunk.as_mut().unwrap();
            // SAFETY: `the_chunk` wraps a valid duckdb_data_chunk.
            if the_chunk.row_count() == 0 {
                self.chunk = None;
                return None;
            }
            // SAFETY: `the_chunk` wraps a valid duckdb_data_chunk whose row count > 0.
            if the_chunk.next_row().is_some() {
                let row_chunk = **the_chunk;
                if row_chunk.is_null() {
                    panic!("Data chunk is null");
                }
                return Some(());
            } else {
                self.chunk = None;
                // Loop to fetch the next chunk.
            }
        }
    }
}

// Exposed API
impl DuckResult {
    /// Returns the current row as a [`DuckRow`].
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk is not available or value conversion fails.
    pub fn current(&mut self) -> Result<DuckRow> {
        let col_names = self.column_names().to_vec().into_boxed_slice();
        let chunk = self.chunk.as_mut().unwrap();
        DuckRow::from_chunk(chunk, col_names, &self.column_types)
    }

    /// Returns the number of rows changed by the last INSERT/UPDATE/DELETE.
    ///
    /// Returns `0` for SELECT statements.
    #[allow(unused)]
    #[inline]
    pub fn changes(&mut self) -> u64 {
        // SAFETY: `self.res` is a valid duckdb_result.
        unsafe { ffi::duckdb_rows_changed(&mut self.res) }
    }

    /// Returns the number of columns in this result.
    #[allow(unused)]
    #[inline]
    pub fn column_count(&self) -> u64 {
        self.col_count
    }

    /// Returns the DuckDB type of the column at `col_index`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `col_index` is out of range.
    #[allow(unused)]
    #[inline]
    pub fn column_type(
        &self,
        col_index: usize,
    ) -> Result<DUCKDB_TYPE> {
        if col_index >= self.col_count as usize {
            return Err(Error::InvalidColumnIndex(col_index));
        }
        Ok(self.column_types[col_index])
    }

    /// Returns the name of the column at `col_index`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `col_index` is out of range.
    #[allow(unused)]
    #[inline]
    pub fn column_name(
        &self,
        col_index: usize,
    ) -> Result<&str> {
        if col_index >= self.col_count as usize {
            return Err(Error::InvalidColumnIndex(col_index));
        }
        Ok(&self.column_names.get().unwrap()[col_index])
    }

    /// Returns a slice of all column names in result order.
    #[allow(unused)]
    #[inline]
    pub fn column_names(&self) -> &[Box<str>] {
        self.column_names.get().map(|v| v.as_ref()).unwrap_or(&[])
    }

    /// Returns the zero-based index of the column with the given name, or `None`
    /// if no column matches.
    #[allow(unused)]
    #[inline]
    pub fn column_idx(
        &self,
        col_name: &str,
    ) -> Option<usize> {
        self.column_names.get().unwrap().iter().position(|name| name.as_ref() == col_name)
    }
}

impl Iterator for DuckResult {
    type Item = Result<DuckRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.advance().is_some() {
            Some(self.current())
        } else {
            None
        }
    }
}

impl Deref for DuckResult {
    type Target = ffi::duckdb_result;

    fn deref(&self) -> &Self::Target {
        &self.res
    }
}
impl DerefMut for DuckResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.res
    }
}
impl Drop for DuckResult {
    fn drop(&mut self) {
        // SAFETY: `self.res` is a valid duckdb_result created by `DuckResult::new`.
        // `duckdb_destroy_result` is called exactly once here in `drop`.
        unsafe {
            duckdb_destroy_result(&mut self.res);
        }
    }
}
