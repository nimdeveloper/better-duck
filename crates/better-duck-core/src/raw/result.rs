use std::{ffi::CStr, ptr};

use libduckdb_sys::{duckdb_column_count, duckdb_column_name, duckdb_destroy_result, DUCKDB_TYPE};

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi,
    raw::row::AbstractRow,
    types::value::DuckValue,
};

use super::data_chunk::RawDataChunk;

// TODO: Implement rows cache by using Box<[DuckValue]> or Vec<DuckValue> to store rows
/// Represents a raw DuckDB result, holding the result pointer, data chunk, column names,
/// column types, and column count.
/// This struct is used to manage the result of a DuckDB query,
/// allowing access to the data in a structured way.
/// It provides methods to resolve column names and types,
/// as well as to retrieve the data in a row-wise manner.
/// # Safety
/// This struct is unsafe because it directly interacts with the DuckDB FFI.
/// It assumes that the DuckDB result is valid and that the underlying data structures
pub struct RawResult {
    pub(super) res: ffi::duckdb_result,
    chunk: Option<RawDataChunk>,
    column_names: Box<[&'static str]>,
    column_types: Box<[DUCKDB_TYPE]>,
    col_count: u64,
}

impl RawResult {
    pub fn new(mut result: ffi::duckdb_result) -> RawResult {
        let mut res = RawResult {
            res: result,
            chunk: None,
            column_names: Box::new([]),
            column_types: Box::new([]),
            col_count: unsafe { duckdb_column_count(&mut result) },
        };
        // TODO: Better error handling?
        res.resolve_columns_name().unwrap();
        res.resolve_columns_types().unwrap();
        return res;
    }

    #[inline]
    unsafe fn get_col_type(
        &mut self,
        col_index: u64,
    ) -> DUCKDB_TYPE {
        ffi::duckdb_column_type(&mut self.res, col_index)
    }
    #[inline]
    unsafe fn get_col_name(
        &mut self,
        col_index: u64,
    ) -> Result<&'static str> {
        let name = duckdb_column_name(&mut self.res, col_index);
        let return_val: Result<&str> = CStr::from_ptr(name).to_str().map_err(|e| {
            Error::ConversionError(DuckDBConversionError::ConversionError(e.to_string()))
        });
        return return_val;
    }
    #[inline]
    fn resolve_columns_types(&mut self) -> Result<()> {
        // TODO: What happens for this var, if the function returns error? (Maybe using https://docs.rs/scopeguard/latest/scopeguard/)
        let mut col_types = Box::<[DUCKDB_TYPE]>::new_uninit_slice(self.col_count as usize);

        for each in 0..self.col_count {
            let temp_col_type = unsafe { self.get_col_type(each as u64) };
            unsafe {
                col_types[each as usize].as_mut_ptr().write(temp_col_type);
                // ptr::write(slice_ptr.add(each as usize), col_name);
            }
        }
        self.column_types = unsafe { col_types.assume_init() };
        return Ok(());
    }
    #[inline]
    fn resolve_columns_name(&mut self) -> Result<()> {
        // TODO: What happens for this var, if the function returns error? (Maybe using https://docs.rs/scopeguard/latest/scopeguard/)
        let mut col_names = Box::<[&'static str]>::new_uninit_slice(self.col_count as usize);
        // let slice_ptr = col_names.as_mut_ptr() as *mut &'static str;

        for each in 0..self.col_count {
            let temp_col_name = unsafe { self.get_col_name(each as u64) };

            if let Ok(col_name) = temp_col_name {
                unsafe {
                    col_names[each as usize].as_mut_ptr().write(col_name);
                    // ptr::write(slice_ptr.add(each as usize), col_name);
                }
            } else {
                return Err(Error::InvalidColumnIndex(each as usize));
            }
        }
        self.column_names = unsafe {
            let raw = Box::into_raw(col_names);
            let types = raw as *mut [&'static str];
            Box::from_raw(types)
        };
        return Ok(());
    }

    unsafe fn get_row(
        &self,
        chunk: ffi::duckdb_data_chunk,
        row_idx: u64,
    ) -> Result<Vec<DuckValue>> {
        let column_count = self.col_count;
        if column_count == 0 {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("No columns in result".to_owned()),
            ));
        }
        let mut values: Vec<DuckValue>;
        // let types: Vec<_>;
        values = Vec::with_capacity(column_count as usize);

        let values_ptr: *mut DuckValue = values.as_mut_ptr();
        // types = Vec::with_capacity(column_count as usize);
        for col_idx in 0..column_count {
            let col_vec = ffi::duckdb_data_chunk_get_vector(chunk, col_idx);

            if col_vec.is_null() {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some("Column is null".to_owned()),
                ));
            }
            let val =
                DuckValue::from_duckdb_vec(col_vec, self.column_types[col_idx as usize], row_idx)
                    .map_err(Error::ConversionError)?;
            ptr::write(values_ptr.add(col_idx as usize), val);
        }
        values.set_len(column_count as usize);
        Ok(values)
    }

    /// Fetch the next chunk of data from the DuckDB result.
    /// This function will attempt to fetch the next chunk of data
    /// and store it as a `RawDataChunk` inside itself. If there are no more chunks,
    /// it will set the chunk to `None`.
    /// # Safety
    /// This function is unsafe because it directly interacts with the DuckDB FFI.
    /// It assumes that the `RawResult` is in a valid state and that the DuckDB
    /// result is properly initialized.
    /// # Returns
    /// * `Option<()>` - Returns `Some(())` if the chunk was advanced successfully,
    /// or `None` if there are no more chunks.
    /// # Errors
    /// Panics if the chunk cannot be fetched or if the result is invalid.
    fn advance(&mut self) -> Option<()> {
        unsafe {
            // We'll use a loop to avoid recursion and handle chunk exhaustion gracefully
            loop {
                if self.chunk.is_none() {
                    let next_chunk = RawDataChunk::from_result(self);
                    match next_chunk {
                        None => {
                            // No more chunks
                            return None;
                        },
                        Some(Err(_)) => {
                            // If we can't fetch the next chunk, we set the pointer to null
                            // to indicate that there are no more chunks to fetch.
                            self.chunk = None;
                            return None;
                        },
                        Some(Ok(chunk)) => {
                            // Found a new chunk!
                            self.chunk = Some(chunk);
                        },
                    }
                }
                let the_chunk = self.chunk.as_mut().unwrap();
                // Check if this chunk has rows
                if the_chunk.row_count() == 0 {
                    self.chunk = None;
                    return None;
                }
                if let Some(_) = the_chunk.next_row() {
                    // We have a valid row - return it
                    let row_chunk = the_chunk.raw();
                    if row_chunk.is_null() {
                        panic!("Data chunk is null");
                    }
                    return Some(());
                } else {
                    // No more rows in current chunk
                    self.chunk = None;
                    // Loop to try next chunk instead of recursing
                }
            }
        }
    }
}

// Exposed API
impl RawResult {
    /// Return current row as a vector of DuckValue
    /// This function assumes that the current row is valid and has been advanced to.
    /// # Returns
    /// * `Result<Vec<DuckValue>>` - The current row as a vector of
    /// `DuckValue`, or an error if the row is invalid or if there are no rows.
    /// # Errors
    /// Returns an error if the current row is invalid or if there are no rows.
    ///
    pub fn current(&mut self) -> Result<AbstractRow> {
        let chunk = self.chunk.as_mut().unwrap();
        let row_idx = chunk.current_row() - 1; // Adjust for 0-based index
        let raw_chunk = unsafe { *chunk.raw() };
        let result = unsafe { self.get_row(raw_chunk, row_idx)? };
        Ok(AbstractRow::new(result, self.column_names.clone()))
    }

    /// Returns the number of columns in the result.
    /// This function assumes that the result is valid and has been properly initialized.
    /// Works for INSERT, UPDATE, DELETE only, and will return 0 for SELECT.
    /// # Returns
    /// * `u64` - The number of columns in the result.
    /// # Errors
    /// Panics if the result is invalid or if the column count cannot be determined.
    #[allow(unused)]
    #[inline]
    pub fn changes(&mut self) -> u64 {
        unsafe { ffi::duckdb_rows_changed(&mut self.res) }
    }

    #[allow(unused)]
    #[inline]
    pub fn column_count(&self) -> u64 {
        self.col_count
    }

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

    #[allow(unused)]
    #[inline]
    pub fn column_name(
        &self,
        col_index: usize,
    ) -> Result<&'static str> {
        if col_index >= self.col_count as usize {
            return Err(Error::InvalidColumnIndex(col_index));
        }
        Ok(self.column_names[col_index])
    }
}
impl Iterator for RawResult {
    type Item = Result<AbstractRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(_) = self.advance() {
            // If we successfully advanced, return the next row
            Some(self.current())
        } else {
            None
        }
    }
}
impl Drop for RawResult {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_result(&mut self.res);
        }
    }
}
