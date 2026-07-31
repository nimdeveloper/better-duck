//! [`TableDescription`] — schema introspection for a table, without issuing
//! a query.

use std::ffi::{c_void, CStr, CString};

use crate::{
    chunk::LogicalType,
    error::{Error, Result},
    ffi::{
        self, duckdb_column_has_default, duckdb_connection, duckdb_free, duckdb_table_description,
        duckdb_table_description_create, duckdb_table_description_destroy,
        duckdb_table_description_error, duckdb_table_description_get_column_count,
        duckdb_table_description_get_column_name, duckdb_table_description_get_column_type,
    },
};

/// An owned handle describing a table's schema — column names, types, and
/// which columns have a `DEFAULT` expression — obtained without running a
/// query. See
/// [`Connection::table_description`](crate::connection::Connection::table_description).
#[derive(Debug)]
pub struct TableDescription {
    ptr: duckdb_table_description,
}

impl TableDescription {
    /// Describes `table` in `schema` (pass `None` for the default schema)
    /// on `con`.
    ///
    /// # Errors
    ///
    /// Returns an error naming the table if it does not exist, or `schema`/
    /// `table` contain an interior NUL byte.
    pub(crate) fn create(
        con: duckdb_connection,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Self> {
        let c_table = CString::new(table)?;
        let c_schema = schema.map(CString::new).transpose()?;
        let schema_ptr = c_schema.as_ref().map_or(std::ptr::null(), |s| s.as_ptr());
        let mut ptr: duckdb_table_description = std::ptr::null_mut();
        // SAFETY: `con` is a valid open connection; `schema_ptr` is either
        // null or a valid NUL-terminated C string for the duration of this
        // call; `c_table` likewise; `&mut ptr` is a valid output pointer.
        // Per the C API contract, `ptr` must be destroyed even on failure —
        // handled by wrapping it in `Self` and letting `Drop` run either way.
        let r =
            unsafe { duckdb_table_description_create(con, schema_ptr, c_table.as_ptr(), &mut ptr) };
        let description = Self { ptr };
        if r != ffi::DuckDBSuccess {
            // SAFETY: `description.ptr` is a valid (possibly error-only)
            // table_description handle for the duration of this call.
            let msg = unsafe { duckdb_table_description_error(description.ptr) };
            // DuckDB's own message (e.g. "No table with that schema+name
            // could be located") doesn't always name the table — always
            // prefix with it ourselves so callers can tell which lookup
            // failed without re-threading `table` through separately.
            let message = if msg.is_null() {
                format!("table `{table}` not found")
            } else {
                // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8
                // message, owned by `description.ptr` — not freed by us; it
                // is deallocated when `description` is dropped.
                let duckdb_message = unsafe { CStr::from_ptr(msg) }.to_string_lossy().into_owned();
                format!("table `{table}`: {duckdb_message}")
            };
            return Err(Error::DuckDBFailure(ffi::Error::new(r), Some(message)));
        }
        Ok(description)
    }

    /// Returns the number of columns in the described table.
    #[inline]
    pub fn column_count(&self) -> usize {
        // SAFETY: `self.ptr` is a valid table_description for the lifetime of `self`.
        unsafe { duckdb_table_description_get_column_count(self.ptr) as usize }
    }

    /// Returns the name of the column at `idx`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn column_name(
        &self,
        idx: usize,
    ) -> Result<String> {
        // SAFETY: `self.ptr` is a valid table_description; `idx` need not be
        // pre-validated — DuckDB returns null for an out-of-range index.
        let raw = unsafe { duckdb_table_description_get_column_name(self.ptr, idx as u64) };
        if raw.is_null() {
            return Err(Error::InvalidColumnIndex(idx));
        }
        // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8 string,
        // caller-owned — freed via `duckdb_free` exactly once, below.
        let name = unsafe { CStr::from_ptr(raw) }.to_string_lossy().into_owned();
        // SAFETY: `raw` was allocated by DuckDB and must be freed with
        // `duckdb_free`, per this function's documented contract.
        unsafe { duckdb_free(raw.cast::<c_void>()) };
        Ok(name)
    }

    /// Returns the full logical type of the column at `idx`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn column_type(
        &self,
        idx: usize,
    ) -> Result<LogicalType> {
        // SAFETY: `self.ptr` is a valid table_description; `idx` need not be
        // pre-validated — DuckDB returns null for an out-of-range index.
        let raw = unsafe { duckdb_table_description_get_column_type(self.ptr, idx as u64) };
        LogicalType::from_raw(raw).map_err(|_| Error::InvalidColumnIndex(idx))
    }

    /// Returns whether the column at `idx` has a `DEFAULT` expression.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn column_has_default(
        &self,
        idx: usize,
    ) -> Result<bool> {
        let mut out = false;
        // SAFETY: `self.ptr` is a valid table_description; `&mut out` is a
        // valid output pointer.
        let r = unsafe { duckdb_column_has_default(self.ptr, idx as u64, &mut out) };
        if r != ffi::DuckDBSuccess {
            return Err(Error::InvalidColumnIndex(idx));
        }
        Ok(out)
    }
}

impl Drop for TableDescription {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid table_description allocated by
            // `duckdb_table_description_create` (even on the error path, per
            // its documented contract) and not yet destroyed; destroyed
            // exactly once here.
            unsafe { duckdb_table_description_destroy(&mut self.ptr) };
        }
    }
}
