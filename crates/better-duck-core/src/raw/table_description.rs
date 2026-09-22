//! RAII wrapper for `duckdb_table_description` — indexed column metadata for a
//! catalog table.
//!
//! DuckDB's table-description API exposes a table's column *names* and whether each
//! column has a `DEFAULT` expression, addressed by index. The retained C-API set has
//! **no** column-count or per-column-type accessor, so this wrapper deliberately does
//! not invent one: a caller obtains the number of columns from a finite bounds source
//! it already trusts (an appender's column count, or a catalog query such as
//! `SELECT * FROM t LIMIT 0`) and then reads names/defaults for `0..count`.
//!
//! The handle is owned: [`TableDescription`] destroys it exactly once on drop, and a
//! creation failure still yields a handle whose [`error`](TableDescription::error)
//! carries DuckDB's catalog-aware message before it is destroyed.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr;

use crate::{
    error::{Error, Result},
    ffi,
    helpers::duck_result::check_state,
};

/// An owned `duckdb_table_description` with indexed column-name/default access.
pub struct TableDescription {
    inner: ffi::duckdb_table_description,
}

impl std::fmt::Debug for TableDescription {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("TableDescription").finish_non_exhaustive()
    }
}

impl TableDescription {
    /// Creates a description for `schema.table` on `con` (default catalog).
    ///
    /// # Errors
    ///
    /// Returns an error if `schema`/`table` contain an interior NUL, or if DuckDB
    /// cannot describe the table (e.g. it does not exist) — the error carries
    /// DuckDB's catalog-aware message.
    pub(crate) fn create(
        con: ffi::duckdb_connection,
        schema: &str,
        table: &str,
    ) -> Result<TableDescription> {
        let c_schema = CString::new(schema)?;
        let c_table = CString::new(table)?;
        let mut out: ffi::duckdb_table_description = ptr::null_mut();
        // SAFETY: `con` is a valid connection; the name pointers are valid and outlive
        // the call; `out` is a valid out-pointer DuckDB writes the new handle into.
        let state = unsafe {
            ffi::duckdb_table_description_create(con, c_schema.as_ptr(), c_table.as_ptr(), &mut out)
        };
        Self::from_create(state, out)
    }

    /// Creates a description for `[catalog.]schema.table` on `con`. A `None` catalog
    /// uses DuckDB's default.
    ///
    /// # Errors
    ///
    /// As [`create`](TableDescription::create), plus an interior NUL in `catalog`.
    pub(crate) fn create_ext(
        con: ffi::duckdb_connection,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
    ) -> Result<TableDescription> {
        let c_catalog = catalog.map(CString::new).transpose()?;
        let c_schema = CString::new(schema)?;
        let c_table = CString::new(table)?;
        let catalog_ptr = c_catalog.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let mut out: ffi::duckdb_table_description = ptr::null_mut();
        // SAFETY: `con` is valid; the (optional) catalog/schema/table pointers are
        // valid null-terminated strings (or null for the default catalog) that outlive
        // the call; `out` receives the new handle.
        let state = unsafe {
            ffi::duckdb_table_description_create_ext(
                con,
                catalog_ptr,
                c_schema.as_ptr(),
                c_table.as_ptr(),
                &mut out,
            )
        };
        Self::from_create(state, out)
    }

    /// Wraps the `(state, out)` of a create call, surfacing a creation error via the
    /// handle's own message and always taking ownership so the handle is destroyed.
    fn from_create(
        state: ffi::duckdb_state,
        out: ffi::duckdb_table_description,
    ) -> Result<TableDescription> {
        if out.is_null() {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(state),
                Some("failed to create table description".to_owned()),
            ));
        }
        let desc = TableDescription { inner: out };
        if state != ffi::DuckDBSuccess {
            let message =
                desc.error().unwrap_or_else(|| "failed to create table description".to_owned());
            // `desc` drops here on the early return, destroying the handle exactly once.
            return Err(Error::DuckDBFailure(ffi::Error::new(state), Some(message)));
        }
        Ok(desc)
    }

    /// The description's error message, if any.
    ///
    /// (`duckdb_table_description_error`.) The returned string is owned by the
    /// description and freed on destroy; it is copied out here and never freed.
    #[must_use]
    pub fn error(&self) -> Option<String> {
        // SAFETY: `self.inner` is a valid table description; the returned pointer is a
        // borrowed string owned by it (must NOT be freed) or null.
        let ptr = unsafe { ffi::duckdb_table_description_error(self.inner) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is a valid, non-null, null-terminated C string owned by `self`.
        Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned())
    }

    /// The name of the column at `index`, or `None` if DuckDB returns no name.
    ///
    /// `index` must be within the table's column count (obtained from a trusted
    /// bounds source — see the module docs); the returned name is owned.
    #[must_use]
    pub fn column_name(
        &self,
        index: u64,
    ) -> Option<String> {
        // SAFETY: `self.inner` is a valid table description; `duckdb_..._get_column_name`
        // returns a heap `char*` (freed with `duckdb_free`) or null.
        let ptr = unsafe {
            ffi::duckdb_table_description_get_column_name(self.inner, index as ffi::idx_t)
        };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is a valid, non-null, null-terminated C string.
        let name = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
        // SAFETY: `ptr` was allocated by DuckDB and ownership transferred to us.
        unsafe { ffi::duckdb_free(ptr as *mut c_void) };
        Some(name)
    }

    /// Whether the column at `index` has a `DEFAULT` expression.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB reports failure for `index` (e.g. out of range).
    pub fn column_has_default(
        &self,
        index: u64,
    ) -> Result<bool> {
        let mut out = false;
        // SAFETY: `self.inner` is a valid table description; `out` is a valid bool
        // out-pointer DuckDB writes into. A failure (bad index) yields `DuckDBError`.
        let state =
            unsafe { ffi::duckdb_column_has_default(self.inner, index as ffi::idx_t, &mut out) };
        check_state(state)?;
        Ok(out)
    }
}

impl Drop for TableDescription {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            // SAFETY: `self.inner` is a valid handle created by one of the create
            // functions and not yet destroyed; `duckdb_table_description_destroy` is
            // called exactly once here and nulls the pointer.
            unsafe { ffi::duckdb_table_description_destroy(&mut self.inner) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;

    #[test]
    fn describes_columns_names_and_defaults() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR DEFAULT 'x')").unwrap();
        let desc = conn.table_description("main", "t").unwrap();
        assert!(desc.error().is_none());

        // Indexed name access (bounds known from the DDL: 2 columns).
        assert_eq!(desc.column_name(0).as_deref(), Some("id"));
        assert_eq!(desc.column_name(1).as_deref(), Some("name"));

        // `id` has no DEFAULT; `name` does.
        assert!(!desc.column_has_default(0).unwrap());
        assert!(desc.column_has_default(1).unwrap());
    }

    #[test]
    fn create_ext_with_default_catalog_matches_create() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t2 (a INTEGER)").unwrap();
        let desc = conn.table_description_ext(None, "main", "t2").unwrap();
        assert_eq!(desc.column_name(0).as_deref(), Some("a"));
    }

    #[test]
    fn missing_table_is_a_catalog_error() {
        let conn = Connection::open_in_memory().unwrap();
        let err = conn.table_description("main", "does_not_exist").unwrap_err();
        // The message is DuckDB's own catalog-aware text (mentions the missing table).
        let msg = format!("{err:?}");
        assert!(msg.contains("does_not_exist") || msg.to_lowercase().contains("table"), "{msg}");
    }

    #[test]
    fn rejects_interior_nul_in_names() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(matches!(conn.table_description("main", "t\0x"), Err(Error::NulError(_))));
    }
}
