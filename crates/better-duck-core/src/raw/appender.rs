use std::ffi::{c_char, CString};
use std::ptr;

use crate::chunk::DataChunkHandle;
use crate::error::{DuckDBConversionError, Error, Result};
use crate::ffi::{
    duckdb_append_data_chunk, duckdb_append_default, duckdb_append_default_to_chunk,
    duckdb_appender, duckdb_appender_begin_row, duckdb_appender_close,
    duckdb_appender_column_count, duckdb_appender_column_type, duckdb_appender_create,
    duckdb_appender_destroy, duckdb_appender_end_row, duckdb_appender_error, duckdb_appender_flush,
    duckdb_destroy_logical_type, duckdb_get_type_id, DuckDBSuccess,
};
use crate::helpers::duck_result::result_from_duckdb_appender;
use crate::raw::connection::RawConnection;
use crate::types::appendable::AppendAble;

/// A DuckDB appender for bulk-inserting rows into a table without going through
/// the SQL parser.
///
/// Call [`append`](Appender::append) for each row and [`save`](Appender::save)
/// to flush the data to the database. Rows are also flushed automatically on drop
/// (errors during the implicit flush are logged to stderr).
pub struct Appender {
    _con: RawConnection,
    inn: duckdb_appender,
}

impl Appender {
    /// Creates a new `Appender` for the given table and schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist or the DuckDB appender cannot
    /// be created.
    pub fn new(
        con: RawConnection,
        table: &str,
        schema: &str,
    ) -> Result<Appender> {
        let mut appender: duckdb_appender = ptr::null_mut();
        let c_table = CString::new(table)?;
        let c_schema = CString::new(schema)?;
        // SAFETY: `con.con` is a valid open duckdb_connection. `c_schema` and `c_table`
        // are valid null-terminated C strings. `appender` is a valid output pointer.
        let res = unsafe {
            duckdb_appender_create(
                con.con,
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut appender,
            )
        };
        result_from_duckdb_appender(res, &mut appender)
            .map(|_| Appender { _con: con, inn: appender })
    }

    /// Appends a row to the table.
    ///
    /// Calls `duckdb_appender_begin_row`, then the value appender, then
    /// `duckdb_appender_end_row`.
    ///
    /// # Errors
    ///
    /// Returns an error if the row cannot be appended.
    #[must_use = "append result should be checked"]
    #[allow(dead_code)]
    pub fn append<T: AppendAble>(
        &mut self,
        row: &mut T,
    ) -> Result<()> {
        // SAFETY: `self.inn` is a valid duckdb_appender created in `new`.
        let _ = unsafe { duckdb_appender_begin_row(self.inn) };
        row.appender_append(self.inn)?;
        // SAFETY: `self.inn` is a valid duckdb_appender; `begin_row` was called above.
        let rc = unsafe { duckdb_appender_end_row(self.inn) };
        result_from_duckdb_appender(rc, &mut self.inn)
    }

    /// Appends every row of `chunk` in one call — the columnar counterpart to
    /// calling [`append`](Self::append) once per row, backed by
    /// `duckdb_append_data_chunk`.
    ///
    /// Requires an **exact** column-count and column-type match against this
    /// appender's target columns — checked up front, entirely on the Rust
    /// side, before any DuckDB call. This is deliberately stricter than the
    /// underlying C API (which attempts an implicit cast on a type mismatch):
    /// a caller who wants DuckDB's cast behavior should convert `chunk`
    /// itself first. Because the mismatch is caught before the appender is
    /// touched, the appender is unaffected by this error and stays usable.
    ///
    /// # Errors
    ///
    /// Returns an error if `chunk`'s column count or any column's type
    /// doesn't match this appender's target columns, or if the underlying
    /// append fails (e.g. a constraint violation).
    #[must_use = "append_chunk result should be checked"]
    pub fn append_chunk(
        &mut self,
        chunk: &mut DataChunkHandle,
    ) -> Result<()> {
        self.check_chunk_shape(chunk)?;
        // SAFETY: `self.inn` is a valid duckdb_appender; `chunk.as_raw()` is a
        // valid data chunk whose shape was just checked above.
        let rc = unsafe { duckdb_append_data_chunk(self.inn, chunk.as_raw()) };
        result_from_duckdb_appender(rc, &mut self.inn)
    }

    /// Compares `chunk`'s column count and per-column types against this
    /// appender's target columns, entirely via `duckdb_appender_*`
    /// introspection — no append is attempted.
    fn check_chunk_shape(
        &self,
        chunk: &DataChunkHandle,
    ) -> Result<()> {
        // SAFETY: `self.inn` is a valid duckdb_appender.
        let expected_cols = unsafe { duckdb_appender_column_count(self.inn) } as usize;
        let got_cols = chunk.num_columns();
        if expected_cols != got_cols {
            return Err(Error::ConversionError(DuckDBConversionError::ConversionError(format!(
                "append_chunk: appender expects {expected_cols} columns, chunk has {got_cols}"
            ))));
        }
        for i in 0..expected_cols {
            // SAFETY: `self.inn` is valid; `i` is within `expected_cols`, just
            // confirmed to match the appender's own column count above. The
            // returned logical type is a fresh, caller-owned handle.
            let mut lt = unsafe { duckdb_appender_column_type(self.inn, i as u64) };
            // SAFETY: `lt` was just obtained above and is a valid logical type.
            let expected_type = unsafe { duckdb_get_type_id(lt) };
            // SAFETY: `lt` was allocated above; destroy exactly once.
            unsafe { duckdb_destroy_logical_type(&mut lt) };
            let got_type = chunk.column_type_id(i)?;
            if expected_type != got_type {
                return Err(Error::ConversionError(DuckDBConversionError::TypeMismatch {
                    expected: expected_type,
                    found: got_type,
                }));
            }
        }
        Ok(())
    }

    /// Appends one row of column `DEFAULT`s (`NULL` for a column with no
    /// `DEFAULT`) — the only way to get a column's declared default rather
    /// than an explicit value.
    ///
    /// `duckdb_append_default` itself appends only *one* column's default
    /// within the current row (mirroring `duckdb_append_int32` and friends);
    /// this wraps it in the same `begin_row`/one-call-per-column/`end_row`
    /// framing [`append`](Self::append) uses, once per
    /// `duckdb_appender_column_count` column.
    ///
    /// # Errors
    ///
    /// Returns an error if the append fails.
    #[must_use = "append_default_row result should be checked"]
    pub fn append_default_row(&mut self) -> Result<()> {
        // SAFETY: `self.inn` is a valid duckdb_appender.
        let _ = unsafe { duckdb_appender_begin_row(self.inn) };
        // SAFETY: `self.inn` is a valid duckdb_appender.
        let col_count = unsafe { duckdb_appender_column_count(self.inn) };
        for _ in 0..col_count {
            // SAFETY: `self.inn` is a valid duckdb_appender; `begin_row` was
            // called above and this call consumes exactly one column's slot,
            // matching `col_count` calls to fill the whole row.
            let rc = unsafe { duckdb_append_default(self.inn) };
            if rc != DuckDBSuccess {
                return result_from_duckdb_appender(rc, &mut self.inn);
            }
        }
        // SAFETY: `self.inn` is a valid duckdb_appender; every column's slot
        // was filled by the loop above.
        let rc = unsafe { duckdb_appender_end_row(self.inn) };
        result_from_duckdb_appender(rc, &mut self.inn)
    }

    /// Writes column `col`'s `DEFAULT` (`NULL` if it has none) into `chunk` at
    /// `row`, via `duckdb_append_default_to_chunk` — the chunk-based
    /// counterpart to [`append_default_row`](Self::append_default_row), for
    /// filling in one column of an otherwise explicitly-populated
    /// [`DataChunkHandle`] before [`append_chunk`](Self::append_chunk).
    ///
    /// # Errors
    ///
    /// Returns an error if `col`/`row` are out of range for `chunk`, or the
    /// underlying call fails.
    #[must_use = "set_chunk_default result should be checked"]
    pub fn set_chunk_default(
        &self,
        chunk: &mut DataChunkHandle,
        col: usize,
        row: usize,
    ) -> Result<()> {
        if col >= chunk.num_columns() {
            return Err(Error::InvalidColumnIndex(col));
        }
        // SAFETY: `self.inn` is a valid duckdb_appender; `chunk.as_raw()` is a
        // valid data chunk; `col` was just bounds-checked. DuckDB itself
        // bounds-checks `row` against the chunk's capacity.
        let rc = unsafe {
            duckdb_append_default_to_chunk(self.inn, chunk.as_raw(), col as u64, row as u64)
        };
        if rc == DuckDBSuccess {
            return Ok(());
        }
        // Deliberately not `result_from_duckdb_appender`: unlike a real
        // append, a failure here doesn't leave the appender's own row-buffer
        // state ambiguous (nothing was flushed or begun), so destroying the
        // whole appender over it would be needlessly destructive.
        // SAFETY: `self.inn` is a valid duckdb_appender.
        let message = unsafe {
            let c_err = duckdb_appender_error(self.inn);
            if c_err.is_null() {
                None
            } else {
                Some(std::ffi::CStr::from_ptr(c_err).to_string_lossy().into_owned())
            }
        };
        Err(Error::DuckDBFailure(crate::ffi::Error::new(rc), message))
    }

    /// Flushes all buffered rows to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    #[must_use = "save result should be checked"]
    #[allow(dead_code)]
    pub fn save(&mut self) -> Result<()> {
        // SAFETY: `self.inn` is a valid duckdb_appender.
        self.flush()
    }

    /// Flushes the appender's internal buffer.
    ///
    /// # Safety
    ///
    /// `self.inn` must be a valid, non-null `duckdb_appender`.
    fn flush(&mut self) -> Result<()> {
        // SAFETY: `self.inn` is a valid duckdb_appender (enforced by the caller).
        let res = unsafe { duckdb_appender_flush(self.inn) };
        result_from_duckdb_appender(res, &mut self.inn)
    }
}

impl Drop for Appender {
    fn drop(&mut self) {
        if self.inn.is_null() {
            return;
        }
        // [err-result-over-panic] — log on flush failure; never panic in Drop.
        // SAFETY: `self.inn` is non-null (checked above); it is a valid duckdb_appender
        // created in `new`. After close and destroy it is invalidated. The null guard
        // above ensures this runs at most once.
        if let Err(e) = self.flush() {
            eprintln!("[better-duck] appender flush on drop failed: {e}");
        }
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender (null guard above).
        // Close and destroy are safe to call in sequence; after destroy the handle is
        // invalid and will not be used again.
        unsafe {
            duckdb_appender_close(self.inn);
            duckdb_appender_destroy(&mut self.inn);
        }
    }
}

#[cfg(test)]
mod appender_tests {
    use crate::{
        ffi::{duckdb_append_int32, duckdb_append_varchar, duckdb_bind_int32, duckdb_bind_varchar},
        types::value::DuckValue,
    };

    use super::*;
    use crate::{config::Config, error::DuckDBConversionError, helpers::path::path_to_cstring};

    #[derive(Debug)]
    struct Row(i32, &'static str);

    impl AppendAble for Row {
        fn appender_append(
            &mut self,
            appender: duckdb_appender,
        ) -> crate::error::Result<()> {
            // SAFETY: `appender` is a valid duckdb_appender from `Appender::new`;
            // we are inside a begin_row/end_row pair. The int32 and varchar values are
            // valid for their respective columns.
            unsafe {
                duckdb_append_int32(appender, self.0);
                let st = CString::new(self.1)
                    .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
                    .unwrap();
                duckdb_append_varchar(appender, st.as_ptr());
            }
            Ok(())
        }
        fn stmt_append(
            &mut self,
            idx: u64,
            stmt: crate::ffi::duckdb_prepared_statement,
        ) -> Result<()> {
            // SAFETY: `stmt` is a valid prepared statement; `idx` is a 1-based parameter
            // index within the statement's parameter count.
            unsafe {
                duckdb_bind_int32(stmt, idx, self.0);
                let st = CString::new(self.1)
                    .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
                    .unwrap();
                duckdb_bind_varchar(stmt, idx + 1, st.as_ptr());
            }
            Ok(())
        }
    }

    fn get_test_connection() -> RawConnection {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    #[test]
    fn test_appender_create_and_drop() {
        let mut con = get_test_connection();

        let create_sql = "CREATE TABLE test_appender (id INTEGER, name VARCHAR)";
        let _ = con.query(create_sql).unwrap();

        let appender = Appender::new(con.clone(), "test_appender", "main");
        assert!(appender.is_ok());
    }

    #[test]
    fn test_appender_append_and_flush() {
        let mut con = get_test_connection();

        let _ = con.query("CREATE TABLE test_append (id INTEGER, name VARCHAR)").unwrap();

        let mut appender = Appender::new(con.clone(), "test_append", "main").unwrap();
        let mut row = Row(1, "Alice");
        let mut row2 = Row(2, "Sara");
        let mut row3 = Row(3, "Charlie");

        appender.append(&mut row).unwrap();
        appender.append(&mut row2).unwrap();
        appender.append(&mut row3).unwrap();
        appender.save().unwrap();

        let mut stmt = con.prepare("SELECT id,name FROM test_append WHERE id=123").unwrap();
        let mut rows = stmt.execute().unwrap();
        assert!(rows.next().is_none(), "Row with id=123 should not exist");

        let mut stmt = con.prepare("SELECT id,name FROM test_append").unwrap();
        let rows = stmt.execute().unwrap();
        for row in rows {
            assert!(row.is_ok());
            let row = row.unwrap();
            let id = match row.get("id").unwrap() {
                DuckValue::Int(id) => id,
                other => panic!("Expected Int for 'id', got {:?}", other),
            };
            assert!([1, 2, 3].contains(id), "Row with id={} should exist", id);
            let name = match row.get("name").unwrap() {
                DuckValue::Text(name) => name.as_str(),
                other => panic!("Expected Str for 'name', got {:?}", other),
            };
            match id {
                1 => assert_eq!(name, "Alice"),
                2 => assert_eq!(name, "Sara"),
                3 => assert_eq!(name, "Charlie"),
                _ => panic!("Unexpected row id: {}", id),
            }
        }
    }

    #[test]
    fn test_appender_error_on_invalid_table() {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        let con = RawConnection::open_with_flags(&c_path, config).unwrap();

        let appender = Appender::new(con, "nonexistent_table", "main");
        assert!(appender.is_err());
    }
}
