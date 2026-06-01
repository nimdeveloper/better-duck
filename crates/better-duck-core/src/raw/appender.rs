use std::ffi::{c_char, CString};
use std::ptr;

use crate::error::Result;
use crate::ffi::{
    duckdb_appender, duckdb_appender_begin_row, duckdb_appender_close, duckdb_appender_create,
    duckdb_appender_destroy, duckdb_appender_end_row, duckdb_appender_flush,
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
