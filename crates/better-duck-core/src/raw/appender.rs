use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use crate::error::{EngineError, Error, Result};
use crate::ffi::{
    duckdb_appender, duckdb_appender_begin_row, duckdb_appender_close, duckdb_appender_create,
    duckdb_appender_destroy, duckdb_appender_end_row, duckdb_appender_error_data,
    duckdb_appender_flush, DuckDBSuccess,
};
use crate::helpers::duck_result::result_from_duckdb_appender;
use crate::raw::connection::ConnectionInner;
use crate::raw::error_data::ErrorData;
use crate::types::appendable::AppendAble;

/// A DuckDB appender for bulk-inserting rows into a table without going through
/// the SQL parser.
///
/// Call [`append`](Appender::append) for each row and [`save`](Appender::save)
/// to flush the data to the database. Rows are also flushed automatically on drop
/// (errors during the implicit flush are logged to stderr).
pub struct Appender {
    /// Keeps the connection this appender was created on open for at least as long
    /// as the appender, and ties appended rows to that connection's transaction.
    _connection: Arc<ConnectionInner>,
    inn: duckdb_appender,
}

impl Appender {
    /// Creates a new `Appender` for the given table and schema.
    ///
    /// Takes the shared connection owner rather than a `RawConnection` so that rows
    /// are appended on the *caller's* connection, and therefore inside any
    /// transaction open on it.
    ///
    /// Crate-internal because [`ConnectionInner`] is: external callers construct
    /// appenders through [`Connection::appender`](crate::connection::Connection::appender).
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist or the DuckDB appender cannot
    /// be created.
    pub(crate) fn new(
        connection: Arc<ConnectionInner>,
        table: &str,
        schema: &str,
    ) -> Result<Appender> {
        let mut appender: duckdb_appender = ptr::null_mut();
        let c_table = CString::new(table)?;
        let c_schema = CString::new(schema)?;
        // SAFETY: `connection`'s handle is a valid open duckdb_connection, kept alive by
        // the `Arc` this appender retains. `c_schema` and `c_table` are valid
        // null-terminated C strings. `appender` is a valid output pointer.
        let res = unsafe {
            duckdb_appender_create(
                connection.handle(),
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut appender,
            )
        };
        result_from_duckdb_appender(res, &mut appender)
            .map(|_| Appender { _connection: connection, inn: appender })
    }

    /// Appends a row to the table.
    ///
    /// Calls `duckdb_appender_begin_row`, then the value appender, then
    /// `duckdb_appender_end_row`.
    ///
    /// # Errors
    ///
    /// Returns an error if the row cannot be appended. Engine-side failures carry
    /// DuckDB's typed classification via [`Error::Engine`].
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
        self.check(rc)
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
    fn flush(&mut self) -> Result<()> {
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender (upheld by the
        // constructor and the Drop null-guard).
        let res = unsafe { duckdb_appender_flush(self.inn) };
        self.check(res)
    }

    /// Maps a DuckDB appender status into a typed `Result`.
    ///
    /// On failure this reads the appender's [`ErrorData`], which carries DuckDB's
    /// own error classification, instead of the deprecated bare-string
    /// `duckdb_appender_error`. The handle stays valid so the caller can recover
    /// or destroy it on drop.
    fn check(
        &mut self,
        code: crate::ffi::duckdb_state,
    ) -> Result<()> {
        if code == DuckDBSuccess {
            return Ok(());
        }
        Err(Error::Engine(self.error_data().unwrap_or_else(|| {
            EngineError::unavailable(Some(
                "appender reported failure without error data".to_owned(),
            ))
        })))
    }

    /// Reads DuckDB's typed error for this appender, if any is set.
    ///
    /// Returns `None` when the appender is not in an error state. The returned
    /// error is fully owned: the underlying `duckdb_error_data` handle is
    /// destroyed before this returns.
    fn error_data(&mut self) -> Option<EngineError> {
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender. DuckDB returns an
        // owned `duckdb_error_data` handle (or null); `ErrorData` takes ownership and
        // destroys it, and `to_engine_error` copies every field out first.
        let data = unsafe { ErrorData::from_raw(duckdb_appender_error_data(self.inn)) }?;
        data.has_error().then(|| data.to_engine_error())
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
        error::{DuckDBConversionError, Error},
        ffi::{duckdb_append_int32, duckdb_append_varchar, duckdb_bind_int32, duckdb_bind_varchar},
        raw::connection::RawConnection,
        types::value::DuckValue,
    };

    use super::*;
    use crate::{config::Config, helpers::path::path_to_cstring};

    const ROW_FAILURE: &str = "intentional row failure";

    #[derive(Debug)]
    struct Row(i32, &'static str);

    struct FailingRow;

    impl AppendAble for FailingRow {
        fn appender_append(
            &mut self,
            _appender: duckdb_appender,
        ) -> Result<()> {
            Err(Error::ConversionError(DuckDBConversionError::ConversionError(
                ROW_FAILURE.to_owned(),
            )))
        }

        fn stmt_append(
            &mut self,
            _idx: u64,
            _stmt: crate::ffi::duckdb_prepared_statement,
        ) -> Result<()> {
            unreachable!("FailingRow is only used with Appender")
        }
    }

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

    fn single_int(
        con: &RawConnection,
        sql: &str,
        column: &str,
    ) -> i32 {
        let mut result = con.prepare(sql).unwrap().execute().unwrap();
        let row = result.next().expect("expected one row").unwrap();
        match row.get(column).unwrap() {
            DuckValue::Int(value) => *value,
            other => panic!("Expected Int for '{column}', got {other:?}"),
        }
    }

    fn assert_row_exists(
        con: &RawConnection,
        table: &str,
        id: i32,
    ) {
        let sql = format!("SELECT id FROM {table} WHERE id = {id}");
        assert_eq!(single_int(con, &sql, "id"), id);
    }

    #[test]
    fn test_appender_create_and_drop() {
        let mut con = get_test_connection();

        let create_sql = "CREATE TABLE test_appender (id INTEGER, name VARCHAR)";
        let _ = con.query(create_sql).unwrap();

        let appender = con.appender("test_appender", "main");
        assert!(appender.is_ok());
    }

    #[test]
    fn test_appender_append_and_flush() {
        let mut con = get_test_connection();

        let _ = con.query("CREATE TABLE test_append (id INTEGER, name VARCHAR)").unwrap();

        let mut appender = con.appender("test_append", "main").unwrap();
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
    fn test_appender_rejects_interior_nul_table_name() {
        let error = get_test_connection()
            .appender("test\0table", "main")
            .err()
            .expect("interior NUL table name should fail");
        assert!(matches!(error, Error::NulError(_)));
    }

    #[test]
    fn test_appender_rejects_interior_nul_schema_name() {
        let error = get_test_connection()
            .appender("test_table", "ma\0in")
            .err()
            .expect("interior NUL schema name should fail");
        assert!(matches!(error, Error::NulError(_)));
    }

    #[test]
    fn test_appender_error_on_nonexistent_schema() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE test_table (id INTEGER)").unwrap();

        let appender = con.appender("test_table", "nonexistent_schema");
        assert!(appender.is_err());
    }

    #[test]
    fn test_appender_error_on_nonexistent_table() {
        let mut con = get_test_connection();

        let appender = con.appender("nonexistent_table", "main");
        assert!(appender.is_err());
    }

    #[test]
    fn test_save_flushes_rows_for_another_connection() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE saved_rows (id INTEGER, name VARCHAR)").unwrap();
        let reader = con.try_clone().unwrap();
        let mut appender = con.appender("saved_rows", "main").unwrap();

        appender.append(&mut Row(11, "saved")).unwrap();
        appender.save().unwrap();

        assert_row_exists(&reader, "saved_rows", 11);
    }

    #[test]
    fn test_drop_flushes_rows_for_another_connection() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE dropped_rows (id INTEGER, name VARCHAR)").unwrap();
        let reader = con.try_clone().unwrap();

        {
            let mut appender = con.appender("dropped_rows", "main").unwrap();
            appender.append(&mut Row(12, "dropped")).unwrap();
        }

        assert_row_exists(&reader, "dropped_rows", 12);
    }

    #[test]
    fn test_append_propagates_row_error_and_connection_remains_usable() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE failed_rows (id INTEGER, name VARCHAR)").unwrap();
        let mut appender = con.appender("failed_rows", "main").unwrap();

        let error = appender.append(&mut FailingRow).unwrap_err();
        assert!(matches!(
            error,
            Error::ConversionError(DuckDBConversionError::ConversionError(ref message))
                if message == ROW_FAILURE
        ));
        drop(appender);

        con.query("INSERT INTO failed_rows VALUES (13, 'usable')").unwrap();
        assert_row_exists(&con, "failed_rows", 13);
    }

    /// An engine-side append failure (here a NOT NULL violation, raised at flush)
    /// surfaces as a typed [`Error::Engine`] carrying DuckDB's classification and
    /// message, read via `duckdb_appender_error_data` — not the deprecated
    /// bare-string path.
    #[test]
    fn engine_append_failure_surfaces_typed_error_data() {
        use crate::error::EngineErrorKind;

        let mut con = get_test_connection();
        // A CHECK constraint (unlike PRIMARY KEY) builds no index, so the appender
        // does not deadlock when dropped after the failed flush. Poisoned-appender
        // recovery is E4's concern; here we only assert the typed error surfaces.
        con.query("CREATE TABLE typed_fail (id INTEGER CHECK (id > 0))").unwrap();
        let mut appender = con.appender("typed_fail", "main").unwrap();

        // Append a value the CHECK rejects. DuckDB defers the constraint check, so
        // the failure appears when the buffered row is flushed.
        appender.append(&mut DuckValue::Int(-1)).unwrap();
        let error = appender.save().unwrap_err();

        let engine = match error {
            Error::Engine(engine) => engine,
            other => panic!("expected a typed engine error, got {other:?}"),
        };
        // DuckDB classifies this rather than leaving it Unavailable, and the driver
        // reports that classification instead of parsing the message text.
        assert_ne!(engine.kind, EngineErrorKind::Unavailable, "kind should be typed by DuckDB");
        assert!(engine.message.is_some(), "DuckDB should supply a message");
    }
}
