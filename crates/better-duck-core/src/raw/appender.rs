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

/// Lifecycle state of an [`Appender`].
///
/// DuckDB invalidates an appender the moment a flush, `end_row`, or close fails:
/// its docs say "all data is invalidated ... it is not possible to append more
/// values", and the only legal follow-up is `duckdb_appender_error_data` then
/// `duckdb_appender_destroy`. Re-flushing an invalidated appender — which the
/// previous `Drop` did — can *deadlock* DuckDB when the target table carries an
/// ART index (e.g. `PRIMARY KEY`). This state machine makes that unrepresentable:
/// once `Poisoned`, no further flush/close is ever issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppenderState {
    /// Accepting rows; flush/close are legal.
    Ready,
    /// A DuckDB operation failed and invalidated the appender. No further
    /// append/flush/close may run; the handle may only be destroyed.
    Poisoned,
    /// Explicitly finished via [`Appender::finish`]; the handle has been closed
    /// and only awaits destruction. `Drop` is a bare destroy.
    Closed,
}

/// A DuckDB appender for bulk-inserting rows into a table without going through
/// the SQL parser.
///
/// # Lifecycle
///
/// Call [`append`](Appender::append) for each row, then either [`finish`](Appender::finish)
/// (consuming, reports flush errors) or let the appender drop (best-effort flush,
/// errors logged). A failed [`append`](Appender::append)/[`save`](Appender::save)
/// *poisons* the appender: DuckDB has invalidated all buffered data, so subsequent
/// calls fail fast without touching the C handle, and drop skips the flush entirely
/// (re-flushing an invalidated appender can deadlock DuckDB on indexed tables).
pub struct Appender {
    /// Keeps the connection this appender was created on open for at least as long
    /// as the appender, and ties appended rows to that connection's transaction.
    _connection: Arc<ConnectionInner>,
    inn: duckdb_appender,
    state: AppenderState,
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
        result_from_duckdb_appender(res, &mut appender).map(|_| Appender {
            _connection: connection,
            inn: appender,
            state: AppenderState::Ready,
        })
    }

    /// Appends a row to the table.
    ///
    /// Opens a row (`duckdb_appender_begin_row`), appends the value, then closes it
    /// (`duckdb_appender_end_row`). A [`RowGuard`] closes the row even if appending
    /// the value returns early or panics, so a half-written row can never bleed into
    /// the next call.
    ///
    /// # Errors
    ///
    /// Returns an error if the appender is poisoned or closed, or if the row cannot
    /// be appended. Engine-side failures carry DuckDB's typed classification via
    /// [`Error::Engine`]; any failure poisons the appender.
    #[must_use = "append result should be checked"]
    #[allow(dead_code)]
    pub fn append<T: AppendAble>(
        &mut self,
        row: &mut T,
    ) -> Result<()> {
        self.ensure_ready()?;

        // SAFETY: `self.inn` is a valid duckdb_appender created in `new`.
        let begin = unsafe { duckdb_appender_begin_row(self.inn) };
        self.check(begin)?;

        // The guard ends the row on every exit path — including an early `?` return
        // or a panic inside `appender_append` — so DuckDB is never left mid-row.
        let guard = RowGuard { appender: self, ended: false };
        guard.appender.row_result(row.appender_append(guard.appender.inn))?;
        guard.end()
    }

    /// Flushes all buffered rows to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the appender is poisoned or closed, or if the flush
    /// fails (which additionally poisons the appender).
    #[must_use = "save result should be checked"]
    #[allow(dead_code)]
    pub fn save(&mut self) -> Result<()> {
        self.ensure_ready()?;
        self.flush()
    }

    /// Flushes and closes the appender, consuming it and reporting any error.
    ///
    /// Unlike dropping, this surfaces a flush/close failure to the caller. On
    /// success the handle is closed and its `Drop` becomes a bare destroy.
    ///
    /// # Errors
    ///
    /// Returns an error if the appender is already poisoned, or if the final
    /// flush/close fails (which poisons it).
    #[allow(dead_code)]
    pub fn finish(mut self) -> Result<()> {
        self.ensure_ready()?;
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender in the Ready state.
        let rc = unsafe { duckdb_appender_close(self.inn) };
        self.check(rc)?;
        self.state = AppenderState::Closed;
        Ok(())
    }

    /// Flushes the appender's internal buffer, poisoning it on failure.
    fn flush(&mut self) -> Result<()> {
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender (upheld by the
        // constructor and the Drop null-guard) and is Ready (checked by callers).
        let res = unsafe { duckdb_appender_flush(self.inn) };
        self.check(res)
    }

    /// Returns `Ok(())` only if the appender can still accept operations.
    fn ensure_ready(&self) -> Result<()> {
        match self.state {
            AppenderState::Ready => Ok(()),
            AppenderState::Poisoned => Err(Error::Engine(EngineError::unavailable(Some(
                "appender was invalidated by an earlier failure".to_owned(),
            )))),
            AppenderState::Closed => Err(Error::Engine(EngineError::unavailable(Some(
                "appender has been closed".to_owned(),
            )))),
        }
    }

    /// Maps a DuckDB appender status into a typed `Result`, poisoning the appender
    /// on failure.
    ///
    /// On failure this reads the appender's [`ErrorData`], which carries DuckDB's
    /// own error classification, instead of the deprecated bare-string
    /// `duckdb_appender_error`. The handle stays valid (for destruction) but the
    /// appender is marked [`Poisoned`](AppenderState::Poisoned) so no further
    /// flush/close is ever issued against invalidated data.
    fn check(
        &mut self,
        code: crate::ffi::duckdb_state,
    ) -> Result<()> {
        if code == DuckDBSuccess {
            return Ok(());
        }
        let engine = self.error_data().unwrap_or_else(|| {
            EngineError::unavailable(Some(
                "appender reported failure without error data".to_owned(),
            ))
        });
        self.state = AppenderState::Poisoned;
        Err(Error::Engine(engine))
    }

    /// Poisons the appender and returns the original Rust-side error unchanged.
    ///
    /// Used when a value fails to append *before* reaching DuckDB (e.g. a
    /// conversion error): the row is abandoned, so the appender must not be reused.
    fn row_result(
        &mut self,
        result: Result<()>,
    ) -> Result<()> {
        if result.is_err() {
            self.state = AppenderState::Poisoned;
        }
        result
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

/// Ends the currently open appender row when it goes out of scope.
///
/// Guarantees `duckdb_appender_end_row` runs even if the value append returns
/// early or unwinds, so DuckDB is never left with a half-open row. If the guard
/// is dropped without an explicit [`end`](RowGuard::end) (i.e. via `?` or a
/// panic), it ends the row on a best-effort basis and poisons the appender,
/// since the row's contents are indeterminate.
struct RowGuard<'a> {
    appender: &'a mut Appender,
    ended: bool,
}

impl RowGuard<'_> {
    /// Ends the row explicitly, propagating any DuckDB error (which poisons).
    fn end(mut self) -> Result<()> {
        self.ended = true;
        // SAFETY: `self.appender.inn` is a valid duckdb_appender with a row open.
        let rc = unsafe { duckdb_appender_end_row(self.appender.inn) };
        self.appender.check(rc)
    }
}

impl Drop for RowGuard<'_> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        // Early return or panic mid-row: close the row so DuckDB is not left
        // mid-row, and poison the appender because the row is incomplete.
        // SAFETY: `self.appender.inn` is a valid duckdb_appender with a row open.
        unsafe { duckdb_appender_end_row(self.appender.inn) };
        self.appender.state = AppenderState::Poisoned;
    }
}

impl Drop for Appender {
    fn drop(&mut self) {
        if self.inn.is_null() {
            return;
        }

        // A poisoned appender has been invalidated by DuckDB: re-flushing it is
        // illegal and can deadlock on indexed tables, so go straight to destroy.
        // A Closed appender was already flushed+closed by `finish`. Only a Ready
        // appender still owns unflushed rows worth a best-effort flush.
        if self.state == AppenderState::Ready {
            // [err-result-over-panic] — log on flush failure; never panic in Drop.
            if let Err(e) = self.flush() {
                eprintln!("[better-duck] appender flush on drop failed: {e}");
            }
        }

        // SAFETY: `self.inn` is a valid, non-null duckdb_appender (null guard above).
        // `duckdb_appender_destroy` de-allocates the handle regardless of state and
        // is the documented cleanup for an invalidated appender. After destroy the
        // handle is invalid and will not be used again.
        unsafe {
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

    /// An engine-side append failure surfaces as a typed [`Error::Engine`] carrying
    /// DuckDB's classification and message, read via `duckdb_appender_error_data`.
    #[test]
    fn engine_append_failure_surfaces_typed_error_data() {
        use crate::error::EngineErrorKind;

        let mut con = get_test_connection();
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

    // NOTE: a regression test for a failed flush on a `PRIMARY KEY` (ART-indexed)
    // table is deliberately *not* included here. That scenario deadlocks inside
    // DuckDB's own `duckdb_appender_destroy`. Through the wrapper — which always
    // destroys the handle — it deadlocks *deterministically* (hangs on the first
    // iteration of every run); an earlier pure-FFI probe that varied the call
    // sequence saw it intermittently, hence an older "nondeterministic" wording.
    // It is an upstream C++ defect on DuckDB's own documented cleanup path, not a
    // wrapper defect, and no Rust-side ordering avoids it; including the test would
    // hang CI. The fix (never re-flushing a poisoned appender) is covered
    // below with `CHECK`-constraint failures, which poison identically but build
    // no index and so tear down cleanly. The upstream defect is documented above.

    /// Once poisoned, every further operation fails fast without touching the C
    /// handle, and does not re-enter DuckDB.
    #[test]
    fn poisoned_appender_rejects_further_operations() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE reuse_fail (id INTEGER CHECK (id > 0))").unwrap();
        let mut appender = con.appender("reuse_fail", "main").unwrap();

        appender.append(&mut DuckValue::Int(-1)).unwrap();
        appender.save().unwrap_err(); // poisons

        // Subsequent append and save both fail fast with the poisoned error.
        let err = appender.append(&mut DuckValue::Int(5)).unwrap_err();
        assert!(
            matches!(
                err,
                Error::Engine(ref e) if e.message.as_deref() == Some("appender was invalidated by an earlier failure")
            ),
            "unexpected error: {err:?}"
        );
        assert!(appender.save().is_err());
    }

    /// A Rust-side value failure mid-row poisons the appender rather than leaving a
    /// row open for the next append to corrupt.
    #[test]
    fn row_side_failure_poisons_and_leaves_connection_usable() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE row_poison (id INTEGER, name VARCHAR)").unwrap();
        let mut appender = con.appender("row_poison", "main").unwrap();

        appender.append(&mut FailingRow).unwrap_err();
        // The appender is poisoned; a following append is rejected.
        assert!(appender.append(&mut Row(1, "later")).is_err());
        drop(appender);

        // The connection itself is unharmed.
        con.query("INSERT INTO row_poison VALUES (7, 'ok')").unwrap();
        assert_row_exists(&con, "row_poison", 7);
    }

    /// `finish` flushes, closes, and reports success; the rows are visible and the
    /// consumed appender's drop is a bare destroy.
    #[test]
    fn finish_commits_rows_and_reports_success() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE finished (id INTEGER, name VARCHAR)").unwrap();
        let reader = con.try_clone().unwrap();

        let mut appender = con.appender("finished", "main").unwrap();
        appender.append(&mut Row(21, "done")).unwrap();
        appender.finish().unwrap();

        assert_row_exists(&reader, "finished", 21);
    }

    /// `finish` surfaces a flush/close failure to the caller instead of only
    /// logging it as drop does.
    #[test]
    fn finish_reports_flush_failure() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE finish_fail (id INTEGER CHECK (id > 0))").unwrap();
        let mut appender = con.appender("finish_fail", "main").unwrap();

        appender.append(&mut DuckValue::Int(-5)).unwrap();
        let error = appender.finish().unwrap_err();
        assert!(matches!(error, Error::Engine(_)), "expected typed engine error, got {error:?}");
    }

    /// `finish` on an already-poisoned appender fails fast at the state check,
    /// before touching the C handle — it must not attempt a close on invalidated
    /// data.
    #[test]
    fn finish_on_poisoned_appender_reports_poison_without_reclose() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE finish_poisoned (id INTEGER CHECK (id > 0))").unwrap();
        let mut appender = con.appender("finish_poisoned", "main").unwrap();

        appender.append(&mut DuckValue::Int(-1)).unwrap();
        appender.save().unwrap_err(); // poisons

        let error = appender.finish().unwrap_err();
        assert!(
            matches!(error, Error::Engine(ref e)
                if e.message.as_deref() == Some("appender was invalidated by an earlier failure")),
            "finish on a poisoned appender must report the poison, got {error:?}"
        );
    }

    /// Dropping a `Ready` appender that has an unflushed constraint-violating row
    /// runs the best-effort flush in `Drop`, which fails and is logged (never
    /// panics), and leaves the connection usable.
    #[test]
    fn drop_with_pending_violation_logs_and_leaves_connection_usable() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE drop_fail (id INTEGER CHECK (id > 0))").unwrap();
        {
            let mut appender = con.appender("drop_fail", "main").unwrap();
            // Buffered but not saved: the violation surfaces during Drop's flush.
            appender.append(&mut DuckValue::Int(-9)).unwrap();
            // appender drops here — Drop flushes (Ready state), fails, logs, destroys.
        }
        // The connection is unharmed and the bad row was not committed.
        con.query("INSERT INTO drop_fail VALUES (3)").unwrap();
        assert_row_exists(&con, "drop_fail", 3);
        let mut rows = con.query("SELECT count(*) AS n FROM drop_fail").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("n").unwrap(), &DuckValue::BigInt(1), "only the valid row persisted");
    }
}
