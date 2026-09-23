use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use crate::error::{EngineError, Error, Result};
use crate::ffi::{
    duckdb_append_data_chunk, duckdb_append_default, duckdb_append_default_to_chunk,
    duckdb_appender, duckdb_appender_add_column, duckdb_appender_begin_row,
    duckdb_appender_clear_columns, duckdb_appender_close, duckdb_appender_column_count,
    duckdb_appender_column_type, duckdb_appender_create, duckdb_appender_create_ext,
    duckdb_appender_create_query, duckdb_appender_destroy, duckdb_appender_end_row,
    duckdb_appender_error_data, duckdb_appender_flush, duckdb_data_chunk_get_column_count,
    duckdb_data_chunk_get_size, duckdb_logical_type, idx_t, DuckDBSuccess,
};
use crate::helpers::duck_result::result_from_duckdb_appender;
use crate::raw::connection::ConnectionInner;
use crate::raw::data_chunk::DataChunk;
use crate::raw::error_data::ErrorData;
use crate::types::appendable::AppendAble;
use crate::types::LogicalType;

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
    /// Whether any row has been appended yet. The active-column-list builders
    /// ([`add_column`](Appender::add_column)/[`clear_columns`](Appender::clear_columns))
    /// must run *before* the first row, so this gates them.
    rows_appended: bool,
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
            rows_appended: false,
        })
    }

    /// Creates an `Appender` for `[catalog.]schema.table`, addressing a table in a
    /// specific attached catalog. A `None` catalog uses DuckDB's default.
    ///
    /// # Errors
    ///
    /// Returns an error if a name contains an interior NUL, or if DuckDB cannot
    /// create the appender (e.g. the table or catalog does not exist).
    pub(crate) fn new_ext(
        connection: Arc<ConnectionInner>,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
    ) -> Result<Appender> {
        let c_catalog = catalog.map(CString::new).transpose()?;
        let c_schema = CString::new(schema)?;
        let c_table = CString::new(table)?;
        let catalog_ptr = c_catalog.as_ref().map_or(ptr::null(), |c| c.as_ptr());
        let mut appender: duckdb_appender = ptr::null_mut();
        // SAFETY: `connection`'s handle is valid and kept alive by the retained `Arc`.
        // The (optional) catalog/schema/table pointers are valid null-terminated
        // strings (or null for the default catalog) that outlive the call; `appender`
        // is a valid out-pointer.
        let res = unsafe {
            duckdb_appender_create_ext(
                connection.handle(),
                catalog_ptr,
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut appender,
            )
        };
        result_from_duckdb_appender(res, &mut appender).map(|_| Appender {
            _connection: connection,
            inn: appender,
            state: AppenderState::Ready,
            rows_appended: false,
        })
    }

    /// Creates a *query* appender: rows appended to it feed `query` (an `INSERT`,
    /// `UPDATE`, `DELETE`, or `MERGE INTO`), which refers to the appended data by
    /// `table_name` (default `"appended_data"`). `types` gives the appended columns'
    /// types; `column_names` optionally names them (default `col1`, `col2`, …).
    ///
    /// # Errors
    ///
    /// Returns an error on an interior NUL in any string, or if DuckDB rejects the
    /// query/columns.
    pub(crate) fn new_query(
        connection: Arc<ConnectionInner>,
        query: &str,
        types: &[LogicalType],
        table_name: Option<&str>,
        column_names: Option<&[&str]>,
    ) -> Result<Appender> {
        let c_query = CString::new(query)?;
        let c_table = table_name.map(CString::new).transpose()?;
        let table_ptr = c_table.as_ref().map_or(ptr::null(), |c| c.as_ptr());

        // DuckDB copies the type handles; collect the raw pointers into a temporary.
        let mut raw_types: Vec<duckdb_logical_type> =
            types.iter().map(LogicalType::as_raw).collect();

        // Optional column names: build owned CStrings, then a pointer array over them.
        let c_names: Option<Vec<CString>> = column_names
            .map(|names| names.iter().map(|n| CString::new(*n)).collect::<Result<_, _>>())
            .transpose()?;
        let mut name_ptrs: Option<Vec<*const c_char>> =
            c_names.as_ref().map(|cs| cs.iter().map(|c| c.as_ptr()).collect());
        let names_ptr = name_ptrs.as_mut().map_or(ptr::null_mut(), |v| v.as_mut_ptr());

        let mut appender: duckdb_appender = ptr::null_mut();
        // SAFETY: `connection`'s handle is valid (kept alive by the retained `Arc`).
        // `c_query`/`table_ptr` are valid (or null) null-terminated strings; `raw_types`
        // holds `types.len()` valid handles DuckDB copies; `names_ptr` is null or points
        // at `types.len()`-ish valid name pointers that outlive the call; `appender` is a
        // valid out-pointer.
        let res = unsafe {
            duckdb_appender_create_query(
                connection.handle(),
                c_query.as_ptr(),
                raw_types.len() as idx_t,
                raw_types.as_mut_ptr(),
                table_ptr,
                names_ptr,
                &mut appender,
            )
        };
        result_from_duckdb_appender(res, &mut appender).map(|_| Appender {
            _connection: connection,
            inn: appender,
            state: AppenderState::Ready,
            rows_appended: false,
        })
    }

    /// The number of columns in the appender's active column list (or, with no
    /// projection set, the receiving table's column count).
    #[must_use]
    pub fn column_count(&self) -> u64 {
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender.
        unsafe { duckdb_appender_column_count(self.inn) as u64 }
    }

    /// The logical type of the appender column at `col_idx`, or `None` if DuckDB
    /// returns no type (e.g. index out of range).
    #[must_use]
    pub fn column_type(
        &self,
        col_idx: u64,
    ) -> Option<LogicalType> {
        // SAFETY: `self.inn` is valid; `duckdb_appender_column_type` returns an owned
        // logical type (destroy once) that the RAII `LogicalType` wraps; null → None.
        LogicalType::from_raw(unsafe { duckdb_appender_column_type(self.inn, col_idx as idx_t) })
            .ok()
    }

    /// Adds `name` to the appender's *active column list*, so subsequent rows supply
    /// only the projected columns (the rest take their `DEFAULT`).
    ///
    /// Must be called before the first row is appended.
    ///
    /// # Errors
    ///
    /// Returns an error if a row has already been appended, on an interior NUL in
    /// `name`, or if DuckDB rejects the column.
    pub fn add_column(
        &mut self,
        name: &str,
    ) -> Result<()> {
        self.ensure_configurable()?;
        let c_name = CString::new(name)?;
        // SAFETY: `self.inn` is valid; `c_name` is a valid null-terminated string that
        // outlives the call and is not retained.
        let rc = unsafe { duckdb_appender_add_column(self.inn, c_name.as_ptr()) };
        self.check(rc)
    }

    /// Clears any active column-list projection, so subsequent rows supply every
    /// column of the receiving table again.
    ///
    /// Must be called before the first row is appended.
    ///
    /// # Errors
    ///
    /// Returns an error if a row has already been appended, or if DuckDB reports a
    /// failure.
    pub fn clear_columns(&mut self) -> Result<()> {
        self.ensure_configurable()?;
        // SAFETY: `self.inn` is a valid, non-null duckdb_appender.
        let rc = unsafe { duckdb_appender_clear_columns(self.inn) };
        self.check(rc)
    }

    /// Appends one row in which every column takes its `DEFAULT` value.
    ///
    /// Fills the active column list with `duckdb_append_default` (a column whose
    /// table has no `DEFAULT` becomes `NULL`). Opens and closes the row like
    /// [`append`](Appender::append), so a failure part-way never leaves a half-open
    /// row.
    ///
    /// # Errors
    ///
    /// Returns an error if the appender is poisoned/closed or DuckDB rejects a
    /// default.
    pub fn append_default_row(&mut self) -> Result<()> {
        self.ensure_ready()?;
        let columns = self.column_count();

        // SAFETY: `self.inn` is a valid duckdb_appender created by a constructor.
        let begin = unsafe { duckdb_appender_begin_row(self.inn) };
        self.check(begin)?;

        // The guard ends the row on every exit path (early `?`/panic).
        let guard = RowGuard { appender: self, ended: false };
        for _ in 0..columns {
            // SAFETY: `guard.appender.inn` is valid with a row open; each call appends
            // the current column's default and advances the column cursor.
            let rc = unsafe { duckdb_append_default(guard.appender.inn) };
            guard.appender.check(rc)?;
        }
        guard.end()?;
        self.rows_appended = true;
        Ok(())
    }

    /// Appends every row of `chunk` to the appender in one call
    /// (`duckdb_append_data_chunk`).
    ///
    /// `chunk` is only read — the caller keeps ownership and it is destroyed on drop
    /// as usual. The chunk's column count must match the appender's active column
    /// list; this is validated Rust-side before the FFI call.
    ///
    /// # Errors
    ///
    /// Returns an error if the appender is poisoned/closed, the chunk's column count
    /// disagrees with the appender's, or DuckDB rejects the chunk (e.g. type
    /// mismatch).
    pub fn append_chunk(
        &mut self,
        chunk: &DataChunk,
    ) -> Result<()> {
        self.ensure_ready()?;
        let appender_cols = self.column_count();
        // SAFETY: `chunk.0` is a valid duckdb_data_chunk owned by `chunk`.
        let chunk_cols = unsafe { duckdb_data_chunk_get_column_count(chunk.0) } as u64;
        if chunk_cols != appender_cols {
            return Err(Error::Engine(EngineError::unavailable(Some(format!(
                "data chunk has {chunk_cols} columns but the appender expects {appender_cols}"
            )))));
        }
        // SAFETY: `self.inn` is a valid appender; `chunk.0` is a valid data chunk whose
        // column count matches. DuckDB reads the chunk and does not take ownership.
        let rc = unsafe { duckdb_append_data_chunk(self.inn, chunk.0) };
        self.check(rc)?;
        // SAFETY: `chunk.0` is valid; a non-empty chunk means rows now exist.
        if unsafe { duckdb_data_chunk_get_size(chunk.0) } > 0 {
            self.rows_appended = true;
        }
        Ok(())
    }

    /// Writes the `DEFAULT` value of appender column `col` into `chunk` at
    /// `(col, row)` (`duckdb_append_default_to_chunk`); a column with no `DEFAULT`
    /// becomes `NULL`.
    ///
    /// `col` must be within the chunk's column count (validated Rust-side).
    ///
    /// # Errors
    ///
    /// Returns an error if the appender is poisoned/closed, `col` is out of range, or
    /// DuckDB reports a failure.
    pub fn append_default_to_chunk(
        &mut self,
        chunk: &mut DataChunk,
        col: u64,
        row: u64,
    ) -> Result<()> {
        self.ensure_ready()?;
        // SAFETY: `chunk.0` is a valid duckdb_data_chunk owned by `chunk`.
        let chunk_cols = unsafe { duckdb_data_chunk_get_column_count(chunk.0) } as u64;
        if col >= chunk_cols {
            return Err(Error::Engine(EngineError::unavailable(Some(format!(
                "column {col} out of range for a {chunk_cols}-column chunk"
            )))));
        }
        // SAFETY: `self.inn` is a valid appender; `chunk.0` is a valid chunk; `col` is
        // in range. DuckDB writes the default into the chunk cell.
        let rc = unsafe {
            duckdb_append_default_to_chunk(self.inn, chunk.0, col as idx_t, row as idx_t)
        };
        self.check(rc)
    }

    /// Returns `Ok(())` only if the active column list may still be reconfigured —
    /// i.e. the appender is Ready and no row has been appended yet.
    fn ensure_configurable(&self) -> Result<()> {
        self.ensure_ready()?;
        if self.rows_appended {
            return Err(Error::Engine(EngineError::unavailable(Some(
                "appender columns must be configured before the first row is appended".to_owned(),
            ))));
        }
        Ok(())
    }

    /// Appends a row to the table.
    ///
    /// Opens a row (`duckdb_appender_begin_row`), appends the value, then closes it
    /// (`duckdb_appender_end_row`). A `RowGuard` closes the row even if appending
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
        guard.end()?;
        // A row now exists, so the active column list may no longer be reconfigured.
        self.rows_appended = true;
        Ok(())
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

    // Catalog-aware / query appenders, schema introspection, projected columns.
    mod builders {
        use crate::connection::Connection;
        use crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER;
        use crate::types::value::DuckValue;
        use crate::types::LogicalType;

        #[test]
        fn appender_ext_default_and_named_catalog() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();

            // Default catalog (None).
            {
                let mut app = conn.appender_ext(None, "main", "t").unwrap();
                app.append(&mut DuckValue::Int(1)).unwrap();
                app.save().unwrap();
            }
            // Explicit in-memory catalog name.
            {
                let mut app = conn.appender_ext(Some("memory"), "main", "t").unwrap();
                app.append(&mut DuckValue::Int(2)).unwrap();
                app.save().unwrap();
            }
            let rows: Vec<_> = conn
                .execute("SELECT id FROM t ORDER BY id")
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
            assert_eq!(rows[1].get("id"), Some(&DuckValue::Int(2)));
        }

        #[test]
        fn column_count_and_type_reflect_the_table() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
            let app = conn.appender("t", "main").unwrap();
            assert_eq!(app.column_count(), 2);
            let ty = app.column_type(0).expect("column 0 type");
            assert_eq!(ty.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        }

        #[test]
        fn add_column_projects_and_fills_defaults() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER DEFAULT 99)").unwrap();
            {
                let mut app = conn.appender("t", "main").unwrap();
                // Project only `a`; `b` should take its DEFAULT.
                app.add_column("a").unwrap();
                assert_eq!(app.column_count(), 1, "active column list is just `a`");
                app.append(&mut DuckValue::Int(7)).unwrap();
                app.save().unwrap();
            }
            let mut rows = conn.execute("SELECT a, b FROM t").unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get("a"), Some(&DuckValue::Int(7)));
            assert_eq!(
                row.get("b"),
                Some(&DuckValue::Int(99)),
                "DEFAULT filled the unprojected column"
            );
        }

        #[test]
        fn clear_columns_restores_full_projection() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
            let mut app = conn.appender("t", "main").unwrap();
            app.add_column("a").unwrap();
            assert_eq!(app.column_count(), 1);
            app.clear_columns().unwrap();
            assert_eq!(app.column_count(), 2, "clear_columns restores every column");
        }

        #[test]
        fn configuration_after_first_row_is_rejected() {
            let mut conn = Connection::open_in_memory().unwrap();
            // Single column so a bare-value row supplies every column and succeeds.
            conn.execute_batch("CREATE TABLE t (a INTEGER)").unwrap();
            let mut app = conn.appender("t", "main").unwrap();
            app.append(&mut DuckValue::Int(1)).unwrap();
            // A row exists, so the active column list can no longer be reconfigured.
            assert!(app.add_column("a").is_err(), "add_column after a row must be rejected");
            assert!(app.clear_columns().is_err(), "clear_columns after a row must be rejected");
        }

        #[test]
        fn appender_query_feeds_an_insert() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE dest (v INTEGER)").unwrap();
            let types = [LogicalType::of::<i32>().unwrap()];
            {
                let mut app = conn
                    .appender_query(
                        "INSERT INTO dest SELECT * FROM appended_data",
                        &types,
                        None,
                        None,
                    )
                    .unwrap();
                app.append(&mut DuckValue::Int(42)).unwrap();
                app.append(&mut DuckValue::Int(43)).unwrap();
                app.save().unwrap();
            }
            let rows: Vec<_> = conn
                .execute("SELECT v FROM dest ORDER BY v")
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("v"), Some(&DuckValue::Int(42)));
            assert_eq!(rows[1].get("v"), Some(&DuckValue::Int(43)));
        }
    }

    // DEFAULT rows/cells and whole-chunk ingestion.
    mod chunk_ingestion {
        use crate::connection::Connection;
        use crate::raw::data_chunk::DataChunk;
        use crate::types::value::DuckValue;
        use crate::types::LogicalType;

        #[test]
        fn append_default_row_fills_table_defaults_and_nulls() {
            let mut conn = Connection::open_in_memory().unwrap();
            // `a` has a DEFAULT, `b` does not (so it becomes NULL).
            conn.execute_batch("CREATE TABLE t (a INTEGER DEFAULT 5, b INTEGER)").unwrap();
            {
                let mut app = conn.appender("t", "main").unwrap();
                app.append_default_row().unwrap();
                app.save().unwrap();
            }
            let mut rows = conn.execute("SELECT a, b FROM t").unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get("a"), Some(&DuckValue::Int(5)), "DEFAULT applied");
            assert_eq!(row.get("b"), Some(&DuckValue::Null), "no DEFAULT -> NULL");
        }

        #[test]
        fn append_chunk_copies_rows_and_validates_column_count() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE src (v INTEGER)").unwrap();
            conn.execute_batch("INSERT INTO src VALUES (1), (2), (3)").unwrap();
            conn.execute_batch("CREATE TABLE dst (v INTEGER)").unwrap();

            // Fetch a chunk of the source rows (owned; independent of the result).
            let chunk = {
                let result = conn.execute("SELECT v FROM src ORDER BY v").unwrap();
                DataChunk::from_result(&result).expect("a chunk").unwrap()
            };

            {
                let mut app = conn.appender("dst", "main").unwrap();
                app.append_chunk(&chunk).unwrap();
                app.save().unwrap();
            }
            let rows: Vec<_> = conn
                .execute("SELECT v FROM dst ORDER BY v")
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].get("v"), Some(&DuckValue::Int(1)));
            assert_eq!(rows[2].get("v"), Some(&DuckValue::Int(3)));

            // A chunk whose column count disagrees with the appender is rejected
            // Rust-side (dst2 has two columns, the chunk has one).
            conn.execute_batch("CREATE TABLE dst2 (v INTEGER, w INTEGER)").unwrap();
            let mut app2 = conn.appender("dst2", "main").unwrap();
            assert!(app2.append_chunk(&chunk).is_err(), "column-count mismatch must error");
        }

        #[test]
        fn append_default_to_chunk_fills_a_cell() {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t (a INTEGER DEFAULT 42)").unwrap();

            // Build a one-column INTEGER chunk to receive the default.
            let int_ty = LogicalType::of::<i32>().unwrap();
            let mut raw_types = [int_ty.as_raw()];
            // SAFETY: `raw_types` holds one valid logical type handle that outlives the
            // call; DuckDB copies it. The returned chunk is wrapped in RAII (`DataChunk`)
            // so it is destroyed exactly once.
            let raw = unsafe { crate::ffi::duckdb_create_data_chunk(raw_types.as_mut_ptr(), 1) };
            let mut chunk = DataChunk::new(raw).unwrap();
            drop(int_ty);

            {
                let mut app = conn.appender("t", "main").unwrap();
                // Write column 0's DEFAULT (42) into chunk cell (0, 0), then size it.
                app.append_default_to_chunk(&mut chunk, 0, 0).unwrap();
                // SAFETY: `chunk` is valid; one row is now populated.
                unsafe { crate::ffi::duckdb_data_chunk_set_size(*chunk, 1) };
                app.append_chunk(&chunk).unwrap();
                app.save().unwrap();

                // Out-of-range column is rejected Rust-side.
                assert!(app.append_default_to_chunk(&mut chunk, 5, 0).is_err());
            }
            let mut rows = conn.execute("SELECT a FROM t").unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get("a"), Some(&DuckValue::Int(42)));
        }
    }
}
