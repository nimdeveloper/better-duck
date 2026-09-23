use std::{
    ffi::{CStr, CString},
    mem, ptr,
    sync::Arc,
};

use crate::ffi::{
    duckdb_bind_parameter_index, duckdb_clear_bindings, duckdb_destroy_prepare,
    duckdb_execute_prepared, duckdb_free, duckdb_nparams, duckdb_param_logical_type,
    duckdb_param_type, duckdb_parameter_name, duckdb_prepare,
    duckdb_prepared_statement_column_count, duckdb_prepared_statement_column_logical_type,
    duckdb_prepared_statement_column_name, duckdb_prepared_statement_column_type,
    duckdb_prepared_statement_type, duckdb_result, duckdb_statement_type, duckdb_type,
    DuckDBSuccess,
};

use crate::{
    error::{Error, Result},
    ffi,
    ffi::duckdb_prepared_statement,
    helpers::duck_result::{result_from_duckdb_prepare, result_from_duckdb_result},
    raw::{
        connection::{ConnectionInner, RawConnection},
        result::DuckResult,
    },
    types::{appendable::AppendAble, LogicalType, TypeInfo},
};

/// The kind of SQL statement a prepared statement holds.
///
/// Mirrors DuckDB's `duckdb_statement_type`. `#[non_exhaustive]` because DuckDB
/// adds statement kinds across releases; an id this build does not recognise is
/// preserved verbatim in [`StatementType::Unknown`] rather than lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StatementType {
    /// `DUCKDB_STATEMENT_TYPE_INVALID`
    Invalid,
    /// `SELECT`
    Select,
    /// `INSERT`
    Insert,
    /// `UPDATE`
    Update,
    /// `EXPLAIN`
    Explain,
    /// `DELETE`
    Delete,
    /// `PREPARE`
    Prepare,
    /// `CREATE`
    Create,
    /// `EXECUTE`
    Execute,
    /// `ALTER`
    Alter,
    /// `TRANSACTION`
    Transaction,
    /// `COPY`
    Copy,
    /// `ANALYZE`
    Analyze,
    /// `SET VARIABLE`
    VariableSet,
    /// `CREATE FUNCTION`
    CreateFunc,
    /// `DROP`
    Drop,
    /// `EXPORT`
    Export,
    /// `PRAGMA`
    Pragma,
    /// `VACUUM`
    Vacuum,
    /// `CALL`
    Call,
    /// `SET`
    Set,
    /// `LOAD`
    Load,
    /// `RELATION`
    Relation,
    /// `EXTENSION`
    Extension,
    /// `LOGICAL_PLAN`
    LogicalPlan,
    /// `ATTACH`
    Attach,
    /// `DETACH`
    Detach,
    /// `MULTI`
    Multi,
    /// A statement kind this build does not recognise; the raw id is preserved.
    Unknown(duckdb_statement_type),
}

impl StatementType {
    /// Classifies a raw `duckdb_statement_type`, preserving unrecognised values.
    #[must_use]
    pub fn from_raw(raw: duckdb_statement_type) -> StatementType {
        use crate::ffi as f;
        match raw {
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INVALID => StatementType::Invalid,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SELECT => StatementType::Select,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INSERT => StatementType::Insert,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE => StatementType::Update,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXPLAIN => StatementType::Explain,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DELETE => StatementType::Delete,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_PREPARE => StatementType::Prepare,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE => StatementType::Create,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXECUTE => StatementType::Execute,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ALTER => StatementType::Alter,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_TRANSACTION => {
                StatementType::Transaction
            },
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_COPY => StatementType::Copy,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ANALYZE => StatementType::Analyze,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_VARIABLE_SET => {
                StatementType::VariableSet
            },
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE_FUNC => StatementType::CreateFunc,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DROP => StatementType::Drop,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXPORT => StatementType::Export,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_PRAGMA => StatementType::Pragma,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_VACUUM => StatementType::Vacuum,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CALL => StatementType::Call,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SET => StatementType::Set,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_LOAD => StatementType::Load,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_RELATION => StatementType::Relation,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXTENSION => StatementType::Extension,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN => {
                StatementType::LogicalPlan
            },
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ATTACH => StatementType::Attach,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DETACH => StatementType::Detach,
            f::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_MULTI => StatementType::Multi,
            other => StatementType::Unknown(other),
        }
    }
}

/// Prepared-statement metadata shared by [`Statement`] and [`CachedStatement`].
///
/// These operate on a raw `duckdb_prepared_statement`; both wrappers delegate to
/// them so parameter/column introspection lives in one place.
///
/// # Safety
///
/// Every function requires `stmt` to be a valid, live `duckdb_prepared_statement`.
mod meta {
    use super::*;

    /// The statement kind (`duckdb_prepared_statement_type`).
    pub(super) fn statement_type(stmt: duckdb_prepared_statement) -> StatementType {
        // SAFETY: `stmt` is a valid prepared statement.
        StatementType::from_raw(unsafe { duckdb_prepared_statement_type(stmt) })
    }

    /// Number of parameters (`duckdb_nparams`).
    pub(super) fn param_count(stmt: duckdb_prepared_statement) -> u64 {
        // SAFETY: `stmt` is valid.
        unsafe { duckdb_nparams(stmt) }
    }

    /// The name of the parameter at the given 1-based index, if any.
    ///
    /// Returns `None` for an out-of-range index (DuckDB returns null) or a
    /// positional (anonymous) parameter.
    pub(super) fn parameter_name(
        stmt: duckdb_prepared_statement,
        idx: u64,
    ) -> Option<String> {
        // SAFETY: `stmt` is valid; `duckdb_parameter_name` returns an owned `char*`
        // (free with `duckdb_free`) or null, which `owned_c_string` copies + frees.
        unsafe { owned_c_string(duckdb_parameter_name(stmt, idx)) }
    }

    /// The coarse `duckdb_type` of the parameter at the 1-based index.
    pub(super) fn param_type(
        stmt: duckdb_prepared_statement,
        idx: u64,
    ) -> duckdb_type {
        // SAFETY: `stmt` is valid.
        unsafe { duckdb_param_type(stmt, idx) }
    }

    /// The lossless [`TypeInfo`] of the parameter at the 1-based index.
    pub(super) fn param_type_info(
        stmt: duckdb_prepared_statement,
        idx: u64,
    ) -> Option<TypeInfo> {
        // SAFETY: `stmt` is valid; `duckdb_param_logical_type` returns an owned handle
        // (or null) wrapped into RAII, described, then destroyed on drop.
        LogicalType::from_raw(unsafe { duckdb_param_logical_type(stmt, idx) })
            .ok()
            .map(|lt| lt.describe())
    }

    /// Resolves a named parameter to its 1-based index
    /// (`duckdb_bind_parameter_index`).
    ///
    /// # Errors
    ///
    /// [`Error::NulError`] if `name` has an interior nul; [`Error::InvalidParameterName`]
    /// if DuckDB does not know the name.
    pub(super) fn parameter_index(
        stmt: duckdb_prepared_statement,
        name: &str,
    ) -> Result<u64> {
        let c_name = CString::new(name)?;
        let mut idx: crate::ffi::idx_t = 0;
        // SAFETY: `stmt` is valid; `c_name` is a valid null-terminated string that
        // outlives the call; `&mut idx` is a valid output pointer.
        let rc = unsafe { duckdb_bind_parameter_index(stmt, &mut idx, c_name.as_ptr()) };
        if rc == DuckDBSuccess {
            Ok(idx)
        } else {
            Err(Error::InvalidParameterName(name.to_owned()))
        }
    }

    /// Number of result columns (`duckdb_prepared_statement_column_count`).
    pub(super) fn column_count(stmt: duckdb_prepared_statement) -> u64 {
        // SAFETY: `stmt` is valid.
        unsafe { duckdb_prepared_statement_column_count(stmt) }
    }

    /// The name of the result column at `idx`, if in range.
    pub(super) fn column_name(
        stmt: duckdb_prepared_statement,
        idx: u64,
    ) -> Option<String> {
        // SAFETY: `stmt` is valid; owned `char*` (or null) copied + freed.
        unsafe { owned_c_string(duckdb_prepared_statement_column_name(stmt, idx)) }
    }

    /// The coarse `duckdb_type` of the result column at `idx`.
    pub(super) fn column_type(
        stmt: duckdb_prepared_statement,
        idx: u64,
    ) -> duckdb_type {
        // SAFETY: `stmt` is valid.
        unsafe { duckdb_prepared_statement_column_type(stmt, idx) }
    }

    /// The lossless [`TypeInfo`] of the result column at `idx`.
    pub(super) fn column_type_info(
        stmt: duckdb_prepared_statement,
        idx: u64,
    ) -> Option<TypeInfo> {
        // SAFETY: `stmt` is valid; owned handle (or null) wrapped, described, dropped.
        LogicalType::from_raw(unsafe { duckdb_prepared_statement_column_logical_type(stmt, idx) })
            .ok()
            .map(|lt| lt.describe())
    }

    /// Copies a DuckDB-owned `char*` into an owned `String`, freeing it with
    /// `duckdb_free`. Returns `None` for null.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or a `char*` DuckDB allocated for the caller to free.
    unsafe fn owned_c_string(ptr: *const std::os::raw::c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        // SAFETY: non-null, valid null-terminated C string per the contract.
        let owned = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
        // SAFETY: DuckDB allocated `ptr`; ownership transferred to us to free.
        unsafe { duckdb_free(ptr as *mut std::os::raw::c_void) };
        Some(owned)
    }
}

/// A prepared DuckDB statement that can be executed one or more times.
///
/// After calling [`execute`](Statement::execute), the statement can be reused
/// by calling [`clear_bindings`](Statement::clear_bindings) and re-binding parameters.
pub struct Statement<'a> {
    /// Reference to the underlying DuckDB connection.
    con: &'a RawConnection,
    /// Pointer to the prepared DuckDB statement (FFI resource).
    stmt: duckdb_prepared_statement,
    /// 1-based index of the next parameter to bind (incremented by each `bind` call).
    bind_idx: u64,
}

impl Statement<'_> {
    /// Prepares a new `Statement` from an SQL string.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be compiled into a prepared statement.
    pub(super) fn new<'a, 'b: 'a>(
        con: &'b RawConnection,
        sql: &str,
    ) -> Result<Statement<'a>> {
        let mut stmt: duckdb_prepared_statement = ptr::null_mut();
        let c_str = std::ffi::CString::new(sql)?;
        // SAFETY: `con` is a live `RawConnection`, so its handle is a valid open
        // `duckdb_connection`; `c_str` is a valid null-terminated C string. `stmt` is a
        // valid output pointer. The `'a` borrow keeps the connection alive for the
        // statement's whole lifetime.
        let resp = unsafe { duckdb_prepare(con.handle(), c_str.as_ptr(), &mut stmt) };
        result_from_duckdb_prepare(resp, stmt)?;
        Ok(Statement { con, stmt, bind_idx: 0 })
    }

    /// Returns a reference to the raw prepared-statement pointer.
    #[allow(unused)]
    #[inline]
    fn raw(&self) -> &duckdb_prepared_statement {
        &self.stmt
    }

    /// Returns a reference to the underlying raw connection.
    #[allow(unused)]
    #[inline]
    fn connection(&self) -> &RawConnection {
        self.con
    }
}

// Exposed API
impl Statement<'_> {
    /// Binds a value to the next positional parameter (1-based).
    ///
    /// The first call binds parameter 1, the second call parameter 2, and so on.
    /// Call [`clear_bindings`](Statement::clear_bindings) to reset the counter.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails.
    #[must_use = "bind result should be checked"]
    #[allow(unused)]
    #[inline]
    pub fn bind<T: AppendAble>(
        &mut self,
        binder: &mut T,
    ) -> Result<()> {
        self.bind_idx += 1;
        // Pass the 1-based index directly to stmt_append.
        self.bind_at(binder, self.bind_idx)
    }

    /// Binds a value to the parameter at the given 1-based index.
    ///
    /// # Arguments
    ///
    /// * `binder` - The value to bind, must implement [`AppendAble`].
    /// * `idx` - The 1-based parameter index.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails.
    #[allow(unused)]
    #[inline]
    pub fn bind_at<T: AppendAble>(
        &self,
        binder: &mut T,
        idx: u64,
    ) -> Result<()> {
        binder.stmt_append(idx, self.stmt)
    }

    /// Executes the prepared statement and returns the result.
    ///
    /// The statement can be re-executed after calling [`clear_bindings`](Statement::clear_bindings)
    /// and re-binding parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if execution fails.
    #[must_use = "execute returns the query result; dropping it without reading discards rows"]
    #[allow(unused)]
    pub fn execute(&mut self) -> Result<DuckResult> {
        // SAFETY: `mem::zeroed::<duckdb_result>()` produces an all-zeros value, which is
        // the correct initial state for a `duckdb_result` output parameter. `duckdb_result`
        // is a small `Copy` struct with no self-referential fields, so it needs no stable
        // heap address — DuckResult::new takes it by value.
        let mut out = unsafe { mem::zeroed::<duckdb_result>() };
        // SAFETY: `self.stmt` is a valid prepared statement. `&mut out` provides a pointer
        // to the stack-local zeroed `duckdb_result`. Ownership transfers to `DuckResult::new`,
        // whose `Drop` calls `duckdb_destroy_result` once.
        let resp = unsafe { duckdb_execute_prepared(self.stmt, &mut out as *mut duckdb_result) };
        // The query is finished: advance the generation so a `QueryControl` minted
        // for it can no longer interrupt a later query.
        self.con.inner().advance_query();
        result_from_duckdb_result(resp, &mut out as *mut duckdb_result)?;
        Ok(DuckResult::new(out))
    }

    /// Returns the number of parameters in the prepared statement.
    #[allow(unused)]
    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        // SAFETY: `self.stmt` is a valid prepared statement.
        unsafe { duckdb_nparams(self.stmt) as usize }
    }

    /// Clears all parameter bindings and resets the bind index to zero.
    ///
    /// After calling this method, subsequent [`bind`](Statement::bind) calls start
    /// from parameter 1 again.
    ///
    /// # Errors
    ///
    /// Returns an error if the DuckDB clear-bindings call fails.
    #[must_use = "clear_bindings result should be checked"]
    #[allow(unused)]
    #[inline]
    pub fn clear_bindings(&mut self) -> Result<()> {
        // SAFETY: `self.stmt` is a valid prepared statement.
        let res = unsafe { duckdb_clear_bindings(self.stmt) };
        if res != DuckDBSuccess {
            Err(Error::DuckDBFailure(
                crate::ffi::Error::new(crate::ffi::DuckDBError),
                Some("Failed to clear bindings".to_owned()),
            ))
        } else {
            self.bind_idx = 0;
            Ok(())
        }
    }

    /// Returns `true` if the prepared statement pointer is null (not initialized).
    #[allow(unused)]
    #[inline]
    pub fn is_null(&self) -> bool {
        self.stmt.is_null()
    }
}

/// Destroys the prepared statement when the `Statement` is dropped.
impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.stmt` is a valid duckdb_prepared_statement (or null).
        // `duckdb_destroy_prepare` is idempotent and handles the non-null check itself,
        // but we guard here as a belt-and-suspenders measure.
        unsafe {
            if !self.stmt.is_null() {
                duckdb_destroy_prepare(&mut self.stmt);
            }
        }
    }
}

/// A prepared statement that can be reset and re-executed with different bindings.
///
/// The statement is prepared on — and therefore belongs to — the connection
/// passed to [`prepare`](CachedStatement::prepare). DuckDB scopes transactions
/// to a connection, so the statement must never be prepared on a different one:
/// doing so would execute outside any `BEGIN`/`ROLLBACK` the caller has open.
///
/// The statement retains that connection, so it cannot outlive it. Unlike a
/// `Statement`, it carries no Rust lifetime, which lets it live in a
/// `StatementCache` alongside the connection it was prepared on.
///
/// This type is used by Diesel statement cache
/// (`StatementCache<DuckDb, CachedStatement>`).
pub struct CachedStatement {
    /// Keeps the *connection* this statement was prepared on open for at least as
    /// long as the statement.
    ///
    /// This is deliberately the connection rather than the database. Retaining only
    /// `Arc<RawDatabase>` would keep the database open while allowing the connection
    /// to be disconnected first, leaving `duckdb_destroy_prepare` to run against a
    /// dead connection. Retaining a *cloned* connection would be equally wrong in the
    /// other direction: that opens a separate DuckDB connection, which would silently
    /// run cached statements outside the caller's transaction.
    _connection: Arc<ConnectionInner>,
    /// The SQL text this statement was prepared from, retained alongside the handle.
    ///
    /// This is `pub(crate)`, so it is not visible outside `better-duck-core`. The
    /// Diesel `StatementCache` keys its entries by its own query fragment and passes
    /// the SQL *into* [`prepare`](CachedStatement::prepare) rather than reading it
    /// back from here. Within the crate it is currently only inspected by tests,
    /// hence `#[allow(dead_code)]` for non-test builds.
    #[allow(dead_code)]
    pub(crate) sql: Box<str>,
    /// Raw prepared-statement handle.
    stmt: ffi::duckdb_prepared_statement,
}

impl CachedStatement {
    /// Prepares `sql` against the given connection.
    ///
    /// The statement is bound to `conn` and shares its transaction state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if DuckDB cannot parse or plan the query,
    /// or [`Error::NulError`] if `sql` contains an interior nul byte.
    pub fn prepare(
        conn: &RawConnection,
        sql: impl AsRef<str>,
    ) -> Result<Self> {
        let sql_str = sql.as_ref();
        let mut stmt: ffi::duckdb_prepared_statement = ptr::null_mut();
        let c_str = CString::new(sql_str)?;
        // SAFETY: `conn`'s handle is a valid open duckdb_connection owned by the
        // caller. `c_str` is a valid null-terminated CString that outlives this
        // call, and `&mut stmt` is a valid output pointer. Preparing on the
        // caller's own connection keeps the statement inside that connection's
        // transaction scope.
        let r = unsafe { ffi::duckdb_prepare(conn.handle(), c_str.as_ptr(), &mut stmt) };
        result_from_duckdb_prepare(r, stmt)?;
        Ok(CachedStatement::from_prepared(Arc::clone(conn.inner()), stmt, sql_str.into()))
    }

    /// Wraps an already-prepared handle into a `CachedStatement`.
    ///
    /// Shared by [`prepare`](CachedStatement::prepare) and the extracted-statement
    /// path ([`ExtractedStatements::prepare`](crate::raw::extracted::ExtractedStatements::prepare)),
    /// so both produce the identical wrapper — same connection-retention and
    /// destruction invariants.
    pub(crate) fn from_prepared(
        connection: Arc<ConnectionInner>,
        stmt: duckdb_prepared_statement,
        sql: Box<str>,
    ) -> CachedStatement {
        CachedStatement { _connection: connection, sql, stmt }
    }

    /// Returns the raw prepared-statement handle, for crate-internal FFI (e.g. the
    /// pending-execution wrapper). The handle stays owned by this `CachedStatement`.
    #[inline]
    pub(crate) fn handle(&self) -> duckdb_prepared_statement {
        self.stmt
    }

    /// Begins incremental ("pending") execution of this statement.
    ///
    /// Bind parameters first. The returned
    /// [`PendingResult`](crate::raw::pending::PendingResult) borrows `self`, runs
    /// the query one task at a time, and materialises the final result on
    /// `execute()`.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB cannot create the pending result.
    pub fn pending(&self) -> Result<crate::raw::pending::PendingResult<'_>> {
        crate::raw::pending::PendingResult::new(self)
    }

    /// Consumes this statement into an owned, `'static` pending execution.
    ///
    /// Unlike [`pending`](CachedStatement::pending), the returned
    /// [`OwnedPending`](crate::raw::pending::OwnedPending) *owns* the statement, so
    /// it can be stepped across `spawn_blocking` dispatches by the async adapter.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB cannot create the pending result.
    #[allow(dead_code)]
    pub fn into_pending(self) -> Result<crate::raw::pending::OwnedPending> {
        crate::raw::pending::OwnedPending::new(self)
    }

    /// Resets all parameter bindings so the statement can be re-executed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if the DuckDB clear-bindings call fails.
    pub fn reset_bindings(&mut self) -> Result<()> {
        // SAFETY: `self.stmt` is a valid prepared statement — the Drop impl enforces this.
        let r = unsafe { ffi::duckdb_clear_bindings(self.stmt) };
        if r == ffi::DuckDBSuccess {
            Ok(())
        } else {
            Err(Error::DuckDBFailure(ffi::Error::new(r), None))
        }
    }

    /// Binds `value` at the given **1-based** parameter index.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails or `idx` is out of range.
    pub fn bind<T: AppendAble + ?Sized>(
        &mut self,
        idx: u64,
        value: &mut T,
    ) -> Result<()> {
        value.stmt_append(idx, self.stmt)
    }

    /// Binds `value` to the parameter identified by `name` (e.g. `$id` → `"id"`).
    ///
    /// Resolves the name to its 1-based index via `duckdb_bind_parameter_index`,
    /// then binds there.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParameterName`] if the statement has no such parameter,
    /// [`Error::NulError`] if `name` contains an interior nul, or a bind failure.
    pub fn bind_named<T: AppendAble + ?Sized>(
        &mut self,
        name: &str,
        value: &mut T,
    ) -> Result<()> {
        let idx = meta::parameter_index(self.stmt, name)?;
        value.stmt_append(idx, self.stmt)
    }

    /// Returns the kind of SQL statement this prepared statement holds.
    #[must_use]
    pub fn statement_type(&self) -> StatementType {
        meta::statement_type(self.stmt)
    }

    /// Number of parameters in the statement.
    #[must_use]
    pub fn parameter_count(&self) -> u64 {
        meta::param_count(self.stmt)
    }

    /// The name of the parameter at the 1-based `index`, or `None` for a
    /// positional parameter or an out-of-range index.
    #[must_use]
    pub fn parameter_name(
        &self,
        index: u64,
    ) -> Option<String> {
        meta::parameter_name(self.stmt, index)
    }

    /// The coarse `duckdb_type` of the parameter at the 1-based `index`.
    #[must_use]
    pub fn parameter_type(
        &self,
        index: u64,
    ) -> duckdb_type {
        meta::param_type(self.stmt, index)
    }

    /// The lossless [`TypeInfo`] of the parameter at the 1-based `index`.
    #[must_use]
    pub fn parameter_logical_type(
        &self,
        index: u64,
    ) -> Option<TypeInfo> {
        meta::param_type_info(self.stmt, index)
    }

    /// Resolves a named parameter to its 1-based index.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParameterName`] if unknown, [`Error::NulError`] on interior nul.
    pub fn parameter_index(
        &self,
        name: &str,
    ) -> Result<u64> {
        meta::parameter_index(self.stmt, name)
    }

    /// Number of result columns the statement will produce.
    #[must_use]
    pub fn column_count(&self) -> u64 {
        meta::column_count(self.stmt)
    }

    /// The name of the result column at `index`, if in range.
    #[must_use]
    pub fn column_name(
        &self,
        index: u64,
    ) -> Option<String> {
        meta::column_name(self.stmt, index)
    }

    /// The coarse `duckdb_type` of the result column at `index`.
    #[must_use]
    pub fn column_type(
        &self,
        index: u64,
    ) -> duckdb_type {
        meta::column_type(self.stmt, index)
    }

    /// The lossless [`TypeInfo`] of the result column at `index`.
    #[must_use]
    pub fn column_logical_type(
        &self,
        index: u64,
    ) -> Option<TypeInfo> {
        meta::column_type_info(self.stmt, index)
    }

    /// Executes the prepared statement and returns the result.
    ///
    /// Works for all statement types:
    /// - **SELECT** — iterate rows via the [`Iterator`] impl on [`DuckResult`].
    /// - **INSERT / UPDATE / DELETE** — check [`DuckResult::changes()`] for affected rows.
    /// - **DDL** (`CREATE TABLE` etc.) — `.changes()` returns `0`, no rows to iterate.
    /// - **INSERT … RETURNING** — iterate rows and/or call `.changes()`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if execution fails.
    #[must_use = "the DuckResult carries both affected-row count (.changes()) and row iterator — consume it"]
    pub fn execute(&mut self) -> Result<DuckResult> {
        // SAFETY: `mem::zeroed::<ffi::duckdb_result>()` is the correct initialization for a
        // DuckDB result output parameter. `duckdb_result` is a small `Copy` struct with no
        // self-referential fields, so it needs no stable heap address — DuckResult::new
        // takes it by value.
        let mut out = unsafe { mem::zeroed::<ffi::duckdb_result>() };
        // SAFETY: `self.stmt` is a valid prepared statement. `&mut out` provides a raw
        // pointer to the stack-local zeroed duckdb_result output buffer. Ownership
        // transfers to DuckResult::new; its Drop calls duckdb_destroy_result exactly once.
        let r =
            unsafe { ffi::duckdb_execute_prepared(self.stmt, &mut out as *mut ffi::duckdb_result) };
        // The query is finished: advance the generation so a `QueryControl` minted
        // for it can no longer interrupt a later query.
        self._connection.advance_query();
        result_from_duckdb_result(r, &mut out as *mut ffi::duckdb_result)?;
        Ok(DuckResult::new(out))
    }
}

impl Drop for CachedStatement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            // SAFETY: `self.stmt` is a valid prepared statement not yet destroyed.
            // The null guard ensures this path runs at most once.
            unsafe { ffi::duckdb_destroy_prepare(&mut self.stmt) };
        }
    }
}

// SAFETY: the only non-`Send` field is the raw `stmt` pointer; `Arc<ConnectionInner>`
// is already `Send` because `ConnectionInner` is `Send + Sync`. DuckDB permits a
// prepared statement to be used from a different thread than the one that prepared it,
// provided it is not used concurrently — `CachedStatement` takes `&mut self` for every
// operation that touches the handle and is deliberately not `Sync`, so no two threads
// can drive it at once. Retaining the connection means moving the statement to another
// thread also keeps its connection alive, so the handle cannot dangle.
unsafe impl Send for CachedStatement {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER;
    use crate::helpers::path::path_to_cstring;
    use crate::raw::connection::RawConnection;
    use crate::types::{appendable::AppendAble, value::DuckValue};

    struct CheckedI32(i32);
    struct DummyAppendAble;

    impl AppendAble for DummyAppendAble {
        fn stmt_append(
            &mut self,
            _idx: u64,
            _stmt: duckdb_prepared_statement,
        ) -> Result<()> {
            Ok(())
        }

        fn appender_append(
            &mut self,
            _appender: ffi::duckdb_appender,
        ) -> Result<()> {
            unreachable!("DummyAppendAble is only used for statement binding")
        }
    }

    impl AppendAble for CheckedI32 {
        fn stmt_append(
            &mut self,
            idx: u64,
            stmt: duckdb_prepared_statement,
        ) -> Result<()> {
            // SAFETY: `stmt` comes from a live Statement and the scalar value is copied.
            let state = unsafe { ffi::duckdb_bind_int32(stmt, idx, self.0) };
            if state == DuckDBSuccess {
                Ok(())
            } else {
                Err(Error::DuckDBFailure(ffi::Error::new(state), None))
            }
        }

        fn appender_append(
            &mut self,
            _appender: crate::ffi::duckdb_appender,
        ) -> Result<()> {
            unreachable!("CheckedI32 is only used for statement binding")
        }
    }

    fn get_test_connection() -> RawConnection {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    fn assert_single_value(
        mut result: DuckResult,
        column: &str,
        expected: DuckValue,
    ) {
        let row = result.next().expect("expected one row").unwrap();
        assert_eq!(row.get(column), Some(&expected));
        assert!(result.next().is_none());
    }

    #[test]
    fn test_new() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let stmt = Statement::new(&con, sql);
        assert!(stmt.is_ok());
    }

    #[test]
    fn test_prepare_rejects_invalid_sql_and_interior_nul() {
        let con = get_test_connection();

        assert!(matches!(Statement::new(&con, "SELEC 1"), Err(Error::DuckDBFailure(..))));
        assert!(matches!(Statement::new(&con, "SELECT \0 1"), Err(Error::NulError(_))));
        assert!(matches!(CachedStatement::prepare(&con, "SELEC 1"), Err(Error::DuckDBFailure(..))));
        assert!(matches!(CachedStatement::prepare(&con, "SELECT \0 1"), Err(Error::NulError(_))));
    }

    #[test]
    fn test_statement_binds_real_values_and_reports_parameter_errors() {
        let con = get_test_connection();
        let mut stmt = Statement::new(&con, "SELECT $1::INTEGER + $2::INTEGER AS total").unwrap();
        assert_eq!(stmt.bind_parameter_count(), 2);

        let mut first = CheckedI32(19);
        let mut second = CheckedI32(23);
        stmt.bind(&mut first).unwrap();
        stmt.bind(&mut second).unwrap();
        assert_single_value(stmt.execute().unwrap(), "total", DuckValue::Int(42));

        let mut out_of_range = CheckedI32(99);
        assert!(matches!(stmt.bind_at(&mut out_of_range, 3), Err(Error::DuckDBFailure(..))));
        assert_single_value(stmt.execute().unwrap(), "total", DuckValue::Int(42));
    }

    #[test]
    fn test_statement_clear_bindings_resets_and_reuses() {
        let con = get_test_connection();
        let mut stmt = Statement::new(&con, "SELECT $1::INTEGER AS value").unwrap();
        let mut first = CheckedI32(7);
        stmt.bind(&mut first).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(7));

        stmt.clear_bindings().unwrap();
        assert_eq!(stmt.bind_idx, 0);
        let mut second = CheckedI32(11);
        stmt.bind(&mut second).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(11));
    }

    #[test]
    fn test_cached_statement_retains_sql_and_resets_for_reuse() {
        let con = get_test_connection();
        let sql = "SELECT $1::INTEGER AS value";
        let mut stmt = CachedStatement::prepare(&con, sql).unwrap();
        assert_eq!(stmt.sql.as_ref(), sql);

        let mut first = CheckedI32(100);
        stmt.bind(1, &mut first).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(100));

        stmt.reset_bindings().unwrap();
        let mut second = CheckedI32(200);
        stmt.bind(1, &mut second).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(200));
    }

    #[test]
    fn borrowing_pending_execution_runs_the_statement() {
        // `pending()` borrows the statement (unlike `into_pending`, which consumes
        // it) and drives the query to completion via `execute()`.
        let con = get_test_connection();
        let stmt = CachedStatement::prepare(&con, "SELECT 1 AS v").unwrap();
        let result = stmt.pending().unwrap().execute().unwrap();
        assert_single_value(result, "v", DuckValue::Int(1));
    }

    #[test]
    fn test_cached_statement_recovers_after_invalid_bind_position() {
        let con = get_test_connection();
        let mut stmt = CachedStatement::prepare(&con, "SELECT $1::INTEGER AS value").unwrap();
        let mut invalid = CheckedI32(1);
        assert!(matches!(stmt.bind(0, &mut invalid), Err(Error::DuckDBFailure(..))));

        stmt.reset_bindings().unwrap();
        let mut valid = CheckedI32(55);
        stmt.bind(1, &mut valid).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(55));
    }

    #[test]
    fn test_raw_and_connection() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let stmt = Statement::new(&con, sql).unwrap();
        let _raw = stmt.raw();
        let _con = stmt.connection();
    }

    #[test]
    fn test_execute() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        let result = stmt.execute();
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_can_be_called_multiple_times() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        assert!(stmt.execute().is_ok());
        assert!(stmt.execute().is_ok());
    }

    #[test]
    fn test_clear_bindings_resets_idx() {
        let con = get_test_connection();
        let sql = "SELECT $1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        let mut dummy = DummyAppendAble;
        stmt.bind(&mut dummy).unwrap();
        assert_eq!(stmt.bind_idx, 1);
        stmt.clear_bindings().unwrap();
        assert_eq!(stmt.bind_idx, 0);
    }

    #[test]
    fn statement_type_is_classified() {
        let mut con = get_test_connection();
        let select = CachedStatement::prepare(&con, "SELECT 1").unwrap();
        assert_eq!(select.statement_type(), StatementType::Select);
        drop(select);

        con.query("CREATE TABLE t (id INTEGER)").unwrap();
        let insert = CachedStatement::prepare(&con, "INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(insert.statement_type(), StatementType::Insert);
    }

    #[test]
    fn prepared_parameter_metadata_names_types_and_index() {
        let con = get_test_connection();
        // Two named parameters. (DuckDB rejects mixing named `$x` and positional
        // `?` in one statement, so both are named here.)
        let stmt = CachedStatement::prepare(&con, "SELECT $id::INTEGER AS a, $label::VARCHAR AS b")
            .unwrap();
        assert_eq!(stmt.parameter_count(), 2);
        // Each named parameter reports its name; index round-trips.
        assert_eq!(stmt.parameter_name(1).as_deref(), Some("id"));
        assert_eq!(stmt.parameter_name(2).as_deref(), Some("label"));
        assert_eq!(stmt.parameter_index("id").unwrap(), 1);
        assert_eq!(stmt.parameter_index("label").unwrap(), 2);
        assert_eq!(stmt.parameter_type(1), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        assert_eq!(
            stmt.parameter_logical_type(1),
            Some(TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER))
        );

        // Missing name and out-of-range index are precise errors / None.
        assert!(
            matches!(stmt.parameter_index("nope"), Err(Error::InvalidParameterName(n)) if n == "nope")
        );
        assert!(matches!(stmt.parameter_index("a\0b"), Err(Error::NulError(_))));
        assert_eq!(stmt.parameter_name(999), None);
    }

    #[test]
    fn prepared_output_column_schema() {
        let con = get_test_connection();
        let stmt = CachedStatement::prepare(
            &con,
            "SELECT 1::INTEGER AS id, CAST(1.5 AS DECIMAL(8,2)) AS amount",
        )
        .unwrap();
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.column_name(0).as_deref(), Some("id"));
        assert_eq!(stmt.column_name(1).as_deref(), Some("amount"));
        assert_eq!(stmt.column_type(0), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        // Lossless output type keeps the declared DECIMAL precision.
        assert_eq!(stmt.column_logical_type(1), Some(TypeInfo::Decimal { width: 8, scale: 2 }));
        assert_eq!(stmt.column_name(999), None);
    }

    #[test]
    fn bind_named_binds_by_parameter_name() {
        let con = get_test_connection();
        let mut stmt = CachedStatement::prepare(&con, "SELECT $value::INTEGER AS value").unwrap();
        let mut v = CheckedI32(77);
        stmt.bind_named("value", &mut v).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(77));

        // An unknown name is a precise error, and the statement stays usable.
        let mut other = CheckedI32(1);
        assert!(matches!(
            stmt.bind_named("missing", &mut other),
            Err(Error::InvalidParameterName(n)) if n == "missing"
        ));
    }
}
