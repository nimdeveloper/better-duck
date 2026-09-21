//! Incremental ("pending") execution of a prepared statement.
//!
//! `duckdb_pending_prepared` turns a prepared statement into a
//! [`PendingResult`], which runs the query one task at a time
//! ([`execute_task`](PendingResult::execute_task)) so control can return to the
//! caller between tasks — e.g. to poll for cancellation via
//! [`QueryControl`](crate::raw::connection::QueryControl). When the state reports
//! ready, [`execute`](PendingResult::execute) materialises the final
//! [`DuckResult`], transferring result ownership exactly once.

use std::ffi::CStr;
use std::marker::PhantomData;
use std::mem;

use crate::{
    error::{EngineError, Error, Result},
    ffi::{
        duckdb_destroy_pending, duckdb_execute_pending, duckdb_pending_error,
        duckdb_pending_execute_check_state, duckdb_pending_execute_task,
        duckdb_pending_execution_is_finished, duckdb_pending_prepared, duckdb_pending_result,
        duckdb_pending_state, duckdb_result, DuckDBSuccess,
    },
    raw::{result::DuckResult, statement::CachedStatement},
};

/// The state of a pending execution, as reported by DuckDB.
///
/// Mirrors `duckdb_pending_state`. `#[non_exhaustive]` and carries
/// [`Unknown`](PendingState::Unknown) so a value a future DuckDB adds is
/// preserved rather than lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PendingState {
    /// The result is ready; call [`execute`](PendingResult::execute).
    Ready,
    /// More tasks remain; call [`execute_task`](PendingResult::execute_task) again.
    NotReady,
    /// Execution failed; [`error`](PendingResult::error) has the message.
    Error,
    /// No tasks are currently available (e.g. waiting on another thread).
    NoTasksAvailable,
    /// A state this build does not recognise; the raw value is preserved.
    Unknown(duckdb_pending_state),
}

impl PendingState {
    fn from_raw(raw: duckdb_pending_state) -> PendingState {
        use crate::ffi as f;
        match raw {
            f::duckdb_pending_state_DUCKDB_PENDING_RESULT_READY => PendingState::Ready,
            f::duckdb_pending_state_DUCKDB_PENDING_RESULT_NOT_READY => PendingState::NotReady,
            f::duckdb_pending_state_DUCKDB_PENDING_ERROR => PendingState::Error,
            f::duckdb_pending_state_DUCKDB_PENDING_NO_TASKS_AVAILABLE => {
                PendingState::NoTasksAvailable
            },
            other => PendingState::Unknown(other),
        }
    }

    /// Returns the raw `duckdb_pending_state` for this state.
    fn to_raw(self) -> duckdb_pending_state {
        use crate::ffi as f;
        match self {
            PendingState::Ready => f::duckdb_pending_state_DUCKDB_PENDING_RESULT_READY,
            PendingState::NotReady => f::duckdb_pending_state_DUCKDB_PENDING_RESULT_NOT_READY,
            PendingState::Error => f::duckdb_pending_state_DUCKDB_PENDING_ERROR,
            PendingState::NoTasksAvailable => {
                f::duckdb_pending_state_DUCKDB_PENDING_NO_TASKS_AVAILABLE
            },
            PendingState::Unknown(raw) => raw,
        }
    }

    /// Returns `true` if this state means execution has finished (result ready).
    ///
    /// Uses DuckDB's own `duckdb_pending_execution_is_finished` rather than
    /// comparing to `Ready` directly, so the definition stays in sync with DuckDB.
    #[must_use]
    pub fn is_finished(self) -> bool {
        // SAFETY: `duckdb_pending_execution_is_finished` is a pure classifier over
        // the state value; no handle is involved.
        unsafe { duckdb_pending_execution_is_finished(self.to_raw()) }
    }
}

/// A pending (incrementally-executed) query, borrowing the statement it runs.
///
/// Owns the `duckdb_pending_result` handle and destroys it in [`Drop`]. Borrows
/// the [`CachedStatement`] for `'a` so the statement (and, through it, the
/// connection) cannot be destroyed while the pending execution references it.
pub struct PendingResult<'a> {
    pending: duckdb_pending_result,
    _stmt: PhantomData<&'a CachedStatement>,
}

impl<'a> PendingResult<'a> {
    /// Creates a pending execution from a prepared statement.
    ///
    /// Bind parameters on `stmt` *before* calling this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Engine`] with DuckDB's message if the pending result
    /// cannot be created. The pending handle is destroyed regardless of outcome
    /// (in `Drop` on success, or here on failure), as DuckDB requires.
    pub(crate) fn new(stmt: &'a CachedStatement) -> Result<PendingResult<'a>> {
        let mut pending: duckdb_pending_result = std::ptr::null_mut();
        // SAFETY: `stmt.handle()` is a valid prepared statement kept alive by the
        // `'a` borrow; `&mut pending` is a valid output pointer. DuckDB requires the
        // pending result to be destroyed regardless of the return code.
        let rc = unsafe { duckdb_pending_prepared(stmt.handle(), &mut pending) };
        if rc != DuckDBSuccess {
            // SAFETY: `pending` is the (possibly-error) handle DuckDB produced; its
            // error string is borrowed until we destroy it, so copy first.
            let message = unsafe { pending_error_message(pending) };
            // SAFETY: `pending` is destroyed exactly once here on the failure path.
            unsafe { duckdb_destroy_pending(&mut pending) };
            return Err(Error::Engine(EngineError::unavailable(Some(
                message.unwrap_or_else(|| "failed to create pending result".to_owned()),
            ))));
        }
        Ok(PendingResult { pending, _stmt: PhantomData })
    }

    /// Executes a single task, returning the resulting [`PendingState`].
    ///
    /// Call repeatedly while the state is [`NotReady`](PendingState::NotReady) or
    /// [`NoTasksAvailable`](PendingState::NoTasksAvailable); stop on
    /// [`Ready`](PendingState::Ready) (then call [`execute`](PendingResult::execute))
    /// or [`Error`](PendingState::Error).
    #[must_use = "the returned state says whether to keep stepping, finish, or stop on error"]
    pub fn execute_task(&mut self) -> PendingState {
        // SAFETY: `self.pending` is a valid, live pending result.
        PendingState::from_raw(unsafe { duckdb_pending_execute_task(self.pending) })
    }

    /// Returns the current [`PendingState`] without executing a task.
    #[must_use]
    pub fn check_state(&mut self) -> PendingState {
        // SAFETY: `self.pending` is a valid, live pending result.
        PendingState::from_raw(unsafe { duckdb_pending_execute_check_state(self.pending) })
    }

    /// Returns DuckDB's current error message for this pending result, if any.
    #[must_use]
    pub fn error(&self) -> Option<String> {
        // SAFETY: `self.pending` is a valid, live pending result; the error string
        // is borrowed (freed on destroy), so it is copied out here.
        unsafe { pending_error_message(self.pending) }
    }

    /// Runs the pending execution to completion and returns the materialised
    /// [`DuckResult`], consuming `self` so the result is transferred exactly once.
    ///
    /// This drives any remaining tasks internally; call it directly for the simple
    /// case, or step with [`execute_task`](PendingResult::execute_task) first when
    /// you need to interleave cancellation checks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Engine`] carrying DuckDB's classification/message if
    /// execution fails.
    #[must_use = "the DuckResult owns the query output; consume it"]
    pub fn execute(mut self) -> Result<DuckResult> {
        // SAFETY: a zeroed `duckdb_result` is the correct initial output state.
        let mut out = unsafe { mem::zeroed::<duckdb_result>() };
        // SAFETY: `self.pending` is valid; `&mut out` is a valid output pointer.
        // `duckdb_execute_pending` writes the (possibly-error) result into `out`;
        // ownership of `out` transfers to `DuckResult`/the error adapter, which
        // destroys it exactly once.
        let rc = unsafe { duckdb_execute_pending(self.pending, &mut out as *mut duckdb_result) };
        // The pending handle has done its job; destroy it now (Drop would also, but
        // `self` is consumed here and we want the result adapter to own `out`).
        // SAFETY: `self.pending` is valid and destroyed exactly once; we then null
        // it so `Drop` is a no-op.
        unsafe { duckdb_destroy_pending(&mut self.pending) };
        crate::helpers::duck_result::result_from_duckdb_result(rc, &mut out as *mut duckdb_result)?;
        Ok(DuckResult::new(out))
    }
}

/// Copies a pending result's borrowed error string into an owned `String`.
///
/// # Safety
///
/// `pending` must be a valid, non-null `duckdb_pending_result`.
unsafe fn pending_error_message(pending: duckdb_pending_result) -> Option<String> {
    if pending.is_null() {
        return None;
    }
    // SAFETY: `pending` is valid per the contract; `duckdb_pending_error` returns a
    // borrowed C string (or null) owned by the pending result — copied, not freed.
    let raw = unsafe { duckdb_pending_error(pending) };
    if raw.is_null() {
        return None;
    }
    // SAFETY: `raw` is a non-null, null-terminated C string borrowed from the
    // pending result; we copy it into an owned String without freeing it.
    Some(unsafe { CStr::from_ptr(raw) }.to_string_lossy().into_owned())
}

impl Drop for PendingResult<'_> {
    fn drop(&mut self) {
        if self.pending.is_null() {
            return;
        }
        // SAFETY: `self.pending` is a valid, non-null handle owned exclusively by
        // this value (null-guarded above); destroyed exactly once.
        unsafe { duckdb_destroy_pending(&mut self.pending) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config, helpers::path::path_to_cstring, raw::connection::RawConnection,
        types::value::DuckValue,
    };

    fn conn() -> RawConnection {
        let path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&path, config).unwrap()
    }

    #[test]
    fn step_to_ready_then_execute_yields_the_result() {
        let con = conn();
        let stmt = CachedStatement::prepare(&con, "SELECT 7 AS v").unwrap();
        let mut pending = PendingResult::new(&stmt).unwrap();

        // Step until finished (bounded — a tiny query needs very few tasks).
        let mut state = pending.check_state();
        for _ in 0..1_000 {
            if state.is_finished() {
                break;
            }
            state = pending.execute_task();
            assert_ne!(state, PendingState::Error, "unexpected error: {:?}", pending.error());
        }
        assert!(state.is_finished(), "pending never reached a finished state");

        let mut result = pending.execute().unwrap();
        let row = result.next().unwrap().unwrap();
        assert_eq!(row.get("v"), Some(&DuckValue::Int(7)));
    }

    #[test]
    fn execute_without_manual_stepping_drives_to_completion() {
        let con = conn();
        let stmt = CachedStatement::prepare(&con, "SELECT 'hi' AS s").unwrap();
        let pending = PendingResult::new(&stmt).unwrap();
        // execute() drives remaining tasks internally.
        let mut result = pending.execute().unwrap();
        let row = result.next().unwrap().unwrap();
        assert_eq!(row.get("s"), Some(&DuckValue::Text("hi".into())));
    }

    #[test]
    fn is_finished_matches_duckdb_classifier() {
        // `Ready` is finished; `NotReady` is not. (The exact classification of
        // `Error`/`NoTasksAvailable` is DuckDB's own via
        // `duckdb_pending_execution_is_finished`, so we don't pin it here.)
        assert!(PendingState::Ready.is_finished());
        assert!(!PendingState::NotReady.is_finished());
    }

    #[test]
    fn unknown_state_round_trips() {
        assert_eq!(PendingState::from_raw(999), PendingState::Unknown(999));
        assert_eq!(PendingState::Unknown(999).to_raw(), 999);
    }

    // NOTE: a pending-execution *runtime error* test is intentionally omitted.
    // `execute()` routes failures through the same `result_from_duckdb_result`
    // adapter as `Statement::execute`/`RawConnection::query`, whose typed-error
    // path is covered in `helpers/duck_result.rs` and the connection/statement
    // suites. Constructing a query that reliably errors *during pending stepping*
    // (not folded at prepare, not aborting across FFI like the extract path) is
    // brittle and build-dependent, so the error path is covered at the shared
    // adapter instead of here.
}
