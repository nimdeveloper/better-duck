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
        // SAFETY: `self.pending` is valid; `finish_pending` runs `duckdb_execute_pending`,
        // destroys the pending handle exactly once, and nulls it so `Drop` is a no-op.
        unsafe { finish_pending(&mut self.pending) }
    }
}

/// Runs `duckdb_execute_pending`, destroys the pending handle exactly once
/// (nulling `*pending` so a later `Drop` is a no-op), and materialises the result.
///
/// Shared by [`PendingResult::execute`] and [`OwnedPending::execute`].
///
/// # Safety
///
/// `*pending` must be a valid, non-null `duckdb_pending_result` not yet destroyed.
unsafe fn finish_pending(pending: &mut duckdb_pending_result) -> Result<DuckResult> {
    // SAFETY: a zeroed `duckdb_result` is the correct initial output state.
    let mut out = unsafe { mem::zeroed::<duckdb_result>() };
    // SAFETY: `*pending` is valid; `&mut out` is a valid output pointer.
    // `duckdb_execute_pending` writes the (possibly-error) result into `out`;
    // ownership of `out` transfers to `DuckResult`/the error adapter, which destroys
    // it exactly once.
    let rc = unsafe { duckdb_execute_pending(*pending, &mut out as *mut duckdb_result) };
    // SAFETY: `*pending` is valid; destroyed exactly once and nulled here.
    unsafe { duckdb_destroy_pending(pending) };
    crate::helpers::duck_result::result_from_duckdb_result(rc, &mut out as *mut duckdb_result)?;
    Ok(DuckResult::new(out))
}

/// An owned pending execution: like [`PendingResult`] but it *owns* its
/// [`CachedStatement`] instead of borrowing one, so it carries no lifetime and is
/// `'static`.
///
/// This is what lets the async adapter step a query one task per
/// `spawn_blocking` dispatch — the whole `OwnedPending` moves in and out of each
/// blocking task, which a borrow-based `PendingResult` could not do.
pub struct OwnedPending {
    /// Destroyed first (declared before `_stmt`): the pending references the
    /// statement, so it must be torn down before the statement's handle.
    pending: duckdb_pending_result,
    /// Owned statement, kept alive (with its connection) for the pending's whole
    /// life. Never read directly — the pending uses the handle DuckDB copied at
    /// creation — but its `Drop` must run *after* the pending's, hence the field
    /// order above.
    _stmt: CachedStatement,
}

// SAFETY: `OwnedPending` owns its pending handle and `CachedStatement` outright
// and exposes no interior mutability. DuckDB permits moving a pending result to a
// different thread as long as it is not used from two at once; `&mut self` on
// every stepping method and the lack of `Sync` guarantee no concurrent use. This
// mirrors `CachedStatement`'s own `Send` (which this value also contains).
unsafe impl Send for OwnedPending {}

impl OwnedPending {
    /// Creates an owned pending execution from an owned prepared statement.
    ///
    /// Bind parameters on `stmt` before calling this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Engine`] if DuckDB cannot create the pending result.
    pub(crate) fn new(stmt: CachedStatement) -> Result<OwnedPending> {
        let mut pending: duckdb_pending_result = std::ptr::null_mut();
        // SAFETY: `stmt.handle()` is a valid prepared statement owned by `stmt`, which
        // this value retains; `&mut pending` is a valid output pointer. DuckDB requires
        // the pending result to be destroyed regardless of the return code.
        let rc = unsafe { duckdb_pending_prepared(stmt.handle(), &mut pending) };
        if rc != DuckDBSuccess {
            // SAFETY: `pending` is the (possibly-error) handle; error string borrowed
            // until destroy, so copy first, then destroy exactly once.
            let message = unsafe { pending_error_message(pending) };
            // SAFETY: destroyed exactly once here on the failure path.
            unsafe { duckdb_destroy_pending(&mut pending) };
            return Err(Error::Engine(EngineError::unavailable(Some(
                message.unwrap_or_else(|| "failed to create pending result".to_owned()),
            ))));
        }
        Ok(OwnedPending { pending, _stmt: stmt })
    }

    /// Executes a single task, returning the resulting [`PendingState`].
    #[must_use = "the returned state says whether to keep stepping, finish, or stop on error"]
    pub fn execute_task(&mut self) -> PendingState {
        // SAFETY: `self.pending` is a valid, live pending result.
        PendingState::from_raw(unsafe { duckdb_pending_execute_task(self.pending) })
    }

    /// Returns the current [`PendingState`] without executing a task.
    ///
    /// On a multi-threaded build [`execute_task`](OwnedPending::execute_task) can keep
    /// reporting [`NoTasksAvailable`](PendingState::NoTasksAvailable) while background
    /// workers run the query; this reports the authoritative readiness in that case.
    #[must_use]
    pub fn check_state(&mut self) -> PendingState {
        // SAFETY: `self.pending` is a valid, live pending result.
        PendingState::from_raw(unsafe { duckdb_pending_execute_check_state(self.pending) })
    }

    /// Returns DuckDB's current error message, if any.
    #[must_use]
    pub fn error(&self) -> Option<String> {
        // SAFETY: `self.pending` is a valid, live pending result.
        unsafe { pending_error_message(self.pending) }
    }

    /// Runs to completion and returns the materialised [`DuckResult`], consuming
    /// `self` so the result is transferred exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Engine`] carrying DuckDB's classification/message on failure.
    #[must_use = "the DuckResult owns the query output; consume it"]
    pub fn execute(mut self) -> Result<DuckResult> {
        // SAFETY: `self.pending` is valid; `finish_pending` executes, destroys the
        // handle once, and nulls it so `Drop` is a no-op. `self._stmt` drops afterwards.
        unsafe { finish_pending(&mut self.pending) }
    }
}

impl Drop for OwnedPending {
    fn drop(&mut self) {
        if self.pending.is_null() {
            return;
        }
        // SAFETY: `self.pending` is a valid, non-null handle owned exclusively by this
        // value (null-guarded above); destroyed exactly once, before `stmt` drops.
        unsafe { duckdb_destroy_pending(&mut self.pending) };
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
            let stepped = pending.execute_task();
            assert_ne!(stepped, PendingState::Error, "unexpected error: {:?}", pending.error());
            // On a multi-threaded build, `execute_task` can report `NoTasksAvailable`
            // while background workers run the query and never itself return `Ready`;
            // `check_state` is the authoritative readiness in that case.
            state = if stepped.is_finished() { stepped } else { pending.check_state() };
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
