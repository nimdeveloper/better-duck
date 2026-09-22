//! RAII wrapper for DuckDB's external task scheduler (`duckdb_task_state`).
//!
//! By default DuckDB runs its own background threads. The external task API lets an
//! application drive DuckDB's work on its *own* threads instead: create a
//! [`TaskState`] for a database, then call [`execute`](TaskState::execute) /
//! [`execute_n`](TaskState::execute_n) from one or more worker threads; those calls
//! run pending tasks until [`finish`](TaskState::finish) is signalled.
//!
//! Safety-critical ownership rule (from the C API): `duckdb_destroy_task_state` must
//! **not** run while any `execute_*` call is still active on that state. [`TaskState`]
//! encodes this by keeping the database [`Arc`] alive for its whole lifetime and by
//! taking `&self` for the executor calls — a `TaskState` shared across worker threads
//! lives behind an [`Arc`], so it is only destroyed once the last worker has dropped
//! its clone. It is `Send + Sync`: the C API explicitly documents that multiple
//! threads may share one task state.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::sync::Arc;

use crate::{
    database::Database,
    ffi::{
        duckdb_create_task_state, duckdb_destroy_task_state, duckdb_execute_n_tasks_state,
        duckdb_execute_tasks, duckdb_execute_tasks_state, duckdb_finish_execution,
        duckdb_task_state, duckdb_task_state_is_finished, idx_t,
    },
    raw::connection::RawDatabase,
};

/// Runs up to `max_tasks` of a database's pending tasks on the calling thread, once.
///
/// A convenience over the stateful API for the common "help DuckDB along" case; for
/// long-running or multi-threaded execution use a [`TaskState`].
pub fn execute_tasks(
    db: &Database,
    max_tasks: u64,
) {
    // SAFETY: `db.handle()` is a valid open database handle kept alive by `db`.
    unsafe { duckdb_execute_tasks(db.handle(), max_tasks as idx_t) };
}

/// An owned `duckdb_task_state` that drives a database's task execution.
///
/// Holds the database [`Arc`] so the underlying database outlives the state.
pub struct TaskState {
    state: duckdb_task_state,
    // Keeps the database alive at least as long as this task state.
    _db: Arc<RawDatabase>,
}

// SAFETY: DuckDB documents that a single `duckdb_task_state` may be shared across
// multiple threads (they all call `duckdb_execute_tasks_state` on it), so it is safe
// to send between and share across threads.
unsafe impl Send for TaskState {}
// SAFETY: as above — concurrent `execute_*` on one state from several threads is the
// documented usage.
unsafe impl Sync for TaskState {}

impl TaskState {
    /// Creates a task state for `db`.
    #[must_use]
    pub fn new(db: &Database) -> TaskState {
        // SAFETY: `db.handle()` is a valid open database handle.
        let state = unsafe { duckdb_create_task_state(db.handle()) };
        TaskState { state, _db: db.arc() }
    }

    /// Executes pending tasks on the calling thread until [`finish`](TaskState::finish)
    /// is signalled. Intended to be run on a dedicated worker thread (and may be run
    /// on several threads sharing this state).
    pub fn execute(&self) {
        // SAFETY: `self.state` is a valid task state; the database is kept alive by the
        // retained `Arc`, and destruction cannot race this call (see the type docs).
        unsafe { duckdb_execute_tasks_state(self.state) };
    }

    /// Executes up to `max_tasks` pending tasks on the calling thread, returning how
    /// many actually ran. Stops early if [`finish`](TaskState::finish) is signalled
    /// or there are no more tasks.
    pub fn execute_n(
        &self,
        max_tasks: u64,
    ) -> u64 {
        // SAFETY: `self.state` is a valid task state kept usable by the retained `Arc`.
        unsafe { duckdb_execute_n_tasks_state(self.state, max_tasks as idx_t) as u64 }
    }

    /// Signals every `execute_*` call on this state to stop.
    pub fn finish(&self) {
        // SAFETY: `self.state` is a valid task state.
        unsafe { duckdb_finish_execution(self.state) };
    }

    /// Whether [`finish`](TaskState::finish) has been signalled on this state.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        // SAFETY: `self.state` is a valid task state.
        unsafe { duckdb_task_state_is_finished(self.state) }
    }
}

impl Drop for TaskState {
    fn drop(&mut self) {
        if !self.state.is_null() {
            // Ensure no executor keeps running, then destroy. The C API forbids
            // destroying a state with an active `execute_*`; by construction every
            // executor borrows `&self`, so no `execute_*` can outlive this drop.
            // SAFETY: `self.state` is a valid, non-null task state; signalling finish
            // then destroying it exactly once is the documented teardown order.
            unsafe {
                duckdb_finish_execution(self.state);
                duckdb_destroy_task_state(self.state);
            }
            self.state = std::ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_state_lifecycle_and_execution() {
        let db = Database::open_in_memory().unwrap();
        // Run some work so there are tasks to drive.
        let mut conn = db.connect().unwrap();
        conn.execute_batch("CREATE TABLE t AS SELECT * FROM range(1000) tbl(i)").unwrap();

        let state = TaskState::new(&db);
        assert!(!state.is_finished(), "a fresh state is not finished");
        // Draining tasks on this thread is safe and returns a count.
        let _ran = state.execute_n(16);
        // Signalling finish flips the flag and makes execute() return promptly.
        state.finish();
        assert!(state.is_finished());
        state.execute();
        // Dropped here: finish + destroy, exactly once.
    }

    #[test]
    fn shared_state_across_threads() {
        let db = Database::open_in_memory().unwrap();
        let state = Arc::new(TaskState::new(&db));
        // Several worker threads share one task state (the documented usage).
        let mut handles = Vec::new();
        for _ in 0..3 {
            let s = Arc::clone(&state);
            handles.push(std::thread::spawn(move || {
                s.execute_n(4);
            }));
        }
        state.finish();
        for h in handles {
            h.join().unwrap();
        }
        assert!(state.is_finished());
    }

    #[test]
    fn execute_tasks_convenience_runs() {
        let db = Database::open_in_memory().unwrap();
        // Just exercises the one-shot helper; it must not panic or hang.
        execute_tasks(&db, 8);
    }
}
