//! Driving DuckDB's task scheduler with `execute_tasks` and `TaskState`.
//!
//! By default DuckDB runs its own background threads. The external task API lets an
//! application run that work on its own threads instead. `execute_tasks(&db, max)` is the
//! one-shot helper; [`TaskState`] is the stateful form: `execute` / `execute_n` run pending
//! tasks (optionally on several threads sharing one state) until `finish` is signalled,
//! after which `is_finished` reports true.

use std::sync::Arc;

use better_duck_core::database::Database;
use better_duck_core::{execute_tasks, TaskState};
use common::{section, show, Result};

fn main() -> Result<()> {
    let db = Database::open_in_memory()?;
    // Create some work so the scheduler has tasks to run.
    let mut conn = db.connect()?;
    conn.execute_batch("CREATE TABLE t AS SELECT * FROM range(1000) tbl(i)")?;

    section("execute_tasks: one-shot, run up to N pending tasks on this thread");
    execute_tasks(&db, 8);
    show("execute_tasks(8) returned", "ok");

    section("TaskState: create, drain, finish");
    let state = TaskState::new(&db);
    show("is_finished (fresh)", state.is_finished());
    assert!(!state.is_finished(), "a fresh state is not finished");

    // Drain up to 16 tasks on this thread; the count is how many actually ran.
    let ran = state.execute_n(16);
    show("execute_n(16) ran", ran);

    // Signalling finish flips the flag and makes execute() return promptly.
    state.finish();
    show("is_finished (after finish)", state.is_finished());
    assert!(state.is_finished());
    state.execute(); // returns immediately now

    section("TaskState is Send + Sync: share one across worker threads");
    let shared = Arc::new(TaskState::new(&db));
    let mut handles = Vec::new();
    for _ in 0..3 {
        let s = Arc::clone(&shared);
        handles.push(std::thread::spawn(move || s.execute_n(4)));
    }
    // Signal the workers to stop, then join them.
    shared.finish();
    let mut total = 0u64;
    for h in handles {
        total += h.join().expect("worker thread ok");
    }
    show("tasks run across worker threads", total);
    assert!(shared.is_finished());

    Ok(())
}
