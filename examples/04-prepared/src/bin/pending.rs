//! Incremental (task-at-a-time) execution with the pending API.
//!
//! `CachedStatement::pending()` returns a `PendingResult` that runs the query one task at
//! a time — useful for cooperative scheduling or progress reporting. `into_pending()`
//! yields an `OwnedPending` that is `Send` + `'static` (no borrow of the statement).

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{CachedStatement, PendingState};
use common::{section, show, Result};

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    section("borrowed PendingResult: step task-by-task, then finalize");
    let stmt = CachedStatement::prepare(conn.db(), "SELECT count(*) AS n FROM range(50000)")?;
    let mut pending = stmt.pending()?;
    let mut steps = 0u32;
    loop {
        let state = pending.execute_task();
        steps += 1;
        if state.is_finished() || matches!(state, PendingState::Error) {
            break;
        }
        // On a multi-threaded build, execute_task can report NoTasksAvailable while
        // background workers run the query; check_state is the authoritative readiness.
        if matches!(state, PendingState::NoTasksAvailable) && pending.check_state().is_finished() {
            break;
        }
    }
    show("tasks stepped", steps);
    let row = pending.execute()?.next().expect("row")?;
    show("count", row.get("n"));
    assert_eq!(row.get("n"), Some(&DuckValue::BigInt(50000)));

    section("OwnedPending: Send + 'static, no borrow of the statement");
    let mut owned = CachedStatement::prepare(conn.db(), "SELECT 42 AS v")?.into_pending()?;
    loop {
        let state = owned.execute_task();
        if state.is_finished() {
            break;
        }
        if matches!(state, PendingState::NoTasksAvailable) && owned.check_state().is_finished() {
            break;
        }
    }
    let row = owned.execute()?.next().expect("row")?;
    assert_eq!(row.get("v"), Some(&DuckValue::Int(42)));

    Ok(())
}
