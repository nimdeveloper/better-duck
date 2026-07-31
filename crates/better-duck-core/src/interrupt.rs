//! Query cancellation and progress reporting — [`InterruptHandle`], obtained
//! via [`Connection::interrupt_handle`](crate::connection::Connection::interrupt_handle).

use std::sync::Arc;

use parking_lot::Mutex;

use crate::ffi::{self, duckdb_connection};

/// A cheap, cloneable handle that can interrupt the query currently running
/// on the [`Connection`](crate::connection::Connection) it was created from,
/// or read its progress — safe to call from a different thread than the one
/// executing the query, which is exactly the point: the connection itself is
/// blocked for the query's whole duration, so cancelling or polling it must
/// happen from elsewhere. DuckDB documents `duckdb_interrupt`/
/// `duckdb_query_progress` as the sanctioned exception to a connection's
/// otherwise single-threaded-only usage.
///
/// Every clone refers to the same underlying connection. The handle can
/// safely outlive that connection — see [`interrupt`](Self::interrupt) and
/// [`progress`](Self::progress).
///
/// # A successfully interrupted connection is spent
///
/// Calling [`interrupt`](Self::interrupt) while the originating connection
/// has a query actually running does not just cancel that query — it also
/// leaves the connection permanently unable to run further queries. Every
/// later query on it fails immediately (safely — no crash, just an `Err`).
/// This isn't a bug in this crate; it reflects DuckDB's own C API behavior
/// for `duckdb_interrupt`. Treat a successful interrupt as consuming the
/// connection: open a fresh one (or interrupt a
/// [`try_clone`](crate::connection::Connection::try_clone)d connection you
/// intend to discard) rather than expecting to reuse it afterward.
///
/// Calling `interrupt()` while the connection is *idle* (no query running)
/// is a true no-op — it does not poison the connection.
#[derive(Clone)]
pub struct InterruptHandle {
    con: duckdb_connection,
    /// Shared with the originating `RawConnection` — `true` while `con` is
    /// open. See `RawConnection::close`, which holds this same lock for its
    /// entire `duckdb_disconnect` call, making every method here race-free
    /// against a concurrent close rather than merely "usually fine".
    alive: Arc<Mutex<bool>>,
}

// SAFETY: `duckdb_interrupt`/`duckdb_query_progress` are documented by DuckDB
// as safe to call from a different thread than the one executing a query on
// `con` — that is this type's entire purpose. `alive` gates every use
// against a concurrent disconnect (see the field's own doc comment above),
// so a stale `con` is never dereferenced after the connection closes.
unsafe impl Send for InterruptHandle {}
// SAFETY: every method takes `&self` and only ever reads `con` (a `Copy`
// value) after taking the `alive` lock; there is no interior mutation of
// `con` itself for two handles to race on.
unsafe impl Sync for InterruptHandle {}

impl InterruptHandle {
    pub(crate) fn new(
        con: duckdb_connection,
        alive: Arc<Mutex<bool>>,
    ) -> Self {
        Self { con, alive }
    }

    /// Interrupts the query currently running on the originating connection,
    /// if any. A no-op if the connection has since been closed, or if no
    /// query is currently running on it.
    ///
    /// After this call, the originating connection can no longer run
    /// queries — see the [type-level docs](Self) for why this isn't a bug.
    pub fn interrupt(&self) {
        let alive = self.alive.lock();
        if *alive {
            // SAFETY: holding `self.alive`'s lock guarantees the connection
            // has not been, and cannot concurrently be, disconnected while
            // this call is in flight.
            unsafe { ffi::duckdb_interrupt(self.con) };
        }
    }

    /// Reads the progress of the query currently running on the originating
    /// connection. Returns `None` if the connection has since been closed;
    /// otherwise a [`QueryProgress`] snapshot, whose
    /// [`percentage`](QueryProgress::percentage) is itself `None` when no
    /// query is running or DuckDB has no estimate available for it.
    pub fn progress(&self) -> Option<QueryProgress> {
        let alive = self.alive.lock();
        if !*alive {
            return None;
        }
        // SAFETY: see `interrupt`.
        let raw = unsafe { ffi::duckdb_query_progress(self.con) };
        Some(QueryProgress::from_raw(raw))
    }
}

/// A snapshot of an in-flight query's execution progress
/// (`duckdb_query_progress_type`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryProgress {
    /// Percent complete in `0.0..=100.0`, or `None` if DuckDB has no
    /// progress estimate available for the current query right now (it
    /// reports a negative sentinel in that case, which this hides).
    pub percentage: Option<f64>,
    /// Rows processed so far.
    pub rows_processed: u64,
    /// Estimated total rows to process.
    pub total_rows_to_process: u64,
}

impl QueryProgress {
    pub(crate) fn from_raw(raw: ffi::duckdb_query_progress_type) -> Self {
        Self {
            percentage: if raw.percentage < 0.0 { None } else { Some(raw.percentage) },
            rows_processed: raw.rows_processed,
            total_rows_to_process: raw.total_rows_to_process,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn interrupt_handle_is_send_and_sync() {
        assert_send_sync::<InterruptHandle>();
    }

    #[test]
    fn query_progress_from_raw_maps_negative_percentage_to_none() {
        let raw = ffi::duckdb_query_progress_type {
            percentage: -1.0,
            rows_processed: 0,
            total_rows_to_process: 0,
        };
        assert_eq!(QueryProgress::from_raw(raw).percentage, None);
    }

    #[test]
    fn query_progress_from_raw_preserves_a_real_percentage() {
        let raw = ffi::duckdb_query_progress_type {
            percentage: 42.5,
            rows_processed: 100,
            total_rows_to_process: 235,
        };
        let progress = QueryProgress::from_raw(raw);
        assert_eq!(progress.percentage, Some(42.5));
        assert_eq!(progress.rows_processed, 100);
        assert_eq!(progress.total_rows_to_process, 235);
    }

    #[test]
    fn interrupt_after_alive_flag_cleared_is_a_harmless_no_op() {
        let alive = Arc::new(Mutex::new(false));
        // A null `con` is safe here specifically *because* `alive` is
        // already false: `interrupt`/`progress` never dereference `con`
        // once that's the case.
        let handle = InterruptHandle::new(std::ptr::null_mut(), alive);
        handle.interrupt();
        assert_eq!(handle.progress(), None);
    }
}
