use std::{path::Path, sync::Arc};

use parking_lot::Mutex;

use crate::{
    connection::Connection,
    error::{Error, Result},
    raw::{
        connection::{ConnectionInner, QueryControl},
        statement::CachedStatement,
    },
    result_set::ResultSet,
    types::{appendable::AppendAble, value::DuckValue},
    Appender,
};

/// An async facade over a [`Connection`].
///
/// Every method dispatches to `tokio::task::spawn_blocking`, so the connection
/// never blocks the async executor. The handle is cheap to clone; clones share
/// one connection, serialized by an internal mutex.
///
/// # Cancellation
///
/// Query control ([`query_control`](AsyncConnection::query_control),
/// [`interrupt`](AsyncConnection::interrupt)) goes through a separate
/// [`QueryControl`] source that does **not** take the connection mutex, so it
/// works *while* a native query holds that mutex on a blocking thread.
///
/// The query-executing futures are cancel-on-drop: dropping one requests
/// interruption of the native query via that control. Note the honest limitation
/// — `tokio::task::spawn_blocking` tasks cannot be aborted, so dropping the
/// future does not instantly stop native work; it signals DuckDB to interrupt
/// and lets the blocking task wind down. The connection becomes usable again once
/// that task releases the mutex.
///
/// # Panics
///
/// These methods panic if called outside a Tokio runtime context, matching
/// `tokio::task::spawn_blocking`.
#[derive(Clone)]
pub struct AsyncConnection {
    inner: Arc<Mutex<Connection>>,
    /// A mutex-free handle to the same underlying connection, used only to mint
    /// [`QueryControl`]s for interrupt/progress. Points at the *same*
    /// `ConnectionInner` as `inner`'s `Connection`, so it observes the generation
    /// that query execution advances.
    control_src: Arc<ConnectionInner>,
}

impl AsyncConnection {
    /// Wraps an existing [`Connection`] for async use.
    pub fn new(conn: Connection) -> AsyncConnection {
        let control_src = Arc::clone(conn.inner());
        AsyncConnection { inner: Arc::new(Mutex::new(conn)), control_src }
    }

    /// Recovers the inner [`Connection`] if this is the last handle.
    ///
    /// Returns `self` unchanged (as `Err`) if other clones of this handle exist.
    pub fn try_into_inner(self) -> std::result::Result<Connection, AsyncConnection> {
        let control_src = Arc::clone(&self.control_src);
        Arc::try_unwrap(self.inner)
            .map(Mutex::into_inner)
            .map_err(|inner| AsyncConnection { inner, control_src })
    }

    /// Returns a [`QueryControl`] for the query currently running (or next to run)
    /// on this connection.
    ///
    /// Does not take the connection mutex, so it can be called — and used to
    /// interrupt — while a query holds that mutex on a blocking thread.
    #[must_use]
    pub fn query_control(&self) -> QueryControl {
        QueryControl::from_inner(Arc::clone(&self.control_src))
    }

    /// Requests interruption of the query currently running on this connection.
    ///
    /// Returns `true` if an interrupt was signalled to DuckDB. Non-blocking and
    /// safe to call from any thread while a query runs.
    pub fn interrupt(&self) -> bool {
        self.query_control().interrupt()
    }

    /// Opens a connection to a DuckDB database at the given file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub async fn open<P>(path: P) -> Result<AsyncConnection>
    where
        P: AsRef<Path> + Send + 'static,
    {
        let conn = tokio::task::spawn_blocking(move || Connection::open(path))
            .await
            .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))??;
        Ok(AsyncConnection::new(conn))
    }

    /// Opens an in-memory DuckDB connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub async fn open_in_memory() -> Result<AsyncConnection> {
        let conn = tokio::task::spawn_blocking(Connection::open_in_memory)
            .await
            .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))??;
        Ok(AsyncConnection::new(conn))
    }

    /// Runs an arbitrary closure against the connection on a blocking thread.
    ///
    /// This is the primitive every other method on this type is built from. Use
    /// it directly for transactions and anything the typed helpers don't cover.
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns one, or if the background task
    /// panics or is cancelled.
    pub async fn with_connection<F, T>(
        &self,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let handle = tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock();
            f(&mut guard)
        });

        // Interrupt the native query if this future is dropped before the blocking
        // task finishes. `spawn_blocking` tasks cannot be aborted, so this does not
        // stop the task instantly; it signals DuckDB to interrupt, letting the task
        // wind down and release the connection. The guard is disarmed on normal
        // completion so a finished query is never spuriously interrupted.
        let mut interrupt_on_drop = InterruptOnDrop::new(self.query_control());
        let result = handle.await.map_err(|e| Error::BackgroundTaskFailed(e.to_string()));
        interrupt_on_drop.disarm();
        result?
    }

    /// Executes one or more SQL statements separated by semicolons.
    ///
    /// # Errors
    ///
    /// Returns an error if any statement fails to execute.
    pub async fn execute_batch<S>(
        &self,
        sql: S,
    ) -> Result<()>
    where
        S: Into<String> + Send,
    {
        let sql = sql.into();
        self.with_connection(move |conn| conn.execute_batch(&sql)).await
    }

    /// Prepares and executes a SQL statement, materializing the result.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB cannot prepare or execute the statement.
    pub async fn execute<S>(
        &self,
        sql: S,
    ) -> Result<ResultSet>
    where
        S: Into<String> + Send,
    {
        let sql = sql.into();
        self.with_connection(move |conn| conn.execute(&sql)?.materialize()).await
    }

    /// Prepares and executes a parameterized SQL statement, materializing the result.
    ///
    /// # Errors
    ///
    /// Returns an error if preparation, binding, or execution fails.
    pub async fn execute_with<S>(
        &self,
        sql: S,
        binds: Vec<DuckValue>,
    ) -> Result<ResultSet>
    where
        S: Into<String> + Send,
    {
        let sql = sql.into();
        self.with_connection(move |conn| {
            let mut owned = binds;
            let mut refs: Vec<&mut dyn AppendAble> =
                owned.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
            conn.execute_with(&sql, &mut refs)?.materialize()
        })
        .await
    }

    /// Prepares and executes `sql` **incrementally**, stepping the query one
    /// DuckDB task per `spawn_blocking` dispatch and yielding to the async runtime
    /// between tasks.
    ///
    /// This is the async form of [`CachedStatement::pending`]: rather than run the
    /// whole query inside one blocking call, each `execute_task` runs on its own
    /// blocking dispatch, so a long query neither monopolises a blocking thread nor
    /// blocks cancellation. Dropping the returned future stops stepping and, like
    /// every query path here, requests interruption of the in-flight task via the
    /// mutex-free [`QueryControl`].
    ///
    /// # Errors
    ///
    /// Returns an error if preparation or any execution task fails.
    pub async fn execute_pending<S>(
        &self,
        sql: S,
    ) -> Result<ResultSet>
    where
        S: Into<String> + Send,
    {
        use crate::raw::pending::{OwnedPending, PendingState};

        let sql = sql.into();

        // Prepare the statement and create the owned pending on one dispatch.
        let mut pending: OwnedPending = self
            .with_connection(move |conn| CachedStatement::prepare(conn.db(), &sql)?.into_pending())
            .await?;

        // Step one task per dispatch, yielding between tasks. `OwnedPending` is
        // `Send` and `'static`, so it moves in and out of each blocking task; the
        // connection mutex is held only for the duration of a single `execute_task`.
        loop {
            // Move the pending into the blocking task, step once, move it back out
            // alongside the resulting state.
            let (next, returned) = self
                .dispatch(move || {
                    let mut state = pending.execute_task();
                    // On a multi-threaded build, `execute_task` can keep reporting
                    // `NoTasksAvailable` while background workers run the query and never
                    // itself return `Ready`; consult the authoritative state so the loop
                    // terminates instead of yielding forever.
                    if matches!(state, PendingState::NoTasksAvailable) {
                        state = pending.check_state();
                    }
                    (state, pending)
                })
                .await?;
            pending = returned;

            match next {
                PendingState::Ready => break,
                PendingState::NotReady | PendingState::NoTasksAvailable => {
                    // Yield so the runtime can poll other tasks / observe a drop.
                    tokio::task::yield_now().await;
                },
                PendingState::Error => {
                    // Surface DuckDB's error; `execute` below would also, but this
                    // avoids a redundant dispatch.
                    let msg = pending.error();
                    return Err(Error::Engine(crate::error::EngineError::unavailable(Some(
                        msg.unwrap_or_else(|| "pending execution failed".to_owned()),
                    ))));
                },
                PendingState::Unknown(_) => {
                    // Treat an unrecognised state as "keep stepping" but yield first.
                    tokio::task::yield_now().await;
                },
            }
        }

        // Materialise the final result on a last dispatch, then own it off-thread.
        self.dispatch(move || pending.execute()?.materialize()).await?
    }

    /// Runs `f` on a blocking thread with cancel-on-drop interruption, but without
    /// taking the connection mutex — `f` owns whatever handles it needs. Used by
    /// [`execute_pending`](AsyncConnection::execute_pending) to move an
    /// `OwnedPending` in and out of each dispatch.
    async fn dispatch<F, T>(
        &self,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::task::spawn_blocking(f);
        let mut interrupt_on_drop = InterruptOnDrop::new(self.query_control());
        let out = handle.await.map_err(|e| Error::BackgroundTaskFailed(e.to_string()));
        interrupt_on_drop.disarm();
        out
    }

    /// Runs a closure against a bulk-insert [`Appender`] for `table`/`schema` on a
    /// blocking thread.
    ///
    /// The appender never leaves the closure — it holds raw FFI pointers, so it
    /// cannot be exposed as a standalone async handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist, the appender cannot be
    /// created, or the closure returns an error.
    pub async fn with_appender<F, T>(
        &self,
        table: impl Into<String> + Send,
        schema: impl Into<String> + Send,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut Appender) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let table = table.into();
        let schema = schema.into();
        self.with_connection(move |conn| {
            let mut appender = conn.appender(&table, &schema)?;
            f(&mut appender)
        })
        .await
    }

    /// Reads the progress of the query currently running on this connection.
    ///
    /// Returns `None` if no query is running. Non-blocking — reads through the
    /// mutex-free control source.
    #[must_use]
    pub fn progress(&self) -> Option<crate::raw::connection::QueryProgress> {
        self.query_control().progress()
    }
}

/// Requests interruption of the running query when dropped, unless disarmed.
///
/// Used to make the async query futures cancel-on-drop: if the caller drops the
/// future (or it is cancelled) before the blocking task completes, the guard
/// signals DuckDB to interrupt the native query. On normal completion the guard
/// is [`disarm`](InterruptOnDrop::disarm)ed so no interrupt is sent.
///
/// Generation scoping in [`QueryControl`] makes a late interrupt safe: if the
/// query already finished, the control is stale and `interrupt` is a no-op, so a
/// subsequent query on the same connection is never hit.
struct InterruptOnDrop {
    control: QueryControl,
    armed: bool,
}

impl InterruptOnDrop {
    fn new(control: QueryControl) -> InterruptOnDrop {
        InterruptOnDrop { control, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.control.interrupt();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(_: T) {}
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_clone<T: Clone>() {}

    #[test]
    fn async_connection_is_send_sync_clone() {
        assert_send_sync::<AsyncConnection>();
        assert_clone::<AsyncConnection>();
    }

    #[tokio::test]
    async fn execute_future_is_send() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        assert_send(conn.execute("SELECT 1"));
    }

    #[tokio::test]
    async fn execute_batch_then_execute() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1)").await.unwrap();
        let result = conn.execute("SELECT id FROM t").await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_pending_steps_a_query_to_completion() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1), (2), (3)").await.unwrap();

        // Incremental stepping yields the same result as one-shot execute().
        let set = conn.execute_pending("SELECT count(*) AS n FROM t").await.unwrap();
        match set.rows()[0].get("n").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 3),
            other => panic!("expected BigInt(3), got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_pending_reports_a_prepare_error() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        // A reference to a missing table fails while preparing the statement (the
        // catalog lookup happens at prepare, before pending), so the pending path
        // must surface that error — a `DuckDBFailure` from the prepare adapter —
        // rather than hang. (Engine-typed errors come from the *execution* path;
        // prepare failures keep the DuckDBFailure shape.)
        let err = conn.execute_pending("SELECT * FROM no_such_table").await.unwrap_err();
        assert!(
            matches!(err, Error::DuckDBFailure(..) | Error::Engine(_)),
            "expected a prepare/engine error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_with_owned_binds() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1), (2), (3)").await.unwrap();
        let result = conn
            .execute_with("SELECT id FROM t WHERE id = $1", vec![DuckValue::Int(2)])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn error_variant_survives_boundary() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        let err = conn.execute_batch("NOT VALID SQL").await.unwrap_err();
        // A malformed statement is a typed engine error (parser/syntax) that must
        // survive being moved out of the blocking task and across the await point.
        assert!(matches!(err, Error::Engine(_)), "unexpected error variant: {err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_queries_on_cloned_handles() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        let mut handles = Vec::new();
        for i in 0..8 {
            let c = conn.clone();
            handles.push(tokio::spawn(async move {
                c.execute_batch(format!("INSERT INTO t VALUES ({i})")).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let result = conn.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 8),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_appender_bulk_insert() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        conn.with_appender("t", "main", |appender| {
            for i in 0..100i32 {
                appender.append(&mut DuckValue::Int(i))?;
            }
            Ok(())
        })
        .await
        .unwrap();
        let result = conn.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 100),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn interrupt_is_a_noop_when_idle_and_control_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<QueryControl>();

        let conn = AsyncConnection::open_in_memory().await.unwrap();
        // A fresh control targets the *next* query to run, so it is active and its
        // interrupt signals DuckDB (which harmlessly no-ops with no query running).
        // After a query completes, that control goes stale.
        let control = conn.query_control();
        assert!(control.is_active(), "a fresh control targets the next query");
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        assert!(!control.is_active(), "control is stale once a query has run");
        assert!(!control.interrupt(), "stale interrupt is a no-op");
    }

    /// The core guarantee: control (interrupt/progress) never waits on the
    /// mutex a running native query holds. A query runs on one task while the main
    /// task calls `interrupt()`/`progress()` — those must return *promptly*, not
    /// block until the query releases the connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn control_does_not_block_on_the_running_query() {
        use std::time::Duration;

        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();

        // Run a query that takes a little while on a background task holding the
        // connection mutex.
        let runner = conn.clone();
        let query = tokio::spawn(async move {
            runner
                .execute_batch(
                    "CREATE TABLE big AS \
                     SELECT t1.range AS a FROM range(200000) t1, range(200) t2",
                )
                .await
        });

        // Give the query time to acquire the mutex and start.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Control calls must return promptly even though the mutex is held. If they
        // took the mutex, this would block until the query finished.
        let control_calls = tokio::time::timeout(Duration::from_secs(5), async {
            let _ = conn.interrupt();
            let _ = conn.progress();
        });
        control_calls.await.expect("interrupt/progress must not block on the running query");

        // Let the query task finish (interrupted or completed) so the connection is
        // released; the test's point is already proven above.
        let _ = query.await.unwrap();
    }

    /// Dropping a query future does not wedge the connection: a subsequent query on
    /// the same `AsyncConnection` still succeeds. The dropped future requests
    /// interruption; whether DuckDB stops the query early or it completes on its
    /// own, the mutex is released and the connection recovers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_query_future_leaves_connection_usable() {
        use std::time::Duration;

        let conn = AsyncConnection::open_in_memory().await.unwrap();

        // A quick query. We drop its future under a 1ms timeout: the future may or
        // may not have finished, but either way the blocking task completes on its
        // own and releases the connection. This exercises the drop path (which arms
        // interrupt-on-drop) without depending on interrupt latency.
        let work = conn.execute_batch("CREATE TABLE small AS SELECT range AS a FROM range(1000)");
        let _ = tokio::time::timeout(Duration::from_millis(1), work).await;

        // The connection recovers within a generous bound.
        let followup =
            tokio::time::timeout(Duration::from_secs(30), conn.execute("SELECT 42 AS v"));
        let result = followup.await.expect("connection must recover within 30s").unwrap();
        match result.rows()[0].get("v").unwrap() {
            DuckValue::Int(n) => assert_eq!(*n, 42),
            other => panic!("expected Int(42), got {other:?}"),
        }
    }

    /// A control minted before a query can observe its progress while it runs, and
    /// a stale control from a finished query reports nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_control_does_not_observe_a_later_query() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();

        // Mint a control, run a query to completion so the control goes stale.
        let stale = conn.query_control();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();

        assert!(!stale.is_active(), "control is stale after its query completed");
        assert!(!stale.interrupt(), "stale interrupt is a no-op");
        assert!(stale.progress().is_none(), "stale control observes no later query");

        // The connection still works normally.
        let result = conn.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 1),
            other => panic!("expected BigInt(1), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn with_connection_transaction_rollback() {
        let conn = AsyncConnection::open_in_memory().await.unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        conn.with_connection(|c| {
            c.execute_batch("BEGIN")?;
            c.execute_batch("INSERT INTO t VALUES (1)")?;
            c.execute_batch("ROLLBACK")?;
            Ok(())
        })
        .await
        .unwrap();
        let result = conn.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 0),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }
}
