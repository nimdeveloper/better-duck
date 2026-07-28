use std::{path::Path, sync::Arc};

use parking_lot::Mutex;

use crate::{
    connection::Connection,
    error::{Error, Result},
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
/// # Panics
///
/// These methods panic if called outside a Tokio runtime context, matching
/// `tokio::task::spawn_blocking`.
#[derive(Clone)]
pub struct AsyncConnection {
    inner: Arc<Mutex<Connection>>,
}

impl AsyncConnection {
    /// Wraps an existing [`Connection`] for async use.
    pub fn new(conn: Connection) -> AsyncConnection {
        AsyncConnection { inner: Arc::new(Mutex::new(conn)) }
    }

    /// Recovers the inner [`Connection`] if this is the last handle.
    ///
    /// Returns `self` unchanged (as `Err`) if other clones of this handle exist.
    pub fn try_into_inner(self) -> std::result::Result<Connection, AsyncConnection> {
        Arc::try_unwrap(self.inner)
            .map(Mutex::into_inner)
            .map_err(|inner| AsyncConnection { inner })
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
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock();
            f(&mut guard)
        })
        .await
        .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))?
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
        assert!(matches!(err, Error::DuckDBFailure(..)));
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
