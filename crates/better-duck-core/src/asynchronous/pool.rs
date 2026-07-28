use crate::{
    connection::Connection,
    error::{Error, Result},
    pool::{Pool, PoolState},
    result_set::ResultSet,
    types::{appendable::AppendAble, value::DuckValue},
};

/// An async facade over a [`Pool`].
///
/// [`with`](AsyncPool::with) is the primitive: it checks a connection out of the
/// pool and runs a closure against it on a blocking thread, releasing the
/// checkout when the closure returns. This is deliberately the only way to touch
/// a pooled connection — holding a long-lived pooled-connection guard across
/// `.await` points is the classic way to deadlock an async pool, since it pins a
/// pool slot for the entire suspension.
#[derive(Clone)]
pub struct AsyncPool {
    inner: Pool,
}

impl AsyncPool {
    /// Wraps an existing [`Pool`] for async use.
    pub fn new(pool: Pool) -> AsyncPool {
        AsyncPool { inner: pool }
    }

    /// Returns the underlying [`Pool`].
    pub fn pool(&self) -> &Pool {
        &self.inner
    }

    /// Returns the pool's current idle/active connection counts.
    pub async fn state(&self) -> PoolState {
        self.inner.state()
    }

    /// Checks a connection out of the pool and runs `f` against it on a blocking
    /// thread. The checkout is released when `f` returns.
    ///
    /// This is the correct place to run a transaction: a transaction must not
    /// span an `.await` point.
    ///
    /// # Errors
    ///
    /// Returns an error if checkout times out, or if the closure returns one.
    pub async fn with<F, T>(
        &self,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| Error::Pool(e.to_string()))?;
            f(&mut conn)
        })
        .await
        .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))?
    }

    /// Executes one or more SQL statements separated by semicolons on a pooled
    /// connection.
    ///
    /// # Errors
    ///
    /// Returns an error if checkout times out or any statement fails to execute.
    pub async fn execute_batch<S>(
        &self,
        sql: S,
    ) -> Result<()>
    where
        S: Into<String> + Send,
    {
        let sql = sql.into();
        self.with(move |conn| conn.execute_batch(&sql)).await
    }

    /// Prepares and executes a SQL statement on a pooled connection, materializing
    /// the result.
    ///
    /// # Errors
    ///
    /// Returns an error if checkout times out or the statement fails.
    pub async fn execute<S>(
        &self,
        sql: S,
    ) -> Result<ResultSet>
    where
        S: Into<String> + Send,
    {
        let sql = sql.into();
        self.with(move |conn| conn.execute(&sql)?.materialize()).await
    }

    /// Prepares and executes a parameterized SQL statement on a pooled connection,
    /// materializing the result.
    ///
    /// # Errors
    ///
    /// Returns an error if checkout times out, or preparation, binding, or
    /// execution fails.
    pub async fn execute_with<S>(
        &self,
        sql: S,
        binds: Vec<DuckValue>,
    ) -> Result<ResultSet>
    where
        S: Into<String> + Send,
    {
        let sql = sql.into();
        self.with(move |conn| {
            let mut owned = binds;
            let mut refs: Vec<&mut dyn AppendAble> =
                owned.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
            conn.execute_with(&sql, &mut refs)?.materialize()
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::DuckDbConnectionManager;

    fn make_pool(max_size: u32) -> AsyncPool {
        let manager = DuckDbConnectionManager::memory().unwrap();
        let pool = Pool::builder().max_size(max_size).build(manager).unwrap();
        AsyncPool::new(pool)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_pool_queries() {
        let pool = make_pool(4);
        pool.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..32 {
            let p = pool.clone();
            handles.push(tokio::spawn(async move {
                p.execute_with("INSERT INTO t VALUES ($1)", vec![DuckValue::Int(1)]).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let result = pool.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 32),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pool_with_runs_transaction() {
        let pool = make_pool(2);
        pool.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();
        pool.with(|conn| {
            conn.execute_batch("BEGIN")?;
            conn.execute_batch("INSERT INTO t VALUES (1)")?;
            conn.execute_batch("COMMIT")?;
            Ok(())
        })
        .await
        .unwrap();
        let result = pool.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            DuckValue::BigInt(n) => assert_eq!(*n, 1),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_does_not_block_the_executor() {
        let pool = make_pool(1);
        let slow = {
            let p = pool.clone();
            tokio::spawn(async move {
                p.with(|conn| {
                    conn.execute_batch("SELECT 1")?;
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    Ok(())
                })
                .await
                .unwrap();
            })
        };
        let start = tokio::time::Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(start.elapsed() < std::time::Duration::from_millis(150));
        slow.await.unwrap();
    }
}
