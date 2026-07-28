use std::path::Path;

use crate::{
    asynchronous::connection::AsyncConnection,
    database::Database,
    error::{Error, Result},
};

/// An async facade over a [`Database`].
///
/// `open`/`open_in_memory` and `connect` dispatch to `tokio::task::spawn_blocking`.
#[derive(Clone, Debug)]
pub struct AsyncDatabase {
    inner: Database,
}

impl AsyncDatabase {
    /// Wraps an existing [`Database`] for async use.
    pub fn from_database(db: Database) -> AsyncDatabase {
        AsyncDatabase { inner: db }
    }

    /// Returns the underlying [`Database`] handle.
    pub fn database(&self) -> &Database {
        &self.inner
    }

    /// Opens a database at the given file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub async fn open<P>(path: P) -> Result<AsyncDatabase>
    where
        P: AsRef<Path> + Send + 'static,
    {
        let db = tokio::task::spawn_blocking(move || Database::open(path))
            .await
            .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))??;
        Ok(AsyncDatabase::from_database(db))
    }

    /// Opens an in-memory database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub async fn open_in_memory() -> Result<AsyncDatabase> {
        let db = tokio::task::spawn_blocking(Database::open_in_memory)
            .await
            .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))??;
        Ok(AsyncDatabase::from_database(db))
    }

    /// Opens a new connection to this database.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub async fn connect(&self) -> Result<AsyncConnection> {
        let db = self.inner.clone();
        let conn = tokio::task::spawn_blocking(move || db.connect())
            .await
            .map_err(|e| Error::BackgroundTaskFailed(e.to_string()))??;
        Ok(AsyncConnection::new(conn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn async_database_parallel_connections() {
        let db = AsyncDatabase::open_in_memory().await.unwrap();
        let a = db.connect().await.unwrap();
        a.execute_batch("CREATE TABLE t (id INTEGER)").await.unwrap();

        let mut handles = Vec::new();
        for i in 0..8 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let conn = db.connect().await.unwrap();
                conn.execute_batch(format!("INSERT INTO t VALUES ({i})")).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let result = a.execute("SELECT count(*) AS c FROM t").await.unwrap();
        match result.rows()[0].get("c").unwrap() {
            crate::types::value::DuckValue::BigInt(n) => assert_eq!(*n, 8),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }
}
