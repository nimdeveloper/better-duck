//! Transaction control for [`Connection`](crate::connection::Connection).
//!
//! DuckDB has no dedicated transaction entry point in its C API, so these helpers
//! drive transactions with plain SQL (`BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK`)
//! through [`Connection::execute_batch`](crate::connection::Connection::execute_batch).
//! Two styles are offered:
//!
//! - [`Connection::transaction`](crate::connection::Connection::transaction) — a closure
//!   scope that commits on `Ok` and rolls back on `Err` (or panic, via the guard's `Drop`).
//!   This is the recommended API.
//! - [`Connection::begin_transaction`](crate::connection::Connection::begin_transaction) — a
//!   RAII [`Transaction`] guard that derefs to the connection and rolls back on drop unless
//!   [`Transaction::commit`] was called.
//!
//! DuckDB does **not** support `SAVEPOINT` / `RELEASE` / `ROLLBACK TO` (only
//! `BEGIN`/`COMMIT`/`ROLLBACK`/`ABORT`), so no savepoint API is exposed here.

use std::ops::{Deref, DerefMut};

use crate::connection::Connection;
use crate::error::Result;

impl Connection {
    /// Issues `BEGIN TRANSACTION`.
    ///
    /// # Errors
    /// Returns an error if the statement fails (e.g. a transaction is already open).
    pub fn begin(&mut self) -> Result<()> {
        self.execute_batch("BEGIN TRANSACTION")
    }

    /// Issues `COMMIT`.
    ///
    /// # Errors
    /// Returns an error if there is no open transaction or the commit fails.
    pub fn commit(&mut self) -> Result<()> {
        self.execute_batch("COMMIT")
    }

    /// Issues `ROLLBACK`.
    ///
    /// # Errors
    /// Returns an error if there is no open transaction or the rollback fails.
    pub fn rollback(&mut self) -> Result<()> {
        self.execute_batch("ROLLBACK")
    }

    /// Runs `f` inside a transaction: `BEGIN`, then `COMMIT` if `f` returns `Ok`,
    /// or `ROLLBACK` if it returns `Err`.
    ///
    /// # Errors
    /// Returns the closure's error (after rolling back) or any transaction-control
    /// error from `BEGIN`/`COMMIT`.
    pub fn transaction<T, F>(
        &mut self,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T>,
    {
        self.begin()?;
        match f(self) {
            Ok(value) => {
                self.commit()?;
                Ok(value)
            },
            Err(error) => {
                // Preserve the original error; a rollback failure here would mask it.
                let _ = self.rollback();
                Err(error)
            },
        }
    }

    /// Begins a transaction and returns a RAII [`Transaction`] guard that derefs to
    /// this connection and rolls back on drop unless [`Transaction::commit`] is called.
    ///
    /// # Errors
    /// Returns an error if `BEGIN TRANSACTION` fails.
    pub fn begin_transaction(&mut self) -> Result<Transaction<'_>> {
        self.begin()?;
        Ok(Transaction { conn: self, completed: false })
    }
}

/// An open transaction bound to a [`Connection`].
///
/// Derefs to the underlying [`Connection`], so queries run through the guard while
/// it is alive. On drop the transaction is rolled back unless
/// [`commit`](Transaction::commit) (or [`rollback`](Transaction::rollback)) already
/// completed it — this is what makes the `?`-operator early-return path safe.
pub struct Transaction<'c> {
    conn: &'c mut Connection,
    completed: bool,
}

impl Transaction<'_> {
    /// Commits the transaction.
    ///
    /// # Errors
    /// Returns an error if `COMMIT` fails; the guard still counts as completed so
    /// `Drop` will not additionally roll back.
    pub fn commit(mut self) -> Result<()> {
        self.completed = true;
        self.conn.commit()
    }

    /// Rolls the transaction back explicitly.
    ///
    /// # Errors
    /// Returns an error if `ROLLBACK` fails.
    pub fn rollback(mut self) -> Result<()> {
        self.completed = true;
        self.conn.rollback()
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl DerefMut for Transaction<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            // Best-effort rollback on early return / panic; nothing to propagate from Drop.
            let _ = self.conn.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;
    use crate::error::Error;
    use crate::types::value::DuckValue;

    fn count(conn: &mut Connection) -> i64 {
        let mut rows = conn.execute("SELECT count(*) AS n FROM t").unwrap();
        match rows.next().unwrap().unwrap().get("n") {
            Some(DuckValue::BigInt(n)) => *n,
            other => panic!("unexpected count value: {other:?}"),
        }
    }

    #[test]
    fn transaction_commits_on_ok_and_rolls_back_on_err() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER)").unwrap();

        conn.transaction(|c| c.execute_batch("INSERT INTO t VALUES (1)")).unwrap();

        let outcome = conn.transaction(|c| {
            c.execute_batch("INSERT INTO t VALUES (2)")?;
            Err::<(), _>(Error::InvalidQuery)
        });
        assert!(outcome.is_err());

        assert_eq!(count(&mut conn), 1, "only the committed row survives");
    }

    #[test]
    fn guard_rolls_back_when_dropped_without_commit() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER)").unwrap();
        {
            let mut tx = conn.begin_transaction().unwrap();
            tx.execute_batch("INSERT INTO t VALUES (1)").unwrap();
            // dropped here without commit -> rollback
        }
        assert_eq!(count(&mut conn), 0);
    }

    #[test]
    fn guard_commit_persists() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER)").unwrap();
        let mut tx = conn.begin_transaction().unwrap();
        tx.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        tx.commit().unwrap();
        assert_eq!(count(&mut conn), 1);
    }
}
