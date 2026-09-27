//! Inline migration engine for the `backend-core` path (the `tauri-plugin-sql`-style
//! model). Each migration runs inside its own transaction; a failure rolls that
//! migration back. Applied versions are tracked in `__better_duck_migrations`.
//!
//! The Diesel-backed engine (`embed_migrations!` + `MigrationHarness`) is layered on the
//! `backend-diesel` path in a later phase; this module is the core-backend equivalent.

use better_duck_core::connection::Connection;
use better_duck_core::types::appendable::AppendAble;
use better_duck_core::types::value::DuckValue;

use crate::error::{Error, Result};

/// Direction of a migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationKind {
    /// Applied when migrating up (the schema-forward direction).
    Up,
    /// Applied when reverting (the schema-back direction).
    Down,
}

/// A single migration step. Mirrors `tauri-plugin-sql`'s model so it is familiar.
#[derive(Debug, Clone)]
pub struct Migration {
    /// Unique, ordering version number.
    pub version: i64,
    /// Human-readable description.
    pub description: String,
    /// The SQL to run (may contain multiple `;`-separated statements).
    pub sql: String,
    /// Whether this step migrates up or down.
    pub kind: MigrationKind,
}

const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS __better_duck_migrations (\
     version BIGINT PRIMARY KEY, \
     description VARCHAR NOT NULL, \
     applied_at TIMESTAMP NOT NULL DEFAULT current_timestamp)";

/// Applies every pending `Up` migration in ascending version order, each inside its own
/// transaction. Already-applied versions are skipped; the whole call is idempotent.
pub(crate) fn run_pending(
    conn: &mut Connection,
    migrations: &[Migration],
) -> Result<()> {
    conn.execute_batch(CREATE_TABLE).map_err(|e| Error::Backend(e.to_string()))?;
    let applied = applied_versions(conn)?;

    let mut ups: Vec<&Migration> =
        migrations.iter().filter(|m| m.kind == MigrationKind::Up).collect();
    ups.sort_by_key(|m| m.version);

    for migration in ups {
        if applied.contains(&migration.version) {
            continue;
        }
        apply_one(conn, migration)?;
    }
    Ok(())
}

/// Reads the set of already-applied migration versions.
pub(crate) fn applied_versions(conn: &mut Connection) -> Result<Vec<i64>> {
    let result = conn
        .execute("SELECT version FROM __better_duck_migrations")
        .map_err(|e| Error::Backend(e.to_string()))?;
    let rows = result.materialize().map_err(|e| Error::Backend(e.to_string()))?;
    let mut versions = Vec::with_capacity(rows.len());
    for row in rows.rows() {
        if let Some(DuckValue::BigInt(v)) = row.get_idx(0) {
            versions.push(*v);
        }
    }
    Ok(versions)
}

/// Returns whether any `Up` migration has not yet been applied. Does not create the
/// bookkeeping table (read-only), so it is safe to call before taking a backup.
pub(crate) fn has_pending(
    conn: &mut Connection,
    migrations: &[Migration],
) -> Result<bool> {
    let exists = migrations_table_exists(conn)?;
    let applied = if exists { applied_versions(conn)? } else { Vec::new() };
    Ok(migrations
        .iter()
        .filter(|m| m.kind == MigrationKind::Up)
        .any(|m| !applied.contains(&m.version)))
}

/// Checks whether the `__better_duck_migrations` table exists (without creating it).
fn migrations_table_exists(conn: &mut Connection) -> Result<bool> {
    let result = conn
        .execute(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_name = '__better_duck_migrations' LIMIT 1",
        )
        .map_err(|e| Error::Backend(e.to_string()))?;
    let rows = result.materialize().map_err(|e| Error::Backend(e.to_string()))?;
    Ok(!rows.rows().is_empty())
}

/// Reverts the most recently applied migration by running its `Down` SQL and removing its
/// bookkeeping row, all in one transaction. Returns the reverted version, or `None` if
/// nothing was applied. Errors if the last-applied version has no `Down` migration.
pub(crate) fn revert_last(
    conn: &mut Connection,
    migrations: &[Migration],
) -> Result<Option<i64>> {
    if !migrations_table_exists(conn)? {
        return Ok(None);
    }
    let Some(version) = applied_versions(conn)?.into_iter().max() else {
        return Ok(None);
    };
    let down = migrations
        .iter()
        .find(|m| m.version == version && m.kind == MigrationKind::Down)
        .ok_or_else(|| Error::Backend(format!("no down migration for version {version}")))?;

    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| Error::Backend(e.to_string()))?;
    let outcome = (|| -> Result<()> {
        conn.execute_batch(&down.sql).map_err(|e| Error::Backend(e.to_string()))?;
        let mut bind = DuckValue::BigInt(version);
        conn.execute_with(
            "DELETE FROM __better_duck_migrations WHERE version = $1",
            &mut [&mut bind as &mut dyn AppendAble],
        )
        .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    })();

    match outcome {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|e| Error::Backend(e.to_string()))?;
            Ok(Some(version))
        },
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        },
    }
}

/// Runs one migration and records its version, all inside a single transaction.
fn apply_one(
    conn: &mut Connection,
    migration: &Migration,
) -> Result<()> {
    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| Error::Backend(e.to_string()))?;

    let outcome = (|| -> Result<()> {
        conn.execute_batch(&migration.sql).map_err(|e| Error::Backend(e.to_string()))?;
        let mut version = DuckValue::BigInt(migration.version);
        let mut description = DuckValue::Text(migration.description.clone());
        conn.execute_with(
            "INSERT INTO __better_duck_migrations (version, description) VALUES ($1, $2)",
            &mut [&mut version, &mut description],
        )
        .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    })();

    match outcome {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|e| Error::Backend(e.to_string()))?;
            Ok(())
        },
        Err(e) => {
            // Best-effort rollback; surface the original migration error.
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn up(
        version: i64,
        sql: &str,
    ) -> Migration {
        Migration {
            version,
            description: format!("m{version}"),
            sql: sql.to_owned(),
            kind: MigrationKind::Up,
        }
    }

    #[test]
    fn runs_pending_and_is_idempotent() {
        let mut conn = Connection::open_in_memory().unwrap();
        let migs = vec![up(1, "CREATE TABLE a (id INTEGER)"), up(2, "CREATE TABLE b (id INTEGER)")];
        run_pending(&mut conn, &migs).unwrap();
        // A second run applies nothing new and must not error (tables already exist).
        run_pending(&mut conn, &migs).unwrap();

        let applied = applied_versions(&mut conn).unwrap();
        assert_eq!(applied.len(), 2);
        conn.execute_batch("INSERT INTO a VALUES (1); INSERT INTO b VALUES (2)").unwrap();
    }

    #[test]
    fn failing_migration_rolls_back_and_is_not_recorded() {
        let mut conn = Connection::open_in_memory().unwrap();
        let migs = vec![up(1, "CREATE TABLE ok (id INTEGER); THIS IS NOT VALID SQL")];
        assert!(run_pending(&mut conn, &migs).is_err());
        // The partial CREATE TABLE must have rolled back, and version 1 not recorded.
        assert!(conn.execute("SELECT * FROM ok").is_err());
        assert!(applied_versions(&mut conn).unwrap().is_empty());
    }
}
