//! Crash-safe migration protocol (backup-in-place).
//!
//! Sits above DuckDB's own WAL recovery: because each migration commits in its own
//! transaction, a multi-migration batch that fails midway would leave earlier migrations
//! committed. Backing up the whole file first lets us restore the pre-batch state, and
//! also guards against file corruption / an interrupted process.
//!
//! Files beside `<db>`: `<db>.bak` (backup) and `<db>.migrate-journal` (journal). The
//! journal's presence at startup means "a migration was in flight and did not confirm
//! success" — see [`migrate_with_backup`].

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use better_duck_core::database::Database;

use crate::error::{Error, Result};
use crate::migration::{has_pending, run_pending, Migration};

/// Appends a suffix to a path's full filename (e.g. `app.db` → `app.db.bak`).
fn sidecar(
    db: &Path,
    suffix: &str,
) -> PathBuf {
    let mut name: OsString = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Runs pending core-inline migrations on an on-disk database with a backup/restore net.
pub(crate) fn migrate_with_backup(
    db_path: &Path,
    migrations: &[Migration],
) -> Result<()> {
    with_backup(
        db_path,
        || compute_pending(db_path, migrations),
        || run(db_path, migrations),
    )
}

/// The crash-safe backup skeleton, generic over the migration runner.
///
/// `has_pending` reports whether any migration remains; `run` applies them. Each opens and
/// closes its own connection. Recovers first if a prior run was interrupted, then (if
/// anything is pending) checkpoints, backs up, journals, migrates, and clears the backup —
/// deleting the journal before the backup so a crash in between only leaves a harmless
/// stale backup. Shared by the core inline engine and the Diesel-native engine.
pub(crate) fn with_backup(
    db_path: &Path,
    has_pending: impl FnOnce() -> Result<bool>,
    run: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let journal = sidecar(db_path, ".migrate-journal");
    let backup = sidecar(db_path, ".bak");

    // 1. A journal means the previous run was interrupted: restore before continuing.
    if journal.exists() {
        if backup.exists() {
            restore(db_path, &backup)?;
        } else {
            let _ = fs::remove_file(&journal); // stale journal, nothing to restore
        }
    }

    // 2. Nothing to do? Clear any leftover artifacts and return.
    if !has_pending()? {
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&backup);
        return Ok(());
    }

    // 3. Fold the WAL into the main file, back it up, then record intent.
    checkpoint(db_path)?;
    copy_synced(db_path, &backup)?;
    write_journal(&journal)?;

    // 4. Migrate. On failure, restore the pre-batch state and surface the error.
    match run() {
        Ok(()) => {
            let _ = fs::remove_file(&journal);
            let _ = fs::remove_file(&backup);
            Ok(())
        },
        Err(e) => {
            restore(db_path, &backup)?;
            Err(e)
        },
    }
}

/// Opens the database briefly to check for pending migrations, then closes it.
fn compute_pending(
    db_path: &Path,
    migrations: &[Migration],
) -> Result<bool> {
    let db = Database::open(db_path).map_err(|e| Error::Backend(e.to_string()))?;
    let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
    has_pending(&mut conn, migrations)
}

/// Applies pending migrations, then closes the database (folding the WAL on clean drop).
fn run(
    db_path: &Path,
    migrations: &[Migration],
) -> Result<()> {
    let db = Database::open(db_path).map_err(|e| Error::Backend(e.to_string()))?;
    let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
    run_pending(&mut conn, migrations)
}

/// Synchronizes the WAL into the main file so a file-level copy is self-contained.
fn checkpoint(db_path: &Path) -> Result<()> {
    let db = Database::open(db_path).map_err(|e| Error::Backend(e.to_string()))?;
    let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
    conn.execute_batch("CHECKPOINT").map_err(|e| Error::Backend(e.to_string()))
}

/// Copies `src` → `dst` and fsyncs the destination.
fn copy_synced(
    src: &Path,
    dst: &Path,
) -> Result<()> {
    fs::copy(src, dst).map_err(|e| Error::Backend(format!("backup copy failed: {e}")))?;
    if let Ok(file) = fs::File::open(dst) {
        let _ = file.sync_all();
    }
    Ok(())
}

/// Writes and fsyncs the journal (its mere presence is the signal; contents are advisory).
fn write_journal(journal: &Path) -> Result<()> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    fs::write(journal, format!("{{\"startedAtEpoch\":{secs}}}"))
        .map_err(|e| Error::Backend(format!("journal write failed: {e}")))?;
    if let Ok(file) = fs::File::open(journal) {
        let _ = file.sync_all();
    }
    Ok(())
}

/// Removes the live DB (+ WAL + temp dir) and atomically moves the backup into place.
fn restore(
    db_path: &Path,
    backup: &Path,
) -> Result<()> {
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(sidecar(db_path, ".wal"));
    let _ = fs::remove_dir_all(sidecar(db_path, ".tmp"));
    let staging = sidecar(db_path, ".restore-tmp");
    copy_synced(backup, &staging)?;
    fs::rename(&staging, db_path).map_err(|e| Error::Backend(format!("restore failed: {e}")))
}

#[cfg(test)]
mod tests {
    use better_duck_core::database::Database;

    use super::{sidecar, with_backup};
    use crate::error::Error;

    /// A migration run that fails partway is rolled back to the pre-batch state via the
    /// backup — the generic skeleton the Diesel-native engine uses (with `run` opening its
    /// own connection, exactly like the real `diesel_run`).
    #[test]
    fn with_backup_restores_on_failed_run() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("app.duckdb");
        {
            let db = Database::open(&db_path).unwrap();
            let mut conn = db.connect().unwrap();
            conn.execute_batch("CREATE TABLE base (id INTEGER); INSERT INTO base VALUES (1);")
                .unwrap();
        }

        let result = with_backup(
            &db_path,
            || Ok(true),
            || {
                let db = Database::open(&db_path).unwrap();
                let mut conn = db.connect().unwrap();
                conn.execute_batch("CREATE TABLE half (id INTEGER)").unwrap();
                Err(Error::Backend("boom".to_owned()))
            },
        );
        assert!(result.is_err(), "the failing run surfaces its error");

        let db = Database::open(&db_path).unwrap();
        let mut conn = db.connect().unwrap();
        assert!(conn.execute("SELECT id FROM base").is_ok(), "pre-batch table survives");
        assert!(conn.execute("SELECT id FROM half").is_err(), "half-applied table is rolled back");
    }

    /// With nothing pending, leftover backup/journal artifacts are cleared and no backup runs.
    #[test]
    fn with_backup_clears_artifacts_when_nothing_pending() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("app.duckdb");
        {
            let db = Database::open(&db_path).unwrap();
            let mut conn = db.connect().unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
        }
        std::fs::write(sidecar(&db_path, ".bak"), b"stale").unwrap();

        with_backup(&db_path, || Ok(false), || panic!("run must not be called")).unwrap();
        assert!(!sidecar(&db_path, ".bak").exists(), "stale backup cleared");
        assert!(!sidecar(&db_path, ".migrate-journal").exists());
    }
}
