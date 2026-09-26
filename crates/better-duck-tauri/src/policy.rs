//! Security policy enforced by the engine (Phase 2 "scopes").
//!
//! The webview is untrusted and DuckDB SQL can reach the filesystem/network, so the app
//! author locks operations down at plugin init via `core::Builder`: read-only mode, a
//! connection-string allow-list, a path allow-list for import/export, and a coarse
//! risky-statement filter.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Statement leading-keywords rejected when `deny_risky_statements` is on. These reach
/// the filesystem/network, load code, or mutate session/catalog state out of band.
const RISKY_KEYWORDS: &[&str] =
    &["ATTACH", "DETACH", "INSTALL", "LOAD", "PRAGMA", "COPY", "SET", "CALL", "EXPORT", "IMPORT"];

/// Runtime security policy applied to every backend operation. Default is fully
/// permissive (unrestricted) — opt into each restriction explicitly.
#[derive(Debug, Default, Clone)]
pub struct Policy {
    /// Open databases read-only and reject write operations (`execute`/`import`/`append_rows`).
    pub read_only: bool,
    /// If `Some`, `load` only accepts these exact connection strings.
    pub allowed_connections: Option<Vec<String>>,
    /// If `Some`, import/export paths must resolve within one of these directories.
    pub allowed_paths: Option<Vec<PathBuf>>,
    /// Reject risky statements (see [`RISKY_KEYWORDS`]) passed to `select`/`execute`.
    pub deny_risky_statements: bool,
    /// Allow loading network-backed extensions (`httpfs`/`aws`/`azure`). Off by default —
    /// these enable SQL-driven network egress (SSRF / data exfiltration).
    pub allow_network: bool,
}

impl Policy {
    /// Rejects a connection string that is not on the allow-list (if one is set).
    pub(crate) fn check_connection(
        &self,
        conn_str: &str,
    ) -> Result<()> {
        match &self.allowed_connections {
            Some(list) if !list.iter().any(|c| c == conn_str) => {
                Err(Error::Denied(format!("connection not allowed: {conn_str:?}")))
            },
            _ => Ok(()),
        }
    }

    /// Rejects a write operation when in read-only mode.
    pub(crate) fn check_writable(&self) -> Result<()> {
        if self.read_only {
            Err(Error::Denied("read-only mode: write operation rejected".to_owned()))
        } else {
            Ok(())
        }
    }

    /// Rejects a file path outside the allow-list (if one is set).
    pub(crate) fn check_path(
        &self,
        path: &str,
    ) -> Result<()> {
        let Some(allowed) = &self.allowed_paths else {
            return Ok(());
        };
        let candidate = normalize(Path::new(path));
        if allowed.iter().any(|dir| candidate.starts_with(normalize(dir))) {
            Ok(())
        } else {
            Err(Error::Denied(format!("path not allowed: {path:?}")))
        }
    }

    /// Rejects a risky leading statement keyword when the filter is enabled.
    pub(crate) fn check_statement(
        &self,
        sql: &str,
    ) -> Result<()> {
        if !self.deny_risky_statements {
            return Ok(());
        }
        let keyword = leading_keyword(sql);
        if RISKY_KEYWORDS.iter().any(|risky| risky.eq_ignore_ascii_case(keyword)) {
            Err(Error::Denied(format!("statement kind not allowed: {keyword}")))
        } else {
            Ok(())
        }
    }

    /// Rejects a network-backed extension unless network access is allowed.
    pub(crate) fn check_extension(
        &self,
        name: &str,
    ) -> Result<()> {
        const NETWORK: &[&str] = &["httpfs", "aws", "azure"];
        if !self.allow_network && NETWORK.iter().any(|e| e.eq_ignore_ascii_case(name)) {
            Err(Error::Denied(format!("network extension {name:?} requires allow_network")))
        } else {
            Ok(())
        }
    }
}

/// The first bareword of a SQL statement (up to whitespace, `(`, or `;`).
fn leading_keyword(sql: &str) -> &str {
    sql.trim_start().split(|c: char| c.is_whitespace() || c == '(' || c == ';').next().unwrap_or("")
}

/// Canonicalizes a path for prefix comparison. Falls back to canonicalizing the parent
/// (so a not-yet-created export target inside an existing directory still resolves).
fn normalize(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) => {
            std::fs::canonicalize(parent).map_or_else(|_| path.to_path_buf(), |c| c.join(name))
        },
        _ => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_blocks_writes() {
        let policy = Policy { read_only: true, ..Policy::default() };
        assert!(policy.check_writable().is_err());
        assert!(Policy::default().check_writable().is_ok());
    }

    #[test]
    fn connection_allow_list() {
        let policy = Policy {
            allowed_connections: Some(vec!["duckdb:app.db".to_owned()]),
            ..Policy::default()
        };
        assert!(policy.check_connection("duckdb:app.db").is_ok());
        assert!(policy.check_connection("duckdb:secret.db").is_err());
        // No list ⇒ unrestricted.
        assert!(Policy::default().check_connection("anything").is_ok());
    }

    #[test]
    fn risky_statement_filter() {
        let policy = Policy { deny_risky_statements: true, ..Policy::default() };
        assert!(policy.check_statement("SELECT 1").is_ok());
        assert!(policy.check_statement("  attach 'x.db' AS y").is_err());
        assert!(policy.check_statement("PRAGMA database_list").is_err());
        assert!(policy.check_statement("COPY t TO 'x'").is_err());
        // Filter off ⇒ allowed.
        assert!(Policy::default().check_statement("ATTACH 'x'").is_ok());
    }

    #[test]
    fn leading_keyword_extraction() {
        assert_eq!(leading_keyword("  SELECT * FROM t"), "SELECT");
        assert_eq!(leading_keyword("attach('x')"), "attach");
        assert_eq!(leading_keyword(""), "");
    }
}
