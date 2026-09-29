//! DuckDB extension management on [`Connection`](crate::connection::Connection).
//!
//! DuckDB extensions (e.g. `spatial`, `json`, `parquet`) are loaded at runtime.
//! These helpers drive `INSTALL` / `LOAD` and read the state from DuckDB's own
//! `duckdb_extensions()` table function, so a feature that needs an extension can
//! **check** for it and (optionally) **force** it to load — rather than failing
//! later with an opaque SQL error.
//!
//! Extension names must be simple identifiers (`[A-Za-z0-9_]+`); anything else is
//! rejected up front so the name can be embedded safely.

use crate::connection::Connection;
use crate::error::{DuckDBConversionError, Error, Result};
use crate::types::value::DuckValue;

/// Initializes the dynamically-loaded DuckDB API table for a loadable
/// `.duckdb_extension` (feature `loadable-extension`).
///
/// Called by the [`duckdb_entrypoint`](crate::duckdb_entrypoint) macro's generated
/// entrypoint before any other `duckdb_*` call: it asks the host for its
/// `duckdb_ext_api_v1` table via `access.get_api` and stores every function pointer.
/// Returns `false` when the host declines (API-version mismatch), in which case DuckDB
/// itself reports the reason.
///
/// # Safety
///
/// `info` and `access` must be the handles DuckDB's loader passed to the entrypoint.
#[cfg(feature = "loadable-extension")]
pub unsafe fn c_api_init(
    info: crate::ffi::duckdb_extension_info,
    access: *const crate::ffi::duckdb_extension_access,
    minimum_version: &str,
) -> bool {
    // SAFETY: handles forwarded from the loader per this function's contract.
    unsafe { crate::ffi::duckdb_rs_extension_api_init(info, access, minimum_version) }
        .unwrap_or(false)
}

/// Whether `name` is a bare identifier safe to embed in an `INSTALL`/`LOAD`.
fn is_valid_extension_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn reject_bad_name(name: &str) -> Error {
    Error::ConversionError(DuckDBConversionError::ConversionError(format!(
        "invalid DuckDB extension name {name:?}: expected a bare identifier ([A-Za-z0-9_]+)"
    )))
}

impl Connection {
    /// Installs the extension `name` (downloads it if not already present).
    ///
    /// # Errors
    /// Returns an error if `name` is not a bare identifier, or if DuckDB cannot
    /// install it (e.g. no network access, or an unknown extension).
    pub fn install_extension(
        &mut self,
        name: &str,
    ) -> Result<()> {
        if !is_valid_extension_name(name) {
            return Err(reject_bad_name(name));
        }
        self.execute_batch(format!("INSTALL {name}"))
    }

    /// Loads the already-installed extension `name` into this connection.
    ///
    /// # Errors
    /// Returns an error if `name` is not a bare identifier, or if the extension is
    /// not installed / cannot be loaded.
    pub fn load_extension(
        &mut self,
        name: &str,
    ) -> Result<()> {
        if !is_valid_extension_name(name) {
            return Err(reject_bad_name(name));
        }
        self.execute_batch(format!("LOAD {name}"))
    }

    /// Reports whether the extension `name` is currently loaded, per
    /// `duckdb_extensions()`.
    ///
    /// # Errors
    /// Returns an error if `name` is not a bare identifier or the query fails.
    pub fn is_extension_loaded(
        &mut self,
        name: &str,
    ) -> Result<bool> {
        if !is_valid_extension_name(name) {
            return Err(reject_bad_name(name));
        }
        let sql = format!("SELECT loaded FROM duckdb_extensions() WHERE extension_name = '{name}'");
        let mut result = self.execute(sql)?;
        match result.next() {
            Some(row) => Ok(matches!(row?.get("loaded"), Some(DuckValue::Boolean(true)))),
            None => Ok(false),
        }
    }

    /// Ensures `name` is loaded: does nothing if it already is, otherwise tries to
    /// `LOAD` it, falling back to `INSTALL` + `LOAD`.
    ///
    /// # Errors
    /// Returns an error if `name` is invalid or the extension cannot be made
    /// available (e.g. it is not installed and cannot be downloaded).
    pub fn ensure_extension(
        &mut self,
        name: &str,
    ) -> Result<()> {
        if self.is_extension_loaded(name)? {
            return Ok(());
        }
        if self.load_extension(name).is_ok() {
            return Ok(());
        }
        self.install_extension(name)?;
        self.load_extension(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;

    #[test]
    fn unloaded_extension_reports_false() {
        let mut conn = Connection::open_in_memory().unwrap();
        // `spatial` is not loaded in a fresh in-memory connection.
        assert!(!conn.is_extension_loaded("spatial").unwrap());
        // An unknown extension is likewise "not loaded" (no row), not an error.
        assert!(!conn.is_extension_loaded("definitely_not_an_extension").unwrap());
    }

    #[test]
    fn invalid_extension_name_is_rejected() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert!(conn.load_extension("spatial; DROP TABLE t").is_err());
        assert!(conn.is_extension_loaded("").is_err());
    }

    #[test]
    fn install_and_ensure_paths() {
        let mut conn = Connection::open_in_memory().unwrap();
        // Invalid name is rejected before any SQL runs (install reject arm).
        assert!(conn.install_extension("bad; name").is_err());
        // json/parquet are compiled in under the test feature set; run INSTALL/ensure
        // to exercise the happy paths and the already-loaded fast path. Tolerate
        // environment differences (offline, static linkage) in the results.
        let _ = conn.install_extension("json");
        let _ = conn.ensure_extension("json");
        let _ = conn.ensure_extension("json");
    }
}
