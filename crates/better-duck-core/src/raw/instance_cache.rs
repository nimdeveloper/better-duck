//! RAII wrapper for `duckdb_instance_cache` — opt-in cache-backed database opening.
//!
//! An instance cache lets several `:memory:`-or-file opens of the *same* path share
//! one underlying database instance, instead of each open creating an independent
//! one. This is strictly opt-in: it never changes [`Database::open`]'s default
//! semantics. [`InstanceCache`] owns the cache handle (destroyed once on drop);
//! databases obtained from it are ordinary [`Database`]s that close independently.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_void, CStr};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use crate::{
    config::Config,
    database::Database,
    error::{Error, Result},
    ffi::{
        duckdb_create_instance_cache, duckdb_database, duckdb_destroy_instance_cache, duckdb_free,
        duckdb_get_or_create_from_cache, duckdb_instance_cache, DuckDBSuccess,
    },
    helpers::path::path_to_cstring,
    raw::connection::RawDatabase,
};

/// An owned `duckdb_instance_cache` (destroyed once on drop).
///
/// Deliberately `!Send + !Sync`: DuckDB gives no concurrency guarantee for a shared
/// instance cache, so a handle must stay on the thread that created it.
pub struct InstanceCache {
    ptr: duckdb_instance_cache,
    _not_thread_safe: std::marker::PhantomData<*const ()>,
}

impl InstanceCache {
    /// Creates a new, empty instance cache.
    #[must_use]
    pub fn new() -> InstanceCache {
        // SAFETY: always valid to call; returns a freshly allocated cache handle.
        let ptr = unsafe { duckdb_create_instance_cache() };
        InstanceCache { ptr, _not_thread_safe: std::marker::PhantomData }
    }

    /// Opens the database at `path` through this cache: if a database for `path`
    /// already exists in the cache it is returned (sharing the same instance),
    /// otherwise a new one is created and cached. `:memory:` and an empty path both
    /// mean an in-memory database.
    ///
    /// The returned [`Database`] closes independently; the cache keeps its own
    /// reference until dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains an interior NUL or DuckDB fails to open
    /// the database (its error message is surfaced).
    pub fn get_or_create<P: AsRef<Path>>(
        &self,
        path: P,
        config: Config,
    ) -> Result<Database> {
        let c_path = path_to_cstring(path.as_ref())?;
        let config = config.with("duckdb_api", "rust")?;
        let mut db: duckdb_database = ptr::null_mut();
        let mut c_err: *mut std::os::raw::c_char = ptr::null_mut();
        // SAFETY: `self.ptr` is a valid cache; `c_path` is a valid NUL-terminated
        // string; `db`/`c_err` are valid out-pointers. On error DuckDB allocates
        // `c_err`, which we free with `duckdb_free`.
        let state = unsafe {
            duckdb_get_or_create_from_cache(
                self.ptr,
                c_path.as_ptr(),
                &mut db,
                config.duckdb_config(),
                &mut c_err,
            )
        };
        if state != DuckDBSuccess {
            let msg = if c_err.is_null() {
                None
            } else {
                // SAFETY: `c_err` is a valid, non-null, NUL-terminated C string DuckDB
                // allocated on failure.
                let m = unsafe { CStr::from_ptr(c_err) }.to_string_lossy().into_owned();
                // SAFETY: `c_err` was allocated by DuckDB; free it once.
                unsafe { duckdb_free(c_err as *mut c_void) };
                Some(m)
            };
            return Err(Error::DuckDBFailure(crate::ffi::Error::new(state), msg));
        }
        // SAFETY: on success `db` is a valid open database handle we now own.
        let raw = unsafe { RawDatabase::new(db) }?;
        Ok(Database::from_raw(Arc::new(raw)))
    }
}

impl Default for InstanceCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InstanceCache {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null cache created by
            // `duckdb_create_instance_cache` and not yet destroyed; destroyed once.
            unsafe { duckdb_destroy_instance_cache(&mut self.ptr) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_shares_one_in_memory_instance_across_opens() {
        let cache = InstanceCache::new();
        // Two opens of the same named in-memory path share one instance.
        let a = cache.get_or_create(":memory:cachetest", Config::default()).unwrap();
        let b = cache.get_or_create(":memory:cachetest", Config::default()).unwrap();
        let mut ca = a.connect().unwrap();
        ca.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        ca.execute_batch("INSERT INTO t VALUES (7)").unwrap();
        // `b` observes `a`'s data because they share the cached instance.
        let mut cb = b.connect().unwrap();
        let mut rows = cb.execute("SELECT v FROM t").unwrap();
        assert_eq!(
            rows.next().unwrap().unwrap().get("v"),
            Some(&crate::types::value::DuckValue::Int(7))
        );
    }

    #[test]
    fn distinct_named_paths_are_independent() {
        let cache = InstanceCache::new();
        let a = cache.get_or_create(":memory:one", Config::default()).unwrap();
        let b = cache.get_or_create(":memory:two", Config::default()).unwrap();
        let mut ca = a.connect().unwrap();
        ca.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        // A different cached path does not see `a`'s table.
        let mut cb = b.connect().unwrap();
        assert!(cb.execute("SELECT v FROM t").is_err());
    }
}
