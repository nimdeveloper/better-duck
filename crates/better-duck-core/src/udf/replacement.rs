//! DuckDB replacement scans: automatically rewrite an unresolved table
//! reference into a table function call — e.g. routing
//! `SELECT * FROM 'data.parquet'` to `read_parquet('data.parquet')` by file
//! extension, without the caller writing the function call explicitly.
//!
//! # Experimental
//!
//! No comparable safe-Rust design exists in the reference `duckdb` crate to
//! model this on — this is a from-scratch design. It deliberately restricts
//! what a replacement scan can do: it may only rewrite the unresolved name
//! into a table function call with literal parameters
//! ([`ReplacementScanInfo::set_function_name`]/[`add_parameter`](ReplacementScanInfo::add_parameter)).
//! There is **no way to run SQL from inside the callback** — DuckDB invokes
//! it while resolving the very query being planned, on the same connection;
//! issuing another query from there would re-enter (and likely deadlock or
//! corrupt) that in-progress resolution. If a rewrite needs data that can
//! only come from a query, compute it before the query that triggers the
//! scan, not inside the callback.

use std::ffi::{c_void, CStr, CString};

use crate::{
    database::Database,
    error::{Error, Result},
    ffi::{
        duckdb_add_replacement_scan, duckdb_destroy_value, duckdb_replacement_scan_add_parameter,
        duckdb_replacement_scan_info, duckdb_replacement_scan_set_error,
        duckdb_replacement_scan_set_function_name,
    },
    types::DuckDialect,
};

use super::callback::{contain_callback, CallbackErrorSink};

/// A hook that rewrites an unresolved table reference into a table function
/// call. See the module docs for the v1 scope restriction (no SQL execution).
///
/// Stateless by design, matching [`VTab`](super::VTab)/[`VScalar`](super::VScalar):
/// `Self` is never constructed, only used as a marker type for
/// [`Database::register_replacement_scan`].
pub trait ReplacementScan {
    /// Called whenever DuckDB can't resolve `table_name` to an existing
    /// table, view, or CTE. Call [`info.set_function_name(...)`](ReplacementScanInfo::set_function_name)
    /// (optionally followed by [`info.add_parameter(...)`](ReplacementScanInfo::add_parameter))
    /// to rewrite it into a table function call; otherwise leave `info`
    /// untouched to decline, and DuckDB reports its normal "table not found"
    /// error.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message instead of
    /// DuckDB's default "table not found".
    fn replace(
        table_name: &str,
        info: &ReplacementScanInfo,
    ) -> super::UdfResult<()>;
}

/// An interface to redirect an unresolved table reference during a
/// replacement scan callback.
pub struct ReplacementScanInfo {
    ptr: duckdb_replacement_scan_info,
}

impl ReplacementScanInfo {
    fn from(ptr: duckdb_replacement_scan_info) -> Self {
        Self { ptr }
    }

    /// Rewrites the unresolved table reference into a call to the table
    /// function named `function_name`.
    ///
    /// Must be called for the rewrite to take effect at all — if
    /// [`ReplacementScan::replace`] returns `Ok(())` without ever calling
    /// this, DuckDB reports its normal "table not found" error, as if no
    /// replacement scan had run.
    ///
    /// # Errors
    ///
    /// Returns an error if `function_name` contains a NUL byte.
    pub fn set_function_name(
        &self,
        function_name: &str,
    ) -> Result<()> {
        let c_name = CString::new(function_name)?;
        // SAFETY: `self.ptr` is valid for the duration of the callback;
        // `c_name` is a valid, NUL-terminated C string for the duration of
        // this call.
        unsafe { duckdb_replacement_scan_set_function_name(self.ptr, c_name.as_ptr()) };
        Ok(())
    }

    /// Adds a literal parameter to the table function call being built, in
    /// the order added.
    ///
    /// There is deliberately no way to bind a *computed* parameter that would
    /// require running SQL — see the module docs.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` cannot be converted to a DuckDB value.
    pub fn add_parameter<T: DuckDialect>(
        &self,
        value: &T,
    ) -> Result<()> {
        let mut v = value.to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `self.ptr` is valid; `v` was just created above. This
        // function does not take ownership of `v` (matching
        // `duckdb_bind_value`/`duckdb_append_value`'s convention elsewhere in
        // this crate) — destroyed exactly once below.
        unsafe { duckdb_replacement_scan_add_parameter(self.ptr, v) };
        // SAFETY: `v` was created above and not yet destroyed.
        unsafe { duckdb_destroy_value(&mut v) };
        Ok(())
    }
}

impl CallbackErrorSink for ReplacementScanInfo {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is valid for the duration of the callback;
        // `error` is a valid, NUL-terminated C string.
        unsafe { duckdb_replacement_scan_set_error(self.ptr, error.as_ptr()) };
    }
}

/// The C trampoline installed via `duckdb_add_replacement_scan`.
///
/// See [`scalar_trampoline`](super::scalar) for why containment must be the
/// outermost thing here.
unsafe extern "C" fn replacement_scan_trampoline<T: ReplacementScan>(
    info: duckdb_replacement_scan_info,
    table_name: *const std::os::raw::c_char,
    _extra_data: *mut c_void,
) {
    let info = ReplacementScanInfo::from(info);
    contain_callback(&info, || {
        // SAFETY: DuckDB always passes a valid, NUL-terminated table name for
        // the duration of this call.
        let name = unsafe { CStr::from_ptr(table_name) }.to_string_lossy();
        T::replace(&name, &info)
    });
}

impl Database {
    /// Registers a replacement scan: `T::replace` is called whenever DuckDB
    /// can't resolve a table reference to an existing table, view, or CTE, on
    /// every connection to this database (replacement scans are scoped
    /// per-database, not per-connection).
    ///
    /// `T` carries no instance data — like [`VTab`](super::VTab)/
    /// [`VScalar`](super::VScalar), it's used purely as a marker type; the
    /// DuckDB C API itself has no failure mode for this registration.
    pub fn register_replacement_scan<T: ReplacementScan>(&self) {
        // SAFETY: `self.raw_db()` is a valid, open duckdb_database.
        // `replacement_scan_trampoline::<T>` matches
        // `duckdb_replacement_callback_t`'s signature. No extra data is
        // passed, so no delete callback is needed.
        unsafe {
            duckdb_add_replacement_scan(
                self.raw_db(),
                Some(replacement_scan_trampoline::<T>),
                std::ptr::null_mut(),
                None,
            )
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::DuckValue;

    /// Routes any unresolved table name ending in `.range` to `range(n)`,
    /// where `n` is parsed from the part before the extension — e.g.
    /// `'5.range'` becomes `range(5)`.
    struct RangeByExtension;

    impl ReplacementScan for RangeByExtension {
        fn replace(
            table_name: &str,
            info: &ReplacementScanInfo,
        ) -> super::super::UdfResult<()> {
            let Some(stem) = table_name.strip_suffix(".range") else {
                return Ok(()); // Decline: not our extension.
            };
            let n: i64 =
                stem.parse().map_err(|_| format!("'{table_name}' has a non-integer stem"))?;
            info.set_function_name("range")?;
            info.add_parameter(&n)?;
            Ok(())
        }
    }

    #[test]
    fn replacement_scan_rewrites_unresolved_table_reference() {
        let db = crate::database::Database::open_in_memory().unwrap();
        db.register_replacement_scan::<RangeByExtension>();
        let mut conn = db.connect().unwrap();

        let result = conn.execute("SELECT * FROM '5.range' ORDER BY range").unwrap();
        let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn replacement_scan_declining_leaves_the_normal_error() {
        let db = crate::database::Database::open_in_memory().unwrap();
        db.register_replacement_scan::<RangeByExtension>();
        let mut conn = db.connect().unwrap();

        let err = match conn.execute("SELECT * FROM does_not_exist") {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        assert!(err.to_string().to_lowercase().contains("does_not_exist"), "{err}");
    }

    #[test]
    fn replacement_scan_error_surfaces_as_query_error_and_connection_stays_usable() {
        let db = crate::database::Database::open_in_memory().unwrap();
        db.register_replacement_scan::<RangeByExtension>();
        let mut conn = db.connect().unwrap();

        let err = match conn.execute("SELECT * FROM 'not-a-number.range'") {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("non-integer stem"), "{err}");
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    }

    #[test]
    fn replacement_scan_applies_to_every_connection_on_the_shared_database() {
        let db = crate::database::Database::open_in_memory().unwrap();
        db.register_replacement_scan::<RangeByExtension>();
        let mut a = db.connect().unwrap();
        let mut b = db.connect().unwrap();

        for conn in [&mut a, &mut b] {
            let result = conn.execute("SELECT * FROM '3.range' ORDER BY range").unwrap();
            let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
            let got: Vec<i64> = rows
                .iter()
                .map(|r| match r.get("range").unwrap() {
                    DuckValue::BigInt(n) => *n,
                    other => panic!("expected BigInt, got {other:?}"),
                })
                .collect();
            assert_eq!(got, vec![0, 1, 2]);
        }
    }
}
