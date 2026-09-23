//! Parse a multi-statement SQL string into individually-preparable statements.
//!
//! DuckDB does the parsing: `duckdb_extract_statements` splits the query into N
//! statements. There is deliberately **no** SQL-splitting heuristic on the Rust
//! side — semicolons inside string literals, comments, or dollar-quoting make
//! naive splitting wrong, and DuckDB's parser is the only correct source.

use std::{
    ffi::{CStr, CString},
    ptr,
    sync::Arc,
};

use crate::{
    error::{EngineError, Error, Result},
    ffi::{
        duckdb_destroy_extracted, duckdb_extract_statements, duckdb_extract_statements_error,
        duckdb_extracted_statements, duckdb_prepare_extracted_statement, duckdb_prepared_statement,
        DuckDBError, Error as FFIError,
    },
    helpers::duck_result::result_from_duckdb_prepare,
    raw::{
        connection::{ConnectionInner, RawConnection},
        statement::CachedStatement,
    },
};

/// A parsed batch of SQL statements, each preparable on demand.
///
/// Produced by [`Connection::extract_statements`](crate::connection::Connection::extract_statements).
/// Owns the
/// `duckdb_extracted_statements` handle and destroys it in [`Drop`]; it retains
/// the connection it parsed against (`Arc<ConnectionInner>`) so a statement
/// prepared from it cannot outlive that connection — the same ownership rule as
/// [`CachedStatement`].
pub struct ExtractedStatements {
    /// Keeps the parsing connection alive for at least as long as this batch and
    /// any statement prepared from it.
    connection: Arc<ConnectionInner>,
    /// Owned handle; destroyed exactly once in `Drop`.
    extracted: duckdb_extracted_statements,
    /// Number of statements DuckDB parsed out of the query.
    count: u64,
}

impl ExtractedStatements {
    /// Extracts all statements from `sql` on `conn`.
    ///
    /// The SQL text is copied by DuckDB, so it need not outlive this call.
    ///
    /// # Errors
    ///
    /// [`Error::NulError`] if `sql` has an interior nul; otherwise, on a parse
    /// failure, [`Error::Engine`] carrying DuckDB's extract-error message (an
    /// [`EngineError::unavailable`] — `duckdb_extract_statements` exposes only a
    /// message, no typed classification).
    pub(crate) fn extract(
        conn: &RawConnection,
        sql: &str,
    ) -> Result<ExtractedStatements> {
        let c_sql = CString::new(sql)?;
        let mut extracted: duckdb_extracted_statements = ptr::null_mut();
        // SAFETY: `conn`'s handle is a valid open connection; `c_sql` is a valid
        // null-terminated string that outlives the call; `&mut extracted` is a valid
        // output pointer. DuckDB requires the handle to be destroyed regardless of
        // the returned count, which `Drop` (success) or the error path (below) does.
        let count =
            unsafe { duckdb_extract_statements(conn.handle(), c_sql.as_ptr(), &mut extracted) };

        if count == 0 {
            // Parse failed: copy the error message out (it is freed by
            // `duckdb_destroy_extracted`, so it must be copied first), then destroy.
            // SAFETY: `extracted` is the (possibly-error) handle DuckDB just produced;
            // `duckdb_extract_statements_error` returns a borrowed C string valid until
            // the handle is destroyed, which we do immediately after copying.
            let message = unsafe {
                let raw = duckdb_extract_statements_error(extracted);
                let msg =
                    (!raw.is_null()).then(|| CStr::from_ptr(raw).to_string_lossy().into_owned());
                duckdb_destroy_extracted(&mut extracted);
                msg
            };
            return Err(Error::Engine(EngineError::unavailable(Some(
                message.unwrap_or_else(|| "failed to extract statements".to_owned()),
            ))));
        }

        Ok(ExtractedStatements { connection: Arc::clone(conn.inner()), extracted, count })
    }

    /// The number of statements parsed from the query.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Returns `true` if no statements were parsed.
    ///
    /// Always `false` in practice — a zero-statement extract is reported as an
    /// error at extraction time — but provided so the
    /// type satisfies the usual `len`/`is_empty` pairing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Prepares the extracted statement at `index` (0-based), reusing the ordinary
    /// [`CachedStatement`] wrapper.
    ///
    /// # Errors
    ///
    /// [`Error::DuckDBFailure`] if `index` is out of range, or if DuckDB fails to
    /// prepare the statement.
    pub fn prepare(
        &self,
        index: u64,
    ) -> Result<CachedStatement> {
        if index >= self.count {
            return Err(Error::DuckDBFailure(
                FFIError::new(DuckDBError),
                Some(format!(
                    "extracted statement index {index} out of range (parsed {} statement(s))",
                    self.count
                )),
            ));
        }

        let mut stmt: duckdb_prepared_statement = ptr::null_mut();
        // SAFETY: `self.connection`'s handle is valid and kept alive by the `Arc`;
        // `self.extracted` is a live handle owned by `self`; `index` is in range
        // (checked above); `&mut stmt` is a valid output pointer. DuckDB requires the
        // prepared statement to be destroyed regardless of outcome — `CachedStatement`
        // does that in its own `Drop`, and `result_from_duckdb_prepare` destroys it on
        // the failure path.
        let rc = unsafe {
            duckdb_prepare_extracted_statement(
                self.connection.handle(),
                self.extracted,
                index,
                &mut stmt,
            )
        };
        result_from_duckdb_prepare(rc, stmt)?;
        // Extracted statements have no standalone per-statement SQL text; the cache
        // key is unused for this path, so an empty label is fine.
        Ok(CachedStatement::from_prepared(Arc::clone(&self.connection), stmt, Box::from("")))
    }
}

impl std::fmt::Debug for ExtractedStatements {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("ExtractedStatements").field("count", &self.count).finish_non_exhaustive()
    }
}

impl Drop for ExtractedStatements {
    fn drop(&mut self) {
        if self.extracted.is_null() {
            return;
        }
        // SAFETY: `self.extracted` is a valid, non-null handle owned exclusively by
        // this value (null-guarded above); `duckdb_destroy_extracted` frees it and is
        // called exactly once.
        unsafe { duckdb_destroy_extracted(&mut self.extracted) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, helpers::path::path_to_cstring, types::value::DuckValue};

    fn conn() -> RawConnection {
        let path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&path, config).unwrap()
    }

    #[test]
    fn extracts_and_prepares_each_statement_of_a_batch() {
        let con = conn();
        let batch =
            ExtractedStatements::extract(&con, "SELECT 1 AS a; SELECT 'two' AS b; SELECT 3 AS c")
                .unwrap();
        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());

        // Each parses/prepares independently and yields its own result.
        let mut s0 = batch.prepare(0).unwrap();
        let v = s0.execute().unwrap().next().unwrap().unwrap();
        assert_eq!(v.get("a"), Some(&DuckValue::Int(1)));

        let mut s2 = batch.prepare(2).unwrap();
        let v = s2.execute().unwrap().next().unwrap().unwrap();
        assert_eq!(v.get("c"), Some(&DuckValue::Int(3)));
    }

    #[test]
    fn semicolon_inside_a_string_literal_is_not_a_split() {
        // A naive splitter would see two statements; DuckDB's parser sees one.
        let con = conn();
        let batch = ExtractedStatements::extract(&con, "SELECT 'a;b' AS s").unwrap();
        assert_eq!(batch.len(), 1);
        let mut s = batch.prepare(0).unwrap();
        let v = s.execute().unwrap().next().unwrap().unwrap();
        assert_eq!(v.get("s"), Some(&DuckValue::Text("a;b".into())));
    }

    #[test]
    fn out_of_range_index_is_rejected() {
        let con = conn();
        let batch = ExtractedStatements::extract(&con, "SELECT 1").unwrap();
        assert!(matches!(
            batch.prepare(5),
            Err(Error::DuckDBFailure(_, Some(m))) if m.contains("out of range")
        ));
    }

    #[test]
    fn empty_query_reports_a_typed_engine_error() {
        // An empty query extracts to zero statements — DuckDB returns count 0 with
        // an extract error, which we surface as a typed engine error. (A *syntax*
        // error like "SELCT 1" is deliberately not tested here: this DuckDB build
        // throws a C++ parser exception across the C API on that path instead of
        // returning 0, and a foreign exception crossing the FFI boundary aborts the
        // process — an upstream limitation, not a wrapper bug.)
        let con = conn();
        let err = ExtractedStatements::extract(&con, "").unwrap_err();
        assert!(matches!(err, Error::Engine(_)), "expected engine error, got {err:?}");
    }

    #[test]
    fn interior_nul_query_is_rejected() {
        let con = conn();
        assert!(matches!(
            ExtractedStatements::extract(&con, "SELECT 1\0; SELECT 2"),
            Err(Error::NulError(_))
        ));
    }

    #[test]
    fn a_prepared_extracted_statement_outlives_the_batch() {
        // The prepared statement retains the connection, not the batch, so dropping
        // the batch first must not invalidate it.
        let con = conn();
        let batch = ExtractedStatements::extract(&con, "SELECT 42 AS v").unwrap();
        let mut stmt = batch.prepare(0).unwrap();
        drop(batch);
        let v = stmt.execute().unwrap().next().unwrap().unwrap();
        assert_eq!(v.get("v"), Some(&DuckValue::Int(42)));
    }
}
