use std::{
    cell::OnceCell,
    ffi::CStr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::ffi::{duckdb_column_count, duckdb_column_name, duckdb_destroy_result, DUCKDB_TYPE};

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi,
    raw::row::DuckRow,
    result_set::ResultSet,
};

use super::data_chunk::DataChunk;

/// Represents the result of a DuckDB query, providing row-by-row iteration over
/// the returned data.
///
/// `DuckResult` owns the underlying `duckdb_result` and destroys it in [`Drop`].
/// It implements [`Iterator`] yielding `Result<DuckRow>`.
///
/// # Safety
///
/// This struct interacts directly with the DuckDB C API. The underlying
/// `duckdb_result` must be a fully initialized result from a successful query.
pub struct DuckResult {
    res: ffi::duckdb_result,
    chunk: Option<DataChunk>,
    /// Owned column names, populated once on construction. `Arc`-shared so every
    /// [`DuckRow`] built from this result clones it in O(1) instead of re-allocating
    /// the whole column-name array per row.
    column_names: OnceCell<Arc<[Box<str>]>>,
    column_types: Box<[DUCKDB_TYPE]>,
    /// Number of columns in the result.
    pub col_count: u64,
    /// Rows already pulled from the underlying result. Only populated once
    /// [`enable_rewind`](DuckResult::enable_rewind) has been called — plain forward
    /// iteration (the common case) never touches this, so it costs nothing unless
    /// a caller opts in.
    cache: Vec<DuckRow>,
    /// Read position into `cache` consulted by the `Iterator` implementation.
    cursor: usize,
    /// `true` once the underlying result has yielded its last row.
    exhausted: bool,
    /// Set by [`enable_rewind`](DuckResult::enable_rewind); gates whether `next()`
    /// clones each row into `cache`.
    rewind_enabled: bool,
    /// A single-row lookahead buffer for [`exists`](DuckResult::exists), independent
    /// of `cache` — peeking a row must work whether or not rewind support is enabled.
    peeked: Option<DuckRow>,
}

impl DuckResult {
    /// Creates a new `DuckResult` from an owned `duckdb_result`.
    ///
    /// Immediately resolves column names and types. Panics if the result is in
    /// an invalid state (should not happen for a result from a successful query).
    pub fn new(mut result: ffi::duckdb_result) -> DuckResult {
        let mut res = DuckResult {
            // SAFETY: `result` is a valid, fully initialized `duckdb_result` that was
            // returned by `duckdb_query` or `duckdb_execute_prepared` and is now moved
            // (heap-allocated by the caller). `duckdb_column_count` reads from this struct.
            col_count: unsafe { duckdb_column_count(&mut result) },
            res: result,
            chunk: None,
            column_names: OnceCell::new(),
            column_types: Box::new([]),
            cache: Vec::new(),
            cursor: 0,
            exhausted: false,
            rewind_enabled: false,
            peeked: None,
        };
        res.resolve_columns_name().expect("failed to resolve column names");
        res.resolve_columns_types().expect("failed to resolve column types");
        res
    }

    #[inline]
    // SAFETY: caller must ensure `col_index` is within [0, col_count).
    fn get_col_type(
        &mut self,
        col_index: u64,
    ) -> DUCKDB_TYPE {
        // SAFETY: `self.res` is valid; `col_index` is within bounds (enforced by caller).
        unsafe { ffi::duckdb_column_type(&mut self.res, col_index) }
    }

    #[inline]
    fn resolve_columns_types(&mut self) -> Result<()> {
        // TODO: guard the uninit slice on early return (consider `scopeguard`)
        let mut col_types = Box::<[DUCKDB_TYPE]>::new_uninit_slice(self.col_count as usize);

        for each in 0..self.col_count {
            // SAFETY: `each` is within [0, col_count), satisfying the invariant of
            // `get_col_type`.
            let temp_col_type = self.get_col_type(each);
            // SAFETY: `col_types[each]` is within the allocation; we write an initialized
            // `DUCKDB_TYPE` value.
            unsafe {
                col_types[each as usize].as_mut_ptr().write(temp_col_type);
            }
        }
        // SAFETY: every element in `col_types` has been initialized above.
        self.column_types = unsafe { col_types.assume_init() };
        Ok(())
    }

    #[inline]
    fn resolve_columns_name(&mut self) -> Result<()> {
        let names = (0..self.col_count)
            .map(|i| {
                // SAFETY: `i` is within [0, col_count). `duckdb_column_name` returns a
                // pointer into result-owned memory valid for the lifetime of `self.res`.
                // We copy the bytes immediately so the raw pointer does not escape.
                let raw = unsafe { duckdb_column_name(&mut self.res, i) };
                if raw.is_null() {
                    return Err(Error::InvalidColumnIndex(i as usize));
                }
                // SAFETY: DuckDB guarantees null-terminated valid UTF-8 for column names.
                unsafe { CStr::from_ptr(raw) }
                    .to_str()
                    .map(|s| s.to_string().into_boxed_str())
                    .map_err(|e| {
                        Error::ConversionError(DuckDBConversionError::ConversionError(
                            e.to_string(),
                        ))
                    })
            })
            .collect::<Result<Vec<Box<str>>>>()?;

        self.column_names
            .set(Arc::from(names))
            .map_err(|_| Error::UNKNOWN("column names already set".into()))
    }

    /// Advances the internal cursor to the next row.
    ///
    /// Returns `Some(())` if a row is available, or `None` if all rows have been
    /// consumed.
    fn advance(&mut self) -> Option<()> {
        loop {
            if self.chunk.is_none() {
                // SAFETY: `self.res` is a valid duckdb_result. `DataChunk::from_result`
                // calls `duckdb_fetch_chunk` which returns null when exhausted.
                let next_chunk = DataChunk::from_result(self);
                match next_chunk {
                    None => return None,
                    Some(Err(_)) => {
                        self.chunk = None;
                        return None;
                    },
                    Some(Ok(chunk)) => {
                        self.chunk = Some(chunk);
                    },
                }
            }
            let the_chunk = self.chunk.as_mut().unwrap();
            // SAFETY: `the_chunk` wraps a valid duckdb_data_chunk.
            if the_chunk.row_count() == 0 {
                self.chunk = None;
                return None;
            }
            // SAFETY: `the_chunk` wraps a valid duckdb_data_chunk whose row count > 0.
            if the_chunk.next_row().is_some() {
                let row_chunk = **the_chunk;
                if row_chunk.is_null() {
                    panic!("Data chunk is null");
                }
                return Some(());
            } else {
                self.chunk = None;
                // Loop to fetch the next chunk.
            }
        }
    }

    /// Pulls the next row directly from the underlying DuckDB result, bypassing
    /// the cache. Returns `None` once the underlying result is exhausted.
    fn pull_next(&mut self) -> Option<Result<DuckRow>> {
        if self.advance().is_some() {
            Some(self.current())
        } else {
            None
        }
    }
}

// Exposed API
impl DuckResult {
    /// Returns the current row as a [`DuckRow`].
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk is not available or value conversion fails.
    pub fn current(&mut self) -> Result<DuckRow> {
        // O(1): `Arc` clone, not a fresh per-row allocation of the column-name array.
        let col_names = self.column_names.get().expect("column names resolved in new()").clone();
        let chunk = self.chunk.as_mut().unwrap();
        DuckRow::from_chunk(chunk, col_names, &self.column_types)
    }

    /// Returns the number of rows changed by the last INSERT/UPDATE/DELETE.
    ///
    /// Returns `0` for SELECT statements.
    #[allow(unused)]
    #[inline]
    pub fn changes(&mut self) -> u64 {
        // SAFETY: `self.res` is a valid duckdb_result.
        unsafe { ffi::duckdb_rows_changed(&mut self.res) }
    }

    /// Returns the number of columns in this result.
    #[allow(unused)]
    #[inline]
    pub fn column_count(&self) -> u64 {
        self.col_count
    }

    /// Returns the DuckDB type of the column at `col_index`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `col_index` is out of range.
    #[allow(unused)]
    #[inline]
    pub fn column_type(
        &self,
        col_index: usize,
    ) -> Result<DUCKDB_TYPE> {
        if col_index >= self.col_count as usize {
            return Err(Error::InvalidColumnIndex(col_index));
        }
        Ok(self.column_types[col_index])
    }

    /// Returns the name of the column at `col_index`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `col_index` is out of range.
    #[allow(unused)]
    #[inline]
    pub fn column_name(
        &self,
        col_index: usize,
    ) -> Result<&str> {
        if col_index >= self.col_count as usize {
            return Err(Error::InvalidColumnIndex(col_index));
        }
        Ok(&self.column_names.get().unwrap()[col_index])
    }

    /// Returns a slice of all column names in result order.
    #[allow(unused)]
    #[inline]
    pub fn column_names(&self) -> &[Box<str>] {
        self.column_names.get().map(|v| v.as_ref()).unwrap_or(&[])
    }

    /// Enables the [`rewind`](DuckResult::rewind) / replay cache.
    ///
    /// Plain forward iteration (the default) never clones or caches rows, so it
    /// costs nothing beyond decoding each row once. Call this *before* consuming any
    /// rows if you need [`rewind`](DuckResult::rewind) to replay from the start —
    /// once enabled, every row pulled through `next()` from that point on is cloned
    /// into a cache so it can be replayed. Rows already consumed before this call was
    /// made cannot be recovered.
    pub fn enable_rewind(&mut self) {
        self.rewind_enabled = true;
    }

    /// Returns the zero-based index of the column with the given name, or `None`
    /// if no column matches.
    #[allow(unused)]
    #[inline]
    pub fn column_idx(
        &self,
        col_name: &str,
    ) -> Option<usize> {
        self.column_names.get().unwrap().iter().position(|name| name.as_ref() == col_name)
    }

    /// Returns whether this result contains at least one more row, without
    /// consuming it — a subsequent call to `next()` still yields that row.
    ///
    /// # Errors
    ///
    /// Returns an error if pulling the first row fails.
    pub fn exists(&mut self) -> Result<bool> {
        if self.rewind_enabled && self.cursor < self.cache.len() {
            return Ok(true);
        }
        if self.peeked.is_some() {
            return Ok(true);
        }
        if self.exhausted {
            return Ok(false);
        }
        match self.pull_next() {
            Some(Ok(row)) => {
                // Buffer the row but don't touch `cursor`/`cache`, so `next()` still
                // returns it (from the peek buffer, then decides whether to cache it).
                self.peeked = Some(row);
                Ok(true)
            },
            Some(Err(e)) => Err(e),
            None => {
                self.exhausted = true;
                Ok(false)
            },
        }
    }

    /// Resets iteration to the first row.
    ///
    /// Requires [`enable_rewind`](DuckResult::enable_rewind) to have been called
    /// before the rows you want to replay were consumed — without it, `cache` is
    /// always empty and this is a no-op (iteration just continues forward as normal).
    pub fn rewind(&mut self) {
        self.cursor = 0;
    }

    /// Consumes this result, materializing every row into an owned [`ResultSet`].
    ///
    /// Unlike `DuckResult`, the returned `ResultSet` holds no FFI handles and is
    /// `Send` + `Sync` + `Clone`, so it can cross thread boundaries (e.g. out of a
    /// `spawn_blocking` closure).
    ///
    /// # Errors
    ///
    /// Returns an error if row conversion fails partway through iteration.
    pub fn materialize(mut self) -> Result<ResultSet> {
        let changes = self.changes();
        let column_names = self.column_names().to_vec().into_boxed_slice();
        let mut rows = Vec::new();
        for row in self {
            rows.push(row?);
        }
        Ok(ResultSet::new(rows, changes, column_names))
    }
}

impl Iterator for DuckResult {
    type Item = Result<DuckRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rewind_enabled && self.cursor < self.cache.len() {
            let row = self.cache[self.cursor].clone();
            self.cursor += 1;
            return Some(Ok(row));
        }
        if let Some(row) = self.peeked.take() {
            if self.rewind_enabled {
                self.cache.push(row.clone());
                self.cursor += 1;
            }
            return Some(Ok(row));
        }
        if self.exhausted {
            return None;
        }
        match self.pull_next() {
            Some(Ok(row)) => {
                if self.rewind_enabled {
                    self.cache.push(row.clone());
                    self.cursor += 1;
                }
                Some(Ok(row))
            },
            Some(Err(e)) => {
                self.exhausted = true;
                Some(Err(e))
            },
            None => {
                self.exhausted = true;
                None
            },
        }
    }

    /// Counts the remaining rows without materializing each one into a [`DuckRow`].
    ///
    /// The default `Iterator::count()` would call [`next`](Self::next) in a loop,
    /// which for every row allocates a `Vec<DuckValue>` and converts every column —
    /// wasted work when the caller only wants the row count. `count(self)` consumes
    /// `self`, so no further iteration can observe `cache`/`peeked` afterwards; the
    /// remaining rows are tallied by advancing the chunk cursor only, skipping the
    /// per-row/per-column conversion entirely.
    fn count(mut self) -> usize
    where
        Self: Sized,
    {
        let mut n = 0usize;
        if self.rewind_enabled && self.cursor < self.cache.len() {
            n += self.cache.len() - self.cursor;
            self.cursor = self.cache.len();
        }
        if self.peeked.take().is_some() {
            n += 1;
        }
        if self.exhausted {
            return n;
        }
        while self.advance().is_some() {
            n += 1;
        }
        n
    }
}

impl Deref for DuckResult {
    type Target = ffi::duckdb_result;

    fn deref(&self) -> &Self::Target {
        &self.res
    }
}
impl DerefMut for DuckResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.res
    }
}
impl Drop for DuckResult {
    fn drop(&mut self) {
        // SAFETY: `self.res` is a valid duckdb_result created by `DuckResult::new`.
        // `duckdb_destroy_result` is called exactly once here in `drop`.
        unsafe {
            duckdb_destroy_result(&mut self.res);
        }
    }
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use crate::{config::Config, helpers::path::path_to_cstring, raw::connection::RawConnection};

    fn get_test_connection() -> RawConnection {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    /// Plain forward iteration (the default, rewind not enabled) must not error and
    /// must yield every row exactly once.
    #[test]
    fn forward_iteration_without_rewind_yields_all_rows() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let result = stmt.execute().unwrap();
        let rows: Vec<_> = result.collect::<Result<_, _>>().unwrap();
        assert_eq!(rows.len(), 3);
    }

    /// `exists()` must peek without consuming — a subsequent full iteration still
    /// yields every row, including the one that was peeked.
    #[test]
    fn exists_peeks_without_consuming() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let mut result = stmt.execute().unwrap();

        assert!(result.exists().unwrap());
        assert!(result.exists().unwrap()); // idempotent — still doesn't consume

        let rows: Vec<_> = result.collect::<Result<_, _>>().unwrap();
        assert_eq!(rows.len(), 2, "the peeked row must still be yielded by next()");
    }

    /// Without `enable_rewind()`, `rewind()` is a documented no-op: the cache stays
    /// empty, so iteration just continues forward from wherever it was.
    #[test]
    fn rewind_without_enable_is_a_no_op() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let mut result = stmt.execute().unwrap();

        let first = result.next().unwrap().unwrap();
        result.rewind();
        // Not the first row again — rewind had nothing cached to replay.
        let second = result.next().unwrap().unwrap();
        assert_ne!(first.get("v"), second.get("v"));
    }

    /// With `enable_rewind()`, `rewind()` replays every row pulled after it was
    /// called, from the start.
    #[test]
    fn enable_rewind_then_rewind_replays_from_start() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let mut result = stmt.execute().unwrap();
        result.enable_rewind();

        let first_pass: Vec<_> = (&mut result).take(3).map(|r| r.unwrap()).collect();
        assert_eq!(first_pass.len(), 3);

        result.rewind();
        let second_pass: Vec<_> = result.collect::<Result<_, _>>().unwrap();
        assert_eq!(second_pass.len(), 3);
        for (a, b) in first_pass.iter().zip(second_pass.iter()) {
            assert_eq!(a.get("v"), b.get("v"));
        }
    }

    /// `enable_rewind()` after `exists()` has already peeked a row must still cache
    /// that row once it's consumed via `next()` — the peek buffer and the rewind
    /// cache must not race each other.
    #[test]
    fn enable_rewind_after_exists_peek_still_caches_the_peeked_row() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let mut result = stmt.execute().unwrap();

        assert!(result.exists().unwrap()); // peeks row 1, before rewind is enabled
        result.enable_rewind();
        let first = result.next().unwrap().unwrap(); // drains the peek, now cached

        result.rewind();
        let replayed = result.next().unwrap().unwrap();
        assert_eq!(first.get("v"), replayed.get("v"));
    }

    /// The fast-path `count()` (which skips per-row `DuckRow` materialization) must
    /// match plain forward iteration for a simple, single-chunk result.
    #[test]
    fn count_matches_iteration_length_for_plain_forward_iteration() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t").unwrap();
        let result = stmt.execute().unwrap();
        assert_eq!(result.count(), 3);
    }

    /// `count()` must correctly tally rows spanning more than one DuckDB vector
    /// chunk (default chunk size is 2048 rows) — `advance()` must be called enough
    /// times to cross chunk boundaries, not just enough for a single chunk.
    #[test]
    fn count_matches_iteration_length_across_multiple_chunks() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t AS SELECT * FROM range(5000) t(v)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t").unwrap();
        let result = stmt.execute().unwrap();
        assert_eq!(result.count(), 5000);
    }

    /// `exists()` peeks a row without consuming it; `count()` must still include
    /// that already-materialized peeked row in its total.
    #[test]
    fn count_after_exists_peek_includes_the_peeked_row() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2), (3)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t").unwrap();
        let mut result = stmt.execute().unwrap();
        assert!(result.exists().unwrap());
        assert_eq!(result.count(), 3);
    }

    /// With rewind enabled, some rows may already sit in `cache` (already pulled,
    /// not yet re-consumed via `cursor`) when `count()` is called on the remainder.
    /// `count()` must count both the not-yet-replayed cached rows and the rows still
    /// to be freshly pulled.
    #[test]
    fn count_with_rewind_enabled_after_partial_consumption() {
        let mut con = get_test_connection();
        con.query("CREATE TABLE t (v INTEGER)").unwrap();
        con.query("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();

        let mut stmt = con.prepare("SELECT v FROM t").unwrap();
        let mut result = stmt.execute().unwrap();
        result.enable_rewind();

        // Pull the first two rows (now cached) and rewind, moving the cursor back
        // to the start of the cache without clearing it.
        let _ = result.next().unwrap().unwrap();
        let _ = result.next().unwrap().unwrap();
        result.rewind();

        // From the start: 2 cached rows to replay + 3 rows still to be pulled fresh.
        assert_eq!(result.count(), 5);
    }
}
