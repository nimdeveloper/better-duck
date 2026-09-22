//! Owned, recursive query-profiling tree.
//!
//! `duckdb_get_profiling_info` returns the root of a profiling tree that is *owned
//! by the connection* and only valid until the next query — there is no destroy
//! function for it, and the child pointers are borrowed the same way. To give
//! callers something that outlives later queries and the connection itself, this
//! module **materialises** the whole tree eagerly into owned [`ProfilingNode`]s
//! (metric name→value strings plus child nodes), copying every string out before
//! returning. Nothing here retains a `duckdb_profiling_info` pointer.
//!
//! When profiling is disabled, or no query has run yet, `duckdb_get_profiling_info`
//! returns null; the safe wrappers surface that as `None`.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;

use crate::ffi::{
    duckdb_connection, duckdb_destroy_value, duckdb_free, duckdb_get_map_key, duckdb_get_map_size,
    duckdb_get_map_value, duckdb_get_profiling_info, duckdb_get_varchar, duckdb_profiling_info,
    duckdb_profiling_info_get_child, duckdb_profiling_info_get_child_count,
    duckdb_profiling_info_get_metrics, duckdb_profiling_info_get_value, duckdb_value,
};

/// An owned node of the query-profiling tree: the metrics recorded at this node
/// (name → value, both strings) plus its child nodes, in DuckDB's order.
///
/// Materialised from `duckdb_get_profiling_info`, so it stays valid after further
/// queries and after the connection is dropped.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProfilingNode {
    metrics: HashMap<String, String>,
    children: Vec<ProfilingNode>,
}

impl ProfilingNode {
    /// All metrics recorded at this node (metric name → value string).
    #[must_use]
    pub fn metrics(&self) -> &HashMap<String, String> {
        &self.metrics
    }

    /// The value of a single metric at this node, if present.
    #[must_use]
    pub fn metric(
        &self,
        key: &str,
    ) -> Option<&str> {
        self.metrics.get(key).map(String::as_str)
    }

    /// This node's child nodes, in DuckDB's order.
    #[must_use]
    pub fn children(&self) -> &[ProfilingNode] {
        &self.children
    }

    /// Recursively materialises the borrowed profiling node `info` into an owned
    /// [`ProfilingNode`].
    ///
    /// # Safety
    ///
    /// `info` must be a valid, non-null `duckdb_profiling_info` (a root from
    /// `duckdb_get_profiling_info` or a child from
    /// `duckdb_profiling_info_get_child`). The pointer is only read, never destroyed.
    unsafe fn materialize(info: duckdb_profiling_info) -> ProfilingNode {
        // SAFETY: `info` is valid; `get_metrics` returns an owned MAP value (or null)
        // that we read and destroy below.
        let metrics = unsafe { metrics_of(info) };

        // SAFETY: `info` is valid.
        let child_count = unsafe { duckdb_profiling_info_get_child_count(info) };
        let mut children = Vec::with_capacity(child_count as usize);
        for i in 0..child_count {
            // SAFETY: `i` is within [0, child_count); the returned child is borrowed
            // (owned by the connection) and must not be destroyed.
            let child = unsafe { duckdb_profiling_info_get_child(info, i) };
            if !child.is_null() {
                // SAFETY: `child` is a valid, non-null borrowed profiling node.
                children.push(unsafe { ProfilingNode::materialize(child) });
            }
        }

        ProfilingNode { metrics, children }
    }
}

/// Reads the metric MAP of a profiling node into an owned `HashMap`.
///
/// # Safety
///
/// `info` must be a valid `duckdb_profiling_info`.
unsafe fn metrics_of(info: duckdb_profiling_info) -> HashMap<String, String> {
    // SAFETY: `info` is valid; `get_metrics` returns an owned MAP `duckdb_value` (or
    // null) that we destroy before returning.
    let mut map_value = unsafe { duckdb_profiling_info_get_metrics(info) };
    if map_value.is_null() {
        return HashMap::new();
    }
    // SAFETY: `map_value` is a valid MAP value; `get_map_size` reads its length.
    let n = unsafe { duckdb_get_map_size(map_value) };
    let mut out = HashMap::with_capacity(n as usize);
    for i in 0..n {
        // SAFETY: `i` is within [0, n); the key/value are owned `duckdb_value`s that
        // `owned_varchar` reads and destroys.
        let key = unsafe { owned_varchar(duckdb_get_map_key(map_value, i)) };
        // SAFETY: as above.
        let value = unsafe { owned_varchar(duckdb_get_map_value(map_value, i)) };
        if let (Some(k), Some(v)) = (key, value) {
            out.insert(k, v);
        }
    }
    // SAFETY: `map_value` was returned by `get_metrics`; destroy exactly once.
    unsafe { duckdb_destroy_value(&mut map_value) };
    out
}

/// Reads an owned `duckdb_value` as a `String` via `duckdb_get_varchar`, destroying
/// both the extracted C string and the value itself. Returns `None` if the value or
/// string is null.
///
/// # Safety
///
/// `value` must be an owned `duckdb_value` (the caller transfers ownership here).
unsafe fn owned_varchar(mut value: duckdb_value) -> Option<String> {
    if value.is_null() {
        return None;
    }
    // SAFETY: `value` is a valid, non-null duckdb_value; `get_varchar` returns a heap
    // `char*` (or null) that must be freed with `duckdb_free`.
    let c = unsafe { duckdb_get_varchar(value) };
    let out = if c.is_null() {
        None
    } else {
        // SAFETY: `c` is a valid, non-null, null-terminated C string.
        let s = unsafe { CStr::from_ptr(c) }.to_string_lossy().into_owned();
        // SAFETY: `c` was allocated by DuckDB and ownership transferred to us.
        unsafe { duckdb_free(c as *mut c_void) };
        Some(s)
    };
    // SAFETY: `value` is owned here; destroy exactly once.
    unsafe { duckdb_destroy_value(&mut value) };
    out
}

/// Materialises the connection's profiling tree, or `None` if profiling is disabled
/// or no query has run yet.
///
/// # Safety
///
/// `con` must be a valid open `duckdb_connection`.
pub(crate) unsafe fn profiling_info(con: duckdb_connection) -> Option<ProfilingNode> {
    // SAFETY: `con` is a valid connection; the returned root is borrowed (owned by
    // the connection, valid until the next query) and must not be destroyed. Null
    // means profiling is disabled or no query has run.
    let root = unsafe { duckdb_get_profiling_info(con) };
    if root.is_null() {
        return None;
    }
    // SAFETY: `root` is a valid, non-null borrowed profiling node.
    Some(unsafe { ProfilingNode::materialize(root) })
}

/// Fetches a single metric from the connection's root profiling node via
/// `duckdb_profiling_info_get_value`, or `None` if unavailable.
///
/// # Safety
///
/// `con` must be a valid open `duckdb_connection`.
pub(crate) unsafe fn profiling_metric(
    con: duckdb_connection,
    key: &str,
) -> Option<String> {
    let c_key = CString::new(key).ok()?;
    // SAFETY: `con` is valid; the returned root is borrowed and not destroyed.
    let root = unsafe { duckdb_get_profiling_info(con) };
    if root.is_null() {
        return None;
    }
    // SAFETY: `root` is a valid profiling node; `c_key` is a valid null-terminated
    // string. `get_value` returns an owned `duckdb_value` (or null) that
    // `owned_varchar` reads and destroys.
    let value = unsafe { duckdb_profiling_info_get_value(root, c_key.as_ptr()) };
    // SAFETY: `value` is owned (or null); `owned_varchar` takes ownership.
    unsafe { owned_varchar(value) }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;

    #[test]
    fn profiling_disabled_by_default_yields_none() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("SELECT 42").unwrap();
        // Profiling is off unless explicitly enabled, so there is no tree.
        assert!(conn.profiling_info().is_none());
        assert!(conn.profiling_metric("OPERATOR_NAME").is_none());
    }

    #[test]
    fn enabled_profiling_materialises_a_tree_that_outlives_later_queries() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA enable_profiling = 'no_output'").unwrap();
        // Run a query so a profile exists.
        let _ = conn.execute("SELECT 1 AS a, 2 AS b").unwrap().count();

        let tree = conn.profiling_info().expect("a profiling tree once enabled");
        // The root records at least one metric, and the tree is a real hierarchy.
        assert!(!tree.metrics().is_empty() || !tree.children().is_empty());

        // Materialised tree survives a subsequent query (the borrowed DuckDB pointer
        // would have been invalidated; the owned copy is unaffected).
        let _ = conn.execute("SELECT 99").unwrap().count();
        // Still readable — no use-after-free, no panic.
        let _ = tree.children().len();
        let _ = tree.metrics().len();
    }

    #[test]
    fn single_metric_lookup_uses_get_value() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA enable_profiling = 'no_output'").unwrap();
        let _ = conn.execute("SELECT 1 AS a").unwrap().count();
        // `QUERY_NAME` is always present once profiling is enabled; get_value returns
        // the executed SQL. (Only *known* keys are safe — see the method docs.)
        assert_eq!(conn.profiling_metric("QUERY_NAME").as_deref(), Some("SELECT 1 AS a"));
        // With profiling disabled the root is null, so lookup is None (and never
        // reaches the C-API defect below).
        let mut off = Connection::open_in_memory().unwrap();
        off.execute_batch("SELECT 1").unwrap();
        assert!(off.profiling_metric("QUERY_NAME").is_none());
    }

    // NOTE: `profiling_metric` with an *unknown* metric key is intentionally not
    // tested. Despite its C-API docs promising a null return for a missing metric,
    // `duckdb_profiling_info_get_value` throws a C++ exception that unwinds across the
    // FFI boundary and aborts the process ("Rust cannot catch foreign exceptions") —
    // the same upstream defect class as get_table_names/extract_statements. The
    // method documents that callers must pass a metric key known to exist (e.g. one
    // from `profiling_info().metrics().keys()`), and the tests only use known keys.
}
