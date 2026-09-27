//! Capability-file scope entries (T2.5).
//!
//! Declares the JSON schema for per-capability scope entries so a Tauri app can
//! constrain this plugin's connection and filesystem reach from its capability
//! files — a declarative complement to the init-time policy the host configures
//! in Rust (see `crate::policy`). The schema is registered from `build.rs` via
//! `tauri_plugin::Builder::global_scope_schema`; at runtime the host reads the
//! resolved entries through `tauri::ipc::GlobalScope<Entry>`.
//!
//! This module is kept free of other crate modules on purpose: `build.rs`
//! includes it directly (`#[path = "src/scope.rs"]`) to derive the schema before
//! the crate itself compiles, so it must not reference sibling modules in code.

use serde::{Deserialize, Serialize};

/// One scope entry in a capability file's `"scope"` array for this plugin.
///
/// Enforcement is **per field**: `load` checks `connection`, while `import`/`export` check
/// `path`. Each field is evaluated independently — a deny match rejects, and if any allow
/// entries exist *for that field* the value must match one. An empty allow list for a field
/// means that field is **unconstrained** (the init-time [`Policy`](crate::policy::Policy)
/// remains the mandatory layer). Consequence: a capability that declares only `path` entries
/// does **not** restrict `connection`, and vice-versa — populate both fields if you mean to
/// constrain both. Path matching is component-aware and rejects `..` traversal.
///
/// Example capability entry allowing an on-disk database and an export directory:
///
/// ```json
/// { "connection": "duckdb:analytics.db", "path": "exports/" }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    /// Allow a database connection string, e.g. `"duckdb:analytics.db"` or
    /// `"duckdb::memory:"`. Omit to leave the connection unconstrained by this entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    /// Allow a filesystem path prefix for import/export sources and targets.
    /// Omit to leave paths unconstrained by this entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}
