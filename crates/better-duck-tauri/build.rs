//! Build script: declare the webview-invokable commands so Tauri can
//! auto-generate their `allow-*` / `deny-*` permissions, and register the
//! capability scope schema (see `src/scope.rs`).

// Included directly so the scope schema can be derived before the crate compiles.
#[path = "src/scope.rs"]
mod scope;

/// Commands callable from the webview. `execute`/`select` take raw SQL and are the
/// pieces gated behind `duck:allow-raw-sql` in a capability (kept out of `default`
/// except `select`, which is read-only). Keep in sync with `src/commands.rs`.
const COMMANDS: &[&str] = &[
    "load",
    "close",
    "select",
    "execute",
    "load_extension",
    "import",
    "export",
    "append_rows",
    "tables",
    "columns",
    "explain",
    "stream",
    "checkpoint",
    "interrupt",
    "revert",
    "query_arrow",
];

fn main() {
    tauri_plugin::Builder::new(COMMANDS)
        .global_scope_schema(schemars::schema_for!(scope::Entry))
        .build();
}
