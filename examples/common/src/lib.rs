//! Shared helpers for the better-duck example suite.
//!
//! Every example binary prints an illustrative trace *and* asserts the expected result,
//! so running an example (`cargo run -p <group> --bin <topic>`) also verifies it. This
//! crate holds the tiny bits of glue shared across groups so individual examples stay
//! focused on the API they demonstrate.

/// The core crate's `Result` alias, re-exported so every example can use `-> Result<()>`.
pub use better_duck_core::error::Result;

/// Print a labelled section header, used to structure an example's console output.
pub fn section(title: &str) {
    println!("\n=== {title} ===");
}

/// Print a `label = value` line (the standard way examples show a computed value).
pub fn show(
    label: &str,
    value: impl std::fmt::Debug,
) {
    println!("  {label} = {value:?}");
}
