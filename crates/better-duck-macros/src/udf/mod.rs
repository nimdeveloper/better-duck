//! Code generation for the UDF attribute macros.
//!
//! Each submodule holds the `expand` for one macro; the thin `#[proc_macro_*]`
//! entry points live in the crate root ([`crate`]). Attribute parsing and
//! signature introspection are shared via [`crate::attrs`] and [`crate::sig`].

pub(crate) mod aggregate;
pub(crate) mod cast;
pub(crate) mod scalar;
pub(crate) mod table;
