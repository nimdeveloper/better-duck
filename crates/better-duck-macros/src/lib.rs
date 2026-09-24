//! Procedural macros for `better-duck` user-defined functions.
//!
//! This crate is not meant to be used directly — depend on `better-duck-core`
//! with the `udf` feature enabled, which re-exports [`duckdb_scalar`],
//! [`duckdb_table_function`], [`duckdb_aggregate`], and [`duckdb_cast`].

mod attrs;
mod derive;
mod sig;
mod udf;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, ItemFn, ItemMod};

/// Registers a plain Rust function as a DuckDB scalar function.
///
/// See `better_duck_core::udf` for usage and supported attribute options.
#[proc_macro_attribute]
pub fn duckdb_scalar(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let original = input.clone();
    match attrs::parse_scalar_attrs(attr).and_then(|attrs| udf::scalar::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            // Re-emit the original item alongside the error so the user gets one
            // good diagnostic instead of a cascade of "cannot find function".
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Registers a plain Rust function as a DuckDB table function.
///
/// See `better_duck_core::udf` for usage and supported attribute options.
#[proc_macro_attribute]
pub fn duckdb_table_function(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let original = input.clone();
    match attrs::parse_table_attrs(attr).and_then(|attrs| udf::table::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Registers a single Rust function as a DuckDB custom cast (`CAST`/`TRY_CAST`).
///
/// Applied to `fn(source) -> target` (or `-> Result<target, E>`); the source and
/// target logical types are inferred from the signature. See `better_duck_core::udf`.
#[proc_macro_attribute]
pub fn duckdb_cast(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let original = input.clone();
    match attrs::parse_cast_attrs(attr).and_then(|attrs| udf::cast::expand(attrs, input)) {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Registers a `mod` of `init`/`update`/`combine`/`finalize` functions as a DuckDB
/// aggregate function.
///
/// The state type is inferred from `init`'s return, the argument types from
/// `update`, and the result type from `finalize`. See `better_duck_core::udf`.
#[proc_macro_attribute]
pub fn duckdb_aggregate(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let input = parse_macro_input!(item as ItemMod);
    let original = input.clone();
    match attrs::parse_aggregate_attrs(attr).and_then(|attrs| udf::aggregate::expand(attrs, input))
    {
        Ok(expanded) => expanded.into(),
        Err(err) => {
            let mut out: TokenStream = quote::quote!(#original).into();
            out.extend(TokenStream::from(err.to_compile_error()));
            out
        },
    }
}

/// Derives `better_duck_core::FromRow` for a struct with named fields: each field
/// is read from a query row by column name via `FromDuckValue`.
///
/// Container options: `#[duck(rename_all = "…")]`, `#[duck(crate = ::path)]`.
/// Field options: `#[duck(rename = "column")]`.
#[proc_macro_derive(FromRow, attributes(duck))]
pub fn derive_from_row(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match derive::from_row::expand(input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derives DuckDB `ENUM` mapping for a Rust unit enum: `From<T> for DuckValue`,
/// `FromDuckValue for T`, and `AppendAble for T` (bound by label).
///
/// Container options: `#[duck_enum(rename_all = "…")]`, `#[duck_enum(crate = ::path)]`.
/// Variant options: `#[duck_enum(rename = "label")]`.
#[proc_macro_derive(DuckEnum, attributes(duck_enum))]
pub fn derive_duck_enum(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match derive::duck_enum::expand(input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
