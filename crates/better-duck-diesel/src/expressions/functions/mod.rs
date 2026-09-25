//! DuckDB scalar functions, grouped by domain, exposed as Diesel DSL via
//! `define_sql_function!`. These are backend-agnostic SQL calls that Diesel does
//! not provide out of the box.

pub mod blob;
pub mod conditional;
pub mod datetime;
pub mod hash;
pub mod json;
pub mod numeric;
pub mod regex;
pub mod string;
pub mod uuid;
