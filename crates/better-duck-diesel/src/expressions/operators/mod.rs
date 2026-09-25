//! DuckDB-specific SQL operators, grouped by domain. Each operator is created with
//! diesel's `infix_operator!` and exposed through an `…ExpressionMethods` trait.

pub mod comparison;
pub mod pattern;
