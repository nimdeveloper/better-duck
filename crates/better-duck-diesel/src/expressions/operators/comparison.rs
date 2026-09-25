//! DuckDB null-safe comparison operators: `IS DISTINCT FROM` / `IS NOT DISTINCT
//! FROM` (Diesel only ships these behind its `postgres` gate).
//!
//! Docs: <https://duckdb.org/docs/current/sql/expressions/comparison_operators>

use diesel::expression::{AsExpression, Expression};
use diesel::infix_operator;
use diesel::sql_types::SqlType;

use crate::backend::DuckDb;

infix_operator!(IsDistinctFrom, " IS DISTINCT FROM ", backend: DuckDb);
infix_operator!(IsNotDistinctFrom, " IS NOT DISTINCT FROM ", backend: DuckDb);

/// Null-safe comparison methods for any DuckDB expression (mirrors diesel's
/// `PgExpressionMethods`, which is unavailable for a third-party backend).
// These are query-builder methods: like all diesel DSL methods they consume `self`
// (the left-hand expression), so the `is_*`-takes-`&self` convention doesn't apply.
#[allow(clippy::wrong_self_convention)]
pub trait DuckExpressionMethods: Expression + Sized {
    /// `IS DISTINCT FROM` — null-safe inequality (treats `NULL` as a value).
    fn is_distinct_from<T: AsExpression<Self::SqlType>>(
        self,
        other: T,
    ) -> IsDistinctFrom<Self, T::Expression>
    where
        Self::SqlType: SqlType,
    {
        IsDistinctFrom::new(self, other.as_expression())
    }

    /// `IS NOT DISTINCT FROM` — null-safe equality.
    fn is_not_distinct_from<T: AsExpression<Self::SqlType>>(
        self,
        other: T,
    ) -> IsNotDistinctFrom<Self, T::Expression>
    where
        Self::SqlType: SqlType,
    {
        IsNotDistinctFrom::new(self, other.as_expression())
    }
}

impl<T: Expression> DuckExpressionMethods for T {}
