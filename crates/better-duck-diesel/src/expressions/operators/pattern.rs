//! DuckDB text-matching operators — the case-insensitive / regex / glob family
//! Diesel only ships behind its `postgres`/`sqlite` feature gates:
//! `ILIKE`, `NOT ILIKE`, `SIMILAR TO`, `NOT SIMILAR TO`, `GLOB`, `~`, `!~`, `^@`.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/pattern_matching>

use diesel::expression::{AsExpression, Expression};
use diesel::infix_operator;
use diesel::sql_types::Text;

use crate::backend::DuckDb;

infix_operator!(ILike, " ILIKE ", backend: DuckDb);
infix_operator!(NotILike, " NOT ILIKE ", backend: DuckDb);
infix_operator!(SimilarTo, " SIMILAR TO ", backend: DuckDb);
infix_operator!(NotSimilarTo, " NOT SIMILAR TO ", backend: DuckDb);
infix_operator!(Glob, " GLOB ", backend: DuckDb);
infix_operator!(RegexpMatches, " ~ ", backend: DuckDb);
infix_operator!(NotRegexpMatches, " !~ ", backend: DuckDb);
infix_operator!(StartsWith, " ^@ ", backend: DuckDb);

/// Generates a text-operator method that binds its right-hand side as `Text`.
macro_rules! text_op {
    ($(#[$m:meta])* $method:ident => $Op:ident) => {
        $(#[$m])*
        fn $method<__Rhs: AsExpression<Text>>(
            self,
            other: __Rhs,
        ) -> $Op<Self, __Rhs::Expression> {
            $Op::new(self, other.as_expression())
        }
    };
}

/// Text-matching methods for any DuckDB expression (mirrors diesel's
/// `PgTextExpressionMethods`, which is unavailable for a third-party backend).
pub trait DuckTextExpressionMethods: Expression + Sized {
    text_op!(/// `ILIKE` — case-insensitive `LIKE`.
        ilike => ILike);
    text_op!(/// `NOT ILIKE`.
        not_ilike => NotILike);
    text_op!(/// `SIMILAR TO` — whole-string regular-expression match.
        similar_to => SimilarTo);
    text_op!(/// `NOT SIMILAR TO`.
        not_similar_to => NotSimilarTo);
    text_op!(/// `GLOB` — Unix-style `*`/`?`/`[…]` pattern match (case-sensitive).
        glob => Glob);
    text_op!(/// `~` — POSIX regular-expression match (`regexp_full_match`).
        regexp_matches => RegexpMatches);
    text_op!(/// `!~` — negated POSIX regular-expression match.
        not_regexp_matches => NotRegexpMatches);
    text_op!(/// `^@` — prefix (`starts_with`) test.
        starts_with_op => StartsWith);
}

impl<T: Expression> DuckTextExpressionMethods for T {}
