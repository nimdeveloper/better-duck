//! `QueryFragment<DuckDb>` implementations for LIMIT / OFFSET clauses.
//!
//! DuckDB supports standard SQL `LIMIT n` and `OFFSET m` clauses, so the
//! implementation mirrors the PostgreSQL approach: delegate to the inner
//! clause fragments, which handle the SQL text.

use diesel::query_builder::{AstPass, QueryFragment};
use diesel::query_builder::{
    BoxedLimitOffsetClause, IntoBoxedClause, LimitClause, LimitOffsetClause, NoLimitClause,
    NoOffsetClause, OffsetClause,
};
use diesel::result::QueryResult;

use crate::backend::DuckDb;

impl<'a> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause { limit: None, offset: None }
    }
}

impl<'a, L> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<DuckDb> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause { limit: Some(Box::new(self.limit_clause)), offset: None }
    }
}

impl<'a, O> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    O: QueryFragment<DuckDb> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause { limit: None, offset: Some(Box::new(self.offset_clause)) }
    }
}

impl<'a, L, O> IntoBoxedClause<'a, DuckDb> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<DuckDb> + Send + 'a,
    O: QueryFragment<DuckDb> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, DuckDb>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause)),
            offset: Some(Box::new(self.offset_clause)),
        }
    }
}

/// Delegates both limit and offset to their inner `QueryFragment` impls.
impl<L, O> QueryFragment<DuckDb> for LimitOffsetClause<L, O>
where
    L: QueryFragment<DuckDb>,
    O: QueryFragment<DuckDb>,
{
    fn walk_ast<'b>(
        &'b self,
        mut out: AstPass<'_, 'b, DuckDb>,
    ) -> QueryResult<()> {
        self.limit_clause.walk_ast(out.reborrow())?;
        self.offset_clause.walk_ast(out.reborrow())?;
        Ok(())
    }
}

/// Handles the boxed (dynamically-dispatched) variant.
impl QueryFragment<DuckDb> for BoxedLimitOffsetClause<'_, DuckDb> {
    fn walk_ast<'b>(
        &'b self,
        mut out: AstPass<'_, 'b, DuckDb>,
    ) -> QueryResult<()> {
        if let Some(ref limit) = self.limit {
            limit.as_ref().walk_ast(out.reborrow())?;
        }
        if let Some(ref offset) = self.offset {
            offset.as_ref().walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}
