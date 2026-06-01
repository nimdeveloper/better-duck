//! `QueryFragment<DuckDb>` implementations for LIMIT / OFFSET clauses.
//!
//! DuckDB supports standard SQL `LIMIT n` and `OFFSET m` clauses, so the
//! implementation mirrors the PostgreSQL approach: delegate to the inner
//! clause fragments, which handle the SQL text.

use diesel::query_builder::{AstPass, QueryFragment};
use diesel::query_builder::{BoxedLimitOffsetClause, LimitOffsetClause};
use diesel::result::QueryResult;

use crate::backend::DuckDb;

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
