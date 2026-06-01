//! The DuckDB backend and associated type-metadata types.

use super::query_builder::DuckDbQueryBuilder;
pub use crate::bind_collector::DuckDbBindCollector;
use crate::types::duckdb_types::{
    DuckHugeInt, DuckInterval, DuckList, DuckTimestamptz, DuckTinyInt, DuckUBigInt, DuckUHugeInt,
    DuckUInt, DuckUSmallInt, DuckUTinyInt,
};
use better_duck_core::types::value_ref::DuckValueRef;
use better_duck_core::types::Type as DuckDbType;
use diesel::backend::sql_dialect::array_comparison::AnsiSqlArrayComparison;
use diesel::sql_types as MustSupport;
use diesel::sql_types::TypeMetadata;
use diesel::{backend::*, sql_types::HasSqlType};

/// The DuckDB backend marker type.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Default)]
pub struct DuckDb;

// DuckDbTypeWrapper

/// Newtype wrapping [`better_duck_core::types::Type`] that satisfies
/// [`diesel::connection::statement_cache::StatementCacheKey`]'s `Hash + Eq` requirement.
///
/// `better_duck_core::types::Type` does not derive `Hash` (its `Array` and `Union`
/// variants contain heap-allocated recursive types). This wrapper provides a
/// correct manual `Hash` implementation so `StatementCacheKey<DuckDb>` can be
/// stored in a `HashMap`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbTypeWrapper(pub(crate) DuckDbType);

/// Recursively hashes a [`DuckDbType`] using its discriminant.
///
/// Non-composite variants are hashed by discriminant alone. `Array` and `Union`
/// recurse into their inner types.
fn hash_duck_type<H: std::hash::Hasher>(
    t: &DuckDbType,
    state: &mut H,
) {
    use std::hash::Hash;
    std::mem::discriminant(t).hash(state);
    match t {
        DuckDbType::Array(inner) => {
            inner.len().hash(state);
            for elem in inner.iter() {
                hash_duck_type(elem, state);
            }
        },
        DuckDbType::Union(inner) => hash_duck_type(inner, state),
        _ => {},
    }
}

impl std::hash::Hash for DuckDbTypeWrapper {
    fn hash<H: std::hash::Hasher>(
        &self,
        state: &mut H,
    ) {
        hash_duck_type(&self.0, state);
    }
}

// Backend trait impls

impl Backend for DuckDb {
    type QueryBuilder = DuckDbQueryBuilder;
    type RawValue<'a> = DuckValueRef<'a>;
    type BindCollector<'a> = DuckDbBindCollector<'a>;
}

impl SqlDialect for DuckDb {
    type ReturningClause = sql_dialect::returning_clause::PgLikeReturningClause;
    type OnConflictClause = DuckDbOnConflictClause;
    type InsertWithDefaultKeyword = sql_dialect::default_keyword_for_insert::IsoSqlDefaultKeyword;
    type BatchInsertSupport = sql_dialect::batch_insert_support::PostgresLikeBatchInsertSupport;
    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;
    type EmptyFromClauseSyntax = sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;
    type SelectStatementSyntax = sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;
    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;
    type ArrayComparison = AnsiSqlArrayComparison;
    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;
}

impl TypeMetadata for DuckDb {
    type TypeMetadata = DuckDbTypeWrapper;
    type MetadataLookup = ();
}

impl DieselReserveSpecialization for DuckDb {}
impl TrustedBackend for DuckDb {}

// HasSqlType — standard Diesel types

impl HasSqlType<MustSupport::Bool> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Boolean)
    }
}

impl HasSqlType<MustSupport::SmallInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::SmallInt)
    }
}

impl HasSqlType<MustSupport::Integer> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Int)
    }
}

impl HasSqlType<MustSupport::BigInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::BigInt)
    }
}

impl HasSqlType<MustSupport::Float> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Float)
    }
}

impl HasSqlType<MustSupport::Double> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Double)
    }
}

impl HasSqlType<MustSupport::Text> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Text)
    }
}

impl HasSqlType<MustSupport::Binary> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Blob)
    }
}

impl HasSqlType<MustSupport::Date> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Date)
    }
}

impl HasSqlType<MustSupport::Time> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Time)
    }
}

impl HasSqlType<MustSupport::Timestamp> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Timestamp)
    }
}

#[cfg(feature = "decimal")]
impl HasSqlType<MustSupport::Numeric> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Decimal)
    }
}

impl HasSqlType<MustSupport::Interval> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Interval)
    }
}

// HasSqlType — DuckDB-specific types

impl HasSqlType<DuckTinyInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::TinyInt)
    }
}

impl HasSqlType<DuckUTinyInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::UTinyInt)
    }
}

impl HasSqlType<DuckUSmallInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::USmallInt)
    }
}

impl HasSqlType<DuckUInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::UInt)
    }
}

impl HasSqlType<DuckUBigInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::UBigInt)
    }
}

impl HasSqlType<DuckHugeInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::HugeInt)
    }
}

impl HasSqlType<DuckUHugeInt> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::UHugeInt)
    }
}

impl HasSqlType<DuckTimestamptz> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Timestamp)
    }
}

impl HasSqlType<DuckInterval> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Interval)
    }
}

impl HasSqlType<DuckList> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::List)
    }
}

// On-conflict clause

/// Marker type for DuckDB's `ON CONFLICT` clause support.
#[derive(Debug, Copy, Clone)]
pub struct DuckDbOnConflictClause;

impl sql_dialect::on_conflict_clause::SupportsOnConflictClause for DuckDbOnConflictClause {}
impl sql_dialect::on_conflict_clause::PgLikeOnConflictClause for DuckDbOnConflictClause {}
impl sql_dialect::on_conflict_clause::SupportsOnConflictClauseWhere for DuckDbOnConflictClause {}
