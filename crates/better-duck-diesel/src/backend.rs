//! The DuckDB backend and associated type-metadata types.

use super::query_builder::DuckDbQueryBuilder;
pub use crate::bind_collector::DuckDbBindCollector;
use crate::types::duckdb_types::{
    DuckArray, DuckBignum, DuckBit, DuckEnum, DuckHugeInt, DuckInterval, DuckList, DuckMap,
    DuckStruct, DuckTimeNs, DuckTimeTz, DuckTimestamptz, DuckTinyInt, DuckUBigInt, DuckUHugeInt,
    DuckUInt, DuckUSmallInt, DuckUTinyInt, DuckUnion, DuckUuid,
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

    type WindowFrameClauseGroupSupport =
        sql_dialect::window_frame_clause_group_support::IsoGroupWindowFrameUnit;
    type WindowFrameExclusionSupport =
        sql_dialect::window_frame_exclusion_support::FrameExclusionSupport;
    type AggregateFunctionExpressions =
        sql_dialect::aggregate_function_expressions::PostgresLikeAggregateFunctionExpressions;
    type BuiltInWindowFunctionRequireOrder =
        sql_dialect::built_in_window_function_require_order::NoOrderRequired;
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
        DuckDbTypeWrapper(DuckDbType::TimestampTz)
    }
}

impl HasSqlType<DuckTimeTz> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::TimeTz)
    }
}

impl HasSqlType<DuckTimeNs> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::TimeNs)
    }
}

impl HasSqlType<DuckEnum> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Enum)
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

impl HasSqlType<DuckStruct> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Struct)
    }
}

impl HasSqlType<DuckMap> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Map)
    }
}

impl HasSqlType<DuckUnion> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Union(Box::new(DuckDbType::Any)))
    }
}

impl HasSqlType<DuckArray> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Array(Box::new([DuckDbType::Any])))
    }
}

impl HasSqlType<DuckUuid> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Uuid)
    }
}

impl HasSqlType<DuckBit> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Bit)
    }
}

impl HasSqlType<DuckBignum> for DuckDb {
    fn metadata(_: &mut Self::MetadataLookup) -> DuckDbTypeWrapper {
        DuckDbTypeWrapper(DuckDbType::Bignum)
    }
}

// On-conflict clause

/// Marker type for DuckDB's `ON CONFLICT` clause support.
#[derive(Debug, Copy, Clone)]
pub struct DuckDbOnConflictClause;

impl sql_dialect::on_conflict_clause::SupportsOnConflictClause for DuckDbOnConflictClause {}
impl sql_dialect::on_conflict_clause::PgLikeOnConflictClause for DuckDbOnConflictClause {}
impl sql_dialect::on_conflict_clause::SupportsOnConflictClauseWhere for DuckDbOnConflictClause {}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use better_duck_core::types::Type;

    use super::{
        DuckArray, DuckBignum, DuckBit, DuckDb, DuckDbTypeWrapper, DuckEnum, DuckHugeInt,
        DuckInterval, DuckList, DuckMap, DuckStruct, DuckTimeNs, DuckTimeTz, DuckTimestamptz,
        DuckTinyInt, DuckUBigInt, DuckUHugeInt, DuckUInt, DuckUSmallInt, DuckUTinyInt, DuckUnion,
        DuckUuid,
    };
    use diesel::sql_types::{
        BigInt, Binary, Bool, Date, Double, Float, HasSqlType, Integer, Interval, SmallInt, Text,
        Time, Timestamp,
    };

    fn metadata<ST>() -> Type
    where
        DuckDb: HasSqlType<ST>,
    {
        <DuckDb as HasSqlType<ST>>::metadata(&mut ()).0
    }

    fn hash(value: &DuckDbTypeWrapper) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn metadata_matches_every_supported_sql_type() {
        assert_eq!(metadata::<Bool>(), Type::Boolean);
        assert_eq!(metadata::<SmallInt>(), Type::SmallInt);
        assert_eq!(metadata::<Integer>(), Type::Int);
        assert_eq!(metadata::<BigInt>(), Type::BigInt);
        assert_eq!(metadata::<Float>(), Type::Float);
        assert_eq!(metadata::<Double>(), Type::Double);
        assert_eq!(metadata::<Text>(), Type::Text);
        assert_eq!(metadata::<Binary>(), Type::Blob);
        assert_eq!(metadata::<Date>(), Type::Date);
        assert_eq!(metadata::<Time>(), Type::Time);
        assert_eq!(metadata::<Timestamp>(), Type::Timestamp);
        assert_eq!(metadata::<Interval>(), Type::Interval);
        #[cfg(feature = "decimal")]
        assert_eq!(metadata::<diesel::sql_types::Numeric>(), Type::Decimal);

        assert_eq!(metadata::<DuckTinyInt>(), Type::TinyInt);
        assert_eq!(metadata::<DuckUTinyInt>(), Type::UTinyInt);
        assert_eq!(metadata::<DuckUSmallInt>(), Type::USmallInt);
        assert_eq!(metadata::<DuckUInt>(), Type::UInt);
        assert_eq!(metadata::<DuckUBigInt>(), Type::UBigInt);
        assert_eq!(metadata::<DuckHugeInt>(), Type::HugeInt);
        assert_eq!(metadata::<DuckUHugeInt>(), Type::UHugeInt);
        assert_eq!(metadata::<DuckInterval>(), Type::Interval);
        assert_eq!(metadata::<DuckTimestamptz>(), Type::TimestampTz);
        assert_eq!(metadata::<DuckTimeTz>(), Type::TimeTz);
        assert_eq!(metadata::<DuckTimeNs>(), Type::TimeNs);
        assert_eq!(metadata::<DuckList>(), Type::List);
        assert_eq!(metadata::<DuckEnum>(), Type::Enum);
        assert_eq!(metadata::<DuckStruct>(), Type::Struct);
        assert_eq!(metadata::<DuckMap>(), Type::Map);
        assert_eq!(metadata::<DuckUnion>(), Type::Union(Box::new(Type::Any)));
        assert_eq!(metadata::<DuckArray>(), Type::Array(Box::new([Type::Any])));
        assert_eq!(metadata::<DuckUuid>(), Type::Uuid);
        assert_eq!(metadata::<DuckBit>(), Type::Bit);
        assert_eq!(metadata::<DuckBignum>(), Type::Bignum);
    }

    #[test]
    fn equal_composite_types_have_equal_hashes() {
        let left = DuckDbTypeWrapper(Type::Array(
            vec![Type::Int, Type::Union(Box::new(Type::Text))].into_boxed_slice(),
        ));
        let right = left.clone();
        assert_eq!(left, right);
        assert_eq!(hash(&left), hash(&right));
    }

    #[test]
    fn distinct_composite_shapes_hash_differently() {
        let array = DuckDbTypeWrapper(Type::Array(vec![Type::Int].into_boxed_slice()));
        let union = DuckDbTypeWrapper(Type::Union(Box::new(Type::Int)));
        let longer_array =
            DuckDbTypeWrapper(Type::Array(vec![Type::Int, Type::Int].into_boxed_slice()));
        assert_ne!(hash(&array), hash(&union));
        assert_ne!(hash(&array), hash(&longer_array));
    }
}
