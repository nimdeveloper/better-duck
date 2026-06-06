# better-duck-diesel test suite

## Implemented types with round-trip coverage

| DuckDB type | Diesel SQL type | Rust type | Test file |
|---|---|---|---|
| BOOLEAN | `Bool` | `bool` | `types_roundtrip.rs` |
| TINYINT | `DuckTinyInt` | `i8` | `types_roundtrip.rs` |
| SMALLINT | `SmallInt` | `i16` | `types_roundtrip.rs` |
| INTEGER | `Integer` | `i32` | `types_roundtrip.rs` |
| BIGINT | `BigInt` | `i64` | `types_roundtrip.rs` |
| UTINYINT | `DuckUTinyInt` | `u8` | `types_roundtrip.rs` |
| USMALLINT | `DuckUSmallInt` | `u16` | `types_roundtrip.rs` |
| UINTEGER | `DuckUInt` | `u32` | `types_roundtrip.rs` |
| UBIGINT | `DuckUBigInt` | `u64` | `types_roundtrip.rs` |
| HUGEINT | `DuckHugeInt` | `i128` | `types_roundtrip.rs` |
| UHUGEINT | `DuckUHugeInt` | `u128` | `types_roundtrip.rs` |
| FLOAT | `Float` | `f32` | `types_roundtrip.rs` |
| DOUBLE | `Double` | `f64` | `types_roundtrip.rs` |
| VARCHAR | `Text` | `String` | `types_roundtrip.rs` |
| BLOB | `Binary` | `Vec<u8>` | `types_roundtrip.rs` |
| DATE | `Date` | `NaiveDate` | `types_roundtrip.rs` (feature=chrono) |
| TIME | `Time` | `NaiveTime` | `types_roundtrip.rs` (feature=chrono) |
| TIMESTAMP | `Timestamp` | `NaiveDateTime` | `types_roundtrip.rs` (feature=chrono) |
| TIMESTAMPTZ | `DuckTimestamptz` | `DateTime<Utc>` | `types_roundtrip.rs` (feature=chrono) |
| TIMETZ | `DuckTimeTz` | `TimeTz` | `types_roundtrip.rs` (feature=chrono) |
| TIME_NS | `DuckTimeNs` | `NaiveTime` | `types_roundtrip.rs` (feature=chrono) |
| LIST | `DuckList` | `Vec<DuckValue>` | `types_roundtrip.rs` |
| ENUM | `DuckEnum` | `String` | `types_roundtrip.rs` |

## Known gaps — not yet implemented

These types exist in DuckDB and in `DuckValueRef` but have no `FromSql`/`ToSql` impl in this crate.
A contributor who wants to add them should follow the pattern in `src/types/list.rs`.

### STRUCT
DuckDB's `STRUCT` is a row type with named, heterogeneous fields:
```sql
STRUCT(x INTEGER, y VARCHAR)
```
**Recommended bridge:** Define a Rust struct, derive `Queryable` and `Insertable`, and implement
`FromSql<DuckStruct, DuckDb> for MyStruct` by pattern-matching `DuckValueRef::Struct(HashMap<String, DuckValueRef>)`.
Each field becomes a `FromSql` call on its inner `DuckValueRef`. Alternatively, map to
`serde_json::Value` for a schema-agnostic representation.

### MAP
DuckDB's `MAP` is an ordered key-value collection (similar to `HashMap` but preserving insertion order):
```sql
MAP(VARCHAR, INTEGER)
```
**Recommended bridge:** `FromSql<DuckMap, DuckDb> for HashMap<K, V>` where `K: FromSql<..>` and
`V: FromSql<..>`. Or use `indexmap::IndexMap` to preserve order. Match on
`DuckValueRef::Map(HashMap<DuckValue, DuckValue>)`.

### UNION
DuckDB's `UNION` is a tagged-union type:
```sql
UNION(n INTEGER, s VARCHAR)
```
**Recommended bridge:** A Rust enum (one variant per union arm) deriving a custom `FromSql` impl
that inspects `DuckValueRef::Union(Box<DuckValueRef>)` and the active tag name (available via
the column schema). Alternatively, map to an untagged `DuckValue`.

### Placeholder tests (ignored)

```rust
#[test]
#[ignore = "STRUCT FromSql/ToSql not yet implemented — see tests/README.md"]
fn struct_roundtrip_placeholder() {}

#[test]
#[ignore = "MAP FromSql/ToSql not yet implemented — see tests/README.md"]
fn map_roundtrip_placeholder() {}

#[test]
#[ignore = "UNION FromSql/ToSql not yet implemented — see tests/README.md"]
fn union_roundtrip_placeholder() {}
```
