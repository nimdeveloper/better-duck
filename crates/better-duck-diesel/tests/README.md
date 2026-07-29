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
| DATE | `Date` | `NaiveDate` (feature=chrono) / `DuckDate` | `types_roundtrip.rs` |
| TIME | `Time` | `NaiveTime` (feature=chrono) / `DuckTime` | `types_roundtrip.rs` |
| TIMESTAMP | `Timestamp` | `NaiveDateTime` (feature=chrono) / `SystemTime` | `types_roundtrip.rs` |
| INTERVAL | `Interval` | `chrono::Duration` (feature=chrono) / `std::time::Duration` | `types_roundtrip.rs` |
| TIMESTAMPTZ | `DuckTimestamptz` | `DateTime<Utc>` (feature=chrono) / `SystemTime` | `types_roundtrip.rs` |
| TIMETZ | `DuckTimeTz` | `date_chrono::TimeTz` (feature=chrono) / `date_native::DuckTimeTz` | `types_roundtrip.rs` |
| TIME_NS | `DuckTimeNs` | `NaiveTime` (feature=chrono) / `date_native::DuckTimeNs` | `types_roundtrip.rs` |
| LIST | `DuckList` | `Vec<DuckValue>` | `types_roundtrip.rs` |
| ARRAY | `DuckArray` | `Vec<DuckValue>` | `types_roundtrip.rs` |
| ENUM | `DuckEnum` | `String` | `types_roundtrip.rs` |
| STRUCT | `DuckStruct` | `HashMap<String, DuckValue>` | `types_roundtrip.rs` |
| MAP | `DuckMap` | `HashMap<DuckValue, DuckValue>` | `types_roundtrip.rs` |
| UNION | `DuckUnion` | `Box<DuckValue>` (active member only — see below) | `types_roundtrip.rs` |
| UUID | `DuckUuid` | `better_duck_core::types::uuid::DuckUuid` | `types_roundtrip.rs` |
| BIT | `DuckBit` | `better_duck_core::types::bit::DuckBit` | `types_roundtrip.rs` |
| BIGNUM | `DuckBignum` | `better_duck_core::types::bignum::DuckBignum` | `types_roundtrip.rs` |

Non-chrono date/time (`Date`/`Time`/`Timestamp`/`Interval`/`DuckTimestamptz`/`DuckTimeTz`/
`DuckTimeNs`) is provided by `src/types/date_native.rs` when the `chrono` feature is off;
`src/types/date_chrono.rs` provides the same set when it's on. Only one set is compiled at a time.

## Known gaps

- **Multi-arm UNION.** `DuckUnion`'s Rust mirror is `Box<DuckValue>` — the active member's value
  only. The member name and tag index are not surfaced, and writing back a value read from a
  multi-arm union column produces a single-member union rather than round-tripping the original
  shape. See the core-level roadmap for the `DuckValue::Union` representation change this would
  require.
- **GEOMETRY / VARIANT / ANY / INTEGER_LITERAL.** Not supported — the DuckDB C API has no value
  accessor for these types (unlike UUID/BIT/BIGNUM). Reading a column of these types panics.
- **DECIMAL precision.** `DECIMAL(18,2)` round-trips through a different declared precision;
  see the core-level roadmap.

Contributors adding a new composite type should follow the pattern in `src/types/list.rs`
(core crate) — reading is already wired for every `DuckValueRef` variant; writing needs the
owned `DuckValueRef::from(DuckValue)` path so the output lifetime unifies with `Output<'b, _>`.
