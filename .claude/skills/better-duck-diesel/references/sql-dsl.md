# The DuckDB SQL DSL

Everything DuckDB-specific that `better-duck-diesel` exposes as typed Diesel DSL — operators, scalar
functions, aggregates, spatial, and calling your own UDFs — with **no raw SQL**. Bring the prelude in
alongside Diesel's:

```rust
use diesel::prelude::*;
use better_duck_diesel::dsl::*;
```

Everything below is verified against `crates/better-duck-diesel/src/expressions/**`. Each item emits real
DuckDB SQL; the Rust name usually matches the SQL name, except where a `#[sql_name]` override is noted.
Do **not** invent functions beyond this list.

## Operators (extension-trait methods)

Two traits are blanket-implemented for every expression and come in via `dsl::*`.

`DuckTextExpressionMethods` (text matching; right-hand side binds as `Text`):

| Method | SQL | Meaning |
|---|---|---|
| `.ilike(pat)` | `ILIKE` | case-insensitive `LIKE` |
| `.not_ilike(pat)` | `NOT ILIKE` | |
| `.similar_to(re)` | `SIMILAR TO` | whole-string regex match |
| `.not_similar_to(re)` | `NOT SIMILAR TO` | |
| `.glob(pat)` | `GLOB` | Unix `*`/`?`/`[…]` glob (case-sensitive) |
| `.regexp_matches(re)` | `~` | POSIX regex match (operator form) |
| `.not_regexp_matches(re)` | `!~` | negated POSIX regex |
| `.starts_with_op(pre)` | `^@` | prefix test |

`DuckExpressionMethods` (null-safe comparison):

| Method | SQL |
|---|---|
| `.is_distinct_from(x)` | `IS DISTINCT FROM` (null-safe `<>`) |
| `.is_not_distinct_from(x)` | `IS NOT DISTINCT FROM` (null-safe `=`) |

```rust
let hits: Vec<i32> = products::table
    .filter(products::name.ilike("wid%"))
    .select(products::id)
    .load(&mut conn)?;
```

> `.regexp_matches()` (an operator method producing `~`) and the free function `regexp_matches(s, pat)`
> (below) are distinct items that happen to share a name.

## Scalar functions

All are free functions. Call them in `.select(...)`, `.filter(...)`, or standalone via
`diesel::select(fn(args)).get_result(&mut conn)?`.

```rust
let n: f64 = diesel::select(sqrt(16.0_f64)).get_result(&mut conn)?; // 4.0
```

**String** (`Text -> …`): `lower`, `upper`, `length` (→`BigInt`), `trim`, `ltrim`, `rtrim`, `reverse`,
`repeat(x, n: Integer)`, `replace(s, from, to)`, `md5`, `contains(haystack, needle)` (→`Bool`),
`starts_with(s, prefix)` (→`Bool`), `ends_with(s, suffix)` (→`Bool`).

**String search / pad** : `strpos(haystack, needle)` (→`BigInt`), `instr(haystack, needle)` (→`BigInt`),
`substr(s, start: Integer, length: Integer)`, `split_part(s, sep, index: Integer)`,
`concat_ws(sep, a, b)`, `left(s, n: Integer)`, `right(s, n: Integer)`, `lpad(s, len: Integer, fill)`,
`rpad(s, len: Integer, fill)`, `ascii(s)` (→`Integer`), `chr(code: Integer)`,
`levenshtein(a, b)` (→`BigInt`), `jaccard(a, b)` (→`Double`).

**String similarity**: `hamming(a, b)` (→`BigInt`), `mismatches(a, b)` (→`BigInt`),
`damerau_levenshtein(a, b)` (→`BigInt`), `jaro_similarity(a, b)` (→`Double`),
`jaro_winkler_similarity(a, b)` (→`Double`).

**Numeric / trig** (`Double -> Double` unless noted): `abs<T>(x)` (generic, preserves type), `sqrt`,
`pow(base, exponent)`, `ceil`, `floor`, `round`, `cbrt`, `exp`, `ln`, `log2`, `log10`, `sign`, `degrees`,
`radians`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2(y, x)`, `gcd(a: BigInt, b: BigInt)` (→`BigInt`),
`lcm(a, b)` (→`BigInt`), `mod_(a, b)` (`#[sql_name = "mod"]`, →`BigInt`), `xor(a, b)` (→`BigInt`),
`bit_count(x: BigInt)` (→`DuckTinyInt`), `greatest<T>(a, b)` (generic), `least<T>(a, b)` (generic).

**Conditional** (generic over `T`): `nullif(a, b)` (→`Nullable<T>`), `ifnull(a: Nullable<T>, b)` (→`T`).

**Regex**: `regexp_matches(s, pattern)` (→`Bool`), `regexp_full_match(s, pattern)` (→`Bool`),
`regexp_replace(s, pattern, replacement)` (→`Text`), `regexp_extract(s, pattern)` (→`Text`).

**Date/time (extract)** (`Timestamp -> BigInt` unless noted): `year`, `month`, `day`, `hour`, `minute`,
`second`, `date_trunc(part: Text, ts)` (→`Timestamp`), `strftime(ts, format: Text)` (→`Text`),
`epoch(ts)` (→`Double`).

**Date/time (build & parts)**: `make_date(year, month, day: Integer)` (→`Date`),
`make_time(hour, minute: Integer, seconds: Double)` (→`Time`), `make_timestamp(micros: BigInt)` (→`Timestamp`),
`dayname(ts)` (→`Text`), `monthname(ts)` (→`Text`), `last_day(d: Date)` (→`Date`), `dayofweek(ts)` (→`BigInt`),
`isodow(ts)` (→`BigInt`), `dayofyear(ts)` (→`BigInt`), `weekofyear(ts)` (→`BigInt`), `quarter(ts)` (→`BigInt`),
`datediff(part: Text, start, finish)` (→`BigInt`). Date/time functions need the `chrono` feature for the
Rust-side mapping (or core's `date_native` without it).

**Formatting**: `concat(a, b)`, `printf(format, arg)`, `format_(format, arg)` (`#[sql_name = "format"]`).

**Hashing**: `sha256(x)` (→`Text`), `sha1(x)` (→`Text`), plus `md5` (listed under String).

**Encoding** (BLOB/base64/hex): `encode(x: Text)` (→`Binary`), `decode(b: Binary)` (→`Text`),
`base64(b: Binary)` (→`Text`), `from_base64(s: Text)` (→`Binary`), `hex(b: Binary)` (→`Text`),
`unhex(s: Text)` (→`Binary`).

**JSON**: `json_valid(x)` (→`Bool`), `json_type(x)` (→`Text`),
`json_extract_string(json, path)` (→`Nullable<Text>`), `json_array_length(x)` (→`BigInt`). Enable the
`json` feature to bundle the extension so these work offline.

**List**: `array_length(list)` (→`BigInt`), `len(list)` (→`BigInt`), `list_reverse(list)` (→`DuckList`),
`list_sort(list)` (→`DuckList`), `list_distinct(list)` (→`DuckList`), `list_concat(a, b)` (→`DuckList`),
`array_to_string(list, sep: Text)` (→`Text`). (`list` args/results typed `DuckList`.)

**Map**: `cardinality(map: DuckMap)` (→`DuckUBigInt`), `map_keys(map)` (→`DuckList`),
`map_values(map)` (→`DuckList`).

**UUID** (→`DuckUuid`): `gen_random_uuid()`, `uuidv4()`, `uuidv7()`.

**Utility**: `version()` (→`Text`), `type_of<T>(x)` (`#[sql_name = "typeof"]`, →`Text`).

## Aggregates

Also free functions (defined with `#[aggregate]`). Use in a grouped/aggregating `.select(...)`.

**General**: `bool_and(x: Bool)`, `bool_or(x: Bool)`, `string_agg(x: Text, sep: Text)`,
`arg_max(arg: Text, val: Double)`, `arg_min(arg: Text, val: Double)`, `product(x: Double)`,
`any_value(x: Text)`, `array_agg(x: Text)` (→`DuckList`). (Most return `Nullable<…>`.)

**Statistical** (`Double -> Nullable<Double>` unless noted): `stddev_samp`, `stddev_pop`, `var_samp`,
`var_pop`, `median`, `corr(y, x)`, `covar_samp(y, x)`, `covar_pop(y, x)`, `skewness`, `kurtosis`,
`entropy`, `mad`, `mode`, `regr_slope(y, x)`, `regr_intercept(y, x)`, `regr_r2(y, x)`,
`regr_count(y, x)` (→`Nullable<BigInt>`).

**Approximate**: `approx_count_distinct(x: Text)` (→`BigInt`), `approx_quantile(x: Double, pos: Float)`,
`reservoir_quantile(x: Double, q: Double)`, `quantile_cont(x: Double, pos: Double)`,
`quantile_disc(x: Double, pos: Double)`.

**Bitwise** (`BigInt -> Nullable<BigInt>`): `bit_and(x)`, `bit_or(x)`, `bit_xor(x)`.

**JSON**: `json_group_array(x: Text)` (→`Text`), `json_group_object(key: Text, value: Text)` (→`Text`).

> Diesel core already ships `count`/`sum`/`avg`/`min`/`max` — this crate adds only what core lacks.

## Spatial (`feature = "spatial"`)

**Not** in `dsl::*`. Import from the module and load the extension first. GEOMETRY values travel as WKB,
so geometry args/results are typed `Binary`.

```rust
use better_duck_diesel::spatial::{ensure_loaded, st_point, st_as_text, st_distance};

ensure_loaded(&mut conn)?; // INSTALL + LOAD the spatial extension on demand (may need network)
let wkt: String = diesel::select(st_as_text(st_point(1.0, 2.0))).get_result(&mut conn)?; // "POINT (1 2)"
```

Available: `st_point(x: Double, y: Double) -> Binary`, `st_geom_from_text(wkt: Text) -> Binary`,
`st_as_text(geom: Binary) -> Text`, `st_distance(a: Binary, b: Binary) -> Double`,
`st_area(geom: Binary) -> Double`, plus `ensure_loaded(conn)`.

`ensure_loaded` returns `Err` when the extension can't be made available (e.g. offline) — the spatial
tests skip in that case rather than fail.

## Calling your own UDFs through the DSL (`feature = "udf"`)

Register a `#[duckdb_*]` UDF (from `better-duck-core`'s `udf` feature) on the underlying core connection,
declare a matching `define_sql_function!`, then call it as a typed method — no raw SQL. Verified in
`tests/udf_dsl.rs`:

```rust
use better_duck_core::duckdb_scalar;
use better_duck_diesel::DuckDbConnection;
use diesel::expression::functions::define_sql_function;
use diesel::prelude::*;
use diesel::sql_types::Integer;

#[duckdb_scalar(name = "add_one")]
fn add_one_impl(x: i32) -> i32 { x + 1 }

define_sql_function! { fn add_one(x: Integer) -> Integer; }

let mut conn = DuckDbConnection::establish(":memory:")?;
add_one_impl::register(conn.inner_mut())?;  // register on the core connection via inner_mut()

let out: Vec<i32> = nums::table.select(add_one(nums::id)).order(nums::id).load(&mut conn)?;
```

The generated registration type is `<fn_name>_impl` when the UDF uses `name = "…"`; call `::register`
with `conn.inner_mut()`. The `define_sql_function!` name/signature must match the registered SQL name and
DuckDB types. Registration is per-connection — with a pool, register on each connection (see
`references/types-and-pooling.md`, `on_connect`). Table/aggregate UDFs follow the same pattern (aggregates
via `#[aggregate]` on `define_sql_function!`).

## Binding custom-typed values in a DSL expression

Bare `u64`/`i128`/`DuckUuid`/… can't be used directly as a DSL expression at a `Duck*` SQL type (a
third-party coherence limit). Wrap them in a `values::*` newtype and `.into_sql::<SqlType>()`:

```rust
use better_duck_diesel::values::UBigInt;
use better_duck_diesel::sql_types::DuckUBigInt;
use diesel::IntoSql;

let v = UBigInt(u64::MAX);
let got: UBigInt = diesel::select(v.into_sql::<DuckUBigInt>()).get_result(&mut conn)?;
```

Full newtype ↔ SQL-type list in `references/types-and-pooling.md`.
