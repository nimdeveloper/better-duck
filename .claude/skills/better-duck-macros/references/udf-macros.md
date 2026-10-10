# UDF macros (`feature = "udf"`)

The user-defined-function macros turn an ordinary Rust function into a DuckDB function. Parameter
and return types are inferred from the signature via `DuckLogicalType`; any type that round-trips
through `AppendAble` (every integer width, floats, `String`/`&str`, `bool`, …) works as a param or
return. No `unsafe`, no manual vector handling.

Each macro generates a `register(&mut Connection)` associated function. Registration is always on a
core `Connection` (the `udf` feature). Through better-duck-diesel's `udf` feature, register on the
underlying connection via `conn.inner_mut()`.

Common semantics across the function macros:

- **`Option<T>`** parameter or return → DuckDB `NULL`. Any `Option<T>` parameter also auto-enables
  `special_handling` (the function is still invoked for `NULL` inputs).
- **`Result<T, E>`** return → on `Err`, the query fails with `E`'s message (`E: Into<Box<dyn Error +
  Send + Sync>>`). Prefer this over panicking. The `duck_bail!("…")` macro is `return Err(format!
  (…).into())` for a fallible body.

## `#[duckdb_scalar]`

Row-wise function for a `SELECT` list or `WHERE` clause.

```rust
use better_duck_core::{connection::Connection, duckdb_scalar};

#[duckdb_scalar]
fn double_or_null(x: Option<i32>) -> Option<i32> { x.map(|v| v * 2) }

#[duckdb_scalar(name = "to_int")]
fn parse_int(s: &str) -> Result<i32, std::num::ParseIntError> { s.parse() }
```

Options (all optional):

| Option | Meaning |
|---|---|
| `name = "sql_name"` | SQL function name (defaults to the Rust fn name) |
| `crate = ::path` | retarget the generated `::better_duck_core` path |
| `volatile` | disable DuckDB's zero-argument constant-folding |
| `state(Type, init_expr)` | a shared `VScalar::State` of `Type`, initialized to `init_expr` |

With `state(...)`, read a `Clone` copy of the state inside the body with `duck_state!(Type)`:

```rust
#[duckdb_scalar(state(i64, 42))]
fn with_offset(x: i64) -> i64 {
    x + better_duck_core::duck_state!(i64)
}
```

## `#[duckdb_table_function]`

Function for a `FROM` clause. Returns `impl Iterator<Item = T> + Send` — `T` for one column, or a
tuple `(A, B, …)` for several. DuckDB pulls rows in chunks, so the iterator is driven lazily and may
be scanned across several calls.

```rust
use better_duck_core::{connection::Connection, duckdb_table_function};

#[duckdb_table_function(name = "series", columns("n"))]
fn series(start: i64, stop: i64) -> impl Iterator<Item = i64> + Send { start..stop }
```

Options:

| Option | Meaning |
|---|---|
| `name = "sql_name"` | SQL function name (defaults to the Rust fn name) |
| `crate = ::path` | retarget the generated `::better_duck_core` path |
| `columns("a", "b")` | result column names (defaults: the fn name for one column, else `column_0`, `column_1`, …) |
| `named_params("a", "b")` | register the listed fn parameters as SQL *named* (keyword) params instead of positional; a non-`Option` field becomes required, an `Option<T>` field optional |
| `projection_pushdown` | declare that the body honors `duck_projection!` to skip unneeded columns |
| `extra_info(Type, init_expr)` | shared registration-time context, read via `duck_extra_info!(Type)` |

`schema = "…"` is intentionally rejected — DuckDB's C API cannot register a function into a schema;
register it, then move it with SQL.

Inside the body:

- `duck_projection!()` → `Vec<usize>` of the 0-based column indices the current query actually
  needs (empty if no pushdown was requested). Only valid under `projection_pushdown`.
- `duck_extra_info!(Type)` → a `Clone` of the registered extra-info value (`Type: Clone`).

Both panic if called in the wrong context, but that panic is contained by the crate's callback
machinery and surfaces as a normal query error, never UB.

## `#[duckdb_aggregate]`

Applied to an **inline `mod { … }`** (not `mod name;`) defining four free functions. It generates a
`VAggregate` impl plus a `register(&mut Connection)`. The model:

- `init() -> State` — the fresh per-group accumulator; `State` is inferred from this return.
- `update(state: &mut State, …values)` — fold one row; value param types are inferred from the args
  after the leading `&mut state`. May return `Result<(), E>` to fail the query.
- `combine(target: &mut State, source: &State)` — merge partial states (parallel aggregation).
- `finalize(state: &State) -> T` — produce the result; return type `T` (or `Result<T, E>`).

```rust
use better_duck_core::duckdb_aggregate;

#[duckdb_aggregate(name = "int_sum")]
mod int_sum {
    fn init() -> i64 { 0 }
    fn update(state: &mut i64, value: i32) { *state += i64::from(value); }
    fn combine(acc: &mut i64, other: &i64) { *acc += *other; }
    fn finalize(state: &i64) -> i64 { *state }
}
```

Options: `name = "…"`, `crate = ::path`, and `special_handling` (still invoke `update` for `NULL`
inputs; off by default). The underlying trait also carries a `Shared` registration state (`()` from
the macro).

## `#[duckdb_cast]`

Applied to a one-argument `fn(source) -> target` (or `-> Result<target, E>`). The source and target
logical types are inferred from the signature; casts are unnamed, so there is no `name`.

```rust
use better_duck_core::duckdb_cast;

#[duckdb_cast]
fn parse_to_int(s: &str) -> Result<i32, std::num::ParseIntError> { s.trim().parse() }
// CAST('42' AS INTEGER) → 42 ; TRY_CAST('oops' AS INTEGER) → NULL ; CAST('oops' …) → query error
```

Options: `crate = ::path` and `implicit_cost = <int>` — the binder's implicit-cast cost (lower =
preferred; negative = explicit-only). Defaults to `-1` (explicit `CAST` only).

## `#[duckdb_entrypoint]` (experimental)

Generates the C-ABI entrypoint for a DuckDB **loadable extension** (`.duckdb_extension`), wrapping a
`fn(&mut Connection) -> Result<…>`. Requires better-duck-core's **`loadable-extension`** FFI mode
(all FFI routed through the host's dynamically-loaded API table), so the crate links no DuckDB of
its own and builds a `cdylib`.

```rust
use better_duck_core::{connection::Connection, duckdb_entrypoint, duckdb_scalar};

#[duckdb_scalar(name = "hello")]
fn hello(name: &str) -> String { format!("hello, {name}!") }

#[duckdb_entrypoint(name = "my_ext", min_duckdb_version = "v1.5.5")]
fn init(conn: &mut Connection) -> better_duck_core::error::Result<()> {
    hello::register(conn)?;
    Ok(())
}
```

Options: `name = "…"` (extension name, driving the generated `<name>_init_c_api` symbol) and
`min_duckdb_version = "v…"`. See `better_duck_core::extension`.

## Panics and `panic = "abort"`

Callback panics are caught and reported to DuckDB as a query error **only** when the crate is built
with `panic = "unwind"` (the default for `dev` and `test`). Under a `panic = "abort"` build — this
workspace's own `release` profile, for instance — a panicking UDF **aborts the process**:
`catch_unwind` cannot catch once unwinding is compiled out. Return `Err` from a fallible function
rather than panicking; the error path never depends on unwinding.

## Related

- Replacement scans (`ReplacementScan`, rewrite a bare `FROM` identifier into a table-function call)
  are an **experimental** part of the same `udf` feature, but have no attribute macro — see
  `better_duck_core::udf::replacement`.
- Calling registered UDFs through the ORM is covered by the **better-duck-diesel** skill.
