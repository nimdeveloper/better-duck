---
name: better-duck-macros
description: >-
  Proc-macro surface of the better-duck-macros crate — DuckDB user-defined functions and data
  derives — consumed through better-duck-core's `feature = "udf"` and `feature = "derive"` (and
  better-duck-diesel's `derive`), never by depending on better-duck-macros directly. Use when
  writing or registering a UDF with `#[duckdb_scalar]`, `#[duckdb_table_function]`,
  `#[duckdb_aggregate]`, `#[duckdb_cast]`, or `#[duckdb_entrypoint]`; when mapping Rust types with
  `#[derive(FromRow|ToRow|DuckEnum|DuckStruct)]`; when reaching for the `transaction!` / `params!`
  helper macros; or when choosing/debugging their attribute options (`name`, `columns`,
  `named_params`, `projection_pushdown`, `state`, `volatile`, `rename_all`).
license: MIT OR Apache-2.0
metadata:
  author: Hamid Reza Shakibi
  version: "0.1.0"
---

# better-duck-macros

`better-duck-macros` is the proc-macro engine behind two better-duck capabilities: **user-defined
functions** (register plain Rust fns as DuckDB scalar / table / aggregate / cast functions) and
**data derives** (map Rust structs and enums to DuckDB rows, `STRUCT`s, and `ENUM`s). Neither needs
`unsafe` or manual vector handling.

> You do **not** depend on `better-duck-macros` directly. The macros are re-exported through
> **better-duck-core**'s `udf` and `derive` features (and **better-duck-diesel**'s `derive` feature
> for ORM `FromSql`/`ToSql` emission). Import everything from `better_duck_core::…`.

```toml
[dependencies]
better-duck-core = { version = "0.1.0-beta.8", features = ["udf", "derive"] }
```

For the DuckDB engine semantics these compile against see the **duckdb** skill; for the driver API
(connections, `DuckValue`, appender, columnar) see **better-duck-core**; this skill is the overview
and router for the macros themselves.

## Quickstart

A scalar UDF (feature `udf`) — parameter/return types inferred from the signature, registered on a
`Connection`:

```rust
use better_duck_core::{connection::Connection, duckdb_scalar};

#[duckdb_scalar]
fn repeat_str(s: &str, n: i32) -> String {
    s.repeat(n.max(0) as usize)
}

# fn main() -> better_duck_core::error::Result<()> {
let mut conn = Connection::open_in_memory()?;
repeat_str::register(&mut conn)?;          // generated associated fn
conn.execute("SELECT repeat_str('ab', 3)")?; // "ababab"
# Ok(())
# }
```

A row-mapping derive (feature `derive`) — read a result straight into a `Vec<T>`:

```rust
use better_duck_core::FromRow;

#[derive(FromRow, Debug)]
struct Task { id: i32, name: String }

# fn demo(conn: &mut better_duck_core::connection::Connection) -> better_duck_core::error::Result<()> {
let tasks: Vec<Task> = conn.execute("SELECT id, name FROM tasks")?.materialize()?.to_structs()?;
# Ok(())
# }
```

## The macros at a glance

**User-defined functions** (feature `udf`) — details in `references/udf-macros.md`:

| Macro | Applies to | Becomes |
|---|---|---|
| `#[duckdb_scalar]` | `fn(args) -> T` | a scalar function (one value per row, for `SELECT`/`WHERE`) |
| `#[duckdb_table_function]` | `fn(args) -> impl Iterator<Item = T> + Send` | a table function (rows + columns, usable in `FROM`) |
| `#[duckdb_aggregate]` | `mod { init, update, combine, finalize }` | a custom aggregate |
| `#[duckdb_cast]` | `fn(source) -> target` | a `CAST` / `TRY_CAST` implementation |
| `#[duckdb_entrypoint]` | `fn(&mut Connection) -> Result<…>` | a loadable-extension C entrypoint (experimental) |

Each UDF macro generates a `register(&mut Connection)` associated item. `Option<T>` propagates
`NULL`, `Result<T, E>` fails the query with `E`'s message, and panics are contained **only** under
`panic = "unwind"` — see the panic caveat in the reference.

**Data derives + helper macros** (feature `derive`) — details in `references/derives.md`:

| Item | Maps / does | Direction |
|---|---|---|
| `#[derive(FromRow)]` | struct ← query row (by column name) | read |
| `#[derive(ToRow)]` | struct → appender row / bound params (declaration order) | write |
| `#[derive(DuckEnum)]` | unit enum ↔ DuckDB `ENUM` | read + write |
| `#[derive(DuckStruct)]` | struct ↔ DuckDB `STRUCT` value | read + write |
| `transaction!(conn, { … })` | commit-on-`Ok` / rollback-on-`Err`-or-panic scope | — |
| `params![a, b, …]` | builds the `&mut [&mut dyn AppendAble]` slice for `execute_with` | — |

Field/variant naming is controlled by `#[duck(rename / rename_all)]` (and `#[duck_enum(rename /
rename_all)]` for `DuckEnum`); a `#[duck(crate = ::path)]` / `#[duck_enum(crate = ::path)]` escape
hatch retargets the generated `::better_duck_core` path.

## Routing

- **Writing or registering a UDF**, or picking attribute options (`name`, `columns`,
  `named_params`, `projection_pushdown`, `state`/`duck_state!`, `extra_info`/`duck_extra_info!`,
  `volatile`, `implicit_cost`), the `VAggregate` model, custom casts, loadable extensions, or the
  panic / `panic = "abort"` caveat → **`references/udf-macros.md`**.
- **Deriving row/struct/enum mappings**, their `#[duck(...)]` / `#[duck_enum(...)]` attributes, the
  `rename_all` casings, Diesel emission, or the `transaction!` / `params!` helpers →
  **`references/derives.md`**.

## Picking the right macro

- One value per row in a `SELECT`/`WHERE` → `#[duckdb_scalar]`.
- Rows for a `FROM` clause → `#[duckdb_table_function]`.
- Fold many rows into one per group → `#[duckdb_aggregate]`.
- A `CAST` / `TRY_CAST` between types → `#[duckdb_cast]`.
- Ship a `.duckdb_extension` loaded into another DuckDB → `#[duckdb_entrypoint]`.
- Read rows into structs → `#[derive(FromRow)]`; bulk-insert / bind structs → `#[derive(ToRow)]`.
- A Rust enum as a DuckDB `ENUM`, or a struct as a `STRUCT` value → `#[derive(DuckEnum)]` /
  `#[derive(DuckStruct)]`.
