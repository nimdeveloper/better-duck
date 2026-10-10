Two kinds of function can be registered on a [`Connection`](crate::connection::Connection):

- **Scalar functions** compute one value per row, for use in a `SELECT` list
  or `WHERE` clause: `SELECT my_func(x) FROM t`.
- **Table functions** produce rows and columns, for use in a `FROM` clause:
  `SELECT * FROM my_func(1, 2)`.

The [`duckdb_scalar`] and
[`duckdb_table_function`] attribute macros turn
an ordinary Rust function into either kind without requiring any `unsafe` code
or manual vector handling. Parameter and return types are inferred from the
Rust signature via [`types::DuckLogicalType`] —
any type that already round-trips through [`AppendAble`]
(all the integer widths, floats, `String`/`&str`, `bool`, …) works here too,
with no extra type table for the macros to maintain.

# Scalar functions

```
use better_duck_core::{connection::Connection, duckdb_scalar};

/// Repeats `s` `n` times.
#[duckdb_scalar]
fn repeat_str(s: &str, n: i32) -> String {
    s.repeat(n.max(0) as usize)
}

/// `Option` parameters/returns propagate `NULL` explicitly.
#[duckdb_scalar]
fn double_or_null(x: Option<i32>) -> Option<i32> {
    x.map(|v| v * 2)
}

/// A `Result<T, E>` return fails the query with `E`'s message on `Err`.
#[duckdb_scalar(name = "to_int")]
fn parse_int(s: &str) -> Result<i32, std::num::ParseIntError> {
    s.parse()
}

# fn main() -> better_duck_core::error::Result<()> {
let mut conn = Connection::open_in_memory()?;
repeat_str::register(&mut conn)?;
parse_int::register(&mut conn)?;

let mut result = conn.execute("SELECT repeat_str('ab', 3) AS r")?;
assert_eq!(result.next().unwrap()?.get("r"), Some(&better_duck_core::types::value::DuckValue::text("ababab")));
# Ok(())
# }
```

# Table functions

The function returns `impl Iterator<Item = T> + Send` — `T` for a single
column, or a tuple `(A, B, ...)` for several. DuckDB pulls rows in chunks, so
the iterator is driven lazily and may be scanned across several calls.

```
use better_duck_core::{connection::Connection, duckdb_table_function};

/// The integers in `[start, stop)`.
#[duckdb_table_function(name = "series", columns("n"))]
fn series(start: i64, stop: i64) -> impl Iterator<Item = i64> + Send {
    start..stop
}

# fn main() -> better_duck_core::error::Result<()> {
let mut conn = Connection::open_in_memory()?;
series::register(&mut conn)?;
let mut result = conn.execute("SELECT sum(n) AS total FROM series(1, 101)")?;
assert_eq!(result.next().unwrap()?.get("total"), Some(&better_duck_core::types::value::DuckValue::HugeInt(5050)));
# Ok(())
# }
```

# Attribute options

| option | scalar | table | meaning |
|---|---|---|---|
| `name = "sql_name"` | yes | yes | the SQL function name (defaults to the Rust fn's name) |
| `crate = ::path` | yes | yes | re-export escape hatch for generated code's `::better_duck_core` path |
| `volatile` | yes | — | disables DuckDB's zero-argument constant-folding |
| `state(Type, init_expr)` | yes | — | shared `VScalar::State`, readable via [`duck_state!`] |
| `columns("a", "b")` | — | yes | result column names (defaults: the fn name for one column, `column_0`, `column_1`, … for several) |
| `named_params("a", "b")` | — | yes | binds the named fn parameters as SQL named (keyword) parameters instead of positional; a non-`Option` field becomes a required named parameter, an `Option<T>` field an optional one |
| `projection_pushdown` | — | yes | declares that this function honors [`duck_projection!`] to skip work for columns the query doesn't need |
| `extra_info(Type, init_expr)` | — | yes | shared, registration-time context, readable via [`duck_extra_info!`] |

`special_handling` (whether `NULL` inputs still invoke the function) is
inferred automatically: present whenever any parameter is `Option<T>`.

# Panics and `panic = "abort"`

Callback panics are caught and reported to DuckDB as a query error whenever
the crate is built with `panic = "unwind"` (the default for `dev` and `test`
profiles). Under a `panic = "abort"` build — this workspace's own `release`
profile, for instance — a panicking user-defined function aborts the process
instead: `catch_unwind` cannot catch anything once unwinding itself has been
compiled out. Prefer returning `Err` from a fallible user-defined function
over panicking; the ordinary error path never depends on unwinding.
