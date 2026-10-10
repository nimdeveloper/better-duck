# Columnar access (feature `columnar`)

Feature `columnar` (also turned on by `udf`) adds a columnar read/write path alongside the
row-oriented API: pull results as data chunks, read fixed-width columns zero-copy, and build chunks
to bulk-insert. The types re-export at the crate root: `better_duck_core::{DataChunkHandle,
VectorRef, VectorMut, FixedWidth}`.

## Contents

- [Reading results as chunks](#reading-results-as-chunks)
- [VectorRef — reading a column](#vectorref--reading-a-column)
- [Building a chunk](#building-a-chunk)
- [VectorMut — writing a column](#vectormut--writing-a-column)
- [The row-vs-chunk rule](#the-row-vs-chunk-rule)

## Reading results as chunks

`DuckResult::fetch_chunk() -> Option<Result<DataChunkHandle>>` pulls the next chunk directly,
bypassing per-row `DuckRow` materialization. `None` means the result is exhausted.

```rust
let mut result = conn.execute("SELECT n FROM range(5000) t(n)")?;
let mut total = 0_i64;
while let Some(chunk) = result.fetch_chunk() {
    let chunk = chunk?;                       // DataChunkHandle
    let n = chunk.len();                      // rows in this chunk
    let col = chunk.vector(0)?;               // VectorRef over column 0
    let data: &[i64] = col.as_slice(n)?;      // zero-copy &[i64]
    total += data.iter().copied().sum::<i64>();
}
```

`DataChunkHandle` accessors:

- `len() -> usize` / `is_empty()` — current row count. `capacity() -> usize` — DuckDB's build-time
  vector size (the max rows per chunk; **read it, never hardcode**).
- `num_columns() -> usize`, `column_type_id(idx) -> Result<DUCKDB_TYPE>`.
- `vector(idx) -> Result<VectorRef>` (read), `vector_mut(idx) -> Result<VectorMut>` (write),
  `vectors_mut() -> Result<Vec<VectorMut>>` (all columns at once — disjoint, so held together).
- Out-of-range index → `Error::InvalidColumnIndex`.

## VectorRef — reading a column

Built from `&DataChunkHandle`, so many read views coexist freely.

- `as_slice::<T: FixedWidth>(len) -> Result<&[T]>` — zero-copy slice of a fixed-width column. Checks
  the column's type id against `T`; a mismatch is `Error::ConversionError` (`TypeMismatch`). **NULL
  rows are not masked** — pair with `validity`/`is_null` when NULLs are possible.
- `validity(len) -> Option<&[u64]>` — packed validity words (bit `r % 64` of word `r / 64` set ⇒ row
  `r` is valid), or `None` when the column has no NULLs (DuckDB elides the mask).
- `is_null(row) -> bool`.
- `get::<T>(row) -> UdfResult<T>` — read one value as `T` (`ScalarArg`: the fixed-width scalars,
  `i128`/`u128`, `&str`, `String`, `Option<T>`).
- `as_duck_value(row) -> Result<DuckValue>` — decode one row dynamically, reusing the query-result
  decoder (works for any column type, not just fixed-width).

`FixedWidth` (sealed) is implemented for `bool`, `i8`/`i16`/`i32`/`i64`, `u8`/`u16`/`u32`/`u64`,
`f32`/`f64` — the types stored as a packed native array. It is **not** implemented for `i128`/`u128`
(DuckDB uses hugeint struct storage) or `String`/`&str` (variable width); read those with
`get`/`as_duck_value` per row instead of `as_slice`.

## Building a chunk

Allocate with column `LogicalType`s, write each column, set the row count, then hand it to the
appender (`append_chunk_handle`, see [appender](appender.md)).

```rust
use better_duck_core::{types::LogicalType, DataChunkHandle};

conn.execute_batch("CREATE TABLE dst (a INTEGER, b DOUBLE)")?;
let types = [LogicalType::of::<i32>()?, LogicalType::of::<f64>()?];
let mut chunk = DataChunkHandle::new(&types)?;       // owned; destroyed on drop

for row in 0..3usize {
    chunk.vector_mut(0)?.set(row, row as i32)?;
    chunk.vector_mut(1)?.set(row, row as f64 * 1.5)?;
}
chunk.set_len(3)?;                                   // mark 3 valid rows (<= capacity())

let mut app = conn.appender("dst", "main")?;
app.append_chunk_handle(&chunk)?;                    // strict shape check vs the table
app.save()?;

chunk.reset();                                       // zero rows, reuse the allocation
```

- `DataChunkHandle::new(&[LogicalType]) -> Result<Self>` — one logical type per column.
- `set_len(n) -> Result<()>` — number of valid rows; errors if `n > capacity()`.
- `reset()` — clear to zero rows to refill and append again (panics on a borrowed chunk, i.e. one
  from a DuckDB callback frame rather than `new`).

## VectorMut — writing a column

Built from `&mut DataChunkHandle`, so the borrow checker guarantees it is the only live view of that
column (closing an aliasing hole present in some other DuckDB bindings).

- `set::<T: ScalarRet>(row, value) -> UdfResult<()>` — write one value (`ScalarRet`: the fixed-width
  scalars, `i128`/`u128`, `&str`, `String`, `Option<T>` where `None` writes NULL).
- `set_null(row)` — mark the row NULL.
- `set_row_valid(row)` — the inverse (un-mask a row when reusing a chunk).
- `set_row_validity(row, valid: bool)` — set NULL/valid in one call.

## The row-vs-chunk rule

A `DuckResult` is consumed **either** row-by-row **or** chunk-by-chunk, never both — they share the
same underlying result cursor. The first method that touches it fixes the mode:

- A row-by-row call (`next`/`exists`/`count`/`materialize`) **after** `fetch_chunk` → the row call
  errors with `Error::MixedIterationMode`.
- `fetch_chunk` **after** any row-by-row call → `Some(Err(Error::MixedIterationMode))`.

Pick one mode per result. (`Error::MixedIterationMode` lives in
[errors-and-extensions](errors-and-extensions.md).)
