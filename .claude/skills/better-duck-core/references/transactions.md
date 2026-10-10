# Transactions

DuckDB has **no** dedicated transaction entry point in its C API, so `better-duck-core` drives
transactions with plain SQL (`BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK`) through
`Connection::execute_batch`. Two styles are offered on `Connection`.

## Closure scope (recommended)

`Connection::transaction` runs a closure inside a transaction: `BEGIN`, then `COMMIT` on `Ok` or
`ROLLBACK` on `Err` (the original error is preserved; a rollback failure never masks it).

```rust
conn.transaction(|c| {
    c.execute_batch("INSERT INTO t VALUES (1)")?;
    c.execute_with("INSERT INTO t VALUES ($1)", &mut [&mut 2_i32])?;
    Ok(())                       // Err here would roll everything back
})?;
```

Signature: `transaction<T, F: FnOnce(&mut Connection) -> Result<T>>(&mut self, f: F) -> Result<T>`.

## RAII guard

`Connection::begin_transaction` returns a `Transaction<'_>` (`better_duck_core::Transaction`) guard
that **derefs to the `Connection`** and rolls back on drop unless committed — this is what makes the
`?`-early-return path safe.

```rust
let mut tx = conn.begin_transaction()?;
tx.execute_batch("INSERT INTO t VALUES (1)")?;   // runs through the Deref to Connection
tx.commit()?;                                     // or tx.rollback()?; or just drop to roll back
```

- `Transaction::commit(self) -> Result<()>` and `rollback(self) -> Result<()>` consume the guard.
- Dropping the guard without either does a best-effort rollback.

## Low-level verbs

`Connection::begin()`, `commit()`, `rollback()` issue the raw statements directly if you need manual
control. The async facade runs these inside `with_connection` / `AsyncPool::with` — a transaction
must not span an `.await` point (see [async-pool-arrow](async-pool-arrow.md)).

## No savepoints

DuckDB supports only `BEGIN` / `COMMIT` / `ROLLBACK` / `ABORT` — **not** `SAVEPOINT` / `RELEASE` /
`ROLLBACK TO`. There is deliberately no savepoint API.

## `transaction!` macro

With the `derive` feature, `transaction!(conn, { … })` is a terser scope that commits on `Ok` and
rolls back on `Err`/panic, and works for both the core `Connection` and a Diesel `DuckDbConnection`
(both expose `transaction(|c| -> Result<_>)`). See the **`better-duck-macros`** skill.
