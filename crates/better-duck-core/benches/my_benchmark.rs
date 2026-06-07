#![allow(missing_docs)]
//! Benchmarks for `better-duck-core` core operations.

use better_duck_core::ffi::{
    duckdb_append_int32, duckdb_appender, duckdb_bind_int32, duckdb_prepared_statement,
};
use better_duck_core::{connection::Connection, types::appendable::AppendAble};
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

/// A minimal two-integer row used for appender benchmarks.
struct BenchRow(i32, i32);

impl AppendAble for BenchRow {
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> better_duck_core::error::Result<()> {
        // SAFETY: `appender` is a valid open appender for a table with two INTEGER columns.
        // `begin_row` has been called by `Appender::append` before this method is invoked.
        unsafe {
            duckdb_append_int32(appender, self.0);
            duckdb_append_int32(appender, self.1);
        }
        Ok(())
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> better_duck_core::error::Result<()> {
        // SAFETY: `stmt` is a valid prepared statement; `idx` is a valid 1-based parameter index.
        unsafe { duckdb_bind_int32(stmt, idx, self.0) };
        Ok(())
    }
}

/// Benchmark iterating 1 000 rows returned by a SELECT.
fn bench_query_1000_rows(c: &mut Criterion) {
    let mut conn = Connection::open_in_memory().expect("in-memory db");
    conn.execute_batch("CREATE TABLE t (v INTEGER)").expect("create table");
    for i in 0..1000 {
        conn.execute_batch(format!("INSERT INTO t VALUES ({i})")).expect("insert");
    }
    c.bench_function("query_1000_rows", |b| {
        b.iter(|| {
            let result = conn.execute("SELECT v FROM t").expect("select");
            let count = result.count();
            black_box(count);
        });
    });
}

/// Benchmark executing a parameterised SELECT 100 times with different bindings.
fn bench_execute_with_param(c: &mut Criterion) {
    let mut conn = Connection::open_in_memory().expect("in-memory db");
    conn.execute_batch("CREATE TABLE t (v INTEGER)").expect("create table");
    for i in 0i32..100 {
        conn.execute_batch(format!("INSERT INTO t VALUES ({i})")).expect("insert");
    }
    c.bench_function("execute_with_param_100x", |b| {
        b.iter(|| {
            for i in 0i32..100 {
                let mut val = black_box(i);
                let result = conn
                    .execute_with(
                        "SELECT v FROM t WHERE v = $1",
                        &mut [&mut val as &mut dyn AppendAble],
                    )
                    .expect("select");
                black_box(result.count());
            }
        });
    });
}

/// Benchmark bulk-inserting 10 000 rows via the DuckDB appender API.
fn bench_appender_10k_rows(c: &mut Criterion) {
    c.bench_function("appender_10k_rows", |b| {
        b.iter(|| {
            let mut conn = Connection::open_in_memory().expect("in-memory db");
            conn.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER)").expect("create table");
            let mut appender = conn.appender("t", "main").expect("appender");
            for i in 0i32..10_000 {
                let mut row = BenchRow(i, i * 2);
                appender.append(&mut row).expect("append row");
            }
            appender.save().expect("flush appender");
            black_box(conn);
        });
    });
}

criterion_group!(benches, bench_query_1000_rows, bench_execute_with_param, bench_appender_10k_rows);
criterion_main!(benches);
