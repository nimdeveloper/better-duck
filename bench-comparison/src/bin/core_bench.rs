//! `better-duck-core` side of the comparison benchmark. Links only
//! `better-duck-core` (+ its vendored `better-duck-sys` DuckDB) — see
//! `bench-comparison/src/lib.rs` for why this can't also link the `duckdb`
//! crate in the same process. Writes raw per-workload [`RawWorkload`]s to
//! `docs/benchmarks/_raw_core.json`; `run_all` merges them with
//! `reference_bench`'s output into the final report.

#![allow(missing_docs)]

use std::hint::black_box;

use bench_comparison::{
    compute_stats, out_dir, run_reps, write_raw, RawWorkload, ALLTYPE_ROWS, ANALYTICAL_ROWS,
    BULK_ROWS, COMPOSITE_ROWS, MEASURE_REPS, PREPARED_QUERIES, PRIMITIVE_ROWS, TYPE_MEASURE_REPS,
    TYPE_WARMUP_REPS, WARMUP_REPS,
};
use better_duck_core::{
    connection::Connection as CoreConn,
    error::Result as CoreResult,
    types::{
        appendable::AppendAble as CoreAppendAble, value::DuckValue, Blob as CoreBlob, DuckStruct,
        DuckUuid,
    },
    CachedStatement as CoreCachedStatement,
};
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use rust_decimal::Decimal;

fn no_workaround(
    group: &str,
    name: &str,
    description: String,
    item_count: usize,
    stats: bench_comparison::Stats,
) -> RawWorkload {
    RawWorkload {
        group: group.to_owned(),
        name: name.to_owned(),
        description,
        item_count,
        stats: Some(stats),
        workaround_actual: None,
        no_write_api: false,
    }
}

// Group 1: Primitive data types

fn bench_primitive_type<T, F>(
    name: &str,
    sql_type: &str,
    count: usize,
    gen: F,
) -> RawWorkload
where
    T: CoreAppendAble,
    F: Fn(i64) -> T,
{
    println!("    → {name} ({sql_type})");
    let ddl = format!("CREATE TABLE t (v {sql_type})");

    let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
        let mut conn = CoreConn::open_in_memory().expect("open db");
        conn.execute_batch(&ddl).expect("create table");
        {
            let mut app = conn.appender("t", "main").expect("appender");
            for i in 0..count as i64 {
                let mut v = gen(i);
                app.append(&mut v).expect("append");
            }
            app.save().expect("flush");
        }
        let result = conn.execute("SELECT v FROM t").expect("select");
        black_box(result.count());
    });
    let stats = compute_stats(samples, count, rss_b, rss_a);

    no_workaround(
        "Primitive types",
        name,
        format!("Insert + full-scan {count} rows of {sql_type}"),
        count,
        stats,
    )
}

fn bench_primitives() -> Vec<RawWorkload> {
    println!("  Primitive types …");
    let n = PRIMITIVE_ROWS;
    let mut results = vec![
        bench_primitive_type::<bool, _>("bool", "BOOLEAN", n, |i| i % 2 == 0),
        bench_primitive_type::<i8, _>("i8", "TINYINT", n, |i| (i % 127) as i8),
        bench_primitive_type::<i16, _>("i16", "SMALLINT", n, |i| (i % 30000) as i16),
        bench_primitive_type::<i32, _>("i32", "INTEGER", n, |i| i as i32),
        bench_primitive_type::<i64, _>("i64", "BIGINT", n, |i| i),
        bench_primitive_type::<i128, _>("i128", "HUGEINT", n, |i| i as i128 * 1_000_000_000_000),
        bench_primitive_type::<u8, _>("u8", "UTINYINT", n, |i| (i % 255) as u8),
        bench_primitive_type::<u16, _>("u16", "USMALLINT", n, |i| (i % 60000) as u16),
        bench_primitive_type::<u32, _>("u32", "UINTEGER", n, |i| i as u32),
        bench_primitive_type::<u64, _>("u64", "UBIGINT", n, |i| i as u64),
        bench_primitive_type::<f32, _>("f32", "FLOAT", n, |i| i as f32 * 1.5),
        bench_primitive_type::<f64, _>("f64", "DOUBLE", n, |i| i as f64 * 2.5),
        bench_primitive_type::<String, _>("String", "VARCHAR", n, |i| format!("row-{i}")),
        bench_primitive_type::<Decimal, _>("Decimal", "DECIMAL(18,4)", n, |i| {
            Decimal::new(i * 12345, 4)
        }),
        bench_primitive_type::<NaiveDate, _>("NaiveDate", "DATE", n, |i| {
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap() + ChronoDuration::days(i % 20_000)
        }),
        bench_primitive_type::<NaiveTime, _>("NaiveTime", "TIME", n, |i| {
            NaiveTime::from_hms_opt(0, 0, 0).unwrap() + ChronoDuration::seconds(i % 86_400)
        }),
        bench_primitive_type::<NaiveDateTime, _>("NaiveDateTime", "TIMESTAMP", n, |i| {
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()
                + ChronoDuration::seconds(i)
        }),
        bench_primitive_type::<ChronoDuration, _>("Duration", "INTERVAL", n, |i| {
            ChronoDuration::seconds(i)
        }),
        // UHUGEINT — both crates bind/read a bare u128 via a direct typed FFI call.
        bench_primitive_type::<u128, _>("u128 (UHUGEINT)", "UHUGEINT", n, |i| {
            i as u128 * 1_000_000_000_000
        }),
    ];

    println!("  Primitive types (hand-written) …");

    // BLOB — core wraps `Vec<u8>` in `Blob`.
    println!("    → Blob (BLOB)");
    {
        let ddl = "CREATE TABLE t (v BLOB)";
        let count = PRIMITIVE_ROWS;
        let mk_bytes = |i: i64| -> Vec<u8> { format!("blob-payload-{i:08}").into_bytes() };

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i64 {
                    let mut v = CoreBlob(mk_bytes(i));
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Primitive types",
            "Blob",
            format!("Insert + full-scan {count} rows of BLOB"),
            count,
            stats,
        ));
    }

    // UUID — core uses `DuckUuid(u128)`.
    println!("    → Uuid (UUID)");
    {
        let ddl = "CREATE TABLE t (v UUID)";
        let count = PRIMITIVE_ROWS;

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i64 {
                    let mut v = DuckUuid(i as u128 * 0x1_0000_0000_0000_0000 + i as u128);
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Primitive types",
            "Uuid",
            format!("Insert + full-scan {count} rows of UUID"),
            count,
            stats,
        ));
    }

    // TIMESTAMPTZ — core wraps `DateTime<Utc>` in `TimestampTz`.
    println!("    → TimestampTz (TIMESTAMPTZ)");
    {
        let ddl = "CREATE TABLE t (v TIMESTAMPTZ)";
        let count = PRIMITIVE_ROWS;
        let mk_dt = |i: i64| Utc.timestamp_opt(1_600_000_000 + i, 0).unwrap();

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i64 {
                    let mut v = better_duck_core::types::date_chrono::TimestampTz(mk_dt(i));
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Primitive types",
            "TimestampTz",
            format!("Insert + full-scan {count} rows of TIMESTAMPTZ"),
            count,
            stats,
        ));
    }

    results
}

// Group 2: Composite data types

fn bench_composites() -> Vec<RawWorkload> {
    println!("  Composite types …");
    let count = COMPOSITE_ROWS;
    let mut results = Vec::new();

    println!("    → List (LIST)");
    {
        let ddl = "CREATE TABLE t (v INTEGER[])";
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i32 {
                    let mut v: Vec<i32> = vec![i, i + 1, i + 2];
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Composite types",
            "List",
            format!(
                "Insert + full-scan {count} rows of INTEGER[] (LIST) — core: appender; \
                 duckdb crate has no write API for LIST, see `other_is_placeholder`"
            ),
            count,
            stats,
        ));
    }

    println!("    → Array (ARRAY)");
    {
        let ddl = "CREATE TABLE t (v INTEGER[3])";
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i32 {
                    let mut v: Box<[i32]> = vec![i, i + 1, i + 2].into_boxed_slice();
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Composite types",
            "Array",
            format!(
                "Insert + full-scan {count} rows of INTEGER[3] (ARRAY) — core: appender; \
                 duckdb crate has no write API for ARRAY, see `other_is_placeholder`"
            ),
            count,
            stats,
        ));
    }

    println!("    → Struct (STRUCT)");
    {
        let ddl = "CREATE TABLE t (v STRUCT(a INTEGER, b VARCHAR))";
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i32 {
                    let mut v = DuckStruct::new(std::collections::HashMap::from([
                        ("a".to_owned(), DuckValue::Int(i)),
                        ("b".to_owned(), DuckValue::text(format!("s-{i}"))),
                    ]));
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Composite types",
            "Struct",
            format!(
                "Insert + full-scan {count} rows of STRUCT(a INTEGER, b VARCHAR) — core: \
                 appender; duckdb crate has no write API for STRUCT, see `other_is_placeholder`"
            ),
            count,
            stats,
        ));
    }

    println!("    → Map (MAP)");
    {
        let ddl = "CREATE TABLE t (v MAP(INTEGER, VARCHAR))";
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i32 {
                    let mut v: std::collections::HashMap<i32, String> =
                        std::collections::HashMap::from([(i, format!("v-{i}"))]);
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(no_workaround(
            "Composite types",
            "Map",
            format!(
                "Insert + full-scan {count} rows of MAP(INTEGER, VARCHAR) — core: appender; \
                 duckdb crate has no write API for MAP, see `other_is_placeholder`"
            ),
            count,
            stats,
        ));
    }

    results
}

// Group 3: Operations

struct I32Row(i32);

impl CoreAppendAble for I32Row {
    fn appender_append(
        &mut self,
        appender: better_duck_core::ffi::duckdb_appender,
    ) -> CoreResult<()> {
        // SAFETY: `appender` is a valid open appender for a table with exactly one
        // INTEGER column. `begin_row` is called by `Appender::append` before us.
        unsafe { better_duck_core::ffi::duckdb_append_int32(appender, self.0) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: better_duck_core::ffi::duckdb_prepared_statement,
    ) -> CoreResult<()> {
        // SAFETY: `stmt` is a valid prepared statement; `idx` is a 1-based parameter
        // index within the statement's parameter count (caller ensures correctness).
        unsafe { better_duck_core::ffi::duckdb_bind_int32(stmt, idx, self.0) };
        Ok(())
    }
}

fn bench_crud() -> RawWorkload {
    println!("  [1/5] CRUD basics …");

    let (samples, rss_b, rss_a) = {
        let mut conn = CoreConn::open_in_memory().expect("open db");
        conn.execute_batch(
            "CREATE TABLE crud (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, val DOUBLE NOT NULL)",
        )
        .expect("create crud table");

        let mut rep = 0i32;
        run_reps(WARMUP_REPS, MEASURE_REPS, || {
            rep += 1;
            conn.execute_batch(format!(
                "INSERT INTO crud VALUES ({rep}, 'item-{rep}', {:.4});
                 UPDATE crud SET val = val + 1.0 WHERE id = {rep};
                 DELETE FROM crud WHERE id = {rep};",
                rep as f64 * 1.5
            ))
            .expect("crud batch");
            let _ = conn
                .execute(format!("SELECT id, name, val FROM crud WHERE id = {rep}"))
                .expect("select");
        })
    };
    let stats = compute_stats(samples, 4, rss_b, rss_a); // 4 ops per rep

    no_workaround(
        "Operations",
        "crud_basics",
        "Single-row INSERT + point SELECT + UPDATE + DELETE".to_owned(),
        4,
        stats,
    )
}

fn bench_bulk_ingest() -> RawWorkload {
    println!("  [2/5] Bulk ingest ({BULK_ROWS} rows) …");

    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut conn = CoreConn::open_in_memory().expect("open db");
        conn.execute_batch("CREATE TABLE bulk (v INTEGER NOT NULL)").expect("create table");
        let mut app = conn.appender("bulk", "main").expect("appender");
        for i in 0i32..BULK_ROWS as i32 {
            app.append(&mut I32Row(i)).expect("append");
        }
        app.save().expect("flush");
    });
    let stats = compute_stats(samples, BULK_ROWS, rss_b, rss_a);

    no_workaround(
        "Operations",
        "bulk_ingest",
        format!("Ingest {BULK_ROWS} rows via each library's appender API"),
        BULK_ROWS,
        stats,
    )
}

fn bench_analytical() -> RawWorkload {
    println!("  [3/5] Analytical query ({ANALYTICAL_ROWS} rows) …");

    const QUERY: &str = "SELECT category, COUNT(*) AS n, AVG(value) AS avg_val \
         FROM bench_data \
         GROUP BY category \
         ORDER BY avg_val DESC \
         LIMIT 10";
    let populate_sql = format!(
        "CREATE TABLE bench_data AS \
         SELECT (range % 10 + 1)::INTEGER AS category, \
                (range * 3.14159 % 1000.0)::DOUBLE AS value \
         FROM range({ANALYTICAL_ROWS})"
    );

    let mut core_conn = CoreConn::open_in_memory().expect("open db");
    core_conn.execute_batch(&populate_sql).expect("populate analytical table");
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let result = core_conn.execute(QUERY).expect("analytical query");
        let _count = result.count();
    });
    let stats = compute_stats(samples, ANALYTICAL_ROWS, rss_b, rss_a);

    no_workaround(
        "Operations",
        "analytical",
        format!(
            "GROUP BY + AVG + ORDER BY on {ANALYTICAL_ROWS} rows (query only; table pre-populated)"
        ),
        ANALYTICAL_ROWS,
        stats,
    )
}

fn bench_prepared_reuse() -> RawWorkload {
    println!("  [4/5] Prepared-statement reuse ({PREPARED_QUERIES} queries) …");

    let setup_sql = format!(
        "CREATE TABLE vals (v INTEGER); \
         INSERT INTO vals SELECT range FROM range({PREPARED_QUERIES})"
    );

    let mut core_conn = CoreConn::open_in_memory().expect("open db");
    core_conn.execute_batch(&setup_sql).expect("setup prepared table");
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut stmt =
            CoreCachedStatement::prepare(core_conn.db(), "SELECT v FROM vals WHERE v = $1")
                .expect("prepare");
        for i in 0i32..PREPARED_QUERIES as i32 {
            let mut row = I32Row(i);
            stmt.bind(1, &mut row).expect("bind");
            let result = stmt.execute().expect("prepared select");
            let _ = result.count();
        }
    });
    let stats = compute_stats(samples, PREPARED_QUERIES, rss_b, rss_a);

    no_workaround(
        "Operations",
        "prepared_reuse",
        format!("{PREPARED_QUERIES} point-SELECT queries reusing one prepared statement"),
        PREPARED_QUERIES,
        stats,
    )
}

fn bench_all_types() -> RawWorkload {
    println!("  [5/5] All-types scan ({ALLTYPE_ROWS} rows) …");

    const DDL: &str = "CREATE TABLE all_types ( \
        id       INTEGER, \
        b        BOOLEAN, \
        ti       TINYINT, \
        si       SMALLINT, \
        i        INTEGER, \
        bi       BIGINT, \
        f        FLOAT, \
        d        DOUBLE, \
        s        VARCHAR, \
        dt       DATE, \
        ts       TIMESTAMP \
    )";
    let insert_sql = format!(
        "INSERT INTO all_types \
         SELECT \
           range::INTEGER                      AS id, \
           (range % 2 = 0)                    AS b, \
           (range % 128)::TINYINT             AS ti, \
           range::SMALLINT                    AS si, \
           range::INTEGER                     AS i, \
           range::BIGINT                      AS bi, \
           (range * 1.1)::FLOAT               AS f, \
           (range * 2.2)::DOUBLE              AS d, \
           'str-' || range::VARCHAR           AS s, \
           DATE '2020-01-01' + range::INTEGER AS dt, \
           to_timestamp(1577836800 + range)   AS ts \
         FROM range({ALLTYPE_ROWS});"
    );

    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut conn = CoreConn::open_in_memory().expect("open db");
        conn.execute_batch(format!("{DDL}; {insert_sql}")).expect("all-types insert");
        let result = conn.execute("SELECT * FROM all_types").expect("all-types scan");
        let _ = result.count();
    });
    let stats = compute_stats(samples, ALLTYPE_ROWS, rss_b, rss_a);

    no_workaround(
        "Operations",
        "all_types",
        format!("INSERT + full-scan of {ALLTYPE_ROWS} rows across 11 column types"),
        ALLTYPE_ROWS,
        stats,
    )
}

fn main() {
    println!("=== better-duck-core benchmark (writes docs/benchmarks/_raw_core.json) ===\n");

    let mut results = Vec::new();
    results.extend(bench_primitives());
    results.extend(bench_composites());
    results.push(bench_crud());
    results.push(bench_bulk_ingest());
    results.push(bench_analytical());
    results.push(bench_prepared_reuse());
    results.push(bench_all_types());

    let out = out_dir();
    std::fs::create_dir_all(&out).expect("create docs/benchmarks");
    let path = out.join("_raw_core.json");
    write_raw(&path, &results).expect("write raw core results");
    println!("\n→ {}", path.display());
}
