//! `duckdb` crate (reference) side of the comparison benchmark. Links only
//! the community `duckdb` crate (+ its vendored `libduckdb-sys`) — see
//! `bench-comparison/lib/src/lib.rs` for why this can't also link
//! `better-duck-core` in the same process. Writes raw per-workload
//! [`RawWorkload`]s to `docs/benchmarks/_raw_reference.json`; `run-all`
//! merges them with `core-bench`'s output into the final report.

#![allow(missing_docs)]

use std::hint::black_box;

use bench_comparison::{
    compute_stats, out_dir, run_reps, write_raw, RawWorkload, ALLTYPE_ROWS, ANALYTICAL_ROWS,
    BULK_ROWS, COMPOSITE_ROWS, MEASURE_REPS, PREPARED_QUERIES, PRIMITIVE_ROWS, TYPE_MEASURE_REPS,
    TYPE_WARMUP_REPS, WARMUP_REPS,
};
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use duckdb::{types::Value as RsValue, Connection as RsConn, ToSql as RsToSql};
use rust_decimal::Decimal;
use uuid::Uuid as RsUuid;

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
    T: RsToSql + duckdb::types::FromSql,
    F: Fn(i64) -> T,
{
    println!("    → {name} ({sql_type})");
    let ddl = format!("CREATE TABLE t (v {sql_type})");

    let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
        let conn = RsConn::open_in_memory().expect("open db");
        conn.execute_batch(&ddl).expect("create table");
        {
            let mut app = conn.appender("t").expect("appender");
            for i in 0..count as i64 {
                app.append_row((gen(i),)).expect("append");
            }
            app.flush().expect("flush");
        }
        let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
        let n = stmt
            .query_map([], |row| row.get::<_, T>(0))
            .expect("query")
            .filter_map(|r| r.ok())
            .count();
        black_box(n);
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

    // BLOB — duckdb-rs binds `Vec<u8>` directly.
    println!("    → Blob (BLOB)");
    {
        let ddl = "CREATE TABLE t (v BLOB)";
        let count = PRIMITIVE_ROWS;
        let mk_bytes = |i: i64| -> Vec<u8> { format!("blob-payload-{i:08}").into_bytes() };

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t").expect("appender");
                for i in 0..count as i64 {
                    app.append_row((mk_bytes(i),)).expect("append");
                }
                app.flush().expect("flush");
            }
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
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

    // UUID — duckdb-rs uses `uuid::Uuid`.
    println!("    → Uuid (UUID)");
    {
        let ddl = "CREATE TABLE t (v UUID)";
        let count = PRIMITIVE_ROWS;

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t").expect("appender");
                for i in 0..count as i64 {
                    let bytes = (i as u128 * 0x1_0000_0000_0000_0000 + i as u128).to_be_bytes();
                    let v = RsUuid::from_bytes(bytes);
                    app.append_row((v,)).expect("append");
                }
                app.flush().expect("flush");
            }
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, RsUuid>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
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

    // TIMESTAMPTZ — duckdb-rs binds `chrono::DateTime<Utc>` bare.
    println!("    → TimestampTz (TIMESTAMPTZ)");
    {
        let ddl = "CREATE TABLE t (v TIMESTAMPTZ)";
        let count = PRIMITIVE_ROWS;
        let mk_dt = |i: i64| Utc.timestamp_opt(1_600_000_000 + i, 0).unwrap();

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t").expect("appender");
                for i in 0..count as i64 {
                    app.append_row((mk_dt(i),)).expect("append");
                }
                app.flush().expect("flush");
            }
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, chrono::DateTime<Utc>>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
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
//
// The `duckdb` crate (v1.10505.0) has no safe API to *write* LIST/ARRAY/
// STRUCT/MAP — neither `Appender::append_row` nor prepared-statement binding
// accept `Value::List`/`Array`/`Struct`/`Map` (confirmed via its own source:
// both fall through to "binding/appending ... is not yet supported").
// Populated via a SQL literal instead; `no_write_api: true` tells `run-all`
// to plot a neutral placeholder rather than this fundamentally-different
// (and much faster) vectorized-bulk-insert timing as if it were comparable.

fn bench_composites() -> Vec<RawWorkload> {
    println!("  Composite types …");
    let count = COMPOSITE_ROWS;
    let mut results = Vec::new();

    println!("    → List (LIST)");
    {
        let ddl = "CREATE TABLE t (v INTEGER[])";
        let insert_sql = format!(
            "INSERT INTO t SELECT [range::INTEGER, range::INTEGER + 1, range::INTEGER + 2] \
             FROM range({count})"
        );
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            conn.execute_batch(&insert_sql).expect("insert");
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, RsValue>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(RawWorkload {
            group: "Composite types".to_owned(),
            name: "List".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of INTEGER[] (LIST) — core: appender; \
                 duckdb crate has no write API for LIST, see `other_is_placeholder`"
            ),
            item_count: count,
            stats: None,
            workaround_actual: Some(stats),
            no_write_api: true,
        });
    }

    println!("    → Array (ARRAY)");
    {
        let ddl = "CREATE TABLE t (v INTEGER[3])";
        let insert_sql = format!(
            "INSERT INTO t SELECT [range::INTEGER, range::INTEGER + 1, range::INTEGER + 2] \
             FROM range({count})"
        );
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            conn.execute_batch(&insert_sql).expect("insert");
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, RsValue>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(RawWorkload {
            group: "Composite types".to_owned(),
            name: "Array".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of INTEGER[3] (ARRAY) — core: appender; \
                 duckdb crate has no write API for ARRAY, see `other_is_placeholder`"
            ),
            item_count: count,
            stats: None,
            workaround_actual: Some(stats),
            no_write_api: true,
        });
    }

    println!("    → Struct (STRUCT)");
    {
        let ddl = "CREATE TABLE t (v STRUCT(a INTEGER, b VARCHAR))";
        let insert_sql = format!(
            "INSERT INTO t SELECT struct_pack(a := range::INTEGER, b := 's-' || range::VARCHAR) \
             FROM range({count})"
        );
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            conn.execute_batch(&insert_sql).expect("insert");
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, RsValue>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(RawWorkload {
            group: "Composite types".to_owned(),
            name: "Struct".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of STRUCT(a INTEGER, b VARCHAR) — core: \
                 appender; duckdb crate has no write API for STRUCT, see `other_is_placeholder`"
            ),
            item_count: count,
            stats: None,
            workaround_actual: Some(stats),
            no_write_api: true,
        });
    }

    println!("    → Map (MAP)");
    {
        let ddl = "CREATE TABLE t (v MAP(INTEGER, VARCHAR))";
        let insert_sql = format!(
            "INSERT INTO t SELECT map([range::INTEGER], ['v-' || range::VARCHAR]) \
             FROM range({count})"
        );
        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            conn.execute_batch(&insert_sql).expect("insert");
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, RsValue>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
        });
        let stats = compute_stats(samples, count, rss_b, rss_a);
        results.push(RawWorkload {
            group: "Composite types".to_owned(),
            name: "Map".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of MAP(INTEGER, VARCHAR) — core: appender; \
                 duckdb crate has no write API for MAP, see `other_is_placeholder`"
            ),
            item_count: count,
            stats: None,
            workaround_actual: Some(stats),
            no_write_api: true,
        });
    }

    results
}

// Group 3: Operations

fn bench_crud() -> RawWorkload {
    println!("  [1/5] CRUD basics …");

    let (samples, rss_b, rss_a) = {
        let conn = RsConn::open_in_memory().expect("open db");
        conn.execute_batch(
            "CREATE TABLE crud (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, val DOUBLE NOT NULL)",
        )
        .expect("create crud table");

        let mut rep = 0i32;
        run_reps(WARMUP_REPS, MEASURE_REPS, || {
            rep += 1;
            conn.execute_batch(&format!(
                "INSERT INTO crud VALUES ({rep}, 'item-{rep}', {:.4});
                 UPDATE crud SET val = val + 1.0 WHERE id = {rep};
                 DELETE FROM crud WHERE id = {rep};",
                rep as f64 * 1.5
            ))
            .expect("crud batch");
            let mut stmt = conn
                .prepare(&format!("SELECT id, name, val FROM crud WHERE id = {rep}"))
                .expect("prepare select");
            let n = stmt.query([]).expect("select").mapped(|_| Ok(())).count();
            black_box(n);
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
        let conn = RsConn::open_in_memory().expect("open db");
        conn.execute_batch("CREATE TABLE bulk (v INTEGER NOT NULL)").expect("create table");
        let mut app = conn.appender("bulk").expect("appender");
        for i in 0i32..BULK_ROWS as i32 {
            app.append_row((i,)).expect("append");
        }
        app.flush().expect("flush");
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

    let rs_conn = RsConn::open_in_memory().expect("open db");
    rs_conn.execute_batch(&populate_sql).expect("populate analytical table");
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut stmt = rs_conn.prepare(QUERY).expect("prepare");
        let n = stmt.query([]).expect("analytical query").mapped(|_| Ok(())).count();
        black_box(n);
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

    let rs_conn = RsConn::open_in_memory().expect("open db");
    rs_conn.execute_batch(&setup_sql).expect("setup prepared table");
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut stmt = rs_conn.prepare("SELECT v FROM vals WHERE v = ?").expect("prepare");
        for i in 0i32..PREPARED_QUERIES as i32 {
            let n = stmt.query([i]).expect("prepared select").mapped(|_| Ok(())).count();
            black_box(n);
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
        let conn = RsConn::open_in_memory().expect("open db");
        conn.execute_batch(&format!("{DDL}; {insert_sql}")).expect("all-types insert");
        let mut stmt = conn.prepare("SELECT * FROM all_types").expect("prepare");
        let n = stmt.query([]).expect("all-types scan").mapped(|_| Ok(())).count();
        black_box(n);
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
    println!("=== duckdb crate benchmark (writes docs/benchmarks/_raw_reference.json) ===\n");

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
    let path = out.join("_raw_reference.json");
    write_raw(&path, &results).expect("write raw reference results");
    println!("\n→ {}", path.display());
}
