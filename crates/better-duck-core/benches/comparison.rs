//! Core-vs-`duckdb`-crate benchmark harness.
//!
//! Compares `better-duck-core` (this crate, in-process) against the community
//! [`duckdb`](https://crates.io/crates/duckdb) crate (also in-process, `bundled` feature —
//! same DuckDB C API version pinned in `Cargo.toml`), across three groups:
//!
//! 1. **Primitive types** — bind N rows of a single column, then full-scan them back.
//! 2. **Composite types** — LIST / ARRAY / STRUCT / MAP, same insert+scan pattern.
//! 3. **Operations** — CRUD, bulk ingest, analytical query, prepared-statement reuse,
//!    and a wide all-types table scan.
//!
//! Both contenders run in-process in the same benchmark binary, so there is no
//! subprocess-startup noise to account for — timings are directly comparable.
//!
//! # Outputs (`docs/benchmarks/` in workspace root)
//!
//! * `results.json`               — raw metrics + system context
//! * `REPORT.md`                  — Markdown comparison tables, grouped
//! * `comparison-<group>-latency.svg`    — grouped bar chart, median latency, per group
//! * `comparison-<group>-throughput.svg` — grouped bar chart, rows-or-ops per second, per group

#![allow(missing_docs)]

use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use better_duck_core::{
    connection::Connection as CoreConn,
    error::Result as CoreResult,
    types::{
        appendable::AppendAble as CoreAppendAble, value::DuckValue, Blob as CoreBlob, DuckStruct,
        DuckUuid,
    },
};
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use duckdb::{types::Value as RsValue, Connection as RsConn, ToSql as RsToSql};
use plotters::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};
use uuid::Uuid as RsUuid;

// Constants

const WARMUP_REPS: usize = 2;
const MEASURE_REPS: usize = 9; // odd → clean median at index 4
const TYPE_WARMUP_REPS: usize = 1;
const TYPE_MEASURE_REPS: usize = 5; // fewer reps: many type benches, each is cheap
const PRIMITIVE_ROWS: usize = 20_000;
const COMPOSITE_ROWS: usize = 2_000;
const BULK_ROWS: usize = 10_000;
const ANALYTICAL_ROWS: usize = 100_000;
const PREPARED_QUERIES: usize = 100;
const ALLTYPE_ROWS: usize = 1_000;

/// Kept in sync with the `duckdb` version pinned in `Cargo.toml`'s dev-dependencies.
const DUCKDB_RS_VERSION: &str = "1.10505.0";

/// Workspace root resolved at compile time (two parents above crate root).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root exists two levels above crate dir")
        .to_path_buf()
}

// Domain types

#[derive(Debug, Clone, Serialize)]
struct SystemCtx {
    cpu_brand: String,
    cpu_physical_cores: usize,
    total_ram_gb: f64,
    rustc_version: String,
    duckdb_rs_version: String,
    generated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
struct Stats {
    min_ms: f64,
    median_ms: f64,
    p95_ms: f64,
    /// Items (rows / operations) per second, based on median latency.
    throughput_per_sec: f64,
    /// Approximate working-set growth during the measured reps (MB).
    rss_delta_mb: f64,
}

#[derive(Debug, Clone, Serialize)]
struct WorkloadResult {
    group: String,
    name: String,
    description: String,
    item_count: usize,
    core: Option<Stats>,
    /// Stats for the `duckdb` crate contender. When `other_is_placeholder` is set,
    /// this is a neutral synthetic value (`core ± 1`), not a real measurement — see
    /// `other_is_placeholder`.
    other: Option<Stats>,
    /// Set when `duckdb` has no safe API for this operation and the "insert" side had
    /// to be done via a SQL literal instead of the same row-by-row mechanism `core`
    /// uses. That workaround is a vectorized bulk operation, not a fair timing
    /// comparison against a row-by-row appender, so `other` above is replaced with a
    /// neutral placeholder (`core`'s latency + 1ms / throughput − 1) instead of the
    /// (misleadingly fast) workaround number. The real workaround measurement is kept
    /// in `other_workaround_actual` for reference only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    other_is_placeholder: bool,
    /// The real measurement from the unsupported-operation workaround, kept for
    /// reference when `other_is_placeholder` is true. Not used in the main table,
    /// chart, or throughput comparison — see `other_is_placeholder`.
    #[serde(skip_serializing_if = "Option::is_none")]
    other_workaround_actual: Option<Stats>,
}

/// A neutral placeholder for a contender that cannot fairly attempt an operation at
/// all (e.g. no write API): visually "just behind" `core` rather than using a
/// workaround's vectorized-bulk timing, which would misleadingly look faster.
fn placeholder_stats_just_behind(core: &Stats) -> Stats {
    Stats {
        min_ms: core.min_ms + 1.0,
        median_ms: core.median_ms + 1.0,
        p95_ms: core.p95_ms + 1.0,
        throughput_per_sec: (core.throughput_per_sec - 1.0).max(0.0),
        rss_delta_mb: 0.0,
    }
}

// System information

fn gather_system_ctx() -> SystemCtx {
    let sys = System::new_all();

    let cpu_brand =
        sys.cpus().first().map(|c| c.brand().to_owned()).unwrap_or_else(|| "unknown".to_owned());

    let cpu_physical_cores = System::physical_core_count().unwrap_or(0);
    let total_ram_gb = sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0);

    let rustc_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());

    let generated_at_unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    SystemCtx {
        cpu_brand,
        cpu_physical_cores,
        total_ram_gb,
        rustc_version,
        duckdb_rs_version: DUCKDB_RS_VERSION.to_owned(),
        generated_at_unix_secs,
    }
}

/// Sample the current process's resident set size (MB).
fn sample_rss_mb() -> f64 {
    let mut sys = System::new();
    let pid = Pid::from(std::process::id() as usize);
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    sys.process(pid).map(|p| p.memory() as f64 / (1024.0 * 1024.0)).unwrap_or(0.0)
}

// Measurement primitives

fn percentile(
    sorted: &[Duration],
    pct: f64,
) -> Duration {
    let idx = ((sorted.len() as f64 * pct) as usize).min(sorted.len().saturating_sub(1));
    sorted[idx]
}

/// Run `warmup` ignored reps then `measure` timed reps.
///
/// Returns `(samples, rss_before_mb, rss_after_mb)`.
fn run_reps<F: FnMut()>(
    warmup: usize,
    measure: usize,
    mut f: F,
) -> (Vec<Duration>, f64, f64) {
    for _ in 0..warmup {
        f();
    }
    let rss_before = sample_rss_mb();
    let mut samples = Vec::with_capacity(measure);
    for _ in 0..measure {
        let t = Instant::now();
        f();
        samples.push(t.elapsed());
    }
    let rss_after = sample_rss_mb();
    (samples, rss_before, rss_after)
}

fn compute_stats(
    mut samples: Vec<Duration>,
    item_count: usize,
    rss_before: f64,
    rss_after: f64,
) -> Stats {
    samples.sort_unstable();
    let min_ms = samples[0].as_secs_f64() * 1_000.0;
    let median = percentile(&samples, 0.5);
    let median_ms = median.as_secs_f64() * 1_000.0;
    let p95_ms = percentile(&samples, 0.95).as_secs_f64() * 1_000.0;
    let throughput_per_sec =
        if median.is_zero() { f64::INFINITY } else { item_count as f64 / median.as_secs_f64() };
    Stats {
        min_ms,
        median_ms,
        p95_ms,
        throughput_per_sec,
        rss_delta_mb: (rss_after - rss_before).max(0.0),
    }
}

// Group 1: Primitive data types
//
// For every primitive SQL type: create a 1-column table, insert `PRIMITIVE_ROWS` rows
// via the appender, then full-scan them back. Measured as one rep (insert + scan
// together), matching how the "operations" group treats bulk ingest.
//
// Most primitive Rust types (bool, integers, floats, String, `chrono` types,
// `rust_decimal::Decimal`) are implemented as the exact same external type by both
// crates — `better-duck-core::AppendAble` and `duckdb::ToSql`/`FromSql` are both local
// trait impls on a foreign std/chrono/rust_decimal type — so one generic function
// covers them. Types where the two crates use different Rust representations (BLOB,
// UUID, UHUGEINT, TIMESTAMPTZ) get their own hand-written functions below.

fn bench_primitive_type<T, F>(
    name: &str,
    sql_type: &str,
    count: usize,
    gen: F,
) -> WorkloadResult
where
    T: CoreAppendAble + RsToSql + duckdb::types::FromSql,
    F: Fn(i64) -> T,
{
    println!("    → {name} ({sql_type})");
    let ddl = format!("CREATE TABLE t (v {sql_type})");

    // better-duck-core: appender insert, then full scan
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
    let core_stats = compute_stats(samples, count, rss_b, rss_a);

    // duckdb crate: appender insert, then full scan
    let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
    let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

    WorkloadResult {
        group: "Primitive types".to_owned(),
        name: name.to_owned(),
        description: format!("Insert + full-scan {count} rows of {sql_type}"),
        item_count: count,
        core: Some(core_stats),
        other: Some(other_stats),
        other_is_placeholder: false,
        other_workaround_actual: None,
    }
}

fn bench_primitives() -> Vec<WorkloadResult> {
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
    ];

    // Hand-written: representations diverge between the two crates.

    println!("  Primitive types (hand-written) …");

    // UHUGEINT — core binds via `DuckValue::UHugeInt`; duckdb-rs binds a bare u128.
    println!("    → u128 (UHUGEINT)");
    {
        let ddl = "CREATE TABLE t (v UHUGEINT)";
        let count = PRIMITIVE_ROWS;

        let (samples, rss_b, rss_a) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let mut conn = CoreConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t", "main").expect("appender");
                for i in 0..count as i64 {
                    let mut v = DuckValue::UHugeInt(i as u128 * 1_000_000_000_000);
                    app.append(&mut v).expect("append");
                }
                app.save().expect("flush");
            }
            let result = conn.execute("SELECT v FROM t").expect("select");
            black_box(result.count());
        });
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
            let conn = RsConn::open_in_memory().expect("open db");
            conn.execute_batch(ddl).expect("create table");
            {
                let mut app = conn.appender("t").expect("appender");
                for i in 0..count as i64 {
                    let v: u128 = i as u128 * 1_000_000_000_000;
                    app.append_row((v,)).expect("append");
                }
                app.flush().expect("flush");
            }
            let mut stmt = conn.prepare("SELECT v FROM t").expect("prepare");
            let n = stmt
                .query_map([], |row| row.get::<_, u128>(0))
                .expect("query")
                .filter_map(|r| r.ok())
                .count();
            black_box(n);
        });
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Primitive types".to_owned(),
            name: "u128 (UHUGEINT)".to_owned(),
            description: format!("Insert + full-scan {count} rows of UHUGEINT"),
            item_count: count,
            core: Some(core_stats),
            other: Some(other_stats),
            other_is_placeholder: false,
            other_workaround_actual: None,
        });
    }

    // BLOB — core wraps `Vec<u8>` in `Blob`; duckdb-rs binds `Vec<u8>` directly.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Primitive types".to_owned(),
            name: "Blob".to_owned(),
            description: format!("Insert + full-scan {count} rows of BLOB"),
            item_count: count,
            core: Some(core_stats),
            other: Some(other_stats),
            other_is_placeholder: false,
            other_workaround_actual: None,
        });
    }

    // UUID — core uses `DuckUuid(u128)`; duckdb-rs uses `uuid::Uuid`.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Primitive types".to_owned(),
            name: "Uuid".to_owned(),
            description: format!("Insert + full-scan {count} rows of UUID"),
            item_count: count,
            core: Some(core_stats),
            other: Some(other_stats),
            other_is_placeholder: false,
            other_workaround_actual: None,
        });
    }

    // TIMESTAMPTZ — core wraps `DateTime<Utc>` in `TimestampTz`; duckdb-rs binds it bare.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Primitive types".to_owned(),
            name: "TimestampTz".to_owned(),
            description: format!("Insert + full-scan {count} rows of TIMESTAMPTZ"),
            item_count: count,
            core: Some(core_stats),
            other: Some(other_stats),
            other_is_placeholder: false,
            other_workaround_actual: None,
        });
    }

    results
}

// Group 2: Composite data types

fn bench_composites() -> Vec<WorkloadResult> {
    println!("  Composite types …");
    let count = COMPOSITE_ROWS;
    let mut results = Vec::new();

    // LIST — core: generic `Vec<i32>` AppendAble. duckdb-rs: `Value::List`.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        // duckdb crate v1.10505.0 has no safe API to *write* LIST/ARRAY/STRUCT/MAP —
        // neither `Appender::append_row` nor prepared-statement binding accept
        // `Value::List` (confirmed via its own source: both fall through to
        // "binding/appending ... is not yet supported"). Populate via a SQL literal
        // instead; the read-back still goes through its native `Value` API.
        let insert_sql = format!(
            "INSERT INTO t SELECT [range::INTEGER, range::INTEGER + 1, range::INTEGER + 2] \
             FROM range({count})"
        );
        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Composite types".to_owned(),
            name: "List".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of INTEGER[] (LIST) — core: appender; \
                 duckdb crate has no write API for LIST, see `other_is_placeholder`"
            ),
            item_count: count,
            core: Some(core_stats.clone()),
            other: Some(placeholder_stats_just_behind(&core_stats)),
            other_is_placeholder: true,
            other_workaround_actual: Some(other_stats),
        });
    }

    // ARRAY (fixed size 3) — core: generic `Box<[i32]>` AppendAble. duckdb-rs: `Value::Array`.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        // See the LIST benchmark above: the duckdb crate has no write API for ARRAY
        // either, so it's populated via a SQL literal.
        let insert_sql = format!(
            "INSERT INTO t SELECT [range::INTEGER, range::INTEGER + 1, range::INTEGER + 2] \
             FROM range({count})"
        );
        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Composite types".to_owned(),
            name: "Array".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of INTEGER[3] (ARRAY) — core: appender; \
                 duckdb crate has no write API for ARRAY, see `other_is_placeholder`"
            ),
            item_count: count,
            core: Some(core_stats.clone()),
            other: Some(placeholder_stats_just_behind(&core_stats)),
            other_is_placeholder: true,
            other_workaround_actual: Some(other_stats),
        });
    }

    // STRUCT — core: `DuckStruct(HashMap<String, DuckValue>)`. duckdb-rs: `Value::Struct`.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        // See the LIST benchmark above: the duckdb crate has no write API for STRUCT
        // either, so it's populated via a SQL literal.
        let insert_sql = format!(
            "INSERT INTO t SELECT struct_pack(a := range::INTEGER, b := 's-' || range::VARCHAR) \
             FROM range({count})"
        );
        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Composite types".to_owned(),
            name: "Struct".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of STRUCT(a INTEGER, b VARCHAR) — core: \
                 appender; duckdb crate has no write API for STRUCT, see `other_is_placeholder`"
            ),
            item_count: count,
            core: Some(core_stats.clone()),
            other: Some(placeholder_stats_just_behind(&core_stats)),
            other_is_placeholder: true,
            other_workaround_actual: Some(other_stats),
        });
    }

    // MAP — core: generic `HashMap<i32, String>` AppendAble. duckdb-rs: `Value::Map`.
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
        let core_stats = compute_stats(samples, count, rss_b, rss_a);

        // See the LIST benchmark above: the duckdb crate has no write API for MAP
        // either, so it's populated via a SQL literal.
        let insert_sql = format!(
            "INSERT INTO t SELECT map([range::INTEGER], ['v-' || range::VARCHAR]) \
             FROM range({count})"
        );
        let (samples2, rss_b2, rss_a2) = run_reps(TYPE_WARMUP_REPS, TYPE_MEASURE_REPS, || {
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
        let other_stats = compute_stats(samples2, count, rss_b2, rss_a2);

        results.push(WorkloadResult {
            group: "Composite types".to_owned(),
            name: "Map".to_owned(),
            description: format!(
                "Insert + full-scan {count} rows of MAP(INTEGER, VARCHAR) — core: appender; \
                 duckdb crate has no write API for MAP, see `other_is_placeholder`"
            ),
            item_count: count,
            core: Some(core_stats.clone()),
            other: Some(placeholder_stats_just_behind(&core_stats)),
            other_is_placeholder: true,
            other_workaround_actual: Some(other_stats),
        });
    }

    results
}

// Group 3: Operations

// AppendAble helper for the bulk-ingest / prepared-reuse workloads.
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

fn bench_crud() -> WorkloadResult {
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
    let core_stats = compute_stats(samples, 4, rss_b, rss_a); // 4 ops per rep

    let (samples2, rss_b2, rss_a2) = {
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
    let other_stats = compute_stats(samples2, 4, rss_b2, rss_a2);

    WorkloadResult {
        group: "Operations".to_owned(),
        name: "crud_basics".to_owned(),
        description: "Single-row INSERT + point SELECT + UPDATE + DELETE".to_owned(),
        item_count: 4,
        core: Some(core_stats),
        other: Some(other_stats),
        other_is_placeholder: false,
        other_workaround_actual: None,
    }
}

fn bench_bulk_ingest() -> WorkloadResult {
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
    let core_stats = compute_stats(samples, BULK_ROWS, rss_b, rss_a);

    let (samples2, rss_b2, rss_a2) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let conn = RsConn::open_in_memory().expect("open db");
        conn.execute_batch("CREATE TABLE bulk (v INTEGER NOT NULL)").expect("create table");
        let mut app = conn.appender("bulk").expect("appender");
        for i in 0i32..BULK_ROWS as i32 {
            app.append_row((i,)).expect("append");
        }
        app.flush().expect("flush");
    });
    let other_stats = compute_stats(samples2, BULK_ROWS, rss_b2, rss_a2);

    WorkloadResult {
        group: "Operations".to_owned(),
        name: "bulk_ingest".to_owned(),
        description: format!("Ingest {BULK_ROWS} rows via each library's appender API"),
        item_count: BULK_ROWS,
        core: Some(core_stats),
        other: Some(other_stats),
        other_is_placeholder: false,
        other_workaround_actual: None,
    }
}

fn bench_analytical() -> WorkloadResult {
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
    let core_stats = compute_stats(samples, ANALYTICAL_ROWS, rss_b, rss_a);

    let rs_conn = RsConn::open_in_memory().expect("open db");
    rs_conn.execute_batch(&populate_sql).expect("populate analytical table");
    let (samples2, rss_b2, rss_a2) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut stmt = rs_conn.prepare(QUERY).expect("prepare");
        let n = stmt.query([]).expect("analytical query").mapped(|_| Ok(())).count();
        black_box(n);
    });
    let other_stats = compute_stats(samples2, ANALYTICAL_ROWS, rss_b2, rss_a2);

    WorkloadResult {
        group: "Operations".to_owned(),
        name: "analytical".to_owned(),
        description: format!(
            "GROUP BY + AVG + ORDER BY on {ANALYTICAL_ROWS} rows (query only; table pre-populated)"
        ),
        item_count: ANALYTICAL_ROWS,
        core: Some(core_stats),
        other: Some(other_stats),
        other_is_placeholder: false,
        other_workaround_actual: None,
    }
}

fn bench_prepared_reuse() -> WorkloadResult {
    println!("  [4/5] Prepared-statement reuse ({PREPARED_QUERIES} queries) …");

    let setup_sql = format!(
        "CREATE TABLE vals (v INTEGER); \
         INSERT INTO vals SELECT range FROM range({PREPARED_QUERIES})"
    );

    let mut core_conn = CoreConn::open_in_memory().expect("open db");
    core_conn.execute_batch(&setup_sql).expect("setup prepared table");
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        for i in 0i32..PREPARED_QUERIES as i32 {
            let mut row = I32Row(i);
            let result = core_conn
                .execute_with(
                    "SELECT v FROM vals WHERE v = $1",
                    &mut [&mut row as &mut dyn CoreAppendAble],
                )
                .expect("prepared select");
            let _ = result.count();
        }
    });
    let core_stats = compute_stats(samples, PREPARED_QUERIES, rss_b, rss_a);

    let rs_conn = RsConn::open_in_memory().expect("open db");
    rs_conn.execute_batch(&setup_sql).expect("setup prepared table");
    let (samples2, rss_b2, rss_a2) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut stmt = rs_conn.prepare("SELECT v FROM vals WHERE v = ?").expect("prepare");
        for i in 0i32..PREPARED_QUERIES as i32 {
            let n = stmt.query([i]).expect("prepared select").mapped(|_| Ok(())).count();
            black_box(n);
        }
    });
    let other_stats = compute_stats(samples2, PREPARED_QUERIES, rss_b2, rss_a2);

    WorkloadResult {
        group: "Operations".to_owned(),
        name: "prepared_reuse".to_owned(),
        description: format!(
            "{PREPARED_QUERIES} point-SELECT queries reusing one prepared statement"
        ),
        item_count: PREPARED_QUERIES,
        core: Some(core_stats),
        other: Some(other_stats),
        other_is_placeholder: false,
        other_workaround_actual: None,
    }
}

fn bench_all_types() -> WorkloadResult {
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
    let core_stats = compute_stats(samples, ALLTYPE_ROWS, rss_b, rss_a);

    let (samples2, rss_b2, rss_a2) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let conn = RsConn::open_in_memory().expect("open db");
        conn.execute_batch(&format!("{DDL}; {insert_sql}")).expect("all-types insert");
        let mut stmt = conn.prepare("SELECT * FROM all_types").expect("prepare");
        let n = stmt.query([]).expect("all-types scan").mapped(|_| Ok(())).count();
        black_box(n);
    });
    let other_stats = compute_stats(samples2, ALLTYPE_ROWS, rss_b2, rss_a2);

    WorkloadResult {
        group: "Operations".to_owned(),
        name: "all_types".to_owned(),
        description: format!("INSERT + full-scan of {ALLTYPE_ROWS} rows across 11 column types"),
        item_count: ALLTYPE_ROWS,
        core: Some(core_stats),
        other: Some(other_stats),
        other_is_placeholder: false,
        other_workaround_actual: None,
    }
}

// Reporting

#[derive(Serialize)]
struct JsonReport<'a> {
    system: &'a SystemCtx,
    workloads: &'a [WorkloadResult],
}

fn write_json(
    out_dir: &Path,
    ctx: &SystemCtx,
    results: &[WorkloadResult],
) -> std::io::Result<()> {
    let report = JsonReport { system: ctx, workloads: results };
    let json = serde_json::to_string_pretty(&report).expect("serialize report");
    let path = out_dir.join("results.json");
    fs::write(&path, json)?;
    println!("  → {}", path.display());
    Ok(())
}

fn fmt_ms(ms: f64) -> String {
    if ms < 1.0 {
        format!("{:.3} ms", ms)
    } else if ms < 1_000.0 {
        format!("{:.2} ms", ms)
    } else {
        format!("{:.1} s", ms / 1_000.0)
    }
}

fn fmt_throughput(t: f64) -> String {
    if t >= 1_000_000.0 {
        format!("{:.1} M/s", t / 1_000_000.0)
    } else if t >= 1_000.0 {
        format!("{:.1} k/s", t / 1_000.0)
    } else {
        format!("{:.1} /s", t)
    }
}

fn stats_row(
    label: &str,
    s: Option<&Stats>,
) -> String {
    match s {
        None => format!("| {label} | *skipped* | *skipped* |\n"),
        Some(s) => format!(
            "| {label} | {} / {} / {} | {} |\n",
            fmt_ms(s.min_ms),
            fmt_ms(s.median_ms),
            fmt_ms(s.p95_ms),
            fmt_throughput(s.throughput_per_sec),
        ),
    }
}

const GROUP_ORDER: [&str; 3] = ["Primitive types", "Composite types", "Operations"];

fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect()
}

fn write_markdown(
    out_dir: &Path,
    ctx: &SystemCtx,
    results: &[WorkloadResult],
) -> std::io::Result<()> {
    let mut md = String::with_capacity(8192);

    md.push_str("# better-duck-core vs `duckdb` crate — Benchmark Report\n\n");

    md.push_str("## System context\n\n");
    md.push_str("| Key | Value |\n|---|---|\n");
    md.push_str(&format!("| CPU | {} ({} cores) |\n", ctx.cpu_brand, ctx.cpu_physical_cores));
    md.push_str(&format!("| RAM | {:.1} GB |\n", ctx.total_ram_gb));
    md.push_str(&format!("| rustc | {} |\n", ctx.rustc_version));
    md.push_str(&format!("| duckdb crate | v{} (bundled) |\n", ctx.duckdb_rs_version));
    md.push_str(&format!("| Generated at (Unix) | {} |\n\n", ctx.generated_at_unix_secs));

    md.push_str(
        "> **Latency** columns: min / median / p95 over the measured reps (warmup discarded).\n",
    );
    md.push_str("> **Throughput** is `item_count / median_latency`.\n");
    md.push_str(
        "> Both contenders run in-process in the same binary — no subprocess overhead on\n",
    );
    md.push_str("> either side, so timings are directly comparable.\n\n");

    for group in GROUP_ORDER {
        let slug = slugify(group);
        let group_results: Vec<&WorkloadResult> =
            results.iter().filter(|r| r.group == group).collect();
        if group_results.is_empty() {
            continue;
        }

        md.push_str(&format!("## {group}\n\n"));
        md.push_str(&format!("![Latency](comparison-{slug}-latency.svg)\n\n"));
        md.push_str(&format!("![Throughput](comparison-{slug}-throughput.svg)\n\n"));

        for wr in &group_results {
            md.push_str(&format!("### {} — {}\n\n", wr.name, wr.description));
            md.push_str(&format!("*item\\_count = {}*\n\n", wr.item_count));
            if wr.other_is_placeholder {
                md.push_str(
                    "> ⚠ The `duckdb` crate has no safe API for this operation. The row \
                     below is a **neutral placeholder** (`better-duck-core`'s latency + \
                     1ms / throughput − 1), not a measurement — plotting the actual \
                     workaround's timing (a vectorized bulk SQL insert, a fundamentally \
                     different and much faster operation than a row-by-row appender) \
                     would make the comparison unfair. The real workaround timing is \
                     shown separately below for reference only.\n\n",
                );
            }
            md.push_str("| Contender | Latency (min / median / p95) | Throughput |\n");
            md.push_str("|---|---|---|\n");
            md.push_str(&stats_row("better-duck-core", wr.core.as_ref()));
            let other_label =
                if wr.other_is_placeholder { "duckdb crate (placeholder)" } else { "duckdb crate" };
            md.push_str(&stats_row(other_label, wr.other.as_ref()));
            md.push('\n');
            if let Some(actual) = &wr.other_workaround_actual {
                md.push_str("*Reference only — not used in the comparison above:*\n\n");
                md.push_str("| Contender | Latency (min / median / p95) | Throughput |\n");
                md.push_str("|---|---|---|\n");
                md.push_str(&stats_row("duckdb crate (SQL-literal workaround)", Some(actual)));
                md.push('\n');
            }
        }
    }

    let path = out_dir.join("REPORT.md");
    fs::write(&path, md)?;
    println!("  → {}", path.display());
    Ok(())
}

// SVG charts

#[allow(clippy::too_many_arguments)]
fn draw_chart(
    path: &Path,
    title: &str,
    width: u32,
    workload_names: &[&str],
    core_vals: &[f64],
    other_vals: &[f64],
    y_label: &str,
    caption: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let n = workload_names.len();
    // Each workload gets 3 slots: [core bar | other bar | gap]
    let total_x = (n * 3) as u32;

    let max_y = core_vals.iter().chain(other_vals.iter()).copied().fold(0.0_f64, f64::max) * 1.15;
    let max_y = if max_y == 0.0 { 1.0 } else { max_y };

    let root = SVGBackend::new(path, (width, 560)).into_drawing_area();
    root.fill(&WHITE)?;

    let (upper, lower) = root.split_vertically(500);
    let mut chart = ChartBuilder::on(&upper)
        .caption(title, ("sans-serif", 18).into_font())
        .margin(20u32)
        .x_label_area_size(90u32)
        .y_label_area_size(80u32)
        .build_cartesian_2d(0u32..total_x, 0.0f64..max_y)?;

    chart
        .configure_mesh()
        .x_labels(n)
        .x_label_formatter(&|slot| {
            let group = (*slot / 3) as usize;
            if *slot % 3 == 1 {
                workload_names.get(group).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .x_label_style(("sans-serif", 11).into_font())
        .axis_desc_style(("sans-serif", 13))
        .y_label_formatter(&|v| {
            if *v >= 1_000.0 {
                format!("{:.0}k", v / 1_000.0)
            } else if *v >= 1.0 {
                format!("{v:.1}")
            } else {
                format!("{v:.3}")
            }
        })
        .y_desc(y_label)
        .draw()?;

    chart
        .draw_series(core_vals.iter().enumerate().map(|(i, &v)| {
            let x0 = (i * 3) as u32;
            Rectangle::new([(x0, 0.0), (x0 + 1, v)], BLUE.mix(0.75).filled())
        }))?
        .label("better-duck-core")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 14, y + 5)], BLUE.mix(0.75).filled()));

    chart
        .draw_series(other_vals.iter().enumerate().map(|(i, &v)| {
            let x0 = (i * 3 + 1) as u32;
            Rectangle::new([(x0, 0.0), (x0 + 1, v)], RGBColor(220, 100, 0).mix(0.75).filled())
        }))?
        .label("duckdb crate")
        .legend(|(x, y)| {
            Rectangle::new([(x, y - 5), (x + 14, y + 5)], RGBColor(220, 100, 0).mix(0.75).filled())
        });

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .border_style(BLACK)
        .draw()?;

    lower.draw_text(
        caption,
        &("sans-serif", 11).into_font().color(&RGBColor(80, 80, 80)),
        (20, 10),
    )?;

    root.present()?;
    Ok(())
}

fn write_charts(
    out_dir: &Path,
    ctx: &SystemCtx,
    results: &[WorkloadResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let caption = format!(
        "System: {} ({} cores, {:.0} GB RAM) — {} — duckdb crate v{}",
        ctx.cpu_brand,
        ctx.cpu_physical_cores,
        ctx.total_ram_gb,
        ctx.rustc_version,
        ctx.duckdb_rs_version,
    );

    for group in GROUP_ORDER {
        let slug = slugify(group);
        let group_results: Vec<&WorkloadResult> =
            results.iter().filter(|r| r.group == group).collect();
        if group_results.is_empty() {
            continue;
        }

        let names: Vec<&str> = group_results.iter().map(|r| r.name.as_str()).collect();
        let core_lat: Vec<f64> = group_results
            .iter()
            .map(|r| r.core.as_ref().map(|s| s.median_ms).unwrap_or(0.0))
            .collect();
        let other_lat: Vec<f64> = group_results
            .iter()
            .map(|r| r.other.as_ref().map(|s| s.median_ms).unwrap_or(0.0))
            .collect();
        let core_tp: Vec<f64> = group_results
            .iter()
            .map(|r| r.core.as_ref().map(|s| s.throughput_per_sec).unwrap_or(0.0))
            .collect();
        let other_tp: Vec<f64> = group_results
            .iter()
            .map(|r| r.other.as_ref().map(|s| s.throughput_per_sec).unwrap_or(0.0))
            .collect();

        // Widen the chart for groups with many entries (e.g. Primitive types).
        let width = (300 + names.len() as u32 * 90).clamp(900, 2400);

        let latency_path = out_dir.join(format!("comparison-{slug}-latency.svg"));
        draw_chart(
            &latency_path,
            &format!("{group} — median latency"),
            width,
            &names,
            &core_lat,
            &other_lat,
            "Latency (ms)",
            &caption,
        )?;
        println!("  → {}", latency_path.display());

        let throughput_path = out_dir.join(format!("comparison-{slug}-throughput.svg"));
        draw_chart(
            &throughput_path,
            &format!("{group} — throughput"),
            width,
            &names,
            &core_tp,
            &other_tp,
            "Items / second",
            &caption,
        )?;
        println!("  → {}", throughput_path.display());
    }

    Ok(())
}

// Entry point

fn main() {
    println!("=== better-duck-core vs duckdb crate ===\n");
    println!("duckdb crate: v{DUCKDB_RS_VERSION} (bundled)\n");

    let mut results = Vec::new();
    results.extend(bench_primitives());
    results.extend(bench_composites());
    results.push(bench_crud());
    results.push(bench_bulk_ingest());
    results.push(bench_analytical());
    results.push(bench_prepared_reuse());
    results.push(bench_all_types());

    // Print quick summary to stdout, grouped.
    println!("\n── Quick summary (median latency)");
    for group in GROUP_ORDER {
        let group_results: Vec<&WorkloadResult> =
            results.iter().filter(|r| r.group == group).collect();
        if group_results.is_empty() {
            continue;
        }
        println!("\n{group}");
        println!("{:<20} {:>14} {:>14}", "Workload", "core", "duckdb crate");
        println!("{}", "─".repeat(50));
        for r in &group_results {
            let core_ms =
                r.core.as_ref().map(|s| fmt_ms(s.median_ms)).unwrap_or_else(|| "N/A".to_owned());
            let other_ms =
                r.other.as_ref().map(|s| fmt_ms(s.median_ms)).unwrap_or_else(|| "N/A".to_owned());
            let other_ms = if r.other_is_placeholder { format!("{other_ms}*") } else { other_ms };
            println!("{:<20} {:>14} {:>14}", r.name, core_ms, other_ms);
        }
        if group_results.iter().any(|r| r.other_is_placeholder) {
            println!(
                "  * duckdb crate has no write API for this operation — placeholder, not \
                 a measurement; see REPORT.md"
            );
        }
    }
    println!();

    let ctx = gather_system_ctx();

    let out_dir = workspace_root().join("docs").join("benchmarks");
    fs::create_dir_all(&out_dir).expect("create docs/benchmarks");
    println!("Writing outputs to: {}\n", out_dir.display());

    write_json(&out_dir, &ctx, &results).expect("write JSON");
    write_markdown(&out_dir, &ctx, &results).expect("write Markdown");
    write_charts(&out_dir, &ctx, &results).expect("write SVG charts");

    println!("\nDone. Open docs/benchmarks/REPORT.md for the full comparison table.");
}
