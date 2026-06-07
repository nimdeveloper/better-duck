//! Core-vs-CLI benchmark harness.
//!
//! Compares `better-duck-core` (in-process) against the `duckdb` CLI subprocess
//! across five representative workloads.  The CLI contender auto-skips when the
//! `duckdb` binary is not on PATH — install it and re-run to populate those columns.
//!
//! # Workloads
//!
//! | # | Name             | item_count         |
//! |---|------------------|--------------------|
//! | 1 | CRUD basics      | 4 ops / rep        |
//! | 2 | Bulk ingest      | 10 000 rows / rep  |
//! | 3 | Analytical query | 100 000 rows setup |
//! | 4 | Prepared reuse   | 100 queries / rep  |
//! | 5 | All-types scan   | 1 000 rows / rep   |
//!
//! # Outputs  (`docs/benchmarks/` in workspace root)
//!
//! * `results.json`              — raw metrics + system context
//! * `REPORT.md`                 — Markdown comparison tables
//! * `comparison-latency.svg`    — grouped bar chart, median latency
//! * `comparison-throughput.svg` — grouped bar chart, rows-or-ops per second

#![allow(missing_docs)]

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use better_duck_core::ffi::{
    duckdb_append_int32, duckdb_appender, duckdb_bind_int32, duckdb_prepared_statement,
};
use better_duck_core::{
    connection::Connection, error::Result as CoreResult, types::appendable::AppendAble,
};
use plotters::prelude::*;
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};

// Constants

const WARMUP_REPS: usize = 2;
const MEASURE_REPS: usize = 9; // odd → clean median at index 4
const BULK_ROWS: usize = 10_000;
const ANALYTICAL_ROWS: usize = 100_000;
const PREPARED_QUERIES: usize = 100;
const ALLTYPE_ROWS: usize = 1_000;

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
    os: String,
    rustc_version: String,
    duckdb_cli_version: Option<String>,
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
    name: String,
    description: String,
    item_count: usize,
    /// `None` = contender ran and produced stats; populated when available.
    core: Option<Stats>,
    /// `None` = CLI was skipped (binary not on PATH) or not applicable.
    cli: Option<Stats>,
}

// AppendAble helpers

/// Single `i32` column — used by bulk-ingest and prepared-reuse workloads.
struct I32Row(i32);

impl AppendAble for I32Row {
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> CoreResult<()> {
        // SAFETY: `appender` is a valid open appender for a table with exactly one
        // INTEGER column. `begin_row` is called by `Appender::append` before us.
        unsafe { duckdb_append_int32(appender, self.0) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> CoreResult<()> {
        // SAFETY: `stmt` is a valid prepared statement; `idx` is a 1-based parameter
        // index within the statement's parameter count (caller ensures correctness).
        unsafe { duckdb_bind_int32(stmt, idx, self.0) };
        Ok(())
    }
}

// System information

fn gather_system_ctx(cli_version: Option<String>) -> SystemCtx {
    let sys = System::new_all();

    let cpu_brand =
        sys.cpus().first().map(|c| c.brand().to_owned()).unwrap_or_else(|| "unknown".to_owned());

    let cpu_physical_cores = System::physical_core_count().unwrap_or(0);
    let total_ram_gb = sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0);
    let os = System::long_os_version().unwrap_or_else(|| "unknown".to_owned());

    let rustc_version = Command::new("rustc")
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
        os,
        rustc_version,
        duckdb_cli_version: cli_version,
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

// CLI helpers

/// Return the `duckdb --version` string if the binary is on PATH.
fn detect_cli() -> Option<String> {
    Command::new("duckdb")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
}

/// Write `sql` to a temp file and run it against an in-memory DuckDB instance.
///
/// Returns the total wall-clock elapsed time (including process startup).
fn run_cli_sql(sql: &str) -> Result<Duration, Box<dyn std::error::Error>> {
    let mut tmp = tempfile::NamedTempFile::new()?;
    tmp.write_all(sql.as_bytes())?;
    tmp.flush()?;

    let t = Instant::now();
    let status = Command::new("duckdb")
        .arg(":memory:")
        .stdin(fs::File::open(tmp.path())?)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    let elapsed = t.elapsed();

    if status.success() {
        Ok(elapsed)
    } else {
        Err(format!("duckdb exited with {status}").into())
    }
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

// Workload 1: CRUD basics

fn bench_crud(cli_available: bool) -> WorkloadResult {
    println!("  [1/5] CRUD basics …");

    // Core
    let (samples, rss_b, rss_a) = {
        let mut conn = Connection::open_in_memory().expect("open db");
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
            // Point SELECT (separate to ensure it's compiled as a prepared query)
            let _ = conn
                .execute(format!("SELECT id, name, val FROM crud WHERE id = {rep}"))
                .expect("select");
        })
    };
    let core_stats = compute_stats(samples, 4, rss_b, rss_a); // 4 ops per rep

    // CLI
    let cli_stats = if cli_available {
        let sql = "
CREATE TABLE crud (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, val DOUBLE NOT NULL);
INSERT INTO crud VALUES (1, 'item-1', 1.5);
SELECT id, name, val FROM crud WHERE id = 1;
UPDATE crud SET val = val + 1.0 WHERE id = 1;
DELETE FROM crud WHERE id = 1;
";
        let mut cli_samples = Vec::with_capacity(MEASURE_REPS);
        for _ in 0..WARMUP_REPS {
            let _ = run_cli_sql(sql);
        }
        let rss_b = sample_rss_mb();
        for _ in 0..MEASURE_REPS {
            match run_cli_sql(sql) {
                Ok(d) => cli_samples.push(d),
                Err(e) => {
                    eprintln!("CLI CRUD error: {e}");
                    break;
                },
            }
        }
        let rss_a = sample_rss_mb();
        if cli_samples.is_empty() {
            None
        } else {
            Some(compute_stats(cli_samples, 4, rss_b, rss_a))
        }
    } else {
        None
    };

    WorkloadResult {
        name: "crud_basics".to_owned(),
        description: "Single-row INSERT + point SELECT + UPDATE + DELETE".to_owned(),
        item_count: 4,
        core: Some(core_stats),
        cli: cli_stats,
    }
}

// Workload 2: Bulk ingest

fn bench_bulk_ingest(cli_available: bool) -> WorkloadResult {
    println!("  [2/5] Bulk ingest ({BULK_ROWS} rows) …");

    // Core — appender
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut conn = Connection::open_in_memory().expect("open db");
        conn.execute_batch("CREATE TABLE bulk (v INTEGER NOT NULL)").expect("create table");
        let mut app = conn.appender("bulk", "main").expect("appender");
        for i in 0i32..BULK_ROWS as i32 {
            app.append(&mut I32Row(i)).expect("append");
        }
        app.save().expect("flush");
    });
    let core_stats = compute_stats(samples, BULK_ROWS, rss_b, rss_a);

    // CLI — range-based INSERT
    let cli_stats = if cli_available {
        let sql =
            format!("CREATE TABLE bulk AS SELECT range::INTEGER AS v FROM range({BULK_ROWS});");
        let mut cli_samples = Vec::with_capacity(MEASURE_REPS);
        for _ in 0..WARMUP_REPS {
            let _ = run_cli_sql(&sql);
        }
        let rss_b = sample_rss_mb();
        for _ in 0..MEASURE_REPS {
            match run_cli_sql(&sql) {
                Ok(d) => cli_samples.push(d),
                Err(e) => {
                    eprintln!("CLI bulk ingest error: {e}");
                    break;
                },
            }
        }
        let rss_a = sample_rss_mb();
        if cli_samples.is_empty() {
            None
        } else {
            Some(compute_stats(cli_samples, BULK_ROWS, rss_b, rss_a))
        }
    } else {
        None
    };

    WorkloadResult {
        name: "bulk_ingest".to_owned(),
        description: format!(
            "Ingest {BULK_ROWS} rows (core: appender; CLI: CREATE TABLE AS SELECT … FROM range)"
        ),
        item_count: BULK_ROWS,
        core: Some(core_stats),
        cli: cli_stats,
    }
}

// Workload 3: Analytical query

fn bench_analytical(cli_available: bool) -> WorkloadResult {
    println!("  [3/5] Analytical query ({ANALYTICAL_ROWS} rows) …");

    const QUERY: &str = "SELECT category, COUNT(*) AS n, AVG(value) AS avg_val \
         FROM bench_data \
         GROUP BY category \
         ORDER BY avg_val DESC \
         LIMIT 10";

    // Core — pre-populate, time only the query
    let mut core_conn = Connection::open_in_memory().expect("open db");
    core_conn
        .execute_batch(format!(
            "CREATE TABLE bench_data AS \
             SELECT (range % 10 + 1)::INTEGER AS category, \
                    (range * 3.14159 % 1000.0)::DOUBLE AS value \
             FROM range({ANALYTICAL_ROWS})"
        ))
        .expect("populate analytical table");

    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let result = core_conn.execute(QUERY).expect("analytical query");
        let _count = result.count();
    });
    let core_stats = compute_stats(samples, ANALYTICAL_ROWS, rss_b, rss_a);

    // CLI — includes table creation per rep
    let cli_stats = if cli_available {
        let sql = format!(
            "CREATE TABLE bench_data AS \
             SELECT (range % 10 + 1)::INTEGER AS category, \
                    (range * 3.14159 % 1000.0)::DOUBLE AS value \
             FROM range({ANALYTICAL_ROWS}); \
             SELECT category, COUNT(*) AS n, AVG(value) AS avg_val \
             FROM bench_data GROUP BY category ORDER BY avg_val DESC LIMIT 10;"
        );
        let mut cli_samples = Vec::with_capacity(MEASURE_REPS);
        for _ in 0..WARMUP_REPS {
            let _ = run_cli_sql(&sql);
        }
        let rss_b = sample_rss_mb();
        for _ in 0..MEASURE_REPS {
            match run_cli_sql(&sql) {
                Ok(d) => cli_samples.push(d),
                Err(e) => {
                    eprintln!("CLI analytical error: {e}");
                    break;
                },
            }
        }
        let rss_a = sample_rss_mb();
        if cli_samples.is_empty() {
            None
        } else {
            Some(compute_stats(cli_samples, ANALYTICAL_ROWS, rss_b, rss_a))
        }
    } else {
        None
    };

    WorkloadResult {
        name: "analytical".to_owned(),
        description: format!(
            "GROUP BY + AVG + ORDER BY on {ANALYTICAL_ROWS} rows \
             (core: query only; CLI: includes table creation)"
        ),
        item_count: ANALYTICAL_ROWS,
        core: Some(core_stats),
        cli: cli_stats,
    }
}

// Workload 4: Prepared-statement reuse

fn bench_prepared_reuse(cli_available: bool) -> WorkloadResult {
    println!("  [4/5] Prepared-statement reuse ({PREPARED_QUERIES} queries) …");

    // Core — single connection, parameterised SELECT in a tight loop
    let mut core_conn = Connection::open_in_memory().expect("open db");
    core_conn
        .execute_batch(format!(
            "CREATE TABLE vals (v INTEGER); \
             INSERT INTO vals SELECT range FROM range({PREPARED_QUERIES})"
        ))
        .expect("setup prepared table");

    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        for i in 0i32..PREPARED_QUERIES as i32 {
            let mut row = I32Row(i);
            let result = core_conn
                .execute_with(
                    "SELECT v FROM vals WHERE v = $1",
                    &mut [&mut row as &mut dyn AppendAble],
                )
                .expect("prepared select");
            let _ = result.count();
        }
    });
    let core_stats = compute_stats(samples, PREPARED_QUERIES, rss_b, rss_a);

    // CLI — one subprocess call containing all queries
    let cli_stats = if cli_available {
        let selects: String = (0..PREPARED_QUERIES)
            .map(|i| format!("SELECT v FROM vals WHERE v = {i};"))
            .collect::<Vec<_>>()
            .join("\n");
        let sql = format!(
            "CREATE TABLE vals AS SELECT range AS v FROM range({PREPARED_QUERIES});\n{selects}"
        );
        let mut cli_samples = Vec::with_capacity(MEASURE_REPS);
        for _ in 0..WARMUP_REPS {
            let _ = run_cli_sql(&sql);
        }
        let rss_b = sample_rss_mb();
        for _ in 0..MEASURE_REPS {
            match run_cli_sql(&sql) {
                Ok(d) => cli_samples.push(d),
                Err(e) => {
                    eprintln!("CLI prepared reuse error: {e}");
                    break;
                },
            }
        }
        let rss_a = sample_rss_mb();
        if cli_samples.is_empty() {
            None
        } else {
            Some(compute_stats(cli_samples, PREPARED_QUERIES, rss_b, rss_a))
        }
    } else {
        None
    };

    WorkloadResult {
        name: "prepared_reuse".to_owned(),
        description: format!(
            "{PREPARED_QUERIES} point-SELECT queries \
             (core: execute_with loop; CLI: single subprocess with {PREPARED_QUERIES} statements)"
        ),
        item_count: PREPARED_QUERIES,
        core: Some(core_stats),
        cli: cli_stats,
    }
}

// Workload 5: All-types scan

fn bench_all_types(cli_available: bool) -> WorkloadResult {
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

    // Core
    let (samples, rss_b, rss_a) = run_reps(WARMUP_REPS, MEASURE_REPS, || {
        let mut conn = Connection::open_in_memory().expect("open db");
        conn.execute_batch(format!(
            "{DDL}; \
             INSERT INTO all_types \
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
        ))
        .expect("all-types insert");
        let result = conn.execute("SELECT * FROM all_types").expect("all-types scan");
        let _ = result.count();
    });
    let core_stats = compute_stats(samples, ALLTYPE_ROWS, rss_b, rss_a);

    // CLI
    let cli_stats = if cli_available {
        let sql = format!(
            "{DDL}; \
             INSERT INTO all_types \
             SELECT range::INTEGER, (range%2=0), (range%128)::TINYINT, range::SMALLINT, \
                    range::INTEGER, range::BIGINT, (range*1.1)::FLOAT, (range*2.2)::DOUBLE, \
                    'str-'||range::VARCHAR, DATE '2020-01-01'+range::INTEGER, \
                    to_timestamp(1577836800+range) \
             FROM range({ALLTYPE_ROWS}); \
             SELECT COUNT(*) FROM all_types;"
        );
        let mut cli_samples = Vec::with_capacity(MEASURE_REPS);
        for _ in 0..WARMUP_REPS {
            let _ = run_cli_sql(&sql);
        }
        let rss_b = sample_rss_mb();
        for _ in 0..MEASURE_REPS {
            match run_cli_sql(&sql) {
                Ok(d) => cli_samples.push(d),
                Err(e) => {
                    eprintln!("CLI all-types error: {e}");
                    break;
                },
            }
        }
        let rss_a = sample_rss_mb();
        if cli_samples.is_empty() {
            None
        } else {
            Some(compute_stats(cli_samples, ALLTYPE_ROWS, rss_b, rss_a))
        }
    } else {
        None
    };

    WorkloadResult {
        name: "all_types".to_owned(),
        description: format!("INSERT + full-scan of {ALLTYPE_ROWS} rows across 11 column types"),
        item_count: ALLTYPE_ROWS,
        core: Some(core_stats),
        cli: cli_stats,
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

fn write_markdown(
    out_dir: &Path,
    ctx: &SystemCtx,
    results: &[WorkloadResult],
) -> std::io::Result<()> {
    let mut md = String::with_capacity(4096);

    md.push_str("# better-duck-core vs DuckDB CLI — Benchmark Report\n\n");

    // System context table
    md.push_str("## System context\n\n");
    md.push_str("| Key | Value |\n|---|---|\n");
    md.push_str(&format!("| CPU | {} ({} cores) |\n", ctx.cpu_brand, ctx.cpu_physical_cores));
    md.push_str(&format!("| RAM | {:.1} GB |\n", ctx.total_ram_gb));
    md.push_str(&format!("| OS | {} |\n", ctx.os));
    md.push_str(&format!("| rustc | {} |\n", ctx.rustc_version));
    md.push_str(&format!(
        "| duckdb CLI | {} |\n",
        ctx.duckdb_cli_version.as_deref().unwrap_or("not found — CLI columns skipped")
    ));
    md.push_str(&format!("| Generated at (Unix) | {} |\n\n", ctx.generated_at_unix_secs));

    md.push_str(
        "> **Latency** columns: min / median / p95 over 9 measured reps (2 warmup discarded).\n",
    );
    md.push_str("> **Throughput** is `item_count / median_latency`.\n");
    md.push_str("> CLI timing includes process startup overhead — this is intentional.\n\n");

    // Per-workload tables
    md.push_str("## Charts\n\n");
    md.push_str("![Latency](comparison-latency.svg)\n\n");
    md.push_str("![Throughput](comparison-throughput.svg)\n\n");

    for wr in results {
        md.push_str(&format!("## {} — {}\n\n", wr.name, wr.description));
        md.push_str(&format!("*item\\_count = {}*\n\n", wr.item_count));
        md.push_str("| Contender | Latency (min / median / p95) | Throughput |\n");
        md.push_str("|---|---|---|\n");
        md.push_str(&stats_row("better-duck-core", wr.core.as_ref()));
        md.push_str(&stats_row("duckdb CLI", wr.cli.as_ref()));
        md.push('\n');
    }

    let path = out_dir.join("REPORT.md");
    fs::write(&path, md)?;
    println!("  → {}", path.display());
    Ok(())
}

// SVG charts

fn draw_chart(
    path: &Path,
    title: &str,
    workload_names: &[&str],
    core_vals: &[f64],
    cli_vals: &[Option<f64>],
    y_label: &str,
    caption: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let n = workload_names.len();
    // Each workload gets 3 slots: [core bar | cli bar | gap]
    let total_x = (n * 3) as u32;

    let max_y =
        core_vals.iter().chain(cli_vals.iter().flatten()).copied().fold(0.0_f64, f64::max) * 1.15;
    let max_y = if max_y == 0.0 { 1.0 } else { max_y };

    let root = SVGBackend::new(path, (1_100, 520)).into_drawing_area();
    root.fill(&WHITE)?;

    let (upper, lower) = root.split_vertically(460);
    let mut chart = ChartBuilder::on(&upper)
        .caption(title, ("sans-serif", 18).into_font())
        .margin(20u32)
        .x_label_area_size(50u32)
        .y_label_area_size(80u32)
        .build_cartesian_2d(0u32..total_x, 0.0f64..max_y)?;

    chart
        .configure_mesh()
        .x_labels(n)
        .x_label_formatter(&|slot| {
            // Show workload name at the midpoint of each group (slot 1 of each group).
            let group = (*slot / 3) as usize;
            if *slot % 3 == 1 {
                workload_names.get(group).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .x_label_style(("sans-serif", 11).into_font())
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

    // Core bars (blue)
    chart
        .draw_series(core_vals.iter().enumerate().map(|(i, &v)| {
            let x0 = (i * 3) as u32;
            Rectangle::new([(x0, 0.0), (x0 + 1, v)], BLUE.mix(0.75).filled())
        }))?
        .label("better-duck-core")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 14, y + 5)], BLUE.mix(0.75).filled()));

    // CLI bars (orange) — skip None entries
    chart
        .draw_series(cli_vals.iter().enumerate().filter_map(|(i, opt)| {
            opt.map(|v| {
                let x0 = (i * 3 + 1) as u32;
                Rectangle::new([(x0, 0.0), (x0 + 1, v)], RGBColor(220, 100, 0).mix(0.75).filled())
            })
        }))?
        .label("duckdb CLI (incl. startup)")
        .legend(|(x, y)| {
            Rectangle::new([(x, y - 5), (x + 14, y + 5)], RGBColor(220, 100, 0).mix(0.75).filled())
        });

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .border_style(BLACK)
        .draw()?;

    // Caption row
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
    let names: Vec<&str> = results.iter().map(|r| r.name.as_str()).collect();
    let core_lat: Vec<f64> =
        results.iter().map(|r| r.core.as_ref().map(|s| s.median_ms).unwrap_or(0.0)).collect();
    let cli_lat: Vec<Option<f64>> =
        results.iter().map(|r| r.cli.as_ref().map(|s| s.median_ms)).collect();
    let core_tp: Vec<f64> = results
        .iter()
        .map(|r| r.core.as_ref().map(|s| s.throughput_per_sec).unwrap_or(0.0))
        .collect();
    let cli_tp: Vec<Option<f64>> =
        results.iter().map(|r| r.cli.as_ref().map(|s| s.throughput_per_sec)).collect();

    let caption = format!(
        "System: {} ({} cores, {:.0} GB RAM) — {} — {}",
        ctx.cpu_brand, ctx.cpu_physical_cores, ctx.total_ram_gb, ctx.os, ctx.rustc_version,
    );

    let latency_path = out_dir.join("comparison-latency.svg");
    draw_chart(
        &latency_path,
        "Median latency — better-duck-core vs duckdb CLI",
        &names,
        &core_lat,
        &cli_lat,
        "Latency (ms)",
        &caption,
    )?;
    println!("  → {}", latency_path.display());

    let throughput_path = out_dir.join("comparison-throughput.svg");
    draw_chart(
        &throughput_path,
        "Throughput — better-duck-core vs duckdb CLI",
        &names,
        &core_tp,
        &cli_tp,
        "Items / second",
        &caption,
    )?;
    println!("  → {}", throughput_path.display());

    Ok(())
}

// Entry point

fn main() {
    println!("=== better-duck-core vs DuckDB CLI ===\n");

    // CLI detection
    let cli_version = detect_cli();
    match &cli_version {
        Some(v) => println!("CLI detected: {v}"),
        None => println!("CLI not found on PATH — CLI columns will be marked as skipped"),
    }
    let cli_available = cli_version.is_some();

    println!("\nRunning workloads ({WARMUP_REPS} warmup + {MEASURE_REPS} measured reps each):\n");

    // Run workloads
    let results = vec![
        bench_crud(cli_available),
        bench_bulk_ingest(cli_available),
        bench_analytical(cli_available),
        bench_prepared_reuse(cli_available),
        bench_all_types(cli_available),
    ];

    // Print quick summary to stdout
    println!("\n── Quick summary (median latency)");
    println!("{:<22} {:>14} {:>14}", "Workload", "core", "CLI");
    println!("{}", "─".repeat(52));
    for r in &results {
        let core_ms =
            r.core.as_ref().map(|s| fmt_ms(s.median_ms)).unwrap_or_else(|| "N/A".to_owned());
        let cli_ms =
            r.cli.as_ref().map(|s| fmt_ms(s.median_ms)).unwrap_or_else(|| "skipped".to_owned());
        println!("{:<22} {:>14} {:>14}", r.name, core_ms, cli_ms);
    }
    println!();

    // Gather system context
    let ctx = gather_system_ctx(cli_version);

    // Write outputs
    let out_dir = workspace_root().join("docs").join("benchmarks");
    fs::create_dir_all(&out_dir).expect("create docs/benchmarks");
    println!("Writing outputs to: {}\n", out_dir.display());

    write_json(&out_dir, &ctx, &results).expect("write JSON");
    write_markdown(&out_dir, &ctx, &results).expect("write Markdown");
    write_charts(&out_dir, &ctx, &results).expect("write SVG charts");

    println!("\nDone. Open docs/benchmarks/REPORT.md for the full comparison table.");
}
