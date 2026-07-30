//! Shared measurement/reporting code for the core-vs-`duckdb`-crate benchmark.
//!
//! Deliberately has **no** dependency on either `better-duck-core` or the
//! `duckdb` crate: `better-duck-core` links its own vendored DuckDB
//! (`better-duck-sys`) and the `duckdb` crate links a separately-vendored
//! DuckDB (`libduckdb-sys`) — both export the same `extern "C"` symbol names
//! (`duckdb_open`, `duckdb_query`, …), so they cannot be linked into the same
//! binary — so they can't even be two `[[bin]]`s of one crate, since a
//! package's dependencies (and so its `links` conflicts) apply to the whole
//! package. `core-bench` and `reference-bench` are separate standalone
//! crates that each link exactly one side, depend on this crate by `path`,
//! run in separate processes, and write their raw measurements via
//! [`write_raw`]; `run-all` (also standalone, no database dependency) reads
//! both JSON files back via [`read_raw`] and [`merge`]s them into the
//! combined report.

#![allow(missing_docs)]

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessesToUpdate, System};

pub const WARMUP_REPS: usize = 2;
pub const MEASURE_REPS: usize = 9; // odd → clean median at index 4
pub const TYPE_WARMUP_REPS: usize = 1;
pub const TYPE_MEASURE_REPS: usize = 5; // fewer reps: many type benches, each is cheap
pub const PRIMITIVE_ROWS: usize = 20_000;
pub const COMPOSITE_ROWS: usize = 2_000;
pub const BULK_ROWS: usize = 10_000;
pub const ANALYTICAL_ROWS: usize = 100_000;
pub const PREPARED_QUERIES: usize = 100;
pub const ALLTYPE_ROWS: usize = 1_000;

/// Kept in sync with the `duckdb` version pinned in `Cargo.toml`.
pub const DUCKDB_RS_VERSION: &str = "1.10505.0";

/// Repository root resolved at compile time. `env!("CARGO_MANIFEST_DIR")`
/// here always resolves to *this* crate's own directory
/// (`bench-comparison/lib/`, regardless of which sibling binary calls this
/// function — `env!` bakes in the value from where it's expanded, not the
/// caller), two levels below the repository root.
pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repository root exists two levels above bench-comparison/lib/")
        .to_path_buf()
}

pub fn out_dir() -> PathBuf {
    workspace_root().join("docs").join("benchmarks")
}

// Shared domain types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemCtx {
    pub cpu_brand: String,
    pub cpu_physical_cores: usize,
    pub total_ram_gb: f64,
    pub rustc_version: String,
    pub duckdb_rs_version: String,
    pub generated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub min_ms: f64,
    pub median_ms: f64,
    pub p95_ms: f64,
    /// Items (rows / operations) per second, based on median latency.
    pub throughput_per_sec: f64,
    /// Approximate working-set growth during the measured reps (MB).
    pub rss_delta_mb: f64,
}

/// One implementation's raw measurement for one workload, as written by
/// `core_bench`/`reference_bench` and read back by `run_all`/`report`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawWorkload {
    pub group: String,
    pub name: String,
    pub description: String,
    pub item_count: usize,
    pub stats: Option<Stats>,
    /// Set only by `reference_bench` for LIST/ARRAY/STRUCT/MAP: the `duckdb`
    /// crate has no safe write API for these, so `stats` above is left `None`
    /// and the real (but not directly comparable — see `no_write_api`) SQL-
    /// literal-insert timing is recorded here instead.
    pub workaround_actual: Option<Stats>,
    /// Set only by `reference_bench`: true when the `duckdb` crate has no
    /// fair way to attempt this workload at all. `run_all` fills in a neutral
    /// placeholder (core's latency + 1ms) rather than plotting
    /// `workaround_actual`'s vectorized-bulk timing, which would misleadingly
    /// look faster than a row-by-row appender.
    #[serde(default)]
    pub no_write_api: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadResult {
    pub group: String,
    pub name: String,
    pub description: String,
    pub item_count: usize,
    pub core: Option<Stats>,
    /// Stats for the `duckdb` crate contender. When `other_is_placeholder` is
    /// set, this is a neutral synthetic value (`core` ± 1), not a real
    /// measurement — see `other_is_placeholder`.
    pub other: Option<Stats>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub other_is_placeholder: bool,
    /// The real measurement from the unsupported-operation workaround, kept
    /// for reference when `other_is_placeholder` is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_workaround_actual: Option<Stats>,
}

/// A neutral placeholder for a contender that cannot fairly attempt an
/// operation at all (e.g. no write API): visually "just behind" `core`
/// rather than using a workaround's vectorized-bulk timing, which would
/// misleadingly look faster.
pub fn placeholder_stats_just_behind(core: &Stats) -> Stats {
    Stats {
        min_ms: core.min_ms + 1.0,
        median_ms: core.median_ms + 1.0,
        p95_ms: core.p95_ms + 1.0,
        throughput_per_sec: (core.throughput_per_sec - 1.0).max(0.0),
        rss_delta_mb: 0.0,
    }
}

// System information

pub fn gather_system_ctx() -> SystemCtx {
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
pub fn sample_rss_mb() -> f64 {
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
pub fn run_reps<F: FnMut()>(
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

pub fn compute_stats(
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

pub const GROUP_ORDER: [&str; 3] = ["Primitive types", "Composite types", "Operations"];

pub fn write_raw(
    path: &Path,
    workloads: &[RawWorkload],
) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(workloads).expect("serialize raw workloads");
    fs::write(path, json)
}

pub fn read_raw(path: &Path) -> std::io::Result<Vec<RawWorkload>> {
    let json = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&json).expect("parse raw workloads"))
}

// Reporting

#[derive(Serialize)]
struct JsonReport<'a> {
    system: &'a SystemCtx,
    workloads: &'a [WorkloadResult],
}

pub fn write_json(
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

fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect()
}

pub fn write_markdown(
    out_dir: &Path,
    results: &[WorkloadResult],
) -> std::io::Result<()> {
    let mut md = String::with_capacity(8192);

    md.push_str("# better-duck-core vs `duckdb` crate — Benchmark Report\n\n");

    md.push_str(
        "> **Latency** columns: min / median / p95 over the measured reps (warmup discarded).\n",
    );
    md.push_str("> **Throughput** is `item_count / median_latency`.\n");
    md.push_str(
        "> `better-duck-core` and the `duckdb` crate each vendor their own copy of DuckDB \
         and cannot be linked into one process (see module docs), so each ran in its own \
         process; timings are still directly comparable — both are in-process, native calls \
         with no subprocess-per-operation overhead on either side.\n\n",
    );

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
    x_label: &str,
    y_label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use plotters::prelude::*;
    use plotters::style::text_anchor::{HPos, Pos, VPos};

    let n = workload_names.len();
    // Each workload gets 3 slots: [core bar | other bar | gap]
    let total_x = (n * 3) as u32;

    let max_y = core_vals.iter().chain(other_vals.iter()).copied().fold(0.0_f64, f64::max) * 1.15;
    let max_y = if max_y == 0.0 { 1.0 } else { max_y };

    // `upper` holds the plot; `lower` holds one rotated label per core/duckdb bar
    // pair (not the mesh's own per-tick labels) plus the x-axis title beneath them.
    let root = SVGBackend::new(path, (width, 620)).into_drawing_area();
    root.fill(&WHITE)?;
    let (upper, lower) = root.split_vertically(430);

    let mut chart = ChartBuilder::on(&upper)
        .caption(title, ("sans-serif", 18).into_font())
        .margin(20u32)
        .x_label_area_size(15u32)
        .y_label_area_size(80u32)
        .build_cartesian_2d(0u32..total_x, 0.0f64..max_y)?;

    chart
        .configure_mesh()
        .x_labels(n)
        .x_label_formatter(&|_| String::new())
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

    // One rotated label per group, centered under its core/duckdb bar pair (the
    // color-coded legend already distinguishes the two bars within a pair).
    let name_style = TextStyle::from(("sans-serif", 10).into_font())
        .transform(FontTransform::Rotate90)
        .pos(Pos::new(HPos::Left, VPos::Center));
    for (i, name) in workload_names.iter().enumerate() {
        // The pair spans [i*3, i*3+2]; its midpoint is the exact boundary between
        // the two bars, at i*3+1.
        let mid_x = (i * 3) as u32 + 1;
        let center_x = chart.plotting_area().map_coordinate(&(mid_x, 0.0)).0;
        lower.draw_text(name, &name_style, (center_x, 4))?;
    }
    lower.draw_text(
        x_label,
        &TextStyle::from(("sans-serif", 13).into_font()).pos(Pos::new(HPos::Center, VPos::Top)),
        ((width / 2) as i32, 175),
    )?;

    root.present()?;
    Ok(())
}

pub fn write_charts(
    out_dir: &Path,
    results: &[WorkloadResult],
) -> Result<(), Box<dyn std::error::Error>> {
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
        let x_label = if group == "Operations" { "Workload" } else { "Type" };

        let latency_path = out_dir.join(format!("comparison-{slug}-latency.svg"));
        draw_chart(
            &latency_path,
            &format!("{group} — median latency"),
            width,
            &names,
            &core_lat,
            &other_lat,
            x_label,
            "Latency (ms)",
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
            x_label,
            "Items / second",
        )?;
        println!("  → {}", throughput_path.display());
    }

    Ok(())
}

/// Merges one `core_bench` raw workload with its matching `reference_bench`
/// raw workload (matched by `(group, name)`) into the final [`WorkloadResult`]
/// used for reporting.
pub fn merge(
    core: &RawWorkload,
    reference: &RawWorkload,
) -> WorkloadResult {
    let (other, other_is_placeholder, other_workaround_actual) = if reference.no_write_api {
        let core_stats = core.stats.as_ref().expect("core side always measures a real Stats");
        (Some(placeholder_stats_just_behind(core_stats)), true, reference.workaround_actual.clone())
    } else {
        (reference.stats.clone(), false, None)
    };

    WorkloadResult {
        group: core.group.clone(),
        name: core.name.clone(),
        description: core.description.clone(),
        item_count: core.item_count,
        core: core.stats.clone(),
        other,
        other_is_placeholder,
        other_workaround_actual,
    }
}
