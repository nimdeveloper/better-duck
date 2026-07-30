//! Merges `core-bench`'s and `reference-bench`'s raw JSON output into the
//! final comparison report. Run after both (in either order) have produced
//! `docs/benchmarks/_raw_core.json` and `docs/benchmarks/_raw_reference.json`
//! — see `bench-comparison/lib/src/lib.rs` module docs for why the three are
//! separate crates/processes rather than one binary.

#![allow(missing_docs)]

use bench_comparison::{gather_system_ctx, merge, out_dir, read_raw, write_charts, write_json, write_markdown};

fn main() {
    let out = out_dir();
    let core_path = out.join("_raw_core.json");
    let reference_path = out.join("_raw_reference.json");

    let core = read_raw(&core_path).unwrap_or_else(|e| {
        panic!(
            "failed to read {}: {e} — run `core-bench` first",
            core_path.display()
        )
    });
    let reference = read_raw(&reference_path).unwrap_or_else(|e| {
        panic!(
            "failed to read {}: {e} — run `reference-bench` first",
            reference_path.display()
        )
    });

    assert_eq!(
        core.len(),
        reference.len(),
        "core-bench and reference-bench produced a different number of workloads — rerun both"
    );

    let results: Vec<_> = core
        .iter()
        .map(|c| {
            let r = reference
                .iter()
                .find(|r| r.group == c.group && r.name == c.name)
                .unwrap_or_else(|| panic!("reference-bench has no matching workload for {}/{}", c.group, c.name));
            merge(c, r)
        })
        .collect();

    let ctx = gather_system_ctx();
    std::fs::create_dir_all(&out).expect("create docs/benchmarks");
    write_json(&out, &ctx, &results).expect("write results.json");
    write_markdown(&out, &results).expect("write REPORT.md");
    write_charts(&out, &results).expect("write charts");
    println!("\nDone.");
}
