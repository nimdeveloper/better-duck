//! Extracts the vendored DuckDB source archive (`vendor/duckdb.tar.gz`) into
//! `OUT_DIR` and compiles it into a static `duckdb` library, driven by a
//! `manifest.json` file inside the archive (produced by `xtask upgrade-duckdb`,
//! see the repo root `xtask` crate).
//!
//! No network access, no git submodule, no `bindgen`/LLVM requirement here —
//! the manifest lists exactly which translation units to compile, and
//! `src/bindings.rs` (checked into this crate, not generated at build time)
//! is the pregenerated FFI surface. Regenerating either is a maintainer-only
//! action; see `xtask/src/main.rs`.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

/// The subset of DuckDB's own manifest that this build needs. Written by
/// `xtask upgrade-duckdb` alongside the vendored source tree, then packed
/// into `vendor/duckdb.tar.gz` at its root.
#[derive(Deserialize)]
struct Manifest {
    /// The DuckDB release tag this archive was vendored from, e.g. `"v1.4.2"`
    /// — surfaced as `DUCKDB_VENDORED_VERSION` for `src/lib.rs` to re-export.
    duckdb_version: String,
    /// Source files to compile, relative to the archive root, always included.
    sources: Vec<String>,
    /// Additional source files to compile only when the `json` feature is on.
    json_sources: Vec<String>,
    /// Additional source files to compile only when the `parquet` feature is on.
    parquet_sources: Vec<String>,
    /// `-I` include directories, relative to the archive root.
    include_dirs: Vec<String>,
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("set by cargo"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("set by cargo"));
    let archive = manifest_dir.join("vendor").join("duckdb.tar.gz");
    println!("cargo:rerun-if-changed={}", archive.display());
    println!("cargo:rerun-if-changed=build.rs");

    let extracted = out_dir.join("duckdb-src");
    extract_archive(&archive, &extracted);

    let manifest_path = extracted.join("manifest.json");
    let manifest: Manifest = serde_json::from_str(
        &fs::read_to_string(&manifest_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display())),
    )
    .unwrap_or_else(|e| panic!("failed to parse {}: {e}", manifest_path.display()));

    println!("cargo:rustc-env=DUCKDB_VENDORED_VERSION={}", manifest.duckdb_version);

    let mut build = cc::Build::new();
    build.cpp(true).std("c++17").warnings(false);

    let target = env::var("TARGET").unwrap_or_default();
    if target.contains("windows") && !target.contains("gnu") {
        // Without this, DuckDB's `DUCKDB_API` macro expands to
        // `__declspec(dllimport)` on MSVC (its default assumption is a DLL
        // build) — fatal for a static library, where a dllimport'd symbol
        // can't also be defined in the same translation unit.
        build.define("DUCKDB_STATIC_BUILD", None);
    }

    for dir in &manifest.include_dirs {
        build.include(extracted.join(dir));
    }
    for src in &manifest.sources {
        build.file(extracted.join(src));
    }

    if cfg!(feature = "json") {
        build.define("DUCKDB_EXTENSION_JSON_LINKED", None);
        for src in &manifest.json_sources {
            build.file(extracted.join(src));
        }
    }
    if cfg!(feature = "parquet") {
        build.define("DUCKDB_EXTENSION_PARQUET_LINKED", None);
        for src in &manifest.parquet_sources {
            build.file(extracted.join(src));
        }
    }

    build.compile("duckdb");
}

/// Extracts `archive` into `dest`, always overwriting. `OUT_DIR` (and so
/// `dest`) persists across builds even when `cargo:rerun-if-changed` causes
/// this build script to rerun for an unrelated reason — a "skip if `dest`
/// looks populated" fast path would silently keep compiling a stale
/// extraction after the archive changes, so this always re-extracts instead;
/// `tar` unpacking a few thousand small files is negligible next to the C++
/// compile that follows.
fn extract_archive(
    archive: &Path,
    dest: &Path,
) {
    let file = fs::File::open(archive)
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", archive.display()));
    let decompressed = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decompressed);
    fs::create_dir_all(dest).unwrap_or_else(|e| panic!("failed to create {}: {e}", dest.display()));
    archive
        .unpack(dest)
        .unwrap_or_else(|e| panic!("failed to extract vendored DuckDB source: {e}"));
}
