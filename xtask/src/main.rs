//! Maintainer tooling for the `better-duck` workspace. Not published.
//!
//! `cargo run -p xtask -- upgrade-duckdb --tag v1.5.5` regenerates
//! `crates/better-duck-sys/vendor/duckdb.tar.gz` and
//! `crates/better-duck-sys/src/bindings.rs` from a specific upstream
//! `duckdb/duckdb` release tag — the *only* place a git checkout of DuckDB's
//! source ever touches this repo. The clone is ephemeral (a `tempfile`
//! directory, deleted when this process exits, on any exit path); nothing is
//! left behind as a submodule reference, and no consumer of
//! `better-duck-sys` ever needs network access or Python to build it.
//!
//! This calls into DuckDB's own `scripts/package_build.py`
//! (`build_package(...)`) to get the amalgamated (unity-build) source file
//! list — the same officially-provided mechanism the community
//! `libduckdb-sys` crate's own `update_sources.py` uses — rather than
//! reimplementing DuckDB's internal source-selection logic in Rust, which
//! would be fragile against upstream changes.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(name = "xtask", about = "Maintainer tooling for the better-duck workspace")]
struct Cli {
    #[command(subcommand)]
    command: Command_,
}

#[derive(Subcommand)]
enum Command_ {
    /// Regenerate the vendored DuckDB source archive and FFI bindings from a
    /// specific upstream release tag.
    UpgradeDuckdb {
        /// The `duckdb/duckdb` git tag to vendor, e.g. `v1.5.5`.
        #[arg(long)]
        tag: String,
    },
}

/// This crate's own manifest schema, written into the vendored archive for
/// `better-duck-sys/build.rs` to consume. Deliberately simpler than DuckDB's
/// own `package_build.py` output — we only need "which files, which include
/// dirs, plus one optional extra source list per Cargo feature".
#[derive(Serialize, Deserialize)]
struct Manifest {
    duckdb_version: String,
    sources: Vec<String>,
    json_sources: Vec<String>,
    parquet_sources: Vec<String>,
    include_dirs: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command_::UpgradeDuckdb { tag } => upgrade_duckdb(&tag),
    }
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.parent().map(Path::to_path_buf).context(
        "xtask's CARGO_MANIFEST_DIR has no parent — expected xtask/ under the workspace root",
    )
}

fn upgrade_duckdb(tag: &str) -> Result<()> {
    let root = workspace_root()?;
    let sys_crate = root.join("crates").join("better-duck-sys");
    if !sys_crate.is_dir() {
        bail!("expected {} to exist", sys_crate.display());
    }

    let work = tempfile_dir("better-duck-xtask-duckdb")?;
    println!("==> shallow-cloning duckdb/duckdb@{tag} into {}", work.display());
    clone_tag(tag, &work)?;

    // Deliberately NOT nested under `work` (the checkout): `package_build.py`
    // returns some paths (from `amalgamation.list_sources()`) relative to the
    // checkout root, so a staging dir inside the checkout would make
    // `collect_sources`'s prefix-stripping ambiguous between "relative to the
    // checkout" and "relative to staging".
    let staging = tempfile_dir("better-duck-xtask-staging")?;
    println!("==> collecting the amalgamated source file list via package_build.py");
    let manifest = collect_sources(&work, &staging, tag)?;

    println!("==> writing manifest.json + packaging vendor/duckdb.tar.gz");
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    fs::write(staging.join("manifest.json"), &manifest_json)?;
    let vendor_dir = sys_crate.join("vendor");
    fs::create_dir_all(&vendor_dir)?;
    package_archive(&staging, &vendor_dir.join("duckdb.tar.gz"))?;

    println!("==> regenerating src/bindings.rs via bindgen");
    let header = work.join("src").join("include").join("duckdb.h");
    if !header.is_file() {
        bail!("expected DuckDB's C header at {}", header.display());
    }
    generate_bindings(&header, &sys_crate.join("src").join("bindings.rs"))?;

    println!(
        "==> done. Review the diff, then commit crates/better-duck-sys/{{vendor/duckdb.tar.gz,src/bindings.rs}}."
    );
    // `work` (the clone) is a tempfile::TempDir — deleted automatically when
    // it drops here, on every return path including `?` early-returns above,
    // since it's owned by this function's stack frame throughout.
    Ok(())
}

/// A directory deleted on drop, regardless of how this function returns.
/// Avoids taking a `tempfile` dependency for one call site: `TempDirGuard`
/// wraps a plain path and removes it in `Drop`.
struct TempDirGuard(PathBuf);
impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
impl std::ops::Deref for TempDirGuard {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

fn tempfile_dir(prefix: &str) -> Result<TempDirGuard> {
    let base = std::env::temp_dir();
    let unique = format!("{prefix}-{}", std::process::id());
    let dir = base.join(unique);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(TempDirGuard(dir))
}

fn clone_tag(
    tag: &str,
    dest: &Path,
) -> Result<()> {
    let status = Command::new("git")
        .args(["clone", "--depth", "1", "--branch", tag, "https://github.com/duckdb/duckdb.git"])
        .arg(dest)
        .status()
        .context("failed to run `git clone` — is git installed and on PATH?")?;
    if !status.success() {
        bail!("git clone of duckdb/duckdb@{tag} failed (exit code {status})");
    }
    Ok(())
}

/// Runs a small embedded Python helper (mirroring the relevant part of
/// `libduckdb-sys`'s own `update_sources.py`) that imports DuckDB's
/// `scripts/package_build.py` and calls `build_package(...)` to get the
/// amalgamated source/include lists, then copies exactly those files into
/// `staging`.
fn collect_sources(
    duckdb_checkout: &Path,
    staging: &Path,
    tag: &str,
) -> Result<Manifest> {
    fs::create_dir_all(staging)?;
    let helper = duckdb_checkout.join("_better_duck_collect_sources.py");
    fs::write(&helper, COLLECT_SOURCES_PY)?;

    // Outside `staging` deliberately: everything under `staging` gets tar'd up
    // verbatim as the vendored archive (see `package_archive`), and this file
    // is just an intermediate handoff to the Rust side, not part of the manifest.
    let output_json = duckdb_checkout.join("_raw_sources.json");
    let status = Command::new("python3")
        .arg(&helper)
        .arg(duckdb_checkout)
        .arg(staging)
        .arg(&output_json)
        .status()
        .context("failed to run python3 — is Python 3 installed and on PATH?")?;
    if !status.success() {
        bail!("package_build.py source collection failed (exit code {status})");
    }

    #[derive(Deserialize)]
    struct RawSources {
        base_cpp_files: Vec<String>,
        base_include_dirs: Vec<String>,
        json_cpp_files: Vec<String>,
        parquet_cpp_files: Vec<String>,
    }
    let raw: RawSources = serde_json::from_str(
        &fs::read_to_string(&output_json).context("reading intermediate sources JSON")?,
    )?;

    Ok(Manifest {
        duckdb_version: tag.to_owned(),
        sources: raw.base_cpp_files,
        json_sources: raw.json_cpp_files,
        parquet_sources: raw.parquet_cpp_files,
        include_dirs: raw.base_include_dirs,
    })
}

/// Copies `staging`'s contents into a `tar.gz` at `dest`, sorted, with a
/// fixed mtime — reproducible archive bytes for a given source tree, so an
/// unrelated re-run doesn't churn the committed archive.
fn package_archive(
    staging: &Path,
    dest: &Path,
) -> Result<()> {
    let file = fs::File::create(dest)?;
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::best());
    let mut builder = tar::Builder::new(encoder);
    let mut entries: Vec<PathBuf> = walkdir::WalkDir::new(staging)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.path().to_path_buf())
        .collect();
    entries.sort();
    for path in entries {
        let rel = path.strip_prefix(staging)?;
        builder.append_path_with_name(&path, rel)?;
    }
    builder.into_inner()?.finish()?;
    Ok(())
}

fn generate_bindings(
    header: &Path,
    dest: &Path,
) -> Result<()> {
    let bindings = bindgen::Builder::default()
        .header(header.to_string_lossy().to_string())
        .allowlist_function("duckdb_.*")
        .allowlist_type("duckdb_.*")
        .allowlist_var("DUCKDB_.*")
        .generate()
        .map_err(|e| anyhow::anyhow!("bindgen failed: {e}"))?;
    bindings.write_to_file(dest).context("writing generated bindings")?;
    Ok(())
}

/// Embedded helper — kept as an external-process Python script (not
/// reimplemented in Rust) so it can call DuckDB's own
/// `scripts/package_build.py` directly, exactly like `libduckdb-sys`'s own
/// `update_sources.py` does. Only used by this xtask, at maintainer-upgrade
/// time; never runs on a consumer's machine.
const COLLECT_SOURCES_PY: &str = r#"
import json
import shutil
import sys
from pathlib import Path

duckdb_checkout = Path(sys.argv[1]).resolve()
staging = Path(sys.argv[2]).resolve()
output_json = Path(sys.argv[3]).resolve()

# `package_build.build_package(target_dir, ...)` (default `folder_name="duckdb"`)
# returns two different path shapes in its source list: ordinary (non-unity-
# build) files come back as `"duckdb/<relative-to-checkout>"` — meant to be
# resolved against `target_dir`'s *parent* — while unity-build (`ub_*.cpp`)
# files it generates itself come back as absolute paths directly under
# `target_dir`. Passing `target_dir = staging/"duckdb"` makes both shapes
# consistent once normalized below: everything ends up expressed relative to
# `staging`, matching where the files were actually written on disk (which is
# what gets tar'd up as the vendored archive).
target_dir = staging / "duckdb"
staging_prefix = staging.as_posix() + "/"

sys.path.append(str(duckdb_checkout / "scripts"))
import package_build

def normalize(path_str):
    p = path_str.replace("\\", "/")
    if p.startswith(staging_prefix):
        return p[len(staging_prefix):]
    return p  # already "duckdb/<relative>" per folder_name

# `core_functions` (basic scalar/aggregate functions like `sum`, `abs`, ...)
# is unconditionally required — DuckDB's own `extension/extension_config.cmake`
# calls it out by name as "loaded by default on every build as [it is] an
# essential part of DuckDB", unlike `json`/`parquet` which really are optional
# file-format support. It's always in `default_linked_extensions` below so its
# `DUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED` fallback in the generated loader is
# unconditionally 1 — no Cargo feature gates it.
extensions = ["core_functions", "json", "parquet"]
# `default_linked_extensions` matters: `build_package` bakes each extension's
# "is it linked" flag into the *content* of the single shared
# `generated_extension_loader_package_build.cpp` file as
# `#ifndef DUCKDB_EXTENSION_X_LINKED / #define ... <default> / #endif`. For
# `json`/`parquet` that fallback is 0 (off), and the `#ifndef` guard means our
# own `-D DUCKDB_EXTENSION_X_LINKED` (added by `better-duck-sys/build.rs` only
# when the matching Cargo feature is on) wins whenever it's present. This file
# is compiled unconditionally as a "base" source, so json/parquet's behavior
# must be controlled entirely by our own `-D` flags, not by whatever extension
# set happened to be passed to `build_package` here.
(source_list, include_list, _) = package_build.build_package(
    str(target_dir), extensions, False, default_linked_extensions=["core_functions"]
)
loader = duckdb_checkout / "generated_extension_loader_package_build.cpp"
loader.unlink(missing_ok=True)

sources = {normalize(s) for s in source_list}
# `include_list` isn't run through the `folder_name` convention by
# `build_package` itself (unlike `source_list`) even though the actual
# header files live under `target_dir` just like the sources — prefix it
# the same way by hand so `-I` dirs resolve consistently with `sources`.
includes = {f"duckdb/{i}" for i in include_list}

# A single `build_package` call (rather than one call per extension
# combination) avoids each call clobbering the previous one's copy of the
# shared loader file on disk. Split the one unified list back apart by each
# extension's own source directory instead.
json_sources = {s for s in sources if "duckdb/extension/json/" in s}
parquet_sources = {s for s in sources if "duckdb/extension/parquet/" in s}
base_sources = sources - json_sources - parquet_sources

result = {
    "base_cpp_files": sorted(base_sources),
    "base_include_dirs": sorted(includes),
    "json_cpp_files": sorted(json_sources),
    "parquet_cpp_files": sorted(parquet_sources),
}
with output_json.open("w") as f:
    json.dump(result, f, indent=2, sort_keys=True)
"#;
