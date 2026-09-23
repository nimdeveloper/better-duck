#!/usr/bin/env bash
# Build and run every example in the suite, asserting each exits cleanly.
#
# Each example binary prints a trace AND asserts its expected results, so a clean exit
# means the example is correct. This script auto-discovers binaries from each crate's
# `src/bin/*.rs`, so it needs no edits as examples are added.
set -euo pipefail
cd "$(dirname "$0")" # the examples/ directory
SHARED_TARGET="$(pwd)/target"

run_crate() {
    local dir="$1"
    shift
    local extra=("$@")
    [ -d "$dir/src/bin" ] || return 0
    for f in "$dir"/src/bin/*.rs; do
        [ -e "$f" ] || continue
        local bin
        bin="$(basename "$f" .rs)"
        echo "=== $dir :: $bin ==="
        cargo run -q --manifest-path "$dir/Cargo.toml" "${extra[@]}" --bin "$bin"
    done
}

# Groups that are members of this workspace (chrono + decimal + pool + async + udf).
for dir in [0-9]*-*/; do
    run_crate "${dir%/}"
done

# Detached feature-fork crates (own workspaces / exclusive feature sets).
# Reuse the warm shared target dir so they don't recompile DuckDB from scratch.
# NOTE: feature-extensions compiles the JSON+Parquet DuckDB variant on first run
# (a one-time ~10-15 min C++ build).
for dir in feature-*/; do
    run_crate "${dir%/}" --target-dir "$SHARED_TARGET"
done

echo
echo "ALL EXAMPLES PASSED"
