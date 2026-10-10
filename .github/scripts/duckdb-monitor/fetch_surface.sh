#!/usr/bin/env bash
#
# Fetch a normalized DuckDB C API surface for one git ref (tag or branch) of
# duckdb/duckdb, writing it to <out>. Prefers the authoritative `api_spec` YAML
# and falls back to the generated `src/include/duckdb.h` header when a ref
# predates `api_spec`.
#
# Usage: fetch_surface.sh <ref> <out_surface.json>
# Exit 3 => no usable API source for this ref (caller should skip code diff).
set -Eeuo pipefail

ref="${1:?ref required}"
out="${2:?out path required}"
scriptdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="${DUCKDB_REPO:-duckdb/duckdb}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Blobless, shallow, sparse clone of just `api_spec` — a few tens of KB rather
# than the full ~GB history/tree.
if ! git clone --depth 1 --filter=blob:none --sparse \
      "https://github.com/${repo}.git" "$work/duckdb" >/dev/null 2>&1; then
  echo "::warning::clone of ${repo} failed for ref ${ref}"
  exit 3
fi
git -C "$work/duckdb" sparse-checkout set api_spec >/dev/null 2>&1 || true

if ! git -C "$work/duckdb" fetch --depth 1 origin "$ref" >/dev/null 2>&1; then
  echo "::warning::could not fetch ref ${ref} from ${repo}"
  exit 3
fi
git -C "$work/duckdb" checkout -q FETCH_HEAD

if [ -d "$work/duckdb/api_spec" ] && \
   python3 "$scriptdir/extract_spec.py" "$work/duckdb/api_spec" "$out"; then
  echo "surface for ${ref}: from api_spec"
  exit 0
fi

# Fallback: pull just the generated header for this ref and parse it.
echo "::notice::api_spec unavailable for ${ref}; falling back to duckdb.h header."
header="$work/duckdb.h"
http_code="$(
  curl --silent --show-error --location --retry 4 --retry-delay 2 --retry-connrefused \
    --write-out '%{http_code}' --output "$header" \
    "https://raw.githubusercontent.com/${repo}/${ref}/src/include/duckdb.h" || echo "000"
)"
if [ "$http_code" = "200" ] && \
   python3 "$scriptdir/extract_header.py" "$header" "$out"; then
  echo "surface for ${ref}: from duckdb.h header"
  exit 0
fi

echo "::warning::no API surface source (api_spec or header) for ref ${ref}"
exit 3
