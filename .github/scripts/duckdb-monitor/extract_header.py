#!/usr/bin/env python3
"""Fallback C API surface extractor: parse the generated ``duckdb.h`` header.

Usage:
    python extract_header.py <duckdb.h> <out_surface.json>

Used only when ``api_spec`` is unavailable for a ref (older DuckDB lines). It
regex-parses the exported-function macro (``DUCKDB_C_API``, or legacy
``DUCKDB_API``) prototypes and ``typedef`` names into the same surface shape the
spec extractor emits (no lifecycle/since; status defaults to ``stable``), so the
differ treats both sources identically. Best-effort by design — comparisons are
only ever header-vs-header.
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List

import surface

# Strip C comments before scanning so a prototype inside a comment is ignored.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"//[^\n]*")

# DuckDB renamed the public export macro from `DUCKDB_API` to `DUCKDB_C_API`;
# modern headers (1.5+) use `DUCKDB_C_API`. Match either so the fallback works
# across the rename. The required whitespace after the macro keeps the version
# guard `DUCKDB_API_VERSION_AT_LEAST(...)` (no space before `_VERSION`) from
# matching as a prototype.
_FUNC = re.compile(
    r"DUCKDB_(?:C_)?API\s+(?P<ret>[A-Za-z_][\w\s\*]*?)\s+(?P<name>duckdb_\w+)\s*\((?P<params>[^;{]*?)\)\s*;",
    re.DOTALL,
)


def _parse_param(raw: str) -> Dict[str, Any]:
    text = " ".join(raw.split())
    if text in ("", "void"):
        return {}
    indirection = text.count("*")
    const = bool(re.search(r"\bconst\b", text))
    cleaned = text.replace("*", " ").replace("const", " ")
    tokens = cleaned.split()
    name = ""
    if len(tokens) > 1 and re.fullmatch(r"[A-Za-z_]\w*", tokens[-1]):
        name = tokens[-1]
        type_tokens = tokens[:-1]
    else:
        type_tokens = tokens
    return {
        "name": name,
        "type": " ".join(type_tokens),
        "indirection": indirection,
        "kind": "",
        "const": const,
    }


def _split_params(params: str) -> List[str]:
    """Splits a parameter list on top-level commas (ignores commas inside parens)."""
    out, depth, start = [], 0, 0
    for i, ch in enumerate(params):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append(params[start:i])
            start = i + 1
    out.append(params[start:])
    return [p for p in (s.strip() for s in out) if p]


def extract(header_text: str) -> Dict[str, Any]:
    text = _LINE_COMMENT.sub("", _BLOCK_COMMENT.sub("", header_text))
    functions: Dict[str, Any] = {}
    for match in _FUNC.finditer(text):
        name = match.group("name")
        params = [p for p in (_parse_param(x) for x in _split_params(match.group("params"))) if p]
        functions[name] = {
            "symbol": name,
            "module": "",
            "role": "",
            "belongsTo": "",
            "returnType": " ".join(match.group("ret").split()),
            "params": params,
            "status": "stable",
            "since": "",
            "deprecated": False,
            "lifecycle": [],
        }

    # Header-derived type tracking is intentionally omitted. Struct/union
    # typedefs span braces (which the prototype-oriented scan cannot reliably
    # delimit without a real C parser), and DuckDB regenerated the header from
    # `api_spec` at 1.5.6, which rewrote every typedef's *declaration style*
    # without changing its meaning — a naive typedef scan reports all of them as
    # spurious changes across that boundary. Functions are the ABI-critical
    # signal a header can be trusted for; the richer `api_spec` path (1.5.6+)
    # carries full, reliable type tracking.
    types: Dict[str, Any] = {}

    return {
        "schema": 1,
        "specVersions": ["header"],
        "prefixes": {"header": "duckdb_"},
        "duckdbVersions": [],
        "functions": functions,
        "types": types,
    }


def main(argv: List[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write("usage: extract_header.py <duckdb.h> <out_surface.json>\n")
        return 2
    try:
        with open(argv[1], "r", encoding="utf-8", errors="replace") as handle:
            header_text = handle.read()
    except OSError as exc:
        sys.stderr.write(f"cannot read header: {exc}\n")
        return surface.EXIT_NO_SOURCE
    result = extract(header_text)
    if not result["functions"]:
        sys.stderr.write("no DUCKDB_API prototypes found in header\n")
        return surface.EXIT_NO_SOURCE
    surface.dump_json(result, argv[2])
    sys.stderr.write(f"extracted {len(result['functions'])} functions from header\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
