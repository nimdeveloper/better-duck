#!/usr/bin/env python3
"""Fallback C API surface extractor: parse the generated ``duckdb.h`` header.

Usage:
    python extract_header.py <duckdb.h> <out_surface.json>

Used only when ``api_spec`` is unavailable for a ref (older DuckDB lines). It
regex-parses ``DUCKDB_API`` function prototypes and ``typedef`` names into the
same surface shape the spec extractor emits (no lifecycle/since; status defaults
to ``stable``), so the differ treats both sources identically. Best-effort by
design — comparisons are only ever header-vs-header.
"""

from __future__ import annotations

import hashlib
import re
import sys
from typing import Any, Dict, List

import surface

# Strip C comments before scanning so a prototype inside a comment is ignored.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"//[^\n]*")

_FUNC = re.compile(
    r"DUCKDB_API\s+(?P<ret>[A-Za-z_][\w\s\*]*?)\s+(?P<name>duckdb_\w+)\s*\((?P<params>[^;{]*?)\)\s*;",
    re.DOTALL,
)
_TYPEDEF = re.compile(r"typedef\b(?P<body>[^;{]*?)\b(?P<name>duckdb_\w+)\s*;", re.DOTALL)


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

    types: Dict[str, Any] = {}
    for match in _TYPEDEF.finditer(text):
        name = match.group("name")
        if name in functions:
            continue
        digest = hashlib.sha256(" ".join(match.group(0).split()).encode("utf-8")).hexdigest()[:16]
        types[name] = {"kind": "typedef", "specVersion": "header", "hash": digest}

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
