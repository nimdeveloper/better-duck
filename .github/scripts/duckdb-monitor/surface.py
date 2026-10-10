"""Shared helpers for the DuckDB C API surface tooling.

A *surface* is a normalized, version-stable snapshot of DuckDB's C API extracted
from the upstream ``api_spec`` YAML (or, as a fallback, the generated
``duckdb.h`` header). The extractors emit it, the differ compares two of them,
and the renderer turns a diff into Markdown for GitHub issues.

This module is a library only — it is imported by the ``*.py`` entry points in
this directory and is never executed directly.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List


# Exit code an extractor uses to signal "no spec/source here, fall back" so the
# workflow can distinguish an empty-but-valid source from a hard error.
EXIT_NO_SOURCE = 3


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def dump_json(obj: Any, path: str) -> None:
    """Writes ``obj`` as deterministic JSON (sorted keys) so a surface file is
    byte-stable across runs and its sha256 only changes when content does."""
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(obj, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")


def normalize_param(name: str, raw: Any) -> Dict[str, Any]:
    """Normalizes one parameter entry from the spec into a stable shape.

    `raw` is the per-parameter map (``{type, indirection?, kind?, const?,
    description?}``); `description` is intentionally dropped (prose, not ABI)."""
    raw = raw or {}
    return {
        "name": name,
        "type": str(raw.get("type", "")),
        "indirection": int(raw.get("indirection", 0) or 0),
        "kind": str(raw.get("kind", "") or ""),
        "const": bool(raw.get("const", False)),
    }


def render_param(param: Dict[str, Any]) -> str:
    """Renders one normalized parameter as a C-ish fragment, e.g. ``const str *name``."""
    pieces: List[str] = []
    if param.get("const"):
        pieces.append("const")
    stars = "*" * int(param.get("indirection", 0) or 0)
    pieces.append(param["type"] + stars if stars else param["type"])
    if param.get("name"):
        pieces.append(param["name"])
    out = " ".join(pieces)
    if param.get("kind") == "OUT":
        out += " /*out*/"
    return out


def render_signature(symbol: str, fn: Dict[str, Any]) -> str:
    """Builds a readable C-ish prototype for a normalized function entry."""
    params = fn.get("params") or []
    rendered = ", ".join(render_param(p) for p in params) if params else "void"
    return f"{fn.get('returnType', 'void')} {symbol}({rendered})"
