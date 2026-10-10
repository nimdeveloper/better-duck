#!/usr/bin/env python3
"""Extract a normalized C API surface from DuckDB's upstream ``api_spec`` tree.

Usage:
    python extract_spec.py <api_spec_dir> <out_surface.json>

``<api_spec_dir>`` is the repository's top-level ``api_spec`` folder (containing
``v1``/``v2`` subdirectories, each with ``metadata.yaml`` + per-module YAML).
Exits with ``surface.EXIT_NO_SOURCE`` (3) when no usable spec is present so the
caller can fall back to the header extractor or the Markdown path.

All YAML is read with ``yaml.safe_load`` — upstream content is untrusted.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from typing import Any, Dict, List

import yaml

import surface


def _coerce_versions(meta: Dict[str, Any]) -> List[str]:
    raw = meta.get("versions")
    out: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict) and "name" in item:
                out.append(str(item["name"]))
    elif isinstance(raw, dict):
        out.extend(str(k) for k in raw.keys())
    return sorted(set(out))


def _latest_lifecycle(lifecycle: Any) -> Dict[str, str]:
    """Returns ``{status, since}`` from the last ``[status, version, date]`` entry."""
    status, since = "", ""
    if isinstance(lifecycle, list) and lifecycle:
        last = lifecycle[-1]
        if isinstance(last, list) and last:
            status = str(last[0])
            since = str(last[1]) if len(last) > 1 else ""
    return {"status": status, "since": since}


def _has_deprecated(lifecycle: Any) -> bool:
    if isinstance(lifecycle, list):
        return any(
            isinstance(e, list) and e and str(e[0]) == "deprecated" for e in lifecycle
        )
    return False


def _normalize_function(prefix: str, module: str, name: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    params_raw = raw.get("parameters") or {}
    params: List[Dict[str, Any]] = []
    if isinstance(params_raw, dict):
        # YAML preserves insertion order, which is the declared parameter order.
        for pname, pdef in params_raw.items():
            params.append(surface.normalize_param(str(pname), pdef))
    life = raw.get("lifecycle")
    latest = _latest_lifecycle(life)
    return {
        "symbol": prefix + name,
        "module": module,
        "role": str(raw.get("role", "") or ""),
        "belongsTo": str(raw.get("belongs_to", "") or ""),
        "returnType": str(raw.get("return_type", "void") or "void"),
        "params": params,
        "status": latest["status"],
        "since": latest["since"],
        "deprecated": _has_deprecated(life) or latest["status"] == "deprecated",
        "lifecycle": life if isinstance(life, list) else [],
    }


def _type_hash(detail: Any) -> str:
    blob = json.dumps(detail, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _iter_spec_files(version_dir: str):
    for root, _dirs, files in os.walk(version_dir):
        for fname in sorted(files):
            if not fname.endswith((".yaml", ".yml")):
                continue
            if fname == "metadata.yaml":
                continue
            yield os.path.join(root, fname)


def extract(api_spec_dir: str) -> Dict[str, Any]:
    functions: Dict[str, Any] = {}
    types: Dict[str, Any] = {}
    spec_versions: List[str] = []
    duckdb_versions: List[str] = []
    prefixes: Dict[str, str] = {}

    for entry in sorted(os.listdir(api_spec_dir)):
        version_dir = os.path.join(api_spec_dir, entry)
        meta_path = os.path.join(version_dir, "metadata.yaml")
        if not (os.path.isdir(version_dir) and os.path.isfile(meta_path)):
            continue
        with open(meta_path, "r", encoding="utf-8") as handle:
            meta = yaml.safe_load(handle) or {}
        prefix = str(meta.get("prefix", "") or "")
        spec_versions.append(entry)
        prefixes[entry] = prefix
        duckdb_versions.extend(_coerce_versions(meta))

        for path in _iter_spec_files(version_dir):
            with open(path, "r", encoding="utf-8") as handle:
                doc = yaml.safe_load(handle) or {}
            if not isinstance(doc, dict):
                continue
            module = str(doc.get("module", "") or "")
            fns = doc.get("functions")
            if isinstance(fns, dict):
                for fname, fdef in fns.items():
                    if not isinstance(fdef, dict):
                        continue
                    norm = _normalize_function(prefix, module, str(fname), fdef)
                    functions[norm["symbol"]] = norm
            # Any other dict-of-dicts top-level key is a type collection
            # (enums/structs/callbacks under common/, whatever it is named).
            for key, value in doc.items():
                if key in ("module", "functions"):
                    continue
                if isinstance(value, dict):
                    for tname, tdef in value.items():
                        types[str(tname)] = {
                            "kind": str(key),
                            "specVersion": entry,
                            "hash": _type_hash(tdef),
                        }

    return {
        "schema": 1,
        "specVersions": sorted(set(spec_versions)),
        "prefixes": prefixes,
        "duckdbVersions": sorted(set(duckdb_versions)),
        "functions": functions,
        "types": types,
    }


def main(argv: List[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write("usage: extract_spec.py <api_spec_dir> <out_surface.json>\n")
        return 2
    api_spec_dir, out_path = argv[1], argv[2]
    if not os.path.isdir(api_spec_dir):
        sys.stderr.write(f"no api_spec directory at {api_spec_dir}\n")
        return surface.EXIT_NO_SOURCE
    result = extract(api_spec_dir)
    if not result["functions"] and not result["types"]:
        sys.stderr.write("api_spec present but no functions/types parsed\n")
        return surface.EXIT_NO_SOURCE
    surface.dump_json(result, out_path)
    sys.stderr.write(
        f"extracted {len(result['functions'])} functions, {len(result['types'])} types "
        f"from spec versions {result['specVersions']}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
