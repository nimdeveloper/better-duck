#!/usr/bin/env python3
"""Diff two normalized C API surfaces into a structured change set.

Usage:
    python diff_surface.py <old_surface.json> <new_surface.json> <out_changes.json>

Deterministic: every list is sorted by symbol/name so the resulting issue body is
byte-stable when nothing changed. A missing/empty old surface (first baseline)
simply reports everything as added — the workflow decides whether to render that.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List

import surface


def _sig_key(fn: Dict[str, Any]) -> Any:
    """The ABI-relevant signature: return type + ordered parameter shape.

    Parameter names are included because a name change in the spec usually marks
    a real semantic change; description/role/lifecycle are intentionally excluded."""
    return (
        fn.get("returnType", ""),
        tuple(
            (p.get("type"), p.get("indirection"), p.get("kind"), p.get("const"), p.get("name"))
            for p in (fn.get("params") or [])
        ),
    )


def diff(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    old_fns: Dict[str, Any] = old.get("functions", {}) or {}
    new_fns: Dict[str, Any] = new.get("functions", {}) or {}

    added, removed, sig_changed, deprecated, stabilized = [], [], [], [], []

    for sym in sorted(new_fns.keys() - old_fns.keys()):
        fn = new_fns[sym]
        added.append(
            {
                "symbol": sym,
                "signature": surface.render_signature(sym, fn),
                "since": fn.get("since", ""),
                "status": fn.get("status", ""),
            }
        )

    for sym in sorted(old_fns.keys() - new_fns.keys()):
        fn = old_fns[sym]
        removed.append({"symbol": sym, "signature": surface.render_signature(sym, fn)})

    for sym in sorted(old_fns.keys() & new_fns.keys()):
        ofn, nfn = old_fns[sym], new_fns[sym]
        if _sig_key(ofn) != _sig_key(nfn):
            sig_changed.append(
                {
                    "symbol": sym,
                    "old": surface.render_signature(sym, ofn),
                    "new": surface.render_signature(sym, nfn),
                }
            )
        if nfn.get("deprecated") and not ofn.get("deprecated"):
            deprecated.append({"symbol": sym, "since": nfn.get("since", "")})
        if ofn.get("status") == "unstable" and nfn.get("status") == "stable":
            stabilized.append({"symbol": sym, "since": nfn.get("since", "")})

    old_types: Dict[str, Any] = old.get("types", {}) or {}
    new_types: Dict[str, Any] = new.get("types", {}) or {}
    types_changed: List[Dict[str, Any]] = []
    for name in sorted(new_types.keys() - old_types.keys()):
        types_changed.append({"name": name, "kind": new_types[name].get("kind", ""), "detail": "added"})
    for name in sorted(old_types.keys() - new_types.keys()):
        types_changed.append({"name": name, "kind": old_types[name].get("kind", ""), "detail": "removed"})
    for name in sorted(old_types.keys() & new_types.keys()):
        if old_types[name].get("hash") != new_types[name].get("hash"):
            types_changed.append(
                {"name": name, "kind": new_types[name].get("kind", ""), "detail": "modified"}
            )

    counts = {
        "added": len(added),
        "removed": len(removed),
        "signatureChanged": len(sig_changed),
        "deprecated": len(deprecated),
        "stabilized": len(stabilized),
        "typesChanged": len(types_changed),
    }
    return {
        "hasChanges": any(counts.values()),
        "oldDuckdbVersions": old.get("duckdbVersions", []),
        "newDuckdbVersions": new.get("duckdbVersions", []),
        "added": added,
        "removed": removed,
        "signatureChanged": sig_changed,
        "deprecated": deprecated,
        "stabilized": stabilized,
        "typesChanged": types_changed,
        "counts": counts,
    }


def main(argv: List[str]) -> int:
    if len(argv) != 4:
        sys.stderr.write("usage: diff_surface.py <old.json> <new.json> <out_changes.json>\n")
        return 2
    old = surface.load_json(argv[1]) if argv[1] != "-" else {"functions": {}, "types": {}}
    new = surface.load_json(argv[2])
    result = diff(old, new)
    surface.dump_json(result, argv[3])
    sys.stderr.write(f"diff counts: {result['counts']} hasChanges={result['hasChanges']}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
