#!/usr/bin/env python3
"""Render a surface diff (``changes.json``) as a Markdown section for issues.

Usage:
    python render_changes.py <changes.json> <old_label> <new_label>

Prints Markdown to stdout. Lists are capped so an unusually large diff cannot
produce a multi-megabyte issue body; the cap is noted inline when hit.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List

import surface

MAX_ITEMS = 80


def _capped(items: List[Any]) -> List[Any]:
    return items[:MAX_ITEMS]


def _overflow_note(items: List[Any]) -> str:
    extra = len(items) - MAX_ITEMS
    return f"\n_…and {extra} more (truncated)._\n" if extra > 0 else ""


def render(changes: Dict[str, Any], old_label: str, new_label: str) -> str:
    out: List[str] = []
    out.append(f"## C API surface changes (`api_spec`, `{old_label}` → `{new_label}`)")
    out.append("")

    if not changes.get("hasChanges"):
        out.append("No C API surface changes were detected between these versions.")
        out.append("")
        return "\n".join(out)

    counts = changes.get("counts", {})
    out.append(
        "**Summary:** "
        + ", ".join(
            f"{v} {k}" for k, v in counts.items() if v
        )
        + "."
    )
    out.append("")

    added = changes.get("added", [])
    if added:
        out.append(f"### Added ({len(added)})")
        out.append("")
        for item in _capped(added):
            since = f" — since `{item['since']}`" if item.get("since") else ""
            status = f" _({item['status']})_" if item.get("status") not in ("", "stable") else ""
            out.append(f"- `{item['signature']}`{since}{status}")
        out.append(_overflow_note(added))

    removed = changes.get("removed", [])
    if removed:
        out.append(f"### Removed ({len(removed)})")
        out.append("")
        for item in _capped(removed):
            out.append(f"- `{item['signature']}`")
        out.append(_overflow_note(removed))

    sig = changes.get("signatureChanged", [])
    if sig:
        out.append(f"### Signature changed ({len(sig)})")
        out.append("")
        for item in _capped(sig):
            out.append(f"- `{item['symbol']}`")
            out.append(f"  - before: `{item['old']}`")
            out.append(f"  - after:  `{item['new']}`")
        out.append(_overflow_note(sig))

    dep = changes.get("deprecated", [])
    if dep:
        out.append(f"### Deprecated ({len(dep)})")
        out.append("")
        for item in _capped(dep):
            since = f" — since `{item['since']}`" if item.get("since") else ""
            out.append(f"- `{item['symbol']}`{since}")
        out.append(_overflow_note(dep))

    stab = changes.get("stabilized", [])
    if stab:
        out.append(f"### Stabilized (unstable → stable) ({len(stab)})")
        out.append("")
        for item in _capped(stab):
            since = f" — since `{item['since']}`" if item.get("since") else ""
            out.append(f"- `{item['symbol']}`{since}")
        out.append(_overflow_note(stab))

    types = changes.get("typesChanged", [])
    if types:
        out.append(f"### Structs / enums / callbacks ({len(types)})")
        out.append("")
        for item in _capped(types):
            out.append(f"- `{item['name']}` ({item.get('kind', '')}) — {item.get('detail', '')}")
        out.append(_overflow_note(types))

    out.append("")
    return "\n".join(out)


def main(argv: List[str]) -> int:
    if len(argv) != 4:
        sys.stderr.write("usage: render_changes.py <changes.json> <old_label> <new_label>\n")
        return 2
    # The rendered Markdown contains non-ASCII (→, …); force UTF-8 stdout so the
    # output is correct regardless of the runner/console locale.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except AttributeError:
        pass
    changes = surface.load_json(argv[1])
    sys.stdout.write(render(changes, argv[2], argv[3]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
