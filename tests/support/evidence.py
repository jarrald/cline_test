"""Write test evidence (markdown + JSON) under docs/results/.

Reliability runs must leave artefacts reviewers can inspect: what was injected,
what the control plane did, and whether the SLOs were met.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parents[2] / "docs" / "results"


def write_evidence(name: str, title: str, rows: list[dict], meta: dict | None = None) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **(meta or {})}
    (RESULTS_DIR / f"{name}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    columns = list(rows[0].keys()) if rows else []
    lines = [f"# {title}", "", f"_generated {payload['generated_at']}_", ""]
    if meta:
        lines += [f"* **{key}**: {value}" for key, value in meta.items()]
        lines.append("")
    if columns:
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "|".join(["---"] * len(columns)) + "|")
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
        lines.append("")
    path = RESULTS_DIR / f"{name}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
