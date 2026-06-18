"""Readiness assessment — is the shadow catalog trustworthy enough to begin the
*execute* phases (transcription cutover, eviction)?

Reads the latest parity + transcribe-comparison snapshots the shadow loop writes
and turns them into a clear go/no-go per phase. Pure and read-only.
"""

from __future__ import annotations

import json

from .db import Catalog


def assess_readiness(cat: Catalog) -> dict:
    """Return per-phase go/no-go plus the checks behind it.

    The bar: the catalog has run a parity pass and disagrees with the live fleet
    in zero places, and its transcribe view matches what the live worker reports.
    """
    parity = cat.meta_get("last_parity_report")
    parity = json.loads(parity) if parity else None
    compare = cat.meta_get("last_compare")
    compare = json.loads(compare) if compare else None
    stats = cat.stats()

    checks: list[dict] = []

    def chk(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    if parity:
        c = parity.get("counts", {})
        chk("no untracked files on disk", c.get("live_only", 0) == 0,
            f"{c.get('live_only', 0)} live_only")
        chk("no tracked recordings missing from disk", c.get("catalog_missing", 0) == 0,
            f"{c.get('catalog_missing', 0)} catalog_missing")
        chk("no size mismatches", c.get("size_mismatch", 0) == 0,
            f"{c.get('size_mismatch', 0)} size_mismatch")
    else:
        chk("parity has run", False,
            "no parity snapshot yet — run `python -m catalog parity` or wait for a shadow pass")

    if compare is not None:
        catalog_only = compare.get("catalog_only", 0)
        chk("transcribe view agrees with the live worker", catalog_only == 0,
            f"{catalog_only} catalog_only (catalog would transcribe files the live "
            f"worker doesn't know about)")
    else:
        chk("transcribe comparison has run", False, "no comparison snapshot yet")

    cold = stats.get("cold", {})
    transcription_ready = all(c["ok"] for c in checks)
    # Eviction additionally needs at least one verified cloud copy to evict toward.
    eviction_ready = transcription_ready and cold.get("archived", 0) > 0

    backlog = next((s["count"] for s in stats.get("jobs", [])
                    if s.get("kind") == "transcribe" and s.get("state") == "ready"), 0)

    return {
        "transcription_cutover_ready": transcription_ready,
        "eviction_ready": eviction_ready,
        "checks": checks,
        "blocking": [c["name"] for c in checks if not c["ok"]],
        "summary": {
            "recordings": stats.get("recordings"),
            "transcribe_backlog": backlog,
            "transcripts_done": stats.get("transcripts_done"),
            "archived": cold.get("archived"),
            "evict_candidates": cold.get("evict_candidates"),
            "reclaimable_bytes": cold.get("reclaimable_bytes"),
            "last_parity": cat.meta_get("last_parity"),
        },
    }
