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
    cold = stats.get("cold", {})

    pc = (parity or {}).get("counts", {})
    have_parity = parity is not None
    have_compare = compare is not None
    live_only = pc.get("live_only", 0)
    catalog_missing = pc.get("catalog_missing", 0)
    size_mismatch = pc.get("size_mismatch", 0)
    # Files the worker already transcribed that the catalog still lists pending —
    # an actual tracking gap (usually an unregistered storage server). catalog_only
    # (files not yet shipped to the transcription box) is normal lag, NOT a blocker.
    stale = (compare or {}).get("already_done_live", 0)

    checks = [
        {"name": "parity has run", "phase": "both", "ok": have_parity,
         "detail": "" if have_parity else "run `python -m catalog parity` or wait for a shadow pass"},
        {"name": "transcript tracking is current with the worker", "phase": "transcription",
         "ok": have_compare and stale == 0,
         "detail": "" if (have_compare and stale == 0) else
         f"{stale} files the worker already transcribed are still listed pending "
         f"(register the storage server holding those transcripts)"},
        {"name": "catalog knows every stored recording", "phase": "transcription",
         "ok": have_parity and catalog_missing == 0,
         "detail": "" if catalog_missing == 0 else f"{catalog_missing} catalog_missing"},
        {"name": "no untracked files on disk", "phase": "eviction",
         "ok": have_parity and live_only == 0,
         "detail": "" if live_only == 0 else f"{live_only} live_only (usually a file mid-record)"},
        {"name": "no size mismatches", "phase": "eviction",
         "ok": have_parity and size_mismatch == 0,
         "detail": "" if size_mismatch == 0 else f"{size_mismatch} size_mismatch (usually a file mid-record)"},
    ]

    # Transcription cutover only needs accurate TRACKING (transcripts current, no
    # lost recordings). Eviction DELETES, so it additionally needs the on-disk
    # inventory to match exactly AND somewhere (cloud) to have evicted toward.
    transcription_ready = have_parity and have_compare and stale == 0 and catalog_missing == 0
    eviction_ready = (transcription_ready and live_only == 0 and size_mismatch == 0
                      and cold.get("archived", 0) > 0)

    blk_t = [c["name"] for c in checks
             if c["phase"] in ("transcription", "both") and not c["ok"]]
    blk_e = [c["name"] for c in checks if not c["ok"]]
    if not (cold.get("archived", 0) > 0):
        blk_e.append("nothing archived to cloud yet")

    backlog = next((s["count"] for s in stats.get("jobs", [])
                    if s.get("kind") == "transcribe" and s.get("state") == "ready"), 0)

    return {
        "transcription_cutover_ready": transcription_ready,
        "eviction_ready": eviction_ready,
        "checks": checks,
        "blocking_transcription": blk_t,
        "blocking_eviction": blk_e,
        "blocking": blk_t if not transcription_ready else blk_e,
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
