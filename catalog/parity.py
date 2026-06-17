"""Compare the catalog against a live inventory and report drift.

This is the whole point of Phase 0: prove the catalog matches reality before
anything depends on it. Pure and read-only — it only writes the report into the
catalog's own meta table.
"""

from __future__ import annotations

import json
import time

from .db import Catalog


def compute_parity(cat: Catalog, live_inventory: list[dict]) -> dict:
    """Diff catalog recordings against a live .mp4 inventory.

    Reports three kinds of drift:
      * live_only       — bytes on disk with no catalog row (untracked)
      * catalog_missing — catalog rows we expect to exist but didn't see live
      * size_mismatch   — same file, different byte size
    """
    live: dict[str, dict] = {}
    for f in live_inventory:
        fn = f.get("filename")
        if not fn or not fn.endswith(".mp4") or fn.endswith("_flv.mp4"):
            continue
        e = live.setdefault(fn, {"size": f.get("size_bytes"), "stores": set()})
        if f.get("store"):
            e["stores"].add(f["store"])
        if e["size"] is None:
            e["size"] = f.get("size_bytes")

    cat_rows = {r["filename"]: dict(r) for r in
                cat.conn.execute("SELECT * FROM recordings").fetchall()}

    live_only, size_mismatch, catalog_missing = [], [], []
    for fn, e in live.items():
        r = cat_rows.get(fn)
        if not r:
            live_only.append({"filename": fn, "stores": sorted(s for s in e["stores"] if s)})
        elif e["size"] and r["byte_size"] and e["size"] != r["byte_size"]:
            size_mismatch.append({"filename": fn, "live": e["size"], "catalog": r["byte_size"]})
    for fn, r in cat_rows.items():
        if r["state"] in ("evicted", "missing"):
            continue                        # legitimately not on hot storage
        if fn not in live:
            catalog_missing.append({"filename": fn, "state": r["state"]})

    report = {
        "checked_at": time.time(),
        "live_files": len(live),
        "catalog_recordings": len(cat_rows),
        "counts": {
            "live_only": len(live_only),
            "catalog_missing": len(catalog_missing),
            "size_mismatch": len(size_mismatch),
        },
        "live_only": live_only,
        "catalog_missing": catalog_missing,
        "size_mismatch": size_mismatch,
    }
    cat.meta_set("last_parity", time.time())
    cat.meta_set("last_parity_report", json.dumps(report)[:200000])
    return report
