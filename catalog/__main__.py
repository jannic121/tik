"""CLI for the strangler catalog (read-only shadow mode).

  python -m catalog stats                      # what the catalog currently knows
  python -m catalog backfill [--control-db P] [--live]
  python -m catalog parity   [--control-db P]  # diff catalog vs the live fleet
  python -m catalog scan-jobs                  # materialise shadow transcribe intent
  python -m catalog jobs                       # job counts by kind/state

CONTROL_PLANE_DB / CATALOG_DB env vars are honoured. Nothing here writes to the
control plane's database.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import backfill as bf
from . import ingest as ing
from . import parity as par
from .db import Catalog


def _control_db(arg) -> str:
    return (arg or os.environ.get("CONTROL_PLANE_DB")
            or str(Path.home() / ".tt-recorder" / "control.sqlite"))


def _dump(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="catalog")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stats")
    b = sub.add_parser("backfill")
    b.add_argument("--control-db")
    b.add_argument("--live", action="store_true", help="also scan the live fleet")
    p = sub.add_parser("parity")
    p.add_argument("--control-db")
    sub.add_parser("jobs")
    sub.add_parser("scan-jobs")
    sub.add_parser("backlog")
    pr = sub.add_parser("promote", help="Phase 2 go-live: create REAL transcribe jobs")
    pr.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)

    cat = Catalog()

    if args.cmd == "stats":
        _dump(cat.stats())

    elif args.cmd == "backfill":
        cdb = _control_db(args.control_db)
        out: dict = {"catalog_db": str(cat.path), "control_db": cdb}
        if Path(cdb).exists():
            out.update(bf.backfill_from_control_db(cat, cdb))
        else:
            print(f"(control db not found at {cdb} — skipping DB import)", file=sys.stderr)
        if args.live:
            live = bf.scan_live(cat, cdb)
            out.update({k: v for k, v in live.items()
                        if k not in ("inventory", "transcript_statuses")})
        _dump(out)

    elif args.cmd == "parity":
        cdb = _control_db(args.control_db)
        if not Path(cdb).exists():
            print(f"control db not found at {cdb}", file=sys.stderr)
            return 1
        inv = bf.collect_live_inventory(cdb)
        rep = par.compute_parity(cat, inv)
        _dump({"checked_at": rep["checked_at"], "live_files": rep["live_files"],
               "catalog_recordings": rep["catalog_recordings"], "counts": rep["counts"],
               "note": "full lists in catalog_meta.last_parity_report"})

    elif args.cmd == "scan-jobs":
        n = ing.generate_shadow_jobs(cat)
        print(f"created {n} shadow transcribe job(s)")
        _dump(cat.jobs_summary())

    elif args.cmd == "jobs":
        _dump(cat.jobs_summary())

    elif args.cmd == "backlog":
        bl = cat.transcribe_backlog()
        _dump({"backlog": len(bl),
               "sample": [r["filename"] for r in bl[:20]]})

    elif args.cmd == "promote":
        n = ing.promote_backlog(cat, limit=args.limit)
        print(f"created {n} REAL (non-shadow) transcribe job(s) — Phase 2 is now live "
              f"for those; start a worker with: python worker_transcribe.py run --execute")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
