#!/usr/bin/env python3
"""Phase 2 — stateless transcribe worker (shadow-first).

Reads the catalog's *real* transcribe backlog (recordings on hot storage with no
done transcript) and, when run with --execute, transcribes the next one with
faster-whisper, writes the .txt next to the .mp4, and records the result in the
catalog. Dry-run by default: it prints the plan and changes nothing.

It runs wherever it can see both the catalog DB and the recording files (i.e.
colocated with a storage node). For a fully remote worker, a claim API on the hub
is the next step — but for shadow validation, direct catalog access is all we need.

  python worker_transcribe.py plan                 # read-only: show the backlog
  python worker_transcribe.py run                  # loop, DRY-RUN (no whisper, no writes)
  python worker_transcribe.py run --execute        # loop, actually transcribe (LIVE)
  python worker_transcribe.py run --execute --once  # do a single file and stop

Env: CATALOG_DB (catalog path), WHISPER_MODEL/_DEVICE/_COMPUTE_TYPE (when executing).
Nothing executes until there are NON-shadow transcribe jobs — i.e. until you flip
Phase 2 live with `python -m catalog promote` — so --execute is inert before then.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from catalog import Catalog


def _fmt(rec: dict, path: str | None) -> str:
    where = path or "(no local copy — can't transcribe here)"
    return f"  {rec['filename']}  [{rec['creator']}]  -> {where}"


def plan(cat: Catalog, limit: int) -> None:
    backlog = cat.transcribe_backlog(limit=limit or None)
    print(f"transcribe backlog: {len(backlog)} recording(s)")
    for rec in backlog[: (limit or 50)]:
        print(_fmt(rec, cat.local_path(rec["id"])))
    if not limit and len(backlog) > 50:
        print(f"  … and {len(backlog) - 50} more")


def _transcribe(cat: Catalog, job: dict) -> None:
    """Execute one real transcribe job. Imports faster-whisper lazily."""
    rid = job["recording_id"]
    rec = cat.get_recording(rid)
    path = cat.local_path(rid) if rec else None
    if not rec or not path or not Path(path).exists():
        cat.fail_job(job["id"], "no local copy to transcribe", backoff=3600)
        return
    from faster_whisper import WhisperModel  # lazy: only needed when executing

    model = WhisperModel(os.environ.get("WHISPER_MODEL", "base"),
                         device=os.environ.get("WHISPER_DEVICE", "cpu"),
                         compute_type=os.environ.get("WHISPER_COMPUTE_TYPE", "int8"))
    segs, info = model.transcribe(path, word_timestamps=True)
    lines, words = [], 0
    for s in segs:
        t = s.text.strip()
        if t:
            h = int(s.start) // 3600; m = int(s.start) // 60 % 60; sec = int(s.start) % 60
            lines.append(f"[{h:02d}:{m:02d}:{sec:02d}] {t}")
        words += len(s.words or [])
    txt = Path(path).with_suffix(".txt")
    header = f"# language={info.language} duration={round(info.duration,1)}s model={os.environ.get('WHISPER_MODEL','base')}\n\n"
    txt.write_text(header + "\n".join(lines), encoding="utf-8")
    cat.set_transcript(rid, "done", tier="local", key=str(txt),
                       language=info.language, words=words)
    cat.complete_job(job["id"])
    print(f"  ✓ transcribed {rec['filename']} ({info.language}, {words} words) -> {txt.name}")


def run(cat: Catalog, execute: bool, once: bool, interval: int, worker: str) -> None:
    mode = "EXECUTE (live)" if execute else "DRY-RUN (no whisper, no writes)"
    print(f"transcribe worker [{mode}] · catalog={cat.path}")
    while True:
        if execute:
            job = cat.claim_job(kinds=["transcribe"], worker=worker)  # non-shadow only
            if job:
                rec = cat.get_recording(job["recording_id"]) or {}
                print(f"claimed {rec.get('filename', job['recording_id'])}")
                try:
                    _transcribe(cat, job)
                except Exception as e:
                    cat.fail_job(job["id"], str(e))
                    print(f"  ✗ failed: {e}")
                if once:
                    return
                continue
            # no real jobs to do
            if once:
                print("no non-shadow transcribe jobs (flip live with `python -m catalog promote`)")
                return
        else:
            plan(cat, limit=0)
            if once:
                return
        time.sleep(interval)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="worker_transcribe")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan"); p.add_argument("--limit", type=int, default=0)
    r = sub.add_parser("run")
    r.add_argument("--execute", action="store_true", help="actually transcribe (LIVE)")
    r.add_argument("--once", action="store_true")
    r.add_argument("--interval", type=int, default=30)
    r.add_argument("--worker", default=os.environ.get("HOSTNAME", "worker"))
    args = ap.parse_args(argv)

    cat = Catalog()
    if args.cmd == "plan":
        plan(cat, args.limit)
    elif args.cmd == "run":
        run(cat, args.execute, args.once, args.interval, args.worker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
