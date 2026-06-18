"""Populate the catalog from existing sources — read-only w.r.t. the running system.

Two sources:
  * the control plane's ``control.sqlite`` (opened read-only) — gives the recording
    inventory and coarse state from the ``transfers`` table;
  * a live inventory scan of the fleet (each backend ``/files`` + each storage
    ``/files/inventory``) — gives exact on-disk locations and sizes.

Neither writes to the control plane. Everything lands in the separate catalog DB.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Optional

from .db import Catalog
from .names import parse_recording_name


def _ro(control_db: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{control_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def backfill_from_control_db(cat: Catalog, control_db: str | Path) -> dict:
    """Import recordings from the control plane's transfers table."""
    src = _ro(control_db)
    imported = 0
    try:
        try:
            rows = src.execute(
                "SELECT backend_pk, backend_label, storage_pk, storage_label, username, "
                "filename, src_path, size_bytes, status, "
                "COALESCE(recorder_deleted,0) AS recorder_deleted FROM transfers"
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []                       # very old DB without these columns
        for r in rows:
            state = "stored" if r["status"] == "done" else "discovered"
            rid = cat.upsert_recording(
                filename=r["filename"], creator=r["username"], node=r["backend_label"],
                backend_pk=r["backend_pk"], byte_size=r["size_bytes"], state=state,
            )
            # Recorder-local copy, unless the upload worker already deleted it.
            if not r["recorder_deleted"] and r["src_path"]:
                cat.add_location(rid, "local", r["backend_label"] or "recorder",
                                 r["src_path"], byte_size=r["size_bytes"])
            # Storage copy (a storage VPS disk is a 'local' hot tier on that node).
            if r["status"] == "done" and r["storage_label"]:
                cat.add_location(rid, "local", r["storage_label"], r["filename"],
                                 byte_size=r["size_bytes"], verified=True)
            imported += 1
    finally:
        src.close()
    cat.meta_set("last_backfill", time.time())
    return {"transfers_imported": imported}


def ingest_inventory(cat: Catalog, inventory: list[dict]) -> dict:
    """Fold a live inventory into the catalog.

    Each item: {filename, path, username, size_bytes, store, [tier]}.
    .mp4 -> a recording + a verified location. .txt -> a done transcript on the
    matching recording. _chat.jsonl and other sidecars are ignored in shadow.
    """
    recs = txts = 0
    for f in inventory:
        fn = f.get("filename") or Path(f.get("path", "")).name
        if not fn:
            continue
        store = f.get("store") or "unknown"
        tier = f.get("tier", "local")
        if fn.endswith(".mp4"):
            if fn.endswith("_flv.mp4"):
                continue                    # intermediate, not a finished recording
            rid = cat.upsert_recording(
                filename=fn, creator=f.get("username"), node=store,
                byte_size=f.get("size_bytes"), state="stored",
            )
            cat.add_location(rid, tier, store, f.get("path") or fn,
                             byte_size=f.get("size_bytes"), verified=True)
            recs += 1
        elif fn.endswith(".txt"):
            # Transcript state lives in the transcripts table, NOT the recording
            # state — a recording can be both transcribed and evicted, so the two
            # are tracked independently.
            mp4 = fn[:-4] + ".mp4"
            rec = cat.find_by_filename(mp4)
            if rec:
                cat.set_transcript(rec["id"], "done", tier=tier, store=store,
                                   key=f.get("path"))
                txts += 1
    return {"recordings_seen": recs, "transcripts_seen": txts}


def collect_live_inventory(control_db: str | Path) -> list[dict]:
    """Query every healthy backend and storage server for its file list, using the
    credentials stored in control.sqlite. Best-effort: unreachable nodes are skipped.
    httpx is imported lazily so the rest of the package stays stdlib-only."""
    import httpx

    inv: list[dict] = []
    src = _ro(control_db)
    try:
        try:
            backends = src.execute(
                "SELECT url, auth_token, backend_id FROM backends WHERE last_health_ok=1"
            ).fetchall()
        except sqlite3.OperationalError:
            backends = []
        for b in backends:
            try:
                r = httpx.get(f"{b['url'].rstrip('/')}/files",
                              headers={"Authorization": f"Bearer {b['auth_token']}"}, timeout=10)
                if r.status_code == 200:
                    for user, files in (r.json() or {}).items():
                        for f in files:
                            inv.append({"filename": Path(f["path"]).name, "path": f["path"],
                                        "username": user, "size_bytes": f.get("size_bytes"),
                                        "store": b["backend_id"], "tier": "local"})
            except Exception:
                pass
        try:
            stores = src.execute(
                "SELECT url, token, label FROM storage_servers WHERE last_health_ok=1"
            ).fetchall()
        except sqlite3.OperationalError:
            stores = []
        for s in stores:
            try:
                r = httpx.get(f"{s['url'].rstrip('/')}/files/inventory",
                              headers={"Authorization": f"Bearer {s['token']}"}, timeout=10)
                if r.status_code == 200:
                    for f in (r.json() or []):
                        inv.append({"filename": f.get("filename"), "path": f.get("path"),
                                    "username": f.get("username"), "size_bytes": f.get("size_bytes"),
                                    "store": s["label"], "tier": "local"})
            except Exception:
                pass
    finally:
        src.close()
    return inv


def collect_transcript_statuses(control_db: str | Path) -> dict:
    """R2: ask every healthy storage server for {filename: done|processing|pending}
    via the EXISTING /transcripts/all-statuses endpoint and merge (done wins).

    This needs no code on the storage/transcription servers — that endpoint already
    ships in the current worker. Because it queries every registered server and
    merges by filename, transcripts living on a *different* box than the recording
    are still picked up, wherever they are."""
    import httpx

    rank = {"done": 3, "processing": 2, "pending": 1, "none": 0}
    statuses: dict[str, str] = {}
    src = _ro(control_db)
    try:
        try:
            stores = src.execute(
                "SELECT url, token FROM storage_servers WHERE last_health_ok=1"
            ).fetchall()
        except sqlite3.OperationalError:
            stores = []
        for s in stores:
            try:
                r = httpx.get(f"{s['url'].rstrip('/')}/transcripts/all-statuses",
                              headers={"Authorization": f"Bearer {s['token']}"}, timeout=10)
                if r.status_code == 200 and isinstance(r.json(), dict):
                    for fn, st in r.json().items():
                        if rank.get(st, 0) > rank.get(statuses.get(fn, "none"), 0):
                            statuses[fn] = st
            except Exception:
                pass
    finally:
        src.close()
    return statuses


def ingest_transcript_statuses(cat: Catalog, statuses: dict) -> dict:
    """Fold merged transcript statuses into the catalog. 'done' marks the
    transcript done and cancels any outstanding transcribe job for that recording;
    recording *state* is left alone (transcribed/evicted are independent facts)."""
    done = pending = 0
    for fn, st in (statuses or {}).items():
        if not str(fn).endswith(".mp4"):
            continue
        rec = cat.find_by_filename(fn)
        if not rec:
            continue
        if st == "done":
            cat.set_transcript(rec["id"], "done")
            cat.cancel_open_jobs(rec["id"], "transcribe")
            done += 1
        elif st in ("processing", "pending"):
            cat.set_transcript(rec["id"], "pending")
            pending += 1
    return {"transcribed": done, "transcribing": pending}


def reconcile_states(cat: Catalog, live_filenames: set) -> dict:
    """R1: a recording marked 'stored' whose .mp4 isn't in the live inventory is no
    longer on hot storage — relabel it 'evicted' so by_state reflects what's
    actually on disk. Non-destructive: nothing is deleted, only the label changes."""
    evicted = gone = 0
    # 'stored' not on disk -> evicted (was on hot, now cleaned up).
    # 'discovered' not on disk -> missing (a past failed/pending transfer that
    # never landed). Both stop them polluting drift; 'recording' (in flight) is
    # left alone since its final file isn't written yet.
    rows = cat.conn.execute(
        "SELECT id, filename, state FROM recordings WHERE state IN ('stored','discovered')"
    ).fetchall()
    for r in rows:
        if r["filename"] in live_filenames:
            continue
        if r["state"] == "stored":
            cat.set_state(r["id"], "evicted"); evicted += 1
        else:
            cat.set_state(r["id"], "missing"); gone += 1
        # No local copy → can't be transcribed here; drop any stale transcribe job.
        cat.cancel_open_jobs(r["id"], "transcribe")
    return {"evicted": evicted, "missing": gone}


def collect_archive_status(control_db: str | Path) -> dict:
    """Phase 3: ask every healthy storage server which files are on the cloud
    remote, via /files/archive-status (reads existing .archived markers). Merged
    by filename. Storage servers not yet updated with that endpoint just 404 and
    are skipped — so this degrades gracefully until the next storage push."""
    import httpx

    out: dict[str, dict] = {}
    src = _ro(control_db)
    try:
        try:
            stores = src.execute(
                "SELECT url, token, label FROM storage_servers WHERE last_health_ok=1"
            ).fetchall()
        except sqlite3.OperationalError:
            stores = []
        for s in stores:
            try:
                r = httpx.get(f"{s['url'].rstrip('/')}/files/archive-status",
                              headers={"Authorization": f"Bearer {s['token']}"}, timeout=10)
                if r.status_code == 200 and isinstance(r.json(), dict):
                    for fn, info in r.json().items():
                        if info.get("archived") and fn not in out:
                            out[fn] = {**info, "store": s["label"]}
            except Exception:
                pass
    finally:
        src.close()
    return out


def ingest_archive_status(cat: Catalog, archive: dict) -> dict:
    """Record a verified cloud (cold-tier) location for each archived recording."""
    n = 0
    for fn, info in (archive or {}).items():
        if not str(fn).endswith(".mp4"):
            continue
        rec = cat.find_by_filename(fn)
        if not rec:
            continue
        cat.add_location(rec["id"], "cloud",
                         info.get("remote") or info.get("store") or "cloud",
                         fn, verified=True)
        n += 1
    return {"archived": n}


def scan_live(cat: Catalog, control_db: str | Path) -> dict:
    """One full self-correcting pass, read-only w.r.t. the fleet:
    inventory -> transcript statuses (R2) -> archive/cold status (Phase 3) ->
    state reconciliation (R1)."""
    inv = collect_live_inventory(control_db)
    out = ingest_inventory(cat, inv)
    statuses = collect_transcript_statuses(control_db)
    tr = ingest_transcript_statuses(cat, statuses)
    archive = collect_archive_status(control_db)
    ar = ingest_archive_status(cat, archive)
    live_names = {f["filename"] for f in inv
                  if (f.get("filename") or "").endswith(".mp4")
                  and not f["filename"].endswith("_flv.mp4")}
    rec = reconcile_states(cat, live_names)
    cat.meta_set("last_backfill", time.time())
    return {**out, **tr, **ar, **rec, "live_files": len(inv),
            "transcript_statuses": statuses, "inventory": inv}
