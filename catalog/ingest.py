"""Phase 1 — write-through from the control plane into the catalog.

``on_event`` is called from the control plane's existing ``POST /events`` handler
(in addition to its current behaviour). It advances catalog state as recordings
start and finish, and enqueues a *shadow* transcribe job on completion.

Everything here is best-effort and never raises into the caller: a catalog problem
must never break event ingestion or anything else in the running system.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .db import Catalog
from .names import parse_recording_name

log = logging.getLogger("catalog.ingest")


def on_event(cat: Catalog, body: dict) -> None:
    """Advance the catalog from a recorder lifecycle webhook payload."""
    try:
        ev = (body or {}).get("event") or {}
        kind = ev.get("kind")
        user = ev.get("username")
        node = (body or {}).get("backend_id")
        detail = ev.get("detail") or {}

        if kind == "session_started":
            path = detail.get("path") or ""
            parsed = parse_recording_name(Path(path).name)
            if not parsed:
                return
            rid = cat.upsert_recording(filename=parsed.filename, creator=user or parsed.creator,
                                       node=node, state="recording")
            if path:
                cat.add_location(rid, "local", node or "recorder", path)

        elif kind == "session_ended":
            final = detail.get("final_path") or detail.get("flv_path") or ""
            parsed = parse_recording_name(Path(final).name)
            if not parsed:
                return
            rid = cat.upsert_recording(filename=parsed.filename, creator=user or parsed.creator,
                                       node=node, state="stored", started_at=parsed.started_at)
            if final:
                cat.add_location(rid, "local", node or "recorder", final, verified=True)
            cat.set_state(rid, "stored")
            # Record the *intent* to transcribe. shadow=True → nothing executes it
            # in Phase 1; it exists so we can compare against the live worker.
            cat.enqueue_job("transcribe", recording_id=rid, shadow=True)

        # 'errored' / 'stopped' / 'heartbeat' carry no file we need to track yet.
    except Exception:
        log.debug("on_event failed (ignored)", exc_info=True)


def generate_shadow_jobs(cat: Catalog) -> int:
    """Create shadow transcribe jobs for stored recordings with no done transcript
    and no open transcribe job. Returns how many were newly created."""
    rows = cat.conn.execute(
        "SELECT r.id FROM recordings r "
        "LEFT JOIN transcripts t ON t.recording_id = r.id "
        "WHERE r.state = 'stored' AND (t.state IS NULL OR t.state != 'done')"
    ).fetchall()
    created = 0
    for r in rows:
        if cat.enqueue_job("transcribe", recording_id=r["id"], shadow=True):
            created += 1
    return created


def promote_backlog(cat: Catalog, limit: int = 0) -> int:
    """Create REAL (non-shadow) transcribe jobs for the current backlog. This is
    the Phase 2 'go live' switch — call it only when you actually want the worker
    to start transcribing. Returns how many real jobs were created."""
    created = 0
    for rec in cat.transcribe_backlog(limit=limit or None):
        if cat.enqueue_job("transcribe", recording_id=rec["id"], shadow=False):
            created += 1
    return created


def compare_to_live(cat: Catalog, live_statuses: dict) -> dict:
    """Compare what the catalog *would* transcribe against what the live worker
    reports, so we can validate the catalog before cutting transcription over.

    ``live_statuses``: {filename: 'done'|'processing'|'pending'|...} from storage.
    """
    would = {
        r["filename"] for r in cat.conn.execute(
            "SELECT r.filename FROM recordings r "
            "LEFT JOIN transcripts t ON t.recording_id = r.id "
            "WHERE r.state = 'stored' AND (t.state IS NULL OR t.state != 'done')"
        ).fetchall()
    }
    done = {f for f, s in (live_statuses or {}).items() if s == "done"}
    pending = {f for f, s in (live_statuses or {}).items() if s in ("pending", "processing")}
    catalog_only = would - done - pending
    return {
        "already_done_live": sorted(would & done),   # catalog stale — worker beat us
        "agree_pending": sorted(would & pending),     # both agree it needs doing
        "catalog_only": sorted(catalog_only),         # catalog wants it, worker unaware
        "counts": {
            "would_transcribe": len(would),
            "already_done_live": len(would & done),
            "agree_pending": len(would & pending),
            "catalog_only": len(catalog_only),
        },
    }
