"""SQLite-backed catalog: the strangler's single source of truth.

Lives in its OWN database file (``CATALOG_DB``), separate from the control plane's
``control.sqlite``, so Phase 0/1 shadow mode can never disturb the running system.
Stdlib-only — no third-party imports here.
"""

from __future__ import annotations

import os
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from .names import parse_recording_name

_SCHEMA = (Path(__file__).resolve().parent / "schema.sql").read_text(encoding="utf-8")

# Monotonic-ish pipeline ordering, used so an upsert/transition never regresses a
# recording's state (a late 'discovered' can't overwrite a known 'stored').
_STATE_RANK = {
    "discovered": 0, "recording": 1, "orphan_flv": 1, "stored": 2,
    "transcribing": 3, "transcribed": 4, "archived": 5, "evicted": 6, "missing": 7,
}


def _rank(state: Optional[str]) -> int:
    return _STATE_RANK.get(state or "", -1)


def default_path() -> Path:
    return Path(os.environ.get("CATALOG_DB", Path.home() / ".tt-recorder" / "catalog.sqlite"))


def _now() -> float:
    return time.time()


def _uid() -> str:
    return str(uuid.uuid4())


def _open(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


class Catalog:
    """A thin, well-defined data layer over the catalog DB.

    All writes go through a lock so the same Catalog instance is safe to share
    between the control plane's event loop and a background shadow task.
    """

    def __init__(self, path: Optional[Path | str] = None):
        self.path = Path(path) if path else default_path()
        self.conn = _open(self.path)
        self._lock = threading.Lock()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    # ---- meta -------------------------------------------------------------

    def meta_get(self, key: str, default=None):
        r = self.conn.execute("SELECT value FROM catalog_meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

    def meta_set(self, key: str, value) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO catalog_meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            self.conn.commit()

    # ---- recordings -------------------------------------------------------

    def upsert_recording(self, *, filename: str, creator: Optional[str] = None,
                         node: Optional[str] = None, backend_pk: Optional[str] = None,
                         byte_size: Optional[int] = None, started_at: Optional[float] = None,
                         state: Optional[str] = None) -> str:
        """Insert or update a recording keyed by its final filename. Gaps are filled
        (COALESCE) and state only ever advances — existing data is never clobbered."""
        parsed = parse_recording_name(filename)
        if parsed:
            filename = parsed.filename                 # normalise _flv -> final
            creator = creator or parsed.creator
            if started_at is None:
                started_at = parsed.started_at
        creator = (creator or "").lstrip("@").lower() or None
        with self._lock:
            now = _now()
            row = self.conn.execute(
                "SELECT id, state FROM recordings WHERE filename=?", (filename,)
            ).fetchone()
            if row:
                rid = row["id"]
                self.conn.execute(
                    "UPDATE recordings SET creator=COALESCE(?,creator), node=COALESCE(?,node), "
                    "backend_pk=COALESCE(?,backend_pk), byte_size=COALESCE(?,byte_size), "
                    "started_at=COALESCE(?,started_at), updated_at=? WHERE id=?",
                    (creator, node, backend_pk, byte_size, started_at, now, rid),
                )
                if state and _rank(state) > _rank(row["state"]):
                    self.conn.execute(
                        "UPDATE recordings SET state=?, updated_at=? WHERE id=?",
                        (state, now, rid),
                    )
                self.conn.commit()
                return rid
            rid = _uid()
            self.conn.execute(
                "INSERT INTO recordings(id,creator,filename,node,backend_pk,byte_size,"
                "started_at,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (rid, creator or "", filename, node, backend_pk, byte_size, started_at,
                 state or "discovered", now, now),
            )
            self.conn.commit()
            return rid

    def set_state(self, recording_id: str, state: str, error: Optional[str] = None,
                  force: bool = False) -> None:
        with self._lock:
            row = self.conn.execute(
                "SELECT state FROM recordings WHERE id=?", (recording_id,)
            ).fetchone()
            if not row:
                return
            if force or _rank(state) >= _rank(row["state"]):
                self.conn.execute(
                    "UPDATE recordings SET state=?, error=?, updated_at=? WHERE id=?",
                    (state, error, _now(), recording_id),
                )
                self.conn.commit()

    def find_by_filename(self, filename: str) -> Optional[dict]:
        parsed = parse_recording_name(filename)
        fn = parsed.filename if parsed else filename
        r = self.conn.execute("SELECT * FROM recordings WHERE filename=?", (fn,)).fetchone()
        return dict(r) if r else None

    def get_recording(self, recording_id: str) -> Optional[dict]:
        r = self.conn.execute("SELECT * FROM recordings WHERE id=?", (recording_id,)).fetchone()
        return dict(r) if r else None

    # ---- locations --------------------------------------------------------

    def add_location(self, recording_id: str, tier: str, store: str, key: str,
                     byte_size: Optional[int] = None, checksum: Optional[str] = None,
                     verified: bool = False) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO blob_locations(id,recording_id,tier,store,key,byte_size,"
                "checksum,verified_at,created_at) VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(recording_id,tier,store) DO UPDATE SET "
                "key=excluded.key, "
                "byte_size=COALESCE(excluded.byte_size, blob_locations.byte_size), "
                "checksum=COALESCE(excluded.checksum, blob_locations.checksum), "
                "verified_at=COALESCE(excluded.verified_at, blob_locations.verified_at)",
                (_uid(), recording_id, tier, store, key, byte_size, checksum,
                 _now() if verified else None, _now()),
            )
            self.conn.commit()

    def locations(self, recording_id: str) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM blob_locations WHERE recording_id=?", (recording_id,)).fetchall()]

    # ---- transcripts ------------------------------------------------------

    def set_transcript(self, recording_id: str, state: str, tier: Optional[str] = None,
                       store: Optional[str] = None, key: Optional[str] = None,
                       language: Optional[str] = None, model: Optional[str] = None,
                       words: Optional[int] = None) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO transcripts(recording_id,tier,store,key,language,model,words,"
                "state,updated_at) VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(recording_id) DO UPDATE SET "
                "tier=COALESCE(excluded.tier, transcripts.tier), "
                "store=COALESCE(excluded.store, transcripts.store), "
                "key=COALESCE(excluded.key, transcripts.key), "
                "language=COALESCE(excluded.language, transcripts.language), "
                "model=COALESCE(excluded.model, transcripts.model), "
                "words=COALESCE(excluded.words, transcripts.words), "
                "state=excluded.state, updated_at=excluded.updated_at",
                (recording_id, tier, store, key, language, model, words, state, _now()),
            )
            self.conn.commit()

    # ---- jobs -------------------------------------------------------------

    def enqueue_job(self, kind: str, recording_id: Optional[str] = None,
                    node: Optional[str] = None, shadow: bool = True,
                    run_after: float = 0.0, max_attempts: int = 5) -> Optional[str]:
        """Create a job. De-duplicates: if an open (ready/running) job of the same
        kind already exists for this recording, returns None and creates nothing."""
        with self._lock:
            if recording_id:
                dup = self.conn.execute(
                    "SELECT id FROM jobs WHERE kind=? AND recording_id=? "
                    "AND state IN ('ready','running')",
                    (kind, recording_id),
                ).fetchone()
                if dup:
                    return None
            jid = _uid()
            now = _now()
            self.conn.execute(
                "INSERT INTO jobs(id,kind,recording_id,node,state,shadow,run_after,"
                "max_attempts,created_at,updated_at) VALUES(?,?,?,?,'ready',?,?,?,?,?)",
                (jid, kind, recording_id, node, 1 if shadow else 0, run_after,
                 max_attempts, now, now),
            )
            self.conn.commit()
            return jid

    def claim_job(self, node: Optional[str] = None, worker: Optional[str] = None,
                  kinds: Optional[list[str]] = None, include_shadow: bool = False
                  ) -> Optional[dict]:
        """Atomically claim the next runnable job (oldest run_after first). Shadow
        jobs are skipped unless include_shadow=True. Version-proof (no RETURNING)."""
        with self._lock:
            now = _now()
            clause = "state='ready' AND run_after<=?"
            params: list = [now]
            if not include_shadow:
                clause += " AND shadow=0"
            if node is not None:
                clause += " AND (node IS NULL OR node=?)"
                params.append(node)
            if kinds:
                clause += " AND kind IN (%s)" % ",".join("?" * len(kinds))
                params += list(kinds)
            row = self.conn.execute(
                f"SELECT * FROM jobs WHERE {clause} ORDER BY run_after LIMIT 1", params
            ).fetchone()
            if not row:
                return None
            cur = self.conn.execute(
                "UPDATE jobs SET state='running', claimed_by=?, claimed_at=?, "
                "attempts=attempts+1, updated_at=? WHERE id=? AND state='ready'",
                (worker, now, now, row["id"]),
            )
            self.conn.commit()
            if cur.rowcount == 0:
                return None
            out = dict(row)
            out.update(state="running", claimed_by=worker, attempts=row["attempts"] + 1)
            return out

    def complete_job(self, job_id: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE jobs SET state='done', progress=1.0, updated_at=? WHERE id=?",
                (_now(), job_id),
            )
            self.conn.commit()

    def fail_job(self, job_id: str, error, backoff: float = 60.0) -> None:
        """Mark a running job failed; retry with backoff until max_attempts, then
        leave it 'failed' for inspection."""
        with self._lock:
            r = self.conn.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if not r:
                return
            now = _now()
            if r["attempts"] >= r["max_attempts"]:
                self.conn.execute(
                    "UPDATE jobs SET state='failed', last_error=?, updated_at=? WHERE id=?",
                    (str(error)[:500], now, job_id),
                )
            else:
                self.conn.execute(
                    "UPDATE jobs SET state='ready', last_error=?, run_after=?, updated_at=? "
                    "WHERE id=?",
                    (str(error)[:500], now + backoff, now, job_id),
                )
            self.conn.commit()

    def jobs_summary(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT kind, state, shadow, COUNT(*) AS count FROM jobs "
            "GROUP BY kind, state, shadow ORDER BY kind, state").fetchall()]

    def cancel_open_jobs(self, recording_id: str, kind: Optional[str] = None) -> int:
        """Cancel ready/running jobs for a recording (e.g. a transcribe job once the
        recording turns out to be already transcribed). Returns rows affected."""
        with self._lock:
            if kind:
                cur = self.conn.execute(
                    "UPDATE jobs SET state='cancelled', updated_at=? WHERE recording_id=? "
                    "AND kind=? AND state IN ('ready','running')",
                    (_now(), recording_id, kind))
            else:
                cur = self.conn.execute(
                    "UPDATE jobs SET state='cancelled', updated_at=? WHERE recording_id=? "
                    "AND state IN ('ready','running')", (_now(), recording_id))
            self.conn.commit()
            return cur.rowcount

    def transcribe_backlog(self, limit: Optional[int] = None) -> list[dict]:
        """Recordings that genuinely still need transcription: on hot storage
        ('stored'), no done transcript. This is the *real* backlog once R2 has
        marked already-transcribed recordings."""
        rows = self.conn.execute(
            "SELECT r.id, r.filename, r.creator, r.started_at FROM recordings r "
            "LEFT JOIN transcripts t ON t.recording_id = r.id "
            "WHERE r.state = 'stored' AND (t.state IS NULL OR t.state != 'done') "
            "ORDER BY r.started_at"
        ).fetchall()
        out = [dict(r) for r in rows]
        return out[:limit] if limit else out

    def local_path(self, recording_id: str) -> Optional[str]:
        """First local blob key (filesystem path) for a recording, if any."""
        r = self.conn.execute(
            "SELECT key FROM blob_locations WHERE recording_id=? AND tier='local' "
            "ORDER BY verified_at DESC LIMIT 1", (recording_id,)).fetchone()
        return r["key"] if r else None

    def evict_candidates(self, min_age_days: float = 7, limit: Optional[int] = None
                         ) -> list[dict]:
        """Phase 3 (SHADOW): recordings SAFE to evict from hot storage — they have
        a verified cloud (cold) copy AND a local copy, and are older than
        min_age_days. Identifies only; nothing is deleted here."""
        cutoff = _now() - min_age_days * 86400
        rows = self.conn.execute(
            "SELECT r.id, r.filename, r.byte_size, r.started_at FROM recordings r "
            "WHERE r.state='stored' "
            "AND EXISTS (SELECT 1 FROM blob_locations b WHERE b.recording_id=r.id AND b.tier='cloud') "
            "AND EXISTS (SELECT 1 FROM blob_locations b WHERE b.recording_id=r.id AND b.tier='local') "
            "AND COALESCE(r.started_at, 0) < ? "
            "ORDER BY r.started_at", (cutoff,)
        ).fetchall()
        out = [dict(r) for r in rows]
        return out[:limit] if limit else out

    # ---- stats ------------------------------------------------------------

    def stats(self) -> dict:
        c = self.conn
        by_state = {r["state"]: r["count"] for r in c.execute(
            "SELECT state, COUNT(*) AS count FROM recordings GROUP BY state").fetchall()}
        locations = {r["tier"]: r["count"] for r in c.execute(
            "SELECT tier, COUNT(*) AS count FROM blob_locations GROUP BY tier").fetchall()}
        total = c.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(byte_size),0) AS b FROM recordings").fetchone()
        transcripts = c.execute(
            "SELECT COUNT(*) AS n FROM transcripts WHERE state='done'").fetchone()["n"]
        archived = c.execute(
            "SELECT COUNT(DISTINCT recording_id) AS n FROM blob_locations "
            "WHERE tier='cloud'").fetchone()["n"]
        ec = self.evict_candidates()
        return {
            "recordings": total["n"],
            "bytes": total["b"],
            "by_state": by_state,
            "locations": locations,
            "transcripts_done": transcripts,
            "cold": {
                "archived": archived,
                "evict_candidates": len(ec),
                "reclaimable_bytes": sum((r["byte_size"] or 0) for r in ec),
            },
            "jobs": self.jobs_summary(),
            "last_backfill": self.meta_get("last_backfill"),
            "last_parity": self.meta_get("last_parity"),
            "search_indexed": self.transcript_index_count(),
        }

    # ---- transcript full-text index --------------------------------------

    @staticmethod
    def _fts_query(q: str) -> str:
        """Turn arbitrary user input into a safe FTS5 MATCH expression: each
        whitespace token becomes a quoted term (AND-ed). Quoting neutralises FTS5
        operators so a stray '*', '"' or '(' can't raise a syntax error."""
        toks = [t for t in re.findall(r"\w+", q, flags=re.UNICODE) if t]
        return " ".join('"' + t + '"' for t in toks)

    def index_transcript(self, filename: str, creator: Optional[str], content: str,
                         store: Optional[str] = None, mtime: Optional[float] = None) -> None:
        """Insert/replace a transcript's text in the FTS index (keyed by filename)."""
        with self._lock:
            self.conn.execute("DELETE FROM transcript_fts WHERE filename=?", (filename,))
            self.conn.execute(
                "INSERT INTO transcript_fts(filename, creator, store, mtime, content) "
                "VALUES(?,?,?,?,?)",
                (filename, creator or "", store or "", str(mtime or ""), content or ""))
            self.conn.commit()

    def indexed_filenames(self) -> set:
        return {r["filename"] for r in
                self.conn.execute("SELECT filename FROM transcript_fts").fetchall()}

    def transcript_index_count(self) -> int:
        try:
            return self.conn.execute("SELECT COUNT(*) AS n FROM transcript_fts").fetchone()["n"]
        except sqlite3.OperationalError:
            return 0

    def search_transcripts(self, query: str, limit: int = 100) -> list[dict]:
        """Full-text search the indexed transcripts. Returns display-ready rows
        with a highlighted-context snippet, newest first."""
        match = self._fts_query(query)
        if not match:
            return []
        rows = self.conn.execute(
            "SELECT filename, creator, store, mtime, "
            "       snippet(transcript_fts, 4, '', '', ' … ', 12) AS snippet "
            "FROM transcript_fts WHERE transcript_fts MATCH ? "
            "ORDER BY rank LIMIT ?",
            (match, limit)).fetchall()
        out = []
        for r in rows:
            try:
                mt = float(r["mtime"]) if r["mtime"] else None
            except (TypeError, ValueError):
                mt = None
            out.append({"filename": r["filename"], "username": r["creator"] or "",
                        "snippet": r["snippet"] or "", "mtime": mt,
                        "storage_label": r["store"] or ""})
        out.sort(key=lambda x: x.get("mtime") or 0, reverse=True)
        return out

    def drop_transcript_index(self) -> None:
        """Wipe the FTS index so the next indexing cycle rebuilds it from scratch."""
        with self._lock:
            self.conn.execute("DELETE FROM transcript_fts")
            self.conn.commit()

    # ---- chat logs (metadata + fuzzy recording match) --------------------

    def match_chat_to_recording(self, creator: Optional[str], started_at: Optional[float],
                                window_sec: float = 1800.0) -> tuple:
        """Fuzzy-match a chat log to a recording: same creator, nearest start time
        within `window_sec`. Returns (recording_id, delta_seconds) or (None, None).
        Chat capture and the recording start a little apart, so this never relies on
        an exact timestamp."""
        if not creator or started_at is None:
            return None, None
        row = self.conn.execute(
            "SELECT id, ABS(started_at - ?) AS d FROM recordings "
            "WHERE creator=? AND started_at IS NOT NULL "
            "ORDER BY d ASC LIMIT 1", (started_at, creator)).fetchone()
        if row and row["d"] is not None and row["d"] <= window_sec:
            return row["id"], row["d"]
        return None, None

    def upsert_chat_log(self, *, filename: str, creator: Optional[str], store: Optional[str],
                        started_at: Optional[float] = None, ended_at: Optional[float] = None,
                        events: Optional[int] = None, comments: Optional[int] = None,
                        gifts: Optional[int] = None) -> None:
        """Record a chat log's metadata and (re)compute its fuzzy recording match."""
        creator = (creator or "").lstrip("@").lower() or None
        rec_id, delta = self.match_chat_to_recording(creator, started_at)
        now = _now()
        with self._lock:
            self.conn.execute(
                "INSERT INTO chat_logs(filename, creator, store, started_at, ended_at, "
                "events, comments, gifts, recording_id, match_delta, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(filename) DO UPDATE SET "
                "creator=excluded.creator, store=excluded.store, "
                "started_at=COALESCE(excluded.started_at, chat_logs.started_at), "
                "ended_at=COALESCE(excluded.ended_at, chat_logs.ended_at), "
                "events=excluded.events, comments=excluded.comments, gifts=excluded.gifts, "
                "recording_id=excluded.recording_id, match_delta=excluded.match_delta, "
                "updated_at=excluded.updated_at",
                (filename, creator, store, started_at, ended_at, events, comments, gifts,
                 rec_id, delta, now))
            self.conn.commit()

    def index_chat(self, filename: str, creator: Optional[str], store: Optional[str],
                   content: str) -> None:
        """Fold a chat log's text into chat_fts and mark it indexed."""
        with self._lock:
            self.conn.execute("DELETE FROM chat_fts WHERE filename=?", (filename,))
            self.conn.execute(
                "INSERT INTO chat_fts(filename, creator, store, content) VALUES(?,?,?,?)",
                (filename, creator or "", store or "", content or ""))
            self.conn.execute("UPDATE chat_logs SET indexed_at=? WHERE filename=?",
                              (_now(), filename))
            self.conn.commit()

    def chat_indexed_filenames(self) -> set:
        return {r["filename"] for r in
                self.conn.execute("SELECT filename FROM chat_logs WHERE indexed_at IS NOT NULL")}

    def chat_index_count(self) -> int:
        try:
            return self.conn.execute("SELECT COUNT(*) AS n FROM chat_fts").fetchone()["n"]
        except sqlite3.OperationalError:
            return 0

    def search_chat(self, query: str, limit: int = 100) -> list[dict]:
        """Full-text search indexed chat logs; returns the log + a context snippet,
        plus its fuzzy-matched recording filename when known."""
        match = self._fts_query(query)
        if not match:
            return []
        rows = self.conn.execute(
            "SELECT f.filename, f.creator, f.store, "
            "       snippet(chat_fts, 3, '', '', ' … ', 12) AS snippet, "
            "       c.recording_id, c.started_at, c.comments, r.filename AS rec_filename "
            "FROM chat_fts f "
            "LEFT JOIN chat_logs c ON c.filename = f.filename "
            "LEFT JOIN recordings r ON r.id = c.recording_id "
            "WHERE chat_fts MATCH ? ORDER BY rank LIMIT ?", (match, limit)).fetchall()
        out = []
        for r in rows:
            out.append({"filename": r["filename"], "username": r["creator"] or "",
                        "snippet": r["snippet"] or "", "storage_label": r["store"] or "",
                        "mtime": r["started_at"], "comments": r["comments"],
                        "recording_filename": r["rec_filename"]})
        out.sort(key=lambda x: x.get("mtime") or 0, reverse=True)
        return out

    def chat_for_recording(self, recording_id: str) -> Optional[dict]:
        r = self.conn.execute(
            "SELECT filename, creator, store, comments, match_delta "
            "FROM chat_logs WHERE recording_id=? ORDER BY match_delta ASC LIMIT 1",
            (recording_id,)).fetchone()
        return dict(r) if r else None

    def transcript_status_map(self) -> dict:
        """{recording_filename: 'done'|'pending'} for the Files tab, from the
        transcripts table joined to recordings. Only non-'none' states."""
        rows = self.conn.execute(
            "SELECT r.filename AS fn, t.state AS st FROM transcripts t "
            "JOIN recordings r ON r.id = t.recording_id WHERE t.state != 'none'").fetchall()
        return {r["fn"]: r["st"] for r in rows if r["fn"]}

    def chat_match_map(self) -> dict:
        """{recording_filename: chat_filename} for all matched chat logs — lets the
        Files tab show a chat link per recording in one query."""
        rows = self.conn.execute(
            "SELECT c.filename AS chat, r.filename AS rec FROM chat_logs c "
            "JOIN recordings r ON r.id = c.recording_id "
            "WHERE c.recording_id IS NOT NULL").fetchall()
        return {r["rec"]: r["chat"] for r in rows}
