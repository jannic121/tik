"""
TT Recorder — Control Plane, Deploy Tool, and Frontend (single file).

Run:
    pip install --break-system-packages fastapi 'uvicorn[standard]' httpx paramiko
    export CONTROL_PLANE_PASSWORD=somepassword
    python3 control_plane.py

Open http://localhost:8080

Tabs:
  Backends  — register and monitor recorder backends
  Watchers  — add/remove creators across all backends
  Files     — browse and delete recordings on any backend
  Deploy    — SSH into a new VPS and provision a recorder backend

The Deploy tab replaces deploy_helper.py. After a deploy completes it
surfaces the generated tokens and offers a one-click "Add to registry" button
that pre-fills the Backends modal.

Env vars (all optional except CONTROL_PLANE_PASSWORD):
  CONTROL_PLANE_PASSWORD   login password (no default)
  CONTROL_PLANE_DB         SQLite path  (default /var/lib/tt-control-plane/control.sqlite)
  CONTROL_PLANE_SECRET_FILE HMAC key file (default /var/lib/tt-control-plane/secret)
  CONTROL_PLANE_HOST       bind host  (default 127.0.0.1)
  CONTROL_PLANE_PORT       bind port  (default 8080)
  DEPLOY_FILES_DIR         where provision.sh/watcher.py/app.py live
                           (default: same dir as this script)
"""

from __future__ import annotations

import base64
import asyncio
import hashlib
import hmac
import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile
import tempfile
import re
import secrets
import sqlite3
from shlex import quote as shlex_quote
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import httpx
import paramiko
import uvicorn
from fastapi import Cookie, Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# Single source of truth for the build id: a VERSION file shipped alongside the
# code (so you bump it once and it travels to every node via deploy/push-update).
# The literal is only a fallback if the file is missing on an older deploy.
def _read_version(default: str = "0.0.0-unstamped") -> str:
    try:
        p = Path(__file__).resolve().parent / "VERSION"
        if p.exists():
            v = p.read_text().strip()
            if v:
                return v
    except Exception:
        pass
    return default

BUILD = _read_version()


def _load_template(name: str, fallback: str = "") -> str:
    """Load a UI template (login.html / dashboard.html) shipped next to this
    file. Kept on disk rather than embedded so the frontend can be edited and
    linted independently. Falls back to a minimal page if the file is missing."""
    try:
        p = Path(__file__).resolve().parent / name
        return p.read_text(encoding="utf-8")
    except Exception as e:
        log.error("could not load template %s: %s", name, e)
        return fallback or (
            "<!doctype html><meta charset=utf-8><body style='font-family:sans-serif;"
            "background:#111;color:#eee;padding:40px'>"
            f"<h2>UI template {name} missing</h2>"
            "<p>Make sure it's deployed next to control_plane.py.</p></body>")

# ---------------------------------------------------------------------------
# Settings

class Settings:
    password: str           = os.environ.get("CONTROL_PLANE_PASSWORD", "")
    db_path: Path           = Path(os.environ.get("CONTROL_PLANE_DB",
                                Path.home() / ".tt-recorder" / "control.sqlite"))
    secret_path: Path       = Path(os.environ.get("CONTROL_PLANE_SECRET_FILE",
                                Path.home() / ".tt-recorder" / "secret"))
    host: str               = os.environ.get("CONTROL_PLANE_HOST", "0.0.0.0")
    port: int               = int(os.environ.get("CONTROL_PLANE_PORT", "8080"))
    session_days: int       = 7
    health_interval: int    = 30
    backend_timeout: float  = 5.0
    files_dir: Path         = Path(os.environ.get("DEPLOY_FILES_DIR",
                                Path(__file__).resolve().parent))
    transcript_url: Optional[str] = os.environ.get("TRANSCRIPT_WORKER_URL") or None
    transcript_token: str   = os.environ.get("TRANSCRIPT_WORKER_TOKEN", "")
    # Upload worker
    upload_enabled: bool    = os.environ.get("UPLOAD_WORKER_ENABLED", "1") == "1"
    upload_interval: int    = int(os.environ.get("UPLOAD_INTERVAL_SEC", "300"))
    upload_min_age: int     = int(os.environ.get("UPLOAD_MIN_AGE_SEC", "300"))
    upload_delete: bool     = os.environ.get("UPLOAD_DELETE_AFTER", "1") == "1"
    upload_max_retries: int = int(os.environ.get("UPLOAD_MAX_RETRIES", "3"))
    # How many transfers to run in parallel, and how many to drain per cycle. The
    # worker now empties the backlog each cycle instead of shipping one file per
    # interval (which capped throughput at ~one file / UPLOAD_INTERVAL_SEC).
    upload_concurrency: int = max(1, int(os.environ.get("UPLOAD_CONCURRENCY", "2")))
    # Shared secret recorders must present on POST /events. Empty => webhook
    # stays open (preserves prior behaviour). To enable: set the SAME value as
    # CONTROL_PLANE_TOKEN on every recorder FIRST, then here, so no event is
    # rejected during rollout.
    ingest_token: str        = os.environ.get("CONTROL_PLANE_TOKEN", "")
    # History retention so the DB can't grow without bound (events + completed
    # transfers; moves are pruned separately). 0 disables a given prune.
    events_retention_days: int    = int(os.environ.get("EVENTS_RETENTION_DAYS", "90"))
    transfers_retention_days: int = int(os.environ.get("TRANSFERS_RETENTION_DAYS", "30"))
    maintenance_interval: int     = int(os.environ.get("MAINTENANCE_INTERVAL_SEC", str(24 * 3600)))


settings = Settings()
RECORDER_DEPLOY_FILES = ["provision.sh", "watcher.py", "app.py", "chat_recorder.py", "VERSION"]
STORAGE_DEPLOY_FILES  = ["provision_storage.sh", "transcription_worker.py", "VERSION"]

# --- Strangler catalog (Phase 0/1, shadow mode) ----------------------------
# Optional and entirely inert unless CATALOG_ENABLED=1. It uses a SEPARATE
# database (CATALOG_DB) and every call site is wrapped so a catalog fault can
# never affect the running control plane. See catalog/README.md.
CATALOG_ENABLED = os.environ.get("CATALOG_ENABLED", "0") == "1"
_catalog = None   # set in lifespan when enabled
# Where the Files tab gets its list: "live" (poll every node, default) or "catalog"
# (one indexed DB read — fast + resilient to a flaky node). Falls back to live if
# the catalog is off or errors. Flip with FILES_SOURCE=catalog; ?source= overrides.
FILES_SOURCE = os.environ.get("FILES_SOURCE", "live")

# Strip ANSI/VT100 escape sequences from SSH output before sending to browser
_ANSI = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


# ---------------------------------------------------------------------------
# SQLite

def _open_db() -> sqlite3.Connection:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(settings.db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    # Each statement runs independently so a pre-existing table with a slightly
    # different shape (from an old version) can't abort the whole init.
    statements = [
        """CREATE TABLE IF NOT EXISTS backends (
            id TEXT PRIMARY KEY, backend_id TEXT UNIQUE NOT NULL, url TEXT NOT NULL,
            auth_token TEXT NOT NULL, region TEXT, added_at REAL NOT NULL,
            last_health_check REAL, last_health_ok INTEGER DEFAULT 0, last_health_data TEXT)""",
        """CREATE TABLE IF NOT EXISTS watchers (
            username TEXT PRIMARY KEY, backend_pk TEXT NOT NULL,
            automatic_interval_min INTEGER NOT NULL DEFAULT 3, created_at REAL NOT NULL,
            FOREIGN KEY (backend_pk) REFERENCES backends(id) ON DELETE CASCADE)""",
        """CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE, backend_id TEXT,
            kind TEXT, username TEXT, payload TEXT, received_at REAL NOT NULL)""",
        "CREATE INDEX IF NOT EXISTS idx_events_username ON events(username)",
        "CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at)",
        """CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS storage_servers (
            id TEXT PRIMARY KEY, label TEXT, url TEXT UNIQUE NOT NULL, token TEXT NOT NULL,
            last_health_ok INTEGER DEFAULT 0, last_disk_free INTEGER, last_model TEXT,
            last_queue INTEGER, last_checked REAL, added_at REAL NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS transfers (
            id TEXT PRIMARY KEY, backend_pk TEXT NOT NULL, backend_label TEXT,
            storage_pk TEXT, storage_label TEXT, username TEXT NOT NULL, filename TEXT NOT NULL,
            src_path TEXT NOT NULL, size_bytes INTEGER, status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER DEFAULT 0, last_attempt REAL, completed_at REAL, error TEXT)""",
        "CREATE INDEX IF NOT EXISTS idx_transfers_filename ON transfers(filename)",
        "CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status)",
        """CREATE TABLE IF NOT EXISTS routing_rules (
            id TEXT PRIMARY KEY, backend_pk TEXT, target_storage_pk TEXT,
            delete_mode TEXT NOT NULL DEFAULT 'immediate',
            delete_delay_sec INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL)""",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_backend ON routing_rules(backend_pk)",
        """CREATE TABLE IF NOT EXISTS moves (
            id TEXT PRIMARY KEY, src_pk TEXT NOT NULL, src_label TEXT,
            dst_pk TEXT NOT NULL, dst_label TEXT, username TEXT, filename TEXT NOT NULL,
            src_path TEXT NOT NULL, kind TEXT, size_bytes INTEGER,
            status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER DEFAULT 0,
            error TEXT, created_at REAL NOT NULL, copied_at REAL, deleted_at REAL,
            batch TEXT)""",
        "CREATE INDEX IF NOT EXISTS idx_moves_status ON moves(status)",
        """CREATE TABLE IF NOT EXISTS ssh_creds (
            host TEXT PRIMARY KEY, ssh_user TEXT, ssh_port INTEGER,
            auth_method TEXT, password_enc TEXT, key_path TEXT, updated_at REAL)""",
    ]
    for stmt in statements:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as e:
            log.warning("schema init: skipped a statement (%s)", e)
    conn.commit()

    # Column migrations — ALTER TABLE is needed for existing installs because
    # CREATE TABLE IF NOT EXISTS does not add new columns to existing tables.
    def _add_col(table: str, col: str, typedef: str) -> None:
        try:
            existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            if col not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
                log.info("schema migration: added %s.%s", table, col)
        except sqlite3.OperationalError as e:
            log.warning("schema migration: could not add %s.%s (%s)", table, col, e)

    _add_col("transfers", "storage_pk", "TEXT")
    _add_col("transfers", "storage_label", "TEXT")
    _add_col("transfers", "delete_at", "REAL")              # when to delete recorder copy
    _add_col("transfers", "recorder_deleted", "INTEGER DEFAULT 0")
    _add_col("storage_servers", "last_reachable", "INTEGER DEFAULT 0")
    _add_col("storage_servers", "last_build", "TEXT")
    _add_col("backends", "last_build", "TEXT")
    _add_col("storage_servers", "last_concurrency", "INTEGER")
    _add_col("storage_servers", "last_transcribe", "INTEGER")
    _add_col("storage_servers", "last_vad", "INTEGER")
    _add_col("storage_servers", "last_beam", "INTEGER")
    # Per-server SSH endpoint, so two machines behind one public IP (distinguished
    # only by a forwarded SSH port) are tracked independently for deploy/updates.
    # NULL = legacy/default: SSH host = the service URL's host, SSH port = 22.
    _add_col("backends", "ssh_host", "TEXT")
    _add_col("backends", "ssh_port", "INTEGER")
    _add_col("storage_servers", "ssh_host", "TEXT")
    _add_col("storage_servers", "ssh_port", "INTEGER")
    # Disk-usage history per storage server, for the capacity / time-to-full view.
    conn.execute("""CREATE TABLE IF NOT EXISTS disk_samples (
        storage_id TEXT NOT NULL, ts REAL NOT NULL,
        free_bytes INTEGER, total_bytes INTEGER)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_disk_samples ON disk_samples(storage_id, ts)")
    conn.commit()


db      = _open_db()
_db_lock = asyncio.Lock()
_init_schema(db)


# ---------------------------------------------------------------------------
# Session auth

def _load_or_create_secret() -> str:
    if settings.secret_path.exists():
        return settings.secret_path.read_text().strip()
    settings.secret_path.parent.mkdir(parents=True, exist_ok=True)
    s = secrets.token_hex(32)
    settings.secret_path.write_text(s)
    os.chmod(settings.secret_path, 0o600)
    return s


def _load_config() -> None:
    """Override settings from persisted DB config (survives restarts).
    Also migrates the legacy single-storage config / env vars into the
    storage_servers table so existing setups keep working."""
    try:
        for key in ("transcript_url", "transcript_token"):
            row = db.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
            if row:
                setattr(settings, key, row["value"])
    except Exception:
        pass
    # One-time migration: if a single storage server is configured (via the old
    # config table or env vars) and the storage_servers table is empty, import it.
    try:
        have_any = db.execute("SELECT COUNT(*) c FROM storage_servers").fetchone()["c"]
        if not have_any and settings.transcript_url:
            _storage_upsert(settings.transcript_url, settings.transcript_token or "")
            log.info("migrated legacy storage server into storage_servers table")
    except Exception:
        log.exception("storage migration failed")


def _label_from_url(url: str) -> str:
    """Derive a short label from a URL, e.g. http://1.2.3.4:8090 -> 1.2.3.4."""
    s = url.split("://", 1)[-1].split("/", 1)[0]
    return s.split(":", 1)[0] or url


def _storage_upsert(url: str, token: str) -> str:
    """Insert or update a storage server by URL. Returns its id."""
    url = url.rstrip("/")
    row = db.execute("SELECT id FROM storage_servers WHERE url=?", (url,)).fetchone()
    if row:
        db.execute("UPDATE storage_servers SET token=? WHERE url=?", (token, url))
        db.commit()
        return row["id"]
    sid = str(uuid.uuid4())
    db.execute(
        "INSERT INTO storage_servers (id,label,url,token,added_at) VALUES (?,?,?,?,?)",
        (sid, _label_from_url(url), url, token, time.time()),
    )
    db.commit()
    return sid


def _storage_all() -> list[dict]:
    rows = db.execute(
        "SELECT * FROM storage_servers ORDER BY added_at ASC"
    ).fetchall()
    return [dict(r) for r in rows]


def _storage_healthy() -> list[dict]:
    return [s for s in _storage_all() if s["last_health_ok"]]


def _pick_storage() -> Optional[dict]:
    """Choose a target storage server for a new transfer: healthy one with the
    most free disk. Servers with unknown disk sort last but stay eligible."""
    healthy = _storage_healthy()
    if not healthy:
        return None
    healthy.sort(
        key=lambda s: (s["last_disk_free"] is not None, s["last_disk_free"] or 0),
        reverse=True,
    )
    return healthy[0]


def _default_delete_mode() -> str:
    """The implicit global default preserves the legacy env-var behaviour
    until the user configures explicit rules."""
    return "immediate" if settings.upload_delete else "never"


def _resolve_routing(backend_pk: str) -> dict:
    """Return the effective pipeline policy for a backend:
       {target_storage_pk, delete_mode, delete_delay_sec}.
    Looks for a backend-specific rule, then the global default ('*'),
    then falls back to the legacy env-var behaviour."""
    rule = db.execute(
        "SELECT target_storage_pk, delete_mode, delete_delay_sec "
        "FROM routing_rules WHERE backend_pk=?", (backend_pk,)
    ).fetchone()
    if not rule:
        rule = db.execute(
            "SELECT target_storage_pk, delete_mode, delete_delay_sec "
            "FROM routing_rules WHERE backend_pk='*'"
        ).fetchone()
    if rule:
        return {"target_storage_pk": rule["target_storage_pk"],
                "delete_mode": rule["delete_mode"],
                "delete_delay_sec": rule["delete_delay_sec"]}
    return {"target_storage_pk": None,            # auto
            "delete_mode": _default_delete_mode(),
            "delete_delay_sec": 0}


def _storage_by_id(sid: str) -> Optional[dict]:
    for s in _storage_all():
        if s["id"] == sid:
            return s
    return None


async def _tw_call(server: dict, method: str, path: str, **kwargs) -> tuple[int, Any]:
    """Call a SPECIFIC storage server."""
    hdrs = {}
    if server.get("token"):
        hdrs["Authorization"] = f"Bearer {server['token']}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.request(
                method, f"{server['url'].rstrip('/')}{path}", headers=hdrs, **kwargs
            )
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        except httpx.RequestError as e:
            return -1, str(e)


SECRET = _load_or_create_secret()


# ---------------------------------------------------------------------------
# SSH credential storage (opt-in convenience). Passwords are encrypted at rest
# with a key derived from the app SECRET, so a raw DB copy doesn't leak them.
# Reversible on the host (it must be, to use them) — NOT a substitute for keys.
from cryptography.fernet import Fernet, InvalidToken  # provided by paramiko's deps

_FERNET = Fernet(base64.urlsafe_b64encode(hashlib.sha256(SECRET.encode()).digest()))


def _host_of(url: str) -> Optional[str]:
    from urllib.parse import urlparse
    try:
        return urlparse(url).hostname
    except Exception:
        return None


def _mkey(host: Optional[str], port) -> str:
    """Canonical machine key for SSH creds + Updates grouping. Including the SSH
    port lets two servers behind one public IP be tracked as distinct machines."""
    try:
        p = int(port) if port else 22
    except (TypeError, ValueError):
        p = 22
    return f"{host or ''}:{p}"


def _ssh_target(row) -> tuple:
    """(ssh_host, ssh_port) for a backend/storage row, falling back to the service
    URL's host and port 22 when the explicit SSH endpoint isn't set."""
    keys = row.keys() if hasattr(row, "keys") else row
    sh = (row["ssh_host"] if "ssh_host" in keys and row["ssh_host"] else _host_of(row["url"]))
    sp = (row["ssh_port"] if "ssh_port" in keys and row["ssh_port"] else None) or 22
    return sh, sp


def _get_creds_mkey(host: Optional[str], port) -> Optional[dict]:
    """Saved creds for a machine. Tries the host:port key, then (for port 22)
    falls back to the legacy bare-host key so existing installs keep their creds."""
    g = _get_ssh_creds(_mkey(host, port))
    if not g and int(port or 22) == 22:
        g = _get_ssh_creds(host)
    return g


def _get_ssh_creds(host: str) -> Optional[dict]:
    if not host:
        return None
    # Separate short-lived connection: these helpers are also called from the
    # update-all background thread, so they must not share the loop's connection.
    with sqlite3.connect(settings.db_path, timeout=10) as c:
        c.row_factory = sqlite3.Row
        r = c.execute("SELECT * FROM ssh_creds WHERE host=?", (host,)).fetchone()
    return dict(r) if r else None


def _save_ssh_creds(host: str, user: str, port: int, auth_method: str,
                    password: Optional[str], key_path: Optional[str]) -> None:
    """Persist SSH creds for a host. Only overwrites the password when a new one
    is supplied (so 'reuse saved password' submissions don't wipe it)."""
    if not host:
        return
    existing = _get_ssh_creds(host)
    enc = existing.get("password_enc") if existing else None
    if password:
        enc = _FERNET.encrypt(password.encode()).decode()
    with sqlite3.connect(settings.db_path, timeout=10) as c:
        c.execute(
            "INSERT INTO ssh_creds (host,ssh_user,ssh_port,auth_method,password_enc,key_path,updated_at) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(host) DO UPDATE SET "
            "ssh_user=excluded.ssh_user, ssh_port=excluded.ssh_port, "
            "auth_method=excluded.auth_method, password_enc=excluded.password_enc, "
            "key_path=excluded.key_path, updated_at=excluded.updated_at",
            (host, user, port, auth_method, enc, key_path, time.time()))
        c.commit()


def _decrypt_pw(enc: Optional[str]) -> Optional[str]:
    if not enc:
        return None
    try:
        return _FERNET.decrypt(enc.encode()).decode()
    except Exception:
        return None


def _threaded_stream(work, media_type: str = "text/plain; charset=utf-8"):
    """Run blocking `work(emit)` in a daemon thread and stream every line it
    emits to the client as a chunked text response. `emit(str)` is thread-safe;
    any exception in `work` is reported and the stream then closes cleanly.

    This is the shared plumbing behind every "do something over SSH and show me
    the live log" endpoint (deploy, push-update, worker-config, oom-protect, …)."""
    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def emit(line: str):
        loop.call_soon_threadsafe(queue.put_nowait, line)

    def runner():
        try:
            work(emit)
        except Exception as e:
            emit(f"[ERROR] {type(e).__name__}: {e}\n")
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, None)

    threading.Thread(target=runner, daemon=True).start()

    async def stream():
        while True:
            chunk = await queue.get()
            if chunk is None:
                return
            yield chunk

    return StreamingResponse(stream(), media_type=media_type)


def _resolve_ssh(host: str, port: int, user: str, auth_method: str,
                 password: Optional[str], key_path: Optional[str]) -> tuple:
    """Merge supplied SSH params with anything stored for the machine, then persist
    the result keyed by host:port. A blank password/key falls back to the saved one.
    The port is authoritative from the caller (form/record) — it identifies the
    machine — so it is not overridden by saved creds."""
    port = port or 22
    stored = _get_creds_mkey(host, port) or {}
    user = user or stored.get("ssh_user") or "root"
    auth_method = auth_method or stored.get("auth_method") or "key"
    if not password and auth_method == "password":
        password = _decrypt_pw(stored.get("password_enc"))
    if not key_path and auth_method == "key":
        key_path = stored.get("key_path")
    _save_ssh_creds(_mkey(host, port), user, port, auth_method, password, key_path)
    return port, user, auth_method, password, key_path


def _ssh_creds_summary(host: str, port=None) -> dict:
    """Non-secret view for prefilling/collapsing the UI (never returns the password).
    With a port, looks up the exact machine; without one, returns the most recent
    saved creds for the host on ANY SSH port (so the UI can collapse on host alone)."""
    if port:
        g = _get_creds_mkey(host, port) or {}
    else:
        with sqlite3.connect(settings.db_path, timeout=10) as c:
            c.row_factory = sqlite3.Row
            r = c.execute("SELECT * FROM ssh_creds WHERE host=? OR host LIKE ? "
                          "ORDER BY updated_at DESC LIMIT 1",
                          (host, f"{host}:%")).fetchone()
        g = dict(r) if r else {}
    return {"host": host, "exists": bool(g),
            "ssh_user": g.get("ssh_user") or "root",
            "ssh_port": g.get("ssh_port") or 22,
            "auth_method": g.get("auth_method") or "key",
            "key_path": g.get("key_path") or "",
            "has_password": bool(g.get("password_enc"))}


def _sign_session() -> str:
    expiry = int(time.time()) + settings.session_days * 86400
    sig = hmac.new(SECRET.encode(), str(expiry).encode(), hashlib.sha256).hexdigest()
    return f"{expiry}.{sig}"


def _verify_session(cookie: Optional[str]) -> bool:
    if not cookie or "." not in cookie:
        return False
    expiry_str, sig = cookie.rsplit(".", 1)
    try:
        if int(expiry_str) < int(time.time()):
            return False
    except ValueError:
        return False
    expected = hmac.new(SECRET.encode(), expiry_str.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, expected)


def require_login(session: Optional[str] = Cookie(default=None)) -> None:
    if not _verify_session(session):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "login required")


# ---------------------------------------------------------------------------
# Pydantic models

class LoginRequest(BaseModel):
    password: str

class BackendCreate(BaseModel):
    url: str        = Field(..., min_length=4)
    auth_token: str = Field(..., min_length=8)
    ssh_host: Optional[str] = None              # SSH endpoint, if not the URL host
    ssh_port: Optional[int] = Field(None, ge=1, le=65535)

class WatcherCreate(BaseModel):
    username: str               = Field(..., min_length=1, max_length=64)
    automatic_interval_min: int = Field(3, ge=1, le=60)
    backend_pk: Optional[str]   = None
    capture_chat: bool          = True

class DeleteFileBody(BaseModel):
    path: str
    backend_pk: Optional[str] = None
    storage_sid: Optional[str] = None

class DeployRequest(BaseModel):
    deploy_type: str            = Field("recorder", pattern="^(recorder|storage)$")
    host: str
    ssh_port: int               = Field(22, ge=1, le=65535)
    ssh_user: str               = "root"
    auth_method: str            = Field(..., pattern="^(password|key)$")
    ssh_password: Optional[str] = None
    ssh_key_path: Optional[str] = None
    # Recorder-only
    backend_id: str             = ""
    region: str                 = ""
    bind_address: str           = "0.0.0.0"
    # Port the service binds on the box (0 = default: recorder 8000 / storage 8090)
    service_port: int           = Field(0, ge=0, le=65535)
    # Storage-only
    whisper_model: str          = "base"


# ---------------------------------------------------------------------------
# Backend HTTP helper

async def _call(url: str, token: str, method: str,
                path: str, body: Optional[dict] = None) -> tuple[int, Any]:
    hdrs = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=settings.backend_timeout) as client:
        try:
            r = await client.request(method, f"{url.rstrip('/')}{path}",
                                     headers=hdrs, json=body)
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        except httpx.RequestError as e:
            return -1, str(e)


# ---------------------------------------------------------------------------
# SSH deploy (runs in a thread; emits lines via the `emit` callback)

class PushUpdateRequest(BaseModel):
    update_type: str            = Field("recorder", pattern="^(recorder|storage|colocated)$")
    target_id:   str            # backend_pk for recorder/colocated, storage id for storage
    ssh_port:    int            = Field(22, ge=1, le=65535)
    ssh_user:    str            = "root"
    auth_method: str            = Field(..., pattern="^(password|key)$")
    ssh_password: Optional[str] = None
    ssh_key_path: Optional[str] = None


class UpdateNodeRequest(BaseModel):
    kind:      str = Field(..., pattern="^(recorder|storage|colocated)$")
    target_id: str


# What each update type pushes and restarts
_UPDATE_PLAN = {
    "recorder": {
        "files":    [("watcher.py", "/opt/tt-backend/watcher.py"),
                     ("app.py",     "/opt/tt-backend/app.py"),
                     ("chat_recorder.py", "/opt/tt-backend/chat_recorder.py"),
                     ("VERSION",    "/opt/tt-backend/VERSION")],
        "services": ["tt-backend"],
        "health":   "http://localhost:8000/health",
    },
    "storage": {
        "files":    [("transcription_worker.py", "/opt/tt-storage/transcription_worker.py"),
                     ("VERSION", "/opt/tt-storage/VERSION")],
        "services": ["tt-transcription"],
        "health":   "http://localhost:8090/health",
    },
    "colocated": {
        "files":    [("watcher.py",               "/opt/tt-backend/watcher.py"),
                     ("app.py",                    "/opt/tt-backend/app.py"),
                     ("chat_recorder.py",          "/opt/tt-backend/chat_recorder.py"),
                     ("VERSION",                   "/opt/tt-backend/VERSION"),
                     ("transcription_worker.py",   "/opt/tt-storage/transcription_worker.py"),
                     ("VERSION",                   "/opt/tt-storage/VERSION")],
        "services": ["tt-backend", "tt-transcription"],
        "health":   "http://localhost:8000/health",
    },
}


def _ssh_connect(host: str, port: int, user: str, auth_method: str,
                 password: Optional[str], key_path: Optional[str], emit) -> Optional[paramiko.SSHClient]:
    """Shared SSH connection helper used by deploy and push-update."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs: dict = {"hostname": host, "port": port, "username": user, "timeout": 10}
    if auth_method == "password":
        if not password:
            emit("[ERROR] password auth chosen but no password provided\n"); return None
        kwargs.update(password=password, allow_agent=False, look_for_keys=False)
    else:
        if not key_path:
            emit("[ERROR] key auth chosen but no key path provided\n"); return None
        kp = os.path.expanduser(key_path)
        if not os.path.exists(kp):
            emit(f"[ERROR] key not found: {kp}\n"); return None
        kwargs["key_filename"] = kp
    emit(f"==> Connecting to {user}@{host}:{port} ...\n")
    try:
        client.connect(**kwargs)
        emit("Connected.\n\n")
        return client
    except paramiko.AuthenticationException as e:
        emit(f"[ERROR] Authentication failed: {e}\n"); return None
    except Exception as e:
        emit(f"[ERROR] Connection failed: {type(e).__name__}: {e}\n"); return None


def _ssh_push_update(req: PushUpdateRequest, host: str, emit) -> None:
    """SFTP the right files for the update type and restart the right services."""
    import time as _time
    plan = _UPDATE_PLAN[req.update_type]
    emit(f"==> Update type: {req.update_type}\n")
    emit(f"==> Will push {len(plan['files'])} file(s), restart: {', '.join(plan['services'])}\n\n")
    client = _ssh_connect(host, req.ssh_port, req.ssh_user,
                          req.auth_method, req.ssh_password, req.ssh_key_path, emit)
    if not client:
        return
    try:
        local_names = [f for f, _ in plan["files"]]
        missing = [f for f in local_names if not (settings.files_dir / f).exists()]
        if missing:
            emit(f"[ERROR] Missing local files: {missing}\n"); return

        emit(f"==> Uploading {len(plan['files'])} file(s) ...\n")
        sftp = client.open_sftp()
        try:
            for fname, remote in plan["files"]:
                emit(f"  {fname} → {remote} ... ")
                sftp.put(str(settings.files_dir / fname), remote)
                emit("ok\n")
        finally:
            sftp.close()

        sudo = "" if req.ssh_user == "root" else "sudo -n "
        for svc in plan["services"]:
            emit(f"\n==> Restarting {svc} ...\n")
            _, stdout, _ = client.exec_command(f"{sudo}systemctl restart {svc}", timeout=30)
            rc = stdout.channel.recv_exit_status()
            emit("  restarted\n" if rc == 0 else f"  [WARN] restart returned {rc}\n")

        emit("\n==> Health check (waiting for service to come up) ...\n")
        h = None
        for attempt in range(15):           # ~30s total
            _time.sleep(2)
            try:
                _, hout, _ = client.exec_command(f"curl -s {plan['health']}", timeout=10)
                raw = hout.read().decode("utf-8", errors="replace").strip()
                if raw:
                    h = json.loads(raw)
                    break
            except Exception:
                pass
        if h:
            build = h.get("build", "?")
            if req.update_type == "storage":
                emit(f"  up · build {build} · model {h.get('model','?')} · "
                     f"watch_dir {h.get('watch_dir','?')}\n")
            else:
                emit(f"  up · build {build} · backend_id {h.get('backend_id','?')}\n")
            emit("\n==> Update complete.\n")
        else:
            emit("\n[WARN] Service didn't respond to /health within ~30s. "
                 "Check `systemctl status` / `journalctl` on the box.\n")
    except Exception as e:
        emit(f"\n[ERROR] {type(e).__name__}: {e}\n")
    finally:
        client.close()



def _ssh_deploy(req: DeployRequest, emit) -> None:
    client = _ssh_connect(req.host, req.ssh_port, req.ssh_user,
                          req.auth_method, req.ssh_password, req.ssh_key_path, emit)
    if not client:
        return

    try:
        # Select file set, command, and args based on deploy type
        bind = "127.0.0.1" if req.host in ("127.0.0.1", "localhost") else "0.0.0.0"
        svc_port = req.service_port or (8090 if req.deploy_type == "storage" else 8000)
        if req.deploy_type == "storage":
            deploy_files = STORAGE_DEPLOY_FILES
            script_name  = "provision_storage.sh"
            script_args  = f"{bind} {svc_port}"   # model is hardcoded to base in the script
        else:
            deploy_files = RECORDER_DEPLOY_FILES
            script_name  = "provision.sh"
            script_args  = f"{bind} {svc_port}"   # backend_id/region auto-detected on VPS
        emit(f"==> Service will listen on {bind}:{svc_port}\n")

        missing = [f for f in deploy_files
                   if not (settings.files_dir / f).exists()]
        if missing:
            emit(f"[ERROR] Missing local files: {missing}\n"
                 f"       Set DEPLOY_FILES_DIR or place them alongside control_plane.py\n")
            return

        emit("==> Uploading files to /tmp on remote\n")
        sftp = client.open_sftp()
        try:
            for fname in deploy_files:
                emit(f"  - {fname} ... ")
                sftp.put(str(settings.files_dir / fname), f"/tmp/{fname}")
                if fname.endswith(".sh"):
                    sftp.chmod(f"/tmp/{fname}", 0o755)
                emit("ok\n")
        finally:
            sftp.close()

        if req.ssh_user != "root":
            if req.auth_method == "password":
                emit(
                    f"\n[WARN] Non-root user with password auth — will fail unless "
                    f"passwordless sudo is configured for '{req.ssh_user}'.\n"
                    f"       Fix: echo '{req.ssh_user} ALL=(ALL) NOPASSWD: ALL' | "
                    f"sudo tee /etc/sudoers.d/{req.ssh_user}\n\n"
                )
            sudo = "sudo -n "
        else:
            sudo = ""

        cmd = f"{sudo}bash /tmp/{script_name} {script_args}"
        emit(f"\n==> Running: {cmd}\n\n")

        _, stdout, stderr = client.exec_command(cmd, get_pty=True, timeout=600)
        for raw in iter(stdout.readline, ""):
            emit(_ANSI.sub("", raw))

        code = stdout.channel.recv_exit_status()
        if code == 0:
            emit("\n==> Deployment complete.\n")
            # Read the authoritative token straight from the env file on the
            # server, so auto-registration works even on a re-deploy where
            # provision.sh didn't reprint it (env file already existed).
            if req.deploy_type == "storage":
                env_file, token_key = "/etc/tt-storage.env", "WHISPER_AUTH_TOKEN"
            else:
                env_file, token_key = "/etc/tt-backend.env", "AUTH_TOKEN"
            emit("\n==> Reading credentials from server for registration ...\n")
            try:
                _, eout, _ = client.exec_command(
                    f"{sudo}grep -E '^(AUTH_TOKEN|WHISPER_AUTH_TOKEN|BACKEND_ID|REGION)=' "
                    f"{env_file}", timeout=15)
                vals = {}
                for line in eout.read().decode("utf-8", errors="replace").splitlines():
                    if "=" in line:
                        k, v = line.split("=", 1)
                        vals[k.strip()] = v.strip()
                tok = vals.get(token_key, "")
                if tok:
                    if req.deploy_type == "storage":
                        emit(f"WHISPER_AUTH_TOKEN: {tok}\n")
                    else:
                        emit(f"Backend ID: {vals.get('BACKEND_ID','')}\n")
                        emit(f"AUTH_TOKEN: {tok}\n")
                    emit("==> Credentials read — registering automatically.\n")
                else:
                    emit(f"[WARN] Could not read {token_key} from {env_file}. "
                         f"Register manually in the relevant tab.\n")
            except Exception as e:
                emit(f"[WARN] Could not read credentials ({type(e).__name__}: {e}). "
                     f"Register manually.\n")
        else:
            emit(f"\n[ERROR] {script_name} exited with code {code}\n")
            tail = stderr.read().decode("utf-8", errors="replace")[-2000:]
            if tail.strip():
                emit("---- stderr ----\n" + tail + "\n----------------\n")
    except Exception as e:
        emit(f"\n[ERROR] {type(e).__name__}: {e}\n")
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Background health checks

_upload_active   = False
_transfer_progress: dict[str, int] = {}   # filename → bytes sent so far

async def _upload_cycle() -> None:
    """One pass: find files on backends not yet on storage, queue and transfer them.
    Guarded so periodic + manual triggers can't run two cycles at once."""
    global _upload_active
    if _upload_active:
        return  # a cycle is already running — skip this trigger
    _upload_active = True
    try:
        await _upload_cycle_inner()
    finally:
        _upload_active = False


async def _upload_cycle_inner() -> None:
    storages = _storage_healthy()
    if not storages:
        return  # nowhere to send recordings

    # 1. What's already on ANY storage server? (merge inventories)
    # dict: filename → storage server row that has it (first server wins)
    storage_fnames: dict[str, dict] = {}
    inventory_reliable = True            # False if any healthy storage's listing failed
    for s in storages:
        code, inv = await _tw_call(s, "GET", "/files/inventory")
        if code == 200 and isinstance(inv, list):
            for fi in inv:
                if fi["filename"] not in storage_fnames:
                    storage_fnames[fi["filename"]] = s
        else:
            # An incomplete view: a file genuinely on this storage will be missing
            # from storage_fnames. Re-shipping 'done' files based on that would
            # re-upload everything whenever the storage box is briefly busy.
            inventory_reliable = False
            log.warning("storage %s inventory unavailable (code %s) — not re-shipping "
                        "'done' files this cycle", s.get("label"), code)

    # 2. What do backends have?
    # All registered storage ids (healthy or not). A 'done' transfer whose storage
    # server is no longer in this set was completed against a storage box that has
    # since been removed — e.g. a recorder that used to be colocated storage. Such
    # files are stranded on the recorder, so we re-ship them below.
    async with _db_lock:
        registered_storage_ids = {
            r["id"] for r in db.execute("SELECT id FROM storage_servers").fetchall()
        }
        backends = db.execute(
            "SELECT id, backend_id, url, auth_token FROM backends WHERE last_health_ok=1"
        ).fetchall()

    now = time.time()
    for b in backends:
        code, files = await _call(b["url"], b["auth_token"], "GET", "/files")
        if code != 200 or not isinstance(files, dict):
            continue
        # Fold in chat logs (*_chat.jsonl) so they ride the exact same transfer +
        # routing + verify pipeline as recordings, then get archived to the cloud.
        ccode, chats = await _call(b["url"], b["auth_token"], "GET", "/files/chat")
        if ccode == 200 and isinstance(chats, list):
            for c in chats:
                files.setdefault(c["username"], []).append(c)
        for username, flist in files.items():
            for f in flist:
                fname = Path(f["path"]).name
                if fname.endswith("_flv.mp4"):
                    continue  # still recording
                if (now - f.get("mtime", now)) < settings.upload_min_age:
                    continue  # too fresh — may still be finalising
                if fname in storage_fnames:
                    # Already on storage — colocated setup or previously transferred
                    # outside the upload worker. Insert a synthetic 'done' record so
                    # the Files tab shows "✓ On storage" instead of "↑ Upload".
                    # NOTE: we deliberately do NOT delete the file here — for colocation
                    # the "storage" and "backend" are the same disk, so deletion would
                    # remove the only copy.
                    srv = storage_fnames[fname]
                    async with _db_lock:
                        already_done = db.execute(
                            "SELECT id FROM transfers "
                            "WHERE filename=? AND backend_pk=? AND status='done'",
                            (fname, b["id"]),
                        ).fetchone()
                        if not already_done:
                            db.execute(
                                "INSERT OR IGNORE INTO transfers "
                                "(id,backend_pk,backend_label,storage_pk,storage_label,"
                                " username,filename,src_path,size_bytes,status,completed_at) "
                                "VALUES (?,?,?,?,?,?,?,?,?,'done',?)",
                                (str(uuid.uuid4()), b["id"], b["backend_id"],
                                 srv["id"], srv["label"],
                                 username, fname, f["path"],
                                 f.get("size_bytes"), time.time()),
                            )
                            db.commit()
                    continue

                async with _db_lock:
                    existing = db.execute(
                        "SELECT status, attempts, storage_pk, recorder_deleted "
                        "FROM transfers WHERE filename=? AND backend_pk=?",
                        (fname, b["id"]),
                    ).fetchone()
                if existing:
                    if existing["status"] == "done":
                        # Normally 'done' means "confirmed on storage" → skip. But if
                        # this file is STILL on the recorder (recorder_deleted=0), is
                        # NOT on any current storage, and was marked done against a
                        # storage server that no longer exists, it's stranded — the
                        # box was colocated storage and isn't anymore. Re-ship it so
                        # the recorder disk can actually drain.
                        stranded = (
                            inventory_reliable
                            and existing["recorder_deleted"] == 0
                            and fname not in storage_fnames
                            and (existing["storage_pk"] is None
                                 or existing["storage_pk"] not in registered_storage_ids)
                        )
                        if stranded:
                            async with _db_lock:
                                db.execute(
                                    "UPDATE transfers SET status='pending', "
                                    "storage_pk=NULL, storage_label=NULL, error=NULL, "
                                    "attempts=0 WHERE filename=? AND backend_pk=?",
                                    (fname, b["id"]),
                                )
                                db.commit()
                            log.info("re-queueing stranded recorder file %s "
                                     "(storage removed; not on any current storage)",
                                     fname)
                        continue
                    if existing["status"] in ("pending", "transferring"):
                        continue
                    if existing["attempts"] >= settings.upload_max_retries:
                        continue
                    # failed but retryable — reset to pending
                    async with _db_lock:
                        db.execute(
                            "UPDATE transfers SET status='pending', error=NULL "
                            "WHERE filename=? AND backend_pk=?",
                            (fname, b["id"]),
                        )
                        db.commit()
                else:
                    async with _db_lock:
                        db.execute(
                            "INSERT INTO transfers "
                            "(id,backend_pk,backend_label,username,filename,"
                            " src_path,size_bytes,status) "
                            "VALUES (?,?,?,?,?,?,?,'pending')",
                            (str(uuid.uuid4()), b["id"], b["backend_id"],
                             username, fname, f["path"], f.get("size_bytes")),
                        )
                        db.commit()

    # 3. Delayed-delete sweep: remove recorder copies whose retention window has
    #    elapsed, but ONLY after re-confirming the file is present on storage so
    #    we never delete the only copy.
    now = time.time()
    async with _db_lock:
        due = db.execute(
            "SELECT t.*, b.url AS b_url, b.auth_token AS b_token "
            "FROM transfers t JOIN backends b ON b.id=t.backend_pk "
            "WHERE t.status='done' AND t.recorder_deleted=0 "
            "AND t.delete_at IS NOT NULL AND t.delete_at<=?",
            (now,),
        ).fetchall()
    for d in due:
        d = dict(d)
        if d["filename"] not in storage_fnames:
            # Not (yet) confirmed on storage — do NOT delete. Try again next cycle.
            log.warning("retention: %s not confirmed on storage; skipping delete",
                        d["filename"])
            continue
        code, _ = await _call(d["b_url"], d["b_token"], "DELETE",
                              f"/files?path={d['src_path']}")
        if code in (200, 204):
            async with _db_lock:
                db.execute("UPDATE transfers SET recorder_deleted=1 WHERE id=?", (d["id"],))
                db.commit()
            log.info("retention: deleted recorder copy of %s", d["filename"])
        else:
            log.warning("retention: delete of %s failed (code %d)", d["filename"], code)

    # 4. Drain the pending queue: ship up to `upload_concurrency` files in parallel,
    #    repeatedly, until the backlog is empty (or nothing can make progress).
    #    Previously this shipped a single file per cycle, capping throughput at
    #    ~one file / UPLOAD_INTERVAL_SEC regardless of how many were waiting.
    conc = max(1, settings.upload_concurrency)
    shipped = 0
    while True:
        async with _db_lock:
            rows = db.execute(
                "SELECT t.*, b.url AS b_url, b.auth_token AS b_token "
                "FROM transfers t JOIN backends b ON b.id=t.backend_pk "
                "WHERE t.status='pending' ORDER BY t.size_bytes ASC LIMIT ?",
                (conc,),
            ).fetchall()
        if not rows:
            break
        results = await asyncio.gather(
            *(_do_transfer(dict(r)) for r in rows), return_exceptions=True)
        # Stop if nothing made progress (e.g. no healthy storage target) so we
        # don't spin on the same rows; the next cycle will retry.
        if not any(r is True for r in results):
            break
        shipped += sum(1 for r in results if r is True)
    if shipped:
        log.info("upload cycle: shipped %d file(s) to storage", shipped)


async def _relay_file(src_url: str, src_headers: dict, src_params: dict,
                      dst_url: str, dst_headers: dict,
                      expected_size: Optional[int], tmp_tag: str,
                      progress_cb=None) -> tuple[int, Optional[int]]:
    """Move one file source→destination via a local temp buffer.

    Buffering to disk (rather than piping the source GET straight into the
    destination PUT) decouples the two transfers, so a slow destination can't
    stall the source read and cause a mid-file disconnect. Verifies the full
    file arrived before uploading, and that the destination stored the same
    byte count. Returns (bytes_downloaded, bytes_stored_at_destination).
    """
    tmp = Path(tempfile.gettempdir()) / f"ttrelay_{tmp_tag}.part"
    try:
        async with httpx.AsyncClient(timeout=None) as cli:
            async with cli.stream("GET", src_url, headers=src_headers,
                                  params=src_params) as resp:
                if resp.status_code != 200:
                    raise RuntimeError(f"source download {resp.status_code}")
                got = 0
                with tmp.open("wb") as f:
                    async for chunk in resp.aiter_bytes(1 << 20):
                        f.write(chunk)
                        got += len(chunk)
                        if progress_cb:
                            progress_cb(got)
        if expected_size and got != int(expected_size):
            verb = "grew to" if got > int(expected_size) else "truncated to"
            raise RuntimeError(
                f"file changed size during copy ({verb} {got} from {expected_size} "
                f"bytes) — it may still be recording; leaving source untouched")

        async def _chunks(path, size=1 << 20):
            with open(path, "rb") as fh:
                while True:
                    b = fh.read(size)
                    if not b:
                        break
                    yield b

        async with httpx.AsyncClient(timeout=None) as up:
            put = await up.put(dst_url, headers=dst_headers, content=_chunks(tmp))
        if put.status_code not in (200, 201):
            detail = ""
            try:
                detail = put.text[:200]
            except Exception:
                pass
            hint = (" — destination storage is running an older worker; push-update it"
                    if put.status_code == 400 and "accepted" in detail else "")
            raise RuntimeError(f"destination PUT {put.status_code}: {detail}{hint}")
        written = (put.json() or {}).get("size_bytes")
        if written is not None and int(written) != got:
            raise RuntimeError(f"size mismatch: copied {got} vs destination {written}")
        return got, written
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


async def _do_transfer(row: dict) -> bool:
    """Ship one recording backend→storage. Returns True if it attempted the
    transfer (success or failure), False if it was blocked (no storage target, so
    the row stays pending) — the drain loop uses this to avoid spinning."""
    tid = row["id"]
    # Resolve the pipeline policy for this backend
    rule = _resolve_routing(row["backend_pk"])
    if rule["target_storage_pk"]:
        target = _storage_by_id(rule["target_storage_pk"])
        if not target or not target.get("last_health_ok"):
            # Assigned destination is unavailable — honour the routing intent
            # by leaving the file pending and retrying, rather than rerouting.
            log.warning("assigned storage for %s unavailable — leaving pending",
                        row["filename"])
            return False
    else:
        target = _pick_storage()           # auto: most free disk
    if not target:
        log.warning("no healthy storage server for %s — leaving pending", row["filename"])
        return False
    async with _db_lock:
        db.execute(
            "UPDATE transfers SET status='transferring', storage_pk=?, storage_label=?, "
            "attempts=attempts+1, last_attempt=? WHERE id=?",
            (target["id"], target["label"], time.time(), tid),
        )
        db.commit()
    log.info("upload: %s/%s from %s → %s",
             row["username"], row["filename"], row["backend_label"], target["label"])

    try:
        src_url = f"{row['b_url'].rstrip('/')}/files/download"
        dst_url = (f"{target['url'].rstrip('/')}"
                   f"/files/{row['username']}/{row['filename']}")
        dst_hdrs: dict = {"Content-Type": "video/mp4"}
        if target.get("token"):
            dst_hdrs["Authorization"] = f"Bearer {target['token']}"

        fname = row["filename"]
        _transfer_progress[fname] = 0
        got, received = await _relay_file(
            src_url, {"Authorization": f"Bearer {row['b_token']}"},
            {"path": row["src_path"]},
            dst_url, dst_hdrs, row.get("size_bytes") or 0,
            tmp_tag=str(tid),
            progress_cb=lambda n: _transfer_progress.__setitem__(fname, n),
        )

        log.info("upload done: %s/%s (%s bytes)",
                 row["username"], row["filename"], f"{(received or got):,}")
        _transfer_progress.pop(row["filename"], None)

        async with _db_lock:
            db.execute(
                "UPDATE transfers SET status='done', completed_at=? WHERE id=?",
                (time.time(), tid),
            )
            db.commit()

        # Apply the backend's delete policy. We only ever delete the recorder
        # copy once the file is confirmed on storage (a later sweep verifies
        # again for delayed deletes).
        mode = rule["delete_mode"]
        if mode == "immediate":
            async with _db_lock:
                db.execute("UPDATE transfers SET recorder_deleted=1 WHERE id=?", (tid,))
                db.commit()
            code, _ = await _call(
                row["b_url"], row["b_token"], "DELETE",
                f"/files?path={row['src_path']}",
            )
            if code not in (200, 204):
                log.warning("post-transfer delete failed for %s (code %d)",
                            row["filename"], code)
                async with _db_lock:
                    db.execute("UPDATE transfers SET recorder_deleted=0 WHERE id=?", (tid,))
                    db.commit()
        elif mode == "delay":
            when = time.time() + max(0, rule["delete_delay_sec"])
            async with _db_lock:
                db.execute("UPDATE transfers SET delete_at=? WHERE id=?", (when, tid))
                db.commit()
            log.info("%s will be deleted from recorder in %ds (after upload)",
                     row["filename"], rule["delete_delay_sec"])
        # mode == "never": leave the recorder copy in place

    except Exception as e:
        log.error("upload failed: %s — %s", row["filename"], e)
        _transfer_progress.pop(row["filename"], None)
        async with _db_lock:
            db.execute(
                "UPDATE transfers SET status='failed', error=? WHERE id=?",
                (str(e)[:500], tid),
            )
            db.commit()
    return True


async def _upload_worker() -> None:
    """Background task: periodic upload cycle."""
    while True:
        await asyncio.sleep(settings.upload_interval)
        if not settings.upload_enabled:
            continue
        try:
            await _upload_cycle()
        except Exception:
            log.exception("upload cycle error")


async def _health_loop() -> None:
    while True:
        try:
            async with _db_lock:
                rows = db.execute(
                    "SELECT id, url, auth_token FROM backends"
                ).fetchall()
            for row in rows:
                code, body = await _call(row["url"], row["auth_token"], "GET", "/health")
                ok = code == 200 and isinstance(body, dict)
                async with _db_lock:
                    db.execute(
                        "UPDATE backends SET last_health_check=?, last_health_ok=?, "
                        "last_health_data=?, last_build=? WHERE id=?",
                        (time.time(), 1 if ok else 0,
                         json.dumps(body) if ok else None,
                         body.get("build") if ok else None, row["id"]),
                    )
                    db.commit()
            # Storage servers: /health (no auth) tells us reachable; /status
            # (authenticated) tells us the token is valid. Distinguishing the
            # two turns a token mismatch into "auth failed" instead of the
            # misleading "unreachable".
            for s in _storage_all():
                hcode, hbody = await _tw_call(s, "GET", "/health")
                reachable = hcode == 200 and isinstance(hbody, dict)
                token_ok = False
                sbody: dict = {}
                if reachable:
                    scode, sb = await _tw_call(s, "GET", "/status")
                    if scode == 200 and isinstance(sb, dict):
                        token_ok, sbody = True, sb
                usable = reachable and token_ok
                info = sbody or (hbody if reachable else {})
                async with _db_lock:
                    db.execute(
                        "UPDATE storage_servers SET last_health_ok=?, last_reachable=?, "
                        "last_build=?, last_disk_free=?, last_model=?, last_queue=?, "
                        "last_concurrency=?, last_transcribe=?, last_vad=?, last_beam=?, "
                        "last_checked=? WHERE id=?",
                        (1 if usable else 0, 1 if reachable else 0,
                         info.get("build"), info.get("disk_free_bytes"),
                         info.get("model"), info.get("queue_depth"),
                         info.get("concurrency"),
                         (1 if info.get("transcribe_enabled") else 0)
                         if "transcribe_enabled" in info else None,
                         (1 if info.get("vad") else 0) if "vad" in info else None,
                         info.get("beam_size"),
                         time.time(), s["id"]),
                    )
                    # Disk-usage history (throttled to ~5 min/server, pruned to 14 days)
                    free, total = info.get("disk_free_bytes"), info.get("disk_total_bytes")
                    if free is not None and total:
                        last = db.execute(
                            "SELECT MAX(ts) AS t FROM disk_samples WHERE storage_id=?",
                            (s["id"],)).fetchone()
                        if not last["t"] or (time.time() - last["t"]) >= 300:
                            db.execute("INSERT INTO disk_samples(storage_id,ts,free_bytes,total_bytes)"
                                       " VALUES(?,?,?,?)", (s["id"], time.time(), free, total))
                            db.execute("DELETE FROM disk_samples WHERE storage_id=? AND ts < ?",
                                       (s["id"], time.time() - 14*86400))
                    db.commit()
        except Exception:
            log.exception("health loop error")
        await asyncio.sleep(settings.health_interval)


# ---------------------------------------------------------------------------
# App

async def _auto_backup_worker() -> None:
    """Write a rotating local snapshot of the DB on startup and once a day,
    keeping the most recent BACKUP_KEEP files in <db_dir>/backups/."""
    keep = int(os.environ.get("BACKUP_KEEP", "7"))
    interval = int(os.environ.get("BACKUP_INTERVAL_SEC", str(24 * 3600)))
    if interval <= 0:
        return  # disabled
    backup_dir = Path(settings.db_path).parent / "backups"
    while True:
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            dest_path = backup_dir / f"control-{time.strftime('%Y%m%d-%H%M%S')}.sqlite"
            async with _db_lock:
                dest = sqlite3.connect(str(dest_path))
                try:
                    db.backup(dest)
                finally:
                    dest.close()
            snaps = sorted(backup_dir.glob("control-*.sqlite"))
            for old in snaps[:-keep]:
                try:
                    old.unlink()
                except OSError:
                    pass
            log.info("db backup written: %s (keeping %d)", dest_path.name, keep)
        except Exception:
            log.exception("auto-backup failed")
        await asyncio.sleep(interval)


async def _maintenance_worker() -> None:
    """Keep the DB from growing without bound. Prunes events older than the
    retention window and COMPLETED transfers older than theirs (in-flight and
    failed transfers are always kept), then truncates the WAL. moves are pruned
    elsewhere. Any prune with a retention of 0 is skipped."""
    while True:
        await asyncio.sleep(settings.maintenance_interval)
        try:
            now = time.time()
            async with _db_lock:
                ev = tr = 0
                if settings.events_retention_days > 0:
                    ev = db.execute("DELETE FROM events WHERE received_at < ?",
                                    (now - settings.events_retention_days * 86400,)).rowcount
                if settings.transfers_retention_days > 0:
                    tr = db.execute(
                        "DELETE FROM transfers WHERE status='done' "
                        "AND COALESCE(completed_at, 0) < ?",
                        (now - settings.transfers_retention_days * 86400,)).rowcount
                db.commit()
                db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            if ev or tr:
                log.info("maintenance: pruned %d event(s), %d transfer(s)", ev, tr)
        except Exception as e:               # never let maintenance kill the loop
            log.warning("maintenance worker error: %s", e)


async def _catalog_shadow_loop() -> None:
    """Phase 1 shadow worker. Periodically materialises transcribe-job *intent*
    in the catalog and compares it against what the live transcription workers
    report, so the catalog can be validated before anything depends on it.

    Creates only shadow jobs — it executes nothing and touches only the separate
    catalog DB. Fully guarded so it can never disturb the running system."""
    from catalog import ingest as _ing, backfill as _bf, parity as _par
    interval = int(os.environ.get("CATALOG_SHADOW_INTERVAL", "900"))
    while True:
        try:
            if _catalog is not None:
                # Full self-correcting pass: inventory + transcript statuses (R2) +
                # archive/cold status (Phase 3) + state reconciliation (R1). Run off
                # the event loop; read-only vs the fleet and vs control.sqlite.
                res = await asyncio.to_thread(_bf.scan_live, _catalog, str(settings.db_path))
                # Refresh the drift snapshot so the readiness gate has live data.
                _par.compute_parity(_catalog, res.get("inventory", []))
                created = _ing.generate_shadow_jobs(_catalog)
                cmp = _ing.compare_to_live(_catalog, res.get("transcript_statuses", {}))
                _catalog.meta_set("last_compare", json.dumps(
                    {**cmp["counts"], "catalog_only_sample": cmp["catalog_only"][:25]}))
                log.info("catalog shadow: backlog=%d transcribed+%d evicted+%d intent+%d",
                         cmp["counts"]["would_transcribe"], res.get("transcribed", 0),
                         res.get("evicted", 0), created)
        except Exception:
            log.debug("catalog shadow loop error (ignored)", exc_info=True)
        await asyncio.sleep(interval)


async def _transcript_index_cycle(batch: int = 40) -> dict:
    """Pull finished transcripts off storage once and fold their text into the
    catalog's full-text index, so search is a fast local query. Indexes up to
    `batch` not-yet-indexed transcripts per call (cheap, incremental)."""
    if _catalog is None:
        return {"indexed": 0, "pending": 0, "total_done": 0}
    servers = _storage_healthy()
    if not servers:
        return {"indexed": 0, "pending": 0, "total_done": 0}
    # filename -> first storage server that reports a 'done' transcript for it
    done: dict[str, dict] = {}
    for s in servers:
        code, body = await _tw_call(s, "GET", "/transcripts/all-statuses")
        if code == 200 and isinstance(body, dict):
            for fn, st in body.items():
                if st == "done" and fn not in done:
                    done[fn] = s
    already = await asyncio.to_thread(_catalog.indexed_filenames)
    todo = [(fn, s) for fn, s in done.items() if fn not in already]
    from catalog.names import parse_recording_name
    indexed = 0
    for fn, s in todo[:batch]:
        code, text = await _tw_call(s, "GET", "/transcripts/view", params={"filename": fn})
        if code != 200 or not isinstance(text, str):
            continue                       # fetch failed — retry next cycle
        # An empty-but-done transcript is still indexed (as ""), so it isn't
        # re-fetched from storage on every cycle forever.
        rec = await asyncio.to_thread(_catalog.find_by_filename, fn)
        if rec:
            creator, mtime = rec["creator"], rec["started_at"]
        else:
            parsed = parse_recording_name(fn)
            creator = parsed.creator if parsed else ""
            mtime = parsed.started_at if parsed else None
        await asyncio.to_thread(_catalog.index_transcript, fn, creator, text,
                                s["label"], mtime)
        indexed += 1
    return {"indexed": indexed, "pending": max(0, len(todo) - indexed),
            "total_done": len(done)}


async def _transcript_index_loop() -> None:
    """Background indexer: keep the transcript FTS index in step with storage."""
    interval = int(os.environ.get("TRANSCRIPT_INDEX_INTERVAL", "180"))
    while True:
        try:
            if _catalog is not None:
                res = await _transcript_index_cycle()
                if res["indexed"]:
                    log.info("transcript index: +%d (pending %d)",
                             res["indexed"], res["pending"])
        except Exception:
            log.debug("transcript index loop error (ignored)", exc_info=True)
        await asyncio.sleep(interval)


async def _chat_index_cycle(batch: int = 30) -> dict:
    """Catalog the chat logs on storage: upsert each log's metadata (recomputing its
    fuzzy recording match every pass, since recordings may be cataloged later) and
    fold not-yet-indexed comment text into chat_fts for fast search."""
    if _catalog is None:
        return {"logs": 0, "indexed": 0, "pending": 0}
    servers = _storage_healthy()
    if not servers:
        return {"logs": 0, "indexed": 0, "pending": 0}
    from catalog.names import parse_chat_name
    logs: dict[str, dict] = {}            # filename -> storage server that has it
    for s in servers:
        code, body = await _tw_call(s, "GET", "/chat")
        if code == 200 and isinstance(body, list):
            for item in body:
                fn = item.get("filename")
                if fn and fn not in logs:
                    logs[fn] = {"server": s, "item": item}
    # Metadata + fuzzy match pass (cheap, local).
    for fn, e in logs.items():
        item = e["item"]
        parsed = parse_chat_name(fn)
        creator = (parsed.creator if parsed else None) or item.get("username")
        started = (parsed.started_at if parsed and parsed.started_at is not None
                   else item.get("started"))
        await asyncio.to_thread(
            _catalog.upsert_chat_log, filename=fn, creator=creator,
            store=e["server"]["label"], started_at=started, ended_at=item.get("ended"),
            events=item.get("events"), comments=item.get("comments"), gifts=item.get("gifts"))
    # Text pass: index logs whose comments aren't in chat_fts yet.
    already = await asyncio.to_thread(_catalog.chat_indexed_filenames)
    todo = [(fn, e["server"]) for fn, e in logs.items() if fn not in already]
    indexed = 0
    for fn, s in todo[:batch]:
        code, body = await _tw_call(s, "GET", "/chat/view",
                                    params={"filename": fn, "limit": 20000})
        if code != 200 or not isinstance(body, dict):
            continue
        parts = []
        for ev in body.get("events", []):
            for k in ("text", "nickname", "user", "gift"):
                v = ev.get(k)
                if v:
                    parts.append(str(v))
        parsed = parse_chat_name(fn)
        creator = (parsed.creator if parsed else None) or body.get("username")
        await asyncio.to_thread(_catalog.index_chat, fn, creator, s["label"], " ".join(parts))
        indexed += 1
    return {"logs": len(logs), "indexed": indexed, "pending": max(0, len(todo) - indexed)}


async def _chat_index_loop() -> None:
    """Background indexer: keep the chat FTS index + recording matches current."""
    interval = int(os.environ.get("CHAT_INDEX_INTERVAL", "300"))
    while True:
        try:
            if _catalog is not None:
                res = await _chat_index_cycle()
                if res["indexed"]:
                    log.info("chat index: +%d (pending %d)", res["indexed"], res["pending"])
        except Exception:
            log.debug("chat index loop error (ignored)", exc_info=True)
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not settings.password:
        raise RuntimeError("CONTROL_PLANE_PASSWORD must be set")
    # Load persisted settings (transcript worker URL/token) from DB
    _load_config()
    # Crash recovery: any transfer left mid-flight is reset so it retries
    try:
        async with _db_lock:
            n = db.execute(
                "UPDATE transfers SET status='pending' WHERE status='transferring'"
            ).rowcount
            db.commit()
        if n:
            log.info("reset %d stuck transfer(s) from transferring → pending", n)
    except Exception:
        log.exception("transfer recovery failed")
    log.info("starting on %s:%d | db=%s | deploy_files=%s",
             settings.host, settings.port, settings.db_path, settings.files_dir)
    log.info("build %s", BUILD)
    # Check deploy files for each deploy type independently
    rec_missing = [f for f in RECORDER_DEPLOY_FILES
                   if not (settings.files_dir / f).exists()]
    sto_missing = [f for f in STORAGE_DEPLOY_FILES
                   if not (settings.files_dir / f).exists()]
    if rec_missing:
        log.warning("Recorder deploy will fail — missing: %s", rec_missing)
    if sto_missing:
        log.warning("Storage server deploy will fail — missing: %s", sto_missing)
    if not rec_missing and not sto_missing:
        log.info("all deploy files present")
    task = asyncio.create_task(_health_loop(), name="health-loop")
    asyncio.create_task(_upload_worker(),  name="upload-worker")
    asyncio.create_task(_auto_backup_worker(), name="auto-backup")
    asyncio.create_task(_move_cycle(), name="move-worker")
    asyncio.create_task(_maintenance_worker(), name="maintenance")
    if CATALOG_ENABLED:
        global _catalog
        try:
            from catalog import Catalog
            _catalog = Catalog()
            log.info("catalog shadow mode ENABLED (db=%s)", _catalog.path)
            asyncio.create_task(_catalog_shadow_loop(), name="catalog-shadow")
            asyncio.create_task(_transcript_index_loop(), name="transcript-index")
            asyncio.create_task(_chat_index_loop(), name="chat-index")
        except Exception:
            log.exception("catalog init failed — continuing without it")
            _catalog = None
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="TT Recorder", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Auth

# Per-IP failed-login tracking: a progressive delay + lockout that makes online
# password brute-forcing impractical without changing anything for normal use.
_login_failures: dict[str, list] = {}   # ip -> [count, last_fail_ts]


@app.post("/api/login")
async def api_login(body: LoginRequest, request: Request):
    ip = request.client.host if request.client else "?"
    now = time.time()
    rec = _login_failures.get(ip)
    if rec and now - rec[1] > 900:       # forget after 15 min of no failures
        rec = None
    fails = rec[0] if rec else 0
    if fails >= 50:                      # hard stop for a sustained attack
        raise HTTPException(429, "too many failed attempts; try again later")
    if fails >= 5:                       # slow down after a few misses
        await asyncio.sleep(min(5.0, 0.5 * fails))

    if not hmac.compare_digest(body.password, settings.password):
        _login_failures[ip] = [fails + 1, now]
        await asyncio.sleep(min(2.0, 0.25 * (fails + 1)))   # progressive delay
        raise HTTPException(401, "wrong password")

    _login_failures.pop(ip, None)        # success clears the counter
    resp = JSONResponse({"ok": True})
    resp.set_cookie("session", _sign_session(),
                    httponly=True, samesite="lax",
                    max_age=settings.session_days * 86400)
    return resp

@app.post("/api/logout")
async def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("session")
    return resp

@app.get("/api/me")
async def api_me(session: Optional[str] = Cookie(default=None)):
    return {"logged_in": _verify_session(session)}


# ---------------------------------------------------------------------------
# Backends

@app.get("/api/backends", dependencies=[Depends(require_login)])
async def list_backends():
    async with _db_lock:
        rows = db.execute("SELECT * FROM backends ORDER BY added_at").fetchall()
    return [
        {
            "id": r["id"], "backend_id": r["backend_id"], "url": r["url"],
            "region": r["region"], "added_at": r["added_at"],
            "last_health_check": r["last_health_check"],
            "last_health_ok": bool(r["last_health_ok"]),
            "health": json.loads(r["last_health_data"]) if r["last_health_data"] else None,
        }
        for r in rows
    ]

@app.post("/api/backends", status_code=201, dependencies=[Depends(require_login)])
async def add_backend(body: BackendCreate):
    url = body.url.rstrip("/")
    code, health = await _call(url, body.auth_token, "GET", "/health")
    if code != 200 or not isinstance(health, dict):
        raise HTTPException(400,
            f"backend probe failed (status={code}): {health if isinstance(health,str) else 'no JSON'}")
    code2, _ = await _call(url, body.auth_token, "GET", "/watchers")
    if code2 == 401:
        raise HTTPException(400, "auth_token rejected by backend")
    if code2 != 200:
        raise HTTPException(400, f"backend /watchers returned {code2}")

    pk = str(uuid.uuid4())
    bid = health.get("backend_id", "unknown")
    ssh_host = (body.ssh_host or "").strip() or None
    async with _db_lock:
        existing = db.execute(
            "SELECT id FROM backends WHERE backend_id=? OR url=?", (bid, url)
        ).fetchone()
        if existing:
            # Already registered (e.g. re-deploy / half-deploy recovery) —
            # refresh the token + health instead of failing on the unique key.
            db.execute(
                "UPDATE backends SET url=?, auth_token=?, region=?, "
                "last_health_check=?, last_health_ok=1, last_health_data=?, "
                "ssh_host=COALESCE(?,ssh_host), ssh_port=COALESCE(?,ssh_port) WHERE id=?",
                (url, body.auth_token, health.get("region", ""), time.time(),
                 json.dumps(health), ssh_host, body.ssh_port, existing["id"]),
            )
            db.commit()
            return {"id": existing["id"], "backend_id": bid,
                    "region": health.get("region"), "updated": True}
        db.execute(
            "INSERT INTO backends (id,backend_id,url,auth_token,region,added_at,"
            "last_health_check,last_health_ok,last_health_data,ssh_host,ssh_port) "
            "VALUES (?,?,?,?,?,?,?,1,?,?,?)",
            (pk, bid, url, body.auth_token,
             health.get("region", ""), time.time(), time.time(), json.dumps(health),
             ssh_host, body.ssh_port),
        )
        db.commit()
    return {"id": pk, "backend_id": bid, "region": health.get("region")}

@app.post("/api/backends/{pk}/purge-confirmed",
          dependencies=[Depends(require_login)])
async def backend_purge_confirmed(pk: str):
    """Delete recorder copies of files already confirmed on any healthy storage
    server. Safe: verifies each file is on storage before deleting it from the
    recorder. Streams a plain-text progress log."""
    async with _db_lock:
        b = db.execute("SELECT * FROM backends WHERE id=?", (pk,)).fetchone()
    if not b:
        raise HTTPException(404, "backend not found")
    b = dict(b)

    async def _stream():
        yield f"Scanning storage inventory…\n"
        storages = _storage_healthy()
        if not storages:
            yield "✗ No healthy storage servers — cannot verify files safely. Aborting.\n"
            return

        on_storage: dict[str, str] = {}   # filename → storage label
        for s in storages:
            code, inv = await _tw_call(s, "GET", "/files/inventory")
            if code == 200 and isinstance(inv, list):
                for fi in inv:
                    fn = fi.get("filename", "")
                    if fn and fn not in on_storage:
                        on_storage[fn] = s.get("label") or s["url"]
        yield f"Storage inventory: {len(on_storage)} file(s) on {len(storages)} server(s).\n"

        yield f"Fetching recorder file list from {b['backend_id']}…\n"
        code, files = await _call(b["url"], b["auth_token"], "GET", "/files")
        if code != 200 or not isinstance(files, dict):
            yield f"✗ Could not reach recorder ({code}). Aborting.\n"
            return

        candidates = [f for flist in files.values() for f in flist]
        yield f"Recorder has {len(candidates)} file(s). Checking which are on storage…\n\n"

        deleted = 0
        skipped = 0
        errors  = 0
        freed   = 0

        for f in candidates:
            fname  = Path(f["path"]).name
            size   = f.get("size_bytes", 0) or 0
            if fname not in on_storage:
                skipped += 1
                continue   # not on storage yet — leave it
            code, _ = await _call(b["url"], b["auth_token"],
                                  "DELETE", f"/files?path={f['path']}")
            if code in (200, 204):
                deleted += 1
                freed   += size
                yield f"  ✓ deleted  {fname}  ({_fmt_bytes(size)})  — on {on_storage[fname]}\n"
                # Mark the transfer record as recorder_deleted so the worker
                # knows not to touch it again.
                async with _db_lock:
                    db.execute(
                        "UPDATE transfers SET recorder_deleted=1 "
                        "WHERE filename=? AND backend_pk=?",
                        (fname, pk),
                    )
                    db.commit()
            else:
                errors += 1
                yield f"  ✗ error    {fname}  (HTTP {code})\n"

        yield (
            f"\nDone. Deleted {deleted} file(s), freed {_fmt_bytes(freed)}"
            + (f", {skipped} skipped (not yet on storage)" if skipped else "")
            + (f", {errors} error(s)" if errors else "")
            + ".\n"
        )

    return StreamingResponse(_stream(), media_type="text/plain; charset=utf-8")


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


@app.delete("/api/backends/{pk}", status_code=204, dependencies=[Depends(require_login)])
async def delete_backend(pk: str):
    async with _db_lock:
        row = db.execute("SELECT * FROM backends WHERE id=?", (pk,)).fetchone()
        if not row:
            raise HTTPException(404, "backend not found")
        ws = db.execute(
            "SELECT username FROM watchers WHERE backend_pk=?", (pk,)
        ).fetchall()
    for w in ws:
        await _call(row["url"], row["auth_token"], "DELETE", f"/watchers/{w['username']}")
    async with _db_lock:
        db.execute("DELETE FROM backends WHERE id=?", (pk,))
        db.commit()
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Watchers

async def _pick_backend() -> sqlite3.Row:
    async with _db_lock:
        rows = db.execute(
            "SELECT b.id,b.url,b.auth_token,b.last_health_data, "
            "COUNT(w.username) AS load "
            "FROM backends b LEFT JOIN watchers w ON w.backend_pk=b.id "
            "WHERE b.last_health_ok=1 GROUP BY b.id ORDER BY load ASC"
        ).fetchall()
    for r in rows:
        h = json.loads(r["last_health_data"] or "{}")
        if r["load"] < h.get("max_watchers", 30):
            return r
    raise HTTPException(503, "no healthy backend with capacity")

@app.get("/api/watchers", dependencies=[Depends(require_login)])
async def list_watchers():
    async with _db_lock:
        rows = db.execute(
            "SELECT w.*, b.url, b.auth_token, b.backend_id AS blabel "
            "FROM watchers w JOIN backends b ON b.id=w.backend_pk "
            "ORDER BY w.created_at DESC"
        ).fetchall()

    by_backend: dict[str, dict] = {}
    for r in rows:
        by_backend.setdefault(r["backend_pk"],
            {"url":r["url"],"auth_token":r["auth_token"],"rows":[]})["rows"].append(r)

    live: dict[tuple, dict] = {}
    for bpk, info in by_backend.items():
        code, body = await _call(info["url"], info["auth_token"], "GET", "/watchers")
        if code == 200 and isinstance(body, list):
            for item in body:
                live[(bpk, item["username"])] = item

    return [
        {
            "username": r["username"],
            "backend_pk": r["backend_pk"],
            "backend_label": r["blabel"],
            "automatic_interval_min": r["automatic_interval_min"],
            "created_at": r["created_at"],
            **(live.get((r["backend_pk"], r["username"])) or {}),
            "reachable": (r["backend_pk"], r["username"]) in live,
        }
        for r in rows
    ]

@app.post("/api/watchers", status_code=201, dependencies=[Depends(require_login)])
async def add_watcher(body: WatcherCreate):
    username = body.username.lstrip("@").strip().lower()
    if not username:
        raise HTTPException(400, "empty username")
    async with _db_lock:
        if db.execute("SELECT 1 FROM watchers WHERE username=?", (username,)).fetchone():
            raise HTTPException(409, f"watcher for {username} already exists")

    if body.backend_pk:
        async with _db_lock:
            row = db.execute(
                "SELECT id,url,auth_token FROM backends WHERE id=?", (body.backend_pk,)
            ).fetchone()
        if not row:
            raise HTTPException(404, "backend_pk not found")
    else:
        row = await _pick_backend()

    code, resp = await _call(row["url"], row["auth_token"], "POST", "/watchers",
                             {"username": username,
                              "automatic_interval_min": body.automatic_interval_min,
                              "capture_chat": body.capture_chat})
    if code != 201:
        raise HTTPException(502 if code < 0 else code, f"backend refused: {resp}")

    async with _db_lock:
        db.execute(
            "INSERT INTO watchers (username,backend_pk,automatic_interval_min,created_at)"
            " VALUES (?,?,?,?)",
            (username, row["id"], body.automatic_interval_min, time.time()),
        )
        db.commit()
    return {"username": username, "backend_pk": row["id"]}

@app.delete("/api/watchers/{username}", status_code=204, dependencies=[Depends(require_login)])
async def delete_watcher(username: str):
    username = username.lstrip("@").strip().lower()
    async with _db_lock:
        row = db.execute(
            "SELECT w.username, b.url, b.auth_token "
            "FROM watchers w JOIN backends b ON b.id=w.backend_pk WHERE w.username=?",
            (username,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "no watcher")
    await _call(row["url"], row["auth_token"], "DELETE", f"/watchers/{username}")
    async with _db_lock:
        db.execute("DELETE FROM watchers WHERE username=?", (username,))
        db.commit()
    return Response(status_code=204)


@app.post("/api/watchers/{username}/restart", dependencies=[Depends(require_login)])
async def restart_watcher(username: str):
    """Re-enable a single watcher on whichever backend hosts it."""
    username = username.lstrip("@").strip().lower()
    async with _db_lock:
        row = db.execute(
            "SELECT w.username, b.url, b.auth_token "
            "FROM watchers w JOIN backends b ON b.id=w.backend_pk WHERE w.username=?",
            (username,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "no watcher")
    code, body = await _call(row["url"], row["auth_token"],
                             "POST", f"/watchers/{username}/restart")
    if code != 200:
        raise HTTPException(502, f"backend error {code}: {body}")
    return {"ok": True, "username": username, "state": (body or {}).get("state")}


class ChatToggleIn(BaseModel):
    enabled: bool


@app.post("/api/watchers/{username}/chat", dependencies=[Depends(require_login)])
async def toggle_watcher_chat(username: str, body: ChatToggleIn):
    """Enable/disable TikTok chat capture for a watcher on its backend."""
    username = username.lstrip("@").strip().lower()
    async with _db_lock:
        row = db.execute(
            "SELECT w.username, b.url, b.auth_token "
            "FROM watchers w JOIN backends b ON b.id=w.backend_pk WHERE w.username=?",
            (username,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "no watcher")
    code, resp = await _call(row["url"], row["auth_token"],
                             "POST", f"/watchers/{username}/chat",
                             {"enabled": body.enabled})
    if code != 200:
        raise HTTPException(502, f"backend error {code}: {resp}")
    return {"ok": True, "username": username,
            "capture_chat": (resp or {}).get("capture_chat", body.enabled)}


@app.post("/api/watchers/restart-errored", dependencies=[Depends(require_login)])
async def restart_errored_watchers():
    """Mass re-enable: restart every watcher currently in 'error' state across
    all healthy backends. Returns how many were restarted."""
    async with _db_lock:
        backends = db.execute(
            "SELECT id, url, auth_token FROM backends WHERE last_health_ok=1"
        ).fetchall()
    restarted, failed = [], []
    for b in backends:
        code, watchers = await _call(b["url"], b["auth_token"], "GET", "/watchers")
        if code != 200 or not isinstance(watchers, list):
            continue
        for w in watchers:
            if w.get("state") == "error":
                u = w["username"]
                rc, _ = await _call(b["url"], b["auth_token"],
                                    "POST", f"/watchers/{u}/restart")
                (restarted if rc == 200 else failed).append(u)
    return {"ok": True, "restarted": restarted, "failed": failed,
            "count": len(restarted)}


# ---------------------------------------------------------------------------
# Files

async def _files_from_catalog() -> list[dict]:
    """Build the Files-tab list from the catalog instead of polling every node.
    Same item shape as the live path, so the frontend is unchanged. Only recordings
    with a retrievable copy (recorder- or storage-local) are listed; the download/
    delete routing maps the catalog's store label back to the backend/storage id."""
    async with _db_lock:
        backends_by_label = {r["backend_id"]: r["id"]
                             for r in db.execute("SELECT id, backend_id FROM backends").fetchall()}
        stores_by_label = {r["label"]: r["id"]
                           for r in db.execute("SELECT id, label FROM storage_servers").fetchall()}
    recs = _catalog.conn.execute(
        "SELECT id, filename, creator, byte_size, started_at FROM recordings "
        "WHERE filename LIKE '%.mp4' ORDER BY started_at DESC LIMIT 5000").fetchall()
    locs_by_rec: dict[str, list] = {}
    for l in _catalog.conn.execute(
            "SELECT recording_id, tier, store, key FROM blob_locations").fetchall():
        locs_by_rec.setdefault(l["recording_id"], []).append(l)
    out: list[dict] = []
    for rec in recs:
        if rec["filename"].endswith("_flv.mp4"):
            continue
        locs = locs_by_rec.get(rec["id"], [])
        rec_loc = next((l for l in locs if l["tier"] == "local"
                        and l["store"] in backends_by_label), None)
        sto_loc = next((l for l in locs if l["tier"] == "local"
                        and l["store"] in stores_by_label), None)
        base = {"username": rec["creator"], "filename": rec["filename"],
                "size_bytes": rec["byte_size"], "mtime": rec["started_at"] or 0}
        if rec_loc:
            out.append({**base, "backend_pk": backends_by_label[rec_loc["store"]],
                        "backend_label": rec_loc["store"], "location": "recorder",
                        "path": rec_loc["key"]})
        elif sto_loc:
            out.append({**base, "backend_pk": None, "backend_label": "—",
                        "storage_sid": stores_by_label[sto_loc["store"]],
                        "storage_label": sto_loc["store"], "location": "storage",
                        "path": sto_loc["key"]})
        # cloud-only / no live copy → omitted in v1 (no direct download yet)
    return sorted(out, key=lambda x: x.get("mtime") or 0, reverse=True)


@app.get("/api/files", dependencies=[Depends(require_login)])
async def list_files(source: Optional[str] = None):
    src = source or FILES_SOURCE
    if src == "catalog" and _catalog is not None:
        try:
            return await _files_from_catalog()
        except Exception:
            log.exception("catalog-backed /api/files failed — falling back to live")
    async with _db_lock:
        rows = db.execute(
            "SELECT id, backend_id, url, auth_token FROM backends WHERE last_health_ok=1"
        ).fetchall()
    result = []
    seen: set[str] = set()        # filenames present on a recorder
    for r in rows:
        code, body = await _call(r["url"], r["auth_token"], "GET", "/files")
        if code == 200 and isinstance(body, dict):
            for uname, files in body.items():
                for f in files:
                    result.append({
                        "backend_pk": r["id"], "backend_label": r["backend_id"],
                        "username": uname, "location": "recorder", **f,
                    })
                    seen.add(Path(f["path"]).name)

    # Merge files that live ONLY on storage (no recorder copy) so the Files tab
    # still shows recordings after their recorder copies are cleaned up. These
    # rows are served/deleted against the storage server instead of a recorder.
    storage_seen: set[str] = set()
    for s in _storage_healthy():
        code, inv = await _tw_call(s, "GET", "/files/inventory")
        if code != 200 or not isinstance(inv, list):
            continue
        for f in inv:
            fn = f.get("filename", "")
            if not fn.endswith(".mp4") or fn.endswith("_flv.mp4"):
                continue                      # mp4 recordings only (skip chat/intermediates)
            if fn in seen or fn in storage_seen:
                continue                      # already shown (on a recorder, or another storage)
            storage_seen.add(fn)
            result.append({
                "backend_pk": None, "backend_label": "—",
                "storage_sid": s["id"], "storage_label": s["label"],
                "username": f.get("username", ""), "location": "storage",
                "path": f.get("path"), "filename": fn,
                "size_bytes": f.get("size_bytes"), "mtime": f.get("mtime", 0),
            })
    return sorted(result, key=lambda x: x.get("mtime", 0), reverse=True)


@app.post("/api/self-update", dependencies=[Depends(require_login)])
async def self_update():
    """Update the control plane in place from an uploaded tt-recorder.zip.

    Workflow the operator follows:
      1. upload tt-recorder.zip to the host (default /root/tt-recorder.zip)
      2. click Update — this unzips it over the control plane's own directory
         (which is also the deploy-files source for pushing to recorders/storage)
      3. the control plane restarts itself to load the new code

    Restart strategy (first that applies):
      • CONTROL_PLANE_RESTART_CMD env, if set (run detached)
      • systemctl restart CONTROL_PLANE_SERVICE, if that env is set
      • otherwise re-exec this process (python control_plane.py) — works whether
        run manually or under systemd, since the PID is preserved.
    """
    zip_path = Path(os.environ.get("CONTROL_PLANE_UPDATE_ZIP", "/root/tt-recorder.zip"))
    target   = Path(__file__).resolve().parent          # where the CP runs from

    async def _stream():
        yield f"Looking for {zip_path}…\n"
        if not zip_path.exists():
            yield (f"✗ Not found. Upload tt-recorder.zip to {zip_path.parent} "
                   f"(or set CONTROL_PLANE_UPDATE_ZIP) and try again.\n")
            return
        # Validate the archive before touching anything.
        try:
            with zipfile.ZipFile(zip_path) as zf:
                bad = zf.testzip()
                if bad:
                    yield f"✗ Archive is corrupt (bad entry: {bad}). Aborting.\n"
                    return
                names = set(zf.namelist())
        except zipfile.BadZipFile:
            yield "✗ Not a valid zip file. Aborting.\n"
            return
        missing_core = [f for f in ("control_plane.py", "dashboard.html", "app.js")
                        if f not in names]
        if missing_core:
            yield ("✗ This zip doesn't look like a tt-recorder bundle "
                   f"(missing {', '.join(missing_core)}). Aborting.\n")
            return
        yield f"Archive OK — {len(names)} files.\n"

        # Back up the files we're about to overwrite, so a bad update can be undone.
        ts = time.strftime("%Y%m%d-%H%M%S")
        backup = target / ".update-backups" / ts
        try:
            backup.mkdir(parents=True, exist_ok=True)
            for n in names:
                cur = target / n
                if cur.exists() and cur.is_file():
                    shutil.copy2(cur, backup / n)
            yield f"Backed up current files → {backup}\n"
            # keep only the 3 most recent backups
            allb = sorted((target / ".update-backups").glob("*"), reverse=True)
            for old in allb[3:]:
                shutil.rmtree(old, ignore_errors=True)
        except Exception as e:
            yield f"⚠ Backup step had an issue ({e}); continuing.\n"

        # Extract over the running directory.
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for n in names:
                    zf.extract(n, target)
            yield f"Unzipped into {target}\n"
        except Exception as e:
            yield f"✗ Unzip failed: {e}\n  Your previous files are intact.\n"
            return

        new_ver = _read_version()
        yield f"Now on build {new_ver}. Restarting the control plane…\n"
        yield "The dashboard will reconnect automatically in a few seconds.\n"

        # Schedule the restart shortly after this response is flushed.
        restart_cmd = os.environ.get("CONTROL_PLANE_RESTART_CMD", "").strip()
        service     = os.environ.get("CONTROL_PLANE_SERVICE", "").strip()

        def _restart():
            try:
                if restart_cmd:
                    subprocess.Popen(restart_cmd, shell=True, start_new_session=True)
                elif service:
                    # systemd-run keeps the restarter outside this unit's cgroup,
                    # so it survives us being stopped.
                    subprocess.Popen(
                        ["systemd-run", "--no-block", "--collect",
                         "systemctl", "restart", service],
                        start_new_session=True)
                else:
                    # Re-exec ourselves with the freshly written code (same PID).
                    os.execv(sys.executable,
                             [sys.executable, str(Path(__file__).resolve())])
            except Exception as e:
                log.error("self-update restart failed: %s", e)

        asyncio.get_running_loop().call_later(1.5, _restart)

    return StreamingResponse(_stream(), media_type="text/plain; charset=utf-8")


CP_SERVICE_NAME = "tt-control-plane"

def _cp_service_instructions() -> str:
    py = sys.executable
    script = str(Path(__file__).resolve())
    wd = str(Path(__file__).resolve().parent)
    return (
        "Run these on the control-plane host as root:\n\n"
        "sudo tee /etc/systemd/system/tt-control-plane.service >/dev/null <<'UNIT'\n"
        "[Unit]\nDescription=TikTok recorder control plane\n"
        "After=network-online.target\nWants=network-online.target\n\n"
        "[Service]\nType=simple\n"
        f"WorkingDirectory={wd}\n"
        f"ExecStart={py} {script}\n"
        "Environment=CONTROL_PLANE_PASSWORD=YOUR_PASSWORD\n"
        "Environment=CONTROL_PLANE_SERVICE=tt-control-plane\n"
        "Restart=always\nRestartSec=3\nOOMScoreAdjust=-800\nTimeoutStopSec=15\n\n"
        "[Install]\nWantedBy=multi-user.target\nUNIT\n\n"
        "sudo systemctl daemon-reload\n"
        "sudo systemctl enable --now tt-control-plane\n"
        "journalctl -u tt-control-plane -f\n")


@app.post("/api/control-plane/install-service", dependencies=[Depends(require_login)])
async def install_control_plane_service():
    """Install (and switch to) a systemd service for the control plane so it
    auto-restarts after any kill (e.g. OOM) and is protected from being the OOM
    victim. Requires running as root; otherwise returns copy-paste instructions."""
    py = sys.executable
    script = str(Path(__file__).resolve())
    wd = str(Path(__file__).resolve().parent)
    unit_path = "/etc/systemd/system/tt-control-plane.service"
    env_path = "/etc/tt-control-plane.env"
    under_systemd = bool(os.environ.get("INVOCATION_ID"))

    async def _stream():
        if os.geteuid() != 0:
            yield ("✗ The control plane isn't running as root, so it can't write "
                   "/etc/systemd. Do it manually:\n\n" + _cp_service_instructions())
            return
        if not shutil.which("systemctl"):
            yield "✗ systemctl not found — this host doesn't use systemd.\n"
            return
        # 1. env file (captures current CONTROL_PLANE_* config, incl. password)
        try:
            lines = [f"{k}={v}" for k, v in sorted(os.environ.items())
                     if k.startswith("CONTROL_PLANE_")]
            have = {l.split("=", 1)[0] for l in lines}
            if "CONTROL_PLANE_SERVICE" not in have:
                lines.append(f"CONTROL_PLANE_SERVICE={CP_SERVICE_NAME}")
            with open(env_path, "w") as f:
                f.write("\n".join(lines) + "\n")
            os.chmod(env_path, 0o600)
            yield f"Wrote {env_path} ({len(lines)} vars, mode 600).\n"
        except Exception as e:
            yield f"✗ Could not write {env_path}: {e}\n"
            return
        # 2. unit file
        unit = (
            "[Unit]\nDescription=TikTok recorder control plane\n"
            "After=network-online.target\nWants=network-online.target\n\n"
            "[Service]\nType=simple\n"
            f"WorkingDirectory={wd}\n"
            f"EnvironmentFile={env_path}\n"
            f"ExecStart={py} {script}\n"
            "Restart=always\nRestartSec=3\nOOMScoreAdjust=-800\nTimeoutStopSec=15\n\n"
            "[Install]\nWantedBy=multi-user.target\n")
        try:
            with open(unit_path, "w") as f:
                f.write(unit)
            yield f"Wrote {unit_path}.\n"
        except Exception as e:
            yield f"✗ Could not write {unit_path}: {e}\n"
            return
        # 3. enable
        try:
            subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=30)
            subprocess.run(["systemctl", "enable", CP_SERVICE_NAME],
                           check=True, timeout=30)
            yield f"Enabled {CP_SERVICE_NAME} (starts on boot).\n"
        except Exception as e:
            yield f"✗ enable failed: {e}\n"
            return
        yield ("OOMScoreAdjust=-800 set, Restart=always set — the site now "
               "self-heals from kills.\n")

        if under_systemd:
            yield "Already running under systemd; reloading config now…\n"
            def _reload():
                try:
                    subprocess.Popen(["systemd-run", "--no-block", "--collect",
                                      "systemctl", "restart", CP_SERVICE_NAME],
                                     start_new_session=True)
                except Exception as e:
                    log.error("cp service reload failed: %s", e)
            asyncio.get_running_loop().call_later(1.5, _reload)
        else:
            yield ("Switching this manually-started process over to systemd. "
                   "The dashboard will reconnect in a few seconds.\n")
            def _handoff():
                # Start the managed instance after we exit (so the port frees),
                # then exit this process.
                try:
                    subprocess.Popen(
                        ["systemd-run", "--no-block", "--collect", "bash", "-c",
                         f"sleep 2; systemctl start {CP_SERVICE_NAME}"],
                        start_new_session=True)
                finally:
                    os._exit(0)
            asyncio.get_running_loop().call_later(1.5, _handoff)

    return StreamingResponse(_stream(), media_type="text/plain; charset=utf-8")
    """Tell a transcription server to clear its give-up markers and re-attempt
    previously-failed files."""
    s = _storage_by_id(sid)
    if not s:
        raise HTTPException(404, "storage server not found")
    code, body = await _tw_call(s, "POST", "/retry-failed")
    if code != 200:
        raise HTTPException(502 if code < 0 else code, f"worker refused: {body}")
    return body


@app.get("/api/transcription-overview", dependencies=[Depends(require_login)])
async def transcription_overview():
    """Aggregate live transcription telemetry across all storage/transcription
    servers for the Transcription page."""
    out = []
    for s in _storage_all():
        entry = {"sid": s["id"], "label": s["label"], "url": s["url"],
                 "reachable": bool(s.get("last_health_ok"))}
        if s.get("last_health_ok"):
            code, body = await _tw_call(s, "GET", "/transcription-detail")
            if code == 200 and isinstance(body, dict):
                entry["detail"] = body
            else:
                entry["reachable"] = False
        out.append(entry)
    return out


@app.get("/api/file-locations", dependencies=[Depends(require_login)])
async def file_locations():
    """Map filename -> the storage server(s) that currently hold it, read live
    from each healthy storage server's inventory. This is ground truth, so it
    reflects server-to-server moves immediately (a moved file shows its new
    home; mid-move it may briefly show both source and destination)."""
    locs: dict[str, list] = {}
    for s in _storage_healthy():
        code, inv = await _tw_call(s, "GET", "/files/inventory")
        if code == 200 and isinstance(inv, list):
            for f in inv:
                fn = f.get("filename")
                if fn:
                    locs.setdefault(fn, []).append(
                        {"sid": s["id"], "label": s["label"]})
    return locs


@app.get("/api/files/download")
async def download_file(path: str, backend_pk: Optional[str] = None,
                        storage_sid: Optional[str] = None, inline: int = 0,
                        request: Request = None,
                        session: Optional[str] = Cookie(default=None)):
    """Proxy a file to the browser from either a recorder (backend_pk) or a
    storage server (storage_sid). Cookie check so a plain <a href> works.
    Forwards Range so an in-browser <video> can seek; `inline=1` serves it for
    playback rather than as a download."""
    if not _verify_session(session):
        raise HTTPException(401, "login required")

    if storage_sid:
        s = _storage_by_id(storage_sid)
        if not s:
            raise HTTPException(404, "storage server not found")
        src_url   = f"{s['url'].rstrip('/')}/files/raw"
        src_token = s["token"]
    else:
        async with _db_lock:
            row = db.execute(
                "SELECT url, auth_token FROM backends WHERE id=?", (backend_pk,)
            ).fetchone()
        if not row:
            raise HTTPException(404, "backend not found")
        src_url   = f"{row['url'].rstrip('/')}/files/download"
        src_token = row["auth_token"]

    filename = Path(path).name
    up_headers = {"Authorization": f"Bearer {src_token}"}
    rng = request.headers.get("range") if request else None
    if rng:
        up_headers["Range"] = rng

    client = httpx.AsyncClient(timeout=None)
    req = client.build_request("GET", src_url, headers=up_headers, params={"path": path})
    r = await client.send(req, stream=True)
    disp = "inline" if inline else "attachment"
    resp_headers = {"Accept-Ranges": "bytes",
                    "Content-Disposition": f'{disp}; filename="{filename}"'}
    for h in ("content-range", "content-length"):
        if h in r.headers:
            resp_headers[h.title()] = r.headers[h]

    async def _stream():
        try:
            async for chunk in r.aiter_bytes(chunk_size=65536):
                yield chunk
        finally:
            await r.aclose()
            await client.aclose()

    return StreamingResponse(_stream(), status_code=r.status_code,
                             media_type="video/mp4", headers=resp_headers)


@app.post("/api/files/delete", status_code=204, dependencies=[Depends(require_login)])
async def delete_file(body: DeleteFileBody):
    if body.storage_sid:
        s = _storage_by_id(body.storage_sid)
        if not s:
            raise HTTPException(404, "storage server not found")
        code, resp = await _tw_call(s, "POST", "/files/delete",
                                    json={"paths": [body.path]})
        if code != 200:
            raise HTTPException(502 if code < 0 else code, f"storage refused: {resp}")
        # Reflect that this file is no longer present (it may have had a 'done'
        # transfer record pointing at a recorder copy that's already gone).
        return Response(status_code=204)
    async with _db_lock:
        row = db.execute(
            "SELECT url, auth_token FROM backends WHERE id=?", (body.backend_pk,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "backend not found")
    code, resp = await _call(row["url"], row["auth_token"], "DELETE",
                             f"/files?path={body.path}")
    if code not in (200, 204):
        raise HTTPException(502 if code < 0 else code, f"backend refused: {resp}")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Transcript worker proxy

@app.get("/api/transcript-statuses", dependencies=[Depends(require_login)])
async def transcript_statuses():
    """Merge {filename: status} across all healthy storage servers.
    'done' wins over any other status if a file appears on more than one."""
    merged: dict[str, str] = {}
    rank = {"done": 3, "processing": 2, "pending": 1, "none": 0}
    for s in _storage_healthy():
        code, body = await _tw_call(s, "GET", "/transcripts/all-statuses")
        if code == 200 and isinstance(body, dict):
            for fname, st in body.items():
                if rank.get(st, 0) > rank.get(merged.get(fname, "none"), 0):
                    merged[fname] = st
    return merged


@app.get("/api/transcript-view", dependencies=[Depends(require_login)],
         response_class=PlainTextResponse)
async def transcript_view(filename: str):
    """Find the transcript on whichever storage server holds it."""
    for s in _storage_healthy():
        code, body = await _tw_call(s, "GET", "/transcripts/view",
                                    params={"filename": filename})
        if code == 200:
            return PlainTextResponse(body)
    raise HTTPException(404, "transcript not available yet")


@app.get("/api/transcript-download", dependencies=[Depends(require_login)])
async def transcript_download(filename: str):
    """Stream the .txt-with-timestamps from whichever storage server has it."""
    dl_name = filename.replace(".mp4", "_subtitles.txt")
    # Find which server has it (cheap status probe first)
    target = None
    for s in _storage_healthy():
        code, _ = await _tw_call(s, "GET", "/transcripts/view",
                                 params={"filename": filename})
        if code == 200:
            target = s
            break
    if not target:
        raise HTTPException(404, "transcript not available yet")

    hdrs = {}
    if target.get("token"):
        hdrs["Authorization"] = f"Bearer {target['token']}"
    url = f"{target['url'].rstrip('/')}/transcripts/download-srt"

    async def _stream():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", url, headers=hdrs,
                                     params={"filename": filename}) as r:
                if r.status_code != 200:
                    return
                async for chunk in r.aiter_bytes(65536):
                    yield chunk

    return StreamingResponse(
        _stream(), media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="{dl_name}"'},
    )


# ---------------------------------------------------------------------------
# Storage server registry

@app.get("/api/storage", dependencies=[Depends(require_login)])
async def list_storage():
    """List all storage servers with last-known health + disk."""
    out = []
    for s in _storage_all():
        tok = s.get("token") or ""
        masked = (tok[:6] + "…" + tok[-4:]) if len(tok) > 12 else ("set" if tok else "")
        reachable = bool(s.get("last_reachable"))
        usable = bool(s["last_health_ok"])
        # Three-state status: healthy / auth (reachable but token rejected) / down.
        state = "healthy" if usable else ("auth" if reachable else "down")
        out.append({
            "id": s["id"], "label": s["label"], "url": s["url"],
            "token_masked": masked,
            "reachable": usable,            # kept for back-compat (means "usable")
            "state": state,
            "is_reachable": reachable,
            "build": s.get("last_build"),
            "concurrency": s.get("last_concurrency"),
            "transcribe_enabled": (None if s.get("last_transcribe") is None
                                   else bool(s.get("last_transcribe"))),
            "vad": (None if s.get("last_vad") is None else bool(s.get("last_vad"))),
            "beam_size": s.get("last_beam"),
            "disk_free_bytes": s["last_disk_free"],
            "model": s["last_model"],
            "queue_depth": s["last_queue"],
            "last_checked": s["last_checked"],
        })
    return out


@app.post("/api/storage", dependencies=[Depends(require_login)])
async def add_storage(url: str, token: str,
                      ssh_host: Optional[str] = None, ssh_port: Optional[int] = None):
    """Add (or update) a storage server. Probes it immediately to validate."""
    url = url.rstrip("/")
    # Probe with the supplied token before saving
    code, body = await _tw_call({"url": url, "token": token}, "GET", "/status")
    async with _db_lock:
        sid = _storage_upsert(url, token)
        sh = (ssh_host or "").strip() or None
        if sh or ssh_port:
            db.execute(
                "UPDATE storage_servers SET ssh_host=COALESCE(?,ssh_host), "
                "ssh_port=COALESCE(?,ssh_port) WHERE id=?", (sh, ssh_port, sid))
        if code == 200 and isinstance(body, dict):
            db.execute(
                "UPDATE storage_servers SET last_health_ok=1, last_disk_free=?, "
                "last_model=?, last_queue=?, last_checked=? WHERE id=?",
                (body.get("disk_free_bytes"), body.get("model"),
                 body.get("queue_depth"), time.time(), sid),
            )
        db.commit()   # persist ssh endpoint + (if reachable) health, even when probe failed
    return {"ok": True, "id": sid, "reachable": code == 200}


@app.post("/api/storage/{sid}/token", dependencies=[Depends(require_login)])
async def update_storage_token(sid: str, body: dict):
    """Update a storage server's token in place (no remove/re-add), then re-probe."""
    token = (body or {}).get("token", "").strip()
    if not token:
        raise HTTPException(400, "token required")
    s = _storage_by_id(sid)
    if not s:
        raise HTTPException(404, "storage server not found")
    code, st = await _tw_call({"url": s["url"], "token": token}, "GET", "/status")
    ok = code == 200 and isinstance(st, dict)
    async with _db_lock:
        db.execute("UPDATE storage_servers SET token=?, last_health_ok=?, "
                   "last_reachable=?, last_build=?, last_checked=? WHERE id=?",
                   (token, 1 if ok else 0, 1 if ok else 0,
                    st.get("build") if ok else None, time.time(), sid))
        db.commit()
    return {"ok": True, "validated": ok}


@app.get("/api/ssh-creds/{host}", dependencies=[Depends(require_login)])
async def get_ssh_creds(host: str, port: Optional[int] = None):
    """Non-secret SSH cred summary for prefilling/collapsing deploy/update forms."""
    return _ssh_creds_summary(host, port)


@app.get("/api/version-status", dependencies=[Depends(require_login)])
async def version_status():
    """Which nodes are running an out-of-date build vs the control plane."""
    expected = BUILD
    nodes = []
    async with _db_lock:
        for b in db.execute("SELECT backend_id, last_build, last_health_ok FROM backends").fetchall():
            nodes.append({"kind": "recorder", "name": b["backend_id"],
                          "build": b["last_build"], "ok": bool(b["last_health_ok"])})
        for s in db.execute("SELECT label, last_build, last_health_ok, last_reachable FROM storage_servers").fetchall():
            nodes.append({"kind": "storage", "name": s["label"],
                          "build": s["last_build"],
                          "ok": bool(s["last_health_ok"]) or bool(s["last_reachable"])})
    outdated = [n for n in nodes if n["ok"] and n["build"] and n["build"] != expected]
    unknown = [n for n in nodes if n["ok"] and not n["build"]]
    return {"expected": expected, "nodes": nodes,
            "outdated": outdated, "unknown": unknown,
            "count_outdated": len(outdated) + len(unknown)}


@app.get("/api/update-status", dependencies=[Depends(require_login)])
async def update_status():
    """Per-machine update status for the Updates tab. Groups recorder/storage
    entries by host so a colocated box is a single row that updates both roles,
    and reports whether one-click (saved-credential) updates are available."""
    expected = BUILD
    hosts: dict[str, dict] = {}

    def _entry(mkey, host, port):
        return hosts.setdefault(mkey, {
            "host": host, "ssh_port": port, "roles": [], "names": {}, "builds": {},
            "reachable": {}, "recorder_id": None, "storage_id": None,
        })

    async with _db_lock:
        recs = db.execute(
            "SELECT id, backend_id, url, last_build, last_health_ok, ssh_host, ssh_port "
            "FROM backends"
        ).fetchall()
        stos = db.execute(
            "SELECT id, label, url, last_build, last_health_ok, last_reachable, "
            "ssh_host, ssh_port FROM storage_servers"
        ).fetchall()

    # Group by SSH endpoint (host:port), so two machines behind one public IP on
    # different forwarded SSH ports are distinct rows, while a genuinely colocated
    # box (one SSH endpoint serving both roles) stays a single row.
    for b in recs:
        sh, sp = _ssh_target(b)
        e = _entry(_mkey(sh, sp), sh, sp)
        e["roles"].append("recorder"); e["recorder_id"] = b["id"]
        e["names"]["recorder"] = b["backend_id"]; e["builds"]["recorder"] = b["last_build"]
        e["reachable"]["recorder"] = bool(b["last_health_ok"])
    for s in stos:
        sh, sp = _ssh_target(s)
        e = _entry(_mkey(sh, sp), sh, sp)
        e["roles"].append("storage"); e["storage_id"] = s["id"]
        e["names"]["storage"] = s["label"]; e["builds"]["storage"] = s["last_build"]
        e["reachable"]["storage"] = bool(s["last_health_ok"]) or bool(s["last_reachable"])

    out = []
    for mkey, e in hosts.items():
        h = e["host"]
        roles = sorted(set(e["roles"]))
        colocated = "recorder" in roles and "storage" in roles
        if colocated:
            kind, tid = "colocated", e["recorder_id"]
        elif "recorder" in roles:
            kind, tid = "recorder", e["recorder_id"]
        else:
            kind, tid = "storage", e["storage_id"]
        # Up to date only if EVERY reachable role reports the expected build.
        # Keying on reachability (not just "build is non-null") means a colocated
        # box can't look current off its recorder alone while its transcription
        # worker is stale or silently not reporting a build.
        relevant = [e["builds"].get(role) for role in roles if e["reachable"].get(role)]
        up_to_date = bool(relevant) and all(v == expected for v in relevant)
        known = [v for v in e["builds"].values() if v]
        reachable = any(e["reachable"].values())
        creds = _get_creds_mkey(h, e["ssh_port"])
        has_creds = bool(creds and (creds.get("password_enc") or creds.get("key_path")))
        out.append({
            "host": h, "ssh_port": e["ssh_port"],
            "name": e["names"].get("recorder") or e["names"].get("storage"),
            "roles": roles, "colocated": colocated,
            "kind": kind, "target_id": tid,
            "build": (known[0] if len(set(known)) == 1 else " / ".join(sorted(set(known)))) if known else None,
            "builds": e["builds"],
            "reachable": reachable, "has_creds": has_creds,
            "ssh_user": (creds or {}).get("ssh_user") if has_creds else None,
            "up_to_date": up_to_date,
        })
    out.sort(key=lambda x: (x["up_to_date"], not x["reachable"], x["host"]))
    n_out = sum(1 for x in out if x["reachable"] and not x["up_to_date"])
    n_ready = sum(1 for x in out if x["reachable"] and not x["up_to_date"] and x["has_creds"])
    return {"expected": expected, "hosts": out,
            "count_outdated": n_out, "count_ready": n_ready}


def _saved_creds_push_request(host: str, port, update_type: str,
                              target_id: str) -> Optional["PushUpdateRequest"]:
    """Build a PushUpdateRequest from a machine's saved SSH credentials, or return
    None if no usable credentials are stored. Single source of truth for the
    one-click update paths (update_node / update_all)."""
    creds = _get_creds_mkey(host, port) if host else None
    if not (creds and (creds.get("password_enc") or creds.get("key_path"))):
        return None
    return PushUpdateRequest(
        update_type=update_type, target_id=target_id,
        ssh_port=port or creds.get("ssh_port") or 22,
        ssh_user=creds.get("ssh_user") or "root",
        auth_method=creds.get("auth_method") or "key",
        ssh_password=_decrypt_pw(creds.get("password_enc")),
        ssh_key_path=creds.get("key_path"))


@app.post("/api/update-node", dependencies=[Depends(require_login)])
async def update_node(req: UpdateNodeRequest):
    """Update a single machine using its saved SSH credentials — no password
    re-entry. Streams progress."""
    async with _db_lock:
        if req.kind in ("recorder", "colocated"):
            row = db.execute("SELECT url, ssh_host, ssh_port FROM backends WHERE id=?",
                             (req.target_id,)).fetchone()
        else:
            row = db.execute("SELECT url, ssh_host, ssh_port FROM storage_servers WHERE id=?",
                             (req.target_id,)).fetchone()
    if not row:
        raise HTTPException(404, "target not found")
    host, port = _ssh_target(row)

    def work(emit):
        pr = _saved_creds_push_request(host, port, req.kind, req.target_id)
        if pr is None:
            emit(f"No saved SSH credentials for {host}:{port}.\n"
                 "Run one Push update for this host in the Deploy tab — your "
                 "details are saved after that, and updates here become one click.\n")
            return
        emit(f"Updating {host}:{port} ({req.kind}) with saved credentials…\n")
        _ssh_push_update(pr, host, emit)

    return _threaded_stream(work)


@app.post("/api/update-all", dependencies=[Depends(require_login)])
async def update_all():
    """Push the latest code to every registered node (recorder + storage) using
    the SSH credentials saved for each machine. Nodes without saved creds are
    skipped with a note. Streams output, node by node."""
    targets = []  # (update_type, target_id, host, port, name)
    async with _db_lock:
        for b in db.execute("SELECT id, url, backend_id, ssh_host, ssh_port FROM backends").fetchall():
            sh, sp = _ssh_target(b)
            targets.append(("recorder", b["id"], sh, sp, b["backend_id"]))
        for s in db.execute("SELECT id, url, label, ssh_host, ssh_port FROM storage_servers").fetchall():
            sh, sp = _ssh_target(s)
            targets.append(("storage", s["id"], sh, sp, s["label"]))

    def work(emit):
        done = 0
        for kind, tid, host, port, name in targets:
            pr = _saved_creds_push_request(host, port, kind, tid)
            if pr is None:
                emit(f"\n===== {name} ({host}:{port}) — SKIPPED: no saved SSH credentials =====\n")
                continue
            emit(f"\n===== Updating {name} ({kind} @ {host}:{port}) =====\n")
            try:
                _ssh_push_update(pr, host, emit)
                done += 1
            except Exception as e:
                emit(f"[ERROR] {name}: {e}\n")
        emit(f"\n==> Done. Updated {done}/{len(targets)} node(s).\n")

    return _threaded_stream(work)


@app.delete("/api/storage/{sid}", dependencies=[Depends(require_login)])
async def delete_storage(sid: str):
    """Disconnect a storage server. Recordings already on it stay there;
    this only stops the control plane from using it."""
    async with _db_lock:
        db.execute("DELETE FROM storage_servers WHERE id=?", (sid,))
        db.commit()
    return {"ok": True}


@app.get("/api/backup", dependencies=[Depends(require_login)])
async def download_backup():
    """Stream a consistent snapshot of the control-plane database as a download.
    Uses SQLite's online backup API so it's safe to run against the live DB."""
    import tempfile
    fd, tmp = tempfile.mkstemp(suffix=".sqlite", prefix="tt-backup-")
    os.close(fd)
    try:
        async with _db_lock:
            dest = sqlite3.connect(tmp)
            try:
                db.backup(dest)          # atomic, consistent snapshot of the live DB
            finally:
                dest.close()
        data = Path(tmp).read_bytes()
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass
    fname = f"tt-control-backup-{time.strftime('%Y%m%d-%H%M%S')}.sqlite"
    return Response(content=data, media_type="application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


# ---------------------------------------------------------------------------
# Routing / retention rules

class RoutingRule(BaseModel):
    backend_pk: str                       # a backend id, or '*' for the global default
    target_storage_pk: Optional[str] = None   # None/'' = auto (most free disk)
    delete_mode: str = Field("immediate", pattern="^(immediate|delay|never)$")
    delete_delay_sec: int = Field(0, ge=0)


@app.get("/api/routing", dependencies=[Depends(require_login)])
async def get_routing():
    """Return all backends, all storage servers, and the configured rules so the
    UI can render the routing table. Includes the implicit global default."""
    async with _db_lock:
        backends = [dict(r) for r in db.execute(
            "SELECT id, backend_id, region, url FROM backends ORDER BY added_at"
        ).fetchall()]
        rules = {r["backend_pk"]: dict(r) for r in db.execute(
            "SELECT backend_pk, target_storage_pk, delete_mode, delete_delay_sec "
            "FROM routing_rules"
        ).fetchall()}
    storages = [{"id": s["id"], "label": s["label"], "url": s["url"],
                 "healthy": bool(s["last_health_ok"])} for s in _storage_all()]
    default_rule = rules.get("*", {
        "backend_pk": "*", "target_storage_pk": None,
        "delete_mode": _default_delete_mode(), "delete_delay_sec": 0,
    })
    return {"backends": backends, "storages": storages,
            "rules": rules, "default": default_rule}


@app.post("/api/routing", dependencies=[Depends(require_login)])
async def set_routing(rule: RoutingRule):
    """Create/update the rule for a backend (or '*' for the global default)."""
    tgt = rule.target_storage_pk or None
    if tgt and not _storage_by_id(tgt):
        raise HTTPException(400, "target storage not found")
    if rule.backend_pk != "*":
        async with _db_lock:
            exists = db.execute("SELECT 1 FROM backends WHERE id=?",
                                (rule.backend_pk,)).fetchone()
        if not exists:
            raise HTTPException(404, "backend not found")
    async with _db_lock:
        db.execute(
            "INSERT INTO routing_rules "
            "(id, backend_pk, target_storage_pk, delete_mode, delete_delay_sec, updated_at) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(backend_pk) DO UPDATE SET "
            "target_storage_pk=excluded.target_storage_pk, "
            "delete_mode=excluded.delete_mode, "
            "delete_delay_sec=excluded.delete_delay_sec, "
            "updated_at=excluded.updated_at",
            (str(uuid.uuid4()), rule.backend_pk, tgt,
             rule.delete_mode, rule.delete_delay_sec, time.time()),
        )
        db.commit()
    return {"ok": True}


@app.delete("/api/routing/{backend_pk}", dependencies=[Depends(require_login)])
async def delete_routing(backend_pk: str):
    """Remove a backend's rule (it then falls back to the global default)."""
    async with _db_lock:
        db.execute("DELETE FROM routing_rules WHERE backend_pk=?", (backend_pk,))
        db.commit()
    return {"ok": True}


@app.get("/api/archive/overview", dependencies=[Depends(require_login)])
async def archive_overview():
    """Per-storage-server cloud-archive configuration + counters, read live from
    each server's /status. Powers the Archive tab's 'current configuration' view."""
    out = []
    for s in _storage_all():
        entry = {"id": s["id"], "label": s["label"] or s["url"], "url": s["url"],
                 "healthy": bool(s["last_health_ok"]), "configured": False,
                 "remote": None, "what": None, "delete_local": None,
                 "evict_high_pct": None, "archived_count": None,
                 "archive_failed_count": None, "last_evict": None}
        code, st = await _tw_call(s, "GET", "/status")
        if code == 200 and isinstance(st, dict):
            entry["remote"] = st.get("archive_remote")
            entry["configured"] = bool(st.get("archive_remote"))
            entry["what"] = st.get("archive_what")
            entry["delete_local"] = st.get("archive_delete_local")
            entry["evict_high_pct"] = st.get("archive_evict_high_pct")
            entry["evict_low_pct"] = st.get("archive_evict_low_pct")
            entry["evict_min_age_sec"] = st.get("archive_evict_min_age_sec")
            entry["archived_count"] = st.get("archived_count")
            entry["archive_failed_count"] = st.get("archive_failed_count")
            entry["last_evict"] = st.get("last_evict")
        out.append(entry)
    return {"servers": out}


@app.get("/api/storage-breakdown", dependencies=[Depends(require_login)])
async def storage_breakdown():
    """Per-storage-server usage with a per-creator breakdown, by querying each
    server's inventory and disk status. Used by the Storage tab breakdown view."""
    servers = _storage_all()
    out = []
    for s in servers:
        entry = {
            "id": s["id"], "label": s["label"] or s["url"], "url": s["url"],
            "healthy": bool(s["last_health_ok"]),
            "disk_free": None, "disk_total": None,
            "recordings_bytes": 0, "file_count": 0, "creators": [],
        }
        # Disk totals from /status
        code, st = await _tw_call(s, "GET", "/status")
        if code == 200 and isinstance(st, dict):
            entry["disk_free"] = st.get("disk_free_bytes")
            entry["disk_total"] = st.get("disk_total_bytes")
            entry["archive_remote"] = st.get("archive_remote")
            entry["archived_count"] = st.get("archived_count")
            entry["active"] = st.get("active") or []
            entry["concurrency"] = st.get("concurrency")
            entry["queue_depth"] = st.get("queue_depth")
            entry["build"] = st.get("build")
        # Per-creator aggregation from inventory
        code, inv = await _tw_call(s, "GET", "/files/inventory")
        if code == 200 and isinstance(inv, list):
            by_creator: dict[str, dict] = {}
            for fi in inv:
                u = fi.get("username", "?")
                c = by_creator.setdefault(u, {"username": u, "files": 0, "bytes": 0,
                                              "last_mtime": 0})
                c["files"] += 1
                c["bytes"] += fi.get("size_bytes", 0) or 0
                c["last_mtime"] = max(c["last_mtime"], fi.get("mtime", 0) or 0)
                entry["recordings_bytes"] += fi.get("size_bytes", 0) or 0
                entry["file_count"] += 1
            entry["creators"] = sorted(by_creator.values(),
                                       key=lambda x: x["bytes"], reverse=True)
        out.append(entry)
    return {"servers": out}


# ---------------------------------------------------------------------------
# Cloud archive (3rd hop) — configured entirely from the UI via SSH.
# The control plane writes an rclone.conf to the storage box and sets the
# ARCHIVE_* env vars, then restarts the service. rclone does the verified
# transfer; we never hand-roll cloud upload.

STORAGE_INSTALL_DIR = "/opt/tt-storage"
STORAGE_ENV_FILE    = "/etc/tt-storage.env"
STORAGE_SERVICE_USER = "tt"
STORAGE_RCLONE_CONF = f"{STORAGE_INSTALL_DIR}/rclone.conf"


class ArchiveConfigRequest(BaseModel):
    ssh_port: int = Field(22, ge=1, le=65535)
    ssh_user: str = "root"
    auth_method: str = Field(..., pattern="^(password|key)$")
    ssh_password: Optional[str] = None
    ssh_key_path: Optional[str] = None
    provider: str = Field(..., pattern="^(s3|b2|dropbox|filen|raw)$")
    remote_name: str = "archive"
    remote_path: str = ""                 # bucket/path, e.g. "mybucket/tt-recordings"
    # provider-specific
    s3_access_key: Optional[str] = None
    s3_secret: Optional[str] = None
    s3_region: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_provider: Optional[str] = None     # AWS|Other|Wasabi|Cloudflare|Minio|DigitalOcean
    b2_account: Optional[str] = None
    b2_key: Optional[str] = None
    dropbox_token: Optional[str] = None   # JSON from `rclone authorize "dropbox"`
    filen_email: Optional[str] = None     # Filen account email (E2E-encrypted; needs rclone >= 1.73)
    filen_password: Optional[str] = None  # Filen account password (obscured on the box)
    filen_2fa: Optional[str] = None       # optional 2FA code, used only when auto-exporting the key
    filen_api_key: Optional[str] = None   # API key (obscured). Leave blank to auto-export it on the box.
    raw_config: Optional[str] = None      # full rclone.conf block (paste path)
    # archive behaviour
    archive_what: str = Field("mp4", pattern="^(mp4|txt|both)$")
    delete_local: bool = False
    delete_delay_sec: int = Field(0, ge=0)
    # disk-aware eviction + rclone tuning
    evict_high_pct: float = Field(85, ge=1, le=100)
    evict_low_pct: float = Field(70, ge=0, le=100)
    evict_min_age_sec: int = Field(86400, ge=0)
    transfers: int = Field(4, ge=1, le=32)
    bwlimit: str = ""
    retry_backoff_sec: int = Field(120, ge=1)


def _build_rclone_conf(req: ArchiveConfigRequest) -> str:
    """Return rclone.conf text for the chosen provider."""
    name = req.remote_name.strip() or "archive"
    if not re.match(r"^[A-Za-z0-9_-]+$", name):
        raise HTTPException(400, "remote name must be alphanumeric/_/-")
    if req.provider == "raw":
        if not req.raw_config or "[" not in req.raw_config:
            raise HTTPException(400, "raw_config must be a full rclone.conf block")
        return req.raw_config.strip() + "\n"
    lines = [f"[{name}]"]
    if req.provider == "s3":
        if not (req.s3_access_key and req.s3_secret):
            raise HTTPException(400, "S3 needs access key and secret")
        lines += ["type = s3",
                  f"provider = {req.s3_provider or ('Other' if req.s3_endpoint else 'AWS')}",
                  f"access_key_id = {req.s3_access_key}",
                  f"secret_access_key = {req.s3_secret}"]
        if req.s3_region:   lines.append(f"region = {req.s3_region}")
        if req.s3_endpoint: lines.append(f"endpoint = {req.s3_endpoint}")
    elif req.provider == "b2":
        if not (req.b2_account and req.b2_key):
            raise HTTPException(400, "B2 needs account ID and application key")
        lines += ["type = b2", f"account = {req.b2_account}", f"key = {req.b2_key}"]
    elif req.provider == "dropbox":
        if not req.dropbox_token:
            raise HTTPException(400, "Dropbox needs a token (from `rclone authorize \"dropbox\"`)")
        lines += ["type = dropbox", f"token = {req.dropbox_token.strip()}"]
    elif req.provider == "filen":
        if not (req.filen_email and req.filen_password
                and req.filen_api_key and req.filen_api_key.strip()):
            raise HTTPException(400, "Filen needs email, password, AND an API key. "
                                     "Get the key once with `filen export-api-key` from the "
                                     "Filen CLI (https://github.com/FilenCloudDienste/filen-cli).")
        # email is plaintext; password + api_key MUST be obscured (rclone obscure).
        # We write placeholders here and obscure them on the storage box in
        # _ssh_archive_config, so the plaintext never lands in rclone.conf or in
        # this process's memory longer than needed. Filen is native in rclone >= 1.73,
        # and its backend reads api_key from config — it does NOT derive it by login,
        # so the api_key is required, not optional.
        lines += ["type = filen", f"email = {req.filen_email.strip()}",
                  "password = __FILEN_OBSCURE_PW__",
                  "api_key = __FILEN_OBSCURE_AK__"]
    return "\n".join(lines) + "\n"


def _filen_derive_api_key(req: ArchiveConfigRequest, host: str, emit) -> Optional[str]:
    """Log in to Filen on the storage box with the Filen CLI and export an API key,
    so the operator never has to run the CLI by hand. Installs the CLI if missing.
    Credentials are passed via env vars (base64'd in transit) and never written to
    disk by us. Returns the key, or None on failure (operator can paste one instead)."""
    import base64, re as _re
    client = _ssh_connect(host, req.ssh_port, req.ssh_user, req.auth_method,
                          req.ssh_password, req.ssh_key_path, emit)
    if not client:
        return None
    try:
        em = base64.b64encode((req.filen_email or "").encode()).decode()
        pw = base64.b64encode((req.filen_password or "").encode()).decode()
        twofa = (req.filen_2fa or "").strip()
        twofa_line = f"export FILEN_2FA_CODE={shlex_quote(twofa)}\n" if twofa else ""
        emit("==> No API key supplied — exporting one on the storage box via the Filen CLI ...\n")
        script = f"""set +e
export FILEN_EMAIL=$(echo {shlex_quote(em)} | base64 -d)
export FILEN_PASSWORD=$(echo {shlex_quote(pw)} | base64 -d)
{twofa_line}FILEN_BIN=$(command -v filen 2>/dev/null)
if [ -z "$FILEN_BIN" ]; then
  echo INSTALLING_FILEN_CLI
  curl -sL https://filen.io/cli.sh | bash >/dev/null 2>&1
fi
# The install script drops the binary at ~/.filen-cli/bin/filen, which is not on
# PATH in a non-login shell — search the known locations explicitly.
if [ -z "$FILEN_BIN" ]; then
  FILEN_BIN=$(command -v filen 2>/dev/null || ls "$HOME/.filen-cli/bin/filen" /usr/local/bin/filen /usr/bin/filen "$HOME/.local/bin/filen" 2>/dev/null | head -1)
fi
if [ -z "$FILEN_BIN" ]; then
  FILEN_BIN=$(find "$HOME" /usr/local /opt -maxdepth 5 -name filen -type f 2>/dev/null | head -1)
fi
if [ -z "$FILEN_BIN" ]; then echo NO_FILEN_CLI; exit 0; fi
chmod +x "$FILEN_BIN" 2>/dev/null
echo KEY_BEGIN
"$FILEN_BIN" export-api-key </dev/null 2>&1
echo KEY_END
"""
        _, so, _ = client.exec_command(script, timeout=240)
        out = so.read().decode("utf-8", errors="replace")
        if "INSTALLING_FILEN_CLI" in out:
            emit("    installed the Filen CLI on the box.\n")
        if "NO_FILEN_CLI" in out:
            emit("    [ERROR] Could not find or install the Filen CLI (need curl + a working install).\n")
            return None
        body = out.split("KEY_BEGIN", 1)[-1].split("KEY_END", 1)[0]
        m = _re.search(r"API Key for[^:]*:\s*([A-Za-z0-9]+)", body)
        key = m.group(1) if m else None
        if not key:                                  # fall back to the longest token
            toks = _re.findall(r"[A-Za-z0-9]{40,}", body)
            key = max(toks, key=len) if toks else None
        if key:
            emit(f"    exported an API key ({len(key)} chars).\n")
            return key
        emit("    [ERROR] Filen CLI ran but no API key was found. Output:\n")
        emit("      " + body.strip()[-600:] + "\n")
        return None
    except Exception as e:
        emit(f"    [ERROR] {type(e).__name__}: {e}\n")
        return None
    finally:
        client.close()


def _ssh_archive_config(req: ArchiveConfigRequest, host: str, emit) -> None:
    import base64
    # Filen: if the operator left the API key blank, export one on the box for them.
    if req.provider == "filen" and not (req.filen_api_key and req.filen_api_key.strip()):
        if not (req.filen_email and req.filen_password):
            emit("[ERROR] Filen needs at least an email and password.\n")
            return
        key = _filen_derive_api_key(req, host, emit)
        if not key:
            emit("[ERROR] Could not auto-export a Filen API key. Run `filen export-api-key`\n"
                 "        yourself and paste the key into the API key field, then retry.\n")
            return
        req.filen_api_key = key
    conf = _build_rclone_conf(req)
    name = (req.remote_name.strip() or "archive") if req.provider != "raw" else \
        (re.search(r"\[([^\]]+)\]", req.raw_config).group(1) if req.raw_config else "archive")
    archive_remote = f"{name}:{req.remote_path.strip().lstrip('/')}"
    b64 = base64.b64encode(conf.encode()).decode()
    sudo = "" if req.ssh_user == "root" else "sudo -n "
    # Filen needs its secrets obscured (rclone obscure) on the box. Do it there so
    # plaintext never persists in the conf; the obscured output is URL-safe base64
    # (A-Za-z0-9-_), so it is safe inside the sed replacement below.
    filen_post = ""
    if req.provider == "filen":
        pw_b64 = base64.b64encode((req.filen_password or "").encode()).decode()
        filen_post = (
            f'FPW=$(echo {shlex_quote(pw_b64)} | base64 -d); '
            f'OBS=$(rclone obscure "$FPW"); '
            f'{sudo}sed -i "s|__FILEN_OBSCURE_PW__|$OBS|" {STORAGE_RCLONE_CONF}\n'
        )
        if req.filen_api_key and req.filen_api_key.strip():
            ak_b64 = base64.b64encode(req.filen_api_key.strip().encode()).decode()
            filen_post += (
                f'FAK=$(echo {shlex_quote(ak_b64)} | base64 -d); '
                f'OBSA=$(rclone obscure "$FAK"); '
                f'{sudo}sed -i "s|__FILEN_OBSCURE_AK__|$OBSA|" {STORAGE_RCLONE_CONF}\n'
            )
    env_kvs = [
        f"RCLONE_CONFIG={STORAGE_RCLONE_CONF}",
        f"ARCHIVE_REMOTE={archive_remote}",
        f"ARCHIVE_WHAT={req.archive_what}",
        f"ARCHIVE_DELETE_LOCAL={'1' if req.delete_local else '0'}",
        f"ARCHIVE_DELETE_DELAY_SEC={req.delete_delay_sec}",
        f"ARCHIVE_EVICT_HIGH_PCT={req.evict_high_pct}",
        f"ARCHIVE_EVICT_LOW_PCT={req.evict_low_pct}",
        f"ARCHIVE_EVICT_MIN_AGE_SEC={req.evict_min_age_sec}",
        f"ARCHIVE_TRANSFERS={req.transfers}",
        f"ARCHIVE_BWLIMIT={req.bwlimit}",
        f"ARCHIVE_RETRY_BACKOFF={req.retry_backoff_sec}",
    ]
    upsert = "\n".join(
        f'{sudo}sed -i "/^{kv.split("=")[0]}=/d" {STORAGE_ENV_FILE}; '
        f'echo {shlex_quote(kv)} | {sudo}tee -a {STORAGE_ENV_FILE} >/dev/null'
        for kv in env_kvs
    )
    script = f"""set -e
echo '{b64}' | base64 -d | {sudo}tee {STORAGE_RCLONE_CONF} >/dev/null
{sudo}chown {STORAGE_SERVICE_USER}:{STORAGE_SERVICE_USER} {STORAGE_RCLONE_CONF}
{sudo}chmod 600 {STORAGE_RCLONE_CONF}
{filen_post}{upsert}
{sudo}systemctl restart tt-transcription
echo "ARCHIVE_CONFIG_DONE"
"""
    client = _ssh_connect(host, req.ssh_port, req.ssh_user,
                          req.auth_method, req.ssh_password, req.ssh_key_path, emit)
    if not client:
        return
    try:
        emit(f"==> Writing rclone remote '{name}' and archive settings to {host} ...\n")
        emit(f"    archive target: {archive_remote}  ({req.archive_what}, "
             f"delete_local={'on' if req.delete_local else 'off'})\n")
        _, stdout, stderr = client.exec_command(script, timeout=60)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        if "ARCHIVE_CONFIG_DONE" in out:
            emit("==> rclone configured, env updated, service restarted.\n")
            emit("==> Verifying rclone can see the remote ...\n")
            _, vo, _ = client.exec_command(
                f"sudo -u {STORAGE_SERVICE_USER} RCLONE_CONFIG={STORAGE_RCLONE_CONF} "
                f"rclone listremotes 2>&1 | head", timeout=20)
            emit("    remotes: " + vo.read().decode("utf-8", errors="replace").strip() + "\n")
            if req.provider == "filen":
                _, rv, _ = client.exec_command("rclone version 2>&1 | head -1", timeout=15)
                ver = rv.read().decode("utf-8", errors="replace").strip()
                emit(f"    {ver}\n")
                emit("    note: Filen is a native rclone backend from v1.73 — if the version\n"
                     "          above is older, run `rclone selfupdate` on the storage box.\n")
            ok = _run_archive_roundtrip(client, archive_remote, emit)
            if not ok:
                emit("\n==> Archive is configured but the self-test did NOT pass — recordings\n"
                     "    may not actually reach the cloud. Fix the issue above and re-test.\n")
                return
            emit("\n==> Cloud archive enabled. New recordings will be archived after transcription.\n")
        else:
            emit("[ERROR] config script did not complete.\n")
            if err.strip():
                emit("---- stderr ----\n" + err[-1500:] + "\n")
    except Exception as e:
        emit(f"[ERROR] {type(e).__name__}: {e}\n")
    finally:
        client.close()


def _ssh_archive_disable(host: str, req_ssh: dict, emit) -> None:
    sudo = "" if req_ssh["ssh_user"] == "root" else "sudo -n "
    script = f"""set -e
{sudo}sed -i "/^ARCHIVE_REMOTE=/d" {STORAGE_ENV_FILE}
{sudo}systemctl restart tt-transcription
echo "ARCHIVE_DISABLED"
"""
    client = _ssh_connect(host, req_ssh["ssh_port"], req_ssh["ssh_user"],
                          req_ssh["auth_method"], req_ssh.get("ssh_password"),
                          req_ssh.get("ssh_key_path"), emit)
    if not client:
        return
    try:
        emit(f"==> Disabling cloud archive on {host} ...\n")
        _, so, se = client.exec_command(script, timeout=30)
        if "ARCHIVE_DISABLED" in so.read().decode("utf-8", errors="replace"):
            emit("==> Archive disabled (rclone.conf kept; remove it manually if you want).\n")
        else:
            emit("[ERROR] " + se.read().decode("utf-8", errors="replace")[-800:] + "\n")
    finally:
        client.close()


class EvictionRequest(BaseModel):
    delete_local: bool      = False
    evict_high_pct: float   = Field(85, ge=1, le=100)
    evict_low_pct: float    = Field(70, ge=0, le=100)
    evict_min_age_sec: int  = Field(86400, ge=0)
    delete_delay_sec: int   = Field(0, ge=0)


def _ssh_archive_eviction(host: str, port: int, user: str, auth_method: str,
                          password, key_path, req: "EvictionRequest", emit) -> None:
    """Update ONLY the disk-eviction env vars on the storage box and restart — no
    rclone.conf rewrite, so eviction can be toggled without re-entering cloud
    credentials. Eviction still only ever deletes copies verified on the remote."""
    sudo = "" if user == "root" else "sudo -n "
    kvs = [
        f"ARCHIVE_DELETE_LOCAL={'1' if req.delete_local else '0'}",
        f"ARCHIVE_EVICT_HIGH_PCT={req.evict_high_pct}",
        f"ARCHIVE_EVICT_LOW_PCT={req.evict_low_pct}",
        f"ARCHIVE_EVICT_MIN_AGE_SEC={req.evict_min_age_sec}",
        f"ARCHIVE_DELETE_DELAY_SEC={req.delete_delay_sec}",
    ]
    upsert = "\n".join(
        f'{sudo}sed -i "/^{kv.split("=")[0]}=/d" {STORAGE_ENV_FILE}; '
        f'echo {shlex_quote(kv)} | {sudo}tee -a {STORAGE_ENV_FILE} >/dev/null'
        for kv in kvs)
    script = f"set -e\n{upsert}\n{sudo}systemctl restart tt-transcription\necho EVICTION_DONE\n"
    client = _ssh_connect(host, port, user, auth_method, password, key_path, emit)
    if not client:
        return
    try:
        emit(f"==> Updating eviction on {host}:{port} "
             f"(reclaim disk = {'ON' if req.delete_local else 'off'}) ...\n")
        _, so, se = client.exec_command(script, timeout=60)
        if "EVICTION_DONE" in so.read().decode("utf-8", errors="replace"):
            if req.delete_local:
                emit(f"==> Eviction enabled: delete verified-on-cloud copies when disk is above "
                     f"{req.evict_high_pct}%, down to {req.evict_low_pct}%, keeping each file at "
                     f"least {req.evict_min_age_sec//3600}h.\n")
            else:
                emit("==> Eviction disabled — local copies are kept.\n")
            emit("    (Only files already verified on the cloud are ever deleted.)\n")
        else:
            emit("[ERROR] " + se.read().decode("utf-8", errors="replace")[-800:] + "\n")
    finally:
        client.close()


def _run_archive_roundtrip(client, archive_remote: str, emit) -> bool:
    """Prove the archive remote actually works: write a tiny probe file, read it
    back, verify the bytes match, then delete it — all as the service user with
    the box's rclone.conf. Catches bad auth, wrong path, an unwritable remote, or
    an rclone too old for the backend (e.g. Filen needs >= 1.73), at config/test
    time instead of during a silent 3am archive sweep. Returns True on success."""
    su = f"sudo -u {STORAGE_SERVICE_USER} env RCLONE_CONFIG={STORAGE_RCLONE_CONF} "
    script = f"""set +e
DEST={shlex_quote(archive_remote.rstrip('/'))}
MARK=".tt-archive-check-$(date +%s)-$$"
WANT="tt-archive-check $(date -u +%Y-%m-%dT%H:%M:%SZ) $RANDOM"
ERR=$(mktemp)
if echo "$WANT" | {su}rclone rcat "$DEST/$MARK" 2>"$ERR"; then
  GOT=$({su}rclone cat "$DEST/$MARK" 2>>"$ERR")
  {su}rclone deletefile "$DEST/$MARK" >/dev/null 2>&1 || true
  if [ "$WANT" = "$GOT" ]; then echo ARCHIVE_TEST_OK; else echo ARCHIVE_TEST_FAIL_VERIFY; fi
else
  echo ARCHIVE_TEST_FAIL_WRITE
fi
echo "----detail----"; tail -c 1400 "$ERR"; rm -f "$ERR"
"""
    emit(f"==> Archive self-test: writing a probe file to {archive_remote} and reading it back ...\n")
    try:
        _, so, _ = client.exec_command(script, timeout=120)
        out = so.read().decode("utf-8", errors="replace")
    except Exception as e:
        emit(f"    ? self-test could not run: {type(e).__name__}: {e}\n")
        return False
    detail = out.split("----detail----", 1)[-1].strip()
    if "ARCHIVE_TEST_OK" in out:
        emit("    ✓ verified: write + read-back + delete all succeeded — archive works.\n")
        return True
    if "ARCHIVE_TEST_FAIL_WRITE" in out:
        emit("    ✗ FAILED to write to the remote — check credentials, path, or rclone version.\n")
    elif "ARCHIVE_TEST_FAIL_VERIFY" in out:
        emit("    ✗ wrote but the read-back did not match — remote is not storing data correctly.\n")
    else:
        emit("    ? inconclusive result.\n")
    if detail:
        emit("      detail: " + detail[-1200:] + "\n")
    return False


def _ssh_archive_test(host: str, req_ssh: dict, emit) -> None:
    """Standalone re-check: read the configured ARCHIVE_REMOTE off the box and run
    the write/read/delete roundtrip against it. Lets the operator confirm a remote
    still works without reconfiguring it."""
    sudo = "" if req_ssh.get("ssh_user") == "root" else "sudo -n "
    client = _ssh_connect(host, req_ssh["ssh_port"], req_ssh["ssh_user"],
                          req_ssh["auth_method"], req_ssh.get("ssh_password"),
                          req_ssh.get("ssh_key_path"), emit)
    if not client:
        return
    try:
        _, so, _ = client.exec_command(
            f"{sudo}grep '^ARCHIVE_REMOTE=' {STORAGE_ENV_FILE} 2>/dev/null | head -1 | cut -d= -f2-",
            timeout=15)
        remote = so.read().decode("utf-8", errors="replace").strip()
        if not remote:
            emit("[ERROR] No ARCHIVE_REMOTE configured on this server — enable archiving first.\n")
            return
        emit(f"==> Configured archive target: {remote}\n")
        _run_archive_roundtrip(client, remote, emit)
    except Exception as e:
        emit(f"[ERROR] {type(e).__name__}: {e}\n")
    finally:
        client.close()


def _archive_host_for(sid: str) -> Optional[str]:
    from urllib.parse import urlparse
    row = db.execute("SELECT url FROM storage_servers WHERE id=?", (sid,)).fetchone()
    return urlparse(row["url"]).hostname if row else None


class WorkerConfigRequest(BaseModel):
    concurrency: Optional[int]      = Field(None, ge=0, le=16)
    transcribe_enabled: Optional[bool] = None
    model: Optional[str]            = Field(None, max_length=64,
                                            pattern=r"^[A-Za-z0-9._/-]+$")
    vad: Optional[bool]             = None
    beam_size: Optional[int]        = Field(None, ge=1, le=10)
    ssh_port: int               = Field(22, ge=1, le=65535)
    ssh_user: str               = "root"
    auth_method: str            = Field(..., pattern="^(password|key)$")
    ssh_password: Optional[str] = None
    ssh_key_path: Optional[str] = None


def _ssh_worker_config(req: "WorkerConfigRequest", host: str, emit) -> None:
    """SSH to a storage box, upsert WHISPER_CONCURRENCY / WHISPER_TRANSCRIBE in
    its env file, and restart the transcription service so the change applies."""
    sudo = "" if req.ssh_user == "root" else "sudo -n "
    kvs = []
    if req.concurrency is not None:
        kvs.append(f"WHISPER_CONCURRENCY={int(req.concurrency)}")
    if req.transcribe_enabled is not None:
        kvs.append(f"WHISPER_TRANSCRIBE={'1' if req.transcribe_enabled else '0'}")
    if req.model:
        kvs.append(f"WHISPER_MODEL={req.model}")
    if req.vad is not None:
        kvs.append(f"WHISPER_VAD={'1' if req.vad else '0'}")
    if req.beam_size is not None:
        kvs.append(f"WHISPER_BEAM_SIZE={int(req.beam_size)}")
    if not kvs:
        emit("[ERROR] nothing to change\n"); return
    upsert = "\n".join(
        f'{sudo}sed -i "/^{kv.split("=")[0]}=/d" {STORAGE_ENV_FILE}; '
        f'echo {shlex_quote(kv)} | {sudo}tee -a {STORAGE_ENV_FILE} >/dev/null'
        for kv in kvs
    )
    script = f"""set -e
{upsert}
{sudo}systemctl restart tt-transcription
echo "WORKER_CONFIG_DONE"
"""
    client = _ssh_connect(host, req.ssh_port, req.ssh_user,
                          req.auth_method, req.ssh_password, req.ssh_key_path, emit)
    if not client:
        return
    try:
        emit(f"==> Applying to {host}: {', '.join(kvs)}\n")
        _, stdout, stderr = client.exec_command(script, timeout=60)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        if "WORKER_CONFIG_DONE" in out:
            emit("    env updated; tt-transcription restarted.\n")
            emit("\n==> Done. New settings take effect now"
                 + (" (transcription off — storage-only)\n"
                    if req.transcribe_enabled is False else ".\n"))
        else:
            emit(f"[WARN] unexpected result:\n{out}\n{err}\n")
    except Exception as e:
        emit(f"\n[ERROR] {type(e).__name__}: {e}\n")
    finally:
        client.close()


def _ssh_oom_protect(host: str, port: int, user: str, auth_method: str,
                     password, key_path, emit) -> None:
    """SSH to a storage box and make the transcription service the preferred OOM
    victim (OOMScoreAdjust=600), so under memory pressure the kernel kills/restarts
    a transcription rather than the control plane. Idempotent."""
    sudo = "" if user == "root" else "sudo -n "
    unit = "/etc/systemd/system/tt-transcription.service"
    script = f"""set -e
U={unit}
if [ ! -f "$U" ]; then echo "NO_UNIT"; exit 0; fi
{sudo}sed -i '/^OOMScoreAdjust=/d' "$U"
{sudo}sed -i '/^\\[Service\\]/a OOMScoreAdjust=600' "$U"
{sudo}systemctl daemon-reload
{sudo}systemctl restart tt-transcription
echo OOM_PROTECT_DONE
"""
    client = _ssh_connect(host, port, user, auth_method, password, key_path, emit)
    if not client:
        return
    try:
        emit(f"==> Setting OOMScoreAdjust=600 on tt-transcription at {host}\n")
        _, stdout, stderr = client.exec_command(script, timeout=60)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        if "OOM_PROTECT_DONE" in out:
            emit("    done — transcription is now the OOM victim; the control "
                 "plane is protected.\n")
        elif "NO_UNIT" in out:
            emit("[WARN] no tt-transcription unit on this host — nothing to do.\n")
        else:
            emit(f"[WARN] unexpected result:\n{out}\n{err}\n")
    except Exception as e:
        emit(f"\n[ERROR] {type(e).__name__}: {e}\n")
    finally:
        client.close()


@app.post("/api/storage/{sid}/oom-protect", dependencies=[Depends(require_login)])
async def storage_oom_protect(sid: str):
    """One-click: apply OOM protection to a transcription box using its saved
    SSH credentials (no password re-entry)."""
    s = _storage_by_id(sid)
    if not s:
        raise HTTPException(404, "storage server not found")
    host, port = _ssh_target(s)
    creds = _get_creds_mkey(host, port) if host else None

    def work(emit):
        if not (creds and (creds.get("password_enc") or creds.get("key_path"))):
            emit(f"No saved SSH credentials for {host}:{port}. Run one Push update for "
                 "this host in the Deploy tab first, then this becomes one click.\n")
            return
        _ssh_oom_protect(host, port,
                         creds.get("ssh_user") or "root",
                         creds.get("auth_method") or "key",
                         _decrypt_pw(creds.get("password_enc")),
                         creds.get("key_path"), emit)

    return _threaded_stream(work)


@app.post("/api/storage/{sid}/worker-config", dependencies=[Depends(require_login)])
async def storage_worker_config(sid: str, req: WorkerConfigRequest):
    host = _archive_host_for(sid)
    if not host:
        raise HTTPException(404, "storage server not found")
    (req.ssh_port, req.ssh_user, req.auth_method,
     req.ssh_password, req.ssh_key_path) = _resolve_ssh(
        host, req.ssh_port, req.ssh_user, req.auth_method,
        req.ssh_password, req.ssh_key_path)
    return _threaded_stream(lambda emit: _ssh_worker_config(req, host, emit))


@app.post("/api/storage/{sid}/archive-config", dependencies=[Depends(require_login)])
async def storage_archive_config(sid: str, req: ArchiveConfigRequest):
    host = _archive_host_for(sid)
    if not host:
        raise HTTPException(404, "storage server not found")
    (req.ssh_port, req.ssh_user, req.auth_method,
     req.ssh_password, req.ssh_key_path) = _resolve_ssh(
        host, req.ssh_port, req.ssh_user, req.auth_method,
        req.ssh_password, req.ssh_key_path)
    return _threaded_stream(lambda emit: _ssh_archive_config(req, host, emit))


@app.post("/api/storage/{sid}/archive-disable", dependencies=[Depends(require_login)])
async def storage_archive_disable(sid: str, req: dict):
    host = _archive_host_for(sid)
    if not host:
        raise HTTPException(404, "storage server not found")
    return _threaded_stream(lambda emit: _ssh_archive_disable(host, req, emit))


@app.post("/api/storage/{sid}/archive-test", dependencies=[Depends(require_login)])
async def storage_archive_test(sid: str, req: dict):
    """Re-verify a configured archive remote with a live write/read/delete probe."""
    host = _archive_host_for(sid)
    if not host:
        raise HTTPException(404, "storage server not found")
    (req["ssh_port"], req["ssh_user"], req["auth_method"],
     req["ssh_password"], req["ssh_key_path"]) = _resolve_ssh(
        host, req.get("ssh_port", 22), req.get("ssh_user", "root"),
        req.get("auth_method", "key"), req.get("ssh_password"), req.get("ssh_key_path"))
    return _threaded_stream(lambda emit: _ssh_archive_test(host, req, emit))


@app.get("/api/storage-capacity", dependencies=[Depends(require_login)])
async def storage_capacity():
    """Per-storage disk usage + fill-rate projection (time to full), from the
    sampled history. fill_rate_bph > 0 means free space is shrinking."""
    out = []
    now = time.time()
    for s in _storage_all():
        async with _db_lock:
            rows = db.execute(
                "SELECT ts, free_bytes, total_bytes FROM disk_samples "
                "WHERE storage_id=? ORDER BY ts DESC LIMIT 4032", (s["id"],)).fetchall()
        e = {"id": s["id"], "label": s["label"] or s["url"],
             "healthy": bool(s["last_health_ok"]), "free": None, "total": None,
             "used_pct": None, "fill_rate_bph": None, "hours_to_full": None,
             "samples": len(rows)}
        if rows:
            newest = rows[0]
            e["free"], e["total"] = newest["free_bytes"], newest["total_bytes"]
            if newest["total_bytes"]:
                e["used_pct"] = round(100 * (1 - newest["free_bytes"] / newest["total_bytes"]), 1)
            # fill rate from the oldest sample within the last 12h (or the oldest we have)
            window_start = now - 12 * 3600
            ref = next((r for r in rows if r["ts"] <= window_start), rows[-1])
            dt_h = (newest["ts"] - ref["ts"]) / 3600.0
            if dt_h > 0.05:
                rate = (ref["free_bytes"] - newest["free_bytes"]) / dt_h   # bytes/hr lost
                e["fill_rate_bph"] = round(rate)
                if rate > 0 and newest["free_bytes"] is not None:
                    e["hours_to_full"] = round(newest["free_bytes"] / rate, 1)
        out.append(e)
    return {"servers": out}


@app.get("/api/archive-statuses", dependencies=[Depends(require_login)])
async def archive_statuses():
    """{filename: {archived, on_cloud, local, username, storage_sid, remote}} merged
    across storage servers. Includes evicted (cloud-only) recordings."""
    out: dict = {}
    for s in _storage_healthy():
        code, body = await _tw_call(s, "GET", "/files/archive-status")
        if code != 200 or not isinstance(body, dict):
            continue
        for fn, info in body.items():
            if fn in out:
                continue
            out[fn] = {"archived": True, "on_cloud": True,
                       "local": bool(info.get("local", True)),
                       "username": info.get("username", ""),
                       "remote": info.get("remote"),
                       "storage_sid": s["id"], "storage_label": s["label"]}
    return out


@app.get("/api/files/cloud-download")
async def files_cloud_download(username: str, filename: str,
                               storage_sid: Optional[str] = None,
                               session: Optional[str] = Cookie(default=None)):
    """Stream a recording back from the cloud archive (for evicted/cold files).
    Proxies the storage worker's `rclone cat`. Tries the named server first, then
    any other archive-configured server."""
    if not _verify_session(session):
        raise HTTPException(401, "login required")
    servers = _storage_healthy()
    ordered = ([s for s in servers if s["id"] == storage_sid] +
               [s for s in servers if s["id"] != storage_sid])
    for s in ordered:
        hdrs = {}
        if s.get("token"):
            hdrs["Authorization"] = f"Bearer {s['token']}"
        url = f"{s['url'].rstrip('/')}/archive/download"
        client = httpx.AsyncClient(timeout=None)
        try:
            req = client.build_request("GET", url, headers=hdrs,
                                       params={"username": username, "filename": filename})
            r = await client.send(req, stream=True)
            if r.status_code != 200:
                await r.aclose(); await client.aclose(); continue

            async def _stream(r=r, client=client):
                try:
                    async for chunk in r.aiter_bytes(65536):
                        yield chunk
                finally:
                    await r.aclose(); await client.aclose()

            return StreamingResponse(
                _stream(), media_type="video/mp4",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'})
        except httpx.RequestError:
            await client.aclose(); continue
    raise HTTPException(404, "not available from any cloud archive")


@app.post("/api/storage/{sid}/evict-now", dependencies=[Depends(require_login)])
async def storage_evict_now(sid: str):
    """Trigger the disk-eviction sweep immediately on a storage server."""
    s = _storage_by_id(sid)
    if not s:
        raise HTTPException(404, "storage server not found")
    code, body = await _tw_call(s, "POST", "/archive/evict-now")
    if code != 200:
        raise HTTPException(502, f"worker returned {code}: {body}")
    return body


@app.post("/api/storage/{sid}/archive-eviction", dependencies=[Depends(require_login)])
async def storage_archive_eviction(sid: str, req: EvictionRequest):
    """One-click: enable/adjust disk eviction on an already-configured archive,
    using saved SSH credentials so cloud credentials never need re-entry."""
    s = _storage_by_id(sid)
    if not s:
        raise HTTPException(404, "storage server not found")
    host, port = _ssh_target(s)
    creds = _get_creds_mkey(host, port) if host else None
    if not (creds and (creds.get("password_enc") or creds.get("key_path"))):
        raise HTTPException(400, "No saved SSH credentials for this server. Configure the "
                                 "archive once (or run a Push update) so the credentials are "
                                 "saved, then eviction is a one-click toggle.")
    user = creds.get("ssh_user") or "root"
    auth = creds.get("auth_method") or "key"
    pw = _decrypt_pw(creds.get("password_enc"))
    keyp = creds.get("key_path")
    return _threaded_stream(
        lambda emit: _ssh_archive_eviction(host, port, user, auth, pw, keyp, req, emit))


# Compatibility shim: the Deploy tab's auto-register still calls this.
# It now adds the server to the storage_servers registry.
@app.post("/api/config/transcript-worker", dependencies=[Depends(require_login)])
async def config_transcript_worker(url: str, token: str):
    """Back-compat: add a storage server to the registry (used by Deploy auto-register)."""
    return await add_storage(url=url, token=token)


# ---------------------------------------------------------------------------
# Upload worker endpoints

@app.get("/api/transfer-statuses", dependencies=[Depends(require_login)])
async def transfer_statuses():
    """Return {filename: status} for all tracked transfers.
    Missing key = file not yet queued ('none')."""
    async with _db_lock:
        rows = db.execute("SELECT filename, status FROM transfers").fetchall()
    return {r["filename"]: r["status"] for r in rows}


@app.get("/api/transfers/summary", dependencies=[Depends(require_login)])
async def transfers_summary():
    """Transfer status counts (for the Transfers controls in the UI)."""
    async with _db_lock:
        rows = db.execute(
            "SELECT status, COUNT(*) AS c FROM transfers GROUP BY status").fetchall()
    return {r["status"]: r["c"] for r in rows}


@app.post("/api/transfers/retry-failed", dependencies=[Depends(require_login)])
async def transfers_retry_failed():
    """Reset all failed transfers back to pending (attempts cleared) so the next
    cycle re-attempts them — e.g. after updating an out-of-date storage worker.
    Files no longer on the recorder will simply fail again and can be cleared."""
    async with _db_lock:
        n = db.execute(
            "UPDATE transfers SET status='pending', attempts=0, error=NULL "
            "WHERE status='failed'").rowcount
        db.commit()
    asyncio.create_task(_upload_cycle(), name="upload-after-retry")
    return {"retried": n}


@app.post("/api/transfers/clear-failed", dependencies=[Depends(require_login)])
async def transfers_clear_failed():
    """Delete failed transfer records (e.g. for recordings that no longer exist on
    the recorder). They'll be re-discovered and re-queued if the file is still there."""
    async with _db_lock:
        n = db.execute("DELETE FROM transfers WHERE status='failed'").rowcount
        db.commit()
    return {"cleared": n}


@app.post("/api/transfer/queue", dependencies=[Depends(require_login)])
async def queue_transfer(backend_pk: str, path: str):
    """Manually queue a specific file for upload to the storage server."""
    async with _db_lock:
        backend = db.execute(
            "SELECT id, backend_id FROM backends WHERE id=?", (backend_pk,)
        ).fetchone()
    if not backend:
        raise HTTPException(404, "backend not found")

    fname = Path(path).name
    username = Path(path).parent.name

    async with _db_lock:
        existing = db.execute(
            "SELECT status FROM transfers WHERE filename=? AND backend_pk=?",
            (fname, backend_pk),
        ).fetchone()
        if existing and existing["status"] in ("pending", "transferring", "done"):
            return {"queued": False, "status": existing["status"]}

        db.execute(
            "INSERT OR REPLACE INTO transfers "
            "(id,backend_pk,backend_label,username,filename,src_path,status) "
            "VALUES (?,?,?,?,?,?,'pending')",
            (str(uuid.uuid4()), backend_pk, backend["backend_id"],
             username, fname, path),
        )
        db.commit()

    # Kick off immediately rather than waiting for the next interval
    asyncio.create_task(_upload_cycle(), name="upload-immediate")
    return {"queued": True, "filename": fname}


@app.post("/api/transfer/run-now", dependencies=[Depends(require_login)])
async def run_upload_now():
    """Trigger an immediate upload cycle without waiting for the interval."""
    asyncio.create_task(_upload_cycle(), name="upload-immediate")
    return {"ok": True}


@app.get("/api/transfer-progress", dependencies=[Depends(require_login)])
async def transfer_progress():
    """Return live byte-count for every active transfer.
    Returns {filename: {bytes_done, size_bytes, pct, storage_label}}"""
    async with _db_lock:
        rows = db.execute(
            "SELECT filename, size_bytes, storage_label FROM transfers "
            "WHERE status='transferring'"
        ).fetchall()
    out = {}
    for r in rows:
        done = _transfer_progress.get(r["filename"], 0)
        total = r["size_bytes"] or 0
        out[r["filename"]] = {
            "bytes_done": done,
            "size_bytes": total,
            "pct": round(done / total * 100) if total else 0,
            "storage_label": r["storage_label"],
        }
    return out


@app.get("/api/transcript-index/status", dependencies=[Depends(require_login)])
async def transcript_index_status():
    """Coverage of the local transcript search index."""
    if _catalog is None:
        return {"enabled": False, "indexed": 0}
    indexed = await asyncio.to_thread(_catalog.transcript_index_count)
    return {"enabled": True, "indexed": indexed}


@app.post("/api/transcript-index/run-now", dependencies=[Depends(require_login)])
async def transcript_index_run_now():
    """Index a batch of finished transcripts immediately."""
    if _catalog is None:
        raise HTTPException(404, "catalog is off (set CATALOG_ENABLED=1)")
    return await _transcript_index_cycle()


@app.post("/api/transcript-index/rebuild", dependencies=[Depends(require_login)])
async def transcript_index_rebuild():
    """Drop the index and re-index a first batch (rest follows in the background)."""
    if _catalog is None:
        raise HTTPException(404, "catalog is off (set CATALOG_ENABLED=1)")
    await asyncio.to_thread(_catalog.drop_transcript_index)
    return await _transcript_index_cycle()


@app.get("/api/transcript-search", dependencies=[Depends(require_login)])
async def transcript_search(q: str):
    """Full-text search across transcripts.

    Prefers the local FTS index (one fast query, works even when a storage box is
    offline). Falls back to live fan-out grep across storage servers when the
    catalog is off or its index is still empty."""
    if not q.strip():
        return []
    # Fast path: the catalog's full-text index.
    if _catalog is not None:
        try:
            count = await asyncio.to_thread(_catalog.transcript_index_count)
            if count:
                hits = await asyncio.to_thread(_catalog.search_transcripts, q, 200)
                return hits
        except Exception:
            log.exception("FTS transcript search failed — falling back to live")
    servers = _storage_healthy()
    if not servers:
        return []

    # Query all servers in parallel
    tasks = [_tw_call(s, "GET", "/transcripts/search", params={"q": q}) for s in servers]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    seen: set[str] = set()
    results = []
    for s, resp in zip(servers, responses):
        if isinstance(resp, Exception):
            continue
        code, body = resp
        if code != 200 or not isinstance(body, list):
            continue
        for item in body:
            fname = item.get("filename", "")
            if fname in seen:
                continue
            seen.add(fname)
            results.append({
                "filename":      fname + ".mp4",
                "username":      item.get("username", ""),
                "snippet":       item.get("snippet", ""),
                "mtime":         item.get("mtime"),
                "storage_label": s["label"],
            })

    # Merge sort by mtime desc
    results.sort(key=lambda x: x.get("mtime") or 0, reverse=True)
    return results


# ---------------------------------------------------------------------------
# Chat logs (TikTok live chat) — list / view / search across storage servers

@app.get("/api/chat/files", dependencies=[Depends(require_login)])
async def chat_files():
    """List all captured chat logs across every storage server."""
    servers = _storage_healthy()
    if not servers:
        return []
    tasks = [_tw_call(s, "GET", "/chat") for s in servers]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    seen: set[str] = set()
    out = []
    for s, resp in zip(servers, responses):
        if isinstance(resp, Exception):
            continue
        code, body = resp
        if code != 200 or not isinstance(body, list):
            continue
        for item in body:
            fn = item.get("filename", "")
            if fn in seen:
                continue
            seen.add(fn)
            item["storage_id"] = s["id"]
            item["storage_label"] = s["label"]
            out.append(item)
    # Attach the fuzzy-matched recording filename to each log (chat -> recording).
    if _catalog is not None and out:
        try:
            rec_to_chat = await asyncio.to_thread(_catalog.chat_match_map)
            chat_to_rec = {c: r for r, c in rec_to_chat.items()}
            for item in out:
                item["recording_filename"] = chat_to_rec.get(item.get("filename"))
        except Exception:
            pass
    out.sort(key=lambda x: x.get("mtime") or 0, reverse=True)
    return out


@app.get("/api/chat/view", dependencies=[Depends(require_login)])
async def chat_view(filename: str, q: str = None, type: str = None):
    """Return parsed events for one chat log from whichever server holds it."""
    params = {"filename": filename}
    if q:
        params["q"] = q
    if type:
        params["type"] = type
    for s in _storage_healthy():
        code, body = await _tw_call(s, "GET", "/chat/view", params=params)
        if code == 200:
            return body
    raise HTTPException(404, "chat log not found")


@app.get("/api/chat-index/status", dependencies=[Depends(require_login)])
async def chat_index_status():
    if _catalog is None:
        return {"enabled": False, "indexed": 0}
    return {"enabled": True,
            "indexed": await asyncio.to_thread(_catalog.chat_index_count)}


@app.get("/api/chat-matches", dependencies=[Depends(require_login)])
async def chat_matches():
    """{recording_filename: chat_filename} for fuzzy-matched logs — lets the Files
    tab show a 💬 chat link per recording."""
    if _catalog is None:
        return {}
    return await asyncio.to_thread(_catalog.chat_match_map)


@app.get("/api/chat/search", dependencies=[Depends(require_login)])
async def chat_search(q: str):
    """Search chat logs. Prefers the local chat FTS index (fast, offline-tolerant,
    and carries the fuzzy-matched recording); falls back to live fan-out grep."""
    if not q.strip():
        return []
    if _catalog is not None:
        try:
            if await asyncio.to_thread(_catalog.chat_index_count):
                return await asyncio.to_thread(_catalog.search_chat, q, 200)
        except Exception:
            log.exception("chat FTS search failed — falling back to live")
    servers = _storage_healthy()
    if not servers:
        return []
    tasks = [_tw_call(s, "GET", "/chat/search", params={"q": q}) for s in servers]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    seen: set[str] = set()
    results = []
    for s, resp in zip(servers, responses):
        if isinstance(resp, Exception):
            continue
        code, body = resp
        if code != 200 or not isinstance(body, list):
            continue
        for item in body:
            fn = item.get("filename", "")
            if fn in seen:
                continue
            seen.add(fn)
            item["storage_label"] = s["label"]
            results.append(item)
    results.sort(key=lambda x: x.get("mtime") or 0, reverse=True)
    return results


@app.get("/api/chat-diag", dependencies=[Depends(require_login)])
async def chat_diag():
    """Fan out to every backend's /diag/chat so the dashboard can show why chat
    capture might not be working (library missing, recorder absent, sign key)."""
    async with _db_lock:
        backends = db.execute(
            "SELECT id, backend_id, url, auth_token, last_health_ok FROM backends"
        ).fetchall()
    out = []
    for b in backends:
        entry = {"backend_pk": b["id"], "backend_id": b["backend_id"],
                 "reachable": False}
        code, body = await _call(b["url"], b["auth_token"], "GET", "/diag/chat")
        if code == 200 and isinstance(body, dict):
            entry["reachable"] = True
            entry.update(body)
        out.append(entry)
    return out


class ChatDepsInstall(BaseModel):
    ssh_port: int = Field(22, ge=1, le=65535)
    ssh_user: str = "root"
    auth_method: str = Field(..., pattern="^(password|key)$")
    ssh_password: Optional[str] = None
    ssh_key_path: Optional[str] = None


@app.post("/api/backends/{pk}/install-chat-deps", dependencies=[Depends(require_login)])
async def install_chat_deps(pk: str, req: ChatDepsInstall):
    """SSH to a backend and install TikTokLive into its venv, then restart the
    service. This is the usual fix when chat was added via push-update (which
    ships code but does not reinstall dependencies)."""
    from urllib.parse import urlparse
    async with _db_lock:
        row = db.execute("SELECT url FROM backends WHERE id=?", (pk,)).fetchone()
    if not row:
        raise HTTPException(404, "backend not found")
    host = urlparse(row["url"]).hostname
    (req.ssh_port, req.ssh_user, req.auth_method,
     req.ssh_password, req.ssh_key_path) = _resolve_ssh(
        host, req.ssh_port, req.ssh_user, req.auth_method,
        req.ssh_password, req.ssh_key_path)
    # Run uv as user 'tt' from a directory tt can read (root's $HOME is 0700, which
    # is what broke config discovery). Mirror provisioning: cd into the install dir.
    as_tt = "sudo -u tt " if req.ssh_user == "root" else "sudo -n -u tt "
    as_root = "" if req.ssh_user == "root" else "sudo -n "
    script = f"""set -e
cd /opt/tt-backend
CHAT_VENV=/opt/tt-backend/.venv-chat
UV=""
for cand in /usr/local/bin/uv "$HOME/.local/bin/uv" /root/.local/bin/uv /opt/tt-backend/.local/bin/uv uv; do
  if [ -x "$cand" ] || command -v "$cand" >/dev/null 2>&1; then UV="$cand"; break; fi
done
if [ -z "$UV" ]; then echo "[ERROR] uv not found on backend"; exit 1; fi
echo "using uv: $UV"
# Dedicated venv for chat so TikTokLive's deps never collide with the recorder's.
# --clear makes this idempotent: recreates cleanly whether or not one exists.
{as_tt}env HOME=/opt/tt-backend XDG_CONFIG_HOME=/opt/tt-backend/.config "$UV" --no-config venv --clear "$CHAT_VENV" --python python3
echo "installing a current TikTokLive (>=6) ..."
# --prerelease=allow lets the required betterproto beta resolve, but we must keep
# httpx on the stable 0.x line — TikTokLive uses the 0.x API (httpx 1.0 dev breaks it).
{as_tt}env HOME=/opt/tt-backend XDG_CONFIG_HOME=/opt/tt-backend/.config "$UV" --no-config pip install --python "$CHAT_VENV/bin/python3" --prerelease=allow -U 'TikTokLive>=6,<7' 'httpx<1'
echo "verifying import ..."
{as_tt}"$CHAT_VENV/bin/python3" -c "import TikTokLive, importlib.metadata as m; print('TikTokLive', m.version('TikTokLive'))"
{as_root}systemctl restart tt-backend
echo CHAT_DEPS_DONE
"""
    def work(emit):
        client = _ssh_connect(host, req.ssh_port, req.ssh_user, req.auth_method,
                              req.ssh_password, req.ssh_key_path, emit)
        if not client:
            return
        try:
            emit(f"==> Installing TikTokLive on {host} (this can take a minute) …\n")
            _, so, se = client.exec_command(script, timeout=300)
            out = so.read().decode("utf-8", "replace")
            err = se.read().decode("utf-8", "replace")
            if out.strip():
                emit(out.strip() + "\n")
            if "CHAT_DEPS_DONE" in out:
                emit("==> Installed and tt-backend restarted.\n")
                emit("==> Re-run the chat health check to confirm.\n")
            else:
                emit("[ERROR] install did not complete.\n")
                if err.strip():
                    emit("---- stderr ----\n" + err[-2000:] + "\n")
        finally:
            client.close()

    return _threaded_stream(work)


# ---------------------------------------------------------------------------
# Move recordings/transcripts/chat between storage servers.
# Two-phase and safe: copy + verify on destination, then keep the source copy
# until the user explicitly confirms deletion.

async def _do_move(move_id: str) -> None:
    """Copy one file from src storage → dst storage and verify it. Leaves the
    source intact (status 'copied'); deletion happens later on confirmation."""
    row = db.execute("SELECT * FROM moves WHERE id=?", (move_id,)).fetchone()
    if not row or row["status"] not in ("pending", "error"):
        return
    src = _storage_by_id(row["src_pk"])
    dst = _storage_by_id(row["dst_pk"])
    if not src or not dst:
        db.execute("UPDATE moves SET status='error', error=? WHERE id=?",
                   ("source or destination storage missing", move_id)); db.commit()
        return
    db.execute("UPDATE moves SET status='copying', attempts=attempts+1 WHERE id=?",
               (move_id,)); db.commit()
    try:
        src_url = f"{src['url'].rstrip('/')}/files/raw"
        src_hdrs = {"Authorization": f"Bearer {src['token']}"} if src.get("token") else {}
        dst_url = f"{dst['url'].rstrip('/')}/files/{row['username']}/{row['filename']}"
        dst_hdrs = {"Content-Type": "application/octet-stream"}
        if dst.get("token"):
            dst_hdrs["Authorization"] = f"Bearer {dst['token']}"

        got, written = await _relay_file(
            src_url, src_hdrs, {"path": row["src_path"]},
            dst_url, dst_hdrs, row["size_bytes"], tmp_tag=move_id)

        db.execute("UPDATE moves SET status='copied', copied_at=?, error=NULL WHERE id=?",
                   (time.time(), move_id)); db.commit()
        log.info("move: copied %s %s→%s (verified)", row["filename"],
                 row["src_label"], row["dst_label"])
    except Exception as e:
        db.execute("UPDATE moves SET status='error', error=? WHERE id=?",
                   (str(e)[:300], move_id)); db.commit()
        log.error("move failed for %s: %s", row["filename"], e)


async def _move_cycle():
    """Process queued moves a few at a time."""
    while True:
        try:
            pending = db.execute(
                "SELECT id FROM moves WHERE status IN ('pending','error') "
                "AND attempts < 4 ORDER BY created_at LIMIT 3").fetchall()
            for r in pending:
                await _do_move(r["id"])
        except Exception:
            log.exception("move cycle error")
        await asyncio.sleep(5)


class MoveRequest(BaseModel):
    src_storage_id: str
    dst_storage_id: str
    scope: str = Field(..., pattern="^(creator|server|files)$")
    username: Optional[str] = None         # for scope=creator
    files: Optional[list[str]] = None       # for scope=files: list of paths on source


@app.post("/api/moves", dependencies=[Depends(require_login)])
async def create_moves(req: MoveRequest):
    src = _storage_by_id(req.src_storage_id)
    dst = _storage_by_id(req.dst_storage_id)
    if not src or not dst:
        raise HTTPException(404, "storage server not found")
    if src["id"] == dst["id"]:
        raise HTTPException(400, "source and destination are the same server")
    # Enumerate files on the source for this scope.
    params = {}
    if req.scope == "creator":
        if not req.username:
            raise HTTPException(400, "username required for creator scope")
        params["username"] = req.username
    code, listing = await _tw_call(src, "GET", "/files/all", params=params)
    if code != 200 or not isinstance(listing, list):
        raise HTTPException(502, "could not list source files")
    wanted = listing
    if req.scope == "files":
        sel = set(req.files or [])
        wanted = [f for f in listing if f["path"] in sel]
        # also pull a recording's transcript sidecar along with it
        stems = {Path(f["filename"]).stem for f in wanted if f["kind"] == "recording"}
        for f in listing:
            if f["kind"] == "transcript" and Path(f["filename"]).stem in stems:
                if f not in wanted:
                    wanted.append(f)
    if not wanted:
        return {"created": 0, "batch": None}
    batch = str(uuid.uuid4())
    now = time.time()
    for f in wanted:
        db.execute(
            "INSERT INTO moves (id,src_pk,src_label,dst_pk,dst_label,username,filename,"
            "src_path,kind,size_bytes,status,created_at,batch) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,'pending',?,?)",
            (str(uuid.uuid4()), src["id"], src["label"], dst["id"], dst["label"],
             f.get("username"), f["filename"], f["path"], f.get("kind"),
             f.get("size_bytes"), now, batch))
    db.commit()
    return {"created": len(wanted), "batch": batch}


@app.get("/api/moves", dependencies=[Depends(require_login)])
async def list_moves():
    rows = db.execute(
        "SELECT * FROM moves ORDER BY created_at DESC LIMIT 500").fetchall()
    return [dict(r) for r in rows]


@app.get("/api/storage/{sid}/files", dependencies=[Depends(require_login)])
async def storage_files(sid: str, username: str = None):
    """List individual files on a storage server (optionally one creator) so the
    UI can offer per-file moves."""
    s = _storage_by_id(sid)
    if not s:
        raise HTTPException(404, "storage server not found")
    params = {"username": username} if username else {}
    code, body = await _tw_call(s, "GET", "/files/all", params=params)
    if code != 200 or not isinstance(body, list):
        raise HTTPException(502, "could not list files")
    return body


@app.post("/api/moves/confirm-delete", dependencies=[Depends(require_login)])
async def confirm_delete_moves(body: dict):
    """Delete the SOURCE copies for moves that are verified ('copied').
    Accepts {batch: id} or {ids: [...]}. Groups deletes per source server."""
    ids = body.get("ids")
    batch = body.get("batch")
    if batch:
        rows = db.execute("SELECT * FROM moves WHERE batch=? AND status='copied'",
                          (batch,)).fetchall()
    elif ids:
        q = ",".join("?" * len(ids))
        rows = db.execute(f"SELECT * FROM moves WHERE id IN ({q}) AND status='copied'",
                          ids).fetchall()
    else:
        raise HTTPException(400, "provide batch or ids")
    by_src: dict[str, list] = {}
    for r in rows:
        by_src.setdefault(r["src_pk"], []).append(r)
    deleted = 0
    for src_pk, items in by_src.items():
        src = _storage_by_id(src_pk)
        if not src:
            continue
        code, resp = await _tw_call(src, "POST", "/files/delete",
                                    json={"paths": [i["src_path"] for i in items]})
        ok_paths = set((resp or {}).get("deleted", [])) if code == 200 else set()
        for i in items:
            if i["src_path"] in ok_paths:
                db.execute("UPDATE moves SET status='done', deleted_at=? WHERE id=?",
                           (time.time(), i["id"])); deleted += 1
            else:
                db.execute("UPDATE moves SET error=? WHERE id=?",
                           ("source delete failed", i["id"]))
    db.commit()
    return {"deleted": deleted}


@app.post("/api/moves/clear", dependencies=[Depends(require_login)])
async def clear_moves(body: dict):
    """Remove finished/cancelled move rows from the list. {what:'done'|'all'}"""
    what = (body or {}).get("what", "done")
    if what == "all":
        db.execute("DELETE FROM moves WHERE status IN ('done','error','copied')")
    else:
        db.execute("DELETE FROM moves WHERE status='done'")
    db.commit()
    return {"ok": True}


@app.post("/api/moves/retry", dependencies=[Depends(require_login)])
async def retry_moves(body: dict):
    """Reset errored moves back to pending (clears the attempt counter) so the
    mover picks them up again — e.g. after updating the destination server.
    Accepts {batch: id} or retries all errored if omitted."""
    batch = (body or {}).get("batch")
    if batch:
        db.execute("UPDATE moves SET status='pending', attempts=0, error=NULL "
                   "WHERE status='error' AND batch=?", (batch,))
    else:
        db.execute("UPDATE moves SET status='pending', attempts=0, error=NULL "
                   "WHERE status='error'")
    db.commit()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Events

@app.post("/events")
async def receive_event(req: Request):
    # Opt-in auth: when an ingest token is configured, recorders must present it
    # (they already send Authorization: Bearer <CONTROL_PLANE_TOKEN>). Unset =>
    # open, so existing fleets keep working until the token is rolled out.
    if settings.ingest_token and \
       req.headers.get("authorization", "") != f"Bearer {settings.ingest_token}":
        raise HTTPException(401, "invalid or missing ingest token")
    body = await req.json()
    eid = body.get("event_id") or str(uuid.uuid4())
    async with _db_lock:
        try:
            db.execute(
                "INSERT OR IGNORE INTO events "
                "(event_id,backend_id,kind,username,payload,received_at) "
                "VALUES (?,?,?,?,?,?)",
                (eid, body.get("backend_id"),
                 body.get("event", {}).get("kind"),
                 body.get("event", {}).get("username"),
                 json.dumps(body), time.time()),
            )
            db.commit()
        except Exception:
            log.exception("event store failed")
    # Phase 1 write-through: advance the shadow catalog from this event. Best-effort
    # and isolated — a catalog fault must never affect event ingestion.
    if _catalog is not None:
        try:
            from catalog import ingest as _cat_ingest
            _cat_ingest.on_event(_catalog, body)
        except Exception:
            log.debug("catalog ingest failed (ignored)", exc_info=True)
    return {"ok": True, "event_id": eid}


# ---------------------------------------------------------------------------
# Strangler catalog (read-only visibility into shadow mode)

@app.get("/api/catalog/stats", dependencies=[Depends(require_login)])
async def catalog_stats():
    if _catalog is None:
        raise HTTPException(404, "catalog shadow mode is off (set CATALOG_ENABLED=1)")
    return _catalog.stats()


@app.get("/api/catalog/drift", dependencies=[Depends(require_login)])
async def catalog_drift():
    """The most recent parity report (catalog vs live inventory), if any, plus the
    last shadow-vs-live transcribe comparison."""
    if _catalog is None:
        raise HTTPException(404, "catalog shadow mode is off (set CATALOG_ENABLED=1)")
    rep = _catalog.meta_get("last_parity_report")
    cmp = _catalog.meta_get("last_compare")
    return {
        "parity": json.loads(rep) if rep else None,
        "transcribe_compare": json.loads(cmp) if cmp else None,
    }


@app.get("/api/catalog/readiness", dependencies=[Depends(require_login)])
async def catalog_readiness():
    """Go/no-go for the execute phases (transcription cutover, eviction), from the
    latest shadow drift + comparison snapshots."""
    if _catalog is None:
        raise HTTPException(404, "catalog shadow mode is off (set CATALOG_ENABLED=1)")
    from catalog import readiness as _rd
    return _rd.assess_readiness(_catalog)


@app.get("/api/flv-status", dependencies=[Depends(require_login)])
async def flv_status_all():
    """Aggregate the orphaned-_flv reaper status from every recorder backend.
    Recorders that haven't been push-updated yet return 404 → flagged
    'not_deployed' so the UI can prompt a push."""
    async with _db_lock:
        backends = db.execute(
            "SELECT id, backend_id, url, auth_token FROM backends"
        ).fetchall()
    out = []
    for b in backends:
        entry = {"backend_pk": b["id"], "backend_id": b["backend_id"], "reachable": False}
        code, body = await _call(b["url"], b["auth_token"], "GET", "/flv/status")
        if code == 200 and isinstance(body, dict):
            entry["reachable"] = True
            entry.update(body)
        elif code == 404:
            entry["reachable"] = True
            entry["not_deployed"] = True
        out.append(entry)
    return out


@app.get("/api/disk-breakdown", dependencies=[Depends(require_login)])
async def disk_breakdown_all():
    """Per-recorder disk usage by category (final vs redundant-flv vs temp vs logs
    vs outside-recordings), so a surprising 'disk full' is explainable + reclaimable."""
    async with _db_lock:
        backends = db.execute(
            "SELECT id, backend_id, url, auth_token FROM backends WHERE last_health_ok=1"
        ).fetchall()
    out = []
    for b in backends:
        entry = {"backend_pk": b["id"], "backend_id": b["backend_id"], "reachable": False}
        code, body = await _call(b["url"], b["auth_token"], "GET", "/disk/breakdown")
        if code == 200 and isinstance(body, dict):
            entry["reachable"] = True
            entry.update(body)
        elif code == 404:
            entry["reachable"] = True
            entry["not_deployed"] = True
        out.append(entry)
    return out


@app.post("/api/backends/{pk}/reap-now", dependencies=[Depends(require_login)])
async def backend_reap_now(pk: str):
    """Trigger the orphan-_flv + temp reaper on a backend immediately."""
    async with _db_lock:
        row = db.execute("SELECT url, auth_token FROM backends WHERE id=?", (pk,)).fetchone()
    if not row:
        raise HTTPException(404, "backend not found")
    code, body = await _call(row["url"], row["auth_token"], "POST", "/flv/reap-now")
    if code != 200:
        raise HTTPException(502 if code < 0 else code, f"backend error {code}: {body}")
    return body


# ---------------------------------------------------------------------------
# Deploy endpoint

@app.post("/api/deploy", dependencies=[Depends(require_login)])
async def deploy(req: DeployRequest):
    (req.ssh_port, req.ssh_user, req.auth_method,
     req.ssh_password, req.ssh_key_path) = _resolve_ssh(
        req.host, req.ssh_port, req.ssh_user, req.auth_method,
        req.ssh_password, req.ssh_key_path)
    return _threaded_stream(lambda emit: _ssh_deploy(req, emit))


@app.post("/api/push-update", dependencies=[Depends(require_login)])
async def push_update(req: PushUpdateRequest):
    """SFTP updated code to a server and restart its service(s).
    Resolves the SSH host from the backends or storage_servers table
    depending on update_type. Streams output like /api/deploy."""
    from urllib.parse import urlparse
    host = None
    async with _db_lock:
        if req.update_type in ("recorder", "colocated"):
            row = db.execute("SELECT url FROM backends WHERE id=?", (req.target_id,)).fetchone()
            if row:
                host = urlparse(row["url"]).hostname
        else:  # storage
            row = db.execute("SELECT url FROM storage_servers WHERE id=?", (req.target_id,)).fetchone()
            if row:
                host = urlparse(row["url"]).hostname
    if not host:
        raise HTTPException(404, "target not found")

    (req.ssh_port, req.ssh_user, req.auth_method,
     req.ssh_password, req.ssh_key_path) = _resolve_ssh(
        host, req.ssh_port, req.ssh_user, req.auth_method,
        req.ssh_password, req.ssh_key_path)

    return _threaded_stream(lambda emit: _ssh_push_update(req, host, emit))


@app.get("/api/backends/{pk}/cookies", dependencies=[Depends(require_login)])
async def get_backend_cookies(pk: str):
    """Proxy GET /cookies to the backend — returns the current cookies.json."""
    async with _db_lock:
        row = db.execute(
            "SELECT url, auth_token FROM backends WHERE id=?", (pk,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "backend not found")
    code, body = await _call(row["url"], row["auth_token"], "GET", "/cookies")
    if code != 200:
        raise HTTPException(502, f"backend error {code}")
    return body


@app.post("/api/backends/{pk}/cookies", dependencies=[Depends(require_login)])
async def set_backend_cookies(pk: str, body: dict):
    """Proxy POST /cookies to the backend — writes a new cookies.json."""
    async with _db_lock:
        row = db.execute(
            "SELECT url, auth_token FROM backends WHERE id=?", (pk,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "backend not found")
    code, resp = await _call(
        row["url"], row["auth_token"], "POST", "/cookies", body
    )
    if code != 200:
        raise HTTPException(502, f"backend error {code}: {resp}")
    return resp


# ---------------------------------------------------------------------------
# Pages

@app.get("/", response_class=HTMLResponse)
async def root(session: Optional[str] = Cookie(default=None)):
    if not _verify_session(session):
        return RedirectResponse("/login", status_code=307)
    # Never cache the dashboard — otherwise browsers serve a stale UI after updates.
    return HTMLResponse(DASHBOARD_HTML.replace("__BUILD__", BUILD), headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
    })

@app.get("/login", response_class=HTMLResponse)
async def login_page(session: Optional[str] = Cookie(default=None)):
    if _verify_session(session):
        return RedirectResponse("/", status_code=307)
    return HTMLResponse(LOGIN_HTML)


@app.get("/app.js")
async def app_js():
    """Serve the dashboard's JavaScript (split out of dashboard.html so it's a
    real, lintable file). Cache-busted per build via the ?v=BUILD query string in
    the <script> tag, so a long cache is safe; revalidate to be doubly sure."""
    return Response(APP_JS, media_type="application/javascript; charset=utf-8",
                    headers={"Cache-Control": "no-cache"})


# ---------------------------------------------------------------------------
# HTML

LOGIN_HTML = _load_template("login.html")

DASHBOARD_HTML = _load_template("dashboard.html")
APP_JS = _load_template("app.js")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if not settings.password:
        print("ERROR: set CONTROL_PLANE_PASSWORD before running")
        raise SystemExit(1)
    print(f"Open http://localhost:{settings.port}")
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="warning")
