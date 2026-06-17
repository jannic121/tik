"""
Recorder backend HTTP API.

Wraps watcher.WatcherProcess in a FastAPI app the control plane can call.

Run:
    uvicorn app:app --host 0.0.0.0 --port 8000

Env vars:
    BACKEND_ID            unique label for this VPS, e.g. "us-east-1"
    REGION                free-form region tag, e.g. "us-east"
    AUTH_TOKEN            shared bearer secret; clients must send `Authorization: Bearer <token>`
    REPO_PATH             absolute path to cloned tiktok-live-recorder repo
    PYTHON_EXECUTABLE     python to invoke (default: python3; use the venv's python)
    RECORDINGS_ROOT       where per-creator subdirs live (default: /data/recordings)
    STATE_FILE            JSON file persisting the watchlist (default: /var/lib/tt-recorder/state.json)
    MAX_WATCHERS          soft cap per backend (default: 30)
    CONTROL_PLANE_URL     base URL where lifecycle events are POSTed (events go to {URL}/events)
    CONTROL_PLANE_TOKEN   bearer token sent on outgoing webhooks
"""

from __future__ import annotations

import asyncio
import json
import logging
import glob
import os
import shutil
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from watcher import WatcherConfig, WatcherEvent, WatcherProcess

log = logging.getLogger(__name__)

# Reported in /health so the control plane can show which build each backend runs.
# Reads a VERSION file shipped next to the code; literal is a fallback.
def _read_version(default: str = "0.0.0-unstamped") -> str:
    try:
        from pathlib import Path as _P
        p = _P(__file__).resolve().parent / "VERSION"
        if p.exists() and p.read_text().strip():
            return p.read_text().strip()
    except Exception:
        pass
    return default

BACKEND_BUILD = _read_version()


# ---------------------------------------------------------------------------
# Settings

class Settings:
    backend_id: str = os.environ.get("BACKEND_ID", "unknown")
    region: str = os.environ.get("REGION", "unknown")
    auth_token: str = os.environ.get("AUTH_TOKEN", "")
    repo_path: Path = Path(os.environ.get("REPO_PATH", "/opt/tiktok-live-recorder"))
    python_executable: str = os.environ.get("PYTHON_EXECUTABLE", "python3")
    recordings_root: Path = Path(os.environ.get("RECORDINGS_ROOT", "/data/recordings"))
    # Don't list a file as available for upload/move until it has been untouched
    # for this long (and isn't held open) — protects in-progress recordings,
    # especially once the recorder and storage are on separate machines.
    settle_sec: int       = int(os.environ.get("RECORDER_SETTLE_SEC", "120"))
    # Orphaned-_flv reaper: finalize/clean up intermediate recordings whose
    # recorder died before remux (the classic disk-filler). See _flv_reaper_loop.
    reap_enabled: bool    = os.environ.get("FLV_REAP_ENABLED", "1") == "1"
    reap_interval: int    = int(os.environ.get("FLV_REAP_INTERVAL_SEC", "600"))
    # 0 = never delete a corrupt/un-remuxable orphan (quarantine it). >0 = delete
    # corrupt orphans older than this many days (reclaims space, loses the file).
    reap_delete_corrupt_days: float = float(os.environ.get("FLV_REAP_DELETE_CORRUPT_DAYS", "0"))
    state_file: Path = Path(os.environ.get("STATE_FILE", "/var/lib/tt-recorder/state.json"))
    max_watchers: int = int(os.environ.get("MAX_WATCHERS", "30"))
    # Max random delay (seconds) before a watcher's first spawn, so a mass
    # start (restart / re-enable) doesn't hit TikTok all at once. 0 disables.
    startup_jitter_sec: float = float(os.environ.get("WATCHER_STARTUP_JITTER_SEC", "20"))
    sign_api_key: str = os.environ.get("SIGN_API_KEY") or os.environ.get("EULER_API_KEY") or ""

    @property
    def chat_python(self) -> str:
        """Python used to run chat_recorder.py. Prefer a dedicated chat venv so
        TikTokLive's deps don't collide with the recorder's; fall back to ours."""
        env = os.environ.get("CHAT_PYTHON")
        if env:
            return env
        cand = Path("/opt/tt-backend/.venv-chat/bin/python3")
        if cand.exists():
            return str(cand)
        return self.python_executable
    control_plane_url: Optional[str] = os.environ.get("CONTROL_PLANE_URL") or None
    control_plane_token: Optional[str] = os.environ.get("CONTROL_PLANE_TOKEN") or None

    def validate(self) -> None:
        if not self.auth_token:
            raise RuntimeError("AUTH_TOKEN env var must be set")
        if not self.repo_path.exists():
            raise RuntimeError(f"REPO_PATH does not exist: {self.repo_path}")


settings = Settings()


# ---------------------------------------------------------------------------
# Webhook delivery

async def _send_webhook(event: WatcherEvent) -> None:
    """Fire-and-forget POST to the control plane. Failures are logged only."""
    if not settings.control_plane_url:
        return

    payload = {
        "backend_id": settings.backend_id,
        "event": {
            "username": event.username,
            "kind": event.kind,
            "timestamp": event.timestamp,
            "detail": event.detail,
        },
    }
    headers = {}
    if settings.control_plane_token:
        headers["Authorization"] = f"Bearer {settings.control_plane_token}"

    url = settings.control_plane_url.rstrip("/") + "/events"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.post(url, json=payload, headers=headers)
            if r.status_code >= 400:
                log.warning("webhook %s -> HTTP %d: %s",
                            event.kind, r.status_code, r.text[:200])
    except Exception as e:
        log.warning("webhook %s failed: %s", event.kind, e)


async def _on_event(event: WatcherEvent) -> None:
    """Top-level handler attached to every WatcherProcess."""
    log.info("[event] %s %s %s", event.username, event.kind, event.detail)
    # Don't block the watcher loop on webhook latency
    asyncio.create_task(_send_webhook(event))


# ---------------------------------------------------------------------------
# Watcher manager

class WatcherManager:
    def __init__(self):
        self._watchers: dict[str, WatcherProcess] = {}
        self._lock = asyncio.Lock()

    async def add(self, username: str, automatic_interval_min: int,
                  capture_chat: bool = True) -> WatcherProcess:
        async with self._lock:
            if username in self._watchers:
                raise HTTPException(409, f"watcher for {username} already exists")
            if len(self._watchers) >= settings.max_watchers:
                raise HTTPException(
                    503,
                    f"backend at capacity ({settings.max_watchers} watchers)",
                )

            config = WatcherConfig(
                username=username,
                repo_path=settings.repo_path,
                recordings_root=settings.recordings_root,
                python_executable=settings.python_executable,
                automatic_interval_min=automatic_interval_min,
                startup_jitter_sec=settings.startup_jitter_sec,
                capture_chat=capture_chat,
                sign_api_key=settings.sign_api_key,
                chat_python=settings.chat_python,
            )
            watcher = WatcherProcess(config, on_event=_on_event)
            await watcher.start()
            self._watchers[username] = watcher
            await self._persist()
            return watcher

    async def remove(self, username: str) -> None:
        async with self._lock:
            watcher = self._watchers.pop(username, None)
            if not watcher:
                raise HTTPException(404, f"no watcher for {username}")
            await self._persist()
        # Stop outside the lock — graceful shutdown can take 30s
        await watcher.stop()

    def get(self, username: str) -> Optional[WatcherProcess]:
        return self._watchers.get(username)

    async def restart(self, username: str) -> WatcherProcess:
        watcher = self._watchers.get(username)
        if not watcher:
            raise HTTPException(404, f"no watcher for {username}")
        await watcher.restart()
        return watcher

    def list_all(self) -> list[WatcherProcess]:
        return list(self._watchers.values())

    async def stop_all(self) -> None:
        watchers = list(self._watchers.values())
        await asyncio.gather(*(w.stop() for w in watchers), return_exceptions=True)
        self._watchers.clear()

    # ---- persistence ----

    async def _persist(self) -> None:
        """Atomic write of the current watchlist to JSON."""
        settings.state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "watchers": [
                {
                    "username": w.config.username,
                    "automatic_interval_min": w.config.automatic_interval_min,
                    "capture_chat": w.config.capture_chat,
                }
                for w in self._watchers.values()
            ],
        }
        # Write to a temp file in the same dir, then rename atomically
        fd, tmp_path = tempfile.mkstemp(
            dir=str(settings.state_file.parent), prefix=".state-", suffix=".json",
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, settings.state_file)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    async def restore(self) -> None:
        """On startup, re-create watchers from the state file."""
        if not settings.state_file.exists():
            log.info("no state file at %s; starting empty", settings.state_file)
            return
        try:
            with open(settings.state_file) as f:
                data = json.load(f)
        except Exception as e:
            log.error("could not read state file %s: %s", settings.state_file, e)
            return

        entries = data.get("watchers", [])
        log.info("restoring %d watcher(s) from state", len(entries))
        for entry in entries:
            try:
                config = WatcherConfig(
                    username=entry["username"],
                    repo_path=settings.repo_path,
                    recordings_root=settings.recordings_root,
                    python_executable=settings.python_executable,
                    automatic_interval_min=entry.get("automatic_interval_min", 3),
                    startup_jitter_sec=settings.startup_jitter_sec,
                    capture_chat=entry.get("capture_chat", True),
                    sign_api_key=settings.sign_api_key,
                    chat_python=settings.chat_python,
                )
                watcher = WatcherProcess(config, on_event=_on_event)
                await watcher.start(jitter=True)   # stagger mass startup
                self._watchers[entry["username"]] = watcher
            except Exception as e:
                log.error("failed to restore watcher for %s: %s",
                          entry.get("username"), e)


manager = WatcherManager()


# ---------------------------------------------------------------------------
# Auth

def require_auth(authorization: str = Header(default="")) -> None:
    expected = f"Bearer {settings.auth_token}"
    if authorization != expected:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing bearer token")


# ---------------------------------------------------------------------------
# Pydantic shapes

class WatcherCreate(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    automatic_interval_min: int = Field(3, ge=1, le=60)
    capture_chat: bool = True


class ChatToggle(BaseModel):
    enabled: bool


class WatcherStatusOut(BaseModel):
    username: str
    state: str
    pid: Optional[int]
    consecutive_failures: int
    last_error: Optional[str]
    automatic_interval_min: int
    capture_chat: bool = True
    chat_running: bool = False


class HealthOut(BaseModel):
    backend_id: str
    build: str = ""
    region: str
    active_watchers: int
    max_watchers: int
    disk_free_bytes: int
    disk_total_bytes: int
    ffmpeg_version: Optional[str]
    recorder_git_rev: Optional[str]


class FileInfo(BaseModel):
    path: str
    size_bytes: int
    mtime: float


# ---------------------------------------------------------------------------
# Helpers for /health

def _ffmpeg_version() -> Optional[str]:
    try:
        out = subprocess.check_output(["ffmpeg", "-version"], text=True, timeout=2)
        return out.splitlines()[0]
    except Exception:
        return None


def _recorder_git_rev() -> Optional[str]:
    try:
        return subprocess.check_output(
            ["git", "-C", str(settings.repo_path), "rev-parse", "--short", "HEAD"],
            text=True, timeout=2,
        ).strip()
    except Exception:
        return None


_HEALTH_CACHE = {"ffmpeg": None, "rev": None, "fetched_at": 0.0}


def _cached_health() -> tuple[Optional[str], Optional[str]]:
    """Cache ffmpeg/git version for 60s — they don't change at runtime."""
    if time.time() - _HEALTH_CACHE["fetched_at"] > 60:
        _HEALTH_CACHE["ffmpeg"] = _ffmpeg_version()
        _HEALTH_CACHE["rev"] = _recorder_git_rev()
        _HEALTH_CACHE["fetched_at"] = time.time()
    return _HEALTH_CACHE["ffmpeg"], _HEALTH_CACHE["rev"]


def _watcher_to_out(w: WatcherProcess) -> WatcherStatusOut:
    s = w.status
    return WatcherStatusOut(
        username=s["username"],
        state=s["state"],
        pid=s["pid"],
        consecutive_failures=s["consecutive_failures"],
        last_error=s["last_error"],
        automatic_interval_min=w.config.automatic_interval_min,
        capture_chat=w.config.capture_chat,
        chat_running=w._chat_running,
    )


# ---------------------------------------------------------------------------
# App lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings.validate()
    settings.recordings_root.mkdir(parents=True, exist_ok=True)
    log.info("backend %s (region=%s) starting", settings.backend_id, settings.region)
    await manager.restore()
    reaper_task = None
    if settings.reap_enabled:
        reaper_task = asyncio.create_task(_flv_reaper_loop(), name="flv-reaper")
        log.info("orphan-_flv reaper enabled (every %ds)", settings.reap_interval)
    try:
        yield
    finally:
        if reaper_task:
            reaper_task.cancel()
        log.info("stopping all watchers gracefully...")
        await manager.stop_all()


app = FastAPI(title="TikTok Recorder Backend", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints

@app.get("/health", response_model=HealthOut)
async def health():
    """No auth on /health so the control plane can probe before authenticating."""
    ffmpeg_v, git_rev = _cached_health()
    du = shutil.disk_usage(settings.recordings_root)
    return HealthOut(
        backend_id=settings.backend_id,
        build=BACKEND_BUILD,
        region=settings.region,
        active_watchers=len(manager.list_all()),
        max_watchers=settings.max_watchers,
        disk_free_bytes=du.free,
        disk_total_bytes=du.total,
        ffmpeg_version=ffmpeg_v,
        recorder_git_rev=git_rev,
    )


@app.get("/watchers", response_model=list[WatcherStatusOut],
         dependencies=[Depends(require_auth)])
async def list_watchers():
    return [_watcher_to_out(w) for w in manager.list_all()]


@app.post("/watchers", response_model=WatcherStatusOut,
          status_code=status.HTTP_201_CREATED,
          dependencies=[Depends(require_auth)])
async def create_watcher(body: WatcherCreate):
    # Normalize: strip leading @, lowercase
    username = body.username.lstrip("@").strip().lower()
    if not username:
        raise HTTPException(400, "username cannot be empty")
    watcher = await manager.add(username, body.automatic_interval_min, body.capture_chat)
    return _watcher_to_out(watcher)


@app.get("/watchers/{username}", response_model=WatcherStatusOut,
         dependencies=[Depends(require_auth)])
async def get_watcher(username: str):
    watcher = manager.get(username.lstrip("@").strip().lower())
    if not watcher:
        raise HTTPException(404, f"no watcher for {username}")
    return _watcher_to_out(watcher)


@app.delete("/watchers/{username}", status_code=status.HTTP_204_NO_CONTENT,
            dependencies=[Depends(require_auth)])
async def delete_watcher(username: str):
    await manager.remove(username.lstrip("@").strip().lower())
    return None


@app.post("/watchers/{username}/restart", response_model=WatcherStatusOut,
          dependencies=[Depends(require_auth)])
async def restart_watcher(username: str):
    """Re-enable a watcher (resets the failure circuit-breaker and restarts it)."""
    watcher = await manager.restart(username.lstrip("@").strip().lower())
    return _watcher_to_out(watcher)


@app.post("/watchers/{username}/chat", response_model=WatcherStatusOut,
          dependencies=[Depends(require_auth)])
async def toggle_chat(username: str, body: ChatToggle):
    """Enable/disable TikTok chat capture for a watcher (restarts it to apply)."""
    username = username.lstrip("@").strip().lower()
    w = manager.get(username)
    if not w:
        raise HTTPException(404, f"no watcher for {username}")
    w.config.capture_chat = body.enabled
    await manager.restart(username)
    await manager._persist()
    return _watcher_to_out(manager.get(username))


@app.get("/diag/chat", dependencies=[Depends(require_auth)])
async def diag_chat():
    """Backend-level chat-capture diagnostics: is the library installed, is the
    recorder script present, is a sign key set, and the live state + recent log
    of each watcher's chat recorder. Powers the dashboard's chat health view."""
    tiktoklive_installed = False
    tiktoklive_version = None
    tiktoklive_error = None
    chat_python = settings.chat_python
    try:
        import subprocess as _sp
        r = _sp.run([chat_python, "-c",
                     "import TikTokLive, importlib.metadata as m; print(m.version('TikTokLive'))"],
                    capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            tiktoklive_installed = True
            tiktoklive_version = r.stdout.strip() or "unknown"
        else:
            tiktoklive_error = (r.stderr.strip() or "import failed")[:300]
    except Exception as e:
        tiktoklive_error = f"{type(e).__name__}: {e}"

    candidates = [settings.repo_path.parent / "chat_recorder.py",
                  Path(__file__).resolve().parent / "chat_recorder.py"]
    recorder_path = next((str(p) for p in candidates if p.exists()), None)

    watchers = []
    for w in manager.list_all():
        cs = w.chat_status()
        cs["username"] = w.config.username
        watchers.append(cs)

    return {
        "backend_id":            settings.backend_id,
        "tiktoklive_installed":  tiktoklive_installed,
        "tiktoklive_version":    tiktoklive_version,
        "tiktoklive_error":      tiktoklive_error,
        "chat_recorder_present": recorder_path is not None,
        "chat_recorder_path":    recorder_path,
        "sign_api_key_set":      bool(settings.sign_api_key),
        "python_executable":     settings.python_executable,
        "chat_python":           chat_python,
        "watchers":              watchers,
    }


def _open_files() -> set:
    """Real paths of files this process tree currently holds open — i.e. files a
    recorder is still writing. Reads /proc/<pid>/fd; empty set on any error."""
    out: set = set()
    try:
        for fd_dir in glob.glob("/proc/[0-9]*/fd"):
            try:
                for fd in os.listdir(fd_dir):
                    try:
                        out.add(os.path.realpath(os.path.join(fd_dir, fd)))
                    except OSError:
                        continue
            except OSError:
                continue
    except Exception:
        pass
    return out


def _in_progress(path: Path, now: float, open_files: set) -> bool:
    """True if a file is still being recorded (fresh mtime, or held open), so it
    should not be offered to the upload worker / move engine yet."""
    try:
        if now - path.stat().st_mtime < settings.settle_sec:
            return True
    except OSError:
        return True
    try:
        if os.path.realpath(path) in open_files:
            return True
    except OSError:
        return True
    return False


# ---------------------------------------------------------------------------
# Orphaned-_flv reaper
#
# Michele0303 records to TK_<user>_<ts>_flv.mp4, then remuxes it to the final
# TK_<user>_<ts>.mp4 on a clean stop. If the recorder is killed first (OOM,
# reboot, crash) the _flv intermediate is orphaned and never cleaned up — and
# app.py's /files deliberately hides _flv, so nothing else ever removes it. They
# pile up and fill the disk. The reaper finalizes/cleans them, safely.

_reap_stats: dict = {"last_run": None, "runs": 0,
                     "redundant": 0, "salvaged": 0, "corrupt": 0, "freed_bytes": 0}

_FLV_SUFFIX = "_flv.mp4"


async def _remux_flv(flv: Path, final: Path) -> bool:
    """Finalize an orphan _flv into its final .mp4 with a fast, lossless
    stream-copy. Atomic (temp → rename); the source is kept until the final
    verifies, so a failed remux can't lose a recording. Returns True on success."""
    if shutil.which("ffmpeg") is None:
        return False
    tmp = final.with_name(final.name + ".remuxing")   # not *.mp4 → scanners ignore it
    try:
        tmp.unlink()
    except OSError:
        pass
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
            "-i", str(flv), "-c", "copy", "-movflags", "+faststart", "-f", "mp4", str(tmp),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await asyncio.wait_for(proc.wait(), timeout=900)
    except Exception:
        try: tmp.unlink()
        except OSError: pass
        return False
    if rc == 0 and tmp.exists() and tmp.stat().st_size > 0:
        try:
            os.replace(tmp, final)
            return True
        except OSError:
            pass
    try: tmp.unlink()
    except OSError: pass
    return False


async def _reap_flv_once() -> dict:
    """One pass over RECORDINGS_ROOT. For each settled _flv NOT still being
    recorded: delete it if the final already exists, else remux it to the final
    and delete it. Corrupt/un-remuxable orphans are quarantined (left in place)
    unless FLV_REAP_DELETE_CORRUPT_DAYS is set."""
    root = settings.recordings_root
    res = {"redundant": 0, "salvaged": 0, "corrupt": 0, "freed_bytes": 0, "skipped": 0}
    if not root.exists():
        return res
    open_files = _open_files()
    now = time.time()
    for flv in root.rglob("*" + _FLV_SUFFIX):
        if not flv.is_file():
            continue
        if _in_progress(flv, now, open_files):
            res["skipped"] += 1
            continue                       # still recording — never touch it
        try:
            size = flv.stat().st_size
        except OSError:
            continue
        final = flv.with_name(flv.name[:-len(_FLV_SUFFIX)] + ".mp4")
        final_ready = (final.exists() and final.stat().st_size > 0
                       and not _in_progress(final, now, open_files))
        try:
            if final_ready:
                flv.unlink()               # redundant leftover of a finished recording
                res["redundant"] += 1
                res["freed_bytes"] += size
                log.info("reaper: removed redundant %s (final exists)", flv.name)
            elif await _remux_flv(flv, final):
                flv.unlink()               # orphan salvaged into a clean final
                res["salvaged"] += 1
                res["freed_bytes"] += size
                log.info("reaper: salvaged orphan %s → %s", flv.name, final.name)
            else:
                res["corrupt"] += 1
                days = settings.reap_delete_corrupt_days
                if days > 0 and (now - flv.stat().st_mtime) > days * 86400:
                    flv.unlink()
                    res["freed_bytes"] += size
                    log.warning("reaper: deleted corrupt orphan %s (older than %.1fd)",
                                flv.name, days)
                else:
                    log.warning("reaper: %s won't remux — quarantined (corrupt/truncated)",
                                flv.name)
        except OSError as e:
            log.warning("reaper: error handling %s: %s", flv.name, e)
    return res


async def _flv_reaper_loop() -> None:
    """Run the reaper shortly after startup (catches reboot/OOM leftovers) then on
    an interval. Disable with FLV_REAP_ENABLED=0."""
    try:
        await asyncio.sleep(30)            # let restore() settle first
    except asyncio.CancelledError:
        return
    while True:
        try:
            r = await _reap_flv_once()
            _reap_stats["last_run"] = time.time()
            _reap_stats["runs"] += 1
            for k in ("redundant", "salvaged", "corrupt", "freed_bytes"):
                _reap_stats[k] += r[k]
            if r["redundant"] or r["salvaged"] or r["corrupt"]:
                log.info("reaper pass: %d redundant, %d salvaged, %d corrupt, %s freed",
                         r["redundant"], r["salvaged"], r["corrupt"], f"{r['freed_bytes']:,}")
        except Exception:
            log.exception("flv reaper pass failed")
        try:
            await asyncio.sleep(settings.reap_interval)
        except asyncio.CancelledError:
            return


@app.get("/files/chat", dependencies=[Depends(require_auth)])
async def list_chat_files():
    """List completed chat logs (*_chat.jsonl) so the upload worker can ship them
    to storage alongside recordings. Kept separate from /files (which is MP4-only
    for the Files tab)."""
    result: list[dict] = []
    if not settings.recordings_root.exists():
        return result
    open_files = _open_files()
    now = time.time()
    for jf in settings.recordings_root.rglob("*_chat.jsonl"):
        if _in_progress(jf, now, open_files):
            continue          # still being written by a live chat recorder
        try:
            st = jf.stat()
            result.append({"path": str(jf), "username": jf.parent.name,
                           "filename": jf.name, "size_bytes": st.st_size,
                           "mtime": st.st_mtime})
        except OSError:
            pass
    return result


@app.get("/files", response_model=dict[str, list[FileInfo]],
         dependencies=[Depends(require_auth)])
async def list_files():
    """Returns a per-creator MP4 inventory. Only .mp4 files are returned —
    transcript sidecars (.txt/.srt/.json) are excluded intentionally."""
    result: dict[str, list[FileInfo]] = {}
    if not settings.recordings_root.exists():
        return result
    open_files = _open_files()
    now = time.time()
    for creator_dir in settings.recordings_root.iterdir():
        if not creator_dir.is_dir():
            continue
        files = []
        for f in creator_dir.iterdir():
            if not f.is_file():
                continue
            if f.suffix.lower() != ".mp4":   # skip .txt .srt .json sidecars
                continue
            if f.name.startswith("_"):        # skip _recorder.log etc
                continue
            if f.name.endswith("_flv.mp4") or _in_progress(f, now, open_files):
                continue                       # still recording — don't offer it
            stat = f.stat()
            files.append(FileInfo(
                path=str(f),
                size_bytes=stat.st_size,
                mtime=stat.st_mtime,
            ))
        if files:
            result[creator_dir.name] = files
    return result


@app.get("/flv/status", dependencies=[Depends(require_auth)])
async def flv_status():
    """Orphan-_flv reaper telemetry: cumulative counts since start + last run."""
    return {**_reap_stats, "enabled": settings.reap_enabled,
            "interval_sec": settings.reap_interval,
            "delete_corrupt_days": settings.reap_delete_corrupt_days}


@app.post("/flv/reap-now", dependencies=[Depends(require_auth)])
async def flv_reap_now():
    """Trigger an immediate reaper pass (handy after a disk fills). Returns the
    per-pass counts."""
    r = await _reap_flv_once()
    _reap_stats["last_run"] = time.time()
    _reap_stats["runs"] += 1
    for k in ("redundant", "salvaged", "corrupt", "freed_bytes"):
        _reap_stats[k] += r[k]
    return r


@app.get("/cookies", dependencies=[Depends(require_auth)])
async def get_cookies():
    """Return the current TikTok session cookies used by Michele0303.
    Returns {} if no cookies file exists yet."""
    cookie_path = settings.repo_path / "src" / "cookies.json"
    if not cookie_path.exists():
        return {}
    try:
        return json.loads(cookie_path.read_text())
    except Exception:
        return {}


@app.post("/cookies", dependencies=[Depends(require_auth)])
async def set_cookies(body: dict):
    """Write TikTok session cookies to cookies.json.
    New cookies are picked up by Michele0303 on its next spawn
    (i.e. the next recording session for each watcher)."""
    cookie_path = settings.repo_path / "src" / "cookies.json"
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookie_path.write_text(json.dumps(body, indent=2))
    return {"ok": True, "path": str(cookie_path)}


@app.get("/files/download", dependencies=[Depends(require_auth)])
async def download_file(path: str):
    """Stream a recording file to the caller.
    Path must live under RECORDINGS_ROOT — same traversal check as DELETE."""
    target = Path(path).resolve()
    root = settings.recordings_root.resolve()
    try:
        target.relative_to(root)
    except ValueError:
        raise HTTPException(400, "path outside recordings root")
    if not target.exists() or not target.is_file():
        raise HTTPException(404, "file not found")
    return FileResponse(
        target,
        filename=target.name,
        media_type="video/mp4",
        headers={"Content-Disposition": f'attachment; filename="{target.name}"'},
    )


@app.delete("/files", status_code=status.HTTP_204_NO_CONTENT,
            dependencies=[Depends(require_auth)])
async def delete_file(path: str):
    """Used by the upload worker after a successful storage handoff:
    'I uploaded this, you can drop it now'. Path must live under RECORDINGS_ROOT."""
    target = Path(path).resolve()
    root = settings.recordings_root.resolve()
    try:
        target.relative_to(root)  # raises ValueError if outside root
    except ValueError:
        raise HTTPException(400, "path outside recordings root")
    if not target.exists():
        raise HTTPException(404, "file not found")
    if not target.is_file():
        raise HTTPException(400, "not a regular file")
    target.unlink()
    return None
