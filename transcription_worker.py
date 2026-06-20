"""
Whisper Transcription Worker

Watches a directory for MP4 recordings and transcribes them with faster-whisper.
Saves .txt only (timestamped, human-readable, with a language/duration header line).

Run:
    pip install --break-system-packages faster-whisper fastapi 'uvicorn[standard]'
    export WHISPER_WATCH_DIR=/archive/recordings
    export WHISPER_AUTH_TOKEN=yourtoken
    python3 transcription_worker.py

On first run the chosen model downloads automatically (~150MB for base) and is
cached at ~/.cache/huggingface/ — subsequent starts are instant.

Models (WHISPER_MODEL env var):
    tiny     ~75MB    ~32x realtime on CPU   fastest
    base     ~150MB   ~16x realtime on CPU   good balance  <-- default
    small    ~480MB   ~6x  realtime on CPU   better accuracy
    medium   ~1.5GB   ~2x  realtime on CPU   high accuracy
    large-v3 ~3GB     ~1x  realtime on CPU   best (needs 4GB+ RAM)

A 1-hour stream on a modest VPS:
    tiny ~2 min | base ~4 min | small ~10 min | medium ~30 min
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import glob
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel

log = logging.getLogger(__name__)

# Reported in /health and /status. Reads a VERSION file shipped next to the
# code so the build id is bumped in one place; literal is a fallback.
def _read_version(default: str = "0.0.0-unstamped") -> str:
    try:
        p = Path(__file__).resolve().parent / "VERSION"
        if p.exists() and p.read_text().strip():
            return p.read_text().strip()
    except Exception:
        pass
    return default

WORKER_BUILD = _read_version()


# ---------------------------------------------------------------------------
# Settings

class Settings:
    watch_dir: Path       = Path(os.environ.get("WHISPER_WATCH_DIR", "/archive/recordings"))
    model: str            = os.environ.get("WHISPER_MODEL", "base")
    language: Optional[str] = os.environ.get("WHISPER_LANGUAGE") or None
    device: str           = os.environ.get("WHISPER_DEVICE", "cpu")
    compute_type: str     = os.environ.get("WHISPER_COMPUTE_TYPE", "int8")
    auth_token: str       = os.environ.get("WHISPER_AUTH_TOKEN", "")
    # Friendly password for the per-node web UI. Accepted in addition to the
    # control-plane bearer token. Default is a placeholder — CHANGE IT on any
    # node reachable beyond a trusted network.
    ui_password: str      = os.environ.get("STORAGE_UI_PASSWORD", "yourpassword")
    host: str             = os.environ.get("WHISPER_HOST", "0.0.0.0")
    port: int             = int(os.environ.get("WHISPER_PORT", "8090"))
    scan_interval: int    = int(os.environ.get("WHISPER_SCAN_INTERVAL", "30"))
    # Don't transcribe a file until it has been untouched for this long. On a
    # colocated box the worker shares the recordings dir with the recorder, so
    # without this it would pick up files that are still being written.
    settle_sec: int       = int(os.environ.get("WHISPER_SETTLE_SEC", "120"))

    # ── concurrency & stuck-process handling ─────────────────────────────────
    # How many files to transcribe at once. Each runs in its own subprocess and
    # loads its own copy of the model, so RAM/VRAM use scales with this number.
    concurrency: int      = max(1, int(os.environ.get("WHISPER_CONCURRENCY", "2")))
    # Storage-only mode: when WHISPER_TRANSCRIBE=0 the server still receives,
    # serves, moves and archives files, but does not transcribe. Use this to
    # designate a box as pure storage (e.g. the recorder ships finished files to
    # a different, transcription-enabled server).
    transcribe_enabled: bool = os.environ.get("WHISPER_TRANSCRIBE", "1") != "0"
    # Voice-activity detection skips silent stretches before transcribing — a big
    # speed win on lives with lots of dead air. On by default; tunable threshold.
    vad: bool             = os.environ.get("WHISPER_VAD", "1") != "0"
    vad_min_silence_ms: int = int(os.environ.get("WHISPER_VAD_MIN_SILENCE_MS", "500"))
    # Decoding beam size. 5 is the accurate default; 1 (greedy) is markedly faster
    # on CPU for a small accuracy cost — the biggest lever for a transcription backlog.
    beam_size: int        = max(1, int(os.environ.get("WHISPER_BEAM_SIZE", "5")))
    # Flag a file as "stalled" in the UI if it makes no progress for this long.
    stall_sec: int        = int(os.environ.get("WHISPER_STALL_SEC", "180"))
    # Hard kill a transcription after this many seconds. 0 = auto: once the audio
    # duration is known, allow 10x realtime + 5 min, else fall back to 2 hours.
    file_timeout_sec: int = int(os.environ.get("WHISPER_FILE_TIMEOUT", "0"))
    # How many times to attempt a file before giving up and writing a .error
    # marker (so it stops being retried forever). The final attempt runs in a
    # safer mode (VAD off, greedy) to recover from VAD/beam-specific failures.
    max_attempts: int     = max(1, int(os.environ.get("WHISPER_MAX_ATTEMPTS", "3")))
    # Base backoff between attempts (doubles each time, capped at 1h).
    retry_backoff_sec: int = int(os.environ.get("WHISPER_RETRY_BACKOFF", "120"))
    # Salvage: when a (corrupt/interrupted) recording won't decode directly, pull
    # whatever audio ffmpeg can recover (skipping bad packets) and transcribe that.
    salvage: bool         = os.environ.get("WHISPER_SALVAGE", "1") != "0"

    # ── 3rd-hop archive (rclone → any cloud) ─────────────────────────────────
    # Set ARCHIVE_REMOTE to an rclone remote+path (e.g. "dropbox:tt-recordings")
    # to push recordings to cloud storage AFTER they're transcribed. Empty = off.
    # rclone + rclone-python must be installed and the remote configured once
    # via `rclone config`.
    archive_remote: str   = os.environ.get("ARCHIVE_REMOTE", "").strip()
    archive_what: str     = os.environ.get("ARCHIVE_WHAT", "mp4").strip()   # mp4|txt|both
    archive_delete_local: bool = os.environ.get("ARCHIVE_DELETE_LOCAL", "0") == "1"
    archive_delete_delay_sec: int = int(os.environ.get("ARCHIVE_DELETE_DELAY_SEC", "0"))
    # Disk-aware eviction: instead of deleting every archived file after a fixed
    # delay, keep recordings hot and only evict (delete the verified-on-cloud local
    # copy) once disk usage crosses the high-water mark — oldest first, down to the
    # low-water mark. Files younger than the min-age are always kept hot.
    archive_evict_high_pct: float  = float(os.environ.get("ARCHIVE_EVICT_HIGH_PCT", "85"))
    archive_evict_low_pct: float   = float(os.environ.get("ARCHIVE_EVICT_LOW_PCT", "70"))
    archive_evict_min_age_sec: int = int(os.environ.get("ARCHIVE_EVICT_MIN_AGE_SEC", "86400"))
    # rclone throughput: parallel transfers and an optional bandwidth cap (e.g.
    # "10M"); empty = unlimited. Plus per-file archive retry backoff.
    archive_transfers: int = max(1, int(os.environ.get("ARCHIVE_TRANSFERS", "4")))
    archive_bwlimit: str   = os.environ.get("ARCHIVE_BWLIMIT", "").strip()
    archive_retry_backoff_sec: int = int(os.environ.get("ARCHIVE_RETRY_BACKOFF", "120"))
    # Age-based lifecycle (0 = off). Both keep the transcript + chat forever.
    #   EVICT: after N days, drop the LOCAL .mp4 of an archived recording (it stays
    #          on the cloud) regardless of disk pressure — proactive cold storage.
    #   PURGE: after M days, also delete the CLOUD copy (rclone delete) and any
    #          local .mp4 — removes the video entirely, keeping txt + chat.
    retention_evict_days: float = float(os.environ.get("RETENTION_EVICT_DAYS", "0") or 0)
    retention_purge_days: float = float(os.environ.get("RETENTION_PURGE_DAYS", "0") or 0)


settings = Settings()


# ---------------------------------------------------------------------------
# Auth

def require_auth(authorization: str = Header(default="")) -> None:
    # Auth is enforced only when a worker token is configured, preserving the
    # "no token => open" behaviour the control plane relies on (so this never
    # breaks an existing node's control-plane access). When enforced, EITHER the
    # control plane's bearer token OR the UI password is accepted.
    if not settings.auth_token:
        return
    supplied = authorization[7:].strip() if authorization.startswith("Bearer ") else ""
    allowed = {settings.auth_token}
    if settings.ui_password:
        allowed.add(settings.ui_password)
    if supplied not in allowed:
        raise HTTPException(401, "invalid or missing credentials")


# ---------------------------------------------------------------------------
# Format helpers

def _fmt_ts(seconds: float) -> str:
    """HH:MM:SS for use in the human-readable .txt output."""
    h  = int(seconds) // 3600
    m  = int(seconds) // 60 % 60
    s  = int(seconds) % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _fmt_srt_ts(seconds: float) -> str:
    """HH:MM:SS,mmm for SRT subtitle files."""
    ms = round((seconds % 1) * 1000)
    return f"{_fmt_ts(seconds)},{ms:03d}"


def _progress_path(mp4: Path) -> Path:
    return mp4.with_suffix(".progress")


def _extract_audio_tolerant(src: Path) -> Optional[Path]:
    """Fault-tolerant audio recovery for corrupt/interrupted recordings: skip bad
    packets and pull whatever audio still decodes into a clean 16 kHz mono WAV that
    Whisper can read. Returns the WAV path, or None if nothing usable came out.
    (A recording missing its moov atom entirely usually can't be opened at all —
    that's the one case this can't rescue.)"""
    import tempfile as _tf
    d = Path(_tf.mkdtemp(prefix="tw-salvage-"))
    out = d / (src.stem + ".wav")
    cmd = ["ffmpeg", "-nostdin", "-y",
           "-err_detect", "ignore_err", "-fflags", "+discardcorrupt+genpts",
           "-i", str(src), "-vn", "-ac", "1", "-ar", "16000",
           "-c:a", "pcm_s16le", str(out)]
    try:
        subprocess.run(cmd, timeout=3600,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"[salvage] ffmpeg failed for {src.name}: {e}", flush=True)
    if out.exists() and out.stat().st_size > 1024:
        return out
    try:
        if out.exists():
            out.unlink()
        d.rmdir()
    except OSError:
        pass
    return None


def _transcribe_one_subprocess(mp4_str: str, safe: bool = False) -> int:
    """Run as a child process: `transcription_worker.py --transcribe-one <mp4> [--safe]`.
    Loads the model, transcribes incrementally, writes progress to <mp4>.progress
    after each segment, then writes the final .txt. Isolated so a hang here can be
    killed by the parent without taking down the worker. In `safe` mode VAD is
    disabled and decoding is greedy (beam 1) — a more robust last-ditch attempt."""
    import json as _json
    import traceback as _tb
    mp4 = Path(mp4_str)
    prog = _progress_path(mp4)
    started = time.time()

    def write_progress(**kw):
        data = {"pid": os.getpid(), "started": started,
                "updated": time.time(), "elapsed": round(time.time() - started, 1)}
        data.update(kw)
        try:
            tmp = prog.with_suffix(".progress.tmp")
            tmp.write_text(_json.dumps(data))
            tmp.rename(prog)
        except OSError:
            pass

    wav = None
    try:
        write_progress(pct=0.0, processed_sec=0.0, duration=None, done=False)
        from faster_whisper import WhisperModel
        vad  = False if safe else settings.vad
        beam = 1 if safe else settings.beam_size
        if safe:
            print(f"[transcribe-one] {mp4.name}: SAFE retry (vad off, beam 1)", flush=True)
        model = WhisperModel(settings.model, device=settings.device,
                             compute_type=settings.compute_type)

        def _decode(src):
            seg_iter, info = model.transcribe(
                str(src), language=settings.language, beam_size=beam,
                word_timestamps=True, vad_filter=vad,
                vad_parameters={"min_silence_duration_ms": settings.vad_min_silence_ms},
            )
            dur = round(info.duration, 1) if info.duration else None
            write_progress(pct=0.0, processed_sec=0.0, duration=dur,
                           language=info.language, done=False)
            segs, last_write = [], 0.0
            for seg in seg_iter:           # generator → incremental progress
                segs.append(seg)
                now = time.time()
                if now - last_write >= 1.0:
                    pct = min(0.999, (seg.end / info.duration)) if info.duration else 0.0
                    write_progress(pct=round(pct, 4), processed_sec=round(seg.end, 1),
                                   duration=dur, language=info.language,
                                   segments=len(segs), done=False)
                    last_write = now
            return segs, info, dur

        # Fast path: decode the file directly. On a decode error — or a
        # suspiciously-empty result from a possibly-corrupt stream — salvage the
        # audio with a fault-tolerant ffmpeg pass (skip bad packets) and transcribe
        # that, recovering as much speech as the file still contains.
        try:
            segments, info, duration = _decode(mp4)
            if not segments and settings.salvage:
                raise RuntimeError("no segments from direct decode")
        except Exception as e:
            if not settings.salvage:
                raise
            print(f"[transcribe-one] {mp4.name}: direct decode problem ({e}); "
                  f"salvaging audio via ffmpeg…", flush=True)
            wav = _extract_audio_tolerant(mp4)
            if not wav:
                raise RuntimeError(f"unsalvageable ({e})")
            segments, info, duration = _decode(wav)

        txt = mp4.with_suffix(".txt")
        header = (f"# language={info.language} duration={duration}s "
                  f"model={settings.model}{' salvaged' if wav else ''}\n\n")
        txt.write_text(header + _segments_to_txt(segments), encoding="utf-8")
        write_progress(pct=1.0, processed_sec=duration or 0.0, duration=duration,
                       language=info.language, segments=len(segments),
                       words=sum(len(s.words or []) for s in segments),
                       done=True, salvaged=bool(wav))
        return 0
    except Exception as e:
        write_progress(done=True, error=str(e)[:500])
        print(f"[transcribe-one] {mp4.name} failed: {e}", flush=True)
        _tb.print_exc()
        return 1
    finally:
        if wav is not None:
            try:
                wav.unlink(missing_ok=True)
                wav.parent.rmdir()
            except OSError:
                pass


def _segments_to_txt(segments: list) -> str:
    """Timestamped human-readable transcript.

    Format:
        [00:00:00] Hello everyone, welcome to the stream!
        [00:00:04] Tonight we are talking about fitness.
    """
    lines = []
    for seg in segments:
        text = seg.text.strip()
        if text:
            lines.append(f"[{_fmt_ts(seg.start)}] {text}")
    return "\n".join(lines)


def _segments_to_srt(segments: list) -> str:
    """Standard SRT subtitle format."""
    out = []
    for i, seg in enumerate(segments, 1):
        text = seg.text.strip()
        if not text:
            continue
        out.append(
            f"{i}\n"
            f"{_fmt_srt_ts(seg.start)} --> {_fmt_srt_ts(seg.end)}\n"
            f"{text}\n"
        )
    return "\n".join(out)


def _segments_to_json(segments: list, info) -> dict:
    return {
        "language":             info.language,
        "language_probability": round(info.language_probability, 4),
        "duration":             round(info.duration, 2),
        "segments": [
            {
                "id":    seg.id,
                "start": round(seg.start, 3),
                "end":   round(seg.end, 3),
                "text":  seg.text.strip(),
                "words": [
                    {"word": w.word, "start": round(w.start, 3),
                     "end": round(w.end, 3), "probability": round(w.probability, 4)}
                    for w in (seg.words or [])
                ],
            }
            for seg in segments
        ],
    }


# ---------------------------------------------------------------------------
# Worker


def _open_files() -> set:
    """Real paths of every file currently held open by a process this worker can
    see. Used to avoid transcribing a recording while it's still being written.
    Reads /proc/<pid>/fd; returns an empty set on any platform/permission issue
    (in which case scan() falls back to the mtime settle check alone)."""
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
                continue          # process gone or fds not visible — skip
    except Exception:
        pass
    return out


def _in_progress(path: Path, now: float, open_files: set) -> bool:
    """True if a file looks like it's still being written by a recorder, so it
    must never be transcribed, moved, or deleted. Combines a freshness check
    (mtime within the settle window) with an open-fd check (a process still has
    it open). Either signal — or being unable to stat it — means 'not safe'."""
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


async def _system_metrics() -> dict:
    """Lightweight host metrics read from /proc (no psutil dependency): CPU %,
    load average, and memory. Used by the Transcription monitoring page."""
    out: dict = {"cpu_count": os.cpu_count()}
    try:
        out["loadavg"] = [round(x, 2) for x in os.getloadavg()]
    except Exception:
        out["loadavg"] = None

    def _cpu_times():
        with open("/proc/stat") as f:
            vals = list(map(int, f.readline().split()[1:]))
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0)   # idle + iowait
        return sum(vals), idle
    try:
        t1, i1 = _cpu_times()
        await asyncio.sleep(0.12)
        t2, i2 = _cpu_times()
        dt, di = t2 - t1, i2 - i1
        out["cpu_percent"] = round(100 * (1 - di / dt), 1) if dt > 0 else None
    except Exception:
        out["cpu_percent"] = None

    try:
        mem = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, v = line.partition(":")
                mem[k] = int(v.strip().split()[0]) * 1024     # kB → bytes
        total, avail = mem.get("MemTotal"), mem.get("MemAvailable")
        out["mem_total"], out["mem_available"] = total, avail
        out["mem_percent"] = (round(100 * (1 - avail / total), 1)
                              if total and avail else None)
    except Exception:
        out["mem_total"] = out["mem_available"] = out["mem_percent"] = None
    return out


async def _remux_flv(flv: Path) -> Optional[Path]:
    """Finalize an orphaned '_flv.mp4' (an interrupted recording the recorder
    never remuxed) into a clean, correctly-named '.mp4' using a fast stream-copy,
    then return the final path. Returns None so the caller can fall back to
    transcribing the _flv directly if remux isn't possible. The source is only
    deleted once the final is verified, so a failed remux can't lose a recording.
    """
    if shutil.which("ffmpeg") is None:
        return None                       # storage box without ffmpeg → fall back
    final = flv.with_name(flv.name.replace("_flv.mp4", ".mp4"))
    if final.exists():
        # The recorder already produced the final; the _flv is a redundant leftover.
        try:
            flv.unlink()
        except OSError:
            pass
        return final
    # Temp name deliberately does NOT end in .mp4 so the scan loop can't pick it up.
    tmp = final.with_name(final.name + ".remuxing")
    try:
        tmp.unlink()
    except OSError:
        pass
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
            "-i", str(flv), "-c", "copy", "-movflags", "+faststart",
            "-f", "mp4", str(tmp),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            rc = await asyncio.wait_for(proc.wait(), timeout=600)
        except asyncio.TimeoutError:
            try:
                proc.kill(); await proc.wait()
            except ProcessLookupError:
                pass
            try: tmp.unlink()
            except OSError: pass
            return None
    except Exception:
        try: tmp.unlink()
        except OSError: pass
        return None
    if rc == 0 and tmp.exists() and tmp.stat().st_size > 0:
        try:
            os.replace(tmp, final)        # atomic; final now exists, named correctly
            try: flv.unlink()             # source verified into final → drop it
            except OSError: pass
            return final
        except OSError:
            try: tmp.unlink()
            except OSError: pass
            return None
    try: tmp.unlink()
    except OSError: pass
    return None


class TranscriptionWorker:
    def __init__(self):
        self._queue: asyncio.Queue[Path] = asyncio.Queue()

        # These sets let us answer per-filename status queries without
        # scanning the filesystem on every request.
        self._pending: set[str]     = set()   # filenames queued but not yet started
        self._processing: str | None = None    # (compat) first active filename
        self._active: dict[str, dict] = {}     # filename → live progress info

        self._completed: list[dict] = []
        self._failed:    list[dict] = []
        self._attempts:  dict[str, int]   = {}   # filename → failed-attempt count
        self._retry_after: dict[str, float] = {} # filename → earliest next-try time
        self._gave_up_count: int = 0             # files with a .error marker (cached)
        self._archived:  list[dict] = []   # recent successful archives
        self._archive_failed: list[dict] = []
        self._archive_attempts: dict[str, int] = {}     # filename → failed attempts
        self._archive_retry_after: dict[str, float] = {} # filename → earliest next try
        self._last_evict: dict = {}                      # last disk-pressure eviction

    # ── file paths ───────────────────────────────────────────────────────────

    @staticmethod
    def _txt(mp4: Path) -> Path:
        # Use mp4.with_suffix() directly — a single replacement avoids the
        # multi-dot pitfall (e.g. TK_user_2026.05.27_18-00-00.mp4)
        return mp4.with_suffix(".txt")

    @staticmethod
    def _error_marker(mp4: Path) -> Path:
        # Persistent "gave up on this file" marker, so it isn't retried forever
        # (survives worker restarts). Cleared by the retry-failed endpoint.
        return mp4.with_suffix(".error")

    @staticmethod
    def _srt(mp4: Path) -> Path:
        return mp4.with_suffix(".srt")

    @staticmethod
    def _jsn(mp4: Path) -> Path:
        return mp4.with_suffix(".json")

    # ── output ───────────────────────────────────────────────────────────────

    def _save_outputs(self, mp4: Path, segments: list, info) -> dict:
        txt = self._txt(mp4)

        # Only .txt is produced. A small header line carries language/duration
        # so that metadata isn't lost (the previous .json sidecar is gone).
        header = (f"# language={info.language} "
                  f"duration={round(info.duration, 1)}s "
                  f"model={settings.model}\n\n")
        txt.write_text(header + _segments_to_txt(segments), encoding="utf-8")

        return {
            "mp4":      str(mp4),
            "txt":      str(txt),
            "language": info.language,
            "duration": round(info.duration, 1),
            "words":    sum(len(seg.words or []) for seg in segments),
        }

    # ── queue management ─────────────────────────────────────────────────────

    def scan(self) -> int:
        """Find MP4s without a .txt and enqueue them. Returns count newly queued."""
        if not settings.transcribe_enabled:
            return 0          # storage-only mode: never enqueue for transcription
        if not settings.watch_dir.exists():
            return 0
        open_files = _open_files()      # files a recorder still has open (colocated)
        queued = 0
        now = time.time()
        for mp4 in sorted(settings.watch_dir.rglob("*.mp4")):
            if self._txt(mp4).exists():
                continue
            fname = mp4.name
            # Gave up on this file already (max attempts hit) — don't loop on it.
            if self._error_marker(mp4).exists():
                continue
            # In a back-off window after a recent failure — wait before retrying.
            if self._retry_after.get(fname, 0) > now:
                continue
            # Skip files already queued (_pending) or in-flight (_active). We check
            # _active rather than _processing because _processing holds only the most
            # recent file, so with concurrency >1 it would miss files being handled by
            # other workers — letting the same file get transcribed twice.
            if fname in self._active or fname in self._pending:
                continue
            # Decide how to treat the "_flv.mp4" intermediate the recorder
            # writes before it remuxes to the final name:
            #   • a final ".mp4" already exists  → this _flv is a leftover; skip
            #     it (the final is the one that gets transcribed)
            #   • no final exists → it's an ORPHAN from an interrupted recording;
            #     fall through and let the settle check decide. If it has stopped
            #     growing we transcribe it rather than ignoring it forever.
            # Either way, the settle check below still skips it while it's being
            # actively written.
            if fname.endswith("_flv.mp4"):
                final = mp4.with_name(fname.replace("_flv.mp4", ".mp4"))
                if final.exists() or self._txt(final).exists():
                    continue
            try:
                if now - mp4.stat().st_mtime < settings.settle_sec:
                    continue
            except OSError:
                continue
            # Hard guard: never transcribe a file a process still has open for
            # writing (i.e. a recorder is still recording it). On a colocated
            # box the recorder runs as the same user, so we can see its fds; on a
            # pure storage box nothing holds the file open, so this is a no-op.
            # This protects live recordings even if the settle window is wrong.
            try:
                if os.path.realpath(mp4) in open_files:
                    continue
            except OSError:
                continue
            self._pending.add(fname)
            self._queue.put_nowait(mp4)
            queued += 1
        # Cheap refresh of the "gave up" count for /status (once per scan).
        try:
            self._gave_up_count = sum(1 for _ in settings.watch_dir.rglob("*.error"))
        except OSError:
            pass
        return queued

    # ── 3rd-hop archive (rclone → any cloud) ─────────────────────────────────

    def _archived_marker(self, mp4: Path) -> Path:
        return mp4.with_suffix(".archived")

    def _archive_targets(self, mp4: Path) -> list[Path]:
        # A chat log is archived as itself, regardless of archive_what.
        if mp4.name.endswith("_chat.jsonl"):
            return [mp4]
        what = settings.archive_what
        out = []
        if what in ("mp4", "both"):
            out.append(mp4)
        if what in ("txt", "both"):
            t = self._txt(mp4)
            if t.exists():
                out.append(t)
        return out

    def archive_one(self, mp4: Path) -> bool:
        """Copy the configured file(s) to the rclone remote under <user>/.
        rclone verifies checksums, so success means a verified copy. Writes a
        .archived marker on success. Returns True on success."""
        if not settings.archive_remote:
            return False
        try:
            from rclone_python import rclone
        except Exception as e:
            log.warning("archive: rclone-python not available (%s)", e)
            return False
        if not rclone.is_installed():
            log.warning("archive: rclone binary not installed — skipping")
            return False

        username = mp4.parent.name
        dest = f"{settings.archive_remote.rstrip('/')}/{username}/"
        args = ["--transfers", str(settings.archive_transfers)]
        if settings.archive_bwlimit:
            args += ["--bwlimit", settings.archive_bwlimit]

        def _copy(src: str, dst: str) -> None:
            try:
                rclone.copy(src, dst, show_progress=False, args=args)
            except TypeError:                    # older rclone-python without args/kwargs
                rclone.copy(src, dst)

        try:
            for f in self._archive_targets(mp4):
                _copy(str(f), dest)              # checksum-verified by rclone
            marker = self._archived_marker(mp4)
            marker.write_text(f"archived_at={time.time()} remote={dest}\n")
            self._archived.insert(0, {"filename": mp4.name, "remote": dest,
                                      "archived_at": time.time()})
            self._archived = self._archived[:20]
            log.info("archived %s → %s", mp4.name, dest)
            return True
        except Exception as e:
            log.error("archive failed for %s: %s", mp4.name, e)
            self._archive_failed.insert(0, {"filename": mp4.name,
                                            "error": str(e)[:300],
                                            "failed_at": time.time()})
            self._archive_failed = self._archive_failed[:20]
            return False

    def _remote_has(self, mp4: Path) -> bool:
        """Confirm the archived copy is still on the remote before deleting the local
        one. The `.archived` marker already means rclone made a checksum-verified copy,
        so presence-by-name is sufficient. A reported size that doesn't match (e.g. an
        end-to-end-encrypted backend that lists sizes differently) is logged but does
        NOT block eviction — otherwise a backend quirk silently lets the disk fill to
        100%. Only a genuinely-absent file (or a listing error) keeps the local copy."""
        try:
            from rclone_python import rclone
            username = mp4.parent.name
            listing = rclone.ls(f"{settings.archive_remote.rstrip('/')}/{username}/")
            for item in (listing or []):
                if item.get("Name") != mp4.name:
                    continue
                size = item.get("Size")
                try:
                    local = mp4.stat().st_size
                    rsize = int(size) if size is not None else None
                    # A remote copy SMALLER than local looks truncated/incomplete —
                    # keep the local file. An equal-or-larger size is fine (an E2E
                    # backend may report a larger, encrypted size).
                    if rsize is not None and rsize < local:
                        log.warning("evict verify: %s remote size %s < local %s — keeping local",
                                    mp4.name, rsize, local)
                        return False
                except (OSError, ValueError, TypeError):
                    pass
                return True                  # present on the remote, not smaller
            return False                     # not found on remote — keep the local copy
        except Exception as e:
            log.warning("archive verify failed for %s: %s", mp4.name, e)
            return False

    def _archive_with_backoff(self, f: Path, now: float) -> None:
        """Archive one file, honouring a per-file exponential backoff so a
        persistently-failing file (auth/network) stops being retried every scan."""
        name = f.name
        if self._archive_retry_after.get(name, 0) > now:
            return
        if self.archive_one(f):
            self._archive_attempts.pop(name, None)
            self._archive_retry_after.pop(name, None)
        else:
            a = self._archive_attempts.get(name, 0) + 1
            self._archive_attempts[name] = a
            backoff = min(3600, settings.archive_retry_backoff_sec * (2 ** (a - 1)))
            self._archive_retry_after[name] = now + backoff

    def _evict_if_pressured(self) -> None:
        """Disk-aware eviction. Only when usage crosses the high-water mark, delete
        the verified-on-cloud local copies of the OLDEST archived recordings until
        usage drops to the low-water mark. Files younger than the min-age are kept
        hot. Never deletes a file not confirmed present on the remote."""
        if not settings.archive_delete_local:
            self._last_evict = {"at": time.time(), "reason": "disabled", "evicted": 0}
            return
        try:
            du = shutil.disk_usage(settings.watch_dir)
        except Exception:
            return
        used_pct = 100 * (1 - du.free / du.total) if du.total else 0.0
        now = time.time()
        min_age = max(settings.archive_delete_delay_sec, settings.archive_evict_min_age_sec)
        # Survey every recording so the diagnostic can explain why nothing was freed.
        total_mp4 = archived = skipped_young = 0
        cands: list[tuple[float, Path]] = []
        for mp4 in settings.watch_dir.rglob("*.mp4"):
            if mp4.name.endswith("_flv.mp4"):
                continue
            total_mp4 += 1
            marker = self._archived_marker(mp4)
            if not marker.exists():
                continue                        # not on the cloud yet → not evictable
            archived += 1
            try:
                if (now - marker.stat().st_mtime) < min_age:
                    skipped_young += 1          # archived too recently — kept hot
                    continue
                cands.append((mp4.stat().st_mtime, mp4))
            except OSError:
                continue
        diag = {"at": now, "evicted": 0, "freed_bytes": 0,
                "disk_pct_before": round(used_pct, 1),
                "high_pct": settings.archive_evict_high_pct,
                "low_pct": settings.archive_evict_low_pct,
                "min_age_h": round(min_age / 3600, 2),
                "total_mp4": total_mp4, "archived": archived,
                "skipped_young": skipped_young, "candidates": len(cands),
                "skipped_remote": 0}
        if used_pct < settings.archive_evict_high_pct:
            diag["reason"] = "disk_below_high"   # plenty of room — keep everything hot
            self._last_evict = diag
            return
        cands.sort()                            # oldest recording first
        target_free = du.total * (1 - settings.archive_evict_low_pct / 100)
        freed = evicted = skipped_remote = 0
        for _, mp4 in cands:
            if (du.free + freed) >= target_free:
                break                           # back under the low-water mark
            if not self._remote_has(mp4):
                skipped_remote += 1             # not confirmed on cloud — never delete
                continue
            try:
                sz = mp4.stat().st_size
                mp4.unlink()
                freed += sz
                evicted += 1
                log.info("evict: removed local %s (disk %.0f%%, on remote)",
                         mp4.name, used_pct)
            except OSError as e:
                log.warning("evict: could not delete %s: %s", mp4.name, e)
        if evicted:
            log.info("evict: freed %s across %d file(s) (disk was %.0f%%)",
                     f"{freed:,}", evicted, used_pct)
        else:
            log.info("evict: nothing freed at %.0f%% — archived=%d candidates=%d "
                     "skipped_young=%d skipped_remote=%d min_age=%.1fh",
                     used_pct, archived, len(cands), skipped_young, skipped_remote,
                     min_age / 3600)
        diag.update({"evicted": evicted, "freed_bytes": freed,
                     "skipped_remote": skipped_remote,
                     "reason": "ran" if evicted else "no_eligible_files"})
        self._last_evict = diag

    def archive_sweep(self) -> None:
        """Archive transcribed-but-unarchived files (with per-file backoff), then
        evict cold local copies if the disk is under pressure. Runs in a worker
        thread (see scan_loop) so a slow upload never blocks the event loop."""
        if not settings.archive_remote or not settings.watch_dir.exists():
            return
        now = time.time()
        # Chat logs: archive as soon as they exist; never auto-evicted (tiny).
        for chat in sorted(settings.watch_dir.rglob("*_chat.jsonl")):
            if not self._archived_marker(chat).exists():
                self._archive_with_backoff(chat, now)
        # Recordings: archive once transcribed (have a .txt).
        for mp4 in sorted(settings.watch_dir.rglob("*.mp4")):
            if self._archived_marker(mp4).exists():
                continue
            if self._txt(mp4).exists():
                self._archive_with_backoff(mp4, now)
        # Keep hot / evict cold under disk pressure.
        self._evict_if_pressured()
        # Age-based lifecycle (proactive evict / purge), independent of pressure.
        self._retention_sweep()

    def _retention_sweep(self) -> None:
        """Age-based lifecycle, keyed on the .archived marker (so it covers evicted
        cloud-only files too). Both keep the transcript + chat:
          • evict: after retention_evict_days, drop the local .mp4 (stays on cloud).
          • purge: after retention_purge_days, delete the cloud copy + local .mp4."""
        if not settings.archive_remote:
            return
        ev_days, pg_days = settings.retention_evict_days, settings.retention_purge_days
        if not ev_days and not pg_days:
            return
        now = time.time()
        for marker in settings.watch_dir.rglob("*.archived"):
            mp4 = marker.with_suffix(".mp4")
            try:
                age_d = (now - marker.stat().st_mtime) / 86400.0
            except OSError:
                continue
            if pg_days and age_d >= pg_days:
                self._purge_recording(mp4, marker, age_d)
            elif ev_days and age_d >= ev_days and mp4.exists():
                if self._remote_has(mp4):
                    try:
                        mp4.unlink()
                        log.info("retention: evicted local %s (age %.1fd, on cloud)",
                                 mp4.name, age_d)
                    except OSError as e:
                        log.warning("retention: could not evict %s: %s", mp4.name, e)

    def _purge_recording(self, mp4: Path, marker: Path, age_d: float) -> None:
        """Delete the cloud copy + any local .mp4 for an old recording, keeping the
        transcript and chat. Only drops local once the cloud delete succeeds (so we
        never lose the last copy on an rclone hiccup)."""
        username = mp4.parent.name
        remote = f"{settings.archive_remote.rstrip('/')}/{username}/{mp4.name}"
        try:
            subprocess.run(["rclone", "deletefile", remote],
                           timeout=120, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            err = (e.stderr or b"").decode("utf-8", errors="replace").strip()[-300:]
            log.warning("retention purge: rclone deletefile failed for %s: %s", mp4.name, err or e)
            return
        except Exception as e:
            log.warning("retention purge: rclone deletefile failed for %s: %s", mp4.name, e)
            return
        try:
            if mp4.exists():
                mp4.unlink()
        except OSError:
            pass
        try:                       # mark purged so it isn't re-processed; keep txt/chat
            marker.rename(marker.with_suffix(".purged"))
        except OSError:
            try:
                marker.unlink()
            except OSError:
                pass
        log.info("retention: purged video %s (cloud+local, age %.1fd) — kept transcript/chat",
                 mp4.name, age_d)

    def _record_failure(self, mp4: Path, fname: str, rc, final: dict,
                        attempt: int, err_path=None):
        """Record a failed attempt: back off and retry up to max_attempts, then
        give up with a persistent .error marker so the file isn't retried forever.
        Captures the subprocess's stderr tail (traceback) for debugging."""
        tail = ""
        if err_path:
            try:
                tail = Path(err_path).read_text(errors="replace").strip()[-2000:]
            except OSError:
                pass
        base = (final.get("error") or
                ("killed (timeout/stall)" if rc == -9 else f"exit {rc}"))
        now = time.time()
        self._attempts[fname] = attempt
        if attempt >= settings.max_attempts:
            try:
                self._error_marker(mp4).write_text(
                    f"gave up after {attempt} attempt(s) at {time.ctime(now)}\n"
                    f"reason: {base}\n\n{tail}\n")
            except OSError:
                pass
            self._retry_after.pop(fname, None)
            self._gave_up_count += 1
            log.error("Giving up on %s after %d attempts — %s", fname, attempt, base)
            self._failed.insert(0, {"mp4": str(mp4), "error": base, "detail": tail,
                                    "attempts": attempt, "permanent": True,
                                    "failed_at": now})
        else:
            backoff = min(3600, settings.retry_backoff_sec * (2 ** (attempt - 1)))
            self._retry_after[fname] = now + backoff
            log.warning("Failed %s (attempt %d/%d) — %s; retrying in %ds",
                        fname, attempt, settings.max_attempts, base, backoff)
            self._failed.insert(0, {"mp4": str(mp4), "error": base, "detail": tail,
                                    "attempts": attempt, "permanent": False,
                                    "retry_in_sec": backoff, "failed_at": now})
        del self._failed[200:]

    def retry_failed(self) -> int:
        """Clear all give-up markers and reset attempt/back-off state so the next
        scan re-enqueues everything that previously failed. Returns the number of
        markers cleared."""
        cleared = 0
        try:
            for m in settings.watch_dir.rglob("*.error"):
                try:
                    m.unlink(); cleared += 1
                except OSError:
                    pass
        except OSError:
            pass
        self._attempts.clear()
        self._retry_after.clear()
        self._gave_up_count = 0
        log.info("retry-failed: cleared %d marker(s); will re-attempt", cleared)
        return cleared

    async def run_queue(self):
        """One queue consumer. Several of these run concurrently (settings.
        concurrency). Each transcribes a file in an isolated subprocess and
        monitors its progress sidecar, with stall flagging and a hard timeout."""
        import json as _json
        while True:
            try:
                mp4 = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            fname = mp4.name
            orig_fname = fname
            # If this is an orphaned '_flv.mp4' (interrupted recording the recorder
            # never finalized), remux it into a clean, correctly-named '.mp4' first
            # and transcribe that instead — so we get a playable file and a properly
            # named transcript. Falls back to the _flv itself if remux can't run.
            # NOTE: we keep orig_fname in _pending across the (awaited) remux so the
            # scan loop can't re-enqueue the same _flv while we're finalizing it.
            if fname.endswith("_flv.mp4"):
                final = await _remux_flv(mp4)
                if final is not None:
                    log.info("Finalized orphan recording: %s -> %s",
                             fname, final.name)
                    mp4 = final
                    fname = mp4.name
            self._pending.discard(orig_fname)
            self._pending.discard(fname)
            if self._txt(mp4).exists():
                self._queue.task_done()
                continue

            started = time.time()
            prog = _progress_path(mp4)
            prog.unlink(missing_ok=True)
            self._active[fname] = {"filename": fname, "started": started,
                                   "pct": 0.0, "processed_sec": 0.0,
                                   "duration": None, "stalled": False,
                                   "elapsed": 0.0}
            self._processing = fname
            attempt = self._attempts.get(fname, 0) + 1
            safe = attempt >= settings.max_attempts and settings.max_attempts > 1
            errf = tempfile.NamedTemporaryFile(prefix="tw-err-", delete=False)
            log.info("Transcribing: %s (attempt %d/%d%s)",
                     fname, attempt, settings.max_attempts, ", safe mode" if safe else "")
            proc = None
            try:
                args = [sys.executable, str(Path(__file__).resolve()),
                        "--transcribe-one", str(mp4)]
                if safe:
                    args.append("--safe")
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=errf,
                    preexec_fn=os.setsid,
                )
                last_progress_pct = 0.0
                last_progress_at = time.time()
                while True:
                    try:
                        rc = await asyncio.wait_for(proc.wait(), timeout=2.0)
                        break
                    except asyncio.TimeoutError:
                        pass  # still running — poll the sidecar
                    info = self._active[fname]
                    info["elapsed"] = round(time.time() - started, 1)
                    try:
                        p = _json.loads(prog.read_text())
                        info["pct"] = p.get("pct", info["pct"]) or info["pct"]
                        info["processed_sec"] = p.get("processed_sec", info["processed_sec"])
                        info["duration"] = p.get("duration", info["duration"])
                        if info["pct"] > last_progress_pct + 1e-6:
                            last_progress_pct = info["pct"]
                            last_progress_at = time.time()
                    except (OSError, ValueError):
                        pass
                    # Stall flag
                    info["stalled"] = (time.time() - last_progress_at) > settings.stall_sec
                    # Hard timeout
                    limit = settings.file_timeout_sec
                    if limit <= 0:
                        dur = info["duration"]
                        limit = int(dur * 10 + 300) if dur else 7200
                    if (time.time() - started) > limit:
                        log.error("Timeout (%ds) transcribing %s — killing", limit, fname)
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except (ProcessLookupError, PermissionError):
                            pass
                        await proc.wait()
                        rc = -9
                        break

                # Interpret result
                final = {}
                try:
                    final = _json.loads(prog.read_text())
                except (OSError, ValueError):
                    pass
                if rc == 0 and self._txt(mp4).exists() and not final.get("error"):
                    elapsed = round(time.time() - started, 1)
                    self._completed.insert(0, {
                        "mp4": str(mp4), "txt": str(self._txt(mp4)),
                        "language": final.get("language"),
                        "duration": final.get("duration"),
                        "words": final.get("words"),
                        "elapsed_sec": elapsed, "completed_at": time.time()})
                    del self._completed[500:]
                    self._attempts.pop(fname, None)
                    self._retry_after.pop(fname, None)
                    self._error_marker(mp4).unlink(missing_ok=True)
                    log.info("Done: %s dur=%ss elapsed=%ss",
                             fname, final.get("duration"), elapsed)
                else:
                    self._record_failure(mp4, fname, rc, final, attempt, errf.name)
            except Exception as e:
                self._record_failure(mp4, fname, None, {"error": str(e)},
                                     attempt, errf.name)
                if proc and proc.returncode is None:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
            finally:
                prog.unlink(missing_ok=True)
                try:
                    errf.close()
                    os.unlink(errf.name)
                except OSError:
                    pass
                self._active.pop(fname, None)
                self._processing = next(iter(self._active), None)
                self._queue.task_done()

    async def scan_loop(self):
        while True:
            try:
                n = self.scan()
                if n:
                    log.info("Enqueued %d file(s) for transcription", n)
            except Exception:
                log.exception("scan error")
            # 3rd-hop archive sweep (no-op unless ARCHIVE_REMOTE is set). Run off
            # the event loop so a large/slow rclone upload can't block health
            # checks, file receives, or transcription monitoring.
            try:
                await asyncio.to_thread(self.archive_sweep)
            except Exception:
                log.exception("archive sweep error")
            await asyncio.sleep(settings.scan_interval)

    # ── status helpers ───────────────────────────────────────────────────────

    def status_of(self, filename: str) -> str:
        """Return 'done'|'processing'|'pending'|'none' for a given MP4 filename."""
        mp4 = self._find_mp4(filename)
        if mp4 and self._txt(mp4).exists():
            return "done"
        if filename in self._active:
            return "processing"
        if filename in self._pending:
            return "pending"
        return "none"

    def _find_mp4(self, filename: str) -> Path | None:
        """Locate an MP4 by filename within the watch dir."""
        for p in settings.watch_dir.rglob(filename):
            return p
        return None

    def all_statuses(self) -> dict[str, str]:
        """Return {filename: status} for every MP4 known to the worker."""
        statuses: dict[str, str] = {}
        if not settings.watch_dir.exists():
            return statuses
        for mp4 in settings.watch_dir.rglob("*.mp4"):
            fname = mp4.name
            if self._txt(mp4).exists():
                statuses[fname] = "done"
            elif fname in self._active:
                statuses[fname] = "processing"
            elif fname in self._pending:
                statuses[fname] = "pending"
            # omit "none" — callers treat missing key as "none"
        return statuses

    def _throughput(self) -> dict:
        """Rolling transcription throughput from recent completions."""
        now = time.time()
        last_hr = [c for c in self._completed
                   if now - c.get("completed_at", 0) <= 3600]
        audio = sum((c.get("duration") or 0) for c in last_hr)
        wall  = sum((c.get("elapsed_sec") or 0) for c in last_hr)
        return {
            "completed_last_hour": len(last_hr),
            "audio_sec_last_hour": round(audio, 1),
            "avg_speed": round(audio / wall, 2) if wall > 0 else None,
        }

    @property
    def status(self) -> dict:
        import shutil as _sh
        disk_free = disk_total = None
        try:
            du = _sh.disk_usage(str(settings.watch_dir))
            disk_free, disk_total = du.free, du.total
        except Exception:
            pass
        return {
            "model":             settings.model,
            "device":            settings.device,
            "watch_dir":         str(settings.watch_dir),
            "scan_interval_sec": settings.scan_interval,
            "queue_depth":       len(self._pending),
            "current_file":      self._processing,
            "concurrency":       settings.concurrency,
            "transcribe_enabled": settings.transcribe_enabled,
            "vad":               settings.vad,
            "beam_size":         settings.beam_size,
            "active":            [dict(a) for a in self._active.values()],
            "stall_sec":         settings.stall_sec,
            "completed_count":   len(self._completed),
            "failed_count":      len(self._failed),
            "gave_up":           self._gave_up_count,
            "max_attempts":      settings.max_attempts,
            "disk_free_bytes":   disk_free,
            "disk_total_bytes":  disk_total,
            "archive_remote":    settings.archive_remote or None,
            "archive_what":      settings.archive_what if settings.archive_remote else None,
            "archive_delete_local": settings.archive_delete_local if settings.archive_remote else None,
            "archived_count":    len(self._archived),
            "archive_failed_count": len(self._archive_failed),
            "archive_evict_high_pct": settings.archive_evict_high_pct if settings.archive_remote else None,
            "archive_evict_low_pct": settings.archive_evict_low_pct if settings.archive_remote else None,
            "archive_evict_min_age_sec": settings.archive_evict_min_age_sec if settings.archive_remote else None,
            "retention_evict_days": settings.retention_evict_days if settings.archive_remote else None,
            "retention_purge_days": settings.retention_purge_days if settings.archive_remote else None,
            "last_evict":        self._last_evict or None,
        }


worker = TranscriptionWorker()


# ---------------------------------------------------------------------------
# App

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not settings.watch_dir.exists():
        log.warning("Watch dir %s does not exist — creating", settings.watch_dir)
        settings.watch_dir.mkdir(parents=True, exist_ok=True)
    log.info("Watching %s every %ds | model=%s | auth=%s",
             settings.watch_dir, settings.scan_interval, settings.model,
             "enabled" if settings.auth_token else "DISABLED (set WHISPER_AUTH_TOKEN)")
    if settings.transcribe_enabled:
        for i in range(settings.concurrency):
            asyncio.create_task(worker.run_queue(), name=f"queue-{i}")
        log.info("started %d transcription worker(s)", settings.concurrency)
    else:
        log.info("transcription DISABLED (WHISPER_TRANSCRIBE=0) — storage-only mode")
    asyncio.create_task(worker.scan_loop(),  name="scanner")
    yield


app = FastAPI(title="Whisper Transcription Worker", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints

STORAGE_UI_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>tt-storage node</title>
<style>
  :root{
    --bg:#0f1216; --panel:#171b21; --panel2:#1b2027; --line:#262b33; --line2:#313844;
    --txt:#e6e9ed; --mut:#8a93a0; --mut2:#5d6675;
    --ok:#34d3a6; --ok-dim:#1f6f59; --amber:#f5b14c; --red:#ff6b6b; --cyan:#5fcfe8;
    --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
    --ui:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
  }
  *{box-sizing:border-box}
  html,body{margin:0}
  body{font-family:var(--ui);background:var(--bg);color:var(--txt);font-size:14px;-webkit-font-smoothing:antialiased}
  a{color:var(--cyan)}
  .wrap{max-width:1180px;margin:0 auto;padding:18px 20px 60px}
  .mono{font-family:var(--mono)}
  .mut{color:var(--mut)}
  /* topbar */
  header{display:flex;align-items:center;gap:14px;flex-wrap:wrap;border-bottom:1px solid var(--line);padding-bottom:14px;margin-bottom:18px}
  header h1{font-size:17px;margin:0;font-weight:700;letter-spacing:-.01em}
  header h1 span{color:var(--ok)}
  .wd{font-family:var(--mono);font-size:12px;color:var(--mut)}
  .grow{flex:1}
  .pill{font-family:var(--mono);font-size:11px;padding:3px 10px;border-radius:20px;border:1px solid var(--line2);color:var(--mut)}
  .pill.on{color:var(--ok);border-color:var(--ok-dim);background:rgba(52,211,166,.08)}
  .pill.off{color:var(--amber);border-color:#5a481f;background:rgba(245,177,76,.08)}
  button{font-family:var(--ui);font-size:13px;background:#222834;color:var(--txt);border:1px solid var(--line2);
    border-radius:8px;padding:6px 12px;cursor:pointer}
  button:hover{border-color:#3d4756}
  button.primary{background:var(--ok-dim);border-color:var(--ok-dim);color:#eafff7}
  button.ghost{background:transparent}
  button:disabled{opacity:.5;cursor:default}
  .toggle{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--mut);font-family:var(--mono)}
  /* banners */
  .banner{border-radius:8px;padding:9px 13px;margin-bottom:14px;font-size:13px;font-family:var(--mono)}
  .banner.demo{background:rgba(95,207,232,.08);border:1px solid #244b56;color:#bfeaf5}
  .banner.err{background:rgba(255,107,107,.08);border:1px solid #5a2a2a;color:#ffc2c2}
  /* vitals */
  .vitals{display:grid;grid-template-columns:repeat(6,1fr);gap:11px;margin-bottom:14px}
  .v{background:var(--panel);border:1px solid var(--line);border-radius:11px;padding:12px 13px}
  .v .l{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--mut)}
  .v .n{font-family:var(--mono);font-size:27px;font-weight:600;margin-top:6px;line-height:1}
  .v .n small{font-size:14px;color:var(--mut)}
  /* panel + bars */
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:15px 16px;margin-bottom:14px}
  .panel h2{font-size:11px;font-family:var(--mono);letter-spacing:.16em;text-transform:uppercase;color:var(--mut);margin:0 0 12px}
  .sysgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}
  .bar .bl{display:flex;justify-content:space-between;font-family:var(--mono);font-size:11px;color:var(--mut)}
  .track{height:6px;border-radius:4px;background:#0a0d11;margin-top:6px;overflow:hidden}
  .fill{height:100%;border-radius:4px;background:var(--ok);transition:width .6s}
  .fill.warn{background:var(--amber)} .fill.bad{background:var(--red)}
  .cfg{font-family:var(--mono);font-size:12px;color:var(--mut);display:flex;gap:16px;flex-wrap:wrap;margin-top:4px}
  .cfg b{color:var(--txt);font-weight:600}
  /* lists */
  .cols{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .row{display:flex;justify-content:space-between;gap:10px;font-family:var(--mono);font-size:12px;padding:6px 0;border-bottom:1px dashed var(--line)}
  .row:last-child{border-bottom:0}
  .row .f{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .ok2{color:var(--ok)} .warn2{color:var(--amber)} .bad2{color:var(--red)}
  .prog{height:6px;border-radius:4px;background:#0a0d11;margin-top:6px;overflow:hidden}
  .prog > i{display:block;height:100%;background:var(--ok);width:0}
  .empty{color:var(--mut2);font-family:var(--mono);font-size:12px;padding:8px 0}
  /* files table */
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{font-family:var(--mono);font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--mut);text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
  td{padding:6px 8px;border-bottom:1px solid var(--line);font-family:var(--mono)}
  td.act{text-align:right;white-space:nowrap}
  td.act button{padding:3px 8px;font-size:11px;margin-left:5px}
  tr:hover td{background:#1b2027}
  /* login */
  .overlay{position:fixed;inset:0;background:rgba(8,10,13,.9);display:none;align-items:center;justify-content:center;z-index:10}
  .overlay.show{display:flex}
  .login{background:var(--panel);border:1px solid var(--line2);border-radius:14px;padding:24px;width:340px}
  .login h3{margin:0 0 4px;font-size:16px}
  .login p{margin:0 0 14px;color:var(--mut);font-size:12px}
  .login input{width:100%;background:#0c0f13;border:1px solid var(--line2);border-radius:8px;color:var(--txt);
    font-family:var(--mono);font-size:13px;padding:9px 11px;margin-bottom:12px}
  .login button{width:100%}
  @media(max-width:820px){.vitals{grid-template-columns:repeat(3,1fr)}.cols,.sysgrid{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>tt<span>·</span>storage</h1>
    <span class="wd" id="wd"></span>
    <span class="grow"></span>
    <span class="pill" id="tx-pill">—</span>
    <span class="pill mono" id="build">—</span>
    <label class="toggle"><input type="checkbox" id="auto" checked> auto</label>
    <button class="ghost" onclick="loadAll()">Refresh</button>
    <button class="ghost" id="logout" onclick="logout()" style="display:none">Log out</button>
  </header>

  <div id="banner"></div>

  <div class="vitals" id="vitals"></div>

  <div class="panel">
    <h2>Host</h2>
    <div class="sysgrid" id="sys"></div>
    <div class="cfg" id="cfg" style="margin-top:14px"></div>
    <div id="pwline" style="margin-top:12px;font-family:var(--mono);font-size:12px;color:var(--mut);display:flex;align-items:center;gap:8px"></div>
  </div>

  <div class="panel">
    <h2>Active transcriptions</h2>
    <div style="display:flex;gap:8px;margin-bottom:10px">
      <button onclick="doScan()">Scan now</button>
      <button id="retry" onclick="doRetry()">↻ Retry given-up</button>
    </div>
    <div id="active"></div>
  </div>

  <div class="cols">
    <div class="panel"><h2>Recent completed</h2><div id="done"></div></div>
    <div class="panel"><h2>Recent failures</h2><div id="fail"></div></div>
  </div>

  <div class="panel">
    <h2>Files</h2>
    <div id="files"></div>
  </div>
</div>

<div class="overlay" id="overlay">
  <div class="login">
    <h3>Storage node access</h3>
    <p>Enter this node's password (default: <span class="mono">yourpassword</span>).</p>
    <input id="token" type="password" placeholder="password" onkeydown="if(event.key==='Enter')saveToken()">
    <button class="primary" onclick="saveToken()">Connect</button>
  </div>
</div>

<script>
const $=id=>document.getElementById(id);
const KEY='tt_storage_token';
let DEMO=false, timer=null, AUTHREQ=true, selected=new Set(), FILES=[], _pw='';

const fmtB=n=>{if(n==null)return'–';const u=['B','KB','MB','GB','TB'];let i=0,v=n;while(v>=1024&&i<u.length-1){v/=1024;i++}return v.toFixed(v<10&&i?1:0)+' '+u[i]};
const fmtDur=s=>{if(s==null)return'–';s=Math.round(s);const h=s/3600|0,m=(s%3600)/60|0,x=s%60;return(h?h+'h':'')+(m||h?m+'m':'')+x+'s'};
const ago=t=>{if(!t)return'';const d=Math.max(0,Date.now()/1000-t);if(d<60)return(d|0)+'s';if(d<3600)return(d/60|0)+'m';if(d<86400)return(d/3600|0)+'h';return(d/86400|0)+'d'};
const esc=s=>(s==null?'':String(s)).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const cls=v=>v>=90?'bad':v>=75?'warn':'';

function token(){return localStorage.getItem(KEY)||''}
function showLogin(){$('overlay').classList.add('show');$('token').focus()}
function hideLogin(){$('overlay').classList.remove('show')}
function saveToken(){const t=$('token').value.trim();if(!t)return;localStorage.setItem(KEY,t);hideLogin();$('logout').style.display='';loadAll()}
function logout(){localStorage.removeItem(KEY);location.reload()}

async function api(path,opts){
  opts=opts||{};opts.headers=Object.assign({'Authorization':'Bearer '+token()},opts.headers||{});
  const r=await fetch(path,opts);
  if(r.status===401){showLogin();throw new Error('unauthorized')}
  if(!r.ok)throw new Error('http '+r.status);
  return r;
}

async function loadAll(){
  if(DEMO){renderAll(DEMO_DETAIL,DEMO_FILES);return}
  if(AUTHREQ && !token()){showLogin();return}
  try{
    const d=await(await api('/transcription-detail')).json();
    let files=[];try{files=await(await api('/files/inventory')).json()}catch(e){}
    renderAll(d,files);
    $('logout').style.display='';
    $('banner').innerHTML='';
  }catch(e){
    if(String(e.message)!=='unauthorized')
      $('banner').innerHTML='<div class="banner err">Could not reach this node: '+esc(e.message)+'</div>';
  }
}

function renderAll(d,files){
  const sys=d.system||{}, tp=d.throughput||{};
  $('wd').textContent=d.watch_dir||'';
  $('build').textContent=d.build||'';
  const on=d.transcribe_enabled!==false;
  $('tx-pill').className='pill '+(on?'on':'off');
  $('tx-pill').textContent=on?'transcribing':'transcribe off';

  const diskUsed=(d.disk_total_bytes&&d.disk_free_bytes!=null)?(1-d.disk_free_bytes/d.disk_total_bytes)*100:null;
  const vit=[
    ['Queue',d.queue_depth??0,''],
    ['Active',(d.active||[]).length,''],
    ['Done / hr',tp.completed_last_hour??0,''],
    ['Speed',tp.avg_speed!=null?tp.avg_speed+'×':'—',''],
    ['Gave up',d.gave_up??0,(d.gave_up>0?'bad2':'')],
    ['Archived',d.archived_count??0,''],
  ];
  $('vitals').innerHTML=vit.map(([l,n,c])=>`<div class="v"><div class="l">${l}</div><div class="n ${c}">${n}</div></div>`).join('');

  const bar=(l,v,extra)=>`<div class="bar"><div class="bl"><span>${l}</span><span>${v==null?'–':v+(extra||'')}</span></div>
    <div class="track"><div class="fill ${v==null?'':cls(v)}" style="width:${v==null?0:Math.min(100,v)}%"></div></div></div>`;
  $('sys').innerHTML=
    bar('CPU',sys.cpu_percent,'%')+
    bar('Memory',sys.mem_percent,'%')+
    bar('Disk',diskUsed!=null?Math.round(diskUsed):null,'%')+
    `<div class="bar"><div class="bl"><span>Load</span><span>${(sys.loadavg||['–']).join(' ')}</span></div>
      <div class="track"><div class="fill ${cls(sys.loadavg?100*sys.loadavg[0]/(sys.cpu_count||1):0)}" style="width:${sys.loadavg?Math.min(100,100*sys.loadavg[0]/(sys.cpu_count||1)):0}%"></div></div></div>`;

  $('cfg').innerHTML=[
    ['model',d.model],['device',d.device],['concurrency',d.concurrency],
    ['vad',d.vad?'on':'off'],['beam',d.beam_size],['scan',`${d.scan_interval_sec}s`],
    ['disk free',fmtB(d.disk_free_bytes)],
    d.archive_remote?['archive',d.archive_remote]:null,
  ].filter(Boolean).map(([k,v])=>`<span>${k} <b>${esc(v)}</b></span>`).join('');
  renderPwLine();

  const act=d.active||[];
  $('active').innerHTML=act.length?act.map(a=>{
    let pct=a.pct||0; if(pct<=1)pct*=100; pct=Math.max(0,Math.min(100,pct));
    const eta=(a.elapsed&&pct>1)?fmtDur(a.elapsed*(100-pct)/pct):'…';
    return `<div style="margin-bottom:10px">
      <div class="row" style="border:0;padding:0"><span class="f">${esc((a.filename||'').split('/').pop())}${a.stalled?' <span class="bad2">stalled</span>':''}</span>
        <span class="mut">${pct.toFixed(0)}% · eta ${eta}</span></div>
      <div class="prog"><i style="width:${pct}%;${a.stalled?'background:var(--red)':''}"></i></div></div>`;
  }).join(''):'<div class="empty">idle — nothing transcribing</div>';

  const dn=d.recent_completed||[];
  $('done').innerHTML=dn.length?dn.map(c=>{
    const sp=(c.duration&&c.elapsed_sec)?(c.duration/c.elapsed_sec).toFixed(1)+'×':'';
    return `<div class="row"><span class="f">${esc((c.mp4||'').split('/').pop())}</span>
      <span class="ok2">✓ ${fmtDur(c.duration)} ${sp}</span></div>`;
  }).join(''):'<div class="empty">none yet</div>';

  const fl=d.recent_failed||[];
  $('fail').innerHTML=fl.length?fl.map(f=>{
    const tag=f.permanent?'<span class="bad2">gave up</span>':`<span class="warn2">retry ${f.attempts||1}/${d.max_attempts||3}</span>`;
    const det=f.detail?` title="${esc((f.detail||'').slice(-400))}"`:'';
    return `<div class="row" style="display:block"${det}>
      <div style="display:flex;justify-content:space-between"><span class="f">${esc((f.mp4||'').split('/').pop())}</span>${tag}</div>
      <div class="mut" style="font-size:11px">${esc(f.error||'')}</div></div>`;
  }).join(''):'<div class="empty">no failures</div>';

  files=files||[]; FILES=files.slice(0,200);
  $('files').innerHTML=FILES.length?
    `<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
       <button id="bulkdl" onclick="downloadSelected()" disabled>Download selected</button>
       <button class="ghost" id="bulkclear" onclick="clearSel()" style="display:none">Clear</button>
       <span class="mut" id="selcount" style="font-family:var(--mono);font-size:12px"></span></div>
     <table><thead><tr>
       <th style="width:22px"><input type="checkbox" id="selall" onchange="toggleAll(this.checked)"></th>
       <th>creator</th><th>file</th><th>size</th><th>age</th><th></th></tr></thead>
    <tbody>${FILES.map((f,i)=>`<tr>
      <td><input type="checkbox" class="rowsel" data-i="${i}" onchange="selToggle(${i},this.checked)" ${selected.has(f.path)?'checked':''}></td>
      <td class="mut">${esc(f.username)}</td>
      <td class="f">${esc(f.filename)}</td>
      <td>${fmtB(f.size_bytes)}</td>
      <td class="mut">${ago(f.mtime)}</td>
      <td class="act">
        <button onclick="dl('${encodeURIComponent(f.path)}','${esc(f.filename)}')">↓</button>
        <button onclick="del('${encodeURIComponent(f.path)}','${esc(f.filename)}')">✕</button>
      </td></tr>`).join('')}</tbody></table>`
    :'<div class="empty">no files</div>';
  syncBulk();
}

async function doScan(){if(DEMO)return;try{await api('/scan',{method:'POST'});setTimeout(loadAll,600)}catch(e){}}
async function doRetry(){if(DEMO)return;const b=$('retry');b.disabled=true;b.textContent='…';try{await api('/retry-failed',{method:'POST'})}catch(e){}b.disabled=false;b.textContent='↻ Retry given-up';setTimeout(loadAll,600)}
async function dl(p,name){
  if(DEMO)return;
  try{const r=await api('/files/raw?path='+p);const b=await r.blob();const u=URL.createObjectURL(b);
    const a=document.createElement('a');a.href=u;a.download=name;a.click();URL.revokeObjectURL(u);}catch(e){alert('download failed: '+e.message)}
}
async function del(p,name){
  if(DEMO)return;
  if(!confirm('Delete '+name+' from this node? This cannot be undone.'))return;
  try{await api('/files/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({paths:[decodeURIComponent(p)]})});loadAll()}catch(e){alert('delete failed: '+e.message)}
}

function selToggle(i,checked){const f=FILES[i];if(!f)return;if(checked)selected.add(f.path);else selected.delete(f.path);syncBulk();}
function toggleAll(checked){FILES.forEach(f=>checked?selected.add(f.path):selected.delete(f.path));document.querySelectorAll('.rowsel').forEach(cb=>cb.checked=checked);syncBulk();}
function clearSel(){selected.clear();document.querySelectorAll('.rowsel').forEach(cb=>cb.checked=false);const sa=$('selall');if(sa)sa.checked=false;syncBulk();}
function syncBulk(){const b=$('bulkdl');if(!b)return;const n=selected.size;
  b.textContent='Download selected'+(n?` (${n})`:'');b.disabled=!n;
  const sc=$('selcount');if(sc)sc.textContent=n?`${n} selected`:'';
  const bc=$('bulkclear');if(bc)bc.style.display=n?'':'none';
  const sa=$('selall');if(sa)sa.checked=FILES.length>0&&FILES.every(f=>selected.has(f.path));}
async function downloadSelected(){
  if(DEMO){alert('Demo mode — connect to a node to download.');return}
  const paths=FILES.filter(f=>selected.has(f.path)).map(f=>f.path);
  if(!paths.length)return;
  const b=$('bulkdl');b.disabled=true;let fails=0;
  for(let i=0;i<paths.length;i++){
    b.textContent=`Downloading ${i+1}/${paths.length}…`;
    try{
      const r=await api('/files/raw?path='+encodeURIComponent(paths[i]));
      const blob=await r.blob();const u=URL.createObjectURL(blob);
      const a=document.createElement('a');a.href=u;a.download=paths[i].split('/').pop();a.click();
      URL.revokeObjectURL(u);
      await new Promise(res=>setTimeout(res,500));
    }catch(e){fails++;}
  }
  b.disabled=false;syncBulk();
  if(fails)alert(fails+' of '+paths.length+' download(s) failed.');
}
function renderPwLine(){
  const el=$('pwline'); if(!el)return;
  if(DEMO){el.innerHTML='node password <b style="color:var(--txt)">yourpassword</b> <span class="mut2">(demo)</span>';return;}
  if(!AUTHREQ){el.innerHTML='<span class="mut2">this node has no password (open access)</span>';return;}
  el.innerHTML='node password <span id="pwval">••••••••</span> <button class="ghost" id="pwbtn" onclick="revealPw()" style="padding:2px 9px;font-size:11px">Show</button> <span id="pwnote"></span>';
}
async function revealPw(){
  try{
    const d=await(await api('/ui-password')).json();
    _pw=d.password||'';
    $('pwval').textContent=_pw||'(none)';
    const b=$('pwbtn'); b.textContent='Copy'; b.setAttribute('onclick','copyPw()');
    $('pwnote').innerHTML=d.is_default?'<span class="warn2">← still the default, change it</span>':'';
  }catch(e){ if($('pwnote'))$('pwnote').innerHTML='<span class="bad2">could not read</span>'; }
}
function copyPw(){ if(_pw&&navigator.clipboard){navigator.clipboard.writeText(_pw); const n=$('pwnote'); if(n)n.innerHTML='<span class="ok2">copied</span>';} }
function startAuto(){if(timer)clearInterval(timer);if($('auto').checked)timer=setInterval(loadAll,5000)}
$('auto').addEventListener('change',startAuto);

// ---- demo data (only used when no node is reachable, e.g. opened standalone) ----
const DEMO_DETAIL={watch_dir:'/archive/recordings',build:'demo',transcribe_enabled:true,
  queue_depth:5,active:[{filename:'@neonsushi_2026.06.05_14-22.mp4',pct:0.63,elapsed:181,stalled:false},
    {filename:'@kilowatt_2026.06.05_13-58.mp4',pct:0.21,elapsed:74,stalled:false}],
  model:'large-v3-turbo',device:'cpu',concurrency:1,vad:true,beam_size:1,scan_interval_sec:30,
  gave_up:1,archived_count:41,max_attempts:3,disk_free_bytes:1.9e12,disk_total_bytes:5.4e12,archive_remote:'b2:tiktok-archive',
  throughput:{completed_last_hour:6,avg_speed:1.8},
  system:{cpu_percent:71,mem_percent:64,loadavg:[1.9,1.6,1.3],cpu_count:4},
  recent_completed:[{mp4:'@dawnpatrol_…14-02.mp4',duration:840,elapsed_sec:440},{mp4:'@vaporwave99_…13-40.mp4',duration:1860,elapsed_sec:880}],
  recent_failed:[{mp4:'@offline_dump_…12-10.mp4',error:'corrupt moov atom',detail:'ValueError: Invalid data found...',permanent:true,attempts:3}]};
const DEMO_FILES=[{username:'neonsushi',filename:'@neonsushi_2026.06.05_14-22.mp4',size_bytes:412e6,mtime:Date.now()/1000-120,path:'/x'},
  {username:'dawnpatrol',filename:'@dawnpatrol_2026.06.05_14-02.txt',size_bytes:48e3,mtime:Date.now()/1000-1400,path:'/y'}];

async function boot(){
  try{
    const r=await fetch('/health',{cache:'no-store'});
    if(!r.ok)throw 0;
    const h=await r.json().catch(()=>({}));
    AUTHREQ=(h.auth_required!==false);
    if(!AUTHREQ){ $('logout').style.display='none'; loadAll(); }
    else if(token()){ $('logout').style.display=''; loadAll(); }
    else { showLogin(); }
    startAuto();
  }catch(e){
    // no node reachable → demo mode (e.g. opened as a standalone preview)
    DEMO=true;
    $('banner').innerHTML='<div class="banner demo">DEMO — no storage node connected. This is what the page looks like with live data.</div>';
    renderAll(DEMO_DETAIL,DEMO_FILES);
  }
}
boot();
</script>
</body>
</html>
'''


@app.get("/")
async def index():
    """Self-contained per-node dashboard (talks to this worker's own API,
    bearer-token login). Served unauthenticated as a static shell; all data
    endpoints stay behind require_auth."""
    return HTMLResponse(STORAGE_UI_HTML)


@app.get("/health")
async def health():
    """No auth — for external monitoring."""
    return {**worker.status, "ok": True, "build": WORKER_BUILD,
            "auth_required": bool(settings.auth_token)}


@app.get("/status", dependencies=[Depends(require_auth)])
async def status_detail():
    return {
        **worker.status,
        "build":            WORKER_BUILD,
        "recent_completed": worker._completed[:5],
        "recent_failed":    worker._failed[:5],
    }


@app.post("/retry-failed", dependencies=[Depends(require_auth)])
async def retry_failed():
    """Clear give-up markers so previously-failed files are re-attempted."""
    return {"cleared": worker.retry_failed()}


@app.get("/ui-password", dependencies=[Depends(require_auth)])
async def ui_password():
    """Return this node's UI password to an ALREADY-AUTHENTICATED caller only.
    Deliberately NOT embedded in the served page — fetched on demand so the
    secret never appears in page source visible to unauthenticated visitors."""
    return {"password": settings.ui_password,
            "is_default": settings.ui_password == "yourpassword"}


@app.get("/transcription-detail", dependencies=[Depends(require_auth)])
async def transcription_detail():
    """Rich telemetry for the Transcription monitoring page: live progress,
    throughput, recent completions/failures, and host CPU/memory/load."""
    return {
        **worker.status,
        "build":            WORKER_BUILD,
        "recent_completed": worker._completed[:15],
        "recent_failed":    worker._failed[:10],
        "throughput":       worker._throughput(),
        "system":           await _system_metrics(),
    }


@app.post("/scan", dependencies=[Depends(require_auth)])
async def trigger_scan():
    """Immediately re-scan the watch directory without waiting for the interval."""
    n = worker.scan()
    return {"queued": n}


@app.get("/transcripts/all-statuses", dependencies=[Depends(require_auth)])
async def all_statuses():
    """Return {filename: 'done'|'processing'|'pending'} for all known MP4s.
    Missing keys mean 'none'. Used by the control plane to populate the Files tab."""
    return worker.all_statuses()


@app.get("/transcripts/view", dependencies=[Depends(require_auth)],
         response_class=PlainTextResponse)
async def view_transcript(filename: str = Query(...)):
    """Return the timestamped .txt transcript for a given MP4 filename."""
    mp4 = worker._find_mp4(filename)
    if not mp4:
        raise HTTPException(404, f"MP4 not found in watch dir: {filename}")
    txt = worker._txt(mp4)
    if not txt.exists():
        raise HTTPException(404, "transcript not yet available")
    return txt.read_text(encoding="utf-8")


@app.get("/transcripts/download-srt", dependencies=[Depends(require_auth)],
         response_class=PlainTextResponse)
async def download_srt(filename: str = Query(...)):
    """Download the timestamped transcript as a .txt file.
    (Endpoint name kept for control-plane compatibility; only .txt is produced now.)"""
    mp4 = worker._find_mp4(filename)
    if not mp4:
        raise HTTPException(404, f"MP4 not found: {filename}")
    txt = worker._txt(mp4)
    if not txt.exists():
        raise HTTPException(404, "transcript not yet available")
    dl_name = filename.replace(".mp4", "_transcript.txt")
    return PlainTextResponse(
        txt.read_text(encoding="utf-8"),
        headers={"Content-Disposition": f'attachment; filename="{dl_name}"'},
    )


@app.get("/transcripts", dependencies=[Depends(require_auth)])
async def list_transcripts():
    """List all transcribed recordings."""
    if not settings.watch_dir.exists():
        return []
    results = []
    for txt in sorted(settings.watch_dir.rglob("*.txt"), reverse=True):
        mp4 = txt.with_suffix(".mp4")
        entry = {
            "mp4":      str(mp4) if mp4.exists() else None,
            "txt":      str(txt),
            "username": txt.parent.name,
            "filename": txt.stem,
            "size_bytes": mp4.stat().st_size if mp4.exists() else None,
            "mtime":      txt.stat().st_mtime,
        }
        # language/duration now live in the first-line header: "# language=en duration=123.4s ..."
        try:
            first = txt.read_text(encoding="utf-8").split("\n", 1)[0]
            if first.startswith("#"):
                for tok in first[1:].split():
                    if tok.startswith("language="):
                        entry["language"] = tok.split("=", 1)[1]
                    elif tok.startswith("duration="):
                        entry["duration"] = tok.split("=", 1)[1].rstrip("s")
        except Exception:
            pass
        results.append(entry)
    return results


@app.get("/transcripts/search", dependencies=[Depends(require_auth)])
async def search(q: str = Query(..., min_length=1)):
    """Full-text search across all transcripts."""
    if not settings.watch_dir.exists():
        return []
    q_lower = q.lower()
    results = []
    for txt in settings.watch_dir.rglob("*.txt"):
        try:
            content = txt.read_text(encoding="utf-8")
        except Exception:
            continue
        if q_lower not in content.lower():
            continue
        idx    = content.lower().index(q_lower)
        start  = max(0, idx - 80)
        end    = min(len(content), idx + len(q) + 80)
        snippet = ("…" if start > 0 else "") + content[start:end] + ("…" if end < len(content) else "")
        results.append({
            "txt":      str(txt),
            "username": txt.parent.name,
            "filename": txt.stem,
            "snippet":  snippet,
            "mtime":    txt.stat().st_mtime,
        })
    return sorted(results, key=lambda x: x["mtime"], reverse=True)


# ---------------------------------------------------------------------------

def _find_chat(filename: str) -> Path | None:
    for p in settings.watch_dir.rglob(filename):
        if p.name.endswith("_chat.jsonl"):
            return p
    return None


def _iter_chat_events(path: Path):
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                continue


@app.get("/chat", dependencies=[Depends(require_auth)])
async def list_chat():
    """List all captured chat logs with a quick summary (events/comments/gifts)."""
    if not settings.watch_dir.exists():
        return []
    out = []
    for jf in sorted(settings.watch_dir.rglob("*_chat.jsonl"), reverse=True):
        try:
            st = jf.stat()
            events = comments = gifts = 0
            first = last = None
            for rec in _iter_chat_events(jf):
                events += 1
                t = rec.get("type")
                if t == "comment":
                    comments += 1
                elif t == "gift":
                    gifts += 1
                ts = rec.get("ts")
                if ts is not None:
                    first = first if first is not None else ts
                    last = ts
            out.append({"path": str(jf), "filename": jf.name,
                        "username": jf.parent.name, "size_bytes": st.st_size,
                        "mtime": st.st_mtime, "events": events,
                        "comments": comments, "gifts": gifts,
                        "started": first, "ended": last})
        except OSError:
            pass
    return out


@app.get("/chat/view", dependencies=[Depends(require_auth)])
async def view_chat(filename: str = Query(...), q: str = Query(None),
                    type: str = Query(None), limit: int = Query(5000, le=20000)):
    """Return parsed events for one chat log, optionally filtered by text/type."""
    jf = _find_chat(filename)
    if not jf:
        raise HTTPException(404, f"chat log not found: {filename}")
    ql = q.lower() if q else None
    events = []
    truncated = False
    for rec in _iter_chat_events(jf):
        if type and rec.get("type") != type:
            continue
        if ql:
            hay = " ".join(str(rec.get(k, "")) for k in
                           ("text", "nickname", "user", "gift")).lower()
            if ql not in hay:
                continue
        events.append(rec)
        if len(events) >= limit:
            truncated = True
            break
    return {"filename": jf.name, "username": jf.parent.name,
            "events": events, "truncated": truncated}


@app.get("/chat/search", dependencies=[Depends(require_auth)])
async def search_chat(q: str = Query(..., min_length=1)):
    """Search every chat log for text in comments/usernames/gifts."""
    if not settings.watch_dir.exists():
        return []
    ql = q.lower()
    results = []
    for jf in settings.watch_dir.rglob("*_chat.jsonl"):
        try:
            matches = []
            for rec in _iter_chat_events(jf):
                hay = " ".join(str(rec.get(k, "")) for k in
                               ("text", "nickname", "user", "gift")).lower()
                if ql not in hay:
                    continue
                matches.append(rec)
                if len(matches) >= 25:
                    break
            if matches:
                results.append({"filename": jf.name, "username": jf.parent.name,
                                "path": str(jf), "mtime": jf.stat().st_mtime,
                                "match_count": len(matches), "matches": matches})
        except OSError:
            pass
    return sorted(results, key=lambda x: x["mtime"], reverse=True)


# ---------------------------------------------------------------------------

@app.get("/files/inventory", dependencies=[Depends(require_auth)])
async def files_inventory():
    """List all MP4 files on the storage server.
    Used by the control plane upload worker to know what's already here."""
    if not settings.watch_dir.exists():
        return []
    result = []
    for f in list(settings.watch_dir.rglob("*.mp4")) + list(settings.watch_dir.rglob("*_chat.jsonl")):
        try:
            stat = f.stat()
            result.append({
                "filename":   f.name,
                "path":       str(f),
                "username":   f.parent.name,
                "size_bytes": stat.st_size,
                "mtime":      stat.st_mtime,
            })
        except OSError:
            pass
    return sorted(result, key=lambda x: x["mtime"], reverse=True)


@app.get("/files/archive-status", dependencies=[Depends(require_auth)])
async def files_archive_status():
    """Per-file cold-tier (archive) status, read from the existing .archived
    markers archive_sweep() writes. Read-only — exposes state that already exists
    so the control-plane catalog can track what's safely on the cloud remote."""
    out: dict = {}
    if not settings.watch_dir.exists():
        return out
    # Iterate the .archived MARKERS (not *.mp4), so evicted cloud-only recordings
    # — whose local .mp4 is gone — are still reported. `local` says whether the
    # mp4 is still on disk; `username` is needed to fetch it back from the cloud.
    for marker in settings.watch_dir.rglob("*.archived"):
        mp4 = marker.with_suffix(".mp4")
        remote = None
        try:
            for tok in marker.read_text().split():
                if tok.startswith("remote="):
                    remote = tok.split("=", 1)[1]
        except OSError:
            pass
        entry = {"archived": True, "remote": remote,
                 "username": marker.parent.name, "local": mp4.exists()}
        try:
            entry["archived_at"] = marker.stat().st_mtime
        except OSError:
            pass
        out[mp4.name] = entry
    return out


@app.post("/archive/evict-now", dependencies=[Depends(require_auth)])
async def archive_evict_now():
    """Run the disk-eviction sweep immediately instead of waiting for the next
    scan. Returns the diagnostic so the caller can see what (if anything) freed."""
    if not settings.archive_remote:
        raise HTTPException(400, "cloud archive is not configured on this server")
    await asyncio.to_thread(worker._evict_if_pressured)
    return {"ok": True, "last_evict": worker._last_evict}


@app.get("/archive/download", dependencies=[Depends(require_auth)])
async def archive_download(username: str = Query(...), filename: str = Query(...)):
    """Stream a recording back from the cloud archive via `rclone cat`, so an
    evicted (cloud-only) file can still be downloaded. Uses the worker's own
    RCLONE_CONFIG (set in the env file), so no extra credentials are needed."""
    if not settings.archive_remote:
        raise HTTPException(400, "cloud archive is not configured on this server")
    for seg in (username, filename):
        if ("/" in seg or "\\" in seg or ".." in seg or seg in ("", ".", "..")
                or any(ord(ch) < 32 for ch in seg)):
            raise HTTPException(400, "invalid path segment")
    remote = f"{settings.archive_remote.rstrip('/')}/{username}/{filename}"
    try:
        # stderr DISCARDED (not PIPE) — an unread PIPE can fill and deadlock rclone.
        proc = await asyncio.create_subprocess_exec(
            "rclone", "cat", remote,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    except FileNotFoundError:
        raise HTTPException(500, "rclone not installed")

    # Peek the first chunk before committing to a 200: if rclone produced nothing
    # and exited non-zero (missing/purged file, bad config), surface a real 404
    # instead of a silent empty download.
    first = await proc.stdout.read(65536)
    if not first:
        rc = await proc.wait()
        if rc != 0:
            raise HTTPException(404, "not found in the cloud archive (or archive misconfigured)")

    async def _stream():
        try:
            if first:
                yield first
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            await proc.wait()

    return StreamingResponse(
        _stream(), media_type="video/mp4",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.put("/files/{username}/{filename}", dependencies=[Depends(require_auth)])
async def receive_file(username: str, filename: str, request: Request):
    """Receive an MP4 streamed via PUT from the control plane upload worker.
    Writes atomically: data → .tmp file, then rename to final path.
    Returns the size of the received file for the control plane to verify."""
    if not (filename.endswith(".mp4") or filename.endswith("_chat.jsonl")
            or filename.endswith(".txt")):
        raise HTTPException(400, "only .mp4, .txt and _chat.jsonl files accepted")
    # Reject path traversal in either segment
    for seg in (username, filename):
        if "/" in seg or "\\" in seg or seg in ("..", ".") or seg.startswith(".."):
            raise HTTPException(400, "invalid path segment")
    dest_dir = settings.watch_dir / username
    # Defense in depth: confirm the resolved path stays under watch_dir
    try:
        dest_dir.resolve().relative_to(settings.watch_dir.resolve())
    except ValueError:
        raise HTTPException(400, "path outside watch dir")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    tmp_path  = dest_dir / f".{filename}.tmp"

    written = 0
    try:
        with tmp_path.open("wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
                written += len(chunk)
        tmp_path.rename(dest_path)   # atomic on Linux (same filesystem)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(500, f"write failed: {e}")

    log.info("received %s/%s (%s bytes)", username, filename, f"{written:,}")
    return {"filename": filename, "path": str(dest_path), "size_bytes": written}


# ---------------------------------------------------------------------------
# Move support: list every movable file, serve raw bytes, delete on confirm.

def _kind_of(name: str) -> str:
    if name.endswith("_chat.jsonl"):
        return "chat"
    if name.endswith(".txt"):
        return "transcript"
    if name.endswith(".mp4"):
        return "recording"
    return "other"


def _safe_under_watch(p: Path) -> Path:
    """Resolve a path and ensure it stays inside watch_dir (no traversal)."""
    rp = p.resolve()
    try:
        rp.relative_to(settings.watch_dir.resolve())
    except ValueError:
        raise HTTPException(400, "path outside watch dir")
    return rp


@app.get("/files/all", dependencies=[Depends(require_auth)])
async def files_all(username: str = Query(None)):
    """Every movable file (recordings, transcripts, chat logs), optionally for
    one creator. Used by the control plane to enumerate a move."""
    if not settings.watch_dir.exists():
        return []
    base = settings.watch_dir / username if username else settings.watch_dir
    if username and not base.exists():
        return []
    out = []
    open_files = _open_files()
    now = time.time()
    for f in base.rglob("*"):
        if not f.is_file():
            continue
        if f.name.endswith(".tmp") or f.name.endswith(".progress") \
           or f.name.endswith(".archived"):
            continue
        if _kind_of(f.name) == "other":
            continue
        # Never offer a file that's still being recorded/written for moving —
        # it would grow mid-copy (size mismatch) and, worse, could be deleted
        # from the source while a recording is still active.
        if _in_progress(f, now, open_files):
            continue
        try:
            st = f.stat()
            out.append({"filename": f.name, "path": str(f),
                        "username": f.parent.name, "kind": _kind_of(f.name),
                        "size_bytes": st.st_size, "mtime": st.st_mtime})
        except OSError:
            pass
    return sorted(out, key=lambda x: x["mtime"], reverse=True)


@app.get("/files/raw", dependencies=[Depends(require_auth)])
async def files_raw(path: str = Query(...)):
    """Stream the raw bytes of a stored file (for moving to another server)."""
    p = _safe_under_watch(Path(path))
    if not p.exists() or not p.is_file():
        raise HTTPException(404, "file not found")
    return FileResponse(str(p), filename=p.name,
                        media_type="application/octet-stream")


@app.post("/files/delete", dependencies=[Depends(require_auth)])
async def files_delete(body: dict):
    """Delete specific files by absolute path (validated under watch_dir).
    Used to remove the SOURCE copy after a verified move, on confirmation."""
    paths = body.get("paths") or []
    deleted, errors = [], []
    for raw in paths:
        try:
            p = _safe_under_watch(Path(raw))
            if p.exists():
                p.unlink()
                deleted.append(str(p))
            # tidy up the sidecar markers if the mp4 is gone
            for sidecar in (p.with_suffix(".progress"), p.with_suffix(".archived")):
                sidecar.unlink(missing_ok=True)
        except HTTPException as e:
            errors.append({"path": raw, "error": e.detail})
        except OSError as e:
            errors.append({"path": raw, "error": str(e)})
    return {"deleted": deleted, "errors": errors}


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--transcribe-one":
        sys.exit(_transcribe_one_subprocess(sys.argv[2], safe=("--safe" in sys.argv[3:])))
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info",
                timeout_graceful_shutdown=10)

