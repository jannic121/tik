"""
Recorder backend: subprocess wrapper for Michele0303/tiktok-live-recorder.

One WatcherProcess per creator. Each runs `main.py -mode automatic` in its own
process group, captures stdout/stderr to a rotating log, and emits lifecycle
events (started, session_started, session_ended, errored, stopped) via callbacks.

Decisions baked in (see chat for reasoning):
  - mode=automatic
  - one process per creator (not -user a,b,c batched)
  - no -bitrate (recorder does fast remux; central server re-encodes — "Option C")
  - no -proxy (IP diversity comes from multiple backends)
  - no -duration (let stream-end drive stop)
  - no -telegram
  - -no-update-check is mandatory (else the recorder may self-update on startup)
  - cookies.json kept in place with empty sessionid_ss (cookieless start)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State

class WatcherState(str, Enum):
    STARTING = "starting"
    IDLE = "idle"           # subprocess up, polling, not currently recording
    RECORDING = "recording" # a file is actively growing in the output dir
    BACKOFF = "backoff"     # subprocess died; waiting before restart
    ERROR = "error"         # subprocess keeps dying; circuit breaker tripped
    STOPPED = "stopped"     # asked to stop


@dataclass
class WatcherConfig:
    username: str
    repo_path: Path                       # path to cloned tiktok-live-recorder
    recordings_root: Path                 # e.g. /data/recordings
    python_executable: str = "python3"    # or path to uv-managed venv python
    automatic_interval_min: int = 3       # passed to -automatic_interval
    max_consecutive_failures: int = 5     # circuit breaker
    backoff_initial_sec: float = 1.0
    backoff_max_sec: float = 60.0
    idle_repoll_sec: float = 15.0         # wait after a clean (offline) exit
                                          # before checking if the creator is live again
    stop_grace_sec: float = 30.0          # how long to wait for SIGINT to finalize
    startup_jitter_sec: float = 0.0       # random 0..N delay before FIRST spawn
                                          # (spreads load when many watchers start at once)
    capture_chat: bool = True             # also record TikTok LIVE chat (chat_recorder.py)
    sign_api_key: str = ""                # optional TikTokLive sign-server API key
    chat_python: str = ""                 # python for chat_recorder (its own venv); blank = same as recorder

    @property
    def output_dir(self) -> Path:
        return self.recordings_root / self.username


# Event payloads passed to callbacks
@dataclass
class WatcherEvent:
    username: str
    kind: str                             # started|session_started|session_ended|errored|stopped|heartbeat
    timestamp: float = field(default_factory=time.time)
    detail: dict = field(default_factory=dict)


EventHandler = Callable[[WatcherEvent], Awaitable[None]]


# ---------------------------------------------------------------------------
# Session detection by polling the output dir

# Recordings are written as TK_<user>_<date>_<time>.mp4 — older Michele0303
# builds used a "_flv.mp4" intermediate, newer ones write straight to ".mp4".
# Match either, and detect an *active* recording by file growth rather than by
# the suffix (which varies by recorder version).
_RECORDING_FILE_RE = re.compile(
    r"^TK_.+_\d{4}\.\d{2}\.\d{2}_\d{2}-\d{2}-\d{2}(_flv)?\.mp4$")
_FINAL_FILE_RE = re.compile(r"^TK_.+_\d{4}\.\d{2}\.\d{2}_\d{2}-\d{2}-\d{2}\.mp4$")


class SessionDetector:
    """
    Polls the per-creator output dir to detect session boundaries:
      - new _flv.mp4 file appearing -> session_started
      - that file's size stable for `stable_seconds` AND the final .mp4 exists
        -> session_ended (with path to the finalized .mp4)
    """

    def __init__(
        self,
        output_dir: Path,
        on_event: EventHandler,
        username: str,
        poll_interval_sec: float = 2.0,
        stable_seconds: float = 10.0,
    ):
        self.output_dir = output_dir
        self.on_event = on_event
        self.username = username
        self.poll_interval = poll_interval_sec
        self.stable_seconds = stable_seconds
        self._active_flv: Optional[Path] = None
        self._last_size: int = -1
        self._last_change_at: float = 0.0
        self._sizes: dict[str, int] = {}     # filename -> last seen size (growth detection)
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"session-detect-{self.username}")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        try:
            while True:
                await self._tick()
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] session detector crashed", self.username)

    async def _tick(self) -> None:
        if not self.output_dir.exists():
            return
        now = time.time()
        candidates = [p for p in self.output_dir.iterdir()
                      if p.is_file() and _RECORDING_FILE_RE.match(p.name)]

        if self._active_flv is None:
            # Idle: a recording is "active" when a matching file is GROWING.
            # (A finalized file from a past session is present but won't grow,
            # so it never trips this — which is why we detect by growth, not
            # by the mere existence of a matching filename.)
            for p in candidates:
                try:
                    sz = p.stat().st_size
                except OSError:
                    continue
                prev = self._sizes.get(p.name)
                self._sizes[p.name] = sz
                if prev is not None and sz > prev:
                    self._active_flv = p
                    self._last_size = sz
                    self._last_change_at = now
                    await self.on_event(WatcherEvent(
                        username=self.username,
                        kind="session_started",
                        detail={"path": str(p)},
                    ))
                    return
            # Forget sizes for files that have disappeared (keeps dict bounded).
            present = {p.name for p in candidates}
            for name in list(self._sizes):
                if name not in present:
                    self._sizes.pop(name, None)
        else:
            # Watching the active recording.
            try:
                size = self._active_flv.stat().st_size
            except OSError:
                size = None
            if size is None:
                # File was renamed/remuxed away → session ended.
                final = self._find_final_mp4(self._active_flv)
                await self.on_event(WatcherEvent(
                    username=self.username,
                    kind="session_ended",
                    detail={"flv_path": str(self._active_flv),
                            "final_path": str(final) if final else None},
                ))
                self._reset_active()
                return
            if size > self._last_size:
                self._last_size = size
                self._last_change_at = now
            elif now - self._last_change_at > self.stable_seconds:
                # Stopped growing → recording finished (file kept its name).
                final = self._find_final_mp4(self._active_flv)
                await self.on_event(WatcherEvent(
                    username=self.username,
                    kind="session_ended",
                    detail={"flv_path": str(self._active_flv),
                            "final_path": str(final or self._active_flv)},
                ))
                self._reset_active()

    def _reset_active(self) -> None:
        if self._active_flv is not None:
            self._sizes.pop(self._active_flv.name, None)
        self._active_flv = None
        self._last_size = -1

    @staticmethod
    def _find_final_mp4(rec_path: Path) -> Optional[Path]:
        # _flv builds: TK_user_TS_flv.mp4 -> TK_user_TS.mp4. Newer builds write
        # the final name directly, so the recording file is already the final one.
        if "_flv.mp4" in rec_path.name:
            candidate = rec_path.with_name(rec_path.name.replace("_flv.mp4", ".mp4"))
            if candidate.exists():
                return candidate
        return rec_path if rec_path.exists() else None


# ---------------------------------------------------------------------------
# Subprocess wrapper

class WatcherProcess:
    """
    Owns one Michele0303 subprocess in its own process group.

    Use start() / stop() / status. Survives recorder crashes via supervised
    restart with exponential backoff. After N consecutive failures, transitions
    to ERROR state and stops trying (caller should surface this in the UI).
    """

    def __init__(self, config: WatcherConfig, on_event: EventHandler):
        self.config = config
        self.on_event = on_event
        self.state: WatcherState = WatcherState.STARTING
        self.last_error: Optional[str] = None
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._supervisor_task: Optional[asyncio.Task] = None
        self._detector: Optional[SessionDetector] = None
        self._stop_requested = False
        self._consecutive_failures = 0
        self._apply_startup_jitter = False
        self._chat_proc: Optional[asyncio.subprocess.Process] = None
        self._chat_task: Optional[asyncio.Task] = None
        self._chat_log: deque = deque(maxlen=120)   # recent recorder output
        self._chat_running = False
        self._chat_pid: Optional[int] = None
        self._chat_restarts = 0
        self._chat_last_exit: Optional[int] = None
        self._chat_started_at: Optional[float] = None

    async def start(self, jitter: bool = False) -> None:
        # Create the per-creator output dir BEFORE the recorder tries to write
        # (Michele0303 will crash on open() if the dir is missing).
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self._apply_startup_jitter = jitter

        self._detector = SessionDetector(
            output_dir=self.config.output_dir,
            on_event=self._wrap_event,
            username=self.config.username,
        )
        self._detector.start()

        self._supervisor_task = asyncio.create_task(
            self._supervise(), name=f"watcher-{self.config.username}",
        )
        if self.config.capture_chat:
            self._chat_task = asyncio.create_task(
                self._supervise_chat(), name=f"chat-{self.config.username}",
            )
        await self.on_event(WatcherEvent(
            username=self.config.username, kind="started",
        ))

    async def _supervise_chat(self) -> None:
        """Run and keep alive the TikTok chat recorder subprocess. The recorder
        connects-when-live and reconnects on its own, so this only restarts it if
        the process exits entirely (crash / library error)."""
        script = self.config.repo_path.parent / "chat_recorder.py"
        if not script.exists():
            script = Path(__file__).resolve().parent / "chat_recorder.py"
        if not script.exists():
            msg = "chat_recorder.py not found — chat capture disabled"
            log.warning("[%s] %s", self.config.username, msg)
            self._chat_log.append(f"[supervisor] {msg}")
            return
        backoff = 2.0
        while not self._stop_requested:
            try:
                env = dict(os.environ)
                if self.config.sign_api_key:
                    env["SIGN_API_KEY"] = self.config.sign_api_key
                self._chat_proc = await asyncio.create_subprocess_exec(
                    (self.config.chat_python or self.config.python_executable), str(script),
                    "--username", self.config.username,
                    "--output-dir", str(self.config.output_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    env=env, preexec_fn=os.setsid,
                )
                self._chat_running = True
                self._chat_pid = self._chat_proc.pid
                self._chat_started_at = time.time()
                self._chat_log.append(f"[supervisor] started pid={self._chat_pid}")
                # Stream the recorder's output into the ring buffer for the UI.
                assert self._chat_proc.stdout is not None
                async for raw in self._chat_proc.stdout:
                    line = raw.decode("utf-8", errors="replace").rstrip()
                    if line:
                        self._chat_log.append(line)
                rc = await self._chat_proc.wait()
                self._chat_running = False
                self._chat_last_exit = rc
                self._chat_pid = None
                if self._stop_requested:
                    return
                self._chat_restarts += 1
                hint = ""
                if rc == 3:
                    hint = " (TikTokLive not installed on this backend)"
                self._chat_log.append(
                    f"[supervisor] exited code={rc}{hint}; restarting in {backoff:.0f}s")
                log.info("[%s] chat recorder exited (code=%s); restarting in %.0fs",
                         self.config.username, rc, backoff)
            except asyncio.CancelledError:
                return
            except Exception as e:
                self._chat_running = False
                self._chat_log.append(f"[supervisor] error: {e}")
                log.warning("[%s] chat recorder error: %s", self.config.username, e)
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                return
            backoff = min(backoff * 2, 60.0)

    def chat_status(self) -> dict:
        return {
            "capture_chat":  self.config.capture_chat,
            "running":       self._chat_running,
            "pid":           self._chat_pid,
            "restarts":      self._chat_restarts,
            "last_exit":     self._chat_last_exit,
            "uptime_sec":    (round(time.time() - self._chat_started_at, 1)
                              if self._chat_running and self._chat_started_at else None),
            "recent_log":    list(self._chat_log),
        }

    async def _stop_chat(self) -> None:
        self._chat_task = self._chat_task
        if self._chat_proc and self._chat_proc.returncode is None:
            try:
                os.killpg(os.getpgid(self._chat_proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        if self._chat_task and not self._chat_task.done():
            self._chat_task.cancel()
            try:
                await self._chat_task
            except (asyncio.CancelledError, Exception):
                pass
        self._chat_task = None
        self._chat_proc = None
        self._chat_running = False
        self._chat_pid = None

    async def restart(self) -> None:
        """Re-enable / restart this watcher from ANY state (ERROR, idle, recording).
        Resets the failure circuit-breaker and launches a fresh supervisor loop."""
        # Tear down any running process
        self._stop_requested = True
        await self._stop_chat()
        if self._proc and self._proc.returncode is None:
            try:
                pgid = os.getpgid(self._proc.pid)
                os.killpg(pgid, signal.SIGINT)
                await asyncio.wait_for(self._proc.wait(),
                                       timeout=self.config.stop_grace_sec)
            except (ProcessLookupError, PermissionError, asyncio.TimeoutError):
                try:
                    os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
        # Cancel any live supervisor (in ERROR state it has already returned)
        if self._supervisor_task and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass
        # Reset state and relaunch
        self._stop_requested = False
        self._consecutive_failures = 0
        self.last_error = None
        self.state = WatcherState.STARTING
        self._apply_startup_jitter = True   # stagger mass re-enable
        if self._detector is None:
            self._detector = SessionDetector(
                output_dir=self.config.output_dir,
                on_event=self._wrap_event,
                username=self.config.username,
            )
            self._detector.start()
        self._supervisor_task = asyncio.create_task(
            self._supervise(), name=f"watcher-{self.config.username}",
        )
        if self.config.capture_chat:
            self._chat_task = asyncio.create_task(
                self._supervise_chat(), name=f"chat-{self.config.username}",
            )
        await self.on_event(WatcherEvent(
            username=self.config.username, kind="started",
        ))

    async def stop(self) -> None:
        self._stop_requested = True
        await self._stop_chat()
        # Send SIGINT to the process group so Michele0303 finalizes cleanly
        if self._proc and self._proc.returncode is None:
            try:
                pgid = os.getpgid(self._proc.pid)
                os.killpg(pgid, signal.SIGINT)
            except (ProcessLookupError, PermissionError) as e:
                log.warning("[%s] could not signal process group: %s",
                            self.config.username, e)

            # Give the recorder a chance to flush + remux
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=self.config.stop_grace_sec)
            except asyncio.TimeoutError:
                log.warning("[%s] grace period exceeded; SIGTERM-ing group",
                            self.config.username)
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

        if self._supervisor_task:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass

        if self._detector:
            await self._detector.stop()

        self.state = WatcherState.STOPPED
        await self.on_event(WatcherEvent(
            username=self.config.username, kind="stopped",
        ))

    async def _supervise(self) -> None:
        """Spawn, monitor, restart loop with exponential backoff."""
        backoff = self.config.backoff_initial_sec

        # Stagger the FIRST spawn by a random delay so that when many watchers
        # start at once (server restart, mass re-enable) they don't all hit
        # TikTok in the same instant and trip rate limits. Only applied when the
        # start was flagged for jitter (restore / re-enable), never a manual add.
        # Crash-restarts are already spaced by the backoff below.
        jitter = self.config.startup_jitter_sec
        if self._apply_startup_jitter and jitter and jitter > 0:
            delay = random.uniform(0, jitter)
            log.info("[%s] startup jitter: waiting %.1fs before first spawn",
                     self.config.username, delay)
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return
            if self._stop_requested:
                return
        self._apply_startup_jitter = False

        try:
            while not self._stop_requested:
                exit_code = await self._spawn_and_wait()

                if self._stop_requested:
                    return

                # Exit code 0 = clean exit. Michele0303 returns 0 when the
                # creator simply isn't live right now. This is NORMAL, not a
                # failure — reset the circuit breaker and re-poll after a short
                # idle wait rather than counting it toward the error threshold.
                if exit_code == 0:
                    self._consecutive_failures = 0
                    self.last_error = None
                    self.state = WatcherState.IDLE
                    backoff = self.config.backoff_initial_sec
                    # Re-check at the creator's configured poll cadence so we
                    # don't hammer TikTok (floor of idle_repoll_sec as a guard).
                    poll_wait = max(self.config.idle_repoll_sec,
                                    self.config.automatic_interval_min * 60)
                    try:
                        await asyncio.sleep(poll_wait)
                    except asyncio.CancelledError:
                        return
                    continue

                # Non-zero exit = the subprocess actually crashed
                self._consecutive_failures += 1
                self.last_error = f"exited with code {exit_code}"
                log.warning(
                    "[%s] recorder crashed (code=%s, failure %d/%d). Backing off %.1fs",
                    self.config.username, exit_code,
                    self._consecutive_failures,
                    self.config.max_consecutive_failures,
                    backoff,
                )

                if self._consecutive_failures >= self.config.max_consecutive_failures:
                    self.state = WatcherState.ERROR
                    await self.on_event(WatcherEvent(
                        username=self.config.username,
                        kind="errored",
                        detail={"reason": "max_failures", "exit_code": exit_code},
                    ))
                    return

                # Reflect "we're waiting before next attempt" honestly,
                # rather than leaving stale state from the previous spawn.
                self.state = WatcherState.BACKOFF
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    return
                backoff = min(backoff * 2, self.config.backoff_max_sec)
        except asyncio.CancelledError:
            raise

    async def _spawn_and_wait(self) -> int:
        cmd = [
            self.config.python_executable,
            str(self.config.repo_path / "src" / "main.py"),
            "-user", self.config.username,
            "-mode", "automatic",
            "-automatic_interval", str(self.config.automatic_interval_min),
            "-output", str(self.config.output_dir),
            "-no-update-check",
        ]

        log_path = self.config.output_dir / "_recorder.log"
        # Open in append mode so the file survives restarts
        log_fh = open(log_path, "ab")

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(self.config.repo_path),
                stdout=log_fh,
                stderr=asyncio.subprocess.STDOUT,
                # Give the child its own process group so we can signal
                # Michele0303's multiprocessing children too.
                start_new_session=True,
            )
            self.state = WatcherState.IDLE
            log.info("[%s] recorder started, pid=%d",
                     self.config.username, self._proc.pid)

            # Once we've stayed up for >60s, reset the failure counter
            asyncio.create_task(self._reset_failures_after(60))

            return await self._proc.wait()
        finally:
            log_fh.close()

    async def _reset_failures_after(self, seconds: int) -> None:
        try:
            await asyncio.sleep(seconds)
            if self._proc and self._proc.returncode is None:
                self._consecutive_failures = 0
        except asyncio.CancelledError:
            pass

    async def _wrap_event(self, event: WatcherEvent) -> None:
        """Intercept session events from the detector to update our own state."""
        if event.kind == "session_started":
            self.state = WatcherState.RECORDING
        elif event.kind == "session_ended":
            self.state = WatcherState.IDLE
        await self.on_event(event)

    @property
    def status(self) -> dict:
        return {
            "username": self.config.username,
            "state": self.state.value,
            "pid": self._proc.pid if self._proc and self._proc.returncode is None else None,
            "consecutive_failures": self._consecutive_failures,
            "last_error": self.last_error,
        }


# ---------------------------------------------------------------------------
# Example usage

async def _demo() -> None:
    """Example: spin up a watcher for a single creator."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    async def print_event(event: WatcherEvent) -> None:
        print(f"[event] {event.kind} {event.username} {event.detail}")

    config = WatcherConfig(
        username="someuser",
        repo_path=Path("/opt/tiktok-live-recorder"),     # where you cloned the repo
        recordings_root=Path("/data/recordings"),         # per-creator subdirs created under here
        python_executable="/opt/tiktok-live-recorder/.venv/bin/python",
        automatic_interval_min=3,
    )

    watcher = WatcherProcess(config, on_event=print_event)
    await watcher.start()

    try:
        # Run for an hour then stop
        await asyncio.sleep(3600)
    finally:
        await watcher.stop()


if __name__ == "__main__":
    asyncio.run(_demo())
