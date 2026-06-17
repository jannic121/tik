#!/usr/bin/env python3
"""
TikTok LIVE chat recorder — spawned per-watcher by watcher.py.

Connects to a creator's LIVE via the TikTokLive library and appends events
(comments, gifts, likes, joins, shares, follows, subscribes) to a JSONL file
in the creator's recording folder, one JSON object per line:

    {"ts": 1717000000.12, "rel": 12.3, "type": "comment",
     "user": "someuser", "nickname": "Some User", "text": "hello"}

A new file is started each time the creator goes live:
    <recordings>/<username>/TK_<username>_<YYYY.MM.DD_HH-MM-SS>_chat.jsonl

No TikTok credentials are required. TikTokLive negotiates the Webcast
connection via a signing service; for many simultaneous creators you may need
a sign-server API key (env SIGN_API_KEY / EULER_API_KEY).

This process runs forever: it connects when the creator is live, logs until the
stream ends, then waits and retries. watcher.py supervises/​restarts it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path


def _log(msg: str) -> None:
    print(f"[chat] {msg}", flush=True)


def _now_stamp() -> str:
    return time.strftime("%Y.%m.%d_%H-%M-%S")


class ChatSession:
    """One live session → one JSONL file."""

    def __init__(self, username: str, output_dir: Path):
        self.username = username
        self.output_dir = output_dir
        self.fh = None
        self.path: Path | None = None
        self.start_ts: float = 0.0
        self.count = 0

    def open(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.start_ts = time.time()
        self.path = self.output_dir / f"TK_{self.username}_{_now_stamp()}_chat.jsonl"
        self.fh = self.path.open("a", encoding="utf-8")
        _log(f"session open → {self.path.name}")

    def write(self, event_type: str, **fields) -> None:
        if not self.fh:
            return
        rec = {"ts": round(time.time(), 2),
               "rel": round(time.time() - self.start_ts, 2),
               "type": event_type, **fields}
        self.fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.fh.flush()
        self.count += 1

    def close(self) -> None:
        if self.fh:
            try:
                self.fh.close()
            except Exception:
                pass
            _log(f"session closed ({self.count} events) {self.path.name if self.path else ''}")
        self.fh = None


async def run(username: str, output_dir: Path, retry_sec: float, api_key: str | None) -> int:
    try:
        from TikTokLive import TikTokLiveClient
        from TikTokLive.events import (
            ConnectEvent, DisconnectEvent, CommentEvent, GiftEvent,
            LikeEvent, JoinEvent, ShareEvent, FollowEvent, SubscribeEvent,
        )
    except Exception as e:  # library not installed / import error
        _log(f"TikTokLive unavailable: {e}")
        _log("FIX: install it on this backend, e.g. "
             "`/opt/tt-backend/.venv/bin/pip install TikTokLive` then restart tt-backend")
        return 3

    try:
        import TikTokLive as _ttl
        _log(f"TikTokLive version {getattr(_ttl, '__version__', '?')}; "
             f"sign key {'set' if api_key else 'NOT set (using free tier)'}")
    except Exception:
        pass

    uid = username if username.startswith("@") else f"@{username}"
    session = ChatSession(username, output_dir)
    stop = asyncio.Event()

    def _sig(*_):
        stop.set()
    try:
        signal.signal(signal.SIGINT, _sig)
        signal.signal(signal.SIGTERM, _sig)
    except Exception:
        pass

    while not stop.is_set():
        client = TikTokLiveClient(unique_id=uid)
        if api_key:
            # Configure the signing service key if the installed version supports it.
            try:
                client.web.signer.sign_api_key = api_key  # best-effort across versions
            except Exception:
                pass

        def _user(ev):
            u = getattr(ev, "user", None)
            return {
                "user": getattr(u, "unique_id", None) if u else None,
                "nickname": getattr(u, "nickname", None) if u else None,
            }

        @client.on(ConnectEvent)
        async def _on_connect(ev):
            session.open()
            session.write("connect", room_id=str(getattr(client, "room_id", "") or ""))

        @client.on(CommentEvent)
        async def _on_comment(ev):
            session.write("comment", **_user(ev), text=getattr(ev, "comment", None))

        @client.on(GiftEvent)
        async def _on_gift(ev):
            g = getattr(ev, "gift", None)
            session.write("gift", **_user(ev),
                          gift=getattr(g, "name", None) if g else None,
                          repeat=getattr(ev, "repeat_count", None))

        @client.on(LikeEvent)
        async def _on_like(ev):
            session.write("like", **_user(ev), count=getattr(ev, "count", None))

        @client.on(JoinEvent)
        async def _on_join(ev):
            session.write("join", **_user(ev))

        @client.on(ShareEvent)
        async def _on_share(ev):
            session.write("share", **_user(ev))

        @client.on(FollowEvent)
        async def _on_follow(ev):
            session.write("follow", **_user(ev))

        @client.on(SubscribeEvent)
        async def _on_sub(ev):
            session.write("subscribe", **_user(ev))

        @client.on(DisconnectEvent)
        async def _on_disconnect(ev):
            session.write("disconnect")
            session.close()

        try:
            _log(f"connecting to {uid} …")
            await client.connect()          # returns when the stream ends/disconnects
        except Exception as e:
            # Most commonly: user is not currently live. Distinguish that from
            # real errors (sign-server/rate-limit/network) so the UI is useful.
            name = type(e).__name__
            msg = str(e)[:160]
            if "offline" in msg.lower() or "not" in msg.lower() and "live" in msg.lower() \
               or "UserOffline" in name or "LiveNotFound" in name:
                _log(f"{uid} is not live right now — will re-check in {retry_sec:.0f}s")
            else:
                _log(f"connect failed ({name}: {msg}) — re-check in {retry_sec:.0f}s")
        finally:
            session.close()
            try:
                await client.disconnect()
            except Exception:
                pass

        if stop.is_set():
            break
        # Wait before checking whether the creator is live again.
        try:
            await asyncio.wait_for(stop.wait(), timeout=retry_sec)
        except asyncio.TimeoutError:
            pass
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--username", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--retry-sec", type=float,
                    default=float(os.environ.get("CHAT_RETRY_SEC", "30")))
    ap.add_argument("--api-key",
                    default=os.environ.get("SIGN_API_KEY")
                    or os.environ.get("EULER_API_KEY") or "")
    args = ap.parse_args()
    try:
        return asyncio.run(run(args.username, Path(args.output_dir),
                               args.retry_sec, args.api_key or None))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
