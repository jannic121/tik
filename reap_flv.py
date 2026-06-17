#!/usr/bin/env python3
"""reap_flv.py — clean up orphaned _flv.mp4 files on a recorder node.

The same logic the in-process reaper (app.py) runs automatically, but as a
one-shot you can run by hand — and DRY-RUN by default, so you can preview exactly
what it would do before trusting it.

  final exists  -> delete the redundant flv
  no final      -> remux (lossless stream-copy) to the final name, then delete the flv
  remux fails   -> leave it, report it (only deleted with --delete-corrupt)

Safe by default: DRY RUN. Add --apply to make changes. Run as root or the 'tt'
service user so the open-file check can see the recorder's fds.

  python3 reap_flv.py                  # dry run, default /data/recordings
  python3 reap_flv.py --apply          # delete redundant flvs, salvage orphans
  python3 reap_flv.py --root /data/recordings --settle 300 --apply
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
import time
from pathlib import Path

SUF = "_flv.mp4"


def held_open() -> set:
    out: set = set()
    for d in glob.glob("/proc/[0-9]*/fd"):
        try:
            for fd in os.listdir(d):
                try:
                    out.add(os.path.realpath(os.path.join(d, fd)))
                except OSError:
                    pass
        except OSError:
            pass
    return out


def active(p: Path, now: float, settle: int, held: set) -> bool:
    try:
        if now - p.stat().st_mtime < settle:
            return True
    except OSError:
        return True
    try:
        return os.path.realpath(p) in held
    except OSError:
        return True


def remux(flv: Path, final: Path) -> bool:
    tmp = final.with_name(final.name + ".remuxing")
    try:
        tmp.unlink()
    except OSError:
        pass
    try:
        r = subprocess.run(
            ["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-i", str(flv),
             "-c", "copy", "-movflags", "+faststart", "-f", "mp4", str(tmp)],
            timeout=900)
    except Exception:
        r = None
    if r and r.returncode == 0 and tmp.exists() and tmp.stat().st_size > 0:
        try:
            os.replace(tmp, final)
            return True
        except OSError:
            pass
    try:
        tmp.unlink()
    except OSError:
        pass
    return False


def h(n: float) -> str:
    f = float(n)
    for u in "B KB MB GB TB".split():
        if f < 1024:
            return f"{f:.1f}{u}"
        f /= 1024
    return f"{f:.1f}PB"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("RECORDINGS_ROOT", "/data/recordings"))
    ap.add_argument("--settle", type=int, default=300,
                    help="seconds a file must be untouched before it's eligible")
    ap.add_argument("--apply", action="store_true", help="actually act (default: dry run)")
    ap.add_argument("--delete-corrupt", action="store_true",
                    help="delete orphans that won't remux (DATA LOSS)")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"root not found: {root}")
        return 1
    held, now = held_open(), time.time()
    red = red_b = orph = corrupt = skip = 0
    for flv in sorted(root.rglob("*" + SUF)):
        if not flv.is_file():
            continue
        if active(flv, now, args.settle, held):
            skip += 1
            continue
        sz = flv.stat().st_size
        final = flv.with_name(flv.name[:-len(SUF)] + ".mp4")
        if final.exists() and final.stat().st_size > 0 and not active(final, now, args.settle, held):
            print(f"[redundant] {flv.name}  {h(sz)}  (final exists)")
            red += 1
            red_b += sz
            if args.apply:
                try:
                    flv.unlink()
                except OSError as e:
                    print(f"   ! {e}")
        else:
            print(f"[orphan]    {flv.name}  {h(sz)}  -> remux")
            if args.apply:
                if remux(flv, final):
                    orph += 1
                    print(f"   salvaged -> {final.name}")
                    try:
                        flv.unlink()
                    except OSError as e:
                        print(f"   ! {e}")
                else:
                    corrupt += 1
                    print("   ! remux failed (truncated/corrupt)")
                    if args.delete_corrupt:
                        try:
                            flv.unlink()
                            red_b += sz
                            print("   deleted (corrupt)")
                        except OSError as e:
                            print(f"   ! {e}")
            else:
                orph += 1
    print(f"\n{'APPLIED' if args.apply else 'DRY RUN — nothing changed'}: "
          f"{red} redundant (~{h(red_b)} freed), {orph} orphan(s) remuxed/kept, "
          f"{corrupt} corrupt, {skip} active/too-fresh skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
