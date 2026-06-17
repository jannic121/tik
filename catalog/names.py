"""Filename parsing for TikTok recordings.

Recordings are named ``TK_<user>_<YYYY.MM.DD>_<HH-MM-SS>.mp4``. While a recording
is in flight (or if the recorder died before remuxing) the intermediate carries a
``_flv`` suffix: ``TK_<user>_<YYYY.MM.DD>_<HH-MM-SS>_flv.mp4``. The final form is
simply the intermediate with ``_flv`` removed — same creator, same timestamp — so
the two always match on filename alone.
"""

from __future__ import annotations

import re
import time
from typing import NamedTuple, Optional

_FLV = "_flv.mp4"

_RE = re.compile(
    r"^TK_(?P<user>.+)_(?P<date>\d{4}\.\d{2}\.\d{2})_(?P<time>\d{2}-\d{2}-\d{2})"
    r"(?P<flv>_flv)?\.mp4$"
)


class RecordingName(NamedTuple):
    creator: str            # normalised: no leading @, lowercase
    filename: str           # final form (without _flv)
    is_flv: bool            # True if the parsed name was the _flv intermediate
    started_at: Optional[float]   # epoch seconds parsed from the name, or None


def parse_recording_name(name: str) -> Optional[RecordingName]:
    """Parse a recording filename. Returns None if it isn't one of ours."""
    m = _RE.match(name)
    if not m:
        return None
    creator = m.group("user").lstrip("@").lower()
    is_flv = bool(m.group("flv"))
    final = name[: -len(_FLV)] + ".mp4" if is_flv else name
    try:
        started = time.mktime(
            time.strptime(f"{m.group('date')}_{m.group('time')}", "%Y.%m.%d_%H-%M-%S")
        )
    except (ValueError, OverflowError):
        started = None
    return RecordingName(creator=creator, filename=final, is_flv=is_flv, started_at=started)


def final_name(name: str) -> str:
    """The finalised .mp4 name for any recording filename (no-op if already final)."""
    return name[: -len(_FLV)] + ".mp4" if name.endswith(_FLV) else name
