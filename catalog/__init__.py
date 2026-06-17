"""TT Recorder strangler catalog (Phase 0/1).

A shadow source-of-truth that runs alongside the existing system without changing
its behaviour. See catalog/README.md for the migration plan and how to enable it.
"""

from __future__ import annotations

from . import backfill, ingest, parity
from .db import Catalog, default_path
from .names import final_name, parse_recording_name

__all__ = [
    "Catalog", "default_path",
    "parse_recording_name", "final_name",
    "backfill", "parity", "ingest",
]
