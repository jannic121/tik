"""Tests for the strangler catalog. No live fleet needed — everything runs against
temp SQLite databases.

Run either way:
    python -m pytest tests/test_catalog.py -q
    python tests/test_catalog.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog import Catalog, parse_recording_name              # noqa: E402
from catalog import backfill, ingest, parity                   # noqa: E402


def _cat() -> Catalog:
    fd, path = tempfile.mkstemp(suffix=".sqlite", prefix="cat-test-")
    os.close(fd)
    os.unlink(path)
    return Catalog(path)


# ---- names ---------------------------------------------------------------

def test_parse_final_and_flv():
    a = parse_recording_name("TK_alice_2026.06.05_14-22-00.mp4")
    assert a and a.creator == "alice" and not a.is_flv
    assert a.filename == "TK_alice_2026.06.05_14-22-00.mp4"
    assert a.started_at is not None

    b = parse_recording_name("TK_alice_2026.06.05_14-22-00_flv.mp4")
    assert b and b.is_flv
    # the _flv intermediate normalises to the same final name + timestamp
    assert b.filename == a.filename and b.started_at == a.started_at


def test_parse_username_with_underscores_and_nonmatch():
    p = parse_recording_name("TK_cool_user_99_2026.01.02_03-04-05.mp4")
    assert p and p.creator == "cool_user_99"
    assert parse_recording_name("random.mp4") is None
    assert parse_recording_name("TK_x_2026.06.05_14-22-00.txt") is None


# ---- recordings ----------------------------------------------------------

def test_upsert_dedup_and_state_only_advances():
    cat = _cat()
    r1 = cat.upsert_recording(filename="TK_bob_2026.06.05_10-00-00_flv.mp4", node="rec1")
    r2 = cat.upsert_recording(filename="TK_bob_2026.06.05_10-00-00.mp4",
                              byte_size=1234, state="stored")
    assert r1 == r2                                   # flv + final collapse to one row
    rec = cat.get_recording(r1)
    assert rec["byte_size"] == 1234 and rec["state"] == "stored"

    # a late 'discovered' must not regress an already-'stored' recording
    cat.upsert_recording(filename="TK_bob_2026.06.05_10-00-00.mp4", state="discovered")
    assert cat.get_recording(r1)["state"] == "stored"


def test_locations_unique_per_tier_store():
    cat = _cat()
    rid = cat.upsert_recording(filename="TK_c_2026.06.05_11-00-00.mp4")
    cat.add_location(rid, "local", "rec1", "/data/recordings/c/TK_c_2026.06.05_11-00-00.mp4",
                     byte_size=10)
    cat.add_location(rid, "local", "rec1", "/data/recordings/c/TK_c_2026.06.05_11-00-00.mp4",
                     byte_size=20)                    # same tier+store -> upsert, not dup
    cat.add_location(rid, "cloud", "b2", "c/TK_c_2026.06.05_11-00-00.mp4")
    locs = cat.locations(rid)
    assert len(locs) == 2
    assert {l["tier"] for l in locs} == {"local", "cloud"}
    assert next(l for l in locs if l["tier"] == "local")["byte_size"] == 20


# ---- jobs ----------------------------------------------------------------

def test_job_dedup_claim_complete_fail():
    cat = _cat()
    rid = cat.upsert_recording(filename="TK_d_2026.06.05_12-00-00.mp4", state="stored")
    j1 = cat.enqueue_job("transcribe", recording_id=rid, shadow=False)
    j2 = cat.enqueue_job("transcribe", recording_id=rid, shadow=False)
    assert j1 and j2 is None                          # no duplicate open job

    claimed = cat.claim_job()                          # j1 is non-shadow
    assert claimed and claimed["id"] == j1 and claimed["state"] == "running"
    assert cat.claim_job() is None                     # nothing else runnable

    cat.fail_job(j1, "boom", backoff=0)                # back to ready (attempts<max)
    again = cat.claim_job()
    assert again and again["id"] == j1
    cat.complete_job(j1)
    summary = {(s["kind"], s["state"]): s["count"] for s in cat.jobs_summary()}
    assert summary.get(("transcribe", "done")) == 1


def test_shadow_jobs_not_claimed_by_default():
    cat = _cat()
    rid = cat.upsert_recording(filename="TK_e_2026.06.05_13-00-00.mp4", state="stored")
    cat.enqueue_job("transcribe", recording_id=rid, shadow=True)
    assert cat.claim_job() is None                     # shadow excluded
    assert cat.claim_job(include_shadow=True) is not None


# ---- backfill ------------------------------------------------------------

def _fake_control_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".sqlite", prefix="ctl-")
    os.close(fd)
    c = sqlite3.connect(path)
    c.execute(
        "CREATE TABLE transfers (id TEXT, backend_pk TEXT, backend_label TEXT, "
        "storage_pk TEXT, storage_label TEXT, username TEXT, filename TEXT, "
        "src_path TEXT, size_bytes INTEGER, status TEXT, recorder_deleted INTEGER)"
    )
    c.executemany(
        "INSERT INTO transfers VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("1", "bpk", "rec1", "spk", "sto1", "alice",
             "TK_alice_2026.06.05_14-22-00.mp4",
             "/data/recordings/alice/TK_alice_2026.06.05_14-22-00.mp4", 500, "done", 0),
            ("2", "bpk", "rec1", None, None, "bob",
             "TK_bob_2026.06.05_15-00-00.mp4",
             "/data/recordings/bob/TK_bob_2026.06.05_15-00-00.mp4", 600, "pending", 0),
        ],
    )
    c.commit()
    c.close()
    return path


def test_backfill_from_control_db():
    cat = _cat()
    ctl = _fake_control_db()
    try:
        out = backfill.backfill_from_control_db(cat, ctl)
        assert out["transfers_imported"] == 2
        alice = cat.find_by_filename("TK_alice_2026.06.05_14-22-00.mp4")
        assert alice["state"] == "stored"
        # alice has both a recorder-local and a storage-local location
        assert len(cat.locations(alice["id"])) == 2
        bob = cat.find_by_filename("TK_bob_2026.06.05_15-00-00.mp4")
        assert bob["state"] == "discovered"
    finally:
        os.unlink(ctl)


def test_ingest_inventory_and_transcript_sidecar():
    cat = _cat()
    inv = [
        {"filename": "TK_z_2026.06.05_16-00-00.mp4", "path": "/d/z/TK_z_2026.06.05_16-00-00.mp4",
         "username": "z", "size_bytes": 900, "store": "sto1"},
        {"filename": "TK_z_2026.06.05_16-00-00.txt", "path": "/d/z/TK_z_2026.06.05_16-00-00.txt",
         "username": "z", "size_bytes": 12, "store": "sto1"},
        {"filename": "TK_z_2026.06.05_16-00-00_flv.mp4", "path": "/d/z/x_flv.mp4",
         "username": "z", "size_bytes": 850, "store": "sto1"},      # intermediate ignored
    ]
    out = backfill.ingest_inventory(cat, inv)
    assert out["recordings_seen"] == 1 and out["transcripts_seen"] == 1
    rec = cat.find_by_filename("TK_z_2026.06.05_16-00-00.mp4")
    # transcript state is decoupled from recording state: the .txt marks the
    # transcript done, but the recording stays 'stored'
    assert rec["state"] == "stored"
    t = cat.conn.execute("SELECT state FROM transcripts WHERE recording_id=?",
                         (rec["id"],)).fetchone()
    assert t and t["state"] == "done"


# ---- parity --------------------------------------------------------------

def test_parity_detects_drift():
    cat = _cat()
    # catalog knows two recordings
    cat.upsert_recording(filename="TK_a_2026.06.05_10-00-00.mp4", byte_size=100, state="stored")
    cat.upsert_recording(filename="TK_b_2026.06.05_11-00-00.mp4", byte_size=200, state="stored")
    # live shows: a (wrong size), c (untracked); b is missing
    live = [
        {"filename": "TK_a_2026.06.05_10-00-00.mp4", "size_bytes": 999, "store": "sto1"},
        {"filename": "TK_c_2026.06.05_12-00-00.mp4", "size_bytes": 300, "store": "sto1"},
    ]
    rep = parity.compute_parity(cat, live)
    assert rep["counts"]["live_only"] == 1
    assert rep["counts"]["catalog_missing"] == 1
    assert rep["counts"]["size_mismatch"] == 1
    assert rep["live_only"][0]["filename"] == "TK_c_2026.06.05_12-00-00.mp4"


# ---- Phase 1 ingest ------------------------------------------------------

def test_on_event_session_lifecycle_and_compare():
    cat = _cat()
    node = "rec1"
    fn = "TK_live_2026.06.05_18-00-00.mp4"
    flv = "/data/recordings/live/TK_live_2026.06.05_18-00-00_flv.mp4"
    final = "/data/recordings/live/TK_live_2026.06.05_18-00-00.mp4"

    ingest.on_event(cat, {"backend_id": node,
                          "event": {"kind": "session_started", "username": "live",
                                    "detail": {"path": flv}}})
    rec = cat.find_by_filename(fn)
    assert rec and rec["state"] == "recording"

    ingest.on_event(cat, {"backend_id": node,
                          "event": {"kind": "session_ended", "username": "live",
                                    "detail": {"flv_path": flv, "final_path": final}}})
    rec = cat.find_by_filename(fn)
    assert rec["state"] == "stored"
    # one shadow transcribe job was recorded; it is NOT runnable by a real worker
    assert cat.claim_job() is None
    summary = {(s["kind"], s["state"], s["shadow"]): s["count"] for s in cat.jobs_summary()}
    assert summary.get(("transcribe", "ready", 1)) == 1

    # comparison against a live worker that already finished it -> "already_done_live"
    cmp = ingest.compare_to_live(cat, {fn: "done"})
    assert cmp["counts"]["already_done_live"] == 1
    assert cmp["counts"]["catalog_only"] == 0


def test_on_event_never_raises_on_garbage():
    cat = _cat()
    for bad in (None, {}, {"event": None}, {"event": {"kind": "session_ended"}},
                {"event": {"kind": "session_started", "detail": {"path": "junk.mp4"}}}):
        ingest.on_event(cat, bad)            # must not raise
    assert cat.stats()["recordings"] == 0


# ---- R1: state reconciliation --------------------------------------------

def test_reconcile_evicts_absent():
    cat = _cat()
    a = cat.upsert_recording(filename="TK_a_2026.06.05_10-00-00.mp4", state="stored")
    b = cat.upsert_recording(filename="TK_b_2026.06.05_11-00-00.mp4", state="stored")
    out = backfill.reconcile_states(cat, {"TK_a_2026.06.05_10-00-00.mp4"})  # only a is live
    assert out["evicted"] == 1
    assert cat.get_recording(a)["state"] == "stored"
    assert cat.get_recording(b)["state"] == "evicted"


# ---- R2: transcript statuses ---------------------------------------------

def test_ingest_transcript_statuses_marks_done_and_cancels_jobs():
    cat = _cat()
    fn = "TK_t_2026.06.05_12-00-00.mp4"
    rid = cat.upsert_recording(filename=fn, state="stored")
    cat.enqueue_job("transcribe", recording_id=rid, shadow=True)
    out = backfill.ingest_transcript_statuses(cat, {fn: "done", "unknown.mp4": "done"})
    assert out["transcribed"] == 1
    t = cat.conn.execute("SELECT state FROM transcripts WHERE recording_id=?",
                         (rid,)).fetchone()
    assert t["state"] == "done"
    js = {(s["kind"], s["state"]): s["count"] for s in cat.jobs_summary()}
    assert js.get(("transcribe", "cancelled")) == 1          # stale shadow job cancelled
    assert all(r["id"] != rid for r in cat.transcribe_backlog())  # gone from backlog


# ---- Phase 2: backlog + go-live -----------------------------------------

def test_transcribe_backlog_and_promote():
    cat = _cat()
    r1 = cat.upsert_recording(filename="TK_p_2026.06.05_09-00-00.mp4", state="stored")
    r2 = cat.upsert_recording(filename="TK_q_2026.06.05_09-30-00.mp4", state="stored")
    cat.set_transcript(r2, "done")                  # already done -> not in backlog
    bl = cat.transcribe_backlog()
    assert len(bl) == 1 and bl[0]["id"] == r1
    assert ingest.promote_backlog(cat) == 1
    job = cat.claim_job(kinds=["transcribe"])       # promoted job is REAL + claimable
    assert job and job["recording_id"] == r1


# ---- runner --------------------------------------------------------------

def _run_all() -> int:
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
        except Exception as e:
            failed += 1
            import traceback
            print(f"  FAIL  {fn.__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
