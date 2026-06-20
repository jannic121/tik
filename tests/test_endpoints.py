"""Smoke tests for the control-plane HTTP surface. No live fleet needed — runs
against a fresh temp DB with the catalog enabled. Locks the response contract of
the endpoints added across the recent feature work so a refactor can't silently
break them.

Run either way:
    python -m pytest tests/test_endpoints.py -q
    python tests/test_endpoints.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_d = tempfile.mkdtemp(prefix="cp-test-")
os.environ.update({
    "CONTROL_PLANE_PASSWORD": "pw",
    "CONTROL_PLANE_DB": os.path.join(_d, "control.sqlite"),
    "CONTROL_PLANE_SECRET_FILE": os.path.join(_d, "secret"),
    "CATALOG_DB": os.path.join(_d, "catalog.sqlite"),
    "CATALOG_ENABLED": "1",
    "UPLOAD_WORKER_ENABLED": "0",
})

import control_plane as cp                                      # noqa: E402
from fastapi.testclient import TestClient                       # noqa: E402

# The catalog is only wired up in lifespan; create it directly for the tests.
from catalog import Catalog                                     # noqa: E402
cp._catalog = Catalog()


def _client() -> TestClient:
    c = TestClient(cp.app, follow_redirects=True)
    r = c.post("/api/login", json={"password": "pw"})
    assert r.status_code == 200, r.text
    return c


_c = _client()


# ---- auth ----------------------------------------------------------------

def test_requires_login():
    fresh = TestClient(cp.app, follow_redirects=False)
    r = fresh.get("/api/files")
    assert r.status_code in (401, 403), r.status_code


def test_login_rejects_bad_password():
    fresh = TestClient(cp.app, follow_redirects=True)
    r = fresh.post("/api/login", json={"password": "nope"})
    assert r.status_code in (401, 403)


# ---- read endpoints all 200 + JSON-shaped --------------------------------

def test_core_get_endpoints_ok():
    for path in (
        "/", "/app.js",
        "/api/backends", "/api/storage", "/api/watchers", "/api/routing",
        "/api/files", "/api/files?source=catalog", "/api/files?source=live",
        "/api/transfer-statuses", "/api/transfer-progress",
        "/api/transcript-statuses", "/api/file-locations",
        "/api/chat-matches", "/api/archive-statuses", "/api/archive/overview",
        "/api/salvaged-statuses",
        "/api/storage-capacity", "/api/update-status",
        "/api/alerts/config",
        "/api/transcript-index/status", "/api/chat-index/status",
        "/api/audio-transcribe/status",
        "/api/transcript-search?q=hello", "/api/chat/search?q=hello",
        "/api/chat/files",
    ):
        r = _c.get(path)
        assert r.status_code == 200, f"{path} -> {r.status_code}: {r.text[:200]}"


def test_files_list_shape_is_a_list():
    for src in ("catalog", "live"):
        r = _c.get(f"/api/files?source={src}")
        assert r.status_code == 200 and isinstance(r.json(), list)


def test_app_js_served_as_javascript():
    r = _c.get("/app.js")
    assert "javascript" in r.headers["content-type"]
    assert "function switchTab" in r.text


# ---- alerts config round-trips -------------------------------------------

def test_alerts_config_roundtrip():
    d = _c.get("/api/alerts/config").json()
    assert d["enabled"] is False and d["channel"] == "webhook"
    r = _c.post("/api/alerts/config", json={
        "enabled": True, "channel": "ntfy", "url": "https://ntfy.sh/x", "disk_pct": 88})
    assert r.status_code == 200
    d2 = _c.get("/api/alerts/config").json()
    assert d2["enabled"] and d2["channel"] == "ntfy" and d2["disk_pct"] == 88
    # restore
    _c.post("/api/alerts/config", json={"enabled": False, "channel": "webhook", "url": ""})


def test_alert_test_returns_ok_field_without_crashing():
    r = _c.post("/api/alerts/test", json={"channel": "webhook", "url": "http://127.0.0.1:9/"})
    assert r.status_code == 200 and "ok" in r.json()   # unreachable -> ok False, no crash


def test_alert_invalid_channel_rejected():
    r = _c.post("/api/alerts/config", json={"channel": "carrier-pigeon", "url": "x"})
    assert r.status_code == 422


# ---- per-storage one-click endpoints 404 on unknown server ---------------

def test_storage_actions_404_on_unknown_server():
    assert _c.post("/api/storage/nope/evict-now").status_code == 404
    assert _c.post("/api/storage/nope/retention", json={"evict_days": 7}).status_code == 404
    assert _c.post("/api/storage/nope/archive-eviction", json={"delete_local": True}).status_code == 404


# ---- catalog-backed Files surfaces a cataloged recording -----------------

def test_catalog_files_lists_a_cataloged_recording():
    import asyncio
    cat = cp._catalog
    fn = "TK_smoketest_2026.06.10_14-00-00.mp4"
    cp.db.execute(
        "INSERT OR IGNORE INTO backends (id,backend_id,url,auth_token,region,added_at,"
        "last_health_check,last_health_ok,last_health_data) VALUES "
        "('b1','rec-1','http://127.0.0.1:8000','t','eu',?,?,1,'{}')",
        (time.time(), time.time()))
    cp.db.commit()
    rid = cat.upsert_recording(filename=fn, creator="smoketest", node="rec-1",
                               byte_size=123, started_at=time.time())
    cat.add_location(rid, "local", "rec-1", "/data/recordings/smoketest/" + fn,
                     byte_size=123)
    items = asyncio.run(cp._files_from_catalog())
    assert any(i["filename"] == fn for i in items), [i.get("filename") for i in items]


# ---- watcher migration guards --------------------------------------------

def test_migrate_unknown_watcher_404():
    r = _c.post("/api/watchers/nobody/migrate", json={"backend_pk": "x"})
    assert r.status_code == 404, r.text


def test_migrate_unknown_target_404():
    # Seed a watcher pointing at an existing backend, aim it at a missing target.
    cp.db.execute(
        "INSERT OR IGNORE INTO backends (id,backend_id,url,auth_token,region,added_at,"
        "last_health_check,last_health_ok,last_health_data) VALUES "
        "('mb1','mig-1','http://127.0.0.1:8000','t','eu',?,?,1,'{}')",
        (time.time(), time.time()))
    cp.db.execute(
        "INSERT OR IGNORE INTO watchers (username,backend_pk,automatic_interval_min,created_at)"
        " VALUES ('migme','mb1',3,?)", (time.time(),))
    cp.db.commit()
    r = _c.post("/api/watchers/migme/migrate", json={"backend_pk": "does-not-exist"})
    assert r.status_code == 404, r.text


# ---- audio-first transcription -------------------------------------------

def test_audio_transcribe_status_shape():
    d = _c.get("/api/audio-transcribe/status").json()
    assert "enabled" in d and "in_flight" in d and "interval_sec" in d


def test_audio_transcribe_run_now_409_when_disabled():
    # Default config has TRANSCRIBE_FROM_AUDIO unset → disabled → 409, not a crash.
    r = _c.post("/api/audio-transcribe/run-now")
    assert r.status_code == 409, r.text


def test_audio_transcribe_config_toggle_roundtrip():
    assert _c.get("/api/audio-transcribe/status").json()["enabled"] is False
    assert _c.post("/api/audio-transcribe/config", json={"enabled": True}).status_code == 200
    assert _c.get("/api/audio-transcribe/status").json()["enabled"] is True
    # now run-now is allowed (no backends, so it just ships 0)
    r = _c.post("/api/audio-transcribe/run-now")
    assert r.status_code == 200 and "pushed" in r.json()
    # restore
    _c.post("/api/audio-transcribe/config", json={"enabled": False})


def test_storage_audio_only_404_on_unknown_server():
    r = _c.post("/api/storage/nope/audio-only", json={"enabled": True})
    assert r.status_code == 404


# ---- catalog cutover -----------------------------------------------------

def test_default_files_source_is_catalog():
    assert cp.FILES_SOURCE == "catalog"


def test_transcript_statuses_served_from_catalog():
    import asyncio
    cat = cp._catalog
    fn = "TK_tsmoke_2026.06.11_09-00-00.mp4"
    rid = cat.upsert_recording(filename=fn, creator="tsmoke", started_at=time.time())
    cat.set_transcript(rid, "done")
    m = cat.transcript_status_map()
    assert m.get(fn) == "done", m
    # endpoint prefers the catalog and surfaces it
    got = _c.get("/api/transcript-statuses").json()
    assert got.get(fn) == "done", got


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
