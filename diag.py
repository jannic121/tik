#!/usr/bin/env python3
"""
Diagnostic for the TikTok recorder control plane.
Run this next to control_plane.py:  python3 diag.py
It does NOT need the server to be running — it boots an in-process copy.
Paste the entire output back.
"""
import os, sys, re, pathlib, tempfile, traceback

print("=" * 60)
print("TT RECORDER DIAGNOSTIC")
print("=" * 60)

# 0. Which file are we actually testing?
cp_path = pathlib.Path("control_plane.py").resolve()
print(f"\n[0] control_plane.py: {cp_path}")
if not cp_path.exists():
    print("    ✗ NOT FOUND in current directory. cd to the right folder.")
    sys.exit(1)
src = cp_path.read_text()
print(f"    size: {len(src)} bytes")
print(f"    'let _currentDeployType' count: {src.count('let _currentDeployType')} (must be 1)")
print(f"    has _init_schema: {'def _init_schema' in src}")
print(f"    has storage_servers table: {'storage_servers' in src}")
print(f"    has /api/storage endpoint: {'/api/storage' in src}")

# 1. Python import
print("\n[1] Importing module...")
tmp = tempfile.mkdtemp()
os.environ.update({
    "CONTROL_PLANE_PASSWORD": "diagpw",
    "CONTROL_PLANE_DB": os.path.join(tmp, "diag.sqlite"),
    "CONTROL_PLANE_SECRET_FILE": os.path.join(tmp, "diag.secret"),
    "UPLOAD_WORKER_ENABLED": "0",
})
try:
    import control_plane as cp
    print("    ✓ imported OK")
except Exception:
    print("    ✗ IMPORT FAILED:")
    traceback.print_exc()
    sys.exit(1)

# 2. Check the REAL database (the one the server uses)
print("\n[2] Checking your real database...")
real_db = os.environ.get("CONTROL_PLANE_DB_REAL") or str(
    pathlib.Path.home() / ".tt-recorder" / "control.sqlite")
print(f"    real DB path: {real_db}")
if pathlib.Path(real_db).exists():
    import sqlite3
    try:
        rc = sqlite3.connect(real_db)
        tables = [r[0] for r in rc.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        print(f"    tables: {tables}")
        tcols = [r[1] for r in rc.execute("PRAGMA table_info(transfers)")]
        print(f"    transfers cols: {tcols}")
        print(f"    has storage_pk: {'storage_pk' in tcols}")
        print(f"    has storage_servers table: {'storage_servers' in tables}")
        print(f"    backends: {rc.execute('SELECT COUNT(*) FROM backends').fetchone()[0]}")
        print(f"    watchers: {rc.execute('SELECT COUNT(*) FROM watchers').fetchone()[0]}")
        rc.close()
    except Exception:
        print("    ✗ DB READ FAILED:")
        traceback.print_exc()
else:
    print("    (real DB doesn't exist yet — fresh install)")

# 3. JS syntax in the served page
print("\n[3] Checking served HTML + JS...")
try:
    from fastapi.testclient import TestClient
    c = TestClient(cp.app, follow_redirects=True)
    r = c.post("/api/login", json={"password": "diagpw"})
    print(f"    login: HTTP {r.status_code}")
    page = c.get("/").text
    print(f"    dashboard size: {len(page)} bytes")
    js = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", page, re.DOTALL))
    # crude JS sanity: balanced braces, single _currentDeployType
    print(f"    JS size: {len(js)} bytes")
    print(f"    JS 'let _currentDeployType': {js.count('let _currentDeployType')} (must be 1)")
    open(os.path.join(tmp, "page.js"), "w").write(js)
    print(f"    JS written to {tmp}/page.js — run: node -e \"new Function(require('fs').readFileSync('{tmp}/page.js','utf8'))\"")
except Exception:
    print("    ✗ PAGE RENDER FAILED:")
    traceback.print_exc()

# 4. Probe every read endpoint
print("\n[4] Probing API endpoints...")
for method, ep in [("GET","/api/me"),("GET","/api/backends"),("GET","/api/watchers"),
                   ("GET","/api/storage"),("GET","/api/files"),
                   ("GET","/api/transfer-statuses"),("GET","/api/transfer-progress"),
                   ("GET","/api/transcript-statuses")]:
    try:
        resp = c.request(method, ep)
        body = resp.text[:80].replace("\n", " ")
        flag = "✓" if resp.status_code == 200 else "✗"
        print(f"    {flag} {method} {ep}: {resp.status_code}  {body}")
    except Exception as e:
        print(f"    ✗ {method} {ep}: EXCEPTION {type(e).__name__}: {e}")

print("\n" + "=" * 60)
print("Done. Paste this entire output back.")
print("Also: open the page in your browser, press F12 → Console,")
print("hard-refresh (Ctrl-Shift-R), and paste any red errors.")
print("=" * 60)
