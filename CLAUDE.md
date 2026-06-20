# TikTok Live Recorder — Claude Code Context

## What this project is

A self-hosted system to monitor TikTok creators, record their live streams across
multiple VPS backends, automatically transfer recordings to a storage server, and
transcribe them with Whisper. A single-file FastAPI web app (`control_plane.py`)
provides the UI and orchestrates everything.

## Versioning

The build id lives in the `VERSION` file (one line, e.g. `2026.06.03`). Bump it on
every change. All three components read it (`_read_version()` → `BUILD`), it travels
to nodes on deploy/push, and the Updates tab + version banner compare each node's
VERSION against the control plane's — so bumping VERSION is what makes update
detection work. Nothing auto-generates it; keep it current.

**Append a short random suffix** (e.g. `2026.06.17.tjey`) rather than a bare date.
Update detection is a string-equality check, so two builds on the same day with a
date-only id look identical and the Updates tab won't flag anything — the suffix
keeps every build distinct.

---

## File map

| File | Runs on | Purpose |
|---|---|---|
| `control_plane.py` | Local machine | FastAPI web UI + upload worker + SSH deploy. Port 8080. ~1700 lines. |
| `app.py` | Recorder VPS | Backend HTTP layer: auth, watcher CRUD, file listing, download, cookies. Port 8000. |
| `watcher.py` | Recorder VPS | Supervises one Michele0303 subprocess per creator. State machine. |
| `transcription_worker.py` | Storage VPS | Whisper transcription + file receive endpoint. Port 8090. |
| `provision.sh` | Uploaded to recorder VPS | Full provisioner: apt, uv, venv, systemd unit `tt-backend`. |
| `provision_storage.sh` | Uploaded to storage VPS | Provisioner for storage: faster-whisper, systemd unit `tt-transcription`. |
| `update-recorder.sh` | Run on recorder VPS | Rolls Michele0303 to a new git tag. |
| `OPERATIONS.md` | Reference | Full ops manual. |

---

## Architecture

```
Browser → control_plane.py :8080
  ├── Recorder backends :8000  (watcher.py + app.py + Michele0303)
  └── Storage servers   :8090  (transcription_worker.py)

Pipeline (fully automatic):
Creator live → Michele0303 records TK_<user>_<ts>_flv.mp4
  → remux on stop → TK_<user>_<ts>.mp4
  → upload worker (every 5 min) streams backend→storage, deletes from backend
  → transcription worker (scan every 30s) → .txt / .srt / .json
  → Files tab shows "✓ On storage" + "✓ Available"
```

---

## Running locally

```bash
pip install --break-system-packages fastapi 'uvicorn[standard]' httpx paramiko
export CONTROL_PLANE_PASSWORD=yourpassword
python3 control_plane.py
# Open http://localhost:8080
```

All deploy files (provision.sh, watcher.py, app.py, provision_storage.sh,
transcription_worker.py) must be in the same directory as control_plane.py.

---

## Key environment variables

### Control plane
| Var | Default | Purpose |
|---|---|---|
| `CONTROL_PLANE_PASSWORD` | **required** | Login password |
| `CONTROL_PLANE_DB` | `~/.tt-recorder/control.sqlite` | SQLite path |
| `CONTROL_PLANE_PORT` | `8080` | Bind port |
| `DEPLOY_FILES_DIR` | script dir | Where provision scripts live |
| `UPLOAD_WORKER_ENABLED` | `1` | Auto-transfer recordings |
| `UPLOAD_INTERVAL_SEC` | `300` | Transfer poll interval |
| `UPLOAD_DELETE_AFTER` | `1` | Delete from backend after transfer |
| `HEALTH_INTERVAL_SEC` | `30` | Backend health check interval |

### Recorder backend (`/etc/tt-backend.env`)
`BACKEND_ID`, `AUTH_TOKEN` (64 hex), `REGION`, `REPO_PATH`, `PYTHON_EXECUTABLE`,
`RECORDINGS_ROOT` (`/data/recordings`), `STATE_FILE` (`/var/lib/tt-recorder/state.json`),
`MAX_WATCHERS` (30)

### Storage server (`/etc/tt-storage.env`)
`WHISPER_WATCH_DIR` (`/data/recordings`), `WHISPER_MODEL` (`base`), `WHISPER_HOST`,
`WHISPER_PORT` (`8090`), `WHISPER_AUTH_TOKEN`, `HF_HOME` (`/opt/tt-storage/models`)

---

## SQLite schema (control plane)

```sql
backends        -- registered recorder VPSes: id, url, auth_token, health
watchers        -- creator watchlist: username, backend_pk, interval
storage_servers -- storage/transcription servers: id, url, token, health, disk
transfers       -- upload worker state: filename, status, storage_pk, attempts
events          -- lifecycle events from backends
config          -- key-value: transcript_url, transcript_token
```

### Critical migration
The `transfers` table gained `storage_pk TEXT` and `storage_label TEXT` columns.
The `storage_servers` table was added. Both are handled in `_open_db()` via
`CREATE TABLE IF NOT EXISTS` and `ALTER TABLE` guards — **runs automatically on startup**.

If you see `sqlite3.OperationalError: no such column: storage_pk`, the migration
didn't run. Fix: restart the control plane.

---

## JavaScript architecture (`app.js`, served separately)

The frontend JS lives in **`app.js`** (a real file next to `control_plane.py`),
served at `/app.js` and referenced from `dashboard.html` via
`<script src="/app.js?v=__BUILD__">`. It is loaded at startup into `APP_JS`, so
**restart the control plane after editing `app.js`** (same as `dashboard.html`).
It's a single classic (non-module) script, so all functions are globals and the
inline `onclick="..."` handlers in `dashboard.html` keep working.
**A JS syntax error silently breaks ALL tab switching and API calls.**

Key functions:
- `switchTab(name)` — shows/hides panels, calls the load function for that tab
- `loadBackends()` — fetches `/api/backends` + `/api/storage` (for colocation badge)
- `loadWatchers()` — fetches `/api/watchers`
- `loadFiles()` — fetches `/api/files`, `/api/transfer-statuses`, `/api/transfer-progress`
- `loadStorage()` — fetches `/api/storage`
- `doSearch()` — fetches `/api/transcript-search?q=`
- `setDeployType(type)` — switches Deploy tab between 'recorder', 'storage', 'push'
- `runDeploy()` — streams from `/api/deploy` or `/api/push-update`
- `openCookies(pk, label)` — opens cookie modal for a backend
- `autoRegisterBackend(host, token)` — called after recorder deploy
- `autoRegisterStorage(url, token)` — called after storage deploy

**To check for JS syntax errors** (now a direct lint of the real file):
```bash
node -e "try{new Function(require('fs').readFileSync('app.js','utf8')); console.log('OK')}catch(e){console.log('ERROR:',e.message)}"
```

`app.js` ships alongside `control_plane.py` + `dashboard.html` — include it in
any control-plane deploy (`cp control_plane.py dashboard.html app.js VERSION …`)
and it's a required member of the self-update bundle.

---

## API surface

### Control plane endpoints
```
POST /api/login                         -- set session cookie
GET  /api/backends                      -- list with health data
POST /api/backends                      -- register (probes first)
DEL  /api/backends/{pk}                 -- remove
GET  /api/watchers                      -- list with live state
POST /api/watchers                      -- add {username, automatic_interval_min}
DEL  /api/watchers/{username}           -- stop and remove
GET  /api/files                         -- per-backend file inventory
GET  /api/files/download                -- proxy download
POST /api/files/delete                  -- delete from backend
GET  /api/transfer-statuses             -- {filename: status}
GET  /api/transfer-progress             -- {filename: {pct, bytes_done, size_bytes}}
POST /api/transfer/run-now              -- trigger immediate upload cycle
POST /api/transfer/queue                -- manually queue a file
GET  /api/storage                       -- list storage servers
POST /api/storage                       -- add/update storage server
DEL  /api/storage/{id}                  -- disconnect
POST /api/config/transcript-worker      -- compat shim → adds to storage registry
GET  /api/transcript-statuses           -- merged across all storage servers
GET  /api/transcript-view               -- find and return .txt from storage
GET  /api/transcript-search             -- full-text search across all storage
GET  /api/backends/{pk}/cookies         -- proxy to backend GET /cookies
POST /api/backends/{pk}/cookies         -- proxy to backend POST /cookies
POST /api/push-update                   -- SFTP watcher.py+app.py + restart
POST /api/deploy                        -- streaming SSH deploy
```

### Backend (app.py) endpoints
```
GET  /health             -- no auth
GET  /watchers           -- list
POST /watchers           -- add
DEL  /watchers/{u}       -- remove
GET  /files              -- MP4-only inventory per creator
GET  /files/download     -- stream file (traversal-checked)
DEL  /files              -- delete file (traversal-checked)
GET  /cookies            -- read cookies.json
POST /cookies            -- write cookies.json
```

### Storage worker (transcription_worker.py) endpoints
```
GET  /health                      -- no auth
GET  /status                      -- auth required (validates token)
GET  /files/inventory             -- list MP4s
PUT  /files/{username}/{filename} -- receive file (atomic write)
GET  /transcripts/all-statuses    -- {filename: done|pending|processing|none}
GET  /transcripts/view            -- return .txt content
GET  /transcripts/download-srt    -- stream .srt
GET  /transcripts/search?q=       -- full-text search with snippets
```

---

## Watcher state machine

```
starting → idle ──► recording ──► idle
               ↓ (crash)
             backoff (1s → 2s → 4s … 60s cap)
               ↓ (5 crashes)
             error  (manual restart needed)
```

Exit code 0 from Michele0303 = clean exit (creator offline, not a crash).
Exit code non-zero = process crashed.

---

## Upload worker behaviour

`_upload_cycle_inner()` runs every `UPLOAD_INTERVAL_SEC` seconds:
1. Fetch inventory from ALL healthy storage servers → `storage_fnames: dict[str, dict]`
   (filename → which storage server has it)
2. For each healthy backend, get file list
3. Files already in `storage_fnames` → write synthetic 'done' transfer record (colocation support)
4. Files not yet on storage → queue as 'pending'
5. Process ONE pending transfer per cycle (smallest first)
6. `_do_transfer()`: stream GET from backend → PUT to storage server → verify size → delete

**Colocation note:** When backend and storage share the same `/data/recordings` directory,
files appear in the storage inventory immediately. The upload worker detects this and marks
them 'done' WITHOUT deleting them (same disk, same file).

`_upload_active` flag prevents concurrent cycles.
Startup resets any 'transferring' transfers to 'pending' (crash recovery).

---

## Known bugs and fixes applied

1. **Duplicate JS variable** — `let _currentDeployType` declared twice silently breaks all JS.
   Check with node's `new Function()` before deploying.

2. **`storage_fnames` type** — was `set[str]`, changed to `dict[str, dict]` for colocation.
   Any `set.update()` call on it would fail.

3. **Path traversal** — `PUT /files/{username}/{filename}` validates segments against `..` and `/`.
   `GET /files/download` and `DELETE /files` use `resolve().relative_to()`.

4. **`ProtectHome=true` + HuggingFace cache** — service unit sets `HF_HOME=/opt/tt-storage/models`
   to avoid permission denial on `~/.cache/huggingface`.

5. **`uv venv` interactive prompt** — add `rm -rf "$INSTALL_DIR/.venv"` before `uv venv` to
   avoid the "replace existing venv?" prompt that hangs non-interactive SSH sessions.

6. **dpkg lock on fresh VPS** — provision scripts wait for `unattended-upgrades` to finish
   with `while pgrep -x 'apt-get|dpkg|unattended-upgrade|apt' > /dev/null; do sleep 3; done`

7. **uv HOME permission** — run uv as service user with `env HOME="$INSTALL_DIR"` to prevent
   permission denied on `/root/uv.toml`.

8. **STATE_DB vs STATE_FILE** — backend env var is `STATE_FILE` (JSON), not `STATE_DB` (SQLite).

9. **Multi-dot filenames** — use `.with_suffix(".txt")` not `.with_suffix("").with_suffix(".txt")`
   for `TK_user_2026.05.29_18-00-00.mp4`.

10. **`_do_transfer` concurrent cycles** — `_upload_active` flag prevents double-transfer.

---

## Testing

Quick smoke test (no VPS needed):
```bash
python3 -c "
import os, pathlib
os.environ.update({'CONTROL_PLANE_PASSWORD':'pw',
    'CONTROL_PLANE_DB':'/tmp/test.sqlite',
    'CONTROL_PLANE_SECRET_FILE':'/tmp/test.secret'})
import control_plane as cp
from fastapi.testclient import TestClient
c = TestClient(cp.app, follow_redirects=True)
r = c.post('/api/login', json={'password':'pw'})
assert r.status_code == 200
c.cookies.set('session', r.cookies.get('session',''))
assert c.get('/api/backends').status_code == 200
assert c.get('/api/storage').status_code == 200
assert c.get('/').status_code == 200
print('All OK')
"
```

Full integration test (requires running services):
- Backend on any port with BACKEND_ID, AUTH_TOKEN, RECORDINGS_ROOT set
- Transcription worker on port 8090 with WHISPER_AUTH_TOKEN, WHISPER_WATCH_DIR set
- Control plane with TRANSCRIPT_WORKER_URL pointing to transcription worker

---

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| All tabs frozen, nothing loads | JS syntax error in page | Check with `node -e "new Function(...)"` |
| `no such column: storage_pk` | Schema migration didn't run | Restart control plane |
| `no such table: storage_servers` | Old DB, migration needed | Restart control plane |
| "Upload" button on all files | Files already on storage, no transfer record | Run upload cycle once |
| Storage server returns 500 | `storage_servers` table missing | Restart control plane |
| Deploy hangs at `uv venv` | Existing venv, interactive prompt | Add `rm -rf .venv` before `uv venv` |
| Deploy fails at `apt-get` | dpkg locked by unattended-upgrades | Wait loop before apt (already in script) |
| `/root/uv.toml` permission denied | Wrong HOME for service user | `env HOME="$INSTALL_DIR" uv ...` |
| Creator watcher exits code 0 | Creator not live, or cookies expired | Check cookies; wait for creator to go live |
| Creator watcher exits code non-0 | Michele0303 crash | Check `/data/recordings/<user>/_recorder.log` |
