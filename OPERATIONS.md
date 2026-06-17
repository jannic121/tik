# TikTok Live Recorder — Operations Manual

---

## What you have

| File | Runs on | Purpose |
|---|---|---|
| `control_plane.py` | Your machine | Web UI on port 8080. Backends, Watchers, Files, Deploy tabs. Upload worker runs here. |
| `app.py` | Each recorder VPS | FastAPI HTTP layer — auth, watcher CRUD, file inventory, download. Port 8000. |
| `watcher.py` | Each recorder VPS | One Michele0303 subprocess per creator. State machine (idle→recording→backoff→error). |
| `transcription_worker.py` | Storage server | Whisper transcription + file receive endpoint. Port 8090. |
| `provision.sh` | Uploaded to recorder VPS | Installs all deps, generates tokens, creates systemd unit `tt-backend`. |
| `provision_storage.sh` | Uploaded to storage VPS | Installs Whisper deps, creates systemd unit `tt-transcription`. |
| `update-recorder.sh` | Run on recorder VPS | Rolls Michele0303 to a new git tag. |

---

## The complete pipeline

```
Creator goes live
  └─► Michele0303 records  ──►  /data/recordings/<user>/TK_<user>_<ts>_flv.mp4  (in progress)
                                └─► remux on stop  ──►  TK_<user>_<ts>.mp4        (final)
                                                         │
                             Control plane upload worker (every 5 min)
                                                         │
                                                    ↓ stream
                                          /data/recordings/<user>/TK_<user>_<ts>.mp4  (storage server)
                                                         │
                                     ┌───────────────────┘
                                     │ auto-scan (every 30s)
                                     ▼
                             Whisper transcription
                                     │
                         TK_<user>_<ts>.txt / .srt / .json
                                     │
                     Files tab: "✓ On storage" + "✓ Available"
```

---

## Quick start

### 1. Install on your local machine

```bash
pip install --break-system-packages fastapi 'uvicorn[standard]' httpx paramiko
```

Place these files in one directory:
```
control_plane.py
provision.sh            ← Deploy tab uploads these two
watcher.py              ← ↑
app.py                  ← ↑
provision_storage.sh    ← Deploy tab uploads these two
transcription_worker.py ← ↑
update-recorder.sh      ← used manually on backends
```

### 2. Run the control plane

```bash
export CONTROL_PLANE_PASSWORD=yourpassword
python3 control_plane.py
# Open http://localhost:8080  (or http://<your-ip>:8080)
```

#### Updating the control plane (Deploy tab → Update control plane)

Upload the new `tt-recorder.zip` to the host (default `/root/tt-recorder.zip`), then click **Update**. It verifies the archive, backs up the current files under `.update-backups/`, unzips over the control plane's own directory (which is also the source for pushing updates to recorders/storage), and restarts. The dashboard reconnects on its own.

How it restarts, first that applies:
- `CONTROL_PLANE_RESTART_CMD` — a shell command you provide (run detached).
- `CONTROL_PLANE_SERVICE` — a systemd unit name; restarted via `systemd-run` so the restarter survives the stop.
- otherwise it re-execs `python3 control_plane.py` in place (same PID) — works for a plain run and under systemd.

Override the zip location with `CONTROL_PLANE_UPDATE_ZIP` if you don't use `/root`.

### 3. Deploy a recorder backend (Deploy tab → Recorder backend)

Fill in SSH credentials, give it a Backend ID and Region, click **Deploy**.
Output streams live. When it finishes, click **"Add to backend registry →"** and then **Add** in the modal.

### 4. Deploy a storage server (Deploy tab → Storage server)

Fill in SSH credentials, pick a Whisper model, click **Deploy**.
Output streams live. When it finishes, the control plane auto-registers the transcript worker — no copy-pasting tokens.

For a colocated setup (storage server on the same machine as the control plane), use `127.0.0.1` as the host; the bind address will be set to localhost automatically.

### 5. Add creators to watch (Watchers tab)

Username, interval (default 3 min), backend (Auto = least-loaded). The watcher polls until the creator goes live, then records. State updates every 5 seconds in the UI.

### 6. What happens automatically from here

- Creator goes live → recording starts
- Creator goes offline → FLV flushed, remuxed to MP4
- Upload worker (every 5 min) → detects new MP4 → streams to storage server → deletes from backend
- Transcription worker (every 30s scan) → detects new MP4 → transcribes with Whisper → saves .txt/.srt/.json
- Files tab shows **✓ On storage** and **✓ Available** (transcript) for completed recordings

---

## Part 1: Control plane

### 1.1 Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `CONTROL_PLANE_PASSWORD` | **yes** | — | Login password. |
| `CONTROL_PLANE_DB` | no | `~/.tt-recorder/control.sqlite` | SQLite registry. |
| `CONTROL_PLANE_SECRET_FILE` | no | `~/.tt-recorder/secret` | HMAC key. Auto-generated, persisted. |
| `CONTROL_PLANE_HOST` | no | `0.0.0.0` | Bind host. |
| `CONTROL_PLANE_PORT` | no | `8080` | Bind port. |
| `DEPLOY_FILES_DIR` | no | Dir of script | Where deploy files live. |
| `TRANSCRIPT_WORKER_URL` | no | — | Auto-set after storage server deploy. |
| `TRANSCRIPT_WORKER_TOKEN` | no | — | Auto-set and persisted to SQLite. |
| `UPLOAD_WORKER_ENABLED` | no | `1` | Set to `0` to disable automatic uploads. |
| `UPLOAD_INTERVAL_SEC` | no | `300` | How often to scan for new files. |
| `UPLOAD_MIN_AGE_SEC` | no | `300` | Minimum file age — avoids grabbing mid-finalise files. |
| `UPLOAD_DELETE_AFTER` | no | `1` | Delete from backend after confirmed upload. |
| `UPLOAD_MAX_RETRIES` | no | `3` | Retries before marking a transfer failed permanently. |

### 1.2 SQLite tables

| Table | Contents |
|---|---|
| `backends` | Registered backends: URL, auth token, region, health |
| `watchers` | Watchlist: username, assigned backend, interval |
| `transfers` | Upload worker state: filename, status, attempts, errors |
| `events` | Lifecycle events from backends (future use) |
| `config` | Persisted settings (transcript worker URL/token) |

### 1.3 The four tabs

**Backends.** Register by URL + AUTH_TOKEN. Probes before saving. Shows health, load, disk free, git rev.

**Watchers.** Add by username. Auto-assigns to least-loaded healthy backend. States: `starting / idle / recording / backoff / error / offline`.

**Files.** Lists recordings from all healthy backends with two status columns:

| Storage column | Meaning |
|---|---|
| `↑ Upload` button | Not yet queued — click to queue immediately |
| `⏳ Queued` | Waiting for next upload cycle |
| `↑ Uploading…` | Transfer in progress |
| `✓ On storage` | Transferred and verified |
| `✕ Failed` + Retry | Failed after max retries |

| Transcript column | Meaning |
|---|---|
| `No transcript` | Not yet seen by transcription worker |
| `⏳ Pending` | In transcription queue |
| `⏳ Transcribing…` | Being processed now |
| `✓ Available` + View + ↓ .txt | Done — click View to read in-page |

**↑ Upload now** button triggers an immediate upload cycle.

**Deploy.** Toggle between Recorder backend and Storage server. SSH credentials + type-specific settings. Streams output live. Auto-registers the storage server on completion.

### 1.4 Control plane API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/login` | Set session cookie |
| GET | `/api/me` | Login state |
| GET | `/api/backends` | List with health |
| POST | `/api/backends` | Register (probes first) |
| DELETE | `/api/backends/{pk}` | Remove + stop watchers |
| GET | `/api/watchers` | List with live state |
| POST | `/api/watchers` | Add |
| DELETE | `/api/watchers/{u}` | Stop + remove |
| GET | `/api/files` | Aggregated from all backends |
| GET | `/api/files/download` | Proxy file to browser |
| POST | `/api/files/delete` | Delete from backend |
| GET | `/api/transcript-statuses` | `{filename: status}` from worker |
| GET | `/api/transcript-view` | View transcript text |
| GET | `/api/transcript-download` | Download .txt with timestamps |
| GET | `/api/transfer-statuses` | `{filename: status}` from transfers table |
| POST | `/api/transfer/queue` | Manually queue a file |
| POST | `/api/transfer/run-now` | Trigger immediate upload cycle |
| POST | `/api/config/transcript-worker` | Persist worker URL+token |
| POST | `/api/deploy` | SSH deploy (streaming) |
| POST | `/events` | Webhook receiver |

---

## Part 2: Recorder backends

### 2.1 Env vars (`/etc/tt-backend.env`)

| Variable | Purpose |
|---|---|
| `BACKEND_ID` | Unique identity per VPS |
| `AUTH_TOKEN` | 64 hex char bearer token |
| `REGION` | Free-form region tag |
| `REPO_PATH` | Path to Michele0303 clone |
| `PYTHON_EXECUTABLE` | Venv python path |
| `RECORDINGS_ROOT` | `/data/recordings` |
| `STATE_FILE` | `/var/lib/tt-recorder/state.json` |
| `MAX_WATCHERS` | Capacity cap (default 30) |
| `CONTROL_PLANE_URL` | If set, events sent here |
| `CONTROL_PLANE_TOKEN` | Bearer for outbound events |

### 2.2 Backend API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/health` | none | Identity, disk, ffmpeg, git rev |
| GET | `/watchers` | bearer | List with live state |
| POST | `/watchers` | bearer | Start watching |
| DELETE | `/watchers/{u}` | bearer | Clean stop + remove |
| GET | `/files` | bearer | Per-creator inventory |
| GET | `/files/download?path=` | bearer | Stream file |
| DELETE | `/files?path=` | bearer | Remove file |

### 2.3 Watcher lifecycle

```
starting → idle ──► recording ──► idle
               ↓ (crash)
             backoff (1s → 2s → 4s … 60s cap)
               ↓ (5 crashes)
             error  (won't restart; Remove + re-add in UI)
```

SIGINT on stop → FLV flush + ffmpeg remux → SIGTERM if not done in 30s.

### 2.4 Session cookie (for restricted streams)

Some creators require a logged-in TikTok account. Symptom: "Unable to retrieve live streaming url" in the recorder log.

```bash
# Get sessionid_ss from Chrome DevTools → Application → Cookies → tiktok.com
sudo nano /opt/tt-backend/tiktok-live-recorder/src/cookies.json
# {"sessionid_ss": "your_value", "tt-target-idc": "useast2a"}
sudo systemctl restart tt-backend
```

Use a throwaway account. Refresh when recordings start failing with the same message again.

---

## Part 3: Storage server

### 3.1 Env vars (`/etc/tt-storage.env`)

| Variable | Purpose |
|---|---|
| `WHISPER_WATCH_DIR` | `/data/recordings` — same dir backends write to if colocated |
| `WHISPER_MODEL` | Model size: `tiny`/`base`/`small`/`medium`/`large-v3` |
| `WHISPER_HOST` | Bind host |
| `WHISPER_PORT` | `8090` |
| `WHISPER_AUTH_TOKEN` | 64 hex char bearer token |
| `WHISPER_SCAN_INTERVAL` | Seconds between scans (default 30) |
| `HF_HOME` | `/opt/tt-storage/models` — model cache (avoids ProtectHome conflict) |

### 3.2 Whisper model guide

| Model | Size | Speed on CPU | Use when |
|---|---|---|---|
| `tiny` | 75MB | ~2 min/hr stream | Speed matters, accuracy less so |
| `base` | 150MB | ~4 min/hr stream | Good default |
| `small` | 480MB | ~10 min/hr stream | Accented speech, background music |
| `medium` | 1.5GB | ~30 min/hr stream | High accuracy, powerful CPU |
| `large-v3` | 3GB | ~60 min/hr stream | Best accuracy, GPU recommended |

Model downloads on first transcription (~150MB for base) and is cached permanently.

### 3.3 Transcription worker API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/health` | none | Status, queue depth, model info |
| GET | `/files/inventory` | bearer | All MP4s on storage server |
| PUT | `/files/{user}/{file}` | bearer | Receive a file (upload worker uses this) |
| GET | `/transcripts/all-statuses` | bearer | `{filename: status}` |
| GET | `/transcripts/view?filename=` | bearer | Timestamped .txt transcript |
| GET | `/transcripts/download-srt?filename=` | bearer | .srt download |
| GET | `/transcripts/search?q=` | bearer | Full-text search with snippets |
| POST | `/scan` | bearer | Trigger immediate scan |

### 3.4 Transcript format

The `.txt` file:
```
[00:00:00] Good evening everyone welcome to the stream
[00:00:04] Tonight we are talking about fitness and nutrition
```

The `.srt` file is standard subtitle format — loadable in VLC alongside the video. The `.json` file has word-level timestamps and confidence scores.

---

## Part 4: Operating

### 4.1 Service management

```bash
# Recorder backend
sudo systemctl status tt-backend
sudo systemctl restart tt-backend
sudo journalctl -u tt-backend -f
tail -f /data/recordings/<creator>/_recorder.log

# Storage server
sudo systemctl status tt-transcription
sudo systemctl restart tt-transcription
sudo journalctl -u tt-transcription -f
```

### 4.2 Updating watcher.py / app.py

```bash
scp watcher.py app.py root@<backend-ip>:/opt/tt-backend/
ssh root@<backend-ip> "sudo systemctl restart tt-backend"
```

In-progress recordings survive if the SIGINT grace period (30s per watcher) is enough. Check the Watchers tab — if all are `idle`, restart freely.

### 4.3 Updating Michele0303

```bash
sudo bash update-recorder.sh 8.0.0   # no v prefix
```

Tags use plain numbers. Check https://github.com/Michele0303/tiktok-live-recorder/releases.

**Rollout procedure:** update one backend (canary), watch for 30 min, roll the rest if stable. The Backends tab shows each backend's `recorder_git_rev`.

### 4.4 TikTok WAF breakage

| Symptom | Likely cause |
|---|---|
| One watcher fails, others fine | That creator or that IP |
| All watchers on one backend fail | That VPS IP rate-limited |
| All backends fail within ~1 hour | TikTok WAF change — wait for Michele0303 release |

### 4.5 Disk management

The upload worker auto-transfers files to the storage server every 5 minutes and deletes them from backends. If a backend disk fills before the next cycle, use the Files tab's **↑ Upload now** button, or on the backend:

```bash
df -h /data/recordings
du -sh /data/recordings/*/
```

---

## Part 5: Troubleshooting

### Control plane

**Won't start** — `CONTROL_PLANE_PASSWORD` not set, or port 8080 in use.

**"Deploy tab will fail — missing files"** — `provision.sh`, `watcher.py`, `app.py` (or `provision_storage.sh`, `transcription_worker.py`) need to be alongside `control_plane.py`.

**"Backend probe failed"** — test: `curl http://<host>:8000/health`. Check TCP 8000 is open in cloud security group.

**"auth_token rejected"** — get correct token: `sudo grep ^AUTH_TOKEN /etc/tt-backend.env`. Tokens are 64 hex chars.

**"no healthy backend with capacity"** — all backends offline or at `MAX_WATCHERS`. Check Backends tab.

### Deploy tab

**"Connection failed"** — SSH not running or firewall. Test: `ssh root@<host>`.

**"provision.sh exited with code 128"** — git tag not found. Tags use no `v` prefix (`8.0.0` not `v8.0.0`).

**"Read-only file system"** — `ProtectHome=true` or `ProtectSystem=strict` blocking a write. Fix:
```bash
sudo sed -i 's|ReadWritePaths=.*|ReadWritePaths=/data/recordings /var/lib/tt-recorder /opt/tt-backend|' \
  /etc/systemd/system/tt-backend.service
sudo systemctl daemon-reload && sudo systemctl restart tt-backend
```

### Watchers

**"Unable to retrieve live streaming url"** — creator's stream needs a session cookie. See §2.4.

**`error` state (5 crashes)** — Remove + re-add. If immediately errors again, persistent issue (WAF block, bad IP, needs cookie).

### Upload worker

**Transfer stuck in `transferring`** — control plane crashed mid-transfer. Restart the control plane; it resets `transferring` → `pending` on startup (next cycle retries).

**Transfer `failed` after retries** — check backend log and storage server log. Common causes: backend went offline, storage server disk full, network timeout.

**Files not being picked up** — `UPLOAD_MIN_AGE_SEC` default is 300 (5 min). Files created in the last 5 minutes are skipped intentionally. Use **↑ Upload now** to force an immediate cycle after a recording ends.

### Transcription

**Model download fails** — `HF_HOME` must point to a path in `ReadWritePaths`. Default: `/opt/tt-storage/models` (inside `/opt/tt-storage` which is in `ReadWritePaths`). If changed, update both.

**"transcript not yet available"** — file hasn't been scanned yet. POST `/scan` to the worker, or wait for the next 30-second interval.

---

## Cheat sheet

```bash
# Run control plane
export CONTROL_PLANE_PASSWORD=yourpassword
python3 control_plane.py

# Update backend files
scp watcher.py app.py root@<ip>:/opt/tt-backend/
ssh root@<ip> "sudo systemctl restart tt-backend"

# Roll Michele0303 (no v prefix)
sudo bash update-recorder.sh 8.0.0

# Service management
sudo systemctl restart tt-backend
sudo systemctl restart tt-transcription
sudo journalctl -u tt-backend -f
tail -f /data/recordings/<creator>/_recorder.log

# Debug curl helpers (run on backend)
TOKEN=$(sudo grep ^AUTH_TOKEN /etc/tt-backend.env | cut -d= -f2)
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8000/health | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8000/watchers | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8000/files | python3 -m json.tool

# Debug curl helpers (run on storage server)
TOKEN=$(sudo grep ^WHISPER_AUTH_TOKEN /etc/tt-storage.env | cut -d= -f2)
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8090/health | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8090/files/inventory | python3 -m json.tool

# Rotate leaked AUTH_TOKEN
NEW=$(openssl rand -hex 32)
sudo sed -i "s/^AUTH_TOKEN=.*/AUTH_TOKEN=$NEW/" /etc/tt-backend.env
sudo systemctl restart tt-backend
# Then remove and re-add the backend in the UI
```

## Running the control plane as a service (auto-restart / OOM protection)

A "Killed" message that takes the site down is almost always the Linux OOM
killer. Run the control plane under systemd so it auto-restarts and is the last
thing the kernel kills.

From the web UI (easiest): **Deploy tab → Run control plane as a service**. If
the control plane runs as root it writes `/etc/tt-control-plane.env` (capturing
your current `CONTROL_PLANE_*` config) and `/etc/systemd/system/tt-control-plane.service`
(`Restart=always`, `OOMScoreAdjust=-800`), enables it, and switches over. If it
isn't root, the button prints the exact commands to run.

By hand:

    sudo tee /etc/systemd/system/tt-control-plane.service >/dev/null <<'UNIT'
    [Unit]
    Description=TikTok recorder control plane
    After=network-online.target
    Wants=network-online.target
    [Service]
    Type=simple
    WorkingDirectory=/root/tt-recorder
    ExecStart=/usr/bin/python3 /root/tt-recorder/control_plane.py
    Environment=CONTROL_PLANE_PASSWORD=YOUR_PASSWORD
    Environment=CONTROL_PLANE_SERVICE=tt-control-plane
    Restart=always
    RestartSec=3
    OOMScoreAdjust=-800
    TimeoutStopSec=15
    [Install]
    WantedBy=multi-user.target
    UNIT
    sudo systemctl daemon-reload
    sudo systemctl enable --now tt-control-plane
    journalctl -u tt-control-plane -f

Pair it with the storage side: **Transcription tab → 🛡 Protect from OOM** on
each transcription box (or `OOMScoreAdjust=600` in `tt-transcription.service`),
so under memory pressure a transcription is killed/restarted instead of the site.
