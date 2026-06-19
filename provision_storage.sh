#!/usr/bin/env bash
#
# Provision a storage server with the Whisper transcription worker.
#
# Usage (called by the control plane Deploy tab — no manual arguments needed):
#   sudo bash provision_storage.sh [BIND_ADDRESS]
#   BIND_ADDRESS defaults to 0.0.0.0 (passed automatically by the control plane)
#
# Whisper model defaults to "base" (~150MB, ~4 min/hr stream).
# To change it after deploy: edit /etc/tt-storage.env and restart tt-transcription.
#
# Re-running is safe: skips steps already done, keeps existing token.

set -euo pipefail
trap 'echo "[ERROR] provision_storage.sh failed at line $LINENO: $BASH_COMMAND" >&2' ERR

BIND_ADDRESS="${1:-0.0.0.0}"
WHISPER_MODEL="base"

INSTALL_DIR=/opt/tt-storage
RECORDINGS_DIR=/data/recordings
ENV_FILE=/etc/tt-storage.env
SERVICE_FILE=/etc/systemd/system/tt-transcription.service
# Use the same 'tt' service user as recorder backends so both can access
# /data/recordings without permission conflicts on colocated installs.
SERVICE_USER=tt

echo "==> Installing system packages"
# Fresh VPS instances run unattended-upgrades automatically on boot and hold
# the dpkg lock for several minutes. Wait for it to finish before proceeding.
export DEBIAN_FRONTEND=noninteractive
echo "    Waiting for apt lock to be free..."
while pgrep -x 'apt-get\|dpkg\|unattended-upgrade\|apt' > /dev/null 2>&1; do
    sleep 3
done
echo "    apt is free"
apt-get -o DPkg::Lock::Timeout=120 update
apt-get -o DPkg::Lock::Timeout=120 install -y --no-install-recommends \
  python3 python3-venv python3-pip ffmpeg curl ca-certificates

echo "==> Installing uv"
if ! command -v uv >/dev/null 2>&1; then
  echo "    Trying curl installer..."
  if curl -LsSf --max-time 60 https://astral.sh/uv/install.sh | sh 2>&1; then
    UV_BIN=$(find /root /home -name "uv" -type f 2>/dev/null | head -1)
    [[ -n "$UV_BIN" ]] && install -m 0755 "$UV_BIN" /usr/local/bin/uv
  fi
  # Fallback: pip install (always available since we just installed python3-pip)
  if ! command -v uv >/dev/null 2>&1; then
    echo "    curl installer failed or uv not found — falling back to pip install..."
    pip3 install --break-system-packages uv
  fi
  if ! command -v uv >/dev/null 2>&1; then
    echo "[ERROR] Could not install uv by any method. Aborting."
    exit 1
  fi
fi
echo "    uv version: $(uv --version)"

echo "==> Creating service user"
id -u $SERVICE_USER &>/dev/null || \
  useradd --system --create-home --shell /usr/sbin/nologin $SERVICE_USER

echo "==> Detecting environment"
# If a recorder backend is already installed on this machine, use its
# recordings directory as the watch dir so transcription is automatic.
if [[ -f /etc/tt-backend.env ]]; then
    EXISTING_ROOT=$(grep ^RECORDINGS_ROOT /etc/tt-backend.env | cut -d= -f2 || true)
    if [[ -n "$EXISTING_ROOT" && -d "$EXISTING_ROOT" ]]; then
        RECORDINGS_DIR="$EXISTING_ROOT"
        echo "    Detected colocated recorder backend — using its recordings dir: $RECORDINGS_DIR"
    fi
fi
install -d -o $SERVICE_USER -g $SERVICE_USER -m 0755 $INSTALL_DIR
install -d -o $SERVICE_USER -g $SERVICE_USER -m 0755 $RECORDINGS_DIR

echo "==> Dropping transcription_worker.py"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
install -o $SERVICE_USER -g $SERVICE_USER -m 0644 \
  "$SCRIPT_DIR/transcription_worker.py" "$INSTALL_DIR/transcription_worker.py"
[ -f "$SCRIPT_DIR/VERSION" ] && install -o $SERVICE_USER -g $SERVICE_USER -m 0644 "$SCRIPT_DIR/VERSION" "$INSTALL_DIR/VERSION" || true

echo "==> Creating venv and installing Python deps"
cd "$INSTALL_DIR"
rm -rf "$INSTALL_DIR/.venv"   # remove any stale venv from a previous partial run
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv venv "$INSTALL_DIR/.venv" --python python3
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv pip install --python "$INSTALL_DIR/.venv/bin/python3" \
  faster-whisper fastapi 'uvicorn[standard]' rclone-python

# rclone (the binary) powers the optional 3rd-hop archive to any cloud.
echo "==> Installing rclone (for cloud archive)"
if ! command -v rclone >/dev/null 2>&1; then
  curl -fsSL https://rclone.org/install.sh | sudo bash || \
    echo "    [WARN] rclone install failed — archive will be disabled until rclone is installed"
fi

echo "==> Generating env file"
if [[ ! -f $ENV_FILE ]]; then
  WHISPER_AUTH_TOKEN=$(openssl rand -hex 32)
  # Per-node web UI password. Generated random (NOT the 'yourpassword' default,
  # which the worker would otherwise accept as a valid API credential).
  STORAGE_UI_PASSWORD=$(openssl rand -hex 12)
  cat > $ENV_FILE <<EOF
WHISPER_WATCH_DIR=$RECORDINGS_DIR
WHISPER_MODEL=$WHISPER_MODEL
WHISPER_HOST=$BIND_ADDRESS
WHISPER_PORT=8090
WHISPER_AUTH_TOKEN=$WHISPER_AUTH_TOKEN
STORAGE_UI_PASSWORD=$STORAGE_UI_PASSWORD
WHISPER_SCAN_INTERVAL=30
# Wait this long after a file stops changing before transcribing it, so a
# colocated worker never transcribes a recording that's still being written.
WHISPER_SETTLE_SEC=120
HF_HOME=$INSTALL_DIR/models
# Transcribe up to N files at once (each loads its own model copy → more RAM).
WHISPER_CONCURRENCY=2
# Skip silent stretches before transcribing (big speed win on lives with dead
# air). 1=on, 0=off. Tune the silence threshold with WHISPER_VAD_MIN_SILENCE_MS.
WHISPER_VAD=1
WHISPER_VAD_MIN_SILENCE_MS=500
# Decoding beam size: 5 = accurate default, 1 = greedy/fastest on CPU.
WHISPER_BEAM_SIZE=5
# Set to 0 to make this a storage-only server (receives/serves/moves files but
# does not transcribe). Useful when another server handles transcription.
WHISPER_TRANSCRIBE=1
# Flag a file as "stalled" in the dashboard after this many seconds with no progress.
WHISPER_STALL_SEC=180
# Hard-kill a transcription after this many seconds (0 = auto: 10x audio length + 5m).
WHISPER_FILE_TIMEOUT=0

# ── 3rd-hop archive (optional) ──────────────────────────────────────
# Push recordings to any cloud AFTER transcription, via rclone.
# 1) Configure a remote once:  sudo -u $SERVICE_USER HOME=$INSTALL_DIR rclone config
#    (creates e.g. a "dropbox:" or end-to-end-encrypted "filen:" remote — Filen
#     is a native backend in rclone >= 1.73, so run 'rclone selfupdate' if older)
# 2) Set ARCHIVE_REMOTE below to "<remote>:<path>" and restart the service.
#    (Easier: use the control plane's Cloud Archive form — it supports S3/B2/
#     Dropbox/Filen and writes all of this for you over SSH.)
# ARCHIVE_REMOTE=dropbox:tt-recordings
# ARCHIVE_WHAT=mp4                 # mp4 | txt | both
# ARCHIVE_DELETE_LOCAL=0           # 1 = enable disk-aware eviction of archived copies
# Disk-aware eviction (when ARCHIVE_DELETE_LOCAL=1): keep recordings hot and only
# delete verified-on-cloud copies once the disk is under pressure — oldest first.
# ARCHIVE_EVICT_HIGH_PCT=85        # start evicting above this disk usage %
# ARCHIVE_EVICT_LOW_PCT=70         # evict down to this %
# ARCHIVE_EVICT_MIN_AGE_SEC=86400  # never evict a file younger than this (kept hot)
# rclone throughput + retry tuning:
# ARCHIVE_TRANSFERS=4              # parallel transfers rclone uses
# ARCHIVE_BWLIMIT=                 # cap, e.g. "10M"; empty = unlimited
# ARCHIVE_RETRY_BACKOFF=120        # base seconds between archive retries (doubles)
EOF
  chmod 0600 $ENV_FILE
  chown root:$SERVICE_USER $ENV_FILE
  echo
  echo "    +--- STORAGE SERVER TOKEN --------------------------------"
  echo "    | WHISPER_AUTH_TOKEN: $WHISPER_AUTH_TOKEN"
  echo "    +---------------------------------------------------------"
  echo
else
  echo "    ($ENV_FILE already exists — token kept)"
  WHISPER_AUTH_TOKEN=$(grep ^WHISPER_AUTH_TOKEN $ENV_FILE | cut -d= -f2)
  echo
  echo "    +--- EXISTING TOKEN (re-printed for registration) --------"
  echo "    | WHISPER_AUTH_TOKEN: $WHISPER_AUTH_TOKEN"
  echo "    +---------------------------------------------------------"
  echo
fi

echo "==> Installing systemd unit"
cat > $SERVICE_FILE <<EOF
[Unit]
Description=TT Storage Server — Whisper Transcription Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$INSTALL_DIR/.venv/bin/python3 $INSTALL_DIR/transcription_worker.py
Restart=on-failure
RestartSec=5
# Whisper is the biggest memory user on the box (each concurrent job loads its
# own model copy). If the machine runs low on RAM, prefer killing/restarting a
# transcription over the control plane or recorder — so the site stays up.
OOMScoreAdjust=600
# Worker caps its own graceful shutdown at 10s; force-kill shortly after so a
# restart can't hang on an in-flight transfer/transcription.
TimeoutStopSec=15
KillMode=mixed
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=$RECORDINGS_DIR $INSTALL_DIR
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictRealtime=true
LockPersonality=true
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable tt-transcription
systemctl restart tt-transcription

echo "==> Checking firewall"
if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
  if ufw status | grep -qE "^8090.*ALLOW"; then
    echo "    UFW: port 8090 is open"
  else
    echo "    [WARN] UFW active but port 8090 not open"
    echo "    Run: sudo ufw allow 8090/tcp && sudo ufw reload"
  fi
else
  echo "    UFW not active (check cloud security group if needed)"
fi

echo "==> Self-test: waiting for service to come up"
sleep 4
if curl -s --max-time 5 http://localhost:8090/health 2>/dev/null | grep -q '"ok":true'; then
  echo "    Service is responding to /health"
else
  echo "    [WARN] Service not responding yet"
  echo "    Check: sudo systemctl status tt-transcription"
  echo "           sudo journalctl -u tt-transcription --no-pager -n 30"
fi

echo
echo "==> Done. Service status:"
systemctl status tt-transcription --no-pager | head -6
echo
MODEL_SIZE=$(case $WHISPER_MODEL in
  tiny) echo "75MB";; base) echo "150MB";; small) echo "480MB";;
  medium) echo "1.5GB";; large*) echo "3GB";; *) echo "varies";;
esac)
echo "Note: Whisper model '$WHISPER_MODEL' (~$MODEL_SIZE) downloads on first"
echo "      transcription. This is automatic — no action needed."
