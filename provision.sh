#!/usr/bin/env bash
#
# Provision a fresh recorder backend on Ubuntu 24.04 LTS.
# Called by the control plane Deploy tab — no manual arguments needed.
#
# Usage:  sudo bash provision.sh [BIND_ADDRESS] [PORT]
#   BIND_ADDRESS defaults to 0.0.0.0, PORT defaults to 8000 (both passed
#   automatically by the control plane Deploy tab)
#
# Everything else is auto-detected on the VPS:
#   BACKEND_ID  — hostname + random hex suffix
#   REGION      — country code from ipinfo.io
#
# Re-running is safe: idempotent except for token generation.

set -euo pipefail
trap 'echo "[ERROR] provision.sh failed at line $LINENO: $BASH_COMMAND" >&2' ERR

BIND_ADDRESS="${1:-0.0.0.0}"
PORT="${2:-8000}"

echo "==> Detecting backend identity"
BACKEND_ID="$(hostname -s 2>/dev/null | sed 's/[^a-zA-Z0-9-]/-/g' | cut -c1-20)-$(openssl rand -hex 4)"
REGION="$(curl -s --max-time 5 https://ipinfo.io/country 2>/dev/null | tr -d '"' | tr -d $'\n' || true)"
[[ -z "$REGION" || "${#REGION}" -gt 3 ]] && REGION="auto"
echo "    Backend ID: $BACKEND_ID"
echo "    Region:     $REGION"
echo "    Bind:       $BIND_ADDRESS:$PORT"

# Pin the Michele0303 version. Bump deliberately when upstream ships fixes.
# Tags use plain numbers with no "v" prefix e.g. 8.0.0, not v8.0.0
RECORDER_TAG="8.0.0"

# Where everything lives
INSTALL_DIR=/opt/tt-backend
RECORDINGS_ROOT=/data/recordings
STATE_DIR=/var/lib/tt-recorder
ENV_FILE=/etc/tt-backend.env
SERVICE_FILE=/etc/systemd/system/tt-backend.service
SERVICE_USER=tt

echo "==> Installing system packages"
export DEBIAN_FRONTEND=noninteractive
echo "    Waiting for apt lock to be free..."
while pgrep -x 'apt-get\|dpkg\|unattended-upgrade\|apt' > /dev/null 2>&1; do
    sleep 3
done
echo "    apt is free"
apt-get -o DPkg::Lock::Timeout=120 update
apt-get -o DPkg::Lock::Timeout=120 install -y --no-install-recommends \
  python3 python3-venv python3-pip \
  ffmpeg git curl ca-certificates \
  sqlite3

# uv: much faster than pip for the install, gives us a clean venv tool
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
id -u $SERVICE_USER &>/dev/null || useradd --system --create-home --shell /usr/sbin/nologin $SERVICE_USER

echo "==> Setting up directories"
install -d -o $SERVICE_USER -g $SERVICE_USER -m 0755 $INSTALL_DIR
install -d -o $SERVICE_USER -g $SERVICE_USER -m 0755 $RECORDINGS_ROOT
install -d -o $SERVICE_USER -g $SERVICE_USER -m 0750 $STATE_DIR

echo "==> Cloning Michele0303 at $RECORDER_TAG"
if [[ ! -d "$INSTALL_DIR/tiktok-live-recorder/.git" ]]; then
  sudo -u $SERVICE_USER git clone --depth 1 --branch $RECORDER_TAG \
    https://github.com/Michele0303/tiktok-live-recorder.git \
    $INSTALL_DIR/tiktok-live-recorder
else
  echo "    (already cloned; run update-recorder.sh to roll forward)"
fi

echo "==> Dropping wrapper code"
# In real usage you'd fetch these from your own repo or release artifact.
# For now, expect them in the same dir as this script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
install -o $SERVICE_USER -g $SERVICE_USER -m 0644 "$SCRIPT_DIR/watcher.py" "$INSTALL_DIR/watcher.py"
install -o $SERVICE_USER -g $SERVICE_USER -m 0644 "$SCRIPT_DIR/app.py" "$INSTALL_DIR/app.py"
# chat_recorder.py is optional; install it if present in the deploy bundle
[ -f "$SCRIPT_DIR/chat_recorder.py" ] && install -o $SERVICE_USER -g $SERVICE_USER -m 0644 "$SCRIPT_DIR/chat_recorder.py" "$INSTALL_DIR/chat_recorder.py" || true
[ -f "$SCRIPT_DIR/VERSION" ] && install -o $SERVICE_USER -g $SERVICE_USER -m 0644 "$SCRIPT_DIR/VERSION" "$INSTALL_DIR/VERSION" || true

echo "==> Creating venv and installing Python deps"
cd $INSTALL_DIR
rm -rf "$INSTALL_DIR/.venv"   # remove any stale venv from a previous partial run
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv venv .venv --python python3
# Michele0303's runtime deps come from its pyproject.toml
# `uv pip install <path>` resolves dependencies from the project's pyproject
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv pip install --python .venv/bin/python3 tiktok-live-recorder/
# Our wrapper's deps
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv pip install --python .venv/bin/python3 \
  fastapi 'uvicorn[standard]' httpx pydantic

# Dedicated venv for live-chat capture. TikTokLive pulls in protobuf/pyee/mashumaro
# which can conflict with the recorder's pinned deps, so we isolate it here and run
# chat_recorder.py with this interpreter (see CHAT_PYTHON / app.py).
echo "==> Creating chat venv + installing TikTokLive"
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv venv "$INSTALL_DIR/.venv-chat" --clear --python python3
sudo -u $SERVICE_USER env HOME="$INSTALL_DIR" uv pip install \
  --python "$INSTALL_DIR/.venv-chat/bin/python3" --prerelease=allow -U 'TikTokLive>=6,<7' 'httpx<1' || \
  echo "    [WARN] TikTokLive install failed — chat capture will be disabled until fixed"

echo "==> Generating env file (if not present)"
if [[ ! -f $ENV_FILE ]]; then
  AUTH_TOKEN=$(openssl rand -hex 32)
  CONTROL_PLANE_TOKEN=$(openssl rand -hex 32)
  cat > $ENV_FILE <<EOF
# Backend identity
BACKEND_ID=$BACKEND_ID
REGION=$REGION

# Inbound auth: control plane uses this to call us
AUTH_TOKEN=$AUTH_TOKEN

# Where things live
REPO_PATH=$INSTALL_DIR/tiktok-live-recorder
PYTHON_EXECUTABLE=$INSTALL_DIR/.venv/bin/python3
RECORDINGS_ROOT=$RECORDINGS_ROOT
STATE_FILE=/var/lib/tt-recorder/state.json
# A recording isn't offered for upload/move until it's been untouched for this
# long and isn't held open — protects in-progress recordings (matters most once
# the recorder and storage server are on separate machines).
RECORDER_SETTLE_SEC=120

# Capacity (0 = unlimited)
MAX_WATCHERS=0

# Max random delay (seconds) before a watcher's first spawn after a restart
# or re-enable, so they don't all hit TikTok at once. 0 disables.
WATCHER_STARTUP_JITTER_SEC=20

# Outbound: where we send lifecycle events
# Fill these in after the control plane is up
CONTROL_PLANE_URL=
CONTROL_PLANE_TOKEN=$CONTROL_PLANE_TOKEN
EOF
  chmod 0600 $ENV_FILE
  chown root:$SERVICE_USER $ENV_FILE
  echo
  echo "    +--- BACKEND TOKENS (save these in your control plane) -------"
  echo "    | BACKEND_ID:          $BACKEND_ID"
  echo "    | AUTH_TOKEN:          $AUTH_TOKEN"
  echo "    | CONTROL_PLANE_TOKEN: $CONTROL_PLANE_TOKEN"
  echo "    +-------------------------------------------------------------"
  echo
else
  echo "    ($ENV_FILE already exists; left untouched)"
fi

echo "==> Installing systemd unit"
cat > $SERVICE_FILE <<EOF
[Unit]
Description=TikTok Live Recorder Backend (FastAPI)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=tt
Group=tt
WorkingDirectory=/opt/tt-backend
EnvironmentFile=/etc/tt-backend.env
ExecStart=/opt/tt-backend/.venv/bin/uvicorn app:app \\
          --host $BIND_ADDRESS --port $PORT \\
          --timeout-graceful-shutdown 60
Restart=on-failure
RestartSec=5
# uvicorn needs time to gracefully stop watchers (SIGINT to each subprocess
# group, 30s grace, then escalation). Don't kill it too early.
TimeoutStopSec=120

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/data/recordings /var/lib/tt-recorder /opt/tt-backend
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
systemctl enable tt-backend
systemctl restart tt-backend

echo
echo "==> Checking firewall"
if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
  if ufw status | grep -qE "^$PORT(/tcp)?[[:space:]].*ALLOW"; then
    echo "    UFW: port $PORT is open. Good."
  else
    echo "    [WARN] UFW is active but port $PORT is not open."
    echo "    Run: sudo ufw allow $PORT/tcp"
    echo "    Then: sudo ufw reload"
  fi
else
  echo "    UFW not active (check your cloud provider's security group instead)"
fi

echo
echo "==> Self-test: waiting for service to come up"
sleep 4
if curl -s --max-time 5 http://localhost:$PORT/health 2>/dev/null | grep -q "backend_id"; then
  echo "    Service is up and responding to /health"
else
  echo "    [WARN] Service not responding yet."
  echo "    Check: sudo systemctl status tt-backend"
  echo "           sudo journalctl -u tt-backend --no-pager -n 30"
fi

echo
echo "==> Done. Service status:"
systemctl status tt-backend --no-pager | head -6
echo
echo "Probe:  curl -s http://127.0.0.1:$PORT/health | jq"
echo
echo "Next steps:"
echo "  1. (if internet-facing) ensure your firewall/security-group allows TCP $PORT"
echo "  2. From your control plane, register this backend:"
echo "       url:   http://<this-server-ip-or-hostname>:$PORT"
echo "                (or http://localhost:$PORT if colocated)"
echo "       token: the AUTH_TOKEN above"
echo "  3. Edit $ENV_FILE to fill in CONTROL_PLANE_URL, then  systemctl restart tt-backend"
