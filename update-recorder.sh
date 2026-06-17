#!/usr/bin/env bash
#
# Roll Michele0303 forward to a new tag.
#
# Usage:  sudo bash update-recorder.sh <tag>
# Example:  sudo bash update-recorder.sh 8.0.0
#
# Tags use plain numbers — no "v" prefix. Check available tags at:
# https://github.com/Michele0303/tiktok-live-recorder/releases
#
# This is the procedure for the operational expectation called out in the spec:
# when TikTok pushes a WAF/sign change and upstream ships a fix, you do this
# on one backend first as a canary, watch its /health for ~30 minutes, then
# roll the rest.
#
# Safe to re-run; aborts if already on the target tag.

set -euo pipefail

TARGET_TAG="${1:-}"

if [[ -z "$TARGET_TAG" ]]; then
  echo "usage: $0 <git-tag>     e.g.  $0 8.0.0"
  exit 1
fi

INSTALL_DIR=/opt/tt-backend
REPO_DIR=$INSTALL_DIR/tiktok-live-recorder
SERVICE_USER=tt

cd $REPO_DIR

CURRENT=$(sudo -u $SERVICE_USER git rev-parse --short HEAD)
echo "Currently at: $CURRENT"

if sudo -u $SERVICE_USER git describe --exact-match --tags HEAD 2>/dev/null | grep -qx "$TARGET_TAG"; then
  echo "Already on $TARGET_TAG. Nothing to do."
  exit 0
fi

echo "Fetching tags..."
sudo -u $SERVICE_USER git fetch --tags --depth 1 origin

echo "Checking out $TARGET_TAG..."
sudo -u $SERVICE_USER git checkout "$TARGET_TAG"

echo "Re-syncing Python deps (in case requirements changed)..."
sudo -u $SERVICE_USER uv pip install --python $INSTALL_DIR/.venv/bin/python \
  $REPO_DIR/

# Verify cookies.json still exists (we keep the default empty file in place)
if [[ ! -f $REPO_DIR/src/cookies.json ]]; then
  echo "WARNING: cookies.json missing after checkout; restoring default"
  cat > $REPO_DIR/src/cookies.json <<'EOF'
{
  "sessionid_ss": "",
  "tt-target-idc": "useast2a"
}
EOF
  chown $SERVICE_USER:$SERVICE_USER $REPO_DIR/src/cookies.json
fi

echo "Restarting tt-backend..."
systemctl restart tt-backend

sleep 3
echo
echo "Health check:"
curl -s http://127.0.0.1:8000/health | python3 -m json.tool || true

echo
echo "Done. Watch the failure counters at /watchers for the next ~30 min."
echo "If any watcher stays in ERROR state, roll back:"
echo "    sudo bash update-recorder.sh $CURRENT"
