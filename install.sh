#!/usr/bin/env bash
# Fraud Ledger installer for one Ubuntu Desktop machine (24.04 or 26.04).
#   bash install.sh              system install, systemd, no demonstration data
#   bash install.sh --user       virtualenv in this clone, no systemd
#   bash install.sh --demo       also load the fictional fixture
#   bash install.sh --no-start   install but do not start the UI
set -euo pipefail

SOURCE="${BASH_SOURCE[0]}"
while [[ -L "$SOURCE" ]]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE"
done
ROOT="$(cd -P "$(dirname "$SOURCE")" && pwd)"
cd "$ROOT"

want_user=0
for arg in "$@"; do
  if [[ "$arg" == "--user" ]]; then
    want_user=1
  fi
done

if [[ "$want_user" -eq 0 && "$(id -u)" -ne 0 ]]; then
  if [[ "${FRAUDLEDGER_INSTALL_REEXEC:-}" == "1" ]]; then
    echo "sudo did not produce a root shell. Re-run as root, or pass --user." >&2
    exit 1
  fi
  if ! command -v sudo >/dev/null 2>&1; then
    echo "sudo is not available. Re-run as root, or pass --user." >&2
    exit 1
  fi
  exec sudo -E env FRAUDLEDGER_INSTALL_REEXEC=1 bash "$ROOT/install.sh" "$@"
fi

NO_DEMO=1
NO_START=0
USER_MODE=$want_user
while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) USER_MODE=1; shift ;;
    --demo) NO_DEMO=0; shift ;;
    --no-demo) NO_DEMO=1; shift ;;
    --no-start) NO_START=1; shift ;;
    -h|--help)
      cat <<'EOF'
Usage: bash install.sh [--user] [--demo] [--no-start]

Without flags, on Ubuntu Desktop 24.04 or 26.04, this installs a system
virtualenv and a local-only systemd service on 127.0.0.1:8088. It does not
load fictional data. Open that address and use Sources to load the public
working set.

  --user      Install into .venv in this clone and start with nohup. No systemd.
  --demo      Load fictional NPIs in the 199999xxxx range. Not for reporting.
  --no-demo   Accepted for older notes. This is already the default.
  --no-start  Do not start the review UI.
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

wait_health() {
  local host="$1"
  local port="$2"
  local url="http://${host}:${port}/health"
  local i
  for i in $(seq 1 30); do
    if command -v curl >/dev/null 2>&1; then
      if curl -sf -o /dev/null --max-time 2 "$url"; then
        echo "Review UI is up at http://${host}:${port}/"
        return 0
      fi
    else
      if python3 - "$url" <<'PY'
import sys, urllib.request
try:
    urllib.request.urlopen(sys.argv[1], timeout=2).read()
except Exception:
    raise SystemExit(1)
PY
      then
        echo "Review UI is up at http://${host}:${port}/"
        return 0
      fi
    fi
    sleep 1
  done
  echo "The UI did not answer $url within 30 seconds." >&2
  return 1
}

read_bind() {
  local config="$1"
  python3 - "$config" <<'PY'
import sys
from pathlib import Path
text = Path(sys.argv[1]).read_text(encoding="utf-8")
host, port = "127.0.0.1", "8088"
for line in text.splitlines():
    if line.startswith("bind_host:"):
        host = line.split(":", 1)[1].strip().strip("'\"")
    elif line.startswith("bind_port:"):
        port = line.split(":", 1)[1].strip()
print(host)
print(port)
PY
}

install_user() {
  if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
    echo "Python 3.10 or newer is required. Ubuntu 26.04 Desktop includes Python 3.14." >&2
    exit 1
  fi
  if ! python3 -m venv "$ROOT/.venv"; then
    echo "python3 -m venv failed. On Ubuntu Desktop run: sudo apt install python3-venv" >&2
    exit 1
  fi
  "$ROOT/.venv/bin/pip" install --upgrade pip
  "$ROOT/.venv/bin/pip" install "$ROOT"
  mkdir -p "$ROOT/var/logs"
  if [[ ! -f "$ROOT/var/config.yaml" ]]; then
    ROOT="$ROOT" python3 - <<'PY'
import json, os
from pathlib import Path
root = Path(os.environ["ROOT"])
text = (root / "src/fraudledger/default_config.yaml").read_text(encoding="utf-8")
data = str(root / "var")
text = text.replace("data_dir: /var/lib/fraudledger", "data_dir: " + json.dumps(data))
(root / "var/config.yaml").write_text(text, encoding="utf-8")
PY
  fi
  if [[ "$NO_DEMO" -eq 0 ]]; then
    FRAUDLEDGER_CONFIG="$ROOT/var/config.yaml" "$ROOT/.venv/bin/fraudledger" demo
  else
    if [[ -f "$ROOT/var/fraudledger.pid" ]]; then
      old="$(cat "$ROOT/var/fraudledger.pid" || true)"
      if [[ -n "${old}" ]] && kill -0 "$old" 2>/dev/null; then
        kill "$old" || true
        sleep 1
      fi
    fi
    FRAUDLEDGER_CONFIG="$ROOT/var/config.yaml" "$ROOT/.venv/bin/fraudledger" drop-demo
  fi
  cat >"$ROOT/.venv/bin/fraudledger" <<EOF
#!/usr/bin/env bash
export FRAUDLEDGER_CONFIG="${ROOT}/var/config.yaml"
exec "${ROOT}/.venv/bin/python" -m fraudledger "\$@"
EOF
  chmod 0755 "$ROOT/.venv/bin/fraudledger"
  if [[ "$NO_START" -eq 0 ]]; then
    if [[ -f "$ROOT/var/fraudledger.pid" ]]; then
      old="$(cat "$ROOT/var/fraudledger.pid" || true)"
      if [[ -n "${old}" ]] && kill -0 "$old" 2>/dev/null; then
        kill "$old" || true
        sleep 1
      fi
    fi
    FRAUDLEDGER_CONFIG="$ROOT/var/config.yaml" \
      nohup "$ROOT/.venv/bin/fraudledger" serve >>"$ROOT/var/logs/serve.log" 2>&1 &
    echo $! >"$ROOT/var/fraudledger.pid"
    mapfile -t bind < <(read_bind "$ROOT/var/config.yaml")
    wait_health "${bind[0]}" "${bind[1]}"
    echo "Logs: $ROOT/var/logs/serve.log"
  fi
  echo "User install finished."
  echo "Command: $ROOT/.venv/bin/fraudledger"
  echo "Config:  $ROOT/var/config.yaml"
  echo "Next: open the Sources page and load the public working set."
}

install_system() {
  export DEBIAN_FRONTEND=noninteractive
  export NEEDRESTART_MODE=a
  if [[ -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    echo "Installing on ${PRETTY_NAME:-unknown}."
    if [[ "${ID:-}" == "ubuntu" && "${VERSION_ID:-}" != "24.04" && "${VERSION_ID:-}" != "26.04" ]]; then
      echo "Tested on Ubuntu Desktop 24.04 and 26.04. This is ${VERSION_ID}. Continuing."
    elif [[ "${ID:-}" != "ubuntu" ]]; then
      echo "Tested on Ubuntu Desktop. This is ${ID:-unknown}. Continuing."
    fi
  fi
  apt-get update
  apt-get install -y python3 python3-venv ca-certificates curl
  if ! id fraudledger >/dev/null 2>&1; then
    nologin="$(command -v nologin || true)"
    useradd --system --shell "${nologin:-/usr/sbin/nologin}" --home-dir /nonexistent --no-create-home fraudledger
  fi
  install -d -o fraudledger -g fraudledger -m 0750 \
    /var/lib/fraudledger \
    /var/lib/fraudledger/raw \
    /var/lib/fraudledger/parquet/facts \
    /var/lib/fraudledger/warehouse \
    /var/lib/fraudledger/runs \
    /var/lib/fraudledger/exports \
    /var/lib/fraudledger/tmp \
    /var/lib/fraudledger/logs \
    /var/log/fraudledger
  install -d -o root -g fraudledger -m 0750 /etc/fraudledger
  install -d -o root -g root -m 0755 /opt/fraudledger
  python3 -m venv /opt/fraudledger/venv
  /opt/fraudledger/venv/bin/pip install --upgrade pip
  /opt/fraudledger/venv/bin/pip install "$ROOT"
  if [[ ! -f /etc/fraudledger/config.yaml ]]; then
    install -o root -g fraudledger -m 0640 \
      "$ROOT/src/fraudledger/default_config.yaml" \
      /etc/fraudledger/config.yaml
  fi
  install -m 0644 "$ROOT/deploy/fraudledger.service" /etc/systemd/system/fraudledger.service
  install -m 0644 "$ROOT/deploy/fraudledger-ingest.service" /etc/systemd/system/fraudledger-ingest.service
  install -m 0644 "$ROOT/deploy/fraudledger-ingest.timer" /etc/systemd/system/fraudledger-ingest.timer
  systemctl daemon-reload
  systemctl enable fraudledger.service fraudledger-ingest.timer
  if [[ "$NO_DEMO" -eq 0 ]]; then
    sudo -u fraudledger env \
      HOME=/var/lib/fraudledger \
      XDG_CACHE_HOME=/var/lib/fraudledger/tmp \
      FRAUDLEDGER_CONFIG=/etc/fraudledger/config.yaml \
      /opt/fraudledger/venv/bin/fraudledger demo
  else
    systemctl stop fraudledger.service || true
    sudo -u fraudledger env \
      HOME=/var/lib/fraudledger \
      XDG_CACHE_HOME=/var/lib/fraudledger/tmp \
      FRAUDLEDGER_CONFIG=/etc/fraudledger/config.yaml \
      /opt/fraudledger/venv/bin/fraudledger drop-demo
  fi
  if [[ "$NO_START" -eq 0 ]]; then
    systemctl restart fraudledger.service
    systemctl start fraudledger-ingest.timer
    mapfile -t bind < <(read_bind /etc/fraudledger/config.yaml)
    wait_health "${bind[0]}" "${bind[1]}"
  fi
  echo "System install finished. Service: fraudledger.service"
  echo "Next: open the Sources page and load the public working set."
}

if [[ "$USER_MODE" -eq 1 ]]; then
  install_user
else
  install_system
fi
