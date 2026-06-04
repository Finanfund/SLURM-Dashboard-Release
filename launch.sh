#!/usr/bin/env bash
# SLURM Dashboard launcher
#
# Usage:
#   DASHBOARD_PASSWORD='change-me' bash launch.sh start 9000
#   DASHBOARD_PASSWORD='change-me' DASHBOARD_PORT=9000 bash launch.sh start
#   bash launch.sh stop
#   bash launch.sh restart 9000
#   bash launch.sh status

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.dashboard.pid"
LOG_FILE="$SCRIPT_DIR/server.log"
CACHE_DIR="$SCRIPT_DIR/.cache"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

banner() {
    echo -e "${CYAN}${BOLD}"
    echo "  SLURM Dashboard"
    echo -e "${NC}"
}

die() {
    echo -e "  ${RED}[ERROR] $*${NC}" >&2
    exit 1
}

activate_python_env() {
    if [[ -z "${DASHBOARD_CONDA_ENV:-}" ]]; then
        return
    fi

    local conda_base="${DASHBOARD_CONDA_BASE:-}"
    if [[ -z "$conda_base" && -n "${CONDA_EXE:-}" ]]; then
        conda_base="$(cd "$(dirname "$CONDA_EXE")/.." && pwd)"
    fi
    if [[ -z "$conda_base" ]] && command -v conda >/dev/null 2>&1; then
        conda_base="$(conda info --base)"
    fi
    [[ -n "$conda_base" && -f "$conda_base/etc/profile.d/conda.sh" ]] || \
        die "Cannot activate conda env; set DASHBOARD_CONDA_BASE or DASHBOARD_PYTHON."

    # shellcheck disable=SC1090
    source "$conda_base/etc/profile.d/conda.sh"
    conda activate "$DASHBOARD_CONDA_ENV"
}

python_bin() {
    echo "${DASHBOARD_PYTHON:-python3}"
}

check_python_deps() {
    local py
    py="$(python_bin)"
    "$py" - <<'PY' || die "Missing dependencies. Run: pip install -r requirements.txt"
import fastapi, uvicorn, jinja2, websockets, aiofiles, itsdangerous, multipart
PY
}

is_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid="$(cat "$PID_FILE")"
        if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
        rm -f "$PID_FILE"
    fi
    return 1
}

validate_port() {
    local port="$1"
    [[ "$port" =~ ^[0-9]+$ ]] || die "Port must be a number."
    (( port > 0 && port <= 65535 )) || die "Port must be between 1 and 65535."
}

resolve_port() {
    local port="${1:-${DASHBOARD_PORT:-}}"
    if [[ -z "$port" && -t 0 ]]; then
        read -r -p "  Port: " port
    fi
    [[ -n "$port" ]] || die "Port is required. Pass a port or set DASHBOARD_PORT."
    validate_port "$port"
    echo "$port"
}

resolve_password() {
    local password="${CUSTOM_PASSWORD:-${DASHBOARD_PASSWORD:-}}"
    if [[ -z "$password" && -t 0 ]]; then
        read -r -s -p "  Access password: " password
        echo
    fi
    [[ -n "$password" ]] || die "DASHBOARD_PASSWORD is required."
    echo "$password"
}

port_in_use() {
    local port="$1"
    if command -v ss >/dev/null 2>&1; then
        ss -tln 2>/dev/null | awk '{print $4}' | grep -Eq "[:.]${port}$"
        return
    fi
    if command -v lsof >/dev/null 2>&1; then
        lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
        return
    fi
    return 1
}

get_hostname() {
    hostname 2>/dev/null || echo "localhost"
}

wait_for_server() {
    local port="$1"
    local max_wait=15
    local waited=0
    echo -ne "  ${YELLOW}Waiting for server to start...${NC}"
    while [[ "$waited" -lt "$max_wait" ]]; do
        if curl -fsS "http://127.0.0.1:$port/login" >/dev/null 2>&1; then
            echo -e " ${GREEN}Ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        waited=$((waited + 1))
    done
    echo -e " ${RED}Timeout${NC}"
    return 1
}

print_access_info() {
    local port="$1"
    local host
    host="$(get_hostname)"

    echo -e "  ${CYAN}${BOLD}Access${NC}"
    echo -e "    Local:      ${BOLD}http://127.0.0.1:${port}${NC}"
    echo -e "    SSH tunnel: ${BOLD}ssh -N -L ${port}:${host}:${port} <user>@<gateway>${NC}"
    echo -e "    Browser:    ${BOLD}http://127.0.0.1:${port}${NC}"
}

do_start() {
    local port
    port="$(resolve_port "${1:-}")"
    local password
    password="$(resolve_password)"
    local host="${DASHBOARD_HOST:-127.0.0.1}"

    local pid
    if pid="$(is_running)"; then
        echo -e "  ${YELLOW}[!] Dashboard is already running (PID: $pid)${NC}"
        print_access_info "$port"
        return 0
    fi

    activate_python_env
    check_python_deps
    mkdir -p "$CACHE_DIR"

    if port_in_use "$port"; then
        die "Port $port is already in use."
    fi

    cd "$SCRIPT_DIR"
    local py
    py="$(python_bin)"
    DASHBOARD_PASSWORD="$password" DASHBOARD_PORT="$port" DASHBOARD_HOST="$host" \
        nohup "$py" app.py --host "$host" --port "$port" >> "$LOG_FILE" 2>&1 &
    local server_pid=$!
    echo "$server_pid" > "$PID_FILE"

    if ! wait_for_server "$port"; then
        echo -e "  ${RED}[ERROR] Server failed to start. Check log: $LOG_FILE${NC}"
        tail -20 "$LOG_FILE" 2>/dev/null || true
        rm -f "$PID_FILE"
        exit 1
    fi

    echo -e "  ${GREEN}${BOLD}[OK] Dashboard started.${NC}"
    echo -e "  ${GREEN}  PID:  $server_pid${NC}"
    echo -e "  ${GREEN}  Port: $port${NC}"
    echo -e "  ${GREEN}  Host: $host${NC}"
    echo -e "  ${GREEN}  Log:  $LOG_FILE${NC}"
    print_access_info "$port"
}

do_stop() {
    local pid
    if pid="$(is_running)"; then
        kill "$pid" 2>/dev/null || true
        rm -f "$PID_FILE"
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
        echo -e "  ${GREEN}[OK] Dashboard stopped (PID: $pid)${NC}"
    else
        echo -e "  ${GREEN}[OK] No running dashboard found.${NC}"
    fi
}

do_status() {
    local pid
    if pid="$(is_running)"; then
        echo -e "  ${GREEN}[RUNNING] Dashboard is active (PID: $pid)${NC}"
    else
        echo -e "  ${YELLOW}[STOPPED] Dashboard is not running.${NC}"
    fi
}

do_restart() {
    local port="${1:-}"
    echo -e "  ${YELLOW}Restarting dashboard...${NC}"
    do_stop
    do_start "$port"
}

usage() {
    cat <<'EOF'
Usage:
  DASHBOARD_PASSWORD='change-me' bash launch.sh start <port>
  DASHBOARD_PASSWORD='change-me' DASHBOARD_PORT=<port> bash launch.sh start
  bash launch.sh stop
  bash launch.sh restart <port>
  bash launch.sh status

Options:
  --password, -p <value>       Set access password for this launch.

Environment:
  DASHBOARD_PASSWORD           Required access password.
  DASHBOARD_PORT               Port to listen on when no port argument is given.
  DASHBOARD_HOST               Bind host, defaults to 127.0.0.1.
  DASHBOARD_FILE_BROWSER_ROOT  Writable file browser root, defaults to $HOME.
  DASHBOARD_PYTHON             Python executable, defaults to python3.
  DASHBOARD_CONDA_ENV          Optional conda environment name/path to activate.
  DASHBOARD_CONDA_BASE         Optional conda base path when conda is not on PATH.
EOF
}

COMMAND="${1:-start}"
if [[ $# -gt 0 ]]; then
    shift
fi
PORT_ARG=""
CUSTOM_PASSWORD=""
POSITIONAL_ARGS=()

if [[ "$COMMAND" =~ ^[0-9]+$ ]]; then
    PORT_ARG="$COMMAND"
    COMMAND="start"
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --password|-p)
            [[ -n "${2:-}" && "$2" != --* ]] || die "--password requires a value"
            CUSTOM_PASSWORD="$2"
            shift 2
            ;;
        --password=*)
            CUSTOM_PASSWORD="${1#--password=}"
            shift
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -z "$PORT_ARG" && ${#POSITIONAL_ARGS[@]} -gt 0 ]]; then
    PORT_ARG="${POSITIONAL_ARGS[0]}"
fi

banner
case "$COMMAND" in
    start)
        do_start "$PORT_ARG"
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_restart "$PORT_ARG"
        ;;
    status)
        do_status
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        die "Unknown command: $COMMAND"
        ;;
esac
