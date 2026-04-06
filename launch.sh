#!/usr/bin/env bash
# ============================================================
#  SLURM Dashboard — One-Click Launcher
#  Usage:
#    bash launch.sh                         # Start on default port 8000 (password: change-me)
#    bash launch.sh 9090                    # Use custom port
#    bash launch.sh --password mypass       # Use custom password
#    bash launch.sh 9090 --password mypass  # Custom port + password
#    bash launch.sh stop                    # Stop running server
#    bash launch.sh restart                 # Restart server
#    bash launch.sh status                  # Check server status
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.dashboard.pid"
LOG_FILE="$SCRIPT_DIR/server.log"
CACHE_DIR="$SCRIPT_DIR/.cache"
DEFAULT_PORT=8000
DEFAULT_PASSWORD="change-me"
CONDA_ENV="${DASHBOARD_CONDA_ENV:-}"
CONDA_ACTIVATE="${DASHBOARD_CONDA_ACTIVATE:-}"

# ── Colors ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

banner() {
    echo -e "${CYAN}${BOLD}"
    echo "  ╔══════════════════════════════════════════════════════╗"
    echo "  ║          SLURM Dashboard — Task Manager             ║"
    echo "  ╚══════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

activate_conda() {
    if [[ -n "$CONDA_ACTIVATE" && -n "$CONDA_ENV" ]]; then
        if [[ -f "$CONDA_ACTIVATE" ]]; then
            source "$CONDA_ACTIVATE" "$CONDA_ENV"
        else
            echo -e "${RED}[ERROR] Cannot find conda at $CONDA_ACTIVATE${NC}"
            exit 1
        fi
    fi
}

is_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        else
            rm -f "$PID_FILE"
        fi
    fi
    return 1
}

get_hostname() {
    hostname 2>/dev/null || echo "localhost"
}

get_public_ip() {
    # Return first non-loopback IP
    hostname -I 2>/dev/null | awk '{print $1}' || echo ""
}

wait_for_server() {
    local port=$1
    local max_wait=15
    local waited=0
    echo -ne "  ${YELLOW}Waiting for server to start...${NC}"
    while [[ $waited -lt $max_wait ]]; do
        if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/login" 2>/dev/null | grep -q "200"; then
            echo -e " ${GREEN}Ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        ((waited++))
    done
    echo -e " ${RED}Timeout${NC}"
    return 1
}

do_start() {
    local port=${1:-$DEFAULT_PORT}
    local password="${DASHBOARD_PASSWORD:-$DEFAULT_PASSWORD}"

    # Check if already running
    local pid
    if pid=$(is_running); then
        echo -e "  ${YELLOW}[!] Dashboard is already running (PID: $pid)${NC}"
        echo -e "  ${YELLOW}    Use '${BOLD}bash launch.sh stop${NC}${YELLOW}' to stop, or '${BOLD}bash launch.sh restart${NC}${YELLOW}' to restart.${NC}"
        echo ""
        print_access_info "$port" "$password"
        return 0
    fi

    # Activate conda
    activate_conda

    # Verify Python + FastAPI
    if ! python3 -c "import fastapi" 2>/dev/null; then
        echo -e "  ${RED}[ERROR] FastAPI not found. Run: pip install fastapi uvicorn${NC}"
        exit 1
    fi

    # Verify itsdangerous (required by SessionMiddleware)
    if ! python3 -c "import itsdangerous" 2>/dev/null; then
        echo -e "  ${YELLOW}[!] itsdangerous not found, installing...${NC}"
        pip install itsdangerous -q
    fi

        # Preserve historical cache and archived jobs across restarts
    mkdir -p "$CACHE_DIR"

    # Check port availability
    if ss -tlnp 2>/dev/null | grep -q ":$port "; then
        echo -e "  ${RED}[ERROR] Port $port is already in use.${NC}"
        echo -e "  ${YELLOW}  Try a different port: bash launch.sh <port>${NC}"
        exit 1
    fi

    # Start server in background, pass password via environment variable
    cd "$SCRIPT_DIR"
    DASHBOARD_PASSWORD="$password" nohup python3 app.py --host 0.0.0.0 --port "$port" >> "$LOG_FILE" 2>&1 &
    local server_pid=$!
    echo "$server_pid" > "$PID_FILE"

    echo -e "  ${CYAN}[*] Access password: ${BOLD}${password}${NC}"
    echo ""

    # Wait for server to be ready
    if ! wait_for_server "$port"; then
        echo -e "  ${RED}[ERROR] Server failed to start. Check log: $LOG_FILE${NC}"
        tail -5 "$LOG_FILE" 2>/dev/null
        rm -f "$PID_FILE"
        exit 1
    fi

    echo -e "  ${GREEN}${BOLD}[OK] Dashboard started successfully!${NC}"
    echo -e "  ${GREEN}  PID:  $server_pid${NC}"
    echo -e "  ${GREEN}  Port: $port${NC}"
    echo -e "  ${GREEN}  Log:  $LOG_FILE${NC}"
    echo ""
    print_access_info "$port" "$password"
}

print_access_info() {
    local port=$1
    local password="${2:-$DEFAULT_PASSWORD}"
    local hn
    hn=$(get_hostname)
    local ip
    ip=$(get_public_ip)

    echo -e "  ${CYAN}${BOLD}── Access Information ──${NC}"
    echo ""
    echo -e "  ${YELLOW}${BOLD}🔑 Password: ${password}${NC}"
    echo ""

    # Internal cluster access
    if [[ -n "$ip" ]]; then
        echo -e "  ${GREEN}${BOLD}▸ Cluster Internal (direct):${NC}"
        echo -e "    ${BOLD}http://${ip}:${port}${NC}"
        echo ""
    fi

    # SSH tunnel access
    echo -e "  ${GREEN}${BOLD}▸ Remote Access (SSH tunnel):${NC}"
    echo -e "    ${YELLOW}Step 1: Run on your ${BOLD}local machine${NC}${YELLOW}:${NC}"
    echo -e "    ${BOLD}ssh -N -L ${port}:${hn}:${port} <user>@<gateway>${NC}"
    echo ""
    echo -e "    ${YELLOW}Step 2: Open in browser:${NC}"
    echo -e "    ${BOLD}http://localhost:${port}${NC}"
    echo ""

    # Direct node access (if public IP)
    if [[ -n "$ip" && "$ip" != "192.168"* && "$ip" != "172.16"* && "$ip" != "10."* ]]; then
        echo -e "  ${GREEN}${BOLD}▸ Direct Access (if firewall allows):${NC}"
        echo -e "    ${BOLD}http://${ip}:${port}${NC}"
        echo ""
    fi

    echo -e "  ${CYAN}──────────────────────────────${NC}"
}

do_stop() {
    local pid
    if pid=$(is_running); then
        kill "$pid" 2>/dev/null
        rm -f "$PID_FILE"
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "  ${YELLOW}[*] Graceful stop timed out, force killing...${NC}"
            kill -9 "$pid" 2>/dev/null
        fi
        echo -e "  ${GREEN}[OK] Dashboard stopped (PID: $pid)${NC}"
    else
        echo -e "  ${GREEN}[OK] No running dashboard found.${NC}"
    fi
}

do_status() {
    local pid
    if pid=$(is_running); then
        echo -e "  ${GREEN}[RUNNING] Dashboard is active (PID: $pid)${NC}"
        local port
        port=$(ss -tlnp 2>/dev/null | grep "pid=$pid" | awk '{print $4}' | grep -oP ':\K[0-9]+' | head -1)
        if [[ -n "$port" ]]; then
            echo -e "  ${GREEN}  Port: $port${NC}"
            print_access_info "$port"
        else
            echo -e "  ${YELLOW}  Port: (checking...)${NC}"
            print_access_info "$DEFAULT_PORT"
        fi
    else
        echo -e "  ${YELLOW}[STOPPED] Dashboard is not running.${NC}"
        echo -e "  ${YELLOW}  Start with: bash launch.sh${NC}"
    fi
}

do_restart() {
    local port=${1:-$DEFAULT_PORT}
    echo -e "  ${YELLOW}[*] Restarting dashboard...${NC}"
    do_stop
    sleep 1
    do_start "$port"
}

# ── Parse --password / -p arguments before the main case ──
CUSTOM_PASSWORD=""
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --password|-p)
            if [[ -n "${2:-}" && "$2" != --* ]]; then
                CUSTOM_PASSWORD="$2"
                shift 2
            else
                echo -e "${RED}[ERROR] --password requires a value${NC}"
                exit 1
            fi
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
# Restore positional parameters
set -- "${POSITIONAL_ARGS[@]:-}"

# Export password so child processes inherit it
if [[ -n "$CUSTOM_PASSWORD" ]]; then
    export DASHBOARD_PASSWORD="$CUSTOM_PASSWORD"
fi

# ── Main ──
banner

case "${1:-start}" in
    stop)
        do_stop
        ;;
    status)
        do_status
        ;;
    restart)
        do_restart "${2:-$DEFAULT_PORT}"
        ;;
    start)
        do_start "${2:-$DEFAULT_PORT}"
        ;;
    [0-9]*)
        # bare port number: bash launch.sh 9090
        do_start "$1"
        ;;
    -h|--help|help)
        echo "  Usage: bash launch.sh [start|stop|restart|status|<port>] [--password <pass>]"
        echo ""
        echo "  Commands:"
        echo "    start [port]    Start dashboard (default port: $DEFAULT_PORT)"
        echo "    stop            Stop running dashboard"
        echo "    restart [port]  Restart dashboard"
        echo "    status          Check if dashboard is running"
        echo "    <port>          Start on specified port"
        echo ""
        echo "  Options:"
        echo "    --password, -p <pass>   Set access password (default: $DEFAULT_PASSWORD)"
        echo "                            Can also set via env: DASHBOARD_PASSWORD=xxx bash launch.sh"
        echo ""
        echo "  Examples:"
        echo "    bash launch.sh                           # port=$DEFAULT_PORT, password=$DEFAULT_PASSWORD"
        echo "    bash launch.sh 9090                      # port=9090"
        echo "    bash launch.sh --password secret         # custom password"
        echo "    bash launch.sh 9090 --password secret    # custom port + password"
        echo "    DASHBOARD_PASSWORD=secret bash launch.sh # via environment variable"
        echo ""
        ;;
    *)
        echo -e "  ${RED}Unknown command: $1${NC}"
        echo "  Usage: bash launch.sh [start|stop|restart|status|<port>] [--password <pass>]"
        exit 1
        ;;
esac
