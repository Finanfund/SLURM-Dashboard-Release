#!/usr/bin/env python3
"""
SLURM Dashboard — Launcher
Usage:
    python start.py              # Start in background on default port (8000)
    python start.py --port 9090  # Use custom port
    python start.py --fg         # Run in foreground (for debugging)
"""
import argparse
import os
import sys
import signal
import subprocess
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(SCRIPT_DIR, ".dashboard.pid")
LOG_FILE = os.path.join(SCRIPT_DIR, "server.log")
CACHE_DIR = os.path.join(SCRIPT_DIR, ".cache")

def find_python():
    """Find the correct Python with FastAPI installed."""
    for p in [sys.executable, "python3", "python"]:
        try:
            out = subprocess.check_output([p, "-c", "import fastapi; print('ok')"],
                                          stderr=subprocess.DEVNULL).decode().strip()
            if out == "ok":
                return p
        except Exception:
            continue
    return None

def is_running():
    """Check if dashboard is already running."""
    if not os.path.exists(PID_FILE):
        return False, 0
    with open(PID_FILE) as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, 0)
        return True, pid
    except OSError:
        os.remove(PID_FILE)
        return False, 0

def clear_cache():
    """Clear history cache on startup."""
    if os.path.exists(CACHE_DIR):
        import shutil
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="SLURM Dashboard Launcher")
    parser.add_argument("--port", type=int, default=8000, help="Server port (default: 8000)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--fg", action="store_true", help="Run in foreground")
    args = parser.parse_args()

    running, pid = is_running()
    if running:
        print(f"\033[33m[!] Dashboard is already running (PID: {pid})\033[0m")
        print(f"    Use 'python stop.py' to stop it first.")
        sys.exit(1)

    python = find_python()
    if not python:
        print("\033[31m[ERROR] Cannot find Python with FastAPI. Run: pip install fastapi uvicorn\033[0m")
        sys.exit(1)

    clear_cache()

    hostname = subprocess.check_output(["hostname"]).decode().strip()

    if args.fg:
        # Foreground mode
        print(f"\033[36m{'='*60}\033[0m")
        print(f"\033[36m  SLURM Dashboard — Starting in foreground\033[0m")
        print(f"\033[36m{'='*60}\033[0m")
        print(f"\033[32m  Local:  http://localhost:{args.port}\033[0m")
        print(f"\033[32m  Remote: ssh -L {args.port}:{hostname}:{args.port} <gateway>\033[0m")
        print(f"\033[32m          then open http://localhost:{args.port}\033[0m")
        print(f"\033[36m{'='*60}\033[0m")
        print(f"  Press Ctrl+C to stop.\n")
        os.chdir(SCRIPT_DIR)
        os.execv(python, [python, os.path.join(SCRIPT_DIR, "app.py"),
                          "--host", args.host, "--port", str(args.port)])
    else:
        # Background (daemon) mode
        log_f = open(LOG_FILE, "a")
        proc = subprocess.Popen(
            [python, os.path.join(SCRIPT_DIR, "app.py"),
             "--host", args.host, "--port", str(args.port)],
            cwd=SCRIPT_DIR,
            stdout=log_f, stderr=log_f,
            start_new_session=True  # detach from terminal
        )
        with open(PID_FILE, "w") as f:
            f.write(str(proc.pid))

        # Wait briefly to check it started
        time.sleep(2)
        if proc.poll() is not None:
            print(f"\033[31m[ERROR] Server failed to start. Check {LOG_FILE}\033[0m")
            os.remove(PID_FILE)
            sys.exit(1)

        print(f"\033[36m{'='*60}\033[0m")
        print(f"\033[36m  SLURM Dashboard — Started in background\033[0m")
        print(f"\033[36m{'='*60}\033[0m")
        print(f"\033[32m  PID:    {proc.pid}\033[0m")
        print(f"\033[32m  Port:   {args.port}\033[0m")
        print(f"\033[32m  Log:    {LOG_FILE}\033[0m")
        print()
        print(f"\033[33m  Access from your local machine:\033[0m")
        print(f"\033[33m    1. Run: ssh -L {args.port}:{hostname}:{args.port} <your_gateway>\033[0m")
        print(f"\033[33m    2. Open: http://localhost:{args.port}\033[0m")
        print()
        print(f"\033[33m  Or from cluster internal network:\033[0m")
        print(f"\033[33m    Open: http://{hostname}:{args.port}\033[0m")
        print(f"\033[36m{'='*60}\033[0m")
        print(f"  To stop: python {os.path.join(SCRIPT_DIR, 'stop.py')}")

if __name__ == "__main__":
    main()
