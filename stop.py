#!/usr/bin/env python3
"""
SLURM Dashboard — Stop Script
Usage:
    python stop.py           # Stop the running dashboard
    python stop.py --force   # Force kill (SIGKILL)
"""
import argparse
import os
import signal
import sys
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(SCRIPT_DIR, ".dashboard.pid")

def find_processes():
    """Find all dashboard-related processes."""
    procs = []
    try:
        out = subprocess.check_output(
            ["ps", "aux"], stderr=subprocess.DEVNULL
        ).decode(errors="replace")
        for line in out.splitlines():
            if "app.py" in line and ("--port" in line or "uvicorn" in line.lower()):
                if "stop.py" not in line and "grep" not in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        procs.append({"pid": int(parts[1]), "cmd": " ".join(parts[10:])})
    except Exception:
        pass
    return procs

def main():
    parser = argparse.ArgumentParser(description="Stop SLURM Dashboard")
    parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    args = parser.parse_args()

    sig = signal.SIGKILL if args.force else signal.SIGTERM

    # Try PID file first
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        try:
            os.kill(pid, 0)  # check alive
            print(f"\033[33m[*] Found dashboard process PID={pid} (from PID file)\033[0m")
            os.kill(pid, sig)
            os.remove(PID_FILE)
            print(f"\033[32m[OK] Dashboard stopped (PID: {pid})\033[0m")
            return
        except ProcessLookupError:
            os.remove(PID_FILE)
            print(f"\033[33m[*] Stale PID file removed (PID {pid} not running)\033[0m")
        except PermissionError:
            print(f"\033[31m[ERROR] Permission denied to kill PID {pid}\033[0m")
            return

    # Scan for processes
    procs = find_processes()
    if not procs:
        print(f"\033[32m[OK] No running dashboard processes found.\033[0m")
        return

    print(f"\033[33m[*] Found {len(procs)} dashboard process(es):\033[0m")
    for i, p in enumerate(procs):
        print(f"    [{i+1}] PID={p['pid']}  {p['cmd']}")

    if len(procs) == 1:
        answer = input(f"\n  Kill PID {procs[0]['pid']}? [Y/n] ").strip().lower()
        if answer in ("", "y", "yes"):
            os.kill(procs[0]["pid"], sig)
            print(f"\033[32m[OK] Killed PID {procs[0]['pid']}\033[0m")
        else:
            print("  Cancelled.")
    else:
        answer = input(f"\n  Kill all? [Y/n/number] ").strip().lower()
        if answer in ("", "y", "yes"):
            for p in procs:
                try:
                    os.kill(p["pid"], sig)
                    print(f"\033[32m  Killed PID {p['pid']}\033[0m")
                except Exception as e:
                    print(f"\033[31m  Failed to kill PID {p['pid']}: {e}\033[0m")
        elif answer.isdigit():
            idx = int(answer) - 1
            if 0 <= idx < len(procs):
                os.kill(procs[idx]["pid"], sig)
                print(f"\033[32m[OK] Killed PID {procs[idx]['pid']}\033[0m")
        else:
            print("  Cancelled.")

    # Clean up PID file
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)

if __name__ == "__main__":
    main()
