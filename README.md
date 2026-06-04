# SLURM Dashboard

A lightweight FastAPI web dashboard for monitoring SLURM clusters. It shows
cluster partitions, nodes, jobs, job logs, historical charts, login-node
processes, and a browser-based file manager for a configured directory.

The frontend is plain JavaScript served by FastAPI. There is no npm or webpack
build step.

## Features

- Real-time cluster snapshots over WebSocket
- Partition, node, and job views backed by `sinfo`, `squeue`, and `scontrol`
- Optional cgroup-based CPU, memory, and NUMA metrics collected over SSH
- Job log tailing for a configured cluster username
- Historical node/job charts persisted in local cache files
- File browser with upload, download, edit, delete, mkdir, and sbatch helpers
- Login-node process view with optional process termination
- Password-protected web UI using a server-side session

## Requirements

- Linux management/login node with access to SLURM CLI tools
- Python 3.9 or newer
- SSH access from the dashboard host to compute nodes
- Python packages listed in `requirements.txt`
- Optional: `paramiko` for faster persistent SSH collection; without it the app
  falls back to system `ssh`

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

This release intentionally ships without a default password, default port,
default cluster username, or recorded user settings.

Required:

```bash
export DASHBOARD_PASSWORD='a-long-random-password'
export DASHBOARD_PORT=9000
```

Common optional settings:

```bash
export DASHBOARD_HOST=127.0.0.1
export DASHBOARD_FILE_BROWSER_ROOT=/path/to/cluster/home
export DASHBOARD_CLUSTER_NAME='SLURM HPC Cluster'
```

`DASHBOARD_FILE_BROWSER_ROOT` defaults to the current user's home directory.
Write operations in the file browser are restricted to that root. The read-only
directory tree can still inspect readable paths outside it, but destructive file
operations are blocked outside the configured root.

Runtime UI settings are saved to `user_settings.json`, which is intentionally
ignored by git.

## Run

One-shot launcher:

```bash
DASHBOARD_PASSWORD='a-long-random-password' bash launch.sh start 9000
```

Using environment variables:

```bash
export DASHBOARD_PASSWORD='a-long-random-password'
export DASHBOARD_PORT=9000
bash launch.sh start
```

Foreground mode without the launcher:

```bash
export DASHBOARD_PASSWORD='a-long-random-password'
python3 app.py --host 127.0.0.1 --port 9000
```

Background mode with the Python helper:

```bash
python3 start.py --port 9000 --password 'a-long-random-password'
```

Stop or inspect the service:

```bash
bash launch.sh status
bash launch.sh stop
python3 stop.py
```

## Remote Access

For a cluster login node, keep the dashboard bound to localhost and forward it
through SSH:

```bash
ssh -N -L 9000:<dashboard-hostname>:9000 <user>@<gateway>
```

Then open:

```text
http://127.0.0.1:9000
```

## Validation

Run the lightweight release smoke check:

```bash
python3 scripts/smoke_check.py
```

The smoke check compiles the Python sources, verifies sanitized defaults, and
tests the login/auth flow without starting SLURM or SSH collection.

For a live cluster check:

```bash
export DASHBOARD_PASSWORD='a-long-random-password'
bash launch.sh start 9000
curl -I http://127.0.0.1:9000/login
bash launch.sh stop
```

## Runtime Files

These files and directories are created locally and should not be committed:

- `.cache/`
- `user_settings.json`
- `server.log`
- `.dashboard.pid`
- Python/tooling caches such as `__pycache__/` and `.pytest_cache/`

## Security Notes

- Set a strong `DASHBOARD_PASSWORD` before starting the service.
- Prefer `DASHBOARD_HOST=127.0.0.1` with SSH tunneling.
- Do not publish `user_settings.json`, logs, cache files, screenshots with real
  job data, or cluster-specific helper scripts.
- Review the file-browser root carefully before enabling write operations for
  other users.
- The dashboard can run `scancel`, `sbatch`, shell scripts, file writes, and
  process termination through the web UI after login. Treat it as an operator
  tool, not as an anonymous public website.

## Repository Layout

```text
.
├── app.py                 # FastAPI application, routes, auth, WebSocket
├── collector.py           # SLURM, SSH, cgroup, history, and file helpers
├── config.py              # Environment and runtime settings
├── launch.sh              # Bash start/stop/status helper
├── start.py               # Python background/foreground launcher
├── stop.py                # Python stop helper
├── requirements.txt       # Python dependencies
├── scripts/smoke_check.py # Lightweight release validation
├── static/                # CSS and JavaScript
└── templates/             # Jinja2 HTML templates
```

## License

No license file is included in this release. Add the license you intend to use
before advertising the repository as reusable open-source software.
