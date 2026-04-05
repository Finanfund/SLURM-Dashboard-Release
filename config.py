"""Configuration for SLURM Dashboard"""
import os
import json
import hashlib

# Server
HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
PORT = int(os.getenv("DASHBOARD_PORT", "8000"))

# Refresh
DEFAULT_REFRESH_INTERVAL = 10  # seconds
MIN_REFRESH_INTERVAL = 2  # 1秒在多节点场景下易导致SSH通道饱和
MAX_REFRESH_INTERVAL = 300

# SSH
SSH_TIMEOUT = 8
SSH_OPTIONS = [
    "-o", "ConnectTimeout=5",
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    # ControlMaster 由 ~/.ssh/config 统一配置（pdsh 和 asyncio SSH 共享连接池）
]

# History
HISTORY_MAX_POINTS = 36000  # ~100 hours at 10s interval

# Cluster
CLUSTER_NAME = os.getenv("DASHBOARD_CLUSTER_NAME", "SLURM HPC Cluster")

# File browser
FILE_BROWSER_ROOT = os.path.realpath(os.getenv("FILE_BROWSER_ROOT", os.path.expanduser("~")))

# Log
LOG_TAIL_LINES = 200

# Cache
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, ".cache")

# Max file size for text editing (5MB)
MAX_EDIT_FILE_SIZE = 5 * 1024 * 1024

# Max upload file size (100MB)
MAX_UPLOAD_SIZE = 100 * 1024 * 1024

# ── User Settings (persisted) ──
USER_SETTINGS_FILE = os.path.join(SCRIPT_DIR, "user_settings.json")

DEFAULT_USER_SETTINGS = {
    "historyDurationMin": 60,       # chart history window in minutes
    "refreshIntervalSec": 10,       # server refresh interval in seconds
    "showJobCurves": False,         # show per-job curves in node chart
    "showFolderSizes": False,       # show folder sizes in file browser
    "maxCacheMB": 100,              # 最大缓存大小 (MB), 0=使用日期限制
    "cacheRetainDate": "",          # 保留自此日期之后的缓存 (当maxCacheMB=0时生效, 格式 YYYY-MM-DD)
    "bookmarks": [],                # 收藏的文件/文件夹路径列表
    "historyTrackUsers": "",        # 需要追踪历史任务的用户名（逗号分隔）
    "clusterUsername": "",          # 本机集群用户名，用于实时采集 stdout/stderr
    "numaTrackEnabled": False,      # 是否记录 NUMA 内存分布趋势
    "theme": "dark",                # UI theme (reserved)
}

def load_user_settings():
    """Load user settings from disk, merge with defaults."""
    settings = dict(DEFAULT_USER_SETTINGS)
    if os.path.exists(USER_SETTINGS_FILE):
        try:
            with open(USER_SETTINGS_FILE, "r") as f:
                saved = json.load(f)
            settings.update(saved)
        except Exception:
            pass
    return settings

def save_user_settings(settings):
    """Save user settings to disk."""
    try:
        with open(USER_SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=2, ensure_ascii=False)
        return True
    except Exception:
        return False

# ── Access Control ──
ACCESS_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "change-me")
# Derive session secret deterministically from password (no extra storage needed)
SESSION_SECRET = hashlib.sha256(
    f"slurm-dashboard-v1-{ACCESS_PASSWORD}".encode()
).hexdigest()
