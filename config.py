"""Configuration for SLURM Dashboard"""
import os
import json
import hashlib

# Server
HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
PORT = int(os.getenv("DASHBOARD_PORT", "8089"))

# Refresh
DEFAULT_REFRESH_INTERVAL = 10  # seconds
MIN_REFRESH_INTERVAL = 1
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
FILE_BROWSER_ROOT = os.getenv("FILE_BROWSER_ROOT", os.path.expanduser("~"))

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
    "historyTrackUsers": "",        # 需要追踪历史任务的用户名（逗号分隔），首次使用请填写
    "clusterUsername": "",            # 本机集群用户名，用于实时采集 stdout/stderr，首次使用请填写
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
_PASSWORD_FILE = os.path.join(SCRIPT_DIR, ".dashboard_password")

def _get_password():
    """获取访问密码：环境变量 > 密码文件 > 自动生成"""
    env_pass = os.getenv("DASHBOARD_PASSWORD")
    if env_pass:
        return env_pass
    if os.path.exists(_PASSWORD_FILE):
        try:
            with open(_PASSWORD_FILE, "r") as f:
                saved = f.read().strip()
            if saved:
                return saved
        except Exception:
            pass
    # 首次运行时自动生成随机密码
    import secrets
    import string
    new_pass = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(10))
    try:
        with open(_PASSWORD_FILE, "w") as f:
            f.write(new_pass)
        os.chmod(_PASSWORD_FILE, 0o600)
    except Exception:
        pass
    print(f"\n{'='*55}")
    print(f"  🔐 首次启动 — 已自动生成访问密码: {new_pass}")
    print(f"  密码已保存至: {_PASSWORD_FILE}")
    print(f"  可通过 --password 参数或 DASHBOARD_PASSWORD 环境变量覆盖")
    print(f"{'='*55}\n")
    return new_pass

ACCESS_PASSWORD = _get_password()
# Derive session secret deterministically from password (no extra storage needed)
SESSION_SECRET = hashlib.sha256(
    f"slurm-dashboard-v1-{ACCESS_PASSWORD}".encode()
).hexdigest()
