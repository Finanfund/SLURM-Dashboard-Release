"""
SLURM Dashboard - FastAPI Application
"""

import asyncio
import json
import logging
import mimetypes
import os
import signal
import shutil
import tempfile
import time
import zipfile
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

import config
from collector import DataCollector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("dashboard")

collector = DataCollector()
ws_clients: List[WebSocket] = []
bg_task: asyncio.Task = None
refresh_interval: int = config.DEFAULT_REFRESH_INTERVAL


# 流水线采集：每个refresh_interval启动一个新采集任务，不等待上一个完成
# 注意: Semaphore 必须在事件循环内创建，否则 Python 3.9 会报 "attached to a different loop"
_collect_semaphore = None  # 在 lifespan 中初始化


def _clamp_refresh_interval(val: int) -> int:
    return max(config.MIN_REFRESH_INTERVAL, min(config.MAX_REFRESH_INTERVAL, int(val)))


def _get_server_refresh_interval() -> int:
    """Server-side collection cadence; do not let websocket clients override it."""
    settings = collector._load_user_settings_cached()
    return _clamp_refresh_interval(settings.get("refreshIntervalSec", config.DEFAULT_REFRESH_INTERVAL))


async def _safe_send_ws(ws: WebSocket, msg: str, timeout: float = 0.8) -> bool:
    try:
        await asyncio.wait_for(ws.send_text(msg), timeout=timeout)
        return True
    except Exception:
        return False

async def collect_and_broadcast():
    """单次采集并广播结果"""
    async with _collect_semaphore:
        try:
            t0 = time.time()
            snapshot = await collector.collect()
            elapsed = time.time() - t0
            running = sum(1 for j in snapshot.jobs.values() if j.state == "RUNNING")
            pending = sum(1 for j in snapshot.jobs.values() if j.state == "PENDING")
            logger.info(f"Collected in {elapsed:.2f}s - {running} running, {pending} pending")
            # Warm-up: 前 2 轮采集的 CPU delta 不可靠（无 prev），跳过广播
            if collector._collect_count <= 2:
                logger.info(f"Warmup cycle {collector._collect_count}, skipping broadcast")
                return
            data = collector.snapshot_to_dict(snapshot)
            data["_collect_time_ms"] = round(elapsed * 1000)
            data["_server_paused"] = collector.paused
            msg = json.dumps(data, ensure_ascii=False)
            clients = list(ws_clients)
            t_send0 = time.time()
            send_results = await asyncio.gather(
                *[_safe_send_ws(ws, msg) for ws in clients],
                return_exceptions=True
            )
            stale = []
            for ws, ok in zip(clients, send_results):
                if ok is not True:
                    stale.append(ws)
            for ws in stale:
                if ws in ws_clients:
                    ws_clients.remove(ws)
            send_elapsed_ms = round((time.time() - t_send0) * 1000)
            if stale or send_elapsed_ms > 400:
                logger.info(f"WS broadcast {len(clients)-len(stale)}/{len(clients)} ok, "
                            f"stale={len(stale)}, send_ms={send_elapsed_ms}")
        except Exception as e:
            logger.error(f"collect_and_broadcast error: {e}", exc_info=True)

async def background_collector():
    global refresh_interval
    while True:
        try:
            if not collector.paused:
                # 反压机制：semaphore无空闲槽时跳过本轮，避免任务堆积
                if _collect_semaphore._value > 0:
                    asyncio.create_task(collect_and_broadcast())
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Collection schedule error: {e}", exc_info=True)
        refresh_interval = _get_server_refresh_interval()
        # 自适应降频：连续全缓存时临时增大间隔，避免SSH通道饱和
        effective_interval = refresh_interval
        if hasattr(collector, '_consecutive_all_cached') and collector._consecutive_all_cached >= 5:
            effective_interval = max(refresh_interval, 5)
        await asyncio.sleep(effective_interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bg_task, _collect_semaphore
    config.validate_runtime_config()
    _collect_semaphore = asyncio.Semaphore(1)  # 串行采集，避免并发SSH竞争
    logger.info(f"Starting SLURM Dashboard on {config.HOST}")
    bg_task = asyncio.create_task(background_collector())
    yield
    bg_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass
    # 关闭前保存缓存和归档，确保不丢失历史数据
    try:
        collector._save_cache()
        collector._save_archived_jobs(force=True)
        logger.info("Shutdown: cache and archived jobs saved successfully")
    except Exception as e:
        logger.error(f"Shutdown save error: {e}")


app = FastAPI(title="SLURM Dashboard", lifespan=lifespan)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
FILE_BROWSER_ROOT_REAL = os.path.realpath(config.FILE_BROWSER_ROOT)


def render_template(request: Request, name: str, context: dict, status_code: int = 200):
    """Render templates across Starlette TemplateResponse signature variants."""
    payload = {"request": request}
    payload.update(context)
    try:
        return templates.TemplateResponse(request, name, payload, status_code=status_code)
    except TypeError:
        return templates.TemplateResponse(name, payload, status_code=status_code)


async def _run_io(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(collector._io_executor, func, *args)


async def _realpath(path: str) -> str:
    return await _run_io(os.path.realpath, path)


async def _isfile(path: str) -> bool:
    return await _run_io(os.path.isfile, path)


async def _isdir(path: str) -> bool:
    return await _run_io(os.path.isdir, path)


async def _access(path: str, mode: int) -> bool:
    return await _run_io(os.access, path, mode)


async def _getsize(path: str) -> int:
    return await _run_io(os.path.getsize, path)


def _within_file_browser_root(path: str) -> bool:
    try:
        return os.path.commonpath([FILE_BROWSER_ROOT_REAL, path]) == FILE_BROWSER_ROOT_REAL
    except ValueError:
        return False


async def _can_read_path(path: str) -> bool:
    if await _isdir(path):
        return await _access(path, os.R_OK | os.X_OK)
    return await _access(path, os.R_OK)


async def _save_user_settings_async(settings: dict) -> bool:
    ok = await _run_io(config.save_user_settings, settings)
    if ok:
        collector.update_user_settings_cache(settings)
    return ok


# ── Auth Middleware ──
class AuthMiddleware(BaseHTTPMiddleware):
    """Block unauthenticated HTTP requests.
    WebSocket auth is handled inside the ws endpoint itself."""
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Public paths: login page, static assets
        if path in ("/login", "/logout") or path.startswith("/static/"):
            return await call_next(request)
        # Check session
        if not request.session.get("authenticated"):
            # API calls get JSON 401
            if path.startswith("/api/"):
                return JSONResponse(
                    {"error": "Unauthorized", "detail": "Please log in first."},
                    status_code=401
                )
            # All other pages redirect to login
            next_path = request.url.path
            return RedirectResponse(f"/login?next={next_path}", status_code=303)
        return await call_next(request)


# ── Register Middleware (order matters: SessionMiddleware must be outermost) ──
# Execution order: SessionMiddleware → AuthMiddleware → routes
app.add_middleware(AuthMiddleware)
app.add_middleware(SessionMiddleware, secret_key=config.SESSION_SECRET, max_age=86400 * 7, https_only=False)


# ── Login / Logout ──
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/"):
    return render_template(request, "login.html", {
        "next": next,
        "cluster_name": config.CLUSTER_NAME,
        "error": None,
    })


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, next: str = "/"):
    form = await request.form()
    password = form.get("password", "")
    if password == config.ACCESS_PASSWORD:
        request.session["authenticated"] = True
        logger.info(f"Successful login from {request.client.host}")
        return RedirectResponse(next if next else "/", status_code=303)
    logger.warning(f"Failed login attempt from {request.client.host}")
    return render_template(request, "login.html", {
        "next": next,
        "cluster_name": config.CLUSTER_NAME,
        "error": "密码错误，请重试。",
    }, status_code=401)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# ── Pages ──
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return render_template(request, "index.html", {
        "cluster_name": config.CLUSTER_NAME,
        "refresh_interval": refresh_interval, "user_settings": collector._load_user_settings_cached(),
        "file_browser_root": config.FILE_BROWSER_ROOT,
    })


# ── WebSocket ──
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # Check auth before accepting connection
    if not ws.session.get("authenticated"):
        await ws.close(code=4001)
        logger.warning(f"Blocked unauthenticated WebSocket from {ws.client.host}")
        return
    await ws.accept()
    ws_clients.append(ws)
    logger.info(f"WS connected ({len(ws_clients)} total)")
    try:
        if collector._last_snapshot:
            data = collector.snapshot_to_dict(collector._last_snapshot)
            data["_server_paused"] = collector.paused
            await ws.send_text(json.dumps(data, ensure_ascii=False))
        while True:
            msg = await ws.receive_text()
            try:
                cmd = json.loads(msg)
                if cmd.get("type") == "set_interval":
                    val = int(cmd.get("value", config.DEFAULT_REFRESH_INTERVAL))
                    client_val = _clamp_refresh_interval(val)
                    logger.debug(f"Client requested ws interval={client_val}s (server cadence unchanged)")
            except (json.JSONDecodeError, ValueError):
                pass
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)
        logger.info(f"WS disconnected ({len(ws_clients)} remaining)")


# ── Server Control ──
@app.post("/api/server/pause")
async def server_pause():
    collector.set_paused(True)
    return {"status": "paused"}

@app.post("/api/server/resume")
async def server_resume():
    collector.set_paused(False)
    return {"status": "running"}

@app.get("/api/server/status")
async def server_status():
    return {"paused": collector.paused, "refresh_interval": refresh_interval, "user_settings": collector._load_user_settings_cached(),
            "clients": len(ws_clients)}

@app.post("/api/server/stop")
async def server_stop():
    """Graceful shutdown."""
    logger.info("Shutdown requested via API")
    asyncio.get_event_loop().call_later(1.0, lambda: os.kill(os.getpid(), signal.SIGTERM))
    return {"status": "shutting_down"}


# ── Snapshot & History ──
@app.get("/api/snapshot")
async def api_snapshot():
    if collector._last_snapshot:
        data = collector.snapshot_to_dict(collector._last_snapshot)
        data["_server_paused"] = collector.paused
        return data
    return {"error": "No data yet"}

@app.get("/api/history/node/{node_name}")
async def api_node_history(node_name: str, points: int = Query(default=0, ge=0),
                           since: float = Query(default=0)):
    data = collector.get_node_history(node_name, last_n=points, since=since)
    return {"node": node_name, "data": data}

@app.get("/api/history/job/{job_id}")
async def api_job_history(job_id: str, points: int = Query(default=0, ge=0),
                          since: float = Query(default=0)):
    data = collector.get_job_history(job_id, last_n=points, since=since)
    return {"job_id": job_id, "data": data}


# ── Job Management ──
@app.get("/api/job/{job_id}")
async def api_job_details(job_id: str):
    details = await collector.get_job_details(job_id)
    if details:
        return details
    return JSONResponse(status_code=404, content={"error": "Job not found"})

@app.post("/api/job/{job_id}/cancel")
async def api_cancel_job(job_id: str):
    result = await collector.cancel_job(job_id)
    return {"status": "ok", "message": result.get("message", "")}

@app.get("/api/history-jobs")
async def api_history_jobs():
    """返回归档的历史任务列表"""
    return {"jobs": collector.get_archived_jobs_list()}

@app.get("/api/log/{job_id}")
async def api_job_log(job_id: str, log_type: str = Query(default="stdout"),
                      lines: int = Query(default=200, ge=1, le=5000)):
    content = await collector.get_job_log(job_id, log_type=log_type, tail=lines)
    if content is not None:
        return {"job_id": job_id, "log_type": log_type, "content": content}
    return JSONResponse(status_code=404, content={"error": "Job log not found"})


@app.get("/api/job/{job_id}/numa")
async def api_job_numa(job_id: str):
    """按需获取作业 NUMA 内存分布分析"""
    result = await collector.get_job_numa_analysis(job_id)
    return JSONResponse(content=result)


# ── File Browser ──
@app.get("/api/files")
async def api_list_files(path: str = Query(default=""),
                         folder_sizes: int = Query(default=0)):
    if not path or not path.strip():
        path = config.FILE_BROWSER_ROOT
    path = await _realpath(path)
    if not await _isdir(path):
        return JSONResponse(status_code=404, content={"error": "目录不存在"})
    if not await _can_read_path(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    entries = await collector.list_directory(path, compute_dir_sizes=bool(folder_sizes))
    return {"path": path, "entries": entries, "allow_write": _within_file_browser_root(path)}


@app.get("/api/file-tree")
async def api_file_tree(path: str = Query(default="/"),
                        limit: int = Query(default=500, ge=1, le=2000)):
    """只读目录树：从 / 开始展开目录，不改变右侧文件操作的安全范围。"""
    path = path if path and path.strip() else "/"
    path = await _realpath(path)
    if not await _isdir(path):
        return JSONResponse(status_code=404, content={"error": "目录不存在", "path": path})
    if not await _can_read_path(path):
        return JSONResponse(status_code=403, content={"error": "Access denied", "path": path})
    result = await collector.list_directory_tree(path, limit=limit)
    result["can_read"] = True
    return JSONResponse(content=result, headers={"Content-Type": "application/json; charset=utf-8"})


@app.get("/api/file-content")
async def api_file_content(path: str = Query(default="")):
    if not path or not path.strip():
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = await _realpath(path)
    if not await _isfile(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    if not await _can_read_path(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    content = await collector.read_file_content(path, max_size=config.MAX_EDIT_FILE_SIZE)
    if content is not None:
        return {"path": path, "content": content, "size": await _getsize(path)}
    return JSONResponse(status_code=404, content={"error": "Cannot read file"})

@app.post("/api/file-save")
async def api_file_save(request: Request):
    body = await request.json()
    path = body.get("path", "")
    content = body.get("content", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = await _realpath(path)
    if not _within_file_browser_root(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    result = await collector.save_file_content(path, content)
    if result["success"]:
        collector.invalidate_directory_cache(os.path.dirname(path))
        return {"status": "ok", "message": result["message"]}
    return JSONResponse(status_code=500, content={"error": result["message"]})

@app.get("/api/file-download")
async def api_file_download(path: str = Query(default="")):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = await _realpath(path)
    if not await _isfile(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    if not await _can_read_path(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    return FileResponse(path, filename=os.path.basename(path))


@app.get("/api/file-preview")
async def api_file_preview(path: str = Query(default="")):
    """Inline, read-only file response for browser previews."""
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = await _realpath(path)
    if not await _isfile(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    if not await _can_read_path(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    media_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
    return FileResponse(
        path,
        filename=os.path.basename(path),
        media_type=media_type,
        content_disposition_type="inline",
    )

@app.get("/api/folder-download")
async def api_folder_download(path: str = Query(default="")):
    """将文件夹打包为zip并下载"""
    if not path:
        return JSONResponse(status_code=400, content={"error": "未提供路径"})
    path = await _realpath(path)
    if not await _isdir(path):
        return JSONResponse(status_code=404, content={"error": "文件夹不存在"})
    if not await _can_read_path(path):
        return JSONResponse(status_code=403, content={"error": "访问被拒绝"})
    folder_name = os.path.basename(path)
    # 在临时目录创建zip文件
    tmp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(tmp_dir, folder_name + ".zip")
    try:
        # 使用异步方式创建zip（在线程池中执行以避免阻塞）
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _create_zip, path, zip_path)
        return FileResponse(
            zip_path,
            filename=folder_name + ".zip",
            media_type="application/zip",
            background=BackgroundTask(shutil.rmtree, tmp_dir, True)
        )
    except Exception as e:
        # 清理临时文件
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return JSONResponse(status_code=500, content={"error": f"打包失败: {e}"})

def _create_zip(folder_path: str, zip_path: str):
    """同步创建zip压缩包"""
    folder_name = os.path.basename(folder_path)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(folder_path):
            for f in files:
                full = os.path.join(root, f)
                arcname = os.path.join(folder_name, os.path.relpath(full, folder_path))
                try:
                    zf.write(full, arcname)
                except (PermissionError, OSError):
                    pass  # 跳过无法读取的文件


def _write_binary_file_sync(path: str, content: bytes):
    with open(path, "wb") as f:
        f.write(content)


def _makedirs_sync(path: str):
    os.makedirs(path, exist_ok=True)


@app.post("/api/file-upload")
async def api_file_upload(file: UploadFile = File(...), dest: str = Form(...)):
    if not dest:
        return JSONResponse(status_code=400, content={"error": "No destination"})
    dest = await _realpath(dest)
    if not _within_file_browser_root(dest):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    target = os.path.join(dest, file.filename) if await _isdir(dest) else dest
    try:
        content = await file.read()
        if len(content) > config.MAX_UPLOAD_SIZE:
            return JSONResponse(status_code=413, content={"error": "File too large"})
        await _run_io(_write_binary_file_sync, target, content)
        collector.invalidate_directory_cache(dest)
        return {"status": "ok", "path": target, "size": len(content)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/file-delete")
async def api_file_delete(request: Request):
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = await _realpath(path)
    if not _within_file_browser_root(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    try:
        if await _isdir(path):
            await _run_io(shutil.rmtree, path)
        elif await _run_io(os.path.exists, path):
            await _run_io(os.remove, path)
        else:
            return JSONResponse(status_code=404, content={"error": "Not found"})
        collector.invalidate_directory_cache(os.path.dirname(path))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/file-mkdir")
async def api_file_mkdir(request: Request):
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = await _realpath(path)
    if not _within_file_browser_root(path):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    try:
        await _run_io(_makedirs_sync, path)
        collector.invalidate_directory_cache(os.path.dirname(path))
        return {"status": "ok", "path": path}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── User Settings API ──
@app.get("/api/settings")
async def api_get_settings():
    return collector._load_user_settings_cached()

@app.post("/api/settings")
async def api_save_settings(request: Request):
    body = await request.json()
    current = collector._load_user_settings_cached()
    # Only update known keys
    for k in config.DEFAULT_USER_SETTINGS:
        if k in body:
            current[k] = body[k]
    # Keep using server-side settings as source of truth; next collector tick will pick it up.
    ok = await _save_user_settings_async(current)
    return {"status": "ok" if ok else "error", "settings": current}


# ── sbatch 提交 ──
@app.post("/api/sbatch")
async def api_sbatch(request: Request):
    """提交sbatch脚本到集群"""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "未提供文件路径"})
    path = await _realpath(path)
    if not _within_file_browser_root(path):
        return JSONResponse(status_code=403, content={"error": "访问被拒绝"})
    if not await _isfile(path):
        return JSONResponse(status_code=404, content={"error": "文件不存在"})
    result = await collector.submit_sbatch(path)
    return JSONResponse(content=result)


# ── bash 脚本运行 ──
@app.post("/api/bash")
async def api_bash(request: Request):
    """在集群上运行bash脚本"""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "未提供文件路径"})
    path = await _realpath(path)
    if not _within_file_browser_root(path):
        return JSONResponse(status_code=403, content={"error": "访问被拒绝"})
    if not await _isfile(path):
        return JSONResponse(status_code=404, content={"error": "文件不存在"})
    result = await collector.run_bash(path)
    return JSONResponse(content=result)


# ── 收藏夹/书签管理 ──
@app.get("/api/bookmarks")
async def api_get_bookmarks():
    """获取收藏列表"""
    settings = collector._load_user_settings_cached()
    return {"bookmarks": settings.get("bookmarks", [])}

@app.post("/api/bookmarks")
async def api_save_bookmarks(request: Request):
    """保存收藏列表"""
    body = await request.json()
    bookmarks = body.get("bookmarks", [])
    settings = collector._load_user_settings_cached()
    settings["bookmarks"] = bookmarks
    ok = await _save_user_settings_async(settings)
    return {"status": "ok" if ok else "error", "bookmarks": bookmarks}


# ── 缓存管理 ──
@app.post("/api/cache/clear")
async def api_clear_cache():
    """清除所有历史缓存"""
    collector.clear_cache()
    return {"status": "ok", "message": "所有缓存已清除"}

@app.get("/api/cache/stats")
async def api_cache_stats():
    """获取缓存统计信息"""
    return collector.get_cache_stats()


@app.get("/api/disk-info")
async def api_disk_info(path: str = Query(default="")):
    """获取指定路径所在分区的磁盘空间信息"""
    result = await collector.get_disk_info(path or config.FILE_BROWSER_ROOT)
    return JSONResponse(content=result)


# ── 登录节点管理 ──
@app.get("/api/login-node/info")
async def api_login_node_info(sort: str = Query(default="cpu_pct"),
                              limit: int = Query(default=50),
                              detail: int = Query(default=-1),
                              exclude_root: int = Query(default=-1)):
    """获取登录节点系统信息和用户进程列表"""
    detailed_commands = None if detail < 0 else bool(detail)
    exclude_root_users = None if exclude_root < 0 else bool(exclude_root)
    result = await collector.get_login_node_info(
        sort_by=sort, limit=limit, detailed_commands=detailed_commands,
        exclude_root=exclude_root_users
    )
    return JSONResponse(content=result, headers={"Content-Type": "application/json; charset=utf-8"})

@app.post("/api/login-node/kill")
async def api_login_node_kill(request: Request):
    """终止登录节点上的指定进程"""
    body = await request.json()
    pid = body.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return JSONResponse({"success": False, "message": "无效的进程ID"}, status_code=400)
    result = await collector.kill_process(pid)
    return JSONResponse(content=result)


if __name__ == "__main__":
    import uvicorn
    import argparse
    parser = argparse.ArgumentParser(description="SLURM Dashboard")
    parser.add_argument("--host", default=config.HOST)
    parser.add_argument("--port", type=int, default=config.PORT)
    args = parser.parse_args()
    if args.port is None:
        parser.error("--port is required unless DASHBOARD_PORT is set")
    config.validate_runtime_config()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
