"""
SLURM Dashboard - FastAPI Application
"""

import asyncio
import json
import logging
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
            stale = []
            for ws in ws_clients:
                try:
                    await ws.send_text(msg)
                except Exception:
                    stale.append(ws)
            for ws in stale:
                if ws in ws_clients:
                    ws_clients.remove(ws)
        except Exception as e:
            logger.error(f"collect_and_broadcast error: {e}", exc_info=True)

async def background_collector():
    global refresh_interval
    while True:
        try:
            if not collector.paused:
                asyncio.create_task(collect_and_broadcast())
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Collection schedule error: {e}", exc_info=True)
        await asyncio.sleep(refresh_interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bg_task, _collect_semaphore
    _collect_semaphore = asyncio.Semaphore(3)  # 在事件循环内创建
    logger.info(f"Starting SLURM Dashboard on {config.HOST}:{config.PORT}")
    bg_task = asyncio.create_task(background_collector())
    yield
    bg_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="SLURM Dashboard", lifespan=lifespan)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


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
    return templates.TemplateResponse("login.html", {
        "request": request,
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
    return templates.TemplateResponse("login.html", {
        "request": request,
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
    return templates.TemplateResponse("index.html", {
        "request": request, "cluster_name": config.CLUSTER_NAME,
        "refresh_interval": refresh_interval, "user_settings": config.load_user_settings(),
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
                    global refresh_interval
                    val = int(cmd.get("value", config.DEFAULT_REFRESH_INTERVAL))
                    refresh_interval = max(config.MIN_REFRESH_INTERVAL, min(config.MAX_REFRESH_INTERVAL, val))
                    logger.info(f"Refresh interval: {refresh_interval}s")
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
    return {"paused": collector.paused, "refresh_interval": refresh_interval, "user_settings": config.load_user_settings(),
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

@app.get("/api/job/{job_id}/numa")
async def api_job_numa(job_id: str):
    """NUMA 内存分布分析"""
    result = await collector.get_job_numa_analysis(job_id)
    return result

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


# ── File Browser ──
@app.get("/api/files")
async def api_list_files(path: str = Query(default=""),
                         folder_sizes: int = Query(default=0)):
    if not path or not path.strip():
        path = config.FILE_BROWSER_ROOT
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    entries = await collector.list_directory(path, compute_dir_sizes=bool(folder_sizes))
    return {"path": path, "entries": entries}

@app.get("/api/file-content")
async def api_file_content(path: str = Query(default="")):
    if not path or not path.strip():
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    content = await collector.read_file_content(path, max_size=config.MAX_EDIT_FILE_SIZE)
    if content is not None:
        return {"path": path, "content": content, "size": os.path.getsize(path)}
    return JSONResponse(status_code=404, content={"error": "Cannot read file"})

@app.post("/api/file-save")
async def api_file_save(request: Request):
    body = await request.json()
    path = body.get("path", "")
    content = body.get("content", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    result = await collector.save_file_content(path, content)
    if result["success"]:
        return {"status": "ok", "message": result["message"]}
    return JSONResponse(status_code=500, content={"error": result["message"]})

@app.get("/api/file-download")
async def api_file_download(path: str = Query(default="")):
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    if not os.path.isfile(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})
    return FileResponse(path, filename=os.path.basename(path))

@app.get("/api/folder-download")
async def api_folder_download(path: str = Query(default="")):
    """将文件夹打包为zip并下载"""
    if not path:
        return JSONResponse(status_code=400, content={"error": "未提供路径"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "访问被拒绝"})
    if not os.path.isdir(path):
        return JSONResponse(status_code=404, content={"error": "文件夹不存在"})
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

@app.post("/api/file-upload")
async def api_file_upload(file: UploadFile = File(...), dest: str = Form(...)):
    if not dest:
        return JSONResponse(status_code=400, content={"error": "No destination"})
    dest = os.path.realpath(dest)
    if not dest.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    target = os.path.join(dest, file.filename) if os.path.isdir(dest) else dest
    try:
        content = await file.read()
        if len(content) > config.MAX_UPLOAD_SIZE:
            return JSONResponse(status_code=413, content={"error": "File too large"})
        with open(target, "wb") as f:
            f.write(content)
        return {"status": "ok", "path": target, "size": len(content)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/file-delete")
async def api_file_delete(request: Request):
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
        else:
            return JSONResponse(status_code=404, content={"error": "Not found"})
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/file-mkdir")
async def api_file_mkdir(request: Request):
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "No path"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "Access denied"})
    try:
        os.makedirs(path, exist_ok=True)
        return {"status": "ok", "path": path}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── User Settings API ──
@app.get("/api/settings")
async def api_get_settings():
    return config.load_user_settings()

@app.post("/api/settings")
async def api_save_settings(request: Request):
    body = await request.json()
    current = config.load_user_settings()
    # Only update known keys
    for k in config.DEFAULT_USER_SETTINGS:
        if k in body:
            current[k] = body[k]
    # Apply refresh interval immediately
    if "refreshIntervalSec" in body:
        global refresh_interval
        val = int(body["refreshIntervalSec"])
        refresh_interval = max(config.MIN_REFRESH_INTERVAL, min(config.MAX_REFRESH_INTERVAL, val))
    ok = config.save_user_settings(current)
    return {"status": "ok" if ok else "error", "settings": current}


# ── sbatch 提交 ──
@app.post("/api/sbatch")
async def api_sbatch(request: Request):
    """提交sbatch脚本到集群"""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return JSONResponse(status_code=400, content={"error": "未提供文件路径"})
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "访问被拒绝"})
    if not os.path.isfile(path):
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
    path = os.path.realpath(path)
    if not path.startswith(config.FILE_BROWSER_ROOT):
        return JSONResponse(status_code=403, content={"error": "访问被拒绝"})
    if not os.path.isfile(path):
        return JSONResponse(status_code=404, content={"error": "文件不存在"})
    result = await collector.run_bash(path)
    return JSONResponse(content=result)


# ── 收藏夹/书签管理 ──
@app.get("/api/bookmarks")
async def api_get_bookmarks():
    """获取收藏列表"""
    settings = config.load_user_settings()
    return {"bookmarks": settings.get("bookmarks", [])}

@app.post("/api/bookmarks")
async def api_save_bookmarks(request: Request):
    """保存收藏列表"""
    body = await request.json()
    bookmarks = body.get("bookmarks", [])
    settings = config.load_user_settings()
    settings["bookmarks"] = bookmarks
    ok = config.save_user_settings(settings)
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


if __name__ == "__main__":
    import uvicorn
    import argparse
    parser = argparse.ArgumentParser(description="SLURM Dashboard")
    parser.add_argument("--host", default=config.HOST)
    parser.add_argument("--port", type=int, default=config.PORT)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
