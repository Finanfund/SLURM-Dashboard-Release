"""
collector.py - SLURM cluster data collection module
Uses SSH + cgroup for accurate CPU/Memory metrics
"""
import asyncio
import glob
import json
import logging
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from collections import deque
from config import SSH_OPTIONS, SSH_TIMEOUT, HISTORY_MAX_POINTS, CACHE_DIR, SCRIPT_DIR

logger = logging.getLogger("collector")

# ── 持久 SSH 连接池（paramiko） ──────────────────────────────────────
try:
    import paramiko
    HAS_PARAMIKO = True
    # 抑制 paramiko 的 verbose 日志（每次连接都输出 Authentication successful 等）
    logging.getLogger("paramiko").setLevel(logging.WARNING)
except ImportError:
    HAS_PARAMIKO = False
    logger.warning("paramiko not installed, falling back to subprocess SSH")


class SSHPool:
    """SSH 持久连接池：复用到各计算节点的 TCP 连接，避免重复握手和 LDAP 查询"""

    def __init__(self, max_workers: int = 20):
        self._conns: Dict[str, 'paramiko.SSHClient'] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # 每节点并发限制：防止多个采集周期同时在同一连接上打开过多通道
        self._node_sems: Dict[str, threading.Semaphore] = {}

    def _get_conn(self, node: str) -> Optional['paramiko.SSHClient']:
        """获取或创建到节点的持久连接"""
        with self._lock:
            client = self._conns.get(node)
            if client:
                transport = client.get_transport()
                if transport and transport.is_active():
                    return client
                # 连接已断开，清理
                try:
                    client.close()
                except Exception:
                    pass
                del self._conns[node]
        # 在锁外创建新连接（避免阻塞其他节点）
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(node, timeout=5, banner_timeout=5, auth_timeout=5)
            with self._lock:
                self._conns[node] = client
            return client
        except Exception as e:
            logger.debug(f"SSH pool: connect to {node} failed: {e}")
            return None

    def exec_cmd(self, node: str, cmd: str, timeout: int = SSH_TIMEOUT) -> Optional[str]:
        """在节点上执行命令（阻塞调用，应在 executor 中使用）"""
        # 节点级并发限制：每节点最多2个并发命令，防止通道饱和
        with self._lock:
            if node not in self._node_sems:
                self._node_sems[node] = threading.Semaphore(2)
        sem = self._node_sems[node]
        if not sem.acquire(timeout=1.0):
            logger.debug(f"SSH pool: {node} channel busy, skipping")
            return None
        try:
            client = self._get_conn(node)
            if not client:
                return None
            try:
                _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
                return stdout.read().decode('utf-8', errors='replace').strip()
            except Exception as e:
                logger.debug(f"SSH pool: exec on {node} failed: {e}")
                # 标记连接为失效
                with self._lock:
                    self._conns.pop(node, None)
                try:
                    client.close()
                except Exception:
                    pass
                return None
        finally:
            sem.release()

    def close_all(self):
        """关闭所有连接"""
        with self._lock:
            for node, client in self._conns.items():
                try:
                    client.close()
                except Exception:
                    pass
            self._conns.clear()

    @property
    def active_count(self) -> int:
        with self._lock:
            return sum(1 for c in self._conns.values()
                       if c.get_transport() and c.get_transport().is_active())

# 全局连接池实例
_ssh_pool = SSHPool() if HAS_PARAMIKO else None


@dataclass
class JobInfo:
    job_id: str = ""
    name: str = ""
    user: str = ""
    state: str = ""
    partition: str = ""
    nodes: str = ""
    num_cpus: int = 0
    num_nodes: int = 0
    time_used: str = ""
    time_limit: str = ""
    submit_time: str = ""
    start_time: str = ""
    work_dir: str = ""
    command: str = ""
    stdout_path: str = ""
    stderr_path: str = ""
    cpu_percent: float = 0.0
    mem_used_gb: float = 0.0
    mem_limit_gb: float = 0.0


@dataclass
class NodeInfo:
    name: str = ""
    state: str = ""
    cpus_total: int = 0
    cpus_alloc: int = 0
    cpus_idle: int = 0
    mem_total_gb: float = 0.0
    mem_used_gb: float = 0.0
    mem_free_gb: float = 0.0
    cpu_percent: float = 0.0
    partitions: str = ""
    jobs: List[str] = field(default_factory=list)


@dataclass
class PartitionInfo:
    name: str = ""
    state: str = ""
    nodes_total: int = 0
    nodes_idle: int = 0
    nodes_alloc: int = 0
    nodes_down: int = 0
    cpus_total: int = 0
    cpus_alloc: int = 0
    timelimit: str = ""
    node_list: List[str] = field(default_factory=list)


@dataclass
class ClusterSnapshot:
    timestamp: float = 0.0
    nodes: Dict[str, NodeInfo] = field(default_factory=dict)
    jobs: Dict[str, JobInfo] = field(default_factory=dict)
    partitions: Dict[str, PartitionInfo] = field(default_factory=dict)


class DataCollector:
    def __init__(self):
        self._prev_cpu_stats: Dict[str, tuple] = {}
        self._prev_job_cpu: Dict[str, tuple] = {}
        self._node_history: Dict[str, deque] = {}
        self._job_history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        self._last_snapshot: Optional[ClusterSnapshot] = None
        self._paused = False
        self._collect_count = 0
        self._last_save_time = 0.0
        # 节点数据缓存：慢节点超时后沿用上次成功采集的数据
        self._last_node_data: Dict[str, dict] = {}
        # 计算后的 metrics 缓存（CPU%/内存等），供 cached 节点直接使用
        self._last_node_metrics: Dict[str, dict] = {}
        self._last_job_metrics: Dict[str, dict] = {}
        # 最近结束的任务跟踪：{job_id: (JobInfo, end_timestamp)}
        self._recently_finished: Dict[str, tuple] = {}
        # 归档任务（持久化跟踪指定用户的所有历史任务）：{job_id: (JobInfo, end_timestamp)}
        self._archived_jobs: Dict[str, tuple] = {}
        # 实时 stdout/stderr 采集缓存：{job_id: {"stdout": str, "stderr": str, "ts": float}}
        self._job_log_cache: Dict[str, dict] = {}
        # 任务日志文件路径缓存（避免重复 scontrol 查询）：{job_id: {"stdout": path, "stderr": path}}
        self._job_log_paths: Dict[str, dict] = {}
        # NUMA 内存分布缓存：{job_id: {"local_mb": float, "remote_mb": float, "per_node": [...], "ts": float}}
        self._job_numa_cache: Dict[str, dict] = {}
        # NUMA 拓扑缓存（每节点只查一次）：{node_name: {nid: cpu_range_str}}
        self._numa_topo_cache: Dict[str, dict] = {}
        # SSH退化检测：连续全缓存周期计数
        self._consecutive_all_cached: int = 0
        os.makedirs(CACHE_DIR, exist_ok=True)
        self._load_cache()
        self._load_archived_jobs()

    @property
    def paused(self):
        return self._paused

    def set_paused(self, val: bool):
        self._paused = val

    async def _run_cmd(self, cmd: str, timeout: int = 10) -> Optional[str]:
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode == 0:
                return stdout.decode("utf-8", errors="replace").strip()
            else:
                logger.debug(f"Command failed: {cmd[:80]}... rc={proc.returncode}")
                return None
        except asyncio.TimeoutError:
            logger.warning(f"Command timeout: {cmd[:80]}...")
            try:
                proc.kill()
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"Command error: {e}")
            return None

    async def _ssh_cmd(self, node: str, remote_cmd: str) -> Optional[str]:
        ssh_args = ["ssh"] + list(SSH_OPTIONS) + [node, remote_cmd]
        try:
            proc = await asyncio.create_subprocess_exec(
                *ssh_args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=SSH_TIMEOUT)
            if proc.returncode == 0:
                return stdout.decode("utf-8", errors="replace").strip()
            else:
                logger.debug(f"SSH to {node} failed: rc={proc.returncode}")
                return None
        except asyncio.TimeoutError:
            logger.warning(f"SSH to {node} timeout")
            try:
                proc.kill()
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"SSH to {node} error: {e}")
            return None

    async def _get_slurm_nodes(self) -> Dict[str, NodeInfo]:
        out = await self._run_cmd("sinfo -N -h -o '%N|%T|%C|%m|%e|%P' 2>/dev/null")
        nodes = {}
        if not out:
            return nodes
        for line in out.strip().split("\n"):
            parts = line.strip().split("|")
            if len(parts) < 6:
                continue
            name = parts[0].strip()
            state = parts[1].strip()
            cpus = parts[2].strip()
            mem_total = parts[3].strip()
            mem_free = parts[4].strip()
            partition = parts[5].strip()
            try:
                cpu_parts = cpus.split("/")
                cpus_alloc = int(cpu_parts[0])
                cpus_idle = int(cpu_parts[1])
                cpus_total = int(cpu_parts[3]) if len(cpu_parts) > 3 else cpus_alloc + cpus_idle
            except (ValueError, IndexError):
                cpus_alloc = cpus_idle = cpus_total = 0
            try:
                mt = float(mem_total) / 1024.0
            except ValueError:
                mt = 0.0
            try:
                mf = float(mem_free) / 1024.0
            except ValueError:
                mf = 0.0
            if name in nodes:
                nodes[name].partitions += "," + partition
            else:
                nodes[name] = NodeInfo(
                    name=name, state=state,
                    cpus_total=cpus_total, cpus_alloc=cpus_alloc, cpus_idle=cpus_idle,
                    mem_total_gb=round(mt, 2), mem_used_gb=round(mt - mf, 2), mem_free_gb=round(mf, 2),
                    partitions=partition
                )
        return nodes

    async def _get_slurm_jobs(self) -> Dict[str, JobInfo]:
        fmt = "%i|%j|%u|%T|%P|%N|%C|%D|%M|%l|%V|%S|%Z|%o"
        out = await self._run_cmd(f"squeue -h -o '{fmt}' 2>/dev/null")
        jobs = {}
        if not out:
            return jobs
        for line in out.strip().split("\n"):
            parts = line.strip().split("|")
            if len(parts) < 14:
                continue
            jid = parts[0].strip()
            jobs[jid] = JobInfo(
                job_id=jid, name=parts[1].strip(), user=parts[2].strip(),
                state=parts[3].strip(), partition=parts[4].strip(),
                nodes=parts[5].strip(),
                num_cpus=int(parts[6]) if parts[6].strip().isdigit() else 0,
                num_nodes=int(parts[7]) if parts[7].strip().isdigit() else 0,
                time_used=parts[8].strip(), time_limit=parts[9].strip(),
                submit_time=parts[10].strip(), start_time=parts[11].strip(),
                work_dir=parts[12].strip(), command=parts[13].strip(),
                # stdout_path/stderr_path 通过 scontrol 获取，不从 squeue 解析
            )
        return jobs

    async def _get_partitions(self) -> Dict[str, PartitionInfo]:
        out = await self._run_cmd("sinfo -h -o '%P|%a|%D|%F|%C|%l|%N' 2>/dev/null")
        partitions = {}
        if not out:
            return partitions
        for line in out.strip().split("\n"):
            parts = line.strip().split("|")
            if len(parts) < 6:
                continue
            name = parts[0].strip().rstrip("*")
            try:
                node_states = parts[3].split("/")
                n_alloc = int(node_states[0])
                n_idle = int(node_states[1])
                n_other = int(node_states[2])
                n_total = int(node_states[3])
            except (ValueError, IndexError):
                n_alloc = n_idle = n_other = n_total = 0
            try:
                cpu_states = parts[4].split("/")
                c_alloc = int(cpu_states[0])
                c_total = int(cpu_states[3]) if len(cpu_states) > 3 else 0
            except (ValueError, IndexError):
                c_alloc = c_total = 0
            node_list_str = parts[6].strip() if len(parts) > 6 else ""
            node_list = self._expand_nodelist(node_list_str) if node_list_str else []
            partitions[name] = PartitionInfo(
                name=name, state=parts[1].strip(),
                nodes_total=n_total, nodes_idle=n_idle,
                nodes_alloc=n_alloc, nodes_down=n_other,
                cpus_total=c_total, cpus_alloc=c_alloc,
                timelimit=parts[5].strip(),
                node_list=node_list
            )
        return partitions

    async def _get_node_realtime(self, node_name: str) -> Optional[dict]:
        remote_cmd = (
            "head -1 /proc/stat;"
            "echo '===SEP===';"
            "grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached:)' /proc/meminfo;"
            "echo '===SEP===';"
            "for d in /sys/fs/cgroup/cpuacct/slurm_$(hostname -s)/uid_*/job_*/; do "
            '[ -d "$d" ] || continue; '
            "jid=$(basename $d); jid=${jid#job_}; "
            "cpu=$(cat ${d}cpuacct.usage 2>/dev/null || echo -1); "
            "memd=${d/cpuacct/memory}; "
            "mem=$(cat ${memd}memory.usage_in_bytes 2>/dev/null || echo -1); "
            'echo "JOB:${jid}:${cpu}:${mem}"; '
            "done"
        )
        out = await self._ssh_cmd(node_name, remote_cmd)
        if not out:
            return None
        sections = out.split("===SEP===")
        if len(sections) < 3:
            return None
        result = {"cpu_stat": sections[0].strip(), "meminfo": sections[1].strip(), "job_cgroups": {}}
        for line in sections[2].strip().split("\n"):
            line = line.strip()
            if not line.startswith("JOB:"):
                continue
            parts = line.split(":")
            if len(parts) >= 4:
                jid = parts[1]
                try:
                    cpu_ns = int(parts[2])
                    mem_bytes = int(parts[3])
                    result["job_cgroups"][jid] = {"cpu_ns": cpu_ns, "mem_bytes": mem_bytes}
                except ValueError:
                    pass
        return result

    def _parse_proc_stat(self, stat_line: str) -> Optional[tuple]:
        parts = stat_line.split()
        if len(parts) < 8 or parts[0] != "cpu":
            return None
        try:
            values = [int(x) for x in parts[1:8]]
            idle = values[3] + values[4]
            total = sum(values)
            return (idle, total)
        except ValueError:
            return None

    def _parse_meminfo(self, meminfo_text: str) -> dict:
        result = {}
        for line in meminfo_text.split("\n"):
            parts = line.split()
            if len(parts) >= 2:
                key = parts[0].rstrip(":")
                try:
                    result[key] = int(parts[1])
                except ValueError:
                    pass
        return result

    def _compute_node_cpu_pct(self, node_name: str, stat_line: str) -> float:
        current = self._parse_proc_stat(stat_line)
        if not current:
            logger.warning(f"CPU: {node_name} stat parse failed: '{stat_line[:80]}'")
            return -1.0  # 返回 -1 表示解析失败（区别于计算出的 0%）
        prev = self._prev_cpu_stats.get(node_name)
        self._prev_cpu_stats[node_name] = current
        if not prev:
            return -1.0  # 首次无 prev，返回 -1
        idle_diff = current[0] - prev[0]
        total_diff = current[1] - prev[1]
        if total_diff <= 0:
            logger.warning(f"CPU: {node_name} total_diff={total_diff} idle_diff={idle_diff} "
                           f"cur={current} prev={prev}")
            return -1.0
        pct = round((1.0 - idle_diff / total_diff) * 100.0, 2)
        if pct < 0:
            logger.warning(f"CPU: {node_name} negative pct={pct}, clamping to 0")
            return 0.0
        return pct

    def _compute_job_cpu_pct(self, job_id: str, cpu_ns: int, node_cpus: int) -> float:
        now = time.time()
        prev = self._prev_job_cpu.get(job_id)
        self._prev_job_cpu[job_id] = (cpu_ns, now)
        if not prev or node_cpus <= 0:
            return -1.0  # 首次无 prev
        dt = now - prev[1]
        if dt < 0.5:
            return -1.0  # 间隔太短
        dns = cpu_ns - prev[0]
        pct = (dns / (dt * 1e9)) * 100.0
        return round(min(pct, node_cpus * 100.0), 2)

    def _update_history(self, snapshot: ClusterSnapshot):
        ts = snapshot.timestamp
        for name, node in snapshot.nodes.items():
            if name not in self._node_history:
                self._node_history[name] = deque(maxlen=HISTORY_MAX_POINTS)
            # 活跃节点 CPU=0% 时沿用上一个有效值，避免图表出现尖刺
            cpu_val = node.cpu_percent
            if cpu_val == 0 and node.jobs and self._node_history[name]:
                cpu_val = self._node_history[name][-1].get("cpu", 0)
            self._node_history[name].append({
                "t": ts, "cpu": cpu_val,
                "mem_used": node.mem_used_gb, "mem_total": node.mem_total_gb
            })
        for jid, job in snapshot.jobs.items():
            if job.state != "RUNNING":
                continue
            if jid not in self._job_history:
                self._job_history[jid] = deque(maxlen=HISTORY_MAX_POINTS)
            # 同样对 job CPU 做保护
            job_cpu = job.cpu_percent
            if job_cpu == 0 and self._job_history.get(jid):
                job_cpu = self._job_history[jid][-1].get("cpu", 0)
            point = {
                "t": ts, "cpu": job_cpu, "mem": job.mem_used_gb,
                "num_cpus": job.num_cpus
            }
            # 如果有 NUMA 缓存数据，按比例拆分 cgroup 内存值（确保总量一致，无图表跳变）
            numa = self._job_numa_cache.get(jid)
            if numa:
                numa_total_mb = numa["local_mb"] + numa["remote_mb"]
                if numa_total_mb > 0:
                    local_ratio = numa["local_mb"] / numa_total_mb
                    point["numa_local"] = round(job.mem_used_gb * local_ratio, 3)
                    point["numa_remote"] = round(job.mem_used_gb * (1 - local_ratio), 3)
            self._job_history[jid].append(point)

    def _save_cache(self):
        """增量保存历史数据到批次文件"""
        try:
            now = time.time()
            batch_data = {"node_history": {}, "job_history": {}, "timestamp": now}
            for k, v in self._node_history.items():
                new_points = [p for p in v if p["t"] > self._last_save_time]
                if new_points:
                    batch_data["node_history"][k] = new_points
            for k, v in self._job_history.items():
                new_points = [p for p in v if p["t"] > self._last_save_time]
                if new_points:
                    batch_data["job_history"][k] = new_points
            if batch_data["node_history"] or batch_data["job_history"]:
                filename = f"cache_{int(now)}.json"
                path = os.path.join(CACHE_DIR, filename)
                with open(path, "w") as f:
                    json.dump(batch_data, f)
                logger.debug(f"Cache batch saved: {filename}")
            self._last_save_time = now
            self._cleanup_cache_files()
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def _load_cache(self):
        """启动时加载所有批次缓存文件并合并"""
        files = sorted(glob.glob(os.path.join(CACHE_DIR, "cache_*.json")))
        if not files:
            # 尝试加载旧版单文件缓存
            old_path = os.path.join(CACHE_DIR, "history_cache.json")
            if os.path.exists(old_path):
                files = [old_path]
        loaded = 0
        for filepath in files:
            try:
                with open(filepath, "r") as f:
                    batch = json.load(f)
                for name, points in batch.get("node_history", {}).items():
                    if name not in self._node_history:
                        self._node_history[name] = deque(maxlen=HISTORY_MAX_POINTS)
                    self._node_history[name].extend(points)
                for jid, points in batch.get("job_history", {}).items():
                    if jid not in self._job_history:
                        self._job_history[jid] = deque(maxlen=HISTORY_MAX_POINTS)
                    self._job_history[jid].extend(points)
                loaded += 1
            except Exception as e:
                logger.warning(f"Failed to load cache file {filepath}: {e}")
        if loaded > 0:
            logger.info(f"Loaded {loaded} cache batch files")
            self._last_save_time = time.time()

    def _load_archived_jobs(self):
        """从磁盘加载归档任务列表"""
        archive_path = os.path.join(CACHE_DIR, "archived_jobs.json")
        if os.path.exists(archive_path):
            try:
                with open(archive_path, "r") as f:
                    data = json.load(f)
                for jid, entry in data.items():
                    ji = JobInfo(**{k: v for k, v in entry["job"].items() if k in JobInfo.__dataclass_fields__})
                    self._archived_jobs[jid] = (ji, entry["end_time"])
                logger.info(f"Loaded {len(self._archived_jobs)} archived jobs from disk")
            except Exception as e:
                logger.warning(f"Failed to load archived jobs: {e}")

    def _save_archived_jobs(self):
        """保存归档任务列表到磁盘"""
        try:
            archive_path = os.path.join(CACHE_DIR, "archived_jobs.json")
            data = {}
            for jid, (ji, end_t) in self._archived_jobs.items():
                data[jid] = {
                    "job": {
                        "job_id": ji.job_id, "name": ji.name, "user": ji.user,
                        "state": ji.state, "partition": ji.partition, "nodes": ji.nodes,
                        "num_cpus": ji.num_cpus, "num_nodes": ji.num_nodes,
                        "time_used": ji.time_used, "time_limit": ji.time_limit,
                        "submit_time": ji.submit_time, "start_time": ji.start_time,
                        "work_dir": ji.work_dir, "command": ji.command,
                        "stdout_path": ji.stdout_path, "stderr_path": ji.stderr_path,
                        "cpu_percent": ji.cpu_percent, "mem_used_gb": ji.mem_used_gb,
                        "mem_limit_gb": ji.mem_limit_gb,
                    },
                    "end_time": end_t
                }
            with open(archive_path, "w") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save archived jobs: {e}")

    def get_archived_jobs_list(self) -> list:
        """返回归档任务列表（用于前端历史任务栏目）"""
        result = []
        # 包含归档任务（已结束的）
        for jid, (ji, end_t) in self._archived_jobs.items():
            d = self._job_to_dict(ji)
            d["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_t))
            d["end_timestamp"] = end_t
            result.append(d)
        # 包含当前正在运行且被追踪的用户的任务
        if self._last_snapshot:
            try:
                with open("user_settings.json", "r") as f:
                    _us = json.load(f)
                    track_users = set(u.strip() for u in _us.get("historyTrackUsers", "zzr").split(",") if u.strip())
            except Exception:
                track_users = {"zzr"}
            for jid, ji in self._last_snapshot.jobs.items():
                if ji.user in track_users and ji.state == "RUNNING" and jid not in self._archived_jobs:
                    d = self._job_to_dict(ji)
                    d["end_time"] = ""  # 仍在运行
                    d["end_timestamp"] = 0
                    result.append(d)
        result.sort(key=lambda x: x.get("end_timestamp", 0), reverse=True)
        return result

    def _cleanup_cache_files(self):
        """根据MB限制或日期保留策略清理旧缓存文件"""
        try:
            from config import load_user_settings
            settings = load_user_settings()
            max_mb = settings.get("maxCacheMB", 100)
            retain_date_str = settings.get("cacheRetainDate", "")
            files = sorted(glob.glob(os.path.join(CACHE_DIR, "cache_*.json")))
            if not files:
                return
            if max_mb > 0:
                max_bytes = max_mb * 1024 * 1024
                total_size = sum(os.path.getsize(f) for f in files)
                while total_size > max_bytes and len(files) > 1:
                    removed_size = os.path.getsize(files[0])
                    os.remove(files[0])
                    files.pop(0)
                    total_size -= removed_size
            elif retain_date_str:
                try:
                    retain_ts = time.mktime(time.strptime(retain_date_str, "%Y-%m-%d"))
                    for f in list(files):
                        basename = os.path.basename(f)
                        try:
                            ts = int(basename.split("_")[1].split(".")[0])
                            if ts < retain_ts:
                                os.remove(f)
                        except (ValueError, IndexError):
                            pass
                except ValueError:
                    pass
        except Exception as e:
            logger.warning(f"Cache cleanup error: {e}")

    async def _get_all_nodes_realtime_batch(self, node_names: list) -> Dict[str, dict]:
        """单子进程批量并行 SSH 查询所有节点（利用 ControlMaster 连接复用，比 asyncio 多进程快 ~10 倍）"""
        if not node_names:
            return {}
        # 确保采集脚本存在于共享文件系统
        script_path = os.path.join(SCRIPT_DIR, "_node_collect.sh")
        if not os.path.exists(script_path):
            with open(script_path, "w") as f:
                f.write('#!/bin/bash\n'
                        'head -1 /proc/stat\n'
                        'echo ===SEP===\n'
                        'grep -E "^(MemTotal|MemFree|MemAvailable|Buffers|Cached:)" /proc/meminfo\n'
                        'echo ===SEP===\n'
                        'for d in /sys/fs/cgroup/cpuacct/slurm_$(hostname -s)/uid_*/job_*/; do\n'
                        '  [ -d "$d" ] || continue\n'
                        '  jid=$(basename "$d"); jid=${jid#job_}\n'
                        '  cpu=$(cat "${d}cpuacct.usage" 2>/dev/null || echo -1)\n'
                        '  memd=${d/cpuacct/memory}\n'
                        '  mem=$(cat "${memd}memory.usage_in_bytes" 2>/dev/null || echo -1)\n'
                        '  echo "JOB:${jid}:${cpu}:${mem}"\n'
                        'done\n')
            os.chmod(script_path, 0o755)
        # 构建 bash 并行 SSH 命令（每个节点 SSH 在后台运行，sed 添加节点前缀）
        ssh_parts = []
        for node in sorted(node_names):
            ssh_parts.append(
                f'ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o BatchMode=yes '
                f'{node} "bash {script_path}" 2>/dev/null | sed "s/^/{node}: /" &'
            )
        bash_cmd = "\n".join(ssh_parts) + "\nwait"
        t0 = time.time()
        try:
            proc = await asyncio.create_subprocess_shell(
                f'bash -c \'{bash_cmd}\'',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=SSH_TIMEOUT + 5)
            out = stdout.decode("utf-8", errors="replace").strip()
        except asyncio.TimeoutError:
            logger.warning("batch SSH query timeout")
            return {}
        except Exception as e:
            logger.warning(f"batch SSH error: {e}")
            return {}
        elapsed = time.time() - t0
        if not out:
            logger.info(f"batch SSH returned empty output ({elapsed:.2f}s)")
            return {}
        # 按节点名分组输出（格式: "node_name: line_content"）
        node_lines: Dict[str, list] = {}
        for line in out.split("\n"):
            if ": " not in line:
                continue
            node, content = line.split(": ", 1)
            node = node.strip()
            if node not in node_lines:
                node_lines[node] = []
            node_lines[node].append(content)
        # 解析每个节点的数据
        results = {}
        for node_name, lines in node_lines.items():
            raw = "\n".join(lines)
            sections = raw.split("===SEP===")
            if len(sections) < 3:
                continue
            result = {"cpu_stat": sections[0].strip(), "meminfo": sections[1].strip(), "job_cgroups": {}}
            for jline in sections[2].strip().split("\n"):
                jline = jline.strip()
                if not jline.startswith("JOB:"):
                    continue
                parts = jline.split(":")
                if len(parts) >= 4:
                    jid = parts[1]
                    try:
                        cpu_ns = int(parts[2])
                        mem_bytes = int(parts[3])
                        result["job_cgroups"][jid] = {"cpu_ns": cpu_ns, "mem_bytes": mem_bytes}
                    except ValueError:
                        pass
            results[node_name] = result
        logger.info(f"batch SSH: {len(results)}/{len(node_names)} nodes in {elapsed:.2f}s")
        return results

    def _ensure_collect_script(self) -> str:
        """确保采集脚本存在于共享文件系统，返回脚本路径"""
        script_path = os.path.join(SCRIPT_DIR, "_node_collect.sh")
        if not os.path.exists(script_path):
            with open(script_path, "w") as f:
                f.write('#!/bin/bash\n'
                        'head -1 /proc/stat\n'
                        'echo ===SEP===\n'
                        'grep -E "^(MemTotal|MemFree|MemAvailable|Buffers|Cached:)" /proc/meminfo\n'
                        'echo ===SEP===\n'
                        'for d in /sys/fs/cgroup/cpuacct/slurm_$(hostname -s)/uid_*/job_*/; do\n'
                        '  [ -d "$d" ] || continue\n'
                        '  jid=$(basename "$d"); jid=${jid#job_}\n'
                        '  cpu=$(cat "${d}cpuacct.usage" 2>/dev/null || echo -1)\n'
                        '  memd=${d/cpuacct/memory}\n'
                        '  mem=$(cat "${memd}memory.usage_in_bytes" 2>/dev/null || echo -1)\n'
                        '  echo "JOB:${jid}:${cpu}:${mem}"\n'
                        'done\n')
            os.chmod(script_path, 0o755)
        return script_path

    def _parse_realtime_output(self, out: str) -> Optional[dict]:
        """解析节点采集脚本输出为结构化数据"""
        sections = out.split("===SEP===")
        if len(sections) < 3:
            return None
        result = {"cpu_stat": sections[0].strip(), "meminfo": sections[1].strip(), "job_cgroups": {}}
        for line in sections[2].strip().split("\n"):
            line = line.strip()
            if not line.startswith("JOB:"):
                continue
            parts = line.split(":")
            if len(parts) >= 4:
                jid = parts[1]
                try:
                    cpu_ns = int(parts[2])
                    mem_bytes = int(parts[3])
                    result["job_cgroups"][jid] = {"cpu_ns": cpu_ns, "mem_bytes": mem_bytes}
                except ValueError:
                    pass
        return result

    async def _get_all_nodes_realtime_paramiko(self, node_names: list) -> Dict[str, dict]:
        """
        使用 paramiko 持久连接池并行采集所有节点数据。
        异步缓存回填策略：
        - 快速节点（<1.5s）：立即返回新鲜数据并更新缓存
        - 慢节点（>1.5s）：先沿用上次缓存数据返回，同时后台继续等待
        - 后台任务完成时：自动更新缓存，下个周期使用新数据
        """
        if not HAS_PARAMIKO or not _ssh_pool:
            return {}
        script_path = self._ensure_collect_script()
        cmd = f"bash {script_path}"
        loop = asyncio.get_event_loop()
        t0 = time.time()

        # 并行提交所有节点到线程池
        node_futures = {}
        for node in node_names:
            future = loop.run_in_executor(_ssh_pool._executor, _ssh_pool.exec_cmd, node, cmd)
            task = asyncio.ensure_future(future)
            node_futures[task] = node

        # 等待最多 BATCH_TIMEOUT：大多数节点 0.1-0.2s 完成
        BATCH_TIMEOUT = 1.5
        done, pending = await asyncio.wait(node_futures.keys(), timeout=BATCH_TIMEOUT)

        # ── 处理已完成的节点：解析数据并更新缓存 ──
        results = {}
        fresh_count = 0
        failed_fresh = []  # 完成但数据无效的节点（连接失败/数据截断）
        for task in done:
            node = node_futures[task]
            try:
                out = task.result()
                if out:
                    parsed = self._parse_realtime_output(out)
                    if parsed:
                        results[node] = parsed
                        self._last_node_data[node] = parsed  # 更新缓存
                        fresh_count += 1
                    else:
                        failed_fresh.append(node)  # 数据截断，解析失败
                else:
                    failed_fresh.append(node)  # exec_cmd 返回 None（连接失败）
            except Exception:
                failed_fresh.append(node)

        # ── 处理超时的慢节点 + 失败的快节点：统一沿用缓存 ──
        cached_nodes = []
        pending_nodes = []
        # 先处理 pending（超时节点）
        for task in pending:
            node = node_futures[task]
            pending_nodes.append(node)
            if node in self._last_node_data:
                cached_copy = dict(self._last_node_data[node])
                cached_copy["_cached"] = True
                results[node] = cached_copy
                cached_nodes.append(node)
        # 再处理 failed_fresh（连接失败/数据截断的节点）→ 也用缓存补救
        for node in failed_fresh:
            if node not in results:
                if node in self._last_node_data:
                    cached_copy = dict(self._last_node_data[node])
                    cached_copy["_cached"] = True
                    results[node] = cached_copy
                    cached_nodes.append(node)
                    logger.debug(f"paramiko: {node} exec failed, using cached data")
                else:
                    pending_nodes.append(node)  # 真正无数据

        # 后台任务：继续等待慢节点，完成后自动更新缓存（不阻塞当前采集）
        if pending:
            asyncio.ensure_future(self._backfill_slow_nodes(pending, node_futures))

        elapsed = time.time() - t0
        parts = []
        parts.append(f"{fresh_count} fresh")
        if cached_nodes:
            parts.append(f"{len(cached_nodes)} cached({','.join(sorted(cached_nodes))})")
        no_data_nodes = [n for n in pending_nodes if n not in self._last_node_data and n not in results]
        if no_data_nodes:
            parts.append(f"{len(no_data_nodes)} no-data({','.join(sorted(no_data_nodes))})")
        if failed_fresh:
            parts.append(f"{len(failed_fresh)} retry({','.join(sorted(failed_fresh))})")
        detail = ", ".join(parts)
        logger.info(f"paramiko pool: {len(results)}/{len(node_names)} nodes in {elapsed:.2f}s "
                     f"[{detail}] (active: {_ssh_pool.active_count})")

        # SSH退化检测：连续全缓存时发出警告
        if fresh_count == 0 and cached_nodes and len(cached_nodes) >= len(node_names):
            self._consecutive_all_cached += 1
            if self._consecutive_all_cached >= 5 and self._consecutive_all_cached % 10 == 5:
                logger.warning(f"SSH degraded: {self._consecutive_all_cached} consecutive all-cached cycles, "
                               "auto-throttling active. Consider increasing refreshIntervalSec.")
        else:
            if self._consecutive_all_cached > 0:
                logger.info(f"SSH recovered after {self._consecutive_all_cached} all-cached cycles")
            self._consecutive_all_cached = 0

        return results

    async def _backfill_slow_nodes(self, pending_tasks, node_futures):
        """
        后台等待慢节点完成，回填缓存并计算 metrics。
        - 更新 _last_node_data（原始数据）
        - 调用 _compute_node_cpu_pct 设置 prev 并计算 CPU delta
        - 计算内存 metrics
        - 保存到 _last_node_metrics，供下次 cached 路径使用
        """
        try:
            done2, _ = await asyncio.wait(pending_tasks, timeout=SSH_TIMEOUT)
            for task in done2:
                node = node_futures[task]
                try:
                    out = task.result()
                    if out:
                        parsed = self._parse_realtime_output(out)
                        if parsed:
                            self._last_node_data[node] = parsed
                            # 计算 CPU delta（同时更新 _prev_cpu_stats）
                            cpu_pct = self._compute_node_cpu_pct(node, parsed["cpu_stat"])
                            # 计算内存
                            mi = self._parse_meminfo(parsed["meminfo"])
                            mem_metrics = {}
                            if mi:
                                total_kb = mi.get("MemTotal", 0)
                                free_kb = mi.get("MemFree", 0)
                                buffers_kb = mi.get("Buffers", 0)
                                cached_kb = mi.get("Cached", 0)
                                used_kb = total_kb - free_kb - buffers_kb - cached_kb
                                mem_metrics = {
                                    "mem_total_gb": round(total_kb / 1048576.0, 2),
                                    "mem_used_gb": round(max(0, used_kb) / 1048576.0, 2),
                                    "mem_free_gb": round((total_kb - max(0, used_kb)) / 1048576.0, 2),
                                }
                            # 保存 metrics（保留上次有效的 CPU%，避免首次 delta 无效覆盖）
                            old_metrics = self._last_node_metrics.get(node, {})
                            new_cpu = cpu_pct if cpu_pct >= 0 else old_metrics.get("cpu_percent", 0)
                            self._last_node_metrics[node] = {
                                "cpu_percent": new_cpu,
                                **mem_metrics,
                            }
                            # 计算 job metrics
                            for jid, cg in parsed.get("job_cgroups", {}).items():
                                if cg["cpu_ns"] >= 0:
                                    node_cpus = 0
                                    if self._last_snapshot and node in self._last_snapshot.nodes:
                                        node_cpus = self._last_snapshot.nodes[node].cpus_total
                                    if node_cpus > 0:
                                        job_cpu = self._compute_job_cpu_pct(jid, cg["cpu_ns"], node_cpus)
                                    else:
                                        job_cpu = -1
                                else:
                                    job_cpu = -1
                                job_mem = round(cg["mem_bytes"] / (1024**3), 3) if cg["mem_bytes"] >= 0 else 0
                                old_job = self._last_job_metrics.get(jid, {})
                                self._last_job_metrics[jid] = {
                                    "cpu_percent": job_cpu if job_cpu >= 0 else old_job.get("cpu_percent", 0),
                                    "mem_used_gb": job_mem if job_mem > 0 else old_job.get("mem_used_gb", 0),
                                }
                            logger.debug(f"backfill: {node} metrics updated (cpu={new_cpu:.1f}%)")
                except Exception as e:
                    logger.debug(f"backfill {node} error: {e}")
        except Exception as e:
            logger.debug(f"backfill error: {e}")

    async def collect(self) -> ClusterSnapshot:
        """采集集群数据，I/O阶段无锁（支持流水线并发），状态更新阶段加锁"""
        if self._paused:
            return self._last_snapshot or ClusterSnapshot(timestamp=time.time())

        # Phase 1: I/O 操作（无锁，可并发）
        nodes_task = self._get_slurm_nodes()
        jobs_task = self._get_slurm_jobs()
        parts_task = self._get_partitions()
        nodes, jobs, partitions = await asyncio.gather(nodes_task, jobs_task, parts_task)

        running_nodes = set()
        for jid, job in jobs.items():
            if job.state == "RUNNING" and job.nodes:
                expanded = self._expand_nodelist(job.nodes)
                for n in expanded:
                    running_nodes.add(n)
                    if n in nodes:
                        nodes[n].jobs.append(jid)

        # 获取节点实时数据：SSH 到所有非 down 节点（包括 idle 节点）以获取准确内存数据
        # 注意：SLURM sinfo 的 free_mem 等于 /proc/meminfo 的 MemFree，不扣除 buffers/caches，
        # 导致 idle 节点也可能显示很高的"已用"内存（实际大部分是可回收的磁盘缓存）。
        # 因此必须 SSH 到所有节点，通过 /proc/meminfo 读取 MemFree+Buffers+Cached 来正确计算。
        query_nodes = [n for n in nodes if "down" not in nodes[n].state.lower()]
        rt_results = {}
        if query_nodes:
            # 方案 1: paramiko 持久连接池（最快，无进程创建开销）
            if HAS_PARAMIKO:
                rt_results = await self._get_all_nodes_realtime_paramiko(query_nodes)
            # 方案 2: 单子进程 bash 并行 SSH
            if not rt_results:
                rt_results = await self._get_all_nodes_realtime_batch(query_nodes)
            # 方案 3: asyncio 逐节点 SSH（最慢但最稳定）
            if not rt_results:
                logger.info("All batch methods failed, falling back to per-node SSH")
                realtime_tasks = {n: self._get_node_realtime(n) for n in query_nodes}
                results = await asyncio.gather(*realtime_tasks.values(), return_exceptions=True)
                for node_name, result in zip(realtime_tasks.keys(), results):
                    if not isinstance(result, Exception) and result is not None:
                        rt_results[node_name] = result

        # Phase 2: 状态更新（加锁保证一致性）
        async with self._lock:
            for node_name, result in rt_results.items():
                if node_name not in nodes:
                    continue
                node = nodes[node_name]

                if result.get("_cached"):
                    # ── 缓存数据：直接沿用上次计算的 metrics，不做 delta 计算 ──
                    saved = self._last_node_metrics.get(node_name, {})
                    if saved:
                        node.cpu_percent = saved.get("cpu_percent", 0)
                        node.mem_total_gb = saved.get("mem_total_gb", 0)
                        node.mem_used_gb = saved.get("mem_used_gb", 0)
                        node.mem_free_gb = saved.get("mem_free_gb", 0)
                    # 沿用缓存的 job metrics
                    for jid in node.jobs:
                        if jid in jobs:
                            saved_job = self._last_job_metrics.get(jid, {})
                            if saved_job:
                                jobs[jid].cpu_percent = saved_job.get("cpu_percent", 0)
                                jobs[jid].mem_used_gb = saved_job.get("mem_used_gb", 0)
                            else:
                                # 冷启动兜底：单 job 节点用节点级内存近似
                                if len(node.jobs) == 1 and node.mem_used_gb > 0:
                                    jobs[jid].mem_used_gb = node.mem_used_gb
                else:
                    # ── 新鲜数据：正常 delta 计算 ──
                    cpu_pct = self._compute_node_cpu_pct(node_name, result["cpu_stat"])
                    if cpu_pct >= 0:
                        # 有效计算结果（包括真正的 0% 空闲节点）
                        node.cpu_percent = cpu_pct
                    elif cpu_pct < 0:
                        # -1 表示计算失败（解析错误/无 prev/delta 异常）
                        # 沿用上次已知的有效 CPU%
                        fallback_cpu = self._last_node_metrics.get(node_name, {}).get("cpu_percent", 0)
                        if fallback_cpu > 0:
                            node.cpu_percent = fallback_cpu
                    mi = self._parse_meminfo(result["meminfo"])
                    if mi:
                        total_kb = mi.get("MemTotal", 0)
                        free_kb = mi.get("MemFree", 0)
                        buffers_kb = mi.get("Buffers", 0)
                        cached_kb = mi.get("Cached", 0)
                        used_kb = total_kb - free_kb - buffers_kb - cached_kb
                        node.mem_total_gb = round(total_kb / 1048576.0, 2)
                        node.mem_used_gb = round(max(0, used_kb) / 1048576.0, 2)
                        node.mem_free_gb = round(node.mem_total_gb - node.mem_used_gb, 2)
                    for jid, cg in result["job_cgroups"].items():
                        matched_jobs = [j for j in node.jobs if j == jid or j.startswith(jid + "_")]
                        for mj in matched_jobs:
                            if mj in jobs:
                                if cg["cpu_ns"] >= 0:
                                    job_cpu = self._compute_job_cpu_pct(
                                        mj, cg["cpu_ns"], node.cpus_total
                                    )
                                    if job_cpu >= 0:
                                        jobs[mj].cpu_percent = job_cpu
                                    else:
                                        # 首次无 prev，沿用上次有效值
                                        fallback = self._last_job_metrics.get(mj, {}).get("cpu_percent", 0)
                                        if fallback > 0:
                                            jobs[mj].cpu_percent = fallback
                                if cg["mem_bytes"] >= 0:
                                    jobs[mj].mem_used_gb = round(cg["mem_bytes"] / (1024**3), 3)
                    # 保存本次计算的 metrics，供下次缓存使用
                    # 注意：不让首轮 delta=0 的 cpu_percent 覆盖缓存中可能存在的有效值
                    old_metrics = self._last_node_metrics.get(node_name, {})
                    self._last_node_metrics[node_name] = {
                        "cpu_percent": node.cpu_percent if node.cpu_percent > 0 else old_metrics.get("cpu_percent", 0),
                        "mem_total_gb": node.mem_total_gb,
                        "mem_used_gb": node.mem_used_gb,
                        "mem_free_gb": node.mem_free_gb,
                    }
                    for jid in node.jobs:
                        if jid in jobs:
                            old_job = self._last_job_metrics.get(jid, {})
                            self._last_job_metrics[jid] = {
                                "cpu_percent": jobs[jid].cpu_percent if jobs[jid].cpu_percent > 0 else old_job.get("cpu_percent", 0),
                                "mem_used_gb": jobs[jid].mem_used_gb if jobs[jid].mem_used_gb > 0 else old_job.get("mem_used_gb", 0),
                            }

            # ── 最终兜底：有活跃任务但 CPU=0% 的节点，用上一轮 snapshot 的数据替代 ──
            # 这发生在 warm-up 首轮（无 delta）或 backfill 尚未完成时
            if self._last_snapshot:
                for node_name, node in nodes.items():
                    if node.cpu_percent == 0 and node.jobs and "down" not in node.state.lower():
                        prev_node = self._last_snapshot.nodes.get(node_name)
                        if prev_node and prev_node.cpu_percent > 0:
                            node.cpu_percent = prev_node.cpu_percent
                        # 同样修复内存（如果未被实时数据覆盖可能是 SLURM 粗略值）
                        if node.mem_used_gb == 0 and prev_node and prev_node.mem_used_gb > 0:
                            node.mem_total_gb = prev_node.mem_total_gb
                            node.mem_used_gb = prev_node.mem_used_gb
                            node.mem_free_gb = prev_node.mem_free_gb
                # 同样修复 job CPU 和内存
                for jid, job in jobs.items():
                    if job.state == "RUNNING":
                        prev_job = self._last_snapshot.jobs.get(jid)
                        if prev_job:
                            if job.cpu_percent == 0 and prev_job.cpu_percent > 0:
                                job.cpu_percent = prev_job.cpu_percent
                            if job.mem_used_gb == 0 and prev_job.mem_used_gb > 0:
                                job.mem_used_gb = prev_job.mem_used_gb

            # ── 检测最近结束的任务：上一轮存在但本轮消失的 RUNNING 任务 ──
            now = time.time()
            _newly_finished_jids = []
            # 加载追踪用户设置
            _cluster_user = "zzr"
            try:
                with open("user_settings.json", "r") as f:
                    _us = json.load(f)
                    track_users = set(u.strip() for u in _us.get("historyTrackUsers", "zzr").split(",") if u.strip())
                    retain_sec = _us.get("historyDurationMin", 60) * 60
                    _cluster_user = _us.get("clusterUsername", "zzr")
            except Exception:
                track_users = {"zzr"}
                retain_sec = 3600
            if self._last_snapshot:
                _archive_changed = False
                for jid, prev_job in self._last_snapshot.jobs.items():
                    if prev_job.state == "RUNNING" and jid not in jobs:
                        # 任务从 squeue 消失，说明已结束
                        finished_job = JobInfo(
                            job_id=prev_job.job_id, name=prev_job.name,
                            user=prev_job.user, state="COMPLETED",
                            partition=prev_job.partition, nodes=prev_job.nodes,
                            num_cpus=prev_job.num_cpus, num_nodes=prev_job.num_nodes,
                            time_used=prev_job.time_used, time_limit=prev_job.time_limit,
                            submit_time=prev_job.submit_time, start_time=prev_job.start_time,
                            work_dir=prev_job.work_dir, command=prev_job.command,
                            cpu_percent=prev_job.cpu_percent,
                            mem_used_gb=prev_job.mem_used_gb,
                            mem_limit_gb=prev_job.mem_limit_gb,
                        )
                        self._recently_finished[jid] = (finished_job, now)
                        _newly_finished_jids.append(jid)
                        logger.info(f"Job {jid} ({prev_job.name}) finished, tracking for display")
                        # 将被追踪用户的任务归档到持久化存储
                        if prev_job.user in track_users:
                            self._archived_jobs[jid] = (finished_job, now)
                            _archive_changed = True
                            logger.info(f"Job {jid} archived for user {prev_job.user}")
                # 同时将正在运行的追踪用户任务也纳入归档（更新最新状态）
                for jid, job in jobs.items():
                    if job.user in track_users and job.state == "RUNNING":
                        if jid in self._archived_jobs:
                            # 更新运行中任务的最新指标
                            self._archived_jobs[jid] = (job, 0)
                            _archive_changed = True
                if _archive_changed:
                    self._save_archived_jobs()

            # 清理过期的已结束任务（超过历史保留时间）
            expired = [jid for jid, (_, end_t) in self._recently_finished.items()
                       if now - end_t > retain_sec]
            for jid in expired:
                del self._recently_finished[jid]

            snapshot = ClusterSnapshot(
                timestamp=time.time(), nodes=nodes, jobs=jobs, partitions=partitions
            )
            # 诊断：检查哪些有任务的节点仍然显示 0% CPU
            zero_cpu_active = [
                n for n, nd in nodes.items()
                if nd.cpu_percent == 0 and nd.jobs and "down" not in nd.state.lower()
            ]
            if zero_cpu_active:
                logger.warning(f"0%CPU with active jobs: {','.join(sorted(zero_cpu_active))} "
                               f"(collect #{self._collect_count})")
            self._update_history(snapshot)
            self._last_snapshot = snapshot
            self._collect_count += 1
            if self._collect_count % 30 == 0:
                self._save_cache()

            # 异步获取新结束任务的 stdout/stderr 路径（通过 scontrol，不阻塞主流程）
            if _newly_finished_jids:
                asyncio.create_task(self._fetch_finished_log_paths(_newly_finished_jids))

            # 异步采集追踪用户运行中作业的 stdout/stderr 输出
            if _cluster_user:
                asyncio.create_task(self._collect_job_logs(jobs, _cluster_user))

            # 异步采集 NUMA 内存分布趋势（每6个采集周期执行一次，减少 SSH 开销）
            try:
                with open("user_settings.json", "r") as f:
                    _numa_settings = json.load(f)
                    _numa_enabled = _numa_settings.get("numaTrackEnabled", False)
            except Exception:
                _numa_enabled = False
            if _numa_enabled and _cluster_user and self._collect_count % 6 == 0:
                asyncio.create_task(self._collect_job_numa(jobs, _cluster_user))

            return snapshot

    async def _fetch_finished_log_paths(self, job_ids: list):
        """异步获取已结束任务的 stdout/stderr 文件路径（通过 scontrol）"""
        for jid in job_ids:
            try:
                details = await self.get_job_details(jid)
                if not details:
                    logger.debug(f"scontrol 已无法查询 job {jid}，跳过路径获取")
                    continue
                stdout_path = details.get("StdOut", "")
                stderr_path = details.get("StdErr", "")
                # 更新 _recently_finished 中的路径
                if jid in self._recently_finished:
                    fj = self._recently_finished[jid][0]
                    fj.stdout_path = stdout_path
                    fj.stderr_path = stderr_path
                # 更新 _archived_jobs 中的路径并持久化
                if jid in self._archived_jobs:
                    aj = self._archived_jobs[jid][0]
                    aj.stdout_path = stdout_path
                    aj.stderr_path = stderr_path
                logger.info(f"Job {jid} log paths: stdout={stdout_path}, stderr={stderr_path}")
            except Exception as e:
                logger.warning(f"获取 job {jid} 日志路径失败: {e}")
        # 如果有归档任务路径更新，保存到磁盘
        if any(jid in self._archived_jobs for jid in job_ids):
            self._save_archived_jobs()

    async def _collect_job_logs(self, jobs: Dict[str, 'JobInfo'], cluster_username: str):
        """周期性采集指定用户运行中作业的 stdout/stderr 输出（本地 tail 命令）"""
        try:
            # 找出需要采集的作业
            target_jobs = {jid: j for jid, j in jobs.items()
                           if j.user == cluster_username and j.state == "RUNNING"}
            if not target_jobs:
                # 清空缓存（所有作业已结束）
                self._job_log_cache.clear()
                return

            for jid, job in target_jobs.items():
                # 获取 stdout/stderr 路径（优先使用缓存，避免重复 scontrol 查询）
                if jid not in self._job_log_paths:
                    details = await self.get_job_details(jid)
                    if details:
                        self._job_log_paths[jid] = {
                            "stdout": details.get("StdOut", ""),
                            "stderr": details.get("StdErr", ""),
                        }
                    else:
                        continue

                paths = self._job_log_paths.get(jid, {})
                stdout_path = paths.get("stdout", "")
                stderr_path = paths.get("stderr", "")

                # 通过 tail 读取最新内容（本地执行，共享文件系统）
                stdout_content = ""
                if stdout_path and stdout_path != "/dev/null":
                    stdout_content = await self._run_cmd(
                        f"tail -n 500 '{stdout_path}' 2>&1", timeout=3) or ""
                stderr_content = ""
                if stderr_path and stderr_path != "/dev/null":
                    stderr_content = await self._run_cmd(
                        f"tail -n 500 '{stderr_path}' 2>&1", timeout=3) or ""

                self._job_log_cache[jid] = {
                    "stdout": stdout_content,
                    "stderr": stderr_content,
                    "stdout_path": stdout_path,
                    "stderr_path": stderr_path,
                    "ts": time.time(),
                }

            # 清理不再运行的作业的缓存
            for jid in list(self._job_log_cache.keys()):
                if jid not in target_jobs:
                    del self._job_log_cache[jid]
            # 同步清理路径缓存
            for jid in list(self._job_log_paths.keys()):
                if jid not in target_jobs and jid not in self._recently_finished:
                    del self._job_log_paths[jid]
        except Exception as e:
            logger.warning(f"采集作业日志失败: {e}")

    async def _collect_job_numa(self, jobs: Dict[str, 'JobInfo'], cluster_username: str):
        """周期性采集指定用户运行中作业的 NUMA 内存分布（每6个周期执行一次）

        通过 SSH 到计算节点读取 cgroup memory.numa_stat（hierarchical_total 行），
        获取各 NUMA 节点的内存页数。这比 numastat -p 更准确，因为它覆盖作业的
        所有进程和页面缓存，与 cgroup memory.usage_in_bytes 值一致。
        """
        try:
            target_jobs = {jid: j for jid, j in jobs.items()
                           if j.user == cluster_username and j.state == "RUNNING" and j.nodes}
            if not target_jobs:
                self._job_numa_cache.clear()
                return

            for jid, job in target_jobs.items():
                try:
                    # 取第一个节点
                    node = job.nodes.split(",")[0].strip()
                    if not node or node == "(None)":
                        continue

                    # 读取 cgroup memory.numa_stat + cpuset + NUMA 拓扑
                    script = f"""
grep '^hierarchical_total' /sys/fs/cgroup/memory/slurm_$(hostname -s)/uid_*/job_{jid}/memory.numa_stat 2>/dev/null
echo "===CPUSET==="
cat /sys/fs/cgroup/cpuset/slurm_$(hostname -s)/uid_*/job_{jid}/cpuset.cpus 2>/dev/null || echo "ALL"
echo "===TOPO==="
for n in /sys/devices/system/node/node*/cpulist; do
    nname=$(dirname "$n" | xargs basename)
    echo "$nname:$(cat $n)"
done
"""
                    # 通过 paramiko 连接池执行（带超时保护）
                    out = None
                    if HAS_PARAMIKO and _ssh_pool:
                        loop = asyncio.get_event_loop()
                        try:
                            out = await asyncio.wait_for(
                                loop.run_in_executor(_ssh_pool._executor, _ssh_pool.exec_cmd, node, script, 10),
                                timeout=12
                            )
                        except asyncio.TimeoutError:
                            logger.debug(f"NUMA 采集超时: job {jid} on {node}")
                            continue
                    else:
                        out = await self._ssh_cmd(node, script)

                    if not out:
                        continue

                    # 解析 hierarchical_total 行: "hierarchical_total=1221604 N0=572744 N1=648860"
                    per_node_pages = {}  # {numa_node_id: pages}
                    cpuset_str = "ALL"
                    numa_topo = {}
                    section = "numastat"
                    for line in out.strip().split("\n"):
                        if line.strip() == "===CPUSET===":
                            section = "cpuset"
                            continue
                        elif line.strip() == "===TOPO===":
                            section = "topo"
                            continue

                        if section == "numastat":
                            stripped = line.strip()
                            if stripped.startswith("hierarchical_total"):
                                for token in stripped.split():
                                    if token.startswith("N") and "=" in token:
                                        try:
                                            nid_str, val_str = token.split("=", 1)
                                            nid = int(nid_str[1:])  # N0 → 0
                                            per_node_pages[nid] = int(val_str)
                                        except ValueError:
                                            pass
                        elif section == "cpuset":
                            cpuset_str = line.strip()
                        elif section == "topo":
                            if ":" in line:
                                parts = line.split(":", 1)
                                nid_str = parts[0].replace("node", "").strip()
                                try:
                                    numa_topo[int(nid_str)] = parts[1].strip()
                                except ValueError:
                                    pass

                    if not per_node_pages:
                        continue

                    # 将页数转换为 MB（page_size = 4096）
                    sorted_nids = sorted(per_node_pages.keys())
                    per_node_mb = [per_node_pages[nid] * 4096 / 1048576 for nid in sorted_nids]

                    # 确定 local/remote
                    local_nodes = set()
                    if cpuset_str and cpuset_str != "ALL":
                        cpuset_cpus = self._parse_cpu_range(cpuset_str)
                        for nid, cpu_range in numa_topo.items():
                            node_cpus = self._parse_cpu_range(cpu_range)
                            if cpuset_cpus & node_cpus:
                                local_nodes.add(nid)
                    else:
                        # 所有 CPU 都分配了
                        local_nodes = set(numa_topo.keys())

                    # 启发式：如果所有 NUMA 节点都"本地"，则内存最多的为本地
                    if local_nodes and len(local_nodes) == len(numa_topo) and len(per_node_mb) > 1:
                        max_idx = max(range(len(per_node_mb)), key=lambda i: per_node_mb[i])
                        local_nodes = {sorted_nids[max_idx]}

                    local_mb = sum(per_node_mb[i] for i in range(len(per_node_mb)) if sorted_nids[i] in local_nodes)
                    remote_mb = sum(per_node_mb[i] for i in range(len(per_node_mb)) if sorted_nids[i] not in local_nodes)

                    self._job_numa_cache[jid] = {
                        "local_mb": round(local_mb, 2),
                        "remote_mb": round(remote_mb, 2),
                        "per_node": per_node_mb,
                        "ts": time.time(),
                    }
                except Exception as e:
                    logger.debug(f"NUMA 采集异常 job {jid}: {e}")

            # 清理不再运行的作业
            for jid in list(self._job_numa_cache.keys()):
                if jid not in target_jobs:
                    del self._job_numa_cache[jid]
        except Exception as e:
            logger.warning(f"NUMA 趋势采集失败: {e}")

    def _expand_nodelist(self, nodelist: str) -> List[str]:
        if not nodelist or nodelist == "(None)":
            return []
        try:
            result = []
            for part in re.findall(r'([a-zA-Z]+)(\[[\d,\-]+\]|\d+)', nodelist):
                prefix = part[0]
                suffix = part[1]
                if suffix.startswith("[") and suffix.endswith("]"):
                    ranges = suffix[1:-1].split(",")
                    for r in ranges:
                        if "-" in r:
                            start, end = r.split("-", 1)
                            width = len(start)
                            for i in range(int(start), int(end) + 1):
                                result.append(f"{prefix}{str(i).zfill(width)}")
                        else:
                            result.append(f"{prefix}{r}")
                else:
                    result.append(f"{prefix}{suffix}")
            if not result:
                result = [nodelist]
            return result
        except Exception:
            return [nodelist]

    async def get_job_details(self, job_id: str) -> Optional[dict]:
        out = await self._run_cmd(f"scontrol show job {job_id} 2>/dev/null")
        if not out:
            return None
        info = {}
        for token in re.split(r'\s+', out):
            if "=" in token:
                k, v = token.split("=", 1)
                info[k] = v
        return info

    async def cancel_job(self, job_id: str) -> dict:
        out = await self._run_cmd(f"scancel {job_id} 2>&1")
        if out is None:
            out = ""
        return {"success": True, "message": f"Cancel signal sent for job {job_id}. {out}".strip()}

    async def get_job_numa_analysis(self, job_id: str) -> dict:
        """按需获取作业 NUMA 内存分布分析（通过 SSH 到计算节点读取 cgroup memory.numa_stat）"""
        # 1. 获取作业详情
        details = await self.get_job_details(job_id)
        if not details:
            return {"error": f"作业 {job_id} 未找到（可能已结束且无法查询 scontrol）"}

        node_list_str = details.get("NodeList", "")
        if not node_list_str:
            return {"error": "无法确定作业运行节点"}

        # 取第一个节点（多节点作业只分析第一个）
        node = node_list_str.split(",")[0].strip()
        if "[" in node:
            import re
            m = re.match(r'([a-zA-Z]+)\[(\d+)', node)
            if m:
                node = m.group(1) + m.group(2)

        # 2. 构建 SSH 命令：读取 cgroup memory.numa_stat + cpuset + NUMA 拓扑
        numa_script = f"""
echo "===NUMA_TOPO==="
for n in /sys/devices/system/node/node*/cpulist; do
    nname=$(dirname "$n" | xargs basename)
    echo "$nname:$(cat $n)"
done
echo "===CPUSET==="
cat /sys/fs/cgroup/cpuset/slurm_$(hostname -s)/uid_*/job_{job_id}/cpuset.cpus 2>/dev/null || echo "N/A"
echo "===CGROUP_NUMA==="
cat /sys/fs/cgroup/memory/slurm_$(hostname -s)/uid_*/job_{job_id}/memory.numa_stat 2>/dev/null || echo "N/A"
echo "===CGROUP_USAGE==="
cat /sys/fs/cgroup/memory/slurm_$(hostname -s)/uid_*/job_{job_id}/memory.usage_in_bytes 2>/dev/null || echo "0"
"""
        # 3. 通过 paramiko 连接池执行命令
        out = None
        if HAS_PARAMIKO and _ssh_pool:
            loop = asyncio.get_event_loop()
            try:
                out = await asyncio.wait_for(
                    loop.run_in_executor(_ssh_pool._executor, _ssh_pool.exec_cmd, node, numa_script, 15),
                    timeout=20
                )
            except asyncio.TimeoutError:
                logger.warning(f"NUMA分析 job {job_id} on {node}: asyncio超时(20s)")
                out = None
            except Exception as e:
                logger.warning(f"NUMA分析 job {job_id} on {node}: 执行异常 {e}")
                out = None
        else:
            try:
                out = await self._ssh_cmd(node, numa_script)
            except Exception as e:
                logger.warning(f"NUMA分析 job {job_id} on {node}: _ssh_cmd异常 {e}")

        if not out:
            return {"error": f"SSH 到节点 {node} 失败或超时"}

        # 4. 解析结果
        return self._parse_numa_output(out, node, job_id)

    def _parse_numa_output(self, raw_output: str, node: str, job_id: str) -> dict:
        """解析 NUMA 分析脚本的结构化输出（基于 cgroup memory.numa_stat）"""
        result = {"node": node, "job_id": job_id, "numa_nodes": [], "local_numa_nodes": []}
        sections = {}
        current_section = None
        current_lines = []
        for line in raw_output.split("\n"):
            if line.startswith("===") and line.endswith("==="):
                if current_section:
                    sections[current_section] = current_lines
                current_section = line.strip("=")
                current_lines = []
            else:
                current_lines.append(line)
        if current_section:
            sections[current_section] = current_lines

        # 解析 NUMA 拓扑：nodeN:cpulist
        numa_topo = {}  # node_id -> cpu_list_str
        for line in sections.get("NUMA_TOPO", []):
            if ":" in line:
                parts = line.split(":", 1)
                nid = parts[0].replace("node", "").strip()
                cpus = parts[1].strip()
                try:
                    numa_topo[int(nid)] = cpus
                except ValueError:
                    pass

        # 解析 cpuset
        cpuset_str = "\n".join(sections.get("CPUSET", [])).strip()
        result["cpuset"] = cpuset_str if cpuset_str != "N/A" else ""

        # 解析 cgroup usage_in_bytes（用于显示总内存参考值）
        usage_bytes = 0
        for line in sections.get("CGROUP_USAGE", []):
            line = line.strip()
            if line.isdigit():
                usage_bytes = int(line)
                break
        result["cgroup_usage_mb"] = round(usage_bytes / 1048576, 1)

        # 确定哪些 NUMA 节点是"本地"的
        local_nodes = set()
        if cpuset_str and cpuset_str != "N/A":
            cpuset_cpus = self._parse_cpu_range(cpuset_str)
            for nid, cpu_range_str in numa_topo.items():
                node_cpus = self._parse_cpu_range(cpu_range_str)
                if cpuset_cpus & node_cpus:
                    local_nodes.add(nid)
        result["local_numa_nodes"] = sorted(local_nodes)

        # 解析 cgroup memory.numa_stat
        # 格式: hierarchical_total=1221604 N0=572744 N1=648860
        cgroup_numa_lines = sections.get("CGROUP_NUMA", [])
        cgroup_numa_text = "\n".join(cgroup_numa_lines)
        if "N/A" in cgroup_numa_text and len(cgroup_numa_text.strip()) <= 3:
            result["error"] = "未找到作业 cgroup 信息（可能已结束）"
            return result

        per_node_pages = {}
        for line in cgroup_numa_lines:
            stripped = line.strip()
            if stripped.startswith("hierarchical_total"):
                for token in stripped.split():
                    if token.startswith("N") and "=" in token:
                        try:
                            nid_str, val_str = token.split("=", 1)
                            nid = int(nid_str[1:])  # N0 → 0
                            per_node_pages[nid] = int(val_str)
                        except ValueError:
                            pass
                break

        if per_node_pages:
            sorted_nids = sorted(per_node_pages.keys())
            for nid in sorted_nids:
                pages = per_node_pages[nid]
                mb = pages * 4096 / 1048576
                cpus_str = numa_topo.get(nid, "")
                result["numa_nodes"].append({
                    "node_id": nid,
                    "total_mb": round(mb, 1),
                    "cpus": cpus_str
                })
        else:
            result["error"] = "无法解析 cgroup NUMA 内存数据"
            return result

        # 如果所有 NUMA 节点都是"本地"的，使用启发式
        if local_nodes and len(local_nodes) == len(numa_topo) and len(result["numa_nodes"]) > 1:
            max_mem = 0
            max_nid = 0
            for nn in result["numa_nodes"]:
                if nn["total_mb"] > max_mem:
                    max_mem = nn["total_mb"]
                    max_nid = nn["node_id"]
            result["local_numa_nodes"] = [max_nid]

        return result

    @staticmethod
    def _parse_cpu_range(range_str: str) -> set:
        """解析 CPU 范围字符串（如 '0-95,96-191'）为 CPU ID 集合"""
        cpus = set()
        for part in range_str.split(","):
            part = part.strip()
            if "-" in part:
                try:
                    start, end = part.split("-", 1)
                    cpus.update(range(int(start), int(end) + 1))
                except ValueError:
                    pass
            elif part.isdigit():
                cpus.add(int(part))
        return cpus

    async def get_job_log(self, job_id: str, log_type: str = "stdout", tail: int = 200) -> Optional[str]:
        """获取任务日志，优先使用周期采集缓存，回退到 scontrol → recently_finished → archived_jobs"""
        # 优先从周期采集缓存读取（运行中的作业，数据更新鲜）
        cached = self._job_log_cache.get(job_id)
        if cached:
            content = cached.get(log_type, "")
            if content:
                return content

        details = await self.get_job_details(job_id)
        if not details:
            # scontrol 找不到任务，依次从 recently_finished 和 archived_jobs 提取路径
            finished = self._recently_finished.get(job_id)
            if not finished:
                finished = self._archived_jobs.get(job_id)
            if finished:
                fj = finished[0]
                path = fj.stderr_path if log_type == "stderr" else fj.stdout_path
                if path and path != "/dev/null":
                    out = await self._run_cmd(f"tail -n {tail} '{path}' 2>&1", timeout=5)
                    return out if out else f"Cannot read {path}"
            return None
        if log_type == "stderr":
            path = details.get("StdErr", "")
        else:
            path = details.get("StdOut", "")
        if not path or path == "/dev/null":
            return f"No {log_type} file configured for job {job_id}"
        out = await self._run_cmd(f"tail -n {tail} '{path}' 2>&1", timeout=5)
        return out if out else f"Cannot read {path}"

    async def list_directory(self, path: str, compute_dir_sizes: bool = False) -> List[dict]:
        entries = []
        dir_tasks = []  # (index, full_path) for async size computation
        try:
            for entry in sorted(os.listdir(path)):
                full = os.path.join(path, entry)
                try:
                    st = os.stat(full)
                    is_dir = os.path.isdir(full)
                    size = st.st_size
                    idx = len(entries)
                    entries.append({
                        "name": entry,
                        "type": "dir" if is_dir else "file",
                        "size": size,
                        "mtime": st.st_mtime
                    })
                    if is_dir and compute_dir_sizes:
                        dir_tasks.append((idx, full))
                except OSError:
                    pass
        except OSError as e:
            logger.error(f"Cannot list directory {path}: {e}")
        # Compute dir sizes in parallel (max 20 concurrent)
        if dir_tasks:
            tasks = [self._get_dir_size(full) for _, full in dir_tasks]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (idx, _), result in zip(dir_tasks, results):
                if isinstance(result, int) and result > 0:
                    entries[idx]["size"] = result
        return entries

    async def _get_dir_size(self, path: str) -> int:
        """Get directory size using du command (async, with short timeout)."""
        try:
            result = await self._run_cmd(f"du -sb --max-depth=0 '{path}' 2>/dev/null | cut -f1", timeout=3)
            if result and result.strip().isdigit():
                return int(result.strip())
        except Exception:
            pass
        return -1

    async def read_file_content(self, path: str, max_size: int = 1048576) -> Optional[str]:
        try:
            if os.path.getsize(path) > max_size:
                return f"[File too large: {os.path.getsize(path)} bytes, limit {max_size}]"
            with open(path, "r", errors="replace") as f:
                return f.read()
        except Exception as e:
            return f"[Error reading file: {e}]"

    async def save_file_content(self, path: str, content: str) -> dict:
        """保存文本内容到文件"""
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            return {"success": True, "message": f"File saved: {path}"}
        except Exception as e:
            return {"success": False, "message": f"Error saving file: {e}"}

    async def submit_sbatch(self, path: str) -> dict:
        """提交sbatch脚本到SLURM集群（自动注入 PYTHONUNBUFFERED=1 禁用 Python 输出缓冲）"""
        out = await self._run_cmd(
            f"sbatch --export=ALL,PYTHONUNBUFFERED=1 '{path}' 2>&1", timeout=10)
        if out is None:
            return {"success": False, "message": "sbatch命令执行失败或超时"}
        if "Submitted batch job" in out:
            return {"success": True, "message": out.strip()}
        return {"success": False, "message": out.strip()}

    def get_node_history(self, node_name: str, last_n: int = 300, since: float = 0) -> List[dict]:
        h = self._node_history.get(node_name, deque())
        data = list(h)
        if since > 0:
            data = [p for p in data if p["t"] >= since]
        return data[-last_n:] if last_n > 0 else data

    def get_job_history(self, job_id: str, last_n: int = 300, since: float = 0) -> List[dict]:
        h = self._job_history.get(job_id, deque())
        data = list(h)
        if since > 0:
            data = [p for p in data if p["t"] >= since]
        return data[-last_n:] if last_n > 0 else data

    def clear_cache(self):
        """清除所有历史缓存（内存+磁盘批次文件）"""
        self._node_history.clear()
        self._job_history.clear()
        self._prev_cpu_stats.clear()
        self._prev_job_cpu.clear()
        self._last_save_time = 0.0
        # 清除磁盘缓存（批次文件+旧版单文件）
        for pattern in ["cache_*.json", "history_cache.json"]:
            for f in glob.glob(os.path.join(CACHE_DIR, pattern)):
                try:
                    os.remove(f)
                except OSError:
                    pass
        logger.info("所有缓存已清除")

    def get_cache_stats(self) -> dict:
        """获取缓存统计信息（含磁盘文件大小）"""
        node_points = sum(len(v) for v in self._node_history.values())
        job_points = sum(len(v) for v in self._job_history.values())
        # 磁盘缓存统计
        cache_files = sorted(glob.glob(os.path.join(CACHE_DIR, "cache_*.json")))
        disk_size_bytes = sum(os.path.getsize(f) for f in cache_files) if cache_files else 0
        first_ts = 0
        if cache_files:
            try:
                bn = os.path.basename(cache_files[0])
                first_ts = int(bn.split("_")[1].split(".")[0])
            except (ValueError, IndexError):
                pass
        return {
            "node_series": len(self._node_history),
            "job_series": len(self._job_history),
            "total_points": node_points + job_points,
            "node_points": node_points,
            "job_points": job_points,
            "disk_size_mb": round(disk_size_bytes / (1024 * 1024), 2),
            "batch_files": len(cache_files),
            "first_timestamp": first_ts
        }

    async def run_bash(self, path: str) -> dict:
        """在集群上运行bash脚本"""
        out = await self._run_cmd(f"bash '{path}' 2>&1", timeout=30)
        if out is None:
            return {"success": False, "message": "bash命令执行失败或超时"}
        return {"success": True, "message": out.strip() if out.strip() else "脚本执行完毕（无输出）"}

    def snapshot_to_dict(self, snap: ClusterSnapshot) -> dict:
        nodes_list = []
        for n in snap.nodes.values():
            nodes_list.append({
                "name": n.name, "state": n.state,
                "cpus_total": n.cpus_total, "cpus_alloc": n.cpus_alloc, "cpus_idle": n.cpus_idle,
                "mem_total_gb": n.mem_total_gb, "mem_used_gb": n.mem_used_gb, "mem_free_gb": n.mem_free_gb,
                "cpu_percent": n.cpu_percent, "partitions": n.partitions, "jobs": n.jobs
            })
        jobs_list = []
        for j in snap.jobs.values():
            jobs_list.append(self._job_to_dict(j))
        # 添加最近结束的任务（带 end_time 标记）
        for jid, (fj, end_t) in self._recently_finished.items():
            d = self._job_to_dict(fj)
            d["end_time"] = time.strftime("%H:%M:%S", time.localtime(end_t))
            d["end_timestamp"] = end_t
            jobs_list.append(d)
        parts_list = []
        for p in snap.partitions.values():
            parts_list.append({
                "name": p.name, "state": p.state,
                "nodes_total": p.nodes_total, "nodes_idle": p.nodes_idle,
                "nodes_alloc": p.nodes_alloc, "nodes_down": p.nodes_down,
                "cpus_total": p.cpus_total, "cpus_alloc": p.cpus_alloc,
                "timelimit": p.timelimit,
                "node_list": p.node_list
            })
        total_cpus = sum(n.cpus_total for n in snap.nodes.values())
        alloc_cpus = sum(n.cpus_alloc for n in snap.nodes.values())
        total_mem = sum(n.mem_total_gb for n in snap.nodes.values())
        used_mem = sum(n.mem_used_gb for n in snap.nodes.values())
        running = sum(1 for j in snap.jobs.values() if j.state == "RUNNING")
        pending = sum(1 for j in snap.jobs.values() if j.state == "PENDING")
        return {
            "timestamp": snap.timestamp,
            "summary": {
                "total_nodes": len(snap.nodes),
                "total_cpus": total_cpus, "alloc_cpus": alloc_cpus,
                "total_mem_gb": round(total_mem, 2), "used_mem_gb": round(used_mem, 2),
                "running_jobs": running, "pending_jobs": pending,
                "total_jobs": len(snap.jobs)
            },
            "nodes": sorted(nodes_list, key=lambda x: x["name"]),
            "jobs": sorted(jobs_list, key=lambda x: x["job_id"]),
            "partitions": sorted(parts_list, key=lambda x: x["name"])
        }

    def _job_to_dict(self, j: JobInfo) -> dict:
        return {
            "job_id": j.job_id, "name": j.name, "user": j.user,
            "state": j.state, "partition": j.partition, "nodes": j.nodes,
            "num_cpus": j.num_cpus, "num_nodes": j.num_nodes,
            "time_used": j.time_used, "time_limit": j.time_limit,
            "submit_time": j.submit_time, "start_time": j.start_time,
            "work_dir": j.work_dir, "cpu_percent": j.cpu_percent,
            "mem_used_gb": j.mem_used_gb
        }
