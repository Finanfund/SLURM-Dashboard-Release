# SLURM Dashboard

一个部署在 SLURM 登录节点上的 Web 面板，用来查看集群状态、追踪作业、浏览文件，并对常见运维操作做轻量封装。

这个发布目录已经清除了个人缓存、个人账号信息、个人密码、个人默认端口和个人文件路径，适合作为通用发行版直接上传到 GitHub。

## 功能概览

- 集群总览：实时查看分区、节点、CPU 和内存使用情况。
- 作业管理：查看运行中和排队作业，支持搜索、排序、详情和取消作业。
- 历史任务：记录指定用户的历史作业，支持搜索和详情回看。
- 文件浏览：浏览、编辑、上传、下载文件，支持收藏夹。
- 日志查看：查看 stdout 和 stderr，并支持自动刷新。
- 脚本执行：可直接对脚本执行 sbatch 或 bash。
- NUMA 分析：支持按需 NUMA 分析和趋势记录。
- 低功耗模式：前端支持降低视觉特效，减少浏览器渲染压力。

## 运行要求

- Linux 登录节点，并且该节点可以执行 squeue、sinfo、scontrol、sbatch。
- Python 3.6 及以上，推荐 3.9 及以上。
- 登录节点能够通过 SSH 访问计算节点。
- 浏览器通过集群内网或 SSH 隧道访问面板。

## 快速开始

### 1. 获取代码

```bash
git clone https://github.com/Finanfund/SLURM-Dashboard-Release.git
cd SLURM-Dashboard-Release
```

或者直接把这个目录上传到登录节点任意位置。

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

如果你使用 conda 或 venv，也可以先激活环境再执行安装。

### 3. 启动服务

推荐使用启动脚本，并显式传入访问密码：

```bash
bash launch.sh --password your-password
```

如果需要指定端口：

```bash
bash launch.sh 9000 --password your-password
```

也可以直接运行 Python：

```bash
export DASHBOARD_PASSWORD=your-password
python app.py --host 0.0.0.0 --port 8000
```

### 4. 访问服务

如果在登录节点内网直接访问：

```text
http://<login-node-host>:<port>
```

如果从本地机器访问，建议使用 SSH 隧道：

```bash
ssh -N -L <port>:localhost:<port> <user>@<cluster>
```

然后在本地浏览器打开：

```text
http://localhost:<port>
```

## 常用命令

```bash
# 启动
bash launch.sh --password your-password
bash launch.sh 9000 --password your-password

# 查看状态
bash launch.sh status

# 重启
bash launch.sh restart 9000

# 停止
bash launch.sh stop

# 前台调试
python start.py --fg --port 8000
```

## 环境变量

```bash
export DASHBOARD_HOST=0.0.0.0
export DASHBOARD_PORT=8000
export DASHBOARD_PASSWORD=your-password
export DASHBOARD_CLUSTER_NAME="My SLURM Cluster"
export FILE_BROWSER_ROOT=/home/your-user
```

说明：

- DASHBOARD_PORT 默认是 8000。
- DASHBOARD_PASSWORD 默认占位值是 change-me，发布使用时应显式设置。
- FILE_BROWSER_ROOT 默认取当前用户家目录。

## 首次登录后建议配置

打开页面后，建议在设置中补全以下项目：

- 本机集群用户名：用于实时日志采集。
- 历史任务追踪用户：用于历史任务归档。
- 刷新间隔：决定采样频率。
- 图表历史窗口：决定前端图表显示多久的数据。
- 缓存策略：按容量或日期清理历史缓存。

如果不填写用户名相关配置，历史追踪和实时日志功能会保持关闭状态，这是当前发布版的默认行为。

## 文件浏览与日志说明

- 文件浏览器只允许访问 FILE_BROWSER_ROOT 之下的路径。
- 对于通过面板提交的 sbatch 脚本，系统会自动附加 PYTHONUNBUFFERED=1，减少 Python 日志缓冲导致的延迟。
- 非 Python 程序如果需要实时日志，建议自行使用 stdbuf -oL 或程序自身的无缓冲参数。

## 运行时生成文件

下面这些文件或目录属于运行产物，不建议提交到仓库：

- server.log
- user_settings.json
- .dashboard.pid
- .dashboard_password
- .cache/

这些路径已经在 [SLURM-Dashboard-Release/.gitignore](SLURM-Dashboard-Release/.gitignore) 中忽略。

## 目录结构

```text
SLURM-Dashboard-Release/
├── app.py
├── collector.py
├── config.py
├── launch.sh
├── start.py
├── stop.py
├── requirements.txt
├── README.md
├── RELEASE_NOTES.md
├── templates/
└── static/
```

## 常见问题

### 1. 页面打不开

- 先执行 bash launch.sh status 确认服务已启动。
- 检查端口是否被占用。
- 如果是远程访问，检查 SSH 隧道是否与实际端口一致。

### 2. 看不到节点数据

- 确认登录节点能 SSH 到计算节点。
- 确认集群节点上可以读取 /proc/stat、/proc/meminfo 和 SLURM cgroup 信息。

### 3. 实时日志不更新

- 检查设置里的“本机集群用户名”是否填写正确。
- 检查作业 stdout/stderr 路径是否可读。
- 对非 Python 程序，考虑手动关闭输出缓冲。

### 4. 历史任务为空

- 检查设置里的“历史任务追踪用户”是否已填写。
- 历史任务依赖运行中和结束时的采样归档，不会自动回填过去未追踪的旧任务。

## 安全建议

- 优先通过 SSH 隧道访问，而不是直接暴露端口到公网。
- 上线前显式设置 DASHBOARD_PASSWORD，不要依赖默认占位密码。
- 根据需要收紧 FILE_BROWSER_ROOT，不要给过大的文件系统范围。

## 版本说明

当前发布说明见 [SLURM-Dashboard-Release/RELEASE_NOTES.md](SLURM-Dashboard-Release/RELEASE_NOTES.md)。
