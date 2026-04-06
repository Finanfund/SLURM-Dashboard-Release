# SLURM Dashboard

一个面向 SLURM 集群登录节点的 Web 管理面板，用来统一查看集群资源、作业状态、历史任务、文件目录、任务日志和登录节点进程信息。

这个发布目录已经清除了个人默认密码、个人默认端口、个人用户名、个人家目录和本地运行产物，适合直接作为通用发行版上传到 GitHub 或部署到新的集群环境。

## 适用场景

- 希望在浏览器里实时查看分区、节点、CPU 和内存占用。
- 希望统一查看运行中任务、排队任务、已结束任务和历史记录。
- 希望直接在网页中浏览工作目录、编辑脚本、上传下载文件、提交 sbatch。
- 希望查看任务 stdout/stderr，并在运行期间自动刷新日志。
- 希望分析作业的 NUMA 内存分布，辅助定位远程内存访问问题。
- 希望在登录节点上快速查看高占用进程并进行清理。

## 核心功能

### 1. 集群状态总览

- 实时展示集群总 CPU、总内存、运行任务数、排队任务数和采集耗时。
- 按分区展示节点卡片，查看分区内节点分布和资源使用情况。
- 节点卡片同时显示 SLURM 分配 CPU 与节点真实 CPU 使用率。
- 节点内存采用 `/proc/meminfo` 计算，避免只看 `MemFree` 带来的误判。
- 支持节点详情展开，直接查看该节点上的任务与历史曲线。

### 2. 作业列表与作业详情

- 展示运行中、排队和已结束任务。
- 支持搜索、排序、按状态过滤。
- 可查看任务名称、用户、状态、分区、节点、CPU、内存和运行时间。
- 任务详情中可查看：
  - 基本信息
  - CPU 历史图
  - 内存历史图
  - stdout / stderr 日志
  - 工作目录跳转
  - 取消任务
- 对已结束任务自动切换到全程历史模式，避免实时刷新覆盖旧数据。

### 3. 历史任务归档

- 支持对指定用户的任务进行历史归档。
- 已结束任务会写入归档缓存，服务重启后仍可恢复。
- 历史任务页支持搜索、排序、重新打开任务详情。
- 可从历史任务直接跳到对应工作目录。

### 4. 文件浏览器

- 限定在 `FILE_BROWSER_ROOT` 之下浏览，避免访问越界。
- 支持目录浏览、返回上级、刷新、创建文件夹。
- 支持文本文件在线查看与编辑。
- 支持文件上传、文件下载、文件夹打包下载。
- 支持收藏夹。
- 支持切换是否计算文件夹大小。
- 支持分栏编辑模式。

### 5. 脚本执行与作业提交

- 可直接对脚本执行 `sbatch` 提交。
- 可直接对脚本执行 `bash`。
- 对通过面板提交的 `sbatch` 脚本，系统会自动注入：

```bash
--export=ALL,PYTHONUNBUFFERED=1
```

这样 Python 程序的 stdout/stderr 可以更快刷新到日志文件。

### 6. 实时日志查看

- 任务详情页支持 stdout 和 stderr 切换。
- 运行中任务的日志会自动刷新。
- 支持跟随模式和锁定模式。
- 支持快速滚到顶部或底部。
- 历史任务仍可查看归档日志路径对应的内容。

### 7. NUMA 分析

- 支持按需查看单个任务的 NUMA 内存分布。
- 支持趋势记录模式，将本地内存和远程内存拆分展示。
- 结果基于 cgroup `memory.numa_stat`，不是简单进程 RSS，精度更高。
- 有助于定位跨 NUMA 节点访问过多带来的性能损失。

### 8. 登录节点监控

- 单独页面展示登录节点：
  - 1/5/15 分钟负载
  - CPU 核数
  - 内存占用
  - 可用内存
  - 在线用户数
- 支持查看登录节点进程列表。
- 支持按 CPU、内存、PID、运行时间等排序。
- 支持对当前用户可控进程发送终止信号。

### 9. 性能与稳定性优化

- 使用 paramiko 持久 SSH 连接池，减少重复握手开销。
- 批量节点采集支持超时回退和缓存补偿。
- 前端支持低功耗模式，降低浏览器渲染压力。
- 页面不可见时减少无意义刷新。
- 日志刷新做了节流，避免对登录节点造成过高压力。
- 登录节点监控接口带有短时间缓存，减少重复 `ps` 扫描。

### 10. 缓存与数据持久化

- 节点和任务历史数据会周期性写入 `.cache/`。
- 历史任务归档会写入 `.cache/archived_jobs.json`。
- 服务关闭时会优雅保存缓存和归档。
- 支持按最大缓存容量或保留日期清理历史缓存。
- 可在设置页中查看缓存统计并手动清空缓存。

## 技术栈

- 后端：FastAPI
- 前端：Vanilla JavaScript + Bootstrap 5 + ECharts
- 集群交互：SLURM 命令行 + SSH + paramiko
- 数据来源：`sinfo`、`squeue`、`scontrol`、`/proc/*`、cgroup、NUMA 统计

## 运行要求

- Linux 登录节点
- Python 3.6+
- 节点可以执行 `sinfo`、`squeue`、`scontrol`、`sbatch`
- 登录节点可以 SSH 到计算节点
- 浏览器可以直接访问服务，或通过 SSH 隧道访问

## 安装与启动

### 1. 获取代码

```bash
git clone https://github.com/Finanfund/SLURM-Dashboard-Release.git
cd SLURM-Dashboard-Release
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

建议至少配置下面几项：

```bash
export DASHBOARD_HOST=0.0.0.0
export DASHBOARD_PORT=8000
export DASHBOARD_PASSWORD='your-strong-password'
export DASHBOARD_CLUSTER_NAME='My SLURM Cluster'
export FILE_BROWSER_ROOT="$HOME"
```

可选的 conda 启动配置：

```bash
export DASHBOARD_CONDA_ACTIVATE=/opt/miniforge/bin/activate
export DASHBOARD_CONDA_ENV=/path/to/your/conda-env
```

### 4. 启动方式

推荐使用启动脚本：

```bash
bash launch.sh --password your-strong-password
```

指定端口：

```bash
bash launch.sh 9000 --password your-strong-password
```

前台调试：

```bash
python start.py --fg --port 8000
```

直接运行：

```bash
python app.py --host 0.0.0.0 --port 8000
```

### 5. 停止与查看状态

```bash
bash launch.sh status
bash launch.sh stop
bash launch.sh restart 9000
python stop.py
```

## 访问方式

### 集群内直接访问

```text
http://<login-node-host>:<port>
```

### 本地通过 SSH 隧道访问

```bash
ssh -N -L <port>:<login-node-host>:<port> <user>@<gateway>
```

然后在浏览器打开：

```text
http://localhost:<port>
```

## 首次启动后的建议配置

打开页面后，在设置中建议补全：

- 本机集群用户名：启用实时日志采集
- 历史任务追踪用户：启用历史任务归档
- 刷新间隔：决定采样频率
- 图表显示时长：决定前端图表窗口
- 缓存大小或保留日期：决定历史缓存策略
- 是否记录 NUMA 趋势：启用后会增加额外采样开销

发布版默认不会替你填入任何个人用户名，所以实时日志和历史追踪默认是关闭态，属于正常行为。

## 目录说明

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
├── static/
└── _node_collect.sh
```

## 运行产物

以下内容属于运行时文件，不建议提交到仓库：

- `.cache/`
- `server.log`
- `user_settings.json`
- `.dashboard.pid`
- `.dashboard_password`

这些文件已经在 `.gitignore` 中忽略。

## 常见问题

### 页面打不开

- 确认进程已启动
- 确认端口未被其他程序占用
- 确认防火墙或 SSH 隧道配置正确

### 集群页面没有数据

- 确认登录节点能执行 `sinfo` 和 `squeue`
- 确认登录节点能 SSH 到计算节点
- 确认节点上可以访问 `/proc` 和 cgroup 信息

### 历史任务为空

- 在设置中填写需要追踪的用户名
- 历史任务不会自动回填未追踪时期的旧任务

### 日志不刷新

- 确认设置里的“本机集群用户名”填写正确
- 确认作业 stdout/stderr 路径可读
- 对非 Python 程序可使用 `stdbuf -oL`

### 文件浏览访问被拒绝

- 检查 `FILE_BROWSER_ROOT`
- 浏览器和 API 只能访问这个根目录之下的内容

## 安全建议

- 不要直接暴露到公网，优先使用 SSH 隧道
- 一定要显式设置 `DASHBOARD_PASSWORD`
- 将 `FILE_BROWSER_ROOT` 收紧到合适范围
- 生产环境建议使用专用服务账户运行

## 发布说明

当前版本说明见 `RELEASE_NOTES.md`。