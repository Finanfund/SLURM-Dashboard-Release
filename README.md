# SLURM HPC Dashboard — 任务管理器

一个功能丰富的 **SLURM 集群监控与管理 Web 面板**，提供实时集群状态、作业管理、文件浏览、历史任务追踪等功能。

> 🌟 **一键部署，开箱即用！** 只需解压到集群登录节点，运行 `bash launch.sh` 即可！

---

## ✨ 功能亮点

| 功能 | 说明 |
|------|------|
| 📊 **集群实时监控** | CPU、内存使用率实时图表，节点状态一目了然 |
| 📋 **作业管理** | 查看、搜索、取消 SLURM 作业，支持已结束任务显示 |
| 📁 **文件浏览器** | 浏览、编辑、上传、下载集群文件，支持收藏夹 |
| 📜 **实时日志** | 查看运行中作业的 stdout/stderr，自动刷新 |
| 📈 **历史任务** | 追踪指定用户的任务历史记录 |
| 🚀 **一键提交** | 直接从文件浏览器提交 sbatch/bash 脚本 |
| 🎨 **现代 UI** | 玻璃态深色主题，响应式设计，ECharts 图表 |

---

## 📋 前置要求

- **Linux 集群登录节点**（需要运行 SLURM 的环境）
- **Python 3.6+**（推荐 3.9+）
- **SSH 免密登录**到集群计算节点（用于采集节点数据）
- **网络访问**：本地机器需能通过 SSH 隧道连接到集群

> ⚠️ 本工具需要部署在 **SLURM 集群的登录节点** 上。它通过 SSH 连接到各计算节点采集 CPU/内存数据，通过 SLURM 命令（squeue、sinfo、scontrol）获取作业信息。

---

## 🚀 快速开始（5分钟部署）

### 第一步：上传到集群

将本文件夹上传到集群登录节点的任意目录：

```bash
# 从本地电脑上传到集群（替换 <user> 和 <cluster> 为你的信息）
scp -r SLURM-Dashboard-Release <user>@<cluster>:~/slurm-dashboard/
```

### 第二步：安装依赖

SSH 登录到集群，安装 Python 依赖：

```bash
ssh <user>@<cluster>
cd ~/slurm-dashboard

# 方式一：使用 pip 直接安装（推荐）
pip install -r requirements.txt

# 方式二：使用 conda 环境
conda create -n dashboard python=3.9 -y
conda activate dashboard
pip install -r requirements.txt
```

### 第三步：配置 SSH 免密登录

确保可以从登录节点 SSH 到计算节点（无密码）：

```bash
# 检查是否已经可以免密登录计算节点
ssh <compute-node-name> hostname

# 如果需要设置免密登录：
ssh-keygen -t rsa -N ""  # 一路回车
ssh-copy-id <compute-node-name>
```

> 💡 通常 HPC 集群已配置好节点间免密 SSH，可以跳过此步。

### 第四步：启动服务

```bash
# 使用一键启动脚本（推荐，自动分配随机端口）
bash launch.sh

# 或指定端口和密码
bash launch.sh 9090 --password mypassword

# 也可以直接用 Python 启动
python app.py --port 9090
```

> 📝 **首次启动**会提示你设置访问密码，密码会保存在 `.dashboard_password` 文件中。后续重启会自动使用已保存的密码。

启动成功后，终端会显示分配的端口号和访问信息。

### 第五步：访问面板

**方式一：SSH 隧道（推荐，远程访问）**

在你的 **本地电脑** 终端执行：

```bash
# 建立 SSH 隧道（替换 <user>、<cluster>、<port> 为实际值）
ssh -N -L <port>:localhost:<port> <user>@<cluster>
```

然后在浏览器打开：**http://localhost:\<port\>**

**方式二：集群内网直接访问**

如果你在集群内网环境中，直接在浏览器打开：

```
http://<login-node-hostname>:<port>
```

**登录密码**：首次启动时设置的密码。可通过 `bash launch.sh --password newpass` 修改。

---

## ⚙️ 首次使用配置

登录后，点击右上角 **⚙ 设置** 按钮，配置以下重要选项：

| 设置项 | 说明 | 建议值 |
|--------|------|--------|
| **本机集群用户名** | 你的集群用户名，用于实时采集作业日志 | 你的用户名 |
| **历史追踪用户** | 需要追踪历史任务的用户名（逗号分隔） | 你的用户名 |
| **刷新间隔** | 数据采集频率（秒） | 10（可设为 1-300） |
| **历史窗口** | 图表显示的历史数据时长（分钟） | 60 |

> ⚠️ **重要**：首次使用请务必在设置中填写 **本机集群用户名** 和 **历史追踪用户**，否则实时日志采集和历史任务追踪功能不会生效！

---

## 📖 功能说明

### 🖥️ 集群状态

- 显示所有 SLURM 分区和节点的实时状态
- 点击节点卡片查看 CPU/内存使用率的实时历史图表
- 已完成的作业会在对应节点上以灰色显示

### 📋 作业列表

- 显示所有 SLURM 作业（RUNNING、PENDING 等）
- 支持搜索、排序、按状态过滤
- 点击作业查看详细信息和实时 CPU/内存图表
- 可查看 stdout/stderr 输出日志
- 已结束的作业会显示在"已结束"过滤中

### 📁 文件浏览器

- 浏览集群文件系统
- 编辑文本文件（语法高亮）
- 上传/下载文件和文件夹
- 收藏常用目录
- **一键提交**：直接对 `.sh` 文件执行 sbatch 或 bash

### 📜 实时日志

- 打开运行中的作业详情时，stdout/stderr 会自动实时刷新
- 支持切换 stdout 和 stderr 视图
- 自动滚动到日志底部（可手动暂停）

> 💡 **关于 Python 输出缓冲**：通过本面板提交的 sbatch 作业会自动添加 `PYTHONUNBUFFERED=1` 环境变量，禁用 Python 的输出缓冲，确保日志实时更新。对于非 Python 程序，建议在 sbatch 脚本中使用 `stdbuf -oL command` 来禁用行缓冲。

### 📈 历史任务

- 自动记录设置中追踪用户的已完成任务
- 可查看历史任务的详细信息和日志
- 支持搜索和过滤

### ⚙️ 设置

- 调整刷新间隔、历史窗口等参数
- 管理缓存（大小限制、日期保留、清除）
- 配置文件收藏夹

---

## 🛠️ 管理命令

```bash
# 启动
bash launch.sh                         # 随机端口（首次运行会提示设置密码）
bash launch.sh 9090                    # 指定端口
bash launch.sh --password secret       # 设置/修改密码
bash launch.sh 9090 --password secret  # 指定端口 + 密码

# 管理
bash launch.sh stop                    # 停止服务
bash launch.sh restart                 # 重启服务
bash launch.sh status                  # 查看状态

# 查看日志
tail -f server.log

# 也可以前台运行（调试用）
python start.py --fg --port 8000
```

**环境变量**（可选）：

```bash
export DASHBOARD_PORT=8000           # 端口号
export DASHBOARD_PASSWORD=mypass     # 访问密码
export DASHBOARD_HOST=0.0.0.0       # 监听地址
export DASHBOARD_CLUSTER_NAME="My Cluster"  # 集群名称
export FILE_BROWSER_ROOT=/home/user  # 文件浏览器根目录
```

---

## 🔧 高级配置

### 修改默认密码

三种方式（任选其一）：

```bash
# 方式一：启动参数
bash launch.sh --password mypassword

# 方式二：环境变量
export DASHBOARD_PASSWORD=mypassword
bash launch.sh

# 方式三：修改 config.py
# 编辑 ACCESS_PASSWORD 行
```

### 修改文件浏览器根目录

编辑 `config.py`：

```python
FILE_BROWSER_ROOT = "/your/home/directory"
```

或设置环境变量：

```bash
export FILE_BROWSER_ROOT=/home/myuser
```

### Conda 环境配置

如果你的集群使用 conda，编辑 `launch.sh` 头部：

```bash
CONDA_ENV="/path/to/your/conda/env"
CONDA_ACTIVATE="/path/to/conda/bin/activate"
```

---

## 📁 目录结构

```
slurm-dashboard/
├── app.py              # FastAPI 应用（路由、WebSocket、认证）
├── collector.py        # 数据采集模块（SSH、SLURM、CPU/内存）
├── config.py           # 全局配置
├── start.py            # Python 启动脚本
├── stop.py             # Python 停止脚本
├── launch.sh           # Bash 一键启动脚本（推荐）
├── requirements.txt    # Python 依赖
├── README.md           # 本文件
├── templates/
│   ├── index.html      # 主页面
│   └── login.html      # 登录页面
└── static/
    ├── css/style.css   # 样式表
    └── js/app.js       # 前端逻辑
```

运行后自动生成：
- `server.log` — 服务日志
- `user_settings.json` — 用户设置
- `.dashboard.pid` — 进程 ID 文件
- `.cache/` — 历史数据缓存

---

## ❓ 常见问题

### Q: 启动时报 "FastAPI not found"

安装依赖：`pip install -r requirements.txt`

### Q: 访问不了页面

1. 确认服务已启动：`bash launch.sh status`
2. 确认 SSH 隧道已建立：`ssh -N -L <port>:localhost:<port> <user>@<cluster>`
3. 确认端口正确且未被占用

### Q: 节点数据显示不全

确保登录节点可以 SSH 到所有计算节点（免密登录）：
```bash
ssh <node-name> hostname  # 应该不需要输入密码
```

### Q: stdout 日志更新不及时

- 通过本面板提交的 sbatch 脚本会自动禁用 Python 缓冲
- 已有的作业可以在脚本中添加 `export PYTHONUNBUFFERED=1`
- 非 Python 程序使用 `stdbuf -oL your_command`

### Q: 中文显示乱码

确保终端编码为 UTF-8：
```bash
export LANG=en_US.UTF-8
export PYTHONIOENCODING=utf-8
```

### Q: 如何修改密码？

```bash
bash launch.sh --password yourpassword
# 或设置环境变量
export DASHBOARD_PASSWORD=yourpassword
# 密码保存在 .dashboard_password 文件中，也可以直接编辑该文件
```

---

## 🔒 安全提示

- 本工具设计用于 **集群内部网络**，建议通过 SSH 隧道访问
- 请勿将端口直接暴露到公网
- 定期修改访问密码
- 文件浏览器有根目录限制，仅能访问 `FILE_BROWSER_ROOT` 下的文件

---

## 📌 技术栈

- **后端**：FastAPI + Uvicorn + Paramiko (SSH)
- **前端**：Bootstrap 5.3.3 + ECharts 5.5.0 + Vanilla JavaScript
- **通信**：WebSocket（实时数据推送）+ REST API
- **集群**：SLURM（squeue、sinfo、scontrol、sbatch）

---

## 📄 版本信息

- **版本**：v15
- **Python 要求**：3.6+（推荐 3.9+）
- **测试环境**：SLURM 23.11.4, Python 3.6.8 / 3.9+

---

*如有问题或建议，欢迎反馈！*
