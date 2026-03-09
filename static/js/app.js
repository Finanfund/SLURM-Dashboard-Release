/* SLURM Dashboard - Frontend Application v10 */
"use strict";

var S = {
    ws: null, data: null,
    activeTab: "cluster",
    selectedNode: null,
    jobSortCol: "job_id", jobSortAsc: true, jobFilter: "all",
    filePath: "", fileSortCol: "name", fileSortAsc: true,
    editingFile: null, editDirty: false, uploadFiles: [],
    historyDuration: 3600,
    refreshInterval: 10,
    charts: {},
    currentJobId: null,
    pollTimer: null, wsConnected: false,
    showFolderSizes: false,
    fileViewMode: "full",
    expandedPartitions: {},
    showJobCurves: false,
    maxCacheMB: 100,
    cacheRetainDate: "",
    bookmarks: [],
    bookmarkViewActive: false,
    historyTrackUsers: "",
    clusterUsername: "",
    _activeLogType: "stdout",
    _historyJobs: [],
    _clusterRendered: false,
    _lastPartKey: "",
    _settingsSaveTimer: null,
    _nodeChartLoading: false,
    _jobChartLoading: false,
    _lastChartRefresh: 0,
    _nodeChartData: null,
    _nodeChartJobData: {},
    _nodeChartSeriesCount: 0,
    _jobModalChartData: null,
    _jobModalNumCpus: 1,
    _jobIsFinished: false,
    _jobFullHistory: false,
    _logAutoFollow: true,
    numaTrackEnabled: false,
    _numaAnalysisLoading: false,
    _jobMemNumaMode: false
};

/* ── Init ── */
document.addEventListener("DOMContentLoaded", function() {
    loadSettingsFromServer();
    fetchSnapshot();
    connectWS();
    startPolling();
    document.querySelectorAll("#mainTabs .nav-link").forEach(function(el) {
        el.addEventListener("shown.bs.tab", function(e) {
            S.activeTab = e.target.dataset.tab;
            if (S.activeTab === "files" && !S.filePath) browsePath("");
            if (S.activeTab === "jobs") renderJobs();
            if (S.activeTab === "cluster") renderCluster();
            if (S.activeTab === "history") loadHistoryJobs();
        });
    });
    var dz = document.getElementById("dropZone");
    if (dz) {
        dz.addEventListener("dragover", function(e) { e.preventDefault(); dz.classList.add("border-primary"); });
        dz.addEventListener("dragleave", function() { dz.classList.remove("border-primary"); });
        dz.addEventListener("drop", function(e) { e.preventDefault(); dz.classList.remove("border-primary"); handleFileSelect(e.dataTransfer.files); });
        dz.addEventListener("click", function() { document.getElementById("uploadFileInput").click(); });
    }
    document.addEventListener("keydown", function(e) {
        if ((e.ctrlKey || e.metaKey) && e.key === "s" && S.editingFile) { e.preventDefault(); editorSave(); }
    });
    // Fix job charts: draw after modal fully visible
    var jobModal = document.getElementById("jobModal");
    if (jobModal) {
        jobModal.addEventListener("shown.bs.modal", function() {
            if (S._pendingJobChartData) {
                drawJobCharts(S._pendingJobChartData.data, S._pendingJobChartData.numCpus, false);
                S._pendingJobChartData = null;
            }
        });
    }
});

window.onerror = function(msg, url, line) { console.error("[Err]", msg, url, line); };

/* ── Settings ── */
function loadSettingsFromServer() {
    fetch("/api/settings").then(function(r) { return r.json(); }).then(function(s) {
        if (s.historyDurationMin) {
            S.historyDuration = s.historyDurationMin * 60;
            var el = document.getElementById("inputHistoryMin");
            if (el) el.value = s.historyDurationMin;
        }
        if (s.refreshIntervalSec) {
            S.refreshInterval = s.refreshIntervalSec;
            var el2 = document.getElementById("inputRefreshSec");
            if (el2) el2.value = s.refreshIntervalSec;
        }
        if (s.showJobCurves) S.showJobCurves = true;
        if (s.showFolderSizes) S.showFolderSizes = true;
        if (typeof s.maxCacheMB === "number") S.maxCacheMB = s.maxCacheMB;
        if (s.cacheRetainDate) S.cacheRetainDate = s.cacheRetainDate;
        if (Array.isArray(s.bookmarks)) S.bookmarks = s.bookmarks;
        if (s.historyTrackUsers) S.historyTrackUsers = s.historyTrackUsers;
        if (s.clusterUsername) S.clusterUsername = s.clusterUsername;
        if (typeof s.numaTrackEnabled === "boolean") S.numaTrackEnabled = s.numaTrackEnabled;
    }).catch(function() {});
}

/* 加载缓存统计信息 */
function loadCacheStats() {
    fetch("/api/cache/stats").then(function(r) { return r.json(); }).then(function(d) {
        var el = document.getElementById("cacheStatsText");
        if (el) {
            var parts = [];
            parts.push("磁盘:" + d.disk_size_mb + "MB");
            parts.push("批次文件:" + d.batch_files);
            parts.push("数据点:" + d.total_points);
            if (d.first_timestamp > 0) {
                parts.push("起始:" + new Date(d.first_timestamp * 1000).toLocaleDateString());
            }
            el.textContent = parts.join(" | ");
        }
    }).catch(function() {});
}

/* 清除所有缓存 */
function clearAllCache() {
    if (!confirm("确定清除所有历史数据缓存？\n这将清除服务器端所有节点和任务的历史数据。")) return;
    fetch("/api/cache/clear", {method: "POST"})
        .then(function(r) { return r.json(); })
        .then(function(d) {
            // 同时清除客户端缓存
            S._nodeChartData = null;
            S._nodeChartJobData = {};
            S._nodeChartSeriesCount = 0;
            S._jobModalChartData = null;
            showToast(d.message || "缓存已清除");
            loadCacheStats();
        })
        .catch(function(e) { alert("清除缓存失败: " + e); });
}

function saveSettingsToServer(settings) {
    fetch("/api/settings", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(settings)
    }).catch(function() {});
}

function onHistoryDurationChange(val) {
    var mins = parseInt(val) || 60;
    if (mins < 1) mins = 1;
    if (mins > 1440) mins = 1440;
    S.historyDuration = mins * 60;
    saveSettingsToServer({historyDurationMin: mins});
    // Immediately reload charts with smooth transition
    if (S.selectedNode) loadNodeChart(S.selectedNode);
}

function onRefreshIntervalChange(val) {
    var secs = parseInt(val) || 10;
    if (secs < 1) secs = 1;
    if (secs > 300) secs = 300;
    S.refreshInterval = secs;
    setRefresh(secs);
    saveSettingsToServer({refreshIntervalSec: secs});
}

function openSettings() {
    var el1 = document.getElementById("settHistoryMin");
    var el2 = document.getElementById("settRefreshSec");
    var el3 = document.getElementById("settShowJobCurves");
    var el4 = document.getElementById("settShowFolderSizes");
    var el5 = document.getElementById("settMaxCacheMB");
    var el6 = document.getElementById("settRetainDate");
    if (el1) el1.value = Math.round(S.historyDuration / 60);
    if (el2) el2.value = S.refreshInterval;
    if (el3) el3.checked = S.showJobCurves;
    if (el4) el4.checked = S.showFolderSizes;
    if (el5) el5.value = S.maxCacheMB;
    if (el6) el6.value = S.cacheRetainDate || "";
    var el7 = document.getElementById("settHistoryTrackUsers");
    if (el7) el7.value = S.historyTrackUsers || "";
    var el8 = document.getElementById("settClusterUsername");
    if (el8) el8.value = S.clusterUsername || "";
    var el9 = document.getElementById("settNumaTrackEnabled");
    if (el9) el9.checked = S.numaTrackEnabled;
    // 灰化逻辑：maxCacheMB > 0 时，日期输入框禁用
    updateRetainDateState();
    if (el5) el5.addEventListener("input", updateRetainDateState);
    // 加载缓存统计
    loadCacheStats();
    new bootstrap.Modal(document.getElementById("settingsModal")).show();
}

function updateRetainDateState() {
    var mbEl = document.getElementById("settMaxCacheMB");
    var dateGroup = document.getElementById("retainDateGroup");
    if (mbEl && dateGroup) {
        var mbVal = parseInt(mbEl.value) || 0;
        if (mbVal > 0) {
            dateGroup.style.opacity = "0.4";
            dateGroup.querySelector("input").disabled = true;
        } else {
            dateGroup.style.opacity = "1";
            dateGroup.querySelector("input").disabled = false;
        }
    }
}

function saveSettings() {
    var mins = parseInt(document.getElementById("settHistoryMin").value) || 60;
    var secs = parseInt(document.getElementById("settRefreshSec").value) || 10;
    var jcurves = document.getElementById("settShowJobCurves").checked;
    var fsizes = document.getElementById("settShowFolderSizes").checked;
    var maxMB = parseInt(document.getElementById("settMaxCacheMB").value) || 0;
    var retainDate = document.getElementById("settRetainDate").value || "";
    var trackUsers = (document.getElementById("settHistoryTrackUsers").value || "").trim();
    var clusterUser = (document.getElementById("settClusterUsername").value || "").trim();
    var numaTrack = document.getElementById("settNumaTrackEnabled").checked;

    S.historyDuration = mins * 60;
    S.refreshInterval = secs;
    S.showJobCurves = jcurves;
    S.showFolderSizes = fsizes;
    S.maxCacheMB = maxMB;
    S.cacheRetainDate = retainDate;
    S.historyTrackUsers = trackUsers;
    S.clusterUsername = clusterUser;
    S.numaTrackEnabled = numaTrack;

    // Update navbar inputs
    document.getElementById("inputHistoryMin").value = mins;
    document.getElementById("inputRefreshSec").value = secs;

    setRefresh(secs);
    saveSettingsToServer({
        historyDurationMin: mins,
        refreshIntervalSec: secs,
        showJobCurves: jcurves,
        showFolderSizes: fsizes,
        maxCacheMB: maxMB,
        cacheRetainDate: retainDate,
        historyTrackUsers: trackUsers,
        clusterUsername: clusterUser,
        numaTrackEnabled: numaTrack
    });

    // Reload chart with new durations
    if (S.selectedNode) loadNodeChart(S.selectedNode);

    bootstrap.Modal.getInstance(document.getElementById("settingsModal")).hide();

    // Brief visual feedback
    showToast("设置已保存");
}

function showToast(msg) {
    var t = document.createElement("div");
    t.className = "position-fixed bottom-0 end-0 p-3";
    t.style.zIndex = "9999";
    t.innerHTML = '<div class="toast show align-items-center text-bg-success border-0" role="alert"><div class="d-flex"><div class="toast-body">' + msg + '</div></div></div>';
    document.body.appendChild(t);
    setTimeout(function() { t.style.transition = "opacity 0.5s"; t.style.opacity = "0"; setTimeout(function() { t.remove(); }, 500); }, 1500);
}

/* ── Polling ── */
function fetchSnapshot() {
    fetch("/api/snapshot").then(function(r) { return r.json(); }).then(function(d) {
        if (d && !d.error) { S.data = d; updateUI(); }
    }).catch(function(e) { console.warn("[Poll]", e); });
}
function startPolling() {
    if (S.pollTimer) clearInterval(S.pollTimer);
    S.pollTimer = setInterval(function() { if (!S.wsConnected) fetchSnapshot(); }, S.refreshInterval * 1000);
}

/* ── WebSocket ── */
function connectWS() {
    try {
        var proto = location.protocol === "https:" ? "wss:" : "ws:";
        S.ws = new WebSocket(proto + "//" + location.host + "/ws");
        S.ws.onopen = function() {
            S.wsConnected = true;
            setStatusBadge("success", "bi-wifi", "已连接");
        };
        S.ws.onclose = function() {
            S.wsConnected = false;
            setStatusBadge("warning", "bi-arrow-repeat", "轮询中");
            setTimeout(connectWS, 5000);
        };
        S.ws.onerror = function() { S.wsConnected = false; };
        S.ws.onmessage = function(e) {
            try { S.data = JSON.parse(e.data); updateUI(); } catch(err) { console.error("[WS updateUI]", err); }
        };
    } catch(e) { S.wsConnected = false; }
}
function setStatusBadge(cls, icon, text) {
    var el = document.getElementById("wsStatus");
    el.className = "badge bg-" + cls + (cls === "warning" ? " text-dark" : "");
    el.innerHTML = '<i class="bi ' + icon + '"></i> ' + text;
}
function setRefresh(v) {
    var val = parseInt(v);
    if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify({type:"set_interval", value:val}));
    if (S.pollTimer) clearInterval(S.pollTimer);
    S.pollTimer = setInterval(function() { if (!S.wsConnected) fetchSnapshot(); }, val * 1000);
}
function manualRefresh() { fetchSnapshot(); }

/* ── Server Controls ── */
function togglePause() {
    var isPaused = S.data && S.data._server_paused;
    fetch("/api/server/" + (isPaused ? "resume" : "pause"), {method:"POST"}).then(function(r) { return r.json(); }).then(function(d) {
        var btn = document.getElementById("btnPause");
        if (d.status === "paused") { btn.innerHTML = '<i class="bi bi-play-fill"></i>'; btn.className = "btn btn-warning btn-sm"; }
        else { btn.innerHTML = '<i class="bi bi-pause-fill"></i>'; btn.className = "btn btn-outline-warning btn-sm"; }
    });
}
function stopServer() {
    if (!confirm("确定停止服务器？")) return;
    fetch("/api/server/stop", {method:"POST"});
}

/* ── UI Update ── */
function updateUI() {
    if (!S.data) return;
    try { updateSummary(); } catch(e) { console.error("[updateSummary]", e); }
    if (S.data._server_paused) {
        var btn = document.getElementById("btnPause");
        if (btn) { btn.innerHTML = '<i class="bi bi-play-fill"></i>'; btn.className = "btn btn-warning btn-sm"; }
    }
    try {
        if (S.activeTab === "cluster") renderCluster();
        else if (S.activeTab === "jobs") renderJobs();
    } catch(e) { console.error("[renderTab]", e); }
    // 自动刷新打开的图表（节点详情/任务详情）—— 独立 try-catch 确保始终执行
    try { autoRefreshCharts(); } catch(e) { console.error("[autoRefreshCharts]", e); }
}
function updateSummary() {
    var s = S.data.summary; if (!s) return;
    setText("sNodes", s.total_nodes); setText("sRunning", s.running_jobs); setText("sPending", s.pending_jobs);
    var cpuPct = s.total_cpus > 0 ? Math.round(s.alloc_cpus / s.total_cpus * 100) : 0;
    var memPct = s.total_mem_gb > 0 ? Math.round(s.used_mem_gb / s.total_mem_gb * 100) : 0;
    setBar("sCpuBar", cpuPct, s.alloc_cpus + "/" + s.total_cpus + " (" + cpuPct + "%)");
    setBar("sMemBar", memPct, s.used_mem_gb.toFixed(0) + "/" + s.total_mem_gb.toFixed(0) + "G (" + memPct + "%)");
    if (S.data._collect_time_ms) setText("sCollect", S.data._collect_time_ms + "ms");
}
function setText(id, v) { var el = document.getElementById(id); if (el) el.textContent = v; }
function setBar(id, pct, label) {
    var el = document.getElementById(id); if (!el) return;
    el.style.width = pct + "%"; el.textContent = label;
}

/* ── 自动刷新打开的图表（无闪烁，增量追加） ── */
function autoRefreshCharts() {
    // 增量追加新数据点到节点图表（节点详情面板打开时）
    if (S.selectedNode && S.activeTab === "cluster" && S._nodeChartData) {
        try { appendNodeChartPoint(); } catch(e) { console.error("[appendNodeChartPoint]", e); }
    }
    // 增量追加新数据点到任务图表（任务模态框打开时）
    var jobModal = document.getElementById("jobModal");
    // 自动刷新日志输出（运行中任务，模态框打开时）
    if (S.currentJobId && jobModal && jobModal.classList.contains("show") && !S._jobIsFinished) {
        try { loadJobLog(null, true); } catch(e) { console.error("[autoRefreshLog]", e); }
    }
    if (S.currentJobId && jobModal && jobModal.classList.contains("show") && !S._jobIsFinished && !S._jobFullHistory) {
        // 如果 _jobModalChartData 为 null（初始加载中），尝试从 WS 数据直接初始化
        if (!S._jobModalChartData) {
            S._jobModalChartData = [];
        }
        try { appendJobChartPoint(); } catch(e) { console.error("[appendJobChartPoint]", e); }
    }
}

/* 从 WebSocket 推送的数据中提取最新点，追加到节点图表缓存 */
function appendNodeChartPoint() {
    if (!S.selectedNode || !S.data || !S._nodeChartData) return;
    var node = null;
    for (var i = 0; i < S.data.nodes.length; i++) {
        if (S.data.nodes[i].name === S.selectedNode) { node = S.data.nodes[i]; break; }
    }
    if (!node) return;
    var t = S.data.timestamp;
    var last = S._nodeChartData.length > 0 ? S._nodeChartData[S._nodeChartData.length - 1] : null;
    if (last && Math.abs(t - last.t) < 1) return; // 跳过重复时间戳
    // 追加节点数据点
    S._nodeChartData.push({
        t: t, cpu: node.cpu_percent,
        mem_used: node.mem_used_gb, mem_total: node.mem_total_gb
    });
    // 追加任务数据点（显示任务曲线时）
    if (S.showJobCurves && node.jobs && S.data.jobs) {
        node.jobs.forEach(function(jid) {
            var job = null;
            for (var k = 0; k < S.data.jobs.length; k++) {
                if (S.data.jobs[k].job_id === jid) { job = S.data.jobs[k]; break; }
            }
            if (job && job.state === "RUNNING") {
                if (!S._nodeChartJobData[jid]) S._nodeChartJobData[jid] = [];
                S._nodeChartJobData[jid].push({
                    t: t, cpu: job.cpu_percent, mem: job.mem_used_gb, num_cpus: job.num_cpus
                });
            }
        });
    }
    // 裁剪超出时间窗口的旧数据
    var since = Date.now()/1000 - S.historyDuration;
    while (S._nodeChartData.length > 0 && S._nodeChartData[0].t < since) S._nodeChartData.shift();
    for (var jid in S._nodeChartJobData) {
        while (S._nodeChartJobData[jid].length > 0 && S._nodeChartJobData[jid][0].t < since) S._nodeChartJobData[jid].shift();
    }
    // 组装任务历史格式
    var jobHistories = [];
    if (S.showJobCurves) {
        for (var jid2 in S._nodeChartJobData) {
            if (S._nodeChartJobData[jid2].length > 0) {
                jobHistories.push({jid: jid2, data: S._nodeChartJobData[jid2]});
            }
        }
    }
    drawNodeChart(S._nodeChartData, jobHistories, true);
}

/* 从 WebSocket 推送的数据中提取最新点，追加到任务图表缓存 */
function appendJobChartPoint() {
    if (!S.currentJobId || !S.data) return;
    if (!S._jobModalChartData) S._jobModalChartData = [];
    var j = null;
    if (S.data.jobs) {
        for (var i = 0; i < S.data.jobs.length; i++) {
            if (String(S.data.jobs[i].job_id) === String(S.currentJobId)) { j = S.data.jobs[i]; break; }
        }
    }
    if (!j) return;
    var t = S.data.timestamp;
    if (!t) return;
    var last = S._jobModalChartData.length > 0 ? S._jobModalChartData[S._jobModalChartData.length - 1] : null;
    if (last && Math.abs(t - last.t) < 1) return; // 降低去重阈值避免流水线采集丢数据
    S._jobModalChartData.push({
        t: t, cpu: j.cpu_percent, mem: j.mem_used_gb, num_cpus: j.num_cpus
    });
    var since = Date.now()/1000 - S.historyDuration;
    while (S._jobModalChartData.length > 0 && S._jobModalChartData[0].t < since) S._jobModalChartData.shift();
    drawJobCharts(S._jobModalChartData, S._jobModalNumCpus, true);
    // 同时更新任务信息文本
    var info1 = document.getElementById("jobDetailInfo");
    if (info1) {
        info1.innerHTML =
            '<table class="table table-sm"><tbody>' +
            '<tr><td>用户</td><td>' + j.user + '</td></tr>' +
            '<tr><td>状态</td><td>' + j.state + '</td></tr>' +
            '<tr><td>分区</td><td>' + j.partition + '</td></tr>' +
            '<tr><td>节点</td><td>' + j.nodes + '</td></tr>' +
            '<tr><td>CPU(申请)</td><td>' + j.num_cpus + '核</td></tr>' +
            '<tr><td>运行时间</td><td>' + j.time_used + '</td></tr>' +
            '</tbody></table>';
    }
}

/* ===== CLUSTER VIEW ===== */
function renderCluster() {
    var d = S.data; if (!d) return;
    var parts = d.partitions || [];
    var nodeMap = {}; (d.nodes || []).forEach(function(n) { nodeMap[n.name] = n; });
    var jobMap = {}; (d.jobs || []).forEach(function(j) { jobMap[j.job_id] = j; });
    var key = parts.map(function(p) { return p.name + ":" + (p.node_list||[]).join(","); }).join("|");
    if (!S._clusterRendered || key !== S._lastPartKey) {
        buildClusterDOM(parts, nodeMap, jobMap);
        S._clusterRendered = true;
        S._lastPartKey = key;
    } else {
        updateClusterDOM(parts, nodeMap, jobMap);
    }
}

function buildClusterDOM(parts, nodeMap, jobMap) {
    var html = "";
    parts.forEach(function(p) {
        var cpuPct = p.cpus_total > 0 ? (p.cpus_alloc / p.cpus_total * 100) : 0;
        var nodeRatio = p.nodes_total > 0 ? (p.nodes_alloc / p.nodes_total * 100) : 0;
        var isExpanded = S.expandedPartitions[p.name] !== false;
        html += '<div class="card mb-2 partition-card" data-part="' + p.name + '">';
        html += '<div class="card-header py-1 d-flex justify-content-between align-items-center" style="cursor:pointer" onclick="togglePartition(\'' + p.name + '\')">';
        html += '<div><i class="bi ' + (isExpanded ? 'bi-chevron-down' : 'bi-chevron-right') + ' me-1" id="icon_part_' + p.name + '"></i><strong>' + p.name + '</strong>';
        html += ' <span class="badge bg-secondary">' + p.timelimit + '</span>';
        html += ' <small class="part-node-count text-muted ms-2">' + p.nodes_alloc + '/' + p.nodes_total + ' nodes</small></div>';
        html += '<div class="d-flex gap-2 align-items-center">';
        html += '<small class="part-cpu-count text-muted">CPU ' + p.cpus_alloc + '/' + p.cpus_total + '</small>';
        html += '<div class="part-bar-outer">';
        html += '<div class="part-bar-blue" style="width:' + nodeRatio.toFixed(1) + '%"></div>';
        html += '<div class="part-bar-red" style="width:' + cpuPct.toFixed(1) + '%"></div>';
        html += '<span class="part-bar-label">' + nodeRatio.toFixed(0) + '%N / ' + cpuPct.toFixed(0) + '%C</span>';
        html += '</div></div></div>';
        html += '<div class="collapse' + (isExpanded ? ' show' : '') + '" id="part_' + p.name + '">';
        html += '<div class="card-body p-2">';
        html += '<div class="node-grid">';
        var pnodes = p.node_list || []; var seen = {};
        pnodes.forEach(function(nname) {
            var n = nodeMap[nname]; if (!n || seen[nname]) return; seen[nname] = true;
            html += buildNodeCard(n, jobMap);
        });
        html += '</div>';
        html += '<div class="node-detail-inline d-none" id="nodeDetail_' + p.name + '"></div>';
        html += '</div></div></div>';
    });
    document.getElementById("clusterView").innerHTML = html;
    if (S.selectedNode && S.data) {
        var n = null;
        for (var i = 0; i < S.data.nodes.length; i++) {
            if (S.data.nodes[i].name === S.selectedNode) { n = S.data.nodes[i]; break; }
        }
        if (n) showNodeDetailInline(n);
    }
}

function buildNodeCard(n, jobMap) {
    var stCls = "secondary";
    if (n.state.indexOf("idle") >= 0) stCls = "success";
    else if (n.state.indexOf("mix") >= 0) stCls = "warning";
    else if (n.state.indexOf("alloc") >= 0) stCls = "primary";
    else if (n.state.indexOf("down") >= 0) stCls = "danger";
    var cpuAlloc = n.cpus_total > 0 ? (n.cpus_alloc / n.cpus_total * 100) : 0;
    var cpuReal = Math.min(n.cpu_percent, 100);
    var memUse = n.mem_total_gb > 0 ? (n.mem_used_gb / n.mem_total_gb * 100) : 0;
    var sel = S.selectedNode === n.name ? " node-card-selected" : "";
    var h = '<div class="node-card' + sel + ' border-' + stCls + '" data-node="' + n.name + '" onclick="selectNode(\'' + n.name + '\')">';
    h += '<div class="nc-name">' + n.name + '</div>';
    h += '<div class="nc-bar" title="CPU: alloc=' + cpuAlloc.toFixed(0) + '% real=' + cpuReal.toFixed(0) + '%">';
    h += '<div class="nc-bar-fill nc-blue" style="width:' + cpuAlloc.toFixed(1) + '%"></div>';
    h += '<div class="nc-bar-fill nc-red" style="width:' + cpuReal.toFixed(1) + '%"></div>';
    h += '<span class="nc-bar-text">C ' + n.cpus_alloc + '/' + n.cpus_total + '</span>';
    h += '</div>';
    h += '<div class="nc-bar" title="RAM: ' + memUse.toFixed(0) + '%">';
    h += '<div class="nc-bar-fill nc-cyan" style="width:' + memUse.toFixed(1) + '%"></div>';
    h += '<span class="nc-bar-text">M ' + n.mem_used_gb.toFixed(0) + '/' + n.mem_total_gb.toFixed(0) + 'G</span>';
    h += '</div>';
    var jobTxt = '';
    if (n.jobs && n.jobs.length > 0 && jobMap) {
        var names = n.jobs.map(function(jid) { var j = jobMap[jid]; return j ? j.name.slice(0, 12) : jid; });
        jobTxt = '<div class="nc-jobs">' + esc(names.join(", ")) + '</div>';
    }
    /* 显示最近在该节点上结束的任务（灰色标注） */
    var finishedTxt = '';
    if (jobMap) {
        var finishedNames = [];
        Object.keys(jobMap).forEach(function(jid) {
            var fj = jobMap[jid];
            if (fj.state === "COMPLETED" && fj.nodes && fj.nodes.indexOf(n.name) >= 0) {
                finishedNames.push(fj.name.slice(0, 12));
            }
        });
        if (finishedNames.length > 0) {
            finishedTxt = '<div class="nc-jobs text-muted" style="opacity:0.6;font-style:italic" title="最近结束的任务">' + esc(finishedNames.join(", ")) + ' ✓</div>';
        }
    }
    h += jobTxt + finishedTxt;
    h += '<span class="badge bg-' + stCls + ' nc-state">' + n.state + '</span>';
    h += '</div>';
    return h;
}

function updateClusterDOM(parts, nodeMap, jobMap) {
    var cv = document.getElementById("clusterView");
    parts.forEach(function(p) {
        var pcard = cv.querySelector('[data-part="' + p.name + '"]');
        if (!pcard) return;
        var cpuPct = p.cpus_total > 0 ? (p.cpus_alloc / p.cpus_total * 100) : 0;
        var nodeRatio = p.nodes_total > 0 ? (p.nodes_alloc / p.nodes_total * 100) : 0;
        var nc = pcard.querySelector(".part-node-count");
        if (nc) nc.textContent = p.nodes_alloc + "/" + p.nodes_total + " nodes";
        var cc = pcard.querySelector(".part-cpu-count");
        if (cc) cc.textContent = "CPU " + p.cpus_alloc + "/" + p.cpus_total;
        var bb = pcard.querySelector(".part-bar-blue");
        if (bb) bb.style.width = nodeRatio.toFixed(1) + "%";
        var rb = pcard.querySelector(".part-bar-red");
        if (rb) rb.style.width = cpuPct.toFixed(1) + "%";
        var lb = pcard.querySelector(".part-bar-label");
        if (lb) lb.textContent = nodeRatio.toFixed(0) + "%N / " + cpuPct.toFixed(0) + "%C";
        (p.node_list || []).forEach(function(nname) {
            var n = nodeMap[nname]; if (!n) return;
            var card = pcard.querySelector('[data-node="' + nname + '"]');
            if (!card) return;
            var stCls = "secondary";
            if (n.state.indexOf("idle") >= 0) stCls = "success";
            else if (n.state.indexOf("mix") >= 0) stCls = "warning";
            else if (n.state.indexOf("alloc") >= 0) stCls = "primary";
            else if (n.state.indexOf("down") >= 0) stCls = "danger";
            var badge = card.querySelector(".nc-state");
            if (badge) { badge.className = "badge bg-" + stCls + " nc-state"; badge.textContent = n.state; }
            card.className = card.className.replace(/border-\w+/g, "border-" + stCls);
            var cpuAlloc = n.cpus_total > 0 ? (n.cpus_alloc / n.cpus_total * 100) : 0;
            var cpuReal = Math.min(n.cpu_percent, 100);
            var memUse = n.mem_total_gb > 0 ? (n.mem_used_gb / n.mem_total_gb * 100) : 0;
            var bars = card.querySelectorAll(".nc-bar");
            if (bars[0]) {
                var b1 = bars[0].querySelector(".nc-blue"); if (b1) b1.style.width = cpuAlloc.toFixed(1) + "%";
                var b2 = bars[0].querySelector(".nc-red"); if (b2) b2.style.width = cpuReal.toFixed(1) + "%";
                var t1 = bars[0].querySelector(".nc-bar-text"); if (t1) t1.textContent = "C " + n.cpus_alloc + "/" + n.cpus_total;
                bars[0].title = "CPU: alloc=" + cpuAlloc.toFixed(0) + "% real=" + cpuReal.toFixed(0) + "%";
            }
            if (bars[1]) {
                var b3 = bars[1].querySelector(".nc-cyan"); if (b3) b3.style.width = memUse.toFixed(1) + "%";
                var t2 = bars[1].querySelector(".nc-bar-text"); if (t2) t2.textContent = "M " + n.mem_used_gb.toFixed(0) + "/" + n.mem_total_gb.toFixed(0) + "G";
                bars[1].title = "RAM: " + memUse.toFixed(0) + "%";
            }
            var jobsEl = card.querySelector(".nc-jobs");
            if (n.jobs && n.jobs.length > 0 && jobMap) {
                var names = n.jobs.map(function(jid) { var j = jobMap[jid]; return j ? j.name.slice(0, 12) : jid; });
                var txt = names.join(", ");
                if (jobsEl) { jobsEl.textContent = txt; }
                else {
                    var jd = document.createElement("div"); jd.className = "nc-jobs"; jd.textContent = txt;
                    var badgeEl = card.querySelector(".nc-state"); if (badgeEl) card.insertBefore(jd, badgeEl);
                }
            } else if (jobsEl) { jobsEl.textContent = ""; }
        });
    });
    if (S.selectedNode) {
        var nd = null;
        for (var i = 0; i < (S.data.nodes || []).length; i++) {
            if (S.data.nodes[i].name === S.selectedNode) { nd = S.data.nodes[i]; break; }
        }
        if (nd) updateNodeDetailInfo(nd, jobMap);
    }
}

function updateNodeDetailInfo(n, jobMap) {
    var details = document.querySelectorAll(".node-detail-inline:not(.d-none)");
    if (details.length === 0) return;
    var detail = details[0];
    var rows = detail.querySelectorAll("table tr");
    if (rows.length < 5) return;
    var cells = [];
    for (var i = 0; i < rows.length; i++) { cells.push(rows[i].querySelector("td:last-child")); }
    if (cells[0]) cells[0].textContent = n.state;
    if (cells[2]) cells[2].textContent = n.cpus_alloc + "/" + n.cpus_total + " (实际" + n.cpu_percent.toFixed(1) + "%)";
    if (cells[3]) cells[3].textContent = n.mem_used_gb.toFixed(1) + "/" + n.mem_total_gb.toFixed(1) + " GB";
    if (cells[4]) {
        var jobLinks = (n.jobs || []).map(function(jid) {
            var ji = jobMap[jid];
            var lbl = jid;
            if (ji) lbl = jid + ' <small class="text-muted">(' + esc(ji.name) + ' — ' + ji.user + ')</small>';
            return '<a href="#" onclick="openJobDetail(\'' + jid + '\');return false">' + lbl + '</a>';
        }).join("<br>");
        cells[4].innerHTML = jobLinks || "无";
    }
}

function togglePartition(partName) {
    var id = "part_" + partName;
    var el = document.getElementById(id);
    var icon = document.getElementById("icon_" + id);
    if (!el) return;
    if (el.classList.contains("show")) {
        el.classList.remove("show");
        S.expandedPartitions[partName] = false;
        if (icon) icon.className = "bi bi-chevron-right me-1";
    } else {
        el.classList.add("show");
        S.expandedPartitions[partName] = true;
        if (icon) icon.className = "bi bi-chevron-down me-1";
    }
}
function expandAll() {
    S.expandedPartitions = {};
    document.querySelectorAll(".partition-card .collapse").forEach(function(el) { el.classList.add("show"); });
    document.querySelectorAll("[id^='icon_part_']").forEach(function(el) { el.className = "bi bi-chevron-down me-1"; });
}
function collapseAll() {
    if (S.data && S.data.partitions) {
        S.data.partitions.forEach(function(p) { S.expandedPartitions[p.name] = false; });
    }
    document.querySelectorAll(".partition-card .collapse").forEach(function(el) { el.classList.remove("show"); });
    document.querySelectorAll("[id^='icon_part_']").forEach(function(el) { el.className = "bi bi-chevron-right me-1"; });
}

function selectNode(name) {
    if (S.selectedNode === name) { closeNodeDetail(); return; }
    S.selectedNode = name;
    var n = null;
    if (S.data && S.data.nodes) {
        for (var i = 0; i < S.data.nodes.length; i++) {
            if (S.data.nodes[i].name === name) { n = S.data.nodes[i]; break; }
        }
    }
    if (!n) return;
    document.querySelectorAll(".node-card").forEach(function(el) { el.classList.remove("node-card-selected"); });
    showNodeDetailInline(n);
}

function showNodeDetailInline(n) {
    document.querySelectorAll(".node-detail-inline").forEach(function(el) { el.classList.add("d-none"); el.innerHTML = ""; });
    document.querySelectorAll(".node-card").forEach(function(el) { el.classList.remove("node-card-selected"); });
    var allCards = document.querySelectorAll(".node-card");
    allCards.forEach(function(el) { if (el.querySelector(".nc-name") && el.querySelector(".nc-name").textContent === n.name) el.classList.add("node-card-selected"); });
    var partName = null;
    if (S.data && S.data.partitions) {
        for (var i = 0; i < S.data.partitions.length; i++) {
            var p = S.data.partitions[i];
            if (p.node_list && p.node_list.indexOf(n.name) >= 0) { partName = p.name; break; }
        }
    }
    if (!partName) return;
    var detailEl = document.getElementById("nodeDetail_" + partName);
    if (!detailEl) return;
    detailEl.classList.remove("d-none");
    var jobLinks = (n.jobs || []).map(function(jid) {
        var ji = null;
        if (S.data && S.data.jobs) { for (var k = 0; k < S.data.jobs.length; k++) { if (S.data.jobs[k].job_id === jid) { ji = S.data.jobs[k]; break; } } }
        var lbl = jid;
        if (ji) lbl = jid + ' <small class="text-muted">(' + esc(ji.name) + ' — ' + ji.user + ')</small>';
        return '<a href="#" onclick="openJobDetail(\'' + jid + '\');return false">' + lbl + '</a>';
    }).join("<br>");
    detailEl.innerHTML =
        '<div class="card mt-2 border-info">' +
        '<div class="card-header py-1 d-flex justify-content-between">' +
        '<span><i class="bi bi-pc-display me-1"></i>节点: <strong>' + n.name + '</strong></span>' +
        '<button class="btn-close btn-close-white btn-sm" onclick="closeNodeDetail()"></button></div>' +
        '<div class="card-body p-2"><div class="row">' +
        '<div class="col-md-4"><table class="table table-sm mb-0"><tbody>' +
        '<tr><td>状态</td><td>' + n.state + '</td></tr>' +
        '<tr><td>分区</td><td>' + n.partitions + '</td></tr>' +
        '<tr><td>CPU</td><td>' + n.cpus_alloc + '/' + n.cpus_total + ' (实际' + n.cpu_percent.toFixed(1) + '%)</td></tr>' +
        '<tr><td>内存</td><td>' + n.mem_used_gb.toFixed(1) + '/' + n.mem_total_gb.toFixed(1) + ' GB</td></tr>' +
        '<tr><td>任务</td><td>' + (jobLinks || '无') + '</td></tr>' +
        '</tbody></table></div>' +
        '<div class="col-md-8"><div id="nodeChart" style="height:280px"></div>' +
        '<div class="mt-1 text-end"><label class="form-check form-check-inline form-switch mb-0"><input type="checkbox" class="form-check-input" id="toggleJobCurves" ' + (S.showJobCurves ? 'checked' : '') + ' onchange="toggleJobCurves()"><span class="form-check-label small">显示任务曲线 🌈</span></label></div></div>' +
        '</div></div></div>';
    loadNodeChart(n.name);
}

function closeNodeDetail() {
    S.selectedNode = null;
    S._nodeChartData = null;
    S._nodeChartJobData = {};
    S._nodeChartSeriesCount = 0;
    document.querySelectorAll(".node-detail-inline").forEach(function(el) { el.classList.add("d-none"); el.innerHTML = ""; });
    document.querySelectorAll(".node-card").forEach(function(el) { el.classList.remove("node-card-selected"); });
}

function loadNodeChart(name) {
    S._lastChartRefresh = Date.now();
    S._nodeChartData = null;
    S._nodeChartJobData = {};
    S._nodeChartSeriesCount = 0;
    var since = S.historyDuration > 0 ? (Date.now()/1000 - S.historyDuration) : 0;
    fetch("/api/history/node/" + name + "?since=" + since).then(function(r) { return r.json(); }).then(function(d) {
        var nodeData = d.data || [];
        S._nodeChartData = nodeData; // 缓存历史数据
        if (!S.showJobCurves) { drawNodeChart(nodeData, [], false); return; }
        var node = null;
        if (S.data && S.data.nodes) { for (var i = 0; i < S.data.nodes.length; i++) { if (S.data.nodes[i].name === name) { node = S.data.nodes[i]; break; } } }
        if (!node || !node.jobs || node.jobs.length === 0) { drawNodeChart(nodeData, [], false); return; }
        var promises = node.jobs.map(function(jid) {
            return fetch("/api/history/job/" + jid + "?since=" + since)
                .then(function(r) { return r.json(); })
                .then(function(dd) {
                    S._nodeChartJobData[jid] = dd.data || []; // 缓存任务数据
                    return {jid: jid, data: dd.data || []};
                })
                .catch(function() { return {jid: jid, data: []}; });
        });
        Promise.all(promises).then(function(jh) { drawNodeChart(nodeData, jh, false); });
    }).catch(function(e) { console.warn("nodeChart err:", e); });
}

function drawNodeChart(data, jobHistories, incremental) {
    var el = document.getElementById("nodeChart");
    if (!el || !window.echarts) return;
    // Reuse existing chart for smooth animation instead of disposing
    if (!S.charts.nodeChart || S.charts.nodeChart.getDom() !== el) {
        if (S.charts.nodeChart) S.charts.nodeChart.dispose();
        S.charts.nodeChart = echarts.init(el, "dark");
        incremental = false; // 新实例必须完整绘制
    }
    var chart = S.charts.nodeChart;
    var times = data.map(function(p) { return new Date(p.t * 1000).toLocaleTimeString(); });
    var nodeCpus = 1;
    if (S.selectedNode && S.data && S.data.nodes) {
        for (var ni = 0; ni < S.data.nodes.length; ni++) {
            if (S.data.nodes[ni].name === S.selectedNode) { nodeCpus = S.data.nodes[ni].cpus_total || 1; break; }
        }
    }
    var legend = [];
    var series = [];
    // 按 job_id 排序，确保堆叠顺序在刷新间保持一致
    if (jobHistories && jobHistories.length > 1) {
        jobHistories.sort(function(a, b) { return String(a.jid).localeCompare(String(b.jid), undefined, {numeric: true}); });
    }
    if (jobHistories && jobHistories.length > 0) {
        for (var ji = 0; ji < jobHistories.length; ji++) {
            var jh = jobHistories[ji];
            var jcolor = jobIdToColor(jh.jid); // 根据 job_id 生成固定颜色
            var jname = jh.jid;
            if (S.data && S.data.jobs) {
                for (var k = 0; k < S.data.jobs.length; k++) {
                    if (S.data.jobs[k].job_id === jh.jid) {
                        jname = S.data.jobs[k].name.slice(0, 15) + " (" + jh.jid + ")";
                        break;
                    }
                }
            }
            var tsMap = {};
            jh.data.forEach(function(p) { tsMap[Math.round(p.t)] = p; });
            var cpuArr = [];
            for (var ti = 0; ti < data.length; ti++) {
                var nt = Math.round(data[ti].t);
                var found = null;
                for (var dd = 0; dd <= 2; dd++) {
                    if (tsMap[nt + dd]) { found = tsMap[nt + dd]; break; }
                    if (dd > 0 && tsMap[nt - dd]) { found = tsMap[nt - dd]; break; }
                }
                cpuArr.push(found ? +(found.cpu / nodeCpus).toFixed(2) : 0);
            }
            legend.push(jname);
            series.push({
                name: jname, type: "line", stack: "jobCpu",
                areaStyle: {opacity: 0.7, color: jcolor},
                lineStyle: {width: 0.5, color: jcolor},
                itemStyle: {color: jcolor},
                data: cpuArr, smooth: true, symbol: "none",
                emphasis: {focus: "series"}
            });
        }
    }
    legend.unshift("总 CPU%");
    series.push({
        name: "总 CPU%", type: "line",
        data: data.map(function(p) { return p.cpu; }),
        smooth: true, symbol: "none",
        lineStyle: {width: 2.5, color: "#fff", type: "solid"},
        itemStyle: {color: "#fff"},
        z: 10
    });
    legend.push("内存%");
    series.push({
        name: "内存%", type: "line",
        data: data.map(function(p) {
            return p.mem_total > 0 ? +(p.mem_used / p.mem_total * 100).toFixed(1) : 0;
        }),
        yAxisIndex: 1, smooth: true, symbol: "none",
        lineStyle: {width: 2, color: "#0dcaf0", type: "dashed"},
        itemStyle: {color: "#0dcaf0"},
        z: 10
    });
    var hasJobs = jobHistories && jobHistories.length > 0;

    // 增量更新模式：仅更新数据，series结构不变时使用merge模式实现无闪烁
    if (incremental && S._nodeChartSeriesCount === series.length) {
        chart.setOption({
            xAxis: {data: times},
            series: series.map(function(s) { return {data: s.data}; })
        });
    } else {
        // 完整绘制：首次加载、切换节点、或series结构变化
        S._nodeChartSeriesCount = series.length;
        chart.setOption({
            animation: true,
            animationDuration: 0,
            animationDurationUpdate: 300,
            animationEasingUpdate: "cubicInOut",
            tooltip: {trigger: "axis", confine: true, axisPointer: {type: "cross"}},
            legend: {data: legend, type: "scroll", textStyle: {fontSize: 10}, top: 0, selected: {}},
            grid: {left: 55, right: 55, top: hasJobs ? 55 : 35, bottom: 25},
            xAxis: {type: "category", data: times, axisLabel: {fontSize: 9}},
            yAxis: [
                {type: "value", name: "CPU% (节点)", min: 0, max: 100, axisLabel: {formatter: "{value}%"}},
                {type: "value", name: "内存% (节点)", min: 0, max: 100, axisLabel: {formatter: "{value}%"}}
            ],
            series: series
        }, true);
    }
}

function generateRainbow(n) {
    var colors = [];
    for (var i = 0; i < n; i++) {
        colors.push("hsl(" + Math.round(i * 300 / Math.max(n, 1)) + ",75%,55%)");
    }
    return colors;
}

/* 根据 job_id 生成固定颜色（同一 job 始终同色）
 * 使用黄金比例角（~137.5°）分散色相，确保相邻任务颜色差异最大化
 * 全局缓存 _jobColorMap 保持同一 job 始终同色 */
var _jobColorMap = {};
var _jobColorIndex = 0;
var _GOLDEN_ANGLE = 137.508;  /* 黄金角度，最大化色相分散 */
function jobIdToColor(jid) {
    var key = String(jid);
    if (_jobColorMap[key]) return _jobColorMap[key];
    /* 用黄金角度乘以递增索引，保证每个新 job 的色相与已有颜色最大化距离 */
    var hue = (_jobColorIndex * _GOLDEN_ANGLE) % 360;
    _jobColorIndex++;
    var color = "hsl(" + Math.round(hue) + ",75%,55%)";
    _jobColorMap[key] = color;
    return color;
}

function toggleJobCurves() {
    S.showJobCurves = !S.showJobCurves;
    if (S.selectedNode) loadNodeChart(S.selectedNode);
}

/* ===== JOBS ===== */
function setJobFilter(f, btn) {
    S.jobFilter = f;
    document.querySelectorAll("#tabJobs .filter-btns .btn").forEach(function(b) { b.classList.remove("active"); });
    if (btn) btn.classList.add("active");
    renderJobs();
}
function sortJobs(col) {
    if (S.jobSortCol === col) S.jobSortAsc = !S.jobSortAsc;
    else { S.jobSortCol = col; S.jobSortAsc = true; }
    renderJobs();
}
function renderJobs() {
    if (!S.data || !S.data.jobs) return;
    var jobs = S.data.jobs.slice();
    var q = (document.getElementById("jobSearch") ? document.getElementById("jobSearch").value : "").toLowerCase();
    if (q) jobs = jobs.filter(function(j) {
        return j.job_id.indexOf(q) >= 0 || j.name.toLowerCase().indexOf(q) >= 0 || j.user.toLowerCase().indexOf(q) >= 0 || (j.nodes||"").toLowerCase().indexOf(q) >= 0;
    });
    if (S.jobFilter !== "all") jobs = jobs.filter(function(j) { return j.state === S.jobFilter; });
    var col = S.jobSortCol, asc = S.jobSortAsc;
    jobs.sort(function(a, b) {
        var va = a[col], vb = b[col];
        if (typeof va === "number") return asc ? va - vb : vb - va;
        return asc ? String(va||"").localeCompare(String(vb||"")) : String(vb||"").localeCompare(String(va||""));
    });
    var html = "";
    jobs.forEach(function(j) {
        var stCls = j.state === "RUNNING" ? "success" : j.state === "PENDING" ? "warning" : j.state === "COMPLETED" ? "info" : "secondary";
        var stLabel = j.state;
        /* 已结束任务：显示"已结束 @HH:MM:SS"徽章 */
        if (j.state === "COMPLETED" && j.end_time) {
            stLabel = "结束@" + j.end_time;
        }
        var cpuNorm = j.num_cpus > 0 ? (j.cpu_percent / j.num_cpus * 100).toFixed(1) : j.cpu_percent.toFixed(1);
        var nameShort = j.name.length > 25 ? j.name.slice(0, 25) + "..." : j.name;
        var rowStyle = j.state === "COMPLETED" ? 'style="cursor:pointer;opacity:0.65"' : 'style="cursor:pointer"';
        html += '<tr onclick="openJobDetail(\'' + j.job_id + '\')" ' + rowStyle + '>';
        html += '<td>' + j.job_id + '</td>';
        html += '<td title="' + escAttr(j.name) + '">' + esc(nameShort) + '</td>';
        html += '<td>' + j.user + '</td>';
        html += '<td><span class="badge bg-' + stCls + '">' + stLabel + '</span></td>';
        html += '<td>' + j.partition + '</td>';
        html += '<td>' + (j.nodes || "-") + '</td>';
        html += '<td>' + j.num_cpus + '</td>';
        html += '<td>' + cpuNorm + '%</td>';
        html += '<td>' + j.mem_used_gb.toFixed(1) + 'G</td>';
        html += '<td>' + j.time_used + '</td>';
        if (j.state === "COMPLETED") {
            html += '<td><span class="text-muted small">已结束</span></td>';
        } else {
            html += '<td><button class="btn btn-sm btn-outline-danger py-0" onclick="event.stopPropagation();cancelJobDirect(\'' + j.job_id + '\')"><i class="bi bi-x-lg"></i></button></td>';
        }
        html += '</tr>';
    });
    document.getElementById("jobsBody").innerHTML = html;
}

/* ── Job Detail Modal ── */
function openJobDetail(jid) {
    S.currentJobId = jid;
    S._pendingJobChartData = null;
    S._jobModalChartData = null; // 清空缓存，等待新数据加载
    S._jobIsFinished = false;
    S._jobFullHistory = false;
    // S._logAutoFollow 不重置，保留用户上次选择的锁定/跟随状态
    S._numaAnalysisLoading = false;
    S._jobMemNumaMode = false; // 重置 NUMA 图表模式
    var btnFH = document.getElementById("btnFullHistory");
    if (btnFH) btnFH.classList.remove("active");
    /* 日志跟随按钮：恢复为上次的状态 */
    var btnFollow = document.getElementById("btnLogFollow");
    if (btnFollow) {
        if (S._logAutoFollow) {
            btnFollow.innerHTML = '<i class="bi bi-unlock-fill"></i>';
            btnFollow.classList.remove("active");
        } else {
            btnFollow.innerHTML = '<i class="bi bi-lock-fill"></i>';
            btnFollow.classList.add("active");
        }
    }
    /* 隐藏 NUMA 分析结果 */
    var numaResult = document.getElementById("numaAnalysisResult");
    if (numaResult) { numaResult.style.display = "none"; numaResult.innerHTML = ""; }
    /* 重置 NUMA 按钮 */
    var btnNuma = document.getElementById("btnNumaAnalysis");
    if (btnNuma) { btnNuma.disabled = false; btnNuma.innerHTML = '<i class="bi bi-cpu me-1"></i>NUMA'; }
    var j = null;
    if (S.data && S.data.jobs) {
        for (var i = 0; i < S.data.jobs.length; i++) { if (S.data.jobs[i].job_id === jid) { j = S.data.jobs[i]; break; } }
    }
    /* 从历史任务列表查找（归档任务可能不在当前快照中） */
    if (!j && S._historyJobs) {
        for (var i = 0; i < S._historyJobs.length; i++) { if (S._historyJobs[i].job_id === jid) { j = S._historyJobs[i]; break; } }
    }
    if (j && j.state === "COMPLETED") S._jobIsFinished = true;
    /* 已结束任务默认开启全程历史 */
    if (S._jobIsFinished) {
        S._jobFullHistory = true;
        var btnFH = document.getElementById("btnFullHistory");
        if (btnFH) btnFH.classList.add("active");
    }
    document.getElementById("jobModalTitle").textContent = "任务 " + jid + (j ? " — " + j.name : "");
    if (j) {
        var stateDisplay = j.state;
        if (j.state === "COMPLETED" && j.end_time) stateDisplay = '<span class="badge bg-info">已结束@' + j.end_time + '</span>';
        else stateDisplay = j.state;
        document.getElementById("jobDetailInfo").innerHTML =
            '<table class="table table-sm"><tbody>' +
            '<tr><td>用户</td><td>' + j.user + '</td></tr>' +
            '<tr><td>状态</td><td>' + stateDisplay + '</td></tr>' +
            '<tr><td>分区</td><td>' + j.partition + '</td></tr>' +
            '<tr><td>节点</td><td>' + j.nodes + '</td></tr>' +
            '<tr><td>CPU(申请)</td><td>' + j.num_cpus + '核</td></tr>' +
            '<tr><td>运行时间</td><td>' + j.time_used + '</td></tr>' +
            '</tbody></table>';
        document.getElementById("jobDetailInfo2").innerHTML =
            '<table class="table table-sm"><tbody>' +
            '<tr><td>时间限制</td><td>' + j.time_limit + '</td></tr>' +
            '<tr><td>提交时间</td><td>' + j.submit_time + '</td></tr>' +
            '<tr><td>开始时间</td><td>' + j.start_time + '</td></tr>' +
            '<tr><td>工作目录</td><td class="text-break small">' + esc(j.work_dir) + '</td></tr>' +
            '</tbody></table>';
    }

    // Fetch job history（已结束任务直接获取全程历史）
    var since = S._jobIsFinished ? 0 :
        (S.historyDuration > 0 ? (Date.now()/1000 - S.historyDuration) : 0);
    var numCpus = j ? j.num_cpus : 1;
    S._jobModalNumCpus = numCpus;
    S._jobModalChartData = null; // 初始加载中
    fetch("/api/history/job/" + jid + "?since=" + since).then(function(r) { return r.json(); }).then(function(d) {
        var histData = d.data || [];
        S._jobModalChartData = histData; // 缓存历史数据
        // Check if modal is already visible (i.e. DOM is rendered and visible)
        var modalEl = document.getElementById("jobModal");
        var isVisible = modalEl && modalEl.classList.contains("show");
        if (isVisible) {
            // Modal already visible, draw immediately
            drawJobCharts(histData, numCpus, false);
        } else {
            // Store data and wait for shown.bs.modal event to draw
            S._pendingJobChartData = {data: histData, numCpus: numCpus};
        }
    }).catch(function(err) {
        console.warn("Job history fetch error:", err);
        // 即使获取历史失败，也初始化空数组以便 WS 增量更新能工作
        if (!S._jobModalChartData) S._jobModalChartData = [];
    });

    document.getElementById("jobLogContent").textContent = "正在加载 stdout...";
    loadJobLog("stdout");  /* 自动加载 stdout 日志 */
    /* 已结束任务隐藏取消按钮 */
    var cancelBtn = document.getElementById("btnCancelJob");
    if (cancelBtn) cancelBtn.style.display = S._jobIsFinished ? "none" : "";

    /* 智能缓冲区提示：通过 scontrol 检查作业环境变量 */
    updateBufferTip(jid);

    var modal = bootstrap.Modal.getOrCreateInstance(document.getElementById("jobModal"));
    modal.show();
}

function drawJobCharts(data, numCpus, incremental) {
    numCpus = numCpus || 1;
    if (!window.echarts || !data || data.length === 0) return;
    var times = data.map(function(p) { return new Date(p.t * 1000).toLocaleTimeString(); });

    // ── CPU Chart ──
    var cpuEl = document.getElementById("jobCpuChart");
    if (cpuEl && cpuEl.offsetWidth > 0 && cpuEl.offsetHeight > 0) {
        if (!S.charts.jobCpu || S.charts.jobCpu.getDom() !== cpuEl) {
            if (S.charts.jobCpu) S.charts.jobCpu.dispose();
            S.charts.jobCpu = echarts.init(cpuEl, "dark");
            incremental = false;
        }

        // Compute raw and clamped CPU values
        var cpuRaw = [];
        var cpuClamped = [];
        for (var ci = 0; ci < data.length; ci++) {
            var nc = data[ci].num_cpus || numCpus;
            var raw = nc > 0 ? data[ci].cpu / nc : 0;
            cpuRaw.push(raw);
            cpuClamped.push(Math.min(raw, 100));
        }
        // 保存到状态，供 tooltip formatter 引用（避免闭包过时）
        S._jobCpuRaw = cpuRaw;

        // Build series data: mark overload (>110% raw) in red
        var cpuSeriesData = [];
        for (var ci2 = 0; ci2 < cpuClamped.length; ci2++) {
            if (cpuRaw[ci2] > 110) {
                cpuSeriesData.push({value: +cpuClamped[ci2].toFixed(1), itemStyle:{color:"#f55"}});
            } else {
                cpuSeriesData.push(+cpuClamped[ci2].toFixed(1));
            }
        }

        if (incremental) {
            // 增量更新：更新数据 + tooltip（保持格式化函数引用最新 cpuRaw）
            S.charts.jobCpu.setOption({
                xAxis: {data: times},
                tooltip: {trigger: "axis", formatter: function(params) {
                    if (!params || !params[0]) return "";
                    var idx = params[0].dataIndex;
                    var r = S._jobCpuRaw && S._jobCpuRaw[idx] != null ? S._jobCpuRaw[idx] : 0;
                    var txt = params[0].axisValue + "<br/>CPU: " + r.toFixed(1) + "%";
                    if (r > 110) txt += " <span style='color:#f55'>\u26a0 OVERLOAD</span>";
                    return txt;
                }},
                series: [{data: cpuSeriesData}]
            });
        } else {
            // 完整绘制
            S.charts.jobCpu.setOption({
                animation: true, animationDuration: 0, animationDurationUpdate: 300, animationEasingUpdate: "cubicInOut",
                title: {text: "CPU (" + numCpus + "\u6838=100%)", textStyle:{fontSize:12}, left:"center"},
                tooltip: {trigger: "axis", formatter: function(params) {
                    if (!params || !params[0]) return "";
                    var idx = params[0].dataIndex;
                    var r = S._jobCpuRaw && S._jobCpuRaw[idx] != null ? S._jobCpuRaw[idx] : 0;
                    var txt = params[0].axisValue + "<br/>CPU: " + r.toFixed(1) + "%";
                    if (r > 110) txt += " <span style='color:#f55'>\u26a0 OVERLOAD</span>";
                    return txt;
                }},
                grid: {left:55, right:15, top:42, bottom:25},
                xAxis: {type:"category", data:times, axisLabel:{fontSize:9}},
                yAxis: {type:"value", name:"%", min:0, max:100},
                series: [{
                    type: "line", data: cpuSeriesData, smooth: true, symbol: "none",
                    areaStyle: {opacity:0.3}, lineStyle: {color:"#4e8cff"}, itemStyle: {color:"#4e8cff"}
                }]
            }, true);
        }
    }

    // ── Memory Chart (支持 NUMA 堆叠模式) ──
    var memEl = document.getElementById("jobMemChart");
    if (memEl && memEl.offsetWidth > 0 && memEl.offsetHeight > 0) {
        if (!S.charts.jobMem || S.charts.jobMem.getDom() !== memEl) {
            if (S.charts.jobMem) S.charts.jobMem.dispose();
            S.charts.jobMem = echarts.init(memEl, "dark");
            incremental = false;
        }

        // 检测是否有 NUMA 数据（任一数据点包含 numa_local 字段）
        var hasNuma = false;
        for (var ni = 0; ni < data.length; ni++) {
            if (data[ni].numa_local != null) { hasNuma = true; break; }
        }

        // 模式切换时（非NUMA↔NUMA），需要销毁并重建图表
        if (S._jobMemNumaMode !== hasNuma) {
            if (S.charts.jobMem) { S.charts.jobMem.dispose(); S.charts.jobMem = null; }
            S._jobMemNumaMode = hasNuma;
            S.charts.jobMem = echarts.init(memEl, "dark");
            incremental = false;
        }

        // Auto-detect unit: MB if max < 0.5 GB
        var maxMem = 0;
        for (var mi = 0; mi < data.length; mi++) { if (data[mi].mem > maxMem) maxMem = data[mi].mem; }
        var useMB = maxMem < 0.5;
        var memUnit = useMB ? "MB" : "GB";
        var scale = useMB ? 1024 : 1;

        if (hasNuma) {
            // ── NUMA 堆叠模式 ──
            var localData = [];
            var remoteData = [];
            // 预扫描：找第一个NUMA点的比例，用于填充之前的无NUMA点
            var lastLocalRatio = 1.0;
            for (var pre = 0; pre < data.length; pre++) {
                if (data[pre].numa_local != null) {
                    var preTotal = data[pre].numa_local + data[pre].numa_remote;
                    if (preTotal > 0) lastLocalRatio = data[pre].numa_local / preTotal;
                    break;
                }
            }
            for (var nd = 0; nd < data.length; nd++) {
                if (data[nd].numa_local != null) {
                    localData.push(+(data[nd].numa_local * scale).toFixed(useMB ? 1 : 3));
                    remoteData.push(+(data[nd].numa_remote * scale).toFixed(useMB ? 1 : 3));
                    // 更新比例，供后续无NUMA点使用
                    var ndTotal = data[nd].numa_local + data[nd].numa_remote;
                    if (ndTotal > 0) lastLocalRatio = data[nd].numa_local / ndTotal;
                } else {
                    // 无NUMA数据的点：按最近已知比例拆分，避免本地占满
                    localData.push(+(data[nd].mem * lastLocalRatio * scale).toFixed(useMB ? 1 : 3));
                    remoteData.push(+(data[nd].mem * (1 - lastLocalRatio) * scale).toFixed(useMB ? 1 : 3));
                }
            }
            if (incremental) {
                S.charts.jobMem.setOption({
                    xAxis: {data: times},
                    series: [{data: localData}, {data: remoteData}]
                });
            } else {
                S.charts.jobMem.setOption({
                    animation: true, animationDuration: 0, animationDurationUpdate: 300, animationEasingUpdate: "cubicInOut",
                    title: {text: "\u5185\u5b58 (NUMA \u5206\u5e03)", textStyle:{fontSize:12}, left:"center"},
                    tooltip: {trigger:"axis", formatter: function(params) {
                        if (!params || params.length === 0) return "";
                        var t = params[0].axisValue;
                        var local = params[0] ? params[0].value : 0;
                        var remote = params[1] ? params[1].value : 0;
                        var total = (parseFloat(local) + parseFloat(remote)).toFixed(useMB ? 1 : 3);
                        var remotePct = (parseFloat(local) + parseFloat(remote)) > 0
                            ? ((parseFloat(remote) / (parseFloat(local) + parseFloat(remote))) * 100).toFixed(1) : "0.0";
                        var txt = t + "<br/>";
                        txt += "<span style='color:#50c878'>\u25cf</span> \u672c\u5730: " + local + " " + memUnit + "<br/>";
                        txt += "<span style='color:#ff7043'>\u25cf</span> \u8fdc\u7a0b: " + remote + " " + memUnit;
                        if (parseFloat(remotePct) > 30) txt += " <span style='color:#f55'>\u26a0 " + remotePct + "%</span>";
                        else txt += " (" + remotePct + "%)";
                        txt += "<br/>\u603b\u8ba1: " + total + " " + memUnit;
                        return txt;
                    }},
                    legend: {data:["\u672c\u5730\u5185\u5b58","\u8fdc\u7a0b\u5185\u5b58"], right:10, top:0, textStyle:{fontSize:10}},
                    grid: {left:55, right:15, top:42, bottom:25},
                    xAxis: {type:"category", data:times, axisLabel:{fontSize:9}},
                    yAxis: {type:"value", name:memUnit},
                    series: [
                        {
                            name: "\u672c\u5730\u5185\u5b58", type: "line", stack: "numa", data: localData,
                            smooth: true, symbol: "none",
                            areaStyle: {opacity:0.6, color:"#50c878"}, lineStyle: {color:"#50c878", width:1}, itemStyle: {color:"#50c878"}
                        },
                        {
                            name: "\u8fdc\u7a0b\u5185\u5b58", type: "line", stack: "numa", data: remoteData,
                            smooth: true, symbol: "none",
                            areaStyle: {opacity:0.6, color:"#ff7043"}, lineStyle: {color:"#ff7043", width:1}, itemStyle: {color:"#ff7043"}
                        }
                    ]
                }, true);
            }
        } else {
            // ── 普通模式（无 NUMA 数据）──
            var memData = data.map(function(p) {
                return useMB ? +(p.mem * 1024).toFixed(1) : +(p.mem).toFixed(3);
            });

            if (incremental) {
                // 增量更新：仅更新数据
                S.charts.jobMem.setOption({
                    xAxis: {data: times},
                    series: [{data: memData}]
                });
            } else {
                // 完整绘制
                S.charts.jobMem.setOption({
                    animation: true, animationDuration: 0, animationDurationUpdate: 300, animationEasingUpdate: "cubicInOut",
                    title: {text: "\u5185\u5b58\u4f7f\u7528", textStyle:{fontSize:12}, left:"center"},
                    tooltip: {trigger:"axis"},
                    grid: {left:55, right:15, top:42, bottom:25},
                    xAxis: {type:"category", data:times, axisLabel:{fontSize:9}},
                    yAxis: {type:"value", name:memUnit},
                    series: [{
                        type: "line", data: memData, smooth: true, symbol: "none",
                        areaStyle: {opacity:0.3}, itemStyle: {color:"#36d"}, lineStyle: {color:"#36d"}
                    }]
                }, true);
            }
        }
    }
}

function loadJobLog(logType, silent) {
    if (!S.currentJobId) return;
    S._activeLogType = logType || S._activeLogType || "stdout";
    var el = document.getElementById("jobLogContent");
    if (!silent && el) el.textContent = "正在加载 " + S._activeLogType + "...";
    fetch("/api/log/" + S.currentJobId + "?log_type=" + S._activeLogType + "&lines=500").then(function(r) { return r.json(); }).then(function(d) {
        var content = d.content || d.error || "无内容";
        if (el) {
            if (silent && !S._logAutoFollow) {
                /* 锁定模式：保持当前阅读位置 */
                var prevTop = el.scrollTop;
                el.textContent = content;
                el.scrollTop = prevTop;
            } else {
                /* 跟随模式 或 首次加载：滚动到底部 */
                el.textContent = content;
                el.scrollTop = el.scrollHeight;
            }
        }
    }).catch(function(e) { if (!silent && el) el.textContent = "加载失败: " + e; });
}

/* 切换任务全程历史模式（toggle按钮，类似文件浏览器"大小"按钮） */
function loadFullJobHistory() {
    if (!S.currentJobId) return;
    S._jobFullHistory = !S._jobFullHistory;
    var btn = document.getElementById("btnFullHistory");
    if (btn) btn.classList.toggle("active", S._jobFullHistory);
    var numCpus = S._jobModalNumCpus || 1;
    if (S._jobFullHistory) {
        /* 切换为全程历史：since=0 获取全量数据 */
        fetch("/api/history/job/" + S.currentJobId + "?since=0").then(function(r) { return r.json(); }).then(function(d) {
            var histData = d.data || [];
            S._jobModalChartData = histData;
            drawJobCharts(histData, numCpus, false);
            showToast("全程历史已开启 (" + histData.length + " 个数据点)");
        }).catch(function(e) { showToast("加载全程历史失败: " + e); });
    } else {
        /* 切换回正常窗口：重新获取窗口内历史，恢复WS增量更新 */
        var since = S.historyDuration > 0 ? (Date.now()/1000 - S.historyDuration) : 0;
        fetch("/api/history/job/" + S.currentJobId + "?since=" + since).then(function(r) { return r.json(); }).then(function(d) {
            var histData = d.data || [];
            S._jobModalChartData = histData;
            drawJobCharts(histData, numCpus, false);
            showToast("已恢复正常历史窗口");
        }).catch(function(e) { showToast("恢复历史失败: " + e); });
    }
}

/* 跳转到当前任务的工作目录 */
function gotoJobWorkDir() {
    if (!S.currentJobId) return;
    var workDir = "";
    /* 先从当前任务列表查找 */
    if (S.data && S.data.jobs) {
        for (var i = 0; i < S.data.jobs.length; i++) {
            if (S.data.jobs[i].job_id === S.currentJobId) { workDir = S.data.jobs[i].work_dir; break; }
        }
    }
    /* 再从历史任务列表查找 */
    if (!workDir && S._historyJobs) {
        for (var i = 0; i < S._historyJobs.length; i++) {
            if (S._historyJobs[i].job_id === S.currentJobId) { workDir = S._historyJobs[i].work_dir; break; }
        }
    }
    if (!workDir) { showToast("未找到工作目录"); return; }
    /* 关闭任务详情模态框 */
    var modal = bootstrap.Modal.getInstance(document.getElementById("jobModal"));
    if (modal) modal.hide();
    /* 切换到文件浏览标签页并导航到工作目录 */
    browseToDir(workDir);
}

/* ── 日志跟随/锁定模式切换 ── */
function toggleLogFollow() {
    S._logAutoFollow = !S._logAutoFollow;
    var btn = document.getElementById("btnLogFollow");
    if (btn) {
        if (S._logAutoFollow) {
            btn.innerHTML = '<i class="bi bi-unlock-fill"></i>';
            btn.title = "跟随模式：自动滚动到底部（类似终端）\n点击切换为锁定模式";
            btn.classList.remove("active");
            /* 立即滚动到底部 */
            var el = document.getElementById("jobLogContent");
            if (el) el.scrollTop = el.scrollHeight;
        } else {
            btn.innerHTML = '<i class="bi bi-lock-fill"></i>';
            btn.title = "锁定模式：保持当前阅读位置\n点击切换为跟随模式";
            btn.classList.add("active");
        }
    }
}

/* ── NUMA 按需分析 ── */
function analyzeJobNuma() {
    if (!S.currentJobId || S._numaAnalysisLoading) return;
    S._numaAnalysisLoading = true;
    var btn = document.getElementById("btnNumaAnalysis");
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="bi bi-hourglass-split me-1"></i>分析中...'; }
    var resultDiv = document.getElementById("numaAnalysisResult");
    if (resultDiv) { resultDiv.style.display = "block"; resultDiv.innerHTML = '<div class="text-muted small"><i class="bi bi-hourglass-split me-1"></i>正在通过 SSH 采集 NUMA 内存分布...</div>'; }

    fetch("/api/job/" + S.currentJobId + "/numa").then(function(r) { return r.json(); }).then(function(d) {
        S._numaAnalysisLoading = false;
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-cpu me-1"></i>NUMA'; }
        if (d.error) {
            if (resultDiv) resultDiv.innerHTML = '<div class="alert alert-warning py-1 small"><i class="bi bi-exclamation-triangle me-1"></i>' + esc(d.error) + '</div>';
            return;
        }
        renderNumaAnalysis(d, resultDiv);
    }).catch(function(e) {
        S._numaAnalysisLoading = false;
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-cpu me-1"></i>NUMA'; }
        if (resultDiv) resultDiv.innerHTML = '<div class="alert alert-danger py-1 small">NUMA 分析失败: ' + esc(String(e)) + '</div>';
    });
}

function renderNumaAnalysis(data, container) {
    var html = '<div class="card bg-dark border-secondary"><div class="card-body py-2">';
    html += '<h6 class="card-title mb-2"><i class="bi bi-cpu me-1"></i>NUMA 内存分布 — 节点 ' + esc(data.node || "?") + '</h6>';

    /* NUMA 拓扑信息 */
    if (data.numa_nodes && data.numa_nodes.length > 0) {
        var totalLocal = 0, totalRemote = 0;
        var localNodes = data.local_numa_nodes || [];

        html += '<table class="table table-sm table-dark mb-2" style="font-size:11px">';
        html += '<thead><tr><th>NUMA 节点</th><th>内存 (MB)</th><th>类型</th><th>占比</th></tr></thead><tbody>';
        var totalMem = 0;
        for (var i = 0; i < data.numa_nodes.length; i++) totalMem += data.numa_nodes[i].total_mb;

        for (var i = 0; i < data.numa_nodes.length; i++) {
            var nn = data.numa_nodes[i];
            var isLocal = localNodes.indexOf(nn.node_id) >= 0;
            var pct = totalMem > 0 ? (nn.total_mb / totalMem * 100).toFixed(1) : "0.0";
            var typeLabel = isLocal ? '<span class="badge bg-success">本地</span>' : '<span class="badge bg-warning text-dark">远程</span>';
            if (isLocal) totalLocal += nn.total_mb; else totalRemote += nn.total_mb;
            html += '<tr><td>Node ' + nn.node_id + ' (CPUs ' + esc(nn.cpus || "?") + ')</td>';
            html += '<td>' + nn.total_mb.toFixed(1) + '</td><td>' + typeLabel + '</td><td>' + pct + '%</td></tr>';
        }
        html += '</tbody></table>';

        /* 汇总条 */
        var totalAll = totalLocal + totalRemote;
        var localPct = totalAll > 0 ? (totalLocal / totalAll * 100).toFixed(1) : "0.0";
        var remotePct = totalAll > 0 ? (totalRemote / totalAll * 100).toFixed(1) : "0.0";
        html += '<div class="d-flex gap-3 small">';
        html += '<span><i class="bi bi-circle-fill text-success me-1"></i>本地内存: ' + (totalLocal / 1024).toFixed(2) + ' GB (' + localPct + '%)</span>';
        html += '<span><i class="bi bi-circle-fill text-warning me-1"></i>远程内存: ' + (totalRemote / 1024).toFixed(2) + ' GB (' + remotePct + '%)</span>';
        html += '<span>总计: ' + (totalAll / 1024).toFixed(2) + ' GB</span>';
        html += '</div>';

        if (parseFloat(remotePct) > 30) {
            html += '<div class="alert alert-warning py-1 mt-2 small mb-0"><i class="bi bi-exclamation-triangle me-1"></i>远程内存占比较高 (' + remotePct + '%)，可能影响性能。建议使用 <code>numactl --cpunodebind=N --membind=N</code> 绑定 CPU 和内存到同一 NUMA 节点。</div>';
        }
    }

    if (data.cpuset) {
        html += '<div class="text-muted small mt-1">作业 CPU 分配: ' + esc(data.cpuset) + '</div>';
    }
    if (data.cgroup_usage_mb) {
        html += '<div class="text-muted small">cgroup 总内存: ' + (data.cgroup_usage_mb / 1024).toFixed(2) + ' GB（含内核/缓存开销）</div>';
    }
    html += '</div></div>';
    container.innerHTML = html;
}

function cancelJob() {
    if (!S.currentJobId || !confirm("取消任务 " + S.currentJobId + "？")) return;
    fetch("/api/job/" + S.currentJobId + "/cancel", {method:"POST"}).then(function(r){return r.json();}).then(function(d){alert(d.message||"已发送取消");});
}
function cancelJobDirect(jid) {
    if (!confirm("取消任务 " + jid + "？")) return;
    fetch("/api/job/" + jid + "/cancel", {method:"POST"}).then(function(r){return r.json();}).then(function(d){alert(d.message||"已发送取消");});
}

/* ── 智能缓冲区提示 ── */
function updateBufferTip(jid) {
    var tipEl = document.getElementById("jobBufferTip");
    if (!tipEl) return;
    /* 已结束任务不需要提示 */
    if (S._jobIsFinished) { tipEl.style.display = "none"; return; }
    /* 默认显示通用提示 */
    tipEl.style.display = "";
    tipEl.innerHTML = '<i class="bi bi-info-circle me-1"></i>提示：若输出更新不及时，请在 sbatch 脚本中添加 <code>export PYTHONUNBUFFERED=1</code>（Python）或使用 <code>stdbuf -oL command</code>（通用）禁用输出缓冲';
    /* 异步检查作业环境变量 */
    fetch("/api/job/" + jid).then(function(r) { return r.json(); }).then(function(d) {
        if (!d || d.error) return;
        var env = (d.Environment || d.Command || "").toLowerCase();
        var hasUnbuf = false;
        /* 检查 PYTHONUNBUFFERED 在 scontrol 输出中 */
        if (d.PYTHONUNBUFFERED || (d.Environment && d.Environment.indexOf("PYTHONUNBUFFERED=1") >= 0)) {
            hasUnbuf = true;
        }
        /* 检查 submit_command 中是否包含 --export=ALL,PYTHONUNBUFFERED */
        if (d.SubmitLine && d.SubmitLine.indexOf("PYTHONUNBUFFERED") >= 0) {
            hasUnbuf = true;
        }
        /* 检查命令行中是否有 stdbuf */
        var hasStdbuf = d.Command && d.Command.indexOf("stdbuf") >= 0;

        if (hasUnbuf) {
            tipEl.innerHTML = '<i class="bi bi-check-circle text-success me-1"></i>已启用 <code>PYTHONUNBUFFERED=1</code>，Python 输出将实时写入磁盘，日志更新及时。';
        } else if (hasStdbuf) {
            tipEl.innerHTML = '<i class="bi bi-check-circle text-success me-1"></i>已使用 <code>stdbuf</code> 禁用输出缓冲，日志更新及时。';
        }
    }).catch(function() {});
}

/* ===== HISTORY JOBS ===== */
function loadHistoryJobs() {
    fetch("/api/history-jobs").then(function(r) { return r.json(); }).then(function(d) {
        S._historyJobs = d.jobs || [];
        renderHistoryJobs();
    }).catch(function(e) { console.error("Failed to load history jobs:", e); });
}
function renderHistoryJobs() {
    var jobs = S._historyJobs || [];
    var q = (document.getElementById("historySearch") ? document.getElementById("historySearch").value : "").toLowerCase();
    if (q) jobs = jobs.filter(function(j) {
        return j.job_id.indexOf(q) >= 0 || j.name.toLowerCase().indexOf(q) >= 0 || j.user.toLowerCase().indexOf(q) >= 0;
    });
    var countEl = document.getElementById("historyCount");
    if (countEl) countEl.textContent = "共 " + jobs.length + " 条记录";
    var html = "";
    jobs.forEach(function(j) {
        var stCls = j.state === "RUNNING" ? "success" : j.state === "COMPLETED" ? "info" : "secondary";
        var stLabel = j.state === "COMPLETED" ? "已结束" : j.state === "RUNNING" ? "运行中" : j.state;
        var endText = j.end_time || "-";
        var nameShort = j.name.length > 25 ? j.name.slice(0, 25) + "..." : j.name;
        html += '<tr onclick="openJobDetail(\'' + j.job_id + '\')" style="cursor:pointer">';
        html += '<td>' + j.job_id + '</td>';
        html += '<td title="' + escAttr(j.name) + '">' + esc(nameShort) + '</td>';
        html += '<td>' + j.user + '</td>';
        html += '<td><span class="badge bg-' + stCls + '">' + stLabel + '</span></td>';
        html += '<td>' + j.partition + '</td>';
        html += '<td>' + (j.nodes || "-") + '</td>';
        html += '<td>' + j.num_cpus + '</td>';
        html += '<td>' + j.time_used + '</td>';
        html += '<td>' + endText + '</td>';
        html += '<td>';
        html += '<button class="btn btn-sm btn-outline-info py-0 me-1" onclick="event.stopPropagation();openJobDetail(\'' + j.job_id + '\')" title="查看详情"><i class="bi bi-eye"></i></button>';
        if (j.work_dir) {
            html += '<button class="btn btn-sm btn-outline-warning py-0" onclick="event.stopPropagation();browseToDir(\'' + escAttr(j.work_dir) + '\')" title="打开工作目录"><i class="bi bi-folder2-open"></i></button>';
        }
        html += '</td></tr>';
    });
    document.getElementById("historyBody").innerHTML = html;
}
/* 跳转到指定目录（从历史任务中调用） */
function browseToDir(dir) {
    var filesTab = document.querySelector('[data-tab="files"]');
    if (filesTab) {
        var bsTab = bootstrap.Tab.getOrCreateInstance(filesTab);
        bsTab.show();
    }
    browsePath(dir);
}

/* ===== FILE BROWSER ===== */
function browsePath(p) {
    S.filePath = p;
    var pi = document.getElementById("pathInput"); if (pi) pi.value = p;
    var url = "/api/files?path=" + encodeURIComponent(p);
    if (S.showFolderSizes) url += "&folder_sizes=1";
    fetch(url).then(function(r) { return r.json(); }).then(function(d) {
        if (d.error) { alert(d.error); return; }
        S.filePath = d.path; if (pi) pi.value = d.path;
        renderFiles(d.entries || []);
    }).catch(function(e) { console.warn("browse err:", e); });
}
function browseParent() {
    if (!S.filePath) return;
    var parts = S.filePath.split("/"); parts.pop();
    browsePath(parts.join("/") || "/");
}
function refreshFiles() { browsePath(S.filePath || ""); }
function sortFiles(col) {
    if (S.fileSortCol === col) S.fileSortAsc = !S.fileSortAsc;
    else { S.fileSortCol = col; S.fileSortAsc = true; }
    renderFiles(S._lastFileEntries || []);
}
function toggleFolderSizes() {
    S.showFolderSizes = !S.showFolderSizes;
    var btn = document.getElementById("btnFolderSizes");
    if (btn) btn.classList.toggle("active", S.showFolderSizes);
    refreshFiles();
}

function toggleFileViewMode() {
    if (S.fileViewMode === "full") {
        S.fileViewMode = "split";
    } else {
        S.fileViewMode = "full";
    }
    applyFileViewMode();
}

function applyFileViewMode() {
    var browserCol = document.getElementById("fileBrowserCol");
    var editorCol = document.getElementById("fileEditorCol");
    var toggleBtn = document.getElementById("btnViewMode");
    if (S.fileViewMode === "full") {
        browserCol.className = "col-12";
        editorCol.classList.add("d-none");
        if (toggleBtn) toggleBtn.innerHTML = '<i class="bi bi-layout-split"></i> 分栏';
    } else {
        browserCol.className = "col-md-5";
        editorCol.classList.remove("d-none");
        if (toggleBtn) toggleBtn.innerHTML = '<i class="bi bi-arrows-fullscreen"></i> 全屏';
    }
}

function renderFiles(entries) {
    S._lastFileEntries = entries;
    // 如果收藏夹视图激活，显示收藏夹
    if (S.bookmarkViewActive) { renderBookmarkList(); return; }
    var sorted = entries.slice();
    var sc = S.fileSortCol, sa = S.fileSortAsc;
    sorted.sort(function(a, b) {
        if (a.type !== b.type) return a.type === "dir" ? -1 : 1;
        var va = a[sc], vb = b[sc];
        if (typeof va === "number" && typeof vb === "number") return sa ? va - vb : vb - va;
        va = String(va || "").toLowerCase(); vb = String(vb || "").toLowerCase();
        return sa ? va.localeCompare(vb) : vb.localeCompare(va);
    });
    var tbody = document.getElementById("filesBody");
    var html = "";
    sorted.forEach(function(e) {
        var icon = e.type === "dir" ? "bi-folder-fill text-warning" : getFileIcon(e.name);
        var size = e.type === "dir" ? (e.size > 0 ? formatSize(e.size) : "-") : formatSize(e.size);
        var mtime = new Date(e.mtime * 1000).toLocaleString();
        var fp = S.filePath + "/" + e.name;
        var isBookmarked = S.bookmarks.indexOf(fp) >= 0;
        var starCls = isBookmarked ? "bi-star-fill text-warning" : "bi-star";
        var starBtn = '<button class="btn btn-sm btn-link py-0 px-1" onclick="event.stopPropagation();toggleBookmark(\'' + escAttr(fp) + '\')" title="' + (isBookmarked ? '取消收藏' : '收藏') + '"><i class="bi ' + starCls + '"></i></button>';
        if (e.type === "dir") {
            html += '<tr><td>' + starBtn + '<i class="bi ' + icon + ' me-1"></i><a href="#" onclick="browsePath(\'' + escAttr(fp) + '\');return false">' + esc(e.name) + '</a></td>';
            html += '<td>' + size + '</td><td class="small text-muted">' + mtime + '</td>';
            html += '<td class="d-flex gap-1">';
            html += '<button class="btn btn-sm btn-outline-info py-0" onclick="event.stopPropagation();downloadFolder(\'' + escAttr(fp) + '\')" title="下载文件夹(zip)"><i class="bi bi-download"></i></button>';
            html += '<button class="btn btn-sm btn-outline-danger py-0" onclick="deleteFile(\'' + escAttr(fp) + '\')"><i class="bi bi-trash"></i></button>';
            html += '</td></tr>';
        } else {
            var canPreview = isPreviewable(e.name);
            var viewFn = canPreview ? "previewFile" : "viewFile";
            html += '<tr><td>' + starBtn + '<i class="bi ' + icon + ' me-1"></i><a href="#" onclick="' + viewFn + '(\'' + escAttr(fp) + '\');return false">' + esc(e.name) + '</a></td>';
            html += '<td>' + size + '</td><td class="small text-muted">' + mtime + '</td>';
            html += '<td class="d-flex gap-1">';
            html += '<a href="/api/file-download?path=' + encodeURIComponent(fp) + '" class="btn btn-sm btn-outline-info py-0" title="下载"><i class="bi bi-download"></i></a>';
            if (e.name.endsWith('.sbatch')) {
                html += '<button class="btn btn-sm btn-outline-warning py-0" onclick="event.stopPropagation();submitSbatch(\'' + escAttr(fp) + '\')" title="提交sbatch作业"><i class="bi bi-send-fill"></i></button>';
            }
            if (e.name.endsWith('.sh')) {
                html += '<button class="btn btn-sm btn-outline-success py-0" onclick="event.stopPropagation();runBash(\'' + escAttr(fp) + '\')" title="运行bash脚本"><i class="bi bi-play-fill"></i></button>';
            }
            html += '<button class="btn btn-sm btn-outline-danger py-0" onclick="deleteFile(\'' + escAttr(fp) + '\')"><i class="bi bi-trash"></i></button>';
            html += '</td></tr>';
        }
    });
    tbody.innerHTML = html;
}

function getFileIcon(name) {
    var ext = (name.split(".").pop() || "").toLowerCase();
    if (["py","pyx","pxd"].indexOf(ext) >= 0) return "bi-filetype-py text-info";
    if (["js","ts"].indexOf(ext) >= 0) return "bi-filetype-js text-warning";
    if (["html","htm"].indexOf(ext) >= 0) return "bi-filetype-html text-danger";
    if (["css"].indexOf(ext) >= 0) return "bi-filetype-css text-primary";
    if (["json"].indexOf(ext) >= 0) return "bi-filetype-json text-success";
    if (["md","txt","log","out","err"].indexOf(ext) >= 0) return "bi-file-earmark-text text-muted";
    if (["png","jpg","jpeg","gif","bmp","svg","webp"].indexOf(ext) >= 0) return "bi-file-earmark-image text-success";
    if (["pdf"].indexOf(ext) >= 0) return "bi-file-earmark-pdf text-danger";
    if (["sh","sbatch","bash"].indexOf(ext) >= 0) return "bi-terminal text-success";
    if (["npz","npy","h5","hdf5"].indexOf(ext) >= 0) return "bi-file-earmark-binary text-info";
    return "bi-file-earmark text-muted";
}

function isPreviewable(name) {
    var ext = (name.split(".").pop() || "").toLowerCase();
    return ["png","jpg","jpeg","gif","bmp","svg","webp","pdf"].indexOf(ext) >= 0;
}

function viewFile(path) {
    if (S.fileViewMode === "full") { S.fileViewMode = "split"; applyFileViewMode(); }
    fetch("/api/file-content?path=" + encodeURIComponent(path)).then(function(r) { return r.json(); }).then(function(d) {
        if (d.error) { alert(d.error); return; }
        S.editingFile = d.path; S.editDirty = false;
        document.getElementById("editorPath").textContent = d.path;
        var ta = document.getElementById("editorTextarea");
        ta.value = d.content; ta.style.display = "block";
        ta.oninput = function() { S.editDirty = true; };
        hideEl("editorPlaceholder"); hideEl("previewContainer");
        showEditorButtons(true);
    }).catch(function(e) { alert("读取失败: " + e); });
}

function previewFile(path) {
    if (S.fileViewMode === "full") { S.fileViewMode = "split"; applyFileViewMode(); }
    S.editingFile = path;
    document.getElementById("editorPath").textContent = path;
    hideEl("editorPlaceholder");
    document.getElementById("editorTextarea").style.display = "none";
    var container = document.getElementById("previewContainer");
    if (!container) {
        var ec = document.getElementById("fileEditorCol");
        container = document.createElement("div");
        container.id = "previewContainer";
        container.style.cssText = "height:calc(100vh - 350px);overflow:auto;text-align:center;background:#1a1a1a;border-radius:6px;padding:10px";
        ec.appendChild(container);
    }
    container.style.display = "block";
    var ext = (path.split(".").pop() || "").toLowerCase();
    var url = "/api/file-download?path=" + encodeURIComponent(path);
    if (["png","jpg","jpeg","gif","bmp","svg","webp"].indexOf(ext) >= 0) {
        container.innerHTML = '<img src="' + url + '" style="max-width:100%;max-height:100%;object-fit:contain">';
    } else if (ext === "pdf") {
        container.innerHTML = '<embed src="' + url + '" type="application/pdf" width="100%" height="100%" style="min-height:500px">';
    }
    showEditorButtons(false);
    var bd = document.getElementById("btnDownload");
    if (bd) bd.classList.remove("d-none");
}

function showEditorButtons(showEditBtns) {
    ["btnUndo","btnRedo","btnSave"].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) { if (showEditBtns) el.classList.remove("d-none"); else el.classList.add("d-none"); }
    });
    var bd = document.getElementById("btnDownload");
    if (bd) bd.classList.remove("d-none");
}

function editorUndo() { document.getElementById("editorTextarea").focus(); document.execCommand("undo"); }
function editorRedo() { document.getElementById("editorTextarea").focus(); document.execCommand("redo"); }
function editorSave() {
    if (!S.editingFile) return;
    var content = document.getElementById("editorTextarea").value;
    fetch("/api/file-save", {method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({path:S.editingFile,content:content})})
    .then(function(r){return r.json();}).then(function(d) {
        if (d.error) { alert("保存失败: " + d.error); return; }
        S.editDirty = false;
        var btn = document.getElementById("btnSave");
        btn.classList.remove("btn-outline-success"); btn.classList.add("btn-success");
        setTimeout(function() { btn.classList.remove("btn-success"); btn.classList.add("btn-outline-success"); }, 1000);
    }).catch(function(e) { alert("保存失败: " + e); });
}
function downloadFile() {
    if (!S.editingFile) return;
    window.open("/api/file-download?path=" + encodeURIComponent(S.editingFile));
}

function downloadFolder(path) {
    showToast("正在打包文件夹，请稍候...");
    window.open("/api/folder-download?path=" + encodeURIComponent(path));
}
function deleteFile(path) {
    if (!confirm("确定删除\n" + path + "？")) return;
    fetch("/api/file-delete", {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:path})})
    .then(function(r){return r.json();}).then(function(d) { if (d.error) alert(d.error); else refreshFiles(); });
}

/* 提交sbatch脚本到集群 */
function submitSbatch(path) {
    if (!confirm("确定提交此sbatch脚本？\n" + path)) return;
    fetch("/api/sbatch", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({path: path})
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
        if (d.success) {
            showToast(d.message || "提交成功");
        } else {
            alert("提交失败: " + (d.message || d.error || "未知错误"));
        }
    })
    .catch(function(e) { alert("提交请求失败: " + e); });
}

/* 运行bash脚本 */
function runBash(path) {
    if (!confirm("确定运行此bash脚本？\n" + path)) return;
    showToast("正在执行脚本...");
    fetch("/api/bash", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({path: path})
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
        if (d.success) {
            // 显示输出结果
            var msg = d.message || "执行完成";
            if (msg.length > 200) {
                alert("脚本输出:\n" + msg);
            } else {
                showToast(msg);
            }
        } else {
            alert("执行失败: " + (d.message || d.error || "未知错误"));
        }
    })
    .catch(function(e) { alert("执行请求失败: " + e); });
}

/* ── 收藏夹/书签功能 ── */
function toggleBookmark(path) {
    var idx = S.bookmarks.indexOf(path);
    if (idx >= 0) {
        S.bookmarks.splice(idx, 1);
    } else {
        S.bookmarks.push(path);
    }
    // 保存到服务器
    fetch("/api/bookmarks", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({bookmarks: S.bookmarks})
    }).catch(function() {});
    // 同步保存到设置
    saveSettingsToServer({bookmarks: S.bookmarks});
    // 刷新文件列表以更新星标
    if (S._lastFileEntries) renderFiles(S._lastFileEntries);
}

function toggleBookmarkView() {
    S.bookmarkViewActive = !S.bookmarkViewActive;
    var btn = document.getElementById("btnBookmarks");
    if (btn) btn.classList.toggle("active", S.bookmarkViewActive);
    if (S.bookmarkViewActive) {
        renderBookmarkList();
    } else {
        // 返回正常文件浏览
        if (S._lastFileEntries) renderFiles(S._lastFileEntries);
        else refreshFiles();
    }
}

function renderBookmarkList() {
    var tbody = document.getElementById("filesBody");
    if (!S.bookmarks || S.bookmarks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted py-3"><i class="bi bi-star me-2"></i>暂无收藏，点击文件/文件夹旁的星标添加</td></tr>';
        return;
    }
    var html = '';
    S.bookmarks.forEach(function(bm) {
        var name = bm.split("/").pop();
        var dir = bm.substring(0, bm.lastIndexOf("/"));
        // 判断是否为目录（简单通过是否有扩展名来猜测）
        var maybeDir = name.indexOf(".") < 0;
        var icon = maybeDir ? "bi-folder-fill text-warning" : getFileIcon(name);
        // 点击跳转时自动退出收藏视图
        var clickFn = maybeDir
            ? "exitBookmarkAndBrowse('" + escAttr(bm) + "')"
            : "exitBookmarkAndView('" + escAttr(bm) + "')";
        html += '<tr><td><i class="bi bi-star-fill text-warning me-1"></i><i class="bi ' + icon + ' me-1"></i>';
        html += '<a href="#" onclick="' + clickFn + ';return false">' + esc(name) + '</a>';
        html += ' <small class="text-muted ms-2">' + esc(dir) + '</small></td>';
        html += '<td>-</td><td>-</td>';
        html += '<td><button class="btn btn-sm btn-outline-danger py-0" onclick="toggleBookmark(\'' + escAttr(bm) + '\')" title="取消收藏"><i class="bi bi-x-lg"></i></button></td>';
        html += '</tr>';
    });
    tbody.innerHTML = html;
}

/* 退出收藏视图并浏览文件夹 */
function exitBookmarkAndBrowse(path) {
    S.bookmarkViewActive = false;
    var btn = document.getElementById("btnBookmarks");
    if (btn) btn.classList.remove("active");
    browsePath(path);
}

/* 退出收藏视图并打开文件 */
function exitBookmarkAndView(path) {
    S.bookmarkViewActive = false;
    var btn = document.getElementById("btnBookmarks");
    if (btn) btn.classList.remove("active");
    // 先浏览到文件所在目录
    var dir = path.substring(0, path.lastIndexOf("/"));
    S.filePath = dir;
    var pi = document.getElementById("pathInput"); if (pi) pi.value = dir;
    viewFile(path);
    // 刷新文件列表显示该目录
    var url = "/api/files?path=" + encodeURIComponent(dir);
    if (S.showFolderSizes) url += "&folder_sizes=1";
    fetch(url).then(function(r) { return r.json(); }).then(function(d) {
        if (!d.error) {
            S.filePath = d.path;
            if (pi) pi.value = d.path;
            S._lastFileEntries = d.entries || [];
            renderFiles(d.entries || []);
        }
    }).catch(function() {});
}

function showUploadDialog() {
    S.uploadFiles = [];
    document.getElementById("uploadList").innerHTML = "";
    var fi = document.getElementById("uploadFileInput"); if (fi) fi.value = "";
    new bootstrap.Modal(document.getElementById("uploadModal")).show();
}
function handleFileSelect(files) {
    S.uploadFiles = Array.prototype.slice.call(files);
    document.getElementById("uploadList").innerHTML = S.uploadFiles.map(function(f) {
        return '<div class="small">' + esc(f.name) + ' (' + formatSize(f.size) + ')</div>';
    }).join("");
}
function doUpload() {
    if (!S.uploadFiles.length || !S.filePath) return;
    var promises = S.uploadFiles.map(function(f) {
        var fd = new FormData(); fd.append("file", f); fd.append("dest", S.filePath);
        return fetch("/api/file-upload", {method:"POST", body:fd}).then(function(r){return r.json();});
    });
    Promise.all(promises).then(function(results) {
        var ok = results.filter(function(r){return r.status === "ok";}).length;
        alert("上传: " + ok + "/" + results.length + " 成功");
        bootstrap.Modal.getInstance(document.getElementById("uploadModal")).hide();
        refreshFiles();
    });
}
function createFolder() {
    var name = prompt("新文件夹名称:");
    if (!name) return;
    var path = S.filePath + "/" + name;
    fetch("/api/file-mkdir", {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:path})})
    .then(function(r){return r.json();}).then(function(d) { if (d.error) alert(d.error); else refreshFiles(); });
}

/* ── Helpers ── */
function hideEl(id) { var el = document.getElementById(id); if (el) el.style.display = "none"; }
function formatSize(bytes) {
    if (bytes < 0) return "-";
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1048576) return (bytes/1024).toFixed(1) + " KB";
    if (bytes < 1073741824) return (bytes/1048576).toFixed(1) + " MB";
    return (bytes/1073741824).toFixed(2) + " GB";
}
function esc(s) { return s ? String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;") : ""; }
function escAttr(s) { return s ? String(s).replace(/\\/g,"\\\\").replace(/'/g,"\'").replace(/"/g,"&quot;") : ""; }
