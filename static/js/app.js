/* SLURM Dashboard - Frontend Application v10 */
"use strict";

var S = {
    ws: null, data: null,
    activeTab: "cluster",
    selectedNode: null,
    jobSortCol: "job_id", jobSortAsc: true, jobFilter: "all",
    historySortCol: "job_id", historySortAsc: false,
    loginSortCol: "cpu_pct", loginSortAsc: false,
    loginProcessSort: "cpu_pct", loginProcessLimit: 50,
    loginDetailedCommands: false,
    loginExcludeRoot: true,
    loginProcessSearch: "",
    _loginProcs: [],
    filePath: "", fileSortCol: "name", fileSortAsc: true,
    editingFile: null, editorMode: null, editDirty: false, uploadFiles: [],
    fileBrowserRoot: "",
    fileLayoutMode: "list",
    fileAllowWrite: true,
    showHiddenFiles: true,
    fileColumnAutoWidth: false,
    fileColumns: {name: true, size: true, mtime: true, owner: true, perm: true, ext: true, actions: true},
    fileColumnWidths: {name: 420, size: 110, mtime: 180, owner: 120, perm: 130, ext: 110, actions: 150},
    historyDuration: 3600,
    refreshInterval: 10,
    charts: {},
    currentJobId: null,
    pollTimer: null, wsConnected: false,
    showFolderSizes: false,
    fileViewMode: "full",
    filePreviewFullscreenDefault: false,
    previewKind: null,
    previewZoom: 1,
    previewZoomStep: 10,
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
    _jobMemNumaMode: false,  // 跟踪当前内存图表是否为 NUMA 堆叠模式
    _nodeVisibility: {},     // 节点显示/记录设置
    _splitterDragging: false,
    _fileTreeDragging: false,
    _fileColumnResize: null,
    _suppressFileSortUntil: 0,
    _fileAutoFitSignature: "",
    _hasCopied: false,       // 是否有复制内容
    filesFilterText: "",     // 文件浏览筛选关键词（绑定当前目录）
    _filesFilterByPath: {},   // path -> filter text
    _fileTreeChildren: {},
    _fileTreeExpanded: {"/": true},
    _fileTreeLoading: {},
    _fileTreeTruncated: {},
    _fileTreeReadable: {"/": true},
    _fileTreeClickTimer: null,
    _fileTreeWidth: 280,
    _fileNavBack: [],
    _fileNavForward: [],
    _fileNavSuppress: false,
    _filesFilterTimer: null,
    _filePreviewSeq: 0,
    _previewToastTimer: null,
    _retainDateListenerBound: false,
    _jobsLastRenderAt: 0,
    _jobsLastSig: ""
};

/* ── Init ── */
document.addEventListener("DOMContentLoaded", function() {
    S.fileBrowserRoot = window.FILE_BROWSER_ROOT || S.fileBrowserRoot || "";
    loadFileBrowserUiPrefs();
    loadSettingsFromServer();
    fetchSnapshot();
    connectWS();
    startPolling();
    document.querySelectorAll("#mainTabs .nav-link").forEach(function(el) {
        el.addEventListener("shown.bs.tab", function(e) {
            S.activeTab = e.target.dataset.tab;
            if (S.activeTab === "files") {
                if (!S.filePath) browsePath(S.fileBrowserRoot || "");
                loadDiskInfo();
            }
            if (S.activeTab === "jobs") renderJobs(true);
            if (S.activeTab === "cluster") renderCluster();
            if (S.activeTab === "history") loadHistoryJobs();
            if (S.activeTab === "loginnode") loadLoginNodeInfo();
        });
    });
    var dz = document.getElementById("dropZone");
    if (dz) {
        dz.addEventListener("dragover", function(e) { e.preventDefault(); dz.classList.add("border-primary"); });
        dz.addEventListener("dragleave", function() { dz.classList.remove("border-primary"); });
        dz.addEventListener("drop", function(e) { e.preventDefault(); dz.classList.remove("border-primary"); handleFileSelect(e.dataTransfer.files); });
        dz.addEventListener("click", function() { document.getElementById("uploadFileInput").click(); });
    }
    var filesFilterInput = document.getElementById("filesFilterInput");
    if (filesFilterInput) {
        filesFilterInput.addEventListener("input", function(e) { setFilesFilter(e.target.value); });
    }
    var loginProcessSearch = document.getElementById("loginProcessSearch");
    if (loginProcessSearch) {
        loginProcessSearch.addEventListener("input", function(e) { setLoginProcessSearch(e.target.value); });
    }
    syncFilesFilterUi();
    renderFileTableHeader();
    syncFileLayoutModeUi();
    syncHiddenFilesUi();
    updateFileNavButtons();
    syncFileWriteControls();
    updateLoginNodeControls();
    document.addEventListener("click", function(e) {
        var menu = document.getElementById("fileColumnMenu");
        if (menu && !menu.classList.contains("d-none") && !menu.contains(e.target)) {
            menu.classList.add("d-none");
        }
    });
    document.addEventListener("keydown", function(e) {
        if (e.key === "Escape") {
            var menu = document.getElementById("fileColumnMenu");
            if (menu) menu.classList.add("d-none");
        }
    });
    document.addEventListener("keydown", function(e) {
        if ((e.ctrlKey || e.metaKey) && e.key === "s" && S.editingFile && S.editorMode === "text") { e.preventDefault(); editorSave(); }
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

/* 页面可见性变化时立即刷新 UI */
document.addEventListener("visibilitychange", function() {
    if (!document.hidden && S.data) {
        try { updateUI(); } catch(e) {}
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
        if (typeof s.loginNodeDetailedCommands === "boolean") S.loginDetailedCommands = s.loginNodeDetailedCommands;
        if (typeof s.loginNodeExcludeRoot === "boolean") S.loginExcludeRoot = s.loginNodeExcludeRoot;
        if (typeof s.filePreviewZoomStep === "number") S.previewZoomStep = clampPreviewZoomStep(s.filePreviewZoomStep);
        if (s.nodeVisibility && typeof s.nodeVisibility === "object") S._nodeVisibility = s.nodeVisibility;
        updateLoginNodeControls();
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

function clampPreviewZoomStep(val) {
    var step = parseInt(val) || 10;
    if (step < 1) step = 1;
    if (step > 50) step = 50;
    return step;
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
    var el10 = document.getElementById("settLoginDetailedCommands");
    if (el10) el10.checked = S.loginDetailedCommands;
    var el11 = document.getElementById("settLoginExcludeRoot");
    if (el11) el11.checked = S.loginExcludeRoot;
    var el12 = document.getElementById("settPreviewZoomStep");
    if (el12) el12.value = clampPreviewZoomStep(S.previewZoomStep);
    // 灰化逻辑：maxCacheMB > 0 时，日期输入框禁用
    updateRetainDateState();
    if (el5 && !S._retainDateListenerBound) {
        el5.addEventListener("input", updateRetainDateState);
        S._retainDateListenerBound = true;
    }
    // 加载节点可见性表格
    loadNodeVisibilityTable();
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
    var loginDetailed = document.getElementById("settLoginDetailedCommands").checked;
    var loginExcludeRoot = document.getElementById("settLoginExcludeRoot").checked;
    var zoomStep = clampPreviewZoomStep(parseInt(document.getElementById("settPreviewZoomStep").value) || 10);

    S.historyDuration = mins * 60;
    S.refreshInterval = secs;
    S.showJobCurves = jcurves;
    S.showFolderSizes = fsizes;
    S.maxCacheMB = maxMB;
    S.cacheRetainDate = retainDate;
    S.historyTrackUsers = trackUsers;
    S.clusterUsername = clusterUser;
    S.numaTrackEnabled = numaTrack;
    S.loginDetailedCommands = loginDetailed;
    S.loginExcludeRoot = loginExcludeRoot;
    S.previewZoomStep = zoomStep;

    // 收集节点可见性设置（表格未加载时保留已有设置）
    var nv = collectNodeVisibility();
    if (Object.keys(nv).length === 0 && Object.keys(S._nodeVisibility || {}).length > 0) {
        nv = S._nodeVisibility;
    }
    S._nodeVisibility = nv;

    // 强制集群视图重建（节点可见性可能变了）
    S._clusterRendered = false;
    S._lastPartKey = "";

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
        numaTrackEnabled: numaTrack,
        loginNodeDetailedCommands: loginDetailed,
        loginNodeExcludeRoot: loginExcludeRoot,
        filePreviewZoomStep: zoomStep,
        nodeVisibility: nv
    });

    // Reload chart with new durations
    if (S.selectedNode) loadNodeChart(S.selectedNode);

    bootstrap.Modal.getInstance(document.getElementById("settingsModal")).hide();

    // Brief visual feedback
    showToast("设置已保存");
}

/* 节点可见性表格 */
function loadNodeVisibilityTable() {
    var tbody = document.getElementById("nodeVisibilityBody");
    if (!tbody) return;
    // 始终从 snapshot 获取完整（未过滤）的分区节点列表
    fetch("/api/snapshot").then(function(r) { return r.json(); }).then(function(d) {
        if (d && d.all_partitions) {
            _buildNodeVisTable(d.all_partitions, [], tbody);
        } else if (d && d.partitions) {
            _buildNodeVisTable(d.partitions, d.nodes || [], tbody);
        }
    }).catch(function() {});
}

function _buildNodeVisTable(partitions, nodes, tbody) {
    // 构建节点->分区映射（从 sinfo 数据，而非过滤后的数据）
    var allNodes = {};
    partitions.forEach(function(p) {
        (p.node_list || []).forEach(function(n) {
            if (!allNodes[n]) allNodes[n] = [];
            allNodes[n].push(p.name);
        });
    });
    // 也从 nodes 列表补充
    nodes.forEach(function(n) {
        if (!allNodes[n.name]) allNodes[n.name] = [n.partitions || ""];
    });
    var sortedNames = Object.keys(allNodes).sort();
    if (sortedNames.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-muted text-center">暂无节点数据</td></tr>';
        return;
    }
    var nv = S._nodeVisibility || {};
    var html = "";
    sortedNames.forEach(function(name) {
        var vis = nv[name] || {};
        var show = vis.show !== false;
        var record = vis.record !== false;
        var parts = allNodes[name].join(",");
        html += '<tr>';
        html += '<td class="small">' + esc(name) + '</td>';
        html += '<td class="small text-muted">' + esc(parts) + '</td>';
        html += '<td><input type="checkbox" class="form-check-input nv-show" data-node="' + esc(name) + '"' + (show ? ' checked' : '') + '></td>';
        html += '<td><input type="checkbox" class="form-check-input nv-record" data-node="' + esc(name) + '"' + (record ? ' checked' : '') + '></td>';
        html += '</tr>';
    });
    tbody.innerHTML = html;
}

function collectNodeVisibility() {
    var nv = {};
    var rows = document.querySelectorAll("#nodeVisibilityBody tr");
    rows.forEach(function(row) {
        var showCb = row.querySelector(".nv-show");
        var recCb = row.querySelector(".nv-record");
        if (showCb && recCb) {
            var name = showCb.dataset.node;
            nv[name] = { show: showCb.checked, record: recCb.checked };
        }
    });
    return nv;
}

function showToast(msg, durationMs) {
    var t = document.createElement("div");
    t.className = "position-fixed bottom-0 end-0 p-3";
    t.style.zIndex = "9999";
    t.innerHTML = '<div class="toast show align-items-center text-bg-success border-0" role="alert"><div class="d-flex"><div class="toast-body">' + msg + '</div></div></div>';
    document.body.appendChild(t);
    setTimeout(function() { t.style.transition = "opacity 0.5s"; t.style.opacity = "0"; setTimeout(function() { t.remove(); }, 500); }, durationMs || 1500);
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
            // 连接后立即同步用户设置的刷新间隔到后端
            if (S.refreshInterval) {
                S.ws.send(JSON.stringify({type:"set_interval", value: S.refreshInterval}));
            }
        };
        S.ws.onclose = function() {
            S.wsConnected = false;
            setStatusBadge("warning", "bi-arrow-repeat", "轮询中");
            setTimeout(connectWS, 5000);
        };
        S.ws.onerror = function() { S.wsConnected = false; };
        S.ws.onmessage = function(e) {
            try {
                S.data = JSON.parse(e.data);
                // 页面不可见时跳过 UI 更新，节省 CPU
                if (!document.hidden) updateUI();
            } catch(err) { console.error("[WS updateUI]", err); }
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
function manualRefresh() {
    if (S.activeTab === "loginnode") {
        loadLoginNodeInfo();
        return;
    }
    fetchSnapshot();
}

/* ── 低功耗模式切换 ── */
function togglePowerMode() {
    var isLow = document.body.classList.toggle("low-power");
    var btn = document.getElementById("btnPowerMode");
    if (btn) {
        btn.title = isLow ? "当前：低功耗模式（点击切换回高性能）" : "切换低功耗模式（减少GPU占用）";
    }
    try { localStorage.setItem("powerMode", isLow ? "low" : "high"); } catch(e) {}
}
/* 启动时恢复低功耗模式设置 */
(function() {
    try {
        if (localStorage.getItem("powerMode") === "low") {
            document.body.classList.add("low-power");
            var btn = document.getElementById("btnPowerMode");
            if (btn) btn.title = "当前：低功耗模式（点击切换回高性能）";
        }
    } catch(e) {}
})();

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
        else if (S.activeTab === "jobs") renderJobs(false);
    } catch(e) { console.error("[renderTab]", e); }
    // 自动刷新打开的图表（节点详情/任务详情）—— 独立 try-catch 确保始终执行
    try { autoRefreshCharts(); } catch(e) { console.error("[autoRefreshCharts]", e); }
}
function updateSummary() {
    var s = S.data.summary; if (!s) return;
    var cpuPct = s.total_cpus > 0 ? Math.round(s.alloc_cpus / s.total_cpus * 100) : 0;
    var memPct = s.total_mem_gb > 0 ? Math.round(s.used_mem_gb / s.total_mem_gb * 100) : 0;
    setBar("sCpuBar", cpuPct, s.alloc_cpus + "/" + s.total_cpus + " (" + cpuPct + "%)");
    setBar("sMemBar", memPct, s.used_mem_gb.toFixed(0) + "/" + s.total_mem_gb.toFixed(0) + "G (" + memPct + "%)");
    if (S.data._collect_time_ms) {
        var el = document.getElementById("sCollect");
        if (el) el.textContent = S.data._collect_time_ms + "ms";
    }
    /* 更新任务过滤按钮上的计数 */
    var jobs = S.data.jobs || [];
    var nRun = 0, nPend = 0, nFin = 0;
    var finStates = ["COMPLETED","TIMEOUT","CANCELLED","FAILED","PREEMPTED"];
    jobs.forEach(function(j) {
        if (j.state === "RUNNING") nRun++;
        else if (j.state === "PENDING") nPend++;
        else if (finStates.indexOf(j.state) >= 0) nFin++;
    });
    setText("cntAll", jobs.length);
    setText("cntRunning", nRun);
    setText("cntPending", nPend);
    setText("cntFinished", nFin || "");
}
function setText(id, v) { var el = document.getElementById(id); if (el) el.textContent = v; }
function setBar(id, pct, label) {
    var el = document.getElementById(id); if (!el) return;
    el.style.width = pct + "%"; el.textContent = label;
}

/* ── 自动刷新打开的图表（无闪烁，增量追加） ── */
function autoRefreshCharts() {
    // 增量追加新数据点到节点图表（节点详情面板打开时）
    if (S.selectedNode && S.activeTab === "cluster") {
        if (S._nodeChartData) {
            try { appendNodeChartPoint(); } catch(e) { console.error("[appendNodeChartPoint]", e); }
        } else if (!S._nodeChartLoading) {
            // 初始 fetch 失败了，自动重试一次
            try { loadNodeChart(S.selectedNode); } catch(e) { console.error("[retryNodeChart]", e); }
        }
    }
    // 增量追加新数据点到任务图表（任务模态框打开时）
    var jobModal = document.getElementById("jobModal");
    // 自动刷新日志输出（运行中任务，模态框打开时，限制频率：每5秒一次）
    if (S.currentJobId && jobModal && jobModal.classList.contains("show") && !S._jobIsFinished) {
        var now = Date.now();
        if (!S._lastLogRefresh || now - S._lastLogRefresh > 5000) {
            S._lastLogRefresh = now;
            try { loadJobLog(null, true); } catch(e) { console.error("[autoRefreshLog]", e); }
        }
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
            if (job && job.state === "RUNNING" && (job.num_nodes || 1) <= 1) {
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
    // 前端节点可见性过滤（确保 show=false 的节点不显示）
    var nv = S._nodeVisibility || {};
    if (Object.keys(nv).length > 0) {
        var filteredNodeMap = {};
        Object.keys(nodeMap).forEach(function(name) {
            if (nv[name] && nv[name].show === false) return;
            filteredNodeMap[name] = nodeMap[name];
        });
        nodeMap = filteredNodeMap;
        parts = parts.map(function(p) {
            return Object.assign({}, p, {
                node_list: (p.node_list || []).filter(function(n) {
                    return !(nv[n] && nv[n].show === false);
                })
            });
        }).filter(function(p) { return (p.node_list || []).length > 0; });
    }
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
    // 保留 summaryRow，只替换分区内容
    var clusterDiv = document.getElementById("clusterView");
    var summaryEl = document.getElementById("summaryRow");
    // 移除旧的分区卡片（保留 summaryRow）
    var oldCards = clusterDiv.querySelectorAll(".partition-card");
    oldCards.forEach(function(el) { el.remove(); });
    var oldDetails = clusterDiv.querySelectorAll(".node-detail-inline");
    oldDetails.forEach(function(el) { el.remove(); });
    // 添加新的分区卡片
    var temp = document.createElement("div");
    temp.innerHTML = html;
    while (temp.firstChild) {
        clusterDiv.appendChild(temp.firstChild);
    }
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
            if (fj.state !== "RUNNING" && fj.state !== "PENDING" && fj.nodes && fj.nodes.indexOf(n.name) >= 0) {
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
    S._nodeChartLoading = true;
    var since = S.historyDuration > 0 ? (Date.now()/1000 - S.historyDuration) : 0;
    fetch("/api/history/node/" + name + "?since=" + since).then(function(r) { return r.json(); }).then(function(d) {
        S._nodeChartLoading = false;
        var nodeData = d.data || [];
        S._nodeChartData = nodeData; // 缓存历史数据
        if (!S.showJobCurves) { drawNodeChart(nodeData, [], false); return; }
        var node = null;
        if (S.data && S.data.nodes) { for (var i = 0; i < S.data.nodes.length; i++) { if (S.data.nodes[i].name === name) { node = S.data.nodes[i]; break; } } }
        if (!node || !node.jobs || node.jobs.length === 0) { drawNodeChart(nodeData, [], false); return; }
        // 多节点任务：cpu_percent 是所有节点总和，不适合在单节点堆叠图中展示
        var singleNodeJobs = node.jobs.filter(function(jid) {
            if (!S.data || !S.data.jobs) return true;
            for (var k = 0; k < S.data.jobs.length; k++) {
                if (S.data.jobs[k].job_id === jid) return (S.data.jobs[k].num_nodes || 1) <= 1;
            }
            return true;
        });
        if (singleNodeJobs.length === 0) { drawNodeChart(nodeData, [], false); return; }
        var promises = singleNodeJobs.map(function(jid) {
            return fetch("/api/history/job/" + jid + "?since=" + since)
                .then(function(r) { return r.json(); })
                .then(function(dd) {
                    S._nodeChartJobData[jid] = dd.data || []; // 缓存任务数据
                    return {jid: jid, data: dd.data || []};
                })
                .catch(function() { return {jid: jid, data: []}; });
        });
        Promise.all(promises).then(function(jh) { drawNodeChart(nodeData, jh, false); });
    }).catch(function(e) {
        S._nodeChartLoading = false;
        S._nodeChartData = []; // 设为空数组（truthy），让后续 WS 增量更新仍能追加数据
        console.warn("nodeChart err:", e);
        try { drawNodeChart([], [], false); } catch(e2) {}
    });
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
    renderJobs(true);
}
function sortJobs(col) {
    if (S.jobSortCol === col) S.jobSortAsc = !S.jobSortAsc;
    else { S.jobSortCol = col; S.jobSortAsc = true; }
    renderJobs(true);
}
function renderJobs(force) {
    if (!S.data || !S.data.jobs) return;
    var now = Date.now();
    var qInput = document.getElementById("jobSearch");
    var q = (qInput ? qInput.value : "").toLowerCase();
    var jobsRaw = S.data.jobs || [];
    // 轻量签名：数据未变化时跳过重复重绘，减少主线程抖动
    var head = jobsRaw[0] || {};
    var tail = jobsRaw[jobsRaw.length - 1] || {};
    var sig = [
        jobsRaw.length,
        S.jobFilter,
        S.jobSortCol,
        S.jobSortAsc ? 1 : 0,
        q,
        S.data.timestamp || 0,
        head.job_id || "", head.state || "", head.time_used || "",
        tail.job_id || "", tail.state || "", tail.time_used || ""
    ].join("|");
    if (!force) {
        if (sig === S._jobsLastSig) return;
        if (now - S._jobsLastRenderAt < 350) return;
    }
    S._jobsLastSig = sig;
    S._jobsLastRenderAt = now;
    var jobs = S.data.jobs.slice();
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
        var stCls = j.state === "RUNNING" ? "success" : j.state === "PENDING" ? "warning" : j.state === "COMPLETED" ? "info" : j.state === "TIMEOUT" ? "warning" : j.state === "CANCELLED" || j.state === "FAILED" ? "danger" : "secondary";
        var stLabel = j.state;
        /* 已结束任务：显示状态@HH:MM:SS徽章 */
        var _fJobStates = ["COMPLETED", "TIMEOUT", "CANCELLED", "FAILED", "PREEMPTED"];
        if (_fJobStates.indexOf(j.state) >= 0 && j.end_time) {
            var _sMap = {"COMPLETED": "结束", "TIMEOUT": "超时", "CANCELLED": "取消", "FAILED": "失败", "PREEMPTED": "抢占"};
            stLabel = (_sMap[j.state] || j.state) + "@" + j.end_time;
        }
        var cpuNorm = j.num_cpus > 0 ? (j.cpu_percent / j.num_cpus * 100).toFixed(1) : j.cpu_percent.toFixed(1);
        var nameShort = j.name.length > 25 ? j.name.slice(0, 25) + "..." : j.name;
        var rowStyle = (j.state !== "RUNNING" && j.state !== "PENDING") ? 'style="cursor:pointer;opacity:0.65"' : 'style="cursor:pointer"';
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
        if (j.state !== "RUNNING" && j.state !== "PENDING") {
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
    var _finishedStates = ["COMPLETED", "TIMEOUT", "CANCELLED", "FAILED", "PREEMPTED"];
    if (j && _finishedStates.indexOf(j.state) >= 0) S._jobIsFinished = true;
    /* 已结束任务默认开启全程历史 */
    if (S._jobIsFinished) {
        S._jobFullHistory = true;
        var btnFH = document.getElementById("btnFullHistory");
        if (btnFH) btnFH.classList.add("active");
    }
    document.getElementById("jobModalTitle").textContent = "任务 " + jid + (j ? " — " + j.name : "");
    if (j) {
        var stateDisplay = j.state;
        var _fStates = ["COMPLETED", "TIMEOUT", "CANCELLED", "FAILED", "PREEMPTED"];
        if (_fStates.indexOf(j.state) >= 0 && j.end_time) {
            var _stMap = {"COMPLETED": "已结束", "TIMEOUT": "超时终止", "CANCELLED": "已取消", "FAILED": "已失败", "PREEMPTED": "被抢占"};
            var _stClsMap = {"COMPLETED": "info", "TIMEOUT": "warning", "CANCELLED": "danger", "FAILED": "danger", "PREEMPTED": "secondary"};
            stateDisplay = '<span class="badge bg-' + (_stClsMap[j.state] || 'info') + '">' + (_stMap[j.state] || j.state) + '@' + j.end_time + '</span>';
        }
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
                S.charts.jobMem.setOption({
                    xAxis: {data: times},
                    series: [{data: memData}]
                });
            } else {
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
function sortHistoryJobs(col) {
    if (S.historySortCol === col) S.historySortAsc = !S.historySortAsc;
    else { S.historySortCol = col; S.historySortAsc = true; }
    renderHistoryJobs();
}
function renderHistoryJobs() {
    var jobs = S._historyJobs || [];
    var q = (document.getElementById("historySearch") ? document.getElementById("historySearch").value : "").toLowerCase();
    if (q) jobs = jobs.filter(function(j) {
        return j.job_id.indexOf(q) >= 0 || j.name.toLowerCase().indexOf(q) >= 0 || j.user.toLowerCase().indexOf(q) >= 0;
    });
    /* 排序 */
    var col = S.historySortCol, asc = S.historySortAsc;
    var _stateOrder = {"RUNNING":0,"PENDING":1,"COMPLETING":2,"COMPLETED":3,"TIMEOUT":4,"CANCELLED":5,"FAILED":6};
    jobs.sort(function(a, b) {
        var va = a[col], vb = b[col];
        if (col === "job_id" || col === "num_cpus") {
            va = parseInt(va) || 0; vb = parseInt(vb) || 0;
            return asc ? va - vb : vb - va;
        }
        if (col === "state") {
            va = _stateOrder[va] !== undefined ? _stateOrder[va] : 9;
            vb = _stateOrder[vb] !== undefined ? _stateOrder[vb] : 9;
            return asc ? va - vb : vb - va;
        }
        return asc ? String(va||"").localeCompare(String(vb||"")) : String(vb||"").localeCompare(String(va||""));
    });
    var countEl = document.getElementById("historyCount");
    if (countEl) countEl.textContent = "共 " + jobs.length + " 条记录";
    var html = "";
    jobs.forEach(function(j) {
        var stCls = j.state === "RUNNING" ? "success" : j.state === "COMPLETED" ? "info" : j.state === "TIMEOUT" ? "warning" : j.state === "CANCELLED" ? "danger" : j.state === "FAILED" ? "danger" : "secondary";
        var stLabel = j.state === "COMPLETED" ? "已结束" : j.state === "RUNNING" ? "运行中" : j.state === "TIMEOUT" ? "超时终止" : j.state === "CANCELLED" ? "已取消" : j.state === "FAILED" ? "已失败" : j.state;
        var endText = (j.state === "RUNNING" || j.state === "PENDING") ? "-" : (j.end_time || "-");
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
var FILE_COLUMN_DEFS = [
    {key: "name", label: "名称", sortable: true, minWidth: 180},
    {key: "size", label: "大小", sortable: true, minWidth: 80},
    {key: "mtime", label: "修改时间", sortable: true, minWidth: 140},
    {key: "owner", label: "所有者", sortable: true, minWidth: 90},
    {key: "perm", label: "权限", sortable: true, minWidth: 110},
    {key: "ext", label: "类型", sortable: true, minWidth: 90},
    {key: "actions", label: "操作", sortable: false, minWidth: 120}
];

function loadFileBrowserUiPrefs() {
    try {
        var raw = localStorage.getItem("slurmFileBrowserUiV1");
        if (!raw) return;
        var prefs = JSON.parse(raw);
        if (prefs.fileLayoutMode === "tree" || prefs.fileLayoutMode === "list") S.fileLayoutMode = prefs.fileLayoutMode;
        if (typeof prefs.showHiddenFiles === "boolean") S.showHiddenFiles = prefs.showHiddenFiles;
        if (typeof prefs.fileColumnAutoWidth === "boolean") S.fileColumnAutoWidth = prefs.fileColumnAutoWidth;
        if (typeof prefs.filePreviewFullscreenDefault === "boolean") S.filePreviewFullscreenDefault = prefs.filePreviewFullscreenDefault;
        if (prefs.fileColumns) {
            FILE_COLUMN_DEFS.forEach(function(def) {
                if (typeof prefs.fileColumns[def.key] === "boolean") S.fileColumns[def.key] = prefs.fileColumns[def.key];
            });
            S.fileColumns.name = true;
        }
        if (prefs.fileColumnWidths) {
            FILE_COLUMN_DEFS.forEach(function(def) {
                var w = Number(prefs.fileColumnWidths[def.key]);
                if (w >= def.minWidth) S.fileColumnWidths[def.key] = w;
            });
        }
        if (Number(prefs.fileTreeWidth) >= 180) S._fileTreeWidth = Number(prefs.fileTreeWidth);
    } catch(e) {}
}

function saveFileBrowserUiPrefs() {
    try {
        localStorage.setItem("slurmFileBrowserUiV1", JSON.stringify({
            fileLayoutMode: S.fileLayoutMode,
            showHiddenFiles: S.showHiddenFiles,
            fileColumnAutoWidth: S.fileColumnAutoWidth,
            filePreviewFullscreenDefault: S.filePreviewFullscreenDefault,
            fileColumns: S.fileColumns,
            fileColumnWidths: S.fileColumnWidths,
            fileTreeWidth: S._fileTreeWidth
        }));
    } catch(e) {}
}

function syncBookmarkButton() {
    var btn = document.getElementById("btnBookmarks");
    if (btn) btn.classList.toggle("active", !!S.bookmarkViewActive);
}

function fileNavLocation() {
    if (S.bookmarkViewActive) return {type: "bookmarks"};
    if (!S.filePath) return null;
    return {type: "path", path: normalizeFilePathKey(S.filePath)};
}

function normalizeFileNavLocation(loc) {
    if (!loc || !loc.type) return null;
    if (loc.type === "bookmarks") return {type: "bookmarks"};
    if (loc.type === "path" && loc.path) return {type: "path", path: normalizeFilePathKey(loc.path)};
    return null;
}

function sameFileNavLocation(a, b) {
    a = normalizeFileNavLocation(a);
    b = normalizeFileNavLocation(b);
    if (!a || !b || a.type !== b.type) return false;
    return a.type === "bookmarks" || a.path === b.path;
}

function pushFileNavTransition(from, to) {
    from = normalizeFileNavLocation(from);
    to = normalizeFileNavLocation(to);
    if (!from || !to || sameFileNavLocation(from, to)) {
        updateFileNavButtons();
        return;
    }
    S._fileNavBack.push(from);
    if (S._fileNavBack.length > 100) S._fileNavBack.shift();
    S._fileNavForward = [];
    updateFileNavButtons();
}

function updateFileNavButtons() {
    var backBtn = document.getElementById("btnFileBack");
    var forwardBtn = document.getElementById("btnFileForward");
    if (backBtn) backBtn.disabled = S._fileNavBack.length === 0;
    if (forwardBtn) forwardBtn.disabled = S._fileNavForward.length === 0;
}

function applyFileNavLocation(loc) {
    loc = normalizeFileNavLocation(loc);
    if (!loc) return Promise.resolve();
    S._fileNavSuppress = true;
    if (loc.type === "bookmarks") {
        S.bookmarkViewActive = true;
        syncBookmarkButton();
        renderBookmarkList();
        S._fileNavSuppress = false;
        updateFileNavButtons();
        return Promise.resolve();
    }
    S.bookmarkViewActive = false;
    syncBookmarkButton();
    return browsePath(loc.path, {skipHistory: true}).then(function() {
        S._fileNavSuppress = false;
        updateFileNavButtons();
    }).catch(function() {
        S._fileNavSuppress = false;
        updateFileNavButtons();
    });
}

function goFileBack() {
    var target = S._fileNavBack.pop();
    if (!target) { updateFileNavButtons(); return; }
    var current = fileNavLocation();
    if (current) S._fileNavForward.push(current);
    updateFileNavButtons();
    applyFileNavLocation(target);
}

function goFileForward() {
    var target = S._fileNavForward.pop();
    if (!target) { updateFileNavButtons(); return; }
    var current = fileNavLocation();
    if (current) S._fileNavBack.push(current);
    updateFileNavButtons();
    applyFileNavLocation(target);
}

function browsePath(p, opts) {
    opts = opts || {};
    var prevPath = S.filePath;
    var fromLocation = opts.fromLocation || fileNavLocation();
    rememberFilesFilterForPath(prevPath, S.filesFilterText);
    S.filePath = p;
    var pi = document.getElementById("pathInput"); if (pi) pi.value = p;
    var url = "/api/files?path=" + encodeURIComponent(p);
    if (S.showFolderSizes) url += "&folder_sizes=1";
    return fetch(url).then(function(r) {
        return r.json().then(function(d) {
            d._status = r.status;
            return d;
        });
    }).then(function(d) {
        if (d.error) {
            showToast(d._status === 403 || d.error === "Access denied" ? "无权限访问" : d.error, 1000);
            // 访问拒绝防呆：回到上一个位置
            if (prevPath && prevPath !== p) {
                S.filePath = prevPath;
                if (pi) pi.value = prevPath;
                applyFilesFilterForPath(prevPath);
            }
            updateFileNavButtons();
            return;
        }
        var shouldCloseEditor = !!opts.closeEditorOnSuccess &&
            normalizeFilePathKey(d.path) !== normalizeFilePathKey(prevPath);
        var commitBrowse = function() {
            S.filePath = d.path; if (pi) pi.value = d.path;
            S.bookmarkViewActive = false;
            syncBookmarkButton();
            S.fileAllowWrite = d.allow_write !== false;
            syncFileWriteControls();
            applyFilesFilterForPath(d.path);
            if (S.fileLayoutMode === "tree") expandFileTreeToPath(d.path);
            if (!S._fileNavSuppress && !opts.skipHistory) {
                pushFileNavTransition(fromLocation, {type: "path", path: d.path});
            }
            renderFiles(d.entries || [], {resetScroll: true});
            if (shouldCloseEditor) closeEditorNow();
            updateFileNavButtons();
        };
        if (shouldCloseEditor && hasUnsavedTextEdit()) {
            return confirmUnsavedEditorChange().then(function(ok) {
                if (!ok) {
                    if (prevPath) {
                        S.filePath = prevPath;
                        if (pi) pi.value = prevPath;
                        applyFilesFilterForPath(prevPath);
                    }
                    updateFileNavButtons();
                    return false;
                }
                commitBrowse();
                return true;
            });
        }
        commitBrowse();
        return true;
    }).catch(function(e) {
        console.warn("browse err:", e);
        if (prevPath && prevPath !== p) {
            S.filePath = prevPath;
            if (pi) pi.value = prevPath;
            applyFilesFilterForPath(prevPath);
        }
        updateFileNavButtons();
    });
}
function browseParent() {
    if (!S.filePath) return;
    var parts = S.filePath.split("/"); parts.pop();
    browsePath(parts.join("/") || "/");
}
function browseHome() {
    browsePath(S.fileBrowserRoot || "");
}
function refreshFiles() {
    browsePath(S.filePath || S.fileBrowserRoot || "", {skipHistory: true});
    loadDiskInfo();
}
function normalizeFilePathKey(path) {
    var key = String(path || "");
    key = key.replace(/\/+$/, "");
    return key || "/";
}
function rememberFilesFilterForPath(path, text) {
    if (!path) return;
    var key = normalizeFilePathKey(path);
    var val = String(text || "").trim().toLowerCase();
    if (val) S._filesFilterByPath[key] = val;
    else delete S._filesFilterByPath[key];
    S.filesFilterText = val;
}
function getFilesFilterForPath(path) {
    var key = normalizeFilePathKey(path || S.filePath);
    return S._filesFilterByPath[key] || "";
}
function applyFilesFilterForPath(path) {
    S.filesFilterText = getFilesFilterForPath(path);
    syncFilesFilterUi();
}
function setFilesFilter(text) {
    rememberFilesFilterForPath(S.filePath, text);
    syncFilesFilterUi();
    if (S._filesFilterTimer) clearTimeout(S._filesFilterTimer);
    S._filesFilterTimer = setTimeout(function() {
        S._filesFilterTimer = null;
        renderFiles(S._lastFileEntries || []);
    }, 120);
}
function clearFilesFilter() {
    rememberFilesFilterForPath(S.filePath, "");
    syncFilesFilterUi();
    renderFiles(S._lastFileEntries || []);
}
function syncFilesFilterUi() {
    var input = document.getElementById("filesFilterInput");
    if (input && input.value !== S.filesFilterText) input.value = S.filesFilterText;
    var clearBtn = document.getElementById("btnClearFilesFilter");
    if (clearBtn) clearBtn.disabled = !S.filesFilterText;
}

function isHiddenFileName(name) {
    return String(name || "").charAt(0) === ".";
}

function syncHiddenFilesUi() {
    var btn = document.getElementById("btnShowHiddenFiles");
    if (!btn) return;
    btn.classList.toggle("active", !!S.showHiddenFiles);
    btn.title = S.showHiddenFiles ? "隐藏 . 开头文件" : "显示 . 开头文件";
    btn.innerHTML = S.showHiddenFiles ? '<i class="bi bi-eye"></i>' : '<i class="bi bi-eye-slash"></i>';
}

function toggleHiddenFiles() {
    S.showHiddenFiles = !S.showHiddenFiles;
    syncHiddenFilesUi();
    renderFiles(S._lastFileEntries || []);
    if (S.fileLayoutMode === "tree") renderFileTree();
    saveFileBrowserUiPrefs();
}

function syncFileWriteControls() {
    ["btnUploadFile", "btnCreateFolder"].forEach(function(id) {
        var btn = document.getElementById(id);
        if (!btn) return;
        btn.disabled = !S.fileAllowWrite;
        btn.classList.toggle("disabled", !S.fileAllowWrite);
        if (!S.fileAllowWrite) btn.title = "当前目录仅允许读取";
        else if (id === "btnUploadFile") btn.title = "上传";
        else btn.title = "新建文件夹";
    });
}

function getVisibleFileColumns() {
    return FILE_COLUMN_DEFS.filter(function(def) {
        return S.fileColumns[def.key] !== false;
    });
}

function getFileColumnDef(key) {
    for (var i = 0; i < FILE_COLUMN_DEFS.length; i++) {
        if (FILE_COLUMN_DEFS[i].key === key) return FILE_COLUMN_DEFS[i];
    }
    return null;
}

function visibleFileColspan() {
    return Math.max(1, getVisibleFileColumns().length);
}

function fileColumnDisplayText(def, e) {
    if (!def) return "";
    if (!e) return def.label || "";
    if (def.key === "name") return String(e.name || "");
    if (def.key === "size") {
        if (e.type === "dir") return e.size > 0 ? formatSize(e.size) : "-";
        return formatSize(e.size || 0);
    }
    if (def.key === "mtime") return e.mtime ? new Date(e.mtime * 1000).toLocaleString() : "-";
    if (def.key === "owner") return e.owner || "-";
    if (def.key === "perm") return e.perm || "-";
    if (def.key === "ext") return fileEntryTypeLabel(e);
    if (def.key === "actions") return "下载 删除 提交 运行 取消收藏";
    return "";
}

function fileAutoFitSignature(entries) {
    var cols = getVisibleFileColumns().map(function(def) { return def.key; }).join(",");
    var entrySig = (entries || []).map(function(e) {
        return [
            e.name || "",
            e.type || "",
            e.size || 0,
            e.mtime || 0,
            e.owner || "",
            e.perm || "",
            e.ext || ""
        ].join("\u0001");
    }).sort().join("\u0002");
    return [
        normalizeFilePathKey(S.bookmarkViewActive ? "bookmarks" : S.filePath),
        cols,
        S.filesFilterText || "",
        S.showHiddenFiles ? "1" : "0",
        entrySig
    ].join("\u0003");
}

function ensureFileColumnMeasurer() {
    var measurer = document.getElementById("fileColumnTextMeasurer");
    if (!measurer) {
        measurer = document.createElement("table");
        measurer.id = "fileColumnTextMeasurer";
        document.body.appendChild(measurer);
    }
    measurer.className = "table table-sm file-column-text-measurer";
    return measurer;
}

function measureFileColumnCellWidth(def, e) {
    var measurer = ensureFileColumnMeasurer();
    measurer.innerHTML = '<tbody><tr>' + fileEntryCellHtml(def, e || {}, "") + '</tr></tbody>';
    var cell = measurer.querySelector("td");
    var width = cell ? Math.ceil(cell.getBoundingClientRect().width) : 0;
    measurer.innerHTML = "";
    return width;
}

function measureFileColumnHeaderWidth(def) {
    var measurer = ensureFileColumnMeasurer();
    measurer.innerHTML = '<thead><tr><th class="file-th file-col-' + def.key + '">' +
        esc(def.label || "") + (def.sortable ? fileSortIcon(def.key) : "") + '</th></tr></thead>';
    var th = measurer.querySelector("th");
    var width = th ? Math.ceil(th.getBoundingClientRect().width) : 0;
    measurer.innerHTML = "";
    return width;
}

function updateFileTableWidth() {
    var table = document.getElementById("fileTable");
    if (!table) return;
    var total = 0;
    getVisibleFileColumns().forEach(function(def) {
        total += Math.max(def.minWidth, Math.ceil(Number(S.fileColumnWidths[def.key] || def.minWidth)));
    });
    table.style.width = total + "px";
}

function autoFitFileColumnWidths(entries, force) {
    if (!S.fileColumnAutoWidth) return;
    var sig = fileAutoFitSignature(entries);
    if (!force && sig === S._fileAutoFitSignature) return;
    S._fileAutoFitSignature = sig;
    var cols = getVisibleFileColumns();
    cols.forEach(function(def) {
        var maxW = measureFileColumnHeaderWidth(def);
        (entries || []).forEach(function(e) {
            maxW = Math.max(maxW, measureFileColumnCellWidth(def, e));
        });
        var buffer = def.key === "name" ? 6 : def.key === "actions" ? 4 : 8;
        var cap = def.key === "name" ? 3200 : def.key === "mtime" ? 230 : def.key === "actions" ? 170 : 260;
        var w = Math.max(def.minWidth, Math.min(cap, maxW + buffer));
        S.fileColumnWidths[def.key] = w;
    });
}

function fileSortIcon(key) {
    if (S.fileSortCol !== key) return '<i class="bi bi-chevron-expand ms-1"></i>';
    return S.fileSortAsc ? '<i class="bi bi-chevron-up ms-1"></i>' : '<i class="bi bi-chevron-down ms-1"></i>';
}

function renderFileTableHeader() {
    var thead = document.getElementById("fileTableHead");
    var table = document.getElementById("fileTable");
    if (!thead || !table) return;
    var cols = getVisibleFileColumns();
    var colgroup = "<colgroup>";
    var totalWidth = 0;
    cols.forEach(function(def) {
        var w = Number(S.fileColumnWidths[def.key] || def.minWidth);
        w = Math.max(def.minWidth, w);
        totalWidth += w;
        colgroup += '<col data-file-col="' + def.key + '" style="width:' + w + 'px">';
    });
    colgroup += "</colgroup>";
    var oldColgroup = table.querySelector("colgroup");
    if (oldColgroup) oldColgroup.remove();
    table.insertAdjacentHTML("afterbegin", colgroup);
    var html = '<tr oncontextmenu="showFileColumnMenu(event)">';
    cols.forEach(function(def) {
        var w = Math.max(def.minWidth, Number(S.fileColumnWidths[def.key] || def.minWidth));
        var sortAttrs = def.sortable ? ' onclick="sortFiles(event,\'' + def.key + '\')"' : '';
        var sortClass = def.sortable ? " sortable" : "";
        html += '<th class="file-th file-col-' + def.key + sortClass + '" data-file-col="' + def.key + '" style="width:' + w + 'px"' + sortAttrs + '>';
        html += '<span class="file-th-label">' + def.label + (def.sortable ? fileSortIcon(def.key) : '') + '</span>';
        html += '<span class="file-col-resizer" onclick="event.preventDefault();event.stopPropagation()" onmousedown="startFileColumnResize(event,\'' + def.key + '\')"></span>';
        html += '</th>';
    });
    html += '</tr>';
    thead.innerHTML = html;
    table.style.width = totalWidth + "px";
}

function showFileColumnMenu(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    var menu = document.getElementById("fileColumnMenu");
    if (!menu) return false;
    var autoChecked = S.fileColumnAutoWidth ? " checked" : "";
    var html = '<div class="file-column-menu-title">显示栏目</div>';
    html += '<label class="file-column-menu-item">';
    html += '<input class="form-check-input me-2" type="checkbox"' + autoChecked + ' onchange="setFileColumnAutoWidth(this.checked)">';
    html += '<span>宽度自适应</span></label>';
    html += '<div class="file-column-menu-sep"></div>';
    FILE_COLUMN_DEFS.forEach(function(def) {
        var checked = S.fileColumns[def.key] !== false ? " checked" : "";
        var disabled = def.key === "name" ? " disabled" : "";
        html += '<label class="file-column-menu-item">';
        html += '<input class="form-check-input me-2" type="checkbox"' + checked + disabled + ' onchange="setFileColumnVisible(\'' + def.key + '\',this.checked)">';
        html += '<span>' + def.label + '</span></label>';
    });
    menu.innerHTML = html;
    var x = event ? event.clientX : 0;
    var y = event ? event.clientY : 0;
    menu.style.left = Math.min(x, window.innerWidth - 190) + "px";
    menu.style.top = Math.min(y, window.innerHeight - 260) + "px";
    menu.classList.remove("d-none");
    return false;
}

function setFileColumnAutoWidth(enabled) {
    S.fileColumnAutoWidth = !!enabled;
    S._fileAutoFitSignature = "";
    if (S.fileColumnAutoWidth) {
        if (S.bookmarkViewActive) renderBookmarkList({forceAutoFit: true});
        else renderFiles(S._lastFileEntries || [], {forceAutoFit: true});
    } else {
        renderFileTableHeader();
    }
    saveFileBrowserUiPrefs();
}

function setFileColumnVisible(key, visible) {
    if (key === "name" && !visible) return;
    S.fileColumns[key] = !!visible;
    S._fileAutoFitSignature = "";
    renderFileTableHeader();
    renderFiles(S._lastFileEntries || [], {forceAutoFit: true});
    saveFileBrowserUiPrefs();
}

function startFileColumnResize(event, key) {
    if (!event) return;
    event.preventDefault();
    event.stopPropagation();
    var def = getFileColumnDef(key);
    if (!def) return;
    if (S.fileColumnAutoWidth) {
        S.fileColumnAutoWidth = false;
        S._fileAutoFitSignature = "";
        saveFileBrowserUiPrefs();
    }
    S._fileColumnResize = {
        key: key,
        startX: event.clientX,
        startWidth: Math.max(def.minWidth, Number(S.fileColumnWidths[key] || def.minWidth)),
        minWidth: def.minWidth
    };
    S._suppressFileSortUntil = Date.now() + 500;
    document.body.classList.add("file-col-resizing");
}

function setFileLayoutMode(mode) {
    S.fileLayoutMode = mode === "tree" ? "tree" : "list";
    syncFileLayoutModeUi();
    saveFileBrowserUiPrefs();
}

function toggleFileSidebar() {
    setFileLayoutMode(S.fileLayoutMode === "tree" ? "list" : "tree");
}

function syncFileLayoutModeUi() {
    var treeMode = S.fileLayoutMode === "tree";
    var treeCol = document.getElementById("fileTreeCol");
    var treeSplitter = document.getElementById("fileTreeSplitter");
    var row = document.getElementById("fileRow");
    if (treeCol) {
        treeCol.classList.toggle("d-none", !treeMode);
        treeCol.style.flexBasis = Math.max(180, S._fileTreeWidth) + "px";
    }
    if (treeSplitter) treeSplitter.classList.toggle("d-none", !treeMode);
    if (row) row.classList.toggle("file-tree-mode", treeMode);
    var sidebarBtn = document.getElementById("btnFileSidebar");
    if (sidebarBtn) {
        sidebarBtn.classList.toggle("active", treeMode);
        sidebarBtn.title = treeMode ? "隐藏目录树侧栏" : "显示目录树侧栏";
    }
    if (treeMode) {
        expandFileTreeToPath(S.filePath || S.fileBrowserRoot || "/");
    }
}

function fileTreeCanBrowse(path) {
    var key = normalizeFilePathKey(path);
    return !!key && S._fileTreeReadable[key] !== false;
}

function loadFileTreePath(path, opts) {
    opts = opts || {};
    var key = normalizeFilePathKey(path || "/");
    if (S._fileTreeReadable[key] === false) {
        if (opts.notifyAccessDenied) showToast("无权限访问", 1000);
        return Promise.resolve([]);
    }
    if (S._fileTreeChildren[key]) return Promise.resolve(S._fileTreeChildren[key]);
    if (S._fileTreeLoading[key]) return S._fileTreeLoading[key];
    var body = document.getElementById("fileTreeBody");
    if (body && key === "/") body.innerHTML = '<div class="text-muted small p-2">正在加载目录树...</div>';
    S._fileTreeLoading[key] = fetch("/api/file-tree?path=" + encodeURIComponent(key))
        .then(function(r) {
            return r.json().then(function(d) {
                d._status = r.status;
                return d;
            });
        })
        .then(function(d) {
            delete S._fileTreeLoading[key];
            if (d.error) {
                if (d._status === 403 || d.error === "Access denied") {
                    S._fileTreeReadable[key] = false;
                    if (opts.notifyAccessDenied) showToast("无权限访问", 1000);
                }
                S._fileTreeChildren[key] = [];
                S._fileTreeTruncated[key] = false;
                renderFileTree();
                return [];
            }
            var normalized = normalizeFilePathKey(d.path || key);
            S._fileTreeReadable[normalized] = d.can_read !== false;
            S._fileTreeChildren[normalized] = Array.isArray(d.entries) ? d.entries : [];
            S._fileTreeChildren[normalized].forEach(function(child) {
                S._fileTreeReadable[normalizeFilePathKey(child.path)] = child.can_read !== false;
            });
            S._fileTreeTruncated[normalized] = !!d.truncated;
            renderFileTree();
            return S._fileTreeChildren[normalized];
        })
        .catch(function() {
            delete S._fileTreeLoading[key];
            S._fileTreeChildren[key] = [];
            renderFileTree();
            return [];
        });
    return S._fileTreeLoading[key];
}

function expandFileTreeToPath(path) {
    if (S.fileLayoutMode !== "tree") return;
    var key = normalizeFilePathKey(path || "/");
    var parts = key.split("/").filter(Boolean);
    var paths = ["/"];
    var cur = "";
    parts.forEach(function(part) {
        cur += "/" + part;
        paths.push(cur);
    });
    var chain = Promise.resolve();
    paths.forEach(function(nodePath, idx) {
        chain = chain.then(function() {
            if (idx < paths.length - 1) S._fileTreeExpanded[normalizeFilePathKey(nodePath)] = true;
            return loadFileTreePath(nodePath);
        });
    });
    chain.then(renderFileTree);
}

function fileTreeDepth(path) {
    return normalizeFilePathKey(path).split("/").filter(Boolean).length;
}

function fileTreeParentPath(path) {
    var key = normalizeFilePathKey(path);
    if (key === "/") return "/";
    var parts = key.split("/");
    parts.pop();
    return parts.join("/") || "/";
}

function visibleFileTreeChildren(path) {
    var key = normalizeFilePathKey(path);
    return sortFileEntriesForView((S._fileTreeChildren[key] || []).filter(function(child) {
        return S.showHiddenFiles || !isHiddenFileName(child.name);
    }));
}

function collectVisibleFileTreePaths(path, out) {
    var key = normalizeFilePathKey(path);
    out.push(key);
    if (!S._fileTreeExpanded[key]) return;
    visibleFileTreeChildren(key).forEach(function(child) {
        collectVisibleFileTreePaths(child.path, out);
    });
}

function collapseDeepestFileTreeLevel() {
    var visible = [];
    collectVisibleFileTreePaths("/", visible);
    var maxDepth = visible.reduce(function(max, path) {
        return Math.max(max, fileTreeDepth(path));
    }, 0);
    if (maxDepth <= 0) return;
    var parents = {};
    visible.forEach(function(path) {
        if (fileTreeDepth(path) === maxDepth) parents[fileTreeParentPath(path)] = true;
    });
    Object.keys(parents).forEach(function(path) {
        S._fileTreeExpanded[path] = false;
    });
    renderFileTree();
}

function collapseFileTreeToCurrentLevel() {
    var current = normalizeFilePathKey(S.filePath || "/");
    var parts = current.split("/").filter(Boolean);
    var keepExpanded = {};
    if (parts.length > 0) keepExpanded["/"] = true;
    var cur = "";
    for (var i = 0; i < parts.length - 1; i++) {
        cur += "/" + parts[i];
        keepExpanded[cur] = true;
    }
    S._fileTreeExpanded = keepExpanded;
    renderFileTree();
    if (parts.length > 0) expandFileTreeToPath(current);
}

function toggleFileTreeNode(eventOrPath, pathMaybe) {
    var event = pathMaybe !== undefined ? eventOrPath : null;
    var path = pathMaybe !== undefined ? pathMaybe : eventOrPath;
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    var key = normalizeFilePathKey(path);
    if (!fileTreeCanBrowse(key)) {
        showToast("无权限访问", 1000);
        return false;
    }
    S._fileTreeExpanded[key] = !S._fileTreeExpanded[key];
    if (S._fileTreeExpanded[key]) loadFileTreePath(key, {notifyAccessDenied: true}).then(renderFileTree);
    else renderFileTree();
    return false;
}

function onFileTreeNodeClick(eventOrPath, pathMaybe) {
    var event = pathMaybe !== undefined ? eventOrPath : null;
    var path = pathMaybe !== undefined ? pathMaybe : eventOrPath;
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    var key = normalizeFilePathKey(path);
    if (!fileTreeCanBrowse(key)) {
        showToast("无权限访问", 1000);
        return false;
    }
    if (S._fileTreeClickTimer) clearTimeout(S._fileTreeClickTimer);
    S._fileTreeClickTimer = setTimeout(function() {
        S._fileTreeClickTimer = null;
        browsePath(key, {closeEditorOnSuccess: true});
    }, 180);
    return false;
}

function onFileTreeNodeDoubleClick(event, path) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    var key = normalizeFilePathKey(path);
    if (!fileTreeCanBrowse(key)) {
        showToast("无权限访问", 1000);
        return false;
    }
    if (S._fileTreeClickTimer) {
        clearTimeout(S._fileTreeClickTimer);
        S._fileTreeClickTimer = null;
    }
    S._fileTreeExpanded[key] = !S._fileTreeExpanded[key];
    if (S._fileTreeExpanded[key]) {
        loadFileTreePath(key, {notifyAccessDenied: true}).then(renderFileTree);
        browsePath(key, {closeEditorOnSuccess: true});
    } else {
        renderFileTree();
    }
    return false;
}

function renderFileTree() {
    var body = document.getElementById("fileTreeBody");
    if (!body) return;
    body.innerHTML = renderFileTreeNode("/", "<root>", 0);
}

function renderFileTreeNode(path, name, depth) {
    var key = normalizeFilePathKey(path);
    var expanded = !!S._fileTreeExpanded[key];
    var children = visibleFileTreeChildren(key);
    var loading = !!S._fileTreeLoading[key];
    var current = normalizeFilePathKey(S.filePath) === key;
    var browseable = fileTreeCanBrowse(key);
    var toggleIcon = loading ? "bi-hourglass-split" : expanded ? "bi-chevron-down" : "bi-chevron-right";
    var folderCls = browseable ? "text-warning" : "text-secondary";
    var html = '<div class="file-tree-node ' + (current ? 'active ' : '') + (browseable ? '' : 'readonly ') + '" onclick="onFileTreeNodeClick(event,\'' + escAttr(key) + '\')" ondblclick="onFileTreeNodeDoubleClick(event,\'' + escAttr(key) + '\')" style="padding-left:' + (depth * 16 + 6) + 'px" title="' + escAttr(key) + '">';
    html += '<button class="file-tree-toggle" onclick="toggleFileTreeNode(event,\'' + escAttr(key) + '\')" title="展开/收起"><i class="bi ' + toggleIcon + '"></i></button>';
    html += '<span class="file-tree-name"><i class="bi bi-folder-fill ' + folderCls + ' me-1"></i>' + esc(name) + '</span>';
    html += '</div>';
    if (expanded) {
        children.forEach(function(child) {
            html += renderFileTreeNode(child.path, child.name, depth + 1);
        });
        if (S._fileTreeTruncated[key]) {
            html += '<div class="file-tree-truncated" style="padding-left:' + ((depth + 1) * 16 + 26) + 'px">已截断，目录过多</div>';
        }
    }
    return html;
}

/* 磁盘空间信息 */
function loadDiskInfo() {
    var path = S.filePath || S.fileBrowserRoot || "";
    fetch("/api/disk-info?path=" + encodeURIComponent(path))
        .then(function(r) { return r.json(); })
        .then(function(d) {
            var el = document.getElementById("diskInfoText");
            if (!el) return;
            if (d.error) { el.textContent = ""; return; }
            var total = formatSizeAuto(d.total);
            var avail = formatSizeAuto(d.avail);
            var used = formatSizeAuto(d.used);
            var pct = d.total > 0 ? Math.round((d.total - d.avail) / d.total * 100) : 0;
            var txt = d.mount + " 剩余:" + avail + "/" + total;
            if (d.user_usage > 0) {
                txt += " | 我的目录:" + formatSizeAuto(d.user_usage);
            } else if (d.du_computing) {
                txt += " | 我的目录:计算中...";
            }
            el.textContent = txt;
            el.title = "分区: " + d.mount + "\n总量: " + total + "\n已用: " + used + " (" + pct + "%)\n可用: " + avail;
            if (d.user_usage > 0) el.title += "\n我的目录 (" + (d.user_path || "") + "): " + formatSizeAuto(d.user_usage);
        })
        .catch(function() {});
}
function formatSizeAuto(bytes) {
    if (bytes < 0) return "-";
    if (bytes < 1024) return bytes + "B";
    if (bytes < 1048576) return (bytes/1024).toFixed(1) + "KB";
    if (bytes < 1073741824) return (bytes/1048576).toFixed(1) + "MB";
    if (bytes < 1099511627776) return (bytes/1073741824).toFixed(1) + "GB";
    return (bytes/1099511627776).toFixed(1) + "TB";
}

function fileFilterTerms(text) {
    return String(text || "").trim().toLowerCase().split(/\s+/).filter(Boolean);
}

function fileEntrySearchHaystack(e) {
    return [
        e.name || "",
        e.owner || "",
        e.perm || "",
        e.ext || "",
        fileEntryTypeLabel(e)
    ].join(" ").toLowerCase();
}

function fileEntryMatchesFilter(e, terms) {
    if (!terms || terms.length === 0) return true;
    var haystack = fileEntrySearchHaystack(e);
    return terms.every(function(term) { return haystack.indexOf(term) >= 0; });
}

function compareFileEntriesForView(a, b, dirFirst) {
    if (dirFirst !== false && a.type !== b.type) return a.type === "dir" ? -1 : 1;
    var sc = S.fileSortCol;
    var va = sc === "ext" ? fileEntryTypeLabel(a) : a[sc];
    var vb = sc === "ext" ? fileEntryTypeLabel(b) : b[sc];
    if (sc === "size" || sc === "mtime") {
        va = Number(va || 0);
        vb = Number(vb || 0);
        return S.fileSortAsc ? va - vb : vb - va;
    }
    va = String(va || "").toLowerCase();
    vb = String(vb || "").toLowerCase();
    return S.fileSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
}

function sortFileEntriesForView(entries, dirFirst) {
    return (entries || []).slice().sort(function(a, b) {
        return compareFileEntriesForView(a, b, dirFirst);
    });
}

function sortFiles(event, col) {
    if (typeof event === "string" && col === undefined) {
        col = event;
        event = null;
    }
    if (event && event.target && event.target.closest && event.target.closest(".file-col-resizer")) return;
    if (Date.now() < (S._suppressFileSortUntil || 0)) return;
    if (S.fileSortCol === col) S.fileSortAsc = !S.fileSortAsc;
    else { S.fileSortCol = col; S.fileSortAsc = true; }
    renderFiles(S._lastFileEntries || [], {preserveScroll: true});
    if (S.fileLayoutMode === "tree") renderFileTree();
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
    var splitter = document.getElementById("fileSplitter");
    var fsBtn = document.getElementById("btnFullscreen");
    if (S.fileViewMode === "full") {
        if (browserCol) {
            browserCol.style.flex = "1";
            browserCol.style.display = "";
        }
        if (editorCol) { editorCol.classList.add("d-none"); editorCol.style.flex = ""; }
        if (splitter) splitter.classList.add("d-none");
        if (fsBtn) fsBtn.innerHTML = '<i class="bi bi-arrows-fullscreen"></i>';
        if (fsBtn) fsBtn.title = "全屏预览";
    } else if (S.fileViewMode === "fullscreen") {
        if (browserCol) browserCol.style.display = "none";
        if (splitter) splitter.classList.add("d-none");
        if (editorCol) { editorCol.classList.remove("d-none"); editorCol.style.flex = "1"; }
        if (fsBtn) fsBtn.innerHTML = '<i class="bi bi-fullscreen-exit"></i>';
        if (fsBtn) fsBtn.title = "退出全屏";
    } else {
        if (browserCol) {
            browserCol.style.flex = "0 0 40%";
            browserCol.style.display = "";
        }
        if (editorCol) { editorCol.classList.remove("d-none"); editorCol.style.flex = "1"; }
        if (splitter) splitter.classList.remove("d-none");
        if (fsBtn) fsBtn.innerHTML = '<i class="bi bi-arrows-fullscreen"></i>';
        if (fsBtn) fsBtn.title = "全屏预览";
    }
}

function openEditorPane() {
    if (S.fileViewMode === "full") {
        S.fileViewMode = S.filePreviewFullscreenDefault ? "fullscreen" : "split";
    }
    applyFileViewMode();
}

function releasePreviewContent() {
    S._filePreviewSeq += 1;
    var container = document.getElementById("previewContainer");
    if (!container) return;
    container.querySelectorAll("iframe").forEach(function(frame) {
        try { frame.removeAttribute("src"); } catch(e) {}
    });
    container.innerHTML = "";
    container.scrollTop = 0;
    container.scrollLeft = 0;
}

function closeEditorNow() {
    if (S.fileViewMode === "fullscreen") {
        S.filePreviewFullscreenDefault = true;
        saveFileBrowserUiPrefs();
    }
    S.fileViewMode = "full";
    S.editingFile = null;
    S.editorMode = null;
    S.editDirty = false;
    S.previewKind = null;
    S.previewZoom = 1;
    var ta = document.getElementById("editorTextarea");
    if (ta) {
        ta.value = "";
        ta.style.display = "none";
        ta.oninput = null;
        ta.onselect = null;
    }
    var ep = document.getElementById("editorPath");
    if (ep) ep.textContent = "选择文件";
    var ph = document.getElementById("editorPlaceholder");
    if (ph) ph.style.display = "";
    releasePreviewContent();
    hideEl("previewContainer");
    syncPreviewZoomControls();
    syncEditorScriptRunButton();
    applyFileViewMode();
}

function closeEditor() {
    confirmUnsavedEditorChange().then(function(ok) {
        if (ok) closeEditorNow();
    });
}

function fullscreenEditor() {
    if (S.fileViewMode === "fullscreen") {
        S.fileViewMode = "split";
        S.filePreviewFullscreenDefault = false;
    } else {
        S.fileViewMode = "fullscreen";
        S.filePreviewFullscreenDefault = true;
    }
    saveFileBrowserUiPrefs();
    applyFileViewMode();
}

/* 复制选中内容 */
function editorCopy() {
    var ta = document.getElementById("editorTextarea");
    if (!ta) return;
    var sel = ta.value.substring(ta.selectionStart, ta.selectionEnd);
    if (sel) {
        navigator.clipboard.writeText(sel).then(function() {
            S._hasCopied = true;
            var btn = document.getElementById("btnPaste");
            if (btn) btn.classList.remove("d-none");
            showToast("已复制");
        }).catch(function() { showToast("复制失败"); });
    }
}

/* 粘贴 */
function editorPaste() {
    var ta = document.getElementById("editorTextarea");
    if (!ta) return;
    navigator.clipboard.readText().then(function(text) {
        var start = ta.selectionStart;
        var end = ta.selectionEnd;
        ta.value = ta.value.substring(0, start) + text + ta.value.substring(end);
        ta.selectionStart = ta.selectionEnd = start + text.length;
        S.editDirty = true;
        ta.focus();
    }).catch(function() { showToast("粘贴失败"); });
}

/* 拖拽分割条 */
(function() {
    var splitterEl = document.getElementById("fileSplitter");
    if (splitterEl) {
        splitterEl.addEventListener("mousedown", function(e) {
            e.preventDefault();
            S._splitterDragging = true;
            document.body.style.cursor = "col-resize";
            document.body.style.userSelect = "none";
        });
    }
    var treeSplitterEl = document.getElementById("fileTreeSplitter");
    if (treeSplitterEl) {
        treeSplitterEl.addEventListener("mousedown", function(e) {
            e.preventDefault();
            S._fileTreeDragging = true;
            document.body.style.cursor = "col-resize";
            document.body.style.userSelect = "none";
        });
    }
    document.addEventListener("mousemove", function(e) {
        if (S._fileColumnResize) {
            e.preventDefault();
            var resize = S._fileColumnResize;
            var nextW = Math.max(resize.minWidth, resize.startWidth + (e.clientX - resize.startX));
            S.fileColumnWidths[resize.key] = nextW;
            var col = document.querySelector('col[data-file-col="' + resize.key + '"]');
            var th = document.querySelector('th[data-file-col="' + resize.key + '"]');
            if (col) col.style.width = nextW + "px";
            if (th) th.style.width = nextW + "px";
            updateFileTableWidth();
            return;
        }
        if (S._fileTreeDragging) {
            e.preventDefault();
            var rowForTree = document.getElementById("fileRow");
            var treeCol = document.getElementById("fileTreeCol");
            if (!rowForTree || !treeCol) return;
            var rectTree = rowForTree.getBoundingClientRect();
            S._fileTreeWidth = Math.max(180, Math.min(520, e.clientX - rectTree.left));
            treeCol.style.flexBasis = S._fileTreeWidth + "px";
            return;
        }
        if (!S._splitterDragging) return;
        e.preventDefault();
        var browserCol = document.getElementById("fileBrowserCol");
        var editorCol = document.getElementById("fileEditorCol");
        if (!browserCol || !editorCol) return;
        var browserRect = browserCol.getBoundingClientRect();
        var editorRect = editorCol.getBoundingClientRect();
        var splitRect = splitterEl ? splitterEl.getBoundingClientRect() : {width: 6};
        var totalWidth = Math.max(1, editorRect.right - browserRect.left);
        var minPane = Math.min(220, Math.max(120, totalWidth * 0.28));
        var nextWidth = e.clientX - browserRect.left - (splitRect.width || 0) / 2;
        nextWidth = Math.max(minPane, Math.min(totalWidth - minPane, nextWidth));
        browserCol.style.flex = "0 0 " + nextWidth + "px";
        editorCol.style.flex = "1 1 0";
    });
    document.addEventListener("mouseup", function() {
        if (S._fileColumnResize) {
            S._fileColumnResize = null;
            S._suppressFileSortUntil = Date.now() + 350;
            document.body.classList.remove("file-col-resizing");
            saveFileBrowserUiPrefs();
        }
        if (S._fileTreeDragging) {
            S._fileTreeDragging = false;
            saveFileBrowserUiPrefs();
            document.body.style.cursor = "";
            document.body.style.userSelect = "";
        }
        if (S._splitterDragging) {
            S._splitterDragging = false;
            document.body.style.cursor = "";
            document.body.style.userSelect = "";
        }
    });
})();

function fileChildPath(name) {
    var base = normalizeFilePathKey(S.filePath || S.fileBrowserRoot || "/");
    return base === "/" ? "/" + name : base + "/" + name;
}

function fileEntryTypeLabel(e) {
    if (e.type === "dir") return "文件夹";
    var ext = String(e.ext || "").toLowerCase();
    if (!ext) ext = (String(e.name || "").split(".").pop() || "").toLowerCase();
    if (!ext || ext === String(e.name || "").toLowerCase()) return "文件";
    return ext;
}

function fileEntryActionHtml(e, fp) {
    var html = '<div class="file-actions d-flex gap-1">';
    if (e.type === "dir") {
        html += '<button class="btn btn-sm btn-outline-info py-0" onclick="event.stopPropagation();downloadFolder(\'' + escAttr(fp) + '\')" title="下载文件夹(zip)"><i class="bi bi-download"></i></button>';
        if (S.fileAllowWrite) {
            html += '<button class="btn btn-sm btn-outline-danger py-0" onclick="event.stopPropagation();deleteFile(\'' + escAttr(fp) + '\')" title="删除"><i class="bi bi-trash"></i></button>';
        }
    } else {
        html += '<a href="/api/file-download?path=' + encodeURIComponent(fp) + '" class="btn btn-sm btn-outline-info py-0" onclick="event.stopPropagation()" title="下载"><i class="bi bi-download"></i></a>';
        if (S.fileAllowWrite && e.name.endsWith('.sbatch')) {
            html += '<button class="btn btn-sm btn-outline-warning py-0" onclick="event.stopPropagation();submitSbatch(\'' + escAttr(fp) + '\')" title="提交sbatch作业"><i class="bi bi-send-fill"></i></button>';
        }
        if (S.fileAllowWrite && e.name.endsWith('.sh')) {
            html += '<button class="btn btn-sm btn-outline-success py-0" onclick="event.stopPropagation();runBash(\'' + escAttr(fp) + '\')" title="运行bash脚本"><i class="bi bi-play-fill"></i></button>';
        }
        if (S.fileAllowWrite) {
            html += '<button class="btn btn-sm btn-outline-danger py-0" onclick="event.stopPropagation();deleteFile(\'' + escAttr(fp) + '\')" title="删除"><i class="bi bi-trash"></i></button>';
        }
    }
    html += '</div>';
    return html;
}

function openFileEntry(path, type, name) {
    if (type === "dir") {
        browsePath(path);
        return;
    }
    confirmUnsavedEditorChange().then(function(ok) {
        if (!ok) return;
        if (isPreviewable(name || path)) previewFile(path);
        else viewFile(path);
    });
}

function fileEntryCellHtml(def, e, fp) {
    if (def.key === "name") {
        var icon = e.type === "dir" ? "bi-folder-fill text-warning" : getFileIcon(e.name);
        var isBookmarked = S.bookmarks.indexOf(fp) >= 0;
        var starCls = isBookmarked ? "bi-star-fill text-warning" : "bi-star";
        var starBtn = '<button class="btn btn-sm btn-link py-0 px-1" onclick="event.stopPropagation();toggleBookmark(\'' + escAttr(fp) + '\')" title="' + (isBookmarked ? '取消收藏' : '收藏') + '"><i class="bi ' + starCls + '"></i></button>';
        return '<td class="file-cell file-cell-name">' + starBtn + '<i class="bi ' + icon + ' me-1"></i><span class="file-name-text" title="' + escAttr(e.name) + '">' + esc(e.name) + '</span></td>';
    }
    if (def.key === "size") {
        var size = e.type === "dir" ? (e.size > 0 ? formatSize(e.size) : "-") : formatSize(e.size);
        return '<td class="file-cell text-end">' + size + '</td>';
    }
    if (def.key === "mtime") return '<td class="file-cell small text-muted">' + new Date(e.mtime * 1000).toLocaleString() + '</td>';
    if (def.key === "owner") return '<td class="file-cell">' + esc(e.owner || "-") + '</td>';
    if (def.key === "perm") return '<td class="file-cell font-monospace">' + esc(e.perm || "-") + '</td>';
    if (def.key === "ext") return '<td class="file-cell">' + esc(fileEntryTypeLabel(e)) + '</td>';
    if (def.key === "actions") return '<td class="file-cell">' + fileEntryActionHtml(e, fp) + '</td>';
    return '<td class="file-cell"></td>';
}

function renderFiles(entries, opts) {
    opts = opts || {};
    S._lastFileEntries = entries;
    // 如果收藏夹视图激活，显示收藏夹
    if (S.bookmarkViewActive) { renderBookmarkList(opts); return; }
    var browserCol = document.getElementById("fileBrowserCol");
    var oldScrollLeft = browserCol ? browserCol.scrollLeft : 0;
    var filterTerms = fileFilterTerms(S.filesFilterText);
    var filtered = entries.filter(function(e) {
        if (!S.showHiddenFiles && isHiddenFileName(e.name)) return false;
        return fileEntryMatchesFilter(e, filterTerms);
    });
    var sorted = sortFileEntriesForView(filtered, true);
    var tbody = document.getElementById("filesBody");
    if (!tbody) return;
    var cols = getVisibleFileColumns();
    autoFitFileColumnWidths(sorted, !!opts.forceAutoFit || !!opts.resetScroll);
    renderFileTableHeader();
    var html = "";
    sorted.forEach(function(e) {
        var fp = fileChildPath(e.name);
        html += '<tr class="file-row" onclick="openFileEntry(\'' + escAttr(fp) + '\',\'' + escAttr(e.type) + '\',\'' + escAttr(e.name) + '\')">';
        cols.forEach(function(def) { html += fileEntryCellHtml(def, e, fp); });
        html += '</tr>';
    });
    if (!html) {
        html = '<tr><td colspan="' + visibleFileColspan() + '" class="text-center text-muted py-3">无匹配文件</td></tr>';
    }
    tbody.innerHTML = html;
    if (browserCol && opts.resetScroll) {
        browserCol.scrollLeft = 0;
    } else if (browserCol && (opts.preserveScroll || !opts.resetScroll)) {
        browserCol.scrollLeft = oldScrollLeft;
    }
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

function hasUnsavedTextEdit() {
    return S.editorMode === "text" && !!S.editingFile && !!S.editDirty;
}

function ensureUnsavedEditorModal() {
    var el = document.getElementById("unsavedEditorModal");
    if (el) return el;
    el = document.createElement("div");
    el.className = "modal fade";
    el.id = "unsavedEditorModal";
    el.tabIndex = -1;
    el.innerHTML = [
        '<div class="modal-dialog modal-dialog-centered"><div class="modal-content">',
        '<div class="modal-header py-2"><h6 class="modal-title">存在未保存的编辑</h6></div>',
        '<div class="modal-body">',
        '<div class="small text-muted mb-2">当前文本文件还没有保存。</div>',
        '<div class="font-monospace small text-break" id="unsavedEditorPath"></div>',
        '</div>',
        '<div class="modal-footer py-2">',
        '<button type="button" class="btn btn-sm btn-outline-secondary" id="unsavedEditorCancel">取消</button>',
        '<button type="button" class="btn btn-sm btn-outline-danger" id="unsavedEditorDiscard">不保存</button>',
        '<button type="button" class="btn btn-sm btn-primary" id="unsavedEditorSave"><i class="bi bi-floppy me-1"></i>保存</button>',
        '</div></div></div>'
    ].join("");
    document.body.appendChild(el);
    return el;
}

function askUnsavedEditorChoice() {
    return new Promise(function(resolve) {
        var el = ensureUnsavedEditorModal();
        var pathEl = document.getElementById("unsavedEditorPath");
        if (pathEl) pathEl.textContent = S.editingFile || "当前文件";
        var modal = bootstrap.Modal.getOrCreateInstance(el, {backdrop: "static", keyboard: false});
        var done = false;
        var finish = function(choice) {
            if (done) return;
            done = true;
            el.removeEventListener("hidden.bs.modal", onHidden);
            document.getElementById("unsavedEditorSave").onclick = null;
            document.getElementById("unsavedEditorDiscard").onclick = null;
            document.getElementById("unsavedEditorCancel").onclick = null;
            modal.hide();
            resolve(choice);
        };
        var onHidden = function() { finish("cancel"); };
        document.getElementById("unsavedEditorSave").onclick = function() { finish("save"); };
        document.getElementById("unsavedEditorDiscard").onclick = function() { finish("discard"); };
        document.getElementById("unsavedEditorCancel").onclick = function() { finish("cancel"); };
        el.addEventListener("hidden.bs.modal", onHidden);
        modal.show();
    });
}

function confirmUnsavedEditorChange() {
    if (!hasUnsavedTextEdit()) return Promise.resolve(true);
    return askUnsavedEditorChoice().then(function(choice) {
        if (choice === "save") {
            return editorSave({silent: true}).then(function(ok) {
                if (ok) showToast("已保存", 1000);
                return ok;
            });
        }
        if (choice === "discard") {
            S.editDirty = false;
            return true;
        }
        return false;
    });
}

function ensurePreviewContainer() {
    var container = document.getElementById("previewContainer");
    if (!container) {
        var ec = document.getElementById("fileEditorCol");
        container = document.createElement("div");
        container.id = "previewContainer";
        container.className = "file-preview-container";
        if (ec) ec.appendChild(container);
    }
    return container;
}

function ensurePreviewZoomControls() {
    var controls = document.getElementById("previewZoomControls");
    if (controls) return controls;
    var ec = document.getElementById("fileEditorCol");
    controls = document.createElement("div");
    controls.id = "previewZoomControls";
    controls.className = "preview-zoom-controls d-none";
    controls.innerHTML = [
        '<button class="btn btn-sm btn-outline-light" id="btnPreviewZoomOut" onclick="zoomPreview(-0.1)" title="缩小"><i class="bi bi-dash-lg"></i></button>',
        '<button class="btn btn-sm btn-outline-light" id="btnPreviewZoomIn" onclick="zoomPreview(0.1)" title="放大"><i class="bi bi-plus-lg"></i></button>'
    ].join("");
    if (ec) ec.appendChild(controls);
    return controls;
}

function syncPreviewZoomControls() {
    var visible = S.editorMode === "text" || S.previewKind === "image";
    var controls = ensurePreviewZoomControls();
    controls.classList.toggle("d-none", !visible);
    var outBtn = document.getElementById("btnPreviewZoomOut");
    var inBtn = document.getElementById("btnPreviewZoomIn");
    if (outBtn) outBtn.disabled = S.previewZoom <= 0.5;
    if (inBtn) inBtn.disabled = S.previewZoom >= 3;
}

function filePreviewUrl(path) {
    return "/api/file-preview?path=" + encodeURIComponent(path);
}

function applyPreviewZoom() {
    var zoom = Math.max(0.5, Math.min(3, Number(S.previewZoom) || 1));
    S.previewZoom = Math.round(zoom * 100) / 100;
    var ta = document.getElementById("editorTextarea");
    if (ta && S.editorMode === "text") {
        ta.style.fontSize = Math.max(10, Math.round(12 * S.previewZoom)) + "px";
    }
    var scaleEl = document.getElementById("previewScaleWrap");
    if (scaleEl && S.previewKind === "image") {
        scaleEl.style.transform = "scale(" + S.previewZoom + ")";
    } else if (scaleEl) {
        scaleEl.style.transform = "";
    }
    syncPreviewZoomControls();
}

function zoomPreview(delta) {
    var step = (clampPreviewZoomStep(S.previewZoomStep) / 100) * (delta < 0 ? -1 : 1);
    setPreviewZoom(S.previewZoom + step, true);
}

function setPreviewZoom(nextZoom, notify) {
    S.previewZoom = Math.max(0.5, Math.min(3, nextZoom));
    applyPreviewZoom();
    if (notify) showPreviewZoomToast();
}

function showPreviewZoomToast() {
    var pct = Math.round(S.previewZoom * 100) + "%";
    var badge = document.getElementById("previewZoomToast");
    if (!badge) {
        var ec = document.getElementById("fileEditorCol");
        badge = document.createElement("div");
        badge.id = "previewZoomToast";
        badge.className = "preview-zoom-toast";
        if (ec) ec.appendChild(badge);
    }
    badge.textContent = pct;
    badge.classList.add("show");
    if (S._previewToastTimer) clearTimeout(S._previewToastTimer);
    S._previewToastTimer = setTimeout(function() {
        badge.classList.remove("show");
        S._previewToastTimer = null;
    }, 3000);
}

function viewFile(path) {
    openEditorPane();
    releasePreviewContent();
    fetch("/api/file-content?path=" + encodeURIComponent(path)).then(function(r) { return r.json(); }).then(function(d) {
        if (d.error) { alert(d.error); return; }
        S.editingFile = d.path;
        S.editorMode = "text";
        S.editDirty = false;
        S.previewKind = "text";
        S.previewZoom = 1;
        document.getElementById("editorPath").textContent = d.path;
        var ta = document.getElementById("editorTextarea");
        ta.value = d.content; ta.style.display = "block";
        ta.oninput = function() { S.editDirty = true; };
        // 监听选中事件以显示复制按钮
        ta.onselect = function() {
            var sel = ta.value.substring(ta.selectionStart, ta.selectionEnd);
            var btn = document.getElementById("btnCopy");
            if (btn) { if (sel) btn.classList.remove("d-none"); else btn.classList.add("d-none"); }
        };
        hideEl("editorPlaceholder"); hideEl("previewContainer");
        showEditorButtons(true);
        applyPreviewZoom();
    }).catch(function(e) { showToast("读取失败: " + e, 2000); });
}

function previewFile(path) {
    openEditorPane();
    releasePreviewContent();
    var previewSeq = S._filePreviewSeq;
    S.editingFile = path;
    S.editorMode = "preview";
    S.editDirty = false;
    document.getElementById("editorPath").textContent = path;
    hideEl("editorPlaceholder");
    document.getElementById("editorTextarea").style.display = "none";
    var container = ensurePreviewContainer();
    container.style.display = "block";
    S.previewZoom = 1;
    var ext = (path.split(".").pop() || "").toLowerCase();
    var url = filePreviewUrl(path);
    if (["png","jpg","jpeg","gif","bmp","svg","webp"].indexOf(ext) >= 0) {
        S.previewKind = "image";
        container.innerHTML = '<div id="previewScaleWrap" class="file-preview-scale-wrap file-preview-scale-wrap-image"><img id="previewImage" class="file-preview-image" src="' + escAttr(url) + '"></div>';
    } else if (ext === "pdf") {
        S.previewKind = "pdf";
        container.innerHTML = '<div id="previewScaleWrap" class="file-preview-scale-wrap file-preview-scale-wrap-pdf"><iframe id="previewPdfFrame" class="file-preview-pdf" title="PDF preview"></iframe></div>';
        var frame = document.getElementById("previewPdfFrame");
        if (frame) {
            setTimeout(function() {
                if (previewSeq === S._filePreviewSeq && frame.isConnected) frame.src = url;
            }, 0);
        }
    } else {
        S.previewKind = null;
        container.innerHTML = '<div class="text-muted py-3">此文件暂不支持预览</div>';
    }
    showEditorButtons(false);
    applyPreviewZoom();
    var bd = document.getElementById("btnDownload");
    if (bd) bd.classList.remove("d-none");
}

function showEditorButtons(showEditBtns) {
    ["btnUndo","btnRedo","btnSave","btnRunScript"].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) { if (showEditBtns) el.classList.remove("d-none"); else el.classList.add("d-none"); }
    });
    var bd = document.getElementById("btnDownload");
    if (bd) bd.classList.remove("d-none");
    // 复制按钮默认隐藏，选中文本时才显示
    var bc = document.getElementById("btnCopy");
    if (bc) bc.classList.add("d-none");
    // 粘贴按钮仅在有复制内容时显示
    var bp = document.getElementById("btnPaste");
    if (bp) { if (S._hasCopied && showEditBtns) bp.classList.remove("d-none"); else bp.classList.add("d-none"); }
    syncEditorScriptRunButton();
}

function currentEditorFileExt() {
    var path = String(S.editingFile || "");
    return (path.split(".").pop() || "").toLowerCase();
}

function isRunableScriptFile() {
    if (S.editorMode !== "text" || !S.editingFile) return false;
    return ["sh", "bash", "sbatch"].indexOf(currentEditorFileExt()) >= 0;
}

function syncEditorScriptRunButton() {
    var btn = document.getElementById("btnRunScript");
    if (!btn) return;
    var visible = isRunableScriptFile();
    btn.classList.toggle("d-none", !visible);
    if (!visible) return;
    if (currentEditorFileExt() === "sbatch") {
        btn.title = "提交 sbatch 脚本";
        btn.innerHTML = '<i class="bi bi-send-fill"></i>';
    } else {
        btn.title = "运行 bash 脚本";
        btn.innerHTML = '<i class="bi bi-play-fill"></i>';
    }
}

function editorUndo() { document.getElementById("editorTextarea").focus(); document.execCommand("undo"); }
function editorRedo() { document.getElementById("editorTextarea").focus(); document.execCommand("redo"); }
function runEditingScript() {
    if (!isRunableScriptFile()) {
        showToast("当前文件不是可运行脚本", 1000);
        return;
    }
    var path = S.editingFile;
    var ext = currentEditorFileExt();
    var proceed = function() {
        if (ext === "sbatch") submitSbatch(path);
        else runBash(path);
    };
    if (!S.editDirty) {
        proceed();
        return;
    }
    if (confirm("脚本有未保存修改，先保存再运行吗？")) {
        editorSave({silent: true}).then(function(ok) {
            if (ok) proceed();
        });
    } else {
        proceed();
    }
}
function editorSave(opts) {
    opts = opts || {};
    if (!S.editingFile || S.editorMode !== "text") {
        if (!opts.silent) showToast("当前预览不可保存", 1000);
        return Promise.resolve(false);
    }
    var content = document.getElementById("editorTextarea").value;
    return fetch("/api/file-save", {method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({path:S.editingFile,content:content})})
    .then(function(r){return r.json();}).then(function(d) {
        if (d.error) { alert("保存失败: " + d.error); return false; }
        S.editDirty = false;
        var btn = document.getElementById("btnSave");
        if (btn && !opts.silent) {
            btn.classList.remove("btn-outline-success"); btn.classList.add("btn-success");
            setTimeout(function() { btn.classList.remove("btn-success"); btn.classList.add("btn-outline-success"); }, 1000);
            showToast("保存成功", 1200);
        }
        syncEditorScriptRunButton();
        return true;
    }).catch(function(e) { alert("保存失败: " + e); return false; });
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
    var fromLocation = fileNavLocation();
    S.bookmarkViewActive = !S.bookmarkViewActive;
    syncBookmarkButton();
    if (!S._fileNavSuppress) pushFileNavTransition(fromLocation, fileNavLocation());
    if (S.bookmarkViewActive) {
        renderBookmarkList();
    } else {
        // 返回正常文件浏览
        if (S._lastFileEntries) renderFiles(S._lastFileEntries);
        else refreshFiles();
    }
    updateFileNavButtons();
}

function renderBookmarkList(opts) {
    opts = opts || {};
    var tbody = document.getElementById("filesBody");
    var browserCol = document.getElementById("fileBrowserCol");
    var oldScrollLeft = browserCol ? browserCol.scrollLeft : 0;
    var cols = getVisibleFileColumns();
    if (!S.bookmarks || S.bookmarks.length === 0) {
        renderFileTableHeader();
        tbody.innerHTML = '<tr><td colspan="' + visibleFileColspan() + '" class="text-center text-muted py-3"><i class="bi bi-star me-2"></i>暂无收藏，点击文件/文件夹旁的星标添加</td></tr>';
        return;
    }
    var bookmarkEntries = S.bookmarks.map(function(bm) {
        var name = bm.split("/").pop();
        var maybeDir = name.indexOf(".") < 0;
        return {
            name: name,
            type: maybeDir ? "dir" : "file",
            size: 0,
            mtime: 0,
            owner: "-",
            perm: "-",
            ext: maybeDir ? "文件夹" : ""
        };
    });
    autoFitFileColumnWidths(bookmarkEntries, !!opts.forceAutoFit || !!opts.resetScroll);
    renderFileTableHeader();
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
        html += '<tr class="file-row" onclick="' + clickFn + '">';
        cols.forEach(function(def) {
            if (def.key === "name") {
                html += '<td class="file-cell file-cell-name"><i class="bi bi-star-fill text-warning me-1"></i><i class="bi ' + icon + ' me-1"></i>';
                html += '<span class="file-name-text">' + esc(name) + '</span>';
                html += ' <small class="text-muted ms-2">' + esc(dir) + '</small></td>';
            } else if (def.key === "ext") {
                html += '<td class="file-cell">' + (maybeDir ? "文件夹" : esc(fileEntryTypeLabel({name:name,type:"file"}))) + '</td>';
            } else if (def.key === "actions") {
                html += '<td class="file-cell"><button class="btn btn-sm btn-outline-danger py-0" onclick="event.stopPropagation();toggleBookmark(\'' + escAttr(bm) + '\')" title="取消收藏"><i class="bi bi-x-lg"></i></button></td>';
            } else {
                html += '<td class="file-cell text-muted">-</td>';
            }
        });
        html += '</tr>';
    });
    tbody.innerHTML = html;
    if (browserCol && opts.resetScroll) {
        browserCol.scrollLeft = 0;
    } else if (browserCol && (opts.preserveScroll || !opts.resetScroll)) {
        browserCol.scrollLeft = oldScrollLeft;
    }
}

/* 退出收藏视图并浏览文件夹 */
function exitBookmarkAndBrowse(path) {
    var fromLocation = fileNavLocation();
    S.bookmarkViewActive = false;
    syncBookmarkButton();
    browsePath(path, {fromLocation: fromLocation});
}

/* 退出收藏视图并打开文件 */
function exitBookmarkAndView(path) {
    var fromLocation = fileNavLocation();
    confirmUnsavedEditorChange().then(function(ok) {
        if (!ok) return;
        S.bookmarkViewActive = false;
        syncBookmarkButton();
        // 先浏览到文件所在目录
        var dir = path.substring(0, path.lastIndexOf("/"));
        rememberFilesFilterForPath(S.filePath, S.filesFilterText);
        S.filePath = dir;
        var pi = document.getElementById("pathInput"); if (pi) pi.value = dir;
        if (isPreviewable(path)) previewFile(path);
        else viewFile(path);
        // 刷新文件列表显示该目录
        var url = "/api/files?path=" + encodeURIComponent(dir);
        if (S.showFolderSizes) url += "&folder_sizes=1";
        fetch(url).then(function(r) { return r.json(); }).then(function(d) {
            if (!d.error) {
                S.filePath = d.path;
                if (pi) pi.value = d.path;
                S._lastFileEntries = d.entries || [];
                applyFilesFilterForPath(d.path);
                if (!S._fileNavSuppress) pushFileNavTransition(fromLocation, {type: "path", path: d.path});
                renderFiles(d.entries || []);
                updateFileNavButtons();
            }
        }).catch(function() {});
    });
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

/* ===== 登录节点管理 ===== */
var LOGIN_PROC_SORT_LABELS = {
    cpu_pct: "CPU%",
    rss_kb: "RAM (RSS)",
    mem_pct: "MEM%",
    elapsed: "运行时间",
    pid: "PID"
};

function updateLoginNodeControls() {
    setText("loginSortLabel", LOGIN_PROC_SORT_LABELS[S.loginProcessSort] || "CPU%");
    setText("loginLimitLabel", "前 " + (S.loginProcessLimit || 50));
    setText("loginCmdModeLabel", S.loginDetailedCommands ? "详细命令" : "快速命令");
    setText("loginRootFilterLabel", S.loginExcludeRoot ? "隐藏 root" : "显示 root");
    var btn = document.getElementById("loginCmdModeBtn");
    if (btn) btn.classList.toggle("active", !!S.loginDetailedCommands);
    var rootBtn = document.getElementById("loginRootFilterBtn");
    if (rootBtn) rootBtn.classList.toggle("active", !!S.loginExcludeRoot);
    syncLoginProcessSearchUi();
}

function syncLoginProcessSearchUi() {
    var input = document.getElementById("loginProcessSearch");
    if (input && input.value !== S.loginProcessSearch) input.value = S.loginProcessSearch;
    var clearBtn = document.getElementById("btnClearLoginProcessSearch");
    if (clearBtn) clearBtn.disabled = !S.loginProcessSearch;
}

function setLoginProcessSearch(text) {
    S.loginProcessSearch = String(text || "").trim().toLowerCase();
    syncLoginProcessSearchUi();
    renderLoginNodeProcesses();
}

function clearLoginProcessSearch() {
    S.loginProcessSearch = "";
    syncLoginProcessSearchUi();
    renderLoginNodeProcesses();
}

function setLoginProcSort(col) {
    if (!LOGIN_PROC_SORT_LABELS[col]) return;
    S.loginProcessSort = col;
    S.loginSortCol = col;
    S.loginSortAsc = false;
    updateLoginNodeControls();
    loadLoginNodeInfo();
}

function setLoginProcLimit(limit) {
    var val = parseInt(limit, 10);
    if (!val || val < 1) val = 50;
    if (val > 500) val = 500;
    S.loginProcessLimit = val;
    updateLoginNodeControls();
    loadLoginNodeInfo();
}

function setLoginCommandMode(detailed) {
    S.loginDetailedCommands = !!detailed;
    updateLoginNodeControls();
    saveSettingsToServer({loginNodeDetailedCommands: S.loginDetailedCommands});
    loadLoginNodeInfo();
}

function setLoginExcludeRoot(excludeRoot) {
    S.loginExcludeRoot = !!excludeRoot;
    updateLoginNodeControls();
    saveSettingsToServer({loginNodeExcludeRoot: S.loginExcludeRoot});
    loadLoginNodeInfo();
}

function loadLoginNodeInfo() {
    var tbody = document.getElementById("loginNodeProcesses");
    if (tbody) tbody.innerHTML = '<tr><td colspan="9" class="text-center text-muted py-3"><div class="spinner-border spinner-border-sm me-2"></div>加载中...</td></tr>';
    updateLoginNodeControls();
    var sort = encodeURIComponent(S.loginProcessSort || "cpu_pct");
    var limit = parseInt(S.loginProcessLimit || 50, 10);
    var detail = S.loginDetailedCommands ? 1 : 0;
    var excludeRoot = S.loginExcludeRoot ? 1 : 0;
    fetch("/api/login-node/info?sort=" + sort + "&limit=" + limit + "&detail=" + detail + "&exclude_root=" + excludeRoot)
        .then(function(r) {
            if (!r.ok) throw new Error("HTTP " + r.status);
            return r.json();
        })
        .then(function(d) {
            if (d.error) { showToast("获取失败: " + d.error); return; }
            var load1 = Number(d.load_1 || 0);
            var load5 = Number(d.load_5 || 0);
            var load15 = Number(d.load_15 || 0);
            var memTotal = Number(d.mem_total || 0);
            var memUsed = Number(d.mem_used || 0);
            var memFree = Number(d.mem_free || 0);
            var memAvail = Number(d.mem_available || memFree || 0);
            setText("loginNodeHostname", "🖥️ " + (d.hostname || "-"));
            setText("loginNodeUptime", d.uptime || "");
            var procText = "进程: " + (d.process_count_total || 0);
            if (d.exclude_root && Number(d.root_process_count || 0) > 0) {
                procText += " / 已隐藏 root " + Number(d.root_process_count || 0);
            }
            setText("loginNodeOnline", "在线用户: " + (d.online_users || 0) + " | " + procText);
            setText("loginNodeLoad", load1.toFixed(2) + " / " + load5.toFixed(2) + " / " + load15.toFixed(2));
            setText("loginNodeCpus", d.cpus || "-");
            var memPct = memTotal > 0 ? Math.round(memUsed / memTotal * 100) : 0;
            setBar("loginNodeMemBar", memPct, formatSizeAuto(memUsed) + "/" + formatSizeAuto(memTotal) + " (" + memPct + "%)");
            setText("loginNodeMemText", formatSizeAuto(memUsed) + " / " + formatSizeAuto(memTotal));
            setText("loginNodeMemAvail", formatSizeAuto(memAvail));
            S._loginProcs = Array.isArray(d.processes) ? d.processes : [];
            if (LOGIN_PROC_SORT_LABELS[d.process_sort]) S.loginProcessSort = d.process_sort;
            if (d.process_limit) S.loginProcessLimit = Number(d.process_limit) || S.loginProcessLimit;
            if (typeof d.exclude_root === "boolean") S.loginExcludeRoot = d.exclude_root;
            updateLoginNodeControls();
            renderLoginNodeProcesses();
        })
        .catch(function(e) {
            showToast("获取登录节点信息失败: " + e);
        });
}

function sortLoginProcs(col) {
    if (S.loginSortCol === col) S.loginSortAsc = !S.loginSortAsc;
    else { S.loginSortCol = col; S.loginSortAsc = (col === 'user' || col === 'state' || col === 'elapsed'); }
    renderLoginNodeProcesses();
}

function renderLoginNodeProcesses() {
    var procs = (S._loginProcs || []).slice();
    var tbody = document.getElementById("loginNodeProcesses");
    if (!tbody) return;
    var q = S.loginProcessSearch;
    if (q) {
        procs = procs.filter(function(p) {
            var pid = String(p.pid || "");
            var user = String(p.user || "").toLowerCase();
            var cmd = String(p.cmd || p.command || "").toLowerCase();
            return pid.indexOf(q) >= 0 || user.indexOf(q) >= 0 || cmd.indexOf(q) >= 0;
        });
    }
    if (!procs.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="text-center text-muted py-3">' + (q ? '无匹配进程' : '无进程') + '</td></tr>';
        return;
    }
    /* 排序 */
    var col = S.loginSortCol, asc = S.loginSortAsc;
    procs.sort(function(a, b) {
        var va, vb;
        if (col === 'elapsed') {
            va = Number(a.elapsed_seconds) || 0; vb = Number(b.elapsed_seconds) || 0;
            return asc ? va - vb : vb - va;
        }
        if (col === 'pid' || col === 'cpu_pct' || col === 'mem_pct' || col === 'rss_kb') {
            va = Number(a[col]) || 0; vb = Number(b[col]) || 0;
            return asc ? va - vb : vb - va;
        }
        va = String(a[col] || ''); vb = String(b[col] || '');
        return asc ? va.localeCompare(vb) : vb.localeCompare(va);
    });
    var html = '';
    procs.forEach(function(p) {
        var rssKb = Number(p.rss_kb || 0);
        var cpuPct = Number(p.cpu_pct || 0);
        var memPct = Number(p.mem_pct || 0);
        var cmd = String(p.cmd || "");
        var rss = rssKb > 1048576 ? (rssKb / 1048576).toFixed(1) + "G" :
                  rssKb > 1024 ? (rssKb / 1024).toFixed(1) + "M" : rssKb + "K";
        var cpuCls = cpuPct > 50 ? 'text-danger fw-bold' : cpuPct > 10 ? 'text-warning' : '';
        var cmdShort = cmd.length > 80 ? cmd.substring(0, 77) + '...' : cmd;
        var userCls = p.user === S.clusterUsername ? 'text-info fw-bold' : '';
        html += '<tr>';
        html += '<td class="' + userCls + '">' + esc(p.user) + '</td>';
        html += '<td>' + p.pid + '</td>';
        html += '<td><span class="badge bg-' + procStateBadge(p.state) + '">' + p.state + '</span></td>';
        html += '<td class="' + cpuCls + '">' + cpuPct.toFixed(1) + '</td>';
        html += '<td>' + memPct.toFixed(1) + '</td>';
        html += '<td>' + rss + '</td>';
        html += '<td class="small">' + esc(p.elapsed) + '</td>';
        html += '<td class="small" title="' + esc(cmd) + '" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(cmdShort) + '</td>';
        html += '<td><button class="btn btn-sm btn-outline-danger py-0" onclick="killLoginProcess(' + p.pid + ')" title="终止进程"><i class="bi bi-x-circle"></i></button></td>';
        html += '</tr>';
    });
    tbody.innerHTML = html;
}

function procStateBadge(state) {
    if (state === 'R') return 'success';
    if (state === 'S') return 'secondary';
    if (state === 'D') return 'warning';
    if (state === 'Z') return 'danger';
    if (state === 'T') return 'info';
    return 'secondary';
}

function killLoginProcess(pid) {
    if (!confirm("确定终止进程 " + pid + " ？")) return;
    fetch("/api/login-node/kill", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({pid: pid})
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
        showToast(d.message || (d.success ? "已终止" : "失败"));
        if (d.success) setTimeout(loadLoginNodeInfo, 1000);
    })
    .catch(function(e) { alert("请求失败: " + e); });
}
