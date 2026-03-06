const $ = (id) => document.getElementById(id);

const DOM = {
  headerStatus: $("headerStatus"),
  registerStatus: $("registerStatus"),
  maintainStatus: $("maintainStatus"),
  successCount: $("successCount"),
  failCount: $("failCount"),
  currentProxyStrategy: $("currentProxyStrategy"),
  currentProxy: $("currentProxy"),
  currentProxyUpstream: $("currentProxyUpstream"),
  currentProxyRegion: $("currentProxyRegion"),
  watchdogRegisterRestarts: $("watchdogRegisterRestarts"),
  watchdogMaintainRestarts: $("watchdogMaintainRestarts"),
  targetCount: $("targetCount"),
  syncStatus: $("syncStatus"),
  lastSyncAt: $("lastSyncAt"),
  lastSyncResult: $("lastSyncResult"),
  poolTotal: $("poolTotal"),
  poolValid: $("poolValid"),
  poolUnknown: $("poolUnknown"),
  poolExpired: $("poolExpired"),
  poolThreshold: $("poolThreshold"),
  poolGap: $("poolGap"),

  startCount: $("startCount"),
  startBtn: $("startBtn"),
  stopBtn: $("stopBtn"),
  maintainBtn: $("maintainBtn"),
  stopMaintainBtn: $("stopMaintainBtn"),
  stopAllBtn: $("stopAllBtn"),
  mergeBtn: $("mergeBtn"),
  syncNowBtn: $("syncNowBtn"),
  actionMsg: $("actionMsg"),

  proxyInput: $("proxyInput"),
  easyProxiesEnabled: $("easyProxiesEnabled"),
  easyProxiesListenProxy: $("easyProxiesListenProxy"),
  easyProxiesApiUrl: $("easyProxiesApiUrl"),
  easyProxiesPassword: $("easyProxiesPassword"),
  easyProxiesSubscriptionEnabled: $("easyProxiesSubscriptionEnabled"),
  easyProxiesSubscriptionUrl: $("easyProxiesSubscriptionUrl"),
  easyProxiesSubscriptionRefreshMinutes: $("easyProxiesSubscriptionRefreshMinutes"),
  easyProxiesRefreshBeforeTask: $("easyProxiesRefreshBeforeTask"),
  easyProxiesRetryForever: $("easyProxiesRetryForever"),
  easyProxiesRetryTimes: $("easyProxiesRetryTimes"),
  easyProxiesRetryIntervalSeconds: $("easyProxiesRetryIntervalSeconds"),
  easyProxiesRotateIntervalSeconds: $("easyProxiesRotateIntervalSeconds"),
  easyProxiesNodeRotationEnabled: $("easyProxiesNodeRotationEnabled"),
  easyProxiesNodeRegisterQuota: $("easyProxiesNodeRegisterQuota"),
  easyProxiesNodeMaintainQuota: $("easyProxiesNodeMaintainQuota"),

  autoMaintain: $("autoMaintain"),
  autoRegister: $("autoRegister"),
  autoTaskPriority: $("autoTaskPriority"),
  guaranteeEnabled: $("guaranteeEnabled"),
  maintainInterval: $("maintainInterval"),
  maintainIntervalHours: $("maintainIntervalHours"),
  autoRegisterIntervalHours: $("autoRegisterIntervalHours"),
  autoRegisterBatchSize: $("autoRegisterBatchSize"),
  guaranteeTargetAccounts: $("guaranteeTargetAccounts"),
  guaranteeWindowHours: $("guaranteeWindowHours"),
  minAccounts: $("minAccounts"),
  registerDefaultCount: $("registerDefaultCount"),

  syncEnabled: $("syncEnabled"),
  syncAfterRegister: $("syncAfterRegister"),
  syncAfterMaintain: $("syncAfterMaintain"),
  syncUrl: $("syncUrl"),
  syncAuthMode: $("syncAuthMode"),
  syncLoginUrl: $("syncLoginUrl"),
  syncApiKey: $("syncApiKey"),
  syncAuthHeaderName: $("syncAuthHeaderName"),
  syncAuthQueryName: $("syncAuthQueryName"),
  syncTimeoutSeconds: $("syncTimeoutSeconds"),

  taskWatchdogEnabled: $("taskWatchdogEnabled"),
  taskStallRestartEnabled: $("taskStallRestartEnabled"),
  taskStallTimeoutSeconds: $("taskStallTimeoutSeconds"),
  taskStallRestartMax: $("taskStallRestartMax"),
  proxyFailGuardEnabled: $("proxyFailGuardEnabled"),
  proxyFailGuardThreshold: $("proxyFailGuardThreshold"),
  proxyFailGuardPauseSeconds: $("proxyFailGuardPauseSeconds"),

  saveConfigBtn: $("saveConfigBtn"),
  testProxyBtn: $("testProxyBtn"),
  testEasyProxiesBtn: $("testEasyProxiesBtn"),
  syncEasyProxiesSubBtn: $("syncEasyProxiesSubBtn"),
  monitorProxyBtn: $("monitorProxyBtn"),
  configMsg: $("configMsg"),
  logs: $("logs"),
};

function setMessage(el, msg, isError = false) {
  if (!el) return;
  el.textContent = msg || "";
  el.className = isError ? "hint error" : "hint";
}

function appendLog(event) {
  if (!DOM.logs) return;
  const row = document.createElement("div");
  const level = String(event.level || "info");
  row.className = `log-row ${level}`;
  row.textContent = `[${event.ts || "--:--:--"}] [${level.toUpperCase()}] ${event.message || ""}`;
  DOM.logs.appendChild(row);
  if (DOM.logs.children.length > 1200) {
    DOM.logs.removeChild(DOM.logs.firstChild);
  }
  DOM.logs.scrollTop = DOM.logs.scrollHeight;
}

async function requestJSON(url, options = {}) {
  const res = await fetch(url, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || data.error || `HTTP ${res.status}`);
  }
  return data;
}

function formatTs(ts) {
  const num = Number(ts || 0);
  if (!num) return "-";
  const d = new Date(num * 1000);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

function renderConfig(cfg) {
  DOM.proxyInput.value = cfg.proxy || "";

  DOM.easyProxiesEnabled.checked = cfg.easyproxies_enabled !== false;
  DOM.easyProxiesListenProxy.value = cfg.easyproxies_listen_proxy || "http://127.0.0.1:2323";
  DOM.easyProxiesApiUrl.value = cfg.easyproxies_api_url || "http://127.0.0.1:7840";
  DOM.easyProxiesPassword.placeholder = cfg.easyproxies_password_preview || "留空表示不修改";
  DOM.easyProxiesSubscriptionEnabled.checked = !!cfg.easyproxies_subscription_enabled;
  DOM.easyProxiesSubscriptionUrl.value = cfg.easyproxies_subscription_url || "";
  DOM.easyProxiesSubscriptionRefreshMinutes.value = cfg.easyproxies_subscription_refresh_minutes || 60;
  DOM.easyProxiesRefreshBeforeTask.checked = cfg.easyproxies_refresh_before_task !== false;
  DOM.easyProxiesRetryForever.checked = cfg.easyproxies_retry_forever !== false;
  DOM.easyProxiesRetryTimes.value = cfg.easyproxies_retry_times || 3;
  DOM.easyProxiesRetryIntervalSeconds.value = cfg.easyproxies_retry_interval_seconds || 8;
  DOM.easyProxiesRotateIntervalSeconds.value = cfg.easyproxies_rotate_interval_seconds || 120;
  DOM.easyProxiesNodeRotationEnabled.checked = cfg.easyproxies_node_rotation_enabled !== false;
  DOM.easyProxiesNodeRegisterQuota.value = cfg.easyproxies_node_register_quota || 5;
  DOM.easyProxiesNodeMaintainQuota.value = cfg.easyproxies_node_maintain_quota || 20;

  DOM.autoMaintain.checked = !!cfg.auto_maintain;
  DOM.autoRegister.checked = !!cfg.auto_register;
  DOM.autoTaskPriority.value = cfg.auto_task_priority || "maintain";
  DOM.guaranteeEnabled.checked = cfg.guarantee_enabled !== false;
  DOM.maintainInterval.value = cfg.maintain_interval_minutes || 30;
  DOM.maintainIntervalHours.value = cfg.maintain_interval_hours || 4;
  DOM.autoRegisterIntervalHours.value = cfg.auto_register_interval_hours || 4;
  DOM.autoRegisterBatchSize.value = cfg.auto_register_batch_size || cfg.max_replenish_per_round || 20;
  DOM.guaranteeTargetAccounts.value = cfg.guarantee_target_accounts || 200;
  DOM.guaranteeWindowHours.value = cfg.guarantee_window_hours || 4;
  DOM.minAccounts.value = cfg.min_accounts || 20;
  DOM.registerDefaultCount.value = cfg.register_default_count || 1;

  DOM.syncEnabled.checked = !!cfg.account_sync_enabled;
  DOM.syncAfterRegister.checked = cfg.account_sync_after_register !== false;
  DOM.syncAfterMaintain.checked = cfg.account_sync_after_maintain !== false;
  DOM.syncUrl.value = cfg.account_sync_url || "";
  DOM.syncAuthMode.value = cfg.account_sync_auth_mode || "session";
  DOM.syncLoginUrl.value = cfg.account_sync_login_url || "";
  DOM.syncApiKey.placeholder = cfg.account_sync_api_key_preview || "留空表示不修改";
  DOM.syncAuthHeaderName.value = cfg.account_sync_auth_header_name || "X-API-Key";
  DOM.syncAuthQueryName.value = cfg.account_sync_auth_query_name || "api_key";
  DOM.syncTimeoutSeconds.value = cfg.account_sync_timeout_seconds || 20;

  DOM.taskWatchdogEnabled.checked = cfg.task_watchdog_enabled !== false;
  DOM.taskStallRestartEnabled.checked = cfg.task_stall_restart_enabled !== false;
  DOM.taskStallTimeoutSeconds.value = cfg.task_stall_timeout_seconds || 300;
  DOM.taskStallRestartMax.value = cfg.task_stall_restart_max ?? 5;
  DOM.proxyFailGuardEnabled.checked = cfg.proxy_fail_guard_enabled !== false;
  DOM.proxyFailGuardThreshold.value = cfg.proxy_fail_guard_threshold || 3;
  DOM.proxyFailGuardPauseSeconds.value = cfg.proxy_fail_guard_pause_seconds || 60;

  DOM.startCount.value = cfg.register_default_count || 1;
}

async function loadConfig() {
  try {
    const cfg = await requestJSON("/api/config");
    renderConfig(cfg);
  } catch (err) {
    setMessage(DOM.configMsg, `加载配置失败: ${err.message}`, true);
  }
}

async function saveConfig() {
  const autoBatch = Math.max(1, Number(DOM.autoRegisterBatchSize.value || 20));

  const payload = {
    proxy: DOM.proxyInput.value.trim(),

    easyproxies_enabled: DOM.easyProxiesEnabled.checked,
    easyproxies_listen_proxy: DOM.easyProxiesListenProxy.value.trim(),
    easyproxies_api_url: DOM.easyProxiesApiUrl.value.trim(),
    easyproxies_password: DOM.easyProxiesPassword.value.trim(),
    easyproxies_subscription_enabled: DOM.easyProxiesSubscriptionEnabled.checked,
    easyproxies_subscription_url: DOM.easyProxiesSubscriptionUrl.value.trim(),
    easyproxies_subscription_refresh_minutes: Number(DOM.easyProxiesSubscriptionRefreshMinutes.value || 60),
    easyproxies_refresh_before_task: DOM.easyProxiesRefreshBeforeTask.checked,
    easyproxies_retry_forever: DOM.easyProxiesRetryForever.checked,
    easyproxies_retry_times: Number(DOM.easyProxiesRetryTimes.value || 3),
    easyproxies_retry_interval_seconds: Number(DOM.easyProxiesRetryIntervalSeconds.value || 8),
    easyproxies_rotate_interval_seconds: Number(DOM.easyProxiesRotateIntervalSeconds.value || 120),
    easyproxies_node_rotation_enabled: DOM.easyProxiesNodeRotationEnabled.checked,
    easyproxies_node_register_quota: Number(DOM.easyProxiesNodeRegisterQuota.value || 5),
    easyproxies_node_maintain_quota: Number(DOM.easyProxiesNodeMaintainQuota.value || 20),

    // Disable legacy modes to avoid strategy conflict.
    proxy_pool_enabled: false,
    proxy_subscription_enabled: false,

    auto_maintain: DOM.autoMaintain.checked,
    auto_register: DOM.autoRegister.checked,
    auto_task_priority: DOM.autoTaskPriority.value || "maintain",
    guarantee_enabled: DOM.guaranteeEnabled.checked,
    maintain_interval_minutes: Number(DOM.maintainInterval.value || 30),
    maintain_interval_hours: Number(DOM.maintainIntervalHours.value || 4),
    auto_register_interval_hours: Number(DOM.autoRegisterIntervalHours.value || 4),
    auto_register_batch_size: autoBatch,
    guarantee_target_accounts: Number(DOM.guaranteeTargetAccounts.value || 200),
    guarantee_window_hours: Number(DOM.guaranteeWindowHours.value || 4),
    min_accounts: Number(DOM.minAccounts.value || 20),
    // Keep legacy key aligned with auto_register_batch_size to avoid conflicts.
    max_replenish_per_round: autoBatch,
    register_default_count: Number(DOM.registerDefaultCount.value || 1),

    account_sync_enabled: DOM.syncEnabled.checked,
    account_sync_url: DOM.syncUrl.value.trim(),
    account_sync_auth_mode: DOM.syncAuthMode.value,
    account_sync_login_url: DOM.syncLoginUrl.value.trim(),
    account_sync_api_key: DOM.syncApiKey.value.trim(),
    account_sync_auth_header_name: DOM.syncAuthHeaderName.value.trim(),
    account_sync_auth_query_name: DOM.syncAuthQueryName.value.trim(),
    account_sync_timeout_seconds: Number(DOM.syncTimeoutSeconds.value || 20),
    account_sync_after_register: DOM.syncAfterRegister.checked,
    account_sync_after_maintain: DOM.syncAfterMaintain.checked,

    task_watchdog_enabled: DOM.taskWatchdogEnabled.checked,
    task_stall_restart_enabled: DOM.taskStallRestartEnabled.checked,
    task_stall_timeout_seconds: Number(DOM.taskStallTimeoutSeconds.value || 300),
    task_stall_restart_max: Number(DOM.taskStallRestartMax.value || 5),
    proxy_fail_guard_enabled: DOM.proxyFailGuardEnabled.checked,
    proxy_fail_guard_threshold: Number(DOM.proxyFailGuardThreshold.value || 3),
    proxy_fail_guard_pause_seconds: Number(DOM.proxyFailGuardPauseSeconds.value || 60),
  };

  try {
    DOM.saveConfigBtn.disabled = true;
    await requestJSON("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    DOM.easyProxiesPassword.value = "";
    DOM.syncApiKey.value = "";
    setMessage(DOM.configMsg, "配置已保存");
  } catch (err) {
    setMessage(DOM.configMsg, `保存失败: ${err.message}`, true);
  } finally {
    DOM.saveConfigBtn.disabled = false;
  }
}

function renderHeaderBadge(registerStatus, maintainStatus) {
  let text = "空闲";
  let klass = "badge idle";
  if (registerStatus === "running") {
    text = "补号运行中";
    klass = "badge running";
  } else if (registerStatus === "stopping") {
    text = "补号停止中";
    klass = "badge stopping";
  } else if (maintainStatus === "running") {
    text = "维护运行中";
    klass = "badge running";
  } else if (maintainStatus === "stopping") {
    text = "维护停止中";
    klass = "badge stopping";
  }
  DOM.headerStatus.textContent = text;
  DOM.headerStatus.className = klass;
}

function renderPool(pool) {
  DOM.poolTotal.textContent = pool.total_files ?? 0;
  DOM.poolValid.textContent = pool.valid_count ?? 0;
  DOM.poolUnknown.textContent = pool.unknown_count ?? 0;
  DOM.poolExpired.textContent = pool.expired_count ?? 0;
  DOM.poolThreshold.textContent = pool.threshold ?? 0;
  DOM.poolGap.textContent = pool.gap ?? 0;
}

async function loadStatus() {
  try {
    const data = await requestJSON("/api/status");
    const registerStatus = data.register_status || "idle";
    const maintainStatus = data.maintain_status || "idle";

    DOM.registerStatus.textContent = registerStatus;
    DOM.maintainStatus.textContent = maintainStatus;
    DOM.successCount.textContent = data.success || 0;
    DOM.failCount.textContent = data.fail || 0;
    DOM.currentProxyStrategy.textContent = data.current_proxy_strategy || "-";
    DOM.currentProxy.textContent = data.current_proxy || "-";
    DOM.currentProxyUpstream.textContent = data.current_proxy_upstream || "-";
    DOM.currentProxyRegion.textContent = data.current_proxy_region || "-";
    DOM.watchdogRegisterRestarts.textContent = data.watchdog_register_restarts || 0;
    DOM.watchdogMaintainRestarts.textContent = data.watchdog_maintain_restarts || 0;
    DOM.targetCount.textContent = data.register_target || 0;
    DOM.syncStatus.textContent = data.sync_status || "idle";
    DOM.lastSyncAt.textContent = formatTs(data.last_sync_at);

    if (data.last_sync_ok === true) {
      DOM.lastSyncResult.textContent = `成功(${data.last_sync_count || 0})`;
    } else if (data.last_sync_ok === false) {
      DOM.lastSyncResult.textContent = `失败: ${data.last_sync_error || "unknown"}`;
    } else {
      DOM.lastSyncResult.textContent = "-";
    }

    renderHeaderBadge(registerStatus, maintainStatus);
    renderPool(data.pool || {});

    const hasRunningOrStopping =
      registerStatus === "running" ||
      registerStatus === "stopping" ||
      maintainStatus === "running" ||
      maintainStatus === "stopping";

    DOM.startBtn.disabled = registerStatus !== "idle" || maintainStatus !== "idle";
    DOM.stopBtn.disabled = registerStatus !== "running";
    DOM.maintainBtn.disabled = maintainStatus !== "idle" || registerStatus !== "idle";
    DOM.stopMaintainBtn.disabled = maintainStatus !== "running";
    DOM.stopAllBtn.disabled = !hasRunningOrStopping;
    DOM.syncNowBtn.disabled = (data.sync_status || "idle") === "running";
  } catch (err) {
    setMessage(DOM.actionMsg, `状态拉取失败: ${err.message}`, true);
  }
}

async function startRegister() {
  const count = Math.max(1, Number(DOM.startCount.value || 1));
  try {
    DOM.startBtn.disabled = true;
    const data = await requestJSON("/api/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count }),
    });
    const total = Number(data.target_count || count);
    const segment = Number(data.segment_count || total);
    const remaining = Number(data.remaining_count || 0);
    setMessage(
      DOM.actionMsg,
      `补号任务已启动: 总量=${total}, 当前分段=${segment}, 剩余=${remaining}, 策略=${data.proxy_strategy || "direct"}, 浏览器代理=${data.proxy || "直连"}, 节点=${data.proxy_node || "-"}`
    );
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `启动失败: ${err.message}`, true);
  } finally {
    DOM.startBtn.disabled = false;
  }
}

async function stopRegister() {
  try {
    DOM.stopBtn.disabled = true;
    await requestJSON("/api/stop", { method: "POST" });
    setMessage(DOM.actionMsg, "已发送停止补号指令");
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `停止失败: ${err.message}`, true);
  } finally {
    DOM.stopBtn.disabled = false;
  }
}

async function runMaintain() {
  try {
    DOM.maintainBtn.disabled = true;
    const data = await requestJSON("/api/maintain", { method: "POST" });
    setMessage(
      DOM.actionMsg,
      `维护任务已启动: 策略=${data.proxy_strategy || "direct"}, 浏览器代理=${data.proxy || "直连"}, 节点=${data.proxy_node || "-"}, limit=${data.limit || "all"}`
    );
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `维护启动失败: ${err.message}`, true);
  } finally {
    DOM.maintainBtn.disabled = false;
  }
}

async function stopMaintain() {
  try {
    DOM.stopMaintainBtn.disabled = true;
    await requestJSON("/api/maintain/stop", { method: "POST" });
    setMessage(DOM.actionMsg, "已发送停止维护指令");
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `停止维护失败: ${err.message}`, true);
  } finally {
    DOM.stopMaintainBtn.disabled = false;
  }
}

async function stopAllTasks() {
  try {
    DOM.stopAllBtn.disabled = true;
    const data = await requestJSON("/api/stop-all", { method: "POST" });
    setMessage(DOM.actionMsg, `已发送一键停止: register=${data.result?.register || "idle"}, maintain=${data.result?.maintain || "idle"}`);
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `一键停止失败: ${err.message}`, true);
  } finally {
    DOM.stopAllBtn.disabled = false;
  }
}

async function mergeAccounts() {
  try {
    DOM.mergeBtn.disabled = true;
    const data = await requestJSON("/api/pool/merge", { method: "POST" });
    setMessage(DOM.actionMsg, `all_account.json 已刷新: ${data.count} 条`);
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `刷新失败: ${err.message}`, true);
  } finally {
    DOM.mergeBtn.disabled = false;
  }
}

async function syncAccountsNow() {
  try {
    DOM.syncNowBtn.disabled = true;
    const data = await requestJSON("/api/sync/accounts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force: true, merge_before_sync: true, reason: "manual" }),
    });
    const result = data.result || {};
    setMessage(DOM.actionMsg, `同步成功: HTTP ${result.status_code || "?"}, count=${result.count || 0}, skipped=${result.skipped_count || 0}`);
    await loadStatus();
  } catch (err) {
    setMessage(DOM.actionMsg, `同步失败: ${err.message}`, true);
  } finally {
    DOM.syncNowBtn.disabled = false;
  }
}

async function testProxy() {
  const proxy = DOM.proxyInput.value.trim();
  if (!proxy) {
    setMessage(DOM.configMsg, "请先填写固定代理", true);
    return;
  }
  try {
    DOM.testProxyBtn.disabled = true;
    const data = await requestJSON("/api/check-proxy", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ proxy }),
    });
    if (data.ok) {
      setMessage(DOM.configMsg, `固定代理可用: loc=${data.loc || "?"} ip=${data.ip || "?"}`);
    } else {
      setMessage(DOM.configMsg, `固定代理不可用: ${data.error || "unknown"}`, true);
    }
  } catch (err) {
    setMessage(DOM.configMsg, `测试失败: ${err.message}`, true);
  } finally {
    DOM.testProxyBtn.disabled = false;
  }
}

async function testEasyProxies() {
  try {
    DOM.testEasyProxiesBtn.disabled = true;
    const data = await requestJSON("/api/easyproxies/test", { method: "POST" });
    if (!data.ok) {
      const stage = data.stage ? `(${data.stage}) ` : "";
      const hint = data.hint ? `；提示: ${data.hint}` : "";
      setMessage(DOM.configMsg, `EasyProxies 测试失败: ${stage}${data.error || "unknown"}${hint}`, true);
      return;
    }
    setMessage(
      DOM.configMsg,
      `EasyProxies 可用: listen=${data.listen_proxy || "?"}, loc=${data.loc || "?"}, ip=${data.ip || "?"}, nodes=${data.healthy_nodes || 0}/${data.total_nodes || 0}`
    );
  } catch (err) {
    setMessage(DOM.configMsg, `EasyProxies 测试失败: ${err.message}`, true);
  } finally {
    DOM.testEasyProxiesBtn.disabled = false;
  }
}

async function syncEasyProxiesSubscription() {
  try {
    DOM.syncEasyProxiesSubBtn.disabled = true;
    const data = await requestJSON("/api/easyproxies/sync-subscription", { method: "POST" });
    const result = data.result || {};
    if (result.ok) {
      setMessage(DOM.configMsg, "EasyProxies 订阅同步并刷新完成");
    } else {
      setMessage(DOM.configMsg, `EasyProxies 订阅同步未执行: ${result.reason || "unknown"}`, true);
    }
    await loadStatus();
  } catch (err) {
    setMessage(DOM.configMsg, `订阅同步失败: ${err.message}`, true);
  } finally {
    DOM.syncEasyProxiesSubBtn.disabled = false;
  }
}

async function monitorProxy() {
  try {
    DOM.monitorProxyBtn.disabled = true;
    const data = await requestJSON("/api/proxy/monitor");
    const current = data.current || {};
    const easy = data.easyproxies || {};
    const trace = data.trace || {};

    const traceMsg = trace.ok
      ? `trace(loc=${trace.loc || "?"}, ip=${trace.ip || "?"}, supported=${trace.supported === false ? "no" : "yes"})`
      : `trace失败(${trace.error || "unknown"})`;

    const easyMsg = easy.ok
      ? `easy(total=${easy.total_nodes || 0}, healthy=${easy.healthy_nodes || 0})`
      : `easy状态=${easy.ok === false ? "失败" : "未启用"}${easy.error ? `(${easy.error})` : ""}`;

    setMessage(
      DOM.configMsg,
      `监测: strategy=${current.strategy || "direct"} | browser=${current.browser_proxy || "direct"} | region=${current.region || "-"} | ${easyMsg} | ${traceMsg}`
    );
  } catch (err) {
    setMessage(DOM.configMsg, `代理监测失败: ${err.message}`, true);
  } finally {
    DOM.monitorProxyBtn.disabled = false;
  }
}

function connectSSE() {
  const es = new EventSource("/api/logs");
  es.onmessage = (e) => {
    try {
      const event = JSON.parse(e.data);
      appendLog(event);
    } catch {
      // ignore
    }
  };
  es.onerror = () => {
    appendLog({ ts: "--:--:--", level: "warn", message: "日志连接断开，3 秒后重连..." });
    es.close();
    setTimeout(connectSSE, 3000);
  };
}

function initEvents() {
  DOM.saveConfigBtn.addEventListener("click", saveConfig);
  DOM.testProxyBtn.addEventListener("click", testProxy);
  DOM.testEasyProxiesBtn.addEventListener("click", testEasyProxies);
  DOM.syncEasyProxiesSubBtn.addEventListener("click", syncEasyProxiesSubscription);
  DOM.monitorProxyBtn.addEventListener("click", monitorProxy);

  DOM.startBtn.addEventListener("click", startRegister);
  DOM.stopBtn.addEventListener("click", stopRegister);
  DOM.maintainBtn.addEventListener("click", runMaintain);
  DOM.stopMaintainBtn.addEventListener("click", stopMaintain);
  DOM.stopAllBtn.addEventListener("click", stopAllTasks);
  DOM.mergeBtn.addEventListener("click", mergeAccounts);
  DOM.syncNowBtn.addEventListener("click", syncAccountsNow);
}

async function bootstrap() {
  initEvents();
  connectSSE();
  await loadConfig();
  await loadStatus();
  setInterval(loadStatus, 5000);
}

bootstrap();
