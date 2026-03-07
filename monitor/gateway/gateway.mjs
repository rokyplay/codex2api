import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdir, readFile } from "node:fs/promises";

import { createLogger } from "../shared/logger.mjs";
import { createIpcEvent } from "../shared/ipc-protocol.mjs";
import { StateStore } from "./state-store.mjs";
import { GotifyNotifier, NotifierHub } from "./notifier-hub.mjs";
import { AlertAggregator } from "./alert-aggregator.mjs";
import { EventRouter } from "./event-router.mjs";
import { ChildSupervisor } from "./child-supervisor.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "../..");

const defaultConfig = {
  enabled: true,
  log_level: "info",
  heartbeat_interval_ms: 5 * 60 * 1000,
  heartbeat_stale_ms: 10 * 60 * 1000,
  state_flush_interval_ms: 30 * 1000,
  state_file: "./tmp/monitor/state/gateway-state.json",
  alerts_file_dir: "./tmp/monitor/alerts",
  dedupe: {
    dedupe_ttl_ms: 15 * 60 * 1000,
    notify_cooldown_ms: 5 * 60 * 1000
  },
  status_report: {
    enabled: true,
    interval_minutes: 60,
    channels: ["gotify"],
    include_metrics: true
  },
  channels: {
    stdout: { enabled: true, min_level: "info" },
    file: { enabled: true, min_level: "info" },
    discord: { enabled: false, min_level: "warn", webhook_url: "" },
    gotify: {
      enabled: true,
      url: process.env.MONITOR_GOTIFY_URL || "https://gotify.example.com",
      token: process.env.MONITOR_GOTIFY_TOKEN || "",
      min_level: "warn",
      rate_limit_per_min: 10
    }
  },
  supervisor: {
    restart_window_ms: 10 * 60 * 1000,
    restart_critical_threshold: 5
  },
  agents: {
    request_monitor: {
      enabled: true,
      max_memory_mb: 48,
      stats_dir: "./data/stats",
      window_ms: 5 * 60 * 1000,
      error_rate_threshold: 0.1,
      error_rate_sustain_ms: 5 * 60 * 1000,
      consecutive_401_threshold: 10
    },
    service_live_monitor: {
      enabled: true,
      max_memory_mb: 48,
      pid_file: "./.server.pid",
      service_port: parseInt(process.env.MONITOR_SERVICE_PORT || "8066", 10),
      service_host: process.env.MONITOR_SERVICE_HOST || "127.0.0.1",
      port_check_interval_ms: 30 * 1000,
      port_fail_threshold: 3,
      process_check_interval_ms: 5 * 1000,
      connect_timeout_ms: 2000
    }
  }
};

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function deepMerge(base, override) {
  if (!isPlainObject(base)) {
    return override;
  }
  const result = { ...base };
  if (!isPlainObject(override)) {
    return result;
  }

  for (const [key, value] of Object.entries(override)) {
    if (isPlainObject(value) && isPlainObject(result[key])) {
      result[key] = deepMerge(result[key], value);
      continue;
    }
    result[key] = value;
  }
  return result;
}

function resolveProjectPath(value) {
  if (typeof value !== "string" || !value) {
    return value;
  }
  if (isAbsolute(value)) {
    return value;
  }
  return resolve(PROJECT_ROOT, value);
}

async function loadGatewayConfig(logger) {
  const configPath = join(PROJECT_ROOT, "config-server.json");
  try {
    const text = await readFile(configPath, "utf8");
    const parsed = JSON.parse(text);
    const section = parsed?.monitor_gateway ?? {};
    const merged = deepMerge(defaultConfig, section);
    merged.state_file = resolveProjectPath(merged.state_file);
    merged.alerts_file_dir = resolveProjectPath(merged.alerts_file_dir);
    merged.agents.request_monitor.stats_dir = resolveProjectPath(merged.agents.request_monitor.stats_dir);
    merged.agents.service_live_monitor.pid_file = resolveProjectPath(merged.agents.service_live_monitor.pid_file);
    return merged;
  } catch (error) {
    logger.warn("解析监控配置文件失败，使用默认监控配置", {
      error: error.message
    });
    const merged = deepMerge(defaultConfig, {});
    merged.state_file = resolveProjectPath(merged.state_file);
    merged.alerts_file_dir = resolveProjectPath(merged.alerts_file_dir);
    merged.agents.request_monitor.stats_dir = resolveProjectPath(merged.agents.request_monitor.stats_dir);
    merged.agents.service_live_monitor.pid_file = resolveProjectPath(merged.agents.service_live_monitor.pid_file);
    return merged;
  }
}

async function ensureGatewayDirs(config) {
  await mkdir(dirname(config.state_file), { recursive: true });
  await mkdir(config.alerts_file_dir, { recursive: true });
  await mkdir(resolveProjectPath("./tmp/monitor/logs"), { recursive: true });
}

function buildAgentSpecs(config) {
  const specs = [];

  const requestCfg = config.agents.request_monitor;
  if (requestCfg.enabled !== false) {
    specs.push({
      name: "request-monitor",
      enabled: true,
      modulePath: join(PROJECT_ROOT, "monitor/agents/request-monitor-agent.mjs"),
      max_memory_mb: requestCfg.max_memory_mb ?? 48,
      config: requestCfg
    });
  }

  const serviceCfg = config.agents.service_live_monitor;
  if (serviceCfg.enabled !== false) {
    specs.push({
      name: "service-live-monitor",
      enabled: true,
      modulePath: join(PROJECT_ROOT, "monitor/agents/service-live-monitor-agent.mjs"),
      max_memory_mb: serviceCfg.max_memory_mb ?? 48,
      config: serviceCfg
    });
  }

  return specs;
}

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function formatPercent(value) {
  return (toNumber(value, 0) * 100).toFixed(2);
}

function formatLatency(value) {
  return Math.round(toNumber(value, 0));
}

function buildAlertListText(activeAlerts) {
  if (!Array.isArray(activeAlerts) || activeAlerts.length === 0) {
    return "- 无";
  }
  return activeAlerts
    .map((item) => {
      const level = String(item.last_level ?? "warn").toUpperCase();
      const title = String(item.last_title ?? item.rule_id ?? "未命名告警");
      const resource = String(item.resource_id ?? "global");
      return `- [${level}] ${title}（${resource}）`;
    })
    .join("\n");
}

function buildStatusReportMessage({
  timestamp,
  serviceState,
  includeMetrics,
  requestStats,
  activeAccounts,
  totalAccounts,
  error401Count,
  activeAlertsCount,
  alertListText
}) {
  const lines = [];
  lines.push(`⏰ 时间：${timestamp}`);
  lines.push(`🟢 服务状态：${serviceState}`);
  lines.push("");
  lines.push("📈 最近1小时统计：");
  if (includeMetrics) {
    lines.push(`- 总请求数：${toNumber(requestStats.total_requests, 0)}`);
    lines.push(`- 成功率：${formatPercent(requestStats.success_rate)}%`);
    lines.push(`- 平均延迟：${formatLatency(requestStats.latency_avg_ms)}ms`);
    lines.push(`- P95延迟：${formatLatency(requestStats.latency_p95_ms)}ms`);
    lines.push(`- 缓存命中率：${formatPercent(requestStats.cache_hit_rate)}%`);
  } else {
    lines.push("- 已关闭详细指标展示");
  }
  lines.push("");
  lines.push("🏊 账号池状态：");
  lines.push(`- 活跃账号：${toNumber(activeAccounts, 0)}/${toNumber(totalAccounts, 0)}`);
  lines.push(`- 最近1小时 401 错误：${toNumber(error401Count, 0)}`);
  lines.push("");
  lines.push("🔍 告警摘要：");
  lines.push(`- 未恢复告警数：${toNumber(activeAlertsCount, 0)}`);
  lines.push(alertListText);
  return lines.join("\n");
}

async function runGateway() {
  const bootLogger = createLogger("monitor-gateway/bootstrap", { level: "info" });
  const config = await loadGatewayConfig(bootLogger);
  const logger = createLogger("monitor-gateway", { level: config.log_level ?? "info" });

  if (config.enabled === false) {
    logger.info("监控网关已按配置禁用");
    return;
  }

  await ensureGatewayDirs(config);

  const stateStore = new StateStore(config.state_file, logger.child("state-store"));
  const persisted = await stateStore.load({
    aggregator: {},
    supervisor: {}
  });

  const notifier = new NotifierHub(
    {
      alerts_file_dir: config.alerts_file_dir,
      channels: config.channels
    },
    logger.child("notifier-hub")
  );
  notifier.registerChannel("gotify", new GotifyNotifier(config.channels?.gotify ?? {}, logger.child("notifier-hub/gotify")));
  await notifier.init();

  const aggregator = new AlertAggregator(
    {
      dedupe_ttl_ms: config.dedupe.dedupe_ttl_ms,
      notify_cooldown_ms: config.dedupe.notify_cooldown_ms,
      state: persisted.aggregator
    },
    logger.child("alert-aggregator")
  );

  const router = new EventRouter({
    logger: logger.child("event-router"),
    aggregator,
    notifier
  });

  const supervisor = new ChildSupervisor({
    logger: logger.child("child-supervisor"),
    projectRoot: PROJECT_ROOT,
    agents: buildAgentSpecs(config),
    restart_window_ms: config.supervisor.restart_window_ms,
    restart_critical_threshold: config.supervisor.restart_critical_threshold,
    heartbeat_timeout_ms: config.heartbeat_stale_ms,
    initialState: persisted.supervisor,
    onEvent: async ({ agentName, event }) => {
      await router.route(event, { agentName });
    }
  });

  await supervisor.startAll();

  const statusReportConfig = {
    enabled: config.status_report?.enabled !== false,
    interval_minutes: Math.max(1, Math.floor(toNumber(config.status_report?.interval_minutes, 60))),
    channels:
      Array.isArray(config.status_report?.channels) && config.status_report.channels.length > 0
        ? config.status_report.channels.map((item) => String(item).trim()).filter(Boolean)
        : ["gotify"],
    include_metrics: config.status_report?.include_metrics !== false
  };

  async function sendGatewayStartupNotice() {
    const ts = new Date().toISOString();
    const message = [
      `⏰ 时间：${ts}`,
      "✅ 监控网关已启动",
      `🤖 子代理：${buildAgentSpecs(config).map((item) => item.name).join("、")}`,
      `📡 主动状态推送：每 ${statusReportConfig.interval_minutes} 分钟`
    ].join("\n");

    await notifier.sendReport({
      source: "monitor-gateway",
      title: "监控网关已启动",
      message,
      data: {
        started_at: ts,
        status_report: statusReportConfig
      },
      channels: statusReportConfig.channels
    });
  }

  async function sendStatusReport(trigger) {
    const statusResponses = await supervisor.collectStatuses(5000);
    const statusByAgent = new Map(statusResponses.map((item) => [item.agent, item]));
    const requestStatus = statusByAgent.get("request-monitor");
    const serviceStatus = statusByAgent.get("service-live-monitor");
    const requestStats = requestStatus?.ok ? requestStatus.data ?? {} : {};

    const snapshots = supervisor.inspectChildren();
    const serviceSnapshot = snapshots.find((item) => item.name === "service-live-monitor");
    const serviceStateRaw = serviceStatus?.ok ? String(serviceStatus.data?.service_state ?? "") : "";
    const serviceState = serviceStateRaw === "running" || serviceStateRaw === "stopped" ? serviceStateRaw : serviceSnapshot?.running ? "running" : "stopped";

    const activeAlerts = aggregator.listActiveAlerts(20);
    const activeAlertsCount = aggregator.countActiveAlerts();
    const alertListText = buildAlertListText(activeAlerts);

    const activeAccounts = toNumber(requestStats.active_accounts, 0);
    const totalAccounts = toNumber(requestStats.total_accounts, 0);
    const error401Count = toNumber(requestStats.error_401_count, 0);
    const timestamp = new Date().toISOString();
    const message = buildStatusReportMessage({
      timestamp,
      serviceState,
      includeMetrics: statusReportConfig.include_metrics,
      requestStats,
      activeAccounts,
      totalAccounts,
      error401Count,
      activeAlertsCount,
      alertListText
    });

    const reportResult = await notifier.sendReport({
      source: "monitor-gateway",
      title: "📊 codex2api 状态报告",
      message,
      data: {
        trigger,
        timestamp,
        service_state: serviceState,
        include_metrics: statusReportConfig.include_metrics,
        request_monitor: requestStatus ?? null,
        service_live_monitor: serviceStatus ?? null,
        request_stats: requestStats,
        account_pool: {
          active: activeAccounts,
          total: totalAccounts,
          error_401_count: error401Count
        },
        active_alerts: activeAlerts,
        active_alert_count: activeAlertsCount
      },
      channels: statusReportConfig.channels
    });

    logger.info("状态报告已推送", {
      trigger,
      channels: statusReportConfig.channels,
      notify_result: reportResult.meta
    });
  }

  const heartbeatTimer = setInterval(async () => {
    const snapshots = supervisor.inspectChildren();
    logger.info("心跳巡检结果", { agents: snapshots });
    for (const item of snapshots) {
      const event = createIpcEvent({
        source: "child-supervisor",
        type: item.heartbeat_stale ? "alert_candidate" : "alert_resolved",
        level: item.heartbeat_stale ? "warn" : "info",
        rule_id: "agent_heartbeat_stale",
        resource_id: item.name,
        title: item.heartbeat_stale ? "子代理心跳超时" : "子代理心跳恢复",
        message: item.heartbeat_stale
          ? `${item.name} 心跳超过阈值 ${config.heartbeat_stale_ms}ms`
          : `${item.name} 心跳已恢复`,
        data: item
      });
      await router.route(event, { agentName: "child-supervisor" });
    }
  }, config.heartbeat_interval_ms);
  heartbeatTimer.unref();

  const stateFlushTimer = setInterval(async () => {
    try {
      stateStore.setSection("aggregator", aggregator.toSerializableState());
      stateStore.setSection("supervisor", supervisor.getSerializableState());
      await stateStore.saveNow();
    } catch (error) {
      logger.error("周期状态落盘失败", { error: error.message });
    }
  }, config.state_flush_interval_ms);
  stateFlushTimer.unref();

  let statusReportTimer = null;
  if (statusReportConfig.enabled) {
    const intervalMs = statusReportConfig.interval_minutes * 60 * 1000;
    statusReportTimer = setInterval(() => {
      sendStatusReport("interval").catch((error) => {
        logger.error("定时状态报告推送失败", { error: error.message });
      });
    }, intervalMs);
    statusReportTimer.unref();
  }

  let shuttingDown = false;
  async function shutdown(signal, exitCode = 0) {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    logger.warn("网关正在关闭", { signal });
    clearInterval(heartbeatTimer);
    clearInterval(stateFlushTimer);
    if (statusReportTimer) {
      clearInterval(statusReportTimer);
    }

    try {
      await supervisor.stopAll();
      stateStore.setSection("aggregator", aggregator.toSerializableState());
      stateStore.setSection("supervisor", supervisor.getSerializableState());
      await stateStore.saveNow();
    } catch (error) {
      logger.error("网关关闭失败", { error: error.message });
      process.exitCode = 1;
    }
    process.exit(exitCode);
  }

  process.on("SIGTERM", () => {
    shutdown("SIGTERM", 0);
  });
  process.on("SIGINT", () => {
    shutdown("SIGINT", 0);
  });
  process.on("uncaughtException", (error) => {
    logger.critical("捕获到未处理异常", error);
    shutdown("uncaughtException", 1);
  });
  process.on("unhandledRejection", (reason) => {
    logger.critical("捕获到未处理的异步拒绝", { reason: String(reason) });
    shutdown("unhandledRejection", 1);
  });

  logger.info("监控网关已启动", {
    project_root: PROJECT_ROOT,
    agents: buildAgentSpecs(config).map((item) => item.name),
    heartbeat_interval_ms: config.heartbeat_interval_ms
  });

  sendGatewayStartupNotice().catch((error) => {
    logger.error("启动通知推送失败", { error: error.message });
  });
}

runGateway().catch((error) => {
  const logger = createLogger("monitor-gateway/fatal", { level: "info" });
  logger.critical("监控网关启动失败", error);
  process.exit(1);
});
