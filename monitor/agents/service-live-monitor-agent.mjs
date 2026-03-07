import fs from "node:fs";
import net from "node:net";
import { basename, dirname, resolve } from "node:path";
import { readFile } from "node:fs/promises";

import { createLogger } from "../shared/logger.mjs";
import { createIpcEvent } from "../shared/ipc-protocol.mjs";

const defaultConfig = {
  pid_file: "./.server.pid",
  service_host: "",
  service_port: 0,
  host: "",
  port: 0,
  process_check_interval_ms: 5 * 1000,
  port_check_interval_ms: 30 * 1000,
  port_fail_threshold: 3,
  connect_timeout_ms: 2000,
  heartbeat_interval_ms: 60 * 1000
};

function deepMerge(base, override) {
  const result = { ...base };
  if (!override || typeof override !== "object") {
    return result;
  }
  for (const [key, value] of Object.entries(override)) {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      result[key] &&
      typeof result[key] === "object" &&
      !Array.isArray(result[key])
    ) {
      result[key] = deepMerge(result[key], value);
      continue;
    }
    result[key] = value;
  }
  return result;
}

function loadConfig() {
  try {
    const fromEnv = process.env.MONITOR_CONFIG ? JSON.parse(process.env.MONITOR_CONFIG) : {};
    const merged = deepMerge(defaultConfig, fromEnv);
    const hostFromIpc = typeof merged.service_host === "string" && merged.service_host.trim()
      ? merged.service_host.trim()
      : (typeof merged.host === "string" ? merged.host.trim() : "");
    const portFromIpc = toInt(merged.service_port) || toInt(merged.port);

    merged.host = hostFromIpc || process.env.MONITOR_SERVICE_HOST || "";
    merged.port = portFromIpc || toInt(process.env.MONITOR_SERVICE_PORT) || 0;
    merged.pid_file = resolve(process.cwd(), merged.pid_file);
    return merged;
  } catch (error) {
    const fallback = deepMerge(defaultConfig, {});
    fallback.host = process.env.MONITOR_SERVICE_HOST || "";
    fallback.port = toInt(process.env.MONITOR_SERVICE_PORT) || 0;
    fallback.pid_file = resolve(process.cwd(), fallback.pid_file);
    return fallback;
  }
}

function toInt(text) {
  const value = Number.parseInt(String(text).trim(), 10);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function isProcessAlive(pid) {
  if (!pid) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "EPERM") {
      return true;
    }
    return false;
  }
}

async function tcpConnect(host, port, timeoutMs) {
  return new Promise((resolvePromise) => {
    const socket = new net.Socket();
    let settled = false;

    function done(ok) {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolvePromise(ok);
    }

    socket.setTimeout(timeoutMs, () => done(false));
    socket.once("connect", () => done(true));
    socket.once("error", () => done(false));
    socket.connect(port, host);
  });
}

const config = loadConfig();
const logger = createLogger("service-live-monitor-agent", { level: "info" });

let watcher = null;
let stopped = false;
let currentPid = null;
let processAlive = false;
let portReachable = false;

let processDownAlertActive = false;
let portFailureCount = 0;
let portDownAlertActive = false;

function sendEvent(event) {
  if (typeof process.send === "function") {
    process.send(event);
  }
}

function sendAlertCandidate(payload) {
  sendEvent(
    createIpcEvent({
      source: "service-live-monitor",
      type: "alert_candidate",
      ...payload
    })
  );
}

function sendAlertResolved(payload) {
  sendEvent(
    createIpcEvent({
      source: "service-live-monitor",
      type: "alert_resolved",
      level: "info",
      ...payload
    })
  );
}

function sendHeartbeat(extra = {}) {
  const memoryRssMb = Math.round((process.memoryUsage().rss / 1024 / 1024) * 100) / 100;
  sendEvent(
    createIpcEvent({
      source: "service-live-monitor",
      type: "heartbeat",
      level: "info",
      title: "服务存活心跳",
      message: "服务存活监控运行中",
      data: {
        rss_mb: memoryRssMb,
        pid: currentPid,
        process_alive: processAlive,
        port_reachable: portReachable,
        port_failure_count: portFailureCount,
        ...extra
      }
    })
  );
}

function buildStatusSummary() {
  return {
    status: "ok",
    service_state: processAlive && portReachable ? "running" : "stopped",
    pid: currentPid,
    process_alive: processAlive,
    port_reachable: portReachable,
    host: config.host,
    port: Number(config.port),
    port_failure_count: portFailureCount,
    process_down_alert_active: processDownAlertActive,
    port_down_alert_active: portDownAlertActive
  };
}

function sendStatusResponse(requestId) {
  if (typeof requestId !== "string" || !requestId) {
    return;
  }
  sendEvent({
    type: "status_response",
    source: "service-live-monitor",
    request_id: requestId,
    ts: Date.now(),
    data: buildStatusSummary()
  });
}

async function readPid() {
  try {
    const text = await readFile(config.pid_file, "utf8");
    return toInt(text);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      logger.warn("读取进程号文件失败", { error: error.message, file: config.pid_file });
    }
    return null;
  }
}

async function checkProcess() {
  currentPid = await readPid();
  processAlive = isProcessAlive(currentPid);

  if (!processAlive) {
    if (!processDownAlertActive) {
      sendAlertCandidate({
        level: "critical",
        rule_id: "service_process_down",
        resource_id: "codex2api",
        title: "主服务进程不可用",
        message: currentPid ? `进程号 ${currentPid} 不存在` : "未找到有效进程号",
        data: {
          pid: currentPid,
          pid_file: config.pid_file
        }
      });
      processDownAlertActive = true;
    }
    return;
  }

  if (processDownAlertActive) {
    sendAlertResolved({
      rule_id: "service_process_down",
      resource_id: "codex2api",
      title: "主服务进程恢复",
      message: `进程号 ${currentPid} 存活`,
      data: {
        pid: currentPid
      }
    });
    processDownAlertActive = false;
  }
}

async function checkPort() {
  const ok = await tcpConnect(config.host, Number(config.port), Number(config.connect_timeout_ms));
  if (ok) {
    portReachable = true;
    if (portDownAlertActive) {
      sendAlertResolved({
        rule_id: "service_port_unreachable",
        resource_id: `${config.host}:${config.port}`,
        title: "服务端口恢复",
        message: `${config.host}:${config.port} 连接恢复`,
        data: {
          host: config.host,
          port: Number(config.port)
        }
      });
      portDownAlertActive = false;
    }
    portFailureCount = 0;
    return;
  }

  portReachable = false;
  portFailureCount += 1;
  if (portFailureCount >= Number(config.port_fail_threshold) && !portDownAlertActive) {
    sendAlertCandidate({
      level: "critical",
      rule_id: "service_port_unreachable",
      resource_id: `${config.host}:${config.port}`,
      title: "服务端口不可达",
      message: `${config.host}:${config.port} 连续 ${portFailureCount} 次连接失败`,
      data: {
        host: config.host,
        port: Number(config.port),
        consecutive_failures: portFailureCount,
        fail_threshold: Number(config.port_fail_threshold)
      }
    });
    portDownAlertActive = true;
  }
}

async function startPidWatcher() {
  const watchDir = dirname(config.pid_file);
  const watchName = basename(config.pid_file);
  watcher = fs.watch(watchDir, { persistent: false }, (eventType, filename) => {
    if (!filename || (eventType !== "change" && eventType !== "rename")) {
      return;
    }
    if (String(filename) !== watchName) {
      return;
    }
    checkProcess().catch((error) => {
      logger.warn("进程号监听触发进程检查失败", { error: error.message });
    });
  });
  watcher.on("error", (error) => {
    logger.error("进程号文件监听异常", { error: error.message });
  });
}

function shutdown(reason, timers) {
  if (stopped) {
    return;
  }
  stopped = true;
  for (const timer of timers) {
    clearInterval(timer);
  }
  if (watcher) {
    watcher.close();
  }
  logger.info("服务存活监控退出", { reason });
  process.exit(0);
}

async function main() {
  logger.info("服务存活监控已启动", {
    pid_file: config.pid_file,
    host: config.host,
    port: Number(config.port)
  });

  await startPidWatcher();
  await checkProcess();
  await checkPort();
  sendHeartbeat({ startup: true });

  const processTimer = setInterval(() => {
    checkProcess().catch((error) => {
      logger.warn("周期进程检查失败", { error: error.message });
    });
  }, Number(config.process_check_interval_ms));
  processTimer.unref();

  const portTimer = setInterval(() => {
    checkPort().catch((error) => {
      logger.warn("周期端口检查失败", { error: error.message });
    });
  }, Number(config.port_check_interval_ms));
  portTimer.unref();

  const heartbeatTimer = setInterval(() => {
    sendHeartbeat();
  }, Number(config.heartbeat_interval_ms));
  heartbeatTimer.unref();

  const timers = [processTimer, portTimer, heartbeatTimer];

  process.on("message", (message) => {
    if (message?.type === "status_request") {
      sendStatusResponse(message.request_id);
      return;
    }
    if (message?.type === "shutdown") {
      shutdown("shutdown_message", timers);
    }
  });
  process.on("SIGTERM", () => shutdown("sigterm", timers));
  process.on("SIGINT", () => shutdown("sigint", timers));
}

main().catch((error) => {
  logger.error("服务存活监控致命错误", { error: error.message });
  process.exit(1);
});
