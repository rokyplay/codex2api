import fs from "node:fs";
import { join, resolve } from "node:path";
import { open, readdir, stat } from "node:fs/promises";

import { RingWindow } from "../shared/ring-window.mjs";
import { createLogger } from "../shared/logger.mjs";
import { createIpcEvent } from "../shared/ipc-protocol.mjs";

const defaultConfig = {
  stats_dir: "./data/stats",
  file_pattern: "^requests-\\d{4}-\\d{2}-\\d{2}\\.jsonl$",
  window_ms: 5 * 60 * 1000,
  status_window_ms: 60 * 60 * 1000,
  status_window_capacity: 60000,
  error_rate_threshold: 0.1,
  error_rate_sustain_ms: 5 * 60 * 1000,
  consecutive_401_threshold: 10,
  metric_interval_ms: 30 * 1000,
  evaluation_interval_ms: 10 * 1000,
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
    merged.stats_dir = resolve(process.cwd(), merged.stats_dir);
    merged.file_regex = new RegExp(merged.file_pattern);
    return merged;
  } catch (error) {
    const fallback = deepMerge(defaultConfig, {});
    fallback.stats_dir = resolve(process.cwd(), fallback.stats_dir);
    fallback.file_regex = new RegExp(fallback.file_pattern);
    return fallback;
  }
}

function percentile(values, p) {
  if (!values.length) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  const safeIndex = Math.min(Math.max(index, 0), sorted.length - 1);
  return sorted[safeIndex];
}

const config = loadConfig();
const logger = createLogger("request-monitor-agent", { level: "info" });

const windowBuffer = new RingWindow({
  windowMs: Number(config.window_ms),
  capacity: 12000,
  getTimestamp: (item) => item.ts
});

const statusWindowBuffer = new RingWindow({
  windowMs: Number(config.status_window_ms),
  capacity: Number(config.status_window_capacity),
  getTimestamp: (item) => item.ts
});

const fileOffsets = new Map();
const lineRemainders = new Map();
const processingFiles = new Set();

let watcher = null;
let stopped = false;

let consecutive401 = 0;
let consecutive401AlertActive = false;

let errorRateBreachSince = 0;
let errorRateAlertActive = false;

function sendEvent(event) {
  if (typeof process.send !== "function") {
    return;
  }
  process.send(event);
}

function sendAlertCandidate(payload) {
  sendEvent(
    createIpcEvent({
      source: "request-monitor",
      type: "alert_candidate",
      ...payload
    })
  );
}

function sendAlertResolved(payload) {
  sendEvent(
    createIpcEvent({
      source: "request-monitor",
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
      source: "request-monitor",
      type: "heartbeat",
      level: "info",
      title: "请求监控心跳",
      message: "请求监控子代理运行中",
      data: {
        rss_mb: memoryRssMb,
        consecutive_401: consecutive401,
        ...extra
      }
    })
  );
}

function computeStats(buffer, windowMs, now = Date.now()) {
  const values = buffer.values(now);
  const total = values.length;
  let successes = 0;
  let errors = 0;
  let error401Count = 0;
  let cacheHitRequests = 0;
  const accounts = new Set();
  const activeAccounts = new Set();
  const latencies = [];
  let latencySum = 0;
  for (const item of values) {
    if (item.isError) {
      errors += 1;
    } else {
      successes += 1;
    }
    if (item.is401) {
      error401Count += 1;
    }
    if (item.cacheHit) {
      cacheHitRequests += 1;
    }
    if (typeof item.account === "string" && item.account) {
      accounts.add(item.account);
      if (!item.isError) {
        activeAccounts.add(item.account);
      }
    }
    if (Number.isFinite(item.latency) && item.latency >= 0) {
      latencies.push(item.latency);
      latencySum += item.latency;
    }
  }
  const errorRate = total > 0 ? errors / total : 0;
  const successRate = total > 0 ? successes / total : 0;
  const avgLatency = latencies.length > 0 ? latencySum / latencies.length : 0;
  return {
    window_ms: Number(windowMs),
    total_requests: total,
    success_requests: successes,
    error_requests: errors,
    error_rate: errorRate,
    success_rate: successRate,
    latency_avg_ms: avgLatency,
    latency_p95_ms: percentile(latencies, 95),
    latency_p99_ms: percentile(latencies, 99),
    consecutive_401: consecutive401,
    error_401_count: error401Count,
    cache_hit_requests: cacheHitRequests,
    cache_hit_rate: total > 0 ? cacheHitRequests / total : 0,
    active_accounts: activeAccounts.size,
    total_accounts: accounts.size
  };
}

function computeWindowStats(now = Date.now()) {
  return computeStats(windowBuffer, Number(config.window_ms), now);
}

function computeStatusWindowStats(now = Date.now()) {
  return computeStats(statusWindowBuffer, Number(config.status_window_ms), now);
}

function sendStatusResponse(requestId) {
  if (typeof requestId !== "string" || !requestId) {
    return;
  }
  const summary = computeStatusWindowStats(Date.now());
  sendEvent({
    type: "status_response",
    source: "request-monitor",
    request_id: requestId,
    ts: Date.now(),
    data: {
      ...summary,
      status: "ok"
    }
  });
}

function evaluateErrorRate(now = Date.now()) {
  const stats = computeWindowStats(now);
  if (stats.total_requests === 0) {
    if (errorRateAlertActive) {
      sendAlertResolved({
        rule_id: "error_rate_5m_gt_10pct",
        resource_id: "global",
        title: "错误率恢复",
        message: "5分钟窗口无流量，错误率告警结束",
        data: stats
      });
      errorRateAlertActive = false;
    }
    errorRateBreachSince = 0;
    return stats;
  }

  if (stats.error_rate > Number(config.error_rate_threshold)) {
    if (errorRateBreachSince === 0) {
      errorRateBreachSince = now;
    }
    if (now - errorRateBreachSince >= Number(config.error_rate_sustain_ms) && !errorRateAlertActive) {
      sendAlertCandidate({
        level: "warn",
        rule_id: "error_rate_5m_gt_10pct",
        resource_id: "global",
        title: "错误率超过阈值",
        message: `5分钟错误率 ${(stats.error_rate * 100).toFixed(2)}%，持续达到阈值`,
        data: {
          ...stats,
          threshold: Number(config.error_rate_threshold),
          sustain_ms: Number(config.error_rate_sustain_ms)
        }
      });
      errorRateAlertActive = true;
    }
    return stats;
  }

  errorRateBreachSince = 0;
  if (errorRateAlertActive) {
    sendAlertResolved({
      rule_id: "error_rate_5m_gt_10pct",
      resource_id: "global",
      title: "错误率恢复",
      message: `5分钟错误率回落到 ${(stats.error_rate * 100).toFixed(2)}%`,
      data: stats
    });
    errorRateAlertActive = false;
  }
  return stats;
}

function handleEntry(entry) {
  const tsRaw = Number(entry.ts ?? entry.timestamp ?? Date.parse(entry.time ?? entry.created_at ?? ""));
  const ts = Number.isFinite(tsRaw) ? tsRaw : Date.now();
  const status = Number(entry.status ?? entry.status_code ?? 0);
  const latency = Number(entry.latency ?? entry.latency_ms ?? entry.ttfb_ms ?? 0);
  const account = typeof entry.account === "string" ? entry.account.trim() : "";
  const cachedTokens = Number(entry.cached_tokens ?? 0);
  const cacheHit = entry.cache_hit === true || entry.from_cache === true || entry.cached === true || (Number.isFinite(cachedTokens) && cachedTokens > 0);
  const isError = Number.isFinite(status) && status >= 400;
  const is401 = status === 401;

  const normalized = {
    ts,
    status,
    latency,
    isError,
    is401,
    cacheHit,
    account
  };

  windowBuffer.push(normalized);
  windowBuffer.prune(ts);
  statusWindowBuffer.push(normalized);
  statusWindowBuffer.prune(ts);

  if (is401) {
    consecutive401 += 1;
    if (!consecutive401AlertActive && consecutive401 >= Number(config.consecutive_401_threshold)) {
      sendAlertCandidate({
        level: "warn",
        rule_id: "consecutive_401_ge_10",
        resource_id: "global",
        title: "连续 401 命中阈值",
        message: `连续 401 数达到 ${consecutive401}`,
        data: {
          consecutive_401: consecutive401,
          threshold: Number(config.consecutive_401_threshold)
        }
      });
      consecutive401AlertActive = true;
    }
  } else {
    if (consecutive401AlertActive) {
      sendAlertResolved({
        rule_id: "consecutive_401_ge_10",
        resource_id: "global",
        title: "连续 401 恢复",
        message: `连续 401 计数已重置，上一计数 ${consecutive401}`,
        data: {
          previous_consecutive_401: consecutive401
        }
      });
      consecutive401AlertActive = false;
    }
    consecutive401 = 0;
  }

  evaluateErrorRate(ts);
}

function handleLine(line, filePath) {
  const text = line.trim();
  if (!text) {
    return;
  }
  try {
    const entry = JSON.parse(text);
    handleEntry(entry);
  } catch (error) {
    logger.warn("统计记录行解析失败", {
      file: filePath,
      error: error.message
    });
  }
}

async function readIncrementalFile(filePath) {
  const fileStat = await stat(filePath);
  let offset = fileOffsets.get(filePath) ?? 0;
  if (fileStat.size < offset) {
    offset = 0;
    lineRemainders.set(filePath, "");
  }
  if (fileStat.size === offset) {
    return;
  }

  const size = fileStat.size - offset;
  const handle = await open(filePath, "r");
  try {
    const buffer = Buffer.alloc(size);
    await handle.read(buffer, 0, size, offset);
    fileOffsets.set(filePath, fileStat.size);

    const chunk = buffer.toString("utf8");
    const text = `${lineRemainders.get(filePath) ?? ""}${chunk}`;
    const lines = text.split("\n");
    const remainder = lines.pop() ?? "";
    lineRemainders.set(filePath, remainder);
    for (const line of lines) {
      handleLine(line, filePath);
    }
  } finally {
    await handle.close();
  }
}

async function processFile(filename) {
  if (stopped) {
    return;
  }
  if (!config.file_regex.test(filename)) {
    return;
  }
  const filePath = join(config.stats_dir, filename);
  if (processingFiles.has(filePath)) {
    return;
  }
  processingFiles.add(filePath);
  try {
    if (!fileOffsets.has(filePath)) {
      const currentStat = await stat(filePath);
      fileOffsets.set(filePath, currentStat.size);
      lineRemainders.set(filePath, "");
      return;
    }
    await readIncrementalFile(filePath);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      logger.warn("处理统计文件失败", { file: filePath, error: error.message });
    }
  } finally {
    processingFiles.delete(filePath);
  }
}

async function bootstrapOffsets() {
  let files = [];
  try {
    files = await readdir(config.stats_dir);
  } catch (error) {
    logger.error("读取统计目录失败", { dir: config.stats_dir, error: error.message });
    return;
  }
  for (const filename of files) {
    if (!config.file_regex.test(filename)) {
      continue;
    }
    const filePath = join(config.stats_dir, filename);
    try {
      const fileStat = await stat(filePath);
      fileOffsets.set(filePath, fileStat.size);
      lineRemainders.set(filePath, "");
    } catch (error) {
      if (error?.code !== "ENOENT") {
        logger.warn("初始化文件状态失败", { file: filePath, error: error.message });
      }
    }
  }
}

async function startWatcher() {
  await bootstrapOffsets();
  watcher = fs.watch(config.stats_dir, { persistent: false }, (eventType, filename) => {
    if (!filename || (eventType !== "change" && eventType !== "rename")) {
      return;
    }
    processFile(String(filename)).catch((error) => {
      logger.warn("文件监听回调执行失败", { error: error.message });
    });
  });
  watcher.on("error", (error) => {
    logger.error("统计目录监听异常", { error: error.message });
  });
}

function emitMetric() {
  const stats = evaluateErrorRate(Date.now());
  sendEvent(
    createIpcEvent({
      source: "request-monitor",
      type: "metric",
      level: "info",
      rule_id: "request_window_stats",
      resource_id: "global",
      title: "请求监控窗口指标",
      message: "5分钟滑动窗口指标",
      data: stats
    })
  );
}

function shutdown(reason) {
  if (stopped) {
    return;
  }
  stopped = true;
  if (watcher) {
    watcher.close();
  }
  logger.info("请求监控子代理退出", { reason });
  process.exit(0);
}

async function main() {
  logger.info("请求监控子代理已启动", {
    stats_dir: config.stats_dir,
    window_ms: config.window_ms
  });
  await startWatcher();
  sendHeartbeat({ startup: true });

  const metricTimer = setInterval(() => {
    emitMetric();
  }, Number(config.metric_interval_ms));
  metricTimer.unref();

  const evaluateTimer = setInterval(() => {
    evaluateErrorRate(Date.now());
  }, Number(config.evaluation_interval_ms));
  evaluateTimer.unref();

  const heartbeatTimer = setInterval(() => {
    const stats = computeWindowStats(Date.now());
    sendHeartbeat(stats);
  }, Number(config.heartbeat_interval_ms));
  heartbeatTimer.unref();

  process.on("message", (message) => {
    if (message?.type === "status_request") {
      sendStatusResponse(message.request_id);
      return;
    }
    if (message?.type === "shutdown") {
      clearInterval(metricTimer);
      clearInterval(evaluateTimer);
      clearInterval(heartbeatTimer);
      shutdown("shutdown_message");
    }
  });
  process.on("SIGTERM", () => {
    clearInterval(metricTimer);
    clearInterval(evaluateTimer);
    clearInterval(heartbeatTimer);
    shutdown("sigterm");
  });
  process.on("SIGINT", () => {
    clearInterval(metricTimer);
    clearInterval(evaluateTimer);
    clearInterval(heartbeatTimer);
    shutdown("sigint");
  });
}

main().catch((error) => {
  logger.error("请求监控子代理致命错误", { error: error.message });
  process.exit(1);
});
