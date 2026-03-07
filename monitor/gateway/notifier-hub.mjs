import { join } from "node:path";
import { appendFile, mkdir } from "node:fs/promises";

const LEVEL_PRIORITY = Object.freeze({
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  critical: 50
});

const GOTIFY_PRIORITY = Object.freeze({
  info: 2,
  warn: 5,
  error: 8,
  critical: 10,
  report: 3
});

const LEVEL_LABEL = Object.freeze({
  debug: "调试",
  info: "信息",
  warn: "告警",
  error: "错误",
  critical: "严重"
});

function normalizeLevel(level) {
  const value = typeof level === "string" ? level.toLowerCase() : "info";
  return LEVEL_PRIORITY[value] ? value : "info";
}

function toLevelLabel(level) {
  return LEVEL_LABEL[normalizeLevel(level)] ?? LEVEL_LABEL.info;
}

function shouldNotify(level, minLevel) {
  return LEVEL_PRIORITY[normalizeLevel(level)] >= LEVEL_PRIORITY[normalizeLevel(minLevel)];
}

function toNumber(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function trimTrailingSlash(url) {
  if (typeof url !== "string") {
    return "";
  }
  return url.replace(/\/+$/, "");
}

function summarizeData(data, maxLength = 600) {
  if (data == null) {
    return "";
  }
  try {
    const text = typeof data === "string" ? data : JSON.stringify(data);
    if (!text) {
      return "";
    }
    if (text.length <= maxLength) {
      return text;
    }
    return `${text.slice(0, maxLength)}...`;
  } catch {
    return "数据无法序列化";
  }
}

export class GotifyNotifier {
  constructor(config, logger) {
    this.config = config ?? {};
    this.logger = logger;
    this.sentTimestamps = [];
  }

  _consumeRateLimit(nowTs) {
    const rateLimitPerMin = toNumber(this.config?.rate_limit_per_min, 10);
    if (rateLimitPerMin <= 0) {
      return true;
    }

    const windowStart = nowTs - 60 * 1000;
    this.sentTimestamps = this.sentTimestamps.filter((item) => item >= windowStart);
    if (this.sentTimestamps.length >= rateLimitPerMin) {
      return false;
    }
    this.sentTimestamps.push(nowTs);
    return true;
  }

  _buildPayload(notification, options = {}) {
    const level = normalizeLevel(notification.level ?? "info");
    if (notification.type === "status_report") {
      const title =
        typeof notification.title === "string" && notification.title.trim()
          ? notification.title.trim()
          : "状态报告";
      const message =
        typeof notification.message === "string" && notification.message.trim()
          ? notification.message.trim()
          : "无详细描述";
      const priorityOverride = toNumber(options.gotifyPriority, toNumber(notification.priority, GOTIFY_PRIORITY.report));
      return {
        title,
        message,
        priority: Math.max(1, Math.round(priorityOverride))
      };
    }

    const ruleName =
      typeof notification.title === "string" && notification.title.trim()
        ? notification.title.trim()
        : typeof notification.rule_id === "string" && notification.rule_id.trim()
          ? notification.rule_id.trim()
          : "未命名规则";
    const title = `[${toLevelLabel(level)}] ${ruleName}`;
    const detail =
      typeof notification.message === "string" && notification.message.trim()
        ? notification.message.trim()
        : "无详细描述";
    const summary = summarizeData(notification.data);
    const message = summary ? `${detail}\n数据摘要: ${summary}` : detail;
    const priority = GOTIFY_PRIORITY[level] ?? GOTIFY_PRIORITY.info;
    return { title, message, priority };
  }

  async notify(notification, options = {}) {
    const gotifyUrl = trimTrailingSlash(this.config?.url ?? "");
    const token = typeof this.config?.token === "string" ? this.config.token.trim() : "";
    if (!gotifyUrl || !token) {
      this.logger.warn("跳过推送通知：缺少地址或令牌");
      return {
        ok: false,
        skipped: true,
        reason: "missing_config"
      };
    }

    const nowTs = Date.now();
    if (!this._consumeRateLimit(nowTs)) {
      this.logger.warn("跳过推送通知：触发速率限制", {
        rate_limit_per_min: toNumber(this.config?.rate_limit_per_min, 10)
      });
      return {
        ok: false,
        skipped: true,
        reason: "rate_limited"
      };
    }

    const requestPayload = this._buildPayload(notification, options);
    const endpoint = `${gotifyUrl}/message`;
    const body = new URLSearchParams();
    body.set("title", requestPayload.title);
    body.set("message", requestPayload.message);
    body.set("priority", String(requestPayload.priority));

    try {
      const response = await fetch(`${endpoint}?token=${encodeURIComponent(token)}`, {
        method: "POST",
        body
      });
      if (!response.ok) {
        const errorText = (await response.text().catch(() => "")).trim();
        throw new Error(`http_${response.status}${errorText ? `: ${errorText.slice(0, 200)}` : ""}`);
      }
      return {
        ok: true,
        priority: requestPayload.priority
      };
    } catch (error) {
      this.logger.error("推送通知发送失败", {
        endpoint,
        error: error instanceof Error ? error.message : String(error)
      });
      return {
        ok: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }
}

export class NotifierHub {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.registeredChannels = new Map();
  }

  async init() {
    await mkdir(this.config.alerts_file_dir, { recursive: true });
  }

  registerChannel(name, notifier) {
    if (typeof name !== "string" || !name.trim()) {
      throw new TypeError("通知通道名称不能为空");
    }
    if (!notifier || typeof notifier.notify !== "function") {
      throw new TypeError(`通知通道 ${name} 必须实现 notify()`);
    }
    this.registeredChannels.set(name, notifier);
  }

  async notify(notification) {
    const results = [];
    const level = normalizeLevel(notification.level ?? "info");
    const payload = {
      ts: new Date().toISOString(),
      ...notification,
      level
    };

    const stdoutConfig = this.config.channels?.stdout ?? { enabled: true, min_level: "info" };
    if (stdoutConfig.enabled && shouldNotify(level, stdoutConfig.min_level)) {
      try {
        this.logger.info("告警通知", payload);
        results.push({ channel: "stdout", ok: true });
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error("标准输出通知失败", { error: errorMessage });
        results.push({ channel: "stdout", ok: false, error: errorMessage });
      }
    }

    const fileConfig = this.config.channels?.file ?? { enabled: true, min_level: "info" };
    if (fileConfig.enabled && shouldNotify(level, fileConfig.min_level)) {
      const day = payload.ts.slice(0, 10);
      const filePath = join(this.config.alerts_file_dir, `alerts-${day}.jsonl`);
      try {
        await appendFile(filePath, `${JSON.stringify(payload)}\n`, "utf8");
        results.push({ channel: "file", ok: true, file: filePath });
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error("文件通知失败", {
          file: filePath,
          error: errorMessage
        });
        results.push({ channel: "file", ok: false, file: filePath, error: errorMessage });
      }
    }

    for (const [channelName, notifier] of this.registeredChannels.entries()) {
      const channelConfig = this.config.channels?.[channelName] ?? {};
      const enabled = channelConfig.enabled !== false;
      if (!enabled || !shouldNotify(level, channelConfig.min_level ?? "info")) {
        continue;
      }
      try {
        const channelResult = await notifier.notify(payload);
        const normalized =
          channelResult && typeof channelResult === "object"
            ? channelResult
            : { ok: Boolean(channelResult) };
        results.push({ channel: channelName, ...normalized });
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error("自定义通道通知失败", {
          channel: channelName,
          error: errorMessage
        });
        results.push({ channel: channelName, ok: false, error: errorMessage });
      }
    }

    return {
      success: true,
      data: results,
      error: null,
      meta: {
        channels: results.length
      }
    };
  }

  async sendReport(report = {}) {
    const level = "info";
    const payload = {
      ts: new Date().toISOString(),
      source: typeof report.source === "string" && report.source.trim() ? report.source.trim() : "monitor-gateway",
      type: "status_report",
      level,
      title: typeof report.title === "string" && report.title.trim() ? report.title.trim() : "状态报告",
      message: typeof report.message === "string" && report.message.trim() ? report.message.trim() : "无详细描述",
      data: report.data && typeof report.data === "object" ? report.data : {},
      priority: GOTIFY_PRIORITY.report
    };

    const channelsInput = Array.isArray(report.channels) ? report.channels : [];
    const channels = channelsInput.length > 0 ? Array.from(new Set(channelsInput.map((item) => String(item).trim()).filter(Boolean))) : ["gotify"];

    const results = [];
    for (const channelName of channels) {
      if (channelName === "stdout") {
        const stdoutConfig = this.config.channels?.stdout ?? { enabled: true };
        if (stdoutConfig.enabled === false) {
          results.push({ channel: "stdout", ok: false, skipped: true, reason: "disabled" });
          continue;
        }
        try {
          this.logger.info("状态报告通知", payload);
          results.push({ channel: "stdout", ok: true });
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.logger.error("标准输出状态报告失败", { error: errorMessage });
          results.push({ channel: "stdout", ok: false, error: errorMessage });
        }
        continue;
      }

      if (channelName === "file") {
        const fileConfig = this.config.channels?.file ?? { enabled: true };
        if (fileConfig.enabled === false) {
          results.push({ channel: "file", ok: false, skipped: true, reason: "disabled" });
          continue;
        }
        const day = payload.ts.slice(0, 10);
        const filePath = join(this.config.alerts_file_dir, `reports-${day}.jsonl`);
        try {
          await appendFile(filePath, `${JSON.stringify(payload)}\n`, "utf8");
          results.push({ channel: "file", ok: true, file: filePath });
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.logger.error("文件状态报告写入失败", { file: filePath, error: errorMessage });
          results.push({ channel: "file", ok: false, file: filePath, error: errorMessage });
        }
        continue;
      }

      const channelConfig = this.config.channels?.[channelName] ?? {};
      if (channelConfig.enabled === false) {
        results.push({ channel: channelName, ok: false, skipped: true, reason: "disabled" });
        continue;
      }
      const notifier = this.registeredChannels.get(channelName);
      if (!notifier) {
        results.push({ channel: channelName, ok: false, skipped: true, reason: "not_registered" });
        continue;
      }

      try {
        const channelResult = await notifier.notify(payload, {
          bypassMinLevel: true,
          gotifyPriority: GOTIFY_PRIORITY.report
        });
        const normalized =
          channelResult && typeof channelResult === "object"
            ? channelResult
            : { ok: Boolean(channelResult) };
        results.push({ channel: channelName, ...normalized });
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error("自定义通道状态报告失败", {
          channel: channelName,
          error: errorMessage
        });
        results.push({ channel: channelName, ok: false, error: errorMessage });
      }
    }

    return {
      success: true,
      data: results,
      error: null,
      meta: {
        channels: results.length,
        report: true
      }
    };
  }
}
