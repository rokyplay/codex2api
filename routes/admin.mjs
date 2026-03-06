/**
 * 管理面板 API 路由
 *
 * 路径:
 *   POST   /admin/api/login             → 管理员登录（返回 session token）
 *   POST   /admin/api/logout            → 登出（销毁 session）
 *   GET    /admin/api/dashboard         → 仪表盘数据
 *   GET    /admin/api/accounts          → 账号列表（脱敏）
 *   GET    /admin/api/accounts/lifespan → 账号寿命统计
 *   POST   /admin/api/accounts/import   → 批量导入账号
 *   GET    /admin/api/accounts/export   → 导出账号（完整）
 *   POST   /admin/api/accounts/:email/action → 账号操作
 *   DELETE /admin/api/accounts/:email   → 删除账号
 *   GET    /admin/api/config            → 配置信息（脱敏）
 *   PUT    /admin/api/config            → 更新配置（部分更新）
 *   GET    /admin/api/rate-limits       → 获取 RPM/TPM 限速配置
 *   PUT    /admin/api/rate-limits       → 更新全局/默认 RPM/TPM 限速
 *   PUT    /admin/api/rate-limits/user/:identity → 设置用户 RPM/TPM 覆盖
 *   DELETE /admin/api/rate-limits/user/:identity → 删除用户 RPM/TPM 覆盖
 *   GET    /admin/api/discord/users     → Discord 用户列表（支持 seq_id 搜索）
 *   GET    /admin/api/logs              → 获取请求日志
 *   GET    /admin/api/logs/stats        → 日志统计
 *   DELETE /admin/api/logs              → 清空日志
 *
 * 零依赖: 全用 Node.js 内置模块
 */

import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readBody, extractBearerToken } from '../lib/http-utils.mjs';
import { getRealClientIp } from '../lib/ip-utils.mjs';
import * as modelMapper from '../lib/converter/model-mapper.mjs';
import { normalizeRateLimitConfig } from '../lib/rate-limiter.mjs';
import { log, C } from '../lib/utils.mjs';
import { refreshAccountToken } from '../lib/token-refresher.mjs';
import { checkAccountBanStatus, checkAccountsBatch } from '../lib/account-checker.mjs';
import { testOneAccount } from '../lib/account-tester.mjs';
import {
  startRegistration,
  getRegistrationStatus,
  ensureServerRunning,
  ensureProxyConfig as ensureProxyConfigFromRegisterClient,
  ensureRegisterProxyConfig as ensureRegisterProxyConfigFromRegisterClient,
  buildRegisterProxyForwardPayload as buildRegisterProxyForwardPayloadFromRegisterClient,
} from '../lib/register-client.mjs';
import {
  generateRandomBase32Secret,
  verifyTotpCode,
  buildOtpAuthUri,
} from '../lib/totp.mjs';


var VERSION = '1.0.0';
var __filename = fileURLToPath(import.meta.url);
var __dirname = dirname(__filename);

// ============ Session 管理 ============

/** @type {Map<string, { username: string, createdAt: number, ip?: string }>} */
var sessions = new Map();
var ADMIN_SESSIONS_FILE = resolve(__dirname, '../data/admin-sessions.json');
var SESSION_PERSIST_DEBOUNCE_MS = 1000;
var _sessionPersistTimer = null;
var _sessionPersistPending = false;
var _sessionPersistInFlight = false;

function serializeSessions() {
  var obj = {};
  for (var entry of sessions.entries()) {
    var token = entry[0];
    var session = entry[1];
    if (!token || typeof token !== 'string') continue;
    if (!session || typeof session !== 'object') continue;
    var createdAt = Number(session.createdAt);
    if (!Number.isFinite(createdAt) || createdAt <= 0) continue;
    var item = {
      username: typeof session.username === 'string' ? session.username : '',
      createdAt: createdAt,
    };
    if (typeof session.ip === 'string' && session.ip) {
      item.ip = session.ip;
    }
    obj[token] = item;
  }
  return obj;
}

function deserializeSessions(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return new Map();
  var map = new Map();
  var tokens = Object.keys(raw);
  for (var i = 0; i < tokens.length; i++) {
    var token = tokens[i];
    var item = raw[token];
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    var createdAt = Number(item.createdAt);
    if (!Number.isFinite(createdAt) || createdAt <= 0) continue;
    var session = {
      username: typeof item.username === 'string' ? item.username : '',
      createdAt: createdAt,
    };
    if (typeof item.ip === 'string' && item.ip) {
      session.ip = item.ip;
    }
    map.set(token, session);
  }
  return map;
}

function loadSessions() {
  try {
    var dir = dirname(ADMIN_SESSIONS_FILE);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    if (!existsSync(ADMIN_SESSIONS_FILE)) return new Map();
    var raw = readFileSync(ADMIN_SESSIONS_FILE, 'utf8');
    if (!raw || !raw.trim()) return new Map();
    return deserializeSessions(JSON.parse(raw));
  } catch (e) {
    log('⚠️', C.yellow, '加载 admin sessions 失败: ' + e.message);
    return new Map();
  }
}

function flushSessionPersist() {
  if (!_sessionPersistPending || _sessionPersistInFlight) return;
  _sessionPersistPending = false;
  _sessionPersistInFlight = true;
  var payload = JSON.stringify(serializeSessions(), null, 2);
  writeFile(ADMIN_SESSIONS_FILE, payload, 'utf8')
    .catch(function (e) {
      log('⚠️', C.yellow, '保存 admin sessions 失败: ' + e.message);
    })
    .finally(function () {
      _sessionPersistInFlight = false;
      if (_sessionPersistPending) {
        scheduleSessionPersist();
      }
    });
}

function scheduleSessionPersist() {
  _sessionPersistPending = true;
  if (_sessionPersistTimer) return;
  _sessionPersistTimer = setTimeout(function () {
    _sessionPersistTimer = null;
    flushSessionPersist();
  }, SESSION_PERSIST_DEBOUNCE_MS);
  if (_sessionPersistTimer.unref) _sessionPersistTimer.unref();
}

sessions = loadSessions();

/**
 * 生成随机 session token
 * @returns {string}
 */
function generateSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

/**
 * 清理过期 sessions（惰性调用）
 */
var SESSION_MAX = 100;
var lastSessionCleanup = 0;
var TOTP_SETUP_TTL_MS = 10 * 60 * 1000;
var LOGIN_FAIL_WINDOW_MS = 10 * 60 * 1000;
var LOGIN_FAIL_MAX_ATTEMPTS = 10;
var ERROR_STRING_MAX_LEN = 100;
var IDENTITY_PATTERN = /^[A-Za-z0-9._:@-]{1,128}$/;

/** @type {Map<string, { secretBase32: string, createdAt: number }>} */
var pendingTotpSetups = new Map();
/** @type {Map<string, number[]>} */
var adminLoginFailures = new Map();

function normalizeClientIp(ip) {
  var value = String(ip || '').trim();
  return value || 'unknown';
}

function pruneAdminLoginFailures(now) {
  for (var entry of adminLoginFailures.entries()) {
    var ip = entry[0];
    var timestamps = Array.isArray(entry[1]) ? entry[1] : [];
    var kept = [];
    for (var i = 0; i < timestamps.length; i++) {
      if (now - timestamps[i] <= LOGIN_FAIL_WINDOW_MS) {
        kept.push(timestamps[i]);
      }
    }
    if (kept.length > 0) {
      adminLoginFailures.set(ip, kept);
    } else {
      adminLoginFailures.delete(ip);
    }
  }
}

function getAdminLoginBlockState(ip, now) {
  var tsNow = Number.isFinite(now) ? now : Date.now();
  var normalizedIp = normalizeClientIp(ip);
  pruneAdminLoginFailures(tsNow);
  var timestamps = adminLoginFailures.get(normalizedIp) || [];
  if (timestamps.length < LOGIN_FAIL_MAX_ATTEMPTS) {
    return { blocked: false, retryAfterSeconds: 0 };
  }
  var oldest = timestamps[0];
  var remainingMs = LOGIN_FAIL_WINDOW_MS - (tsNow - oldest);
  if (remainingMs <= 0) {
    return { blocked: false, retryAfterSeconds: 0 };
  }
  return {
    blocked: true,
    retryAfterSeconds: Math.max(1, Math.ceil(remainingMs / 1000)),
  };
}

function recordAdminLoginFailure(ip, now) {
  var tsNow = Number.isFinite(now) ? now : Date.now();
  var normalizedIp = normalizeClientIp(ip);
  pruneAdminLoginFailures(tsNow);
  var timestamps = adminLoginFailures.get(normalizedIp) || [];
  timestamps.push(tsNow);
  adminLoginFailures.set(normalizedIp, timestamps);
}

function clearAdminLoginFailures(ip) {
  var normalizedIp = normalizeClientIp(ip);
  adminLoginFailures.delete(normalizedIp);
}

function cleanupSessions(config) {
  var now = Date.now();
  if (now - lastSessionCleanup < 60000) return; // 最多每分钟清理一次
  lastSessionCleanup = now;
  var maxAge = (config.server && config.server.session_max_age_hours) || 24;
  var maxAgeMs = maxAge * 60 * 60 * 1000;
  var changed = false;
  for (var [token, session] of sessions) {
    if (now - session.createdAt > maxAgeMs) {
      sessions.delete(token);
      changed = true;
    }
  }
  // 如果仍超上限，删最旧的
  if (sessions.size > SESSION_MAX) {
    var entries = Array.from(sessions.entries());
    entries.sort(function (a, b) { return a[1].createdAt - b[1].createdAt; });
    while (sessions.size > SESSION_MAX) {
      sessions.delete(entries.shift()[0]);
      changed = true;
    }
  }
  if (changed) scheduleSessionPersist();
}

/**
 * 检查 session 是否有效
 * @param {string} token
 * @param {object} config
 * @returns {boolean}
 */
function isSessionValid(token, config) {
  if (!token) return false;
  var session = sessions.get(token);
  if (!session) return false;

  var maxAge = (config.server && config.server.session_max_age_hours) || 24;
  var maxAgeMs = maxAge * 60 * 60 * 1000;
  if (Date.now() - session.createdAt > maxAgeMs) {
    sessions.delete(token);
    scheduleSessionPersist();
    return false;
  }
  return true;
}

/**
 * 销毁 session
 * @param {string} token
 */
function destroySession(token) {
  if (!token) return;
  if (sessions.delete(token)) {
    scheduleSessionPersist();
  }
}

function cleanupPendingTotpSetups() {
  var now = Date.now();
  for (var entry of pendingTotpSetups.entries()) {
    if (now - entry[1].createdAt > TOTP_SETUP_TTL_MS) {
      pendingTotpSetups.delete(entry[0]);
    }
  }
}

function setPendingTotpSetup(sessionToken, secretBase32) {
  if (!sessionToken) return;
  cleanupPendingTotpSetups();
  pendingTotpSetups.set(sessionToken, {
    secretBase32: secretBase32,
    createdAt: Date.now(),
  });
}

function getPendingTotpSetup(sessionToken) {
  if (!sessionToken) return null;
  cleanupPendingTotpSetups();
  var pending = pendingTotpSetups.get(sessionToken);
  if (!pending) return null;
  if (Date.now() - pending.createdAt > TOTP_SETUP_TTL_MS) {
    pendingTotpSetups.delete(sessionToken);
    return null;
  }
  return pending;
}

function clearPendingTotpSetup(sessionToken) {
  if (!sessionToken) return;
  pendingTotpSetups.delete(sessionToken);
}

function getTotpSettings(config) {
  var server = (config && config.server) || {};
  var stepSeconds = Number.isFinite(server.totp_period_seconds) && server.totp_period_seconds > 0
    ? Math.floor(server.totp_period_seconds)
    : 30;
  var digits = Number.isFinite(server.totp_digits) && server.totp_digits >= 4 && server.totp_digits <= 10
    ? Math.floor(server.totp_digits)
    : 6;
  var window = Number.isFinite(server.totp_window) && server.totp_window >= 0
    ? Math.floor(server.totp_window)
    : 1;
  return {
    enabled: server.totp_enabled === true,
    secretBase32: typeof server.totp_secret === 'string' ? server.totp_secret.trim() : '',
    stepSeconds: stepSeconds,
    digits: digits,
    window: window,
    issuer: (typeof server.totp_issuer === 'string' && server.totp_issuer.trim()) || 'codex2api',
    allowPasswordless: server.totp_allow_passwordless !== false,
  };
}

async function persistConfigAndHotReload(config, ctx, configPath) {
  writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');

  try {
    var modelMapperModule = await import('../lib/converter/model-mapper.mjs');
    if (typeof modelMapperModule.hotReload === 'function') {
      modelMapperModule.hotReload(config.models);
    } else {
      modelMapperModule.init(config.models);
    }

    if (config.scheduler) {
      var schedulerModule = await import('../lib/scheduler.mjs');
      var newScheduler = schedulerModule.createScheduler(config.scheduler.mode || 'round_robin');
      ctx.pool.setScheduler(newScheduler);
    }

    if (ctx && ctx.rateLimiter && typeof ctx.rateLimiter.updateConfig === 'function') {
      ctx.rateLimiter.updateConfig(config.rate_limits || {});
    }

    if (ctx && ctx.autoRelogin) {
      if (config && config.credentials && config.credentials.auto_relogin === true) {
        if (typeof ctx.autoRelogin.start === 'function') ctx.autoRelogin.start();
      } else {
        if (typeof ctx.autoRelogin.stop === 'function') ctx.autoRelogin.stop();
      }
    }
  } catch (reloadErr) {
    log('⚠️', C.yellow, '组件热更新部分失败: ' + reloadErr.message);
  }
}

// ============ 安全工具 ============

/**
 * 时序安全的字符串比较 — 防止 timing attack
 * @param {string} a
 * @param {string} b
 * @returns {boolean}
 */
function safeCompare(a, b) {
  var bufA = Buffer.from(String(a), 'utf8');
  var bufB = Buffer.from(String(b), 'utf8');

  // 长度不同时做假比较，避免长度泄漏
  if (bufA.length !== bufB.length) {
    var dummy = Buffer.alloc(bufA.length);
    crypto.timingSafeEqual(bufA, dummy);
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * 递归脱敏 — 匹配敏感关键词的字段值替换为 '***'
 * @param {*} obj
 * @returns {*}
 */
var SENSITIVE_KEYWORDS = ['password', 'token', 'secret', 'key', 'credential'];
var SENSITIVE_EXACT_KEYS = ['totp_secret'];

function sanitizeConfig(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) {
    return obj.map(function (item) { return sanitizeConfig(item); });
  }

  var result = {};
  var keys = Object.keys(obj);
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    var lowerKey = k.toLowerCase();
    var isSensitive = SENSITIVE_EXACT_KEYS.indexOf(lowerKey) >= 0;
    for (var j = 0; j < SENSITIVE_KEYWORDS.length; j++) {
      if (lowerKey.indexOf(SENSITIVE_KEYWORDS[j]) >= 0) {
        isSensitive = true;
        break;
      }
    }
    if (isSensitive && typeof obj[k] === 'string' && obj[k].length > 0) {
      result[k] = '***';
    } else {
      result[k] = sanitizeConfig(obj[k]);
    }
  }
  return result;
}

// ============ 日志收集器（环形缓冲区） ============

var LOG_BUFFER_MAX = 1000;

var logBuffer = [];
var logStats = { total: 0, info: 0, warn: 0, error: 0, request: 0 };

var LOG_FILE = null;
var _logSaveTimer = null;

/**
 * 初始化日志持久化
 */
export function initLogPersistence(dataDir) {
  LOG_FILE = resolve(dataDir, 'logs.json');
  if (existsSync(LOG_FILE)) {
    try {
      var saved = JSON.parse(readFileSync(LOG_FILE, 'utf8'));
      if (saved.logs && Array.isArray(saved.logs)) {
        logBuffer = saved.logs.slice(-LOG_BUFFER_MAX);
      }
      if (saved.stats) {
        logStats = Object.assign({ total: 0, info: 0, warn: 0, error: 0, request: 0 }, saved.stats);
      }
    } catch (_) {
      // 文件损坏，忽略
    }
  }
}

function _scheduleLogSave() {
  if (!LOG_FILE || _logSaveTimer) return;
  _logSaveTimer = setTimeout(function () {
    _logSaveTimer = null;
    _doLogSave();
  }, 10000);
  if (_logSaveTimer.unref) _logSaveTimer.unref();
}

function _doLogSave() {
  if (!LOG_FILE) return;
  if (_logSaveTimer) {
    clearTimeout(_logSaveTimer);
    _logSaveTimer = null;
  }
  try {
    var dir = dirname(LOG_FILE);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    writeFileSync(LOG_FILE, JSON.stringify({ logs: logBuffer, stats: logStats }));
  } catch (_) {
    // 静默失败
  }
}

/**
 * 日志收集器 — 导出供 server.mjs 使用
 */
export var logCollector = {
  /**
   * 添加日志
   * @param {'info'|'warn'|'error'|'request'} level
   * @param {string} message
   * @param {object} [meta]
   */
  add: function (level, message, meta) {
    var entry = {
      timestamp: Date.now(),
      level: level,
      message: message,
      meta: meta || null,
    };

    if (logBuffer.length >= LOG_BUFFER_MAX) {
      logBuffer.shift();
    }
    logBuffer.push(entry);

    logStats.total++;
    if (logStats[level] !== undefined) {
      logStats[level]++;
    }

    _scheduleLogSave();
  },

  /**
   * 获取日志（支持过滤）
   * @param {{ level?: string, search?: string, limit?: number, offset?: number }} filters
   * @returns {{ logs: Array, total: number }}
   */
  getAll: function (filters) {
    filters = filters || {};
    var filtered = logBuffer;

    if (filters.level) {
      var targetLevel = filters.level;
      filtered = filtered.filter(function (entry) {
        return entry.level === targetLevel;
      });
    }

    if (filters.search) {
      var searchLower = filters.search.toLowerCase();
      filtered = filtered.filter(function (entry) {
        return entry.message.toLowerCase().indexOf(searchLower) >= 0;
      });
    }

    var total = filtered.length;
    var offset = parseInt(filters.offset, 10) || 0;
    var limit = parseInt(filters.limit, 10) || 50;

    // 按时间倒序
    var sorted = filtered.slice().reverse();
    var paged = sorted.slice(offset, offset + limit);

    return { logs: paged, total: total };
  },

  /**
   * 获取统计
   * @returns {{ total: number, info: number, warn: number, error: number, request: number }}
   */
  getStats: function () {
    return Object.assign({}, logStats);
  },

  /**
   * 清空日志
   */
  clear: function () {
    logBuffer = [];
    logStats = { total: 0, info: 0, warn: 0, error: 0, request: 0 };
    _doLogSave();
  },

  forceSave: function () {
    _doLogSave();
  },
};

// ============ 认证中间件 ============

/**
 * 认证中间件 — 验证 Bearer session token
 *
 * @param {IncomingMessage} req
 * @param {object} config
 * @returns {{ ok: boolean, reason?: string }}
 */
function requireAuth(req, config) {
  var password = config.server && config.server.admin_password;

  // 空密码时，管理面板 API 全部禁止访问
  if (!password) {
    return { ok: false, reason: 'password_required' };
  }

  var token = extractBearerToken(req.headers['authorization'] || '');
  if (!token) {
    return { ok: false, reason: 'unauthorized' };
  }

  if (!isSessionValid(token, config)) {
    return { ok: false, reason: 'session_expired' };
  }

  return { ok: true };
}

// ============ 辅助函数 ============

/**
 * 脱敏 accessToken — 只保留前 20 字符
 */
function maskToken(token) {
  if (!token || typeof token !== 'string') return '';
  if (token.length <= 20) return token;
  return token.substring(0, 20) + '...';
}

function truncateErrorString(value) {
  var text = String(value || '');
  if (text.length <= ERROR_STRING_MAX_LEN) return text;
  return text.slice(0, ERROR_STRING_MAX_LEN);
}

function sanitizeErrorPayload(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return truncateErrorString(value);
  if (Array.isArray(value)) {
    var arr = [];
    for (var i = 0; i < value.length; i++) {
      arr.push(sanitizeErrorPayload(value[i]));
    }
    return arr;
  }
  if (typeof value === 'object') {
    var out = {};
    var keys = Object.keys(value);
    for (var j = 0; j < keys.length; j++) {
      var key = keys[j];
      out[key] = sanitizeErrorPayload(value[key]);
    }
    return out;
  }
  return value;
}

/**
 * JSON 响应辅助
 */
function jsonResponse(res, statusCode, data) {
  var payload = data;
  if (statusCode >= 400) {
    payload = sanitizeErrorPayload(data);
  }
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function _setAccountStatusWithTimestamp(account, status) {
  if (!account || typeof status !== 'string' || !status) return false;
  if (account.status === status) {
    if (!account.status_changed_at || !Number.isFinite(Number(account.status_changed_at))) {
      account.status_changed_at = Date.now();
    }
    return false;
  }
  account.status = status;
  account.status_changed_at = Date.now();
  return true;
}

/**
 * 解析 URL 路径参数
 * /admin/api/accounts/user@example.com/action → "user@example.com"
 */
function extractEmailFromPath(path) {
  var match = path.match(/^\/admin\/api\/accounts\/([^/]+)\/action$/);
  if (match) return decodeURIComponent(match[1]);
  return null;
}

function extractEmailForDelete(path) {
  var match = path.match(/^\/admin\/api\/accounts\/([^/]+)$/);
  if (match && match[1] !== 'import' && match[1] !== 'export') {
    return decodeURIComponent(match[1]);
  }
  return null;
}

function extractApiKeyIdFromPath(path) {
  var match = path.match(/^\/admin\/api\/api-keys\/([^/]+)$/);
  if (match) return decodeURIComponent(match[1]);
  return null;
}

function extractApiKeyRotateIdFromPath(path) {
  var match = path.match(/^\/admin\/api\/api-keys\/([^/]+)\/rotate$/);
  if (match) return decodeURIComponent(match[1]);
  return null;
}

function extractAbuseUserId(path) {
  var match = path.match(/^\/admin\/api\/abuse\/user\/([^/]+)$/);
  if (match) return decodeURIComponent(match[1]);
  return null;
}

function extractAbuseUserActionId(path) {
  var match = path.match(/^\/admin\/api\/abuse\/user\/([^/]+)\/action$/);
  if (match) return decodeURIComponent(match[1]);
  return null;
}

function extractRateLimitIdentity(path) {
  var match = path.match(/^\/admin\/api\/rate-limits\/user\/([^/]+)$/);
  if (match) return decodeURIComponent(match[1]);
  return null;
}

function ensureApiKeyStore(config) {
  if (!config.server || typeof config.server !== 'object') {
    config.server = {};
  }
  if (!Array.isArray(config.server.api_keys)) {
    config.server.api_keys = [];
  }
  return config.server.api_keys;
}

function ensureRateLimitsConfig(config) {
  if (!config || typeof config !== 'object') {
    return normalizeRateLimitConfig({});
  }
  config.rate_limits = normalizeRateLimitConfig(config.rate_limits || {});
  return config.rate_limits;
}

function parseRateLimitNumber(value, fallback, fieldName) {
  if (value === undefined) return fallback;
  var n = Number(value);
  if (!isFinite(n) || n < 0) {
    throw new Error(fieldName + '_invalid');
  }
  return Math.floor(n);
}

function parseRateLimitPair(value, fallback, fieldPrefix) {
  var base = fallback && typeof fallback === 'object' ? fallback : { rpm: 0, tpm: 0 };
  if (value === undefined) {
    return {
      rpm: parseRateLimitNumber(undefined, base.rpm || 0, fieldPrefix + '.rpm'),
      tpm: parseRateLimitNumber(undefined, base.tpm || 0, fieldPrefix + '.tpm'),
    };
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(fieldPrefix + '_invalid');
  }
  return {
    rpm: parseRateLimitNumber(value.rpm, base.rpm || 0, fieldPrefix + '.rpm'),
    tpm: parseRateLimitNumber(value.tpm, base.tpm || 0, fieldPrefix + '.tpm'),
  };
}

function isValidApiKeyId(id) {
  return typeof id === 'string' && /^[A-Za-z0-9._:-]{1,64}$/.test(id);
}

function validateIdentityValue(identity) {
  var normalized = String(identity || '').trim();
  if (!normalized) {
    return { ok: false, reason: 'identity_required', value: '' };
  }
  if (!IDENTITY_PATTERN.test(normalized)) {
    return { ok: false, reason: 'identity_invalid', value: normalized };
  }
  return { ok: true, reason: '', value: normalized };
}

function maskApiKey(key) {
  var raw = String(key || '');
  if (!raw) return '';
  if (raw.length <= 12) return raw;
  return raw.substring(0, 8) + '...' + raw.substring(raw.length - 4);
}

function sanitizeApiKeyItem(item) {
  return {
    id: item.id ? String(item.id) : '',
    identity: item.identity ? String(item.identity) : '',
    key: maskApiKey(item.key),
    enabled: item.enabled !== false,
    created_at: item.created_at || null,
    updated_at: item.updated_at || null,
    rotated_at: item.rotated_at || null,
  };
}

function persistConfig(config, configPath) {
  writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
}

/**
 * 解析 URL 查询参数
 */
function parseQuery(url) {
  var idx = url.indexOf('?');
  if (idx < 0) return {};
  var qs = url.substring(idx + 1);
  var params = {};
  var pairs = qs.split('&');
  for (var i = 0; i < pairs.length; i++) {
    var eqIdx = pairs[i].indexOf('=');
    if (eqIdx >= 0) {
      var key = decodeURIComponent(pairs[i].substring(0, eqIdx));
      var val = decodeURIComponent(pairs[i].substring(eqIdx + 1));
      params[key] = val;
    }
  }
  return params;
}

function _dateStrFromTs(ts) {
  var d = new Date(ts);
  var y = d.getFullYear();
  var m = String(d.getMonth() + 1).padStart(2, '0');
  var day = String(d.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + day;
}

function _isValidDateStr(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function parseStatsOptions(url) {
  var query = parseQuery(url);
  var total = query.total === '1' || query.total === 'true';
  if (total) {
    return { mode: 'total' };
  }

  if (query.hours) {
    var h = parseInt(query.hours, 10);
    if (h > 0 && h <= 720) return { mode: 'hours', hours: h };
  }

  var from = query.from;
  var to = query.to;
  if (_isValidDateStr(from) && _isValidDateStr(to)) {
    if (from > to) {
      var tmp = from;
      from = to;
      to = tmp;
    }
    return { mode: 'range', from: from, to: to };
  }

  var days = parseInt(query.days, 10);
  if (!days || days < 1) days = 1;
  if (days > 90) days = 90;
  var toDate = _dateStrFromTs(Date.now());
  var fromDate = _dateStrFromTs(Date.now() - (days - 1) * 86400000);
  return { mode: 'range', from: fromDate, to: toDate, days: days };
}

/**
 * 深度合并对象（immutable — 返回新对象）
 */
function deepMerge(target, source) {
  if (!source || typeof source !== 'object' || Array.isArray(source)) return source;
  if (!target || typeof target !== 'object' || Array.isArray(target)) return source;

  var result = Object.assign({}, target);
  var keys = Object.keys(source);
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    if (typeof source[k] === 'object' && source[k] !== null && !Array.isArray(source[k]) &&
        typeof result[k] === 'object' && result[k] !== null && !Array.isArray(result[k])) {
      result[k] = deepMerge(result[k], source[k]);
    } else {
      result[k] = source[k];
    }
  }
  return result;
}

function cloneDeep(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function isPlainObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function validateConfigPatchSchema(patch, schema, basePath) {
  if (!isPlainObject(patch)) {
    return { ok: false, reason: (basePath || 'root') + '_must_be_object' };
  }
  if (!isPlainObject(schema)) {
    return { ok: false, reason: 'config_schema_unavailable' };
  }

  var keys = Object.keys(patch);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var path = basePath ? (basePath + '.' + key) : key;
    if (!Object.prototype.hasOwnProperty.call(schema, key)) {
      return { ok: false, reason: 'unknown_field:' + path };
    }
    var nextValue = patch[key];
    var expected = schema[key];
    if (isPlainObject(nextValue)) {
      if (!isPlainObject(expected)) {
        return { ok: false, reason: 'type_mismatch:' + path };
      }
      var nested = validateConfigPatchSchema(nextValue, expected, path);
      if (!nested.ok) return nested;
      continue;
    }
    if (Array.isArray(nextValue) && !Array.isArray(expected)) {
      return { ok: false, reason: 'type_mismatch:' + path };
    }
  }
  return { ok: true, reason: '' };
}

// ============ 路由创建 ============

/**
 * 创建管理路由处理器
 *
 * @param {object} ctx - { pool, config, i18n, t, configPath }
 * @returns {function} handler(req, res)
 */
export function createAdminRoutes(ctx) {
  var pool = ctx.pool;
  var config = ctx.config;
  var t = ctx.t;
  var configPath = ctx.configPath || './config-server.json';

  return async function (req, res) {
    var path = req.url.split('?')[0];
    var method = req.method;

    // ====== GET /admin/api/login/options ======
    if (method === 'GET' && path === '/admin/api/login/options') {
      return handleLoginOptions(res, config);
    }

    // ====== POST /admin/api/login ======
    if (method === 'POST' && path === '/admin/api/login') {
      return await handleLogin(req, res, config, t);
    }

    // ====== POST /admin/api/logout ======
    if (method === 'POST' && path === '/admin/api/logout') {
      return handleLogout(req, res, config, t);
    }

    // 以下接口需要认证
    var authResult = requireAuth(req, config);
    if (!authResult.ok) {
      var errorKey = 'admin.' + authResult.reason;
      return jsonResponse(res, authResult.reason === 'password_required' ? 403 : 401, {
        error: t(errorKey),
      });
    }

    // ====== GET /admin/api/totp/status ======
    if (method === 'GET' && path === '/admin/api/totp/status') {
      return handleTotpStatus(res, config);
    }

    // ====== POST /admin/api/totp/setup/init ======
    if (method === 'POST' && path === '/admin/api/totp/setup/init') {
      return await handleTotpSetupInit(req, res, config, t);
    }

    // ====== POST /admin/api/totp/setup/confirm ======
    if (method === 'POST' && path === '/admin/api/totp/setup/confirm') {
      return await handleTotpSetupConfirm(req, res, config, ctx, configPath, t);
    }

    // ====== POST /admin/api/totp/disable ======
    if (method === 'POST' && path === '/admin/api/totp/disable') {
      return await handleTotpDisable(req, res, config, ctx, configPath, t);
    }

    // ====== GET /admin/api/dashboard ======
    if (method === 'GET' && path === '/admin/api/dashboard') {
      return handleDashboard(res, pool, config, t);
    }

    // ====== GET /admin/api/accounts ======
    if (method === 'GET' && path === '/admin/api/accounts') {
      return handleListAccounts(res, pool);
    }

    // ====== GET /admin/api/accounts/lifespan ======
    if (method === 'GET' && path === '/admin/api/accounts/lifespan') {
      return handleAccountsLifespan(res, pool);
    }

    // ====== POST /admin/api/accounts/import ======
    if (method === 'POST' && path === '/admin/api/accounts/import') {
      return await handleImportAccounts(req, res, pool, t);
    }

    // ====== GET /admin/api/accounts/export ======
    if (method === 'GET' && path === '/admin/api/accounts/export') {
      return handleExportAccounts(res, pool);
    }


    // ====== POST /admin/api/accounts/verify-batch ======
    if (method === 'POST' && path === '/admin/api/accounts/verify-batch') {
      return await handleVerifyBatch(req, res, pool, config);
    }

    // ====== DELETE /admin/api/accounts/:email ======
    var deleteEmail = extractEmailForDelete(path);
    if (method === 'DELETE' && deleteEmail) {
      return await handleDeleteAccount(req, res, pool, deleteEmail, config, t);
    }

    // ====== POST /admin/api/accounts/:email/action ======
    var email = extractEmailFromPath(path);
    if (method === 'POST' && email) {
      return await handleAccountAction(req, res, pool, email, t, config);
    }

    // ====== API Keys 管理 ======
    if (method === 'GET' && path === '/admin/api/api-keys') {
      return handleListApiKeys(res, config);
    }
    if (method === 'POST' && path === '/admin/api/api-keys') {
      return await handleCreateApiKey(req, res, config, configPath, t);
    }
    var apiKeyRotateId = extractApiKeyRotateIdFromPath(path);
    if (method === 'POST' && apiKeyRotateId) {
      return handleRotateApiKey(res, config, configPath, apiKeyRotateId, t);
    }
    var apiKeyId = extractApiKeyIdFromPath(path);
    if (method === 'PUT' && apiKeyId) {
      return await handleUpdateApiKey(req, res, config, configPath, apiKeyId, t);
    }
    if (method === 'DELETE' && apiKeyId) {
      return handleDeleteApiKey(res, config, configPath, apiKeyId, t);
    }

    // ====== GET /admin/api/config ======
    if (method === 'GET' && path === '/admin/api/config') {
      return handleConfig(res, config);
    }

    // ====== GET /admin/api/rate-limits ======
    if (method === 'GET' && path === '/admin/api/rate-limits') {
      return handleRateLimitsGet(res, config);
    }

    // ====== PUT /admin/api/rate-limits ======
    if (method === 'PUT' && path === '/admin/api/rate-limits') {
      return await handleRateLimitsPut(req, res, ctx, config, configPath, t);
    }

    var rateLimitIdentity = extractRateLimitIdentity(path);
    if (method === 'PUT' && rateLimitIdentity) {
      return await handleRateLimitsUserPut(req, res, ctx, config, configPath, rateLimitIdentity, t);
    }
    if (method === 'DELETE' && rateLimitIdentity) {
      return await handleRateLimitsUserDelete(res, ctx, config, configPath, rateLimitIdentity, t);
    }

    // ====== GET /admin/api/models/config ======
    if (method === 'GET' && path === '/admin/api/models/config') {
      return handleModelsConfigGet(res, config);
    }

    // ====== PUT /admin/api/models/config ======
    if (method === 'PUT' && path === '/admin/api/models/config') {
      return await handleModelsConfigPut(req, res, config, ctx, configPath, t);
    }

    // ====== POST /admin/api/models/refresh ======
    if (method === 'POST' && path === '/admin/api/models/refresh') {
      return await handleModelsRefresh(res, ctx, config);
    }

    // ====== PUT /admin/api/config ======
    if (method === 'PUT' && path === '/admin/api/config') {
      return await handleUpdateConfig(req, res, config, ctx, configPath, t);
    }

    // ====== GET /admin/api/logs ======
    if (method === 'GET' && path === '/admin/api/logs') {
      return handleGetLogs(req, res);
    }

    // ====== GET /admin/api/logs/stats ======
    if (method === 'GET' && path === '/admin/api/logs/stats') {
      return handleLogStats(res);
    }

    // ====== DELETE /admin/api/logs ======
    if (method === 'DELETE' && path === '/admin/api/logs') {
      return handleClearLogs(res, t);
    }

    // ====== Stats API ======
    if (method === 'GET' && path === '/admin/api/stats/overview') {
      return handleStatsOverview(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/stats/timeseries') {
      return handleStatsTimeseries(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/stats/models') {
      return handleStatsModels(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/stats/accounts') {
      return handleStatsAccounts(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/stats/callers') {
      return handleStatsCallers(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/stats/recent') {
      var rq = parseQuery(req.url);
      var rPage = Math.max(1, parseInt(rq.page) || 1);
      var rFilter = (rq.filter === 'success' || rq.filter === 'error') ? rq.filter : '';
      var rSearch = typeof rq.search === 'string' ? rq.search.trim() : '';
      var rSource = rq.source === 'file' ? 'file' : 'memory';
      var rDate = typeof rq.date === 'string' ? rq.date.trim() : '';
      var rHours = undefined;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(rDate)) rDate = '';
      if (typeof rq.hours === 'string' && rq.hours.trim() !== '') {
        var parsedHours = parseInt(rq.hours, 10);
        if (parsedHours > 0 && parsedHours <= 720) rHours = parsedHours;
      }
      var rLimit = undefined;
      if (typeof rq.limit === 'string' && rq.limit.trim() !== '') {
        var parsedLimit = parseInt(rq.limit, 10);
        if (parsedLimit > 0) {
          rLimit = (rSource === 'file' || rSearch)
            ? Math.min(parsedLimit, 2000)
            : Math.min(parsedLimit, 100);
        }
      }
      return handleStatsRecent(res, ctx, rPage, rLimit, rFilter, rSearch, rSource, rDate, rHours);
    }
    if (method === 'GET' && path === '/admin/api/discord/users') {
      return handleDiscordUsers(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/abuse/overview') {
      return handleAbuseOverview(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/abuse/users') {
      return handleAbuseUsers(req, res, ctx);
    }
    var abuseUserId = extractAbuseUserId(path);
    if (method === 'GET' && abuseUserId) {
      return handleAbuseUserDetail(req, res, ctx, abuseUserId);
    }
    var abuseUserActionId = extractAbuseUserActionId(path);
    if (method === 'POST' && abuseUserActionId) {
      return await handleAbuseUserAction(req, res, ctx, abuseUserActionId);
    }
    if (method === 'GET' && path === '/admin/api/abuse/events') {
      return handleAbuseEvents(req, res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/abuse/rules') {
      return handleAbuseRulesGet(res, ctx, config);
    }
    if (method === 'PUT' && path === '/admin/api/abuse/rules') {
      return await handleAbuseRulesPut(req, res, ctx, config, configPath);
    }
    if (method === 'GET' && path === '/admin/api/accounts/index-map') {
      return handleAccountIndexMap(res, ctx);
    }
    if (method === 'GET' && path === '/admin/api/pool-health/status') {
      return handlePoolHealthStatus(res, ctx);
    }

    // ====== GET /admin/api/relogin/status ======
    if (method === 'GET' && path === '/admin/api/relogin/status') {
      return handleReloginStatus(res, ctx);
    }

    // ====== POST /admin/api/models/test ======
    if (method === 'POST' && path === '/admin/api/models/test') {
      return await handleTestModels(req, res, pool, config);
    }

    // ====== GET /admin/api/proxy/presets ======
    if (method === 'GET' && path === '/admin/api/proxy/presets') {
      return handleProxyPresets(res, config);
    }

    // ====== PUT /admin/api/proxy/select ======
    if (method === 'PUT' && path === '/admin/api/proxy/select') {
      return await handleProxySelect(req, res, config, configPath);
    }

    // ====== GET /admin/api/proxy/test ======
    if (method === 'GET' && path === '/admin/api/proxy/test') {
      return handleProxyTest(req, res, config);
    }

    // ====== Register API (代理转发到注册服务器) ======
    if (path.indexOf('/admin/api/register/') === 0) {
      return handleRegisterProxy(req, res, config, path, method);
    }

        // ====== POST /admin/api/accounts/check-status ======
    if (method === "POST" && path === "/admin/api/accounts/check-status") {
      return await handleCheckStatus(req, res, pool, ctx);
    }

    // ====== POST /admin/api/accounts/test-batch ======
    if (method === 'POST' && path === '/admin/api/accounts/test-batch') {
      return await handleTestBatch(req, res, pool, config);
    }

    // ====== POST /admin/api/accounts/check-batch ======
    if (method === "POST" && path === "/admin/api/accounts/check-batch") {
      return await handleCheckBatch(req, res, pool, ctx);
    }

    // 未匹配的 admin API
    jsonResponse(res, 404, { error: 'Not found: ' + path });
  };
}

// ============ 各端点处理函数 ============

/**
 * 登录 — 验证用户名密码，返回 session token
 */
function handleLoginOptions(res, config) {
  var totp = getTotpSettings(config);
  return jsonResponse(res, 200, {
    totp_enabled: totp.enabled,
    totp_allow_passwordless: totp.allowPasswordless,
    totp_digits: totp.digits,
  });
}

function createAdminSession(username, config) {
  cleanupSessions(config);
  var token = generateSessionToken();
  sessions.set(token, { username: username, createdAt: Date.now() });
  scheduleSessionPersist();
  return token;
}

async function handleLogin(req, res, config, t) {
  var expectedPassword = (config.server && config.server.admin_password) || '';
  var clientIp = normalizeClientIp((getRealClientIp(req).ip || (req.socket && req.socket.remoteAddress) || ''));

  // 空密码时禁止登录
  if (!expectedPassword) {
    return jsonResponse(res, 403, { error: t('admin.password_required') });
  }

  var blockState = getAdminLoginBlockState(clientIp, Date.now());
  if (blockState.blocked) {
    return jsonResponse(res, 429, {
      error: 'too_many_login_attempts',
      retry_after_seconds: blockState.retryAfterSeconds,
    });
  }

  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  var username = body.username || '';
  var password = body.password || '';
  var expectedUsername = (config.server && config.server.admin_username) || 'rok';
  var usernameMatch = safeCompare(username, expectedUsername);
  var totp = getTotpSettings(config);

  // TOTP 未启用时保持原行为：仅用户名 + 密码
  if (!totp.enabled) {
    var passwordMatch = safeCompare(password, expectedPassword);
    if (usernameMatch && passwordMatch) {
      var legacyToken = createAdminSession(username, config);
      clearAdminLoginFailures(clientIp);
      logCollector.add('info', 'Admin login success: ' + username);
      return jsonResponse(res, 200, { token: legacyToken });
    }
    recordAdminLoginFailure(clientIp, Date.now());
    log('⚠️', C.yellow, 'Admin login failed: ' + username);
    logCollector.add('warn', 'Admin login failed: ' + username);
    return jsonResponse(res, 401, { error: t('admin.login_failed') });
  }

  if (!totp.secretBase32) {
    return jsonResponse(res, 503, { error: t('admin.totp_not_configured') });
  }

  var mode = body.mode || 'password_totp';
  var totpCode = body.totp_code || '';

  if (!totpCode) {
    recordAdminLoginFailure(clientIp, Date.now());
    return jsonResponse(res, 401, { error: '需要TOTP验证码', require_totp: true });
  }

  var totpMatch = verifyTotpCode({
    secretBase32: totp.secretBase32,
    code: totpCode,
    timestampMs: Date.now(),
    stepSeconds: totp.stepSeconds,
    digits: totp.digits,
    window: totp.window,
  });

  if (mode === 'totp_only') {
    if (!totp.allowPasswordless) {
      return jsonResponse(res, 403, { error: t('admin.totp_passwordless_disabled') });
    }
    if (usernameMatch && totpMatch) {
      var totpOnlyToken = createAdminSession(username, config);
      clearAdminLoginFailures(clientIp);
      logCollector.add('info', 'Admin login success (totp_only): ' + username);
      return jsonResponse(res, 200, { token: totpOnlyToken });
    }
  } else if (mode === 'password_totp') {
    var passwordTotpMatch = safeCompare(password, expectedPassword);
    if (usernameMatch && passwordTotpMatch && totpMatch) {
      var passwordTotpToken = createAdminSession(username, config);
      clearAdminLoginFailures(clientIp);
      logCollector.add('info', 'Admin login success (password_totp): ' + username);
      return jsonResponse(res, 200, { token: passwordTotpToken });
    }
  } else {
    return jsonResponse(res, 400, { error: t('admin.totp_mode_invalid') });
  }

  recordAdminLoginFailure(clientIp, Date.now());
  log('⚠️', C.yellow, 'Admin login failed: ' + username);
  logCollector.add('warn', 'Admin login failed: ' + username);
  return jsonResponse(res, 401, { error: t('admin.login_failed') });
}

/**
 * 登出 — 销毁 session
 */
function handleLogout(req, res, config, t) {
  var token = extractBearerToken(req.headers['authorization'] || '');
  destroySession(token);
  clearPendingTotpSetup(token);
  logCollector.add('info', 'Admin logout');
  return jsonResponse(res, 200, { success: true, message: t('admin.logout_success') });
}

function handleTotpStatus(res, config) {
  var totp = getTotpSettings(config);
  return jsonResponse(res, 200, {
    enabled: totp.enabled,
    configured: !!totp.secretBase32,
  });
}

async function handleTotpSetupInit(req, res, config, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  var expectedPassword = (config.server && config.server.admin_password) || '';
  var adminPassword = body.admin_password || '';
  if (!expectedPassword || !safeCompare(adminPassword, expectedPassword)) {
    return jsonResponse(res, 403, { error: t('admin.password_wrong') });
  }

  var token = extractBearerToken(req.headers['authorization'] || '');
  if (!token) {
    return jsonResponse(res, 401, { error: t('admin.unauthorized') });
  }

  var totp = getTotpSettings(config);
  var accountName = (config.server && config.server.admin_username) || 'rok';
  var secretBase32 = generateRandomBase32Secret(20);
  var otpauthUri = buildOtpAuthUri({
    secretBase32: secretBase32,
    issuer: totp.issuer,
    accountName: accountName,
    digits: totp.digits,
    period: totp.stepSeconds,
  });

  setPendingTotpSetup(token, secretBase32);
  logCollector.add('info', 'TOTP setup init');
  return jsonResponse(res, 200, {
    secret_base32: secretBase32,
    otpauth_uri: otpauthUri,
  });
}

async function handleTotpSetupConfirm(req, res, config, ctx, configPath, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  var token = extractBearerToken(req.headers['authorization'] || '');
  if (!token) {
    return jsonResponse(res, 401, { error: t('admin.unauthorized') });
  }
  var pending = getPendingTotpSetup(token);
  if (!pending) {
    return jsonResponse(res, 400, { error: t('admin.totp_setup_pending_not_found') });
  }

  var totp = getTotpSettings(config);
  var code = body.code || body.totp_code || '';
  var verified = verifyTotpCode({
    secretBase32: pending.secretBase32,
    code: code,
    timestampMs: Date.now(),
    stepSeconds: totp.stepSeconds,
    digits: totp.digits,
    window: totp.window,
  });

  if (!verified) {
    return jsonResponse(res, 400, { error: t('admin.totp_code_invalid') });
  }

  if (!config.server) config.server = {};
  config.server.totp_secret = pending.secretBase32;
  config.server.totp_enabled = true;

  try {
    await persistConfigAndHotReload(config, ctx, configPath);
  } catch (e) {
    return jsonResponse(res, 500, {
      error: t('admin.config_update_failed', { reason: e.message }),
    });
  }

  clearPendingTotpSetup(token);
  logCollector.add('info', 'TOTP setup confirmed');
  return jsonResponse(res, 200, { success: true, enabled: true, configured: true });
}

async function handleTotpDisable(req, res, config, ctx, configPath, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  var expectedPassword = (config.server && config.server.admin_password) || '';
  var adminPassword = body.admin_password || '';
  if (!expectedPassword || !safeCompare(adminPassword, expectedPassword)) {
    return jsonResponse(res, 403, { error: t('admin.password_wrong') });
  }

  var totp = getTotpSettings(config);
  if (!totp.enabled) {
    return jsonResponse(res, 400, { error: t('admin.totp_not_enabled') });
  }
  if (!totp.secretBase32) {
    return jsonResponse(res, 400, { error: t('admin.totp_not_configured') });
  }

  var code = body.totp_code || body.code || '';
  var verified = verifyTotpCode({
    secretBase32: totp.secretBase32,
    code: code,
    timestampMs: Date.now(),
    stepSeconds: totp.stepSeconds,
    digits: totp.digits,
    window: totp.window,
  });

  if (!verified) {
    return jsonResponse(res, 400, { error: t('admin.totp_code_invalid') });
  }

  if (!config.server) config.server = {};
  config.server.totp_enabled = false;
  config.server.totp_secret = '';

  try {
    await persistConfigAndHotReload(config, ctx, configPath);
  } catch (e) {
    return jsonResponse(res, 500, {
      error: t('admin.config_update_failed', { reason: e.message }),
    });
  }

  clearPendingTotpSetup(extractBearerToken(req.headers['authorization'] || ''));
  logCollector.add('info', 'TOTP disabled');
  return jsonResponse(res, 200, { success: true, enabled: false, configured: false });
}

/**
 * 仪表盘 — 汇总数据
 */
function handleDashboard(res, pool, config, t) {
  var schedulerMode = (config.scheduler && config.scheduler.mode) || 'round_robin';
  var lifespan = pool.getLifespanStats ? pool.getLifespanStats() : null;
  var data = {
    accounts: pool.getStats(),
    account_lifespan: lifespan,
    uptime: process.uptime(),
    scheduler: schedulerMode,
    models: modelMapper.listModels(),
    version: VERSION,
    node_version: process.version,
  };
  return jsonResponse(res, 200, data);
}

/**
 * 账号列表 — 脱敏
 */
function handleListAccounts(res, pool) {
  var accounts = pool.listAccounts();
  // listAccounts 已经不含 accessToken，但以防万一做一层脱敏
  var masked = accounts.map(function (a) {
    return Object.assign({}, a, {
      accessToken: a.accessToken ? maskToken(a.accessToken) : undefined,
    });
  });
  var stats = pool.getStats ? pool.getStats() : {};
  var lifespan = pool.getLifespanStats ? pool.getLifespanStats() : null;
  return jsonResponse(res, 200, { accounts: masked, stats: stats, lifespan: lifespan });
}

function handleAccountsLifespan(res, pool) {
  var lifespan = pool.getLifespanStats ? pool.getLifespanStats() : {};
  return jsonResponse(res, 200, { lifespan: lifespan });
}

/**
 * 导入账号 — 批量添加
 */
async function handleImportAccounts(req, res, pool, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  // 支持 { accounts: [...] } 或直接 [...]
  var list = Array.isArray(body) ? body : (body.accounts || []);

  if (!Array.isArray(list) || list.length === 0) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  var imported = 0;
  var errors = [];

  for (var i = 0; i < list.length; i++) {
    try {
      pool.addAccount(list[i]);
      imported++;
    } catch (e) {
      errors.push({ index: i, email: list[i].email || '', error: e.message });
    }
  }

  log('📥', C.green, t('admin.import_success', { count: imported }));
  logCollector.add('info', t('admin.import_success', { count: imported }));
  return jsonResponse(res, 200, { imported: imported, errors: errors });
}

/**
 * 删除账号 — 彻底从池中移除（需管理员密码验证）
 */
async function handleDeleteAccount(req, res, pool, email, config, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    body = {};
  }

  // 校验管理员密码
  var expectedPassword = (config.server && config.server.admin_password) || '';
  if (!expectedPassword || body.admin_password !== expectedPassword) {
    return jsonResponse(res, 403, { error: t('admin.password_wrong') });
  }

  var removed = pool.removeAccount(email);
  if (!removed) {
    return jsonResponse(res, 404, { error: t('admin.account_not_found', { email: email }) });
  }
  log('🗑️', C.yellow, t('accounts.deleted', { email: email }));
  logCollector.add('info', t('accounts.deleted', { email: email }));
  return jsonResponse(res, 200, { message: t('accounts.deleted', { email: email }), email: email });
}

/**
 * 导出账号 — 完整数据（含完整 token）
 */
function handleExportAccounts(res, pool) {
  var data = pool.exportAccounts();
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Disposition': 'attachment; filename="accounts-export.json"',
  });
  res.end(JSON.stringify(data, null, 2));
}

function handleListApiKeys(res, config) {
  var list = ensureApiKeyStore(config);
  var items = list.map(function (item) { return sanitizeApiKeyItem(item); });
  items.sort(function (a, b) {
    return String(a.id).localeCompare(String(b.id));
  });
  return jsonResponse(res, 200, { api_keys: items });
}

async function handleCreateApiKey(req, res, config, configPath, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.api_key_invalid_body') });
  }
  body = body && typeof body === 'object' && !Array.isArray(body) ? body : {};

  var id = String(body.id || '').trim();
  if (!isValidApiKeyId(id)) {
    return jsonResponse(res, 400, { error: t('admin.api_key_invalid_id') });
  }
  var identityCheck = validateIdentityValue(body.identity === undefined ? id : body.identity);
  if (!identityCheck.ok) {
    return jsonResponse(res, 400, { error: identityCheck.reason });
  }
  var identity = identityCheck.value;
  var enabled = body.enabled !== false;
  var key = body.key ? String(body.key).trim() : crypto.randomBytes(32).toString('hex');
  if (!key) {
    return jsonResponse(res, 400, { error: t('admin.api_key_invalid_key') });
  }

  var list = ensureApiKeyStore(config);
  for (var i = 0; i < list.length; i++) {
    if (String(list[i].id || '') === id) {
      return jsonResponse(res, 409, { error: t('admin.api_key_exists') });
    }
  }

  var now = new Date().toISOString();
  var item = {
    id: id,
    identity: identity,
    key: key,
    enabled: enabled,
    created_at: now,
  };
  list.push(item);

  try {
    persistConfig(config, configPath);
  } catch (e2) {
    return jsonResponse(res, 500, { error: t('admin.config_update_failed', { reason: e2.message }) });
  }

  logCollector.add('info', t('admin.api_key_created', { id: id }), { id: id, identity: identity });
  return jsonResponse(res, 201, {
    success: true,
    api_key: {
      id: id,
      identity: identity,
      key: key,
      enabled: enabled,
      created_at: now,
    },
  });
}

async function handleUpdateApiKey(req, res, config, configPath, apiKeyId, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.api_key_invalid_body') });
  }
  body = body && typeof body === 'object' && !Array.isArray(body) ? body : {};

  var list = ensureApiKeyStore(config);
  var idx = -1;
  for (var i = 0; i < list.length; i++) {
    if (String(list[i].id || '') === apiKeyId) {
      idx = i;
      break;
    }
  }
  if (idx < 0) {
    return jsonResponse(res, 404, { error: t('admin.api_key_not_found', { id: apiKeyId }) });
  }

  var item = list[idx];
  if (body.id !== undefined) {
    var newId = String(body.id || '').trim();
    if (!isValidApiKeyId(newId)) {
      return jsonResponse(res, 400, { error: t('admin.api_key_invalid_id') });
    }
    if (newId !== apiKeyId) {
      for (var j = 0; j < list.length; j++) {
        if (j === idx) continue;
        if (String(list[j].id || '') === newId) {
          return jsonResponse(res, 409, { error: t('admin.api_key_exists') });
        }
      }
      item.id = newId;
    }
  }
  if (body.identity !== undefined) {
    var identityCheck = validateIdentityValue(body.identity);
    if (!identityCheck.ok) {
      return jsonResponse(res, 400, { error: identityCheck.reason });
    }
    item.identity = identityCheck.value;
  }
  if (body.enabled !== undefined) {
    if (typeof body.enabled !== 'boolean') {
      return jsonResponse(res, 400, { error: t('admin.api_key_invalid_enabled') });
    }
    item.enabled = body.enabled;
  }
  item.updated_at = new Date().toISOString();

  try {
    persistConfig(config, configPath);
  } catch (e2) {
    return jsonResponse(res, 500, { error: t('admin.config_update_failed', { reason: e2.message }) });
  }

  var itemId = String(item.id || apiKeyId);
  logCollector.add('info', t('admin.api_key_updated', { id: itemId }), { id: itemId, identity: item.identity || '' });
  return jsonResponse(res, 200, { success: true, api_key: sanitizeApiKeyItem(item) });
}

function handleDeleteApiKey(res, config, configPath, apiKeyId, t) {
  var list = ensureApiKeyStore(config);
  var idx = -1;
  for (var i = 0; i < list.length; i++) {
    if (String(list[i].id || '') === apiKeyId) {
      idx = i;
      break;
    }
  }
  if (idx < 0) {
    return jsonResponse(res, 404, { error: t('admin.api_key_not_found', { id: apiKeyId }) });
  }

  var removed = list[idx];
  list.splice(idx, 1);

  try {
    persistConfig(config, configPath);
  } catch (e2) {
    return jsonResponse(res, 500, { error: t('admin.config_update_failed', { reason: e2.message }) });
  }

  logCollector.add('info', t('admin.api_key_deleted', { id: apiKeyId }), { id: apiKeyId });
  return jsonResponse(res, 200, {
    success: true,
    deleted: sanitizeApiKeyItem(removed),
  });
}

function handleRotateApiKey(res, config, configPath, apiKeyId, t) {
  var list = ensureApiKeyStore(config);
  var item = null;
  for (var i = 0; i < list.length; i++) {
    if (String(list[i].id || '') === apiKeyId) {
      item = list[i];
      break;
    }
  }
  if (!item) {
    return jsonResponse(res, 404, { error: t('admin.api_key_not_found', { id: apiKeyId }) });
  }

  var newKey = crypto.randomBytes(32).toString('hex');
  var now = new Date().toISOString();
  item.key = newKey;
  item.updated_at = now;
  item.rotated_at = now;

  try {
    persistConfig(config, configPath);
  } catch (e2) {
    return jsonResponse(res, 500, { error: t('admin.config_update_failed', { reason: e2.message }) });
  }

  logCollector.add('info', t('admin.api_key_rotated', { id: apiKeyId }), { id: apiKeyId });
  return jsonResponse(res, 200, {
    success: true,
    api_key: {
      id: String(item.id || ''),
      identity: item.identity ? String(item.identity) : '',
      key: newKey,
      enabled: item.enabled !== false,
      created_at: item.created_at || null,
      updated_at: item.updated_at || null,
      rotated_at: item.rotated_at || null,
    },
  });
}

/**
 * 账号操作 — refresh / cooldown / waste / activate
 */
async function handleAccountAction(req, res, pool, email, t, config) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }

  var action = body.action || '';
  var validActions = ['refresh', 'cooldown', 'waste', 'activate', 'verify', 'test'];

  if (validActions.indexOf(action) < 0) {
    return jsonResponse(res, 400, { error: t('admin.invalid_action', { action: action }) });
  }

  // 查找账号 — listAccounts 包含活跃 + 废弃
  var accounts = pool.listAccounts();
  var foundAccount = null;
  for (var i = 0; i < accounts.length; i++) {
    if (accounts[i].email === email) {
      foundAccount = accounts[i];
      break;
    }
  }

  if (!foundAccount) {
    return jsonResponse(res, 404, { error: t('admin.account_not_found', { email: email }) });
  }

  switch (action) {
    case 'refresh': {
      // 真正执行 token 刷新（而非仅标记 expired）
      var fullAccount = pool.getFullAccount(email);
      if (!fullAccount) {
        return jsonResponse(res, 404, { error: t('admin.account_not_found', { email: email }) });
      }
      if (!fullAccount.sessionToken) {
        return jsonResponse(res, 400, { error: '该账号无 sessionToken，无法刷新' });
      }
      if (!pool.lockAccount(email)) {
        return jsonResponse(res, 409, { ok: false, error: '该账号正在执行刷新，请稍后重试' });
      }
      try {
        var refreshTokenVersion = typeof fullAccount._tokenVersion === 'number' ? fullAccount._tokenVersion : 0;
        var refreshResult = await refreshAccountToken(fullAccount);
        if (refreshResult.success) {
          var refreshCas = pool.applyRefreshResultCAS(fullAccount, refreshResult, refreshTokenVersion);
          if (!refreshCas.applied && refreshCas.reason === 'stale_version') {
            log('ℹ️', C.cyan, '管理面板刷新跳过（版本落后）: ' + email);
            return jsonResponse(res, 200, { ok: true, message: '账号已被其他刷新任务更新', refreshed: true, skipped: true });
          }
          if (!refreshCas.applied) {
            log('❌', C.red, '管理面板刷新写回失败: ' + email + ' (' + (refreshCas.reason || 'unknown') + ')');
            return jsonResponse(res, 500, { ok: false, error: '刷新写回失败' });
          }
          pool.forceSave();
          log('✅', C.green, '管理面板刷新成功: ' + email);
          return jsonResponse(res, 200, { ok: true, message: t('admin.action_success', { action: 'refresh', email: email }), refreshed: true });
        } else {
          var failCode = (refreshResult && refreshResult.statusCode) || 0;
          var failDetail = (refreshResult && (refreshResult.detail || refreshResult.error)) || 'unknown';
          pool.markError(email, failCode, failDetail);
          log('❌', C.red, '管理面板刷新失败: ' + email + ' (' + failDetail + ')');
          return jsonResponse(res, 200, { ok: false, message: '刷新失败: ' + failDetail, refreshed: false });
        }
      } finally {
        pool.unlockAccount(email);
      }
    }

    case 'cooldown':
      // 手动冷却
      pool.markError(email, 429, '');
      break;

    case 'waste':
      // 标记废弃
      pool.markWasted(email);
      break;

    case 'activate':
      // 重新激活 — 先尝试从废弃池恢复，否则直接更新状态
      if (foundAccount.status === 'wasted') {
        pool.activateWasted(email);
      } else {
        pool.updateToken(email, null, null);
      }
      break;

    case 'verify': {
      // 验证失效账号：先激活 → 尝试 refreshToken → 成功保留 / 失败直接废弃
      var wasWasted = foundAccount.status === 'wasted';
      if (!pool.lockAccount(email)) {
        return jsonResponse(res, 409, { ok: false, error: '该账号正在执行刷新，请稍后重试' });
      }
      try {
        if (wasWasted) {
          pool.activateWasted(email);
        }
        var verifyAccount = pool.getFullAccount(email);
        if (!verifyAccount || !verifyAccount.sessionToken) {
          if (verifyAccount) {
            verifyAccount.last_error_type = 'no_session_token';
            verifyAccount.last_error_code = 401;
            pool.markWasted(email);
            pool.forceSave();
          }
          return jsonResponse(res, 400, { error: '该账号无 sessionToken，无法验证' });
        }
        var verifyTokenVersion = typeof verifyAccount._tokenVersion === 'number' ? verifyAccount._tokenVersion : 0;
        var verifyResult = await refreshAccountToken(verifyAccount);
        if (verifyResult.success) {
          var verifyCas = pool.applyRefreshResultCAS(verifyAccount, verifyResult, verifyTokenVersion);
          if (!verifyCas.applied && verifyCas.reason === 'stale_version') {
            log('ℹ️', C.cyan, '账号验证跳过（版本落后）: ' + email);
            return jsonResponse(res, 200, { success: true, verified: true, skipped: true, message: '账号已被其他刷新任务更新' });
          }
          if (!verifyCas.applied) {
            log('❌', C.red, '账号验证写回失败: ' + email + ' (' + (verifyCas.reason || 'unknown') + ')');
            return jsonResponse(res, 500, { success: false, error: '验证写回失败' });
          }
          pool.forceSave();
          log('✅', C.green, '账号验证成功: ' + email);
          return jsonResponse(res, 200, { success: true, verified: true, message: '账号有效，已恢复为活跃状态' });
        } else {
          var verifyAccountToUpdate = pool.getFullAccount(email);
          if (verifyAccountToUpdate) {
            verifyAccountToUpdate.last_error_type = 'verify_failed';
            verifyAccountToUpdate.last_error_code = (verifyResult && verifyResult.statusCode) || 401;
            pool.markWasted(email);
            pool.forceSave();
          }
          var verifyDetail = (verifyResult && (verifyResult.detail || verifyResult.error)) || 'unknown';
          log('❌', C.red, '账号验证失败: ' + email + ' (' + verifyDetail + ')');
          return jsonResponse(res, 200, { success: true, verified: false, message: '账号验证失败，已标记废弃: ' + verifyDetail });
        }
      } finally {
        pool.unlockAccount(email);
      }
    }

    case 'test': {
      var testAccount = pool.getFullAccount(email);
      if (!testAccount || !testAccount.accessToken) {
        return jsonResponse(res, 400, { ok: false, error: '该账号无 accessToken' });
      }
      var testModel = (config.models && config.models.default) || 'gpt-5.3-codex';
      var result = await testOneAccount(testAccount, testModel);
      if (result.ok) {
        log('✅', C.green, '账号测试成功: ' + email + ' (' + result.latency + 'ms)');
        return jsonResponse(res, 200, { ok: true, message: '测试成功', latency: result.latency, status: result.status });
      } else {
        log('❌', C.red, '账号测试失败: ' + email + ' ' + (result.status || '') + ': ' + (result.error || ''));
        return jsonResponse(res, 200, {
          ok: false,
          message: result.networkError ? '测试异常: ' + result.error : '测试失败: HTTP ' + result.status,
          latency: result.latency,
          status: result.status,
          error: result.error,
        });
      }
    }
  }

  log('🔧', C.cyan, t('admin.action_success', { action: action, email: email }));
  logCollector.add('info', t('admin.action_success', { action: action, email: email }));

  // 获取更新后的账号信息
  var updatedAccounts = pool.listAccounts();
  var updatedAccount = null;
  for (var j = 0; j < updatedAccounts.length; j++) {
    if (updatedAccounts[j].email === email) {
      updatedAccount = updatedAccounts[j];
      break;
    }
  }

  return jsonResponse(res, 200, {
    success: true,
    account: updatedAccount || { email: email, status: action === 'waste' ? 'wasted' : 'unknown' },
  });
}

function ensureProxyConfig(config) {
  if (!config.proxy || typeof config.proxy !== 'object') {
    config.proxy = {};
  }
  var proxy = config.proxy;
  if (!proxy.host) proxy.host = '127.0.0.1';
  if (!proxy.presets || typeof proxy.presets !== 'object') proxy.presets = {};
  if (!Array.isArray(proxy.node_groups)) proxy.node_groups = [];
  if (proxy.active_preset === undefined || proxy.active_preset === null) {
    proxy.active_preset = '';
  } else {
    proxy.active_preset = String(proxy.active_preset);
  }
  ensureRegisterProxyConfig(proxy);
  return proxy;
}

var REGISTER_PROXY_SOCKS_HOST = '127.0.0.1';
var REGISTER_PROXY_SOCKS_USERNAME = process.env.REGISTER_PROXY_SOCKS_USERNAME || '';
var REGISTER_PROXY_SOCKS_PASSWORD = process.env.REGISTER_PROXY_SOCKS_PASSWORD || '';
var REGISTER_PROXY_DEFAULT_PORT = 7860;
var REGISTER_PROXY_PRESET_TARGET_MAP = {
  all: 'all',
  us: 'us',
  asia: 'asia',
  japan: 'jp',
  jp: 'jp',
  europe: 'eu',
  eu: 'eu',
  vless_cf: '自建',
  ikuuu: 'ikuuu',
  mojie: '魔戒',
};
var REGISTER_PROXY_DEFAULT_SERVER = buildRegisterPoolServerUrl('all');

function buildRegisterSocksUrl(port) {
  var safePort = parseInt(port, 10);
  if (!safePort || safePort < 1 || safePort > 65535) safePort = REGISTER_PROXY_DEFAULT_PORT;
  var authPart = '';
  if (REGISTER_PROXY_SOCKS_USERNAME || REGISTER_PROXY_SOCKS_PASSWORD) {
    authPart = encodeURIComponent(REGISTER_PROXY_SOCKS_USERNAME) + ':' +
      encodeURIComponent(REGISTER_PROXY_SOCKS_PASSWORD) + '@';
  }
  return 'socks5://' + authPart + REGISTER_PROXY_SOCKS_HOST + ':' + safePort;
}

function resolvePresetPort(proxy, presetKey) {
  if (!proxy || !proxy.presets || typeof proxy.presets !== 'object') return 0;
  var preset = proxy.presets[presetKey];
  if (!preset) return 0;
  return parseInt(preset.port, 10) || 0;
}

function resolveNodePort(proxy, nodeName) {
  if (!proxy || !Array.isArray(proxy.node_groups) || !nodeName) return 0;
  for (var i = 0; i < proxy.node_groups.length; i++) {
    var group = proxy.node_groups[i];
    if (!group || !Array.isArray(group.nodes)) continue;
    for (var j = 0; j < group.nodes.length; j++) {
      var node = group.nodes[j];
      if (!node) continue;
      if (String(node.name || '').trim() === String(nodeName).trim()) {
        return parseInt(node.port, 10) || 0;
      }
    }
  }
  return 0;
}

function findPresetKeyByMappedTarget(mappedTarget) {
  if (!mappedTarget) return '';
  var keys = Object.keys(REGISTER_PROXY_PRESET_TARGET_MAP);
  for (var i = 0; i < keys.length; i++) {
    if (REGISTER_PROXY_PRESET_TARGET_MAP[keys[i]] === mappedTarget) return keys[i];
  }
  return '';
}

function buildRegisterPoolServerUrl(target, proxy) {
  var safeTarget = target === undefined || target === null ? 'all' : String(target).trim();
  if (!safeTarget) safeTarget = 'all';
  if (proxy) {
    // 直接用 target 作为 preset key 查端口
    var presetPort = resolvePresetPort(proxy, safeTarget);
    if (presetPort) return buildRegisterSocksUrl(presetPort);
    // target 可能是映射后的值（如 'jp', 'eu', '自建', '魔戒'），反查 preset key
    var originalKey = findPresetKeyByMappedTarget(safeTarget);
    if (originalKey) {
      var mappedPort = resolvePresetPort(proxy, originalKey);
      if (mappedPort) return buildRegisterSocksUrl(mappedPort);
    }
    // 用节点名查端口
    var nodePort = resolveNodePort(proxy, safeTarget);
    if (nodePort) return buildRegisterSocksUrl(nodePort);
  }
  return buildRegisterSocksUrl(REGISTER_PROXY_DEFAULT_PORT);
}

function resolveRegisterPresetTarget(presetKey) {
  var key = presetKey === undefined || presetKey === null ? '' : String(presetKey).trim();
  if (!key) return '';
  var mapped = REGISTER_PROXY_PRESET_TARGET_MAP[key.toLowerCase()];
  return mapped || key;
}

function parseRegisterTargetFromServer(server) {
  if (!server) return '';
  var raw = String(server).trim();
  if (!raw) return '';
  // SOCKS5 URL: 从端口反查 target（preset key 或 node name）
  if (/^socks5:\/\//i.test(raw)) {
    return '';
  }
  // Legacy PP HTTP relay URL fallback
  var marker = '/fetch/';
  var idx = raw.indexOf(marker);
  if (idx < 0) return '';
  var tail = raw.slice(idx + marker.length);
  var slashPos = tail.indexOf('/');
  if (slashPos >= 0) tail = tail.slice(0, slashPos);
  var queryPos = tail.indexOf('?');
  if (queryPos >= 0) tail = tail.slice(0, queryPos);
  if (!tail) return '';
  try {
    return decodeURIComponent(tail);
  } catch (_) {
    return tail;
  }
}

function findNodeNameByPort(proxy, port) {
  if (!proxy || !Array.isArray(proxy.node_groups)) return '';
  for (var i = 0; i < proxy.node_groups.length; i++) {
    var group = proxy.node_groups[i];
    if (!group || !Array.isArray(group.nodes)) continue;
    for (var j = 0; j < group.nodes.length; j++) {
      var node = group.nodes[j];
      if (!node) continue;
      var nodePort = parseInt(node.port, 10);
      if (nodePort === port && node.name !== undefined && node.name !== null) {
        var nodeName = String(node.name).trim();
        if (nodeName) return nodeName;
      }
    }
  }
  return '';
}

function parseProxyPortFromServer(server) {
  if (!server) return 0;
  var raw = String(server).trim();
  if (!raw) return 0;
  try {
    var parsed = new URL(raw);
    if (parsed.port) return parseInt(parsed.port, 10) || 0;
  } catch (_) {
    // ignore
  }
  var m = raw.match(/:(\d+)(?:\/|$)/);
  return m ? (parseInt(m[1], 10) || 0) : 0;
}

function findPresetTargetByPort(proxy, port) {
  if (!proxy || !proxy.presets || typeof proxy.presets !== 'object') return '';
  var keys = Object.keys(proxy.presets);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var preset = proxy.presets[key];
    if (!preset) continue;
    var presetPort = parseInt(preset.port, 10);
    if (presetPort !== port) continue;
    var mapped = resolveRegisterPresetTarget(key);
    if (mapped) return mapped;
  }
  return '';
}

function resolveRegisterTargetByPort(proxy, port) {
  if (!port || port < 1 || port > 65535) return '';
  var presetTarget = findPresetTargetByPort(proxy, port);
  if (presetTarget) return presetTarget;
  return findNodeNameByPort(proxy, port);
}

function resolveLocalProxyTarget(proxy) {
  if (!proxy || typeof proxy !== 'object') return '';
  if (proxy.active_preset) {
    var presetTarget = resolveRegisterPresetTarget(proxy.active_preset);
    if (presetTarget) return presetTarget;
  }
  var localPort = parseProxyPortFromServer(proxy.server || '');
  if (localPort) {
    var byPort = resolveRegisterTargetByPort(proxy, localPort);
    if (byPort) return byPort;
  }
  return '';
}

function resolveRegisterProxyTarget(proxy, registerProxy) {
  if (registerProxy && registerProxy.active_preset) {
    var presetTarget = resolveRegisterPresetTarget(registerProxy.active_preset);
    if (presetTarget) return presetTarget;
  }
  var fromServer = parseRegisterTargetFromServer(registerProxy && registerProxy.server ? registerProxy.server : '');
  if (fromServer) return fromServer;
  var registerPort = parseProxyPortFromServer(registerProxy && registerProxy.server ? registerProxy.server : '');
  if (registerPort) {
    var byRegisterPort = resolveRegisterTargetByPort(proxy, registerPort);
    if (byRegisterPort) return byRegisterPort;
  }
  var fromLocal = resolveLocalProxyTarget(proxy);
  if (fromLocal) return fromLocal;
  return 'all';
}

function buildRegisterProxyForwardPayload(proxy, registerProxy) {
  var target = resolveRegisterProxyTarget(proxy, registerProxy);
  var server = buildRegisterPoolServerUrl(target, proxy);
  return {
    enabled: !!(registerProxy && registerProxy.enabled),
    server: server,
    active_preset: registerProxy && registerProxy.active_preset ? String(registerProxy.active_preset) : '',
    target: target,
  };
}

function ensureRegisterProxyConfig(proxy) {
  if (!proxy.register_proxy || typeof proxy.register_proxy !== 'object' || Array.isArray(proxy.register_proxy)) {
    proxy.register_proxy = {};
  }
  var registerProxy = proxy.register_proxy;
  if (typeof registerProxy.enabled !== 'boolean') registerProxy.enabled = false;
  if (registerProxy.active_preset === undefined || registerProxy.active_preset === null) {
    registerProxy.active_preset = '';
  } else {
    registerProxy.active_preset = String(registerProxy.active_preset);
  }
  // 如果 server 已经是合法的 SOCKS5 URL，保留不动
  if (registerProxy.server && /^socks5:\/\//i.test(String(registerProxy.server).trim())) {
    return registerProxy;
  }
  // 否则迁移到 SOCKS5 URL
  var migrateTarget = resolveRegisterProxyTarget(proxy, registerProxy);
  if (!migrateTarget) migrateTarget = 'all';
  registerProxy.server = buildRegisterPoolServerUrl(migrateTarget, proxy);
  return registerProxy;
}

function buildProxyServerUrl(proxy, port) {
  var host = proxy.host || '127.0.0.1';
  return 'socks5://' + host + ':' + port;
}

function formatProxyConfig(proxy) {
  var registerProxy = ensureRegisterProxyConfigFromRegisterClient(proxy);
  var registerTarget = resolveRegisterProxyTarget(proxy, registerProxy);
  var registerLocalPort = 0;
  if (registerTarget) {
    var keys = Object.keys(proxy.presets || {});
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var mapped = resolveRegisterPresetTarget(key);
      if (mapped !== registerTarget) continue;
      var preset = proxy.presets[key];
      var p = preset ? parseInt(preset.port, 10) : 0;
      if (p > 0) {
        registerLocalPort = p;
        break;
      }
    }
    if (!registerLocalPort && Array.isArray(proxy.node_groups)) {
      for (var gi = 0; gi < proxy.node_groups.length; gi++) {
        var group = proxy.node_groups[gi];
        if (!group || !Array.isArray(group.nodes)) continue;
        for (var ni = 0; ni < group.nodes.length; ni++) {
          var node = group.nodes[ni];
          if (!node || node.name === undefined || node.name === null) continue;
          if (String(node.name).trim() !== registerTarget) continue;
          var np = parseInt(node.port, 10);
          if (np > 0) {
            registerLocalPort = np;
            break;
          }
        }
        if (registerLocalPort) break;
      }
    }
  }
  return {
    enabled: !!proxy.enabled,
    server: proxy.server || '',
    current_server: proxy.server || '',
    username: proxy.username || '',
    password: '',
    password_masked: proxy.password ? '***' : '',
    active_preset: proxy.active_preset ? String(proxy.active_preset) : '',
    presets: proxy.presets || {},
    node_groups: proxy.node_groups || [],
    host: proxy.host || '127.0.0.1',
    register_proxy: {
      enabled: !!registerProxy.enabled,
      server: registerProxy.server || buildRegisterSocksUrl(REGISTER_PROXY_DEFAULT_PORT),
      active_preset: registerProxy.active_preset ? String(registerProxy.active_preset) : '',
      target: registerTarget || '',
      local_port: registerLocalPort,
    },
  };
}

function handleProxyPresets(res, config) {
  var proxy = ensureProxyConfigFromRegisterClient(config);
  return jsonResponse(res, 200, formatProxyConfig(proxy));
}

async function handleProxySelect(req, res, config, configPath) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: 'Invalid request body' });
  }

  body = body || {};
  var proxy = ensureProxyConfigFromRegisterClient(config);
  var nextProxy = JSON.parse(JSON.stringify(proxy));
  var registerProxy = ensureRegisterProxyConfigFromRegisterClient(nextProxy);

  var hasPreset = body.preset !== undefined && body.preset !== null && body.preset !== '';
  var hasPort = body.port !== undefined && body.port !== null && body.port !== '';
  var hasServer = body.server !== undefined && body.server !== null && String(body.server).trim() !== '';
  var hasEnabled = body.enabled !== undefined;
  var hasLocalUpdate = hasPreset || hasPort || hasServer || hasEnabled;
  var hasRegisterPayload = body.register_proxy !== undefined;

  if (hasRegisterPayload && (!body.register_proxy || typeof body.register_proxy !== 'object' || Array.isArray(body.register_proxy))) {
    return jsonResponse(res, 400, { error: 'Invalid register_proxy payload' });
  }
  if (hasEnabled && typeof body.enabled !== 'boolean') {
    return jsonResponse(res, 400, { error: 'Invalid enabled type (boolean expected)' });
  }

  if (hasEnabled && body.enabled === false) {
    nextProxy.enabled = false;
  } else if (hasPreset) {
    var presetKey = String(body.preset);
    var preset = nextProxy.presets[presetKey];
    if (!preset || preset.port === undefined || preset.port === null) {
      return jsonResponse(res, 400, { error: 'Invalid preset' });
    }
    var presetPort = parseInt(preset.port, 10);
    if (!presetPort || presetPort < 1 || presetPort > 65535) {
      return jsonResponse(res, 400, { error: 'Invalid preset port' });
    }
    nextProxy.server = buildProxyServerUrl(nextProxy, presetPort);
    nextProxy.active_preset = presetKey;
    nextProxy.enabled = true;
  } else if (hasPort) {
    var port = parseInt(body.port, 10);
    if (!port || port < 1 || port > 65535) {
      return jsonResponse(res, 400, { error: 'Invalid port' });
    }
    nextProxy.server = buildProxyServerUrl(nextProxy, port);
    nextProxy.active_preset = '';
    nextProxy.enabled = true;
  } else if (hasServer) {
    nextProxy.server = String(body.server).trim();
    nextProxy.active_preset = '';
    nextProxy.enabled = true;
  } else if (hasEnabled) {
    nextProxy.enabled = body.enabled;
  }

  var registerUpdated = false;
  if (hasRegisterPayload) {
    var registerBody = body.register_proxy;
    var hasRegisterPreset = registerBody.preset !== undefined && registerBody.preset !== null && registerBody.preset !== '';
    var hasRegisterPort = registerBody.port !== undefined && registerBody.port !== null && registerBody.port !== '';
    var hasRegisterServer = registerBody.server !== undefined && registerBody.server !== null && String(registerBody.server).trim() !== '';
    var hasRegisterEnabled = registerBody.enabled !== undefined;
    if (hasRegisterEnabled && typeof registerBody.enabled !== 'boolean') {
      return jsonResponse(res, 400, { error: 'Invalid register_proxy.enabled type (boolean expected)' });
    }

    if (hasRegisterPreset) {
      var registerPresetKey = String(registerBody.preset);
      var registerPreset = nextProxy.presets[registerPresetKey];
      if (!registerPreset) {
        return jsonResponse(res, 400, { error: 'Invalid register_proxy.preset' });
      }
      var registerPresetPort = parseInt(registerPreset.port, 10);
      if (!registerPresetPort || registerPresetPort < 1 || registerPresetPort > 65535) {
        return jsonResponse(res, 400, { error: 'Invalid register_proxy.preset port' });
      }
      registerProxy.server = buildRegisterSocksUrl(registerPresetPort);
      registerProxy.active_preset = registerPresetKey;
      registerProxy.enabled = true;
      registerUpdated = true;
    } else if (hasRegisterPort) {
      var registerPort = parseInt(registerBody.port, 10);
      if (!registerPort || registerPort < 1 || registerPort > 65535) {
        return jsonResponse(res, 400, { error: 'Invalid register_proxy.port' });
      }
      var registerNodeTarget = findNodeNameByPort(nextProxy, registerPort);
      if (!registerNodeTarget) {
        return jsonResponse(res, 400, { error: 'Invalid register_proxy.port target' });
      }
      registerProxy.server = buildRegisterSocksUrl(registerPort);
      registerProxy.active_preset = '';
      registerProxy.enabled = true;
      registerUpdated = true;
    } else if (hasRegisterServer) {
      var registerServer = String(registerBody.server).trim();
      if (!/^socks5:\/\//i.test(registerServer)) {
        return jsonResponse(res, 400, { error: 'Invalid register_proxy.server (expected socks5:// URL)' });
      }
      registerProxy.server = registerServer;
      registerProxy.active_preset = '';
      registerProxy.enabled = true;
      registerUpdated = true;
    } else if (hasRegisterEnabled) {
      registerProxy.enabled = registerBody.enabled;
      registerUpdated = true;
    }
  }

  if (!hasLocalUpdate && !hasRegisterPayload) {
    return jsonResponse(res, 400, { error: 'Missing preset/port/server/enabled/register_proxy' });
  }

  if (hasRegisterPayload && !hasLocalUpdate && !registerUpdated) {
    return jsonResponse(res, 400, { error: 'Missing register_proxy fields' });
  }

  config.proxy = nextProxy;

  try {
    writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
  } catch (e) {
    return jsonResponse(res, 500, { error: 'Failed to persist proxy config: ' + e.message });
  }

  // 通知注册机更新 register_proxy 配置
  var registerSyncResult = null;
  if (registerUpdated) {
    var registerCfg = config.register || {};
    var registerApiUrl = registerCfg.api_url || '';
    var registerApiToken = registerCfg.api_token || '';
    if (registerApiUrl && registerApiToken) {
      var syncUrl = registerApiUrl.replace(/\/+$/, '') + '/api/proxy-config';
      var syncBody = JSON.stringify({
        server: registerProxy.server,
        enabled: !!registerProxy.enabled,
      });
      try {
        var syncResp = await fetch(syncUrl, {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer ' + registerApiToken,
            'Content-Type': 'application/json',
          },
          body: syncBody,
          signal: AbortSignal.timeout(10000),
        });
        var syncText = await syncResp.text();
        registerSyncResult = {
          ok: syncResp.ok,
          status: syncResp.status,
          body: syncText.length > 500 ? syncText.slice(0, 500) : syncText,
        };
      } catch (syncErr) {
        registerSyncResult = {
          ok: false,
          error: syncErr.message || String(syncErr),
        };
      }
    }
  }

  return jsonResponse(res, 200, {
    proxy: formatProxyConfig(nextProxy),
    updated: {
      local_proxy: hasLocalUpdate,
      register_proxy: registerUpdated,
    },
    register_sync: registerSyncResult,
  });
}

function handleProxyTest(req, res, config) {
  var query = parseQuery(req.url);
  var port = parseInt(query.port, 10);

  if (!port || port < 1 || port > 65535) {
    return jsonResponse(res, 400, { error: 'Invalid port (1-65535)' });
  }

  var proxy = ensureProxyConfigFromRegisterClient(config);
  var host = proxy.host || '127.0.0.1';
  var user = proxy.username || '';
  var pass = proxy.password || '';

  var authPart = '';
  if (user || pass) {
    authPart = encodeURIComponent(user) + ':' + encodeURIComponent(pass) + '@';
  }
  var proxyUrl = 'socks5h://' + authPart + host + ':' + port;

  execFile(
    'curl',
    ['-x', proxyUrl, '-s', '-m', '10', 'https://api.ipify.org'],
    { timeout: 15000 },
    function (err, stdout, stderr) {
      if (err) {
        var errorText = (stderr && String(stderr).trim()) || err.message || 'Proxy test failed';
        return jsonResponse(res, 200, { success: false, error: errorText });
      }
      var ip = (stdout || '').trim();
      if (!ip) {
        return jsonResponse(res, 200, { success: false, error: 'Empty IP response' });
      }
      return jsonResponse(res, 200, { success: true, ip: ip });
    }
  );
}

var MODEL_NAME_RE = /^[A-Za-z0-9._:-]{1,128}$/;

function isValidModelName(name) {
  return typeof name === 'string' && MODEL_NAME_RE.test(name);
}

function ensureModelsConfig(config) {
  if (!config.models || typeof config.models !== 'object' || Array.isArray(config.models)) {
    config.models = {};
  }
  if (!config.models.available || typeof config.models.available !== 'object' || Array.isArray(config.models.available)) {
    config.models.available = {};
  }
  if (!config.models.aliases || typeof config.models.aliases !== 'object' || Array.isArray(config.models.aliases)) {
    config.models.aliases = {};
  }
  if (typeof config.models.prefix !== 'string') {
    config.models.prefix = '';
  }
  if (typeof config.models.default !== 'string') {
    var firstModel = Object.keys(config.models.available)[0] || 'gpt-5-codex-mini';
    config.models.default = firstModel;
  }
  return config.models;
}

function toModelAdminShape(modelsConfig) {
  var models = ensureModelsConfig({ models: JSON.parse(JSON.stringify(modelsConfig || {})) });
  var availableObj = models.available || {};
  var aliasesObj = models.aliases || {};

  var available = [];
  var modelNames = Object.keys(availableObj);
  for (var i = 0; i < modelNames.length; i++) {
    var name = modelNames[i];
    var info = availableObj[name] || {};
    available.push({
      name: name,
      display_name: typeof info.display_name === 'string' && info.display_name.trim()
        ? info.display_name.trim()
        : name,
      enabled: info.enabled !== false,
    });
  }

  var aliases = [];
  var aliasNames = Object.keys(aliasesObj);
  for (var j = 0; j < aliasNames.length; j++) {
    var alias = aliasNames[j];
    aliases.push({
      alias: alias,
      target: String(aliasesObj[alias] || '').trim(),
    });
  }

  var defaultModel = String(models.default || '').trim();
  if (!defaultModel || !availableObj[defaultModel] || availableObj[defaultModel].enabled === false) {
    for (var k = 0; k < available.length; k++) {
      if (available[k].enabled) {
        defaultModel = available[k].name;
        break;
      }
    }
    if (!defaultModel && available.length > 0) {
      defaultModel = available[0].name;
    }
  }

  return {
    prefix: typeof models.prefix === 'string' ? models.prefix : '',
    default: defaultModel,
    available: available,
    aliases: aliases,
  };
}

function validateAndBuildModelsConfig(body, currentModels) {
  var raw = body && typeof body === 'object' ? body : {};
  var current = toModelAdminShape(currentModels);

  var prefix = raw.prefix !== undefined ? String(raw.prefix || '').trim() : current.prefix;
  if (prefix.length > 64) {
    throw new Error('models.prefix_too_long');
  }

  var availableInput = raw.available !== undefined ? raw.available : current.available;
  var availableList = [];

  if (Array.isArray(availableInput)) {
    availableList = availableInput;
  } else if (availableInput && typeof availableInput === 'object') {
    var modelKeys = Object.keys(availableInput);
    for (var i = 0; i < modelKeys.length; i++) {
      var modelName = modelKeys[i];
      var modelInfo = availableInput[modelName] || {};
      availableList.push({
        name: modelName,
        display_name: modelInfo.display_name,
        enabled: modelInfo.enabled,
      });
    }
  } else {
    throw new Error('models.available_invalid');
  }

  var nextAvailable = {};
  for (var j = 0; j < availableList.length; j++) {
    var item = availableList[j] || {};
    var name = String(item.name || '').trim();
    if (!isValidModelName(name)) {
      throw new Error('models.invalid_name:' + name);
    }
    if (nextAvailable[name]) {
      throw new Error('models.duplicate_name:' + name);
    }
    var displayName = String(item.display_name || '').trim();
    nextAvailable[name] = {
      display_name: displayName || name,
      enabled: item.enabled !== false,
    };
  }

  var availableNames = Object.keys(nextAvailable);
  if (availableNames.length === 0) {
    throw new Error('models.available_empty');
  }

  var aliasesInput = raw.aliases !== undefined ? raw.aliases : current.aliases;
  var aliasesList = [];
  if (Array.isArray(aliasesInput)) {
    aliasesList = aliasesInput;
  } else if (aliasesInput && typeof aliasesInput === 'object') {
    var aliasKeys = Object.keys(aliasesInput);
    for (var a = 0; a < aliasKeys.length; a++) {
      var aliasName = aliasKeys[a];
      aliasesList.push({
        alias: aliasName,
        target: aliasesInput[aliasName],
      });
    }
  } else {
    throw new Error('models.aliases_invalid');
  }

  var nextAliases = {};
  for (var x = 0; x < aliasesList.length; x++) {
    var aliasItem = aliasesList[x] || {};
    var alias = String(aliasItem.alias || '').trim();
    var target = String(aliasItem.target || '').trim();
    if (!alias) continue;
    if (!isValidModelName(alias)) {
      throw new Error('models.invalid_alias:' + alias);
    }
    if (!target || !isValidModelName(target)) {
      throw new Error('models.invalid_alias_target:' + alias);
    }
    if (!nextAvailable[target]) {
      throw new Error('models.alias_target_missing:' + alias);
    }
    if (nextAvailable[alias]) {
      throw new Error('models.alias_conflict_with_model:' + alias);
    }
    if (nextAliases[alias]) {
      throw new Error('models.duplicate_alias:' + alias);
    }
    nextAliases[alias] = target;
  }

  var defaultModel = raw.default !== undefined ? String(raw.default || '').trim() : current.default;
  if (!defaultModel || !nextAvailable[defaultModel] || nextAvailable[defaultModel].enabled === false) {
    var fallback = '';
    for (var n = 0; n < availableNames.length; n++) {
      var candidate = availableNames[n];
      if (nextAvailable[candidate].enabled !== false) {
        fallback = candidate;
        break;
      }
    }
    defaultModel = fallback || availableNames[0];
  }

  return {
    prefix: prefix,
    default: defaultModel,
    available: nextAvailable,
    aliases: nextAliases,
  };
}

function handleModelsConfigGet(res, config) {
  var models = ensureModelsConfig(config);
  var discovery = typeof modelMapper.getUpstreamModelsSnapshot === 'function'
    ? modelMapper.getUpstreamModelsSnapshot()
    : null;
  return jsonResponse(res, 200, {
    success: true,
    models: toModelAdminShape(models),
    discovery: discovery,
  });
}

async function handleModelsConfigPut(req, res, config, ctx, configPath, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (_) {
    return jsonResponse(res, 400, { success: false, error: t('admin.no_body') });
  }

  try {
    var nextModels = validateAndBuildModelsConfig(body, config.models || {});
    config.models = nextModels;
    await persistConfigAndHotReload(config, ctx, configPath);

    log('🧩', C.green, '模型配置已更新: models=' + Object.keys(nextModels.available || {}).length + ', aliases=' + Object.keys(nextModels.aliases || {}).length);
    logCollector.add('info', '模型配置已更新', {
      models: Object.keys(nextModels.available || {}).length,
      aliases: Object.keys(nextModels.aliases || {}).length,
      default_model: nextModels.default,
    });

    return jsonResponse(res, 200, {
      success: true,
      models: toModelAdminShape(nextModels),
    });
  } catch (err) {
    return jsonResponse(res, 400, {
      success: false,
      error: t('admin.config_update_failed', { reason: err.message || 'models_update_failed' }),
    });
  }
}

async function handleModelsRefresh(res, ctx, config) {
  if (!ctx || typeof ctx.refreshUpstreamModels !== 'function') {
    return jsonResponse(res, 200, {
      success: false,
      error: 'upstream_model_discovery_not_available',
    });
  }

  try {
    var refreshResult = await ctx.refreshUpstreamModels(true);
    var ok = !!(refreshResult && refreshResult.success);
    var discovery = typeof modelMapper.getUpstreamModelsSnapshot === 'function'
      ? modelMapper.getUpstreamModelsSnapshot()
      : null;
    var models = ensureModelsConfig(config);

    if (ok) {
      logCollector.add('info', '手动刷新上游模型成功', {
        source: discovery ? discovery.source : '',
        codex_count: discovery ? discovery.codex_count : 0,
      });
    } else {
      logCollector.add('warn', '手动刷新上游模型失败', {
        error: refreshResult && refreshResult.error ? String(refreshResult.error) : 'unknown',
        last_status: discovery ? discovery.last_status : 0,
      });
    }

    return jsonResponse(res, 200, {
      success: ok,
      error: ok ? '' : (refreshResult && refreshResult.error ? String(refreshResult.error) : 'upstream_models_refresh_failed'),
      discovery: discovery,
      models: toModelAdminShape(models),
      active_model_ids: modelMapper.listModels({ includePrefix: false }).map(function (item) { return item.id; }),
    });
  } catch (err) {
    return jsonResponse(res, 200, {
      success: false,
      error: err && err.message ? err.message : 'upstream_models_refresh_exception',
    });
  }
}

/**
 * 配置信息 — 递归脱敏
 */
function handleConfig(res, config) {
  var safe = sanitizeConfig(JSON.parse(JSON.stringify(config)));
  return jsonResponse(res, 200, safe);
}

/**
 * 更新配置 — 部分更新（深度合并）
 */
async function handleUpdateConfig(req, res, config, ctx, configPath, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    return jsonResponse(res, 400, { error: t('admin.no_body') });
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return jsonResponse(res, 400, { error: 'invalid_config_payload' });
  }

  var adminPasswordProof = body._admin_password;
  delete body._admin_password;

  var schemaValidation = validateConfigPatchSchema(body, config, '');
  if (!schemaValidation.ok) {
    return jsonResponse(res, 400, {
      error: t('admin.config_update_failed', { reason: schemaValidation.reason }),
    });
  }

  // 安全验证：不允许通过 API 清空 admin_password
  if (body.server && typeof body.server === 'object') {
    if (body.server.admin_password !== undefined && !body.server.admin_password) {
      return jsonResponse(res, 400, {
        error: t('admin.config_update_failed', { reason: 'admin_password cannot be empty' }),
      });
    }
  }

  // 敏感字段变更需要管理员密码验证
  var sensitiveServerFields = [
    'password',
    'admin_password',
    'admin_username',
    'totp_secret',
    'totp_enabled',
    'totp_allow_passwordless',
    'totp_period_seconds',
    'totp_digits',
    'totp_window',
    'totp_issuer',
  ];
  var hasSensitive = false;
  if (body.server && typeof body.server === 'object') {
    for (var sf = 0; sf < sensitiveServerFields.length; sf++) {
      if (body.server[sensitiveServerFields[sf]] !== undefined) {
        hasSensitive = true;
        break;
      }
    }
  }
  if (body.credentials && typeof body.credentials === 'object') {
    if (body.credentials.api_token !== undefined) {
      hasSensitive = true;
    }
  }

  if (hasSensitive) {
    var expectedPassword = (config.server && config.server.admin_password) || '';
    if (!expectedPassword || !safeCompare(String(adminPasswordProof || ''), expectedPassword)) {
      return jsonResponse(res, 403, { error: t('admin.password_wrong') });
    }
  }

  try {
    // 深度合并到 config 对象（直接修改引用，让整个进程生效）
    var merged = deepMerge(config, body);
    var configKeys = Object.keys(merged);
    for (var i = 0; i < configKeys.length; i++) {
      config[configKeys[i]] = merged[configKeys[i]];
    }

    await persistConfigAndHotReload(config, ctx, configPath);

    if (ctx.abuse && ctx.abuse.ruleEngine && typeof ctx.abuse.ruleEngine.updateConfig === 'function') {
      ctx.abuse.ruleEngine.updateConfig(config.abuse_detection || {});
    }
    if (ctx.abuse && ctx.abuse.riskLogger && typeof ctx.abuse.riskLogger.updateConfig === 'function') {
      ctx.abuse.riskLogger.updateConfig(config.abuse_detection || {});
    }

    log('📝', C.green, t('admin.config_updated'));
    logCollector.add('info', t('admin.config_updated'));

    var safe = sanitizeConfig(JSON.parse(JSON.stringify(config)));
    return jsonResponse(res, 200, safe);
  } catch (e) {
    logCollector.add('error', t('admin.config_update_failed', { reason: e.message }));
    return jsonResponse(res, 500, {
      error: t('admin.config_update_failed', { reason: e.message }),
    });
  }
}

function handleRateLimitsGet(res, config) {
  var rateLimits = ensureRateLimitsConfig(config);
  return jsonResponse(res, 200, {
    success: true,
    rate_limits: cloneDeep(rateLimits),
  });
}

async function handleRateLimitsPut(req, res, ctx, config, configPath, t) {
  var body;
  try {
    body = await readBody(req);
  } catch (_) {
    return jsonResponse(res, 400, { success: false, error: t('admin.rate_limits_invalid_body') });
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return jsonResponse(res, 400, { success: false, error: t('admin.rate_limits_invalid_body') });
  }

  var current = ensureRateLimitsConfig(config);
  var next;
  try {
    next = normalizeRateLimitConfig({
      enabled: body.enabled === undefined ? current.enabled : body.enabled === true,
      global: parseRateLimitPair(body.global, current.global, 'rate_limits.global'),
      default_per_user: parseRateLimitPair(body.default_per_user, current.default_per_user, 'rate_limits.default_per_user'),
      overrides: current.overrides,
    });
  } catch (err) {
    return jsonResponse(res, 400, {
      success: false,
      error: t('admin.config_update_failed', { reason: err.message || 'rate_limits_invalid' }),
    });
  }

  config.rate_limits = next;
  try {
    await persistConfigAndHotReload(config, ctx, configPath);
  } catch (e) {
    return jsonResponse(res, 500, {
      success: false,
      error: t('admin.config_update_failed', { reason: e.message || 'rate_limits_persist_failed' }),
    });
  }

  logCollector.add('info', '限速配置已更新', {
    enabled: next.enabled,
    global_rpm: next.global.rpm,
    global_tpm: next.global.tpm,
    user_default_rpm: next.default_per_user.rpm,
    user_default_tpm: next.default_per_user.tpm,
  });
  return jsonResponse(res, 200, {
    success: true,
    rate_limits: cloneDeep(next),
  });
}

async function handleRateLimitsUserPut(req, res, ctx, config, configPath, identity, t) {
  var identityCheck = validateIdentityValue(identity);
  if (!identityCheck.ok) {
    return jsonResponse(res, 400, {
      success: false,
      error: identityCheck.reason === 'identity_required' ? t('admin.rate_limits_identity_required') : 'identity_invalid',
    });
  }
  var normalizedIdentity = identityCheck.value;

  var body;
  try {
    body = await readBody(req);
  } catch (_) {
    return jsonResponse(res, 400, { success: false, error: t('admin.rate_limits_invalid_body') });
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return jsonResponse(res, 400, { success: false, error: t('admin.rate_limits_invalid_body') });
  }

  var current = ensureRateLimitsConfig(config);
  var nextOverride;
  try {
    nextOverride = parseRateLimitPair(
      body,
      Object.prototype.hasOwnProperty.call(current.overrides, normalizedIdentity) ? current.overrides[normalizedIdentity] : current.default_per_user,
      'rate_limits.override'
    );
  } catch (err) {
    return jsonResponse(res, 400, {
      success: false,
      error: t('admin.config_update_failed', { reason: err.message || 'rate_limits_override_invalid' }),
    });
  }

  var nextOverrides = cloneDeep(current.overrides);
  nextOverrides[normalizedIdentity] = nextOverride;
  var next = normalizeRateLimitConfig({
    enabled: current.enabled,
    global: current.global,
    default_per_user: current.default_per_user,
    overrides: nextOverrides,
  });
  config.rate_limits = next;

  try {
    await persistConfigAndHotReload(config, ctx, configPath);
  } catch (e) {
    return jsonResponse(res, 500, {
      success: false,
      error: t('admin.config_update_failed', { reason: e.message || 'rate_limits_override_persist_failed' }),
    });
  }

  logCollector.add('info', '用户限速覆盖已更新', {
    identity: normalizedIdentity,
    rpm: nextOverride.rpm,
    tpm: nextOverride.tpm,
  });
  return jsonResponse(res, 200, {
    success: true,
    identity: normalizedIdentity,
    limits: cloneDeep(nextOverride),
    rate_limits: cloneDeep(next),
  });
}

async function handleRateLimitsUserDelete(res, ctx, config, configPath, identity, t) {
  var identityCheck = validateIdentityValue(identity);
  if (!identityCheck.ok) {
    return jsonResponse(res, 400, {
      success: false,
      error: identityCheck.reason === 'identity_required' ? t('admin.rate_limits_identity_required') : 'identity_invalid',
    });
  }
  var normalizedIdentity = identityCheck.value;

  var current = ensureRateLimitsConfig(config);
  var nextOverrides = cloneDeep(current.overrides);
  var existed = Object.prototype.hasOwnProperty.call(nextOverrides, normalizedIdentity);
  delete nextOverrides[normalizedIdentity];

  var next = normalizeRateLimitConfig({
    enabled: current.enabled,
    global: current.global,
    default_per_user: current.default_per_user,
    overrides: nextOverrides,
  });
  config.rate_limits = next;

  try {
    await persistConfigAndHotReload(config, ctx, configPath);
  } catch (e) {
    return jsonResponse(res, 500, {
      success: false,
      error: t('admin.config_update_failed', { reason: e.message || 'rate_limits_override_delete_failed' }),
    });
  }

  if (existed) {
    logCollector.add('info', '用户限速覆盖已删除', { identity: normalizedIdentity });
  }
  return jsonResponse(res, 200, {
    success: true,
    identity: normalizedIdentity,
    rate_limits: cloneDeep(next),
  });
}

/**
 * 获取请求日志
 */
function handleGetLogs(req, res) {
  var query = parseQuery(req.url);
  var result = logCollector.getAll({
    level: query.level || undefined,
    search: query.search || undefined,
    limit: query.limit || 50,
    offset: query.offset || 0,
  });
  return jsonResponse(res, 200, result);
}

/**
 * 日志统计
 */
function handleLogStats(res) {
  return jsonResponse(res, 200, logCollector.getStats());
}

/**
 * 清空日志
 */
function handleClearLogs(res, t) {
  logCollector.clear();
  log('🗑️', C.cyan, t('admin.logs_cleared'));
  return jsonResponse(res, 200, { success: true, message: t('admin.logs_cleared') });
}

/**
 * 浏览器登录 — 用 email+password 自动登录 ChatGPT 提取凭证
 */

// ============ Stats API 端点 ============

function _dateStrToUtcTs(s) {
  var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s || '');
  if (!m) return NaN;
  return Date.UTC(parseInt(m[1], 10), parseInt(m[2], 10) - 1, parseInt(m[3], 10));
}

function _getNearestStatsDateWithData(stats, from, to) {
  if (!stats || typeof stats.getAvailableDates !== 'function') return null;
  var dates = stats.getAvailableDates();
  if (!Array.isArray(dates) || dates.length === 0) return null;

  var targetTs = _dateStrToUtcTs(to || from);
  var bestDate = null;
  var bestDist = Number.POSITIVE_INFINITY;

  for (var i = 0; i < dates.length; i++) {
    var date = dates[i];
    if (!_isValidDateStr(date)) continue;
    var dayOverview = stats.getOverviewRange(date, date);
    if ((Number(dayOverview && dayOverview.total_requests) || 0) <= 0) continue;

    if (!isFinite(targetTs)) {
      if (!bestDate || date > bestDate) bestDate = date;
      continue;
    }

    var ts = _dateStrToUtcTs(date);
    if (!isFinite(ts)) continue;
    var dist = Math.abs(ts - targetTs);
    if (dist < bestDist) {
      bestDist = dist;
      bestDate = date;
    }
  }

  return bestDate;
}

function _isOverviewEmpty(data) {
  return (Number(data && data.total_requests) || 0) <= 0;
}

function _isTimeseriesEmpty(data) {
  if (!Array.isArray(data) || data.length === 0) return true;
  for (var i = 0; i < data.length; i++) {
    var row = data[i] || {};
    if ((Number(row.requests) || 0) > 0) return false;
    if ((Number(row.success) || 0) > 0) return false;
    if ((Number(row.input) || 0) > 0) return false;
    if ((Number(row.output) || 0) > 0) return false;
  }
  return true;
}

function _isRankedRowsEmpty(data) {
  if (!Array.isArray(data) || data.length === 0) return true;
  for (var i = 0; i < data.length; i++) {
    var row = data[i] || {};
    if ((Number(row.requests) || 0) > 0) return false;
    if ((Number(row.input) || 0) > 0) return false;
    if ((Number(row.output) || 0) > 0) return false;
    if ((Number(row.errors) || 0) > 0) return false;
  }
  return true;
}

function _resolveStatsRangeWithFallback(stats, opts, getter, isEmpty) {
  var data = getter(opts.from, opts.to);
  if (!isEmpty(data)) return data;

  var fallbackDate = _getNearestStatsDateWithData(stats, opts.from, opts.to);
  if (!fallbackDate) return data;
  if (fallbackDate === opts.from && fallbackDate === opts.to) return data;
  return getter(fallbackDate, fallbackDate);
}

function handleStatsOverview(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, {});
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getOverviewTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getOverviewLastHours(opts.hours));
  if (opts.mode === 'range') {
    var data = _resolveStatsRangeWithFallback(
      ctx.stats,
      opts,
      function (from, to) { return ctx.stats.getOverviewRange(from, to); },
      _isOverviewEmpty
    );
    return jsonResponse(res, 200, data);
  }
  return jsonResponse(res, 200, ctx.stats.getOverview());
}

function handleStatsTimeseries(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getTimeseriesTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getTimeseriesLastHours(opts.hours));
  var data = _resolveStatsRangeWithFallback(
    ctx.stats,
    opts,
    function (from, to) { return ctx.stats.getTimeseriesRange(from, to); },
    _isTimeseriesEmpty
  );
  return jsonResponse(res, 200, data);
}

function handleStatsModels(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getModelStatsTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getModelStatsLastHours(opts.hours));
  var data = _resolveStatsRangeWithFallback(
    ctx.stats,
    opts,
    function (from, to) { return ctx.stats.getModelStatsRange(from, to); },
    _isRankedRowsEmpty
  );
  return jsonResponse(res, 200, data);
}

function handleStatsAccounts(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getAccountStatsTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getAccountStatsLastHours(opts.hours));
  var data = _resolveStatsRangeWithFallback(
    ctx.stats,
    opts,
    function (from, to) { return ctx.stats.getAccountStatsRange(from, to); },
    _isRankedRowsEmpty
  );
  return jsonResponse(res, 200, data);
}

function handleStatsCallers(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getCallerStatsTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getCallerStatsLastHours(opts.hours));
  var data = _resolveStatsRangeWithFallback(
    ctx.stats,
    opts,
    function (from, to) { return ctx.stats.getCallerStatsRange(from, to); },
    _isRankedRowsEmpty
  );
  return jsonResponse(res, 200, data);
}

function handleStatsRecent(res, ctx, page, limit, filter, search, source, date, hours) {
  var normalizedSearch = typeof search === 'string' ? search.trim() : '';
  var normalizedSource = source === 'file' ? 'file' : 'memory';
  var normalizedHours = undefined;
  if (hours !== undefined && hours !== null && hours !== '') {
    var parsedHours = parseInt(hours, 10);
    if (parsedHours > 0 && parsedHours <= 720) normalizedHours = parsedHours;
  }
  if (!ctx.stats) {
    var fallbackLimit = parseInt(limit, 10);
    if (!fallbackLimit || fallbackLimit < 1) {
      fallbackLimit = (normalizedSource === 'file' || normalizedSearch) ? 500 : 20;
    }
    if (normalizedSource === 'file' || normalizedSearch) {
      if (fallbackLimit > 2000) fallbackLimit = 2000;
    } else if (fallbackLimit > 100) {
      fallbackLimit = 100;
    }
    return jsonResponse(res, 200, { data: [], total: 0, page: 1, pages: 1, limit: fallbackLimit });
  }
  return jsonResponse(res, 200, ctx.stats.getRecentRequests(page, limit, filter, normalizedSearch, normalizedSource, date, normalizedHours));
}

function handleAccountIndexMap(res, ctx) {
  if (!ctx.pool) return jsonResponse(res, 200, {});
  return jsonResponse(res, 200, ctx.pool.getAccountIndexMap());
}

function handlePoolHealthStatus(res, ctx) {
  var monitor = ctx.poolHealthMonitor;
  if (!monitor) {
    return jsonResponse(res, 200, { enabled: false, probe: null, guard: null });
  }
  return jsonResponse(res, 200, monitor.getStatus());
}

function envelope(res, statusCode, success, data, error, meta) {
  return jsonResponse(res, statusCode, {
    success: success,
    data: data === undefined ? null : data,
    error: error || null,
    meta: meta || {},
  });
}

function parseSeqNumber(seqId) {
  var raw = String(seqId || '').trim();
  var matched = raw.match(/^discord_(\d+)$/);
  if (!matched) return 0;
  var n = Number(matched[1]);
  if (!isFinite(n) || n <= 0) return 0;
  return Math.floor(n);
}

function normalizeSeqId(seqId) {
  var numberValue = parseSeqNumber(seqId);
  if (!numberValue) return '';
  return 'discord_' + String(numberValue);
}

function parseIsoTime(isoString) {
  var ts = Date.parse(String(isoString || ''));
  if (!isFinite(ts)) return Number.MAX_SAFE_INTEGER;
  return ts;
}

function resolveDiscordUserStore(ctx) {
  if (!ctx || typeof ctx !== 'object') return null;
  if (ctx.discordUserStore && typeof ctx.discordUserStore.listUsers === 'function') {
    return ctx.discordUserStore;
  }
  if (ctx.stores && ctx.stores.userStore && typeof ctx.stores.userStore.listUsers === 'function') {
    return ctx.stores.userStore;
  }
  var engine = getAbuseRuleEngine(ctx);
  if (engine && engine._userStore && typeof engine._userStore.listUsers === 'function') {
    return engine._userStore;
  }
  return null;
}

function normalizeDiscordUserForAdmin(raw) {
  var row = raw && typeof raw === 'object' ? raw : {};
  var seqId = normalizeSeqId(row.seq_id);
  return {
    discord_user_id: String(row.discord_user_id || ''),
    seq_id: seqId,
    username: String(row.username || ''),
    global_name: String(row.global_name || ''),
    avatar: String(row.avatar || ''),
    status: String(row.status || 'active'),
    roles: Array.isArray(row.roles) ? row.roles.slice() : [],
    created_at: String(row.created_at || ''),
    last_login_at: String(row.last_login_at || ''),
  };
}

function handleDiscordUsers(req, res, ctx) {
  var userStore = resolveDiscordUserStore(ctx);
  if (!userStore || typeof userStore.listUsers !== 'function') {
    return envelope(res, 200, true, [], null, {
      total: 0,
      page: 1,
      pages: 1,
      limit: 50,
    });
  }

  var query = parseQuery(req.url);
  var keyword = String(query.q || query.search || '').trim().toLowerCase();
  var statusFilter = String(query.status || '').trim().toLowerCase();
  var page = Math.max(1, parseInt(query.page || '1', 10) || 1);
  var limit = parseInt(query.limit || '50', 10) || 50;
  if (limit < 1) limit = 1;
  if (limit > 500) limit = 500;

  var users = userStore.listUsers();
  var rows = [];
  for (var i = 0; i < users.length; i++) {
    rows.push(normalizeDiscordUserForAdmin(users[i]));
  }

  rows.sort(function (a, b) {
    var seqDiff = parseSeqNumber(a.seq_id) - parseSeqNumber(b.seq_id);
    if (seqDiff !== 0) return seqDiff;
    var createDiff = parseIsoTime(a.created_at) - parseIsoTime(b.created_at);
    if (createDiff !== 0) return createDiff;
    return a.discord_user_id.localeCompare(b.discord_user_id);
  });

  var filtered = [];
  for (var j = 0; j < rows.length; j++) {
    var item = rows[j];
    if (statusFilter && String(item.status || '').toLowerCase() !== statusFilter) continue;
    if (keyword) {
      var target = (
        item.seq_id + ' '
        + item.discord_user_id + ' '
        + item.username + ' '
        + item.global_name
      ).toLowerCase();
      if (target.indexOf(keyword) < 0) continue;
    }
    filtered.push(item);
  }

  var total = filtered.length;
  var pages = Math.max(1, Math.ceil(total / limit));
  if (page > pages) page = pages;
  var start = (page - 1) * limit;
  var data = filtered.slice(start, start + limit);

  return envelope(res, 200, true, data, null, {
    total: total,
    page: page,
    pages: pages,
    limit: limit,
    q: keyword,
    status: statusFilter || '',
  });
}

function getAbuseRuleEngine(ctx) {
  var abuse = ctx && ctx.abuse ? ctx.abuse : null;
  if (!abuse) return null;
  return abuse.ruleEngine || null;
}

function getAbuseRiskLogger(ctx) {
  var abuse = ctx && ctx.abuse ? ctx.abuse : null;
  if (!abuse) return null;
  return abuse.riskLogger || null;
}

function handleAbuseOverview(req, res, ctx) {
  var engine = getAbuseRuleEngine(ctx);
  if (!engine || typeof engine.getOverview !== 'function') {
    return envelope(res, 200, true, {
      enabled: false,
      total_users: 0,
      levels: { low: 0, medium: 0, high: 0, critical: 0 },
      actions: { observe: 0, throttle: 0, challenge: 0, suspend: 0 },
      today_events: 0,
    }, null, {});
  }
  var overview = engine.getOverview();
  return envelope(res, 200, true, overview, null, {});
}

function handleAbuseUsers(req, res, ctx) {
  var engine = getAbuseRuleEngine(ctx);
  if (!engine || typeof engine.listUsers !== 'function') {
    return envelope(res, 200, true, [], null, { total: 0, page: 1, pages: 1, limit: 50 });
  }
  var query = parseQuery(req.url);
  var result = engine.listUsers({
    page: parseInt(query.page || '1', 10),
    limit: parseInt(query.limit || '50', 10),
    level: query.level || '',
    action: query.action || '',
    sort: query.sort || 'score_desc',
    keyword: query.q || query.keyword || '',
  });
  return envelope(res, 200, true, result.data || [], null, {
    total: result.total || 0,
    page: result.page || 1,
    pages: result.pages || 1,
    limit: result.limit || 50,
  });
}

function handleAbuseUserDetail(req, res, ctx, userId) {
  var identityCheck = validateIdentityValue(userId);
  if (!identityCheck.ok) {
    return envelope(res, 400, false, null, { message: identityCheck.reason }, {});
  }
  var normalizedUserId = identityCheck.value;
  var engine = getAbuseRuleEngine(ctx);
  if (!engine || typeof engine.getUserDetail !== 'function') {
    return envelope(res, 404, false, null, { message: 'abuse_engine_not_available' }, {});
  }
  var query = parseQuery(req.url);
  var detail = engine.getUserDetail(normalizedUserId, {
    page: parseInt(query.page || '1', 10),
    limit: parseInt(query.limit || '100', 10),
  });
  if (!detail) {
    return envelope(res, 404, false, null, { message: 'abuse_user_not_found' }, {});
  }
  return envelope(res, 200, true, detail, null, {});
}

async function handleAbuseUserAction(req, res, ctx, userId) {
  var identityCheck = validateIdentityValue(userId);
  if (!identityCheck.ok) {
    return envelope(res, 400, false, null, { message: identityCheck.reason }, {});
  }
  var normalizedUserId = identityCheck.value;
  var engine = getAbuseRuleEngine(ctx);
  if (!engine || typeof engine.applyManualAction !== 'function') {
    return envelope(res, 404, false, null, { message: 'abuse_engine_not_available' }, {});
  }
  var body;
  try {
    body = await readBody(req);
  } catch (_) {
    return envelope(res, 400, false, null, { message: 'invalid_body' }, {});
  }
  var action = body && body.action ? String(body.action) : '';
  var reason = body && body.reason ? String(body.reason) : '';
  if (!action) {
    return envelope(res, 400, false, null, { message: 'missing_action' }, {});
  }
  try {
    var state = engine.applyManualAction(normalizedUserId, action, {
      reason: reason,
      operator: 'admin',
      ip: (getRealClientIp(req).ip || ''),
    });
    return envelope(res, 200, true, state, null, {});
  } catch (e) {
    return envelope(res, 400, false, null, { message: e.message || 'manual_action_failed' }, {});
  }
}

function handleAbuseEvents(req, res, ctx) {
  var logger = getAbuseRiskLogger(ctx);
  if (!logger || typeof logger.listEvents !== 'function') {
    return envelope(res, 200, true, [], null, { total: 0, page: 1, pages: 1, limit: 100 });
  }
  var query = parseQuery(req.url);
  var callerIdentity = String(query.caller_identity || query.identity || '').trim();
  if (callerIdentity) {
    var identityCheck = validateIdentityValue(callerIdentity);
    if (!identityCheck.ok) {
      return envelope(res, 400, false, null, { message: 'identity_invalid' }, {});
    }
    callerIdentity = identityCheck.value;
  }
  var result = logger.listEvents({
    page: parseInt(query.page || '1', 10),
    limit: parseInt(query.limit || '100', 10),
    caller_identity: callerIdentity,
    action: query.action || '',
    rule_id: query.rule_id || '',
    from: query.from || '',
    to: query.to || '',
  });
  return envelope(res, 200, true, result.data || [], null, {
    total: result.total || 0,
    page: result.page || 1,
    pages: result.pages || 1,
    limit: result.limit || 100,
  });
}

function handleAbuseRulesGet(res, ctx, config) {
  var engine = getAbuseRuleEngine(ctx);
  var rules = null;
  if (engine && typeof engine.getRulesConfig === 'function') {
    rules = engine.getRulesConfig();
  } else {
    rules = (config && config.abuse_detection) ? config.abuse_detection : {};
  }
  return envelope(res, 200, true, rules, null, {});
}

async function handleAbuseRulesPut(req, res, ctx, config, configPath) {
  var body;
  try {
    body = await readBody(req);
  } catch (_) {
    return envelope(res, 400, false, null, { message: 'invalid_body' }, {});
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return envelope(res, 400, false, null, { message: 'invalid_body' }, {});
  }

  var current = (config && config.abuse_detection && typeof config.abuse_detection === 'object')
    ? config.abuse_detection
    : {};
  var merged = deepMerge(current, body);
  config.abuse_detection = merged;

  try {
    persistConfig(config, configPath);
  } catch (e) {
    return envelope(res, 500, false, null, { message: e.message || 'config_persist_failed' }, {});
  }

  var engine = getAbuseRuleEngine(ctx);
  if (engine && typeof engine.updateConfig === 'function') {
    engine.updateConfig(merged);
  }
  var riskLogger = getAbuseRiskLogger(ctx);
  if (riskLogger && typeof riskLogger.updateConfig === 'function') {
    riskLogger.updateConfig(merged);
  }

  return envelope(res, 200, true, merged, null, {});
}

function handleReloginStatus(res, ctx) {
  var credConfig = ctx.config && ctx.config.credentials;
  var autoReloginEnabled = !!(credConfig && credConfig.auto_relogin === true);
  var enabled = autoReloginEnabled && !!(credConfig && credConfig.session_invalidated_relogin === true);
  var threshold = (credConfig && credConfig.session_invalidated_relogin_threshold) || 3;
  var reloginAccounts = ctx.pool ? ctx.pool.getReloginAccounts() : [];
  var allRelogin = ctx.pool ? ctx.pool.listAccounts().filter(function (a) {
    return a.status === 'relogin_needed';
  }) : [];
  return jsonResponse(res, 200, {
    enabled: enabled,
    auto_relogin_enabled: autoReloginEnabled,
    threshold: threshold,
    relogin_needed_count: allRelogin.length,
    with_password: reloginAccounts.length,
    without_password: allRelogin.length - reloginAccounts.length,
    accounts: allRelogin.map(function (a) {
      return {
        email: a.email,
        has_password: !!(ctx.pool.getFullAccount(a.email) || {}).password,
        session_invalidated_count: a.last_error_type === 'session_invalidated'
          ? (ctx.pool.getFullAccount(a.email) || {}).session_invalidated_count || 0
          : 0,
      };
    }),
  });
}

// ============ Register 代理转发 ============

/**
 * 注册服务器代理 — 转发请求到 gpt-reg API
 *
 * 路由映射:
 *   POST /admin/api/register/start    → POST /api/jobs/start
 *   GET  /admin/api/register/status   → GET  /api/jobs/status
 *   POST /admin/api/register/stop     → POST /api/jobs/stop
 *   GET  /admin/api/register/accounts → GET  /api/accounts
 *   GET  /admin/api/register/accounts/stats → GET /api/accounts/stats
 *   GET  /admin/api/register/proxy-config → GET /api/proxy-config
 *   POST /admin/api/register/proxy-config → POST /api/proxy-config
 *   POST /admin/api/register/accounts/upload → POST /api/accounts/upload
 */
/**
 * 批量测试模型可用性
 *
 * 对每个模型发送轻量级 /v1/chat/completions 请求（通过本地回环），
 * 检测模型是否可正常响应。
 *
 * 修复 BUG-002: 原始实现中 fetch 异常时 upstreamResp 为 undefined，
 * 导致访问 upstreamResp.status 报 "Cannot read properties of undefined (reading 'status')"。
 * 修复方案: 在 catch 中不访问 response 对象，直接从 error 获取信息。
 */
async function handleTestModels(req, res, pool, config) {
  var models = modelMapper.listModels();
  if (!models || models.length === 0) {
    return jsonResponse(res, 200, { total: 0, results: [] });
  }

  var password = (config.server && config.server.password) || '';
  var port = (config.server && config.server.port) || 8066;
  var baseUrl = 'http://127.0.0.1:' + port;

  var results = [];
  var okCount = 0;
  var failCount = 0;

  for (var i = 0; i < models.length; i++) {
    var model = models[i];
    var modelId = model.id || '';
    var startTime = Date.now();

    // 关键修复: 整个 fetch 链路用 try/catch 包裹
    // 确保任何异常都不会导致 undefined.status 问题
    var testResult;
    try {
      var headers = { 'Content-Type': 'application/json' };
      if (password) {
        headers['Authorization'] = 'Bearer ' + password;
      }

      var resp = await fetch(baseUrl + '/v1/chat/completions', {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({
          model: modelId,
          messages: [{ role: 'user', content: 'hi' }],
          stream: false,
          max_tokens: 1,
        }),
        signal: AbortSignal.timeout(30000),
      });

      var latency = Date.now() - startTime;

      // 防御性检查: resp 可能为 null/undefined（理论上不会，但做防御）
      if (!resp || typeof resp.status !== 'number') {
        testResult = { model: modelId, status: 'fail', latency: latency, error: 'invalid_response', http_status: 0 };
      } else if (resp.status >= 200 && resp.status < 300) {
        testResult = { model: modelId, status: 'ok', latency: latency, error: '', http_status: resp.status };
      } else {
        // 读取错误体
        var errBody = '';
        try {
          errBody = await resp.text();
        } catch (_) {}
        var errMsg = 'HTTP ' + resp.status;
        try {
          var parsed = JSON.parse(errBody);
          if (parsed && parsed.error) {
            if (typeof parsed.error === 'string') {
              errMsg = parsed.error;
            } else if (parsed.error.message) {
              errMsg = parsed.error.message;
            }
          }
        } catch (_) {}
        testResult = { model: modelId, status: 'fail', latency: latency, error: errMsg, http_status: resp.status };
      }
    } catch (err) {
      // 修复核心: fetch 抛出异常时（网络错误、超时等）
      // 不访问 resp.status（此时 resp 可能是 undefined！）
      // 直接从 err 对象获取错误信息
      var errLatency = Date.now() - startTime;
      testResult = {
        model: modelId,
        status: 'fail',
        latency: errLatency,
        error: (err && err.message) ? err.message : 'unknown_error',
        http_status: 0,
      };
    }

    if (testResult.status === 'ok') {
      okCount++;
    } else {
      failCount++;
    }
    results.push(testResult);
    log('🧪', C.cyan, 'model test: ' + modelId + ' → ' + testResult.status + ' (' + testResult.latency + 'ms)' + (testResult.error ? ' | ' + testResult.error : ''));
  }

  return jsonResponse(res, 200, {
    total: models.length,
    ok: okCount,
    fail: failCount,
    results: results,
  });
}

/**
 * 批量验证失效账号
 */
async function handleVerifyBatch(req, res, pool, config) {
  var verifiable = pool.getVerifiableAccounts();
  if (verifiable.length === 0) {
    return jsonResponse(res, 200, { total: 0, verified_ok: 0, verified_fail: 0, skipped: 0, results: [] });
  }

  var phConfig = (config && config.pool_health) || {};
  var configuredConcurrency = parseInt(phConfig.manual_verify_concurrency, 10);
  var concurrency = Number.isFinite(configuredConcurrency) && configuredConcurrency > 0
    ? configuredConcurrency
    : 20;
  var results = [];
  var stats = { verified_ok: 0, verified_fail: 0, skipped: 0 };

  for (var i = 0; i < verifiable.length; i += concurrency) {
    var batch = verifiable.slice(i, i + concurrency);
    var promises = batch.map(function (account) {
      var email = account.email;
      var wasWasted = account.status === 'wasted';
      if (pool.isLocked(email)) {
        stats.skipped++;
        return Promise.resolve({ email: email, status: 'skipped', reason: 'account locked' });
      }
      if (!pool.lockAccount(email)) {
        stats.skipped++;
        return Promise.resolve({ email: email, status: 'skipped', reason: 'account locked' });
      }

      if (wasWasted) {
        pool.activateWasted(email);
      }

      var fullAccount = pool.getFullAccount(email);
      if (!fullAccount || !fullAccount.sessionToken) {
        if (fullAccount) {
          fullAccount.last_error_type = 'no_session_token';
          fullAccount.last_error_code = 401;
          pool.markWasted(email);
        }
        stats.verified_fail++;
        pool.unlockAccount(email);
        return Promise.resolve({ email: email, status: 'fail', reason: 'no session token' });
      }

      var verifyTokenVersion = typeof fullAccount._tokenVersion === 'number' ? fullAccount._tokenVersion : 0;

      return refreshAccountToken(fullAccount).then(function (result) {
        if (result.success) {
          var verifyCas = pool.applyRefreshResultCAS(fullAccount, result, verifyTokenVersion);
          if (verifyCas.applied) {
            stats.verified_ok++;
            log('✅', C.green, '批量验证成功: ' + email);
            return { email: email, status: 'ok' };
          }
          if (verifyCas.reason === 'stale_version') {
            stats.skipped++;
            log('ℹ️', C.cyan, '批量验证跳过（版本落后）: ' + email);
            return { email: email, status: 'skipped', reason: 'token already refreshed' };
          }
          fullAccount.last_error_type = 'cas_apply_failed';
          fullAccount.last_error_code = 0;
          pool.markWasted(email);
          stats.verified_fail++;
          return { email: email, status: 'fail', reason: verifyCas.reason || 'cas_apply_failed' };
        } else {
          fullAccount.last_error_type = 'verify_failed';
          fullAccount.last_error_code = (result && result.statusCode) || 401;
          pool.markWasted(email);
          stats.verified_fail++;
          var detail = (result && (result.detail || result.error)) || 'unknown';
          log('❌', C.red, '批量验证失败: ' + email + ' (' + detail + ')');
          return { email: email, status: 'fail', reason: detail };
        }
      }).catch(function (err) {
        fullAccount.last_error_type = 'network_error';
        fullAccount.last_error_code = 0;
        pool.markWasted(email);
        stats.verified_fail++;
        return { email: email, status: 'fail', reason: err.message };
      }).finally(function () {
        pool.unlockAccount(email);
      });
    });

    var batchResults = await Promise.all(promises);
    for (var j = 0; j < batchResults.length; j++) {
      results.push(batchResults[j]);
    }
  }

  pool.forceSave();
  return jsonResponse(res, 200, {
    total: verifiable.length,
    verified_ok: stats.verified_ok,
    verified_fail: stats.verified_fail,
    skipped: stats.skipped,
    results: results,
  });
}

async function handleRegisterProxy(req, res, config, path, method) {
  var regConfig = config.register;
  if (!regConfig || !regConfig.enabled) {
    return jsonResponse(res, 503, { error: 'Registration service not configured' });
  }
  if (!regConfig.api_url || !regConfig.api_token) {
    return jsonResponse(res, 503, { error: 'Registration service URL or token not configured' });
  }

  if (method === 'POST' && path === '/admin/api/register/start') {
    var startParams = {};
    try {
      startParams = await readBody(req);
    } catch (_) {
      startParams = {};
    }
    if (!startParams || typeof startParams !== 'object' || Array.isArray(startParams)) {
      startParams = {};
    }
    var ensureResult = await ensureServerRunning(config, {
      reason: 'admin_manual_start',
    });
    if (!ensureResult.ok) {
      return jsonResponse(res, 502, { error: ensureResult.error || 'Failed to ensure registration server online' });
    }

    var startResult = await startRegistration(config, startParams, {
      ensureServerRunning: false,
      ensureReason: 'admin_manual_start',
    });
    if (!startResult.ok) {
      return jsonResponse(res, 502, { error: startResult.error || 'Failed to start registration' });
    }
    return jsonResponse(res, 200, startResult.data || {});
  }

  if (method === 'GET' && path === '/admin/api/register/status') {
    var statusResult = await getRegistrationStatus(config);
    if (!statusResult.ok) {
      return jsonResponse(res, 502, { error: statusResult.error || 'Failed to get registration status' });
    }
    var statusPayload = statusResult.data;
    if (!statusPayload || typeof statusPayload !== 'object' || Array.isArray(statusPayload)) {
      statusPayload = {};
    }
    if (!statusPayload.proxy || typeof statusPayload.proxy !== 'object' || typeof statusPayload.proxy.enabled !== 'boolean') {
      var localProxyForStatus = ensureProxyConfigFromRegisterClient(config);
      var localRegisterProxyForStatus = ensureRegisterProxyConfigFromRegisterClient(localProxyForStatus);
      statusPayload.proxy = {
        enabled: !!localRegisterProxyForStatus.enabled,
        server: localRegisterProxyForStatus.server || REGISTER_PROXY_DEFAULT_SERVER,
        active_preset: localRegisterProxyForStatus.active_preset ? String(localRegisterProxyForStatus.active_preset) : '',
      };
    }
    return jsonResponse(res, 200, statusPayload);
  }

  // 路由映射
  var routeMap = {
    'POST:/admin/api/register/start': 'POST:/api/jobs/start',
    'GET:/admin/api/register/status': 'GET:/api/jobs/status',
    'POST:/admin/api/register/stop': 'POST:/api/jobs/stop',
    'GET:/admin/api/register/accounts': 'GET:/api/accounts',
    'GET:/admin/api/register/accounts/stats': 'GET:/api/accounts/stats',
    'POST:/admin/api/register/accounts/upload': 'POST:/api/accounts/upload',
    'GET:/admin/api/register/jobs/stats': 'GET:/api/jobs/stats',
    'GET:/admin/api/register/jobs/history': 'GET:/api/jobs/history',
    'GET:/admin/api/register/proxy-config': 'GET:/api/proxy-config',
    'POST:/admin/api/register/proxy-config': 'POST:/api/proxy-config',
  };

  var routeKey = method + ':' + path;
  var target = routeMap[routeKey];
  if (!target) {
    return jsonResponse(res, 404, { error: 'Unknown register endpoint: ' + path });
  }

  var targetMethod = target.split(':')[0];
  var targetPath = target.split(':')[1];

  // 转发查询参数
  var queryIdx = req.url.indexOf('?');
  var queryStr = queryIdx >= 0 ? req.url.substring(queryIdx) : '';
  var upstreamUrl = regConfig.api_url + targetPath + queryStr;

  try {
    var fetchOpts = {
      method: targetMethod,
      headers: {
        'Authorization': 'Bearer ' + regConfig.api_token,
        'Content-Type': 'application/json',
      },
      signal: AbortSignal.timeout(30000),
    };

    // POST 请求转发 body（允许空 body）
    if (targetMethod === 'POST') {
      var body = {};
      try {
        body = await readBody(req);
      } catch (_) {
        body = {};
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        body = {};
      }

      // 启动注册任务时，附带本地 register_proxy 配置给注册机 API
      if (routeKey === 'POST:/admin/api/register/start') {
        var localProxyForStart = ensureProxyConfigFromRegisterClient(config);
        var localRegisterProxyForStart = ensureRegisterProxyConfigFromRegisterClient(localProxyForStart);
        var registerProxyPayload = buildRegisterProxyForwardPayloadFromRegisterClient(localProxyForStart, localRegisterProxyForStart);

        if (typeof body.proxy !== 'boolean') {
          body.proxy = !!registerProxyPayload.enabled;
        }
        body.register_proxy = {
          enabled: body.proxy ? true : false,
          server: registerProxyPayload.server,
          active_preset: registerProxyPayload.active_preset,
          target: registerProxyPayload.target,
        };
        if (body.proxy) {
          body.proxy_server = registerProxyPayload.server;
          body.proxy_target = registerProxyPayload.target;
        }
      }
      fetchOpts.body = JSON.stringify(body);
    }

    var upstream = await fetch(upstreamUrl, fetchOpts);
    var upstreamBody = await upstream.text();
    var responseBody = upstreamBody;

    if (routeKey === 'GET:/admin/api/register/status') {
      try {
        var statusPayload = JSON.parse(upstreamBody || '{}');
        if (statusPayload && typeof statusPayload === 'object' && !Array.isArray(statusPayload)) {
          if (!statusPayload.proxy || typeof statusPayload.proxy !== 'object' || typeof statusPayload.proxy.enabled !== 'boolean') {
            var localProxy = ensureProxyConfigFromRegisterClient(config);
            var localRegisterProxy = ensureRegisterProxyConfigFromRegisterClient(localProxy);
            statusPayload.proxy = {
              enabled: !!localRegisterProxy.enabled,
              server: localRegisterProxy.server || REGISTER_PROXY_DEFAULT_SERVER,
              active_preset: localRegisterProxy.active_preset ? String(localRegisterProxy.active_preset) : '',
            };
            responseBody = JSON.stringify(statusPayload);
          }
        }
      } catch (_) {}
    }

    res.writeHead(upstream.status, {
      'Content-Type': upstream.headers.get('content-type') || 'application/json',
    });
    res.end(responseBody);

  } catch (err) {
    log('❌', C.red, 'Register proxy error: ' + (err.message || String(err)));
    return jsonResponse(res, 502, {
      error: 'Cannot reach registration server: ' + (err.message || String(err)),
    });
  }
}

// ============ 账号封禁快速检测 ============

async function handleCheckStatus(req, res, pool, ctx) {
  if (!(ctx && ctx.config && ctx.config.credentials && ctx.config.credentials.auto_relogin === true)) {
    return jsonResponse(res, 403, { error: 'relogin_disabled' });
  }

  var body;
  try { body = await readBody(req); } catch (e) {
    return jsonResponse(res, 400, { error: 'invalid body' });
  }
  var email = body.email;
  if (!email) return jsonResponse(res, 400, { error: 'email required' });

  var fullAccount = pool.getFullAccount(email);
  if (!fullAccount) return jsonResponse(res, 404, { error: 'account not found' });

  log('🔍', C.cyan, '检测账号封禁状态: ' + email);
  var registerConfig = ctx.config && ctx.config.register ? {
    api_url: ctx.config.register.api_url,
    api_token: ctx.config.register.api_token,
    relogin_timeout_ms: 120000,
    auto_relogin: !!(ctx.config && ctx.config.credentials && ctx.config.credentials.auto_relogin === true),
  } : null;
  var result = await checkAccountBanStatus(email, fullAccount, registerConfig);

  if (result.status === 'banned') {
    pool.markWasted(email);
    pool.forceSave();
    log('🚫', C.red, '账号被封，已标记废弃: ' + email + ' (' + result.detail + ')');
    logCollector.add('warn', '账号被封: ' + email + ' (' + result.detail + ')');
  } else if (result.status === 'active') {
    log('✅', C.green, '账号正常: ' + email + ' (path: ' + result.path + ')');
  }

  return jsonResponse(res, 200, {
    email: email,
    check_status: result.status,
    detail: result.detail,
    path: result.path || null,
    elapsed_ms: result.elapsed_ms,
  });
}


/**
 * 批量测试账号 — 对所有 active 账号发真实请求
 * 5 并发，每个账号发一个小请求测试
 */
async function handleTestBatch(req, res, pool, config) {
  var body;
  try { body = await readBody(req); } catch (e) { body = {}; }

  var concurrency = body.concurrency || 5;
  var filter = body.filter || 'active';
  var allAccounts = pool.listAccounts();
  var testAccounts = [];

  for (var i = 0; i < allAccounts.length; i++) {
    var a = allAccounts[i];
    if (filter === 'all' || a.status === filter) {
      var full = pool.getFullAccount(a.email);
      if (full && full.accessToken) testAccounts.push(full);
    }
  }

  if (testAccounts.length === 0) {
    return jsonResponse(res, 200, { total: 0, ok: 0, fail: 0, results: [] });
  }

  log('🧪', C.cyan, '批量测试 ' + testAccounts.length + ' 个账号 (并发' + concurrency + ')...');

  var testModel = (config.models && config.models.default) || 'gpt-5.3-codex';
  var okCount = 0, failCount = 0;
  var results = [];
  var queue = testAccounts.slice();

  async function testOne() {
    while (queue.length > 0) {
      var acc = queue.shift();
      var result = await testOneAccount(acc, testModel);
      if (result.ok) {
        okCount++;
        pool.markSuccess(acc.email, null);
        results.push({ email: acc.email, ok: true, latency: result.latency, status: result.status });
      } else if (result.networkError) {
        // 网络异常不改池状态
        failCount++;
        results.push({ email: acc.email, ok: false, latency: result.latency, error: result.error });
      } else {
        failCount++;
        var errResult = pool.markError(acc.email, result.status, result.error || '');
        results.push({ email: acc.email, ok: false, latency: result.latency, status: result.status, error_type: errResult.type, action: errResult.action, error: (result.error || '').substring(0, 150) });
      }
    }
  }

  var workers = [];
  for (var w = 0; w < Math.min(concurrency, testAccounts.length); w++) {
    workers.push(testOne());
  }
  await Promise.all(workers);

  if (failCount > 0) pool.forceSave();
  log('📊', C.cyan, '批量测试完成: ' + okCount + ' 成功, ' + failCount + ' 失败 / ' + testAccounts.length + ' 总计');

  return jsonResponse(res, 200, { total: testAccounts.length, ok: okCount, fail: failCount, results: results });
}

async function handleCheckBatch(req, res, pool, ctx) {
  if (!(ctx && ctx.config && ctx.config.credentials && ctx.config.credentials.auto_relogin === true)) {
    return jsonResponse(res, 403, { error: 'relogin_disabled' });
  }

  var body;
  try { body = await readBody(req); } catch (e) {
    return jsonResponse(res, 400, { error: 'invalid body' });
  }

  var emails = body.emails;
  if (!Array.isArray(emails) || emails.length === 0) {
    var filter = body.filter || 'expired';
    var allAccounts = pool.listAccounts();
    emails = [];
    for (var i = 0; i < allAccounts.length; i++) {
      var a = allAccounts[i];
      if (filter === 'all') { emails.push(a.email); }
      else if (filter === 'expired' && (a.status === 'expired' || a.status === 'relogin_needed')) { emails.push(a.email); }
      else if (a.status === filter) { emails.push(a.email); }
    }
  }

  if (emails.length === 0) return jsonResponse(res, 200, { results: [], total: 0, banned: 0, active: 0, error: 0 });
  if (emails.length > 500) emails = emails.slice(0, 500);

  log('🔍', C.cyan, '批量检测 ' + emails.length + ' 个账号...');
  var bannedCount = 0, activeCount = 0, errorCount = 0;

  // 组装带 token 的账号数组
  var accountsWithTokens = [];
  for (var k = 0; k < emails.length; k++) {
    var fullAcc = pool.getFullAccount(emails[k]);
    accountsWithTokens.push(fullAcc ? fullAcc : { email: emails[k] });
  }

  var registerConfig = ctx.config && ctx.config.register ? {
    api_url: ctx.config.register.api_url,
    api_token: ctx.config.register.api_token,
    relogin_timeout_ms: 120000,
    auto_relogin: !!(ctx.config && ctx.config.credentials && ctx.config.credentials.auto_relogin === true),
  } : null;
  var results = await checkAccountsBatch(accountsWithTokens, registerConfig, function (result) {
    if (result.status === 'banned') { bannedCount++; pool.markWasted(result.email); }
    else if (result.status === 'active') { activeCount++; }
    else { errorCount++; }
  });

  if (bannedCount > 0) {
    pool.forceSave();
    logCollector.add('warn', '批量检测: ' + bannedCount + ' 个被封');
  }

  log('📊', C.cyan, '批量检测完成: ' + activeCount + ' 正常, ' + bannedCount + ' 被封, ' + errorCount + ' 错误');
  return jsonResponse(res, 200, { results: results, total: emails.length, banned: bannedCount, active: activeCount, error: errorCount });
}
