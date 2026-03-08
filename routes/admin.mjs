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
 *   POST   /admin/api/credentials/import/gpa → GPA 凭证导入（管理会话）
 *   GET    /admin/api/credentials/export/gpa  → GPA 凭证导出（管理会话）
 *   POST   /admin/api/accounts/:email/action → 账号操作
 *   DELETE /admin/api/accounts/:email   → 删除账号
 *   GET    /admin/api/config            → 配置信息（脱敏）
 *   PUT    /admin/api/config            → 更新配置（部分更新）
 *   GET    /admin/api/rate-limits       → 获取 RPM/TPM 限速配置
 *   PUT    /admin/api/rate-limits       → 更新全局/默认 RPM/TPM 限速
 *   PUT    /admin/api/rate-limits/user/:identity → 设置用户 RPM/TPM 覆盖
 *   DELETE /admin/api/rate-limits/user/:identity → 删除用户 RPM/TPM 覆盖
 *   GET    /admin/api/discord/users     → Discord 用户列表（支持 seq_id 搜索）
 *   GET    /admin/api/abuse/user/:identity/history → 指定用户请求历史
 *   GET    /admin/api/logs              → 获取请求日志
 *   GET    /admin/api/logs/stats        → 日志统计
 *   DELETE /admin/api/logs              → 清空日志
 *
 * 零依赖: 全用 Node.js 内置模块
 */

import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
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
var require = createRequire(import.meta.url);

// ============ Session 管理 ============

var ADMIN_SESSION_DB_PATH = resolve(process.cwd(), 'data/accounts.db');
var _adminSessionDb = null;
var _adminSessionStmt = null;

function loadBetterSqlite3() {
  try {
    require.resolve('better-sqlite3');
  } catch (_) {
    throw new Error('better-sqlite3 is not installed. Run: npm install better-sqlite3');
  }
  var loaded = require('better-sqlite3');
  return loaded && loaded.default ? loaded.default : loaded;
}

function toInt(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function getSessionMaxAgeMs(config) {
  var maxAgeHours = Number((config && config.server && config.server.session_max_age_hours) || 24);
  if (!Number.isFinite(maxAgeHours) || maxAgeHours <= 0) maxAgeHours = 24;
  return Math.floor(maxAgeHours * 60 * 60 * 1000);
}

function hashAdminSessionToken(token) {
  return crypto.createHash('sha256').update(String(token || ''), 'utf8').digest('hex');
}

function ensureAdminSessionDb() {
  if (_adminSessionDb && _adminSessionStmt) return _adminSessionDb;

  var Database = loadBetterSqlite3();
  var dir = dirname(ADMIN_SESSION_DB_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

  _adminSessionDb = new Database(ADMIN_SESSION_DB_PATH);
  _adminSessionDb.pragma('journal_mode = WAL');
  _adminSessionDb.pragma('busy_timeout = 5000');

  _adminSessionDb.exec(`
    CREATE TABLE IF NOT EXISTS admin_sessions (
      session_hash TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      expires_at_ms INTEGER NOT NULL,
      ip TEXT NOT NULL DEFAULT '',
      updated_at_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_admin_sessions_user ON admin_sessions(username);
    CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires ON admin_sessions(expires_at_ms);
  `);

  _adminSessionStmt = {
    upsert: _adminSessionDb.prepare(`
      INSERT INTO admin_sessions (session_hash, username, created_at_ms, expires_at_ms, ip, updated_at_ms)
      VALUES (@session_hash, @username, @created_at_ms, @expires_at_ms, @ip, @updated_at_ms)
      ON CONFLICT(session_hash) DO UPDATE SET
        username = excluded.username,
        created_at_ms = excluded.created_at_ms,
        expires_at_ms = excluded.expires_at_ms,
        ip = excluded.ip,
        updated_at_ms = excluded.updated_at_ms
    `),
    getByHash: _adminSessionDb.prepare(`
      SELECT session_hash, username, created_at_ms, expires_at_ms, ip
      FROM admin_sessions
      WHERE session_hash = ?
      LIMIT 1
    `),
    deleteByHash: _adminSessionDb.prepare('DELETE FROM admin_sessions WHERE session_hash = ?'),
    deleteExpired: _adminSessionDb.prepare('DELETE FROM admin_sessions WHERE expires_at_ms <= ?'),
    countAll: _adminSessionDb.prepare('SELECT COUNT(1) AS total FROM admin_sessions'),
    deleteOldest: _adminSessionDb.prepare(`
      DELETE FROM admin_sessions
      WHERE session_hash IN (
        SELECT session_hash
        FROM admin_sessions
        ORDER BY created_at_ms ASC
        LIMIT ?
      )
    `),
  };

  return _adminSessionDb;
}

function getAdminSession(token) {
  if (!token) return null;
  ensureAdminSessionDb();
  var row = _adminSessionStmt.getByHash.get(hashAdminSessionToken(token));
  if (!row) return null;
  return {
    username: String(row.username || ''),
    createdAt: toInt(row.created_at_ms, 0),
    expiresAt: toInt(row.expires_at_ms, 0),
    ip: String(row.ip || ''),
  };
}

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
  ensureAdminSessionDb();
  _adminSessionStmt.deleteExpired.run(now);
  var total = toInt((_adminSessionStmt.countAll.get() || {}).total, 0);
  if (total > SESSION_MAX) {
    _adminSessionStmt.deleteOldest.run(total - SESSION_MAX);
  }
}

/**
 * 检查 session 是否有效
 * @param {string} token
 * @param {object} config
 * @returns {boolean}
 */
function isSessionValid(token, config) {
  if (!token) return false;
  var session = getAdminSession(token);
  if (!session) return false;

  var now = Date.now();
  var maxAgeMs = getSessionMaxAgeMs(config);
  if (session.expiresAt <= now || (session.createdAt > 0 && (now - session.createdAt > maxAgeMs))) {
    destroySession(token);
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
  ensureAdminSessionDb();
  _adminSessionStmt.deleteByHash.run(hashAdminSessionToken(token));
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

// ============ 日志收集器（SQLite） ============

var LOG_BUFFER_MAX = 1000;
var LOG_DEFAULT_STATS = { total: 0, info: 0, warn: 0, error: 0, request: 0 };
var LOG_ALLOWED_LEVELS = ['info', 'warn', 'error', 'request'];

var logBuffer = [];
var logStats = Object.assign({}, LOG_DEFAULT_STATS);
var _logDb = null;
var _logStmt = null;

function normalizeLogLevel(level) {
  var raw = String(level || '').toLowerCase();
  return LOG_ALLOWED_LEVELS.indexOf(raw) >= 0 ? raw : 'info';
}

function normalizeLogStats(raw) {
  raw = raw && typeof raw === 'object' ? raw : {};
  return {
    total: Math.max(0, toInt(raw.total, 0)),
    info: Math.max(0, toInt(raw.info, 0)),
    warn: Math.max(0, toInt(raw.warn, 0)),
    error: Math.max(0, toInt(raw.error, 0)),
    request: Math.max(0, toInt(raw.request, 0)),
  };
}

function normalizeLogEntry(raw) {
  var row = raw && typeof raw === 'object' ? raw : {};
  return {
    timestamp: Math.max(0, toInt(row.timestamp !== undefined ? row.timestamp : row.timestamp_ms, Date.now())),
    level: normalizeLogLevel(row.level),
    message: String(row.message || ''),
    meta: row.meta && typeof row.meta === 'object' ? row.meta : null,
  };
}

function safeParseJsonObjectOrNull(text) {
  try {
    var parsed = JSON.parse(String(text || ''));
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
    return null;
  } catch (_) {
    return null;
  }
}

function ensureLogDb() {
  if (_logDb && _logStmt) return _logDb;
  var Database = loadBetterSqlite3();
  var dir = dirname(ADMIN_SESSION_DB_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

  _logDb = new Database(ADMIN_SESSION_DB_PATH);
  _logDb.pragma('journal_mode = WAL');
  _logDb.pragma('busy_timeout = 5000');

  _logDb.exec(`
    CREATE TABLE IF NOT EXISTS runtime_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp_ms INTEGER NOT NULL,
      level TEXT NOT NULL,
      message TEXT NOT NULL DEFAULT '',
      meta_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_logs_time ON runtime_logs(timestamp_ms DESC);
    CREATE INDEX IF NOT EXISTS idx_runtime_logs_level_time ON runtime_logs(level, timestamp_ms DESC);

    CREATE TABLE IF NOT EXISTS runtime_log_totals (
      singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
      total INTEGER NOT NULL DEFAULT 0,
      info INTEGER NOT NULL DEFAULT 0,
      warn INTEGER NOT NULL DEFAULT 0,
      error INTEGER NOT NULL DEFAULT 0,
      request INTEGER NOT NULL DEFAULT 0,
      updated_at_ms INTEGER NOT NULL DEFAULT 0
    );
  `);

  _logStmt = {
    ensureTotalsRow: _logDb.prepare(`
      INSERT INTO runtime_log_totals (singleton_id, total, info, warn, error, request, updated_at_ms)
      VALUES (1, 0, 0, 0, 0, 0, @updated_at_ms)
      ON CONFLICT(singleton_id) DO NOTHING
    `),
    getTotals: _logDb.prepare(`
      SELECT total, info, warn, error, request
      FROM runtime_log_totals
      WHERE singleton_id = 1
      LIMIT 1
    `),
    setTotals: _logDb.prepare(`
      INSERT INTO runtime_log_totals (singleton_id, total, info, warn, error, request, updated_at_ms)
      VALUES (1, @total, @info, @warn, @error, @request, @updated_at_ms)
      ON CONFLICT(singleton_id) DO UPDATE SET
        total = excluded.total,
        info = excluded.info,
        warn = excluded.warn,
        error = excluded.error,
        request = excluded.request,
        updated_at_ms = excluded.updated_at_ms
    `),
    incrementTotals: _logDb.prepare(`
      UPDATE runtime_log_totals SET
        total = total + 1,
        info = info + @inc_info,
        warn = warn + @inc_warn,
        error = error + @inc_error,
        request = request + @inc_request,
        updated_at_ms = @updated_at_ms
      WHERE singleton_id = 1
    `),
    insertLog: _logDb.prepare(`
      INSERT INTO runtime_logs (timestamp_ms, level, message, meta_json)
      VALUES (@timestamp_ms, @level, @message, @meta_json)
    `),
    countLogs: _logDb.prepare('SELECT COUNT(1) AS total FROM runtime_logs'),
    clearLogs: _logDb.prepare('DELETE FROM runtime_logs'),
  };

  _logStmt.ensureTotalsRow.run({ updated_at_ms: Date.now() });
  return _logDb;
}

function loadLogStatsFromDb() {
  ensureLogDb();
  var row = _logStmt.getTotals.get();
  if (!row) return Object.assign({}, LOG_DEFAULT_STATS);
  return normalizeLogStats(row);
}

function updateLogStatsInDb(stats) {
  ensureLogDb();
  var normalized = normalizeLogStats(stats);
  _logStmt.setTotals.run({
    total: normalized.total,
    info: normalized.info,
    warn: normalized.warn,
    error: normalized.error,
    request: normalized.request,
    updated_at_ms: Date.now(),
  });
  logStats = normalized;
}

function loadRecentLogsFromDb(limit) {
  ensureLogDb();
  var maxRows = Math.max(1, Math.min(LOG_BUFFER_MAX, toInt(limit, LOG_BUFFER_MAX) || LOG_BUFFER_MAX));
  var rows = _logDb.prepare(`
    SELECT timestamp_ms, level, message, meta_json
    FROM runtime_logs
    ORDER BY timestamp_ms DESC, id DESC
    LIMIT ?
  `).all(maxRows);
  var out = [];
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i] || {};
    out.push({
      timestamp: toInt(row.timestamp_ms, 0),
      level: normalizeLogLevel(row.level),
      message: String(row.message || ''),
      meta: safeParseJsonObjectOrNull(row.meta_json),
    });
  }
  return out;
}

function queryLogsFromDb(filters) {
  ensureLogDb();
  var options = filters && typeof filters === 'object' ? filters : {};
  var whereList = [];
  var params = {};
  var level = String(options.level || '').trim().toLowerCase();
  if (level) {
    whereList.push('level = @level');
    params.level = normalizeLogLevel(level);
  }
  var search = String(options.search || '').trim().toLowerCase();
  if (search) {
    whereList.push('LOWER(message) LIKE @search');
    params.search = '%' + search + '%';
  }

  var whereSql = whereList.length > 0 ? (' WHERE ' + whereList.join(' AND ')) : '';
  var offset = Math.max(0, toInt(options.offset, 0));
  var limit = Math.max(1, Math.min(200000, toInt(options.limit, 50) || 50));

  var totalRow = _logDb.prepare('SELECT COUNT(1) AS total FROM runtime_logs' + whereSql).get(params);
  var sql = ''
    + 'SELECT timestamp_ms, level, message, meta_json '
    + 'FROM runtime_logs'
    + whereSql
    + ' ORDER BY timestamp_ms DESC, id DESC'
    + ' LIMIT @limit OFFSET @offset';
  var rows = _logDb.prepare(sql).all(Object.assign({}, params, { limit: limit, offset: offset }));

  var result = [];
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i] || {};
    result.push({
      timestamp: toInt(row.timestamp_ms, 0),
      level: normalizeLogLevel(row.level),
      message: String(row.message || ''),
      meta: safeParseJsonObjectOrNull(row.meta_json),
    });
  }
  return {
    logs: result,
    total: toInt(totalRow && totalRow.total, 0),
  };
}

/**
 * 初始化日志持久化
 */
export function initLogPersistence(dataDir) {
  void dataDir;
  ensureLogDb();
  logBuffer = loadRecentLogsFromDb(LOG_BUFFER_MAX);
  logStats = loadLogStatsFromDb();
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
    ensureLogDb();
    var entry = normalizeLogEntry({
      timestamp: Date.now(),
      level: level,
      message: message,
      meta: meta || null,
    });

    _logStmt.insertLog.run({
      timestamp_ms: entry.timestamp,
      level: entry.level,
      message: entry.message,
      meta_json: JSON.stringify(entry.meta || {}),
    });
    _logStmt.incrementTotals.run({
      inc_info: entry.level === 'info' ? 1 : 0,
      inc_warn: entry.level === 'warn' ? 1 : 0,
      inc_error: entry.level === 'error' ? 1 : 0,
      inc_request: entry.level === 'request' ? 1 : 0,
      updated_at_ms: entry.timestamp,
    });

    logStats = loadLogStatsFromDb();
    if (logBuffer.length >= LOG_BUFFER_MAX) {
      logBuffer.pop();
    }
    logBuffer.unshift(entry);
  },

  /**
   * 获取日志（支持过滤）
   * @param {{ level?: string, search?: string, limit?: number, offset?: number }} filters
   * @returns {{ logs: Array, total: number }}
   */
  getAll: function (filters) {
    return queryLogsFromDb(filters || {});
  },

  /**
   * 获取统计
   * @returns {{ total: number, info: number, warn: number, error: number, request: number }}
   */
  getStats: function () {
    logStats = loadLogStatsFromDb();
    return Object.assign({}, logStats);
  },

  /**
   * 清空日志
   */
  clear: function () {
    ensureLogDb();
    _logStmt.clearLogs.run();
    updateLogStatsInDb(LOG_DEFAULT_STATS);
    logBuffer = [];
  },

  forceSave: function () {
    // SQLite 即时持久化，无需额外 flush
    return;
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

function isCredentialsApiTokenAuthorized(req, config) {
  var apiToken = config && config.credentials && config.credentials.api_token;
  if (!apiToken) {
    return { ok: false, reason: 'not_configured' };
  }
  var token = extractBearerToken(req.headers['authorization'] || '');
  if (!token || token !== apiToken) {
    return { ok: false, reason: 'unauthorized' };
  }
  return { ok: true, mode: 'credentials_api_token' };
}

function requireCredentialsApiToken(req, res, config, t) {
  var auth = isCredentialsApiTokenAuthorized(req, config);
  if (auth.ok) return auth;
  if (auth.reason === 'not_configured') {
    logCollector.add('error', '凭证 API 未配置 api_token', {
      operation: 'credentials_api_auth',
      status: 'failed',
      reason: 'not_configured',
    });
    jsonResponse(res, 503, { error: t('credentials.not_configured') });
    return null;
  }
  logCollector.add('warn', '凭证 API 认证失败', {
    operation: 'credentials_api_auth',
    status: 'failed',
    reason: 'unauthorized',
  });
  jsonResponse(res, 401, { error: t('credentials.unauthorized') });
  return null;
}

function authorizeAdminSessionOrCredentialsToken(req, config) {
  var token = extractBearerToken(req.headers['authorization'] || '');
  if (!token) return { ok: false, reason: 'unauthorized' };

  var credentialsToken = config && config.credentials && config.credentials.api_token;
  if (credentialsToken && token === credentialsToken) {
    return { ok: true, mode: 'credentials_api_token' };
  }

  if (isSessionValid(token, config)) {
    return { ok: true, mode: 'admin_session' };
  }

  return { ok: false, reason: 'unauthorized' };
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

function getEventRepository(pool) {
  if (!pool || typeof pool !== 'object') return null;
  if (typeof pool.getRepository === 'function') {
    try {
      var repo = pool.getRepository();
      if (repo) return repo;
    } catch (_) {}
  }
  if (pool.repository) return pool.repository;
  return pool._repository || null;
}

function hasPoolRepository(pool) {
  return !!getEventRepository(pool);
}

function toFiniteEventInt(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function normalizeEventDetail(payload) {
  if (payload === null || payload === undefined) return {};
  if (typeof payload === 'string') {
    var text = payload.trim();
    if (!text) return {};
    try {
      var parsedText = JSON.parse(text);
      if (parsedText && typeof parsedText === 'object' && !Array.isArray(parsedText)) {
        return parsedText;
      }
    } catch (_) {}
    return { message: truncateErrorString(text) };
  }
  if (typeof payload === 'object' && !Array.isArray(payload)) {
    return payload;
  }
  return { value: payload };
}

function buildAccountEvent(email, eventType, options) {
  var opts = options || {};
  var rawStatusCode = opts.statusCode;
  var statusCode = null;
  if (rawStatusCode !== null && rawStatusCode !== undefined && rawStatusCode !== '') {
    statusCode = toFiniteEventInt(rawStatusCode, 0);
  }
  var detail = normalizeEventDetail(opts.detail !== undefined ? opts.detail : opts.payload);
  if (statusCode !== null && detail.status_code === undefined) detail.status_code = statusCode;
  if (opts.errorType && detail.error_type === undefined) detail.error_type = String(opts.errorType);
  var oldStatus = opts.oldStatus === undefined ? (detail.from_status !== undefined ? detail.from_status : null) : opts.oldStatus;
  var newStatus = opts.newStatus === undefined ? (detail.to_status !== undefined ? detail.to_status : null) : opts.newStatus;
  return {
    email: String(email || ''),
    event_type: String(eventType || ''),
    old_status: oldStatus === null || oldStatus === undefined ? null : String(oldStatus),
    new_status: newStatus === null || newStatus === undefined ? null : String(newStatus),
    detail: detail,
    status_code: statusCode,
    error_type: opts.errorType ? String(opts.errorType) : null,
    payload_json: detail,
    created_at_ms: toFiniteEventInt(opts.createdAtMs, Date.now()),
  };
}

async function appendAccountEvent(pool, event) {
  if (!event || !event.email || !event.event_type) return false;

  if (pool && typeof pool.appendEvent === 'function') {
    try {
      await pool.appendEvent(event);
      return true;
    } catch (e1) {
      log('⚠️', C.yellow, 'appendEvent(pool) 失败: ' + (e1 && e1.message ? e1.message : String(e1)));
    }
  }

  var repository = getEventRepository(pool);
  if (!repository) return false;

  if (typeof repository.appendEvent === 'function') {
    try {
      await repository.appendEvent(event);
      return true;
    } catch (e2) {
      log('⚠️', C.yellow, 'appendEvent(repository) 失败: ' + (e2 && e2.message ? e2.message : String(e2)));
    }
  }

  if (repository._db && typeof repository._db.prepare === 'function') {
    try {
      if (!repository._appendEventStmt) {
        repository._appendEventStmt = repository._db.prepare(
          'INSERT INTO account_events (email, event_type, old_status, new_status, detail, status_code, error_type, payload_json, created_at_ms) '
          + 'VALUES (@email, @event_type, @old_status, @new_status, @detail, @status_code, @error_type, @payload_json, @created_at_ms)'
        );
      }
      var detailJson = JSON.stringify(normalizeEventDetail(event.detail !== undefined ? event.detail : event.payload_json));
      repository._appendEventStmt.run({
        email: String(event.email || ''),
        event_type: String(event.event_type || ''),
        old_status: event.old_status === undefined || event.old_status === null ? null : String(event.old_status),
        new_status: event.new_status === undefined || event.new_status === null ? null : String(event.new_status),
        detail: detailJson,
        status_code: event.status_code === null || event.status_code === undefined ? null : toFiniteEventInt(event.status_code, 0),
        error_type: event.error_type ? String(event.error_type) : null,
        payload_json: detailJson,
        created_at_ms: toFiniteEventInt(event.created_at_ms, Date.now()),
      });
      return true;
    } catch (e3) {
      log('⚠️', C.yellow, 'appendEvent(SQLite fallback) 失败: ' + (e3 && e3.message ? e3.message : String(e3)));
    }
  }

  return false;
}

async function appendAccountEvents(pool, events) {
  var list = Array.isArray(events) ? events.filter(Boolean) : [];
  if (list.length === 0) return 0;

  if (pool && typeof pool.appendEventBatch === 'function') {
    try {
      await pool.appendEventBatch(list);
      return list.length;
    } catch (e1) {
      log('⚠️', C.yellow, 'appendEventBatch(pool) 失败: ' + (e1 && e1.message ? e1.message : String(e1)));
    }
  }

  var repository = getEventRepository(pool);
  if (repository && typeof repository.appendEventBatch === 'function') {
    try {
      await repository.appendEventBatch(list);
      return list.length;
    } catch (e2) {
      log('⚠️', C.yellow, 'appendEventBatch(repository) 失败: ' + (e2 && e2.message ? e2.message : String(e2)));
    }
  }

  var written = 0;
  for (var i = 0; i < list.length; i++) {
    if (await appendAccountEvent(pool, list[i])) written++;
  }
  return written;
}

/**
 * 解析 URL 路径参数
 * /admin/api/accounts/foo@bar.com/action → "foo@bar.com"
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

function extractAbuseUserHistoryId(path) {
  var match = path.match(/^\/admin\/api\/abuse\/user\/([^/]+)\/history$/);
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
  var totalRaw = String(query.total || '').trim().toLowerCase();
  var modeRaw = String(query.mode || '').trim().toLowerCase();
  var presetRaw = String(query.preset || '').trim().toLowerCase();
  var total = totalRaw === '1'
    || totalRaw === 'true'
    || totalRaw === 'yes'
    || modeRaw === 'total'
    || presetRaw === 'total';
  if (total) {
    return { mode: 'total' };
  }

  if (query.hours) {
    var h = parseInt(query.hours, 10);
    if (h > 0 && h <= 720) return { mode: 'hours', hours: h };
  }

  var from = query.from;
  var to = query.to;
  var hasFrom = _isValidDateStr(from);
  var hasTo = _isValidDateStr(to);
  if (hasFrom || hasTo) {
    if (!hasFrom) from = to;
    if (!hasTo) to = from;
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
  ensureAdminSessionDb();

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
      return await handleListAccounts(req, res, pool);
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

    // ====== POST /admin/api/credentials/import/gpa ======
    if (method === 'POST' && path === '/admin/api/credentials/import/gpa') {
      return await handleGpaCredentialsImport(req, res, ctx, {
        enforceCredentialsToken: false,
        authMode: 'admin_session',
      });
    }

    // ====== GET /admin/api/credentials/export/gpa ======
    if (method === 'GET' && path === '/admin/api/credentials/export/gpa') {
      return await handleGpaCredentialsExport(req, res, ctx, {
        skipAuthCheck: true,
        authMode: 'admin_session',
      });
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
          rLimit = Math.min(parsedLimit, 200);
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
    var abuseUserHistoryId = extractAbuseUserHistoryId(path);
    if (method === 'GET' && abuseUserHistoryId) {
      return handleAbuseUserHistory(req, res, ctx, abuseUserHistoryId);
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
    if (method === 'GET' && path === '/admin/api/account-health/status') {
      return handleAccountHealthStatus(res, ctx);
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

function createAdminSession(username, clientIp, config) {
  cleanupSessions(config);
  var token = generateSessionToken();
  var now = Date.now();
  var maxAgeMs = getSessionMaxAgeMs(config);
  ensureAdminSessionDb();
  _adminSessionStmt.upsert.run({
    session_hash: hashAdminSessionToken(token),
    username: String(username || ''),
    created_at_ms: now,
    expires_at_ms: now + maxAgeMs,
    ip: normalizeClientIp(clientIp),
    updated_at_ms: now,
  });
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
  var expectedUsername = (config.server && config.server.admin_username) || '';
  if (!expectedUsername) {
    return jsonResponse(res, 503, { error: 'admin_username_not_configured' });
  }
  var usernameMatch = safeCompare(username, expectedUsername);
  var totp = getTotpSettings(config);

  // TOTP 未启用时保持原行为：仅用户名 + 密码
  if (!totp.enabled) {
    var passwordMatch = safeCompare(password, expectedPassword);
    if (usernameMatch && passwordMatch) {
      var legacyToken = createAdminSession(username, clientIp, config);
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
      var totpOnlyToken = createAdminSession(username, clientIp, config);
      clearAdminLoginFailures(clientIp);
      logCollector.add('info', 'Admin login success (totp_only): ' + username);
      return jsonResponse(res, 200, { token: totpOnlyToken });
    }
  } else if (mode === 'password_totp') {
    var passwordTotpMatch = safeCompare(password, expectedPassword);
    if (usernameMatch && passwordTotpMatch && totpMatch) {
      var passwordTotpToken = createAdminSession(username, clientIp, config);
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
  var accountName = (config.server && config.server.admin_username) || '';
  if (!accountName) {
    return jsonResponse(res, 503, { error: 'admin_username_not_configured' });
  }
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
  var rawStats = pool && typeof pool.getStats === 'function' ? pool.getStats() : {};
  var data = {
    accounts: normalizeAccountStatsShape(rawStats, 0),
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
function applyAccountListFilters(accounts, statusFilter, searchFilter) {
  var filtered = Array.isArray(accounts) ? accounts : [];
  if (statusFilter && statusFilter !== 'all') {
    filtered = filtered.filter(function (a) {
      var status = String(a && a.status || '').toLowerCase();
      return status === statusFilter;
    });
  }
  if (searchFilter) {
    filtered = filtered.filter(function (a) {
      var email = String(a && a.email || '').toLowerCase();
      return email.indexOf(searchFilter) >= 0;
    });
  }
  return filtered;
}

function describeAccountListPayload(result) {
  if (Array.isArray(result)) {
    var statuses = [];
    for (var i = 0; i < result.length && i < 5; i++) {
      statuses.push(String(result[i] && result[i].status || ''));
    }
    return {
      kind: 'array',
      length: result.length,
      sample_statuses: statuses.join(','),
    };
  }
  if (!result || typeof result !== 'object') {
    return { kind: typeof result };
  }
  var rows = Array.isArray(result.accounts) ? result.accounts : [];
  var rowStatuses = [];
  for (var j = 0; j < rows.length && j < 5; j++) {
    rowStatuses.push(String(rows[j] && rows[j].status || ''));
  }
  return {
    kind: 'paged',
    length: rows.length,
    total: toFiniteEventInt(result.total, rows.length),
    page: toFiniteEventInt(result.page, 1),
    limit: toFiniteEventInt(result.limit, 50),
    sample_statuses: rowStatuses.join(','),
  };
}

function normalizeAccountStatsShape(rawStats, fallbackTotal) {
  var input = rawStats && typeof rawStats === 'object' ? rawStats : {};
  var totalValue = Math.max(0, toFiniteEventInt(input.total, fallbackTotal || 0));
  var coolingValue = Math.max(0, toFiniteEventInt(input.cooling, toFiniteEventInt(input.cooldown, 0)));
  return {
    total: totalValue,
    active: Math.max(0, toFiniteEventInt(input.active, 0)),
    cooling: coolingValue,
    cooldown: coolingValue,
    banned: Math.max(0, toFiniteEventInt(input.banned, 0)),
    expired: Math.max(0, toFiniteEventInt(input.expired, 0)),
    wasted: Math.max(0, toFiniteEventInt(input.wasted, 0)),
  };
}

async function resolveAccountListStats(pool, fallbackTotal) {
  var memoryStats = pool && typeof pool.getStats === 'function' ? pool.getStats() : {};
  return normalizeAccountStatsShape(memoryStats, fallbackTotal);
}

async function handleListAccounts(req, res, pool) {
  var query = parseQuery(req.url || '');
  var page = parseInt(query.page || '1', 10) || 1;
  var limit = parseInt(query.limit || '50', 10) || 50;
  var statusFilter = String(query.status || '').trim().toLowerCase();
  var searchFilter = String(query.search || '').trim().toLowerCase();
  var debugPaging = String(query.debug_accounts || query.debug || '').trim() === '1';
  if (page < 1) page = 1;
  if (limit < 1) limit = 1;
  if (limit > 200000) limit = 200000;

  var pageItems = [];
  var total = 0;
  var resolvedPage = page;
  var resolvedLimit = limit;
  var pages = 1;
  var usedDbResult = false;
  var useRepositoryPaging = !!(pool && typeof pool.listAccounts === 'function');
  var sourceList = null;
  var repositoryAttached = hasPoolRepository(pool);

  if (debugPaging) {
    console.log('[admin/accounts] paging-debug:init', {
      useRepositoryPaging: useRepositoryPaging,
      hasPoolRepository: repositoryAttached,
      storageMode: pool && pool._storageMode ? pool._storageMode : '',
      hasPoolRepositoryField: !!(pool && pool._repository),
      requestPage: page,
      requestLimit: limit,
      statusFilter: statusFilter,
      hasSearch: !!searchFilter,
    });
    if (useRepositoryPaging) {
      try {
        var probeResult = await Promise.resolve(pool.listAccounts({
          page: 1,
          limit: 5,
        }));
        console.log('[admin/accounts] paging-debug:probe', describeAccountListPayload(probeResult));
      } catch (probeErr) {
        console.log('[admin/accounts] paging-debug:probe-error', probeErr && probeErr.message ? probeErr.message : String(probeErr));
      }
    }
  }

  if (useRepositoryPaging) {
    try {
      var dbResult = await Promise.resolve(pool.listAccounts({
        status: statusFilter && statusFilter !== 'all' ? statusFilter : '',
        search: searchFilter,
        page: page,
        limit: limit,
      }));
      if (dbResult && typeof dbResult === 'object' && Array.isArray(dbResult.accounts)) {
        pageItems = dbResult.accounts;
        total = toFiniteEventInt(dbResult.total, pageItems.length);
        if (total < 0) total = pageItems.length;
        resolvedPage = Math.max(1, toFiniteEventInt(dbResult.page, page));
        resolvedLimit = Math.max(1, toFiniteEventInt(dbResult.limit, limit));
        pages = Math.max(1, toFiniteEventInt(
          dbResult.pages,
          Math.ceil(Math.max(total, 0) / resolvedLimit)
        ));
        if (resolvedPage > pages) resolvedPage = pages;
        usedDbResult = true;
        if (debugPaging) {
          console.log('[admin/accounts] paging-debug:result', describeAccountListPayload(dbResult));
        }
      } else if (Array.isArray(dbResult)) {
        sourceList = dbResult;
        if (debugPaging) {
          console.log('[admin/accounts] paging-debug:array-fallback', describeAccountListPayload(dbResult));
        }
      }
    } catch (err) {
      log('⚠️', C.yellow, '管理端 DB 分页回退到内存过滤: ' + (err && err.message ? err.message : String(err)));
    }
  }

  if (!usedDbResult && sourceList === null) {
    var fallbackResult = await Promise.resolve(pool.listAccounts());
    if (Array.isArray(fallbackResult)) {
      sourceList = fallbackResult;
    } else if (fallbackResult && typeof fallbackResult === 'object' && Array.isArray(fallbackResult.accounts)) {
      if (typeof pool._allAccountsArray === 'function' && typeof pool._toListAccountSummary === 'function') {
        var rawAll = pool._allAccountsArray();
        var recovered = [];
        for (var ai = 0; ai < rawAll.length; ai++) {
          recovered.push(pool._toListAccountSummary(rawAll[ai]));
        }
        sourceList = recovered;
      } else {
        sourceList = fallbackResult.accounts;
      }
    } else {
      sourceList = [];
    }
  }

  if (sourceList !== null) {
    var filtered = applyAccountListFilters(sourceList, statusFilter, searchFilter);
    total = filtered.length;
    pages = Math.max(1, Math.ceil(total / resolvedLimit));
    if (resolvedPage > pages) resolvedPage = pages;
    var start = (resolvedPage - 1) * resolvedLimit;
    pageItems = filtered.slice(start, start + resolvedLimit);
  }

  // listAccounts 已经不含 accessToken，但以防万一做一层脱敏
  var masked = pageItems.map(function (a) {
    return Object.assign({}, a, {
      accessToken: a && a.accessToken ? maskToken(a.accessToken) : undefined,
    });
  });
  var stats = await resolveAccountListStats(pool, total);
  var lifespan = pool.getLifespanStats ? pool.getLifespanStats() : null;
  var hasMore = resolvedPage < pages;
  return jsonResponse(res, 200, {
    accounts: masked,
    total: total,
    page: resolvedPage,
    pages: pages,
    limit: resolvedLimit,
    hasMore: hasMore,
    stats: stats,
    lifespan: lifespan,
  });
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

  var beforeDeleteAccount = pool.getFullAccount ? pool.getFullAccount(email) : null;
  var removed = pool.removeAccount(email);
  if (!removed) {
    return jsonResponse(res, 404, { error: t('admin.account_not_found', { email: email }) });
  }
  await appendAccountEvent(pool, buildAccountEvent(email, 'manual_delete', {
    statusCode: 0,
    payload: {
      source: 'admin_api',
      previous_status: beforeDeleteAccount && beforeDeleteAccount.status
        ? String(beforeDeleteAccount.status)
        : String((removed && removed.status) || ''),
    },
  }));
  log('🗑️', C.yellow, t('accounts.deleted', { email: email }));
  logCollector.add('info', t('accounts.deleted', { email: email }));
  return jsonResponse(res, 200, { message: t('accounts.deleted', { email: email }), email: email });
}

/**
 * 导出账号 — 完整数据（含完整 token）
 */
function handleExportAccounts(res, pool) {
  var data = pool.exportAccounts();
  var sanitized = Array.isArray(data)
    ? data.map(function (item) {
      var next = Object.assign({}, item || {});
      delete next.account_id;
      delete next.accountId;
      delete next.last_error;
      delete next.lastError;
      return next;
    })
    : [];
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Disposition': 'attachment; filename="accounts-export.json"',
  });
  res.end(JSON.stringify(sanitized, null, 2));
}

function parseRfc3339ToUnixSeconds(value) {
  if (typeof value !== 'string' || !value.trim()) return 0;
  var ts = Date.parse(value);
  if (!isFinite(ts) || ts <= 0) return 0;
  return Math.floor(ts / 1000);
}

function buildGpaExportFileName(email) {
  var safeEmail = String(email || 'unknown').replace(/[^A-Za-z0-9@._+-]/g, '_');
  return 'codex-' + safeEmail + '.json';
}

function createGpaRejectError(code, message) {
  return {
    code: String(code || 'invalid_credential'),
    message: String(message || '凭证格式无效'),
  };
}

function convertGpaCredential(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return {
      ok: false,
      email: '',
      warnings: [],
      error: createGpaRejectError('invalid_credential_format', '凭证内容必须是 JSON 对象'),
      converted: null,
    };
  }

  var provider = String(raw.type || '').trim().toLowerCase();
  if (provider !== 'codex') {
    return {
      ok: false,
      email: String(raw.email || '').trim(),
      warnings: [],
      error: createGpaRejectError('unsupported_provider', '仅支持 type=codex 的 GPA 凭证'),
      converted: null,
    };
  }

  var email = String(raw.email || '').trim();
  var accessToken = String(raw.access_token || raw.accessToken || '').trim();
  if (!email || !accessToken) {
    return {
      ok: false,
      email: email,
      warnings: [],
      error: createGpaRejectError('missing_required_fields', '缺少必填字段 email 或 access_token'),
      converted: null,
    };
  }

  var converted = {
    email: email,
    accessToken: accessToken,
  };

  var warnings = [];
  var sessionToken = raw.session_token || raw.sessionToken || '';
  if (sessionToken) {
    converted.sessionToken = String(sessionToken);
  } else {
    warnings.push('missing_session_token: token refresh unavailable');
  }

  if (raw.cookies && typeof raw.cookies === 'object' && !Array.isArray(raw.cookies)) {
    converted.cookies = raw.cookies;
  }

  if (typeof raw.password === 'string' && raw.password) {
    converted.password = raw.password;
  }

  var tokenExpiresAt = parseRfc3339ToUnixSeconds(raw.expired);
  if (tokenExpiresAt > 0) {
    converted.token_expires_at = tokenExpiresAt;
  }

  return {
    ok: true,
    email: email,
    warnings: warnings,
    error: null,
    converted: converted,
  };
}

function normalizeGpaImportItems(body) {
  var dryRun = false;
  var items = [];

  if (Array.isArray(body)) {
    for (var i = 0; i < body.length; i++) {
      items.push({ file: 'inline', raw: body[i] });
    }
    return { ok: true, dryRun: dryRun, items: items, error: null };
  }

  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return {
      ok: false,
      dryRun: false,
      items: [],
      error: createGpaRejectError('invalid_body', '请求体必须是数组或对象'),
    };
  }

  dryRun = body.dryRun === true;

  if (Array.isArray(body.files)) {
    for (var f = 0; f < body.files.length; f++) {
      var file = body.files[f];
      if (!file || typeof file !== 'object' || Array.isArray(file)) {
        items.push({ file: 'file-' + (f + 1), raw: null });
        continue;
      }
      var fileName = String(file.name || ('file-' + (f + 1) + '.json'));
      var content = file.content;
      if (Array.isArray(content)) {
        for (var j = 0; j < content.length; j++) {
          items.push({ file: fileName, raw: content[j] });
        }
      } else {
        items.push({ file: fileName, raw: content });
      }
    }
    if (items.length === 0) {
      return {
        ok: false,
        dryRun: dryRun,
        items: [],
        error: createGpaRejectError('empty_files', 'files 为空或未包含可导入凭证'),
      };
    }
    return { ok: true, dryRun: dryRun, items: items, error: null };
  }

  if (body.content && (Array.isArray(body.content) || typeof body.content === 'object')) {
    var contentSource = body.content;
    if (Array.isArray(contentSource)) {
      for (var c = 0; c < contentSource.length; c++) {
        items.push({ file: String(body.name || 'inline'), raw: contentSource[c] });
      }
    } else {
      items.push({ file: String(body.name || 'inline'), raw: contentSource });
    }
    return { ok: true, dryRun: dryRun, items: items, error: null };
  }

  if (body.type || body.email || body.access_token || body.accessToken) {
    items.push({ file: 'inline', raw: body });
    return { ok: true, dryRun: dryRun, items: items, error: null };
  }

  return {
    ok: false,
    dryRun: dryRun,
    items: [],
    error: createGpaRejectError('invalid_body', '请求体未包含 files 或 GPA 凭证数组'),
  };
}

function toGpaImportErrorResponse(err) {
  return {
    code: err && err.code ? String(err.code) : 'import_failed',
    message: err && err.message ? String(err.message) : '导入失败',
  };
}

function logGpaImportDetail(level, meta) {
  var email = String(meta && meta.email || '');
  var status = String(meta && meta.status || '');
  var reason = String(meta && meta.reason || '');
  var message = 'GPA 凭证导入 [' + status + '] ' + (email || '(unknown)') + (reason ? (' - ' + reason) : '');
  logCollector.add(level, message, meta || {});
  if (level === 'error') {
    log('❌', C.red, message);
  } else if (level === 'warn') {
    log('⚠️', C.yellow, message);
  } else {
    log('🧾', C.cyan, message);
  }
}

async function handleGpaCredentialsImport(req, res, ctx, options) {
  options = options || {};
  var config = (ctx && ctx.config) || {};
  var t = (ctx && ctx.t) || function (key) { return key; };
  var pool = ctx && ctx.pool;
  var authMode = String(options.authMode || '');

  if (!pool || typeof pool.addAccount !== 'function') {
    return jsonResponse(res, 500, { error: { code: 'pool_unavailable', message: '账号池不可用' } });
  }

  if (options.enforceCredentialsToken !== false) {
    var tokenAuth = requireCredentialsApiToken(req, res, config, t);
    if (!tokenAuth) return;
    authMode = tokenAuth.mode;
  }

  var body;
  try {
    body = await readBody(req);
  } catch (_) {
    return jsonResponse(res, 400, { error: { code: 'invalid_body', message: t('credentials.invalid_body') } });
  }

  var normalized = normalizeGpaImportItems(body);
  if (!normalized.ok) {
    return jsonResponse(res, 400, { error: normalized.error || createGpaRejectError('invalid_body', t('credentials.invalid_body')) });
  }

  var dryRun = normalized.dryRun === true;
  var items = normalized.items || [];

  var imported = 0;
  var updated = 0;
  var rejected = 0;
  var details = [];

  var existing = {};
  var _rawCurrent = pool.listAccounts ? await pool.listAccounts() : [];
  var currentAccounts = Array.isArray(_rawCurrent) ? _rawCurrent : (_rawCurrent && _rawCurrent.accounts || []);
  for (var i = 0; i < currentAccounts.length; i++) {
    var currentEmail = String(currentAccounts[i] && currentAccounts[i].email || '').trim();
    if (currentEmail) existing[currentEmail] = true;
  }

  for (var j = 0; j < items.length; j++) {
    var item = items[j] || {};
    var converted = convertGpaCredential(item.raw);
    if (!converted.ok) {
      rejected++;
      var rejectDetail = {
        email: converted.email || '',
        status: 'rejected',
        error: toGpaImportErrorResponse(converted.error),
      };
      details.push(rejectDetail);
      logGpaImportDetail('warn', {
        operation: 'credentials_import_gpa',
        auth_mode: authMode || '',
        dry_run: dryRun,
        file: String(item.file || ''),
        email: rejectDetail.email,
        status: 'rejected',
        reason: rejectDetail.error.code,
      });
      continue;
    }

    var email = converted.email;
    var willUpdate = !!existing[email];
    var status = willUpdate ? 'updated' : 'imported';

    if (!dryRun) {
      try {
        pool.addAccount(converted.converted);
      } catch (err) {
        rejected++;
        var importError = toGpaImportErrorResponse({
          code: 'import_failed',
          message: (err && err.message) || '写入账号池失败',
        });
        details.push({
          email: email,
          status: 'rejected',
          error: importError,
        });
        logGpaImportDetail('error', {
          operation: 'credentials_import_gpa',
          auth_mode: authMode || '',
          dry_run: false,
          file: String(item.file || ''),
          email: email,
          status: 'rejected',
          reason: importError.message,
        });
        continue;
      }
    }

    if (willUpdate) {
      updated++;
    } else {
      imported++;
      existing[email] = true;
    }

    var detail = {
      email: email,
      status: status,
    };
    if (converted.warnings && converted.warnings.length > 0) {
      detail.warnings = converted.warnings.slice();
    }
    details.push(detail);
    logGpaImportDetail(converted.warnings && converted.warnings.length > 0 ? 'warn' : 'info', {
      operation: 'credentials_import_gpa',
      auth_mode: authMode || '',
      dry_run: dryRun,
      file: String(item.file || ''),
      email: email,
      status: status,
      reason: (converted.warnings && converted.warnings.join('; ')) || '',
    });
  }

  logCollector.add('info', 'GPA 凭证导入完成', {
    operation: 'credentials_import_gpa',
    auth_mode: authMode || '',
    dry_run: dryRun,
    imported: imported,
    updated: updated,
    rejected: rejected,
    total: items.length,
  });

  return jsonResponse(res, 200, {
    imported: imported,
    updated: updated,
    rejected: rejected,
    details: details,
  });
}

async function handleGpaCredentialsExport(req, res, ctx, options) {
  options = options || {};
  var config = (ctx && ctx.config) || {};
  var pool = ctx && ctx.pool;
  var authMode = String(options.authMode || '');

  if (!pool || typeof pool.listAccounts !== 'function' || typeof pool.getFullAccount !== 'function') {
    return jsonResponse(res, 500, { error: { code: 'pool_unavailable', message: '账号池不可用' } });
  }

  if (options.skipAuthCheck !== true) {
    var auth = authorizeAdminSessionOrCredentialsToken(req, config);
    if (!auth.ok) {
      logCollector.add('warn', 'GPA 凭证导出认证失败', {
        operation: 'credentials_export_gpa',
        status: 'failed',
        reason: 'unauthorized',
      });
      return jsonResponse(res, 401, { error: { code: 'unauthorized', message: '凭证导出认证失败' } });
    }
    authMode = auth.mode;
  }

  var query = parseQuery(req.url || '');
  var statusFilter = String(query.status || 'active').trim().toLowerCase() || 'active';
  var _rawList = await pool.listAccounts();
  var list = Array.isArray(_rawList) ? _rawList : (_rawList && _rawList.accounts || []);
  var files = [];

  for (var i = 0; i < list.length; i++) {
    var account = list[i];
    var status = String(account && account.status || '').toLowerCase();
    if (statusFilter && status !== statusFilter) continue;

    var full = pool.getFullAccount(account.email);
    if (!full || !full.accessToken) {
      logCollector.add('warn', 'GPA 凭证导出跳过（缺少 accessToken）', {
        operation: 'credentials_export_gpa',
        auth_mode: authMode || '',
        status: 'skipped',
        reason: 'missing_access_token',
        email: String(account && account.email || ''),
      });
      continue;
    }

    var expiresAt = Number(full.token_expires_at || 0);
    var expiredRfc3339 = '';
    if (isFinite(expiresAt) && expiresAt > 0) {
      expiredRfc3339 = new Date(expiresAt * 1000).toISOString();
    }

    var email = String(full.email || account.email || '').trim();
    var content = {
      type: 'codex',
      email: email,
      access_token: String(full.accessToken || ''),
      expired: expiredRfc3339,
    };

    files.push({
      name: buildGpaExportFileName(email),
      content: content,
    });

    logCollector.add('info', 'GPA 凭证导出: ' + email, {
      operation: 'credentials_export_gpa',
      auth_mode: authMode || '',
      status: 'exported',
      reason: '',
      email: email,
      filter_status: statusFilter,
    });
  }

  logCollector.add('info', 'GPA 凭证导出完成', {
    operation: 'credentials_export_gpa',
    auth_mode: authMode || '',
    status: 'success',
    reason: '',
    count: files.length,
    filter_status: statusFilter,
  });

  return jsonResponse(res, 200, {
    count: files.length,
    files: files,
  });
}

export async function handleCredentialsImportGpaApi(req, res, ctx) {
  return handleGpaCredentialsImport(req, res, ctx, {
    enforceCredentialsToken: true,
  });
}

export async function handleCredentialsExportGpaApi(req, res, ctx) {
  return handleGpaCredentialsExport(req, res, ctx, {
    skipAuthCheck: false,
  });
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
  var _rawAccounts = await pool.listAccounts();
  var accounts = Array.isArray(_rawAccounts) ? _rawAccounts : (_rawAccounts && _rawAccounts.accounts || []);
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
  var initialStatus = String(foundAccount.status || '');

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
          await pool.flush();
          log('✅', C.green, '管理面板刷新成功: ' + email);
          return jsonResponse(res, 200, { ok: true, message: t('admin.action_success', { action: 'refresh', email: email }), refreshed: true });
        } else {
          var failCode = (refreshResult && refreshResult.statusCode) || 0;
          var failDetail = (refreshResult && (refreshResult.detail || refreshResult.error)) || 'unknown';
          var refreshErrorResult = pool.markError(email, failCode, failDetail);
          var statusAfterRefreshFail = pool.getFullAccount ? pool.getFullAccount(email) : null;
          await appendAccountEvent(pool, buildAccountEvent(email, 'manual_status_change', {
            statusCode: failCode,
            errorType: refreshErrorResult && refreshErrorResult.type ? refreshErrorResult.type : 'refresh_failed',
            payload: {
              source: 'admin_api',
              action: 'refresh',
              from_status: initialStatus,
              to_status: statusAfterRefreshFail && statusAfterRefreshFail.status
                ? String(statusAfterRefreshFail.status)
                : '',
              detail: truncateErrorString(failDetail),
            },
          }));
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
            await pool.flush();
            await appendAccountEvent(pool, buildAccountEvent(email, 'verify_fail', {
              statusCode: 401,
              errorType: 'no_session_token',
              payload: {
                source: 'admin_api',
                mode: 'single_verify',
                from_status: initialStatus,
                to_status: 'wasted',
                reason: 'no_session_token',
              },
            }));
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
          await pool.flush();
          log('✅', C.green, '账号验证成功: ' + email);
          return jsonResponse(res, 200, { success: true, verified: true, message: '账号有效，已恢复为活跃状态' });
        } else {
          var verifyAccountToUpdate = pool.getFullAccount(email);
          if (verifyAccountToUpdate) {
            verifyAccountToUpdate.last_error_type = 'verify_failed';
            verifyAccountToUpdate.last_error_code = (verifyResult && verifyResult.statusCode) || 401;
            pool.markWasted(email);
            await pool.flush();
            await appendAccountEvent(pool, buildAccountEvent(email, 'verify_fail', {
              statusCode: (verifyResult && verifyResult.statusCode) || 401,
              errorType: 'verify_failed',
              payload: {
                source: 'admin_api',
                mode: 'single_verify',
                from_status: initialStatus,
                to_status: 'wasted',
                detail: truncateErrorString((verifyResult && (verifyResult.detail || verifyResult.error)) || 'unknown'),
              },
            }));
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
  var _rawUpdated = await pool.listAccounts();
  var updatedAccounts = Array.isArray(_rawUpdated) ? _rawUpdated : (_rawUpdated && _rawUpdated.accounts || []);
  var updatedAccount = null;
  for (var j = 0; j < updatedAccounts.length; j++) {
    if (updatedAccounts[j].email === email) {
      updatedAccount = updatedAccounts[j];
      break;
    }
  }

  if (action === 'cooldown' || action === 'waste' || action === 'activate') {
    await appendAccountEvent(pool, buildAccountEvent(email, 'manual_status_change', {
      statusCode: 0,
      payload: {
        source: 'admin_api',
        action: action,
        from_status: initialStatus,
        to_status: updatedAccount && updatedAccount.status ? String(updatedAccount.status) : '',
      },
    }));
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
  ensureRegisterProxyConfig(proxy, config);
  return proxy;
}

var REGISTER_PROXY_SOCKS_HOST = process.env.REGISTER_PROXY_HOST || '127.0.0.1';
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

function getRegisterProxyUsername(config) {
  return (
    (config && config.register_proxy && config.register_proxy.username) ||
    process.env.REGISTER_PROXY_USERNAME ||
    ''
  );
}

function getRegisterProxyPassword(config) {
  return (
    (config && config.register_proxy && config.register_proxy.password) ||
    process.env.REGISTER_PROXY_PASSWORD ||
    ''
  );
}

function maskProxySecret(value) {
  var text = String(value || '');
  if (!text) return '';
  if (text.length <= 2) return text;
  return text.slice(0, 2) + '***';
}

function maskRegisterSocksUrlForLog(url) {
  var raw = String(url || '');
  if (!raw) return raw;
  try {
    var parsed = new URL(raw);
    if (!/^socks5h?:$/i.test(parsed.protocol)) {
      return raw;
    }
    if (!parsed.password) {
      return raw;
    }
    var masked = maskProxySecret(decodeURIComponent(parsed.password));
    var username = parsed.username ? decodeURIComponent(parsed.username) : '';
    var authPart = encodeURIComponent(username) + ':' + encodeURIComponent(masked) + '@';
    return parsed.protocol + '//' + authPart + parsed.host;
  } catch (_) {
    return raw;
  }
}

function buildRegisterSocksUrl(port, config) {
  var safePort = parseInt(port, 10);
  if (!safePort || safePort < 1 || safePort > 65535) safePort = REGISTER_PROXY_DEFAULT_PORT;
  var username = getRegisterProxyUsername(config);
  var password = getRegisterProxyPassword(config);
  return 'socks5://' + encodeURIComponent(username) + ':' +
    encodeURIComponent(password) + '@' +
    REGISTER_PROXY_SOCKS_HOST + ':' + safePort;
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

function buildRegisterPoolServerUrl(target, proxy, config) {
  var safeTarget = target === undefined || target === null ? 'all' : String(target).trim();
  if (!safeTarget) safeTarget = 'all';
  if (proxy) {
    // 直接用 target 作为 preset key 查端口
    var presetPort = resolvePresetPort(proxy, safeTarget);
    if (presetPort) return buildRegisterSocksUrl(presetPort, config);
    // target 可能是映射后的值（如 'jp', 'eu', '自建', '魔戒'），反查 preset key
    var originalKey = findPresetKeyByMappedTarget(safeTarget);
    if (originalKey) {
      var mappedPort = resolvePresetPort(proxy, originalKey);
      if (mappedPort) return buildRegisterSocksUrl(mappedPort, config);
    }
    // 用节点名查端口
    var nodePort = resolveNodePort(proxy, safeTarget);
    if (nodePort) return buildRegisterSocksUrl(nodePort, config);
  }
  return buildRegisterSocksUrl(REGISTER_PROXY_DEFAULT_PORT, config);
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

function buildRegisterProxyForwardPayload(proxy, registerProxy, config) {
  var target = resolveRegisterProxyTarget(proxy, registerProxy);
  var server = buildRegisterPoolServerUrl(target, proxy, config);
  return {
    enabled: !!(registerProxy && registerProxy.enabled),
    server: server,
    active_preset: registerProxy && registerProxy.active_preset ? String(registerProxy.active_preset) : '',
    target: target,
  };
}

function ensureRegisterProxyConfig(proxy, config) {
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
  registerProxy.server = buildRegisterPoolServerUrl(migrateTarget, proxy, config);
  return registerProxy;
}

function buildProxyServerUrl(proxy, port) {
  var host = proxy.host || '127.0.0.1';
  return 'socks5://' + host + ':' + port;
}

function formatProxyConfig(proxy, config) {
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
    password: proxy.password || '',
    active_preset: proxy.active_preset ? String(proxy.active_preset) : '',
    presets: proxy.presets || {},
    node_groups: proxy.node_groups || [],
    host: proxy.host || '127.0.0.1',
    register_proxy: {
      enabled: !!registerProxy.enabled,
      server: registerProxy.server || buildRegisterSocksUrl(REGISTER_PROXY_DEFAULT_PORT, config),
      active_preset: registerProxy.active_preset ? String(registerProxy.active_preset) : '',
      target: registerTarget || '',
      local_port: registerLocalPort,
    },
  };
}

function handleProxyPresets(res, config) {
  var proxy = ensureProxyConfigFromRegisterClient(config);
  return jsonResponse(res, 200, formatProxyConfig(proxy, config));
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
      registerProxy.server = buildRegisterSocksUrl(registerPresetPort, config);
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
      registerProxy.server = buildRegisterSocksUrl(registerPort, config);
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
    proxy: formatProxyConfig(nextProxy, config),
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

function enrichCallerIdentityRow(row, identity, resolvedProfile) {
  var target = row && typeof row === 'object' ? row : {};
  var rawIdentity = String(identity || '').trim();
  var resolved = resolvedProfile && typeof resolvedProfile === 'object' ? resolvedProfile : null;
  var canonicalIdentity = resolved && resolved.identity ? String(resolved.identity || '').trim() : rawIdentity;
  if (canonicalIdentity) {
    target.identity = canonicalIdentity;
    target.caller_identity = canonicalIdentity;
  }

  var discordUserId = resolved ? String(resolved.discord_user_id || '').trim() : '';
  var seqId = resolved ? String(resolved.seq_id || '').trim() : '';
  var username = resolved ? String(resolved.username || '').trim() : '';
  var displayName = resolved ? String(resolved.display_name || '').trim() : '';
  var avatarUrl = resolved ? String(resolved.avatar_url || '').trim() : '';
  if (discordUserId || seqId || username || displayName || avatarUrl) {
    if (discordUserId) target.discord_user_id = discordUserId;
    if (!target.seq_id && seqId) target.seq_id = seqId;
    if (!target.username && username) target.username = username;
    if (!target.display_name && displayName) target.display_name = displayName;
    if (!target.avatar_url && avatarUrl) target.avatar_url = avatarUrl;
    if (!target.display_name) {
      target.display_name = target.username || target.seq_id || discordUserId;
    }
    if (!target.username) {
      target.username = target.display_name || target.seq_id || discordUserId;
    }
  }

  return target;
}

function enrichStatsCallerRows(rows, userStore) {
  var list = Array.isArray(rows) ? rows : [];
  if (list.length === 0) return [];
  var discordIndex = buildDiscordUsersIndex(userStore);
  var enriched = [];
  for (var i = 0; i < list.length; i++) {
    var raw = list[i] && typeof list[i] === 'object' ? Object.assign({}, list[i]) : {};
    var identity = String(raw.identity || raw.caller_identity || raw.id || '').trim();
    var resolved = resolveDiscordProfileByIdentity(identity, discordIndex);
    enriched.push(enrichCallerIdentityRow(raw, identity, resolved));
  }
  return enriched;
}

function enrichStatsRecentPayload(payload, userStore) {
  var base = payload && typeof payload === 'object' ? payload : {};
  var list = Array.isArray(base.data) ? base.data : [];
  if (list.length === 0) {
    return {
      data: [],
      total: Math.max(0, toFiniteEventInt(base.total, 0)),
      page: Math.max(1, toFiniteEventInt(base.page, 1)),
      pages: Math.max(1, toFiniteEventInt(base.pages, 1)),
      limit: Math.max(1, toFiniteEventInt(base.limit, 50)),
    };
  }
  var discordIndex = buildDiscordUsersIndex(userStore);
  var enriched = [];
  for (var i = 0; i < list.length; i++) {
    var raw = list[i] && typeof list[i] === 'object' ? Object.assign({}, list[i]) : {};
    var identity = String(raw.caller_identity || raw.identity || '').trim();
    var resolved = resolveDiscordProfileByIdentity(identity, discordIndex);
    enriched.push(enrichCallerIdentityRow(raw, identity, resolved));
  }
  return {
    data: enriched,
    total: Math.max(0, toFiniteEventInt(base.total, enriched.length)),
    page: Math.max(1, toFiniteEventInt(base.page, 1)),
    pages: Math.max(1, toFiniteEventInt(base.pages, 1)),
    limit: Math.max(1, toFiniteEventInt(base.limit, 50)),
  };
}

function handleStatsOverview(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, {});
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getOverviewTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getOverviewLastHours(opts.hours));
  if (opts.mode === 'range') {
    return jsonResponse(res, 200, ctx.stats.getOverviewRange(opts.from, opts.to));
  }
  return jsonResponse(res, 200, ctx.stats.getOverview());
}

function handleStatsTimeseries(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getTimeseriesTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getTimeseriesLastHours(opts.hours));
  return jsonResponse(res, 200, ctx.stats.getTimeseriesRange(opts.from, opts.to));
}

function handleStatsModels(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getModelStatsTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getModelStatsLastHours(opts.hours));
  return jsonResponse(res, 200, ctx.stats.getModelStatsRange(opts.from, opts.to));
}

function handleStatsAccounts(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  if (opts.mode === 'total') return jsonResponse(res, 200, ctx.stats.getAccountStatsTotal());
  if (opts.mode === 'hours') return jsonResponse(res, 200, ctx.stats.getAccountStatsLastHours(opts.hours));
  return jsonResponse(res, 200, ctx.stats.getAccountStatsRange(opts.from, opts.to));
}

function handleStatsCallers(req, res, ctx) {
  if (!ctx.stats) return jsonResponse(res, 200, []);
  var opts = parseStatsOptions(req.url);
  var rows = [];
  if (opts.mode === 'total') rows = ctx.stats.getCallerStatsTotal();
  else if (opts.mode === 'hours') rows = ctx.stats.getCallerStatsLastHours(opts.hours);
  else rows = ctx.stats.getCallerStatsRange(opts.from, opts.to);
  var userStore = resolveDiscordUserStore(ctx);
  return jsonResponse(res, 200, enrichStatsCallerRows(rows, userStore));
}

function handleStatsRecent(res, ctx, page, limit, filter, search, source, date, hours) {
  var normalizedSearch = typeof search === 'string' ? search.trim() : '';
  var normalizedSource = source === 'file' ? 'file' : 'memory';
  var normalizedLimit = parseInt(limit, 10);
  if (!normalizedLimit || normalizedLimit < 1) normalizedLimit = 50;
  if (normalizedLimit > 200) normalizedLimit = 200;
  var normalizedHours = undefined;
  if (hours !== undefined && hours !== null && hours !== '') {
    var parsedHours = parseInt(hours, 10);
    if (parsedHours > 0 && parsedHours <= 720) normalizedHours = parsedHours;
  }
  if (!ctx.stats) {
    var fallbackLimit = normalizedLimit;
    return jsonResponse(res, 200, { data: [], total: 0, page: 1, pages: 1, limit: fallbackLimit });
  }
  var payload = ctx.stats.getRecentRequests(page, normalizedLimit, filter, normalizedSearch, normalizedSource, date, normalizedHours);
  var userStore = resolveDiscordUserStore(ctx);
  return jsonResponse(res, 200, enrichStatsRecentPayload(payload, userStore));
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

function handleAccountHealthStatus(res, ctx) {
  var checker = ctx.accountHealthChecker;
  if (!checker || typeof checker.getStatus !== 'function') {
    return jsonResponse(res, 200, {
      enabled: false,
      config: null,
      runtime: { running: false, timer_active: false },
      summary: {
        last_check_at: null,
        coverage_percent: 0,
        issues_in_cycle: 0,
        suspect_queue_size: 0,
      },
      totals: null,
      suspects: [],
    });
  }
  return jsonResponse(res, 200, checker.getStatus());
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
  if (!raw) return 0;
  var matched = raw.match(/^discord_(\d+)$/);
  var numericText = matched ? matched[1] : (/^\d+$/.test(raw) ? raw : '');
  if (!numericText) return 0;
  var n = Number(numericText);
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
  var discordUserId = String(row.discord_user_id || '').trim();
  var username = String(row.username || '').trim();
  var globalName = String(row.global_name || '').trim();
  if (!username) username = discordUserId || seqId;
  if (!globalName) globalName = username;
  return {
    discord_user_id: discordUserId,
    seq_id: seqId,
    username: username,
    global_name: globalName,
    avatar: String(row.avatar || ''),
    status: String(row.status || 'active'),
    roles: Array.isArray(row.roles) ? row.roles.slice() : [],
    created_at: String(row.created_at || ''),
    last_login_at: String(row.last_login_at || ''),
  };
}

function isDiscordSeqId(value) {
  return /^discord_\d+$/.test(String(value || '').trim());
}

function buildDiscordUsersIndex(userStore) {
  var byDiscordId = {};
  var bySeqId = {};
  if (userStore && typeof userStore.listUsers === 'function') {
    try {
      var users = userStore.listUsers();
      if (Array.isArray(users)) {
        for (var i = 0; i < users.length; i++) {
          var user = users[i] && typeof users[i] === 'object' ? users[i] : {};
          var discordUserId = String(user.discord_user_id || '').trim();
          if (!discordUserId) continue;
          var profile = {
            discord_user_id: discordUserId,
            seq_id: normalizeSeqId(user.seq_id),
            username: String(user.username || '').trim(),
            global_name: String(user.global_name || '').trim(),
            avatar: String(user.avatar || '').trim(),
            created_at: String(user.created_at || '').trim(),
          };
          byDiscordId[discordUserId] = profile;
          if (profile.seq_id) {
            bySeqId[profile.seq_id] = profile;
          }
        }
      }
    } catch (_) {}
  }
  return {
    byDiscordId: byDiscordId,
    bySeqId: bySeqId,
  };
}

function buildDiscordUsersMap(userStore) {
  return buildDiscordUsersIndex(userStore).byDiscordId;
}

function resolveDiscordProfileByIdentity(identity, discordIndex) {
  var index = discordIndex && typeof discordIndex === 'object'
    ? discordIndex
    : { byDiscordId: {}, bySeqId: {} };
  var byDiscordId = index.byDiscordId && typeof index.byDiscordId === 'object' ? index.byDiscordId : {};
  var bySeqId = index.bySeqId && typeof index.bySeqId === 'object' ? index.bySeqId : {};
  var raw = String(identity || '').trim();

  var discordUserId = '';
  var seqId = '';
  var profile = null;

  if (raw.indexOf('discord:') === 0) {
    var suffix = raw.slice('discord:'.length).trim();
    if (isDiscordSeqId(suffix)) {
      seqId = normalizeSeqId(suffix);
    } else if (/^\d+$/.test(suffix)) {
      discordUserId = suffix;
    }
  } else if (isDiscordSeqId(raw)) {
    seqId = normalizeSeqId(raw);
  } else if (/^\d+$/.test(raw)) {
    discordUserId = raw;
  }

  if (discordUserId && byDiscordId[discordUserId]) {
    profile = byDiscordId[discordUserId];
  }
  if (!profile && seqId && bySeqId[seqId]) {
    profile = bySeqId[seqId];
  }
  if (profile && !discordUserId) {
    discordUserId = String(profile.discord_user_id || '').trim();
  }
  if (profile && !seqId) {
    seqId = String(profile.seq_id || '').trim();
  }

  var canonicalIdentity = raw;
  if (seqId) {
    if (raw.indexOf('discord:') === 0) {
      canonicalIdentity = 'discord:' + seqId;
    } else {
      canonicalIdentity = seqId;
    }
  } else if (discordUserId) {
    canonicalIdentity = 'discord:' + discordUserId;
  }

  var username = profile ? String(profile.username || '').trim() : '';
  var displayName = profile ? String(profile.global_name || profile.username || '').trim() : '';
  var avatarUrl = '';
  if (profile) {
    avatarUrl = buildDiscordAvatarUrl(discordUserId, profile.avatar);
  }
  if (!displayName) displayName = username;
  if (!displayName) displayName = discordUserId || seqId || canonicalIdentity;
  if (!username) username = displayName || seqId || discordUserId || canonicalIdentity;

  return {
    identity: canonicalIdentity,
    discord_user_id: discordUserId,
    seq_id: seqId,
    username: username,
    display_name: displayName,
    avatar_url: avatarUrl,
    profile: profile,
  };
}

function buildDiscordAvatarUrl(discordUserId, avatar) {
  var userId = String(discordUserId || '').trim();
  var hash = String(avatar || '').trim();
  if (!userId || !hash) return '';
  if (/^https?:\/\//i.test(hash)) return hash;
  return 'https://cdn.discordapp.com/avatars/' + userId + '/' + hash + '.png?size=64';
}

function normalizeIdentityForAdminSeq(value) {
  var identity = String(value || '').trim();
  if (!identity || identity === 'unknown') return '';
  if (identity.indexOf('discord:') === 0) return '';
  if (isDiscordSeqId(identity)) return '';
  return identity;
}

function appendAdminIdentity(list, seen, identity) {
  var normalized = normalizeIdentityForAdminSeq(identity);
  if (!normalized || seen.has(normalized)) return;
  seen.add(normalized);
  list.push(normalized);
}

function buildAdminIdentityList(config, rows) {
  var list = [];
  var seen = new Set();
  var serverCfg = (config && config.server) || {};

  appendAdminIdentity(list, seen, serverCfg.default_identity);
  appendAdminIdentity(list, seen, serverCfg.admin_username);

  var arrayFields = [
    serverCfg.whitelist,
    serverCfg.whitelist_identities,
    serverCfg.admin_users,
    serverCfg.admin_identities,
  ];
  for (var i = 0; i < arrayFields.length; i++) {
    var arr = arrayFields[i];
    if (!Array.isArray(arr)) continue;
    for (var j = 0; j < arr.length; j++) {
      appendAdminIdentity(list, seen, arr[j]);
    }
  }

  var apiKeys = Array.isArray(serverCfg.api_keys) ? serverCfg.api_keys : [];
  for (var k = 0; k < apiKeys.length; k++) {
    var apiKey = apiKeys[k] || {};
    if (apiKey.enabled === false) continue;
    appendAdminIdentity(list, seen, apiKey.identity);
  }

  var extra = [];
  for (var n = 0; n < rows.length; n++) {
    var identity = normalizeIdentityForAdminSeq(rows[n] && rows[n].caller_identity);
    if (!identity || seen.has(identity)) continue;
    extra.push(identity);
  }
  extra.sort();
  for (var x = 0; x < extra.length; x++) {
    appendAdminIdentity(list, seen, extra[x]);
  }

  return list;
}

function enrichAbuseUsersWithIdentityProfile(rows, config, userStore) {
  var list = Array.isArray(rows) ? rows : [];
  if (list.length === 0) return [];

  var discordIndex = buildDiscordUsersIndex(userStore);
  var adminIdentities = buildAdminIdentityList(config, list);
  var adminSeqMap = new Map();
  for (var i = 0; i < adminIdentities.length; i++) {
    adminSeqMap.set(adminIdentities[i], 'admin_' + String(i + 1));
  }

  var enriched = [];
  for (var j = 0; j < list.length; j++) {
    var row = list[j] && typeof list[j] === 'object' ? Object.assign({}, list[j]) : {};
    var identity = String(row.identity || row.caller_identity || row.id || '').trim();
    if (!identity || identity === 'unknown') continue;
    var resolved = resolveDiscordProfileByIdentity(identity, discordIndex);
    var profile = resolved.profile && typeof resolved.profile === 'object' ? resolved.profile : null;
    var discordUserId = String(resolved.discord_user_id || '').trim();
    if (!discordUserId && row.user && typeof row.user === 'object') {
      var nestedDiscordUserId = String(row.user.discord_user_id || '').trim();
      if (nestedDiscordUserId) {
        discordUserId = nestedDiscordUserId;
      }
    }
    if (resolved.identity) {
      identity = String(resolved.identity || '').trim() || identity;
    } else if (discordUserId) {
      identity = 'discord:' + discordUserId;
    }

    var seqId = String(row.seq_id || '').trim();
    var username = String(row.username || row.discord_username || '').trim();
    var displayName = String(row.display_name || row.discord_display_name || '').trim();
    var avatarUrl = String(row.avatar_url || '').trim();
    var createdAt = String(row.created_at || '').trim();
    if (resolved.seq_id && !seqId) seqId = String(resolved.seq_id || '').trim();
    if (!username && resolved.username) username = String(resolved.username || '').trim();
    if (!displayName && resolved.display_name) displayName = String(resolved.display_name || '').trim();
    if (!avatarUrl && resolved.avatar_url) avatarUrl = String(resolved.avatar_url || '').trim();
    if (profile) {
      if (!seqId) seqId = String(profile.seq_id || '').trim();
      if (!username) username = String(profile.username || '').trim();
      if (!displayName) displayName = String(profile.global_name || profile.username || '').trim();
      if (!avatarUrl) avatarUrl = buildDiscordAvatarUrl(discordUserId, profile.avatar);
      if (!createdAt) createdAt = String(profile.created_at || '').trim();
    } else {
      if (!seqId) seqId = adminSeqMap.get(identity) || '';
      if (!displayName) displayName = identity;
    }

    if (!username) {
      if (discordUserId) {
        username = discordUserId || seqId;
      } else {
        username = identity;
      }
    }
    if (!displayName) {
      if (discordUserId) {
        displayName = username || seqId || discordUserId || identity;
      } else {
        displayName = username || identity;
      }
    }

    var hitRules = [];
    if (Array.isArray(row.hit_rules)) {
      for (var h = 0; h < row.hit_rules.length; h++) {
        var hit = String(row.hit_rules[h] || '').trim();
        if (hit) hitRules.push(hit);
      }
    } else if (Array.isArray(row.reasons)) {
      for (var hr = 0; hr < row.reasons.length; hr++) {
        var reason = row.reasons[hr] || {};
        var ruleId = String(reason.rule_id || '').trim();
        if (ruleId) hitRules.push(ruleId);
      }
    }

    row.identity = identity;
    row.id = identity;
    row.caller_identity = identity;
    row.seq_id = seqId;
    row.username = username;
    row.display_name = displayName;
    row.avatar_url = avatarUrl;
    row.created_at = createdAt;
    row.requests = Math.max(0, Math.floor(toFiniteNumber(row.requests, 0)));
    row.input_tokens = Math.max(0, Math.floor(toFiniteNumber(row.input_tokens, 0)));
    row.output_tokens = Math.max(0, Math.floor(toFiniteNumber(row.output_tokens, 0)));
    row.cached_tokens = Math.max(0, Math.floor(toFiniteNumber(row.cached_tokens, 0)));
    row.first_seen = Math.max(0, Math.floor(toFiniteNumber(row.first_seen, 0)));
    row.last_seen = Math.max(0, Math.floor(toFiniteNumber(row.last_seen, 0)));
    row.score = Math.max(0, Math.floor(toFiniteNumber(row.score, 0)));
    row.level = String(row.level || 'low');
    row.action = String(row.action || 'observe');
    row.hit_rules = hitRules;
    row.reasons_count = Math.max(hitRules.length, Math.floor(toFiniteNumber(row.reasons_count, hitRules.length)));

    // 兼容旧字段，避免前端和其他调用方受影响
    row.discord_username = username;
    row.discord_display_name = displayName;

    if (!row.user || typeof row.user !== 'object') {
      row.user = {
        discord_user_id: discordUserId,
        username: username,
        global_name: displayName,
        status: '',
        last_login_at: '',
      };
    }
    enriched.push(row);
  }
  return enriched;
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
  if (limit > 200000) limit = 200000;

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
      risk_users: 0,
      levels: { low: 0, medium: 0, high: 0, critical: 0 },
      actions: { observe: 0, throttle: 0, challenge: 0, suspend: 0 },
      today_events: 0,
    }, null, {});
  }
  var overview = engine.getOverview();
  return envelope(res, 200, true, overview, null, {});
}

function buildApiCallerStatsMap(stats, userStore, hours) {
  var out = new Map();
  if (!stats || typeof stats.getCallerStatsLastHours !== 'function') return out;
  var rows = [];
  try {
    rows = stats.getCallerStatsLastHours(hours || 24);
  } catch (_) {
    rows = [];
  }
  var enriched = enrichStatsCallerRows(rows, userStore);
  for (var i = 0; i < enriched.length; i++) {
    var row = enriched[i] && typeof enriched[i] === 'object' ? enriched[i] : {};
    var identity = String(row.identity || row.caller_identity || '').trim();
    if (!identity) continue;
    out.set(identity, row);
  }
  return out;
}

function attachApiUsageToAbuseRows(rows, apiMap) {
  var list = Array.isArray(rows) ? rows : [];
  var map = apiMap instanceof Map ? apiMap : new Map();
  var out = [];
  for (var i = 0; i < list.length; i++) {
    var row = list[i] && typeof list[i] === 'object' ? Object.assign({}, list[i]) : {};
    var identity = String(row.identity || row.caller_identity || '').trim();
    var api = identity ? (map.get(identity) || null) : null;
    row.api_requests = Math.max(0, toFiniteEventInt(api && api.requests, 0));
    row.api_input_tokens = Math.max(0, toFiniteEventInt(api && api.input, 0));
    row.api_output_tokens = Math.max(0, toFiniteEventInt(api && api.output, 0));
    row.api_cached_tokens = Math.max(0, toFiniteEventInt(api && api.cached, 0));
    row.api_reasoning_tokens = Math.max(0, toFiniteEventInt(api && api.reasoning, 0));
    row.api_errors = Math.max(0, toFiniteEventInt(api && api.errors, 0));
    out.push(row);
  }
  return out;
}

function handleAbuseUsers(req, res, ctx) {
  var engine = getAbuseRuleEngine(ctx);
  if (!engine || typeof engine.listUsers !== 'function') {
    return envelope(res, 200, true, [], null, { total: 0, page: 1, pages: 1, limit: 50 });
  }
  var query = parseQuery(req.url);
  var requestPage = Math.max(1, parseInt(query.page || '1', 10) || 1);
  var requestLimit = parseInt(query.limit || '50', 10) || 50;
  if (requestLimit < 1) requestLimit = 1;
  if (requestLimit > 500) requestLimit = 500;
  var sort = String(query.sort || 'requests_desc').trim() || 'requests_desc';
  var activityMode = String(query.activity || '').trim().toLowerCase();
  if (!activityMode) activityMode = sort === 'last_seen_desc' ? 'api' : 'all';
  if (activityMode !== 'api' && activityMode !== 'all') activityMode = 'all';

  var baseOptions = {
    level: query.level || '',
    action: query.action || '',
    sort: sort,
    keyword: query.q || query.keyword || '',
  };
  var userStore = resolveDiscordUserStore(ctx);
  var result = null;
  var rows = [];

  if (activityMode === 'api') {
    var allRows = [];
    var scanPage = 1;
    var scanLimit = 500;
    var maxScanPages = 1000;
    var totalPages = 1;
    while (scanPage <= totalPages && scanPage <= maxScanPages) {
      var pageResult = engine.listUsers(Object.assign({}, baseOptions, {
        page: scanPage,
        limit: scanLimit,
      })) || {};
      var pageRows = Array.isArray(pageResult.data) ? pageResult.data : [];
      for (var r = 0; r < pageRows.length; r++) {
        allRows.push(pageRows[r]);
      }
      totalPages = Math.max(1, toFiniteEventInt(pageResult.pages, 1));
      scanPage += 1;
    }

    var enrichedAllRows = enrichAbuseUsersWithIdentityProfile(
      allRows,
      ctx && ctx.config ? ctx.config : null,
      userStore
    );
    var apiMap = buildApiCallerStatsMap(ctx && ctx.stats ? ctx.stats : null, userStore, 24);
    var rowsWithApi = attachApiUsageToAbuseRows(enrichedAllRows, apiMap);
    var apiOnlyRows = [];
    for (var i = 0; i < rowsWithApi.length; i++) {
      if (toFiniteEventInt(rowsWithApi[i] && rowsWithApi[i].api_requests, 0) > 0) {
        apiOnlyRows.push(rowsWithApi[i]);
      }
    }
    var filteredTotal = apiOnlyRows.length;
    var filteredPages = Math.max(1, Math.ceil(filteredTotal / requestLimit));
    var filteredPage = requestPage > filteredPages ? filteredPages : requestPage;
    var filteredStart = (filteredPage - 1) * requestLimit;
    rows = apiOnlyRows.slice(filteredStart, filteredStart + requestLimit);
    result = {
      total: filteredTotal,
      page: filteredPage,
      pages: filteredPages,
      limit: requestLimit,
    };
  } else {
    result = engine.listUsers(Object.assign({}, baseOptions, {
      page: requestPage,
      limit: requestLimit,
    })) || {};
    rows = enrichAbuseUsersWithIdentityProfile(
      result.data || [],
      ctx && ctx.config ? ctx.config : null,
      userStore
    );
    var apiStatsMap = buildApiCallerStatsMap(ctx && ctx.stats ? ctx.stats : null, userStore, 24);
    rows = attachApiUsageToAbuseRows(rows, apiStatsMap);
  }

  return envelope(res, 200, true, rows, null, {
    total: result && result.total ? result.total : 0,
    page: result && result.page ? result.page : 1,
    pages: result && result.pages ? result.pages : 1,
    limit: result && result.limit ? result.limit : requestLimit,
    activity_mode: activityMode,
  });
}

function toFiniteNumber(value, fallback) {
  var n = Number(value);
  if (!isFinite(n)) return fallback || 0;
  return n;
}

function parsePageLimit(value, fallback, minValue, maxValue) {
  var n = parseInt(value, 10);
  if (!isFinite(n)) return fallback;
  if (n < minValue) return minValue;
  if (n > maxValue) return maxValue;
  return n;
}

function listRecentHistoryDates(days) {
  var count = Math.max(1, Math.floor(toFiniteNumber(days, 3)));
  var out = [];
  var now = Date.now();
  for (var i = 0; i < count; i++) {
    out.push(_dateStrFromTs(now - i * 86400000));
  }
  return out;
}

function resolveHistoryDates(dateValue) {
  var date = String(dateValue || '').trim();
  if (_isValidDateStr(date)) return [date];
  return listRecentHistoryDates(3);
}

function readAbuseUserHistory(ctx, identity, dates) {
  if (!ctx || !ctx.stats || typeof ctx.stats.searchRequests !== 'function') return [];
  var rows = [];
  var dateList = Array.isArray(dates) ? dates : [];
  for (var i = 0; i < dateList.length; i++) {
    var date = String(dateList[i] || '').trim();
    if (!_isValidDateStr(date)) continue;
    var page = 1;
    var scanLimit = 500;
    var maxPages = 200;
    while (page <= maxPages) {
      var result = ctx.stats.searchRequests({
        page: page,
        limit: scanLimit,
        search: identity,
        from: date,
        to: date,
      }) || {};
      var list = Array.isArray(result.data) ? result.data : [];
      for (var j = 0; j < list.length; j++) {
        var raw = list[j] && typeof list[j] === 'object' ? list[j] : {};
        var callerIdentity = String(raw.caller_identity || '').trim();
        if (callerIdentity !== identity) continue;
        var ts = Math.floor(toFiniteNumber(raw.ts, 0));
        var statusCode = Math.floor(toFiniteNumber(raw.status, 0));
        var latencyMs = Math.max(0, Math.floor(toFiniteNumber(raw.latency, 0)));
        rows.push({
          _sort_ts: ts,
          ts: ts,
          timestamp: ts > 0 ? new Date(ts).toISOString() : '',
          model: String(raw.model || ''),
          input_tokens: Math.max(0, Math.floor(toFiniteNumber(raw.input_tokens, 0))),
          output_tokens: Math.max(0, Math.floor(toFiniteNumber(raw.output_tokens, 0))),
          cached_tokens: Math.max(0, Math.floor(toFiniteNumber(raw.cached_tokens, 0))),
          status: statusCode,
          status_code: statusCode,
          latency: latencyMs,
          latency_ms: latencyMs,
          ip: String(raw.ip || ''),
        });
      }

      var totalPages = parseInt(result.pages, 10);
      if (!isFinite(totalPages) || totalPages <= 0 || page >= totalPages) break;
      page += 1;
    }
  }

  rows.sort(function (a, b) {
    return toFiniteNumber(b._sort_ts, 0) - toFiniteNumber(a._sort_ts, 0);
  });
  return rows;
}

function handleAbuseUserHistory(req, res, ctx, userId) {
  var identityCheck = validateIdentityValue(userId);
  if (!identityCheck.ok) {
    return envelope(res, 400, false, null, { message: identityCheck.reason }, {});
  }
  var normalizedUserId = identityCheck.value;
  var query = parseQuery(req.url);
  var page = parsePageLimit(query.page, 1, 1, 1000000);
  var limit = parsePageLimit(query.limit, 20, 1, 200);
  var date = String(query.date || '').trim();
  var dates = resolveHistoryDates(date);

  var allRows = readAbuseUserHistory(ctx, normalizedUserId, dates);
  var total = allRows.length;
  var pages = Math.max(1, Math.ceil(total / limit));
  if (page > pages) page = pages;
  var start = (page - 1) * limit;
  var pageRows = allRows.slice(start, start + limit);
  var data = [];
  for (var i = 0; i < pageRows.length; i++) {
    var row = Object.assign({}, pageRows[i]);
    delete row._sort_ts;
    data.push(row);
  }

  return envelope(res, 200, true, data, null, {
    total: total,
    page: page,
    pages: pages,
    limit: limit,
    date: _isValidDateStr(date) ? date : '',
    days: _isValidDateStr(date) ? 1 : 3,
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
  var threshold = (credConfig && credConfig.session_invalidated_relogin_threshold) || 1;
  return jsonResponse(res, 200, {
    enabled: false,
    auto_relogin_enabled: false,
    deprecated: true,
    threshold: threshold,
    relogin_needed_count: 0,
    with_password: 0,
    without_password: 0,
    accounts: [],
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
  var verifyEvents = [];

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
          verifyEvents.push(buildAccountEvent(email, 'verify_fail', {
            statusCode: 401,
            errorType: 'no_session_token',
            payload: {
              source: 'verify_batch',
              reason: 'no_session_token',
              from_status: String(account.status || ''),
              to_status: 'wasted',
            },
          }));
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
          verifyEvents.push(buildAccountEvent(email, 'verify_fail', {
            statusCode: 0,
            errorType: 'cas_apply_failed',
            payload: {
              source: 'verify_batch',
              reason: verifyCas.reason || 'cas_apply_failed',
              from_status: String(account.status || ''),
              to_status: 'wasted',
            },
          }));
          stats.verified_fail++;
          return { email: email, status: 'fail', reason: verifyCas.reason || 'cas_apply_failed' };
        } else {
          fullAccount.last_error_type = 'verify_failed';
          fullAccount.last_error_code = (result && result.statusCode) || 401;
          pool.markWasted(email);
          verifyEvents.push(buildAccountEvent(email, 'verify_fail', {
            statusCode: (result && result.statusCode) || 401,
            errorType: 'verify_failed',
            payload: {
              source: 'verify_batch',
              reason: (result && (result.detail || result.error)) || 'unknown',
              from_status: String(account.status || ''),
              to_status: 'wasted',
            },
          }));
          stats.verified_fail++;
          var detail = (result && (result.detail || result.error)) || 'unknown';
          log('❌', C.red, '批量验证失败: ' + email + ' (' + detail + ')');
          return { email: email, status: 'fail', reason: detail };
        }
      }).catch(function (err) {
        fullAccount.last_error_type = 'network_error';
        fullAccount.last_error_code = 0;
        pool.markWasted(email);
        verifyEvents.push(buildAccountEvent(email, 'verify_fail', {
          statusCode: 0,
          errorType: 'network_error',
          payload: {
            source: 'verify_batch',
            reason: err && err.message ? err.message : 'network_error',
            from_status: String(account.status || ''),
            to_status: 'wasted',
          },
        }));
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

  await pool.flush();
  await appendAccountEvents(pool, verifyEvents);
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
    if (Object.prototype.hasOwnProperty.call(startParams, 'count')) {
      var startCount = parseInt(startParams.count, 10);
      if (!Number.isFinite(startCount) || startCount < 1) {
        return jsonResponse(res, 400, { error: 'count must be at least 1' });
      }
      startParams.count = startCount;
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
      var defaultRegisterServerForStatus = buildRegisterPoolServerUrl('all', localProxyForStatus, config);
      statusPayload.proxy = {
        enabled: !!localRegisterProxyForStatus.enabled,
        server: localRegisterProxyForStatus.server || defaultRegisterServerForStatus,
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
            var defaultRegisterServer = buildRegisterPoolServerUrl('all', localProxy, config);
            statusPayload.proxy = {
              enabled: !!localRegisterProxy.enabled,
              server: localRegisterProxy.server || defaultRegisterServer,
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
    var safeError = maskRegisterSocksUrlForLog(err && err.message ? err.message : String(err));
    log('❌', C.red, 'Register proxy error: ' + safeError);
    return jsonResponse(res, 502, {
      error: 'Cannot reach registration server: ' + safeError,
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
    await pool.flush();
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
 * 50 并发，每个账号发一个小请求测试
 */
async function handleTestBatch(req, res, pool, config) {
  var body;
  try { body = await readBody(req); } catch (e) { body = {}; }

  var concurrency = body.concurrency || 50;
  var filter = body.filter || 'active';
  var statusFilter = filter === 'all' ? '' : filter;
  var rawAccounts = await Promise.resolve(pool.listAccounts({ status: statusFilter, page: 1, limit: 999999 }));
  var allAccounts = rawAccounts && typeof rawAccounts === 'object' && Array.isArray(rawAccounts.accounts)
    ? rawAccounts.accounts
    : (Array.isArray(rawAccounts) ? rawAccounts : []);
  var testAccounts = [];

  for (var i = 0; i < allAccounts.length; i++) {
    var a = allAccounts[i];
    var full = pool.getFullAccount(a.email);
    if (full && full.accessToken) testAccounts.push(full);
  }

  if (testAccounts.length === 0) {
    return jsonResponse(res, 200, { total: 0, ok: 0, fail: 0, results: [] });
  }

  log('🧪', C.cyan, '批量测试 ' + testAccounts.length + ' 个账号 (并发' + concurrency + ')...');

  var testModel = (config.models && config.models.default) || 'gpt-5.3-codex';
  var okCount = 0, failCount = 0;
  var results = [];
  var testEvents = [];
  var queue = testAccounts.slice();

  async function testOne() {
    while (queue.length > 0) {
      var acc = queue.shift();
      var result = await testOneAccount(acc, testModel);
      if (result.ok) {
        okCount++;
        pool.markSuccess(acc.email, null);
        testEvents.push(buildAccountEvent(acc.email, 'test_success', {
          statusCode: result.status || 200,
          payload: {
            source: 'test_batch',
            model: testModel,
            latency_ms: result.latency || 0,
            filter: filter,
          },
        }));
        results.push({ email: acc.email, ok: true, latency: result.latency, status: result.status });
      } else if (result.networkError) {
        // 网络异常不改池状态
        failCount++;
        testEvents.push(buildAccountEvent(acc.email, 'test_fail', {
          statusCode: 0,
          errorType: 'network_error',
          payload: {
            source: 'test_batch',
            model: testModel,
            latency_ms: result.latency || 0,
            error: truncateErrorString(result.error || 'network_error'),
            filter: filter,
          },
        }));
        results.push({ email: acc.email, ok: false, latency: result.latency, error: result.error });
      } else {
        failCount++;
        var errResult = pool.markError(acc.email, result.status, result.error || '');
        testEvents.push(buildAccountEvent(acc.email, 'test_fail', {
          statusCode: result.status || 0,
          errorType: errResult && errResult.type ? errResult.type : 'test_failed',
          payload: {
            source: 'test_batch',
            model: testModel,
            latency_ms: result.latency || 0,
            action: errResult && errResult.action ? errResult.action : '',
            error: truncateErrorString(result.error || ''),
            filter: filter,
          },
        }));
        results.push({ email: acc.email, ok: false, latency: result.latency, status: result.status, error_type: errResult.type, action: errResult.action, error: (result.error || '').substring(0, 150) });
      }
    }
  }

  var workers = [];
  for (var w = 0; w < Math.min(concurrency, testAccounts.length); w++) {
    workers.push(testOne());
  }
  await Promise.all(workers);

  if (failCount > 0) await pool.flush();
  await appendAccountEvents(pool, testEvents);
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
    var _rawAll = await pool.listAccounts();
    var allAccounts = Array.isArray(_rawAll) ? _rawAll : (_rawAll && _rawAll.accounts || []);
    emails = [];
    for (var i = 0; i < allAccounts.length; i++) {
      var a = allAccounts[i];
      if (filter === 'all') { emails.push(a.email); }
      else if (filter === 'expired' && a.status === 'expired') { emails.push(a.email); }
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
    await pool.flush();
    logCollector.add('warn', '批量检测: ' + bannedCount + ' 个被封');
  }

  log('📊', C.cyan, '批量检测完成: ' + activeCount + ' 正常, ' + bannedCount + ' 被封, ' + errorCount + ' 错误');
  return jsonResponse(res, 200, { results: results, total: emails.length, banned: bannedCount, active: activeCount, error: errorCount });
}
