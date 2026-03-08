/**
 * 请求级统计收集器
 *
 * 功能:
 *   - 记录每条请求的详细信息(模型/账号/token用量/延迟/状态码)
 *   - 内存聚合: 今日汇总 + 按小时/模型/账号维度 + 滑动窗口RPM
 *   - SQLite 持久化到 daily_stats + request_logs
 *   - 环形缓冲区保留最近 N 条详细记录
 *   - 启动时从 SQLite 恢复, 跨天自动轮转
 *
 * 依赖: better-sqlite3
 */

import { existsSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { createRequire } from 'node:module';

var require = createRequire(import.meta.url);

function dateStr(ts) {
  var d = new Date(ts);
  var y = d.getFullYear();
  var m = String(d.getMonth() + 1).padStart(2, '0');
  var day = String(d.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + day;
}

function isDateStr(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function toFiniteNumber(value) {
  var n = Number(value);
  return isFinite(n) ? n : 0;
}

function toInteger(value, fallback) {
  var n = Number(value);
  if (!isFinite(n)) return fallback;
  return Math.floor(n);
}

function isObjectLike(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function loadBetterSqlite3() {
  try {
    require.resolve('better-sqlite3');
  } catch (_) {
    throw new Error('better-sqlite3 is not installed. Run: npm install better-sqlite3');
  }
  var loaded = require('better-sqlite3');
  return loaded && loaded.default ? loaded.default : loaded;
}

function pickMetric(src, primaryKey, fallbackKey) {
  if (!src || typeof src !== 'object') return 0;
  if (src[primaryKey] !== undefined && src[primaryKey] !== null) {
    return toFiniteNumber(src[primaryKey]);
  }
  return toFiniteNumber(src[fallbackKey]);
}

function normalizeCallerIdentity(identity, defaultIdentity) {
  var id = String(identity || '').trim();
  if (!id) return '';
  var fallback = String(defaultIdentity || '').trim();
  if (fallback && (id === 'local' || id === 'legacy_password')) return fallback;
  return id;
}

function isSeqIdText(value) {
  return /^discord_\d+$/.test(String(value || '').trim());
}

function toSeqIdText(value) {
  var raw = String(value || '').trim();
  if (!raw) return '';
  if (isSeqIdText(raw)) return raw;
  if (/^\d+$/.test(raw)) return 'discord_' + String(Math.floor(Number(raw)));
  return '';
}

function extractSeqIdFromIdentity(value) {
  var raw = String(value || '').trim();
  if (!raw) return '';
  if (isSeqIdText(raw)) return raw;
  if (raw.indexOf('discord:') === 0) {
    var suffix = raw.slice('discord:'.length).trim();
    if (isSeqIdText(suffix)) return suffix;
  }
  return '';
}

function mergeCallerBuckets(source, defaultIdentity) {
  if (!source || typeof source !== 'object' || Array.isArray(source)) return {};
  var merged = {};
  var keys = Object.keys(source);
  for (var i = 0; i < keys.length; i++) {
    var rawIdentity = keys[i];
    var normalizedIdentity = normalizeCallerIdentity(rawIdentity, defaultIdentity) || rawIdentity;
    if (!normalizedIdentity) continue;
    var src = source[rawIdentity] || {};
    if (!merged[normalizedIdentity]) {
      merged[normalizedIdentity] = {
        identity: normalizedIdentity,
        requests: 0,
        input: 0,
        output: 0,
        cached: 0,
        reasoning: 0,
        errors: 0,
      };
    }
    var dst = merged[normalizedIdentity];
    dst.requests += toFiniteNumber(src.requests);
    dst.input += pickMetric(src, 'input', 'input_tokens');
    dst.output += pickMetric(src, 'output', 'output_tokens');
    dst.cached += pickMetric(src, 'cached', 'cached_tokens');
    dst.reasoning += pickMetric(src, 'reasoning', 'reasoning_tokens');
    dst.errors += toFiniteNumber(src.errors);
  }
  return merged;
}

function normalizeCallerBucketsInTree(node, defaultIdentity) {
  if (!node || typeof node !== 'object') return false;
  var changed = false;

  if (Array.isArray(node)) {
    for (var a = 0; a < node.length; a++) {
      if (normalizeCallerBucketsInTree(node[a], defaultIdentity)) changed = true;
    }
    return changed;
  }

  var callerMapFields = ['callers', 'by_caller', 'by_caller_identity', 'per_caller'];
  for (var i = 0; i < callerMapFields.length; i++) {
    var key = callerMapFields[i];
    var map = node[key];
    if (!map || typeof map !== 'object' || Array.isArray(map)) continue;
    var before = JSON.stringify(map);
    var afterMap = mergeCallerBuckets(map, defaultIdentity);
    var after = JSON.stringify(afterMap);
    if (before !== after) {
      node[key] = afterMap;
      changed = true;
    }
  }

  var keys = Object.keys(node);
  for (var k = 0; k < keys.length; k++) {
    var child = node[keys[k]];
    if (child && typeof child === 'object') {
      if (normalizeCallerBucketsInTree(child, defaultIdentity)) changed = true;
    }
  }
  return changed;
}

function emptyDay(date) {
  var hours = [];
  for (var i = 0; i < 24; i++) {
    hours.push({ requests: 0, success: 0, input: 0, output: 0, cached: 0, reasoning: 0 });
  }
  return {
    date: date,
    total_requests: 0,
    success_requests: 0,
    total_input_tokens: 0,
    total_output_tokens: 0,
    total_cached_tokens: 0,
    total_reasoning_tokens: 0,
    total_latency_ms: 0,
    by_model: {},
    by_account: {},
    by_caller_identity: {},
    by_hour: hours,
  };
}

function mergeEntry(agg, entry) {
  agg.total_requests++;
  if (entry.status >= 200 && entry.status < 400) {
    agg.success_requests++;
  }
  agg.total_input_tokens += entry.input_tokens || 0;
  agg.total_output_tokens += entry.output_tokens || 0;
  agg.total_cached_tokens += entry.cached_tokens || 0;
  agg.total_reasoning_tokens += entry.reasoning_tokens || 0;
  agg.total_latency_ms += entry.latency || 0;

  // 按模型
  if (entry.model) {
    var m = agg.by_model[entry.model];
    if (!m) {
      m = { requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, latency: 0 };
      agg.by_model[entry.model] = m;
    }
    m.requests++;
    m.input += entry.input_tokens || 0;
    m.output += entry.output_tokens || 0;
    m.cached += entry.cached_tokens || 0;
    m.reasoning += entry.reasoning_tokens || 0;
    m.latency += entry.latency || 0;
  }

  // 按账号
  if (entry.account) {
    var a = agg.by_account[entry.account];
    if (!a) {
      a = { requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
      agg.by_account[entry.account] = a;
    }
    a.requests++;
    a.input += entry.input_tokens || 0;
    a.output += entry.output_tokens || 0;
    a.cached += entry.cached_tokens || 0;
    a.reasoning += entry.reasoning_tokens || 0;
    if (entry.error_type) a.errors++;
  }

  // 按调用身份
  if (entry.caller_identity) {
    var c = agg.by_caller_identity[entry.caller_identity];
    if (!c) {
      c = { identity: entry.caller_identity, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
      agg.by_caller_identity[entry.caller_identity] = c;
    }
    c.requests++;
    c.input += entry.input_tokens || 0;
    c.output += entry.output_tokens || 0;
    c.cached += entry.cached_tokens || 0;
    c.reasoning += entry.reasoning_tokens || 0;
    if (entry.error_type) c.errors++;
  }

  // 按小时
  var hour = new Date(entry.ts).getHours();
  var h = agg.by_hour[hour];
  h.requests++;
  if (entry.status >= 200 && entry.status < 400) h.success++;
  h.input += entry.input_tokens || 0;
  h.output += entry.output_tokens || 0;
  h.cached += entry.cached_tokens || 0;
  h.reasoning += entry.reasoning_tokens || 0;
}

function matchRecordKeyword(r, kw) {
  return (r.path && r.path.toLowerCase().indexOf(kw) >= 0)
    || (r.route && r.route.toLowerCase().indexOf(kw) >= 0)
    || (r.model && r.model.toLowerCase().indexOf(kw) >= 0)
    || (r.account && r.account.toLowerCase().indexOf(kw) >= 0)
    || (r.caller_identity && r.caller_identity.toLowerCase().indexOf(kw) >= 0)
    || (r.ip && String(r.ip).toLowerCase().indexOf(kw) >= 0)
    || (r.ua_hash && String(r.ua_hash).toLowerCase().indexOf(kw) >= 0)
    || (r.session_hint && String(r.session_hint).toLowerCase().indexOf(kw) >= 0)
    || (r.error_type && r.error_type.toLowerCase().indexOf(kw) >= 0)
    || (String(r.status).indexOf(kw) >= 0);
}

export class StatsCollector {
  constructor(opts) {
    opts = opts || {};
    var cfg = (opts && opts.config && opts.config.stats) || {};
    var serverCfg = (opts && opts.config && opts.config.server) || {};
    var dataRoot = resolve(opts.dataDir || 'data');
    var storageCfg = (opts && opts.config && opts.config.storage) || {};
    var sqliteCfg = (storageCfg && storageCfg.sqlite) || {};

    this._retentionDays = cfg.retention_days || 30;
    this._recentSize = cfg.recent_buffer_size || 2000;
    this._enabled = cfg.enabled !== false;
    this._defaultCallerIdentity = String(serverCfg.default_identity || '').trim();
    this._dbPath = resolve(String(sqliteCfg.path || resolve(dataRoot, 'accounts.db')));
    this._db = null;
    this._stmt = {};
    this._seqToDiscordIdCache = new Map();

    this._today = emptyDay(dateStr(Date.now()));
    this._recent = [];
    this._rpmBuckets = new Array(60).fill(0);
    this._rpmIndex = 0;
    this._rpmLastSec = Math.floor(Date.now() / 1000);

    this._tpmBuckets = new Array(60).fill(0);
    this._tpmIndex = 0;
    this._tpmLastSec = Math.floor(Date.now() / 1000);

    this._saveTimer = null;
    this._rpmTimer = null;

    if (this._enabled) {
      this._initDb();
      this._loadToday();
      this._startRpmTicker();
      this._cleanup();
    }
  }

  _isDbReady() {
    return !!(this._db && this._stmt && this._stmt.insertRequest);
  }

  _initDb() {
    try {
      var Database = loadBetterSqlite3();
      var dbDir = dirname(this._dbPath);
      if (!existsSync(dbDir)) {
        mkdirSync(dbDir, { recursive: true });
      }
      this._db = new Database(this._dbPath);
      this._db.pragma('journal_mode = WAL');
      this._db.pragma('busy_timeout = 5000');

      this._db.exec(`
        CREATE TABLE IF NOT EXISTS daily_stats (
          date TEXT PRIMARY KEY,
          total_requests INTEGER NOT NULL DEFAULT 0,
          success_requests INTEGER NOT NULL DEFAULT 0,
          total_input_tokens INTEGER NOT NULL DEFAULT 0,
          total_output_tokens INTEGER NOT NULL DEFAULT 0,
          total_cached_tokens INTEGER NOT NULL DEFAULT 0,
          total_reasoning_tokens INTEGER NOT NULL DEFAULT 0,
          total_latency_ms INTEGER NOT NULL DEFAULT 0,
          by_model_json TEXT NOT NULL DEFAULT '{}',
          by_account_json TEXT NOT NULL DEFAULT '{}',
          by_caller_json TEXT NOT NULL DEFAULT '{}',
          hourly_json TEXT NOT NULL DEFAULT '[]',
          updated_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);

        CREATE TABLE IF NOT EXISTS request_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          timestamp INTEGER NOT NULL,
          date_key TEXT NOT NULL,
          route TEXT NOT NULL DEFAULT '',
          path TEXT NOT NULL DEFAULT '',
          model TEXT NOT NULL DEFAULT '',
          account TEXT NOT NULL DEFAULT '',
          caller TEXT NOT NULL DEFAULT '',
          status INTEGER NOT NULL DEFAULT 0,
          latency_ms INTEGER NOT NULL DEFAULT 0,
          ttfb_ms INTEGER NOT NULL DEFAULT 0,
          input_tokens INTEGER NOT NULL DEFAULT 0,
          output_tokens INTEGER NOT NULL DEFAULT 0,
          cached_tokens INTEGER NOT NULL DEFAULT 0,
          reasoning_tokens INTEGER NOT NULL DEFAULT 0,
          error_type TEXT,
          error TEXT,
          stream INTEGER NOT NULL DEFAULT 0,
          ip TEXT NOT NULL DEFAULT '',
          ua_hash TEXT NOT NULL DEFAULT '',
          session_hint TEXT NOT NULL DEFAULT '',
          created_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_request_logs_ts ON request_logs(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_request_logs_date_ts ON request_logs(date_key, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_request_logs_caller_ts ON request_logs(caller, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_request_logs_account_ts ON request_logs(account, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_request_logs_model_ts ON request_logs(model, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_request_logs_status_ts ON request_logs(status, timestamp DESC);
      `);

      var requestLogColumns = this._db.prepare('PRAGMA table_info(request_logs)').all();
      var hasErrorType = false;
      var hasError = false;
      for (var colIdx = 0; colIdx < requestLogColumns.length; colIdx++) {
        var col = requestLogColumns[colIdx] || {};
        if (col.name === 'error_type') hasErrorType = true;
        if (col.name === 'error') hasError = true;
      }
      if (!hasErrorType) {
        this._db.exec('ALTER TABLE request_logs ADD COLUMN error_type TEXT');
      }
      if (!hasError) {
        this._db.exec('ALTER TABLE request_logs ADD COLUMN error TEXT');
      }
      this._db.exec('CREATE INDEX IF NOT EXISTS idx_request_logs_error_ts ON request_logs(error_type, timestamp DESC)');

      this._stmt.upsertDaily = this._db.prepare(`
        INSERT INTO daily_stats (
          date, total_requests, success_requests, total_input_tokens, total_output_tokens,
          total_cached_tokens, total_reasoning_tokens, total_latency_ms,
          by_model_json, by_account_json, by_caller_json, hourly_json, updated_at_ms
        ) VALUES (
          @date, @total_requests, @success_requests, @total_input_tokens, @total_output_tokens,
          @total_cached_tokens, @total_reasoning_tokens, @total_latency_ms,
          @by_model_json, @by_account_json, @by_caller_json, @hourly_json, @updated_at_ms
        )
        ON CONFLICT(date) DO UPDATE SET
          total_requests = excluded.total_requests,
          success_requests = excluded.success_requests,
          total_input_tokens = excluded.total_input_tokens,
          total_output_tokens = excluded.total_output_tokens,
          total_cached_tokens = excluded.total_cached_tokens,
          total_reasoning_tokens = excluded.total_reasoning_tokens,
          total_latency_ms = excluded.total_latency_ms,
          by_model_json = excluded.by_model_json,
          by_account_json = excluded.by_account_json,
          by_caller_json = excluded.by_caller_json,
          hourly_json = excluded.hourly_json,
          updated_at_ms = excluded.updated_at_ms
      `);
      this._stmt.insertRequest = this._db.prepare(`
        INSERT INTO request_logs (
          timestamp, date_key, route, path, model, account, caller, status, latency_ms, ttfb_ms,
          input_tokens, output_tokens, cached_tokens, reasoning_tokens, error_type, error, stream, ip, ua_hash, session_hint, created_at_ms
        ) VALUES (
          @timestamp, @date_key, @route, @path, @model, @account, @caller, @status, @latency_ms, @ttfb_ms,
          @input_tokens, @output_tokens, @cached_tokens, @reasoning_tokens, @error_type, @error, @stream, @ip, @ua_hash, @session_hint, @created_at_ms
        )
      `);
      this._stmt.selectRecent = this._db.prepare('SELECT * FROM request_logs ORDER BY timestamp DESC LIMIT @limit');
      this._stmt.selectDates = this._db.prepare('SELECT date FROM daily_stats ORDER BY date ASC');
      this._stmt.cleanupRequestLogs = this._db.prepare('DELETE FROM request_logs WHERE timestamp < ?');
      this._stmt.selectByDate = this._db.prepare('SELECT * FROM daily_stats WHERE date = ? LIMIT 1');
      this._stmt.updateRequestCallerById = this._db.prepare('UPDATE request_logs SET caller = @caller WHERE id = @id');
      try {
        this._stmt.selectDiscordUserIdBySeq = this._db.prepare('SELECT discord_user_id FROM discord_users WHERE seq_id = ? LIMIT 1');
      } catch (_) {
        this._stmt.selectDiscordUserIdBySeq = null;
      }
      this._stmt.sumAll = this._db.prepare(`
        SELECT
          COALESCE(SUM(total_requests), 0) AS total_requests,
          COALESCE(SUM(success_requests), 0) AS success_requests,
          COALESCE(SUM(total_input_tokens), 0) AS total_input_tokens,
          COALESCE(SUM(total_output_tokens), 0) AS total_output_tokens,
          COALESCE(SUM(total_cached_tokens), 0) AS total_cached_tokens,
          COALESCE(SUM(total_reasoning_tokens), 0) AS total_reasoning_tokens,
          COALESCE(SUM(total_latency_ms), 0) AS total_latency_ms
        FROM daily_stats
      `);
      this._stmt.sumRange = this._db.prepare(`
        SELECT
          COALESCE(SUM(total_requests), 0) AS total_requests,
          COALESCE(SUM(success_requests), 0) AS success_requests,
          COALESCE(SUM(total_input_tokens), 0) AS total_input_tokens,
          COALESCE(SUM(total_output_tokens), 0) AS total_output_tokens,
          COALESCE(SUM(total_cached_tokens), 0) AS total_cached_tokens,
          COALESCE(SUM(total_reasoning_tokens), 0) AS total_reasoning_tokens,
          COALESCE(SUM(total_latency_ms), 0) AS total_latency_ms
        FROM daily_stats
        WHERE date BETWEEN @from_date AND @to_date
      `);
      this._stmt.selectRangeDays = this._db.prepare(`
        SELECT * FROM daily_stats WHERE date BETWEEN @from_date AND @to_date ORDER BY date ASC
      `);
      this._migrateLegacyCallerIdentitiesInRequestLogs();
      this._migrateLegacyCallerIdentitiesInDailyStats();
    } catch (e) {
      this._db = null;
      this._stmt = {};
      throw new Error('[stats-collector] SQLite init failed: ' + (e && e.message ? e.message : String(e)));
    }
  }

  _resolveDiscordUserIdBySeq(seqId) {
    var normalizedSeq = toSeqIdText(seqId);
    if (!normalizedSeq) return '';
    if (this._seqToDiscordIdCache.has(normalizedSeq)) {
      return this._seqToDiscordIdCache.get(normalizedSeq) || '';
    }
    var discordUserId = '';
    try {
      if (this._stmt.selectDiscordUserIdBySeq) {
        var row = this._stmt.selectDiscordUserIdBySeq.get(normalizedSeq);
        discordUserId = row && row.discord_user_id ? String(row.discord_user_id || '').trim() : '';
      }
    } catch (_) {
      discordUserId = '';
    }
    this._seqToDiscordIdCache.set(normalizedSeq, discordUserId);
    return discordUserId;
  }

  _canonicalizeCallerIdentity(identity) {
    var normalized = this._normalizeCallerIdentity(identity);
    if (!normalized) return '';
    var seqId = extractSeqIdFromIdentity(normalized);
    if (!seqId) return normalized;
    if (normalized.indexOf('discord:') === 0) {
      return 'discord:' + seqId;
    }
    return seqId;
  }

  _migrateLegacyCallerIdentitiesInRequestLogs() {
    if (!this._isDbReady() || !this._stmt.updateRequestCallerById) return;
    var legacyRows = [];
    try {
      legacyRows = this._db.prepare(`
        SELECT id, caller
        FROM request_logs
        WHERE caller GLOB 'discord_[0-9]*'
           OR caller GLOB 'discord:discord_[0-9]*'
      `).all();
    } catch (_) {
      legacyRows = [];
    }
    if (!Array.isArray(legacyRows) || legacyRows.length === 0) return;
    var updates = [];
    for (var i = 0; i < legacyRows.length; i++) {
      var row = legacyRows[i] || {};
      var rowId = toInteger(row.id, 0);
      if (rowId <= 0) continue;
      var currentCaller = String(row.caller || '').trim();
      if (!currentCaller) continue;
      var canonicalCaller = this._canonicalizeCallerIdentity(currentCaller);
      if (!canonicalCaller || canonicalCaller === currentCaller) continue;
      updates.push({ id: rowId, caller: canonicalCaller });
    }
    if (updates.length === 0) return;
    var runUpdates = this._db.transaction(function (payload) {
      for (var idx = 0; idx < payload.length; idx++) {
        this._stmt.updateRequestCallerById.run(payload[idx]);
      }
    }.bind(this));
    try {
      runUpdates(updates);
    } catch (_) {
      // ignore migration failure to avoid blocking service start
    }
  }

  _migrateLegacyCallerIdentitiesInDailyStats() {
    if (!this._isDbReady()) return;
    var rows = [];
    try {
      rows = this._db.prepare('SELECT date, by_caller_json FROM daily_stats').all();
    } catch (_) {
      rows = [];
    }
    if (!Array.isArray(rows) || rows.length === 0) return;
    var updates = [];
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i] || {};
      var date = String(row.date || '').trim();
      if (!date) continue;
      var source = {};
      try {
        source = row.by_caller_json ? JSON.parse(row.by_caller_json) : {};
      } catch (_) {
        source = {};
      }
      if (!source || typeof source !== 'object' || Array.isArray(source)) continue;
      var keys = Object.keys(source);
      if (keys.length === 0) continue;
      var changed = false;
      var merged = {};
      for (var k = 0; k < keys.length; k++) {
        var key = keys[k];
        var src = source[key] && typeof source[key] === 'object' ? source[key] : {};
        var canonicalIdentity = this._canonicalizeCallerIdentity(src.identity || key) || this._normalizeCallerIdentity(src.identity || key) || key;
        if (!canonicalIdentity) continue;
        if (canonicalIdentity !== key || String(src.identity || '').trim() !== canonicalIdentity) {
          changed = true;
        }
        if (!merged[canonicalIdentity]) {
          merged[canonicalIdentity] = {
            identity: canonicalIdentity,
            requests: 0,
            input: 0,
            output: 0,
            cached: 0,
            reasoning: 0,
            errors: 0,
          };
        }
        var dst = merged[canonicalIdentity];
        dst.requests += Math.max(0, toInteger(src.requests, 0));
        dst.input += Math.max(0, toInteger(pickMetric(src, 'input', 'input_tokens'), 0));
        dst.output += Math.max(0, toInteger(pickMetric(src, 'output', 'output_tokens'), 0));
        dst.cached += Math.max(0, toInteger(pickMetric(src, 'cached', 'cached_tokens'), 0));
        dst.reasoning += Math.max(0, toInteger(pickMetric(src, 'reasoning', 'reasoning_tokens'), 0));
        dst.errors += Math.max(0, toInteger(src.errors, 0));
      }
      if (!changed) continue;
      updates.push({
        date: date,
        by_caller_json: JSON.stringify(merged),
        updated_at_ms: Date.now(),
      });
    }
    if (updates.length === 0) return;
    var updateStmt = this._db.prepare(`
      UPDATE daily_stats
      SET by_caller_json = @by_caller_json,
          updated_at_ms = @updated_at_ms
      WHERE date = @date
    `);
    var runUpdates = this._db.transaction(function (payload) {
      for (var idx = 0; idx < payload.length; idx++) {
        updateStmt.run(payload[idx]);
      }
    });
    try {
      runUpdates(updates);
    } catch (_) {
      // ignore migration failure to avoid blocking service start
    }
  }

  _recordToDbRow(record) {
    var ts = toInteger(record && record.ts, Date.now());
    if (!isFinite(ts) || ts <= 0) ts = Date.now();
    return {
      timestamp: ts,
      date_key: dateStr(ts),
      route: String((record && record.route) || ''),
      path: String((record && record.path) || ''),
      model: String((record && record.model) || ''),
      account: String((record && record.account) || ''),
      caller: this._canonicalizeCallerIdentity((record && record.caller_identity) || ''),
      status: toInteger(record && record.status, 0),
      latency_ms: toInteger(record && record.latency, 0),
      ttfb_ms: toInteger(record && record.ttfb_ms, 0),
      input_tokens: toInteger(record && record.input_tokens, 0),
      output_tokens: toInteger(record && record.output_tokens, 0),
      cached_tokens: toInteger(record && record.cached_tokens, 0),
      reasoning_tokens: toInteger(record && record.reasoning_tokens, 0),
      error_type: record && record.error_type ? String(record.error_type) : null,
      error: record && record.error_type ? String(record.error_type) : null,
      stream: record && record.stream ? 1 : 0,
      ip: String((record && record.ip) || ''),
      ua_hash: String((record && record.ua_hash) || ''),
      session_hint: String((record && record.session_hint) || ''),
      created_at_ms: Date.now(),
    };
  }

  _rowToRecord(row) {
    if (!row || typeof row !== 'object') return null;
    return {
      ts: toFiniteNumber(row.timestamp),
      route: row.route || '',
      path: row.path || '',
      model: row.model || '',
      account: row.account || '',
      caller_identity: this._canonicalizeCallerIdentity(row.caller || ''),
      status: toFiniteNumber(row.status),
      latency: toFiniteNumber(row.latency_ms),
      ttfb_ms: toFiniteNumber(row.ttfb_ms),
      input_tokens: toFiniteNumber(row.input_tokens),
      output_tokens: toFiniteNumber(row.output_tokens),
      cached_tokens: toFiniteNumber(row.cached_tokens),
      reasoning_tokens: toFiniteNumber(row.reasoning_tokens),
      error_type: row.error_type || row.error || null,
      stream: Number(row.stream) > 0,
      ip: row.ip || '',
      ua_hash: row.ua_hash || '',
      session_hint: row.session_hint || '',
    };
  }

  _dayToDbRow(day) {
    var snapshot = this._normalizeLegacyDaySnapshot(day || {}, day && day.date ? String(day.date) : dateStr(Date.now())) || emptyDay(dateStr(Date.now()));
    return {
      date: snapshot.date,
      total_requests: Math.max(0, toInteger(snapshot.total_requests, 0)),
      success_requests: Math.max(0, toInteger(snapshot.success_requests, 0)),
      total_input_tokens: Math.max(0, toInteger(snapshot.total_input_tokens, 0)),
      total_output_tokens: Math.max(0, toInteger(snapshot.total_output_tokens, 0)),
      total_cached_tokens: Math.max(0, toInteger(snapshot.total_cached_tokens, 0)),
      total_reasoning_tokens: Math.max(0, toInteger(snapshot.total_reasoning_tokens, 0)),
      total_latency_ms: Math.max(0, toInteger(snapshot.total_latency_ms, 0)),
      by_model_json: JSON.stringify(snapshot.by_model || {}),
      by_account_json: JSON.stringify(snapshot.by_account || {}),
      by_caller_json: JSON.stringify(snapshot.by_caller_identity || {}),
      hourly_json: JSON.stringify(snapshot.by_hour || []),
      updated_at_ms: Date.now(),
    };
  }

  _dailyRowToDay(row, fallbackDate) {
    if (!row || typeof row !== 'object') return null;
    var parsed = {
      date: row.date || fallbackDate || '',
      total_requests: row.total_requests,
      success_requests: row.success_requests,
      total_input_tokens: row.total_input_tokens,
      total_output_tokens: row.total_output_tokens,
      total_cached_tokens: row.total_cached_tokens,
      total_reasoning_tokens: row.total_reasoning_tokens,
      total_latency_ms: row.total_latency_ms,
      by_model: {},
      by_account: {},
      by_caller_identity: {},
      by_hour: [],
    };
    try {
      parsed.by_model = row.by_model_json ? JSON.parse(row.by_model_json) : {};
    } catch (_) {
      parsed.by_model = {};
    }
    try {
      parsed.by_account = row.by_account_json ? JSON.parse(row.by_account_json) : {};
    } catch (_) {
      parsed.by_account = {};
    }
    try {
      parsed.by_caller_identity = row.by_caller_json ? JSON.parse(row.by_caller_json) : {};
    } catch (_) {
      parsed.by_caller_identity = {};
    }
    try {
      parsed.by_hour = row.hourly_json ? JSON.parse(row.hourly_json) : [];
    } catch (_) {
      parsed.by_hour = [];
    }
    return this._normalizeLegacyDaySnapshot(parsed, parsed.date || fallbackDate || dateStr(Date.now()));
  }

  _insertRequestRow(record) {
    if (!this._isDbReady()) return false;
    try {
      this._stmt.insertRequest.run(this._recordToDbRow(record));
      return true;
    } catch (_) {
      return false;
    }
  }

  _loadRecentFromDb(limit) {
    if (!this._isDbReady()) return [];
    var normalizedLimit = Math.max(1, Math.min(10000, toInteger(limit, this._recentSize || 2000)));
    try {
      var rows = this._stmt.selectRecent.all({ limit: normalizedLimit });
      var out = [];
      for (var i = rows.length - 1; i >= 0; i--) {
        var record = this._rowToRecord(rows[i]);
        if (record) out.push(record);
      }
      return out;
    } catch (_) {
      return [];
    }
  }

  _loadDayFromDb(date) {
    if (!this._isDbReady()) return null;
    if (!isDateStr(date)) return null;
    try {
      var row = this._stmt.selectByDate.get(date);
      if (!row) return null;
      return this._dailyRowToDay(row, date);
    } catch (_) {
      return null;
    }
  }

  _listSearchDatesFromDb() {
    if (!this._isDbReady()) return [];
    try {
      var rows = this._stmt.selectDates.all();
      var out = [];
      for (var i = 0; i < rows.length; i++) {
        var key = String((rows[i] && rows[i].date) || '').trim();
        if (!isDateStr(key)) continue;
        out.push(key);
      }
      return out;
    } catch (_) {
      return [];
    }
  }

  _normalizeLegacyDaySnapshot(raw, date) {
    if (!isObjectLike(raw)) return null;
    var day = emptyDay(date);
    day.date = isDateStr(raw.date) ? raw.date : date;
    day.total_requests = Math.max(0, toInteger(raw.total_requests, 0));
    day.success_requests = Math.max(0, toInteger(raw.success_requests, 0));
    day.total_input_tokens = Math.max(0, toInteger(raw.total_input_tokens, 0));
    day.total_output_tokens = Math.max(0, toInteger(raw.total_output_tokens, 0));
    day.total_cached_tokens = Math.max(0, toInteger(raw.total_cached_tokens, 0));
    day.total_reasoning_tokens = Math.max(0, toInteger(raw.total_reasoning_tokens, 0));
    day.total_latency_ms = Math.max(0, toInteger(raw.total_latency_ms, 0));

    var byModel = isObjectLike(raw.by_model) ? raw.by_model : {};
    var modelKeys = Object.keys(byModel);
    for (var i = 0; i < modelKeys.length; i++) {
      var model = modelKeys[i];
      var modelSrc = isObjectLike(byModel[model]) ? byModel[model] : {};
      day.by_model[model] = {
        requests: Math.max(0, toInteger(modelSrc.requests, 0)),
        input: Math.max(0, toInteger(pickMetric(modelSrc, 'input', 'input_tokens'), 0)),
        output: Math.max(0, toInteger(pickMetric(modelSrc, 'output', 'output_tokens'), 0)),
        cached: Math.max(0, toInteger(pickMetric(modelSrc, 'cached', 'cached_tokens'), 0)),
        reasoning: Math.max(0, toInteger(pickMetric(modelSrc, 'reasoning', 'reasoning_tokens'), 0)),
        latency: Math.max(0, toInteger(modelSrc.latency, 0)),
      };
    }

    var byAccount = isObjectLike(raw.by_account) ? raw.by_account : {};
    var accountKeys = Object.keys(byAccount);
    for (var j = 0; j < accountKeys.length; j++) {
      var account = accountKeys[j];
      var accountSrc = isObjectLike(byAccount[account]) ? byAccount[account] : {};
      day.by_account[account] = {
        requests: Math.max(0, toInteger(accountSrc.requests, 0)),
        input: Math.max(0, toInteger(pickMetric(accountSrc, 'input', 'input_tokens'), 0)),
        output: Math.max(0, toInteger(pickMetric(accountSrc, 'output', 'output_tokens'), 0)),
        cached: Math.max(0, toInteger(pickMetric(accountSrc, 'cached', 'cached_tokens'), 0)),
        reasoning: Math.max(0, toInteger(pickMetric(accountSrc, 'reasoning', 'reasoning_tokens'), 0)),
        errors: Math.max(0, toInteger(accountSrc.errors, 0)),
      };
    }

    var byCaller = isObjectLike(raw.by_caller_identity)
      ? raw.by_caller_identity
      : (isObjectLike(raw.by_caller) ? raw.by_caller : {});
    var callerKeys = Object.keys(byCaller);
    for (var k = 0; k < callerKeys.length; k++) {
      var callerKey = callerKeys[k];
      var callerSrc = isObjectLike(byCaller[callerKey]) ? byCaller[callerKey] : {};
      var callerIdentity = this._canonicalizeCallerIdentity(callerSrc.identity || callerKey)
        || this._normalizeCallerIdentity(callerSrc.identity || callerKey)
        || callerKey;
      if (!day.by_caller_identity[callerIdentity]) {
        day.by_caller_identity[callerIdentity] = {
          identity: callerIdentity,
          requests: 0,
          input: 0,
          output: 0,
          cached: 0,
          reasoning: 0,
          errors: 0,
        };
      }
      var callerTarget = day.by_caller_identity[callerIdentity];
      callerTarget.requests += Math.max(0, toInteger(callerSrc.requests, 0));
      callerTarget.input += Math.max(0, toInteger(pickMetric(callerSrc, 'input', 'input_tokens'), 0));
      callerTarget.output += Math.max(0, toInteger(pickMetric(callerSrc, 'output', 'output_tokens'), 0));
      callerTarget.cached += Math.max(0, toInteger(pickMetric(callerSrc, 'cached', 'cached_tokens'), 0));
      callerTarget.reasoning += Math.max(0, toInteger(pickMetric(callerSrc, 'reasoning', 'reasoning_tokens'), 0));
      callerTarget.errors += Math.max(0, toInteger(callerSrc.errors, 0));
    }

    var byHour = Array.isArray(raw.by_hour) ? raw.by_hour : [];
    for (var h = 0; h < 24; h++) {
      var hourSrc = isObjectLike(byHour[h]) ? byHour[h] : {};
      day.by_hour[h] = {
        requests: Math.max(0, toInteger(hourSrc.requests, 0)),
        success: Math.max(0, toInteger(hourSrc.success, 0)),
        input: Math.max(0, toInteger(pickMetric(hourSrc, 'input', 'input_tokens'), 0)),
        output: Math.max(0, toInteger(pickMetric(hourSrc, 'output', 'output_tokens'), 0)),
        cached: Math.max(0, toInteger(pickMetric(hourSrc, 'cached', 'cached_tokens'), 0)),
        reasoning: Math.max(0, toInteger(pickMetric(hourSrc, 'reasoning', 'reasoning_tokens'), 0)),
      };
    }

    this._normalizeCallerBuckets(day);
    return day;
  }

  _getLastHoursRows(hours) {
    if (!this._isDbReady()) return null;
    var meta = this._getLastHoursMeta(hours);
    try {
      var rows = this._db.prepare(`
        SELECT timestamp, model, account, caller, status, latency_ms,
               input_tokens, output_tokens, cached_tokens, reasoning_tokens, error_type
        FROM request_logs
        WHERE timestamp >= @cutoff_ts AND timestamp <= @now_ts
        ORDER BY timestamp ASC
      `).all({
        cutoff_ts: Math.floor(meta.cutoffTs),
        now_ts: Math.floor(meta.nowTs),
      });
      return {
        meta: meta,
        rows: Array.isArray(rows) ? rows : [],
      };
    } catch (_) {
      return null;
    }
  }

  _searchRequestsFromDb(options) {
    if (!this._isDbReady()) return null;
    options = options || {};

    var page = Math.max(1, Math.floor(toFiniteNumber(options.page, 1)));
    var limit = Math.max(1, Math.min(2000, Math.floor(toFiniteNumber(options.limit, 20))));
    var filter = String(options.filter || '').trim();
    var search = String(options.search || '').trim().toLowerCase();
    var from = String(options.from || '').trim();
    var to = String(options.to || '').trim();
    var sinceTs = Number(options.sinceTs);
    if (!isFinite(sinceTs) || sinceTs <= 0) sinceTs = 0;
    if (!sinceTs && options.hours !== undefined && options.hours !== null && options.hours !== '') {
      var windowHours = this._normalizeHours(options.hours);
      sinceTs = Date.now() - windowHours * 3600000;
    }

    var whereParts = [];
    var params = {};

    if (filter === 'success') {
      whereParts.push('(status >= 200 AND status < 400)');
    } else if (filter === 'error') {
      whereParts.push('(status < 200 OR status >= 400)');
    }

    if (sinceTs > 0) {
      whereParts.push('timestamp >= @since_ts');
      params.since_ts = Math.floor(sinceTs);
    } else {
      if (isDateStr(from)) {
        params.from_date = from;
        whereParts.push('date_key >= @from_date');
      }
      if (isDateStr(to)) {
        params.to_date = to;
        whereParts.push('date_key <= @to_date');
      }
      if (params.from_date && params.to_date && params.from_date > params.to_date) {
        var tempDate = params.from_date;
        params.from_date = params.to_date;
        params.to_date = tempDate;
      }
    }

    if (search) {
      params.search = '%' + search + '%';
      params.search_status = '%' + search + '%';
      whereParts.push(`(
        LOWER(path) LIKE @search
        OR LOWER(route) LIKE @search
        OR LOWER(model) LIKE @search
        OR LOWER(account) LIKE @search
        OR LOWER(caller) LIKE @search
        OR LOWER(ip) LIKE @search
        OR LOWER(ua_hash) LIKE @search
        OR LOWER(session_hint) LIKE @search
        OR LOWER(COALESCE(error_type, error, '')) LIKE @search
        OR CAST(status AS TEXT) LIKE @search_status
      )`);
    }

    var whereSql = whereParts.length ? (' WHERE ' + whereParts.join(' AND ')) : '';
    var totalSql = 'SELECT COUNT(1) AS total FROM request_logs' + whereSql;
    var totalRow = this._db.prepare(totalSql).get(params);
    var total = totalRow ? Math.max(0, toInteger(totalRow.total, 0)) : 0;
    var pages = Math.ceil(total / limit) || 1;
    if (page > pages) page = pages;
    var offset = (page - 1) * limit;

    var rowsSql = 'SELECT * FROM request_logs' + whereSql + ' ORDER BY timestamp DESC LIMIT @limit OFFSET @offset';
    var rows = this._db.prepare(rowsSql).all(Object.assign({}, params, { limit: limit, offset: offset }));
    var data = [];
    for (var i = 0; i < rows.length; i++) {
      var record = this._rowToRecord(rows[i]);
      if (record) data.push(record);
    }
    return { data: data, total: total, page: page, pages: pages, limit: limit };
  }

  _normalizeCallerIdentity(identity) {
    return normalizeCallerIdentity(identity, this._defaultCallerIdentity);
  }

  _normalizeCallerBuckets(dayData) {
    if (!dayData || typeof dayData !== 'object') return dayData;
    normalizeCallerBucketsInTree(dayData, this._defaultCallerIdentity);
    var byCaller = dayData.by_caller_identity;
    if (!byCaller || typeof byCaller !== 'object' || Array.isArray(byCaller)) {
      return dayData;
    }
    var callerKeys = Object.keys(byCaller);
    if (callerKeys.length === 0) return dayData;
    var merged = {};
    var changed = false;
    for (var i = 0; i < callerKeys.length; i++) {
      var key = callerKeys[i];
      var src = byCaller[key] && typeof byCaller[key] === 'object' ? byCaller[key] : {};
      var canonical = this._canonicalizeCallerIdentity(src.identity || key)
        || this._normalizeCallerIdentity(src.identity || key)
        || key;
      if (!canonical) continue;
      if (canonical !== key || String(src.identity || '').trim() !== canonical) {
        changed = true;
      }
      if (!merged[canonical]) {
        merged[canonical] = {
          identity: canonical,
          requests: 0,
          input: 0,
          output: 0,
          cached: 0,
          reasoning: 0,
          errors: 0,
        };
      }
      var dst = merged[canonical];
      dst.requests += Math.max(0, toInteger(src.requests, 0));
      dst.input += Math.max(0, toInteger(pickMetric(src, 'input', 'input_tokens'), 0));
      dst.output += Math.max(0, toInteger(pickMetric(src, 'output', 'output_tokens'), 0));
      dst.cached += Math.max(0, toInteger(pickMetric(src, 'cached', 'cached_tokens'), 0));
      dst.reasoning += Math.max(0, toInteger(pickMetric(src, 'reasoning', 'reasoning_tokens'), 0));
      dst.errors += Math.max(0, toInteger(src.errors, 0));
    }
    if (changed) {
      dayData.by_caller_identity = merged;
    }
    return dayData;
  }

  _loadToday() {
    var today = dateStr(Date.now());
    var loadedToday = this._loadDayFromDb(today);
    this._today = loadedToday || emptyDay(today);
    this._normalizeCallerBuckets(this._today);
    this._recent = this._loadRecentFromDb(this._recentSize);
  }

  _startRpmTicker() {
    // 每秒推进 RPM + TPM 滑动窗口
    this._rpmTimer = setInterval(function () {
      var now = Math.floor(Date.now() / 1000);
      while (this._rpmLastSec < now) {
        this._rpmLastSec++;
        this._rpmIndex = (this._rpmIndex + 1) % 60;
        this._rpmBuckets[this._rpmIndex] = 0;
      }
      while (this._tpmLastSec < now) {
        this._tpmLastSec++;
        this._tpmIndex = (this._tpmIndex + 1) % 60;
        this._tpmBuckets[this._tpmIndex] = 0;
      }
    }.bind(this), 1000);
    if (this._rpmTimer.unref) this._rpmTimer.unref();
  }

  /**
   * 记录一条请求
   * @param {object} entry - { ts, route, path, model, account, caller_identity, status, latency, ttfb_ms, input_tokens, output_tokens, cached_tokens, reasoning_tokens, error_type, stream, ip, ua_hash, session_hint }
   */
  record(entry) {
    if (!this._enabled) return;

    var ts = entry.ts || Date.now();
    var day = dateStr(ts);

    // 跨天轮转
    if (day !== this._today.date) {
      this._doSave();
      this._today = this._loadDayFromDb(day) || emptyDay(day);
    }

    var record = {
      ts: ts,
      route: entry.route || '',
      path: entry.path || '',
      model: entry.model || '',
      account: entry.account || '',
      caller_identity: this._normalizeCallerIdentity(entry.caller_identity || ''),
      status: entry.status || 0,
      latency: entry.latency || 0,
      ttfb_ms: entry.ttfb_ms || 0,
      input_tokens: entry.input_tokens || 0,
      output_tokens: entry.output_tokens || 0,
      cached_tokens: entry.cached_tokens || 0,
      reasoning_tokens: entry.reasoning_tokens || 0,
      error_type: entry.error_type || null,
      stream: !!entry.stream,
      ip: entry.ip || '',
      ua_hash: entry.ua_hash || '',
      session_hint: entry.session_hint || '',
    };

    // 更新今日聚合
    mergeEntry(this._today, record);

    // 环形缓冲区
    this._recent.push(record);
    this._appendRequestLog(record);
    if (this._recent.length > this._recentSize) {
      this._recent.shift();
    }

    // RPM 计数
    var nowSec = Math.floor(ts / 1000);
    if (nowSec !== this._rpmLastSec) {
      while (this._rpmLastSec < nowSec) {
        this._rpmLastSec++;
        this._rpmIndex = (this._rpmIndex + 1) % 60;
        this._rpmBuckets[this._rpmIndex] = 0;
      }
    }
    this._rpmBuckets[this._rpmIndex]++;

    // TPM 计数
    var tokens = (record.input_tokens || 0) + (record.output_tokens || 0);
    if (tokens > 0) {
      if (nowSec !== this._tpmLastSec) {
        while (this._tpmLastSec < nowSec) {
          this._tpmLastSec++;
          this._tpmIndex = (this._tpmIndex + 1) % 60;
          this._tpmBuckets[this._tpmIndex] = 0;
        }
      }
      this._tpmBuckets[this._tpmIndex] += tokens;
    }

    // 防抖保存
    this._scheduleSave();
  }

  _scheduleSave() {
    if (this._saveTimer) return;
    this._saveTimer = setTimeout(function () {
      this._saveTimer = null;
      this._doSave();
    }.bind(this), 5000);
    if (this._saveTimer.unref) this._saveTimer.unref();
  }

  _doSave() {
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
    if (!this._enabled || !this._isDbReady()) return;
    try {
      this._stmt.upsertDaily.run(this._dayToDbRow(this._today));
    } catch (_) {
      // ignore save failure to avoid affecting request path
    }
  }

  _appendRequestLog(record) {
    if (!this._insertRequestRow(record)) {
      throw new Error('stats-collector SQLite insert failed');
    }
  }

  forceSave() {
    if (this._enabled) this._doSave();
  }

  /**
   * 清理过期统计
   */
  _cleanup() {
    try {
      var cutoffTs = Date.now() - this._retentionDays * 86400000;
      this._stmt.cleanupRequestLogs.run(cutoffTs);
    } catch (_) {
      // ignore db cleanup failure
    }
  }

  /**
   * 加载指定日期的聚合数据
   */
  _loadDay(date) {
    if (date === this._today.date) return this._today;
    return this._loadDayFromDb(date);
  }

  /**
   * 获取今日概览 + RPM
   */
  getOverview() {
    var t = this._today;
    var rpm = 0;
    for (var i = 0; i < 60; i++) {
      rpm += this._rpmBuckets[i];
    }
    return {
      date: t.date,
      total_requests: t.total_requests,
      success_requests: t.success_requests,
      success_rate: t.total_requests > 0
        ? Math.round(t.success_requests / t.total_requests * 10000) / 100
        : 0,
      total_input_tokens: t.total_input_tokens,
      total_output_tokens: t.total_output_tokens,
      total_cached_tokens: t.total_cached_tokens || 0,
      total_reasoning_tokens: t.total_reasoning_tokens || 0,
      avg_latency: t.total_requests > 0
        ? Math.round(t.total_latency_ms / t.total_requests)
        : 0,
      rpm: rpm,
      tpm: this.getTPM(),
    };
  }

  /**
   * 获取可用日期列表（升序）
   * @returns {string[]}
   */
  getAvailableDates() {
    this._doSave();
    var dateMap = {};
    var dbDates = this._listSearchDatesFromDb();
    for (var i = 0; i < dbDates.length; i++) {
      if (isDateStr(dbDates[i])) dateMap[dbDates[i]] = true;
    }
    dateMap[this._today.date] = true;
    var out = Object.keys(dateMap);
    out.sort();
    return out;
  }

  /**
   * 生成闭区间日期列表
   * @param {string} from YYYY-MM-DD
   * @param {string} to YYYY-MM-DD
   * @returns {string[]}
   */
  _getDatesBetween(from, to) {
    if (!from || !to || from > to) return [];
    var mFrom = /^(\d{4})-(\d{2})-(\d{2})$/.exec(from);
    var mTo = /^(\d{4})-(\d{2})-(\d{2})$/.exec(to);
    if (!mFrom || !mTo) return [];

    var start = Date.UTC(parseInt(mFrom[1], 10), parseInt(mFrom[2], 10) - 1, parseInt(mFrom[3], 10));
    var end = Date.UTC(parseInt(mTo[1], 10), parseInt(mTo[2], 10) - 1, parseInt(mTo[3], 10));
    if (!isFinite(start) || !isFinite(end) || start > end) return [];

    var out = [];
    for (var ts = start; ts <= end; ts += 86400000) {
      var d = new Date(ts);
      out.push(
        d.getUTCFullYear() + '-' +
        String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(d.getUTCDate()).padStart(2, '0')
      );
    }
    return out;
  }

  _loadDaysRangeFromDb(from, to) {
    var dates = this._getDatesBetween(from, to);
    if (dates.length === 0) {
      return { dates: [], byDate: {} };
    }
    this._doSave();
    var rows = [];
    try {
      rows = this._stmt.selectRangeDays.all({
        from_date: dates[0],
        to_date: dates[dates.length - 1],
      });
    } catch (_) {
      rows = [];
    }
    var byDate = {};
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      if (!row || !isDateStr(row.date)) continue;
      var day = this._dailyRowToDay(row, row.date);
      if (!day) continue;
      byDate[row.date] = day;
    }
    return { dates: dates, byDate: byDate };
  }

  _normalizeHours(hours) {
    var n = parseInt(hours, 10);
    if (!n || n < 1) n = 24;
    if (n > 720) n = 720;
    return n;
  }

  _getLastHoursMeta(hours) {
    var n = this._normalizeHours(hours);
    var now = new Date();
    var cutoff = new Date(now.getTime() - n * 3600000);
    var from = dateStr(cutoff.getTime());
    var to = dateStr(now.getTime());
    return {
      hours: n,
      nowTs: now.getTime(),
      cutoffTs: cutoff.getTime(),
      from: from,
      to: to,
      dates: this._getDatesBetween(from, to),
    };
  }

  _hourOverlapsLastHours(date, hour, meta) {
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date);
    if (!m) return false;
    var y = parseInt(m[1], 10);
    var mon = parseInt(m[2], 10);
    var d = parseInt(m[3], 10);
    var slotStart = new Date(y, mon - 1, d, hour, 0, 0, 0).getTime();
    if (!isFinite(slotStart)) return false;
    var slotEnd = slotStart + 3600000;
    return slotEnd > meta.cutoffTs && slotStart <= meta.nowTs;
  }

  _getRecentEntriesLastHours(hours) {
    var n = this._normalizeHours(hours);
    var nowTs = Date.now();
    var cutoffTs = nowTs - n * 3600000;
    var out = [];
    for (var i = 0; i < this._recent.length; i++) {
      var r = this._recent[i];
      var ts = Number(r && r.ts) || 0;
      if (ts >= cutoffTs && ts <= nowTs) out.push(r);
    }
    return out;
  }

  _buildOverviewFromTotals(label, totals, useRealtime) {
    var totalRequests = Math.max(0, toInteger(totals && totals.total_requests, 0));
    var successRequests = Math.max(0, toInteger(totals && totals.success_requests, 0));
    var totalInputTokens = Math.max(0, toInteger(totals && totals.total_input_tokens, 0));
    var totalOutputTokens = Math.max(0, toInteger(totals && totals.total_output_tokens, 0));
    var totalCachedTokens = Math.max(0, toInteger(totals && totals.total_cached_tokens, 0));
    var totalReasoningTokens = Math.max(0, toInteger(totals && totals.total_reasoning_tokens, 0));
    var totalLatencyMs = Math.max(0, toInteger(totals && totals.total_latency_ms, 0));
    return {
      date: label || '',
      total_requests: totalRequests,
      success_requests: successRequests,
      success_rate: totalRequests > 0
        ? Math.round(successRequests / totalRequests * 10000) / 100
        : 0,
      total_input_tokens: totalInputTokens,
      total_output_tokens: totalOutputTokens,
      total_cached_tokens: totalCachedTokens,
      total_reasoning_tokens: totalReasoningTokens,
      avg_latency: totalRequests > 0
        ? Math.round(totalLatencyMs / totalRequests)
        : 0,
      rpm: useRealtime ? this.getRPM() : 0,
      tpm: useRealtime ? this.getTPM() : 0,
    };
  }

  /**
   * 指定日期范围概览
   */
  getOverviewRange(from, to) {
    var dates = this._getDatesBetween(from, to);
    if (dates.length === 0) return this._buildOverviewFromTotals('', null, false);
    this._doSave();
    var row = this._stmt.sumRange.get({ from_date: dates[0], to_date: dates[dates.length - 1] });
    var label = dates[0] + '~' + dates[dates.length - 1];
    var useRealtime = dates.length === 1 && dates[0] === this._today.date;
    return this._buildOverviewFromTotals(label, row, useRealtime);
  }

  /**
   * 总计概览（全量历史）
   */
  getOverviewTotal() {
    this._doSave();
    var dates = this.getAvailableDates();
    var label = dates.length > 0 ? (dates[0] + '~' + dates[dates.length - 1]) : '';
    var row = this._stmt.sumAll.get();
    return this._buildOverviewFromTotals(label, row, false);
  }

  /**
   * 最近 N 小时概览
   */
  getOverviewLastHours(hours) {
    var pack = this._getLastHoursRows(hours);
    if (pack) {
      var rows = pack.rows;
      var totalRequests = rows.length;
      var successRequests = 0;
      var totalInputTokens = 0;
      var totalOutputTokens = 0;
      var totalCachedTokens = 0;
      var totalReasoningTokens = 0;
      var totalLatency = 0;
      for (var i = 0; i < rows.length; i++) {
        var row = rows[i] || {};
        var status = toInteger(row.status, 0);
        if (status >= 200 && status < 400) successRequests++;
        totalInputTokens += Math.max(0, toInteger(row.input_tokens, 0));
        totalOutputTokens += Math.max(0, toInteger(row.output_tokens, 0));
        totalCachedTokens += Math.max(0, toInteger(row.cached_tokens, 0));
        totalReasoningTokens += Math.max(0, toInteger(row.reasoning_tokens, 0));
        totalLatency += Math.max(0, toInteger(row.latency_ms, 0));
      }
      return {
        date: pack.meta.from + '~' + pack.meta.to,
        total_requests: totalRequests,
        success_requests: successRequests,
        success_rate: totalRequests > 0
          ? Math.round(successRequests / totalRequests * 10000) / 100
          : 0,
        total_input_tokens: totalInputTokens,
        total_output_tokens: totalOutputTokens,
        total_cached_tokens: totalCachedTokens,
        total_reasoning_tokens: totalReasoningTokens,
        avg_latency: totalRequests > 0
          ? Math.round(totalLatency / totalRequests)
          : 0,
        rpm: this.getRPM(),
        tpm: this.getTPM(),
      };
    }

    var series = this.getTimeseriesLastHours(hours);
    var sumRequests = 0;
    var sumSuccess = 0;
    var sumInput = 0;
    var sumOutput = 0;
    var sumCached = 0;
    var sumReasoning = 0;
    for (var j = 0; j < series.length; j++) {
      var item = series[j] || {};
      sumRequests += item.requests || 0;
      sumSuccess += item.success || 0;
      sumInput += item.input || 0;
      sumOutput += item.output || 0;
      sumCached += item.cached || 0;
      sumReasoning += item.reasoning || 0;
    }

    var recent = this._getRecentEntriesLastHours(hours);
    var sumLatency = 0;
    for (var k = 0; k < recent.length; k++) {
      sumLatency += (recent[k] && recent[k].latency) || 0;
    }

    var meta = this._getLastHoursMeta(hours);
    return {
      date: meta.from + '~' + meta.to,
      total_requests: sumRequests,
      success_requests: sumSuccess,
      success_rate: sumRequests > 0
        ? Math.round(sumSuccess / sumRequests * 10000) / 100
        : 0,
      total_input_tokens: sumInput,
      total_output_tokens: sumOutput,
      total_cached_tokens: sumCached,
      total_reasoning_tokens: sumReasoning,
      avg_latency: recent.length > 0
        ? Math.round(sumLatency / recent.length)
        : 0,
      rpm: this.getRPM(),
      tpm: this.getTPM(),
    };
  }

  /**
   * 指定日期范围时间序列
   */
  getTimeseriesRange(from, to) {
    var pack = this._loadDaysRangeFromDb(from, to);
    var dates = pack.dates;
    var byDate = pack.byDate;
    var result = [];
    for (var i = 0; i < dates.length; i++) {
      var date = dates[i];
      var data = byDate[date] || null;
      if (data && data.by_hour) {
        for (var h = 0; h < 24; h++) {
          var slot = data.by_hour[h] || {};
          result.push({
            date: date,
            hour: h,
            label: date + ' ' + String(h).padStart(2, '0') + ':00',
            requests: slot.requests || 0,
            success: slot.success || 0,
            input: slot.input || 0,
            output: slot.output || 0,
            cached: slot.cached || 0,
            reasoning: slot.reasoning || 0,
          });
        }
      } else {
        for (var h2 = 0; h2 < 24; h2++) {
          result.push({
            date: date,
            hour: h2,
            label: date + ' ' + String(h2).padStart(2, '0') + ':00',
            requests: 0,
            success: 0,
            input: 0,
            output: 0,
            cached: 0,
            reasoning: 0,
          });
        }
      }
    }
    return result;
  }

  /**
   * 总计时间序列（全量历史）
   */
  getTimeseriesTotal() {
    var dates = this.getAvailableDates();
    if (dates.length === 0) return [];
    return this.getTimeseriesRange(dates[0], dates[dates.length - 1]);
  }

  /**
   * 最近 N 小时时间序列
   */
  getTimeseriesLastHours(hours) {
    var pack = this._getLastHoursRows(hours);
    if (pack) {
      var metaFromDb = pack.meta;
      var bucketMap = {};
      var resultFromDb = [];
      for (var d = 0; d < metaFromDb.dates.length; d++) {
        var dateKey = metaFromDb.dates[d];
        for (var hourKey = 0; hourKey < 24; hourKey++) {
          if (!this._hourOverlapsLastHours(dateKey, hourKey, metaFromDb)) continue;
          var key = dateKey + '|' + hourKey;
          var bucket = {
            date: dateKey,
            hour: hourKey,
            label: dateKey + ' ' + String(hourKey).padStart(2, '0') + ':00',
            requests: 0,
            success: 0,
            input: 0,
            output: 0,
            cached: 0,
            reasoning: 0,
          };
          bucketMap[key] = bucket;
          resultFromDb.push(bucket);
        }
      }

      var rows = pack.rows;
      for (var i = 0; i < rows.length; i++) {
        var row = rows[i] || {};
        var ts = toInteger(row.timestamp, 0);
        if (ts <= 0 || ts < metaFromDb.cutoffTs || ts > metaFromDb.nowTs) continue;
        var date = dateStr(ts);
        var hour = new Date(ts).getHours();
        var bucketKey = date + '|' + hour;
        var slot = bucketMap[bucketKey];
        if (!slot) continue;
        slot.requests += 1;
        var status = toInteger(row.status, 0);
        if (status >= 200 && status < 400) slot.success += 1;
        slot.input += Math.max(0, toInteger(row.input_tokens, 0));
        slot.output += Math.max(0, toInteger(row.output_tokens, 0));
        slot.cached += Math.max(0, toInteger(row.cached_tokens, 0));
        slot.reasoning += Math.max(0, toInteger(row.reasoning_tokens, 0));
      }
      return resultFromDb;
    }

    var meta = this._getLastHoursMeta(hours);
    var result = [];
    for (var i = 0; i < meta.dates.length; i++) {
      var date = meta.dates[i];
      var data = this._loadDay(date);
      for (var h = 0; h < 24; h++) {
        if (!this._hourOverlapsLastHours(date, h, meta)) continue;
        var slot = (data && data.by_hour && data.by_hour[h]) ? data.by_hour[h] : null;
        result.push({
          date: date,
          hour: h,
          label: date + ' ' + String(h).padStart(2, '0') + ':00',
          requests: slot ? (slot.requests || 0) : 0,
          success: slot ? (slot.success || 0) : 0,
          input: slot ? (slot.input || 0) : 0,
          output: slot ? (slot.output || 0) : 0,
          cached: slot ? (slot.cached || 0) : 0,
          reasoning: slot ? (slot.reasoning || 0) : 0,
        });
      }
    }
    return result;
  }

  /**
   * 按小时时间序列
   * @param {number} days - 查询天数(1=今天, 7=最近7天)
   */
  getTimeseries(days) {
    var n = Math.max(1, parseInt(days, 10) || 1);
    var to = dateStr(Date.now());
    var from = dateStr(Date.now() - (n - 1) * 86400000);
    return this.getTimeseriesRange(from, to);
  }

  /**
   * 指定日期范围模型聚合
   */
  getModelStatsRange(from, to) {
    var merged = {};
    var pack = this._loadDaysRangeFromDb(from, to);
    var dates = pack.dates;
    var byDate = pack.byDate;
    for (var d = 0; d < dates.length; d++) {
      var data = byDate[dates[d]] || null;
      if (data && data.by_model) {
        var models = Object.keys(data.by_model);
        for (var i = 0; i < models.length; i++) {
          var model = models[i];
          var src = data.by_model[model];
          if (!merged[model]) {
            merged[model] = { model: model, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, latency: 0 };
          }
          merged[model].requests += src.requests || 0;
          merged[model].input += src.input || 0;
          merged[model].output += src.output || 0;
          merged[model].cached += src.cached || 0;
          merged[model].reasoning += src.reasoning || 0;
          merged[model].latency += src.latency || 0;
        }
      }
    }
    var arr = Object.values(merged).map(function (m) {
      return Object.assign({}, m, {
        avg_latency: m.requests > 0 ? Math.round(m.latency / m.requests) : 0,
      });
    });
    arr.sort(function (a, b) { return b.requests - a.requests; });
    return arr;
  }

  getModelStatsTotal() {
    var dates = this.getAvailableDates();
    if (dates.length === 0) return [];
    return this.getModelStatsRange(dates[0], dates[dates.length - 1]);
  }

  /**
   * 最近 N 小时模型聚合
   */
  getModelStatsLastHours(hours) {
    var merged = {};
    var pack = this._getLastHoursRows(hours);
    if (pack) {
      var rows = pack.rows;
      for (var i = 0; i < rows.length; i++) {
        var row = rows[i] || {};
        var model = String(row.model || '');
        if (!model) continue;
        if (!merged[model]) {
          merged[model] = { model: model, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, latency: 0 };
        }
        merged[model].requests += 1;
        merged[model].input += Math.max(0, toInteger(row.input_tokens, 0));
        merged[model].output += Math.max(0, toInteger(row.output_tokens, 0));
        merged[model].cached += Math.max(0, toInteger(row.cached_tokens, 0));
        merged[model].reasoning += Math.max(0, toInteger(row.reasoning_tokens, 0));
        merged[model].latency += Math.max(0, toInteger(row.latency_ms, 0));
      }
    } else {
      var recent = this._getRecentEntriesLastHours(hours);
      for (var r = 0; r < recent.length; r++) {
        var rec = recent[r] || {};
        var mName = rec.model || '';
        if (!mName) continue;
        if (!merged[mName]) {
          merged[mName] = { model: mName, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, latency: 0 };
        }
        merged[mName].requests += 1;
        merged[mName].input += rec.input_tokens || 0;
        merged[mName].output += rec.output_tokens || 0;
        merged[mName].cached += rec.cached_tokens || 0;
        merged[mName].reasoning += rec.reasoning_tokens || 0;
        merged[mName].latency += rec.latency || 0;
      }
    }

    var arr = Object.values(merged).map(function (m) {
      return Object.assign({}, m, {
        avg_latency: m.requests > 0 ? Math.round(m.latency / m.requests) : 0,
      });
    });
    arr.sort(function (a, b) { return b.requests - a.requests; });
    return arr;
  }

  /**
   * 按模型聚合
   */
  getModelStats(days) {
    var n = Math.max(1, parseInt(days, 10) || 1);
    var to = dateStr(Date.now());
    var from = dateStr(Date.now() - (n - 1) * 86400000);
    return this.getModelStatsRange(from, to);
  }

  /**
   * 指定日期范围账号聚合
   */
  getAccountStatsRange(from, to) {
    var merged = {};
    var pack = this._loadDaysRangeFromDb(from, to);
    var dates = pack.dates;
    var byDate = pack.byDate;
    for (var d = 0; d < dates.length; d++) {
      var data = byDate[dates[d]] || null;
      if (data && data.by_account) {
        var accts = Object.keys(data.by_account);
        for (var i = 0; i < accts.length; i++) {
          var email = accts[i];
          var src = data.by_account[email];
          if (!merged[email]) {
            merged[email] = { email: email, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
          }
          merged[email].requests += src.requests || 0;
          merged[email].input += src.input || 0;
          merged[email].output += src.output || 0;
          merged[email].cached += src.cached || 0;
          merged[email].reasoning += src.reasoning || 0;
          merged[email].errors += src.errors || 0;
        }
      }
    }
    var arr = Object.values(merged);
    arr.sort(function (a, b) { return b.requests - a.requests; });
    return arr;
  }

  getAccountStatsTotal() {
    var dates = this.getAvailableDates();
    if (dates.length === 0) return [];
    return this.getAccountStatsRange(dates[0], dates[dates.length - 1]);
  }

  /**
   * 最近 N 小时账号聚合
   */
  getAccountStatsLastHours(hours) {
    var merged = {};
    var pack = this._getLastHoursRows(hours);
    if (pack) {
      var rows = pack.rows;
      for (var i = 0; i < rows.length; i++) {
        var row = rows[i] || {};
        var email = String(row.account || '');
        if (!email) continue;
        if (!merged[email]) {
          merged[email] = { email: email, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
        }
        merged[email].requests += 1;
        merged[email].input += Math.max(0, toInteger(row.input_tokens, 0));
        merged[email].output += Math.max(0, toInteger(row.output_tokens, 0));
        merged[email].cached += Math.max(0, toInteger(row.cached_tokens, 0));
        merged[email].reasoning += Math.max(0, toInteger(row.reasoning_tokens, 0));
        if (row.error_type) merged[email].errors += 1;
      }
    } else {
      var recent = this._getRecentEntriesLastHours(hours);
      for (var r = 0; r < recent.length; r++) {
        var rec = recent[r] || {};
        var account = rec.account || '';
        if (!account) continue;
        if (!merged[account]) {
          merged[account] = { email: account, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
        }
        merged[account].requests += 1;
        merged[account].input += rec.input_tokens || 0;
        merged[account].output += rec.output_tokens || 0;
        merged[account].cached += rec.cached_tokens || 0;
        merged[account].reasoning += rec.reasoning_tokens || 0;
        if (rec.error_type) merged[account].errors += 1;
      }
    }

    var arr = Object.values(merged);
    arr.sort(function (a, b) { return b.requests - a.requests; });
    return arr;
  }

  /**
   * 按账号聚合
   */
  getAccountStats(days) {
    var n = Math.max(1, parseInt(days, 10) || 1);
    var to = dateStr(Date.now());
    var from = dateStr(Date.now() - (n - 1) * 86400000);
    return this.getAccountStatsRange(from, to);
  }

  /**
   * 指定日期范围调用身份聚合
   */
  getCallerStatsRange(from, to) {
    var merged = {};
    var pack = this._loadDaysRangeFromDb(from, to);
    var dates = pack.dates;
    var byDate = pack.byDate;
    for (var d = 0; d < dates.length; d++) {
      var data = byDate[dates[d]] || null;
      if (data && data.by_caller_identity) {
        var callers = Object.keys(data.by_caller_identity);
        for (var i = 0; i < callers.length; i++) {
          var identity = callers[i];
          var src = data.by_caller_identity[identity];
          if (!merged[identity]) {
            merged[identity] = { identity: identity, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
          }
          merged[identity].requests += src.requests || 0;
          merged[identity].input += src.input || 0;
          merged[identity].output += src.output || 0;
          merged[identity].cached += src.cached || 0;
          merged[identity].reasoning += src.reasoning || 0;
          merged[identity].errors += src.errors || 0;
        }
      }
    }
    var arr = Object.values(merged);
    arr.sort(function (a, b) { return b.requests - a.requests; });
    return arr;
  }

  getCallerStatsTotal() {
    var dates = this.getAvailableDates();
    if (dates.length === 0) return [];
    return this.getCallerStatsRange(dates[0], dates[dates.length - 1]);
  }

  /**
   * 最近 N 小时调用身份聚合
   */
  getCallerStatsLastHours(hours) {
    var merged = {};
    var pack = this._getLastHoursRows(hours);
    if (pack) {
      var rows = pack.rows;
      for (var i = 0; i < rows.length; i++) {
        var row = rows[i] || {};
        var identity = this._canonicalizeCallerIdentity(String(row.caller || ''));
        if (!identity) continue;
        if (!merged[identity]) {
          merged[identity] = { identity: identity, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
        }
        merged[identity].requests += 1;
        merged[identity].input += Math.max(0, toInteger(row.input_tokens, 0));
        merged[identity].output += Math.max(0, toInteger(row.output_tokens, 0));
        merged[identity].cached += Math.max(0, toInteger(row.cached_tokens, 0));
        merged[identity].reasoning += Math.max(0, toInteger(row.reasoning_tokens, 0));
        if (row.error_type) merged[identity].errors += 1;
      }
    } else {
      var recent = this._getRecentEntriesLastHours(hours);
      for (var r = 0; r < recent.length; r++) {
        var rec = recent[r] || {};
        var caller = this._canonicalizeCallerIdentity(rec.caller_identity || '');
        if (!caller) continue;
        if (!merged[caller]) {
          merged[caller] = { identity: caller, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
        }
        merged[caller].requests += 1;
        merged[caller].input += rec.input_tokens || 0;
        merged[caller].output += rec.output_tokens || 0;
        merged[caller].cached += rec.cached_tokens || 0;
        merged[caller].reasoning += rec.reasoning_tokens || 0;
        if (rec.error_type) merged[caller].errors += 1;
      }
    }

    var arr = Object.values(merged);
    arr.sort(function (a, b) { return b.requests - a.requests; });
    return arr;
  }

  getCallerStats(days) {
    var n = Math.max(1, parseInt(days, 10) || 1);
    var to = dateStr(Date.now());
    var from = dateStr(Date.now() - (n - 1) * 86400000);
    return this.getCallerStatsRange(from, to);
  }

  /**
   * 搜索请求日志（SQLite）
   * @param {object} options - { page, limit, filter, search, from, to, hours, sinceTs }
   * @returns {{ data: Array, total: number, page: number, pages: number, limit: number }}
   */
  searchRequests(options) {
    var dbResult = this._searchRequestsFromDb(options || {});
    if (dbResult) return dbResult;
    var fallbackLimit = Math.max(1, parseInt(options && options.limit, 10) || 20);
    return { data: [], total: 0, page: 1, pages: 1, limit: fallbackLimit };
  }

  _getSearchDates(from, to) {
    if (from && to) {
      return this._getDatesBetween(from, to);
    }
    return this.getAvailableDates();
  }

  /**
   * 最近N条请求详情
   */
  getRecentRequests(page, limit, filter, search, source, date, hours) {
    var normalizedPage = Math.max(1, parseInt(page, 10) || 1);
    var normalizedSearch = typeof search === 'string' ? search.trim() : '';
    var normalizedSource = source === 'file' ? 'file' : 'memory';
    var normalizedHours = null;
    if (hours !== undefined && hours !== null && hours !== '') {
      normalizedHours = this._normalizeHours(hours);
    }
    var sinceTs = normalizedHours ? (Date.now() - normalizedHours * 3600000) : 0;
    var fileMode = normalizedSource === 'file';

    if (fileMode) {
      var fileLimit = parseInt(limit, 10);
      if (!fileLimit || fileLimit < 1) fileLimit = 50;
      if (fileLimit > 200) fileLimit = 200;

      var normalizedDate = typeof date === 'string' ? date.trim() : '';
      var opts = {
        page: normalizedPage,
        limit: fileLimit,
        filter: filter,
        search: normalizedSearch,
      };
      if (sinceTs > 0) {
        opts.sinceTs = sinceTs;
      } else if (isDateStr(normalizedDate)) {
        opts.from = normalizedDate;
        opts.to = normalizedDate;
      }
      return this.searchRequests(opts);
    }

    var memoryLimit = parseInt(limit, 10);
    if (!memoryLimit || memoryLimit < 1) memoryLimit = 20;
    // _recent 按时间升序，倒序后最新在前
    var recentSource = this._recent.slice().reverse();
    if (sinceTs > 0) {
      recentSource = recentSource.filter(function (r) {
        return (Number(r && r.ts) || 0) >= sinceTs;
      });
    }
    if (normalizedSearch) {
      var searchKw = normalizedSearch.toLowerCase();
      recentSource = recentSource.filter(function (r) {
        return matchRecordKeyword(r || {}, searchKw);
      });
    }
    // 按状态过滤
    if (filter === 'success') {
      recentSource = recentSource.filter(function (r) { return r.status >= 200 && r.status < 400; });
    } else if (filter === 'error') {
      recentSource = recentSource.filter(function (r) { return !r.status || r.status >= 400; });
    }
    var total = recentSource.length;
    var pages = Math.ceil(total / memoryLimit) || 1;
    if (normalizedPage > pages) normalizedPage = pages;
    var start = (normalizedPage - 1) * memoryLimit;
    var data = recentSource.slice(start, start + memoryLimit);
    return { data: data, total: total, page: normalizedPage, pages: pages, limit: memoryLimit };
  }

  /**
   * 当前 RPM
   */
  getRPM() {
    var total = 0;
    for (var i = 0; i < 60; i++) {
      total += this._rpmBuckets[i];
    }
    return total;
  }

  /**
   * 当前 TPM (Tokens Per Minute)
   */
  getTPM() {
    var total = 0;
    for (var i = 0; i < 60; i++) {
      total += this._tpmBuckets[i];
    }
    return total;
  }

  /**
   * 停止定时器
   */
  stop() {
    this._doSave();
    if (this._rpmTimer) {
      clearInterval(this._rpmTimer);
      this._rpmTimer = null;
    }
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
    if (this._db) {
      try {
        this._db.close();
      } catch (_) {
        // ignore
      }
      this._db = null;
      this._stmt = {};
    }
  }
}
