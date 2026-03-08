import { existsSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { createRequire } from 'node:module';

var require = createRequire(import.meta.url);

function toFiniteNumber(value, fallback) {
  var n = Number(value);
  if (!isFinite(n)) return fallback || 0;
  return n;
}

function dateStr(ts) {
  var d = new Date(ts);
  var y = d.getFullYear();
  var m = String(d.getMonth() + 1).padStart(2, '0');
  var day = String(d.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + day;
}

function normalizeJsonObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

function safeJsonStringify(value, fallback) {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return fallback;
  }
}

function safeJsonParseObject(text, fallback) {
  try {
    var parsed = JSON.parse(String(text || ''));
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
    return fallback;
  } catch (_) {
    return fallback;
  }
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

export class RiskLogger {
  constructor(opts) {
    var options = opts || {};
    var baseDir = resolve(options.dataDir || 'data');
    this._dbPath = resolve(String(options.dbPath || options.sqlitePath || resolve(baseDir, 'accounts.db')));
    var cfg = normalizeJsonObject(options.config);
    this._retentionDays = Math.max(1, Math.floor(toFiniteNumber(cfg.retention_days, 90)));
    this._cleanupIntervalHours = Math.max(1, Math.floor(toFiniteNumber(cfg.cleanup_interval_hours, 6)));
    this._cleanupTimer = null;
    this._db = null;
    this._stmt = {};

    this._initDb();
    this.cleanupOldFiles();
    this._startCleanupTicker();
  }

  _ensureDbDir() {
    var dir = dirname(this._dbPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  _isDbReady() {
    return !!(this._db && this._stmt && this._stmt.insertEvent);
  }

  _initDb() {
    try {
      var Database = loadBetterSqlite3();
      this._ensureDbDir();
      this._db = new Database(this._dbPath);
      this._db.pragma('journal_mode = WAL');
      this._db.pragma('busy_timeout = 5000');

      this._db.exec(`
        CREATE TABLE IF NOT EXISTS abuse_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          date_key TEXT NOT NULL,
          caller_identity TEXT NOT NULL,
          ip TEXT NOT NULL DEFAULT '',
          ua_hash TEXT NOT NULL DEFAULT '',
          rule_id TEXT NOT NULL,
          score INTEGER NOT NULL DEFAULT 0,
          action TEXT NOT NULL DEFAULT 'observe',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          created_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_abuse_events_identity_time ON abuse_events(caller_identity, ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_abuse_events_rule_time ON abuse_events(rule_id, ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_abuse_events_action_time ON abuse_events(action, ts_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_abuse_events_date ON abuse_events(date_key, ts_ms DESC);
      `);

      this._stmt.insertEvent = this._db.prepare(`
        INSERT INTO abuse_events (
          ts_ms, date_key, caller_identity, ip, ua_hash, rule_id, score, action, evidence_json, created_at_ms
        ) VALUES (
          @ts_ms, @date_key, @caller_identity, @ip, @ua_hash, @rule_id, @score, @action, @evidence_json, @created_at_ms
        )
      `);
      this._stmt.countAll = this._db.prepare('SELECT COUNT(1) AS total FROM abuse_events');
      this._stmt.countToday = this._db.prepare('SELECT COUNT(1) AS total FROM abuse_events WHERE date_key = ?');
      this._stmt.cleanup = this._db.prepare('DELETE FROM abuse_events WHERE ts_ms < ?');
    } catch (_) {
      this._db = null;
      this._stmt = {};
      throw new Error('risk-logger SQLite init failed');
    }
  }

  _eventToDbRow(event) {
    var nowTs = Date.now();
    var ts = Math.floor(toFiniteNumber(event && event.ts, nowTs));
    if (!isFinite(ts) || ts <= 0) ts = nowTs;
    return {
      ts_ms: ts,
      date_key: dateStr(ts),
      caller_identity: String((event && event.caller_identity) || '').trim(),
      ip: String((event && event.ip) || '').trim(),
      ua_hash: String((event && event.ua_hash) || '').trim(),
      rule_id: String((event && event.rule_id) || '').trim(),
      score: Math.floor(toFiniteNumber(event && event.score, 0)),
      action: String((event && event.action) || 'observe').trim() || 'observe',
      evidence_json: safeJsonStringify((event && event.evidence) || {}, '{}'),
      created_at_ms: nowTs,
    };
  }

  _rowToEvent(row) {
    if (!row || typeof row !== 'object') return null;
    return {
      ts: Math.floor(toFiniteNumber(row.ts_ms, Date.now())),
      caller_identity: String(row.caller_identity || '').trim(),
      ip: String(row.ip || '').trim(),
      ua_hash: String(row.ua_hash || '').trim(),
      rule_id: String(row.rule_id || '').trim(),
      score: Math.floor(toFiniteNumber(row.score, 0)),
      evidence: safeJsonParseObject(row.evidence_json, {}),
      action: String(row.action || 'observe').trim() || 'observe',
    };
  }

  _startCleanupTicker() {
    if (this._cleanupTimer) return;
    var intervalMs = this._cleanupIntervalHours * 60 * 60 * 1000;
    this._cleanupTimer = setInterval(function () {
      this.cleanupOldFiles();
    }.bind(this), intervalMs);
    if (this._cleanupTimer.unref) this._cleanupTimer.unref();
  }

  updateConfig(cfg) {
    var normalized = normalizeJsonObject(cfg);
    var retentionDays = Math.max(1, Math.floor(toFiniteNumber(normalized.retention_days, this._retentionDays)));
    var cleanupHours = Math.max(1, Math.floor(toFiniteNumber(normalized.cleanup_interval_hours, this._cleanupIntervalHours)));
    var restartTimer = cleanupHours !== this._cleanupIntervalHours;

    this._retentionDays = retentionDays;
    this._cleanupIntervalHours = cleanupHours;
    if (restartTimer) {
      this.stop();
      this._startCleanupTicker();
    }
  }

  logEvent(event) {
    var payload = {
      ts: Math.floor(toFiniteNumber(event && event.ts, Date.now())),
      caller_identity: String((event && event.caller_identity) || '').trim(),
      ip: String((event && event.ip) || '').trim(),
      ua_hash: String((event && event.ua_hash) || '').trim(),
      rule_id: String((event && event.rule_id) || '').trim(),
      score: Math.floor(toFiniteNumber(event && event.score, 0)),
      evidence: (event && event.evidence) || {},
      action: String((event && event.action) || 'observe').trim() || 'observe',
    };

    try {
      this._stmt.insertEvent.run(this._eventToDbRow(payload));
      return payload;
    } catch (_) {
      return null;
    }
  }

  _listEventsFromDb(options) {
    var opts = normalizeJsonObject(options);
    var page = Math.max(1, Math.floor(toFiniteNumber(opts.page, 1)));
    var limit = Math.max(1, Math.min(2000, Math.floor(toFiniteNumber(opts.limit, 100))));
    var identity = String(opts.caller_identity || '').trim();
    var action = String(opts.action || '').trim();
    var ruleId = String(opts.rule_id || '').trim();
    var from = String(opts.from || '').trim();
    var to = String(opts.to || '').trim();

    var whereParts = [];
    var params = {};

    if (identity) {
      whereParts.push('caller_identity = @caller_identity');
      params.caller_identity = identity;
    }
    if (action) {
      whereParts.push('action = @action');
      params.action = action;
    }
    if (ruleId) {
      whereParts.push('rule_id = @rule_id');
      params.rule_id = ruleId;
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      whereParts.push('date_key >= @from_date');
      params.from_date = from;
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      whereParts.push('date_key <= @to_date');
      params.to_date = to;
    }
    if (params.from_date && params.to_date && params.from_date > params.to_date) {
      var temp = params.from_date;
      params.from_date = params.to_date;
      params.to_date = temp;
    }

    var whereSql = whereParts.length ? (' WHERE ' + whereParts.join(' AND ')) : '';
    var totalRow = this._db.prepare('SELECT COUNT(1) AS total FROM abuse_events' + whereSql).get(params);
    var total = totalRow ? Math.max(0, Math.floor(toFiniteNumber(totalRow.total, 0))) : 0;
    var pages = Math.ceil(total / limit) || 1;
    if (page > pages) page = pages;
    var offset = (page - 1) * limit;

    var rows = this._db
      .prepare('SELECT * FROM abuse_events' + whereSql + ' ORDER BY ts_ms DESC LIMIT @limit OFFSET @offset')
      .all(Object.assign({}, params, { limit: limit, offset: offset }));

    var data = [];
    for (var i = 0; i < rows.length; i++) {
      var event = this._rowToEvent(rows[i]);
      if (event) data.push(event);
    }

    return {
      data: data,
      total: total,
      page: page,
      pages: pages,
      limit: limit,
    };
  }

  listEvents(options) {
    return this._listEventsFromDb(options);
  }

  getTodayCount() {
    try {
      var row = this._stmt.countToday.get(dateStr(Date.now()));
      return row ? Math.max(0, Math.floor(toFiniteNumber(row.total, 0))) : 0;
    } catch (_) {
      return 0;
    }
  }

  cleanupOldFiles() {
    var cutoffTs = Date.now() - (this._retentionDays * 24 * 60 * 60 * 1000);
    try {
      this._stmt.cleanup.run(cutoffTs);
    } catch (_) {
      // ignore db cleanup failure
    }
  }

  stop() {
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
      this._cleanupTimer = null;
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
