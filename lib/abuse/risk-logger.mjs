import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, unlinkSync } from 'node:fs';
import { resolve } from 'node:path';

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

function parseDateTs(date) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) return NaN;
  return new Date(date + 'T00:00:00.000Z').getTime();
}

function normalizeJsonObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

export class RiskLogger {
  constructor(opts) {
    var options = opts || {};
    this._dataDir = resolve(options.dataDir || 'data', 'abuse');
    var cfg = normalizeJsonObject(options.config);
    this._retentionDays = Math.max(1, Math.floor(toFiniteNumber(cfg.retention_days, 90)));
    this._cleanupIntervalHours = Math.max(1, Math.floor(toFiniteNumber(cfg.cleanup_interval_hours, 6)));
    this._cleanupTimer = null;
    this._ensureDir();
    this.cleanupOldFiles();
    this._startCleanupTicker();
  }

  _ensureDir() {
    if (!existsSync(this._dataDir)) {
      mkdirSync(this._dataDir, { recursive: true });
    }
  }

  _filePath(ts) {
    return resolve(this._dataDir, 'risk-events-' + dateStr(ts) + '.jsonl');
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
    try {
      this._ensureDir();
      var nowTs = Date.now();
      var ts = toFiniteNumber(event && event.ts, nowTs);
      var payload = {
        ts: ts,
        caller_identity: String((event && event.caller_identity) || '').trim(),
        ip: String((event && event.ip) || '').trim(),
        ua_hash: String((event && event.ua_hash) || '').trim(),
        rule_id: String((event && event.rule_id) || '').trim(),
        score: toFiniteNumber(event && event.score, 0),
        evidence: (event && event.evidence) || {},
        action: String((event && event.action) || 'observe').trim() || 'observe',
      };
      appendFileSync(this._filePath(ts), JSON.stringify(payload) + '\n');
      return payload;
    } catch (_) {
      return null;
    }
  }

  _getDateRange(from, to) {
    var today = dateStr(Date.now());
    var start = /^\d{4}-\d{2}-\d{2}$/.test(from || '') ? from : today;
    var end = /^\d{4}-\d{2}-\d{2}$/.test(to || '') ? to : today;
    if (start > end) {
      var temp = start;
      start = end;
      end = temp;
    }

    var out = [];
    var startTs = parseDateTs(start);
    var endTs = parseDateTs(end);
    if (!isFinite(startTs) || !isFinite(endTs)) return out;
    for (var ts = startTs; ts <= endTs; ts += 24 * 60 * 60 * 1000) {
      out.push(dateStr(ts));
    }
    return out;
  }

  listEvents(options) {
    var opts = normalizeJsonObject(options);
    var page = Math.max(1, Math.floor(toFiniteNumber(opts.page, 1)));
    var limit = Math.max(1, Math.min(2000, Math.floor(toFiniteNumber(opts.limit, 100))));
    var identity = String(opts.caller_identity || '').trim();
    var action = String(opts.action || '').trim();
    var ruleId = String(opts.rule_id || '').trim();
    var from = String(opts.from || '').trim();
    var to = String(opts.to || '').trim();

    var dates = this._getDateRange(from, to);
    var all = [];

    for (var i = dates.length - 1; i >= 0; i--) {
      var file = resolve(this._dataDir, 'risk-events-' + dates[i] + '.jsonl');
      if (!existsSync(file)) continue;
      try {
        var lines = readFileSync(file, 'utf8').split('\n');
        for (var j = lines.length - 1; j >= 0; j--) {
          var line = lines[j].trim();
          if (!line) continue;
          try {
            var row = JSON.parse(line);
            if (identity && String(row.caller_identity || '') !== identity) continue;
            if (action && String(row.action || '') !== action) continue;
            if (ruleId && String(row.rule_id || '') !== ruleId) continue;
            all.push(row);
          } catch (_) {
            // ignore line parse error
          }
        }
      } catch (_) {
        // ignore file read error
      }
    }

    var total = all.length;
    var pages = Math.ceil(total / limit) || 1;
    if (page > pages) page = pages;
    var start = (page - 1) * limit;
    var data = all.slice(start, start + limit);
    return {
      data: data,
      total: total,
      page: page,
      pages: pages,
      limit: limit,
    };
  }

  getTodayCount() {
    var today = dateStr(Date.now());
    var file = resolve(this._dataDir, 'risk-events-' + today + '.jsonl');
    if (!existsSync(file)) return 0;
    try {
      var lines = readFileSync(file, 'utf8').split('\n');
      var count = 0;
      for (var i = 0; i < lines.length; i++) {
        if (lines[i].trim()) count += 1;
      }
      return count;
    } catch (_) {
      return 0;
    }
  }

  cleanupOldFiles() {
    this._ensureDir();
    var cutoffTs = Date.now() - (this._retentionDays * 24 * 60 * 60 * 1000);
    try {
      var files = readdirSync(this._dataDir);
      for (var i = 0; i < files.length; i++) {
        var match = /^risk-events-(\d{4}-\d{2}-\d{2})\.jsonl$/.exec(files[i]);
        if (!match) continue;
        var ts = parseDateTs(match[1]);
        if (!isFinite(ts)) continue;
        if (ts < cutoffTs) {
          unlinkSync(resolve(this._dataDir, files[i]));
        }
      }
    } catch (_) {
      // ignore cleanup failure
    }
  }

  stop() {
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
      this._cleanupTimer = null;
    }
  }
}
