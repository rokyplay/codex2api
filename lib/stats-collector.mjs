/**
 * 请求级统计收集器
 *
 * 功能:
 *   - 记录每条请求的详细信息(模型/账号/token用量/延迟/状态码)
 *   - 内存聚合: 今日汇总 + 按小时/模型/账号维度 + 滑动窗口RPM
 *   - 按天 JSON 持久化到 data/stats/YYYY-MM-DD.json
 *   - 环形缓冲区保留最近 N 条详细记录
 *   - 启动时从文件恢复, 跨天自动轮转
 *
 * 零依赖: Node.js 22 内置 fs
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync, appendFileSync, readdirSync, unlinkSync } from 'node:fs';
import { resolve, basename } from 'node:path';

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
    var cfg = (opts && opts.config && opts.config.stats) || {};
    var serverCfg = (opts && opts.config && opts.config.server) || {};
    this._dir = resolve(opts.dataDir || 'data', 'stats');
    this._retentionDays = cfg.retention_days || 30;
    this._recentSize = cfg.recent_buffer_size || 2000;
    this._enabled = cfg.enabled !== false;
    this._defaultCallerIdentity = String(serverCfg.default_identity || '').trim();

    this._today = emptyDay(dateStr(Date.now()));
    this._recent = [];
    this._recentFile = resolve(this._dir, 'recent.json');
    this._rpmBuckets = new Array(60).fill(0);
    this._rpmIndex = 0;
    this._rpmLastSec = Math.floor(Date.now() / 1000);

    this._tpmBuckets = new Array(60).fill(0);
    this._tpmIndex = 0;
    this._tpmLastSec = Math.floor(Date.now() / 1000);

    this._saveTimer = null;
    this._rpmTimer = null;

    if (this._enabled) {
      this._ensureDir();
      this._loadToday();
      this._migrateRecentToJsonl();
      this._startRpmTicker();
      this._cleanup();
    }
  }

  _ensureDir() {
    if (!existsSync(this._dir)) {
      mkdirSync(this._dir, { recursive: true });
    }
  }

  _filePath(date) {
    return resolve(this._dir, date + '.json');
  }

  _normalizeCallerIdentity(identity) {
    return normalizeCallerIdentity(identity, this._defaultCallerIdentity);
  }

  _normalizeCallerBuckets(dayData) {
    if (!dayData || typeof dayData !== 'object') return dayData;
    normalizeCallerBucketsInTree(dayData, this._defaultCallerIdentity);
    return dayData;
  }

  _rebuildDayFromJsonl(date) {
    var logFile = resolve(this._dir, 'requests-' + date + '.jsonl');
    if (!existsSync(logFile)) return null;

    var content = '';
    try {
      content = readFileSync(logFile, 'utf8');
    } catch (_) {
      return null;
    }

    var rebuilt = emptyDay(date);
    var lines = content.split('\n');
    var lineCount = 0;
    var parsedCount = 0;
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (!line) continue;
      lineCount++;
      try {
        var raw = JSON.parse(line);
        parsedCount++;
        var ts = toFiniteNumber(raw.ts);
        if (ts <= 0) ts = Date.now();
        mergeEntry(rebuilt, {
          ts: ts,
          route: raw.route || '',
          path: raw.path || '',
          model: raw.model || '',
          account: raw.account || '',
          caller_identity: this._normalizeCallerIdentity(raw.caller_identity || ''),
          status: toFiniteNumber(raw.status),
          latency: toFiniteNumber(raw.latency),
          ttfb_ms: toFiniteNumber(raw.ttfb_ms),
          input_tokens: toFiniteNumber(raw.input_tokens),
          output_tokens: toFiniteNumber(raw.output_tokens),
          cached_tokens: toFiniteNumber(raw.cached_tokens),
          reasoning_tokens: toFiniteNumber(raw.reasoning_tokens),
          error_type: raw.error_type || null,
          stream: !!raw.stream,
          ip: raw.ip || '',
          ua_hash: raw.ua_hash || '',
          session_hint: raw.session_hint || '',
        });
      } catch (_) {
        // 忽略单行损坏
      }
    }

    this._normalizeCallerBuckets(rebuilt);
    return {
      day: rebuilt,
      line_count: lineCount,
      parsed_count: parsedCount,
    };
  }

  _loadToday() {
    var today = dateStr(Date.now());
    var fp = this._filePath(today);
    if (existsSync(fp)) {
      try {
        var data = JSON.parse(readFileSync(fp, 'utf8'));
        // 恢复聚合数据
        this._today = Object.assign(emptyDay(today), {
          total_requests: data.total_requests || 0,
          success_requests: data.success_requests || 0,
          total_input_tokens: data.total_input_tokens || 0,
          total_output_tokens: data.total_output_tokens || 0,
          total_cached_tokens: data.total_cached_tokens || 0,
          total_reasoning_tokens: data.total_reasoning_tokens || 0,
          total_latency_ms: data.total_latency_ms || 0,
          by_model: data.by_model || {},
          by_account: data.by_account || {},
          by_caller_identity: data.by_caller_identity || {},
        });
        // 恢复按小时数据
        if (data.by_hour && data.by_hour.length === 24) {
          this._today.by_hour = data.by_hour;
        }
        this._normalizeCallerBuckets(this._today);
      } catch (_) {
        // 文件损坏，用空的
      }
    }
    // 恢复 _recent
    if (existsSync(this._recentFile)) {
      try {
        var recentData = JSON.parse(readFileSync(this._recentFile, 'utf8'));
        if (Array.isArray(recentData)) {
          this._recent = recentData.slice(-this._recentSize);
        }
      } catch (_) {
        // 文件损坏，忽略
      }
    }

    // 启动重建: 当天聚合明显小于 JSONL 请求行数时, 以 JSONL 为准
    var rebuilt = this._rebuildDayFromJsonl(today);
    if (rebuilt && rebuilt.parsed_count > 0) {
      var threshold = Math.floor(rebuilt.parsed_count * 0.8);
      if (threshold < 1) threshold = 1;
      if ((this._today.total_requests || 0) < threshold) {
        this._today = rebuilt.day;
        this._doSave();
      }
    }
  }

  _migrateRecentToJsonl() {
    var today = this._today.date;
    var logFile = resolve(this._dir, 'requests-' + today + '.jsonl');
    if (existsSync(logFile)) return;

    var lines = [];
    for (var i = 0; i < this._recent.length; i++) {
      var r = this._recent[i];
      var rDate = dateStr(r.ts);
      if (rDate === today) {
        lines.push(JSON.stringify(r));
      }
    }
    if (lines.length > 0) {
      try {
        writeFileSync(logFile, lines.join('\n') + '\n');
      } catch (_) {
        // 静默失败
      }
    }
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
      this._today = emptyDay(day);
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
    try {
      this._ensureDir();
      var fp = this._filePath(this._today.date);
      writeFileSync(fp, JSON.stringify(this._today, null, 2));
      // 保存 _recent
      writeFileSync(this._recentFile, JSON.stringify(this._recent));
    } catch (_) {
      // 静默失败
    }
  }

  _appendRequestLog(record) {
    try {
      var date = this._today.date;
      var logFile = resolve(this._dir, 'requests-' + date + '.jsonl');
      appendFileSync(logFile, JSON.stringify(record) + '\n');
    } catch (_) {
      // 静默失败
    }
  }

  forceSave() {
    if (this._enabled) this._doSave();
  }

  /**
   * 清理过期文件
   */
  _cleanup() {
    try {
      var cutoff = Date.now() - this._retentionDays * 86400000;
      var files = readdirSync(this._dir);
      for (var i = 0; i < files.length; i++) {
        var file = files[i];
        var datePart = '';
        var dayMatch = /^(\d{4}-\d{2}-\d{2})\.json$/.exec(file);
        if (dayMatch) {
          datePart = dayMatch[1];
        } else {
          var reqMatch = /^requests-(\d{4}-\d{2}-\d{2})\.jsonl$/.exec(file);
          if (reqMatch) datePart = reqMatch[1];
        }
        if (!datePart) continue;
        var ts = new Date(datePart + 'T00:00:00.000Z').getTime();
        if (!isFinite(ts)) continue;
        if (ts < cutoff) {
          unlinkSync(resolve(this._dir, file));
        }
      }
    } catch (_) {
      // 忽略
    }
  }

  /**
   * 加载指定日期的聚合数据
   */
  _loadDay(date) {
    if (date === this._today.date) return this._today;
    var fp = this._filePath(date);
    if (!existsSync(fp)) return null;
    try {
      var dayData = JSON.parse(readFileSync(fp, 'utf8'));
      return this._normalizeCallerBuckets(dayData);
    } catch (_) {
      return null;
    }
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
    var dates = [];
    try {
      var files = readdirSync(this._dir);
      for (var i = 0; i < files.length; i++) {
        var f = files[i];
        if (!/^\d{4}-\d{2}-\d{2}\.json$/.test(f)) continue;
        dates.push(basename(f, '.json'));
      }
    } catch (_) {
      // 忽略
    }

    // _today 可能尚未落盘，补进可用日期
    if (dates.indexOf(this._today.date) < 0) {
      dates.push(this._today.date);
    }

    dates.sort();
    return dates;
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

  _aggregateOverviewDates(dates) {
    var agg = {
      total_requests: 0,
      success_requests: 0,
      total_input_tokens: 0,
      total_output_tokens: 0,
      total_cached_tokens: 0,
      total_reasoning_tokens: 0,
      total_latency_ms: 0,
    };

    for (var i = 0; i < dates.length; i++) {
      var day = this._loadDay(dates[i]);
      if (!day) continue;
      agg.total_requests += day.total_requests || 0;
      agg.success_requests += day.success_requests || 0;
      agg.total_input_tokens += day.total_input_tokens || 0;
      agg.total_output_tokens += day.total_output_tokens || 0;
      agg.total_cached_tokens += day.total_cached_tokens || 0;
      agg.total_reasoning_tokens += day.total_reasoning_tokens || 0;
      agg.total_latency_ms += day.total_latency_ms || 0;
    }

    return {
      date: dates.length > 0 ? (dates[0] + '~' + dates[dates.length - 1]) : '',
      total_requests: agg.total_requests,
      success_requests: agg.success_requests,
      success_rate: agg.total_requests > 0
        ? Math.round(agg.success_requests / agg.total_requests * 10000) / 100
        : 0,
      total_input_tokens: agg.total_input_tokens,
      total_output_tokens: agg.total_output_tokens,
      total_cached_tokens: agg.total_cached_tokens,
      total_reasoning_tokens: agg.total_reasoning_tokens,
      avg_latency: agg.total_requests > 0
        ? Math.round(agg.total_latency_ms / agg.total_requests)
        : 0,
      // 区间聚合没有实时 RPM/TPM 概念；单日今天时沿用实时值更符合直觉
      rpm: (dates.length === 1 && dates[0] === this._today.date) ? this.getRPM() : 0,
      tpm: (dates.length === 1 && dates[0] === this._today.date) ? this.getTPM() : 0,
    };
  }

  /**
   * 指定日期范围概览
   */
  getOverviewRange(from, to) {
    return this._aggregateOverviewDates(this._getDatesBetween(from, to));
  }

  /**
   * 总计概览（全量历史）
   */
  getOverviewTotal() {
    return this._aggregateOverviewDates(this.getAvailableDates());
  }

  /**
   * 最近 N 小时概览
   */
  getOverviewLastHours(hours) {
    var series = this.getTimeseriesLastHours(hours);
    var totalRequests = 0;
    var successRequests = 0;
    var totalInputTokens = 0;
    var totalOutputTokens = 0;
    var totalCachedTokens = 0;
    var totalReasoningTokens = 0;
    for (var i = 0; i < series.length; i++) {
      var item = series[i] || {};
      totalRequests += item.requests || 0;
      successRequests += item.success || 0;
      totalInputTokens += item.input || 0;
      totalOutputTokens += item.output || 0;
      totalCachedTokens += item.cached || 0;
      totalReasoningTokens += item.reasoning || 0;
    }

    // by_hour 不含延迟，优先使用 recent 估算窗口平均延迟
    var recent = this._getRecentEntriesLastHours(hours);
    var totalLatency = 0;
    for (var j = 0; j < recent.length; j++) {
      totalLatency += (recent[j] && recent[j].latency) || 0;
    }

    var meta = this._getLastHoursMeta(hours);
    return {
      date: meta.from + '~' + meta.to,
      total_requests: totalRequests,
      success_requests: successRequests,
      success_rate: totalRequests > 0
        ? Math.round(successRequests / totalRequests * 10000) / 100
        : 0,
      total_input_tokens: totalInputTokens,
      total_output_tokens: totalOutputTokens,
      total_cached_tokens: totalCachedTokens,
      total_reasoning_tokens: totalReasoningTokens,
      avg_latency: recent.length > 0
        ? Math.round(totalLatency / recent.length)
        : 0,
      rpm: this.getRPM(),
      tpm: this.getTPM(),
    };
  }

  /**
   * 指定日期范围时间序列
   */
  getTimeseriesRange(from, to) {
    var dates = this._getDatesBetween(from, to);
    var result = [];
    for (var i = 0; i < dates.length; i++) {
      var date = dates[i];
      var data = this._loadDay(date);
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
    var dates = this._getDatesBetween(from, to);
    for (var d = 0; d < dates.length; d++) {
      var data = this._loadDay(dates[d]);
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
    var meta = this._getLastHoursMeta(hours);
    var merged = {};
    var hasHourlyModelBreakdown = false;

    for (var d = 0; d < meta.dates.length; d++) {
      var day = this._loadDay(meta.dates[d]);
      if (!day || !day.by_hour) continue;
      for (var h = 0; h < 24; h++) {
        if (!this._hourOverlapsLastHours(meta.dates[d], h, meta)) continue;
        var slot = day.by_hour[h];
        var byModel = slot && (slot.models || slot.by_model);
        if (!byModel) continue;
        hasHourlyModelBreakdown = true;
        var modelKeys = Object.keys(byModel);
        for (var i = 0; i < modelKeys.length; i++) {
          var model = modelKeys[i];
          var src = byModel[model] || {};
          if (!merged[model]) {
            merged[model] = { model: model, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, latency: 0 };
          }
          merged[model].requests += src.requests || 0;
          merged[model].input += src.input || src.input_tokens || 0;
          merged[model].output += src.output || src.output_tokens || 0;
          merged[model].cached += src.cached || src.cached_tokens || 0;
          merged[model].reasoning += src.reasoning || src.reasoning_tokens || 0;
          merged[model].latency += src.latency || 0;
        }
      }
    }

    if (!hasHourlyModelBreakdown) {
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
    var dates = this._getDatesBetween(from, to);
    for (var d = 0; d < dates.length; d++) {
      var data = this._loadDay(dates[d]);
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
    var meta = this._getLastHoursMeta(hours);
    var merged = {};
    var hasHourlyAccountBreakdown = false;

    for (var d = 0; d < meta.dates.length; d++) {
      var day = this._loadDay(meta.dates[d]);
      if (!day || !day.by_hour) continue;
      for (var h = 0; h < 24; h++) {
        if (!this._hourOverlapsLastHours(meta.dates[d], h, meta)) continue;
        var slot = day.by_hour[h];
        var byAccount = slot && (slot.accounts || slot.by_account);
        if (!byAccount) continue;
        hasHourlyAccountBreakdown = true;
        var accountKeys = Object.keys(byAccount);
        for (var i = 0; i < accountKeys.length; i++) {
          var email = accountKeys[i];
          var src = byAccount[email] || {};
          if (!merged[email]) {
            merged[email] = { email: email, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
          }
          merged[email].requests += src.requests || 0;
          merged[email].input += src.input || src.input_tokens || 0;
          merged[email].output += src.output || src.output_tokens || 0;
          merged[email].cached += src.cached || src.cached_tokens || 0;
          merged[email].reasoning += src.reasoning || src.reasoning_tokens || 0;
          merged[email].errors += src.errors || 0;
        }
      }
    }

    if (!hasHourlyAccountBreakdown) {
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
    var dates = this._getDatesBetween(from, to);
    for (var d = 0; d < dates.length; d++) {
      var data = this._loadDay(dates[d]);
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
    var meta = this._getLastHoursMeta(hours);
    var merged = {};
    var hasHourlyCallerBreakdown = false;

    for (var d = 0; d < meta.dates.length; d++) {
      var day = this._loadDay(meta.dates[d]);
      if (!day || !day.by_hour) continue;
      for (var h = 0; h < 24; h++) {
        if (!this._hourOverlapsLastHours(meta.dates[d], h, meta)) continue;
        var slot = day.by_hour[h];
        var byCaller = slot && (slot.callers || slot.by_caller_identity);
        if (!byCaller) continue;
        hasHourlyCallerBreakdown = true;
        var callerKeys = Object.keys(byCaller);
        for (var i = 0; i < callerKeys.length; i++) {
          var identity = callerKeys[i];
          var src = byCaller[identity] || {};
          if (!merged[identity]) {
            merged[identity] = { identity: identity, requests: 0, input: 0, output: 0, cached: 0, reasoning: 0, errors: 0 };
          }
          merged[identity].requests += src.requests || 0;
          merged[identity].input += src.input || src.input_tokens || 0;
          merged[identity].output += src.output || src.output_tokens || 0;
          merged[identity].cached += src.cached || src.cached_tokens || 0;
          merged[identity].reasoning += src.reasoning || src.reasoning_tokens || 0;
          merged[identity].errors += src.errors || 0;
        }
      }
    }

    if (!hasHourlyCallerBreakdown) {
      var recent = this._getRecentEntriesLastHours(hours);
      for (var r = 0; r < recent.length; r++) {
        var rec = recent[r] || {};
        var caller = this._normalizeCallerIdentity(rec.caller_identity || '');
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
   * 搜索请求日志（扫描 JSONL 文件）
   * @param {object} options - { page, limit, filter, search, from, to, hours, sinceTs }
   * @returns {{ data: Array, total: number, page: number, pages: number, limit: number }}
   */
  searchRequests(options) {
    var page = options.page || 1;
    var limit = options.limit || 20;
    var filter = options.filter || '';
    var search = (options.search || '').toLowerCase();
    var from = options.from || '';
    var to = options.to || '';
    var sinceTs = Number(options.sinceTs);
    if (!isFinite(sinceTs) || sinceTs <= 0) sinceTs = 0;
    if (!sinceTs && options.hours !== undefined && options.hours !== null && options.hours !== '') {
      var windowHours = this._normalizeHours(options.hours);
      sinceTs = Date.now() - windowHours * 3600000;
    }

    var dates = [];
    if (sinceTs > 0 && !(from && to)) {
      dates = this._getDatesBetween(dateStr(sinceTs), dateStr(Date.now()));
    } else {
      dates = this._getSearchDates(from, to);
    }
    var allMatches = [];

    for (var i = dates.length - 1; i >= 0; i--) {
      var logFile = resolve(this._dir, 'requests-' + dates[i] + '.jsonl');
      if (!existsSync(logFile)) continue;
      try {
        var content = readFileSync(logFile, 'utf8');
        var lines = content.split('\n');
        for (var j = lines.length - 1; j >= 0; j--) {
          var line = lines[j].trim();
          if (!line) continue;
          try {
            var r = JSON.parse(line);
            if (filter === 'success' && !(r.status >= 200 && r.status < 400)) continue;
            if (filter === 'error' && r.status >= 200 && r.status < 400) continue;
            if (search && !matchRecordKeyword(r, search)) continue;
            if (sinceTs > 0) {
              var ts = Number(r.ts) || 0;
              if (ts < sinceTs) continue;
            }
            allMatches.push(r);
          } catch (_) {
            // 忽略单行解析失败
          }
        }
      } catch (_) {
        // 忽略读取失败
      }
    }

    var total = allMatches.length;
    var pages = Math.ceil(total / limit) || 1;
    if (page > pages) page = pages;
    var start = (page - 1) * limit;
    var data = allMatches.slice(start, start + limit);
    return { data: data, total: total, page: page, pages: pages, limit: limit };
  }

  _getSearchDates(from, to) {
    if (from && to) {
      return this._getDatesBetween(from, to);
    }

    var files = [];
    try {
      var entries = readdirSync(this._dir);
      for (var i = 0; i < entries.length; i++) {
        var m = entries[i].match(/^requests-(\d{4}-\d{2}-\d{2})\.jsonl$/);
        if (m) files.push(m[1]);
      }
      files.sort();
    } catch (_) {
      // 忽略读取失败
    }

    var today = dateStr(Date.now());
    if (files.indexOf(today) < 0) files.push(today);
    return files;
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
    var fileMode = normalizedSource === 'file' || !!normalizedSearch;

    if (fileMode) {
      var fileLimit = parseInt(limit, 10);
      if (!fileLimit || fileLimit < 1) fileLimit = 500;
      if (fileLimit > 2000) fileLimit = 2000;

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
    if (this._rpmTimer) {
      clearInterval(this._rpmTimer);
      this._rpmTimer = null;
    }
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
  }
}
