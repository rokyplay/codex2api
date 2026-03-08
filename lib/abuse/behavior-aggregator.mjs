/**
 * 行为聚合引擎（内存）
 *
 * 维度：
 * - caller_identity：频率、并发、token、错误率、活跃小时、周期性
 * - IP：关联 caller 数
 * - UA Hash：关联 caller 数
 *
 * 窗口：默认 24 小时
 */

var ONE_MINUTE_MS = 60 * 1000;
var ONE_HOUR_MS = 60 * ONE_MINUTE_MS;

function toFiniteNumber(value, fallback) {
  var n = Number(value);
  if (!isFinite(n)) return fallback || 0;
  return n;
}

function normalizeIdentity(value) {
  var s = String(value || '').trim();
  if (!s || s === 'unknown') return '';
  return s;
}

function normalizeKey(value) {
  var s = String(value || '').trim();
  if (!s) return '';
  return s;
}

function minuteKey(ts) {
  return Math.floor(ts / ONE_MINUTE_MS);
}

function hourKey(ts) {
  return Math.floor(ts / ONE_HOUR_MS);
}

function createMinuteBucket() {
  return { requests: 0, errors: 0, tokens: 0, peak_inflight: 0 };
}

function createHourBucket() {
  return { requests: 0, errors: 0, tokens: 0 };
}

function createIdentityState(identity) {
  return {
    identity: identity,
    current_inflight: 0,
    minute_buckets: new Map(),
    hour_buckets: new Map(),
    timestamps: [],
    ips: new Map(),
    uas: new Map(),
    updated_at: Date.now(),
  };
}

function safeMean(values) {
  if (!Array.isArray(values) || values.length === 0) return 0;
  var total = 0;
  for (var i = 0; i < values.length; i++) {
    total += values[i];
  }
  return total / values.length;
}

function safeStd(values, mean) {
  if (!Array.isArray(values) || values.length < 2) return 0;
  var m = isFinite(mean) ? mean : safeMean(values);
  var total = 0;
  for (var i = 0; i < values.length; i++) {
    var diff = values[i] - m;
    total += diff * diff;
  }
  return Math.sqrt(total / values.length);
}

export class BehaviorAggregator {
  constructor(opts) {
    var cfg = (opts && opts.config) || {};
    this._windowHours = Math.max(1, Math.floor(toFiniteNumber(cfg.window_hours, 24)));
    this._windowMs = this._windowHours * ONE_HOUR_MS;
    this._cleanupIntervalMs = Math.max(60 * 1000, Math.floor(toFiniteNumber(cfg.cleanup_interval_ms, 10 * ONE_MINUTE_MS)));
    this._identityStates = new Map();
    this._ipCallers = new Map();
    this._uaCallers = new Map();
    this._cleanupTimer = null;

    this._startCleanupTicker();
  }

  _startCleanupTicker() {
    if (this._cleanupTimer) return;
    this._cleanupTimer = setInterval(function () {
      this.cleanup();
    }.bind(this), this._cleanupIntervalMs);
    if (this._cleanupTimer && this._cleanupTimer.unref) this._cleanupTimer.unref();
  }

  _ensureState(identity) {
    var state = this._identityStates.get(identity);
    if (state) return state;
    state = createIdentityState(identity);
    this._identityStates.set(identity, state);
    return state;
  }

  _touchRelation(store, key, identity, ts) {
    if (!key || !identity) return;
    var rel = store.get(key);
    if (!rel) {
      rel = new Map();
      store.set(key, rel);
    }
    rel.set(identity, ts);
  }

  _pruneRelationStore(store, cutoffTs) {
    for (var entry of store.entries()) {
      var key = entry[0];
      var rel = entry[1];
      if (!rel || rel.size === 0) {
        store.delete(key);
        continue;
      }
      for (var relEntry of rel.entries()) {
        if (toFiniteNumber(relEntry[1], 0) < cutoffTs) {
          rel.delete(relEntry[0]);
        }
      }
      if (rel.size === 0) {
        store.delete(key);
      }
    }
  }

  _sumMinuteBuckets(state, nowTs, minutes, field) {
    var nowMinute = minuteKey(nowTs);
    var minMinute = nowMinute - Math.max(0, minutes - 1);
    var total = 0;
    for (var entry of state.minute_buckets.entries()) {
      var key = entry[0];
      var bucket = entry[1];
      if (key < minMinute || key > nowMinute) continue;
      total += toFiniteNumber(bucket[field], 0);
    }
    return total;
  }

  _maxMinutePeak(state, nowTs, minutes) {
    var nowMinute = minuteKey(nowTs);
    var minMinute = nowMinute - Math.max(0, minutes - 1);
    var maxValue = 0;
    for (var entry of state.minute_buckets.entries()) {
      var key = entry[0];
      var bucket = entry[1];
      if (key < minMinute || key > nowMinute) continue;
      var peak = toFiniteNumber(bucket.peak_inflight, 0);
      if (peak > maxValue) maxValue = peak;
    }
    return maxValue;
  }

  _sumHourBuckets(state, nowTs, hours, field) {
    var nowHour = hourKey(nowTs);
    var minHour = nowHour - Math.max(0, hours - 1);
    var total = 0;
    for (var entry of state.hour_buckets.entries()) {
      var key = entry[0];
      var bucket = entry[1];
      if (key < minHour || key > nowHour) continue;
      total += toFiniteNumber(bucket[field], 0);
    }
    return total;
  }

  _countActiveHours(state, nowTs, hours) {
    var nowHour = hourKey(nowTs);
    var minHour = nowHour - Math.max(0, hours - 1);
    var count = 0;
    for (var entry of state.hour_buckets.entries()) {
      var key = entry[0];
      var bucket = entry[1];
      if (key < minHour || key > nowHour) continue;
      if (toFiniteNumber(bucket.requests, 0) > 0) count += 1;
    }
    return count;
  }

  _calcPeriodicMetrics(state) {
    var timestamps = state.timestamps;
    if (!Array.isArray(timestamps) || timestamps.length < 3) {
      return { cv: null, samples: 0, mean_interval_ms: 0 };
    }
    var maxSamples = 120;
    var begin = Math.max(0, timestamps.length - maxSamples);
    var intervals = [];
    for (var i = begin + 1; i < timestamps.length; i++) {
      var diff = timestamps[i] - timestamps[i - 1];
      if (diff <= 0) continue;
      intervals.push(diff);
    }
    if (intervals.length < 2) {
      return { cv: null, samples: intervals.length, mean_interval_ms: intervals[0] || 0 };
    }
    var mean = safeMean(intervals);
    if (mean <= 0) {
      return { cv: null, samples: intervals.length, mean_interval_ms: mean };
    }
    var std = safeStd(intervals, mean);
    return {
      cv: std / mean,
      samples: intervals.length,
      mean_interval_ms: mean,
    };
  }

  _relatedCallerStatsForKeys(globalStore, keyMap, limit) {
    var maxCallers = 0;
    var topKey = '';
    var details = [];
    var keys = Array.from(keyMap.keys());
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var rel = globalStore.get(key);
      var count = rel ? rel.size : 0;
      if (count > maxCallers) {
        maxCallers = count;
        topKey = key;
      }
      details.push({ key: key, callers: count });
    }
    details.sort(function (a, b) { return b.callers - a.callers; });
    if (typeof limit === 'number' && limit > 0 && details.length > limit) {
      details = details.slice(0, limit);
    }
    return {
      max_callers: maxCallers,
      max_key: topKey,
      top: details,
    };
  }

  observeRequestStart(entry) {
    var identity = normalizeIdentity(entry && entry.caller_identity);
    if (!identity) return null;
    var ts = toFiniteNumber(entry && entry.ts, Date.now());
    var ip = normalizeKey(entry && entry.ip);
    var uaHash = normalizeKey(entry && entry.ua_hash);
    var state = this._ensureState(identity);
    var mKey = minuteKey(ts);
    var hKey = hourKey(ts);

    var minute = state.minute_buckets.get(mKey);
    if (!minute) {
      minute = createMinuteBucket();
      state.minute_buckets.set(mKey, minute);
    }
    minute.requests += 1;

    var hour = state.hour_buckets.get(hKey);
    if (!hour) {
      hour = createHourBucket();
      state.hour_buckets.set(hKey, hour);
    }
    hour.requests += 1;

    state.timestamps.push(ts);
    state.current_inflight += 1;
    if (state.current_inflight > minute.peak_inflight) {
      minute.peak_inflight = state.current_inflight;
    }

    if (ip) {
      state.ips.set(ip, ts);
      this._touchRelation(this._ipCallers, ip, identity, ts);
    }
    if (uaHash) {
      state.uas.set(uaHash, ts);
      this._touchRelation(this._uaCallers, uaHash, identity, ts);
    }

    state.updated_at = ts;
    return { identity: identity, inflight: state.current_inflight };
  }

  observeRequestEnd(entry) {
    var identity = normalizeIdentity(entry && entry.caller_identity);
    if (!identity) return null;
    var ts = toFiniteNumber(entry && entry.ts, Date.now());
    var state = this._ensureState(identity);
    var mKey = minuteKey(ts);
    var hKey = hourKey(ts);
    var status = Math.floor(toFiniteNumber(entry && entry.status, 0));
    var errorType = normalizeKey(entry && entry.error_type);
    var isError = !!errorType || status >= 400 || status <= 0;
    var tokens = toFiniteNumber(entry && entry.input_tokens, 0)
      + toFiniteNumber(entry && entry.output_tokens, 0)
      + toFiniteNumber(entry && entry.cached_tokens, 0)
      + toFiniteNumber(entry && entry.reasoning_tokens, 0);

    var minute = state.minute_buckets.get(mKey);
    if (!minute) {
      minute = createMinuteBucket();
      state.minute_buckets.set(mKey, minute);
    }
    minute.tokens += tokens;
    if (isError) minute.errors += 1;

    var hour = state.hour_buckets.get(hKey);
    if (!hour) {
      hour = createHourBucket();
      state.hour_buckets.set(hKey, hour);
    }
    hour.tokens += tokens;
    if (isError) hour.errors += 1;

    if (state.current_inflight > 0) state.current_inflight -= 1;
    state.updated_at = ts;
    return { identity: identity, inflight: state.current_inflight };
  }

  getIdentityFeatures(identity, nowTs) {
    var normalizedIdentity = normalizeIdentity(identity);
    var now = toFiniteNumber(nowTs, Date.now());
    if (!normalizedIdentity) {
      return {
        caller_identity: '',
        requests_1m: 0,
        requests_5m: 0,
        requests_1h: 0,
        requests_24h: 0,
        tokens_24h: 0,
        errors_1h: 0,
        errors_24h: 0,
        error_rate_1h: 0,
        error_rate_24h: 0,
        active_hours_24h: 0,
        current_inflight: 0,
        max_concurrency_5m: 0,
        unique_ips_24h: 0,
        unique_uas_24h: 0,
        max_callers_per_ip_24h: 0,
        max_callers_per_ua_24h: 0,
        periodic_cv: null,
        periodic_samples: 0,
        periodic_mean_interval_ms: 0,
        ip_evidence: [],
        ua_evidence: [],
      };
    }

    var state = this._identityStates.get(normalizedIdentity);
    if (!state) {
      return this.getIdentityFeatures('', now);
    }

    this._pruneIdentityState(state, now);
    var requests1m = this._sumMinuteBuckets(state, now, 1, 'requests');
    var requests5m = this._sumMinuteBuckets(state, now, 5, 'requests');
    var requests1h = this._sumHourBuckets(state, now, 1, 'requests');
    var requests24h = this._sumHourBuckets(state, now, this._windowHours, 'requests');
    var errors1h = this._sumHourBuckets(state, now, 1, 'errors');
    var errors24h = this._sumHourBuckets(state, now, this._windowHours, 'errors');
    var tokens24h = this._sumHourBuckets(state, now, this._windowHours, 'tokens');
    var activeHours24h = this._countActiveHours(state, now, this._windowHours);
    var maxConcurrency5m = this._maxMinutePeak(state, now, 5);
    var periodic = this._calcPeriodicMetrics(state);

    var ipStats = this._relatedCallerStatsForKeys(this._ipCallers, state.ips, 5);
    var uaStats = this._relatedCallerStatsForKeys(this._uaCallers, state.uas, 5);

    return {
      caller_identity: normalizedIdentity,
      requests_1m: requests1m,
      requests_5m: requests5m,
      requests_1h: requests1h,
      requests_24h: requests24h,
      tokens_24h: tokens24h,
      errors_1h: errors1h,
      errors_24h: errors24h,
      error_rate_1h: requests1h > 0 ? (errors1h / requests1h) : 0,
      error_rate_24h: requests24h > 0 ? (errors24h / requests24h) : 0,
      active_hours_24h: activeHours24h,
      current_inflight: state.current_inflight,
      max_concurrency_5m: Math.max(maxConcurrency5m, state.current_inflight),
      unique_ips_24h: state.ips.size,
      unique_uas_24h: state.uas.size,
      max_callers_per_ip_24h: ipStats.max_callers,
      max_callers_per_ua_24h: uaStats.max_callers,
      periodic_cv: periodic.cv,
      periodic_samples: periodic.samples,
      periodic_mean_interval_ms: periodic.mean_interval_ms,
      ip_evidence: ipStats.top,
      ua_evidence: uaStats.top,
      updated_at: state.updated_at,
    };
  }

  listIdentityFeatures(options) {
    var opts = options || {};
    var now = toFiniteNumber(opts.nowTs, Date.now());
    var levelFilter = String(opts.level || '').trim();
    var keyword = String(opts.keyword || '').trim().toLowerCase();
    var out = [];
    for (var entry of this._identityStates.entries()) {
      var identity = entry[0];
      if (keyword && identity.toLowerCase().indexOf(keyword) < 0) continue;
      var features = this.getIdentityFeatures(identity, now);
      if (levelFilter) {
        // 该层过滤由 rule-engine 负责，这里保留兼容参数以便调用方复用
      }
      out.push(features);
    }
    return out;
  }

  _pruneIdentityState(state, nowTs) {
    if (!state) return;
    var cutoffTs = nowTs - this._windowMs;
    var cutoffMinute = minuteKey(cutoffTs);
    var cutoffHour = hourKey(cutoffTs);

    for (var mEntry of state.minute_buckets.entries()) {
      if (mEntry[0] < cutoffMinute) state.minute_buckets.delete(mEntry[0]);
    }
    for (var hEntry of state.hour_buckets.entries()) {
      if (hEntry[0] < cutoffHour) state.hour_buckets.delete(hEntry[0]);
    }

    if (Array.isArray(state.timestamps) && state.timestamps.length > 0) {
      while (state.timestamps.length > 0 && state.timestamps[0] < cutoffTs) {
        state.timestamps.shift();
      }
      if (state.timestamps.length > 4000) {
        state.timestamps = state.timestamps.slice(-4000);
      }
    }

    for (var ipEntry of state.ips.entries()) {
      if (toFiniteNumber(ipEntry[1], 0) < cutoffTs) {
        state.ips.delete(ipEntry[0]);
      }
    }
    for (var uaEntry of state.uas.entries()) {
      if (toFiniteNumber(uaEntry[1], 0) < cutoffTs) {
        state.uas.delete(uaEntry[0]);
      }
    }
  }

  cleanup(nowTs) {
    var now = toFiniteNumber(nowTs, Date.now());
    var cutoffTs = now - this._windowMs;

    for (var entry of this._identityStates.entries()) {
      var identity = entry[0];
      var state = entry[1];
      this._pruneIdentityState(state, now);
      var isEmpty = state.minute_buckets.size === 0
        && state.hour_buckets.size === 0
        && state.timestamps.length === 0
        && state.current_inflight <= 0
        && state.ips.size === 0
        && state.uas.size === 0;
      if (isEmpty) {
        this._identityStates.delete(identity);
      }
    }

    this._pruneRelationStore(this._ipCallers, cutoffTs);
    this._pruneRelationStore(this._uaCallers, cutoffTs);
  }

  getIdentityCount() {
    return this._identityStates.size;
  }

  stop() {
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
      this._cleanupTimer = null;
    }
  }
}

