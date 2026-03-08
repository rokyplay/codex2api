var WINDOW_MS = 60 * 1000;
var GLOBAL_KEY = '__global__';
var CLEANUP_INTERVAL_MS = 5 * 60 * 1000;

function normalizeObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

function toNonNegativeInt(value, fallback) {
  var n = Number(value);
  if (!isFinite(n) || n < 0) return fallback || 0;
  return Math.floor(n);
}

function normalizeIdentity(value) {
  var s = String(value || '').trim();
  if (!s) return 'unknown';
  return s;
}

function cloneValue(value) {
  return JSON.parse(JSON.stringify(value));
}

function normalizeLimitPair(value, fallback) {
  var source = normalizeObject(value);
  var base = normalizeObject(fallback);
  return {
    rpm: toNonNegativeInt(source.rpm, toNonNegativeInt(base.rpm, 0)),
    tpm: toNonNegativeInt(source.tpm, toNonNegativeInt(base.tpm, 0)),
  };
}

function normalizeOverrides(value) {
  var source = normalizeObject(value);
  var out = {};
  var keys = Object.keys(source);
  for (var i = 0; i < keys.length; i++) {
    var identity = String(keys[i] || '').trim();
    if (!identity) continue;
    out[identity] = normalizeLimitPair(source[identity], { rpm: 0, tpm: 0 });
  }
  return out;
}

function normalizeConfig(value) {
  var source = normalizeObject(value);
  return {
    enabled: source.enabled !== false,
    global: normalizeLimitPair(source.global, { rpm: 0, tpm: 0 }),
    default_per_user: normalizeLimitPair(source.default_per_user, { rpm: 0, tpm: 0 }),
    overrides: normalizeOverrides(source.overrides),
  };
}

function createCounterState() {
  return {
    events: [],
    total: 0,
    last_seen: 0,
  };
}

export class RateLimiter {
  constructor(config) {
    this._windowMs = WINDOW_MS;
    this._config = normalizeConfig(config);
    this._rpmCounters = new Map();
    this._tpmCounters = new Map();
    this._lastCleanupAt = 0;
  }

  getConfig() {
    return cloneValue(this._config);
  }

  updateConfig(config) {
    this._config = normalizeConfig(config);
    return this.getConfig();
  }

  setUserOverride(identity, limits) {
    var key = String(identity || '').trim();
    if (!key) throw new Error('identity_required');
    this._config.overrides[key] = normalizeLimitPair(limits, { rpm: 0, tpm: 0 });
    return cloneValue(this._config.overrides[key]);
  }

  deleteUserOverride(identity) {
    var key = String(identity || '').trim();
    if (!key) return false;
    if (!Object.prototype.hasOwnProperty.call(this._config.overrides, key)) return false;
    delete this._config.overrides[key];
    return true;
  }

  _getUserLimits(identity) {
    if (Object.prototype.hasOwnProperty.call(this._config.overrides, identity)) {
      return this._config.overrides[identity];
    }
    return this._config.default_per_user;
  }

  _getStore(metric) {
    return metric === 'tpm' ? this._tpmCounters : this._rpmCounters;
  }

  _getState(metric, key) {
    var store = this._getStore(metric);
    var state = store.get(key);
    if (state) return state;
    state = createCounterState();
    store.set(key, state);
    return state;
  }

  _pruneState(state, now) {
    if (!state || !Array.isArray(state.events)) return;
    var cutoff = now - this._windowMs;
    while (state.events.length > 0 && state.events[0].ts <= cutoff) {
      var ev = state.events.shift();
      state.total -= ev.value;
    }
    if (state.total < 0) state.total = 0;
  }

  _getCurrent(metric, key, now) {
    var state = this._getState(metric, key);
    this._pruneState(state, now);
    state.last_seen = now;
    return state.total;
  }

  _add(metric, key, value, now) {
    var amount = toNonNegativeInt(value, 0);
    if (amount <= 0) return 0;
    var state = this._getState(metric, key);
    this._pruneState(state, now);
    state.events.push({ ts: now, value: amount });
    state.total += amount;
    state.last_seen = now;
    return state.total;
  }

  _computeResetAt(metric, key, limit, pending, now) {
    var cappedLimit = toNonNegativeInt(limit, 0);
    if (cappedLimit <= 0) return now;
    var requiredPending = Math.max(0, toNonNegativeInt(pending, 0));
    var state = this._getState(metric, key);
    this._pruneState(state, now);
    if (state.total + requiredPending <= cappedLimit) return now;

    var target = cappedLimit - requiredPending;
    var removed = 0;
    for (var i = 0; i < state.events.length; i++) {
      removed += state.events[i].value;
      if (state.total - removed <= target) {
        return state.events[i].ts + this._windowMs;
      }
    }
    return now + this._windowMs;
  }

  _cleanupStore(metric, now) {
    var store = this._getStore(metric);
    for (var entry of store.entries()) {
      var key = entry[0];
      var state = entry[1];
      this._pruneState(state, now);
      if (state.total <= 0 && state.last_seen > 0 && now - state.last_seen > this._windowMs) {
        store.delete(key);
      }
    }
  }

  _maybeCleanup(now) {
    if (this._lastCleanupAt > 0 && now - this._lastCleanupAt < CLEANUP_INTERVAL_MS) return;
    this._lastCleanupAt = now;
    this._cleanupStore('rpm', now);
    this._cleanupStore('tpm', now);
  }

  _buildRemaining(globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm) {
    var rpmParts = [];
    var tpmParts = [];
    if (globalLimits.rpm > 0) rpmParts.push(Math.max(0, globalLimits.rpm - globalRpm));
    if (userLimits.rpm > 0) rpmParts.push(Math.max(0, userLimits.rpm - userRpm));
    if (globalLimits.tpm > 0) tpmParts.push(Math.max(0, globalLimits.tpm - globalTpm));
    if (userLimits.tpm > 0) tpmParts.push(Math.max(0, userLimits.tpm - userTpm));
    return {
      rpm: rpmParts.length ? Math.min.apply(Math, rpmParts) : null,
      tpm: tpmParts.length ? Math.min.apply(Math, tpmParts) : null,
    };
  }

  _buildDecision(allowed, now, info) {
    var decision = {
      allowed: allowed === true,
      remaining: info && info.remaining ? info.remaining : { rpm: null, tpm: null },
      reset_at: info && typeof info.reset_at === 'number' ? info.reset_at : now,
      limit: info && info.limit ? info.limit : { type: 'none', scope: 'none', value: 0, global: { rpm: 0, tpm: 0 }, user: { rpm: 0, tpm: 0 } },
    };
    var retryAfterMs = decision.reset_at - now;
    decision.retry_after = retryAfterMs > 0 ? Math.max(1, Math.ceil(retryAfterMs / 1000)) : 1;
    return decision;
  }

  _deny(metric, scope, value, identity, now, globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm, pending) {
    var key = scope === 'global' ? GLOBAL_KEY : identity;
    var resetAt = this._computeResetAt(metric, key, value, pending, now);
    var remaining = this._buildRemaining(globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm);
    if (metric === 'rpm') remaining.rpm = 0;
    if (metric === 'tpm') remaining.tpm = 0;
    return this._buildDecision(false, now, {
      remaining: remaining,
      reset_at: resetAt,
      limit: {
        type: metric,
        scope: scope,
        value: value,
        global: cloneValue(globalLimits),
        user: cloneValue(userLimits),
      },
    });
  }

  check(identity) {
    var now = Date.now();
    var callerIdentity = normalizeIdentity(identity);
    this._maybeCleanup(now);

    var globalLimits = this._config.global;
    var userLimits = this._getUserLimits(callerIdentity);

    var globalRpm = this._getCurrent('rpm', GLOBAL_KEY, now);
    var userRpm = this._getCurrent('rpm', callerIdentity, now);
    var globalTpm = this._getCurrent('tpm', GLOBAL_KEY, now);
    var userTpm = this._getCurrent('tpm', callerIdentity, now);

    var baseRemaining = this._buildRemaining(globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm);

    if (!this._config.enabled) {
      return this._buildDecision(true, now, {
        remaining: baseRemaining,
        reset_at: now + this._windowMs,
        limit: {
          type: 'none',
          scope: 'none',
          value: 0,
          global: cloneValue(globalLimits),
          user: cloneValue(userLimits),
        },
      });
    }

    if (globalLimits.rpm > 0 && globalRpm + 1 > globalLimits.rpm) {
      return this._deny('rpm', 'global', globalLimits.rpm, callerIdentity, now, globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm, 1);
    }
    if (userLimits.rpm > 0 && userRpm + 1 > userLimits.rpm) {
      return this._deny('rpm', 'user', userLimits.rpm, callerIdentity, now, globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm, 1);
    }
    if (globalLimits.tpm > 0 && globalTpm >= globalLimits.tpm) {
      return this._deny('tpm', 'global', globalLimits.tpm, callerIdentity, now, globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm, 0);
    }
    if (userLimits.tpm > 0 && userTpm >= userLimits.tpm) {
      return this._deny('tpm', 'user', userLimits.tpm, callerIdentity, now, globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm, 0);
    }

    globalRpm = this._add('rpm', GLOBAL_KEY, 1, now);
    userRpm = this._add('rpm', callerIdentity, 1, now);

    return this._buildDecision(true, now, {
      remaining: this._buildRemaining(globalLimits, userLimits, globalRpm, userRpm, globalTpm, userTpm),
      reset_at: now + this._windowMs,
      limit: {
        type: 'none',
        scope: 'none',
        value: 0,
        global: cloneValue(globalLimits),
        user: cloneValue(userLimits),
      },
    });
  }

  _extractTokens(usageOrTokens) {
    if (typeof usageOrTokens === 'number') {
      return toNonNegativeInt(usageOrTokens, 0);
    }
    var usage = normalizeObject(usageOrTokens);
    return toNonNegativeInt(
      usage.total_tokens,
      toNonNegativeInt(usage.input_tokens, 0)
        + toNonNegativeInt(usage.output_tokens, 0)
        + toNonNegativeInt(usage.cached_tokens, 0)
        + toNonNegativeInt(usage.reasoning_tokens, 0)
        + toNonNegativeInt(usage.prompt_tokens, 0)
        + toNonNegativeInt(usage.completion_tokens, 0)
    );
  }

  recordTokens(identity, usageOrTokens) {
    var now = Date.now();
    var callerIdentity = normalizeIdentity(identity);
    this._maybeCleanup(now);
    if (!this._config.enabled) return null;

    var tokens = this._extractTokens(usageOrTokens);
    if (tokens <= 0) return null;

    var globalTpm = this._add('tpm', GLOBAL_KEY, tokens, now);
    var userTpm = this._add('tpm', callerIdentity, tokens, now);
    return {
      tokens: tokens,
      global_tpm: globalTpm,
      user_tpm: userTpm,
    };
  }
}

export function normalizeRateLimitConfig(config) {
  return normalizeConfig(config);
}
