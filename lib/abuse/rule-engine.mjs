function toFiniteNumber(value, fallback) {
  var n = Number(value);
  if (!isFinite(n)) return fallback || 0;
  return n;
}

function toTimestampMs(value, fallback) {
  var num = Number(value);
  if (isFinite(num) && num > 0) return Math.floor(num);
  var parsed = Date.parse(String(value || ''));
  if (isFinite(parsed) && parsed > 0) return Math.floor(parsed);
  return fallback || 0;
}

function normalizeObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

function normalizeIdentity(value) {
  var s = String(value || '').trim();
  if (!s || s === 'unknown') return '';
  return s;
}

function cloneValue(value) {
  return JSON.parse(JSON.stringify(value));
}

function isoNow(ts) {
  return new Date(ts || Date.now()).toISOString();
}

function round3(value) {
  return Math.round(toFiniteNumber(value, 0) * 1000) / 1000;
}

function defaultRuleConfig() {
  return {
    high_frequency: { enabled: true, threshold: 120, score: 20 },
    high_concurrency: { enabled: true, threshold: 8, score: 15 },
    multi_account_ip: { enabled: true, threshold: 5, score: 20 },
    no_sleep_window: { enabled: true, threshold: 20, score: 10 },
    periodic_pattern: { enabled: true, max_cv: 0.35, min_samples: 12, score: 15 },
    high_error_rate: { enabled: true, threshold: 0.5, min_requests: 20, score: 15 },
    token_abuse: { enabled: true, threshold: 2000000, score: 20 },
  };
}

function defaultThresholds() {
  return {
    observe_max: 29,
    throttle_min: 30,
    challenge_min: 60,
    suspend_min: 80,
  };
}

function defaultActions() {
  return {
    throttle_min_interval_ms: 3000,
  };
}

function defaultAutoDecay() {
  return {
    enabled: true,
    interval_ms: 600000,
  };
}

function normalizeRules(raw) {
  var defaults = defaultRuleConfig();
  var source = normalizeObject(raw);

  // 支持数组格式：[{ id, ... }]
  if (Array.isArray(raw)) {
    source = {};
    for (var i = 0; i < raw.length; i++) {
      var row = normalizeObject(raw[i]);
      var id = String(row.id || row.rule_id || '').trim();
      if (!id) continue;
      source[id] = row;
    }
  }

  var out = {};
  var keys = Object.keys(defaults);
  for (var k = 0; k < keys.length; k++) {
    var key = keys[k];
    var merged = Object.assign({}, defaults[key], normalizeObject(source[key]));
    merged.enabled = merged.enabled !== false;
    if (merged.threshold !== undefined) merged.threshold = toFiniteNumber(merged.threshold, defaults[key].threshold || 0);
    if (merged.max_cv !== undefined) merged.max_cv = toFiniteNumber(merged.max_cv, defaults[key].max_cv || 0);
    if (merged.min_samples !== undefined) merged.min_samples = Math.max(1, Math.floor(toFiniteNumber(merged.min_samples, defaults[key].min_samples || 1)));
    if (merged.min_requests !== undefined) merged.min_requests = Math.max(1, Math.floor(toFiniteNumber(merged.min_requests, defaults[key].min_requests || 1)));
    merged.score = Math.max(0, Math.floor(toFiniteNumber(merged.score, defaults[key].score || 0)));
    out[key] = merged;
  }
  return out;
}

function normalizeConfig(cfg) {
  var source = normalizeObject(cfg);
  var thresholds = Object.assign({}, defaultThresholds(), normalizeObject(source.thresholds));
  thresholds.observe_max = Math.max(0, Math.floor(toFiniteNumber(thresholds.observe_max, 29)));
  thresholds.throttle_min = Math.max(0, Math.floor(toFiniteNumber(thresholds.throttle_min, 30)));
  thresholds.challenge_min = Math.max(0, Math.floor(toFiniteNumber(thresholds.challenge_min, 60)));
  thresholds.suspend_min = Math.max(0, Math.floor(toFiniteNumber(thresholds.suspend_min, 80)));
  var actions = Object.assign({}, defaultActions(), normalizeObject(source.actions));
  actions.throttle_min_interval_ms = Math.max(1000, Math.floor(toFiniteNumber(actions.throttle_min_interval_ms, 3000)));
  var autoDecay = Object.assign({}, defaultAutoDecay(), normalizeObject(source.auto_decay));
  autoDecay.enabled = autoDecay.enabled !== false;
  autoDecay.interval_ms = Math.max(1000, Math.floor(toFiniteNumber(autoDecay.interval_ms, 600000)));

  return {
    enabled: source.enabled !== false,
    retention_days: Math.max(1, Math.floor(toFiniteNumber(source.retention_days, 90))),
    cleanup_interval_hours: Math.max(1, Math.floor(toFiniteNumber(source.cleanup_interval_hours, 6))),
    rules: normalizeRules(source.rules),
    thresholds: thresholds,
    actions: actions,
    auto_decay: autoDecay,
  };
}

function getDefaultFeatures(identity) {
  return {
    caller_identity: identity || '',
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

function reasonSignature(reasons) {
  if (!Array.isArray(reasons) || reasons.length === 0) return '';
  var items = [];
  for (var i = 0; i < reasons.length; i++) {
    var row = reasons[i] || {};
    items.push(String(row.rule_id || '') + ':' + String(row.value || ''));
  }
  items.sort();
  return items.join('|');
}

export class RuleEngine {
  constructor(opts) {
    var options = opts || {};
    this._aggregator = options.aggregator || null;
    this._riskLogger = options.riskLogger || null;
    this._userStore = options.userStore || null;
    this._getUserCount = typeof options.getUserCount === 'function' ? options.getUserCount : null;
    this._getRegisteredUsers = typeof options.getRegisteredUsers === 'function' ? options.getRegisteredUsers : null;
    this._config = normalizeConfig(options.config);
    this._allIdentities = new Map();
    this._states = new Map();
    this._manualActions = new Map();
    this._throttleState = new Map();
  }

  setUserStore(userStore) {
    this._userStore = userStore || null;
  }

  _createIdentityStats(ts) {
    var seenTs = Math.max(0, Math.floor(toFiniteNumber(ts, Date.now())));
    return {
      requests: 0,
      input_tokens: 0,
      output_tokens: 0,
      cached_tokens: 0,
      first_seen: seenTs,
      last_seen: seenTs,
      _feature_tokens_24h: 0,
    };
  }

  _toPublicIdentityStats(stats) {
    var row = stats && typeof stats === 'object' ? stats : {};
    return {
      requests: Math.max(0, Math.floor(toFiniteNumber(row.requests, 0))),
      input_tokens: Math.max(0, Math.floor(toFiniteNumber(row.input_tokens, 0))),
      output_tokens: Math.max(0, Math.floor(toFiniteNumber(row.output_tokens, 0))),
      cached_tokens: Math.max(0, Math.floor(toFiniteNumber(row.cached_tokens, 0))),
      first_seen: Math.max(0, Math.floor(toFiniteNumber(row.first_seen, 0))),
      last_seen: Math.max(0, Math.floor(toFiniteNumber(row.last_seen, 0))),
    };
  }

  _trackIdentity(identity, ts) {
    var normalizedIdentity = normalizeIdentity(identity);
    if (!normalizedIdentity) return;
    var seenTs = Math.max(0, Math.floor(toFiniteNumber(ts, Date.now())));
    var stats = this._allIdentities.get(normalizedIdentity);
    if (!stats) {
      stats = this._createIdentityStats(seenTs);
      this._allIdentities.set(normalizedIdentity, stats);
      return;
    }
    if (stats.first_seen <= 0 || (seenTs > 0 && seenTs < stats.first_seen)) {
      stats.first_seen = seenTs;
    }
    if (seenTs > toFiniteNumber(stats.last_seen, 0)) {
      stats.last_seen = seenTs;
    }
  }

  _trackRequestStats(identity, features, context, ts) {
    var normalizedIdentity = normalizeIdentity(identity);
    if (!normalizedIdentity) return;

    var ctx = normalizeObject(context);
    var usage = normalizeObject(ctx.usage);
    var hasStatus = typeof ctx.status === 'number' && isFinite(ctx.status);
    var hasUsage = Object.keys(usage).length > 0;
    if (!hasStatus && !hasUsage) return;

    this._trackIdentity(normalizedIdentity, ts);
    var stats = this._allIdentities.get(normalizedIdentity);
    if (!stats) return;

    stats.requests += 1;
    var inputTokens = Math.max(0, Math.floor(toFiniteNumber(usage.input_tokens, 0)));
    var outputTokens = Math.max(0, Math.floor(toFiniteNumber(usage.output_tokens, 0)));
    var cachedTokens = Math.max(0, Math.floor(toFiniteNumber(usage.cached_tokens, 0)));

    if (!hasUsage) {
      // features.tokens_24h 是窗口累计值，这里只在缺失 usage 时用增量做兜底
      var featureTokens24h = Math.max(0, Math.floor(toFiniteNumber(features && features.tokens_24h, 0)));
      var prevFeatureTokens24h = Math.max(0, Math.floor(toFiniteNumber(stats._feature_tokens_24h, 0)));
      if (featureTokens24h > prevFeatureTokens24h) {
        inputTokens += (featureTokens24h - prevFeatureTokens24h);
      }
      stats._feature_tokens_24h = featureTokens24h;
    } else {
      stats._feature_tokens_24h = Math.max(0, Math.floor(toFiniteNumber(features && features.tokens_24h, stats._feature_tokens_24h)));
    }

    stats.input_tokens += inputTokens;
    stats.output_tokens += outputTokens;
    stats.cached_tokens += cachedTokens;

    var seenTs = Math.max(0, Math.floor(toFiniteNumber(ts, Date.now())));
    if (stats.first_seen <= 0 || (seenTs > 0 && seenTs < stats.first_seen)) {
      stats.first_seen = seenTs;
    }
    if (seenTs > toFiniteNumber(stats.last_seen, 0)) {
      stats.last_seen = seenTs;
    }
  }

  updateConfig(cfg) {
    this._config = normalizeConfig(cfg);
    return this.getRulesConfig();
  }

  getRulesConfig() {
    return cloneValue({
      enabled: this._config.enabled,
      retention_days: this._config.retention_days,
      cleanup_interval_hours: this._config.cleanup_interval_hours,
      rules: this._config.rules,
      thresholds: this._config.thresholds,
      actions: this._config.actions,
      auto_decay: this._config.auto_decay,
    });
  }

  _resolveLevel(score) {
    var s = toFiniteNumber(score, 0);
    if (s >= this._config.thresholds.suspend_min) return 'critical';
    if (s >= this._config.thresholds.challenge_min) return 'high';
    if (s >= this._config.thresholds.throttle_min) return 'medium';
    return 'low';
  }

  _resolveAutoAction(score) {
    var s = toFiniteNumber(score, 0);
    if (s >= this._config.thresholds.suspend_min) return 'suspend';
    if (s >= this._config.thresholds.challenge_min) return 'challenge';
    if (s >= this._config.thresholds.throttle_min) return 'throttle';
    return 'observe';
  }

  _resolveIdentityFromInput(identityOrId) {
    var raw = normalizeIdentity(identityOrId);
    if (!raw) return '';
    if (raw.indexOf(':') >= 0) return raw;
    if (this._userStore && typeof this._userStore.findByDiscordId === 'function') {
      try {
        var user = this._userStore.findByDiscordId(raw);
        if (user) return 'discord:' + raw;
      } catch (_) {
        // ignore
      }
    }
    return raw;
  }

  _extractDiscordUserId(identity) {
    if (!identity || identity.indexOf('discord:') !== 0) return '';
    return identity.slice('discord:'.length);
  }

  _buildBaseState(identity, features, ts) {
    var nowIso = isoNow(ts);
    return {
      caller_identity: identity,
      score: 0,
      level: 'low',
      reasons: [],
      flags: {},
      features: {
        requests_1m: toFiniteNumber(features.requests_1m, 0),
        requests_5m: toFiniteNumber(features.requests_5m, 0),
        requests_1h: toFiniteNumber(features.requests_1h, 0),
        requests_24h: toFiniteNumber(features.requests_24h, 0),
        tokens_24h: toFiniteNumber(features.tokens_24h, 0),
        error_rate_1h: round3(features.error_rate_1h),
        active_hours_24h: toFiniteNumber(features.active_hours_24h, 0),
        max_concurrency_5m: toFiniteNumber(features.max_concurrency_5m, 0),
        max_callers_per_ip_24h: toFiniteNumber(features.max_callers_per_ip_24h, 0),
        max_callers_per_ua_24h: toFiniteNumber(features.max_callers_per_ua_24h, 0),
        periodic_cv: features.periodic_cv === null ? null : round3(features.periodic_cv),
        periodic_samples: toFiniteNumber(features.periodic_samples, 0),
        unique_ips_24h: toFiniteNumber(features.unique_ips_24h, 0),
        unique_uas_24h: toFiniteNumber(features.unique_uas_24h, 0),
      },
      actions: {
        suggested: 'observe',
        applied: 'observe',
        manual: '',
        manual_reason: '',
        manual_set_at: '',
      },
      last_eval_at: nowIso,
      last_auto_action_at: '',
      updated_at: nowIso,
    };
  }

  _evaluateRules(features) {
    var rules = this._config.rules;
    var score = 0;
    var reasons = [];
    var flags = {};

    function hit(ruleId, value, threshold, extra, scoreValue) {
      score += scoreValue;
      flags[ruleId] = true;
      var reason = {
        rule_id: ruleId,
        value: value,
        threshold: threshold,
      };
      if (extra && typeof extra === 'object') {
        var keys = Object.keys(extra);
        for (var i = 0; i < keys.length; i++) {
          reason[keys[i]] = extra[keys[i]];
        }
      }
      reasons.push(reason);
    }

    if (rules.high_frequency && rules.high_frequency.enabled) {
      var hfThreshold = toFiniteNumber(rules.high_frequency.threshold, 120);
      var hfValue = toFiniteNumber(features.requests_1m, 0);
      if (hfValue > hfThreshold) {
        hit('high_frequency', hfValue, hfThreshold, null, rules.high_frequency.score);
      }
    }

    if (rules.high_concurrency && rules.high_concurrency.enabled) {
      var hcThreshold = toFiniteNumber(rules.high_concurrency.threshold, 8);
      var hcValue = Math.max(toFiniteNumber(features.max_concurrency_5m, 0), toFiniteNumber(features.current_inflight, 0));
      if (hcValue > hcThreshold) {
        hit('high_concurrency', hcValue, hcThreshold, null, rules.high_concurrency.score);
      }
    }

    if (rules.multi_account_ip && rules.multi_account_ip.enabled) {
      var mipThreshold = toFiniteNumber(rules.multi_account_ip.threshold, 5);
      var mipValue = toFiniteNumber(features.max_callers_per_ip_24h, 0);
      if (mipValue > mipThreshold) {
        hit('multi_account_ip', mipValue, mipThreshold, {
          top_ip: (features.ip_evidence && features.ip_evidence[0]) ? features.ip_evidence[0].key : '',
        }, rules.multi_account_ip.score);
      }
    }

    if (rules.no_sleep_window && rules.no_sleep_window.enabled) {
      var nsThreshold = toFiniteNumber(rules.no_sleep_window.threshold, 20);
      var nsValue = toFiniteNumber(features.active_hours_24h, 0);
      if (nsValue >= nsThreshold) {
        hit('no_sleep_window', nsValue, nsThreshold, null, rules.no_sleep_window.score);
      }
    }

    if (rules.periodic_pattern && rules.periodic_pattern.enabled) {
      var ppMaxCv = toFiniteNumber(rules.periodic_pattern.max_cv, 0.35);
      var ppMinSamples = Math.max(2, Math.floor(toFiniteNumber(rules.periodic_pattern.min_samples, 12)));
      var ppSamples = toFiniteNumber(features.periodic_samples, 0);
      var ppCv = features.periodic_cv === null ? null : toFiniteNumber(features.periodic_cv, 0);
      if (ppCv !== null && ppSamples >= ppMinSamples && ppCv <= ppMaxCv) {
        hit('periodic_pattern', round3(ppCv), ppMaxCv, {
          samples: ppSamples,
          mean_interval_ms: Math.round(toFiniteNumber(features.periodic_mean_interval_ms, 0)),
        }, rules.periodic_pattern.score);
      }
    }

    if (rules.high_error_rate && rules.high_error_rate.enabled) {
      var herThreshold = toFiniteNumber(rules.high_error_rate.threshold, 0.5);
      var herMinRequests = Math.max(1, Math.floor(toFiniteNumber(rules.high_error_rate.min_requests, 20)));
      var herRequests = toFiniteNumber(features.requests_1h, 0);
      var herRate = toFiniteNumber(features.error_rate_1h, 0);
      if (herRequests >= herMinRequests && herRate > herThreshold) {
        hit('high_error_rate', round3(herRate), herThreshold, {
          requests_1h: herRequests,
        }, rules.high_error_rate.score);
      }
    }

    if (rules.token_abuse && rules.token_abuse.enabled) {
      var taThreshold = toFiniteNumber(rules.token_abuse.threshold, 2000000);
      var taValue = toFiniteNumber(features.tokens_24h, 0);
      if (taValue > taThreshold) {
        hit('token_abuse', taValue, taThreshold, null, rules.token_abuse.score);
      }
    }

    return { score: score, reasons: reasons, flags: flags };
  }

  _syncUserRisk(identity, state, prevState) {
    if (!this._userStore || typeof this._userStore.updateRisk !== 'function') return;
    var discordUserId = this._extractDiscordUserId(identity);
    if (!discordUserId) return;

    try {
      this._userStore.updateRisk(discordUserId, {
        score: state.score,
        level: state.level,
        reasons: state.reasons,
        flags: state.flags,
        actions: state.actions,
        last_eval_at: state.last_eval_at,
        last_auto_action_at: state.last_auto_action_at,
      });
    } catch (_) {
      // ignore user risk sync failure
    }

    if (state.actions.applied === 'suspend' && (!prevState || !prevState.actions || prevState.actions.applied !== 'suspend')) {
      if (typeof this._userStore.banUser === 'function') {
        try {
          this._userStore.banUser(discordUserId, 'abuse_auto_suspend');
        } catch (_) {
          // ignore auto ban failure
        }
      }
    }
  }

  _logRiskChanges(identity, ip, uaHash, state, prevState) {
    if (!this._riskLogger || typeof this._riskLogger.logEvent !== 'function') return;
    var normalizedUaHash = String(uaHash || '').trim();
    var prevSig = reasonSignature(prevState && prevState.reasons);
    var nextSig = reasonSignature(state.reasons);
    var prevAction = prevState && prevState.actions ? prevState.actions.applied : '';
    var nextAction = state.actions.applied;
    var changed = (prevSig !== nextSig) || (prevAction !== nextAction) || (toFiniteNumber(prevState && prevState.score, -1) !== state.score);
    if (!changed) return;

    if (!state.reasons || state.reasons.length === 0) {
      this._riskLogger.logEvent({
        caller_identity: identity,
        ip: ip || '',
        ua_hash: normalizedUaHash,
        rule_id: 'risk_state',
        score: state.score,
        evidence: {
          from_action: prevAction || 'observe',
          to_action: nextAction,
        },
        action: nextAction,
      });
      return;
    }

    for (var i = 0; i < state.reasons.length; i++) {
      var reason = state.reasons[i];
      this._riskLogger.logEvent({
        caller_identity: identity,
        ip: ip || '',
        ua_hash: normalizedUaHash,
        rule_id: reason.rule_id,
        score: state.score,
        evidence: {
          reason: reason,
          level: state.level,
          features: state.features,
        },
        action: nextAction,
      });
    }
  }

  evaluate(identityOrId, context, options) {
    var opts = normalizeObject(options);
    var identity = this._resolveIdentityFromInput(identityOrId);
    if (!identity) {
      return this._buildBaseState('', getDefaultFeatures(''), Date.now());
    }
    var ctx = normalizeObject(context);
    var nowTs = toFiniteNumber(ctx.ts, Date.now());
    this._trackIdentity(identity, nowTs);
    var features = this._aggregator && typeof this._aggregator.getIdentityFeatures === 'function'
      ? this._aggregator.getIdentityFeatures(identity, nowTs)
      : getDefaultFeatures(identity);
    this._trackRequestStats(identity, features, ctx, nowTs);
    var prevState = this._states.get(identity) || null;
    var base = this._buildBaseState(identity, features, nowTs);
    var manual = this._manualActions.get(identity) || null;
    var hasManualOverride = !!(manual && manual.action);

    if (hasManualOverride && manual.action === 'observe') {
      base.actions.suggested = 'observe';
      base.actions.applied = 'observe';
      base.actions.manual = 'observe';
      base.actions.manual_reason = manual && manual.reason ? manual.reason : '';
      base.actions.manual_set_at = manual && manual.set_at ? manual.set_at : '';
      if (prevState && prevState.last_auto_action_at) {
        base.last_auto_action_at = prevState.last_auto_action_at;
      }
      this._states.set(identity, base);
      if (opts.syncUser !== false) {
        this._syncUserRisk(identity, base, prevState);
      }
      return cloneValue(base);
    }

    if (!this._config.enabled) {
      base.level = 'low';
      base.actions.suggested = 'observe';
      base.actions.applied = 'observe';
      this._states.set(identity, base);
      return cloneValue(base);
    }

    var evalResult = this._evaluateRules(features);
    base.score = evalResult.score;
    base.level = this._resolveLevel(base.score);
    base.reasons = evalResult.reasons;
    base.flags = evalResult.flags;

    var autoAction = this._resolveAutoAction(base.score);
    var autoDecay = this._config.auto_decay || defaultAutoDecay();
    if (!hasManualOverride && autoAction !== 'observe' && autoDecay.enabled !== false) {
      var previousAutoAt = prevState && prevState.last_auto_action_at ? String(prevState.last_auto_action_at) : '';
      if (previousAutoAt) {
        var previousAutoTs = Date.parse(previousAutoAt);
        if (isFinite(previousAutoTs) && previousAutoTs > 0 && (nowTs - previousAutoTs) >= autoDecay.interval_ms) {
          autoAction = 'observe';
        }
      }
    }

    base.actions.suggested = autoAction;
    base.actions.applied = hasManualOverride ? manual.action : autoAction;
    base.actions.manual = hasManualOverride ? manual.action : '';
    base.actions.manual_reason = manual && manual.reason ? manual.reason : '';
    base.actions.manual_set_at = manual && manual.set_at ? manual.set_at : '';

    var prevAppliedAction = prevState && prevState.actions
      ? String(prevState.actions.applied || 'observe')
      : 'observe';
    if (!hasManualOverride && autoAction !== 'observe') {
      if (prevState && prevState.last_auto_action_at && prevAppliedAction === autoAction) {
        base.last_auto_action_at = prevState.last_auto_action_at;
      } else {
        base.last_auto_action_at = base.updated_at;
      }
    } else if (prevState && prevState.last_auto_action_at) {
      base.last_auto_action_at = prevState.last_auto_action_at;
    }

    this._states.set(identity, base);

    if (opts.emitEvents !== false) {
      this._logRiskChanges(identity, ctx.ip || '', ctx.ua_hash || '', base, prevState);
    }
    if (opts.syncUser !== false) {
      this._syncUserRisk(identity, base, prevState);
    }

    return cloneValue(base);
  }

  enforceRequest(identityOrId, context) {
    var identity = this._resolveIdentityFromInput(identityOrId);
    if (!identity || !this._config.enabled) {
      return { allowed: true, action: 'observe', status: 200, reason: '' };
    }

    var state = this.evaluate(identity, context, { emitEvents: false, syncUser: true });
    var action = state && state.actions ? state.actions.applied : 'observe';
    if (action === 'suspend') {
      return {
        allowed: false,
        action: action,
        status: 403,
        reason: 'abuse_suspended',
        message: 'Request blocked by abuse detection policy (suspended).',
        state: state,
      };
    }
    if (action === 'challenge') {
      return {
        allowed: false,
        action: action,
        status: 403,
        reason: 'abuse_challenge_required',
        message: 'Request blocked by abuse detection policy (challenge required).',
        state: state,
      };
    }
    if (action === 'throttle') {
      var nowTs = Date.now();
      var intervalMs = this._config.actions.throttle_min_interval_ms;
      var lastAt = toFiniteNumber(this._throttleState.get(identity), 0);
      if (lastAt > 0 && nowTs - lastAt < intervalMs) {
        return {
          allowed: false,
          action: action,
          status: 429,
          reason: 'abuse_throttled',
          message: 'Request throttled by abuse detection policy.',
          retry_after: Math.max(1, Math.ceil((intervalMs - (nowTs - lastAt)) / 1000)),
          state: state,
        };
      }
      this._throttleState.set(identity, nowTs);
    }
    return { allowed: true, action: action, status: 200, reason: '', state: state };
  }

  getState(identityOrId) {
    var identity = this._resolveIdentityFromInput(identityOrId);
    if (!identity) return null;
    var state = this._states.get(identity);
    return state ? cloneValue(state) : null;
  }

  _getUserSummary(identity) {
    var discordUserId = this._extractDiscordUserId(identity);
    if (!discordUserId) return null;
    if (!this._userStore || typeof this._userStore.findByDiscordId !== 'function') return null;
    try {
      var user = this._userStore.findByDiscordId(discordUserId);
      if (!user) return null;
      return {
        discord_user_id: discordUserId,
        username: String(user.username || ''),
        global_name: String(user.global_name || ''),
        status: String(user.status || ''),
        last_login_at: String(user.last_login_at || ''),
      };
    } catch (_) {
      return null;
    }
  }

  listUsers(options) {
    var opts = normalizeObject(options);
    var page = Math.max(1, Math.floor(toFiniteNumber(opts.page, 1)));
    var limit = Math.max(1, Math.min(500, Math.floor(toFiniteNumber(opts.limit, 50))));
    var level = String(opts.level || '').trim();
    var action = String(opts.action || '').trim();
    var keyword = String(opts.keyword || opts.q || opts.search || '').trim().toLowerCase();
    var sort = String(opts.sort || 'requests_desc').trim();

    var profiles = new Map();
    function ensureProfile(identity) {
      var normalizedIdentity = normalizeIdentity(identity);
      if (!normalizedIdentity) return null;
      var row = profiles.get(normalizedIdentity);
      if (row) return row;
      row = {
        identity: normalizedIdentity,
        seq_id: '',
        username: '',
        display_name: '',
        avatar_url: '',
        created_at: '',
        _created_ts: 0,
      };
      profiles.set(normalizedIdentity, row);
      return row;
    }

    var registeredUsers = [];
    if (typeof this._getRegisteredUsers === 'function') {
      try {
        var rawRegisteredUsers = this._getRegisteredUsers();
        if (Array.isArray(rawRegisteredUsers)) {
          registeredUsers = rawRegisteredUsers;
        }
      } catch (_) {
        registeredUsers = [];
      }
    }

    for (var i = 0; i < registeredUsers.length; i++) {
      var rawRegistered = normalizeObject(registeredUsers[i]);
      var identity = normalizeIdentity(rawRegistered.identity);
      if (!identity) {
        var discordUserId = String(rawRegistered.discord_user_id || '').trim();
        if (discordUserId) identity = 'discord:' + discordUserId;
      }
      if (!identity) continue;
      var profile = ensureProfile(identity);
      if (!profile) continue;

      var seqId = String(rawRegistered.seq_id || '').trim();
      var username = String(rawRegistered.username || '').trim();
      var displayName = String(rawRegistered.display_name || rawRegistered.global_name || '').trim();
      var avatarUrl = String(rawRegistered.avatar_url || '').trim();
      var createdAt = String(rawRegistered.created_at || '').trim();

      if (seqId && !profile.seq_id) profile.seq_id = seqId;
      if (username && !profile.username) profile.username = username;
      if (displayName && !profile.display_name) profile.display_name = displayName;
      if (avatarUrl && !profile.avatar_url) profile.avatar_url = avatarUrl;
      if (createdAt) {
        var createdTs = toTimestampMs(createdAt, 0);
        if (!profile.created_at || (createdTs > 0 && (profile._created_ts <= 0 || createdTs < profile._created_ts))) {
          profile.created_at = createdAt;
          profile._created_ts = createdTs;
        }
      }
    }

    for (var identityInAll of this._allIdentities.keys()) {
      ensureProfile(identityInAll);
    }
    for (var identityInState of this._states.keys()) {
      ensureProfile(identityInState);
    }

    var all = [];
    for (var profileRow of profiles.values()) {
      var resolvedIdentity = profileRow.identity;
      var state = this._states.get(resolvedIdentity) || null;
      var stats = this._toPublicIdentityStats(this._allIdentities.get(resolvedIdentity));
      var userSummary = this._getUserSummary(resolvedIdentity);

      var usernameValue = profileRow.username;
      if (!usernameValue && userSummary) {
        usernameValue = String(userSummary.username || '').trim();
      }
      if (!usernameValue && resolvedIdentity.indexOf('discord:') !== 0) {
        usernameValue = resolvedIdentity;
      }

      var displayNameValue = profileRow.display_name;
      if (!displayNameValue) {
        displayNameValue = userSummary
          ? String(userSummary.global_name || userSummary.username || '').trim()
          : '';
      }
      if (!displayNameValue) displayNameValue = usernameValue || resolvedIdentity;

      var hitRules = [];
      if (state && Array.isArray(state.reasons)) {
        for (var hitIndex = 0; hitIndex < state.reasons.length; hitIndex++) {
          var hit = state.reasons[hitIndex] || {};
          var ruleId = String(hit.rule_id || '').trim();
          if (ruleId) hitRules.push(ruleId);
        }
      }

      var createdTsValue = toTimestampMs(profileRow.created_at, 0);
      var item = {
        identity: resolvedIdentity,
        id: resolvedIdentity,
        caller_identity: resolvedIdentity,
        seq_id: String(profileRow.seq_id || '').trim(),
        username: usernameValue,
        display_name: displayNameValue,
        avatar_url: String(profileRow.avatar_url || '').trim(),
        requests: stats.requests,
        input_tokens: stats.input_tokens,
        output_tokens: stats.output_tokens,
        cached_tokens: stats.cached_tokens,
        first_seen: stats.first_seen,
        last_seen: stats.last_seen,
        score: state ? toFiniteNumber(state.score, 0) : 0,
        level: state ? String(state.level || 'low') : 'low',
        action: state && state.actions ? String(state.actions.applied || 'observe') : 'observe',
        suggested_action: state && state.actions ? String(state.actions.suggested || 'observe') : 'observe',
        hit_rules: hitRules,
        reasons_count: hitRules.length,
        last_eval_at: state ? String(state.last_eval_at || '') : '',
        updated_at: state ? String(state.updated_at || '') : '',
        created_at: profileRow.created_at || '',
        user: userSummary,
        _created_ts: createdTsValue,
        _last_seen_sort: Math.max(0, Math.floor(toFiniteNumber(stats.last_seen, 0))),
        _updated_ts: toTimestampMs(state ? state.updated_at : '', 0),
      };

      if (level && item.level !== level) continue;
      if (action && item.action !== action) continue;
      if (keyword) {
        var target = (
          item.identity + ' '
          + item.username + ' '
          + item.display_name + ' '
          + item.seq_id
        ).toLowerCase();
        if (target.indexOf(keyword) < 0) continue;
      }
      all.push(item);
    }

    if (sort === 'score_asc') {
      all.sort(function (a, b) {
        if (a.score !== b.score) return a.score - b.score;
        if (a.requests !== b.requests) return b.requests - a.requests;
        return b._last_seen_sort - a._last_seen_sort;
      });
    } else if (sort === 'requests_desc') {
      all.sort(function (a, b) {
        if (a.requests !== b.requests) return b.requests - a.requests;
        if (a.input_tokens !== b.input_tokens) return b.input_tokens - a.input_tokens;
        return b._last_seen_sort - a._last_seen_sort;
      });
    } else if (sort === 'last_seen_desc') {
      all.sort(function (a, b) {
        if (a._last_seen_sort !== b._last_seen_sort) return b._last_seen_sort - a._last_seen_sort;
        if (a.requests !== b.requests) return b.requests - a.requests;
        return b.score - a.score;
      });
    } else if (sort === 'created_desc') {
      all.sort(function (a, b) {
        if (a._created_ts !== b._created_ts) return b._created_ts - a._created_ts;
        return b.requests - a.requests;
      });
    } else if (sort === 'updated_asc') {
      all.sort(function (a, b) { return a._updated_ts - b._updated_ts; });
    } else if (sort === 'updated_desc') {
      all.sort(function (a, b) { return b._updated_ts - a._updated_ts; });
    } else {
      all.sort(function (a, b) {
        if (a.score !== b.score) return b.score - a.score;
        if (a.requests !== b.requests) return b.requests - a.requests;
        return b._last_seen_sort - a._last_seen_sort;
      });
    }

    for (var cleanIndex = 0; cleanIndex < all.length; cleanIndex++) {
      delete all[cleanIndex]._created_ts;
      delete all[cleanIndex]._last_seen_sort;
      delete all[cleanIndex]._updated_ts;
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

  getUserDetail(identityOrId, options) {
    var opts = normalizeObject(options);
    var identity = this._resolveIdentityFromInput(identityOrId);
    if (!identity) return null;
    var state = this._states.get(identity);
    if (!state) {
      state = this.evaluate(identity, { ts: Date.now() }, { emitEvents: false, syncUser: true });
    }
    var features = this._aggregator && typeof this._aggregator.getIdentityFeatures === 'function'
      ? this._aggregator.getIdentityFeatures(identity, Date.now())
      : getDefaultFeatures(identity);
    var timeline = { data: [], total: 0, page: 1, pages: 1, limit: 100 };
    if (this._riskLogger && typeof this._riskLogger.listEvents === 'function') {
      timeline = this._riskLogger.listEvents({
        caller_identity: identity,
        page: Math.max(1, Math.floor(toFiniteNumber(opts.page, 1))),
        limit: Math.max(1, Math.min(500, Math.floor(toFiniteNumber(opts.limit, 100)))),
      });
    }
    return {
      identity: identity,
      risk: cloneValue(state),
      features: cloneValue(features),
      stats: this.getUserStats(identity),
      user: this._getUserSummary(identity),
      timeline: timeline,
    };
  }

  getOverview() {
    var levels = { low: 0, medium: 0, high: 0, critical: 0 };
    var actions = { observe: 0, throttle: 0, challenge: 0, suspend: 0 };

    for (var state of this._states.values()) {
      var level = String(state.level || 'low');
      var action = state.actions ? String(state.actions.applied || 'observe') : 'observe';
      if (levels[level] !== undefined) levels[level] += 1;
      if (actions[action] !== undefined) actions[action] += 1;
    }

    var todayEvents = this._riskLogger && typeof this._riskLogger.getTodayCount === 'function'
      ? this._riskLogger.getTodayCount()
      : 0;
    var identitySet = new Set();
    for (var identityInAll of this._allIdentities.keys()) {
      identitySet.add(identityInAll);
    }
    for (var identityInState of this._states.keys()) {
      identitySet.add(identityInState);
    }
    if (typeof this._getRegisteredUsers === 'function') {
      try {
        var registeredUsers = this._getRegisteredUsers();
        if (Array.isArray(registeredUsers)) {
          for (var i = 0; i < registeredUsers.length; i++) {
            var row = normalizeObject(registeredUsers[i]);
            var identity = normalizeIdentity(row.identity);
            if (!identity) {
              var discordId = String(row.discord_user_id || '').trim();
              if (discordId) identity = 'discord:' + discordId;
            }
            if (!identity) continue;
            identitySet.add(identity);
          }
        }
      } catch (_) {
        // ignore getRegisteredUsers callback errors
      }
    }
    var totalUsers = identitySet.size;
    if (typeof this._getUserCount === 'function') {
      try {
        var configuredTotal = toFiniteNumber(this._getUserCount(), totalUsers);
        if (configuredTotal >= 0) {
          totalUsers = Math.floor(configuredTotal);
        }
      } catch (_) {
        // ignore getUserCount callback errors
      }
    }
    return {
      total_users: totalUsers,
      risk_users: this._states.size,
      levels: levels,
      actions: actions,
      today_events: todayEvents,
      enabled: this._config.enabled,
    };
  }

  getUserStats(identityOrId) {
    var identity = this._resolveIdentityFromInput(identityOrId);
    if (!identity) return null;
    var stats = this._allIdentities.get(identity);
    if (!stats) return null;
    return cloneValue(this._toPublicIdentityStats(stats));
  }

  applyManualAction(identityOrId, action, options) {
    var identity = this._resolveIdentityFromInput(identityOrId);
    var requestedAction = String(action || '').trim();
    var opts = normalizeObject(options);
    var operator = String(opts.operator || 'admin').trim() || 'admin';
    var reason = String(opts.reason || '').trim();
    if (!identity) return null;

    var allowed = ['observe', 'throttle', 'challenge', 'suspend', 'restore'];
    if (allowed.indexOf(requestedAction) < 0) {
      throw new Error('invalid_action');
    }

    if (requestedAction === 'restore') {
      this._manualActions.delete(identity);
      var discordId = this._extractDiscordUserId(identity);
      if (discordId && this._userStore && typeof this._userStore.unbanUser === 'function') {
        try {
          this._userStore.unbanUser(discordId);
        } catch (_) {
          // ignore unban failure
        }
      }
    } else {
      this._manualActions.set(identity, {
        action: requestedAction,
        reason: reason,
        operator: operator,
        set_at: isoNow(Date.now()),
      });
      if (requestedAction === 'suspend') {
        var suspendUserId = this._extractDiscordUserId(identity);
        if (suspendUserId && this._userStore && typeof this._userStore.banUser === 'function') {
          try {
            this._userStore.banUser(suspendUserId, reason || 'abuse_manual_suspend');
          } catch (_) {
            // ignore manual ban failure
          }
        }
      }
    }

    var state = this.evaluate(identity, { ts: Date.now(), ip: String(opts.ip || '') }, { emitEvents: true, syncUser: true });
    if (this._riskLogger && typeof this._riskLogger.logEvent === 'function') {
      this._riskLogger.logEvent({
        caller_identity: identity,
        ip: String(opts.ip || ''),
        rule_id: requestedAction === 'restore' ? 'manual_restore' : 'manual_action',
        score: toFiniteNumber(state && state.score, 0),
        evidence: {
          requested_action: requestedAction,
          operator: operator,
          reason: reason,
        },
        action: requestedAction === 'restore' ? 'observe' : requestedAction,
      });
    }
    return state;
  }
}
