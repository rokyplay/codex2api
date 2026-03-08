import { log, C } from '../lib/utils.mjs';
import * as sessionStoreModule from '../lib/discord/session-store.mjs';
import * as userStoreModule from '../lib/discord/user-store.mjs';

var TAG = '[user-api]';

function routeLog(icon, color, message) {
  log(icon, color, TAG + ' ' + message);
}

function maybeAwait(value) {
  if (value && typeof value.then === 'function') return value;
  return Promise.resolve(value);
}

function toObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

var RISK_REASON_DESC_MAP = {
  high_frequency: '1小时请求频率过高',
  high_concurrency: '并发请求数量过高',
  multi_account_ip: '同一 IP 关联账号过多',
  no_sleep_window: '24小时活跃时段异常',
  periodic_pattern: '请求间隔呈异常周期性',
  high_error_rate: '请求错误率过高',
  token_abuse: '24小时Token消耗过大',
};

function toSafeInt(value, fallback, minValue) {
  var n = Math.floor(Number(value));
  if (!isFinite(n)) n = Math.floor(Number(fallback || 0));
  if (!isFinite(n)) n = 0;
  if (isFinite(Number(minValue)) && n < Number(minValue)) n = Number(minValue);
  return n;
}

function normalizeAutoDecayConfig(input) {
  var source = toObject(input);
  return {
    enabled: source.enabled !== false,
    interval_ms: toSafeInt(source.interval_ms, 600000, 1000),
  };
}

function getRiskReasonDescription(ruleId) {
  var id = String(ruleId || '').trim();
  if (!id) return '未知风险规则';
  return RISK_REASON_DESC_MAP[id] || ('命中规则：' + id);
}

function getRuleScore(ruleConfigMap, ruleId) {
  var id = String(ruleId || '').trim();
  if (!id) return 0;
  var row = toObject(ruleConfigMap[id]);
  return toSafeInt(row.score, 0, 0);
}

function mapRiskReasons(reasons, ruleConfigMap) {
  if (!Array.isArray(reasons)) return [];
  var out = [];
  for (var i = 0; i < reasons.length; i++) {
    var row = toObject(reasons[i]);
    var ruleId = String(row.rule_id || '').trim();
    if (!ruleId) continue;
    out.push({
      rule_id: ruleId,
      description: getRiskReasonDescription(ruleId),
      score: getRuleScore(ruleConfigMap, ruleId),
      value: row.value !== undefined ? row.value : null,
      threshold: row.threshold !== undefined ? row.threshold : null,
    });
  }
  return out;
}

function getAutoDecayRemainingMs(state, autoDecayConfig, nowTs) {
  var decay = toObject(autoDecayConfig);
  if (decay.enabled === false) return 0;
  var risk = toObject(state);
  var actions = toObject(risk.actions);
  if (String(actions.applied || 'observe') === 'observe') return 0;
  if (String(actions.manual || '').trim()) return 0;
  var lastAutoAt = String(risk.last_auto_action_at || '').trim();
  if (!lastAutoAt) return 0;
  var lastAutoTs = Date.parse(lastAutoAt);
  if (!isFinite(lastAutoTs) || lastAutoTs <= 0) return 0;
  var elapsed = toSafeInt(nowTs, Date.now(), 0) - lastAutoTs;
  var remaining = toSafeInt(decay.interval_ms, 600000, 1000) - elapsed;
  return remaining > 0 ? remaining : 0;
}

function normalizeConfig(input) {
  var wrapper = toObject(input);
  var config = wrapper.config ? toObject(wrapper.config) : wrapper;
  var discord = toObject(config.discord_auth);
  var abuse = toObject(config.abuse_detection);
  var stats = wrapper.stats || null;
  var stores = toObject(wrapper.stores);
  return {
    appConfig: config,
    stats: stats,
    ruleEngine: wrapper.ruleEngine || null,
    discord: {
      enabled: discord.enabled === true,
      guild_id: String(discord.guild_id || ''),
      api_key_prefix: String(discord.api_key_prefix || 'dk-'),
    },
    abuse: {
      rules: toObject(abuse.rules),
      auto_decay: normalizeAutoDecayConfig(abuse.auto_decay),
    },
    stores: stores,
  };
}

function createUserStoreAdapter(stores, discordConfig) {
  if (stores && stores.userStore && typeof stores.userStore === 'object') {
    return stores.userStore;
  }
  if (typeof userStoreModule.createDiscordUserStore === 'function') {
    return userStoreModule.createDiscordUserStore(discordConfig);
  }
  return userStoreModule;
}

function createSessionStoreAdapter(stores, discordConfig) {
  if (stores && stores.sessionStore && typeof stores.sessionStore === 'object') {
    return stores.sessionStore;
  }
  if (typeof sessionStoreModule.createDiscordSessionStore === 'function') {
    return sessionStoreModule.createDiscordSessionStore(discordConfig);
  }
  return sessionStoreModule;
}

function parseCookieMap(req) {
  var cookie = String((req && req.headers && req.headers.cookie) || '');
  if (!cookie) return {};
  var out = {};
  var parts = cookie.split(';');
  for (var i = 0; i < parts.length; i++) {
    var item = parts[i];
    var idx = item.indexOf('=');
    if (idx <= 0) continue;
    var key = item.slice(0, idx).trim();
    if (!key) continue;
    var value = item.slice(idx + 1).trim();
    try {
      out[key] = decodeURIComponent(value);
    } catch (_) {
      out[key] = value;
    }
  }
  return out;
}

function normalizeRoleList(value) {
  if (!Array.isArray(value)) return [];
  var out = [];
  for (var i = 0; i < value.length; i++) {
    var role = String(value[i] || '').trim();
    if (!role) continue;
    if (out.indexOf(role) >= 0) continue;
    out.push(role);
  }
  return out;
}

function userHasApiKey(user) {
  var obj = toObject(user);
  if (!obj || Object.keys(obj).length === 0) return false;
  if (obj.has_api_key === true) return true;
  if (obj.api_key || obj.apiKey) return true;
  if (obj.api_key_hash || obj.apiKeyHash) return true;
  if (obj.api_key_masked || obj.apiKeyMasked) return true;
  if (obj.api_key_last4 || obj.apiKeyLast4) return true;
  return false;
}

function maskApiKey(rawKey) {
  var key = String(rawKey || '');
  if (!key) return '';
  if (key.length <= 8) return '****';
  return key.slice(0, 4) + '...' + key.slice(-4);
}

function extractDiscordUserId(userObj) {
  return _extractDiscordUserId(userObj, null);
}

function isDiscordSeqId(value) {
  return /^discord_\d+$/.test(String(value || '').trim());
}

function isNumericDiscordUserId(value) {
  return /^\d{5,32}$/.test(String(value || '').trim());
}

function _resolveDiscordUserIdFromCandidate(value, userStore) {
  var raw = String(value || '').trim();
  if (!raw) return '';
  if (isNumericDiscordUserId(raw)) return raw;
  if (isDiscordSeqId(raw) && userStore && typeof userStore.findBySeqId === 'function') {
    try {
      var user = userStore.findBySeqId(raw);
      var resolved = String(user && user.discord_user_id || '').trim();
      if (isNumericDiscordUserId(resolved)) return resolved;
    } catch (_) {}
  }
  return '';
}

function _extractDiscordUserId(userObj, userStore) {
  var user = toObject(userObj);
  var direct = _resolveDiscordUserIdFromCandidate(user.discord_user_id, userStore);
  if (direct) return direct;
  direct = _resolveDiscordUserIdFromCandidate(user.discordId, userStore);
  if (direct) return direct;
  direct = _resolveDiscordUserIdFromCandidate(user.id, userStore);
  if (direct) return direct;
  if (user.user && typeof user.user === 'object') return _extractDiscordUserId(user.user, userStore);
  return '';
}

function json(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function getTodayDateString() {
  var now = new Date();
  var y = now.getFullYear();
  var m = String(now.getMonth() + 1).padStart(2, '0');
  var d = String(now.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + d;
}

function buildEmptyBucket() {
  return {
    requests: 0,
    input_tokens: 0,
    output_tokens: 0,
    cached_tokens: 0,
    reasoning_tokens: 0,
    total_tokens: 0,
    errors: 0,
  };
}

function normalizeBucket(bucket) {
  var row = toObject(bucket);
  var input = Number(row.input || row.input_tokens || 0);
  var output = Number(row.output || row.output_tokens || 0);
  var cached = Number(row.cached || row.cached_tokens || 0);
  var reasoning = Number(row.reasoning || row.reasoning_tokens || 0);
  return {
    requests: Number(row.requests || 0),
    input_tokens: input,
    output_tokens: output,
    cached_tokens: cached,
    reasoning_tokens: reasoning,
    total_tokens: input + output + cached + reasoning,
    errors: Number(row.errors || 0),
  };
}

function findIdentityBucket(rows, identity) {
  if (!Array.isArray(rows)) return null;
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i] || {};
    if (String(row.identity || '') === identity) {
      return row;
    }
  }
  return null;
}

function buildUsagePayload(stats, identity) {
  if (!stats || !identity) {
    return {
      identity: identity || '',
      today: buildEmptyBucket(),
      total: buildEmptyBucket(),
    };
  }

  var today = getTodayDateString();
  var todayRows = typeof stats.getCallerStatsRange === 'function'
    ? stats.getCallerStatsRange(today, today)
    : [];
  var totalRows = typeof stats.getCallerStatsTotal === 'function'
    ? stats.getCallerStatsTotal()
    : [];

  var todayBucket = normalizeBucket(findIdentityBucket(todayRows, identity));
  var totalBucket = normalizeBucket(findIdentityBucket(totalRows, identity));

  return {
    identity: identity,
    today: todayBucket,
    total: totalBucket,
  };
}

function parsePagination(searchParams) {
  var page = parseInt(searchParams.get('page') || '1', 10);
  if (!page || page < 1) page = 1;
  var limit = parseInt(searchParams.get('limit') || '20', 10);
  if (!limit || limit < 1) limit = 20;
  if (limit > 100) limit = 100;

  var hours = searchParams.get('hours');
  var normalizedHours = null;
  if (hours !== null && hours !== '') {
    var h = parseInt(hours, 10);
    if (h > 0 && h <= 720) normalizedHours = h;
  }

  return {
    page: page,
    limit: limit,
    hours: normalizedHours,
  };
}

function mapHistoryRecord(record) {
  var row = toObject(record);
  return {
    ts: Number(row.ts || 0),
    route: String(row.route || ''),
    path: String(row.path || ''),
    model: String(row.model || ''),
    status: Number(row.status || 0),
    latency: Number(row.latency || 0),
    ttfb_ms: Number(row.ttfb_ms || 0),
    stream: row.stream === true,
    input_tokens: Number(row.input_tokens || 0),
    output_tokens: Number(row.output_tokens || 0),
    cached_tokens: Number(row.cached_tokens || 0),
    reasoning_tokens: Number(row.reasoning_tokens || 0),
    error_type: row.error_type ? String(row.error_type) : null,
  };
}

function collectHistoryFromSearch(stats, identity, page, limit, hours) {
  if (!stats || typeof stats.searchRequests !== 'function') {
    return null;
  }

  var maxScanPage = 40;
  var scanPage = 1;
  var scanLimit = Math.max(limit * 5, 100);
  if (scanLimit > 1000) scanLimit = 1000;
  var all = [];

  while (scanPage <= maxScanPage) {
    var options = {
      page: scanPage,
      limit: scanLimit,
      search: identity,
    };
    if (hours) options.hours = hours;
    var result = stats.searchRequests(options) || {};
    var list = Array.isArray(result.data) ? result.data : [];
    for (var i = 0; i < list.length; i++) {
      var row = toObject(list[i]);
      if (String(row.caller_identity || '') === identity) {
        all.push(mapHistoryRecord(row));
      }
    }

    var totalPages = Number(result.pages || 0);
    if (!totalPages || scanPage >= totalPages) break;
    scanPage++;
  }

  var total = all.length;
  var pages = Math.ceil(total / limit) || 1;
  var normalizedPage = page > pages ? pages : page;
  var start = (normalizedPage - 1) * limit;
  var data = all.slice(start, start + limit);

  return {
    data: data,
    total: total,
    page: normalizedPage,
    pages: pages,
    limit: limit,
  };
}

function collectHistoryFromMemory(stats, identity, page, limit, hours) {
  if (!stats || typeof stats.getRecentRequests !== 'function') {
    return { data: [], total: 0, page: 1, pages: 1, limit: limit };
  }

  var base = stats.getRecentRequests(1, 2000, 'all', '', 'memory', '', hours || undefined) || {};
  var list = Array.isArray(base.data) ? base.data : [];
  var filtered = [];
  for (var i = 0; i < list.length; i++) {
    var row = toObject(list[i]);
    if (String(row.caller_identity || '') === identity) {
      filtered.push(mapHistoryRecord(row));
    }
  }

  var total = filtered.length;
  var pages = Math.ceil(total / limit) || 1;
  var normalizedPage = page > pages ? pages : page;
  var start = (normalizedPage - 1) * limit;
  var data = filtered.slice(start, start + limit);

  return {
    data: data,
    total: total,
    page: normalizedPage,
    pages: pages,
    limit: limit,
  };
}

export function createUserApiRoutes(config) {
  var normalized = normalizeConfig(config);
  var appConfig = normalized.appConfig;
  var stats = normalized.stats;
  var ruleEngine = normalized.ruleEngine;
  var abuseConfig = normalized.abuse;
  var stores = normalized.stores;
  var discordConfig = toObject(appConfig.discord_auth);
  var sessionStore = createSessionStoreAdapter(stores, discordConfig);
  var userStore = createUserStoreAdapter(stores, discordConfig);

  var initialized = false;
  var initPromise = null;
  var lastSessionCleanupAt = 0;

  async function ensureInit() {
    if (initialized) return;
    if (initPromise) return initPromise;
    initPromise = (async function () {
      if (typeof userStore.init === 'function') {
        await maybeAwait(userStore.init(appConfig));
      }
      if (typeof sessionStore.init === 'function') {
        await maybeAwait(sessionStore.init(appConfig));
      }
      initialized = true;
      routeLog('✅', C.green, '用户 API 路由初始化完成');
    })();
    return initPromise;
  }

  async function maybeCleanExpiredSessions() {
    var now = Date.now();
    if (now - lastSessionCleanupAt < 60 * 1000) return;
    lastSessionCleanupAt = now;
    if (typeof sessionStore.cleanExpired === 'function') {
      try {
        await maybeAwait(sessionStore.cleanExpired());
      } catch (err) {
        routeLog('⚠️', C.yellow, '清理过期 session 失败: ' + err.message);
      }
    }
  }

  async function getSessionFromCookie(req) {
    await ensureInit();
    await maybeCleanExpiredSessions();

    var cookies = parseCookieMap(req);
    var sessionId = String(cookies.session_id || '').trim();
    if (!sessionId || typeof sessionStore.getSession !== 'function') return null;

    var session = await maybeAwait(sessionStore.getSession(sessionId));
    if (!session) return null;

    var sessionObj = toObject(session);
    var expiresAt = Number(sessionObj.expires_at || sessionObj.expiresAt || 0);
    if (expiresAt > 0 && Date.now() > expiresAt) {
      if (typeof sessionStore.destroySession === 'function') {
        await maybeAwait(sessionStore.destroySession(sessionId));
      }
      return null;
    }

    var discordUserId = _extractDiscordUserId(sessionObj, userStore);
    if (!discordUserId) discordUserId = _extractDiscordUserId(sessionObj.user || {}, userStore);

    return {
      session_id: sessionId,
      session: sessionObj,
      discord_user_id: discordUserId,
    };
  }

  async function requireSession(req, res) {
    var current = await getSessionFromCookie(req);
    if (current) return current;
    json(res, 401, { success: false, error: 'unauthorized' });
    return null;
  }

  async function handleProfile(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    var discordUserId = current.discord_user_id;
    if (!discordUserId) {
      return json(res, 400, { success: false, error: 'invalid_session_user' });
    }

    var user = null;
    if (typeof userStore.findByDiscordId === 'function') {
      user = await maybeAwait(userStore.findByDiscordId(discordUserId));
    }
    user = toObject(user);

    var identity = 'discord:' + discordUserId;
    var usage = buildUsagePayload(stats, identity);
    var rawApiKey = String(user.api_key || user.apiKey || '');
    var maskedApiKey = String(user.api_key_masked || user.apiKeyMasked || maskApiKey(rawApiKey));
    if (!maskedApiKey && user.api_key_last4) {
      maskedApiKey = String(discordConfig.api_key_prefix || 'dk-') + '***' + String(user.api_key_last4);
    }
    if (!maskedApiKey && userHasApiKey(user)) {
      maskedApiKey = String(discordConfig.api_key_prefix || 'dk-') + '***';
    }

    return json(res, 200, {
      success: true,
      data: {
        user: {
          discord_user_id: discordUserId,
          seq_id: String(user.seq_id || ''),
          username: String(user.username || ''),
          global_name: String(user.global_name || ''),
          avatar: String(user.avatar || ''),
          guild_id: String(user.guild_id || discordConfig.guild_id || ''),
          roles: normalizeRoleList(user.roles || []),
          is_banned: user.is_banned === true || String(user.status || '').toLowerCase() === 'banned',
          last_login_at: String(user.last_login_at || ''),
        },
        api_key: {
          has_key: userHasApiKey(user),
          masked: maskedApiKey || '',
        },
        usage: usage,
      },
    });
  }

  async function handleUsage(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    var discordUserId = current.discord_user_id;
    if (!discordUserId) {
      return json(res, 400, { success: false, error: 'invalid_session_user' });
    }

    var identity = 'discord:' + discordUserId;
    var usage = buildUsagePayload(stats, identity);
    json(res, 200, { success: true, data: usage });
  }

  async function handleHistory(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    var discordUserId = current.discord_user_id;
    if (!discordUserId) {
      return json(res, 400, { success: false, error: 'invalid_session_user' });
    }
    var identity = 'discord:' + discordUserId;
    var parsedUrl = new URL(req.url, 'http://localhost');
    var paging = parsePagination(parsedUrl.searchParams);

    var history = collectHistoryFromSearch(stats, identity, paging.page, paging.limit, paging.hours);
    if (!history) {
      history = collectHistoryFromMemory(stats, identity, paging.page, paging.limit, paging.hours);
    }

    json(res, 200, {
      success: true,
      data: history.data,
      meta: {
        total: history.total,
        page: history.page,
        pages: history.pages,
        limit: history.limit,
      },
    });
  }

  async function handlePublicStats(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    var overview = stats && typeof stats.getOverviewTotal === 'function'
      ? stats.getOverviewTotal()
      : (stats && typeof stats.getOverview === 'function' ? stats.getOverview() : {});

    var data = toObject(overview);
    var totalInput = Number(data.total_input_tokens || 0);
    var totalOutput = Number(data.total_output_tokens || 0);
    var totalCached = Number(data.total_cached_tokens || 0);
    var totalReasoning = Number(data.total_reasoning_tokens || 0);

    json(res, 200, {
      success: true,
      data: {
        total_requests: Number(data.total_requests || 0),
        total_tokens: totalInput + totalOutput + totalCached + totalReasoning,
        success_rate: Number(data.success_rate || 0),
      },
    });
  }

  function mapGlobalRecentRecord(record) {
    var row = toObject(record);
    return {
      ts: Number(row.ts || 0),
      status: Number(row.status || 0),
      model: String(row.model || ''),
      input_tokens: Number(row.input_tokens || 0),
      output_tokens: Number(row.output_tokens || 0),
      error: row.error_type
        ? String(row.error_type)
        : (row.error ? String(row.error) : null),
    };
  }

  async function handleGlobalStats(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    var parsedUrl = new URL(req.url, 'http://localhost');
    var paging = parsePagination(parsedUrl.searchParams);
    var hours = paging.hours || 1;

    var recentPayload = stats && typeof stats.getRecentRequests === 'function'
      ? stats.getRecentRequests(1, 100, 'all', '', 'memory', '', hours)
      : {};
    var recentList = Array.isArray(recentPayload && recentPayload.data) ? recentPayload.data : [];
    var recent = [];
    for (var i = 0; i < recentList.length && i < 100; i++) {
      recent.push(mapGlobalRecentRecord(recentList[i]));
    }

    var overview = stats && typeof stats.getOverviewTotal === 'function'
      ? stats.getOverviewTotal()
      : (stats && typeof stats.getOverview === 'function' ? stats.getOverview() : {});
    var total = toObject(overview);

    var requests1h = 0;
    var requests24h = 0;
    if (stats && typeof stats.getOverviewLastHours === 'function') {
      var last1h = toObject(stats.getOverviewLastHours(1));
      var last24h = toObject(stats.getOverviewLastHours(24));
      requests1h = Number(last1h.total_requests || 0);
      requests24h = Number(last24h.total_requests || 0);
    } else if (stats && typeof stats.getRecentRequests === 'function') {
      var oneHour = toObject(stats.getRecentRequests(1, 1, 'all', '', 'file', '', 1));
      var day = toObject(stats.getRecentRequests(1, 1, 'all', '', 'file', '', 24));
      requests1h = Number(oneHour.total || 0);
      requests24h = Number(day.total || 0);
    }

    json(res, 200, {
      success: true,
      data: {
        recent: recent,
        summary: {
          total_requests: Number(total.total_requests || 0),
          success_rate: Number(total.success_rate || 0),
          total_input_tokens: Number(total.total_input_tokens || 0),
          total_output_tokens: Number(total.total_output_tokens || 0),
          requests_1h: requests1h,
          requests_24h: requests24h,
        },
      },
    });
  }

  async function handleRiskStatus(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    if (!ruleEngine || typeof ruleEngine.evaluate !== 'function') {
      return json(res, 503, { success: false, error: 'abuse_engine_not_available' });
    }

    var discordUserId = current.discord_user_id;
    if (!discordUserId) {
      return json(res, 400, { success: false, error: 'invalid_session_user' });
    }

    var identity = 'discord:' + discordUserId;
    var nowTs = Date.now();
    var riskState;
    try {
      riskState = ruleEngine.evaluate(identity, { ts: nowTs }, { emitEvents: false, syncUser: true });
    } catch (err) {
      routeLog('⚠️', C.yellow, '读取风险状态失败: ' + (err && err.message ? err.message : 'unknown'));
      return json(res, 500, { success: false, error: 'risk_status_load_failed' });
    }

    var risk = toObject(riskState);
    var rulesConfig = toObject(abuseConfig && abuseConfig.rules);
    if (ruleEngine && typeof ruleEngine.getRulesConfig === 'function') {
      var snapshot = toObject(ruleEngine.getRulesConfig());
      rulesConfig = toObject(snapshot.rules);
    }

    var score = Number(risk.score || 0);
    if (!isFinite(score)) score = 0;
    var actions = toObject(risk.actions);
    var autoDecayRemainingMs = getAutoDecayRemainingMs(risk, abuseConfig.auto_decay, nowTs);

    return json(res, 200, {
      success: true,
      data: {
        score: score,
        level: String(risk.level || 'low'),
        reasons: mapRiskReasons(risk.reasons, rulesConfig),
        actions: {
          applied: String(actions.applied || 'observe'),
        },
        auto_decay_remaining_ms: autoDecayRemainingMs,
      },
    });
  }

  async function handleRequest(req, res, pathname, method) {
    var path = String(pathname || '');
    var m = String(method || req.method || '').toUpperCase();

    if (m === 'GET' && path === '/user/api/profile') {
      await handleProfile(req, res);
      return true;
    }
    if (m === 'GET' && path === '/user/api/usage') {
      await handleUsage(req, res);
      return true;
    }
    if (m === 'GET' && path === '/user/api/history') {
      await handleHistory(req, res);
      return true;
    }
    if (m === 'GET' && path === '/user/api/stats/public') {
      await handlePublicStats(req, res);
      return true;
    }
    if (m === 'GET' && path === '/user/api/stats/global') {
      await handleGlobalStats(req, res);
      return true;
    }
    if (m === 'GET' && path === '/user/api/risk-status') {
      await handleRiskStatus(req, res);
      return true;
    }
    return false;
  }

  return {
    handleRequest: handleRequest,
    getSessionFromCookie: getSessionFromCookie,
  };
}
