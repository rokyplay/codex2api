/**
 * 模型名映射 + 前缀处理
 *
 *
 * 核心功能:
 *   - resolveModel('codex/gpt-5-codex-mini') → 'gpt-5-codex-mini'
 *   - resolveModel('gpt-5-codex-latest') → 'gpt-5.3-codex'（通过 aliases）
 *   - addPrefix('gpt-5-codex-mini') → 'codex/gpt-5-codex-mini'
 *   - listModels() → 默认带 prefix 的模型数组（可配置关闭）
 *   - isModelAvailable(model) → boolean
 *   - fetchUpstreamModels(getToken) → 从上游动态发现模型（带缓存）
 */

var _config = null;
var _upstreamCache = {
  available: null,
  order: [],
  source: '',
  fetchedAt: 0,
  expiresAt: 0,
  rawCount: 0,
  codexCount: 0,
  triedEndpoints: [],
  lastError: '',
  lastStatus: 0,
};
var _upstreamFetchPromise = null;

var DEFAULT_CACHE_TTL_MS = 60 * 60 * 1000; // 1h
var DEFAULT_TIMEOUT_MS = 20000;
var DEFAULT_CLIENT_VERSION = '0.99.0';
var DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.88 Safari/537.36';

var REASONING_EFFORT_SET = {
  low: true,
  medium: true,
  high: true,
  xhigh: true,
};

function parseReasoningEffortSuffix(model) {
  var raw = String(model || '').trim();
  if (!raw) return { model: raw, reasoningEffort: '' };
  var match = raw.match(/^(.*)\(([^)]+)\)\s*$/);
  if (!match) return { model: raw, reasoningEffort: '' };
  var baseModel = String(match[1] || '').trim();
  var effort = String(match[2] || '').trim().toLowerCase();
  if (!baseModel || !REASONING_EFFORT_SET[effort]) {
    return { model: raw, reasoningEffort: '' };
  }
  return {
    model: baseModel,
    reasoningEffort: effort,
  };
}

function safeNumber(value, fallback) {
  var n = Number(value);
  if (!isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function deepClone(value) {
  if (value === null || value === undefined) return value;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_) {
    return value;
  }
}

function hasValidUpstreamCache() {
  return !!(
    _upstreamCache
    && _upstreamCache.available
    && _upstreamCache.order
    && _upstreamCache.order.length > 0
    && _upstreamCache.expiresAt > Date.now()
  );
}

function safeTrim(value) {
  return String(value || '').trim();
}

function normalizeModelId(value) {
  return safeTrim(value);
}

function normalizeModelDisplayName(value, fallbackId) {
  var text = safeTrim(value);
  return text || String(fallbackId || '');
}

function getFirstEnabledModel(available) {
  if (!available || typeof available !== 'object') return '';
  var keys = Object.keys(available);
  for (var i = 0; i < keys.length; i++) {
    var name = keys[i];
    var item = available[name];
    if (!item || item.enabled === false) continue;
    return name;
  }
  return keys[0] || '';
}

function buildEffectiveConfig() {
  var base = _config && typeof _config === 'object' ? _config : {};
  var prefix = safeTrim(base.prefix || '');
  var baseAliases = (base.aliases && typeof base.aliases === 'object') ? base.aliases : {};
  var baseAvailable = (base.available && typeof base.available === 'object') ? base.available : {};

  if (!hasValidUpstreamCache()) {
    return {
      prefix: prefix,
      default: safeTrim(base.default || '') || getFirstEnabledModel(baseAvailable) || 'gpt-5-codex-mini',
      available: baseAvailable,
      aliases: baseAliases,
    };
  }

  var available = {};
  for (var i = 0; i < _upstreamCache.order.length; i++) {
    var name = _upstreamCache.order[i];
    var item = _upstreamCache.available[name] || {};
    available[name] = {
      display_name: normalizeModelDisplayName(item.display_name, name),
      enabled: item.enabled !== false,
    };
  }

  var aliases = {};
  var aliasKeys = Object.keys(baseAliases);
  for (var j = 0; j < aliasKeys.length; j++) {
    var aliasName = safeTrim(aliasKeys[j]);
    var target = safeTrim(baseAliases[aliasName]);
    if (!aliasName || !target) continue;
    if (!available[target]) continue;
    if (available[aliasName]) continue;
    aliases[aliasName] = target;
  }

  var defaultModel = safeTrim(base.default || '');
  if (!defaultModel || !available[defaultModel] || available[defaultModel].enabled === false) {
    defaultModel = getFirstEnabledModel(available) || defaultModel || 'gpt-5-codex-mini';
  }

  return {
    prefix: prefix,
    default: defaultModel,
    available: available,
    aliases: aliases,
  };
}

function normalizeGetTokenResult(tokenResult) {
  if (!tokenResult) return null;
  if (typeof tokenResult === 'string') {
    var rawToken = safeTrim(tokenResult);
    if (!rawToken) return null;
    return { accessToken: rawToken };
  }
  if (typeof tokenResult !== 'object') return null;
  var accessToken = safeTrim(tokenResult.accessToken || tokenResult.token || '');
  if (!accessToken) return null;
  return {
    accessToken: accessToken,
    sessionToken: safeTrim(tokenResult.sessionToken || ''),
    cookies: tokenResult.cookies && typeof tokenResult.cookies === 'object' ? tokenResult.cookies : {},
    userAgent: safeTrim(tokenResult.userAgent || tokenResult['User-Agent'] || ''),
    deviceId: safeTrim(tokenResult.deviceId || tokenResult.oaiDeviceId || ''),
    headers: tokenResult.headers && typeof tokenResult.headers === 'object' ? tokenResult.headers : {},
    email: safeTrim(tokenResult.email || ''),
  };
}

function buildCookieHeader(tokenInfo) {
  var parts = [];
  if (tokenInfo && tokenInfo.sessionToken) {
    parts.push('__Secure-next-auth.session-token=' + tokenInfo.sessionToken);
  }
  var cookies = tokenInfo && tokenInfo.cookies ? tokenInfo.cookies : {};
  var neededCookieKeys = ['oai-did', 'cf_clearance', '__cf_bm'];
  for (var i = 0; i < neededCookieKeys.length; i++) {
    var key = neededCookieKeys[i];
    if (!cookies[key]) continue;
    parts.push(key + '=' + String(cookies[key]));
  }
  return parts.join('; ');
}

function buildUpstreamHeaders(tokenInfo, userAgent) {
  var headers = {
    'Authorization': 'Bearer ' + tokenInfo.accessToken,
    'Accept': 'application/json',
    'User-Agent': userAgent || DEFAULT_USER_AGENT,
  };

  var deviceId = safeTrim(tokenInfo.deviceId || (tokenInfo.cookies && tokenInfo.cookies['oai-did']) || '');
  if (deviceId) {
    headers['Oai-Device-Id'] = deviceId;
  }

  var cookieHeader = buildCookieHeader(tokenInfo);
  if (cookieHeader) {
    headers['Cookie'] = cookieHeader;
  }

  var extraHeaders = tokenInfo.headers || {};
  var extraKeys = Object.keys(extraHeaders);
  for (var i = 0; i < extraKeys.length; i++) {
    var k = extraKeys[i];
    if (!k) continue;
    var v = extraHeaders[k];
    if (v === undefined || v === null) continue;
    if (headers[k] !== undefined) continue;
    headers[k] = String(v);
  }

  return headers;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_) {
    return null;
  }
}

function summarizeErrorBody(rawText) {
  var text = String(rawText || '');
  if (!text) return '';
  var parsed = safeJsonParse(text);
  if (parsed && typeof parsed === 'object') {
    if (typeof parsed.error === 'string') return parsed.error;
    if (parsed.error && typeof parsed.error.message === 'string') return parsed.error.message;
    if (typeof parsed.message === 'string') return parsed.message;
    if (typeof parsed.detail === 'string') return parsed.detail;
    try {
      return JSON.stringify(parsed).slice(0, 400);
    } catch (_) {
      return text.slice(0, 400);
    }
  }
  return text.slice(0, 400);
}

function extractRawModels(payload) {
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload.models)) return payload.models;
  if (Array.isArray(payload.data)) return payload.data;
  return [];
}

function normalizeModelEntry(raw, index) {
  if (!raw) return null;
  if (typeof raw === 'string') {
    var idFromString = normalizeModelId(raw);
    if (!idFromString) return null;
    return {
      id: idFromString,
      display_name: idFromString,
      priority: index,
      shell_type: '',
    };
  }
  if (typeof raw !== 'object') return null;

  var id = normalizeModelId(raw.slug || raw.id || raw.name || raw.model || '');
  if (!id) return null;

  return {
    id: id,
    display_name: normalizeModelDisplayName(raw.display_name || raw.title || raw.name || raw.id, id),
    priority: isFinite(Number(raw.priority)) ? Number(raw.priority) : index,
    shell_type: safeTrim(raw.shell_type || ''),
    supported_in_api: raw.supported_in_api,
  };
}

function isCodexRelatedModel(entry, endpointName) {
  var idLower = String(entry.id || '').toLowerCase();
  var displayLower = String(entry.display_name || '').toLowerCase();
  if (idLower.indexOf('codex') >= 0) return true;
  if (displayLower.indexOf('codex') >= 0) return true;
  // codex 端点返回的 shell_command 模型视为可用于 codex 能力
  if (String(endpointName || '').indexOf('codex') >= 0 && String(entry.shell_type || '').toLowerCase() === 'shell_command') {
    return true;
  }
  return false;
}

function mergeDiscoveredModels(entries) {
  var map = {};
  for (var i = 0; i < entries.length; i++) {
    var item = entries[i];
    var id = normalizeModelId(item && item.id);
    if (!id) continue;
    if (!map[id]) {
      map[id] = {
        display_name: normalizeModelDisplayName(item.display_name, id),
        enabled: true,
        priority: isFinite(Number(item.priority)) ? Number(item.priority) : i,
      };
      continue;
    }
    var newPriority = isFinite(Number(item.priority)) ? Number(item.priority) : i;
    if (newPriority < map[id].priority) {
      map[id].priority = newPriority;
    }
    if (!map[id].display_name && item.display_name) {
      map[id].display_name = normalizeModelDisplayName(item.display_name, id);
    }
  }

  var keys = Object.keys(map);
  keys.sort(function (a, b) {
    var p1 = map[a].priority;
    var p2 = map[b].priority;
    if (p1 !== p2) return p1 - p2;
    return String(a).localeCompare(String(b));
  });

  var available = {};
  for (var j = 0; j < keys.length; j++) {
    var name = keys[j];
    var data = map[name];
    available[name] = {
      display_name: normalizeModelDisplayName(data.display_name, name),
      enabled: true,
    };
  }

  return {
    order: keys,
    available: available,
  };
}

function listDefaultEndpoints(clientVersion) {
  var cv = safeTrim(clientVersion || DEFAULT_CLIENT_VERSION) || DEFAULT_CLIENT_VERSION;
  return [
    {
      name: 'chatgpt_codex_models',
      url: 'https://chatgpt.com/backend-api/codex/models?client_version=' + encodeURIComponent(cv),
    },
    {
      name: 'chatgpt_models',
      url: 'https://chatgpt.com/backend-api/models',
    },
    {
      name: 'openai_v1_models',
      url: 'https://api.openai.com/v1/models',
    },
  ];
}

async function requestUpstreamModels(endpoint, headers, timeoutMs) {
  var timeout = safeNumber(timeoutMs, DEFAULT_TIMEOUT_MS);
  var statusCode = 0;
  var contentType = '';
  try {
    var resp = await fetch(endpoint.url, {
      method: 'GET',
      headers: headers,
      signal: AbortSignal.timeout(timeout),
    });
    statusCode = resp.status;
    contentType = safeTrim(resp.headers.get('content-type') || '');
    var text = await resp.text();
    if (!resp.ok) {
      return {
        ok: false,
        status: statusCode,
        endpoint: endpoint,
        error: summarizeErrorBody(text) || ('http_' + statusCode),
        contentType: contentType,
      };
    }
    var payload = safeJsonParse(text);
    if (!payload || typeof payload !== 'object') {
      return {
        ok: false,
        status: statusCode,
        endpoint: endpoint,
        error: 'invalid_json',
        contentType: contentType,
      };
    }
    var rawModels = extractRawModels(payload);
    if (!rawModels.length) {
      return {
        ok: false,
        status: statusCode,
        endpoint: endpoint,
        error: 'empty_models',
        contentType: contentType,
      };
    }

    var normalized = [];
    for (var i = 0; i < rawModels.length; i++) {
      var item = normalizeModelEntry(rawModels[i], i);
      if (!item) continue;
      if (item.supported_in_api === false) continue;
      if (!isCodexRelatedModel(item, endpoint.name)) continue;
      normalized.push(item);
    }

    if (!normalized.length) {
      return {
        ok: false,
        status: statusCode,
        endpoint: endpoint,
        error: 'no_codex_models',
        rawCount: rawModels.length,
        contentType: contentType,
      };
    }

    return {
      ok: true,
      status: statusCode,
      endpoint: endpoint,
      rawCount: rawModels.length,
      codexCount: normalized.length,
      models: normalized,
      contentType: contentType,
    };
  } catch (err) {
    return {
      ok: false,
      status: statusCode,
      endpoint: endpoint,
      error: err && err.message ? err.message : 'network_error',
      contentType: contentType,
    };
  }
}

function updateUpstreamCacheSuccess(discovered, source, ttlMs, rawCount, codexCount, attempts) {
  var now = Date.now();
  _upstreamCache.available = deepClone(discovered.available || {});
  _upstreamCache.order = Array.isArray(discovered.order) ? discovered.order.slice() : [];
  _upstreamCache.source = safeTrim(source || '');
  _upstreamCache.fetchedAt = now;
  _upstreamCache.expiresAt = now + safeNumber(ttlMs, DEFAULT_CACHE_TTL_MS);
  _upstreamCache.rawCount = safeNumber(rawCount, 0);
  _upstreamCache.codexCount = safeNumber(codexCount, _upstreamCache.order.length);
  _upstreamCache.triedEndpoints = Array.isArray(attempts) ? attempts.slice() : [];
  _upstreamCache.lastError = '';
  _upstreamCache.lastStatus = 200;
}

function updateUpstreamCacheFailure(errorMessage, statusCode, attempts) {
  _upstreamCache.triedEndpoints = Array.isArray(attempts) ? attempts.slice() : [];
  _upstreamCache.lastError = safeTrim(errorMessage || '') || 'fetch_failed';
  _upstreamCache.lastStatus = safeNumber(statusCode, 0);
}

function buildFetchResult(success, extra) {
  var payload = extra && typeof extra === 'object' ? extra : {};
  var snapshot = getUpstreamModelsSnapshot();
  payload.success = !!success;
  payload.cache = snapshot;
  return payload;
}

/**
 * 初始化，传入 config-server.json 的 models 段
 */
export function init(modelsConfig) {
  _config = modelsConfig;
}

/**
 * 模型配置热更新（与 init 语义一致）
 *
 * @param {object} modelsConfig
 */
export function hotReload(modelsConfig) {
  init(modelsConfig);
}

/**
 * 获取当前模型配置快照（调试/管理接口）
 *
 * @returns {object|null}
 */
export function getConfigSnapshot() {
  if (!_config) return null;
  return JSON.parse(JSON.stringify(_config));
}

/**
 * 获取动态模型缓存快照（管理面板/调试用）
 *
 * @returns {{
 *  cache_valid: boolean,
 *  source: string,
 *  fetched_at: number,
 *  expires_at: number,
 *  expires_in_ms: number,
 *  raw_count: number,
 *  codex_count: number,
 *  models: string[],
 *  tried_endpoints: Array,
 *  last_error: string,
 *  last_status: number
 * }}
 */
export function getUpstreamModelsSnapshot() {
  var now = Date.now();
  var models = Array.isArray(_upstreamCache.order) ? _upstreamCache.order.slice() : [];
  var expiresInMs = (_upstreamCache.expiresAt || 0) - now;
  return {
    cache_valid: hasValidUpstreamCache(),
    source: safeTrim(_upstreamCache.source || ''),
    fetched_at: safeNumber(_upstreamCache.fetchedAt, 0),
    expires_at: safeNumber(_upstreamCache.expiresAt, 0),
    expires_in_ms: expiresInMs > 0 ? expiresInMs : 0,
    raw_count: safeNumber(_upstreamCache.rawCount, 0),
    codex_count: safeNumber(_upstreamCache.codexCount, models.length),
    models: models,
    tried_endpoints: Array.isArray(_upstreamCache.triedEndpoints) ? deepClone(_upstreamCache.triedEndpoints) : [],
    last_error: safeTrim(_upstreamCache.lastError || ''),
    last_status: safeNumber(_upstreamCache.lastStatus, 0),
  };
}

/**
 * 从上游拉取模型列表并更新本地缓存
 *
 * @param {() => Promise<string|object>|string|object} getToken
 * @param {{
 *  force?: boolean,
 *  cacheTtlMs?: number,
 *  timeoutMs?: number,
 *  clientVersion?: string,
 *  userAgent?: string,
 *  endpoints?: Array<{name:string,url:string}>,
 *  logger?: (message: string, meta?: object) => void
 * }} [options]
 * @returns {Promise<object>}
 */
export async function fetchUpstreamModels(getToken, options) {
  var opts = options || {};
  var force = opts.force === true;
  var cacheTtlMs = safeNumber(opts.cacheTtlMs, DEFAULT_CACHE_TTL_MS);
  var timeoutMs = safeNumber(opts.timeoutMs, DEFAULT_TIMEOUT_MS);
  var clientVersion = safeTrim(opts.clientVersion || DEFAULT_CLIENT_VERSION) || DEFAULT_CLIENT_VERSION;
  var logger = typeof opts.logger === 'function' ? opts.logger : null;

  if (!force && hasValidUpstreamCache()) {
    return buildFetchResult(true, {
      from_cache: true,
      source: _upstreamCache.source,
      models: _upstreamCache.order.slice(),
    });
  }

  if (_upstreamFetchPromise) {
    return _upstreamFetchPromise;
  }

  _upstreamFetchPromise = (async function () {
    if (typeof getToken !== 'function') {
      var noGetterError = 'missing_get_token';
      updateUpstreamCacheFailure(noGetterError, 0, []);
      return buildFetchResult(false, {
        error: noGetterError,
      });
    }

    var tokenResult = null;
    try {
      tokenResult = await getToken();
    } catch (tokenErr) {
      var tokenError = tokenErr && tokenErr.message ? tokenErr.message : 'get_token_failed';
      updateUpstreamCacheFailure(tokenError, 0, []);
      return buildFetchResult(false, {
        error: tokenError,
      });
    }

    var tokenInfo = normalizeGetTokenResult(tokenResult);
    if (!tokenInfo || !tokenInfo.accessToken) {
      var noTokenError = 'no_access_token';
      updateUpstreamCacheFailure(noTokenError, 0, []);
      return buildFetchResult(false, {
        error: noTokenError,
      });
    }

    var headers = buildUpstreamHeaders(tokenInfo, safeTrim(opts.userAgent || tokenInfo.userAgent || '') || DEFAULT_USER_AGENT);
    var endpoints = Array.isArray(opts.endpoints) && opts.endpoints.length > 0
      ? opts.endpoints
      : listDefaultEndpoints(clientVersion);

    var attempts = [];
    for (var i = 0; i < endpoints.length; i++) {
      var endpoint = endpoints[i];
      if (!endpoint || !endpoint.url) continue;
      var result = await requestUpstreamModels(endpoint, headers, timeoutMs);
      attempts.push({
        name: endpoint.name || '',
        url: endpoint.url,
        status: result.status || 0,
        ok: result.ok === true,
        error: safeTrim(result.error || ''),
        raw_count: safeNumber(result.rawCount, 0),
        codex_count: safeNumber(result.codexCount, 0),
      });

      if (!result.ok) continue;

      var discovered = mergeDiscoveredModels(result.models || []);
      if (!discovered.order.length) {
        continue;
      }

      updateUpstreamCacheSuccess(
        discovered,
        endpoint.url,
        cacheTtlMs,
        result.rawCount,
        result.codexCount,
        attempts
      );

      if (logger) {
        logger('模型动态发现成功', {
          source: endpoint.url,
          count: discovered.order.length,
          email: tokenInfo.email || '',
        });
      }

      return buildFetchResult(true, {
        from_cache: false,
        source: endpoint.url,
        models: discovered.order.slice(),
      });
    }

    var last = attempts.length > 0 ? attempts[attempts.length - 1] : null;
    var failError = (last && last.error) || 'upstream_models_unavailable';
    var failStatus = (last && last.status) || 0;
    updateUpstreamCacheFailure(failError, failStatus, attempts);
    if (logger) {
      logger('模型动态发现失败', {
        error: failError,
        status: failStatus,
      });
    }
    return buildFetchResult(false, {
      error: failError,
      status: failStatus,
      tried_endpoints: attempts,
    });
  })();

  try {
    return await _upstreamFetchPromise;
  } finally {
    _upstreamFetchPromise = null;
  }
}

/**
 * 解析模型名 → 上游实际模型名
 *
 * 处理顺序:
 * 1. 去掉 prefix（如 'codex/gpt-5-codex-mini' → 'gpt-5-codex-mini'）
 * 2. 查 aliases（如 'gpt-5-codex-latest' → 'gpt-5.3-codex'）
 * 3. 检查是否在 available 列表中
 * 4. 如果都不匹配，返回 default 模型
 *
 * @param {string} model - 客户端传入的模型名
 * @returns {{ resolved: string, original: string, found: boolean, reasoningEffort: string }}
 */
export function resolveModel(model) {
  var activeConfig = buildEffectiveConfig();
  if (!activeConfig) {
    var originalModel = model;
    var parsedModelOnly = parseReasoningEffortSuffix(originalModel);
    return {
      resolved: parsedModelOnly.model,
      original: originalModel,
      found: false,
      reasoningEffort: parsedModelOnly.reasoningEffort,
    };
  }

  var original = model || '';
  var prefix = activeConfig.prefix || '';
  var resolved = original;
  var reasoningEffort = '';

  // 1. 去掉 prefix
  if (prefix && resolved.startsWith(prefix)) {
    resolved = resolved.substring(prefix.length);
  }

  // 1.1 解析模型后缀括号语法，如 gpt-5.3-codex(xhigh)
  var parsedModel = parseReasoningEffortSuffix(resolved);
  resolved = parsedModel.model;
  reasoningEffort = parsedModel.reasoningEffort;

  // 2. 查 aliases
  var aliases = activeConfig.aliases || {};
  if (aliases[resolved]) {
    resolved = aliases[resolved];
  }

  // 3. 检查 available
  var available = activeConfig.available || {};
  if (available[resolved] && available[resolved].enabled !== false) {
    return {
      resolved: resolved,
      original: original,
      found: true,
      reasoningEffort: reasoningEffort,
    };
  }

  // 4. 如果传了空的或不认识的，用 default
  if (!resolved || !available[resolved]) {
    var def = activeConfig.default || Object.keys(available)[0] || 'gpt-5-codex-mini';
    return {
      resolved: def,
      original: original,
      found: !original,
      reasoningEffort: reasoningEffort,
    };
  }

  return {
    resolved: resolved,
    original: original,
    found: false,
    reasoningEffort: reasoningEffort,
  };
}

/**
 * 给模型名加 prefix
 */
export function addPrefix(model) {
  var activeConfig = buildEffectiveConfig();
  if (!activeConfig) return model;
  var prefix = activeConfig.prefix || '';
  if (prefix && !model.startsWith(prefix)) {
    return prefix + model;
  }
  return model;
}

/**
 * 列出所有可用模型（默认带 prefix）
 *
 * @param {{ includePrefix?: boolean }} [options]
 * @returns {Array<{ id: string, display_name: string }>}
 */
export function listModels(options) {
  var activeConfig = buildEffectiveConfig();
  if (!activeConfig) return [];
  var opts = options || {};
  var includePrefix = opts.includePrefix !== false;
  var available = activeConfig.available || {};
  var prefix = activeConfig.prefix || '';
  var result = [];

  function toModelId(name) {
    if (!includePrefix) return name;
    return prefix + name;
  }

  var keys = Object.keys(available);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var info = available[key];
    if (info.enabled === false) continue;
    result.push({
      id: toModelId(key),
      display_name: info.display_name || key,
    });
  }

  // 添加 aliases
  var aliases = activeConfig.aliases || {};
  var aliasKeys = Object.keys(aliases);
  for (var j = 0; j < aliasKeys.length; j++) {
    var aliasName = aliasKeys[j];
    var target = aliases[aliasName];
    if (available[target] && available[target].enabled !== false) {
      result.push({
        id: toModelId(aliasName),
        display_name: aliasName + ' → ' + target,
      });
    }
  }

  return result;
}

/**
 * 检查模型是否可用
 */
export function isModelAvailable(model) {
  return resolveModel(model).found;
}

/**
 * 获取默认模型名
 */
export function getDefaultModel() {
  var activeConfig = buildEffectiveConfig();
  if (!activeConfig) return 'gpt-5-codex-mini';
  return activeConfig.default || Object.keys(activeConfig.available || {})[0] || 'gpt-5-codex-mini';
}
