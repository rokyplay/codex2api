import crypto from 'node:crypto';
import { extractBearerToken } from './http-utils.mjs';

var DEFAULT_SOURCE_ORDER = ['authorization', 'x-api-key', 'query.key'];
var _discordUserStore = null;

export function setDiscordApiKeyUserStore(userStore) {
  if (userStore && typeof userStore.verifyApiKey === 'function') {
    _discordUserStore = userStore;
    return;
  }
  _discordUserStore = null;
}

function safeCompare(a, b) {
  var aBuf = Buffer.from(String(a || ''), 'utf8');
  var bBuf = Buffer.from(String(b || ''), 'utf8');
  if (aBuf.length !== bBuf.length) {
    var maxLen = Math.max(aBuf.length, bBuf.length, 1);
    var aPad = Buffer.alloc(maxLen);
    var bPad = Buffer.alloc(maxLen);
    aBuf.copy(aPad);
    bBuf.copy(bPad);
    try {
      crypto.timingSafeEqual(aPad, bPad);
    } catch (_) {}
    return false;
  }
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function isLocalRequest(req) {
  var remoteAddr = (req && req.socket && req.socket.remoteAddress) || '';
  return remoteAddr === '127.0.0.1'
    || remoteAddr === '::1'
    || remoteAddr === '::ffff:127.0.0.1';
}

function readQueryKey(req) {
  if (!req || !req.url) return '';
  try {
    var parsed = new URL(req.url, 'http://localhost');
    return parsed.searchParams.get('key') || '';
  } catch (_) {
    return '';
  }
}

function extractTokenMap(req) {
  var headers = (req && req.headers) || {};
  return {
    authorization: extractBearerToken(headers['authorization'] || ''),
    'x-api-key': String(headers['x-api-key'] || ''),
    'query.key': readQueryKey(req),
  };
}

function pickToken(tokenMap, sourceOrder) {
  for (var i = 0; i < sourceOrder.length; i++) {
    var source = sourceOrder[i];
    if (!Object.prototype.hasOwnProperty.call(tokenMap, source)) continue;
    if (tokenMap[source]) {
      return { token: tokenMap[source], source: source };
    }
  }
  return { token: '', source: '' };
}

function normalizeSourceOrder(sourceOrder) {
  if (!Array.isArray(sourceOrder) || sourceOrder.length === 0) {
    return DEFAULT_SOURCE_ORDER.slice();
  }
  var out = [];
  for (var i = 0; i < sourceOrder.length; i++) {
    var source = String(sourceOrder[i] || '').trim();
    if (!source) continue;
    if (DEFAULT_SOURCE_ORDER.indexOf(source) < 0) continue;
    if (out.indexOf(source) < 0) out.push(source);
  }
  if (out.length === 0) {
    return DEFAULT_SOURCE_ORDER.slice();
  }
  return out;
}

function normalizeApiKeys(config) {
  var server = (config && config.server) || {};
  var list = Array.isArray(server.api_keys) ? server.api_keys : [];
  var keys = [];
  for (var i = 0; i < list.length; i++) {
    var item = list[i];
    if (!item || typeof item !== 'object') continue;
    if (item.enabled === false) continue;
    var key = String(item.key || '');
    if (!key) continue;
    keys.push({
      id: item.id ? String(item.id) : '',
      identity: item.identity ? String(item.identity) : '',
      key: key,
    });
  }
  return keys;
}

function resolveDiscordApiKeyPrefix(config, options) {
  var opts = options || {};
  var directPrefix = String(opts.discordApiKeyPrefix || '').trim();
  if (directPrefix) return directPrefix;
  var discord = (config && config.discord_auth) || {};
  var fromConfig = String(discord.api_key_prefix || '').trim();
  if (fromConfig) return fromConfig;
  return 'dk-';
}

function getDiscordUserStore(options) {
  var opts = options || {};
  if (opts.discordUserStore && typeof opts.discordUserStore.verifyApiKey === 'function') {
    return opts.discordUserStore;
  }
  if (_discordUserStore && typeof _discordUserStore.verifyApiKey === 'function') {
    return _discordUserStore;
  }
  return null;
}

function extractDiscordUserIdFromVerifyResult(result) {
  var data = result;
  if (!data || typeof data !== 'object') return '';
  if (data.discord_user_id !== undefined) return String(data.discord_user_id || '');
  if (data.discordUserId !== undefined) return String(data.discordUserId || '');
  if (data.user_id !== undefined) return String(data.user_id || '');
  if (data.id !== undefined) return String(data.id || '');
  if (data.user && typeof data.user === 'object') {
    return extractDiscordUserIdFromVerifyResult(data.user);
  }
  return '';
}

function normalizeDiscordVerifyResult(rawResult) {
  if (!rawResult) {
    return { ok: false, discord_user_id: '', data: null };
  }
  if (typeof rawResult === 'boolean') {
    return { ok: rawResult === true, discord_user_id: '', data: null };
  }
  if (typeof rawResult !== 'object') {
    return { ok: false, discord_user_id: '', data: null };
  }

  var data = rawResult.user && typeof rawResult.user === 'object'
    ? rawResult.user
    : rawResult;
  var explicitInvalid = rawResult.valid === false || rawResult.ok === false;
  var explicitValid = rawResult.valid === true || rawResult.ok === true || rawResult.success === true;
  var discordUserId = extractDiscordUserIdFromVerifyResult(data) || extractDiscordUserIdFromVerifyResult(rawResult);
  var ok = explicitValid || (!!discordUserId && !explicitInvalid);
  return {
    ok: ok,
    discord_user_id: discordUserId,
    data: data,
  };
}

function updateDiscordUsageStats(userStore, discordUserId, tokenSource) {
  if (!userStore || !discordUserId) return;
  var nowIso = new Date().toISOString();

  // 首选显式 usage 记录函数；若不存在，回退到 createOrUpdateUser 的最小可兼容更新。
  if (typeof userStore.recordUsage === 'function') {
    try {
      var usageResult = userStore.recordUsage(discordUserId, {
        requests: 1,
        source: tokenSource || '',
        ts: nowIso,
      });
      if (usageResult && typeof usageResult.then === 'function') {
        usageResult.catch(function () {});
      }
      return;
    } catch (_) {}
  }

  if (typeof userStore.createOrUpdateUser === 'function') {
    try {
      var updateResult = userStore.createOrUpdateUser({
        discord_user_id: discordUserId,
        usage: {
          request_increment: 1,
          last_request_at: nowIso,
          last_request_source: tokenSource || '',
        },
      });
      if (updateResult && typeof updateResult.then === 'function') {
        updateResult.catch(function () {});
      }
    } catch (_) {}
  }
}

function buildFailure(status, reason, source) {
  return {
    ok: false,
    status: status,
    reason: reason,
    identity: null,
    key_id: null,
    source: source || null,
    is_legacy_password: false,
  };
}

/**
 * 统一 API Key 认证
 *
 * 返回:
 * { ok, status, reason, identity, key_id, source, is_legacy_password }
 */
export function authenticateApiKey(req, config, options) {
  var opts = options || {};
  var sourceOrder = normalizeSourceOrder(opts.sourceOrder);
  var allowLocalBypass = opts.allowLocalBypass === true;
  var server = (config && config.server) || {};
  var defaultIdentity = String(server.default_identity || '').trim();
  var legacyPassword = String(server.password || '');
  var apiKeys = normalizeApiKeys(config);
  var hasAuthConfig = apiKeys.length > 0 || !!legacyPassword;
  var localRequest = isLocalRequest(req);

  function buildLocalBypassResult(reason) {
    return {
      ok: true,
      status: 200,
      reason: reason,
      identity: defaultIdentity || 'local',
      key_id: 'local',
      source: 'local_bypass',
      is_legacy_password: false,
    };
  }

  var tokenMap = extractTokenMap(req);
  var picked = pickToken(tokenMap, sourceOrder);
  var token = picked.token;
  var source = picked.source;

  if (!token) {
    if (allowLocalBypass && localRequest) {
      return buildLocalBypassResult('local_bypass');
    }
    if (!hasAuthConfig) {
      return {
        ok: true,
        status: 200,
        reason: 'auth_not_configured',
        identity: 'anonymous',
        key_id: 'anonymous',
        source: null,
        is_legacy_password: false,
      };
    }
    return buildFailure(401, 'missing_api_key', null);
  }

  var discordPrefix = resolveDiscordApiKeyPrefix(config, opts);
  var discordStore = getDiscordUserStore(opts);
  if (discordPrefix && token.indexOf(discordPrefix) === 0 && discordStore && typeof discordStore.verifyApiKey === 'function') {
    var verifyResult = null;
    try {
      verifyResult = discordStore.verifyApiKey(token);
      if (verifyResult && typeof verifyResult.then === 'function') {
        return buildFailure(401, 'discord_verify_async_not_supported', source || null);
      }
    } catch (_) {
      return buildFailure(401, 'discord_verify_exception', source || null);
    }

    var normalized = normalizeDiscordVerifyResult(verifyResult);
    if (normalized.ok && normalized.discord_user_id) {
      var identity = 'discord:' + normalized.discord_user_id;
      if (req && typeof req === 'object') {
        req._apiKeyIdentity = identity;
        req._discord_user_id = normalized.discord_user_id;
      }
      updateDiscordUsageStats(discordStore, normalized.discord_user_id, source || 'authorization');
      return {
        ok: true,
        status: 200,
        reason: 'ok',
        identity: identity,
        key_id: 'discord_api_key',
        source: source || null,
        is_legacy_password: false,
      };
    }
  }

  for (var i = 0; i < apiKeys.length; i++) {
    var item = apiKeys[i];
    if (safeCompare(token, item.key)) {
      return {
        ok: true,
        status: 200,
        reason: 'ok',
        identity: item.identity || item.id || 'api_key',
        key_id: item.id || null,
        source: source || null,
        is_legacy_password: false,
      };
    }
  }

  if (legacyPassword && safeCompare(token, legacyPassword)) {
    return {
      ok: true,
      status: 200,
      reason: 'ok',
      identity: defaultIdentity || 'legacy_password',
      key_id: 'legacy_password',
      source: 'legacy_password',
      is_legacy_password: true,
    };
  }

  if (allowLocalBypass && localRequest) {
    return buildLocalBypassResult('local_bypass_no_key_match');
  }

  return buildFailure(401, 'invalid_api_key', source || null);
}
