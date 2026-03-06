import { log, C } from '../utils.mjs';

var DISCORD_BASE = 'https://discord.com';
var DISCORD_API_BASE = 'https://discord.com/api/v10';
var DEFAULT_TIMEOUT_MS = 15000;

function normalizeConfig(config) {
  var cfg = config || {};
  return {
    enabled: cfg.enabled !== false,
    client_id: String(cfg.client_id || ''),
    client_secret: String(cfg.client_secret || ''),
    redirect_uri: String(cfg.redirect_uri || ''),
    scopes: String(cfg.scopes || 'identify'),
    guild_id: String(cfg.guild_id || ''),
  };
}

function shorten(text, maxLen) {
  text = String(text || '');
  maxLen = maxLen || 240;
  if (text.length <= maxLen) return text;
  return text.slice(0, maxLen) + '...';
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_) {
    return null;
  }
}

function redactSensitiveObject(value) {
  if (Array.isArray(value)) {
    var arr = [];
    for (var i = 0; i < value.length; i++) {
      arr.push(redactSensitiveObject(value[i]));
    }
    return arr;
  }
  if (!value || typeof value !== 'object') {
    return value;
  }
  var out = {};
  var keys = Object.keys(value);
  for (var j = 0; j < keys.length; j++) {
    var key = keys[j];
    var lowerKey = key.toLowerCase();
    if (lowerKey === 'access_token'
      || lowerKey === 'refresh_token'
      || lowerKey === 'id_token'
      || lowerKey === 'token'
      || lowerKey === 'client_secret'
      || lowerKey === 'authorization') {
      out[key] = '[REDACTED]';
      continue;
    }
    out[key] = redactSensitiveObject(value[key]);
  }
  return out;
}

function sanitizeBodyForLog(text, maxLen) {
  var parsed = safeJsonParse(text);
  if (parsed !== null) {
    return shorten(JSON.stringify(redactSensitiveObject(parsed)), maxLen || 400);
  }

  var raw = String(text || '');
  if (!raw) return '';
  var masked = raw
    .replace(/(access_token=)[^&\s]+/gi, '$1[REDACTED]')
    .replace(/(refresh_token=)[^&\s]+/gi, '$1[REDACTED]')
    .replace(/(client_secret=)[^&\s]+/gi, '$1[REDACTED]')
    .replace(/(authorization:\s*bearer\s+)[^\s]+/gi, '$1[REDACTED]');
  return shorten(masked, maxLen || 400);
}

function maskToken(token) {
  token = String(token || '');
  if (!token) return '(empty)';
  if (token.length <= 10) return token;
  return token.slice(0, 6) + '...' + token.slice(-4);
}

function ensureEnabled(cfg) {
  if (!cfg.enabled) {
    throw new Error('Discord OAuth 已禁用');
  }
}

async function requestJson(url, options, actionName) {
  log('🌐', C.cyan, '[discord-oauth] ' + actionName + ' request: ' + options.method + ' ' + url);
  var resp = await fetch(url, Object.assign({}, options, {
    signal: AbortSignal.timeout(DEFAULT_TIMEOUT_MS),
  }));
  var text = await resp.text();
  var data = safeJsonParse(text);
  var safeBody = sanitizeBodyForLog(text, 400);
  log('📨', C.cyan, '[discord-oauth] ' + actionName + ' response: status=' + resp.status + ' body=' + safeBody);
  if (!resp.ok) {
    throw new Error('Discord API ' + actionName + ' 失败: HTTP ' + resp.status + ', body=' + sanitizeBodyForLog(text, 300));
  }
  if (data === null && text && text.trim()) {
    throw new Error('Discord API ' + actionName + ' 返回非 JSON: ' + sanitizeBodyForLog(text, 200));
  }
  return data || {};
}

export function createDiscordOAuthClient(config) {
  var cfg = normalizeConfig(config);

  function getAuthorizeUrl(state) {
    ensureEnabled(cfg);
    var url = new URL(DISCORD_BASE + '/oauth2/authorize');
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('client_id', cfg.client_id);
    url.searchParams.set('redirect_uri', cfg.redirect_uri);
    url.searchParams.set('scope', cfg.scopes);
    if (state) {
      url.searchParams.set('state', String(state));
    }
    log('🔐', C.cyan, '[discord-oauth] 生成授权链接: state=' + shorten(state, 40));
    return url.toString();
  }

  async function exchangeCode(code) {
    ensureEnabled(cfg);
    code = String(code || '').trim();
    if (!code) {
      throw new Error('exchangeCode 缺少 code');
    }

    var body = new URLSearchParams();
    body.set('client_id', cfg.client_id);
    body.set('client_secret', cfg.client_secret);
    body.set('grant_type', 'authorization_code');
    body.set('code', code);
    body.set('redirect_uri', cfg.redirect_uri);

    return requestJson(DISCORD_API_BASE + '/oauth2/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: body,
    }, 'exchangeCode');
  }

  async function getUser(accessToken) {
    ensureEnabled(cfg);
    accessToken = String(accessToken || '');
    if (!accessToken) {
      throw new Error('getUser 缺少 accessToken');
    }
    log('🧾', C.cyan, '[discord-oauth] 获取用户信息: token=' + maskToken(accessToken));
    return requestJson(DISCORD_API_BASE + '/users/@me', {
      method: 'GET',
      headers: {
        Authorization: 'Bearer ' + accessToken,
      },
    }, 'getUser');
  }

  async function getGuildMember(accessToken, guildId) {
    ensureEnabled(cfg);
    accessToken = String(accessToken || '');
    guildId = String(guildId || cfg.guild_id || '').trim();
    if (!accessToken) {
      throw new Error('getGuildMember 缺少 accessToken');
    }
    if (!guildId) {
      throw new Error('getGuildMember 缺少 guildId');
    }
    log('🏠', C.cyan, '[discord-oauth] 获取 Guild 成员信息: guild_id=' + guildId + ', token=' + maskToken(accessToken));
    return requestJson(DISCORD_API_BASE + '/users/@me/guilds/' + encodeURIComponent(guildId) + '/member', {
      method: 'GET',
      headers: {
        Authorization: 'Bearer ' + accessToken,
      },
    }, 'getGuildMember');
  }

  async function revokeToken(token) {
    ensureEnabled(cfg);
    token = String(token || '').trim();
    if (!token) {
      throw new Error('revokeToken 缺少 token');
    }

    var body = new URLSearchParams();
    body.set('client_id', cfg.client_id);
    body.set('client_secret', cfg.client_secret);
    body.set('token', token);
    body.set('token_type_hint', 'access_token');

    log('🧹', C.cyan, '[discord-oauth] 撤销 token: token=' + maskToken(token));

    return requestJson(DISCORD_API_BASE + '/oauth2/token/revoke', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: body,
    }, 'revokeToken');
  }

  return {
    getAuthorizeUrl: getAuthorizeUrl,
    exchangeCode: exchangeCode,
    getUser: getUser,
    getGuildMember: getGuildMember,
    revokeToken: revokeToken,
  };
}
