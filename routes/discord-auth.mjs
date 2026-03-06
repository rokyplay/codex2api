import { readBody } from '../lib/http-utils.mjs';
import { log, C } from '../lib/utils.mjs';
import * as oauthClientModule from '../lib/discord/oauth-client.mjs';
import * as userStoreModule from '../lib/discord/user-store.mjs';
import * as sessionStoreModule from '../lib/discord/session-store.mjs';
import * as securityModule from '../lib/discord/security.mjs';

var TAG = '[discord-auth]';

function routeLog(level, color, message) {
  log(level, color, TAG + ' ' + message);
}

function maybeAwait(value) {
  if (value && typeof value.then === 'function') return value;
  return Promise.resolve(value);
}

function toObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
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

function normalizeConfig(input) {
  var wrapper = toObject(input);
  var config = wrapper.config ? toObject(wrapper.config) : wrapper;
  var stats = wrapper.stats || null;
  var discord = toObject(config.discord_auth);
  var turnstile = toObject(discord.turnstile);
  return {
    appConfig: config,
    stats: stats,
    discord: {
      enabled: discord.enabled === true,
      guild_id: String(discord.guild_id || ''),
      required_roles: normalizeRoleList(discord.required_roles),
      session_ttl_hours: Number(discord.session_ttl_hours) > 0 ? Number(discord.session_ttl_hours) : 48,
      state_ttl_seconds: Number(discord.state_ttl_seconds) > 0 ? Number(discord.state_ttl_seconds) : 300,
      api_key_prefix: String(discord.api_key_prefix || 'dk-'),
      turnstile: {
        enabled: turnstile.enabled === true,
      },
    },
  };
}

function getClientIp(req) {
  var headers = (req && req.headers) || {};
  var forwarded = String(headers['x-forwarded-for'] || '').trim();
  if (forwarded) {
    var first = forwarded.split(',')[0];
    if (first && first.trim()) return first.trim();
  }
  var realIp = String(headers['x-real-ip'] || '').trim();
  if (realIp) return realIp;
  if (req && req.socket && req.socket.remoteAddress) {
    return String(req.socket.remoteAddress);
  }
  return '';
}

function parseCookieMap(req) {
  var headers = (req && req.headers) || {};
  var cookie = String(headers.cookie || '');
  if (!cookie) return {};
  var items = cookie.split(';');
  var out = {};
  for (var i = 0; i < items.length; i++) {
    var raw = items[i];
    var idx = raw.indexOf('=');
    if (idx <= 0) continue;
    var key = raw.slice(0, idx).trim();
    if (!key) continue;
    var value = raw.slice(idx + 1).trim();
    try {
      out[key] = decodeURIComponent(value);
    } catch (_) {
      out[key] = value;
    }
  }
  return out;
}

function maskApiKey(rawKey) {
  var key = String(rawKey || '');
  if (!key) return '';
  if (key.length <= 8) return '****';
  return key.slice(0, 4) + '...' + key.slice(-4);
}

function htmlEscape(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function json(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function redirect(res, location, headers) {
  var merged = Object.assign({ Location: location }, headers || {});
  res.writeHead(302, merged);
  res.end();
}

function extractDiscordUserId(userObj) {
  var user = toObject(userObj);
  if (user.discord_user_id !== undefined) return String(user.discord_user_id || '');
  if (user.discordId !== undefined) return String(user.discordId || '');
  if (user.id !== undefined) return String(user.id || '');
  if (user.user && typeof user.user === 'object') return extractDiscordUserId(user.user);
  return '';
}

function toPositiveInt(value, fallback) {
  var n = Number(value);
  if (!isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function resolveSessionId(created) {
  if (!created) return '';
  if (typeof created === 'string') return created;
  if (typeof created !== 'object') return '';
  if (created.session_id !== undefined) return String(created.session_id || '');
  if (created.sessionId !== undefined) return String(created.sessionId || '');
  if (created.id !== undefined) return String(created.id || '');
  if (created.token !== undefined) return String(created.token || '');
  return '';
}

function resolveApiKeyValue(value) {
  if (!value) return '';
  if (typeof value === 'string') return value;
  if (typeof value !== 'object') return '';
  if (value.api_key !== undefined) return String(value.api_key || '');
  if (value.apiKey !== undefined) return String(value.apiKey || '');
  if (value.key !== undefined) return String(value.key || '');
  return '';
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

function extractTurnstileToken(body) {
  var payload = toObject(body);
  if (payload.token !== undefined) return String(payload.token || '');
  if (payload.turnstile_token !== undefined) return String(payload.turnstile_token || '');
  if (payload['cf-turnstile-response'] !== undefined) return String(payload['cf-turnstile-response'] || '');
  return '';
}

function normalizeVerifyStateResult(result) {
  if (result === true) return true;
  if (!result || typeof result !== 'object') return false;
  if (result.valid === true) return true;
  if (result.ok === true) return true;
  return false;
}

function normalizeTurnstileResult(result) {
  if (result === true) return { success: true, detail: null };
  if (result === false) return { success: false, detail: null };
  var data = toObject(result);
  if (data.success === true || data.ok === true || data.valid === true) {
    return { success: true, detail: data };
  }
  return { success: false, detail: data };
}

function buildUsageSummary(stats, identity) {
  if (!stats || !identity) {
    return {
      today: { requests: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, reasoning_tokens: 0, errors: 0 },
      total: { requests: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, reasoning_tokens: 0, errors: 0 },
    };
  }

  function pickBucket(rows) {
    if (!Array.isArray(rows)) return null;
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i] || {};
      if (String(row.identity || '') === identity) return row;
    }
    return null;
  }

  var now = new Date();
  var y = now.getFullYear();
  var m = String(now.getMonth() + 1).padStart(2, '0');
  var d = String(now.getDate()).padStart(2, '0');
  var today = y + '-' + m + '-' + d;

  var todayRows = typeof stats.getCallerStatsRange === 'function'
    ? stats.getCallerStatsRange(today, today)
    : [];
  var totalRows = typeof stats.getCallerStatsTotal === 'function'
    ? stats.getCallerStatsTotal()
    : [];

  var todayBucket = pickBucket(todayRows) || {};
  var totalBucket = pickBucket(totalRows) || {};

  function normalizeBucket(bucket) {
    var input = Number(bucket.input || 0);
    var output = Number(bucket.output || 0);
    var cached = Number(bucket.cached || 0);
    var reasoning = Number(bucket.reasoning || 0);
    return {
      requests: Number(bucket.requests || 0),
      input_tokens: input,
      output_tokens: output,
      cached_tokens: cached,
      reasoning_tokens: reasoning,
      total_tokens: input + output + cached + reasoning,
      errors: Number(bucket.errors || 0),
    };
  }

  return {
    today: normalizeBucket(todayBucket),
    total: normalizeBucket(totalBucket),
  };
}

function renderErrorPage(statusCode, title, message, hint) {
  return '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
    + '<title>' + htmlEscape(title) + '</title>'
    + '<style>body{font-family:system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#f5f7fb;color:#111827;padding:24px;}'
    + '.box{max-width:640px;margin:48px auto;background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:24px;}'
    + 'h1{margin:0 0 12px;font-size:22px;}p{line-height:1.65;margin:8px 0;}'
    + '.hint{margin-top:12px;color:#6b7280;font-size:14px;}</style></head><body><div class="box">'
    + '<h1>' + htmlEscape(title) + '</h1>'
    + '<p>' + htmlEscape(message) + '</p>'
    + (hint ? '<p class="hint">' + htmlEscape(hint) + '</p>' : '')
    + '</div></body></html>';
}

function sendErrorPage(res, statusCode, title, message, hint) {
  res.writeHead(statusCode, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(renderErrorPage(statusCode, title, message, hint));
}

function createOAuthAdapter(discordConfig) {
  if (typeof oauthClientModule.createDiscordOAuthClient === 'function') {
    return oauthClientModule.createDiscordOAuthClient(discordConfig);
  }
  return oauthClientModule;
}

function createUserStoreAdapter(discordConfig) {
  if (typeof userStoreModule.createDiscordUserStore === 'function') {
    return userStoreModule.createDiscordUserStore(discordConfig);
  }
  return userStoreModule;
}

function createSessionStoreAdapter(discordConfig) {
  if (typeof sessionStoreModule.createDiscordSessionStore === 'function') {
    return sessionStoreModule.createDiscordSessionStore(discordConfig);
  }
  return sessionStoreModule;
}

function createSecurityAdapter(discordConfig) {
  if (typeof securityModule.createDiscordSecurity === 'function') {
    return securityModule.createDiscordSecurity(discordConfig);
  }
  return securityModule;
}

export function createDiscordAuthRoutes(config) {
  var normalized = normalizeConfig(config);
  var appConfig = normalized.appConfig;
  var discordConfig = normalized.discord;
  var stats = normalized.stats;
  var discordModuleConfig = toObject(appConfig.discord_auth);
  var oauthAdapter = createOAuthAdapter(discordModuleConfig);
  var userStore = createUserStoreAdapter(discordModuleConfig);
  var sessionStore = createSessionStoreAdapter(discordModuleConfig);
  var security = createSecurityAdapter(discordModuleConfig);

  var initialized = false;
  var initPromise = null;
  var lastSessionCleanupAt = 0;

  async function ensureInit() {
    if (initialized) return;
    if (initPromise) return initPromise;

    initPromise = (async function () {
      if (typeof oauthAdapter.init === 'function') {
        await maybeAwait(oauthAdapter.init(appConfig));
      } else if (typeof oauthClientModule.init === 'function') {
        await maybeAwait(oauthClientModule.init(appConfig));
      }

      if (typeof userStore.init === 'function') {
        await maybeAwait(userStore.init(appConfig));
      }
      if (typeof sessionStore.init === 'function') {
        await maybeAwait(sessionStore.init(appConfig));
      }
      if (typeof security.init === 'function') {
        await maybeAwait(security.init(appConfig));
      }

      initialized = true;
      routeLog('✅', C.green, 'Discord 路由模块初始化完成');
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
    if (!sessionId) return null;
    if (typeof sessionStore.getSession !== 'function') return null;

    var session = await maybeAwait(sessionStore.getSession(sessionId));
    if (!session) return null;

    var sessionObj = toObject(session);
    var now = Date.now();
    var expiresAt = Number(sessionObj.expires_at || sessionObj.expiresAt || 0);
    if (expiresAt > 0 && now > expiresAt) {
      if (typeof sessionStore.destroySession === 'function') {
        await maybeAwait(sessionStore.destroySession(sessionId));
      }
      return null;
    }

    var discordUserId = extractDiscordUserId(sessionObj);
    if (!discordUserId) {
      discordUserId = extractDiscordUserId(sessionObj.user || {});
    }
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

  function buildSessionCookie(sessionId) {
    var ttlSeconds = toPositiveInt(discordConfig.session_ttl_hours, 48) * 3600;
    return 'session_id=' + encodeURIComponent(sessionId)
      + '; HttpOnly; Secure; SameSite=Lax; Max-Age=' + ttlSeconds + '; Path=/';
  }

  function buildClearSessionCookie() {
    return 'session_id=; HttpOnly; Secure; SameSite=Lax; Max-Age=0; Path=/';
  }

  async function handleStartAuth(req, res) {
    await ensureInit();
    if (!discordConfig.enabled) {
      return json(res, 404, { success: false, error: 'discord_auth_disabled' });
    }

    if (typeof security.generateState !== 'function') {
      return json(res, 500, { success: false, error: 'state_generator_missing' });
    }
    if (typeof oauthAdapter.getAuthorizeUrl !== 'function') {
      return json(res, 500, { success: false, error: 'oauth_client_missing' });
    }

    var stateResult = await maybeAwait(security.generateState({
      ttl_seconds: discordConfig.state_ttl_seconds,
      ip: getClientIp(req),
    }));
    var state = typeof stateResult === 'string'
      ? stateResult
      : String((stateResult && stateResult.state) || '');

    if (!state) {
      return json(res, 500, { success: false, error: 'state_generate_failed' });
    }

    var authorizeUrl = oauthAdapter.getAuthorizeUrl(state);
    routeLog('🔐', C.cyan, '发起 OAuth 授权跳转');
    redirect(res, authorizeUrl);
  }

  async function verifyStateValue(req, state) {
    if (typeof security.verifyState !== 'function') return false;
    var result = await maybeAwait(security.verifyState(state, {
      ip: getClientIp(req),
      ttl_seconds: discordConfig.state_ttl_seconds,
    }));
    return normalizeVerifyStateResult(result);
  }

  async function loadOrCreateUser(discordUser, memberInfo, tokenResp, req) {
    var discordUserId = extractDiscordUserId(discordUser);
    var profile = {
      discord_user_id: discordUserId,
      username: String(discordUser.username || ''),
      global_name: String(discordUser.global_name || ''),
      discriminator: String(discordUser.discriminator || ''),
      avatar: String(discordUser.avatar || ''),
      guild_id: discordConfig.guild_id,
      roles: normalizeRoleList(memberInfo.roles || []),
      last_login_at: new Date().toISOString(),
      last_login_ip: getClientIp(req),
      token_expires_in: tokenResp && tokenResp.expires_in ? Number(tokenResp.expires_in) : 0,
    };

    var savedUser = null;
    if (typeof userStore.createOrUpdateUser === 'function') {
      savedUser = await maybeAwait(userStore.createOrUpdateUser(profile));
    }
    if (!savedUser && typeof userStore.findByDiscordId === 'function') {
      savedUser = await maybeAwait(userStore.findByDiscordId(discordUserId));
    }
    return savedUser || profile;
  }

  async function ensureApiKeyExists(discordUserId, userRecord) {
    if (userHasApiKey(userRecord)) return { generated: false, value: '' };
    if (typeof userStore.generateApiKey !== 'function') return { generated: false, value: '' };
    var result = await maybeAwait(userStore.generateApiKey(discordUserId));
    var apiKey = resolveApiKeyValue(result);
    return { generated: !!apiKey, value: apiKey };
  }

  async function createSessionCompat(discordUserId, req, ttlSeconds) {
    if (typeof sessionStore.createSession !== 'function') {
      return '';
    }
    var clientIp = getClientIp(req);
    var userAgent = String((req && req.headers && req.headers['user-agent']) || '');
    var createdSession = null;

    try {
      createdSession = await maybeAwait(sessionStore.createSession(discordUserId, clientIp, userAgent, ttlSeconds));
    } catch (_) {}

    var sessionId = resolveSessionId(createdSession);
    if (sessionId) return sessionId;

    try {
      createdSession = await maybeAwait(sessionStore.createSession({
        discord_user_id: discordUserId,
        user: {
          discord_user_id: discordUserId,
        },
        ip: clientIp,
        user_agent: userAgent,
      }, ttlSeconds));
    } catch (_) {
      return '';
    }

    return resolveSessionId(createdSession);
  }

  async function handleAuthCallback(req, res) {
    await ensureInit();

    var parsedUrl = new URL(req.url, 'http://localhost');
    var code = String(parsedUrl.searchParams.get('code') || '').trim();
    var state = String(parsedUrl.searchParams.get('state') || '').trim();

    if (!code || !state) {
      return sendErrorPage(res, 400, '登录失败', '缺少 code 或 state 参数');
    }

    var stateOk = await verifyStateValue(req, state);
    if (!stateOk) {
      return sendErrorPage(res, 400, '登录失败', 'state 校验失败，请重新登录');
    }

    if (typeof oauthAdapter.exchangeCode !== 'function'
      || typeof oauthAdapter.getUser !== 'function'
      || typeof oauthAdapter.getGuildMember !== 'function') {
      return sendErrorPage(res, 500, '登录失败', 'OAuth 客户端能力不足');
    }

    try {
      var tokenResp = await maybeAwait(oauthAdapter.exchangeCode(code));
      var accessToken = String((tokenResp && tokenResp.access_token) || '');
      if (!accessToken) {
        return sendErrorPage(res, 502, '登录失败', 'Discord 未返回 access_token');
      }

      var discordUser = await maybeAwait(oauthAdapter.getUser(accessToken));
      var discordUserId = extractDiscordUserId(discordUser);
      if (!discordUserId) {
        return sendErrorPage(res, 502, '登录失败', '无法获取 Discord 用户 ID');
      }

      var memberInfo = await maybeAwait(oauthAdapter.getGuildMember(accessToken, discordConfig.guild_id));
      var roles = normalizeRoleList(memberInfo && memberInfo.roles);
      var requiredRoles = discordConfig.required_roles;
      var roleMatched = requiredRoles.length === 0;
      for (var i = 0; i < requiredRoles.length; i++) {
        if (roles.indexOf(requiredRoles[i]) >= 0) {
          roleMatched = true;
          break;
        }
      }

      if (!roleMatched) {
        return sendErrorPage(
          res,
          403,
          '访问被拒绝',
          '你需要在 Discord 服务器中拥有指定身份组',
          '请联系管理员确认你的身份组权限'
        );
      }

      var userRecord = await loadOrCreateUser(discordUser, memberInfo, tokenResp, req);
      var apiKeyResult = await ensureApiKeyExists(discordUserId, userRecord);
      if (apiKeyResult.generated) {
        routeLog('🔑', C.green, '用户首次登录，自动生成 API key: ' + maskApiKey(apiKeyResult.value));
      }

      if (typeof sessionStore.createSession !== 'function') {
        return sendErrorPage(res, 500, '登录失败', 'sessionStore.createSession 不可用');
      }

      var ttlSeconds = toPositiveInt(discordConfig.session_ttl_hours, 48) * 3600;
      var sessionId = await createSessionCompat(discordUserId, req, ttlSeconds);
      if (!sessionId) {
        return sendErrorPage(res, 500, '登录失败', '会话创建失败');
      }

      routeLog('✅', C.green, '用户登录成功: discord=' + discordUserId);
      redirect(res, '/portal/', {
        'Set-Cookie': buildSessionCookie(sessionId),
      });
    } catch (err) {
      routeLog('❌', C.red, 'OAuth 回调处理失败: ' + err.message);
      sendErrorPage(res, 500, '登录失败', '处理 Discord 授权回调时发生错误', '请稍后重试，或联系管理员排查日志');
    }
  }

  async function handleTurnstileVerify(req, res) {
    await ensureInit();
    if (!discordConfig.turnstile.enabled) {
      return json(res, 200, { success: true, skipped: true, reason: 'turnstile_disabled' });
    }
    if (typeof security.verifyTurnstile !== 'function') {
      return json(res, 500, { success: false, error: 'verifyTurnstile_not_implemented' });
    }

    var body;
    try {
      body = await readBody(req);
    } catch (err) {
      return json(res, 400, { success: false, error: 'invalid_request_body' });
    }

    var token = extractTurnstileToken(body);
    if (!token) {
      return json(res, 400, { success: false, error: 'missing_turnstile_token' });
    }

    try {
      var verifyResult = await maybeAwait(security.verifyTurnstile(token, getClientIp(req)));
      var normalizedResult = normalizeTurnstileResult(verifyResult);
      if (!normalizedResult.success) {
        return json(res, 403, {
          success: false,
          error: 'turnstile_verify_failed',
          detail: normalizedResult.detail,
        });
      }
      return json(res, 200, { success: true });
    } catch (err) {
      routeLog('❌', C.red, 'Turnstile 校验异常: ' + err.message);
      return json(res, 500, { success: false, error: 'turnstile_verify_exception' });
    }
  }

  async function handleProfile(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;

    var discordUserId = current.discord_user_id;
    var user = null;
    if (discordUserId && typeof userStore.findByDiscordId === 'function') {
      user = await maybeAwait(userStore.findByDiscordId(discordUserId));
    }
    user = toObject(user);

    var identity = discordUserId ? ('discord:' + discordUserId) : '';
    var usage = buildUsageSummary(stats, identity);

    var rawApiKey = String(user.api_key || user.apiKey || '');
    var maskedApiKey = String(user.api_key_masked || user.apiKeyMasked || maskApiKey(rawApiKey));
    if (!maskedApiKey && user.api_key_last4) {
      maskedApiKey = String(discordConfig.api_key_prefix || 'dk-') + '***' + String(user.api_key_last4);
    }

    return json(res, 200, {
      success: true,
      data: {
        user: {
          discord_user_id: discordUserId || '',
          seq_id: String(user.seq_id || ''),
          username: String(user.username || ''),
          global_name: String(user.global_name || ''),
          avatar: String(user.avatar || ''),
          guild_id: String(user.guild_id || discordConfig.guild_id || ''),
          roles: normalizeRoleList(user.roles || []),
          is_banned: user.is_banned === true,
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

  async function handleRotateApiKey(req, res) {
    var current = await requireSession(req, res);
    if (!current) return;
    var discordUserId = current.discord_user_id;
    if (!discordUserId) {
      return json(res, 400, { success: false, error: 'invalid_session_user' });
    }
    if (typeof userStore.rotateApiKey !== 'function') {
      return json(res, 500, { success: false, error: 'rotate_api_key_not_implemented' });
    }

    try {
      var rotateResult = await maybeAwait(userStore.rotateApiKey(discordUserId));
      var newApiKey = resolveApiKeyValue(rotateResult);
      if (!newApiKey) {
        return json(res, 500, { success: false, error: 'rotate_api_key_failed' });
      }
      routeLog('🔁', C.cyan, '用户轮换 API key: discord=' + discordUserId);
      return json(res, 200, {
        success: true,
        data: {
          api_key: newApiKey,
          masked: maskApiKey(newApiKey),
          one_time_visible: true,
        },
      });
    } catch (err) {
      routeLog('❌', C.red, '轮换 API key 失败: ' + err.message);
      return json(res, 500, { success: false, error: 'rotate_api_key_exception' });
    }
  }

  async function handleLogout(req, res) {
    await ensureInit();
    var current = await getSessionFromCookie(req);
    if (current && current.session_id && typeof sessionStore.destroySession === 'function') {
      try {
        await maybeAwait(sessionStore.destroySession(current.session_id));
      } catch (err) {
        routeLog('⚠️', C.yellow, '销毁 session 失败: ' + err.message);
      }
    }
    redirect(res, '/login', {
      'Set-Cookie': buildClearSessionCookie(),
    });
  }

  async function handleRequest(req, res, pathname, method) {
    var path = String(pathname || '');
    var m = String(method || req.method || '').toUpperCase();

    if (m === 'GET' && path === '/auth/discord') {
      await handleStartAuth(req, res);
      return true;
    }
    if (m === 'GET' && path === '/auth/discord/callback') {
      await handleAuthCallback(req, res);
      return true;
    }
    if (m === 'POST' && path === '/auth/turnstile/verify') {
      await handleTurnstileVerify(req, res);
      return true;
    }
    if (m === 'GET' && path === '/auth/profile') {
      await handleProfile(req, res);
      return true;
    }
    if (m === 'POST' && path === '/auth/api-key/rotate') {
      await handleRotateApiKey(req, res);
      return true;
    }
    if (m === 'POST' && path === '/auth/logout') {
      await handleLogout(req, res);
      return true;
    }
    return false;
  }

  return {
    handleRequest: handleRequest,
    getSessionFromCookie: getSessionFromCookie,
    userStore: userStore,
    sessionStore: sessionStore,
  };
}
