import crypto from 'node:crypto';
import { log, C } from '../utils.mjs';

var TURNSTILE_VERIFY_URL = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';
var TURNSTILE_TIMEOUT_MS = 10000;
var DEFAULT_STATE_TTL_SECONDS = 300;

// 进程级随机 HMAC 密钥（重启后失效，符合预期）
var STATE_HMAC_SECRET = crypto.randomBytes(32);

function normalizePositiveInt(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function signStatePayload(payload) {
  return crypto
    .createHmac('sha256', STATE_HMAC_SECRET)
    .update(String(payload || ''), 'utf8')
    .digest('hex');
}

function safeHexEqual(a, b) {
  var aBuf = Buffer.from(String(a || ''), 'utf8');
  var bBuf = Buffer.from(String(b || ''), 'utf8');
  if (aBuf.length !== bBuf.length) {
    var len = Math.max(aBuf.length, bBuf.length, 1);
    var aPad = Buffer.alloc(len);
    var bPad = Buffer.alloc(len);
    aBuf.copy(aPad);
    bBuf.copy(bPad);
    try {
      crypto.timingSafeEqual(aPad, bPad);
    } catch (_) {}
    return false;
  }
  return crypto.timingSafeEqual(aBuf, bBuf);
}

export function createDiscordSecurity(config) {
  var cfg = config || {};
  var stateTtlSeconds = normalizePositiveInt(cfg.state_ttl_seconds, DEFAULT_STATE_TTL_SECONDS);
  var turnstile = (cfg.turnstile && typeof cfg.turnstile === 'object') ? cfg.turnstile : {};
  var turnstileEnabled = turnstile.enabled === true;
  var turnstileSecretKey = String(turnstile.secret_key || '');
  var issuedStateNonceMap = new Map();
  var maxStateNonceEntries = 10000;

  log('🔐', C.cyan, '[discord-security] 初始化完成: state_ttl_seconds=' + stateTtlSeconds + ', turnstile_enabled=' + turnstileEnabled);

  function cleanupIssuedStateNonce(nowTs) {
    var now = Number(nowTs);
    if (!Number.isFinite(now) || now <= 0) {
      now = Math.floor(Date.now() / 1000);
    }
    for (var entry of issuedStateNonceMap.entries()) {
      var nonce = entry[0];
      var expiresAt = Number(entry[1] || 0);
      if (!Number.isFinite(expiresAt) || expiresAt <= now) {
        issuedStateNonceMap.delete(nonce);
      }
    }
  }

  function registerStateNonce(nonce, ts) {
    nonce = String(nonce || '').trim();
    if (!nonce) return;
    var expiresAt = Number(ts) + stateTtlSeconds;
    issuedStateNonceMap.set(nonce, expiresAt);
    if (issuedStateNonceMap.size <= maxStateNonceEntries) return;

    var overflow = issuedStateNonceMap.size - maxStateNonceEntries;
    var removed = 0;
    for (var key of issuedStateNonceMap.keys()) {
      issuedStateNonceMap.delete(key);
      removed += 1;
      if (removed >= overflow) break;
    }
  }

  function consumeStateNonce(nonce, nowTs) {
    nonce = String(nonce || '').trim();
    if (!nonce) return false;
    var expiresAt = Number(issuedStateNonceMap.get(nonce) || 0);
    if (!Number.isFinite(expiresAt) || expiresAt <= 0) {
      return false;
    }
    issuedStateNonceMap.delete(nonce);
    return expiresAt > nowTs;
  }

  function generateState() {
    var nonce = crypto.randomBytes(32).toString('hex');
    var ts = Math.floor(Date.now() / 1000);
    var payload = nonce + '.' + ts;
    var sig = signStatePayload(payload);
    var state = payload + '.' + sig;

    cleanupIssuedStateNonce(ts);
    registerStateNonce(nonce, ts);

    log('🧷', C.cyan, '[discord-security] 生成 state: ts=' + ts + ', nonce_prefix=' + nonce.slice(0, 12));
    return state;
  }

  function verifyState(state) {
    state = String(state || '').trim();
    if (!state) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: state 为空');
      return false;
    }

    var parts = state.split('.');
    if (parts.length !== 3) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: 格式非法');
      return false;
    }

    var nonce = parts[0];
    var tsText = parts[1];
    var gotSig = parts[2];
    if (!nonce || !tsText || !gotSig) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: 字段缺失');
      return false;
    }

    var ts = Number(tsText);
    if (!Number.isFinite(ts) || ts <= 0) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: 时间戳非法');
      return false;
    }

    var payload = nonce + '.' + tsText;
    var expectedSig = signStatePayload(payload);
    if (!safeHexEqual(gotSig, expectedSig)) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: 签名不匹配');
      return false;
    }

    var nowTs = Math.floor(Date.now() / 1000);
    var age = nowTs - ts;
    if (age < -30) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: 时间戳超前 age=' + age);
      return false;
    }
    if (age > stateTtlSeconds) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: state 过期 age=' + age + 's');
      return false;
    }

    cleanupIssuedStateNonce(nowTs);
    if (!consumeStateNonce(nonce, nowTs)) {
      log('🚫', C.yellow, '[discord-security] verifyState 失败: state 未签发或已消费');
      return false;
    }

    log('✅', C.green, '[discord-security] verifyState 成功: age=' + age + 's');
    return true;
  }

  async function verifyTurnstile(token, ip) {
    token = String(token || '').trim();
    ip = String(ip || '').trim();

    if (!turnstileEnabled) {
      log('ℹ️', C.blue, '[discord-security] Turnstile 未启用，跳过验证');
      return {
        success: true,
        skipped: true,
      };
    }

    if (!turnstileSecretKey) {
      log('⚠️', C.yellow, '[discord-security] Turnstile 已启用但 secret_key 缺失');
      return {
        success: false,
        error: 'turnstile_secret_missing',
      };
    }

    if (!token) {
      log('🚫', C.yellow, '[discord-security] Turnstile token 缺失');
      return {
        success: false,
        error: 'turnstile_token_missing',
      };
    }

    var body = new URLSearchParams();
    body.set('secret', turnstileSecretKey);
    body.set('response', token);
    if (ip) {
      body.set('remoteip', ip);
    }

    log('🌐', C.cyan, '[discord-security] 请求 Turnstile 验证: ip=' + (ip || '(empty)'));

    try {
      var resp = await fetch(TURNSTILE_VERIFY_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: body,
        signal: AbortSignal.timeout(TURNSTILE_TIMEOUT_MS),
      });
      var text = await resp.text();
      var data = {};
      try {
        data = JSON.parse(text || '{}');
      } catch (_) {
        data = { success: false, error: 'turnstile_invalid_json', raw: text };
      }

      log('📨', C.cyan, '[discord-security] Turnstile 响应: http=' + resp.status + ', success=' + !!data.success + ', error-codes=' + JSON.stringify(data['error-codes'] || []));

      if (!resp.ok) {
        return {
          success: false,
          error: 'turnstile_http_' + resp.status,
          details: data,
        };
      }

      return data;
    } catch (e) {
      log('⚠️', C.yellow, '[discord-security] Turnstile 请求失败: ' + e.message);
      return {
        success: false,
        error: 'turnstile_request_failed',
        message: e.message,
      };
    }
  }

  return {
    generateState: generateState,
    verifyState: verifyState,
    verifyTurnstile: verifyTurnstile,
  };
}
