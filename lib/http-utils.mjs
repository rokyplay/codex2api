/**
 * 共享 HTTP 工具函数
 *
 * - readBody: 读取请求体并解析 JSON（带大小限制）
 * - extractBearerToken: 从 Authorization header 严格提取 Bearer token
 * - resolveUpstreamTimeout: 统一上游请求超时解析（流式/非流式）
 * - resolveRetryPolicy: 统一重试与超时策略解析
 * - createTimeoutError / isTimeoutError: 统一超时错误对象
 * - isNetworkError / isRetryableError: 统一可重试判定
 * - pickRetryAccount: 重试时选择不同账号
 * - derivePromptCacheSessionId: 统一 prompt cache 的 session_id 派生
 */
import crypto from 'node:crypto';

function toPositiveInt(value, fallback) {
  var num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return fallback;
  return Math.floor(num);
}

/**
 * 读取 HTTP 请求体并解析 JSON
 * @param {IncomingMessage} req
 * @param {number} [maxBytes=10485760] - 最大字节数（默认 10MB）
 */
export function readBody(req, maxBytes) {
  maxBytes = maxBytes || 10 * 1024 * 1024;
  return new Promise(function (resolve, reject) {
    var chunks = [];
    var totalBytes = 0;
    req.on('data', function (chunk) {
      totalBytes += chunk.length;
      if (totalBytes > maxBytes) {
        req.destroy();
        reject(new Error('Request body too large'));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', function () {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

/**
 * 从 Authorization header 严格提取 Bearer token
 */
export function extractBearerToken(authHeader) {
  if (!authHeader) return '';
  var parts = authHeader.split(' ');
  if (parts.length === 2 && parts[0].toLowerCase() === 'bearer') {
    return parts[1];
  }
  return '';
}

/**
 * 统一解析上游请求超时（毫秒）
 *
 * 配置优先级:
 *   1) upstream.request_timeout.stream_ms / non_stream_ms / default_ms
 *   2) upstream.stream_timeout / upstream.timeout
 *   3) 兜底 30000ms
 */
export function resolveUpstreamTimeout(upstreamConfig, isStream) {
  var upstream = upstreamConfig || {};
  var requestTimeout = upstream.request_timeout || {};
  var streamMs = requestTimeout.stream_ms || requestTimeout.default_ms || upstream.stream_timeout || upstream.timeout || 120000;
  var nonStreamMs = requestTimeout.non_stream_ms || requestTimeout.default_ms || upstream.timeout || 120000;
  var raw = isStream ? streamMs : nonStreamMs;
  var timeoutMs = Number(raw);
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return 120000;
  }
  return Math.floor(timeoutMs);
}

/**
 * 统一解析重试与超时策略
 *
 * 优先级:
 *   1) retry.max_retries / retry.first_byte_timeout_ms / retry.idle_timeout_ms / retry.total_timeout_ms
 *   2) upstream.request_timeout.* / upstream.stream_timeout / upstream.timeout
 *   3) 默认值: 3 / 30000 / 60000 / 120000
 */
export function resolveRetryPolicy(config, isStream) {
  var cfg = config || {};
  var retry = cfg.retry || {};
  var upstream = cfg.upstream || {};
  var requestTimeout = upstream.request_timeout || {};

  var maxRetries = toPositiveInt(retry.max_retries, 3);
  var firstByteTimeoutMs = toPositiveInt(retry.first_byte_timeout_ms, 30000);
  var idleTimeoutMs = toPositiveInt(retry.idle_timeout_ms, 60000);

  var fallbackTotal = isStream
    ? (requestTimeout.stream_ms || requestTimeout.default_ms || upstream.stream_timeout || upstream.timeout || 120000)
    : (requestTimeout.non_stream_ms || requestTimeout.default_ms || upstream.timeout || 120000);
  var totalTimeoutMs = toPositiveInt(retry.total_timeout_ms, toPositiveInt(fallbackTotal, 120000));

  return {
    max_retries: maxRetries,
    first_byte_timeout_ms: firstByteTimeoutMs,
    idle_timeout_ms: idleTimeoutMs,
    total_timeout_ms: totalTimeoutMs,
  };
}

export function createTimeoutError(kind, timeoutMs, message) {
  var stage = kind || 'timeout';
  var ms = toPositiveInt(timeoutMs, 0);
  var readable = message || ('upstream_' + stage + '_after_' + ms + 'ms');
  var err = new Error(readable);
  err.name = 'UpstreamTimeoutError';
  err.code = stage.indexOf('timeout') >= 0 ? stage : (stage + '_timeout');
  err.timeout_ms = ms;
  err.retryable = true;
  return err;
}

export function isTimeoutError(err) {
  function hasTimeoutKeyword(text) {
    var lower = String(text || '').toLowerCase();
    return lower.indexOf('timed out') >= 0
      || lower.indexOf('timeout') >= 0
      || lower.indexOf('due to timeout') >= 0;
  }

  function walk(node, depth, seen) {
    if (!node || depth > 5) return false;
    if (typeof node === 'string') return hasTimeoutKeyword(node);
    if (typeof node !== 'object') return false;
    if (seen.has(node)) return false;
    seen.add(node);

    var name = String(node.name || '').toLowerCase();
    var code = String(node.code || '').toLowerCase();
    var msg = String(node.message || '').toLowerCase();
    var timeoutMs = Number(node.timeout_ms || 0);
    var signalReason = node.signal && node.signal.reason;
    var cause = node.cause;
    var reason = node.reason;

    if (name.indexOf('timeout') >= 0 || code.indexOf('timeout') >= 0) return true;
    if (timeoutMs > 0) return true;
    if (hasTimeoutKeyword(msg)) return true;

    var isAbort = name === 'aborterror' || code === 'abort_err';
    if (isAbort) {
      return walk(cause, depth + 1, seen)
        || walk(reason, depth + 1, seen)
        || walk(signalReason, depth + 1, seen);
    }

    return walk(cause, depth + 1, seen)
      || walk(reason, depth + 1, seen)
      || walk(signalReason, depth + 1, seen);
  }

  return walk(err, 0, new Set());
}

export function isNetworkError(err) {
  if (!err) return false;
  if (isTimeoutError(err)) return true;
  var code = String(err.code || '').toLowerCase();
  var msg = String(err.message || '').toLowerCase();
  var causeCode = String(err.cause && err.cause.code || '').toLowerCase();
  var merged = [code, causeCode, msg].join(' ');
  return merged.indexOf('econnreset') >= 0
    || merged.indexOf('econnrefused') >= 0
    || merged.indexOf('etimedout') >= 0
    || merged.indexOf('enotfound') >= 0
    || merged.indexOf('ehostunreach') >= 0
    || merged.indexOf('socket hang up') >= 0
    || merged.indexOf('network error') >= 0
    || merged.indexOf('fetch failed') >= 0;
}

export function isRetryableStatusCode(statusCode) {
  var status = Number(statusCode);
  if (!Number.isFinite(status) || status <= 0) return false;
  if (status === 429) return true;
  return status >= 500 && status < 600;
}

/**
 * 统一可重试判定
 *
 * 说明:
 * - 已向下游发送 chunk 的流式请求不能重试
 * - content_filter / 请求格式错误归为不可重试
 */
export function isRetryableError(opts) {
  var options = opts || {};
  if (options.has_sent_data) return false;

  var status = Number(options.status || 0);
  var errorType = String(options.error_type || '').toLowerCase();
  var errorCode = String(options.error_code || '').toLowerCase();
  var message = String(options.message || '').toLowerCase();
  var accountIssue = !!options.account_issue;

  function hasKeyword(keyword) {
    return errorType.indexOf(keyword) >= 0
      || errorCode.indexOf(keyword) >= 0
      || message.indexOf(keyword) >= 0;
  }

  if (hasKeyword('content_filter')) return false;
  if (hasKeyword('invalid_request') || hasKeyword('validation') || hasKeyword('bad_request')) return false;

  if (options.timeout || isTimeoutError(options.error)) return true;
  if (options.network || isNetworkError(options.error)) return true;

  if (isRetryableStatusCode(status)) return true;
  if ((status === 401 || status === 403) && accountIssue) return true;

  if (hasKeyword('server_error')
    || hasKeyword('internal_error')
    || hasKeyword('upstream_error')
    || hasKeyword('overloaded')
    || hasKeyword('rate_limit')
    || hasKeyword('insufficient_quota')
    || hasKeyword('network')
    || hasKeyword('connection')
    || hasKeyword('timeout')
    || hasKeyword('fetch failed')
    || hasKeyword('econn')
    || hasKeyword('empty')
    || hasKeyword('no_data')
    || hasKeyword('missing_completed_response')
    || hasKeyword('response_failed')) {
    return true;
  }

  return false;
}

/**
 * 重试时挑选与失败账号不同的账号
 */
export function pickRetryAccount(pool, excludeEmails) {
  if (!pool || typeof pool.getAccount !== 'function') return null;
  var excludes = Array.isArray(excludeEmails) ? excludeEmails : [];
  var excludeSet = new Set();
  for (var i = 0; i < excludes.length; i++) {
    if (excludes[i]) excludeSet.add(String(excludes[i]));
  }

  var locked = [];
  try {
    if (typeof pool.lockAccount === 'function') {
      excludeSet.forEach(function (email) {
        if (pool.lockAccount(email)) locked.push(email);
      });
    }
    var account = pool.getAccount();
    if (!account || !account.email) return null;
    if (excludeSet.has(account.email)) return null;
    return account;
  } finally {
    if (typeof pool.unlockAccount === 'function') {
      for (var li = 0; li < locked.length; li++) {
        pool.unlockAccount(locked[li]);
      }
    }
  }
}

/**
 * 统一派生 prompt cache session_id
 *
 * 设计目标:
 *   - 客户端显式 session_id 优先
 *   - 派生值不依赖 account，避免换号导致 cache key 频繁变化
 *   - 在 caller 维度上保持稳定，必要时叠加 route/model/user 增强区分度
 */
export function derivePromptCacheSessionId(opts) {
  var options = opts || {};
  var clientSessionId = (options.clientSessionId || '').trim();
  if (clientSessionId) {
    return clientSessionId;
  }

  var caller = (options.callerIdentity || 'unknown').trim() || 'unknown';
  var route = (options.route || 'unknown').trim() || 'unknown';
  var model = (options.model || '').trim();
  var user = (options.user || '').trim();
  var promptCacheKey = (options.promptCacheKey || '').trim();

  // 固定拼接顺序，确保相同输入得到相同派生值。
  var seed = [caller, route, model, promptCacheKey || user].join('|');
  return crypto.createHash('sha256').update(seed).digest('hex').substring(0, 32);
}
