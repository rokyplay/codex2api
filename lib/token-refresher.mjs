/**
 * Token 自动刷新器
 *
 *
 * 功能:
 *   - 启动时批量刷新所有过期 token
 *   - 定时检查，过期前 N 秒自动刷新
 *   - 复用已有 session.mjs 的 _refreshToken() 逻辑
 */

import { log, C } from './utils.mjs';

var AUTH_SESSION_API = 'https://chatgpt.com/api/auth/session';
var UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.88 Safari/537.36';

function classifyRefreshError(detail) {
  var text = String(detail || '').toLowerCase();
  if (!text) return '';
  if (text.indexOf('logged out') >= 0
    || text.indexOf('signed in to another account') >= 0
    || text.indexOf('could not be refreshed') >= 0
    || text.indexOf('invalid session') >= 0
    || text.indexOf('session invalid') >= 0
    || text.indexOf('no_session_token') >= 0
    || text.indexOf('token_invalidated') >= 0
    || text.indexOf('token has been invalidated') >= 0) {
    return 'session_invalidated';
  }
  return '';
}

function extractDetailFromObject(obj) {
  if (!obj) return '';
  if (typeof obj === 'string') return obj;
  var parts = [];
  if (typeof obj.error === 'string') parts.push(obj.error);
  if (typeof obj.message === 'string') parts.push(obj.message);
  if (typeof obj.detail === 'string') parts.push(obj.detail);
  if (Array.isArray(obj.errors)) {
    for (var i = 0; i < obj.errors.length; i++) {
      var item = obj.errors[i];
      if (typeof item === 'string') parts.push(item);
      else if (item && typeof item.message === 'string') parts.push(item.message);
    }
  }
  if (parts.length > 0) return parts.join(' | ');
  try {
    return JSON.stringify(obj);
  } catch (_) {
    return '';
  }
}

function extractDetailFromText(text) {
  if (!text) return '';
  try {
    var parsed = JSON.parse(text);
    return extractDetailFromObject(parsed) || text;
  } catch (_) {
    return text;
  }
}

function inferStatusCode(result) {
  if (result && typeof result.statusCode === 'number' && result.statusCode > 0) {
    return result.statusCode;
  }
  var code = result && result.error ? String(result.error) : '';
  if (code.indexOf('http_') === 0) {
    var n = parseInt(code.substring(5), 10);
    if (isFinite(n) && n > 0) return n;
  }
  if (code === 'session_invalidated' || code === 'no_session_token' || code === 'no_access_token_in_response') {
    return 401;
  }
  return 0;
}

/**
 * 刷新单个账号的 token
 *
 * 复用 session.mjs 的逻辑，但独立函数方便 pool 调用
 *
 * @param {object} account - pool 中的账号状态对象
 * @returns {{ success: boolean, error?: string }}
 */
export async function refreshAccountToken(account) {
  if (!account.sessionToken) {
    return { success: false, error: 'no_session_token', statusCode: 401, detail: 'no_session_token' };
  }

  var cookieStr = '__Secure-next-auth.session-token=' + account.sessionToken;
  if (account.cookies && account.cookies['oai-did']) {
    cookieStr += '; oai-did=' + account.cookies['oai-did'];
  }

  try {
    var resp = await fetch(AUTH_SESSION_API, {
      headers: {
        'User-Agent': UA,
        'Cookie': cookieStr,
        'Accept': 'application/json',
      },
      signal: AbortSignal.timeout(15000),
    });

    if (!resp.ok) {
      var errText = await resp.text().catch(function () { return ''; });
      var errDetail = extractDetailFromText(errText) || ('http_' + resp.status);
      var classified = classifyRefreshError(errDetail);
      return {
        success: false,
        error: classified || ('http_' + resp.status),
        statusCode: resp.status,
        detail: errDetail,
      };
    }

    var data = await resp.json();

    if (!data.accessToken) {
      var noTokenDetail = extractDetailFromObject(data) || 'no_access_token_in_response';
      var noTokenClassified = classifyRefreshError(noTokenDetail);
      return {
        success: false,
        error: noTokenClassified || 'no_access_token_in_response',
        statusCode: 401,
        detail: noTokenDetail,
      };
    }

    // 解析新 token
    var newToken = data.accessToken;
    var newExpiry = 0;
    var newAccountId = '';
    try {
      var payload = JSON.parse(Buffer.from(newToken.split('.')[1], 'base64url').toString());
      newExpiry = payload.exp || 0;
      var auth = payload['https://api.openai.com/auth'] || {};
      newAccountId = auth.chatgpt_account_id || '';
    } catch (e) {
      // JWT 解析失败不阻止更新
    }

    // 更新 session token（如果 set-cookie 返回了新的）
    var newSessionToken = null;
    var setCookies = resp.headers.getSetCookie ? resp.headers.getSetCookie() : [];
    for (var i = 0; i < setCookies.length; i++) {
      var parts = setCookies[i].split(';')[0].split('=');
      if (parts[0].trim() === '__Secure-next-auth.session-token') {
        newSessionToken = parts.slice(1).join('=').trim();
      }
    }

    // 验证新 token 实际可用（防止假续期：session 被封但仍返回 JWT）
    try {
      var verifyResp = await fetch('https://chatgpt.com/backend-api/me', {
        headers: {
          'User-Agent': UA,
          'Authorization': 'Bearer ' + newToken,
          'Accept': 'application/json',
        },
        signal: AbortSignal.timeout(10000),
      });
      if (!verifyResp.ok) {
        var verifyText = await verifyResp.text().catch(function () { return ''; });
        var verifyDetail = extractDetailFromText(verifyText) || ('verify_http_' + verifyResp.status);
        var verifyClassified = classifyRefreshError(verifyDetail);
        return {
          success: false,
          error: verifyClassified || ('token_verify_failed_' + verifyResp.status),
          statusCode: verifyResp.status,
          detail: 'token_refreshed_but_verify_failed: ' + verifyDetail,
        };
      }
    } catch (verifyErr) {
      return {
        success: false,
        error: 'token_verify_error',
        statusCode: 0,
        detail: 'token_refreshed_but_verify_error: ' + (verifyErr.message || 'unknown'),
      };
    }

    return {
      success: true,
      accessToken: newToken,
      sessionToken: newSessionToken,
      tokenExpiresAt: newExpiry,
      accountId: newAccountId,
    };
  } catch (err) {
    return { success: false, error: err.message || 'unknown', statusCode: 0, detail: err.message || 'unknown' };
  }
}

/**
 * TokenRefresher — Token 自动刷新管理器
 */
export class TokenRefresher {

  /**
   * @param {AccountPool} pool
   * @param {object} config - config-server.json 的 credentials 段
   * @param {object} i18n - i18n.account 段
   */
  constructor(pool, config, i18n, onEvent) {
    this._pool = pool;
    this._config = config || {};
    this._i18n = i18n || {};
    this._timer = null;
    this._refreshing = false;
    this._onEvent = typeof onEvent === 'function' ? onEvent : null;
  }

  _emit(level, message, meta) {
    if (!this._onEvent) return;
    try {
      this._onEvent(level, message, meta || {});
    } catch (_) {
      // ignore
    }
  }

  /**
   * 启动 — 批量刷新 + 定时任务
   */
  async start() {
    await this.refreshAll();

    // 2. 定时检查（每分钟检查一次）
    var self = this;
    this._timer = setInterval(function () {
      self.refreshAll().catch(function (err) {
        log('❌', C.red, 'Token 定时刷新出错: ' + err.message);
      });
    }, 60000);

    // 3. Session 保活 — 定期刷新所有活跃账号（防 session_token 过期）
    var keepaliveHours = this._config.session_keepalive_hours || 12;
    this._keepaliveTimer = setInterval(function () {
      self._keepaliveSessions().catch(function (err) {
        log('❌', C.red, 'Session 保活出错: ' + err.message);
      });
    }, keepaliveHours * 3600000);

    // 4. 注册即时刷新回调 — getAccount() 发现过期账号时不等 60 秒，立刻触发
    this._pool.onExpiredDetected(function () {
      if (self._refreshing) return; // 已在刷新中就跳过
      self.refreshAll().catch(function (err) {
        log('❌', C.red, 'Token 即时刷新出错: ' + err.message);
      });
    });
  }

  /**
   * 停止定时任务
   */
  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    if (this._keepaliveTimer) {
      clearInterval(this._keepaliveTimer);
      this._keepaliveTimer = null;
    }
  }

  /**
   * 批量刷新所有需要刷新的账号
   *
   */
  async refreshAll() {
    if (this._refreshing) return; // 防并发
    this._refreshing = true;

    try {
      var beforeExpiry = this._config.refresh_before_expiry_seconds || 300;
      var expired = this._pool.getExpiredAccounts(beforeExpiry);

      if (expired.length === 0) return;

      log('🔄', C.yellow, '需要刷新 ' + expired.length + ' 个账号的 token...');

      // 并发刷新，但限制为 3 个并发（避免触发限流）
      var concurrency = 3;
      var results = { success: 0, failed: 0, skipped: 0 };
      var pool = this._pool;

      for (var i = 0; i < expired.length; i += concurrency) {
        var batch = expired.slice(i, i + concurrency);
        var promises = batch.map(function (account) {
          // 跳过被锁定的账号（正在重登）
          if (pool.isLocked(account.email)) return Promise.resolve();
          if (!pool.lockAccount(account.email)) return Promise.resolve();

          var tokenVersion = typeof account._tokenVersion === 'number' ? account._tokenVersion : 0;

          return refreshAccountToken(account).then(function (result) {
            if (result.success) {
              if (result.tokenExpiresAt > 0 && result.tokenExpiresAt < Math.floor(Date.now() / 1000) + 60) {
                results.failed++;
                // 不设 _lastRefreshAt — 让 60s 定时器下一轮重试
                var shortTtlMsg = 'Token 续期失败(寿命过短): ' + account.email + ' (exp=' + result.tokenExpiresAt + ')';
                log('⚠️', C.yellow, shortTtlMsg);
                this._emit('warn', shortTtlMsg, {
                  email: account.email,
                  error: 'token_ttl_too_short',
                  detail: 'token_expires_too_soon',
                  statusCode: 0,
                });
                return;
              }
              var casResult = pool.applyRefreshResultCAS(account, result, tokenVersion);
              if (casResult.applied) {
                results.success++;
                var okMsg = 'Token 续期成功: ' + account.email;
                log('✅', C.green, okMsg);
                this._emit('info', okMsg, { email: account.email, type: 'token_refresh_success' });
              } else if (casResult.reason === 'stale_version') {
                results.skipped++;
                var staleMsg = 'Token 续期跳过（版本落后）: ' + account.email;
                log('ℹ️', C.cyan, staleMsg);
                this._emit('info', staleMsg, {
                  email: account.email,
                  type: 'token_refresh_skipped_stale',
                  expectedVersion: tokenVersion,
                  currentVersion: casResult.currentVersion,
                });
              } else {
                results.failed++;
                // 不设 _lastRefreshAt — 让 60s 定时器下一轮重试
                var casFailMsg = 'Token 续期写回失败: ' + account.email + ' (' + (casResult.reason || 'unknown') + ')';
                log('❌', C.red, casFailMsg);
                this._emit('warn', casFailMsg, { email: account.email, type: 'token_refresh_apply_failed' });
              }
            } else {
              results.failed++;
              var statusCode = inferStatusCode(result);
              var detail = result.detail || result.error || 'unknown';
              var classified = classifyRefreshError(detail);
              var isNetworkOr5xx = statusCode === 0 || statusCode >= 500;
              if (classified === 'session_invalidated') {
                var markResult = pool.markError(account.email, statusCode, detail);
                account._lastRefreshAt = Date.now();

                  var sessionFailMsg = 'Token 续期失败(session失效): ' + account.email
                    + ' (' + detail + ') -> ' + markResult.action
                    + ' (invalidated_count=' + (account.session_invalidated_count || 0) + ')';
                  log('❌', C.red, sessionFailMsg);
                  this._emit('warn', sessionFailMsg, {
                    email: account.email, error: result.error || '', detail: detail,
                    statusCode: statusCode, classified: markResult.type, action: markResult.action,
                    session_invalidated_count: account.session_invalidated_count || 0,
                  });
              } else if (isNetworkOr5xx) {
                var retryableMark = pool.markRefreshRetryableFailure(account.email, statusCode, detail);
                var retryFailMsg = 'Token 续期失败(将重试): ' + account.email + ' (' + detail + ')';
                log('⚠️', C.yellow, retryFailMsg);
                this._emit('warn', retryFailMsg, {
                  email: account.email,
                  error: result.error || '',
                  detail: detail,
                  statusCode: statusCode,
                  classified: retryableMark.type || 'refresh_retryable',
                  action: 'retry_next_round',
                });
              } else {
                var classifyResult = pool.markError(account.email, statusCode, detail);
                var failMsg = 'Token 续期失败(' + classifyResult.type + '): ' + account.email + ' (' + detail + ')';
                log('⚠️', C.yellow, failMsg);
                this._emit('warn', failMsg, {
                  email: account.email,
                  error: result.error || '',
                  detail: detail,
                  statusCode: statusCode,
                  classified: classifyResult.type,
                  action: classifyResult.action,
                });
              }
            }
          }.bind(this)).finally(function () {
            pool.unlockAccount(account.email);
          });
        }.bind(this));

        await Promise.all(promises);
      }

      if (results.success > 0 || results.failed > 0 || results.skipped > 0) {
        log('📊', C.cyan, 'Token 刷新完成: 成功 ' + results.success + '，失败 ' + results.failed + '，跳过 ' + results.skipped);
      }

      // 刷新成功后立即持久化，避免崩溃丢失新 token
      if (results.success > 0) {
        this._pool.forceSave();
      }
    } finally {
      this._refreshing = false;
    }
  }

  /**
   * Session 保活 — 定期刷新所有活跃账号的 session（防 session_token 过期）
   */
  async _keepaliveSessions() {
    var allActive = this._pool.getActiveAccountObjects();
    if (allActive.length === 0) return;

    log('🔄', C.cyan, 'Session 保活: 刷新 ' + allActive.length + ' 个活跃账号...');

    var concurrency = 3;
    var results = { success: 0, failed: 0, skipped: 0 };
    var pool = this._pool;

    for (var i = 0; i < allActive.length; i += concurrency) {
      var batch = allActive.slice(i, i + concurrency);
      var promises = batch.map(function (account) {
        if (pool.isLocked(account.email)) return Promise.resolve();
        if (!pool.lockAccount(account.email)) return Promise.resolve();

        var tokenVersion = typeof account._tokenVersion === 'number' ? account._tokenVersion : 0;

        return refreshAccountToken(account).then(function (result) {
          if (result.success) {
            var casResult = pool.applyRefreshResultCAS(account, result, tokenVersion);
            if (casResult.applied) {
              results.success++;
            } else if (casResult.reason === 'stale_version') {
              results.skipped++;
            } else {
              results.failed++;
            }
          } else {
            results.failed++;
            // 保活失败也要正确分类处理，避免已死 session 永远留在 active
            var keepaliveDetail = result.detail || result.error || 'unknown';
            var keepaliveClassified = classifyRefreshError(keepaliveDetail);
            var keepaliveStatusCode = inferStatusCode(result);
            if (keepaliveClassified === 'session_invalidated') {
              pool.markError(account.email, keepaliveStatusCode, keepaliveDetail);
              account._lastRefreshAt = Date.now();
              log('❌', C.red, 'Session 保活失败(session失效): ' + account.email + ' (' + keepaliveDetail + ')');
            } else if (keepaliveStatusCode === 0 || keepaliveStatusCode >= 500) {
              // 网络/上游错误 — 不影响账号状态，下次保活重试
              log('⚠️', C.yellow, 'Session 保活失败(将重试): ' + account.email + ' (' + keepaliveDetail + ')');
            } else {
              pool.markError(account.email, keepaliveStatusCode, keepaliveDetail);
              log('⚠️', C.yellow, 'Session 保活失败: ' + account.email + ' (' + keepaliveDetail + ')');
            }
          }
        }).finally(function () {
          pool.unlockAccount(account.email);
        });
      });

      await Promise.all(promises);
    }

    log('📊', C.cyan, 'Session 保活完成: 成功 ' + results.success + ', 失败 ' + results.failed + ', 跳过 ' + results.skipped);
    if (results.success > 0) {
      this._pool.forceSave();
    }
  }
}
