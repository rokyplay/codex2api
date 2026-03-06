/**
 * 自动重登模块
 *
 * 当 token-refresher 把账号标为 relogin_needed 时，自动调 register-server 重登。
 * - 重登成功 → register-server 自动上传新 token，pool 通过 credentials API 更新
 * - 重登失败（deactivated/deleted）→ 标废
 * - 其他错误 → 延迟重试
 */

import { log, C } from './utils.mjs';

function isBanError(errStr) {
  var lower = String(errStr || '').toLowerCase();
  return lower.indexOf('deactivated') >= 0
    || lower.indexOf('deleted') >= 0
    || lower.indexOf('banned') >= 0;
}

export class AutoRelogin {

  constructor(pool, config, onEvent) {
    this._pool = pool;
    this._config = config || {};
    this._onEvent = typeof onEvent === 'function' ? onEvent : null;
    this._queue = [];
    this._activeCount = 0;
    this._maxConcurrency = this._resolveMaxConcurrency();
    this._timer = null;
    this._attempted = {};  // email → { count, lastAttempt }
    this._lastBusyPauseLogAt = 0;
    this._pauseUntil = 0;
    this._pauseReason = '';
    this._resumeTimer = null;
    this._bootstrapTimer = null;
  }

  _emit(level, message, meta) {
    if (!this._onEvent) return;
    try { this._onEvent(level, message, meta || {}); } catch (_) {}
  }

  _getRegisterConfig() {
    return this._config.register || null;
  }

  _isFeatureEnabled() {
    return !!(this._config
      && this._config.credentials
      && this._config.credentials.auto_relogin === true);
  }

  _disableIfFeatureOff() {
    if (this._isFeatureEnabled()) return false;
    this.stop();
    return true;
  }

  _resolveMaxConcurrency() {
    var fromCred = this._config && this._config.credentials ? this._config.credentials.relogin_concurrency : null;
    var fromReg = this._config && this._config.register ? this._config.register.relogin_concurrency : null;
    var raw = fromCred !== undefined && fromCred !== null ? fromCred : fromReg;
    var parsed = parseInt(raw, 10);
    if (!isFinite(parsed) || parsed <= 0) return 3;
    if (parsed > 20) return 20;
    return parsed;
  }

  _decrementAttemptCount(email) {
    if (!email || !this._attempted[email]) return;
    this._attempted[email].count = Math.max(0, (this._attempted[email].count || 0) - 1);
  }

  _isTransientInfraError(errStr) {
    var lower = String(errStr || '').toLowerCase();
    if (!lower) return false;
    return lower.indexOf('timeout') >= 0
      || lower.indexOf('timed out') >= 0
      || lower.indexOf('aborted') >= 0
      || lower.indexOf('fetch failed') >= 0
      || lower.indexOf('econn') >= 0
      || lower.indexOf('socket hang up') >= 0
      || lower.indexOf('network') >= 0
      || lower.indexOf('service unavailable') >= 0
      || lower.indexOf('gateway timeout') >= 0
      || lower.indexOf('register busy') >= 0
      || lower.indexOf('job running') >= 0
      || lower.indexOf('task running') >= 0
      || lower.indexOf('注册机忙') >= 0
      || lower.indexOf('繁忙') >= 0;
  }

  _isPaused() {
    return this._pauseUntil > Date.now();
  }

  _setPause(ms, reason) {
    if (this._disableIfFeatureOff()) return;
    var pauseMs = parseInt(ms, 10);
    if (!isFinite(pauseMs) || pauseMs <= 0) pauseMs = 15000;
    var until = Date.now() + pauseMs;
    if (until > this._pauseUntil) {
      this._pauseUntil = until;
      this._pauseReason = reason || '';
    }
    if (this._resumeTimer) {
      clearTimeout(this._resumeTimer);
      this._resumeTimer = null;
    }
    var delay = Math.max(100, this._pauseUntil - Date.now());
    var self = this;
    this._resumeTimer = setTimeout(function () {
      self._resumeTimer = null;
      self._processNext().catch(function () {});
    }, delay);
  }

  /**
   * 启动自动重登
   */
  start() {
    if (!this._isFeatureEnabled()) {
      this.stop();
      log('⏹️', C.yellow, 'AutoRelogin: disabled by credentials.auto_relogin=false');
      return;
    }

    var reg = this._getRegisterConfig();
    if (!reg || !reg.api_url || !reg.api_token) {
      this.stop();
      log('⚠️', C.yellow, 'AutoRelogin: register server not configured, disabled');
      return;
    }

    this._maxConcurrency = this._resolveMaxConcurrency();
    if (this._timer || this._bootstrapTimer) {
      return;
    }

    log('🔑', C.cyan, 'AutoRelogin: enabled, endpoint=' + reg.api_url);

    var self = this;
    // 每 60 秒扫描一次 relogin_needed 账号
    var interval = (this._config.credentials && this._config.credentials.relogin_interval_ms) || 60000;
    this._timer = setInterval(function () {
      self._scan();
    }, interval);

    // 启动后 10 秒首次扫描
    this._bootstrapTimer = setTimeout(function () { self._scan(); }, 10000);
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    if (this._resumeTimer) {
      clearTimeout(this._resumeTimer);
      this._resumeTimer = null;
    }
    if (this._bootstrapTimer) {
      clearTimeout(this._bootstrapTimer);
      this._bootstrapTimer = null;
    }
    this._pauseUntil = 0;
    this._pauseReason = '';
    this._queue = [];
    this._attempted = {};
    this._lastBusyPauseLogAt = 0;
  }

  /**
   * 手动入队一个账号
   */
  enqueue(email) {
    if (this._disableIfFeatureOff()) return;
    if (this._queue.indexOf(email) >= 0) return;
    this._queue.push(email);
    this._processNext();
  }

  /**
   * 扫描所有 relogin_needed 账号并入队
   */
  _scan() {
    if (this._disableIfFeatureOff()) return;
    var accounts = this._pool.getReloginAccounts ? this._pool.getReloginAccounts() : [];
    var maxRetries = (this._config.credentials && this._config.credentials.relogin_max_retries) || 3;
    var now = Date.now();
    var enqueued = 0;

    for (var i = 0; i < accounts.length; i++) {
      var email = accounts[i].email;
      var att = this._attempted[email];

      // 跳过已达重试上限的
      if (att && att.count >= maxRetries) continue;
      // 跳过最近 5 分钟内尝试过的
      if (att && (now - att.lastAttempt) < 300000) continue;
      // 跳过已在队列中的
      if (this._queue.indexOf(email) >= 0) continue;

      this._queue.push(email);
      enqueued++;
    }

    if (enqueued > 0) {
      log('🔑', C.cyan, 'AutoRelogin: queued ' + enqueued + ' accounts');
    }
    if (this._queue.length > 0 && !this._isPaused()) this._processNext();
  }

  _logRegisterBusyPause() {
    var now = Date.now();
    if ((now - this._lastBusyPauseLogAt) < 30000) return;
    this._lastBusyPauseLogAt = now;
    log('⏸️', C.yellow, 'AutoRelogin: 注册机忙碌中，暂停重登队列');
    this._emit('info', '注册机忙碌中，暂停重登队列', { type: 'auto_relogin_paused_busy' });
  }

  _inferRegisterRunning(payload) {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return false;
    var candidates = [
      payload,
      payload.data,
      payload.job,
      payload.current_job,
      payload.task,
      payload.current_task,
    ];
    for (var i = 0; i < candidates.length; i++) {
      var c = candidates[i];
      if (!c || typeof c !== 'object' || Array.isArray(c)) continue;
      if (typeof c.running === 'boolean') return c.running;
      if (typeof c.is_running === 'boolean') return c.is_running;
      if (typeof c.busy === 'boolean') return c.busy;
      if (typeof c.status === 'string') {
        var lowerStatus = c.status.trim().toLowerCase();
        if (lowerStatus === 'running' || lowerStatus === 'processing' || lowerStatus === 'in_progress') {
          return true;
        }
      }
    }
    return false;
  }

  async _isRegisterBusy() {
    var reg = this._getRegisterConfig();
    if (!reg || !reg.api_url || !reg.api_token) return false;
    var url = reg.api_url.replace(/\/+$/, '') + '/api/jobs/status';
    var timeoutMs = (reg.relogin_status_timeout_ms) || 15000;
    try {
      var resp = await fetch(url, {
        method: 'GET',
        headers: {
          'Authorization': 'Bearer ' + reg.api_token,
        },
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!resp.ok) {
        this._setPause(15000, 'status_http_' + resp.status);
        return true;
      }
      var data;
      try {
        data = await resp.json();
      } catch (_) {
        this._setPause(15000, 'status_invalid_json');
        return true;
      }
      // 检查重登队列容量，而不是注册任务状态
      // 重登有独立队列和并发控制，不应被注册任务阻塞
      var reloginApi = data.relogin && data.relogin.api;
      if (!reloginApi) reloginApi = data.relogin_api;
      if (reloginApi && typeof reloginApi === 'object') {
        var queueFull = reloginApi.queued >= (reloginApi.queue_limit || 50);
        var atCapacity = reloginApi.running >= (reloginApi.concurrency_limit || 20);
        if (queueFull || atCapacity) {
          this._setPause(5000, 'relogin_queue_full');
          return true;
        }
        return false;
      }
      // 兜底：无 relogin 数据时用旧逻辑
      var running = this._inferRegisterRunning(data);
      if (running) {
        this._setPause(5000, 'register_running');
      }
      return running;
    } catch (err) {
      this._setPause(15000, 'status_error');
      this._emit('warn', 'AutoRelogin status check failed', {
        type: 'auto_relogin_status_error',
        detail: err && err.message ? err.message : String(err || ''),
      });
      return true;
    }
  }

  async _processNext() {
    if (this._disableIfFeatureOff()) return;
    if (this._queue.length <= 0) return;
    if (this._isPaused()) return;
    var registerBusy = await this._isRegisterBusy();
    if (registerBusy) {
      this._logRegisterBusyPause();
      return;
    }
    while (this._activeCount < this._maxConcurrency && this._queue.length > 0) {
      this._activeCount++;
      this._processOne(this._queue.shift());
    }
  }

  async _processOne(email) {

    if (this._disableIfFeatureOff()) {
      this._activeCount--;
      return;
    }

    var account = this._pool.getFullAccount(email);

    if (!account || !account.password || account.status === 'wasted') {
      this._activeCount--;
      this._processNext();
      return;
    }

    var reg = this._getRegisterConfig();
    if (!reg || !reg.api_url || !reg.api_token) {
      this._activeCount--;
      return;
    }

    var registerBusy = await this._isRegisterBusy();
    if (registerBusy) {
      this._logRegisterBusyPause();
      if (this._queue.indexOf(email) < 0) this._queue.unshift(email);
      this._setPause(5000, 'register_busy_preflight');
      this._activeCount--;
      return;
    }

    if (this._disableIfFeatureOff()) {
      this._activeCount--;
      return;
    }

    // 记录尝试
    if (!this._attempted[email]) {
      this._attempted[email] = { count: 0, lastAttempt: 0 };
    }
    this._attempted[email].count++;
    this._attempted[email].lastAttempt = Date.now();

    log('🔑', C.cyan, 'AutoRelogin: attempting ' + email + ' (attempt ' + this._attempted[email].count + ')');

    try {
      var url = reg.api_url.replace(/\/+$/, '') + '/api/relogin';
      var timeout = (reg.relogin_timeout_ms) || 120000;

      var resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + reg.api_token,
        },
        body: JSON.stringify({ email: email, password: account.password }),
        signal: AbortSignal.timeout(timeout),
      });

      var data;
      try { data = await resp.json(); } catch (e) {
        data = { success: false, error: 'invalid response from register server' };
      }

      if (data.success) {
        // 重登成功！register-server 自动上传 token 到 codex2api 的 credentials API
        // pool 会通过 credentials API 自动更新
        log('✅', C.green, 'AutoRelogin success: ' + email);
        this._emit('info', 'AutoRelogin success: ' + email, { email: email, type: 'auto_relogin_success' });
        // 兜底：若新 token 已回写但状态仍未切回 active，主动恢复
        var statusRestoreSelf = this;
        setTimeout(function () {
          var refreshed = statusRestoreSelf._pool.getFullAccount ? statusRestoreSelf._pool.getFullAccount(email) : null;
          if (!refreshed || refreshed.status === 'active') return;
          var nowSec = Math.floor(Date.now() / 1000);
          if (refreshed.accessToken && refreshed.token_expires_at && refreshed.token_expires_at > nowSec + 90) {
            statusRestoreSelf._pool.updateToken(email, null, null);
            statusRestoreSelf._pool.forceSave();
            log('✅', C.green, 'AutoRelogin: restored active status: ' + email);
          }
        }, 3000);
        // 清理尝试记录
        delete this._attempted[email];
      } else {
        var errStr = data.error || data.detail || '';

        if (isBanError(errStr)) {
          // 被封 → 标废
          this._pool.markWasted(email);
          this._pool.forceSave();
          log('🚫', C.red, 'AutoRelogin: account banned, marked wasted: ' + email + ' (' + errStr.substring(0, 80) + ')');
          this._emit('warn', 'AutoRelogin banned: ' + email, { email: email, type: 'auto_relogin_banned', detail: errStr });
          delete this._attempted[email];
        } else if (this._isTransientInfraError(errStr)) {
          // 基础设施错误（超时/忙碌/网络）不计入账号重试额度，避免“注册机抖动”把账号打满重试上限
          this._decrementAttemptCount(email);
          var retryDelayMs = (reg.relogin_retry_backoff_ms) || 30000;
          this._setPause(retryDelayMs, 'relogin_transient_error');
          if (!this._disableIfFeatureOff() && this._queue.indexOf(email) < 0) this._queue.push(email);
          log('⚠️', C.yellow, 'AutoRelogin transient failure: ' + email + ' — ' + errStr.substring(0, 100));
          this._emit('warn', 'AutoRelogin transient failure: ' + email, {
            email: email,
            type: 'auto_relogin_retryable',
            detail: errStr,
            pause_ms: retryDelayMs,
          });
        } else {
          // 其他错误 → 下次重试
          log('❌', C.red, 'AutoRelogin failed: ' + email + ' — ' + errStr.substring(0, 100));
          this._emit('warn', 'AutoRelogin failed: ' + email, { email: email, type: 'auto_relogin_failed', detail: errStr });
        }
      }
    } catch (err) {
      var errMsg = err && err.message ? err.message : String(err);
      if (this._isTransientInfraError(errMsg)) {
        this._decrementAttemptCount(email);
        var catchRetryDelayMs = (reg.relogin_retry_backoff_ms) || 30000;
        this._setPause(catchRetryDelayMs, 'relogin_transient_exception');
        if (!this._disableIfFeatureOff() && this._queue.indexOf(email) < 0) this._queue.push(email);
        log('⚠️', C.yellow, 'AutoRelogin transient error: ' + email + ' — ' + errMsg);
        this._emit('warn', 'AutoRelogin transient error: ' + email, {
          email: email,
          type: 'auto_relogin_retryable_error',
          detail: errMsg,
          pause_ms: catchRetryDelayMs,
        });
      } else {
        log('❌', C.red, 'AutoRelogin error: ' + email + ' — ' + errMsg);
        this._emit('warn', 'AutoRelogin error: ' + email, { email: email, type: 'auto_relogin_error', detail: errMsg });
      }
    }

    this._activeCount--;
    var self = this;
    setTimeout(function () { self._processNext(); }, 1000);
  }
}
