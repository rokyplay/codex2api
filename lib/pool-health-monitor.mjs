import { testOneAccount } from './account-tester.mjs';
import { startRegistration, getRegistrationStatus, ensureServerRunning, selectBestProxy } from './register-client.mjs';

export default class PoolHealthMonitor {
  constructor(pool, config, logCollector) {
    this._pool = pool;
    this._config = config;
    this._logCollector = logCollector;
    this._probeTimer = null;
    this._guardTimer = null;
    this._guardBootstrapTimer = null;
    this._guardCooldownRetryTimer = null;
    this._guardExecuting = false;
    this._registering = false;
    this._registeringSince = null;
    this._guardCooldownUntil = 0;
    this._guardCooldownReason = '';
    this._lastProbeResult = null;
    this._lastGuardResult = null;
    this._lastProbeTime = null;
    this._lastGuardTime = null;
    this._newAccountQueue = new Map(); // email -> { email, addedAt }
    this._newAccountVerifyTimer = null;
    this._newAccountVerifyRunning = false;
    this._lastNewAccountVerifyResult = null;
  }

  start() {
    var phConfig = this._config.pool_health;
    if (!phConfig || !phConfig.enabled) {
      this._log('PoolHealthMonitor: disabled');
      return;
    }
    this._log('PoolHealthMonitor: enabled');
    var probeEnabled = phConfig.probe_enabled !== false;

    var probeInterval = this._asPositiveInt(
      phConfig.auto_check_interval_ms,
      this._asPositiveInt(phConfig.health_probe_interval_ms, 600000)
    );
    var guardInterval = this._asPositiveInt(phConfig.pool_guard_interval_ms, 7200000);

    if (typeof this._pool.onNewAccountAdded === 'function') {
      this._pool.onNewAccountAdded((email) => this._enqueueNewAccount(email));
    } else if (typeof this._pool.onNewAccount === 'function') {
      this._pool.onNewAccount((email) => this._enqueueNewAccount(email));
    }

    if (probeEnabled) {
      this._probeTimer = setInterval(() => this._runProbe(), probeInterval);
      setTimeout(() => this._runProbe(), 10000);
    } else {
      this._log('PoolHealthMonitor: probe disabled by config');
    }
    this._newAccountVerifyTimer = setInterval(() => this._runNewAccountVerify(), 60000);

    setTimeout(() => this._runNewAccountVerify(), 20000);
    this._guardBootstrapTimer = setTimeout(() => {
      this._guardBootstrapTimer = null;
      this._runGuard();
      this._guardTimer = setInterval(() => this._runGuard(), guardInterval);
    }, 30000);
  }

  stop() {
    if (this._probeTimer) {
      clearInterval(this._probeTimer);
      this._probeTimer = null;
    }
    if (this._guardTimer) {
      clearInterval(this._guardTimer);
      this._guardTimer = null;
    }
    if (this._guardBootstrapTimer) {
      clearTimeout(this._guardBootstrapTimer);
      this._guardBootstrapTimer = null;
    }
    if (this._newAccountVerifyTimer) {
      clearInterval(this._newAccountVerifyTimer);
      this._newAccountVerifyTimer = null;
    }
    if (typeof this._pool.onNewAccountAdded === 'function') {
      this._pool.onNewAccountAdded(null);
    }
    this._clearGuardRetryTimer();
    this._log('PoolHealthMonitor: stopped');
  }

  async _runProbe() {
    var phConfig = this._config.pool_health || {};
    var sampleSize = this._asPositiveInt(
      phConfig.auto_check_batch_size,
      this._asPositiveInt(phConfig.health_probe_sample_size, 100)
    );
    var probeConcurrency = this._asPositiveInt(phConfig.health_probe_concurrency, 10);
    var testModel = phConfig.test_model || (this._config.models && this._config.models.default) || 'gpt-5.3-codex';

    var _rawAll = await this._pool.listAccounts();
    var allAccounts = Array.isArray(_rawAll) ? _rawAll : (_rawAll && _rawAll.accounts || []);
    var activeAccounts = [];
    for (var i = 0; i < allAccounts.length; i++) {
      if (allAccounts[i].status === 'active') {
        var full = this._pool.getFullAccount(allAccounts[i].email);
        if (full && full.accessToken) activeAccounts.push(full);
      }
    }

    if (activeAccounts.length === 0) {
      this._log('Health Probe: no active accounts to test');
      return;
    }

    var sample = [];
    var shuffled = activeAccounts.slice();
    for (var j = shuffled.length - 1; j > 0; j--) {
      var k = Math.floor(Math.random() * (j + 1));
      var tmp = shuffled[j];
      shuffled[j] = shuffled[k];
      shuffled[k] = tmp;
    }
    sample = shuffled.slice(0, Math.min(sampleSize, shuffled.length));

    this._log('Health Probe: testing ' + sample.length + ' accounts...');
    var okCount = 0;
    var failCount = 0;
    var networkErrorCount = 0;
    var wastedCount = 0;
    var rateLimitedCount = 0;

    var queue = sample.slice();
    var self = this;
    async function worker() {
      while (queue.length > 0) {
        var acc = queue.shift();
        var result = await testOneAccount(acc, testModel);
        if (result.ok) {
          okCount++;
          self._pool.markSuccess(acc.email, null);
          self._log('Health Probe: ok ' + acc.email + ' HTTP ' + (result.status || 200));
        } else if (result.networkError) {
          networkErrorCount++;
          self._log('Health Probe: network error for ' + acc.email + ': ' + result.error);
        } else {
          var statusCode = self._asNonNegativeInt(result.status);
          if (statusCode === 429) {
            rateLimitedCount++;
          }

          failCount++;
          wastedCount++;
          self._pool.markWasted(acc.email);
          self._log('Health Probe: wasted ' + acc.email + ' HTTP ' + statusCode + ', reason=' + (result.error || (statusCode === 429 ? 'probe_rate_limited' : 'probe_failed')));
        }
      }
    }

    var workers = [];
    for (var w = 0; w < Math.min(probeConcurrency, sample.length); w++) {
      workers.push(worker());
    }
    await Promise.all(workers);

    if (failCount > 0) await this._pool.flush();

    this._lastProbeTime = new Date().toISOString();
    this._lastProbeResult = {
      tested: sample.length,
      ok: okCount,
      fail: failCount,
      wasted_count: wastedCount,
      rate_limited_count: rateLimitedCount,
      networkError: networkErrorCount,
    };
    this._log('Health Probe complete: ' + okCount + ' ok, ' + failCount + ' fail, ' + wastedCount + ' wasted, ' + rateLimitedCount + ' rate-limited, ' + networkErrorCount + ' network errors');
    if (this._logCollector) {
      this._logCollector.add('info', 'Health Probe: ' + okCount + '/' + sample.length + ' ok, fail=' + failCount + ', wasted=' + wastedCount + ', rateLimited=' + rateLimitedCount);
    }
  }

  _enqueueNewAccount(email) {
    if (!email || typeof email !== 'string') return;
    var phConfig = this._config.pool_health || {};
    if (phConfig.new_account_verify_enabled === false) return;
    var now = Date.now();
    this._newAccountQueue.set(email, { email: email, addedAt: now });
    this._log('New Account Verify: queued ' + email + ' at ' + new Date(now).toISOString());
  }

  async _runNewAccountVerify() {
    var phConfig = this._config.pool_health || {};
    if (phConfig.new_account_verify_enabled === false) return;
    if (this._newAccountVerifyRunning) {
      this._log('New Account Verify: previous run still executing, skip');
      return;
    }

    var delayMs = this._asPositiveInt(phConfig.new_account_verify_delay_ms, 300000);
    var verifyConcurrency = this._asPositiveInt(phConfig.health_probe_concurrency, 10);
    var testModel = phConfig.test_model || (this._config.models && this._config.models.default) || 'gpt-5.3-codex';
    var now = Date.now();
    var dueEmails = [];
    this._newAccountQueue.forEach(function(item, email) {
      if (!item || !item.addedAt) return;
      if (now - item.addedAt >= delayMs) dueEmails.push(email);
    });

    if (dueEmails.length === 0) {
      this._lastNewAccountVerifyResult = {
        at: new Date().toISOString(),
        tested: 0,
        verified_count: 0,
        wasted_count: 0,
        queue_size: this._newAccountQueue.size,
      };
      return;
    }

    this._newAccountVerifyRunning = true;
    var testedCount = 0;
    var verifiedCount = 0;
    var wastedCount = 0;
    var skippedCount = 0;
    var networkErrorCount = 0;
    var otherFailCount = 0;
    var queue = dueEmails.slice();
    var forceSaveNeeded = false;
    var self = this;

    this._log('New Account Verify: testing ' + dueEmails.length + ' due account(s)');
    try {
      async function worker() {
        while (queue.length > 0) {
          var email = queue.shift();
          var account = self._pool.getFullAccount(email);
          if (!account || !account.accessToken || account.status !== 'active') {
            skippedCount++;
            self._newAccountQueue.delete(email);
            self._log('New Account Verify: skip ' + email + ', account unavailable or not active');
            continue;
          }

          var result = await testOneAccount(account, testModel);
          testedCount++;

          if (result.ok) {
            if (self._pool.setAccountField) {
              self._pool.setAccountField(email, 'verified_at', new Date().toISOString());
            } else {
              account.verified_at = new Date().toISOString();
            }
            verifiedCount++;
            forceSaveNeeded = true;
            self._newAccountQueue.delete(email);
            self._log('New Account Verify: verified ' + email + ' HTTP ' + (result.status || 200));
            continue;
          }

          if (result.networkError) {
            networkErrorCount++;
            self._log('New Account Verify: network error ' + email + ': ' + result.error);
            continue;
          }

          var statusCode = self._asNonNegativeInt(result.status);
          otherFailCount++;
          wastedCount++;
          forceSaveNeeded = true;
          account.last_error_type = statusCode === 429 ? 'rate_limited' : 'verify_failed';
          account.last_error_code = statusCode || 0;
          self._pool.markWasted(email);
          self._newAccountQueue.delete(email);
          self._log('New Account Verify: wasted ' + email + ' HTTP ' + statusCode + ', reason=' + (result.error || (statusCode === 429 ? 'verify_rate_limited' : 'verify_failed')));
          continue;
        }
      }

      var workers = [];
      for (var i = 0; i < Math.min(verifyConcurrency, dueEmails.length); i++) {
        workers.push(worker());
      }
      await Promise.all(workers);

      if (forceSaveNeeded) await this._pool.flush();

      this._lastNewAccountVerifyResult = {
        at: new Date().toISOString(),
        tested: testedCount,
        due_count: dueEmails.length,
        verified_count: verifiedCount,
        wasted_count: wastedCount,
        network_error_count: networkErrorCount,
        failed_count: otherFailCount,
        skipped_count: skippedCount,
        queue_size: this._newAccountQueue.size,
      };
      this._log(
        'New Account Verify complete: tested=' + testedCount +
        ', verified=' + verifiedCount +
        ', wasted=' + wastedCount +
        ', networkErrors=' + networkErrorCount +
        ', failed=' + otherFailCount +
        ', skipped=' + skippedCount +
        ', pending=' + this._newAccountQueue.size
      );
    } catch (e) {
      this._lastNewAccountVerifyResult = {
        at: new Date().toISOString(),
        tested: testedCount,
        due_count: dueEmails.length,
        verified_count: verifiedCount,
        wasted_count: wastedCount,
        network_error_count: networkErrorCount,
        failed_count: otherFailCount,
        skipped_count: skippedCount,
        queue_size: this._newAccountQueue.size,
        error: e && e.message ? e.message : String(e),
      };
      this._log('New Account Verify error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      this._newAccountVerifyRunning = false;
    }
  }

  async _runGuard() {
    var phConfig = this._config.pool_health || {};
    var minActive = this._asPositiveInt(phConfig.pool_guard_min_active, 3000);
    var maxRegister = this._asPositiveInt(phConfig.pool_guard_max_register, 9999);
    var minRegister = this._asPositiveInt(phConfig.pool_guard_min_register, 20);
    var batchSize = this._asPositiveInt(phConfig.pool_guard_batch_size, 20);
    var batchIntervalMs = this._asPositiveInt(phConfig.pool_guard_batch_interval_ms, 3000);
    var batchPollIntervalMs = this._asPositiveInt(phConfig.pool_guard_batch_poll_interval_ms, 10000);
    var batchWaitTimeoutMs = this._asPositiveInt(phConfig.pool_guard_batch_wait_timeout_ms, 300000);
    var totalTimeoutMs = this._asPositiveInt(phConfig.pool_guard_total_timeout_ms, 21600000);
    var statusFailureThreshold = this._asPositiveInt(phConfig.pool_guard_status_failure_threshold, 5);
    var remoteBusyCooldownMs = this._asPositiveInt(phConfig.pool_guard_remote_busy_cooldown_ms, Math.max(batchPollIntervalMs * 3, 60000));
    var remoteErrorCooldownMs = this._asPositiveInt(phConfig.pool_guard_remote_error_cooldown_ms, Math.max(batchPollIntervalMs * 2, 45000));
    var batchTimeoutCooldownMs = this._asPositiveInt(phConfig.pool_guard_batch_timeout_cooldown_ms, Math.max(batchWaitTimeoutMs, 120000));

    var stats = this._pool.getStats();
    var activeCount = stats.active;
    var nowMs = Date.now();

    this._lastGuardTime = new Date().toISOString();

    if (this._guardExecuting) {
      this._lastGuardResult = { active: activeCount, min: minActive, action: 'skipped', reason: 'guard_loop_in_progress' };
      this._logGuard('已有 Guard 循环在执行，跳过本次触发');
      return;
    }
    this._guardExecuting = true;

    try {
      if (this._guardCooldownUntil > nowMs) {
        var cooldownLeftMs = this._guardCooldownUntil - nowMs;
        this._lastGuardResult = {
          active: activeCount,
          min: minActive,
          action: 'skipped',
          reason: 'cooldown_active',
          cooldown_reason: this._guardCooldownReason,
          cooldown_left_ms: cooldownLeftMs,
        };
        this._logGuard(
          '冷却中，跳过本次补号: reason=' + this._guardCooldownReason +
          ', left=' + cooldownLeftMs + 'ms'
        );
        return;
      }

      var preRemoteStatus = await getRegistrationStatus(this._config, {
        ensureServerRunning: false,
        ensureReason: 'pool_guard_precheck_running',
      });
      if (preRemoteStatus && preRemoteStatus.ok && preRemoteStatus.running) {
        this._logGuard('预检查发现注册机任务运行中，等待其完成后继续补号');
        var precheckWaitResult = await this._waitForRegistrationIdle(
          '预检查',
          0,
          batchPollIntervalMs,
          batchWaitTimeoutMs,
          statusFailureThreshold,
          0
        );
        if (precheckWaitResult.totalTimeout || precheckWaitResult.timeout) {
          this._setGuardCooldown(batchTimeoutCooldownMs, 'precheck_wait_timeout');
          this._lastGuardResult = {
            active: activeCount,
            min: minActive,
            action: 'skipped',
            reason: 'precheck_wait_timeout',
            detail: 'waited ' + precheckWaitResult.waited_ms + 'ms',
          };
          this._logGuard('预检查等待注册机空闲超时，停止本轮补号');
          return;
        }
        if (precheckWaitResult.statusUnknown) {
          this._setGuardCooldown(remoteErrorCooldownMs, 'precheck_wait_status_unknown');
          this._lastGuardResult = {
            active: activeCount,
            min: minActive,
            action: 'skipped',
            reason: 'precheck_wait_status_unknown',
            error: precheckWaitResult.error || 'unknown',
          };
          this._logGuard('预检查等待期间状态连续异常，停止本轮补号: ' + (precheckWaitResult.error || 'unknown'));
          return;
        }
        this._logGuard('预检查等待完成，注册机已空闲，继续补号流程');
      }

      var ensureOnlineResult = await ensureServerRunning(this._config, {
        reason: 'pool_guard_round',
      });
      if (!ensureOnlineResult || !ensureOnlineResult.ok) {
        var ensureError = (ensureOnlineResult && ensureOnlineResult.error) || 'unknown';
        this._setGuardCooldown(remoteErrorCooldownMs, 'remote_auto_start_failed');
        this._lastGuardResult = {
          active: activeCount,
          min: minActive,
          action: 'skipped',
          reason: 'remote_auto_start_failed',
          error: ensureError,
        };
        this._logGuard('注册机在线保障失败，跳过补号并进入冷却: ' + ensureError);
        return;
      }

      var remoteStatus = await getRegistrationStatus(this._config, {
        ensureServerRunning: false,
        ensureReason: 'pool_guard_remote_status',
      });
      if (!remoteStatus || !remoteStatus.ok) {
        var statusError = (remoteStatus && remoteStatus.error) || 'unknown';
        this._setGuardCooldown(remoteErrorCooldownMs, 'remote_status_unknown');
        this._lastGuardResult = {
          active: activeCount,
          min: minActive,
          action: 'skipped',
          reason: 'remote_status_unknown',
          error: statusError,
        };
        this._logGuard('无法确认远端任务状态，跳过补号并进入冷却: ' + statusError);
        return;
      }
      if (remoteStatus.running) {
        this._logGuard('补号开始前发现注册机仍在运行，等待其完成后继续');
        var beforeLoopWaitResult = await this._waitForRegistrationIdle(
          '补号前检查',
          0,
          batchPollIntervalMs,
          batchWaitTimeoutMs,
          statusFailureThreshold,
          0
        );
        if (beforeLoopWaitResult.totalTimeout || beforeLoopWaitResult.timeout) {
          this._setGuardCooldown(batchTimeoutCooldownMs, 'preloop_wait_timeout');
          this._lastGuardResult = {
            active: activeCount,
            min: minActive,
            action: 'skipped',
            reason: 'preloop_wait_timeout',
            detail: 'waited ' + beforeLoopWaitResult.waited_ms + 'ms',
          };
          this._logGuard('补号开始前等待注册机空闲超时，停止本轮补号');
          return;
        }
        if (beforeLoopWaitResult.statusUnknown) {
          this._setGuardCooldown(remoteErrorCooldownMs, 'preloop_wait_status_unknown');
          this._lastGuardResult = {
            active: activeCount,
            min: minActive,
            action: 'skipped',
            reason: 'preloop_wait_status_unknown',
            error: beforeLoopWaitResult.error || 'unknown',
          };
          this._logGuard('补号开始前等待期间状态连续异常，停止本轮补号: ' + (beforeLoopWaitResult.error || 'unknown'));
          return;
        }
        this._logGuard('补号开始前等待完成，注册机已空闲');
      }

      if (activeCount >= minActive) {
        this._lastGuardResult = { active: activeCount, min: minActive, action: 'none', reason: 'sufficient' };
        this._logGuard(activeCount + ' active >= ' + minActive + ' min, no action needed');
        return;
      }

      if (this._registering) {
        this._lastGuardResult = {
          active: activeCount,
          min: minActive,
          action: 'skipped',
          reason: 'already_registering',
          registering_since: this._registeringSince,
        };
        this._logGuard('registration already in progress, skipping');
        return;
      }

      var needed = Math.max(0, minActive - activeCount);
      var deficit = needed;
      var buffer = minRegister;
      var toRegister = Math.max(minRegister, needed + buffer);
      var totalBatches = Math.ceil(toRegister / batchSize);

      var guardSelectedProxy = await selectBestProxy(this._config);
      if (!guardSelectedProxy) {
        this._lastGuardResult = {
          active: activeCount,
          min: minActive,
          action: 'skipped',
          reason: 'proxy_unavailable',
        };
        this._logGuard('[PROXY-ROTATE] 无可用代理，跳过本轮补号');
        return;
      }
      var guardProxyPort = 0;
      try {
        var parsedGuardProxy = new URL(guardSelectedProxy);
        guardProxyPort = parsedGuardProxy.port ? (parseInt(parsedGuardProxy.port, 10) || 0) : 0;
      } catch (_) {
        guardProxyPort = 0;
      }
      this._logGuard('[PROXY-ROTATE] 代理预检通过 port=' + (guardProxyPort || 'unknown'));

      this._registering = true;
      this._registeringSince = new Date().toISOString();
      this._clearGuardCooldown();
      this._logGuard(
        'active=' + activeCount +
        ' < min=' + minActive +
        ', deficit=' + deficit +
        ', buffer=' + buffer +
        ', toRegister=' + toRegister +
        ', maxRegister=' + maxRegister +
        ', batchSize=' + batchSize +
        ', batchIntervalMs=' + batchIntervalMs +
        ', batchPollIntervalMs=' + batchPollIntervalMs +
        ', batchWaitTimeoutMs=' + batchWaitTimeoutMs +
        ', totalTimeoutMs=' + totalTimeoutMs +
        ', statusFailureThreshold=' + statusFailureThreshold
      );

      var successCount = 0;
      var failedCount = 0;
      var completedCount = 0;
      var startedAt = Date.now();
      var totalDeadlineMs = startedAt + totalTimeoutMs;
      var stopReason = '';
      var stopDetail = '';

      for (var i = 0; i < totalBatches; i++) {
        if (Date.now() >= totalDeadlineMs) {
          stopReason = 'guard_total_timeout';
          stopDetail = 'guard loop exceeded ' + totalTimeoutMs + 'ms';
          this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
          this._logGuard('触发总超时保护，停止后续补号');
          break;
        }
        var batchNumber = i + 1;
        var batchCount = Math.min(batchSize, toRegister - i * batchSize);
        var batchSuccess = 0;
        var batchFailed = 0;
        this._logGuard('补号批次 ' + batchNumber + '/' + totalBatches + ' 开始，本批 ' + batchCount + ' 个');

        try {
          var beforeBatchStatus = await this._getBatchPreflightStatus(batchNumber);
          if (!beforeBatchStatus || !beforeBatchStatus.ok) {
            stopReason = 'batch_preflight_status_unknown';
            stopDetail = (beforeBatchStatus && beforeBatchStatus.error) || 'unknown';
            this._setGuardCooldown(remoteErrorCooldownMs, stopReason);
            this._logGuard('第 ' + batchNumber + ' 批启动前状态检查连续 3 次失败，停止后续补号: ' + stopDetail);
            break;
          }
          if (beforeBatchStatus.running) {
            this._logGuard('第 ' + batchNumber + ' 批启动前发现注册机运行中，等待上一批完成');
            var remainingPreflightMs = totalDeadlineMs - Date.now();
            if (remainingPreflightMs <= 0) {
              stopReason = 'guard_total_timeout';
              stopDetail = 'guard loop exceeded ' + totalTimeoutMs + 'ms before batch preflight wait';
              this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
              this._logGuard('第 ' + batchNumber + ' 批启动前触发总超时保护，停止后续补号');
              break;
            }
            var preflightWaitResult = await this._waitForRegistrationIdle(
              '批次预检查',
              batchNumber,
              batchPollIntervalMs,
              Math.min(batchWaitTimeoutMs, remainingPreflightMs),
              statusFailureThreshold,
              totalDeadlineMs
            );
            if (preflightWaitResult.totalTimeout) {
              stopReason = 'guard_total_timeout';
              stopDetail = 'guard loop exceeded ' + totalTimeoutMs + 'ms while waiting batch preflight';
              this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
              this._logGuard('第 ' + batchNumber + ' 批启动前等待触发总超时保护，停止后续补号');
              break;
            }
            if (preflightWaitResult.timeout) {
              stopReason = 'batch_preflight_wait_timeout';
              stopDetail = 'waited ' + preflightWaitResult.waited_ms + 'ms for remote idle';
              this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
              this._logGuard('第 ' + batchNumber + ' 批启动前等待远端空闲超时，停止后续补号');
              break;
            }
            if (preflightWaitResult.statusUnknown) {
              stopReason = 'batch_preflight_wait_status_unknown';
              stopDetail = preflightWaitResult.error || 'status unknown';
              this._setGuardCooldown(remoteErrorCooldownMs, stopReason);
              this._logGuard('第 ' + batchNumber + ' 批启动前等待期间状态连续异常，停止后续补号: ' + stopDetail);
              break;
            }
            this._logGuard('第 ' + batchNumber + ' 批启动前等待完成，注册机已空闲，继续启动本批');
          }

          var regResult = await startRegistration(
            this._config,
            { count: batchCount, concurrency: batchSize },
            {
              ensureServerRunning: false,
              ensureReason: 'pool_guard_batch_start',
            }
          );
          if (!regResult.ok) {
            if (regResult.busy) {
              stopReason = 'batch_start_busy';
              stopDetail = regResult.error || 'remote busy';
              this._setGuardCooldown(remoteBusyCooldownMs, stopReason);
              this._logGuard('第 ' + batchNumber + ' 批启动被远端拒绝(忙碌)，停止后续补号: ' + stopDetail);
              break;
            }
            if (regResult.retryable) {
              stopReason = 'batch_start_retryable_error';
              stopDetail = regResult.error || 'retryable start error';
              this._setGuardCooldown(remoteErrorCooldownMs, stopReason);
              this._logGuard('第 ' + batchNumber + ' 批启动出现可重试错误，停止后续补号: ' + stopDetail);
              break;
            }

            batchFailed = batchCount;
            this._logGuard('第 ' + batchNumber + ' 批启动失败: ' + regResult.error);
          } else {
            var startData = regResult.data;
            if (!startData || typeof startData !== 'object' || Array.isArray(startData)) {
              startData = {};
            }
            if (typeof startData.started === 'boolean' && !startData.started) {
              batchFailed = batchCount;
              this._logGuard('第 ' + batchNumber + ' 批启动返回 started=false，按失败计入本批');
            } else {
              this._logGuard(
                '第 ' + batchNumber + ' 批已启动，开始轮询（每 ' + batchPollIntervalMs +
                'ms，超时 ' + Math.floor(batchWaitTimeoutMs / 1000) + 's，受总超时 ' +
                Math.floor(totalTimeoutMs / 1000) + 's 保护）'
              );
              var remainingBatchWaitMs = totalDeadlineMs - Date.now();
              if (remainingBatchWaitMs <= 0) {
                batchFailed = batchCount;
                stopReason = 'guard_total_timeout';
                stopDetail = 'guard loop exceeded ' + totalTimeoutMs + 'ms before batch wait';
                this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
                this._logGuard('第 ' + batchNumber + ' 批启动后触发总超时保护，按失败计入并停止后续补号');
              }
              var effectiveBatchWaitTimeoutMs = Math.min(batchWaitTimeoutMs, Math.max(1, remainingBatchWaitMs));
              var waitResult = null;
              if (!stopReason) {
                waitResult = await this._waitForRegistrationBatch(
                  batchNumber,
                  batchCount,
                  batchPollIntervalMs,
                  effectiveBatchWaitTimeoutMs,
                  statusFailureThreshold,
                  totalDeadlineMs
                );
              }
              if (waitResult && waitResult.totalTimeout) {
                batchFailed = batchCount;
                stopReason = 'guard_total_timeout';
                stopDetail = 'guard loop exceeded ' + totalTimeoutMs + 'ms while waiting batch ' + batchNumber;
                this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
              } else if (waitResult && waitResult.timeout) {
                batchFailed = batchCount;
                stopReason = 'batch_wait_timeout';
                stopDetail = 'batch ' + batchNumber + ' wait timed out';
                this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
              } else if (waitResult && waitResult.statusUnknown) {
                batchFailed = batchCount;
                stopReason = 'batch_wait_status_unknown';
                stopDetail = waitResult.error || 'status unknown';
                this._setGuardCooldown(remoteErrorCooldownMs, stopReason);
              } else if (waitResult) {
                batchSuccess = waitResult.success;
                batchFailed = waitResult.failed;
                var unresolvedCount = batchCount - (batchSuccess + batchFailed);
                if (unresolvedCount > 0) {
                  batchFailed += unresolvedCount;
                  this._logGuard('第 ' + batchNumber + ' 批有 ' + unresolvedCount + ' 个未归类结果，按失败计入');
                } else if (unresolvedCount < 0) {
                  var overflow = -unresolvedCount;
                  batchFailed = Math.max(0, batchFailed - overflow);
                  this._logGuard('第 ' + batchNumber + ' 批结果超出本批数量，已裁剪 ' + overflow + ' 个');
                }
              }
            }
          }
        } catch (e) {
          batchFailed = batchCount;
          stopReason = 'batch_exception';
          stopDetail = e.message || String(e);
          this._setGuardCooldown(remoteErrorCooldownMs, stopReason);
          this._logGuard('第 ' + batchNumber + ' 批异常，停止后续补号: ' + stopDetail);
        }

        successCount += batchSuccess;
        failedCount += batchFailed;
        completedCount += batchCount;
        this._logGuard(
          '第 ' + batchNumber + ' 批完成: 成功 ' + batchSuccess +
          '，失败 ' + batchFailed + '，剩余 ' + (toRegister - completedCount) + ' 个'
        );
        this._logGuard('补号进度: 已完成 ' + completedCount + '/' + toRegister + '，共 ' + totalBatches + ' 批');

        if (stopReason) {
          this._logGuard(
            '检测到停止条件，终止后续批次: reason=' + stopReason +
            (stopDetail ? ', detail=' + stopDetail : '')
          );
          break;
        }

        if (i < totalBatches - 1) {
          var remainingIntervalMs = totalDeadlineMs - Date.now();
          if (remainingIntervalMs <= 0) {
            stopReason = 'guard_total_timeout';
            stopDetail = 'guard loop exceeded ' + totalTimeoutMs + 'ms before next batch interval';
            this._setGuardCooldown(batchTimeoutCooldownMs, stopReason);
            this._logGuard('批次间隔前触发总超时保护，停止后续补号');
            break;
          }
          var intervalWaitMs = Math.min(batchIntervalMs, remainingIntervalMs);
          this._logGuard('第 ' + batchNumber + ' 批结束，等待 ' + intervalWaitMs + 'ms 后开始下一批');
          await this._sleep(intervalWaitMs);
        }
      }

      var durationMs = Date.now() - startedAt;
      var action = 'completed';
      if (completedCount === 0 && stopReason) {
        action = 'skipped';
      } else if (stopReason || failedCount > 0) {
        action = 'partial';
      }
      this._lastGuardResult = {
        active: activeCount,
        min: minActive,
        action: action,
        deficit: deficit,
        planned: toRegister,
        completed: completedCount,
        batches: totalBatches,
        batch_size: batchSize,
        batch_interval_ms: batchIntervalMs,
        batch_poll_interval_ms: batchPollIntervalMs,
        batch_wait_timeout_ms: batchWaitTimeoutMs,
        total_timeout_ms: totalTimeoutMs,
        status_failure_threshold: statusFailureThreshold,
        success: successCount,
        failed: failedCount,
        stopped_reason: stopReason || '',
        stopped_detail: stopDetail || '',
        cooldown_until: this._guardCooldownUntil > 0 ? new Date(this._guardCooldownUntil).toISOString() : null,
        cooldown_reason: this._guardCooldownReason || '',
        duration_ms: durationMs,
      };
      this._logGuard('补号完成: 共计划 ' + toRegister + ' 个，完成 ' + completedCount + ' 个，成功 ' + successCount + ' 个，失败 ' + failedCount + ' 个，耗时 ' + durationMs + 'ms');
      if (this._logCollector) {
        this._logCollector.add('info', '[POOL-GUARD] 补号完成 ' + successCount + '/' + toRegister + '，失败 ' + failedCount);
      }
    } catch (e) {
      this._setGuardCooldown(remoteErrorCooldownMs, 'guard_exception');
      this._lastGuardResult = { active: activeCount, min: minActive, action: 'error', error: e.message || String(e) };
      this._logGuard('registration error: ' + (e.message || e));
    } finally {
      this._registering = false;
      this._registeringSince = null;
      this._guardExecuting = false;
    }
  }

  getStatus() {
    var phConfig = this._config.pool_health || {};
    var stats = this._pool.getStats();
    return {
      enabled: !!(phConfig.enabled),
      probe: {
        enabled: phConfig.probe_enabled !== false,
        running: !!this._probeTimer,
        interval_ms: this._asPositiveInt(
          phConfig.auto_check_interval_ms,
          this._asPositiveInt(phConfig.health_probe_interval_ms, 600000)
        ),
        sample_size: this._asPositiveInt(
          phConfig.auto_check_batch_size,
          this._asPositiveInt(phConfig.health_probe_sample_size, 100)
        ),
        concurrency: this._asPositiveInt(phConfig.health_probe_concurrency, 10),
        last_run: this._lastProbeTime,
        last_result: this._lastProbeResult,
      },
      new_account_verify: {
        enabled: phConfig.new_account_verify_enabled !== false,
        running: !!this._newAccountVerifyTimer,
        executing: this._newAccountVerifyRunning,
        interval_ms: 60000,
        delay_ms: this._asPositiveInt(phConfig.new_account_verify_delay_ms, 300000),
        queue_size: this._newAccountQueue.size,
        last_result: this._lastNewAccountVerifyResult,
      },
      guard: {
        running: !!this._guardTimer || !!this._guardBootstrapTimer,
        executing: this._guardExecuting,
        interval_ms: phConfig.pool_guard_interval_ms || 7200000,
        min_active: this._asPositiveInt(phConfig.pool_guard_min_active, 2000),
        max_register: this._asPositiveInt(phConfig.pool_guard_max_register, 9999),
        min_register: this._asPositiveInt(phConfig.pool_guard_min_register, 20),
        batch_size: this._asPositiveInt(phConfig.pool_guard_batch_size, 20),
        batch_interval_ms: this._asPositiveInt(phConfig.pool_guard_batch_interval_ms, 3000),
        batch_poll_interval_ms: this._asPositiveInt(phConfig.pool_guard_batch_poll_interval_ms, 10000),
        batch_wait_timeout_ms: this._asPositiveInt(phConfig.pool_guard_batch_wait_timeout_ms, 300000),
        total_timeout_ms: this._asPositiveInt(phConfig.pool_guard_total_timeout_ms, 21600000),
        status_failure_threshold: this._asPositiveInt(phConfig.pool_guard_status_failure_threshold, 5),
        remote_busy_cooldown_ms: this._asPositiveInt(phConfig.pool_guard_remote_busy_cooldown_ms, Math.max(this._asPositiveInt(phConfig.pool_guard_batch_poll_interval_ms, 10000) * 3, 60000)),
        remote_error_cooldown_ms: this._asPositiveInt(phConfig.pool_guard_remote_error_cooldown_ms, Math.max(this._asPositiveInt(phConfig.pool_guard_batch_poll_interval_ms, 10000) * 2, 45000)),
        batch_timeout_cooldown_ms: this._asPositiveInt(phConfig.pool_guard_batch_timeout_cooldown_ms, Math.max(this._asPositiveInt(phConfig.pool_guard_batch_wait_timeout_ms, 300000), 120000)),
        current_active: stats.active,
        registering: this._registering,
        registering_since: this._registeringSince,
        cooldown_until: this._guardCooldownUntil > 0 ? new Date(this._guardCooldownUntil).toISOString() : null,
        cooldown_reason: this._guardCooldownReason || '',
        last_run: this._lastGuardTime,
        last_result: this._lastGuardResult,
      },
    };
  }

  async _getBatchPreflightStatus(batchNumber) {
    var retryDelaysMs = [3000, 5000];
    var statusResult = null;
    for (var attempt = 1; attempt <= retryDelaysMs.length + 1; attempt++) {
      try {
        statusResult = await getRegistrationStatus(this._config, {
          ensureServerRunning: false,
          ensureReason: 'pool_guard_batch_preflight',
        });
      } catch (e) {
        statusResult = { ok: false, running: true, error: e && e.message ? e.message : String(e) };
      }

      if (statusResult && statusResult.ok) {
        return statusResult;
      }

      if (attempt <= retryDelaysMs.length) {
        var waitMs = retryDelaysMs[attempt - 1];
        this._logGuard(
          '第 ' + batchNumber + ' 批启动前状态检查失败(' + attempt + '/3): ' +
          ((statusResult && statusResult.error) || 'unknown') +
          '，' + Math.floor(waitMs / 1000) + 's 后重试'
        );
        await this._sleep(waitMs);
      }
    }
    return statusResult;
  }

  async _waitForRegistrationIdle(label, batchNumber, pollIntervalMs, maxWaitMs, statusFailureThreshold, totalDeadlineMs) {
    var startedAt = Date.now();
    var statusFailStreak = 0;
    var name = label || '状态等待';
    while (true) {
      var nowMs = Date.now();
      if (totalDeadlineMs > 0 && nowMs >= totalDeadlineMs) {
        return {
          timeout: true,
          totalTimeout: true,
          statusUnknown: false,
          waited_ms: nowMs - startedAt,
        };
      }
      var waitedMs = nowMs - startedAt;
      if (waitedMs > maxWaitMs) {
        return {
          timeout: true,
          totalTimeout: false,
          statusUnknown: false,
          waited_ms: waitedMs,
        };
      }

      var statusResult;
      try {
        statusResult = await getRegistrationStatus(this._config, {
          ensureServerRunning: false,
          ensureReason: 'pool_guard_wait_idle',
        });
      } catch (e) {
        statusResult = { ok: false, running: true, error: e && e.message ? e.message : String(e) };
      }

      if (!statusResult || !statusResult.ok) {
        var idlePollError = this._extractStatusErrorMessage(statusResult);
        if (this._isStatusPollTimeout(statusResult)) {
          this._logGuard(name + '等待空闲轮询超时（不计失败次数，继续等待）: ' + idlePollError);
        } else {
          statusFailStreak++;
          this._logGuard(
            name + '等待空闲状态失败(' + statusFailStreak + '/' + statusFailureThreshold + '): ' + idlePollError
          );
          if (statusFailStreak >= statusFailureThreshold) {
            return {
              timeout: false,
              totalTimeout: false,
              statusUnknown: true,
              error: idlePollError || 'status polling failed',
              waited_ms: waitedMs,
            };
          }
        }
      } else {
        statusFailStreak = 0;
        this._logGuard(
          name +
          (batchNumber > 0 ? (' 第 ' + batchNumber + ' 批') : '') +
          '等待空闲轮询: running=' + (statusResult.running ? 'true' : 'false')
        );
        if (!statusResult.running) {
          return {
            timeout: false,
            totalTimeout: false,
            statusUnknown: false,
            waited_ms: waitedMs,
          };
        }
      }

      var sleepMs = pollIntervalMs;
      if (totalDeadlineMs > 0) {
        var remainingMs = totalDeadlineMs - Date.now();
        if (remainingMs <= 0) continue;
        sleepMs = Math.min(sleepMs, remainingMs);
      }
      await this._sleep(sleepMs);
    }
  }

  async _waitForRegistrationBatch(batchNumber, batchCount, pollIntervalMs, maxWaitMs, statusFailureThreshold, totalDeadlineMs) {
    var startedAt = Date.now();
    var statusFailStreak = 0;
    while (true) {
      if (totalDeadlineMs > 0 && Date.now() >= totalDeadlineMs) {
        this._logGuard('第 ' + batchNumber + ' 批触发总超时保护，停止等待');
        return { timeout: true, totalTimeout: true, success: 0, failed: batchCount };
      }
      await this._sleep(pollIntervalMs);
      var waitedMs = Date.now() - startedAt;
      var statusResult;
      try {
        statusResult = await getRegistrationStatus(this._config, {
          ensureServerRunning: false,
          ensureReason: 'pool_guard_batch_poll',
        });
      } catch (e) {
        statusResult = { ok: false, running: true, error: e && e.message ? e.message : String(e) };
      }

      if (!statusResult || !statusResult.ok) {
        var pollError = this._extractStatusErrorMessage(statusResult);
        if (this._isStatusPollTimeout(statusResult)) {
          this._logGuard(
            '第 ' + batchNumber + ' 批轮询超时（注册机可能繁忙），不计失败次数，继续等待: ' + pollError
          );
        } else {
          statusFailStreak++;
          var failureType = this._isStatusPollNetworkFailure(statusResult) ? '网络不可达' : '状态检查失败';
          this._logGuard(
            '第 ' + batchNumber + ' 批轮询' + failureType + '(' + statusFailStreak + '/' + statusFailureThreshold + '): ' +
            pollError
          );
          if (statusFailStreak >= statusFailureThreshold) {
            return {
              timeout: false,
              statusUnknown: true,
              success: 0,
              failed: batchCount,
              error: pollError || 'status polling failed',
            };
          }
        }
      } else {
        statusFailStreak = 0;
        var progress = this._extractRegistrationProgress(statusResult.data);
        var total = this._asNonNegativeInt(progress.total);
        var completed = this._asNonNegativeInt(progress.completed);
        var success = this._asNonNegativeInt(progress.success);
        var failed = this._asNonNegativeInt(progress.failed);
        var shownTotal = total > 0 ? total : batchCount;

        this._logGuard(
          '第 ' + batchNumber + ' 批轮询: running=' + (statusResult.running ? 'true' : 'false') +
          '，进度 ' + completed + '/' + shownTotal +
          '，成功 ' + success + '，失败 ' + failed
        );

        if (!statusResult.running) {
          return { timeout: false, success: success, failed: failed };
        }
      }

      if (waitedMs > maxWaitMs) {
        this._logGuard('第 ' + batchNumber + ' 批超时（' + Math.floor(maxWaitMs / 1000) + 's），停止后续批次并进入冷却');
        return { timeout: true, totalTimeout: false, success: 0, failed: batchCount };
      }
    }
  }

  _asPositiveInt(value, fallback) {
    var parsed = parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return parsed;
  }

  _asNonNegativeInt(value) {
    var parsed = parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed < 0) return 0;
    return parsed;
  }

  _extractRegistrationProgress(payload) {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {};
    var candidates = [
      payload.progress,
      payload.job && payload.job.progress,
      payload.current_job && payload.current_job.progress,
      payload.task && payload.task.progress,
      payload.current_task && payload.current_task.progress,
      payload.data && payload.data.progress,
    ];
    for (var i = 0; i < candidates.length; i++) {
      var c = candidates[i];
      if (c && typeof c === 'object' && !Array.isArray(c)) return c;
    }
    return {};
  }

  _extractStatusErrorMessage(statusResult) {
    if (!statusResult || typeof statusResult !== 'object') return 'unknown';
    if (typeof statusResult.error === 'string' && statusResult.error.trim()) return statusResult.error.trim();
    if (typeof statusResult.message === 'string' && statusResult.message.trim()) return statusResult.message.trim();
    return 'unknown';
  }

  _isStatusPollTimeout(statusResult) {
    if (!statusResult || typeof statusResult !== 'object') return false;
    if (statusResult.timeout === true) return true;
    var lower = this._extractStatusErrorMessage(statusResult).toLowerCase();
    return lower.indexOf('timeout') >= 0 || lower.indexOf('timed out') >= 0 || lower.indexOf('aborted') >= 0;
  }

  _isStatusPollNetworkFailure(statusResult) {
    if (!statusResult || typeof statusResult !== 'object') return false;
    if (statusResult.network_error === true) return true;
    var lower = this._extractStatusErrorMessage(statusResult).toLowerCase();
    var markers = [
      'econnrefused',
      'econnreset',
      'ehostunreach',
      'enetunreach',
      'enotfound',
      'eai_again',
      'failed to fetch',
      'fetch failed',
      'network error',
      'socket hang up',
      'connection refused',
    ];
    for (var i = 0; i < markers.length; i++) {
      if (lower.indexOf(markers[i]) >= 0) return true;
    }
    return false;
  }

  _setGuardCooldown(ms, reason) {
    var ttl = this._asPositiveInt(ms, 0);
    if (ttl <= 0) return;
    this._guardCooldownUntil = Date.now() + ttl;
    this._guardCooldownReason = reason || 'unknown';
    this._logGuard('进入冷却: reason=' + this._guardCooldownReason + ', ttl=' + ttl + 'ms');
    this._scheduleGuardRetryAfterCooldown(ttl);
  }

  _clearGuardCooldown() {
    this._guardCooldownUntil = 0;
    this._guardCooldownReason = '';
    this._clearGuardRetryTimer();
  }

  _scheduleGuardRetryAfterCooldown(ttlMs) {
    this._clearGuardRetryTimer();
    var safeDelayMs = this._asPositiveInt(ttlMs, 0);
    if (safeDelayMs <= 0) return;
    this._guardCooldownRetryTimer = setTimeout(() => {
      this._guardCooldownRetryTimer = null;
      var phConfig = this._config.pool_health || {};
      if (!phConfig.enabled) return;
      this._logGuard('冷却结束，立即触发一次补号重试');
      this._runGuard().catch((e) => {
        this._logGuard('冷却后补号重试异常: ' + (e && e.message ? e.message : String(e)));
      });
    }, safeDelayMs + 100);
  }

  _clearGuardRetryTimer() {
    if (!this._guardCooldownRetryTimer) return;
    clearTimeout(this._guardCooldownRetryTimer);
    this._guardCooldownRetryTimer = null;
  }

  _sleep(ms) {
    return new Promise(function(resolve) {
      setTimeout(resolve, ms);
    });
  }

  _logGuard(message) {
    this._log('[POOL-GUARD] ' + message);
  }

  _log(message) {
    var ts = new Date().toISOString();
    console.log('[' + ts + '] [PoolHealthMonitor]', message);
  }
}
