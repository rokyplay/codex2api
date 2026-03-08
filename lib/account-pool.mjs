/**
 * 多账号池管理
 *
 *
 * 功能:
 *   - 从外部账号源导入/导出
 *   - 账号状态追踪（active/cooldown/banned/expired）
 *   - 错误细分处理（不笼统）
 *   - wasted 账号管理
 *   - 用量统计
 */

import { parseJwtExp, parseJwtAuth } from './converter/utils.mjs';

function normalizeErrorText(body) {
  if (typeof body === 'string') return body;
  try {
    return JSON.stringify(body || '');
  } catch (_) {
    return String(body || '');
  }
}

function isUsageLimitError(text) {
  var lower = String(text || '').toLowerCase();
  if (!lower) return false;
  return lower.indexOf('usage limit') >= 0
    || lower.indexOf('upgrade to plus') >= 0
    || lower.indexOf('try again at') >= 0
    || lower.indexOf('rate limit reached') >= 0
    || lower.indexOf('quota exceeded') >= 0
    || lower.indexOf('使用额度') >= 0
    || lower.indexOf('使用上限') >= 0;
}

function isAccountDeactivatedError(text) {
  var lower = String(text || '').toLowerCase();
  if (!lower) return false;
  return lower.indexOf('account_deactivated') >= 0
    || lower.indexOf('deactivated account') >= 0
    || lower.indexOf('account has been deactivated') >= 0
    || lower.indexOf('account disabled') >= 0
    || lower.indexOf('account deleted') >= 0;
}

var CANONICAL_STATUSES = {
  active: 1,
  expired: 1,
  cooldown: 1,
  banned: 1,
  wasted: 1,
};
var DEFAULT_USAGE_LIMIT_COOLDOWN_MS = 30 * 60 * 1000;
var DEFAULT_MAX_CONSECUTIVE_ERRORS_TO_WASTED = 10;

function extractUsageLimitRetryTimestampMs(text) {
  var raw = String(text || '');
  if (!raw) return 0;
  var match = raw.match(/try again at\s+([^\n.]+)/i);
  if (!match || !match[1]) return 0;
  var tsText = match[1].trim().replace(/(\d+)(st|nd|rd|th)/gi, '$1');
  var parsed = Date.parse(tsText);
  if (!isFinite(parsed) || parsed <= Date.now()) return 0;
  return parsed;
}

function toPositiveInt(value, fallback) {
  var n = parseInt(value, 10);
  if (!isFinite(n) || n <= 0) return fallback;
  return n;
}

function toTimestampMs(value) {
  var n = Number(value);
  if (!isFinite(n) || n <= 0) return 0;
  return Math.floor(n);
}

function ensureStatusChangedAt(account) {
  if (!account) return 0;
  var createdAt = toTimestampMs(account.created_at) || Date.now();
  account.created_at = createdAt;
  var statusChangedAt = toTimestampMs(account.status_changed_at);
  if (!statusChangedAt) {
    statusChangedAt = createdAt;
    account.status_changed_at = statusChangedAt;
  } else {
    account.status_changed_at = statusChangedAt;
  }
  return statusChangedAt;
}

function setAccountStatus(account, nextStatus, changedAtMs) {
  if (!account || typeof nextStatus !== 'string' || !nextStatus) return false;
  ensureStatusChangedAt(account);
  if (account.status === nextStatus) return false;
  account.status = nextStatus;
  account.status_changed_at = toTimestampMs(changedAtMs) || Date.now();
  return true;
}

function normalizeAccountStatus(rawStatus) {
  var status = String(rawStatus || '').trim().toLowerCase();
  if (!status) return 'active';
  if (status === 'relogin_needed') return 'wasted';
  if (status === 'usage_limited') return 'cooldown';
  return CANONICAL_STATUSES[status] ? status : 'active';
}

function roundOneDecimal(value) {
  return Math.round(value * 10) / 10;
}

function avgMs(values) {
  if (!Array.isArray(values) || values.length === 0) return 0;
  var sum = 0;
  for (var i = 0; i < values.length; i++) sum += values[i];
  return sum / values.length;
}

function medianMs(values) {
  if (!Array.isArray(values) || values.length === 0) return 0;
  var sorted = values.slice().sort(function (a, b) { return a - b; });
  var mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

/**
 * 错误分类器 — 细分，不笼统
 */
var ERROR_CLASSIFIERS = {
  401: function (body) {
    var text = normalizeErrorText(body);
    if (isUsageLimitError(text)) return 'usage_limited';
    if (isAccountDeactivatedError(text)) return 'account_banned';
    var lower = text.toLowerCase();
    if (lower.indexOf('logged out') >= 0
      || lower.indexOf('signed in to another account') >= 0
      || lower.indexOf('could not be refreshed') >= 0
      || lower.indexOf('invalid session') >= 0
      || lower.indexOf('session invalid') >= 0
      || lower.indexOf('no_session_token') >= 0
      || lower.indexOf('token_invalidated') >= 0
      || lower.indexOf('token has been invalidated') >= 0) {
      return 'session_invalidated';
    }
    return 'token_expired';
  },
  400: function (body) {
    var text = normalizeErrorText(body);
    if (isAccountDeactivatedError(text)) return 'account_banned';
    if (isUsageLimitError(text)) return 'usage_limited';
    return 'bad_request';
  },
  403: function (body) {
    var text = normalizeErrorText(body);
    if (isAccountDeactivatedError(text)) return 'account_banned';
    if (text.indexOf('unusual_activity') >= 0 || text.indexOf('unusual activity') >= 0) return 'ip_blocked';
    if (text.indexOf('mfa') >= 0 || text.indexOf('two-factor') >= 0) return 'mfa_needed';
    return 'account_banned';
  },
  429: function (body) {
    var text = normalizeErrorText(body);
    if (isUsageLimitError(text)) return 'usage_limited';
    return 'rate_limited';
  },
  500: function () { return 'upstream_error'; },
  502: function () { return 'upstream_unavailable'; },
  503: function () { return 'upstream_overloaded'; },
  0: function () { return 'network_error'; },
};

/**
 * 创建单个账号的初始状态
 */
function createAccountState(raw) {
  var now = Date.now();
  var hasRawCreatedAt = !!raw.created_at;
  var hasRawStatusChangedAt = !!raw.status_changed_at;
  var state = {
    email: raw.email || '',
    password: raw.password || '',
    accessToken: raw.accessToken || '',
    sessionToken: raw.sessionToken || '',
    cookies: raw.cookies || {},
    accountId: '',
    status: 'active',
    request_count: 0,
    last_request_at: 0,
    consecutive_errors: 0,
    cooldown_until: 0,
    last_error_code: null,
    last_error_type: null,
    last_error: '',
    session_usage: { input_tokens: 0, output_tokens: 0 },
    token_expires_at: 0,
    created_at: now,
    status_changed_at: now,
    session_invalidated_count: 0,
    usage_limited_count: 0,
    verified_at: '',
    _tokenVersion: 0,
    _lastRefreshAt: 0,
  };

  // 恢复已有的运行时状态（从状态文件加载时保留）
  if (raw.status) {
    // 历史状态映射：relogin_needed -> wasted, usage_limited -> cooldown
    state.status = normalizeAccountStatus(raw.status);
  }
  if (raw.request_count) state.request_count = raw.request_count;
  if (raw.last_request_at) state.last_request_at = raw.last_request_at;
  if (raw.consecutive_errors) state.consecutive_errors = raw.consecutive_errors;
  if (raw.cooldown_until) state.cooldown_until = raw.cooldown_until;
  if (raw.last_error_code !== undefined) state.last_error_code = raw.last_error_code;
  if (raw.last_error_type !== undefined) state.last_error_type = raw.last_error_type;
  if (raw.last_error !== undefined) state.last_error = raw.last_error;
  if (raw.session_usage) state.session_usage = { input_tokens: raw.session_usage.input_tokens || 0, output_tokens: raw.session_usage.output_tokens || 0 };
  if (raw.token_expires_at) state.token_expires_at = raw.token_expires_at;
  if (raw.accountId) state.accountId = raw.accountId;
  if (hasRawCreatedAt) state.created_at = raw.created_at;
  if (hasRawStatusChangedAt) state.status_changed_at = raw.status_changed_at;
  if (raw.session_invalidated_count) state.session_invalidated_count = raw.session_invalidated_count;
  if (raw.usage_limited_count) state.usage_limited_count = raw.usage_limited_count;
  if (raw.verified_at) state.verified_at = raw.verified_at;
  if (typeof raw._tokenVersion === 'number' && isFinite(raw._tokenVersion) && raw._tokenVersion >= 0) {
    state._tokenVersion = raw._tokenVersion;
  }
  if (typeof raw._lastRefreshAt === 'number' && isFinite(raw._lastRefreshAt) && raw._lastRefreshAt > 0) {
    state._lastRefreshAt = raw._lastRefreshAt;
  }

  // 解析 JWT — 没有 token 的账号标为 expired
  if (state.accessToken) {
    // 只有在没有已保存的 token_expires_at 时才解析 JWT
    if (!state.token_expires_at) {
      state.token_expires_at = parseJwtExp(state.accessToken);
    }
    var auth = parseJwtAuth(state.accessToken);
    state.accountId = state.accountId || auth.chatgpt_account_id || '';
    // 无论是否有已保存 status，都检查 token 过期（停机期间可能过期）
    if (state.token_expires_at > 0 && state.token_expires_at < Date.now() / 1000) {
      // 只有 active 状态才降级为 expired（cooldown/banned/wasted 保持原状）
      if (state.status === 'active') {
        setAccountStatus(state, 'expired');
      }
    }
  } else if (!raw.status) {
    setAccountStatus(state, 'expired');
  }
  state.created_at = toTimestampMs(state.created_at) || now;
  if (hasRawStatusChangedAt) {
    state.status_changed_at = toTimestampMs(state.status_changed_at) || 0;
  } else if (!hasRawCreatedAt) {
    state.status_changed_at = state.created_at;
  } else {
    state.status_changed_at = 0;
  }

  return state;
}

function normalizeStorageMode(storage) {
  var raw = storage && typeof storage.mode === 'string' ? storage.mode.trim().toLowerCase() : '';
  if (raw === 'db' || raw === 'dual' || raw === 'file') return 'db';
  return 'db';
}

function resolveStorageConfig(config, overrideStorageConfig) {
  if (overrideStorageConfig && typeof overrideStorageConfig === 'object') {
    return overrideStorageConfig;
  }
  return (config && config.storage && typeof config.storage === 'object') ? config.storage : {};
}

var ACCOUNT_PERSIST_FIELDS = [
  'password',
  'accessToken',
  'sessionToken',
  'cookies',
  'accountId',
  'status',
  'request_count',
  'last_request_at',
  'consecutive_errors',
  'cooldown_until',
  'last_error_code',
  'last_error_type',
  'last_error',
  'session_usage',
  'token_expires_at',
  'created_at',
  'status_changed_at',
  'session_invalidated_count',
  'usage_limited_count',
  '_tokenVersion',
  '_lastRefreshAt',
  'verified_at',
];

/**
 * AccountPool — 多账号池
 */
export class AccountPool {

  constructor(config, i18n) {
    this._config = config || {};
    this._i18n = i18n || {};
    this._accounts = new Map();
    this._wasted = new Map();
    this._scheduler = null;
    this._saveTimer = null;
    this._lastExpiredNotifyAt = 0;
    this._lastLifespanLogAt = 0;
    this._newAccountCallback = null;
    this._storageConfig = resolveStorageConfig(this._config);
    this._storageMode = normalizeStorageMode(this._storageConfig);
    this._repository = null;
    this._dirtyEmails = new Set();
    this._deletedEmails = new Set();
    this._dirtyAll = false;
    this._saveInFlight = false;
    this._saveQueued = false;
    this._currentFlushPromise = null;
    this._readyQueue = [];
    this._readySet = new Set();
  }

  /**
   * 设置调度器
   */
  setScheduler(scheduler) {
    this._scheduler = scheduler;
  }

  /**
   * 注册过期检测回调 — getAccount() 发现过期账号时立刻调用
   */
  onExpiredDetected(fn) {
    this._onExpiredDetected = typeof fn === 'function' ? fn : null;
  }

  /**
   * 兼容旧接口：当前状态机不再进入 relogin_needed
   */
  onReloginNeeded(fn) {
    this._onReloginNeeded = typeof fn === 'function' ? fn : null;
  }

  onNewAccount(callback) {
    this._newAccountCallback = typeof callback === 'function' ? callback : null;
  }

  onNewAccountAdded(callback) {
    this._newAccountCallback = typeof callback === 'function' ? callback : null;
  }

  _emitNewAccount(email) {
    if (!this._newAccountCallback) return;
    try { this._newAccountCallback(email); } catch (_) {}
  }

  _appendEvent(event) {
    if (!event || !event.email) return;
    if (!this._repository || !this._hasRepositoryMode() || typeof this._repository.appendEvent !== 'function') {
      return;
    }
    var detail = event.detail;
    if (detail === undefined || detail === null) {
      detail = {};
    } else if (typeof detail === 'string') {
      detail = { message: detail.substring(0, 500) };
    } else if (typeof detail !== 'object' || Array.isArray(detail)) {
      detail = { value: detail };
    }
    var record = {
      email: String(event.email || '').trim(),
      event_type: String(event.event_type || 'error'),
      old_status: event.old_status === undefined || event.old_status === null ? null : String(event.old_status),
      new_status: event.new_status === undefined || event.new_status === null ? null : String(event.new_status),
      detail: detail,
      created_at_ms: toTimestampMs(event.created_at_ms) || Date.now(),
    };
    if (!record.email || !record.event_type) return;
    this._repository.appendEvent(record).catch(function (e) {
      console.error('[account-pool] repository appendEvent failed:', e && e.message ? e.message : String(e));
    });
  }

  async appendEvent(event) {
    if (!this._repository || !this._hasRepositoryMode() || typeof this._repository.appendEvent !== 'function') {
      return false;
    }
    return this._repository.appendEvent(event || {});
  }

  async appendEventBatch(events) {
    if (!this._repository || !this._hasRepositoryMode()) return { processed: 0, inserted: 0 };
    var list = Array.isArray(events) ? events : [];
    if (list.length === 0) return { processed: 0, inserted: 0 };
    if (typeof this._repository.appendEventBatch === 'function') {
      return this._repository.appendEventBatch(list);
    }
    if (typeof this._repository.appendEvent !== 'function') {
      return { processed: list.length, inserted: 0 };
    }
    var inserted = 0;
    for (var i = 0; i < list.length; i++) {
      var ok = await this._repository.appendEvent(list[i]);
      if (ok) inserted++;
    }
    return { processed: list.length, inserted: inserted };
  }

  _setAccountStatus(account, nextStatus, meta) {
    if (!account || !account.email) return false;
    var previousStatus = String(account.status || '');
    var changedAtMs = meta && meta.changed_at_ms;
    var changed = setAccountStatus(account, nextStatus, changedAtMs);
    if (!changed) return false;
    var payload = {
      from_status: previousStatus,
      to_status: String(nextStatus || ''),
    };
    if (meta && meta.reason) payload.reason = String(meta.reason);
    if (meta && meta.payload && typeof meta.payload === 'object') {
      payload = Object.assign(payload, meta.payload);
    }
    this._appendEvent({
      email: account.email,
      event_type: 'status_change',
      old_status: previousStatus || null,
      new_status: String(nextStatus || ''),
      detail: payload,
      created_at_ms: Date.now(),
    });
    return true;
  }

  _emitErrorEvent(account, statusCode, errorType, action, errorBody) {
    if (!account || !account.email) return;
    var payload = {
      action: String(action || 'none'),
      status: String(account.status || ''),
      consecutive_errors: toPositiveInt(account.consecutive_errors, 0),
    };
    if (account.cooldown_until) payload.cooldown_until = toTimestampMs(account.cooldown_until) || 0;
    if (errorBody !== undefined && errorBody !== null) {
      payload.message = String(errorBody).substring(0, 200);
    }
    payload.status_code = (typeof statusCode === 'number' && isFinite(statusCode)) ? statusCode : null;
    payload.error_type = errorType ? String(errorType) : null;
    this._appendEvent({
      email: account.email,
      event_type: 'error',
      old_status: String(account.status || ''),
      new_status: String(account.status || ''),
      detail: payload,
      created_at_ms: Date.now(),
    });
  }

  _moveToWasted(account, meta) {
    if (!account || !account.email) return false;
    var email = account.email;
    if (this._accounts.has(email)) {
      this._accounts.delete(email);
    }
    this._removeFromReadyQueue(email);
    var changed = this._setAccountStatus(account, 'wasted', meta || {});
    account.accessToken = '';
    account.sessionToken = '';
    account.cookies = {};
    account.accountId = '';
    account.token_expires_at = 0;
    account.cooldown_until = 0;
    this._wasted.set(email, account);
    this._syncReadyForAccount(account);
    return changed;
  }

  /**
   * 设置自动持久化路径
   */
  setSavePath(path) {
    void path;
  }

  getRepository() {
    return this._repository || null;
  }

  async initRepository(storageConfig) {
    this._storageConfig = resolveStorageConfig(this._config, storageConfig);
    this._storageMode = normalizeStorageMode(this._storageConfig);

    if (!this._hasRepositoryMode()) {
      this._repository = null;
      return { mode: this._storageMode, initialized: false };
    }

    var driver = (this._storageConfig && this._storageConfig.driver) || 'sqlite';
    if (driver !== 'sqlite') {
      throw new Error('unsupported_storage_driver:' + driver);
    }

    if (!this._repository) {
      var repositoryModule = await import('./storage/sqlite-account-repository.mjs');
      var RepositoryCtor = repositoryModule.SqliteAccountRepository
        || repositoryModule.SQLiteAccountRepository
        || repositoryModule.default;
      if (typeof RepositoryCtor !== 'function') {
        throw new Error('sqlite_repository_constructor_not_found');
      }

      var sqliteCfg = (this._storageConfig && this._storageConfig.sqlite && typeof this._storageConfig.sqlite === 'object')
        ? this._storageConfig.sqlite
        : {};
      var repoOptions = Object.assign({}, sqliteCfg);
      if (!repoOptions.path && this._storageConfig && typeof this._storageConfig.path === 'string') {
        repoOptions.path = this._storageConfig.path;
      }
      if (!repoOptions.path && this._storageConfig && this._storageConfig.sqlite
        && typeof this._storageConfig.sqlite.path === 'string') {
        repoOptions.path = this._storageConfig.sqlite.path;
      }
      if (!repoOptions.batchSize && this._storageConfig && this._storageConfig.batch_size !== undefined) {
        repoOptions.batchSize = this._storageConfig.batch_size;
      }
      if (!repoOptions.busyTimeoutMs && this._storageConfig && this._storageConfig.busy_timeout_ms !== undefined) {
        repoOptions.busyTimeoutMs = this._storageConfig.busy_timeout_ms;
      }
      if (!repoOptions.journalSizeLimit && this._storageConfig && this._storageConfig.journal_size_limit !== undefined) {
        repoOptions.journalSizeLimit = this._storageConfig.journal_size_limit;
      }

      this._repository = new RepositoryCtor(repoOptions);
    }

    await this._repository.init();

    if (this._storageMode === 'dual') {
      var allAccounts = this._allAccountsArray();
      if (allAccounts.length > 0 && typeof this._repository.upsertBatch === 'function') {
        await this._repository.upsertBatch(allAccounts);
      }
    }

    return { mode: this._storageMode, initialized: true };
  }

  async _loadAllFromRepository() {
    if (!this._repository) {
      return { loaded: 0, active: 0 };
    }

    this._accounts.clear();
    this._wasted.clear();
    this._readyQueue = [];
    this._readySet.clear();

    var list = typeof this._repository.loadAllSync === 'function'
      ? this._repository.loadAllSync()
      : [];

    if (!Array.isArray(list) || list.length === 0) {
      this._rebuildReadyQueue();
      return { loaded: 0, active: 0 };
    }

    for (var i = 0; i < list.length; i++) {
      var state = createAccountState(list[i] || {});
      if (state.status === 'wasted') {
        this._wasted.set(state.email, state);
      } else {
        this._accounts.set(state.email, state);
      }
    }

    this._rebuildReadyQueue();
    return { loaded: list.length, active: this.getActiveCount() };
  }

  async loadFromRepository() {
    return this._loadAllFromRepository();
  }

  async close() {
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
    await this.flush();
    if (this._repository && typeof this._repository.close === 'function') {
      await this._repository.close();
    }
  }

  _hasRepositoryMode() {
    return true;
  }

  _getSaveDelayMs() {
    if (!this._hasRepositoryMode()) return 2000;
    return toPositiveInt(this._storageConfig && this._storageConfig.flush_interval_ms, 5000);
  }

  _activeAccountsArray() {
    return Array.from(this._accounts.values());
  }

  _wastedAccountsArray() {
    return Array.from(this._wasted.values());
  }

  _allAccountsArray() {
    return this._activeAccountsArray().concat(this._wastedAccountsArray());
  }

  _markDirty(email, opts) {
    var options = opts || {};
    if (typeof email !== 'string' || !email) {
      this._dirtyAll = true;
      return;
    }
    if (options.deleted) {
      this._deletedEmails.add(email);
      this._dirtyEmails.delete(email);
      return;
    }
    this._deletedEmails.delete(email);
    this._dirtyEmails.add(email);
  }

  _buildPersistFields(account) {
    var fields = {};
    if (!account || typeof account !== 'object') return fields;
    for (var i = 0; i < ACCOUNT_PERSIST_FIELDS.length; i++) {
      var key = ACCOUNT_PERSIST_FIELDS[i];
      fields[key] = account[key];
    }
    if (!fields.session_usage || typeof fields.session_usage !== 'object') {
      fields.session_usage = { input_tokens: 0, output_tokens: 0 };
    }
    return fields;
  }

  async _writeDirtyAccountsToRepository() {
    if (!this._hasRepositoryMode() || !this._repository) {
      this._dirtyEmails.clear();
      this._deletedEmails.clear();
      this._dirtyAll = false;
      return;
    }

    var hasDeleteFailures = false;
    var deleted = Array.from(this._deletedEmails);
    for (var i = 0; i < deleted.length; i++) {
      var deletedEmail = deleted[i];
      try {
        await this._repository.delete(deletedEmail);
        this._deletedEmails.delete(deletedEmail);
      } catch (e) {
        hasDeleteFailures = true;
        console.error('[account-pool] repository delete failed:', deletedEmail, e && e.message ? e.message : String(e));
      }
    }

    var targets = [];
    if (this._dirtyAll) {
      targets = this._allAccountsArray();
    } else {
      var dirtyEmails = Array.from(this._dirtyEmails);
      for (var j = 0; j < dirtyEmails.length; j++) {
        var email = dirtyEmails[j];
        var acc = this._findByEmail(email) || this._findInWasted(email);
        if (acc) targets.push(acc);
      }
    }

    if (targets.length === 0) {
      if (this._dirtyAll && !hasDeleteFailures) {
        this._dirtyAll = false;
      }
      if (!this._dirtyAll) {
        this._dirtyEmails.clear();
      }
      return;
    }

    var hasUpdateFailures = false;
    if (typeof this._repository.updateFieldsBatch === 'function') {
      var batchItems = [];
      for (var k = 0; k < targets.length; k++) {
        var target = targets[k];
        batchItems.push({ email: target.email, fields: this._buildPersistFields(target) });
      }
      try {
        var batchResult = await this._repository.updateFieldsBatch(batchItems);
        var failedSet = new Set();
        if (batchResult && Array.isArray(batchResult.failed)) {
          for (var f = 0; f < batchResult.failed.length; f++) {
            var failedEmail = String(batchResult.failed[f] || '').trim();
            if (failedEmail) failedSet.add(failedEmail);
          }
        }
        hasUpdateFailures = failedSet.size > 0;
        if (this._dirtyAll) {
          if (!hasUpdateFailures) {
            this._dirtyAll = false;
            this._dirtyEmails.clear();
          }
        } else {
          for (var u = 0; u < batchItems.length; u++) {
            var updatedEmail = batchItems[u].email;
            if (!failedSet.has(updatedEmail)) {
              this._dirtyEmails.delete(updatedEmail);
            }
          }
        }
      } catch (e2) {
        hasUpdateFailures = true;
        console.error('[account-pool] repository updateFieldsBatch failed:', e2 && e2.message ? e2.message : String(e2));
      }
    } else {
      for (var m = 0; m < targets.length; m++) {
        var account = targets[m];
        try {
          await this._repository.updateFields(account.email, this._buildPersistFields(account));
          if (!this._dirtyAll) this._dirtyEmails.delete(account.email);
        } catch (e3) {
          hasUpdateFailures = true;
          console.error('[account-pool] repository updateFields failed:', account && account.email, e3 && e3.message ? e3.message : String(e3));
        }
      }
      if (this._dirtyAll && !hasUpdateFailures) {
        this._dirtyAll = false;
        this._dirtyEmails.clear();
      }
    }

    if (!hasDeleteFailures && !hasUpdateFailures) {
      this._deletedEmails.clear();
    }
  }

  /**
   * 防抖自动保存（2 秒内无新变更才写盘）
   */
  _scheduleSave(email, opts) {
    this._markDirty(email, opts);
    if (!this._hasRepositoryMode()) return;
    if (this._saveTimer) clearTimeout(this._saveTimer);
    var self = this;
    this._saveTimer = setTimeout(function () {
      self._saveTimer = null;
      self.flush().catch(function (e) {
        console.error('[account-pool] scheduled flush failed:', e && e.message ? e.message : String(e));
      });
    }, this._getSaveDelayMs());
  }

  /**
   * 强制立即保存（兼容旧调用）
   */
  forceSave() {
    return this.flush();
  }

  /**
   * 立即 flush（供 async 调用方）
   */
  async flush() {
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
    if (this._saveInFlight && this._currentFlushPromise) {
      this._saveQueued = true;
      await this._currentFlushPromise;
      if (this._saveQueued) {
        this._saveQueued = false;
        await this.flush();
      }
      return;
    }

    this._saveInFlight = true;
    this._currentFlushPromise = (async function (self) {
      if (self._hasRepositoryMode()) {
        await self._writeDirtyAccountsToRepository();
        if (self._repository && typeof self._repository.flush === 'function') {
          await self._repository.flush();
        }
      }
    })(this);

    try {
      await this._currentFlushPromise;
    } finally {
      this._saveInFlight = false;
      this._currentFlushPromise = null;
    }

    if (this._saveQueued) {
      this._saveQueued = false;
      await this.flush();
    }
  }

  /**
   * 导出全部账号（含状态 + 废弃账号）
   */
  exportAccounts(path) {
    void path;
    var allAccounts = this._allAccountsArray();
    var data = allAccounts.map(function (a) {
      var entry = {
        email: a.email,
        accessToken: a.accessToken,
        sessionToken: a.sessionToken,
        cookies: a.cookies,
        status: a.status,
        request_count: a.request_count,
        session_usage: a.session_usage,
        usage_limited_count: a.usage_limited_count || 0,
      };
      if (a.password) entry.password = a.password;
      return entry;
    });
    return data;
  }

  /**
   * 运行时添加账号（浏览器登录后动态添加）
   */
  addAccount(raw) {
    // 先检查 _wasted 中是否有同 email（恢复已废弃的账号）
    var wastedAccount = this._findInWasted(raw.email);
    if (wastedAccount) {
      // 从废弃池移回活跃池
      this._wasted.delete(raw.email);
      wastedAccount.accessToken = raw.accessToken || wastedAccount.accessToken;
      wastedAccount.sessionToken = raw.sessionToken || wastedAccount.sessionToken;
      wastedAccount.cookies = raw.cookies || wastedAccount.cookies;
      wastedAccount.password = raw.password || wastedAccount.password;
      this._setAccountStatus(wastedAccount, 'active', { reason: 'import_recover_from_wasted' });
      wastedAccount.consecutive_errors = 0;
      wastedAccount.cooldown_until = 0;
      wastedAccount.last_error_code = null;
      wastedAccount.last_error_type = null;
      wastedAccount.last_error = '';
      wastedAccount.session_invalidated_count = 0;
      if (wastedAccount.accessToken) {
        wastedAccount.token_expires_at = parseJwtExp(wastedAccount.accessToken);
        var wastedAuth = parseJwtAuth(wastedAccount.accessToken);
        wastedAccount.accountId = wastedAuth.chatgpt_account_id || '';
      }
      this._accounts.set(wastedAccount.email, wastedAccount);
      this._syncReadyForAccount(wastedAccount);
      this._emitNewAccount(raw.email);
      this._appendEvent({
        email: wastedAccount.email,
        event_type: 'credential_update',
        old_status: null,
        new_status: String(wastedAccount.status || ''),
        detail: { source: 'addAccount', operation: 'recover_from_wasted' },
        created_at_ms: Date.now(),
      });
      this._markDirty(wastedAccount.email);
      if (this._repository && this._hasRepositoryMode()) {
        this._repository.upsert(wastedAccount).catch(function (e) {
          console.error('[account-pool] repository upsert failed:', e && e.message ? e.message : String(e));
        });
      }
      this._scheduleSave(wastedAccount.email);
      return wastedAccount;
    }

    if (this._findByEmail(raw.email)) {
      // 已存在，更新 token
      var existing = this._findByEmail(raw.email);
      existing.accessToken = raw.accessToken || existing.accessToken;
      existing.sessionToken = raw.sessionToken || existing.sessionToken;
      existing.cookies = raw.cookies || existing.cookies;
      existing.password = raw.password || existing.password;
      this._setAccountStatus(existing, 'active', { reason: 'import_refresh_existing' });
      existing.consecutive_errors = 0;
      existing.cooldown_until = 0;
      existing.last_error_code = null;
      existing.last_error_type = null;
      existing.last_error = '';
      existing.session_invalidated_count = 0;
      if (existing.accessToken) {
        existing.token_expires_at = parseJwtExp(existing.accessToken);
        var auth = parseJwtAuth(existing.accessToken);
        existing.accountId = auth.chatgpt_account_id || '';
      }
      this._syncReadyForAccount(existing);
      this._emitNewAccount(raw.email);
      this._appendEvent({
        email: existing.email,
        event_type: 'credential_update',
        old_status: null,
        new_status: String(existing.status || ''),
        detail: { source: 'addAccount', operation: 'update_existing' },
        created_at_ms: Date.now(),
      });
      this._markDirty(existing.email);
      if (this._repository && this._hasRepositoryMode()) {
        this._repository.upsert(existing).catch(function (e2) {
          console.error('[account-pool] repository upsert failed:', e2 && e2.message ? e2.message : String(e2));
        });
      }
      this._scheduleSave(existing.email);
      return existing;
    }
    var state = createAccountState(raw);
    this._accounts.set(state.email, state);
    this._syncReadyForAccount(state);
    this._emitNewAccount(raw.email);
    this._appendEvent({
      email: state.email,
      event_type: 'credential_update',
      old_status: null,
      new_status: String(state.status || ''),
      detail: { source: 'addAccount', operation: 'create' },
      created_at_ms: Date.now(),
    });
    this._markDirty(state.email);
    if (this._repository && this._hasRepositoryMode()) {
      this._repository.upsert(state).catch(function (e3) {
        console.error('[account-pool] repository upsert failed:', e3 && e3.message ? e3.message : String(e3));
      });
    }
    this._scheduleSave(state.email);
    return state;
  }

  /**
   * 彻底删除账号（从活跃池和废弃池中移除）
   */
  removeAccount(email) {
    var found = false;
    if (this._accounts.has(email)) {
      this._accounts.delete(email);
      this._removeFromReadyQueue(email);
      found = true;
    }
    if (!found && this._wasted.has(email)) {
      this._wasted.delete(email);
      this._removeFromReadyQueue(email);
      found = true;
    }
    if (found) {
      this._appendEvent({
        email: email,
        event_type: 'credential_update',
        old_status: null,
        new_status: null,
        detail: { source: 'removeAccount', operation: 'delete' },
        created_at_ms: Date.now(),
      });
      this._scheduleSave(email, { deleted: true });
    }
    return found;
  }

  /**
   * 总账号数（含 wasted）
   */
  getTotalCount() {
    return this._accounts.size + this._wasted.size;
  }

  /**
   * 活跃账号数
   */
  getActiveCount() {
    return this._activeAccountsArray().filter(function (a) { return a.status === 'active'; }).length;
  }

  /**
   * 获取可用账号（通过调度器选择）
   */
  getAccount() {
    this._recoverCooldowns();
    if (this._readyQueue.length === 0) {
      this._rebuildReadyQueue();
    }

    var queueResult = this._pickAccountFromReadyQueue();
    var finalResult = queueResult;
    if (!queueResult.account) {
      var fallback = this._getAccountByScanFallback();
      finalResult = {
        account: fallback.account,
        foundExpired: !!(queueResult.foundExpired || fallback.foundExpired),
        foundNearExpiry: !!(queueResult.foundNearExpiry || fallback.foundNearExpiry),
        foundDemoted: !!(queueResult.foundDemoted || fallback.foundDemoted),
      };
    }

    if (finalResult.foundExpired || finalResult.foundDemoted) {
      this._scheduleSave();
    }
    if (finalResult.foundExpired || finalResult.foundNearExpiry) {
      this._notifyExpiredDetected();
    }

    return finalResult.account || null;
  }

  /**
   * 标记请求成功
   */
  markSuccess(email, usage) {
    var account = this._findByEmail(email);
    if (!account) return;

    account.request_count++;
    account.last_request_at = Date.now();
    account.consecutive_errors = 0;
    account.last_error_code = null;
    account.last_error_type = null;
    account.session_invalidated_count = 0;

    if (usage) {
      account.session_usage.input_tokens += usage.input_tokens || 0;
      account.session_usage.output_tokens += usage.output_tokens || 0;
    }

    this._syncReadyForAccount(account);
    this._scheduleSave(email);
  }

  /**
   * 标记请求失败 — 细分错误类型
   *
   *
   * @returns {{ type: string, action: string }}
   */
  markError(email, statusCode, errorBody) {
    var account = this._findByEmail(email);
    if (!account) return { type: 'unknown', action: 'none' };

    account.consecutive_errors++;
    var codeNum = Number(statusCode);
    var code = isFinite(codeNum) ? Math.trunc(codeNum) : 0;
    account.last_error_code = code;

    // 分类错误
    var classifier = ERROR_CLASSIFIERS[code];
    var errorType = classifier ? classifier(errorBody) : 'unknown';
    account.last_error_type = errorType;
    account.last_error = errorBody ? String(errorBody).substring(0, 200) : '';

    var action = 'retry';
    var nowMs = Date.now();
    var statusChangedMeta = { status_code: code, error_type: errorType };

    switch (errorType) {
      case 'token_expired':
        this._setAccountStatus(account, 'expired', Object.assign({ reason: 'token_expired' }, statusChangedMeta));
        account.cooldown_until = 0;
        action = 'refresh_token';
        this._notifyExpiredDetected();
        break;

      case 'session_invalidated':
        account.session_invalidated_count = (account.session_invalidated_count || 0) + 1;
        this._moveToWasted(account, Object.assign({
          reason: 'session_invalidated',
          payload: { session_invalidated_count: account.session_invalidated_count },
        }, statusChangedMeta));
        action = 'mark_wasted';
        break;

      case 'ip_blocked':
        this._setAccountStatus(account, 'cooldown', Object.assign({ reason: 'ip_blocked' }, statusChangedMeta));
        account.cooldown_until = nowMs + 60000;
        action = 'switch_account';
        break;

      case 'mfa_needed':
        this._setAccountStatus(account, 'cooldown', Object.assign({ reason: 'mfa_needed' }, statusChangedMeta));
        account.cooldown_until = nowMs + 3600000;
        action = 'manual_intervention';
        break;

      case 'account_banned':
        this._setAccountStatus(account, 'banned', Object.assign({ reason: 'account_banned' }, statusChangedMeta));
        account.cooldown_until = 0;
        action = 'disable_account';
        break;

      case 'rate_limited':
        this._setAccountStatus(account, 'cooldown', Object.assign({ reason: 'rate_limited' }, statusChangedMeta));
        var cooldownMs = (this._config.rate_limit && this._config.rate_limit.cooldown_ms) || 300000;
        var rateLimitRetryAt = extractUsageLimitRetryTimestampMs(errorBody);
        if (rateLimitRetryAt > nowMs) {
          account.cooldown_until = rateLimitRetryAt;
        } else {
          account.cooldown_until = nowMs + cooldownMs;
        }
        action = 'switch_account';
        break;

      case 'usage_limited':
        account.usage_limited_count = (account.usage_limited_count || 0) + 1;
        this._setAccountStatus(account, 'cooldown', Object.assign({ reason: 'usage_limited' }, statusChangedMeta));
        var usageRetryAt = extractUsageLimitRetryTimestampMs(errorBody);
        account.cooldown_until = usageRetryAt > nowMs ? usageRetryAt : (nowMs + DEFAULT_USAGE_LIMIT_COOLDOWN_MS);
        action = 'switch_account';
        break;

      case 'bad_request':
        account.consecutive_errors = Math.max(0, account.consecutive_errors - 1);
        action = 'ignore_request_error';
        break;

      case 'upstream_error':
      case 'upstream_unavailable':
      case 'upstream_overloaded':
        action = 'retry';
        break;

      case 'network_error':
        action = 'retry';
        break;

      default:
        this._setAccountStatus(account, 'cooldown', Object.assign({ reason: 'unknown_error' }, statusChangedMeta));
        account.cooldown_until = nowMs + 60000;
        action = 'retry';
    }

    if (account.status !== 'wasted' && account.consecutive_errors >= this._getMaxConsecutiveErrorsToWasted()) {
      this._moveToWasted(account, {
        reason: 'consecutive_errors_exceeded',
        status_code: code,
        error_type: errorType,
        payload: { consecutive_errors: account.consecutive_errors },
      });
      action = 'mark_wasted';
    }

    this._emitErrorEvent(account, code, errorType, action, errorBody);
    this._syncReadyForAccount(account);
    this._scheduleSave(email);
    return { type: errorType, action: action };
  }

  /**
   * 标记 token 刷新可重试失败（网络/5xx 等）
   *
   * 目标：保持账号为 expired，交给下一轮 refreshAll() 自动重试
   */
  markRefreshRetryableFailure(email, statusCode, detail) {
    var account = this._findByEmail(email);
    if (!account) return { updated: false, reason: 'account_not_found' };

    var code = (typeof statusCode === 'number' && isFinite(statusCode)) ? statusCode : 0;
    var detailText = '';
    if (typeof detail === 'string') {
      detailText = detail;
    } else if (detail) {
      try {
        detailText = JSON.stringify(detail);
      } catch (_) {
        detailText = '';
      }
    }

    var errorType = 'refresh_retryable';
    if (code === 0) {
      errorType = 'network_error';
    } else if (code === 500) {
      errorType = 'upstream_error';
    } else if (code === 502) {
      errorType = 'upstream_unavailable';
    } else if (code === 503) {
      errorType = 'upstream_overloaded';
    } else if (detailText) {
      var lower = detailText.toLowerCase();
      if (lower.indexOf('timeout') >= 0
        || lower.indexOf('network') >= 0
        || lower.indexOf('econn') >= 0) {
        errorType = 'network_error';
      }
    }

    this._setAccountStatus(account, 'expired', {
      reason: 'refresh_retryable_failure',
      status_code: code,
      error_type: errorType,
    });
    account.cooldown_until = 0;
    account.last_error_code = code;
    account.last_error_type = errorType;
    account.last_error = detailText ? String(detailText).substring(0, 200) : '';
    this._emitErrorEvent(account, code, errorType, 'refresh_retry', detailText);
    this._syncReadyForAccount(account);
    this._scheduleSave(email);
    return { updated: true, type: errorType };
  }

  /**
   */
  markWasted(email) {
    var account = this._findByEmail(email) || this._findInWasted(email);
    if (!account) return;
    this._moveToWasted(account, { reason: 'manual_mark_wasted' });
    this._scheduleSave(email);
  }

  /**
   * 更新账号的 token（刷新后调用）
   */
  updateToken(email, newAccessToken, newSessionToken) {
    var account = this._findByEmail(email);
    if (!account) return;
    var changed = false;
    var currentVersion = typeof account._tokenVersion === 'number' ? account._tokenVersion : 0;

    if (newAccessToken) {
      account.accessToken = newAccessToken;
      account.token_expires_at = parseJwtExp(newAccessToken);
      var auth = parseJwtAuth(newAccessToken);
      account.accountId = auth.chatgpt_account_id || '';
      changed = true;
    }
    if (newSessionToken) {
      account.sessionToken = newSessionToken;
      changed = true;
    }
    var statusChanged = this._setAccountStatus(account, 'active', { reason: 'token_refresh' });
    account.consecutive_errors = 0;
    account.cooldown_until = 0;
    account.last_error_code = null;
    account.last_error_type = null;
    account.last_error = '';
    account.session_invalidated_count = 0;
    if (changed) {
      account._tokenVersion = currentVersion + 1;
    } else {
      account._tokenVersion = currentVersion;
    }
    if (changed || statusChanged) {
      this._appendEvent({
        email: account.email,
        event_type: 'token_refresh',
        old_status: null,
        new_status: String(account.status || ''),
        detail: {
          source: 'updateToken',
          changed: !!changed,
          status_changed: !!statusChanged,
          token_version: account._tokenVersion,
        },
        created_at_ms: Date.now(),
      });
    }
    this._syncReadyForAccount(account);
    this._scheduleSave(email);
  }

  /**
   * 带版本保护地写回 refresh 结果，防止并发刷新覆盖
   *
   * @param {object} account
   * @param {object} refreshResult
   * @param {number} expectedVersion
   * @returns {{ applied: boolean, reason?: string, version?: number, currentVersion?: number }}
   */
  applyRefreshResultCAS(account, refreshResult, expectedVersion) {
    if (!account) return { applied: false, reason: 'account_not_found' };
    if (!refreshResult || !refreshResult.success) return { applied: false, reason: 'invalid_refresh_result' };

    var currentVersion = typeof account._tokenVersion === 'number' ? account._tokenVersion : 0;
    if (typeof expectedVersion === 'number' && currentVersion !== expectedVersion) {
      return { applied: false, reason: 'stale_version', currentVersion: currentVersion };
    }

    if (refreshResult.accessToken) {
      account.accessToken = refreshResult.accessToken;
      account.token_expires_at = refreshResult.tokenExpiresAt || parseJwtExp(refreshResult.accessToken);
      var auth = parseJwtAuth(refreshResult.accessToken);
      account.accountId = refreshResult.accountId || auth.chatgpt_account_id || '';
    }
    if (refreshResult.sessionToken) {
      account.sessionToken = refreshResult.sessionToken;
    }
    var statusChanged = this._setAccountStatus(account, 'active', { reason: 'token_refresh' });
    account.consecutive_errors = 0;
    account.cooldown_until = 0;
    account.last_error_code = null;
    account.last_error_type = null;
    account.last_error = '';
    account.session_invalidated_count = 0;
    account._lastRefreshAt = Date.now();
    account._tokenVersion = currentVersion + 1;
    this._appendEvent({
      email: account.email,
      event_type: 'token_refresh',
      old_status: null,
      new_status: String(account.status || ''),
      detail: {
        source: 'applyRefreshResultCAS',
        status_changed: !!statusChanged,
        token_version: account._tokenVersion,
      },
      created_at_ms: Date.now(),
    });
    this._syncReadyForAccount(account);
    this._scheduleSave(account.email);
    return { applied: true, version: account._tokenVersion };
  }

  /**
   * 激活废弃账号（从 _wasted 移回 _accounts）
   */
  activateWasted(email) {
    var account = this._findInWasted(email);
    if (!account) return null;
    this._wasted.delete(email);
    this._setAccountStatus(account, 'active', { reason: 'manual_activate_wasted' });
    account.consecutive_errors = 0;
    account.cooldown_until = 0;
    account.last_error_code = null;
    account.last_error_type = null;
    account.last_error = '';
    account.session_invalidated_count = 0;
    this._accounts.set(email, account);
    this._syncReadyForAccount(account);
    this._scheduleSave(email);
    return account;
  }

  /**
   * 启动自愈：把可恢复的 wasted 账号迁回自动恢复队列
   *
   * 规则：
   * - 统一迁回 expired（交给 refreshAll 重试）
   */
  recoverWastedAccounts() {
    // wasted 为终态，不做自动恢复
    return 0;
  }

  /**
   * 获取所有需要刷新 token 的账号
   */
  getExpiredAccounts(beforeExpirySec) {
    beforeExpirySec = beforeExpirySec || 300;
    // 定时刷新路径也要恢复冷却，避免仅依赖 getAccount() 才解锁
    this._recoverCooldowns();
    var nowMs = Date.now();
    var threshold = Math.floor(nowMs / 1000) + beforeExpirySec;
    var refreshRetryIntervalMs = (this._config && this._config.refresh_retry_interval_ms) || 55000;

    return this._activeAccountsArray().filter(function (a) {
      if (a.status !== 'active' && a.status !== 'expired') return false;
      if (a.cooldown_until && a.cooldown_until > nowMs) return false;
      // 节流：最近刚刷新过的账号跳过（避免并发/抖动导致的重复刷新）
      // session_invalidated 类型的 expired 也要节流，避免无限快速重试已死 session
      if (a._lastRefreshAt && nowMs - a._lastRefreshAt < refreshRetryIntervalMs) {
        // 只有 active + 非 session_invalidated 的旧逻辑允许跳过节流
        // 现在统一节流，防止 session_invalidated → expired → 立刻重试 的死循环
        return false;
      }
      // 已标记 expired 的账号无论 token_expires_at 如何都需要刷新
      if (a.status === 'expired') return true;
      // active 账号：token 即将过期时提前刷新
      return a.token_expires_at > 0 && a.token_expires_at < threshold;
    });
  }

  /**
   * 获取所有可验证的失效账号（expired + wasted 中有 sessionToken 的）
   */
  getVerifiableAccounts() {
    var result = [];
    var active = this._activeAccountsArray();
    for (var i = 0; i < active.length; i++) {
      var a = active[i];
      if (a.status === 'expired' && a.sessionToken) {
        result.push(a);
      }
    }
    var wasted = this._wastedAccountsArray();
    for (var j = 0; j < wasted.length; j++) {
      var w = wasted[j];
      if (w.sessionToken) {
        result.push(w);
      }
    }
    return result;
  }

  /**
   * 获取账号编号映射 (email → 1-based index)
   */
  getAccountIndexMap() {
    var map = {};
    var active = this._activeAccountsArray();
    var wasted = this._wastedAccountsArray();
    for (var i = 0; i < active.length; i++) {
      map[active[i].email] = i + 1;
    }
    for (var j = 0; j < wasted.length; j++) {
      map[wasted[j].email] = active.length + j + 1;
    }
    return map;
  }

  /**
   * 全池统计
   */
  getStats() {
    this._recoverCooldowns();
    var active = this._activeAccountsArray();
    var stats = { total: this._accounts.size + this._wasted.size, active: 0, cooldown: 0, banned: 0, expired: 0, wasted: this._wasted.size };
    for (var i = 0; i < active.length; i++) {
      var s = active[i].status;
      if (s === 'active') stats.active++;
      else if (s === 'cooldown') stats.cooldown++;
      else if (s === 'banned') stats.banned++;
      else if (s === 'expired') stats.expired++;
    }
    return stats;
  }

  /**
   * 账号寿命统计（从加入池到失效状态）
   *
   * dead 状态：banned / wasted / expired
   */
  getLifespanStats() {
    this._recoverCooldowns();
    var nowMs = Date.now();
    var deadStatuses = { banned: true, wasted: true, expired: true };
    var allAccounts = this._allAccountsArray();
    var deadDurations = [];
    var aliveDurations = [];
    var byStatusDurations = { banned: [], wasted: [], expired: [] };
    var deadSourceCount = {
      status_changed_at: 0,
      last_request_at: 0,
      last_refresh_at: 0,
      now: 0,
    };

    for (var i = 0; i < allAccounts.length; i++) {
      var account = allAccounts[i];
      if (!account) continue;
      var joinAt = toTimestampMs(account.created_at);
      if (!joinAt) continue;
      var status = account.status || '';

      if (deadStatuses[status]) {
        var endAt = 0;
        var source = '';
        var changedAt = toTimestampMs(account.status_changed_at);
        if (changedAt >= joinAt) {
          endAt = changedAt;
          source = 'status_changed_at';
        }

        if (!endAt) {
          var lastRequestAt = toTimestampMs(account.last_request_at);
          if (lastRequestAt >= joinAt) {
            endAt = lastRequestAt;
            source = 'last_request_at';
          }
        }

        if (!endAt) {
          var lastRefreshAt = toTimestampMs(account._lastRefreshAt);
          if (lastRefreshAt >= joinAt) {
            endAt = lastRefreshAt;
            source = 'last_refresh_at';
          }
        }

        if (!endAt) {
          endAt = nowMs;
          source = 'now';
        }

        var lifespanMs = Math.max(0, endAt - joinAt);
        deadDurations.push(lifespanMs);
        if (byStatusDurations[status]) byStatusDurations[status].push(lifespanMs);
        deadSourceCount[source] = (deadSourceCount[source] || 0) + 1;
      } else {
        var aliveAgeMs = Math.max(0, nowMs - joinAt);
        aliveDurations.push(aliveAgeMs);
      }
    }

    var avgLifespanMs = avgMs(deadDurations);
    var medianLifespanMs = medianMs(deadDurations);
    var minLifespanMs = deadDurations.length > 0 ? Math.min.apply(null, deadDurations) : 0;
    var maxLifespanMs = deadDurations.length > 0 ? Math.max.apply(null, deadDurations) : 0;
    var avgAliveMs = avgMs(aliveDurations);
    var byStatusAvgHours = {
      banned: byStatusDurations.banned.length > 0 ? roundOneDecimal(avgMs(byStatusDurations.banned) / 3600000) : null,
      wasted: byStatusDurations.wasted.length > 0 ? roundOneDecimal(avgMs(byStatusDurations.wasted) / 3600000) : null,
      expired: byStatusDurations.expired.length > 0 ? roundOneDecimal(avgMs(byStatusDurations.expired) / 3600000) : null,
    };

    var result = {
      avg_lifespan_ms: Math.round(avgLifespanMs),
      avg_lifespan_hours: roundOneDecimal(avgLifespanMs / 3600000),
      median_lifespan_hours: roundOneDecimal(medianLifespanMs / 3600000),
      min_lifespan_hours: roundOneDecimal(minLifespanMs / 3600000),
      max_lifespan_hours: roundOneDecimal(maxLifespanMs / 3600000),
      total_dead: deadDurations.length,
      total_alive: aliveDurations.length,
      avg_alive_hours: roundOneDecimal(avgAliveMs / 3600000),
      by_status_avg_hours: byStatusAvgHours,
      avg_lifespan_by_status_hours: byStatusAvgHours,
      dead_source_count: deadSourceCount,
      computed_at: nowMs,
    };

    if (!this._lastLifespanLogAt || nowMs - this._lastLifespanLogAt >= 30000) {
      this._lastLifespanLogAt = nowMs;
      console.log(
        '[account-pool] lifespan stats: dead=' + result.total_dead
        + ', alive=' + result.total_alive
        + ', avg=' + result.avg_lifespan_hours + 'h'
        + ', median=' + result.median_lifespan_hours + 'h'
        + ', source=' + JSON.stringify(deadSourceCount)
      );
    }

    return result;
  }

  /**
   * 获取所有账号摘要（管理用，含废弃账号）
   */
  listAccounts(options) {
    if (this._hasRepositoryMode() && this._repository && typeof this._repository.getAll === 'function') {
      var opts = (options && typeof options === 'object') ? options : {};
      var query = {
        status: opts.status,
        search: opts.search,
        page: opts.page,
        limit: opts.limit,
      };
      var self = this;
      return this._repository.getAll(query).then(function (result) {
        var rows = result && Array.isArray(result.accounts) ? result.accounts : [];
        var mapped = rows.map(function (row) {
          return self._toListAccountSummary(createAccountState(row || {}));
        });
        var total = result && typeof result.total === 'number' ? result.total : mapped.length;
        if (!isFinite(total) || total < 0) total = mapped.length;
        var page = toPositiveInt(result && result.page, toPositiveInt(opts.page, 1));
        var limit = toPositiveInt(result && result.limit, toPositiveInt(opts.limit, 50));
        var pages = Math.max(1, Math.ceil(total / limit));
        if (page > pages) page = pages;
        return {
          accounts: mapped,
          total: total,
          page: page,
          limit: limit,
          pages: pages,
          hasMore: page < pages,
        };
      });
    }

    this._recoverCooldowns();
    var all = this._allAccountsArray();
    var result = [];
    for (var i = 0; i < all.length; i++) {
      result.push(this._toListAccountSummary(all[i]));
    }
    return result;
  }

  _toListAccountSummary(a) {
    var nowMs = Date.now();
    var cdUntil = a.cooldown_until || 0;
    var cdRemaining = (cdUntil > nowMs) ? (cdUntil - nowMs) : 0;
    return {
      email: a.email,
      status: a.status,
      request_count: a.request_count,
      consecutive_errors: a.consecutive_errors,
      cooldown_until: cdUntil,
      cooldown_remaining_ms: cdRemaining,
      last_error_code: a.last_error_code,
      last_error_type: a.last_error_type,
      last_error: a.last_error || '',
      session_usage: a.session_usage,
      token_expires_at: a.token_expires_at,
      usage_limited_count: a.usage_limited_count || 0,
      created_at: a.created_at || 0,
      status_changed_at: a.status_changed_at || 0,
      last_request_at: a.last_request_at || 0,
    };
  }

  /**
   * 获取完整账号对象（含 token，供刷新等内部操作用）
   */
  getFullAccount(email) {
    return this._findByEmail(email) || this._findInWasted(email);
  }

  setAccountField(email, field, value) {
    var acc = this._findByEmail(email) || this._findInWasted(email);
    if (acc) {
      acc[field] = value;
      this._syncReadyForAccount(acc);
      this._scheduleSave(email);
    }
  }

  /**
   * 获取待重新登录的账号
   */
  getReloginAccounts() {
    return [];
  }

  /**
   * 获取所有活跃账号对象（供 session 保活用）
   */
  getActiveAccountObjects() {
    return this._activeAccountsArray().filter(function (a) {
      return a.status === 'active' && a.sessionToken;
    });
  }

  /**
   * 账号级锁 — 防止 TokenRefresher 和 ReloginManager 同时操作同一账号
   */
  lockAccount(email) {
    if (!this._lockedEmails) this._lockedEmails = new Set();
    if (this._lockedEmails.has(email)) return false;
    this._lockedEmails.add(email);
    return true;
  }

  unlockAccount(email) {
    if (this._lockedEmails) this._lockedEmails.delete(email);
    var account = this._findByEmail(email);
    if (account) this._syncReadyForAccount(account);
  }

  isLocked(email) {
    return this._lockedEmails ? this._lockedEmails.has(email) : false;
  }

  _notifyExpiredDetected() {
    if (!this._onExpiredDetected) return;
    var now = Date.now();
    if (this._lastExpiredNotifyAt > 0 && now - this._lastExpiredNotifyAt < 3000) {
      return;
    }
    this._lastExpiredNotifyAt = now;
    this._onExpiredDetected();
  }

  _getRefreshBeforeExpirySec() {
    return toPositiveInt(
      this._config && this._config.credentials && this._config.credentials.refresh_before_expiry_seconds,
      300
    );
  }

  _getMinRequestTokenTtlSec() {
    return toPositiveInt(
      this._config && this._config.credentials && this._config.credentials.min_request_token_ttl_seconds,
      90
    );
  }

  _getMaxConsecutiveErrorsToWasted() {
    return toPositiveInt(
      this._config && this._config.credentials && this._config.credentials.max_consecutive_errors_to_wasted,
      DEFAULT_MAX_CONSECUTIVE_ERRORS_TO_WASTED
    );
  }

  _buildServeContext() {
    var nowMs = Date.now();
    return {
      nowMs: nowMs,
      nowSec: Math.floor(nowMs / 1000),
      refreshBeforeSec: this._getRefreshBeforeExpirySec(),
      minServeTtlSec: this._getMinRequestTokenTtlSec(),
    };
  }

  _isReadyForQueue(account, nowMs) {
    if (!account || !account.email) return false;
    if (account.status !== 'active') return false;
    if (!this._accounts.has(account.email)) return false;
    var now = toTimestampMs(nowMs) || Date.now();
    return !(account.cooldown_until && account.cooldown_until > now);
  }

  _enqueueReadyEmail(email) {
    var key = String(email || '').trim();
    if (!key) return;
    if (this._readySet.has(key)) return;
    this._readyQueue.push(key);
    this._readySet.add(key);
  }

  _removeFromReadyQueue(email) {
    var key = String(email || '').trim();
    if (!key) return;
    if (!this._readySet.has(key)) return;
    this._readySet.delete(key);
    var nextQueue = [];
    for (var i = 0; i < this._readyQueue.length; i++) {
      if (this._readyQueue[i] !== key) nextQueue.push(this._readyQueue[i]);
    }
    this._readyQueue = nextQueue;
  }

  _syncReadyForAccount(account, nowMs) {
    if (!account || !account.email) return;
    if (this._isReadyForQueue(account, nowMs)) {
      this._enqueueReadyEmail(account.email);
      return;
    }
    this._removeFromReadyQueue(account.email);
  }

  _rebuildReadyQueue() {
    this._readyQueue = [];
    this._readySet.clear();
    var nowMs = Date.now();
    var active = this._activeAccountsArray();
    for (var i = 0; i < active.length; i++) {
      var account = active[i];
      if (this._isReadyForQueue(account, nowMs)) {
        this._enqueueReadyEmail(account.email);
      }
    }
  }

  _evaluateAccountForServe(account, context) {
    var result = {
      eligible: false,
      foundExpired: false,
      foundNearExpiry: false,
      foundDemoted: false,
      locked: false,
    };

    if (!account || account.status === 'expired' || account.status === 'wasted') return result;

    if (account.status === 'active' && account.consecutive_errors >= this._getMaxConsecutiveErrorsToWasted()) {
      this._moveToWasted(account, {
        reason: 'consecutive_errors_exceeded_while_scheduling',
        error_type: account.last_error_type || null,
        status_code: account.last_error_code || null,
        payload: { consecutive_errors: account.consecutive_errors },
      });
      this._syncReadyForAccount(account, context.nowMs);
      result.foundDemoted = true;
      return result;
    }

    if (account.status !== 'active') return result;

    if (this.isLocked(account.email)) {
      result.locked = true;
      return result;
    }

    if (account.cooldown_until && account.cooldown_until > context.nowMs) return result;

    if (!account.accessToken) {
      this._setAccountStatus(account, 'expired', { reason: 'missing_access_token' });
      account.cooldown_until = 0;
      this._syncReadyForAccount(account, context.nowMs);
      result.foundExpired = true;
      return result;
    }

    if (!account.token_expires_at) {
      account.token_expires_at = parseJwtExp(account.accessToken);
    }

    if (!account.token_expires_at || account.token_expires_at <= context.nowSec) {
      this._setAccountStatus(account, 'expired', { reason: 'token_expired_or_invalid' });
      account.cooldown_until = 0;
      this._syncReadyForAccount(account, context.nowMs);
      result.foundExpired = true;
      return result;
    }

    var ttlSec = account.token_expires_at - context.nowSec;
    if (ttlSec <= context.minServeTtlSec) {
      this._setAccountStatus(account, 'expired', { reason: 'token_ttl_below_minimum' });
      account.cooldown_until = 0;
      this._syncReadyForAccount(account, context.nowMs);
      result.foundExpired = true;
      return result;
    }

    if (ttlSec <= context.refreshBeforeSec) {
      result.foundNearExpiry = true;
    }
    result.eligible = true;
    return result;
  }

  _pickAccountFromReadyQueue() {
    var context = this._buildServeContext();
    var foundExpired = false;
    var foundNearExpiry = false;
    var foundDemoted = false;
    var attempts = this._readyQueue.length;

    for (var i = 0; i < attempts; i++) {
      var email = this._readyQueue.shift();
      if (!email) continue;
      this._readySet.delete(email);

      var account = this._findByEmail(email);
      if (!account) continue;

      var evaluation = this._evaluateAccountForServe(account, context);
      if (evaluation.foundExpired) foundExpired = true;
      if (evaluation.foundNearExpiry) foundNearExpiry = true;
      if (evaluation.foundDemoted) foundDemoted = true;

      if (!evaluation.eligible) {
        if (evaluation.locked && this._isReadyForQueue(account, context.nowMs)) {
          this._enqueueReadyEmail(email);
        }
        continue;
      }

      this._enqueueReadyEmail(email);
      return {
        account: account,
        foundExpired: foundExpired,
        foundNearExpiry: foundNearExpiry,
        foundDemoted: foundDemoted,
      };
    }

    return {
      account: null,
      foundExpired: foundExpired,
      foundNearExpiry: foundNearExpiry,
      foundDemoted: foundDemoted,
    };
  }

  _getAccountByScanFallback() {
    var context = this._buildServeContext();
    var foundExpired = false;
    var foundNearExpiry = false;
    var foundDemoted = false;
    var available = [];
    var active = this._activeAccountsArray();
    for (var i = 0; i < active.length; i++) {
      var account = active[i];
      var evaluation = this._evaluateAccountForServe(account, context);
      if (evaluation.foundExpired) foundExpired = true;
      if (evaluation.foundNearExpiry) foundNearExpiry = true;
      if (evaluation.foundDemoted) foundDemoted = true;
      if (evaluation.eligible) available.push(account);
    }

    if (available.length === 0) {
      return {
        account: null,
        foundExpired: foundExpired,
        foundNearExpiry: foundNearExpiry,
        foundDemoted: foundDemoted,
      };
    }

    var selected = this._scheduler ? this._scheduler.select(available) : available[0];
    if (selected && selected.email) this._enqueueReadyEmail(selected.email);
    return {
      account: selected || null,
      foundExpired: foundExpired,
      foundNearExpiry: foundNearExpiry,
      foundDemoted: foundDemoted,
    };
  }

  // ============ 内部方法 ============

  _findByEmail(email) {
    return this._accounts.get(email) || null;
  }

  _findInWasted(email) {
    return this._wasted.get(email) || null;
  }

  _recoverCooldowns() {
    var now = Date.now();
    var changed = false;
    var active = this._activeAccountsArray();
    for (var i = 0; i < active.length; i++) {
      var a = active[i];
      if (a.status === 'cooldown' && a.cooldown_until > 0 && a.cooldown_until <= now) {
        this._setAccountStatus(a, 'active', {
          reason: 'cooldown_elapsed',
          error_type: a.last_error_type || null,
          status_code: a.last_error_code || null,
        });
        a.cooldown_until = 0;
        a.consecutive_errors = 0;
        a.last_error_code = null;
        a.last_error_type = null;
        a.last_error = '';
        this._syncReadyForAccount(a);
        changed = true;
      }
    }

    // 清理活跃账号的陈旧错误（cooldown 已过期但 last_error 未清）
    for (var j = 0; j < active.length; j++) {
      var b = active[j];
      if (b.status === 'active' && b.consecutive_errors === 0 && (!b.cooldown_until || b.cooldown_until <= now) && (b.last_error_code || b.last_error_type)) {
        b.last_error_code = null;
        b.last_error_type = null;
        b.last_error = '';
        changed = true;
      }
    }
    if (changed) this._scheduleSave();
  }
}
