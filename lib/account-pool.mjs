/**
 * 多账号池管理
 *
 *
 * 功能:
 *   - 从 accounts.json 导入/导出
 *   - 账号状态追踪（active/cooldown/banned/expired）
 *   - 错误细分处理（不笼统）
 *   - wasted 账号管理
 *   - 用量统计
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
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

function shouldErrorBurstExpireAccount(errorType) {
  return errorType === 'token_expired'
    || errorType === 'session_invalidated'
    || errorType === 'account_banned';
}

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
    session_usage: { input_tokens: 0, output_tokens: 0 },
    token_expires_at: 0,
    created_at: now,
    status_changed_at: now,
    session_invalidated_count: 0,
    usage_limited_count: 0,
    _tokenVersion: 0,
    _lastRefreshAt: 0,
  };

  // 恢复已有的运行时状态（从状态文件加载时保留）
  if (raw.status) {
    // 非标准状态（如注册机的 success）映射为 active
    var VALID_STATUSES = { active: 1, expired: 1, cooldown: 1, banned: 1, wasted: 1, relogin_needed: 1 };
    state.status = VALID_STATUSES[raw.status] ? raw.status : 'active';
  }
  if (raw.request_count) state.request_count = raw.request_count;
  if (raw.last_request_at) state.last_request_at = raw.last_request_at;
  if (raw.consecutive_errors) state.consecutive_errors = raw.consecutive_errors;
  if (raw.cooldown_until) state.cooldown_until = raw.cooldown_until;
  if (raw.last_error_code !== undefined) state.last_error_code = raw.last_error_code;
  if (raw.last_error_type !== undefined) state.last_error_type = raw.last_error_type;
  if (raw.session_usage) state.session_usage = { input_tokens: raw.session_usage.input_tokens || 0, output_tokens: raw.session_usage.output_tokens || 0 };
  if (raw.token_expires_at) state.token_expires_at = raw.token_expires_at;
  if (hasRawCreatedAt) state.created_at = raw.created_at;
  if (hasRawStatusChangedAt) state.status_changed_at = raw.status_changed_at;
  if (raw.session_invalidated_count) state.session_invalidated_count = raw.session_invalidated_count;
  if (raw.usage_limited_count) state.usage_limited_count = raw.usage_limited_count;
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
    state.accountId = auth.chatgpt_account_id || '';
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

/**
 * AccountPool — 多账号池
 */
export class AccountPool {

  constructor(config, i18n) {
    this._config = config || {};
    this._i18n = i18n || {};
    this._accounts = [];
    this._wasted = [];
    this._scheduler = null;
    this._savePath = '';
    this._saveTimer = null;
    this._lastExpiredNotifyAt = 0;
    this._lastLifespanLogAt = 0;
    this._newAccountCallback = null;
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
   * 注册重登检测回调 — 标记 relogin_needed 时立刻调用
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

  /**
   * 设置自动持久化路径
   */
  setSavePath(path) {
    this._savePath = path;
  }

  /**
   * 防抖自动保存（2 秒内无新变更才写盘）
   */
  _scheduleSave() {
    if (!this._savePath) return;
    if (this._saveTimer) clearTimeout(this._saveTimer);
    var self = this;
    this._saveTimer = setTimeout(function () {
      self._doSave();
    }, 2000);
  }

  /**
   * 强制立即保存（退出时调用）
   */
  forceSave() {
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
    this._doSave();
  }

  /**
   * 执行保存
   */
  _doSave() {
    if (!this._savePath) return;
    try {
      var dir = dirname(this._savePath);
      if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
      }
      var allAccounts = this._accounts.concat(this._wasted);
      var data = allAccounts.map(function (a) {
        var entry = {
          email: a.email,
          accessToken: a.accessToken,
          sessionToken: a.sessionToken,
          cookies: a.cookies,
          status: a.status,
          request_count: a.request_count,
          last_request_at: a.last_request_at || 0,
          session_usage: a.session_usage,
          consecutive_errors: a.consecutive_errors,
          cooldown_until: a.cooldown_until,
          last_error_code: a.last_error_code,
          last_error_type: a.last_error_type,
          token_expires_at: a.token_expires_at,
          created_at: a.created_at,
          status_changed_at: a.status_changed_at || a.created_at || 0,
          session_invalidated_count: a.session_invalidated_count || 0,
          usage_limited_count: a.usage_limited_count || 0,
          _tokenVersion: typeof a._tokenVersion === 'number' ? a._tokenVersion : 0,
          _lastRefreshAt: typeof a._lastRefreshAt === 'number' ? a._lastRefreshAt : 0,
        };
        // 批量注册的机器账号密码也需要持久化，用于 session 失效后自动重登
        if (a.password) entry.password = a.password;
        return entry;
      });
      writeFileSync(this._savePath, JSON.stringify(data, null, 2));
    } catch (e) {
      console.error('[account-pool] 持久化失败:', e.message);
    }
  }

  /**
   * 从 accounts.json 导入
   */
  loadAccounts(path) {
    path = path || this._config.accounts_source || './accounts.json';
    if (!existsSync(path)) {
      return { loaded: 0, active: 0 };
    }
    var raw = JSON.parse(readFileSync(path, 'utf8'));
    var list = Array.isArray(raw) ? raw : [raw];
    var beforeCount = this._accounts.length + this._wasted.length;

    for (var i = 0; i < list.length; i++) {
      var item = list[i];
      // 去重：同 email 不重复加 — 但回填密码（accounts.json 有密码，accounts-state.json 没有）
      var existingActive = this._findByEmail(item.email);
      if (existingActive) {
        if (item.password && !existingActive.password) {
          existingActive.password = item.password;
        }
        continue;
      }
      var existingWasted = this._findInWasted(item.email);
      if (existingWasted) {
        if (item.password && !existingWasted.password) {
          existingWasted.password = item.password;
        }
        continue;
      }
      var state = createAccountState(item);
      // wasted 账号放入 _wasted 池，其余放入 _accounts
      if (state.status === 'wasted') {
        this._wasted.push(state);
      } else {
        this._accounts.push(state);
      }
    }

    var active = this._accounts.filter(function (a) { return a.status === 'active'; }).length;
    return { loaded: (this._accounts.length + this._wasted.length) - beforeCount, active: active };
  }

  /**
   * 导出全部账号（含状态 + 废弃账号）
   */
  exportAccounts(path) {
    var allAccounts = this._accounts.concat(this._wasted);
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
    if (path) {
      var dir = dirname(path);
      if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
      }
      writeFileSync(path, JSON.stringify(data, null, 2));
    }
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
      for (var w = 0; w < this._wasted.length; w++) {
        if (this._wasted[w].email === raw.email) {
          this._wasted.splice(w, 1);
          break;
        }
      }
      wastedAccount.accessToken = raw.accessToken || wastedAccount.accessToken;
      wastedAccount.sessionToken = raw.sessionToken || wastedAccount.sessionToken;
      wastedAccount.cookies = raw.cookies || wastedAccount.cookies;
      wastedAccount.password = raw.password || wastedAccount.password;
      setAccountStatus(wastedAccount, 'active');
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
      this._accounts.push(wastedAccount);
      this._emitNewAccount(raw.email);
      this._scheduleSave();
      return wastedAccount;
    }

    if (this._findByEmail(raw.email)) {
      // 已存在，更新 token
      var existing = this._findByEmail(raw.email);
      existing.accessToken = raw.accessToken || existing.accessToken;
      existing.sessionToken = raw.sessionToken || existing.sessionToken;
      existing.cookies = raw.cookies || existing.cookies;
      existing.password = raw.password || existing.password;
      setAccountStatus(existing, 'active');
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
      this._emitNewAccount(raw.email);
      this._scheduleSave();
      return existing;
    }
    var state = createAccountState(raw);
    this._accounts.push(state);
    this._emitNewAccount(raw.email);
    this._scheduleSave();
    return state;
  }

  /**
   * 彻底删除账号（从活跃池和废弃池中移除）
   */
  removeAccount(email) {
    var found = false;
    for (var i = this._accounts.length - 1; i >= 0; i--) {
      if (this._accounts[i].email === email) {
        this._accounts.splice(i, 1);
        found = true;
        break;
      }
    }
    if (!found) {
      for (var w = this._wasted.length - 1; w >= 0; w--) {
        if (this._wasted[w].email === email) {
          this._wasted.splice(w, 1);
          found = true;
          break;
        }
      }
    }
    if (found) this._scheduleSave();
    return found;
  }

  /**
   * 总账号数（含 wasted）
   */
  getTotalCount() {
    return this._accounts.length + this._wasted.length;
  }

  /**
   * 活跃账号数
   */
  getActiveCount() {
    return this._accounts.filter(function (a) { return a.status === 'active'; }).length;
  }

  /**
   * 获取可用账号（通过调度器选择）
   */
  getAccount() {
    // 先恢复冷却到期的账号
    this._recoverCooldowns();

    var self = this;
    var nowSec = Math.floor(Date.now() / 1000);
    var refreshBeforeSec = this._getRefreshBeforeExpirySec();
    var minServeTtlSec = this._getMinRequestTokenTtlSec();
    var foundExpired = false;
    var foundNearExpiry = false;
    var foundDemoted = false;
    var available = this._accounts.filter(function (a) {
      if (a.status === 'expired' || a.status === 'wasted') return false;
      if (a.status === 'active' && a.consecutive_errors > 5) {
        if (shouldErrorBurstExpireAccount(a.last_error_type)) {
          setAccountStatus(a, 'expired');
          a.cooldown_until = 0;
          foundExpired = true;
        } else {
          setAccountStatus(a, 'cooldown');
          a.cooldown_until = Date.now() + 60000;
        }
        foundDemoted = true;
        return false;
      }
      if (a.status !== 'active') return false;
      if (self.isLocked(a.email)) return false;
      // BUG-2 fix: defensive check - skip accounts still in cooldown
      if (a.cooldown_until && a.cooldown_until > Date.now()) return false;
      if (!a.accessToken) {
        setAccountStatus(a, 'expired');
        foundExpired = true;
        return false;
      }
      if (!a.token_expires_at) {
        a.token_expires_at = parseJwtExp(a.accessToken);
      }
      // 主动识别 JWT 已过期的账号，不等请求碰 401
      if (!a.token_expires_at || a.token_expires_at <= nowSec) {
        setAccountStatus(a, 'expired');
        foundExpired = true;
        return false;
      }

      var ttlSec = a.token_expires_at - nowSec;
      // 请求前预检：寿命过短的 token 直接转 expired，避免边界请求撞 401
      if (ttlSec <= minServeTtlSec) {
        setAccountStatus(a, 'expired');
        foundExpired = true;
        return false;
      }

      // 触发提前刷新（但账号仍可用），避免刚好过期
      if (ttlSec <= refreshBeforeSec) {
        foundNearExpiry = true;
      }
      return true;
    });

    // 发现过期账号 → 持久化 + 立刻触发异步刷新（不等 60 秒周期）
    if (foundExpired || foundDemoted) {
      this._scheduleSave();
    }

    if (foundExpired || foundNearExpiry) {
      this._notifyExpiredDetected();
    }

    if (available.length === 0) return null;

    // 有调度器用调度器，没有就第一个
    if (this._scheduler) {
      return this._scheduler.select(available);
    }
    return available[0];
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

    this._scheduleSave();
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
    account.last_error_code = statusCode;

    // 分类错误
    var classifier = ERROR_CLASSIFIERS[statusCode];
    var errorType = classifier ? classifier(errorBody) : 'unknown';
    account.last_error_type = errorType;
    if (errorType !== 'session_invalidated') {
      account.session_invalidated_count = 0;
    }

    var action = 'none';

    switch (errorType) {
      case 'token_expired':
        setAccountStatus(account, 'expired');
        account.cooldown_until = 0;
        action = 'refresh_token';
        this._notifyExpiredDetected();
        break;

      case 'session_invalidated':
        account.session_invalidated_count = (account.session_invalidated_count || 0) + 1;
        var reloginThreshold = this._getSessionInvalidatedReloginThreshold();

        // 默认优先自动刷新，不先卡 cooldown，避免“长时间停在冷却里”
        if (!this._isSessionInvalidatedReloginEnabled() || account.session_invalidated_count < reloginThreshold) {
          setAccountStatus(account, 'expired');
          account.cooldown_until = 0;
          action = 'refresh_token';
          this._notifyExpiredDetected();
          break;
        }

        // 连续多次 session_invalidated 且显式启用后才进入重登队列
        // 无论有无密码都标记 relogin_needed，停止无意义的自动刷新重试
        // 有密码: 等待重登能力上线后自动重登
        // 无密码: 需要管理员手动处理（通过管理面板添加密码或重新注册）
        setAccountStatus(account, 'relogin_needed');
        action = account.password ? 'relogin' : 'needs_password';
        if (this._onReloginNeeded) this._onReloginNeeded(account.email);
        break;

      case 'ip_blocked':
        // IP 被阻止 — 不废弃账号，设短冷却
        setAccountStatus(account, 'cooldown');
        account.cooldown_until = Date.now() + 60000; // 1 分钟
        action = 'switch_account';
        break;

      case 'mfa_needed':
        setAccountStatus(account, 'cooldown');
        account.cooldown_until = Date.now() + 3600000; // 1 小时
        action = 'manual_intervention';
        break;

      case 'account_banned':
        // 不做自动废弃：优先进入自动重登/续期流程
        if (account.password) {
          setAccountStatus(account, 'relogin_needed');
          action = 'relogin';
          if (this._onReloginNeeded) this._onReloginNeeded(account.email);
        } else {
          setAccountStatus(account, 'expired');
          account.cooldown_until = 0;
          action = 'needs_password';
          if (this._onExpiredDetected) this._onExpiredDetected();
        }
        break;

      case 'rate_limited':
        setAccountStatus(account, 'cooldown');
        var cooldownMs = (this._config.rate_limit && this._config.rate_limit.cooldown_ms) || 300000;
        // BUG-3 fix: parse retry-after time from error body
        var rateLimitRetryAt = extractUsageLimitRetryTimestampMs(errorBody);
        if (rateLimitRetryAt > Date.now()) {
          account.cooldown_until = rateLimitRetryAt;
        } else {
          account.cooldown_until = Date.now() + cooldownMs;
        }
        account.last_error = errorBody ? String(errorBody).substring(0, 200) : '';
        action = 'switch_account';
        break;

      case 'usage_limited':
        account.usage_limited_count = (account.usage_limited_count || 0) + 1;
        setAccountStatus(account, 'usage_limited');
        var usageRetryAt = extractUsageLimitRetryTimestampMs(errorBody);
        // 优先用错误消息里的确切重试时间（如 "try again at Mar 7th, 2026 1:30 AM"）
        // 解析失败才 fallback 30 分钟
        account.cooldown_until = usageRetryAt > Date.now() ? usageRetryAt : (Date.now() + 1800000);
        account.last_error = errorBody ? String(errorBody).substring(0, 200) : '';
        action = 'switch_account';
        break;

      case 'bad_request':
        // 客户端参数错误，不惩罚账号
        setAccountStatus(account, 'active');
        account.cooldown_until = 0;
        account.consecutive_errors = Math.max(0, account.consecutive_errors - 1);
        action = 'ignore_request_error';
        break;

      case 'upstream_error':
      case 'upstream_unavailable':
      case 'upstream_overloaded':
        // 上游问题 — 不影响账号状态
        if (account.consecutive_errors >= 5) {
          setAccountStatus(account, 'cooldown');
          account.cooldown_until = Date.now() + 60000;
        }
        action = 'retry';
        break;

      case 'network_error':
        // 网络错误 — 不影响账号状态，连续多次才短暂冷却
        if (account.consecutive_errors >= 5) {
          setAccountStatus(account, 'cooldown');
          account.cooldown_until = Date.now() + 60000;
        }
        action = 'retry';
        break;

      default:
        // 未知错误不自动废弃，短冷却后继续自动恢复
        setAccountStatus(account, 'cooldown');
        account.cooldown_until = Date.now() + 60000;
        action = 'retry';
    }

    if (account.status === 'active' && account.consecutive_errors > 5) {
      if (shouldErrorBurstExpireAccount(account.last_error_type)) {
        setAccountStatus(account, 'expired');
        account.cooldown_until = 0;
        if (action === 'none' || action === 'ignore_request_error') action = 'refresh_token';
        this._notifyExpiredDetected();
      } else {
        setAccountStatus(account, 'cooldown');
        account.cooldown_until = Date.now() + 60000;
        if (action === 'none' || action === 'ignore_request_error') action = 'switch_account';
      }
    }

    this._scheduleSave();
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

    setAccountStatus(account, 'expired');
    account.cooldown_until = 0;
    account.last_error_code = code;
    account.last_error_type = errorType;
    this._scheduleSave();
    return { updated: true, type: errorType };
  }

  /**
   */
  markWasted(email) {
    var idx = -1;
    for (var i = 0; i < this._accounts.length; i++) {
      if (this._accounts[i].email === email) { idx = i; break; }
    }
    if (idx < 0) return;
    var account = this._accounts.splice(idx, 1)[0];
    setAccountStatus(account, 'wasted');
    this._wasted.push(account);
    this._scheduleSave();
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
    setAccountStatus(account, 'active');
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
    this._scheduleSave();
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
    setAccountStatus(account, 'active');
    account.consecutive_errors = 0;
    account.cooldown_until = 0;
    account.last_error_code = null;
    account.last_error_type = null;
    account.last_error = '';
    account.session_invalidated_count = 0;
    account._lastRefreshAt = Date.now();
    account._tokenVersion = currentVersion + 1;
    this._scheduleSave();
    return { applied: true, version: account._tokenVersion };
  }

  /**
   * 激活废弃账号（从 _wasted 移回 _accounts）
   */
  activateWasted(email) {
    var idx = -1;
    for (var i = 0; i < this._wasted.length; i++) {
      if (this._wasted[i].email === email) { idx = i; break; }
    }
    if (idx < 0) return null;
    var account = this._wasted.splice(idx, 1)[0];
    setAccountStatus(account, 'active');
    account.consecutive_errors = 0;
    account.cooldown_until = 0;
    account.last_error_code = null;
    account.last_error_type = null;
    account.last_error = '';
    account.session_invalidated_count = 0;
    this._accounts.push(account);
    this._scheduleSave();
    return account;
  }

  /**
   * 启动自愈：把可恢复的 wasted 账号迁回自动恢复队列
   *
   * 规则：
   * - 统一迁回 expired（交给 refreshAll 重试）
   */
  recoverWastedAccounts() {
    var recovered = 0;

    for (var i = this._wasted.length - 1; i >= 0; i--) {
      var w = this._wasted[i];
      if (!w) continue;

      // 若活跃池已有同邮箱，优先更新已有项，避免重复
      var existing = this._findByEmail(w.email);
      if (existing) {
        if (!existing.password && w.password) existing.password = w.password;
        if (!existing.sessionToken && w.sessionToken) existing.sessionToken = w.sessionToken;
        if (!existing.accessToken && w.accessToken) existing.accessToken = w.accessToken;
        setAccountStatus(existing, 'expired');
        existing.last_error_type = existing.last_error_type || 'token_expired';
        existing.last_error_code = existing.last_error_code || 401;
        existing.cooldown_until = 0;
        this._wasted.splice(i, 1);
        recovered++;
        continue;
      }

      this._wasted.splice(i, 1);
      w.cooldown_until = 0;
      w.consecutive_errors = 0;
      setAccountStatus(w, 'expired');
      w.last_error_type = w.last_error_type || 'token_expired';
      w.last_error_code = w.last_error_code || 401;
      this._accounts.push(w);
      recovered++;
    }

    if (recovered > 0) this._scheduleSave();
    return recovered;
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

    return this._accounts.filter(function (a) {
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
    for (var i = 0; i < this._accounts.length; i++) {
      var a = this._accounts[i];
      if ((a.status === 'expired' || a.status === 'relogin_needed') && a.sessionToken) {
        result.push(a);
      }
    }
    for (var j = 0; j < this._wasted.length; j++) {
      var w = this._wasted[j];
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
    for (var i = 0; i < this._accounts.length; i++) {
      map[this._accounts[i].email] = i + 1;
    }
    for (var j = 0; j < this._wasted.length; j++) {
      map[this._wasted[j].email] = this._accounts.length + j + 1;
    }
    return map;
  }

  /**
   * 全池统计
   */
  getStats() {
    this._recoverCooldowns();
    var stats = { total: this._accounts.length + this._wasted.length, active: 0, cooldown: 0, banned: 0, expired: 0, relogin_needed: 0, usage_limited: 0, wasted: this._wasted.length };
    for (var i = 0; i < this._accounts.length; i++) {
      var s = this._accounts[i].status;
      if (s === 'active') stats.active++;
      else if (s === 'cooldown') stats.cooldown++;
      else if (s === 'banned') stats.banned++;
      else if (s === 'expired') stats.expired++;
      else if (s === 'relogin_needed') stats.relogin_needed++;
      else if (s === 'usage_limited') stats.usage_limited++;
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
    var allAccounts = this._accounts.concat(this._wasted);
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
  listAccounts() {
    this._recoverCooldowns();
    var nowMs = Date.now();
    var mapFn = function (a) {
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
    };
    return this._accounts.map(mapFn).concat(this._wasted.map(mapFn));
  }

  /**
   * 获取完整账号对象（含 token，供刷新等内部操作用）
   */
  getFullAccount(email) {
    return this._findByEmail(email) || this._findInWasted(email);
  }

  setAccountField(email, field, value) {
    var acc = this._findByEmail(email) || this._findInWasted(email);
    if (acc) acc[field] = value;
  }

  /**
   * 获取待重新登录的账号
   */
  getReloginAccounts() {
    return this._accounts.filter(function (a) {
      return a.status === 'relogin_needed' && a.password;
    });
  }

  /**
   * 获取所有活跃账号对象（供 session 保活用）
   */
  getActiveAccountObjects() {
    return this._accounts.filter(function (a) {
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

  _isSessionInvalidatedReloginEnabled() {
    return !!(
      this._config
      && this._config.credentials
      && this._config.credentials.session_invalidated_relogin === true
    );
  }

  _getSessionInvalidatedReloginThreshold() {
    var configured = toPositiveInt(
      this._config && this._config.credentials && this._config.credentials.session_invalidated_relogin_threshold,
      3
    );
    // 默认把 relogin 触发阈值抬到 2，避免单次 401 抖动导致 active 大量瞬时降级。
    // 如需回到单次触发，可显式配置 session_invalidated_relogin_min_threshold=1。
    var floorRaw = this._config && this._config.credentials && this._config.credentials.session_invalidated_relogin_min_threshold;
    var floor = 2;
    if (floorRaw === 1) {
      floor = 1;
    } else {
      floor = toPositiveInt(floorRaw, 2);
    }
    return Math.max(configured, floor);
  }

  // ============ 内部方法 ============

  _findByEmail(email) {
    for (var i = 0; i < this._accounts.length; i++) {
      if (this._accounts[i].email === email) return this._accounts[i];
    }
    return null;
  }

  _findInWasted(email) {
    for (var i = 0; i < this._wasted.length; i++) {
      if (this._wasted[i].email === email) return this._wasted[i];
    }
    return null;
  }

  _recoverCooldowns() {
    var now = Date.now();
    var nowSec = Math.floor(now / 1000);
    var minServeTtlSec = this._getMinRequestTokenTtlSec();
    var changed = false;
    var needRefresh = false;
    for (var i = 0; i < this._accounts.length; i++) {
      var a = this._accounts[i];
      if ((a.status === 'cooldown' || a.status === 'usage_limited') && a.cooldown_until > 0 && a.cooldown_until <= now) {
        var toExpired = a.last_error_type === 'session_invalidated';
        if (!toExpired) {
          if (!a.accessToken) {
            toExpired = true;
          } else {
            if (!a.token_expires_at) {
              a.token_expires_at = parseJwtExp(a.accessToken);
            }
            if (!a.token_expires_at || a.token_expires_at <= nowSec + minServeTtlSec) {
              toExpired = true;
            }
          }
        }
        setAccountStatus(a, toExpired ? 'expired' : 'active');
        a.cooldown_until = 0;
        a.consecutive_errors = 0;
        if (!toExpired) {
          a.last_error_code = null;
          a.last_error_type = null;
          a.last_error = '';
        }
        changed = true;
        if (toExpired) needRefresh = true;
      }
    }

    // 清理活跃账号的陈旧错误（cooldown 已过期但 last_error 未清）
    for (var j = 0; j < this._accounts.length; j++) {
      var b = this._accounts[j];
      if (b.status === 'active' && b.consecutive_errors === 0 && (!b.cooldown_until || b.cooldown_until <= now) && (b.last_error_code || b.last_error_type)) {
        b.last_error_code = null;
        b.last_error_type = null;
        b.last_error = '';
        changed = true;
      }
    }
    if (changed) this._scheduleSave();
    if (needRefresh) this._notifyExpiredDetected();
  }
}
