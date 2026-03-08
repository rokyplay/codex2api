import { existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { createRequire } from 'node:module';
import { AccountRepository } from './account-repository.mjs';

const require = createRequire(import.meta.url);

const DEFAULT_DB_PATH = resolve(process.cwd(), 'data/accounts.db');
const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_BUSY_TIMEOUT_MS = 5000;
const DEFAULT_JOURNAL_SIZE_LIMIT = 67108864; // 64MB
const DEFAULT_WAL_AUTOCHECKPOINT = 1000;

const JS_TO_DB_FIELD_MAP = {
  email: 'email',
  password: 'password',
  accessToken: 'access_token',
  sessionToken: 'session_token',
  cookies: 'cookies_json',
  accountId: 'account_id',
  status: 'status',
  requestCount: 'request_count',
  lastRequestAtMs: 'last_request_at_ms',
  consecutiveErrors: 'consecutive_errors',
  cooldownUntilMs: 'cooldown_until_ms',
  lastErrorCode: 'last_error_code',
  lastErrorType: 'last_error_type',
  lastError: 'last_error',
  sessionInputTokens: 'session_input_tokens',
  sessionOutputTokens: 'session_output_tokens',
  tokenExpiresAtS: 'token_expires_at_s',
  createdAtMs: 'created_at_ms',
  statusChangedAtMs: 'status_changed_at_ms',
  sessionInvalidatedCount: 'session_invalidated_count',
  usageLimitedCount: 'usage_limited_count',
  tokenVersion: 'token_version',
  lastRefreshAtMs: 'last_refresh_at_ms',
  verifiedAtMs: 'verified_at_ms',
  updatedAtMs: 'updated_at_ms',
};

const INPUT_KEY_TO_JS_FIELD_MAP = {
  email: 'email',
  password: 'password',
  accessToken: 'accessToken',
  access_token: 'accessToken',
  sessionToken: 'sessionToken',
  session_token: 'sessionToken',
  cookies: 'cookies',
  cookies_json: 'cookies',
  accountId: 'accountId',
  account_id: 'accountId',
  status: 'status',
  requestCount: 'requestCount',
  request_count: 'requestCount',
  lastRequestAtMs: 'lastRequestAtMs',
  last_request_at_ms: 'lastRequestAtMs',
  last_request_at: 'lastRequestAtMs',
  consecutiveErrors: 'consecutiveErrors',
  consecutive_errors: 'consecutiveErrors',
  cooldownUntilMs: 'cooldownUntilMs',
  cooldown_until_ms: 'cooldownUntilMs',
  cooldown_until: 'cooldownUntilMs',
  lastErrorCode: 'lastErrorCode',
  last_error_code: 'lastErrorCode',
  lastErrorType: 'lastErrorType',
  last_error_type: 'lastErrorType',
  lastError: 'lastError',
  last_error: 'lastError',
  sessionInputTokens: 'sessionInputTokens',
  session_input_tokens: 'sessionInputTokens',
  sessionOutputTokens: 'sessionOutputTokens',
  session_output_tokens: 'sessionOutputTokens',
  tokenExpiresAtS: 'tokenExpiresAtS',
  token_expires_at_s: 'tokenExpiresAtS',
  token_expires_at: 'tokenExpiresAtS',
  createdAtMs: 'createdAtMs',
  created_at_ms: 'createdAtMs',
  created_at: 'createdAtMs',
  statusChangedAtMs: 'statusChangedAtMs',
  status_changed_at_ms: 'statusChangedAtMs',
  status_changed_at: 'statusChangedAtMs',
  sessionInvalidatedCount: 'sessionInvalidatedCount',
  session_invalidated_count: 'sessionInvalidatedCount',
  usageLimitedCount: 'usageLimitedCount',
  usage_limited_count: 'usageLimitedCount',
  tokenVersion: 'tokenVersion',
  token_version: 'tokenVersion',
  _tokenVersion: 'tokenVersion',
  lastRefreshAtMs: 'lastRefreshAtMs',
  last_refresh_at_ms: 'lastRefreshAtMs',
  _lastRefreshAt: 'lastRefreshAtMs',
  verifiedAtMs: 'verifiedAtMs',
  verified_at_ms: 'verifiedAtMs',
  verified_at: 'verifiedAtMs',
  updatedAtMs: 'updatedAtMs',
  updated_at_ms: 'updatedAtMs',
};

const UPSERT_COLUMNS = [
  'email',
  'password',
  'access_token',
  'session_token',
  'cookies_json',
  'account_id',
  'status',
  'request_count',
  'last_request_at_ms',
  'consecutive_errors',
  'cooldown_until_ms',
  'last_error_code',
  'last_error_type',
  'last_error',
  'session_input_tokens',
  'session_output_tokens',
  'token_expires_at_s',
  'created_at_ms',
  'status_changed_at_ms',
  'session_invalidated_count',
  'usage_limited_count',
  'token_version',
  'last_refresh_at_ms',
  'verified_at_ms',
  'updated_at_ms',
];

const ACCOUNT_LIST_ORDER_SQL =
  "CASE status"
  + " WHEN 'active' THEN 0"
  + " WHEN 'cooldown' THEN 1"
  + " WHEN 'usage_limited' THEN 1"
  + " WHEN 'expired' THEN 2"
  + " WHEN 'banned' THEN 3"
  + " WHEN 'wasted' THEN 4"
  + " WHEN 'relogin_needed' THEN 4"
  + " ELSE 9 END ASC"
  + ", status_changed_at_ms DESC"
  + ", last_request_at_ms DESC"
  + ", id DESC";

function loadBetterSqlite3() {
  try {
    require.resolve('better-sqlite3');
  } catch (_) {
    throw new Error('better-sqlite3 is not installed. Run: npm install better-sqlite3');
  }
  const loaded = require('better-sqlite3');
  return loaded && loaded.default ? loaded.default : loaded;
}

function toInteger(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function pickFirstDefined() {
  for (let i = 0; i < arguments.length; i++) {
    const value = arguments[i];
    if (value !== undefined && value !== null && value !== '') {
      return value;
    }
  }
  return undefined;
}

function toNullableInteger(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function toNullableTimestampMs(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return Math.trunc(parsed);
  }
  return toNullableInteger(value);
}

function toStringValue(value, fallback) {
  if (value === null || value === undefined) return fallback;
  return String(value);
}

function normalizeCookies(rawCookies) {
  if (!rawCookies) return {};
  if (typeof rawCookies === 'string') {
    try {
      const parsed = JSON.parse(rawCookies);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
      return {};
    } catch (_) {
      return {};
    }
  }
  if (typeof rawCookies === 'object' && !Array.isArray(rawCookies)) {
    return rawCookies;
  }
  return {};
}

function safeStringify(obj, fallback) {
  try {
    return JSON.stringify(obj);
  } catch (_) {
    return fallback;
  }
}

function normalizeAccountStatus(rawStatus) {
  const status = String(rawStatus || '').trim().toLowerCase();
  if (!status) return 'active';
  if (status === 'relogin_needed') return 'wasted';
  if (status === 'usage_limited') return 'cooldown';
  if (status === 'active' || status === 'expired' || status === 'cooldown' || status === 'banned' || status === 'wasted') {
    return status;
  }
  return 'active';
}

function normalizeEventType(rawType) {
  const type = String(rawType || '').trim().toLowerCase();
  if (type === 'import' || type === 'delete') {
    return 'credential_update';
  }
  if (type === 'status_change' || type === 'error' || type === 'token_refresh' || type === 'credential_update') {
    return type;
  }
  return type || 'error';
}

function normalizeEventDetail(rawDetail) {
  if (rawDetail === null || rawDetail === undefined) return '{}';
  if (typeof rawDetail === 'string') {
    const text = rawDetail.trim();
    if (!text) return '{}';
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return JSON.stringify(parsed);
      }
      return JSON.stringify({ message: text });
    } catch (_) {
      return JSON.stringify({ message: text });
    }
  }
  if (typeof rawDetail === 'object' && !Array.isArray(rawDetail)) {
    return safeStringify(rawDetail, '{}');
  }
  return JSON.stringify({ value: rawDetail });
}

function parseDetailObject(detailText) {
  if (!detailText) return {};
  try {
    const parsed = JSON.parse(detailText);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
    return {};
  } catch (_) {
    return {};
  }
}

function normalizeEventRecord(event) {
  const entry = event && typeof event === 'object' ? event : {};
  const email = String(entry.email || '').trim();
  if (!email) return null;

  const eventType = normalizeEventType(entry.event_type || entry.eventType);
  const detailText = normalizeEventDetail(entry.detail ?? entry.payload_json ?? entry.payload);
  const detailObj = parseDetailObject(detailText);

  const rawOldStatus = entry.old_status ?? entry.oldStatus ?? detailObj.from_status ?? null;
  const rawNewStatus = entry.new_status ?? entry.newStatus ?? detailObj.to_status ?? null;
  let statusCode = entry.status_code ?? entry.statusCode ?? detailObj.status_code ?? null;
  if (statusCode === '') statusCode = null;
  let errorType = entry.error_type ?? entry.errorType ?? detailObj.error_type ?? null;
  if (errorType === '') errorType = null;

  return {
    account_id: toNullableInteger(entry.account_id ?? entry.accountId),
    email,
    event_type: eventType,
    old_status: rawOldStatus === null || rawOldStatus === undefined ? null : normalizeAccountStatus(rawOldStatus),
    new_status: rawNewStatus === null || rawNewStatus === undefined ? null : normalizeAccountStatus(rawNewStatus),
    detail: detailText,
    status_code: toNullableInteger(statusCode),
    error_type: errorType === null || errorType === undefined ? null : String(errorType),
    payload_json: detailText,
    created_at_ms: toInteger(entry.created_at_ms ?? entry.createdAtMs, Date.now()),
  };
}

function normalizeStatusFilter(status, params) {
  const rawInput = String(status || '').trim().toLowerCase();
  if (!rawInput || rawInput === 'all') return '';
  const raw = normalizeAccountStatus(rawInput);
  if (!raw || raw === 'all') return '';
  if (raw === 'cooldown') {
    params.status_0 = 'cooldown';
    params.status_1 = 'usage_limited';
    return 'status IN (@status_0, @status_1)';
  }
  if (raw === 'wasted') {
    params.status_0 = 'wasted';
    params.status_1 = 'relogin_needed';
    return 'status IN (@status_0, @status_1)';
  }
  params.status_0 = raw;
  return 'status = @status_0';
}

function normalizeInputFields(input) {
  const normalized = {};
  if (!input || typeof input !== 'object') return normalized;

  for (const key of Object.keys(input)) {
    if (key === 'sessionUsage' || key === 'session_usage') continue;
    const jsField = INPUT_KEY_TO_JS_FIELD_MAP[key];
    if (jsField) normalized[jsField] = input[key];
  }

  if (input.sessionUsage && typeof input.sessionUsage === 'object') {
    if (input.sessionUsage.inputTokens !== undefined) {
      normalized.sessionInputTokens = input.sessionUsage.inputTokens;
    } else if (input.sessionUsage.input_tokens !== undefined) {
      normalized.sessionInputTokens = input.sessionUsage.input_tokens;
    }
    if (input.sessionUsage.outputTokens !== undefined) {
      normalized.sessionOutputTokens = input.sessionUsage.outputTokens;
    } else if (input.sessionUsage.output_tokens !== undefined) {
      normalized.sessionOutputTokens = input.sessionUsage.output_tokens;
    }
  }

  if (input.session_usage && typeof input.session_usage === 'object') {
    if (input.session_usage.inputTokens !== undefined) {
      normalized.sessionInputTokens = input.session_usage.inputTokens;
    } else if (input.session_usage.input_tokens !== undefined) {
      normalized.sessionInputTokens = input.session_usage.input_tokens;
    }
    if (input.session_usage.outputTokens !== undefined) {
      normalized.sessionOutputTokens = input.session_usage.outputTokens;
    } else if (input.session_usage.output_tokens !== undefined) {
      normalized.sessionOutputTokens = input.session_usage.output_tokens;
    }
  }

  return normalized;
}

function toDbRecord(account, nowMs) {
  const input = normalizeInputFields(account);
  const createdAtMs = toInteger(input.createdAtMs, nowMs);
  const statusChangedAtMs = toInteger(input.statusChangedAtMs, createdAtMs);
  const updatedAtMs = toInteger(input.updatedAtMs, nowMs);

  return {
    email: toStringValue(input.email, '').trim(),
    password: toStringValue(input.password, ''),
    access_token: toStringValue(input.accessToken, ''),
    session_token: toStringValue(input.sessionToken, ''),
    cookies_json: safeStringify(normalizeCookies(input.cookies), '{}'),
    account_id: toStringValue(input.accountId, ''),
    status: normalizeAccountStatus(input.status),
    request_count: toInteger(input.requestCount, 0),
    last_request_at_ms: toInteger(input.lastRequestAtMs, 0),
    consecutive_errors: toInteger(input.consecutiveErrors, 0),
    cooldown_until_ms: toInteger(input.cooldownUntilMs, 0),
    last_error_code: toNullableInteger(input.lastErrorCode),
    last_error_type: input.lastErrorType === null || input.lastErrorType === undefined ? null : String(input.lastErrorType),
    last_error: toStringValue(input.lastError, ''),
    session_input_tokens: toInteger(input.sessionInputTokens, 0),
    session_output_tokens: toInteger(input.sessionOutputTokens, 0),
    token_expires_at_s: toInteger(input.tokenExpiresAtS, 0),
    created_at_ms: createdAtMs,
    status_changed_at_ms: statusChangedAtMs,
    session_invalidated_count: toInteger(input.sessionInvalidatedCount, 0),
    usage_limited_count: toInteger(input.usageLimitedCount, 0),
    token_version: toInteger(input.tokenVersion, 0),
    last_refresh_at_ms: toInteger(input.lastRefreshAtMs, 0),
    verified_at_ms: toNullableTimestampMs(input.verifiedAtMs),
    updated_at_ms: updatedAtMs,
  };
}

function fromDbRow(row) {
  if (!row) return null;
  return {
    email: toStringValue(row.email, ''),
    password: toStringValue(row.password, ''),
    accessToken: toStringValue(row.access_token, ''),
    sessionToken: toStringValue(row.session_token, ''),
    cookies: normalizeCookies(row.cookies_json),
    accountId: toStringValue(row.account_id, ''),
    status: normalizeAccountStatus(row.status),
    request_count: toInteger(row.request_count, 0),
    last_request_at: toInteger(row.last_request_at_ms, 0),
    consecutive_errors: toInteger(row.consecutive_errors, 0),
    cooldown_until: toInteger(row.cooldown_until_ms, 0),
    last_error_code: row.last_error_code === null || row.last_error_code === undefined ? null : toInteger(row.last_error_code, null),
    last_error_type: row.last_error_type === null || row.last_error_type === undefined ? null : String(row.last_error_type),
    last_error: toStringValue(row.last_error, ''),
    session_usage: {
      input_tokens: toInteger(row.session_input_tokens, 0),
      output_tokens: toInteger(row.session_output_tokens, 0),
    },
    token_expires_at: toInteger(row.token_expires_at_s, 0),
    created_at: toInteger(row.created_at_ms, 0),
    status_changed_at: toInteger(row.status_changed_at_ms, 0),
    session_invalidated_count: toInteger(row.session_invalidated_count, 0),
    usage_limited_count: toInteger(row.usage_limited_count, 0),
    _tokenVersion: toInteger(row.token_version, 0),
    _lastRefreshAt: toInteger(row.last_refresh_at_ms, 0),
    verified_at: row.verified_at_ms === null || row.verified_at_ms === undefined ? '' : toInteger(row.verified_at_ms, 0),
  };
}

function buildUpdatePayload(email, fields, nowMs) {
  const key = String(email || '').trim();
  if (!key) throw new Error('update payload requires email');
  if (!fields || typeof fields !== 'object') return null;

  const normalized = normalizeInputFields(fields);
  const assignments = [];
  const params = { email: key };

  for (const [jsField, value] of Object.entries(normalized)) {
    if (jsField === 'email') continue;
    const dbField = JS_TO_DB_FIELD_MAP[jsField];
    if (!dbField || dbField === 'updated_at_ms') continue;
    if (jsField === 'cookies') {
      params[dbField] = safeStringify(normalizeCookies(value), '{}');
    } else if (jsField === 'status') {
      params[dbField] = normalizeAccountStatus(value);
    } else if (jsField === 'lastErrorCode') {
      params[dbField] = toNullableInteger(value);
    } else if (jsField === 'verifiedAtMs') {
      params[dbField] = toNullableTimestampMs(value);
    } else if (value === null) {
      params[dbField] = null;
    } else if (typeof value === 'number') {
      params[dbField] = toInteger(value, 0);
    } else {
      params[dbField] = value;
    }
    assignments.push(dbField + ' = @' + dbField);
  }

  if (assignments.length === 0) return null;
  params.updated_at_ms = toInteger(nowMs, Date.now());
  assignments.push('updated_at_ms = @updated_at_ms');

  return {
    email: key,
    assignments,
    params,
    statementKey: assignments.join('|'),
  };
}

export class SqliteAccountRepository extends AccountRepository {
  constructor(options) {
    super();
    const opts = options || {};
    const sqlite = opts.sqlite && typeof opts.sqlite === 'object' ? opts.sqlite : {};
    this._dbPath = resolve(String(
      pickFirstDefined(sqlite.path, opts.path, DEFAULT_DB_PATH)
    ));
    this._batchSize = toInteger(
      pickFirstDefined(sqlite.batchSize, sqlite.batch_size, opts.batchSize, opts.batch_size),
      DEFAULT_BATCH_SIZE
    );
    this._busyTimeoutMs = toInteger(
      pickFirstDefined(sqlite.busyTimeoutMs, sqlite.busy_timeout_ms, opts.busyTimeoutMs, opts.busy_timeout_ms),
      DEFAULT_BUSY_TIMEOUT_MS
    );
    this._journalSizeLimit = toInteger(
      pickFirstDefined(sqlite.journalSizeLimit, sqlite.journal_size_limit, opts.journalSizeLimit, opts.journal_size_limit),
      DEFAULT_JOURNAL_SIZE_LIMIT
    );
    this._db = null;
    this._stmt = {};
  }

  async init() {
    if (this._db) return this;
    const Database = loadBetterSqlite3();
    const dir = dirname(this._dbPath);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

    this._db = new Database(this._dbPath);
    this._db.pragma('journal_mode = WAL');
    this._db.pragma('busy_timeout = ' + this._busyTimeoutMs);
    this._db.pragma('journal_size_limit = ' + this._journalSizeLimit);
    this._db.pragma('wal_autocheckpoint = ' + DEFAULT_WAL_AUTOCHECKPOINT);

    this._db.exec(`
      CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        password TEXT DEFAULT '',
        access_token TEXT NOT NULL,
        session_token TEXT DEFAULT '',
        cookies_json TEXT DEFAULT '{}',
        account_id TEXT DEFAULT '',
        status TEXT NOT NULL,
        request_count INTEGER NOT NULL DEFAULT 0,
        last_request_at_ms INTEGER NOT NULL DEFAULT 0,
        consecutive_errors INTEGER NOT NULL DEFAULT 0,
        cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
        last_error_code INTEGER,
        last_error_type TEXT,
        last_error TEXT DEFAULT '',
        session_input_tokens INTEGER NOT NULL DEFAULT 0,
        session_output_tokens INTEGER NOT NULL DEFAULT 0,
        token_expires_at_s INTEGER NOT NULL DEFAULT 0,
        created_at_ms INTEGER NOT NULL,
        status_changed_at_ms INTEGER NOT NULL,
        session_invalidated_count INTEGER NOT NULL DEFAULT 0,
        usage_limited_count INTEGER NOT NULL DEFAULT 0,
        token_version INTEGER NOT NULL DEFAULT 0,
        last_refresh_at_ms INTEGER NOT NULL DEFAULT 0,
        verified_at_ms INTEGER,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
      CREATE INDEX IF NOT EXISTS idx_accounts_status_cooldown ON accounts(status, cooldown_until_ms);
      CREATE INDEX IF NOT EXISTS idx_accounts_token_expiry ON accounts(token_expires_at_s);
      CREATE INDEX IF NOT EXISTS idx_accounts_last_refresh ON accounts(last_refresh_at_ms);
      DROP INDEX IF EXISTS idx_accounts_email_prefix;

      CREATE TABLE IF NOT EXISTS account_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER,
        email TEXT NOT NULL,
        event_type TEXT NOT NULL,
        old_status TEXT,
        new_status TEXT,
        detail TEXT DEFAULT '{}',
        status_code INTEGER,
        error_type TEXT,
        payload_json TEXT DEFAULT '{}',
        created_at_ms INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_account_events_email_time ON account_events(email, created_at_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_account_events_account_time ON account_events(account_id, created_at_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_account_events_type_time ON account_events(event_type, created_at_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_account_events_created_at ON account_events(created_at_ms);

      CREATE TABLE IF NOT EXISTS discord_users (
        discord_user_id TEXT PRIMARY KEY,
        seq_id TEXT NOT NULL UNIQUE,
        username TEXT NOT NULL DEFAULT '',
        global_name TEXT NOT NULL DEFAULT '',
        avatar TEXT NOT NULL DEFAULT '',
        roles_json TEXT NOT NULL DEFAULT '[]',
        api_key_id TEXT NOT NULL DEFAULT '',
        api_key_hash TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'active',
        created_at_ms INTEGER NOT NULL,
        last_login_at_ms INTEGER NOT NULL,
        usage_requests_today INTEGER NOT NULL DEFAULT 0,
        usage_tokens_today INTEGER NOT NULL DEFAULT 0,
        usage_requests_total INTEGER NOT NULL DEFAULT 0,
        usage_tokens_total INTEGER NOT NULL DEFAULT 0,
        usage_day TEXT NOT NULL DEFAULT '',
        risk_json TEXT NOT NULL DEFAULT '{}',
        banned_reason TEXT NOT NULL DEFAULT '',
        banned_at_ms INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE UNIQUE INDEX IF NOT EXISTS idx_discord_users_api_key_hash
        ON discord_users(api_key_hash)
        WHERE api_key_hash <> '';
      CREATE INDEX IF NOT EXISTS idx_discord_users_status ON discord_users(status);
      CREATE INDEX IF NOT EXISTS idx_discord_users_last_login ON discord_users(last_login_at_ms DESC);

      CREATE TABLE IF NOT EXISTS discord_sessions (
        session_hash TEXT PRIMARY KEY,
        discord_user_id TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        ip TEXT NOT NULL DEFAULT '',
        ua TEXT NOT NULL DEFAULT '',
        updated_at_ms INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_discord_sessions_user ON discord_sessions(discord_user_id);
      CREATE INDEX IF NOT EXISTS idx_discord_sessions_expires ON discord_sessions(expires_at_ms);

      CREATE TABLE IF NOT EXISTS admin_sessions (
        session_hash TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        ip TEXT NOT NULL DEFAULT '',
        updated_at_ms INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_admin_sessions_user ON admin_sessions(username);
      CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires ON admin_sessions(expires_at_ms);

      CREATE TABLE IF NOT EXISTS stats_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER NOT NULL,
        date_key TEXT NOT NULL,
        route TEXT NOT NULL DEFAULT '',
        path TEXT NOT NULL DEFAULT '',
        model TEXT NOT NULL DEFAULT '',
        account_email TEXT NOT NULL DEFAULT '',
        caller_identity TEXT NOT NULL DEFAULT '',
        status INTEGER NOT NULL DEFAULT 0,
        latency_ms INTEGER NOT NULL DEFAULT 0,
        ttfb_ms INTEGER NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cached_tokens INTEGER NOT NULL DEFAULT 0,
        reasoning_tokens INTEGER NOT NULL DEFAULT 0,
        error_type TEXT,
        stream INTEGER NOT NULL DEFAULT 0,
        ip TEXT NOT NULL DEFAULT '',
        ua_hash TEXT NOT NULL DEFAULT '',
        session_hint TEXT NOT NULL DEFAULT '',
        created_at_ms INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_stats_requests_ts ON stats_requests(ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_stats_requests_date_ts ON stats_requests(date_key, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_stats_requests_caller_ts ON stats_requests(caller_identity, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_stats_requests_account_ts ON stats_requests(account_email, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_stats_requests_model_ts ON stats_requests(model, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_stats_requests_status_ts ON stats_requests(status, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_stats_requests_error_ts ON stats_requests(error_type, ts_ms DESC);

      CREATE TABLE IF NOT EXISTS stats_hourly_overview (
        hour_bucket INTEGER PRIMARY KEY,
        date_key TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        success_requests INTEGER NOT NULL DEFAULT 0,
        error_requests INTEGER NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cached_tokens INTEGER NOT NULL DEFAULT 0,
        reasoning_tokens INTEGER NOT NULL DEFAULT 0,
        latency_total_ms INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_stats_hourly_overview_date ON stats_hourly_overview(date_key, hour_bucket);

      CREATE TABLE IF NOT EXISTS stats_hourly_model (
        hour_bucket INTEGER NOT NULL,
        model TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        success_requests INTEGER NOT NULL DEFAULT 0,
        error_requests INTEGER NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cached_tokens INTEGER NOT NULL DEFAULT 0,
        reasoning_tokens INTEGER NOT NULL DEFAULT 0,
        latency_total_ms INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (hour_bucket, model)
      );
      CREATE INDEX IF NOT EXISTS idx_stats_hourly_model_model_time ON stats_hourly_model(model, hour_bucket DESC);

      CREATE TABLE IF NOT EXISTS stats_hourly_account (
        hour_bucket INTEGER NOT NULL,
        account_email TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        success_requests INTEGER NOT NULL DEFAULT 0,
        error_requests INTEGER NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cached_tokens INTEGER NOT NULL DEFAULT 0,
        reasoning_tokens INTEGER NOT NULL DEFAULT 0,
        latency_total_ms INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (hour_bucket, account_email)
      );
      CREATE INDEX IF NOT EXISTS idx_stats_hourly_account_email_time ON stats_hourly_account(account_email, hour_bucket DESC);

      CREATE TABLE IF NOT EXISTS stats_hourly_identity (
        hour_bucket INTEGER NOT NULL,
        caller_identity TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        success_requests INTEGER NOT NULL DEFAULT 0,
        error_requests INTEGER NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cached_tokens INTEGER NOT NULL DEFAULT 0,
        reasoning_tokens INTEGER NOT NULL DEFAULT 0,
        latency_total_ms INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (hour_bucket, caller_identity)
      );
      CREATE INDEX IF NOT EXISTS idx_stats_hourly_identity_time ON stats_hourly_identity(caller_identity, hour_bucket DESC);

      CREATE TABLE IF NOT EXISTS abuse_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER NOT NULL,
        date_key TEXT NOT NULL,
        caller_identity TEXT NOT NULL,
        ip TEXT NOT NULL DEFAULT '',
        ua_hash TEXT NOT NULL DEFAULT '',
        rule_id TEXT NOT NULL,
        score INTEGER NOT NULL DEFAULT 0,
        action TEXT NOT NULL DEFAULT 'observe',
        evidence_json TEXT NOT NULL DEFAULT '{}',
        created_at_ms INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_abuse_events_identity_time ON abuse_events(caller_identity, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_abuse_events_rule_time ON abuse_events(rule_id, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_abuse_events_action_time ON abuse_events(action, ts_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_abuse_events_date ON abuse_events(date_key, ts_ms DESC);

      CREATE TABLE IF NOT EXISTS runtime_state (
        state_key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL DEFAULT '{}',
        updated_at_ms INTEGER NOT NULL
      );
    `);

    const upsertColumns = UPSERT_COLUMNS.join(', ');
    const upsertBindings = UPSERT_COLUMNS.map((column) => '@' + column).join(', ');
    this._stmt.upsert = this._db.prepare(
      `INSERT OR REPLACE INTO accounts (${upsertColumns}) VALUES (${upsertBindings})`
    );
    this._stmt.getByEmail = this._db.prepare('SELECT * FROM accounts WHERE email = ? LIMIT 1');
    this._stmt.deleteByEmail = this._db.prepare('DELETE FROM accounts WHERE email = ?');
    this._stmt.countAll = this._db.prepare('SELECT COUNT(1) AS total FROM accounts');
    this._stmt.countByStatus = this._db.prepare('SELECT COUNT(1) AS total FROM accounts WHERE status = ?');
    this._ensureAccountEventsSchema();
    this._stmt.insertEvent = this._db.prepare(`
      INSERT INTO account_events (account_id, email, event_type, old_status, new_status, detail, status_code, error_type, payload_json, created_at_ms)
      VALUES (@account_id, @email, @event_type, @old_status, @new_status, @detail, @status_code, @error_type, @payload_json, @created_at_ms)
    `);
    this._stmt.cleanupEvents = this._db.prepare('DELETE FROM account_events WHERE created_at_ms < ?');
    this._stmt.upsertAdminSession = this._db.prepare(`
      INSERT INTO admin_sessions (session_hash, username, created_at_ms, expires_at_ms, ip, updated_at_ms)
      VALUES (@session_hash, @username, @created_at_ms, @expires_at_ms, @ip, @updated_at_ms)
      ON CONFLICT(session_hash) DO UPDATE SET
        username = excluded.username,
        created_at_ms = excluded.created_at_ms,
        expires_at_ms = excluded.expires_at_ms,
        ip = excluded.ip,
        updated_at_ms = excluded.updated_at_ms
    `);
    this._stmt.getAdminSession = this._db.prepare('SELECT * FROM admin_sessions WHERE session_hash = ? LIMIT 1');
    this._stmt.deleteAdminSession = this._db.prepare('DELETE FROM admin_sessions WHERE session_hash = ?');
    this._stmt.cleanupAdminSessions = this._db.prepare('DELETE FROM admin_sessions WHERE expires_at_ms > 0 AND expires_at_ms <= ?');
    this._stmt.listAdminSessions = this._db.prepare('SELECT * FROM admin_sessions ORDER BY created_at_ms DESC');
    this._stmt.updateByKey = new Map();
    return this;
  }

  _ensureAccountEventsSchema() {
    const rows = this._db.prepare('PRAGMA table_info(account_events)').all();
    const names = new Set(rows.map((row) => row && row.name).filter(Boolean));
    const alterSqlList = [];
    if (!names.has('account_id')) alterSqlList.push('ALTER TABLE account_events ADD COLUMN account_id INTEGER');
    if (!names.has('old_status')) alterSqlList.push('ALTER TABLE account_events ADD COLUMN old_status TEXT');
    if (!names.has('new_status')) alterSqlList.push('ALTER TABLE account_events ADD COLUMN new_status TEXT');
    if (!names.has('detail')) alterSqlList.push("ALTER TABLE account_events ADD COLUMN detail TEXT DEFAULT '{}'");
    if (!names.has('status_code')) alterSqlList.push('ALTER TABLE account_events ADD COLUMN status_code INTEGER');
    if (!names.has('error_type')) alterSqlList.push('ALTER TABLE account_events ADD COLUMN error_type TEXT');
    if (!names.has('payload_json')) alterSqlList.push("ALTER TABLE account_events ADD COLUMN payload_json TEXT DEFAULT '{}'");
    for (const sql of alterSqlList) {
      this._db.exec(sql);
    }
    this._db.exec('CREATE INDEX IF NOT EXISTS idx_account_events_account_time ON account_events(account_id, created_at_ms DESC)');
    this._db.exec('CREATE INDEX IF NOT EXISTS idx_account_events_type_time ON account_events(event_type, created_at_ms DESC)');
  }

  _assertReady() {
    if (!this._db) {
      throw new Error('SqliteAccountRepository is not initialized');
    }
  }

  async close() {
    if (!this._db) return;
    this._db.close();
    this._db = null;
    this._stmt = {};
  }

  getDb() {
    this._assertReady();
    return this._db;
  }

  async upsertAdminSession(session) {
    this._assertReady();
    const input = session && typeof session === 'object' ? session : {};
    const row = {
      session_hash: String(input.session_hash ?? input.sessionHash ?? '').trim(),
      username: String(input.username || '').trim(),
      created_at_ms: toInteger(input.created_at_ms ?? input.createdAtMs, Date.now()),
      expires_at_ms: toInteger(input.expires_at_ms ?? input.expiresAtMs, 0),
      ip: String(input.ip || ''),
      updated_at_ms: toInteger(input.updated_at_ms ?? input.updatedAtMs, Date.now()),
    };
    if (!row.session_hash) {
      throw new Error('upsertAdminSession(session) requires session_hash');
    }
    this._stmt.upsertAdminSession.run(row);
    return row;
  }

  async getAdminSession(sessionHash) {
    this._assertReady();
    const hash = String(sessionHash || '').trim();
    if (!hash) return null;
    const row = this._stmt.getAdminSession.get(hash);
    if (!row) return null;
    return {
      session_hash: String(row.session_hash || ''),
      username: String(row.username || ''),
      created_at_ms: toInteger(row.created_at_ms, 0),
      expires_at_ms: toInteger(row.expires_at_ms, 0),
      ip: String(row.ip || ''),
      updated_at_ms: toInteger(row.updated_at_ms, 0),
    };
  }

  async listAdminSessions() {
    this._assertReady();
    const rows = this._stmt.listAdminSessions.all();
    return rows.map((row) => ({
      session_hash: String(row.session_hash || ''),
      username: String(row.username || ''),
      created_at_ms: toInteger(row.created_at_ms, 0),
      expires_at_ms: toInteger(row.expires_at_ms, 0),
      ip: String(row.ip || ''),
      updated_at_ms: toInteger(row.updated_at_ms, 0),
    }));
  }

  async deleteAdminSession(sessionHash) {
    this._assertReady();
    const hash = String(sessionHash || '').trim();
    if (!hash) return false;
    const result = this._stmt.deleteAdminSession.run(hash);
    return !!(result && result.changes > 0);
  }

  async cleanupAdminSessions(nowMs) {
    this._assertReady();
    const ts = toInteger(nowMs, Date.now());
    const result = this._stmt.cleanupAdminSessions.run(ts);
    return result && typeof result.changes === 'number' ? result.changes : 0;
  }

  async getByEmail(email) {
    this._assertReady();
    const key = String(email || '').trim();
    if (!key) return null;
    const row = this._stmt.getByEmail.get(key);
    return fromDbRow(row);
  }

  async getAll(filters) {
    this._assertReady();
    const opts = filters || {};
    let page = toInteger(opts.page, 1);
    let limit = toInteger(opts.limit, 50);
    if (page < 1) page = 1;
    if (limit < 1) limit = 1;
    if (limit > 200000) limit = 200000;
    const offset = (page - 1) * limit;

    const params = {};
    const whereParts = [];
    const statusClause = normalizeStatusFilter(opts.status, params);
    if (statusClause) whereParts.push(statusClause);

    const search = String(opts.search || '').trim().toLowerCase();
    if (search) {
      params.search = '%' + search + '%';
      whereParts.push('LOWER(email) LIKE @search');
    }
    const whereSql = whereParts.length ? 'WHERE ' + whereParts.join(' AND ') : '';

    const totalRow = this._db
      .prepare('SELECT COUNT(1) AS total FROM accounts ' + whereSql)
      .get(params);
    const total = totalRow ? toInteger(totalRow.total, 0) : 0;

    const rows = this._db
      .prepare('SELECT * FROM accounts ' + whereSql + ' ORDER BY ' + ACCOUNT_LIST_ORDER_SQL + ' LIMIT @limit OFFSET @offset')
      .all(Object.assign({}, params, { limit: limit, offset: offset }));

    return {
      accounts: rows.map(fromDbRow),
      total: total,
      page: page,
      limit: limit,
    };
  }

  loadAllSync() {
    this._assertReady();
    const rows = this._db.prepare('SELECT * FROM accounts').all();
    return rows.map(fromDbRow);
  }

  async getByStatus(status) {
    this._assertReady();
    const params = {};
    const clause = normalizeStatusFilter(status, params);
    const whereSql = clause ? ' WHERE ' + clause : '';
    const rows = this._db
      .prepare('SELECT * FROM accounts' + whereSql + ' ORDER BY ' + ACCOUNT_LIST_ORDER_SQL)
      .all(params);
    return rows.map(fromDbRow);
  }

  async upsert(account) {
    this._assertReady();
    const nowMs = Date.now();
    const record = toDbRecord(account, nowMs);
    if (!record.email) throw new Error('upsert(account) requires email');
    this._stmt.upsert.run(record);
    const saved = this._stmt.getByEmail.get(record.email);
    return fromDbRow(saved);
  }

  async upsertBatch(accounts) {
    this._assertReady();
    const list = Array.isArray(accounts) ? accounts : [];
    if (list.length === 0) return { processed: 0 };

    const chunkSize = this._batchSize > 0 ? this._batchSize : DEFAULT_BATCH_SIZE;
    const runChunk = this._db.transaction((chunk) => {
      const nowMs = Date.now();
      for (const item of chunk) {
        const record = toDbRecord(item, nowMs);
        if (!record.email) continue;
        this._stmt.upsert.run(record);
      }
    });

    for (let i = 0; i < list.length; i += chunkSize) {
      runChunk(list.slice(i, i + chunkSize));
    }
    return { processed: list.length };
  }

  async updateFields(email, fields) {
    this._assertReady();
    const key = String(email || '').trim();
    if (!key) throw new Error('updateFields(email, fields) requires email');
    if (!fields || typeof fields !== 'object') return null;

    const payload = buildUpdatePayload(key, fields, Date.now());
    if (!payload) {
      const existing = this._stmt.getByEmail.get(key);
      return fromDbRow(existing);
    }

    let stmt = this._stmt.updateByKey.get(payload.statementKey);
    if (!stmt) {
      const sql = 'UPDATE accounts SET ' + payload.assignments.join(', ') + ' WHERE email = @email';
      stmt = this._db.prepare(sql);
      this._stmt.updateByKey.set(payload.statementKey, stmt);
    }
    stmt.run(payload.params);
    const row = this._stmt.getByEmail.get(key);
    return fromDbRow(row);
  }

  async updateFieldsBatch(items) {
    this._assertReady();
    const list = Array.isArray(items) ? items : [];
    if (list.length === 0) return { processed: 0, updated: 0, failed: [] };

    const payloads = [];
    const failed = [];
    const nowMs = Date.now();

    for (const item of list) {
      const email = item && typeof item.email === 'string' ? item.email : '';
      const fields = item && item.fields && typeof item.fields === 'object' ? item.fields : null;
      try {
        const payload = buildUpdatePayload(email, fields, nowMs);
        if (payload) payloads.push(payload);
      } catch (_) {
        if (email) failed.push(String(email).trim());
      }
    }

    if (payloads.length === 0) {
      return { processed: list.length, updated: 0, failed: Array.from(new Set(failed.filter(Boolean))) };
    }

    const runBatch = this._db.transaction((entries) => {
      for (const payload of entries) {
        let stmt = this._stmt.updateByKey.get(payload.statementKey);
        if (!stmt) {
          const sql = 'UPDATE accounts SET ' + payload.assignments.join(', ') + ' WHERE email = @email';
          stmt = this._db.prepare(sql);
          this._stmt.updateByKey.set(payload.statementKey, stmt);
        }
        stmt.run(payload.params);
      }
    });

    let updated = 0;
    try {
      runBatch(payloads);
      updated = payloads.length;
    } catch (_) {
      for (const payload of payloads) {
        try {
          let stmt = this._stmt.updateByKey.get(payload.statementKey);
          if (!stmt) {
            const sql = 'UPDATE accounts SET ' + payload.assignments.join(', ') + ' WHERE email = @email';
            stmt = this._db.prepare(sql);
            this._stmt.updateByKey.set(payload.statementKey, stmt);
          }
          stmt.run(payload.params);
          updated++;
        } catch (_) {
          failed.push(payload.email);
        }
      }
    }

    return {
      processed: list.length,
      updated,
      failed: Array.from(new Set(failed.filter(Boolean))),
    };
  }

  async appendEvent(event) {
    this._assertReady();
    const record = normalizeEventRecord(event);
    if (!record) return { inserted: 0 };
    this._stmt.insertEvent.run(record);
    return { inserted: 1 };
  }

  async appendEventBatch(events) {
    this._assertReady();
    const list = Array.isArray(events) ? events : [];
    if (list.length === 0) return { processed: 0, inserted: 0 };

    const records = [];
    for (const item of list) {
      const record = normalizeEventRecord(item);
      if (record) records.push(record);
    }
    if (records.length === 0) return { processed: list.length, inserted: 0 };

    const runBatch = this._db.transaction((items) => {
      for (const record of items) {
        this._stmt.insertEvent.run(record);
      }
    });
    runBatch(records);
    return { processed: list.length, inserted: records.length };
  }

  async cleanupEvents(olderThanMs) {
    this._assertReady();
    const threshold = toInteger(olderThanMs, 0);
    if (threshold <= 0) return { deleted: 0 };
    const result = this._stmt.cleanupEvents.run(threshold);
    return { deleted: result && typeof result.changes === 'number' ? result.changes : 0 };
  }

  async delete(email) {
    this._assertReady();
    const key = String(email || '').trim();
    if (!key) return false;
    const result = this._stmt.deleteByEmail.run(key);
    return !!(result && result.changes > 0);
  }

  async count(status) {
    this._assertReady();
    const raw = String(status || '').trim();
    if (!raw) {
      const row = this._stmt.countAll.get();
      return row ? toInteger(row.total, 0) : 0;
    }
    const normalized = normalizeAccountStatus(raw);
    let row = null;
    if (normalized === 'cooldown') {
      row = this._db
        .prepare("SELECT COUNT(1) AS total FROM accounts WHERE status IN ('cooldown', 'usage_limited')")
        .get();
    } else if (normalized === 'wasted') {
      row = this._db
        .prepare("SELECT COUNT(1) AS total FROM accounts WHERE status IN ('wasted', 'relogin_needed')")
        .get();
    } else {
      row = this._stmt.countByStatus.get(normalized);
    }
    return row ? toInteger(row.total, 0) : 0;
  }

  async flush() {
    this._assertReady();
    try {
      this._db.pragma('wal_checkpoint(PASSIVE)');
    } catch (_) {
      return;
    }
  }
}
