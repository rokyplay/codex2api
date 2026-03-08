import crypto from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { createRequire } from 'node:module';

var require = createRequire(import.meta.url);

var LOG_LEVELS = ['info', 'warn', 'error', 'request'];
var DISCORD_USER_STATUS = ['active', 'banned', 'revoked'];
var ADMIN_SESSION_DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function loadBetterSqlite3() {
  try {
    require.resolve('better-sqlite3');
  } catch (_) {
    throw new Error('better-sqlite3 is not installed. Run: npm install better-sqlite3');
  }
  var loaded = require('better-sqlite3');
  return loaded && loaded.default ? loaded.default : loaded;
}

function toInt(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toNullableInt(value) {
  if (value === null || value === undefined || value === '') return null;
  var n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.floor(n);
}

function toTimestampMs(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0) return fallback;
    return Math.floor(value);
  }
  var text = String(value || '').trim();
  if (!text) return fallback;
  if (/^-?\d+(\.\d+)?$/.test(text)) {
    var direct = Number(text);
    if (Number.isFinite(direct) && direct > 0) return Math.floor(direct);
  }
  var parsed = Date.parse(text);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function normalizeObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value;
}

function normalizeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeLogLevel(level) {
  var raw = String(level || '').trim().toLowerCase();
  return LOG_LEVELS.indexOf(raw) >= 0 ? raw : 'info';
}

function normalizeDiscordStatus(status) {
  var raw = String(status || '').trim().toLowerCase();
  return DISCORD_USER_STATUS.indexOf(raw) >= 0 ? raw : 'active';
}

function normalizeUsage(raw) {
  var usage = normalizeObject(raw);
  var day = String(usage.day || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
    day = new Date().toISOString().slice(0, 10);
  }
  return {
    requests_today: Math.max(0, toInt(usage.requests_today, 0)),
    tokens_today: Math.max(0, toInt(usage.tokens_today, 0)),
    requests_total: Math.max(0, toInt(usage.requests_total, 0)),
    tokens_total: Math.max(0, toInt(usage.tokens_total, 0)),
    day: day,
  };
}

function normalizeRoles(raw) {
  var roles = normalizeArray(raw);
  var out = [];
  for (var i = 0; i < roles.length; i++) {
    var role = String(roles[i] || '').trim();
    if (!role || out.indexOf(role) >= 0) continue;
    out.push(role);
  }
  return out;
}

function parseSeqNumber(seqId) {
  var text = String(seqId || '').trim();
  if (!text) return 0;
  var matched = text.match(/^discord_(\d+)$/);
  var numericText = matched ? matched[1] : (/^\d+$/.test(text) ? text : '');
  if (!numericText) return 0;
  var n = Number(numericText);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.floor(n);
}

function formatSeqId(numberValue) {
  var n = Number(numberValue);
  if (!Number.isFinite(n) || n <= 0) return '';
  return 'discord_' + String(Math.floor(n));
}

function normalizeSeqId(seqId) {
  return formatSeqId(parseSeqNumber(seqId));
}

function safeParseJson(text, fallback) {
  try {
    return JSON.parse(String(text || ''));
  } catch (_) {
    return fallback;
  }
}

function stableStringify(value) {
  if (value === null) return 'null';
  var type = typeof value;
  if (type === 'boolean') return value ? 'true' : 'false';
  if (type === 'number') return Number.isFinite(value) ? String(value) : 'null';
  if (type === 'string') return JSON.stringify(value);
  if (type === 'bigint') return JSON.stringify(String(value));
  if (type === 'undefined' || type === 'function' || type === 'symbol') return 'null';
  if (Array.isArray(value)) {
    var items = [];
    for (var i = 0; i < value.length; i++) {
      items.push(stableStringify(value[i]));
    }
    return '[' + items.join(',') + ']';
  }
  if (type === 'object') {
    var keys = Object.keys(value).sort();
    var parts = [];
    for (var k = 0; k < keys.length; k++) {
      var key = keys[k];
      var v = value[key];
      if (typeof v === 'undefined' || typeof v === 'function' || typeof v === 'symbol') continue;
      parts.push(JSON.stringify(key) + ':' + stableStringify(v));
    }
    return '{' + parts.join(',') + '}';
  }
  return JSON.stringify(String(value));
}

function sha256Hex(text) {
  return crypto.createHash('sha256').update(String(text || ''), 'utf8').digest('hex');
}

function safeStat(filePath) {
  try {
    var st = statSync(filePath);
    if (!st || !st.isFile()) return null;
    return {
      file_path: filePath,
      file_mtime_ms: Math.floor(st.mtimeMs),
      file_size_bytes: Math.max(0, Number(st.size) || 0),
    };
  } catch (_) {
    return null;
  }
}

function normalizeAccountCookies(value) {
  if (!value) return {};
  if (typeof value === 'string') {
    var parsed = safeParseJson(value, {});
    return normalizeObject(parsed);
  }
  return normalizeObject(value);
}

function normalizeSourceAccount(raw) {
  var row = normalizeObject(raw);
  var nowMs = Date.now();
  var sessionUsage = normalizeObject(row.session_usage || row.sessionUsage);
  return {
    email: String(row.email || '').trim(),
    password: String(row.password || ''),
    access_token: String(row.accessToken || row.access_token || ''),
    session_token: String(row.sessionToken || row.session_token || ''),
    cookies_json: stableStringify(normalizeAccountCookies(row.cookies || row.cookies_json)),
    account_id: String(row.accountId || row.account_id || ''),
    status: String(row.status || 'active').toLowerCase(),
    request_count: Math.max(0, toInt(row.request_count !== undefined ? row.request_count : row.requestCount, 0)),
    last_request_at_ms: Math.max(0, toInt(row.last_request_at ?? row.last_request_at_ms ?? row.lastRequestAtMs, 0)),
    consecutive_errors: Math.max(0, toInt(row.consecutive_errors ?? row.consecutiveErrors, 0)),
    cooldown_until_ms: Math.max(0, toInt(row.cooldown_until ?? row.cooldown_until_ms ?? row.cooldownUntilMs, 0)),
    last_error_code: toNullableInt(row.last_error_code ?? row.lastErrorCode),
    last_error_type: row.last_error_type !== undefined || row.lastErrorType !== undefined
      ? String((row.last_error_type ?? row.lastErrorType) || '')
      : null,
    last_error: String(row.last_error || row.lastError || ''),
    session_input_tokens: Math.max(0, toInt(sessionUsage.input_tokens ?? sessionUsage.inputTokens, 0)),
    session_output_tokens: Math.max(0, toInt(sessionUsage.output_tokens ?? sessionUsage.outputTokens, 0)),
    token_expires_at_s: Math.max(0, toInt(row.token_expires_at ?? row.token_expires_at_s ?? row.tokenExpiresAtS, 0)),
    created_at_ms: Math.max(0, toInt(row.created_at ?? row.created_at_ms ?? row.createdAtMs, nowMs)),
    status_changed_at_ms: Math.max(0, toInt(row.status_changed_at ?? row.status_changed_at_ms ?? row.statusChangedAtMs, nowMs)),
    session_invalidated_count: Math.max(0, toInt(row.session_invalidated_count ?? row.sessionInvalidatedCount, 0)),
    usage_limited_count: Math.max(0, toInt(row.usage_limited_count ?? row.usageLimitedCount, 0)),
    token_version: Math.max(0, toInt(row._tokenVersion ?? row.token_version ?? row.tokenVersion, 0)),
    last_refresh_at_ms: Math.max(0, toInt(row._lastRefreshAt ?? row.last_refresh_at_ms ?? row.lastRefreshAtMs, 0)),
    verified_at_ms: toNullableInt(row.verified_at ?? row.verified_at_ms ?? row.verifiedAtMs),
    updated_at_ms: nowMs,
  };
}

function normalizeAbuseEvent(raw) {
  var row = normalizeObject(raw);
  var nowMs = Date.now();
  var tsMs = toTimestampMs(row.ts ?? row.timestamp ?? row.timestamp_ms, nowMs);
  return {
    ts_ms: tsMs,
    date_key: new Date(tsMs).toISOString().slice(0, 10),
    caller_identity: String(row.caller_identity || row.identity || '').trim(),
    ip: String(row.ip || '').trim(),
    ua_hash: String(row.ua_hash || '').trim(),
    rule_id: String(row.rule_id || '').trim(),
    score: Math.max(0, toInt(row.score, 0)),
    action: String(row.action || 'observe').trim() || 'observe',
    evidence_json: stableStringify(normalizeObject(row.evidence)),
    created_at_ms: tsMs,
  };
}

function runtimeLogDedupHash(row) {
  var metaObj = normalizeObject(row.meta);
  return sha256Hex(stableStringify({
    timestamp: Math.max(0, toInt(row.timestamp, 0)),
    level: normalizeLogLevel(row.level),
    message: String(row.message || ''),
    meta: metaObj,
  }));
}

function abuseEventDedupHash(row) {
  return sha256Hex(stableStringify({
    ts_ms: Math.max(0, toInt(row.ts_ms, 0)),
    caller_identity: String(row.caller_identity || ''),
    ip: String(row.ip || ''),
    ua_hash: String(row.ua_hash || ''),
    rule_id: String(row.rule_id || ''),
    score: Math.max(0, toInt(row.score, 0)),
    action: String(row.action || 'observe'),
    evidence_json: String(row.evidence_json || '{}'),
  }));
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS runtime_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp_ms INTEGER NOT NULL,
      level TEXT NOT NULL,
      message TEXT NOT NULL DEFAULT '',
      meta_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_logs_time ON runtime_logs(timestamp_ms DESC);
    CREATE INDEX IF NOT EXISTS idx_runtime_logs_level_time ON runtime_logs(level, timestamp_ms DESC);

    CREATE TABLE IF NOT EXISTS runtime_log_totals (
      singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
      total INTEGER NOT NULL DEFAULT 0,
      info INTEGER NOT NULL DEFAULT 0,
      warn INTEGER NOT NULL DEFAULT 0,
      error INTEGER NOT NULL DEFAULT 0,
      request INTEGER NOT NULL DEFAULT 0,
      updated_at_ms INTEGER NOT NULL DEFAULT 0
    );

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

    CREATE TABLE IF NOT EXISTS discord_users (
      discord_user_id TEXT PRIMARY KEY,
      seq_id TEXT NOT NULL UNIQUE,
      username TEXT NOT NULL DEFAULT '',
      global_name TEXT NOT NULL DEFAULT '',
      avatar TEXT NOT NULL DEFAULT '',
      roles_json TEXT NOT NULL DEFAULT '[]',
      api_key_id TEXT NOT NULL DEFAULT '',
      api_key_hash TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','banned','revoked')),
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

    CREATE TABLE IF NOT EXISTS discord_user_store_meta (
      meta_key TEXT PRIMARY KEY,
      meta_value TEXT NOT NULL DEFAULT '',
      updated_at_ms INTEGER NOT NULL
    );

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

    CREATE TABLE IF NOT EXISTS legacy_json_migrations (
      source_key TEXT PRIMARY KEY,
      file_path TEXT NOT NULL,
      file_mtime_ms INTEGER NOT NULL,
      file_size_bytes INTEGER NOT NULL,
      imported_rows INTEGER NOT NULL DEFAULT 0,
      updated_at_ms INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS legacy_json_row_dedup (
      source_key TEXT NOT NULL,
      row_hash TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (source_key, row_hash)
    );
  `);
}

function createStatements(db) {
  return {
    getMigration: db.prepare(`
      SELECT source_key, file_path, file_mtime_ms, file_size_bytes, imported_rows, updated_at_ms
      FROM legacy_json_migrations
      WHERE source_key = ?
      LIMIT 1
    `),
    upsertMigration: db.prepare(`
      INSERT INTO legacy_json_migrations (source_key, file_path, file_mtime_ms, file_size_bytes, imported_rows, updated_at_ms)
      VALUES (@source_key, @file_path, @file_mtime_ms, @file_size_bytes, @imported_rows, @updated_at_ms)
      ON CONFLICT(source_key) DO UPDATE SET
        file_path = excluded.file_path,
        file_mtime_ms = excluded.file_mtime_ms,
        file_size_bytes = excluded.file_size_bytes,
        imported_rows = excluded.imported_rows,
        updated_at_ms = excluded.updated_at_ms
    `),
    insertDedup: db.prepare(`
      INSERT OR IGNORE INTO legacy_json_row_dedup (source_key, row_hash, created_at_ms)
      VALUES (@source_key, @row_hash, @created_at_ms)
    `),
    getRuntimeState: db.prepare('SELECT value_json FROM runtime_state WHERE state_key = ? LIMIT 1'),
    upsertRuntimeState: db.prepare(`
      INSERT INTO runtime_state (state_key, value_json, updated_at_ms)
      VALUES (@state_key, @value_json, @updated_at_ms)
      ON CONFLICT(state_key) DO UPDATE SET
        value_json = excluded.value_json,
        updated_at_ms = excluded.updated_at_ms
    `),
    listRuntimeLogsForSeed: db.prepare('SELECT timestamp_ms, level, message, meta_json FROM runtime_logs'),
    listAbuseEventsForSeed: db.prepare(`
      SELECT ts_ms, caller_identity, ip, ua_hash, rule_id, score, action, evidence_json
      FROM abuse_events
    `),
    insertRuntimeLog: db.prepare(`
      INSERT INTO runtime_logs (timestamp_ms, level, message, meta_json)
      VALUES (@timestamp_ms, @level, @message, @meta_json)
    `),
    computeRuntimeTotals: db.prepare(`
      SELECT
        COUNT(1) AS total,
        SUM(CASE WHEN level = 'info' THEN 1 ELSE 0 END) AS info,
        SUM(CASE WHEN level = 'warn' THEN 1 ELSE 0 END) AS warn,
        SUM(CASE WHEN level = 'error' THEN 1 ELSE 0 END) AS error,
        SUM(CASE WHEN level = 'request' THEN 1 ELSE 0 END) AS request
      FROM runtime_logs
    `),
    upsertRuntimeTotals: db.prepare(`
      INSERT INTO runtime_log_totals (singleton_id, total, info, warn, error, request, updated_at_ms)
      VALUES (1, @total, @info, @warn, @error, @request, @updated_at_ms)
      ON CONFLICT(singleton_id) DO UPDATE SET
        total = excluded.total,
        info = excluded.info,
        warn = excluded.warn,
        error = excluded.error,
        request = excluded.request,
        updated_at_ms = excluded.updated_at_ms
    `),
    upsertAdminSession: db.prepare(`
      INSERT INTO admin_sessions (session_hash, username, created_at_ms, expires_at_ms, ip, updated_at_ms)
      VALUES (@session_hash, @username, @created_at_ms, @expires_at_ms, @ip, @updated_at_ms)
      ON CONFLICT(session_hash) DO UPDATE SET
        username = excluded.username,
        created_at_ms = excluded.created_at_ms,
        expires_at_ms = excluded.expires_at_ms,
        ip = excluded.ip,
        updated_at_ms = excluded.updated_at_ms
    `),
    upsertDiscordSession: db.prepare(`
      INSERT INTO discord_sessions (session_hash, discord_user_id, created_at_ms, expires_at_ms, ip, ua, updated_at_ms)
      VALUES (@session_hash, @discord_user_id, @created_at_ms, @expires_at_ms, @ip, @ua, @updated_at_ms)
      ON CONFLICT(session_hash) DO UPDATE SET
        discord_user_id = excluded.discord_user_id,
        created_at_ms = excluded.created_at_ms,
        expires_at_ms = excluded.expires_at_ms,
        ip = excluded.ip,
        ua = excluded.ua,
        updated_at_ms = excluded.updated_at_ms
    `),
    selectDiscordUserSeqById: db.prepare(`
      SELECT discord_user_id, seq_id
      FROM discord_users
      WHERE discord_user_id = ?
      LIMIT 1
    `),
    selectDiscordUserBySeq: db.prepare(`
      SELECT discord_user_id
      FROM discord_users
      WHERE seq_id = ?
      LIMIT 1
    `),
    listDiscordSeq: db.prepare('SELECT seq_id FROM discord_users'),
    selectDiscordMetaNextSeq: db.prepare(`
      SELECT meta_value
      FROM discord_user_store_meta
      WHERE meta_key = 'next_seq'
      LIMIT 1
    `),
    upsertDiscordMetaNextSeq: db.prepare(`
      INSERT INTO discord_user_store_meta (meta_key, meta_value, updated_at_ms)
      VALUES ('next_seq', @meta_value, @updated_at_ms)
      ON CONFLICT(meta_key) DO UPDATE SET
        meta_value = excluded.meta_value,
        updated_at_ms = excluded.updated_at_ms
    `),
    upsertDiscordUser: db.prepare(`
      INSERT INTO discord_users (
        discord_user_id, seq_id, username, global_name, avatar, roles_json,
        api_key_id, api_key_hash, status, created_at_ms, last_login_at_ms,
        usage_requests_today, usage_tokens_today, usage_requests_total, usage_tokens_total,
        usage_day, risk_json, banned_reason, banned_at_ms, updated_at_ms
      ) VALUES (
        @discord_user_id, @seq_id, @username, @global_name, @avatar, @roles_json,
        @api_key_id, @api_key_hash, @status, @created_at_ms, @last_login_at_ms,
        @usage_requests_today, @usage_tokens_today, @usage_requests_total, @usage_tokens_total,
        @usage_day, @risk_json, @banned_reason, @banned_at_ms, @updated_at_ms
      )
      ON CONFLICT(discord_user_id) DO UPDATE SET
        seq_id = excluded.seq_id,
        username = excluded.username,
        global_name = excluded.global_name,
        avatar = excluded.avatar,
        roles_json = excluded.roles_json,
        api_key_id = excluded.api_key_id,
        api_key_hash = excluded.api_key_hash,
        status = excluded.status,
        created_at_ms = excluded.created_at_ms,
        last_login_at_ms = excluded.last_login_at_ms,
        usage_requests_today = excluded.usage_requests_today,
        usage_tokens_today = excluded.usage_tokens_today,
        usage_requests_total = excluded.usage_requests_total,
        usage_tokens_total = excluded.usage_tokens_total,
        usage_day = excluded.usage_day,
        risk_json = excluded.risk_json,
        banned_reason = excluded.banned_reason,
        banned_at_ms = excluded.banned_at_ms,
        updated_at_ms = excluded.updated_at_ms
    `),
    insertAccountIfMissing: db.prepare(`
      INSERT OR IGNORE INTO accounts (
        email, password, access_token, session_token, cookies_json, account_id, status,
        request_count, last_request_at_ms, consecutive_errors, cooldown_until_ms,
        last_error_code, last_error_type, last_error,
        session_input_tokens, session_output_tokens, token_expires_at_s,
        created_at_ms, status_changed_at_ms, session_invalidated_count, usage_limited_count,
        token_version, last_refresh_at_ms, verified_at_ms, updated_at_ms
      ) VALUES (
        @email, @password, @access_token, @session_token, @cookies_json, @account_id, @status,
        @request_count, @last_request_at_ms, @consecutive_errors, @cooldown_until_ms,
        @last_error_code, @last_error_type, @last_error,
        @session_input_tokens, @session_output_tokens, @token_expires_at_s,
        @created_at_ms, @status_changed_at_ms, @session_invalidated_count, @usage_limited_count,
        @token_version, @last_refresh_at_ms, @verified_at_ms, @updated_at_ms
      )
    `),
    insertAbuseEvent: db.prepare(`
      INSERT INTO abuse_events (
        ts_ms, date_key, caller_identity, ip, ua_hash, rule_id, score, action, evidence_json, created_at_ms
      ) VALUES (
        @ts_ms, @date_key, @caller_identity, @ip, @ua_hash, @rule_id, @score, @action, @evidence_json, @created_at_ms
      )
    `),
  };
}

function shouldSkipBySignature(stmt, sourceKey, fileSig) {
  var existing = stmt.getMigration.get(sourceKey);
  if (!existing) return false;
  return toInt(existing.file_mtime_ms, -1) === toInt(fileSig.file_mtime_ms, -2)
    && toInt(existing.file_size_bytes, -1) === toInt(fileSig.file_size_bytes, -2);
}

function updateMigrationSignature(stmt, sourceKey, fileSig, importedRows) {
  stmt.upsertMigration.run({
    source_key: sourceKey,
    file_path: String(fileSig.file_path || ''),
    file_mtime_ms: toInt(fileSig.file_mtime_ms, 0),
    file_size_bytes: Math.max(0, toInt(fileSig.file_size_bytes, 0)),
    imported_rows: Math.max(0, toInt(importedRows, 0)),
    updated_at_ms: Date.now(),
  });
}

function ensureDedupSeeded(db, stmt, logger) {
  var seedKey = 'legacy_json_dedup_seed_v1';
  var existing = stmt.getRuntimeState.get(seedKey);
  if (existing && existing.value_json) return;

  var nowMs = Date.now();
  var runtimeInserted = 0;
  var abuseInserted = 0;
  var runtimeRows = stmt.listRuntimeLogsForSeed.all();
  var abuseRows = stmt.listAbuseEventsForSeed.all();
  var insertDedupTx = db.transaction(function () {
    for (var i = 0; i < runtimeRows.length; i++) {
      var row = runtimeRows[i] || {};
      var hash = runtimeLogDedupHash({
        timestamp: toInt(row.timestamp_ms, 0),
        level: row.level,
        message: row.message,
        meta: safeParseJson(row.meta_json, {}),
      });
      var inserted = stmt.insertDedup.run({
        source_key: 'runtime_logs',
        row_hash: hash,
        created_at_ms: nowMs,
      });
      if (inserted && inserted.changes > 0) runtimeInserted += inserted.changes;
    }
    for (var j = 0; j < abuseRows.length; j++) {
      var abuse = abuseRows[j] || {};
      var abuseHash = abuseEventDedupHash({
        ts_ms: toInt(abuse.ts_ms, 0),
        caller_identity: abuse.caller_identity,
        ip: abuse.ip,
        ua_hash: abuse.ua_hash,
        rule_id: abuse.rule_id,
        score: abuse.score,
        action: abuse.action,
        evidence_json: String(abuse.evidence_json || '{}'),
      });
      var insertedAbuse = stmt.insertDedup.run({
        source_key: 'abuse_events',
        row_hash: abuseHash,
        created_at_ms: nowMs,
      });
      if (insertedAbuse && insertedAbuse.changes > 0) abuseInserted += insertedAbuse.changes;
    }
  });
  insertDedupTx();

  stmt.upsertRuntimeState.run({
    state_key: seedKey,
    value_json: stableStringify({
      runtime_seeded: runtimeInserted,
      abuse_seeded: abuseInserted,
      seeded_at_ms: nowMs,
    }),
    updated_at_ms: nowMs,
  });

  if (typeof logger === 'function') {
    logger('legacy JSON dedup seed done: runtime_logs=' + runtimeInserted + ', abuse_events=' + abuseInserted);
  }
}

function refreshRuntimeLogTotals(stmt) {
  var totals = stmt.computeRuntimeTotals.get() || {};
  stmt.upsertRuntimeTotals.run({
    total: Math.max(0, toInt(totals.total, 0)),
    info: Math.max(0, toInt(totals.info, 0)),
    warn: Math.max(0, toInt(totals.warn, 0)),
    error: Math.max(0, toInt(totals.error, 0)),
    request: Math.max(0, toInt(totals.request, 0)),
    updated_at_ms: Date.now(),
  });
}

function migrateLogsJson(db, stmt, sourceKey, filePath) {
  var text = readFileSync(filePath, 'utf8');
  var parsed = safeParseJson(text, {});
  var payload = normalizeObject(parsed);
  var logs = normalizeArray(payload.logs);
  var insertedRows = 0;
  var skippedRows = 0;
  var tx = db.transaction(function () {
    for (var i = 0; i < logs.length; i++) {
      var item = normalizeObject(logs[i]);
      var row = {
        timestamp: Math.max(0, toInt(item.timestamp !== undefined ? item.timestamp : item.timestamp_ms, Date.now())),
        level: normalizeLogLevel(item.level),
        message: String(item.message || ''),
        meta: normalizeObject(item.meta),
      };
      var hash = runtimeLogDedupHash(row);
      var inserted = stmt.insertDedup.run({
        source_key: 'runtime_logs',
        row_hash: hash,
        created_at_ms: Date.now(),
      });
      if (!inserted || inserted.changes <= 0) {
        skippedRows += 1;
        continue;
      }
      stmt.insertRuntimeLog.run({
        timestamp_ms: row.timestamp,
        level: row.level,
        message: row.message,
        meta_json: stableStringify(row.meta),
      });
      insertedRows += 1;
    }
  });
  tx();
  refreshRuntimeLogTotals(stmt);
  return {
    processed_rows: logs.length,
    inserted_rows: insertedRows,
    skipped_rows: skippedRows,
    source_key: sourceKey,
    file_path: filePath,
  };
}

function migrateAdminSessionsJson(db, stmt, sourceKey, filePath) {
  void db;
  var text = readFileSync(filePath, 'utf8');
  var parsed = normalizeObject(safeParseJson(text, {}));
  var keys = Object.keys(parsed);
  var upserted = 0;
  var tx = db.transaction(function () {
    for (var i = 0; i < keys.length; i++) {
      var hash = String(keys[i] || '').trim();
      if (!hash) continue;
      var row = normalizeObject(parsed[hash]);
      var createdAtMs = toTimestampMs(row.createdAt ?? row.created_at ?? row.created_at_ms, Date.now());
      var expiresAtMs = Math.max(createdAtMs + ADMIN_SESSION_DEFAULT_MAX_AGE_MS, toTimestampMs(row.expiresAt ?? row.expires_at_ms, 0));
      stmt.upsertAdminSession.run({
        session_hash: hash,
        username: String(row.username || ''),
        created_at_ms: createdAtMs,
        expires_at_ms: expiresAtMs,
        ip: String(row.ip || ''),
        updated_at_ms: Date.now(),
      });
      upserted += 1;
    }
  });
  tx();
  return {
    processed_rows: keys.length,
    inserted_rows: upserted,
    skipped_rows: 0,
    source_key: sourceKey,
    file_path: filePath,
  };
}

function migrateDiscordSessionsJson(db, stmt, sourceKey, filePath) {
  void db;
  var text = readFileSync(filePath, 'utf8');
  var parsed = normalizeObject(safeParseJson(text, {}));
  var sessions = normalizeObject(parsed.sessions);
  if (Object.keys(sessions).length === 0) {
    sessions = parsed;
  }
  var ttlHours = Math.max(1, toInt(parsed.session_ttl_hours, 48));
  var ttlMs = ttlHours * 60 * 60 * 1000;
  var keys = Object.keys(sessions);
  var upserted = 0;
  var tx = db.transaction(function () {
    for (var i = 0; i < keys.length; i++) {
      var hash = String(keys[i] || '').trim();
      if (!hash) continue;
      var item = normalizeObject(sessions[hash]);
      var createdAtMs = toTimestampMs(item.created_at ?? item.createdAt ?? item.created_at_ms, Date.now());
      var expiresAtMs = Math.max(createdAtMs + ttlMs, toTimestampMs(item.expires_at ?? item.expires_at_ms, 0));
      stmt.upsertDiscordSession.run({
        session_hash: hash,
        discord_user_id: String(item.discord_user_id || item.discordUserId || '').trim(),
        created_at_ms: createdAtMs,
        expires_at_ms: expiresAtMs,
        ip: String(item.ip || ''),
        ua: String(item.ua || ''),
        updated_at_ms: Date.now(),
      });
      upserted += 1;
    }
  });
  tx();
  return {
    processed_rows: keys.length,
    inserted_rows: upserted,
    skipped_rows: 0,
    source_key: sourceKey,
    file_path: filePath,
  };
}

function getDiscordNextSeq(stmt, fileNextSeq) {
  var maxSeq = 0;
  var seqRows = stmt.listDiscordSeq.all();
  for (var i = 0; i < seqRows.length; i++) {
    var parsed = parseSeqNumber(seqRows[i] && seqRows[i].seq_id);
    if (parsed > maxSeq) maxSeq = parsed;
  }
  var dbMeta = stmt.selectDiscordMetaNextSeq.get();
  var dbNext = dbMeta ? Math.max(1, toInt(dbMeta.meta_value, 1)) : 1;
  var fileNext = Math.max(1, toInt(fileNextSeq, 1));
  return Math.max(maxSeq + 1, dbNext, fileNext);
}

function resolveDiscordSeqId(stmt, discordUserId, suggestedSeqId, nextSeqRef) {
  var existingById = stmt.selectDiscordUserSeqById.get(discordUserId);
  if (existingById && existingById.seq_id) {
    var existingSeq = normalizeSeqId(existingById.seq_id);
    if (existingSeq) {
      var existingNum = parseSeqNumber(existingSeq);
      if (existingNum >= nextSeqRef.value) nextSeqRef.value = existingNum + 1;
      return existingSeq;
    }
  }

  var normalized = normalizeSeqId(suggestedSeqId);
  if (normalized) {
    var owner = stmt.selectDiscordUserBySeq.get(normalized);
    if (!owner || String(owner.discord_user_id || '') === String(discordUserId || '')) {
      var parsed = parseSeqNumber(normalized);
      if (parsed >= nextSeqRef.value) nextSeqRef.value = parsed + 1;
      return normalized;
    }
  }

  while (true) {
    var generated = formatSeqId(nextSeqRef.value);
    nextSeqRef.value += 1;
    if (!generated) continue;
    var owner2 = stmt.selectDiscordUserBySeq.get(generated);
    if (!owner2 || String(owner2.discord_user_id || '') === String(discordUserId || '')) {
      return generated;
    }
  }
}

function migrateDiscordUsersJson(db, stmt, sourceKey, filePath) {
  var text = readFileSync(filePath, 'utf8');
  var parsed = normalizeObject(safeParseJson(text, {}));
  var usersObj = normalizeObject(parsed.users);
  if (Object.keys(usersObj).length === 0) {
    usersObj = parsed;
  }
  var keys = Object.keys(usersObj);
  var nextSeqRef = { value: getDiscordNextSeq(stmt, parsed.meta && parsed.meta.next_seq) };
  var nowMs = Date.now();
  var upserted = 0;

  var tx = db.transaction(function () {
    for (var i = 0; i < keys.length; i++) {
      var key = String(keys[i] || '').trim();
      var item = normalizeObject(usersObj[key]);
      var discordUserId = String(item.discord_user_id || key).trim();
      if (!discordUserId) continue;
      var seqId = resolveDiscordSeqId(stmt, discordUserId, item.seq_id, nextSeqRef);
      var createdAtMs = toTimestampMs(item.created_at ?? item.createdAt ?? item.created_at_ms, nowMs);
      var lastLoginAtMs = toTimestampMs(item.last_login_at ?? item.lastLoginAt ?? item.last_login_at_ms, createdAtMs);
      var usage = normalizeUsage(item.usage);
      var roles = normalizeRoles(item.roles);
      var risk = normalizeObject(item.risk);
      var bannedAtMs = toTimestampMs(item.banned_at ?? item.bannedAt ?? item.banned_at_ms, 0);

      stmt.upsertDiscordUser.run({
        discord_user_id: discordUserId,
        seq_id: seqId || resolveDiscordSeqId(stmt, discordUserId, '', nextSeqRef),
        username: String(item.username || ''),
        global_name: String(item.global_name || ''),
        avatar: String(item.avatar || ''),
        roles_json: stableStringify(roles),
        api_key_id: String(item.api_key_id || ''),
        api_key_hash: String(item.api_key_hash || ''),
        status: normalizeDiscordStatus(item.status),
        created_at_ms: Math.max(0, toInt(createdAtMs, nowMs)),
        last_login_at_ms: Math.max(0, toInt(lastLoginAtMs, createdAtMs)),
        usage_requests_today: usage.requests_today,
        usage_tokens_today: usage.tokens_today,
        usage_requests_total: usage.requests_total,
        usage_tokens_total: usage.tokens_total,
        usage_day: usage.day,
        risk_json: stableStringify(risk),
        banned_reason: String(item.banned_reason || ''),
        banned_at_ms: Math.max(0, toInt(bannedAtMs, 0)),
        updated_at_ms: nowMs,
      });
      upserted += 1;
    }

    stmt.upsertDiscordMetaNextSeq.run({
      meta_value: String(Math.max(1, nextSeqRef.value)),
      updated_at_ms: nowMs,
    });
  });
  tx();

  return {
    processed_rows: keys.length,
    inserted_rows: upserted,
    skipped_rows: 0,
    source_key: sourceKey,
    file_path: filePath,
  };
}

function migrateAccountsStateJson(db, stmt, sourceKey, filePath) {
  var text = readFileSync(filePath, 'utf8');
  var parsed = safeParseJson(text, []);
  var accounts = Array.isArray(parsed)
    ? parsed
    : (Array.isArray(parsed && parsed.accounts) ? parsed.accounts : []);
  var inserted = 0;
  var skipped = 0;
  var tx = db.transaction(function () {
    for (var i = 0; i < accounts.length; i++) {
      var normalized = normalizeSourceAccount(accounts[i]);
      if (!normalized.email || !normalized.access_token) {
        skipped += 1;
        continue;
      }
      var result = stmt.insertAccountIfMissing.run(normalized);
      if (result && result.changes > 0) {
        inserted += result.changes;
      } else {
        skipped += 1;
      }
    }
  });
  tx();
  return {
    processed_rows: accounts.length,
    inserted_rows: inserted,
    skipped_rows: skipped,
    source_key: sourceKey,
    file_path: filePath,
  };
}

function migrateAbuseJsonlFile(db, stmt, sourceKey, filePath) {
  var text = readFileSync(filePath, 'utf8');
  var lines = String(text || '').split(/\r?\n/);
  var processed = 0;
  var inserted = 0;
  var skipped = 0;
  var tx = db.transaction(function () {
    for (var i = 0; i < lines.length; i++) {
      var line = String(lines[i] || '').trim();
      if (!line) continue;
      processed += 1;
      var parsed = safeParseJson(line, null);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        skipped += 1;
        continue;
      }
      var row = normalizeAbuseEvent(parsed);
      if (!row.caller_identity || !row.rule_id) {
        skipped += 1;
        continue;
      }
      var hash = abuseEventDedupHash(row);
      var dedup = stmt.insertDedup.run({
        source_key: 'abuse_events',
        row_hash: hash,
        created_at_ms: Date.now(),
      });
      if (!dedup || dedup.changes <= 0) {
        skipped += 1;
        continue;
      }
      stmt.insertAbuseEvent.run(row);
      inserted += 1;
    }
  });
  tx();
  return {
    processed_rows: processed,
    inserted_rows: inserted,
    skipped_rows: skipped,
    source_key: sourceKey,
    file_path: filePath,
  };
}

function runFileMigration(summary, stmt, sourceKey, filePath, migrateFn, logger) {
  var sig = safeStat(filePath);
  if (!sig) {
    summary.skipped_by_missing += 1;
    return;
  }
  if (shouldSkipBySignature(stmt, sourceKey, sig)) {
    summary.skipped_by_signature += 1;
    return;
  }
  try {
    var result = migrateFn(sourceKey, filePath);
    updateMigrationSignature(stmt, sourceKey, sig, result.inserted_rows);
    summary.sources.push(result);
    summary.total_processed += Math.max(0, toInt(result.processed_rows, 0));
    summary.total_inserted += Math.max(0, toInt(result.inserted_rows, 0));
    summary.total_skipped_rows += Math.max(0, toInt(result.skipped_rows, 0));
    if (typeof logger === 'function') {
      logger(
        'legacy migrate [' + sourceKey + ']: processed=' + result.processed_rows
        + ', inserted=' + result.inserted_rows
        + ', skipped=' + result.skipped_rows
      );
    }
  } catch (err) {
    var message = err && err.message ? err.message : String(err);
    summary.errors.push(sourceKey + ': ' + message);
    if (typeof logger === 'function') {
      logger('legacy migrate [' + sourceKey + '] failed: ' + message);
    }
  }
}

export function migrateLegacyJsonToSqlite(options) {
  var opts = normalizeObject(options);
  var dataDir = resolve(String(opts.dataDir || resolve(process.cwd(), 'data')));
  var dbPath = resolve(String(opts.dbPath || resolve(dataDir, 'accounts.db')));
  var logger = typeof opts.logger === 'function' ? opts.logger : null;

  var summary = {
    db_path: dbPath,
    data_dir: dataDir,
    migrated_at: new Date().toISOString(),
    sources: [],
    total_processed: 0,
    total_inserted: 0,
    total_skipped_rows: 0,
    skipped_by_signature: 0,
    skipped_by_missing: 0,
    errors: [],
  };

  if (!existsSync(dataDir)) {
    summary.errors.push('data directory not found: ' + dataDir);
    return summary;
  }

  var dbDir = dirname(dbPath);
  if (!existsSync(dbDir)) {
    mkdirSync(dbDir, { recursive: true });
  }

  var Database = loadBetterSqlite3();
  var db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');

  try {
    ensureSchema(db);
    var stmt = createStatements(db);
    ensureDedupSeeded(db, stmt, logger);

    runFileMigration(
      summary,
      stmt,
      'legacy_logs_json',
      join(dataDir, 'logs.json'),
      function (sourceKey, filePath) { return migrateLogsJson(db, stmt, sourceKey, filePath); },
      logger
    );

    runFileMigration(
      summary,
      stmt,
      'legacy_admin_sessions_json',
      join(dataDir, 'admin-sessions.json'),
      function (sourceKey, filePath) { return migrateAdminSessionsJson(db, stmt, sourceKey, filePath); },
      logger
    );

    runFileMigration(
      summary,
      stmt,
      'legacy_discord_sessions_json',
      join(dataDir, 'discord-sessions.json'),
      function (sourceKey, filePath) { return migrateDiscordSessionsJson(db, stmt, sourceKey, filePath); },
      logger
    );

    runFileMigration(
      summary,
      stmt,
      'legacy_discord_users_json',
      join(dataDir, 'discord-users.json'),
      function (sourceKey, filePath) { return migrateDiscordUsersJson(db, stmt, sourceKey, filePath); },
      logger
    );

    runFileMigration(
      summary,
      stmt,
      'legacy_accounts_state_json',
      join(dataDir, 'accounts-state.json'),
      function (sourceKey, filePath) { return migrateAccountsStateJson(db, stmt, sourceKey, filePath); },
      logger
    );

    var abuseDir = join(dataDir, 'abuse');
    if (existsSync(abuseDir)) {
      var names = readdirSync(abuseDir).filter(function (name) { return /\.jsonl$/i.test(name); }).sort();
      for (var i = 0; i < names.length; i++) {
        var name = names[i];
        var sourceKey = 'legacy_abuse_jsonl:' + name;
        var filePath = join(abuseDir, name);
        runFileMigration(
          summary,
          stmt,
          sourceKey,
          filePath,
          function (_, actualPath) { return migrateAbuseJsonlFile(db, stmt, sourceKey, actualPath); },
          logger
        );
      }
    }

    refreshRuntimeLogTotals(stmt);
  } catch (err) {
    summary.errors.push(err && err.message ? err.message : String(err));
  } finally {
    try {
      db.close();
    } catch (_) {}
  }

  return summary;
}
