import crypto from 'node:crypto';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { log, C } from '../utils.mjs';

var __filename = fileURLToPath(import.meta.url);
var __dirname = dirname(__filename);
var require = createRequire(import.meta.url);
var DEFAULT_DB_PATH = resolve(__dirname, '../../data/accounts.db');
var SAVE_DEBOUNCE_MS = 500;

function nowIso() {
  return new Date().toISOString();
}

function todayKey() {
  return new Date().toISOString().slice(0, 10);
}

function safeClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function sha256Hex(text) {
  return crypto.createHash('sha256').update(String(text || ''), 'utf8').digest('hex');
}

function safeHashEqual(a, b) {
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

function loadBetterSqlite3() {
  try {
    require.resolve('better-sqlite3');
  } catch (_) {
    throw new Error('better-sqlite3 is not installed. Run: npm install better-sqlite3');
  }
  var loaded = require('better-sqlite3');
  return loaded && loaded.default ? loaded.default : loaded;
}

function toTimestampMs(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'number') {
    if (!isFinite(value)) return fallback;
    return Math.floor(value);
  }
  var parsed = Date.parse(String(value || ''));
  if (!isFinite(parsed)) return fallback;
  return Math.floor(parsed);
}

function toIsoString(ms, fallback) {
  var n = Number(ms);
  if (!isFinite(n) || n <= 0) return fallback;
  return new Date(Math.floor(n)).toISOString();
}

function toNonNegativeInt(value, fallback) {
  var n = Number(value);
  if (!isFinite(n)) return fallback || 0;
  n = Math.floor(n);
  if (n < 0) return 0;
  return n;
}

function safeJsonParseObject(text, fallback) {
  try {
    var parsed = JSON.parse(String(text || ''));
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
    return fallback;
  } catch (_) {
    return fallback;
  }
}

function safeJsonParseArray(text, fallback) {
  try {
    var parsed = JSON.parse(String(text || ''));
    if (Array.isArray(parsed)) return parsed;
    return fallback;
  } catch (_) {
    return fallback;
  }
}

function safeJsonStringify(value, fallback) {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return fallback;
  }
}

function normalizeRoles(roles) {
  if (!Array.isArray(roles)) return [];
  var out = [];
  for (var i = 0; i < roles.length; i++) {
    var role = String(roles[i] || '').trim();
    if (!role) continue;
    if (out.indexOf(role) >= 0) continue;
    out.push(role);
  }
  return out;
}

function normalizeStatus(status) {
  status = String(status || '').toLowerCase();
  if (status === 'active' || status === 'banned' || status === 'revoked') {
    return status;
  }
  return 'active';
}

function parseSeqNumber(seqId) {
  var raw = String(seqId || '').trim();
  if (!raw) return 0;
  var matched = raw.match(/^discord_(\d+)$/);
  if (!matched) matched = raw.match(/^discord:discord_(\d+)$/);
  var numericText = matched ? matched[1] : (/^\d+$/.test(raw) ? raw : '');
  if (!numericText) return 0;
  var n = Number(numericText);
  if (!isFinite(n) || n <= 0) return 0;
  return Math.floor(n);
}

function formatSeqId(numberValue) {
  var n = Number(numberValue);
  if (!isFinite(n) || n <= 0) return '';
  return 'discord_' + String(Math.floor(n));
}

function normalizeSeqId(seqId) {
  return formatSeqId(parseSeqNumber(seqId));
}

function normalizeStoreMeta(raw) {
  raw = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : {};
  var nextSeq = Number(raw.next_seq);
  if (!isFinite(nextSeq) || nextSeq <= 0) nextSeq = 1;
  return {
    next_seq: Math.floor(nextSeq),
  };
}

function parseCreatedAtTs(isoString) {
  var ts = Date.parse(String(isoString || ''));
  if (!isFinite(ts)) return Number.MAX_SAFE_INTEGER;
  return ts;
}

function normalizeUsage(raw) {
  raw = raw && typeof raw === 'object' ? raw : {};
  var usage = {
    requests_today: Number(raw.requests_today) || 0,
    tokens_today: Number(raw.tokens_today) || 0,
    requests_total: Number(raw.requests_total) || 0,
    tokens_total: Number(raw.tokens_total) || 0,
    day: String(raw.day || todayKey()),
  };
  if (usage.requests_today < 0) usage.requests_today = 0;
  if (usage.tokens_today < 0) usage.tokens_today = 0;
  if (usage.requests_total < 0) usage.requests_total = 0;
  if (usage.tokens_total < 0) usage.tokens_total = 0;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(usage.day)) {
    usage.day = todayKey();
  }
  return usage;
}

function defaultRiskState() {
  return {
    score: 0,
    level: 'low',
    reasons: [],
    flags: {},
    actions: {
      suggested: 'observe',
      applied: 'observe',
      manual: '',
      manual_reason: '',
      manual_set_at: '',
    },
    last_eval_at: '',
    last_auto_action_at: '',
  };
}

function normalizeRiskLevel(level) {
  var value = String(level || '').trim().toLowerCase();
  if (value === 'low' || value === 'medium' || value === 'high' || value === 'critical') {
    return value;
  }
  return 'low';
}

function normalizeRiskActions(raw) {
  raw = raw && typeof raw === 'object' ? raw : {};
  return {
    suggested: String(raw.suggested || 'observe'),
    applied: String(raw.applied || 'observe'),
    manual: String(raw.manual || ''),
    manual_reason: String(raw.manual_reason || ''),
    manual_set_at: String(raw.manual_set_at || ''),
  };
}

function normalizeRisk(raw) {
  var base = defaultRiskState();
  raw = raw && typeof raw === 'object' ? raw : {};
  base.score = Number(raw.score) || 0;
  if (base.score < 0) base.score = 0;
  base.level = normalizeRiskLevel(raw.level || base.level);
  base.reasons = Array.isArray(raw.reasons) ? safeClone(raw.reasons).slice(-50) : [];
  base.flags = raw.flags && typeof raw.flags === 'object' && !Array.isArray(raw.flags) ? safeClone(raw.flags) : {};
  base.actions = normalizeRiskActions(raw.actions);
  base.last_eval_at = String(raw.last_eval_at || '');
  base.last_auto_action_at = String(raw.last_auto_action_at || '');
  return base;
}

function rollDailyUsageIfNeeded(user) {
  var day = todayKey();
  if (!user || !user.usage) return;
  if (user.usage.day === day) return;
  user.usage.day = day;
  user.usage.requests_today = 0;
  user.usage.tokens_today = 0;
}

function normalizeUserRecord(raw) {
  raw = raw && typeof raw === 'object' ? raw : {};
  var now = nowIso();
  return {
    discord_user_id: String(raw.discord_user_id || ''),
    seq_id: normalizeSeqId(raw.seq_id),
    username: String(raw.username || ''),
    global_name: String(raw.global_name || ''),
    avatar: String(raw.avatar || ''),
    roles: normalizeRoles(raw.roles),
    api_key_id: String(raw.api_key_id || ''),
    api_key_hash: String(raw.api_key_hash || ''),
    status: normalizeStatus(raw.status),
    created_at: String(raw.created_at || now),
    last_login_at: String(raw.last_login_at || now),
    usage: normalizeUsage(raw.usage),
    risk: normalizeRisk(raw.risk),
    banned_reason: raw.banned_reason ? String(raw.banned_reason) : '',
    banned_at: raw.banned_at ? String(raw.banned_at) : '',
  };
}

export function createDiscordUserStore(config) {
  var cfg = config || {};
  var dbPath = resolve(String(cfg.db_path || cfg.sqlite_path || DEFAULT_DB_PATH));
  var apiKeyPrefix = String(cfg.api_key_prefix || 'dk-');

  /** @type {Map<string, any>} */
  var users = new Map();
  var nextSeq = 1;
  var saveTimer = null;
  var savePending = false;
  var saveInFlight = false;
  var db = null;
  var insertStmt = null;
  var loadAllStmt = null;
  var loadByDiscordIdStmt = null;
  var loadBySeqIdStmt = null;
  var loadMetaNextSeqStmt = null;
  var upsertMetaNextSeqStmt = null;

  function syncNextSeqFromUsers() {
    var maxSeq = 0;
    for (var user of users.values()) {
      var seqNum = parseSeqNumber(user && user.seq_id);
      if (seqNum > maxSeq) maxSeq = seqNum;
    }
    if (nextSeq <= maxSeq) {
      nextSeq = maxSeq + 1;
    }
    if (nextSeq < 1) nextSeq = 1;
  }

  function allocateSeqId() {
    syncNextSeqFromUsers();
    var seqId = formatSeqId(nextSeq);
    nextSeq += 1;
    return seqId;
  }

  function normalizeExistingUserSeqId(user) {
    if (!user || typeof user !== 'object') return false;
    var normalized = normalizeSeqId(user.seq_id);
    var changed = user.seq_id !== normalized;
    user.seq_id = normalized;
    var current = parseSeqNumber(normalized);
    if (current >= nextSeq) nextSeq = current + 1;
    return changed;
  }

  function ensureUserSeqId(user) {
    if (!user || typeof user !== 'object') return false;
    normalizeExistingUserSeqId(user);
    var current = parseSeqNumber(user.seq_id);
    if (current > 0) {
      user.seq_id = formatSeqId(current);
      if (current >= nextSeq) nextSeq = current + 1;
      return false;
    }
    var rebuilt = migrateSeqIdsOnLoad();
    if (parseSeqNumber(user.seq_id) > 0) {
      return rebuilt;
    }
    user.seq_id = allocateSeqId();
    return true;
  }

  function migrateSeqIdsOnLoad() {
    if (users.size === 0) {
      nextSeq = 1;
      return false;
    }

    var changed = false;
    var rows = [];
    for (var entry of users.entries()) {
      var user = entry[1];
      if (!user || typeof user !== 'object') continue;
      rows.push(user);
    }

    rows.sort(function (a, b) {
      var diff = parseCreatedAtTs(a && a.created_at) - parseCreatedAtTs(b && b.created_at);
      if (diff !== 0) return diff;
      return String((a && a.discord_user_id) || '').localeCompare(String((b && b.discord_user_id) || ''));
    });

    for (var i = 0; i < rows.length; i++) {
      var target = rows[i];
      if (!target) continue;
      var expected = formatSeqId(i + 1);
      if (target.seq_id !== expected) {
        target.seq_id = expected;
        changed = true;
      }
    }
    nextSeq = rows.length + 1;
    return changed;
  }

  function loadNextSeqFromDbMeta() {
    try {
      if (!loadMetaNextSeqStmt) return 1;
      var row = loadMetaNextSeqStmt.get();
      var value = row ? row.meta_value : '';
      return normalizeStoreMeta({ next_seq: value }).next_seq;
    } catch (_) {
      return 1;
    }
  }

  function persistNextSeqToDbMeta(nowMs) {
    if (!upsertMetaNextSeqStmt) return;
    nextSeq = normalizeStoreMeta({ next_seq: nextSeq }).next_seq;
    upsertMetaNextSeqStmt.run({
      meta_value: String(nextSeq),
      updated_at_ms: toNonNegativeInt(nowMs, Date.now()),
    });
  }

  function ensureDbDir() {
    var dir = dirname(dbPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  function ensureDb() {
    if (db) return;

    var Database = loadBetterSqlite3();
    ensureDbDir();
    db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('busy_timeout = 5000');
    db.pragma('foreign_keys = ON');

    db.exec(`
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
    `);

    insertStmt = db.prepare(`
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
    `);

    loadAllStmt = db.prepare(`
      SELECT
        discord_user_id, seq_id, username, global_name, avatar, roles_json,
        api_key_id, api_key_hash, status, created_at_ms, last_login_at_ms,
        usage_requests_today, usage_tokens_today, usage_requests_total, usage_tokens_total,
        usage_day, risk_json, banned_reason, banned_at_ms, updated_at_ms
      FROM discord_users
      ORDER BY created_at_ms ASC, discord_user_id ASC
    `);

    loadByDiscordIdStmt = db.prepare(`
      SELECT
        discord_user_id, seq_id, username, global_name, avatar, roles_json,
        api_key_id, api_key_hash, status, created_at_ms, last_login_at_ms,
        usage_requests_today, usage_tokens_today, usage_requests_total, usage_tokens_total,
        usage_day, risk_json, banned_reason, banned_at_ms, updated_at_ms
      FROM discord_users
      WHERE discord_user_id = ?
      LIMIT 1
    `);

    loadBySeqIdStmt = db.prepare(`
      SELECT
        discord_user_id, seq_id, username, global_name, avatar, roles_json,
        api_key_id, api_key_hash, status, created_at_ms, last_login_at_ms,
        usage_requests_today, usage_tokens_today, usage_requests_total, usage_tokens_total,
        usage_day, risk_json, banned_reason, banned_at_ms, updated_at_ms
      FROM discord_users
      WHERE seq_id = ?
      LIMIT 1
    `);

    loadMetaNextSeqStmt = db.prepare(`
      SELECT meta_value
      FROM discord_user_store_meta
      WHERE meta_key = 'next_seq'
      LIMIT 1
    `);

    upsertMetaNextSeqStmt = db.prepare(`
      INSERT INTO discord_user_store_meta (meta_key, meta_value, updated_at_ms)
      VALUES ('next_seq', @meta_value, @updated_at_ms)
      ON CONFLICT(meta_key) DO UPDATE SET
        meta_value = excluded.meta_value,
        updated_at_ms = excluded.updated_at_ms
    `);
  }

  function loadUserFromDbByDiscordId(discordUserId) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) return null;
    try {
      ensureDb();
      if (!loadByDiscordIdStmt) return null;
      var row = loadByDiscordIdStmt.get(discordUserId);
      if (!row) return null;
      var user = fromDbUserRow(row);
      if (!user || !user.discord_user_id) return null;
      users.set(user.discord_user_id, user);
      if (ensureUserSeqId(user)) {
        scheduleSaveUsers();
      }
      return user;
    } catch (e) {
      log('⚠️', C.yellow, '[discord-user-store] 按用户 ID 读取 SQLite 失败: ' + e.message);
      return null;
    }
  }

  function loadUserFromDbBySeqId(seqId) {
    var normalizedSeqId = normalizeSeqId(seqId);
    if (!normalizedSeqId) return null;
    try {
      ensureDb();
      if (!loadBySeqIdStmt) return null;
      var row = loadBySeqIdStmt.get(normalizedSeqId);
      if (!row) return null;
      var user = fromDbUserRow(row);
      if (!user || !user.discord_user_id) return null;
      users.set(user.discord_user_id, user);
      if (ensureUserSeqId(user)) {
        scheduleSaveUsers();
      }
      return user;
    } catch (e) {
      log('⚠️', C.yellow, '[discord-user-store] 按 seq_id 读取 SQLite 失败: ' + e.message);
      return null;
    }
  }

  function toDbUserRow(user, nowMs) {
    var normalized = normalizeUserRecord(user);
    var usage = normalizeUsage(normalized.usage);
    var risk = normalizeRisk(normalized.risk);
    var createdAtMs = toTimestampMs(normalized.created_at, nowMs);
    var lastLoginAtMs = toTimestampMs(normalized.last_login_at, createdAtMs);
    var bannedAtMs = normalized.banned_at ? toTimestampMs(normalized.banned_at, 0) : 0;

    return {
      discord_user_id: String(normalized.discord_user_id || '').trim(),
      seq_id: normalizeSeqId(normalized.seq_id),
      username: String(normalized.username || ''),
      global_name: String(normalized.global_name || ''),
      avatar: String(normalized.avatar || ''),
      roles_json: safeJsonStringify(normalizeRoles(normalized.roles), '[]'),
      api_key_id: String(normalized.api_key_id || ''),
      api_key_hash: String(normalized.api_key_hash || ''),
      status: normalizeStatus(normalized.status),
      created_at_ms: toNonNegativeInt(createdAtMs, nowMs),
      last_login_at_ms: toNonNegativeInt(lastLoginAtMs, nowMs),
      usage_requests_today: toNonNegativeInt(usage.requests_today, 0),
      usage_tokens_today: toNonNegativeInt(usage.tokens_today, 0),
      usage_requests_total: toNonNegativeInt(usage.requests_total, 0),
      usage_tokens_total: toNonNegativeInt(usage.tokens_total, 0),
      usage_day: String(usage.day || todayKey()),
      risk_json: safeJsonStringify(risk, '{}'),
      banned_reason: String(normalized.banned_reason || ''),
      banned_at_ms: toNonNegativeInt(bannedAtMs, 0),
      updated_at_ms: toNonNegativeInt(nowMs, Date.now()),
    };
  }

  function fromDbUserRow(row) {
    var createdAt = toIsoString(row.created_at_ms, '');
    if (!createdAt) createdAt = toIsoString(row.updated_at_ms, '');
    if (!createdAt) createdAt = '1970-01-01T00:00:00.000Z';
    var lastLoginAt = toIsoString(row.last_login_at_ms, createdAt);
    return normalizeUserRecord({
      discord_user_id: row.discord_user_id || '',
      seq_id: row.seq_id || '',
      username: row.username || '',
      global_name: row.global_name || '',
      avatar: row.avatar || '',
      roles: safeJsonParseArray(row.roles_json, []),
      api_key_id: row.api_key_id || '',
      api_key_hash: row.api_key_hash || '',
      status: row.status || 'active',
      created_at: createdAt,
      last_login_at: lastLoginAt,
      usage: {
        requests_today: toNonNegativeInt(row.usage_requests_today, 0),
        tokens_today: toNonNegativeInt(row.usage_tokens_today, 0),
        requests_total: toNonNegativeInt(row.usage_requests_total, 0),
        tokens_total: toNonNegativeInt(row.usage_tokens_total, 0),
        day: String(row.usage_day || todayKey()),
      },
      risk: safeJsonParseObject(row.risk_json, {}),
      banned_reason: row.banned_reason || '',
      banned_at: toNonNegativeInt(row.banned_at_ms, 0) > 0 ? toIsoString(row.banned_at_ms, '') : '',
    });
  }

  function flushSaveUsers(options) {
    var opts = options && typeof options === 'object' ? options : {};
    var throwOnError = opts.throw_on_error === true || opts.throwOnError === true;
    if (!savePending || saveInFlight) return false;
    savePending = false;
    saveInFlight = true;
    var saveError = null;

    try {
      ensureDb();
      migrateSeqIdsOnLoad();
      syncNextSeqFromUsers();
      var nowMs = Date.now();
      var rows = [];

      for (var entry of users.entries()) {
        var userId = String(entry[0] || '').trim();
        var user = entry[1];
        if (!userId || !user) continue;
        if (!ensureUserSeqId(user)) {
          user.seq_id = normalizeSeqId(user.seq_id);
        }
        rows.push(toDbUserRow(user, nowMs));
      }

      db.transaction(function (payloadRows) {
        for (var j = 0; j < payloadRows.length; j++) {
          insertStmt.run(payloadRows[j]);
        }

        persistNextSeqToDbMeta(nowMs);
      })(rows);

      log('💾', C.green, '[discord-user-store] 已保存用户数据到 SQLite: path=' + dbPath + ', count=' + rows.length);
    } catch (e) {
      saveError = e;
      savePending = true;
      log('⚠️', C.yellow, '[discord-user-store] 保存到 SQLite 失败: ' + e.message);
    } finally {
      saveInFlight = false;
      if (savePending) {
        scheduleSaveUsers();
      }
    }

    if (saveError && throwOnError) {
      throw saveError;
    }
    return !saveError;
  }

  function scheduleSaveUsers() {
    savePending = true;
    if (saveTimer) return;
    saveTimer = setTimeout(function () {
      saveTimer = null;
      flushSaveUsers();
    }, SAVE_DEBOUNCE_MS);
    if (saveTimer.unref) saveTimer.unref();
  }

  function loadUsers() {
    try {
      ensureDb();
      var rows = loadAllStmt.all();
      var metaNextSeq = loadNextSeqFromDbMeta();
      var loadedMap = new Map();
      for (var i = 0; i < rows.length; i++) {
        var user = fromDbUserRow(rows[i]);
        if (!user.discord_user_id) continue;
        loadedMap.set(user.discord_user_id, user);
      }
      users = loadedMap;
      nextSeq = metaNextSeq;

      var seqChanged = migrateSeqIdsOnLoad();
      var seqMetaDrift = nextSeq !== metaNextSeq;
      if (!seqChanged) {
        var beforeSync = nextSeq;
        syncNextSeqFromUsers();
        if (nextSeq !== beforeSync) seqMetaDrift = true;
      }

      var shouldPersist = seqChanged || seqMetaDrift;
      if (shouldPersist) {
        savePending = true;
        flushSaveUsers();
      }

      if (users.size === 0) {
        users = new Map();
        log('ℹ️', C.blue, '[discord-user-store] SQLite 用户表为空，使用空存储: ' + dbPath);
        return users;
      }

      log('📦', C.green, '[discord-user-store] 已从 SQLite 加载用户数据: count=' + users.size);
      if (seqChanged) {
        log('🧱', C.blue, '[discord-user-store] 已完成 seq_id 排序迁移并写入: next_seq=' + nextSeq + ', count=' + users.size);
      }
      return users;
    } catch (e) {
      log('⚠️', C.yellow, '[discord-user-store] 加载失败，回退为空存储: ' + e.message);
      users = new Map();
      nextSeq = 1;
      return users;
    }
  }

  async function saveUsers() {
    savePending = true;
    if (saveTimer) {
      clearTimeout(saveTimer);
      saveTimer = null;
    }
    flushSaveUsers({ throwOnError: true });
  }

  function findByDiscordId(discordUserId) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) return null;
    var user = users.get(discordUserId);
    if (!user) {
      user = loadUserFromDbByDiscordId(discordUserId);
    }
    if (!user) return null;
    rollDailyUsageIfNeeded(user);
    if (normalizeExistingUserSeqId(user)) scheduleSaveUsers();
    return safeClone(user);
  }

  function findBySeqId(seqId) {
    var normalized = normalizeSeqId(seqId);
    if (!normalized) return null;
    var changed = false;
    for (var user of users.values()) {
      if (!user) continue;
      if (normalizeExistingUserSeqId(user)) changed = true;
      if (normalizeSeqId(user.seq_id) !== normalized) continue;
      rollDailyUsageIfNeeded(user);
      if (changed) scheduleSaveUsers();
      return safeClone(user);
    }
    var loaded = loadUserFromDbBySeqId(normalized);
    if (loaded) {
      if (changed) scheduleSaveUsers();
      return safeClone(loaded);
    }
    if (changed) scheduleSaveUsers();
    return null;
  }

  function createOrUpdateUser(input) {
    input = input || {};
    var discordUserId = String(input.discord_user_id || '').trim();
    if (!discordUserId) {
      throw new Error('createOrUpdateUser 缺少 discord_user_id');
    }

    var inputCreatedAtMs = toTimestampMs(
      input.created_at_ms !== undefined ? input.created_at_ms : input.created_at,
      0
    );
    var inputLastLoginAtMs = toTimestampMs(
      input.last_login_at_ms !== undefined ? input.last_login_at_ms : input.last_login_at,
      0
    );
    var nowMs = Date.now();
    var now = nowIso();
    var createdAtIso = toIsoString(inputCreatedAtMs, now);
    var loginAtIso = toIsoString(inputLastLoginAtMs, now);
    var user = users.get(discordUserId);
    if (!user) {
      var assignedSeqId = allocateSeqId();
      user = normalizeUserRecord({
        discord_user_id: discordUserId,
        seq_id: assignedSeqId,
        status: 'active',
        created_at: createdAtIso,
        last_login_at: loginAtIso,
        usage: {
          requests_today: 0,
          tokens_today: 0,
          requests_total: 0,
          tokens_total: 0,
          day: todayKey(),
        },
      });
      log('👤', C.green, '[discord-user-store] 创建用户: discord_user_id=' + discordUserId + ', seq_id=' + assignedSeqId);
    }

    user.username = String(input.username || user.username || '');
    user.global_name = String(input.global_name || user.global_name || '');
    user.avatar = String(input.avatar || user.avatar || '');
    if (Array.isArray(input.roles)) {
      user.roles = normalizeRoles(input.roles);
    }
    if (toTimestampMs(user.created_at, 0) <= 0 && inputCreatedAtMs > 0) {
      user.created_at = createdAtIso;
    }
    if (inputLastLoginAtMs <= 0) {
      inputLastLoginAtMs = nowMs;
      loginAtIso = toIsoString(inputLastLoginAtMs, now);
    }
    user.last_login_at = loginAtIso;
    rollDailyUsageIfNeeded(user);
    user.risk = normalizeRisk(user.risk);
    ensureUserSeqId(user);

    users.set(discordUserId, user);
    scheduleSaveUsers();
    return safeClone(user);
  }

  function generateApiKey(discordUserId) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) {
      throw new Error('generateApiKey 缺少 discordUserId');
    }

    var user = users.get(discordUserId);
    if (!user) {
      user = loadUserFromDbByDiscordId(discordUserId);
    }
    if (!user) {
      throw new Error('用户不存在: ' + discordUserId);
    }

    var apiKeyPlain = apiKeyPrefix + crypto.randomBytes(32).toString('hex');
    user.api_key_id = crypto.randomBytes(8).toString('hex');
    user.api_key_hash = sha256Hex(apiKeyPlain);
    user.last_login_at = nowIso();
    rollDailyUsageIfNeeded(user);
    user.risk = normalizeRisk(user.risk);
    ensureUserSeqId(user);
    if (user.status === 'revoked') {
      user.status = 'active';
    }

    users.set(discordUserId, user);
    scheduleSaveUsers();

    log('🔑', C.green, '[discord-user-store] 生成 API Key: discord_user_id=' + discordUserId + ', key_id=' + user.api_key_id);
    return apiKeyPlain;
  }

  function rotateApiKey(discordUserId) {
    log('🔄', C.blue, '[discord-user-store] 轮换 API Key: discord_user_id=' + String(discordUserId || ''));
    return generateApiKey(discordUserId);
  }

  function revokeApiKey(discordUserId) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) return false;

    var user = users.get(discordUserId);
    if (!user) return false;

    user.api_key_id = '';
    user.api_key_hash = '';
    user.status = 'revoked';
    users.set(discordUserId, user);
    scheduleSaveUsers();

    log('🛑', C.yellow, '[discord-user-store] 吊销 API Key: discord_user_id=' + discordUserId);
    return true;
  }

  function verifyApiKey(keyPlaintext) {
    keyPlaintext = String(keyPlaintext || '').trim();
    if (!keyPlaintext) {
      log('🚫', C.yellow, '[discord-user-store] verifyApiKey: 空 key');
      return null;
    }
    if (apiKeyPrefix && keyPlaintext.indexOf(apiKeyPrefix) !== 0) {
      log('🚫', C.yellow, '[discord-user-store] verifyApiKey: 前缀不匹配');
      return null;
    }

    var targetHash = sha256Hex(keyPlaintext);
    for (var user of users.values()) {
      if (!user || user.status !== 'active') continue;
      if (!user.api_key_hash) continue;
      if (!safeHashEqual(user.api_key_hash, targetHash)) continue;

      user.last_login_at = nowIso();
      rollDailyUsageIfNeeded(user);
      user.risk = normalizeRisk(user.risk);
      normalizeExistingUserSeqId(user);
      scheduleSaveUsers();
      log('✅', C.green, '[discord-user-store] verifyApiKey 成功: discord_user_id=' + user.discord_user_id);
      return safeClone(user);
    }

    log('🚫', C.yellow, '[discord-user-store] verifyApiKey 失败: 未命中任何用户');
    return null;
  }

  function banUser(discordUserId, reason) {
    discordUserId = String(discordUserId || '').trim();
    reason = String(reason || '').trim();
    if (!discordUserId) {
      throw new Error('banUser 缺少 discordUserId');
    }

    var user = users.get(discordUserId);
    if (!user) {
      throw new Error('用户不存在: ' + discordUserId);
    }

    user.status = 'banned';
    user.banned_reason = reason || 'no_reason';
    user.banned_at = nowIso();
    user.risk = normalizeRisk(user.risk);
    user.risk.actions.applied = 'suspend';
    user.risk.last_auto_action_at = nowIso();
    users.set(discordUserId, user);
    scheduleSaveUsers();

    log('⛔', C.yellow, '[discord-user-store] 已封禁用户: discord_user_id=' + discordUserId + ', reason=' + user.banned_reason);
    return safeClone(user);
  }

  function unbanUser(discordUserId) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) {
      throw new Error('unbanUser 缺少 discordUserId');
    }

    var user = users.get(discordUserId);
    if (!user) {
      throw new Error('用户不存在: ' + discordUserId);
    }

    user.status = 'active';
    user.banned_reason = '';
    user.banned_at = '';
    user.risk = normalizeRisk(user.risk);
    user.risk.actions.applied = 'observe';
    users.set(discordUserId, user);
    scheduleSaveUsers();

    log('✅', C.green, '[discord-user-store] 已解封用户: discord_user_id=' + discordUserId);
    return safeClone(user);
  }

  function updateRisk(discordUserId, patch) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) {
      throw new Error('updateRisk 缺少 discordUserId');
    }
    var user = users.get(discordUserId);
    if (!user) {
      throw new Error('用户不存在: ' + discordUserId);
    }

    var risk = normalizeRisk(user.risk);
    var body = patch && typeof patch === 'object' ? patch : {};

    if (body.score !== undefined) {
      risk.score = Number(body.score) || 0;
      if (risk.score < 0) risk.score = 0;
    }
    if (body.level !== undefined) {
      risk.level = normalizeRiskLevel(body.level);
    }
    if (body.reasons !== undefined) {
      risk.reasons = Array.isArray(body.reasons) ? safeClone(body.reasons).slice(-100) : [];
    }
    if (body.flags !== undefined) {
      risk.flags = body.flags && typeof body.flags === 'object' && !Array.isArray(body.flags) ? safeClone(body.flags) : {};
    }
    if (body.actions !== undefined) {
      risk.actions = normalizeRiskActions(body.actions);
    }
    if (body.last_eval_at !== undefined) {
      risk.last_eval_at = String(body.last_eval_at || '');
    }
    if (body.last_auto_action_at !== undefined) {
      risk.last_auto_action_at = String(body.last_auto_action_at || '');
    }

    user.risk = risk;
    users.set(discordUserId, user);
    scheduleSaveUsers();
    return safeClone(risk);
  }

  function appendRiskReason(discordUserId, reason) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) {
      throw new Error('appendRiskReason 缺少 discordUserId');
    }
    var user = users.get(discordUserId);
    if (!user) {
      throw new Error('用户不存在: ' + discordUserId);
    }

    user.risk = normalizeRisk(user.risk);
    if (!Array.isArray(user.risk.reasons)) user.risk.reasons = [];
    if (reason && typeof reason === 'object') {
      user.risk.reasons.push(safeClone(reason));
      if (user.risk.reasons.length > 100) {
        user.risk.reasons = user.risk.reasons.slice(-100);
      }
    }
    user.risk.last_eval_at = nowIso();
    users.set(discordUserId, user);
    scheduleSaveUsers();
    return safeClone(user.risk);
  }

  function listUsers() {
    var out = [];
    var changed = false;
    for (var entry of users.values()) {
      if (!entry || typeof entry !== 'object') continue;
      rollDailyUsageIfNeeded(entry);
      if (normalizeExistingUserSeqId(entry)) changed = true;
      out.push(safeClone(normalizeUserRecord(entry)));
    }
    if (changed) scheduleSaveUsers();
    return out;
  }

  loadUsers();

  return {
    loadUsers: loadUsers,
    saveUsers: saveUsers,
    findByDiscordId: findByDiscordId,
    findBySeqId: findBySeqId,
    createOrUpdateUser: createOrUpdateUser,
    generateApiKey: generateApiKey,
    rotateApiKey: rotateApiKey,
    revokeApiKey: revokeApiKey,
    verifyApiKey: verifyApiKey,
    banUser: banUser,
    unbanUser: unbanUser,
    updateRisk: updateRisk,
    appendRiskReason: appendRiskReason,
    listUsers: listUsers,
  };
}
