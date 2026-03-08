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
var CLEAN_INTERVAL_MS = 5 * 60 * 1000;

function safeClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function sha256Hex(text) {
  return crypto.createHash('sha256').update(String(text || ''), 'utf8').digest('hex');
}

function normalizePositiveInt(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function normalizeTimestampMs(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
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

export function createDiscordSessionStore(config) {
  var cfg = config || {};
  var dbPath = resolve(String(cfg.db_path || cfg.sqlite_path || DEFAULT_DB_PATH));
  var sessionTtlHours = normalizePositiveInt(cfg.session_ttl_hours, 48);
  var sessionTtlMs = sessionTtlHours * 60 * 60 * 1000;

  var db = null;
  var stmt = {};
  var cleanTimer = null;

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

    db.exec(`
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
    `);

    stmt.upsert = db.prepare(`
      INSERT INTO discord_sessions (
        session_hash, discord_user_id, created_at_ms, expires_at_ms, ip, ua, updated_at_ms
      ) VALUES (
        @session_hash, @discord_user_id, @created_at_ms, @expires_at_ms, @ip, @ua, @updated_at_ms
      )
      ON CONFLICT(session_hash) DO UPDATE SET
        discord_user_id = excluded.discord_user_id,
        created_at_ms = excluded.created_at_ms,
        expires_at_ms = excluded.expires_at_ms,
        ip = excluded.ip,
        ua = excluded.ua,
        updated_at_ms = excluded.updated_at_ms
    `);
    stmt.getByHash = db.prepare('SELECT * FROM discord_sessions WHERE session_hash = ? LIMIT 1');
    stmt.deleteByHash = db.prepare('DELETE FROM discord_sessions WHERE session_hash = ?');
    stmt.deleteExpired = db.prepare('DELETE FROM discord_sessions WHERE expires_at_ms > 0 AND expires_at_ms <= ?');
    stmt.countAll = db.prepare('SELECT COUNT(1) AS total FROM discord_sessions');
    stmt.listActive = db.prepare('SELECT * FROM discord_sessions WHERE expires_at_ms > ? ORDER BY created_at_ms DESC');
  }

  function resolveTtlMs(ttlSeconds) {
    var ttlSec = normalizePositiveInt(ttlSeconds, 0);
    if (ttlSec > 0) return ttlSec * 1000;
    return sessionTtlMs;
  }

  function normalizeSessionOutput(row) {
    if (!row || typeof row !== 'object') return null;
    return {
      discord_user_id: String(row.discord_user_id || ''),
      created_at: normalizeTimestampMs(row.created_at_ms, Date.now()),
      expires_at: normalizeTimestampMs(row.expires_at_ms, Date.now() + sessionTtlMs),
      ip: String(row.ip || ''),
      ua: String(row.ua || ''),
    };
  }

  function upsertSession(sessionHash, session) {
    ensureDb();
    var now = Date.now();
    stmt.upsert.run({
      session_hash: sessionHash,
      discord_user_id: session.discord_user_id,
      created_at_ms: session.created_at,
      expires_at_ms: session.expires_at,
      ip: session.ip,
      ua: session.ua,
      updated_at_ms: now,
    });
  }

  function resolveCreateInput(firstArg, secondArg, thirdArg, fourthArg) {
    var discordUserId = '';
    var ip = '';
    var ua = '';
    var ttlSeconds = 0;

    if (firstArg && typeof firstArg === 'object') {
      var obj = firstArg;
      discordUserId = String(obj.discord_user_id || (obj.user && obj.user.discord_user_id) || (obj.user && obj.user.id) || '').trim();
      ip = String(obj.ip || obj.client_ip || '');
      ua = String(obj.ua || obj.user_agent || obj.userAgent || '');
      ttlSeconds = normalizePositiveInt(secondArg, 0);
    } else {
      discordUserId = String(firstArg || '').trim();
      ip = String(secondArg || '');
      ua = String(thirdArg || '');
      ttlSeconds = normalizePositiveInt(fourthArg, 0);
    }

    return {
      discord_user_id: discordUserId,
      ip: ip,
      ua: ua,
      ttl_seconds: ttlSeconds,
    };
  }

  function createSession(firstArg, secondArg, thirdArg, fourthArg) {
    var input = resolveCreateInput(firstArg, secondArg, thirdArg, fourthArg);
    var discordUserId = input.discord_user_id;
    if (!discordUserId) {
      throw new Error('createSession 缺少 discordUserId');
    }

    var sessionId = crypto.randomBytes(32).toString('hex');
    var sessionHash = sha256Hex(sessionId);
    var now = Date.now();
    var session = {
      discord_user_id: discordUserId,
      created_at: now,
      expires_at: now + resolveTtlMs(input.ttl_seconds),
      ip: String(input.ip || ''),
      ua: String(input.ua || ''),
    };
    upsertSession(sessionHash, session);

    log('✅', C.green, '[discord-session-store] 创建会话: discord_user_id=' + discordUserId + ', session_hash=' + sessionHash.slice(0, 12));
    return sessionId;
  }

  function getSession(sessionId) {
    sessionId = String(sessionId || '').trim();
    if (!sessionId) return null;

    ensureDb();
    var sessionHash = sha256Hex(sessionId);
    var row = stmt.getByHash.get(sessionHash);
    var session = normalizeSessionOutput(row);

    if (!session) {
      log('🚫', C.yellow, '[discord-session-store] getSession 未命中: session_hash=' + sessionHash.slice(0, 12));
      return null;
    }

    if (Date.now() > session.expires_at) {
      stmt.deleteByHash.run(sessionHash);
      log('⌛', C.yellow, '[discord-session-store] getSession 命中但已过期: session_hash=' + sessionHash.slice(0, 12));
      return null;
    }

    return safeClone(session);
  }

  function destroySession(sessionId) {
    sessionId = String(sessionId || '').trim();
    if (!sessionId) return false;

    ensureDb();
    var sessionHash = sha256Hex(sessionId);
    var result = stmt.deleteByHash.run(sessionHash);
    var deleted = !!(result && result.changes > 0);
    if (deleted) {
      log('🧹', C.blue, '[discord-session-store] 销毁会话: session_hash=' + sessionHash.slice(0, 12));
    }
    return deleted;
  }

  function cleanExpired() {
    ensureDb();
    var now = Date.now();
    var result = stmt.deleteExpired.run(now);
    var removed = result && typeof result.changes === 'number' ? result.changes : 0;
    if (removed > 0) {
      log('🧼', C.blue, '[discord-session-store] 清理过期会话: removed=' + removed);
    }
    return removed;
  }

  function loadSessions() {
    ensureDb();
    cleanExpired();
    var now = Date.now();
    var rows = stmt.listActive.all(now);
    var next = new Map();
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var sessionHash = String((row && row.session_hash) || '').trim();
      if (!sessionHash) continue;
      var session = normalizeSessionOutput(row);
      if (!session || !session.discord_user_id) continue;
      if (session.expires_at <= now) continue;
      next.set(sessionHash, session);
    }
    log('📦', C.green, '[discord-session-store] 已从 SQLite 加载会话数据: count=' + next.size);
    return next;
  }

  async function saveSessions() {
    // SQLite 即时持久化，无需额外 flush
    return;
  }

  function startCleaner() {
    if (cleanTimer) return;
    cleanTimer = setInterval(function () {
      try {
        cleanExpired();
      } catch (e) {
        log('⚠️', C.yellow, '[discord-session-store] 定时清理失败: ' + e.message);
      }
    }, CLEAN_INTERVAL_MS);
    if (cleanTimer.unref) cleanTimer.unref();
  }

  function stopCleaner() {
    if (!cleanTimer) return;
    clearInterval(cleanTimer);
    cleanTimer = null;
  }

  ensureDb();
  cleanExpired();
  startCleaner();

  return {
    createSession: createSession,
    getSession: getSession,
    destroySession: destroySession,
    cleanExpired: cleanExpired,
    loadSessions: loadSessions,
    saveSessions: saveSessions,
    stopCleaner: stopCleaner,
  };
}
