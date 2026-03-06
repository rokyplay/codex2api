import crypto from 'node:crypto';
import { readFileSync, existsSync, mkdirSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { log, C } from '../utils.mjs';

var __filename = fileURLToPath(import.meta.url);
var __dirname = dirname(__filename);
var DEFAULT_SESSIONS_FILE = resolve(__dirname, '../../data/discord-sessions.json');
var SAVE_DEBOUNCE_MS = 500;
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

function normalizeSessionRecord(raw) {
  raw = raw && typeof raw === 'object' ? raw : {};
  var createdAt = Number(raw.created_at);
  if (!Number.isFinite(createdAt) || createdAt <= 0) {
    createdAt = Date.now();
  }
  return {
    discord_user_id: String(raw.discord_user_id || ''),
    created_at: createdAt,
    ip: String(raw.ip || ''),
    ua: String(raw.ua || ''),
  };
}

function normalizeStoreData(raw) {
  if (!raw || typeof raw !== 'object') return {};
  if (raw.sessions && typeof raw.sessions === 'object' && !Array.isArray(raw.sessions)) {
    return raw.sessions;
  }
  return raw;
}

export function createDiscordSessionStore(config) {
  var cfg = config || {};
  var sessionsFile = String(cfg.file_path || DEFAULT_SESSIONS_FILE);
  var sessionTtlHours = normalizePositiveInt(cfg.session_ttl_hours, 48);
  var sessionTtlMs = sessionTtlHours * 60 * 60 * 1000;

  /** @type {Map<string, any>} */
  var sessions = new Map();
  var saveTimer = null;
  var savePending = false;
  var saveInFlight = false;
  var cleanTimer = null;

  function ensureDataDir() {
    var dir = dirname(sessionsFile);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  function serializeSessions() {
    var out = {};
    for (var entry of sessions.entries()) {
      var sessionHash = entry[0];
      var session = entry[1];
      if (!sessionHash) continue;
      if (!session || !session.discord_user_id) continue;
      out[sessionHash] = normalizeSessionRecord(session);
    }
    return {
      sessions: out,
      updated_at: new Date().toISOString(),
      session_ttl_hours: sessionTtlHours,
    };
  }

  function flushSaveSessions() {
    if (!savePending || saveInFlight) return;
    savePending = false;
    saveInFlight = true;
    ensureDataDir();
    var payload = JSON.stringify(serializeSessions(), null, 2);
    writeFile(sessionsFile, payload, 'utf8')
      .then(function () {
        log('💾', C.green, '[discord-session-store] 已保存会话数据: path=' + sessionsFile + ', count=' + sessions.size);
      })
      .catch(function (e) {
        log('⚠️', C.yellow, '[discord-session-store] 保存失败: ' + e.message);
      })
      .finally(function () {
        saveInFlight = false;
        if (savePending) {
          scheduleSaveSessions();
        }
      });
  }

  function scheduleSaveSessions() {
    savePending = true;
    if (saveTimer) return;
    saveTimer = setTimeout(function () {
      saveTimer = null;
      flushSaveSessions();
    }, SAVE_DEBOUNCE_MS);
    if (saveTimer.unref) saveTimer.unref();
  }

  function loadSessions() {
    try {
      ensureDataDir();
      if (!existsSync(sessionsFile)) {
        sessions = new Map();
        log('ℹ️', C.blue, '[discord-session-store] 会话文件不存在，使用空存储: ' + sessionsFile);
        return sessions;
      }
      var text = readFileSync(sessionsFile, 'utf8');
      if (!text || !text.trim()) {
        sessions = new Map();
        log('ℹ️', C.blue, '[discord-session-store] 会话文件为空，使用空存储');
        return sessions;
      }
      var parsed = JSON.parse(text);
      var rawSessions = normalizeStoreData(parsed);
      var next = new Map();
      var hashes = Object.keys(rawSessions || {});
      for (var i = 0; i < hashes.length; i++) {
        var sessionHash = String(hashes[i] || '').trim();
        if (!sessionHash) continue;
        var session = normalizeSessionRecord(rawSessions[sessionHash]);
        if (!session.discord_user_id) continue;
        next.set(sessionHash, session);
      }
      sessions = next;
      log('📦', C.green, '[discord-session-store] 已加载会话数据: count=' + sessions.size);
      return sessions;
    } catch (e) {
      log('⚠️', C.yellow, '[discord-session-store] 加载失败，回退为空存储: ' + e.message);
      sessions = new Map();
      return sessions;
    }
  }

  async function saveSessions() {
    savePending = true;
    if (saveTimer) {
      clearTimeout(saveTimer);
      saveTimer = null;
    }
    flushSaveSessions();
  }

  function createSession(discordUserId, ip, ua) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) {
      throw new Error('createSession 缺少 discordUserId');
    }

    var sessionId = crypto.randomBytes(32).toString('hex');
    var sessionHash = sha256Hex(sessionId);
    sessions.set(sessionHash, {
      discord_user_id: discordUserId,
      created_at: Date.now(),
      ip: String(ip || ''),
      ua: String(ua || ''),
    });
    scheduleSaveSessions();

    log('✅', C.green, '[discord-session-store] 创建会话: discord_user_id=' + discordUserId + ', session_hash=' + sessionHash.slice(0, 12));
    return sessionId;
  }

  function getSession(sessionId) {
    sessionId = String(sessionId || '').trim();
    if (!sessionId) return null;

    var sessionHash = sha256Hex(sessionId);
    var session = sessions.get(sessionHash);
    if (!session) {
      log('🚫', C.yellow, '[discord-session-store] getSession 未命中: session_hash=' + sessionHash.slice(0, 12));
      return null;
    }

    if (Date.now() - session.created_at > sessionTtlMs) {
      sessions.delete(sessionHash);
      scheduleSaveSessions();
      log('⌛', C.yellow, '[discord-session-store] getSession 命中但已过期: session_hash=' + sessionHash.slice(0, 12));
      return null;
    }

    return safeClone(session);
  }

  function destroySession(sessionId) {
    sessionId = String(sessionId || '').trim();
    if (!sessionId) return false;

    var sessionHash = sha256Hex(sessionId);
    var deleted = sessions.delete(sessionHash);
    if (deleted) {
      scheduleSaveSessions();
      log('🧹', C.blue, '[discord-session-store] 销毁会话: session_hash=' + sessionHash.slice(0, 12));
    }
    return deleted;
  }

  function cleanExpired() {
    var now = Date.now();
    var removed = 0;
    for (var entry of sessions.entries()) {
      var sessionHash = entry[0];
      var session = entry[1];
      if (!session || !session.created_at) {
        sessions.delete(sessionHash);
        removed += 1;
        continue;
      }
      if (now - session.created_at > sessionTtlMs) {
        sessions.delete(sessionHash);
        removed += 1;
      }
    }

    if (removed > 0) {
      scheduleSaveSessions();
      log('🧼', C.blue, '[discord-session-store] 清理过期会话: removed=' + removed + ', remaining=' + sessions.size);
    }
    return removed;
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

  loadSessions();
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
