import crypto from 'node:crypto';
import { readFileSync, existsSync, mkdirSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { log, C } from '../utils.mjs';

var __filename = fileURLToPath(import.meta.url);
var __dirname = dirname(__filename);
var DEFAULT_USERS_FILE = resolve(__dirname, '../../data/discord-users.json');
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
  var matched = raw.match(/^discord_(\d+)$/);
  if (!matched) return 0;
  var n = Number(matched[1]);
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

function normalizeStoreData(raw) {
  if (!raw || typeof raw !== 'object') {
    return {
      users: {},
      meta: normalizeStoreMeta({}),
    };
  }
  if (raw.users && typeof raw.users === 'object' && !Array.isArray(raw.users)) {
    return {
      users: raw.users,
      meta: normalizeStoreMeta(raw.meta),
    };
  }
  return {
    users: raw,
    meta: normalizeStoreMeta({}),
  };
}

export function createDiscordUserStore(config) {
  var cfg = config || {};
  var usersFile = String(cfg.file_path || DEFAULT_USERS_FILE);
  var apiKeyPrefix = String(cfg.api_key_prefix || 'dk-');

  /** @type {Map<string, any>} */
  var users = new Map();
  var nextSeq = 1;
  var saveTimer = null;
  var savePending = false;
  var saveInFlight = false;

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

  function ensureUserSeqId(user) {
    if (!user || typeof user !== 'object') return false;
    var current = parseSeqNumber(user.seq_id);
    if (current > 0) {
      user.seq_id = formatSeqId(current);
      if (current >= nextSeq) nextSeq = current + 1;
      return false;
    }
    user.seq_id = allocateSeqId();
    return true;
  }

  function migrateSeqIdsOnLoad() {
    if (users.size === 0) {
      if (nextSeq < 1) nextSeq = 1;
      return false;
    }

    var changed = false;
    var seen = new Set();
    var maxSeq = 0;
    var missing = [];

    for (var entry of users.entries()) {
      var user = entry[1];
      var seqNum = parseSeqNumber(user && user.seq_id);
      if (seqNum > 0 && !seen.has(seqNum)) {
        seen.add(seqNum);
        if (seqNum > maxSeq) maxSeq = seqNum;
        if (user.seq_id !== formatSeqId(seqNum)) {
          user.seq_id = formatSeqId(seqNum);
          changed = true;
        }
        continue;
      }
      if (user && user.seq_id) changed = true;
      if (user) user.seq_id = '';
      missing.push(user);
    }

    missing.sort(function (a, b) {
      var diff = parseCreatedAtTs(a && a.created_at) - parseCreatedAtTs(b && b.created_at);
      if (diff !== 0) return diff;
      return String((a && a.discord_user_id) || '').localeCompare(String((b && b.discord_user_id) || ''));
    });

    var seqCursor = Math.max(nextSeq, maxSeq + 1, 1);
    for (var i = 0; i < missing.length; i++) {
      while (seen.has(seqCursor)) seqCursor += 1;
      var target = missing[i];
      if (!target) continue;
      target.seq_id = formatSeqId(seqCursor);
      seen.add(seqCursor);
      if (seqCursor > maxSeq) maxSeq = seqCursor;
      seqCursor += 1;
      changed = true;
    }

    var targetNextSeq = Math.max(seqCursor, maxSeq + 1, 1);
    if (targetNextSeq !== nextSeq) {
      nextSeq = targetNextSeq;
      changed = true;
    }
    return changed;
  }

  function ensureDataDir() {
    var dir = dirname(usersFile);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  function serializeUsers() {
    var out = {};
    for (var entry of users.entries()) {
      var userId = entry[0];
      var user = entry[1];
      if (!userId) continue;
      out[userId] = normalizeUserRecord(user);
    }
    return {
      users: out,
      meta: {
        next_seq: Math.max(1, Math.floor(Number(nextSeq) || 1)),
      },
      updated_at: nowIso(),
    };
  }

  function flushSaveUsers() {
    if (!savePending || saveInFlight) return;
    savePending = false;
    saveInFlight = true;
    ensureDataDir();
    var payload = JSON.stringify(serializeUsers(), null, 2);
    writeFile(usersFile, payload, 'utf8')
      .then(function () {
        log('💾', C.green, '[discord-user-store] 已保存用户数据: path=' + usersFile + ', count=' + users.size);
      })
      .catch(function (e) {
        log('⚠️', C.yellow, '[discord-user-store] 保存失败: ' + e.message);
      })
      .finally(function () {
        saveInFlight = false;
        if (savePending) {
          scheduleSaveUsers();
        }
      });
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
      ensureDataDir();
      if (!existsSync(usersFile)) {
        users = new Map();
        nextSeq = 1;
        log('ℹ️', C.blue, '[discord-user-store] 用户文件不存在，使用空存储: ' + usersFile);
        return users;
      }
      var text = readFileSync(usersFile, 'utf8');
      if (!text || !text.trim()) {
        users = new Map();
        nextSeq = 1;
        log('ℹ️', C.blue, '[discord-user-store] 用户文件为空，使用空存储');
        return users;
      }
      var parsed = JSON.parse(text);
      var storeData = normalizeStoreData(parsed);
      var rawUsers = storeData.users;
      nextSeq = storeData.meta.next_seq;
      var next = new Map();
      var ids = Object.keys(rawUsers || {});
      for (var i = 0; i < ids.length; i++) {
        var userId = String(ids[i] || '').trim();
        if (!userId) continue;
        var user = normalizeUserRecord(rawUsers[userId]);
        if (!user.discord_user_id) {
          user.discord_user_id = userId;
        }
        next.set(user.discord_user_id, user);
      }
      users = next;
      var changed = migrateSeqIdsOnLoad();
      if (changed) {
        scheduleSaveUsers();
        log('🧱', C.blue, '[discord-user-store] 已完成 seq_id 迁移: next_seq=' + nextSeq + ', count=' + users.size);
      }
      log('📦', C.green, '[discord-user-store] 已加载用户数据: count=' + users.size);
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
    flushSaveUsers();
  }

  function findByDiscordId(discordUserId) {
    discordUserId = String(discordUserId || '').trim();
    if (!discordUserId) return null;
    var user = users.get(discordUserId);
    if (!user) return null;
    rollDailyUsageIfNeeded(user);
    if (ensureUserSeqId(user)) scheduleSaveUsers();
    return safeClone(user);
  }

  function findBySeqId(seqId) {
    var normalized = normalizeSeqId(seqId);
    if (!normalized) return null;
    var changed = false;
    for (var user of users.values()) {
      if (!user) continue;
      if (ensureUserSeqId(user)) changed = true;
      if (normalizeSeqId(user.seq_id) !== normalized) continue;
      rollDailyUsageIfNeeded(user);
      if (changed) scheduleSaveUsers();
      return safeClone(user);
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

    var now = nowIso();
    var user = users.get(discordUserId);
    if (!user) {
      var assignedSeqId = allocateSeqId();
      user = normalizeUserRecord({
        discord_user_id: discordUserId,
        seq_id: assignedSeqId,
        status: 'active',
        created_at: now,
        last_login_at: now,
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
    user.last_login_at = now;
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
      ensureUserSeqId(user);
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
      if (ensureUserSeqId(entry)) changed = true;
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
