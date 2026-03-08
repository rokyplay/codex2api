import crypto from 'node:crypto';
import { initAuthSchema } from './auth-schema.mjs';

function ensureDb(db) {
  if (!db || typeof db.prepare !== 'function' || typeof db.transaction !== 'function') {
    throw new Error('migrateDiscordUsers(db) requires a better-sqlite3 database instance');
  }
}

function toPositiveInt(value, fallback) {
  var n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function normalizeText(value) {
  return String(value || '').trim();
}

function normalizeStatus(status) {
  var s = String(status || '').trim().toLowerCase();
  if (s === 'banned' || s === 'revoked' || s === 'suspended') return 'suspended';
  if (s === 'deleted') return 'deleted';
  return 'active';
}

function buildDiscordAvatarUrl(discordUserId, avatar) {
  var userId = normalizeText(discordUserId);
  var avatarHash = normalizeText(avatar);
  if (!avatarHash) return '';
  if (/^https?:\/\//i.test(avatarHash)) return avatarHash;
  if (!userId) return avatarHash;
  return 'https://cdn.discordapp.com/avatars/' + userId + '/' + avatarHash + '.png?size=128';
}

function makeUserRecord(row, forcedUserId) {
  var discordUserId = normalizeText(row && row.discord_user_id);
  var now = Date.now();
  var createdAtMs = toPositiveInt(row && row.created_at_ms, now);
  var lastLoginAtMs = toPositiveInt(row && row.last_login_at_ms, createdAtMs);
  var displayName = normalizeText(row && row.global_name) || normalizeText(row && row.username) || ('discord:' + discordUserId);
  return {
    id: normalizeText(forcedUserId) || crypto.randomUUID(),
    display_name: displayName,
    avatar_url: buildDiscordAvatarUrl(discordUserId, row && row.avatar),
    status: normalizeStatus(row && row.status),
    primary_email: '',
    email_verified: 0,
    created_at_ms: createdAtMs,
    updated_at_ms: now,
    last_login_at_ms: lastLoginAtMs,
  };
}

function makeIdentityRecord(row, userId) {
  var now = Date.now();
  var discordUserId = normalizeText(row && row.discord_user_id);
  var providerUsername = normalizeText(row && row.global_name) || normalizeText(row && row.username);
  var linkedAtMs = toPositiveInt(row && row.created_at_ms, now);
  var lastUsedAtMs = toPositiveInt(row && row.last_login_at_ms, linkedAtMs);
  return {
    id: crypto.randomUUID(),
    user_id: normalizeText(userId),
    provider: 'discord',
    provider_user_id: discordUserId,
    provider_username: providerUsername,
    provider_email: '',
    profile_json: JSON.stringify({
      discord_user_id: discordUserId,
      seq_id: normalizeText(row && row.seq_id),
      username: normalizeText(row && row.username),
      global_name: normalizeText(row && row.global_name),
      avatar: normalizeText(row && row.avatar),
      status: normalizeText(row && row.status),
      roles_json: normalizeText(row && row.roles_json),
    }),
    verified_at_ms: linkedAtMs,
    linked_at_ms: linkedAtMs,
    last_used_at_ms: lastUsedAtMs,
  };
}

function hasTable(db, tableName) {
  var row = db.prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1"
  ).get(String(tableName || '').trim());
  return !!(row && row.name);
}

export function migrateDiscordUsers(db) {
  ensureDb(db);
  initAuthSchema(db);

  var summary = {
    scanned: 0,
    skipped_invalid: 0,
    created_users: 0,
    created_identities: 0,
    existing_identities: 0,
    repaired_missing_users: 0,
    missing_source_table: false,
  };

  if (!hasTable(db, 'discord_users')) {
    summary.missing_source_table = true;
    return summary;
  }

  var sourceRows = db.prepare(
    'SELECT * FROM discord_users ORDER BY created_at_ms ASC, discord_user_id ASC'
  ).all();

  var stmt = {
    findIdentity: db.prepare(
      'SELECT id, user_id FROM user_auth_identities WHERE provider = ? AND provider_user_id = ? LIMIT 1'
    ),
    findUser: db.prepare('SELECT id FROM users WHERE id = ? LIMIT 1'),
    insertUser: db.prepare(`
      INSERT OR IGNORE INTO users (
        id, display_name, avatar_url, status, primary_email, email_verified,
        created_at_ms, updated_at_ms, last_login_at_ms
      ) VALUES (
        @id, @display_name, @avatar_url, @status, @primary_email, @email_verified,
        @created_at_ms, @updated_at_ms, @last_login_at_ms
      )
    `),
    insertIdentity: db.prepare(`
      INSERT OR IGNORE INTO user_auth_identities (
        id, user_id, provider, provider_user_id, provider_username, provider_email,
        profile_json, verified_at_ms, linked_at_ms, last_used_at_ms
      ) VALUES (
        @id, @user_id, @provider, @provider_user_id, @provider_username, @provider_email,
        @profile_json, @verified_at_ms, @linked_at_ms, @last_used_at_ms
      )
    `),
    updateIdentityByProviderUid: db.prepare(`
      UPDATE user_auth_identities SET
        user_id = @user_id,
        provider_username = @provider_username,
        provider_email = @provider_email,
        profile_json = @profile_json,
        verified_at_ms = CASE
          WHEN verified_at_ms > 0 THEN verified_at_ms
          ELSE @verified_at_ms
        END,
        linked_at_ms = CASE
          WHEN linked_at_ms > 0 THEN linked_at_ms
          ELSE @linked_at_ms
        END,
        last_used_at_ms = CASE
          WHEN last_used_at_ms > @last_used_at_ms THEN last_used_at_ms
          ELSE @last_used_at_ms
        END
      WHERE provider = @provider AND provider_user_id = @provider_user_id
    `),
  };

  var run = db.transaction(function migrateAll(rows) {
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i] || {};
      var discordUserId = normalizeText(row.discord_user_id);
      if (!discordUserId) {
        summary.skipped_invalid += 1;
        continue;
      }
      summary.scanned += 1;

      var existingIdentity = stmt.findIdentity.get('discord', discordUserId);
      if (existingIdentity) {
        summary.existing_identities += 1;
        var existingUserId = normalizeText(existingIdentity.user_id);
        if (!existingUserId) {
          existingUserId = crypto.randomUUID();
        }
        var userRow = stmt.findUser.get(existingUserId);
        if (!userRow) {
          var repairedUser = makeUserRecord(row, existingUserId);
          stmt.insertUser.run(repairedUser);
          summary.repaired_missing_users += 1;
        }
        var updatedIdentity = makeIdentityRecord(row, existingUserId);
        stmt.updateIdentityByProviderUid.run(updatedIdentity);
        continue;
      }

      var user = makeUserRecord(row, '');
      stmt.insertUser.run(user);
      summary.created_users += 1;

      var identity = makeIdentityRecord(row, user.id);
      stmt.insertIdentity.run(identity);
      summary.created_identities += 1;
    }
  });

  run(sourceRows);
  return summary;
}

export default {
  migrateDiscordUsers,
};
