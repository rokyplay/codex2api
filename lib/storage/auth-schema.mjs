function ensureDb(db) {
  if (!db || typeof db.exec !== 'function') {
    throw new Error('initAuthSchema(db) requires a better-sqlite3 database instance');
  }
}

export function initAuthSchema(db) {
  ensureDb(db);

  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      display_name TEXT NOT NULL DEFAULT '',
      avatar_url TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active',
      primary_email TEXT NOT NULL DEFAULT '',
      email_verified INTEGER NOT NULL DEFAULT 0,
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL,
      last_login_at_ms INTEGER NOT NULL DEFAULT 0
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_primary_email_unique
      ON users(lower(primary_email))
      WHERE primary_email <> '';

    CREATE TABLE IF NOT EXISTS user_auth_identities (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      provider TEXT NOT NULL,
      provider_user_id TEXT NOT NULL,
      provider_username TEXT NOT NULL DEFAULT '',
      provider_email TEXT NOT NULL DEFAULT '',
      profile_json TEXT NOT NULL DEFAULT '{}',
      verified_at_ms INTEGER NOT NULL DEFAULT 0,
      linked_at_ms INTEGER NOT NULL,
      last_used_at_ms INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_identity_provider_uid
      ON user_auth_identities(provider, provider_user_id);
    CREATE INDEX IF NOT EXISTS idx_auth_identity_user
      ON user_auth_identities(user_id, provider);

    CREATE TABLE IF NOT EXISTS auth_sessions (
      session_hash TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      expires_at_ms INTEGER NOT NULL,
      ip TEXT NOT NULL DEFAULT '',
      ua TEXT NOT NULL DEFAULT '',
      updated_at_ms INTEGER NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id);
    CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at_ms);

    CREATE TABLE IF NOT EXISTS email_login_tokens (
      id TEXT PRIMARY KEY,
      email TEXT NOT NULL,
      token_hash TEXT NOT NULL,
      intent TEXT NOT NULL,
      requested_by_user_id TEXT NOT NULL DEFAULT '',
      ip TEXT NOT NULL DEFAULT '',
      ua_hash TEXT NOT NULL DEFAULT '',
      expires_at_ms INTEGER NOT NULL,
      used_at_ms INTEGER NOT NULL DEFAULT 0,
      created_at_ms INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_email_login_token_hash ON email_login_tokens(token_hash);
    CREATE INDEX IF NOT EXISTS idx_email_login_email_time ON email_login_tokens(lower(email), created_at_ms DESC);

    CREATE TABLE IF NOT EXISTS oauth_states (
      state_hash TEXT PRIMARY KEY,
      provider TEXT NOT NULL,
      intent TEXT NOT NULL,
      user_id TEXT NOT NULL DEFAULT '',
      ip TEXT NOT NULL DEFAULT '',
      redirect_after TEXT NOT NULL DEFAULT '',
      expires_at_ms INTEGER NOT NULL,
      created_at_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_oauth_states_exp ON oauth_states(expires_at_ms);
  `);
}

export default {
  initAuthSchema,
};
