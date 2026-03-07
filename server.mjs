/**
 * codex2api 反代服务入口
 *
 * 将免费 ChatGPT 账号的 Codex/Chat API 能力对外暴露为标准 API 格式。
 *
 * 启动流程:
 *   1. 加载 config-server.json
 *   2. 加载 i18n/zh.json
 *   3. 初始化 modelMapper
 *   4. AccountPool.loadAccounts()
 *   5. 设置调度器
 *   6. TokenRefresher.start()
 *   7. 注册路由（openai, codex, anthropic, gemini）
 *   8. 监听端口
 *   9. Ctrl+C 优雅退出
 *
 * 零依赖: 全用 Node.js 22 内置 http, fs, fetch
 */

import http from 'node:http';
import crypto from 'node:crypto';
import { readFileSync, existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { resolve, normalize, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { log, C } from './lib/utils.mjs';
import {
  readBody,
  extractBearerToken,
  derivePromptCacheSessionId,
  resolveRetryPolicy,
  isRetryableError,
  pickRetryAccount,
  isTimeoutError,
  isNetworkError,
  createTimeoutError,
} from './lib/http-utils.mjs';
import { AccountPool } from './lib/account-pool.mjs';
import { createScheduler } from './lib/scheduler.mjs';
import { TokenRefresher } from './lib/token-refresher.mjs';
import { AutoRelogin } from './lib/auto-relogin.mjs';
import PoolHealthMonitor from './lib/pool-health-monitor.mjs';
import * as modelMapper from './lib/converter/model-mapper.mjs';
import { createOpenAIRoutes } from './routes/openai.mjs';
import { createCodexRoutes } from './routes/codex.mjs';
import { createAnthropicRoutes } from './routes/anthropic.mjs';
import { createGeminiRoutes } from './routes/gemini.mjs';
import {
  createAdminRoutes,
  logCollector,
  initLogPersistence,
  handleCredentialsImportGpaApi,
  handleCredentialsExportGpaApi,
} from './routes/admin.mjs';
import { StatsCollector } from './lib/stats-collector.mjs';
import * as codexResponses from './lib/converter/openai-responses.mjs';
import { normalizeCollectedUsage } from './lib/converter/openai-responses.mjs';
import { parseSSEStream } from './lib/converter/stream/sse-parser.mjs';
import { authenticateApiKey, setDiscordApiKeyUserStore } from './lib/api-key-auth.mjs';
import { getRealClientIp } from './lib/ip-utils.mjs';
import { BehaviorAggregator } from './lib/abuse/behavior-aggregator.mjs';
import { RiskLogger } from './lib/abuse/risk-logger.mjs';
import { RuleEngine } from './lib/abuse/rule-engine.mjs';
import { RateLimiter } from './lib/rate-limiter.mjs';

// ============ 路径常量 ============

var __filename = fileURLToPath(import.meta.url);
var __dirname = resolve(__filename, '..');

var CONFIG_PATH = resolve(__dirname, 'config-server.json');
var I18N_PATH = resolve(__dirname, 'i18n/zh.json');

// ============ 加载配置 ============

var config = {};
var i18n = {};

try {
  config = JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
  log('✅', C.green, '配置已加载: ' + CONFIG_PATH);
} catch (e) {
  log('❌', C.red, '配置加载失败: ' + e.message);
  process.exit(1);
}

try {
  i18n = JSON.parse(readFileSync(I18N_PATH, 'utf8'));
} catch (e) {
  log('⚠️', C.yellow, 'i18n 加载失败，使用空字典: ' + e.message);
}

/**
 * i18n 模板替换
 * t('account.loaded', { count: 5, active: 3 }) → "已加载 5 个账号（3 可用）"
 */
function t(key, params) {
  var parts = key.split('.');
  var val = i18n;
  for (var i = 0; i < parts.length; i++) {
    val = val && val[parts[i]];
  }
  if (typeof val !== 'string') return key;
  if (params) {
    var keys = Object.keys(params);
    for (var j = 0; j < keys.length; j++) {
      val = val.split('{' + keys[j] + '}').join(String(params[keys[j]]));
    }
  }
  return val;
}

// ============ 启动时安全检查 ============

var adminPassword = (config.server && config.server.admin_password) || '';
var apiPassword = (config.server && config.server.password) || '';

if (adminPassword && adminPassword.length < 6) {
  log('⚠️', C.yellow, t('admin.weak_password'));
}

if (!apiPassword) {
  log('⚠️', C.yellow, t('admin.no_api_password'));
}

// ============ 初始化组件 ============

// 模型映射器
modelMapper.init(config.models);

// 账号池
var pool = new AccountPool(config, i18n.account);
var STATE_FILE = resolve(__dirname, 'data/accounts-state.json');
pool.setSavePath(STATE_FILE);

// 优先加载持久化状态文件（包含运行时状态），再加载 accounts.json（可能有新注册的账号）
if (existsSync(STATE_FILE)) {
  var stateResult = pool.loadAccounts(STATE_FILE);
  log('💾', C.cyan, '从状态文件加载: ' + stateResult.loaded + ' 个账号');
}
var loadResult = pool.loadAccounts(config.accounts_source);
if (loadResult.loaded > 0) {
  log('📋', C.cyan, '从 accounts.json 新增: ' + loadResult.loaded + ' 个账号');
}

// wasted 账号不再自动恢复 — 由 auto-relogin 确认封禁后标记，重启不复活

// 总账号数检查
var totalActive = pool.getActiveCount ? pool.getActiveCount() : loadResult.active;
log('📋', C.cyan, t('account.loaded', { count: pool.getTotalCount(), active: totalActive }));
if (pool.getTotalCount() === 0) {
  log('⚠️', C.yellow, t('server.no_accounts'));
}

// 调度器
var schedulerMode = (config.scheduler && config.scheduler.mode) || 'round_robin';
var scheduler = createScheduler(schedulerMode);
pool.setScheduler(scheduler);
log('🔄', C.cyan, t('scheduler.' + schedulerMode) || schedulerMode);

// 重登检测回调 — session 连续失效达阈值后触发
// 当前无浏览器环境做实际 auth0 重登，仅记录日志 + 标记状态
// 账号会停在 relogin_needed 状态，不再被 refreshAll 无限重试
// 有密码的账号可通过管理面板手动触发重登，或等待注册机 relogin API 上线
// 自动重登器
var autoRelogin = new AutoRelogin(pool, config, function (level, message, meta) {
  logCollector.add(level, message, meta || {});
});
function isAutoReloginEnabled() {
  return !!(config.credentials && config.credentials.auto_relogin === true);
}

pool.onReloginNeeded(function (email) {
  var account = pool.getFullAccount(email);
  var hasPassword = !!(account && account.password);
  var autoReloginEnabledNow = isAutoReloginEnabled();
  if (!autoReloginEnabledNow) {
    autoRelogin.stop();
  }
  var canAutoRelogin = hasPassword && autoReloginEnabledNow;
  var msg = 'Session 连续失效，账号需要重登: ' + email
    + (canAutoRelogin ? ' (已入队自动重登)' : (hasPassword ? ' (自动重登已关闭)' : ' (无密码，需手动处理)'));
  log('🔑', C.yellow, msg);
  logCollector.add('warn', msg, {
    email: email,
    type: 'relogin_needed',
    has_password: hasPassword,
    auto_relogin_enabled: autoReloginEnabledNow,
    session_invalidated_count: account ? account.session_invalidated_count : 0,
  });
  if (canAutoRelogin) {
    autoRelogin.enqueue(email);
  }
});

// Token 刷新器
var tokenRefresher = new TokenRefresher(pool, config.credentials, i18n.account, function (level, message, meta) {
  logCollector.add(level, message, meta || {});
});
var poolHealthMonitor = new PoolHealthMonitor(pool, config, logCollector);

// 统计收集器
var statsCollector = new StatsCollector({
  config: config,
  dataDir: resolve(__dirname, 'data'),
});

var ABUSE_CONFIG = (config && config.abuse_detection) || {};
var DISCORD_USERS_FILE = resolve(__dirname, 'data/discord-users.json');

function normalizeUserIdentity(value) {
  var identity = String(value || '').trim();
  if (!identity || identity === 'unknown') return '';
  return identity;
}

function appendConfiguredIdentity(set, value) {
  var identity = normalizeUserIdentity(value);
  if (!identity) return;
  if (identity.indexOf('discord:') === 0) return;
  set.add(identity);
}

function collectConfiguredNonDiscordIdentities() {
  var serverCfg = (config && config.server) || {};
  var identitySet = new Set();

  appendConfiguredIdentity(identitySet, serverCfg.default_identity);
  appendConfiguredIdentity(identitySet, serverCfg.admin_username);

  var arrayFields = [
    serverCfg.whitelist,
    serverCfg.whitelist_identities,
    serverCfg.admin_users,
    serverCfg.admin_identities,
  ];
  for (var i = 0; i < arrayFields.length; i++) {
    var identities = arrayFields[i];
    if (!Array.isArray(identities)) continue;
    for (var j = 0; j < identities.length; j++) {
      appendConfiguredIdentity(identitySet, identities[j]);
    }
  }

  var apiKeys = Array.isArray(serverCfg.api_keys) ? serverCfg.api_keys : [];
  for (var k = 0; k < apiKeys.length; k++) {
    var apiKey = apiKeys[k] || {};
    if (apiKey.enabled === false) continue;
    appendConfiguredIdentity(identitySet, apiKey.identity);
  }

  return Array.from(identitySet);
}

function getDiscordRegisteredUserCount() {
  if (!existsSync(DISCORD_USERS_FILE)) return 0;
  try {
    var raw = JSON.parse(readFileSync(DISCORD_USERS_FILE, 'utf8'));
    var users = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw.users : null;
    if (!users || typeof users !== 'object' || Array.isArray(users)) return 0;
    return Object.keys(users).length;
  } catch (_) {
    return 0;
  }
}

function getAbuseOverviewUserCount() {
  var registeredDiscordUsers = getDiscordRegisteredUserCount();
  var adminUsers = collectConfiguredNonDiscordIdentities();
  return registeredDiscordUsers + adminUsers.length;
}

var behaviorAggregator = new BehaviorAggregator({
  config: ABUSE_CONFIG,
});
var riskLogger = new RiskLogger({
  dataDir: resolve(__dirname, 'data'),
  config: ABUSE_CONFIG,
});
var ruleEngine = new RuleEngine({
  config: ABUSE_CONFIG,
  aggregator: behaviorAggregator,
  riskLogger: riskLogger,
  getUserCount: getAbuseOverviewUserCount,
});
var rateLimiter = new RateLimiter((config && config.rate_limits) || {});

// 日志持久化初始化
initLogPersistence(resolve(__dirname, 'data'));

var MODEL_DISCOVERY_DEFAULT_INTERVAL_MS = 60 * 60 * 1000; // 1h
var MODEL_DISCOVERY_DEFAULT_TIMEOUT_MS = 20000;
var MODEL_DISCOVERY_DEFAULT_CLIENT_VERSION = '0.99.0';
var modelDiscoveryTimer = null;

function toPositiveInt(value, fallback) {
  var n = parseInt(value, 10);
  if (!isFinite(n) || n <= 0) return fallback;
  return n;
}

function getModelDiscoveryConfig() {
  var models = (config && config.models) || {};
  if (models.discovery && typeof models.discovery === 'object') {
    return models.discovery;
  }
  return {};
}

function isModelDiscoveryEnabled() {
  var discovery = getModelDiscoveryConfig();
  return discovery.enabled !== false;
}

function getModelDiscoveryIntervalMs() {
  var discovery = getModelDiscoveryConfig();
  return toPositiveInt(discovery.interval_ms, MODEL_DISCOVERY_DEFAULT_INTERVAL_MS);
}

function buildModelDiscoveryTokenContext() {
  var account = pool.getAccount();
  if (!account || !account.accessToken) return null;
  return {
    accessToken: account.accessToken,
    sessionToken: account.sessionToken || '',
    cookies: account.cookies || {},
    email: account.email || '',
  };
}

async function refreshUpstreamModels(force) {
  if (!isModelDiscoveryEnabled()) {
    return {
      success: false,
      disabled: true,
      error: 'model_discovery_disabled',
    };
  }
  var discovery = getModelDiscoveryConfig();
  var result = await modelMapper.fetchUpstreamModels(buildModelDiscoveryTokenContext, {
    force: force === true,
    cacheTtlMs: toPositiveInt(discovery.cache_ttl_ms, MODEL_DISCOVERY_DEFAULT_INTERVAL_MS),
    timeoutMs: toPositiveInt(discovery.timeout_ms, MODEL_DISCOVERY_DEFAULT_TIMEOUT_MS),
    clientVersion: String(discovery.client_version || MODEL_DISCOVERY_DEFAULT_CLIENT_VERSION),
  });

  if (result && result.success) {
    var models = (result.models && Array.isArray(result.models)) ? result.models : [];
    log('🧩', C.green, '上游模型同步成功: ' + models.length + ' 个 [' + models.join(', ') + ']');
  } else if (result && !result.disabled) {
    var errText = (result && (result.error || (result.cache && result.cache.last_error))) || 'unknown';
    log('⚠️', C.yellow, '上游模型同步失败，回退本地配置: ' + errText);
  }
  return result;
}

// ============ 路由上下文 ============

var ctx = {
  pool: pool,
  config: config,
  i18n: i18n,
  t: t,
  configPath: CONFIG_PATH,
  autoRelogin: autoRelogin,
  stats: statsCollector,
  poolHealthMonitor: poolHealthMonitor,
  abuse: {
    aggregator: behaviorAggregator,
    riskLogger: riskLogger,
    ruleEngine: ruleEngine,
  },
  rateLimiter: rateLimiter,
  refreshUpstreamModels: function (force) {
    return refreshUpstreamModels(force);
  },
  getUpstreamModelsSnapshot: function () {
    if (typeof modelMapper.getUpstreamModelsSnapshot === 'function') {
      return modelMapper.getUpstreamModelsSnapshot();
    }
    return null;
  },
};

// 路由处理器
var openaiRoutes = createOpenAIRoutes(ctx);
var codexRoutes = createCodexRoutes(ctx);
var anthropicRoutes = createAnthropicRoutes(ctx);
var geminiRoutes = createGeminiRoutes(ctx);
var adminRoutes = createAdminRoutes(ctx);
var discordAuthRoutes = null;
var userApiRoutes = null;

function normalizeHostHeader(hostHeader) {
  var host = String(hostHeader || '').trim().toLowerCase();
  if (!host) return '';
  if (host.indexOf('[') === 0) {
    var closingIndex = host.indexOf(']');
    if (closingIndex > 0) {
      return host.slice(1, closingIndex);
    }
    return host;
  }
  var colonIndex = host.indexOf(':');
  if (colonIndex >= 0) {
    return host.slice(0, colonIndex);
  }
  return host;
}

var DISCORD_AUTH_ENABLED = !!(config.discord_auth && config.discord_auth.enabled === true);
var DISCORD_PUBLIC_DOMAIN = normalizeHostHeader(config.discord_auth && config.discord_auth.public_domain);

if (DISCORD_AUTH_ENABLED) {
  try {
    var discordAuthModule = await import('./routes/discord-auth.mjs');
    var userApiModule = await import('./routes/user-api.mjs');
    if (discordAuthModule && typeof discordAuthModule.createDiscordAuthRoutes === 'function') {
      discordAuthRoutes = await discordAuthModule.createDiscordAuthRoutes({
        config: config,
        stats: statsCollector,
      });
    }
    if (userApiModule && typeof userApiModule.createUserApiRoutes === 'function') {
      userApiRoutes = await userApiModule.createUserApiRoutes({
        config: config,
        stats: statsCollector,
        ruleEngine: ruleEngine,
        stores: {
          userStore: discordAuthRoutes && discordAuthRoutes.userStore ? discordAuthRoutes.userStore : null,
          sessionStore: discordAuthRoutes && discordAuthRoutes.sessionStore ? discordAuthRoutes.sessionStore : null,
        },
      });
    }

    if (discordAuthRoutes && discordAuthRoutes.userStore) {
      setDiscordApiKeyUserStore(discordAuthRoutes.userStore);
      if (ruleEngine && typeof ruleEngine.setUserStore === 'function') {
        ruleEngine.setUserStore(discordAuthRoutes.userStore);
      }
    } else if (ruleEngine && typeof ruleEngine.setUserStore === 'function') {
      ruleEngine.setUserStore(null);
    }
    log('✅', C.green, 'Discord 认证路由已启用 | public_domain=' + (DISCORD_PUBLIC_DOMAIN || '(empty)'));
  } catch (err) {
    DISCORD_AUTH_ENABLED = false;
    discordAuthRoutes = null;
    userApiRoutes = null;
    setDiscordApiKeyUserStore(null);
    if (ruleEngine && typeof ruleEngine.setUserStore === 'function') {
      ruleEngine.setUserStore(null);
    }
    log('⚠️', C.yellow, 'Discord 路由加载失败，已自动禁用: ' + err.message);
  }
} else {
  setDiscordApiKeyUserStore(null);
  if (ruleEngine && typeof ruleEngine.setUserStore === 'function') {
    ruleEngine.setUserStore(null);
  }
}

// ============ 静态文件服务 ============

var PUBLIC_DIR = resolve(__dirname, 'public');
var PUBLIC_USER_DIR = resolve(PUBLIC_DIR, 'user');
var MIME_TYPES = {
  html: 'text/html',
  css: 'text/css',
  js: 'application/javascript',
  mjs: 'application/javascript',
  json: 'application/json',
  txt: 'text/plain',
  svg: 'image/svg+xml',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
  mp3: 'audio/mpeg',
  m4a: 'audio/mp4',
  avif: 'image/avif',
  ico: 'image/x-icon',
  map: 'application/json',
  woff: 'font/woff',
  woff2: 'font/woff2',
};
var STATIC_CACHE_MAX_BYTES = 2 * 1024 * 1024;
var staticFileCache = new Map();

function getMimeTypeByPath(filePath) {
  var idx = String(filePath || '').lastIndexOf('.');
  if (idx < 0) return 'application/octet-stream';
  var ext = String(filePath).slice(idx + 1).toLowerCase();
  return MIME_TYPES[ext] || 'application/octet-stream';
}

function buildStaticContentTypeHeader(mime) {
  var baseMime = String(mime || '');
  var needsCharset = baseMime.indexOf('text/') === 0
    || baseMime.indexOf('application/json') === 0
    || baseMime.indexOf('application/javascript') === 0;
  return needsCharset ? (baseMime + '; charset=utf-8') : baseMime;
}

function resolveStaticPath(rootDir, filePath) {
  var decoded = decodeURIComponent(String(filePath || ''));
  var fullPath = resolve(rootDir, normalize(decoded));
  var rootPrefix = rootDir + sep;
  if (fullPath !== rootDir && !fullPath.startsWith(rootPrefix)) {
    return '';
  }
  return fullPath;
}

function staticFileExists(rootDir, filePath) {
  var fullPath = '';
  try {
    fullPath = resolveStaticPath(rootDir, filePath);
  } catch (_) {
    return false;
  }
  if (!fullPath) return false;
  return existsSync(fullPath);
}

async function serveStaticFromDir(res, rootDir, filePath, contentType) {
  var fullPath;
  try {
    fullPath = resolveStaticPath(rootDir, filePath);
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Bad request' }));
    return;
  }

  if (!fullPath) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Forbidden' }));
    return;
  }
  var mime = contentType || getMimeTypeByPath(filePath);
  var header = buildStaticContentTypeHeader(mime);
  var cached = staticFileCache.get(fullPath);
  if (cached && cached.header === header) {
    res.writeHead(200, { 'Content-Type': cached.header });
    res.end(cached.content);
    return;
  }

  var readStart = Date.now();
  try {
    var content = await readFile(fullPath);
    if (content.length <= STATIC_CACHE_MAX_BYTES) {
      staticFileCache.set(fullPath, {
        content: content,
        header: header,
      });
    }
    res.writeHead(200, { 'Content-Type': header });
    res.end(content);
    var readCost = Date.now() - readStart;
    if (readCost > 200) {
      log('⏱️', C.yellow, '静态文件读取偏慢: ' + String(filePath || '') + ' | ' + readCost + 'ms');
    }
  } catch (e) {
    if (e && (e.code === 'ENOENT' || e.code === 'ENOTDIR')) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
      return;
    }
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Internal server error' }));
  }
}

function serveStatic(res, filePath, contentType) {
  return serveStaticFromDir(res, PUBLIC_DIR, filePath, contentType);
}

async function serveFirstExistingUserHtml(res, candidates) {
  for (var i = 0; i < candidates.length; i++) {
    var candidate = String(candidates[i] || '');
    if (!candidate) continue;
    if (!staticFileExists(PUBLIC_USER_DIR, candidate)) continue;
    await serveStaticFromDir(res, PUBLIC_USER_DIR, candidate, 'text/html');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
}

function hasFileExtension(pathname) {
  var path = String(pathname || '');
  var slashIndex = path.lastIndexOf('/');
  var dotIndex = path.lastIndexOf('.');
  return dotIndex > slashIndex;
}

async function servePublicUserRoute(res, pathname) {
  var path = String(pathname || '');

  if (path === '/login' || path === '/login/') {
    await serveFirstExistingUserHtml(res, ['login.html', 'login/index.html']);
    return true;
  }

  if (path.startsWith('/login/')) {
    var loginFile = path.substring('/login/'.length);
    if (!loginFile || !hasFileExtension(loginFile)) {
      await serveFirstExistingUserHtml(res, ['login/index.html', 'login.html']);
      return true;
    }
    await serveStaticFromDir(res, PUBLIC_USER_DIR, 'login/' + loginFile);
    return true;
  }

  if (path === '/portal' || path === '/portal/' || path === '/portal/index.html') {
    await serveFirstExistingUserHtml(res, ['portal.html', 'portal/index.html']);
    return true;
  }

  if (path.startsWith('/portal/')) {
    var portalFile = path.substring('/portal/'.length);
    if (!portalFile || !hasFileExtension(portalFile)) {
      await serveFirstExistingUserHtml(res, ['portal.html', 'portal/index.html']);
      return true;
    }
    await serveStaticFromDir(res, PUBLIC_USER_DIR, 'portal/' + portalFile);
    return true;
  }

  if (path === '/error' || path === '/error/' || path === '/error/index.html') {
    await serveFirstExistingUserHtml(res, ['error.html', 'error/index.html']);
    return true;
  }

  if (path.startsWith('/error/')) {
    var errorFile = path.substring('/error/'.length);
    if (!errorFile || !hasFileExtension(errorFile)) {
      await serveFirstExistingUserHtml(res, ['error.html', 'error/index.html']);
      return true;
    }
    await serveStaticFromDir(res, PUBLIC_USER_DIR, 'error/' + errorFile);
    return true;
  }

  if (path === '/terms' || path === '/terms/' || path === '/terms/index.html') {
    await serveFirstExistingUserHtml(res, ['terms.html', 'terms/index.html']);
    return true;
  }

  if (path.startsWith('/terms/')) {
    var termsFile = path.substring('/terms/'.length);
    if (!termsFile || !hasFileExtension(termsFile)) {
      await serveFirstExistingUserHtml(res, ['terms.html', 'terms/index.html']);
      return true;
    }
    await serveStaticFromDir(res, PUBLIC_USER_DIR, 'terms/' + termsFile);
    return true;
  }

  if (path === '/user/terms' || path === '/user/terms/') {
    await serveFirstExistingUserHtml(res, ['terms.html', 'terms/index.html']);
    return true;
  }

  if (path === '/user/terms.html') {
    await serveFirstExistingUserHtml(res, ['terms.html']);
    return true;
  }

  return false;
}

function normalizePublicStaticPath(pathname) {
  var path = String(pathname || '');
  if (!path || path === '/' || !path.startsWith('/')) return '';
  if (!hasFileExtension(path)) return '';
  var relativePath = path.replace(/^\/+/, '');
  if (!relativePath) return '';
  if (relativePath.startsWith('admin/')) return '';
  return relativePath;
}

async function servePublicStaticRoute(res, pathname) {
  var staticPath = normalizePublicStaticPath(pathname);
  if (!staticPath) return false;
  if (!staticFileExists(PUBLIC_DIR, staticPath)) return false;
  await serveStatic(res, staticPath);
  return true;
}

async function primeStaticCache() {
  var candidates = [
    'index.html',
    'style.css',
    'js/main.js',
    'js/dashboard.js',
    'js/auth.js',
    'js/utils.js',
    'user/login.html',
    'user/portal.html',
    'user/error.html',
    'user/terms.html',
  ];
  var startAt = Date.now();
  var warmed = 0;

  await Promise.all(candidates.map(async function (candidate) {
    var fullPath = '';
    try {
      fullPath = resolveStaticPath(PUBLIC_DIR, candidate);
    } catch (_) {
      return;
    }
    if (!fullPath || staticFileCache.has(fullPath)) return;
    try {
      var content = await readFile(fullPath);
      if (content.length > STATIC_CACHE_MAX_BYTES) return;
      staticFileCache.set(fullPath, {
        content: content,
        header: buildStaticContentTypeHeader(getMimeTypeByPath(candidate)),
      });
      warmed++;
    } catch (_) {}
  }));

  var cost = Date.now() - startAt;
  log('⚡', C.cyan, '静态缓存预热完成: ' + warmed + ' 个文件 | ' + cost + 'ms');
}

await primeStaticCache();

// ============ 认证辅助 — 用于健康检查详细信息 ============

/**
 * 检查请求是否携带有效的管理认证
 */
function hasAdminAuth(req) {
  var password = config.server && config.server.admin_password;
  if (!password) return false;
  var token = extractBearerToken(req.headers['authorization'] || '');
  if (!token) return false;
  // 复用 admin 的 session 判断（简单检查 token 非空且在 sessions 中）
  // 此处直接引用 createAdminRoutes 内部不可访问，所以用 API 密码做备用
  var apiPw = config.server && config.server.password;
  if (apiPw && token === apiPw) return true;
  // 对于 session token 无法在这里验证（sessions 在 admin.mjs 内部），
  // 所以也接受 admin_password 本身作为认证（仅健康检查详情）
  if (password && token === password) return true;
  return false;
}

function maskAuthorizationHeader(authHeader) {
  if (!authHeader) return '';
  var s = String(authHeader);
  if (/^Bearer\s+/i.test(s)) {
    return 'Bearer ****';
  }
  return '****';
}
function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return '"[unserializable:' + (err && err.message ? err.message : 'unknown') + ']"';
  }
}
function isWebSearchRelevantSSEEvent(eventType, dataOrRaw) {
  var type = eventType || '';
  if (type.indexOf('response.web_search_call.') === 0) return true;
  if (type === 'response.output_item.added' || type === 'response.output_item.done') return true;
  if (type === 'response.function_call_arguments.delta' || type === 'response.function_call_arguments.done') return true;
  var raw = typeof dataOrRaw === 'string' ? dataOrRaw : safeStringify(dataOrRaw);
  if (raw.indexOf('"web_search"') !== -1 || raw.indexOf('"web_search_call"') !== -1 || raw.indexOf('"response.web_search_call.') !== -1) {
    return true;
  }
  return false;
}

function isPublicHostRequest(req) {
  if (!DISCORD_AUTH_ENABLED) return false;
  if (!DISCORD_PUBLIC_DOMAIN) return false;
  var host = normalizeHostHeader(req && req.headers && req.headers.host);
  return host === DISCORD_PUBLIC_DOMAIN;
}

function sha256Hex(text) {
  return crypto.createHash('sha256').update(String(text || ''), 'utf8').digest('hex');
}

function normalizeUserAgent(ua) {
  return String(ua || '').trim().toLowerCase().replace(/\s+/g, ' ').slice(0, 512);
}

function parseCookieMap(req) {
  var cookie = String((req && req.headers && req.headers.cookie) || '');
  if (!cookie) return {};
  var out = {};
  var parts = cookie.split(';');
  for (var i = 0; i < parts.length; i++) {
    var item = parts[i];
    var idx = item.indexOf('=');
    if (idx <= 0) continue;
    var key = item.slice(0, idx).trim();
    if (!key) continue;
    var value = item.slice(idx + 1).trim();
    out[key] = value;
  }
  return out;
}

function buildSessionHint(req) {
  var headers = (req && req.headers) || {};
  var direct = headers['x-session-id'] || headers['session_id'] || headers['x-codex-session-id'];
  if (direct) return String(direct).slice(0, 128);
  var cookies = parseCookieMap(req);
  if (cookies.session_id) {
    return String(cookies.session_id).slice(0, 128);
  }
  return '';
}

async function resolveCallerIdentity(req, path, isPublicHost) {
  if (req && req._apiKeyIdentity) return req._apiKeyIdentity;
  if (!isPublicHost) return 'unknown';
  if (!path || (!path.startsWith('/user/api/') && !path.startsWith('/auth/'))) return 'unknown';
  if (!discordAuthRoutes || typeof discordAuthRoutes.getSessionFromCookie !== 'function') return 'unknown';
  try {
    var session = await discordAuthRoutes.getSessionFromCookie(req);
    var discordUserId = session && session.discord_user_id ? String(session.discord_user_id).trim() : '';
    if (discordUserId) return 'discord:' + discordUserId;
  } catch (_) {
    // ignore session identity resolve failure
  }
  return 'unknown';
}

function isAbuseProtectedPath(path) {
  if (!path) return false;
  if (path === '/health') return false;
  if (path === '/admin' || path.startsWith('/admin/')) return false;
  if (path === '/api/credentials') return false;
  if (path.startsWith('/v1/')) return true;
  if (path.startsWith('/backend-api/')) return true;
  if (path.startsWith('/user/api/')) return true;
  if (path.startsWith('/auth/')) return true;
  return false;
}

function isRateLimitedPath(path) {
  if (!path) return false;
  if (path.startsWith('/admin/')) return false;
  if (path.startsWith('/v1/')) return true;
  if (path.startsWith('/backend-api/')) return true;
  if (path.startsWith('/v1beta/')) return true;
  if (path.startsWith('/user/api/')) return true;
  if (path.startsWith('/auth/')) return true;
  return false;
}

var serverInflightRequests = 0;

// ============ HTTP 服务器 ============

var server = http.createServer(async function (req, res) {
  var path = req.url.split('?')[0];
  var isPublicHost = isPublicHostRequest(req);
  req._isPublicHost = isPublicHost;
  var requestStart = Date.now();
  serverInflightRequests += 1;
  req._statsMeta = null; // 路由中填充: { route, model, account, usage, error_type, stream, caller_identity?, path?, ttfb_ms? }
  req._apiKeyIdentity = '';
  req._callerIdentity = 'unknown';

  var inflightReleased = false;
  function releaseInflight() {
    if (inflightReleased) return;
    inflightReleased = true;
    if (serverInflightRequests > 0) {
      serverInflightRequests -= 1;
    }
  }

  var ipInfo = getRealClientIp(req);
  var clientIP = ipInfo && ipInfo.ip ? ipInfo.ip : '';
  var uaHash = sha256Hex(normalizeUserAgent(req.headers['user-agent'] || ''));
  var sessionHint = buildSessionHint(req);
  req._clientMeta = {
    ip: clientIP,
    ip_source: (ipInfo && ipInfo.ip_source) || 'unknown',
    ip_chain: (ipInfo && ipInfo.ip_chain) || [],
    ua_hash: uaHash,
    session_hint: sessionHint,
    inflight_at_arrival: serverInflightRequests,
  };

  var authProbe = authenticateApiKey(req, config, { allowLocalBypass: true });
  if (authProbe.ok && authProbe.identity) {
    req._apiKeyIdentity = authProbe.identity;
  }
  var callerIdentity = await resolveCallerIdentity(req, path, isPublicHost);
  req._callerIdentity = callerIdentity;

  // 全局访问日志 — 记录每一个收到的请求，无一遗漏
  var reqHeaders = JSON.stringify({
    auth: maskAuthorizationHeader(req.headers['authorization'] || ''),
    ct: req.headers['content-type'] || '',
    accept: req.headers['accept'] || '',
    ua: (req.headers['user-agent'] || '').substring(0, 60),
  });
  log('📥', C.dim, req.method + ' ' + req.url + ' ← ' + clientIP + ' | caller=' + callerIdentity + ' | ' + reqHeaders);

  var shouldCheckRateLimit = !!(rateLimiter
    && typeof rateLimiter.check === 'function'
    && req.method !== 'OPTIONS'
    && isRateLimitedPath(path));
  var rateLimitIdentity = req._apiKeyIdentity || callerIdentity || 'unknown';
  if (shouldCheckRateLimit) {
    var rateDecision = rateLimiter.check(rateLimitIdentity);
    if (rateDecision && rateDecision.allowed === false) {
      var retryAfter = rateDecision.retry_after;
      if (!isFinite(retryAfter) || retryAfter <= 0) {
        retryAfter = 1;
      }
      req._statsMeta = {
        route: 'rate_limit',
        path: path,
        model: '',
        account: '',
        stream: false,
        usage: {
          input_tokens: 0,
          output_tokens: 0,
          cached_tokens: 0,
          reasoning_tokens: 0,
        },
        error_type: 'rate_limit_exceeded',
        caller_identity: rateLimitIdentity,
        status_override: 429,
      };
      res.setHeader('Retry-After', String(Math.max(1, Math.floor(retryAfter))));
      res.writeHead(429, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        error: {
          message: 'Rate limit exceeded. Please try again later.',
          type: 'rate_limit_error',
          param: null,
          code: 'rate_limit_exceeded',
        },
      }));
      return;
    }
    if (rateDecision && rateDecision.remaining && rateDecision.remaining.rpm !== null && rateDecision.remaining.rpm !== undefined) {
      res.setHeader('X-RateLimit-Remaining', String(rateDecision.remaining.rpm));
    }
    if (rateDecision && typeof rateDecision.reset_at === 'number' && isFinite(rateDecision.reset_at)) {
      res.setHeader('X-RateLimit-Reset', String(Math.ceil(rateDecision.reset_at / 1000)));
    }
  }

  var shouldObserveAbuse = !(config && config.abuse_detection && config.abuse_detection.enabled === false)
    && isAbuseProtectedPath(path)
    && req.method !== 'OPTIONS'
    && callerIdentity
    && callerIdentity !== 'unknown';
  var observedCallerIdentity = shouldObserveAbuse ? callerIdentity : '';
  if (observedCallerIdentity) {
    behaviorAggregator.observeRequestStart({
      caller_identity: observedCallerIdentity,
      ts: requestStart,
      ip: clientIP,
      ua_hash: uaHash,
    });
  }

  var abuseObservationClosed = false;
  function finalizeAbuseObservation(payload) {
    if (!observedCallerIdentity || abuseObservationClosed) return;
    abuseObservationClosed = true;
    var row = payload && typeof payload === 'object' ? payload : {};
    var evalTs = (typeof row.ts === 'number' && isFinite(row.ts)) ? row.ts : Date.now();
    var usage = row.usage && typeof row.usage === 'object' ? row.usage : {};
    var status = (typeof row.status === 'number' && isFinite(row.status)) ? row.status : 0;
    behaviorAggregator.observeRequestEnd({
      ts: evalTs,
      caller_identity: observedCallerIdentity,
      status: status,
      error_type: row.error_type || null,
      input_tokens: usage.input_tokens || 0,
      output_tokens: usage.output_tokens || 0,
      cached_tokens: usage.cached_tokens || 0,
      reasoning_tokens: usage.reasoning_tokens || 0,
    });
    ruleEngine.evaluate(observedCallerIdentity, {
      ts: evalTs,
      ip: row.ip || clientIP,
      ua_hash: row.ua_hash || uaHash,
      path: row.path || path,
      status: status,
    }, {
      emitEvents: true,
      syncUser: true,
    });
  }

  // 响应元数据
  var _responseContentType = '';

  // 统一记录首字节耗时（TTFB）
  var firstByteAt = 0;
  function markFirstByte() {
    if (firstByteAt) return;
    firstByteAt = Date.now();
    if (req._statsMeta) {
      req._statsMeta.ttfb_ms = firstByteAt - requestStart;
    }
  }

  var rawWrite = res.write;
  res.write = function () {
    markFirstByte();
    return rawWrite.apply(res, arguments);
  };

  var rawEnd = res.end;
  res.end = function () {
    markFirstByte();
    _responseContentType = res.getHeader('content-type') || '';
    return rawEnd.apply(res, arguments);
  };

  res.on('close', function () {
    releaseInflight();
    if (res.writableEnded) return;
    var closeMeta = req._statsMeta || {};
    var closeUsage = closeMeta.usage && typeof closeMeta.usage === 'object'
      ? closeMeta.usage
      : {};
    var closeStatus = (typeof closeMeta.status_override === 'number' && isFinite(closeMeta.status_override) && closeMeta.status_override > 0)
      ? closeMeta.status_override
      : (res.statusCode || 0);
    finalizeAbuseObservation({
      ts: Date.now(),
      status: closeStatus,
      error_type: closeMeta.error_type || 'connection_closed',
      usage: {
        input_tokens: closeUsage.input_tokens || 0,
        output_tokens: closeUsage.output_tokens || 0,
        cached_tokens: closeUsage.cached_tokens || 0,
        reasoning_tokens: closeUsage.reasoning_tokens || 0,
      },
      ip: clientIP,
      ua_hash: uaHash,
      path: closeMeta.path || path,
    });
  });

  // 请求完成时记录统计 + 响应元数据日志
  res.on('finish', function () {
    releaseInflight();
    // 全局响应日志 — 记录返回状态与元数据，不采集响应正文
    var respLatency = Date.now() - requestStart;
    var finalCallerIdentity = req._apiKeyIdentity || callerIdentity || 'unknown';
    var sc = res.statusCode;
    var scColor = sc >= 200 && sc < 300 ? C.green : sc >= 400 && sc < 500 ? C.yellow : C.red;
    log('📤', C.dim, req.method + ' ' + req.url + ' → ' + scColor + sc + C.reset + ' | caller=' + finalCallerIdentity + ' | ct=' + (_responseContentType || '-') + ' | ' + respLatency + 'ms');
    if (respLatency > 1000 && (path === '/admin' || path === '/admin/' || path.startsWith('/admin/'))) {
      var reqHost = normalizeHostHeader(req && req.headers && req.headers.host);
      log('⏱️', C.yellow, 'Admin 请求偏慢: ' + req.method + ' ' + path + ' | ' + respLatency + 'ms | host=' + reqHost + ' | public=' + (isPublicHost ? 'yes' : 'no'));
    }
    var meta = req._statsMeta || {};
    var recordedStatus = (typeof meta.status_override === 'number' && isFinite(meta.status_override) && meta.status_override > 0)
      ? meta.status_override
      : res.statusCode;
    var now = Date.now();
    var latency = now - requestStart;
    var ttfbMs = meta.ttfb_ms;
    if (typeof ttfbMs !== 'number' || !isFinite(ttfbMs) || ttfbMs < 0) {
      ttfbMs = latency;
    }
    var recordEntry = {
      ts: requestStart,
      route: meta.route || '',
      path: meta.path || path,
      model: meta.model || '',
      account: meta.account || '',
      status: recordedStatus,
      latency: latency,
      ttfb_ms: ttfbMs,
      input_tokens: (meta.usage && meta.usage.input_tokens) || 0,
      output_tokens: (meta.usage && meta.usage.output_tokens) || 0,
      cached_tokens: (meta.usage && meta.usage.cached_tokens) || 0,
      reasoning_tokens: (meta.usage && meta.usage.reasoning_tokens) || 0,
      error_type: meta.error_type || null,
      stream: !!meta.stream,
      caller_identity: meta.caller_identity || finalCallerIdentity || 'unknown',
      ip: (req._clientMeta && req._clientMeta.ip) || '',
      ua_hash: (req._clientMeta && req._clientMeta.ua_hash) || '',
      session_hint: (req._clientMeta && req._clientMeta.session_hint) || '',
    };

    if (req._statsMeta && rateLimiter && typeof rateLimiter.recordTokens === 'function') {
      rateLimiter.recordTokens(recordEntry.caller_identity, {
        input_tokens: recordEntry.input_tokens,
        output_tokens: recordEntry.output_tokens,
        cached_tokens: recordEntry.cached_tokens,
        reasoning_tokens: recordEntry.reasoning_tokens,
      });
    }

    finalizeAbuseObservation({
      ts: now,
      status: recordedStatus,
      error_type: recordEntry.error_type,
      usage: {
        input_tokens: recordEntry.input_tokens,
        output_tokens: recordEntry.output_tokens,
        cached_tokens: recordEntry.cached_tokens,
        reasoning_tokens: recordEntry.reasoning_tokens,
      },
      ip: recordEntry.ip,
      ua_hash: recordEntry.ua_hash,
      path: recordEntry.path,
    });

    if (!req._statsMeta) return; // 非 API 请求(静态文件/OPTIONS/admin)不记录
    statsCollector.record(recordEntry);
  });

  // ====== CORS 分区 ======
  // 管理 API 不设 CORS（仅同源访问）
  // API 路由保持 Access-Control-Allow-Origin: *
  var isAdminPath = !isPublicHost && (path === '/admin' || path.startsWith('/admin/'));

  if (!isAdminPath) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-api-key, anthropic-version');
  }

  if (req.method === 'OPTIONS') {
    if (!isAdminPath) {
      res.writeHead(204);
    } else {
      res.writeHead(204);
    }
    res.end();
    return;
  }

  // 记录请求日志
  logCollector.add('request', req.method + ' ' + req.url, {
    ip: clientIP,
    ip_source: req._clientMeta && req._clientMeta.ip_source,
    ua_hash: req._clientMeta && req._clientMeta.ua_hash,
    session_hint: req._clientMeta && req._clientMeta.session_hint,
    userAgent: req.headers['user-agent'] || '',
    caller_identity: callerIdentity,
  });

  if (observedCallerIdentity) {
    var guardDecision = ruleEngine.enforceRequest(observedCallerIdentity, {
      ts: requestStart,
      ip: clientIP,
      ua_hash: uaHash,
      path: path,
      method: req.method,
    });
    if (!guardDecision.allowed) {
      req._statsMeta = {
        route: 'abuse_guard',
        path: path,
        model: '',
        account: '',
        stream: false,
        usage: {
          input_tokens: 0,
          output_tokens: 0,
          cached_tokens: 0,
          reasoning_tokens: 0,
        },
        error_type: guardDecision.reason || 'abuse_blocked',
        caller_identity: observedCallerIdentity,
        status_override: guardDecision.status,
      };
      if (guardDecision.retry_after) {
        res.setHeader('Retry-After', String(guardDecision.retry_after));
      }
      res.writeHead(guardDecision.status || 429, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        success: false,
        error: {
          code: guardDecision.reason || 'abuse_blocked',
          message: guardDecision.message || 'Request blocked by abuse policy.',
        },
        action: guardDecision.action,
        retry_after: guardDecision.retry_after || 0,
      }));
      return;
    }
  }

  // 根路径：公共域跳 /login，私有域保持跳管理面板
  if (path === '/') {
    if (isPublicHost) {
      res.writeHead(302, { 'Location': '/login' });
    } else {
      res.writeHead(302, { 'Location': '/admin/' });
    }
    res.end();
    return;
  }

  // 健康检查 — 默认脱敏，带认证返回详细
  if (path === '/health') {
    if (hasAdminAuth(req)) {
      var stats = pool.getStats();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        accounts: stats,
        scheduler: schedulerMode,
      }));
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
    }
    return;
  }

  // ====== 凭证上传 API（专用 token 认证）======
  if (path === '/api/credentials' && req.method === 'POST') {
    return await handleCredentialsAPI(req, res);
  }
  if (path === '/api/credentials/import/gpa' && req.method === 'POST') {
    return await handleCredentialsImportGpaApi(req, res, ctx);
  }
  if (path === '/api/credentials/export/gpa' && req.method === 'GET') {
    return await handleCredentialsExportGpaApi(req, res, ctx);
  }

  // ====== 公共域路由隔离 ======
  if (isPublicHost) {
    if (path === '/admin' || path === '/admin/' || path.startsWith('/admin/')) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
      return;
    }

    if (path.startsWith('/auth/')) {
      if (!DISCORD_AUTH_ENABLED || !discordAuthRoutes || typeof discordAuthRoutes.handleRequest !== 'function') {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
        return;
      }
      var authHandled = await discordAuthRoutes.handleRequest(req, res, path, req.method);
      if (authHandled) return;
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
      return;
    }

    if (path.startsWith('/user/api/')) {
      if (!DISCORD_AUTH_ENABLED || !userApiRoutes || typeof userApiRoutes.handleRequest !== 'function') {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
        return;
      }
      var userApiHandled = await userApiRoutes.handleRequest(req, res, path, req.method);
      if (userApiHandled) return;
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
      return;
    }

    if (path.startsWith('/user/assets/')) {
      var userAssetFile = path.substring('/user/'.length);
      if (!userAssetFile || !hasFileExtension(userAssetFile)) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
        return;
      }
      await serveStaticFromDir(res, PUBLIC_USER_DIR, userAssetFile);
      return;
    }

    if (path === '/login'
      || path === '/login/'
      || path.startsWith('/login/')
      || path === '/portal'
      || path === '/portal/'
      || path.startsWith('/portal/')
      || path === '/error'
      || path === '/error/'
      || path.startsWith('/error/')
      || path === '/terms'
      || path === '/terms/'
      || path.startsWith('/terms/')
      || path === '/user/terms'
      || path === '/user/terms/'
      || path === '/user/terms.html') {
      if (await servePublicUserRoute(res, path)) return;
    }

    if (await servePublicStaticRoute(res, path)) return;
  }

  // 兼容本地探测：允许非公共域访问 public 根静态资源（如 /style.css）
  if (await servePublicStaticRoute(res, path)) return;

  // ====== 管理面板 API ======
  if (path.startsWith('/admin/api/')) {
    return await adminRoutes(req, res);
  }

  // ====== 管理面板静态文件 ======
  if (path === '/admin' || path === '/admin/') {
    await serveStatic(res, 'index.html', 'text/html');
    return;
  }
  if (path.startsWith('/admin/') && !path.startsWith('/admin/api/')) {
    // /admin/style.css → style.css (相对于 public 目录)
    var staticFile = path.substring('/admin/'.length);
    var ext = staticFile.substring(staticFile.lastIndexOf('.') + 1);
    var mimeTypes = { html: 'text/html', css: 'text/css', js: 'application/javascript', json: 'application/json', png: 'image/png', svg: 'image/svg+xml', ico: 'image/x-icon' };
    var mime = mimeTypes[ext] || 'application/octet-stream';
    await serveStatic(res, staticFile, mime);
    return;
  }

  // 路由分发
  try {
    // OpenAI Chat Completions
    if (path.startsWith('/v1/chat/') || path === '/v1/models') {
      return await openaiRoutes(req, res);
    }

    // Codex CLI 透传
    if (path.startsWith('/backend-api/codex/')) {
      return await codexRoutes(req, res);
    }

    // Codex CLI 0.106+ 使用 /v1/responses (OpenAI Responses API)
    // 与 /backend-api/codex/ 同为 Responses API 格式，但需要模型解析
    if (path === '/v1/responses' || path === '/v1/responses/compact') {
      return await handleResponsesAPI(req, res);
    }

    // Anthropic Messages
    if (path === '/v1/messages') {
      return await anthropicRoutes(req, res);
    }

    // Gemini
    if (path.startsWith('/v1beta/')) {
      return await geminiRoutes(req, res);
    }

    // 404
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Not found: ' + path } }));
  } catch (err) {
    log('❌', C.red, '请求处理异常: ' + err.message);
    logCollector.add('error', '请求处理异常: ' + err.message, {
      path: path,
      method: req.method,
      stack: err.stack || '',
    });
    if (!res.writableEnded) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'Internal server error' } }));
    }
  }
});

// ============ /v1/responses 路由（Codex CLI 0.106+）============

/**
 * 处理 /v1/responses 请求
 *
 * Codex CLI 0.106+ 直接发 OpenAI Responses API 格式请求。
 * ChatGPT Backend (/backend-api/codex/responses) 也是同一种格式。
 * 所以只需: 认证 + 模型解析 + 透传 + 流式回传。
 */
async function handleResponsesAPI(req, res) {
  var rpath = req.url.split('?')[0];
  var requestIp = (req._clientMeta && req._clientMeta.ip) || getRealClientIp(req).ip || 'unknown';
  log('📨', C.cyan, '[responses] POST ' + rpath + ' from ' + requestIp);

  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    log('❌', C.red, '[responses] Invalid request body');
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Invalid request body' } }));
    return;
  }

  log('📋', C.dim, '[responses] req body: model=' + (body.model || '-') + ' stream=' + body.stream + ' input_type=' + (Array.isArray(body.input) ? 'array[' + body.input.length + ']' : typeof body.input));

  var auth = authenticateApiKey(req, config, { allowLocalBypass: true });
  if (!auth.ok) {
    log('🔒', C.yellow, '[responses] 认证失败 from ' + requestIp);
    res.writeHead(auth.status || 401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Unauthorized' } }));
    return;
  }
  req._apiKeyIdentity = auth.identity;

  if (body.model) {
    var modelResult = modelMapper.resolveModel(body.model);
    if (!modelResult.found && body.model) {
      log('⚠️', C.yellow, '[responses] 未知模型: ' + body.model + ' → 回退到: ' + modelResult.resolved);
    }
    body.model = modelResult.resolved;
    if (modelResult.reasoningEffort) {
      if (!body.reasoning || typeof body.reasoning !== 'object' || Array.isArray(body.reasoning)) {
        body.reasoning = {};
      }
      body.reasoning.effort = modelResult.reasoningEffort;
    }
  }

  var clientWantsStream = body.stream === true;
  req._statsMeta = {
    route: 'responses',
    model: body.model || '',
    account: '',
    stream: clientWantsStream,
    usage: null,
    error_type: null,
    caller_identity: req._apiKeyIdentity || auth.identity || 'unknown',
  };

  var upstreamUrl = (config.upstream && config.upstream.base_url || 'https://chatgpt.com/backend-api') + '/codex/responses';
  if (rpath.endsWith('/compact')) upstreamUrl += '/compact';
  var isCompact = rpath.endsWith('/compact');

  var clientSessionId = req.headers['session_id'] || req.headers['x-session-id'] || req.headers['x-codex-session-id'] || '';
  var sessionId = clientSessionId;
  if (!sessionId && config.prompt_cache && config.prompt_cache.enabled) {
    sessionId = derivePromptCacheSessionId({
      callerIdentity: (req._statsMeta && req._statsMeta.caller_identity) || 'unknown',
      route: 'responses',
      model: body.model || '',
      promptCacheKey: body.prompt_cache_key,
      user: body.user,
    });
  }

  if (!isCompact) {
    body.stream = true;
  }
  if (config.prompt_cache && config.prompt_cache.enabled) {
    if (!body.prompt_cache_key && sessionId) body.prompt_cache_key = sessionId;
    if (!body.prompt_cache_retention && config.prompt_cache.default_retention) {
      body.prompt_cache_retention = config.prompt_cache.default_retention;
    }
  }

  delete body.temperature;
  delete body.max_output_tokens;
  delete body.max_tokens;
  delete body.top_p;
  delete body.frequency_penalty;
  delete body.presence_penalty;
  delete body.stop;
  delete body.logit_bias;
  delete body.n;

  codexResponses.adaptResponsesBody(body, isCompact);
  var requestMeta = {
    model: body && body.model ? String(body.model) : '',
    stream: body && body.stream === true,
    field_count: body && typeof body === 'object' ? Object.keys(body).length : 0,
    tools_count: Array.isArray(body && body.tools) ? body.tools.length : 0,
    input_items: Array.isArray(body && body.input) ? body.input.length : (body && body.input ? 1 : 0),
  };
  log('🔎', C.cyan, '[WEB-SEARCH] [responses] upstream request meta=' + safeStringify(requestMeta));

  var retryPolicy = resolveRetryPolicy(config, clientWantsStream);
  var maxRetries = retryPolicy.max_retries;
  var attempt = 0;
  var triedAccounts = [];
  var nextRetryAccount = null;

  function acquireRetryAccount() {
    if (nextRetryAccount) {
      var preset = nextRetryAccount;
      nextRetryAccount = null;
      return preset;
    }
    if (attempt === 1) return pool.getAccount();
    return pickRetryAccount(pool, triedAccounts);
  }

  function planRetry(reason, failedAccount, extra) {
    if (!failedAccount || !failedAccount.email) return false;
    triedAccounts.push(failedAccount.email);
    nextRetryAccount = pickRetryAccount(pool, triedAccounts);
    if (!nextRetryAccount) {
      log('⚠️', C.yellow, '[responses] retry blocked: no alternate account | retry_attempt=' + (attempt + 1) + '/' + maxRetries + ' | reason=' + reason + ' | failed_account=' + failedAccount.email);
      return false;
    }
    log('🔄', C.yellow, '[responses] retry_attempt=' + (attempt + 1) + '/' + maxRetries + ' | reason=' + reason + ' | account=' + failedAccount.email + ' -> ' + nextRetryAccount.email + (extra ? ' | ' + extra : ''));
    return true;
  }

  function sendResponsesError(statusCode, message, code, streamStarted) {
    if (res.writableEnded) return;
    var payload = JSON.stringify({ error: { code: code, message: message } });
    if (streamStarted || res.headersSent) {
      if (!res.writableEnded) {
        try { res.write('event: error\ndata: ' + payload + '\n\n'); } catch (_) {}
        try { res.end(); } catch (_) {}
      }
      return;
    }
    try {
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(payload);
    } catch (e) {
      log('⚠️', C.yellow, '[responses] sendResponsesError writeHead failed: ' + e.message);
      if (!res.writableEnded) try { res.end(); } catch (_) {}
    }
  }

  async function readChunkWithTimeout(reader, timeoutMs, kind) {
    if (!timeoutMs || timeoutMs <= 0) return reader.read();
    var timer = null;
    var timeoutPromise = new Promise(function (_, reject) {
      timer = setTimeout(function () {
        reject(createTimeoutError(kind, timeoutMs, 'upstream_' + kind + '_after_' + timeoutMs + 'ms'));
      }, timeoutMs);
    });
    try {
      return await Promise.race([reader.read(), timeoutPromise]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  log('🔗', C.cyan, '[responses] → upstream ' + upstreamUrl + ' | model=' + (body.model || 'unknown'));

  while (attempt < maxRetries) {
    attempt++;
    var account = acquireRetryAccount();
    if (!account) break;
    req._statsMeta.account = account.email;
    var responseStarted = false;
    var upstreamHeaders = {
      'Authorization': 'Bearer ' + account.accessToken,
      'Content-Type': 'application/json',
      'Accept': 'text/event-stream',
      'originator': 'codex_cli_rs',
    };
    if (account.accountId) upstreamHeaders['chatgpt-account-id'] = account.accountId;
    if (sessionId) upstreamHeaders['session_id'] = sessionId;

    try {
      var fetchStart = Date.now();
      var upstreamResp = await fetch(upstreamUrl, {
        method: 'POST',
        headers: upstreamHeaders,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(retryPolicy.total_timeout_ms),
      });

      if (!upstreamResp.ok) {
        var errBody = await upstreamResp.text().catch(function () { return ''; });
        var errSnippet = errBody.length > 200 ? errBody.substring(0, 200) + '...' : errBody;
        log('❌', C.red, '[responses] ← ' + upstreamResp.status + ' ERROR | attempt=' + attempt + '/' + maxRetries + ' | account=' + account.email + ' | ' + errSnippet);
        var errResult = pool.markError(account.email, upstreamResp.status, errBody);
        req._statsMeta.error_type = 'upstream_' + upstreamResp.status;
        var retryableStatus = isRetryableError({
          status: upstreamResp.status,
          error_type: (errResult && errResult.type) || ('upstream_' + upstreamResp.status),
          message: errBody,
          account_issue: upstreamResp.status === 401 || upstreamResp.status === 403 || !!(errResult && (errResult.action === 'switch_account' || errResult.action === 'retry' || errResult.action === 'refresh_token' || errResult.action === 'relogin')),
        });
        if (retryableStatus && attempt < maxRetries && planRetry('upstream_' + upstreamResp.status, account, 'error_type=' + ((errResult && errResult.type) || 'unknown'))) {
          continue;
        }
        sendResponsesError(upstreamResp.status >= 500 ? 502 : upstreamResp.status, 'Upstream error (' + upstreamResp.status + ')', 'upstream_' + upstreamResp.status, false);
        return;
      }

      var successLatency = Date.now() - fetchStart;
      log('✅', C.green, '[responses] ← 200 OK | latency=' + successLatency + 'ms | account=' + account.email);
      req._statsMeta.error_type = null;

      var upstreamContentType = upstreamResp.headers.get('content-type') || '';
      var isExplicitJSON = upstreamContentType.indexOf('application/json') !== -1;
      var isUpstreamSSE = !isExplicitJSON;
      var usageData = null;

      if (!isUpstreamSSE) {
        var jsonBody = await upstreamResp.text();
        if (!jsonBody || !jsonBody.trim()) {
          pool.markError(account.email, 0, 'upstream_empty_response');
          req._statsMeta.error_type = 'empty_response';
          if (attempt < maxRetries && planRetry('empty_response', account, 'message=upstream_empty_response')) {
            continue;
          }
          sendResponsesError(502, 'Upstream returned empty response body', 'empty_response', false);
          return;
        }
        var parsedUsage = extractResponsesUsageFromBody(jsonBody);
        if (clientWantsStream) {
          res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
          responseStarted = true;
          res.write('data: ' + jsonBody.trim() + '\n\n');
          res.write('data: [DONE]\n\n');
          res.end();
        } else {
          res.writeHead(200, { 'Content-Type': upstreamContentType || 'application/json' });
          res.end(jsonBody);
        }
        if (parsedUsage) {
          req._statsMeta.usage = parsedUsage;
          pool.markSuccess(account.email, parsedUsage);
          log('📊', C.cyan, 'responses usage: in=' + parsedUsage.input_tokens + ' out=' + parsedUsage.output_tokens + ' [' + account.email + ']');
        } else {
          pool.markSuccess(account.email, {});
        }
        return;
      }

      if (!clientWantsStream) {
        var collected = await codexResponses.collectNonStreamResponseFromSSE(upstreamResp.body, {
          firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
          idleTimeoutMs: retryPolicy.idle_timeout_ms,
        });
        var collectFailed = !collected || !collected.success || !collected.response;
        var collectErr = collected && collected.error ? collected.error : 'invalid_sse_response';
        if (collectFailed) {
          pool.markError(account.email, 502, collectErr);
          req._statsMeta.error_type = 'upstream_sse_parse_failed';
          if (attempt < maxRetries && isRetryableError({
            status: 502,
            error_type: 'upstream_sse_parse_failed',
            message: collectErr,
            has_sent_data: false,
          }) && planRetry('upstream_sse_parse_failed', account, 'message=' + collectErr)) {
            continue;
          }
          sendResponsesError(502, 'Invalid upstream SSE response: ' + collectErr, 'upstream_sse_parse_failed', false);
          return;
        }
        if (collected.usage) {
          req._statsMeta.usage = collected.usage;
          pool.markSuccess(account.email, collected.usage);
          log('📊', C.cyan, 'responses usage: in=' + collected.usage.input_tokens + ' out=' + collected.usage.output_tokens + ' [' + account.email + ']');
        } else {
          pool.markSuccess(account.email, {});
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(collected.response));
        return;
      }

      if (!upstreamResp.body || typeof upstreamResp.body.getReader !== 'function') {
        pool.markError(account.email, 0, 'upstream_empty_stream');
        req._statsMeta.error_type = 'empty_response';
        if (attempt < maxRetries && planRetry('empty_response', account, 'message=upstream_empty_stream')) {
          continue;
        }
        sendResponsesError(502, 'Upstream returned empty stream', 'empty_response', false);
        return;
      }

      var parseState = codexResponses.createParseState();
      var sawClientChunk = false;
      var streamToClient = upstreamResp.body;
      var streamToParse = null;
      if (typeof upstreamResp.body.tee === 'function') {
        var tee = upstreamResp.body.tee();
        streamToClient = tee[0];
        streamToParse = tee[1];
      }

      var forwardPromise = (async function () {
        var reader = streamToClient.getReader();
        try {
          while (true) {
            var timeoutForRead = sawClientChunk ? retryPolicy.idle_timeout_ms : retryPolicy.first_byte_timeout_ms;
            var chunk = await readChunkWithTimeout(reader, timeoutForRead, sawClientChunk ? 'idle' : 'first_byte');
            if (chunk.done) break;
            if (!sawClientChunk) {
              if (res.headersSent) {
                log('⚠️', C.yellow, '[responses] writeHead skipped: headers already sent | account=' + account.email);
                break;
              }
              res.writeHead(200, {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
              });
              responseStarted = true;
              sawClientChunk = true;
            }
            if (!res.writableEnded) res.write(chunk.value);
          }
        } finally {
          reader.releaseLock();
        }
      })();

      var parsePromise = streamToParse
        ? parseSSEStream(streamToParse, function (eventType, data) {
          if (!data && eventType === 'done') return;
          if (isWebSearchRelevantSSEEvent(eventType, data)) {
            log('🔎', C.cyan, '[WEB-SEARCH] [responses] upstream parsed SSE event=' + eventType + ' data=' + safeStringify(data));
          }
          var universalEvent = codexResponses.parseSSEEvent(eventType, data, parseState);
          if (!universalEvent) return;
          var universalEvents = Array.isArray(universalEvent) ? universalEvent : [universalEvent];
          for (var ue = 0; ue < universalEvents.length; ue++) {
            var evt = universalEvents[ue];
            if (evt && evt.usage) usageData = evt.usage || null;
          }
        }, {
          firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
          idleTimeoutMs: retryPolicy.idle_timeout_ms,
        })
        : Promise.resolve();
      var parseRejected = false;
      var tasks = await Promise.allSettled([forwardPromise, parsePromise]);
      if (tasks[0].status === 'rejected') {
        var streamErr = tasks[0].reason;
        var timeoutStream = isTimeoutError(streamErr);
        var streamCode = timeoutStream ? 'upstream_timeout' : 'stream_interrupted';
        var streamMessage = (streamErr && streamErr.message) || streamCode;
        log('❌', C.red, '[responses] stream forward error | account=' + account.email + ' | ' + streamMessage);
        pool.markError(account.email, 0, streamMessage);
        req._statsMeta.error_type = streamCode;
        if (!responseStarted && attempt < maxRetries && isRetryableError({
          status: timeoutStream ? 504 : 502,
          error_type: streamCode,
          message: streamMessage,
          timeout: timeoutStream,
          network: isNetworkError(streamErr),
          error: streamErr,
          has_sent_data: false,
        }) && planRetry(streamCode, account, 'message=' + streamMessage)) {
          continue;
        }
        sendResponsesError(timeoutStream ? 504 : 502, 'Upstream stream interrupted. Please retry.', streamCode, responseStarted);
        return;
      }
      if (!sawClientChunk) {
        pool.markError(account.email, 0, 'upstream_empty_stream');
        req._statsMeta.error_type = 'empty_response';
        if (attempt < maxRetries && planRetry('empty_response', account, 'message=upstream_empty_stream')) {
          continue;
        }
        sendResponsesError(502, 'Upstream returned empty stream', 'empty_response', false);
        return;
      }
      if (tasks[1].status === 'rejected') {
        parseRejected = true;
        var parseErr = tasks[1].reason;
        var timeoutParse = isTimeoutError(parseErr);
        var parseCode = timeoutParse ? 'upstream_timeout' : 'stream_parse_error';
        var parseMessage = (parseErr && parseErr.message) || parseCode;
        log('⚠️', C.yellow, '[responses] usage parse failed | account=' + account.email + ' | ' + parseMessage);
        req._statsMeta.error_type = 'usage_parse_failed';
        if (!responseStarted && attempt < maxRetries && isRetryableError({
          status: timeoutParse ? 504 : 502,
          error_type: parseCode,
          message: parseMessage,
          timeout: timeoutParse,
          network: isNetworkError(parseErr),
          error: parseErr,
          has_sent_data: false,
        }) && planRetry(parseCode, account, 'message=' + parseMessage)) {
          continue;
        }
      }
      if (usageData) {
        req._statsMeta.usage = usageData;
        pool.markSuccess(account.email, usageData);
      } else {
        req._statsMeta.error_type = 'usage_missing';
        pool.markSuccess(account.email, {});
      }
      if (parseRejected) req._statsMeta.error_type = 'usage_parse_failed';
      if (!res.writableEnded) res.end();
      return;

    } catch (err) {
      var timeoutFetch = isTimeoutError(err);
      var netCode = timeoutFetch ? 'upstream_timeout' : 'network_error';
      var effectiveErr = err;
      var errMessage = (effectiveErr && effectiveErr.message) || (err && err.message) || netCode;
      pool.markError(account.email, 0, errMessage);
      req._statsMeta.error_type = netCode;
      if (!responseStarted && attempt < maxRetries && isRetryableError({
        status: timeoutFetch ? 504 : 502,
        error_type: netCode,
        message: errMessage,
        timeout: timeoutFetch,
        network: isNetworkError(effectiveErr),
        error: effectiveErr,
        has_sent_data: responseStarted,
      }) && planRetry(netCode, account, effectiveErr && effectiveErr.code ? ('code=' + effectiveErr.code) : '')) {
        continue;
      }
      sendResponsesError(timeoutFetch ? 504 : 502, (timeoutFetch ? 'Upstream timeout: ' : 'Network error: ') + errMessage, netCode, responseStarted);
      return;
    }
  }

  req._statsMeta.error_type = 'no_account';
  if (!res.headersSent) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
  }
  if (!res.writableEnded) res.end(JSON.stringify({ error: { message: 'No available account' } }));
}

function normalizeUsage(usage) {
  return normalizeCollectedUsage(usage);
}

function extractResponsesUsageFromBody(text) {
  if (!text) return null;
  try {
    var parsed = JSON.parse(text);
    return normalizeUsage((parsed.response && parsed.response.usage) || parsed.usage);
  } catch (_) {
    return null;
  }
}

// ============ 凭证上传 API ============

/**
 * POST /api/credentials
 *
 * 专用凭证上传接口，用 credentials.api_token 长字符串认证。
 * 支持在线追加/更新账号，无需访问管理面板。
 *
 * 认证: Authorization: Bearer <api_token>
 *
 * 请求体:
 *   { accounts: [{ email, accessToken, sessionToken?, password?, cookies? }, ...] }
 *   或直接数组:
 *   [{ email, accessToken, sessionToken?, password?, cookies? }, ...]
 *
 * 响应:
 *   { imported: 3, updated: 1, errors: [] }
 */
async function handleCredentialsAPI(req, res) {
  // 认证: 检查 credentials.api_token
  var apiToken = config.credentials && config.credentials.api_token;
  if (!apiToken) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: t('credentials.not_configured') }));
    return;
  }

  var authHeader = req.headers['authorization'] || '';
  var token = extractBearerToken(authHeader);
  if (!token || token !== apiToken) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: t('credentials.unauthorized') }));
    return;
  }

  // 解析请求体
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: t('credentials.invalid_body') }));
    return;
  }

  // 支持 { accounts: [...] } 或直接 [...]
  var list = Array.isArray(body) ? body : (body.accounts || []);
  if (!Array.isArray(list) || list.length === 0) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: t('credentials.empty_list') }));
    return;
  }

  var imported = 0;
  var updated = 0;
  var errors = [];

  for (var i = 0; i < list.length; i++) {
    var item = list[i];
    if (!item || typeof item !== 'object') {
      errors.push({ index: i, email: '', error: t('credentials.missing_fields') });
      continue;
    }
    if (!item.email || !item.accessToken) {
      errors.push({ index: i, email: item.email || '', error: t('credentials.missing_fields') });
      continue;
    }
    try {
      // addAccount 会自动判断新增还是更新
      var existing = pool.listAccounts().some(function (a) { return a.email === item.email; });
      pool.addAccount(item);
      if (existing) {
        updated++;
      } else {
        imported++;
      }
    } catch (e) {
      errors.push({ index: i, email: item.email || '', error: e.message });
    }
  }

  log('🔑', C.green, t('credentials.import_result', { imported: imported, updated: updated, errors: errors.length }));
  logCollector.add('info', t('credentials.import_result', { imported: imported, updated: updated, errors: errors.length }));

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ imported: imported, updated: updated, errors: errors }));
}

// ============ 全局异常兜底 ============
process.on('uncaughtException', function (err) {
  log('💀', C.red, '[FATAL] uncaughtException (process kept alive): ' + err.message);
  log('💀', C.red, err.stack || '');
});
process.on('unhandledRejection', function (reason) {
  log('💀', C.red, '[FATAL] unhandledRejection (process kept alive): ' + (reason && reason.message ? reason.message : String(reason)));
  if (reason && reason.stack) log('💀', C.red, reason.stack);
});

// ============ 启动 ============

var host = (config.server && config.server.host) || '0.0.0.0';
var port = (config.server && config.server.port) || 8066;
var IS_DRY_RUN = String(process.env.DRY_RUN || '').trim() === '1';

if (!IS_DRY_RUN) {
  server.listen(port, host, async function () {
    log('🚀', C.green, t('server.started', { host: host, port: port }));
    log('📌', C.cyan, '路由:');
    log('  ', C.gray, 'OpenAI:    POST /v1/chat/completions, GET /v1/models');
    log('  ', C.gray, 'Anthropic: POST /v1/messages');
    log('  ', C.gray, 'Gemini:    POST /v1beta/models/{model}:streamGenerateContent');
    log('  ', C.gray, 'Codex:     POST /backend-api/codex/responses');
    log('  ', C.gray, '管理面板:  GET /admin/');
    log('  ', C.gray, '管理API:   /admin/api/*');
    log('  ', C.gray, '凭证API:   POST /api/credentials');
    log('  ', C.gray, 'GPA导入:   POST /api/credentials/import/gpa');
    log('  ', C.gray, 'GPA导出:   GET /api/credentials/export/gpa');

    logCollector.add('info', t('server.started', { host: host, port: port }));

    // 启动时同步一次上游模型，并按间隔自动刷新
    await refreshUpstreamModels(true);
    var modelDiscoveryIntervalMs = getModelDiscoveryIntervalMs();
    if (isModelDiscoveryEnabled() && modelDiscoveryIntervalMs > 0) {
      modelDiscoveryTimer = setInterval(function () {
        refreshUpstreamModels(false).catch(function (err) {
          log('⚠️', C.yellow, '定时同步上游模型失败: ' + err.message);
        });
      }, modelDiscoveryIntervalMs);
      if (modelDiscoveryTimer.unref) modelDiscoveryTimer.unref();
    }

    // 启动 token 刷新
    if (config.credentials && config.credentials.auto_refresh !== false) {
      await tokenRefresher.start();
    }
    if (isAutoReloginEnabled()) {
      autoRelogin.start();
    } else {
      autoRelogin.stop();
      log('⏹️', C.yellow, 'AutoRelogin disabled by credentials.auto_relogin=false');
    }

    if (config.pool_health && config.pool_health.enabled) {
      poolHealthMonitor.start();
    }

  });
} else {
  log('🧪', C.cyan, 'DRY_RUN=1，跳过 server.listen');
}

// ============ 优雅退出 ============

var _shuttingDown = false;

function gracefulShutdown(signal) {
  if (_shuttingDown) return;
  _shuttingDown = true;
  log('🛑', C.yellow, t('server.graceful_shutdown') + ' (' + signal + ')');
  if (modelDiscoveryTimer) {
    clearInterval(modelDiscoveryTimer);
    modelDiscoveryTimer = null;
  }
  tokenRefresher.stop();
  autoRelogin.stop();
  poolHealthMonitor.stop();
  statsCollector.stop();
  behaviorAggregator.stop();
  riskLogger.stop();
  try {
    pool.forceSave();
    statsCollector.forceSave();
    logCollector.forceSave();
    log('💾', C.green, '账号状态、统计数据和日志已保存');
  } catch (e) {
    // ignore
  }
  server.close(function () {
    log('👋', C.green, t('server.stopped'));
    process.exit(0);
  });
  setTimeout(function () { process.exit(0); }, 3000);
}

process.on('SIGINT', function () { gracefulShutdown('SIGINT'); });
process.on('SIGTERM', function () { gracefulShutdown('SIGTERM'); });
