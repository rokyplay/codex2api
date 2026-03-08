/**
 * OpenAI Chat Completions 兼容路由
 *
 * 路径:
 *   GET  /v1/models           → 模型列表
 *   POST /v1/chat/completions → 对话补全（核心）
 *
 * 流程:
 *   客户端 OpenAI Chat 请求
 *   → parseRequest → Universal
 *   → resolveModel（去 prefix、解 alias）
 *   → pool.getAccount()
 *   → formatRequest → Codex Responses body
 *   → fetch 上游 + SSE 逐行解析
 *   → Universal 事件 → formatSSEChunk → 客户端
 *   → 成功 markSuccess / 429 自动换号重试
 *
 * fakeNonStream:
 *   客户端 stream=false → 内部 stream=true → 收集 → 非流式 JSON
 *
 */

import * as openaiChat from '../lib/converter/openai-chat.mjs';
import * as codexResponses from '../lib/converter/openai-responses.mjs';
import * as modelMapper from '../lib/converter/model-mapper.mjs';
import { createStreamCollector } from '../lib/converter/universal.mjs';
import { parseSSEStream } from '../lib/converter/stream/sse-parser.mjs';
import { SSE_HEADERS, createHeartbeat } from '../lib/converter/stream/openai-chat-sse.mjs';
import {
  readBody,
  derivePromptCacheSessionId,
  resolveRetryPolicy,
  isRetryableError,
  pickRetryAccount,
  isTimeoutError,
  isNetworkError,
} from '../lib/http-utils.mjs';
import { authenticateApiKey } from '../lib/api-key-auth.mjs';
import { C, timestamp } from '../lib/utils.mjs';

// 路由日志辅助
var TAG = C.cyan + '[openai]' + C.reset;
function rlog(msg) {
  console.log(C.gray + '[' + timestamp() + ']' + C.reset + ' ' + TAG + ' ' + msg);
}
function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return '"[unserializable:' + (err && err.message ? err.message : 'unknown') + ']"';
  }
}
function normalizeWebSearchAlias(value) {
  if (!value || typeof value !== 'string') return '';
  var lower = value.toLowerCase();
  if (lower.indexOf('web_search') === 0) return 'web_search';
  return '';
}
function isWebSearchRelatedTool(tool) {
  if (!tool) return false;
  if (typeof tool === 'string') return normalizeWebSearchAlias(tool) === 'web_search';
  if (typeof tool !== 'object') return false;
  if (normalizeWebSearchAlias(tool.type) === 'web_search') return true;
  if (tool.type === 'function' && tool.function && normalizeWebSearchAlias(tool.function.name) === 'web_search') return true;
  if (normalizeWebSearchAlias(tool.name) === 'web_search') return true;
  return false;
}
function statusColor(code) {
  if (code >= 200 && code < 300) return C.green;
  if (code >= 400 && code < 500) return C.yellow;
  return C.red;
}

function resolveUpstreamOriginator(req) {
  var raw = req && req.headers ? req.headers['x-upstream-originator'] : undefined;
  if (raw === undefined) return undefined;
  var value = Array.isArray(raw) ? String(raw[0] || '') : String(raw);
  if (value === '__omit__') return null;
  if (value === '__empty__') return '';
  return value;
}

/**
 * 注册 OpenAI 路由
 *
 * @param {object} ctx - { pool, config, i18n, t }
 * @returns {function} handler(req, res)
 */
export function createOpenAIRoutes(ctx) {
  return async function (req, res) {
    var path = req.url.split('?')[0];

    // GET /v1/models
    if (req.method === 'GET' && path === '/v1/models') {
      return handleModels(req, res, ctx);
    }

    // POST /v1/chat/completions
    if (req.method === 'POST' && path === '/v1/chat/completions') {
      return handleChatCompletions(req, res, ctx);
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(openaiChat.formatErrorResponse(404, 'Not found')));
  };
}

/**
 * GET /v1/models
 */
async function handleModels(req, res, ctx) {
  var isPublicHost = !!(req && req._isPublicHost);
  if (!isPublicHost) {
    var auth = authenticateApiKey(req, ctx.config);
    if (!auth.ok) {
      res.writeHead(auth.status || 401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(openaiChat.formatErrorResponse(auth.status || 401, ctx.t('request.auth_invalid'))));
      return;
    }
  }
  var models = modelMapper.listModels({ includePrefix: !isPublicHost });
  var body = openaiChat.formatModelsResponse(models);
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

/**
 * POST /v1/chat/completions
 */
async function handleChatCompletions(req, res, ctx) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(openaiChat.formatErrorResponse(400, 'Invalid request body')));
    return;
  }

  // 请求 body 日志
  rlog('📋 req body: model=' + (body.model || '-') + ' stream=' + body.stream + ' messages=' + (body.messages ? body.messages.length : 0) + ' max_tokens=' + (body.max_tokens || '-'));
  var requestTools = Array.isArray(body.tools) ? body.tools : [];
  rlog('[WEB-SEARCH] request.tools=' + safeStringify(requestTools));
  rlog('[WEB-SEARCH] request.web_search_options=' + safeStringify(body.web_search_options || null));
  var requestWebSearchTools = [];
  for (var rt = 0; rt < requestTools.length; rt++) {
    if (isWebSearchRelatedTool(requestTools[rt])) requestWebSearchTools.push(requestTools[rt]);
  }
  if (requestWebSearchTools.length > 0) {
    rlog('[WEB-SEARCH] request web_search-related tools=' + safeStringify(requestWebSearchTools));
  }

  // 认证检查
  var auth = authenticateApiKey(req, ctx.config);
  if (!auth.ok) {
    res.writeHead(auth.status || 401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(openaiChat.formatErrorResponse(auth.status || 401, ctx.t('request.auth_invalid'))));
    return;
  }
  req._apiKeyIdentity = auth.identity;

  // 解析请求 → Universal
  var universal = openaiChat.parseRequest(body);
  rlog('[WEB-SEARCH] universal.tools=' + safeStringify(universal.tools || []));

  // 模型解析
  var modelResult = modelMapper.resolveModel(universal.model);
  if (!modelResult.found && universal.model) {
    rlog(C.yellow + 'model not found: ' + C.reset + universal.model);
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(openaiChat.formatErrorResponse(400, ctx.t('request.model_not_found', { model: universal.model }))));
    return;
  }
  universal.model = modelResult.resolved;
  if (modelResult.reasoningEffort) {
    universal.metadata = universal.metadata || {};
    universal.metadata.reasoning_effort = modelResult.reasoningEffort;
  }

  // 客户端是否要流式（OpenAI API 规范: stream 未指定时默认 false）
  var clientWantsStream = universal.stream;

  // 请求进入日志
  rlog('POST /v1/chat/completions | model=' + C.bold + universal.model + C.reset + ' | stream=' + clientWantsStream);

  // 早期设置 _statsMeta，确保成功和失败请求都能被统计
  req._statsMeta = {
    route: 'openai',
    model: universal.model,
    account: '',
    stream: clientWantsStream,
    usage: null,
    error_type: null,
    status_override: null,
    caller_identity: req._apiKeyIdentity || auth.identity || 'unknown',
  };
  var clientSessionId = req.headers['session_id'] || req.headers['x-session-id'] || req.headers['x-codex-session-id'] || '';
  var metadata = universal.metadata || {};
  var clientPromptCacheKey = metadata.prompt_cache_key || body.prompt_cache_key || '';
  var clientUserSeed = metadata.user || body.user || '';
  var retryPolicy = resolveRetryPolicy(ctx.config, clientWantsStream);
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
    if (attempt === 1) return ctx.pool.getAccount();
    return pickRetryAccount(ctx.pool, triedAccounts);
  }

  function planRetry(reason, failedAccount, extra) {
    if (!failedAccount || !failedAccount.email) return false;
    triedAccounts.push(failedAccount.email);
    nextRetryAccount = pickRetryAccount(ctx.pool, triedAccounts);
    if (!nextRetryAccount) {
      rlog(C.yellow + '⚠ retry blocked: no alternate account' + C.reset + ' | retry_attempt=' + (attempt + 1) + '/' + maxRetries + ' | reason=' + reason + ' | failed_account=' + C.dim + failedAccount.email + C.reset);
      return false;
    }
    var details = extra ? ' | ' + extra : '';
    rlog(C.yellow + '⟳ retry_attempt=' + (attempt + 1) + '/' + maxRetries + C.reset
      + ' | reason=' + reason
      + ' | account=' + C.dim + failedAccount.email + C.reset
      + ' → ' + C.dim + nextRetryAccount.email + C.reset
      + details);
    return true;
  }

  while (attempt < maxRetries) {
    attempt++;

    // 获取可用账号
    var account = acquireRetryAccount();
    if (!account) {
      rlog(C.red + '✗ no available account' + C.reset + ' | attempt=' + attempt + '/' + maxRetries);
      req._statsMeta.error_type = 'no_account';
      var statusCode = 503;
      var errMsg = ctx.t('scheduler.no_available');
      if (clientWantsStream) {
        res.writeHead(statusCode, SSE_HEADERS);
        res.write('data: ' + JSON.stringify(openaiChat.formatErrorResponse(statusCode, errMsg)) + '\n\n');
        res.end();
      } else {
        res.writeHead(statusCode, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(openaiChat.formatErrorResponse(statusCode, errMsg)));
      }
      return;
    }

    var sessionId = clientSessionId;
    if (!sessionId && ctx.config.prompt_cache && ctx.config.prompt_cache.enabled) {
      // 关键修复: 派生 session_id 不再绑定 account，避免换号后 cache key 震荡导致命中率归零。
      sessionId = derivePromptCacheSessionId({
        callerIdentity: (req._statsMeta && req._statsMeta.caller_identity) || 'unknown',
        route: 'openai',
        model: universal.model,
        user: clientUserSeed,
        promptCacheKey: clientPromptCacheKey,
      });
    }

    // Universal → Codex Responses body
    var codexBody = codexResponses.formatRequest(universal);
    // 关键修复: 与 codex/responses 路由保持一致，统一做 Responses 体规范化，过滤上游不支持的 tool/type。
    codexResponses.adaptResponsesBody(codexBody, false);
    if (ctx.config.prompt_cache && ctx.config.prompt_cache.enabled) {
      if (!codexBody.prompt_cache_key && sessionId) codexBody.prompt_cache_key = sessionId;
      if (!codexBody.prompt_cache_retention && ctx.config.prompt_cache.default_retention) {
        codexBody.prompt_cache_retention = ctx.config.prompt_cache.default_retention;
      }
    }
    var originatorOverride = resolveUpstreamOriginator(req);
    var codexHeaders = originatorOverride === undefined
      ? codexResponses.formatHeaders(account.accessToken, account.accountId, sessionId)
      : codexResponses.formatHeaders(account.accessToken, account.accountId, sessionId, { originator: originatorOverride });
    var upstreamUrl = codexResponses.getEndpointUrl(ctx.config.upstream && ctx.config.upstream.base_url);

    // 详细调试日志：转换后的请求体
    var forwardedOriginator = Object.prototype.hasOwnProperty.call(codexHeaders, 'originator')
      ? codexHeaders.originator
      : '(omitted)';
    rlog('📮 upstream header originator=' + JSON.stringify(forwardedOriginator));
    rlog('📦 codexBody: input.length=' + (codexBody.input ? codexBody.input.length : 0) + ' tools=' + (codexBody.tools ? codexBody.tools.length : 0) + ' instructions.length=' + (codexBody.instructions ? codexBody.instructions.length : 0));
    rlog('[WEB-SEARCH] codexBody.tools=' + safeStringify(codexBody.tools || []));
    if (codexBody.input) {
      for (var di = 0; di < codexBody.input.length; di++) {
        var ditem = codexBody.input[di];
        var dsummary = 'type=' + ditem.type;
        if (ditem.role) dsummary += ' role=' + ditem.role;
        if (ditem.name) dsummary += ' name=' + ditem.name;
        if (ditem.call_id) dsummary += ' call_id=' + ditem.call_id;
        if (ditem.content && Array.isArray(ditem.content)) dsummary += ' content_blocks=' + ditem.content.length;
        if (ditem.output) dsummary += ' output.len=' + ditem.output.length;
        if (ditem.arguments) dsummary += ' args.len=' + ditem.arguments.length;
        rlog('  input[' + di + '] ' + dsummary);
      }
    }
    rlog('→ upstream POST ' + C.dim + upstreamUrl + C.reset + ' | model=' + universal.model + ' | attempt=' + attempt + '/' + maxRetries + ' | account=' + C.dim + account.email + C.reset);

    try {
      var fetchStartTime = Date.now();
      var upstreamResp = await fetch(upstreamUrl, {
        method: 'POST',
        headers: codexHeaders,
        body: JSON.stringify(codexBody),
        signal: AbortSignal.timeout(retryPolicy.total_timeout_ms),
      });

      // 错误处理
      if (!upstreamResp.ok) {
        var errLatency = Date.now() - fetchStartTime;
        var errBody = await upstreamResp.text().catch(function () { return ''; });
        rlog('← ' + statusColor(upstreamResp.status) + upstreamResp.status + ' ERROR' + C.reset + ' | latency=' + errLatency + 'ms | account=' + C.dim + account.email + C.reset + ' | body: ' + (errBody || '(empty)'));
        var errResult = ctx.pool.markError(account.email, upstreamResp.status, errBody);
        req._statsMeta.account = account.email;
        req._statsMeta.error_type = 'upstream_' + upstreamResp.status;

        var retryableUpstream = isRetryableError({
          status: upstreamResp.status,
          message: errBody,
          error_type: (errResult && errResult.type) || ('upstream_' + upstreamResp.status),
          account_issue: upstreamResp.status === 401 || upstreamResp.status === 403 || !!(errResult && (errResult.action === 'switch_account' || errResult.action === 'retry' || errResult.action === 'refresh_token' || errResult.action === 'relogin')),
        });
        if (retryableUpstream && attempt < maxRetries && planRetry('upstream_' + upstreamResp.status, account, 'error_type=' + ((errResult && errResult.type) || 'unknown'))) {
          continue;
        }

        // 其他错误直接返回
        var upstreamErrMsg = extractUpstreamErrorMessage(errBody, upstreamResp.status);
        var errJson = openaiChat.formatErrorResponse(upstreamResp.status, upstreamErrMsg);
        if (clientWantsStream) {
          res.writeHead(upstreamResp.status, SSE_HEADERS);
          res.write('data: ' + JSON.stringify(errJson) + '\n\n');
          res.end();
        } else {
          res.writeHead(upstreamResp.status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(errJson));
        }
        return;
      }

      // 成功 — 处理响应流
      var successLatency = Date.now() - fetchStartTime;
      rlog('← ' + C.green + '200 OK' + C.reset + ' | latency=' + successLatency + 'ms | account=' + C.dim + account.email + C.reset);
      var prefixedModel = modelMapper.addPrefix(universal.model);
      req._statsMeta.account = account.email;
      req._statsMeta.error_type = null;

      var processingResult = null;
      if (clientWantsStream) {
        // 流式：逐行转换 SSE
        processingResult = await handleStreamResponse(upstreamResp, res, account, prefixedModel, ctx, req, retryPolicy);
      } else {
        // 假非流：收集完整响应后返回 JSON
        processingResult = await handleFakeNonStreamResponse(upstreamResp, res, account, prefixedModel, ctx, req, retryPolicy);
      }

      if (!processingResult || processingResult.success) {
        return;
      }
      if (processingResult.retryable && attempt < maxRetries && !processingResult.has_sent_data) {
        if (planRetry(processingResult.reason || processingResult.error_type || 'upstream_retryable_failure', account, processingResult.message ? ('message=' + processingResult.message) : '')) {
          continue;
        }
      }
      if (processingResult.response_sent) {
        return;
      }
      return;

    } catch (err) {
      // 网络错误
      var timeoutFlag = isTimeoutError(err);
      var networkFlag = isNetworkError(err) || timeoutFlag;
      var reasonType = timeoutFlag ? 'upstream_timeout' : 'network_error';
      rlog(C.red + '✗ ' + reasonType + ': ' + C.reset + err.message + ' | account=' + C.dim + account.email + C.reset + ' | attempt=' + attempt + '/' + maxRetries + (timeoutFlag ? ' | timeout_ms=' + retryPolicy.total_timeout_ms : ''));
      ctx.pool.markError(account.email, 0, err.message);
      req._statsMeta.account = account.email;
      req._statsMeta.error_type = reasonType;
      if (networkFlag && attempt < maxRetries && planRetry(reasonType, account, err.code ? ('code=' + err.code) : '')) {
        continue;
      }

      var finalStatus = timeoutFlag ? 504 : 502;
      var networkErr = openaiChat.formatErrorResponse(finalStatus, (timeoutFlag ? 'Upstream timeout: ' : 'Network error: ') + err.message);
      if (res.writableEnded) return;
      try {
        if (res.headersSent) {
          if (clientWantsStream) {
            res.write('data: ' + JSON.stringify(networkErr) + '\n\n');
          }
          res.end();
        } else if (clientWantsStream) {
          res.writeHead(finalStatus, SSE_HEADERS);
          res.write('data: ' + JSON.stringify(networkErr) + '\n\n');
          res.end();
        } else {
          res.writeHead(finalStatus, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(networkErr));
        }
      } catch (_) {
        if (!res.writableEnded) try { res.end(); } catch (__) {}
      }
      return;
    }
  }
}

/**
 * 提取上游错误消息，优先使用上游 JSON 中的 error.message，兜底为 HTTP 状态描述。
 */
function extractUpstreamErrorMessage(errBody, statusCode) {
  if (!errBody) return 'Upstream error: ' + statusCode;
  try {
    var parsed = JSON.parse(errBody);
    if (parsed && parsed.error) {
      if (typeof parsed.error === 'string' && parsed.error) {
        return parsed.error.substring(0, 500);
      }
      if (parsed.error.message) {
        return String(parsed.error.message).substring(0, 500);
      }
    }
  } catch (_) {}
  var cleaned = String(errBody).replace(/\s+/g, ' ').trim();
  if (!cleaned) return 'Upstream error: ' + statusCode;
  return cleaned.substring(0, 500);
}

function extractResponseFailureMessage(responsePayload) {
  if (!responsePayload || typeof responsePayload !== 'object') return 'Upstream response failed';
  var errObj = responsePayload.error || {};
  if (typeof errObj === 'string' && errObj) return errObj.substring(0, 500);
  if (errObj && typeof errObj === 'object') {
    if (errObj.message) return String(errObj.message).substring(0, 500);
    if (errObj.code) return String(errObj.code).substring(0, 500);
  }
  if (responsePayload.incomplete_details && responsePayload.incomplete_details.reason) {
    return String(responsePayload.incomplete_details.reason).substring(0, 500);
  }
  return 'Upstream response failed';
}

function isUsageEmpty(usage) {
  if (!usage || typeof usage !== 'object') return true;
  return (usage.input_tokens || 0) === 0
    && (usage.output_tokens || 0) === 0
    && (usage.cached_tokens || 0) === 0
    && (usage.reasoning_tokens || 0) === 0;
}

function hasUsableUniversalResponse(response) {
  if (!response || typeof response !== 'object') return false;
  if (response.content && String(response.content).length > 0) return true;
  if (response.reasoning && String(response.reasoning).length > 0) return true;
  if (Array.isArray(response.tool_calls) && response.tool_calls.length > 0) return true;
  if (Array.isArray(response.annotations) && response.annotations.length > 0) return true;
  return false;
}

function normalizeSSEErrorInfo(errorLike, fallbackMessage, fallbackStatus) {
  var message = fallbackMessage || 'upstream_stream_error';
  var code = '';
  var type = '';
  var status = fallbackStatus || 502;
  if (typeof errorLike === 'string') {
    message = errorLike || message;
  } else if (errorLike && typeof errorLike === 'object') {
    if (errorLike.message) message = String(errorLike.message);
    else if (errorLike.error && typeof errorLike.error === 'string') message = errorLike.error;
    else if (errorLike.detail && typeof errorLike.detail === 'string') message = errorLike.detail;
    if (errorLike.code) code = String(errorLike.code);
    if (errorLike.type) type = String(errorLike.type);
    if (typeof errorLike.status === 'number' && isFinite(errorLike.status) && errorLike.status > 0) {
      status = Math.floor(errorLike.status);
    }
  } else if (errorLike !== undefined && errorLike !== null) {
    message = String(errorLike);
  }
  return {
    message: message || fallbackMessage || 'upstream_stream_error',
    code: code || '',
    type: type || '',
    status: status || fallbackStatus || 502,
  };
}

function classifySSEError(errorInfo) {
  var info = normalizeSSEErrorInfo(errorInfo, 'upstream_stream_error', 502);
  var code = String(info.code || '').toLowerCase();
  var type = String(info.type || '').toLowerCase();
  var messageLower = String(info.message || '').toLowerCase();

  function hasFlag(flag) {
    return code.indexOf(flag) !== -1 || type.indexOf(flag) !== -1 || messageLower.indexOf(flag) !== -1;
  }

  if (hasFlag('content_filter')) {
    info.status = 400;
    info.category = 'model_error';
    info.error_type = 'content_filter';
    return info;
  }
  if (hasFlag('rate_limit') || hasFlag('insufficient_quota')) {
    info.status = 429;
    info.category = 'transient_upstream';
    info.error_type = 'rate_limit';
    return info;
  }
  if (hasFlag('overloaded')) {
    info.status = 503;
    info.category = 'transient_upstream';
    info.error_type = 'overloaded';
    return info;
  }
  if (hasFlag('server_error') || hasFlag('internal_error') || hasFlag('upstream_error')) {
    info.status = 503;
    info.category = 'transient_upstream';
    info.error_type = 'server_error';
    return info;
  }
  if (hasFlag('network') || hasFlag('connection') || hasFlag('timeout') || hasFlag('parse_error')) {
    info.status = 502;
    info.category = 'network_error';
    info.error_type = 'network_error';
    return info;
  }
  if (info.status === 401 || hasFlag('authentication')) {
    info.status = 401;
    info.category = 'auth_error';
    info.error_type = 'authentication_error';
    return info;
  }

  info.status = info.status || 502;
  info.category = 'upstream_error';
  info.error_type = info.code || info.type || 'upstream_stream_error';
  return info;
}

/**
 * 流式响应处理
 */
async function handleStreamResponse(upstreamResp, res, account, model, ctx, req, retryPolicy) {
  var streamStartTime = Date.now();
  var heartbeat = null;
  var sseCtx = openaiChat.createStreamContext(model);
  var parseState = codexResponses.createParseState();
  var totalUsage = { input_tokens: 0, output_tokens: 0 };
  var sawStreamError = false;
  var streamErrorInfo = null;
  var headersSent = false;
  var chunksSent = 0;
  var meaningfulChunksSent = 0;
  var pendingStartChunk = '';
  var pendingDoneChunk = '';
  var shouldFinalizeResponse = true;

  function ensureSSEHeaders() {
    if (headersSent || res.writableEnded || res.headersSent) return;
    res.writeHead(200, SSE_HEADERS);
    headersSent = true;
    if (!heartbeat) heartbeat = createHeartbeat(res);
  }

  function writeSSEChunk(chunk) {
    if (!chunk || res.writableEnded) return;
    ensureSSEHeaders();
    res.write(chunk);
    chunksSent++;
  }

  try {
    var eventCounter = 0;
    await parseSSEStream(upstreamResp.body, function (eventType, data) {
      eventCounter++;

      // [DONE] 信号 — SSE parser 传入 ('done', null)，直接忽略
      // Codex 通道通过 response.completed 事件发送 done
      if (!data && eventType === 'done') {
        rlog('  SSE[' + eventCounter + '] [DONE] signal');
        return;
      }
      if (eventType === 'parse_error') {
        sawStreamError = true;
        streamErrorInfo = normalizeSSEErrorInfo(
          { code: 'parse_error', type: 'parse_error', message: (data && data.error) || 'sse_parse_error', status: 502 },
          'sse_parse_error',
          502
        );
        return;
      }

      // 详细记录上游事件
      var evtType = (data && data.type) || eventType;
      rlog('  SSE[' + eventCounter + '] event=' + evtType + (data && data.delta ? ' delta.len=' + String(data.delta).length : '') + (data && data.response ? ' resp.status=' + (data.response.status || '-') : ''));
      if (data && typeof data === 'object') {
        var rawUsage = codexResponses.normalizeCollectedUsage((data.response && data.response.usage) || data.usage);
        if (rawUsage) totalUsage = rawUsage;
      }

      var universalEvent = codexResponses.parseSSEEvent(eventType, data, parseState);
      if (!universalEvent) {
        rlog('  SSE[' + eventCounter + '] → (ignored)');
        return;
      }
      var universalEvents = Array.isArray(universalEvent) ? universalEvent : [universalEvent];
      for (var ue = 0; ue < universalEvents.length; ue++) {
        var evt = universalEvents[ue];
        if (!evt) continue;
        rlog('  SSE[' + eventCounter + '] → universal type=' + evt.type + (evt.content ? ' content.len=' + evt.content.length : '') + (evt.tool_call ? ' tool=' + (evt.tool_call.name || '-') : '') + (evt.finish_reason ? ' finish=' + evt.finish_reason : ''));
        if (evt.type === 'error') {
          sawStreamError = true;
          streamErrorInfo = normalizeSSEErrorInfo(evt.error || evt, 'upstream_stream_error', 502);
          continue;
        }
        if (evt.usage) {
          totalUsage = evt.usage;
        }
        var chunk = openaiChat.formatSSEChunk(evt, sseCtx);
        if (evt.type === 'start' && meaningfulChunksSent === 0) {
          pendingStartChunk += chunk || '';
          continue;
        }
        if (evt.type === 'done' && meaningfulChunksSent === 0 && isUsageEmpty(totalUsage) && !evt.error) {
          pendingDoneChunk += chunk || '';
          continue;
        }
        if (pendingStartChunk) {
          writeSSEChunk(pendingStartChunk);
          pendingStartChunk = '';
        }
        if (evt.type !== 'start' && evt.type !== 'done') meaningfulChunksSent++;
        writeSSEChunk(chunk);
      }
    }, {
      firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
      idleTimeoutMs: retryPolicy.idle_timeout_ms,
    });
    rlog('  SSE total events: ' + eventCounter);

    if (sawStreamError) {
      var classifiedError = classifySSEError(streamErrorInfo || { message: 'upstream_stream_error', status: 502 });
      ctx.pool.markError(account.email, 0, classifiedError.message || 'upstream_stream_error');
      if (req._statsMeta) {
        req._statsMeta.error_type = classifiedError.error_type || 'upstream_stream_error';
        req._statsMeta.status_override = classifiedError.status || 502;
        if (!isUsageEmpty(totalUsage)) req._statsMeta.usage = totalUsage;
      }
      var canRetryStreamError = isRetryableError({
        status: classifiedError.status || 502,
        error_type: classifiedError.error_type,
        error_code: classifiedError.code,
        message: classifiedError.message,
        has_sent_data: chunksSent > 0,
      });
      if (chunksSent > 0) {
        var errorDoneChunk = openaiChat.formatSSEChunk({
          type: 'done',
          finish_reason: 'error',
          usage: isUsageEmpty(totalUsage) ? null : totalUsage,
          error: classifiedError,
        }, sseCtx);
        writeSSEChunk(errorDoneChunk);
        return { success: false, retryable: false, has_sent_data: true, response_sent: true };
      }
      if (canRetryStreamError) {
        shouldFinalizeResponse = false;
        return {
          success: false,
          retryable: true,
          has_sent_data: false,
          reason: classifiedError.error_type || 'upstream_stream_error',
          error_type: classifiedError.error_type || 'upstream_stream_error',
          message: classifiedError.message || 'upstream_stream_error',
        };
      }
      if (!res.writableEnded) {
        res.writeHead(classifiedError.status || 502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(openaiChat.formatErrorResponse(
          classifiedError.status || 502,
          classifiedError.message || 'Upstream stream error',
          classifiedError.type || 'server_error',
          null,
          classifiedError.code || undefined
        )));
      }
      rlog(C.yellow + '⚠ stream ended with upstream error' + C.reset + ' | model=' + model + ' | account=' + C.dim + account.email + C.reset + ' | message=' + (classifiedError.message || 'upstream_stream_error'));
      return { success: false, retryable: false, has_sent_data: false, response_sent: true };
    } else {
      if (chunksSent === 0 && isUsageEmpty(totalUsage)) {
        var emptyStreamMessage = 'upstream_empty_stream';
        ctx.pool.markError(account.email, 0, emptyStreamMessage);
        if (req._statsMeta) {
          req._statsMeta.error_type = 'empty_response';
          req._statsMeta.status_override = 502;
        }
        shouldFinalizeResponse = false;
        return {
          success: false,
          retryable: true,
          has_sent_data: false,
          reason: 'empty_response',
          error_type: 'empty_response',
          message: emptyStreamMessage,
        };
      }
      if (chunksSent === 0 && pendingStartChunk && !isUsageEmpty(totalUsage)) {
        writeSSEChunk(pendingStartChunk);
        pendingStartChunk = '';
      }
      if (chunksSent === 0 && pendingDoneChunk && !isUsageEmpty(totalUsage)) {
        writeSSEChunk(pendingDoneChunk);
        pendingDoneChunk = '';
      }
      ctx.pool.markSuccess(account.email, totalUsage);
      if (req._statsMeta) req._statsMeta.usage = totalUsage;
    }
    var streamLatency = Date.now() - streamStartTime;
    rlog(C.green + '✓ done' + C.reset + ' | model=' + model + ' | tokens: ' + (totalUsage.input_tokens || 0) + '→' + (totalUsage.output_tokens || 0) + ' | latency=' + streamLatency + 'ms' + (sawStreamError ? ' | upstream_error=1' : ''));
    return { success: true };
  } catch (err) {
    var parseErr = classifySSEError({
      code: isTimeoutError(err) ? 'upstream_timeout' : 'network_error',
      type: isTimeoutError(err) ? 'upstream_timeout' : 'network_error',
      message: err.message,
      status: isTimeoutError(err) ? 504 : 502,
    });
    rlog(C.red + '✗ stream error: ' + C.reset + parseErr.message + ' | account=' + C.dim + account.email + C.reset);
    ctx.pool.markError(account.email, 0, parseErr.message);
    if (req._statsMeta) {
      req._statsMeta.error_type = parseErr.error_type || 'stream_error';
      req._statsMeta.status_override = parseErr.status || 502;
      if (!isUsageEmpty(totalUsage)) req._statsMeta.usage = totalUsage;
    }
    if (chunksSent > 0) {
      var catchDoneChunk = openaiChat.formatSSEChunk({
        type: 'done',
        finish_reason: 'error',
        usage: isUsageEmpty(totalUsage) ? null : totalUsage,
        error: parseErr,
      }, sseCtx);
      writeSSEChunk(catchDoneChunk);
      return { success: false, retryable: false, has_sent_data: true, response_sent: true };
    }
    var canRetryParseError = isRetryableError({
      status: parseErr.status || 502,
      error_type: parseErr.error_type || 'stream_error',
      error_code: parseErr.code || '',
      message: parseErr.message || '',
      timeout: isTimeoutError(err),
      network: isNetworkError(err),
      error: err,
      has_sent_data: false,
    });
    if (canRetryParseError) {
      shouldFinalizeResponse = false;
      return {
        success: false,
        retryable: true,
        has_sent_data: false,
        reason: parseErr.error_type || 'stream_error',
        error_type: parseErr.error_type || 'stream_error',
        message: parseErr.message || 'stream_error',
      };
    }
    if (!res.writableEnded) {
      res.writeHead(parseErr.status || 502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(openaiChat.formatErrorResponse(
        parseErr.status || 502,
        parseErr.message || 'Stream error',
        parseErr.type || 'server_error',
        null,
        parseErr.code || undefined
      )));
    }
    return { success: false, retryable: false, has_sent_data: false, response_sent: true };
  } finally {
    if (heartbeat) heartbeat.stop();
    if (shouldFinalizeResponse && !res.writableEnded) res.end();
  }
}

/**
 *
 * 上游 stream=true → 收集所有 delta → 返回完整 JSON
 */
async function handleFakeNonStreamResponse(upstreamResp, res, account, model, ctx, req, retryPolicy) {
  var collectStartTime = Date.now();

  try {
    var collected = await codexResponses.collectNonStreamResponseFromSSE(upstreamResp.body, {
      firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
      idleTimeoutMs: retryPolicy.idle_timeout_ms,
    });
    var responsePayload = collected && collected.response ? collected.response : null;
    var collectedErrorInfo = null;
    if (collected && (collected.error_info || collected.error)) {
      collectedErrorInfo = classifySSEError(collected.error_info || { message: collected.error || 'upstream_stream_error', status: 502 });
    }

    var universalResp = null;
    if (responsePayload) {
      var parseState = codexResponses.createParseState();
      var responseType = responsePayload.type;
      var envelope = (
        responseType === 'response.completed'
        || responseType === 'response.incomplete'
        || responseType === 'response.failed'
      )
        ? responsePayload
        : { type: 'response.completed', response: responsePayload };
      var envelopeCollector = createStreamCollector();
      var universalEvent = codexResponses.parseSSEEvent(envelope.type || 'response.completed', envelope, parseState);
      if (universalEvent) {
        var universalEvents = Array.isArray(universalEvent) ? universalEvent : [universalEvent];
        for (var ue = 0; ue < universalEvents.length; ue++) {
          if (universalEvents[ue] && universalEvents[ue].type !== 'error') envelopeCollector.push(universalEvents[ue]);
        }
      }
      universalResp = envelopeCollector.toResponse();
    }

    if (!universalResp && collected && collected.universal_response && typeof collected.universal_response === 'object') {
      universalResp = collected.universal_response;
    }
    if (!universalResp || typeof universalResp !== 'object') {
      universalResp = {
        id: '',
        model: model,
        content: '',
        reasoning: '',
        finish_reason: 'stop',
        usage: collected && collected.usage ? collected.usage : { input_tokens: 0, output_tokens: 0 },
        tool_calls: [],
        annotations: [],
      };
    }

    universalResp.model = model;
    if (collected && collected.usage && (!universalResp.usage || isUsageEmpty(universalResp.usage))) {
      universalResp.usage = collected.usage;
    }

    var hasPartial = hasUsableUniversalResponse(universalResp);
    var hasSSEError = !!(collected && (collected.saw_error || collected.error_info || collected.error));

    if (!hasPartial && (!collected || !collected.success || !responsePayload)) {
      var failedInfo = collectedErrorInfo || classifySSEError({ message: (collected && collected.error) || 'missing_completed_response', status: 502 });
      var failedMessage = responsePayload ? extractResponseFailureMessage(responsePayload) : failedInfo.message;
      rlog(C.yellow + '⚠ upstream failed response in non-stream: ' + C.reset + failedMessage + ' | account=' + C.dim + account.email + C.reset);
      ctx.pool.markError(account.email, 0, failedMessage);
      if (req._statsMeta) {
        req._statsMeta.error_type = failedInfo.error_type || 'upstream_response_failed';
        req._statsMeta.status_override = failedInfo.status || 502;
        if (!isUsageEmpty(universalResp.usage)) req._statsMeta.usage = universalResp.usage;
      }
      var retryableFailedResponse = isRetryableError({
        status: failedInfo.status || 502,
        error_type: failedInfo.error_type || 'upstream_response_failed',
        error_code: failedInfo.code || '',
        message: failedMessage || failedInfo.message || 'upstream_response_failed',
        has_sent_data: false,
      });
      if (retryableFailedResponse) {
        return {
          success: false,
          retryable: true,
          has_sent_data: false,
          reason: failedInfo.error_type || 'upstream_response_failed',
          error_type: failedInfo.error_type || 'upstream_response_failed',
          message: failedMessage || failedInfo.message || 'upstream_response_failed',
        };
      }
      res.writeHead(failedInfo.status || 502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(openaiChat.formatErrorResponse(
        failedInfo.status || 502,
        failedMessage || failedInfo.message || 'Upstream response failed',
        failedInfo.type || 'server_error',
        null,
        failedInfo.code || undefined
      )));
      return { success: false, retryable: false, has_sent_data: false, response_sent: true };
    }

    if (!hasPartial && isUsageEmpty(universalResp.usage)) {
      var emptyMessage = 'upstream_empty_response';
      rlog(C.yellow + '⚠ non-stream empty response' + C.reset + ' | model=' + model + ' | account=' + C.dim + account.email + C.reset);
      ctx.pool.markError(account.email, 0, emptyMessage);
      if (req._statsMeta) {
        req._statsMeta.error_type = 'empty_response';
        req._statsMeta.status_override = 502;
      }
      return {
        success: false,
        retryable: true,
        has_sent_data: false,
        reason: 'empty_response',
        error_type: 'empty_response',
        message: emptyMessage,
      };
    }

    var nonStreamBody = openaiChat.formatNonStreamResponse(universalResp);
    if (hasSSEError && hasPartial) {
      var bestEffortError = collectedErrorInfo || classifySSEError({ message: 'upstream_stream_error', status: 502 });
      ctx.pool.markError(account.email, 0, bestEffortError.message || 'upstream_stream_error');
      if (req._statsMeta) {
        req._statsMeta.error_type = bestEffortError.error_type || 'upstream_stream_error';
        req._statsMeta.status_override = bestEffortError.status || 502;
        if (!isUsageEmpty(universalResp.usage)) req._statsMeta.usage = universalResp.usage;
      }
      var retryableBestEffort = isRetryableError({
        status: bestEffortError.status || 502,
        error_type: bestEffortError.error_type || 'upstream_stream_error',
        error_code: bestEffortError.code || '',
        message: bestEffortError.message || 'upstream_stream_error',
        has_sent_data: false,
      });
      if (retryableBestEffort) {
        return {
          success: false,
          retryable: true,
          has_sent_data: false,
          reason: bestEffortError.error_type || 'upstream_stream_error',
          error_type: bestEffortError.error_type || 'upstream_stream_error',
          message: bestEffortError.message || 'upstream_stream_error',
        };
      }
      rlog(C.yellow + '⚠ non-stream best-effort response with upstream error' + C.reset + ' | model=' + model + ' | account=' + C.dim + account.email + C.reset + ' | message=' + (bestEffortError.message || 'upstream_stream_error'));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(nonStreamBody));
      return { success: false, retryable: false, has_sent_data: false, response_sent: true };
    }

    ctx.pool.markSuccess(account.email, universalResp.usage);
    if (req._statsMeta) req._statsMeta.usage = universalResp.usage;
    var collectLatency = Date.now() - collectStartTime;
    rlog(C.green + '✓ done (non-stream)' + C.reset + ' | model=' + model + ' | tokens: ' + ((universalResp.usage && universalResp.usage.input_tokens) || 0) + '→' + ((universalResp.usage && universalResp.usage.output_tokens) || 0) + ' | latency=' + collectLatency + 'ms');

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(nonStreamBody));
    return { success: true };
  } catch (err) {
    rlog(C.red + '✗ stream collection error: ' + C.reset + err.message + ' | account=' + C.dim + account.email + C.reset);
    ctx.pool.markError(account.email, 0, err.message);
    var retryableCollectErr = isRetryableError({
      status: isTimeoutError(err) ? 504 : 502,
      error_type: isTimeoutError(err) ? 'upstream_timeout' : 'stream_error',
      message: err.message || 'stream_error',
      timeout: isTimeoutError(err),
      network: isNetworkError(err),
      error: err,
      has_sent_data: false,
    });
    if (req._statsMeta) {
      req._statsMeta.error_type = isTimeoutError(err) ? 'upstream_timeout' : 'stream_error';
      req._statsMeta.status_override = isTimeoutError(err) ? 504 : 502;
    }
    if (retryableCollectErr) {
      return {
        success: false,
        retryable: true,
        has_sent_data: false,
        reason: isTimeoutError(err) ? 'upstream_timeout' : 'stream_error',
        error_type: isTimeoutError(err) ? 'upstream_timeout' : 'stream_error',
        message: err.message || 'stream_collection_error',
      };
    }
    var statusCode = isTimeoutError(err) ? 504 : 502;
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(openaiChat.formatErrorResponse(statusCode, 'Stream collection error: ' + err.message)));
    return { success: false, retryable: false, has_sent_data: false, response_sent: true };
  }
}
