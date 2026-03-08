/**
 * Anthropic Messages API 路由（骨架）
 *
 * 路径:
 *   POST /v1/messages → Anthropic Messages 格式
 *
 * 流程与 openai.mjs 类似:
 *   客户端 Anthropic 请求 → Universal → Codex → Universal → Anthropic SSE
 */

import * as anthropic from '../lib/converter/anthropic.mjs';
import * as codexResponses from '../lib/converter/openai-responses.mjs';
import * as modelMapper from '../lib/converter/model-mapper.mjs';
import { createStreamCollector } from '../lib/converter/universal.mjs';
import { parseSSEStream } from '../lib/converter/stream/sse-parser.mjs';
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
var TAG = C.yellow + '[anthropic]' + C.reset;
function rlog(msg) {
  console.log(C.gray + '[' + timestamp() + ']' + C.reset + ' ' + TAG + ' ' + msg);
}
function statusColor(code) {
  if (code >= 200 && code < 300) return C.green;
  if (code >= 400 && code < 500) return C.yellow;
  return C.red;
}

export function createAnthropicRoutes(ctx) {
  return async function (req, res) {
    var path = req.url.split('?')[0];

    if (req.method === 'POST' && path === '/v1/messages') {
      return handleMessages(req, res, ctx);
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(anthropic.formatErrorResponse(404, 'Not found')));
  };
}

async function handleMessages(req, res, ctx) {
  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(anthropic.formatErrorResponse(400, 'Invalid request body')));
    return;
  }

  // 认证检查（保持 x-api-key 优先）
  var auth = authenticateApiKey(req, ctx.config, {
    sourceOrder: ['x-api-key', 'authorization', 'query.key'],
  });
  if (!auth.ok) {
    res.writeHead(auth.status || 401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(anthropic.formatErrorResponse(auth.status || 401, ctx.t('request.auth_invalid'))));
    return;
  }
  req._apiKeyIdentity = auth.identity;

  // 解析请求 → Universal
  var universal = anthropic.parseRequest(body);

  // 模型解析
  var modelResult = modelMapper.resolveModel(universal.model);
  if (!modelResult.found && universal.model) {
    rlog(C.yellow + '⚠ unknown model: ' + C.reset + universal.model + ' → fallback: ' + modelResult.resolved);
  }
  universal.model = modelResult.resolved;
  if (modelResult.reasoningEffort) {
    universal.metadata = universal.metadata || {};
    universal.metadata.reasoning_effort = modelResult.reasoningEffort;
  }

  var clientWantsStream = body.stream === true;

  // 请求进入日志
  rlog('POST /v1/messages | model=' + C.bold + universal.model + C.reset + ' | stream=' + clientWantsStream);

  // 早期设置 _statsMeta，确保成功和失败请求都能被统计
  req._statsMeta = {
    route: 'anthropic',
    model: universal.model,
    account: '',
    stream: clientWantsStream,
    usage: null,
    error_type: null,
    caller_identity: req._apiKeyIdentity || auth.identity || 'unknown',
  };
  var requestStartTime = Date.now();
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
    rlog(C.yellow + '⟳ retry_attempt=' + (attempt + 1) + '/' + maxRetries + C.reset + ' | reason=' + reason + ' | account=' + C.dim + failedAccount.email + C.reset + ' → ' + C.dim + nextRetryAccount.email + C.reset + details);
    return true;
  }

  function isUsageEmpty(usage) {
    if (!usage || typeof usage !== 'object') return true;
    return (usage.input_tokens || 0) === 0
      && (usage.output_tokens || 0) === 0
      && (usage.cached_tokens || 0) === 0
      && (usage.reasoning_tokens || 0) === 0;
  }

  function hasUsableResponse(resp) {
    if (!resp || typeof resp !== 'object') return false;
    if (resp.content && String(resp.content).length > 0) return true;
    if (resp.reasoning && String(resp.reasoning).length > 0) return true;
    if (Array.isArray(resp.tool_calls) && resp.tool_calls.length > 0) return true;
    return false;
  }

  var clientSessionId = req.headers['session_id'] || req.headers['x-session-id'] || req.headers['x-codex-session-id'] || '';
  var sessionId = clientSessionId;
  if (!sessionId && ctx.config.prompt_cache && ctx.config.prompt_cache.enabled) {
    sessionId = derivePromptCacheSessionId({
      callerIdentity: (req._statsMeta && req._statsMeta.caller_identity) || 'unknown',
      route: 'anthropic',
      model: universal.model,
      promptCacheKey: body.prompt_cache_key,
      user: body.user,
    });
  }

  // Universal → Codex body
  var codexBody = codexResponses.formatRequest(universal);
  if (ctx.config.prompt_cache && ctx.config.prompt_cache.enabled) {
    if (!codexBody.prompt_cache_key && sessionId) codexBody.prompt_cache_key = sessionId;
    if (!codexBody.prompt_cache_retention && ctx.config.prompt_cache.default_retention) {
      codexBody.prompt_cache_retention = ctx.config.prompt_cache.default_retention;
    }
  }
  var upstreamUrl = codexResponses.getEndpointUrl(ctx.config.upstream && ctx.config.upstream.base_url);

  while (attempt < maxRetries) {
    attempt++;
    var account = acquireRetryAccount();
    if (!account) break;
    req._statsMeta.account = account.email;
    var responseStarted = false;
    var codexHeaders = codexResponses.formatHeaders(account.accessToken, account.accountId, sessionId);

    rlog('→ upstream POST ' + C.dim + upstreamUrl + C.reset + ' | model=' + universal.model + ' | attempt=' + attempt + '/' + maxRetries + ' | account=' + C.dim + account.email + C.reset);

    try {
      var fetchStartTime = Date.now();
      var upstreamResp = await fetch(upstreamUrl, {
        method: 'POST',
        headers: codexHeaders,
        body: JSON.stringify(codexBody),
        signal: AbortSignal.timeout(retryPolicy.total_timeout_ms),
      });

      if (!upstreamResp.ok) {
        var errLatency = Date.now() - fetchStartTime;
        var errBody = await upstreamResp.text().catch(function () { return ''; });
        rlog('← ' + statusColor(upstreamResp.status) + upstreamResp.status + ' ERROR' + C.reset + ' | latency=' + errLatency + 'ms | account=' + C.dim + account.email + C.reset + ' | body: ' + (errBody || '(empty)'));
        var errResult = ctx.pool.markError(account.email, upstreamResp.status, errBody);
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
        res.writeHead(upstreamResp.status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(anthropic.formatErrorResponse(upstreamResp.status, 'Upstream error')));
        return;
      }

      var successLatency = Date.now() - fetchStartTime;
      rlog('← ' + C.green + '200 OK' + C.reset + ' | latency=' + successLatency + 'ms | account=' + C.dim + account.email + C.reset);
      var prefixedModel = modelMapper.addPrefix(universal.model);
      req._statsMeta.error_type = null;

      if (clientWantsStream) {
        var sseCtx = anthropic.createSSEContext({ model: prefixedModel });
        var parseState = codexResponses.createParseState();
        var totalUsage = { input_tokens: 0, output_tokens: 0 };
        var streamStartTime = Date.now();
        var chunksSent = 0;
        var streamFailed = false;
        var streamErrorMessage = '';
        var streamErrorType = '';

        try {
          await parseSSEStream(upstreamResp.body, function (eventType, data) {
            if (!data && eventType === 'done') return;
            if (eventType === 'parse_error') {
              streamFailed = true;
              streamErrorType = 'stream_parse_error';
              streamErrorMessage = (data && data.error) || 'stream_parse_error';
              return;
            }
            var event = codexResponses.parseSSEEvent(eventType, data, parseState);
            if (!event) return;
            var events = Array.isArray(event) ? event : [event];
            for (var ei = 0; ei < events.length; ei++) {
              var parsedEvent = events[ei];
              if (!parsedEvent) continue;
              if (parsedEvent.type === 'error') {
                streamFailed = true;
                streamErrorType = (parsedEvent.error && parsedEvent.error.code) || (parsedEvent.error && parsedEvent.error.type) || 'upstream_stream_error';
                streamErrorMessage = (parsedEvent.error && parsedEvent.error.message) || 'upstream_stream_error';
                continue;
              }
              if (parsedEvent.type === 'done' && parsedEvent.usage) {
                totalUsage = parsedEvent.usage;
              }
              var chunk = anthropic.formatSSEChunk(parsedEvent, sseCtx);
              if (!chunk || res.writableEnded) continue;
              if (!responseStarted) {
                if (res.headersSent) break;
                res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
                responseStarted = true;
              }
              res.write(chunk);
              chunksSent++;
            }
          }, {
            firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
            idleTimeoutMs: retryPolicy.idle_timeout_ms,
          });
        } catch (streamErr) {
          streamFailed = true;
          streamErrorType = isTimeoutError(streamErr) ? 'upstream_timeout' : 'network_error';
          streamErrorMessage = streamErr.message || streamErrorType;
        }

        if (streamFailed) {
          ctx.pool.markError(account.email, 0, streamErrorMessage || streamErrorType || 'upstream_stream_error');
          req._statsMeta.error_type = streamErrorType || 'upstream_stream_error';
          var retryableStream = isRetryableError({
            status: streamErrorType === 'upstream_timeout' ? 504 : 502,
            error_type: streamErrorType || 'upstream_stream_error',
            message: streamErrorMessage || 'upstream_stream_error',
            has_sent_data: responseStarted || chunksSent > 0,
          });
          if (retryableStream && !responseStarted && attempt < maxRetries && planRetry(streamErrorType || 'upstream_stream_error', account, 'message=' + (streamErrorMessage || ''))) {
            continue;
          }
          if (!responseStarted && !res.headersSent) {
            res.writeHead(streamErrorType === 'upstream_timeout' ? 504 : 502, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(anthropic.formatErrorResponse(streamErrorType === 'upstream_timeout' ? 504 : 502, streamErrorMessage || 'Upstream stream error')));
          } else if (!res.writableEnded) {
            res.end();
          }
          return;
        }

        if (chunksSent === 0 && isUsageEmpty(totalUsage)) {
          ctx.pool.markError(account.email, 0, 'upstream_empty_stream');
          req._statsMeta.error_type = 'empty_response';
          if (attempt < maxRetries && planRetry('empty_response', account, 'message=upstream_empty_stream')) {
            continue;
          }
          if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'application/json' });
          if (!res.writableEnded) res.end(JSON.stringify(anthropic.formatErrorResponse(502, 'Upstream returned empty stream')));
          return;
        }

        ctx.pool.markSuccess(account.email, totalUsage);
        if (req._statsMeta) req._statsMeta.usage = totalUsage;
        var streamLatency = Date.now() - streamStartTime;
        rlog(C.green + '✓ done' + C.reset + ' | model=' + prefixedModel + ' | tokens: ' + (totalUsage.input_tokens || 0) + '→' + (totalUsage.output_tokens || 0) + ' | latency=' + streamLatency + 'ms');
        if (responseStarted && !res.writableEnded) res.end();
        return;
      }

      var collector = createStreamCollector();
      var parseState2 = codexResponses.createParseState();
      var collectStartTime = Date.now();
      var nonStreamError = null;
      await parseSSEStream(upstreamResp.body, function (eventType, data) {
        if (!data && eventType === 'done') return;
        if (eventType === 'parse_error') {
          nonStreamError = { type: 'stream_parse_error', message: (data && data.error) || 'stream_parse_error' };
          return;
        }
        var event = codexResponses.parseSSEEvent(eventType, data, parseState2);
        if (!event) return;
        var events = Array.isArray(event) ? event : [event];
        for (var ei = 0; ei < events.length; ei++) {
          if (events[ei] && events[ei].type === 'error') {
            nonStreamError = {
              type: (events[ei].error && (events[ei].error.code || events[ei].error.type)) || 'upstream_stream_error',
              message: (events[ei].error && events[ei].error.message) || 'upstream_stream_error',
            };
            continue;
          }
          if (events[ei]) collector.push(events[ei]);
        }
      }, {
        firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
        idleTimeoutMs: retryPolicy.idle_timeout_ms,
      });

      var resp = collector.toResponse();
      resp.model = prefixedModel;

      if (!nonStreamError && !hasUsableResponse(resp) && isUsageEmpty(resp.usage)) {
        nonStreamError = { type: 'empty_response', message: 'upstream_empty_response' };
      }

      if (nonStreamError) {
        ctx.pool.markError(account.email, 0, nonStreamError.message || nonStreamError.type || 'upstream_stream_error');
        req._statsMeta.error_type = nonStreamError.type || 'upstream_stream_error';
        var retryableNonStream = isRetryableError({
          status: nonStreamError.type === 'upstream_timeout' ? 504 : 502,
          error_type: nonStreamError.type || 'upstream_stream_error',
          message: nonStreamError.message || 'upstream_stream_error',
          has_sent_data: false,
        });
        if (retryableNonStream && attempt < maxRetries && planRetry(nonStreamError.type || 'upstream_stream_error', account, 'message=' + (nonStreamError.message || ''))) {
          continue;
        }
        res.writeHead(nonStreamError.type === 'upstream_timeout' ? 504 : 502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(anthropic.formatErrorResponse(nonStreamError.type === 'upstream_timeout' ? 504 : 502, nonStreamError.message || 'Upstream error')));
        return;
      }

      var jsonResp = anthropic.formatNonStreamResponse(resp);
      ctx.pool.markSuccess(account.email, resp.usage);
      if (req._statsMeta) req._statsMeta.usage = resp.usage;
      var collectLatency = Date.now() - collectStartTime;
      rlog(C.green + '✓ done (non-stream)' + C.reset + ' | model=' + prefixedModel + ' | tokens: ' + ((resp.usage && resp.usage.input_tokens) || 0) + '→' + ((resp.usage && resp.usage.output_tokens) || 0) + ' | latency=' + collectLatency + 'ms');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(jsonResp));
      return;
    } catch (err) {
      var timeoutFlag = isTimeoutError(err);
      var errorType = timeoutFlag ? 'upstream_timeout' : 'network_error';
      rlog(C.red + '✗ ' + errorType + ': ' + C.reset + err.message + ' | account=' + C.dim + account.email + C.reset + ' | attempt=' + attempt + '/' + maxRetries);
      ctx.pool.markError(account.email, 0, err.message);
      req._statsMeta.error_type = errorType;
      var retryableNetwork = isRetryableError({
        status: timeoutFlag ? 504 : 502,
        error_type: errorType,
        message: err.message || errorType,
        timeout: timeoutFlag,
        network: isNetworkError(err),
        error: err,
        has_sent_data: responseStarted,
      });
      if (retryableNetwork && !responseStarted && attempt < maxRetries && planRetry(errorType, account, err.code ? ('code=' + err.code) : '')) {
        continue;
      }
      if (!res.headersSent) {
        res.writeHead(timeoutFlag ? 504 : 502, { 'Content-Type': 'application/json' });
      }
      if (!res.writableEnded) {
        res.end(JSON.stringify(anthropic.formatErrorResponse(timeoutFlag ? 504 : 502, err.message)));
      }
      return;
    }
  }

  req._statsMeta.error_type = 'no_account';
  if (!res.headersSent) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
  }
  if (!res.writableEnded) res.end(JSON.stringify(anthropic.formatErrorResponse(503, ctx.t('scheduler.no_available'))));
}
