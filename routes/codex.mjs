/**
 * Codex CLI 透传路由
 *
 * 路径:
 *   POST /backend-api/codex/responses         → 透传
 *   POST /backend-api/codex/responses/compact  → 透传
 *
 * 只做: 账号选择 + token 注入 + 错误处理，不做格式转换
 *
 */

import * as codexResponses from '../lib/converter/openai-responses.mjs';
import { normalizeCollectedUsage } from '../lib/converter/openai-responses.mjs';
import * as modelMapper from '../lib/converter/model-mapper.mjs';
import {
  readBody,
  derivePromptCacheSessionId,
  resolveRetryPolicy,
  isRetryableError,
  pickRetryAccount,
  isTimeoutError,
  isNetworkError,
  createTimeoutError,
} from '../lib/http-utils.mjs';
import { authenticateApiKey } from '../lib/api-key-auth.mjs';
import { C, timestamp } from '../lib/utils.mjs';
import { parseSSEStream } from '../lib/converter/stream/sse-parser.mjs';

// 路由日志辅助
var TAG = C.magenta + '[codex]' + C.reset;
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
function isWebSearchRelevantSSEEvent(eventType, dataOrRaw) {
  var type = eventType || '';
  if (type.indexOf('response.web_search_call.') === 0) return true;
  if (type === 'response.output_item.added' || type === 'response.output_item.done') return true;
  if (type === 'response.function_call_arguments.delta' || type === 'response.function_call_arguments.done') return true;
  var raw = '';
  if (typeof dataOrRaw === 'string') {
    raw = dataOrRaw;
  } else {
    raw = safeStringify(dataOrRaw);
  }
  if (raw.indexOf('"web_search"') !== -1 || raw.indexOf('"web_search_call"') !== -1 || raw.indexOf('"response.web_search_call.') !== -1) {
    return true;
  }
  return false;
}
async function logRawSSEEvents(stream) {
  if (!stream || typeof stream.getReader !== 'function') return;
  var reader = stream.getReader();
  var decoder = new TextDecoder();
  var buffer = '';
  var currentEvent = '';
  var currentData = '';

  function flushCurrentEvent() {
    if (!currentEvent && !currentData) return;
    if (currentData === '[DONE]') {
      currentEvent = '';
      currentData = '';
      return;
    }
    if (isWebSearchRelevantSSEEvent(currentEvent, currentData)) {
      rlog('[WEB-SEARCH] upstream raw SSE event=' + (currentEvent || '(none)') + ' data=' + currentData);
    }
    currentEvent = '';
    currentData = '';
  }

  function consumeLine(line) {
    if (line.indexOf('event:') === 0) {
      currentEvent = line.substring(6).trim();
      return;
    }
    if (line.indexOf('data:') === 0) {
      var piece = line.substring(5).trim();
      if (currentData) currentData += '\n' + piece;
      else currentData = piece;
      return;
    }
    if (line === '' || line === '\r') {
      flushCurrentEvent();
    }
  }

  try {
    while (true) {
      var chunk = await reader.read();
      if (chunk.done) break;
      buffer += decoder.decode(chunk.value, { stream: true });
      var lines = buffer.split('\n');
      buffer = lines.pop();
      for (var i = 0; i < lines.length; i++) {
        consumeLine(lines[i]);
      }
    }
    var tail = decoder.decode();
    if (tail) buffer += tail;
    if (buffer) {
      var tailLines = buffer.split('\n');
      for (var t = 0; t < tailLines.length; t++) {
        consumeLine(tailLines[t]);
      }
    }
    if (currentData || currentEvent) flushCurrentEvent();
  } finally {
    reader.releaseLock();
  }
}
function statusColor(code) {
  if (code >= 200 && code < 300) return C.green;
  if (code >= 400 && code < 500) return C.yellow;
  return C.red;
}

/**
 * 注册 Codex 透传路由
 */
export function createCodexRoutes(ctx) {
  return async function (req, res) {
    var path = req.url.split('?')[0];

    if (req.method === 'POST' && (
      path === '/backend-api/codex/responses' ||
      path === '/backend-api/codex/responses/compact'
    )) {
      return handleCodexPassthrough(req, res, ctx, path);
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  };
}

/**
 * Codex 透传处理
 */
async function handleCodexPassthrough(req, res, ctx, path) {
  // 认证检查
  var auth = authenticateApiKey(req, ctx.config);
  if (!auth.ok) {
    res.writeHead(auth.status || 401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Unauthorized' } }));
    return;
  }
  req._apiKeyIdentity = auth.identity;

  var body;
  try {
    body = await readBody(req);
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid request body' }));
    return;
  }

  // 记住下游是否要流式（适配用，即使上游不接受 stream 参数）
  var clientWantsStream = body.stream !== false;

  // 内容协商: 如果客户端明确只接受 JSON，强制非流式
  var acceptHeader = req.headers['accept'] || '*/*';
  if (clientWantsStream && acceptHeader.indexOf('text/event-stream') < 0 && acceptHeader.indexOf('*/*') < 0 && acceptHeader.indexOf('*') < 0) {
    rlog('⚠ client Accept=' + acceptHeader + ' → force non-stream');
    clientWantsStream = false;
  }

  // 请求进入日志
  rlog('POST ' + path + ' | model=' + C.bold + (body.model || 'unknown') + C.reset + ' | stream=' + clientWantsStream + ' (passthrough)');

  // 早期设置 _statsMeta，确保成功和失败请求都能被统计
  req._statsMeta = {
    route: 'codex',
    model: body.model || '',
    account: '',
    stream: clientWantsStream,
    usage: null,
    error_type: null,
    caller_identity: req._apiKeyIdentity || auth.identity || 'unknown',
  };
  var requestStartTime = Date.now();

  // 模型名映射（去掉 codex/ 前缀，解析 aliases）
  if (body.model) {
    var mapped = modelMapper.resolveModel(body.model);
    body.model = mapped.resolved;
    req._statsMeta.model = body.model || req._statsMeta.model;
    if (mapped.reasoningEffort) {
      if (!body.reasoning || typeof body.reasoning !== 'object' || Array.isArray(body.reasoning)) {
        body.reasoning = {};
      }
      body.reasoning.effort = mapped.reasoningEffort;
    }
  }

  // 生成 session_id
  var clientSessionId = req.headers['session_id'] || req.headers['x-session-id'] || req.headers['x-codex-session-id'] || '';
  var sessionId = clientSessionId;
  if (!sessionId && ctx.config.prompt_cache && ctx.config.prompt_cache.enabled) {
    // 统一派生逻辑：不绑定 account，避免换号后 cache key 抖动。
    sessionId = derivePromptCacheSessionId({
      callerIdentity: (req._statsMeta && req._statsMeta.caller_identity) || 'unknown',
      route: 'codex',
      model: body.model || '',
      promptCacheKey: body.prompt_cache_key,
      user: body.user,
    });
  }

  // 注入 token，透传请求
  var headers = codexResponses.formatHeaders(account.accessToken, account.accountId, sessionId);
  var upstreamUrl = (ctx.config.upstream && ctx.config.upstream.base_url || 'https://chatgpt.com/backend-api')
    + path.replace('/backend-api', '');

  // 等等，path 已经包含 /backend-api 前缀
  // 上游 URL 是 base_url + '/codex/responses'
  upstreamUrl = (ctx.config.upstream && ctx.config.upstream.base_url || 'https://chatgpt.com/backend-api')
    + path.substring('/backend-api'.length);

  if (ctx.config.prompt_cache && ctx.config.prompt_cache.enabled) {
    if (!body.prompt_cache_key && sessionId) body.prompt_cache_key = sessionId;
    if (!body.prompt_cache_retention && ctx.config.prompt_cache.default_retention) {
      body.prompt_cache_retention = ctx.config.prompt_cache.default_retention;
    }
  }

  // 过滤上游不支持的采样参数
  delete body.temperature;
  delete body.max_output_tokens;
  delete body.max_tokens;
  delete body.top_p;
  delete body.frequency_penalty;
  delete body.presence_penalty;
  delete body.stop;
  delete body.logit_bias;
  delete body.n;

  // 适配 tool_call ID 和 tool schema（透传也需要规范化）
  var isCompact = path.endsWith('/compact');
  // 上游 /codex/responses 强制要求 stream=true；compact 不接受 stream 字段
  if (!isCompact) {
    body.stream = true;
  } else {
    delete body.stream;
  }
  codexResponses.adaptResponsesBody(body, isCompact);
  var requestMeta = {
    model: body && body.model ? String(body.model) : '',
    stream: body && body.stream === true,
    field_count: body && typeof body === 'object' ? Object.keys(body).length : 0,
    tools_count: Array.isArray(body && body.tools) ? body.tools.length : 0,
    input_items: Array.isArray(body && body.input) ? body.input.length : (body && body.input ? 1 : 0),
  };
  rlog('[WEB-SEARCH] upstream request meta=' + safeStringify(requestMeta));

  var retryPolicy = resolveRetryPolicy(ctx.config, clientWantsStream);
  var maxRetries = retryPolicy.max_retries;
  var attempt = 0;
  var triedAccounts = [];
  var nextRetryAccount = null;
  var responseStarted = false;

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

  async function readChunkWithTimeout(reader, timeoutMs, kind) {
    if (!timeoutMs || timeoutMs <= 0) {
      return reader.read();
    }
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

  while (attempt < maxRetries) {
    attempt++;
    var account = acquireRetryAccount();
    if (!account) break;
    req._statsMeta.account = account.email;
    var headers = codexResponses.formatHeaders(account.accessToken, account.accountId, sessionId);
    rlog('→ upstream POST ' + C.dim + upstreamUrl + C.reset + ' | model=' + (body.model || 'unknown') + ' | attempt=' + attempt + '/' + maxRetries + ' | account=' + C.dim + account.email + C.reset);

    try {
      var fetchStartTime = Date.now();
      var upstreamResp = await fetch(upstreamUrl, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(retryPolicy.total_timeout_ms),
      });

      if (!upstreamResp.ok) {
        var errLatency = Date.now() - fetchStartTime;
        var errBody = await upstreamResp.text().catch(function () { return ''; });
        rlog('← ' + statusColor(upstreamResp.status) + upstreamResp.status + ' ERROR' + C.reset + ' | latency=' + errLatency + 'ms | account=' + C.dim + account.email + C.reset + ' | body: ' + (errBody || '(empty)').substring(0, 200));
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
        sendErrorResponse(res, responseStarted, upstreamResp.status >= 500 ? 502 : upstreamResp.status, 'upstream_' + upstreamResp.status, 'Upstream error (' + upstreamResp.status + '). Retries exhausted.');
        return;
      }

      var successLatency = Date.now() - fetchStartTime;
      var upstreamContentType = upstreamResp.headers.get('content-type') || '';
      rlog('← ' + C.green + '200 OK' + C.reset + ' | latency=' + successLatency + 'ms | content-type=' + upstreamContentType + ' | account=' + C.dim + account.email + C.reset);
      req._statsMeta.error_type = null;
      responseStarted = false;
      var usageData = null;

      var isExplicitJSON = upstreamContentType.indexOf('application/json') !== -1;
      var isUpstreamSSE = !isExplicitJSON;

      if (isUpstreamSSE) {
        if (!clientWantsStream) {
          var collected = await codexResponses.collectNonStreamResponseFromSSE(upstreamResp.body, {
            firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
            idleTimeoutMs: retryPolicy.idle_timeout_ms,
          });
          var collectError = collected && collected.error ? collected.error : 'invalid_sse_response';
          var collectFailed = !collected || !collected.success || !collected.response;
          if (collectFailed) {
            ctx.pool.markError(account.email, 502, collectError);
            req._statsMeta.error_type = 'upstream_sse_parse_failed';
            var retryableCollect = isRetryableError({
              status: 502,
              error_type: 'upstream_sse_parse_failed',
              message: collectError,
              has_sent_data: false,
            });
            if (retryableCollect && attempt < maxRetries && planRetry('upstream_sse_parse_failed', account, 'error=' + collectError)) {
              continue;
            }
            sendErrorResponse(res, false, 502, 'upstream_sse_parse_failed', 'Invalid upstream SSE response: ' + collectError);
            return;
          }
          usageData = collected.usage || null;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          responseStarted = true;
          res.end(JSON.stringify(collected.response));
        } else {
          if (upstreamResp.body && typeof upstreamResp.body.tee === 'function') {
            var tee = upstreamResp.body.tee();
            var streamToClient = tee[0];
            var streamToParse = tee[1];
            var streamToRawLog = null;
            if (streamToParse && typeof streamToParse.tee === 'function') {
              var parseTee = streamToParse.tee();
              streamToParse = parseTee[0];
              streamToRawLog = parseTee[1];
            }
            var parseState = codexResponses.createParseState();
            var sawClientChunk = false;

            var forwardPromise = (async function () {
              var reader = streamToClient.getReader();
              try {
                while (true) {
                  var timeoutForRead = sawClientChunk ? retryPolicy.idle_timeout_ms : retryPolicy.first_byte_timeout_ms;
                  var result = await readChunkWithTimeout(reader, timeoutForRead, sawClientChunk ? 'idle' : 'first_byte');
                  if (result.done) break;
                  if (!sawClientChunk) {
                    if (res.headersSent) { break; }
                    res.writeHead(200, {
                      'Content-Type': 'text/event-stream',
                      'Cache-Control': 'no-cache',
                      'Connection': 'keep-alive',
                    });
                    responseStarted = true;
                    sawClientChunk = true;
                  }
                  if (!res.writableEnded) res.write(result.value);
                }
              } finally {
                reader.releaseLock();
              }
            })();

            var parsePromise = parseSSEStream(streamToParse, function (eventType, data) {
              if (!data && eventType === 'done') return;
              if (isWebSearchRelevantSSEEvent(eventType, data)) {
                rlog('[WEB-SEARCH] upstream parsed SSE event=' + eventType + ' data=' + safeStringify(data));
              }
              var universalEvent = codexResponses.parseSSEEvent(eventType, data, parseState);
              if (!universalEvent) return;
              var universalEvents = Array.isArray(universalEvent) ? universalEvent : [universalEvent];
              for (var ue = 0; ue < universalEvents.length; ue++) {
                var evt = universalEvents[ue];
                if (evt && evt.usage) usageData = evt.usage;
              }
            }, {
              firstByteTimeoutMs: retryPolicy.first_byte_timeout_ms,
              idleTimeoutMs: retryPolicy.idle_timeout_ms,
            });
            var rawLogPromise = streamToRawLog ? logRawSSEEvents(streamToRawLog) : Promise.resolve();

            var tasks = await Promise.allSettled([forwardPromise, parsePromise, rawLogPromise]);
            if (tasks[0].status === 'rejected') {
              var streamErr = tasks[0].reason;
              var timeoutForward = isTimeoutError(streamErr);
              var forwardCode = timeoutForward ? 'upstream_timeout' : 'stream_interrupted';
              var forwardMessage = (streamErr && streamErr.message) || forwardCode;
              rlog(C.red + '✗ stream forward error: ' + C.reset + forwardMessage + ' | account=' + C.dim + account.email + C.reset);
              ctx.pool.markError(account.email, 0, forwardMessage);
              req._statsMeta.error_type = forwardCode;
              var retryableForward = isRetryableError({
                status: timeoutForward ? 504 : 502,
                error_type: forwardCode,
                message: forwardMessage,
                timeout: timeoutForward,
                network: isNetworkError(streamErr),
                error: streamErr,
                has_sent_data: responseStarted,
              });
              if (retryableForward && !responseStarted && attempt < maxRetries && planRetry(forwardCode, account, 'message=' + forwardMessage)) {
                continue;
              }
              sendErrorResponse(res, responseStarted, timeoutForward ? 504 : 502, forwardCode, 'Upstream stream interrupted. Please retry.');
              return;
            }
            if (!sawClientChunk) {
              var emptyStreamError = 'upstream_empty_stream';
              ctx.pool.markError(account.email, 0, emptyStreamError);
              req._statsMeta.error_type = 'empty_response';
              if (attempt < maxRetries && planRetry('empty_response', account, 'message=' + emptyStreamError)) {
                continue;
              }
              sendErrorResponse(res, false, 502, 'empty_response', 'Upstream returned empty stream.');
              return;
            }
            if (tasks[1].status === 'rejected') {
              var parseErr = tasks[1].reason;
              var timeoutParse = isTimeoutError(parseErr);
              var parseCode = timeoutParse ? 'upstream_timeout' : 'stream_parse_error';
              var parseMessage = (parseErr && parseErr.message) || parseCode;
              rlog(C.yellow + '⚠ usage parse failed: ' + C.reset + parseMessage);
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
            if (tasks[2].status === 'rejected') {
              rlog(C.yellow + '⚠ raw SSE log failed: ' + C.reset + (tasks[2].reason && tasks[2].reason.message ? tasks[2].reason.message : String(tasks[2].reason)));
            }
            if (!res.writableEnded) res.end();
          } else {
            var readerNoTee = upstreamResp.body.getReader();
            var sawNoTeeChunk = false;
            try {
              while (true) {
                var timeoutNoTee = sawNoTeeChunk ? retryPolicy.idle_timeout_ms : retryPolicy.first_byte_timeout_ms;
                var noTeeChunk = await readChunkWithTimeout(readerNoTee, timeoutNoTee, sawNoTeeChunk ? 'idle' : 'first_byte');
                if (noTeeChunk.done) break;
                if (!sawNoTeeChunk) {
                  res.writeHead(200, {
                    'Content-Type': 'text/event-stream',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                  });
                  responseStarted = true;
                  sawNoTeeChunk = true;
                }
                if (!res.writableEnded) res.write(noTeeChunk.value);
              }
            } catch (noTeeErr) {
              var timeoutNoTeeErr = isTimeoutError(noTeeErr);
              var noTeeCode = timeoutNoTeeErr ? 'upstream_timeout' : 'stream_interrupted';
              var noTeeMsg = (noTeeErr && noTeeErr.message) || noTeeCode;
              rlog(C.red + '✗ stream read error: ' + C.reset + noTeeMsg + ' | account=' + C.dim + account.email + C.reset);
              ctx.pool.markError(account.email, 0, noTeeMsg);
              req._statsMeta.error_type = noTeeCode;
              if (!responseStarted && attempt < maxRetries && isRetryableError({
                status: timeoutNoTeeErr ? 504 : 502,
                error_type: noTeeCode,
                message: noTeeMsg,
                timeout: timeoutNoTeeErr,
                network: isNetworkError(noTeeErr),
                error: noTeeErr,
                has_sent_data: false,
              }) && planRetry(noTeeCode, account, 'message=' + noTeeMsg)) {
                continue;
              }
              sendErrorResponse(res, responseStarted, timeoutNoTeeErr ? 504 : 502, noTeeCode, 'Upstream stream interrupted. Please retry.');
              return;
            } finally {
              readerNoTee.releaseLock();
            }
            if (!sawNoTeeChunk) {
              ctx.pool.markError(account.email, 0, 'upstream_empty_stream');
              req._statsMeta.error_type = 'empty_response';
              if (attempt < maxRetries && planRetry('empty_response', account, 'message=upstream_empty_stream')) {
                continue;
              }
              sendErrorResponse(res, false, 502, 'empty_response', 'Upstream returned empty stream.');
              return;
            }
            if (!res.writableEnded) res.end();
          }
        }
      } else {
        var jsonBody = await upstreamResp.text();
        usageData = extractResponsesUsageFromBody(jsonBody);
        if (!jsonBody || !jsonBody.trim()) {
          ctx.pool.markError(account.email, 0, 'upstream_empty_response');
          req._statsMeta.error_type = 'empty_response';
          if (attempt < maxRetries && planRetry('empty_response', account, 'message=upstream_empty_response')) {
            continue;
          }
          sendErrorResponse(res, false, 502, 'empty_response', 'Upstream returned empty response body.');
          return;
        }
        if (clientWantsStream) {
          res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
          });
          responseStarted = true;
          res.write('data: ' + jsonBody.trim() + '\n\n');
          res.write('data: [DONE]\n\n');
          res.end();
        } else {
          res.writeHead(200, {
            'Content-Type': upstreamContentType || 'application/json',
          });
          responseStarted = true;
          res.end(jsonBody);
        }
      }

      if (usageData) {
        req._statsMeta.usage = usageData;
        ctx.pool.markSuccess(account.email, usageData);
      } else {
        ctx.pool.markSuccess(account.email, {});
      }
      var totalLatency = Date.now() - requestStartTime;
      rlog(C.green + '✓ done (passthrough)' + C.reset + ' | model=' + (body.model || 'unknown') + ' | tokens=' + ((usageData && usageData.input_tokens) || 0) + '→' + ((usageData && usageData.output_tokens) || 0) + ' | latency=' + totalLatency + 'ms');
      return;

    } catch (err) {
      var timeoutErr = isTimeoutError(err);
      var netCode = timeoutErr ? 'upstream_timeout' : 'network_error';
      rlog(C.red + '✗ ' + netCode + ': ' + C.reset + err.message + ' | account=' + C.dim + account.email + C.reset + ' | attempt=' + attempt + '/' + maxRetries);
      ctx.pool.markError(account.email, 0, err.message);
      req._statsMeta.error_type = netCode;

      var retryableNetwork = isRetryableError({
        status: timeoutErr ? 504 : 502,
        error_type: netCode,
        message: err.message,
        timeout: timeoutErr,
        network: isNetworkError(err),
        error: err,
        has_sent_data: responseStarted,
      });
      if (retryableNetwork && !responseStarted && attempt < maxRetries && planRetry(netCode, account, err.code ? ('code=' + err.code) : '')) {
        continue;
      }
      sendErrorResponse(res, responseStarted, timeoutErr ? 504 : 502, netCode, (timeoutErr ? 'Upstream timeout: ' : 'Network error: ') + err.message);
      return;
    }
  }

  req._statsMeta.error_type = 'no_account';
  sendErrorResponse(res, responseStarted, 503, 'no_available_account', 'No available account. All retries exhausted.');
}

/**
 * 统一错误响应 — 根据 headersSent 决定发 JSON 还是 SSE 错误事件
 */
function sendErrorResponse(res, headersSent, statusCode, errorCode, message) {
  if (res.writableEnded) return;
  var errPayload = JSON.stringify({
    type: 'error',
    error: { type: 'upstream_error', code: errorCode, message: message },
  });

  if (headersSent || res.headersSent) {
    if (!res.writableEnded) {
      try { res.write('event: error\ndata: ' + errPayload + '\n\n'); } catch (_) {}
      try { res.end(); } catch (_) {}
    }
  } else {
    try {
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(errPayload);
    } catch (e) {
      if (!res.writableEnded) try { res.end(); } catch (_) {}
    }
  }
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
