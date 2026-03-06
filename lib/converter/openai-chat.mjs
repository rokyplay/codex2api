/**
 * OpenAI Chat Completions 客户端适配器
 *
 * 客户端路径: POST /v1/chat/completions, GET /v1/models
 * 格式: OpenAI Chat Completions API (标准)
 *
 * 职责:
 *   1. 客户端请求 → Universal（parseRequest）
 *   2. Universal 流式事件 → OpenAI SSE（formatSSEChunk）
 *   3. Universal 完整响应 → 非流式 JSON（formatNonStreamResponse）
 *   4. /v1/models 响应格式（formatModelsResponse）
 *
 */

import { createRequest } from './universal.mjs';
import { timestamp } from '../utils.mjs';

var WEB_SEARCH_TAG = '[WEB-SEARCH][openai-chat]';
function wsLog(message) {
  console.log('[' + timestamp() + '] ' + WEB_SEARCH_TAG + ' ' + message);
}
function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return '"[unserializable:' + (err && err.message ? err.message : 'unknown') + ']"';
  }
}

/**
 * OpenAI Chat Completions 请求 → Universal
 *
 *   - messages: 含 system/developer/user/assistant/tool 所有角色
 *   - model, stream, temperature, top_p, max_tokens/max_completion_tokens
 *   - stop, tools, tool_choice, response_format
 *   - frequency_penalty, presence_penalty, logprobs, top_logprobs, logit_bias
 *   - seed, n, user, reasoning_effort
 *   - stream_options (include_usage)
 *   - parallel_tool_calls, service_tier
 *
 * @param {object} body - 客户端请求体
 * @returns {UniversalRequest}
 */
export function parseRequest(body) {
  var system = '';
  var messages = [];
  var incomingTools = Array.isArray(body.tools) ? body.tools : [];
  wsLog('parseRequest incoming tools=' + safeStringify(incomingTools));
  wsLog('parseRequest incoming web_search_options=' + safeStringify(body.web_search_options || null));
  var incomingWebSearchTools = [];
  for (var it = 0; it < incomingTools.length; it++) {
    if (isWebSearchRelatedClientTool(incomingTools[it])) {
      incomingWebSearchTools.push(incomingTools[it]);
    }
  }
  if (incomingWebSearchTools.length > 0) {
    wsLog('parseRequest incoming web_search-related tools=' + safeStringify(incomingWebSearchTools));
  }

  var rawMessages = body.messages || [];
  for (var i = 0; i < rawMessages.length; i++) {
    var msg = rawMessages[i];
    if (!msg || typeof msg !== 'object') continue;

    // 提取 system → universal.system
    if (msg.role === 'system') {
      system += (system ? '\n' : '') + extractTextContent(msg.content);
      continue;
    }

    // developer 消息保留为独立消息，按 Codex 风格进入 input(role=developer)
    if (msg.role === 'developer') {
      messages.push({
        role: 'developer',
        content: msg.content,
      });
      continue;
    }

    // tool 结果消息 — 保留原始内容不强制 extractTextContent
    if (msg.role === 'tool') {
      var toolContent = '';
      if (typeof msg.content === 'string') {
        toolContent = msg.content;
      } else if (msg.content !== undefined && msg.content !== null) {
        toolContent = JSON.stringify(msg.content);
      }
      messages.push({
        role: 'tool',
        content: toolContent,
        tool_call_id: (msg.tool_call_id !== undefined && msg.tool_call_id !== null) ? String(msg.tool_call_id) : '',
      });
      continue;
    }

    // assistant 带 tool_calls — 标准化 tool_calls 格式
    if (msg.role === 'assistant' && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
      var normalizedToolCalls = [];
      for (var tc = 0; tc < msg.tool_calls.length; tc++) {
        var call = msg.tool_calls[tc] || {};
        var fnArgs = (call.function && call.function.arguments);
        var normalizedArgs = '{}';
        if (typeof fnArgs === 'string' && fnArgs) {
          normalizedArgs = fnArgs;
        } else if (fnArgs && typeof fnArgs === 'object') {
          try {
            normalizedArgs = JSON.stringify(fnArgs);
          } catch (_) {
            normalizedArgs = '{}';
          }
        } else if (fnArgs !== undefined && fnArgs !== null) {
          normalizedArgs = String(fnArgs);
        }
        normalizedToolCalls.push({
          id: call.id ? String(call.id) : ('call_msg' + i + '_tool' + tc),
          type: call.type || 'function',
          function: {
            name: (call.function && call.function.name) || '',
            arguments: normalizedArgs,
          },
        });
      }
      var assistantToolMsg = {
        role: 'assistant',
        content: extractTextContent(msg.content),
        tool_calls: normalizedToolCalls,
      };
      // assistant + tool_calls 也可能附带 reasoning_content，需透传
      if (msg.reasoning_content) {
        assistantToolMsg.reasoning_content = msg.reasoning_content;
      }
      messages.push(assistantToolMsg);
      continue;
    }

    // assistant 普通消息 — 可能有 reasoning_content
    if (msg.role === 'assistant') {
      var assistantMsg = {
        role: 'assistant',
        content: extractTextContent(msg.content),
      };
      // 保留 reasoning_content（o1/o3 模型的推理内容）
      if (msg.reasoning_content) {
        assistantMsg.reasoning_content = msg.reasoning_content;
      }
      messages.push(assistantMsg);
      continue;
    }

    // user 消息 — 保留原始格式（string 或 content blocks，含 image_url/input_audio 等）
    messages.push({
      role: msg.role || 'user',
      content: msg.content,
    });
  }

  // 兼容 OpenAI Chat 的 web_search_options 写法:
  // 允许与其它 tools 共存，若当前 tools 中还没有 web_search 再注入。
  var requestTools = Array.isArray(body.tools) ? body.tools.slice() : [];
  var webSearchTool = buildWebSearchToolFromChatOptions(body.web_search_options);
  if (webSearchTool && !hasWebSearchTool(requestTools)) {
    requestTools.push(webSearchTool);
  }
  wsLog('parseRequest normalized tools=' + safeStringify(requestTools));

  return createRequest({
    model: body.model || '',
    system: system,
    messages: messages,
    stream: body.stream === true,
    temperature: body.temperature,
    max_tokens: body.max_tokens || body.max_completion_tokens,
    top_p: body.top_p,
    stop: body.stop,
    tools: requestTools,
    tool_choice: body.tool_choice || 'auto',
    metadata: {
      user: body.user,
      // 兼容 Chat 与 Responses 风格：reasoning_effort 或 reasoning.effort
      reasoning_effort: body.reasoning_effort || (body.reasoning && body.reasoning.effort),
      reasoning_summary: body.reasoning && body.reasoning.summary,
      // 兼容 top-level verbosity 与 text.verbosity
      text_verbosity: body.verbosity || (body.text && body.text.verbosity),
      text_format: body.text && body.text.format,
      // 以下字段透传给上游，按需使用
      frequency_penalty: body.frequency_penalty,
      presence_penalty: body.presence_penalty,
      logprobs: body.logprobs,
      top_logprobs: body.top_logprobs,
      logit_bias: body.logit_bias,
      seed: body.seed,
      n: body.n,
      response_format: body.response_format,
      stream_options: body.stream_options,
      parallel_tool_calls: body.parallel_tool_calls,
      service_tier: body.service_tier,
      // 关键修复: 透传 prompt cache 字段，避免 openai 路由丢失客户端 cache key。
      prompt_cache_key: body.prompt_cache_key,
      prompt_cache_retention: body.prompt_cache_retention,
    },
  });
}

/**
 * Chat Completions `web_search_options` → built-in web_search tool
 */
function buildWebSearchToolFromChatOptions(webSearchOptions) {
  if (!webSearchOptions || typeof webSearchOptions !== 'object' || Array.isArray(webSearchOptions)) {
    return null;
  }
  var tool = { type: 'web_search', external_web_access: true };
  if (typeof webSearchOptions.external_web_access === 'boolean') {
    tool.external_web_access = webSearchOptions.external_web_access;
  }
  if (typeof webSearchOptions.search_context_size === 'string' && webSearchOptions.search_context_size) {
    tool.search_context_size = webSearchOptions.search_context_size;
  }
  if (webSearchOptions.user_location && typeof webSearchOptions.user_location === 'object' && !Array.isArray(webSearchOptions.user_location)) {
    tool.user_location = webSearchOptions.user_location;
  }
  wsLog('buildWebSearchToolFromChatOptions normalized=' + safeStringify(tool));
  return tool;
}

function normalizeWebSearchAlias(value) {
  if (!value || typeof value !== 'string') return '';
  var lower = value.toLowerCase();
  if (lower.indexOf('web_search') === 0) return 'web_search';
  return '';
}

function isWebSearchRelatedClientTool(tool) {
  if (!tool) return false;
  if (typeof tool === 'string') return normalizeWebSearchAlias(tool) === 'web_search';
  if (typeof tool !== 'object') return false;
  if (normalizeWebSearchAlias(tool.type) === 'web_search') return true;
  if (tool.type === 'function' && tool.function && normalizeWebSearchAlias(tool.function.name) === 'web_search') return true;
  if (normalizeWebSearchAlias(tool.name) === 'web_search') return true;
  return false;
}

function hasWebSearchTool(tools) {
  if (!Array.isArray(tools) || tools.length === 0) return false;
  for (var i = 0; i < tools.length; i++) {
    if (isWebSearchRelatedClientTool(tools[i])) return true;
  }
  return false;
}

/**
 * Universal 流式事件 → OpenAI Chat Completions SSE chunk
 *
 * 格式: data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","choices":[{"delta":{...}}]}
 *
 *
 * SSE chunk 完整结构:
 *   { id, object: "chat.completion.chunk", created, model, system_fingerprint?,
 *     choices: [{ index, delta: { role?, content?, reasoning_content?, tool_calls? },
 *                 logprobs?, finish_reason }],
 *     usage? }
 *
 * @param {UniversalStreamEvent} event
 * @param {object} ctx - { id, model, created, toolCallIndex }
 * @returns {string|null} SSE 行文本（含 data: 前缀和尾部换行）
 */
export function formatSSEChunk(event, ctx) {
  ctx = ctx || {};
  var id = ctx.id || 'chatcmpl-' + Date.now();
  var model = ctx.model || event.model || '';
  var created = ctx.created || Math.floor(Date.now() / 1000);
  if (!Array.isArray(ctx.annotations)) ctx.annotations = [];
  if (!ctx.annotationKeyMap) ctx.annotationKeyMap = {};
  if (typeof ctx.sentAnnotationCount !== 'number') ctx.sentAnnotationCount = 0;

  if (event.type === 'start') {
    // 更新 ctx 元数据
    // 不用上游 resp_xxx 覆盖 ctx.id，保持 chatcmpl-xxx 格式（OpenAI 规范要求同一 stream 所有 chunk 的 ID 一致）
    // 不要用上游 model 覆盖客户端请求的 model（修复模型映射 bug）
    return formatSSEData({
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: { role: 'assistant', content: '' },
        logprobs: null,
        finish_reason: null,
      }],
    });
  }

  // 文本增量
  if (event.type === 'delta') {
    var deltaContent = normalizeStreamText(event.content);
    if (!deltaContent) return null;
    ctx.sentContentLength = (ctx.sentContentLength || 0) + deltaContent.length;
    return formatSSEData({
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: { content: deltaContent },
        logprobs: null,
        finish_reason: null,
      }],
    });
  }

  // 用于 o1/o3/DeepSeek 等模型的推理链输出
  if (event.type === 'reasoning' && event.reasoning) {
    return formatSSEData({
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: { reasoning_content: event.reasoning },
        logprobs: null,
        finish_reason: null,
      }],
    });
  }

  // URL 引用等注解增量
  if (event.type === 'annotation' && event.annotation) {
    var annotation = normalizeAnnotation(event.annotation);
    if (!annotation) return null;
    if (!addUniqueAnnotationToContext(ctx, annotation)) return null;
    ctx.sentAnnotationCount = (ctx.sentAnnotationCount || 0) + 1;
    return formatSSEData({
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: { annotations: [annotation] },
        logprobs: null,
        finish_reason: null,
      }],
    });
  }

  // 流式 tool_calls 需要通过 index 追踪多个并行调用
  // 首次发送时包含 id+type+function.name，后续只发 function.arguments 增量
  if (event.type === 'tool_call' && event.tool_call) {
    var tc = event.tool_call;
    // 使用 ctx 中的 toolCallIndex 追踪当前工具调用索引
    var tcIndex = tc.index !== undefined ? tc.index : (ctx.toolCallIndex || 0);
    var toolDelta = {
      index: tcIndex,
    };

    // 首次发送（有 id 或 name）— 包含完整工具信息
    if (tc.id) toolDelta.id = tc.id;
    if (tc.id || tc.name) toolDelta.type = 'function';

    // done 事件：如果携带完整 arguments 则兜底发送（防止增量丢失时参数缺失）
    if (tc.done) {
      var doneChunk = null;
      if (tc.arguments) {
        // 发送完整参数作为兜底
        if (!toolDelta.type) toolDelta.type = 'function';
        toolDelta.function = { arguments: tc.arguments };
        if (tc.name && !toolDelta.function.name) toolDelta.function.name = tc.name;
        var donePayload = {
          id: id,
          object: 'chat.completion.chunk',
          created: created,
          model: model,
          choices: [{
            index: 0,
            delta: { tool_calls: [toolDelta] },
            logprobs: null,
            finish_reason: null,
          }],
        };
        wsLog('formatSSEChunk tool_call payload=' + safeStringify(donePayload));
        doneChunk = formatSSEData(donePayload);
      }
      ctx.toolCallIndex = (ctx.toolCallIndex || 0) + 1;
      return doneChunk;
    }

    // 只在有 name/arguments 时创建 function 对象
    if (tc.name || tc.arguments_delta || tc.arguments) {
      toolDelta.function = {};
      if (tc.name) toolDelta.function.name = tc.name;
      if (tc.arguments_delta) toolDelta.function.arguments = tc.arguments_delta;
      else if (tc.arguments) toolDelta.function.arguments = tc.arguments;
    }

    var toolCallPayload = {
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: { tool_calls: [toolDelta] },
        logprobs: null,
        finish_reason: null,
      }],
    };
    wsLog('formatSSEChunk tool_call payload=' + safeStringify(toolCallPayload));
    return formatSSEData(toolCallPayload);
  }

  // usage 独立事件（stream_options.include_usage = true 时发送）
  if (event.type === 'usage' && event.usage) {
    return formatSSEData({
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [],
      usage: {
        prompt_tokens: event.usage.input_tokens || 0,
        completion_tokens: event.usage.output_tokens || 0,
        total_tokens: (event.usage.input_tokens || 0) + (event.usage.output_tokens || 0),
        prompt_tokens_details: { cached_tokens: event.usage.cached_tokens || 0 },
        completion_tokens_details: { reasoning_tokens: event.usage.reasoning_tokens || 0 },
      },
    });
  }

  if (event.type === 'done') {
    var chunks = '';
    var doneContent = normalizeStreamText(event.content);
    var doneErrorInfo = normalizeErrorInfo(event.error);
    if (Array.isArray(event.annotations) && event.annotations.length > 0) {
      mergeAnnotationsIntoContext(ctx, event.annotations);
    }

    var finishReason = event.finish_reason || 'stop';
    // 标准 OpenAI finish_reason: "stop" | "length" | "tool_calls" | "content_filter" | "function_call"
    // 如果有未发送的 tool_calls，finish_reason 应该是 "tool_calls"
    if (ctx.toolCallIndex && ctx.toolCallIndex > 0 && finishReason === 'stop') {
      finishReason = 'tool_calls';
    }

    // done 兜底文本：若流式 delta 丢失，先补一个 content chunk 再发 finish chunk
    if (doneContent) {
      var sentContentLength = ctx.sentContentLength || 0;
      if (doneContent.length > sentContentLength) {
        var remainingContent = doneContent.slice(sentContentLength);
        if (remainingContent) {
          chunks += formatSSEData({
            id: id,
            object: 'chat.completion.chunk',
            created: created,
            model: model,
            choices: [{
              index: 0,
              delta: { content: remainingContent },
              logprobs: null,
              finish_reason: null,
            }],
          });
          ctx.sentContentLength = doneContent.length;
        }
      }
    }

    // done 兜底注解：仅补发尚未下发过的 annotations
    var sentAnnotationCount = ctx.sentAnnotationCount || 0;
    if (ctx.annotations.length > sentAnnotationCount) {
      var remainingAnnotations = ctx.annotations.slice(sentAnnotationCount);
      chunks += formatSSEData({
        id: id,
        object: 'chat.completion.chunk',
        created: created,
        model: model,
        choices: [{
          index: 0,
          delta: { annotations: remainingAnnotations },
          logprobs: null,
          finish_reason: null,
        }],
      });
      ctx.sentAnnotationCount = ctx.annotations.length;
    }

    // 最终 chunk（带 finish_reason）
    var donePayload = {
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: {},
        logprobs: null,
        finish_reason: finishReason,
      }],
      usage: event.usage ? {
        prompt_tokens: event.usage.input_tokens || 0,
        completion_tokens: event.usage.output_tokens || 0,
        total_tokens: (event.usage.input_tokens || 0) + (event.usage.output_tokens || 0),
        prompt_tokens_details: { cached_tokens: event.usage.cached_tokens || 0 },
        completion_tokens_details: { reasoning_tokens: event.usage.reasoning_tokens || 0 },
      } : undefined,
    };
    if (doneErrorInfo) {
      donePayload.error = doneErrorInfo;
    }
    chunks += formatSSEData(donePayload);

    chunks += 'data: [DONE]\n\n';
    return chunks;
  }

  // error 事件 — 以 SSE 格式发送错误信息
  if (event.type === 'error') {
    var errorInfo = normalizeErrorInfo(event.error);
    return formatSSEData({
      id: id,
      object: 'chat.completion.chunk',
      created: created,
      model: model,
      choices: [{
        index: 0,
        delta: {},
        finish_reason: 'error',
      }],
      error: errorInfo || {
        message: 'Unknown error',
        type: 'server_error',
        code: 'server_error',
      },
    }) + 'data: [DONE]\n\n';
  }

  return null;
}

/**
 * Universal 完整响应 → OpenAI Chat Completions 非流式 JSON
 *
 * 用于 fakeNonStream: 上游流式收集完成后，返回完整 JSON
 *
 *
 * 完整响应结构:
 *   { id, object: "chat.completion", created, model, system_fingerprint?,
 *     choices: [{ index, message: { role, content, reasoning_content?, tool_calls?, refusal? },
 *                 logprobs?, finish_reason }],
 *     usage: { prompt_tokens, completion_tokens, total_tokens,
 *              prompt_tokens_details?, completion_tokens_details? },
 *     service_tier? }
 *
 * @param {UniversalResponse} response
 * @returns {object}
 */
export function formatNonStreamResponse(response) {
  var message = {
    role: 'assistant',
    content: response.content || '',
  };

  var hasToolCalls = response.tool_calls && response.tool_calls.length > 0;
  if (hasToolCalls) {
    message.tool_calls = response.tool_calls.map(function (tc, i) {
      // 合并分片的 arguments（流式收集器可能产生多个 delta）
      var args = tc.arguments || '';
      if (!args && tc.arguments_delta) {
        args = tc.arguments_delta;
      }
      // 确保 arguments 是有效 JSON 字符串
      if (typeof args !== 'string') {
        args = JSON.stringify(args);
      }
      if (!args) args = '{}';
      return {
        id: tc.id || 'call_' + Date.now() + '_' + i,
        type: 'function',
        function: {
          name: tc.name || '',
          arguments: args,
        },
      };
    });
    if (!response.content) message.content = null;
  }

  if (response.reasoning) {
    message.reasoning_content = response.reasoning;
  }

  if (Array.isArray(response.annotations) && response.annotations.length > 0) {
    var annotations = [];
    for (var a = 0; a < response.annotations.length; a++) {
      var normalized = normalizeAnnotation(response.annotations[a]);
      if (normalized) annotations.push(normalized);
    }
    if (annotations.length > 0) {
      message.annotations = annotations;
    }
  }

  var finishReason = response.finish_reason || 'stop';
  if (hasToolCalls && finishReason === 'stop') {
    finishReason = 'tool_calls';
  }

  var usage = response.usage || {};
  var promptTokens = usage.input_tokens || usage.prompt_tokens || 0;
  var completionTokens = usage.output_tokens || usage.completion_tokens || 0;
  var cachedTokens = usage.cached_tokens || 0;
  var reasoningTokens = usage.reasoning_tokens || 0;

  return {
    id: (response.id && response.id.startsWith('chatcmpl-')) ? response.id : ('chatcmpl-' + Date.now() + Math.random().toString(36).substring(2, 8)),
    object: 'chat.completion',
    created: response.created || Math.floor(Date.now() / 1000),
    model: response.model || '',
    choices: [{
      index: 0,
      message: message,
      logprobs: null,
      finish_reason: finishReason,
    }],
    usage: {
      prompt_tokens: promptTokens,
      completion_tokens: completionTokens,
      total_tokens: promptTokens + completionTokens,
      prompt_tokens_details: { cached_tokens: cachedTokens },
      completion_tokens_details: { reasoning_tokens: reasoningTokens },
    },
  };
}

/**
 * /v1/models 响应格式
 *
 *
 * @param {Array} models - [{ id, display_name, owned_by }]
 * @returns {object}
 */
export function formatModelsResponse(models) {
  var created = Math.floor(Date.now() / 1000);
  return {
    object: 'list',
    data: models.map(function (m) {
      return {
        id: m.id,
        object: 'model',
        created: m.created || created,
        owned_by: m.owned_by || 'openai',
      };
    }),
  };
}

/**
 * 错误响应格式（标准 OpenAI 错误格式）
 *
 * 完整错误结构:
 *   { error: { message, type, param?, code? } }
 *
 * @param {number} status - HTTP 状态码
 * @param {string} message - 错误信息
 * @param {string} [type] - 错误类型
 * @param {string} [param] - 引发错误的参数
 * @param {string} [code] - 错误代码
 * @returns {object}
 */
export function formatErrorResponse(status, message, type, param, code) {
  var error = {
    message: message,
    type: type || 'invalid_request_error',
  };
  if (param) error.param = param;
  if (code) {
    error.code = code;
  } else {
    error.code = mapStatusToErrorCode(status);
  }
  return { error: error };
}

/**
 * 创建流式响应的上下文对象
 * 用于在多个 formatSSEChunk 调用之间共享状态
 *
 * @param {string} [model] - 模型名
 * @returns {object} ctx
 */
export function createStreamContext(model) {
  return {
    id: 'chatcmpl-' + Date.now() + Math.random().toString(36).substring(2, 8),
    model: model || '',
    created: Math.floor(Date.now() / 1000),
    toolCallIndex: 0,
    sentContentLength: 0,
    annotations: [],
    annotationKeyMap: {},
    sentAnnotationCount: 0,
  };
}

// ============ 内部辅助 ============

/**
 * 从消息内容中提取纯文本
 * 支持: string | null | Array<{ type: "text", text: string } | ...>
 *
 */
function extractTextContent(content) {
  if (!content) return '';
  if (typeof content === 'string') return content;
  if (typeof content === 'object' && !Array.isArray(content)) {
    if ((content.type === 'text' || content.type === 'input_text' || content.type === 'output_text') && content.text) {
      return String(content.text);
    }
    return '';
  }
  if (Array.isArray(content)) {
    var texts = [];
    for (var i = 0; i < content.length; i++) {
      var block = content[i];
      if (typeof block === 'string') {
        texts.push(block);
      } else if (block && (block.type === 'text' || block.type === 'input_text' || block.type === 'output_text') && block.text) {
        texts.push(block.text);
      }
    }
    return texts.join('\n');
  }
  return '';
}

/**
 * 归一化流式文本片段，确保下游 delta.content 始终是 string
 */
function normalizeStreamText(content) {
  if (typeof content === 'string') return content;
  if (content === undefined || content === null) return '';
  if (Array.isArray(content)) {
    var text = '';
    for (var i = 0; i < content.length; i++) {
      text += normalizeStreamText(content[i]);
    }
    return text;
  }
  if (typeof content === 'object') {
    if (typeof content.text === 'string') return content.text;
    if (typeof content.delta === 'string') return content.delta;
    if (typeof content.value === 'string') return content.value;
    return '';
  }
  return String(content);
}

function normalizeErrorInfo(error) {
  if (!error) return null;
  if (typeof error === 'string') {
    return {
      message: error,
      type: 'server_error',
      code: 'server_error',
    };
  }
  if (typeof error !== 'object') {
    return {
      message: String(error),
      type: 'server_error',
      code: 'server_error',
    };
  }
  var message = error.message || error.error || 'Unknown error';
  var type = error.type || 'server_error';
  var code = error.code || type || 'server_error';
  var normalized = {
    message: String(message),
    type: String(type),
    code: String(code),
  };
  if (typeof error.status === 'number' && isFinite(error.status) && error.status > 0) {
    normalized.status = Math.floor(error.status);
  }
  return normalized;
}

function normalizeAnnotation(annotation) {
  if (!annotation || typeof annotation !== 'object' || Array.isArray(annotation)) return null;
  var normalized = {};
  var keys = Object.keys(annotation);
  for (var i = 0; i < keys.length; i++) {
    normalized[keys[i]] = annotation[keys[i]];
  }
  var annotationType = readAnnotationString(normalized.type);
  var hasUrlCitation = normalized.url_citation
    && typeof normalized.url_citation === 'object'
    && !Array.isArray(normalized.url_citation);
  if (annotationType === 'url_citation' || hasUrlCitation) {
    var citation = hasUrlCitation ? normalized.url_citation : normalized;
    var citationUrl = readAnnotationString(citation.url || normalized.url);
    if (!citationUrl) return null;
    var citationTitle = readAnnotationString(citation.title || normalized.title || citation.name || normalized.name || citationUrl);
    var citationContent = readAnnotationString(
      citation.content || normalized.content
      || citation.snippet || normalized.snippet
      || citation.summary || normalized.summary
      || citation.description || normalized.description
    );
    var citationStart = normalizeAnnotationIndex(
      citation.start_index !== undefined ? citation.start_index : normalized.start_index,
      0
    );
    var citationEnd = normalizeAnnotationIndex(
      citation.end_index !== undefined ? citation.end_index : normalized.end_index,
      citationStart
    );
    normalized.type = 'url_citation';
    normalized.url = citationUrl;
    normalized.title = citationTitle || citationUrl;
    normalized.content = citationContent;
    normalized.start_index = citationStart;
    normalized.end_index = citationEnd;
    normalized.url_citation = {
      url: normalized.url,
      title: normalized.title,
      content: normalized.content,
      start_index: normalized.start_index,
      end_index: normalized.end_index,
    };
    return normalized;
  }

  if (!annotationType) normalized.type = 'annotation';
  return normalized;
}

function buildAnnotationKey(annotation) {
  var fields = ['type', 'url', 'title', 'content', 'start_index', 'end_index'];
  var keyParts = [];
  for (var i = 0; i < fields.length; i++) {
    var value = readAnnotationField(annotation, fields[i]);
    if (value !== undefined && value !== null && value !== '') {
      keyParts.push(fields[i] + ':' + String(value));
    }
  }
  if (keyParts.length > 0) {
    return keyParts.join('|');
  }
  try {
    return JSON.stringify(annotation);
  } catch (_) {
    return String(annotation.type || 'annotation');
  }
}

function readAnnotationString(value) {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return '';
}

function normalizeAnnotationIndex(value, fallback) {
  var defaultIndex = (typeof fallback === 'number' && fallback >= 0) ? fallback : 0;
  if (value === undefined || value === null || value === '') return defaultIndex;
  var index = Number(value);
  if (!Number.isFinite(index) || index < 0) return defaultIndex;
  return Math.floor(index);
}

function readAnnotationField(annotation, field) {
  if (!annotation || typeof annotation !== 'object') return '';
  var value = annotation[field];
  if (value !== undefined && value !== null && value !== '') return value;
  var citation = annotation.url_citation;
  if (citation && typeof citation === 'object' && !Array.isArray(citation)) {
    var citationValue = citation[field];
    if (citationValue !== undefined && citationValue !== null && citationValue !== '') {
      return citationValue;
    }
  }
  return '';
}

function addUniqueAnnotationToContext(ctx, annotation) {
  if (!ctx || !annotation) return false;
  if (!Array.isArray(ctx.annotations)) ctx.annotations = [];
  if (!ctx.annotationKeyMap) ctx.annotationKeyMap = {};
  var key = buildAnnotationKey(annotation);
  if (ctx.annotationKeyMap[key]) return false;
  ctx.annotationKeyMap[key] = true;
  ctx.annotations.push(annotation);
  return true;
}

function mergeAnnotationsIntoContext(ctx, annotations) {
  if (!Array.isArray(annotations)) return 0;
  var added = 0;
  for (var i = 0; i < annotations.length; i++) {
    var normalized = normalizeAnnotation(annotations[i]);
    if (normalized && addUniqueAnnotationToContext(ctx, normalized)) {
      added++;
    }
  }
  return added;
}

/**
 * 格式化 SSE data 行
 */
function formatSSEData(data) {
  return 'data: ' + JSON.stringify(data) + '\n\n';
}

/**
 * HTTP 状态码 → OpenAI 错误码映射
 */
function mapStatusToErrorCode(status) {
  if (status === 400) return 'invalid_request_error';
  if (status === 401) return 'invalid_api_key';
  if (status === 403) return 'insufficient_quota';
  if (status === 404) return 'model_not_found';
  if (status === 429) return 'rate_limit_exceeded';
  if (status === 500) return 'server_error';
  if (status === 503) return 'overloaded';
  return String(status);
}
