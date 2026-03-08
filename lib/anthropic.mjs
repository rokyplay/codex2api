/**
 * Anthropic Messages API 客户端适配器
 *
 * 客户端路径: POST /v1/messages
 * 格式: Anthropic Messages API
 *
 * 职责:
 *   1. Anthropic 请求 → Universal（parseRequest）
 *   2. Universal 流式事件 → Anthropic SSE（formatSSEChunk）
 *   3. Universal 完整响应 → 非流式 JSON（formatNonStreamResponse）
 *
 * 完整支持:
 *   - system: string | ContentBlock[] (含 cache_control)
 *   - messages: user(text/image/tool_result), assistant(text/thinking/tool_use)
 *   - tools: custom(input_schema) + built-in(bash_20250124, text_editor_*, web_search_20250305)
 *   - tool_choice: auto | any | none | { type: "tool", name } (string 或 object 格式)
 *   - thinking: { type: "enabled", budget_tokens }
 *   - metadata: { user_id }
 *   - stop_sequences, top_k, top_p, temperature, max_tokens
 *   - 多模态: image(base64), image_url, document(base64/url)
 *   - SSE 完整事件序列含 ping, thinking_delta, signature_delta
 *   - Claude Code 客户端: built-in tools pass-through, cache_control 清理
 *
 */

import { createRequest, createStreamEvent } from './universal.mjs';

// ============ 内置工具类型（Claude Code / Computer Use）============
var BUILTIN_TOOL_TYPES = [
  'bash_20250124',
  'text_editor_20250124',
  'text_editor_20250429',
  'text_editor_20250728',
  'web_search_20250305',
  'computer_20250124',
];

/**
 * Anthropic Messages 请求 → Universal
 *
 * 完整处理所有 Anthropic 请求字段:
 *   - system: string 或 content blocks 数组（含 cache_control）
 *   - messages: user/assistant 消息，支持所有 content block 类型
 *   - tools: custom + built-in 工具（Claude Code 内置工具 pass-through）
 *   - tool_choice: auto/any/none/tool (string 或 object 格式)
 *   - thinking: { type: "enabled", budget_tokens }
 *   - metadata: { user_id }
 *   - 所有采样参数: temperature, top_p, top_k, max_tokens, stop_sequences
 *
 * 参考:
 *
 * @param {object} body - 客户端请求体
 * @returns {UniversalRequest}
 */
export function parseRequest(body) {
  var system = '';

  // Anthropic system 是顶级字段，支持两种格式:
  // 1. string: "You are a helpful assistant"
  // 2. ContentBlock[]: [{ type: "text", text: "...", cache_control: {...} }]
  if (body.system) {
    if (typeof body.system === 'string') {
      system = body.system;
    } else if (Array.isArray(body.system)) {
      system = body.system.map(function (b) {
        if (typeof b === 'string') return b;
        return b.text || '';
      }).filter(Boolean).join('\n');
    }
  }

  var messages = [];
  var rawMessages = body.messages || [];

  for (var i = 0; i < rawMessages.length; i++) {
    var msg = rawMessages[i];

    if (msg.role === 'user') {
      if (Array.isArray(msg.content)) {
        // user 消息可以同时包含 text/image/tool_result 多种 block
        var toolResults = [];
        var userBlocks = [];

        for (var j = 0; j < msg.content.length; j++) {
          var block = msg.content[j];
          if (block.type === 'tool_result') {
            toolResults.push({
              role: 'tool',
              content: extractToolResultContent(block.content),
              tool_call_id: block.tool_use_id || '',
              is_error: block.is_error || false,
            });
          } else if (block.type === 'image') {
            // Anthropic 原生图片: { type: "image", source: { type: "base64", media_type, data } }
            userBlocks.push({
              type: 'image',
              source: block.source,
            });
          } else if (block.type === 'image_url') {
            // OpenAI 兼容格式 image_url
            userBlocks.push({
              type: 'image_url',
              image_url: block.image_url,
            });
          } else if (block.type === 'document') {
            // PDF 等文档: { type: "document", source: { type: "base64", media_type, data } }
            userBlocks.push({
              type: 'document',
              source: block.source,
            });
          } else if (block.type === 'text') {
            userBlocks.push({ type: 'text', text: block.text || '' });
          }
        }

        // tool_result 转为独立的 tool 消息（每个一条）
        for (var tr = 0; tr < toolResults.length; tr++) {
          messages.push(toolResults[tr]);
        }

        // 非 tool_result 的内容作为 user 消息
        if (userBlocks.length > 0) {
          messages.push({
            role: 'user',
            content: extractContentFromBlocks(userBlocks),
            _multimodal: hasMultimodal(userBlocks) ? userBlocks : undefined,
          });
        }
      } else {
        messages.push({
          role: 'user',
          content: typeof msg.content === 'string' ? msg.content : '',
        });
      }
      continue;
    }

    if (msg.role === 'assistant') {
      if (Array.isArray(msg.content)) {
        // assistant 消息可以包含: text, thinking, redacted_thinking, tool_use
        var text = '';
        var thinking = '';
        var thinkingSignature = null;
        var toolCalls = [];
        var hasRedactedThinking = false;

        for (var k = 0; k < msg.content.length; k++) {
          var aBlock = msg.content[k];
          if (aBlock.type === 'text') {
            text += aBlock.text || '';
          } else if (aBlock.type === 'tool_use') {
            toolCalls.push({
              id: aBlock.id || '',
              type: 'function',
              function: {
                name: aBlock.name || '',
                arguments: typeof aBlock.input === 'string'
                  ? aBlock.input
                  : JSON.stringify(aBlock.input || {}),
              },
            });
          } else if (aBlock.type === 'thinking') {
            // Extended thinking block
            thinking += aBlock.thinking || '';
            if (aBlock.signature) {
              thinkingSignature = aBlock.signature;
            }
          } else if (aBlock.type === 'redacted_thinking') {
            // Redacted thinking — 服务端审查后的 thinking
            hasRedactedThinking = true;
          } else if (aBlock.type === 'server_tool_use') {
            // 服务端工具调用（web_search 等），pass-through
          } else if (aBlock.type === 'web_search_tool_result') {
            // Web 搜索结果，pass-through
          }
        }

        var assistantMsg = {
          role: 'assistant',
          content: text,
          tool_calls: toolCalls.length > 0 ? toolCalls : undefined,
        };

        // 保留 thinking 信息供下游使用
        if (thinking) {
          assistantMsg._thinking = thinking;
        }
        if (thinkingSignature) {
          assistantMsg._thinking_signature = thinkingSignature;
        }

        messages.push(assistantMsg);
      } else {
        messages.push({
          role: 'assistant',
          content: typeof msg.content === 'string' ? msg.content : '',
        });
      }
      continue;
    }
  }

  return createRequest({
    model: body.model || '',
    system: system,
    messages: messages,
    stream: body.stream === true,
    temperature: body.temperature,
    max_tokens: body.max_tokens,
    top_p: body.top_p,
    stop: body.stop_sequences,
    tools: convertAnthropicTools(body.tools),
    tool_choice: convertAnthropicToolChoice(body.tool_choice),
    metadata: {
      thinking: body.thinking || undefined,
      top_k: body.top_k,
      user_id: body.metadata ? body.metadata.user_id : undefined,
      // Claude Code 特有字段: speed, service_tier, output_config
      speed: body.speed,
      service_tier: body.service_tier,
      output_config: body.output_config,
      cache_control: body.cache_control,
      // 原始 system blocks（保留 cache_control 供 pass-through）
      _raw_system: body.system,
      // 原始 tools（保留 built-in 工具类型供 pass-through）
      _raw_tools: body.tools,
    },
  });
}

/**
 * Universal 流式事件 → Anthropic SSE
 *
 * Anthropic SSE 完整事件序列:
 *   message_start → [ping] →
 *     content_block_start(thinking) → thinking_delta... → signature_delta → content_block_stop →
 *     content_block_start(text) → text_delta... → content_block_stop →
 *     content_block_start(tool_use) → input_json_delta... → content_block_stop →
 *   message_delta → message_stop
 *
 * 参考:
 *
 * ctx 状态对象字段:
 *   - id: 消息 ID
 *   - model: 模型名
 *   - blockIndex: 当前 content block 索引
 *   - thinkingBlockIndex: thinking block 索引
 *   - thinkingStarted: thinking 是否已开始
 *   - textStarted: text block 是否已开始
 *   - toolBlockIndices: 工具 block 索引映射
 *   - inputTokens: 输入 token 数
 *
 * @param {UniversalStreamEvent} event
 * @param {object} ctx - 状态上下文（可变，跨调用保持）
 * @returns {string|null}
 */
export function formatSSEChunk(event, ctx) {
  ctx = ctx || {};
  var id = ctx.id || 'msg_' + Date.now();
  var model = ctx.model || event.model || '';

  // 初始化 ctx 默认值
  if (ctx._blockCounter === undefined) {
    ctx._blockCounter = 0;
    ctx._thinkingStarted = false;
    ctx._textStarted = false;
    ctx._toolBlocks = {};
    ctx._openBlocks = [];
  }

  // ---- 开始事件 ----
  if (event.type === 'start') {
    var result = formatAnthropicSSE('message_start', {
      type: 'message_start',
      message: {
        id: id,
        type: 'message',
        role: 'assistant',
        content: [],
        model: model,
        stop_reason: null,
        stop_sequence: null,
        usage: {
          input_tokens: ctx.inputTokens || 0,
          output_tokens: 0,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: ctx.cachedTokens || 0,
        },
      },
    });

    // 紧跟一个 ping 事件（Anthropic 标准行为）
    result += formatAnthropicSSE('ping', { type: 'ping' });

    return result;
  }

  // ---- 推理内容 (thinking) ----
  if (event.type === 'reasoning' && event.reasoning) {
    var thinkingResult = '';

    // 如果 thinking block 还没开始，先发 content_block_start
    if (!ctx._thinkingStarted) {
      ctx._thinkingStarted = true;
      ctx._thinkingBlockIdx = ctx._blockCounter;
      ctx._blockCounter++;
      ctx._openBlocks.push(ctx._thinkingBlockIdx);

      thinkingResult += formatAnthropicSSE('content_block_start', {
        type: 'content_block_start',
        index: ctx._thinkingBlockIdx,
        content_block: { type: 'thinking', thinking: '' },
      });
    }

    thinkingResult += formatAnthropicSSE('content_block_delta', {
      type: 'content_block_delta',
      index: ctx._thinkingBlockIdx,
      delta: { type: 'thinking_delta', thinking: event.reasoning },
    });

    return thinkingResult;
  }

  // ---- thinking 签名 (signature_delta) ----
  if (event.type === 'signature' && event.signature) {
    if (ctx._thinkingStarted && ctx._thinkingBlockIdx !== undefined) {
      return formatAnthropicSSE('content_block_delta', {
        type: 'content_block_delta',
        index: ctx._thinkingBlockIdx,
        delta: { type: 'signature_delta', signature: event.signature },
      });
    }
    return null;
  }

  // ---- 文本增量 ----
  if (event.type === 'delta' && event.content) {
    var textResult = '';

    // 如果 thinking block 还开着，先关闭
    if (ctx._thinkingStarted && ctx._openBlocks.indexOf(ctx._thinkingBlockIdx) !== -1) {
      textResult += formatAnthropicSSE('content_block_stop', {
        type: 'content_block_stop',
        index: ctx._thinkingBlockIdx,
      });
      ctx._openBlocks.splice(ctx._openBlocks.indexOf(ctx._thinkingBlockIdx), 1);
    }

    // 如果 text block 还没开始，发 content_block_start
    if (!ctx._textStarted) {
      ctx._textStarted = true;
      ctx._textBlockIdx = ctx._blockCounter;
      ctx._blockCounter++;
      ctx._openBlocks.push(ctx._textBlockIdx);

      textResult += formatAnthropicSSE('content_block_start', {
        type: 'content_block_start',
        index: ctx._textBlockIdx,
        content_block: { type: 'text', text: '' },
      });
    }

    textResult += formatAnthropicSSE('content_block_delta', {
      type: 'content_block_delta',
      index: ctx._textBlockIdx,
      delta: { type: 'text_delta', text: event.content },
    });

    return textResult;
  }

  // ---- 工具调用 ----
  if (event.type === 'tool_call' && event.tool_call) {
    var tc = event.tool_call;
    var toolResult = '';

    // 工具调用开始（有 name 且不是 done）
    if ((tc.name || tc.id) && !tc.done) {
      // 如果 thinking block 还开着，先关闭
      if (ctx._thinkingStarted && ctx._openBlocks.indexOf(ctx._thinkingBlockIdx) !== -1) {
        toolResult += formatAnthropicSSE('content_block_stop', {
          type: 'content_block_stop',
          index: ctx._thinkingBlockIdx,
        });
        ctx._openBlocks.splice(ctx._openBlocks.indexOf(ctx._thinkingBlockIdx), 1);
      }

      // 如果 text block 还开着，先关闭
      if (ctx._textStarted && ctx._openBlocks.indexOf(ctx._textBlockIdx) !== -1) {
        toolResult += formatAnthropicSSE('content_block_stop', {
          type: 'content_block_stop',
          index: ctx._textBlockIdx,
        });
        ctx._openBlocks.splice(ctx._openBlocks.indexOf(ctx._textBlockIdx), 1);
        ctx._textStarted = false;
      }

      var toolBlockIdx = ctx._blockCounter;
      ctx._blockCounter++;
      ctx._toolBlocks[tc.id || 'tool_' + toolBlockIdx] = toolBlockIdx;
      ctx._openBlocks.push(toolBlockIdx);

      toolResult += formatAnthropicSSE('content_block_start', {
        type: 'content_block_start',
        index: toolBlockIdx,
        content_block: {
          type: 'tool_use',
          id: tc.id || 'toolu_' + Date.now(),
          name: tc.name,
          input: {},
        },
      });

      return toolResult;
    }

    // 工具参数增量
    if (tc.arguments_delta) {
      var tcBlockIdx = ctx._toolBlocks[tc.id] !== undefined
        ? ctx._toolBlocks[tc.id]
        : (ctx._blockCounter > 0 ? ctx._blockCounter - 1 : 0);

      return formatAnthropicSSE('content_block_delta', {
        type: 'content_block_delta',
        index: tcBlockIdx,
        delta: { type: 'input_json_delta', partial_json: tc.arguments_delta },
      });
    }

    // 工具调用结束
    if (tc.done && tc.id && ctx._toolBlocks[tc.id] !== undefined) {
      var doneBlockIdx = ctx._toolBlocks[tc.id];
      if (ctx._openBlocks.indexOf(doneBlockIdx) !== -1) {
        ctx._openBlocks.splice(ctx._openBlocks.indexOf(doneBlockIdx), 1);
        return formatAnthropicSSE('content_block_stop', {
          type: 'content_block_stop',
          index: doneBlockIdx,
        });
      }
    }
  }

  // ---- 完成 ----
  if (event.type === 'done') {
    var doneResult = '';

    // 关闭所有还开着的 content block
    for (var ob = 0; ob < ctx._openBlocks.length; ob++) {
      doneResult += formatAnthropicSSE('content_block_stop', {
        type: 'content_block_stop',
        index: ctx._openBlocks[ob],
      });
    }
    ctx._openBlocks = [];

    // 映射 finish_reason → stop_reason
    var stopReason = mapFinishReasonToStopReason(event.finish_reason);

    // message_delta (含 usage)
    doneResult += formatAnthropicSSE('message_delta', {
      type: 'message_delta',
      delta: {
        stop_reason: stopReason,
        stop_sequence: null,
      },
      usage: {
        output_tokens: event.usage ? (event.usage.output_tokens || 0) : 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: event.usage ? (event.usage.cached_tokens || 0) : 0,
      },
    });

    // message_stop
    doneResult += formatAnthropicSSE('message_stop', {
      type: 'message_stop',
    });

    return doneResult;
  }

  // ---- 错误事件 ----
  // event.error 可能是 string（来自 openai-responses.mjs）或 object
  if (event.type === 'error' && event.error) {
    var errObj = typeof event.error === 'string'
      ? { type: 'api_error', message: event.error }
      : { type: event.error.type || 'api_error', message: event.error.message || 'Unknown error' };
    return formatAnthropicSSE('error', {
      type: 'error',
      error: errObj,
    });
  }

  return null;
}

/**
 * Universal 完整响应 → Anthropic Messages 非流式 JSON
 *
 * 完整生成 Anthropic Messages API 非流式响应格式:
 *   - id: msg_xxx
 *   - type: "message"
 *   - role: "assistant"
 *   - content: ContentBlock[]（包含 thinking, text, tool_use）
 *   - model
 *   - stop_reason: end_turn | max_tokens | stop_sequence | tool_use
 *   - stop_sequence: null | string
 *   - usage: { input_tokens, output_tokens, cache_creation_input_tokens?, cache_read_input_tokens? }
 *
 * 参考:
 *
 * @param {UniversalResponse} response
 * @returns {object}
 */
export function formatNonStreamResponse(response) {
  var content = [];

  // thinking block（如果有推理内容）
  if (response.reasoning) {
    var thinkingBlock = {
      type: 'thinking',
      thinking: response.reasoning,
    };
    // 如果有签名，附加上去
    if (response._thinking_signature) {
      thinkingBlock.signature = response._thinking_signature;
    }
    content.push(thinkingBlock);
  }

  // text block
  if (response.content) {
    content.push({ type: 'text', text: response.content });
  }

  // tool_use blocks
  if (response.tool_calls && response.tool_calls.length > 0) {
    for (var i = 0; i < response.tool_calls.length; i++) {
      var tc = response.tool_calls[i];
      var toolInput;

      if (typeof tc.arguments === 'string') {
        toolInput = safeParseJSON(tc.arguments);
      } else if (tc.input) {
        toolInput = tc.input;
      } else {
        toolInput = {};
      }

      content.push({
        type: 'tool_use',
        id: tc.id || 'toolu_' + generateId(),
        name: tc.name || (tc.function ? tc.function.name : ''),
        input: toolInput,
      });
    }
  }

  // 如果 content 为空，至少放一个空 text block（Anthropic 要求 content 非空）
  if (content.length === 0) {
    content.push({ type: 'text', text: '' });
  }

  var stopReason = mapFinishReasonToStopReason(response.finish_reason);

  // 确保 id 以 msg_ 开头
  var responseId = response.id || 'msg_' + generateId();
  if (responseId.indexOf('msg_') !== 0) {
    responseId = 'msg_' + responseId;
  }

  return {
    id: responseId,
    type: 'message',
    role: 'assistant',
    content: content,
    model: response.model || '',
    stop_reason: stopReason,
    stop_sequence: null,
    usage: {
      input_tokens: response.usage ? (response.usage.input_tokens || 0) : 0,
      output_tokens: response.usage ? (response.usage.output_tokens || 0) : 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: response.usage ? (response.usage.cached_tokens || 0) : 0,
    },
  };
}

/**
 * 错误响应格式（Anthropic 标准错误格式）
 *
 *
 * @param {number} status - HTTP 状态码
 * @param {string} message - 错误消息
 * @param {string} [type] - 错误类型
 * @returns {object}
 */
export function formatErrorResponse(status, message, type) {
  // 根据状态码映射错误类型
  var errorType = type;
  if (!errorType) {
    if (status === 400) errorType = 'invalid_request_error';
    else if (status === 401) errorType = 'authentication_error';
    else if (status === 403) errorType = 'permission_error';
    else if (status === 404) errorType = 'not_found_error';
    else if (status === 429) errorType = 'rate_limit_error';
    else if (status === 529) errorType = 'overloaded_error';
    else errorType = 'api_error';
  }

  return {
    type: 'error',
    error: {
      type: errorType,
      message: message,
    },
  };
}

/**
 * 格式化流式错误事件为 SSE
 *
 * @param {string} message - 错误消息
 * @param {string} [type] - 错误类型
 * @returns {string}
 */
export function formatErrorSSE(message, type) {
  return formatAnthropicSSE('error', {
    type: 'error',
    error: {
      type: type || 'api_error',
      message: message,
    },
  });
}

/**
 * 生成 ping 事件（保持连接，防止超时）
 *
 *
 * @returns {string}
 */
export function formatPingSSE() {
  return formatAnthropicSSE('ping', { type: 'ping' });
}

// ============ 内部辅助函数 ============

/**
 * 从 tool_result 的 content 字段提取文本
 *
 * tool_result.content 可以是:
 *   - string: 直接返回
 *   - ContentBlock[]: 提取 text，保留 image
 *   - object: JSON 序列化
 *   - null/undefined: 空字符串
 *
 */
function extractToolResultContent(content) {
  if (!content) return '';
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    var parts = [];
    for (var i = 0; i < content.length; i++) {
      var block = content[i];
      if (typeof block === 'string') {
        parts.push(block);
      } else if (block && block.type === 'text') {
        parts.push(block.text || '');
      } else if (block && block.type === 'image') {
        parts.push('[Image]');
      }
    }
    return parts.join('\n');
  }
  if (typeof content === 'object') {
    return JSON.stringify(content);
  }
  return String(content);
}

/**
 * 从 content blocks 提取纯文本内容
 */
function extractContentFromBlocks(blocks) {
  if (!blocks || blocks.length === 0) return '';
  var textParts = [];
  for (var i = 0; i < blocks.length; i++) {
    if (blocks[i].type === 'text') {
      textParts.push(blocks[i].text || '');
    }
  }
  return textParts.join('\n') || '';
}

/**
 * 检查 blocks 是否包含多模态内容
 */
function hasMultimodal(blocks) {
  if (!blocks) return false;
  for (var i = 0; i < blocks.length; i++) {
    var t = blocks[i].type;
    if (t === 'image' || t === 'image_url' || t === 'document') {
      return true;
    }
  }
  return false;
}

/**
 * 转换 Anthropic 工具定义 → Universal
 *
 * 支持两类工具:
 *   1. Custom tool: { name, description, input_schema }
 *   2. Built-in tool: { type: "bash_20250124", name: "bash" } — 没有 input_schema
 *
 * 参考:
 */
function convertAnthropicTools(tools) {
  if (!tools || !Array.isArray(tools)) return [];

  var result = [];

  for (var i = 0; i < tools.length; i++) {
    var t = tools[i];
    if (!t) continue;

    // 检查是否是 built-in 工具（有 type 字段且在 BUILTIN_TOOL_TYPES 中）
    if (t.type && BUILTIN_TOOL_TYPES.indexOf(t.type) !== -1) {
      result.push({
        type: 'builtin',
        name: t.name || t.type,
        builtin_type: t.type,
        _raw: t,
      });
      continue;
    }

    // Custom tool
    result.push({
      type: 'function',
      function: {
        name: t.name || '',
        description: t.description || '',
        parameters: t.input_schema || { type: 'object', properties: {} },
      },
    });
  }

  return result;
}

/**
 * 转换 Anthropic tool_choice → Universal
 *
 * Anthropic tool_choice 格式:
 *   - string: "auto", "any", "none" (非标准但某些客户端发送)
 *   - object: { type: "auto" } | { type: "any" } | { type: "none" } | { type: "tool", name: "xxx" }
 *   - 含 disable_parallel_tool_use 选项
 *
 */
function convertAnthropicToolChoice(tc) {
  if (!tc) return 'auto';

  // string 格式（某些客户端发送）
  if (typeof tc === 'string') {
    if (tc === 'auto') return 'auto';
    if (tc === 'any') return 'required';
    if (tc === 'none') return 'none';
    return 'auto';
  }

  // object 格式
  if (tc.type === 'auto') return 'auto';
  if (tc.type === 'any') return 'required';
  if (tc.type === 'none') return 'none';
  if (tc.type === 'tool' && tc.name) {
    return { type: 'function', function: { name: tc.name } };
  }

  return 'auto';
}

/**
 * 映射 Universal finish_reason → Anthropic stop_reason
 *
 *   - end_turn: 正常结束
 *   - max_tokens: 达到 token 上限
 *   - stop_sequence: 遇到停止序列
 *   - tool_use: 需要调用工具
 *   - refusal: 拒绝回答
 */
function mapFinishReasonToStopReason(finishReason) {
  if (!finishReason) return 'end_turn';

  var mapping = {
    'stop': 'end_turn',
    'end_turn': 'end_turn',
    'length': 'max_tokens',
    'max_tokens': 'max_tokens',
    'tool_calls': 'tool_use',
    'tool_use': 'tool_use',
    'function_call': 'tool_use',
    'stop_sequence': 'stop_sequence',
    'content_filter': 'end_turn',
    'refusal': 'refusal',
  };

  return mapping[finishReason] || 'end_turn';
}

/**
 * 格式化 Anthropic SSE 事件
 *
 * 格式: event: {eventType}\ndata: {json}\n\n
 *
 * @param {string} eventType - 事件类型
 * @param {object} data - 事件数据
 * @returns {string}
 */
function formatAnthropicSSE(eventType, data) {
  return 'event: ' + eventType + '\ndata: ' + JSON.stringify(data) + '\n\n';
}

/**
 * 安全 JSON 解析
 */
function safeParseJSON(str) {
  if (!str) return {};
  if (typeof str !== 'string') return str;
  try {
    return JSON.parse(str);
  } catch (e) {
    return {};
  }
}

/**
 * 生成唯一 ID（不依赖 crypto）
 */
function generateId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2, 9);
}

/**
 * 创建 SSE 上下文对象（工厂函数）
 * 避免路由层手动构造不完整的 ctx
 *
 * @param {object} [opts] - 选项
 * @param {string} [opts.id] - 消息 ID
 * @param {string} [opts.model] - 模型名
 * @param {number} [opts.inputTokens] - 输入 token 数
 * @returns {object} ctx
 */
export function createSSEContext(opts) {
  opts = opts || {};
  return {
    id: opts.id || 'msg_' + Date.now() + Math.random().toString(36).substring(2, 8),
    model: opts.model || '',
    inputTokens: opts.inputTokens || 0,
    _blockCounter: 0,
    _thinkingStarted: false,
    _textStarted: false,
    _toolBlocks: {},
    _openBlocks: [],
  };
}
