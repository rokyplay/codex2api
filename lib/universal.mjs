/**
 * Universal 中间表示格式
 *
 * 所有 API 格式（OpenAI Chat / Responses / Anthropic / Gemini）先转成 Universal，
 * 再从 Universal 转出目标格式。N 种格式只需 2N 个转换函数而不是 N²。
 *
 */

/**
 * 创建 Universal 请求对象
 *
 * @param {object} opts
 * @returns {UniversalRequest}
 */
export function createRequest(opts) {
  return {
    model: opts.model || '',
    system: opts.system || '',
    messages: opts.messages || [],
    stream: opts.stream !== false,
    temperature: opts.temperature,
    max_tokens: opts.max_tokens,
    top_p: opts.top_p,
    stop: opts.stop || null,
    tools: opts.tools || [],
    tool_choice: opts.tool_choice || 'auto',
    metadata: opts.metadata || {},
  };
}

/**
 * 创建 Universal 流式增量事件
 *
 * @param {string} type - 'start' | 'delta' | 'tool_call' | 'reasoning' | 'annotation' | 'usage' | 'done' | 'error'
 * @param {object} data
 * @returns {UniversalStreamEvent}
 */
export function createStreamEvent(type, data) {
  return {
    type: type,
    id: data.id || null,
    model: data.model || null,
    content: data.content || '',
    reasoning: data.reasoning || '',
    tool_call: data.tool_call || null,
    annotation: data.annotation || null,
    annotations: data.annotations || [],
    finish_reason: data.finish_reason || null,
    usage: data.usage || null,
    error: data.error || null,
  };
}

/**
 * 创建 Universal 完整响应（非流式 / 流式收集完成后）
 *
 * @param {object} opts
 * @returns {UniversalResponse}
 */
export function createResponse(opts) {
  return {
    id: opts.id || '',
    model: opts.model || '',
    content: opts.content || '',
    reasoning: opts.reasoning || '',
    finish_reason: opts.finish_reason || 'stop',
    usage: opts.usage || { input_tokens: 0, output_tokens: 0 },
    tool_calls: opts.tool_calls || [],
    annotations: opts.annotations || [],
    created: opts.created || Math.floor(Date.now() / 1000),
  };
}

/**
 * 流式事件收集器 — 将多个 delta 收集成完整 UniversalResponse
 * 用于 fakeNonStream: 上游流式获取 → 客户端完整响应
 *
 *
 * tool_call 累积逻辑:
 *   1. 首次收到 tool_call（有 id+name）→ 创建新条目
 *   2. 后续 arguments_delta → 拼接到对应条目
 *   3. done=true → 标记该 tool_call 完成
 */
export function createStreamCollector() {
  var state = {
    id: null,
    model: null,
    content: '',
    reasoning: '',
    // tool_calls 按 id 索引以支持增量累积
    toolCallMap: {},
    toolCallOrder: [],
    annotations: [],
    annotationKeys: {},
    finish_reason: null,
    usage: null,
  };

  function buildAnnotationKey(annotation) {
    if (!annotation || typeof annotation !== 'object') return '';
    var fields = ['type', 'url', 'title', 'start_index', 'end_index'];
    var parts = [];
    for (var i = 0; i < fields.length; i++) {
      var value = annotation[fields[i]];
      if (value !== undefined && value !== null && value !== '') {
        parts.push(fields[i] + ':' + String(value));
      }
    }
    if (parts.length > 0) return parts.join('|');
    try {
      return JSON.stringify(annotation);
    } catch (_) {
      return String(annotation.type || 'annotation');
    }
  }

  function addAnnotation(annotation) {
    if (!annotation || typeof annotation !== 'object' || Array.isArray(annotation)) return;
    var key = buildAnnotationKey(annotation);
    if (!key || state.annotationKeys[key]) return;
    state.annotationKeys[key] = true;
    state.annotations.push(annotation);
  }

  return {
    push: function (event) {
      if (event.id) state.id = event.id;
      if (event.model) state.model = event.model;

      if (event.type === 'delta' && event.content) {
        state.content += event.content;
      }

      if (event.type === 'reasoning' && event.reasoning) {
        state.reasoning += event.reasoning;
      }

      if (event.type === 'annotation' && event.annotation) {
        addAnnotation(event.annotation);
      }

      if (event.type === 'tool_call' && event.tool_call) {
        var tc = event.tool_call;
        var tcId = tc.id || '';

        // 查找或创建 tool_call 条目
        if (tcId && !state.toolCallMap[tcId]) {
          state.toolCallMap[tcId] = {
            id: tcId,
            name: tc.name || '',
            arguments: '',
          };
          state.toolCallOrder.push(tcId);
        }

        // 尝试通过 id 或最后一个条目匹配
        var entry = state.toolCallMap[tcId];
        if (!entry && state.toolCallOrder.length > 0) {
          var lastId = state.toolCallOrder[state.toolCallOrder.length - 1];
          entry = state.toolCallMap[lastId];
        }

        if (entry) {
          // 更新 name（首次可能为空，后续可能补充）
          if (tc.name && !entry.name) entry.name = tc.name;
          // 累积 arguments
          if (tc.arguments_delta) {
            entry.arguments += tc.arguments_delta;
          }
          // 完整参数覆盖（done 事件）
          if (tc.done && tc.arguments) {
            entry.arguments = tc.arguments;
          }
        }
      }

      if (event.type === 'done') {
        state.finish_reason = event.finish_reason || 'stop';
        if (event.usage) state.usage = event.usage;
        if (Array.isArray(event.annotations)) {
          for (var j = 0; j < event.annotations.length; j++) {
            addAnnotation(event.annotations[j]);
          }
        }
        // done 事件可能携带额外内容作为兜底
        if (event.content && !state.content) {
          state.content = event.content;
        }
      }
    },

    toResponse: function () {
      // 将 toolCallMap 转换为有序数组
      var toolCalls = [];
      for (var i = 0; i < state.toolCallOrder.length; i++) {
        var entry = state.toolCallMap[state.toolCallOrder[i]];
        if (entry) {
          toolCalls.push({
            id: entry.id,
            name: entry.name,
            arguments: entry.arguments || '{}',
          });
        }
      }

      return createResponse({
        id: state.id,
        model: state.model,
        content: state.content,
        reasoning: state.reasoning,
        finish_reason: state.finish_reason || 'stop',
        usage: state.usage || { input_tokens: 0, output_tokens: 0 },
        tool_calls: toolCalls,
        annotations: state.annotations,
      });
    },

    /** 获取当前收集状态（用于调试） */
    getState: function () {
      return {
        id: state.id,
        model: state.model,
        contentLength: state.content.length,
        reasoningLength: state.reasoning.length,
        toolCallCount: state.toolCallOrder.length,
        annotationCount: state.annotations.length,
        finishReason: state.finish_reason,
        hasUsage: state.usage !== null,
      };
    },
  };
}
