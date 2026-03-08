/**
 * ChatGPT Conversation 上游适配器（Chat 通道）
 *
 * 上游端点: POST https://chatgpt.com/backend-api/conversation
 * 格式: ChatGPT 自有 SSE 格式
 * 需要: sentinel pipeline（VM + PoW + Turnstile + Conduit）
 *
 * 职责:
 *   1. Universal → /conversation 请求体（formatRequest）
 *   2. ChatGPT SSE → Universal 流式事件（parseSSEEvent）
 *   3. sentinel headers 注入（formatHeaders）
 *
 * 状态: 接口完整，sentinel pipeline 待实现
 *
 *   - session.mjs 已有的 chat() 方法
 */

import { createStreamEvent } from './universal.mjs';

/**
 * Universal → ChatGPT /conversation 请求体
 *
 * @param {UniversalRequest} universal
 * @param {object} ctx - 上下文（conversationId, parentMessageId）
 * @returns {object}
 */
export function formatRequest(universal, ctx) {
  ctx = ctx || {};

  var messages = [];
  // system 提示词作为第一条 system 消息
  if (universal.system) {
    messages.push({
      id: generateUUID(),
      author: { role: 'system' },
      content: { content_type: 'text', parts: [universal.system] },
    });
  }

  // 历史消息 — /conversation 只发最后一条 user 消息
  // 多轮通过 conversation_id + parent_message_id 维持
  var lastUserMsg = '';
  for (var i = universal.messages.length - 1; i >= 0; i--) {
    if (universal.messages[i].role === 'user') {
      lastUserMsg = typeof universal.messages[i].content === 'string'
        ? universal.messages[i].content
        : JSON.stringify(universal.messages[i].content);
      break;
    }
  }

  messages.push({
    id: generateUUID(),
    author: { role: 'user' },
    content: { content_type: 'text', parts: [lastUserMsg] },
  });

  var body = {
    action: 'next',
    messages: messages,
    model: mapModelForConversation(universal.model),
    parent_message_id: ctx.parentMessageId || generateUUID(),
  };

  if (ctx.conversationId) {
    body.conversation_id = ctx.conversationId;
  }

  return body;
}

/**
 * Codex 模型名 → Chat 模型名映射
 * Codex 端点用 gpt-5-codex-* 系列，Chat 端点用 gpt-5/gpt-5-mini 等
 */
function mapModelForConversation(model) {
  // 如果已经是 chat 模型名，直接返回
  var chatModels = ['auto', 'gpt-5', 'gpt-5-mini', 'gpt-5-2', 'gpt-5-1', 'gpt-5-t-mini', 'research'];
  if (chatModels.indexOf(model) >= 0) return model;

  // codex 模型 → 最近的 chat 模型
  if (model.indexOf('codex-mini') >= 0) return 'gpt-5-mini';
  if (model.indexOf('codex') >= 0) return 'auto';

  return 'auto';
}

/**
 * 解析 ChatGPT /conversation SSE 事件 → Universal 流式事件
 *
 * ChatGPT SSE 格式:
 *   data: {"message":{"id":"...","content":{"parts":["Hello"]},...},"conversation_id":"..."}
 *   data: [DONE]
 *
 * @param {string} eventType
 * @param {object} data
 * @returns {UniversalStreamEvent|null}
 */
export function parseSSEEvent(eventType, data) {
  if (!data) return null;

  // [DONE] 信号
  if (eventType === 'done') {
    return createStreamEvent('done', { finish_reason: 'stop' });
  }

  // 正常消息
  if (data.message && data.message.content && data.message.content.parts) {
    var parts = data.message.content.parts;
    var text = parts.join('');
    var msgId = data.message.id;

    // ChatGPT SSE 每次发完整文本，不是 delta
    // 我们需要记住上次文本来计算 delta
    return createStreamEvent('delta', {
      id: data.conversation_id,
      model: data.message.metadata ? data.message.metadata.model_slug : null,
      content: text, // 调用方需自行计算增量
      _fullText: true, // 标记这是完整文本，不是增量
      _messageId: msgId,
      _conversationId: data.conversation_id,
    });
  }

  return null;
}

/**
 * 构造上游请求的 headers（含 sentinel pipeline）
 *
 * @param {string} accessToken
 * @param {object} sentinel - sentinel tokens
 *   sentinel.requirementsToken
 *   sentinel.proofToken
 *   sentinel.turnstileToken
 *   sentinel.conduitToken
 *   sentinel.deviceId
 *   sentinel.buildVersion
 * @returns {object}
 */
export function formatHeaders(accessToken, sentinel) {
  sentinel = sentinel || {};
  var headers = {
    'Authorization': 'Bearer ' + accessToken,
    'Content-Type': 'application/json',
    'Accept': 'text/event-stream',
    'Oai-Language': 'en-US',
  };

  // sentinel pipeline headers
  if (sentinel.deviceId) {
    headers['Oai-Device-Id'] = sentinel.deviceId;
  }
  if (sentinel.buildVersion) {
    headers['Oai-Client-Version'] = sentinel.buildVersion;
  }
  if (sentinel.requirementsToken) {
    headers['openai-sentinel-chat-requirements-token'] = sentinel.requirementsToken;
  }
  if (sentinel.proofToken) {
    headers['openai-sentinel-proof-token'] = sentinel.proofToken;
  }
  if (sentinel.turnstileToken) {
    headers['openai-sentinel-turnstile-token'] = sentinel.turnstileToken;
  }
  if (sentinel.conduitToken) {
    headers['x-conduit-token'] = sentinel.conduitToken;
  }

  return headers;
}

/**
 * 上游 URL
 */
export function getEndpointUrl(baseUrl) {
  return (baseUrl || 'https://chatgpt.com/backend-api') + '/conversation';
}

/**
 * ChatGPT SSE 增量计算器
 * ChatGPT 每次返回完整文本，需要计算增量
 */
export function createDeltaCalculator() {
  var lastText = '';
  return {
    /**
     * 传入完整文本，返回增量
     */
    getDelta: function (fullText) {
      if (fullText.startsWith(lastText)) {
        var delta = fullText.substring(lastText.length);
        lastText = fullText;
        return delta;
      }
      // 如果不是追加模式（罕见），返回完整文本
      lastText = fullText;
      return fullText;
    },
  };
}

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    var r = Math.random() * 16 | 0;
    var v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}
