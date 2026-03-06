/**
 * 格式转换统一入口
 *
 * 双向矩阵架构:
 *   客户端适配器（入）         上游适配器（出）
 *   ─────────────           ──────────
 *   openai-chat.mjs    ←→   openai-responses.mjs (Codex 通道)
 *   anthropic.mjs      ←→   chatgpt-conversation.mjs (Chat 通道)
 *   gemini.mjs         ←→   (未来更多上游通道)
 *
 * 流: 客户端请求 → parseRequest → Universal → formatRequest → 上游
 *     上游 SSE → parseSSEEvent → Universal → formatSSEChunk → 客户端 SSE
 *
 * fakeNonStream: 客户端 stream=false → 内部 stream=true → 收集 → 非流式 JSON
 */

// 中间格式
export * as universal from './universal.mjs';

// 客户端适配器
export * as openaiChat from './openai-chat.mjs';
export * as anthropic from './anthropic.mjs';
export * as gemini from './gemini.mjs';

// 上游适配器
export * as codexResponses from './openai-responses.mjs';
export * as chatConversation from './chatgpt-conversation.mjs';

// 工具
export * as modelMapper from './model-mapper.mjs';
export { parseSSEStream, parseSSEText, formatSSELine } from './stream/sse-parser.mjs';

/**
 * 根据请求路径自动检测客户端格式
 *
 * @param {string} path - 请求路径
 * @param {object} body - 请求体（用于特征检测）
 * @returns {'openai-chat' | 'anthropic' | 'gemini' | 'codex-passthrough' | 'unknown'}
 */
export function detectClientFormat(path, body) {
  // 路径优先匹配
  if (path.startsWith('/v1/chat/completions') || path.startsWith('/v1/models')) {
    return 'openai-chat';
  }
  if (path.startsWith('/v1/messages')) {
    return 'anthropic';
  }
  if (path.startsWith('/v1beta/') || path.indexOf(':generateContent') >= 0 || path.indexOf(':streamGenerateContent') >= 0) {
    return 'gemini';
  }
  if (path.startsWith('/backend-api/codex/')) {
    return 'codex-passthrough';
  }
  if (path.startsWith('/backend-api/conversation')) {
    return 'chat-passthrough';
  }

  // 请求体特征检测
  if (body) {
    // Anthropic 特征: 有顶级 system 字段 + messages 里没有 system role
    if (body.system !== undefined && body.messages && body.max_tokens) {
      return 'anthropic';
    }
    // Gemini 特征: 有 contents 字段
    if (body.contents) {
      return 'gemini';
    }
    // OpenAI 特征: 有 messages 字段
    if (body.messages) {
      return 'openai-chat';
    }
    // Codex 特征: 有 input 字段 + instructions
    if (body.input && body.instructions !== undefined) {
      return 'codex-passthrough';
    }
  }

  return 'unknown';
}

/**
 * 根据模型名选择上游通道
 *
 * @param {string} model - 解析后的模型名
 * @returns {'codex' | 'conversation'}
 */
export function selectUpstream(model) {
  // codex 模型 → Codex Responses 通道
  if (model.indexOf('codex') >= 0) {
    return 'codex';
  }
  // chat 模型 → Conversation 通道
  var chatModels = ['auto', 'gpt-5', 'gpt-5-mini', 'gpt-5-2', 'gpt-5-1', 'gpt-5-t-mini', 'research'];
  if (chatModels.indexOf(model) >= 0) {
    return 'conversation';
  }
  // 默认用 codex（已验证可用）
  return 'codex';
}
