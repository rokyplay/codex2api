/**
 * OpenAI Responses API 上游适配器（Codex 通道）
 *
 * 上游端点: POST https://chatgpt.com/backend-api/codex/responses
 * 格式: OpenAI Responses API，必须 stream: true
 * Headers: Authorization: Bearer {token} + originator: codex_cli_rs
 *
 * 职责:
 *   1. Universal → Codex Responses 请求体（formatRequest）
 *   2. Codex Responses SSE → Universal 流式事件（parseSSEEvent）
 *   3. 透传模式：直接转发 Responses 格式请求（passthrough）
 *
 *   - session.mjs 已验证的 codex() 方法
 *   - OpenAI Responses API 官方文档（streaming events 完整列表）
 */

import crypto from 'node:crypto';
import { createStreamEvent, createStreamCollector } from './universal.mjs';
import { parseSSEStream } from './stream/sse-parser.mjs';
import { timestamp } from '../utils.mjs';

var LOG_TAG = '[openai-responses]';
function dlog(message) {
  console.log('[' + timestamp() + '] ' + LOG_TAG + ' ' + message);
}
function wlog(message) {
  console.warn('[' + timestamp() + '] ' + LOG_TAG + ' ' + message);
}
function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return '"[unserializable:' + (err && err.message ? err.message : 'unknown') + ']"';
  }
}

// ==================== Tool Call ID 适配 ====================
// 上游 (ChatGPT Backend) 期望 call_id 格式为: fc_ + 24 位字母数字
// 客户端可能传入短 ID (如 "call_1") 或无效格式，需要适配
// 参考: responses-proxy fallback ID: call_{request_id}_{index}
// 参考: OpenAI Codex 官方 call_id vs id 区分

var CALL_ID_PATTERN = /^fc_[A-Za-z0-9]{24}$/;

/**
 * 生成有效的 tool call ID
 * 格式: fc_ + 24 位字符（由 seed 派生，确保可复现）
 * 上游明确要求: "Expected an ID that begins with 'fc'."
 */
function _generateCallId(seed) {
  var material = String(seed || 'default');
  var digest = crypto.createHash('sha256').update(material).digest('hex');
  return 'fc_' + digest.substring(0, 24);
}

/**
 * 检查 call_id 是否为上游可接受的格式
 * 上游只接受以 fc_ 开头且足够长度的 ID
 */
function _isValidCallId(id) {
  if (!id || typeof id !== 'string') return false;
  return CALL_ID_PATTERN.test(id);
}

/**
 * 获取有效的 call_id，必要时生成新 ID 并记录映射
 * @param {string} originalId - 客户端传入的原始 ID
 * @param {object} idMap - ID 映射表 { originalId → validId }
 * @param {string} fallbackSeed - originalId 缺失时的稳定种子
 * @returns {string} 有效的 call_id
 */
function _resolveCallId(originalId, idMap, fallbackSeed) {
  var normalizedOriginal = (originalId === undefined || originalId === null) ? '' : String(originalId);
  if (!normalizedOriginal) {
    return _generateUniqueCallId('missing:' + String(fallbackSeed || ''));
  }
  if (_isValidCallId(normalizedOriginal)) return normalizedOriginal;
  // 检查是否已有映射
  if (idMap[normalizedOriginal]) return idMap[normalizedOriginal];
  // 生成稳定新 ID 并记录映射
  var newId = _generateUniqueCallId('invalid:' + normalizedOriginal);
  idMap[normalizedOriginal] = newId;
  return newId;

  function _generateUniqueCallId(seedBase) {
    var nextId = '';
    for (var attempt = 0; attempt < 8; attempt++) {
      var salt = attempt === 0 ? '' : ('|' + attempt);
      nextId = _generateCallId(seedBase + salt);
      var duplicated = false;
      var keys = Object.keys(idMap);
      for (var i = 0; i < keys.length; i++) {
        if (idMap[keys[i]] === nextId) {
          duplicated = true;
          break;
        }
      }
      if (!duplicated) return nextId;
    }
    return _generateCallId(seedBase + '|fallback');
  }
}

// ==================== JSON Schema 规范化 ====================
// 参考: OpenAI Codex 官方 sanitize_json_schema()
// 确保 schema 符合上游要求:
//   - 每个 schema 节点都有 type 字段
//   - object 类型必须有 properties
//   - array 类型必须有 items

/**
 * 规范化 JSON Schema，确保上游可接受
 * 递归处理所有嵌套 schema
 * @param {*} schema - JSON Schema 对象
 * @returns {object} 规范化后的 schema
 */
function sanitizeJsonSchema(schema) {
  if (!schema || typeof schema !== 'object') {
    return { type: 'string' };
  }

  // 布尔 schema → 字符串（JSON Schema 支持 true/false 作为 schema）
  if (typeof schema === 'boolean') {
    return { type: 'string' };
  }

  var result = {};
  var keys = Object.keys(schema);
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    result[k] = schema[k];
  }

  // 递归处理 properties 中的每个子 schema
  if (result.properties && typeof result.properties === 'object') {
    var props = {};
    var pkeys = Object.keys(result.properties);
    for (var p = 0; p < pkeys.length; p++) {
      props[pkeys[p]] = sanitizeJsonSchema(result.properties[pkeys[p]]);
    }
    result.properties = props;
  }

  // 递归处理 items
  if (result.items && typeof result.items === 'object') {
    result.items = sanitizeJsonSchema(result.items);
  }

  // 递归处理 oneOf / anyOf / allOf / prefixItems
  var combiners = ['oneOf', 'anyOf', 'allOf', 'prefixItems'];
  for (var c = 0; c < combiners.length; c++) {
    var ck = combiners[c];
    if (Array.isArray(result[ck])) {
      result[ck] = result[ck].map(function (s) { return sanitizeJsonSchema(s); });
    }
  }

  // 递归处理 additionalProperties（如果是 schema 对象）
  if (result.additionalProperties && typeof result.additionalProperties === 'object'
      && typeof result.additionalProperties !== 'boolean') {
    result.additionalProperties = sanitizeJsonSchema(result.additionalProperties);
  }

  // 确保 type 字段存在 — 根据已有字段推断
  if (!result.type) {
    // type 是数组（union type）→ 取第一个有效值
    if (Array.isArray(schema.type)) {
      var validTypes = ['object', 'array', 'string', 'number', 'integer', 'boolean'];
      for (var t = 0; t < schema.type.length; t++) {
        if (validTypes.indexOf(schema.type[t]) !== -1) {
          result.type = schema.type[t];
          break;
        }
      }
    }
    // 从关键字推断
    if (!result.type) {
      if (result.properties || result.required || result.additionalProperties) {
        result.type = 'object';
      } else if (result.items || result.prefixItems) {
        result.type = 'array';
      } else if (result.enum || result.const || result.format) {
        result.type = 'string';
      } else if (result.minimum !== undefined || result.maximum !== undefined
                 || result.exclusiveMinimum !== undefined || result.exclusiveMaximum !== undefined
                 || result.multipleOf !== undefined) {
        result.type = 'number';
      } else {
        result.type = 'string'; // 最终兜底
      }
    }
  }

  // object 必须有 properties
  if (result.type === 'object' && !result.properties) {
    result.properties = {};
  }

  // array 必须有 items
  if (result.type === 'array' && !result.items) {
    result.items = { type: 'string' };
  }

  return result;
}

/**
 * Universal → Codex Responses API 请求体
 *
 * 核心转换:
 *   - universal.system → instructions
 *   - universal.messages → input (Responses API 格式)
 *   - 始终 stream: true（上游强制要求）
 *   - 适配无效 tool_call ID → 生成上游可接受的有效 ID
 *
 * Responses API input 格式:
 *   - { type: "message", role, content: [{ type: "input_text"|"output_text", text }] }
 *   - { type: "function_call", id, call_id, name, arguments }
 *   - { type: "function_call_output", call_id, output }
 *
 * @param {UniversalRequest} universal
 * @returns {object} Codex Responses API body
 */
export function formatRequest(universal) {
  var input = [];
  var idMap = {}; // 无效 ID → 有效 ID 映射表
  var lastFunctionCallId = '';

  // 第一遍: 扫描所有 tool_calls，构建 ID 映射
  for (var m = 0; m < universal.messages.length; m++) {
    var scanMsg = universal.messages[m];
    if (scanMsg.role === 'assistant' && Array.isArray(scanMsg.tool_calls)) {
      for (var s = 0; s < scanMsg.tool_calls.length; s++) {
        var scanCall = scanMsg.tool_calls[s];
        if (scanCall.id && !_isValidCallId(scanCall.id)) {
          _resolveCallId(scanCall.id, idMap, 'scan:' + m + ':' + s);
        }
      }
    }
  }

  // 第二遍: 构建 input 数组，应用 ID 映射
  for (var i = 0; i < universal.messages.length; i++) {
    var msg = universal.messages[i];
    var role = msg.role;

    // system 消息不放 input，放 instructions
    if (role === 'system') continue;

    // tool 结果消息 → function_call_output
    // 容错: 个别客户端可能漏传 tool_call_id，回退关联到最近一次 function_call
    if (role === 'tool') {
      var output = msg.content;
      if (typeof output !== 'string') {
        output = JSON.stringify(output);
      }
      var sourceToolCallId = '';
      if (msg.tool_call_id !== undefined && msg.tool_call_id !== null) {
        sourceToolCallId = String(msg.tool_call_id);
      } else if (lastFunctionCallId) {
        sourceToolCallId = lastFunctionCallId;
      } else {
        sourceToolCallId = 'tool_msg_' + i;
      }
      // 适配: 如果原始 tool_call_id 在映射表中，使用映射后的有效 ID
      var resolvedCallId = idMap[sourceToolCallId] || sourceToolCallId;
      if (!_isValidCallId(resolvedCallId)) {
        resolvedCallId = _resolveCallId(sourceToolCallId, idMap, 'tool:' + i);
      }
      input.push({
        type: 'function_call_output',
        call_id: resolvedCallId,
        output: output,
      });
      continue;
    }

    // assistant 带 tool_calls → message + function_call(s)
    if (role === 'assistant' && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
      // 先推文本内容（如果有）
      var assistantText = extractText(msg.content);
      if (assistantText) {
        input.push({
          type: 'message',
          role: 'assistant',
          content: [{ type: 'output_text', text: assistantText }],
        });
      }
      // 再推 function calls
      for (var tc = 0; tc < msg.tool_calls.length; tc++) {
        var call = msg.tool_calls[tc];
        var fnName = '';
        var fnArgs = '{}';
        if (call.function) {
          fnName = call.function.name || '';
          fnArgs = call.function.arguments || '{}';
        }
        // 适配: 使用有效的 call_id
        var validCallId = _resolveCallId(call.id, idMap, 'assistant:' + i + ':' + tc);
        lastFunctionCallId = validCallId;
        input.push({
          type: 'function_call',
          id: validCallId,
          call_id: validCallId,
          name: fnName,
          arguments: fnArgs,
        });
      }
      continue;
    }

    // 普通 user / assistant 消息 → message
    var contentBlocks = convertContentToBlocks(msg.content, role);
    if (contentBlocks.length > 0) {
      var normalizedRole = 'user';
      if (role === 'assistant') normalizedRole = 'assistant';
      if (role === 'developer') normalizedRole = 'developer';
      input.push({
        type: 'message',
        role: normalizedRole,
        content: contentBlocks,
      });
    }
  }

  // ============ Roo Code 语义归一化 ============
  // Roo Code 在每次 tool 执行后注入 <environment_details> 作为独立 user 消息，
  // 且将真实用户回复嵌入 attempt_completion 的 function_call_output 中（<user_message>标签）。
  // 这导致：(a) 大量噪声 user 回合 (b) 真实用户意图被埋在 tool 输出里 (c) 对话以 metadata-only user 收尾。
  // 归一化：提取 <user_message> 为真实 user 消息，environment_details 转为 developer 上下文。
  var normalizeBeforeStats = _collectNormalizeStats(input);
  input = _normalizeRooCodeInput(input);
  var normalizeAfterStats = _collectNormalizeStats(input);
  dlog('[NORMALIZE] formatRequest input ' + normalizeBeforeStats.total + ' -> ' + normalizeAfterStats.total
    + ' | env_tag=' + normalizeBeforeStats.envTagCount
    + ' env_only=' + normalizeBeforeStats.envOnlyCount
    + ' user_message_tag=' + normalizeBeforeStats.userMessageTagCount
    + ' | env_only_idx=' + (normalizeBeforeStats.envOnlyIndexes.length > 0 ? normalizeBeforeStats.envOnlyIndexes.join(',') : '-')
    + ' user_message_idx=' + (normalizeBeforeStats.userMessageTagIndexes.length > 0 ? normalizeBeforeStats.userMessageTagIndexes.join(',') : '-')
    + ' | last_before=' + normalizeBeforeStats.lastSummary
    + ' | last_after=' + normalizeAfterStats.lastSummary);

  // ============ 提取 instructions ============
  // 优先用 universal.system；否则尝试把第一条超长 user 消息提升为 instructions
  var defaultInstructions = 'You are a helpful assistant.';
  var instructions = universal.system || defaultInstructions;
  var promotedLongUserMessage = false;
  if (instructions === defaultInstructions && input.length > 0) {
    // Roo Code 可能把超长系统提示塞在首批 user 消息里，这里做提升
    var promoteIndex = -1;
    var promoteText = '';
    var scanLimit = input.length < 8 ? input.length : 8;
    for (var p = 0; p < scanLimit; p++) {
      var candidate = input[p];
      if (!candidate || candidate.type !== 'message' || candidate.role !== 'user' || !candidate.content) continue;
      var candidateText = _extractItemText(candidate);
      var hasRooUserTag = candidateText.indexOf('<user_message>') !== -1;
      var hasRooEnvTag = candidateText.indexOf('<environment_details>') !== -1;
      if (candidateText.length > 1000 && !hasRooUserTag && !hasRooEnvTag) {
        promoteIndex = p;
        promoteText = candidateText;
        break;
      }
    }
    if (promoteIndex >= 0) {
      instructions = promoteText;
      promotedLongUserMessage = true;
      input.splice(promoteIndex, 1);
      dlog('promoted long user message[' + promoteIndex + '] to instructions (' + instructions.length + ' chars)');
    }
  }
  if (!instructions) {
    instructions = defaultInstructions;
  }

  // 兜底：对话若以 function_call_output 收尾，补一条用户续接消息触发模型继续生成文本
  if (input.length > 0) {
    var tailItem = input[input.length - 1];
    if (tailItem && tailItem.type === 'function_call_output') {
      input.push({
        type: 'message',
        role: 'user',
        content: [{ type: 'input_text', text: 'Please continue based on the latest tool result and answer the user.' }],
      });
      dlog('appended continuation user message after trailing function_call_output');
    }
  }

  // 防御：上游要求 input 非空；且不建议以 function_call/function_call_output 开头
  if (input.length === 0) {
    input.push({
      type: 'message',
      role: 'user',
      content: [{
        type: 'input_text',
        text: promotedLongUserMessage
          ? 'Please answer based on the instructions above.'
          : 'Please continue the conversation.',
      }],
    });
    dlog('inserted placeholder user message because input became empty');
  } else {
    var headItem = input[0];
    if (headItem && (headItem.type === 'function_call' || headItem.type === 'function_call_output')) {
      input.unshift({
        type: 'message',
        role: 'user',
        content: [{ type: 'input_text', text: 'Please continue the conversation.' }],
      });
      dlog('inserted placeholder user message before leading ' + headItem.type);
    }
  }

  if (!_hasNonDeveloperMessage(input)) {
    input.push({
      type: 'message',
      role: 'user',
      content: [{ type: 'input_text', text: 'Please continue the conversation.' }],
    });
    dlog('appended placeholder user message because no user/assistant message remained');
  }

  var body = {
    model: universal.model,
    instructions: instructions,
    input: input,
    stream: true, // 上游强制要求
    store: false,
    reasoning: { summary: 'auto' },
    include: ['reasoning.encrypted_content'],
  };

  // tools — 仅在有工具定义时添加
  dlog('[WEB-SEARCH] formatRequest universal.tools=' + safeStringify(universal.tools || []));
  var formattedTools = formatTools(universal.tools);
  dlog('[WEB-SEARCH] formatRequest formatted tools=' + safeStringify(formattedTools));
  if (formattedTools.length > 0) {
    body.tools = formattedTools;
    var normalizedChoice = normalizeToolChoice(universal.tool_choice || 'auto');
    body.tool_choice = sanitizeToolChoiceForTools(normalizedChoice, formattedTools);
    // 透传 parallel_tool_calls（Roo Code 通常为 true）
    var ptc = universal.metadata && universal.metadata.parallel_tool_calls;
    body.parallel_tool_calls = ptc !== undefined ? ptc : true;
  }

  // 注意: 上游 ChatGPT Codex Responses API 不支持 temperature / max_output_tokens / top_p
  // 这些采样参数会导致 400 "Unsupported parameter"，故不传递

  var metadata = universal.metadata || {};
  if (metadata.reasoning_summary !== undefined && metadata.reasoning_summary !== null && metadata.reasoning_summary !== '') {
    body.reasoning = body.reasoning || {};
    body.reasoning.summary = metadata.reasoning_summary;
  }
  if (metadata.reasoning_effort) {
    body.reasoning = body.reasoning || {};
    body.reasoning.effort = metadata.reasoning_effort;
  }
  var normalizedModel = (universal.model || '').toLowerCase();
  if (!body.reasoning.effort && normalizedModel === 'gpt-5.3-codex') {
    body.reasoning.effort = 'xhigh';
  }

  // text 参数透传（verbosity / format）
  if (metadata.text_verbosity) {
    body.text = body.text || {};
    body.text.verbosity = metadata.text_verbosity;
  }
  if (metadata.text_format) {
    body.text = body.text || {};
    body.text.format = metadata.text_format;
  }
  if (!body.text && normalizedModel === 'gpt-5.3-codex') {
    body.text = { verbosity: 'low' };
  }

  // prompt_cache_key — 透传客户端值
  if (metadata.prompt_cache_key) {
    body.prompt_cache_key = metadata.prompt_cache_key;
  }

  // prompt_cache_retention — 透传客户端值
  if (metadata.prompt_cache_retention) {
    body.prompt_cache_retention = metadata.prompt_cache_retention;
  }

  return body;
}

/**
 * Roo Code 语义归一化
 *
 * 处理 Roo Code 特有的对话模式：
 * 1. 从 attempt_completion 的 function_call_output 中提取 <user_message> 为独立 user 消息
 * 2. 纯 <environment_details> 的 user 消息 → 合并到前一个 function_call_output（作为执行上下文）
 * 3. 确保对话不以 metadata-only 消息收尾
 */
function _normalizeRooCodeInput(input) {
  var result = [];
  var envDetailsCount = 0;
  var envTagCount = 0;
  var envTagButNotOnlyCount = 0;
  var extractedUserMsgCount = 0;
  var mergedEnvCount = 0;
  var skippedEnvCount = 0;

  for (var i = 0; i < input.length; i++) {
    var item = input[i];

    // ---- 处理 function_call_output：提取 <user_message> ----
    if (item.type === 'function_call_output' && item.output) {
      var outputText = typeof item.output === 'string' ? item.output : JSON.stringify(item.output);
      var normalizedOutputItem = Object.assign({}, item, { output: outputText });
      var userMsgMatch = outputText.match(/<user_message>\s*([\s\S]*?)\s*<\/user_message>/);
      if (userMsgMatch && userMsgMatch[1].trim()) {
        // 保留原始 function_call_output
        result.push(normalizedOutputItem);
        // 提取 <user_message> 为真实 user 消息
        result.push({
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: userMsgMatch[1].trim() }],
        });
        extractedUserMsgCount++;
        continue;
      }
      result.push(normalizedOutputItem);
      continue;
    }

    // ---- 处理纯 <environment_details> 的 user 消息 ----
    if (item.type === 'message' && item.role === 'user') {
      var userText = _extractItemText(item);
      var hasEnvTag = !!(userText && userText.indexOf('<environment_details>') !== -1);
      if (hasEnvTag) envTagCount++;
      var isEnvOnly = _isEnvironmentDetailsOnly(userText);
      if (hasEnvTag && !isEnvOnly) envTagButNotOnlyCount++;
      if (isEnvOnly) {
        envDetailsCount++;
        // 前一条原始输入是 function_call_output 时，将环境元数据并入该工具输出
        var prevInputItem = i > 0 ? input[i - 1] : null;
        if (prevInputItem && prevInputItem.type === 'function_call_output') {
          var merged = false;
          for (var r = result.length - 1; r >= 0; r--) {
            if (result[r] && result[r].type === 'function_call_output') {
              result[r].output += '\n\n[Environment Context]\n' + userText;
              merged = true;
              mergedEnvCount++;
              break;
            }
          }
          if (!merged && i === input.length - 1) {
            result.push({
              type: 'message',
              role: 'developer',
              content: item.content,
            });
          } else if (!merged) {
            skippedEnvCount++;
          }
        }
        else if (i === input.length - 1) {
          // 最后一条 environment_details → 转 developer 消息
          result.push({
            type: 'message',
            role: 'developer',
            content: item.content,
          });
        } else {
          skippedEnvCount++;
        }
        // 中间的 environment_details 且无法合并 → 跳过（信息冗余）
        continue;
      }
    }

    result.push(item);
  }

  // 防御：避免输入以 metadata-only user 消息收尾
  while (result.length > 0) {
    var tail = result[result.length - 1];
    if (tail.type === 'message' && tail.role === 'user' && _isEnvironmentDetailsOnly(_extractItemText(tail))) {
      result.pop();
      skippedEnvCount++;
      continue;
    }
    break;
  }

  // 防御：归一化后不允许完全清空 input，避免上游 missing_required_parameter
  if (result.length === 0 && input.length > 0) {
    var fallback = input[input.length - 1];
    if (fallback && fallback.type === 'function_call_output') {
      var fallbackOutput = typeof fallback.output === 'string' ? fallback.output : JSON.stringify(fallback.output || '');
      result.push(Object.assign({}, fallback, { output: fallbackOutput }));
    } else if (fallback && fallback.type === 'function_call') {
      result.push(fallback);
    } else if (fallback && fallback.type === 'message') {
      var fallbackText = _extractItemText(fallback);
      if (fallback.role === 'user' && _isEnvironmentDetailsOnly(fallbackText)) {
        result.push({
          type: 'message',
          role: 'developer',
          content: fallback.content || [{ type: 'input_text', text: fallbackText }],
        });
      } else if (fallbackText) {
        result.push(fallback);
      }
    }
  }

  // 防御：归一化后如果只剩 developer 元数据，补一条 user 续接消息
  var hasActionableItem = false;
  for (var ai = 0; ai < result.length; ai++) {
    var aiItem = result[ai];
    if (!aiItem) continue;
    if (aiItem.type === 'function_call' || aiItem.type === 'function_call_output') {
      hasActionableItem = true;
      break;
    }
    if (aiItem.type === 'message' && aiItem.role !== 'developer') {
      var aiText = _extractItemText(aiItem);
      if ((aiText && aiText.trim()) || (Array.isArray(aiItem.content) && aiItem.content.length > 0)) {
        hasActionableItem = true;
        break;
      }
    }
  }
  if (!hasActionableItem && result.length > 0) {
    result.push({
      type: 'message',
      role: 'user',
      content: [{ type: 'input_text', text: 'Please continue the conversation.' }],
    });
  }

  if (result.length === 0) {
    result.push({
      type: 'message',
      role: 'user',
      content: [{ type: 'input_text', text: 'Please continue the conversation.' }],
    });
  }

  if (envTagCount > 0 || envDetailsCount > 0 || extractedUserMsgCount > 0) {
    dlog('[NORMALIZE] roo normalization: env_tag=' + envTagCount
      + ' env_only=' + envDetailsCount
      + ' env_tag_not_only=' + envTagButNotOnlyCount
      + ' merged=' + mergedEnvCount
      + ' skipped=' + skippedEnvCount
      + ' extracted_user_message=' + extractedUserMsgCount
      + ' input ' + input.length + ' -> ' + result.length);
  }

  return result;
}

function _collectNormalizeStats(input) {
  var stats = {
    total: Array.isArray(input) ? input.length : 0,
    envTagCount: 0,
    envOnlyCount: 0,
    envOnlyIndexes: [],
    userMessageTagCount: 0,
    userMessageTagIndexes: [],
    lastSummary: 'none',
  };
  if (!Array.isArray(input) || input.length === 0) return stats;

  for (var i = 0; i < input.length; i++) {
    var item = input[i];
    if (!item) continue;
    if (item.type === 'message' && item.role === 'user') {
      var userText = _extractItemText(item);
      if (userText && userText.indexOf('<environment_details>') !== -1) {
        stats.envTagCount++;
      }
      if (_isEnvironmentDetailsOnly(userText)) {
        stats.envOnlyCount++;
        if (stats.envOnlyIndexes.length < 10) stats.envOnlyIndexes.push(i);
      }
    }
    if (item.type === 'function_call_output' && item.output) {
      var outputText = typeof item.output === 'string' ? item.output : JSON.stringify(item.output);
      if (outputText && outputText.indexOf('<user_message>') !== -1) {
        stats.userMessageTagCount++;
        if (stats.userMessageTagIndexes.length < 10) stats.userMessageTagIndexes.push(i);
      }
    }
  }

  var last = input[input.length - 1] || {};
  var lastText = '';
  if (last.type === 'message') {
    lastText = _extractItemText(last);
  } else if (last.type === 'function_call_output' && last.output) {
    lastText = typeof last.output === 'string' ? last.output : JSON.stringify(last.output);
  }
  lastText = (lastText || '').replace(/\s+/g, ' ').trim();
  if (lastText.length > 120) lastText = lastText.slice(0, 120) + '...';
  stats.lastSummary = 'type=' + (last.type || '-')
    + ',role=' + (last.role || '-')
    + ',text="' + lastText + '"';
  return stats;
}

function _hasNonDeveloperMessage(input) {
  if (!Array.isArray(input) || input.length === 0) return false;
  for (var i = 0; i < input.length; i++) {
    var item = input[i];
    if (!item || item.type !== 'message') continue;
    if (item.role === 'developer') continue;
    var text = _extractItemText(item);
    if (text && text.trim()) return true;
    if (Array.isArray(item.content) && item.content.length > 0) return true;
  }
  return false;
}

/**
 * 判断文本是否纯 <environment_details> 内容
 */
function _isEnvironmentDetailsOnly(text) {
  if (!text) return false;
  var trimmed = text.trim();
  if (!trimmed.startsWith('<environment_details>')) return false;

  var closeTag = '</environment_details>';
  var closeIdx = trimmed.indexOf(closeTag);
  // 兼容截断消息：未闭合仍按纯 environment_details 处理
  if (closeIdx === -1) return true;

  var tail = trimmed.slice(closeIdx + closeTag.length).trim();
  if (!tail) return true;

  // 兼容尾部系统标签（非用户输入）
  while (tail) {
    var withoutBlockReminder = tail.replace(/^<system-reminder\b[^>]*>[\s\S]*?<\/system-reminder>\s*/i, '');
    if (withoutBlockReminder !== tail) {
      tail = withoutBlockReminder.trim();
      continue;
    }

    var withoutSelfClosingReminder = tail.replace(/^<system-reminder\b[^>]*\/>\s*/i, '');
    if (withoutSelfClosingReminder !== tail) {
      tail = withoutSelfClosingReminder.trim();
      continue;
    }

    break;
  }

  return tail.length === 0;
}

/**
 * 从 input item 中提取文本内容
 */
function _extractItemText(item) {
  if (!item || !item.content) return '';
  if (typeof item.content === 'string') return item.content;
  if (typeof item.content === 'object' && !Array.isArray(item.content)) {
    if (item.content.text) return String(item.content.text);
    return '';
  }
  if (Array.isArray(item.content)) {
    var text = '';
    for (var j = 0; j < item.content.length; j++) {
      if (typeof item.content[j] === 'string') {
        text += item.content[j];
        continue;
      }
      if (item.content[j] && item.content[j].text) {
        text += item.content[j].text;
      }
    }
    return text;
  }
  return '';
}

/**
 * 规范化 tool_choice，兼容不同客户端形态
 * 支持:
 *   - "auto" | "none" | "required" | "any"
 *   - { type: "auto"|"none"|"required"|"any" }
 *   - { type: "function", function: { name } } / { type: "function", name }
 *   - { type: "tool", name }（Anthropic 风格）
 */
function normalizeToolChoice(toolChoice) {
  if (!toolChoice) return 'auto';

  if (typeof toolChoice === 'string') {
    var s = toolChoice.toLowerCase();
    if (s === 'auto' || s === 'none' || s === 'required') return s;
    if (s === 'any') return 'required';
    var builtinFromString = coerceBuiltinToolChoiceType(s);
    if (builtinFromString) {
      return { type: builtinFromString };
    }
    return 'auto';
  }

  if (typeof toolChoice !== 'object') return 'auto';

  var type = (typeof toolChoice.type === 'string' ? toolChoice.type.toLowerCase() : '');
  if (type === 'auto' || type === 'none' || type === 'required') return type;
  if (type === 'any') return 'required';

  var name = '';
  if (type === 'function') {
    name = toolChoice.name
      || (toolChoice.function && toolChoice.function.name)
      || (toolChoice.tool && toolChoice.tool.name)
      || '';
    if (name) {
      return { type: 'function', name: name };
    }
    return 'required';
  }

  if (type === 'tool') {
    name = toolChoice.name
      || (toolChoice.function && toolChoice.function.name)
      || (toolChoice.tool && toolChoice.tool.name)
      || '';
    if (name) {
      var builtinFromToolName = coerceBuiltinToolChoiceType(name);
      if (builtinFromToolName) {
        return { type: builtinFromToolName };
      }
      return { type: 'function', name: name };
    }
    return 'required';
  }

  var builtinFromType = coerceBuiltinToolChoiceType(type);
  if (builtinFromType) {
    return { type: builtinFromType };
  }

  if (toolChoice.function && toolChoice.function.name) {
    return { type: 'function', name: toolChoice.function.name };
  }

  if (toolChoice.name && typeof toolChoice.name === 'string') {
    var builtinFromName = coerceBuiltinToolChoiceType(toolChoice.name);
    if (builtinFromName) {
      return { type: builtinFromName };
    }
  }

  return 'auto';
}

/**
 * 将 tool_choice 约束为当前 tools 中实际可用的项，避免上游报 tool_choice 不存在
 */
function sanitizeToolChoiceForTools(toolChoice, tools) {
  if (!toolChoice) return 'auto';

  if (!Array.isArray(tools) || tools.length === 0) {
    if (typeof toolChoice === 'string') {
      return toolChoice === 'none' ? 'none' : 'auto';
    }
    return 'auto';
  }

  if (typeof toolChoice === 'string') {
    return toolChoice;
  }

  if (typeof toolChoice !== 'object') return 'auto';

  var type = typeof toolChoice.type === 'string' ? toolChoice.type : '';
  if (!type) return 'auto';

  if (type === 'function') {
    var targetName = toolChoice.name || '';
    if (!targetName) return 'auto';
    for (var i = 0; i < tools.length; i++) {
      if (tools[i] && tools[i].type === 'function' && tools[i].name === targetName) {
        return { type: 'function', name: targetName };
      }
    }
    return 'auto';
  }

  for (var j = 0; j < tools.length; j++) {
    if (tools[j] && tools[j].type === type) {
      return { type: type };
    }
  }
  return 'auto';
}

/**
 * 归一化 built-in 工具类型（同类工具别名映射）
 */
function normalizeBuiltinToolType(rawType) {
  if (!rawType || typeof rawType !== 'string') return '';
  var t = rawType.toLowerCase();

  // ChatGPT Backend 使用 'web_search'（不是 OpenAI API 的 'web_search_preview'）
  if (t.indexOf('web_search') === 0) return 'web_search';
  if (t.indexOf('file_search') === 0) return 'file_search';
  if (t.indexOf('computer_use') === 0 || t.indexOf('computer_') === 0) return 'computer_use_preview';
  // Responses API 里 code_interpreter_tool_call 是输出项类型，tool_choice/tools 统一收敛到 code_interpreter。
  if (t.indexOf('code_interpreter') === 0) return 'code_interpreter';
  if (t.indexOf('mcp') === 0) return 'mcp';
  if (t.indexOf('image_generation') === 0) return 'image_generation';
  if (t === 'local_shell') return 'local_shell';
  if (t === 'custom') return 'custom';

  return rawType;
}

/**
 * 归一化并校验 built-in tool type，无法识别时返回空字符串
 */
function coerceBuiltinToolChoiceType(rawType) {
  var normalized = normalizeBuiltinToolType(rawType);
  var supportedBuiltinTypes = {
    web_search: 1,
    file_search: 1,
    computer_use_preview: 1,
    code_interpreter: 1,
    mcp: 1,
    image_generation: 1,
    local_shell: 1,
    custom: 1,
  };
  if (normalized && supportedBuiltinTypes[normalized]) {
    return normalized;
  }
  return '';
}

/**
 * 转换并归一化 tools
 *
 * 支持:
 *   - OpenAI Chat function: { type: "function", function: { ... } }
 *   - Responses function:   { type: "function", name, parameters, ... }
 *   - Anthropic custom:     { name, description, input_schema }
 *   - built-in alias:       web_search、file_search、computer_use、code_interpreter、mcp、image_generation、local_shell 系列
 */
function isLikelyEmptyToolSchema(schema) {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) return true;
  var keys = Object.keys(schema);
  if (keys.length === 0) return true;
  var properties = schema.properties;
  var propertyCount = (properties && typeof properties === 'object' && !Array.isArray(properties))
    ? Object.keys(properties).length
    : 0;
  var requiredCount = Array.isArray(schema.required) ? schema.required.length : 0;
  if (propertyCount === 0 && requiredCount === 0) {
    var meaningfulKeys = 0;
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      if (key === 'type' || key === 'properties' || key === 'required' || key === 'additionalProperties' || key === 'title' || key === 'description') {
        continue;
      }
      meaningfulKeys++;
    }
    if (meaningfulKeys === 0) return true;
  }
  return false;
}

function formatTools(tools) {
  if (!tools || tools.length === 0) return [];

  var result = [];
  for (var i = 0; i < tools.length; i++) {
    var tool = tools[i];
    var before = safeStringify(tool);
    if (!tool) {
      dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=null reason=empty');
      continue;
    }

    // 允许字符串形式的 built-in 工具
    if (typeof tool === 'string') {
      var stringToolType = normalizeBuiltinToolType(tool);
      if (stringToolType) {
        var normalizedStringTool = { type: stringToolType };
        if (stringToolType === 'web_search') normalizedStringTool.external_web_access = true;
        result.push(normalizedStringTool);
        dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=' + safeStringify(normalizedStringTool));
      } else {
        dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=null reason=unsupported_string_tool');
      }
      continue;
    }

    // function 工具（OpenAI Chat / Responses / Anthropic custom）
    var fnName = '';
    var fnDesc = '';
    var fnParams = null;
    var fnStrict = false;

    if (tool.type === 'function' && tool.function) {
      fnName = tool.function.name || '';
      fnDesc = tool.function.description || '';
      fnParams = tool.function.parameters;
      fnStrict = !!tool.function.strict;
    } else if (tool.type === 'function') {
      fnName = tool.name || '';
      fnDesc = tool.description || '';
      fnParams = tool.parameters;
      fnStrict = !!tool.strict;
    } else if (!tool.type && tool.name && (tool.input_schema || tool.parameters || tool.description)) {
      fnName = tool.name || '';
      fnDesc = tool.description || '';
      fnParams = tool.input_schema || tool.parameters;
      fnStrict = !!tool.strict;
    }

    if (fnName) {
      var fnBuiltinType = normalizeBuiltinToolType(fnName);
      if (fnBuiltinType === 'web_search' && isLikelyEmptyToolSchema(fnParams)) {
        var webSearchFromFunction = {
          type: 'web_search',
          external_web_access: true,
        };
        var functionSourceExternalWebAccess = tool.external_web_access;
        if (functionSourceExternalWebAccess === undefined && tool.function && tool.function.external_web_access !== undefined) {
          functionSourceExternalWebAccess = tool.function.external_web_access;
        }
        if (typeof functionSourceExternalWebAccess === 'boolean') {
          webSearchFromFunction.external_web_access = functionSourceExternalWebAccess;
        }
        var functionSourceSearchContext = tool.search_context_size || (tool.function && tool.function.search_context_size);
        if (typeof functionSourceSearchContext === 'string' && functionSourceSearchContext) {
          webSearchFromFunction.search_context_size = functionSourceSearchContext;
        }
        var functionSourceUserLocation = tool.user_location || (tool.function && tool.function.user_location);
        if (functionSourceUserLocation && typeof functionSourceUserLocation === 'object' && !Array.isArray(functionSourceUserLocation)) {
          webSearchFromFunction.user_location = functionSourceUserLocation;
        }
        result.push(webSearchFromFunction);
        dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=' + safeStringify(webSearchFromFunction) + ' note=function_like_web_search_to_builtin');
        continue;
      }
      var schemaSource = fnParams;
      if (!schemaSource || typeof schemaSource !== 'object' || Array.isArray(schemaSource)) {
        schemaSource = { type: 'object', properties: {} };
      } else if (Object.keys(schemaSource).length === 0) {
        schemaSource = { type: 'object', properties: {} };
      }
      var normalizedFn = {
        type: 'function',
        name: fnName,
        description: fnDesc,
        parameters: sanitizeJsonSchema(schemaSource),
      };
      if (fnStrict) normalizedFn.strict = true;
      result.push(normalizedFn);
      dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=' + safeStringify(normalizedFn));
      continue;
    }

    // built-in 工具（透传附加字段，仅归一化 type）
    var source = null;
    if (tool.type === 'builtin') {
      source = (tool._raw && typeof tool._raw === 'object')
        ? Object.assign({}, tool._raw)
        : {};
    } else if (tool.type && tool.type !== 'function') {
      source = Object.assign({}, tool);
    }

    if (source) {
      var builtinType = normalizeBuiltinToolType(source.type || tool.builtin_type || tool.name || tool.type);
      if (!builtinType) {
        dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=null reason=unsupported_builtin_type');
        continue;
      }
      source.type = builtinType;
      // web_search 必须带 external_web_access: true，否则后端不执行搜索
      if (builtinType === 'web_search' && source.external_web_access === undefined) {
        source.external_web_access = true;
      }
      delete source._raw;
      delete source.builtin_type;
      delete source.function;
      result.push(source);
      dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=' + safeStringify(source));
      continue;
    }

    dlog('[WEB-SEARCH] formatTools compare index=' + i + ' before=' + before + ' after=null reason=unsupported_shape');
  }

  return result;
}

/**
 * 将消息 content 转换为 Responses API content blocks
 *
 * 支持的输入格式:
 *   - string → [{ type: "input_text"|"output_text", text }]
 *   - null/undefined → []
 *   - Array<ContentBlock> → 逐个转换（text, image_url, input_audio）
 */
function convertContentToBlocks(content, role) {
  var isAssistant = role === 'assistant';
  var textType = isAssistant ? 'output_text' : 'input_text';

  if (!content) return [];

  if (typeof content === 'string') {
    if (!content) return [];
    return [{ type: textType, text: content }];
  }

  if (typeof content === 'object' && !Array.isArray(content)) {
    content = [content];
  }

  if (!Array.isArray(content)) return [];

  var blocks = [];
  for (var j = 0; j < content.length; j++) {
    var block = content[j];
    if (!block) continue;

    // 纯字符串元素
    if (typeof block === 'string') {
      blocks.push({ type: textType, text: block });
      continue;
    }

    // text content block
    if (block.type === 'text' && block.text) {
      blocks.push({ type: textType, text: block.text });
      continue;
    }

    // image_url content block → input_image
    if (block.type === 'image_url' && block.image_url) {
      blocks.push({
        type: 'input_image',
        image_url: block.image_url.url || block.image_url,
        detail: block.image_url.detail || 'auto',
      });
      continue;
    }

    // input_audio content block（Responses API 支持）
    if (block.type === 'input_audio' && block.input_audio) {
      blocks.push({
        type: 'input_audio',
        data: block.input_audio.data,
        format: block.input_audio.format || 'wav',
      });
      continue;
    }

    var fallbackText = JSON.stringify(block);
    if (fallbackText === undefined) fallbackText = String(block);
    wlog('unknown content block type, fallback to input_text: ' + (block.type || typeof block));
    blocks.push({
      type: 'input_text',
      text: fallbackText,
    });
  }
  return blocks;
}

/**
 * 从 content 中提取纯文本
 */
function extractText(content) {
  if (!content) return '';
  if (typeof content === 'string') return content;
  if (typeof content === 'object' && !Array.isArray(content)) {
    if ((content.type === 'text' || content.type === 'output_text' || content.type === 'input_text') && content.text) {
      return String(content.text);
    }
    return '';
  }
  if (Array.isArray(content)) {
    var parts = [];
    for (var i = 0; i < content.length; i++) {
      if (typeof content[i] === 'string') {
        parts.push(content[i]);
      } else if (
        content[i]
        && (content[i].type === 'text' || content[i].type === 'output_text' || content[i].type === 'input_text')
        && content[i].text
      ) {
        parts.push(content[i].text);
      }
    }
    return parts.join('\n');
  }
  return '';
}

/**
 * 归一化 SSE 文本片段为字符串
 * 兼容上游字段偶发返回 object/array 结构，避免 content 丢失
 */
function normalizeSSETextChunk(value) {
  if (typeof value === 'string') return value;
  if (value === undefined || value === null) return '';
  if (Array.isArray(value)) {
    var parts = [];
    for (var i = 0; i < value.length; i++) {
      var partText = normalizeSSETextChunk(value[i]);
      if (partText) parts.push(partText);
    }
    return parts.join('');
  }
  if (typeof value === 'object') {
    if (typeof value.text === 'string') return value.text;
    if (typeof value.delta === 'string') return value.delta;
    if (typeof value.value === 'string') return value.value;
    if (Array.isArray(value.content)) return normalizeSSETextChunk(value.content);
    return '';
  }
  return String(value);
}

/**
 * 从输出文本相关 SSE payload 提取文本，按字段优先级兜底
 */
function extractOutputTextFromSSEPayload(payload, preferDeltaField) {
  if (!payload || typeof payload !== 'object') return '';
  var candidates = preferDeltaField
    ? [payload.delta, payload.text, payload.part && payload.part.text]
    : [payload.text, payload.delta, payload.part && payload.part.text];
  for (var i = 0; i < candidates.length; i++) {
    var text = normalizeSSETextChunk(candidates[i]);
    if (text) return text;
  }
  return '';
}

/**
 * 解析 Codex Responses SSE 事件 → Universal 流式事件
 *
 *
 * 生命周期事件:
 *   - response.created → start
 *   - response.in_progress → (ignored)
 *   - response.completed → done (含 usage)
 *   - response.failed → error
 *   - response.incomplete → done (finish_reason: length)
 *
 * Output item 事件:
 *   - response.output_item.added → 新输出项开始（message/function_call）
 *   - response.output_item.done → 输出项完成
 *
 * Content part 事件:
 *   - response.content_part.added → 内容块开始
 *   - response.content_part.done → 内容块完成
 *
 * 文本事件:
 *   - response.output_text.delta → delta（文本增量）
 *   - response.output_text.done → 兜底 delta（仅在有未发送文本时）
 *   - response.output_text.annotation.added → annotation（URL 引用等）
 *
 * 推理事件:
 *   - response.reasoning.delta → reasoning（推理增量，明文可读时透传）
 *   - response.reasoning.done → reasoning（兜底补发未下发部分）
 *   - response.reasoning_summary_text.delta → reasoning（推理摘要增量）
 *   - response.reasoning_summary_text.done → (ignored)
 *
 * Function call 事件:
 *   - response.function_call_arguments.delta → tool_call（参数增量）
 *   - response.function_call_arguments.done → tool_call（参数完成）
 *
 * 错误事件:
 *   - error → error
 *
 * @param {string} eventType - SSE event 字段
 * @param {object} data - 解析后的 JSON
 * @param {object} [state] - 跨事件状态（用于追踪 function call 元信息）
 * @returns {UniversalStreamEvent|UniversalStreamEvent[]|null}
 */
function shouldLogWebSearchSSEEvent(type, data) {
  if (!type || typeof type !== 'string') return false;
  if (type === 'response.output_item.added' || type === 'response.output_item.done') return true;
  if (type === 'response.function_call_arguments.delta' || type === 'response.function_call_arguments.done') return true;
  if (type.indexOf('response.web_search_call.') === 0) return true;
  var itemType = data && data.item && data.item.type ? String(data.item.type) : '';
  if (itemType && itemType.indexOf('web_search') === 0) return true;
  return false;
}

export function parseSSEEvent(eventType, data, state) {
  if (!data) return null;
  var type = data.type || eventType;
  if (shouldLogWebSearchSSEEvent(type, data)) {
    dlog('[WEB-SEARCH] parseSSEEvent event=' + type + ' data=' + safeStringify(data));
  }

  // 初始化跨事件状态
  if (!state) state = {};
  if (!state.functionCalls) state.functionCalls = {};
  if (!state.outputTextLengthByIndex) state.outputTextLengthByIndex = {};
  if (!state.outputTextByIndex) state.outputTextByIndex = {};
  if (!state.annotationsByIndex) state.annotationsByIndex = {};
  if (!state.annotationKeys) state.annotationKeys = {};
  if (!state.webSearchCalls) state.webSearchCalls = {};
  if (typeof state.reasoningLength !== 'number') state.reasoningLength = 0;

  function resolveResponseContainer(payload) {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {};
    if (payload.response && typeof payload.response === 'object' && !Array.isArray(payload.response)) {
      return payload.response;
    }
    return payload;
  }

  function resolveUsageFromPayload(responsePayload, rawPayload) {
    var usageCandidate = null;
    if (responsePayload && typeof responsePayload === 'object' && !Array.isArray(responsePayload)) {
      if (responsePayload.usage && typeof responsePayload.usage === 'object' && !Array.isArray(responsePayload.usage)) {
        usageCandidate = responsePayload.usage;
      }
    }
    if (!usageCandidate && rawPayload && typeof rawPayload === 'object' && !Array.isArray(rawPayload)) {
      if (rawPayload.usage && typeof rawPayload.usage === 'object' && !Array.isArray(rawPayload.usage)) {
        usageCandidate = rawPayload.usage;
      }
    }
    return normalizeCollectedUsage(usageCandidate) || {
      input_tokens: 0,
      output_tokens: 0,
      cached_tokens: 0,
      reasoning_tokens: 0,
    };
  }

  function getSentTextLength(outputIndex) {
    var key = String(outputIndex || 0);
    var sentLength = state.outputTextLengthByIndex[key];
    if (typeof sentLength === 'number' && sentLength > 0) return sentLength;
    return 0;
  }

  function markSentTextLength(outputIndex, sentLength) {
    var key = String(outputIndex || 0);
    state.outputTextLengthByIndex[key] = sentLength > 0 ? sentLength : 0;
    if (sentLength > 0) {
      state.outputTextByIndex[key] = true;
    }
  }

  function buildPendingDelta(outputIndex, finalText) {
    if (!finalText || typeof finalText !== 'string') return '';
    var sentLength = getSentTextLength(outputIndex);
    if (sentLength >= finalText.length) return '';
    var pending = finalText.slice(sentLength);
    if (pending.length > 0) {
      markSentTextLength(outputIndex, finalText.length);
    }
    return pending;
  }

  function readString(value) {
    if (value === undefined || value === null) return '';
    if (typeof value === 'string') return value.trim();
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    return '';
  }

  function getSentReasoningLength() {
    var sentLength = state.reasoningLength;
    if (typeof sentLength === 'number' && sentLength > 0) return sentLength;
    return 0;
  }

  function markSentReasoningLength(sentLength) {
    state.reasoningLength = sentLength > 0 ? sentLength : 0;
  }

  function buildPendingReasoning(finalText) {
    if (!finalText || typeof finalText !== 'string') return '';
    var sentLength = getSentReasoningLength();
    if (sentLength >= finalText.length) return '';
    var pending = finalText.slice(sentLength);
    if (pending.length > 0) {
      markSentReasoningLength(finalText.length);
    }
    return pending;
  }

  function extractReasoningSummaryText(payload) {
    if (!payload || typeof payload !== 'object') return '';
    var parts = [];

    function pushPart(value) {
      var text = normalizeSSETextChunk(value);
      if (text) parts.push(text);
    }

    function collect(entry) {
      if (!entry) return;
      if (typeof entry === 'string' || typeof entry === 'number' || typeof entry === 'boolean') {
        pushPart(entry);
        return;
      }
      if (Array.isArray(entry)) {
        for (var i = 0; i < entry.length; i++) collect(entry[i]);
        return;
      }
      if (typeof entry !== 'object') return;
      if (entry.type === 'summary_text') {
        pushPart(entry.text);
        pushPart(entry.summary_text);
        return;
      }
      pushPart(entry.text);
      pushPart(entry.summary_text);
      if (Array.isArray(entry.summary)) collect(entry.summary);
      if (Array.isArray(entry.content)) collect(entry.content);
    }

    collect(payload.summary);
    if (parts.length === 0) {
      collect(payload.content);
    }
    if (parts.length === 0) {
      collect(payload.text);
    }
    return parts.join('\n');
  }

  function looksEncryptedReasoningText(text) {
    if (!text || typeof text !== 'string') return false;
    var compact = text.replace(/\s+/g, '');
    if (compact.length < 80) return false;
    if (!/^[A-Za-z0-9+/=]+$/.test(compact)) return false;
    // 近似判定: 长串 base64 且无自然语言分隔符，按加密内容忽略
    if (text.indexOf(' ') !== -1 || text.indexOf('\n') !== -1 || text.indexOf('\t') !== -1) return false;
    return true;
  }

  function sanitizeReasoningText(text) {
    if (!text) return '';
    if (looksEncryptedReasoningText(text)) return '';
    return text;
  }

  function extractReasoningText(payload, preferDeltaField) {
    if (!payload || typeof payload !== 'object') return '';
    var candidates = preferDeltaField
      ? [payload.delta, payload.reasoning, payload.summary, payload.text]
      : [payload.text, payload.reasoning, payload.summary, payload.delta];

    for (var i = 0; i < candidates.length; i++) {
      var candidate = candidates[i];
      var text = sanitizeReasoningText(normalizeSSETextChunk(candidate));
      if (text) return text;
      var summaryText = sanitizeReasoningText(extractReasoningSummaryText(candidate));
      if (summaryText) return summaryText;
    }

    return sanitizeReasoningText(extractReasoningSummaryText(payload));
  }

  function normalizeAnnotationIndex(value, fallback) {
    var defaultIndex = (typeof fallback === 'number' && fallback >= 0) ? fallback : 0;
    if (value === undefined || value === null || value === '') return defaultIndex;
    var index = Number(value);
    if (!Number.isFinite(index) || index < 0) return defaultIndex;
    return Math.floor(index);
  }

  function getAnnotationField(annotation, field) {
    if (!annotation || typeof annotation !== 'object') return '';
    var value = annotation[field];
    if (value !== undefined && value !== null && value !== '') return value;
    var nested = annotation.url_citation;
    if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
      var nestedValue = nested[field];
      if (nestedValue !== undefined && nestedValue !== null && nestedValue !== '') {
        return nestedValue;
      }
    }
    return '';
  }

  function buildAnnotationKey(outputIndex, annotation) {
    var keyParts = [];
    var fields = ['type', 'url', 'title', 'content', 'start_index', 'end_index'];
    for (var i = 0; i < fields.length; i++) {
      var value = getAnnotationField(annotation, fields[i]);
      if (value !== undefined && value !== null && value !== '') {
        keyParts.push(fields[i] + ':' + String(value));
      }
    }
    if (keyParts.length > 0) {
      return String(outputIndex || 0) + '|' + keyParts.join('|');
    }
    var fallback = '';
    try {
      fallback = JSON.stringify(annotation);
    } catch (_) {
      fallback = String(annotation.type || 'annotation');
    }
    return String(outputIndex || 0) + '|' + fallback;
  }

  function normalizeAnnotation(annotation) {
    if (!annotation || typeof annotation !== 'object' || Array.isArray(annotation)) return null;
    var normalized = {};
    var keys = Object.keys(annotation);
    for (var i = 0; i < keys.length; i++) {
      normalized[keys[i]] = annotation[keys[i]];
    }

    var annotationType = readString(normalized.type);
    var hasUrlCitation = normalized.url_citation
      && typeof normalized.url_citation === 'object'
      && !Array.isArray(normalized.url_citation);
    if (annotationType === 'url_citation' || hasUrlCitation) {
      var citation = hasUrlCitation ? normalized.url_citation : normalized;
      var citationUrl = readString(citation.url || normalized.url);
      if (!citationUrl) return null;
      var citationTitle = readString(citation.title || normalized.title || citation.name || normalized.name || citationUrl);
      var citationContent = readString(
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

  function rememberAnnotation(outputIndex, annotation) {
    var normalized = normalizeAnnotation(annotation);
    if (!normalized) return null;
    var dedupeKey = buildAnnotationKey(outputIndex, normalized);
    if (state.annotationKeys[dedupeKey]) return null;
    state.annotationKeys[dedupeKey] = true;
    var idxKey = String(outputIndex || 0);
    if (!state.annotationsByIndex[idxKey]) state.annotationsByIndex[idxKey] = [];
    state.annotationsByIndex[idxKey].push(normalized);
    return normalized;
  }

  function buildAnnotationEvents(annotations) {
    if (!Array.isArray(annotations) || annotations.length === 0) return null;
    if (annotations.length === 1) {
      return createStreamEvent('annotation', { annotation: annotations[0] });
    }
    var events = [];
    for (var i = 0; i < annotations.length; i++) {
      events.push(createStreamEvent('annotation', { annotation: annotations[i] }));
    }
    return events;
  }

  function extractWebSearchQueries(payload) {
    if (!payload || typeof payload !== 'object') return [];
    var collected = [];
    function appendQuery(value) {
      var text = readString(value);
      if (!text) return;
      if (collected.indexOf(text) === -1) {
        collected.push(text);
      }
    }
    if (Array.isArray(payload.queries)) {
      for (var i = 0; i < payload.queries.length; i++) {
        appendQuery(payload.queries[i]);
      }
    }
    appendQuery(payload.query);
    return collected;
  }

  function updateWebSearchMeta(itemId, outputIndex, payload, status) {
    var key = readString(itemId);
    if (!key) return null;
    var meta = state.webSearchCalls[key] || {
      id: key,
      output_index: outputIndex || 0,
      status: '',
      query: '',
      queries: [],
    };
    if (typeof outputIndex === 'number' && outputIndex >= 0) {
      meta.output_index = outputIndex;
    }
    var effectiveStatus = readString(status || (payload && payload.status));
    if (effectiveStatus) meta.status = effectiveStatus;
    if (payload && typeof payload === 'object') {
      var action = payload.action && typeof payload.action === 'object' ? payload.action : payload;
      var queries = extractWebSearchQueries(action);
      if (queries.length > 0) {
        meta.queries = queries;
        if (!meta.query) meta.query = queries[0];
      }
      var query = readString(action.query || payload.query);
      if (query) meta.query = query;
      if (action && action !== payload) {
        var actionQueries = extractWebSearchQueries(payload);
        if (actionQueries.length > 0 && (!meta.queries || meta.queries.length === 0)) {
          meta.queries = actionQueries;
          if (!meta.query) meta.query = actionQueries[0];
        }
      }
    }
    state.webSearchCalls[key] = meta;
    return meta;
  }

  function extractSnippet(value, depth) {
    if (depth > 4 || value === undefined || value === null) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    if (Array.isArray(value)) {
      var parts = [];
      for (var i = 0; i < value.length; i++) {
        var part = extractSnippet(value[i], depth + 1);
        if (part) parts.push(part);
      }
      return parts.join('\n');
    }
    if (typeof value !== 'object') return '';
    if (typeof value.text === 'string') return value.text;
    if (typeof value.snippet === 'string') return value.snippet;
    if (typeof value.summary === 'string') return value.summary;
    if (typeof value.description === 'string') return value.description;
    if (typeof value.content === 'string') return value.content;
    if (typeof value.excerpt === 'string') return value.excerpt;
    if (typeof value.encrypted_content === 'string') return value.encrypted_content;
    if (Array.isArray(value.content)) return extractSnippet(value.content, depth + 1);
    return '';
  }

  function pushWebSearchCitation(sink, candidate) {
    if (!sink || !candidate || typeof candidate !== 'object') return;
    var source = candidate.source && typeof candidate.source === 'object' ? candidate.source : null;
    var url = readString(
      candidate.url
      || (source && source.url)
      || (candidate.link && candidate.link.url)
      || (candidate.url_citation && candidate.url_citation.url)
    );
    if (!url) return;
    var title = readString(
      candidate.title
      || candidate.name
      || (source && (source.title || source.name))
      || (candidate.link && candidate.link.title)
      || (candidate.url_citation && candidate.url_citation.title)
      || url
    );
    var snippet = readString(
      candidate.content
      || candidate.snippet
      || candidate.summary
      || candidate.description
      || candidate.excerpt
      || candidate.encrypted_content
      || (candidate.url_citation && candidate.url_citation.content)
    );
    if (!snippet) {
      snippet = extractSnippet(candidate.content, 0) || extractSnippet(candidate.data, 0);
    }
    var annotation = normalizeAnnotation({
      type: 'url_citation',
      url: url,
      title: title || url,
      content: snippet || '',
      start_index: candidate.start_index,
      end_index: candidate.end_index,
    });
    if (annotation) sink.push(annotation);
  }

  function collectWebSearchCandidates(payload, sink, depth) {
    if (!payload || depth > 6) return;
    if (Array.isArray(payload)) {
      for (var i = 0; i < payload.length; i++) {
        collectWebSearchCandidates(payload[i], sink, depth + 1);
      }
      return;
    }
    if (typeof payload !== 'object') return;

    if (payload.type === 'url_citation' || payload.url_citation) {
      pushWebSearchCitation(sink, payload);
    }

    if (payload.type === 'web_search_result') {
      pushWebSearchCitation(sink, payload);
    } else if (payload.type === 'search_result') {
      if (payload.source && typeof payload.source === 'object') {
        pushWebSearchCitation(sink, {
          source: payload.source,
          content: payload.content,
          snippet: extractSnippet(payload.content, 0),
          start_index: payload.start_index,
          end_index: payload.end_index,
        });
      }
    } else if (payload.url || (payload.source && payload.source.url) || (payload.link && payload.link.url)) {
      pushWebSearchCitation(sink, payload);
    }

    var nestedFields = [
      'action',
      'data',
      'content',
      'results',
      'sources',
      'search_results',
      'web_results',
      'items',
      'hits',
      'documents',
      'references',
      'citations',
      'annotations',
      'annotation',
    ];
    for (var f = 0; f < nestedFields.length; f++) {
      var field = nestedFields[f];
      if (payload[field] !== undefined && payload[field] !== null) {
        collectWebSearchCandidates(payload[field], sink, depth + 1);
      }
    }
  }

  function rememberWebSearchAnnotations(outputIndex, payload) {
    var extracted = [];
    collectWebSearchCandidates(payload, extracted, 0);
    if (extracted.length === 0) return [];
    var added = [];
    for (var i = 0; i < extracted.length; i++) {
      var remembered = rememberAnnotation(outputIndex, extracted[i]);
      if (remembered) added.push(remembered);
    }
    return added;
  }

  function buildHostTitleFromUrl(url) {
    var normalizedUrl = readString(url);
    if (!normalizedUrl) return '';
    try {
      var parsed = new URL(normalizedUrl);
      var host = readString(parsed.hostname).replace(/^www\./i, '');
      if (!host) return normalizedUrl;
      return host;
    } catch (_) {
      return normalizedUrl;
    }
  }

  function normalizeCitationLine(line) {
    var text = readString(line);
    if (!text) return '';
    text = text.replace(/^\s*[-*]\s*/, '');
    text = text.replace(/^\s*\d+[\.\)\-]\s*/, '');
    text = text.replace(/\*\*/g, '');
    text = text.replace(/`/g, '');
    return text.trim();
  }

  function cleanupExtractedUrl(rawUrl) {
    var cleaned = readString(rawUrl);
    if (!cleaned) return '';
    return cleaned.replace(/[),.;!?]+$/, '');
  }

  function pickCitationTitleFromContext(lines, lineIndex, lineText, url, explicitTitle) {
    var title = normalizeCitationLine(explicitTitle);
    if (title) return title;

    var currentLineTitle = normalizeCitationLine(
      readString(lineText).replace(url, '').replace(/^来源[:：]\s*/i, '')
    );
    if (currentLineTitle && currentLineTitle.indexOf('http://') === -1 && currentLineTitle.indexOf('https://') === -1) {
      return currentLineTitle;
    }

    for (var i = lineIndex - 1; i >= 0 && i >= lineIndex - 3; i--) {
      var prev = normalizeCitationLine(lines[i]);
      if (!prev) continue;
      if (prev.indexOf('http://') !== -1 || prev.indexOf('https://') !== -1) continue;
      if (/^来源[:：]/i.test(prev)) {
        var sourceTitle = normalizeCitationLine(prev.replace(/^来源[:：]\s*/i, ''));
        if (sourceTitle) return sourceTitle;
        continue;
      }
      return prev;
    }
    return buildHostTitleFromUrl(url);
  }

  function pickCitationSnippetFromContext(lines, lineIndex, lineText, url) {
    var snippets = [];
    for (var i = lineIndex - 2; i <= lineIndex + 1; i++) {
      if (i < 0 || i >= lines.length) continue;
      var part = normalizeCitationLine(readString(lines[i]).replace(url, ''));
      if (!part) continue;
      if (/^来源[:：]/i.test(part)) continue;
      if (snippets.indexOf(part) === -1) {
        snippets.push(part);
      }
    }
    if (snippets.length > 0) {
      return snippets.join(' ').slice(0, 320);
    }
    return normalizeCitationLine(readString(lineText).replace(url, ''));
  }

  function extractTextCitations(text) {
    var rawText = readString(text);
    if (!rawText) return [];
    var lines = rawText.split('\n');
    var citations = [];
    var seen = {};
    var markdownLinkRegex = /\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g;
    var urlRegex = /https?:\/\/[^\s<>"\]\)}]+/g;

    for (var lineIndex = 0; lineIndex < lines.length; lineIndex++) {
      var line = lines[lineIndex] || '';
      var markdownTitleByUrl = {};
      var markdownMatch = null;
      markdownLinkRegex.lastIndex = 0;
      while ((markdownMatch = markdownLinkRegex.exec(line)) !== null) {
        var mdUrl = cleanupExtractedUrl(markdownMatch[2]);
        if (!mdUrl) continue;
        markdownTitleByUrl[mdUrl] = normalizeCitationLine(markdownMatch[1]);
      }

      var urlMatch = null;
      urlRegex.lastIndex = 0;
      while ((urlMatch = urlRegex.exec(line)) !== null) {
        var url = cleanupExtractedUrl(urlMatch[0]);
        if (!url || seen[url]) continue;
        seen[url] = true;
        var citationTitle = pickCitationTitleFromContext(
          lines,
          lineIndex,
          line,
          url,
          markdownTitleByUrl[url] || ''
        );
        var citationSnippet = pickCitationSnippetFromContext(lines, lineIndex, line, url);
        var citation = normalizeAnnotation({
          type: 'url_citation',
          url: url,
          title: citationTitle || buildHostTitleFromUrl(url),
          content: citationSnippet || '',
          start_index: 0,
          end_index: 0,
        });
        if (citation) citations.push(citation);
      }
    }
    return citations;
  }

  function rememberTextCitations(outputIndex, text) {
    var extracted = extractTextCitations(text);
    if (extracted.length === 0) return [];
    var added = [];
    for (var i = 0; i < extracted.length; i++) {
      var remembered = rememberAnnotation(outputIndex, extracted[i]);
      if (remembered) added.push(remembered);
    }
    return added;
  }

  function appendContentBlockAnnotations(outputIndex, contentBlocks) {
    if (!Array.isArray(contentBlocks)) return;
    for (var i = 0; i < contentBlocks.length; i++) {
      var block = contentBlocks[i];
      if (!block || typeof block !== 'object') continue;
      if (Array.isArray(block.annotations)) {
        for (var a = 0; a < block.annotations.length; a++) {
          rememberAnnotation(outputIndex, block.annotations[a]);
        }
      }
      if (block.annotation) {
        rememberAnnotation(outputIndex, block.annotation);
      }
    }
  }

  function collectOrderedAnnotations() {
    var keys = Object.keys(state.annotationsByIndex);
    keys.sort(function (a, b) { return Number(a) - Number(b); });
    var merged = [];
    for (var i = 0; i < keys.length; i++) {
      var list = state.annotationsByIndex[keys[i]] || [];
      for (var j = 0; j < list.length; j++) {
        merged.push(list[j]);
      }
    }
    return merged;
  }

  // ===== 生命周期事件 =====

  // 响应创建
  if (type === 'response.created') {
    var resp = data.response || {};
    return createStreamEvent('start', {
      id: resp.id,
      model: resp.model,
    });
  }

  // 响应进行中（忽略，不需要转发给客户端）
  if (type === 'response.in_progress') {
    return null;
  }

  // ===== Output item 事件 =====

  // 新输出项添加 — 追踪 function_call 元信息
  if (type === 'response.output_item.added') {
    var addedItem = data.item || {};
    // 记录 function_call 的 id 和 name，供后续 arguments.delta 使用
    if (addedItem.type === 'function_call') {
      // 上游有两个 ID: call_id (call_xxx) 和 id (fc_xxx)
      // 必须用 call_id 作为对外 ID（OpenAI 兼容），同时用 id 做查找映射
      var callId = addedItem.call_id || addedItem.id || '';
      var itemId = addedItem.id || '';
      var meta = {
        id: callId,
        name: addedItem.name || '',
        index: data.output_index || 0,
        argumentsDone: false,
        // 标记是否已通过 delta 流输出过参数，避免 done 再次发送完整 arguments 导致客户端拼接出非法 JSON。
        argumentsDeltaSeen: false,
      };
      state.functionCalls[callId] = meta;
      // 同时用 item.id 映射，确保 arguments.delta/done 能找到（它们可能用 item_id/call_id 的不同字段）
      if (itemId && itemId !== callId) {
        state.functionCalls[itemId] = meta;
      }
      // 发送 tool_call 开始事件（含 id 和 name，无参数）
      return createStreamEvent('tool_call', {
        tool_call: {
          id: callId,
          name: addedItem.name || '',
          index: data.output_index || 0,
          arguments_delta: '',
        },
      });
    }
    if (addedItem.type === 'web_search_call') {
      updateWebSearchMeta(
        addedItem.id || data.item_id || '',
        data.output_index || 0,
        addedItem,
        addedItem.status || 'in_progress'
      );
    }
    return null;
  }

  // 输出项完成 — 兜底提取完整内容（防止增量事件丢失时客户端收不到内容）
  if (type === 'response.output_item.done') {
    var doneItem = data.item || {};
    // function_call 完成 — 兜底发送完整 tool_call
    if (doneItem.type === 'function_call') {
      var doneCallId = doneItem.call_id || doneItem.id || '';
      var doneMeta = state.functionCalls[doneCallId]
        || (doneItem.id ? state.functionCalls[doneItem.id] : null)
        || (doneItem.call_id ? state.functionCalls[doneItem.call_id] : null)
        || {};
      // 已有 function_call_arguments.done 时不重复发
      if (doneMeta.argumentsDone) {
        return null;
      }
      doneMeta.argumentsDone = true;
      if (doneCallId) state.functionCalls[doneCallId] = doneMeta;
      if (doneItem.id) state.functionCalls[doneItem.id] = doneMeta;
      if (doneItem.call_id) state.functionCalls[doneItem.call_id] = doneMeta;
      var doneCanonicalId = doneMeta.id || doneCallId;
      dlog('fallback output_item.done -> tool_call (' + doneCanonicalId + ')');
      return createStreamEvent('tool_call', {
        tool_call: {
          id: doneCanonicalId,
          name: doneMeta.name || doneItem.name || '',
          index: doneMeta.index || data.output_index || 0,
          arguments: doneItem.arguments || '{}',
          done: true,
        },
      });
    }
    // reasoning 完成 — 兜底提取 summary 文本（部分上游不会发 reasoning_summary_text.delta）
    if (doneItem.type === 'reasoning') {
      var doneReasoningText = extractReasoningSummaryText(doneItem);
      var doneReasoningDelta = buildPendingReasoning(doneReasoningText);
      if (doneReasoningDelta) {
        dlog('fallback output_item.done -> reasoning (' + doneReasoningDelta.length + ' chars)');
        return createStreamEvent('reasoning', { reasoning: doneReasoningDelta });
      }
      return null;
    }
    if (doneItem.type === 'web_search_call') {
      var doneWebSearchOutputIndex = data.output_index || 0;
      updateWebSearchMeta(
        doneItem.id || data.item_id || '',
        doneWebSearchOutputIndex,
        doneItem,
        doneItem.status || 'completed'
      );
      var doneWebSearchAnnotations = rememberWebSearchAnnotations(doneWebSearchOutputIndex, doneItem);
      return buildAnnotationEvents(doneWebSearchAnnotations);
    }
    // message 完成 — 兜底发送文本内容
    if (doneItem.type === 'message' && doneItem.content) {
      var doneOutputIndex = data.output_index || 0;
      var doneText = extractText(doneItem.content);
      var doneTextAnnotations = rememberTextCitations(doneOutputIndex, doneText);
      var doneDelta = buildPendingDelta(doneOutputIndex, doneText);
      var doneEvents = [];
      var doneAnnotationEvents = buildAnnotationEvents(doneTextAnnotations);
      if (Array.isArray(doneAnnotationEvents)) {
        doneEvents = doneEvents.concat(doneAnnotationEvents);
      } else if (doneAnnotationEvents) {
        doneEvents.push(doneAnnotationEvents);
      }
      if (doneDelta) {
        dlog('fallback output_item.done -> delta (' + doneDelta.length + ' chars)');
        doneEvents.push(createStreamEvent('delta', { content: doneDelta }));
      }
      if (doneEvents.length === 1) return doneEvents[0];
      if (doneEvents.length > 1) return doneEvents;
      if (doneTextAnnotations.length > 0 && doneEvents.length === 0) {
        dlog('[SSE-DIAG] message done extracted annotations=' + doneTextAnnotations.length);
      }
    }
    return null;
  }

  // ===== Content part 事件 =====
  if (type === 'response.content_part.added') {
    return null;
  }

  if (type === 'response.content_part.done') {
    var partText = extractOutputTextFromSSEPayload(data, false);
    var partDelta = buildPendingDelta(data.output_index || 0, partText);
    if (partDelta) {
      return createStreamEvent('delta', { content: partDelta });
    }
    return null;
  }

  // ===== 文本事件 =====

  // 文本增量
  if (type === 'response.output_text.delta') {
    var deltaText = extractOutputTextFromSSEPayload(data, true);
    if (!deltaText) {
      dlog('[SSE-DIAG] response.output_text.delta ignored: output_index=' + (data.output_index || 0)
        + ' delta.type=' + typeof data.delta
        + ' text.type=' + typeof data.text);
      return null;
    }
    var deltaOutputIndex = data.output_index || 0;
    var sentLength = getSentTextLength(deltaOutputIndex) + deltaText.length;
    markSentTextLength(deltaOutputIndex, sentLength);
    return createStreamEvent('delta', {
      content: deltaText,
    });
  }

  // 文本完成（非空文本兜底）
  if (type === 'response.output_text.done') {
    var doneOutputIndex = data.output_index || 0;
    var doneText = extractOutputTextFromSSEPayload(data, false);
    var outputDoneDelta = buildPendingDelta(doneOutputIndex, doneText);
    if (outputDoneDelta) {
      return createStreamEvent('delta', { content: outputDoneDelta });
    }
    dlog('[SSE-DIAG] response.output_text.done ignored: output_index=' + doneOutputIndex
      + ' text.len=' + doneText.length
      + ' sent.len=' + getSentTextLength(doneOutputIndex)
      + ' data.text.type=' + typeof data.text);
    return null;
  }

  // 文本注解（URL 引用等）
  if (type === 'response.output_text.annotation.added') {
    var annotationOutputIndex = data.output_index || 0;
    var addedAnnotation = rememberAnnotation(annotationOutputIndex, data.annotation);
    if (!addedAnnotation) return null;
    return createStreamEvent('annotation', {
      annotation: addedAnnotation,
    });
  }

  // ===== Web Search 工具状态事件 =====

  if (type === 'response.web_search_call.in_progress') {
    updateWebSearchMeta(data.item_id || '', data.output_index || 0, data, 'in_progress');
    return null;
  }

  // 搜索中状态对 Chat Completions 无直接字段映射，保留为内部状态即可
  if (type === 'response.web_search_call.searching') {
    updateWebSearchMeta(data.item_id || '', data.output_index || 0, data, 'searching');
    return null;
  }

  // 搜索完成时优先提取 citation，兼容上游把结果挂在 web_search_call.completed 事件上
  if (type === 'response.web_search_call.completed') {
    var completedWebSearchOutputIndex = data.output_index || 0;
    updateWebSearchMeta(data.item_id || '', completedWebSearchOutputIndex, data, 'completed');
    var completedWebSearchAnnotations = rememberWebSearchAnnotations(completedWebSearchOutputIndex, data);
    return buildAnnotationEvents(completedWebSearchAnnotations);
  }

  // ===== 推理事件 =====

  // 推理增量（优先处理 response.reasoning.*，兼容上游新版事件）
  if (type === 'response.reasoning.delta') {
    var rawReasoningDelta = extractReasoningText(data, true);
    if (!rawReasoningDelta) {
      return null;
    }
    markSentReasoningLength(getSentReasoningLength() + rawReasoningDelta.length);
    return createStreamEvent('reasoning', {
      reasoning: rawReasoningDelta,
    });
  }

  // 推理完成（补发尚未下发部分）
  if (type === 'response.reasoning.done') {
    var rawReasoningDone = extractReasoningText(data, false);
    var pendingReasoningDone = buildPendingReasoning(rawReasoningDone);
    if (pendingReasoningDone) {
      dlog('fallback reasoning.done -> reasoning (' + pendingReasoningDone.length + ' chars)');
      return createStreamEvent('reasoning', { reasoning: pendingReasoningDone });
    }
    return null;
  }

  // 推理摘要增量
  if (type === 'response.reasoning_summary_text.delta') {
    var reasoningDelta = extractReasoningText(data, true);
    if (!reasoningDelta) {
      return null;
    }
    markSentReasoningLength(getSentReasoningLength() + reasoningDelta.length);
    return createStreamEvent('reasoning', {
      reasoning: reasoningDelta,
    });
  }

  // 推理摘要完成（兜底补发未下发的剩余摘要）
  if (type === 'response.reasoning_summary_text.done') {
    var reasoningDoneText = extractReasoningText(data, false);
    var reasoningDoneDelta = buildPendingReasoning(reasoningDoneText);
    if (reasoningDoneDelta) {
      dlog('fallback reasoning_summary_text.done -> reasoning (' + reasoningDoneDelta.length + ' chars)');
      return createStreamEvent('reasoning', { reasoning: reasoningDoneDelta });
    }
    return null;
  }

  // ===== Function call 事件 =====

  // function call 参数增量
  if (type === 'response.function_call_arguments.delta') {
    var fcCallId = data.call_id || data.item_id || '';
    // 从 state 获取该 function call 的元信息（尝试 call_id 和 item_id）
    var fcMeta = state.functionCalls[fcCallId]
      || (data.item_id ? state.functionCalls[data.item_id] : null)
      || (data.call_id ? state.functionCalls[data.call_id] : null)
      || {};
    // 使用 meta 中的 canonical id (call_id)，不是 item_id
    var canonicalId = fcMeta.id || fcCallId;
    fcMeta.id = canonicalId;
    if (!fcMeta.name && data.name) fcMeta.name = data.name;
    if (fcMeta.index === undefined || fcMeta.index === null) fcMeta.index = data.output_index || 0;
    fcMeta.argumentsDeltaSeen = true;
    if (fcCallId) state.functionCalls[fcCallId] = fcMeta;
    if (data.item_id) state.functionCalls[data.item_id] = fcMeta;
    if (data.call_id) state.functionCalls[data.call_id] = fcMeta;
    return createStreamEvent('tool_call', {
      tool_call: {
        id: canonicalId,
        name: fcMeta.name || data.name || '',
        index: fcMeta.index || 0,
        arguments_delta: data.delta || '',
      },
    });
  }

  // function call 参数完成
  if (type === 'response.function_call_arguments.done') {
    var fcDoneCallId = data.call_id || data.item_id || '';
    var fcDoneMeta = state.functionCalls[fcDoneCallId]
      || (data.item_id ? state.functionCalls[data.item_id] : null)
      || (data.call_id ? state.functionCalls[data.call_id] : null)
      || {};
    var sawArgumentsDelta = !!fcDoneMeta.argumentsDeltaSeen;
    fcDoneMeta.argumentsDone = true;
    if (fcDoneCallId) state.functionCalls[fcDoneCallId] = fcDoneMeta;
    if (data.item_id) state.functionCalls[data.item_id] = fcDoneMeta;
    if (data.call_id) state.functionCalls[data.call_id] = fcDoneMeta;
    // 已经通过 delta 发过完整参数时，done 只做状态收敛，避免重复发送导致客户端拼接出 `}{` 非法 JSON。
    if (sawArgumentsDelta) {
      return null;
    }
    var fcDoneCanonicalId = fcDoneMeta.id || fcDoneCallId;
    return createStreamEvent('tool_call', {
      tool_call: {
        id: fcDoneCanonicalId,
        name: fcDoneMeta.name || data.name || '',
        index: fcDoneMeta.index || 0,
        arguments: data.arguments || '',
        done: true,
      },
    });
  }

  // ===== 响应完成事件 =====

  if (type === 'response.completed') {
    var r = resolveResponseContainer(data);
    var usage = resolveUsageFromPayload(r, data);
    var finishReason = 'stop';

    // 从 output 中提取完整文本和 tool_calls（作为兜底）
    var fullText = '';
    var toolCallsFromOutput = [];
    var hasToolCallOutput = false;
    var fallbackToolCallEvents = [];
    var fallbackDeltaEvents = [];
    if (r.output && Array.isArray(r.output)) {
      for (var i = 0; i < r.output.length; i++) {
        var item = r.output[i];
        // 提取 message 内容
        if (item.type === 'message' && item.content) {
          var messageText = extractText(item.content);
          if (messageText) {
            fullText += messageText;
            var completedDelta = buildPendingDelta(i, messageText);
            if (completedDelta) {
              fallbackDeltaEvents.push(createStreamEvent('delta', { content: completedDelta }));
            }
          }
          appendContentBlockAnnotations(i, item.content);
          rememberTextCitations(i, messageText);
        }
        if (item.type === 'web_search_call') {
          updateWebSearchMeta(
            item.id || '',
            i,
            item,
            item.status || 'completed'
          );
          rememberWebSearchAnnotations(i, item);
        }
        // 提取 function_call
        if (item.type === 'function_call') {
          hasToolCallOutput = true;
          var outputCallId = item.call_id || item.id || '';
          var outputMeta = state.functionCalls[outputCallId]
            || (item.id ? state.functionCalls[item.id] : null)
            || (item.call_id ? state.functionCalls[item.call_id] : null)
            || null;
          var alreadyDone = !!(outputMeta && outputMeta.argumentsDone);
          if (!alreadyDone) {
            if (!outputMeta) {
              outputMeta = {
                id: outputCallId,
                name: item.name || '',
                index: i,
                argumentsDone: false,
              };
            }
            outputMeta.id = outputMeta.id || outputCallId;
            if (!outputMeta.name && item.name) outputMeta.name = item.name;
            if (outputMeta.index === undefined || outputMeta.index === null) outputMeta.index = i;
            outputMeta.argumentsDone = true;
            if (outputCallId) state.functionCalls[outputCallId] = outputMeta;
            if (item.id) state.functionCalls[item.id] = outputMeta;
            if (item.call_id) state.functionCalls[item.call_id] = outputMeta;

            var fallbackToolCall = {
              id: outputMeta.id || outputCallId,
              name: outputMeta.name || item.name || '',
              index: outputMeta.index || 0,
              arguments: item.arguments || '{}',
              done: true,
            };
            toolCallsFromOutput.push(fallbackToolCall);
            fallbackToolCallEvents.push(createStreamEvent('tool_call', { tool_call: fallbackToolCall }));
          }
        }
      }
    }

    // 检查 status 映射 finish_reason
    if (r.status === 'incomplete') {
      finishReason = 'length';
    } else if (r.status === 'failed') {
      finishReason = 'stop';
    }
    // 有 function_call 输出时 finish_reason 应为 tool_calls
    if (hasToolCallOutput && finishReason === 'stop') {
      finishReason = 'tool_calls';
    }
    var allAnnotations = collectOrderedAnnotations();
    dlog('[SSE-DIAG] response.completed: status=' + (r.status || '-')
      + ' output_items=' + (Array.isArray(r.output) ? r.output.length : 0)
      + ' full_text.len=' + fullText.length
      + ' fallback_delta=' + fallbackDeltaEvents.length
      + ' fallback_tool_call=' + fallbackToolCallEvents.length
      + ' annotations=' + allAnnotations.length
      + ' finish=' + finishReason
      + ' usage=' + (usage.input_tokens || 0) + '->' + (usage.output_tokens || 0));

    var doneEvent = createStreamEvent('done', {
      id: r.id,
      model: r.model,
      content: fullText,
      finish_reason: finishReason,
      annotations: allAnnotations,
      usage: usage,
    });

    if (fallbackToolCallEvents.length > 0 || fallbackDeltaEvents.length > 0) {
      var completionEvents = fallbackToolCallEvents.concat(fallbackDeltaEvents);
      completionEvents.push(doneEvent);
      return completionEvents;
    }

    return doneEvent;
  }

  // 响应不完整
  if (type === 'response.incomplete') {
    var incompleteResp = resolveResponseContainer(data);
    var incompleteUsage = resolveUsageFromPayload(incompleteResp, data);
    return createStreamEvent('done', {
      id: incompleteResp.id,
      model: incompleteResp.model,
      finish_reason: 'length',
      usage: incompleteUsage,
    });
  }

  // 响应失败
  if (type === 'response.failed') {
    var failedResp = resolveResponseContainer(data);
    var failedError = normalizeCollectedErrorObject(
      failedResp.error || data.error || failedResp,
      (failedResp.incomplete_details && failedResp.incomplete_details.reason) || 'Response failed',
      502
    );
    if (!failedError.code && failedResp && failedResp.incomplete_details && failedResp.incomplete_details.reason) {
      failedError.code = String(failedResp.incomplete_details.reason || '');
    }
    return createStreamEvent('error', {
      error: failedError,
    });
  }

  // ===== 错误事件 =====
  if (type === 'error') {
    var errorObj = normalizeCollectedErrorObject(data.error || data, data.message || 'Unknown error', 502);
    return createStreamEvent('error', {
      error: errorObj,
    });
  }

  // 其他事件忽略（response.queued, rate_limits.updated 等）
  return null;
}

/**
 * 创建跨事件状态对象
 * 用于在多次 parseSSEEvent 调用之间共享 function call 元信息
 *
 * @returns {object} state
 */
export function createParseState() {
  return {
    functionCalls: {},
    outputTextByIndex: {},
    outputTextLengthByIndex: {},
    reasoningLength: 0,
    annotationsByIndex: {},
    annotationKeys: {},
    webSearchCalls: {},
  };
}

export function normalizeCollectedUsage(usage) {
  if (!usage || typeof usage !== 'object') return null;
  var inputDetails = usage.input_tokens_details || {};
  var outputDetails = usage.output_tokens_details || {};
  return {
    input_tokens: usage.input_tokens || usage.prompt_tokens || 0,
    output_tokens: usage.output_tokens || usage.completion_tokens || 0,
    cached_tokens: inputDetails.cached_tokens || usage.cached_tokens || 0,
    reasoning_tokens: outputDetails.reasoning_tokens || usage.reasoning_tokens || 0,
  };
}

function mapCollectedErrorCodeToStatus(code, fallbackStatus) {
  var normalized = String(code || '').toLowerCase();
  if (!normalized) return fallbackStatus || 502;
  if (normalized.indexOf('rate_limit') !== -1 || normalized === 'insufficient_quota') return 429;
  if (normalized === 'invalid_api_key' || normalized === 'authentication_error') return 401;
  if (normalized === 'content_filter' || normalized === 'context_length_exceeded' || normalized === 'invalid_request_error') return 400;
  if (normalized === 'overloaded') return 503;
  if (normalized === 'server_error' || normalized === 'internal_error' || normalized === 'upstream_error') return 502;
  return fallbackStatus || 502;
}

function normalizeCollectedErrorObject(errorLike, fallbackMessage, fallbackStatus) {
  var message = fallbackMessage || 'upstream_error';
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
  }
  if (code) {
    status = mapCollectedErrorCodeToStatus(code, status);
  }
  return {
    message: message || fallbackMessage || 'upstream_error',
    code: code || '',
    type: type || '',
    status: status || fallbackStatus || 502,
  };
}

function hasCollectedPartialUniversalResponse(response) {
  if (!response || typeof response !== 'object') return false;
  if (response.content && String(response.content).length > 0) return true;
  if (response.reasoning && String(response.reasoning).length > 0) return true;
  if (Array.isArray(response.tool_calls) && response.tool_calls.length > 0) return true;
  if (Array.isArray(response.annotations) && response.annotations.length > 0) return true;
  return false;
}

function extractCollectedErrorMessage(parsed) {
  if (!parsed || typeof parsed !== 'object') return '';
  if (parsed.type === 'error') {
    var normalizedError = normalizeCollectedErrorObject(parsed.error || parsed, parsed.message || 'upstream_error', 502);
    return normalizedError.message;
  }
  if (parsed.type === 'response.failed') {
    var failedResp = parsed.response || {};
    var failedNormalized = normalizeCollectedErrorObject(failedResp.error || parsed.error || failedResp, 'response_failed', 502);
    return failedNormalized.message;
  }
  return '';
}

/**
 * 把 Responses SSE 聚合为单个非流式 JSON 响应
 * 用于客户端 stream=false 但上游仍返回 SSE 的场景
 *
 * @param {ReadableStream} stream
 * @param {object} opts
 * @returns {Promise<{ success: boolean, response?: object, usage?: object|null, error?: string }>}
 */
export async function collectNonStreamResponseFromSSE(stream, opts) {
  if (!stream || typeof stream.getReader !== 'function') {
    return { success: false, error: 'empty_sse_stream' };
  }
  var parseOpts = opts || {};

  var parseStream = stream;
  var rawBodyPromise = Promise.resolve('');
  if (typeof stream.tee === 'function') {
    var branches = stream.tee();
    parseStream = branches[0];
    rawBodyPromise = readStreamAsText(branches[1]);
  }

  var finalResponse = null;
  var usageData = null;
  var errorInfo = null;
  var errorMessage = '';
  var rawBody = '';
  var parsedSSEEventCount = 0;
  var parseState = createParseState();
  var collector = createStreamCollector();

  function readStreamAsText(readable) {
    return (async function () {
      if (!readable || typeof readable.getReader !== 'function') return '';
      var reader = readable.getReader();
      var decoder = new TextDecoder();
      var text = '';
      try {
        while (true) {
          var chunk = await reader.read();
          if (chunk.done) break;
          text += decoder.decode(chunk.value, { stream: true });
        }
        text += decoder.decode();
      } catch (_) {
        // ignore raw branch read errors; primary parse branch decides result
      } finally {
        try { reader.releaseLock(); } catch (_) {}
      }
      return text;
    })();
  }

  function responseStatusFromPayload(payload) {
    if (!payload || typeof payload !== 'object') return '';
    if (payload.response && typeof payload.response === 'object') {
      return String(payload.response.status || '').toLowerCase();
    }
    return String(payload.status || '').toLowerCase();
  }

  function payloadType(payload) {
    if (!payload || typeof payload !== 'object') return '';
    return String(payload.type || '').toLowerCase();
  }

  function isFailurePayload(payload) {
    var t = payloadType(payload);
    if (t === 'error' || t === 'response.failed') return true;
    return responseStatusFromPayload(payload) === 'failed';
  }

  function isCompletedPayload(payload) {
    var t = payloadType(payload);
    if (t === 'response.completed' || t === 'response.incomplete') return true;
    var status = responseStatusFromPayload(payload);
    return status === 'completed' || status === 'incomplete';
  }

  function isUsageEmpty(usage) {
    if (!usage || typeof usage !== 'object') return true;
    return (usage.input_tokens || 0) === 0
      && (usage.output_tokens || 0) === 0
      && (usage.cached_tokens || 0) === 0
      && (usage.reasoning_tokens || 0) === 0;
  }

  function rememberUsage(usage) {
    var normalized = normalizeCollectedUsage(usage);
    if (!normalized) return;
    usageData = normalized;
  }

  function rememberError(errorLike, fallbackMessage, fallbackStatus) {
    var normalized = normalizeCollectedErrorObject(errorLike, fallbackMessage, fallbackStatus);
    errorInfo = normalized;
    if (normalized && normalized.message) errorMessage = normalized.message;
  }

  function collectUniversalEvents(eventType, data) {
    var universalEvent = parseSSEEvent(eventType, data, parseState);
    if (!universalEvent) return;
    var events = Array.isArray(universalEvent) ? universalEvent : [universalEvent];
    for (var i = 0; i < events.length; i++) {
      var evt = events[i];
      if (!evt) continue;
      if (evt.type === 'error') {
        rememberError(evt.error || evt, 'upstream_error', 502);
        continue;
      }
      if (evt.usage) rememberUsage(evt.usage);
      collector.push(evt);
    }
  }

  try {
    await parseSSEStream(parseStream, function (eventType, data) {
      if (!data && eventType === 'done') return;
      if (eventType === 'parse_error') {
        rememberError(
          { code: 'parse_error', type: 'parse_error', message: (data && data.error) || 'sse_parse_error', status: 502 },
          'sse_parse_error',
          502
        );
        return;
      }
      if (!data || typeof data !== 'object') return;
      parsedSSEEventCount++;

      var type = data.type || eventType;
      if (type === 'response.completed' || type === 'response.incomplete') {
        if (data.response && typeof data.response === 'object') {
          finalResponse = data.response;
          rememberUsage(data.response.usage || data.usage);
        } else {
          finalResponse = data;
          rememberUsage(data.usage);
        }
      } else if (type === 'response.failed') {
        var failedPayload = data.response || data;
        rememberError(failedPayload.error || data.error || failedPayload, extractCollectedErrorMessage(data) || 'response_failed', 502);
        rememberUsage((failedPayload && failedPayload.usage) || data.usage);
      } else if (type === 'error') {
        rememberError(data.error || data, extractCollectedErrorMessage(data) || 'upstream_error', 502);
        rememberUsage(data.usage);
      }

      collectUniversalEvents(eventType, data);
    }, parseOpts);
    if (!rawBody) {
      rawBody = await rawBodyPromise;
    }
  } catch (err) {
    rememberError(
      { code: 'network_error', type: 'network_error', message: (err && err.message) || 'sse_read_failed', status: 502 },
      'sse_read_failed',
      502
    );
    var partialOnReadError = collector.toResponse();
    if (usageData && (!partialOnReadError.usage || isUsageEmpty(partialOnReadError.usage))) {
      partialOnReadError.usage = usageData;
    }
    var hasPartialOnReadError = hasCollectedPartialUniversalResponse(partialOnReadError);
    if (hasPartialOnReadError) {
      return {
        success: true,
        response: null,
        usage: usageData || null,
        error: errorMessage || 'sse_read_failed',
        error_info: errorInfo,
        universal_response: partialOnReadError,
        has_partial: true,
        saw_error: true,
      };
    }
    return {
      success: false,
      error: errorMessage || 'sse_read_failed',
      error_info: errorInfo,
      usage: usageData || null,
      universal_response: partialOnReadError,
      has_partial: false,
      saw_error: true,
    };
  }

  // compact 场景上游可能返回裸 JSON（且 content-type 缺失），这里做 JSON 回退解析
  if (!finalResponse) {
    var rawTrimmed = rawBody.trim();
    if (rawTrimmed) {
      try {
        var parsedRaw = JSON.parse(rawTrimmed);
        if (parsedRaw && typeof parsedRaw === 'object') {
          if (isFailurePayload(parsedRaw)) {
            rememberError(parsedRaw.error || parsedRaw, extractCollectedErrorMessage(parsedRaw) || 'response_failed', 502);
            if (!usageData) rememberUsage((parsedRaw.response && parsedRaw.response.usage) || parsedRaw.usage);
          } else if (isCompletedPayload(parsedRaw)) {
            finalResponse = (parsedRaw.response && typeof parsedRaw.response === 'object')
              ? parsedRaw.response
              : parsedRaw;
            if (!usageData) rememberUsage((parsedRaw.response && parsedRaw.response.usage) || parsedRaw.usage);
            dlog('collectNonStreamResponseFromSSE raw JSON fallback: parsed_events=' + parsedSSEEventCount
              + ' raw_size=' + rawTrimmed.length);
          }
        }
      } catch (_) {}
    }
  }

  var universalResponse = collector.toResponse();
  if (usageData && (!universalResponse.usage || isUsageEmpty(universalResponse.usage))) {
    universalResponse.usage = usageData;
  }
  var hasPartial = hasCollectedPartialUniversalResponse(universalResponse);
  if (!finalResponse && !hasPartial) {
    return {
      success: false,
      error: errorMessage || 'missing_completed_response',
      error_info: errorInfo,
      usage: usageData || null,
      universal_response: universalResponse,
      has_partial: false,
      saw_error: !!errorInfo,
    };
  }

  return {
    success: true,
    response: finalResponse || null,
    usage: usageData || null,
    error: errorMessage || '',
    error_info: errorInfo,
    universal_response: universalResponse,
    has_partial: hasPartial,
    saw_error: !!errorInfo,
  };
}

/**
 * 构造上游请求的 headers
 *
 * @param {string} accessToken
 * @param {string} accountId - chatgpt_account_id (可选)
 * @param {string} sessionId - session_id (可选)
 * @returns {object}
 */
export function formatHeaders(accessToken, accountId, sessionId, options) {
  var opts = (options && typeof options === 'object') ? options : {};
  var headers = {
    'Authorization': 'Bearer ' + accessToken,
    'Content-Type': 'application/json',
    'Accept': 'text/event-stream',
  };
  var hasOriginator = Object.prototype.hasOwnProperty.call(opts, 'originator');
  var originatorValue = hasOriginator ? opts.originator : 'codex_cli_rs';
  if (originatorValue !== undefined && originatorValue !== null) {
    headers['originator'] = String(originatorValue);
  }
  if (accountId) {
    headers['chatgpt-account-id'] = accountId;
  }
  if (sessionId) {
    headers['session_id'] = sessionId;
  }
  return headers;
}

/**
 * 上游 URL
 */
export function getEndpointUrl(baseUrl) {
  return (baseUrl || 'https://chatgpt.com/backend-api') + '/codex/responses';
}

/**
 * 适配 Responses API 请求中的 tool_call ID 和 tool schema
 * 用于 /v1/responses 和 /backend-api/codex/responses 透传路由
 *
 * 原地修改 body:
 *   - input 中 function_call 的 id/call_id → 有效格式
 *   - input 中 function_call_output 的 call_id → 对应映射
 *   - tools 归一化（function/custom/built-in 别名）
 *   - tool_choice 归一化（string/object 多形态）
 *
 * @param {object} body - Responses API 请求体
 */
export function adaptResponsesBody(body, isCompact) {
  // Codex CLI 用 items，上游用 input
  if (body.items && !body.input) {
    body.input = body.items;
    delete body.items;
  }

  // 上游强制要求 instructions 字段
  if (!body.instructions) {
    body.instructions = 'You are a helpful assistant.';
  }

  // 上游强制要求 store: false
  if (body.store === undefined) {
    body.store = false;
  }

  // compact 端点不接受 stream / store 参数
  if (isCompact) {
    delete body.stream;
    delete body.store;
  }

  // 适配 tool call IDs
  if (body.input && Array.isArray(body.input)) {
    var idMap = {};
    // 第一遍: 扫描 function_call 项，构建映射
    for (var i = 0; i < body.input.length; i++) {
      var item = body.input[i];
      if (item.type === 'function_call') {
        var origId = item.call_id || item.id;
        if (origId && !_isValidCallId(origId)) {
          _resolveCallId(origId, idMap, 'adapt:' + i);
        }
      }
    }
    // 第二遍: 应用映射
    if (Object.keys(idMap).length > 0) {
      for (var j = 0; j < body.input.length; j++) {
        var item2 = body.input[j];
        if (item2.type === 'function_call') {
          var fcId = item2.call_id || item2.id;
          if (fcId && idMap[fcId]) {
            item2.call_id = idMap[fcId];
            item2.id = idMap[fcId];
          }
        } else if (item2.type === 'function_call_output') {
          if (item2.call_id && idMap[item2.call_id]) {
            item2.call_id = idMap[item2.call_id];
          }
        }
      }
    }
  }

  // 规范化 tools + tool_choice
  // 说明: 这里保留所有已识别的 built-in 工具类型，避免被错误降级或过滤。
  var _backendSupportedTypes = {
    function: 1,
    custom: 1,
    web_search: 1,
    file_search: 1,
    computer_use_preview: 1,
    code_interpreter: 1,
    image_generation: 1,
    mcp: 1,
    local_shell: 1,
  };

  if (body.tools && Array.isArray(body.tools)) {
    var normalizedTools = formatTools(body.tools);
    var supportedTools = [];
    for (var t = 0; t < normalizedTools.length; t++) {
      var toolType = normalizedTools[t].type;
      if (_backendSupportedTypes[toolType]) {
        supportedTools.push(normalizedTools[t]);
      }
    }
    if (supportedTools.length > 0) {
      body.tools = supportedTools;
      var normalizedToolChoice = normalizeToolChoice(body.tool_choice || 'auto');
      body.tool_choice = sanitizeToolChoiceForTools(normalizedToolChoice, supportedTools);
    } else {
      delete body.tools;
      delete body.tool_choice;
    }
  } else if (body.tool_choice !== undefined) {
    body.tool_choice = sanitizeToolChoiceForTools(normalizeToolChoice(body.tool_choice), []);
  }
}
