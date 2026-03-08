/**
 * Google Gemini API 客户端适配器
 *
 * 客户端路径: POST /v1beta/models/{model}:generateContent
 *             POST /v1beta/models/{model}:streamGenerateContent
 *             POST /v1beta/models/{model}:streamGenerateContent?alt=sse
 * 格式: Google Gemini API
 *
 * 职责:
 *   1. Gemini 请求 → Universal（parseRequest）
 *   2. Universal 流式事件 → Gemini SSE（formatSSEChunk）
 *   3. Universal 完整响应 → 非流式 JSON（formatNonStreamResponse）
 *
 */

import { createRequest, createStreamEvent } from './universal.mjs';

// ==================== 常量定义 ====================

/**
 * Universal finish_reason → Gemini finishReason 映射
 * 参考: Gemini API FinishReason 枚举
 */
var FINISH_REASON_TO_GEMINI = {
  stop: 'STOP',
  length: 'MAX_TOKENS',
  content_filter: 'SAFETY',
  tool_calls: 'STOP',
};

/**
 * Gemini finishReason → Universal finish_reason 映射
 * 完整覆盖 Gemini API 所有 FinishReason 值
 */
var FINISH_REASON_FROM_GEMINI = {
  STOP: 'stop',
  MAX_TOKENS: 'length',
  SAFETY: 'content_filter',
  RECITATION: 'content_filter',
  BLOCKLIST: 'content_filter',
  PROHIBITED_CONTENT: 'content_filter',
  SPII: 'content_filter',
  MALFORMED_FUNCTION_CALL: 'stop',
  OTHER: 'stop',
  LANGUAGE: 'stop',
  FINISH_REASON_UNSPECIFIED: 'stop',
};

/**
 * Gemini safetySettings 默认值 — 全部设为 BLOCK_NONE
 */
var DEFAULT_SAFETY_SETTINGS = [
  { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'BLOCK_NONE' },
  { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'BLOCK_NONE' },
  { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'BLOCK_NONE' },
  { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_NONE' },
  { category: 'HARM_CATEGORY_CIVIC_INTEGRITY', threshold: 'BLOCK_NONE' },
];

/**
 * Gemini toolConfig mode 映射
 */
var TOOL_CHOICE_TO_GEMINI = {
  auto: 'AUTO',
  none: 'NONE',
  required: 'ANY',
};

// ==================== parseRequest ====================

/**
 * Gemini schema 类型大写转小写（OBJECT→object, STRING→string 等）
 * 上游 Codex Responses API 只接受小写 JSON Schema 类型
 */
function _lowercaseSchemaTypes(schema) {
  if (!schema || typeof schema !== 'object') return schema;
  var result = {};
  var keys = Object.keys(schema);
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    var v = schema[k];
    if (k === 'type' && typeof v === 'string') {
      result[k] = v.toLowerCase();
    } else if (k === 'properties' && v && typeof v === 'object') {
      var props = {};
      var pkeys = Object.keys(v);
      for (var j = 0; j < pkeys.length; j++) {
        props[pkeys[j]] = _lowercaseSchemaTypes(v[pkeys[j]]);
      }
      result[k] = props;
    } else if (k === 'items' && v && typeof v === 'object') {
      result[k] = _lowercaseSchemaTypes(v);
    } else {
      result[k] = v;
    }
  }
  return result;
}

/**
 * Gemini 请求 → Universal
 *
 * 完整处理:
 *   - systemInstruction (顶级字段，parts 数组)
 *   - contents (消息数组，role: user/model)
 *   - generationConfig (temperature, topP, topK, maxOutputTokens, stopSequences,
 *     candidateCount, seed, presencePenalty, frequencyPenalty, responseMimeType,
 *     responseSchema, thinkingConfig)
 *   - tools (functionDeclarations, googleSearch, codeExecution)
 *   - toolConfig (functionCallingConfig.mode, allowedFunctionNames)
 *   - safetySettings
 *   - cachedContent
 *
 *
 * @param {object} body - Gemini API 请求体
 * @returns {UniversalRequest}
 */
export function parseRequest(body) {
  // ========== 1. 提取 systemInstruction ==========
  var system = '';
  if (body.systemInstruction) {
    var sysParts = body.systemInstruction.parts || [];
    var sysTexts = [];
    for (var si = 0; si < sysParts.length; si++) {
      if (sysParts[si].text) {
        sysTexts.push(sysParts[si].text);
      }
    }
    system = sysTexts.join('\n');
  }

  // ========== 2. 解析 contents → messages ==========
  var messages = [];
  var contents = body.contents || [];

  // 建立 functionCall name→id 映射，用于匹配 functionResponse 的 tool_call_id
  var funcCallIdByName = {};

  for (var i = 0; i < contents.length; i++) {
    var content = contents[i];
    var role = content.role === 'model' ? 'assistant' : 'user';
    var cParts = content.parts || [];

    // 收集当前 content 中的所有 parts
    var textParts = [];
    var thoughtParts = [];
    var toolCalls = [];
    var toolResponses = [];
    var multimodalParts = [];

    for (var j = 0; j < cParts.length; j++) {
      var part = cParts[j];

      // thinking/thought 部分
      if (part.thought === true && part.text) {
        thoughtParts.push(part.text);
        continue;
      }

      // functionCall 部分
      if (part.functionCall) {
        var fcName = part.functionCall.name || 'unknown_function';
        var fcId = part.functionCall.id || (fcName + '_call_' + i + '_' + j);
        funcCallIdByName[fcName] = fcId;

        toolCalls.push({
          id: fcId,
          type: 'function',
          function: {
            name: fcName,
            arguments: JSON.stringify(part.functionCall.args || {}),
          },
        });
        continue;
      }

      // functionResponse 部分
      if (part.functionResponse) {
        var frName = part.functionResponse.name || 'unknown_function';
        var frId = part.functionResponse.id || funcCallIdByName[frName] || (frName + '_resp_' + i + '_' + j);
        toolResponses.push({
          name: frName,
          tool_call_id: frId,
          content: JSON.stringify(part.functionResponse.response || {}),
        });
        continue;
      }

      // inlineData (图片/文件等多模态)
      if (part.inlineData) {
        var mimeType = part.inlineData.mimeType || 'application/octet-stream';
        var base64Data = part.inlineData.data || '';
        multimodalParts.push({
          type: 'image_url',
          image_url: {
            url: 'data:' + mimeType + ';base64,' + base64Data,
          },
        });
        continue;
      }

      // fileData (外部文件引用)
      if (part.fileData) {
        multimodalParts.push({
          type: 'file',
          file: {
            mime_type: part.fileData.mimeType || '',
            file_uri: part.fileData.fileUri || '',
          },
        });
        continue;
      }

      // executableCode
      if (part.executableCode) {
        var lang = (part.executableCode.language || 'python').toLowerCase();
        textParts.push('```' + lang + '\n' + (part.executableCode.code || '') + '\n```');
        continue;
      }

      // codeExecutionResult
      if (part.codeExecutionResult) {
        var outcome = part.codeExecutionResult.outcome || '';
        var output = part.codeExecutionResult.output || '';
        if (output) {
          var label = outcome === 'OUTCOME_OK' ? 'output' : 'error';
          textParts.push('```' + label + '\n' + output + '\n```');
        }
        continue;
      }

      // 普通文本
      if (part.text !== undefined && part.text !== null) {
        textParts.push(part.text);
        continue;
      }
    }

    // 如果有 toolCalls，构建 assistant 消息（必须在 toolResponses 之前）
    if (toolCalls.length > 0) {
      var tcMsg = {
        role: 'assistant',
        content: textParts.join('\n') || null,
        tool_calls: toolCalls,
      };
      // 附加 reasoning (thinking) 内容
      if (thoughtParts.length > 0) {
        tcMsg.reasoning = thoughtParts.join('\n');
      }
      messages.push(tcMsg);
    }

    // 将 toolResponses 作为 tool 消息推入（在 assistant 消息之后）
    for (var tr = 0; tr < toolResponses.length; tr++) {
      messages.push({
        role: 'tool',
        content: toolResponses[tr].content,
        tool_call_id: toolResponses[tr].tool_call_id,
        name: toolResponses[tr].name,
      });
    }

    // 如果有 toolCalls，已经处理完毕，跳过后续
    if (toolCalls.length > 0) {
      continue;
    }

    // 普通消息（文本、多模态）
    if (textParts.length > 0 || multimodalParts.length > 0) {
      var msgContent;
      if (multimodalParts.length > 0) {
        // 混合内容：构建 content array 格式
        msgContent = [];
        if (textParts.length > 0) {
          msgContent.push({ type: 'text', text: textParts.join('\n') });
        }
        for (var mp = 0; mp < multimodalParts.length; mp++) {
          msgContent.push(multimodalParts[mp]);
        }
      } else {
        msgContent = textParts.join('\n');
      }

      var msg = {
        role: role,
        content: msgContent,
      };
      // 附加 reasoning (thinking) 内容
      if (thoughtParts.length > 0) {
        msg.reasoning = thoughtParts.join('\n');
      }
      messages.push(msg);
    } else if (thoughtParts.length > 0 && toolResponses.length === 0) {
      // 只有 thought 内容的消息
      messages.push({
        role: role,
        content: '',
        reasoning: thoughtParts.join('\n'),
      });
    }
  }

  // ========== 3. 解析 tools → Universal tools ==========
  var tools = [];
  var hasGoogleSearch = false;
  var hasCodeExecution = false;
  if (body.tools) {
    for (var t = 0; t < body.tools.length; t++) {
      var toolDef = body.tools[t];
      // functionDeclarations
      var decls = toolDef.functionDeclarations || [];
      for (var d = 0; d < decls.length; d++) {
        tools.push({
          type: 'function',
          function: {
            name: decls[d].name,
            description: decls[d].description || '',
            parameters: _lowercaseSchemaTypes(decls[d].parameters || {}),
          },
        });
      }
      // googleSearch — 记录标志，稍后存入 metadata
      if (toolDef.googleSearch !== undefined) {
        hasGoogleSearch = true;
      }
      // codeExecution — 记录标志，稍后存入 metadata
      if (toolDef.codeExecution !== undefined) {
        hasCodeExecution = true;
      }
    }
  }

  // ========== 4. 解析 toolConfig → tool_choice ==========
  var toolChoice = 'auto';
  if (body.toolConfig && body.toolConfig.functionCallingConfig) {
    var fcc = body.toolConfig.functionCallingConfig;
    var mode = (fcc.mode || '').toUpperCase();
    if (mode === 'NONE') {
      toolChoice = 'none';
    } else if (mode === 'ANY') {
      if (fcc.allowedFunctionNames && fcc.allowedFunctionNames.length === 1) {
        toolChoice = {
          type: 'function',
          function: { name: fcc.allowedFunctionNames[0] },
        };
      } else {
        toolChoice = 'required';
      }
    }
    // AUTO → 'auto'（默认）
  }

  // ========== 5. 解析 generationConfig ==========
  var gc = body.generationConfig || {};
  var metadata = {};

  // thinkingConfig 存入 metadata
  if (gc.thinkingConfig) {
    metadata.thinkingConfig = {
      thinkingBudget: gc.thinkingConfig.thinkingBudget,
      thinkingLevel: gc.thinkingConfig.thinkingLevel,
      includeThoughts: gc.thinkingConfig.includeThoughts,
    };
  }

  // responseMimeType / responseSchema
  if (gc.responseMimeType) {
    metadata.responseMimeType = gc.responseMimeType;
  }
  if (gc.responseSchema) {
    metadata.responseSchema = gc.responseSchema;
  }

  // candidateCount
  if (gc.candidateCount) {
    metadata.candidateCount = gc.candidateCount;
  }

  // seed
  if (gc.seed !== undefined) {
    metadata.seed = gc.seed;
  }

  // frequencyPenalty / presencePenalty
  if (gc.frequencyPenalty !== undefined) {
    metadata.frequencyPenalty = gc.frequencyPenalty;
  }
  if (gc.presencePenalty !== undefined) {
    metadata.presencePenalty = gc.presencePenalty;
  }

  // safetySettings 原样保存
  if (body.safetySettings) {
    metadata.safetySettings = body.safetySettings;
  }

  // cachedContent
  if (body.cachedContent) {
    metadata.cachedContent = body.cachedContent;
  }

  // googleSearch / codeExecution 工具标志
  if (hasGoogleSearch) {
    metadata.googleSearch = true;
  }
  if (hasCodeExecution) {
    metadata.codeExecution = true;
  }

  return createRequest({
    model: body.model || '',
    system: system,
    messages: messages,
    stream: body._stream !== false,
    temperature: gc.temperature,
    max_tokens: gc.maxOutputTokens,
    top_p: gc.topP,
    stop: gc.stopSequences || null,
    tools: tools,
    tool_choice: toolChoice,
    metadata: metadata,
  });
}

// ==================== formatSSEChunk ====================

/**
 * Universal 流式事件 → Gemini SSE
 *
 * Gemini SSE 格式:
 *   data: {"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"},"index":0}]}
 *
 * 支持事件类型:
 *   - start: 流开始，发送角色指示
 *   - delta: 文本增量
 *   - reasoning: 思考增量（thought: true）
 *   - tool_call: 函数调用（单次完整 / 增量积累）
 *   - usage: 单独的 usage 事件
 *   - done: 流结束，含 finishReason 和 usageMetadata
 *   - error: 错误事件
 *
 *
 * @param {UniversalStreamEvent} event
 * @param {object} ctx - 流上下文（可用于跨 chunk 状态）
 * @returns {string|null} SSE 格式字符串 "data: {...}\n\n" 或 null
 */
export function formatSSEChunk(event, ctx) {
  ctx = ctx || {};

  // ===== start: 流开始 =====
  if (event.type === 'start') {
    // Gemini 不需要显式的 start 事件，但可以发送一个空的角色指示
    // 某些客户端期望第一个 chunk 包含 role
    return buildSSELine({
      candidates: [{
        content: {
          parts: [{ text: '' }],
          role: 'model',
        },
        index: 0,
      }],
    });
  }

  // ===== delta: 文本增量 =====
  if (event.type === 'delta' && event.content) {
    return buildSSELine({
      candidates: [{
        content: {
          parts: [{ text: event.content }],
          role: 'model',
        },
        index: 0,
      }],
    });
  }

  // ===== reasoning: 思考增量 =====
  if (event.type === 'reasoning' && event.reasoning) {
    return buildSSELine({
      candidates: [{
        content: {
          parts: [{ thought: true, text: event.reasoning }],
          role: 'model',
        },
        index: 0,
      }],
    });
  }

  // ===== tool_call: 函数调用 =====
  if (event.type === 'tool_call' && event.tool_call) {
    // Gemini 不支持增量 tool_call，只在完成时发送
    if (event.tool_call.done) {
      var fcPart = {
        functionCall: {
          name: event.tool_call.name,
          args: safeParseJSON(event.tool_call.arguments),
        },
      };

      // 如果有 id，附加到 functionCall
      if (event.tool_call.id) {
        fcPart.functionCall.id = event.tool_call.id;
      }

      return buildSSELine({
        candidates: [{
          content: {
            parts: [fcPart],
            role: 'model',
          },
          index: 0,
          finishReason: 'STOP',
        }],
      });
    }

    // 增量 tool_call: 在 ctx 中积累
    // 参考: 某些上游以增量方式发送 function name 和 arguments
    if (!ctx._pendingToolCalls) {
      ctx._pendingToolCalls = {};
    }
    var tcId = event.tool_call.id || '_default';
    if (!ctx._pendingToolCalls[tcId]) {
      ctx._pendingToolCalls[tcId] = { name: '', arguments: '' };
    }
    if (event.tool_call.name) {
      ctx._pendingToolCalls[tcId].name += event.tool_call.name;
    }
    if (event.tool_call.arguments) {
      ctx._pendingToolCalls[tcId].arguments += event.tool_call.arguments;
    }
    // 增量时不发送任何东西
    return null;
  }

  // ===== usage: 单独的 usage 事件 =====
  if (event.type === 'usage' && event.usage) {
    return buildSSELine({
      usageMetadata: buildUsageMetadata(event.usage),
    });
  }

  // ===== done: 流结束 =====
  if (event.type === 'done') {
    var finishReason = mapFinishReasonToGemini(event.finish_reason);

    // 发送积累的 tool_calls（如果有）
    var pendingChunks = '';
    if (ctx._pendingToolCalls) {
      var pendingIds = Object.keys(ctx._pendingToolCalls);
      for (var pi = 0; pi < pendingIds.length; pi++) {
        var ptc = ctx._pendingToolCalls[pendingIds[pi]];
        var pendingFcPart = {
          functionCall: {
            name: ptc.name,
            args: safeParseJSON(ptc.arguments),
          },
        };
        if (pendingIds[pi] !== '_default') {
          pendingFcPart.functionCall.id = pendingIds[pi];
        }
        pendingChunks += buildSSELine({
          candidates: [{
            content: {
              parts: [pendingFcPart],
              role: 'model',
            },
            index: 0,
          }],
        });
      }
      ctx._pendingToolCalls = null;
    }

    // 最终 done chunk
    var donePayload = {
      candidates: [{
        content: { parts: [], role: 'model' },
        finishReason: finishReason,
        index: 0,
      }],
    };

    // 附加 usageMetadata
    if (event.usage) {
      donePayload.usageMetadata = buildUsageMetadata(event.usage);
    }

    // 附加 safetyRatings（如果上游提供）
    if (ctx._safetyRatings) {
      donePayload.candidates[0].safetyRatings = ctx._safetyRatings;
    }

    return pendingChunks + buildSSELine(donePayload);
  }

  // ===== error: 错误事件 =====
  if (event.type === 'error' && event.error) {
    return buildSSELine({
      error: {
        code: event.error.code || 500,
        message: event.error.message || 'Unknown error',
        status: event.error.status || 'INTERNAL',
      },
    });
  }

  return null;
}

// ==================== formatNonStreamResponse ====================

/**
 * Universal 完整响应 → Gemini 非流式 JSON
 *
 * 完整字段:
 *   - candidates[].content.parts (text, thought, functionCall, inlineData)
 *   - candidates[].finishReason
 *   - candidates[].safetyRatings
 *   - candidates[].citationMetadata
 *   - candidates[].index
 *   - usageMetadata (promptTokenCount, candidatesTokenCount, totalTokenCount, thoughtsTokenCount, cachedContentTokenCount)
 *   - modelVersion
 *
 *
 * @param {UniversalResponse} response
 * @returns {object} Gemini 格式的完整响应 JSON
 */
export function formatNonStreamResponse(response) {
  var parts = [];

  // 1. reasoning/thought 部分（放在前面，参考 Gemini 原生顺序）
  if (response.reasoning) {
    parts.push({ thought: true, text: response.reasoning });
  }

  // 2. 文本内容
  if (response.content) {
    parts.push({ text: response.content });
  }

  // 3. 函数调用
  if (response.tool_calls && response.tool_calls.length > 0) {
    for (var i = 0; i < response.tool_calls.length; i++) {
      var tc = response.tool_calls[i];
      var fcPart = {
        functionCall: {
          name: tc.name || 'unknown_function',
          args: safeParseJSON(tc.arguments || '{}'),
        },
      };
      // 附加 id（如果有）
      if (tc.id) {
        fcPart.functionCall.id = tc.id;
      }
      parts.push(fcPart);
    }
  }

  // 确保至少有一个 part
  if (parts.length === 0) {
    parts.push({ text: '' });
  }

  // 构建 finishReason
  var finishReason = mapFinishReasonToGemini(response.finish_reason);

  // 如果有 tool_calls 且是 stop/tool_calls，保持 STOP
  if (response.tool_calls && response.tool_calls.length > 0 &&
      (response.finish_reason === 'stop' || response.finish_reason === 'tool_calls')) {
    finishReason = 'STOP';
  }

  // 构建 candidate
  var candidate = {
    content: {
      parts: parts,
      role: 'model',
    },
    finishReason: finishReason,
    index: 0,
  };

  // 安全评级（如果有）
  candidate.safetyRatings = buildDefaultSafetyRatings();

  // 构建 usageMetadata
  var usageMeta = buildUsageMetadata(response.usage || {});

  return {
    candidates: [candidate],
    usageMetadata: usageMeta,
    modelVersion: response.model || '',
  };
}

// ==================== 辅助转换函数 ====================

/**
 * Universal → Gemini 请求体构建（用于将 Universal 请求发送到 Gemini 后端）
 *
 * 完整字段映射:
 *   - system → systemInstruction
 *   - messages → contents
 *   - temperature/top_p/max_tokens/stop → generationConfig
 *   - tools → tools[].functionDeclarations
 *   - tool_choice → toolConfig
 *   - metadata.thinkingConfig → generationConfig.thinkingConfig
 *   - metadata.responseMimeType → generationConfig.responseMimeType
 *   - metadata.responseSchema → generationConfig.responseSchema
 *   - metadata.safetySettings → safetySettings
 *
 *
 * @param {UniversalRequest} req - Universal 请求
 * @returns {object} Gemini 格式请求体
 */
export function buildGeminiRequest(req) {
  var result = {};

  // ===== systemInstruction =====
  if (req.system) {
    result.systemInstruction = {
      parts: [{ text: req.system }],
    };
  }

  // ===== contents =====
  result.contents = buildGeminiContents(req.messages);

  // ===== generationConfig =====
  var gc = {};
  if (req.temperature !== undefined && req.temperature !== null) {
    gc.temperature = req.temperature;
  }
  if (req.top_p !== undefined && req.top_p !== null) {
    gc.topP = req.top_p;
  }
  if (req.max_tokens !== undefined && req.max_tokens !== null) {
    gc.maxOutputTokens = req.max_tokens;
  }
  if (req.stop) {
    gc.stopSequences = Array.isArray(req.stop) ? req.stop : [req.stop];
  }

  // metadata 中的扩展配置
  var meta = req.metadata || {};
  if (meta.thinkingConfig) {
    gc.thinkingConfig = {};
    if (meta.thinkingConfig.thinkingBudget !== undefined) {
      gc.thinkingConfig.thinkingBudget = meta.thinkingConfig.thinkingBudget;
    }
    if (meta.thinkingConfig.thinkingLevel !== undefined) {
      gc.thinkingConfig.thinkingLevel = meta.thinkingConfig.thinkingLevel;
    }
    if (meta.thinkingConfig.includeThoughts !== undefined) {
      gc.thinkingConfig.includeThoughts = meta.thinkingConfig.includeThoughts;
    }
  }
  if (meta.responseMimeType) {
    gc.responseMimeType = meta.responseMimeType;
  }
  if (meta.responseSchema) {
    gc.responseSchema = meta.responseSchema;
  }
  if (meta.candidateCount) {
    gc.candidateCount = meta.candidateCount;
  }
  if (meta.seed !== undefined) {
    gc.seed = meta.seed;
  }
  if (meta.frequencyPenalty !== undefined) {
    gc.frequencyPenalty = meta.frequencyPenalty;
  }
  if (meta.presencePenalty !== undefined) {
    gc.presencePenalty = meta.presencePenalty;
  }

  if (Object.keys(gc).length > 0) {
    result.generationConfig = gc;
  }

  // ===== tools =====
  if (req.tools && req.tools.length > 0) {
    var functionDeclarations = [];
    for (var t = 0; t < req.tools.length; t++) {
      var tool = req.tools[t];
      if (tool.type === 'function' && tool.function) {
        var decl = {
          name: tool.function.name,
          description: tool.function.description || '',
        };
        if (tool.function.parameters && Object.keys(tool.function.parameters).length > 0) {
          decl.parameters = cleanSchemaForGemini(tool.function.parameters);
        }
        functionDeclarations.push(decl);
      }
    }
    if (functionDeclarations.length > 0) {
      result.tools = [{ functionDeclarations: functionDeclarations }];
    }
  }

  // ===== toolConfig =====
  if (req.tool_choice && req.tool_choice !== 'auto') {
    result.toolConfig = buildToolConfig(req.tool_choice);
  }

  // ===== safetySettings =====
  if (meta.safetySettings) {
    result.safetySettings = meta.safetySettings;
  } else {
    result.safetySettings = DEFAULT_SAFETY_SETTINGS.map(function(s) { return { category: s.category, threshold: s.threshold }; });
  }

  // ===== cachedContent =====
  if (meta.cachedContent) {
    result.cachedContent = meta.cachedContent;
  }

  return result;
}

/**
 * Universal messages → Gemini contents
 *
 * 处理:
 *   - role 映射 (assistant → model, user/system → user)
 *   - 文本内容 → parts[].text
 *   - 多模态内容 → parts[].inlineData
 *   - tool_calls → parts[].functionCall
 *   - tool 消息 → 合并为连续的 functionResponse parts
 *   - reasoning → parts[].thought: true
 *
 *
 * @param {Array} messages - Universal messages
 * @returns {Array} Gemini contents
 */
function buildGeminiContents(messages) {
  var contents = [];
  var pendingToolResponses = [];

  function flushToolResponses() {
    if (pendingToolResponses.length === 0) return;
    contents.push({
      role: 'user',
      parts: pendingToolResponses.slice(),
    });
    pendingToolResponses = [];
  }

  for (var i = 0; i < messages.length; i++) {
    var msg = messages[i];
    var role = msg.role;

    // tool 消息 → functionResponse，累积后合并发送
    if (role === 'tool') {
      var responseData;
      try {
        responseData = typeof msg.content === 'string' ? JSON.parse(msg.content) : msg.content;
      } catch (e) {
        responseData = { result: String(msg.content || '') };
      }
      if (typeof responseData !== 'object' || responseData === null) {
        responseData = { result: responseData };
      }
      pendingToolResponses.push({
        functionResponse: {
          name: msg.name || 'unknown_function',
          response: responseData,
        },
      });
      continue;
    }

    // 遇到非 tool 消息，先刷新累积的 tool 响应
    flushToolResponses();

    // system 消息跳过（应该已经处理为 systemInstruction）
    if (role === 'system') {
      continue;
    }

    var geminiRole = role === 'assistant' ? 'model' : 'user';
    var parts = [];

    // 添加 reasoning/thought（如果有）
    if (msg.reasoning) {
      parts.push({ thought: true, text: msg.reasoning });
    }

    // 处理 content
    var content = msg.content;
    if (typeof content === 'string') {
      if (content) {
        parts.push({ text: content });
      }
    } else if (Array.isArray(content)) {
      // 多模态内容数组
      for (var ci = 0; ci < content.length; ci++) {
        var item = content[ci];
        if (!item || typeof item !== 'object') continue;

        if (item.type === 'text' && item.text) {
          parts.push({ text: item.text });
        } else if (item.type === 'image_url' && item.image_url && item.image_url.url) {
          var imageUrl = item.image_url.url;
          if (imageUrl.indexOf('data:') === 0) {
            // data URI → inlineData
            var parsed = parseDataUri(imageUrl);
            if (parsed) {
              parts.push({
                inlineData: {
                  mimeType: parsed.mimeType,
                  data: parsed.data,
                },
              });
            }
          } else {
            // 外部 URL — Gemini 需要 fileData 或先上传
            // 暂时描述为文本提示
            parts.push({ text: '[Image: ' + imageUrl + ']' });
          }
        } else if (item.type === 'file' && item.file) {
          parts.push({
            fileData: {
              mimeType: item.file.mime_type || '',
              fileUri: item.file.file_uri || '',
            },
          });
        }
      }
    }

    // 处理 tool_calls → functionCall parts
    if (msg.tool_calls && msg.tool_calls.length > 0) {
      for (var tc = 0; tc < msg.tool_calls.length; tc++) {
        var toolCall = msg.tool_calls[tc];
        var fcArgs;
        if (toolCall.function) {
          fcArgs = safeParseJSON(toolCall.function.arguments || '{}');
        } else {
          fcArgs = safeParseJSON(toolCall.arguments || '{}');
        }

        var fcPartObj = {
          functionCall: {
            name: toolCall.function ? toolCall.function.name : (toolCall.name || 'unknown_function'),
            args: fcArgs,
          },
        };

        if (toolCall.id) {
          fcPartObj.functionCall.id = toolCall.id;
        }

        parts.push(fcPartObj);
      }
    }

    if (parts.length > 0) {
      contents.push({
        role: geminiRole,
        parts: parts,
      });
    }
  }

  // 最终刷新剩余的 tool 响应
  flushToolResponses();

  // 确保 contents 不为空
  if (contents.length === 0) {
    contents.push({
      role: 'user',
      parts: [{ text: '' }],
    });
  }

  return contents;
}

/**
 * 解析 Gemini 流式/非流式响应 → Universal 事件列表
 *
 * 用于将 Gemini 后端响应转换回 Universal 格式（反方向）
 *
 *
 * @param {object} geminiResponse - Gemini API 响应 JSON
 * @returns {Array<UniversalStreamEvent>} Universal 事件数组
 */
export function parseGeminiResponse(geminiResponse) {
  var events = [];

  // 处理 GeminiCLI 的 response 包装格式
  if (geminiResponse.response && !geminiResponse.candidates) {
    geminiResponse = geminiResponse.response;
  }

  var candidates = geminiResponse.candidates || [];
  if (candidates.length === 0) {
    // 没有 candidates，可能是错误响应
    if (geminiResponse.error) {
      events.push(createStreamEvent('error', {
        error: {
          code: geminiResponse.error.code || 500,
          message: geminiResponse.error.message || 'Unknown error',
          status: geminiResponse.error.status || 'INTERNAL',
        },
      }));
    }
    events.push(createStreamEvent('done', { finish_reason: 'stop' }));
    return events;
  }

  var candidate = candidates[0];
  var parts = (candidate.content && candidate.content.parts) || [];

  // 逐 part 解析
  for (var p = 0; p < parts.length; p++) {
    var part = parts[p];

    // thought (thinking)
    if (part.thought === true && part.text) {
      events.push(createStreamEvent('reasoning', { reasoning: part.text }));
      continue;
    }

    // functionCall
    if (part.functionCall) {
      events.push(createStreamEvent('tool_call', {
        tool_call: {
          id: part.functionCall.id || (part.functionCall.name + '_call_' + p),
          name: part.functionCall.name || 'unknown_function',
          arguments: JSON.stringify(part.functionCall.args || {}),
          done: true,
        },
      }));
      continue;
    }

    // inlineData (图片)
    if (part.inlineData) {
      var mime = part.inlineData.mimeType || 'image/png';
      var b64 = part.inlineData.data || '';
      events.push(createStreamEvent('delta', {
        content: '![image](data:' + mime + ';base64,' + b64 + ')',
      }));
      continue;
    }

    // executableCode
    if (part.executableCode) {
      var eLang = (part.executableCode.language || 'python').toLowerCase();
      events.push(createStreamEvent('delta', {
        content: '\n```' + eLang + '\n' + (part.executableCode.code || '') + '\n```\n',
      }));
      continue;
    }

    // codeExecutionResult
    if (part.codeExecutionResult) {
      var eOut = part.codeExecutionResult.output || '';
      if (eOut) {
        var eLabel = part.codeExecutionResult.outcome === 'OUTCOME_OK' ? 'output' : 'error';
        events.push(createStreamEvent('delta', {
          content: '\n```' + eLabel + '\n' + eOut + '\n```\n',
        }));
      }
      continue;
    }

    // 普通文本
    if (part.text !== undefined && part.text !== null) {
      events.push(createStreamEvent('delta', { content: part.text }));
      continue;
    }
  }

  // finishReason 和 usage
  var geminiFinishReason = candidate.finishReason || 'STOP';
  var universalFinishReason = mapFinishReasonFromGemini(geminiFinishReason);

  var usageMeta = geminiResponse.usageMetadata;
  var usage = null;
  if (usageMeta) {
    usage = {
      input_tokens: usageMeta.promptTokenCount || 0,
      output_tokens: usageMeta.candidatesTokenCount || 0,
      cached_tokens: usageMeta.cachedContentTokenCount || 0,
    };
  }

  events.push(createStreamEvent('done', {
    finish_reason: universalFinishReason,
    usage: usage,
  }));

  return events;
}

// ==================== Schema 清理 ====================

/**
 * 清理 JSON Schema 为 Gemini 兼容格式
 *
 *
 * 处理:
 *   - 移除不支持的字段 ($ref, $defs, additionalProperties, allOf, oneOf, etc.)
 *   - 类型映射 (string → STRING, number → NUMBER, etc.)
 *   - type 数组处理 (["string", "null"] → STRING)
 *   - default 值移到 description
 *   - 递归处理 properties 和 items
 *
 * @param {object} schema
 * @returns {object}
 */
function cleanSchemaForGemini(schema) {
  if (!schema || typeof schema !== 'object') return schema;
  return cleanSchemaRecursive(schema, new WeakSet());
}

function cleanSchemaRecursive(schema, visited) {
  if (!schema || typeof schema !== 'object') return schema;

  if (visited.has(schema)) return {};
  visited.add(schema);

  var result = {};

  // 不支持的字段
  var unsupported = {
    '$ref': 1, '$defs': 1, '$schema': 1, '$id': 1,
    definitions: 1, additionalProperties: 1, title: 1, strict: 1,
    exclusiveMaximum: 1, exclusiveMinimum: 1, oneOf: 1, allOf: 1,
    examples: 1, example: 1, readOnly: 1, writeOnly: 1,
    'const': 1, additionalItems: 1, contains: 1,
    patternProperties: 1, dependencies: 1, propertyNames: 1,
    'if': 1, then: 1, 'else': 1, contentEncoding: 1, contentMediaType: 1,
    '$anchor': 1, '$dynamicRef': 1, '$dynamicAnchor': 1,
    '$vocabulary': 1, '$comment': 1,
  };

  var keys = Object.keys(schema);
  for (var k = 0; k < keys.length; k++) {
    var key = keys[k];
    var value = schema[key];

    if (unsupported[key]) continue;

    if (key === 'type') {
      // 类型映射
      if (Array.isArray(value)) {
        var primary = null;
        for (var tv = 0; tv < value.length; tv++) {
          if (value[tv] !== 'null') { primary = value[tv]; break; }
        }
        result.type = mapSchemaType(primary || 'string');
      } else {
        result.type = mapSchemaType(value);
      }
    } else if (key === 'properties' && typeof value === 'object') {
      result.properties = {};
      var propKeys = Object.keys(value);
      for (var pk = 0; pk < propKeys.length; pk++) {
        result.properties[propKeys[pk]] = cleanSchemaRecursive(value[propKeys[pk]], visited);
      }
    } else if (key === 'items') {
      if (Array.isArray(value)) {
        // Tuple 定义
        if (value.length > 0) {
          result.items = cleanSchemaRecursive(value[0], visited);
        } else {
          result.items = {};
        }
      } else if (typeof value === 'object') {
        result.items = cleanSchemaRecursive(value, visited);
      } else {
        result.items = value;
      }
    } else if (key === 'anyOf' && Array.isArray(value)) {
      // 尝试转为 enum
      var allConst = true;
      for (var av = 0; av < value.length; av++) {
        if (!value[av] || value[av]['const'] === undefined) { allConst = false; break; }
      }
      if (allConst && value.length > 0) {
        result.type = 'STRING';
        result['enum'] = [];
        for (var ae = 0; ae < value.length; ae++) {
          var cv = value[ae]['const'];
          if (cv !== '' && cv !== null) {
            result['enum'].push(String(cv));
          }
        }
      } else {
        // 取第一个有效的
        for (var af = 0; af < value.length; af++) {
          if (value[af] && (value[af].type || value[af]['enum'])) {
            var cleaned = cleanSchemaRecursive(value[af], visited);
            var cKeys = Object.keys(cleaned);
            for (var ck = 0; ck < cKeys.length; ck++) {
              result[cKeys[ck]] = cleaned[cKeys[ck]];
            }
            break;
          }
        }
      }
    } else if (key === 'default') {
      // 移到 description
      var origDesc = result.description || schema.description || '';
      result.description = (origDesc + ' (Default: ' + JSON.stringify(value) + ')').trim();
    } else if (typeof value === 'object' && !Array.isArray(value)) {
      result[key] = cleanSchemaRecursive(value, visited);
    } else {
      result[key] = value;
    }
  }

  // 确保有 type（如果有 properties 但没有 type）
  if (result.properties && !result.type) {
    result.type = 'OBJECT';
  }

  // 去重 required
  if (result.required && Array.isArray(result.required)) {
    var seen = {};
    var deduped = [];
    for (var ri = 0; ri < result.required.length; ri++) {
      if (!seen[result.required[ri]]) {
        seen[result.required[ri]] = true;
        deduped.push(result.required[ri]);
      }
    }
    result.required = deduped;
  }

  return result;
}

/**
 * 小写类型名 → Gemini 大写类型名
 */
function mapSchemaType(t) {
  var map = {
    string: 'STRING',
    number: 'NUMBER',
    integer: 'INTEGER',
    boolean: 'BOOLEAN',
    array: 'ARRAY',
    object: 'OBJECT',
  };
  if (typeof t === 'string') {
    return map[t.toLowerCase()] || t;
  }
  return t;
}

// ==================== 内部辅助函数 ====================

/**
 * 构建 SSE 行
 * @param {object} payload
 * @returns {string} "data: {...}\n\n"
 */
function buildSSELine(payload) {
  return 'data: ' + JSON.stringify(payload) + '\n\n';
}

/**
 * Universal finish_reason → Gemini finishReason
 */
function mapFinishReasonToGemini(reason) {
  if (!reason) return 'STOP';
  return FINISH_REASON_TO_GEMINI[reason] || 'STOP';
}

/**
 * Gemini finishReason → Universal finish_reason
 */
function mapFinishReasonFromGemini(reason) {
  if (!reason) return 'stop';
  return FINISH_REASON_FROM_GEMINI[reason] || 'stop';
}

/**
 * 构建 Gemini usageMetadata
 *
 * 完整字段: promptTokenCount, candidatesTokenCount, totalTokenCount,
 *           thoughtsTokenCount, cachedContentTokenCount
 *
 * @param {object} usage - Universal usage { input_tokens, output_tokens, reasoning_tokens, cached_tokens }
 * @returns {object} Gemini usageMetadata
 */
function buildUsageMetadata(usage) {
  if (!usage) return {};
  var inputTokens = usage.input_tokens || 0;
  var outputTokens = usage.output_tokens || 0;
  var reasoningTokens = usage.reasoning_tokens || 0;
  var cachedTokens = usage.cached_tokens || 0;

  var meta = {
    promptTokenCount: inputTokens,
    candidatesTokenCount: outputTokens,
    totalTokenCount: inputTokens + outputTokens + reasoningTokens,
  };

  if (reasoningTokens > 0) {
    meta.thoughtsTokenCount = reasoningTokens;
  }

  if (cachedTokens > 0) {
    meta.cachedContentTokenCount = cachedTokens;
  }

  return meta;
}

/**
 * 构建默认的 safetyRatings
 * 参考: Gemini API 响应中的 safetyRatings 字段
 */
function buildDefaultSafetyRatings() {
  return [
    { category: 'HARM_CATEGORY_HARASSMENT', probability: 'NEGLIGIBLE' },
    { category: 'HARM_CATEGORY_HATE_SPEECH', probability: 'NEGLIGIBLE' },
    { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', probability: 'NEGLIGIBLE' },
    { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', probability: 'NEGLIGIBLE' },
  ];
}

/**
 * 构建 toolConfig
 *
 * @param {string|object} toolChoice - Universal tool_choice
 * @returns {object} Gemini toolConfig
 */
function buildToolConfig(toolChoice) {
  if (typeof toolChoice === 'string') {
    var mode = TOOL_CHOICE_TO_GEMINI[toolChoice] || 'AUTO';
    return { functionCallingConfig: { mode: mode } };
  }

  if (typeof toolChoice === 'object' && toolChoice.type === 'function') {
    var funcName = toolChoice.function ? toolChoice.function.name : null;
    if (funcName) {
      return {
        functionCallingConfig: {
          mode: 'ANY',
          allowedFunctionNames: [funcName],
        },
      };
    }
  }

  return { functionCallingConfig: { mode: 'AUTO' } };
}

/**
 * 解析 data URI → { mimeType, data }
 *
 * @param {string} uri - "data:image/png;base64,xxx"
 * @returns {object|null} { mimeType, data } 或 null
 */
function parseDataUri(uri) {
  if (!uri || uri.indexOf('data:') !== 0) return null;
  var semicolonIdx = uri.indexOf(';');
  if (semicolonIdx === -1) return null;
  var commaIdx = uri.indexOf(',', semicolonIdx);
  if (commaIdx === -1) return null;

  var mimeType = uri.substring(5, semicolonIdx);
  var data = uri.substring(commaIdx + 1);
  return { mimeType: mimeType, data: data };
}

/**
 * 安全 JSON 解析
 */
function safeParseJSON(str) {
  if (typeof str !== 'string') return str || {};
  try {
    return JSON.parse(str);
  } catch (e) {
    return {};
  }
}
