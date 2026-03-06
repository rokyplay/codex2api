/**
 * ChatGPT 会话管理模块
 *
 * 功能：
 *   1. 用 access_token 调用 ChatGPT API
 *   2. token 过期时用 session cookie 自动续期
 *   3. chat() — ChatGPT /conversation 端点（需要 sentinel pipeline，当前 403）
 *   4. codex() — Codex /responses 端点（已验证可用，无需 sentinel token）
 *
 * 使用方式：
 *   import { ChatSession } from './lib/session.mjs';
 *   var session = new ChatSession(account);
 *   var reply = await session.codex('Write a hello world in Python');
 */

import { log, C } from './utils.mjs';

var CHAT_API = 'https://chatgpt.com/backend-api';
var AUTH_SESSION_API = 'https://chatgpt.com/api/auth/session';
var UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.88 Safari/537.36';

/**
 * ChatGPT 会话
 */
export class ChatSession {

  /**
   * @param {object} account - accounts.json 中的一条记录
   *   { email, password, accessToken, sessionToken, cookies }
   */
  constructor(account) {
    this._email = account.email;
    this._accessToken = account.accessToken || '';
    this._sessionToken = account.sessionToken || '';
    this._cookies = account.cookies || {};
    this._tokenExpiresAt = 0;
    this._conversationId = null;
    this._parentMessageId = null;
    this._accountId = '';

    // 解析 JWT
    if (this._accessToken) {
      this._tokenExpiresAt = parseJwtExp(this._accessToken);
      var authInfo = parseJwtAuth(this._accessToken);
      this._accountId = authInfo.chatgpt_account_id || '';
    }
  }

  /**
   * 获取可用的 access_token（自动续期）
   */
  async getAccessToken() {
    // 还有 60 秒以上有效期，直接用
    if (this._accessToken && Date.now() / 1000 < this._tokenExpiresAt - 60) {
      return this._accessToken;
    }

    // 需要续期
    log('🔄', C.yellow, '续期 access_token (' + this._email + ')...');
    await this._refreshToken();
    return this._accessToken;
  }

  /**
   * 发送聊天消息
   */
  async chat(message, opts) {
    opts = opts || {};
    var token = await this.getAccessToken();
    if (!token) {
      throw new Error('无可用 access_token');
    }

    var model = opts.model || 'auto';
    var msgId = crypto.randomUUID ? crypto.randomUUID() : generateUUID();
    var parentId = this._parentMessageId || generateUUID();

    var body = {
      action: 'next',
      messages: [{
        id: msgId,
        author: { role: 'user' },
        content: { content_type: 'text', parts: [message] },
      }],
      model: model,
      parent_message_id: parentId,
    };

    if (this._conversationId) {
      body.conversation_id = this._conversationId;
    }

    var resp = await fetch(CHAT_API + '/conversation', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json',
        'User-Agent': UA,
        'Accept': 'text/event-stream',
        'Oai-Device-Id': this._cookies['oai-did'] || '',
        'Oai-Language': 'en-US',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(opts.timeout || 60000),
    });

    if ((resp.status === 401 || resp.status === 403) && !opts._retried) {
      // token 过期，刷新后重试一次
      log('🔄', C.yellow, 'Chat API ' + resp.status + '，刷新 token 后重试...');
      await this._refreshToken();
      return this.chat(message, Object.assign({}, opts, { _retried: true }));
    }

    if (!resp.ok) {
      var errBody = await resp.text().catch(function () { return ''; });
      throw new Error('Chat API ' + resp.status + ': ' + errBody.substring(0, 200));
    }

    // 解析 SSE 流
    var text = await resp.text();
    var result = parseSSEResponse(text);

    // 更新会话状态
    if (result.conversationId) {
      this._conversationId = result.conversationId;
    }
    if (result.messageId) {
      this._parentMessageId = result.messageId;
    }

    return result;
  }

  /**
   * 调用 Codex API（已验证可用，无需 sentinel token）
   *
   * @param {string} message - 用户消息
   * @param {object} opts - 选项
   *   opts.model — 模型 (默认 'gpt-5-codex-mini')
   *   opts.instructions — 系统指令
   *   opts.timeout — 超时毫秒数
   *   opts.onDelta — 流式回调 function(deltaText)
   * @returns {{ text, responseId, model, usage }}
   */
  async codex(message, opts) {
    opts = opts || {};
    var token = await this.getAccessToken();
    if (!token) {
      throw new Error('无可用 access_token');
    }

    var model = opts.model || 'gpt-5-codex-mini';
    var instructions = opts.instructions || 'You are a helpful coding assistant. Respond concisely.';

    var body = {
      model: model,
      instructions: instructions,
      input: [
        {
          type: 'message',
          role: 'user',
          content: [
            { type: 'input_text', text: message }
          ]
        }
      ],
      tools: [],
      tool_choice: 'auto',
      parallel_tool_calls: false,
      reasoning: { summary: 'auto' },
      store: false,
      stream: true,
      include: ['reasoning.encrypted_content'],
    };

    var headers = {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json',
      'Accept': 'text/event-stream',
      'User-Agent': UA,
      'originator': 'codex_cli_rs',
    };
    if (this._accountId) {
      headers['chatgpt-account-id'] = this._accountId;
    }

    var resp = await fetch(CHAT_API + '/codex/responses', {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(opts.timeout || 120000),
    });

    if ((resp.status === 401 || resp.status === 403) && !opts._retried) {
      log('🔄', C.yellow, 'Codex API ' + resp.status + '，刷新 token 后重试...');
      await this._refreshToken();
      return this.codex(message, Object.assign({}, opts, { _retried: true }));
    }

    if (!resp.ok) {
      var errBody = await resp.text().catch(function () { return ''; });
      throw new Error('Codex API ' + resp.status + ': ' + errBody.substring(0, 300));
    }

    // 解析 Responses API SSE 流
    var text = await resp.text();
    return parseResponsesSSE(text, opts.onDelta);
  }

  /**
   * 获取当前用户信息
   */
  async getMe() {
    var token = await this.getAccessToken();
    var resp = await fetch(CHAT_API + '/me', {
      headers: {
        'Authorization': 'Bearer ' + token,
        'User-Agent': UA,
      },
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) {
      throw new Error('/me returned ' + resp.status);
    }
    return await resp.json();
  }

  /**
   * 重置会话（新对话）
   */
  resetConversation() {
    this._conversationId = null;
    this._parentMessageId = null;
  }

  /**
   * 导出当前凭证（可保存供下次使用）
   */
  exportCredentials() {
    return {
      email: this._email,
      accessToken: this._accessToken,
      sessionToken: this._sessionToken,
      cookies: this._cookies,
      tokenExpiresAt: this._tokenExpiresAt,
    };
  }

  // === 内部方法 ===

  async _refreshToken() {
    if (!this._sessionToken) {
      throw new Error('无 sessionToken，无法续期');
    }

    var cookieStr = '__Secure-next-auth.session-token=' + this._sessionToken;
    // 加上其他重要 cookies
    if (this._cookies['oai-did']) {
      cookieStr += '; oai-did=' + this._cookies['oai-did'];
    }
    if (this._cookies['cf_clearance']) {
      cookieStr += '; cf_clearance=' + this._cookies['cf_clearance'];
    }
    if (this._cookies['__cf_bm']) {
      cookieStr += '; __cf_bm=' + this._cookies['__cf_bm'];
    }

    var resp = await fetch(AUTH_SESSION_API, {
      headers: {
        'User-Agent': UA,
        'Cookie': cookieStr,
        'Accept': 'application/json',
      },
      signal: AbortSignal.timeout(15000),
    });

    if (!resp.ok) {
      throw new Error('续期失败: HTTP ' + resp.status);
    }

    var data = await resp.json();

    if (data.accessToken) {
      this._accessToken = data.accessToken;
      this._tokenExpiresAt = parseJwtExp(data.accessToken);
      var authInfo = parseJwtAuth(data.accessToken);
      this._accountId = authInfo.chatgpt_account_id || this._accountId;
      log('✅', C.green, 'access_token 续期成功，有效至 ' + new Date(this._tokenExpiresAt * 1000).toLocaleString());
    } else {
      throw new Error('续期响应中无 accessToken');
    }

    // 更新 session token（如果返回了新的 set-cookie）
    var setCookies = resp.headers.getSetCookie ? resp.headers.getSetCookie() : [];
    for (var i = 0; i < setCookies.length; i++) {
      var parts = setCookies[i].split(';')[0].split('=');
      if (parts[0].trim() === '__Secure-next-auth.session-token') {
        this._sessionToken = parts.slice(1).join('=').trim();
      }
    }
  }
}

// ============ 辅助函数 ============

function parseJwtExp(jwt) {
  try {
    var payload = jwt.split('.')[1];
    var decoded = JSON.parse(Buffer.from(payload, 'base64url').toString());
    return decoded.exp || 0;
  } catch (e) {
    return 0;
  }
}

function parseJwtAuth(jwt) {
  try {
    var payload = jwt.split('.')[1];
    var decoded = JSON.parse(Buffer.from(payload, 'base64url').toString());
    return decoded['https://api.openai.com/auth'] || {};
  } catch (e) {
    return {};
  }
}

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    var r = Math.random() * 16 | 0;
    var v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * 解析 Responses API SSE 流（Codex 使用）
 * 事件类型: response.output_text.delta, response.completed, etc.
 */
function parseResponsesSSE(text, onDelta) {
  var lines = text.split('\n');
  var output = '';
  var responseId = null;
  var modelUsed = null;
  var usage = null;

  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim();
    if (!line.startsWith('data: ')) continue;

    try {
      var data = JSON.parse(line.substring(6));

      // 文本增量
      if (data.type === 'response.output_text.delta' && data.delta) {
        output += data.delta;
        if (onDelta) onDelta(data.delta);
      }

      // 响应完成
      if (data.type === 'response.completed' && data.response) {
        responseId = data.response.id || null;
        modelUsed = data.response.model || null;
        usage = data.response.usage || null;

        // 从完成事件中提取完整文本（作为兜底）
        if (!output && data.response.output) {
          for (var j = 0; j < data.response.output.length; j++) {
            var item = data.response.output[j];
            if (item.type === 'message' && item.content) {
              for (var k = 0; k < item.content.length; k++) {
                if (item.content[k].text) {
                  output += item.content[k].text;
                }
              }
            }
          }
        }
      }

      // 响应创建（拿 ID）
      if (data.type === 'response.created' && data.response) {
        responseId = data.response.id || responseId;
      }
    } catch (e) {
      // skip malformed
    }
  }

  return {
    text: output,
    responseId: responseId,
    model: modelUsed,
    usage: usage,
  };
}

function parseSSEResponse(text) {
  var lines = text.split('\n');
  var lastData = null;

  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim();
    if (line.startsWith('data: ') && line !== 'data: [DONE]') {
      try {
        lastData = JSON.parse(line.substring(6));
      } catch (e) {
        // skip malformed
      }
    }
  }

  if (!lastData) {
    return { text: '', conversationId: null, messageId: null };
  }

  var reply = '';
  if (lastData.message && lastData.message.content && lastData.message.content.parts) {
    reply = lastData.message.content.parts.join('');
  }

  return {
    text: reply,
    conversationId: lastData.conversation_id || null,
    messageId: lastData.message ? lastData.message.id : null,
    model: lastData.message ? lastData.message.metadata.model_slug : null,
    raw: lastData,
  };
}
