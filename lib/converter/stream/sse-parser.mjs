/**
 * 通用 SSE 流解析器
 *
 *   - session.mjs parseResponsesSSE() — 已验证的 Codex SSE 解析逻辑
 *
 * 功能:
 *   - 从 ReadableStream 逐行读取 SSE
 *   - 处理 "event:" 和 "data:" 行
 *   - 支持多行 data（按 SSE 规范拼接）
 *   - 回调: onEvent(eventType, parsedData)
 */

/**
 * 从 fetch Response 的 body 流逐行解析 SSE 事件
 *
 * @param {ReadableStream} stream - response.body
 * @param {function} onEvent - 回调 (eventType, data)
 * @param {object} opts
 *   opts.onRawLine — 原始行回调（调试用）
 *   opts.firstByteTimeoutMs — 首个事件超时（毫秒）
 *   opts.idleTimeoutMs — 事件间空闲超时（毫秒）
 * @returns {Promise<{ event_count: number, first_event_at: number, last_event_at: number }>}
 */
export async function parseSSEStream(stream, onEvent, opts) {
  opts = opts || {};
  var reader = stream.getReader();
  var decoder = new TextDecoder();
  var buffer = '';
  var currentEvent = '';
  var dataLines = [];
  var eventCount = 0;
  var firstEventAt = 0;
  var lastEventAt = 0;
  var hasDispatchedEvent = false;
  var firstByteTimeoutMs = Number(opts.firstByteTimeoutMs || 0);
  var idleTimeoutMs = Number(opts.idleTimeoutMs || 0);

  function createSSETimeoutError(kind, timeoutMs) {
    var timeout = Number(timeoutMs || 0);
    var err = new Error('sse_' + kind + '_after_' + timeout + 'ms');
    err.name = 'SSETimeoutError';
    err.code = kind;
    err.timeout_ms = timeout;
    err.retryable = true;
    return err;
  }

  async function readWithTimeout(timeoutMs, kind) {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      return reader.read();
    }
    var timer = null;
    var timeoutPromise = new Promise(function (_, reject) {
      timer = setTimeout(function () {
        reject(createSSETimeoutError(kind, timeoutMs));
      }, timeoutMs);
    });
    try {
      return await Promise.race([reader.read(), timeoutPromise]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  function dispatchEvent(eventType, data) {
    eventCount++;
    lastEventAt = Date.now();
    if (!firstEventAt) firstEventAt = lastEventAt;
    hasDispatchedEvent = true;
    onEvent(eventType, data);
  }

  try {
    while (true) {
      var timeoutMs = hasDispatchedEvent ? idleTimeoutMs : firstByteTimeoutMs;
      var timeoutKind = hasDispatchedEvent ? 'idle_timeout' : 'first_byte_timeout';
      var result = await readWithTimeout(timeoutMs, timeoutKind);
      if (result.done) break;

      buffer += decoder.decode(result.value, { stream: true });
      var lines = buffer.split('\n');
      // 最后一个可能不完整，留到下次
      buffer = lines.pop() || '';

      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];
        if (opts.onRawLine) opts.onRawLine(line);

        if (line.startsWith('event:')) {
          var eventContent = line.substring(6);
          if (eventContent.charAt(0) === ' ') eventContent = eventContent.substring(1);
          currentEvent = eventContent.trim();
        } else if (line.startsWith('data:')) {
          var dataContent = line.substring(5);
          if (dataContent.charAt(0) === ' ') dataContent = dataContent.substring(1);
          dataLines.push(dataContent);
        } else if (line.trim() === '') {
          // 空行 = 事件结束，分发
          if (dataLines.length > 0) {
            var rawData = dataLines.join('\n');
            dataLines = [];

            if (rawData === '[DONE]') {
              dispatchEvent('done', null);
            } else {
              try {
                var parsed = JSON.parse(rawData);
                dispatchEvent(currentEvent || 'message', parsed);
              } catch (e) {
                dispatchEvent('parse_error', { raw: rawData, error: e.message });
              }
            }
            currentEvent = '';
          }
        }
      }
    }

    // 处理尾部残留
    if (buffer.trim()) {
      if (buffer.startsWith('data:')) {
        var tailContent = buffer.substring(5);
        if (tailContent.charAt(0) === ' ') tailContent = tailContent.substring(1);
        var tail = tailContent;
        if (tail !== '[DONE]') {
          try {
            dispatchEvent(currentEvent || 'message', JSON.parse(tail));
          } catch (e) {
            // ignore
          }
        } else {
          dispatchEvent('done', null);
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  return {
    event_count: eventCount,
    first_event_at: firstEventAt,
    last_event_at: lastEventAt,
  };
}

/**
 * 从 SSE 文本（一次性获取的完整响应体）解析所有事件
 *
 * @param {string} text - 完整 SSE 文本
 * @param {function} onEvent - 回调 (eventType, data)
 */
export function parseSSEText(text, onEvent) {
  var lines = text.split('\n');
  var currentEvent = '';
  var dataLines = [];

  for (var i = 0; i < lines.length; i++) {
    var line = lines[i];

    if (line.startsWith('event:')) {
      var eventContent = line.substring(6);
      if (eventContent.charAt(0) === ' ') eventContent = eventContent.substring(1);
      currentEvent = eventContent.trim();
    } else if (line.startsWith('data:')) {
      var dataContent = line.substring(5);
      if (dataContent.charAt(0) === ' ') dataContent = dataContent.substring(1);
      dataLines.push(dataContent);
    } else if (line.trim() === '') {
      if (dataLines.length > 0) {
        var rawData = dataLines.join('\n');
        dataLines = [];

        if (rawData === '[DONE]') {
          onEvent('done', null);
        } else {
          try {
            var parsed = JSON.parse(rawData);
            onEvent(currentEvent || 'message', parsed);
          } catch (e) {
            // skip malformed
          }
        }
        currentEvent = '';
      }
    }
  }

  // 尾部残留
  if (dataLines.length > 0) {
    var tail = dataLines.join('\n');
    if (tail === '[DONE]') {
      onEvent('done', null);
    } else {
      try {
        onEvent(currentEvent || 'message', JSON.parse(tail));
      } catch (e) {
        // skip
      }
    }
  }
}

/**
 * 将 SSE 事件格式化为标准 SSE 行
 *
 * @param {string} eventType - 事件类型（可选）
 * @param {object|string} data - 数据
 * @returns {string} SSE 格式文本
 */
export function formatSSELine(eventType, data) {
  var out = '';
  if (eventType) {
    out += 'event: ' + eventType + '\n';
  }
  if (typeof data === 'string') {
    out += 'data: ' + data + '\n\n';
  } else {
    out += 'data: ' + JSON.stringify(data) + '\n\n';
  }
  return out;
}
