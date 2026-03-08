/**
 * Codex Responses API SSE 解析工具
 *
 * 封装 openai-responses.mjs 的 parseSSEEvent，
 * 配合 sse-parser.mjs 的 parseSSEStream 使用
 */

import { parseSSEEvent, createParseState } from '../openai-responses.mjs';

/**
 * 创建 Codex Responses SSE 流处理管道
 *
 * 用法:
 *   var pipeline = createCodexSSEPipeline(onUniversalEvent);
 *   await parseSSEStream(response.body, pipeline.onEvent);
 *
 * @param {function} onUniversalEvent - 回调(UniversalStreamEvent)
 * @returns {{ onEvent: function }}
 */
export function createCodexSSEPipeline(onUniversalEvent) {
  var parseState = createParseState();
  return {
    onEvent: function (eventType, data) {
      var universalEvent = parseSSEEvent(eventType, data, parseState);
      if (universalEvent) {
        var events = Array.isArray(universalEvent) ? universalEvent : [universalEvent];
        for (var i = 0; i < events.length; i++) {
          if (events[i]) onUniversalEvent(events[i]);
        }
      }
    },
  };
}
