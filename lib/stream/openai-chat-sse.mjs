/**
 * OpenAI Chat Completions SSE 生成工具
 *
 */

/**
 * SSE 响应头
 */
export var SSE_HEADERS = {
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache',
  'Connection': 'keep-alive',
  'X-Accel-Buffering': 'no',
};

/**
 * 创建心跳定时器（防止 CF/nginx 超时）
 *
 * @param {ServerResponse} res
 * @param {number} intervalMs - 心跳间隔（默认 15 秒）
 * @returns {{ stop: function }}
 */
export function createHeartbeat(res, intervalMs) {
  var interval = intervalMs || 15000;
  var timer = setInterval(function () {
    if (!res.writableEnded) {
      res.write(': heartbeat\n\n');
    } else {
      clearInterval(timer);
    }
  }, interval);

  return {
    stop: function () { clearInterval(timer); },
  };
}
