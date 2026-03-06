/**
 * 格式转换工具函数
 *
 */

/**
 * 生成唯一 ID
 */
export function generateId(prefix) {
  prefix = prefix || 'chatcmpl-';
  return prefix + Date.now().toString(36) + Math.random().toString(36).substring(2, 8);
}

/**
 * 安全 JSON 解析
 */
export function safeParseJSON(str) {
  try { return JSON.parse(str); } catch (e) { return null; }
}

/**
 * 截断文本
 */
export function truncate(text, maxLen) {
  maxLen = maxLen || 200;
  if (!text || text.length <= maxLen) return text;
  return text.substring(0, maxLen) + '...';
}

/**
 * 从 JWT 解析过期时间
 */
export function parseJwtExp(jwt) {
  try {
    var payload = jwt.split('.')[1];
    var decoded = JSON.parse(Buffer.from(payload, 'base64url').toString());
    return decoded.exp || 0;
  } catch (e) {
    return 0;
  }
}

/**
 * 从 JWT 解析 auth 信息
 */
export function parseJwtAuth(jwt) {
  try {
    var payload = jwt.split('.')[1];
    var decoded = JSON.parse(Buffer.from(payload, 'base64url').toString());
    return decoded['https://api.openai.com/auth'] || {};
  } catch (e) {
    return {};
  }
}
