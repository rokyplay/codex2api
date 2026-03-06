/**
 * RokMail 临时邮箱 API 封装
 */

import { log, C, sleep, extractCode } from './utils.mjs';

/**
 * 创建一个邮件会话（token + 邮箱）
 */
export async function createMailSession(apiUrl, domain) {
  // Step 1: 获取 token
  log('📧', C.blue, '创建临时邮箱 token...');

  var tokenResp = await fetch(apiUrl + '/api/temp/token', {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  var tokenData = await tokenResp.json();

  if (!tokenData.success) {
    throw new Error('创建 token 失败: ' + JSON.stringify(tokenData));
  }

  var token = tokenData.data.token;
  // 已登录用户可能没有 token 返回
  if (!token) {
    throw new Error('未返回 token（可能需要非登录态调用）');
  }

  log('🔑', C.gray, 'Token: ' + token.substring(0, 12) + '...');

  // Step 2: 创建邮箱
  var mbResp = await fetch(apiUrl + '/api/temp/mailbox', {
    method: 'POST',
    headers: {
      'X-Temp-Token': token,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ domain: domain }),
    signal: AbortSignal.timeout(10000),
  });
  var mbData = await mbResp.json();

  if (!mbData.success) {
    throw new Error('创建邮箱失败: ' + JSON.stringify(mbData));
  }

  var address = mbData.data.address;
  log('📬', C.green, '临时邮箱: ' + C.bold + address + C.reset);

  return {
    token: token,
    address: address,
    domain: domain,
    expiresAt: mbData.data.expires_at,
  };
}

/**
 * 轮询等待验证码邮件
 */
export async function waitForCode(apiUrl, session, opts) {
  opts = opts || {};
  var pollInterval = opts.pollIntervalMs || 3000;
  var timeout = opts.pollTimeoutMs || 120000;
  var startTime = Date.now();

  log('⏳', C.yellow, '等待验证码邮件... (超时 ' + (timeout / 1000) + 's)');

  while (Date.now() - startTime < timeout) {
    var resp = await fetch(
      apiUrl + '/api/temp/mailbox/' + encodeURIComponent(session.address) + '/emails',
      {
        headers: { 'X-Temp-Token': session.token },
        signal: AbortSignal.timeout(10000),
      }
    );
    var data = await resp.json();

    if (data.success && data.data.total > 0) {
      var emails = data.data.emails;
      // 找最新的一封
      var latest = emails[0];
      log('📨', C.green, '收到邮件! From: ' + latest.sender + ' Subject: ' + (latest.subject || '').substring(0, 50));

      // 获取邮件详情
      var detailResp = await fetch(
        apiUrl + '/api/temp/mailbox/' + encodeURIComponent(session.address) + '/emails/' + latest.id,
        {
          headers: { 'X-Temp-Token': session.token },
          signal: AbortSignal.timeout(10000),
        }
      );
      var detailData = await detailResp.json();

      if (!detailData.success) {
        log('⚠️', C.yellow, '获取邮件详情失败，继续等待...');
        await sleep(pollInterval);
        continue;
      }

      var emailDetail = detailData.data;

      // 从邮件文本中提取验证码
      var code = extractCode(emailDetail.body_text);
      if (!code && emailDetail.body_html) {
        // 尝试从 HTML 中提取（去掉标签后再搜索）
        var plainFromHtml = emailDetail.body_html
          .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
          .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
          .replace(/<[^>]+>/g, ' ')
          .replace(/&nbsp;/g, ' ')
          .replace(/\s+/g, ' ');
        code = extractCode(plainFromHtml);
      }
      // 也检查标题
      if (!code) {
        code = extractCode(emailDetail.subject);
      }

      if (code) {
        log('🔢', C.green + C.bold, '验证码: ' + code);
        return code;
      }

      // 没找到验证码，可能是链接验证方式
      log('⚠️', C.yellow, '邮件中未找到数字验证码，尝试查找验证链接...');

      // 查找验证链接
      var linkMatch = (emailDetail.body_html || emailDetail.body_text || '').match(
        /https?:\/\/[^\s"'<>]+(?:verify|confirm|activate|auth)[^\s"'<>]*/i
      );
      if (linkMatch) {
        log('🔗', C.cyan, '找到验证链接: ' + linkMatch[0].substring(0, 80) + '...');
        return { type: 'link', url: linkMatch[0] };
      }

      log('❌', C.red, '邮件中既没有验证码也没有验证链接');
      log('📄', C.gray, '邮件内容前200字: ' + (emailDetail.body_text || '').substring(0, 200));
      return null;
    }

    var elapsed = Math.round((Date.now() - startTime) / 1000);
    process.stdout.write('\r' + C.gray + '[' + elapsed + 's]' + C.reset + ' ⏳ 等待邮件中...   ');
    await sleep(pollInterval);
  }

  process.stdout.write('\r' + ' '.repeat(60) + '\r');
  log('❌', C.red, '等待验证码超时 (' + (timeout / 1000) + 's)');
  return null;
}

/**
 * 销毁临时邮箱
 */
export async function destroyMailbox(apiUrl, session) {
  try {
    await fetch(
      apiUrl + '/api/temp/mailbox/' + encodeURIComponent(session.address),
      {
        method: 'DELETE',
        headers: { 'X-Temp-Token': session.token },
        signal: AbortSignal.timeout(5000),
      }
    );
    log('🗑️', C.gray, '已销毁邮箱: ' + session.address);
  } catch (e) {
    // 忽略清理失败
  }
}
