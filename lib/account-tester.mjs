var TAG = '[AccountTester]';

/**
 * 对单个账号发真实 Codex API 请求，仅返回测试结果，不产生任何 pool 副作用
 * @param {object} acc - 必须含 accessToken, email, accountId(可选)
 * @param {string} model - 测试用模型名
 * @returns {Promise<{ok: boolean, latency: number, status?: number, error?: string, networkError?: boolean}>}
 */
export async function testOneAccount(acc, model) {
  void TAG;
  var startTime = Date.now();
  try {
    var testBody = JSON.stringify({
      model: model,
      instructions: 'Reply with ok.',
      input: [{ role: 'user', content: [{ type: 'input_text', text: 'Say ok' }] }],
      stream: true,
      store: false,
    });
    var testHeaders = {
      'Authorization': 'Bearer ' + acc.accessToken,
      'Content-Type': 'application/json',
      'Accept': 'text/event-stream',
      'originator': 'codex_cli_rs',
    };
    if (acc.accountId) {
      testHeaders['chatgpt-account-id'] = acc.accountId;
    }
    var resp = await fetch('https://chatgpt.com/backend-api/codex/responses', {
      method: 'POST',
      headers: testHeaders,
      body: testBody,
      signal: AbortSignal.timeout(30000),
    });
    var latency = Date.now() - startTime;

    if (resp.ok) {
      var reader = resp.body.getReader();
      await reader.read();
      reader.cancel();
      return { ok: true, latency: latency, status: resp.status };
    }

    var errText = await resp.text().catch(function() { return ''; });
    var errSnippet = errText.substring(0, 200);
    return {
      ok: false,
      latency: latency,
      status: resp.status,
      error: errSnippet,
      networkError: false,
    };
  } catch (e) {
    return {
      ok: false,
      latency: Date.now() - startTime,
      error: e && e.message ? e.message : String(e),
      networkError: true,
    };
  }
}
