/**
 * 统一真实客户端 IP 提取
 *
 * 返回:
 *   {
 *     ip: string,
 *     ip_source: 'cf-connecting-ip'|'true-client-ip'|'x-forwarded-for'|'x-real-ip'|'remote-address'|'unknown',
 *     ip_chain: string[]
 *   }
 */

function toSingleHeaderValue(value) {
  if (Array.isArray(value)) return String(value[0] || '').trim();
  return String(value || '').trim();
}

function normalizeIp(raw) {
  var ip = String(raw || '').trim();
  if (!ip) return '';
  if (ip.indexOf(',') >= 0) {
    ip = ip.split(',')[0].trim();
  }
  if (ip.indexOf(':') >= 0 && ip.indexOf('::ffff:') === 0) {
    ip = ip.slice('::ffff:'.length);
  }
  return ip;
}

function splitForwardedFor(rawValue) {
  var value = toSingleHeaderValue(rawValue);
  if (!value) return [];
  var list = value.split(',');
  var out = [];
  for (var i = 0; i < list.length; i++) {
    var ip = normalizeIp(list[i]);
    if (!ip) continue;
    out.push(ip);
  }
  return out;
}

function pushUnique(list, value) {
  if (!value) return;
  if (list.indexOf(value) >= 0) return;
  list.push(value);
}

export function getRealClientIp(req) {
  var headers = (req && req.headers) || {};
  var chain = [];

  var cfIp = normalizeIp(toSingleHeaderValue(headers['cf-connecting-ip']));
  var trueClientIp = normalizeIp(toSingleHeaderValue(headers['true-client-ip']));
  var forwardedChain = splitForwardedFor(headers['x-forwarded-for']);
  var xRealIp = normalizeIp(toSingleHeaderValue(headers['x-real-ip']));
  var remoteAddress = normalizeIp(
    (req && req.socket && req.socket.remoteAddress)
      || (req && req.connection && req.connection.remoteAddress)
      || ''
  );

  pushUnique(chain, cfIp);
  pushUnique(chain, trueClientIp);
  for (var i = 0; i < forwardedChain.length; i++) {
    pushUnique(chain, forwardedChain[i]);
  }
  pushUnique(chain, xRealIp);
  pushUnique(chain, remoteAddress);

  if (cfIp) {
    return { ip: cfIp, ip_source: 'cf-connecting-ip', ip_chain: chain };
  }
  if (trueClientIp) {
    return { ip: trueClientIp, ip_source: 'true-client-ip', ip_chain: chain };
  }
  if (forwardedChain.length > 0) {
    return { ip: forwardedChain[0], ip_source: 'x-forwarded-for', ip_chain: chain };
  }
  if (xRealIp) {
    return { ip: xRealIp, ip_source: 'x-real-ip', ip_chain: chain };
  }
  if (remoteAddress) {
    return { ip: remoteAddress, ip_source: 'remote-address', ip_chain: chain };
  }
  return { ip: '', ip_source: 'unknown', ip_chain: chain };
}

