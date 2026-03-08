import crypto from 'node:crypto';

var BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
var BASE32_LOOKUP = Object.create(null);

for (var i = 0; i < BASE32_ALPHABET.length; i++) {
  BASE32_LOOKUP[BASE32_ALPHABET[i]] = i;
}

function normalizeBase32(input) {
  return String(input || '')
    .toUpperCase()
    .replace(/=+$/g, '')
    .replace(/[\s-]+/g, '');
}

function encodeBufferToBase32(buffer) {
  var bits = 0;
  var value = 0;
  var output = '';

  for (var i = 0; i < buffer.length; i++) {
    value = (value << 8) | buffer[i];
    bits += 8;
    while (bits >= 5) {
      output += BASE32_ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }

  if (bits > 0) {
    output += BASE32_ALPHABET[(value << (5 - bits)) & 31];
  }

  return output;
}

export function decodeBase32ToBuffer(base32) {
  var normalized = normalizeBase32(base32);
  if (!normalized) return Buffer.alloc(0);

  var bits = 0;
  var value = 0;
  var bytes = [];

  for (var i = 0; i < normalized.length; i++) {
    var ch = normalized[i];
    var idx = BASE32_LOOKUP[ch];
    if (idx === undefined) {
      throw new Error('Invalid Base32 character: ' + ch);
    }
    value = (value << 5) | idx;
    bits += 5;

    while (bits >= 8) {
      bytes.push((value >>> (bits - 8)) & 255);
      bits -= 8;
    }
  }

  return Buffer.from(bytes);
}

export function generateRandomBase32Secret(bytes) {
  var size = Number.isFinite(bytes) && bytes > 0 ? Math.floor(bytes) : 20;
  return encodeBufferToBase32(crypto.randomBytes(size));
}

export function generateTotpCode(options) {
  var secretBase32 = options && options.secretBase32;
  var timestampMs = Number.isFinite(options && options.timestampMs) ? options.timestampMs : Date.now();
  var stepSeconds = Number.isFinite(options && options.stepSeconds) && options.stepSeconds > 0
    ? Math.floor(options.stepSeconds)
    : 30;
  var digits = Number.isFinite(options && options.digits) && options.digits > 0
    ? Math.floor(options.digits)
    : 6;

  var secret = decodeBase32ToBuffer(secretBase32);
  if (!secret.length) {
    throw new Error('TOTP secret is empty');
  }

  var counter = BigInt(Math.floor(timestampMs / 1000 / stepSeconds));
  var counterBuffer = Buffer.alloc(8);
  counterBuffer.writeBigUInt64BE(counter);

  var hmac = crypto.createHmac('sha1', secret).update(counterBuffer).digest();
  var offset = hmac[hmac.length - 1] & 15;
  var binary =
    ((hmac[offset] & 127) << 24) |
    ((hmac[offset + 1] & 255) << 16) |
    ((hmac[offset + 2] & 255) << 8) |
    (hmac[offset + 3] & 255);

  var mod = Math.pow(10, digits);
  var code = binary % mod;
  return String(code).padStart(digits, '0');
}

function normalizeCode(code, digits) {
  if (code === null || code === undefined) return '';
  var normalized = String(code).trim().replace(/\s+/g, '');
  if (!/^\d+$/.test(normalized)) return '';
  if (normalized.length > digits) return '';
  return normalized.padStart(digits, '0');
}

function timingSafeCodeEqual(a, b) {
  var strA = String(a || '');
  var strB = String(b || '');
  var len = Math.max(strA.length, strB.length, 1);
  var bufA = Buffer.alloc(len);
  var bufB = Buffer.alloc(len);
  bufA.write(strA, 0, 'utf8');
  bufB.write(strB, 0, 'utf8');
  var matched = crypto.timingSafeEqual(bufA, bufB);
  return matched && strA.length === strB.length;
}

export function verifyTotpCode(options) {
  var secretBase32 = options && options.secretBase32;
  var code = options && options.code;
  var timestampMs = Number.isFinite(options && options.timestampMs) ? options.timestampMs : Date.now();
  var stepSeconds = Number.isFinite(options && options.stepSeconds) && options.stepSeconds > 0
    ? Math.floor(options.stepSeconds)
    : 30;
  var digits = Number.isFinite(options && options.digits) && options.digits > 0
    ? Math.floor(options.digits)
    : 6;
  var window = Number.isFinite(options && options.window) && options.window >= 0
    ? Math.floor(options.window)
    : 1;

  var provided = normalizeCode(code, digits);
  var matched = false;
  var compareValue = provided || ''.padStart(digits, '0');

  try {
    for (var offset = -window; offset <= window; offset++) {
      var expected = generateTotpCode({
        secretBase32: secretBase32,
        timestampMs: timestampMs + offset * stepSeconds * 1000,
        stepSeconds: stepSeconds,
        digits: digits,
      });
      if (timingSafeCodeEqual(compareValue, expected) && provided) {
        matched = true;
      }
    }
  } catch (_) {
    return false;
  }

  return matched;
}

export function buildOtpAuthUri(options) {
  var secretBase32 = normalizeBase32(options && options.secretBase32);
  var issuer = (options && options.issuer) || 'codex2api';
  var accountName = (options && options.accountName) || '';
  var digits = Number.isFinite(options && options.digits) && options.digits > 0
    ? Math.floor(options.digits)
    : 6;
  var period = Number.isFinite(options && options.period) && options.period > 0
    ? Math.floor(options.period)
    : 30;

  if (!secretBase32) {
    throw new Error('TOTP secret is required');
  }
  if (!accountName) {
    throw new Error('TOTP accountName is required');
  }

  var label = encodeURIComponent(issuer + ':' + accountName);
  var params = new URLSearchParams({
    secret: secretBase32,
    issuer: issuer,
    algorithm: 'SHA1',
    digits: String(digits),
    period: String(period),
  });

  return 'otpauth://totp/' + label + '?' + params.toString();
}
