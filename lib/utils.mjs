/**
 * 工具函数模块
 */

// ANSI 颜色
export var C = {
  reset: '\x1b[0m', red: '\x1b[31m', green: '\x1b[32m', yellow: '\x1b[33m',
  blue: '\x1b[34m', magenta: '\x1b[35m', cyan: '\x1b[36m', gray: '\x1b[90m',
  bold: '\x1b[1m', dim: '\x1b[2m',
};

export function sleep(ms) {
  return new Promise(function (r) { setTimeout(r, ms); });
}

export function randInt(a, b) {
  return Math.floor(Math.random() * (b - a + 1)) + a;
}

export function randDelay(minMs, maxMs) {
  return sleep(randInt(minMs, maxMs));
}

export function timestamp() {
  // 使用 UTC ISO-8601，保证所有日志行时间格式可机器稳定解析。
  return new Date().toISOString();
}

export function log(icon, color, msg) {
  console.log(C.gray + '[' + timestamp() + ']' + C.reset + ' ' + icon + ' ' + color + msg + C.reset);
}

// 常见英文名
var FIRST_NAMES = [
  'James', 'Mary', 'Robert', 'Patricia', 'John', 'Jennifer', 'Michael', 'Linda',
  'David', 'Elizabeth', 'William', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
  'Thomas', 'Sarah', 'Charles', 'Karen', 'Daniel', 'Lisa', 'Matthew', 'Nancy',
  'Anthony', 'Betty', 'Mark', 'Margaret', 'Donald', 'Sandra', 'Steven', 'Ashley',
  'Paul', 'Dorothy', 'Andrew', 'Kimberly', 'Joshua', 'Emily', 'Kenneth', 'Donna',
  'Kevin', 'Michelle', 'Brian', 'Carol', 'George', 'Amanda', 'Timothy', 'Melissa',
  'Ronald', 'Deborah', 'Edward', 'Stephanie', 'Jason', 'Rebecca', 'Jeffrey', 'Sharon',
  'Ryan', 'Laura', 'Jacob', 'Cynthia', 'Gary', 'Kathleen', 'Nicholas', 'Amy',
  'Eric', 'Angela', 'Jonathan', 'Shirley', 'Stephen', 'Anna', 'Larry', 'Brenda',
  'Justin', 'Pamela', 'Scott', 'Emma', 'Brandon', 'Nicole', 'Benjamin', 'Helen',
  'Samuel', 'Samantha', 'Raymond', 'Katherine', 'Gregory', 'Christine', 'Frank', 'Debra',
  'Alexander', 'Rachel', 'Patrick', 'Carolyn', 'Jack', 'Janet', 'Dennis', 'Catherine',
];

var LAST_NAMES = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
  'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
  'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
  'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
  'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
  'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker',
  'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris', 'Morales', 'Murphy',
  'Cook', 'Rogers', 'Gutierrez', 'Ortiz', 'Morgan', 'Cooper', 'Peterson', 'Bailey',
  'Reed', 'Kelly', 'Howard', 'Ramos', 'Kim', 'Cox', 'Ward', 'Richardson',
];

export function randomFirstName() {
  return FIRST_NAMES[randInt(0, FIRST_NAMES.length - 1)];
}

export function randomLastName() {
  return LAST_NAMES[randInt(0, LAST_NAMES.length - 1)];
}

/**
 * 生成随机密码（12位+，满足 OpenAI 要求）
 */
export function randomPassword(prefix) {
  prefix = prefix || 'Gpt2026!';
  var chars = 'abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  var suffix = '';
  for (var i = 0; i < 8; i++) {
    suffix += chars[randInt(0, chars.length - 1)];
  }
  return prefix + suffix;
}

/**
 * 生成随机生日（18-40岁）
 */
export function randomBirthday() {
  var year = randInt(2000, 2008);
  var month = randInt(1, 12);
  var day = randInt(1, 28);
  return {
    year: year,
    month: String(month).padStart(2, '0'),
    day: String(day).padStart(2, '0'),
    str: year + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0'),
  };
}

/**
 * 从邮件文本中提取 6 位数字验证码（兜底方案）
 */
export function extractCode(text) {
  if (!text) return null;
  // 优先匹配 "code is XXXXXX" 或 "code: XXXXXX" 模式
  var patterns = [
    /(?:code|验证码|verify)[:\s]+(\d{6})/i,
    /\b(\d{6})\b/,
  ];
  for (var i = 0; i < patterns.length; i++) {
    var m = text.match(patterns[i]);
    if (m) return m[1];
  }
  return null;
}
