/**
 * 调度器 — 多账号选择策略
 *
 *
 * 从 config scheduler.mode 读取策略名，零硬编码。
 */

/**
 *
 * 最简单的策略：index++，均匀分配
 */
function createRoundRobin() {
  var index = 0;

  return {
    name: 'round_robin',
    select: function (accounts) {
      if (accounts.length === 0) return null;
      var account = accounts[index % accounts.length];
      index++;
      return account;
    },
  };
}

/**
 *
 * 粘性选择：持续使用同一个账号，直到触发 429 才切换
 * 适合「榨干一个再换下一个」的场景
 */
function createQuotaExhausted() {
  var currentEmail = null;

  return {
    name: 'quota_exhausted',
    select: function (accounts) {
      if (accounts.length === 0) return null;

      // 如果当前账号还在可用列表中，继续用它
      if (currentEmail) {
        for (var i = 0; i < accounts.length; i++) {
          if (accounts[i].email === currentEmail) {
            return accounts[i];
          }
        }
      }

      // 当前账号不可用了（被冷却/废弃），换第一个
      currentEmail = accounts[0].email;
      return accounts[0];
    },
  };
}

/**
 *
 * 选 request_count 最少的账号，均衡负载
 */
function createLeastUsed() {
  return {
    name: 'least_used',
    select: function (accounts) {
      if (accounts.length === 0) return null;

      var min = accounts[0];
      for (var i = 1; i < accounts.length; i++) {
        if (accounts[i].request_count < min.request_count) {
          min = accounts[i];
        }
      }
      return min;
    },
  };
}

/**
 * 创建调度器实例
 *
 * @param {string} mode - 策略名（从 config.scheduler.mode 读取）
 * @returns {Scheduler}
 */
export function createScheduler(mode) {
  switch (mode) {
    case 'round_robin':
      return createRoundRobin();
    case 'quota_exhausted':
      return createQuotaExhausted();
    case 'least_used':
      return createLeastUsed();
    default:
      return createRoundRobin();
  }
}
