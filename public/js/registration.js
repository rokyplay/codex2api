/**
 * codex2api 管理面板 — 注册模块
 * 远程控制注册服务器的批量注册任务
 */

var Registration = (function () {
  'use strict';

  var _pollTimer = null;
  var _isRunning = false;
  var _currentPage = 1;

  /**
   * 初始化 — 进入页面时调用
   */
  function init() {
    _checkConnection();
    _checkJobStatus();
    _loadRemoteAccounts();
    _loadJobStats();
    _loadJobHistory();
    _bindEvents();
  }

  /**
   * 清理 — 离开页面时停止轮询
   */
  function cleanup() {
    _stopPolling();
  }

  // ─── 事件绑定 ──────────────────────────────────────────

  var _bound = false;

  function _bindEvents() {
    if (_bound) return;
    _bound = true;

    var startBtn = document.getElementById('regStartBtn');
    var stopBtn = document.getElementById('regStopBtn');
    var refreshBtn = document.getElementById('regRefreshAccounts');

    if (startBtn) {
      startBtn.addEventListener('click', function () {
        _startJob();
      });
    }
    if (stopBtn) {
      stopBtn.addEventListener('click', function () {
        _stopJob();
      });
    }
    if (refreshBtn) {
      refreshBtn.addEventListener('click', function () {
        _loadRemoteAccounts();
      });
    }
  }

  // ─── 连接检查 ──────────────────────────────────────────

  function _checkConnection() {
    var dot = document.getElementById('regStatusDot');
    var text = document.getElementById('regStatusText');
    if (text) text.textContent = t('registration.checking');
    if (dot) dot.className = 'reg-status-indicator reg-status-checking';

    api('GET', '/register/status')
      .then(function (data) {
        if (dot) dot.className = 'reg-status-indicator reg-status-online';
        if (text) text.textContent = t('registration.server_online');
        var proxyCheckbox = document.getElementById('regProxy');
        if (proxyCheckbox && data && data.proxy && typeof data.proxy.enabled === 'boolean') {
          proxyCheckbox.checked = data.proxy.enabled;
        }
      })
      .catch(function () {
        if (dot) dot.className = 'reg-status-indicator reg-status-offline';
        if (text) text.textContent = t('registration.server_offline');
      });
  }

  // ─── 任务控制 ──────────────────────────────────────────

  function _startJob() {
    var countInput = document.getElementById('regCount');
    var concurrencySelect = document.getElementById('regConcurrency');
    var proxyCheckbox = document.getElementById('regProxy');
    var autoUploadCheckbox = document.getElementById('regAutoUpload');

    var count = parseInt(countInput ? countInput.value : '20', 10) || 20;
    var concurrency = parseInt(concurrencySelect ? concurrencySelect.value : '10', 10) || 10;

    if (count < 1) {
      toast(t('registration.invalid_count'), 'error');
      return;
    }

    var opts = {
      count: count,
      concurrency: concurrency,
      proxy: proxyCheckbox ? proxyCheckbox.checked : true,
      auto_upload: autoUploadCheckbox ? autoUploadCheckbox.checked : true,
    };

    showConfirm(t('registration.confirm_start', { count: count }), t('registration.start'))
      .then(function (confirmed) {
        if (!confirmed) return;

        var startBtn = document.getElementById('regStartBtn');
        if (startBtn) startBtn.disabled = true;

        api('POST', '/register/start', opts)
          .then(function (data) {
            if (data.started) {
              toast(t('registration.job_started', { count: count }), 'success');
              _setRunning(true);
              _startPolling();
            } else {
              toast(t('registration.start_failed'), 'error');
            }
          })
          .catch(function (err) {
            toast(t('registration.start_failed') + ': ' + err.message, 'error');
          })
          .finally(function () {
            if (startBtn) startBtn.disabled = false;
          });
      });
  }

  function _stopJob() {
    api('POST', '/register/stop')
      .then(function (data) {
        if (data.stopped) {
          toast(t('registration.job_stopping'), 'success');
        }
      })
      .catch(function (err) {
        toast(t('registration.stop_failed') + ': ' + err.message, 'error');
      });
  }

  // ─── 状态轮询 ──────────────────────────────────────────

  function _checkJobStatus() {
    api('GET', '/register/status')
      .then(function (data) {
        _updateProgress(data);
        if (data.running) {
          _setRunning(true);
          _startPolling();
        } else {
          _setRunning(false);
        }
      })
      .catch(function () {
        // 连接失败，静默处理
      });
  }

  function _startPolling() {
    _stopPolling();
    _pollTimer = setInterval(function () {
      api('GET', '/register/status')
        .then(function (data) {
          _updateProgress(data);
          _loadJobStats();
          if (!data.running) {
            _setRunning(false);
            _stopPolling();
            _loadRemoteAccounts();
            _loadJobHistory();
            toast(t('registration.job_completed'), 'success');
          }
        })
        .catch(function () {
          // 静默处理
        });
    }, 3000);
  }

  function _stopPolling() {
    if (_pollTimer) {
      clearInterval(_pollTimer);
      _pollTimer = null;
    }
  }

  // ─── UI 更新 ──────────────────────────────────────────

  function _setRunning(running) {
    _isRunning = running;
    var startBtn = document.getElementById('regStartBtn');
    var stopBtn = document.getElementById('regStopBtn');
    var progressDiv = document.getElementById('regProgress');

    if (startBtn) startBtn.style.display = running ? 'none' : '';
    if (stopBtn) stopBtn.style.display = running ? '' : 'none';
    if (progressDiv) progressDiv.style.display = (running || _hasProgress()) ? '' : 'none';
  }

  function _hasProgress() {
    var el = document.getElementById('regSuccessCount');
    return el && el.textContent !== '0';
  }

  function _updateProgress(data) {
    var p = data.progress || {};
    var total = p.total || 0;
    var completed = p.completed || 0;
    var pct = total > 0 ? Math.round((completed / total) * 100) : 0;

    var bar = document.getElementById('regProgressBar');
    var text = document.getElementById('regProgressText');
    var successEl = document.getElementById('regSuccessCount');
    var failedEl = document.getElementById('regFailedCount');
    var uncertainEl = document.getElementById('regUncertainCount');
    var elapsedEl = document.getElementById('regElapsed');
    var progressDiv = document.getElementById('regProgress');

    if (bar) bar.style.width = pct + '%';
    if (text) text.textContent = completed + ' / ' + total;
    if (successEl) successEl.textContent = p.success || 0;
    if (failedEl) failedEl.textContent = p.failed || 0;
    if (uncertainEl) uncertainEl.textContent = p.uncertain || 0;
    if (progressDiv) progressDiv.style.display = '';

    if (elapsedEl && data.elapsed_ms > 0) {
      var secs = Math.round(data.elapsed_ms / 1000);
      var mins = Math.floor(secs / 60);
      var remSecs = secs % 60;
      elapsedEl.textContent = mins > 0 ? mins + 'm ' + remSecs + 's' : secs + 's';
    }
    // elapsed_ms=0 时不清空，保留上次的值
  }

  // ─── 远程账号列表 ──────────────────────────────────────

  function _loadRemoteAccounts() {
    _loadRemoteStats();

    api('GET', '/register/accounts?page=' + _currentPage + '&limit=20')
      .then(function (data) {
        _renderAccounts(data.accounts || []);
        _renderPagination(data);
      })
      .catch(function () {
        var body = document.getElementById('regAccountsBody');
        if (body) {
          body.innerHTML = '<tr><td colspan="4"><div class="empty-state"><span>' + escapeHtml(t('registration.load_failed')) + '</span></div></td></tr>';
        }
      });
  }

  function _loadRemoteStats() {
    api('GET', '/register/accounts/stats')
      .then(function (stats) {
        var el = document.getElementById('regRemoteStats');
        if (!el) return;
        el.innerHTML =
          '<div class="reg-remote-stat">' +
            '<span class="reg-remote-stat-value">' + (stats.total || 0) + '</span>' +
            '<span class="reg-remote-stat-label">' + escapeHtml(t('registration.total')) + '</span>' +
          '</div>' +
          '<div class="reg-remote-stat">' +
            '<span class="reg-remote-stat-value" style="color:var(--color-success)">' + (stats.success || 0) + '</span>' +
            '<span class="reg-remote-stat-label">' + escapeHtml(t('registration.success')) + '</span>' +
          '</div>' +
          '<div class="reg-remote-stat">' +
            '<span class="reg-remote-stat-value" style="color:var(--color-error)">' + (stats.failed || 0) + '</span>' +
            '<span class="reg-remote-stat-label">' + escapeHtml(t('registration.failed')) + '</span>' +
          '</div>' +
          '<div class="reg-remote-stat">' +
            '<span class="reg-remote-stat-value" style="color:var(--color-accent)">' + (stats.with_token || 0) + '</span>' +
            '<span class="reg-remote-stat-label">' + escapeHtml(t('registration.with_token')) + '</span>' +
          '</div>';
      })
      .catch(function () {});
  }

  function _renderAccounts(accounts) {
    var body = document.getElementById('regAccountsBody');
    if (!body) return;

    if (accounts.length === 0) {
      body.innerHTML = '<tr><td colspan="4"><div class="empty-state"><span>' + escapeHtml(t('registration.no_accounts')) + '</span></div></td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < accounts.length; i++) {
      var a = accounts[i];
      var statusClass = a.status === 'success' ? 'badge-active' : a.status === 'failed' ? 'badge-banned' : 'badge-expired';
      var statusText = a.status === 'success' ? t('registration.success') : a.status === 'failed' ? t('registration.failed') : t('registration.uncertain');
      var tokenBadge = a.hasToken ? '<span class="badge badge-active">Yes</span>' : '<span class="badge badge-expired">No</span>';
      var timeStr = a.registeredAt ? new Date(a.registeredAt).toLocaleString() : '-';

      html +=
        '<tr>' +
          '<td class="td-email" title="' + escapeHtml(displayEmail(a.email || '-')) + '">' + escapeHtml(displayEmail(a.email || '-')) + '</td>' +
          '<td><span class="badge ' + statusClass + '">' + escapeHtml(statusText) + '</span></td>' +
          '<td>' + tokenBadge + '</td>' +
          '<td class="td-mono td-time">' + escapeHtml(timeStr) + '</td>' +
        '</tr>';
    }
    body.innerHTML = html;
  }

  // ─── 成功率监控 ──────────────────────────────────────

  var _stepOrder = [
    { key: 'mail_create', i18n: 'registration.step_mail_create' },
    { key: 'oauth_init', i18n: 'registration.step_oauth_init' },
    { key: 'sentinel_reg', i18n: 'registration.step_sentinel_reg' },
    { key: 'email_otp', i18n: 'registration.step_email_otp' },
    { key: 'sentinel_create', i18n: 'registration.step_sentinel_create' },
    { key: 'oauth_callback', i18n: 'registration.step_oauth_callback' },
  ];

  function _loadJobStats() {
    api('GET', '/register/jobs/stats')
      .then(function (data) {
        _renderJobStats(data);
      })
      .catch(function () {
        // 静默处理
      });
  }

  function _renderJobStats(data) {
    var card = document.getElementById('regMonitorCard');
    var container = document.getElementById('regMonitor');
    if (!container || !card) return;

    // 如果没有任何数据，隐藏面板
    if (!data || !data.steps || data.total_accounts === 0) {
      card.style.display = 'none';
      return;
    }

    card.style.display = '';
    var html = '';

    for (var i = 0; i < _stepOrder.length; i++) {
      var step = _stepOrder[i];
      var s = data.steps[step.key] || { attempts: 0, success: 0, fail: 0, rate: '0%' };
      var pctNum = s.attempts > 0 ? (s.success / s.attempts * 100) : 0;
      var barClass = pctNum >= 80 ? '' : pctNum >= 50 ? ' rate-warn' : ' rate-danger';

      html +=
        '<div class="reg-monitor-row">' +
          '<span class="reg-monitor-label">' + escapeHtml(t(step.i18n)) + '</span>' +
          '<div class="reg-monitor-bar-wrap">' +
            '<div class="reg-monitor-bar' + barClass + '" style="width:' + pctNum.toFixed(1) + '%"></div>' +
          '</div>' +
          '<span class="reg-monitor-pct">' + escapeHtml(s.rate || '0%') + '</span>' +
          '<span class="reg-monitor-detail">' + s.success + '/' + s.attempts + '</span>' +
        '</div>';
    }

    // 总成功率
    var ov = data.overall || {};
    var total = data.total_accounts || 0;
    var ovRate = ov.rate || '0%';
    html +=
      '<div class="reg-monitor-overall">' +
        '<span class="reg-monitor-overall-label">' + escapeHtml(t('registration.overall_rate')) + '</span>' +
        '<span class="reg-monitor-overall-value">' + escapeHtml(ovRate) + ' (' + (ov.success || 0) + '/' + total + ')</span>' +
      '</div>';

    container.innerHTML = html;
  }

  // ─── 分页 ──────────────────────────────────────────

  function _renderPagination(data) {
    var el = document.getElementById('regPagination');
    if (!el) return;
    if (!data.pages || data.pages <= 1) {
      el.innerHTML = '';
      return;
    }

    var html = '';
    var page = data.page || 1;
    var pages = data.pages;

    if (page > 1) {
      html += '<button class="btn btn-secondary btn-sm reg-page-btn" data-page="' + (page - 1) + '">&laquo;</button>';
    }
    html += '<span class="reg-page-info">' + page + ' / ' + pages + '</span>';
    if (page < pages) {
      html += '<button class="btn btn-secondary btn-sm reg-page-btn" data-page="' + (page + 1) + '">&raquo;</button>';
    }
    el.innerHTML = html;

    // 绑定分页点击
    var btns = el.querySelectorAll('.reg-page-btn');
    for (var i = 0; i < btns.length; i++) {
      btns[i].addEventListener('click', function (e) {
        var p = parseInt(e.currentTarget.getAttribute('data-page'), 10);
        if (p > 0) {
          _currentPage = p;
          _loadRemoteAccounts();
        }
      });
    }
  }

  // ─── 历史任务 ──────────────────────────────────────

  function _loadJobHistory() {
    api('GET', '/register/jobs/history')
      .then(function (data) {
        _renderJobHistory(data.history || []);
      })
      .catch(function () {
        // 静默处理
      });
  }

  function _renderJobHistory(history) {
    var body = document.getElementById('regHistoryBody');
    if (!body) return;

    if (!history || history.length === 0) {
      body.innerHTML = '<tr><td colspan="6"><div class="empty-state"><span>' +
        escapeHtml(t('registration.history_empty')) + '</span></div></td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < history.length; i++) {
      var h = history[i];
      var p = h.progress || {};
      var startTime = h.started_at ? new Date(h.started_at).toLocaleString() : '-';

      var elapsed = '-';
      if (h.elapsed_ms > 0) {
        var secs = Math.round(h.elapsed_ms / 1000);
        var mins = Math.floor(secs / 60);
        var remSecs = secs % 60;
        elapsed = mins > 0 ? mins + 'm ' + remSecs + 's' : secs + 's';
      }

      var resultHtml =
        '<span class="badge badge-active">' + (p.success || 0) + '</span> / ' +
        '<span class="badge badge-banned">' + (p.failed || 0) + '</span>';
      if (p.uncertain > 0) {
        resultHtml += ' / <span class="badge badge-expired">' + p.uncertain + '</span>';
      }

      var detailId = 'regHistorySteps_' + i;

      html +=
        '<tr>' +
          '<td class="td-mono">' + escapeHtml(h.job_id || '-') + '</td>' +
          '<td class="td-mono td-time">' + escapeHtml(startTime) + '</td>' +
          '<td>' + (p.total || 0) + '</td>' +
          '<td>' + resultHtml + '</td>' +
          '<td class="td-mono">' + escapeHtml(elapsed) + '</td>' +
          '<td>' +
            '<button class="btn btn-secondary btn-sm reg-history-expand-btn" data-target="' + detailId + '">' +
              '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>' +
            '</button>' +
          '</td>' +
        '</tr>' +
        '<tr class="reg-history-steps-row" id="' + detailId + '" style="display:none;">' +
          '<td colspan="6">' + _renderHistorySteps(h.steps, h.overall) + '</td>' +
        '</tr>';
    }
    body.innerHTML = html;

    var expandBtns = body.querySelectorAll('.reg-history-expand-btn');
    for (var j = 0; j < expandBtns.length; j++) {
      expandBtns[j].addEventListener('click', function (e) {
        var target = document.getElementById(e.currentTarget.getAttribute('data-target'));
        if (target) {
          var isHidden = target.style.display === 'none';
          target.style.display = isHidden ? '' : 'none';
          var svg = e.currentTarget.querySelector('svg');
          if (svg) svg.style.transform = isHidden ? 'rotate(180deg)' : '';
        }
      });
    }
  }

  function _renderHistorySteps(steps, overall) {
    if (!steps) return '<div class="empty-state"><span>' + escapeHtml(t('registration.no_stats')) + '</span></div>';

    var html = '<div class="reg-history-steps-panel">';

    for (var i = 0; i < _stepOrder.length; i++) {
      var step = _stepOrder[i];
      var s = steps[step.key] || { attempts: 0, success: 0, fail: 0 };
      var pctNum = s.attempts > 0 ? (s.success / s.attempts * 100) : 0;
      var barClass = pctNum >= 80 ? '' : pctNum >= 50 ? ' rate-warn' : ' rate-danger';
      var rateStr = s.attempts > 0 ? pctNum.toFixed(1) + '%' : '0%';

      html +=
        '<div class="reg-monitor-row">' +
          '<span class="reg-monitor-label">' + escapeHtml(t(step.i18n)) + '</span>' +
          '<div class="reg-monitor-bar-wrap">' +
            '<div class="reg-monitor-bar' + barClass + '" style="width:' + pctNum.toFixed(1) + '%"></div>' +
          '</div>' +
          '<span class="reg-monitor-pct">' + escapeHtml(rateStr) + '</span>' +
          '<span class="reg-monitor-detail">' + s.success + '/' + s.attempts + '</span>' +
        '</div>';
    }

    if (overall) {
      html +=
        '<div class="reg-monitor-overall">' +
          '<span class="reg-monitor-overall-label">' + escapeHtml(t('registration.overall_rate')) + '</span>' +
          '<span class="reg-monitor-overall-value">' + escapeHtml(overall.rate || '0%') + ' (' + (overall.success || 0) + ')</span>' +
        '</div>';
    }

    html += '</div>';
    return html;
  }

  return {
    init: init,
    cleanup: cleanup,
  };
})();
