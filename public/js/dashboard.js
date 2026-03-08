/**
 * codex2api 管理面板 — 仪表盘模块
 */

var Dashboard = (function () {
  'use strict';

  var _healthTimer = null;
  var _activeTab = 'token';
  var _timeseriesData = null;
  var _modelsData = null;
  var _accountsData = null;
  var _callersData = null;
  var _dashboardData = null;
  var _accountHealthStatus = null;
  var _analyticsEventsBound = false;

  var _heroIcons = {
    requests: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>',
    tokens: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>',
    rpm: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>',
    tpm: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20.24 12.24a6 6 0 0 0-8.49-8.49L5 10.5V19h8.5z"/><line x1="16" y1="8" x2="2" y2="22"/><line x1="17.5" y1="15" x2="9" y2="15"/></svg>',
    success: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
  };

  var _accountIcons = {
    total: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
    active: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>',
    cooldown: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>',
    banned: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg>',
    expired: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
    wasted: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>',
  };

  function _fmtNum(n) {
    if (n == null || n === 0) return '0';
    if (n >= 1000000000) return (n / 1000000000).toFixed(1) + 'B';
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return String(n);
  }

  function _getCacheHitRatePercent(cachedTokens, inputTokens) {
    var cached = Number(cachedTokens) || 0;
    var input = Number(inputTokens) || 0;
    if (cached <= 0 || input <= 0) return null;
    return Math.round((cached / input) * 100);
  }

  function _fmtCacheHitBadgeValue(cachedTokens, inputTokens) {
    var cached = Number(cachedTokens) || 0;
    var rate = _getCacheHitRatePercent(cached, inputTokens);
    if (cached <= 0 || rate == null) return '-';
    return rate + '% (' + _fmtNum(cached) + ')';
  }

  function _formatLifespan(hours) {
    if (typeof formatLifespanHours === 'function') {
      return formatLifespanHours(hours);
    }
    var h = Number(hours);
    if (!Number.isFinite(h) || h < 0) return '-';
    if (h < 1) return (Math.round(h * 600) / 10) + 'min';
    if (h <= 48) return (Math.round(h * 10) / 10) + 'h';
    return (Math.round((h / 24) * 10) / 10) + 'd';
  }

  function _toFiniteNumber(value) {
    var n = Number(value);
    if (!Number.isFinite(n)) return 0;
    return n;
  }

  function _toPct(value, max) {
    if (max <= 0) return 0;
    var pct = Math.round((value / max) * 100);
    if (pct < 0) return 0;
    if (pct > 100) return 100;
    return pct;
  }

  function _toPctStep(value, max) {
    var pct = _toPct(value, max);
    return Math.round(pct / 5) * 5;
  }

  function _isOverviewAllZero(data) {
    if (!data || typeof data !== 'object') return true;
    return (Number(data.total_requests) || 0) === 0
      && (Number(data.total_input_tokens) || 0) === 0
      && (Number(data.total_output_tokens) || 0) === 0
      && (Number(data.rpm) || 0) === 0
      && (Number(data.tpm) || 0) === 0
      && (Number(data.success_rate) || 0) === 0;
  }

  function _isHoursStatsMode() {
    if (typeof dateRangeToQuery !== 'function') return false;
    var q = dateRangeToQuery();
    return !!(q && q.hours);
  }

  function _shouldFallbackOverview(data) {
    if (!_isOverviewAllZero(data)) return false;
    if (_isHoursStatsMode()) return false;
    if (typeof getDateRange !== 'function') return false;
    var r = getDateRange();
    return !!(r && r.mode === 'preset' && r.preset === 'today');
  }

  function _shouldFallbackAnalytics() {
    if (_isHoursStatsMode()) return false;
    if (typeof getDateRange !== 'function') return false;
    var r = getDateRange();
    return !!(r && r.mode === 'preset' && r.preset === 'today');
  }

  function _buildStatsQuery() {
    if (typeof getDateRange === 'function') {
      var range = getDateRange();
      if (range && range.mode === 'total') return '?total=true';
    }
    return buildQueryString(dateRangeToQuery());
  }

  function _isTimeseriesAllZero(data) {
    if (!Array.isArray(data) || data.length === 0) return true;
    for (var i = 0; i < data.length; i++) {
      var row = data[i] || {};
      if ((Number(row.requests) || 0) > 0) return false;
      if ((Number(row.success) || 0) > 0) return false;
      if ((Number(row.input) || 0) > 0) return false;
      if ((Number(row.output) || 0) > 0) return false;
    }
    return true;
  }

  function _isRankRowsAllZero(data) {
    if (!Array.isArray(data) || data.length === 0) return true;
    for (var i = 0; i < data.length; i++) {
      var row = data[i] || {};
      if ((Number(row.requests) || 0) > 0) return false;
      if ((Number(row.input) || 0) > 0) return false;
      if ((Number(row.output) || 0) > 0) return false;
      if ((Number(row.errors) || 0) > 0) return false;
    }
    return true;
  }

  function load() {
    _bindAnalyticsTabEvents();
    var query = _buildStatsQuery();
    api('GET', '/dashboard')
      .then(function (data) {
        _dashboardData = data || {};
        _renderAccountHealthBar(data.accounts || {}, data.account_lifespan || {});
        _renderModels(data.models || []);
        _renderServiceInfo(data);
      })
      .catch(function (err) {
        toast(t('dashboard.load_failed') + ': ' + err.message, 'error');
      });
    _loadAccountHealthStatus();
    api('GET', '/stats/overview' + query)
      .then(function (data) {
        if (_shouldFallbackOverview(data)) {
          return api('GET', '/stats/overview?total=true')
            .then(function (fallbackData) {
              _renderHeroStats(fallbackData || data);
            })
            .catch(function () {
              _renderHeroStats(data);
            });
        }
        _renderHeroStats(data);
      })
      .catch(function () {});
    _loadAnalytics();
  }

  function _loadAccountHealthStatus() {
    api('GET', '/account-health/status')
      .then(function (data) {
        _accountHealthStatus = data || null;
        if (_dashboardData) _renderServiceInfo(_dashboardData);
      })
      .catch(function () {
        _accountHealthStatus = null;
        if (_dashboardData) _renderServiceInfo(_dashboardData);
      });
  }

  function _bindAnalyticsTabEvents() {
    if (_analyticsEventsBound) return;
    var tabs = document.getElementById('analyticsTabs');
    if (!tabs) return;
    tabs.addEventListener('click', function (e) {
      var btn = e.target && e.target.closest ? e.target.closest('.analytics-tab[data-tab]') : null;
      if (!btn) return;
      _switchTab(btn.getAttribute('data-tab'));
    });
    _analyticsEventsBound = true;
  }

  function _setAnalyticsLoading() {
    var el = document.getElementById('analyticsContent');
    if (!el) return;
    el.innerHTML = '<div class="chart-empty">' + escapeHtml(t('common.loading')) + '</div>';
  }

  function _loadAnalytics() {
    _timeseriesData = null;
    _modelsData = null;
    _accountsData = null;
    _callersData = null;
    _switchTab(_activeTab, true);
  }

  function _loadActiveAnalytics(forceReload) {
    var force = !!forceReload;
    if (_activeTab === 'token' || _activeTab === 'request') {
      if (!force && _timeseriesData !== null) {
        _renderActiveAnalytics();
        return;
      }
      _setAnalyticsLoading();
      _loadTimeseries();
      return;
    }
    if (_activeTab === 'model') {
      if (!force && _modelsData !== null) {
        _renderActiveAnalytics();
        return;
      }
      _setAnalyticsLoading();
      _loadModels();
      return;
    }
    if (_activeTab === 'account') {
      if (!force && _accountsData !== null) {
        _renderActiveAnalytics();
        return;
      }
      _setAnalyticsLoading();
      _loadAccounts();
      return;
    }
    if (!force && _callersData !== null) {
      _renderActiveAnalytics();
      return;
    }
    _setAnalyticsLoading();
    _loadCallers();
  }

  function _loadTimeseries() {
    var query = _buildStatsQuery();
    api('GET', '/stats/timeseries' + query).then(function (data) {
      var list = Array.isArray(data) ? data : [];
      if (_shouldFallbackAnalytics() && _isTimeseriesAllZero(list)) {
        return api('GET', '/stats/timeseries?total=true').then(function (fallbackData) {
          _timeseriesData = Array.isArray(fallbackData) ? fallbackData : list;
          if (_activeTab === 'token' || _activeTab === 'request') {
            _renderActiveAnalytics();
          }
        }).catch(function () {
          _timeseriesData = list;
          if (_activeTab === 'token' || _activeTab === 'request') {
            _renderActiveAnalytics();
          }
        });
      }
      _timeseriesData = list;
      if (_activeTab === 'token' || _activeTab === 'request') {
        _renderActiveAnalytics();
      }
    }).catch(function () {
      _timeseriesData = [];
      if (_activeTab === 'token' || _activeTab === 'request') {
        _renderActiveAnalytics();
      }
    });
  }

  function _loadModels() {
    var query = _buildStatsQuery();
    api('GET', '/stats/models' + query).then(function (data) {
      var list = Array.isArray(data) ? data : [];
      if (_shouldFallbackAnalytics() && _isRankRowsAllZero(list)) {
        return api('GET', '/stats/models?total=true').then(function (fallbackData) {
          _modelsData = Array.isArray(fallbackData) ? fallbackData : list;
          if (_activeTab === 'model') _renderActiveAnalytics();
        }).catch(function () {
          _modelsData = list;
          if (_activeTab === 'model') _renderActiveAnalytics();
        });
      }
      _modelsData = list;
      if (_activeTab === 'model') _renderActiveAnalytics();
    }).catch(function () {
      _modelsData = [];
      if (_activeTab === 'model') _renderActiveAnalytics();
    });
  }

  function _loadAccounts() {
    var query = _buildStatsQuery();
    api('GET', '/stats/accounts' + query).then(function (data) {
      var list = Array.isArray(data) ? data : [];
      if (_shouldFallbackAnalytics() && _isRankRowsAllZero(list)) {
        return api('GET', '/stats/accounts?total=true').then(function (fallbackData) {
          _accountsData = Array.isArray(fallbackData) ? fallbackData : list;
          if (_activeTab === 'account') _renderActiveAnalytics();
        }).catch(function () {
          _accountsData = list;
          if (_activeTab === 'account') _renderActiveAnalytics();
        });
      }
      _accountsData = list;
      if (_activeTab === 'account') _renderActiveAnalytics();
    }).catch(function () {
      _accountsData = [];
      if (_activeTab === 'account') _renderActiveAnalytics();
    });
  }

  function _loadCallers() {
    var query = _buildStatsQuery();
    api('GET', '/stats/callers' + query).then(function (data) {
      var list = Array.isArray(data) ? data : [];
      if (_shouldFallbackAnalytics() && _isRankRowsAllZero(list)) {
        return api('GET', '/stats/callers?total=true').then(function (fallbackData) {
          _callersData = Array.isArray(fallbackData) ? fallbackData : list;
          if (_activeTab === 'caller') _renderActiveAnalytics();
        }).catch(function () {
          _callersData = list;
          if (_activeTab === 'caller') _renderActiveAnalytics();
        });
      }
      _callersData = list;
      if (_activeTab === 'caller') _renderActiveAnalytics();
    }).catch(function () {
      _callersData = [];
      if (_activeTab === 'caller') _renderActiveAnalytics();
    });
  }

  function _switchTab(tab, forceReload) {
    var next = tab || 'token';
    if (next !== 'token' && next !== 'request' && next !== 'model' && next !== 'account' && next !== 'caller') {
      next = 'token';
    }
    _activeTab = next;

    var tabs = document.getElementById('analyticsTabs');
    if (tabs) {
      var buttons = tabs.querySelectorAll('.analytics-tab');
      for (var i = 0; i < buttons.length; i++) {
        var btn = buttons[i];
        if (btn.getAttribute('data-tab') === _activeTab) btn.classList.add('active');
        else btn.classList.remove('active');
      }
    }
    _loadActiveAnalytics(!!forceReload);
  }

  function _renderActiveAnalytics() {
    if (_activeTab === 'token') {
      _renderTokenChart(_timeseriesData);
      return;
    }
    if (_activeTab === 'request') {
      _renderRequestChart(_timeseriesData);
      return;
    }
    if (_activeTab === 'model') {
      _renderModelTable(_modelsData);
      return;
    }
    if (_activeTab === 'account') {
      _renderAccountTable(_accountsData);
      return;
    }
    _renderCallerTable(_callersData);
  }

  function _renderBarChart(containerId, items, valueKey, color) {
    var el = document.getElementById(containerId);
    if (!el) return;
    if (!items || items.length === 0) {
      el.innerHTML = '<div class="chart-empty">' + escapeHtml(t('stats.no_data')) + '</div>';
      return;
    }
    var max = 0;
    for (var i = 0; i < items.length; i++) {
      var v = _toFiniteNumber(items[i][valueKey]);
      if (v > max) max = v;
    }
    if (max === 0) max = 1;

    var html = '';
    for (var j = 0; j < items.length; j++) {
      var item = items[j];
      var val = _toFiniteNumber(item[valueKey]);
      var pct = _toPctStep(val, max);
      var label = _formatTimeseriesHourLabel(item, j);
      html += '<div class="bar-col" title="' + escapeHtml(item.label || label) + ': ' + _fmtNum(val) + '">'
        + '<div class="bar-fill bar-' + color + ' pct-h-' + pct + '"></div>'
        + '<span class="bar-label">' + escapeHtml(label) + '</span>'
        + '</div>';
    }
    el.innerHTML = html;
  }

  function _formatTimeseriesHourLabel(item, idx) {
    var full = String((item && item.label) || '');
    if (/^\d{4}-\d{2}-\d{2}$/.test(full)) {
      return full.substring(5);
    }
    var m = /(\d{2}:\d{2})$/.exec(full);
    if (m && m[1]) return m[1];
    if (item && item.date && /^\d{4}-\d{2}-\d{2}$/.test(String(item.date))) {
      return String(item.date).substring(5);
    }
    if (item && item.hour != null) {
      return String(item.hour).padStart(2, '0') + ':00';
    }
    return String(idx);
  }

  function _renderTokenChart(data) {
    var container = document.getElementById('analyticsContent');
    if (!container) return;
    if (data == null) {
      _setAnalyticsLoading();
      return;
    }
    container.innerHTML = '<div class="bar-chart" id="analyticsBarChart"></div>';
    var items = data.map(function (d) {
      return {
        hour: d.hour,
        label: d.label,
        tokens: _toFiniteNumber(d.input) + _toFiniteNumber(d.output),
      };
    });
    _renderBarChart('analyticsBarChart', items, 'tokens', 'accent');
  }

  function _renderRequestChart(data) {
    var container = document.getElementById('analyticsContent');
    if (!container) return;
    if (data == null) {
      _setAnalyticsLoading();
      return;
    }
    container.innerHTML = '<div class="bar-chart" id="analyticsBarChart"></div>';
    _renderBarChart('analyticsBarChart', data, 'requests', 'success');
  }

  function _renderModelTable(data) {
    var el = document.getElementById('analyticsContent');
    if (!el) return;
    if (data == null) {
      _setAnalyticsLoading();
      return;
    }
    if (!data || data.length === 0) {
      el.innerHTML = '<div class="chart-empty">' + escapeHtml(t('stats.no_data')) + '</div>';
      return;
    }
    var maxReq = _toFiniteNumber(data[0].requests);
    if (maxReq === 0) maxReq = 1;
    var html = '<table><thead><tr>'
      + '<th>' + escapeHtml(t('stats.th_model')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_requests')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_input')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_output')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_avg_latency')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_proportion')) + '</th>'
      + '</tr></thead><tbody>';
    for (var i = 0; i < data.length; i++) {
      var m = data[i];
      var req = _toFiniteNumber(m.requests);
      var pct = _toPctStep(req, maxReq);
      html += '<tr>'
        + '<td class="td-mono">' + escapeHtml(m.model || '-') + '</td>'
        + '<td class="td-mono">' + _fmtNum(req) + '</td>'
        + '<td class="td-mono">' + _fmtNum(m.input || 0) + '</td>'
        + '<td class="td-mono">' + _fmtNum(m.output || 0) + '</td>'
        + '<td class="td-mono">' + _fmtNum(m.avg_latency || 0) + escapeHtml(t('time.ms')) + '</td>'
        + '<td><div class="progress-inline"><div class="progress-fill bar-accent pct-w-' + pct + '"></div></div></td>'
        + '</tr>';
    }
    html += '</tbody></table>';
    el.innerHTML = html;
  }

  function _renderAccountTable(data) {
    var el = document.getElementById('analyticsContent');
    if (!el) return;
    if (data == null) {
      _setAnalyticsLoading();
      return;
    }
    if (!data || data.length === 0) {
      el.innerHTML = '<div class="chart-empty">' + escapeHtml(t('stats.no_data')) + '</div>';
      return;
    }
    var maxReq = _toFiniteNumber(data[0].requests);
    if (maxReq === 0) maxReq = 1;
    var html = '<table><thead><tr>'
      + '<th>' + escapeHtml(t('stats.th_account')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_requests')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_input')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_output')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_errors')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_proportion')) + '</th>'
      + '</tr></thead><tbody>';
    for (var i = 0; i < data.length; i++) {
      var a = data[i];
      var req = _toFiniteNumber(a.requests);
      var pct = _toPctStep(req, maxReq);
      var emailDisplay = displayEmail(a.email);
      var emailShort = emailDisplay.length > 24 ? emailDisplay.substring(0, 22) + '..' : emailDisplay;
      html += '<tr>'
        + '<td class="td-mono td-email" title="' + escapeHtml(emailDisplay) + '">' + escapeHtml(emailShort) + '</td>'
        + '<td class="td-mono">' + _fmtNum(req) + '</td>'
        + '<td class="td-mono">' + _fmtNum(a.input || 0) + '</td>'
        + '<td class="td-mono">' + _fmtNum(a.output || 0) + '</td>'
        + '<td class="td-mono">' + _fmtNum(a.errors || 0) + '</td>'
        + '<td><div class="progress-inline"><div class="progress-fill bar-success pct-w-' + pct + '"></div></div></td>'
        + '</tr>';
    }
    html += '</tbody></table>';
    el.innerHTML = html;
  }

  function _renderCallerTable(data) {
    var el = document.getElementById('analyticsContent');
    if (!el) return;
    if (data == null) {
      _setAnalyticsLoading();
      return;
    }
    if (!data || data.length === 0) {
      el.innerHTML = '<div class="chart-empty">' + escapeHtml(t('stats.no_data')) + '</div>';
      return;
    }
    var maxReq = _toFiniteNumber(data[0].requests);
    if (maxReq === 0) maxReq = 1;
    var html = '<table><thead><tr>'
      + '<th>' + escapeHtml(t('stats.caller')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_requests')) + '</th>'
      + '<th>' + escapeHtml(t('stats.field_total_tokens')) + '</th>'
      + '<th>' + escapeHtml(t('stats.th_proportion')) + '</th>'
      + '</tr></thead><tbody>';
    for (var i = 0; i < data.length; i++) {
      var c = data[i] || {};
      var req = _toFiniteNumber(c.requests);
      var totalTokens = _toFiniteNumber(c.input) + _toFiniteNumber(c.output);
      var pct = _toPctStep(req, maxReq);
      var identity = c.identity ? String(c.identity).trim() : '-';
      var username = String(c.display_name || c.username || '').trim();
      if (!username) {
        var identityI18nKey = 'stats.caller_identity.' + identity;
        username = t(identityI18nKey);
        if (!username || username === identityI18nKey) username = identity;
      }
      var seqId = String(c.seq_id || '').trim();
      var discordUserId = String(c.discord_user_id || '').trim();
      var avatarUrl = String(c.avatar_url || '').trim();
      if (username && ((seqId && username === seqId) || (discordUserId && username === discordUserId))) {
        username = '';
      }
      var callerText = username || identity;
      if (seqId) {
        callerText = seqId + (username ? (' (' + username + ')') : '');
      }
      if (!callerText) callerText = '-';
      var titleText = callerText;
      if (discordUserId && titleText.indexOf(discordUserId) < 0) titleText += ' [' + discordUserId + ']';
      var identityShort = callerText.length > 36 ? callerText.substring(0, 34) + '..' : callerText;
      var callerHtml = '';
      if (avatarUrl) {
        callerHtml += '<span class="abuse-avatar-wrap"><img class="abuse-avatar" src="' + escapeHtml(avatarUrl) + '" alt=""></span>';
      }
      callerHtml += '<span class="td-email">' + escapeHtml(identityShort) + '</span>';
      html += '<tr>'
        + '<td class="td-mono" title="' + escapeHtml(titleText) + '">' + callerHtml + '</td>'
        + '<td class="td-mono">' + _fmtNum(req) + '</td>'
        + '<td class="td-mono">' + _fmtNum(totalTokens) + '</td>'
        + '<td><div class="progress-inline"><div class="progress-fill bar-accent pct-w-' + pct + '"></div></div></td>'
        + '</tr>';
    }
    html += '</tbody></table>';
    el.innerHTML = html;
  }

  function _renderHeroStats(data) {
    var container = document.getElementById('heroStats');
    if (!container) return;
    var rangeLabel = dateRangeLabel();
    var cards = [
      { label: rangeLabel + ' · ' + t('stats.total_requests'), value: _fmtNum(data.total_requests), icon: _heroIcons.requests, cls: 'hero-info' },
      { label: rangeLabel + ' · ' + t('stats.input_tokens') + '+' + t('stats.output_tokens'), value: _fmtNum((data.total_input_tokens || 0) + (data.total_output_tokens || 0)), icon: _heroIcons.tokens, cls: 'hero-accent' },
      { label: t('dashboard.current_rpm'), value: data.rpm || 0, icon: _heroIcons.rpm, cls: 'hero-warning' },
      { label: t('dashboard.current_tpm'), value: _fmtNum(data.tpm || 0), icon: _heroIcons.tpm, cls: 'hero-cyan' },
      { label: t('dashboard.success_rate'), value: (data.success_rate || 0) + '%', icon: _heroIcons.success, cls: 'hero-success' },
    ];
    var html = '';
    for (var i = 0; i < cards.length; i++) {
      var c = cards[i];
      html += '<div class="hero-card ' + c.cls + '">'
        + '<div class="hero-icon">' + c.icon + '</div>'
        + '<div class="hero-content">'
        + '<div class="hero-value">' + escapeHtml(String(c.value)) + '</div>'
        + '<div class="hero-label">' + escapeHtml(c.label) + '</div>'
        + '</div></div>';
    }
    container.innerHTML = html;
    _renderCacheHitBadge(container, data || {});
  }

  function _renderCacheHitBadge(heroContainer, overviewData) {
    if (!heroContainer || !heroContainer.parentNode) return;
    var badgeWrap = document.getElementById('dashboardCacheSummary');
    if (!badgeWrap) {
      badgeWrap = document.createElement('div');
      badgeWrap.id = 'dashboardCacheSummary';
      badgeWrap.className = 'stats-summary-bar dashboard-cache-summary';
      heroContainer.insertAdjacentElement('afterend', badgeWrap);
    }
    if (!Object.prototype.hasOwnProperty.call(overviewData, 'total_cached_tokens')) {
      badgeWrap.innerHTML = '';
      return;
    }
    var value = _fmtCacheHitBadgeValue(overviewData.total_cached_tokens, overviewData.total_input_tokens);
    badgeWrap.innerHTML = '<span class="stats-badge stats-badge-success">'
      + escapeHtml(t('stats.cache_hit_rate')) + ': '
      + '<strong>' + escapeHtml(value) + '</strong>'
      + '</span>';
  }

  function _renderLifespanBadge(labelKey, value, cls) {
    return '<span class="stats-badge ' + cls + '">'
      + escapeHtml(t(labelKey)) + ': '
      + '<strong>' + escapeHtml(String(value)) + '</strong>'
      + '</span>';
  }

  function _renderAccountHealthBar(stats, lifespan) {
    var container = document.getElementById('accountHealthBar');
    if (!container) return;
    var items = [
      { key: 'total', labelKey: 'dashboard.total', cls: '', filter: null },
      { key: 'active', labelKey: 'dashboard.active', cls: 'health-active', filter: 'active' },
      { key: 'cooldown', labelKey: 'dashboard.cooldown', cls: 'health-cooldown', filter: 'cooldown' },
      { key: 'banned', labelKey: 'dashboard.banned', cls: 'health-banned', filter: 'banned' },
      { key: 'expired', labelKey: 'dashboard.expired', cls: 'health-expired', filter: 'expired' },
      { key: 'wasted', labelKey: 'dashboard.wasted', cls: 'health-wasted', filter: 'wasted' },
    ];
    var html = '<div class="health-bar-inner">';
    for (var i = 0; i < items.length; i++) {
      var item = items[i];
      var value = stats[item.key] || 0;
      var filterAttr = item.filter ? ' data-filter="' + item.filter + '"' : '';
      html += '<div class="health-item ' + item.cls + '"' + filterAttr + '>'
        + '<span class="health-icon">' + (_accountIcons[item.key] || '') + '</span>'
        + '<span class="health-label">' + escapeHtml(t(item.labelKey)) + '</span>'
        + '<span class="health-value">' + value + '</span>'
        + '</div>';
    }
    html += '</div>';
    if (lifespan && typeof lifespan === 'object' && (lifespan.total_dead != null || lifespan.total_alive != null)) {
      var byStatus = lifespan.by_status_avg_hours || {};
      html += '<div class="stats-summary-bar account-lifespan-bar">'
        + _renderLifespanBadge('accounts.avg_lifespan', _formatLifespan(lifespan.avg_lifespan_hours), 'stats-badge-info')
        + _renderLifespanBadge('accounts.median_lifespan', _formatLifespan(lifespan.median_lifespan_hours), 'stats-badge-accent')
        + _renderLifespanBadge('accounts.alive_age', _formatLifespan(lifespan.avg_alive_hours), 'stats-badge-success')
        + _renderLifespanBadge('accounts.dead_count', _fmtNum(lifespan.total_dead || 0), 'stats-badge-warning')
        + _renderLifespanBadge('accounts.alive_count', _fmtNum(lifespan.total_alive || 0), 'stats-badge-cyan')
        + _renderLifespanBadge('accounts.lifespan_banned', _formatLifespan(byStatus.banned), 'stats-badge-warning')
        + _renderLifespanBadge('accounts.lifespan_wasted', _formatLifespan(byStatus.wasted), 'stats-badge-info')
        + _renderLifespanBadge('accounts.lifespan_expired', _formatLifespan(byStatus.expired), 'stats-badge-accent')
        + '</div>';
    }
    container.innerHTML = html;
  }

  var _lastModels = [];
  var _testInProgress = false;

  function _renderModels(models) {
    var modelList = document.getElementById('modelList');
    if (!modelList) return;
    _lastModels = Array.isArray(models) ? models : [];
    if (_lastModels.length === 0) {
      modelList.innerHTML = '<div class="empty-state"><span>' + escapeHtml(t('dashboard.no_models')) + '</span></div>';
      return;
    }
    var html = '';
    for (var j = 0; j < _lastModels.length; j++) {
      var m = _lastModels[j];
      html += '<div class="model-item" id="model-row-' + j + '">'
        + '<span class="model-name">' + escapeHtml(m.id || '') + '</span>'
        + '<span class="model-display">' + escapeHtml(m.display_name || '') + '</span>'
        + '<span class="model-test-status" id="model-test-' + j + '"></span>'
        + '</div>';
    }
    modelList.innerHTML = html;
  }

  /**
   * 批量测试所有模型
   *
   * 通过后端 POST /admin/api/models/test 端点测试，
   * 后端对每个模型发送本地回环请求，避免前端需要知道 API 密码。
   *
   * 修复 BUG-002: "Cannot read properties of undefined (reading 'status')"
   * 根因: 原始实现中前端直接 fetch API 测试模型时，
   *       fetch 抛出网络异常后 response 为 undefined，
   *       后续代码仍尝试读取 response.status 导致 TypeError。
   * 修复: 后端端点内对 fetch 异常做了完整的 try/catch，
   *       前端也对 API 返回的结果做防御性检查。
   */
  function testModels() {
    if (_testInProgress) return;
    if (!_lastModels || _lastModels.length === 0) {
      toast(t('dashboard.no_models'), 'info');
      return;
    }

    _testInProgress = true;
    var btn = document.getElementById('btnTestModels');
    if (btn) {
      btn.disabled = true;
      btn.textContent = t('dashboard.testing_models');
    }

    // 先标记所有模型为"测试中"
    for (var k = 0; k < _lastModels.length; k++) {
      var statusEl = document.getElementById('model-test-' + k);
      if (statusEl) {
        statusEl.innerHTML = '<span class="badge badge-cooldown">' + escapeHtml(t('dashboard.testing_models')) + '</span>';
      }
    }

    api('POST', '/models/test')
      .then(function (data) {
        // 防御性检查: data 和 data.results 都可能为 undefined/null
        var resultList = (data && Array.isArray(data.results)) ? data.results : [];
        var okCount = (data && typeof data.ok === 'number') ? data.ok : 0;
        var failCount = (data && typeof data.fail === 'number') ? data.fail : 0;

        // 按模型名匹配结果（后端返回顺序可能和前端不一致）
        var resultMap = {};
        for (var r = 0; r < resultList.length; r++) {
          var item = resultList[r];
          if (item && item.model) {
            resultMap[item.model] = item;
          }
        }

        for (var i = 0; i < _lastModels.length; i++) {
          var model = _lastModels[i];
          var el = document.getElementById('model-test-' + i);
          if (!el) continue;

          // 防御性检查: result 可能不存在（模型在后端被跳过等）
          var result = resultMap[model.id];
          if (!result) {
            el.innerHTML = '<span class="badge badge-expired">' + escapeHtml(t('dashboard.test_error')) + '</span>';
            continue;
          }

          // 安全读取 result.status -- 这里 result 一定不是 undefined（上面已检查）
          if (result.status === 'ok') {
            el.innerHTML = '<span class="badge badge-active">' + escapeHtml(t('dashboard.test_ok'))
              + '</span> <span class="model-latency">' + escapeHtml(t('dashboard.test_latency', { ms: result.latency || 0 })) + '</span>';
          } else {
            var errorText = result.error || t('dashboard.test_error');
            if (errorText.length > 50) errorText = errorText.substring(0, 47) + '...';
            el.innerHTML = '<span class="badge badge-banned">' + escapeHtml(t('dashboard.test_fail'))
              + '</span> <span class="model-error" title="' + escapeHtml(result.error || '') + '">' + escapeHtml(errorText) + '</span>';
          }
        }

        toast(t('dashboard.test_complete', { ok: okCount, fail: failCount }),
          failCount === 0 ? 'success' : 'warning');
      })
      .catch(function (err) {
        // API 调用本身失败
        for (var j = 0; j < _lastModels.length; j++) {
          var errEl = document.getElementById('model-test-' + j);
          if (errEl) {
            errEl.innerHTML = '<span class="badge badge-banned">' + escapeHtml(t('dashboard.test_error')) + '</span>';
          }
        }
        toast(t('dashboard.load_failed') + ': ' + ((err && err.message) || 'unknown'), 'error');
      })
      .finally(function () {
        _testInProgress = false;
        if (btn) {
          btn.disabled = false;
          btn.textContent = t('dashboard.test_models');
        }
      });
  }

  function _renderServiceInfo(data) {
    var serviceInfo = document.getElementById('serviceInfo');
    if (!serviceInfo) return;
    var infoItems = [
      { label: t('dashboard.uptime'), value: formatUptime(data.uptime) },
      { label: t('dashboard.scheduler'), value: (function (s) {
        var key = { round_robin: 'config.scheduler_round_robin', random: 'config.scheduler_random', least_used: 'config.scheduler_least_used' }[s];
        return key ? t(key) : (s || '-');
      })(data.scheduler) },
      { label: t('dashboard.auto_health_title'), value: _formatAccountHealthSummary(_accountHealthStatus) },
      { label: t('dashboard.version'), value: data.version || '-' },
      { label: t('dashboard.node_version'), value: data.node_version || '-' },
    ];
    var html = '';
    for (var k = 0; k < infoItems.length; k++) {
      var item = infoItems[k];
      html += '<div class="info-row">'
        + '<span class="info-label">' + escapeHtml(item.label) + '</span>'
        + '<span class="info-value">' + escapeHtml(item.value) + '</span>'
        + '</div>';
    }
    serviceInfo.innerHTML = html;
  }

  function _formatIsoDateTime(value) {
    if (!value) return '-';
    var d = new Date(value);
    if (!d || Number.isNaN(d.getTime())) return '-';
    return d.toLocaleString();
  }

  function _formatAccountHealthSummary(status) {
    if (!status || typeof status !== 'object') return t('common.loading');
    if (status.enabled === false) return t('common.disabled');

    var runtime = status.runtime && typeof status.runtime === 'object' ? status.runtime : {};
    var summary = status.summary && typeof status.summary === 'object' ? status.summary : {};
    var stateText = runtime.running ? t('dashboard.health_checking') : t('pool_health.idle');
    var lastCheckAt = _formatIsoDateTime(summary.last_check_at);
    var coverage = Number(summary.coverage_percent);
    if (!Number.isFinite(coverage) || coverage < 0) coverage = 0;
    var issues = Number(summary.issues_in_cycle);
    if (!Number.isFinite(issues) || issues < 0) issues = 0;

    return stateText
      + ' | ' + t('dashboard.auto_health_last_check') + ': ' + lastCheckAt
      + ' | ' + t('dashboard.auto_health_coverage') + ': ' + coverage + '%'
      + ' | ' + t('dashboard.auto_health_issues') + ': ' + issues;
  }

  function render(data) {
    _dashboardData = data || {};
    _renderAccountHealthBar(data.accounts || {}, data.account_lifespan || {});
    _renderModels(data.models || []);
    _renderServiceInfo(data);
  }

  function checkHealth() {
    var statusEl = document.getElementById('healthStatus');
    if (!statusEl) return;
    api('GET', '/dashboard')
      .then(function () {
        statusEl.className = 'health-dot health-online';
        statusEl.setAttribute('title', t('dashboard.health_online'));
      })
      .catch(function () {
        statusEl.className = 'health-dot health-offline';
        statusEl.setAttribute('title', t('dashboard.health_offline'));
      });
  }

  function startHealthCheck() {
    checkHealth();
    if (_healthTimer) clearInterval(_healthTimer);
    _healthTimer = setInterval(checkHealth, 30000);
  }

  function stopHealthCheck() {
    if (_healthTimer) {
      clearInterval(_healthTimer);
      _healthTimer = null;
    }
  }

  window.addEventListener('daterange-change', function () {
    var activePage = document.querySelector('.page.active');
    if (activePage && activePage.id === 'pageDashboard') {
      load();
    }
  });

  return {
    load: load,
    render: render,
    checkHealth: checkHealth,
    startHealthCheck: startHealthCheck,
    stopHealthCheck: stopHealthCheck,
    testModels: testModels,
  };
})();
