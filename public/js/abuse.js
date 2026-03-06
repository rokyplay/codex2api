/**
 * codex2api 管理面板 — 风控模块
 */

var Abuse = (function () {
  'use strict';

  var _bound = false;
  var _refreshTimer = null;
  var _currentIdentity = '';
  var _userQuery = {
    page: 1,
    limit: 50,
    level: '',
    sort: 'score_desc',
    keyword: '',
  };

  function _getData(resp) {
    if (!resp || typeof resp !== 'object') return null;
    if (Object.prototype.hasOwnProperty.call(resp, 'data')) return resp.data;
    return resp;
  }

  function _getMeta(resp, fallback) {
    if (!resp || typeof resp !== 'object' || !resp.meta || typeof resp.meta !== 'object') {
      return fallback || {};
    }
    return resp.meta;
  }

  function _levelLabel(level) {
    return t('abuse.level_' + String(level || 'low'));
  }

  function _actionLabel(action) {
    return t('abuse.action_' + String(action || 'observe'));
  }

  function _badgeClassByLevel(level) {
    if (level === 'critical') return 'stats-badge-warning';
    if (level === 'high') return 'stats-badge-accent';
    if (level === 'medium') return 'stats-badge-info';
    return 'stats-badge-success';
  }

  function _badgeClassByAction(action) {
    if (action === 'suspend') return 'stats-badge-warning';
    if (action === 'challenge') return 'stats-badge-accent';
    if (action === 'throttle') return 'stats-badge-info';
    return 'stats-badge-success';
  }

  function _renderOverview(overview) {
    var container = document.getElementById('abuseOverviewCards');
    if (!container) return;
    overview = overview || {};
    var levels = overview.levels || {};
    var actions = overview.actions || {};
    var cards = [
      {
        label: t('abuse.card_total_users'),
        value: overview.total_users || 0,
        cls: 'hero-info',
        icon: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>',
      },
      {
        label: t('abuse.card_critical_users'),
        value: levels.critical || 0,
        cls: 'hero-warning',
        icon: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86l-1.82 3.68a2 2 0 0 1-1.5 1.09l-4.06.59 2.94 2.87a2 2 0 0 1 .58 1.77l-.69 4.04 3.63-1.91a2 2 0 0 1 1.86 0l3.63 1.91-.69-4.04a2 2 0 0 1 .58-1.77l2.94-2.87-4.06-.59a2 2 0 0 1-1.5-1.09l-1.82-3.68z"/></svg>',
      },
      {
        label: t('abuse.card_suspend_users'),
        value: actions.suspend || 0,
        cls: 'hero-accent',
        icon: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2l7 4v6c0 5-3.5 9-7 10-3.5-1-7-5-7-10V6l7-4z"/><path d="M9 9l6 6"/><path d="M15 9l-6 6"/></svg>',
      },
      {
        label: t('abuse.card_today_events'),
        value: overview.today_events || 0,
        cls: 'hero-cyan',
        icon: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 2v4"/><path d="M16 2v4"/><rect x="3" y="4" width="18" height="18" rx="2"/><path d="M3 10h18"/></svg>',
      },
    ];

    var html = '';
    for (var i = 0; i < cards.length; i++) {
      var c = cards[i];
      html += '<div class="hero-card ' + c.cls + '">'
        + '<div class="hero-icon">' + c.icon + '</div>'
        + '<div class="hero-content">'
        + '<div class="hero-value">' + escapeHtml(String(c.value)) + '</div>'
        + '<div class="hero-label">' + escapeHtml(c.label) + '</div>'
        + '</div>'
        + '</div>';
    }
    container.innerHTML = html;
  }

  function _renderUsers(users, meta) {
    var body = document.getElementById('abuseUsersBody');
    if (!body) return;
    if (!Array.isArray(users) || users.length === 0) {
      body.innerHTML = '<tr><td colspan="7"><div class="empty-state"><span>' + escapeHtml(t('abuse.users_empty')) + '</span></div></td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < users.length; i++) {
      var row = users[i] || {};
      var user = row.user || null;
      var identity = String(row.caller_identity || row.id || '');
      var score = Number(row.score || 0);
      var level = String(row.level || 'low');
      var action = String(row.action || 'observe');
      var reasonCount = Number(row.reasons_count || 0);
      var displayName = identity;
      if (user && (user.global_name || user.username)) {
        displayName = (user.global_name || user.username) + ' (' + identity + ')';
      }
      html += '<tr>'
        + '<td class="td-mono td-email" title="' + escapeHtml(identity) + '">' + escapeHtml(displayName) + '</td>'
        + '<td class="td-mono">' + escapeHtml(String(score)) + '</td>'
        + '<td><span class="stats-badge ' + _badgeClassByLevel(level) + '">' + escapeHtml(_levelLabel(level)) + '</span></td>'
        + '<td><span class="stats-badge ' + _badgeClassByAction(action) + '">' + escapeHtml(_actionLabel(action)) + '</span></td>'
        + '<td class="td-mono">' + escapeHtml(String(reasonCount)) + '</td>'
        + '<td class="td-mono">' + escapeHtml(formatDateTime(row.last_eval_at || row.updated_at || '')) + '</td>'
        + '<td><button class="btn btn-secondary btn-sm abuse-detail-btn" data-identity="' + escapeHtml(identity) + '">' + escapeHtml(t('abuse.view_detail')) + '</button></td>'
        + '</tr>';
    }
    body.innerHTML = html;

    var total = meta.total || users.length;
    var page = meta.page || 1;
    var pages = meta.pages || 1;
    var title = document.getElementById('pageTitle');
    if (title && document.getElementById('pageAbuse') && document.getElementById('pageAbuse').classList.contains('active')) {
      title.textContent = t('nav.abuse') + ' · ' + page + '/' + pages + ' · ' + total;
    }
  }

  function _renderEvents(events) {
    var list = document.getElementById('abuseEventsList');
    if (!list) return;
    if (!Array.isArray(events) || events.length === 0) {
      list.innerHTML = '<div class="empty-state"><span>' + escapeHtml(t('abuse.events_empty')) + '</span></div>';
      return;
    }
    var html = '';
    for (var i = 0; i < events.length; i++) {
      var e = events[i] || {};
      var ts = formatDateTime(Number(e.ts || 0));
      var identity = String(e.caller_identity || '-');
      var ruleId = String(e.rule_id || '-');
      var action = String(e.action || 'observe');
      var score = Number(e.score || 0);
      html += '<div class="log-item">'
        + '<div class="log-meta"><span class="log-time">' + escapeHtml(ts) + '</span><span class="log-level-badge log-level-warn">' + escapeHtml(_actionLabel(action)) + '</span></div>'
        + '<div class="log-message">' + escapeHtml(identity) + ' · ' + escapeHtml(ruleId) + ' · ' + escapeHtml(t('abuse.th_score')) + '=' + escapeHtml(String(score)) + '</div>'
        + '</div>';
    }
    list.innerHTML = html;
  }

  function _renderRulesEditor(rulesConfig) {
    var editor = document.getElementById('abuseRulesEditor');
    if (!editor) return;
    try {
      editor.value = JSON.stringify(rulesConfig || {}, null, 2);
    } catch (_) {
      editor.value = '{}';
    }
  }

  function _renderDetail(detail) {
    var summary = document.getElementById('abuseDetailSummary');
    var rules = document.getElementById('abuseDetailRules');
    var timeline = document.getElementById('abuseDetailTimeline');
    if (!summary || !rules || !timeline) return;
    if (!detail) {
      summary.innerHTML = '';
      rules.innerHTML = '';
      timeline.innerHTML = '';
      return;
    }
    var risk = detail.risk || {};
    var identity = detail.identity || '';
    var level = String(risk.level || 'low');
    var action = risk.actions ? (risk.actions.applied || 'observe') : 'observe';
    summary.innerHTML = '<span class="stats-badge stats-badge-info">' + escapeHtml(t('abuse.th_identity')) + ': <strong>' + escapeHtml(identity) + '</strong></span>'
      + '<span class="stats-badge ' + _badgeClassByLevel(level) + '">' + escapeHtml(t('abuse.th_level')) + ': <strong>' + escapeHtml(_levelLabel(level)) + '</strong></span>'
      + '<span class="stats-badge ' + _badgeClassByAction(action) + '">' + escapeHtml(t('abuse.th_action')) + ': <strong>' + escapeHtml(_actionLabel(action)) + '</strong></span>'
      + '<span class="stats-badge stats-badge-accent">' + escapeHtml(t('abuse.th_score')) + ': <strong>' + escapeHtml(String(risk.score || 0)) + '</strong></span>';

    var reasons = Array.isArray(risk.reasons) ? risk.reasons : [];
    if (reasons.length === 0) {
      rules.innerHTML = '<div class="empty-state"><span>' + escapeHtml(t('abuse.rules_empty')) + '</span></div>';
    } else {
      var rulesHtml = '<div class="table-wrapper"><div class="table-scroll"><table><thead><tr>'
        + '<th>' + escapeHtml(t('abuse.th_rule')) + '</th>'
        + '<th>' + escapeHtml(t('abuse.th_value')) + '</th>'
        + '<th>' + escapeHtml(t('abuse.th_threshold')) + '</th>'
        + '</tr></thead><tbody>';
      for (var i = 0; i < reasons.length; i++) {
        var r = reasons[i] || {};
        rulesHtml += '<tr>'
          + '<td class="td-mono">' + escapeHtml(String(r.rule_id || '-')) + '</td>'
          + '<td class="td-mono">' + escapeHtml(String(r.value || '-')) + '</td>'
          + '<td class="td-mono">' + escapeHtml(String(r.threshold || '-')) + '</td>'
          + '</tr>';
      }
      rulesHtml += '</tbody></table></div></div>';
      rules.innerHTML = rulesHtml;
    }

    var events = (detail.timeline && detail.timeline.data) ? detail.timeline.data : [];
    if (!Array.isArray(events) || events.length === 0) {
      timeline.innerHTML = '<div class="empty-state"><span>' + escapeHtml(t('abuse.events_empty')) + '</span></div>';
    } else {
      var timelineHtml = '';
      for (var j = 0; j < events.length; j++) {
        var e = events[j] || {};
        timelineHtml += '<div class="log-item">'
          + '<div class="log-meta"><span class="log-time">' + escapeHtml(formatDateTime(e.ts || 0)) + '</span></div>'
          + '<div class="log-message">' + escapeHtml(String(e.rule_id || '-')) + ' · ' + escapeHtml(String(e.action || '-')) + ' · ' + escapeHtml(t('abuse.th_score')) + '=' + escapeHtml(String(e.score || 0)) + '</div>'
          + '</div>';
      }
      timeline.innerHTML = timelineHtml;
    }
  }

  function _loadOverview() {
    return api('GET', '/abuse/overview')
      .then(function (resp) {
        _renderOverview(_getData(resp));
      })
      .catch(function (err) {
        toast(t('abuse.load_failed') + ': ' + err.message, 'error');
      });
  }

  function _loadUsers() {
    var params = [];
    params.push('page=' + encodeURIComponent(String(_userQuery.page)));
    params.push('limit=' + encodeURIComponent(String(_userQuery.limit)));
    if (_userQuery.level) params.push('level=' + encodeURIComponent(_userQuery.level));
    if (_userQuery.sort) params.push('sort=' + encodeURIComponent(_userQuery.sort));
    if (_userQuery.keyword) params.push('q=' + encodeURIComponent(_userQuery.keyword));
    return api('GET', '/abuse/users?' + params.join('&'))
      .then(function (resp) {
        _renderUsers(_getData(resp), _getMeta(resp, {}));
      })
      .catch(function (err) {
        toast(t('abuse.load_failed') + ': ' + err.message, 'error');
      });
  }

  function _loadEvents() {
    return api('GET', '/abuse/events?limit=80')
      .then(function (resp) {
        _renderEvents(_getData(resp));
      })
      .catch(function (err) {
        toast(t('abuse.load_failed') + ': ' + err.message, 'error');
      });
  }

  function _loadRules() {
    return api('GET', '/abuse/rules')
      .then(function (resp) {
        _renderRulesEditor(_getData(resp));
      })
      .catch(function (err) {
        toast(t('abuse.load_failed') + ': ' + err.message, 'error');
      });
  }

  function _loadDetail(identity) {
    if (!identity) return Promise.resolve();
    _currentIdentity = identity;
    return api('GET', '/abuse/user/' + encodeURIComponent(identity))
      .then(function (resp) {
        _renderDetail(_getData(resp));
        openModal('abuseDetailModal');
      })
      .catch(function (err) {
        toast(t('abuse.load_failed') + ': ' + err.message, 'error');
      });
  }

  function _saveRules() {
    var editor = document.getElementById('abuseRulesEditor');
    if (!editor) return;
    var payload = null;
    try {
      payload = JSON.parse(editor.value || '{}');
    } catch (e) {
      toast(t('abuse.invalid_rules_json'), 'error');
      return;
    }
    api('PUT', '/abuse/rules', payload)
      .then(function (resp) {
        _renderRulesEditor(_getData(resp));
        toast(t('abuse.rules_saved'), 'success');
        _reloadAll();
      })
      .catch(function (err) {
        toast(t('abuse.save_failed') + ': ' + err.message, 'error');
      });
  }

  function _applyAction(action) {
    if (!_currentIdentity) return;
    showConfirm(t('abuse.confirm_action', { action: _actionLabel(action) }), t('abuse.detail_title')).then(function (ok) {
      if (!ok) return;
      api('POST', '/abuse/user/' + encodeURIComponent(_currentIdentity) + '/action', {
        action: action,
        reason: 'admin_manual',
      })
        .then(function () {
          toast(t('abuse.action_done'), 'success');
          return _loadDetail(_currentIdentity);
        })
        .then(function () {
          _loadOverview();
          _loadUsers();
          _loadEvents();
        })
        .catch(function (err) {
          toast(t('abuse.action_failed') + ': ' + err.message, 'error');
        });
    });
  }

  function _reloadAll() {
    _loadOverview();
    _loadUsers();
    _loadEvents();
    _loadRules();
  }

  function _startAutoRefresh() {
    _stopAutoRefresh();
    _refreshTimer = setInterval(function () {
      _loadOverview();
      _loadUsers();
      _loadEvents();
    }, 15000);
  }

  function _stopAutoRefresh() {
    if (_refreshTimer) {
      clearInterval(_refreshTimer);
      _refreshTimer = null;
    }
  }

  function _bindEvents() {
    if (_bound) return;
    _bound = true;

    var levelFilter = document.getElementById('abuseLevelFilter');
    if (levelFilter) {
      levelFilter.addEventListener('change', function () {
        _userQuery.level = levelFilter.value || '';
        _userQuery.page = 1;
        _loadUsers();
      });
    }

    var sortSelect = document.getElementById('abuseSortSelect');
    if (sortSelect) {
      sortSelect.addEventListener('change', function () {
        _userQuery.sort = sortSelect.value || 'score_desc';
        _userQuery.page = 1;
        _loadUsers();
      });
    }

    var searchInput = document.getElementById('abuseSearchInput');
    if (searchInput) {
      searchInput.addEventListener('input', debounce(function () {
        _userQuery.keyword = String(searchInput.value || '').trim();
        _userQuery.page = 1;
        _loadUsers();
      }, 300));
    }

    var refreshUsersBtn = document.getElementById('abuseRefreshUsersBtn');
    if (refreshUsersBtn) {
      refreshUsersBtn.addEventListener('click', function () {
        _loadUsers();
      });
    }

    var refreshEventsBtn = document.getElementById('abuseRefreshEventsBtn');
    if (refreshEventsBtn) {
      refreshEventsBtn.addEventListener('click', function () {
        _loadEvents();
      });
    }

    var saveRulesBtn = document.getElementById('abuseSaveRulesBtn');
    if (saveRulesBtn) {
      saveRulesBtn.addEventListener('click', _saveRules);
    }

    var usersBody = document.getElementById('abuseUsersBody');
    if (usersBody) {
      usersBody.addEventListener('click', function (e) {
        var btn = e.target.closest('.abuse-detail-btn');
        if (!btn) return;
        var identity = btn.getAttribute('data-identity') || '';
        _loadDetail(identity);
      });
    }

    var modalClose = document.getElementById('abuseDetailModalClose');
    if (modalClose) {
      modalClose.addEventListener('click', function () {
        closeModal('abuseDetailModal');
      });
    }
    var modalCancel = document.getElementById('abuseDetailModalCancel');
    if (modalCancel) {
      modalCancel.addEventListener('click', function () {
        closeModal('abuseDetailModal');
      });
    }

    var modal = document.getElementById('abuseDetailModal');
    if (modal) {
      modal.addEventListener('click', function (e) {
        var actionBtn = e.target.closest('[data-abuse-action]');
        if (!actionBtn) return;
        var action = actionBtn.getAttribute('data-abuse-action');
        if (!action) return;
        _applyAction(action);
      });
    }
  }

  function load() {
    _bindEvents();
    _reloadAll();
    _startAutoRefresh();
  }

  function cleanup() {
    _stopAutoRefresh();
  }

  return {
    load: load,
    cleanup: cleanup,
  };
})();
