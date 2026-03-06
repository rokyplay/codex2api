/**
 * codex2api 管理面板 — 请求日志页
 *
 * 功能:
 *   - 顶部 RPM/TPM/成功率 badge
 *   - 请求日志表格 (时间/路由/模型/账号编号/用时+首字/输入/输出/状态/详情)
 *   - 服务端分页
 *   - 行内展开详情
 *   - 筛选: 全部/成功/失败
 */

var Statistics = (function () {
  'use strict';

  function _todayStr() {
    var d = new Date();
    return d.getFullYear()
      + '-' + String(d.getMonth() + 1).padStart(2, '0')
      + '-' + String(d.getDate()).padStart(2, '0');
  }

  function _isValidDateStr(s) {
    return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
  }

  function _normalizeMemoryLimit(limit) {
    var n = parseInt(limit, 10);
    if (!n || n < 1) n = 20;
    if (n > 100) n = 100;
    return n;
  }

  function _normalizeFileLimit(limit) {
    var n = parseInt(limit, 10);
    if (!n || n < 1) n = 500;
    if (n > 2000) n = 2000;
    return n;
  }

  var _state = {
    page: 1,
    limit: 20,
    filter: 'all',
    search: '',
    source: 'memory',
    historyDate: _todayStr(),
    searchTimer: null,
    accountMap: {},
    data: [],
    total: 0,
    pages: 0,
    expandedIdx: -1,
    refreshTimer: null,
    rendered: false,
    overviewData: null,
    callerStats: [],
  };

  function _normalizeStateBySource() {
    if (_state.source === 'file') {
      _state.limit = _normalizeFileLimit(_state.limit);
      if (!_isValidDateStr(_state.historyDate)) _state.historyDate = _todayStr();
      return;
    }
    _state.limit = _normalizeMemoryLimit(_state.limit);
  }

  function _syncRefreshTimer() {
    var shouldRefresh = _state.source === 'memory';
    if (shouldRefresh) {
      if (_state.refreshTimer) return;
      _state.refreshTimer = setInterval(function () {
        if (_state.source !== 'memory') return;
        _loadOverview();
        _loadCallerStats();
        _loadData();
      }, 15000);
      return;
    }
    if (_state.refreshTimer) {
      clearInterval(_state.refreshTimer);
      _state.refreshTimer = null;
    }
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

  function _shouldFallbackCaller(data) {
    if (!_isRankRowsAllZero(data)) return false;
    if (_isHoursStatsMode()) return false;
    if (typeof getDateRange !== 'function') return false;
    var r = getDateRange();
    return !!(r && r.mode === 'preset' && r.preset === 'today');
  }

  function init(force) {
    _normalizeStateBySource();
    _render(force);
    _loadAccountMap();
    _loadOverview();
    _loadCallerStats();
    _loadData();
    _syncRefreshTimer();
  }

  function cleanup() {
    if (_state.refreshTimer) {
      clearInterval(_state.refreshTimer);
      _state.refreshTimer = null;
    }
    if (_state.searchTimer) {
      clearTimeout(_state.searchTimer);
      _state.searchTimer = null;
    }
  }

  // ─── 页面骨架 ───

  function _render(force) {
    var el = document.getElementById('pageStatistics');
    if (!el) return;
    if (!force && _state.rendered) return;
    _state.rendered = true;
    var hasDatePicker = (typeof DatePicker !== 'undefined' && DatePicker && typeof DatePicker.init === 'function');
    var historyDateInput = hasDatePicker
      ? '<div class="stats-history-date-wrap"><input type="date" class="stats-history-date-input" id="statsHistoryDateInput" value="' + _state.historyDate + '"></div>'
      : '<input type="date" class="stats-history-date-input" id="statsHistoryDateInput" value="' + _state.historyDate + '">';

    el.innerHTML = ''
      + '<div class="stats-summary-bar" id="statsSummaryBar"></div>'
      + '<div class="stats-filter-bar">'
      +   '<div class="recent-filters" id="statsFilters">'
      +     '<button class="recent-filter-btn active" data-filter="all">' + t('stats.filter_all') + '</button>'
      +     '<button class="recent-filter-btn" data-filter="success">' + t('stats.filter_success') + '</button>'
      +     '<button class="recent-filter-btn" data-filter="error">' + t('stats.filter_error') + '</button>'
      +   '</div>'
      +   '<div class="stats-search-wrap">'
      +     '<input type="text" class="stats-search-input" id="statsSearchInput" placeholder="' + t('stats.search_placeholder') + '">'
      +   '</div>'
      + '</div>'
      + '<div class="stats-mode-tools">'
      +   '<button class="btn btn-secondary btn-sm stats-source-toggle" id="statsSourceToggle" type="button"></button>'
      +   '<div class="stats-history-tools" id="statsHistoryTools">'
      +     '<label class="stats-history-label" for="statsHistoryDateInput">' + t('stats.history_date') + '</label>'
      +     historyDateInput
      +   '</div>'
      +   '<span class="stats-total-info" id="statsTotalInfo"></span>'
      + '</div>'
      + '<div class="card" style="margin-top:8px;">'
      +   '<div class="table-scroll" id="statsTableWrap"></div>'
      + '</div>'
      + '<div class="stats-pagination" id="statsPagination"></div>';

    _bindEvents(el);
    _renderModeControls();
    _renderTotalInfo();
  }

  function _renderModeControls() {
    var sourceBtn = document.getElementById('statsSourceToggle');
    var historyTools = document.getElementById('statsHistoryTools');
    var dateInput = document.getElementById('statsHistoryDateInput');
    if (sourceBtn) {
      sourceBtn.textContent = _state.source === 'file' ? t('stats.switch_to_recent') : t('stats.switch_to_history');
      sourceBtn.classList.toggle('active', _state.source === 'file');
    }
    if (historyTools) {
      historyTools.style.display = _state.source === 'file' ? 'inline-flex' : 'none';
    }
    if (dateInput && _isValidDateStr(_state.historyDate)) {
      dateInput.value = _state.historyDate;
    }
    _renderTotalInfo();
  }

  function _renderTotalInfo() {
    var el = document.getElementById('statsTotalInfo');
    if (!el) return;
    var modeKey = _state.source === 'file' ? 'stats.mode_history' : 'stats.mode_recent';
    el.textContent = t('stats.mode_total', { mode: t(modeKey), total: _state.total || 0 });
  }

  // ─── 事件绑定 ───

  function _bindEvents(root) {
    root.addEventListener('click', function (e) {
      // Source mode toggle
      var sourceBtn = e.target.closest('#statsSourceToggle');
      if (sourceBtn) {
        if (_state.source === 'memory') {
          _state.source = 'file';
          _state.limit = 500;
          if (!_isValidDateStr(_state.historyDate)) _state.historyDate = _todayStr();
        } else {
          _state.source = 'memory';
          _state.limit = _normalizeMemoryLimit(_state.limit);
          if (_state.search) {
            _state.search = '';
            var input = root.querySelector('#statsSearchInput');
            if (input) input.value = '';
          }
        }
        _state.page = 1;
        _state.expandedIdx = -1;
        _renderModeControls();
        _syncRefreshTimer();
        _loadData();
        return;
      }

      // Filter buttons
      var filterBtn = e.target.closest('.recent-filter-btn[data-filter]');
      if (filterBtn) {
        _state.filter = filterBtn.getAttribute('data-filter') || 'all';
        _state.page = 1;
        _state.expandedIdx = -1;
        // Update active class
        var btns = root.querySelectorAll('.recent-filter-btn');
        for (var i = 0; i < btns.length; i++) {
          btns[i].classList.toggle('active', btns[i] === filterBtn);
        }
        _loadData();
        return;
      }

      // Pagination
      var pageBtn = e.target.closest('.stats-page-btn[data-page]');
      if (pageBtn) {
        var p = parseInt(pageBtn.getAttribute('data-page'), 10);
        if (p > 0 && p !== _state.page) {
          _state.page = p;
          _state.expandedIdx = -1;
          _loadData();
        }
        return;
      }

      // Per-page selector
      var ppBtn = e.target.closest('.stats-pp-opt[data-limit]');
      if (ppBtn) {
        var newLimit = parseInt(ppBtn.getAttribute('data-limit'), 10);
        if (newLimit > 0 && newLimit !== _state.limit) {
          _state.limit = _state.source === 'file'
            ? _normalizeFileLimit(newLimit)
            : _normalizeMemoryLimit(newLimit);
          _state.page = 1;
          _state.expandedIdx = -1;
          _loadData();
        }
        return;
      }

      // Expand row
      var expandBtn = e.target.closest('.stats-expand-btn');
      if (expandBtn) {
        var idx = parseInt(expandBtn.getAttribute('data-idx'), 10);
        _state.expandedIdx = (_state.expandedIdx === idx) ? -1 : idx;
        _renderTable();
        return;
      }
    });

    var searchInput = root.querySelector('#statsSearchInput');
    if (searchInput) {
      searchInput.value = _state.search || '';
      searchInput.addEventListener('input', function () {
        var val = searchInput.value;
        if (_state.searchTimer) {
          clearTimeout(_state.searchTimer);
        }
        _state.searchTimer = setTimeout(function () {
          var nextSearch = val.trim();
          _state.searchTimer = null;
          if (nextSearch === _state.search) return;
          _state.search = nextSearch;
          if (_state.search && _state.source !== 'file') {
            _state.source = 'file';
            if (_state.limit <= 100) _state.limit = 500;
            _state.limit = _normalizeFileLimit(_state.limit);
            if (!_isValidDateStr(_state.historyDate)) _state.historyDate = _todayStr();
          }
          _state.page = 1;
          _state.expandedIdx = -1;
          _renderModeControls();
          _syncRefreshTimer();
          _loadData();
        }, 300);
      });
    }

    var historyDateInput = root.querySelector('#statsHistoryDateInput');
    if (historyDateInput) {
      historyDateInput.value = _state.historyDate || _todayStr();
      historyDateInput.addEventListener('change', function () {
        var nextDate = historyDateInput.value;
        if (!_isValidDateStr(nextDate)) {
          historyDateInput.value = _state.historyDate;
          return;
        }
        if (nextDate === _state.historyDate) return;
        _state.historyDate = nextDate;
        if (_state.source !== 'file') {
          _state.source = 'file';
          _state.limit = 500;
          _renderModeControls();
          _syncRefreshTimer();
        }
        _state.page = 1;
        _state.expandedIdx = -1;
        _loadData();
      });
    }
  }

  // ─── 数据加载 ───

  function _loadAccountMap() {
    api('GET', '/accounts/index-map').then(function (map) {
      _state.accountMap = map || {};
    }).catch(function () {});
  }

  function _loadOverview() {
    var query = buildQueryString(dateRangeToQuery());
    api('GET', '/stats/overview' + query).then(function (data) {
      if (_shouldFallbackOverview(data)) {
        return api('GET', '/stats/overview?total=true').then(function (fallbackData) {
          _state.overviewData = fallbackData || data;
          _renderSummaryBar(_state.overviewData);
        }).catch(function () {
          _state.overviewData = data;
          _renderSummaryBar(data);
        });
      }
      _state.overviewData = data;
      _renderSummaryBar(data);
    }).catch(function () {});
  }

  function _loadCallerStats() {
    var query = buildQueryString(dateRangeToQuery());
    api('GET', '/stats/callers' + query).then(function (data) {
      var list = Array.isArray(data) ? data : [];
      if (_shouldFallbackCaller(list)) {
        return api('GET', '/stats/callers?total=true').then(function (fallbackData) {
          _state.callerStats = Array.isArray(fallbackData) ? fallbackData : list;
          _renderSummaryBar(_state.overviewData || {});
        }).catch(function () {
          _state.callerStats = list;
          _renderSummaryBar(_state.overviewData || {});
        });
      }
      _state.callerStats = list;
      _renderSummaryBar(_state.overviewData || {});
    }).catch(function () {
      _state.callerStats = [];
      _renderSummaryBar(_state.overviewData || {});
    });
  }

  function _loadData() {
    _normalizeStateBySource();
    var query = {
      page: _state.page,
      limit: _state.limit,
    };
    var rangeQuery = (typeof dateRangeToQuery === 'function') ? dateRangeToQuery() : null;
    if (rangeQuery && typeof rangeQuery === 'object') {
      query = Object.assign(query, rangeQuery);
    }
    if (_state.filter !== 'all') query.filter = _state.filter;
    if (_state.search) query.search = _state.search;
    if (_state.source === 'file') {
      query.source = 'file';
      if (!_state.search && _isValidDateStr(_state.historyDate)) {
        query.date = _state.historyDate;
      }
    }
    var qs = buildQueryString(query);

    api('GET', '/stats/recent' + qs).then(function (result) {
      _state.data = result.data || [];
      _state.total = result.total || 0;
      _state.pages = result.pages || 1;
      _state.page = result.page || 1;
      _state.limit = _state.source === 'file'
        ? _normalizeFileLimit(result.limit || _state.limit)
        : _normalizeMemoryLimit(result.limit || _state.limit);
      _renderModeControls();
      _renderTotalInfo();
      _renderTable();
      _renderPagination();
    }).catch(function () {
      _state.data = [];
      _state.total = 0;
      _state.pages = 1;
      _state.page = 1;
      _renderTotalInfo();
      _renderTable();
      _renderPagination();
    });
  }

  // ─── 渲染: 顶部 badge ───

  function _renderSummaryBar(d) {
    var el = document.getElementById('statsSummaryBar');
    if (!el) return;
    var data = d || {};
    var html = ''
      + '<span class="stats-badge stats-badge-info">' + t('stats.total_requests') + ': ' + _fmtNum(data.total_requests) + '</span>'
      + '<span class="stats-badge stats-badge-success">' + t('stats.success_rate') + ': ' + (data.success_rate || 0) + '%</span>'
      + '<span class="stats-badge stats-badge-warning">RPM: ' + (data.rpm || 0) + '</span>'
      + '<span class="stats-badge stats-badge-cyan">TPM: ' + _fmtNum(data.tpm || 0) + '</span>'
      + '<span class="stats-badge stats-badge-accent">' + t('stats.avg_latency') + ': ' + (data.avg_latency || 0) + 'ms</span>';
    if (Object.prototype.hasOwnProperty.call(data, 'total_cached_tokens')) {
      var cachedSummaryText = _fmtCacheWithRate(data.total_cached_tokens, data.total_input_tokens);
      html += '<span class="stats-badge stats-badge-success">'
        + escapeHtml(t('stats.cache_total')) + ': ' + escapeHtml(cachedSummaryText)
        + '</span>';
    }
    var callers = Array.isArray(_state.callerStats) ? _state.callerStats : [];
    for (var i = 0; i < callers.length; i++) {
      var c = callers[i] || {};
      var identity = c.identity ? String(c.identity) : '-';
      var identityI18nKey = 'stats.caller_identity.' + identity;
      var displayName = t(identityI18nKey);
      if (!displayName || displayName === identityI18nKey) displayName = identity;
      var shortIdentity = displayName.length > 20 ? displayName.substring(0, 18) + '..' : displayName;
      html += '<span class="stats-badge stats-badge-info" title="' + escapeHtml(displayName) + '">'
        + escapeHtml(t('stats.caller')) + ': ' + escapeHtml(shortIdentity) + ' ' + _fmtNum(c.requests || 0)
        + '</span>';
    }
    el.innerHTML = html;
  }

  // ─── 渲染: 表格 ───

  function _renderTable() {
    var el = document.getElementById('statsTableWrap');
    if (!el) return;

    var data = _state.data;
    if (!data || data.length === 0) {
      el.innerHTML = '<div class="chart-empty">' + t('stats.no_data') + '</div>';
      return;
    }

    var html = '<table><thead><tr>'
      + '<th>' + t('stats.th_time') + '</th>'
      + '<th>' + t('stats.th_route') + '</th>'
      + '<th>' + t('stats.th_model') + '</th>'
      + '<th>' + t('stats.th_account') + '</th>'
      + '<th>' + t('stats.th_duration') + '</th>'
      + '<th>' + t('stats.th_input') + '</th>'
      + '<th>' + t('stats.th_output') + '</th>'
      + '<th>' + t('stats.th_cached') + '</th>'
      + '<th>' + t('stats.th_status') + '</th>'
      + '<th>' + t('stats.th_detail') + '</th>'
      + '</tr></thead><tbody>';

    for (var i = 0; i < data.length; i++) {
      var r = data[i];
      var isExpanded = (i === _state.expandedIdx);
      var statusCode = r.status || 0;
      var isOk = statusCode >= 200 && statusCode < 400;
      var statusCls = isOk ? 'badge-active' : 'badge-banned';
      var routeCls = 'route-badge route-badge-' + (r.route || 'default');

      var cachedCellText = _fmtCacheWithRate(r.cached_tokens, r.input_tokens);
      var cacheRate = _getCacheHitRatePercent(r.cached_tokens, r.input_tokens);
      var cachedCellClass = _cacheRateClass(cacheRate);
      html += '<tr class="stats-row' + (isExpanded ? ' is-expanded' : '') + '">'
        + '<td class="td-mono td-time">' + _fmtTime(r.ts) + '</td>'
        + '<td><span class="' + routeCls + '">' + escapeHtml(r.route || '-') + '</span></td>'
        + '<td class="td-mono td-model" title="' + escapeHtml(r.model || '') + '">' + _fmtModel(r.model) + '</td>'
        + '<td class="td-mono">' + _fmtAccount(r.account) + '</td>'
        + '<td class="td-duration">'
        +   '<span class="duration-total">' + _fmtDuration(r.latency) + '</span>'
        +   '<span class="duration-ttfb">' + _fmtDuration(r.ttfb_ms) + '</span>'
        + '</td>'
        + '<td class="td-mono">' + _fmtNum(r.input_tokens) + '</td>'
        + '<td class="td-mono">' + _fmtNum(r.output_tokens) + '</td>'
        + '<td class="td-mono td-cached' + (cachedCellClass ? ' ' + cachedCellClass : '') + '">' + escapeHtml(cachedCellText) + '</td>'
        + '<td><span class="badge ' + statusCls + '">' + statusCode + '</span></td>'
        + '<td>'
        +   '<button class="btn btn-secondary btn-sm stats-expand-btn" data-idx="' + i + '">'
        +     '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"'
        +     ' style="transition:transform 0.2s;' + (isExpanded ? 'transform:rotate(180deg)' : '') + '">'
        +     '<polyline points="6 9 12 15 18 9"/></svg>'
        +   '</button>'
        + '</td>'
        + '</tr>';

      if (isExpanded) {
        html += _renderExpandedRow(r);
      }
    }

    html += '</tbody></table>';
    el.innerHTML = html;
  }

  function _renderExpandedRow(r) {
    var totalTokens = (r.input_tokens || 0) + (r.output_tokens || 0);
    var cachedTokens = Number(r.cached_tokens) || 0;
    var reasoningTokens = Number(r.reasoning_tokens) || 0;
    var cacheHitRate = _fmtCacheHitRateOnly(cachedTokens, r.input_tokens);
    var fullEmail = r.account ? displayEmail(r.account) : '-';
    return '<tr class="stats-expand-row"><td colspan="10"><div class="stats-detail-grid">'
      + _detailKV(t('stats.th_time'), _fmtDateTime(r.ts))
      + _detailKV(t('stats.th_path'), r.path || '-')
      + _detailKV(t('stats.th_model'), r.model || '-')
      + _detailKV(t('stats.th_account'), fullEmail)
      + _detailKV(t('stats.th_latency'), (r.latency || 0) + 'ms')
      + _detailKV(t('stats.th_ttfb'), (r.ttfb_ms || 0) + 'ms')
      + _detailKV(t('stats.th_input'), _fmtNum(r.input_tokens))
      + _detailKV(t('stats.th_output'), _fmtNum(r.output_tokens))
      + _detailKV(t('stats.field_cached_tokens'), cachedTokens > 0 ? _fmtNum(cachedTokens) : '-')
      + _detailKV(t('stats.field_reasoning_tokens'), reasoningTokens > 0 ? _fmtNum(reasoningTokens) : '-')
      + _detailKV(t('stats.field_cache_hit_rate'), cacheHitRate)
      + _detailKV(t('stats.field_total_tokens'), _fmtNum(totalTokens))
      + _detailKV(t('stats.field_stream'), r.stream ? t('common.yes') : t('common.no'))
      + _detailKV(t('stats.th_error_type'), r.error_type || '-')
      + '</div></td></tr>';
  }

  function _detailKV(k, v) {
    return '<div class="stats-detail-item">'
      + '<span class="stats-detail-k">' + escapeHtml(k) + '</span>'
      + '<span class="stats-detail-v td-mono">' + escapeHtml(String(v)) + '</span>'
      + '</div>';
  }

  // ─── 渲染: 分页 ───

  function _renderPagination() {
    var el = document.getElementById('statsPagination');
    if (!el) return;

    var total = _state.total;
    var pages = _state.pages;
    var page = _state.page;
    var limit = _state.limit;

    if (total === 0) {
      el.innerHTML = '';
      return;
    }

    var from = (page - 1) * limit + 1;
    var to = Math.min(page * limit, total);

    var html = '<div class="stats-page-info">'
      + t('stats.showing', { from: from, to: to, total: total })
      + '</div>';

    // Page buttons
    html += '<div class="stats-page-btns">';

    if (page > 1) {
      html += '<button class="btn btn-secondary btn-sm stats-page-btn" data-page="' + (page - 1) + '">&laquo;</button>';
    }

    var startPage = Math.max(1, page - 2);
    var endPage = Math.min(pages, page + 2);
    if (startPage > 1) {
      html += '<button class="btn btn-secondary btn-sm stats-page-btn" data-page="1">1</button>';
      if (startPage > 2) html += '<span class="stats-page-ellipsis">...</span>';
    }
    for (var i = startPage; i <= endPage; i++) {
      var cls = i === page ? 'btn btn-primary btn-sm stats-page-btn active' : 'btn btn-secondary btn-sm stats-page-btn';
      html += '<button class="' + cls + '" data-page="' + i + '">' + i + '</button>';
    }
    if (endPage < pages) {
      if (endPage < pages - 1) html += '<span class="stats-page-ellipsis">...</span>';
      html += '<button class="btn btn-secondary btn-sm stats-page-btn" data-page="' + pages + '">' + pages + '</button>';
    }

    if (page < pages) {
      html += '<button class="btn btn-secondary btn-sm stats-page-btn" data-page="' + (page + 1) + '">&raquo;</button>';
    }
    html += '</div>';

    // Per-page selector
    html += '<div class="stats-per-page">'
      + t('stats.per_page') + ': ';
    var perPageOptions = _state.source === 'file' ? [100, 200, 500, 1000, 2000] : [10, 20, 50, 100];
    for (var j = 0; j < perPageOptions.length; j++) {
      var pp = perPageOptions[j];
      var ppCls = pp === limit ? 'stats-pp-opt active' : 'stats-pp-opt';
      html += '<span class="' + ppCls + '" data-limit="' + pp + '">' + pp + '</span>';
    }
    html += '</div>';

    el.innerHTML = html;
  }

  // ─── 工具函数 ───

  function _fmtTime(ts) {
    var d = new Date(ts || 0);
    return String(d.getHours()).padStart(2, '0') + ':'
      + String(d.getMinutes()).padStart(2, '0') + ':'
      + String(d.getSeconds()).padStart(2, '0');
  }

  function _fmtDateTime(ts) {
    var d = new Date(ts || 0);
    return d.getFullYear()
      + '-' + String(d.getMonth() + 1).padStart(2, '0')
      + '-' + String(d.getDate()).padStart(2, '0')
      + ' ' + String(d.getHours()).padStart(2, '0')
      + ':' + String(d.getMinutes()).padStart(2, '0')
      + ':' + String(d.getSeconds()).padStart(2, '0');
  }

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

  function _fmtCacheWithRate(cachedTokens, inputTokens) {
    var cached = Number(cachedTokens) || 0;
    var rate = _getCacheHitRatePercent(cached, inputTokens);
    if (cached <= 0) return '-';
    if (rate == null) return _fmtNum(cached);
    return _fmtNum(cached) + ' (' + rate + '%)';
  }

  function _fmtCacheHitRateOnly(cachedTokens, inputTokens) {
    var rate = _getCacheHitRatePercent(cachedTokens, inputTokens);
    if (rate == null) return '-';
    return rate + '%';
  }

  function _cacheRateClass(rate) {
    if (rate == null) return '';
    if (rate > 50) return 'cache-hit-high';
    if (rate < 10) return 'cache-hit-low';
    return '';
  }

  function _fmtDuration(ms) {
    if (!ms || ms <= 0) return '-';
    if (ms < 1000) return ms + 'ms';
    var secs = (ms / 1000).toFixed(1);
    return secs + 's';
  }

  function _fmtModel(model) {
    if (!model) return '-';
    if (model.length > 22) return escapeHtml(model.substring(0, 20)) + '..';
    return escapeHtml(model);
  }

  function _fmtAccount(email) {
    if (!email) return '-';
    var idx = _state.accountMap[email];
    if (idx !== undefined && idx !== null) return '#' + String(idx).padStart(2, '0');
    // fallback: 显示 email 前缀
    var at = email.indexOf('@');
    if (at > 0) return escapeHtml(email.substring(0, Math.min(at, 4))) + '..';
    return escapeHtml(email.substring(0, 6));
  }

  window.addEventListener('daterange-change', function () {
    var activePage = document.querySelector('.page.active');
    if (activePage && activePage.id === 'pageStatistics') {
      _state.page = 1;
      _state.expandedIdx = -1;
      _loadOverview();
      _loadCallerStats();
      _loadData();
    }
  });

  return {
    init: init,
    cleanup: cleanup,
  };
})();
