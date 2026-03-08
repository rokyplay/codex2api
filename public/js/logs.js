/**
 * codex2api 管理面板 — 请求日志模块
 * 日志列表、筛选、统计、清空、自动刷新
 */

var Logs = (function () {
  'use strict';

  var _state = {
    logs: [],
    total: 0,
    currentLevel: 'all',
    searchKeyword: '',
    offset: 0,
    limit: 100,
    maxLogs: 500,
    autoRefresh: false,
    autoRefreshTimer: null,
    stats: { total: 0, info: 0, warn: 0, error: 0, request: 0 },
  };

  /**
   * 级别配置
   */
  var _levelConfig = {
    info:    { icon: 'i',  cls: 'log-info',    labelKey: 'logs.filter_info'    },
    warn:    { icon: '!',  cls: 'log-warn',    labelKey: 'logs.filter_warn'    },
    error:   { icon: '✕', cls: 'log-error',   labelKey: 'logs.filter_error'   },
    request: { icon: '→', cls: 'log-request', labelKey: 'logs.filter_request' },
  };

  /**
   * 加载日志
   */
  function loadLogs(append) {
    if (!append) {
      _state.offset = 0;
    }

    var params = new URLSearchParams();
    if (_state.currentLevel && _state.currentLevel !== 'all') {
      params.set('level', _state.currentLevel);
    }
    if (_state.searchKeyword) {
      params.set('search', _state.searchKeyword);
    }
    params.set('limit', String(_state.limit));
    params.set('offset', String(_state.offset));

    api('GET', '/logs?' + params.toString())
      .then(function (data) {
        {
          var logs = data.logs || [];
          var total = data.total || 0;

          if (append) {
            _state.logs = _state.logs.concat(logs);
          } else {
            _state.logs = logs;
          }

          // 限制内存
          if (_state.logs.length > _state.maxLogs) {
            _state.logs = _state.logs.slice(-_state.maxLogs);
          }

          _state.total = total;
          renderLogs();
        }
      })
      .catch(function (err) {
        toast(t('logs.load_failed') + ': ' + err.message, 'error');
      });
  }

  /**
   * 加载统计
   */
  function loadStats() {
    api('GET', '/logs/stats')
      .then(function (data) {
        _state.stats = data || _state.stats;
        renderStats();
      })
      .catch(function () {
        // silent
      });
  }

  /**
   * 渲染日志统计卡片
   */
  function renderStats() {
    var container = document.getElementById('logStats');
    if (!container) return;

    var items = [
      { key: 'all',     label: t('logs.filter_all'),     value: _state.stats.total || 0, cls: '' },
      { key: 'info',    label: t('logs.filter_info'),    value: _state.stats.info || 0,  cls: 'stat-info' },
      { key: 'warn',    label: t('logs.filter_warn'),    value: _state.stats.warn || 0,  cls: 'stat-warn' },
      { key: 'error',   label: t('logs.filter_error'),   value: _state.stats.error || 0, cls: 'stat-error' },
      { key: 'request', label: t('logs.filter_request'), value: _state.stats.request || 0, cls: 'stat-request' },
    ];

    var html = '';
    for (var i = 0; i < items.length; i++) {
      var item = items[i];
      var activeClass = (_state.currentLevel === item.key) ? ' active' : '';
      html +=
        '<div class="log-stat-item ' + item.cls + activeClass + '" data-level="' + item.key + '">' +
          '<span class="log-stat-num">' + item.value + '</span>' +
          '<span class="log-stat-label">' + escapeHtml(item.label) + '</span>' +
        '</div>';
    }
    container.innerHTML = html;
  }

  /**
   * 渲染日志列表
   */
  function renderLogs() {
    var container = document.getElementById('logList');
    if (!container) return;

    if (_state.logs.length === 0) {
      container.innerHTML =
        '<div class="empty-state">' +
          '<div class="empty-icon"><svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" opacity="0.5"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg></div>' +
          '<span>' + escapeHtml(t('logs.no_logs')) + '</span>' +
        '</div>';
      _updateLoadMoreBtn();
      return;
    }

    // 按时间正序显示（旧的在上，新的在下）
    var sorted = _state.logs.slice().reverse();

    var html = '';
    for (var i = 0; i < sorted.length; i++) {
      var log = sorted[i];
      var cfg = _levelConfig[log.level] || { icon: '-', cls: 'log-info', labelKey: '' };
      var time = formatDateTime(log.timestamp);

      var message = escapeHtml(log.message);
      if (_state.searchKeyword) {
        var regex = new RegExp('(' + escapeRegExp(_state.searchKeyword) + ')', 'gi');
        message = message.replace(regex, '<mark>$1</mark>');
      }

      html +=
        '<div class="log-item ' + cfg.cls + '" data-log-index="' + i + '">' +
          '<div class="log-item-header">' +
            '<span class="log-level-badge ' + cfg.cls + '">' + log.level.toUpperCase() + '</span>' +
            '<span class="log-time">' + escapeHtml(time) + '</span>' +
            '<button class="log-copy-btn" data-log-index="' + i + '" title="' + escapeHtml(t('logs.copy')) + '">&#x2398;</button>' +
          '</div>' +
          '<div class="log-message">' + message + '</div>' +
        '</div>';
    }

    container.innerHTML = html;

    // 滚动到底部
    container.scrollTop = container.scrollHeight;

    _updateLoadMoreBtn();
  }

  function _updateLoadMoreBtn() {
    var btn = document.getElementById('loadMoreLogsBtn');
    if (!btn) return;
    var hasMore = _state.logs.length < _state.total;
    btn.style.display = hasMore ? 'block' : 'none';
    if (hasMore) {
      btn.textContent = t('logs.load_more') + ' (' + _state.logs.length + '/' + _state.total + ')';
    }
  }

  /**
   * 筛选日志级别
   */
  function filterLevel(level) {
    _state.currentLevel = level || 'all';
    _state.offset = 0;
    renderStats();
    loadLogs();
  }

  /**
   * 搜索日志
   */
  function search(keyword) {
    _state.searchKeyword = keyword || '';
    _state.offset = 0;
    loadLogs();
  }

  /**
   * 加载更多
   */
  function loadMore() {
    _state.offset += _state.limit;
    loadLogs(true);
  }

  /**
   * 切换自动刷新
   */
  function toggleAutoRefresh() {
    _state.autoRefresh = !_state.autoRefresh;
    var btn = document.getElementById('autoRefreshBtn');

    if (_state.autoRefresh) {
      if (btn) {
        btn.classList.add('active');
        btn.textContent = t('logs.auto_refresh_stop');
      }
      _state.autoRefreshTimer = setInterval(function () {
        loadLogs();
        loadStats();
      }, 3000);
    } else {
      if (btn) {
        btn.classList.remove('active');
        btn.textContent = t('logs.auto_refresh');
      }
      if (_state.autoRefreshTimer) {
        clearInterval(_state.autoRefreshTimer);
        _state.autoRefreshTimer = null;
      }
    }
  }

  /**
   * 清空日志
   */
  function clearLogs() {
    showConfirm(t('logs.clear_confirm'), t('logs.clear_title'))
      .then(function (confirmed) {
        if (!confirmed) return;

        api('DELETE', '/logs')
          .then(function () {
            toast(t('logs.clear_success'), 'success');
            _state.logs = [];
            _state.total = 0;
            _state.stats = { total: 0, info: 0, warn: 0, error: 0, request: 0 };
            renderLogs();
            renderStats();
          })
          .catch(function (err) {
            toast(t('logs.clear_failed') + ': ' + err.message, 'error');
          });
      });
  }

  /**
   * 复制日志内容
   */
  function copyLog(index) {
    var sorted = _state.logs.slice().reverse();
    var log = sorted[index];
    if (!log) return;

    navigator.clipboard.writeText(log.message)
      .then(function () {
        toast(t('logs.copied'), 'success');
      })
      .catch(function () {
        toast(t('logs.copy_failed'), 'error');
      });
  }

  /**
   * 初始化日志页（进入页面时调用）
   */
  function init() {
    loadLogs();
    loadStats();
  }

  /**
   * 清理日志页（离开页面时调用）
   */
  function cleanup() {
    if (_state.autoRefreshTimer) {
      clearInterval(_state.autoRefreshTimer);
      _state.autoRefreshTimer = null;
    }
    _state.autoRefresh = false;
    _state.logs = [];
    _state.total = 0;
    _state.offset = 0;
  }

  return {
    init: init,
    cleanup: cleanup,
    loadLogs: loadLogs,
    loadStats: loadStats,
    renderLogs: renderLogs,
    renderStats: renderStats,
    filterLevel: filterLevel,
    search: search,
    loadMore: loadMore,
    toggleAutoRefresh: toggleAutoRefresh,
    clearLogs: clearLogs,
    copyLog: copyLog,
  };
})();
