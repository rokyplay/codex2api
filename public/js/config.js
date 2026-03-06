/**
 * codex2api 管理面板 — 配置管理模块
 * 表单式编辑、分区展示、保存
 */

var Config = (function () {
  'use strict';

  var _configData = null;
  var _proxyData = null;
  var _apiKeysData = [];
  var _modelsData = null;
  var _totpStatus = { enabled: false, configured: false };

  /**
   * 配置字段定义 — 按分区分组
   * type: text / number / password / select / toggle
   */
  var _sections = [
    {
      key: 'server',
      titleKey: 'config.section_server',
      fields: [
        { name: 'host', labelKey: 'config.host', tipKey: 'config.host_tip', type: 'text', path: 'server.host' },
        { name: 'port', labelKey: 'config.port', tipKey: 'config.port_tip', type: 'number', path: 'server.port' },
        { name: 'password', labelKey: 'config.api_password', tipKey: 'config.api_password_tip', type: 'password', path: 'server.password' },
        { name: 'admin_username', labelKey: 'config.admin_username', tipKey: 'config.admin_username_tip', type: 'text', path: 'server.admin_username' },
        { name: 'admin_password', labelKey: 'config.admin_password', tipKey: 'config.admin_password_tip', type: 'password', path: 'server.admin_password' },
      ],
    },
    {
      key: 'upstream',
      titleKey: 'config.section_upstream',
      fields: [
        { name: 'base_url', labelKey: 'config.base_url', tipKey: 'config.base_url_tip', type: 'text', path: 'upstream.base_url' },
        { name: 'timeout', labelKey: 'config.timeout', tipKey: 'config.timeout_tip', type: 'number', path: 'upstream.timeout' },
        { name: 'stream_timeout', labelKey: 'config.stream_timeout', tipKey: 'config.stream_timeout_tip', type: 'number', path: 'upstream.stream_timeout' },
      ],
    },
    {
      key: 'scheduler',
      titleKey: 'config.section_scheduler',
      fields: [
        {
          name: 'scheduler_mode', labelKey: 'config.scheduler_mode', tipKey: 'config.scheduler_mode_tip',
          type: 'select', path: 'scheduler.mode',
          options: [
            { value: 'round_robin', labelKey: 'config.scheduler_round_robin' },
            { value: 'random', labelKey: 'config.scheduler_random' },
            { value: 'least_used', labelKey: 'config.scheduler_least_used' },
          ],
        },
      ],
    },
    {
      key: 'retry',
      titleKey: 'config.section_retry',
      fields: [
        { name: 'max_retries', labelKey: 'config.max_retries', tipKey: 'config.max_retries_tip', type: 'number', path: 'retry.max_retries' },
        { name: 'backoff_ms', labelKey: 'config.backoff_ms', tipKey: 'config.backoff_ms_tip', type: 'number', path: 'retry.backoff_ms' },
      ],
    },
    {
      key: 'rate_limit',
      titleKey: 'config.section_rate_limit',
      fields: [
        { name: 'rpm', labelKey: 'config.rpm', tipKey: 'config.rpm_tip', type: 'number', path: 'rate_limit.requests_per_minute' },
        { name: 'cooldown_ms', labelKey: 'config.cooldown_ms', tipKey: 'config.cooldown_ms_tip', type: 'number', path: 'rate_limit.cooldown_ms' },
      ],
    },
    {
      key: 'health_check',
      titleKey: 'config.section_health_check',
      fields: [
        { name: 'health_enabled', labelKey: 'config.health_enabled', tipKey: 'config.health_enabled_tip', type: 'toggle', path: 'health_check.enabled' },
        { name: 'health_interval', labelKey: 'config.health_interval', tipKey: 'config.health_interval_tip', type: 'number', path: 'health_check.interval_minutes' },
      ],
    },
    {
      key: 'credentials',
      titleKey: 'config.section_credentials',
      fields: [
        { name: 'auto_refresh', labelKey: 'config.auto_refresh', tipKey: 'config.auto_refresh_tip', type: 'toggle', path: 'credentials.auto_refresh' },
        { name: 'refresh_before', labelKey: 'config.refresh_before', tipKey: 'config.refresh_before_tip', type: 'number', path: 'credentials.refresh_before_expiry_seconds' },
        { name: 'api_token', labelKey: 'config.api_token', tipKey: 'config.api_token_tip', type: 'password', path: 'credentials.api_token' },
      ],
    },
  ];

  /**
   * 从嵌套对象按 dot path 取值
   */
  function _getByPath(obj, path) {
    if (!obj || !path) return undefined;
    var parts = path.split('.');
    var current = obj;
    for (var i = 0; i < parts.length; i++) {
      if (current === null || current === undefined) return undefined;
      current = current[parts[i]];
    }
    return current;
  }

  /**
   * 向嵌套对象按 dot path 设值
   */
  function _setByPath(obj, path, value) {
    var parts = path.split('.');
    var current = obj;
    for (var i = 0; i < parts.length - 1; i++) {
      if (current[parts[i]] === undefined || current[parts[i]] === null) {
        current[parts[i]] = {};
      }
      current = current[parts[i]];
    }
    current[parts[parts.length - 1]] = value;
  }

  /**
   * 加载配置
   */
  function load() {
    api('GET', '/config')
      .then(function (data) {
        _configData = data;
        render(data);
      })
      .catch(function (err) {
        toast(t('config.load_failed') + ': ' + err.message, 'error');
      });
  }

  /**
   * 渲染配置表单
   */
  function render(data) {
    var container = document.getElementById('configContent');
    if (!container) return;

    var html = '<form id="configForm" class="config-form">';

    for (var s = 0; s < _sections.length; s++) {
      var section = _sections[s];
      html +=
        '<div class="config-section">' +
          '<h3 class="config-section-title">' + escapeHtml(t(section.titleKey)) + '</h3>' +
          '<div class="config-fields">';

      for (var f = 0; f < section.fields.length; f++) {
        var field = section.fields[f];
        var value = _getByPath(data, field.path);
        var label = t(field.labelKey);
        var tip = t(field.tipKey);

        html += '<div class="config-field">';
        html += '<label class="config-label">';
        html += escapeHtml(label);
        if (tip && tip !== field.tipKey) {
          html += ' <span class="config-tip" title="' + escapeHtml(tip) + '">?</span>';
        }
        html += '</label>';

        if (field.type === 'toggle') {
          var checked = !!value;
          html +=
            '<label class="toggle-switch">' +
              '<input type="checkbox" name="' + escapeHtml(field.name) + '" data-path="' + escapeHtml(field.path) + '"' + (checked ? ' checked' : '') + '>' +
              '<span class="toggle-slider"></span>' +
            '</label>';
        } else if (field.type === 'select') {
          html += '<select class="input config-input" name="' + escapeHtml(field.name) + '" data-path="' + escapeHtml(field.path) + '">';
          for (var o = 0; o < field.options.length; o++) {
            var opt = field.options[o];
            var selected = (String(value) === String(opt.value)) ? ' selected' : '';
            var optLabel = opt.labelKey ? t(opt.labelKey) : (opt.label || opt.value);
            html += '<option value="' + escapeHtml(opt.value) + '"' + selected + '>' + escapeHtml(optLabel) + '</option>';
          }
          html += '</select>';
        } else {
          var inputType = field.type || 'text';
          var displayVal = (value !== undefined && value !== null) ? String(value) : '';
          // 敏感字段显示掩码值
          if (inputType === 'password' && displayVal && displayVal.indexOf('***') >= 0) {
            displayVal = '';
          }
          html +=
            '<input class="input config-input" type="' + inputType + '" name="' + escapeHtml(field.name) + '" ' +
              'data-path="' + escapeHtml(field.path) + '" ' +
              'value="' + escapeHtml(displayVal) + '">';
        }

        html += '</div>';
      }

      html += '</div></div>';
    }

    html +=
      '<div class="config-actions">' +
        '<button type="button" class="btn btn-secondary" id="btnConfigReload">' + escapeHtml(t('config.reload')) + '</button>' +
        '<button type="submit" class="btn btn-primary">' + escapeHtml(t('config.save')) + '</button>' +
      '</div>';

    html += '</form>';
    html += _renderModelsManagerSection();
    html += _renderProxySection();
    html += _renderApiKeysSection();
    html += _renderTotpSection();

    container.innerHTML = html;

    // 绑定事件
    var form = document.getElementById('configForm');
    if (form) {
      form.addEventListener('submit', function (e) {
        e.preventDefault();
        save();
      });
    }

    var reloadBtn = document.getElementById('btnConfigReload');
    if (reloadBtn) {
      reloadBtn.addEventListener('click', function () {
        load();
        toast(t('accounts.refreshed'), 'info');
      });
    }

    _bindProxyEvents();
    _loadProxyData();
    _bindApiKeyEvents();
    _loadApiKeys();
    _bindTotpEvents();
    _loadTotpStatus();
    _bindModelsEvents();
    _loadModelsConfig();
  }

  /**
   * 渲染代理设置区块（独立于配置表单）
   */
  function _renderProxySection() {
    var html = '';
    html += '<div class="proxy-panels" id="proxySection">';
    html += _renderProxyPanel('local', 'config.proxy_local_title');
    html += _renderProxyPanel('register', 'config.register_proxy_title');
    html += '</div>';
    return html;
  }

  function _renderProxyPanel(idPrefix, titleKey) {
    var html = '';
    html += '<div class="config-section proxy-section" id="' + escapeHtml(idPrefix) + 'ProxySection">';
    html += '<div class="proxy-section-header">';
    html += '<h3 class="config-section-title">' + escapeHtml(t(titleKey)) + '</h3>';
    html += '<label class="toggle-switch">';
    html += '<input type="checkbox" id="' + escapeHtml(idPrefix) + 'EnabledToggle">';
    html += '<span class="toggle-slider"></span>';
    html += '</label>';
    html += '</div>';

    html += '<div class="proxy-section-body" id="' + escapeHtml(idPrefix) + 'ProxyBody" style="display:none">';

    html += '<div class="proxy-field">';
    html += '<label class="config-label" for="' + escapeHtml(idPrefix) + 'PresetSelect">' + escapeHtml(t('config.proxy_preset')) + '</label>';
    html += '<select class="input proxy-preset-select" id="' + escapeHtml(idPrefix) + 'PresetSelect"></select>';
    html += '</div>';

    html += '<div class="proxy-field">';
    html += '<span class="config-label">' + escapeHtml(t('config.proxy_node_select')) + '</span>';
    html += '<div class="proxy-node-groups" id="' + escapeHtml(idPrefix) + 'NodeGroups"></div>';
    html += '</div>';

    html += '<div class="proxy-field">';
    html += '<span class="config-label">' + escapeHtml(t('config.proxy_current')) + '</span>';
    html += '<div class="proxy-current" id="' + escapeHtml(idPrefix) + 'CurrentText"></div>';
    html += '</div>';

    html += '<div class="proxy-field">';
    html += '<button type="button" class="btn btn-secondary proxy-test-btn" id="' + escapeHtml(idPrefix) + 'TestBtn">' + escapeHtml(t('config.proxy_test')) + '</button>';
    html += '<div class="proxy-test-result" id="' + escapeHtml(idPrefix) + 'TestResult"></div>';
    html += '</div>';

    html += '</div>';
    html += '</div>';
    return html;
  }

  function _renderApiKeysSection() {
    var html = '';
    html += '<div class="config-section api-keys-section" id="apiKeysSection">';
    html += '<h3 class="config-section-title">' + escapeHtml(t('config.api_keys_title')) + '</h3>';
    html += '<p class="config-desc">' + escapeHtml(t('config.api_keys_desc')) + '</p>';
    html += '<div class="api-keys-toolbar">';
    html += '<input class="input config-input" id="apiKeyIdInput" placeholder="' + escapeHtml(t('config.api_key_id')) + '">';
    html += '<input class="input config-input" id="apiKeyIdentityInput" placeholder="' + escapeHtml(t('config.api_key_identity')) + '">';
    html += '<label class="api-keys-enable-wrap"><input type="checkbox" id="apiKeyEnabledInput" checked> ' + escapeHtml(t('config.api_key_enabled')) + '</label>';
    html += '<button type="button" class="btn btn-primary" id="btnApiKeyCreate">' + escapeHtml(t('config.api_keys_add')) + '</button>';
    html += '<button type="button" class="btn btn-secondary" id="btnApiKeyRefresh">' + escapeHtml(t('config.api_keys_refresh')) + '</button>';
    html += '</div>';
    html += '<div class="api-key-plaintext hidden" id="apiKeyPlaintext"></div>';
    html += '<div class="table-wrap api-keys-table-wrap">';
    html += '<table class="table api-keys-table">';
    html += '<thead><tr>';
    html += '<th>' + escapeHtml(t('config.api_key_id')) + '</th>';
    html += '<th>' + escapeHtml(t('config.api_key_identity')) + '</th>';
    html += '<th>' + escapeHtml(t('config.api_key_key')) + '</th>';
    html += '<th>' + escapeHtml(t('config.api_key_enabled')) + '</th>';
    html += '<th>' + escapeHtml(t('config.api_key_created_at')) + '</th>';
    html += '<th>' + escapeHtml(t('config.api_key_actions')) + '</th>';
    html += '</tr></thead>';
    html += '<tbody id="apiKeysTableBody"><tr><td colspan="6">' + escapeHtml(t('common.loading')) + '</td></tr></tbody>';
    html += '</table>';
    html += '</div>';
    html += '</div>';
    return html;
  }

  function _apiKeyTodayYYYYMMDD() {
    var now = new Date();
    return String(now.getFullYear())
      + String(now.getMonth() + 1).padStart(2, '0')
      + String(now.getDate()).padStart(2, '0');
  }

  function _sanitizeApiKeyIdBase(value) {
    var s = String(value || '').trim();
    if (!s) return '';
    s = s.replace(/\s+/g, '_').replace(/[^A-Za-z0-9._:-]/g, '_');
    s = s.replace(/_+/g, '_').replace(/^_+|_+$/g, '');
    return s;
  }

  function _buildAutoApiKeyId(identity) {
    var datePart = _apiKeyTodayYYYYMMDD();
    var base = _sanitizeApiKeyIdBase(identity) || 'key';
    var maxBaseLen = Math.max(1, 64 - 1 - datePart.length);
    if (base.length > maxBaseLen) base = base.substring(0, maxBaseLen);
    return base + '_' + datePart;
  }

  function _bindApiKeyEvents() {
    var createBtn = document.getElementById('btnApiKeyCreate');
    if (createBtn) {
      createBtn.addEventListener('click', function () {
        var idEl = document.getElementById('apiKeyIdInput');
        var identityEl = document.getElementById('apiKeyIdentityInput');
        var enabledEl = document.getElementById('apiKeyEnabledInput');
        var id = idEl ? String(idEl.value || '').trim() : '';
        var identity = identityEl ? String(identityEl.value || '').trim() : '';
        var enabled = enabledEl ? !!enabledEl.checked : true;

        if (!identity && id) {
          identity = id;
        }

        if (!id) {
          id = _buildAutoApiKeyId(identity);
        }

        if (!id && !identity) {
          toast(t('config.api_key_create_failed') + ': ' + t('config.api_key_id'), 'error');
          return;
        }

        if (!identity) {
          identity = id;
        }

        api('POST', '/api-keys', {
          id: id,
          identity: identity,
          enabled: enabled,
        }).then(function (resp) {
          toast(t('config.api_key_created'), 'success');
          _showApiKeyPlaintext(resp && resp.api_key ? resp.api_key.key : '');
          if (idEl) idEl.value = '';
          if (identityEl) identityEl.value = '';
          _loadApiKeys();
        }).catch(function (err) {
          toast(t('config.api_key_create_failed') + ': ' + err.message, 'error');
        });
      });
    }

    var refreshBtn = document.getElementById('btnApiKeyRefresh');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', function () {
        _loadApiKeys();
      });
    }

    var tableBody = document.getElementById('apiKeysTableBody');
    if (tableBody) {
      tableBody.addEventListener('click', function (e) {
        var btn = e.target.closest('button[data-act]');
        if (!btn) return;
        var act = btn.getAttribute('data-act');
        var id = btn.getAttribute('data-id') || '';
        if (!id) return;

        if (act === 'rotate') {
          api('POST', '/api-keys/' + encodeURIComponent(id) + '/rotate', {})
            .then(function (resp) {
              toast(t('config.api_key_rotated'), 'success');
              _showApiKeyPlaintext(resp && resp.api_key ? resp.api_key.key : '');
              _loadApiKeys();
            })
            .catch(function (err) {
              toast(t('config.api_key_rotate_failed') + ': ' + err.message, 'error');
            });
          return;
        }

        if (act === 'delete') {
          if (!window.confirm(t('config.api_key_delete') + ' #' + id + '?')) return;
          api('DELETE', '/api-keys/' + encodeURIComponent(id))
            .then(function () {
              toast(t('config.api_key_deleted'), 'success');
              _loadApiKeys();
            })
            .catch(function (err) {
              toast(t('config.api_key_delete_failed') + ': ' + err.message, 'error');
            });
          return;
        }

        if (act === 'toggle') {
          var enabled = btn.getAttribute('data-enabled') === 'true';
          api('PUT', '/api-keys/' + encodeURIComponent(id), { enabled: !enabled })
            .then(function () {
              _loadApiKeys();
            })
            .catch(function (err) {
              toast(t('config.api_key_update_failed') + ': ' + err.message, 'error');
            });
        }
      });
    }
  }

  function _showApiKeyPlaintext(key) {
    var el = document.getElementById('apiKeyPlaintext');
    if (!el) return;
    var val = String(key || '');
    if (!val) {
      el.classList.add('hidden');
      el.textContent = '';
      return;
    }
    el.classList.remove('hidden');
    el.textContent = t('config.api_key_plaintext_tip') + ': ' + val;
  }

  function _loadApiKeys() {
    api('GET', '/api-keys')
      .then(function (resp) {
        _apiKeysData = (resp && Array.isArray(resp.api_keys)) ? resp.api_keys : [];
        _renderApiKeysTable();
      })
      .catch(function (err) {
        _apiKeysData = [];
        _renderApiKeysTable();
        toast(t('config.load_failed') + ': ' + err.message, 'error');
      });
  }

  function _shortApiKey(key) {
    var val = String(key || '');
    if (!val) return '';
    if (val.length <= 16) return val;
    return val.slice(0, 8) + '...' + val.slice(-4);
  }

  function _renderApiKeysTable() {
    var tbody = document.getElementById('apiKeysTableBody');
    if (!tbody) return;

    if (!_apiKeysData || _apiKeysData.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6">' + escapeHtml(t('config.api_keys_empty')) + '</td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < _apiKeysData.length; i++) {
      var item = _apiKeysData[i] || {};
      var id = item.id ? String(item.id) : '';
      var identity = item.identity ? String(item.identity) : '';
      var key = item.key ? String(item.key) : '';
      var keyDisplay = _shortApiKey(key);
      var enabled = item.enabled !== false;
      var createdAt = item.created_at ? formatDateTime(item.created_at) : '-';
      html += '<tr>';
      html += '<td class="td-mono api-key-id-cell" title="' + escapeHtml(id) + '">' + escapeHtml(id) + '</td>';
      html += '<td class="api-key-identity-cell" title="' + escapeHtml(identity) + '">' + escapeHtml(identity) + '</td>';
      html += '<td class="td-mono api-key-key-cell" title="' + escapeHtml(key) + '"><code>' + escapeHtml(keyDisplay) + '</code></td>';
      html += '<td>' + (enabled ? 'ON' : 'OFF') + '</td>';
      html += '<td>' + escapeHtml(createdAt) + '</td>';
      html += '<td class="api-key-actions-cell">';
      html += '<div class="api-key-actions">';
      html += '<button type="button" class="btn btn-secondary btn-sm api-key-action-btn" data-act="toggle" data-id="' + escapeHtml(id) + '" data-enabled="' + (enabled ? 'true' : 'false') + '" title="' + escapeHtml(enabled ? t('config.api_key_disable') : t('config.api_key_enable')) + '">' + escapeHtml(enabled ? t('config.api_key_disable') : t('config.api_key_enable')) + '</button>';
      html += '<button type="button" class="btn btn-secondary btn-sm api-key-action-btn" data-act="rotate" data-id="' + escapeHtml(id) + '" title="' + escapeHtml(t('config.api_key_rotate')) + '">' + escapeHtml(t('config.api_key_rotate')) + '</button>';
      html += '<button type="button" class="btn btn-danger btn-sm api-key-action-btn" data-act="delete" data-id="' + escapeHtml(id) + '" title="' + escapeHtml(t('config.api_key_delete')) + '">' + escapeHtml(t('config.api_key_delete')) + '</button>';
      html += '</div>';
      html += '</td>';
      html += '</tr>';
    }
    tbody.innerHTML = html;
  }

  function _renderTotpSection() {
    var html = '';
    html += '<div class="config-section totp-section" id="totpSection">';
    html += '<div class="totp-header">';
    html += '<h3 class="config-section-title">' + escapeHtml(t('config.totp_section')) + '</h3>';
    html += '<button type="button" class="btn btn-secondary btn-sm" id="btnTotpStatusRefresh">' + escapeHtml(t('config.totp_refresh_status')) + '</button>';
    html += '</div>';

    html += '<div class="totp-status-row">';
    html += '<span class="config-label">' + escapeHtml(t('config.totp_status_label')) + '</span>';
    html += '<span class="totp-status-badge" id="totpStatusBadge">' + escapeHtml(t('common.loading')) + '</span>';
    html += '<span class="totp-status-extra" id="totpConfiguredText"></span>';
    html += '</div>';

    html += '<div class="totp-grid">';

    html += '<div class="totp-card">';
    html += '<h4 class="totp-card-title">' + escapeHtml(t('config.totp_enable_title')) + '</h4>';
    html += '<p class="totp-card-desc">' + escapeHtml(t('config.totp_enable_desc')) + '</p>';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="totpInitPassword">' + escapeHtml(t('common.admin_password')) + '</label>';
    html += '<input class="input config-input" type="password" id="totpInitPassword" autocomplete="off" placeholder="' + escapeHtml(t('common.enter_admin_password')) + '">';
    html += '</div>';
    html += '<button type="button" class="btn btn-primary" id="btnTotpSetupInit">' + escapeHtml(t('config.totp_setup_init')) + '</button>';

    html += '<div class="totp-setup-block hidden" id="totpSetupBlock">';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="totpSecretValue">' + escapeHtml(t('config.totp_secret')) + '</label>';
    html += '<input class="input config-input totp-mono" type="text" id="totpSecretValue" readonly>';
    html += '</div>';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="totpUriValue">' + escapeHtml(t('config.totp_uri')) + '</label>';
    html += '<textarea class="input config-input totp-uri" id="totpUriValue" readonly rows="3"></textarea>';
    html += '</div>';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="totpSetupCode">' + escapeHtml(t('config.totp_code')) + '</label>';
    html += '<input class="input config-input" type="text" inputmode="numeric" maxlength="6" id="totpSetupCode" placeholder="' + escapeHtml(t('config.totp_code_placeholder')) + '" autocomplete="one-time-code">';
    html += '</div>';
    html += '<button type="button" class="btn btn-success" id="btnTotpSetupConfirm">' + escapeHtml(t('config.totp_setup_confirm')) + '</button>';
    html += '</div>';
    html += '</div>';

    html += '<div class="totp-card">';
    html += '<h4 class="totp-card-title">' + escapeHtml(t('config.totp_disable_title')) + '</h4>';
    html += '<p class="totp-card-desc">' + escapeHtml(t('config.totp_disable_desc')) + '</p>';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="totpDisablePassword">' + escapeHtml(t('common.admin_password')) + '</label>';
    html += '<input class="input config-input" type="password" id="totpDisablePassword" autocomplete="off" placeholder="' + escapeHtml(t('common.enter_admin_password')) + '">';
    html += '</div>';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="totpDisableCode">' + escapeHtml(t('config.totp_code')) + '</label>';
    html += '<input class="input config-input" type="text" inputmode="numeric" maxlength="6" id="totpDisableCode" placeholder="' + escapeHtml(t('config.totp_code_placeholder')) + '" autocomplete="one-time-code">';
    html += '</div>';
    html += '<button type="button" class="btn btn-danger" id="btnTotpDisable">' + escapeHtml(t('config.totp_disable')) + '</button>';
    html += '</div>';

    html += '</div>';
    html += '</div>';
    return html;
  }

  function _createDefaultModelsData() {
    return {
      prefix: '',
      default: '',
      available: [],
      aliases: [],
    };
  }

  function _normalizeModelsData(raw) {
    var data = _createDefaultModelsData();
    var source = (raw && typeof raw === 'object') ? raw : {};

    data.prefix = source.prefix ? String(source.prefix) : '';
    data.default = source.default ? String(source.default) : '';

    var availableInput = source.available;
    if (!Array.isArray(availableInput) && availableInput && typeof availableInput === 'object') {
      availableInput = Object.keys(availableInput).map(function (name) {
        var item = availableInput[name] || {};
        return {
          name: name,
          display_name: item.display_name,
          enabled: item.enabled,
        };
      });
    }
    if (!Array.isArray(availableInput)) availableInput = [];

    var seenModels = {};
    for (var i = 0; i < availableInput.length; i++) {
      var modelItem = availableInput[i] || {};
      var name = String(modelItem.name || '').trim();
      if (!name || seenModels[name]) continue;
      seenModels[name] = true;
      data.available.push({
        name: name,
        display_name: String(modelItem.display_name || '').trim(),
        enabled: modelItem.enabled !== false,
      });
    }

    var aliasesInput = source.aliases;
    if (!Array.isArray(aliasesInput) && aliasesInput && typeof aliasesInput === 'object') {
      aliasesInput = Object.keys(aliasesInput).map(function (alias) {
        return {
          alias: alias,
          target: aliasesInput[alias],
        };
      });
    }
    if (!Array.isArray(aliasesInput)) aliasesInput = [];

    var seenAliases = {};
    for (var j = 0; j < aliasesInput.length; j++) {
      var aliasItem = aliasesInput[j] || {};
      var alias = String(aliasItem.alias || '').trim();
      var target = String(aliasItem.target || '').trim();
      if (!alias || seenAliases[alias]) continue;
      seenAliases[alias] = true;
      data.aliases.push({
        alias: alias,
        target: target,
      });
    }

    if (!data.default && data.available.length > 0) {
      data.default = data.available[0].name;
    }

    return data;
  }

  function _renderModelsManagerSection() {
    var html = '';
    html += '<div class="config-section model-manager-section" id="modelsManagerSection">';
    html += '<div class="model-manager-header">';
    html += '<h3 class="config-section-title">' + escapeHtml(t('config.models_section')) + '</h3>';
    html += '<div class="model-manager-actions">';
    html += '<button type="button" class="btn btn-secondary btn-sm" id="btnModelsReload">' + escapeHtml(t('config.models_reload')) + '</button>';
    html += '<button type="button" class="btn btn-secondary btn-sm" id="btnModelsRefreshUpstream">' + escapeHtml(t('config.models_discovery_refresh')) + '</button>';
    html += '<button type="button" class="btn btn-primary btn-sm" id="btnModelsSave">' + escapeHtml(t('config.models_save')) + '</button>';
    html += '</div>';
    html += '</div>';
    html += '<p class="config-desc">' + escapeHtml(t('config.models_desc')) + '</p>';

    html += '<div class="config-fields model-manager-top-fields">';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="modelsPrefixInput">' + escapeHtml(t('config.models_prefix')) + '</label>';
    html += '<input class="input config-input" type="text" id="modelsPrefixInput">';
    html += '</div>';
    html += '<div class="config-field">';
    html += '<label class="config-label" for="modelsDefaultInput">' + escapeHtml(t('config.models_default')) + '</label>';
    html += '<input class="input config-input" type="text" id="modelsDefaultInput">';
    html += '</div>';
    html += '</div>';

    html += '<div class="model-manager-subsection">';
    html += '<div class="model-manager-subhead">';
    html += '<span class="config-label">' + escapeHtml(t('config.models_available_title')) + '</span>';
    html += '<button type="button" class="btn btn-secondary btn-sm" id="btnModelAdd">' + escapeHtml(t('config.models_add')) + '</button>';
    html += '</div>';
    html += '<div class="api-keys-table-wrap">';
    html += '<table class="api-keys-table model-manager-table">';
    html += '<thead><tr>';
    html += '<th>' + escapeHtml(t('config.models_col_name')) + '</th>';
    html += '<th>' + escapeHtml(t('config.models_col_display')) + '</th>';
    html += '<th>' + escapeHtml(t('config.models_col_enabled')) + '</th>';
    html += '<th>' + escapeHtml(t('config.models_col_actions')) + '</th>';
    html += '</tr></thead>';
    html += '<tbody id="modelsTableBody"><tr><td colspan="4">' + escapeHtml(t('common.loading')) + '</td></tr></tbody>';
    html += '</table>';
    html += '</div>';
    html += '</div>';

    html += '<div class="model-manager-subsection">';
    html += '<div class="model-manager-subhead">';
    html += '<span class="config-label">' + escapeHtml(t('config.models_aliases_title')) + '</span>';
    html += '<button type="button" class="btn btn-secondary btn-sm" id="btnModelAliasAdd">' + escapeHtml(t('config.models_alias_add')) + '</button>';
    html += '</div>';
    html += '<div class="api-keys-table-wrap">';
    html += '<table class="api-keys-table model-manager-table">';
    html += '<thead><tr>';
    html += '<th>' + escapeHtml(t('config.models_col_alias')) + '</th>';
    html += '<th>' + escapeHtml(t('config.models_col_target')) + '</th>';
    html += '<th>' + escapeHtml(t('config.models_col_actions')) + '</th>';
    html += '</tr></thead>';
    html += '<tbody id="modelAliasesTableBody"><tr><td colspan="3">' + escapeHtml(t('common.loading')) + '</td></tr></tbody>';
    html += '</table>';
    html += '</div>';
    html += '</div>';

    html += '</div>';
    return html;
  }

  function _renderModelsTables() {
    if (!_modelsData) _modelsData = _createDefaultModelsData();

    var prefixEl = document.getElementById('modelsPrefixInput');
    var defaultEl = document.getElementById('modelsDefaultInput');
    if (prefixEl) prefixEl.value = String(_modelsData.prefix || '');
    if (defaultEl) defaultEl.value = String(_modelsData.default || '');

    var modelsBody = document.getElementById('modelsTableBody');
    if (modelsBody) {
      if (!_modelsData.available.length) {
        modelsBody.innerHTML = '<tr><td colspan="4">' + escapeHtml(t('config.models_empty')) + '</td></tr>';
      } else {
        var rowsHtml = '';
        for (var i = 0; i < _modelsData.available.length; i++) {
          var model = _modelsData.available[i] || {};
          rowsHtml += '<tr data-model-row="' + i + '">';
          rowsHtml += '<td><input class="input config-input model-name-input td-mono" type="text" value="' + escapeHtml(model.name || '') + '"></td>';
          rowsHtml += '<td><input class="input config-input model-display-input" type="text" value="' + escapeHtml(model.display_name || '') + '"></td>';
          rowsHtml += '<td><label class="toggle-switch"><input type="checkbox" class="model-enabled-input"' + (model.enabled !== false ? ' checked' : '') + '><span class="toggle-slider"></span></label></td>';
          rowsHtml += '<td><button type="button" class="btn btn-danger btn-sm" data-model-action="delete" data-model-index="' + i + '">' + escapeHtml(t('config.api_key_delete')) + '</button></td>';
          rowsHtml += '</tr>';
        }
        modelsBody.innerHTML = rowsHtml;
      }
    }

    var aliasesBody = document.getElementById('modelAliasesTableBody');
    if (aliasesBody) {
      if (!_modelsData.aliases.length) {
        aliasesBody.innerHTML = '<tr><td colspan="3">' + escapeHtml(t('config.models_alias_empty')) + '</td></tr>';
      } else {
        var aliasHtml = '';
        for (var j = 0; j < _modelsData.aliases.length; j++) {
          var aliasItem = _modelsData.aliases[j] || {};
          aliasHtml += '<tr data-alias-row="' + j + '">';
          aliasHtml += '<td><input class="input config-input alias-name-input td-mono" type="text" value="' + escapeHtml(aliasItem.alias || '') + '"></td>';
          aliasHtml += '<td><input class="input config-input alias-target-input td-mono" type="text" value="' + escapeHtml(aliasItem.target || '') + '"></td>';
          aliasHtml += '<td><button type="button" class="btn btn-danger btn-sm" data-alias-action="delete" data-alias-index="' + j + '">' + escapeHtml(t('config.api_key_delete')) + '</button></td>';
          aliasHtml += '</tr>';
        }
        aliasesBody.innerHTML = aliasHtml;
      }
    }
  }

  function _collectModelsPayloadFromDom(strictValidate) {
    var payload = {
      prefix: '',
      default: '',
      available: [],
      aliases: [],
    };

    var prefixEl = document.getElementById('modelsPrefixInput');
    var defaultEl = document.getElementById('modelsDefaultInput');
    payload.prefix = prefixEl ? String(prefixEl.value || '').trim() : '';
    payload.default = defaultEl ? String(defaultEl.value || '').trim() : '';

    var modelRows = document.querySelectorAll('#modelsTableBody tr[data-model-row]');
    for (var i = 0; i < modelRows.length; i++) {
      var row = modelRows[i];
      var nameEl = row.querySelector('.model-name-input');
      var displayEl = row.querySelector('.model-display-input');
      var enabledEl = row.querySelector('.model-enabled-input');
      var name = nameEl ? String(nameEl.value || '').trim() : '';
      var displayName = displayEl ? String(displayEl.value || '').trim() : '';
      if (!name) continue;
      payload.available.push({
        name: name,
        display_name: displayName,
        enabled: enabledEl ? !!enabledEl.checked : true,
      });
    }

    var aliasRows = document.querySelectorAll('#modelAliasesTableBody tr[data-alias-row]');
    for (var j = 0; j < aliasRows.length; j++) {
      var aliasRow = aliasRows[j];
      var aliasEl = aliasRow.querySelector('.alias-name-input');
      var targetEl = aliasRow.querySelector('.alias-target-input');
      var alias = aliasEl ? String(aliasEl.value || '').trim() : '';
      var target = targetEl ? String(targetEl.value || '').trim() : '';
      if (!alias && !target) continue;
      if (!alias) {
        if (!strictValidate) continue;
        throw new Error(t('config.models_missing_name'));
      }
      if (!target) {
        if (!strictValidate) continue;
        throw new Error(t('config.models_missing_target'));
      }
      payload.aliases.push({
        alias: alias,
        target: target,
      });
    }

    return payload;
  }

  function _loadModelsConfig() {
    api('GET', '/models/config')
      .then(function (resp) {
        _modelsData = _normalizeModelsData(resp && resp.models ? resp.models : {});
        _renderModelsTables();
      })
      .catch(function (err) {
        _modelsData = _createDefaultModelsData();
        _renderModelsTables();
        toast(t('config.models_load_failed') + ': ' + err.message, 'error');
      });
  }

  function _saveModelsConfig() {
    var payload;
    try {
      payload = _collectModelsPayloadFromDom(true);
    } catch (err) {
      toast(err.message || t('config.models_save_failed'), 'error');
      return;
    }

    if (!payload.available.length) {
      toast(t('config.models_empty'), 'error');
      return;
    }

    showLoading(t('config.models_saving'));
    api('PUT', '/models/config', payload)
      .then(function (resp) {
        hideLoading();
        _modelsData = _normalizeModelsData(resp && resp.models ? resp.models : payload);
        _renderModelsTables();
        toast(t('config.models_save_success'), 'success');
      })
      .catch(function (err) {
        hideLoading();
        toast(t('config.models_save_failed') + ': ' + err.message, 'error');
      });
  }

  function _refreshModelsFromUpstream() {
    showLoading(t('config.models_discovery_refreshing'));
    api('POST', '/models/refresh', {})
      .then(function (resp) {
        hideLoading();
        if (!resp || resp.success !== true) {
          var errText = (resp && resp.error) ? String(resp.error) : t('config.models_discovery_refresh_failed');
          toast(t('config.models_discovery_refresh_failed') + ': ' + errText, 'error');
          return;
        }
        _loadModelsConfig();
        var modelCount = (resp.discovery && Array.isArray(resp.discovery.models))
          ? resp.discovery.models.length
          : 0;
        toast(t('config.models_discovery_refresh_success') + ' (' + modelCount + ')', 'success');
      })
      .catch(function (err) {
        hideLoading();
        toast(t('config.models_discovery_refresh_failed') + ': ' + err.message, 'error');
      });
  }

  function _bindModelsEvents() {
    var saveBtn = document.getElementById('btnModelsSave');
    if (saveBtn) {
      saveBtn.addEventListener('click', function () {
        _saveModelsConfig();
      });
    }

    var reloadBtn = document.getElementById('btnModelsReload');
    if (reloadBtn) {
      reloadBtn.addEventListener('click', function () {
        _loadModelsConfig();
      });
    }

    var refreshUpstreamBtn = document.getElementById('btnModelsRefreshUpstream');
    if (refreshUpstreamBtn) {
      refreshUpstreamBtn.addEventListener('click', function () {
        _refreshModelsFromUpstream();
      });
    }

    var addModelBtn = document.getElementById('btnModelAdd');
    if (addModelBtn) {
      addModelBtn.addEventListener('click', function () {
        var next = _collectModelsPayloadFromDom(false);
        next.available.push({
          name: '',
          display_name: '',
          enabled: true,
        });
        _modelsData = _normalizeModelsData(next);
        _renderModelsTables();
      });
    }

    var addAliasBtn = document.getElementById('btnModelAliasAdd');
    if (addAliasBtn) {
      addAliasBtn.addEventListener('click', function () {
        var next = _collectModelsPayloadFromDom(false);
        next.aliases.push({
          alias: '',
          target: '',
        });
        _modelsData = _normalizeModelsData(next);
        _renderModelsTables();
      });
    }

    var modelsBody = document.getElementById('modelsTableBody');
    if (modelsBody) {
      modelsBody.addEventListener('click', function (e) {
        var btn = e.target.closest('button[data-model-action="delete"]');
        if (!btn) return;
        var index = Number(btn.getAttribute('data-model-index'));
        if (!Number.isFinite(index) || index < 0) return;
        var next = _collectModelsPayloadFromDom(false);
        next.available.splice(index, 1);
        _modelsData = _normalizeModelsData(next);
        _renderModelsTables();
      });
    }

    var aliasBody = document.getElementById('modelAliasesTableBody');
    if (aliasBody) {
      aliasBody.addEventListener('click', function (e) {
        var btn = e.target.closest('button[data-alias-action="delete"]');
        if (!btn) return;
        var index = Number(btn.getAttribute('data-alias-index'));
        if (!Number.isFinite(index) || index < 0) return;
        var next = _collectModelsPayloadFromDom(false);
        next.aliases.splice(index, 1);
        _modelsData = _normalizeModelsData(next);
        _renderModelsTables();
      });
    }
  }

  /**
   * 绑定代理区块交互
   */
  function _bindProxyEvents() {
    _bindProxyPanelEvents('local', false);
    _bindProxyPanelEvents('register', true);
  }

  function _bindProxyPanelEvents(prefix, isRegister) {
    var enabledEl = document.getElementById(prefix + 'EnabledToggle');
    var presetEl = document.getElementById(prefix + 'PresetSelect');
    var nodeGroupsEl = document.getElementById(prefix + 'NodeGroups');
    var testBtn = document.getElementById(prefix + 'TestBtn');
    var bodyEl = document.getElementById(prefix + 'ProxyBody');

    if (enabledEl) {
      enabledEl.addEventListener('change', function () {
        var targetEnabled = !!enabledEl.checked;
        if (bodyEl) {
          bodyEl.style.display = targetEnabled ? 'block' : 'none';
        }
        _applyProxySelection(_buildProxyPayload(isRegister, { enabled: targetEnabled }))
          .catch(function () {
            enabledEl.checked = !targetEnabled;
            if (bodyEl) {
              bodyEl.style.display = enabledEl.checked ? 'block' : 'none';
            }
            _updateProxyDom();
          });
      });
    }

    if (presetEl) {
      presetEl.addEventListener('change', function () {
        var preset = presetEl.value;
        if (!preset) {
          _updateProxyDom();
          return;
        }
        _applyProxySelection(_buildProxyPayload(isRegister, { preset: preset }))
          .catch(function () {
            _updateProxyDom();
          });
      });
    }

    if (nodeGroupsEl) {
      nodeGroupsEl.addEventListener('click', function (e) {
        var chip = e.target.closest('.proxy-chip');
        if (chip) {
          var port = Number(chip.getAttribute('data-port'));
          if (port > 0) {
            _applyProxySelection(_buildProxyPayload(isRegister, { port: port }))
              .catch(function () {
                _updateProxyDom();
              });
          }
          return;
        }

        var header = e.target.closest('.proxy-node-group-header');
        if (header) {
          var group = header.parentNode;
          if (group && group.classList) {
            group.classList.toggle('expanded');
          }
        }
      });
    }

    if (testBtn) {
      testBtn.addEventListener('click', function () {
        var currentEl = document.getElementById(prefix + 'CurrentText');
        var server = currentEl ? String(currentEl.textContent || '').trim() : '';
        var port = currentEl ? Number(currentEl.getAttribute('data-port') || 0) : 0;
        if (!port) port = _extractProxyPort(server);
        if (!port) {
          _setProxyTestResult(prefix, false, t('config.proxy_test_fail'));
          return;
        }
        api('GET', '/proxy/test?port=' + encodeURIComponent(String(port)))
          .then(function (res) {
            if (res && res.success) {
              var successText = t('config.proxy_test_success', { ip: (res.ip || '') });
              if (res.ip && successText.indexOf(res.ip) < 0) successText += ': ' + res.ip;
              _setProxyTestResult(prefix, true, successText);
            } else {
              var failText = t('config.proxy_test_fail', { error: (res && res.error) ? res.error : '' });
              if (res && res.error) failText += ': ' + res.error;
              _setProxyTestResult(prefix, false, failText);
            }
          })
          .catch(function (err) {
            var msg = t('config.proxy_test_fail', { error: (err && err.message) ? err.message : '' });
            if (err && err.message) msg += ': ' + err.message;
            _setProxyTestResult(prefix, false, msg);
          });
      });
    }
  }

  function _buildProxyPayload(isRegister, payload) {
    if (isRegister) {
      return { register_proxy: payload };
    }
    return payload;
  }

  /**
   * 加载代理数据
   */
  function _loadProxyData() {
    api('GET', '/proxy/presets')
      .then(function (data) {
        _proxyData = data || {};
        _updateProxyDom();
      })
      .catch(function (err) {
        if (err && err.message) {
          toast(err.message, 'error');
        }
      });
  }

  /**
   * 更新代理区块 DOM
   */
  function _updateProxyDom() {
    if (!_proxyData) return;

    _updateProxyPanelDom('local', {
      enabled: !!_proxyData.enabled,
      server: _getCurrentProxyServer(_proxyData),
      active_preset: _proxyData.active_preset ? String(_proxyData.active_preset) : '',
    });
    _updateProxyPanelDom('register', _getRegisterProxy(_proxyData));
  }

  function _updateProxyPanelDom(prefix, data) {
    var enabledEl = document.getElementById(prefix + 'EnabledToggle');
    var bodyEl = document.getElementById(prefix + 'ProxyBody');
    var presetEl = document.getElementById(prefix + 'PresetSelect');
    var nodeGroupsEl = document.getElementById(prefix + 'NodeGroups');
    var currentEl = document.getElementById(prefix + 'CurrentText');
    if (!enabledEl || !bodyEl || !presetEl || !nodeGroupsEl || !currentEl) return;

    var panelData = data || {};
    var server = panelData.server ? String(panelData.server) : '';
    var activePreset = panelData.active_preset ? String(panelData.active_preset) : '';
    var activePort = Number(panelData.local_port || 0) || _extractProxyPort(server);

    enabledEl.checked = !!panelData.enabled;
    bodyEl.style.display = panelData.enabled ? 'block' : 'none';

    presetEl.innerHTML = _renderProxyPresetOptions(_proxyData);
    var hasPresetOption = false;
    for (var i = 0; i < presetEl.options.length; i++) {
      if (presetEl.options[i].value === activePreset) {
        hasPresetOption = true;
        break;
      }
    }
    presetEl.value = hasPresetOption ? activePreset : '';

    nodeGroupsEl.innerHTML = _renderProxyNodeGroups((_proxyData && _proxyData.node_groups) ? _proxyData.node_groups : [], activePort);
    currentEl.textContent = server || '-';
    if (activePort > 0) {
      currentEl.setAttribute('data-port', String(activePort));
    } else {
      currentEl.removeAttribute('data-port');
    }
  }

  /**
   * 渲染预设下拉选项
   */
  function _renderProxyPresetOptions(data) {
    var html = '';
    var presets = (data && data.presets) ? data.presets : {};

    for (var key in presets) {
      if (!Object.prototype.hasOwnProperty.call(presets, key)) continue;
      if (key === 'custom') continue;
      var preset = presets[key] || {};
      var label = preset.label || key;
      html += '<option value="' + escapeHtml(key) + '">' + escapeHtml(label) + '</option>';
    }

    return html;
  }

  /**
   * 渲染节点分组（默认折叠）
   */
  function _renderProxyNodeGroups(groups, activePort) {
    if (!groups || !groups.length) return '';

    var html = '';
    for (var i = 0; i < groups.length; i++) {
      var group = groups[i] || {};
      var nodes = Array.isArray(group.nodes) ? group.nodes : [];
      if (!nodes.length) continue;

      var count = nodes.length;
      var title = String(group.label || '') + ' (' + count + ')';
      html += '<div class="proxy-node-group">';
      html += '<button type="button" class="proxy-node-group-header">';
      html += '<span>' + escapeHtml(title) + '</span>';
      html += '<span class="proxy-node-group-arrow">▾</span>';
      html += '</button>';
      html += '<div class="proxy-node-chips">';

      for (var n = 0; n < nodes.length; n++) {
        var node = nodes[n] || {};
        var port = Number(node.port);
        if (!port) continue;
        var nodeName = node.name ? String(node.name) : String(port);
        var activeClass = (port === activePort) ? ' active' : '';
        html += '<button type="button" class="proxy-chip' + activeClass + '" data-port="' + port + '">' + escapeHtml(nodeName) + '</button>';
      }

      html += '</div></div>';
    }
    return html;
  }

  /**
   * 提交代理选择更新
   */
  function _applyProxySelection(payload) {
    return api('PUT', '/proxy/select', payload)
      .then(function (res) {
        if (!_proxyData) _proxyData = {};
        if (res && res.proxy) {
          for (var key in res.proxy) {
            if (Object.prototype.hasOwnProperty.call(res.proxy, key)) {
              _proxyData[key] = res.proxy[key];
            }
          }
          if (res.proxy.server) {
            _proxyData.current_server = res.proxy.server;
          }
        }
        _updateProxyDom();
        var hasLocalPayload = payload && (payload.preset !== undefined || payload.port !== undefined || payload.server !== undefined || payload.enabled !== undefined);
        var hasRegisterPayload = payload && payload.register_proxy !== undefined;
        var toastKey = (hasRegisterPayload && !hasLocalPayload) ? 'config.register_proxy_updated' : 'config.proxy_updated';
        toast(t(toastKey), 'success');
        return res;
      })
      .catch(function (err) {
        if (err && err.message) {
          toast(err.message, 'error');
        }
        throw err;
      });
  }

  /**
   * 解析当前代理地址
   */
  function _getCurrentProxyServer(data) {
    if (!data) return '';
    if (data.current_server) return String(data.current_server);
    if (data.server) return String(data.server);
    if (data.proxy && data.proxy.server) return String(data.proxy.server);
    return '';
  }

  function _getRegisterProxy(data) {
    var fallback = {
      enabled: false,
      server: 'socks5://YOUR_PROXY_HOST:YOUR_PROXY_PORT',
      active_preset: '',
      target: '',
      local_port: 0,
    };
    if (!data || !data.register_proxy || typeof data.register_proxy !== 'object') return fallback;
    return {
      enabled: !!data.register_proxy.enabled,
      server: data.register_proxy.server ? String(data.register_proxy.server) : fallback.server,
      active_preset: data.register_proxy.active_preset ? String(data.register_proxy.active_preset) : '',
      target: data.register_proxy.target ? String(data.register_proxy.target) : '',
      local_port: Number(data.register_proxy.local_port || 0),
    };
  }

  /**
   * 从代理 URL 解析端口
   */
  function _extractProxyPort(server) {
    if (!server) return 0;
    try {
      var parsed = new URL(server);
      if (parsed.port) return Number(parsed.port);
    } catch (e) {}
    var match = String(server).match(/:(\d+)(?:\/)?$/);
    return match ? Number(match[1]) : 0;
  }

  /**
   * 展示连通性测试结果
   */
  function _setProxyTestResult(prefix, success, text) {
    var resultEl = document.getElementById(prefix + 'TestResult');
    if (!resultEl) return;
    resultEl.textContent = text || '';
    resultEl.className = 'proxy-test-result' + (text ? (success ? ' success' : ' fail') : '');
  }

  function _bindTotpEvents() {
    var refreshBtn = document.getElementById('btnTotpStatusRefresh');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', function () {
        _loadTotpStatus();
      });
    }

    var initBtn = document.getElementById('btnTotpSetupInit');
    if (initBtn) {
      initBtn.addEventListener('click', function () {
        _initTotpSetup();
      });
    }

    var confirmBtn = document.getElementById('btnTotpSetupConfirm');
    if (confirmBtn) {
      confirmBtn.addEventListener('click', function () {
        _confirmTotpSetup();
      });
    }

    var disableBtn = document.getElementById('btnTotpDisable');
    if (disableBtn) {
      disableBtn.addEventListener('click', function () {
        _disableTotp();
      });
    }
  }

  function _updateTotpStatusDom(status) {
    _totpStatus = {
      enabled: !!(status && status.enabled),
      configured: !!(status && status.configured),
    };

    var badge = document.getElementById('totpStatusBadge');
    var configuredText = document.getElementById('totpConfiguredText');
    if (badge) {
      var enabled = _totpStatus.enabled;
      badge.textContent = enabled ? t('common.enabled') : t('common.disabled');
      badge.className = 'totp-status-badge ' + (enabled ? 'enabled' : 'disabled');
    }
    if (configuredText) {
      configuredText.textContent = _totpStatus.configured
        ? t('config.totp_configured')
        : t('config.totp_not_configured');
      configuredText.className = 'totp-status-extra ' + (_totpStatus.configured ? 'ok' : 'warn');
    }
  }

  function _loadTotpStatus() {
    api('GET', '/totp/status')
      .then(function (status) {
        _updateTotpStatusDom(status);
      })
      .catch(function (err) {
        toast(t('config.totp_status_load_failed') + ': ' + err.message, 'error');
      });
  }

  function _toggleTotpSetupBlock(visible) {
    var block = document.getElementById('totpSetupBlock');
    if (!block) return;
    block.classList.toggle('hidden', !visible);
  }

  function _initTotpSetup() {
    var passwordInput = document.getElementById('totpInitPassword');
    var password = passwordInput ? String(passwordInput.value || '') : '';
    if (!password) {
      toast(t('common.password_required'), 'error');
      return;
    }

    showLoading(t('config.totp_setup_init_loading'));
    api('POST', '/totp/setup/init', {
      admin_password: password,
    })
      .then(function (data) {
        hideLoading();
        var secretInput = document.getElementById('totpSecretValue');
        var uriInput = document.getElementById('totpUriValue');
        var codeInput = document.getElementById('totpSetupCode');
        if (secretInput) secretInput.value = data.secret_base32 || '';
        if (uriInput) uriInput.value = data.otpauth_uri || '';
        if (codeInput) codeInput.value = '';
        _toggleTotpSetupBlock(true);
        if (codeInput) codeInput.focus();
        toast(t('config.totp_setup_init_success'), 'success');
      })
      .catch(function (err) {
        hideLoading();
        toast(t('config.totp_setup_init_failed') + ': ' + err.message, 'error');
      });
  }

  function _confirmTotpSetup() {
    var codeInput = document.getElementById('totpSetupCode');
    var code = codeInput ? String(codeInput.value || '').trim() : '';
    if (!code) {
      toast(t('config.totp_code_required'), 'error');
      return;
    }

    showLoading(t('config.totp_setup_confirm_loading'));
    api('POST', '/totp/setup/confirm', { code: code })
      .then(function () {
        hideLoading();
        _toggleTotpSetupBlock(false);
        var passwordInput = document.getElementById('totpInitPassword');
        if (passwordInput) passwordInput.value = '';
        if (codeInput) codeInput.value = '';
        _loadTotpStatus();
        toast(t('config.totp_setup_confirm_success'), 'success');
      })
      .catch(function (err) {
        hideLoading();
        toast(t('config.totp_setup_confirm_failed') + ': ' + err.message, 'error');
      });
  }

  function _disableTotp() {
    var passwordInput = document.getElementById('totpDisablePassword');
    var codeInput = document.getElementById('totpDisableCode');
    var password = passwordInput ? String(passwordInput.value || '') : '';
    var code = codeInput ? String(codeInput.value || '').trim() : '';

    if (!password) {
      toast(t('common.password_required'), 'error');
      return;
    }
    if (!code) {
      toast(t('config.totp_code_required'), 'error');
      return;
    }

    showLoading(t('config.totp_disable_loading'));
    api('POST', '/totp/disable', {
      admin_password: password,
      totp_code: code,
    })
      .then(function () {
        hideLoading();
        if (passwordInput) passwordInput.value = '';
        if (codeInput) codeInput.value = '';
        _toggleTotpSetupBlock(false);
        _loadTotpStatus();
        toast(t('config.totp_disable_success'), 'success');
      })
      .catch(function (err) {
        hideLoading();
        toast(t('config.totp_disable_failed') + ': ' + err.message, 'error');
      });
  }

  // 敏感配置路径 — 修改这些需要管理员密码确认
  var _sensitiveFields = [
    'server.password',
    'server.admin_password',
    'server.admin_username',
    'credentials.api_token',
  ];

  /**
   * 判断 payload 是否涉及敏感字段变更
   */
  function _hasSensitiveChanges(payload) {
    for (var i = 0; i < _sensitiveFields.length; i++) {
      var val = _getByPath(payload, _sensitiveFields[i]);
      if (val !== undefined) return true;
    }
    return false;
  }

  /**
   * 收集表单数据
   */
  function _collectPayload() {
    var payload = {};
    for (var s = 0; s < _sections.length; s++) {
      var section = _sections[s];
      for (var f = 0; f < section.fields.length; f++) {
        var field = section.fields[f];
        var el = document.querySelector('[data-path="' + field.path + '"]');
        if (!el) continue;

        var val;
        if (field.type === 'toggle') {
          val = el.checked;
        } else if (field.type === 'number') {
          val = el.value === '' ? undefined : Number(el.value);
        } else if (field.type === 'password') {
          val = el.value || undefined;
        } else {
          val = el.value;
        }

        if (val !== undefined) {
          _setByPath(payload, field.path, val);
        }
      }
    }
    return payload;
  }

  /**
   * 实际发送保存请求
   */
  function _doSave(payload) {
    showLoading(t('config.saving'));
    api('PUT', '/config', payload)
      .then(function () {
        hideLoading();
        toast(t('config.save_success'), 'success');
        load();
      })
      .catch(function (err) {
        hideLoading();
        toast(t('config.save_failed') + ': ' + err.message, 'error');
      });
  }

  /**
   * 保存配置 — 敏感字段变更需密码确认
   */
  function save() {
    var payload = _collectPayload();

    if (_hasSensitiveChanges(payload)) {
      showPasswordConfirm(t('config.sensitive_confirm'), t('config.sensitive_title'))
        .then(function (result) {
          if (result.confirmed && result.password) {
            payload._admin_password = result.password;
            _doSave(payload);
          } else if (result.confirmed && !result.password) {
            toast(t('common.password_required'), 'error');
          }
        });
    } else {
      _doSave(payload);
    }
  }

  return {
    load: load,
    render: render,
    save: save,
  };
})();
