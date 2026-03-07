/**
 * codex2api 管理面板 — 账号管理模块
 * 列表、搜索、状态筛选、操作、导入导出
 */

var Accounts = (function () {
  'use strict';

  var _accounts = [];
  var _lifespanStats = null;
  var _currentFilter = 'all';
  var _searchTerm = '';
  var _gpaBusy = false;

  /**
   * 状态徽章 class
   */
  function _statusBadgeClass(status) {
    var map = {
      active: 'badge-active',
      cooldown: 'badge-cooldown',
      banned: 'badge-banned',
      expired: 'badge-expired',
      wasted: 'badge-wasted',
      relogin_needed: 'badge-relogin',
    };
    return map[status] || 'badge-expired';
  }

  /**
   * 错误类型中文
   */
  function _errorLabel(type) {
    if (!type) return '-';
    var key = 'error.' + type;
    var label = t(key);
    return label === key ? type : label;
  }

  function _formatDuration(ms) {
    var sec = Math.max(0, Math.ceil((ms || 0) / 1000));
    if (sec <= 0) return '0s';
    var h = Math.floor(sec / 3600);
    var m = Math.floor((sec % 3600) / 60);
    var s = sec % 60;
    if (h > 0) return h + 'h ' + String(m).padStart(2, '0') + 'm';
    if (m > 0) return m + 'm ' + String(s).padStart(2, '0') + 's';
    return s + 's';
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

  function _renderLifespanSummary() {
    var box = document.getElementById('accountsLifespanSummary');
    if (!box) return;
    var data = _lifespanStats;
    if (!data || typeof data !== 'object') {
      box.innerHTML = '';
      return;
    }
    var byStatus = data.by_status_avg_hours || {};
    var badges = [
      { key: 'accounts.avg_lifespan', value: _formatLifespan(data.avg_lifespan_hours), cls: 'stats-badge-info' },
      { key: 'accounts.median_lifespan', value: _formatLifespan(data.median_lifespan_hours), cls: 'stats-badge-accent' },
      { key: 'accounts.alive_age', value: _formatLifespan(data.avg_alive_hours), cls: 'stats-badge-success' },
      { key: 'accounts.dead_count', value: String(data.total_dead || 0), cls: 'stats-badge-warning' },
      { key: 'accounts.alive_count', value: String(data.total_alive || 0), cls: 'stats-badge-cyan' },
      { key: 'accounts.lifespan_banned', value: _formatLifespan(byStatus.banned), cls: 'stats-badge-warning' },
      { key: 'accounts.lifespan_wasted', value: _formatLifespan(byStatus.wasted), cls: 'stats-badge-info' },
      { key: 'accounts.lifespan_expired', value: _formatLifespan(byStatus.expired), cls: 'stats-badge-accent' },
    ];
    var html = '';
    for (var i = 0; i < badges.length; i++) {
      var b = badges[i];
      html += '<span class="stats-badge ' + b.cls + '">'
        + escapeHtml(t(b.key)) + ': '
        + '<strong>' + escapeHtml(String(b.value)) + '</strong>'
        + '</span>';
    }
    box.innerHTML = html;
  }

  /**
   * 加载账号列表
   */
  function load() {
    api('GET', '/accounts')
      .then(function (data) {
        _accounts = data.accounts || data || [];
        _lifespanStats = data.lifespan || null;
        render();
      })
      .catch(function (err) {
        toast(t('accounts.load_failed') + ': ' + err.message, 'error');
      });
  }

  /**
   * 设置筛选状态
   */
  function setFilter(filter) {
    _currentFilter = filter || 'all';

    // 更新筛选按钮状态
    var filterBtns = document.querySelectorAll('.filter-btn');
    for (var i = 0; i < filterBtns.length; i++) {
      var btn = filterBtns[i];
      if (btn.getAttribute('data-filter') === _currentFilter) {
        btn.classList.add('active');
      } else {
        btn.classList.remove('active');
      }
    }

    render();
  }

  /**
   * 设置搜索关键词
   */
  function setSearch(term) {
    _searchTerm = (term || '').toLowerCase();
    render();
  }

  /**
   * 渲染账号列表
   */
  function render() {
    var accountsBody = document.getElementById('accountsBody');
    if (!accountsBody) return;
    _renderLifespanSummary();

    // 统计各状态数量并更新筛选按钮
    var counts = { all: _accounts.length, active: 0, cooldown: 0, banned: 0, expired: 0, wasted: 0 };
    for (var ci = 0; ci < _accounts.length; ci++) {
      var s = _accounts[ci].status;
      // relogin_needed 归入过期，usage_limited 归入冷却
      var mapped = s === 'relogin_needed' ? 'expired' : s === 'usage_limited' ? 'cooldown' : s;
      if (counts[mapped] !== undefined) counts[mapped]++;
    }
    var filterBtns = document.querySelectorAll('.filter-btn');
    for (var fi = 0; fi < filterBtns.length; fi++) {
      var btn = filterBtns[fi];
      var key = btn.getAttribute('data-filter');
      var label = btn.getAttribute('data-i18n') ? t(btn.getAttribute('data-i18n')) : btn.textContent;
      // 去掉已有的数量后缀
      label = label.replace(/\s*\(\d+\)$/, '');
      var count = counts[key];
      if (count !== undefined) {
        btn.textContent = label + ' (' + count + ')';
      }
    }

    var filtered = _accounts;

    // 状态筛选
    if (_currentFilter !== 'all') {
      filtered = filtered.filter(function (a) {
        var s = a.status;
        if (_currentFilter === 'expired') return s === 'expired' || s === 'relogin_needed';
        if (_currentFilter === 'cooldown') return s === 'cooldown' || s === 'usage_limited';
        return s === _currentFilter;
      });
    }

    // 搜索
    if (_searchTerm) {
      filtered = filtered.filter(function (a) {
        return (a.email || '').toLowerCase().indexOf(_searchTerm) >= 0;
      });
    }

    if (filtered.length === 0) {
      accountsBody.innerHTML =
        '<tr><td colspan="7">' +
          '<div class="empty-state">' +
            '<div class="empty-icon"><svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" opacity="0.5"><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2"/></svg></div>' +
            '<span>' + escapeHtml(_searchTerm ? t('accounts.no_match') : t('accounts.no_accounts')) + '</span>' +
          '</div>' +
        '</td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < filtered.length; i++) {
      var a = filtered[i];
      var statusKey = 'status.' + a.status;
      var statusText = t(statusKey);
      if (statusText === statusKey) statusText = a.status;

      // 根据状态决定操作按钮
      var actionsHtml = '';
      // 所有状态都有删除按钮
      var deleteBtn = '<button class="btn btn-sm btn-danger" data-action="delete" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_delete')) + '</button>';

      var checkBtn = '<button class="btn btn-sm btn-info" data-action="check" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_check')) + '</button>';
      if (a.status === 'active') {
        actionsHtml =
          '<button class="btn btn-sm btn-primary" data-action="test" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_test')) + '</button>' +
          '<button class="btn btn-sm btn-success" data-action="refresh" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_refresh_token')) + '</button>' +
          '<button class="btn btn-sm btn-secondary" data-action="cooldown" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_cooldown')) + '</button>' +
          '<button class="btn btn-sm btn-danger" data-action="waste" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_waste')) + '</button>' +
          deleteBtn;
      } else if (a.status === 'wasted' || a.status === 'expired') {
        actionsHtml =
          '<button class="btn btn-sm btn-warning" data-action="verify" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_verify')) + '</button>' +
          '<button class="btn btn-sm btn-success" data-action="activate" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_activate')) + '</button>' +
          deleteBtn;
      } else if (a.status === 'cooldown') {
        actionsHtml =
          '<button class="btn btn-sm btn-success" data-action="refresh" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_refresh_token')) + '</button>' +
          '<button class="btn btn-sm btn-danger" data-action="waste" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_waste')) + '</button>' +
          deleteBtn;
      } else if (a.status === 'banned') {
        actionsHtml =
          '<button class="btn btn-sm btn-danger" data-action="waste" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_waste')) + '</button>' +
          deleteBtn;
      } else if (a.status === 'relogin_needed') {
        actionsHtml =
          '<button class="btn btn-sm btn-warning" data-action="verify" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_verify')) + '</button>' +
          '<button class="btn btn-sm btn-danger" data-action="waste" data-email="' + escapeHtml(a.email) + '">' + escapeHtml(t('accounts.btn_waste')) + '</button>' +
          deleteBtn;
      }

      var errorText = _errorLabel(a.last_error_type);
      if (a.status === 'cooldown' && a.cooldown_until) {
        var leftMs = a.cooldown_until - Date.now();
        if (leftMs > 0) {
          errorText += ' · ' + _formatDuration(leftMs);
        }
      }
      if (a.last_error_code) {
        errorText += ' #' + a.last_error_code;
      }

      html +=
        '<tr>' +
          '<td class="td-email" title="' + escapeHtml(displayEmail(a.email)) + '">' + escapeHtml(displayEmail(a.email)) + '</td>' +
          '<td><span class="badge ' + _statusBadgeClass(a.status) + '">' + escapeHtml(statusText) + '</span></td>' +
          '<td class="td-mono">' + (a.request_count || 0) + '</td>' +
          '<td class="td-mono td-time">' + escapeHtml(formatTime(a.token_expires_at)) + '</td>' +
          '<td class="td-mono">' + (a.consecutive_errors || 0) + '</td>' +
          '<td class="td-error">' + escapeHtml(errorText) + '</td>' +
          '<td class="td-actions">' + actionsHtml + '</td>' +
        '</tr>';
    }
    accountsBody.innerHTML = html;
  }

  /**
   * 账号操作（刷新token、冷却、废弃、激活）— 事件委托处理
   */
  function handleAction(email, action) {

    if (action === 'check') { _doCheckSingle(email); return; }
    if (action === 'test') { _doTestAccount(email); return; }    if (action === 'delete') {
      showPasswordConfirm(t('accounts.confirm_delete'), t('accounts.btn_delete'))
        .then(function (result) {
          if (result.confirmed && result.password) {
            _doDelete(email, result.password);
          } else if (result.confirmed && !result.password) {
            toast(t('common.password_required'), 'error');
          }
        });
      return;
    }
    if (action === 'waste') {
      showConfirm(t('accounts.confirm_waste'), t('accounts.btn_waste'))
        .then(function (confirmed) {
          if (confirmed) {
            _doAction(email, action);
          }
        });
      return;
    }
    _doAction(email, action);
  }

  function _doDelete(email, password) {
    api('DELETE', '/accounts/' + encodeURIComponent(email), { admin_password: password })
      .then(function () {
        toast(t('accounts.delete_success') + ': ' + email, 'success');
        load();
      })
      .catch(function (err) {
        toast(t('accounts.delete_failed') + ': ' + err.message, 'error');
      });
  }

  function _doAction(email, action) {
    api('POST', '/accounts/' + encodeURIComponent(email) + '/action', { action: action })
      .then(function () {
        toast(t('accounts.action_success') + ': ' + email, 'success');
        load();
      })
      .catch(function (err) {
        toast(t('accounts.action_failed') + ': ' + err.message, 'error');
      });
  }

  /**
   * 导入账号
   */
  function importAccounts(jsonStr) {
    var data;
    try {
      data = JSON.parse(jsonStr);
    } catch (e) {
      toast(t('accounts.import_json_error') + ': ' + e.message, 'error');
      return;
    }

    var accounts = Array.isArray(data) ? data : [data];

    api('POST', '/accounts/import', { accounts: accounts })
      .then(function (result) {
        toast(t('accounts.import_success') + ' (' + (result.imported || 0) + ')', 'success');
        closeModal('importModal');
        load();
      })
      .catch(function (err) {
        toast(t('accounts.import_failed') + ': ' + err.message, 'error');
      });
  }

  /**
   * 导出账号
   */
  function exportAccounts() {
    apiRaw('GET', '/accounts/export')
      .then(function (res) {
        return res.blob();
      })
      .then(function (blob) {
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'accounts-export.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        toast(t('accounts.export_success'), 'success');
      })
      .catch(function (err) {
        toast(t('accounts.export_failed') + ': ' + err.message, 'error');
      });
  }

  function _setGpaResult(data) {
    var output = document.getElementById('gpaResultOutput');
    if (!output) return;
    try {
      output.textContent = JSON.stringify(data, null, 2);
    } catch (_) {
      output.textContent = String(data || '-');
    }
  }

  function _setGpaBusyState(busy) {
    _gpaBusy = busy === true;
    var ids = ['btnGpaPreview', 'btnGpaImport', 'btnGpaExport'];
    for (var i = 0; i < ids.length; i++) {
      var btn = document.getElementById(ids[i]);
      if (btn) btn.disabled = _gpaBusy;
    }
  }

  function _readFileAsText(file) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader();
      reader.onload = function (ev) {
        resolve(String((ev && ev.target && ev.target.result) || ''));
      };
      reader.onerror = function () {
        reject(new Error((file && file.name ? file.name + ': ' : '') + t('accounts.gpa_file_read_failed')));
      };
      reader.readAsText(file);
    });
  }

  function _buildGpaImportPayload(dryRun) {
    var fileInput = document.getElementById('gpaImportFileInput');
    var files = fileInput && fileInput.files ? Array.prototype.slice.call(fileInput.files) : [];
    if (files.length > 0) {
      var tasks = files.map(function (file) {
        return _readFileAsText(file).then(function (text) {
          try {
            var parsed = JSON.parse(text);
            return { name: file.name || 'credential.json', content: parsed };
          } catch (e) {
            throw new Error((file && file.name ? file.name + ': ' : '') + t('accounts.import_json_error') + ': ' + e.message);
          }
        });
      });
      return Promise.all(tasks).then(function (parsedFiles) {
        return {
          dryRun: dryRun === true,
          files: parsedFiles,
        };
      });
    }

    var textarea = document.getElementById('gpaImportTextarea');
    var text = textarea ? String(textarea.value || '').trim() : '';
    if (!text) {
      return Promise.reject(new Error(t('accounts.gpa_import_empty')));
    }

    var parsedText;
    try {
      parsedText = JSON.parse(text);
    } catch (e2) {
      return Promise.reject(new Error(t('accounts.import_json_error') + ': ' + e2.message));
    }

    if (parsedText && typeof parsedText === 'object' && !Array.isArray(parsedText) && Array.isArray(parsedText.files)) {
      var passthrough = Object.assign({}, parsedText);
      passthrough.dryRun = dryRun === true;
      return Promise.resolve(passthrough);
    }

    return Promise.resolve({
      dryRun: dryRun === true,
      files: [{ name: 'pasted.json', content: parsedText }],
    });
  }

  function _importGpaByDryRun(dryRun) {
    if (_gpaBusy) return;
    _setGpaBusyState(true);
    _buildGpaImportPayload(dryRun)
      .then(function (payload) {
        return api('POST', '/credentials/import/gpa', payload);
      })
      .then(function (result) {
        _setGpaResult(result);
        if (dryRun) {
          toast(t('accounts.gpa_preview_success'), 'success');
        } else {
          toast(t('accounts.gpa_import_done', {
            imported: result.imported || 0,
            updated: result.updated || 0,
            rejected: result.rejected || 0,
          }), (result.rejected || 0) > 0 ? 'warning' : 'success');
          load();
        }
      })
      .catch(function (err) {
        toast((dryRun ? t('accounts.gpa_preview_failed') : t('accounts.gpa_import_failed')) + ': ' + err.message, 'error');
      })
      .finally(function () {
        _setGpaBusyState(false);
      });
  }

  function previewGpaImport() {
    _importGpaByDryRun(true);
  }

  function importGpaCredentials() {
    _importGpaByDryRun(false);
  }

  function exportGpaCredentials() {
    if (_gpaBusy) return;
    _setGpaBusyState(true);
    api('GET', '/credentials/export/gpa?status=active')
      .then(function (result) {
        _setGpaResult(result);
        var blob = new Blob([JSON.stringify(result, null, 2)], { type: 'application/json;charset=utf-8' });
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'gpa-credentials-export.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        toast(t('accounts.gpa_export_success', { count: result.count || 0 }), 'success');
      })
      .catch(function (err) {
        toast(t('accounts.gpa_export_failed') + ': ' + err.message, 'error');
      })
      .finally(function () {
        _setGpaBusyState(false);
      });
  }

  function onGpaFileSelection() {
    var hint = document.getElementById('gpaImportFileHint');
    var fileInput = document.getElementById('gpaImportFileInput');
    if (!hint) return;
    var count = (fileInput && fileInput.files && fileInput.files.length) ? fileInput.files.length : 0;
    if (count <= 0) {
      hint.textContent = t('accounts.gpa_file_hint');
    } else {
      hint.textContent = t('accounts.gpa_file_selected', { count: count });
    }
  }

  /**
   * 浏览器登录添加账号
   */
  function browserLogin() {
    var emailInput = document.getElementById('browserLoginEmail');
    var passwordInput = document.getElementById('browserLoginPassword');
    var submitBtn = document.getElementById('browserLoginSubmitBtn');

    var email = emailInput ? emailInput.value.trim() : '';
    var password = passwordInput ? passwordInput.value : '';

    if (!email || !password) {
      toast(t('accounts.browser_login_empty'), 'error');
      return;
    }

    // 禁用按钮，显示加载状态
    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.textContent = t('accounts.browser_login_submitting');
    }

    api('POST', '/accounts/browser-login', { email: email, password: password })
      .then(function () {
        toast(t('accounts.browser_login_success') + ': ' + email, 'success');
        closeModal('browserLoginModal');
        // 清空表单
        if (emailInput) emailInput.value = '';
        if (passwordInput) passwordInput.value = '';
        load();
      })
      .catch(function (err) {
        toast(t('accounts.browser_login_failed') + ': ' + err.message, 'error');
      })
      .finally(function () {
        if (submitBtn) {
          submitBtn.disabled = false;
          submitBtn.textContent = t('accounts.browser_login_submit');
        }
      });
  }

  /**
   * 批量验证失效账号
   */
  function verifyBatch() {
    // 统计本地可验证账号（expired + relogin_needed + wasted）
    var count = 0;
    for (var i = 0; i < _accounts.length; i++) {
      var s = _accounts[i].status;
      if (s === 'expired' || s === 'relogin_needed' || s === 'wasted') count++;
    }
    if (count === 0) {
      toast(t('accounts.no_verifiable'), 'info');
      return;
    }

    showConfirm(t('accounts.confirm_verify_batch', { count: count }), t('accounts.btn_verify'))
      .then(function (confirmed) {
        if (!confirmed) return;

        var btn = document.getElementById('btnVerifyBatch');
        if (btn) {
          btn.disabled = true;
          btn.querySelector('span').textContent = t('accounts.verifying');
        }

        api('POST', '/accounts/verify-batch')
          .then(function (data) {
            toast(t('accounts.verify_result', { ok: data.verified_ok || 0, fail: data.verified_fail || 0 }), 'success');
            load();
          })
          .catch(function (err) {
            toast(t('accounts.action_failed') + ': ' + err.message, 'error');
          })
          .finally(function () {
            if (btn) {
              btn.disabled = false;
              btn.querySelector('span').textContent = t('accounts.verify_batch');
            }
          });
      });
  }

  /**
   * 获取当前筛选
   */
  function getCurrentFilter() {
    return _currentFilter;
  }

  
  function _doTestAccount(email) {
    toast('测试中: ' + email, 'info');
    // 找到按钮并标记 loading
    var btns = document.querySelectorAll('[data-action="test"][data-email="' + email + '"]');
    btns.forEach(function(b) { b.disabled = true; b.textContent = '...'; });
    api('POST', '/accounts/' + encodeURIComponent(email) + '/action', { action: 'test' })
      .then(function (data) {
        if (data.ok) {
          toast(email + ': 测试成功 (' + (data.latency || 0) + 'ms)', 'success');
        } else {
          toast(email + ': ' + (data.message || '测试失败'), 'error');
        }
        load();
      })
      .catch(function(e) { toast('测试异常: ' + e.message, 'error'); load(); })
      .finally(function() {
        btns.forEach(function(b) { b.disabled = false; b.textContent = t('accounts.btn_test'); });
      });
  }

  function _doCheckSingle(email) {
    toast(t('accounts.checking') + ' ' + email, 'info');
    api('POST', '/accounts/check-status', { email: email })
      .then(function (data) {
        if (data.check_status === 'banned') toast(email + ': ' + t('accounts.check_result_banned') + ' (' + (data.detail||'') + ')', 'error');
        else if (data.check_status === 'active') toast(email + ': ' + t('accounts.check_result_active'), 'success');
        else toast(email + ': ' + t('accounts.check_result_error') + ' (' + (data.detail||'') + ')', 'warning');
        load();
      }).catch(function(e) { toast(t('accounts.check_result_error') + ': ' + e.message, 'error'); });
  }

  function testBatch() {
    var count = _accounts.filter(function(a) { return a.status === 'active'; }).length;
    if (!count) { toast('没有活跃账号可测试', 'info'); return; }
    showConfirm('确认对 ' + count + ' 个活跃账号发送测试请求？(5并发)', '批量测试').then(function(ok) {
      if (!ok) return;
      var btn = document.getElementById('btnTestBatch');
      if (btn) { btn.disabled = true; btn.querySelector('span').textContent = '测试中...'; }
      api('POST', '/accounts/test-batch', { filter: 'active', concurrency: 5 })
        .then(function(d) {
          var msg = '批量测试完成: ' + (d.ok || 0) + ' 成功, ' + (d.fail || 0) + ' 失败 / ' + (d.total || 0) + ' 总计';
          toast(msg, d.fail > 0 ? 'warning' : 'success');
          load();
        })
        .catch(function(e) { toast('批量测试失败: ' + e.message, 'error'); })
        .finally(function() { if(btn){btn.disabled=false; btn.querySelector('span').textContent='批量测试';} });
    });
  }

  function checkBatch() {
    var count = _accounts.length;
    if (!count) { toast(t('accounts.no_checkable'), 'info'); return; }
    showConfirm(t('accounts.confirm_check_batch', {count:count}), t('accounts.check_batch')).then(function(ok) {
      if (!ok) return;
      var btn = document.getElementById('btnCheckBatch');
      if (btn) { btn.disabled = true; btn.querySelector('span').textContent = t('accounts.checking'); }
      api('POST', '/accounts/check-batch', {filter:'all'}).then(function(d) {
        toast(t('accounts.check_done', {active:d.active||0, banned:d.banned||0, error:d.error||0}), d.banned>0?'warning':'success');
        load();
      }).catch(function(e) { toast(t('accounts.action_failed')+': '+e.message,'error'); })
      .finally(function() { if(btn){btn.disabled=false; btn.querySelector('span').textContent=t('accounts.check_batch');} });
    });
  }

  return {
    load: load,
    render: render,
    setFilter: setFilter,
    setSearch: setSearch,
    handleAction: handleAction,
    importAccounts: importAccounts,
    exportAccounts: exportAccounts,
    previewGpaImport: previewGpaImport,
    importGpaCredentials: importGpaCredentials,
    exportGpaCredentials: exportGpaCredentials,
    onGpaFileSelection: onGpaFileSelection,
    browserLogin: browserLogin,
    verifyBatch: verifyBatch,
    getCurrentFilter: getCurrentFilter,
    testBatch: testBatch,
    checkBatch: checkBatch,
  };
})();
