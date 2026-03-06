/**
 * codex2api 管理面板 — 主入口
 * 初始化、路由、页面切换、全局事件绑定
 */

var App = (function () {
  'use strict';

  var _currentPage = 'dashboard';
  var _refreshTimer = null;
  var _loginOptions = {
    totp_enabled: false,
    totp_allow_passwordless: true,
    totp_digits: 6,
  };

  var _pageTitleMap = {
    dashboard: 'nav.dashboard',
    accounts: 'nav.accounts',
    statistics: 'nav.statistics',
    registration: 'nav.registration',
    config: 'nav.config',
    logs: 'nav.logs',
    abuse: 'nav.abuse',
  };

  function _refreshLoginModeVisibility() {
    var passWrap = document.getElementById('loginPassWrap');
    var passInput = document.getElementById('loginPass');
    var modeSelect = document.getElementById('loginMode');
    var mode = modeSelect ? modeSelect.value : 'password_totp';
    var hidePassword = _loginOptions.totp_enabled && mode === 'totp_only';

    if (passWrap) {
      passWrap.classList.toggle('hidden', hidePassword);
    }
    if (passInput) {
      passInput.disabled = hidePassword;
      if (hidePassword) passInput.value = '';
    }
  }

  function _applyLoginOptions(options) {
    _loginOptions = options || _loginOptions;

    var totpWrap = document.getElementById('loginTotpWrap');
    var modeWrap = document.getElementById('loginModeWrap');
    var totpInput = document.getElementById('loginTotp');
    var modeSelect = document.getElementById('loginMode');
    var totpOnlyOption = document.getElementById('loginModeTotpOnly');

    if (totpWrap) {
      totpWrap.classList.toggle('hidden', !_loginOptions.totp_enabled);
    }
    if (modeWrap) {
      modeWrap.classList.toggle('hidden', !_loginOptions.totp_enabled);
    }
    if (totpInput) {
      totpInput.maxLength = _loginOptions.totp_digits || 6;
      totpInput.placeholder = t('login.totp_placeholder', { digits: _loginOptions.totp_digits || 6 });
      if (!_loginOptions.totp_enabled) {
        totpInput.value = '';
      }
    }
    if (modeSelect) {
      if (!_loginOptions.totp_enabled) {
        modeSelect.value = 'password_totp';
      }
      if (totpOnlyOption) {
        var allowPasswordless = _loginOptions.totp_allow_passwordless === true;
        totpOnlyOption.disabled = !allowPasswordless;
        totpOnlyOption.hidden = !allowPasswordless;
        if (!allowPasswordless && modeSelect.value === 'totp_only') {
          modeSelect.value = 'password_totp';
        }
      }
    }

    _refreshLoginModeVisibility();
  }

  /* ============ 页面导航 ============ */

  function navigateTo(page) {
    // 离开页面时清理
    if (_currentPage === 'logs' && page !== 'logs') {
      Logs.cleanup();
    }
    if (_currentPage === 'statistics' && page !== 'statistics') {
      Statistics.cleanup();
    }
    if (_currentPage === 'registration' && page !== 'registration') {
      Registration.cleanup();
    }
    if (_currentPage === 'abuse' && page !== 'abuse') {
      Abuse.cleanup();
    }

    _currentPage = page;

    // 更新 URL hash
    if (window.location.hash !== '#' + page) {
      history.replaceState(null, '', '#' + page);
    }

    // 更新标题
    var pageTitle = document.getElementById('pageTitle');
    if (pageTitle) {
      var titleKey = _pageTitleMap[page] || page;
      pageTitle.textContent = t(titleKey);
    }

    // 更新导航高亮
    var navItems = document.querySelectorAll('.nav-item');
    for (var i = 0; i < navItems.length; i++) {
      if (navItems[i].getAttribute('data-page') === page) {
        navItems[i].classList.add('active');
      } else {
        navItems[i].classList.remove('active');
      }
    }

    // 切换页面
    var pages = document.querySelectorAll('.page');
    for (var j = 0; j < pages.length; j++) {
      pages[j].classList.remove('active');
    }
    var targetId = 'page' + page.charAt(0).toUpperCase() + page.slice(1);
    var targetPage = document.getElementById(targetId);
    if (targetPage) {
      targetPage.classList.add('active');
    }

    // 关闭移动端侧边栏
    _closeMobileSidebar();

    // 加载数据
    _loadPageData(page);
  }

  function _loadPageData(page) {
    switch (page) {
      case 'dashboard':
        Dashboard.load();
        break;
      case 'accounts':
        Accounts.load();
        break;
      case 'config':
        Config.load();
        break;
      case 'statistics':
        Statistics.init();
        break;
      case 'logs':
        Logs.init();
        break;
      case 'registration':
        Registration.init();
        break;
      case 'abuse':
        Abuse.load();
        break;
    }
  }

  /* ============ 自动刷新 ============ */

  function _startAutoRefresh() {
    _stopAutoRefresh();
    _refreshTimer = setInterval(function () {
      if (_currentPage === 'dashboard') {
        Dashboard.load();
      } else if (_currentPage === 'accounts') {
        Accounts.load();
      }
    }, 10000);
  }

  function _stopAutoRefresh() {
    if (_refreshTimer) {
      clearInterval(_refreshTimer);
      _refreshTimer = null;
    }
  }

  /* ============ 移动端侧边栏 ============ */

  function _toggleMobileSidebar() {
    var sidebar = document.getElementById('sidebar');
    var overlay = document.getElementById('sidebarOverlay');
    if (sidebar) sidebar.classList.toggle('mobile-open');
    if (overlay) overlay.classList.toggle('visible');
  }

  function _closeMobileSidebar() {
    var sidebar = document.getElementById('sidebar');
    var overlay = document.getElementById('sidebarOverlay');
    if (sidebar) sidebar.classList.remove('mobile-open');
    if (overlay) overlay.classList.remove('visible');
  }

  /* ============ 主题切换 ============ */

  function _initTheme() {
    var saved = localStorage.getItem('codex2api_theme');
    if (saved === 'dark') {
      document.documentElement.setAttribute('data-theme', 'dark');
    } else {
      document.documentElement.removeAttribute('data-theme');
    }
  }

  function _toggleTheme() {
    var isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    document.documentElement.classList.add('theme-transitioning');
    if (isDark) {
      document.documentElement.removeAttribute('data-theme');
      localStorage.setItem('codex2api_theme', 'light');
    } else {
      document.documentElement.setAttribute('data-theme', 'dark');
      localStorage.setItem('codex2api_theme', 'dark');
    }
    setTimeout(function () {
      document.documentElement.classList.remove('theme-transitioning');
    }, 350);
  }

  /* ============ 事件绑定 ============ */

  function _bindEvents() {
    // 登录表单
    var loginForm = document.getElementById('loginForm');
    if (loginForm) {
      loginForm.addEventListener('submit', function (e) {
        e.preventDefault();
        var user = document.getElementById('loginUser').value.trim();
        var pass = document.getElementById('loginPass').value;
        var totpCode = (document.getElementById('loginTotp') || {}).value || '';
        var mode = ((document.getElementById('loginMode') || {}).value) || 'password_totp';
        var loginError = document.getElementById('loginError');
        var needsPassword = !_loginOptions.totp_enabled || mode !== 'totp_only';

        if (!user || (needsPassword && !pass)) {
          if (loginError) {
            loginError.textContent = t('login.error_empty');
            loginError.classList.add('visible');
          }
          return;
        }
        if (_loginOptions.totp_enabled && !String(totpCode).trim()) {
          if (loginError) {
            loginError.textContent = t('login.totp_required');
            loginError.classList.add('visible');
          }
          return;
        }
        if (loginError) loginError.classList.remove('visible');

        Auth.login(user, pass, totpCode, mode).then(function (success) {
          if (success) {
            _showApp();
          }
        });
      });
    }

    var loginMode = document.getElementById('loginMode');
    if (loginMode) {
      loginMode.addEventListener('change', function () {
        _refreshLoginModeVisibility();
      });
    }

    // 退出按钮
    var logoutBtn = document.getElementById('logoutBtn');
    if (logoutBtn) {
      logoutBtn.addEventListener('click', function () {
        Auth.logout();
      });
    }

    // 主题切换
    var themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
      themeToggle.addEventListener('click', _toggleTheme);
    }

    // 邮箱显隐切换
    var emailToggleBtn = document.getElementById('emailToggleBtn');
    if (emailToggleBtn) {
      emailToggleBtn.addEventListener('click', toggleEmailMask);
    }

    // 语言切换
    var langToggle = document.getElementById('langToggle');
    if (langToggle) {
      langToggle.addEventListener('click', function () {
        var newLang = getLang() === 'zh' ? 'en' : 'zh';
        setLang(newLang);
        var label = document.getElementById('langLabel');
        if (label) label.textContent = newLang === 'zh' ? '中' : 'EN';
        // 更新页面标题
        var pageTitle = document.getElementById('pageTitle');
        if (pageTitle) {
          var titleKey = _pageTitleMap[_currentPage] || _currentPage;
          pageTitle.textContent = t(titleKey);
        }
        if (typeof DatePicker !== 'undefined' && DatePicker.refreshLabel) {
          DatePicker.refreshLabel();
        }
        _applyLoginOptions(Auth.getLoginOptions());
        // 清空统计页缓存，使其能重新渲染
        var statsPage = document.getElementById('pageStatistics');
        if (statsPage) statsPage.innerHTML = '';
        // 重新加载当前页面数据（会重新渲染动态内容）
        _loadPageData(_currentPage);
      });
    }

    // 侧边栏导航 — 事件委托
    var sidebarNav = document.querySelector('.sidebar-nav');
    if (sidebarNav) {
      sidebarNav.addEventListener('click', function (e) {
        var navItem = e.target.closest('.nav-item');
        if (navItem) {
          var page = navItem.getAttribute('data-page');
          if (page) {
            navigateTo(page);
          }
        }
      });
    }

    // 移动端菜单 toggle
    var menuToggle = document.getElementById('menuToggle');
    if (menuToggle) {
      menuToggle.addEventListener('click', _toggleMobileSidebar);
    }

    var sidebarOverlay = document.getElementById('sidebarOverlay');
    if (sidebarOverlay) {
      sidebarOverlay.addEventListener('click', _closeMobileSidebar);
    }

    // 模型测试按钮
    var btnTestModels = document.getElementById('btnTestModels');
    if (btnTestModels) {
      btnTestModels.addEventListener('click', function () {
        Dashboard.testModels();
      });
    }

    // 仪表盘账号健康横条点击 → 跳转到账号页 + 筛选 — 事件委托
    var accountHealthBar = document.getElementById('accountHealthBar');
    if (accountHealthBar) {
      accountHealthBar.addEventListener('click', function (e) {
        var item = e.target.closest('.health-item[data-filter]');
        if (item) {
          var filter = item.getAttribute('data-filter');
          navigateTo('accounts');
          setTimeout(function () {
            Accounts.setFilter(filter);
          }, 100);
        }
      });
    }

    // 账号管理 — 事件委托（操作按钮）
    var accountsBody = document.getElementById('accountsBody');
    if (accountsBody) {
      accountsBody.addEventListener('click', function (e) {
        var btn = e.target.closest('button[data-action]');
        if (btn) {
          var action = btn.getAttribute('data-action');
          var email = btn.getAttribute('data-email');
          if (action && email) {
            Accounts.handleAction(email, action);
          }
        }
      });
    }

    // 账号搜索 — 防抖
    var accountSearch = document.getElementById('accountSearch');
    if (accountSearch) {
      accountSearch.addEventListener('input', debounce(function () {
        Accounts.setSearch(accountSearch.value);
      }, 300));
    }

    // 账号状态筛选 — 事件委托
    var filterBar = document.getElementById('filterBar');
    if (filterBar) {
      filterBar.addEventListener('click', function (e) {
        var btn = e.target.closest('.filter-btn');
        if (btn) {
          var filter = btn.getAttribute('data-filter');
          Accounts.setFilter(filter);
        }
      });
    }

    // 导入按钮
    var btnImport = document.getElementById('btnImport');
    if (btnImport) {
      btnImport.addEventListener('click', function () {
        var textarea = document.getElementById('importTextarea');
        if (textarea) textarea.value = '';
        openModal('importModal');
      });
    }

    // 导入弹窗关闭
    var importModalClose = document.getElementById('importModalClose');
    if (importModalClose) {
      importModalClose.addEventListener('click', function () {
        closeModal('importModal');
      });
    }

    var importCancelBtn = document.getElementById('importCancelBtn');
    if (importCancelBtn) {
      importCancelBtn.addEventListener('click', function () {
        closeModal('importModal');
      });
    }

    // 导入确认
    var importConfirmBtn = document.getElementById('importConfirmBtn');
    if (importConfirmBtn) {
      importConfirmBtn.addEventListener('click', function () {
        var textarea = document.getElementById('importTextarea');
        var text = textarea ? textarea.value.trim() : '';
        if (!text) {
          toast(t('accounts.import_empty'), 'error');
          return;
        }
        Accounts.importAccounts(text);
      });
    }

    // 文件上传区域
    var fileUploadArea = document.getElementById('fileUploadArea');
    var fileUploadInput = document.getElementById('fileUploadInput');
    if (fileUploadArea && fileUploadInput) {
      fileUploadArea.addEventListener('click', function () {
        fileUploadInput.click();
      });

      fileUploadArea.addEventListener('dragover', function (e) {
        e.preventDefault();
        e.stopPropagation();
        fileUploadArea.classList.add('dragover');
      });

      fileUploadArea.addEventListener('dragleave', function (e) {
        e.preventDefault();
        e.stopPropagation();
        fileUploadArea.classList.remove('dragover');
      });

      fileUploadArea.addEventListener('drop', function (e) {
        e.preventDefault();
        e.stopPropagation();
        fileUploadArea.classList.remove('dragover');
        var file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
        if (file) _readUploadedFile(file);
      });

      fileUploadInput.addEventListener('change', function (e) {
        var file = e.target.files && e.target.files[0];
        if (file) _readUploadedFile(file);
        e.target.value = '';
      });
    }

    // 导出按钮
    var btnExport = document.getElementById('btnExport');
    if (btnExport) {
      btnExport.addEventListener('click', function () {
        Accounts.exportAccounts();
      });
    }

    // 刷新账号按钮
    var btnRefreshAccounts = document.getElementById('btnRefreshAccounts');
    if (btnRefreshAccounts) {
      btnRefreshAccounts.addEventListener('click', function () {
        Accounts.load();
        toast(t('accounts.refreshed'), 'info');
      });
    }

    // 批量验证按钮
    var btnVerifyBatch = document.getElementById('btnVerifyBatch');
    if (btnVerifyBatch) {
      btnVerifyBatch.addEventListener('click', function () {
        Accounts.verifyBatch();
      });


    var btnTestBatch = document.getElementById('btnTestBatch');
    if (btnTestBatch) {
      btnTestBatch.addEventListener('click', function () {
        Accounts.testBatch();
      });
    }

    var btnCheckBatch = document.getElementById("btnCheckBatch");
    if (btnCheckBatch) {
      btnCheckBatch.addEventListener("click", function () {
        Accounts.checkBatch();
      });
    }    }

    // 浏览器登录按钮
    var btnBrowserLogin = document.getElementById('btnBrowserLogin');
    if (btnBrowserLogin) {
      btnBrowserLogin.addEventListener('click', function () {
        var emailInput = document.getElementById('browserLoginEmail');
        var passwordInput = document.getElementById('browserLoginPassword');
        if (emailInput) emailInput.value = '';
        if (passwordInput) passwordInput.value = '';
        openModal('browserLoginModal');
      });
    }

    // 浏览器登录弹窗关闭
    var browserLoginModalClose = document.getElementById('browserLoginModalClose');
    if (browserLoginModalClose) {
      browserLoginModalClose.addEventListener('click', function () {
        closeModal('browserLoginModal');
      });
    }

    var browserLoginCancelBtn = document.getElementById('browserLoginCancelBtn');
    if (browserLoginCancelBtn) {
      browserLoginCancelBtn.addEventListener('click', function () {
        closeModal('browserLoginModal');
      });
    }

    // 浏览器登录确认
    var browserLoginSubmitBtn = document.getElementById('browserLoginSubmitBtn');
    if (browserLoginSubmitBtn) {
      browserLoginSubmitBtn.addEventListener('click', function () {
        Accounts.browserLogin();
      });
    }

    // 浏览器登录表单回车提交
    var browserLoginPassword = document.getElementById('browserLoginPassword');
    if (browserLoginPassword) {
      browserLoginPassword.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') {
          e.preventDefault();
          Accounts.browserLogin();
        }
      });
    }

    // 导入弹窗背景点击关闭
    wireModalBackdropClose('importModal');
    wireModalBackdropClose('browserLoginModal');
    wireModalBackdropClose('abuseDetailModal');

    // 日志页面事件 — 事件委托

    // 日志统计项点击筛选
    var logStats = document.getElementById('logStats');
    if (logStats) {
      logStats.addEventListener('click', function (e) {
        var item = e.target.closest('.log-stat-item');
        if (item) {
          var level = item.getAttribute('data-level');
          Logs.filterLevel(level);
        }
      });
    }

    // 日志搜索
    var logSearchInput = document.getElementById('logSearchInput');
    if (logSearchInput) {
      logSearchInput.addEventListener('input', debounce(function () {
        Logs.search(logSearchInput.value);
      }, 300));
    }

    // 日志操作按钮
    var autoRefreshBtn = document.getElementById('autoRefreshBtn');
    if (autoRefreshBtn) {
      autoRefreshBtn.addEventListener('click', function () {
        Logs.toggleAutoRefresh();
      });
    }

    var manualRefreshBtn = document.getElementById('manualRefreshBtn');
    if (manualRefreshBtn) {
      manualRefreshBtn.addEventListener('click', function () {
        Logs.loadLogs();
        Logs.loadStats();
      });
    }

    var clearLogsBtn = document.getElementById('clearLogsBtn');
    if (clearLogsBtn) {
      clearLogsBtn.addEventListener('click', function () {
        Logs.clearLogs();
      });
    }

    var loadMoreLogsBtn = document.getElementById('loadMoreLogsBtn');
    if (loadMoreLogsBtn) {
      loadMoreLogsBtn.addEventListener('click', function () {
        Logs.loadMore();
      });
    }

    // 日志列表事件委托 — 复制按钮
    var logList = document.getElementById('logList');
    if (logList) {
      logList.addEventListener('click', function (e) {
        var copyBtn = e.target.closest('.log-copy-btn');
        if (copyBtn) {
          var index = parseInt(copyBtn.getAttribute('data-log-index'), 10);
          Logs.copyLog(index);
        }
      });
    }

    // URL hash 变化
    window.addEventListener('hashchange', function () {
      var hash = window.location.hash.slice(1);
      if (hash && _pageTitleMap[hash]) {
        navigateTo(hash);
      }
    });

    // ESC 关闭弹窗 / 侧边栏
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        closeModal('importModal');
        closeModal('browserLoginModal');
        closeModal('abuseDetailModal');
        _closeMobileSidebar();
      }
    });
  }

  function _readUploadedFile(file) {
    var reader = new FileReader();
    reader.onload = function (ev) {
      var textarea = document.getElementById('importTextarea');
      if (textarea) {
        textarea.value = ev.target.result;
      }
    };
    reader.readAsText(file);
  }

  /* ============ 显示应用 ============ */

  function _showApp() {
    Auth.showAppPage();
    if (typeof DatePicker !== 'undefined' && DatePicker.init) {
      DatePicker.init();
    }
    var hash = window.location.hash.slice(1);
    if (hash && _pageTitleMap[hash]) {
      navigateTo(hash);
    } else {
      navigateTo('dashboard');
    }
    Dashboard.startHealthCheck();
    _startAutoRefresh();
  }

  /* ============ 初始化 ============ */

  function init() {
    // 初始化主题（DOM 渲染前，避免闪烁）
    _initTheme();

    // 初始化语言标签
    var langLabel = document.getElementById('langLabel');
    if (langLabel) langLabel.textContent = getLang() === 'zh' ? '中' : 'EN';

    // 应用 i18n
    applyI18n();

    Auth.fetchLoginOptions()
      .then(function (options) {
        _applyLoginOptions(options);
      })
      .catch(function () {})
      .finally(function () {
        // 绑定事件
        _bindEvents();

        // 检查认证状态
        Auth.checkAuth().then(function (loggedIn) {
          if (loggedIn) {
            _showApp();
          } else {
            Auth.showLoginPage();
          }
        });
      });
  }

  /* ============ 公共接口 ============ */

  return {
    init: init,
    navigateTo: navigateTo,
    logout: Auth.logout,
    stopAutoRefresh: _stopAutoRefresh,
  };
})();

// DOM Ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', App.init);
} else {
  App.init();
}
