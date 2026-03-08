/**
 * codex2api 管理面板 — 认证模块
 * 登录、登出、session 检查
 */

var Auth = (function () {
  'use strict';

  var _loginOptions = {
    totp_enabled: false,
    totp_allow_passwordless: true,
    totp_digits: 6,
  };

  function _normalizeLoginOptions(data) {
    var options = data || {};
    return {
      totp_enabled: options.totp_enabled === true,
      totp_allow_passwordless: options.totp_allow_passwordless !== false,
      totp_digits: (Number.isFinite(options.totp_digits) && options.totp_digits > 0)
        ? Math.floor(options.totp_digits)
        : 6,
    };
  }

  function fetchLoginOptions() {
    return fetch('/admin/api/login/options', {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
    })
      .then(function (res) {
        if (!res.ok) return _loginOptions;
        return res.json();
      })
      .then(function (data) {
        _loginOptions = _normalizeLoginOptions(data);
        return _loginOptions;
      })
      .catch(function () {
        return _loginOptions;
      });
  }

  function getLoginOptions() {
    return Object.assign({}, _loginOptions);
  }

  /**
   * 登录
   */
  function login(username, password, totpCode, mode) {
    var loginBtn = document.getElementById('loginBtn');
    var loginError = document.getElementById('loginError');

    if (loginBtn) {
      loginBtn.disabled = true;
      loginBtn.textContent = t('login.submitting');
    }

    var payload = {
      username: username,
      mode: mode || 'password_totp',
    };
    if (password !== undefined && password !== null && password !== '') {
      payload.password = password;
    }
    if (totpCode !== undefined && totpCode !== null && String(totpCode).trim() !== '') {
      payload.totp_code = String(totpCode).trim();
    }

    return api('POST', '/login', payload)
      .then(function (data) {
        localStorage.setItem('codex2api_token', data.token);
        localStorage.setItem('codex2api_user', username);
        toast(t('login.success'), 'success');
        return true;
      })
      .catch(function (err) {
        if (loginError) {
          loginError.textContent = err.message || t('login.failed');
          loginError.classList.add('visible');
        }
        return false;
      })
      .finally(function () {
        if (loginBtn) {
          loginBtn.disabled = false;
          loginBtn.textContent = t('login.submit');
        }
      });
  }

  /**
   * 登出
   */
  function logout() {
    // 尝试通知后端
    apiRaw('POST', '/logout', {}).catch(function () {
      // ignore
    });

    localStorage.removeItem('codex2api_token');
    localStorage.removeItem('codex2api_user');

    // 清除自动刷新定时器
    if (typeof App !== 'undefined' && App.stopAutoRefresh) {
      App.stopAutoRefresh();
    }

    showLoginPage();
    toast(t('login.logged_out'), 'info');
  }

  /**
   * 检查已有 token 是否仍然有效
   * @returns {Promise<boolean>}
   */
  function checkAuth() {
    var token = localStorage.getItem('codex2api_token');
    if (!token) {
      return Promise.resolve(false);
    }

    return api('GET', '/dashboard')
      .then(function () {
        return true;
      })
      .catch(function () {
        return false;
      });
  }

  /**
   * 获取当前用户名
   */
  function getUsername() {
    return localStorage.getItem('codex2api_user') || '';
  }

  /**
   * 是否已登录
   */
  function isLoggedIn() {
    return !!localStorage.getItem('codex2api_token');
  }

  /* ============ 页面切换 ============ */

  function showLoginPage() {
    var loginPage = document.getElementById('loginPage');
    var appLayout = document.getElementById('appLayout');
    var loginError = document.getElementById('loginError');
    var loginUser = document.getElementById('loginUser');
    var loginPass = document.getElementById('loginPass');
    var loginPassWrap = document.getElementById('loginPassWrap');
    var loginTotp = document.getElementById('loginTotp');
    var loginMode = document.getElementById('loginMode');

    if (loginPage) loginPage.classList.remove('hidden');
    if (appLayout) appLayout.classList.add('hidden');
    if (loginError) loginError.classList.remove('visible');
    if (loginUser) loginUser.value = '';
    if (loginPass) loginPass.value = '';
    if (loginPass) loginPass.disabled = false;
    if (loginPassWrap) loginPassWrap.classList.remove('hidden');
    if (loginTotp) loginTotp.value = '';
    if (loginMode) {
      loginMode.value = 'password_totp';
      try {
        loginMode.dispatchEvent(new Event('change'));
      } catch (_) {}
    }
    if (loginUser) loginUser.focus();
  }

  function showAppPage() {
    var loginPage = document.getElementById('loginPage');
    var appLayout = document.getElementById('appLayout');
    var topbarUser = document.getElementById('topbarUser');

    if (loginPage) loginPage.classList.add('hidden');
    if (appLayout) appLayout.classList.remove('hidden');
    if (topbarUser) topbarUser.textContent = getUsername();
  }

  return {
    login: login,
    fetchLoginOptions: fetchLoginOptions,
    getLoginOptions: getLoginOptions,
    logout: logout,
    checkAuth: checkAuth,
    getUsername: getUsername,
    isLoggedIn: isLoggedIn,
    showLoginPage: showLoginPage,
    showAppPage: showAppPage,
  };
})();
