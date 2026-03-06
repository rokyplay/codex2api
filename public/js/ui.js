/**
 * codex2api 管理面板 — UI 组件
 * Toast 通知、确认弹窗、Loading Overlay、模态框管理
 */

/* ============ Toast 通知 ============ */

var _toastManager = {
  maxToasts: 5,
  activeToasts: [],

  add: function (toast) {
    this.activeToasts.push(toast);
    while (this.activeToasts.length > this.maxToasts) {
      var oldest = this.activeToasts.shift();
      if (oldest && oldest.parentNode) {
        oldest.remove();
      }
    }
  },

  remove: function (toast) {
    var index = this.activeToasts.indexOf(toast);
    if (index > -1) {
      this.activeToasts.splice(index, 1);
    }
  },
};

/**
 * 显示 toast 通知
 * @param {string} message - 消息内容
 * @param {string} type - 类型：success / error / warning / info
 */
function toast(message, type) {
  type = type || 'info';
  var container = document.getElementById('toastContainer');
  if (!container) return;

  var icons = { success: '✓', error: '✕', warning: '!', info: 'i' };
  var el = document.createElement('div');
  el.className = 'toast toast-' + type;

  var safeMessage = escapeHtml(message);
  el.innerHTML =
    '<span class="toast-icon toast-icon-' + type + '">' + (icons[type] || 'i') + '</span>' +
    '<span class="toast-text">' + safeMessage + '</span>';

  container.appendChild(el);
  _toastManager.add(el);

  var removeToast = function () {
    el.style.opacity = '0';
    el.style.transform = 'translateX(40px)';
    el.style.transition = 'all 0.3s ease';
    setTimeout(function () {
      _toastManager.remove(el);
      if (el.parentNode) {
        el.parentNode.removeChild(el);
      }
    }, 300);
  };

  setTimeout(removeToast, 3000);
}

/* ============ 确认弹窗 ============ */

/**
 * Promise-based 自定义确认弹窗（替代 window.confirm）
 * @param {string} message - 提示消息
 * @param {string} title - 弹窗标题
 * @returns {Promise<boolean>}
 */
function showConfirm(message, title) {
  title = title || t('common.confirm');
  return new Promise(function (resolve) {
    var overlay = document.createElement('div');
    overlay.className = 'modal-overlay visible';

    var safeTitle = escapeHtml(title);
    var safeMessage = escapeHtml(message);

    overlay.innerHTML =
      '<div class="modal confirm-modal">' +
        '<div class="modal-header">' +
          '<h3 class="modal-title">' + safeTitle + '</h3>' +
        '</div>' +
        '<div class="modal-body">' +
          '<p class="confirm-message">' + safeMessage + '</p>' +
        '</div>' +
        '<div class="modal-footer">' +
          '<button class="btn btn-secondary" data-action="cancel">' + t('common.cancel') + '</button>' +
          '<button class="btn btn-danger" data-action="ok">' + t('common.confirm') + '</button>' +
        '</div>' +
      '</div>';

    document.body.appendChild(overlay);

    var cleanup = function () {
      if (overlay.parentNode) {
        overlay.parentNode.removeChild(overlay);
      }
    };

    overlay.addEventListener('click', function (e) {
      var action = e.target.getAttribute('data-action');
      if (action === 'cancel') {
        cleanup();
        resolve(false);
      } else if (action === 'ok') {
        cleanup();
        resolve(true);
      } else if (e.target === overlay) {
        cleanup();
        resolve(false);
      }
    });
  });
}

/**
 * Promise-based 密码确认弹窗（带密码输入框）
 * @param {string} message - 提示消息
 * @param {string} title - 弹窗标题
 * @returns {Promise<{confirmed: boolean, password: string}>}
 */
function showPasswordConfirm(message, title) {
  title = title || t('common.confirm');
  return new Promise(function (resolve) {
    var overlay = document.createElement('div');
    overlay.className = 'modal-overlay visible';

    var safeTitle = escapeHtml(title);
    var safeMessage = escapeHtml(message);

    overlay.innerHTML =
      '<div class="modal confirm-modal">' +
        '<div class="modal-header">' +
          '<h3 class="modal-title">' + safeTitle + '</h3>' +
        '</div>' +
        '<div class="modal-body">' +
          '<p class="confirm-message">' + safeMessage + '</p>' +
          '<div class="config-field" style="margin-top:12px">' +
            '<label class="config-label">' + escapeHtml(t('common.admin_password')) + '</label>' +
            '<input class="input config-input" type="password" id="confirmPasswordInput" autocomplete="off" placeholder="' + escapeHtml(t('common.enter_admin_password')) + '">' +
          '</div>' +
        '</div>' +
        '<div class="modal-footer">' +
          '<button class="btn btn-secondary" data-action="cancel">' + escapeHtml(t('common.cancel')) + '</button>' +
          '<button class="btn btn-danger" data-action="ok">' + escapeHtml(t('common.confirm')) + '</button>' +
        '</div>' +
      '</div>';

    document.body.appendChild(overlay);

    // 聚焦密码框
    var pwInput = overlay.querySelector('#confirmPasswordInput');
    setTimeout(function () { if (pwInput) pwInput.focus(); }, 50);

    var cleanup = function () {
      if (overlay.parentNode) {
        overlay.parentNode.removeChild(overlay);
      }
    };

    // Enter 键提交
    if (pwInput) {
      pwInput.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') {
          e.preventDefault();
          cleanup();
          resolve({ confirmed: true, password: pwInput.value });
        }
      });
    }

    overlay.addEventListener('click', function (e) {
      var action = e.target.getAttribute('data-action');
      if (action === 'cancel') {
        cleanup();
        resolve({ confirmed: false, password: '' });
      } else if (action === 'ok') {
        cleanup();
        resolve({ confirmed: true, password: pwInput ? pwInput.value : '' });
      } else if (e.target === overlay) {
        cleanup();
        resolve({ confirmed: false, password: '' });
      }
    });
  });
}

/* ============ Loading Overlay ============ */

var _currentLoadingOverlay = null;

/**
 * 显示全局 loading 遮罩
 * @param {string} text - 提示文字
 */
function showLoading(text) {
  hideLoading();
  text = text || t('common.loading');

  var overlay = document.createElement('div');
  overlay.className = 'loading-overlay';
  overlay.id = 'loadingOverlay';

  var safeText = escapeHtml(text);
  overlay.innerHTML =
    '<div class="loading-box">' +
      '<div class="spinner"></div>' +
      '<div class="loading-text">' + safeText + '</div>' +
    '</div>';

  document.body.appendChild(overlay);
  _currentLoadingOverlay = overlay;
}

/**
 * 隐藏全局 loading 遮罩
 */
function hideLoading() {
  if (_currentLoadingOverlay && _currentLoadingOverlay.parentNode) {
    _currentLoadingOverlay.parentNode.removeChild(_currentLoadingOverlay);
  }
  _currentLoadingOverlay = null;

  // 备用清理
  var existing = document.getElementById('loadingOverlay');
  if (existing && existing.parentNode) {
    existing.parentNode.removeChild(existing);
  }
}

/* ============ 模态框通用管理 ============ */

/**
 * 打开模态框
 * @param {string} modalId - 模态框元素 ID
 */
function openModal(modalId) {
  var modal = document.getElementById(modalId);
  if (modal) {
    modal.classList.add('visible');
  }
}

/**
 * 关闭模态框
 * @param {string} modalId - 模态框元素 ID
 */
function closeModal(modalId) {
  var modal = document.getElementById(modalId);
  if (modal) {
    modal.classList.remove('visible');
  }
}

/**
 * 绑定模态框背景点击关闭
 * @param {string} modalId - 模态框元素 ID
 */
function wireModalBackdropClose(modalId) {
  var modal = document.getElementById(modalId);
  if (!modal) return;

  modal.addEventListener('click', function (e) {
    if (e.target === modal) {
      closeModal(modalId);
    }
  });
}
