/**
 * codex2api 管理面板 — 全局日期选择器
 */

var DatePicker = (function () {
  'use strict';

  var _inited = false;
  var _wrapper = null;
  var _btn = null;
  var _label = null;
  var _dropdown = null;
  var _fromInput = null;
  var _toInput = null;

  function _syncInputsFromState() {
    var r = getDateRange();
    if (!_fromInput || !_toInput) return;
    if (r.mode === 'custom') {
      _fromInput.value = r.from || '';
      _toInput.value = r.to || '';
    }
  }

  function _syncPresetActive() {
    if (!_dropdown) return;
    var r = getDateRange();
    var presetBtns = _dropdown.querySelectorAll('.dp-preset-btn[data-preset]');
    for (var i = 0; i < presetBtns.length; i++) {
      var btn = presetBtns[i];
      var preset = btn.getAttribute('data-preset');
      var active = false;
      if (r.mode === 'total' && preset === 'total') active = true;
      if (r.mode === 'preset' && r.preset === preset) active = true;
      btn.classList.toggle('active', active);
    }
  }

  function _refreshLabel() {
    if (_label) _label.textContent = dateRangeLabel();
    _syncPresetActive();
    _syncInputsFromState();
  }

  function _toggle(open) {
    if (!_wrapper) return;
    var next = (typeof open === 'boolean') ? open : !_wrapper.classList.contains('open');
    _wrapper.classList.toggle('open', next);
  }

  function _close() {
    _toggle(false);
  }

  function _applyPreset(preset) {
    if (preset === 'total') {
      setDateRange({ mode: 'total', preset: 'total' });
    } else {
      setDateRange({ mode: 'preset', preset: preset });
    }
    _close();
  }

  function _applyCustom() {
    if (!_fromInput || !_toInput) return;
    var from = (_fromInput.value || '').trim();
    var to = (_toInput.value || '').trim();
    if (!from || !to) {
      toast(t('common.warning') + ': ' + t('datepicker.from') + ' / ' + t('datepicker.to'), 'warning');
      return;
    }
    if (from > to) {
      toast(t('common.warning') + ': ' + t('datepicker.from') + ' <= ' + t('datepicker.to'), 'warning');
      return;
    }
    setDateRange({ mode: 'custom', from: from, to: to });
    _close();
  }

  function _bindEvents() {
    if (_btn) {
      _btn.addEventListener('click', function (e) {
        e.stopPropagation();
        _toggle();
      });
    }

    if (_dropdown) {
      _dropdown.addEventListener('click', function (e) {
        var presetBtn = e.target.closest('.dp-preset-btn[data-preset]');
        if (presetBtn) {
          _applyPreset(presetBtn.getAttribute('data-preset'));
          return;
        }
        var applyBtn = e.target.closest('#dpApplyBtn');
        if (applyBtn) {
          _applyCustom();
        }
      });
    }

    document.addEventListener('click', function (e) {
      if (_wrapper && !_wrapper.contains(e.target)) {
        _close();
      }
    });

    window.addEventListener('daterange-change', _refreshLabel);
  }

  function init() {
    if (_inited) {
      _refreshLabel();
      return;
    }

    _wrapper = document.getElementById('datePickerWrapper');
    _btn = document.getElementById('datePickerBtn');
    _label = document.getElementById('datePickerLabel');
    _dropdown = document.getElementById('datePickerDropdown');
    _fromInput = document.getElementById('dpFromInput');
    _toInput = document.getElementById('dpToInput');

    if (!_wrapper || !_btn || !_label || !_dropdown) return;

    restoreDateRange();
    _bindEvents();
    _refreshLabel();

    _inited = true;
  }

  return {
    init: init,
    refreshLabel: _refreshLabel,
  };
})();
