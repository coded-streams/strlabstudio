/**
 * sql-visual-panel-hooks.js  —  Str:::lab Studio v0.0.22
 * ─────────────────────────────────────────────────────────────────
 * Hooks the SQL Visual Panel into the studio execution lifecycle.
 * Non-destructive monkey-patches — no existing files need editing.
 *
 * Key behaviours:
 *  - INSERT INTO / streaming SQL → panel animates CONTINUOUSLY
 *    (never stops until user clicks ■ Stop or cancels)
 *  - DDL / SELECT → animates then stops when results arrive
 *  - Session change / project load → resets visualiser + job graph
 *  - UDF / VIEW creation → registers the name so parser recognises
 *    future references to it in SELECT columns
 * ─────────────────────────────────────────────────────────────────
 */
(function () {
    'use strict';

    // ── Wait until panel + execution are both ready ───────────────────
    function _waitAndHook() {
        if (typeof executeSQL !== 'function' || !window._svp) {
            setTimeout(_waitAndHook, 250);
            return;
        }
        _applyHooks();
    }

    // ── Detect streaming / INSERT INTO queries ────────────────────────
    function _isStreaming(sql) {
        if (!sql) return false;
        const U = sql.replace(/--[^\n]*/g, '').toUpperCase();
        return !!(U.match(/\bINSERT\s+INTO\b/) || U.match(/\bEMIT\s+CHANGES\b/));
    }

    // ── Detect UDF/function/view creation and register with parser ────
    function _trackUserDefinitions(sql) {
        if (!sql || !window._svp) return;
        const fnRe = /CREATE\s+(?:TEMPORARY\s+)?(?:SYSTEM\s+)?(?:FUNCTION|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/gi;
        let m;
        while ((m = fnRe.exec(sql)) !== null) {
            const name = m[1].toUpperCase();
            if (window._svp._userUdfs) window._svp._userUdfs.add(name);
            // Also tell the parser state directly if accessible
            if (window._svp._addUdf) window._svp._addUdf(name);
        }
    }

    // ── Safety fallback — stop after 5 minutes max ────────────────────
    let _safetyTimer = null;
    function _setSafetyStop() {
        clearTimeout(_safetyTimer);
        _safetyTimer = setTimeout(() => {
            if (window._svp) window._svp.forceStopAnimation();
        }, 300_000); // 5 min
    }

    // ── Hook application ──────────────────────────────────────────────
    function _applyHooks() {

        // ── executeSQL ──────────────────────────────────────────────────
        const _origExec = window.executeSQL;
        window.executeSQL = function () {
            const ed  = document.getElementById('sql-editor');
            const sql = ed ? ed.value : '';
            if (window._svp && window._svp.getState() !== 'collapsed') {
                _trackUserDefinitions(sql);
                window._svp.startAnimation(_isStreaming(sql));
            }
            _setSafetyStop();
            return _origExec.apply(this, arguments);
        };

        // ── executeSelected ─────────────────────────────────────────────
        if (typeof executeSelected === 'function') {
            const _origSel = window.executeSelected;
            window.executeSelected = function () {
                const ed   = document.getElementById('sql-editor');
                const sel  = ed ? ed.value.slice(ed.selectionStart, ed.selectionEnd) || ed.value : '';
                if (window._svp && window._svp.getState() !== 'collapsed') {
                    _trackUserDefinitions(sel);
                    window._svp.startAnimation(_isStreaming(sel));
                }
                _setSafetyStop();
                return _origSel.apply(this, arguments);
            };
        }

        // ── cancelOperation ─────────────────────────────────────────────
        if (typeof cancelOperation === 'function') {
            const _origCancel = window.cancelOperation;
            window.cancelOperation = function () {
                clearTimeout(_safetyTimer);
                if (window._svp) window._svp.forceStopAnimation();
                return _origCancel.apply(this, arguments);
            };
        }

        // ── Watch #stop-btn visibility as execution-done signal ──────────
        const stopBtn = document.getElementById('stop-btn');
        if (stopBtn) {
            new MutationObserver(() => {
                const hidden = stopBtn.style.display === 'none';
                if (hidden && window._svp) window._svp.stopAnimation(); // respects looping flag
            }).observe(stopBtn, { attributes: true, attributeFilter: ['style'] });
        }

        // ── Watch result badge — when rows arrive, DDL/SELECT is done ────
        const badge = document.getElementById('result-row-badge');
        if (badge) {
            new MutationObserver(() => {
                const n = parseInt(badge.textContent, 10) || 0;
                if (n > 0 && window._svp) window._svp.stopAnimation();
            }).observe(badge, { childList: true, subtree: true, characterData: true });
        }

        // ── Watch log badge — ERR stops animation too ────────────────────
        const logBadge = document.getElementById('log-badge');
        if (logBadge) {
            new MutationObserver(() => {
                // If an error was logged while streaming, stop
                const panel = document.getElementById('log-panel');
                if (panel && panel.querySelector('.log-line-err') && window._svp) {
                    window._svp.stopAnimation();
                }
            }).observe(logBadge, { childList: true, subtree: true, characterData: true });
        }

        // ── Ctrl+` shortcut: toggle collapsed ↔ default ──────────────────
        document.addEventListener('keydown', e => {
            if (e.ctrlKey && e.key === '`') {
                e.preventDefault();
                if (!window._svp) return;
                const cur = window._svp.getState();
                window._svp.setState(cur === 'collapsed' ? 'default' : 'collapsed');
            }
        });

        // ── Tab switching — re-parse the newly loaded tab's SQL ───────────
        document.addEventListener('click', e => {
            const tabEl = e.target.closest?.('.tab[data-id]') || e.target.closest?.('.tab-btn');
            if (tabEl && window._svp) {
                setTimeout(() => {
                    const ed = document.getElementById('sql-editor');
                    if (ed && window._svp) {
                        // Force re-parse on next debounce cycle
                        window._svp._lastSqlReset?.();
                    }
                }, 80);
            }
        });
    }

    _waitAndHook();

})();