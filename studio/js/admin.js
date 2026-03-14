/* FlinkSQL Studio — Admin Session UI
 * Handles: topbar admin badge, Sessions button, report type UI,
 *           query count tracking hook, Apache Flink docs link in About modal.
 */

// ── Show/hide admin report options ───────────────────────────────────────────
function updateAdminReportUI() {
    const adminOpts = document.getElementById('admin-report-options');
    if (!adminOpts) return;
    adminOpts.style.display = (typeof state !== 'undefined' && state.isAdminSession) ? 'block' : 'none';
}

// ── Inject admin topbar elements ──────────────────────────────────────────────
function ensureAdminTopbarElements() {
    if (typeof state === 'undefined' || !state.isAdminSession) return;

    // Sessions button (before Disconnect)
    if (!document.getElementById('admin-sessions-topbar-btn')) {
        const actions = document.querySelector('.topbar-actions');
        if (actions) {
            const btn = document.createElement('button');
            btn.id        = 'admin-sessions-topbar-btn';
            btn.className = 'topbar-btn';
            btn.title     = 'Admin — View All Sessions Activity';
            btn.style.cssText = 'color:var(--yellow,#f5a623);border-color:rgba(245,166,35,0.3);';
            btn.innerHTML = '🛡 Sessions';
            btn.onclick   = () => { if (typeof openAdminSessionsView === 'function') openAdminSessionsView(); };
            const disc = actions.querySelector('.danger');
            if (disc) actions.insertBefore(btn, disc);
            else actions.appendChild(btn);
        }
    }

    // Statusbar admin indicator
    if (!document.getElementById('sb-admin-indicator')) {
        const sb = document.getElementById('statusbar');
        if (sb) {
            const item = document.createElement('div');
            item.id = 'sb-admin-indicator';
            item.className = 'status-item';
            item.style.cssText = 'color:var(--yellow,#f5a623);font-weight:700;cursor:pointer;';
            item.innerHTML = '🛡 ADMIN SESSION';
            item.title     = 'Admin session — all cluster jobs visible';
            item.onclick   = () => { if (typeof openAdminSettingsModal === 'function') openAdminSettingsModal(); };
            sb.appendChild(item);
        }
    }
}

// ── Remove admin-specific UI on disconnect ────────────────────────────────────
function clearAdminUI() {
    ['admin-session-badge','admin-sessions-topbar-btn','sb-admin-indicator'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.remove();
    });
    updateAdminReportUI();
}

// ── Add Apache Flink SQL docs link to About modal ────────────────────────────
function ensureFlinkDocsLink() {
    const aboutModal = document.getElementById('modal-about');
    if (!aboutModal || document.getElementById('flink-docs-link')) return;

    // Find the modal body
    const body = aboutModal.querySelector('.modal-body');
    if (!body) return;

    const docsSection = document.createElement('div');
    docsSection.id    = 'flink-docs-link';
    docsSection.style.cssText = 'background:rgba(0,151,255,0.06);border:1px solid rgba(0,151,255,0.2);padding:12px 14px;border-radius:var(--radius);';
    docsSection.innerHTML = `
    <div style="font-size:10px;color:var(--blue);font-weight:700;letter-spacing:0.5px;text-transform:uppercase;margin-bottom:8px;">📚 Learn Apache Flink SQL</div>
    <div style="display:flex;flex-direction:column;gap:6px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/"
         target="_blank" rel="noopener"
         style="font-size:12px;color:var(--blue);text-decoration:none;display:flex;align-items:center;gap:6px;">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
        Flink SQL Overview — Official Docs
      </a>
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/"
         target="_blank" rel="noopener"
         style="font-size:12px;color:var(--blue);text-decoration:none;display:flex;align-items:center;gap:6px;">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
        Table API Connectors — Kafka, Filesystem, JDBC &amp; more
      </a>
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-agg/"
         target="_blank" rel="noopener"
         style="font-size:12px;color:var(--blue);text-decoration:none;display:flex;align-items:center;gap:6px;">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
        Window Aggregation — TUMBLE, HOP, SESSION, CUMULATE
      </a>
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/"
         target="_blank" rel="noopener"
         style="font-size:12px;color:var(--blue);text-decoration:none;display:flex;align-items:center;gap:6px;">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
        Checkpointing &amp; State Backends
      </a>
    </div>`;

    // Insert before the last btn group
    const footer = aboutModal.querySelector('.modal-footer');
    if (footer) body.insertBefore(docsSection, footer);
    else body.appendChild(docsSection);
}

// ── Query counting is handled directly in execution.js via _onQuerySubmitted()
// No window.addToHistory patching needed — direct call is reliable.

// ── Main init loop ────────────────────────────────────────────────────────────
(function initAdminUI() {
    let _patched = false;

    function check() {
        if (typeof state === 'undefined') return;

        if (!_patched) {
            _patched = true;

            // Hook launchApp
            const _origLaunch = window.launchApp;
            if (typeof _origLaunch === 'function') {
                window.launchApp = function(...args) {
                    _origLaunch.apply(this, args);
                    setTimeout(() => {
                        updateAdminReportUI();
                        ensureAdminTopbarElements();
                        ensureFlinkDocsLink();
                    }, 300);
                };
            }

            // Hook disconnectAll
            const _origDisconnect = window.disconnectAll;
            if (typeof _origDisconnect === 'function') {
                window.disconnectAll = function(...args) {
                    clearAdminUI();
                    _origDisconnect.apply(this, args);
                };
            }

            // Always add docs link once modal exists
            setTimeout(ensureFlinkDocsLink, 1000);
        }

        ensureAdminTopbarElements();
        updateAdminReportUI();
    }

    setInterval(check, 800);
})();