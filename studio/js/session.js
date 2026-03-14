// SESSION MANAGEMENT
// ──────────────────────────────────────────────

// ── Audit log: tracks admin actions against user sessions ────────────────────
// Format: { sessionHandle, action, adminName, timestamp, detail }
if (!window._auditLog) window._auditLog = [];

function recordAudit(sessionHandle, action, detail) {
  const adminName = (typeof state !== 'undefined' && state.adminName) ? state.adminName : 'Admin';
  const entry = {
    sessionHandle,
    action,
    adminName,
    timestamp: new Date(),
    detail: detail || '',
  };
  window._auditLog.push(entry);
  // Also mark on the affected session so normal users see the notice
  if (typeof state !== 'undefined') {
    const sess = state.sessions.find(s => s.handle === sessionHandle);
    if (sess) {
      if (!sess._auditTrail) sess._auditTrail = [];
      sess._auditTrail.push(entry);
    }
  }
}

// ── Session creation ─────────────────────────────────────────────────────────
function openNewSessionModal() {
  document.getElementById('modal-session-name').value  = '';
  document.getElementById('modal-session-props').value = '';
  openModal('modal-new-session');
}

async function createSession() {
  const name     = document.getElementById('modal-session-name').value.trim();
  const propsRaw = document.getElementById('modal-session-props').value.trim();
  const props    = parseProps(propsRaw);

  if (!state.gateway || !state.gateway.baseUrl) {
    closeModal('modal-new-session');
    toast('Not connected — please reconnect to the gateway first', 'err');
    addLog('WARN', 'Cannot create session: not connected to Flink SQL Gateway.');
    return;
  }

  try {
    const body = {};
    if (name) body.sessionName = name;
    if (Object.keys(props).length) body.properties = props;

    const resp = await api('POST', '/v1/sessions', body);
    if (!resp || !resp.sessionHandle) throw new Error('Gateway did not return a session handle.');

    // Save current session state before switching
    if (state.activeSession) {
      const cur = state.sessions.find(s => s.handle === state.activeSession);
      if (cur) {
        if (state.activeTab) {
          const tab = state.tabs.find(t => t.id === state.activeTab);
          if (tab) tab.sql = document.getElementById('sql-editor').value;
        }
        cur._savedState = {
          tabs:       JSON.parse(JSON.stringify(state.tabs)),
          activeTab:  state.activeTab,
          logLines:   state.logLines.slice(),
          operations: state.operations.slice(),
          history:    state.history.slice(),
        };
      }
    }

    state.sessions.push({
      handle:      resp.sessionHandle,
      name:        name || ('session-' + state.sessions.length),
      created:     new Date(),
      isAdmin:     false,
      jobIds:      [],
      queryCount:  0,
      _auditTrail: [],
    });
    state.activeSession = resp.sessionHandle;

    // Clean slate
    state.tabs = []; state.activeTab = null;
    state.logLines = []; state.operations = []; state.history = [];
    state.results = []; state.resultColumns = []; state.resultSlots = [];
    tabCounter = 0;
    addTab('Query 1');
    clearResults();
    document.getElementById('log-panel').innerHTML = '';
    state.logBadge = 0;
    document.getElementById('log-badge').textContent  = '0';
    document.getElementById('ops-badge').textContent  = '0';
    renderOps(); renderHistory();

    document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
    document.getElementById('sb-session').textContent        = shortHandle(state.activeSession);
    renderSessionsList();
    closeModal('modal-new-session');
    addLog('INFO', `New session started: ${shortHandle(resp.sessionHandle)} — clean workspace`);
    toast(`New session: ${shortHandle(resp.sessionHandle)}`, 'ok');
    refreshCatalog();
  } catch (e) {
    if (!state.gateway) {
      closeModal('modal-new-session');
      toast('Not connected — please reconnect to the gateway first', 'err');
    } else {
      addLog('ERR', 'Session creation failed: ' + e.message);
      toast('Session creation failed: ' + (e.message || '').slice(0, 80), 'err');
    }
  }
}

// ── Sessions list renderer ────────────────────────────────────────────────────
function renderSessionsList() {
  const list = document.getElementById('sessions-list');
  if (!list) return;

  if (state.activeSession && !state.sessions.find(s => s.handle === state.activeSession)) {
    const savedName = (() => { try { return localStorage.getItem('flinksql_last_session_name') || 'default'; } catch(_) { return 'default'; } })();
    state.sessions.push({ handle: state.activeSession, name: savedName, created: new Date(), jobIds: [], queryCount: 0, _auditTrail: [] });
  }

  // Admin: pull ALL sessions from gateway so other users' sessions appear in the sidebar
  if (state.isAdminSession && state.gateway) {
    list.innerHTML = '<div class="tree-loading" style="font-size:10px;">Loading all cluster sessions…</div>';
    _loadGatewaySessionsForAdmin().then(() => _renderSessionsListInner(list)).catch(() => _renderSessionsListInner(list));
    return;
  }

  _renderSessionsListInner(list);
}

// Fetch all sessions from the SQL Gateway (discovers sessions from other users)
async function _loadGatewaySessionsForAdmin() {
  try {
    const resp = await api('GET', '/v1/sessions');
    const gwList = resp.sessions || resp || [];
    if (!Array.isArray(gwList)) return;
    gwList.forEach(gws => {
      const handle = gws.sessionHandle || gws.handle || (typeof gws === 'string' ? gws : null);
      if (!handle) return;
      const name = gws.sessionName || gws.name || '';
      if (!state.sessions.find(s => s.handle === handle)) {
        state.sessions.push({
          handle,
          name:         name || shortHandle(handle),
          created:      new Date(),
          isAdmin:      false,
          jobIds:       [],
          queryCount:   0,
          _auditTrail:  [],
          _fromGateway: true,   // discovered from gateway, not created in this browser
        });
      }
    });
  } catch(e) {
    // /v1/sessions listing may not be supported on all gateway versions — silent fail
    addLog('INFO', 'Note: gateway did not return a session list (GET /v1/sessions not supported). Only locally registered sessions are shown.');
  }
}

function _renderSessionsListInner(list) {
  if (!list) return;

  if (state.sessions.length === 0) {
    list.innerHTML = '<div class="tree-loading">No sessions — click Connect to start one.</div>';
    return;
  }

  // Count live jobs per session
  const liveJobs = (typeof perf !== 'undefined' && perf.lastJobs) ? perf.lastJobs : [];

  list.innerHTML = state.sessions.map(s => {
    const isActive  = s.handle === state.activeSession;
    const isAdmin   = s.isAdmin;
    const jobCount  = (s.jobIds || []).length;
    const runJobs   = liveJobs.filter(j => (s.jobIds || []).includes(j.jid) && j.state === 'RUNNING').length;
    const qCount    = s.queryCount || (s._savedState ? (s._savedState.history || []).length : 0);
    const hasAudit  = (s._auditTrail || []).length > 0;

    return `
    <div class="session-item" style="${isActive ? 'border-left:2px solid var(--accent);' : ''}${isAdmin ? 'background:rgba(245,166,35,0.04);' : ''}">
      <div class="session-item-header">
        <span class="session-item-id" title="${s.handle}">${isAdmin ? '🛡 ' : ''}${shortHandle(s.handle)}</span>
        ${isActive ? '<span class="session-item-active">● ACTIVE</span>' : ''}
        ${isAdmin  ? '<span style="font-size:9px;background:rgba(245,166,35,0.15);color:var(--yellow,#f5a623);padding:1px 5px;border-radius:2px;">ADMIN</span>' : ''}
        ${hasAudit && !isAdmin ? '<span style="font-size:9px;background:rgba(255,77,109,0.12);color:var(--red);padding:1px 5px;border-radius:2px;" title="Admin made changes to this session">⚠ Admin activity</span>' : ''}
      </div>
      <div class="session-item-meta" style="font-size:10px;color:var(--text2);">
        ${escHtml(s.name || 'default')} · ${s.created instanceof Date ? s.created.toLocaleTimeString() : '—'}
      </div>
      <!-- Live stats row -->
      <div style="display:flex;gap:10px;margin-top:4px;font-size:10px;font-family:var(--mono);">
        <span title="Queries run" style="color:var(--text3);">📋 <span style="color:var(--text1);">${qCount}</span> queries</span>
        <span title="Jobs submitted" style="color:var(--text3);">⚡ <span style="color:${runJobs>0?'var(--green)':'var(--text1)'};">${runJobs}</span>/<span style="color:var(--text1);">${jobCount}</span> jobs</span>
      </div>
      ${hasAudit && !isAdmin ? `
      <div style="margin-top:5px;font-size:10px;background:rgba(255,77,109,0.06);border:1px solid rgba(255,77,109,0.2);padding:4px 8px;border-radius:2px;color:var(--text2);">
        ${(s._auditTrail || []).slice(-1).map(a =>
        `🛡 ${escHtml(a.adminName)} — ${escHtml(a.action)} at ${a.timestamp instanceof Date ? a.timestamp.toLocaleTimeString() : '—'}`
    ).join('')}
      </div>` : ''}
      <div class="session-item-actions" style="margin-top:5px;">
        ${!isActive ? `<button class="mini-btn" onclick="switchSession('${s.handle}')">Switch</button>` : ''}
        ${state.isAdminSession ? `<button class="mini-btn" onclick="openAdminSessionDetail('${s.handle}')">Inspect</button>` : ''}
        <button class="mini-btn" onclick="viewSessionJobs('${s.handle}')">Jobs</button>
        <button class="mini-btn danger" onclick="deleteSession('${s.handle}')">Delete</button>
      </div>
    </div>`;
  }).join('');

  // Update session count badge in cluster stats if visible
  _updateSessionCountBadge();
}  // end _renderSessionsListInner

// ── Update cluster session count display ──────────────────────────────────────
function _updateSessionCountBadge() {
  const el = document.getElementById('cs-sessions-count');
  if (el) el.textContent = state.sessions.length || '—';
}

// ── Admin: inspect individual session detail modal ────────────────────────────
function openAdminSessionDetail(handle) {
  const sess = state.sessions.find(s => s.handle === handle);
  if (!sess) return;

  let modal = document.getElementById('modal-admin-session-detail');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'modal-admin-session-detail';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
      <div class="modal" style="width:680px;max-height:88vh;display:flex;flex-direction:column;">
        <div class="modal-header" style="background:linear-gradient(135deg,rgba(245,166,35,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(245,166,35,0.2);">
          <span style="color:var(--yellow,#f5a623);display:flex;align-items:center;gap:8px;font-size:13px;">
            🛡 Session Inspector
            <span id="asd-session-name" style="font-size:11px;color:var(--text2);font-family:var(--mono);"></span>
          </span>
          <button class="modal-close" onclick="closeModal('modal-admin-session-detail')">×</button>
        </div>
        <div style="display:flex;gap:0;border-bottom:1px solid var(--border);">
          <button class="mini-btn" id="asd-tab-overview"  onclick="switchAsdTab('overview')"  style="flex:1;border-radius:0;padding:8px;font-size:10px;background:var(--accent);color:#000;border:none;">Overview</button>
          <button class="mini-btn" id="asd-tab-queries"   onclick="switchAsdTab('queries')"   style="flex:1;border-radius:0;padding:8px;font-size:10px;background:var(--bg3);border:none;">Queries</button>
          <button class="mini-btn" id="asd-tab-jobs"      onclick="switchAsdTab('jobs')"      style="flex:1;border-radius:0;padding:8px;font-size:10px;background:var(--bg3);border:none;">Jobs</button>
          <button class="mini-btn" id="asd-tab-audit"     onclick="switchAsdTab('audit')"     style="flex:1;border-radius:0;padding:8px;font-size:10px;background:var(--bg3);border:none;">Audit Log</button>
        </div>
        <div class="modal-body" style="flex:1;overflow-y:auto;padding:0;">
          <div id="asd-pane-overview" style="padding:16px;"></div>
          <div id="asd-pane-queries"  style="padding:16px;display:none;"></div>
          <div id="asd-pane-jobs"     style="padding:16px;display:none;"></div>
          <div id="asd-pane-audit"    style="padding:16px;display:none;"></div>
        </div>
        <div class="modal-footer" style="justify-content:space-between;">
          <div style="display:flex;gap:8px;">
            <button class="btn btn-secondary" style="font-size:11px;" onclick="refreshAsdContent()">⟳ Refresh</button>
          </div>
          <button class="btn btn-primary" onclick="closeModal('modal-admin-session-detail')">Close</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-admin-session-detail'); });
  }

  // Store current session handle being inspected
  modal._inspectingHandle = handle;
  document.getElementById('asd-session-name').textContent = `${escHtml(sess.name || shortHandle(handle))} (${shortHandle(handle)})`;
  switchAsdTab('overview');
  openModal('modal-admin-session-detail');
}

function switchAsdTab(tab) {
  ['overview','queries','jobs','audit'].forEach(t => {
    const btn  = document.getElementById(`asd-tab-${t}`);
    const pane = document.getElementById(`asd-pane-${t}`);
    if (btn)  { btn.style.background  = t === tab ? 'var(--accent)' : 'var(--bg3)'; btn.style.color = t === tab ? '#000' : 'var(--text2)'; }
    if (pane) pane.style.display = t === tab ? 'block' : 'none';
  });
  refreshAsdContent(tab);
}

async function refreshAsdContent(tab) {
  const modal = document.getElementById('modal-admin-session-detail');
  if (!modal) return;
  const handle = modal._inspectingHandle;
  const sess   = handle ? state.sessions.find(s => s.handle === handle) : null;
  if (!sess) return;

  // For the ACTIVE session live data lives in state.* directly.
  // For non-active sessions it lives in sess._savedState.
  const isActiveSession = (handle === state.activeSession);
  const liveJobs  = (typeof perf !== 'undefined' && perf.lastJobs) ? perf.lastJobs : [];
  const sessJobs  = liveJobs.filter(j => (sess.jobIds || []).includes(j.jid));
  const savedSt   = isActiveSession ? {} : (sess._savedState || {});
  const history   = isActiveSession ? (state.history || []) : (savedSt.history || []);
  const tabs      = isActiveSession ? (state.tabs    || []) : (savedSt.tabs    || []);
  const qCount    = isActiveSession ? (state.history || []).length : (sess.queryCount || history.length);
  const activeTab = tab || 'overview';

  // ── Overview pane ──────────────────────────────────────────────────────────
  if (activeTab === 'overview') {
    const runJobs  = sessJobs.filter(j => j.state === 'RUNNING').length;
    const failJobs = sessJobs.filter(j => j.state === 'FAILED').length;
    document.getElementById('asd-pane-overview').innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px;">
        ${_asdStat('Queries Run',    qCount,             'var(--accent)')}
        ${_asdStat('Jobs Submitted', (sess.jobIds||[]).length, 'var(--blue)')}
        ${_asdStat('Running Jobs',   runJobs,            'var(--green)')}
        ${_asdStat('Failed Jobs',    failJobs,           failJobs>0?'var(--red)':'var(--text3)')}
      </div>
      <div style="margin-bottom:12px;">
        <div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Session Info</div>
        ${_asdRow('Session Handle', handle)}
        ${_asdRow('Session Name',   sess.name || '—')}
        ${_asdRow('Created',        sess.created instanceof Date ? sess.created.toLocaleString() : '—')}
        ${_asdRow('Active Tabs',    tabs.length)}
        ${_asdRow('Type',           sess.isAdmin ? '🛡 Admin' : 'Regular')}
      </div>
      <div>
        <div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Open Tabs</div>
        ${tabs.length === 0
        ? '<div style="font-size:11px;color:var(--text3);">No tabs recorded</div>'
        : tabs.map(t => `
            <div style="display:flex;align-items:center;gap:8px;padding:5px 8px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
              <span style="color:var(--text3);">📄</span>
              <span style="color:var(--text0);flex:1;">${escHtml(t.name || 'Untitled')}</span>
              <span style="color:var(--text3);font-size:10px;font-family:var(--mono);">${(t.sql||'').split('\n').length} lines</span>
            </div>`).join('')}
      </div>`;
  }

  // ── Queries pane ───────────────────────────────────────────────────────────
  if (activeTab === 'queries') {
    document.getElementById('asd-pane-queries').innerHTML = history.length === 0
        ? '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No query history for this session</div>'
        : `<div style="font-size:10px;color:var(--text3);margin-bottom:10px;">${history.length} queries executed in this session</div>` +
        history.slice().reverse().map((h, i) => `
          <div style="padding:8px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:6px;">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
              <span style="font-size:9px;padding:1px 6px;border-radius:2px;font-family:var(--mono);font-weight:700;
                background:${h.status==='ok'?'rgba(57,211,83,0.15)':'rgba(255,77,109,0.15)'};
                color:${h.status==='ok'?'var(--green)':'var(--red)'};">${h.status?.toUpperCase()||'—'}</span>
              <span style="font-size:10px;color:var(--text3);font-family:var(--mono);">${h.ts ? new Date(h.ts).toLocaleTimeString() : '—'}</span>
            </div>
            <div style="font-size:11px;color:var(--text1);font-family:var(--mono);white-space:pre-wrap;word-break:break-all;max-height:60px;overflow:hidden;">${escHtml((h.sql||'').slice(0,200))}${(h.sql||'').length>200?'…':''}</div>
          </div>`).join('');
  }

  // ── Jobs pane ──────────────────────────────────────────────────────────────
  if (activeTab === 'jobs') {
    document.getElementById('asd-pane-jobs').innerHTML = sessJobs.length === 0
        ? '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No jobs attributed to this session</div>'
        : sessJobs.map(j => `
          <div style="display:flex;align-items:center;gap:10px;padding:8px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:6px;font-size:11px;">
            <span style="width:8px;height:8px;border-radius:50%;flex-shrink:0;background:${j.state==='RUNNING'?'var(--green)':j.state==='FAILED'?'var(--red)':'var(--text3)'};${j.state==='RUNNING'?'box-shadow:0 0 5px var(--green);':''};"></span>
            <span style="flex:1;color:var(--text0);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(j.name||j.jid)}">${escHtml((j.name||j.jid).slice(0,45))}</span>
            <span style="color:var(--text3);font-family:var(--mono);font-size:10px;">${j.jid.slice(0,8)}…</span>
            <span style="font-size:9px;padding:1px 6px;border-radius:2px;
              background:${j.state==='RUNNING'?'rgba(57,211,83,0.15)':j.state==='FAILED'?'rgba(255,77,109,0.15)':'rgba(100,100,100,0.2)'};
              color:${j.state==='RUNNING'?'var(--green)':j.state==='FAILED'?'var(--red)':'var(--text2)'};">${j.state}</span>
            ${j.state==='RUNNING'?`
              <button onclick="
                recordAudit('${handle}','Cancel Job','Job ${j.jid.slice(0,8)}');
                showCancelJobConfirm('${j.jid}','${escHtml((j.name||'').replace(/'/g,"\\'"))}');
                closeModal('modal-admin-session-detail');"
                style="font-size:9px;padding:1px 6px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.08);color:var(--red);cursor:pointer;">
                Cancel
              </button>` : ''}
          </div>`).join('');
  }

  // ── Audit log pane ─────────────────────────────────────────────────────────
  if (activeTab === 'audit') {
    // Merge: global audit log entries for this session + session-local trail
    const auditFromGlobal = (window._auditLog || []).filter(a => a.sessionHandle === handle);
    const auditLocal      = sess._auditTrail || [];
    const auditSeen = new Set();
    const allAudit  = [...auditFromGlobal, ...auditLocal].filter(a => {
      const key = String(a.timestamp) + a.action;
      if (auditSeen.has(key)) return false;
      auditSeen.add(key); return true;
    }).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    document.getElementById('asd-pane-audit').innerHTML = allAudit.length === 0
        ? '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No admin actions recorded yet for this session.<br><span style="font-size:10px;">Actions (cancel job, delete session) will appear here.</span></div>'
        : `<div style="font-size:10px;color:var(--text3);margin-bottom:10px;">${allAudit.length} admin action(s) recorded</div>` +
        allAudit.map(a => `
          <div style="display:flex;gap:10px;padding:8px 10px;background:rgba(245,166,35,0.05);border:1px solid rgba(245,166,35,0.2);border-radius:var(--radius);margin-bottom:6px;font-size:11px;">
            <span style="font-size:14px;flex-shrink:0;">🛡</span>
            <div style="flex:1;">
              <div style="font-weight:600;color:var(--yellow,#f5a623);margin-bottom:2px;">${escHtml(a.action)}</div>
              <div style="color:var(--text2);">By <strong style="color:var(--text1);">${escHtml(a.adminName)}</strong> at ${a.timestamp instanceof Date ? a.timestamp.toLocaleString() : '—'}</div>
              ${a.detail ? `<div style="color:var(--text3);font-size:10px;margin-top:2px;">${escHtml(a.detail)}</div>` : ''}
            </div>
          </div>`).join('');
  }
}

function _asdStat(label, val, color) {
  return `<div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;text-align:center;">
    <div style="font-size:22px;font-weight:700;color:${color};font-family:var(--mono);line-height:1.1;">${val}</div>
    <div style="font-size:10px;color:var(--text3);margin-top:3px;text-transform:uppercase;letter-spacing:0.5px;">${label}</div>
  </div>`;
}

function _asdRow(label, val) {
  return `<div style="display:flex;justify-content:space-between;padding:5px 8px;border-bottom:1px solid var(--border);font-size:11px;">
    <span style="color:var(--text3);">${label}</span>
    <span style="color:var(--text1);font-family:var(--mono);">${escHtml(String(val))}</span>
  </div>`;
}

// ── View session jobs ─────────────────────────────────────────────────────────
function viewSessionJobs(handle) {
  const session = state.sessions.find(s => s.handle === handle);
  if (!session) return;
  const jobIds = session.jobIds || [];
  if (jobIds.length === 0) { toast('No jobs tracked for this session yet', 'info'); return; }
  const jgBtn = document.getElementById('jobgraph-tab-btn');
  if (jgBtn) switchResultTab('jobgraph', jgBtn);
  refreshJobGraphList();
  toast(`Showing jobs for session ${shortHandle(handle)}`, 'info');
}

// ── Switch session ────────────────────────────────────────────────────────────
function switchSession(handle) {
  if (handle === state.activeSession) return;
  if (state.currentOp) {
    const { opHandle, sessionHandle: oldSess } = state.currentOp;
    state.currentOp = null;
    api('DELETE', `/v1/sessions/${oldSess || state.activeSession}/operations/${opHandle}/cancel`).catch(()=>{});
    document.getElementById('stop-btn').style.display = 'none';
    setExecuting(false);
  }

  const prevHandle = state.activeSession;
  if (prevHandle) {
    const curSession = state.sessions.find(s => s.handle === prevHandle);
    if (curSession) {
      if (state.activeTab) {
        const cur = state.tabs.find(t => t.id === state.activeTab);
        if (cur) cur.sql = document.getElementById('sql-editor').value;
      }
      curSession._savedState = {
        tabs:       JSON.parse(JSON.stringify(state.tabs)),
        activeTab:  state.activeTab,
        logLines:   state.logLines.slice(),
        operations: state.operations.slice(),
        history:    state.history.slice(),
      };
    }
  }

  state.activeSession = handle;
  document.getElementById('topbar-session-id').textContent = shortHandle(handle);
  document.getElementById('sb-session').textContent        = shortHandle(handle);

  const newSession = state.sessions.find(s => s.handle === handle);
  if (newSession && newSession._savedState) {
    const saved = newSession._savedState;
    state.tabs = saved.tabs; state.activeTab = saved.activeTab;
    state.logLines = saved.logLines; state.operations = saved.operations;
    state.history  = saved.history;
    _syncTabCounter();
    renderTabs();
    const tab = state.tabs.find(t => t.id === state.activeTab);
    document.getElementById('sql-editor').value = tab ? (tab.sql || '') : '';
    updateLineNumbers(); renderLog(); renderHistory(); renderOps();
    toast(`Switched to session ${shortHandle(handle)} — workspace restored`, 'info');
  } else {
    state.tabs = []; state.activeTab = null; state.logLines = [];
    state.operations = []; state.history = []; tabCounter = 0;
    addTab('Query 1'); clearResults();
    document.getElementById('log-panel').innerHTML = '';
    state.logBadge = 0;
    document.getElementById('log-badge').textContent = '0';
    document.getElementById('ops-badge').textContent = '0';
    renderOps(); renderHistory();
    addLog('INFO', `Switched to session ${shortHandle(handle)} — clean workspace`);
    toast(`Switched to session ${shortHandle(handle)} — fresh workspace`, 'info');
  }

  renderSessionsList();
  refreshCatalog();
}

// ── Delete session ────────────────────────────────────────────────────────────
async function deleteSession(handle) {
  if (!confirm(`Delete session ${shortHandle(handle)}?`)) return;
  // Admin audit trail
  if (state.isAdminSession && handle !== state.activeSession) {
    recordAudit(handle, 'Session Deleted', `Admin ${state.adminName||'Admin'} deleted this session`);
  }
  try {
    await api('DELETE', `/v1/sessions/${handle}`);
    state.sessions = state.sessions.filter(s => s.handle !== handle);
    if (state.activeSession === handle) {
      state.activeSession = state.sessions.length ? state.sessions[0].handle : null;
      document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
      document.getElementById('sb-session').textContent        = shortHandle(state.activeSession);
    }
    renderSessionsList();
    toast('Session deleted', 'ok');
  } catch (e) {
    toast('Delete failed: ' + e.message, 'err');
  }
}

// ── Job scoping ───────────────────────────────────────────────────────────────
function registerJobForSession(jobId) {
  if (!jobId || !state.activeSession) return;
  const session = state.sessions.find(s => s.handle === state.activeSession);
  if (session) {
    if (!session.jobIds) session.jobIds = [];
    if (!session.jobIds.includes(jobId)) session.jobIds.push(jobId);
  }
  if (!state._jobSessionMap) state._jobSessionMap = {};
  state._jobSessionMap[jobId] = state.activeSession;
  renderSessionsList();
}

function filterJobsForCurrentSession(jobs) {
  if (!jobs || !Array.isArray(jobs)) return [];
  if (state.isAdminSession) return jobs;
  const session = state.sessions.find(s => s.handle === state.activeSession);
  const sessionJobIds = (session && session.jobIds) ? session.jobIds : [];
  if (sessionJobIds.length === 0) return [];
  return jobs.filter(j => sessionJobIds.includes(j.jid));
}

// ── Query count tracking ──────────────────────────────────────────────────────
function incrementSessionQueryCount() {
  const session = state.sessions.find(s => s.handle === state.activeSession);
  if (session) {
    session.queryCount = (session.queryCount || 0) + 1;
    // Keep renderSessionsList lightweight — only update count badge, not full re-render
    _updateSessionCountBadge();
  }
}

// Direct hook: called from execution.js submitStatement after operation starts
// This ensures counts are accurate without relying on window.addToHistory patching
function _onQuerySubmitted() {
  incrementSessionQueryCount();
}

// ── Duplicate submission guard ────────────────────────────────────────────────
let _runningStatements = new Set();

function _sqlHash(sql) {
  let h = 0;
  const s = sql.trim().replace(/\s+/g, ' ').toLowerCase();
  for (let i = 0; i < s.length; i++) { h = ((h << 5) - h) + s.charCodeAt(i); h |= 0; }
  return String(h);
}

function checkDuplicateSubmission(sql) { return _runningStatements.has(_sqlHash(sql)); }
function markStatementRunning(sql)     { _runningStatements.add(_sqlHash(sql)); }
function unmarkStatementRunning(sql)   { _runningStatements.delete(_sqlHash(sql)); }

// ── Cancel job with confirmation modal ───────────────────────────────────────
function showCancelJobConfirm(jid, jobName) {
  let modal = document.getElementById('modal-cancel-confirm');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'modal-cancel-confirm';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
      <div class="modal" style="width:440px;">
        <div class="modal-header" style="color:var(--red);">
          ⚠ Cancel Job
          <button class="modal-close" onclick="closeModal('modal-cancel-confirm')">×</button>
        </div>
        <div class="modal-body" style="display:flex;flex-direction:column;gap:14px;">
          <div style="background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.25);padding:12px 14px;border-radius:var(--radius);">
            <div style="font-size:13px;font-weight:600;color:var(--red);margin-bottom:6px;">Are you sure you want to cancel this job?</div>
            <div style="font-size:12px;color:var(--text1);font-family:var(--mono);" id="cancel-job-name-label"></div>
            <div style="font-size:10px;color:var(--text2);margin-top:6px;line-height:1.7;">
              This will send a cancel signal to the Flink JobManager.<br>
              Running state will be discarded. This action <strong>cannot be undone</strong>.
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-cancel-confirm')">Keep Running</button>
          <button class="btn" style="background:var(--red);color:#fff;border:none;" id="cancel-job-confirm-btn">Cancel Job</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-cancel-confirm'); });
  }
  const nameLabel = document.getElementById('cancel-job-name-label');
  if (nameLabel) nameLabel.textContent = `Job ID: ${jid.slice(0,8)}…  ${jobName ? '| ' + jobName.slice(0,40) : ''}`;
  const confirmBtn = document.getElementById('cancel-job-confirm-btn');
  if (confirmBtn) {
    confirmBtn.onclick = async () => {
      closeModal('modal-cancel-confirm');
      await _doCancelJob(jid);
    };
  }
  openModal('modal-cancel-confirm');
}

async function _doCancelJob(jid) {
  try {
    await jmApi(`/jobs/${jid}/yarn-cancel`);
    addLog('WARN', `Job ${jid.slice(0,8)}… cancel requested`);
    toast('Job cancel requested', 'info');
    setTimeout(refreshJobGraphList, 1500);
  } catch(e) {
    try {
      await fetch(`${state.gateway?.baseUrl || ''}/jobmanager-api/jobs/${jid}`, { method: 'PATCH' });
      addLog('WARN', `Job ${jid.slice(0,8)}… cancel signal sent`);
      toast('Job cancel signal sent', 'info');
      setTimeout(refreshJobGraphList, 1500);
    } catch(e2) {
      addLog('ERR', `Cancel failed: ${e2.message}`);
      toast('Cancel failed', 'err');
    }
  }
}

// ── Catalog / database quick-insert ──────────────────────────────────────────
function insertCatalogSnippet(type) {
  const ed = document.getElementById('sql-editor');
  const prefix = ed.value.length > 0 ? '\n\n' : '';
  const templates = {
    catalog:  `-- Create a named in-memory catalog and switch to it\nCREATE CATALOG my_catalog WITH ('type' = 'generic_in_memory');\nUSE CATALOG my_catalog;\n\nCREATE DATABASE IF NOT EXISTS my_db;\nUSE my_db;`,
    database: `-- Create a database in the current catalog\nCREATE DATABASE IF NOT EXISTS my_db;\nUSE my_db;`,
  };
  const sql = templates[type] || '';
  const s = ed.selectionStart;
  ed.value = ed.value.slice(0, s) + prefix + sql + '\n' + ed.value.slice(ed.selectionEnd);
  ed.focus();
  updateLineNumbers();
  toast(`${type === 'catalog' ? 'CREATE CATALOG' : 'CREATE DATABASE'} template inserted`, 'ok');
}

function openSnippets(filterTag) {
  const list  = document.getElementById('snippets-list');
  const items = filterTag ? SNIPPETS.filter(s => s.tag === filterTag) : SNIPPETS;
  list.innerHTML = items.map((s, i) => {
    const idx = filterTag ? SNIPPETS.indexOf(s) : i;
    return `
    <div class="snippet-item" onclick="insertSnippet(${idx})">
      <div class="snippet-title">${escHtml(s.title)}</div>
      <div class="snippet-desc">${escHtml(s.desc)}</div>
      <span class="snippet-tag ${escHtml(s.tag)}">${escHtml(s.tag)}</span>
    </div>`;
  }).join('');
  openModal('modal-snippets');
}

function insertSnippet(idx) {
  const snip = SNIPPETS[idx];
  const ed   = document.getElementById('sql-editor');
  const s    = ed.selectionStart;
  const prefix = ed.value.length > 0 ? '\n\n' : '';
  ed.value = ed.value.slice(0, s) + prefix + snip.sql + '\n' + ed.value.slice(ed.selectionEnd);
  ed.focus();
  updateLineNumbers();
  closeModal('modal-snippets');
  toast(`Snippet inserted: ${snip.title}`, 'ok');
}

// ── Resizer ───────────────────────────────────────────────────────────────────
(function setupResizer() {
  const resizer      = document.getElementById('v-resizer');
  const resultsPanel = document.getElementById('results-panel');
  let startY, startH;
  resizer.addEventListener('mousedown', e => {
    startY = e.clientY; startH = resultsPanel.offsetHeight;
    resizer.classList.add('dragging');
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup',   onUp);
    e.preventDefault();
  });
  function onMove(e) {
    const delta = startY - e.clientY;
    const newH  = Math.max(80, Math.min(window.innerHeight * 0.8, startH + delta));
    resultsPanel.style.height = newH + 'px';
  }
  function onUp() {
    resizer.classList.remove('dragging');
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup',   onUp);
  }
})();

// ── Result tabs ───────────────────────────────────────────────────────────────
function switchResultTab(tab, el) {
  document.getElementById('result-data-tab').style.display = tab === 'data'     ? 'flex'  : 'none';
  document.getElementById('log-panel').style.display        = tab === 'log'      ? 'block' : 'none';
  document.getElementById('ops-panel').style.display        = tab === 'ops'      ? 'block' : 'none';
  const perfEl = document.getElementById('perf-panel');
  if (perfEl) perfEl.classList.toggle('active', tab === 'perf');
  const jgEl   = document.getElementById('job-graph-panel');
  if (jgEl)   jgEl.classList.toggle('active', tab === 'jobgraph');
  document.querySelectorAll('.result-tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  if (tab === 'log') {
    state.logBadge = 0;
    document.getElementById('log-badge').textContent = '0';
    const logBtn = document.getElementById('export-log-btn');
    const resBtn = document.getElementById('export-results-btn');
    if (logBtn) logBtn.style.display = 'inline-flex';
    if (resBtn) resBtn.style.display = 'none';
  } else {
    const logBtn = document.getElementById('export-log-btn');
    const resBtn = document.getElementById('export-results-btn');
    if (logBtn) logBtn.style.display = 'none';
    if (resBtn) resBtn.style.display = 'inline-flex';
  }
  if (tab === 'perf') {
    refreshPerf();
    if (typeof switchPerfTab === 'function') switchPerfTab('overview');
  }
  if (tab === 'jobgraph') refreshJobGraphList();
}

// ── Sidebar tabs ──────────────────────────────────────────────────────────────
function switchSidebarTab(idx, btn) {
  document.querySelectorAll('.sidebar-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.sidebar-tab').forEach(t => t.classList.remove('active'));
  document.getElementById(`sidebar-panel-${idx}`).classList.add('active');
  btn.classList.add('active');
  if (idx === 2) renderSessionsList();
}

// ── Modal helpers ─────────────────────────────────────────────────────────────
function openModal(id)  { const el = document.getElementById(id); if (el) el.classList.add('open');    }
function closeModal(id) { const el = document.getElementById(id); if (el) el.classList.remove('open'); }

document.querySelectorAll('.modal-overlay').forEach(m => {
  m.addEventListener('click', e => { if (e.target === m) m.classList.remove('open'); });
});

// ── Toast ─────────────────────────────────────────────────────────────────────
let toastTimer;
function toast(msg, type = 'info') {
  const el    = document.getElementById('toast');
  const icons = { ok: '✓', err: '✗', info: 'ℹ' };
  el.className = `show ${type}`;
  el.innerHTML = `<span>${icons[type]}</span> ${escHtml(msg)}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.className = ''; }, 3500);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }