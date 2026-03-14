// CONNECTION
// ──────────────────────────────────────────────

// ── Admin session config ──────────────────────────────────────────────────────
// Passcode stored in localStorage — changeable from within the admin session
// via the 🛡 Settings badge after connecting.
const ADMIN_DEFAULT_PASSCODE = 'admin1234';
let _adminPasscode = (() => {
  try { return localStorage.getItem('flinksql_admin_pass') || ADMIN_DEFAULT_PASSCODE; } catch(_) { return ADMIN_DEFAULT_PASSCODE; }
})();

function toggleProps() {
  const area = document.getElementById('props-area');
  area.classList.toggle('open');
  const btn = document.querySelector('.props-toggle');
  btn.textContent = area.classList.contains('open') ? '▼ Advanced properties' : '▶ Advanced properties';
}

function setConnectStatus(type, msg) {
  const el = document.getElementById('connect-status');
  el.className = `connect-status show ${type}`;
  const icons = { ok: '✓', err: '✗', loading: '' };
  el.innerHTML = type === 'loading'
      ? `<div class="spinner"></div> ${msg}`
      : `<span>${icons[type]}</span> ${msg}`;
}

let connMode = 'proxy';
let authMode = 'bearer';

function setMode(mode) {
  connMode = mode;
  ['proxy','direct','remote'].forEach(m => {
    const btn = document.getElementById('mode-' + m);
    if (btn) btn.classList.toggle('active', m === mode);
    const info = document.getElementById('mode-' + m + '-info');
    if (info) info.style.display = (m === mode) ? 'block' : 'none';
  });
}

function setAuthMode(mode) {
  authMode = mode;
  ['none','bearer','basic'].forEach(m => {
    const btn = document.getElementById('auth-' + m);
    if (btn) btn.classList.toggle('active', m === mode);
  });
  const bearerF = document.getElementById('auth-bearer-fields');
  const basicF  = document.getElementById('auth-basic-fields');
  if (bearerF) bearerF.style.display = mode === 'bearer' ? 'block' : 'none';
  if (basicF)  basicF.style.display  = mode === 'basic'  ? 'block' : 'none';
}

function getBaseUrl() {
  if (connMode === 'proxy') return `${window.location.protocol}//${window.location.host}/flink-api`;
  if (connMode === 'remote') {
    const url = (document.getElementById('inp-remote-url')?.value || '').trim();
    return url.replace(/\/$/, '');
  }
  const host = document.getElementById('inp-host')?.value.trim() || 'localhost';
  const port = document.getElementById('inp-port')?.value.trim() || '8084';
  return `http://${host}:${port}`;
}

function getAuthHeaders() {
  if (connMode !== 'remote') return {};
  if (authMode === 'bearer') {
    const token = (document.getElementById('inp-token')?.value || '').trim();
    return token ? { 'Authorization': 'Bearer ' + token } : {};
  }
  if (authMode === 'basic') {
    const user = (document.getElementById('inp-basic-user')?.value || '').trim();
    const pass = (document.getElementById('inp-basic-pass')?.value || '').trim();
    if (user) return { 'Authorization': 'Basic ' + btoa(user + ':' + pass) };
  }
  return {};
}

async function testConnection() {
  const baseUrl = getBaseUrl();
  setConnectStatus('loading', `Testing ${baseUrl}/v1/info …`);
  try {
    const resp = await fetchWithTimeout(`${baseUrl}/v1/info`, { headers: { 'Accept': 'application/json' } }, 8000);
    if (!resp.ok) {
      let msg = `HTTP ${resp.status}`;
      if (resp.status === 502) msg = `502 — Flink SQL Gateway still starting. Wait 30s and retry.`;
      if (resp.status === 503) msg = `503 — Flink SQL Gateway not ready yet.`;
      setConnectStatus('err', msg);
      return;
    }
    const info = await resp.json();
    const ver = info.flinkVersion || info.version || 'unknown';
    setConnectStatus('ok', `✓ Gateway reachable! &nbsp; Apache Flink ${ver} &nbsp; — click Connect`);
    toast('Connection test passed', 'ok');
    // Also load existing sessions now
    loadExistingSessionsFromGateway(baseUrl);
  } catch (e) {
    let hint = '';
    if (connMode === 'proxy') {
      hint = `<br>Make sure you opened the IDE at <strong>http://localhost:3030</strong> (not as a local file).<br>Run: <strong>docker compose ps</strong> — flink-studio and flink-sql-gateway must be Up.`;
    } else {
      hint = `<br>Check host/port and that the CORS proxy is running.`;
    }
    setConnectStatus('err', `${e.message}${hint}`);
  }
}

// ── Load existing sessions from gateway for the "Use Existing" dropdown ──────
async function loadExistingSessionsFromGateway(baseUrl) {
  try {
    const url = baseUrl || getBaseUrl();
    const resp = await fetchWithTimeout(`${url}/v1/sessions`, { headers: { Accept: 'application/json' } }, 6000);
    if (!resp.ok) return;
    const data = await resp.json();
    const sessions = data.sessions || data || [];
    if (!Array.isArray(sessions) || sessions.length === 0) return;

    // Populate dropdown in existing-session area
    let sel = document.getElementById('existing-sessions-select');
    if (!sel) {
      // Create the dropdown if not present
      const existArea = document.getElementById('sess-existing-area');
      if (existArea) {
        const wrap = document.createElement('div');
        wrap.style.cssText = 'margin-top:8px;';
        wrap.innerHTML = `<label class="field-label" style="font-size:10px;color:var(--text2);letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;display:block;">Existing Sessions</label>
          <select id="existing-sessions-select" class="field-input" style="font-size:11px;cursor:pointer;">
            <option value="">— Select a session —</option>
          </select>
          <div style="font-size:10px;color:var(--text3);margin-top:3px;">Select to auto-fill handle, or paste manually above</div>`;
        existArea.appendChild(wrap);
        sel = document.getElementById('existing-sessions-select');
        if (sel) {
          sel.addEventListener('change', () => {
            const handleInput = document.getElementById('inp-session-handle');
            if (handleInput && sel.value) handleInput.value = sel.value;
          });
        }
      }
    }
    if (!sel) return;
    sel.innerHTML = '<option value="">— Select a session —</option>';
    sessions.forEach(s => {
      const handle = s.sessionHandle || s.handle || s;
      const name   = s.sessionName   || s.name   || '';
      const opt = document.createElement('option');
      opt.value = handle;
      opt.textContent = name ? `${name} (${handle.slice(0,8)}…)` : `${handle.slice(0,8)}…`;
      sel.appendChild(opt);
    });
  } catch(_) {}
}

// ── Connect screen session mode toggle ───────────────────────────────────────
let _sessionMode = 'new'; // 'new' | 'existing' | 'admin'
function setSessionMode(mode) {
  _sessionMode = mode;
  const newArea   = document.getElementById('sess-new-area');
  const exArea    = document.getElementById('sess-existing-area');
  const adminArea = document.getElementById('sess-admin-area');
  const newBtn    = document.getElementById('sess-mode-new');
  const existBtn  = document.getElementById('sess-mode-existing');
  const adminBtn  = document.getElementById('sess-mode-admin');

  [newArea, exArea, adminArea].forEach(a => { if (a) a.style.display = 'none'; });
  [newBtn, existBtn, adminBtn].forEach(b => {
    if (b) { b.style.background = 'var(--bg3)'; b.style.color = 'var(--text2)'; b.style.fontWeight = ''; }
  });

  const activeArea = mode === 'new' ? newArea : mode === 'existing' ? exArea : adminArea;
  const activeBtn  = mode === 'new' ? newBtn  : mode === 'existing' ? existBtn : adminBtn;
  if (activeArea) activeArea.style.display = '';
  if (activeBtn)  { activeBtn.style.background = 'var(--accent)'; activeBtn.style.color = '#000'; activeBtn.style.fontWeight = '700'; }

  // Load sessions if switching to existing
  if (mode === 'existing') loadExistingSessionsFromGateway();
}

async function doConnect() {
  const baseUrl   = getBaseUrl();
  const host      = connMode === 'proxy' ? window.location.hostname : (document.getElementById('inp-host')?.value.trim() || 'localhost');
  const port      = connMode === 'proxy' ? (window.location.port || '80') : (document.getElementById('inp-port')?.value.trim() || '8084');
  const sessionName = (document.getElementById('inp-session-name')?.value || '').trim();
  const propsRaw  = (document.getElementById('inp-props')?.value || '').trim();

  setConnectStatus('loading', `Connecting via ${baseUrl} …`);
  document.getElementById('connect-btn').disabled = true;

  // Temporarily set gateway so api() works during connect
  state.gateway = { host, port, baseUrl };

  try {
    // Verify gateway reachable
    const verifyResp = await fetchWithTimeout(`${baseUrl}/v1/info`, { headers: { Accept: 'application/json' } }, 8000);
    if (!verifyResp.ok) throw new Error(`Gateway returned HTTP ${verifyResp.status} — is it fully started?`);

    if (_sessionMode === 'admin') {
      // ── Admin session ──────────────────────────────────────────────────────
      const passcode = (document.getElementById('inp-admin-pass')?.value || '').trim();
      if (!passcode) throw new Error('Admin passcode is required.');
      if (passcode !== _adminPasscode) throw new Error('Incorrect admin passcode.');

      const sessResp = await api('POST', '/v1/sessions', { sessionName: 'admin-session' });
      state.activeSession  = sessResp.sessionHandle;
      state.isAdminSession = true;
      state.adminName      = (document.getElementById('inp-admin-name')?.value || '').trim() || 'Admin';
      state.sessions = [{ handle: sessResp.sessionHandle, name: 'admin-session', created: new Date(), isAdmin: true }];
      launchApp(host, port);
      toast(`Admin session started — welcome, ${state.adminName}`, 'ok');
      addLog('OK', `Admin session created: ${shortHandle(sessResp.sessionHandle)}`);

    } else if (_sessionMode === 'existing') {
      // ── Reconnect to existing session ──────────────────────────────────────
      const existingHandle = (document.getElementById('inp-session-handle')?.value || '').trim();
      if (!existingHandle || !existingHandle.includes('-')) {
        throw new Error('Please paste a valid session UUID handle.');
      }
      setConnectStatus('loading', 'Verifying existing session…');
      try {
        await api('POST', `/v1/sessions/${existingHandle}/heartbeat`);
      } catch(e) {
        throw new Error(`Session '${existingHandle.slice(0,8)}…' does not exist or has expired. Create a new session instead.`);
      }
      state.activeSession  = existingHandle;
      state.isAdminSession = false;
      state.sessions = [{ handle: existingHandle, name: 'reconnected', created: new Date() }];
      launchApp(host, port);
      toast('Reconnected to existing session', 'ok');
      addLog('OK', `Reconnected to session ${existingHandle.slice(0,8)}…`);

    } else {
      // ── Create new session (default) ───────────────────────────────────────
      const props = parseProps(propsRaw);
      const sessionBody = {};
      if (sessionName) sessionBody.sessionName = sessionName;
      if (Object.keys(props).length) sessionBody.properties = props;

      const sessResp = await api('POST', '/v1/sessions', sessionBody);
      if (!sessResp || !sessResp.sessionHandle) throw new Error('Gateway did not return a session handle. Check gateway logs.');
      state.activeSession  = sessResp.sessionHandle;
      state.isAdminSession = false;
      state.sessions = [{ handle: sessResp.sessionHandle, name: sessionName || 'default', created: new Date() }];
      launchApp(host, port);
      toast(`Session '${sessionName || sessResp.sessionHandle.slice(0,8)}' created`, 'ok');
    }
  } catch (e) {
    setConnectStatus('err', `Failed: ${e.message}`);
    document.getElementById('connect-btn').disabled = false;
    state.gateway = null;
  }
}

function parseProps(raw) {
  const props = {};
  (raw || '').split('\n').forEach(line => {
    const idx = line.indexOf('=');
    if (idx > 0) {
      const k = line.slice(0, idx).trim();
      const v = line.slice(idx + 1).trim();
      if (k) props[k] = v;
    }
  });
  return props;
}

function launchApp(host, port) {
  document.getElementById('connect-screen').style.display = 'none';
  document.getElementById('app').classList.add('visible');

  state.gateway = { host, port, baseUrl: getBaseUrl() };

  // Update topbar / statusbar
  document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
  state._lastSessionHandle = state.activeSession;
  document.getElementById('topbar-host-label').textContent = `${host}:${port}`;
  document.getElementById('sb-host').textContent = `${host}:${port}`;
  document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
  state.activeCatalog  = 'default_catalog';
  state.activeDatabase = 'default';
  updateCatalogStatus('default_catalog', 'default');

  // Admin badge in topbar
  _applyAdminUI();

  try {
    const savedTheme = localStorage.getItem('flinksql_theme');
    if (savedTheme) state.theme = savedTheme;
  } catch(_) {}
  applyTheme();

  // Workspace restore logic
  const savedSessionName = (() => { try { return localStorage.getItem('flinksql_last_session_name') || ''; } catch(_) { return ''; } })();
  const sessionName = (() => { try { return document.getElementById('inp-session-name')?.value.trim() || ''; } catch(_) { return ''; } })();
  const isNewNamedSession = sessionName && sessionName !== savedSessionName;

  if (isNewNamedSession) {
    if (state.tabs.length > 0) {
      try { localStorage.setItem('flinksql_workspace_backup', JSON.stringify({ tabs: state.tabs, savedAt: Date.now() })); } catch(_) {}
    }
    state.tabs = []; state.activeTab = null; state.history = [];
    state.logLines = []; state.operations = []; state.results = []; state.resultColumns = [];
    window._workspaceRestored = true;
    addTab('Query 1');
    setTimeout(() => toast(`New session "${sessionName}" — fresh workspace started`, 'ok'), 500);
    try { localStorage.setItem('flinksql_last_session_name', sessionName); } catch(_) {}
  } else {
    if (!window._workspaceRestored) {
      restoreWorkspace();
      window._workspaceRestored = true;
      if (state.tabs.length > 0) {
        const count = state.tabs.length;
        setTimeout(() => toast(`${count} tab${count>1?'s':''} restored`, 'info'), 800);
      }
    }
    if (sessionName) { try { localStorage.setItem('flinksql_last_session_name', sessionName); } catch(_) {} }
  }

  if (state.tabs.length === 0) addTab('Query 1');
  else renderTabs();

  refreshCatalog();
  renderSessionsList();
  renderHistory();
  startHeartbeat();
  if (typeof startResourceMonitor === 'function') startResourceMonitor();

  addLog('INFO', `Connected to Flink SQL Gateway. ${state.isAdminSession ? 'ADMIN session active — full cluster visibility enabled.' : 'Session scoped to your jobs only.'}`);

  // Show tips modal after a brief delay (first load only, or every time per IntelliJ behaviour)
  setTimeout(() => showTipsModal(), 1200);
}

// ── Apply admin UI indicators ────────────────────────────────────────────────
function _applyAdminUI() {
  // Admin badge in topbar
  let badge = document.getElementById('admin-session-badge');
  if (state.isAdminSession) {
    if (!badge) {
      badge = document.createElement('div');
      badge.id = 'admin-session-badge';
      badge.style.cssText = `
        display:flex;align-items:center;gap:6px;padding:2px 10px;
        background:rgba(245,166,35,0.15);border:1px solid rgba(245,166,35,0.4);
        border-radius:3px;font-size:10px;font-weight:700;color:var(--yellow);
        letter-spacing:0.5px;cursor:pointer;
      `;
      badge.title = 'Admin session — click to manage';
      badge.innerHTML = `🛡 ADMIN &nbsp;<span style="font-size:9px;opacity:0.7;">${escHtml(state.adminName || 'Admin')}</span>`;
      badge.addEventListener('click', openAdminSettingsModal);
      const topbarActions = document.querySelector('.topbar-actions');
      if (topbarActions) topbarActions.prepend(badge);
    }
  } else {
    if (badge) badge.remove();
  }

  // Show/hide change-passcode item in topbar
  const sbItem = document.getElementById('sb-admin-indicator');
  if (state.isAdminSession) {
    if (!sbItem) {
      const sb = document.getElementById('statusbar');
      if (sb) {
        const item = document.createElement('div');
        item.id = 'sb-admin-indicator';
        item.className = 'status-item';
        item.style.cssText = 'color:var(--yellow);font-weight:700;cursor:pointer;';
        item.innerHTML = '🛡 ADMIN SESSION';
        item.title = 'Admin session — all cluster jobs visible';
        sb.appendChild(item);
      }
    }
  } else {
    const existing = document.getElementById('sb-admin-indicator');
    if (existing) existing.remove();
  }
}

// ── Admin Settings Modal ─────────────────────────────────────────────────────
function openAdminSettingsModal() {
  let modal = document.getElementById('modal-admin-settings');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'modal-admin-settings';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
      <div class="modal" style="width:480px;">
        <div class="modal-header" style="color:var(--yellow,#f5a623);">
          🛡 Admin Settings
          <button class="modal-close" onclick="closeModal('modal-admin-settings')">×</button>
        </div>
        <div class="modal-body" style="display:flex;flex-direction:column;gap:14px;">
          <div style="background:rgba(245,166,35,0.08);border:1px solid rgba(245,166,35,0.25);padding:10px 12px;border-radius:var(--radius,3px);font-size:12px;color:var(--yellow,#f5a623);line-height:1.7;">
            <strong>Admin Session:</strong> <span id="admin-modal-session-id" style="font-family:var(--mono);"></span><br>
            <strong>Admin Name:</strong> <span id="admin-modal-name"></span>
          </div>
          <div class="modal-field">
            <label class="field-label">Admin Display Name</label>
            <input class="field-input" id="admin-name-input" placeholder="Admin" style="font-size:12px;" />
          </div>
          <div class="modal-field">
            <label class="field-label">Change Admin Passcode</label>
            <input class="field-input" id="admin-old-pass"     type="password" placeholder="Current passcode"        style="font-size:12px;margin-bottom:6px;" />
            <input class="field-input" id="admin-new-pass"     type="password" placeholder="New passcode (min 6 chars)" style="font-size:12px;margin-top:6px;" />
            <input class="field-input" id="admin-confirm-pass" type="password" placeholder="Confirm new passcode"       style="font-size:12px;margin-top:6px;" />
            <div id="admin-pass-status" style="font-size:11px;margin-top:6px;min-height:16px;"></div>
          </div>
          <div style="background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:10px 12px;border-radius:var(--radius,3px);font-size:11px;color:var(--text1);line-height:1.7;">
            <strong style="color:var(--accent);">Admin capabilities:</strong><br>
            • View ALL jobs running on the cluster across all sessions<br>
            • See per-session activity, queries, and job history<br>
            • Cancel any job on the cluster<br>
            • Generate Technical or Business/Management PDF reports<br>
            • Tag all submitted jobs with session name for traceability
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary" onclick="closeModal('modal-admin-settings')">Close</button>
          <button class="btn btn-primary" style="background:var(--yellow,#f5a623);color:#000;" onclick="saveAdminSettings()">Save Changes</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-admin-settings'); });
  }
  document.getElementById('admin-modal-session-id').textContent = shortHandle(state.activeSession);
  document.getElementById('admin-modal-name').textContent = state.adminName || 'Admin';
  document.getElementById('admin-name-input').value  = state.adminName || 'Admin';
  document.getElementById('admin-old-pass').value     = '';
  document.getElementById('admin-new-pass').value     = '';
  document.getElementById('admin-confirm-pass').value = '';
  document.getElementById('admin-pass-status').textContent = '';
  openModal('modal-admin-settings');
}

function saveAdminSettings() {
  const newName  = document.getElementById('admin-name-input').value.trim();
  const oldPass  = document.getElementById('admin-old-pass').value;
  const newPass  = document.getElementById('admin-new-pass').value;
  const confPass = document.getElementById('admin-confirm-pass').value;
  const statusEl = document.getElementById('admin-pass-status');

  if (newName) {
    state.adminName = newName;
    const badge = document.getElementById('admin-session-badge');
    if (badge) badge.innerHTML = `🛡 ADMIN &nbsp;<span style="font-size:9px;opacity:0.7;">${escHtml(newName)}</span>`;
  }

  if (oldPass || newPass || confPass) {
    if (oldPass !== _adminPasscode) {
      statusEl.style.color = 'var(--red)'; statusEl.textContent = '✗ Current passcode is incorrect.'; return;
    }
    if (newPass.length < 6) {
      statusEl.style.color = 'var(--red)'; statusEl.textContent = '✗ New passcode must be at least 6 characters.'; return;
    }
    if (newPass !== confPass) {
      statusEl.style.color = 'var(--red)'; statusEl.textContent = '✗ Passcodes do not match.'; return;
    }
    _adminPasscode = newPass;
    try { localStorage.setItem('flinksql_admin_pass', newPass); } catch(_) {}
    statusEl.style.color = 'var(--green)'; statusEl.textContent = '✓ Passcode updated successfully.';
    toast('Admin passcode updated', 'ok');
    return;
  }

  closeModal('modal-admin-settings');
  toast('Admin settings saved', 'ok');
}

function shortHandle(h) {
  if (!h) return '—';
  return h.length > 12 ? h.slice(0, 8) + '…' : h;
}

function disconnectAll(silent = false, prefillName = '') {
  if (!silent && !confirm('Disconnect from Flink SQL Gateway?')) return;
  state.pollTimer && clearInterval(state.pollTimer);
  if (_heartbeatTimer) { clearInterval(_heartbeatTimer); _heartbeatTimer = null; }
  _catalogGen++;
  state.currentOp = null;
  state.gateway = null;
  state.activeSession = null;
  state.isAdminSession = false;
  document.getElementById('app').classList.remove('visible');
  document.getElementById('connect-screen').style.display = 'flex';
  document.getElementById('connect-status').className = 'connect-status';
  document.getElementById('connect-btn').disabled = false;
  const stopBtn = document.getElementById('stop-btn');
  if (stopBtn) stopBtn.style.display = 'none';
  // Remove admin badge
  const badge = document.getElementById('admin-session-badge');
  if (badge) badge.remove();
  const sbAdmin = document.getElementById('sb-admin-indicator');
  if (sbAdmin) sbAdmin.remove();
  // Pre-fill session name
  const nameInput = document.getElementById('inp-session-name');
  const savedName = prefillName || (() => { try { return localStorage.getItem('flinksql_last_session_name') || ''; } catch(_) { return ''; } })();
  if (nameInput && savedName) nameInput.value = savedName;
  const handleInput = document.getElementById('inp-session-handle');
  if (handleInput && state._lastSessionHandle) handleInput.value = state._lastSessionHandle;
  if (typeof setSessionMode === 'function') setSessionMode('new');
  setConnectStatus('ok', silent
      ? 'Idle disconnect. Your tabs are preserved — reconnect to continue.'
      : 'Disconnected. You can reconnect.');
}

// ──────────────────────────────────────────────
// HEARTBEAT + SESSION GUARD
// ──────────────────────────────────────────────
let _heartbeatTimer = null;
let _lastActivityTime = Date.now();
const IDLE_EXPIRE_MS = 30 * 60 * 1000;

function _touchActivity() { _lastActivityTime = Date.now(); }
if (typeof document !== 'undefined') {
  ['keydown','mousedown','click'].forEach(evt =>
      document.addEventListener(evt, _touchActivity, { passive: true })
  );
}

function _hasRunningJobs() {
  return (state.operations || []).some(op => op.status === 'running' || op.status === 'RUNNING');
}

function startHeartbeat() {
  if (_heartbeatTimer) clearInterval(_heartbeatTimer);
  _heartbeatTimer = setInterval(async () => {
    if (!state.activeSession || !state.gateway) return;

    const idleSecs = (Date.now() - _lastActivityTime) / 1000;
    const hasJobs  = _hasRunningJobs();
    if (!hasJobs && idleSecs > IDLE_EXPIRE_MS / 1000) {
      const sname = (() => { try { return localStorage.getItem('flinksql_last_session_name') || ''; } catch(_) { return ''; } })();
      addLog('WARN', `Auto-disconnecting after ${Math.round(idleSecs/60)}min idle.`);
      toast('Idle disconnect — click reconnect to resume', 'info');
      disconnectAll(true, sname);
      return;
    }
    try {
      await api('POST', `/v1/sessions/${state.activeSession}/heartbeat`);
    } catch (e) {
      const msg = e.message || '';
      if (msg.includes('404') || msg.includes('does not exist') || msg.includes('Session')) {
        showSessionExpiredBanner();
      }
    }
  }, 30000);
}

function showSessionExpiredBanner() {
  if (document.getElementById('session-expired-banner')) return;
  const banner = document.createElement('div');
  banner.id = 'session-expired-banner';
  banner.style.cssText = `
    position:fixed;top:0;left:0;right:0;z-index:9999;
    background:rgba(245,166,35,0.95);color:#111;
    font-family:var(--mono);font-size:12px;font-weight:600;
    padding:10px 16px;display:flex;align-items:center;gap:12px;
    border-bottom:2px solid #c88000;box-shadow:0 2px 12px rgba(0,0,0,0.4);
  `;
  banner.innerHTML = `
    <span style="font-size:16px;">⚠</span>
    <span>Your Flink session has expired. TEMPORARY tables are gone — you need to re-run your CREATE TABLE statements.</span>
    <button onclick="renewSession()" style="
      margin-left:auto;background:#111;color:#f5a623;border:1px solid #c88000;
      font-family:var(--mono);font-size:11px;font-weight:700;padding:4px 12px;
      border-radius:3px;cursor:pointer;white-space:nowrap;
    ">Create New Session</button>
    <button onclick="this.closest('#session-expired-banner').remove()" style="
      background:none;border:none;color:#111;font-size:18px;cursor:pointer;padding:0 4px;
    ">×</button>`;
  document.body.prepend(banner);
  addLog('WARN', 'Session expired — TEMPORARY tables are no longer available. Click "Create New Session" or re-connect.');
  toast('Session expired — re-run your CREATE TABLE statements', 'err');
}

async function renewSession() {
  const banner = document.getElementById('session-expired-banner');
  if (banner) banner.remove();
  if (!state.gateway) {
    toast('Not connected — please use the Connect screen', 'err');
    return;
  }
  try {
    const sessResp = await api('POST', '/v1/sessions', { sessionName: 'renewed-' + Date.now() });
    state.activeSession = sessResp.sessionHandle;
    state.sessions = [{ handle: sessResp.sessionHandle, name: 'renewed', created: new Date() }];
    document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
    document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
    renderSessionsList();
    addLog('OK', `New session created: ${shortHandle(state.activeSession)}`);
    addLog('WARN', 'Remember: all TEMPORARY tables are gone. Re-run your CREATE TABLE statements before running pipelines.');
    toast('New session ready — re-run CREATE TABLE statements', 'info');
    try {
      await api('POST', `/v1/sessions/${state.activeSession}/statements`, { statement: 'USE CATALOG default_catalog', executionTimeout: 0 });
      await api('POST', `/v1/sessions/${state.activeSession}/statements`, { statement: 'USE `default`', executionTimeout: 0 });
    } catch(_) {}
  } catch(e) {
    addLog('ERR', `Failed to create new session: ${e.message}`);
    toast('Could not create session — try reconnecting', 'err');
  }
}

// ── Tips Modal ────────────────────────────────────────────────────────────────

// ── Tips Modal ────────────────────────────────────────────────────────────────
const TIPS_DATA = [
  // ── GETTING STARTED ──────────────────────────────────────────────────────
  {
    icon: '⚡', category: 'Getting Started',
    title: 'Run your first query',
    body: `Type <code>SHOW CATALOGS;</code> and press <kbd>Ctrl+Enter</kbd>. Results stream into the Results tab below. Use the <strong>Snippets</strong> button for the Recommended Streaming Config — run it first every session.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Run this at the start of every session
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default'    = '2';
SHOW CATALOGS;</pre>`
  },
  {
    icon: '🗂', category: 'Getting Started',
    title: 'Multi-tab pipeline workflow',
    body: `Organise complex pipelines across tabs so each statement group runs independently:
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Tab 1: Session Config  (SET statements)
-- Tab 2: DDL             (CREATE TABLE)
-- Tab 3: Producer        (INSERT INTO ... SELECT FROM datagen)
-- Tab 4: Consumer        (SELECT * FROM kafka_source)
-- Tab 5: Aggregation     (INSERT INTO ... TUMBLE window)</pre>
Double-click any tab to rename it. <kbd>Ctrl+Enter</kbd> runs all SQL in the active tab.`
  },
  {
    icon: '▶', category: 'Getting Started',
    title: 'Run only selected SQL',
    body: `Highlight any portion of SQL and click <strong>▶ Selection</strong> to run just that fragment — essential when a tab has multiple CREATE TABLE statements you want to run one at a time.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Select just this line and press Ctrl+Shift+Enter:
CREATE TABLE orders (...) WITH ('connector' = 'kafka', ...);</pre>`
  },

  // ── IDE TIPS ─────────────────────────────────────────────────────────────
  {
    icon: '💾', category: 'IDE Tips',
    title: 'Export your workspace — never lose scripts',
    body: `Flink sessions are ephemeral — restarts wipe them. Your SQL scripts auto-save to <code>localStorage</code> and can be exported as JSON via <strong>Workspace → Export</strong>. After a restart: import the file → all tabs and history are restored.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- After importing workspace, re-run your DDL:
USE CATALOG default_catalog;
USE \`default\`;
-- Then re-run your CREATE TABLE statements</pre>`
  },
  {
    icon: '📊', category: 'IDE Tips',
    title: 'Performance tab — live cluster metrics',
    body: `The <strong>Performance</strong> tab shows: records in/out per second, backpressure gauges, slot utilisation, checkpoint health, and per-query timing bars. Toggle <strong>▶ Live</strong> to auto-refresh every 2 seconds while jobs are running.`
  },
  {
    icon: '◈', category: 'IDE Tips',
    title: 'Job Graph — visualise your running pipeline',
    body: `After INSERT INTO, the IDE auto-switches to the <strong>Job Graph</strong> tab and renders your pipeline DAG live. Node colours: SOURCE (blue), PROCESS (green), SINK (amber). Double-click any node for detailed per-operator metrics including backpressure, records/s, and JVM heap.`
  },

  // ── FLINK ARCHITECTURE ────────────────────────────────────────────────────
  {
    icon: '🏗', category: 'Flink Architecture',
    title: 'JobManager + TaskManagers',
    body: `Flink clusters have one <strong>JobManager</strong> (coordinator: scheduling, checkpoints, failure recovery) and one or more <strong>TaskManagers</strong> (workers). Each TaskManager provides a fixed number of <em>slots</em> — one slot runs one parallel task instance. The SQL Gateway is a separate process that translates SQL into dataflow jobs and submits them to the JobManager.`
  },
  {
    icon: '🔀', category: 'Flink Architecture',
    title: 'Dataflow graphs and operators',
    body: `A Flink SQL query compiles into a <strong>dataflow graph</strong>: operators connected by streams. Each operator runs in parallel. Exchange strategies (HASH, FORWARD, REBALANCE) control how data moves between operators.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- This single SQL compiles to a 3-operator DAG:
-- [KafkaSource] → [FilterProject] → [KafkaSink]
INSERT INTO output_topic
SELECT user_id, SUM(amount) AS total
FROM payments_source
WHERE status = 'APPROVED'
GROUP BY user_id;</pre>`
  },

  // ── FLINK CONCEPTS ────────────────────────────────────────────────────────
  {
    icon: '💧', category: 'Flink Concepts',
    title: 'Watermarks and event time',
    body: `Watermarks tell Flink how far behind real time your stream is. Without them, time-based windows never trigger.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">CREATE TABLE orders (
  order_id   STRING,
  amount     DOUBLE,
  event_time TIMESTAMP(3),
  -- Allow up to 5s of late-arriving data:
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH ('connector' = 'kafka', ...);</pre>`
  },
  {
    icon: '🪟', category: 'Flink Concepts',
    title: 'Window types: Tumble, Hop, Session',
    body: `<strong>TUMBLE</strong> — fixed, non-overlapping. <strong>HOP</strong> — sliding/overlapping. <strong>SESSION</strong> — gap-based.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Tumble: one bucket per minute
SELECT window_start, COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start;

-- Hop: 5-minute buckets that slide every 1 minute
SELECT window_start, window_end, SUM(amount)
FROM TABLE(HOP(TABLE orders, DESCRIPTOR(event_time),
    INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
GROUP BY window_start, window_end;</pre>`
  },
  {
    icon: '💾', category: 'Flink Concepts',
    title: 'State and checkpointing',
    body: `Stateful operators (aggregations, joins, deduplication) store state locally. Checkpoints snapshot state to durable storage for fault tolerance.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Enable checkpointing every 10 seconds:
SET 'execution.checkpointing.interval' = '10000';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
-- Set state backend to filesystem (S3/MinIO):
SET 'state.backend'         = 'filesystem';
SET 'state.checkpoints.dir' = 's3://my-bucket/checkpoints';</pre>`
  },
  {
    icon: '🔄', category: 'Flink Concepts',
    title: 'Deduplication and Top-N patterns',
    body: `Use ROW_NUMBER() to deduplicate streams or rank records within a group:
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Keep only the first occurrence of each order_id:
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY event_time ASC
  ) AS rn FROM orders
) WHERE rn = 1;

-- Top 3 products by sales per category:
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY category ORDER BY total_sales DESC
  ) AS rn FROM product_sales
) WHERE rn <= 3;</pre>`
  },

  // ── CONNECTORS ────────────────────────────────────────────────────────────
  {
    icon: '📨', category: 'Connectors',
    title: 'Connecting to Apache Kafka',
    body: `Kafka is the most common Flink source and sink. The connector JAR must match your Flink version (e.g. <code>flink-sql-connector-kafka-3.3.0-1.19.jar</code>).
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">CREATE TABLE kafka_events (
  event_id   STRING,
  user_id    STRING,
  amount     DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'user-events',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'flink-consumer-01',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);</pre>`
  },
  {
    icon: '📋', category: 'Connectors',
    title: 'Connecting to Confluent Schema Registry',
    body: `Use Avro with Confluent Schema Registry for schema evolution and type safety across Kafka topics.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">CREATE TABLE payments_avro (
  payment_id STRING,
  amount     DOUBLE,
  currency   STRING,
  ts         TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'payments',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'flink-payments-01',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = 'http://schemaregistry0-01:8081',
  'scan.startup.mode'            = 'earliest-offset'
);</pre>
Requires: <code>flink-sql-avro-confluent-registry-&lt;ver&gt;.jar</code> and <code>flink-sql-avro-&lt;ver&gt;.jar</code> in <code>/opt/flink/lib/</code>.`
  },
  {
    icon: '🪣', category: 'Connectors',
    title: 'Checkpointing and sinking to S3 / MinIO',
    body: `Use the filesystem connector to write results to S3 or MinIO in Parquet/ORC/CSV format, and checkpoint state there for fault tolerance.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- 1. Set checkpoint backend to S3/MinIO
SET 'state.backend'         = 'filesystem';
SET 'state.checkpoints.dir' = 's3://flink-checkpoints/cp';

-- 2. Create a filesystem sink (Parquet, partitioned by date)
CREATE TABLE trade_archive (
  trade_date STRING,
  symbol     STRING,
  volume     DOUBLE,
  total_usd  DOUBLE
) PARTITIONED BY (trade_date)
WITH (
  'connector'           = 'filesystem',
  'path'                = 's3://my-data-lake/trades/',
  'format'              = 'parquet',
  'sink.rolling-policy.rollover-interval' = '10 min',
  'sink.partition-commit.delay'           = '1 min',
  'sink.partition-commit.policy.kind'     = 'success-file'
);</pre>`
  },
  {
    icon: '🔍', category: 'Connectors',
    title: 'Connecting to Elasticsearch',
    body: `Stream enriched or aggregated results directly into Elasticsearch for dashboards and search.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">CREATE TABLE es_trade_metrics (
  symbol      STRING,
  window_time TIMESTAMP(3),
  trade_count BIGINT,
  total_value DOUBLE,
  PRIMARY KEY (symbol) NOT ENFORCED
) WITH (
  'connector'                = 'elasticsearch-7',
  'hosts'                    = 'http://elasticsearch:9200',
  'index'                    = 'trade-metrics',
  'sink.bulk-flush.interval' = '5s',
  'sink.bulk-flush.max-actions' = '1000'
);

-- Now stream windowed aggregates into Elasticsearch:
INSERT INTO es_trade_metrics
SELECT symbol,
       window_start AS window_time,
       COUNT(*)     AS trade_count,
       SUM(amount)  AS total_value
FROM TABLE(TUMBLE(TABLE trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY symbol, window_start;</pre>`
  },

  // ── AI / ML WORKLOADS ─────────────────────────────────────────────────────
  {
    icon: '🤖', category: 'AI & ML Workloads',
    title: 'Real-time ML inference with a hosted model',
    body: `Call a hosted ML model (REST API, ONNX server, TensorFlow Serving, SageMaker, Vertex AI) from within a Flink pipeline using a <strong>User-Defined Function (UDF)</strong>. The UDF makes an HTTP call per record (or micro-batch) and returns the prediction as a column.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- 1. Register a UDF that calls your model endpoint
-- (UDF jar must be on the classpath: /opt/flink/lib/)
CREATE FUNCTION predict_risk AS
  'com.mycompany.flink.RiskScoringUDF';

-- 2. Use it inline in your pipeline
SELECT
  trade_id,
  symbol,
  amount,
  predict_risk(symbol, amount, volume, volatility) AS risk_score
FROM enriched_trades_source
WHERE predict_risk(symbol, amount, volume, volatility) > 0.75;</pre>
The UDF calls <code>POST https://your-model-api/predict</code> and returns the score. Cache model weights in the UDF constructor to avoid per-call network overhead.`
  },
  {
    icon: '🧠', category: 'AI & ML Workloads',
    title: 'Feature engineering pipelines for ML',
    body: `Use Flink SQL to build real-time feature stores — aggregate raw events into ML features and sink them to Redis, Kafka, or a feature store (Feast, Tecton).
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Compute rolling 5-minute user features for fraud detection:
INSERT INTO user_features_sink
SELECT
  user_id,
  COUNT(*)           AS txn_count_5m,
  SUM(amount)        AS total_spend_5m,
  AVG(amount)        AS avg_txn_5m,
  MAX(amount)        AS max_txn_5m,
  COUNT(DISTINCT merchant_id) AS unique_merchants_5m,
  window_end         AS feature_time
FROM TABLE(HOP(TABLE transactions,
    DESCRIPTOR(event_time),
    INTERVAL '1' MINUTE,
    INTERVAL '5' MINUTE))
GROUP BY user_id, window_start, window_end;</pre>`
  },
  {
    icon: '🔗', category: 'AI & ML Workloads',
    title: 'Connecting AI workloads via Kafka',
    body: `A common pattern: Flink processes and enriches the stream, publishes results to a Kafka topic, and a Python AI service consumes predictions. Flink handles the high-throughput streaming; the AI service handles model inference.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">-- Flink: enrich + publish features to Kafka
CREATE TABLE ml_features_kafka (
  entity_id   STRING,
  feature_1   DOUBLE,
  feature_2   DOUBLE,
  feature_3   DOUBLE,
  ts          TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'ml-features',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json'
);

INSERT INTO ml_features_kafka
SELECT entity_id, feat_a, feat_b, feat_c, event_time
FROM enriched_stream;

-- Python AI service: kafka-python consumer → model.predict()
-- → publishes predictions back to 'ml-predictions' topic

-- Flink: join predictions back into the main stream
SELECT s.entity_id, s.amount, p.score, p.label
FROM stream s JOIN predictions_source p
  ON s.entity_id = p.entity_id AND p.ts BETWEEN s.ts - INTERVAL '5' SECOND AND s.ts + INTERVAL '5' SECOND;</pre>`
  },

  // ── PERFORMANCE TIPS ──────────────────────────────────────────────────────
  {
    icon: '⚡', category: 'Performance Tips',
    title: 'MiniBatch aggregation',
    body: `MiniBatch buffers input rows before processing aggregations — dramatically reduces state access overhead on high-throughput topics.
<pre style="margin-top:10px;background:var(--bg0);border:1px solid var(--border);padding:10px 12px;border-radius:4px;font-size:11px;color:var(--text1);">SET 'table.exec.mini-batch.enabled'       = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500 ms';
SET 'table.exec.mini-batch.size'          = '5000';
-- Also enable two-phase (local-global) aggregation:
SET 'table.optimizer.agg-phase-strategy'  = 'TWO_PHASE';</pre>`
  },
  {
    icon: '🚫', category: 'Performance Tips',
    title: 'Avoid duplicate pipeline submissions',
    body: `FlinkSQL Studio blocks re-submitting the same INSERT INTO while it's already RUNNING. But if you restart the IDE, the guard resets. Always check the <strong>Job Graph</strong> tab before submitting — if the job is RUNNING, do not re-submit. Duplicate pipelines consume double the slots, produce duplicate records in your sinks, and cause state corruption.`
  },

  // ── ADMIN ─────────────────────────────────────────────────────────────────
  {
    icon: '🛡', category: 'Admin',
    title: 'Admin session — full cluster visibility',
    body: `Connect with the <strong>🛡️ Admin</strong> button on the connect screen and enter the admin passcode (default: <code>admin1234</code> — change it after first login via the 🛡️ badge in the topbar).
<ul style="margin:10px 0 0 16px;font-size:12px;line-height:1.9;">
  <li>See ALL jobs running across all sessions on the cluster</li>
  <li>Inspect any session: queries run, jobs submitted, open tabs, audit log</li>
  <li>Cancel any running job on the cluster (with confirmation dialog)</li>
  <li>Generate Technical or Business/Management PDF reports</li>
</ul>
Every admin action (cancel, delete, inspect) is recorded with the admin's name and timestamp.`
  },
];


// ── Tips shuffle queue ────────────────────────────────────────────────────────
// Picks tips in random order. Once every tip has been shown, shuffles again
// so no tip repeats until the whole deck has been seen — like a card deck.
// The queue resets only when showTipsModal() is called (i.e. modal reopened).
let _tipsQueue   = [];   // remaining indices in this shuffle pass
let _tipsCurrent = null; // index of the tip currently on screen

function _shuffledQueue() {
  const indices = TIPS_DATA.map((_, i) => i);
  // Fisher-Yates shuffle
  for (let i = indices.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [indices[i], indices[j]] = [indices[j], indices[i]];
  }
  return indices;
}

function showTipsModal() {
  let modal = document.getElementById('modal-tips');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'modal-tips';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
      <div class="modal" style="width:600px;max-height:88vh;display:flex;flex-direction:column;overflow:hidden;">
        <!-- Fixed header -->
        <div class="modal-header" style="border-bottom:1px solid var(--border2);padding:14px 20px;flex-shrink:0;">
          <div style="display:flex;flex-direction:column;gap:3px;">
            <div style="font-size:14px;font-weight:700;color:var(--text0);">Tips &amp; Concepts</div>
            <div style="font-size:10px;color:var(--accent);letter-spacing:1px;text-transform:uppercase;" id="tips-category">Getting Started</div>
          </div>
          <button class="modal-close" onclick="closeModal('modal-tips')" style="font-size:20px;">×</button>
        </div>
        <!-- Scrollable body — grows to fill space between header and footer -->
        <div style="flex:1;overflow-y:auto;padding:20px 24px 8px;min-height:0;">
          <div style="font-size:15px;font-weight:600;color:var(--text0);margin-bottom:10px;" id="tips-title"></div>
          <div style="font-size:12px;color:var(--text1);line-height:1.8;" id="tips-body"></div>
        </div>
        <!-- Category tag pills — fixed above footer -->
        <div style="padding:10px 24px 6px;display:flex;flex-wrap:wrap;gap:6px;flex-shrink:0;border-top:1px solid var(--border);" id="tips-tags"></div>
        <!-- Fixed footer — always visible -->
        <div class="modal-footer" style="justify-content:space-between;align-items:center;flex-shrink:0;">
          <div style="display:flex;gap:8px;align-items:center;">
            <button class="btn btn-secondary" style="font-size:11px;padding:6px 14px;" onclick="tipsPrev()">← Prev</button>
            <button class="btn btn-secondary" style="font-size:11px;padding:6px 14px;" onclick="tipsNext()">Next →</button>
          </div>
          <div style="display:flex;gap:8px;align-items:center;">
            <label style="display:flex;align-items:center;gap:5px;font-size:11px;color:var(--text2);cursor:pointer;">
              <input type="checkbox" id="tips-dontshow" style="cursor:pointer;"> Don't show on startup
            </label>
            <button class="btn btn-primary" onclick="closeModal('modal-tips')">Got it →</button>
          </div>
        </div>
      </div>`;
    // Inject scoped styles for tips code blocks
    const tipsStyle = document.createElement('style');
    tipsStyle.textContent = `
      #modal-tips pre {
        max-height: 300px;
        overflow-y: auto;
        overflow-x: auto;
        background: var(--bg0, #080b0f);
        border: 1px solid var(--border, #1e2d42);
        border-left: 3px solid var(--accent, #00d4aa);
        border-radius: 4px;
        padding: 12px 14px;
        font-size: 11px;
        font-family: var(--mono, monospace);
        color: var(--text1, #a8b8cc);
        line-height: 1.6;
        white-space: pre;
        margin: 10px 0;
      }
      #modal-tips pre code {
        background: none;
        border: none;
        padding: 0;
        font-size: inherit;
        color: inherit;
      }
      #modal-tips code:not(pre code) {
        font-family: var(--mono, monospace);
        font-size: 11px;
        color: var(--accent, #00d4aa);
        background: rgba(0,212,170,0.08);
        border: 1px solid rgba(0,212,170,0.15);
        padding: 1px 5px;
        border-radius: 3px;
      }
    `;
    document.head.appendChild(tipsStyle);
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-tips'); });
  }

  const dontShow = (() => { try { return localStorage.getItem('flinksql_tips_hide') === '1'; } catch(_) { return false; } })();
  if (dontShow) return;

  const dontShowCb = document.getElementById('tips-dontshow');
  if (dontShowCb) {
    dontShowCb.checked = false;
    dontShowCb.onchange = () => {
      try { localStorage.setItem('flinksql_tips_hide', dontShowCb.checked ? '1' : '0'); } catch(_) {}
    };
  }

  // Fresh shuffle every time the modal is opened
  _tipsQueue = _shuffledQueue();
  _tipsCurrent = _tipsQueue.shift();
  _renderTip(_tipsCurrent);
  openModal('modal-tips');
}

function _renderTip(idx) {
  const tip = TIPS_DATA[idx];
  if (!tip) return;
  _tipsCurrent = idx;
  document.getElementById('tips-category').textContent = tip.category;
  document.getElementById('tips-title').textContent    = tip.title;
  document.getElementById('tips-body').innerHTML       = tip.body;

  // Category tag pills — highlight the active category, others are navigation shortcuts
  const tagColors = {
    'Getting Started':    'var(--accent)',
    'IDE Tips':           'var(--blue)',
    'Flink Architecture': 'var(--green)',
    'Flink Concepts':     'var(--accent3,#7ee8d0)',
    'Connectors':         'var(--green)',
    'AI & ML Workloads':  'var(--purple,#9b72cf)',
    'Performance Tips':   'var(--yellow)',
    'Admin':              'var(--yellow)',
  };
  const cats   = [...new Set(TIPS_DATA.map(t => t.category))];
  const tagsEl = document.getElementById('tips-tags');
  if (tagsEl) {
    tagsEl.innerHTML = cats.map(c => {
      const col    = tagColors[c] || 'var(--text2)';
      const active = c === tip.category;
      return `<span onclick="_jumpToCategory('${c}')" style="
        font-size:9px;padding:2px 8px;border-radius:3px;cursor:pointer;font-family:var(--mono);
        letter-spacing:0.5px;text-transform:uppercase;transition:all 0.12s;
        background:${active ? 'rgba(0,0,0,0.3)' : 'transparent'};
        border:1px solid ${col};color:${col};
        opacity:${active ? 1 : 0.5};
      ">${c}</span>`;
    }).join('');
  }
}

function tipsNext() {
  // Pull from queue; when empty, reshuffle for next pass (but never show _tipsCurrent first)
  if (_tipsQueue.length === 0) {
    _tipsQueue = _shuffledQueue().filter(i => i !== _tipsCurrent);
  }
  _tipsCurrent = _tipsQueue.shift();
  _renderTip(_tipsCurrent);
}

function tipsPrev() {
  // "Prev" picks a different random tip — there's no ordered history to go back to
  // We pick randomly from tips that are NOT the current one
  const others = TIPS_DATA.map((_, i) => i).filter(i => i !== _tipsCurrent);
  const pick   = others[Math.floor(Math.random() * others.length)];
  // Put current back into queue so it comes up again later, then show pick
  if (_tipsCurrent !== null) _tipsQueue.push(_tipsCurrent);
  _tipsCurrent = pick;
  _renderTip(pick);
}

function _jumpToCategory(cat) {
  // Jump to a random tip within that category that isn't the current tip
  const candidates = TIPS_DATA
      .map((t, i) => ({ t, i }))
      .filter(({ t, i }) => t.category === cat && i !== _tipsCurrent);
  if (candidates.length === 0) return; // only one tip in category and it's current
  const pick = candidates[Math.floor(Math.random() * candidates.length)];
  // Put current back in queue
  if (_tipsCurrent !== null) _tipsQueue.push(_tipsCurrent);
  _tipsCurrent = pick.i;
  _renderTip(pick.i);
}