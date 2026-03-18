// CONNECTION
// ──────────────────────────────────────────────

// ── Admin session config ──────────────────────────────────────────────────────
const ADMIN_DEFAULT_PASSCODE = 'admin1234';
let _adminPasscode = (() => {
  try { return localStorage.getItem('strlabstudio_admin_pass') || ADMIN_DEFAULT_PASSCODE; } catch(_) { return ADMIN_DEFAULT_PASSCODE; }
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

// ── Via Studio proxy diagnosis ────────────────────────────────────────────────
// Returns a human-readable explanation when the nginx proxy returns 500/502.
// The most common cause is the Studio container not being on the same Docker
// network as the Flink cluster, OR the env vars not being set (defaulting to
// localhost which resolves to the container itself, not the gateway).
function _proxyDiagnosticHtml() {
  return `<br><br>
<strong style="color:var(--yellow,#f5a623);">Via Studio proxy returned an error.</strong>
The most common causes are:<br><br>
<strong>1. Missing environment variables (most likely)</strong><br>
The container defaults are <code>FLINK_GATEWAY_HOST=localhost</code> which points to itself.
You must pass the gateway hostname explicitly:<br>
<pre style="margin:6px 0;background:rgba(0,0,0,0.3);padding:8px 10px;border-radius:3px;font-size:10px;overflow-x:auto;">docker run -p 3030:80 \
  --network &lt;your-flink-network&gt; \
  -e FLINK_GATEWAY_HOST=flink-gateway-cors-proxy \
  -e FLINK_GATEWAY_PORT=8084 \
  -e JOBMANAGER_HOST=flink-jobmanager \
  -e JOBMANAGER_PORT=8081 \
  codedstreams/strlabstudio:latest</pre>
<strong>2. Container not on the same Docker network</strong><br>
Without <code>--network &lt;flink-network&gt;</code>, the Studio container cannot resolve
your gateway container name. Run <code>docker network ls</code> to find your Flink network name.<br><br>
<strong>3. Flink cluster is down</strong><br>
Verify the gateway is running: <code>docker ps | grep gateway</code><br><br>
<strong>Alternative: use Direct Gateway mode</strong> — click "Direct Gateway", enter
<code>http://localhost:8084</code> (if the gateway port is forwarded to your host).`;
}

async function testConnection() {
  const baseUrl = getBaseUrl();
  setConnectStatus('loading', `Testing ${baseUrl}/v1/info …`);
  try {
    const resp = await fetchWithTimeout(`${baseUrl}/v1/info`, { headers: { 'Accept': 'application/json' } }, 8000);
    if (!resp.ok) {
      let msg = `HTTP ${resp.status}`;
      if (resp.status === 502 || resp.status === 500) {
        if (connMode === 'proxy') {
          setConnectStatus('err', `HTTP ${resp.status} — nginx proxy cannot reach the Flink Gateway.${_proxyDiagnosticHtml()}`);
          return;
        }
        msg = `${resp.status} — Flink SQL Gateway not reachable. Check it is running.`;
      }
      if (resp.status === 503) msg = `503 — Flink SQL Gateway not ready yet. Wait 30s and retry.`;
      setConnectStatus('err', msg);
      return;
    }
    const info = await resp.json();
    const ver = info.flinkVersion || info.version || 'unknown';
    setConnectStatus('ok', `✓ Gateway reachable! &nbsp; Flink ${ver} &nbsp; — click Connect`);
    toast('Connection test passed', 'ok');
    loadExistingSessionsFromGateway(baseUrl);
  } catch (e) {
    let hint = '';
    if (connMode === 'proxy') {
      hint = _proxyDiagnosticHtml();
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

    let sel = document.getElementById('existing-sessions-select');
    if (!sel) {
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
let _sessionMode = 'new';
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

  state.gateway = { host, port, baseUrl };

  try {
    const verifyResp = await fetchWithTimeout(`${baseUrl}/v1/info`, { headers: { Accept: 'application/json' } }, 8000);
    if (!verifyResp.ok) {
      // Distinguish proxy failure from gateway failure
      if ((verifyResp.status === 500 || verifyResp.status === 502) && connMode === 'proxy') {
        throw new Error(`Nginx proxy returned ${verifyResp.status} — the Studio container cannot reach the Flink Gateway. Set the correct FLINK_GATEWAY_HOST env var and make sure the container is on the same Docker network as your Flink cluster (--network flag).`);
      }
      throw new Error(`Gateway returned HTTP ${verifyResp.status} — is it fully started?`);
    }

    if (_sessionMode === 'admin') {
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

  document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
  state._lastSessionHandle = state.activeSession;
  document.getElementById('topbar-host-label').textContent = `${host}:${port}`;
  document.getElementById('sb-host').textContent = `${host}:${port}`;
  document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
  state.activeCatalog  = 'default_catalog';
  state.activeDatabase = 'default';
  updateCatalogStatus('default_catalog', 'default');

  _applyAdminUI();

  try {
    const savedTheme = localStorage.getItem('strlabstudio_theme');
    if (savedTheme) state.theme = savedTheme;
  } catch(_) {}
  applyTheme();

  const savedSessionName = (() => { try { return localStorage.getItem('strlabstudio_last_session_name') || ''; } catch(_) { return ''; } })();
  const sessionName = (() => { try { return document.getElementById('inp-session-name')?.value.trim() || ''; } catch(_) { return ''; } })();
  const isNewNamedSession = sessionName && sessionName !== savedSessionName;

  if (isNewNamedSession) {
    if (state.tabs.length > 0) {
      try { localStorage.setItem('strlabstudio_workspace_backup', JSON.stringify({ tabs: state.tabs, savedAt: Date.now() })); } catch(_) {}
    }
    state.tabs = []; state.activeTab = null; state.history = [];
    state.logLines = []; state.operations = []; state.results = []; state.resultColumns = [];
    window._workspaceRestored = true;
    addTab('Query 1');
    setTimeout(() => toast(`New session "${sessionName}" — fresh workspace started`, 'ok'), 500);
    try { localStorage.setItem('strlabstudio_last_session_name', sessionName); } catch(_) {}
  } else {
    if (!window._workspaceRestored) {
      restoreWorkspace();
      window._workspaceRestored = true;
      if (state.tabs.length > 0) {
        const count = state.tabs.length;
        setTimeout(() => toast(`${count} tab${count>1?'s':''} restored`, 'info'), 800);
      }
    }
    if (sessionName) { try { localStorage.setItem('strlabstudio_last_session_name', sessionName); } catch(_) {} }
  }

  if (state.tabs.length === 0) addTab('Query 1');
  else renderTabs();

  refreshCatalog();
  renderSessionsList();
  renderHistory();
  startHeartbeat();
  if (typeof startResourceMonitor === 'function') startResourceMonitor();

  addLog('INFO', `Connected to Flink SQL Gateway. ${state.isAdminSession ? 'ADMIN session active — full cluster visibility enabled.' : 'Session scoped to your jobs only.'}`);

  setTimeout(() => showTipsModal(), 1200);
}

// ── Apply admin UI indicators ─────────────────────────────────────────────────
function _applyAdminUI() {
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

// ── Admin Settings Modal ──────────────────────────────────────────────────────
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
            <input class="field-input" id="admin-old-pass"     type="password" placeholder="Current passcode"           style="font-size:12px;margin-bottom:6px;" />
            <input class="field-input" id="admin-new-pass"     type="password" placeholder="New passcode (min 6 chars)" style="font-size:12px;margin-top:6px;" />
            <input class="field-input" id="admin-confirm-pass" type="password" placeholder="Confirm new passcode"        style="font-size:12px;margin-top:6px;" />
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
    try { localStorage.setItem('strlabstudio_admin_pass', newPass); } catch(_) {}
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
  const badge = document.getElementById('admin-session-badge');
  if (badge) badge.remove();
  const sbAdmin = document.getElementById('sb-admin-indicator');
  if (sbAdmin) sbAdmin.remove();
  const nameInput = document.getElementById('inp-session-name');
  const savedName = prefillName || (() => { try { return localStorage.getItem('strlabstudio_last_session_name') || ''; } catch(_) { return ''; } })();
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
      const sname = (() => { try { return localStorage.getItem('strlabstudio_last_session_name') || ''; } catch(_) { return ''; } })();
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
    // NOTE: DO NOT run USE CATALOG / USE `default` here.
    // default_catalog has no database named "default" in a standard Flink setup.
    // Running it generates a CatalogException in the gateway logs every session.
    // Users should run SHOW DATABASES to see what databases exist and USE the correct one.
  } catch(e) {
    addLog('ERR', `Failed to create new session: ${e.message}`);
    toast('Could not create session — try reconnecting', 'err');
  }
}