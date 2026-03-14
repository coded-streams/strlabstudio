/* FlinkSQL Studio — Error modal, job graph modals, cancel job, admin sessions */

// ── Error detail modal ────────────────────────────────────────────────────────
function showErrorModal(idxOrRaw) {
  let friendly, raw;
  if (typeof idxOrRaw === 'number') {
    const entry = state.logLines[idxOrRaw];
    raw      = entry?.raw || entry?.msg || 'No details available.';
    friendly = entry?.msg || parseFlinkError(raw);
  } else {
    raw      = idxOrRaw || state.lastErrorRaw || 'No details available.';
    friendly = parseFlinkError(raw);
  }
  document.getElementById('error-summary').textContent = friendly;
  document.getElementById('error-stacktrace').innerHTML = _highlightStackTrace(raw);
  const badge = document.getElementById('error-type-badge');
  if (badge) {
    const type = _classifyError(raw);
    badge.textContent    = type.label;
    badge.style.background = type.bg;
    badge.style.color      = type.color;
  }
  openModal('modal-error');
}

function _classifyError(raw) {
  if (!raw) return { label: 'ERROR', bg: 'rgba(255,77,109,0.15)', color: 'var(--red)' };
  const s = raw.toUpperCase();
  if (s.includes('PARSE') || s.includes('ENCOUNTERED') || s.includes('SYNTAX'))
    return { label: 'SQL PARSE ERROR', bg: 'rgba(245,166,35,0.15)', color: 'var(--yellow)' };
  if (s.includes('VALIDATION') || s.includes('VALIDATIONEXCEPTION'))
    return { label: 'VALIDATION ERROR', bg: 'rgba(245,166,35,0.15)', color: 'var(--yellow)' };
  if (s.includes('CLASSNOTFOUND') || s.includes('NOCLASSDEF') || s.includes('JAR'))
    return { label: 'MISSING JAR / CONNECTOR', bg: 'rgba(0,151,255,0.15)', color: 'var(--blue)' };
  if (s.includes('TIMEOUT') || s.includes('CONNECT'))
    return { label: 'CONNECTION ERROR', bg: 'rgba(0,151,255,0.15)', color: 'var(--blue)' };
  if (s.includes('TABLE') && s.includes('NOT FOUND') || s.includes("OBJECT '"))
    return { label: 'TABLE NOT FOUND', bg: 'rgba(245,166,35,0.15)', color: 'var(--yellow)' };
  if (s.includes('HTTP 4') || s.includes('HTTP 5'))
    return { label: 'GATEWAY ERROR', bg: 'rgba(255,77,109,0.15)', color: 'var(--red)' };
  return { label: 'RUNTIME ERROR', bg: 'rgba(255,77,109,0.15)', color: 'var(--red)' };
}

function _highlightStackTrace(raw) {
  if (!raw) return '<span style="color:var(--text2)">No details available.</span>';
  return escHtml(raw)
      .replace(/(Caused by:[^\n]+)/g, '<span style="color:var(--red);font-weight:600">$1</span>')
      .replace(/(\b\w+(?:Exception|Error)\b)/g, '<span style="color:var(--yellow)">$1</span>')
      .replace(/(^\s+at (?:com|org|net|java|sun|io)\.[^\n]+)/gm, '<span style="color:var(--text3)">$1</span>')
      .replace(/\b(line \d+|col(?:umn)? \d+)\b/gi, '<span style="color:var(--accent)">$1</span>')
      .replace(/\b(SELECT|INSERT|CREATE|DROP|ALTER|FROM|WHERE|TABLE|CATALOG|DATABASE)\b/g, '<span style="color:var(--blue)">$1</span>');
}

function copyErrorToClipboard() {
  const text = document.getElementById('error-stacktrace').textContent;
  navigator.clipboard.writeText(text)
      .then(() => toast('Stack trace copied to clipboard', 'ok'))
      .catch(() => toast('Could not copy — select text manually', 'err'));
}

// ── Job graph: load job list (scoped by session) ──────────────────────────────
async function refreshJobGraphList() {
  if (!state.gateway) return;
  const sel = document.getElementById('jg-job-select');
  if (!sel) return;
  try {
    const data = await jmApi('/jobs/overview');
    if (!data || !data.jobs) { toast('No jobs found on JobManager', 'info'); return; }

    const allJobs     = data.jobs || [];
    const visibleJobs = filterJobsForCurrentSession(allJobs);

    sel.innerHTML = '<option value="">— Select a job —</option>';
    visibleJobs.forEach(job => {
      const opt = document.createElement('option');
      opt.value = job.jid;
      const dur   = job.duration ? Math.round(job.duration / 1000) + 's' : '';
      let   label = `[${job.state}] ${job.name.slice(0,40)} ${dur ? '(' + dur + ')' : ''}`;
      if (state.isAdminSession && state._jobSessionMap) {
        const sessHandle = state._jobSessionMap[job.jid];
        if (sessHandle) {
          const sess = state.sessions.find(s => s.handle === sessHandle);
          label += ` — ${sess ? escHtml(sess.name || shortHandle(sessHandle)) : shortHandle(sessHandle)}`;
        }
      }
      opt.textContent = label;
      sel.appendChild(opt);
    });

    renderJobList(visibleJobs);

    if (visibleJobs.length === 0 && allJobs.length > 0 && !state.isAdminSession) {
      const empty = document.getElementById('job-compare-empty');
      if (empty) {
        empty.style.display = 'flex';
        empty.textContent   = `${allJobs.length} job(s) running on cluster — connect as Admin to see all`;
      }
    }

    const running = visibleJobs.find(j => j.state === 'RUNNING');
    if (running && !sel.value)             { sel.value = running.jid;         loadJobGraph(running.jid); }
    else if (visibleJobs.length > 0 && !sel.value) { sel.value = visibleJobs[0].jid; loadJobGraph(visibleJobs[0].jid); }
  } catch(e) {
    addLog('WARN', `Could not load jobs: ${e.message}`);
  }
}

// ── Cancel job with confirmation ──────────────────────────────────────────────
async function cancelSelectedJob() {
  const sel = document.getElementById('jg-job-select');
  const jid = sel ? sel.value : '';
  if (!jid) { toast('No job selected', 'err'); return; }
  const jobName = sel.options[sel.selectedIndex]?.text || '';
  showCancelJobConfirm(jid, jobName);
}

// ── Admin: all-sessions overview ──────────────────────────────────────────────
function openAdminSessionsView() {
  if (!state.isAdminSession) { toast('Admin session required', 'err'); return; }

  let modal = document.getElementById('modal-admin-sessions');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'modal-admin-sessions';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
      <div class="modal" style="width:740px;max-height:90vh;display:flex;flex-direction:column;">
        <div class="modal-header" style="background:linear-gradient(135deg,rgba(245,166,35,0.1),rgba(0,0,0,0));border-bottom:1px solid rgba(245,166,35,0.2);">
          <span style="color:var(--yellow,#f5a623);display:flex;align-items:center;gap:8px;font-size:13px;">
            🛡 Admin — All Sessions
          </span>
          <button class="modal-close" onclick="closeModal('modal-admin-sessions')">×</button>
        </div>

        <!-- Summary stats bar -->
        <div id="admin-sessions-summary" style="display:flex;gap:1px;background:var(--border);border-bottom:1px solid var(--border);flex-shrink:0;"></div>

        <div class="modal-body" style="flex:1;overflow-y:auto;padding:0;">
          <div id="admin-sessions-content" style="padding:14px;"></div>
        </div>
        <div class="modal-footer" style="justify-content:space-between;">
          <div style="font-size:10px;color:var(--text3);font-family:var(--mono);" id="admin-sessions-refreshed">—</div>
          <div style="display:flex;gap:8px;">
            <button class="btn btn-secondary" onclick="refreshAdminSessionsView()">⟳ Refresh</button>
            <button class="btn btn-primary"   onclick="closeModal('modal-admin-sessions')">Close</button>
          </div>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-admin-sessions'); });
  }
  openModal('modal-admin-sessions');
  refreshAdminSessionsView();
}

async function refreshAdminSessionsView() {
  const container = document.getElementById('admin-sessions-content');
  const summaryEl = document.getElementById('admin-sessions-summary');
  const refreshEl = document.getElementById('admin-sessions-refreshed');
  if (!container) return;

  // Fetch all cluster jobs
  let allJobs = [];
  try {
    const data = await jmApi('/jobs/overview');
    allJobs = (data && data.jobs) ? data.jobs : [];
  } catch(_) {}

  // Fetch ALL sessions from the gateway — not just locally registered ones.
  // This shows sessions created by OTHER users connected to the same cluster.
  try {
    const gwSessions = await api('GET', '/v1/sessions');
    const gwList = gwSessions.sessions || gwSessions || [];
    if (Array.isArray(gwList)) {
      gwList.forEach(gws => {
        const handle = gws.sessionHandle || gws.handle || gws;
        const name   = gws.sessionName   || gws.name   || '';
        if (handle && !state.sessions.find(s => s.handle === handle)) {
          // Register unknown session from gateway with minimal info
          state.sessions.push({
            handle,
            name:        name || 'remote-session',
            created:     new Date(),
            isAdmin:     false,
            jobIds:      [],
            queryCount:  0,
            _auditTrail: [],
            _fromGateway: true,  // flag: discovered from gateway, not created here
          });
        }
      });
    }
  } catch(_) {}  // gateway /v1/sessions may not be supported on all versions

  const sessions    = state.sessions;
  const totalSess   = sessions.length;
  const totalJobs   = allJobs.length;
  const runningJobs = allJobs.filter(j => j.state === 'RUNNING').length;
  const failedJobs  = allJobs.filter(j => j.state === 'FAILED').length;
  const totalQ      = sessions.reduce((sum, s) => {
    if (s.handle === state.activeSession) return sum + (state.history||[]).length;
    return sum + (s.queryCount || (s._savedState ? (s._savedState.history||[]).length : 0));
  }, 0);

  // Summary bar
  if (summaryEl) {
    summaryEl.innerHTML = [
      ['Sessions',      totalSess,   'var(--accent)'],
      ['Total Jobs',    totalJobs,   'var(--blue)'],
      ['Running',       runningJobs, 'var(--green)'],
      ['Failed',        failedJobs,  failedJobs > 0 ? 'var(--red)' : 'var(--text3)'],
      ['Total Queries', totalQ,      'var(--accent3,#f5a623)'],
    ].map(([label, val, color]) => `
      <div style="flex:1;background:var(--bg2);padding:8px 12px;text-align:center;">
        <div style="font-size:18px;font-weight:700;color:${color};font-family:var(--mono);">${val}</div>
        <div style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:0.5px;margin-top:2px;">${label}</div>
      </div>`).join('');
  }

  if (refreshEl) {
    refreshEl.textContent = `Last refreshed: ${new Date().toLocaleTimeString()}`;
  }

  if (sessions.length === 0) {
    container.innerHTML = '<div style="color:var(--text3);font-size:12px;text-align:center;padding:24px;">No sessions registered in this Studio instance</div>';
    return;
  }

  let html = '';
  for (const sess of sessions) {
    const isActive = sess.handle === state.activeSession;
    const sessJobs = allJobs.filter(j => (sess.jobIds || []).includes(j.jid));
    const runJ     = sessJobs.filter(j => j.state === 'RUNNING').length;
    const failJ    = sessJobs.filter(j => j.state === 'FAILED').length;
    const qCount   = sess.queryCount || (sess._savedState ? (sess._savedState.history||[]).length : 0);
    const tabs     = (sess._savedState ? sess._savedState.tabs : []) || [];
    const auditLen = (sess._auditTrail || []).length;

    html += `
      <div style="margin-bottom:10px;border:1px solid ${isActive?'var(--accent)':'var(--border)'};border-radius:var(--radius);overflow:hidden;">
        <!-- Session header -->
        <div style="padding:10px 14px;background:${isActive?'rgba(0,212,170,0.05)':'var(--bg2)'};display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
          <span style="font-family:var(--mono);font-size:12px;font-weight:700;color:${isActive?'var(--accent)':'var(--text0)'};">
            ${sess.isAdmin ? '🛡 ' : ''}${escHtml(sess.name || shortHandle(sess.handle))}
          </span>
          <span style="font-size:10px;color:var(--text3);font-family:var(--mono);" title="${sess.handle}">${shortHandle(sess.handle)}</span>
          ${isActive ? '<span style="font-size:9px;background:rgba(0,212,170,0.15);color:var(--accent);padding:1px 6px;border-radius:2px;">ACTIVE</span>' : ''}
          ${auditLen > 0 ? `<span style="font-size:9px;background:rgba(245,166,35,0.15);color:var(--yellow,#f5a623);padding:1px 6px;border-radius:2px;">⚠ ${auditLen} admin action${auditLen>1?'s':''}</span>` : ''}
          ${sess._fromGateway ? `<span style="font-size:9px;background:rgba(79,163,224,0.15);color:var(--blue);padding:1px 6px;border-radius:2px;" title="Session discovered from gateway — created by another user">🌐 Remote</span>` : ''}
          <span style="margin-left:auto;font-size:10px;color:var(--text3);">Created: ${sess.created instanceof Date ? sess.created.toLocaleTimeString() : '—'}</span>
        </div>

        <!-- Stats row -->
        <div style="display:flex;gap:0;border-top:1px solid var(--border);border-bottom:1px solid var(--border);">
          ${_asStat('Queries',      qCount,    'var(--accent)')}
          ${_asStat('Jobs Total',   sessJobs.length, 'var(--blue)')}
          ${_asStat('Running',      runJ,      'var(--green)')}
          ${_asStat('Failed',       failJ,     failJ>0?'var(--red)':'var(--text3)')}
          ${_asStat('Open Tabs',    tabs.length,'var(--text2)')}
        </div>

        <!-- Job list (collapsed inline) -->
        ${sessJobs.length > 0 ? `
        <div style="padding:8px 14px;background:var(--bg1);">
          ${sessJobs.slice(0,3).map(j => `
            <div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:11px;border-bottom:1px solid rgba(255,255,255,0.04);">
              <span style="width:7px;height:7px;border-radius:50%;flex-shrink:0;background:${j.state==='RUNNING'?'var(--green)':j.state==='FAILED'?'var(--red)':'var(--text3)'};"></span>
              <span style="flex:1;color:var(--text1);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml((j.name||j.jid).slice(0,44))}</span>
              <span style="font-size:9px;color:var(--text3);font-family:var(--mono);">${j.jid.slice(0,8)}</span>
              <span style="font-size:9px;padding:1px 5px;border-radius:2px;background:${j.state==='RUNNING'?'rgba(57,211,83,0.15)':j.state==='FAILED'?'rgba(255,77,109,0.15)':'rgba(100,100,100,0.2)'};color:${j.state==='RUNNING'?'var(--green)':j.state==='FAILED'?'var(--red)':'var(--text2)'};">${j.state}</span>
              ${j.state==='RUNNING' ? `
                <button onclick="recordAudit('${sess.handle}','Cancel Job','Job ${j.jid.slice(0,8)}');showCancelJobConfirm('${j.jid}','${escHtml((j.name||'').replace(/'/g,"\\'")).slice(0,30)}');closeModal('modal-admin-sessions');"
                  style="font-size:9px;padding:1px 6px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.08);color:var(--red);cursor:pointer;flex-shrink:0;">
                  Cancel
                </button>` : ''}
            </div>`).join('')}
          ${sessJobs.length > 3 ? `<div style="font-size:10px;color:var(--text3);padding:4px 0;">+${sessJobs.length-3} more jobs</div>` : ''}
        </div>` : ''}

        <!-- Action row -->
        <div style="padding:8px 14px;display:flex;gap:8px;background:var(--bg1);">
          <button class="mini-btn" onclick="closeModal('modal-admin-sessions');openAdminSessionDetail('${sess.handle}');" style="font-size:10px;">
            🔍 Inspect Session
          </button>
          ${!isActive ? `<button class="mini-btn" onclick="closeModal('modal-admin-sessions');switchSession('${sess.handle}');">Switch To</button>` : ''}
          <button class="mini-btn danger" onclick="recordAudit('${sess.handle}','Session Deleted','');deleteSession('${sess.handle}');refreshAdminSessionsView();">Delete</button>
        </div>
      </div>`;
  }

  // Unattributed jobs (running on cluster but not tracked in any session)
  const trackedJids    = new Set(Object.keys(state._jobSessionMap || {}));
  const unattributed   = allJobs.filter(j => !trackedJids.has(j.jid));
  if (unattributed.length > 0) {
    html += `
      <div style="margin-bottom:10px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;opacity:0.75;">
        <div style="padding:8px 14px;background:var(--bg2);font-size:11px;color:var(--text2);">
          Other cluster jobs — not tracked by any Studio session
        </div>
        <div style="padding:6px 14px 10px;">
          ${unattributed.slice(0,5).map(j => `
            <div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:11px;border-bottom:1px solid rgba(255,255,255,0.04);">
              <span style="width:7px;height:7px;border-radius:50%;flex-shrink:0;background:${j.state==='RUNNING'?'var(--green)':'var(--text3)'}"></span>
              <span style="flex:1;color:var(--text1);overflow:hidden;text-overflow:ellipsis;">${escHtml((j.name||j.jid).slice(0,44))}</span>
              <span style="font-size:9px;color:var(--text3);">${j.state}</span>
            </div>`).join('')}
        </div>
      </div>`;
  }

  container.innerHTML = html;
}

function _asStat(label, val, color) {
  return `<div style="flex:1;padding:6px 10px;text-align:center;border-right:1px solid var(--border);background:var(--bg1);">
    <div style="font-size:14px;font-weight:700;color:${color};font-family:var(--mono);">${val}</div>
    <div style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:0.3px;">${label}</div>
  </div>`;
}