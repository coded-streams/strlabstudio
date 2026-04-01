/* Str:::lab Studio — JAR Submission Module v1.2.0
 *
 * HOW ROUTING WORKS IN THIS STUDIO:
 *   jmApi('/jobs/overview')  →  Studio proxy  →  JobManager:8081/jobs/overview
 *
 * The Flink JAR REST API lives on the SAME JobManager REST port (8081):
 *   GET    /jars                  list uploaded JARs
 *   POST   /jars/upload           upload a JAR (multipart/form-data)
 *   POST   /jars/:jarId/run       submit job
 *   DELETE /jars/:jarId           remove JAR
 *
 * So jmApi('/jars/...') is the correct call — same as jmApi('/jobs/...').
 *
 * For the XHR upload (needed for progress %) we need the raw URL.
 * We read it from the same internal base that jmApi() uses, exposed via
 * state.gateway.baseUrl which already points at the proxy root.
 */

// ── Storage ───────────────────────────────────────────────────────────────────
const JAR_STORAGE_KEY = 'strlabstudio_jar_history';
const JAR_MAX_HISTORY = 20;

function _jarLoadHistory() {
    try { return JSON.parse(localStorage.getItem(JAR_STORAGE_KEY) || '[]'); } catch (_) { return []; }
}
function _jarSaveHistory(h) {
    try { localStorage.setItem(JAR_STORAGE_KEY, JSON.stringify(h.slice(0, JAR_MAX_HISTORY))); } catch (_) {}
}
function _jarAddHistory(entry) {
    const h = _jarLoadHistory();
    h.unshift({ ...entry, ts: new Date().toISOString() });
    _jarSaveHistory(h);
}

// ── Environment guard ─────────────────────────────────────────────────────────
function _jarCheckEnvironment() {
    if (window.location.protocol === 'file:') {
        return {
            ok: false,
            msg: 'Page opened as a local file (file://).\nServe via Docker at http://localhost:3030 — the proxy only works when served.',
        };
    }
    if (typeof state === 'undefined' || !state.gateway) {
        return { ok: false, msg: 'Not connected to a Flink cluster. Connect first.' };
    }
    return { ok: true };
}

// ── Derive the raw XHR upload URL from the same proxy the Studio uses ─────────
// jmApi() calls e.g. GET http://localhost:3030/jobmanager-api/jobs/overview
// Upload must go to:      POST http://localhost:3030/jobmanager-api/jars/upload
//
// state.gateway.baseUrl is "http://localhost:3030/flink-api"
// The jobmanager-api proxy prefix is one segment up from /flink-api.
function _jarXhrBase() {
    if (typeof state !== 'undefined' && state.gateway && state.gateway.baseUrl) {
        return state.gateway.baseUrl.replace(/\/flink-api\/?$/, '') + '/jobmanager-api';
    }
    return '/jobmanager-api';
}

// ── Safe jmApi wrapper (uses the real one from connection.js when available) ───
async function _jarApi(path, opts) {
    if (typeof jmApi === 'function') {
        const result = await jmApi(path, opts);
        // jmApi can return null on empty 200 bodies — normalise
        return result ?? {};
    }
    // Minimal fallback
    const res = await fetch(_jarXhrBase() + path, opts || {});
    if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        try { const j = await res.json(); msg = j.errors?.[0] || j.message || msg; } catch (_) {}
        throw new Error(msg);
    }
    if (res.status === 204) return {};
    const json = await res.json().catch(() => null);
    return json ?? {};
}

// ── Inject JAR tab when Project Manager opens ─────────────────────────────────
const _jarOrigOpenPM = window.openProjectManager;
window.openProjectManager = function () {
    if (_jarOrigOpenPM) _jarOrigOpenPM();
    // Defer slightly so _pmBuildModal() has time to run
    setTimeout(_jarInjectTab, 0);
};

function _jarInjectTab() {
    if (document.getElementById('pm-tab-jar')) return;

    const tabBar = document.querySelector('#modal-project-manager .udf-tab-btn')?.parentElement;
    if (!tabBar) return;

    const btn = document.createElement('button');
    btn.id = 'pm-tab-jar';
    btn.className = 'udf-tab-btn';
    btn.title = 'Upload & submit a compiled Flink DataStream/Table JAR to the cluster';
    btn.textContent = '⬡ JAR Submit';
    btn.onclick = () => switchPmTab('jar');
    tabBar.appendChild(btn);

    const pane = document.createElement('div');
    pane.id = 'pm-pane-jar';
    pane.style.cssText = 'display:none;flex-direction:column;height:100%;overflow:hidden;';
    pane.innerHTML = _jarPaneHTML();

    // Insert into the scrollable body div (3rd child of .modal)
    const body = document.querySelector('#modal-project-manager .modal > div:nth-child(3)');
    if (body) body.appendChild(pane);

    _jarBindEvents();
}

// ── Patch switchPmTab ─────────────────────────────────────────────────────────
const _jarOrigSwitch = window.switchPmTab;
window.switchPmTab = function (tab) {
    _jarOrigSwitch(tab);

    const jarBtn  = document.getElementById('pm-tab-jar');
    const jarPane = document.getElementById('pm-pane-jar');
    if (!jarBtn || !jarPane) return;

    if (tab === 'jar') {
        document.querySelectorAll('#modal-project-manager .udf-tab-btn')
            .forEach(b => b.classList.remove('active-udf-tab'));
        document.querySelectorAll('#modal-project-manager [id^="pm-pane-"]')
            .forEach(p => { p.style.display = 'none'; });
        jarBtn.classList.add('active-udf-tab');
        jarPane.style.display = 'flex';
        _jarCheckAndShowBanner();
        _jarRefreshClusterList();
        _jarRenderHistory();
    } else {
        jarBtn.classList.remove('active-udf-tab');
        jarPane.style.display = 'none';
    }
};

// ── Pane HTML ─────────────────────────────────────────────────────────────────
function _jarPaneHTML() {
    return `
<div style="display:flex;flex-direction:column;height:100%;overflow:hidden;min-height:0;">

  <!-- Warning banner -->
  <div id="jar-env-banner" style="display:none;flex-shrink:0;margin:12px 16px 0;
       background:rgba(245,166,35,0.09);border:1px solid rgba(245,166,35,0.4);border-radius:5px;
       padding:10px 14px;font-size:11px;color:var(--yellow,#f5a623);
       line-height:1.8;white-space:pre-wrap;font-family:var(--mono);"></div>

  <!-- Two-column body -->
  <div style="display:flex;flex:1;min-height:0;overflow:hidden;">

    <!-- LEFT: Upload form -->
    <div style="flex:1;min-width:0;overflow-y:auto;padding:16px 14px 16px 18px;
                border-right:1px solid var(--border);display:flex;flex-direction:column;gap:14px;">

      <!-- Drop zone -->
      <div>
        <label class="field-label" style="display:flex;align-items:center;gap:5px;margin-bottom:6px;">
          <span style="color:var(--accent);">①</span> JAR File
        </label>
        <div id="jar-dropzone"
          style="border:2px dashed rgba(0,212,170,0.28);border-radius:6px;
                 background:rgba(0,212,170,0.025);padding:22px 16px;text-align:center;
                 cursor:pointer;transition:border-color 0.15s,background 0.15s;
                 display:flex;flex-direction:column;align-items:center;gap:7px;"
          onclick="document.getElementById('jar-file-input').click()"
          ondragover="_jarDragOver(event)" ondragleave="_jarDragLeave(event)" ondrop="_jarDrop(event)">
          <div id="jar-dz-icon" style="font-size:26px;opacity:0.5;">📦</div>
          <div id="jar-dz-title" style="font-size:12px;font-weight:600;color:var(--text0);">Drop JAR here or click to browse</div>
          <div id="jar-dz-sub"   style="font-size:10px;color:var(--text3);">Compiled Flink application JAR · max ~500 MB</div>
          <input type="file" id="jar-file-input" accept=".jar" style="display:none;" onchange="_jarFileSelected(event)" />
        </div>
        <!-- Selected file card -->
        <div id="jar-file-card" style="display:none;margin-top:8px;background:var(--bg2);
             border:1px solid rgba(0,212,170,0.22);border-radius:5px;padding:9px 11px;
             align-items:center;gap:8px;">
          <span style="font-size:18px;flex-shrink:0;">📦</span>
          <div style="flex:1;min-width:0;">
            <div id="jar-file-name" style="font-size:11px;font-weight:600;color:var(--text0);
                 overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"></div>
            <div id="jar-file-meta" style="font-size:10px;color:var(--text3);margin-top:1px;font-family:var(--mono);"></div>
          </div>
          <button onclick="_jarClearFile()" title="Remove"
            style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:15px;
                   flex-shrink:0;padding:1px 4px;line-height:1;">×</button>
        </div>
      </div>

      <!-- Entry class -->
      <div>
        <label class="field-label" style="display:flex;align-items:center;gap:5px;margin-bottom:5px;">
          <span style="color:var(--accent);">②</span> Entry Class
          <span style="color:var(--text3);font-weight:400;font-size:10px;">(optional — uses JAR manifest if blank)</span>
        </label>
        <input id="jar-entry-class" class="field-input" type="text"
          placeholder="com.example.MyFlinkJob" style="font-size:11px;font-family:var(--mono);" />
      </div>

      <!-- Program args -->
      <div>
        <label class="field-label" style="display:flex;align-items:center;gap:5px;margin-bottom:5px;">
          <span style="color:var(--accent);">③</span> Program Arguments
          <span style="color:var(--text3);font-weight:400;font-size:10px;">(optional)</span>
        </label>
        <input id="jar-args" class="field-input" type="text"
          placeholder="--bootstrap-servers localhost:9092 --topic my-topic"
          style="font-size:11px;font-family:var(--mono);" />
        <div style="font-size:10px;color:var(--text3);margin-top:3px;">
          Space-separated — passed as <code style="color:var(--accent);background:rgba(0,212,170,0.08);padding:1px 4px;border-radius:2px;">String[]</code> to <code style="color:var(--accent);background:rgba(0,212,170,0.08);padding:1px 4px;border-radius:2px;">main()</code>
        </div>
      </div>

      <!-- Parallelism + Savepoint -->
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div>
          <label class="field-label" style="display:flex;align-items:center;gap:5px;margin-bottom:5px;">
            <span style="color:var(--accent);">④</span> Parallelism
          </label>
          <input id="jar-parallelism" class="field-input" type="number" min="1" max="512"
            placeholder="cluster default" style="font-size:11px;font-family:var(--mono);" />
        </div>
        <div>
          <label class="field-label" style="display:flex;align-items:center;gap:5px;margin-bottom:5px;">
            <span style="color:var(--accent);">⑤</span> Savepoint Path
            <span style="color:var(--text3);font-weight:400;font-size:10px;">(optional)</span>
          </label>
          <input id="jar-savepoint" class="field-input" type="text"
            placeholder="hdfs:///flink/savepoints/sp-..." style="font-size:11px;font-family:var(--mono);" />
        </div>
      </div>

      <!-- Allow non-restored state -->
      <label style="display:flex;align-items:center;gap:7px;font-size:11px;color:var(--text1);cursor:pointer;"
        title="Skip operators whose savepoint state cannot be remapped — safe to use after schema changes">
        <input type="checkbox" id="jar-allow-non-restored" style="cursor:pointer;" />
        Allow non-restored state <span style="color:var(--text3);font-size:10px;">(savepoint tolerance)</span>
      </label>

    </div><!-- /left -->

    <!-- RIGHT: Cluster JARs + History -->
    <div style="width:290px;flex-shrink:0;display:flex;flex-direction:column;overflow:hidden;min-width:0;">

      <!-- Cluster JARs -->
      <div style="padding:12px 14px 10px;border-bottom:1px solid var(--border);flex-shrink:0;">
        <div style="display:flex;align-items:center;margin-bottom:8px;">
          <span style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:0.8px;text-transform:uppercase;">Cluster JARs</span>
          <button onclick="_jarRefreshClusterList()" title="Reload from cluster"
            style="margin-left:auto;font-size:10px;background:none;border:1px solid var(--border);
                   border-radius:2px;color:var(--text2);cursor:pointer;padding:2px 7px;line-height:1.4;">⟳</button>
        </div>
        <div id="jar-cluster-list"
          style="max-height:180px;overflow-y:auto;display:flex;flex-direction:column;gap:4px;min-width:0;">
          <span style="font-size:11px;color:var(--text3);">Connect then click ⟳</span>
        </div>
      </div>

      <!-- Submission history -->
      <div style="padding:12px 14px;flex:1;overflow-y:auto;min-width:0;">
        <div style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:0.8px;
                    text-transform:uppercase;margin-bottom:8px;">Submission History</div>
        <div id="jar-history-list" style="display:flex;flex-direction:column;gap:5px;min-width:0;">
          <span style="font-size:11px;color:var(--text3);">No submissions yet.</span>
        </div>
      </div>

    </div><!-- /right -->
  </div><!-- /two-col -->

  <!-- Progress bar -->
  <div id="jar-progress-wrap" style="display:none;flex-shrink:0;padding:8px 18px 4px;">
    <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text1);margin-bottom:4px;">
      <span id="jar-progress-label">Uploading…</span>
      <span id="jar-progress-pct" style="font-family:var(--mono);color:var(--accent);">0%</span>
    </div>
    <div style="background:var(--bg3);border-radius:3px;height:5px;overflow:hidden;">
      <div id="jar-progress-bar"
        style="height:100%;width:0%;border-radius:3px;transition:width 0.2s;
               background:linear-gradient(90deg,var(--accent),#00ffcc);"></div>
    </div>
  </div>

  <!-- Footer -->
  <div style="padding:10px 18px;border-top:1px solid var(--border);background:var(--bg2);
              display:flex;align-items:center;gap:8px;flex-shrink:0;">
    <div id="jar-status-msg" style="font-size:11px;flex:1;min-width:0;font-family:var(--mono);
         overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"></div>
    <button class="btn btn-secondary" style="font-size:11px;flex-shrink:0;" onclick="_jarReset()">✕ Clear</button>
    <button id="jar-submit-btn" class="btn btn-primary"
      style="font-size:11px;font-weight:700;flex-shrink:0;min-width:148px;
             display:flex;align-items:center;justify-content:center;gap:5px;"
      onclick="_jarSubmit()">
      <span>⬡</span><span id="jar-submit-label">Upload &amp; Submit</span>
    </button>
  </div>

</div>`;
}

// ── Bind events ───────────────────────────────────────────────────────────────
function _jarBindEvents() {
    _jarRenderHistory();
    _jarCheckAndShowBanner();
    const dz = document.getElementById('jar-dropzone');
    if (dz) {
        dz.addEventListener('mouseenter', () => { if (!dz.classList.contains('dz-active')) dz.style.borderColor = 'rgba(0,212,170,0.55)'; });
        dz.addEventListener('mouseleave', () => { if (!dz.classList.contains('dz-active')) dz.style.borderColor = 'rgba(0,212,170,0.28)'; });
    }
}

function _jarCheckAndShowBanner() {
    const banner = document.getElementById('jar-env-banner');
    if (!banner) return;
    const env = _jarCheckEnvironment();
    banner.style.display = env.ok ? 'none' : 'block';
    if (!env.ok) banner.textContent = '⚠  ' + env.msg;
}

// ── Drag & Drop ───────────────────────────────────────────────────────────────
function _jarDragOver(e) {
    e.preventDefault();
    const dz = document.getElementById('jar-dropzone');
    if (dz) { dz.style.borderColor = 'var(--accent)'; dz.style.background = 'rgba(0,212,170,0.07)'; dz.classList.add('dz-active'); }
}
function _jarDragLeave(e) {
    const dz = document.getElementById('jar-dropzone');
    if (dz) { dz.style.borderColor = 'rgba(0,212,170,0.28)'; dz.style.background = 'rgba(0,212,170,0.025)'; dz.classList.remove('dz-active'); }
}
function _jarDrop(e) { e.preventDefault(); _jarDragLeave(e); const f = e.dataTransfer?.files?.[0]; if (f) _jarSetFile(f); }
function _jarFileSelected(e) { const f = e.target?.files?.[0]; if (f) _jarSetFile(f); }

let _jarCurrentFile = null;

function _jarSetFile(file) {
    if (!file.name.toLowerCase().endsWith('.jar')) { _jarSetStatus('✗ Only .jar files are accepted.', 'err'); return; }
    _jarCurrentFile = file;
    const el = id => document.getElementById(id);
    if (el('jar-file-name')) el('jar-file-name').textContent = file.name;
    if (el('jar-file-meta')) el('jar-file-meta').textContent = _jarFmtBytes(file.size) + '  ·  ' + new Date(file.lastModified).toLocaleString();
    if (el('jar-file-card')) el('jar-file-card').style.display = 'flex';
    if (el('jar-dz-icon'))   el('jar-dz-icon').textContent   = '✅';
    if (el('jar-dz-title'))  el('jar-dz-title').textContent  = 'JAR selected — ready to submit';
    if (el('jar-dz-sub'))    el('jar-dz-sub').textContent    = file.name;
    const dz = el('jar-dropzone');
    if (dz) { dz.style.borderColor = 'rgba(0,212,170,0.6)'; dz.style.background = 'rgba(0,212,170,0.04)'; }
    _jarSetStatus('', '');
}

function _jarClearFile() {
    _jarCurrentFile = null;
    const el = id => document.getElementById(id);
    if (el('jar-file-card')) el('jar-file-card').style.display = 'none';
    if (el('jar-dz-icon'))   el('jar-dz-icon').textContent    = '📦';
    if (el('jar-dz-title'))  el('jar-dz-title').textContent   = 'Drop JAR here or click to browse';
    if (el('jar-dz-sub'))    el('jar-dz-sub').textContent     = 'Compiled Flink application JAR · max ~500 MB';
    const dz = el('jar-dropzone');
    if (dz) { dz.style.borderColor = 'rgba(0,212,170,0.28)'; dz.style.background = 'rgba(0,212,170,0.025)'; }
    const inp = el('jar-file-input'); if (inp) inp.value = '';
    _jarSetStatus('', '');
}

function _jarReset() {
    _jarClearFile();
    ['jar-entry-class', 'jar-args', 'jar-parallelism', 'jar-savepoint'].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = '';
    });
    const cb = document.getElementById('jar-allow-non-restored'); if (cb) cb.checked = false;
    _jarSetStatus('', '');
}

// ── UI helpers ────────────────────────────────────────────────────────────────
function _jarSetStatus(msg, type) {
    const el = document.getElementById('jar-status-msg'); if (!el) return;
    el.style.color = { ok: 'var(--green)', err: 'var(--red)', info: 'var(--accent)', warn: 'var(--yellow,#f5a623)', '': 'var(--text2)' }[type] || 'var(--text2)';
    el.textContent = msg;
    el.title = msg; // show full text on hover when truncated
}

function _jarSetProgress(label, pct) {
    const wrap = document.getElementById('jar-progress-wrap'); if (!wrap) return;
    if (pct < 0) { wrap.style.display = 'none'; return; }
    wrap.style.display = 'block';
    const bar = document.getElementById('jar-progress-bar'); if (bar) bar.style.width = Math.min(100, pct) + '%';
    const lbl = document.getElementById('jar-progress-label'); if (lbl) lbl.textContent = label;
    const pctEl = document.getElementById('jar-progress-pct'); if (pctEl) pctEl.textContent = Math.min(100, Math.round(pct)) + '%';
}

function _jarSetSubmitBusy(busy, label) {
    const btn = document.getElementById('jar-submit-btn');
    const lbl = document.getElementById('jar-submit-label');
    if (lbl) lbl.textContent = label || (busy ? 'Working…' : 'Upload & Submit');
    if (btn) { btn.disabled = busy; btn.style.opacity = busy ? '0.65' : '1'; btn.style.cursor = busy ? 'not-allowed' : 'pointer'; }
}

// ── MAIN SUBMIT PIPELINE ──────────────────────────────────────────────────────
async function _jarSubmit() {
    const env = _jarCheckEnvironment();
    if (!env.ok) { _jarSetStatus('✗ ' + env.msg.split('\n')[0], 'err'); _jarCheckAndShowBanner(); return; }
    if (!_jarCurrentFile) { _jarSetStatus('✗ Select a JAR file first.', 'err'); return; }

    const entryClass  = (document.getElementById('jar-entry-class')?.value || '').trim();
    const args        = (document.getElementById('jar-args')?.value         || '').trim();
    const parallelism = parseInt(document.getElementById('jar-parallelism')?.value || '0', 10) || null;
    const savepoint   = (document.getElementById('jar-savepoint')?.value    || '').trim();
    const allowNR     = document.getElementById('jar-allow-non-restored')?.checked || false;

    _jarSetSubmitBusy(true, 'Uploading…');
    _jarSetProgress('Uploading JAR…', 5);
    _jarSetStatus('Uploading JAR to cluster…', 'info');

    let jarId;
    try {
        // ── STEP 1: Upload JAR via XHR (for progress tracking) ─────────────
        // The Studio proxy exposes Flink's REST API (port 8081) at /jobmanager-api/
        // So POST /jobmanager-api/jars/upload is identical to what jmApi('/jars/upload') would do.
        const uploadUrl = _jarXhrBase() + '/jars/upload';

        jarId = await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', uploadUrl);

            // Copy auth headers from the gateway state (Bearer tokens, etc.)
            // Do NOT set Content-Type — browser sets it with the correct multipart boundary
            if (typeof state !== 'undefined' && state.gateway?.headers) {
                Object.entries(state.gateway.headers).forEach(([k, v]) => {
                    if (k.toLowerCase() !== 'content-type') xhr.setRequestHeader(k, v);
                });
            }

            xhr.upload.onprogress = e => {
                if (e.lengthComputable) _jarSetProgress('Uploading JAR…', (e.loaded / e.total) * 70);
            };

            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    try {
                        const resp = JSON.parse(xhr.responseText || 'null');
                        if (!resp) { reject(new Error('Empty response from /jars/upload')); return; }
                        // Flink returns: { filename: "/opt/flink/web-upload/uuid_name.jar", status: "success" }
                        // jarId for subsequent /jars/:id/run is the FULL filename value (not just basename)
                        const filename = resp.filename || resp.jarId || resp.id || '';
                        if (!filename) { reject(new Error('No filename in upload response: ' + xhr.responseText.slice(0, 300))); return; }
                        resolve(filename);
                    } catch (e) {
                        reject(new Error('Could not parse upload response: ' + xhr.responseText.slice(0, 200)));
                    }
                } else {
                    let msg = `Upload failed: HTTP ${xhr.status}`;
                    try { const j = JSON.parse(xhr.responseText); msg = j.errors?.[0] || j.message || msg; } catch (_) {}
                    if (xhr.status === 0) msg = 'Network error — check the proxy is running at ' + uploadUrl;
                    reject(new Error(msg));
                }
            };
            xhr.onerror = () => reject(new Error('Network error uploading JAR — is the Studio proxy running?'));
            const fd = new FormData();
            fd.append('jarfile', _jarCurrentFile, _jarCurrentFile.name);
            xhr.send(fd);
        });

        _jarSetProgress('JAR uploaded ✓', 75);
        _jarSetStatus('JAR uploaded — submitting job…', 'info');
        _jarSetSubmitBusy(true, 'Submitting…');

        // ── STEP 2: Run the JAR via the same proxy ─────────────────────────
        // POST /jars/:jarId/run
        // The jarId returned by Flink is typically the full path like:
        //   /opt/flink/web-upload/uuid_filename.jar
        // Flink accepts either the full path or just the basename as :jarId.
        // We encode only the basename to be safe.
        const jarBasename = jarId.split('/').pop();

        const runPayload = {};
        if (entryClass)  runPayload.entryClass            = entryClass;
        if (args)        runPayload.programArgsList        = args.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) || [args];
        if (parallelism) runPayload.parallelism            = parallelism;
        if (savepoint)   runPayload.savepointPath          = savepoint;
        if (allowNR)     runPayload.allowNonRestoredState  = true;

        const runResp = await _jarApi(
            `/jars/${encodeURIComponent(jarBasename)}/run`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(runPayload),
            }
        );

        _jarSetProgress('Job submitted!', 100);

        const jobId   = runResp?.jobid || runResp?.jobId || runResp?.id || '';
        const shortId = jobId ? jobId.slice(0, 8) + '…' : '(no ID — check Flink UI)';

        _jarAddHistory({ file: _jarCurrentFile.name, size: _jarCurrentFile.size, jarId: jarBasename, jobId, entryClass: entryClass || '(manifest)', args, parallelism });

        _jarSetStatus(`✓ Job submitted!  ID: ${shortId}`, 'ok');
        _jarSetSubmitBusy(false, 'Upload & Submit');

        if (typeof toast    === 'function') toast('Job submitted: ' + shortId, 'ok');
        if (typeof addLog   === 'function') addLog('OK', 'JAR submitted: ' + _jarCurrentFile.name + ' → job ' + shortId);

        setTimeout(() => _jarSetProgress('', -1), 3000);
        _jarRenderHistory();
        _jarRefreshClusterList();

        // Navigate to Job Graph
        setTimeout(() => _jarGoToJobGraph(jobId), 1000);

    } catch (err) {
        _jarSetProgress('', -1);
        _jarSetStatus('✗ ' + err.message, 'err');
        _jarSetSubmitBusy(false, 'Upload & Submit');
        if (typeof toast  === 'function') toast('JAR submit failed: ' + err.message, 'err');
        if (typeof addLog === 'function') addLog('ERR', 'JAR submit failed: ' + err.message);
    }
}

// ── Navigate to Job Graph and select the new job ──────────────────────────────
function _jarGoToJobGraph(jobId) {
    if (typeof closeModal === 'function') closeModal('modal-project-manager');

    // Click the Job Graph tab in the results panel
    const jgBtn = document.querySelector('[onclick="switchResultTab(\'jobgraph\',this)"]');
    if (jgBtn) jgBtn.click();

    if (!jobId) return;

    // Poll until the job appears in the dropdown
    const trySelect = (n) => {
        if (n <= 0) { if (typeof refreshJobGraphList === 'function') refreshJobGraphList(); return; }
        const sel = document.getElementById('jg-job-select');
        const found = sel && Array.from(sel.options).find(o => o.value === jobId);
        if (found) {
            sel.value = jobId;
            if (typeof loadJobGraph === 'function') loadJobGraph(jobId);
        } else if (typeof refreshJobGraphList === 'function') {
            refreshJobGraphList()
                .then(() => setTimeout(() => trySelect(n - 1), 1200))
                .catch(() => setTimeout(() => trySelect(n - 1), 1200));
        } else {
            setTimeout(() => trySelect(n - 1), 1200);
        }
    };

    if (typeof refreshJobGraphList === 'function') {
        refreshJobGraphList().then(() => trySelect(10)).catch(() => trySelect(10));
    } else {
        setTimeout(() => trySelect(10), 600);
    }
}

// ── Cluster JAR list (right panel) ───────────────────────────────────────────
async function _jarRefreshClusterList() {
    const el = document.getElementById('jar-cluster-list'); if (!el) return;
    const env = _jarCheckEnvironment();
    if (!env.ok) {
        el.innerHTML = `<span style="font-size:10px;color:var(--yellow,#f5a623);">⚠ Not connected</span>`;
        return;
    }
    el.innerHTML = `<span style="font-size:10px;color:var(--text3);">Loading…</span>`;
    try {
        const data = await _jarApi('/jars');
        // Handle all Flink response shapes
        const jars = Array.isArray(data)       ? data
            : Array.isArray(data?.files) ? data.files
                : Array.isArray(data?.jars)  ? data.jars
                    : [];

        if (!jars.length) {
            el.innerHTML = `<span style="font-size:10px;color:var(--text3);">No JARs on cluster yet.</span>`;
            return;
        }

        el.innerHTML = jars.map(j => {
            // Flink can return { id, name } or { filename, name } depending on version
            const rawId  = j.id || j.filename || j.name || '';
            const name   = j.name || rawId.split('/').pop();
            const size   = _jarFmtBytes(j.size || 0);
            // Use basename for the run/delete calls
            const basename = rawId.split('/').pop() || rawId;
            const safeId   = encodeURIComponent(basename);
            const safeName = escHtml ? escHtml(name) : name.replace(/</g,'&lt;');
            return `
<div style="border:1px solid var(--border);border-radius:4px;padding:6px 8px;background:var(--bg1);
            display:flex;align-items:center;gap:5px;min-width:0;">
  <span style="font-size:13px;flex-shrink:0;">📦</span>
  <div style="flex:1;min-width:0;overflow:hidden;">
    <div style="font-size:10px;font-weight:600;color:var(--text0);overflow:hidden;
                text-overflow:ellipsis;white-space:nowrap;" title="${safeName}">${safeName}</div>
    <div style="font-size:9px;color:var(--text3);font-family:var(--mono);">${size}</div>
  </div>
  <div style="display:flex;gap:3px;flex-shrink:0;">
    <button title="Submit this JAR with current config"
      onclick="_jarRunExisting('${safeId}','${safeName}')"
      style="font-size:9px;padding:2px 6px;border-radius:2px;cursor:pointer;
             background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);color:var(--accent);">▶</button>
    <button title="Delete JAR from cluster"
      onclick="_jarDeleteJar('${safeId}','${safeName}')"
      style="font-size:9px;padding:2px 6px;border-radius:2px;cursor:pointer;
             background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.25);color:var(--red);">🗑</button>
  </div>
</div>`;
        }).join('');
    } catch (e) {
        el.innerHTML = `<span style="font-size:10px;color:var(--red);">✗ ${e.message}</span>`;
    }
}

async function _jarRunExisting(encodedId, displayName) {
    const env = _jarCheckEnvironment();
    if (!env.ok) { _jarSetStatus('✗ Not connected.', 'err'); return; }

    const entryClass  = (document.getElementById('jar-entry-class')?.value || '').trim();
    const args        = (document.getElementById('jar-args')?.value         || '').trim();
    const parallelism = parseInt(document.getElementById('jar-parallelism')?.value || '0', 10) || null;
    const savepoint   = (document.getElementById('jar-savepoint')?.value    || '').trim();
    const allowNR     = document.getElementById('jar-allow-non-restored')?.checked || false;

    _jarSetSubmitBusy(true, 'Submitting…');
    _jarSetStatus('Submitting existing JAR…', 'info');
    try {
        const runPayload = {};
        if (entryClass)  runPayload.entryClass            = entryClass;
        if (args)        runPayload.programArgsList        = args.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) || [args];
        if (parallelism) runPayload.parallelism            = parallelism;
        if (savepoint)   runPayload.savepointPath          = savepoint;
        if (allowNR)     runPayload.allowNonRestoredState  = true;

        const runResp = await _jarApi(`/jars/${encodedId}/run`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(runPayload),
        });

        const jobId   = runResp?.jobid || runResp?.jobId || runResp?.id || '';
        const shortId = jobId ? jobId.slice(0, 8) + '…' : '(no ID)';

        _jarAddHistory({ file: decodeURIComponent(displayName || encodedId), size: 0, jarId: decodeURIComponent(encodedId), jobId, entryClass: entryClass || '(manifest)', args, parallelism });
        _jarSetStatus(`✓ Job submitted!  ID: ${shortId}`, 'ok');
        _jarSetSubmitBusy(false, 'Upload & Submit');
        if (typeof toast  === 'function') toast('Job submitted: ' + shortId, 'ok');
        if (typeof addLog === 'function') addLog('OK', 'JAR job submitted: ' + shortId);
        _jarRenderHistory();
        setTimeout(() => _jarGoToJobGraph(jobId), 1000);
    } catch (e) {
        _jarSetStatus('✗ ' + e.message, 'err');
        _jarSetSubmitBusy(false, 'Upload & Submit');
        if (typeof toast === 'function') toast('Submit failed: ' + e.message, 'err');
    }
}

async function _jarDeleteJar(encodedId, displayName) {
    const name = decodeURIComponent(displayName || encodedId);
    if (!confirm(`Delete "${name}" from the cluster?\n\nRunning jobs are NOT affected.`)) return;
    try {
        await _jarApi('/jars/' + encodedId, { method: 'DELETE' });
        if (typeof toast === 'function') toast('JAR deleted: ' + name, 'ok');
        if (typeof addLog === 'function') addLog('WARN', 'JAR deleted from cluster: ' + name);
        _jarRefreshClusterList();
    } catch (e) {
        if (typeof toast === 'function') toast('Delete failed: ' + e.message, 'err');
    }
}

// ── Submission history ────────────────────────────────────────────────────────
function _jarRenderHistory() {
    const el = document.getElementById('jar-history-list'); if (!el) return;
    const hist = _jarLoadHistory();
    if (!hist.length) { el.innerHTML = '<span style="font-size:10px;color:var(--text3);">No submissions yet.</span>'; return; }
    el.innerHTML = hist.map(h => {
        const ts      = new Date(h.ts).toLocaleString();
        const shortId = (h.jobId || '').slice(0, 8);
        const safeName = escHtml ? escHtml(h.file) : (h.file || '').replace(/</g,'&lt;');
        const safeClass = h.entryClass && h.entryClass !== '(manifest)' ? (escHtml ? escHtml(h.entryClass) : h.entryClass) : '';
        return `
<div style="border:1px solid var(--border);border-radius:4px;padding:7px 9px;background:var(--bg1);min-width:0;overflow:hidden;">
  <div style="font-size:10px;font-weight:600;color:var(--text0);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
       title="${safeName}">${safeName}</div>
  <div style="font-size:9px;color:var(--text3);font-family:var(--mono);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
    ${shortId ? 'job: <span style="color:var(--accent);">' + shortId + '…</span>' : ''}${safeClass ? ' · ' + safeClass : ''}
  </div>
  <div style="font-size:9px;color:var(--text3);margin-top:2px;">${ts}</div>
  ${h.jobId ? `<button onclick="_jarViewJob('${h.jobId}')" title="View in Job Graph"
    style="font-size:9px;padding:2px 7px;border-radius:2px;margin-top:4px;cursor:pointer;
           background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.25);color:var(--accent);">◈ View Job</button>` : ''}
</div>`;
    }).join('');
}

function _jarViewJob(jobId) {
    if (typeof closeModal === 'function') closeModal('modal-project-manager');
    const jgBtn = document.querySelector('[onclick="switchResultTab(\'jobgraph\',this)"]');
    if (jgBtn) jgBtn.click();
    setTimeout(() => {
        const sel = document.getElementById('jg-job-select');
        if (sel && Array.from(sel.options).find(o => o.value === jobId)) {
            sel.value = jobId;
            if (typeof loadJobGraph === 'function') loadJobGraph(jobId);
        } else if (typeof refreshJobGraphList === 'function') {
            refreshJobGraphList().then(() => {
                const s = document.getElementById('jg-job-select');
                if (s) { s.value = jobId; if (typeof loadJobGraph === 'function') loadJobGraph(jobId); }
            }).catch(() => {});
        }
    }, 500);
}

// ── Utility ───────────────────────────────────────────────────────────────────
function _jarFmtBytes(b) {
    b = Number(b) || 0;
    if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GB';
    if (b >= 1048576)    return (b / 1048576).toFixed(1) + ' MB';
    if (b >= 1024)       return (b / 1024).toFixed(0) + ' KB';
    return b + ' B';
}

// ── CSS ───────────────────────────────────────────────────────────────────────
(function () {
    const s = document.createElement('style');
    s.textContent = `
    #jar-dropzone:hover { border-color:rgba(0,212,170,0.55)!important; background:rgba(0,212,170,0.05)!important; }
    #jar-dropzone.dz-active { border-color:var(--accent)!important; background:rgba(0,212,170,0.08)!important; }
    #jar-cluster-list::-webkit-scrollbar, #jar-history-list::-webkit-scrollbar { width:3px; }
    #jar-cluster-list::-webkit-scrollbar-thumb, #jar-history-list::-webkit-scrollbar-thumb { background:var(--border2);border-radius:2px; }
    @keyframes _jarPulse { 0%,100%{opacity:1} 50%{opacity:.5} }
    #jar-progress-bar { animation:_jarPulse 1.4s ease-in-out infinite; }
    `;
    document.head.appendChild(s);
})();