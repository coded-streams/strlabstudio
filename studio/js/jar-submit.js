/* Str:::lab Studio — JAR Submission Module v1.1.0
 * Adds a "⬡ JAR Submit" tab to the Project Manager modal.
 * Allows uploading a compiled Flink DataStream/Table API JAR and
 * submitting it to the Flink cluster via the Flink REST API.
 * On successful submission, navigates to the Job Graph panel.
 *
 * FLINK REST API used (same JobManager base as jobgraph.js):
 *   GET  /jars                     → list uploaded JARs
 *   POST /jars/upload              → upload JAR (multipart/form-data)
 *   POST /jars/:jarId/run          → submit job
 *   DELETE /jars/:jarId            → delete uploaded JAR
 *
 * Uses jmApi() — the same gateway helper used by jobgraph.js/perf.js.
 * For uploads (XHR progress tracking) it derives the raw upload URL
 * from the same gateway base that jmApi() resolves against.
 *
 * Integration: place in js/ and add after project-manager.js in index.html:
 *   <script src="js/jar-submit.js"></script>
 */

// ── Constants ─────────────────────────────────────────────────────────────────
const JAR_STORAGE_KEY = 'strlabstudio_jar_history';
const JAR_MAX_HISTORY = 20;

// ── JAR history helpers ───────────────────────────────────────────────────────
function _jarLoadHistory() {
    try { return JSON.parse(localStorage.getItem(JAR_STORAGE_KEY) || '[]'); } catch(_) { return []; }
}
function _jarSaveHistory(h) {
    try { localStorage.setItem(JAR_STORAGE_KEY, JSON.stringify(h.slice(0, JAR_MAX_HISTORY))); } catch(_) {}
}
function _jarAddHistory(entry) {
    const h = _jarLoadHistory();
    h.unshift({ ...entry, ts: new Date().toISOString() });
    _jarSaveHistory(h);
}

// ── Guard: detect file:// and no-connection situations ───────────────────────
function _jarCheckEnvironment() {
    if (window.location.protocol === 'file:') {
        return {
            ok: false,
            msg: 'Page opened as a local file (file://).\n' +
                'Serve Str:::lab Studio through Docker (docker-compose up) and\n' +
                'open http://localhost:3030 — the JAR proxy only works when served.',
        };
    }
    if (typeof state === 'undefined' || !state.gateway) {
        return {
            ok: false,
            msg: 'Not connected to a Flink cluster.\n' +
                'Go back to the Connection screen and connect first.',
        };
    }
    return { ok: true };
}

// ── Resolve upload URL from the same gateway as jmApi() ──────────────────────
// jmApi() calls /jobmanager-api/<path> through the proxy.
// For XHR (needed for upload progress) we need the absolute URL.
function _jarUploadUrl() {
    if (typeof state !== 'undefined' && state.gateway && state.gateway.baseUrl) {
        // baseUrl is e.g. "http://localhost:3030/flink-api"
        // jobmanager-api lives at the same origin one segment up
        const base = state.gateway.baseUrl.replace(/\/flink-api\/?$/, '');
        return base + '/jobmanager-api/jars/upload';
    }
    return '/jobmanager-api/jars/upload';
}

// ── jmApi wrapper ─────────────────────────────────────────────────────────────
// Uses the real jmApi() from connection.js/jobgraph.js when available.
// Falls back to a minimal implementation so the module never hard-errors on load.
async function _jarJmApi(path, opts) {
    if (typeof jmApi === 'function') {
        return jmApi(path, opts);
    }
    // Minimal fallback
    const base = (typeof state !== 'undefined' && state.gateway && state.gateway.baseUrl)
        ? state.gateway.baseUrl.replace(/\/flink-api\/?$/, '') + '/jobmanager-api'
        : '/jobmanager-api';
    const res = await fetch(base + path, opts || {});
    if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        try { const j = await res.json(); msg = j.errors?.[0] || j.message || msg; } catch(_) {}
        throw new Error(msg);
    }
    if (res.status === 204) return {};
    const json = await res.json();
    // Flink can legitimately return null for empty responses — normalise to {}
    return json ?? {};
}

// ── Inject JAR tab into Project Manager modal ─────────────────────────────────
const _jarOrigOpenPM = window.openProjectManager;
window.openProjectManager = function() {
    if (_jarOrigOpenPM) _jarOrigOpenPM();
    _jarInjectTab();
};

function _jarInjectTab() {
    if (document.getElementById('pm-tab-jar')) return;

    const tabBar = document.querySelector('#modal-project-manager .udf-tab-btn')?.parentElement;
    if (!tabBar) return;

    // Tab button
    const btn = document.createElement('button');
    btn.id        = 'pm-tab-jar';
    btn.className = 'udf-tab-btn';
    btn.title     = 'Upload & submit a compiled Flink JAR application to the cluster';
    btn.innerHTML = '⬡ JAR Submit';
    btn.onclick   = () => switchPmTab('jar');
    tabBar.appendChild(btn);

    // Pane
    const pane = document.createElement('div');
    pane.id    = 'pm-pane-jar';
    pane.style.cssText = 'display:none;flex-direction:column;height:100%;';
    pane.innerHTML = _jarPaneHTML();

    const body = document.querySelector('#modal-project-manager .modal > div:nth-child(3)');
    if (body) body.appendChild(pane);

    _jarBindEvents();
}

// ── Pane markup ───────────────────────────────────────────────────────────────
function _jarPaneHTML() {
    return `
<div style="display:flex;flex-direction:column;height:100%;overflow:hidden;">

  <!-- Two-column layout -->
  <div style="display:flex;flex:1;min-height:0;gap:0;">

    <!-- LEFT: Upload + Config -->
    <div style="flex:1;min-width:0;overflow-y:auto;padding:18px 16px 18px 20px;
                border-right:1px solid var(--border);display:flex;flex-direction:column;gap:16px;">

      <!-- Environment warning banner (shown when file:// or not connected) -->
      <div id="jar-env-banner"
           style="display:none;background:rgba(255,166,35,0.08);border:1px solid rgba(255,166,35,0.35);
                  border-radius:5px;padding:11px 14px;font-size:11px;color:var(--yellow,#f5a623);
                  line-height:1.8;white-space:pre-wrap;font-family:var(--mono);"></div>

      <!-- Drop zone -->
      <div>
        <label class="field-label" style="display:flex;align-items:center;gap:6px;">
          <span style="color:var(--accent);font-size:13px;">①</span> JAR File
        </label>
        <div id="jar-dropzone"
          style="border:2px dashed rgba(0,212,170,0.3);border-radius:6px;
                 background:rgba(0,212,170,0.03);padding:28px 20px;text-align:center;
                 cursor:pointer;transition:all 0.18s;
                 display:flex;flex-direction:column;align-items:center;gap:8px;"
          onclick="document.getElementById('jar-file-input').click()"
          ondragover="_jarDragOver(event)"
          ondragleave="_jarDragLeave(event)"
          ondrop="_jarDrop(event)">
          <div id="jar-dz-icon" style="font-size:28px;opacity:0.5;">📦</div>
          <div id="jar-dz-title" style="font-size:13px;font-weight:600;color:var(--text0);">
            Drop JAR here or click to browse
          </div>
          <div id="jar-dz-sub" style="font-size:10px;color:var(--text3);">
            Accepts .jar files · max ~500 MB
          </div>
          <input type="file" id="jar-file-input" accept=".jar" style="display:none;"
                 onchange="_jarFileSelected(event)" />
        </div>

        <!-- File info card (shown after selection) -->
        <div id="jar-file-card"
             style="display:none;margin-top:8px;background:var(--bg2);
                    border:1px solid rgba(0,212,170,0.25);border-radius:5px;
                    padding:10px 12px;align-items:center;gap:10px;">
          <span style="font-size:20px;">📦</span>
          <div style="flex:1;min-width:0;">
            <div id="jar-file-name"
                 style="font-size:12px;font-weight:600;color:var(--text0);
                        overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"></div>
            <div id="jar-file-meta"
                 style="font-size:10px;color:var(--text3);margin-top:2px;font-family:var(--mono);"></div>
          </div>
          <button onclick="_jarClearFile()"
                  style="background:none;border:none;color:var(--text3);cursor:pointer;
                         font-size:16px;flex-shrink:0;padding:2px 4px;line-height:1;"
                  title="Remove selected file">×</button>
        </div>
      </div>

      <!-- Entry class -->
      <div>
        <label class="field-label" style="display:flex;align-items:center;gap:6px;">
          <span style="color:var(--accent);font-size:13px;">②</span> Entry Class
          <span style="color:var(--text3);font-weight:400;font-size:10px;">
            (optional — uses JAR manifest if blank)
          </span>
        </label>
        <input id="jar-entry-class" class="field-input" type="text"
               placeholder="com.example.MyFlinkJob"
               style="font-size:12px;font-family:var(--mono);" />
      </div>

      <!-- Program args -->
      <div>
        <label class="field-label" style="display:flex;align-items:center;gap:6px;">
          <span style="color:var(--accent);font-size:13px;">③</span> Program Arguments
          <span style="color:var(--text3);font-weight:400;font-size:10px;">(optional)</span>
        </label>
        <input id="jar-args" class="field-input" type="text"
               placeholder="--input hdfs://... --output kafka://..."
               style="font-size:12px;font-family:var(--mono);" />
        <div style="font-size:10px;color:var(--text3);margin-top:3px;">
          Space-separated — passed as
          <code style="color:var(--accent);background:rgba(0,212,170,0.08);
                       padding:1px 4px;border-radius:2px;">String[]</code>
          to <code style="color:var(--accent);background:rgba(0,212,170,0.08);
                          padding:1px 4px;border-radius:2px;">main()</code>
        </div>
      </div>

      <!-- Parallelism + Savepoint -->
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div>
          <label class="field-label" style="display:flex;align-items:center;gap:6px;">
            <span style="color:var(--accent);font-size:13px;">④</span> Parallelism
          </label>
          <input id="jar-parallelism" class="field-input" type="number" min="1" max="512"
                 placeholder="cluster default"
                 style="font-size:12px;font-family:var(--mono);" />
        </div>
        <div>
          <label class="field-label" style="display:flex;align-items:center;gap:6px;">
            <span style="color:var(--accent);font-size:13px;">⑤</span> Savepoint Path
            <span style="color:var(--text3);font-weight:400;font-size:10px;">(optional)</span>
          </label>
          <input id="jar-savepoint" class="field-input" type="text"
                 placeholder="hdfs:///flink/savepoints/sp-..."
                 style="font-size:12px;font-family:var(--mono);" />
        </div>
      </div>

      <!-- Allow non-restored state -->
      <div style="display:flex;align-items:center;gap:8px;">
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;
                      color:var(--text1);cursor:pointer;"
               title="Skip operators whose savepoint state cannot be mapped — useful after schema changes">
          <input type="checkbox" id="jar-allow-non-restored" style="cursor:pointer;" />
          Allow non-restored state
        </label>
        <span style="font-size:10px;color:var(--text3);">(savepoint tolerance)</span>
      </div>

    </div><!-- /left col -->

    <!-- RIGHT: Cluster JARs + History -->
    <div style="width:310px;flex-shrink:0;display:flex;flex-direction:column;overflow:hidden;">

      <!-- Cluster JARs -->
      <div style="padding:14px 16px 10px;border-bottom:1px solid var(--border);">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">
          <span style="font-size:11px;font-weight:700;color:var(--text1);
                       letter-spacing:0.5px;text-transform:uppercase;">Cluster JARs</span>
          <button onclick="_jarRefreshClusterList()" title="Reload JAR list from Flink cluster"
                  style="margin-left:auto;font-size:10px;background:none;
                         border:1px solid var(--border);border-radius:2px;
                         color:var(--text2);cursor:pointer;padding:2px 7px;">⟳</button>
        </div>
        <div id="jar-cluster-list"
             style="max-height:172px;overflow-y:auto;display:flex;flex-direction:column;gap:4px;">
          <span style="font-size:11px;color:var(--text3);padding:4px 0;">
            Connect to the cluster, then click ⟳
          </span>
        </div>
      </div>

      <!-- Submission history -->
      <div style="padding:14px 16px;flex:1;overflow-y:auto;">
        <div style="font-size:11px;font-weight:700;color:var(--text1);
                    letter-spacing:0.5px;text-transform:uppercase;margin-bottom:8px;">
          Submission History
        </div>
        <div id="jar-history-list"
             style="display:flex;flex-direction:column;gap:5px;font-size:11px;">
          <span style="color:var(--text3);">No submissions yet.</span>
        </div>
      </div>

    </div><!-- /right col -->
  </div><!-- /two-col -->

  <!-- Upload progress bar -->
  <div id="jar-progress-wrap" style="display:none;padding:10px 20px 6px;">
    <div style="display:flex;justify-content:space-between;font-size:11px;
                color:var(--text1);margin-bottom:4px;">
      <span id="jar-progress-label">Uploading…</span>
      <span id="jar-progress-pct" style="font-family:var(--mono);color:var(--accent);">0%</span>
    </div>
    <div style="background:var(--bg3);border-radius:3px;height:6px;overflow:hidden;">
      <div id="jar-progress-bar"
           style="height:100%;width:0%;border-radius:3px;
                  background:linear-gradient(90deg,var(--accent),#00ffcc);
                  transition:width 0.2s;"></div>
    </div>
  </div>

  <!-- Footer / Submit -->
  <div style="padding:12px 20px;border-top:1px solid var(--border);background:var(--bg2);
              display:flex;align-items:center;gap:10px;flex-shrink:0;">
    <div id="jar-status-msg"
         style="font-size:11px;min-height:16px;flex:1;font-family:var(--mono);"></div>
    <button class="btn btn-secondary" style="font-size:11px;"
            onclick="_jarReset()" title="Clear form">✕ Clear</button>
    <button id="jar-submit-btn" class="btn btn-primary"
            style="font-size:12px;font-weight:700;letter-spacing:0.5px;min-width:155px;
                   display:flex;align-items:center;justify-content:center;gap:6px;"
            onclick="_jarSubmit()"
            title="Upload JAR to Flink cluster and submit job">
      <span>⬡</span>
      <span id="jar-submit-label">Upload &amp; Submit</span>
    </button>
  </div>

</div>`;
}

// ── Bind events after pane is injected ────────────────────────────────────────
function _jarBindEvents() {
    _jarRenderHistory();
    _jarCheckAndShowBanner();

    const dz = document.getElementById('jar-dropzone');
    if (dz) {
        dz.addEventListener('mouseenter', () => {
            if (!dz.classList.contains('dz-active'))
                dz.style.borderColor = 'rgba(0,212,170,0.55)';
        });
        dz.addEventListener('mouseleave', () => {
            if (!dz.classList.contains('dz-active'))
                dz.style.borderColor = 'rgba(0,212,170,0.3)';
        });
    }
}

function _jarCheckAndShowBanner() {
    const banner = document.getElementById('jar-env-banner');
    if (!banner) return;
    const env = _jarCheckEnvironment();
    if (!env.ok) {
        banner.style.display = 'block';
        banner.textContent   = '⚠  ' + env.msg;
    } else {
        banner.style.display = 'none';
    }
}

// ── Patch switchPmTab to handle 'jar' ────────────────────────────────────────
const _jarOrigSwitch = window.switchPmTab;
window.switchPmTab = function(tab) {
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

// ── Drag & Drop ───────────────────────────────────────────────────────────────
function _jarDragOver(e) {
    e.preventDefault();
    const dz = document.getElementById('jar-dropzone');
    if (dz) {
        dz.style.borderColor = 'var(--accent)';
        dz.style.background  = 'rgba(0,212,170,0.07)';
        dz.classList.add('dz-active');
    }
}
function _jarDragLeave(e) {
    const dz = document.getElementById('jar-dropzone');
    if (dz) {
        dz.style.borderColor = 'rgba(0,212,170,0.3)';
        dz.style.background  = 'rgba(0,212,170,0.03)';
        dz.classList.remove('dz-active');
    }
}
function _jarDrop(e) {
    e.preventDefault();
    _jarDragLeave(e);
    const file = e.dataTransfer?.files?.[0];
    if (file) _jarSetFile(file);
}
function _jarFileSelected(e) {
    const file = e.target?.files?.[0];
    if (file) _jarSetFile(file);
}

let _jarCurrentFile = null;

function _jarSetFile(file) {
    if (!file.name.toLowerCase().endsWith('.jar')) {
        _jarSetStatus('✗ Only .jar files are accepted.', 'err');
        return;
    }
    _jarCurrentFile = file;

    const card = document.getElementById('jar-file-card');
    const name = document.getElementById('jar-file-name');
    const meta = document.getElementById('jar-file-meta');
    const icon = document.getElementById('jar-dz-icon');
    const ttl  = document.getElementById('jar-dz-title');
    const sub  = document.getElementById('jar-dz-sub');
    const dz   = document.getElementById('jar-dropzone');

    if (name) name.textContent = file.name;
    if (meta) meta.textContent =
        _jarFmtBytes(file.size) + '  ·  modified: ' + new Date(file.lastModified).toLocaleString();
    if (card) card.style.display = 'flex';
    if (icon) icon.textContent   = '✅';
    if (ttl)  ttl.textContent    = 'JAR selected — ready to submit';
    if (sub)  sub.textContent    = file.name;
    if (dz)   {
        dz.style.borderColor = 'rgba(0,212,170,0.6)';
        dz.style.background  = 'rgba(0,212,170,0.04)';
    }
    _jarSetStatus('', '');
}

function _jarClearFile() {
    _jarCurrentFile = null;
    const card = document.getElementById('jar-file-card');
    const icon = document.getElementById('jar-dz-icon');
    const ttl  = document.getElementById('jar-dz-title');
    const sub  = document.getElementById('jar-dz-sub');
    const dz   = document.getElementById('jar-dropzone');
    const inp  = document.getElementById('jar-file-input');

    if (card) card.style.display = 'none';
    if (icon) icon.textContent   = '📦';
    if (ttl)  ttl.textContent    = 'Drop JAR here or click to browse';
    if (sub)  sub.textContent    = 'Accepts .jar files · max ~500 MB';
    if (dz)   {
        dz.style.borderColor = 'rgba(0,212,170,0.3)';
        dz.style.background  = 'rgba(0,212,170,0.03)';
    }
    if (inp)  inp.value = '';
    _jarSetStatus('', '');
}

function _jarReset() {
    _jarClearFile();
    ['jar-entry-class', 'jar-args', 'jar-parallelism', 'jar-savepoint']
        .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    const cb = document.getElementById('jar-allow-non-restored');
    if (cb) cb.checked = false;
    _jarSetStatus('', '');
}

// ── Status / progress helpers ─────────────────────────────────────────────────
function _jarSetStatus(msg, type) {
    const el = document.getElementById('jar-status-msg');
    if (!el) return;
    const colors = { ok: 'var(--green)', err: 'var(--red)', info: 'var(--accent)', '': 'var(--text2)' };
    el.style.color = colors[type] || colors[''];
    el.textContent = msg;
}

function _jarSetProgress(label, pct) {
    const wrap  = document.getElementById('jar-progress-wrap');
    const bar   = document.getElementById('jar-progress-bar');
    const lbl   = document.getElementById('jar-progress-label');
    const pctEl = document.getElementById('jar-progress-pct');
    if (!wrap) return;
    if (pct < 0) { wrap.style.display = 'none'; return; }
    wrap.style.display = 'block';
    if (bar)   bar.style.width   = Math.min(100, pct) + '%';
    if (lbl)   lbl.textContent   = label;
    if (pctEl) pctEl.textContent = Math.min(100, Math.round(pct)) + '%';
}

function _jarSetSubmitBusy(busy, label) {
    const btn = document.getElementById('jar-submit-btn');
    const lbl = document.getElementById('jar-submit-label');
    if (lbl) lbl.textContent = label || (busy ? 'Working…' : 'Upload & Submit');
    if (btn) {
        btn.disabled      = busy;
        btn.style.opacity = busy ? '0.65' : '1';
        btn.style.cursor  = busy ? 'not-allowed' : 'pointer';
    }
}

// ── Upload + Submit pipeline ──────────────────────────────────────────────────
async function _jarSubmit() {
    const env = _jarCheckEnvironment();
    if (!env.ok) {
        _jarSetStatus('✗ ' + env.msg.split('\n')[0], 'err');
        _jarCheckAndShowBanner();
        return;
    }
    if (!_jarCurrentFile) {
        _jarSetStatus('✗ Please select a JAR file first.', 'err');
        return;
    }

    const entryClass  = (document.getElementById('jar-entry-class')?.value || '').trim();
    const args        = (document.getElementById('jar-args')?.value         || '').trim();
    const parallelism = parseInt(document.getElementById('jar-parallelism')?.value || '0', 10) || null;
    const savepoint   = (document.getElementById('jar-savepoint')?.value    || '').trim();
    const allowNR     = document.getElementById('jar-allow-non-restored')?.checked || false;

    _jarSetSubmitBusy(true, 'Uploading…');
    _jarSetProgress('Uploading JAR to cluster…', 5);
    _jarSetStatus('Uploading JAR…', 'info');

    let jarId;
    try {
        // ── Step 1: Upload via XHR for progress tracking ────────────────────
        const formData  = new FormData();
        formData.append('jarfile', _jarCurrentFile, _jarCurrentFile.name);
        const uploadUrl = _jarUploadUrl();

        jarId = await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', uploadUrl);

            // Forward any auth headers the gateway requires (Bearer token, etc.)
            if (typeof state !== 'undefined' && state.gateway && state.gateway.headers) {
                Object.entries(state.gateway.headers).forEach(([k, v]) => {
                    // Do NOT set Content-Type — the browser must set the multipart boundary itself
                    if (k.toLowerCase() !== 'content-type') xhr.setRequestHeader(k, v);
                });
            }

            xhr.upload.onprogress = (e) => {
                if (e.lengthComputable) {
                    _jarSetProgress('Uploading JAR…', (e.loaded / e.total) * 72);
                }
            };

            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    try {
                        const resp = JSON.parse(xhr.responseText);
                        // resp can be null if Flink returns bare "null"
                        if (!resp) { reject(new Error('Empty upload response from Flink')); return; }
                        // Flink REST response: { filename: "/tmp/flink-jars/uuid_name.jar", status: "success" }
                        // The jarId used for /jars/:id/run is the basename of that path.
                        const fn = resp.filename || resp.jarId || resp.id || '';
                        const id = fn.split('/').pop().split('\\').pop() || fn;
                        if (!id) { reject(new Error('Could not extract JAR id from upload response: ' + xhr.responseText.slice(0,200))); return; }
                        resolve(id);
                    } catch(e) {
                        reject(new Error(
                            'Unexpected upload response: ' + xhr.responseText.slice(0, 200)
                        ));
                    }
                } else {
                    let msg = `HTTP ${xhr.status}`;
                    try {
                        const j = JSON.parse(xhr.responseText);
                        msg = j.errors?.[0] || j.message || j.error || msg;
                    } catch(_) {
                        if (xhr.status === 404) {
                            msg = 'Endpoint not found (/jars/upload). ' +
                                'Is the Flink JobManager reachable through the proxy? ' +
                                'Expected: ' + uploadUrl;
                        } else if (xhr.status === 0) {
                            msg = 'Network error — CORS or proxy unreachable. ' +
                                'Open the app via http://localhost:3030, not file://';
                        }
                    }
                    reject(new Error(msg));
                }
            };

            xhr.onerror = () => {
                reject(new Error(
                    window.location.protocol === 'file:'
                        ? 'Cannot upload from file:// — serve the app through the Docker container at http://localhost:3030'
                        : 'Network error during upload — check that the Flink proxy is running'
                ));
            };

            xhr.send(formData);
        });

        _jarSetProgress('JAR uploaded ✓ — submitting job…', 78);
        _jarSetStatus('JAR uploaded ✓ — submitting job…', 'info');
        _jarSetSubmitBusy(true, 'Submitting…');

        // ── Step 2: Run via jmApi (same as jobgraph.js calls) ──────────────
        const runPayload = {};
        if (entryClass)  runPayload.entryClass            = entryClass;
        if (args)        runPayload.programArgs           = args;
        if (parallelism) runPayload.parallelism           = parallelism;
        if (savepoint)   runPayload.savepointPath         = savepoint;
        if (allowNR)     runPayload.allowNonRestoredState = true;

        const runResp = await _jarJmApi(
            `/jars/${encodeURIComponent(jarId)}/run`,
            {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify(runPayload),
            }
        );

        const jobId   = runResp.jobid || runResp.jobId || '';
        const shortId = jobId ? jobId.slice(0, 8) + '…' : '(no ID returned)';

        _jarSetProgress('Job submitted!', 100);

        _jarAddHistory({
            file:       _jarCurrentFile.name,
            size:       _jarCurrentFile.size,
            jarId,
            jobId,
            entryClass: entryClass || '(manifest)',
            args,
            parallelism,
        });

        _jarSetStatus(`✓ Job submitted!  ID: ${shortId}`, 'ok');
        _jarSetSubmitBusy(false, 'Upload & Submit');

        if (typeof toast  === 'function') toast('Job submitted: ' + shortId, 'ok');
        if (typeof addLog === 'function') addLog('OK', 'JAR job submitted: ' + shortId);

        setTimeout(() => _jarSetProgress('', -1), 2800);
        _jarRenderHistory();
        _jarRefreshClusterList();

        // ── Step 3: Open Job Graph ──────────────────────────────────────────
        setTimeout(() => _jarNavigateToJobGraph(jobId), 950);

    } catch(err) {
        _jarSetProgress('', -1);
        _jarSetStatus('✗ ' + err.message, 'err');
        _jarSetSubmitBusy(false, 'Upload & Submit');
        if (typeof toast  === 'function') toast('JAR submit failed: ' + err.message, 'err');
        if (typeof addLog === 'function') addLog('ERR', 'JAR submit failed: ' + err.message);
    }
}

// ── Navigate to Job Graph after submission ────────────────────────────────────
function _jarNavigateToJobGraph(jobId) {
    if (typeof closeModal === 'function') closeModal('modal-project-manager');

    const jgBtn = document.querySelector('[onclick="switchResultTab(\'jobgraph\',this)"]');
    if (jgBtn) jgBtn.click();

    if (!jobId) return;

    // Poll the dropdown until the new job appears (cluster may take a moment to register it)
    const trySelect = (attemptsLeft) => {
        if (attemptsLeft <= 0) {
            if (typeof refreshJobGraphList === 'function') refreshJobGraphList();
            return;
        }
        const sel = document.getElementById('jg-job-select');
        if (sel) {
            const opt = Array.from(sel.options).find(o => o.value === jobId);
            if (opt) {
                sel.value = jobId;
                if (typeof loadJobGraph === 'function') loadJobGraph(jobId);
                return;
            }
        }
        if (typeof refreshJobGraphList === 'function') {
            refreshJobGraphList()
                .then(() => setTimeout(() => trySelect(attemptsLeft - 1), 1000))
                .catch(() => setTimeout(() => trySelect(attemptsLeft - 1), 1000));
        } else {
            setTimeout(() => trySelect(attemptsLeft - 1), 1000);
        }
    };

    if (typeof refreshJobGraphList === 'function') {
        refreshJobGraphList().then(() => trySelect(8)).catch(() => trySelect(8));
    } else {
        setTimeout(() => trySelect(8), 600);
    }

    if (typeof toast === 'function') toast('Opening Job Graph…', 'info');
}

// ── Cluster JAR list ──────────────────────────────────────────────────────────
async function _jarRefreshClusterList() {
    const el = document.getElementById('jar-cluster-list');
    if (!el) return;

    const env = _jarCheckEnvironment();
    if (!env.ok) {
        el.innerHTML = `<span style="color:var(--yellow,#f5a623);font-size:11px;">
            ⚠ Not connected — connect to cluster first.</span>`;
        return;
    }

    el.innerHTML = '<span style="color:var(--text3);font-size:11px;">Loading…</span>';
    try {
        const data = await _jarJmApi('/jars');
        // Flink versions differ in response shape:
        //   { files: [...] }   ← Flink 1.16+
        //   { jars:  [...] }   ← some builds
        //   [...]              ← rare direct array
        //   null / {}          ← empty cluster
        const jars = Array.isArray(data)
            ? data
            : (Array.isArray(data?.files) ? data.files
                : (Array.isArray(data?.jars)  ? data.jars
                    : []));

        if (!jars.length) {
            el.innerHTML =
                '<span style="color:var(--text3);font-size:11px;">No JARs uploaded to cluster yet.</span>';
            return;
        }

        el.innerHTML = jars.map(j => {
            const name    = (j.name || j.id || '').split('/').pop();
            const size    = _jarFmtBytes(j.size || 0);
            const jarIdE  = encodeURIComponent(j.id || name);
            const nameEsc = escHtml(name);
            return `
<div style="border:1px solid var(--border);border-radius:4px;padding:6px 8px;
            background:var(--bg1);display:flex;align-items:center;gap:6px;">
  <span style="font-size:14px;flex-shrink:0;">📦</span>
  <div style="flex:1;min-width:0;">
    <div style="font-size:11px;font-weight:600;color:var(--text0);overflow:hidden;
                text-overflow:ellipsis;white-space:nowrap;" title="${nameEsc}">${nameEsc}</div>
    <div style="font-size:9px;color:var(--text3);font-family:var(--mono);">${size}</div>
  </div>
  <div style="display:flex;gap:3px;flex-shrink:0;">
    <button onclick="_jarRunExisting('${jarIdE}','${nameEsc}')"
      title="Submit this JAR with current config fields"
      style="font-size:9px;padding:2px 7px;border-radius:2px;
             background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);
             color:var(--accent);cursor:pointer;">▶ Run</button>
    <button onclick="_jarDeleteFromCluster('${jarIdE}','${nameEsc}')"
      title="Delete JAR from cluster"
      style="font-size:9px;padding:2px 6px;border-radius:2px;
             background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.25);
             color:var(--red);cursor:pointer;">🗑</button>
  </div>
</div>`;
        }).join('');
    } catch(e) {
        el.innerHTML = `<span style="color:var(--red);font-size:11px;">✗ ${escHtml(e.message)}</span>`;
    }
}

async function _jarRunExisting(encodedJarId, displayName) {
    const env = _jarCheckEnvironment();
    if (!env.ok) { _jarSetStatus('✗ ' + env.msg.split('\n')[0], 'err'); return; }

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
        if (args)        runPayload.programArgs           = args;
        if (parallelism) runPayload.parallelism           = parallelism;
        if (savepoint)   runPayload.savepointPath         = savepoint;
        if (allowNR)     runPayload.allowNonRestoredState = true;

        const runResp = await _jarJmApi(
            `/jars/${encodedJarId}/run`,
            {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify(runPayload),
            }
        );

        const jobId   = runResp.jobid || runResp.jobId || '';
        const shortId = jobId ? jobId.slice(0, 8) + '…' : '(no ID)';

        _jarAddHistory({
            file:       decodeURIComponent(displayName || encodedJarId),
            size:       0,
            jarId:      decodeURIComponent(encodedJarId),
            jobId,
            entryClass: entryClass || '(manifest)',
            args,
            parallelism,
        });

        _jarSetStatus(`✓ Job submitted!  ID: ${shortId}`, 'ok');
        _jarSetSubmitBusy(false, 'Upload & Submit');
        if (typeof toast  === 'function') toast('Job submitted: ' + shortId, 'ok');
        if (typeof addLog === 'function') addLog('OK', 'JAR job submitted: ' + shortId);

        _jarRenderHistory();
        setTimeout(() => _jarNavigateToJobGraph(jobId), 950);
    } catch(e) {
        _jarSetStatus('✗ ' + e.message, 'err');
        _jarSetSubmitBusy(false, 'Upload & Submit');
        if (typeof toast === 'function') toast('Submit failed: ' + e.message, 'err');
    }
}

async function _jarDeleteFromCluster(encodedJarId, displayName) {
    const name = decodeURIComponent(displayName || encodedJarId);
    if (!confirm(`Delete "${name}" from the cluster?\n\nRunning jobs are not affected.`)) return;
    try {
        await _jarJmApi('/jars/' + encodedJarId, { method: 'DELETE' });
        if (typeof toast === 'function') toast('JAR deleted: ' + name, 'ok');
        _jarRefreshClusterList();
    } catch(e) {
        if (typeof toast === 'function') toast('Delete failed: ' + e.message, 'err');
    }
}

// ── Submission history panel ──────────────────────────────────────────────────
function _jarRenderHistory() {
    const el = document.getElementById('jar-history-list');
    if (!el) return;
    const hist = _jarLoadHistory();
    if (!hist.length) {
        el.innerHTML = '<span style="color:var(--text3);">No submissions yet.</span>';
        return;
    }
    el.innerHTML = hist.map(h => {
        const ts      = new Date(h.ts).toLocaleString();
        const shortId = (h.jobId || '').slice(0, 8);
        return `
<div style="border:1px solid var(--border);border-radius:4px;padding:7px 9px;background:var(--bg1);">
  <div style="font-size:11px;font-weight:600;color:var(--text0);overflow:hidden;
              text-overflow:ellipsis;white-space:nowrap;"
       title="${escHtml(h.file)}">${escHtml(h.file)}</div>
  <div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:2px;">
    ${shortId ? 'job: <span style="color:var(--accent);">' + shortId + '…</span>' : ''}
    ${h.entryClass && h.entryClass !== '(manifest)' ? ' · ' + escHtml(h.entryClass) : ''}
  </div>
  <div style="font-size:9px;color:var(--text3);margin-top:2px;">${ts}</div>
  ${h.jobId ? `
  <button onclick="_jarNavigateToJob('${escHtml(h.jobId)}')"
          title="Open this job in the Job Graph"
          style="font-size:9px;padding:2px 7px;border-radius:2px;margin-top:5px;
                 background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.25);
                 color:var(--accent);cursor:pointer;">◈ View Job</button>` : ''}
</div>`;
    }).join('');
}

function _jarNavigateToJob(jobId) {
    if (typeof closeModal === 'function') closeModal('modal-project-manager');
    const jgBtn = document.querySelector('[onclick="switchResultTab(\'jobgraph\',this)"]');
    if (jgBtn) jgBtn.click();
    setTimeout(() => {
        const sel = document.getElementById('jg-job-select');
        if (sel && sel.querySelector(`option[value="${jobId}"]`)) {
            sel.value = jobId;
            if (typeof loadJobGraph === 'function') loadJobGraph(jobId);
        } else if (typeof refreshJobGraphList === 'function') {
            refreshJobGraphList().then(() => {
                const sel2 = document.getElementById('jg-job-select');
                if (sel2) {
                    sel2.value = jobId;
                    if (typeof loadJobGraph === 'function') loadJobGraph(jobId);
                }
            }).catch(() => {});
        }
    }, 500);
}

// ── Utility ───────────────────────────────────────────────────────────────────
function _jarFmtBytes(b) {
    b = Number(b) || 0;
    if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GB';
    if (b >= 1048576)    return (b / 1048576).toFixed(1)    + ' MB';
    if (b >= 1024)       return (b / 1024).toFixed(0)       + ' KB';
    return b + ' B';
}

// ── CSS ───────────────────────────────────────────────────────────────────────
(function _jarInjectCSS() {
    const style = document.createElement('style');
    style.textContent = `
    #jar-dropzone:hover {
        border-color: rgba(0,212,170,0.55) !important;
        background:   rgba(0,212,170,0.05) !important;
    }
    #jar-dropzone.dz-active {
        border-color: var(--accent) !important;
        background:   rgba(0,212,170,0.08) !important;
        transform: scale(1.01);
    }
    #jar-cluster-list::-webkit-scrollbar,
    #jar-history-list::-webkit-scrollbar { width: 4px; }
    #jar-cluster-list::-webkit-scrollbar-thumb,
    #jar-history-list::-webkit-scrollbar-thumb {
        background: var(--border2); border-radius: 2px;
    }
    @keyframes _jarProgressPulse {
        0%, 100% { opacity: 1; }
        50%       { opacity: 0.55; }
    }
    #jar-progress-bar { animation: _jarProgressPulse 1.4s ease-in-out infinite; }
    `;
    document.head.appendChild(style);
})();