/* udf-manager-upload-fix.js — v1.5.0
 * Drop-in fix for UDF Manager Upload JAR tab.
 * Load AFTER udf-manager.js in index.html:
 *   <script src="js/udf-manager.js"></script>
 *   <script src="js/udf-manager-upload-fix.js"></script>
 *
 * WHAT THIS FIXES:
 *   The Upload JAR tab used nginx WebDAV PUT to /var/www/udf-jars/
 *   which requires WebDAV configuration and a shared volume mount.
 *   This fix replaces it with Flink's own REST API (/jars/upload)
 *   — no nginx config needed, works with the standard Studio setup.
 *
 * HOW IT WORKS:
 *   1. POST JAR to /jobmanager-api/jars/upload (same as JAR Submit tab)
 *   2. Flink stores it at /tmp/flink-web-upload/<uuid>_<name>.jar
 *   3. That server path is returned and used for ADD JAR in the session
 *   4. ADD JAR makes it available in the SQL Gateway for CREATE FUNCTION
 */

// ═══════════════════════════════════════════════════════════════════════════
// JAR UPLOAD — FIXED v1.5.0
// Uses Flink JobManager REST API (/jars/upload) instead of nginx WebDAV PUT.
// The returned server path (e.g. /tmp/flink-web-upload/uuid_name.jar) is used
// for ADD JAR — this path exists on both the JobManager and SQL Gateway
// containers since they share the Flink cluster filesystem.
// ═══════════════════════════════════════════════════════════════════════════

function _getJmUploadUrl() {
    // Same logic as _jarXhrBase() in jar-submit.js — uses the Studio proxy
    if (typeof state !== 'undefined' && state.gateway && state.gateway.baseUrl) {
        return state.gateway.baseUrl.replace(/\/flink-api\/?$/, '') + '/jobmanager-api/jars/upload';
    }
    return '/jobmanager-api/jars/upload';
}

function _getJmJarsUrl() {
    if (typeof state !== 'undefined' && state.gateway && state.gateway.baseUrl) {
        return state.gateway.baseUrl.replace(/\/flink-api\/?$/, '') + '/jobmanager-api/jars';
    }
    return '/jobmanager-api/jars';
}

let _selJar = null;

function _jDragOver(e) {
    e.preventDefault();
    const d = document.getElementById('udf-jar-dropzone');
    if (d) { d.style.borderColor = 'var(--accent)'; d.style.background = 'rgba(0,212,170,0.06)'; }
}
function _jDragLeave(e) {
    const d = document.getElementById('udf-jar-dropzone');
    if (d) { d.style.borderColor = 'var(--border2)'; d.style.background = 'var(--bg1)'; }
}
function _jDrop(e) { e.preventDefault(); _jDragLeave(e); const f = e.dataTransfer?.files?.[0]; if (f) _jSetFile(f); }
function _jFileSelected(e) { const f = e.target?.files?.[0]; if (f) _jSetFile(f); }

function _jSetFile(file) {
    if (!file.name.endsWith('.jar')) { _jStatus('✗ Only .jar files.', 'var(--red)'); return; }
    _selJar = file;
    window._lastUploadedJarName = file.name;
    const i  = document.getElementById('udf-jar-file-info'); if (i) i.style.display = 'block';
    const n  = document.getElementById('udf-jar-fname');     if (n) n.textContent = file.name;
    const sz = document.getElementById('udf-jar-fsize');     if (sz) sz.textContent = _fmtB(file.size);
    _jStatus('', '');
    const w = document.getElementById('udf-jar-addjar-wrap'); if (w) w.style.display = 'none';
}

function _jClear() {
    _selJar = null;
    const i = document.getElementById('udf-jar-file-info'); if (i) i.style.display = 'none';
    const f = document.getElementById('udf-jar-input');     if (f) f.value = '';
    _jStatus('', '');
}

function _jStatus(msg, color) {
    const el = document.getElementById('udf-jar-status');
    if (!el) return;
    el.style.color = color || 'var(--text2)';
    el.innerHTML = msg;
}

function _fmtB(b) {
    if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB';
    if (b >= 1024)    return (b / 1024).toFixed(1) + ' KB';
    return b + ' B';
}

function _jCopyAddJar() {
    const p = window._lastUploadedJarPath;
    if (!p) return;
    navigator.clipboard.writeText(`ADD JAR '${p}';`).then(() => toast('Copied', 'ok'));
}

async function _jUpload() {
    if (!_selJar) { _jStatus('✗ Select a JAR file first.', 'var(--red)'); return; }
    if (!state.gateway) { _jStatus('✗ Not connected to a Flink session.', 'var(--red)'); return; }

    const pw      = document.getElementById('udf-jar-progress-wrap');
    const pb      = document.getElementById('udf-jar-prog-bar');
    const pp      = document.getElementById('udf-jar-prog-pct');
    const pl      = document.getElementById('udf-jar-prog-label');
    const msgEl   = document.getElementById('udf-jar-addjar-msg');
    const wrapEl  = document.getElementById('udf-jar-addjar-wrap');
    const copyBtn = document.getElementById('udf-jar-copy-path');

    if (pw) pw.style.display = 'block';
    if (wrapEl) wrapEl.style.display = 'none';

    const uploadUrl = _getJmUploadUrl();
    const jarName   = _selJar.name;

    if (pl) pl.textContent = 'Uploading ' + jarName + ' to Flink cluster…';

    // ── Step 1: Upload JAR to Flink JobManager via REST API ─────────────────
    let serverPath = '';
    try {
        serverPath = await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', uploadUrl);

            // Forward auth headers if the gateway requires them
            if (state.gateway?.headers) {
                Object.entries(state.gateway.headers).forEach(([k, v]) => {
                    if (k.toLowerCase() !== 'content-type') xhr.setRequestHeader(k, v);
                });
            }

            xhr.upload.onprogress = e => {
                if (e.lengthComputable) {
                    const p = Math.round(e.loaded / e.total * 70);
                    if (pb) pb.style.width = p + '%';
                    if (pp) pp.textContent = p + '%';
                }
            };

            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    try {
                        const resp = JSON.parse(xhr.responseText || 'null');
                        if (!resp) { reject(new Error('Empty response from /jars/upload')); return; }
                        // Flink returns: { filename: "/tmp/flink-web-upload/uuid_name.jar", status: "success" }
                        const fn = resp.filename || resp.jarId || resp.id || '';
                        if (!fn) { reject(new Error('No filename in upload response')); return; }
                        resolve(fn); // full server path
                    } catch(e) {
                        reject(new Error('Could not parse upload response: ' + xhr.responseText.slice(0, 200)));
                    }
                } else {
                    let msg = `Upload failed: HTTP ${xhr.status}`;
                    try { const j = JSON.parse(xhr.responseText); msg = j.errors?.[0] || j.message || msg; } catch(_) {}
                    if (xhr.status === 0)   msg = 'Network error — check Studio proxy is running';
                    if (xhr.status === 404) msg = 'Flink JobManager /jars/upload not found — is the cluster running?';
                    reject(new Error(msg));
                }
            };
            xhr.onerror = () => reject(new Error('Network error uploading JAR'));

            const fd = new FormData();
            fd.append('jarfile', _selJar, _selJar.name);
            xhr.send(fd);
        });

        if (pb) pb.style.width = '75%';
        if (pp) pp.textContent = '75%';
        addLog('OK', `JAR uploaded to Flink: ${jarName} → ${serverPath}`);

    } catch(uploadErr) {
        if (pw) pw.style.display = 'none';
        _jStatus(`✗ Upload failed: ${uploadErr.message}`, 'var(--red)');
        if (wrapEl) wrapEl.style.display = 'block';
        if (msgEl) msgEl.innerHTML = `<span style="color:var(--red);">✗ ${escHtml(uploadErr.message)}</span>`;
        addLog('ERR', 'JAR upload failed: ' + uploadErr.message);
        return;
    }

    // ── Step 2: ADD JAR in the SQL Gateway session ───────────────────────────
    // The path returned by Flink is the server-side full path.
    // The SQL Gateway can access this path since it's on the same Flink cluster filesystem.
    if (pl) pl.textContent = 'Running ADD JAR in Gateway session…';
    if (pb) pb.style.width = '80%';

    const addJarPath = serverPath; // use the full server path returned by Flink

    try {
        await _runQ(`ADD JAR '${addJarPath.replace(/'/g, "\\'")}'`);

        window._lastUploadedJarPath = addJarPath;
        window._lastUploadedJarName = jarName;

        if (pb) pb.style.width = '100%';
        if (pp) pp.textContent = '100%';

        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML =
            `<span style="color:var(--green);">✓ JAR uploaded to Flink + ADD JAR succeeded</span>\n\n` +
            `Server path: <strong style="color:var(--accent);">${escHtml(addJarPath)}</strong>\n\n` +
            `JAR is on the session classpath. → Click "Go to Register UDF".`;

        _jStatus(`✓ ${jarName} ready on session classpath.`, 'var(--green)');
        toast(jarName + ' ready — go to Register UDF', 'ok');

        // Pre-fill Step 1 path in Register tab
        const pathInput = document.getElementById('s1-path');
        if (pathInput) pathInput.value = addJarPath;
        const b1 = document.getElementById('s1-badge');
        if (b1) { b1.dataset.s = 'ok'; b1.textContent = 'jar loaded ✓'; }
        const jd = document.getElementById('s1-jars');
        if (jd) { jd.style.display = 'block'; jd.style.color = 'var(--green)'; jd.textContent = '✓ JAR on classpath: ' + addJarPath; }

    } catch(addErr) {
        // Upload succeeded but ADD JAR failed — still show the path so user can try manually
        window._lastUploadedJarPath = addJarPath;
        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML =
            `<span style="color:var(--green);">✓ JAR uploaded to Flink: ${escHtml(serverPath)}</span>\n\n` +
            `<span style="color:var(--red);">✗ ADD JAR failed: ${escHtml(addErr.message)}</span>\n\n` +
            `Try running ADD JAR manually in the Register tab using the path above.`;
        const pathInput = document.getElementById('s1-path');
        if (pathInput) pathInput.value = addJarPath;
        _jStatus('⚠ Uploaded to Flink — ADD JAR failed. Try manually in Register tab.', 'var(--yellow,#f5a623)');
        addLog('WARN', 'ADD JAR failed after upload: ' + addErr.message);
    }

    _jClear();
    if (pw) setTimeout(() => { if (pw) pw.style.display = 'none'; }, 3000);

    // Refresh the cluster JAR list
    setTimeout(_jLoadList, 500);
}

// Load JAR list from Flink JobManager (replaces nginx directory listing)
async function _jLoadList() {
    const el = document.getElementById('udf-jar-list'); if (!el) return;

    if (!state.gateway) {
        el.innerHTML = '<div style="font-size:11px;color:var(--text3);">Not connected to cluster.</div>';
        return;
    }

    el.innerHTML = '<div style="font-size:11px;color:var(--text3);">Loading…</div>';

    try {
        const jarsUrl = _getJmJarsUrl();
        const r       = await fetch(jarsUrl);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const data = await r.json();

        // Handle all Flink response shapes
        const jars = Array.isArray(data)        ? data
            : Array.isArray(data?.files) ? data.files
                : Array.isArray(data?.jars)  ? data.jars
                    : [];

        if (!jars.length) {
            el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs on cluster yet. Upload one above.</div>';
            return;
        }

        el.innerHTML = jars.map(j => {
            const rawId  = j.id || j.filename || j.name || '';
            const name   = j.name || rawId.split('/').pop();
            const size   = _fmtB(j.size || 0);
            const safeId = encodeURIComponent(rawId.split('/').pop() || rawId);
            return `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);
                border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
              <span>📦</span>
              <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
                          font-family:var(--mono);color:var(--text0);" title="${escHtml(rawId)}">${escHtml(name)}</div>
              <span style="color:var(--text3);flex-shrink:0;">${size}</span>
              <button onclick="_jUseInReg('${escHtml(rawId)}','${escHtml(name)}')"
                style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid var(--border);
                       background:var(--bg3);color:var(--text1);cursor:pointer;">Use →</button>
              <button onclick="_jDeleteJar('${safeId}','${escHtml(name)}')"
                style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);
                       background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Delete</button>
            </div>`;
        }).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);">Could not load JAR list: ${escHtml(e.message)}</div>`;
    }
}

function _jUseInReg(serverPath, name) {
    switchUdfTab('register');
    // Use the full server path for ADD JAR
    const p = document.getElementById('s1-path');
    if (p) p.value = serverPath;
    window._lastUploadedJarPath = serverPath;
    window._lastUploadedJarName = name;
    toast('Path pre-filled in Step 1 — click SHOW JARS to verify, or ADD JAR if needed', 'info');
}

async function _jDeleteJar(encodedId, displayName) {
    if (!confirm('Delete "' + displayName + '" from the Flink cluster?\n\nRunning jobs are not affected.')) return;
    try {
        const base = _getJmJarsUrl();
        const r    = await fetch(base + '/' + encodedId, { method: 'DELETE' });
        if (!r.ok && r.status !== 204) throw new Error('HTTP ' + r.status);
        toast(displayName + ' deleted', 'ok');
        _jLoadList();
    } catch(e) {
        toast('Delete failed: ' + e.message, 'err');
    }
}

// Stub out old nginx-based helpers so nothing breaks if called
function _jSvrTest()    { /* replaced by Flink REST — no nginx needed */ }
function _jSvrPreview() { /* replaced by Flink REST — no nginx needed */ }
function _getJarBase()  { return _getJmUploadUrl().replace('/jars/upload', ''); }
function _getJmBase()   { return _getJmJarsUrl().replace('/jars', ''); }
function _getContainerJarPath(name) { return '/tmp/flink-web-upload/' + name; }

// ── Connection check helper (used by the patched Upload pane UI) ──────────────
function _jCheckConn() {
    const dot   = document.getElementById('upl-conn-dot');
    const label = document.getElementById('upl-conn-label');
    if (!state.gateway) {
        if (dot)   dot.style.background   = 'var(--red)';
        if (label) label.textContent      = 'Not connected — connect to a Flink cluster first';
        return;
    }
    if (dot)   dot.style.background   = 'var(--green)';
    const url = _getJmUploadUrl();
    if (label) label.textContent = 'Connected — uploads go to: ' + url.replace('/jars/upload','');
}

// ── Patch the Upload JAR pane HTML on modal open ──────────────────────────────
// Replaces the nginx WebDAV UI with the Flink REST API UI.
(function _patchUploadPane() {
    const ORIGINAL_OPEN = window.openUdfManager;
    window.openUdfManager = function() {
        if (ORIGINAL_OPEN) ORIGINAL_OPEN.apply(this, arguments);
        // Replace upload pane content after modal is built
        setTimeout(() => {
            const pane = document.getElementById('udf-pane-upload');
            if (!pane || pane.dataset.patched) return;
            pane.dataset.patched = '1';
            pane.innerHTML = `<!-- ══════════════════════════════════ UPLOAD JAR ════════════════════ -->
    <div id="udf-pane-upload" style="padding:20px;display:none;">
      <p style="font-size:12px;color:var(--text2);margin:0 0 14px;line-height:1.7;">
        Upload a JAR directly to the Flink cluster and register it in the active Gateway session.
        <strong style="color:var(--accent);">No nginx configuration needed</strong> — uses the Flink REST API.
      </p>

      <!-- Connection status -->
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
                  padding:9px 14px;margin-bottom:14px;display:flex;align-items:center;gap:10px;">
        <div id="upl-conn-dot" style="width:8px;height:8px;border-radius:50%;background:var(--text3);flex-shrink:0;"></div>
        <div style="flex:1;font-size:11px;color:var(--text2);" id="upl-conn-label">Connect to cluster to upload JARs</div>
        <button class="btn btn-secondary" style="font-size:10px;padding:3px 9px;" onclick="_jCheckConn()">⟳ Check</button>
      </div>

      <div id="udf-jar-dropzone"
        style="border:2px dashed var(--border2);border-radius:var(--radius);padding:28px 20px;text-align:center;
               cursor:pointer;background:var(--bg1);margin-bottom:12px;transition:border-color 0.15s,background 0.15s;"
        onclick="document.getElementById('udf-jar-input').click()"
        ondragover="_jDragOver(event)" ondragleave="_jDragLeave(event)" ondrop="_jDrop(event)">
        <div style="font-size:26px;margin-bottom:6px;">📦</div>
        <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">Drop JAR here or click to browse</div>
        <div style="font-size:11px;color:var(--text3);">Accepts <code>.jar</code> files · Uploaded directly to Flink cluster</div>
        <input type="file" id="udf-jar-input" accept=".jar" style="display:none;" onchange="_jFileSelected(event)" />
      </div>

      <div id="udf-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);
           padding:8px 12px;border-radius:var(--radius);margin-bottom:12px;">
        <div style="display:flex;align-items:center;gap:10px;">
          <span>📦</span>
          <div style="flex:1;">
            <div id="udf-jar-fname" style="font-family:var(--mono);color:var(--text0);font-weight:600;font-size:12px;"></div>
            <div id="udf-jar-fsize" style="color:var(--text3);font-size:11px;margin-top:2px;"></div>
          </div>
          <button onclick="_jClear()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
        </div>
      </div>

      <div id="udf-jar-progress-wrap" style="display:none;margin-bottom:12px;">
        <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text2);margin-bottom:4px;">
          <span id="udf-jar-prog-label">Uploading…</span><span id="udf-jar-prog-pct">0%</span>
        </div>
        <div style="background:var(--bg3);border-radius:4px;height:5px;overflow:hidden;">
          <div id="udf-jar-prog-bar" style="height:100%;width:0%;background:var(--accent);border-radius:4px;transition:width 0.2s;"></div>
        </div>
      </div>

      <div id="udf-jar-status" style="font-size:12px;min-height:16px;margin-bottom:12px;line-height:1.8;"></div>

      <div id="udf-jar-addjar-wrap" style="display:none;background:var(--bg2);border:1px solid var(--border);
           border-radius:var(--radius);padding:12px 14px;margin-bottom:12px;">
        <div style="font-size:10px;font-weight:700;color:var(--accent);letter-spacing:0.8px;
                    text-transform:uppercase;margin-bottom:8px;">Upload Result</div>
        <div id="udf-jar-addjar-msg" style="font-size:11px;font-family:var(--mono);color:var(--text1);
             line-height:1.9;white-space:pre-wrap;"></div>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
          <button id="udf-jar-copy-path" onclick="_jCopyAddJar()"
            style="display:none;font-size:10px;padding:3px 10px;border-radius:2px;
                   background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">
            Copy ADD JAR SQL
          </button>
          <button onclick="switchUdfTab('register')"
            style="font-size:10px;padding:3px 10px;border-radius:2px;background:rgba(0,212,170,0.1);
                   border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">
            → Go to Register UDF
          </button>
        </div>
      </div>

      <button class="btn btn-primary" style="font-size:12px;width:100%;padding:10px;" onclick="_jUpload()">
        ⬆ Upload JAR to Flink Cluster
      </button>

      <div style="margin-top:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">
            JARs on Flink cluster
          </span>
          <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_jLoadList()">⟳ Refresh</button>
        </div>
        <div id="udf-jar-list">
          <div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div>
        </div>
      </div>
    </div>`;
            // Trigger connection check
            _jCheckConn();
        }, 50);
    };
    // Also patch switchUdfTab to check conn when upload tab opens
    const ORIG_SWITCH = window.switchUdfTab;
    if (typeof ORIG_SWITCH === 'function') {
        window.switchUdfTab = function(tab) {
            ORIG_SWITCH.apply(this, arguments);
            if (tab === 'upload') {
                setTimeout(() => { _jCheckConn(); _jLoadList(); }, 80);
            }
        };
    }
})();