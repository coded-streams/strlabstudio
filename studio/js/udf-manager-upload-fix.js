/* udf-manager-upload-fix.js — v1.7.0
 * ─────────────────────────────────────────────────────────────────────────
 * Drop-in fix for the UDF Manager Upload JAR tab.
 * Load AFTER udf-manager.js in index.html:
 *
 *   <script src="js/udf-manager.js"></script>
 *   <script src="js/udf-manager-upload-fix.js"></script>
 *
 * HOW IT WORKS (tuned for this docker-compose setup):
 *
 *   Browser (Studio, wherever it runs):
 *     PUT http://localhost:8084/udf-jars/my.jar
 *     → nginx (flink-gateway-cors-proxy) stores to /var/www/udf-jars/my.jar
 *
 *   SQL Gateway (inside Docker on kafka-network-02):
 *     ADD JAR 'http://flink-gateway-cors-proxy:8084/udf-jars/my.jar'
 *     → Gateway resolves flink-gateway-cors-proxy by Docker DNS
 *     → nginx serves the file over HTTP
 *     → Flink downloads it to its temp dir and adds to session classpath
 *
 *   No shared volume on the Gateway container needed.
 *   No filesystem path assumptions. Works across container restarts.
 *
 * PREREQUISITES (one-time setup):
 *   1. Replace nginx/flink-cors.conf with the provided flink-cors.conf
 *      (adds /udf-jars/ location with WebDAV PUT + autoindex)
 *   2. Add volume mount to flink-gateway-cors-proxy in docker-compose.yml:
 *        volumes:
 *          - ./nginx/flink-cors.conf:/etc/nginx/conf.d/default.conf:ro
 *          - udf-jars:/var/www/udf-jars          ← add this
 *   3. docker-compose up -d --no-deps flink-gateway-cors-proxy
 * ─────────────────────────────────────────────────────────────────────────
 */

// ── URL constants ─────────────────────────────────────────────────────────────
// BROWSER_JAR_BASE: where the browser PUTs the JAR (public port)
// GATEWAY_JAR_BASE: where the SQL Gateway fetches it from (Docker internal DNS)
//
// The user can override GATEWAY_JAR_BASE in the UI if their setup differs.

function _jBrowserBase() {
    // Browser always talks to nginx via the public port
    // Auto-detect: use current page origin but swap port to 8084 (cors proxy)
    const origin = window.location.origin;
    // If running on 8084 already, use as-is. Otherwise point to 8084.
    if (origin.includes(':8084')) return origin + '/udf-jars';
    // Replace whatever port is in the origin with 8084
    return origin.replace(/:\d+$/, ':8084') + '/udf-jars';
}

function _jGatewayBase() {
    const override = (document.getElementById('udf-gateway-base-input')?.value || '').trim();
    if (override) return override.replace(/\/+$/, '');
    // Default: Docker internal DNS name for the cors proxy container
    return 'http://flink-gateway-cors-proxy:8084/udf-jars';
}

function _jBrowserPutUrl(jarName) {
    return _jBrowserBase() + '/' + encodeURIComponent(jarName);
}
function _jGatewayAddJarUrl(jarName) {
    return _jGatewayBase() + '/' + encodeURIComponent(jarName);
}

// ── Drag & Drop ───────────────────────────────────────────────────────────────
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
function _jDrop(e)         { e.preventDefault(); _jDragLeave(e); const f = e.dataTransfer?.files?.[0]; if (f) _jSetFile(f); }
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
    _jUpdateUrlPreview();
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

function _jUpdateUrlPreview() {
    const el = document.getElementById('upl-url-preview');
    if (!el) return;
    const name = _selJar?.name || 'your-udf.jar';
    el.textContent = _jGatewayAddJarUrl(name);
}

// ── MAIN UPLOAD FUNCTION ─────────────────────────────────────────────────────
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

    if (pw)    pw.style.display    = 'block';
    if (wrapEl) wrapEl.style.display = 'none';

    const jarName      = _selJar.name;
    const browserPutUrl = _jBrowserPutUrl(jarName);
    const gatewayUrl    = _jGatewayAddJarUrl(jarName);
    const bytes         = await _selJar.arrayBuffer();

    // ── STEP 1: Browser PUTs JAR to nginx via public port ────────────────────
    if (pl) pl.textContent = `Uploading ${jarName} to nginx…`;

    try {
        await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.upload.onprogress = e => {
                if (e.lengthComputable) {
                    const p = Math.round(e.loaded / e.total * 70);
                    if (pb) pb.style.width = p + '%';
                    if (pp) pp.textContent = p + '%';
                }
            };
            xhr.onload = () => {
                if ([200, 201, 204].includes(xhr.status)) { resolve(); return; }
                let msg = `HTTP ${xhr.status}`;
                if (xhr.status === 405) msg = '405 Method Not Allowed — ensure dav_methods PUT; is in your nginx /udf-jars/ location block. See the provided flink-cors.conf.';
                if (xhr.status === 403) msg = '403 Forbidden — /var/www/udf-jars/ not writable. Check the volume mount and directory permissions.';
                if (xhr.status === 413) msg = '413 Request Entity Too Large — add client_max_body_size 512m; to nginx.conf.';
                if (xhr.status === 404) msg = '404 — /udf-jars/ location not in nginx config. Replace nginx/flink-cors.conf with the provided file and restart nginx.';
                reject(new Error(msg));
            };
            xhr.onerror = () => reject(new Error(`Network error — could not reach ${browserPutUrl}. Is the cors proxy running on port 8084?`));
            xhr.open('PUT', browserPutUrl);
            xhr.setRequestHeader('Content-Type', 'application/java-archive');
            xhr.send(bytes);
        });

        if (pb) pb.style.width = '75%';
        if (pp) pp.textContent = '75%';
        addLog('OK', `JAR saved to nginx: ${jarName} → ${browserPutUrl}`);

    } catch(putErr) {
        if (pw) pw.style.display = 'none';
        _jStatus(`✗ Upload failed: ${putErr.message}`, 'var(--red)');
        if (wrapEl) wrapEl.style.display = 'block';
        if (msgEl) msgEl.innerHTML = `<span style="color:var(--red);">✗ ${escHtml(putErr.message)}</span>`;
        addLog('ERR', 'JAR PUT failed: ' + putErr.message);
        return;
    }

    // ── STEP 2: Gateway runs ADD JAR using its own internal HTTP URL ──────────
    // The Gateway resolves flink-gateway-cors-proxy via Docker DNS (kafka-network-02)
    if (pl) pl.textContent = `Running ADD JAR in Gateway session…`;
    if (pb) pb.style.width = '82%';

    window._lastUploadedJarPath = gatewayUrl;
    window._lastUploadedJarName = jarName;

    try {
        await _runQ(`ADD JAR '${gatewayUrl}'`);

        if (pb) pb.style.width = '100%';
        if (pp) pp.textContent = '100%';

        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML =
            `<span style="color:var(--green);">✓ Uploaded + ADD JAR succeeded</span>\n\n` +
            `Browser uploaded to:\n  ${escHtml(browserPutUrl)}\n\n` +
            `Gateway fetched via:\n  <strong style="color:var(--accent);">${escHtml(gatewayUrl)}</strong>\n\n` +
            `JAR is on the session classpath. → Click "Go to Register UDF".`;

        _jStatus(`✓ ${jarName} ready on session classpath.`, 'var(--green)');
        toast(jarName + ' ready — go to Register UDF', 'ok');

        // Pre-fill Step 1 in Register tab with the gateway URL
        const pathInput = document.getElementById('s1-path');
        if (pathInput) pathInput.value = gatewayUrl;
        const b1 = document.getElementById('s1-badge');
        if (b1) { b1.dataset.s = 'ok'; b1.textContent = 'jar loaded ✓'; }
        const jd = document.getElementById('s1-jars');
        if (jd) {
            jd.style.display = 'block';
            jd.style.color   = 'var(--green)';
            jd.textContent   = '✓ JAR on classpath:\n  ' + gatewayUrl;
        }

    } catch(addErr) {
        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML =
            `<span style="color:var(--green);">✓ JAR saved to nginx: ${escHtml(jarName)}</span>\n\n` +
            `<span style="color:var(--red);">✗ ADD JAR failed: ${escHtml(addErr.message)}</span>\n\n` +
            `The Gateway tried to fetch:\n  ${escHtml(gatewayUrl)}\n\n` +
            `Check that flink-gateway-cors-proxy is on kafka-network-02 and the /udf-jars/ ` +
            `location is configured in nginx. If the container name is different, update the ` +
            `"Gateway-internal URL base" field above and try again.`;

        const pathInput = document.getElementById('s1-path');
        if (pathInput) pathInput.value = gatewayUrl;
        _jStatus('⚠ Saved to nginx — ADD JAR failed. See details above.', 'var(--yellow,#f5a623)');
        addLog('WARN', 'ADD JAR failed: ' + addErr.message + ' | url: ' + gatewayUrl);
    }

    _jClear();
    if (pw) setTimeout(() => { if (pw) pw.style.display = 'none'; }, 3000);
    setTimeout(_jLoadList, 500);
}

// ── JAR list — reads from nginx /udf-jars/ via public port ───────────────────
async function _jLoadList() {
    const el = document.getElementById('udf-jar-list'); if (!el) return;
    el.innerHTML = '<div style="font-size:11px;color:var(--text3);">Loading…</div>';
    const base = _jBrowserBase();
    try {
        const r = await fetch(base + '/', { signal: AbortSignal.timeout(4000) });
        if (!r.ok) throw new Error('HTTP ' + r.status + ' — /udf-jars/ not configured in nginx');
        const text = await r.text();
        let jars = [];
        try { const parsed = JSON.parse(text); jars = parsed.filter(f => f.name?.endsWith('.jar')); } catch(_) {}
        if (!jars.length) {
            el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>';
            return;
        }
        el.innerHTML = jars.map(j => {
            const name       = j.name;
            const gatewayUrl = _jGatewayAddJarUrl(name);
            return `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);
                border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
              <span>📦</span>
              <div style="flex:1;min-width:0;overflow:hidden;">
                <div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);
                     color:var(--text0);" title="${escHtml(gatewayUrl)}">${escHtml(name)}</div>
                <div style="font-size:9px;color:var(--text3);font-family:var(--mono);margin-top:1px;
                     overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(gatewayUrl)}</div>
              </div>
              <span style="color:var(--text3);flex-shrink:0;">${j.size ? _fmtB(j.size) : '—'}</span>
              <button onclick="_jUseInReg('${escHtml(gatewayUrl)}','${escHtml(name)}')"
                style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid var(--border);
                       background:var(--bg3);color:var(--text1);cursor:pointer;flex-shrink:0;">Use →</button>
              <button onclick="_jDelete('${escHtml(name)}')"
                style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);
                       background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;flex-shrink:0;">Del</button>
            </div>`;
        }).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);">
            Could not load: ${escHtml(e.message)}
        </div>`;
    }
}

function _jUseInReg(gatewayUrl, name) {
    switchUdfTab('register');
    const p = document.getElementById('s1-path');
    if (p) p.value = gatewayUrl;
    window._lastUploadedJarPath = gatewayUrl;
    window._lastUploadedJarName = name;
    toast('Path pre-filled in Step 1 — click ADD JAR to load into session', 'info');
}

async function _jDelete(name) {
    if (!confirm('Delete ' + name + '?')) return;
    const url = _jBrowserPutUrl(name);
    try {
        const r = await fetch(url, { method: 'DELETE' });
        if (!r.ok && r.status !== 404) throw new Error('HTTP ' + r.status);
        toast(name + ' deleted', 'ok');
        _jLoadList();
    } catch(e) { toast('Delete failed: ' + e.message, 'err'); }
}

// Stub out old helpers
function _jSvrTest()    {}
function _jSvrPreview() {}
function _getJarBase()  { return _jBrowserBase(); }
function _getJmBase()   { return _jBrowserBase().replace('/udf-jars', ''); }
function _getContainerJarPath(name) { return _jGatewayAddJarUrl(name); }

// ── Patch the Upload JAR pane HTML ────────────────────────────────────────────
(function _patchUploadPane() {
    function _doInject() {
        const pane = document.getElementById('udf-pane-upload');
        if (!pane || pane.dataset.v7patched) return;
        pane.dataset.v7patched = '1';
        pane.innerHTML = `
<p style="font-size:12px;color:var(--text2);margin:0 0 12px;line-height:1.7;">
  Upload JAR to nginx, then ADD JAR via the Docker-internal HTTP URL.
  <strong style="color:var(--accent);">No shared volume needed.</strong>
</p>

<!-- Setup status -->
<div style="background:rgba(0,212,170,0.05);border:1px solid rgba(0,212,170,0.2);
     border-radius:var(--radius);padding:10px 14px;margin-bottom:12px;font-size:11px;color:var(--text1);line-height:1.9;">
  <div style="font-weight:700;color:var(--accent);margin-bottom:4px;font-size:10px;letter-spacing:0.8px;text-transform:uppercase;">How it works</div>
  Browser PUT → <code style="color:var(--accent);">localhost:8084/udf-jars/</code> (nginx)<br>
  Gateway ADD JAR → <code style="color:var(--accent);">http://flink-gateway-cors-proxy:8084/udf-jars/</code> (Docker DNS)
</div>

<!-- Gateway URL override -->
<div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
     padding:10px 14px;margin-bottom:12px;">
  <div style="font-size:10px;color:var(--text3);margin-bottom:5px;font-weight:600;">
    Gateway-internal URL base
    <span style="font-weight:400;margin-left:4px;">(override if your container name differs)</span>
  </div>
  <input id="udf-gateway-base-input" class="field-input" type="text"
    placeholder="http://flink-gateway-cors-proxy:8084/udf-jars  (default)"
    style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;"
    oninput="_jUpdateUrlPreview()" />
  <div style="font-size:10px;color:var(--text3);margin-top:5px;">
    ADD JAR will use: <code id="upl-url-preview" style="color:var(--accent);word-break:break-all;">
      http://flink-gateway-cors-proxy:8084/udf-jars/your-udf.jar
    </code>
  </div>
</div>

<div id="udf-jar-dropzone"
  style="border:2px dashed var(--border2);border-radius:var(--radius);padding:24px 20px;
         text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;
         transition:border-color 0.15s,background 0.15s;"
  onclick="document.getElementById('udf-jar-input').click()"
  ondragover="_jDragOver(event)" ondragleave="_jDragLeave(event)" ondrop="_jDrop(event)">
  <div style="font-size:26px;margin-bottom:6px;">📦</div>
  <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">Drop JAR here or click to browse</div>
  <div style="font-size:11px;color:var(--text3);">Accepts <code>.jar</code> files</div>
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
              text-transform:uppercase;margin-bottom:8px;">Upload &amp; ADD JAR result</div>
  <div id="udf-jar-addjar-msg" style="font-size:11px;font-family:var(--mono);color:var(--text1);
       line-height:1.9;white-space:pre-wrap;word-break:break-all;"></div>
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
  ⬆ Upload JAR &amp; Add to Session
</button>

<div style="margin-top:18px;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
    <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">
      JARs on nginx server
    </span>
    <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_jLoadList()">⟳ Refresh</button>
  </div>
  <div id="udf-jar-list">
    <div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div>
  </div>
</div>`;
    }

    const ORIG_OPEN = window.openUdfManager;
    window.openUdfManager = function () {
        if (ORIG_OPEN) ORIG_OPEN.apply(this, arguments);
        setTimeout(_doInject, 60);
    };

    const ORIG_SWITCH = window.switchUdfTab;
    if (typeof ORIG_SWITCH === 'function') {
        window.switchUdfTab = function (tab) {
            ORIG_SWITCH.apply(this, arguments);
            if (tab === 'upload') {
                setTimeout(() => { _doInject(); _jLoadList(); }, 80);
            }
        };
    }
})();