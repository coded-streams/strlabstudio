/* udf-manager-upload-fix.js — v2.1.0
 * ─────────────────────────────────────────────────────────────────────────
 * Correct architecture for the pre-built Studio image setup:
 *
 *   Browser PUT /udf-jars/x.jar
 *     → Studio nginx (same origin, port 3030)
 *     → saved to /var/www/udf-jars/x.jar  (flink-cluster_udf-jars volume)
 *
 *   SQL Gateway: ADD JAR '/var/www/udf-jars/x.jar'
 *     → reads directly from /var/www/udf-jars/  (same shared volume)
 *     → works because flink-sql-gateway now mounts udf-jars:/var/www/udf-jars
 *
 * PREREQUISITES (already in place except #2):
 *   1. studio.conf         — has /udf-jars/ WebDAV location ✓
 *   2. cluster compose     — flink-sql-gateway needs: udf-jars:/var/www/udf-jars ← THE FIX
 *   3. studio compose      — mounts flink-cluster_udf-jars:/var/www/udf-jars ✓
 *   4. entrypoint.sh       — chowns /var/www/udf-jars to nginx ✓
 * ─────────────────────────────────────────────────────────────────────────
 */

// ── Path helpers ──────────────────────────────────────────────────────────────

/** Browser PUT URL — always same-origin since Studio nginx handles /udf-jars/ */
function _jBrowserPutUrl(jarName) {
    const override = (document.getElementById('udf-upload-base-override')?.value || '').trim();
    const base = override ? override.replace(/\/+$/, '') : window.location.origin;
    return base + '/udf-jars/' + encodeURIComponent(jarName);
}

/** Filesystem path used for ADD JAR inside the Gateway container */
function _jGatewayFsPath(jarName) {
    const override = (document.getElementById('udf-gateway-path-override')?.value || '').trim();
    const base = override ? override.replace(/\/+$/, '') : '/var/www/udf-jars';
    return base + '/' + jarName;
}

function _jUpdatePreviews() {
    const name = _selJar?.name || 'your-udf.jar';
    const putEl  = document.getElementById('udf-put-url-preview');
    const pathEl = document.getElementById('udf-addjar-path-preview');
    if (putEl)  putEl.textContent  = _jBrowserPutUrl(name);
    if (pathEl) pathEl.textContent = _jGatewayFsPath(name);
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
    if (!file.name.endsWith('.jar')) { _jStatus('✗ Only .jar files accepted.', 'var(--red)'); return; }
    _selJar = file;
    window._lastUploadedJarName = file.name;
    const i  = document.getElementById('udf-jar-file-info'); if (i) i.style.display = 'block';
    const n  = document.getElementById('udf-jar-fname');     if (n) n.textContent = file.name;
    const sz = document.getElementById('udf-jar-fsize');     if (sz) sz.textContent = _fmtB(file.size);
    _jStatus('', '');
    const w = document.getElementById('udf-jar-addjar-wrap'); if (w) w.style.display = 'none';
    _jUpdatePreviews();
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

// ── MAIN UPLOAD ───────────────────────────────────────────────────────────────
async function _jUpload() {
    if (!_selJar)       { _jStatus('✗ Select a JAR file first.', 'var(--red)'); return; }
    if (!state.gateway) { _jStatus('✗ Not connected to a Flink session.', 'var(--red)'); return; }

    const pw      = document.getElementById('udf-jar-progress-wrap');
    const pb      = document.getElementById('udf-jar-prog-bar');
    const pp      = document.getElementById('udf-jar-prog-pct');
    const pl      = document.getElementById('udf-jar-prog-label');
    const msgEl   = document.getElementById('udf-jar-addjar-msg');
    const wrapEl  = document.getElementById('udf-jar-addjar-wrap');
    const copyBtn = document.getElementById('udf-jar-copy-path');

    if (pw)     pw.style.display     = 'block';
    if (wrapEl) wrapEl.style.display = 'none';

    const jarName = _selJar.name;
    const putUrl  = _jBrowserPutUrl(jarName);
    const fsPath  = _jGatewayFsPath(jarName);
    const bytes   = await _selJar.arrayBuffer();

    // ── Step 1: PUT JAR to Studio nginx ──────────────────────────────────────
    if (pl) pl.textContent = `Uploading ${jarName}…`;

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
                const hints = {
                    404: `404 — /udf-jars/ not found in Studio nginx.\nstudio.conf should have a /udf-jars/ WebDAV location block.`,
                    405: `405 Method Not Allowed — dav_methods PUT not enabled in studio.conf.`,
                    403: `403 Forbidden — /var/www/udf-jars/ not writable.\nentrypoint.sh should run: chown -R nginx:nginx /var/www/udf-jars`,
                    413: `413 Too Large — add client_max_body_size 512m; to studio.conf.`,
                };
                reject(new Error(hints[xhr.status] || `HTTP ${xhr.status} — ${xhr.statusText}`));
            };
            xhr.onerror = () => reject(new Error(
                `Cannot reach ${putUrl}\n` +
                `Is Studio running and accessible at ${window.location.origin}?`
            ));
            xhr.open('PUT', putUrl);
            xhr.setRequestHeader('Content-Type', 'application/java-archive');
            xhr.send(bytes);
        });

        if (pb) pb.style.width = '75%';
        if (pp) pp.textContent = '75%';
        addLog('OK', `JAR uploaded: ${jarName} → ${putUrl}`);

    } catch(putErr) {
        if (pw) pw.style.display = 'none';
        _jStatus(`✗ ${putErr.message.split('\n')[0]}`, 'var(--red)');
        if (wrapEl) wrapEl.style.display = 'block';
        if (msgEl)  msgEl.innerHTML = `<span style="color:var(--red);">✗ Upload failed</span>\n\n${escHtml(putErr.message)}`;
        addLog('ERR', 'JAR PUT failed: ' + putErr.message);
        return;
    }

    // ── Step 2: ADD JAR via shared volume filesystem path ─────────────────────
    // Both Studio and flink-sql-gateway mount flink-cluster_udf-jars at
    // /var/www/udf-jars/. The file is now on that volume, so the Gateway
    // can read it directly — no HTTP fetch needed.
    if (pl) pl.textContent = `Running ADD JAR '${fsPath}'…`;
    if (pb) pb.style.width = '82%';

    window._lastUploadedJarPath = fsPath;
    window._lastUploadedJarName = jarName;

    try {
        await _runQ(`ADD JAR '${fsPath.replace(/'/g, "\\'")}'`);

        if (pb) pb.style.width = '100%';
        if (pp) pp.textContent = '100%';

        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML =
            `<span style="color:var(--green);">✓ Upload + ADD JAR succeeded</span>\n\n` +
            `Uploaded to:          ${escHtml(putUrl)}\n` +
            `Gateway loaded from:  <strong style="color:var(--accent);">${escHtml(fsPath)}</strong>\n\n` +
            `JAR is on the session classpath. → Click "Go to Register UDF".`;

        _jStatus(`✓ ${jarName} ready on session classpath.`, 'var(--green)');
        toast(`${jarName} ready — go to Register UDF`, 'ok');

        const pathInput = document.getElementById('s1-path');
        if (pathInput) pathInput.value = fsPath;
        const b1 = document.getElementById('s1-badge');
        if (b1) { b1.dataset.s = 'ok'; b1.textContent = 'jar loaded ✓'; }
        const jd = document.getElementById('s1-jars');
        if (jd) {
            jd.style.display = 'block';
            jd.style.color   = 'var(--green)';
            jd.textContent   = '✓ JAR on classpath:\n  ' + fsPath;
        }

    } catch(addErr) {
        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';

        const isNotFound = addErr.message.toLowerCase().includes('not found')
            || addErr.message.toLowerCase().includes('filenotfound');

        const fix = isNotFound
            ? `The volume is NOT mounted on flink-sql-gateway.\n\n` +
            `Add this to flink-sql-gateway in your CLUSTER docker-compose.yml:\n\n` +
            `  volumes:\n` +
            `    - udf-jars:/var/www/udf-jars\n\n` +
            `And add to the volumes section at the bottom:\n\n` +
            `  volumes:\n` +
            `    udf-jars:\n\n` +
            `Then restart the gateway:\n` +
            `  docker compose up -d --no-deps flink-sql-gateway`
            : `Check the Gateway logs:\n  docker compose logs flink-sql-gateway`;

        if (msgEl) msgEl.innerHTML =
            `<span style="color:var(--green);">✓ JAR saved to shared volume</span>\n\n` +
            `<span style="color:var(--red);">✗ ADD JAR failed: ${escHtml(addErr.message.split('\n')[0])}</span>\n\n` +
            `${escHtml(fix)}`;

        const pathInput = document.getElementById('s1-path');
        if (pathInput) pathInput.value = fsPath;
        _jStatus('⚠ JAR saved — ADD JAR failed. See details above.', 'var(--yellow,#f5a623)');
        addLog('WARN', 'ADD JAR failed: ' + addErr.message);
    }

    _jClear();
    if (pw) setTimeout(() => { if (pw) pw.style.display = 'none'; }, 3000);
    setTimeout(_jLoadList, 600);
}

// ── JAR list ──────────────────────────────────────────────────────────────────
async function _jLoadList() {
    const el = document.getElementById('udf-jar-list'); if (!el) return;
    el.innerHTML = '<div style="font-size:11px;color:var(--text3);">Loading…</div>';
    try {
        const r = await fetch(window.location.origin + '/udf-jars/', { signal: AbortSignal.timeout(4000) });
        if (!r.ok) throw new Error(
            `HTTP ${r.status} — /udf-jars/ not available.\n` +
            `Check studio.conf has the /udf-jars/ WebDAV location block.`
        );
        const text = await r.text();
        let jars = [];
        try { const parsed = JSON.parse(text); jars = parsed.filter(f => f.name?.endsWith('.jar')); } catch(_) {}
        if (!jars.length) {
            el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>';
            return;
        }
        el.innerHTML = jars.map(j => {
            const name   = j.name;
            const fsPath = _jGatewayFsPath(name);
            return `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);
                border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
              <span>📦</span>
              <div style="flex:1;min-width:0;overflow:hidden;">
                <div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
                     font-family:var(--mono);color:var(--text0);">${escHtml(name)}</div>
                <div style="font-size:9px;color:var(--text3);font-family:var(--mono);margin-top:1px;">${escHtml(fsPath)}</div>
              </div>
              <span style="color:var(--text3);flex-shrink:0;">${j.size ? _fmtB(j.size) : '—'}</span>
              <button onclick="_jUseInReg('${escHtml(fsPath)}','${escHtml(name)}')"
                style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid var(--border);
                       background:var(--bg3);color:var(--text1);cursor:pointer;">Use →</button>
              <button onclick="_jDelete('${escHtml(name)}')"
                style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);
                       background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Del</button>
            </div>`;
        }).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);white-space:pre-wrap;">${escHtml(e.message)}</div>`;
    }
}

function _jUseInReg(fsPath, name) {
    switchUdfTab('register');
    const p = document.getElementById('s1-path');
    if (p) p.value = fsPath;
    window._lastUploadedJarPath = fsPath;
    window._lastUploadedJarName = name;
    toast('Path pre-filled — click ADD JAR to load into session', 'info');
}

async function _jDelete(name) {
    if (!confirm('Delete ' + name + '?')) return;
    try {
        const r = await fetch(window.location.origin + '/udf-jars/' + encodeURIComponent(name), { method: 'DELETE' });
        if (!r.ok && r.status !== 404) throw new Error('HTTP ' + r.status);
        toast(name + ' deleted', 'ok');
        _jLoadList();
    } catch(e) { toast('Delete failed: ' + e.message, 'err'); }
}

// Compatibility stubs
function _jSvrTest()    {}
function _jSvrPreview() {}
function _getJarBase()  { return window.location.origin; }
function _getContainerJarPath(name) { return _jGatewayFsPath(name); }

// ── Patch Upload pane HTML ────────────────────────────────────────────────────
(function _patchUploadPane() {
    function _doInject() {
        const pane = document.getElementById('udf-pane-upload');
        if (!pane || pane.dataset.v21patched) return;
        pane.dataset.v21patched = '1';
        pane.innerHTML = `
<p style="font-size:12px;color:var(--text2);margin:0 0 12px;line-height:1.7;">
  Upload JAR → Studio nginx stores it on the shared Docker volume →
  SQL Gateway reads it directly from the same volume path.
</p>

<!-- Flow summary -->
<div style="background:rgba(0,212,170,0.05);border:1px solid rgba(0,212,170,0.2);
     border-radius:var(--radius);padding:10px 14px;margin-bottom:12px;font-size:11px;
     color:var(--text1);line-height:2;">
  <div style="font-size:10px;font-weight:700;color:var(--accent);letter-spacing:0.8px;
              text-transform:uppercase;margin-bottom:4px;">Upload flow</div>
  Browser PUT →
    <code style="color:var(--accent);">${window.location.origin}/udf-jars/</code>
    <span style="color:var(--text3);font-size:10px;">(Studio nginx, same origin)</span><br>
  Gateway ADD JAR →
    <code id="udf-addjar-path-preview" style="color:var(--text2);">/var/www/udf-jars/your-udf.jar</code>
    <span style="color:var(--text3);font-size:10px;">(shared volume filesystem)</span>
</div>

<!-- Advanced overrides -->
<details style="margin-bottom:12px;">
  <summary style="font-size:10px;color:var(--text3);cursor:pointer;user-select:none;
                  letter-spacing:0.5px;text-transform:uppercase;font-weight:700;">
    ⚙ Advanced overrides
  </summary>
  <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
       padding:10px 14px;margin-top:6px;display:flex;flex-direction:column;gap:8px;">
    <div>
      <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;">
        Browser upload base URL
        <span style="font-weight:400;"> — defaults to current page origin</span>
      </label>
      <input id="udf-upload-base-override" class="field-input" type="text"
        placeholder="${window.location.origin}  (default)"
        style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;"
        oninput="_jUpdatePreviews();" />
    </div>
    <div>
      <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;">
        Gateway filesystem path base
        <span style="font-weight:400;"> — for ADD JAR, must match volume mount in Gateway</span>
      </label>
      <input id="udf-gateway-path-override" class="field-input" type="text"
        placeholder="/var/www/udf-jars  (default)"
        style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;"
        oninput="_jUpdatePreviews();" />
    </div>
  </div>
</details>

<!-- Drop zone -->
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
              text-transform:uppercase;margin-bottom:8px;">Result</div>
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
      JARs on shared volume
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