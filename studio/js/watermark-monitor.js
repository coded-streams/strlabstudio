/**
 * watermark-monitor.js  —  Str:::lab Studio v0.0.22
 * ─────────────────────────────────────────────────────────────────
 * Feature 6: Watermark Health Monitor
 *
 * Real-time panel showing:
 *   • Watermark progress per source vertex (current output watermark)
 *   • Event-time vs processing-time lag per vertex
 *   • Late event drop rate
 *   • Visual timeline of watermark advancement
 *   • Stall alerts when watermark stops advancing
 *   • Window emission readiness indicator
 *
 * Polls Flink JobManager metrics API every 2 seconds.
 * ─────────────────────────────────────────────────────────────────
 */

const _WM = {
    jobId        : null,
    polling      : false,
    pollTimer    : null,
    history      : {},     // vertexId → [{ ts, watermarkMs, procTimeMs }]
    maxHistory   : 120,    // keep 120 samples (4 min at 2s)
    stallThresh  : 10000,  // ms — watermark considered stalled if no advance in 10s
    stalls       : {},     // vertexId → last-advance timestamp
    POLL_MS      : 2000,
};

// ── Entry point ────────────────────────────────────────────────────
function openWatermarkMonitor() {
    if (!document.getElementById('wm-modal')) _wmBuildModal();
    openModal('wm-modal');
    _wmPopulateJobs();
}

// ── Populate jobs ──────────────────────────────────────────────────
async function _wmPopulateJobs() {
    const sel = document.getElementById('wm-job-sel');
    if (!sel) return;
    sel.innerHTML = '<option value="">Loading jobs…</option>';
    try {
        const data  = await jmApi('/jobs/overview');
        const jobs  = (data && data.jobs) ? data.jobs : [];
        const running = jobs.filter(j => j.state === 'RUNNING');
        if (!running.length) {
            sel.innerHTML = '<option value="">No RUNNING jobs on cluster</option>';
            return;
        }
        sel.innerHTML = '<option value="">— Select a running job —</option>' +
            running.map(j => `<option value="${j.jid}">[${j.state}] ${j.name.slice(0,55)}</option>`).join('');
        const jgSel = document.getElementById('jg-job-select');
        if (jgSel && jgSel.value) sel.value = jgSel.value;
    } catch(e) {
        sel.innerHTML = `<option value="">Error: ${e.message}</option>`;
    }
}

// ── Start / stop polling ───────────────────────────────────────────
function _wmStartPolling() {
    const sel = document.getElementById('wm-job-sel');
    const jid = sel ? sel.value : '';
    if (!jid) { toast('Select a running job first', 'err'); return; }
    _WM.jobId   = jid;
    _WM.history = {};
    _WM.stalls  = {};

    if (_WM.polling) _wmStopPolling();
    _WM.polling = true;
    _wmUpdateBtn(true);
    _wmPoll();
    addLog('INFO', `Watermark monitor started for job ${jid.slice(0,8)}…`);
}

function _wmStopPolling() {
    _WM.polling = false;
    if (_WM.pollTimer) { clearTimeout(_WM.pollTimer); _WM.pollTimer = null; }
    _wmUpdateBtn(false);
}

function _wmUpdateBtn(running) {
    const btn = document.getElementById('wm-toggle-btn');
    if (!btn) return;
    btn.textContent = running ? '⏹ Stop' : '▶ Start';
    btn.style.background = running ? 'rgba(255,77,109,0.12)' : 'rgba(0,212,170,0.12)';
    btn.style.color = running ? 'var(--red)' : 'var(--accent)';
    btn.style.borderColor = running ? 'rgba(255,77,109,0.35)' : 'rgba(0,212,170,0.35)';
}

// ── Poll cycle ─────────────────────────────────────────────────────
async function _wmPoll() {
    if (!_WM.polling) return;
    const jid = _WM.jobId;

    try {
        const [plan, detail] = await Promise.all([
            jmApi(`/jobs/${jid}/plan`),
            jmApi(`/jobs/${jid}`),
        ]);

        if (!plan || !plan.plan || !plan.plan.nodes) throw new Error('No plan data');
        const nodes    = plan.plan.nodes;
        const vertices = (detail && detail.vertices) ? detail.vertices : [];
        const now      = Date.now();
        const procTime = now;

        // Fetch watermark metric for each vertex
        const wmResults = [];
        for (const nd of nodes) {
            try {
                const vm = await jmApi(
                    `/jobs/${jid}/vertices/${nd.id}/metrics?get=currentOutputWatermark,numLateRecordsDropped,idleTimeMsPerSecond`
                );
                if (!vm || !Array.isArray(vm)) continue;

                const getSum = key => vm
                    .filter(m => m.id === key || m.id.endsWith('.'+key))
                    .reduce((s,m) => s + (parseFloat(m.value)||0), 0);

                const rawWm   = getSum('currentOutputWatermark');
                const lateRec = getSum('numLateRecordsDropped');
                const idle    = getSum('idleTimeMsPerSecond') / 10;

                // Flink uses Long.MIN_VALUE (-9223372036854775808) when no watermark emitted
                const wmMs = rawWm < -1e15 ? null : rawWm;

                if (!_WM.history[nd.id]) _WM.history[nd.id] = [];
                _WM.history[nd.id].push({ ts: now, wmMs, procTime, lateRec, idle });
                if (_WM.history[nd.id].length > _WM.maxHistory)
                    _WM.history[nd.id].shift();

                // Stall detection
                if (wmMs !== null) {
                    const prev = _WM.history[nd.id].slice(-5,-1);
                    const prevWms = prev.map(p => p.wmMs).filter(w => w !== null);
                    const advancing = prevWms.length === 0 || prevWms.some(w => w < wmMs);
                    if (advancing) _WM.stalls[nd.id] = now;
                    else if (!_WM.stalls[nd.id]) _WM.stalls[nd.id] = prev[0]?.ts || now;
                }

                const vx = vertices.find(v => v.id === nd.id) || {};
                const name = _wmVertexName(nd, vx);
                wmResults.push({ id: nd.id, name, wmMs, procTime, lateRec, idle, nd, vx });
            } catch(_) {}
        }

        _wmRender(wmResults, now);

    } catch(e) {
        _wmSetStatus('Poll error: ' + e.message, 'err');
    }

    if (_WM.polling) {
        _WM.pollTimer = setTimeout(_wmPoll, _WM.POLL_MS);
    }
}

// ── Render ─────────────────────────────────────────────────────────
function _wmRender(results, now) {
    const wrap = document.getElementById('wm-rows');
    if (!wrap) return;

    if (!results.length) {
        wrap.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:20px;text-align:center;">No watermark metrics available — vertices may not be emitting watermarks yet.</div>';
        return;
    }

    // Sort: sources first, then by watermark lag descending
    results.sort((a,b) => {
        const aLag = a.wmMs !== null ? now - a.wmMs : Infinity;
        const bLag = b.wmMs !== null ? now - b.wmMs : Infinity;
        return bLag - aLag;
    });

    wrap.innerHTML = results.map(r => {
        const lagMs      = r.wmMs !== null ? Math.max(0, now - r.wmMs) : null;
        const lagSec     = lagMs !== null ? (lagMs / 1000).toFixed(1) : '—';
        const wmStr      = r.wmMs !== null ? new Date(r.wmMs).toISOString().replace('T',' ').slice(0,23) : 'No watermark';
        const stallMs    = _WM.stalls[r.id] ? now - _WM.stalls[r.id] : 0;
        const stalled    = stallMs > _WM.stallThresh && r.wmMs !== null;
        const lagColor   = lagMs === null ? 'var(--text3)'
            : lagMs < 5000  ? 'var(--green)'
                : lagMs < 30000 ? 'var(--yellow)'
                    : 'var(--red)';
        const lagBar     = lagMs !== null ? Math.min(100, lagMs / 600) : 0; // 60s = 100%

        // Sparkline from history
        const hist = (_WM.history[r.id] || []).filter(h => h.wmMs !== null);
        const sparkSvg = _wmSparkline(hist, now);

        const stallWarning = stalled
            ? `<div style="font-size:9px;color:var(--red);margin-top:3px;">
           ⚠ Watermark stalled for ${(stallMs/1000).toFixed(0)}s
         </div>` : '';

        const lateStr = r.lateRec > 0
            ? `<span style="color:var(--red);margin-left:10px;">⚠ ${Math.round(r.lateRec)} late records dropped</span>`
            : '';

        return `<div style="background:var(--bg2);border:1px solid ${stalled?'rgba(255,77,109,0.4)':'var(--border)'};
      border-radius:6px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;
      ${stalled?'box-shadow:0 0 10px rgba(255,77,109,0.15);':''}">

      <div style="display:flex;align-items:flex-start;gap:10px;">
        <div style="flex:1;min-width:0;">
          <div style="font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);
            white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${_escWm(r.name)}</div>
          <div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:2px;">
            Watermark: <span style="color:var(--accent);">${_escWm(wmStr)}</span>
            ${lateStr}
          </div>
          ${stallWarning}
        </div>
        <div style="text-align:right;flex-shrink:0;">
          <div style="font-size:18px;font-weight:700;color:${lagColor};font-family:var(--mono);line-height:1.1;">${lagSec}s</div>
          <div style="font-size:9px;color:var(--text3);">event-time lag</div>
        </div>
      </div>

      <!-- Lag bar -->
      <div>
        <div style="height:4px;background:var(--bg3);border-radius:2px;overflow:hidden;">
          <div style="height:100%;width:${lagBar.toFixed(1)}%;background:${lagColor};border-radius:2px;transition:width 0.4s;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--text3);margin-top:2px;">
          <span>Idle: ${r.idle.toFixed(0)}%</span>
          <span>Max shown: 60s</span>
        </div>
      </div>

      <!-- Sparkline -->
      <div style="background:var(--bg0);border:1px solid var(--border);border-radius:4px;padding:4px 8px;">
        <div style="font-size:9px;color:var(--text3);margin-bottom:2px;font-family:var(--mono);">
          Watermark lag (last ${(_WM.maxHistory * _WM.POLL_MS / 60000).toFixed(0)} min)
        </div>
        ${sparkSvg}
      </div>
    </div>`;
    }).join('');

    _wmRenderTimeline(results, now);
    _wmSetStatus(`Updated ${new Date().toLocaleTimeString('en-US',{hour12:false})}`, 'ok');
}

// ── Sparkline per vertex ───────────────────────────────────────────
function _wmSparkline(hist, now) {
    if (hist.length < 2) return '<div style="height:28px;font-size:9px;color:var(--text3);display:flex;align-items:center;">Collecting data…</div>';
    const lags = hist.map(h => Math.max(0, now - h.wmMs));
    const maxLag = Math.max(...lags, 1);
    const W = 300, H = 28;
    const pts = lags.map((l,i) => {
        const x = (i / (lags.length-1)) * W;
        const y = H - (l / maxLag) * H;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    const color = lags[lags.length-1] > 30000 ? '#ef4444' : lags[lags.length-1] > 5000 ? '#f59e0b' : '#00d4aa';
    return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px;display:block;">
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linejoin="round"/>
    <line x1="0" y1="${H-1}" x2="${W}" y2="${H-1}" stroke="rgba(255,255,255,0.05)" stroke-width="1"/>
  </svg>`;
}

// ── Cross-vertex watermark timeline ────────────────────────────────
function _wmRenderTimeline(results, now) {
    const canvas = document.getElementById('wm-timeline-canvas');
    if (!canvas) return;
    const W = canvas.offsetWidth || 600;
    const H = 60;
    canvas.width  = W;
    canvas.height = H;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0,0,W,H);

    if (!results.length) return;

    // Draw each vertex as a horizontal bar showing watermark position relative to now
    const rowH = Math.min(18, Math.floor((H - 10) / results.length));
    const maxLag = 60000; // 60s window

    results.forEach((r, i) => {
        const y = 5 + i * (rowH + 3);
        const lagMs = r.wmMs !== null ? Math.max(0, Math.min(maxLag, now - r.wmMs)) : maxLag;
        const barW  = W - 10 - (lagMs / maxLag) * (W - 10);
        const color = lagMs < 5000 ? '#00d4aa' : lagMs < 30000 ? '#f59e0b' : '#ef4444';

        // Background
        ctx.fillStyle = 'rgba(255,255,255,0.04)';
        ctx.fillRect(5, y, W-10, rowH);

        // Bar
        ctx.fillStyle = color;
        ctx.fillRect(5, y, barW, rowH);

        // Label
        ctx.fillStyle = 'rgba(255,255,255,0.6)';
        ctx.font = `${Math.max(8,rowH-4)}px monospace`;
        ctx.fillText(r.name.slice(0,22), 8, y + rowH - 3);
    });

    // Now line
    ctx.strokeStyle = 'rgba(255,255,255,0.3)';
    ctx.setLineDash([3,3]);
    ctx.beginPath();
    ctx.moveTo(W-10, 0);
    ctx.lineTo(W-10, H);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = 'rgba(255,255,255,0.4)';
    ctx.font = '8px monospace';
    ctx.fillText('NOW', W-26, 10);
}

// ── Helpers ────────────────────────────────────────────────────────
function _wmVertexName(nd, vx) {
    const raw = (vx && vx.name) || nd.description || nd.id || '';
    return raw.replace(/<[^>]+>/g,' ').replace(/\[.*?\]/g,'').replace(/\s+/g,' ').trim().slice(0,60);
}
function _wmSetStatus(msg, type) {
    const el = document.getElementById('wm-status');
    if (!el) return;
    el.textContent = msg;
    el.style.color = type === 'err' ? 'var(--red)' : type === 'ok' ? 'var(--green)' : 'var(--text3)';
}
function _escWm(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// ── Modal builder ──────────────────────────────────────────────────
function _wmBuildModal() {
    const modal = document.createElement('div');
    modal.id = 'wm-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
<div class="modal" style="width:min(860px,96vw);max-height:91vh;display:flex;flex-direction:column;">
  <div class="modal-header" style="background:rgba(79,163,224,0.05);border-bottom:1px solid rgba(79,163,224,0.2);">
    <span style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--blue,#4fa3e0)" stroke-width="2">
        <circle cx="12" cy="12" r="10"/>
        <polyline points="12 6 12 12 16 14"/>
      </svg>
      Watermark Health Monitor
      <span style="font-size:9px;background:rgba(79,163,224,0.12);color:var(--blue,#4fa3e0);
        padding:2px 8px;border-radius:10px;">REAL-TIME · 2s REFRESH</span>
    </span>
    <button class="modal-close" onclick="_wmStopPolling();closeModal('wm-modal');">×</button>
  </div>

  <div class="modal-body" style="flex:1;overflow-y:auto;padding:14px 18px;display:flex;flex-direction:column;gap:12px;">

    <!-- Controls -->
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
      <select id="wm-job-sel" class="field-input" style="flex:1;font-size:11px;font-family:var(--mono);">
        <option value="">— Select a running job —</option>
      </select>
      <button id="wm-toggle-btn" onclick="if(_WM.polling){_wmStopPolling();}else{_wmStartPolling();}"
        style="font-size:11px;padding:5px 14px;border-radius:var(--radius);
        background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.35);
        color:var(--accent);cursor:pointer;font-weight:600;">▶ Start</button>
      <button onclick="_wmPopulateJobs()"
        style="font-size:11px;padding:5px 10px;border-radius:var(--radius);
        border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;">⟳</button>
      <span id="wm-status" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
    </div>

    <!-- Stall threshold -->
    <div style="display:flex;align-items:center;gap:10px;font-size:11px;color:var(--text2);">
      <span>Stall alert after:</span>
      <select onchange="_WM.stallThresh=parseInt(this.value)"
        style="background:var(--bg3);border:1px solid var(--border);color:var(--text1);
        font-family:var(--mono);font-size:10px;padding:2px 6px;border-radius:3px;">
        <option value="5000">5s</option>
        <option value="10000" selected>10s</option>
        <option value="30000">30s</option>
        <option value="60000">60s</option>
      </select>
      <span style="color:var(--text3);font-size:10px;">
        Lag &lt; 5s = healthy · 5–30s = elevated · &gt; 30s = critical
      </span>
    </div>

    <!-- Timeline -->
    <div>
      <div style="font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
        color:var(--text3);font-family:var(--mono);margin-bottom:6px;">
        Cross-vertex watermark timeline (← lagging · right edge = now)
      </div>
      <canvas id="wm-timeline-canvas"
        style="width:100%;height:60px;background:var(--bg0);border:1px solid var(--border);
        border-radius:5px;display:block;"></canvas>
    </div>

    <!-- Per-vertex rows -->
    <div>
      <div style="font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
        color:var(--text3);font-family:var(--mono);margin-bottom:8px;">
        Per-vertex watermark details
      </div>
      <div id="wm-rows" style="display:flex;flex-direction:column;gap:8px;">
        <div style="font-size:11px;color:var(--text3);padding:20px;text-align:center;">
          Select a job and click ▶ Start to begin monitoring.
        </div>
      </div>
    </div>

  </div>
  <div style="padding:8px 18px;border-top:1px solid var(--border);background:var(--bg2);
    display:flex;gap:8px;flex-shrink:0;font-size:10px;color:var(--text3);align-items:center;">
    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/>
      <line x1="12" y1="16" x2="12.01" y2="16"/>
    </svg>
    Watermark values shown in event-time (UTC). Lag = processing time − watermark time.
    <button class="btn btn-secondary" style="margin-left:auto;font-size:10px;padding:3px 10px;"
      onclick="_wmStopPolling();closeModal('wm-modal');">Close</button>
  </div>
</div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) { _wmStopPolling(); closeModal('wm-modal'); }});
}