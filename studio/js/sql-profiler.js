/**
 * sql-profiler.js  —  Str:::lab Studio v0.0.22
 * ─────────────────────────────────────────────────────────────────
 * Feature 8: SQL Profiler — Execution Replay
 *
 * A flight recorder for running Flink jobs. Records time-windowed
 * snapshots of per-operator metrics while a job runs, then lets
 * you scrub back in time to replay any past moment as an animated
 * DAG state — like a DVR for your streaming pipeline.
 *
 * What it records (every N seconds, configurable):
 *   • records/s in + out per vertex
 *   • backpressure % per vertex
 *   • busy % per vertex
 *   • state size (if available)
 *   • checkpoint duration
 *
 * Replay UI:
 *   • Time scrubber → drag to any past snapshot
 *   • Animated DAG showing metric values at that moment
 *   • Metric sparklines showing full recording history
 *   • Export recording as JSON for offline analysis
 * ─────────────────────────────────────────────────────────────────
 */

const _PROF = {
    jobId        : null,
    jobName      : '',
    recording    : false,
    recordTimer  : null,
    intervalSec  : 5,           // snapshot interval
    maxSnapshots : 360,         // 30 min at 5s = 360 snapshots
    snapshots    : [],          // [{ ts, vertices:{id:{bp,busy,rin,rout,state}}, cpDur }]
    plan         : null,        // cached plan nodes
    vertices     : [],          // cached vertex list
    scrubIdx     : 0,           // currently displayed snapshot index
    playing      : false,
    playTimer    : null,
    playSpeed    : 1,           // 1x, 2x, 4x playback
};

// ── Entry point ────────────────────────────────────────────────────
function openSqlProfiler() {
    if (!document.getElementById('prof-modal')) _profBuildModal();
    openModal('prof-modal');
    _profPopulateJobs();
}

// ── Populate jobs ──────────────────────────────────────────────────
async function _profPopulateJobs() {
    const sel = document.getElementById('prof-job-sel');
    if (!sel) return;
    sel.innerHTML = '<option value="">Loading…</option>';
    try {
        const data  = await jmApi('/jobs/overview');
        const jobs  = (data && data.jobs) ? data.jobs : [];
        const active = jobs.filter(j => ['RUNNING','RESTARTING'].includes(j.state));
        if (!active.length) {
            sel.innerHTML = '<option value="">No RUNNING jobs found</option>';
            return;
        }
        sel.innerHTML = '<option value="">— Select a job to profile —</option>' +
            active.map(j => `<option value="${j.jid}">[${j.state}] ${j.name.slice(0,55)}</option>`).join('');
        const jgSel = document.getElementById('jg-job-select');
        if (jgSel && jgSel.value) sel.value = jgSel.value;
    } catch(e) {
        sel.innerHTML = `<option value="">Error: ${e.message}</option>`;
    }
}

// ── Start / stop recording ─────────────────────────────────────────
async function _profStartRecording() {
    const sel = document.getElementById('prof-job-sel');
    const jid = sel ? sel.value : '';
    if (!jid) { toast('Select a job to profile', 'err'); return; }

    // Fetch plan
    try {
        const planData = await jmApi(`/jobs/${jid}/plan`);
        const detail   = await jmApi(`/jobs/${jid}`);
        if (!planData || !planData.plan) throw new Error('No plan data returned');
        _PROF.plan     = planData.plan;
        _PROF.vertices = (detail && detail.vertices) ? detail.vertices : [];
        _PROF.jobName  = (detail && detail.name) || jid.slice(0,12);
    } catch(e) {
        toast('Could not load job plan: ' + e.message, 'err');
        return;
    }

    _PROF.jobId     = jid;
    _PROF.recording = true;
    _PROF.snapshots = [];
    _PROF.scrubIdx  = 0;

    _profUpdateRecBtn(true);
    _profShowPane('recording');
    _profSetStatus('Recording…', true);
    addLog('INFO', `SQL Profiler: recording started for ${_PROF.jobName}`);

    await _profTakeSnapshot();  // immediate first snapshot
    _PROF.recordTimer = setInterval(_profTakeSnapshot, _PROF.intervalSec * 1000);
}

function _profStopRecording() {
    _PROF.recording = false;
    if (_PROF.recordTimer) { clearInterval(_PROF.recordTimer); _PROF.recordTimer = null; }
    _profUpdateRecBtn(false);

    if (_PROF.snapshots.length === 0) {
        _profShowPane('empty');
        _profSetStatus('No snapshots recorded', false);
        return;
    }

    _profSetStatus(`Recording stopped. ${_PROF.snapshots.length} snapshot(s) captured.`, false);
    _profShowPane('replay');
    _profScrubTo(_PROF.snapshots.length - 1);
    _profRenderSparklines();
    addLog('INFO', `SQL Profiler: recording stopped. ${_PROF.snapshots.length} snapshots.`);
}

function _profUpdateRecBtn(recording) {
    const btn = document.getElementById('prof-rec-btn');
    if (!btn) return;
    btn.textContent = recording ? '⏹ Stop Recording' : '⏺ Start Recording';
    btn.style.background = recording ? 'rgba(255,77,109,0.15)' : 'rgba(0,212,170,0.12)';
    btn.style.color       = recording ? 'var(--red)' : 'var(--accent)';
    btn.style.borderColor = recording ? 'rgba(255,77,109,0.4)' : 'rgba(0,212,170,0.35)';
}

// ── Take a snapshot ────────────────────────────────────────────────
async function _profTakeSnapshot() {
    if (!_PROF.recording || !_PROF.jobId) return;
    const jid   = _PROF.jobId;
    const nodes = _PROF.plan ? _PROF.plan.nodes : [];
    const snap  = { ts: Date.now(), vertices: {}, cpDur: null };

    const KEYS = 'backPressuredTimeMsPerSecond,busyTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond';

    for (const nd of nodes.slice(0, 16)) { // cap at 16 vertices
        try {
            const vm = await jmApi(`/jobs/${jid}/vertices/${nd.id}/metrics?get=${encodeURIComponent(KEYS)}`);
            if (!vm || !Array.isArray(vm)) continue;
            const getSum = key => vm
                .filter(m => m.id === key || m.id.endsWith('.'+key))
                .reduce((s,m) => s + (parseFloat(m.value)||0), 0);
            snap.vertices[nd.id] = {
                bp  : Math.min(100, getSum('backPressuredTimeMsPerSecond') / 10),
                busy: Math.min(100, getSum('busyTimeMsPerSecond') / 10),
                rin : getSum('numRecordsInPerSecond'),
                rout: getSum('numRecordsOutPerSecond'),
                bin : getSum('numBytesInPerSecond'),
            };
        } catch(_) {}
    }

    // Checkpoint duration
    try {
        const cp = await jmApi(`/jobs/${jid}/checkpoints`);
        if (cp && cp.latest && cp.latest.completed) snap.cpDur = cp.latest.completed.duration || null;
    } catch(_) {}

    _PROF.snapshots.push(snap);
    if (_PROF.snapshots.length > _PROF.maxSnapshots) _PROF.snapshots.shift();

    // Update live counter and mini live DAG
    _profUpdateLiveCounter();
    if (_PROF.snapshots.length === 1) _profRenderDag(); // initial render
}

function _profUpdateLiveCounter() {
    const el = document.getElementById('prof-snap-count');
    if (el) el.textContent = `${_PROF.snapshots.length} snapshot${_PROF.snapshots.length!==1?'s':''} · ${(_PROF.snapshots.length * _PROF.intervalSec)}s recorded`;
}

// ── Replay controls ────────────────────────────────────────────────
function _profScrubTo(idx) {
    if (!_PROF.snapshots.length) return;
    _PROF.scrubIdx = Math.max(0, Math.min(_PROF.snapshots.length - 1, idx));
    const snap = _PROF.snapshots[_PROF.scrubIdx];

    // Update scrubber
    const scrub = document.getElementById('prof-scrubber');
    if (scrub) {
        scrub.max   = _PROF.snapshots.length - 1;
        scrub.value = _PROF.scrubIdx;
    }

    // Update timestamp
    const tsEl = document.getElementById('prof-ts-label');
    if (tsEl) tsEl.textContent = new Date(snap.ts).toLocaleTimeString('en-US',{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});

    // Update index label
    const idxEl = document.getElementById('prof-idx-label');
    if (idxEl) idxEl.textContent = `${_PROF.scrubIdx + 1} / ${_PROF.snapshots.length}`;

    // Checkpoint info
    const cpEl = document.getElementById('prof-cp-label');
    if (cpEl) cpEl.textContent = snap.cpDur != null ? `Last CP: ${(snap.cpDur/1000).toFixed(1)}s` : '';

    // Re-render DAG with this snapshot's data
    _profRenderDag(snap);
}

function _profPlayPause() {
    _PROF.playing = !_PROF.playing;
    const btn = document.getElementById('prof-play-btn');
    if (!_PROF.playing) {
        if (_PROF.playTimer) { clearInterval(_PROF.playTimer); _PROF.playTimer = null; }
        if (btn) btn.textContent = '▶ Play';
        return;
    }
    if (btn) btn.textContent = '⏸ Pause';
    const frameMs = Math.round(500 / _PROF.playSpeed);
    _PROF.playTimer = setInterval(() => {
        const next = _PROF.scrubIdx + 1;
        if (next >= _PROF.snapshots.length) {
            _PROF.playing = false;
            if (_PROF.playTimer) { clearInterval(_PROF.playTimer); _PROF.playTimer = null; }
            if (btn) btn.textContent = '▶ Play';
            return;
        }
        _profScrubTo(next);
    }, frameMs);
}

function _profSetSpeed(x) {
    _PROF.playSpeed = x;
    document.querySelectorAll('.prof-speed-btn').forEach(b => {
        b.style.background = parseFloat(b.dataset.speed) === x ? 'rgba(0,212,170,0.18)' : 'var(--bg3)';
        b.style.color = parseFloat(b.dataset.speed) === x ? 'var(--accent)' : 'var(--text2)';
    });
    // Restart play timer with new speed if playing
    if (_PROF.playing && _PROF.playTimer) {
        clearInterval(_PROF.playTimer);
        _PROF.playTimer = setInterval(() => {
            const next = _PROF.scrubIdx + 1;
            if (next >= _PROF.snapshots.length) { _profPlayPause(); return; }
            _profScrubTo(next);
        }, Math.round(500 / _PROF.playSpeed));
    }
}

// ── DAG renderer ───────────────────────────────────────────────────
function _profRenderDag(snap) {
    const wrap = document.getElementById('prof-dag-wrap');
    if (!wrap || !_PROF.plan) return;

    const nodes = _PROF.plan.nodes;
    if (!nodes || !nodes.length) { wrap.innerHTML = '<div style="padding:20px;color:var(--text3);font-size:11px;">No plan data.</div>'; return; }

    const metrics = (snap && snap.vertices) ? snap.vertices : {};

    // Layout
    const NW=160, NH=52, HGAP=60, VGAP=12, PAD=16;
    const inDeg={}, ch={};
    nodes.forEach(n=>{inDeg[n.id]=0;ch[n.id]=[];});
    nodes.forEach(n=>(n.inputs||[]).forEach(i=>{inDeg[n.id]++;ch[i.id].push(n.id);}));
    let q=nodes.filter(n=>inDeg[n.id]===0).map(n=>n.id);
    const layers={};const vis=new Set();let col=0;
    while(q.length){const nx=[];q.forEach(id=>{if(vis.has(id))return;vis.add(id);layers[id]=col;(ch[id]||[]).forEach(cid=>{inDeg[cid]--;if(inDeg[cid]===0)nx.push(cid);});});q=nx;col++;}
    nodes.filter(n=>!vis.has(n.id)).forEach(n=>{layers[n.id]=col++;});
    const byL={};nodes.forEach(n=>{const l=layers[n.id]||0;(byL[l]=byL[l]||[]).push(n.id);});
    const pos={};
    Object.entries(byL).forEach(([l,ids])=>{ids.forEach((id,ri)=>{pos[id]={x:PAD+parseInt(l)*(NW+HGAP),y:PAD+ri*(NH+VGAP)};});});

    const maxX=Math.max(...nodes.map(n=>(pos[n.id]||{x:0}).x+NW))+PAD;
    const maxY=Math.max(...nodes.map(n=>(pos[n.id]||{y:0}).y+NH))+PAD;

    let svg=`<svg viewBox="0 0 ${maxX} ${maxY}" style="width:100%;height:auto;max-height:280px;display:block;" xmlns="http://www.w3.org/2000/svg">
    <defs><marker id="prof-arr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
      <path d="M0,0 L0,6 L6,3 z" fill="#374151"/></marker></defs>`;

    // Edges
    nodes.forEach(n=>(n.inputs||[]).forEach(inp=>{
        const fp=pos[inp.id],tp=pos[n.id];
        if(!fp||!tp)return;
        svg+=`<path d="M${fp.x+NW},${fp.y+NH/2} C${fp.x+NW+25},${fp.y+NH/2} ${tp.x-25},${tp.y+NH/2} ${tp.x},${tp.y+NH/2}"
      stroke="#374151" stroke-width="1.6" fill="none" marker-end="url(#prof-arr)"/>`;
    }));

    // Nodes
    nodes.forEach(n=>{
        const p=pos[n.id];if(!p)return;
        const m=metrics[n.id]||{};
        const bp=m.bp||0, busy=m.busy||0, rin=m.rin||0, rout=m.rout||0;
        const isSource=(n.inputs||[]).length===0;
        const isSink=!nodes.some(o=>(o.inputs||[]).some(i=>i.id===n.id));
        const baseColor=isSource?'#1a4a6a':isSink?'#0d2e1e':'#1e1040';
        const bdrColor =isSource?'#4fa3e0':isSink?'#00d4aa':'#818cf8';
        // Backpressure overlay: tint red proportionally
        const bpR=Math.round(bp*2.55), bpG=Math.round(Math.max(0,255-bp*3.5));
        const bpFill=bp>5?`rgba(${bpR},${bpG},40,0.25)`:'transparent';
        const shortName=_profVertexName(n).slice(0,20);

        svg+=`<g>
      <rect x="${p.x}" y="${p.y}" width="${NW}" height="${NH}" rx="5"
        fill="${baseColor}" stroke="${bp>50?'#ef4444':bdrColor}" stroke-width="${bp>50?2:1.5}"/>
      ${bp>5?`<rect x="${p.x}" y="${p.y}" width="${NW}" height="${NH}" rx="5" fill="${bpFill}"/>`:''}
      <text x="${p.x+8}" y="${p.y+14}" font-family="monospace" font-size="9" font-weight="700"
        fill="${isSource?'#4fa3e0':isSink?'#00d4aa':'#a78bfa'}">${_escProf(shortName)}</text>
      <text x="${p.x+8}" y="${p.y+26}" font-family="monospace" font-size="9"
        fill="${bp>50?'#ef4444':bp>20?'#f59e0b':'#6b7280'}">
        BP:<tspan font-weight="700"> ${bp.toFixed(0)}%</tspan>  Busy:${busy.toFixed(0)}%
      </text>
      <text x="${p.x+8}" y="${p.y+39}" font-family="monospace" font-size="8" fill="#4b5563">
        In:${_fmtProfRate(rin)}/s  Out:${_fmtProfRate(rout)}/s
      </text>
      ${rin>0&&rout<rin*0.1?`<text x="${p.x+NW-6}" y="${p.y+14}" text-anchor="end" font-family="monospace" font-size="10" fill="#ef4444">⚠</text>`:''}
    </g>`;
    });

    svg+='</svg>';
    wrap.innerHTML = svg;
}

// ── Sparklines for all vertices ────────────────────────────────────
function _profRenderSparklines() {
    const wrap = document.getElementById('prof-sparklines');
    if (!wrap || !_PROF.plan) return;
    const nodes = _PROF.plan.nodes;
    if (!nodes || !nodes.length) { wrap.innerHTML=''; return; }

    wrap.innerHTML = nodes.slice(0,12).map(n => {
        const bps  = _PROF.snapshots.map(s => (s.vertices[n.id]||{}).bp||0);
        const rins = _PROF.snapshots.map(s => (s.vertices[n.id]||{}).rin||0);
        const name = _profVertexName(n).slice(0,28);
        const maxBp  = Math.max(...bps,  1);
        const maxRin = Math.max(...rins, 1);
        const W=180, H=24;

        const spark = (data, max, color) => {
            if (data.length < 2) return '';
            const pts = data.map((v,i) => `${((i/(data.length-1))*W).toFixed(1)},${(H-(v/max)*H).toFixed(1)}`).join(' ');
            return `<polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.4"/>`;
        };

        return `<div style="background:var(--bg2);border:1px solid var(--border);border-radius:5px;
      padding:8px 10px;min-width:200px;">
      <div style="font-size:9px;font-weight:700;color:var(--text2);font-family:var(--mono);
        margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${_escProf(name)}</div>
      <div style="display:flex;gap:8px;align-items:flex-end;">
        <div>
          <div style="font-size:8px;color:var(--text3);margin-bottom:2px;">Backpressure %</div>
          <svg viewBox="0 0 ${W} ${H}" style="width:${W}px;height:${H}px;background:var(--bg0);border-radius:2px;">
            ${spark(bps, maxBp, '#ef4444')}
          </svg>
        </div>
        <div>
          <div style="font-size:8px;color:var(--text3);margin-bottom:2px;">Records In/s</div>
          <svg viewBox="0 0 ${W} ${H}" style="width:${W}px;height:${H}px;background:var(--bg0);border-radius:2px;">
            ${spark(rins, maxRin, '#00d4aa')}
          </svg>
        </div>
      </div>
      <div style="display:flex;gap:12px;font-size:9px;color:var(--text3);margin-top:3px;font-family:var(--mono);">
        <span>Peak BP: <span style="color:#ef4444;">${Math.max(...bps).toFixed(0)}%</span></span>
        <span>Peak In: <span style="color:#00d4aa;">${_fmtProfRate(Math.max(...rins))}/s</span></span>
      </div>
    </div>`;
    }).join('');
}

// ── Export recording ───────────────────────────────────────────────
function _profExport() {
    if (!_PROF.snapshots.length) { toast('No recording to export', 'err'); return; }
    const data = {
        jobId    : _PROF.jobId,
        jobName  : _PROF.jobName,
        interval : _PROF.intervalSec,
        snapshots: _PROF.snapshots,
        exportedAt: new Date().toISOString(),
    };
    const blob = new Blob([JSON.stringify(data, null, 2)], { type:'application/json' });
    const a    = document.createElement('a');
    a.href     = URL.createObjectURL(blob);
    a.download = `flink-profile-${_PROF.jobName.replace(/[^a-z0-9]/gi,'-').slice(0,30)}-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(a.href);
    toast(`Recording exported — ${_PROF.snapshots.length} snapshots`, 'ok');
}

// ── Helpers ────────────────────────────────────────────────────────
function _profVertexName(nd) {
    const raw = nd.description || nd.id || '';
    return raw.replace(/<[^>]+>/g,' ').replace(/\[.*?\]/g,'').replace(/\+\-\s*/g,'').replace(/\s+/g,' ').trim();
}
function _fmtProfRate(n) {
    if (n > 999999) return (n/1000000).toFixed(1)+'M';
    if (n > 999)    return (n/1000).toFixed(1)+'K';
    return Math.round(n).toString();
}
function _escProf(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function _profSetStatus(msg, spin) {
    const el = document.getElementById('prof-status');
    if (!el) return;
    el.innerHTML = spin
        ? `<span style="display:inline-block;width:10px;height:10px;border:2px solid var(--border2);
        border-top-color:var(--accent);border-radius:50%;animation:prof-spin 0.7s linear infinite;
        margin-right:6px;vertical-align:middle;"></span>${_escProf(msg)}`
        : _escProf(msg);
}
function _profShowPane(pane) {
    ['empty','recording','replay'].forEach(p => {
        const el = document.getElementById('prof-pane-'+p);
        if (el) el.style.display = p === pane ? 'block' : 'none';
    });
}

// ── Modal builder ──────────────────────────────────────────────────
function _profBuildModal() {
    const s = document.createElement('style');
    s.textContent = `
    @keyframes prof-spin { to { transform:rotate(360deg); } }
    #prof-modal .modal { width:min(1000px,97vw); max-height:93vh; display:flex; flex-direction:column; }
    #prof-scrubber {
      -webkit-appearance:none; appearance:none; width:100%; height:5px;
      border-radius:3px; background:var(--bg3); outline:none; cursor:pointer;
    }
    #prof-scrubber::-webkit-slider-thumb {
      -webkit-appearance:none; width:14px; height:14px; border-radius:50%;
      background:var(--accent); cursor:pointer; border:2px solid var(--bg0);
    }
    #prof-scrubber::-moz-range-thumb {
      width:14px; height:14px; border-radius:50%;
      background:var(--accent); cursor:pointer; border:2px solid var(--bg0);
    }
  `;
    document.head.appendChild(s);

    const modal = document.createElement('div');
    modal.id = 'prof-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
<div class="modal">
  <div class="modal-header" style="background:rgba(129,140,248,0.05);border-bottom:1px solid rgba(129,140,248,0.2);">
    <span style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent3,#818cf8)" stroke-width="2">
        <circle cx="12" cy="12" r="10"/>
        <polyline points="12 6 12 12 16 14"/>
        <circle cx="12" cy="12" r="3" fill="var(--accent3,#818cf8)" stroke="none"/>
      </svg>
      SQL Profiler — Execution Replay
      <span style="font-size:9px;background:rgba(129,140,248,0.12);color:var(--accent3,#818cf8);
        padding:2px 8px;border-radius:10px;">FLIGHT RECORDER</span>
    </span>
    <button class="modal-close" onclick="_profStopRecording();if(_PROF.playTimer){clearInterval(_PROF.playTimer);}closeModal('prof-modal');">×</button>
  </div>

  <div class="modal-body" style="flex:1;overflow-y:auto;padding:14px 18px;display:flex;flex-direction:column;gap:12px;">

    <!-- Controls bar -->
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
      <select id="prof-job-sel" class="field-input" style="flex:1;font-size:11px;font-family:var(--mono);">
        <option value="">— Select a job to profile —</option>
      </select>
      <button id="prof-rec-btn"
        onclick="if(_PROF.recording){_profStopRecording();}else{_profStartRecording();}"
        style="font-size:11px;padding:5px 14px;border-radius:var(--radius);font-weight:600;
        background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.35);
        color:var(--accent);cursor:pointer;">⏺ Start Recording</button>
      <select onchange="_PROF.intervalSec=parseInt(this.value)"
        style="font-size:10px;background:var(--bg3);border:1px solid var(--border);
        color:var(--text1);font-family:var(--mono);padding:3px 6px;border-radius:3px;">
        <option value="2">2s interval</option>
        <option value="5" selected>5s interval</option>
        <option value="10">10s interval</option>
        <option value="30">30s interval</option>
      </select>
      <button onclick="_profExport()"
        style="font-size:10px;padding:4px 10px;border-radius:3px;border:1px solid var(--border);
        background:var(--bg3);color:var(--text2);cursor:pointer;">⬇ Export JSON</button>
      <button onclick="_profPopulateJobs()"
        style="font-size:11px;padding:5px 8px;border-radius:var(--radius);border:1px solid var(--border);
        background:var(--bg3);color:var(--text3);cursor:pointer;">⟳</button>
      <span id="prof-status" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
    </div>

    <!-- Empty pane -->
    <div id="prof-pane-empty" style="text-align:center;padding:40px;color:var(--text3);font-size:12px;line-height:1.9;">
      Select a running job and click <strong style="color:var(--accent);">⏺ Start Recording</strong>.<br>
      The profiler will capture operator metrics every 5 seconds.<br>
      Stop recording to replay the timeline.
    </div>

    <!-- Recording pane (live view) -->
    <div id="prof-pane-recording" style="display:none;display:flex;flex-direction:column;gap:10px;">
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="width:10px;height:10px;background:var(--red);border-radius:50%;
          display:inline-block;animation:prof-spin 2s linear infinite;flex-shrink:0;
          box-shadow:0 0 6px var(--red);"></span>
        <span style="font-size:12px;font-weight:700;color:var(--text0);">Recording</span>
        <span id="prof-snap-count" style="font-size:11px;color:var(--text3);font-family:var(--mono);margin-left:4px;"></span>
      </div>
      <div id="prof-dag-wrap" style="background:var(--bg0);border:1px solid var(--border);
        border-radius:5px;padding:10px;overflow-x:auto;min-height:80px;"></div>
      <div style="font-size:10px;color:var(--text3);">Live DAG updates with each snapshot. Stop recording to access the replay scrubber.</div>
    </div>

    <!-- Replay pane -->
    <div id="prof-pane-replay" style="display:none;display:flex;flex-direction:column;gap:12px;">

      <!-- Scrubber -->
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:12px 16px;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
          <button id="prof-play-btn" onclick="_profPlayPause()"
            style="font-size:11px;padding:4px 12px;border-radius:3px;font-weight:600;
            background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.35);
            color:var(--accent);cursor:pointer;">▶ Play</button>
          <!-- Speed buttons -->
          ${[1,2,4].map(x => `<button class="prof-speed-btn" data-speed="${x}" onclick="_profSetSpeed(${x})"
            style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);
            background:${x===1?'rgba(0,212,170,0.18)':'var(--bg3)'};
            color:${x===1?'var(--accent)':'var(--text2)'};cursor:pointer;">${x}×</button>`).join('')}
          <button onclick="_profScrubTo(0)"
            style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);
            background:var(--bg3);color:var(--text3);cursor:pointer;">|◀</button>
          <button onclick="_profScrubTo(_PROF.snapshots.length-1)"
            style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);
            background:var(--bg3);color:var(--text3);cursor:pointer;">▶|</button>
          <span id="prof-ts-label" style="font-family:var(--mono);font-size:12px;color:var(--accent);margin-left:auto;"></span>
          <span id="prof-idx-label" style="font-family:var(--mono);font-size:10px;color:var(--text3);"></span>
          <span id="prof-cp-label"  style="font-family:var(--mono);font-size:10px;color:var(--yellow);"></span>
        </div>
        <input type="range" id="prof-scrubber" min="0" max="100" value="0"
          oninput="_profScrubTo(parseInt(this.value))"
          style="width:100%;">
        <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--text3);
          font-family:var(--mono);margin-top:4px;">
          <span id="prof-t-start">start</span>
          <span style="color:var(--text3);">drag scrubber or press ▶ Play to replay</span>
          <span id="prof-t-end">end</span>
        </div>
      </div>

      <!-- DAG replay -->
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
          color:var(--text3);font-family:var(--mono);margin-bottom:6px;">Pipeline state at selected time</div>
        <div id="prof-dag-wrap" style="background:var(--bg0);border:1px solid var(--border);
          border-radius:5px;padding:10px;overflow-x:auto;min-height:80px;"></div>
        <div style="display:flex;gap:16px;font-size:9px;color:var(--text3);margin-top:5px;font-family:var(--mono);">
          <span style="color:var(--green);">■ 0–20% BP normal</span>
          <span style="color:var(--yellow);">■ 20–50% elevated</span>
          <span style="color:var(--red);">■ 50–100% critical</span>
          <span>⚠ = output &lt; 10% of input rate</span>
        </div>
      </div>

      <!-- Sparklines -->
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
          color:var(--text3);font-family:var(--mono);margin-bottom:6px;">Full recording history per operator</div>
        <div id="prof-sparklines" style="display:flex;flex-wrap:wrap;gap:8px;overflow-x:auto;"></div>
      </div>

    </div>
  </div>

  <div style="padding:8px 18px;border-top:1px solid var(--border);background:var(--bg2);
    display:flex;gap:8px;flex-shrink:0;font-size:10px;color:var(--text3);align-items:center;">
    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/>
      <line x1="12" y1="16" x2="12.01" y2="16"/>
    </svg>
    Recordings are stored in-memory and cleared when the modal is reopened.
    Export as JSON to preserve recordings for offline analysis.
    <button class="btn btn-secondary" style="margin-left:auto;font-size:10px;padding:3px 10px;"
      onclick="_profStopRecording();if(_PROF.playTimer)clearInterval(_PROF.playTimer);closeModal('prof-modal');">Close</button>
  </div>
</div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => {
        if (e.target === modal) {
            _profStopRecording();
            if (_PROF.playTimer) clearInterval(_PROF.playTimer);
            closeModal('prof-modal');
        }
    });
}