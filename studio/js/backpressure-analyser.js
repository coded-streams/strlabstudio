/**
 * ─────────────────────────────────────────────────────────────────
 *
 * When a running Flink job shows backpressure this tool:
 *   1. Fetches the full job graph + vertex metrics
 *   2. Traces the backpressure propagation chain upstream
 *   3. Correlates with: checkpoint duration, GC pause, heap usage,
 *      records-in/out ratio, idle time, busy time per vertex
 *   4. Produces a plain-English root-cause diagnosis with
 *      severity scoring, the bottleneck vertex, and actionable
 *      recommendations
 *   5. Shows a visual heatmap of backpressure across the DAG
 * ─────────────────────────────────────────────────────────────────
 */

const _BPA = {
    jobId       : null,
    jobName     : '',
    analysing   : false,
    liveTimer   : null,
    liveEnabled : false,
    lastResult  : null,
};

// ── Entry point ────────────────────────────────────────────────────
function openBackpressureAnalyser() {
    if (!document.getElementById('bpa-modal')) _bpaBuildModal();
    openModal('bpa-modal');
    _bpaPopulateJobs();
}

// ── Populate job selector from current cluster ─────────────────────
async function _bpaPopulateJobs() {
    const sel = document.getElementById('bpa-job-sel');
    if (!sel) return;
    sel.innerHTML = '<option value="">Loading jobs…</option>';
    try {
        const data = await jmApi('/jobs/overview');
        const jobs  = (data && data.jobs) ? data.jobs : [];
        const running = jobs.filter(j => j.state === 'RUNNING');
        if (!running.length) {
            sel.innerHTML = '<option value="">No RUNNING jobs on cluster</option>';
            return;
        }
        sel.innerHTML = '<option value="">— Select a running job —</option>' +
            running.map(j =>
                `<option value="${j.jid}">[${j.state}] ${j.name.slice(0,55)}</option>`
            ).join('');
        // Pre-select if one already chosen in Job Graph tab
        const jgSel = document.getElementById('jg-job-select');
        if (jgSel && jgSel.value) sel.value = jgSel.value;
    } catch(e) {
        sel.innerHTML = '<option value="">Error loading jobs: ' + (e.message||'') + '</option>';
    }
}

// ── Run analysis ───────────────────────────────────────────────────
async function _bpaRunAnalysis() {
    const sel = document.getElementById('bpa-job-sel');
    const jid = sel ? sel.value : '';
    if (!jid) { toast('Select a running job first', 'err'); return; }

    _BPA.jobId = jid;
    _BPA.analysing = true;
    _bpaSetStatus('Fetching job graph…', true);
    _bpaShowPane('loading');

    try {
        // ── 1. Fetch plan + job detail ───────────────────────────────
        const [plan, detail, tmData] = await Promise.all([
            jmApi(`/jobs/${jid}/plan`),
            jmApi(`/jobs/${jid}`),
            jmApi('/taskmanagers').catch(() => ({ taskmanagers: [] })),
        ]);

        if (!plan || !plan.plan || !plan.plan.nodes) throw new Error('No plan data returned');
        const nodes    = plan.plan.nodes;
        const vertices = (detail && detail.vertices) ? detail.vertices : [];
        _BPA.jobName   = (detail && detail.name) ? detail.name : jid.slice(0,12);

        _bpaSetStatus(`Fetching metrics for ${nodes.length} vertices…`, true);

        // ── 2. Fetch metrics per vertex ──────────────────────────────
        const METRIC_KEYS = [
            'backPressuredTimeMsPerSecond',
            'idleTimeMsPerSecond',
            'busyTimeMsPerSecond',
            'numRecordsInPerSecond',
            'numRecordsOutPerSecond',
            'numBytesInPerSecond',
            'numBytesOutPerSecond',
            'numLateRecordsDropped',
            'currentOutputWatermark',
        ].join(',');

        const vertexMetrics = {};
        for (const nd of nodes) {
            try {
                const vm = await jmApi(`/jobs/${jid}/vertices/${nd.id}/metrics?get=${encodeURIComponent(METRIC_KEYS)}`);
                if (vm && Array.isArray(vm)) {
                    vertexMetrics[nd.id] = {};
                    vm.forEach(m => {
                        const seg = m.id.includes('.') ? m.id.slice(m.id.lastIndexOf('.')+1) : m.id;
                        const cur = vertexMetrics[nd.id][seg];
                        const val = parseFloat(m.value) || 0;
                        vertexMetrics[nd.id][seg] = cur !== undefined ? cur + val : val;
                    });
                }
            } catch(_) {}
        }

        // ── 3. Fetch checkpoint metrics ──────────────────────────────
        let cpData = null;
        try { cpData = await jmApi(`/jobs/${jid}/checkpoints`); } catch(_) {}

        // ── 4. Fetch JVM metrics for TMs ────────────────────────────
        const tmMetrics = {};
        const tms = (tmData && tmData.taskmanagers) ? tmData.taskmanagers : [];
        for (const tm of tms.slice(0,4)) {
            try {
                const gcData = await jmApi(`/taskmanagers/${tm.id}/metrics?get=Status.JVM.GarbageCollector.G1_Old_Generation.Time,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max`);
                if (gcData && Array.isArray(gcData)) {
                    tmMetrics[tm.id] = {};
                    gcData.forEach(m => { tmMetrics[tm.id][m.id] = parseFloat(m.value) || 0; });
                }
            } catch(_) {}
        }

        _bpaSetStatus('Analysing patterns…', true);

        // ── 5. Build analysis result ─────────────────────────────────
        const result = _bpaAnalyse(nodes, vertices, vertexMetrics, cpData, tmMetrics, detail);
        _BPA.lastResult = result;

        _bpaRenderResult(result, nodes, vertices, vertexMetrics);
        _bpaSetStatus('Analysis complete', false);
        _bpaShowPane('result');

    } catch(e) {
        _bpaSetStatus('Analysis failed: ' + e.message, false);
        _bpaShowPane('error');
        document.getElementById('bpa-error-msg').textContent = e.message;
        addLog('ERR', 'Backpressure analysis failed: ' + e.message);
    } finally {
        _BPA.analysing = false;
    }
}

// ── Core analysis engine ───────────────────────────────────────────
function _bpaAnalyse(nodes, vertices, metrics, cpData, tmMetrics, detail) {
    const result = {
        severity       : 'none',  // none | low | medium | high | critical
        severityScore  : 0,       // 0-100
        bottleneck     : null,    // vertex id of the bottleneck
        bottleneckName : '',
        chain          : [],      // backpressure propagation chain [source→sink]
        findings       : [],      // { level, title, detail, recommendation }
        cpHealth       : null,
        gcPressure     : false,
        diagnosis      : '',
        recommendations: [],
    };

    // Build adjacency: inputs map → upstream for each vertex
    const upstream = {};
    nodes.forEach(n => {
        upstream[n.id] = (n.inputs || []).map(i => i.id);
    });

    // ── Score each vertex ────────────────────────────────────────
    const scores = {};
    nodes.forEach(n => {
        const vm  = metrics[n.id] || {};
        const bp  = (vm.backPressuredTimeMsPerSecond || 0) / 10;  // → %
        const idle= (vm.idleTimeMsPerSecond || 0) / 10;
        const busy= (vm.busyTimeMsPerSecond || 0) / 10;
        const rin = vm.numRecordsInPerSecond  || 0;
        const rout= vm.numRecordsOutPerSecond || 0;
        const ratio = rin > 0 ? rout / rin : 1;

        scores[n.id] = {
            bp, idle, busy, rin, rout, ratio,
            name: _bpaVertexName(n, vertices),
            isSource: (n.inputs || []).length === 0,
            isSink  : !nodes.some(o => (o.inputs||[]).some(i => i.id === n.id)),
        };
    });

    // ── Find bottleneck: highest bp% that is NOT a source ────────
    let maxBp = 0, bottleneckId = null;
    nodes.forEach(n => {
        const s = scores[n.id];
        if (!s.isSource && s.bp > maxBp) { maxBp = s.bp; bottleneckId = n.id; }
    });

    if (!bottleneckId) {
        // No backpressure at process nodes — check sinks
        nodes.forEach(n => {
            const s = scores[n.id];
            if (s.isSink && s.bp > maxBp) { maxBp = s.bp; bottleneckId = n.id; }
        });
    }

    result.severityScore = Math.round(maxBp);
    result.severity = maxBp < 5 ? 'none' : maxBp < 25 ? 'low' : maxBp < 60 ? 'medium' : maxBp < 85 ? 'high' : 'critical';
    result.bottleneck     = bottleneckId;
    result.bottleneckName = bottleneckId ? scores[bottleneckId].name : '—';

    if (result.severity === 'none') {
        result.diagnosis = 'No significant backpressure detected. The pipeline is flowing normally.';
        return result;
    }

    // ── Trace propagation chain ──────────────────────────────────
    const chain = [];
    if (bottleneckId) {
        // Walk upstream from bottleneck
        const visited = new Set();
        const walk = (id) => {
            if (visited.has(id)) return;
            visited.add(id);
            chain.push(id);
            (upstream[id] || []).forEach(walk);
        };
        // Also walk downstream from bottleneck
        const walkDown = (id) => {
            if (visited.has(id)) return;
            visited.add(id);
            chain.push(id);
            nodes.forEach(n => {
                if ((n.inputs||[]).some(i => i.id === id)) walkDown(n.id);
            });
        };
        walk(bottleneckId);
        walkDown(bottleneckId);
        result.chain = chain;
    }

    // ── Checkpoint health ────────────────────────────────────────
    if (cpData && cpData.latest && cpData.latest.completed) {
        const cp = cpData.latest.completed;
        const dur = cp.duration || 0;
        const sz  = (cp['state_size'] || 0) / (1024*1024);
        result.cpHealth = { duration: dur, sizeMb: sz.toFixed(1) };
        if (dur > 30000) {
            result.findings.push({
                level: 'warn',
                title: 'Slow checkpoints',
                detail: `Last checkpoint took ${(dur/1000).toFixed(1)}s. Long checkpoints indicate state is too large or the sink is slow.`,
                recommendation: 'Consider increasing state TTL, enabling incremental checkpoints, or adding parallelism to the bottleneck operator.',
            });
        }
    }

    // ── GC pressure ─────────────────────────────────────────────
    const gcTimes = Object.values(tmMetrics)
        .map(tm => Object.entries(tm)
            .filter(([k]) => k.toLowerCase().includes('gc') || k.toLowerCase().includes('garbage'))
            .reduce((s, [,v]) => s + v, 0));
    if (gcTimes.some(t => t > 5000)) {
        result.gcPressure = true;
        result.findings.push({
            level: 'warn',
            title: 'GC pressure on TaskManagers',
            detail: 'GC pause time is elevated. This can cause artificial backpressure due to stop-the-world pauses.',
            recommendation: 'Use G1GC with -XX:MaxGCPauseMillis=100. Consider using RocksDB state backend to move state off-heap.',
        });
    }

    // ── Per-vertex findings ──────────────────────────────────────
    nodes.forEach(n => {
        const s = scores[n.id];
        if (!s) return;

        // Bottleneck vertex deep analysis
        if (n.id === bottleneckId) {
            if (s.isSink) {
                result.findings.push({
                    level: 'high',
                    title: `Bottleneck: sink operator "${s.name}"`,
                    detail: `Sink is ${s.bp.toFixed(0)}% backpressured. Writing speed (${_fmtRate(s.rin)}/s in) cannot keep up with upstream throughput.`,
                    recommendation: `Increase sink parallelism. For Kafka sinks: tune 'sink.buffer-flush.max-rows' and 'sink.buffer-flush.interval'. For JDBC: enable batching.`,
                });
            } else if (s.ratio < 0.3 && s.rin > 100) {
                result.findings.push({
                    level: 'high',
                    title: `Processing bottleneck: "${s.name}"`,
                    detail: `Operator receives ${_fmtRate(s.rin)}/s but only emits ${_fmtRate(s.rout)}/s (${(s.ratio*100).toFixed(0)}% throughput ratio). State lookups or complex logic is slowing this vertex.`,
                    recommendation: `Profile this operator. If stateful: check state TTL and RocksDB compaction. Consider pre-aggregating upstream or splitting into multiple operators.`,
                });
            } else {
                result.findings.push({
                    level: 'high',
                    title: `Bottleneck at "${s.name}"`,
                    detail: `${s.bp.toFixed(0)}% backpressure. Busy ${s.busy.toFixed(0)}% of the time.`,
                    recommendation: `Increase parallelism for this operator. Check if downstream consumers are keeping up.`,
                });
            }
        }

        // Source idleness — upstream starving
        if (s.isSource && s.idle > 80 && result.severity !== 'none') {
            result.findings.push({
                level: 'info',
                title: `Source "${s.name}" is idle (${s.idle.toFixed(0)}%)`,
                detail: `Source is waiting for data most of the time. This is expected if the downstream bottleneck is rate-limiting consumption.`,
                recommendation: `Once the bottleneck is resolved, the source will catch up automatically. Monitor consumer lag in Kafka if applicable.`,
            });
        }

        // Late records
        const lateDropped = (metrics[n.id] || {}).numLateRecordsDropped || 0;
        if (lateDropped > 100) {
            result.findings.push({
                level: 'warn',
                title: `Late records dropped at "${s.name}"`,
                detail: `${Math.round(lateDropped)} late records dropped. Watermark strategy is too aggressive for the actual event-time skew.`,
                recommendation: `Increase the watermark delay (INTERVAL value). Consider using AllowedLateness or a side output for late data.`,
            });
        }
    });

    // ── Build plain-English diagnosis ────────────────────────────
    const bScore = result.bottleneckName;
    const sev    = result.severity.toUpperCase();
    result.diagnosis = `[${sev}] The pipeline "${_BPA.jobName}" has ${result.severityScore}% backpressure. ` +
        `The bottleneck is "${bScore}". ` +
        (result.cpHealth ? `Last checkpoint: ${(result.cpHealth.duration/1000).toFixed(1)}s, state ${result.cpHealth.sizeMb} MB. ` : '') +
        (result.gcPressure ? `GC pressure detected on TaskManagers. ` : '') +
        `${result.findings.length} finding(s) identified.`;

    // ── Top recommendations ──────────────────────────────────────
    result.recommendations = result.findings
        .filter(f => f.recommendation)
        .map(f => f.recommendation);

    return result;
}

// ── Render result ──────────────────────────────────────────────────
function _bpaRenderResult(result, nodes, vertices, metrics) {
    // Severity badge
    const sevEl = document.getElementById('bpa-severity');
    if (sevEl) {
        const colors = { none:'var(--green)', low:'var(--accent)', medium:'var(--yellow)', high:'var(--red)', critical:'#ff0000' };
        sevEl.textContent = result.severity.toUpperCase();
        sevEl.style.color = colors[result.severity] || 'var(--text2)';
    }

    // Score bar
    const barEl = document.getElementById('bpa-score-bar');
    if (barEl) {
        barEl.style.width = result.severityScore + '%';
        barEl.style.background = result.severityScore < 25 ? 'var(--green)' :
            result.severityScore < 60 ? 'var(--yellow)' : 'var(--red)';
    }
    const scoreEl = document.getElementById('bpa-score-val');
    if (scoreEl) scoreEl.textContent = result.severityScore + '%';

    // Diagnosis
    const diagEl = document.getElementById('bpa-diagnosis');
    if (diagEl) diagEl.textContent = result.diagnosis;

    // Bottleneck
    const bnEl = document.getElementById('bpa-bottleneck');
    if (bnEl) {
        if (result.bottleneck) {
            bnEl.innerHTML = `<span style="color:var(--red);font-weight:700;">${_escBpa(result.bottleneckName)}</span>`;
        } else {
            bnEl.textContent = 'None detected';
        }
    }

    // Heatmap
    _bpaRenderHeatmap(nodes, vertices, metrics);

    // Findings
    const findEl = document.getElementById('bpa-findings');
    if (findEl) {
        if (!result.findings.length) {
            findEl.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:10px 0;">No significant issues detected.</div>';
        } else {
            findEl.innerHTML = result.findings.map(f => {
                const icon  = { high:'🔴', warn:'🟡', info:'🔵' }[f.level] || '⚪';
                const color = { high:'var(--red)', warn:'var(--yellow)', info:'var(--accent)' }[f.level] || 'var(--text2)';
                return `<div style="padding:10px 14px;border-left:3px solid ${color};
          background:rgba(0,0,0,0.2);border-radius:0 5px 5px 0;margin-bottom:8px;">
          <div style="font-size:11px;font-weight:700;color:${color};margin-bottom:4px;">${icon} ${_escBpa(f.title)}</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.7;margin-bottom:6px;">${_escBpa(f.detail)}</div>
          <div style="font-size:10px;color:var(--accent);line-height:1.6;">
            <strong style="color:var(--text3);">Recommendation:</strong> ${_escBpa(f.recommendation)}
          </div>
        </div>`;
            }).join('');
        }
    }
}

// ── Heatmap of DAG with backpressure overlaid ──────────────────────
function _bpaRenderHeatmap(nodes, vertices, metrics) {
    const wrap = document.getElementById('bpa-heatmap');
    if (!wrap) return;

    // Simple left-right layout like the job graph
    const NODE_W=170, NODE_H=54, H_GAP=60, V_GAP=14, PAD=20;
    const inDeg={}, children={};
    nodes.forEach(n=>{inDeg[n.id]=0;children[n.id]=[];});
    nodes.forEach(n=>(n.inputs||[]).forEach(i=>{inDeg[n.id]++;children[i.id].push(n.id);}));
    let queue=nodes.filter(n=>inDeg[n.id]===0).map(n=>n.id);
    const layers={}; const vis=new Set(); let col=0;
    while(queue.length){const nxt=[];queue.forEach(id=>{if(vis.has(id))return;vis.add(id);layers[id]=col;(children[id]||[]).forEach(cid=>{inDeg[cid]--;if(inDeg[cid]===0)nxt.push(cid);});});queue=nxt;col++;}
    nodes.filter(n=>!vis.has(n.id)).forEach(n=>{layers[n.id]=col++;});
    const byLayer={}; nodes.forEach(n=>{const l=layers[n.id]||0;(byLayer[l]=byLayer[l]||[]).push(n.id);});
    const pos={};
    Object.entries(byLayer).forEach(([l,ids])=>{
        ids.forEach((id,ri)=>{pos[id]={x:PAD+parseInt(l)*(NODE_W+H_GAP),y:PAD+ri*(NODE_H+V_GAP)};});
    });
    const maxX=Math.max(...nodes.map(n=>pos[n.id].x+NODE_W))+PAD;
    const maxY=Math.max(...nodes.map(n=>pos[n.id].y+NODE_H))+PAD;

    let svg=`<svg width="${maxX}" height="${maxY}" viewBox="0 0 ${maxX} ${maxY}"
    style="width:100%;height:auto;display:block;" xmlns="http://www.w3.org/2000/svg">
    <defs><marker id="bpa-arr" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto">
      <path d="M0,0 L0,6 L7,3 z" fill="#4b5563"/></marker></defs>`;

    // Edges
    nodes.forEach(n=>(n.inputs||[]).forEach(inp=>{
        if(!pos[n.id]||!pos[inp.id])return;
        const x1=pos[inp.id].x+NODE_W,y1=pos[inp.id].y+NODE_H/2;
        const x2=pos[n.id].x,y2=pos[n.id].y+NODE_H/2;
        svg+=`<path d="M${x1},${y1} C${x1+30},${y1} ${x2-30},${y2} ${x2},${y2}"
      stroke="#374151" stroke-width="1.8" fill="none" marker-end="url(#bpa-arr)"/>`;
    }));

    // Nodes
    nodes.forEach(n=>{
        const {x,y}=pos[n.id];
        const vm=metrics[n.id]||{};
        const bp=Math.min(100,(vm.backPressuredTimeMsPerSecond||0)/10);
        const busy=Math.min(100,(vm.busyTimeMsPerSecond||0)/10);
        const name=_bpaVertexName(n,vertices);
        const shortName=name.length>22?name.slice(0,22)+'…':name;
        const rin=vm.numRecordsInPerSecond||0;
        const rout=vm.numRecordsOutPerSecond||0;
        const isBottleneck=n.id===_BPA.lastResult?.bottleneck;
        // Colour: green→yellow→red based on bp%
        const r=Math.round(Math.min(255, bp*2.55));
        const g=Math.round(Math.max(0, 255 - bp*2.55));
        const fill=`rgba(${r},${g},40,0.25)`;
        const stroke= isBottleneck ? '#ff4d6d' : bp>60 ? '#f59e0b' : bp>25 ? '#fcd34d' : '#1f4e3d';
        const strokeW= isBottleneck ? 2.5 : 1.5;
        svg+=`<g>
      <rect x="${x}" y="${y}" width="${NODE_W}" height="${NODE_H}" rx="5"
        fill="${fill}" stroke="${stroke}" stroke-width="${strokeW}"/>
      ${isBottleneck?`<rect x="${x}" y="${y}" width="${NODE_W}" height="3" rx="2" fill="#ff4d6d"/>`:``}
      <text x="${x+10}" y="${y+17}" font-family="monospace" font-size="10"
        font-weight="700" fill="${bp>25?'#fcd34d':'#9ca3af'}">${_escBpa(shortName)}</text>
      <text x="${x+10}" y="${y+31}" font-family="monospace" font-size="9" fill="#6b7280">
        BP: <tspan fill="${bp>60?'#ef4444':bp>25?'#f59e0b':'#39d353'}" font-weight="700">${bp.toFixed(0)}%</tspan>
        &nbsp; Busy: ${busy.toFixed(0)}%
      </text>
      <text x="${x+10}" y="${y+44}" font-family="monospace" font-size="9" fill="#4b5563">
        In: ${_fmtRate(rin)}/s &nbsp; Out: ${_fmtRate(rout)}/s
      </text>
    </g>`;
    });

    svg += '</svg>';
    wrap.innerHTML = svg;
}

// ── Live refresh ────────────────────────────────────────────────────
function _bpaToggleLive() {
    _BPA.liveEnabled = !_BPA.liveEnabled;
    const btn = document.getElementById('bpa-live-btn');
    if (_BPA.liveEnabled) {
        if (btn) { btn.textContent = '⏸ Pause Live'; btn.style.color = 'var(--accent)'; }
        _BPA.liveTimer = setInterval(() => {
            if (!_BPA.analysing) _bpaRunAnalysis();
        }, 8000);
        addLog('INFO', 'Backpressure analyser: live mode enabled (8s refresh)');
    } else {
        if (_BPA.liveTimer) { clearInterval(_BPA.liveTimer); _BPA.liveTimer = null; }
        if (btn) { btn.textContent = '▶ Live'; btn.style.color = 'var(--text2)'; }
    }
}

// ── Helpers ────────────────────────────────────────────────────────
function _bpaVertexName(n, vertices) {
    const v = vertices.find(x => x.id === n.id);
    const raw = (v && v.name) || n.description || n.id || '';
    return raw.replace(/<[^>]+>/g,' ').replace(/\[.*?\]/g,'').replace(/\s+/g,' ').trim().slice(0,50);
}
function _fmtRate(n) {
    if (n > 999999) return (n/1000000).toFixed(1)+'M';
    if (n > 999)    return (n/1000).toFixed(1)+'K';
    return Math.round(n).toString();
}
function _escBpa(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function _bpaSetStatus(msg, spin) {
    const el = document.getElementById('bpa-status');
    if (el) el.innerHTML = spin
        ? `<span style="display:inline-block;width:10px;height:10px;border:2px solid var(--border2);
        border-top-color:var(--accent);border-radius:50%;animation:bpa-spin 0.7s linear infinite;
        margin-right:6px;vertical-align:middle;"></span>${_escBpa(msg)}`
        : msg;
}
function _bpaShowPane(pane) {
    ['loading','result','error'].forEach(p => {
        const el = document.getElementById('bpa-pane-'+p);
        if (el) el.style.display = p === pane ? 'block' : 'none';
    });
}

// ── Modal builder ──────────────────────────────────────────────────
function _bpaBuildModal() {
    const s = document.createElement('style');
    s.textContent = `
    @keyframes bpa-spin { to { transform:rotate(360deg); } }
    #bpa-modal .modal { width:min(980px,96vw); max-height:91vh; display:flex; flex-direction:column; }
    .bpa-section-lbl { font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
      color:var(--text3);font-family:var(--mono);margin-bottom:6px; }
    .bpa-finding { }
  `;
    document.head.appendChild(s);

    const modal = document.createElement('div');
    modal.id = 'bpa-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
<div class="modal">
  <div class="modal-header" style="background:rgba(255,77,109,0.05);border-bottom:1px solid rgba(255,77,109,0.2);">
    <span style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--red,#ff4d6d)" stroke-width="2">
        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
        <line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
      </svg>
      Backpressure Root Cause Analyser
    </span>
    <button class="modal-close" onclick="closeModal('bpa-modal');if(_BPA.liveTimer){clearInterval(_BPA.liveTimer);_BPA.liveTimer=null;}">×</button>
  </div>
  <div class="modal-body" style="flex:1;overflow-y:auto;padding:16px 20px;display:flex;flex-direction:column;gap:14px;">

    <!-- Controls -->
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
      <select id="bpa-job-sel" class="field-input" style="flex:1;font-size:11px;font-family:var(--mono);">
        <option value="">— Select a running job —</option>
      </select>
      <button onclick="_bpaRunAnalysis()" class="btn btn-primary" style="font-size:11px;padding:5px 14px;">
        ◎ Analyse
      </button>
      <button id="bpa-live-btn" onclick="_bpaToggleLive()"
        style="font-size:11px;padding:5px 12px;border-radius:var(--radius);border:1px solid var(--border2);
        background:var(--bg3);color:var(--text2);cursor:pointer;">▶ Live</button>
      <button onclick="_bpaPopulateJobs()"
        style="font-size:11px;padding:5px 10px;border-radius:var(--radius);border:1px solid var(--border);
        background:var(--bg3);color:var(--text3);cursor:pointer;">⟳</button>
      <span id="bpa-status" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
    </div>

    <!-- Loading pane -->
    <div id="bpa-pane-loading" style="display:none;text-align:center;padding:40px;color:var(--text3);">
      <div style="display:inline-block;width:28px;height:28px;border:3px solid var(--border2);
        border-top-color:var(--accent);border-radius:50%;animation:bpa-spin 0.8s linear infinite;"></div>
      <div style="margin-top:14px;font-size:12px;">Fetching metrics from JobManager…</div>
    </div>

    <!-- Error pane -->
    <div id="bpa-pane-error" style="display:none;">
      <div style="background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.3);
        border-radius:5px;padding:14px;font-size:12px;color:var(--red);" id="bpa-error-msg"></div>
    </div>

    <!-- Result pane -->
    <div id="bpa-pane-result" style="display:none;display:flex;flex-direction:column;gap:14px;">

      <!-- Severity row -->
      <div style="display:flex;align-items:center;gap:14px;background:var(--bg2);
        border:1px solid var(--border);border-radius:6px;padding:12px 16px;">
        <div>
          <div class="bpa-section-lbl">Severity</div>
          <div id="bpa-severity" style="font-size:22px;font-weight:700;font-family:var(--mono);">—</div>
        </div>
        <div style="flex:1;">
          <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text3);margin-bottom:4px;">
            <span>Backpressure</span><span id="bpa-score-val">0%</span>
          </div>
          <div style="height:6px;background:var(--bg3);border-radius:3px;overflow:hidden;">
            <div id="bpa-score-bar" style="height:100%;width:0%;border-radius:3px;transition:width 0.6s;"></div>
          </div>
        </div>
        <div style="text-align:right;">
          <div class="bpa-section-lbl">Bottleneck Operator</div>
          <div id="bpa-bottleneck" style="font-size:12px;font-family:var(--mono);color:var(--text0);">—</div>
        </div>
      </div>

      <!-- Diagnosis -->
      <div>
        <div class="bpa-section-lbl">Diagnosis</div>
        <div id="bpa-diagnosis" style="font-size:12px;color:var(--text1);line-height:1.8;
          background:var(--bg2);border:1px solid var(--border);border-radius:5px;padding:12px 14px;
          font-family:var(--mono);"></div>
      </div>

      <!-- Heatmap -->
      <div>
        <div class="bpa-section-lbl">Pipeline Backpressure Heatmap</div>
        <div id="bpa-heatmap" style="background:var(--bg0);border:1px solid var(--border);
          border-radius:5px;padding:12px;overflow-x:auto;min-height:80px;"></div>
        <div style="display:flex;gap:14px;font-size:9px;color:var(--text3);margin-top:6px;font-family:var(--mono);">
          <span style="color:var(--green);">■ 0–24% normal</span>
          <span style="color:var(--yellow);">■ 25–59% elevated</span>
          <span style="color:var(--red);">■ 60–100% critical</span>
          <span style="color:#ff4d6d;">■ bottleneck</span>
        </div>
      </div>

      <!-- Findings -->
      <div>
        <div class="bpa-section-lbl">Findings</div>
        <div id="bpa-findings" style="display:flex;flex-direction:column;gap:6px;"></div>
      </div>

    </div>
  </div>
  <div style="padding:8px 20px;border-top:1px solid var(--border);background:var(--bg2);
    display:flex;gap:8px;flex-shrink:0;">
    <button class="btn btn-secondary" style="font-size:11px;" onclick="closeModal('bpa-modal')">Close</button>
  </div>
</div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('bpa-modal'); });
}