/**
 * pipeline-dependency-graph.js  —  Str:::lab Studio v0.0.23
 * ─────────────────────────────────────────────────────────────────
 * Feature 7: Multi-Pipeline Dependency Graph
 *
 * Shows ALL running jobs on the cluster as a single connected
 * topology — Kafka topics flowing between pipelines, sink tables,
 * and their relationships.
 *
 * For each running job:
 *   1. Fetches /jobs/{jid}/plan to get operator descriptions
 *   2. Extracts Kafka topic names, JDBC tables, ES indices from
 *      operator node descriptions (these appear as string labels)
 *   3. Builds a graph: topic/table → Pipeline → topic/table
 *   4. Detects: circular dependencies, dead-end topics,
 *      shared sources (multiple jobs reading same topic)
 *
 * Renders as an interactive SVG with pan + zoom.
 * ─────────────────────────────────────────────────────────────────
 */

const _PDG = {
    jobs       : [],    // [{ jid, name, state, nodes:[], topics:{src:[],sink:[]} }]
    graph      : null,  // { nodes:[], edges:[] }
    zoom       : 1,
    panX       : 0,
    panY       : 0,
    isPanning  : false,
    panSX      : 0,
    panSY      : 0,
    panOX      : 0,
    panOY      : 0,
    loading    : false,
    issues     : [],    // detected topology issues
};

// ── Entry point ────────────────────────────────────────────────────
function openPipelineDependencyGraph() {
    if (!document.getElementById('pdg-modal')) _pdgBuildModal();
    openModal('pdg-modal');
    _pdgLoad();
}

// ── Load all jobs and build graph ──────────────────────────────────
async function _pdgLoad() {
    if (_PDG.loading) return;
    _PDG.loading = true;
    _pdgSetStatus('Fetching all jobs from cluster…', true);
    _pdgShowPane('loading');

    try {
        const overview = await jmApi('/jobs/overview');
        const allJobs  = (overview && overview.jobs) ? overview.jobs : [];
        const active   = allJobs.filter(j => ['RUNNING','FINISHED','RESTARTING'].includes(j.state));

        if (!active.length) {
            _pdgShowPane('empty');
            _pdgSetStatus('No jobs found on cluster', false);
            _PDG.loading = false;
            return;
        }

        _pdgSetStatus(`Fetching plans for ${active.length} jobs…`, true);

        // Fetch plan for each job in parallel (cap at 12)
        const results = await Promise.allSettled(
            active.slice(0,12).map(j => _pdgFetchJobPlan(j))
        );
        _PDG.jobs = results
            .filter(r => r.status === 'fulfilled' && r.value)
            .map(r => r.value);

        _pdgSetStatus('Building dependency graph…', true);
        _PDG.graph  = _pdgBuildGraph(_PDG.jobs);
        _PDG.issues = _pdgDetectIssues(_PDG.graph, _PDG.jobs);

        _pdgRenderGraph();
        _pdgRenderIssues();
        _pdgRenderJobList();
        _pdgShowPane('graph');
        _pdgSetStatus(
            `${_PDG.jobs.length} pipeline(s) · ${_PDG.graph.nodes.filter(n=>n.type!=='pipeline').length} topics/tables · ${_PDG.issues.length} issue(s)`,
            false
        );

    } catch(e) {
        _pdgSetStatus('Error: ' + e.message, false);
        _pdgShowPane('error');
        const errEl = document.getElementById('pdg-error-msg');
        if (errEl) errEl.textContent = e.message;
        addLog('ERR', 'Pipeline dependency graph error: ' + e.message);
    } finally {
        _PDG.loading = false;
    }
}

// ── Fetch job plan and extract topics/tables ───────────────────────
async function _pdgFetchJobPlan(job) {
    try {
        const plan = await jmApi(`/jobs/${job.jid}/plan`);
        if (!plan || !plan.plan || !plan.plan.nodes) return null;

        const nodes   = plan.plan.nodes;
        const sources = [];
        const sinks   = [];

        nodes.forEach(nd => {
            const desc  = (nd.description || nd.id || '').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim();
            const isSource = (nd.inputs || []).length === 0;
            const isSink   = !nodes.some(o => (o.inputs||[]).some(i => i.id === nd.id));

            // Extract topic/table from operator description
            const topics = _pdgExtractTopics(desc);
            if (isSource) sources.push(...topics.map(t => ({ label: t, kind: _pdgKind(desc, t) })));
            if (isSink)   sinks.push(...topics.map(t => ({ label: t, kind: _pdgKind(desc, t) })));
        });

        return {
            jid    : job.jid,
            name   : job.name || job.jid.slice(0,12),
            state  : job.state,
            nodes,
            sources: _pdgDedupe(sources),
            sinks  : _pdgDedupe(sinks),
        };
    } catch(_) { return null; }
}

// ── Extract topic / table names from operator description ──────────
function _pdgExtractTopics(desc) {
    const topics = [];
    // Kafka topic pattern: "Source: xxx[xxx]" or topic=xxx
    const patterns = [
        /[Tt]opic[:\s='"]+([a-zA-Z0-9._\-]+)/g,
        /Source:\s*([a-zA-Z0-9._\-]+)/g,
        /Sink:\s*([a-zA-Z0-9._\-]+)/g,
        /table[:\s='"]+([a-zA-Z0-9._\-]+)/gi,
        /index[:\s='"]+([a-zA-Z0-9._\-]+)/gi,
        /\[([a-zA-Z0-9._\-]{3,60})\]/g,
    ];
    patterns.forEach(re => {
        let m;
        while ((m = re.exec(desc)) !== null) {
            const v = m[1].trim();
            if (v.length >= 3 && v.length <= 60 && !/^\d+$/.test(v)) topics.push(v);
        }
    });
    // Fallback: use the first meaningful word in the description
    if (!topics.length) {
        const words = desc.replace(/[^a-zA-Z0-9._\-\s]/g,' ').split(/\s+/)
            .filter(w => w.length >= 3 && w.length <= 50 && !/^(source|sink|select|from|where|table|kafka|jdbc)$/i.test(w));
        if (words.length) topics.push(words[0]);
    }
    return [...new Set(topics)];
}

function _pdgKind(desc, topic) {
    const d = desc.toLowerCase();
    if (d.includes('kafka'))         return 'kafka';
    if (d.includes('jdbc') || d.includes('postgres') || d.includes('mysql')) return 'jdbc';
    if (d.includes('elasticsearch') || d.includes('opensearch')) return 'elastic';
    if (d.includes('filesystem') || d.includes('s3') || d.includes('parquet')) return 'filesystem';
    if (d.includes('datagen'))       return 'datagen';
    if (d.includes('print') || d.includes('blackhole')) return 'print';
    return 'kafka'; // default assumption
}

function _pdgDedupe(arr) {
    const seen = new Set();
    return arr.filter(x => { const k=x.label+'|'+x.kind; return !seen.has(k) && seen.add(k); });
}

// ── Build graph ────────────────────────────────────────────────────
function _pdgBuildGraph(jobs) {
    const gNodes = [];
    const gEdges = [];
    let seq = 0;
    const uid = () => 'g' + (++seq);

    // Map from topic label → node id
    const topicMap = {};
    const topicNode = (label, kind) => {
        const key = label + '|' + kind;
        if (topicMap[key]) return topicMap[key];
        const id = uid();
        gNodes.push({ id, label, kind, type: 'topic' });
        topicMap[key] = id;
        return id;
    };

    // Pipeline nodes
    jobs.forEach(j => {
        const pid = uid();
        gNodes.push({ id: pid, label: j.name, jid: j.jid, state: j.state, type: 'pipeline' });

        j.sources.forEach(s => {
            const tid = topicNode(s.label, s.kind);
            gEdges.push({ from: tid, to: pid, kind: s.kind });
        });
        j.sinks.forEach(s => {
            const tid = topicNode(s.label, s.kind);
            gEdges.push({ from: pid, to: tid, kind: s.kind });
        });
    });

    return { nodes: gNodes, edges: gEdges };
}

// ── Issue detection ────────────────────────────────────────────────
function _pdgDetectIssues(graph, jobs) {
    const issues = [];

    // 1. Dead-end topics: topics that are only sinks (nothing reads them)
    const sourceTopics = new Set(graph.edges.filter(e => {
        const from = graph.nodes.find(n => n.id === e.from);
        return from && from.type === 'topic';
    }).map(e => e.from));
    const sinkTopics = new Set(graph.edges.filter(e => {
        const to = graph.nodes.find(n => n.id === e.to);
        return to && to.type === 'topic';
    }).map(e => e.to));
    sinkTopics.forEach(tid => {
        if (!sourceTopics.has(tid)) {
            const nd = graph.nodes.find(n => n.id === tid);
            if (nd) issues.push({ level: 'info', title: `Dead-end: "${nd.label}"`, detail: 'This topic/table is written to but never read by another pipeline on this cluster.' });
        }
    });

    // 2. Shared sources: multiple pipelines reading same topic
    const topicReadCount = {};
    graph.edges.forEach(e => {
        const from = graph.nodes.find(n => n.id === e.from);
        if (from && from.type === 'topic') topicReadCount[from.id] = (topicReadCount[from.id]||0)+1;
    });
    Object.entries(topicReadCount).forEach(([tid, cnt]) => {
        if (cnt > 1) {
            const nd = graph.nodes.find(n => n.id === tid);
            if (nd) issues.push({ level: 'warn', title: `Shared source: "${nd.label}"`, detail: `${cnt} pipelines are reading from this topic simultaneously. Ensure consumer groups are different.` });
        }
    });

    // 3. Circular dependency (simple cycle detection)
    const adj = {};
    graph.nodes.forEach(n => { adj[n.id] = []; });
    graph.edges.forEach(e => { if (adj[e.from]) adj[e.from].push(e.to); });
    const visited = new Set(), recStack = new Set();
    const hasCycle = (id) => {
        if (recStack.has(id)) return true;
        if (visited.has(id)) return false;
        visited.add(id); recStack.add(id);
        const found = (adj[id]||[]).some(hasCycle);
        recStack.delete(id);
        return found;
    };
    graph.nodes.forEach(n => {
        if (!visited.has(n.id) && hasCycle(n.id)) {
            issues.push({ level: 'high', title: 'Circular dependency detected', detail: 'One or more pipelines form a cycle in the topic graph. This can cause unbounded state growth or deadlocks.' });
        }
    });

    return issues;
}

// ── Render SVG graph ───────────────────────────────────────────────
function _pdgRenderGraph() {
    const wrap = document.getElementById('pdg-svg-wrap');
    if (!wrap) return;

    const { nodes, edges } = _PDG.graph;
    if (!nodes.length) { wrap.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-size:12px;">No pipeline topology to display.</div>'; return; }

    // Layout: left = topic sources, centre = pipelines, right = topic sinks
    const topicSrc = nodes.filter(n => n.type==='topic' && edges.some(e=>e.from===n.id));
    const pipelines = nodes.filter(n => n.type==='pipeline');
    const topicSnk  = nodes.filter(n => n.type==='topic' && !edges.some(e=>e.from===n.id));
    const allTopics = nodes.filter(n => n.type==='topic');

    const NW=170, NHP=54, NHT=38, HGAP=90, PAD=20, VGAP=14;
    const pos = {};

    // Topics that appear on both sides go in the middle-left
    const assign = (arr, col) => arr.forEach((n,i) => {
        pos[n.id] = { x: PAD + col*(NW+HGAP), y: PAD + i*(NHT+VGAP) };
    });

    assign(topicSrc, 0);
    assign(pipelines, 1);
    assign(topicSnk, 2);
    // Any topic not positioned yet
    allTopics.filter(n => !pos[n.id]).forEach((n,i) => { pos[n.id] = { x: PAD, y: PAD + (topicSrc.length+i)*(NHT+VGAP) }; });

    const maxX = Math.max(...nodes.map(n => (pos[n.id]||{x:0}).x + NW)) + PAD;
    const maxY = Math.max(...nodes.map(n => (pos[n.id]||{y:0}).y + NHP)) + PAD;

    // Kind colours
    const kindColor = { kafka:'#4fa3e0', jdbc:'#34d399', elastic:'#fb923c', filesystem:'#a78bfa', datagen:'#f472b6', print:'#6b7280' };
    const stateColor = { RUNNING:'#39d353', FINISHED:'#6b7280', RESTARTING:'#f59e0b', FAILED:'#ef4444' };

    let svg = `<svg id="pdg-svg" viewBox="0 0 ${maxX} ${maxY}" xmlns="http://www.w3.org/2000/svg"
    style="width:${maxX}px;height:${maxY}px;min-width:${maxX}px;">
    <defs>
      <marker id="pdg-arr" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto">
        <path d="M0,0 L0,6 L7,3 z" fill="#4b5563"/>
      </marker>
    </defs>`;

    // Edges
    edges.forEach(e => {
        const fp = pos[e.from], tp = pos[e.to];
        if (!fp || !tp) return;
        const fnStr = edges.some(x=>x.from===e.from) ? NW : NW;
        const x1=fp.x+NW, y1=fp.y+NHT/2;
        const x2=tp.x,    y2=tp.y+NHT/2;
        const kc = kindColor[e.kind] || '#4b5563';
        svg += `<path d="M${x1},${y1} C${x1+40},${y1} ${x2-40},${y2} ${x2},${y2}"
      stroke="${kc}" stroke-width="1.8" fill="none" opacity="0.6" marker-end="url(#pdg-arr)"/>`;
    });

    // Nodes
    nodes.forEach(n => {
        const p = pos[n.id];
        if (!p) return;
        const isPipeline = n.type === 'pipeline';
        const h = isPipeline ? NHP : NHT;
        const sc = isPipeline ? (stateColor[n.state]||'#6b7280') : (kindColor[n.kind]||'#4fa3e0');
        const bg = isPipeline ? 'rgba(0,212,170,0.08)' : 'rgba(79,163,224,0.08)';
        const icon = isPipeline ? '⚡' :
            { kafka:'⬡', jdbc:'◈', elastic:'◎', filesystem:'▤', datagen:'⊛', print:'⊘' }[n.kind] || '◦';
        const label = (n.label||'').length>22 ? n.label.slice(0,22)+'…' : (n.label||'');

        svg += `<g>
      <rect x="${p.x}" y="${p.y}" width="${NW}" height="${h}" rx="5"
        fill="${bg}" stroke="${sc}" stroke-width="${isPipeline?2:1.5}"/>
      <text x="${p.x+10}" y="${p.y+15}" font-family="monospace" font-size="11"
        fill="${sc}">${icon}</text>
      <text x="${p.x+26}" y="${p.y+15}" font-family="monospace" font-size="10"
        font-weight="${isPipeline?'700':'400'}" fill="${isPipeline?'#e8f0f8':'#9ca3af'}">${_escPdg(label)}</text>
      ${isPipeline ? `<text x="${p.x+10}" y="${p.y+30}" font-family="monospace" font-size="9"
        fill="${sc}">${n.state}</text>
        <text x="${p.x+NW-8}" y="${p.y+15}" text-anchor="end" font-family="monospace" font-size="8"
          fill="#4b5563">${n.jid?n.jid.slice(0,8):''}</text>` :
            `<text x="${p.x+10}" y="${p.y+29}" font-family="monospace" font-size="9"
          fill="#4b5563">${n.kind||''}</text>`}
    </g>`;
    });

    // Legend
    const lx = PAD, ly = maxY + 10;
    svg += `</svg>`;

    wrap.innerHTML = svg;

    // Wire pan/zoom on the outer container
    _pdgWireInteraction(wrap);
    _pdgApplyTransform();
}

// ── Render issues panel ────────────────────────────────────────────
function _pdgRenderIssues() {
    const el = document.getElementById('pdg-issues');
    if (!el) return;
    if (!_PDG.issues.length) {
        el.innerHTML = '<div style="font-size:11px;color:var(--green);padding:6px 0;">✓ No topology issues detected.</div>';
        return;
    }
    el.innerHTML = _PDG.issues.map(iss => {
        const icon  = { high:'🔴', warn:'🟡', info:'🔵' }[iss.level] || '⚪';
        const color = { high:'var(--red)', warn:'var(--yellow)', info:'var(--accent)' }[iss.level] || 'var(--text2)';
        return `<div style="padding:8px 12px;border-left:3px solid ${color};background:rgba(0,0,0,0.2);
      border-radius:0 4px 4px 0;margin-bottom:6px;">
      <div style="font-size:11px;font-weight:700;color:${color};">${icon} ${_escPdg(iss.title)}</div>
      <div style="font-size:11px;color:var(--text2);margin-top:3px;line-height:1.6;">${_escPdg(iss.detail)}</div>
    </div>`;
    }).join('');
}

// ── Render job list sidebar ────────────────────────────────────────
function _pdgRenderJobList() {
    const el = document.getElementById('pdg-job-list');
    if (!el) return;
    el.innerHTML = _PDG.jobs.map(j => {
        const sc = { RUNNING:'var(--green)', FINISHED:'var(--text3)', RESTARTING:'var(--yellow)', FAILED:'var(--red)' }[j.state] || 'var(--text3)';
        const srcNames = j.sources.map(s=>s.label).join(', ') || '—';
        const snkNames = j.sinks.map(s=>s.label).join(', ')   || '—';
        return `<div style="padding:8px 10px;border-bottom:1px solid var(--border);font-size:10px;">
      <div style="font-weight:700;color:var(--text0);font-family:var(--mono);
        white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${_escPdg(j.name.slice(0,38))}</div>
      <div style="color:${sc};font-size:9px;margin-top:2px;">${j.state}</div>
      <div style="color:var(--text3);margin-top:4px;">↪ Sources: <span style="color:var(--accent);">${_escPdg(srcNames.slice(0,50))}</span></div>
      <div style="color:var(--text3);">↩ Sinks: <span style="color:var(--accent3,#7ee8d0);">${_escPdg(snkNames.slice(0,50))}</span></div>
    </div>`;
    }).join('') || '<div style="font-size:11px;color:var(--text3);padding:10px;">No jobs loaded.</div>';
}

// ── Pan / zoom ─────────────────────────────────────────────────────
function _pdgWireInteraction(wrap) {
    _PDG.zoom = 1; _PDG.panX = 0; _PDG.panY = 0;

    wrap.addEventListener('wheel', e => {
        e.preventDefault();
        const r = wrap.getBoundingClientRect();
        const mx = e.clientX - r.left, my = e.clientY - r.top;
        const prev = _PDG.zoom;
        _PDG.zoom = Math.max(0.2, Math.min(3, _PDG.zoom + (e.deltaY < 0 ? 0.12 : -0.12)));
        _PDG.panX = mx - (mx - _PDG.panX) * (_PDG.zoom / prev);
        _PDG.panY = my - (my - _PDG.panY) * (_PDG.zoom / prev);
        _pdgApplyTransform();
    }, { passive: false });

    wrap.addEventListener('mousedown', e => {
        _PDG.isPanning = true;
        _PDG.panSX = e.clientX; _PDG.panSY = e.clientY;
        _PDG.panOX = _PDG.panX; _PDG.panOY = _PDG.panY;
        wrap.style.cursor = 'grabbing';
        e.preventDefault();
    });
    window.addEventListener('mousemove', e => {
        if (!_PDG.isPanning) return;
        _PDG.panX = _PDG.panOX + (e.clientX - _PDG.panSX);
        _PDG.panY = _PDG.panOY + (e.clientY - _PDG.panSY);
        _pdgApplyTransform();
    });
    window.addEventListener('mouseup', () => {
        _PDG.isPanning = false;
        const wrap = document.getElementById('pdg-svg-wrap');
        if (wrap) wrap.style.cursor = 'default';
    });
    wrap.addEventListener('dblclick', () => { _PDG.zoom=1; _PDG.panX=0; _PDG.panY=0; _pdgApplyTransform(); });
}

function _pdgApplyTransform() {
    const svg = document.getElementById('pdg-svg');
    if (svg) svg.style.transform = `translate(${_PDG.panX}px,${_PDG.panY}px) scale(${_PDG.zoom})`;
    const lbl = document.getElementById('pdg-zoom-label');
    if (lbl) lbl.textContent = Math.round(_PDG.zoom*100)+'%';
}

// ── Helpers ────────────────────────────────────────────────────────
function _pdgSetStatus(msg, spin) {
    const el = document.getElementById('pdg-status');
    if (!el) return;
    el.innerHTML = spin
        ? `<span style="display:inline-block;width:10px;height:10px;border:2px solid var(--border2);
        border-top-color:var(--accent);border-radius:50%;animation:pdg-spin 0.7s linear infinite;
        margin-right:6px;vertical-align:middle;"></span>${_escPdg(msg)}`
        : _escPdg(msg);
}
function _pdgShowPane(pane) {
    ['loading','graph','empty','error'].forEach(p => {
        const el = document.getElementById('pdg-pane-'+p);
        if (el) el.style.display = p === pane ? (p==='graph'?'flex':'block') : 'none';
    });
}
function _escPdg(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// ── Modal builder ──────────────────────────────────────────────────
function _pdgBuildModal() {
    const s = document.createElement('style');
    s.textContent = `@keyframes pdg-spin{to{transform:rotate(360deg);}}
    #pdg-svg-wrap { overflow:hidden; position:relative; cursor:default; }
    #pdg-svg { transform-origin:0 0; will-change:transform; }`;
    document.head.appendChild(s);

    const modal = document.createElement('div');
    modal.id = 'pdg-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
<div class="modal" style="width:min(1100px,97vw);max-height:93vh;display:flex;flex-direction:column;">
  <div class="modal-header" style="background:rgba(99,153,255,0.05);border-bottom:1px solid rgba(99,153,255,0.2);">
    <span style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--blue,#4fa3e0)" stroke-width="2">
        <circle cx="5" cy="12" r="2"/><circle cx="19" cy="5" r="2"/><circle cx="19" cy="19" r="2"/>
        <circle cx="12" cy="12" r="2"/>
        <line x1="7" y1="12" x2="10" y2="12"/><line x1="14" y1="12" x2="17" y2="12"/>
        <line x1="19" y1="7" x2="19" y2="10"/><line x1="19" y1="14" x2="19" y2="17"/>
        <line x1="6" y1="10.5" x2="11" y2="13.5"/><line x1="6" y1="13.5" x2="11" y2="10.5"/>
      </svg>
      Multi-Pipeline Dependency Graph
    </span>
    <button class="modal-close" onclick="closeModal('pdg-modal')">×</button>
  </div>

  <div class="modal-body" style="flex:1;overflow:hidden;display:flex;flex-direction:column;gap:0;padding:0;">

    <!-- Toolbar -->
    <div style="display:flex;align-items:center;gap:8px;padding:10px 16px;
      border-bottom:1px solid var(--border);flex-shrink:0;flex-wrap:wrap;">
      <button onclick="_pdgLoad()" class="btn btn-primary" style="font-size:11px;padding:5px 14px;">⟳ Refresh</button>
      <button onclick="_PDG.zoom-=0.15;_pdgApplyTransform()"
        style="font-size:14px;padding:2px 8px;border-radius:3px;border:1px solid var(--border);
        background:var(--bg3);color:var(--text2);cursor:pointer;line-height:1;">−</button>
      <span id="pdg-zoom-label" style="font-size:10px;color:var(--text3);font-family:var(--mono);min-width:34px;text-align:center;">100%</span>
      <button onclick="_PDG.zoom+=0.15;_pdgApplyTransform()"
        style="font-size:14px;padding:2px 8px;border-radius:3px;border:1px solid var(--border);
        background:var(--bg3);color:var(--text2);cursor:pointer;line-height:1;">+</button>
      <button onclick="_PDG.zoom=1;_PDG.panX=0;_PDG.panY=0;_pdgApplyTransform()"
        style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);
        background:var(--bg3);color:var(--text3);cursor:pointer;">⊙ Reset</button>
      <span style="font-size:10px;color:var(--text3);margin-left:4px;">scroll to zoom · drag to pan · dblclick to reset</span>
      <span id="pdg-status" style="margin-left:auto;font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
    </div>

    <!-- Main area: graph + sidebar -->
    <div style="flex:1;overflow:hidden;display:flex;min-height:0;">

      <!-- Graph canvas -->
      <div style="flex:1;overflow:hidden;display:flex;flex-direction:column;min-width:0;">

        <!-- Loading/empty/error panes -->
        <div id="pdg-pane-loading" style="display:none;flex:1;align-items:center;justify-content:center;flex-direction:column;gap:10px;color:var(--text3);">
          <div style="width:28px;height:28px;border:3px solid var(--border2);border-top-color:var(--accent);
            border-radius:50%;animation:pdg-spin 0.8s linear infinite;"></div>
          <div style="font-size:12px;">Building dependency graph…</div>
        </div>
        <div id="pdg-pane-empty" style="display:none;padding:40px;text-align:center;color:var(--text3);font-size:12px;">
          No jobs found on the cluster. Start some Flink pipelines and refresh.
        </div>
        <div id="pdg-pane-error" style="display:none;padding:20px;">
          <div style="background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.3);
            border-radius:5px;padding:14px;font-size:12px;color:var(--red);" id="pdg-error-msg"></div>
        </div>

        <!-- Graph SVG wrap -->
        <div id="pdg-pane-graph" style="display:none;flex:1;flex-direction:column;min-height:0;">
          <div id="pdg-svg-wrap"
            style="flex:1;overflow:hidden;background:var(--bg0);border-bottom:1px solid var(--border);"></div>

          <!-- Legend -->
          <div style="display:flex;gap:16px;padding:6px 14px;font-size:9px;
            color:var(--text3);font-family:var(--mono);flex-shrink:0;flex-wrap:wrap;">
            <span>⬡ <span style="color:#4fa3e0;">Kafka</span></span>
            <span>◈ <span style="color:#34d399;">JDBC</span></span>
            <span>◎ <span style="color:#fb923c;">Elasticsearch</span></span>
            <span>▤ <span style="color:#a78bfa;">Filesystem/S3</span></span>
            <span>⚡ <span style="color:var(--accent);">Pipeline</span></span>
            <span>→ data flow direction</span>
          </div>

          <!-- Issues -->
          <div style="padding:8px 14px;flex-shrink:0;border-top:1px solid var(--border);">
            <div style="font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
              color:var(--text3);font-family:var(--mono);margin-bottom:6px;">Topology issues</div>
            <div id="pdg-issues" style="display:flex;flex-direction:column;gap:4px;max-height:100px;overflow-y:auto;"></div>
          </div>
        </div>
      </div>

      <!-- Sidebar: job list -->
      <div style="width:240px;flex-shrink:0;border-left:1px solid var(--border);
        overflow-y:auto;background:var(--bg1);">
        <div style="padding:8px 10px;border-bottom:1px solid var(--border);
          font-size:9px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
          color:var(--text3);font-family:var(--mono);">Pipelines</div>
        <div id="pdg-job-list"></div>
      </div>
    </div>

  </div>
  <div style="padding:8px 16px;border-top:1px solid var(--border);background:var(--bg2);
    display:flex;gap:8px;flex-shrink:0;">
    <button class="btn btn-secondary" style="font-size:11px;" onclick="closeModal('pdg-modal')">Close</button>
  </div>
</div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('pdg-modal'); });
}