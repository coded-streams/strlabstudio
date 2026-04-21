/**
 * pipeline-dependency-graph.js  —  Str:::lab Studio v0.0.22
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
        gNodes.push({ id: pid, label: j.name, jid: j.jid, state: j.state, type: 'pipeline', sources: j.sources||[], sinks: j.sinks||[], operators: j.nodes||[] });

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
        svg += `<path data-from="${e.from}" data-to="${e.to}"
      d="M${x1},${y1} C${x1+40},${y1} ${x2-40},${y2} ${x2},${y2}"
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

        // Store node data for the click handler
        const nodeData = JSON.stringify({ id: n.id, type: n.type, label: n.label,
            state: n.state||'', jid: n.jid||'', kind: n.kind||'',
            sources: (n.sources||[]).map(s=>s.label), sinks: (n.sinks||[]).map(s=>s.label),
            operators: n.operators||[] }).replace(/"/g,"'");
        svg += `<g class="pdg-node-g" data-node="${_escPdg(nodeData)}" style="cursor:pointer;">
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

    // Store node positions and dimensions for the animation system
    _PDG._pos = pos;
    _PDG._NW  = NW;
    _PDG._NHT = NHT;

    // Wire node dragging — mousedown on a node group starts drag
    const svgEl = document.getElementById('pdg-svg');
    if (svgEl) {
        svgEl.querySelectorAll('.pdg-node-g').forEach(g => {
            let dragging = false, dragId = null, dragSX = 0, dragSY = 0, dragOX = 0, dragOY = 0;
            let clickPrevented = false;

            g.addEventListener('mousedown', ev => {
                if (ev.button !== 0) return;
                let nodeId = null;
                try { nodeId = JSON.parse(g.dataset.node.replace(/'/g, '"')).id; } catch(_) {}
                if (!nodeId) return;
                dragging = true; dragId = nodeId; clickPrevented = false;
                const svgRect = svgEl.getBoundingClientRect();
                dragSX = (ev.clientX - svgRect.left - _PDG.panX) / _PDG.zoom;
                dragSY = (ev.clientY - svgRect.top  - _PDG.panY) / _PDG.zoom;
                dragOX = (_PDG._pos[nodeId]||{x:0}).x;
                dragOY = (_PDG._pos[nodeId]||{y:0}).y;
                ev.stopPropagation();
                ev.preventDefault();
            });

            window.addEventListener('mousemove', ev => {
                if (!dragging || dragId !== ((() => { try { return JSON.parse(g.dataset.node.replace(/'/g,'"')).id; } catch(_){return null;} })()) ) return;
                const svgRect = svgEl.getBoundingClientRect();
                const nx = (ev.clientX - svgRect.left - _PDG.panX) / _PDG.zoom;
                const ny = (ev.clientY - svgRect.top  - _PDG.panY) / _PDG.zoom;
                const dx = nx - dragSX, dy = ny - dragSY;
                if (Math.abs(dx) > 3 || Math.abs(dy) > 3) clickPrevented = true;
                const newX = Math.max(0, dragOX + dx);
                const newY = Math.max(0, dragOY + dy);
                _PDG._pos[dragId] = { x: newX, y: newY };

                // Move the node group via transform
                g.setAttribute('transform', `translate(${dx},${dy})`);

                // Update all connected edges live
                const allEdges = (_PDG.graph||{}).edges || [];
                svgEl.querySelectorAll('path[data-from], path[data-to]').forEach(p => {
                    const from = p.getAttribute('data-from');
                    const to   = p.getAttribute('data-to');
                    if (from !== dragId && to !== dragId) return;
                    const fp = _PDG._pos[from] || {}; const tp = _PDG._pos[to] || {};
                    const x1 = (fp.x||0) + NW, y1 = (fp.y||0) + NHT/2;
                    const x2 = (tp.x||0),      y2 = (tp.y||0) + NHT/2;
                    p.setAttribute('d', `M${x1},${y1} C${x1+40},${y1} ${x2-40},${y2} ${x2},${y2}`);
                });
            });

            window.addEventListener('mouseup', () => {
                if (!dragging) return;
                dragging = false;
                // Commit the final position by removing transform and baking into pos
                const finalPos = _PDG._pos[dragId];
                if (finalPos) {
                    // Clear the CSS transform — position is now baked into _PDG._pos
                    g.removeAttribute('transform');
                    // Update all rect/text elements inside this group to the new absolute position
                    g.querySelectorAll('rect').forEach(r => {
                        r.setAttribute('x', finalPos.x);
                        r.setAttribute('y', finalPos.y);
                    });
                    g.querySelectorAll('text').forEach((t, i) => {
                        const baseX = finalPos.x;
                        const baseY = finalPos.y;
                        // Re-read original offsets from current cx/y minus old position
                        // Simpler: just re-render the whole graph
                    });
                    // Re-render cleanly with new positions
                    const { nodes: rn, edges: re } = _PDG.graph || { nodes:[], edges:[] };
                    _pdgRenderGraph(rn, re, _PDG._pos);
                }
                dragId = null;
            });
        });
    }

    // Wire pan/zoom on the outer container
    _pdgWireInteraction(wrap);
    _pdgApplyTransform();

    // Start particle animation for RUNNING pipelines
    _pdgStartAnimation();
}

// ── Running pipeline animation ─────────────────────────────────────
let _pdgAnimTimer = null;
const _pdgParticles = [];

function _pdgStartAnimation() {
    if (_pdgAnimTimer) { cancelAnimationFrame(_pdgAnimTimer); _pdgAnimTimer = null; }
    _pdgParticles.length = 0;

    const { nodes, edges } = _PDG.graph || { nodes: [], edges: [] };
    const pos = _PDG._pos || {};
    const NW  = _PDG._NW  || 170;
    const NHT = _PDG._NHT || 38;

    const runningIds = new Set(
        nodes.filter(n => n.type === 'pipeline' && n.state === 'RUNNING').map(n => n.id)
    );
    if (!runningIds.size) return;

    edges.forEach(e => {
        if (!runningIds.has(e.from) && !runningIds.has(e.to)) return;
        if (!pos[e.from] || !pos[e.to]) return;
        for (let i = 0; i < 2; i++) {
            _pdgParticles.push({
                edgeFrom : e.from,
                edgeTo   : e.to,
                t        : Math.random(),
                spd      : 0.007 + Math.random() * 0.006,
            });
        }
    });
    if (!_pdgParticles.length) return;

    const frame = () => {
        const svgEl = document.getElementById('pdg-svg');
        if (!svgEl) { _pdgAnimTimer = null; return; }

        let pg = svgEl.querySelector('#pdg-anim-g');
        if (!pg) {
            pg = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            pg.id = 'pdg-anim-g';
            svgEl.appendChild(pg);
        }

        let html = '';
        _pdgParticles.forEach(p => {
            p.t += p.spd;
            if (p.t > 1) p.t -= 1;

            const fp = _PDG._pos[p.edgeFrom];
            const tp = _PDG._pos[p.edgeTo];
            if (!fp || !tp) return;

            const x1 = fp.x + NW,  y1 = fp.y + NHT / 2;
            const x2 = tp.x,       y2 = tp.y + NHT / 2;
            const cx1 = x1 + 40,   cy1 = y1;
            const cx2 = x2 - 40,   cy2 = y2;

            const t = p.t, mt = 1 - t;
            const px = mt*mt*mt*x1 + 3*mt*mt*t*cx1 + 3*mt*t*t*cx2 + t*t*t*x2;
            const py = mt*mt*mt*y1 + 3*mt*mt*t*cy1 + 3*mt*t*t*cy2 + t*t*t*y2;
            const alpha = (Math.sin(p.t * Math.PI) * 0.85).toFixed(2);
            html += `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="3.5" fill="#00d4aa" opacity="${alpha}"/>`;
        });
        pg.innerHTML = html;
        _pdgAnimTimer = requestAnimationFrame(frame);
    };
    _pdgAnimTimer = requestAnimationFrame(frame);
}

function _pdgStopAnimation() {
    if (_pdgAnimTimer) { cancelAnimationFrame(_pdgAnimTimer); _pdgAnimTimer = null; }
    _pdgParticles.length = 0;
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

    let _activeJobId = null;

    el.innerHTML = _PDG.jobs.map(j => {
        const sc = { RUNNING:'var(--green)', FINISHED:'var(--text3)', RESTARTING:'var(--yellow)', FAILED:'var(--red)' }[j.state] || 'var(--text3)';
        const srcNames = j.sources.map(s=>s.label).join(', ') || '—';
        const snkNames = j.sinks.map(s=>s.label).join(', ')   || '—';
        return `<div class="pdg-job-item" data-jid="${_escPdg(j.jid)}"
      style="padding:8px 10px;border-bottom:1px solid var(--border);font-size:10px;
      cursor:pointer;transition:background 0.15s;"
      onmouseenter="this.style.background='rgba(0,212,170,0.06)'"
      onmouseleave="this.style.background=this.classList.contains('pdg-job-active')?'rgba(0,212,170,0.08)':''"
      onclick="_pdgOnJobClick(this,'${_escPdg(j.jid)}')">
      <div style="display:flex;align-items:center;gap:6px;">
        <span style="font-size:9px;color:${sc};">●</span>
        <div style="font-weight:700;color:var(--text0);font-family:var(--mono);
          white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;">${_escPdg(j.name.slice(0,34))}</div>
      </div>
      <div style="color:${sc};font-size:9px;margin-top:2px;padding-left:15px;">${j.state}</div>
      <div style="color:var(--text3);margin-top:4px;padding-left:15px;">
        ↪ <span style="color:var(--accent);">${_escPdg(srcNames.slice(0,40))}</span>
      </div>
      <div style="color:var(--text3);padding-left:15px;">
        ↩ <span style="color:var(--accent3,#7ee8d0);">${_escPdg(snkNames.slice(0,40))}</span>
      </div>
    </div>`;
    }).join('') || '<div style="font-size:11px;color:var(--text3);padding:10px;">No jobs loaded.</div>';
}

function _pdgOnJobClick(el, jobId) {
    const container = document.getElementById('pdg-job-list');
    if (!container) return;

    const alreadyActive = el.classList.contains('pdg-job-active');

    // Deactivate all
    container.querySelectorAll('.pdg-job-item').forEach(item => {
        item.classList.remove('pdg-job-active');
        item.style.background = '';
        item.style.borderLeft = '';
    });

    if (alreadyActive) {
        // Second click: deselect and clear highlights
        _pdgHighlightPipeline(null);
        return;
    }

    // Activate clicked item
    el.classList.add('pdg-job-active');
    el.style.background = 'rgba(0,212,170,0.08)';
    el.style.borderLeft = '3px solid var(--accent,#00d4aa)';

    // Find the graph node id that corresponds to this job
    const gNodes = (_PDG.graph || {}).nodes || [];
    const gNode  = gNodes.find(n => n.jid === jobId || n.id === jobId);
    const highlightId = gNode ? gNode.id : jobId;

    // Highlight on the graph
    _pdgHighlightPipeline(highlightId);

    // Scroll that node into view in the SVG
    const pos = (_PDG._pos || {})[highlightId];
    if (pos) {
        const wrap = document.getElementById('pdg-svg-wrap');
        if (wrap) {
            const wRect = wrap.getBoundingClientRect();
            // Center the node in the view
            _PDG.panX = wRect.width  / 2 - (pos.x + (_PDG._NW||120)/2) * _PDG.zoom;
            _PDG.panY = wRect.height / 2 - (pos.y + 20) * _PDG.zoom;
            _pdgApplyTransform();
        }
    }
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
    wrap.addEventListener('dblclick', e => {
        // Check if click landed on a node group
        const g = e.target.closest('.pdg-node-g');
        if (g && g.dataset.node) {
            try {
                // Parse the single-quoted JSON back to double-quoted
                const data = JSON.parse(g.dataset.node.replace(/'/g, '"'));
                _pdgShowNodeDetail(data);
            } catch(_) {}
        } else {
            // Double-click on background resets pan/zoom
            _PDG.zoom=1; _PDG.panX=0; _PDG.panY=0; _pdgApplyTransform();
        }
    });
}

// ── Node detail modal ─────────────────────────────────────────────
function _pdgShowNodeDetail(node) {
    // Remove any existing detail panel
    const existing = document.getElementById('pdg-node-detail');
    if (existing) existing.remove();

    const isPipeline = node.type === 'pipeline';
    const stateColor = { RUNNING:'var(--green)', FINISHED:'var(--text3)', RESTARTING:'var(--yellow)', FAILED:'var(--red)' };
    const kindColor  = { kafka:'var(--blue)', jdbc:'var(--green)', elastic:'var(--orange)', filesystem:'var(--purple)', datagen:'var(--pink,#f472b6)', print:'var(--text3)' };
    const sc = isPipeline ? (stateColor[node.state] || 'var(--text2)') : (kindColor[node.kind] || 'var(--blue)');

    const panel = document.createElement('div');
    panel.id = 'pdg-node-detail';
    panel.style.cssText = `
    position:fixed; z-index:9999;
    top:50%; left:50%; transform:translate(-50%,-50%);
    width:min(520px,92vw);
    background:var(--bg1,#0d1117); border:1px solid ${sc};
    border-radius:10px; box-shadow:0 20px 60px rgba(0,0,0,0.6);
    font-family:var(--mono,monospace); overflow:hidden;
    animation:pdg-node-fade-in 0.15s ease;
  `;

    const icon = isPipeline ? '⚡' :
        { kafka:'⬡', jdbc:'◈', elastic:'◎', filesystem:'▤', datagen:'⊛', print:'⊘' }[node.kind] || '◦';

    const sourcesHtml = node.sources && node.sources.length
        ? node.sources.map(s => `<div style="padding:3px 0;color:var(--blue,#4fa3e0);font-size:11px;">↪ ${_escPdg(s)}</div>`).join('')
        : '<div style="color:var(--text3);font-size:11px;">—</div>';

    const sinksHtml = node.sinks && node.sinks.length
        ? node.sinks.map(s => `<div style="padding:3px 0;color:var(--accent,#00d4aa);font-size:11px;">↩ ${_escPdg(s)}</div>`).join('')
        : '<div style="color:var(--text3);font-size:11px;">—</div>';

    const opsHtml = node.operators && node.operators.length
        ? `<div style="margin-top:12px;">
        <div style="font-size:9px;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:6px;">Operators (${node.operators.length})</div>
        <div style="display:flex;flex-direction:column;gap:3px;max-height:160px;overflow-y:auto;">
          ${node.operators.slice(0,20).map(op => `
            <div style="display:flex;justify-content:space-between;padding:4px 8px;
              background:var(--bg2,#131920);border-radius:3px;font-size:10px;">
              <span style="color:var(--text1);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:70%;">${_escPdg((op.description||op.id||'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,60))}</span>
              <span style="color:var(--text3);flex-shrink:0;margin-left:8px;">p${op.parallelism||'?'}</span>
            </div>`).join('')}
        </div>
      </div>`
        : '';

    panel.innerHTML = `
    <style>@keyframes pdg-node-fade-in{from{opacity:0;transform:translate(-50%,-47%)}to{opacity:1;transform:translate(-50%,-50%)}}</style>
    <!-- Header -->
    <div style="display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,0.06);background:rgba(0,0,0,0.25);">
      <span style="font-size:18px;line-height:1;">${icon}</span>
      <div style="flex:1;min-width:0;">
        <div style="font-size:13px;font-weight:700;color:var(--text0);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${_escPdg(node.label||'')}</div>
        ${isPipeline
        ? `<div style="font-size:10px;color:${sc};margin-top:2px;">${_escPdg(node.state)} ${node.jid ? '· <span style="color:var(--text3);">'+node.jid.slice(0,8)+'…</span>' : ''}</div>`
        : `<div style="font-size:10px;color:${sc};margin-top:2px;text-transform:uppercase;letter-spacing:0.5px;">${node.kind||'topic/table'}</div>`
    }
      </div>
      <button onclick="document.getElementById('pdg-node-detail').remove()"
        style="background:none;border:none;color:var(--text3);font-size:18px;cursor:pointer;
        padding:0 4px;line-height:1;flex-shrink:0;">×</button>
    </div>

    <!-- Body -->
    <div style="padding:14px 16px;">
      ${isPipeline ? `
        <!-- Pipeline detail -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;">
          <div style="background:var(--bg2,#131920);border-radius:5px;padding:10px;">
            <div style="font-size:9px;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:6px;">Sources</div>
            ${sourcesHtml}
          </div>
          <div style="background:var(--bg2,#131920);border-radius:5px;padding:10px;">
            <div style="font-size:9px;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:6px;">Sinks</div>
            ${sinksHtml}
          </div>
        </div>
        ${opsHtml}
        <div style="margin-top:12px;display:flex;gap:8px;">
          <a href="#" onclick="event.preventDefault();document.getElementById('pdg-node-detail').remove();
            const sel=document.getElementById('jg-job-select');if(sel){sel.value='${_escPdg(node.jid||'')}';const ev=new Event('change');sel.dispatchEvent(ev);}"
            style="font-size:11px;padding:5px 14px;border-radius:4px;font-weight:600;
            background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);
            color:var(--accent);text-decoration:none;cursor:pointer;">
            Open in Job Graph →
          </a>
        </div>
      ` : `
        <!-- Topic/Table detail -->
        <div style="margin-bottom:10px;">
          <div style="font-size:9px;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:6px;">Written by</div>
          ${sourcesHtml}
        </div>
        <div>
          <div style="font-size:9px;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:6px;">Read by</div>
          ${sinksHtml}
        </div>
      `}

      <!-- Connected edges info -->
      <div style="margin-top:12px;padding-top:12px;border-top:1px solid rgba(255,255,255,0.05);
        font-size:9px;color:var(--text3);text-align:right;">
        Double-click background to reset view · Click Open in Job Graph to inspect
      </div>
    </div>`;

    document.body.appendChild(panel);

    // Close on outside click
    setTimeout(() => {
        const handler = e => {
            if (!panel.contains(e.target)) { panel.remove(); document.removeEventListener('click', handler); }
        };
        document.addEventListener('click', handler);
    }, 100);
}

// ── Maximize / restore the PDG modal ──────────────────────────────
function _pdgToggleMaximize() {
    const modalEl = document.querySelector('#pdg-modal .modal');
    const btn = document.getElementById('pdg-maximize-btn');
    if (!modalEl) return;
    const isMax = modalEl.getAttribute('data-pdg-max') === '1';
    if (isMax) {
        modalEl.removeAttribute('data-pdg-max');
        modalEl.style.width = '';
        modalEl.style.maxHeight = '';
        modalEl.style.height = '';
        modalEl.style.borderRadius = '';
        if (btn) btn.textContent = '⊞';
    } else {
        modalEl.setAttribute('data-pdg-max', '1');
        modalEl.style.width = '100vw';
        modalEl.style.maxHeight = '100vh';
        modalEl.style.height = '100vh';
        modalEl.style.borderRadius = '0';
        if (btn) btn.textContent = '⊟';
    }
    // Re-render graph after resize
    setTimeout(() => {
        if (_PDG.graph && _PDG.graph.nodes.length) _pdgRenderGraph(_PDG.graph.nodes, _PDG.graph.edges, _PDG._pos || {});
    }, 80);
}

// ── Highlight a pipeline and its connected nodes ────────────────────
function _pdgHighlightPipeline(jobId) {
    const svgEl = document.getElementById('pdg-svg');
    if (!svgEl) return;

    // Remove existing highlights
    svgEl.querySelectorAll('.pdg-node-g').forEach(g => {
        const rect = g.querySelector('rect');
        if (rect) {
            rect.style.filter = '';
            rect.style.opacity = '1';
        }
    });
    svgEl.querySelectorAll('path[data-from]').forEach(p => {
        p.style.stroke = '';
        p.style.strokeWidth = '';
        p.style.opacity = '0.5';
    });

    if (!jobId) return; // just clear

    // Find the node whose id matches
    const nodes = _PDG.graph ? _PDG.graph.nodes : [];
    const edges = _PDG.graph ? _PDG.graph.edges : [];

    // Collect connected node ids (the pipeline itself + all its source/sink topics)
    const relatedIds = new Set([jobId]);
    edges.forEach(e => {
        if (e.from === jobId) relatedIds.add(e.to);
        if (e.to   === jobId) relatedIds.add(e.from);
    });

    // Dim all, then highlight related
    svgEl.querySelectorAll('.pdg-node-g').forEach(g => {
        let nodeId = null;
        // Try to get id from data-node attribute
        if (g.dataset.node) {
            try { nodeId = JSON.parse(g.dataset.node.replace(/'/g, '"')).id; } catch(_) {}
        }
        const rect = g.querySelector('rect');
        if (!rect) return;
        if (nodeId && relatedIds.has(nodeId)) {
            rect.style.filter = 'drop-shadow(0 0 6px var(--accent,#00d4aa))';
            rect.style.opacity = '1';
            // Pulse animation on the group
            g.style.animation = 'pdg-spin 0s'; // trigger reflow
            g.style.animation = '';
        } else {
            rect.style.filter = '';
            rect.style.opacity = '0.35';
        }
    });

    // Highlight edges connected to this pipeline
    svgEl.querySelectorAll('path').forEach(p => {
        const from = p.getAttribute('data-from');
        const to   = p.getAttribute('data-to');
        if (from && to && (from === jobId || to === jobId)) {
            p.style.stroke = 'var(--accent,#00d4aa)';
            p.style.strokeWidth = '2.5';
            p.style.opacity = '1';
        } else if (from && to) {
            p.style.opacity = '0.15';
        }
    });
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
    <button class="modal-close" onclick="_pdgStopAnimation();closeModal('pdg-modal')">×</button>
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
      <button id="pdg-maximize-btn" onclick="_pdgToggleMaximize()"
        title="Maximize / restore"
        style="font-size:13px;padding:2px 8px;border-radius:3px;border:1px solid var(--border);
        background:var(--bg3);color:var(--text3);cursor:pointer;line-height:1;">⊞</button>
      <span style="font-size:10px;color:var(--text3);margin-left:4px;">scroll · drag · dblclick reset · drag nodes</span>
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