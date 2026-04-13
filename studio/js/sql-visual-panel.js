/**
 * sql-visual-panel.js  —  Str:::lab Studio v0.0.22
 * ─────────────────────────────────────────────────────────────────
 * Live SQL Pipeline Visualiser embedded in the SQL editor.
 *
 * PANEL STATES
 *   collapsed  → zero-width, vertical "◈ PIPELINE" tab on right edge
 *   default    → side panel (~280 px, drag-resizable 160–55%)
 *   expanded   → overlays full editor-wrapper area (SQL still editable)
 *
 * CANVAS
 *   Pan   : drag on the canvas background
 *   Zoom  : mouse wheel, or +/- buttons in header
 *   Reset : double-click canvas background
 *
 * ANIMATION
 *   Starts when executeSQL fires, runs continuously while animating
 *   (INSERT INTO / streaming queries keep looping; DDL/SELECT stop on result)
 *
 * SQL PARSING
 *   CREATE TABLE/VIEW, INSERT INTO, SELECT, UDF/FUNCTION, SET,
 *   DROP, TEMPORARY, CATALOG, DATABASE, SYSTEM, ROUND, TABLE(),
 *   WITH (connector), SHOW, DESCRIBE, EXPLAIN
 * ─────────────────────────────────────────────────────────────────
 */
(function (root) {
    'use strict';

    // ── IDs ────────────────────────────────────────────────────────────
    const PANEL_ID      = 'svp-panel';
    const CANVAS_ID     = 'svp-canvas';
    const VIEWPORT_ID   = 'svp-viewport';   // panned/zoomed inner div
    const TOGGLE_ID     = 'svp-toggle-btn';
    const RESIZE_ID     = 'svp-resize-handle';
    const TAB_ID        = 'svp-tab';

    // ── Constants ──────────────────────────────────────────────────────
    const DEBOUNCE_MS   = 300;
    const DEFAULT_W     = 280;
    const MIN_W         = 140;
    const MAX_W_FRAC    = 0.58;
    const ANIM_FPS      = 25;
    const NODE_W        = 120;
    const NODE_H        = 50;
    const COL_GAP       = 52;
    const ROW_GAP       = 22;
    const PAD           = 24;
    const ZOOM_MIN      = 0.25;
    const ZOOM_MAX      = 3.0;
    const ZOOM_STEP     = 0.15;

    // ── Node colour palette ────────────────────────────────────────────
    const C = {
        source   : { bg:'#0d2840', border:'#4fa3e0', text:'#90ccf0' },
        sink     : { bg:'#0d2e1e', border:'#00d4aa', text:'#80e8cc' },
        window   : { bg:'#2e2200', border:'#f5c518', text:'#fde99a' },
        join     : { bg:'#1e1040', border:'#a78bfa', text:'#cdb8ff' },
        agg      : { bg:'#280840', border:'#e879f9', text:'#f0b0fe' },
        filter   : { bg:'#0d2810', border:'#34d399', text:'#90e8be' },
        project  : { bg:'#0d1e38', border:'#60a5fa', text:'#a0c8ff' },
        cte      : { bg:'#1e1e1e', border:'#9ca3af', text:'#d1d5db' },
        udf      : { bg:'#2a1a00', border:'#fb923c', text:'#fec090' },
        ddl      : { bg:'#1a1a2e', border:'#818cf8', text:'#c0c8ff' },
        set      : { bg:'#1a2e1a', border:'#6ee7b7', text:'#a0f0d0' },
        util     : { bg:'#1e1e1e', border:'#6b7280', text:'#b0b8c0' },
        default  : { bg:'#1e2130', border:'#4b5563', text:'#d1d5db' },
    };

    // ── State ──────────────────────────────────────────────────────────
    const _s = {
        panelState  : 'collapsed',
        panelWidth  : DEFAULT_W,
        nodes       : [],
        edges       : [],
        userUdfs    : new Set(),   // tracks user-defined function/view names
        animating   : false,
        looping     : false,       // true for INSERT INTO / streaming — never stops
        animTimer   : null,
        particles   : [],
        debounceTimer: null,
        pollTimer   : null,
        lastSql     : '',
        // canvas pan/zoom
        panX        : 0,
        panY        : 0,
        zoom        : 1,
        isPanning   : false,
        panStartX   : 0,
        panStartY   : 0,
        panOriginX  : 0,
        panOriginY  : 0,
        // panel resize
        resizing    : false,
        resStartX   : 0,
        resStartW   : 0,
        _laidOut    : [],
    };

    // ── SQL Parser ─────────────────────────────────────────────────────
    function _parseSql(sql) {
        const empty = { nodes: [], edges: [] };
        if (!sql || sql.trim().length < 3) return empty;

        const nodes = [];
        const edges = [];
        let seq = 0;
        const uid = () => 'n' + (++seq);

        // Strip comments, normalise whitespace for parsing
        const clean = sql
            .replace(/--[^\n]*/g, ' ')
            .replace(/\/\*[\s\S]*?\*\//g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
        const U = clean.toUpperCase();

        // Helper: find/create a node by label
        const findOrAdd = (label, type) => {
            const lo = label.toLowerCase();
            let n = nodes.find(x => x.label.toLowerCase() === lo);
            if (!n) { n = { id: uid(), label, type, sql: '' }; nodes.push(n); }
            return n;
        };

        // ── SET statements ─────────────────────────────────────────────
        const setRe = /SET\s+'([^']+)'\s*=\s*'([^']*)'/gi;
        let sm;
        while ((sm = setRe.exec(clean)) !== null) {
            nodes.push({ id: uid(), label: `SET\n${sm[1].split('.').pop()}`, type: 'set', sql: sm[0] });
        }

        // ── DROP TABLE / VIEW ──────────────────────────────────────────
        const dropRe = /DROP\s+(?:TEMPORARY\s+)?(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = dropRe.exec(clean)) !== null) {
            const name = sm[1].replace(/[`'"]/g,'').replace(/[;]/g,'');
            nodes.push({ id: uid(), label: `DROP\n${name}`, type: 'ddl', sql: sm[0] });
        }

        // ── CREATE CATALOG ─────────────────────────────────────────────
        const catRe = /CREATE\s+CATALOG\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = catRe.exec(clean)) !== null) {
            const name = sm[1].replace(/[`'"]/g,'').replace(/[;]/g,'');
            nodes.push({ id: uid(), label: `CATALOG\n${name}`, type: 'ddl', sql: sm[0] });
        }

        // ── CREATE DATABASE ────────────────────────────────────────────
        const dbRe = /CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = dbRe.exec(clean)) !== null) {
            const name = sm[1].replace(/[`'"]/g,'').replace(/[;]/g,'');
            nodes.push({ id: uid(), label: `DATABASE\n${name}`, type: 'ddl', sql: sm[0] });
        }

        // ── CREATE FUNCTION / UDF ──────────────────────────────────────
        const fnRe = /CREATE\s+(?:TEMPORARY\s+)?(?:SYSTEM\s+)?FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = fnRe.exec(clean)) !== null) {
            const name = sm[1].replace(/[`'"]/g,'').replace(/[;(]/g,'');
            _s.userUdfs.add(name.toUpperCase());
            nodes.push({ id: uid(), label: `UDF\n${name}`, type: 'udf', sql: sm[0] });
        }

        // ── CREATE TABLE / TEMPORARY TABLE ────────────────────────────
        const ctRe = /CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = ctRe.exec(clean)) !== null) {
            const tbl = sm[1].replace(/[`'"]/g,'').replace(/[;(]/g,'');
            const chunk = clean.slice(sm.index, Math.min(sm.index + 800, clean.length)).toUpperCase();
            let type = 'source';
            // Connector detection
            if (chunk.includes("'CONNECTOR'")) {
                if (chunk.match(/'CONNECTOR'\s*=\s*'(BLACKHOLE|PRINT)'/)) type = 'sink';
                else if (chunk.match(/'CONNECTOR'\s*=\s*'KAFKA'/) && chunk.includes('SINK')) type = 'sink';
                else if (chunk.match(/'CONNECTOR'\s*=\s*'(JDBC|ELASTICSEARCH|MONGODB|FILESYSTEM|REDIS)'/)) {
                    type = chunk.includes('SINK') ? 'sink' : 'source';
                }
                else if (chunk.match(/'CONNECTOR'\s*=\s*'(DATAGEN|KAFKA|PULSAR|KINESIS)'/)) type = 'source';
            }
            if (nodes.find(n => n.label === tbl)) continue; // already added
            nodes.push({ id: uid(), label: tbl, type, sql: sm[0] });
        }

        // ── CREATE VIEW ────────────────────────────────────────────────
        const cvRe = /CREATE\s+(?:TEMPORARY\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = cvRe.exec(clean)) !== null) {
            const vw = sm[1].replace(/[`'"]/g,'').replace(/[;(]/g,'');
            _s.userUdfs.add(vw.toUpperCase()); // views also act as custom identifiers
            if (nodes.find(n => n.label === vw)) continue;
            nodes.push({ id: uid(), label: vw, type: 'cte', sql: sm[0] });
        }

        // ── SHOW / DESCRIBE / EXPLAIN (utility) ───────────────────────
        const utilRe = /^(SHOW\s+\w+|DESCRIBE\s+\S+|EXPLAIN\s+)/i;
        if (utilRe.test(clean) && !U.includes('INSERT INTO') && !U.includes('SELECT')) {
            const kw = clean.match(utilRe)[0].trim().toUpperCase().split(/\s+/).slice(0, 2).join(' ');
            nodes.push({ id: uid(), label: kw, type: 'util', sql: clean });
            return { nodes, edges: [] };
        }

        // ── INSERT INTO … SELECT … ────────────────────────────────────
        const insRe = /INSERT\s+INTO\s+(\S+)\s+SELECT([\s\S]*?)FROM\s+(TABLE\s*\([^)]*\)|\S+)([\s\S]*?)(?=INSERT\s+INTO|;|$)/gi;
        while ((sm = insRe.exec(clean)) !== null) {
            const sinkName = sm[1].replace(/[`'"]/g,'').replace(/[;]/g,'');
            const rawSrc   = sm[3].replace(/[`'"]/g,'').replace(/[;)]/g,'').trim();
            // Handle TABLE(...) TVF syntax
            const srcName  = rawSrc.toUpperCase().startsWith('TABLE') ? rawSrc : rawSrc.split(/\s/)[0];
            const body     = sm[4] || '';
            const bodyU    = body.toUpperCase();
            const selCols  = sm[2] || '';

            const srcNode  = findOrAdd(srcName, 'source');
            const sinkNode = findOrAdd(sinkName, 'sink');

            const xforms = _extractTransforms(selCols, body, bodyU, uid, clean);
            let prev = srcNode.id;
            xforms.forEach(t => { nodes.push(t); edges.push({ from: prev, to: t.id }); prev = t.id; });
            edges.push({ from: prev, to: sinkNode.id });
        }

        // ── Standalone SELECT ─────────────────────────────────────────
        if (!U.includes('INSERT INTO') && U.includes('SELECT')) {
            const selRe = /SELECT([\s\S]*?)FROM\s+(TABLE\s*\([^)]*\)|\S+)([\s\S]*?)(?:;|$)/i;
            const sm2 = selRe.exec(clean);
            if (sm2) {
                const rawSrc  = sm2[2].replace(/[`'"]/g,'').replace(/[;)]/g,'').trim();
                const srcName = rawSrc.toUpperCase().startsWith('TABLE') ? rawSrc : rawSrc.split(/\s/)[0];
                const srcNode = findOrAdd(srcName, 'source');
                const body    = sm2[3] || '';
                const bodyU   = body.toUpperCase();
                const xforms  = _extractTransforms(sm2[1], body, bodyU, uid, clean);
                let prev = srcNode.id;
                xforms.forEach(t => { nodes.push(t); edges.push({ from: prev, to: t.id }); prev = t.id; });
                const out = findOrAdd('Results', 'sink');
                edges.push({ from: prev, to: out.id });
            }
        }

        // ── WITH … AS (CTE blocks not yet handled above) ──────────────
        const cteRe = /WITH\s+(\w+)\s+AS\s*\(/gi;
        while ((sm = cteRe.exec(clean)) !== null) {
            const nm = sm[1];
            if (!nodes.find(n => n.label === nm)) {
                nodes.push({ id: uid(), label: nm, type: 'cte', sql: '' });
            }
        }

        // Deduplicate edges
        const seen = new Set();
        const deduped = edges.filter(e => {
            const k = e.from + '→' + e.to;
            if (seen.has(k) || e.from === e.to) return false;
            seen.add(k); return true;
        });

        return { nodes, edges: deduped };
    }

    function _extractTransforms(selCols, body, bodyU, uid, fullSql) {
        const xforms = [];
        const colsU  = selCols.toUpperCase();

        // Window TVFs
        if (bodyU.includes('TUMBLE(') || colsU.includes('TUMBLE(')) {
            const wm = (body + selCols).match(/INTERVAL\s+'([^']+)'\s*(?:MINUTE|SECOND|HOUR|DAY)?/i);
            xforms.push({ id: uid(), label: `Tumble\n${wm ? wm[1] : ''}`, type: 'window', sql: '' });
        }
        if (bodyU.includes('HOP(') || colsU.includes('HOP('))
            xforms.push({ id: uid(), label: 'Hop Window', type: 'window', sql: '' });
        if (bodyU.includes('SESSION(') || colsU.includes('SESSION('))
            xforms.push({ id: uid(), label: 'Session Window', type: 'window', sql: '' });
        if (bodyU.includes('CUMULATE(') || colsU.includes('CUMULATE('))
            xforms.push({ id: uid(), label: 'Cumulate', type: 'window', sql: '' });

        // JOINs
        const joinRe = /(LEFT\s+OUTER|RIGHT\s+OUTER|FULL\s+OUTER|LEFT|RIGHT|INNER|CROSS)?\s*JOIN\s+(\S+)/gi;
        let jm;
        while ((jm = joinRe.exec(body)) !== null) {
            const jt  = (jm[1] || 'INNER').trim().split(/\s+/)[0];
            const jtb = jm[2].replace(/[`'"]/g,'').replace(/[;)]/g,'');
            xforms.push({ id: uid(), label: `${jt} JOIN\n${jtb}`, type: 'join', sql: '' });
        }

        // MATCH_RECOGNIZE
        if (bodyU.includes('MATCH_RECOGNIZE'))
            xforms.push({ id: uid(), label: 'CEP\nMATCH_RECOGNIZE', type: 'agg', sql: '' });

        // ROW_NUMBER
        if (colsU.includes('ROW_NUMBER()'))
            xforms.push({ id: uid(), label: 'Dedup / Top-N', type: 'project', sql: '' });

        // WHERE filter
        if (bodyU.includes('WHERE ') && !bodyU.includes('ROW_NUMBER')) {
            const wm = body.match(/WHERE\s+(.{1,55})/i);
            xforms.push({ id: uid(), label: `Filter\n${wm ? wm[1].slice(0,40) : ''}`, type: 'filter', sql: '' });
        }

        // GROUP BY
        if (bodyU.includes('GROUP BY')) {
            const gm = body.match(/GROUP\s+BY\s+(.{1,60})/i);
            xforms.push({ id: uid(), label: `Group By\n${gm ? gm[1].slice(0,35) : ''}`, type: 'agg', sql: '' });
        }

        // ROUND() — scalar transform
        if (colsU.match(/\bROUND\s*\(/))
            xforms.push({ id: uid(), label: 'ROUND\n(scalar)', type: 'project', sql: '' });

        // User-defined UDFs / VIEWs referenced in SELECT
        _s.userUdfs.forEach(fn => {
            if (colsU.includes(fn + '(') || colsU.includes(fn + ' ')) {
                xforms.push({ id: uid(), label: `UDF\n${fn}`, type: 'udf', sql: '' });
            }
        });

        // Generic project if SELECT has expressions and no other transforms
        if (!xforms.length && (colsU.includes('CASE') || colsU.includes('CAST(') || colsU.match(/\w+\s*\(/)))
            xforms.push({ id: uid(), label: 'Project / Map', type: 'project', sql: '' });

        return xforms;
    }

    // ── Layout ─────────────────────────────────────────────────────────
    function _layout(nodes, edges) {
        if (!nodes.length) return [];

        const inDeg = {}, children = {};
        nodes.forEach(n => { inDeg[n.id] = 0; children[n.id] = []; });
        edges.forEach(e => {
            if (inDeg[e.to]  !== undefined) inDeg[e.to]++;
            if (children[e.from])           children[e.from].push(e.to);
        });

        let queue  = nodes.filter(n => inDeg[n.id] === 0).map(n => n.id);
        const layer = {};
        const visited = new Set();
        let col = 0;
        while (queue.length) {
            const nxt = [];
            queue.forEach(id => {
                if (visited.has(id)) return;
                visited.add(id); layer[id] = col;
                (children[id] || []).forEach(cid => { inDeg[cid]--; if (inDeg[cid] === 0) nxt.push(cid); });
            });
            queue = nxt; col++;
        }
        nodes.filter(n => !visited.has(n.id)).forEach(n => { layer[n.id] = col++; });

        const layers = {};
        nodes.forEach(n => { const l = layer[n.id] || 0; (layers[l] = layers[l] || []).push(n.id); });

        const positioned = nodes.map(n => ({ ...n, w: NODE_W, h: NODE_H }));
        Object.entries(layers).forEach(([lStr, ids]) => {
            const l = parseInt(lStr);
            ids.forEach((id, ri) => {
                const nd = positioned.find(n => n.id === id);
                if (!nd) return;
                nd.x = PAD + l * (NODE_W + COL_GAP);
                const colH    = ids.length * NODE_H + (ids.length - 1) * ROW_GAP;
                const startY  = PAD + ri * (NODE_H + ROW_GAP);
                nd.y = startY;
                nd._colH = colH;
            });
        });
        return positioned;
    }

    // ── Render ─────────────────────────────────────────────────────────
    function _render() {
        const vp = document.getElementById(VIEWPORT_ID);
        if (!vp) return;
        vp.innerHTML = '';

        if (!_s.nodes.length) {
            vp.innerHTML = `<div style="position:absolute;inset:0;display:flex;flex-direction:column;
        align-items:center;justify-content:center;gap:10px;pointer-events:none;user-select:none;">
        <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="var(--border2,#333)" stroke-width="1">
          <circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/>
          <line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/>
        </svg>
        <span style="font-size:10px;color:var(--text3,#555);font-family:var(--mono,monospace);
          text-align:center;line-height:1.7;">Type SQL to see<br>pipeline components</span>
        <span style="font-size:9px;color:var(--text3,#444);font-family:var(--mono,monospace);">
          scroll to zoom · drag to pan</span>
      </div>`;
            _applyTransform();
            return;
        }

        const laidOut = _layout(_s.nodes, _s.edges);
        _s._laidOut   = laidOut;

        // Calculate total canvas size for the viewport div
        const maxX = Math.max(...laidOut.map(n => n.x + n.w)) + PAD;
        const maxY = Math.max(...laidOut.map(n => n.y + n.h)) + PAD;
        vp.style.width  = maxX + 'px';
        vp.style.height = maxY + 'px';

        // SVG layer for edges + particles
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('style', `position:absolute;left:0;top:0;width:${maxX}px;height:${maxY}px;overflow:visible;pointer-events:none;z-index:0;`);

        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
        defs.innerHTML = `
      <marker id="svp-arr" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto">
        <path d="M0,0 L0,6 L7,3 z" fill="rgba(100,160,220,0.6)"/>
      </marker>
      <marker id="svp-arr-act" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto">
        <path d="M0,0 L0,6 L7,3 z" fill="#00d4aa"/>
      </marker>`;
        svg.appendChild(defs);

        const edgeG = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        edgeG.id = 'svp-edges-g';
        _s.edges.forEach(e => {
            const fn = laidOut.find(n => n.id === e.from);
            const tn = laidOut.find(n => n.id === e.to);
            if (!fn || !tn) return;
            const x1 = fn.x + fn.w, y1 = fn.y + fn.h / 2;
            const x2 = tn.x,        y2 = tn.y + tn.h / 2;
            const cx1 = x1 + (x2 - x1) * 0.45;
            const cx2 = x1 + (x2 - x1) * 0.55;
            const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            p.setAttribute('d', `M${x1},${y1} C${cx1},${y1} ${cx2},${y2} ${x2},${y2}`);
            p.setAttribute('stroke', 'rgba(100,160,220,0.4)');
            p.setAttribute('stroke-width', '1.8');
            p.setAttribute('fill', 'none');
            p.setAttribute('marker-end', 'url(#svp-arr)');
            p.id = `svp-e-${e.from}-${e.to}`;
            edgeG.appendChild(p);
        });
        svg.appendChild(edgeG);

        const partG = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        partG.id = 'svp-parts-g';
        svg.appendChild(partG);
        vp.appendChild(svg);

        // Node divs
        laidOut.forEach(nd => {
            const c     = C[nd.type] || C.default;
            const icon  = _icon(nd.type);
            const lines = nd.label.split('\n');
            const el    = document.createElement('div');
            el.id       = 'svp-node-' + nd.id;
            el.style.cssText = `position:absolute;left:${nd.x}px;top:${nd.y}px;width:${nd.w}px;
        height:${nd.h}px;background:${c.bg};border:1.5px solid ${c.border};border-radius:6px;
        display:flex;flex-direction:column;align-items:flex-start;justify-content:center;
        padding:4px 8px;box-sizing:border-box;z-index:2;cursor:default;
        transition:box-shadow 0.25s,border-color 0.25s;user-select:none;overflow:hidden;`;
            el.innerHTML = `
        <div style="display:flex;align-items:center;gap:5px;width:100%;min-width:0;">
          <span style="font-size:12px;flex-shrink:0;line-height:1;">${icon}</span>
          <div style="flex:1;min-width:0;">
            <div style="font-size:9px;font-weight:700;color:${c.text};font-family:var(--mono,monospace);
              white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.3;">${_esc(lines[0])}</div>
            ${lines[1] ? `<div style="font-size:8px;color:${c.border};font-family:var(--mono,monospace);
              white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.2;opacity:0.85;">${_esc(lines[1])}</div>` : ''}
          </div>
        </div>`;
            if (nd.sql) el.title = nd.sql.slice(0, 200);
            vp.appendChild(el);
        });

        _applyTransform();

        // Re-apply animation highlight if already running
        if (_s.animating) _highlightNodes(true);
    }

    function _icon(t) {
        return { source:'⬡', sink:'◈', window:'🪟', join:'⟕', agg:'Σ', filter:'⊘',
            project:'↦', cte:'⊕', udf:'⨍', ddl:'◻', set:'⚙', util:'ℹ' }[t] || '◦';
    }

    // ── Pan / Zoom ─────────────────────────────────────────────────────
    function _applyTransform() {
        const vp = document.getElementById(VIEWPORT_ID);
        if (vp) vp.style.transform = `translate(${_s.panX}px,${_s.panY}px) scale(${_s.zoom})`;
        _updateZoomLabel();
    }

    function _updateZoomLabel() {
        const el = document.getElementById('svp-zoom-label');
        if (el) el.textContent = Math.round(_s.zoom * 100) + '%';
    }

    function _resetView() {
        _s.panX = 0; _s.panY = 0; _s.zoom = 1;
        _applyTransform();
    }

    function _zoomBy(delta, cx, cy) {
        const canvas = document.getElementById(CANVAS_ID);
        if (!canvas) return;
        const r = canvas.getBoundingClientRect();
        const mx = (cx !== undefined ? cx : r.left + r.width  / 2) - r.left;
        const my = (cy !== undefined ? cy : r.top  + r.height / 2) - r.top;
        const prev = _s.zoom;
        _s.zoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, _s.zoom + delta));
        // Zoom toward pointer
        _s.panX = mx - (mx - _s.panX) * (_s.zoom / prev);
        _s.panY = my - (my - _s.panY) * (_s.zoom / prev);
        _applyTransform();
    }

    function _wireCanvasInteraction(canvas) {
        // Wheel zoom
        canvas.addEventListener('wheel', e => {
            e.preventDefault();
            e.stopPropagation();
            const r = canvas.getBoundingClientRect();
            _zoomBy(e.deltaY < 0 ? ZOOM_STEP : -ZOOM_STEP, e.clientX - r.left, e.clientY - r.top);
        }, { passive: false });

        // Pan (mousedown on canvas background — not on node divs)
        canvas.addEventListener('mousedown', e => {
            // Only pan if not clicking a node
            if (e.target.id !== CANVAS_ID && e.target.id !== VIEWPORT_ID && !e.target.closest('svg')) return;
            if (e.button !== 0) return;
            _s.isPanning   = true;
            _s.panStartX   = e.clientX;
            _s.panStartY   = e.clientY;
            _s.panOriginX  = _s.panX;
            _s.panOriginY  = _s.panY;
            canvas.style.cursor = 'grabbing';
            e.preventDefault();
        });

        const onMove = e => {
            if (!_s.isPanning) return;
            _s.panX = _s.panOriginX + (e.clientX - _s.panStartX);
            _s.panY = _s.panOriginY + (e.clientY - _s.panStartY);
            _applyTransform();
        };

        const onUp = e => {
            if (!_s.isPanning) return;
            _s.isPanning = false;
            canvas.style.cursor = 'default';
        };

        // Double-click on background resets view
        canvas.addEventListener('dblclick', e => {
            if (e.target.id === CANVAS_ID || e.target.id === VIEWPORT_ID) _resetView();
        });

        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
    }

    // ── Animation ──────────────────────────────────────────────────────
    function startAnimation(looping) {
        // looping = true for INSERT INTO / streaming; false for DDL/SELECT
        _s.looping   = !!looping;
        if (_s.animating) return;
        _s.animating = true;
        _s.particles = [];
        _highlightNodes(true);
        _scheduleParticles();
        if (_s.animTimer) clearInterval(_s.animTimer);
        _s.animTimer = setInterval(_animFrame, Math.round(1000 / ANIM_FPS));
    }

    function stopAnimation() {
        if (_s.looping) return;   // streaming — keep running
        _s.animating = false;
        if (_s.animTimer) { clearInterval(_s.animTimer); _s.animTimer = null; }
        _s.particles = [];
        _clearParticles();
        _highlightNodes(false);
    }

    function forceStopAnimation() {
        _s.looping   = false;
        _s.animating = false;
        if (_s.animTimer) { clearInterval(_s.animTimer); _s.animTimer = null; }
        _s.particles = [];
        _clearParticles();
        _highlightNodes(false);
    }

    function _highlightNodes(on) {
        (_s._laidOut || []).forEach((nd, idx) => {
            const el = document.getElementById('svp-node-' + nd.id);
            if (!el) return;
            const c = C[nd.type] || C.default;
            if (on) {
                setTimeout(() => {
                    if (!_s.animating) return;
                    el.style.boxShadow   = `0 0 12px ${c.border}88, 0 0 4px ${c.border}66`;
                    el.style.animation   = 'svp-pulse 1.8s ease-in-out infinite';
                    el.style.borderColor = c.border;
                }, idx * 100);
            } else {
                el.style.boxShadow   = '';
                el.style.animation   = '';
                el.style.borderColor = '';
            }
        });

        // Edge highlight
        document.querySelectorAll('#svp-edges-g path').forEach(p => {
            p.setAttribute('stroke',       on ? 'rgba(0,212,170,0.65)' : 'rgba(100,160,220,0.4)');
            p.setAttribute('stroke-width', on ? '2.2' : '1.8');
            p.setAttribute('marker-end',   on ? 'url(#svp-arr-act)' : 'url(#svp-arr)');
        });
    }

    function _scheduleParticles() {
        if (!_s.animating) return;
        _s.edges.forEach(e => {
            const delay = Math.random() * 500;
            setTimeout(() => {
                if (!_s.animating) return;
                _s.particles.push({ from: e.from, to: e.to, t: 0, spd: 0.016 + Math.random() * 0.014, r: 3.5 });
            }, delay);
        });
        // Continuous respawn
        setTimeout(() => { if (_s.animating) _scheduleParticles(); }, 1000);
    }

    function _animFrame() {
        if (!_s.animating) return;
        const g = document.getElementById('svp-parts-g');
        if (!g) return;
        const lo = _s._laidOut;
        let html = '';
        for (let i = _s.particles.length - 1; i >= 0; i--) {
            const p  = _s.particles[i];
            const fn = lo.find(n => n.id === p.from);
            const tn = lo.find(n => n.id === p.to);
            if (!fn || !tn) { _s.particles.splice(i, 1); continue; }
            p.t += p.spd;
            if (p.t > 1) { _s.particles.splice(i, 1); continue; }
            const x1 = fn.x + fn.w, y1 = fn.y + fn.h / 2;
            const x2 = tn.x,        y2 = tn.y + tn.h / 2;
            const cx1= x1 + (x2-x1)*0.45, cx2= x1 + (x2-x1)*0.55;
            const t = p.t, mt = 1-t;
            const px = mt*mt*mt*x1 + 3*mt*mt*t*cx1 + 3*mt*t*t*cx2 + t*t*t*x2;
            const py = mt*mt*mt*y1 + 3*mt*mt*t*y1  + 3*mt*t*t*y2  + t*t*t*y2;
            const a  = Math.sin(t * Math.PI);
            html += `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="${p.r}" fill="#00d4aa" opacity="${a.toFixed(2)}"/>`;
        }
        g.innerHTML = html;
    }

    function _clearParticles() {
        const g = document.getElementById('svp-parts-g');
        if (g) g.innerHTML = '';
    }

    // ── Panel DOM ──────────────────────────────────────────────────────
    function _buildPanel() {
        _injectCSS();

        // Collapsed tab
        const tab = document.createElement('button');
        tab.id = TAB_ID;
        tab.title = 'Show pipeline visualiser';
        tab.innerHTML = `
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/>
        <line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/>
      </svg>
      <span>PIPELINE</span>`;
        tab.addEventListener('click', () => _setState('default'));

        // Panel
        const panel = document.createElement('div');
        panel.id = PANEL_ID;
        panel.style.width = _s.panelWidth + 'px';

        // Resize handle (left edge)
        const handle = document.createElement('div');
        handle.id = RESIZE_ID;
        panel.appendChild(handle);
        _wireResize(handle);

        // Collapse button on left edge of panel
        const toggleBtn = document.createElement('button');
        toggleBtn.id    = TOGGLE_ID;
        toggleBtn.title = 'Collapse panel';
        toggleBtn.innerHTML = _toggleIcon();
        toggleBtn.addEventListener('click', _cycleState);
        panel.appendChild(toggleBtn);

        // Header
        const hdr = document.createElement('div');
        hdr.id = 'svp-hdr';
        hdr.innerHTML = `
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="var(--accent,#00d4aa)" stroke-width="2">
        <circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/>
        <line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/>
      </svg>
      <span style="font-size:9px;font-weight:700;color:var(--text3,#4a6a7a);letter-spacing:1.2px;
        text-transform:uppercase;font-family:var(--mono,monospace);flex:1;">Pipeline</span>
      <span id="svp-node-count" style="font-size:9px;color:var(--text3,#4a6a7a);
        font-family:var(--mono,monospace);margin-right:4px;"></span>
      <span id="svp-zoom-label" style="font-size:9px;color:var(--text3,#4a6a7a);
        font-family:var(--mono,monospace);min-width:30px;text-align:right;">100%</span>
      <button onclick="window._svp._zoomBy(-0.15)" title="Zoom out"
        style="background:none;border:none;cursor:pointer;color:var(--text3,#4a6a7a);
        padding:1px 4px;font-size:13px;line-height:1;">−</button>
      <button onclick="window._svp._zoomBy(0.15)" title="Zoom in"
        style="background:none;border:none;cursor:pointer;color:var(--text3,#4a6a7a);
        padding:1px 4px;font-size:13px;line-height:1;">+</button>
      <button onclick="window._svp._resetView()" title="Reset view (dbl-click canvas)"
        style="background:none;border:none;cursor:pointer;color:var(--text3,#4a6a7a);
        padding:1px 4px;font-size:10px;line-height:1;">⊙</button>
      <button id="svp-expand-btn" onclick="window._svp._toggleExpand()" title="Expand / shrink"
        style="background:none;border:none;cursor:pointer;color:var(--text3,#4a6a7a);
        padding:1px 4px;font-size:12px;line-height:1;">⊞</button>`;
        panel.appendChild(hdr);

        // Canvas (outer scroll + interaction surface)
        const canvas = document.createElement('div');
        canvas.id = CANVAS_ID;
        // Viewport (inner panned/zoomed surface)
        const vp = document.createElement('div');
        vp.id    = VIEWPORT_ID;
        canvas.appendChild(vp);
        _wireCanvasInteraction(canvas);
        panel.appendChild(canvas);

        return { panel, tab };
    }

    function _toggleIcon() {
        return _s.panelState === 'collapsed'
            ? `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>`
            : `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>`;
    }

    function _cycleState() {
        _setState(_s.panelState === 'collapsed' ? 'default' : 'collapsed');
    }

    function _setState(state) {
        _s.panelState = state;
        const panel  = document.getElementById(PANEL_ID);
        const tab    = document.getElementById(TAB_ID);
        const btn    = document.getElementById(TOGGLE_ID);
        const expBtn = document.getElementById('svp-expand-btn');
        if (!panel) return;

        panel.classList.remove('svp-collapsed', 'svp-expanded');
        if (tab) tab.classList.remove('svp-tab-vis');

        if (state === 'collapsed') {
            panel.classList.add('svp-collapsed');
            panel.style.width = '0';
            if (tab) tab.classList.add('svp-tab-vis');
        } else if (state === 'expanded') {
            panel.classList.add('svp-expanded');
            panel.style.width = '';
            if (expBtn) expBtn.textContent = '⊟';
        } else {
            panel.style.width = _s.panelWidth + 'px';
            if (expBtn) expBtn.textContent = '⊞';
        }
        if (btn) btn.innerHTML = _toggleIcon();

        // Give layout a tick then render
        setTimeout(_render, 30);
    }

    // ── Panel resize (drag handle) ─────────────────────────────────────
    function _wireResize(handle) {
        handle.addEventListener('mousedown', e => {
            e.preventDefault();
            _s.resizing  = true;
            _s.resStartX = e.clientX;
            _s.resStartW = _s.panelWidth;
            const onMove = e2 => {
                if (!_s.resizing) return;
                const ea  = document.getElementById('editor-area');
                const maxW= (ea ? ea.offsetWidth : window.innerWidth) * MAX_W_FRAC;
                _s.panelWidth = Math.max(MIN_W, Math.min(maxW, _s.resStartW + (_s.resStartX - e2.clientX)));
                const p = document.getElementById(PANEL_ID);
                if (p && _s.panelState === 'default') p.style.width = _s.panelWidth + 'px';
            };
            const onUp = () => {
                _s.resizing = false;
                window.removeEventListener('mousemove', onMove);
                window.removeEventListener('mouseup',   onUp);
                _render();
            };
            window.addEventListener('mousemove', onMove);
            window.addEventListener('mouseup',   onUp);
        });
    }

    // ── CSS injection ─────────────────────────────────────────────────
    function _injectCSS() {
        if (document.getElementById('svp-css')) return;
        const s = document.createElement('style');
        s.id = 'svp-css';
        s.textContent = `
      @keyframes svp-pulse {
        0%,100% { filter: brightness(1); }
        50%      { filter: brightness(1.35); }
      }
      #svp-panel {
        position: relative;
        display: flex;
        flex-direction: column;
        background: var(--bg1,#0c1219);
        border-left: 1px solid var(--border,#1a2a3a);
        flex-shrink: 0;
        overflow: hidden;
        transition: width 0.2s ease;
        z-index: 5;
      }
      #svp-panel.svp-collapsed {
        width: 0 !important;
        border-left: none;
        min-width: 0 !important;
      }
      /* Expanded: absolutely fills the svp-editor-row, SQL editor still usable below */
      #svp-panel.svp-expanded {
        position: absolute !important;
        inset: 0 !important;
        width: 100% !important;
        z-index: 18 !important;
        border-left: none !important;
        transition: none !important;
      }
      #svp-hdr {
        display: flex;
        align-items: center;
        padding: 4px 8px;
        border-bottom: 1px solid var(--border,#1a2a3a);
        background: var(--bg2,#0f1924);
        flex-shrink: 0;
        gap: 4px;
        min-height: 28px;
      }
      #${CANVAS_ID} {
        flex: 1;
        position: relative;
        overflow: hidden;
        background: var(--bg0,#080b0f);
        cursor: default;
      }
      #${VIEWPORT_ID} {
        position: absolute;
        top: 0; left: 0;
        transform-origin: 0 0;
        will-change: transform;
      }
      #${RESIZE_ID} {
        position: absolute;
        left: 0; top: 0; bottom: 0;
        width: 5px;
        cursor: col-resize;
        z-index: 10;
        background: transparent;
        transition: background 0.15s;
      }
      #${RESIZE_ID}:hover { background: rgba(0,212,170,0.2); }
      #${TOGGLE_ID} {
        position: absolute;
        left: -20px;
        top: 50%;
        transform: translateY(-50%);
        z-index: 30;
        background: var(--bg2,#0f1924);
        border: 1px solid var(--border,#1a2a3a);
        border-right: none;
        color: var(--text3,#4a6a7a);
        cursor: pointer;
        padding: 9px 4px;
        border-radius: 4px 0 0 4px;
        line-height: 1;
        display: flex;
        flex-direction: column;
        align-items: center;
      }
      #${TOGGLE_ID}:hover { color: var(--accent,#00d4aa); background: var(--bg1,#0c1219); }
      #${TAB_ID} {
        position: absolute;
        right: 0;
        top: 50%;
        transform: translateY(-50%);
        z-index: 25;
        background: var(--bg2,#0f1924);
        border: 1px solid var(--border,#1a2a3a);
        border-right: none;
        border-radius: 4px 0 0 4px;
        padding: 10px 5px;
        cursor: pointer;
        display: none;
        flex-direction: column;
        align-items: center;
        gap: 4px;
        color: var(--text3,#4a6a7a);
        font-size: 9px;
        font-family: var(--mono,monospace);
        writing-mode: vertical-rl;
        letter-spacing: 1px;
      }
      #${TAB_ID}:hover { color: var(--accent,#00d4aa); }
      #${TAB_ID}.svp-tab-vis { display: flex; }
      /* When expanded, allow SQL editor to still receive pointer events via pointer-events layering */
      #svp-editor-row { isolation: isolate; }
    `;
        document.head.appendChild(s);
    }

    // ── Editor watcher ─────────────────────────────────────────────────
    function _watchEditor() {
        const ed = document.getElementById('sql-editor');
        if (!ed) { setTimeout(_watchEditor, 400); return; }

        const onChange = () => {
            clearTimeout(_s.debounceTimer);
            _s.debounceTimer = setTimeout(() => {
                const sql = ed.value || '';
                if (sql === _s.lastSql) return;
                _s.lastSql = sql;
                const res = _parseSql(sql);
                _s.nodes  = res.nodes;
                _s.edges  = res.edges;
                const cnt = document.getElementById('svp-node-count');
                if (cnt) cnt.textContent = _s.nodes.length ? `${_s.nodes.length} nodes` : '';
                if (_s.panelState !== 'collapsed') _render();
            }, DEBOUNCE_MS);
        };

        ed.addEventListener('input', onChange);
        // Poll for programmatic changes (tab switch, project load)
        _s.pollTimer = setInterval(() => {
            const sql = (ed.value || '');
            if (sql !== _s.lastSql) onChange();
        }, 1500);

        // Initial parse
        if (ed.value) onChange();
    }

    // ── Session / project change hooks ────────────────────────────────
    // Patches clearResults and switchSession to reset the visualiser state
    function _patchSessionClear() {
        // Clear visualiser when the studio clears results/logs (new project loaded)
        const _origClearResults = root.clearResults;
        root.clearResults = function () {
            _resetVisualisations();
            if (_origClearResults) return _origClearResults.apply(this, arguments);
        };

        // Patch switchSession to clear job graph display and SVP state
        const _origSwitch = root.switchSession;
        root.switchSession = function () {
            _resetVisualisations();
            if (_origSwitch) return _origSwitch.apply(this, arguments);
        };

        // Also patch addTab / loadHistoryItem to re-parse on tab switch
        const _origLoadHistory = root.loadHistoryItem;
        root.loadHistoryItem = function () {
            const res = _origLoadHistory ? _origLoadHistory.apply(this, arguments) : undefined;
            // Re-trigger parse on next tick
            setTimeout(() => {
                const ed = document.getElementById('sql-editor');
                if (ed) { _s.lastSql = ''; /* force reparse */ }
            }, 80);
            return res;
        };
    }

    function _resetVisualisations() {
        forceStopAnimation();
        _s.nodes    = [];
        _s.edges    = [];
        _s.lastSql  = '';
        _s.userUdfs = new Set();
        const cnt = document.getElementById('svp-node-count');
        if (cnt) cnt.textContent = '';
        if (_s.panelState !== 'collapsed') _render();
        // Also clear job graph from previous project
        const jgSel = document.getElementById('jg-job-select');
        const jgWrap= document.getElementById('jg-canvas-wrap');
        if (jgSel) {
            jgSel.innerHTML = '<option value="">— Select a job —</option>';
        }
        if (jgWrap) {
            jgWrap.innerHTML = `<div style="display:flex;flex-direction:column;align-items:center;
        justify-content:center;height:100%;gap:10px;color:var(--text3);">
        <div style="font-size:28px;opacity:0.3;">◈</div>
        <div style="font-size:12px;">Session changed — refresh jobs to reload</div>
      </div>`;
        }
        // Reset status badge
        const badge = document.getElementById('jg-job-status-badge');
        if (badge) badge.style.display = 'none';
    }

    // ── Public API ─────────────────────────────────────────────────────
    root._svp = {
        startAnimation,
        stopAnimation,
        forceStopAnimation,
        setState     : _setState,
        getState     : () => _s.panelState,
        _toggleExpand: () => _setState(_s.panelState === 'expanded' ? 'default' : 'expanded'),
        _resetView   : _resetView,
        _zoomBy      : _zoomBy,
        resetVis     : _resetVisualisations,
    };

    // ── Initialise ─────────────────────────────────────────────────────
    function _init() {
        const editorArea    = document.getElementById('editor-area');
        const editorWrapper = document.getElementById('editor-wrapper');
        if (!editorArea || !editorWrapper) { setTimeout(_init, 400); return; }

        // Create flex-row container around editor-wrapper + panel
        let rowEl = document.getElementById('svp-editor-row');
        if (!rowEl) {
            rowEl = document.createElement('div');
            rowEl.id = 'svp-editor-row';
            rowEl.style.cssText = 'display:flex;flex:1;overflow:hidden;min-height:0;position:relative;';
            editorWrapper.parentNode.insertBefore(rowEl, editorWrapper);
            rowEl.appendChild(editorWrapper);
            // Make editor-wrapper flex:1 so it fills remaining space
            editorWrapper.style.flex = '1';
            editorWrapper.style.minWidth = '0';
        }

        const { panel, tab } = _buildPanel();
        rowEl.appendChild(panel);
        rowEl.appendChild(tab);

        _setState('default');
        _watchEditor();
        _patchSessionClear();
    }

    function _esc(s) {
        return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        setTimeout(_init, 80);
    }

})(window);