/**
 * sql-visual-panel  —  Str:::lab Studio v0.0.22
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

        // Helper: find/create a node by label.
        // When INSERT INTO creates a source/sink by name, search ALL available SQL
        // sources for a matching CREATE TABLE DDL — not just the current tab.
        const findOrAdd = (label, type) => {
            const lo = label.toLowerCase();
            let n = nodes.find(x => x.label.toLowerCase() === lo);
            if (!n) {
                // Search all available SQL sources for a CREATE TABLE matching this label.
                // _svpFindDdl also checks the current tab's raw sql first via DOM.
                // Also directly search the raw sql passed to _parseSql (fastest path).
                let ddl = '';
                const _esc2 = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const _re = new RegExp(
                    'CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?' +
                    _esc2 + '\\s*\\(', 'i'
                );
                const _m = _re.exec(sql);
                if (_m) {
                    // Fast path: found in current tab's raw sql
                    let depth = 0, pos = _m.index, inBody = false;
                    while (pos < sql.length) {
                        const ch = sql[pos];
                        if (ch === '(')      { depth++; inBody = true; }
                        else if (ch === ')') { depth--; }
                        else if (ch === ';' && depth === 0 && inBody) { pos++; break; }
                        pos++;
                        if (inBody && depth === 0) {
                            const rest = sql.slice(pos);
                            const wm = rest.match(/^\s*WITH\s*\(/i);
                            if (wm) {
                                pos += rest.indexOf('(') + 1;
                                let d2 = 1;
                                while (pos < sql.length && d2 > 0) {
                                    if (sql[pos] === '(') d2++;
                                    else if (sql[pos] === ')') d2--;
                                    pos++;
                                }
                            }
                            const semi = sql.slice(pos).match(/^\s*;/);
                            if (semi) pos += semi[0].length;
                            break;
                        }
                    }
                    ddl = sql.slice(_m.index, pos).trim();
                } else {
                    // Slow path: search all other accessible SQL sources
                    ddl = _svpFindDdl(label);
                }

                n = { id: uid(), label, type, sql: ddl };
                nodes.push(n);
            }
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

            // Capture full DDL for this table (up to semicolon)
            const ddlStart = sm.index;
            const ddlEnd   = clean.indexOf(';', ddlStart);
            const tableSql = ddlEnd > -1
                ? clean.slice(ddlStart, ddlEnd + 1)
                : clean.slice(ddlStart, Math.min(ddlStart + 1200, clean.length));

            nodes.push({ id: uid(), label: tbl, type, sql: tableSql });
        }

        // ── CREATE VIEW ────────────────────────────────────────────────
        const cvRe = /CREATE\s+(?:TEMPORARY\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/gi;
        while ((sm = cvRe.exec(clean)) !== null) {
            const vw = sm[1].replace(/[`'"]/g,'').replace(/[;(]/g,'');
            _s.userUdfs.add(vw.toUpperCase()); // views also act as custom identifiers
            if (nodes.find(n => n.label === vw)) continue;

            const vddlStart = sm.index;
            const vddlEnd   = clean.indexOf(';', vddlStart);
            const viewSql   = vddlEnd > -1
                ? clean.slice(vddlStart, vddlEnd + 1)
                : clean.slice(vddlStart, Math.min(vddlStart + 1200, clean.length));

            nodes.push({ id: uid(), label: vw, type: 'cte', sql: viewSql });
        }

        // ── SHOW / DESCRIBE / EXPLAIN (utility) ───────────────────────
        // If the script ONLY contains utility statements, return early.
        // If mixed with DML, add utility nodes but continue parsing.
        const utilRe = /^(SHOW\s+\w+|DESCRIBE\s+\S+|EXPLAIN\s+)/i;
        if (utilRe.test(clean) && !U.includes('INSERT INTO') && !U.includes('SELECT')) {
            const kw = clean.match(utilRe)[0].trim().toUpperCase().split(/\s+/).slice(0, 2).join(' ');
            nodes.push({ id: uid(), label: kw, type: 'util', sql: clean });
            return { nodes, edges: [] };
        }
        // Mid-script SHOW/DESCRIBE/EXPLAIN — add a node but don't short-circuit
        const midUtilRe = /;\s*(SHOW\s+\w+|DESCRIBE\s+\S+|EXPLAIN\s+[A-Z]+)/gi;
        let muM;
        while ((muM = midUtilRe.exec(clean)) !== null) {
            const kw = muM[1].trim().toUpperCase().split(/\s+/).slice(0, 2).join(' ');
            if (!nodes.find(n => n.label === kw)) {
                nodes.push({ id: uid(), label: kw, type: 'util', sql: muM[1] });
            }
        }

        // ── INSERT INTO … SELECT … ────────────────────────────────────
        const insRe = /INSERT\s+INTO\s+(\S+)\s+SELECT([\s\S]*?)FROM\s+(TABLE\s*\([^)]*\)|\S+)([\s\S]*?)(?=INSERT\s+INTO|;|$)/gi;
        while ((sm = insRe.exec(clean)) !== null) {
            const sinkName = sm[1].replace(/[`'"]/g,'').replace(/[;]/g,'');
            const rawSrc   = sm[3].replace(/[`'"]/g,'').replace(/[;)]/g,'').trim();
            // Handle TABLE(...) TVF syntax — extract inner table name e.g. TABLE(TUMBLE(TABLE src,...))
            let srcName;
            if (rawSrc.toUpperCase().startsWith('TABLE')) {
                // Try to find TABLE <name> inside the TVF args
                const innerTbl = rawSrc.match(/TABLE\s+(\w+)/i);
                srcName = innerTbl ? innerTbl[1] : rawSrc.split(/\s/)[0];
            } else {
                srcName = rawSrc.split(/\s/)[0];
            }
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
        // Fire for any SELECT that is NOT part of an INSERT INTO … SELECT statement.
        // Works whether or not INSERT INTO is also present in the script.
        if (U.includes('SELECT')) {
            // Collect positions of SELECTs that belong to INSERT INTO statements
            const _insSelPositions = new Set();
            const _insScan = /INSERT\s+INTO\s+\S+\s+(SELECT)/gi;
            let _ism;
            while ((_ism = _insScan.exec(clean)) !== null) {
                _insSelPositions.add(_ism.index + _ism[0].lastIndexOf('SELECT'));
            }

            // Find the first standalone SELECT (not part of an INSERT INTO)
            const _selScan = /\bSELECT\b/gi;
            let _ssm, _standalonePos = -1;
            while ((_ssm = _selScan.exec(clean)) !== null) {
                if (!_insSelPositions.has(_ssm.index)) { _standalonePos = _ssm.index; break; }
            }

            // Parse the standalone SELECT if found
            if (_standalonePos > -1) {
                const _selFragment = clean.slice(_standalonePos);
                const selRe = /SELECT([\s\S]*?)FROM\s+(TABLE\s*\([^)]*\)|\S+)([\s\S]*?)(?:;|$)/i;
                const sm2 = selRe.exec(_selFragment);
                if (sm2) {
                    const rawSrc  = sm2[2].replace(/[`'"]/g,'').replace(/[;)]/g,'').trim();
                    let srcName;
                    if (rawSrc.toUpperCase().startsWith('TABLE')) {
                        const innerTbl = rawSrc.match(/TABLE\s+(\w+)/i);
                        srcName = innerTbl ? innerTbl[1] : rawSrc.split(/\s/)[0];
                    } else {
                        srcName = rawSrc.split(/\s/)[0];
                    }
                    const srcNode = findOrAdd(srcName, 'source');
                    const body    = sm2[3] || '';
                    const bodyU   = body.toUpperCase();
                    const xforms  = _extractTransforms(sm2[1], body, bodyU, uid, clean);
                    let prev = srcNode.id;
                    xforms.forEach(t => { nodes.push(t); edges.push({ from: prev, to: t.id }); prev = t.id; });
                    const out = findOrAdd('Results', 'sink');
                    edges.push({ from: prev, to: out.id });
                } else {
                    // SELECT without FROM — pure expression query (e.g. UDF smoke-test, scalar calls)
                    const selNoFrom = /SELECT\s+([\s\S]*?)(?:;|$)/i.exec(clean);
                    if (selNoFrom) {
                        const cols  = selNoFrom[1];
                        const colsU = cols.toUpperCase();
                        // Find every function-call pattern: word( — collect unique function names
                        const fnRe  = /\b([A-Z_][A-Z0-9_]*)\s*\(/gi;
                        let fm;
                        const builtins = new Set(['COUNT','SUM','AVG','MIN','MAX','CAST','TRY_CAST',
                            'COALESCE','NULLIF','IF','CONCAT','SUBSTRING','TRIM','UPPER','LOWER',
                            'LENGTH','REPLACE','DATE_FORMAT','TO_TIMESTAMP','UNIX_TIMESTAMP',
                            'TUMBLE','HOP','SESSION','CUMULATE','ROW_NUMBER','RANK','DENSE_RANK',
                            'LEAD','LAG','FIRST_VALUE','LAST_VALUE','NTH_VALUE','REGEXP_EXTRACT',
                            'REGEXP_REPLACE','JSON_VALUE','JSON_QUERY','ARRAY','MAP','ROW',
                            'CARDINALITY','ROUND','FLOOR','CEIL','ABS','MOD','POWER','SQRT',
                            'PROCTIME','NOW','CURRENT_TIMESTAMP','CURRENT_DATE','CURRENT_TIME']);
                        const udfsInQuery = new Set();
                        while ((fm = fnRe.exec(colsU)) !== null) {
                            const name = fm[1];
                            if (!builtins.has(name)) udfsInQuery.add(name);
                        }
                        // Also add any names already registered in _s.userUdfs
                        _s.userUdfs.forEach(fn => { if (colsU.includes(fn + '(')) udfsInQuery.add(fn); });

                        if (udfsInQuery.size) {
                            const srcNode = findOrAdd('Input', 'source');
                            let prev = srcNode.id;
                            udfsInQuery.forEach(fn => {
                                const t = { id: uid(), label: `UDF\n${fn}`, type: 'udf', sql: '' };
                                nodes.push(t);
                                edges.push({ from: prev, to: t.id });
                                prev = t.id;
                            });
                            const out = findOrAdd('Results', 'sink');
                            edges.push({ from: prev, to: out.id });
                        } else {
                            // Scalar-only SELECT — show as a single project node
                            const srcNode = findOrAdd('Input', 'source');
                            const t = { id: uid(), label: 'Project / Map', type: 'project', sql: '' };
                            nodes.push(t);
                            edges.push({ from: srcNode.id, to: t.id });
                            const out = findOrAdd('Results', 'sink');
                            edges.push({ from: t.id, to: out.id });
                        }
                    }
                }
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

        // User-defined UDFs / VIEWs referenced in SELECT columns
        // First: names explicitly registered via CREATE FUNCTION in this script
        _s.userUdfs.forEach(fn => {
            if (colsU.includes(fn + '(') || colsU.includes(fn + ' ')) {
                xforms.push({ id: uid(), label: `UDF\n${fn}`, type: 'udf', sql: '' });
            }
        });
        // Second: any unrecognised function call pattern that wasn't handled above
        // Catches UDFs whose CREATE FUNCTION is in a different tab or was run earlier
        const _builtinFns = new Set(['COUNT','SUM','AVG','MIN','MAX','CAST','TRY_CAST',
            'COALESCE','NULLIF','IF','CONCAT','SUBSTRING','TRIM','UPPER','LOWER',
            'LENGTH','REPLACE','DATE_FORMAT','TO_TIMESTAMP','UNIX_TIMESTAMP',
            'TUMBLE','HOP','SESSION','CUMULATE','ROW_NUMBER','RANK','DENSE_RANK',
            'LEAD','LAG','FIRST_VALUE','LAST_VALUE','NTH_VALUE','REGEXP_EXTRACT',
            'REGEXP_REPLACE','JSON_VALUE','JSON_QUERY','ARRAY','MAP','ROW',
            'CARDINALITY','ROUND','FLOOR','CEIL','ABS','MOD','POWER','SQRT',
            'PROCTIME','NOW','CURRENT_TIMESTAMP','CURRENT_DATE','CURRENT_TIME',
            'TUMBLE_START','TUMBLE_END','HOP_START','HOP_END','SESSION_START','SESSION_END']);
        const _fnCallRe = /\b([A-Z_][A-Z0-9_]*)\s*\(/g;
        const _alreadyAdded = new Set(xforms.map(x => x.label.split('\n').pop()));
        let _fm;
        while ((_fm = _fnCallRe.exec(colsU)) !== null) {
            const name = _fm[1];
            if (!_builtinFns.has(name) && !_s.userUdfs.has(name) && !_alreadyAdded.has(name)) {
                xforms.push({ id: uid(), label: `UDF\n${name}`, type: 'udf', sql: '' });
                _alreadyAdded.add(name);
            }
        }

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

    // ── Live DDL lookup (used by both parser and modal) ─────────────────
    // Builds a corpus of all SQL accessible in the browser right now and
    // searches it for a CREATE TABLE matching the given table name.
    function _svpFindDdl(label) {
        const corpus = [];

        // 1. Current editor tab
        try {
            const ed = document.getElementById('sql-editor');
            if (ed && ed.value) corpus.push(ed.value);
        } catch(_) {}

        // 2. All textarea elements (other open tabs)
        try {
            document.querySelectorAll('textarea').forEach(el => {
                if (el.value && el.value.length > 10 && el !== document.getElementById('sql-editor'))
                    corpus.push(el.value);
            });
        } catch(_) {}

        // 3. CodeMirror instances
        try {
            document.querySelectorAll('.CodeMirror').forEach(el => {
                if (el.CodeMirror) {
                    const v = el.CodeMirror.getValue();
                    if (v && v.length > 10) corpus.push(v);
                }
            });
        } catch(_) {}

        // 4. Studio global state objects
        try {
            ['state','_state','appState','sessions','_sessions','tabs','_tabs',
                'editorState','studioState','projectState'].forEach(key => {
                const obj = window[key];
                if (!obj) return;
                const arr = Array.isArray(obj) ? obj : Object.values(obj);
                arr.forEach(item => {
                    if (typeof item === 'string' && item.length > 10) { corpus.push(item); return; }
                    if (!item || typeof item !== 'object') return;
                    const s = item.sql || item.content || item.text || item.query || item.script || item.value || '';
                    if (s && s.length > 10) corpus.push(s);
                    ['tabs','sessions','queries','scripts'].forEach(k => {
                        if (Array.isArray(item[k])) item[k].forEach(t => {
                            const ts = typeof t === 'string' ? t : (t.sql || t.content || t.text || '');
                            if (ts && ts.length > 10) corpus.push(ts);
                        });
                    });
                });
            });
        } catch(_) {}

        // 5. sessionStorage / localStorage
        try {
            [sessionStorage, localStorage].forEach(store => {
                try {
                    for (let i = 0; i < store.length; i++) {
                        const key = store.key(i);
                        if (!key || !/sql|tab|session|query|script|editor|content/i.test(key)) continue;
                        try {
                            const val = store.getItem(key);
                            if (!val || val.length < 10) continue;
                            const c0 = val.trimStart()[0];
                            if (c0 === '[' || c0 === '{') {
                                try {
                                    const parsed = JSON.parse(val);
                                    const items = Array.isArray(parsed) ? parsed : Object.values(parsed);
                                    items.forEach(item => {
                                        const s = typeof item === 'string' ? item
                                            : (item.sql || item.content || item.text || item.query || '');
                                        if (s && s.length > 10) corpus.push(s);
                                        if (Array.isArray(item.tabs)) item.tabs.forEach(t => {
                                            const ts = t.sql || t.content || t.text || '';
                                            if (ts && ts.length > 10) corpus.push(ts);
                                        });
                                    });
                                } catch(_) { corpus.push(val); }
                            } else { corpus.push(val); }
                        } catch(_) {}
                    }
                } catch(_) {}
            });
        } catch(_) {}

        if (!corpus.length) return '';

        // Search corpus for CREATE TABLE <label>
        const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const re1 = new RegExp(
            'CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?' +
            '[`\'\"]+?' + escaped + '[`\'\"]+?\\s*\\(', 'i'
        );
        const re2 = new RegExp(
            'CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?' +
            escaped + '\\s*\\(', 'i'
        );

        for (const src of corpus) {
            const m = re1.exec(src) || re2.exec(src);
            if (!m) continue;
            // Extract full DDL including WITH block and semicolon
            let depth = 0, pos = m.index, inBody = false;
            while (pos < src.length) {
                const ch = src[pos];
                if (ch === '(')      { depth++; inBody = true; }
                else if (ch === ')') { depth--; }
                else if (ch === ';' && depth === 0 && inBody) { pos++; break; }
                pos++;
                if (inBody && depth === 0) {
                    const rest = src.slice(pos);
                    const withM = rest.match(/^\s*WITH\s*\(/i);
                    if (withM) {
                        pos += rest.indexOf('(') + 1;
                        let d2 = 1;
                        while (pos < src.length && d2 > 0) {
                            if (src[pos] === '(') d2++;
                            else if (src[pos] === ')') d2--;
                            pos++;
                        }
                    }
                    const semi = src.slice(pos).match(/^\s*;/);
                    if (semi) pos += semi[0].length;
                    break;
                }
            }
            const ddl = src.slice(m.index, pos).trim();
            if (ddl) return ddl;
        }
        return '';
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
        padding:4px 8px;box-sizing:border-box;z-index:2;cursor:pointer;
        transition:box-shadow 0.25s,border-color 0.25s;user-select:none;overflow:hidden;`;
            el.addEventListener('mouseenter', () => {
                el.style.boxShadow = `0 0 0 2px ${c.border}`;
                el.style.borderColor = c.text;
            });
            el.addEventListener('mouseleave', () => {
                el.style.boxShadow = '';
                el.style.borderColor = c.border;
            });
            el.addEventListener('dblclick', e => {
                e.stopPropagation();
                _svpShowNodeDetail(nd, c);
            });
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


    // ── Node detail modal ─────────────────────────────────────────────
    function _svpShowNodeDetail(nd, c) {
        const old = document.getElementById('svp-node-detail');
        if (old) old.remove();

        const type  = nd.type || 'default';
        const lines = (nd.label || '').split('\n');
        const title = lines[0] || type;
        const sub   = lines[1] || '';
        const sql   = nd.sql || '';

        // ── Shared render helpers ──────────────────────────────────────────

        const _esc = s => (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

        const pill = (text, color) =>
            `<span style="display:inline-block;font-size:9px;font-family:var(--mono);padding:1px 7px;
      border-radius:10px;background:${color}22;border:1px solid ${color}55;color:${color};
      white-space:nowrap;">${_esc(text)}</span>`;

        const kv = (label, val, extra) =>
            `<div style="display:flex;align-items:baseline;gap:8px;padding:5px 0;
      border-bottom:1px solid rgba(255,255,255,0.04);">
      <span style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;
        color:var(--text3);font-family:var(--mono);width:88px;flex-shrink:0;">${label}</span>
      <span style="font-size:11px;font-family:var(--mono);color:var(--text1);
        flex:1;word-break:break-all;">${val}${extra ? `<span style="margin-left:6px;">${extra}</span>` : ''}</span>
    </div>`;

        const section = (heading, html) =>
            `<div style="margin-bottom:14px;">
      <div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:1px;
        color:var(--text3);font-family:var(--mono);margin-bottom:6px;padding-bottom:4px;
        border-bottom:1px solid rgba(255,255,255,0.06);">${heading}</div>
      ${html}
    </div>`;

        const noData = msg =>
            `<div style="font-size:11px;color:var(--text3);padding:4px 0;">${msg}</div>`;

        const codeSnippet = text =>
            `<pre style="margin:0;padding:8px 10px;background:var(--bg0);border:1px solid var(--border);
      border-radius:4px;font-size:10px;font-family:var(--mono);color:var(--text1);
      line-height:1.6;overflow-x:auto;white-space:pre-wrap;word-break:break-all;
      max-height:180px;overflow-y:auto;">${_esc(text)}</pre>`;

        // ── SQL parsers ────────────────────────────────────────────────────

        function parseColumns(ddl) {
            const cols = [];
            const bodyMatch = ddl.match(/CREATE\s+(?:TEMPORARY\s+)?TABLE[^(]*\(([\s\S]+?)(?:\)\s*(?:WITH|COMMENT|;|$))/i);
            if (!bodyMatch) return cols;
            const body = bodyMatch[1];
            const segments = [];
            let depth = 0, cur = '';
            for (const ch of body) {
                if (ch === '(') { depth++; cur += ch; }
                else if (ch === ')') { depth--; cur += ch; }
                else if (ch === ',' && depth === 0) { segments.push(cur.trim()); cur = ''; }
                else cur += ch;
            }
            if (cur.trim()) segments.push(cur.trim());
            const skipKw = /^(PRIMARY\s+KEY|UNIQUE\s+KEY|KEY\s+|INDEX\s+|CONSTRAINT\s+|CHECK\s*\(|WATERMARK\s+FOR|PERIOD\s+FOR)/i;
            segments.forEach(seg => {
                if (!seg || skipKw.test(seg)) return;
                const m = seg.match(/^[`"]?(\w+)[`"]?\s+((?:ARRAY\s*<[^>]+>|MAP\s*<[^>]+>|ROW\s*\([^)]+\)|MULTISET\s*<[^>]+>|\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?))(.*)$/i);
                if (!m) return;
                const name = m[1];
                const type = m[2].trim().toUpperCase();
                const rest = (m[3] || '').trim();
                const extras = [];
                if (/NOT\s+NULL/i.test(rest))    extras.push('NOT NULL');
                if (/PRIMARY\s+KEY/i.test(rest)) extras.push('PK');
                if (/METADATA/i.test(rest))      extras.push('METADATA');
                if (/VIRTUAL/i.test(rest))       extras.push('VIRTUAL');
                const commentM = rest.match(/COMMENT\s+'([^']{0,60})'/i);
                if (commentM) extras.push(`"${commentM[1]}"`);
                cols.push({ name, type, extras });
            });
            return cols;
        }

        function parseWatermark(ddl) {
            const m = ddl.match(/WATERMARK\s+FOR\s+(\w+)\s+AS\s+([^\n,)]+)/i);
            return m ? { col: m[1], expr: m[2].trim() } : null;
        }

        function parseWith(ddl) {
            const props = {};
            const re = /'([^']+)'\s*=\s*'([^']*)'/g;
            let m;
            while ((m = re.exec(ddl)) !== null) props[m[1].toLowerCase()] = m[2];
            return props;
        }

        function parseUdfDdl(ddl) {
            const sigM = ddl.match(/FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+?)\s*\(([^)]*)\)\s*(?:RETURNS\s+(\S+))?/i);
            if (!sigM) return null;
            const name    = sigM[1].replace(/[`'"]/g,'');
            const rawArgs = sigM[2].trim();
            const ret     = sigM[3] ? sigM[3].toUpperCase() : null;
            const args = [];
            if (rawArgs) {
                rawArgs.split(',').forEach(a => {
                    const p = a.trim().match(/^(\w+)\s+(.+)$/);
                    if (p) args.push({ name: p[1], type: p[2].trim().toUpperCase() });
                    else if (a.trim()) args.push({ name: '—', type: a.trim().toUpperCase() });
                });
            }
            const langM  = ddl.match(/LANGUAGE\s+(\w+)/i);
            const classM = ddl.match(/AS\s+'([^']+)'/i);
            const jarM   = ddl.match(/USING\s+JAR\s+'([^']+)'/i);
            return { name, args, returns: ret, language: langM?.[1]?.toUpperCase(), className: classM?.[1], jar: jarM?.[1] };
        }

        function parseGroupBy(sqlStr) {
            const m = sqlStr.match(/GROUP\s+BY\s+([\s\S]+?)(?:HAVING|ORDER|LIMIT|;|$)/i);
            if (!m) return [];
            return m[1].split(',').map(s => s.trim()).filter(Boolean);
        }

        function parseAggFunctions(colStr) {
            const re = /\b(COUNT|SUM|AVG|MIN|MAX|FIRST_VALUE|LAST_VALUE|LISTAGG|STDDEV|VARIANCE)\s*\(([^)]*)\)(?:\s+AS\s+(\w+))?/gi;
            const fns = [];
            let m;
            while ((m = re.exec(colStr)) !== null)
                fns.push({ fn: m[1].toUpperCase(), arg: m[2].trim().slice(0,60), alias: m[3] || null });
            return fns;
        }

        // ── Build body per node type ────────────────────────────────────────

        let bodyHtml = '';

        // ─── SOURCE or SINK table ────────────────────────────────────────
        if (type === 'source' || type === 'sink') {

            // ── Results / Input virtual nodes ─────────────────────────────
            if (title === 'Results') {
                const cntEl = document.querySelector('#result-row-count,[id*="row-count"],[id*="results-count"]');
                const rowCount = cntEl ? cntEl.textContent.trim() : null;
                const tbl = document.querySelector('#results-table tbody,.results-table tbody,[id*="results"] tbody');
                const previewRows = [];
                if (tbl) {
                    tbl.querySelectorAll('tr').forEach((tr, i) => {
                        if (i >= 5) return;
                        const cells = Array.from(tr.querySelectorAll('td')).map(td => td.textContent.trim());
                        if (cells.length) previewRows.push(cells);
                    });
                }
                if (rowCount) bodyHtml += section('Status',
                    kv('rows received', `<span style="color:${c.border};font-weight:700;">${_esc(rowCount)}</span>`)
                );
                if (previewRows.length) {
                    const rowsHtml = previewRows.map(cells =>
                        `<div style="display:flex;gap:6px;padding:3px 8px;border-bottom:1px solid rgba(255,255,255,0.04);
              font-family:var(--mono);font-size:10px;flex-wrap:wrap;">
              ${cells.map(v => `<span style="color:${c.text};white-space:nowrap;overflow:hidden;
                text-overflow:ellipsis;max-width:110px;" title="${_esc(v)}">${_esc(v.slice(0,40))}</span>`
                        ).join('<span style="color:var(--border);margin:0 2px;">│</span>')}
            </div>`
                    ).join('');
                    bodyHtml += section(`Live preview (${previewRows.length} row${previewRows.length!==1?'s':''})`, rowsHtml);
                } else {
                    bodyHtml += section('Results sink',
                        `<div style="font-size:11px;color:var(--text2);line-height:1.8;">
              Rows stream into the <strong style="color:var(--text1);">Results</strong> tab as your query executes.
              Run a <code style="font-size:10px;color:var(--accent);">SELECT</code> query to see live data.
            </div>`);
                }
            }

            else if (title === 'Input') {
                bodyHtml += section('Input',
                    `<div style="font-size:11px;color:var(--text2);line-height:1.8;">
            Scalar input — no table source. Values come directly from literal expressions or function arguments.
          </div>`);
            }

            // ── Named table — DDL not in parsed SQL; try live lookup ──────
            else if (!sql) {
                // Do a fresh search right now across all open tabs + storage
                const liveDdl = _svpFindDdl(title);
                if (liveDdl) {
                    // Found DDL in another tab — parse and display it
                    const props = parseWith(liveDdl);
                    const cols  = parseColumns(liveDdl);
                    const wm    = parseWatermark(liveDdl);
                    const connector = props['connector'] || props['type'] || null;

                    if (connector || Object.keys(props).length) {
                        let connHtml = '';
                        if (connector) {
                            const connColor = {
                                kafka:'#4fa3e0', 'upsert-kafka':'#60a5fa', datagen:'#34d399',
                                jdbc:'#a78bfa', elasticsearch:'#e879f9', blackhole:'#6b7280',
                                print:'#6b7280', filesystem:'#fb923c', redis:'#f5c518',
                                pulsar:'#60a5fa', kinesis:'#00d4aa', mongodb:'#47a248'
                            }[connector.toLowerCase()] || c.border;
                            connHtml += kv('connector', `<span style="color:${connColor};font-weight:700;">${_esc(connector.toUpperCase())}</span>`);
                        }
                        if (props['topic'])                              connHtml += kv('topic',       `<span style="color:var(--accent);">${_esc(props['topic'])}</span>`);
                        if (props['properties.bootstrap.servers'])       connHtml += kv('bootstrap',   _esc(props['properties.bootstrap.servers'].slice(0,60)));
                        if (props['properties.group.id'])                connHtml += kv('group.id',    _esc(props['properties.group.id']));
                        if (props['scan.startup.mode'])                  connHtml += kv('startup',     _esc(props['scan.startup.mode']));
                        if (props['rows-per-second'])                    connHtml += kv('rows/s',      `<span style="color:var(--green);">${_esc(props['rows-per-second'])}</span>`);
                        if (props['number-of-rows'])                     connHtml += kv('# rows',      _esc(props['number-of-rows']));
                        if (props['format'])                             connHtml += kv('format',      _esc(props['format']));
                        if (props['json.fail-on-missing-field'])         connHtml += kv('fail-missing',_esc(props['json.fail-on-missing-field']));
                        if (props['json.ignore-parse-errors'])           connHtml += kv('ignore-errs', _esc(props['json.ignore-parse-errors']));
                        if (props['avro-confluent.schema-registry.url']) connHtml += kv('schema-reg',  _esc(props['avro-confluent.schema-registry.url'].slice(0,50)));
                        if (props['avro-confluent.schema-id'])           connHtml += kv('schema-id',   _esc(props['avro-confluent.schema-id']));
                        if (props['url'])                                connHtml += kv('url',         _esc(props['url'].slice(0,70)));
                        if (props['table-name'])                         connHtml += kv('table-name',  _esc(props['table-name']));
                        if (props['index'])                              connHtml += kv('index',       _esc(props['index']));
                        if (props['username'])                           connHtml += kv('username',    _esc(props['username']));
                        if (props['path'] || props['file.path'])         connHtml += kv('path',        _esc(props['path'] || props['file.path']));
                        if (props['sink.rolling-policy.file-size'])      connHtml += kv('roll-size',   _esc(props['sink.rolling-policy.file-size']));
                        if (props['sink.parallelism'])                   connHtml += kv('parallelism', _esc(props['sink.parallelism']));
                        if (props['sink.buffer-flush.max-rows'])         connHtml += kv('flush-rows',  _esc(props['sink.buffer-flush.max-rows']));
                        bodyHtml += section('Connector', connHtml || noData('No WITH properties found.'));
                    }

                    if (cols.length) {
                        const colRows = cols.map(col => {
                            const extraPills = col.extras.map(e => {
                                const color = e==='PK'?'#f5c518':e==='NOT NULL'?'#e879f9':e==='METADATA'?'#60a5fa':e==='VIRTUAL'?'#34d399':'#9ca3af';
                                return pill(e, color);
                            }).join(' ');
                            return `<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;
                border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
                <span style="font-size:11px;color:${c.text};flex:1;white-space:nowrap;overflow:hidden;
                  text-overflow:ellipsis;">${_esc(col.name)}</span>
                <span style="font-size:10px;color:${c.border};flex-shrink:0;">${_esc(col.type)}</span>
                ${extraPills ? `<span style="display:flex;gap:4px;flex-shrink:0;">${extraPills}</span>` : ''}
              </div>`;
                        }).join('');
                        const header = `<div style="display:flex;gap:8px;padding:3px 8px 5px;
              border-bottom:1px solid rgba(255,255,255,0.08);font-family:var(--mono);">
              <span style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);flex:1;">Column</span>
              <span style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);">Type</span>
            </div>`;
                        bodyHtml += section(`Schema (${cols.length} column${cols.length!==1?'s':''})`, header + colRows);
                    } else {
                        bodyHtml += section('DDL', codeSnippet(liveDdl.slice(0,400)));
                    }

                    if (wm) {
                        bodyHtml += section('Watermark',
                            kv('event-time col', `<span style="color:${c.text};">${_esc(wm.col)}</span>`) +
                            kv('strategy', _esc(wm.expr))
                        );
                    }

                    // Note that DDL came from another tab
                    bodyHtml += `<div style="margin-top:8px;font-size:9px;color:var(--text3);
            font-style:italic;border-top:1px solid rgba(255,255,255,0.05);padding-top:6px;">
            DDL resolved from another open tab
          </div>`;

                } else {
                    // Genuinely not found anywhere
                    const dir = type === 'source' ? 'Reading from' : 'Writing to';
                    const dirIcon = type === 'source' ? '↪' : '↩';
                    bodyHtml += `
            <div style="display:flex;align-items:center;gap:10px;padding:10px 0 14px;">
              <span style="font-size:22px;opacity:0.6;">${dirIcon}</span>
              <div>
                <div style="font-size:13px;font-weight:700;color:var(--text0);font-family:var(--mono);">
                  ${_esc(title)}
                </div>
                <div style="font-size:10px;color:var(--text3);margin-top:2px;">${dir}</div>
              </div>
            </div>
            <div style="padding:10px 12px;background:var(--bg0);border:1px solid var(--border);
              border-radius:6px;font-size:11px;color:var(--text3);line-height:1.8;">
              No <code style="font-size:10px;color:var(--text2);">CREATE TABLE</code> definition found.
              Open the tab containing this table's DDL alongside this one,
              or add the DDL to this script.
            </div>`;
                }
            }

            // ── Named table — DDL found ─────────────────────────────────
            else {
                const props = parseWith(sql);
                const cols  = parseColumns(sql);
                const wm    = parseWatermark(sql);
                const connector = props['connector'] || props['type'] || null;

                if (connector || Object.keys(props).length) {
                    let connHtml = '';
                    if (connector) {
                        const connColor = {
                            kafka:'#4fa3e0', 'upsert-kafka':'#60a5fa', datagen:'#34d399',
                            jdbc:'#a78bfa', elasticsearch:'#e879f9', blackhole:'#6b7280',
                            print:'#6b7280', filesystem:'#fb923c', redis:'#f5c518',
                            pulsar:'#60a5fa', kinesis:'#00d4aa', mongodb:'#47a248'
                        }[connector.toLowerCase()] || c.border;
                        connHtml += kv('connector', `<span style="color:${connColor};font-weight:700;">${_esc(connector.toUpperCase())}</span>`);
                    }
                    if (props['topic'])                              connHtml += kv('topic',       `<span style="color:var(--accent);">${_esc(props['topic'])}</span>`);
                    if (props['properties.bootstrap.servers'])       connHtml += kv('bootstrap',   _esc(props['properties.bootstrap.servers'].slice(0,60)));
                    if (props['properties.group.id'])                connHtml += kv('group.id',    _esc(props['properties.group.id']));
                    if (props['scan.startup.mode'])                  connHtml += kv('startup',     _esc(props['scan.startup.mode']));
                    if (props['rows-per-second'])                    connHtml += kv('rows/s',      `<span style="color:var(--green);">${_esc(props['rows-per-second'])}</span>`);
                    if (props['number-of-rows'])                     connHtml += kv('# rows',      _esc(props['number-of-rows']));
                    if (props['format'])                             connHtml += kv('format',      _esc(props['format']));
                    if (props['json.fail-on-missing-field'])         connHtml += kv('fail-missing',_esc(props['json.fail-on-missing-field']));
                    if (props['json.ignore-parse-errors'])           connHtml += kv('ignore-errs', _esc(props['json.ignore-parse-errors']));
                    if (props['avro-confluent.schema-registry.url']) connHtml += kv('schema-reg',  _esc(props['avro-confluent.schema-registry.url'].slice(0,50)));
                    if (props['avro-confluent.schema-id'])           connHtml += kv('schema-id',   _esc(props['avro-confluent.schema-id']));
                    if (props['url'])                                connHtml += kv('url',         _esc(props['url'].slice(0,70)));
                    if (props['table-name'])                         connHtml += kv('table-name',  _esc(props['table-name']));
                    if (props['index'])                              connHtml += kv('index',       _esc(props['index']));
                    if (props['username'])                           connHtml += kv('username',    _esc(props['username']));
                    if (props['path'] || props['file.path'])         connHtml += kv('path',        _esc(props['path'] || props['file.path']));
                    if (props['sink.rolling-policy.file-size'])      connHtml += kv('roll-size',   _esc(props['sink.rolling-policy.file-size']));
                    if (props['sink.parallelism'])                   connHtml += kv('parallelism', _esc(props['sink.parallelism']));
                    if (props['sink.buffer-flush.max-rows'])         connHtml += kv('flush-rows',  _esc(props['sink.buffer-flush.max-rows']));
                    bodyHtml += section('Connector', connHtml || noData('No WITH properties found.'));
                }

                if (cols.length) {
                    const colRows = cols.map(col => {
                        const extraPills = col.extras.map(e => {
                            const color = e === 'PK' ? '#f5c518' : e === 'NOT NULL' ? '#e879f9'
                                : e === 'METADATA' ? '#60a5fa' : e === 'VIRTUAL' ? '#34d399' : '#9ca3af';
                            return pill(e, color);
                        }).join(' ');
                        return `<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;
              border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
              <span style="font-size:11px;color:${c.text};flex:1;white-space:nowrap;overflow:hidden;
                text-overflow:ellipsis;">${_esc(col.name)}</span>
              <span style="font-size:10px;color:${c.border};flex-shrink:0;">${_esc(col.type)}</span>
              ${extraPills ? `<span style="display:flex;gap:4px;flex-shrink:0;">${extraPills}</span>` : ''}
            </div>`;
                    }).join('');
                    const header = `<div style="display:flex;gap:8px;padding:3px 8px 5px;
            border-bottom:1px solid rgba(255,255,255,0.08);font-family:var(--mono);">
            <span style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);flex:1;">Column</span>
            <span style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);">Type</span>
          </div>`;
                    bodyHtml += section(`Schema (${cols.length} column${cols.length!==1?'s':''})`, header + colRows);
                } else if (sql) {
                    bodyHtml += section('DDL snippet', codeSnippet(sql.slice(0, 400)));
                }

                if (wm) {
                    bodyHtml += section('Watermark',
                        kv('event-time col', `<span style="color:${c.text};">${_esc(wm.col)}</span>`) +
                        kv('strategy', _esc(wm.expr))
                    );
                }

                const fieldKinds = {};
                Object.entries(props).forEach(([k,v]) => {
                    const m = k.match(/^fields\.([^.]+)\.(kind|length|max|min|var-len)$/i);
                    if (m) { fieldKinds[m[1]] = fieldKinds[m[1]] || {}; fieldKinds[m[1]][m[2]] = v; }
                });
                if (Object.keys(fieldKinds).length) {
                    const rows = Object.entries(fieldKinds).map(([field, cfg]) =>
                        `<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;
              border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
              <span style="font-size:11px;color:${c.text};flex:1;">${_esc(field)}</span>
              <span style="font-size:10px;color:${c.border};">${_esc(cfg.kind || '—')}</span>
              ${cfg.min !== undefined ? `<span style="font-size:9px;color:var(--text3);">min:${cfg.min}</span>` : ''}
              ${cfg.max !== undefined ? `<span style="font-size:9px;color:var(--text3);">max:${cfg.max}</span>` : ''}
              ${cfg.length !== undefined ? `<span style="font-size:9px;color:var(--text3);">len:${cfg.length}</span>` : ''}
            </div>`
                    ).join('');
                    bodyHtml += section('Datagen field config', rows);
                }
            }
        }

        // ─── UDF / FUNCTION ──────────────────────────────────────────────
        else if (type === 'udf') {
            const fnName = sub || title.replace(/^UDF\n?/i, '').trim();
            const parsed = sql ? parseUdfDdl(sql) : null;

            if (parsed) {
                let infoHtml = kv('function', `<span style="color:${c.border};font-weight:700;">${_esc(parsed.name)}</span>`);
                if (parsed.language)  infoHtml += kv('language', pill(parsed.language, c.border));
                if (parsed.className) infoHtml += kv('class',    _esc(parsed.className));
                if (parsed.jar)       infoHtml += kv('jar',      _esc(parsed.jar.split('/').pop()));
                if (parsed.returns)   infoHtml += kv('returns',  `<span style="color:${c.text};">${_esc(parsed.returns)}</span>`);
                bodyHtml += section('Function info', infoHtml);
                if (parsed.args.length) {
                    const argRows = parsed.args.map((a, i) =>
                        `<div style="display:flex;align-items:center;gap:10px;padding:5px 8px;
              border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
              <span style="font-size:9px;color:var(--text3);width:20px;flex-shrink:0;">${i+1}</span>
              <span style="font-size:11px;color:${c.text};flex:1;">${_esc(a.name)}</span>
              <span style="font-size:10px;color:${c.border};">${_esc(a.type)}</span>
            </div>`
                    ).join('');
                    bodyHtml += section(`Parameters (${parsed.args.length})`, argRows);
                } else {
                    bodyHtml += section('Parameters', noData('No parameters — scalar function with no args.'));
                }
                if (sql) bodyHtml += section('DDL', codeSnippet(sql.slice(0,500)));
            } else {
                bodyHtml += section('Function', kv('name', `<span style="color:${c.border};font-weight:700;">${_esc(fnName)}</span>`));
                const editorSql = (document.getElementById('sql-editor') || {}).value || '';
                const callRe = new RegExp(fnName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*\\(([^)]{0,200})\\)', 'gi');
                const calls = [];
                let cm;
                while ((cm = callRe.exec(editorSql)) !== null) calls.push(cm[1].trim());
                if (calls.length) {
                    const parseArgs = str => {
                        const args = []; let depth = 0, cur = '';
                        for (const ch of str) {
                            if (ch === '(') { depth++; cur += ch; }
                            else if (ch === ')') { depth--; cur += ch; }
                            else if (ch === ',' && depth === 0) { args.push(cur.trim()); cur = ''; }
                            else cur += ch;
                        }
                        if (cur.trim()) args.push(cur.trim());
                        return args;
                    };
                    const inferType = expr => {
                        const u = expr.toUpperCase();
                        const castM = u.match(/CAST\s*\([^)]+AS\s+(\w+(?:\(\d+\))?)/);
                        if (castM) return castM[1];
                        if (/^\d+\.\d+$/.test(expr)) return 'DOUBLE';
                        if (/^\d+$/.test(expr))       return 'BIGINT';
                        if (/^'[^']*'$/.test(expr))   return 'STRING';
                        if (/^true$|^false$/i.test(expr)) return 'BOOLEAN';
                        if (/\bINTERVAL\b/i.test(u))  return 'INTERVAL';
                        if (/\bCURRENT_TIMESTAMP\b/i.test(u)) return 'TIMESTAMP';
                        return 'expression';
                    };
                    const args = parseArgs(calls[0]);
                    const argRows = args.map((a, i) =>
                        `<div style="display:flex;align-items:center;gap:10px;padding:5px 8px;
              border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
              <span style="font-size:9px;color:var(--text3);width:20px;flex-shrink:0;">${i+1}</span>
              <span style="font-size:11px;color:${c.text};flex:1;word-break:break-all;">${_esc(a.slice(0,80))}</span>
              <span style="font-size:10px;color:${c.border};">${_esc(inferType(a))}</span>
            </div>`
                    ).join('');
                    bodyHtml += section(`Inferred call-site args (${args.length})`, argRows);
                    bodyHtml += section('Note', noData('CREATE FUNCTION not found in current script — args inferred from usage.'));
                } else {
                    bodyHtml += section('Note', noData('Function referenced in SELECT columns but no CREATE FUNCTION or call-site found in this script.'));
                }
            }
        }

        // ─── CTE / VIEW ──────────────────────────────────────────────────
        else if (type === 'cte') {
            const isView = /CREATE\s+(?:TEMPORARY\s+)?VIEW/i.test(sql);
            if (isView) {
                bodyHtml += section('View',
                    kv('name', `<span style="color:${c.border};">${_esc(title)}</span>`) +
                    (sql.match(/TEMPORARY/i) ? kv('scope', pill('TEMPORARY', '#9ca3af')) : '')
                );
                const cols = parseColumns(sql);
                if (cols.length) {
                    const colRows = cols.map(col =>
                        `<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;
              border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
              <span style="font-size:11px;color:${c.text};flex:1;">${_esc(col.name)}</span>
              <span style="font-size:10px;color:${c.border};">${_esc(col.type)}</span>
            </div>`
                    ).join('');
                    bodyHtml += section(`Schema (${cols.length} columns)`, colRows);
                }
                const selM = sql.match(/AS\s+(SELECT[\s\S]{0,400})/i);
                if (selM) bodyHtml += section('Underlying SELECT', codeSnippet(selM[1].trim().slice(0,400)));
                else if (sql) bodyHtml += section('DDL', codeSnippet(sql.slice(0,400)));
            } else {
                bodyHtml += section('CTE', kv('name', `<span style="color:${c.border};">${_esc(title)}</span>`));
                if (sql) bodyHtml += section('Definition', codeSnippet(sql.slice(0,400)));
            }
        }

        // ─── WINDOW ──────────────────────────────────────────────────────
        else if (type === 'window') {
            const winType = title.split('\n')[0];
            const edSql   = (document.getElementById('sql-editor') || {}).value || '';
            const winRe   = new RegExp(`(${winType.toUpperCase()})\\s*\\(([^)]{0,300})\\)`, 'i');
            const winCall = winRe.exec(edSql);
            let winHtml = kv('function', `<span style="color:${c.border};font-weight:700;">${_esc(winType.toUpperCase())}()</span>`);
            if (winCall) {
                const args = winCall[2].split(',').map(s => s.trim());
                if (args[0]) winHtml += kv('table',    _esc(args[0]));
                if (args[1]) winHtml += kv('time col', _esc(args[1]));
                if (winType.toUpperCase().startsWith('HOP') && args[2] && args[3]) {
                    winHtml += kv('slide', _esc(args[2]));
                    winHtml += kv('size',  _esc(args[3]));
                } else if (args[2]) {
                    winHtml += kv('size', _esc(args[2]));
                }
                if (winType.toUpperCase().startsWith('CUMULATE') && args[3]) winHtml += kv('max size', _esc(args[3]));
            } else {
                const intervalM = (nd.label + edSql).match(/INTERVAL\s+'([^']+)'\s*(MINUTE|SECOND|HOUR|DAY|MONTH)?/i);
                if (intervalM) winHtml += kv('size', `${_esc(intervalM[1])} ${_esc(intervalM[2] || '')}`);
            }
            const partM = edSql.match(/PARTITION\s+BY\s+([^\n)]+)/i);
            if (partM) winHtml += kv('partition by', _esc(partM[1].trim().slice(0,80)));
            bodyHtml += section('Window function', winHtml);
        }

        // ─── JOIN ────────────────────────────────────────────────────────
        else if (type === 'join') {
            const joinTypeParts = title.split('\n');
            const joinKind  = joinTypeParts[0];
            const joinTable = sub || joinTypeParts[1] || '—';
            const edSql     = (document.getElementById('sql-editor') || {}).value || '';
            let joinHtml =
                kv('join type',    `<span style="color:${c.border};font-weight:700;">${_esc(joinKind)}</span>`) +
                kv('joined table', `<span style="color:${c.text};">${_esc(joinTable)}</span>`);
            const onRe = new RegExp(`JOIN\\s+${joinTable.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}\\s+(?:\\w+\\s+)?ON\\s+([^\\n;]{1,120})`, 'i');
            const onM  = onRe.exec(edSql) || edSql.match(/ON\s+([^\n;]{1,120})/i);
            if (onM) joinHtml += kv('ON', _esc(onM[1].trim().slice(0,100)));
            const intM = edSql.match(/INTERVAL\s+'([^']+)'\s*(MINUTE|SECOND|HOUR|DAY)?/i);
            if (intM)  joinHtml += kv('interval', `${_esc(intM[1])} ${_esc(intM[2]||'')}`);
            if (/FOR\s+SYSTEM_TIME\s+AS\s+OF/i.test(edSql))
                joinHtml += kv('join style', pill('TEMPORAL', c.border));
            else if (intM)
                joinHtml += kv('join style', pill('INTERVAL', c.border));
            else
                joinHtml += kv('join style', pill('REGULAR', '#9ca3af'));
            bodyHtml += section('Join details', joinHtml);
        }

        // ─── AGG / GROUP BY ──────────────────────────────────────────────
        else if (type === 'agg') {
            const edSql  = (document.getElementById('sql-editor') || {}).value || '';
            const selM   = edSql.match(/SELECT\s+([\s\S]+?)\s+FROM/i);
            const selCols= selM ? selM[1] : '';
            const aggFns = parseAggFunctions(selCols || edSql);
            const gbCols = parseGroupBy(edSql);
            if (/MATCH_RECOGNIZE/i.test(title + edSql)) {
                const patternM = edSql.match(/PATTERN\s*\(([^)]+)\)/i);
                const defineM  = edSql.match(/DEFINE\s+([\s\S]{0,200})(?:MEASURES|PATTERN|;)/i);
                let mrHtml = kv('operator', pill('CEP MATCH_RECOGNIZE', c.border));
                if (patternM) mrHtml += kv('pattern', _esc(patternM[1].trim()));
                if (defineM)  mrHtml += kv('define',  _esc(defineM[1].trim().slice(0,120)));
                const partM = edSql.match(/PARTITION\s+BY\s+([^\n)]+)/i);
                if (partM)  mrHtml += kv('partition', _esc(partM[1].trim().slice(0,80)));
                bodyHtml += section('CEP / Pattern', mrHtml);
            } else {
                if (gbCols.length) {
                    const colPills = gbCols.map(col => pill(col, c.border)).join(' ');
                    bodyHtml += section('Group by columns',
                        `<div style="display:flex;flex-wrap:wrap;gap:6px;padding:4px 0;">${colPills}</div>`);
                }
                if (aggFns.length) {
                    const aggRows = aggFns.map(f =>
                        `<div style="display:flex;align-items:center;gap:8px;padding:5px 8px;
              border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
              <span style="font-size:11px;color:${c.border};font-weight:700;flex-shrink:0;">${_esc(f.fn)}</span>
              <span style="font-size:11px;color:${c.text};flex:1;">(${_esc(f.arg)})</span>
              ${f.alias ? `<span style="font-size:9px;color:var(--text3);">→ ${_esc(f.alias)}</span>` : ''}
            </div>`
                    ).join('');
                    bodyHtml += section(`Aggregate functions (${aggFns.length})`, aggRows);
                }
                if (!gbCols.length && !aggFns.length) {
                    if (edSql) bodyHtml += section('SQL context', codeSnippet(edSql.slice(0,300)));
                    else bodyHtml += section('Aggregation', noData('GROUP BY or aggregate functions detected.'));
                }
                const havingM = edSql.match(/HAVING\s+(.{1,120})/i);
                if (havingM) bodyHtml += section('Having', codeSnippet(havingM[1].trim()));
            }
        }

        // ─── FILTER / WHERE ──────────────────────────────────────────────
        else if (type === 'filter') {
            const edSql  = (document.getElementById('sql-editor') || {}).value || '';
            const whereM = edSql.match(/WHERE\s+([\s\S]{1,400}?)(?:GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|;|$)/i);
            if (whereM) {
                bodyHtml += section('Filter condition', codeSnippet(whereM[1].trim()));
                const cond  = whereM[1];
                const preds = [];
                if (/IS\s+NOT\s+NULL|IS\s+NULL/i.test(cond))       preds.push('null check');
                if (/BETWEEN\s+/i.test(cond))                       preds.push('BETWEEN range');
                if (/LIKE\s+'/i.test(cond))                         preds.push('LIKE pattern');
                if (/REGEXP_/i.test(cond))                          preds.push('regex');
                if (/IN\s*\(/i.test(cond))                          preds.push('IN list');
                if (/>|>=|<|<=/g.test(cond))                        preds.push('comparison');
                if (/=\s*'[^']+'/g.test(cond))                      preds.push('equality');
                if (/\bAND\b/i.test(cond) || /\bOR\b/i.test(cond)) {
                    const ands = (cond.match(/\bAND\b/gi)||[]).length;
                    const ors  = (cond.match(/\bOR\b/gi)||[]).length;
                    preds.push(`compound (${ands} AND / ${ors} OR)`);
                }
                if (preds.length) {
                    bodyHtml += section('Predicate types',
                        `<div style="display:flex;flex-wrap:wrap;gap:6px;padding:4px 0;">${preds.map(p=>pill(p,c.border)).join('')}</div>`);
                }
            } else {
                bodyHtml += section('Filter', noData('WHERE condition could not be extracted.'));
            }
        }

        // ─── PROJECT / MAP ────────────────────────────────────────────────
        else if (type === 'project') {
            const edSql  = (document.getElementById('sql-editor') || {}).value || '';
            const selM   = edSql.match(/SELECT\s+([\s\S]+?)\s+FROM/i);
            const selCols= selM ? selM[1].trim() : '';
            if (selCols) {
                const splitCols = [];
                let depth2 = 0, cur2 = '';
                for (const ch of selCols) {
                    if (ch === '(') { depth2++; cur2 += ch; }
                    else if (ch === ')') { depth2--; cur2 += ch; }
                    else if (ch === ',' && depth2 === 0) { splitCols.push(cur2.trim()); cur2 = ''; }
                    else cur2 += ch;
                }
                if (cur2.trim()) splitCols.push(cur2.trim());
                const colRows = splitCols.slice(0,20).map(col => {
                    const aliasM = col.match(/\bAS\s+(\w+)$/i);
                    const alias  = aliasM ? aliasM[1] : null;
                    const expr   = alias ? col.slice(0, col.lastIndexOf(aliasM[0])).trim() : col;
                    return `<div style="display:flex;align-items:center;gap:8px;padding:4px 8px;
            border-bottom:1px solid rgba(255,255,255,0.04);font-family:var(--mono);">
            <span style="font-size:11px;color:${c.text};flex:1;word-break:break-all;
              white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
              title="${_esc(expr)}">${_esc(expr.slice(0,70))}</span>
            ${alias ? `<span style="font-size:9px;color:var(--text3);flex-shrink:0;">→ ${_esc(alias)}</span>` : ''}
          </div>`;
                }).join('');
                bodyHtml += section(`Projected columns (${Math.min(splitCols.length,20)}${splitCols.length>20?' of '+splitCols.length:''})`, colRows);
            } else {
                bodyHtml += section('Projection', noData('No SELECT columns found.'));
            }
        }

        // ─── DDL ─────────────────────────────────────────────────────────
        else if (type === 'ddl') {
            const op   = title.split('\n')[0];
            const name = title.split('\n')[1] || sub;
            let ddlHtml = kv('operation', `<span style="color:${c.border};font-weight:700;">${_esc(op)}</span>`);
            if (name) ddlHtml += kv('object', `<span style="color:${c.text};">${_esc(name)}</span>`);
            if (/IF\s+NOT\s+EXISTS/i.test(sql)) ddlHtml += kv('guard', pill('IF NOT EXISTS', '#34d399'));
            if (/IF\s+EXISTS/i.test(sql))       ddlHtml += kv('guard', pill('IF EXISTS', '#fb923c'));
            if (/TEMPORARY/i.test(sql))         ddlHtml += kv('scope', pill('TEMPORARY', '#9ca3af'));
            if (sql) ddlHtml += `<div style="margin-top:8px;">${codeSnippet(sql.slice(0,300))}</div>`;
            bodyHtml += section('DDL statement', ddlHtml);
        }

        // ─── SET ─────────────────────────────────────────────────────────
        else if (type === 'set') {
            const m = sql.match(/SET\s+'([^']+)'\s*=\s*'([^']*)'/i);
            if (m) {
                bodyHtml += section('Session config',
                    kv('key',   _esc(m[1])) +
                    kv('value', `<span style="color:${c.border};">${_esc(m[2])}</span>`)
                );
            } else {
                bodyHtml += section('SET', kv('statement', _esc(sql.slice(0,100))));
            }
        }

        // ─── UTILITY ─────────────────────────────────────────────────────
        else if (type === 'util') {
            const opM = sql.match(/^(SHOW\s+\w+|DESCRIBE\s+\S+|EXPLAIN\s+)/i);
            const op  = opM ? opM[0].trim().toUpperCase() : title;
            bodyHtml += section('Utility statement', kv('operation', pill(op, c.border)));
            if (sql && sql.length > op.length + 2)
                bodyHtml += section('Full statement', codeSnippet(sql.slice(0,300)));
        }

        // ─── Fallback ─────────────────────────────────────────────────────
        else {
            if (sql) bodyHtml += section('SQL', codeSnippet(sql.slice(0,400)));
            else     bodyHtml  = noData('No details available for this node.');
        }

        // ── Assemble panel ──────────────────────────────────────────────────

        const icon = _icon(type);

        const panel = document.createElement('div');
        panel.id = 'svp-node-detail';
        panel.style.cssText = `
      position:fixed; z-index:9999;
      top:50%; left:50%; transform:translate(-50%,-50%);
      width:min(500px,94vw);
      background:var(--bg1,#0d1117);
      border:1.5px solid ${c.border};
      border-radius:10px;
      box-shadow:0 24px 64px rgba(0,0,0,0.75),0 0 0 1px rgba(0,0,0,0.3);
      font-family:var(--sans,sans-serif);
      overflow:hidden;
      animation:svp-node-in 0.14s ease;
    `;

        panel.innerHTML = `
      <style>
        @keyframes svp-node-in{
          from{opacity:0;transform:translate(-50%,-46%)}
          to  {opacity:1;transform:translate(-50%,-50%)}
        }
      </style>
      <div style="display:flex;align-items:center;gap:10px;padding:12px 14px;
        background:rgba(0,0,0,0.3);border-bottom:1px solid rgba(255,255,255,0.06);">
        <span style="font-size:16px;line-height:1;flex-shrink:0;">${icon}</span>
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:700;color:var(--text0);
            overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${_esc(title)}</div>
          ${sub ? `<div style="font-size:10px;color:${c.border};font-family:var(--mono);margin-top:1px;">${_esc(sub)}</div>` : ''}
        </div>
        <span style="font-size:9px;font-family:var(--mono);padding:2px 7px;border-radius:10px;
          background:${c.bg};border:1px solid ${c.border};color:${c.text};
          text-transform:uppercase;letter-spacing:.5px;flex-shrink:0;">${type}</span>
        <button onclick="document.getElementById('svp-node-detail').remove()"
          style="background:none;border:none;color:var(--text3);font-size:20px;
          cursor:pointer;padding:0 2px;line-height:1;flex-shrink:0;margin-left:4px;">×</button>
      </div>
      <div style="padding:14px;max-height:65vh;overflow-y:auto;">
        ${bodyHtml}
      </div>
      <div style="padding:6px 14px;border-top:1px solid rgba(255,255,255,0.05);
        background:rgba(0,0,0,0.2);font-size:9px;color:var(--text3);font-family:var(--mono);">
        Double-click a node to inspect &nbsp;·&nbsp; Double-click canvas background to reset zoom
      </div>`;

        document.body.appendChild(panel);

        setTimeout(() => {
            const handler = e => {
                if (!panel.contains(e.target)) {
                    panel.remove();
                    document.removeEventListener('mousedown', handler);
                }
            };
            document.addEventListener('mousedown', handler);
        }, 80);
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
        // Collapsed tab — clicking it restores the panel
        tab.addEventListener('click', () => {
            // Move tab back into svp-editor-row before setState reattaches it
            const rowEl = document.getElementById('svp-editor-row');
            if (rowEl && tab.parentElement !== rowEl) rowEl.appendChild(tab);
            _setState('default');
        });

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
        const panel      = document.getElementById(PANEL_ID);
        const tab        = document.getElementById(TAB_ID);
        const btn        = document.getElementById('toggle-btn-svp') || document.getElementById(TOGGLE_ID);
        const expBtn     = document.getElementById('svp-expand-btn');
        const rowEl      = document.getElementById('svp-editor-row');
        const editorArea = document.getElementById('editor-area');
        if (!panel) return;

        panel.classList.remove('svp-collapsed', 'svp-expanded');
        if (tab) tab.classList.remove('svp-tab-vis');

        if (state === 'collapsed') {
            panel.classList.add('svp-collapsed');
            panel.style.width = '0';
            // Reparent tab onto #editor-area — places it outside the flex row
            // so it is pinned to the right edge of the full editor column
            if (tab && editorArea) {
                editorArea.style.position = 'relative';
                if (tab.parentElement !== editorArea) editorArea.appendChild(tab);
                tab.style.cssText = ''; // reset inline styles; let CSS take over
            }
            if (tab) tab.classList.add('svp-tab-vis');

        } else if (state === 'expanded') {
            panel.classList.add('svp-expanded');
            panel.style.width = '';
            if (expBtn) expBtn.textContent = '⊟';
            if (tab && rowEl && tab.parentElement !== rowEl) rowEl.appendChild(tab);

        } else {
            // default side panel
            panel.style.width = _s.panelWidth + 'px';
            if (expBtn) expBtn.textContent = '⊞';
            if (tab && rowEl && tab.parentElement !== rowEl) rowEl.appendChild(tab);
        }

        const toggleBtn = document.getElementById(TOGGLE_ID);
        if (toggleBtn) toggleBtn.innerHTML = _toggleIcon();

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

      /* ── Panel: normal side panel state ─────────────────────── */
      #svp-panel {
        position: relative;
        display: flex;
        flex-direction: column;
        background: var(--bg1,#0c1219);
        border-left: 1px solid var(--border,#1a2a3a);
        flex-shrink: 0;
        overflow: hidden;
        transition: width 0.22s ease;
        z-index: 5;
        min-width: 0;
      }

      /* ── Collapsed: zero-width, removed from layout flow ────── */
      #svp-panel.svp-collapsed {
        width: 0 !important;
        min-width: 0 !important;
        border-left: none !important;
        overflow: hidden !important;
        flex: 0 0 0px !important;
        opacity: 0;
        pointer-events: none;
        transition: width 0.22s ease, opacity 0.15s ease;
      }

      /* ── Collapsed tab: pinned to right edge of #editor-area ───
         When collapsed the tab is reparented onto #editor-area
         (position:relative) so it sits flush on the right border
         of the entire editor column and is always reachable.      */
      #${TAB_ID} {
        /* default: hidden, inside svp-editor-row (pre-collapse) */
        display: none;
        position: absolute;
        right: 0;
        /* vertically centred inside whatever parent it's in */
        top: 50%;
        transform: translateY(-50%);
        z-index: 60;
        /* visual appearance */
        background: var(--bg2,#0f1924);
        border: 1px solid var(--border2,#2a4a5a);
        border-right: none;
        border-radius: 6px 0 0 6px;
        padding: 18px 5px;
        cursor: pointer;
        flex-direction: column;
        align-items: center;
        gap: 5px;
        color: var(--text3,#4a6a7a);
        font-size: 9px;
        font-family: var(--mono,monospace);
        writing-mode: vertical-rl;
        letter-spacing: 1.2px;
        text-transform: uppercase;
        box-shadow: -3px 0 14px rgba(0,0,0,0.5);
        transition: color 0.15s, background 0.15s, border-color 0.15s;
        white-space: nowrap;
        user-select: none;
      }
      #${TAB_ID}:hover {
        color: var(--accent,#00d4aa);
        background: var(--bg1,#0c1219);
        border-color: rgba(0,212,170,0.4);
        box-shadow: -3px 0 18px rgba(0,212,170,0.15);
      }
      /* .svp-tab-vis is set when collapsed — makes tab visible */
      #${TAB_ID}.svp-tab-vis { display: flex; }

      /* ── Expanded: overlays the full editor-row ─────────────── */
      #svp-panel.svp-expanded {
        position: absolute !important;
        inset: 0 !important;
        width: 100% !important;
        z-index: 18 !important;
        border-left: none !important;
        transition: none !important;
        opacity: 1;
        pointer-events: auto;
      }

      /* ── Structural ─────────────────────────────────────────── */
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

      /* ── Collapse toggle button on left edge of panel ────────── */
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

    // ── Public API
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