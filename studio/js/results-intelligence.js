/* Str:::lab Studio — Results Intelligence v3
 * Fixes: button style toggle, legend visibility when pagination hidden,
 *        clearSmartColoring full reset, column header parsing with type suffix.
 * Button renamed to "✦ Colour Describe"
 */


// ── Brand name patch: intercept window.open to rewrite brand in PDF reports ──
// perf.js generates the PDF by opening a new window and calling .write(html)
// We intercept window.open and document.write on the new window to replace
// any residual "FlinkSQL Studio" brand references before the user prints.
(function() {
    const _origOpen = window.open.bind(window);
    window.open = function(url, name, features) {
        const win = _origOpen(url, name, features);
        if (!win) return win;

        // Wrap document.write to intercept the HTML perf.js writes
        const _origWrite = win.document.write.bind(win.document);
        win.document.write = function(html) {
            if (typeof html === 'string') {
                html = html
                    .replace(/FlinkSQL Studio — Results Export/g, 'Str:::lab Studio — Results Export')
                    .replace(/FlinkSQL Studio/g,  'Str:::lab Studio')
                    .replace(/FlinkSQL/g,         'Str:::lab');
            }
            return _origWrite(html);
        };

        // Also patch writeln just in case
        const _origWriteln = win.document.writeln.bind(win.document);
        win.document.writeln = function(html) {
            if (typeof html === 'string') {
                html = html
                    .replace(/FlinkSQL Studio — Results Export/g, 'Str:::lab Studio — Results Export')
                    .replace(/FlinkSQL Studio/g,  'Str:::lab Studio')
                    .replace(/FlinkSQL/g,         'Str:::lab');
            }
            return _origWriteln(html);
        };

        return win;
    };
})();

window._smartColorEnabled = false;

// ── Column semantic rules ─────────────────────────────────────────────────────
const SEMANTIC_RULES = [
    {
        colMatch: /^(tower_status|status|state|connection_status|alert_status|node_status|job_status|tower_state)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase();
            if (/OFFLINE|DOWN|FAILED|CRITICAL|ERROR|DEAD|DISCONNECTED/.test(v)) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (/CONGESTED|DEGRADED|WARN|ALERT|SLOW|THROTTL|IMPAIRED/.test(v))  return { bg:'#3d2200', text:'#f5a623', bold:false };
            if (/ACTIVE|UP|RUNNING|OK|HEALTHY|ONLINE|NORMAL|AVAILABLE/.test(v)) return { bg:'#0a2a14', text:'#39d353', bold:false };
            if (/MAINTENANCE|PAUSED|SUSPEND|STANDBY|IDLE/.test(v))               return { bg:'#0a1a2e', text:'#4fa3e0', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(fraud|is_fraud|fraud_flag|fraud_score|risk|risk_score|risk_level|anomaly|suspicious|flagged|is_anomaly)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (v==='1'||v==='TRUE'||v==='YES'||v==='FRAUD'||v==='HIGH'||v==='FRAUDULENT')
                return { bg:'#3d0a14', text:'#ff6b7a', bold:true };
            if (v==='0'||v==='FALSE'||v==='NO'||v==='CLEAN'||v==='LOW'||v==='LEGITIMATE')
                return { bg:'#0a2a14', text:'#39d353', bold:false };
            if (v==='MEDIUM'||v==='MODERATE'||v==='REVIEW')
                return { bg:'#3d2200', text:'#f5a623', bold:false };
            const n = parseFloat(val);
            if (!isNaN(n)) {
                if (n>=0.75) return { bg:'#3d0a14', text:'#ff6b7a', bold:true  };
                if (n>=0.4)  return { bg:'#3d2200', text:'#f5a623', bold:false };
                if (n<=0.2)  return { bg:'#0a2a14', text:'#39d353', bold:false };
            }
            return null;
        }
    },
    {
        colMatch: /^(signal_quality|signal|quality|grade|signal_strength|signal_grade|link_quality)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (/POOR|BAD|WEAK|CRITICAL/.test(v))     return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (/FAIR|MODERATE|AVERAGE/.test(v))       return { bg:'#3d2200', text:'#f5a623', bold:false };
            if (/GOOD|EXCELLENT|STRONG|OPTIMAL/.test(v)) return { bg:'#0a2a14', text:'#39d353', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(severity|level|priority|urgency|alert_level|sla_class|incident_level|criticality)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (/CRITICAL|HIGH|EMERGENCY|P0|SEV0|SEV1|BREACH|BLOCKER/.test(v))  return { bg:'#3d0a14', text:'#ff6b7a', bold:true  };
            if (/MEDIUM|MODERATE|WARNING|P1|P2|SEV2|AT_RISK|MAJOR/.test(v))     return { bg:'#3d2200', text:'#f5a623', bold:false };
            if (/LOW|INFO|MINOR|P3|P4|P5|SEV3|MET|NORMAL|TRIVIAL/.test(v))      return { bg:'#0a2a14', text:'#39d353', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(packet_loss|packet_loss_pct|error_rate|loss_pct|drop_rate|loss_percent|err_rate)$/i,
        rowColor: (val) => {
            const n = parseFloat(val);
            if (isNaN(n)) return null;
            if (n>=10) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>= 3) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(latency|latency_ms|response_time|rtt|delay_ms|ping_ms|round_trip)$/i,
        rowColor: (val) => {
            const n = parseFloat(val);
            if (isNaN(n)) return null;
            if (n>=500) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>=150) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(cpu|cpu_pct|cpu_load|cpu_usage|cpu_percent|load_avg)$/i,
        rowColor: (val) => {
            const n = parseFloat(val);
            if (isNaN(n)) return null;
            if (n>=90) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>=70) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(memory|mem_pct|memory_usage|heap_pct|mem_used_pct)$/i,
        rowColor: (val) => {
            const n = parseFloat(val);
            if (isNaN(n)) return null;
            if (n>=90) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>=75) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
];

// ── Extract clean column name from header text (strips " VARCHAR", " DOUBLE" etc) ─
function _cleanColName(headerText) {
    // Headers look like "TOWER_STATUS VARCHAR" or "RSSI_DBM DOUBLE" — take first token
    return String(headerText).trim().split(/\s+/)[0];
}

function _detectColRule(colName) {
    const clean = _cleanColName(colName);
    return SEMANTIC_RULES.find(r => r.colMatch.test(clean)) || null;
}

const _priority = (s) => s ? ({'#ff6b7a':4,'#f5a623':3,'#4fa3e0':2,'#39d353':1}[s.text]||0) : 0;

// ── Toggle ────────────────────────────────────────────────────────────────────
function toggleSmartColoring() {
    window._smartColorEnabled = !window._smartColorEnabled;
    _updateColorBtn();
    if (window._smartColorEnabled) {
        applySmartRowColoring();
    } else {
        clearSmartColoring();
    }
}

function _updateColorBtn() {
    const btn = document.getElementById('smart-color-toggle-btn');
    if (!btn) return;
    if (window._smartColorEnabled) {
        btn.textContent        = '✦ Colour On';
        btn.style.background   = 'var(--accent)';
        btn.style.color        = '#000';
        btn.style.borderColor  = 'var(--accent)';
        btn.style.fontWeight   = '600';
    } else {
        btn.textContent        = '✦ Colour Describe';
        btn.style.background   = '';
        btn.style.color        = '';
        btn.style.borderColor  = '';
        btn.style.fontWeight   = '';
    }
}

function clearSmartColoring() {
    const table = document.querySelector('#result-table-wrap table.result-table');
    if (table) {
        table.querySelectorAll('tbody tr').forEach(tr => {
            tr.style.removeProperty('background');
            tr.querySelectorAll('td').forEach(td => {
                td.style.removeProperty('background-color');
                td.style.removeProperty('color');
                td.style.removeProperty('font-weight');
                td.style.removeProperty('-webkit-print-color-adjust');
                td.style.removeProperty('print-color-adjust');
            });
        });
    }
    // Remove legend bar if we created it; otherwise just remove the legend span
    document.querySelectorAll('.smart-color-legend').forEach(el => el.remove());
    const createdBar = document.getElementById('smart-legend-bar');
    if (createdBar) createdBar.remove();
}

// ── Apply coloring ────────────────────────────────────────────────────────────
function applySmartRowColoring() {
    if (!window._smartColorEnabled) return;
    const table = document.querySelector('#result-table-wrap table.result-table');
    if (!table) return;

    const headers = Array.from(table.querySelectorAll('th'));
    if (!headers.length) return;

    const colRules = headers.map((th, i) => i === 0 ? null : _detectColRule(th.textContent));
    if (!colRules.some(Boolean)) {
        // No recognised columns — show a small tooltip on the button
        const btn = document.getElementById('smart-color-toggle-btn');
        if (btn) {
            btn.title = 'No semantic columns detected in this result set. Colour Describe works with columns named: status, signal_quality, fraud, risk_score, severity, packet_loss_pct, latency_ms, cpu, memory...';
            btn.textContent = '✦ No Match';
            setTimeout(() => { if (window._smartColorEnabled) btn.textContent = '✦ Colour On'; }, 2500);
        }
        return;
    }

    let coloredCount = 0;
    table.querySelectorAll('tbody tr').forEach(tr => {
        const cells = Array.from(tr.querySelectorAll('td'));
        let best = null; let bestIdx = -1;
        for (let i = 1; i < cells.length; i++) {
            if (!colRules[i]) continue;
            const s = colRules[i].rowColor(cells[i].textContent.trim());
            if (s && _priority(s) > _priority(best)) { best = s; bestIdx = i; }
        }
        if (best) {
            coloredCount++;
            // Apply background to EVERY cell inline — required for PDF/print
            cells.forEach(td => {
                td.style.setProperty('background-color',            best.bg,  'important');
                td.style.setProperty('-webkit-print-color-adjust',  'exact',  'important');
                td.style.setProperty('print-color-adjust',          'exact',  'important');
            });
            // Highlight the triggering cell with coloured text
            if (bestIdx >= 0) {
                cells[bestIdx].style.setProperty('color',        best.text,            'important');
                cells[bestIdx].style.setProperty('font-weight',  best.bold ? '700':'600', 'important');
            }
        }
    });

    _injectColorLegend(coloredCount);
}

// ── Legend — always visible, placed after the table ──────────────────────────
function _injectColorLegend(coloredCount) {
    // Remove any existing legend
    document.querySelectorAll('.smart-color-legend').forEach(el => el.remove());
    const existingBar = document.getElementById('smart-legend-bar');
    if (existingBar) existingBar.remove();

    // Find the real pagination bar — it exists but may be display:none when 1 page
    const pagBar = document.getElementById('result-pagination');

    const legend = document.createElement('span');
    legend.className = 'smart-color-legend';
    legend.style.cssText = 'display:inline-flex;gap:10px;align-items:center;font-size:10px;color:var(--text2);flex-wrap:wrap;padding:3px 0;';
    legend.innerHTML = `
    <span style="color:var(--text3);font-size:9px;letter-spacing:0.5px;text-transform:uppercase;white-space:nowrap;">✦ ${coloredCount || 0} row${coloredCount===1?'':'s'} coloured:</span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#3d0a14;border:1px solid #ff6b7a;display:inline-block;"></span><span style="color:#ff6b7a;">Critical/Offline</span></span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#3d2200;border:1px solid #f5a623;display:inline-block;"></span><span style="color:#f5a623;">Warning/Congested</span></span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#0a2a14;border:1px solid #39d353;display:inline-block;"></span><span style="color:#39d353;">Healthy/Active</span></span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#0a1a2e;border:1px solid #4fa3e0;display:inline-block;"></span><span style="color:#4fa3e0;">Maintenance</span></span>
  `;

    if (pagBar) {
        // Pagination bar exists — make it visible and append legend to it
        pagBar.style.display = 'flex';
        pagBar.style.alignItems = 'center';
        // The legend goes after the existing pagination content
        pagBar.appendChild(legend);
    } else {
        // No pagination bar — create a dedicated legend bar below the table
        const wrap = document.getElementById('result-table-wrap');
        if (!wrap) return;
        const bar = document.createElement('div');
        bar.id = 'smart-legend-bar';
        bar.style.cssText = 'display:flex;align-items:center;padding:5px 12px;background:var(--bg1);border-top:1px solid var(--border);flex-wrap:wrap;';
        bar.appendChild(legend);
        wrap.appendChild(bar);
    }
}

// ── Inject toggle button ──────────────────────────────────────────────────────
function _injectColorToggle() {
    if (document.getElementById('smart-color-toggle-btn')) return;
    const actions = document.querySelector('.results-actions');
    if (!actions) return;
    const btn = document.createElement('button');
    btn.id        = 'smart-color-toggle-btn';
    btn.className = 'topbar-btn';
    btn.title     = 'Colour Describe: intelligently colour rows based on column semantics (status, risk, quality, latency, etc.)';
    btn.style.cssText = 'font-size:10px;transition:background 0.15s,color 0.15s,border-color 0.15s;';
    btn.textContent   = '✦ Colour Describe';
    btn.onclick = toggleSmartColoring;
    const exportBtn = document.getElementById('export-results-btn');
    if (exportBtn) actions.insertBefore(btn, exportBtn);
    else actions.appendChild(btn);
}

// ── Hook renderResults ────────────────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof renderResults !== 'function') return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            setTimeout(() => {
                _injectColorToggle();
                // Reapply and update button state after new results render
                if (window._smartColorEnabled) {
                    applySmartRowColoring();
                    _updateColorBtn();
                }
            }, 100);
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 400); }
})();

// ── Print/PDF: force background colour output ─────────────────────────────────
(function() {
    const s = document.createElement('style');
    s.textContent = `
    @media print {
      * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; color-adjust: exact !important; }
      .result-table td { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
      .smart-color-legend { display:inline-flex !important; }
      #smart-legend-bar { display:flex !important; }
      #result-pagination { display:flex !important; }
    }
    .result-table td { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  `;
    document.head.appendChild(s);
})();

// ── Pipeline type detection ───────────────────────────────────────────────────
function _detectPipelineType(sql) {
    if (!sql) return 'general';
    const s = sql.toUpperCase();
    if (/FRAUD|SUSPICIOUS|ANOMALY|FLAGGED|RISK_SCORE/.test(s))   return 'fraud_detection';
    if (/SIGNAL|TOWER|RSSI|LATENCY|PACKET_LOSS|SINR/.test(s))   return 'network_analysis';
    if (/SENSOR|TEMPERATURE|HUMIDITY|PRESSURE|IOT/.test(s))      return 'iot_sensor';
    if (/TRADE|ORDER|STOCK|PRICE|TICKER|EXECUTION/.test(s))      return 'financial_trading';
    if (/CLICK|PAGEVIEW|USER_ID|VISIT|SESSION_ID/.test(s))       return 'clickstream';
    if (/LOG|EXCEPTION|STACK_TRACE|LOG_LEVEL/.test(s))           return 'log_analysis';
    if (/ALERT|INCIDENT|OUTAGE|SLA|SLA_CLASS/.test(s))           return 'alerting';
    return 'general';
}

const PIPELINE_DESCRIPTIONS = {
    fraud_detection:  'Fraud Detection Pipeline — Identifies suspicious activity in real time. Red = high-risk/fraud, Yellow = review required, Green = clean.',
    network_analysis: 'Network Signal Analysis Pipeline — Monitors tower/node health. Red = offline/critical, Yellow = congested/degraded, Green = healthy.',
    iot_sensor:       'IoT Sensor Monitoring Pipeline — Processes sensor readings. Highlighted rows are outside normal operating thresholds.',
    financial_trading:'Financial Trading Pipeline — Streams live trade and order data. Large or anomalous values are highlighted for review.',
    clickstream:      'Clickstream Analytics Pipeline — Tracks user sessions and page interactions in real time.',
    log_analysis:     'Log Analysis Pipeline — Processes application/system log streams. Error and exception events are highlighted.',
    alerting:         'Alerting & Monitoring Pipeline — Detects and routes infrastructure and SLA alerts.',
    general:          'Data Streaming Pipeline — Processes and transforms streaming data from source to sink in real time.',
};

// ── Patch generateSessionReport ───────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof generateSessionReport !== 'function') return false;
        const _orig = generateSessionReport;
        window.generateSessionReport = function() {
            const activeSlot = (state.resultSlots || []).find(s => s.id === state.activeSlot);
            const sql = activeSlot ? activeSlot.sql : ((state.history||[]).slice(-1)[0]||{}).sql || '';
            const ptype = _detectPipelineType(sql);
            window._reportPipelineDescription = PIPELINE_DESCRIPTIONS[ptype];
            window._reportPipelineType        = ptype;
            window._reportColoringActive      = !!window._smartColorEnabled;
            if (window._smartColorEnabled) applySmartRowColoring();
            const statusEl = document.getElementById('report-status');
            if (statusEl) {
                const pLabel = ptype.replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase());
                statusEl.innerHTML = `<span style="color:var(--accent);font-size:10px;">📊 ${pLabel}${window._smartColorEnabled?' · ✦ Colours included':''}</span>`;
            }
            return _orig.apply(this, arguments);
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 500); }
})();

// ── Per-job checkboxes in comparison chart ────────────────────────────────────
window._jcHiddenJobs = new Set();
const JC_COLORS = ['#00d4aa','#4fa3e0','#f5a623','#ff6b7a','#b06dff','#39d353',
    '#ff9f43','#1dd1a1','#feca57','#ff6348','#74b9ff','#a29bfe'];

function _renderJobCompareCheckboxes(jobs) {
    const legend = document.getElementById('job-compare-legend');
    if (!legend) return;
    legend.innerHTML = '';
    if (!jobs || !jobs.length) return;
    jobs.forEach((job, i) => {
        const color = JC_COLORS[i % JC_COLORS.length];
        const jid   = job.jid;
        const name  = (job.name || jid).slice(0, 26);
        const hidden = window._jcHiddenJobs.has(jid);
        const wrap  = document.createElement('label');
        wrap.style.cssText = `display:flex;align-items:center;gap:4px;cursor:pointer;font-size:10px;color:${hidden?'var(--text3)':color};font-family:var(--mono);user-select:none;`;
        wrap.title = job.name || jid;
        const cb = document.createElement('input');
        cb.type = 'checkbox'; cb.checked = !hidden;
        cb.style.cssText = `accent-color:${color};cursor:pointer;`;
        cb.onchange = () => {
            if (cb.checked) window._jcHiddenJobs.delete(jid); else window._jcHiddenJobs.add(jid);
            wrap.style.color = cb.checked ? color : 'var(--text3)';
            if (typeof redrawJobCompare === 'function') redrawJobCompare();
        };
        const dot = document.createElement('span');
        dot.style.cssText = `width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0;`;
        const lbl = document.createElement('span'); lbl.textContent = name;
        wrap.append(cb, dot, lbl);
        legend.appendChild(wrap);
    });
}

(function() {
    function _patch() {
        if (typeof renderJobList !== 'function') return false;
        const _orig = renderJobList;
        window.renderJobList = function(jobs) {
            const filtered = (typeof filterJobsForCurrentSession === 'function')
                ? filterJobsForCurrentSession(jobs) : jobs;
            _orig(filtered);
            _renderJobCompareCheckboxes(filtered);
            if (!state.isAdminSession && (jobs||[]).length > 0 && filtered.length === 0) {
                const list = document.getElementById('perf-job-list');
                if (list) list.innerHTML = `<div style="font-size:11px;color:var(--text3);padding:8px 0;line-height:1.7;">No jobs submitted in this session.<br><span style="font-size:10px;">${jobs.length} job(s) on cluster belong to other sessions. Connect as Admin to see all.</span></div>`;
            }
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 500); }
})();

// ── Metric selector fix ───────────────────────────────────────────────────────
(function() {
    function _wireSelectors() {
        const inline = document.getElementById('job-compare-metric');
        if (inline && !inline._ri_patched) {
            inline._ri_patched = true;
            inline.addEventListener('change', () => { if (typeof redrawJobCompare==='function') redrawJobCompare(); });
        }
    }
    setTimeout(_wireSelectors, 800);
    function _patchModal() {
        if (typeof openJobCompareModal !== 'function') return false;
        const _orig = openJobCompareModal;
        window.openJobCompareModal = function() {
            _orig.apply(this, arguments);
            setTimeout(() => {
                const sel = document.getElementById('jc-modal-metric');
                if (sel && !sel._ri_patched) {
                    sel._ri_patched = true;
                    sel.addEventListener('change', () => {
                        const inline = document.getElementById('job-compare-metric');
                        if (inline) inline.value = sel.value;
                        if (typeof redrawJobCompare==='function') redrawJobCompare();
                    });
                }
                _wireSelectors();
            }, 200);
        };
        return true;
    }
    if (!_patchModal()) { const t = setInterval(() => { if (_patchModal()) clearInterval(t); }, 600); }
})();

// ── Live events dual-line chart ───────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof _drawNdChart==='undefined'||typeof _ndChartData==='undefined') return false;
        window._drawNdChart = function() {
            const canvas = document.getElementById('nd-throughput-canvas');
            if (!canvas) return;
            const rect = canvas.getBoundingClientRect();
            if (rect.width > 0) canvas.width = Math.round(rect.width);
            const ctx = canvas.getContext('2d');
            const W = canvas.width, H = canvas.height;
            ctx.clearRect(0,0,W,H);
            ctx.strokeStyle='rgba(255,255,255,0.05)'; ctx.lineWidth=1;
            [0.25,0.5,0.75].forEach(f=>{ctx.beginPath();ctx.moveTo(0,H*f);ctx.lineTo(W,H*f);ctx.stroke();});
            const inD  = (_ndChartData.recIn  ||[]).slice();
            const outD = (_ndChartData.recOut ||[]).slice();
            const all  = [...inD,...outD].filter(v=>v>0);
            const mx   = all.length ? Math.max(...all)*1.1 : 1;
            function ds(data,color,fill) {
                if (data.length<2) return;
                const pts = data.map((v,i)=>({x:(i/(data.length-1))*(W-4)+2,y:H-4-(v/mx)*(H-8)}));
                ctx.beginPath(); ctx.moveTo(pts[0].x,H);
                pts.forEach(p=>ctx.lineTo(p.x,p.y));
                ctx.lineTo(pts[pts.length-1].x,H); ctx.closePath();
                ctx.fillStyle=fill; ctx.fill();
                ctx.beginPath(); ctx.strokeStyle=color; ctx.lineWidth=1.5; ctx.lineJoin='round';
                pts.forEach((p,i)=>i===0?ctx.moveTo(p.x,p.y):ctx.lineTo(p.x,p.y)); ctx.stroke();
                const l=pts[pts.length-1];
                ctx.beginPath(); ctx.arc(l.x,l.y,3,0,Math.PI*2); ctx.fillStyle=color; ctx.fill();
            }
            ds(inD, '#4fa3e0','rgba(79,163,224,0.12)');
            ds(outD,'#00d4aa','rgba(0,212,170,0.12)');
        };
        return true;
    }
    if (!_patch()) { const t=setInterval(()=>{if(_patch())clearInterval(t);},600); }
})();

// ── Checkpoint panel ──────────────────────────────────────────────────────────
(function() {
    function _patchCP() {
        if (typeof refreshCheckpointPanel!=='function'||typeof jmApi!=='function') return false;
        const _o=refreshCheckpointPanel;
        window.refreshCheckpointPanel=async function(jid){
            if(jid)return _o(jid);
            try{const d=await jmApi('/jobs/overview');const r=(d&&d.jobs||[]).find(j=>j.state==='RUNNING');if(r)return _o(r.jid);}catch(_){}
            return _o(jid);
        };
        return true;
    }
    function _patchPerf() {
        if (typeof refreshPerf!=='function') return false;
        const _o=refreshPerf;
        window.refreshPerf=async function(){
            await _o.apply(this,arguments);
            try{const d=await jmApi('/jobs/overview');const r=(d&&d.jobs||[]).find(j=>j.state==='RUNNING');if(r&&typeof refreshCheckpointPanel==='function')refreshCheckpointPanel(r.jid);}catch(_){}
        };
        return true;
    }
    if(!_patchCP())  {const t=setInterval(()=>{if(_patchCP())  clearInterval(t);},600);}
    if(!_patchPerf()){const t=setInterval(()=>{if(_patchPerf())clearInterval(t);},700);}
})();

// ── Cancel button: hide for non-owned jobs ────────────────────────────────────
(function() {
    function _patch() {
        if (typeof onJgJobChange!=='function') return false;
        const _o=onJgJobChange;
        window.onJgJobChange=function(jid){
            _o.apply(this,arguments);
            if(!jid||state.isAdminSession) return;
            const btn=document.getElementById('jg-cancel-btn');
            if(!btn) return;
            const sess=state.sessions.find(s=>s.handle===state.activeSession);
            if(sess&&!(sess.jobIds||[]).includes(jid)) btn.style.display='none';
        };
        return true;
    }
    if(!_patch()){const t=setInterval(()=>{if(_patch())clearInterval(t);},600);}
})();