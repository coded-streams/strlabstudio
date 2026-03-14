/* FlinkSQL Studio — Results Intelligence
 * Smart row coloring in the results table based on column semantics.
 * Colors are applied AFTER the standard results render, without touching results.js.
 *
 * Also patches generateSessionReport() to include colored data sections
 * and a plain-language pipeline description in the PDF.
 */

// ── Column semantic classification ───────────────────────────────────────────
// Maps column names (lowercased, partial match) to value→color rules.
const SEMANTIC_RULES = [
    // ── STATUS / STATE columns ─────────────────────────────────────────────────
    {
        colMatch: /status|state|tower_status|connection_status|alert_status/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase();
            if (/OFFLINE|DOWN|FAILED|CRITICAL|ERROR|DEAD/.test(v))     return { bg: 'rgba(255,77,109,0.18)',  text: '#ff4d6d' };
            if (/CONGESTED|DEGRADED|WARN|ALERT|SLOW|THROTTL/.test(v)) return { bg: 'rgba(245,166,35,0.18)',  text: '#f5a623' };
            if (/ACTIVE|UP|RUNNING|OK|HEALTHY|ONLINE|NORMAL/.test(v)) return { bg: 'rgba(57,211,83,0.12)',   text: '#39d353' };
            if (/MAINTENANCE|PAUSED|SUSPEND/.test(v))                  return { bg: 'rgba(79,163,224,0.15)', text: '#4fa3e0' };
            return null;
        }
    },
    // ── FRAUD / RISK / ANOMALY columns ────────────────────────────────────────
    {
        colMatch: /fraud|is_fraud|fraud_flag|risk|risk_score|anomaly|suspicious|flagged/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (v === '1' || v === 'TRUE' || v === 'YES' || v === 'FRAUD' || v === 'HIGH')
                return { bg: 'rgba(255,77,109,0.20)', text: '#ff4d6d', bold: true };
            if (v === '0' || v === 'FALSE' || v === 'NO' || v === 'CLEAN' || v === 'LOW')
                return { bg: 'rgba(57,211,83,0.10)', text: '#39d353' };
            if (v === 'MEDIUM' || v === 'MODERATE')
                return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
            // Numeric risk score
            const n = parseFloat(val);
            if (!isNaN(n)) {
                if (n >= 0.8)  return { bg: 'rgba(255,77,109,0.20)', text: '#ff4d6d', bold: true };
                if (n >= 0.5)  return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
                if (n < 0.3)   return { bg: 'rgba(57,211,83,0.10)',  text: '#39d353' };
            }
            return null;
        }
    },
    // ── SIGNAL QUALITY columns ────────────────────────────────────────────────
    {
        colMatch: /signal|signal_quality|signal_strength|quality|grade/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (v === 'POOR' || v === 'BAD' || v === 'WEAK')        return { bg: 'rgba(255,77,109,0.18)', text: '#ff4d6d' };
            if (v === 'FAIR' || v === 'MODERATE' || v === 'MEDIUM') return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
            if (v === 'GOOD' || v === 'EXCELLENT' || v === 'STRONG')return { bg: 'rgba(57,211,83,0.12)',  text: '#39d353' };
            return null;
        }
    },
    // ── SEVERITY columns ──────────────────────────────────────────────────────
    {
        colMatch: /severity|level|priority|urgency/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (/CRITICAL|HIGH|EMERGENCY|P0|SEV1/.test(v))  return { bg: 'rgba(255,77,109,0.20)', text: '#ff4d6d', bold: true };
            if (/MEDIUM|MODERATE|WARNING|P1|P2|SEV2/.test(v))return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
            if (/LOW|INFO|MINOR|P3|P4|SEV3/.test(v))        return { bg: 'rgba(57,211,83,0.10)',  text: '#39d353' };
            return null;
        }
    },
    // ── PACKET LOSS / ERROR RATE columns ─────────────────────────────────────
    {
        colMatch: /packet_loss|error_rate|loss_pct|drop_rate|loss_percent/i,
        rowColor: (val) => {
            const n = parseFloat(val);
            if (isNaN(n)) return null;
            if (n >= 10)  return { bg: 'rgba(255,77,109,0.18)', text: '#ff4d6d' };
            if (n >= 3)   return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
            return { bg: 'rgba(57,211,83,0.08)', text: '#39d353' };
        }
    },
    // ── LATENCY / RESPONSE TIME columns ──────────────────────────────────────
    {
        colMatch: /latency|response_time|rtt|delay_ms|latency_ms/i,
        rowColor: (val) => {
            const n = parseFloat(val);
            if (isNaN(n)) return null;
            if (n >= 500) return { bg: 'rgba(255,77,109,0.18)', text: '#ff4d6d' };
            if (n >= 200) return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
            return null;
        }
    },
    // ── TRANSACTION / AMOUNT anomaly (large transactions flagged) ─────────────
    {
        colMatch: /amount|transaction_amount|value|total_amount/i,
        rowColor: (val) => {
            const n = parseFloat(String(val).replace(/[$,]/g, ''));
            if (isNaN(n)) return null;
            if (n >= 50000) return { bg: 'rgba(245,166,35,0.15)', text: '#f5a623' };
            if (n >= 100000)return { bg: 'rgba(255,77,109,0.18)', text: '#ff4d6d' };
            return null;
        }
    },
];

// ── Detect which semantic rule applies to a given column name ─────────────────
function _detectColRule(colName) {
    for (const rule of SEMANTIC_RULES) {
        if (rule.colMatch.test(colName)) return rule;
    }
    return null;
}

// ── Determine pipeline type from SQL for report descriptions ─────────────────
function _detectPipelineType(sql) {
    if (!sql) return null;
    const s = sql.toUpperCase();
    if (/FRAUD|SUSPICIOUS|ANOMALY|FLAGGED/.test(s))          return 'fraud_detection';
    if (/SIGNAL|TOWER|RSSI|LATENCY|PACKET_LOSS/.test(s))     return 'network_analysis';
    if (/SENSOR|TEMPERATURE|HUMIDITY|PRESSURE|IOT/.test(s))  return 'iot_sensor';
    if (/TRADE|ORDER|STOCK|PRICE|TICKER/.test(s))            return 'financial_trading';
    if (/CLICK|SESSION|PAGE|USER_ID|VISIT/.test(s))          return 'clickstream';
    if (/LOG|ERROR|EXCEPTION|STACK_TRACE/.test(s))           return 'log_analysis';
    if (/ALERT|INCIDENT|OUTAGE|MONITOR/.test(s))             return 'alerting';
    return 'general';
}

const PIPELINE_DESCRIPTIONS = {
    fraud_detection:    'Fraud Detection Pipeline — Analyses transaction streams in real time to identify suspicious activity. Rows highlighted in red indicate transactions flagged as fraudulent or high-risk. Yellow indicates transactions requiring review.',
    network_analysis:   'Network Signal Analysis Pipeline — Monitors tower and device signal quality across the network. Red rows indicate offline or critically degraded nodes. Yellow indicates congestion or maintenance. Green indicates healthy active nodes.',
    iot_sensor:         'IoT Sensor Monitoring Pipeline — Processes sensor readings in real time. Highlighted rows indicate readings outside normal operating thresholds.',
    financial_trading:  'Financial Trading Pipeline — Streams live trade and order data. Large-value transactions and anomalous prices are highlighted for review.',
    clickstream:        'Clickstream Analytics Pipeline — Tracks user sessions and page interactions in real time.',
    log_analysis:       'Log Analysis Pipeline — Processes application and system log streams. Error and exception events are highlighted.',
    alerting:           'Alerting & Monitoring Pipeline — Detects and routes alerts from infrastructure and application streams.',
    general:            'Data Streaming Pipeline — Processes and transforms streaming data from source to sink in real time.',
};

// ── Apply smart row coloring to the result table after render ──────────────────
function applySmartRowColoring() {
    const table = document.querySelector('#result-table-wrap table.result-table');
    if (!table) return;

    const headers = Array.from(table.querySelectorAll('th'));
    if (headers.length === 0) return;

    // Map column index → semantic rule (skip row-number column at index 0)
    const colRules = headers.map((th, i) => {
        if (i === 0) return null; // row number column
        const colName = th.textContent.replace(/\s+/g, ' ').trim().split(' ')[0]; // first word = col name
        return _detectColRule(colName);
    });

    const hasAnyRule = colRules.some(r => r !== null);
    if (!hasAnyRule) return;

    const rows = Array.from(table.querySelectorAll('tbody tr'));
    rows.forEach(tr => {
        const cells = Array.from(tr.querySelectorAll('td'));
        let rowStyle = null;

        // Find the most significant color from any triggering column
        for (let i = 1; i < cells.length; i++) {
            const rule = colRules[i];
            if (!rule) continue;
            const val = cells[i].textContent.trim();
            const style = rule.rowColor(val);
            if (style) {
                // Priority: red > yellow > blue > green
                const priority = (s) => {
                    if (s.text === '#ff4d6d') return 4;
                    if (s.text === '#f5a623') return 3;
                    if (s.text === '#4fa3e0') return 2;
                    if (s.text === '#39d353') return 1;
                    return 0;
                };
                if (!rowStyle || priority(style) > priority(rowStyle)) {
                    rowStyle = style;
                    rowStyle._triggerCol = i;
                    rowStyle._triggerVal = val;
                }
            }
        }

        if (rowStyle) {
            tr.style.background = rowStyle.bg;
            // Highlight the triggering cell specifically
            if (rowStyle._triggerCol !== undefined) {
                const trigCell = cells[rowStyle._triggerCol];
                trigCell.style.color     = rowStyle.text;
                trigCell.style.fontWeight = rowStyle.bold ? '700' : '600';
            }
        }
    });

    // Add legend below the table if coloring was applied
    _injectColorLegend(table, colRules, headers);
}

function _injectColorLegend(table, colRules, headers) {
    const wrap = document.getElementById('result-table-wrap');
    if (!wrap) return;
    const existingLegend = wrap.querySelector('.smart-color-legend');
    if (existingLegend) existingLegend.remove();

    const activeCols = headers
        .map((th, i) => ({ name: th.textContent.split(' ')[0], rule: colRules[i] }))
        .filter(c => c.rule);
    if (activeCols.length === 0) return;

    const legend = document.createElement('div');
    legend.className = 'smart-color-legend';
    legend.style.cssText = 'display:flex;gap:16px;align-items:center;padding:5px 12px;background:var(--bg1);border-top:1px solid var(--border);flex-shrink:0;font-size:10px;color:var(--text2);flex-wrap:wrap;';

    legend.innerHTML = `
    <span style="color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-size:9px;">Smart coloring:</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:2px;background:rgba(255,77,109,0.3);display:inline-block;"></span> Critical / Fraud / Offline</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:2px;background:rgba(245,166,35,0.3);display:inline-block;"></span> Warning / Congested / Degraded</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:2px;background:rgba(57,211,83,0.25);display:inline-block;"></span> Healthy / Active / Normal</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:2px;background:rgba(79,163,224,0.25);display:inline-block;"></span> Maintenance / Paused</span>
  `;

    // Insert after the table
    if (table.parentNode === wrap) {
        wrap.insertBefore(legend, table.nextSibling);
    } else {
        wrap.appendChild(legend);
    }
}

// ── Hook into renderResults to apply coloring after every render ──────────────
(function _hookRenderResults() {
    function _patch() {
        if (typeof renderResults !== 'function') return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            // Small delay so DOM is fully updated before we walk it
            setTimeout(applySmartRowColoring, 60);
        };
        return true;
    }
    if (!_patch()) {
        const t = setInterval(() => { if (_patch()) clearInterval(t); }, 500);
    }
})();

// ── Patch generateSessionReport to add pipeline description + colored rows ────
(function _hookReport() {
    function _patch() {
        if (typeof generateSessionReport !== 'function') return false;
        const _orig = generateSessionReport;
        window.generateSessionReport = function() {
            // Inject intelligence before delegating to original
            _enrichReportContext();
            return _orig.apply(this, arguments);
        };
        return true;
    }
    if (!_patch()) {
        const t = setInterval(() => { if (_patch()) clearInterval(t); }, 600);
    }
})();

function _enrichReportContext() {
    // Detect pipeline type from active query
    const activeSlot = (state.resultSlots || []).find(s => s.id === state.activeSlot);
    const sql = activeSlot ? activeSlot.sql : (state.history && state.history.length > 0 ? state.history[state.history.length-1].sql : '');
    const pipelineType = _detectPipelineType(sql);
    const description  = PIPELINE_DESCRIPTIONS[pipelineType] || PIPELINE_DESCRIPTIONS.general;

    // Store for the report generator to pick up
    window._reportPipelineDescription = description;
    window._reportPipelineType        = pipelineType;
    window._reportColorRules          = SEMANTIC_RULES;
    window._reportColHeaders          = (state.resultColumns || []).map(c => c.name || c);

    // Inject description into the report status element as a preview
    const statusEl = document.getElementById('report-status');
    if (statusEl) {
        statusEl.innerHTML = `<span style="color:var(--accent);font-size:10px;">📊 Detected: <strong>${(pipelineType||'general').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase())}</strong> — ${description.split('—')[0]}</span>`;
    }
}

// ── Live events dual-line chart fix ───────────────────────────────────────────
// The original _drawNdChart only shows one line because for SOURCE nodes it
// maps both series to OUT. We patch it to always draw both IN (blue) and
// OUT (teal) as truly separate lines with proper labels.
(function _patchNdChart() {
    function _patch() {
        // _drawNdChart lives in jobgraph.js — check it exists
        if (typeof _drawNdChart !== 'function') return false;
        // _ndChartData is a module-level object in jobgraph.js
        if (typeof _ndChartData === 'undefined') return false;

        const _orig = _drawNdChart;
        window._drawNdChart = function() {
            const canvas = document.getElementById('nd-throughput-canvas');
            if (!canvas) return;

            const rect = canvas.getBoundingClientRect();
            if (rect.width > 0) canvas.width = Math.round(rect.width);
            const ctx = canvas.getContext('2d');
            const W = canvas.width, H = canvas.height;
            ctx.clearRect(0, 0, W, H);

            // Draw grid lines
            ctx.strokeStyle = 'rgba(255,255,255,0.04)';
            ctx.lineWidth = 1;
            [H*0.25, H*0.5, H*0.75].forEach(y => {
                ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
            });

            const inData  = _ndChartData.recIn  || [];
            const outData = _ndChartData.recOut || [];

            if (inData.length < 2 && outData.length < 2) return;

            const allVals = [...inData, ...outData].filter(v => v > 0);
            const mx = allVals.length > 0 ? Math.max(...allVals) : 1;

            function drawLine(data, color, fillColor) {
                if (data.length < 2) return;
                const mn = 0;
                const pts = data.map((v, i) => ({
                    x: (i / (data.length - 1)) * (W - 4) + 2,
                    y: H - 4 - ((v - mn) / mx) * (H - 8),
                }));

                // Fill area
                ctx.beginPath();
                ctx.moveTo(pts[0].x, H);
                pts.forEach(p => ctx.lineTo(p.x, p.y));
                ctx.lineTo(pts[pts.length-1].x, H);
                ctx.closePath();
                ctx.fillStyle = fillColor;
                ctx.fill();

                // Line
                ctx.beginPath();
                ctx.strokeStyle = color;
                ctx.lineWidth = 1.5;
                ctx.lineJoin = 'round';
                pts.forEach((p, i) => i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y));
                ctx.stroke();

                // Dot at end
                const last = pts[pts.length-1];
                ctx.beginPath();
                ctx.arc(last.x, last.y, 3, 0, Math.PI*2);
                ctx.fillStyle = color;
                ctx.fill();
            }

            // Draw IN first (behind), then OUT on top
            drawLine(inData,  '#4fa3e0', 'rgba(79,163,224,0.10)');
            drawLine(outData, '#7ee8d0', 'rgba(126,232,208,0.12)');
        };

        return true;
    }

    if (!_patch()) {
        const t = setInterval(() => { if (_patch()) clearInterval(t); }, 600);
    }
})();