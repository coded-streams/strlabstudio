/* Str:::lab Studio — results-charts.js v6
 * ─────────────────────────────────────────────────────────────────────────────
 * NEW in v6:
 *  1. SCROLL ZOOM — plain mouse-wheel over the chart zooms in/out (no Ctrl
 *     needed). Zoom is centred on the cursor so you zoom into exactly where
 *     you're looking. When zoomed in the inner div expands and the outer
 *     scrolls, so Chart.js tooltips/hover remain fully interactive.
 *  2. DRAG TO PAN — click-drag anywhere on the chart to pan when zoomed in.
 *  3. CORRELATION MATRIX — new chart type (⬡). Computes Pearson r between
 *     every pair of selected numeric columns. Colour-coded grid; teal =
 *     positive, red = negative. Auto-detects numeric columns if no Y fields
 *     selected. Ideal for spotting relationships in streaming data.
 *  4. DUAL-AXIS TIME SERIES — new chart type (⇅). Two Y fields on independent
 *     left and right axes for comparing fields with very different scales
 *     (e.g. latency_ms vs fraud_score). Y2 selector in left panel.
 *  5. Modal widened to 1280px, chart canvas min-height 480px.
 *  6. All v5 fixes retained.
 * ─────────────────────────────────────────────────────────────────────────────
 */

'use strict';

const CHART_TYPES = [
    { value:'bar',         label:'Bar',         icon:'▐█' },
    { value:'line',        label:'Line',        icon:'📈' },
    { value:'area',        label:'Area',        icon:'▲'  },
    { value:'pie',         label:'Pie',         icon:'🥧' },
    { value:'donut',       label:'Donut',       icon:'🍩' },
    { value:'histogram',   label:'Histogram',   icon:'▐▌' },
    { value:'scatter',     label:'Scatter',     icon:'⋱'  },
    { value:'heatmap',     label:'Heatmap',     icon:'🌡' },
    { value:'correlation', label:'Correlation', icon:'⬡'  },
    { value:'dualaxis',    label:'Dual Axis',   icon:'⇅'  },
];

const CHART_COLORS_PALETTE = [
    '#00d4aa','#4fa3e0','#f5a623','#ff5090','#b06dff',
    '#39d353','#ff9f43','#00cec9','#fd79a8','#6c5ce7',
];

window._chartReportDefs    = window._chartReportDefs   || [];
window._chartReportSlotId  = window._chartReportSlotId || null;
window._chartReportType    = window._chartReportType   || 'bar';
window._chartReportInst    = window._chartReportInst   || null;
window._chartReportTimer   = window._chartReportTimer  || null;
window._chartReportXCol    = window._chartReportXCol   || null;
window._chartZoomLevel     = window._chartZoomLevel    || 1.0;

function _crEsc(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function def_agg_any(defs) { return defs.some(d => d.agg && d.agg !== 'none'); }

function _crGetColumns(slotId) {
    const slots = (typeof state !== 'undefined' && state && state.resultSlots) ? state.resultSlots : [];
    const slot  = slotId ? slots.find(s => s.id === slotId) : slots.find(s => s.rows && s.rows.length > 0);
    if (slot && slot.columns && slot.columns.length)
        return slot.columns.map(c => c.name || String(c)).filter(Boolean);
    const table = document.querySelector('#result-table-wrap table');
    if (table)
        return Array.from(table.querySelectorAll('thead th'))
            .slice(1).map(th => th.textContent.trim().split(/\s+/)[0]).filter(Boolean);
    return [];
}

function _crGetRows(slotId) {
    const slots = (typeof state !== 'undefined' && state && state.resultSlots) ? state.resultSlots : [];
    const slot  = slotId ? slots.find(s => s.id === slotId) : slots.find(s => s.rows && s.rows.length > 0);
    if (slot && slot.rows && slot.rows.length)
        return slot.rows.map(row => {
            const fields = Array.isArray(row?.fields) ? row.fields : Object.values(row?.fields || row || {});
            return fields;
        });
    const table = document.querySelector('#result-table-wrap table');
    if (table)
        return Array.from(table.querySelectorAll('tbody tr')).map(tr =>
            Array.from(tr.querySelectorAll('td')).slice(1).map(td => td.textContent.trim())
        );
    return [];
}

// ── Open modal ────────────────────────────────────────────────────────────────
function openChartReportModal() {
    let modal = document.getElementById('modal-chart-report');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'modal-chart-report';
        modal.className = 'modal-overlay';
        modal.innerHTML = _crBuildModalHTML();
        document.body.appendChild(modal);
        modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-chart-report'); });
        setTimeout(_crInstallZoomControls, 120);
    }
    openModal('modal-chart-report');
    _crPopulateSlots();
    _crRenderFieldList();
    _crWaitForChartJs(() => setTimeout(_crRenderChart, 100));
}

function _crWaitForChartJs(cb) {
    if (typeof Chart !== 'undefined') { cb(); return; }
    const st = document.getElementById('cr-chart-status');
    if (st) st.textContent = 'Loading Chart.js…';
    let n = 0;
    const t = setInterval(() => {
        if (typeof Chart !== 'undefined') { clearInterval(t); if (st) st.textContent = ''; cb(); }
        else if (++n > 40) { clearInterval(t); if (st) st.textContent = 'Chart.js failed to load'; }
    }, 250);
}

// ── Modal HTML ────────────────────────────────────────────────────────────────
function _crBuildModalHTML() {
    const typeGrid = CHART_TYPES.map(t => `
        <button class="cr-type-btn ${t.value==='bar'?'cr-type-active':''}"
          data-type="${t.value}" onclick="_crSelectType('${t.value}',this)" title="${t.label}"
          style="padding:5px 2px;border-radius:3px;border:1px solid var(--border);background:var(--bg2);
                 color:var(--text1);cursor:pointer;font-size:8px;text-align:center;font-family:var(--mono);">
          <div style="font-size:12px;margin-bottom:1px;">${t.icon}</div>${t.label}
        </button>`).join('');

    return `
<div class="modal" style="width:1280px;max-width:97vw;max-height:95vh;display:flex;flex-direction:column;">
  <div class="modal-header" style="display:flex;align-items:center;gap:10px;flex-shrink:0;">
    <span style="font-size:14px;">📊</span>
    <span style="font-weight:700;font-size:13px;">Chart Report</span>
    <div style="font-size:9px;color:var(--text3);margin-left:2px;font-family:var(--mono);">scroll over chart to zoom · drag to pan</div>
    <div style="margin-left:auto;display:flex;align-items:center;gap:6px;">
      <button onclick="_crExportPDF()" style="font-size:10px;padding:3px 10px;border-radius:3px;cursor:pointer;
        border:1px solid rgba(0,212,170,0.3);background:rgba(0,212,170,0.08);color:var(--accent);font-family:var(--mono);">📄 PDF</button>
      <button class="modal-close" onclick="closeModal('modal-chart-report')">×</button>
    </div>
  </div>

  <div class="modal-body" style="flex:1;overflow:hidden;display:flex;gap:0;padding:0;min-height:0;">

    <!-- LEFT PANEL -->
    <div style="width:255px;flex-shrink:0;border-right:1px solid var(--border);display:flex;flex-direction:column;overflow-y:auto;padding:14px 12px;gap:12px;">

      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);text-transform:uppercase;margin-bottom:6px;">Query / Job</div>
        <select id="cr-slot-select" onchange="_crOnSlotChange()" style="width:100%;font-size:11px;padding:5px 8px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text0);border-radius:var(--radius);cursor:pointer;">
          <option value="">— no results yet —</option>
        </select>
        <div id="cr-slot-info" style="font-size:10px;color:var(--text3);margin-top:4px;"></div>
      </div>

      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);text-transform:uppercase;margin-bottom:6px;">Chart Type</div>
        <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:3px;">${typeGrid}</div>
      </div>

      <div id="cr-xaxis-wrap" style="display:none;">
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);text-transform:uppercase;margin-bottom:6px;">X Axis (Labels)</div>
        <select id="cr-xaxis-select" onchange="_crOnXAxisChange()" style="width:100%;font-size:11px;padding:5px 8px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text0);border-radius:var(--radius);cursor:pointer;">
          <option value="">— row index —</option>
        </select>
        <div style="font-size:9px;color:var(--text3);margin-top:3px;">Category labels on X axis</div>
      </div>

      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);text-transform:uppercase;margin-bottom:6px;">
          <span id="cr-yaxis-label">Y Axis (Values)</span>
        </div>
        <div id="cr-field-list" style="display:flex;flex-direction:column;gap:6px;">
          <div style="font-size:11px;color:var(--text3);">Select a query first</div>
        </div>
        <button id="cr-add-field-btn" onclick="_crAddField()" style="margin-top:8px;width:100%;padding:5px;font-size:11px;font-family:var(--mono);border:1px dashed rgba(0,212,170,0.4);background:rgba(0,212,170,0.05);color:var(--accent);border-radius:3px;cursor:pointer;display:none;">＋ Add Y Field</button>
      </div>

      <div id="cr-y2-wrap" style="display:none;">
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);text-transform:uppercase;margin-bottom:6px;">Y2 Axis (Right Scale)</div>
        <select id="cr-y2-select" onchange="_crRenderChart()" style="width:100%;font-size:11px;padding:5px 8px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text0);border-radius:var(--radius);cursor:pointer;">
          <option value="">— none —</option>
        </select>
        <div style="font-size:9px;color:var(--text3);margin-top:3px;">Plotted on a separate right-hand Y axis</div>
      </div>

      <div style="display:flex;align-items:center;gap:8px;padding-top:4px;border-top:1px solid var(--border);">
        <input type="checkbox" id="cr-live-chk" checked onchange="_crToggleLive()" style="cursor:pointer;" />
        <label for="cr-live-chk" style="font-size:11px;color:var(--text2);cursor:pointer;">Live — refresh every 2s</label>
      </div>

      <div style="display:flex;align-items:center;gap:6px;">
        <label style="font-size:10px;color:var(--text3);white-space:nowrap;">Max rows:</label>
        <input type="number" id="cr-max-rows" value="200" min="10" max="5000" step="50" onchange="_crRenderChart()"
          style="width:70px;font-size:11px;padding:3px 5px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text0);border-radius:3px;" />
      </div>

      <button onclick="_crWaitForChartJs(_crRenderChart)" style="padding:6px;font-size:11px;font-family:var(--mono);width:100%;border:1px solid var(--border);background:var(--bg3);color:var(--text1);border-radius:3px;cursor:pointer;">⟳ Refresh Chart</button>
    </div>

    <!-- RIGHT PANEL -->
    <div style="flex:1;display:flex;flex-direction:column;overflow:hidden;padding:10px 14px 14px;">

      <!-- Toolbar -->
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-shrink:0;flex-wrap:wrap;">
        <div id="cr-chart-status" style="font-size:10px;color:var(--text3);flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:var(--mono);"></div>
        <div style="display:flex;align-items:center;gap:4px;flex-shrink:0;">
          <span style="font-size:9px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-right:2px;">Zoom</span>
          <button onclick="_crZoomStep(-0.25)" title="Zoom out" style="width:24px;height:22px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;font-size:14px;line-height:1;">−</button>
          <span id="cr-zoom-label" style="font-size:10px;font-family:var(--mono);color:var(--text2);min-width:36px;text-align:center;">100%</span>
          <button onclick="_crZoomStep(0.25)" title="Zoom in" style="width:24px;height:22px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;font-size:14px;line-height:1;">＋</button>
          <button id="cr-zoom-reset" onclick="_crZoomReset()" title="Reset zoom" style="padding:0 7px;height:22px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;font-size:10px;font-family:var(--mono);">1:1</button>
        </div>
      </div>

      <!-- Canvas viewport -->
      <div id="cr-canvas-outer" style="flex:1;overflow:auto;position:relative;cursor:grab;background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);">
        <div id="cr-canvas-inner" style="position:relative;width:100%;height:100%;min-height:480px;transform-origin:0 0;">
          <canvas id="cr-chart-canvas" style="display:block;width:100%;height:100%;min-height:480px;"></canvas>
          <div id="cr-chart-empty" style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;color:var(--text3);pointer-events:none;">
            <span style="font-size:36px;opacity:0.25;">📊</span>
            <span style="font-size:12px;">Select a query, configure axes, then click Refresh</span>
          </div>
        </div>
      </div>

      <div id="cr-chart-legend" style="display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;font-size:11px;min-height:18px;flex-shrink:0;"></div>
    </div>
  </div>
</div>`;
}

// ═══════════════════════════════════════════════════════════════════════════
// ZOOM & PAN
// ═══════════════════════════════════════════════════════════════════════════

function _crZoomStep(delta) {
    const outer = document.getElementById('cr-canvas-outer');
    _crZoomAt(delta, outer ? outer.clientWidth/2 : 0, outer ? outer.clientHeight/2 : 0);
}

function _crZoomAt(delta, cx, cy) {
    const outer = document.getElementById('cr-canvas-outer');
    const inner = document.getElementById('cr-canvas-inner');
    if (!inner || !outer) return;
    const prevZ = window._chartZoomLevel;
    const newZ  = Math.min(8.0, Math.max(0.25, prevZ + delta));
    if (Math.abs(newZ - prevZ) < 0.001) return;
    window._chartZoomLevel = newZ;

    // Keep the pixel under the cursor stationary
    const logX = (outer.scrollLeft + cx) / prevZ;
    const logY = (outer.scrollTop  + cy) / prevZ;
    _crApplyZoom();
    outer.scrollLeft = logX * newZ - cx;
    outer.scrollTop  = logY * newZ - cy;
}

function _crZoomReset() {
    window._chartZoomLevel = 1.0;
    _crApplyZoom();
    const outer = document.getElementById('cr-canvas-outer');
    if (outer) { outer.scrollLeft = 0; outer.scrollTop = 0; }
}

function _crApplyZoom() {
    const inner = document.getElementById('cr-canvas-inner');
    const outer = document.getElementById('cr-canvas-outer');
    const label = document.getElementById('cr-zoom-label');
    if (!inner || !outer) return;
    const z  = window._chartZoomLevel;
    const ow = outer.clientWidth  || 900;
    const oh = outer.clientHeight || 500;

    if (z <= 1.0) {
        // Shrink via CSS scale — inner stays 100%/100% so the outer never scrolls
        inner.style.transform     = `scale(${z})`;
        inner.style.transformOrigin = '0 0';
        inner.style.width         = '100%';
        inner.style.height        = '100%';
        inner.style.minHeight     = '480px';
    } else {
        // Expand inner div so outer scrolls; no CSS scale so Chart.js hit-areas are correct
        inner.style.transform  = 'none';
        inner.style.width      = Math.round(ow * z) + 'px';
        inner.style.height     = Math.round(oh * z) + 'px';
        inner.style.minHeight  = Math.round(Math.max(480, oh * z)) + 'px';
        if (window._chartReportInst) { try { window._chartReportInst.resize(); } catch(_) {} }
    }

    if (label) label.textContent = Math.round(z * 100) + '%';
    const rb = document.getElementById('cr-zoom-reset');
    if (rb) rb.style.color = Math.abs(z - 1.0) > 0.05 ? 'var(--accent)' : 'var(--text3)';
}

function _crInstallZoomControls() {
    const outer = document.getElementById('cr-canvas-outer');
    if (!outer || outer._crZoomInstalled) return;
    outer._crZoomInstalled = true;

    // ── Scroll = zoom (no modifier needed) ───────────────────────────────────
    outer.addEventListener('wheel', (e) => {
        e.preventDefault();
        const rect  = outer.getBoundingClientRect();
        const cx    = e.clientX - rect.left;
        const cy    = e.clientY - rect.top;
        // Natural scroll: down = zoom out, up = zoom in
        // Sensitivity scales with current zoom so it feels consistent
        const speed = 0.0009 * (window._chartZoomLevel || 1.0);
        _crZoomAt(-e.deltaY * speed, cx, cy);
    }, { passive: false });

    // ── Drag to pan ───────────────────────────────────────────────────────────
    let _dragging = false, _ox = 0, _oy = 0;
    outer.addEventListener('mousedown', (e) => {
        if (e.button !== 0) return;
        _dragging = true;
        _ox = e.clientX + outer.scrollLeft;
        _oy = e.clientY + outer.scrollTop;
        outer.style.cursor = 'grabbing';
        e.preventDefault();
    });
    document.addEventListener('mousemove', (e) => {
        if (!_dragging) return;
        outer.scrollLeft = _ox - e.clientX;
        outer.scrollTop  = _oy - e.clientY;
    });
    document.addEventListener('mouseup', () => {
        if (!_dragging) return;
        _dragging = false;
        if (outer) outer.style.cursor = 'grab';
    });

    // ── Pinch to zoom ─────────────────────────────────────────────────────────
    let _ptrs = {}, _lastDist = null;
    outer.addEventListener('pointerdown', (e) => { _ptrs[e.pointerId] = e; });
    outer.addEventListener('pointermove', (e) => {
        if (!_ptrs[e.pointerId]) return;
        _ptrs[e.pointerId] = e;
        const pl = Object.values(_ptrs);
        if (pl.length === 2) {
            const dx = pl[0].clientX - pl[1].clientX, dy = pl[0].clientY - pl[1].clientY;
            const dist = Math.sqrt(dx*dx + dy*dy);
            if (_lastDist !== null) {
                const r = outer.getBoundingClientRect();
                const mx = (pl[0].clientX + pl[1].clientX) / 2 - r.left;
                const my = (pl[0].clientY + pl[1].clientY) / 2 - r.top;
                _crZoomAt((dist - _lastDist) * 0.006, mx, my);
            }
            _lastDist = dist;
        }
    });
    ['pointerup','pointercancel'].forEach(ev =>
        outer.addEventListener(ev, (e) => { delete _ptrs[e.pointerId]; if (Object.keys(_ptrs).length < 2) _lastDist = null; })
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SLOTS & FIELD MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════

function _crIsPlottable(slot) {
    if (!slot) return false;
    if (!slot.rows?.length && slot.status !== 'streaming') return false;
    const sql = (slot.sql || slot.label || '').replace(/^\/\*[\s\S]*?\*\/|^\s*--.*$/mg,'').trim();
    if (!sql) return slot.columns?.length > 1;
    return /^SELECT\b/i.test(sql);
}

function _crPopulateSlots() {
    const sel = document.getElementById('cr-slot-select');
    if (!sel) return;
    const slots = ((typeof state !== 'undefined' && state?.resultSlots) ? state.resultSlots : []).filter(_crIsPlottable);
    if (!slots.length) { sel.innerHTML = '<option value="">— run a SELECT query first —</option>'; return; }
    sel.innerHTML = slots.map((s, i) => {
        const lbl  = s.label || s.sql?.slice(0,30) || `Query ${i+1}`;
        const rows = s.rows ? s.rows.length : 0;
        return `<option value="${_crEsc(s.id)}">${_crEsc(lbl)} (${rows} rows)${s.status==='streaming'?' 🔴':''}</option>`;
    }).join('');
    const active = window._chartReportSlotId || (typeof state!=='undefined'&&state?.activeSlot) || slots[slots.length-1]?.id;
    if (active) sel.value = active;
    _crOnSlotChange();
}

function _crOnSlotChange() {
    const sel = document.getElementById('cr-slot-select');
    if (!sel) return;
    window._chartReportSlotId = sel.value || null;
    const cols = _crGetColumns(window._chartReportSlotId);
    const rows = _crGetRows(window._chartReportSlotId);
    const info = document.getElementById('cr-slot-info');
    if (info) info.textContent = `${rows.length} rows · ${cols.length} columns`;

    // X-axis
    const xWrap = document.getElementById('cr-xaxis-wrap');
    const xSel  = document.getElementById('cr-xaxis-select');
    if (xWrap) xWrap.style.display = cols.length ? 'block' : 'none';
    if (xSel && cols.length) {
        xSel.innerHTML = '<option value="">— row index —</option>' + cols.map(c=>`<option value="${_crEsc(c)}">${_crEsc(c)}</option>`).join('');
        const prevX = window._chartReportXCol;
        if (prevX && cols.includes(prevX)) xSel.value = prevX;
        else {
            const defX = cols.find(c => {
                const slot = (state?.resultSlots||[]).find(s=>s.id===window._chartReportSlotId);
                const col  = (slot?.columns||[]).find(col=>(col.name||col)===c);
                const type = (col?.logicalType?.type||col?.type||'').toUpperCase();
                return type.includes('CHAR')||type.includes('STRING')||type.includes('VARCHAR')||type.includes('TIME')||type.includes('DATE');
            }) || cols[0];
            xSel.value = defX || '';
            window._chartReportXCol = xSel.value || null;
        }
    }

    // Y2
    const y2Sel = document.getElementById('cr-y2-select');
    if (y2Sel && cols.length) {
        const prev = y2Sel.value;
        y2Sel.innerHTML = '<option value="">— none —</option>' + cols.map(c=>`<option value="${_crEsc(c)}">${_crEsc(c)}</option>`).join('');
        if (prev && cols.includes(prev)) y2Sel.value = prev;
    }

    // Auto first Y
    if (cols.length && !window._chartReportDefs.length) {
        const defY = cols.length > 1 ? cols[1] : cols[0];
        window._chartReportDefs = [{ field: defY, color: CHART_COLORS_PALETTE[0], agg: 'none', label: defY }];
    }

    _crUpdatePanelForType(window._chartReportType);
    _crRenderFieldList();
    _crWaitForChartJs(_crRenderChart);
}

function _crOnXAxisChange() {
    const xSel = document.getElementById('cr-xaxis-select');
    window._chartReportXCol = xSel ? (xSel.value || null) : null;
    _crRenderChart();
}

function _crSelectType(type, btn) {
    window._chartReportType = type;
    document.querySelectorAll('.cr-type-btn').forEach(b => b.classList.remove('cr-type-active'));
    if (btn) btn.classList.add('cr-type-active');
    _crUpdatePanelForType(type);
    _crRenderChart();
}

function _crUpdatePanelForType(type) {
    const xWrap  = document.getElementById('cr-xaxis-wrap');
    const y2Wrap = document.getElementById('cr-y2-wrap');
    const yLabel = document.getElementById('cr-yaxis-label');
    const addBtn = document.getElementById('cr-add-field-btn');
    if (xWrap)  xWrap.style.display  = (type === 'correlation') ? 'none' : 'block';
    if (y2Wrap) y2Wrap.style.display = (type === 'dualaxis')    ? 'block' : 'none';
    if (yLabel) yLabel.textContent   = type === 'correlation' ? 'Numeric Fields (≥ 2)' :
        type === 'dualaxis'   ? 'Y1 Axis (Left Scale)' : 'Y Axis (Values)';
    if (addBtn) addBtn.style.display =
        ['pie','donut','heatmap','correlation','dualaxis'].includes(type) ? 'none' : 'block';
}

function _crRenderFieldList() {
    const container = document.getElementById('cr-field-list');
    if (!container) return;
    const cols = _crGetColumns(window._chartReportSlotId);
    if (!cols.length) { container.innerHTML = '<div style="font-size:11px;color:var(--text3);">No columns available</div>'; return; }
    const defs = window._chartReportDefs;
    if (!defs.length) { container.innerHTML = '<div style="font-size:11px;color:var(--text3);">Click ＋ Add Y Field</div>'; return; }

    container.innerHTML = defs.map((def, idx) => {
        const colOpts = cols.map(c=>`<option value="${_crEsc(c)}" ${c===def.field?'selected':''}>${_crEsc(c)}</option>`).join('');
        return `<div style="background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:7px 8px;display:flex;flex-direction:column;gap:5px;">
      <div style="display:flex;align-items:center;gap:6px;">
        <input type="color" value="${def.color}" onchange="window._chartReportDefs[${idx}].color=this.value;_crRenderChart()"
          style="width:24px;height:24px;border:1px solid var(--border);border-radius:3px;cursor:pointer;padding:1px;background:var(--bg1);flex-shrink:0;"/>
        <select onchange="window._chartReportDefs[${idx}].field=this.value;window._chartReportDefs[${idx}].label=this.value;_crRenderChart()"
          style="flex:1;font-size:11px;padding:3px 5px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text0);border-radius:3px;cursor:pointer;">${colOpts}</select>
        <button onclick="window._chartReportDefs.splice(${idx},1);_crRenderFieldList();_crRenderChart()"
          style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:14px;padding:0;line-height:1;flex-shrink:0;">×</button>
      </div>
      <div style="display:flex;gap:4px;align-items:center;">
        <select onchange="window._chartReportDefs[${idx}].agg=this.value;_crRenderChart()"
          style="font-size:10px;padding:2px 4px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text1);border-radius:3px;cursor:pointer;flex:1;">
          <option value="none"  ${def.agg==='none' ?'selected':''}>Raw values</option>
          <option value="count" ${def.agg==='count'?'selected':''}>COUNT</option>
          <option value="sum"   ${def.agg==='sum'  ?'selected':''}>SUM</option>
          <option value="avg"   ${def.agg==='avg'  ?'selected':''}>AVG</option>
          <option value="max"   ${def.agg==='max'  ?'selected':''}>MAX</option>
          <option value="min"   ${def.agg==='min'  ?'selected':''}>MIN</option>
        </select>
        <input type="text" value="${_crEsc(def.label)}" placeholder="label"
          onchange="window._chartReportDefs[${idx}].label=this.value;_crRenderChart()"
          style="font-size:10px;padding:2px 5px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text0);border-radius:3px;flex:1;min-width:0;"/>
      </div>
    </div>`;
    }).join('');
}

function _crAddField() {
    const cols = _crGetColumns(window._chartReportSlotId);
    if (!cols.length) return;
    const used  = window._chartReportDefs.map(d => d.field);
    const next  = cols.find(c => !used.includes(c)) || cols[0];
    const color = CHART_COLORS_PALETTE[window._chartReportDefs.length % CHART_COLORS_PALETTE.length];
    window._chartReportDefs.push({ field: next, color, agg: 'none', label: next });
    _crRenderFieldList(); _crRenderChart();
}

function _crToggleLive() {
    const chk = document.getElementById('cr-live-chk');
    if (!chk) return;
    if (chk.checked) {
        window._chartReportTimer = setInterval(() => {
            if (document.getElementById('modal-chart-report')?.classList.contains('open')) _crRenderChart();
            else _crStopLive();
        }, 2000);
    } else _crStopLive();
}
function _crStopLive() {
    if (window._chartReportTimer) { clearInterval(window._chartReportTimer); window._chartReportTimer = null; }
}

// ═══════════════════════════════════════════════════════════════════════════
// RENDER DISPATCH
// ═══════════════════════════════════════════════════════════════════════════

function _crRenderChart() {
    const canvas   = document.getElementById('cr-chart-canvas');
    const emptyEl  = document.getElementById('cr-chart-empty');
    const statusEl = document.getElementById('cr-chart-status');
    const legendEl = document.getElementById('cr-chart-legend');
    if (!canvas) return;

    const defs = window._chartReportDefs;
    const type = window._chartReportType;
    const cols = _crGetColumns(window._chartReportSlotId);
    const rows = _crGetRows(window._chartReportSlotId);

    if (!rows.length || !cols.length) {
        if (emptyEl) emptyEl.style.display = 'flex';
        if (statusEl) statusEl.textContent = 'No data — run a SELECT query first';
        return;
    }
    if (!defs.length && type !== 'correlation') {
        if (emptyEl) emptyEl.style.display = 'flex';
        if (statusEl) statusEl.textContent = 'Add a Y field to chart';
        return;
    }
    if (emptyEl) emptyEl.style.display = 'none';

    if (window._chartReportInst) { try { window._chartReportInst.destroy(); } catch(_){} window._chartReportInst = null; }

    const maxRows = parseInt(document.getElementById('cr-max-rows')?.value || '200') || 200;
    const data    = rows.slice(-maxRows);

    if (statusEl) statusEl.textContent =
        `${data.length} rows · ${CHART_TYPES.find(t=>t.value===type)?.label||type} · scroll to zoom`;

    if (type === 'heatmap')     return _crDrawHeatmapChart(canvas, cols, data, defs, legendEl);
    if (type === 'correlation') return _crDrawCorrelationMatrix(canvas, cols, data, defs, legendEl, statusEl);
    if (type === 'dualaxis')    return _crDrawDualAxis(canvas, cols, data, defs, legendEl);
    if (typeof Chart === 'undefined') { if (statusEl) statusEl.textContent = '⏳ Chart.js loading…'; return; }
    _crDrawStandardChart(canvas, cols, data, defs, type, legendEl);
}

// ═══════════════════════════════════════════════════════════════════════════
// STANDARD CHART.JS
// ═══════════════════════════════════════════════════════════════════════════

function _crDrawStandardChart(canvas, cols, data, defs, type, legendEl) {
    const xColName   = window._chartReportXCol || (document.getElementById('cr-xaxis-select')?.value || null);
    const xIdx       = xColName ? cols.indexOf(xColName) : -1;
    const isDark     = !document.body.classList.contains('theme-light');
    const gridColor  = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.07)';
    const tickColor  = isDark ? '#8b949e' : '#666';
    const isMulti    = ['pie','doughnut'].includes(type==='donut'?'doughnut':type);
    const useAgg     = def_agg_any(defs);

    let chartLabels, useData = data;
    if (useAgg) {
        const groups = {};
        data.forEach((row,i) => { const k = xIdx>=0 ? String(row[xIdx]??'null') : String(i+1); if(!groups[k])groups[k]=[]; groups[k].push(row); });
        const entries = Object.entries(groups).slice(0,100);
        chartLabels = entries.map(e=>e[0]); useData = entries.map(e=>e[1]);
    } else {
        chartLabels = data.map((row,i) => xIdx>=0 ? String(row[xIdx]??i).slice(0,24) : String(i+1));
    }

    const datasets = defs.map(def => {
        const yIdx = cols.indexOf(def.field);
        let values;
        if (useAgg) {
            values = useData.map(rg => {
                const nums = rg.map(r => parseFloat(r[yIdx>=0?yIdx:0])||0);
                switch(def.agg){case 'count':return nums.length;case 'sum':return nums.reduce((a,b)=>a+b,0);
                    case 'avg':return nums.reduce((a,b)=>a+b,0)/nums.length;
                    case 'max':return Math.max(...nums);case 'min':return Math.min(...nums);default:return nums[nums.length-1];}
            });
        } else {
            values = data.map(row => { const v=row[yIdx>=0?yIdx:0]; return v!=null?(parseFloat(v)||0):0; });
        }
        const scatter = type==='scatter' ? data.map((row,i)=>({x:xIdx>=0?(parseFloat(row[xIdx])||i):i,y:yIdx>=0?(parseFloat(row[yIdx])||0):0})) : null;
        return {
            label: def.label||def.field, data: scatter||values,
            backgroundColor: isMulti ? CHART_COLORS_PALETTE.map(c=>c+'99') : def.color+'40',
            borderColor:     isMulti ? CHART_COLORS_PALETTE : def.color,
            borderWidth:2, fill:type==='area', tension:0.3,
            pointRadius: type==='scatter'?4:(data.length>150?0:2), pointBackgroundColor:def.color,
        };
    });

    let cjsType = type;
    if (type==='area')      cjsType='line';
    if (type==='histogram') cjsType='bar';
    if (type==='donut')     cjsType='doughnut';

    const ctx = canvas.getContext('2d');
    window._chartReportInst = new Chart(ctx, {
        type: cjsType, data: { labels: chartLabels, datasets },
        options: {
            responsive:true, maintainAspectRatio:false, animation:{duration:150},
            plugins: {
                legend: { display: defs.length>1||isMulti, position:'bottom',
                    labels:{color:tickColor,font:{size:11,family:'IBM Plex Mono'},boxWidth:12} },
                tooltip: {
                    backgroundColor:isDark?'#131920':'#fff', borderColor:isDark?'#253348':'#ddd', borderWidth:1,
                    titleColor:isDark?'#f0f6fc':'#111', bodyColor:isDark?'#c9d1d9':'#444',
                    callbacks:{title:(items)=>{ const lbl=items[0]?.label||''; return xColName?`${xColName}: ${lbl}`:`Row ${lbl}`; }},
                },
            },
            scales: isMulti ? {} : {
                x: { title:{display:!!xColName,text:xColName||'',color:tickColor,font:{size:10}},
                    ticks:{color:tickColor,font:{size:10},maxRotation:45,maxTicksLimit:25}, grid:{color:gridColor} },
                y: { title:{display:defs.length===1,text:defs[0]?.label||defs[0]?.field||'',color:tickColor,font:{size:10}},
                    ticks:{color:tickColor,font:{size:10}}, grid:{color:gridColor} },
            },
        },
    });
    if (legendEl) _crRenderLegend(legendEl, defs);
}

// ═══════════════════════════════════════════════════════════════════════════
// NEW: DUAL-AXIS TIME SERIES (⇅)
// Two Y fields on separate left/right axes — great for comparing metrics
// with very different scales in a live streaming pipeline view.
// ═══════════════════════════════════════════════════════════════════════════

function _crDrawDualAxis(canvas, cols, data, defs, legendEl) {
    if (typeof Chart === 'undefined') return;
    const xColName = window._chartReportXCol || (document.getElementById('cr-xaxis-select')?.value || null);
    const xIdx     = xColName ? cols.indexOf(xColName) : -1;
    const y2Field  = document.getElementById('cr-y2-select')?.value || null;
    const isDark   = !document.body.classList.contains('theme-light');
    const grid     = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.07)';
    const tick     = isDark ? '#8b949e' : '#666';

    const labels = data.map((row,i) => xIdx>=0 ? String(row[xIdx]??i).slice(0,24) : String(i+1));
    const def1   = defs[0] || { field:cols[0], color:CHART_COLORS_PALETTE[0], label:cols[0] };
    const y1Idx  = cols.indexOf(def1.field);
    const y2Idx  = y2Field ? cols.indexOf(y2Field) : -1;
    const y2Color = CHART_COLORS_PALETTE[1];

    const datasets = [{
        label: def1.label||def1.field,
        data:  data.map(row => parseFloat(row[y1Idx>=0?y1Idx:0])||0),
        borderColor:def1.color, backgroundColor:def1.color+'25',
        borderWidth:2, fill:false, tension:0.3,
        pointRadius: data.length>150?0:2, yAxisID:'y1',
    }];
    if (y2Idx >= 0) datasets.push({
        label: y2Field,
        data:  data.map(row => parseFloat(row[y2Idx])||0),
        borderColor:y2Color, backgroundColor:y2Color+'25',
        borderWidth:2, fill:false, tension:0.3, borderDash:[5,3],
        pointRadius: data.length>150?0:2, yAxisID:'y2',
    });

    const ctx = canvas.getContext('2d');
    window._chartReportInst = new Chart(ctx, {
        type:'line', data:{labels,datasets},
        options:{
            responsive:true, maintainAspectRatio:false, animation:{duration:150},
            interaction:{mode:'index',intersect:false},
            plugins:{
                legend:{display:true,position:'bottom',labels:{color:tick,font:{size:11,family:'IBM Plex Mono'},boxWidth:12}},
                tooltip:{backgroundColor:isDark?'#131920':'#fff',borderColor:isDark?'#253348':'#ddd',borderWidth:1,
                    titleColor:isDark?'#f0f6fc':'#111',bodyColor:isDark?'#c9d1d9':'#444'},
            },
            scales:{
                x:{ ticks:{color:tick,font:{size:10},maxRotation:45,maxTicksLimit:25}, grid:{color:grid},
                    title:{display:!!xColName,text:xColName||'',color:tick,font:{size:10}} },
                y1:{ type:'linear',position:'left',
                    title:{display:true,text:def1.label||def1.field,color:def1.color,font:{size:10}},
                    ticks:{color:def1.color,font:{size:10}}, grid:{color:grid} },
                y2:{ type:'linear',position:'right',
                    title:{display:!!y2Field,text:y2Field||'',color:y2Color,font:{size:10}},
                    ticks:{color:y2Color,font:{size:10}}, grid:{drawOnChartArea:false} },
            },
        },
    });
    if (legendEl) legendEl.innerHTML = datasets.map(d=>`
        <div style="display:flex;align-items:center;gap:5px;color:var(--text2);">
          <div style="width:18px;height:2px;background:${d.borderColor};flex-shrink:0;${d.borderDash?'border-top:2px dashed '+d.borderColor+';background:none;':''}"></div>
          <span>${_crEsc(d.label)}</span>
          <span style="font-size:9px;color:var(--text3);">(${d.yAxisID==='y1'?'left':'right'} axis)</span>
        </div>`).join('');
}

// ═══════════════════════════════════════════════════════════════════════════
// NEW: CORRELATION MATRIX (⬡)
// Pearson r between every pair of selected numeric columns.
// Colour-coded grid: teal = positive, red = negative, dim = none.
// Auto-detects numeric columns when < 2 Y fields are configured.
// ═══════════════════════════════════════════════════════════════════════════

function _crDrawCorrelationMatrix(canvas, cols, data, defs, legendEl, statusEl) {
    // Determine columns to correlate
    let fields = defs.length >= 2
        ? defs.map(d=>d.field).filter(f=>cols.includes(f))
        : cols.filter(c => {
            const vals = data.map(row=>parseFloat(row[cols.indexOf(c)])).filter(v=>!isNaN(v));
            return vals.length > data.length * 0.4;
        }).slice(0, 12);

    if (fields.length < 2) {
        if (statusEl) statusEl.textContent = '⬡ Correlation needs ≥ 2 numeric columns — add Y fields or check data';
        return;
    }

    // Pearson r
    const getVals = f => data.map(row => parseFloat(row[cols.indexOf(f)])).filter(v=>!isNaN(v));
    const series  = fields.map(getVals);
    function pearson(xs, ys) {
        const len = Math.min(xs.length, ys.length); if (len < 2) return 0;
        const xm = xs.slice(0,len).reduce((a,b)=>a+b,0)/len;
        const ym = ys.slice(0,len).reduce((a,b)=>a+b,0)/len;
        let num=0,sx=0,sy=0;
        for (let i=0;i<len;i++){const dx=xs[i]-xm,dy=ys[i]-ym;num+=dx*dy;sx+=dx*dx;sy+=dy*dy;}
        const d=Math.sqrt(sx*sy); return d===0?0:num/d;
    }
    const n   = fields.length;
    const mat = Array.from({length:n},(_,i)=>Array.from({length:n},(_,j)=>i===j?1.0:pearson(series[i],series[j])));

    // Draw
    const isDark = !document.body.classList.contains('theme-light');
    const ctx = canvas.getContext('2d');
    const inner = document.getElementById('cr-canvas-inner');
    const W = canvas.width  = inner ? inner.offsetWidth  || 720 : 720;
    const H = canvas.height = inner ? inner.offsetHeight || 520 : 520;
    ctx.clearRect(0,0,W,H);

    const pad  = {t:24,b:90,l:100,r:24};
    const cellW = Math.floor((W-pad.l-pad.r)/n);
    const cellH = Math.floor((H-pad.t-pad.b)/n);
    const fs    = Math.max(8, Math.min(13, Math.floor(cellW*0.26)));
    const bright = isDark ? '#e8edf5' : '#111';
    const dim    = isDark ? '#6a7f96' : '#888';

    function rColor(r) {
        const a = Math.abs(r), alpha = 0.12 + a*0.88;
        if (r >= 0) return `rgba(0,${Math.round(80+a*132)},${Math.round(80+a*90)},${alpha})`;
        return `rgba(${Math.round(100+a*155)},${Math.round(30+a*47)},${Math.round(30+a*79)},${alpha})`;
    }

    for (let i=0;i<n;i++) for (let j=0;j<n;j++) {
        const r=mat[i][j], x=pad.l+j*cellW, y=pad.t+i*cellH;
        ctx.fillStyle=rColor(r); ctx.fillRect(x,y,cellW,cellH);
        ctx.strokeStyle=isDark?'rgba(255,255,255,0.04)':'rgba(0,0,0,0.06)'; ctx.lineWidth=0.5; ctx.strokeRect(x,y,cellW,cellH);
        if (cellW > 28) {
            ctx.fillStyle=Math.abs(r)>0.45?bright:dim;
            ctx.font=`${i===j?'bold ':''}${fs}px monospace`;
            ctx.textAlign='center'; ctx.textBaseline='middle';
            ctx.fillText(r.toFixed(2), x+cellW/2, y+cellH/2);
        }
    }

    // Labels
    ctx.font=`${fs}px monospace`; ctx.fillStyle=dim;
    for (let j=0;j<n;j++) {
        ctx.save(); ctx.translate(pad.l+j*cellW+cellW/2, pad.t+n*cellH+6);
        ctx.rotate(Math.PI/5); ctx.textAlign='left'; ctx.textBaseline='top';
        ctx.fillText(fields[j].slice(0,16),0,0); ctx.restore();
    }
    ctx.textAlign='right'; ctx.textBaseline='middle';
    for (let i=0;i<n;i++) ctx.fillText(fields[i].slice(0,14), pad.l-6, pad.t+i*cellH+cellH/2);

    // Gradient scale bar
    const bx=pad.l, by=H-20, bw=W-pad.l-pad.r, bh=10;
    const grad=ctx.createLinearGradient(bx,0,bx+bw,0);
    grad.addColorStop(0,'rgba(255,77,109,0.9)');
    grad.addColorStop(0.5,isDark?'rgba(18,26,36,0.5)':'rgba(210,218,226,0.6)');
    grad.addColorStop(1,'rgba(0,212,170,0.9)');
    ctx.fillStyle=grad; ctx.fillRect(bx,by,bw,bh);
    ctx.strokeStyle='rgba(255,255,255,0.08)'; ctx.lineWidth=0.5; ctx.strokeRect(bx,by,bw,bh);
    ctx.font='9px monospace'; ctx.fillStyle=dim;
    ctx.textBaseline='bottom';
    ctx.textAlign='left';   ctx.fillText('−1.0  negative',bx,by-1);
    ctx.textAlign='center'; ctx.fillText('0  no correlation',bx+bw/2,by-1);
    ctx.textAlign='right';  ctx.fillText('+1.0  positive',bx+bw,by-1);

    if (statusEl) statusEl.textContent=`Correlation matrix · ${n} fields · ${data.length} rows · scroll to zoom`;
    if (legendEl) legendEl.innerHTML=`<span style="font-size:10px;color:var(--text3);">Fields: ${
        fields.map(f=>`<span style="color:var(--text1);font-family:var(--mono);">${_crEsc(f)}</span>`).join(', ')
    } &nbsp;·&nbsp; <span style="color:#00d4aa;">■</span> positive &nbsp;<span style="color:#ff4d6d;">■</span> negative &nbsp;<span style="color:var(--text3);">■</span> none</span>`;
}

// ── Heatmap ───────────────────────────────────────────────────────────────────
function _crDrawHeatmapChart(canvas, cols, data, defs, legendEl) {
    const def    = defs[0];
    const xColNm = window._chartReportXCol || (document.getElementById('cr-xaxis-select')?.value||null);
    const xIdx   = xColNm ? cols.indexOf(xColNm) : -1;
    const yIdx   = cols.indexOf(def.field);
    const labels = data.map((row,i) => xIdx>=0 ? String(row[xIdx]??i).slice(0,15) : String(i+1));
    const values = data.map(row => parseFloat(row[yIdx>=0?yIdx:0])||0);

    const ctx = canvas.getContext('2d');
    const inner = document.getElementById('cr-canvas-inner');
    const W = canvas.width  = inner ? inner.offsetWidth  || 820 : 820;
    const H = canvas.height = inner ? inner.offsetHeight || 480 : 480;
    const pad={t:20,b:58,l:58,r:20};
    ctx.clearRect(0,0,W,H);
    const maxV=Math.max(...values,1), minV=Math.min(...values,0), range=maxV-minV||1;
    const n=Math.min(labels.length,80), cw=(W-pad.l-pad.r)/n, ch=H-pad.t-pad.b;
    const clr=def.color, r=parseInt(clr.slice(1,3),16)||0, g=parseInt(clr.slice(3,5),16)||212, b=parseInt(clr.slice(5,7),16)||170;
    for (let i=0;i<n;i++){
        const norm=(values[i]-minV)/range, alpha=0.1+norm*0.9, x=pad.l+i*cw;
        ctx.fillStyle=`rgba(${r},${g},${b},${alpha})`; ctx.fillRect(x+1,pad.t,cw-2,ch);
        ctx.fillStyle=norm>0.5?'#fff':'#8b949e'; ctx.font=`${Math.max(8,Math.min(11,cw-4))}px monospace`;
        ctx.textAlign='center'; ctx.textBaseline='middle';
        if (cw>22) ctx.fillText(String(values[i]).slice(0,6),x+cw/2,pad.t+ch/2);
        ctx.fillStyle='#8b949e'; ctx.font='9px monospace'; ctx.textBaseline='top';
        ctx.save(); ctx.translate(x+cw/2,H-pad.b+4); if(n>8)ctx.rotate(-Math.PI/5);
        ctx.fillText(String(labels[i]).slice(0,8),0,0); ctx.restore();
    }
    ctx.strokeStyle='#253348'; ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,H-pad.b); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(pad.l,H-pad.b); ctx.lineTo(W-pad.r,H-pad.b); ctx.stroke();
    if (legendEl) _crRenderLegend(legendEl, defs);
}

// ── Legend & PDF ──────────────────────────────────────────────────────────────
function _crRenderLegend(el, defs) {
    if (!el) return;
    const xCN = window._chartReportXCol;
    el.innerHTML = (xCN?`<div style="display:flex;align-items:center;gap:4px;color:var(--text3);font-size:10px;"><span style="font-weight:700;color:var(--text2);">X:</span> ${_crEsc(xCN)}</div><span style="color:var(--border);margin:0 4px;">|</span>`:'')
        + defs.map(d=>`<div style="display:flex;align-items:center;gap:5px;color:var(--text2);">
      <div style="width:10px;height:10px;border-radius:2px;background:${d.color};flex-shrink:0;"></div>
      <span style="font-size:10px;font-weight:700;color:var(--text3);">Y:</span>
      <span>${_crEsc(d.label||d.field)}</span>
      ${d.agg&&d.agg!=='none'?`<span style="font-size:9px;color:var(--text3);">(${d.agg.toUpperCase()})</span>`:''}
    </div>`).join('');
}

function _crExportPDF() {
    const canvas=document.getElementById('cr-chart-canvas');
    if (!canvas){if(typeof toast==='function')toast('No chart to export','err');return;}
    const img=canvas.toDataURL('image/png');
    const type=CHART_TYPES.find(t=>t.value===window._chartReportType)?.label||window._chartReportType;
    const xCol=window._chartReportXCol||'row index';
    const yFields=window._chartReportDefs.map(d=>d.label||d.field).join(', ');
    const win=window.open('','_blank');
    if (!win){if(typeof toast==='function')toast('Allow popups for PDF export','err');return;}
    win.document.write(`<!DOCTYPE html><html><head><title>Chart Report — Str:::lab Studio</title>
    <style>body{font-family:'IBM Plex Mono',monospace;padding:32px;background:#090c12;color:#f0f6fc;}
    h1{font-size:18px;color:#00d4aa;margin-bottom:4px;}p{font-size:12px;color:#8b949e;margin:0 0 20px;}
    img{max-width:100%;border:1px solid #1e2d3d;border-radius:8px;}
    @media print{body{background:#fff;color:#111;}}</style></head><body>
    <h1>📊 Chart Report</h1>
    <p>Type: ${type} · X: ${xCol} · Y: ${yFields} · Generated: ${new Date().toLocaleString()} · Str:::lab Studio</p>
    <img src="${img}"/><script>window.onload=()=>window.print();<\/script></body></html>`);
    win.document.close();
    if(typeof toast==='function')toast('Print dialog — save as PDF','ok');
}

// ── CSS ───────────────────────────────────────────────────────────────────────
(function(){
    if (document.getElementById('cr-styles')) return;
    const s=document.createElement('style'); s.id='cr-styles';
    s.textContent=`.cr-type-btn{transition:background 0.15s,border-color 0.15s,color 0.15s;}
    .cr-type-active{border-color:var(--accent)!important;background:rgba(0,212,170,0.12)!important;color:var(--accent)!important;}
    .cr-type-btn:hover{background:var(--bg3)!important;}
    #cr-canvas-outer{user-select:none;scrollbar-width:thin;scrollbar-color:var(--border) transparent;}
    #cr-canvas-outer::-webkit-scrollbar{width:5px;height:5px;}
    #cr-canvas-outer::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px;}`;
    document.head.appendChild(s);
})();

// ── Patch renderResults ───────────────────────────────────────────────────────
(function(){
    function patch(){
        if(typeof renderResults!=='function'||renderResults._crPatched)return false;
        const _o=renderResults;
        window.renderResults=function(){
            _o.apply(this,arguments);
            const m=document.getElementById('modal-chart-report');
            if(m&&m.classList.contains('open'))setTimeout(_crPopulateSlots,150);
        };
        renderResults._crPatched=true; return true;
    }
    if(!patch()){const t=setInterval(()=>{if(patch())clearInterval(t);},500);}
})();

// ── openModal hook ────────────────────────────────────────────────────────────
(function(){
    const _oo=window.openModal;
    window.openModal=function(id){
        if(typeof _oo==='function')_oo.apply(this,arguments);
        if(id==='modal-chart-report'){
            _crStopLive();
            setTimeout(_crInstallZoomControls,150);
            window._chartZoomLevel=1.0; _crApplyZoom();
            const chk=document.getElementById('cr-live-chk');
            if(!chk||chk.checked){
                window._chartReportTimer=setInterval(()=>{
                    const m=document.getElementById('modal-chart-report');
                    if(m&&m.classList.contains('open')){if(typeof Chart!=='undefined')_crRenderChart();}
                    else _crStopLive();
                },2000);
            }
        }
    };
})();

window.openChartReportModal=openChartReportModal;