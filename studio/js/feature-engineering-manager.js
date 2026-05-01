/**
 * feature-engineering-manager.js  —  Str:::lab Studio
 * Feature Engineering Manager with validation, history, and canvas view
 */

const _FEM = {
    step: 0,
    schemaMode: 'manual',
    srConnected: false,
    srSubjects: [],
    srSchemaCache: {},
    srUrl: '',
    srAuth: '',
    sourceTable: '',
    sourceFormat: 'kafka',
    rawColumns: [],
    selectedFeatures: [],
    transformations: [],
    windows: [],
    noWinGroupBy: '',
    noWinHaving: '',
    noWinAggs: '',
    timeCol: '',
    watermarkDelay: '5 SECOND',
    outputTable: '',
    sinkType: 'kafka',
    generatedSql: '',
    history: [],
};

// ── Load history from localStorage
(function () {
    try {
        const raw = localStorage.getItem('strlabstudio_fem_history');
        if (raw) _FEM.history = JSON.parse(raw);
    } catch (_) {}
})();

function _femSaveHistory() {
    try {
        // Build the output column schema from the generated SQL + transformations
        // so the Inference Manager can auto-load them without re-parsing SQL
        const outCols = _femDeriveOutputColumns();

        const entry = {
            id: 'fem_' + Date.now(),
            ts: new Date().toISOString(),
            sourceTable: _FEM.sourceTable,
            outputTable: _FEM.outputTable,
            sinkType: _FEM.sinkType,
            columnCount: _FEM.selectedFeatures.length || _FEM.rawColumns.length,
            windowCount: _FEM.windows.length,
            transformCount: _FEM.transformations.filter(t => t.transformId !== 'passthrough').length,
            sql: _FEM.generatedSql,
            // ── Schema snapshots so IFM can directly load columns ──
            sourceColumns: _FEM.rawColumns,
            outputColumns: outCols,
        };
        _FEM.history.unshift(entry);
        if (_FEM.history.length > 20) _FEM.history.pop();
        localStorage.setItem('strlabstudio_fem_history', JSON.stringify(_FEM.history));

        // Also write a compact "last pipeline state" key for quick IFM access
        const state = {
            sourceTable:   _FEM.sourceTable,
            outputTable:   _FEM.outputTable,
            sourceColumns: _FEM.rawColumns,
            outputColumns: outCols,
            savedAt:       entry.ts,
        };
        localStorage.setItem('strlabstudio_fem_state', JSON.stringify(state));
    } catch (_) {}
}

// Derive the output column schema (what ends up in the sink table)
// from transformations + window aggregations so IFM can use it directly.
function _femDeriveOutputColumns() {
    const cols = [];
    const seenNames = new Set();
    const add = (name, type) => {
        if (!name || seenNames.has(name)) return;
        seenNames.add(name);
        cols.push({ name, type: type || 'STRING' });
    };

    // Passthrough / transformed feature columns
    const txList = _FEM.transformations.filter(t => t.colName !== '_expr_');
    if (txList.length) {
        txList.forEach(t => {
            const alias = t.alias || t.colName;
            const srcCol = (_FEM.selectedFeatures.length ? _FEM.selectedFeatures : _FEM.rawColumns)
                .find(c => c.name === t.colName);
            // Derive output type from transform kind
            let outType = srcCol?.type || 'STRING';
            if (t.transformId === 'cast')        outType = t.param || 'DOUBLE';
            if (t.transformId === 'round')       outType = 'DOUBLE';
            if (t.transformId === 'abs')         outType = srcCol?.type || 'DOUBLE';
            if (t.transformId === 'log')         outType = 'DOUBLE';
            if (t.transformId === 'hash_encode') outType = 'STRING';
            if (t.transformId === 'upper' || t.transformId === 'lower' || t.transformId === 'trim'
                || t.transformId === 'substring' || t.transformId === 'date_format'
                || t.transformId === 'bucket')   outType = 'STRING';
            if (t.transformId === 'ts_to_epoch') outType = 'BIGINT';
            if (t.transformId === 'epoch_to_ts') outType = 'STRING';
            add(alias, outType);
        });
    } else {
        // No transforms — pass all selected features through as-is
        (_FEM.selectedFeatures.length ? _FEM.selectedFeatures : _FEM.rawColumns).forEach(c => add(c.name, c.type));
    }

    // Custom expression columns
    _FEM.transformations.filter(t => t.colName === '_expr_' && t.param).forEach(t => {
        // Try to extract alias from "… AS alias" pattern
        const m = t.param.match(/\bAS\s+(\w+)\s*$/i);
        if (m) add(m[1], 'STRING');
    });

    // Window aggregation columns (window_start, window_end + group_by + aggs)
    _FEM.windows.forEach((w, wi) => {
        add('window_start', 'TIMESTAMP(3)');
        add('window_end',   'TIMESTAMP(3)');
        if (w.groupBy) w.groupBy.split(',').map(c => c.trim()).filter(Boolean).forEach(c => {
            const srcCol = (_FEM.rawColumns || []).find(rc => rc.name === c);
            add(c, srcCol?.type || 'STRING');
        });
        if (w.aggs) w.aggs.split('\n').map(l => l.trim()).filter(Boolean).forEach(agg => {
            const m = agg.match(/\bAS\s+(\w+)\s*$/i);
            if (m) {
                const aggUpper = agg.toUpperCase();
                let type = 'BIGINT';
                if (/SUM|AVG|STDDEV|VAR/.test(aggUpper)) type = 'DOUBLE';
                if (/MAX|MIN/.test(aggUpper)) {
                    // Inherit type from the source column if identifiable
                    const colM = agg.match(/\((\w+)\)/);
                    const srcC = colM && (_FEM.rawColumns || []).find(rc => rc.name === colM[1]);
                    type = srcC?.type || 'DOUBLE';
                }
                add(m[1], type);
            }
        });
    });

    // No-window group aggregation
    if (_FEM.noWinGroupBy?.trim()) {
        _FEM.noWinGroupBy.split(',').map(c => c.trim()).filter(Boolean).forEach(c => {
            const srcCol = (_FEM.rawColumns || []).find(rc => rc.name === c);
            add(c, srcCol?.type || 'STRING');
        });
    }
    if (_FEM.noWinAggs?.trim()) {
        _FEM.noWinAggs.split('\n').map(l => l.trim()).filter(Boolean).forEach(agg => {
            const m = agg.match(/\bAS\s+(\w+)\s*$/i);
            if (m) {
                const aggUpper = agg.toUpperCase();
                let type = 'BIGINT';
                if (/SUM|AVG|STDDEV|VAR/.test(aggUpper)) type = 'DOUBLE';
                add(m[1], type);
            }
        });
    }

    return cols;
}

function openFeatureEngineeringManager() {
    if (!document.getElementById('fem-modal')) _femBuildModal();
    openModal('fem-modal');
    _femGoStep(0);
}

function _femInjectCss() {
    if (document.getElementById('fem-css')) return;
    const s = document.createElement('style');
    s.id = 'fem-css';
    s.textContent = `
#fem-modal .modal{width:min(1180px,97vw);height:90vh;max-height:90vh;display:flex;flex-direction:column;overflow:hidden;}
#fem-wizard-wrap{flex:1;display:flex;flex-direction:column;overflow:hidden;}
#fem-step-bar{display:flex;align-items:center;gap:0;background:var(--bg2);border-bottom:1px solid var(--border);padding:0 16px;flex-shrink:0;overflow-x:auto;}
.fem-step-item{display:flex;align-items:center;gap:6px;padding:9px 12px;font-size:11px;font-family:var(--mono);color:var(--text3);white-space:nowrap;cursor:pointer;border-bottom:2px solid transparent;transition:all 0.15s;user-select:none;}
.fem-step-item.active{color:var(--accent);border-bottom-color:var(--accent);font-weight:700;}
.fem-step-item.done{color:var(--green);}
.fem-step-item.done .fem-step-num{background:var(--green);color:#000;}
.fem-step-item.invalid{color:var(--red);}
.fem-step-num{width:17px;height:17px;border-radius:50%;background:var(--bg3);color:var(--text3);font-size:9px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;}
.fem-step-item.active .fem-step-num{background:var(--accent);color:#000;}
.fem-step-connector{width:20px;height:1px;background:var(--border);flex-shrink:0;}
#fem-body{flex:1;overflow-y:auto;padding:20px 24px;}
#fem-footer{flex-shrink:0;padding:11px 20px;border-top:1px solid var(--border);background:var(--bg2);display:flex;align-items:center;gap:8px;}
.fem-section-title{font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:9px;padding-bottom:4px;border-bottom:1px solid var(--border);}
.fem-card{background:var(--bg2);border:1px solid var(--border);border-radius:5px;padding:13px 15px;margin-bottom:11px;}
.fem-radio-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:13px;}
.fem-radio-btn{display:flex;align-items:center;gap:7px;padding:7px 13px;border:1px solid var(--border2);border-radius:4px;cursor:pointer;font-size:11px;font-family:var(--mono);color:var(--text1);background:var(--bg3);transition:all 0.12s;user-select:none;}
.fem-radio-btn:hover{background:var(--bg1);border-color:var(--accent);}
.fem-radio-btn.selected{background:rgba(0,212,170,0.1);border-color:var(--accent);color:var(--accent);font-weight:700;}
.fem-col-tag{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:3px;font-size:10px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;transition:all 0.1s;user-select:none;margin:2px;}
.fem-col-tag.selected{background:rgba(0,212,170,0.12);border-color:rgba(0,212,170,0.4);color:var(--accent);}
.fem-col-tag .fem-tag-type{font-size:8px;opacity:0.55;margin-left:2px;}
.fem-transform-row{display:flex;align-items:center;gap:7px;padding:7px 9px;background:var(--bg0);border:1px solid var(--border);border-radius:4px;margin-bottom:6px;font-size:11px;font-family:var(--mono);}
.fem-transform-row .fem-col-name{color:var(--accent);font-weight:700;min-width:100px;overflow:hidden;text-overflow:ellipsis;}
.fem-transform-row select,.fem-transform-row input{background:var(--bg3);border:1px solid var(--border2);color:var(--text0);font-family:var(--mono);font-size:10px;padding:3px 5px;border-radius:3px;outline:none;}
.fem-btn-add{font-size:10px;padding:3px 10px;border-radius:3px;background:rgba(0,212,170,0.08);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;font-family:var(--mono);}
.fem-btn-del{background:none;border:none;color:var(--red);cursor:pointer;font-size:14px;line-height:1;padding:0 3px;flex-shrink:0;}
.fem-sql-preview{background:var(--bg0);border:1px solid var(--border);border-radius:5px;padding:12px;font-size:10px;font-family:var(--mono);color:var(--text1);line-height:1.75;white-space:pre;overflow:auto;height:100%;}
.fem-info-box{background:rgba(0,212,170,0.04);border:1px solid rgba(0,212,170,0.18);border-left:3px solid var(--accent);border-radius:3px;padding:8px 12px;font-size:11px;color:var(--text1);line-height:1.7;margin-bottom:13px;}
.fem-err-box{background:rgba(255,77,109,0.06);border:1px solid rgba(255,77,109,0.3);border-left:3px solid var(--red);border-radius:3px;padding:7px 11px;font-size:11px;color:var(--red);line-height:1.6;margin-bottom:10px;}
.fem-badge{font-size:9px;padding:2px 6px;border-radius:10px;font-weight:600;font-family:var(--mono);background:rgba(0,212,170,0.1);color:var(--accent);margin-left:4px;}
.fem-sr-subject{display:flex;align-items:center;gap:7px;padding:5px 8px;border-radius:3px;cursor:pointer;font-size:11px;color:var(--text1);font-family:var(--mono);transition:background 0.1s;}
.fem-sr-subject:hover{background:rgba(255,255,255,0.05);}
.fem-sr-subject.selected{background:rgba(0,212,170,0.1);color:var(--accent);}
.fem-hist-item{padding:9px 12px;border:1px solid var(--border);border-radius:4px;background:var(--bg2);margin-bottom:6px;display:flex;align-items:center;gap:10px;cursor:pointer;transition:background 0.1s;}
.fem-hist-item:hover{background:var(--bg1);}
.fem-field-label{font-size:10px;color:var(--text2);margin-bottom:3px;display:block;font-family:var(--mono);}
.fem-required-star{color:var(--red);}
.fem-validation-msg{font-size:10px;color:var(--red);margin-top:3px;font-family:var(--mono);display:none;}
.fem-input-invalid{border-color:rgba(255,77,109,0.6)!important;background:rgba(255,77,109,0.05)!important;}
`;
    document.head.appendChild(s);
}

const FEM_STEPS = [
    { label: 'Data Source',    icon: '⬡' },
    { label: 'Feature Cols',   icon: '◈' },
    { label: 'Transforms',     icon: '⨍' },
    { label: 'Windows & Agg',  icon: '🪟' },
    { label: 'Output Sink',    icon: '⤵' },
    { label: 'Review & SQL',   icon: '⟨/⟩' },
];

function _femBuildModal() {
    _femInjectCss();
    const stepBar = FEM_STEPS.map((s, i) => `
    ${i > 0 ? '<div class="fem-step-connector"></div>' : ''}
    <div class="fem-step-item" id="fem-step-item-${i}" onclick="_femTryGoStep(${i})">
      <div class="fem-step-num" id="fem-step-num-${i}">${i + 1}</div>
      <span>${s.icon} ${s.label}</span>
    </div>`).join('');

    const m = document.createElement('div');
    m.id = 'fem-modal';
    m.className = 'modal-overlay';
    m.innerHTML = `
<div class="modal">
  <div class="modal-header" style="background:rgba(0,212,170,0.05);border-bottom:1px solid rgba(0,212,170,0.2);flex-shrink:0;">
    <span style="display:flex;align-items:center;gap:9px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2">
        <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
      </svg>
      Feature Engineering Manager
      <span style="font-size:9px;font-weight:400;color:var(--text3);font-family:var(--mono);">Real-time Flink SQL Feature Pipeline Builder</span>
    </span>
    <div style="display:flex;align-items:center;gap:8px;margin-left:auto;">
      <button onclick="_femShowHistory()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid var(--border2);background:var(--bg3);color:var(--text2);cursor:pointer;font-family:var(--mono);">
        🕓 History <span id="fem-hist-count" style="color:var(--accent);"></span>
      </button>
      <button onclick="_femResetAll()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;font-family:var(--mono);">↺ Reset</button>
      <button class="modal-close" onclick="closeModal('fem-modal')">×</button>
    </div>
  </div>
  <div id="fem-wizard-wrap">
    <div id="fem-step-bar">${stepBar}</div>
    <div id="fem-validation-banner" style="display:none;background:rgba(255,77,109,0.08);border-bottom:1px solid rgba(255,77,109,0.3);padding:6px 18px;font-size:11px;color:var(--red);font-family:var(--mono);flex-shrink:0;">
      <span id="fem-validation-msg-text"></span>
    </div>
    <div id="fem-body" style="flex:1;overflow-y:auto;padding:20px 24px;"></div>
    <div id="fem-footer">
      <button id="fem-btn-back" class="btn btn-secondary" style="font-size:11px;display:none;" onclick="_femBack()">← Back</button>
      <span id="fem-step-label" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
      <div style="flex:1;"></div>
      <button id="fem-btn-next" class="btn btn-primary" style="font-size:11px;" onclick="_femNext()">Next →</button>
      <button id="fem-btn-generate" style="font-size:11px;display:none;padding:7px 14px;border-radius:var(--radius);background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.4);color:var(--accent);cursor:pointer;font-family:var(--mono);font-weight:700;" onclick="_femInsertSql()">
        ⤵ Insert SQL into Editor
      </button>
    </div>
  </div>
</div>`;
    document.body.appendChild(m);
    m.addEventListener('click', e => { if (e.target === m) closeModal('fem-modal'); });
    _femUpdateHistCount();
}

function _femUpdateHistCount() {
    const el = document.getElementById('fem-hist-count');
    if (el) el.textContent = _FEM.history.length ? `(${_FEM.history.length})` : '';
}

function _femResetAll() {
    if (!confirm('Reset all Feature Engineering settings?')) return;
    Object.assign(_FEM, {
        step: 0, schemaMode: 'manual', srConnected: false, srSubjects: [], srSchemaCache: {},
        sourceTable: '', sourceFormat: 'kafka', rawColumns: [], selectedFeatures: [],
        transformations: [], windows: [], noWinGroupBy: '', noWinHaving: '', noWinAggs: '',
        timeCol: '', watermarkDelay: '5 SECOND', outputTable: '', sinkType: 'kafka', generatedSql: '',
    });
    _femGoStep(0);
}

// ── Validation ────────────────────────────────────────────────────────────────
function _femValidateStep(step) {
    switch (step) {
        case 0: {
            const table = document.getElementById('fem-src-table')?.value?.trim() || _FEM.sourceTable;
            if (!table) return 'Source Table Name is required.';
            if (_FEM.schemaMode === 'manual') {
                if (!_FEM.rawColumns.length) return 'Define at least one column in the schema.';
            } else {
                if (!_FEM.rawColumns.length) return 'Connect to Schema Registry and select a subject first.';
            }
            return null;
        }
        case 1: {
            if (!_FEM.selectedFeatures.length && !_FEM.rawColumns.length)
                return 'Select at least one feature column to continue.';
            return null;
        }
        case 2: return null;
        case 3: {
            for (let i = 0; i < _FEM.windows.length; i++) {
                const w = _FEM.windows[i];
                if (!w.timeCol) return `Window ${i + 1}: Time Column is required.`;
                if ((w.kind === 'tumble' || w.kind === 'hop' || w.kind === 'cumulate') && !w.size)
                    return `Window ${i + 1}: Window Size is required.`;
                if (w.kind === 'hop' && !w.slide) return `Window ${i + 1}: Slide Interval is required.`;
                if (w.kind === 'session' && !w.gap) return `Window ${i + 1}: Idle Gap is required.`;
                if (!w.aggs?.trim()) return `Window ${i + 1}: At least one aggregation is required.`;
            }
            return null;
        }
        case 4: {
            const out = document.getElementById('fem-out-table')?.value?.trim() || _FEM.outputTable;
            if (!out) return 'Output Table Name is required.';
            return null;
        }
        default: return null;
    }
}

function _femShowValidationError(msg) {
    const banner = document.getElementById('fem-validation-banner');
    const txt = document.getElementById('fem-validation-msg-text');
    if (banner && txt) {
        txt.textContent = '⚠ ' + msg;
        banner.style.display = '';
        setTimeout(() => { if (banner) banner.style.display = 'none'; }, 4000);
    }
}

function _femHideValidation() {
    const banner = document.getElementById('fem-validation-banner');
    if (banner) banner.style.display = 'none';
}

function _femTryGoStep(n) {
    // Allow going back freely; only validate going forward
    if (n > _FEM.step) {
        for (let s = _FEM.step; s < n; s++) {
            const err = _femValidateStep(s);
            if (err) { _femShowValidationError(err); return; }
        }
    }
    _femCollectCurrentStep();
    _femGoStep(n);
}

function _femCollectCurrentStep() {
    switch (_FEM.step) {
        case 0:
            _FEM.sourceTable = document.getElementById('fem-src-table')?.value?.trim() || _FEM.sourceTable;
            _FEM.sourceFormat = document.getElementById('fem-src-format')?.value || _FEM.sourceFormat;
            if (_FEM.schemaMode === 'manual') {
                const ta = document.getElementById('fem-manual-cols');
                if (ta) _femParseManualCols(ta.value);
            }
            break;
        case 1:
            _FEM.timeCol = document.getElementById('fem-time-col')?.value || _FEM.timeCol;
            _FEM.watermarkDelay = document.getElementById('fem-watermark-delay')?.value || _FEM.watermarkDelay;
            if (!_FEM.selectedFeatures.length) _FEM.selectedFeatures = [..._FEM.rawColumns];
            break;
        case 3:
            _FEM.noWinGroupBy = document.getElementById('fem-nowin-groupby')?.value || _FEM.noWinGroupBy;
            _FEM.noWinHaving = document.getElementById('fem-nowin-having')?.value || _FEM.noWinHaving;
            _FEM.noWinAggs = document.getElementById('fem-nowin-aggs')?.value || _FEM.noWinAggs;
            break;
        case 4:
            _FEM.outputTable = document.getElementById('fem-out-table')?.value?.trim() || _FEM.outputTable;
            _FEM.sinkType = document.getElementById('fem-sink-type')?.value || _FEM.sinkType;
            break;
    }
}

function _femGoStep(n) {
    _FEM.step = n;
    const body = document.getElementById('fem-body');
    if (!body) return;
    _femHideValidation();

    document.querySelectorAll('.fem-step-item').forEach((el, i) => {
        el.classList.toggle('active', i === n);
        el.classList.toggle('done', i < n);
        el.classList.remove('invalid');
    });
    document.querySelectorAll('.fem-step-num').forEach((el, i) => {
        el.textContent = i < n ? '✓' : String(i + 1);
    });

    const back = document.getElementById('fem-btn-back');
    const next = document.getElementById('fem-btn-next');
    const gen  = document.getElementById('fem-btn-generate');
    const lbl  = document.getElementById('fem-step-label');
    if (back) back.style.display = n === 0 ? 'none' : '';
    if (next) next.style.display = n === 5 ? 'none' : '';
    if (gen)  gen.style.display  = n === 5 ? '' : 'none';
    if (lbl)  lbl.textContent    = `Step ${n + 1} of ${FEM_STEPS.length} — ${FEM_STEPS[n].label}`;

    const renderers = [
        _femRenderStep0, _femRenderStep1, _femRenderStep2,
        _femRenderStep3, _femRenderStep4, _femRenderStep5,
    ];
    body.innerHTML = '';
    renderers[n]?.();
}

function _femNext() {
    const err = _femValidateStep(_FEM.step);
    if (err) { _femShowValidationError(err); return; }
    _femCollectCurrentStep();
    if (_FEM.step < 5) _femGoStep(_FEM.step + 1);
}

function _femBack() {
    _femCollectCurrentStep();
    if (_FEM.step > 0) _femGoStep(_FEM.step - 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 0 — Data Source
// ─────────────────────────────────────────────────────────────────────────────
function _femRenderStep0() {
    const body = document.getElementById('fem-body');
    body.innerHTML = `
<div class="fem-info-box">
  Define your streaming data source. Choose <strong>Manual Column Entry</strong> to type column definitions, or <strong>Schema Registry</strong> to import a schema directly from Confluent Schema Registry or Apicurio.
</div>
<div class="fem-section-title">Schema Input Method</div>
<div class="fem-radio-row" id="fem-schema-mode-row">
  <div class="fem-radio-btn ${_FEM.schemaMode==='manual'?'selected':''}" onclick="_femSetSchemaMode('manual')">
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 1 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
    Manual Column Entry
  </div>
  <div class="fem-radio-btn ${_FEM.schemaMode==='registry'?'selected':''}" onclick="_femSetSchemaMode('registry')">
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>
    Schema Registry
  </div>
</div>
<div id="fem-source-form"></div>`;
    _femRenderSourceForm();
}

function _femSetSchemaMode(mode) {
    _FEM.schemaMode = mode;
    document.querySelectorAll('#fem-schema-mode-row .fem-radio-btn').forEach((el, i) => {
        el.classList.toggle('selected', (i === 0 && mode === 'manual') || (i === 1 && mode === 'registry'));
    });
    _femRenderSourceForm();
}

function _femRenderSourceForm() {
    const wrap = document.getElementById('fem-source-form');
    if (!wrap) return;
    const srcTypes = ['kafka','datagen','jdbc','filesystem','kinesis','pulsar'];
    const commonHeader = `
<div class="fem-card">
  <div class="fem-section-title">Source Connector</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:4px;">
    <div>
      <label class="fem-field-label">Source Table Name <span class="fem-required-star">*</span></label>
      <input class="field-input" id="fem-src-table" type="text" placeholder="transactions_raw"
        value="${_escFem(_FEM.sourceTable)}" oninput="_FEM.sourceTable=this.value;_femClearInvalid(this)"
        style="font-family:var(--mono);font-size:11px;" />
      <div id="fem-src-table-err" class="fem-validation-msg">Table name is required.</div>
    </div>
    <div>
      <label class="fem-field-label">Connector Type</label>
      <select class="field-input" id="fem-src-format" style="font-size:11px;" onchange="_FEM.sourceFormat=this.value">
        ${srcTypes.map(t => `<option value="${t}" ${_FEM.sourceFormat===t?'selected':''}>${t.toUpperCase()}</option>`).join('')}
      </select>
    </div>
  </div>
</div>`;

    if (_FEM.schemaMode === 'manual') {
        wrap.innerHTML = commonHeader + `
<div class="fem-card">
  <div class="fem-section-title">Column Definitions <span class="fem-required-star">*</span></div>
  <div style="font-size:11px;color:var(--text2);margin-bottom:8px;line-height:1.7;">
    Enter columns as <code style="background:var(--bg0);padding:1px 5px;border-radius:2px;color:var(--accent);">name TYPE</code> — one per line.
    Types: <code style="color:var(--blue);font-size:10px;">BIGINT INT DOUBLE FLOAT STRING BOOLEAN TIMESTAMP(3) DECIMAL(p,s) BYTES</code>
  </div>
  <textarea id="fem-manual-cols" class="field-input" rows="9"
    style="font-family:var(--mono);font-size:11px;resize:vertical;line-height:1.8;"
    placeholder="user_id BIGINT&#10;merchant_id BIGINT&#10;amount DOUBLE&#10;currency STRING&#10;status STRING&#10;is_international BOOLEAN&#10;channel STRING&#10;device_type STRING&#10;event_time TIMESTAMP(3)"
    oninput="_femParseManualCols(this.value)">${_FEM.rawColumns.map(c=>c.name+' '+c.type).join('\n')}</textarea>
  <div id="fem-col-parse-feedback" style="font-size:10px;margin-top:5px;color:var(--text3);font-family:var(--mono);"></div>
</div>`;
        setTimeout(() => {
            const ta = document.getElementById('fem-manual-cols');
            if (ta && ta.value.trim()) _femParseManualCols(ta.value);
        }, 50);
    } else {
        wrap.innerHTML = commonHeader + `
<div class="fem-card">
  <div class="fem-section-title">Schema Registry Connection</div>
  <div style="display:grid;grid-template-columns:1fr auto auto;gap:8px;align-items:end;margin-bottom:10px;">
    <div>
      <label class="fem-field-label">Registry URL <span class="fem-required-star">*</span></label>
      <input id="fem-sr-url" class="field-input" type="text" placeholder="http://schema-registry:8081"
        value="${_escFem(_FEM.srUrl)}" style="font-size:11px;font-family:var(--mono);" />
    </div>
    <div>
      <label class="fem-field-label">API Key</label>
      <input id="fem-sr-user" class="field-input" type="text" placeholder="optional" style="font-size:11px;width:120px;" />
    </div>
    <div>
      <label class="fem-field-label">Secret</label>
      <input id="fem-sr-pass" class="field-input" type="password" placeholder="optional" style="font-size:11px;width:120px;" />
    </div>
  </div>
  <button class="btn btn-primary" style="font-size:11px;" onclick="_femSrConnect()">Connect to Registry</button>
  <span id="fem-sr-status" style="font-size:10px;margin-left:10px;font-family:var(--mono);"></span>
</div>
<div id="fem-sr-subjects-wrap" style="${_FEM.srConnected?'':'display:none'}">
  <div class="fem-card" style="padding:10px;">
    <div class="fem-section-title">Select a Subject <span class="fem-required-star">*</span></div>
    <input id="fem-sr-search" class="field-input" type="text" placeholder="Filter subjects…"
      style="font-size:11px;margin-bottom:8px;" oninput="_femFilterSrSubjects(this.value)" />
    <div id="fem-sr-subject-list" style="max-height:160px;overflow-y:auto;border:1px solid var(--border);border-radius:3px;background:var(--bg0);padding:4px;"></div>
  </div>
  <div id="fem-sr-preview-wrap" style="display:none;" class="fem-card">
    <div class="fem-section-title" id="fem-sr-preview-title">Schema Preview</div>
    <div id="fem-sr-fields-wrap"></div>
  </div>
</div>`;
        if (_FEM.srConnected) _femRenderSrSubjectList(_FEM.srSubjects);
    }
}

function _femClearInvalid(el) {
    if (el) el.classList.remove('fem-input-invalid');
}

function _femParseManualCols(raw) {
    const lines = raw.split('\n').map(l => l.trim()).filter(Boolean);
    const cols = [];
    lines.forEach(line => {
        const parts = line.replace(/,/g, '').trim().split(/\s+/);
        if (parts.length >= 2) cols.push({ name: parts[0], type: parts.slice(1).join(' '), nullable: true });
    });
    _FEM.rawColumns = cols;
    const fb = document.getElementById('fem-col-parse-feedback');
    if (fb) fb.textContent = cols.length ? `✓ ${cols.length} column${cols.length > 1 ? 's' : ''} parsed` : '';
}

async function _femSrConnect() {
    const url  = document.getElementById('fem-sr-url')?.value?.trim();
    const user = document.getElementById('fem-sr-user')?.value?.trim();
    const pass = document.getElementById('fem-sr-pass')?.value?.trim();
    const stat = document.getElementById('fem-sr-status');
    if (!url) { if (stat) { stat.textContent = '⚠ Enter a URL'; stat.style.color = 'var(--red)'; } return; }
    if (stat) { stat.textContent = 'Connecting…'; stat.style.color = 'var(--text3)'; }
    _FEM.srUrl = url;
    _FEM.srAuth = (user && pass) ? 'Basic ' + btoa(user + ':' + pass) : '';
    try {
        const headers = { 'Accept': 'application/json' };
        if (_FEM.srAuth) headers['Authorization'] = _FEM.srAuth;
        const res = await fetch(url.replace(/\/$/, '') + '/subjects', { headers });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const subjects = await res.json();
        _FEM.srSubjects = subjects;
        _FEM.srConnected = true;
        if (stat) { stat.textContent = `✓ Connected — ${subjects.length} subjects`; stat.style.color = 'var(--green)'; }
        const wrap = document.getElementById('fem-sr-subjects-wrap');
        if (wrap) wrap.style.display = '';
        _femRenderSrSubjectList(subjects);
    } catch (e) {
        _FEM.srConnected = false;
        if (stat) { stat.textContent = '✗ ' + e.message; stat.style.color = 'var(--red)'; }
    }
}

function _femRenderSrSubjectList(subjects) {
    const el = document.getElementById('fem-sr-subject-list');
    if (!el) return;
    if (!subjects.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text3);padding:8px;">No subjects found.</div>'; return; }
    el.innerHTML = subjects.map(s => `
    <div class="fem-sr-subject" data-subject="${_escFem(s)}" onclick="_femSrSelectSubject('${_escFem(s)}')">
      <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="opacity:0.4;flex-shrink:0;">
        <ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/>
      </svg>
      <span style="overflow:hidden;text-overflow:ellipsis;">${_escFem(s)}</span>
    </div>`).join('');
}

function _femFilterSrSubjects(q) {
    const filtered = q ? _FEM.srSubjects.filter(s => s.toLowerCase().includes(q.toLowerCase())) : _FEM.srSubjects;
    _femRenderSrSubjectList(filtered);
}

async function _femSrSelectSubject(subject) {
    document.querySelectorAll('.fem-sr-subject').forEach(el =>
        el.classList.toggle('selected', el.dataset.subject === subject));
    const url = _FEM.srUrl.replace(/\/$/, '');
    const headers = { 'Accept': 'application/json' };
    if (_FEM.srAuth) headers['Authorization'] = _FEM.srAuth;
    try {
        const res = await fetch(`${url}/subjects/${encodeURIComponent(subject)}/versions/latest`, { headers });
        const data = await res.json();
        _FEM.srSchemaCache[subject] = data;
        let parsed = {};
        try { parsed = JSON.parse(data.schema || '{}'); } catch (_) {}
        const typeMap = { string:'STRING', int:'INT', long:'BIGINT', float:'FLOAT', double:'DOUBLE', boolean:'BOOLEAN', bytes:'BYTES', null:'STRING' };
        const fields = (parsed.fields || []).map(f => {
            const rawType = Array.isArray(f.type) ? f.type.find(t => t !== 'null') || 'string' : f.type;
            return { name: f.name, type: typeMap[String(rawType).toLowerCase()] || 'STRING', nullable: true };
        });
        _FEM.rawColumns = fields;
        const title = document.getElementById('fem-sr-preview-title');
        const wrap  = document.getElementById('fem-sr-preview-wrap');
        const fw    = document.getElementById('fem-sr-fields-wrap');
        if (title) title.textContent = `Schema Preview — ${subject} (${fields.length} fields)`;
        if (wrap)  wrap.style.display = '';
        if (fw) {
            fw.innerHTML = `<div style="display:flex;flex-wrap:wrap;">${fields.map(f =>
                `<div class="fem-col-tag selected" style="cursor:default;">${_escFem(f.name)}<span class="fem-tag-type">${f.type}</span></div>`
            ).join('')}</div>
      <div style="font-size:10px;color:var(--green);margin-top:8px;font-family:var(--mono);">✓ ${fields.length} columns loaded — click Next to continue</div>`;
        }
        if (!_FEM.sourceTable) {
            _FEM.sourceTable = subject.replace(/-value|-key$/i,'').replace(/[^a-zA-Z0-9_]/g,'_').toLowerCase();
            const inp = document.getElementById('fem-src-table');
            if (inp) inp.value = _FEM.sourceTable;
        }
    } catch (e) {
        if (typeof toast === 'function') toast('Schema load failed: ' + e.message, 'err');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1 — Feature Column Selection
// ─────────────────────────────────────────────────────────────────────────────
function _femRenderStep1() {
    const body = document.getElementById('fem-body');
    if (!_FEM.rawColumns.length) {
        body.innerHTML = `<div class="fem-err-box">⚠ No columns defined. Go back to Step 1 and enter column definitions or connect to Schema Registry.</div>`;
        return;
    }
    const isTime = c => /timestamp|time|ts|created|updated|event/i.test(c.name) || c.type.toUpperCase().startsWith('TIMESTAMP');
    body.innerHTML = `
<div class="fem-info-box">
  Select which columns to include in your feature set. Click to toggle. <strong>Time/timestamp</strong> columns are marked with a clock icon for watermark assignment.
</div>
<div style="display:flex;align-items:center;gap:8px;margin-bottom:11px;flex-wrap:wrap;">
  <button class="fem-btn-add" onclick="_femSelectAllCols(true)">Select All</button>
  <button class="fem-btn-add" style="background:rgba(255,77,109,0.08);border-color:rgba(255,77,109,0.3);color:var(--red);" onclick="_femSelectAllCols(false)">Clear All</button>
  <button class="fem-btn-add" onclick="_femSelectByType('numeric')" style="background:rgba(79,163,224,0.08);border-color:rgba(79,163,224,0.3);color:var(--blue);">Numeric Only</button>
  <span id="fem-sel-count" style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-left:auto;"></span>
</div>
<div class="fem-card">
  <div class="fem-section-title">Available Columns <span class="fem-badge">${_FEM.rawColumns.length}</span></div>
  <div id="fem-col-tags-wrap" style="display:flex;flex-wrap:wrap;">
    ${_FEM.rawColumns.map(c => {
        const isSel = _FEM.selectedFeatures.length === 0 || _FEM.selectedFeatures.find(f => f.name === c.name);
        const isTs  = isTime(c);
        if (_FEM.selectedFeatures.length === 0 && !_FEM.selectedFeatures.find(f => f.name === c.name)) {
            // Pre-select all on first visit
        }
        return `<div class="fem-col-tag ${isSel?'selected':''}" id="fem-tag-${_escId(c.name)}" onclick="_femToggleCol('${_escFem(c.name)}')"
        style="${isTs?'border-color:rgba(79,163,224,0.4);':''}" title="${c.type}">
        ${isTs?'<svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="var(--blue)" stroke-width="2.5" style="flex-shrink:0;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>':''}
        ${_escFem(c.name)}<span class="fem-tag-type">${c.type}</span>
      </div>`;
    }).join('')}
  </div>
</div>
<div class="fem-card">
  <div class="fem-section-title">Event Time & Watermark</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
    <div>
      <label class="fem-field-label">Event Time Column</label>
      <select class="field-input" id="fem-time-col" style="font-size:11px;">
        <option value="">— none (processing time) —</option>
        ${_FEM.rawColumns.filter(isTime).map(c =>
        `<option value="${_escFem(c.name)}" ${_FEM.timeCol===c.name?'selected':''}>${c.name} (${c.type})</option>`).join('')}
        ${_FEM.rawColumns.filter(c => !isTime(c)).map(c =>
        `<option value="${_escFem(c.name)}" ${_FEM.timeCol===c.name?'selected':''}>${c.name} (${c.type})</option>`).join('')}
      </select>
    </div>
    <div>
      <label class="fem-field-label">Max Out-of-Orderness</label>
      <select class="field-input" id="fem-watermark-delay" style="font-size:11px;">
        ${['0 SECOND','5 SECOND','10 SECOND','30 SECOND','1 MINUTE','5 MINUTE'].map(v =>
        `<option value="${v}" ${(_FEM.watermarkDelay||'5 SECOND')===v?'selected':''}>${v}</option>`).join('')}
      </select>
    </div>
  </div>
</div>`;

    // Pre-select all on first visit
    if (_FEM.selectedFeatures.length === 0) {
        _FEM.selectedFeatures = [..._FEM.rawColumns];
        document.querySelectorAll('.fem-col-tag').forEach(el => el.classList.add('selected'));
    }
    _femUpdateSelCount();
}

function _femToggleCol(name) {
    const col = _FEM.rawColumns.find(c => c.name === name);
    if (!col) return;
    const idx = _FEM.selectedFeatures.findIndex(f => f.name === name);
    if (idx >= 0) _FEM.selectedFeatures.splice(idx, 1);
    else _FEM.selectedFeatures.push({ ...col });
    const tag = document.getElementById('fem-tag-' + _escId(name));
    if (tag) tag.classList.toggle('selected', idx < 0);
    _femUpdateSelCount();
}

function _femSelectAllCols(sel) {
    _FEM.selectedFeatures = sel ? [..._FEM.rawColumns] : [];
    document.querySelectorAll('.fem-col-tag').forEach(el => el.classList.toggle('selected', sel));
    _femUpdateSelCount();
}

function _femSelectByType(kind) {
    const numTypes = ['INT','BIGINT','DOUBLE','FLOAT','DECIMAL','SMALLINT','TINYINT'];
    _FEM.selectedFeatures = _FEM.rawColumns.filter(c =>
        numTypes.some(t => c.type.toUpperCase().startsWith(t))
    );
    document.querySelectorAll('.fem-col-tag').forEach(el => {
        const name = el.id.replace('fem-tag-', '');
        el.classList.toggle('selected', _FEM.selectedFeatures.some(f => _escId(f.name) === name));
    });
    _femUpdateSelCount();
}

function _femUpdateSelCount() {
    const el = document.getElementById('fem-sel-count');
    if (el) el.textContent = `${_FEM.selectedFeatures.length} / ${_FEM.rawColumns.length} selected`;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2 — Transformations
// ─────────────────────────────────────────────────────────────────────────────
const FEM_TRANSFORMS = [
    { id:'passthrough', label:'Pass-through (no change)', hasParams:false },
    { id:'cast',        label:'CAST to type',             hasParams:true, paramLabel:'Target Type',         placeholder:'DOUBLE' },
    { id:'math',        label:'Math expression',          hasParams:true, paramLabel:'Expression',          placeholder:'amount * 1.1' },
    { id:'coalesce',    label:'COALESCE / default',       hasParams:true, paramLabel:'Default value',       placeholder:'0' },
    { id:'upper',       label:'UPPER()',                  hasParams:false },
    { id:'lower',       label:'LOWER()',                  hasParams:false },
    { id:'trim',        label:'TRIM()',                   hasParams:false },
    { id:'substring',   label:'SUBSTRING',                hasParams:true, paramLabel:'FROM x FOR y',        placeholder:'1 FOR 3' },
    { id:'round',       label:'ROUND to decimals',        hasParams:true, paramLabel:'Decimals',            placeholder:'2' },
    { id:'abs',         label:'ABS()',                    hasParams:false },
    { id:'log',         label:'LN / LOG (natural log)',   hasParams:false },
    { id:'bucket',      label:'Bucketing / CASE WHEN',    hasParams:true, paramLabel:'Thresholds (comma)',  placeholder:'100,500,1000' },
    { id:'hash_encode', label:'MD5 / SHA2 encode',        hasParams:false },
    { id:'date_format', label:'DATE_FORMAT',              hasParams:true, paramLabel:'Format pattern',      placeholder:'yyyy-MM-dd' },
    { id:'ts_to_epoch', label:'UNIX_TIMESTAMP',           hasParams:false },
    { id:'epoch_to_ts', label:'FROM_UNIXTIME',            hasParams:false },
    { id:'ifnull',      label:'IFNULL / NVL',             hasParams:true, paramLabel:'Fallback value',      placeholder:'0.0' },
    { id:'lag',         label:'LAG (over partition)',      hasParams:true, paramLabel:'Partition col',       placeholder:'user_id' },
    { id:'normalise',   label:'Min-Max Normalise (hint)',  hasParams:true, paramLabel:'Min,Max',             placeholder:'0,1000' },
    { id:'custom',      label:'Custom SQL expression',    hasParams:true, paramLabel:'Full SQL expression', placeholder:"CASE WHEN amount>1000 THEN 'HIGH' ELSE 'LOW' END AS risk_tier" },
];

function _femRenderStep2() {
    if (_FEM.transformations.length === 0) {
        const cols = _FEM.selectedFeatures.length ? _FEM.selectedFeatures : _FEM.rawColumns;
        _FEM.transformations = cols.map(c => ({
            colName: c.name, colType: c.type, transformId: 'passthrough', param: '', alias: c.name
        }));
    }

    const body = document.getElementById('fem-body');
    body.innerHTML = `
<div class="fem-info-box">
  Configure per-column transformations. <strong>Pass-through</strong> emits the column unchanged. Each row produces an output alias — rename to your feature store column name.
</div>
<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
  <span style="font-size:11px;font-weight:700;color:var(--text0);">Column Transformations</span>
  <span style="font-size:10px;color:var(--text3);font-family:var(--mono);">${_FEM.transformations.length} columns</span>
</div>
<div style="background:var(--bg0);border:1px solid var(--border);border-radius:4px;padding:6px 8px;margin-bottom:10px;display:flex;gap:12px;font-size:10px;color:var(--text3);font-family:var(--mono);">
  <span style="min-width:100px;">COLUMN</span>
  <span style="min-width:60px;">TYPE</span>
  <span style="flex:1.2;">TRANSFORMATION</span>
  <span style="flex:1;">PARAMETER</span>
  <span style="width:130px;">OUTPUT ALIAS</span>
  <span style="width:22px;"></span>
</div>
<div id="fem-transforms-list"></div>
<div style="margin-top:12px;padding:12px;background:var(--bg2);border:1px solid var(--border);border-radius:4px;">
  <div class="fem-section-title">Add Custom Feature Expression</div>
  <div style="display:flex;gap:8px;align-items:flex-end;">
    <div style="flex:1;">
      <label class="fem-field-label">Full SQL expression (must include AS alias)</label>
      <input id="fem-custom-expr-inp" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
        placeholder="CASE WHEN amount > 1000 AND is_international THEN 'HIGH_RISK' ELSE 'LOW_RISK' END AS risk_tier" />
    </div>
    <button class="fem-btn-add" onclick="_femAddCustomExpr()">+ Add</button>
  </div>
</div>`;
    _femRenderTransformsList();
}

function _femRenderTransformsList() {
    const wrap = document.getElementById('fem-transforms-list');
    if (!wrap) return;
    wrap.innerHTML = _FEM.transformations.map((t, i) => {
        const def = FEM_TRANSFORMS.find(x => x.id === t.transformId) || FEM_TRANSFORMS[0];
        return `
<div class="fem-transform-row">
  <span class="fem-col-name" title="${_escFem(t.colType)}">${_escFem(t.colName === '_expr_' ? '[expr]' : t.colName)}</span>
  <span style="font-size:9px;color:var(--text3);min-width:62px;overflow:hidden;text-overflow:ellipsis;" title="${_escFem(t.colType)}">${_escFem(t.colType)}</span>
  <select onchange="_femSetTransform(${i},this.value)" style="flex:1.2;">
    ${FEM_TRANSFORMS.map(td => `<option value="${td.id}" ${t.transformId===td.id?'selected':''}>${td.label}</option>`).join('')}
  </select>
  ${def.hasParams
            ? `<input type="text" value="${_escFem(t.param||'')}" placeholder="${_escFem(def.placeholder||def.paramLabel||'')}"
        onchange="_FEM.transformations[${i}].param=this.value" style="flex:1;min-width:0;" />`
            : `<span style="flex:1;font-size:9px;color:var(--text3);padding:0 4px;">${def.label.includes('()')?def.label:''}</span>`}
  <input type="text" value="${_escFem(t.alias||t.colName)}" placeholder="alias"
    onchange="_FEM.transformations[${i}].alias=this.value" style="width:130px;" />
  <button class="fem-btn-del" onclick="_FEM.transformations.splice(${i},1);_femRenderTransformsList();" title="Remove">×</button>
</div>`;
    }).join('') || '<div style="font-size:11px;color:var(--text3);padding:8px 0;">No transforms defined.</div>';
}

function _femSetTransform(i, val) {
    _FEM.transformations[i].transformId = val;
    _FEM.transformations[i].param = '';
    _femRenderTransformsList();
}

function _femAddCustomExpr() {
    const inp = document.getElementById('fem-custom-expr-inp');
    const val = inp?.value?.trim();
    if (!val) return;
    _FEM.transformations.push({ colName:'_expr_', colType:'CUSTOM', transformId:'custom', param:val, alias:'' });
    if (inp) inp.value = '';
    _femRenderTransformsList();
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3 — Windows & Aggregations
// ─────────────────────────────────────────────────────────────────────────────
function _femRenderStep3() {
    const body = document.getElementById('fem-body');
    body.innerHTML = `
<div class="fem-info-box">
  Define streaming window aggregations. Multiple windows of different sizes model real-world feature stores (e.g. 1-min, 5-min, 1-hour). Fields marked <span style="color:var(--red);">*</span> are required before proceeding.
</div>
<div id="fem-windows-list"></div>
<button class="fem-btn-add" style="margin-bottom:14px;" onclick="_femAddWindow()">+ Add Window</button>
<div class="fem-card">
  <div class="fem-section-title">Keyless Group Aggregation <span style="font-size:9px;font-weight:400;color:var(--text3);">(no time window)</span></div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:8px;">
    <div>
      <label class="fem-field-label">GROUP BY Columns</label>
      <input id="fem-nowin-groupby" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
        placeholder="user_id, merchant_id" value="${_escFem(_FEM.noWinGroupBy)}" />
    </div>
    <div>
      <label class="fem-field-label">HAVING Clause (optional)</label>
      <input id="fem-nowin-having" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
        placeholder="cnt > 3" value="${_escFem(_FEM.noWinHaving)}" />
    </div>
  </div>
  <label class="fem-field-label">Aggregations <span style="font-size:9px;color:var(--text3);">(one per line: FUNC(col) AS alias)</span></label>
  <textarea id="fem-nowin-aggs" class="field-input" rows="3"
    style="font-size:11px;font-family:var(--mono);resize:vertical;"
    placeholder="COUNT(*) AS tx_count&#10;SUM(amount) AS total_amount&#10;AVG(amount) AS avg_amount&#10;MAX(amount) AS max_amount">${_escFem(_FEM.noWinAggs)}</textarea>
</div>`;
    _femRenderWindowsList();
}

function _femAddWindow() {
    _FEM.windows.push({
        kind:'tumble', timeCol:_FEM.timeCol||'', size:'5 MINUTE',
        slide:'1 MINUTE', gap:'30 SECOND', maxSize:'1 HOUR', groupBy:'', aggs:''
    });
    _femRenderWindowsList();
}

function _femRenderWindowsList() {
    const wrap = document.getElementById('fem-windows-list');
    if (!wrap) return;
    if (!_FEM.windows.length) {
        wrap.innerHTML = `<div style="font-size:11px;color:var(--text3);padding:6px 0 10px;">No windows defined — use group aggregation below or add a window above.</div>`;
        return;
    }

    const inp = (label, key, i, ph, req) => `
<div>
  <label class="fem-field-label">${label}${req?` <span class="fem-required-star">*</span>`:''}</label>
  <input type="text" value="${_escFem(_FEM.windows[i][key]||'')}" placeholder="${ph}"
    onchange="_FEM.windows[${i}]['${key}']=this.value"
    style="width:100%;box-sizing:border-box;background:var(--bg3);border:1px solid var(--border);color:var(--text0);font-family:var(--mono);font-size:11px;padding:4px 7px;border-radius:3px;outline:none;" />
</div>`;

    wrap.innerHTML = _FEM.windows.map((w, i) => `
<div class="fem-card" style="border-left:3px solid var(--accent);margin-bottom:10px;">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
    <span style="font-size:11px;font-weight:700;color:var(--accent);font-family:var(--mono);">Window ${i+1}</span>
    <select onchange="_FEM.windows[${i}].kind=this.value;_femRenderWindowsList();"
      style="background:var(--bg3);border:1px solid var(--border2);color:var(--text0);font-family:var(--mono);font-size:11px;padding:3px 7px;border-radius:3px;outline:none;">
      <option value="tumble"  ${w.kind==='tumble'  ?'selected':''}>TUMBLE — fixed non-overlapping</option>
      <option value="hop"     ${w.kind==='hop'     ?'selected':''}>HOP — sliding overlapping</option>
      <option value="session" ${w.kind==='session' ?'selected':''}>SESSION — activity-based gap</option>
      <option value="cumulate"${w.kind==='cumulate'?'selected':''}>CUMULATE — running total</option>
    </select>
    <button class="fem-btn-del" onclick="_FEM.windows.splice(${i},1);_femRenderWindowsList();" style="margin-left:auto;">× Remove</button>
  </div>
  <div style="display:grid;grid-template-columns:${w.kind==='hop'?'repeat(4,1fr)':'repeat(3,1fr)'};gap:8px;margin-bottom:8px;">
    ${inp('Time Column','timeCol',i,'event_time',true)}
    ${w.kind==='tumble'||w.kind==='cumulate'?inp('Window Size','size',i,'5 MINUTE',true):''}
    ${w.kind==='hop'?inp('Slide Interval','slide',i,'1 MINUTE',true):''}
    ${w.kind==='hop'?inp('Window Size','size',i,'5 MINUTE',true):''}
    ${w.kind==='session'?inp('Idle Gap','gap',i,'30 SECOND',true):''}
    ${w.kind==='cumulate'?inp('Max Window Size','maxSize',i,'1 HOUR',true):''}
    ${inp('GROUP BY Columns','groupBy',i,'user_id, merchant_id',false)}
  </div>
  <div>
    <label class="fem-field-label">Aggregations <span class="fem-required-star">*</span>
      <span style="font-size:9px;color:var(--text3);font-weight:400;">(one per line: FUNC(col) AS alias)</span>
    </label>
    <textarea onchange="_FEM.windows[${i}].aggs=this.value"
      style="width:100%;box-sizing:border-box;background:var(--bg3);border:1px solid var(--border);color:var(--text0);font-family:var(--mono);font-size:11px;padding:6px 8px;border-radius:3px;resize:vertical;min-height:58px;outline:none;"
      placeholder="COUNT(*) AS tx_count_${(w.size||'5m').replace(/ /g,'_')}&#10;SUM(amount) AS total_amt_${(w.size||'5m').replace(/ /g,'_')}">${_escFem(w.aggs)}</textarea>
  </div>
</div>`).join('');
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 4 — Output Sink
// ─────────────────────────────────────────────────────────────────────────────
function _femRenderStep4() {
    const body = document.getElementById('fem-body');
    body.innerHTML = `
<div class="fem-info-box">
  Configure the output sink for your computed features. The output table receives all transformed and aggregated feature columns.
</div>
<div class="fem-card">
  <div class="fem-section-title">Output Configuration</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;">
    <div>
      <label class="fem-field-label">Output Table Name <span class="fem-required-star">*</span></label>
      <input id="fem-out-table" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
        placeholder="user_transaction_features"
        value="${_escFem(_FEM.outputTable || (_FEM.sourceTable?_FEM.sourceTable+'_features':'features_out'))}"
        oninput="_FEM.outputTable=this.value;_femClearInvalid(this)" />
    </div>
    <div>
      <label class="fem-field-label">Sink / Feature Store Type</label>
      <select id="fem-sink-type" class="field-input" style="font-size:11px;"
        onchange="_FEM.sinkType=this.value;_femRenderSinkParams(this.value)">
        <option value="kafka">Apache Kafka</option>
        <option value="jdbc">JDBC (PostgreSQL / MySQL / Redshift)</option>
        <option value="filesystem">File System / S3 / GCS / HDFS</option>
        <option value="iceberg">Apache Iceberg</option>
        <option value="hudi">Apache Hudi</option>
        <option value="elasticsearch">Elasticsearch / OpenSearch</option>
        <option value="redis">Redis (via Flink-Redis connector)</option>
        <option value="feast">Feast Feature Store</option>
        <option value="print">Print (debug stdout)</option>
        <option value="blackhole">Blackhole (throughput test)</option>
      </select>
    </div>
  </div>
  <div id="fem-sink-params"></div>
</div>`;

    const savedType = _FEM.sinkType || 'kafka';
    document.getElementById('fem-sink-type').value = savedType;
    _femRenderSinkParams(savedType);
}

function _femRenderSinkParams(type) {
    _FEM.sinkType = type;
    const wrap = document.getElementById('fem-sink-params');
    if (!wrap) return;
    const fi = `style="width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:3px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;"`;
    const sel = (label, id, opts) => `<div><label class="fem-field-label">${label}</label><select ${fi} id="${id}">${opts.map(o=>`<option>${o}</option>`).join('')}</select></div>`;
    const inp = (label, id, ph, isPass) => `<div><label class="fem-field-label">${label}</label><input type="${isPass?'password':'text'}" ${fi} id="${id}" placeholder="${ph}" /></div>`;

    const forms = {
        kafka: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Bootstrap Servers <span style="color:var(--red)">*</span>','fem-sk-bootstrap','kafka:9092')}
      ${inp('Output Topic <span style="color:var(--red)">*</span>','fem-sk-topic','user-features-out')}
      ${sel('Format','fem-sk-format',['json','avro','avro-confluent','csv','raw'])}
      ${sel('Security Protocol','fem-sk-security',['PLAINTEXT (none)','SASL_SSL','SSL','SASL_PLAINTEXT'])}
      ${inp('SASL Username / API Key (if SASL)','fem-sk-sasl-user','api-key')}
      ${inp('SASL Password / Secret','fem-sk-sasl-pass','api-secret',true)}
    </div>`,
        jdbc: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('JDBC URL <span style="color:var(--red)">*</span>','fem-sk-jdbc-url','jdbc:postgresql://localhost:5432/features')}
      ${inp('Target Table <span style="color:var(--red)">*</span>','fem-sk-jdbc-table','features.user_transaction_features')}
      ${inp('Username','fem-sk-jdbc-user','flink_writer')}
      ${inp('Password','fem-sk-jdbc-pass','●●●●●●',true)}
      ${sel('Driver','fem-sk-jdbc-driver',['org.postgresql.Driver','com.mysql.cj.jdbc.Driver','com.amazon.redshift.jdbc.Driver','org.apache.hive.jdbc.HiveDriver'])}
      ${inp('Sink Parallelism','fem-sk-jdbc-par','4')}
    </div>`,
        filesystem: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Sink Path <span style="color:var(--red)">*</span>','fem-sk-fs-path','s3://my-bucket/features/')}
      ${sel('Format','fem-sk-fs-format',['parquet','orc','json','csv','avro'])}
      ${inp('Rolling Interval','fem-sk-fs-roll','10 min')}
      ${inp('Partition By Column','fem-sk-fs-part','date_col')}
    </div>`,
        elasticsearch: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('ES Hosts <span style="color:var(--red)">*</span>','fem-sk-es-hosts','http://elasticsearch:9200')}
      ${inp('Index <span style="color:var(--red)">*</span>','fem-sk-es-index','user-features')}
      ${sel('ES Version','fem-sk-es-ver',['7','8'])}
      ${inp('Username','fem-sk-es-user','elastic')}
      ${inp('Password','fem-sk-es-pass','changeme',true)}
      ${inp('Document ID Column','fem-sk-es-docid','user_id')}
    </div>`,
        iceberg: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Catalog Name','fem-sk-ice-cat','iceberg_catalog')}
      ${inp('Database.Table <span style="color:var(--red)">*</span>','fem-sk-ice-tbl','features.user_features')}
      ${sel('Catalog Type','fem-sk-ice-cattype',['hive','hadoop','rest','glue','nessie'])}
      ${inp('Warehouse Path','fem-sk-ice-wh','s3://warehouse/iceberg')}
      ${inp('Sort Order Column','fem-sk-ice-sort','user_id')}
      ${sel('Write Mode','fem-sk-ice-mode',['upsert','append'])}
    </div>`,
        hudi: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Table Path <span style="color:var(--red)">*</span>','fem-sk-hudi-path','s3://warehouse/hudi/user_features')}
      ${sel('Table Type','fem-sk-hudi-type',['COPY_ON_WRITE','MERGE_ON_READ'])}
      ${inp('Record Key Field','fem-sk-hudi-rkey','user_id')}
      ${inp('Precombine Field','fem-sk-hudi-pre','event_time')}
      ${inp('Partition Field','fem-sk-hudi-part','partition_date')}
      ${inp('Hudi Sync Class (opt)','fem-sk-hudi-sync','org.apache.hudi.hive.HiveSyncTool')}
    </div>`,
        feast: `<div style="background:rgba(79,163,224,0.05);border:1px solid rgba(79,163,224,0.2);border-radius:4px;padding:10px 12px;margin-bottom:10px;font-size:11px;color:var(--text1);line-height:1.7;">
      Feast requires a custom HTTP sink or gRPC UDF. The SQL below will generate the query; adapt the connector to your Feast server SDK version.
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Feast Server URL','fem-sk-feast-url','http://feast-server:6566')}
      ${inp('Feature View Name <span style="color:var(--red)">*</span>','fem-sk-feast-view','user_transaction_features')}
      ${inp('Project / Namespace','fem-sk-feast-proj','fraud_detection')}
      ${inp('Entity Key Column','fem-sk-feast-entity','user_id')}
    </div>`,
        redis: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Redis Host:Port <span style="color:var(--red)">*</span>','fem-sk-redis-host','redis:6379')}
      ${inp('Password','fem-sk-redis-pass','optional',true)}
      ${inp('Key Column','fem-sk-redis-key','user_id')}
      ${inp('TTL (seconds)','fem-sk-redis-ttl','3600')}
      ${sel('Redis Mode','fem-sk-redis-mode',['standalone','sentinel','cluster'])}
      ${inp('Sentinel Master (sentinel mode)','fem-sk-redis-master','mymaster')}
    </div>`,
        print:     `<div style="font-size:11px;color:var(--text3);padding:6px 0;">Outputs rows to TaskManager stdout. Use for development and debugging only.</div>`,
        blackhole: `<div style="font-size:11px;color:var(--text3);padding:6px 0;">Discards all records. Use to benchmark pipeline throughput without I/O overhead.</div>`,
    };
    wrap.innerHTML = forms[type] || '';
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 5 — Review, Canvas & SQL
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// FEM Canvas state (zoom / pan / maximise)
// ─────────────────────────────────────────────────────────────────────────────
const _FEMC = {
    zoom: 1, panX: 0, panY: 0,
    panning: false, panSX: 0, panSY: 0, panOX: 0, panOY: 0,
    maximised: false, origStyle: null,
    svgW: 0, svgH: 0,   // logical size of the rendered SVG content
};

function _femApplyCanvasTransform() {
    const svg = document.getElementById('fem-canvas-svg');
    if (svg) svg.style.transform = `translate(${_FEMC.panX}px,${_FEMC.panY}px) scale(${_FEMC.zoom})`;
    const lbl = document.getElementById('fem-canvas-zoom-lbl');
    if (lbl) lbl.textContent = Math.round(_FEMC.zoom * 100) + '%';
}

function _femCanvasZoom(delta, cx, cy) {
    const wrap = document.getElementById('fem-canvas-wrap');
    if (!wrap) return;
    const r = wrap.getBoundingClientRect();
    const mx = cx !== undefined ? cx - r.left : r.width / 2;
    const my = cy !== undefined ? cy - r.top  : r.height / 2;
    const prev = _FEMC.zoom;
    _FEMC.zoom = Math.max(0.15, Math.min(4, _FEMC.zoom + delta));
    _FEMC.panX = mx - (mx - _FEMC.panX) * (_FEMC.zoom / prev);
    _FEMC.panY = my - (my - _FEMC.panY) * (_FEMC.zoom / prev);
    _femApplyCanvasTransform();
}

function _femCanvasFitToView() {
    const wrap = document.getElementById('fem-canvas-wrap');
    if (!wrap || !_FEMC.svgW || !_FEMC.svgH) return;
    const PAD = 24;
    const wW = wrap.clientWidth - PAD * 2;
    const wH = wrap.clientHeight - PAD * 2;
    _FEMC.zoom = Math.min(4, Math.max(0.15, Math.min(wW / _FEMC.svgW, wH / _FEMC.svgH)));
    _FEMC.panX = (wrap.clientWidth  - _FEMC.svgW * _FEMC.zoom) / 2;
    _FEMC.panY = (wrap.clientHeight - _FEMC.svgH * _FEMC.zoom) / 2;
    _femApplyCanvasTransform();
}

function _femToggleCanvasMaximise() {
    const container = document.getElementById('fem-canvas-container');
    const btn = document.getElementById('fem-canvas-max-btn');
    if (!container || !btn) return;
    if (!_FEMC.maximised) {
        _FEMC.origStyle = container.getAttribute('style') || '';
        _FEMC.maximised = true;
        container.style.cssText = `
      position:fixed;inset:0;z-index:9990;
      background:var(--bg0,#050810);
      display:flex;flex-direction:column;
      border:none;border-radius:0;
    `;
        btn.textContent = '⊟';
        btn.title = 'Restore';
    } else {
        _FEMC.maximised = false;
        container.setAttribute('style', _FEMC.origStyle || '');
        btn.textContent = '⊞';
        btn.title = 'Maximise canvas';
    }
    setTimeout(_femCanvasFitToView, 60);
}

function _femWireCanvasInteraction() {
    const wrap = document.getElementById('fem-canvas-wrap');
    if (!wrap || wrap._femWired) return;
    wrap._femWired = true;

    // Scroll → zoom
    wrap.addEventListener('wheel', e => {
        e.preventDefault();
        _femCanvasZoom(e.deltaY < 0 ? 0.12 : -0.12, e.clientX, e.clientY);
    }, { passive: false });

    // Drag → pan
    wrap.addEventListener('mousedown', e => {
        if (e.button !== 0) return;
        _FEMC.panning = true;
        _FEMC.panSX = e.clientX; _FEMC.panSY = e.clientY;
        _FEMC.panOX = _FEMC.panX; _FEMC.panOY = _FEMC.panY;
        wrap.style.cursor = 'grabbing';
        e.preventDefault();
    });
    const onMove = e => {
        if (!_FEMC.panning) return;
        _FEMC.panX = _FEMC.panOX + (e.clientX - _FEMC.panSX);
        _FEMC.panY = _FEMC.panOY + (e.clientY - _FEMC.panSY);
        _femApplyCanvasTransform();
    };
    const onUp = () => {
        if (!_FEMC.panning) return;
        _FEMC.panning = false;
        wrap.style.cursor = 'grab';
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);

    // Double-click background → fit to view
    wrap.addEventListener('dblclick', () => {
        _FEMC.zoom = 1; _FEMC.panX = 0; _FEMC.panY = 0;
        _femCanvasFitToView();
    });
}

function _femRenderStep5() {
    _FEM.outputTable = document.getElementById('fem-out-table')?.value?.trim() || _FEM.outputTable || 'features_out';
    const sql = _femGenerateSql();
    _FEM.generatedSql = sql;
    _femSaveHistory();
    _femUpdateHistCount();

    // Reset canvas pan/zoom for fresh render
    _FEMC.zoom = 1; _FEMC.panX = 0; _FEMC.panY = 0;
    _FEMC.maximised = false; _FEMC.origStyle = null;

    const body = document.getElementById('fem-body');
    body.innerHTML = `
<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
  <div>
    <div style="font-size:13px;font-weight:700;color:var(--text0);">Generated Feature Pipeline SQL</div>
    <div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:2px;">
      <span style="color:var(--blue);">${_escFem(_FEM.sourceTable||'source')}</span>
      → <span style="color:var(--accent);">${_FEM.transformations.filter(t=>t.transformId!=='passthrough').length} transforms</span>
      → <span style="color:var(--yellow);">${_FEM.windows.length} window(s)</span>
      → <span style="color:var(--blue);">${_escFem(_FEM.outputTable)}</span>
      <span style="margin-left:6px;background:rgba(0,212,170,0.1);color:var(--accent);padding:1px 7px;border-radius:10px;font-size:9px;font-weight:700;">SAVED TO HISTORY</span>
    </div>
  </div>
  <div style="margin-left:auto;display:flex;gap:6px;">
    <button onclick="navigator.clipboard.writeText(document.getElementById('fem-sql-out').textContent).then(()=>{if(typeof toast==='function')toast('SQL copied','ok');})"
      style="font-size:10px;padding:4px 10px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;font-family:var(--mono);">Copy SQL</button>
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;height:calc(88vh - 255px);min-height:380px;">

  <!-- ── Canvas panel ── -->
  <div id="fem-canvas-container" style="display:flex;flex-direction:column;overflow:hidden;background:var(--bg0);border:1px solid var(--border);border-radius:5px;">
    <!-- Canvas toolbar -->
    <div style="display:flex;align-items:center;gap:4px;padding:5px 8px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;">
      <span style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.2px;text-transform:uppercase;flex:1;">Pipeline Canvas</span>
      <button onclick="_femCanvasZoom(-0.15)" title="Zoom out"
        style="font-size:13px;line-height:1;padding:1px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">−</button>
      <span id="fem-canvas-zoom-lbl" style="font-size:10px;color:var(--text3);font-family:var(--mono);min-width:36px;text-align:center;">100%</span>
      <button onclick="_femCanvasZoom(0.15)" title="Zoom in"
        style="font-size:13px;line-height:1;padding:1px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">+</button>
      <button onclick="_femCanvasFitToView()" title="Fit to view"
        style="font-size:10px;padding:2px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;">⊙ Fit</button>
      <button id="fem-canvas-max-btn" onclick="_femToggleCanvasMaximise()" title="Maximise canvas"
        style="font-size:12px;line-height:1;padding:2px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;">⊞</button>
    </div>
    <!-- Canvas viewport -->
    <div id="fem-canvas-wrap" style="flex:1;overflow:hidden;position:relative;cursor:grab;background:var(--bg0);">
      <svg id="fem-canvas-svg" style="transform-origin:0 0;will-change:transform;display:block;overflow:visible;"></svg>
    </div>
    <!-- Legend -->
    <div style="display:flex;gap:10px;padding:4px 10px;border-top:1px solid var(--border);flex-shrink:0;background:var(--bg2);flex-wrap:wrap;">
      <span style="font-size:9px;color:#4e9de8;font-family:var(--mono);">◉ Source</span>
      <span style="font-size:9px;color:#b080e0;font-family:var(--mono);">⨍ Transform</span>
      <span style="font-size:9px;color:#f5a623;font-family:var(--mono);">▦ Window/Agg</span>
      <span style="font-size:9px;color:#4e9de8;font-family:var(--mono);">▣ Output</span>
      <span style="font-size:9px;color:var(--text3);font-family:var(--mono);margin-left:auto;">scroll=zoom · drag=pan · dblclick=fit</span>
    </div>
  </div>

  <!-- ── SQL panel ── -->
  <div style="display:flex;flex-direction:column;overflow:hidden;">
    <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:7px;">GENERATED SQL</div>
    <pre id="fem-sql-out" class="fem-sql-preview">${_escFem(sql)}</pre>
  </div>
</div>

<div style="margin-top:10px;background:rgba(0,212,170,0.04);border:1px solid rgba(0,212,170,0.15);border-radius:4px;padding:9px 13px;font-size:11px;color:var(--text1);line-height:1.7;">
  <strong style="color:var(--accent);">Next steps:</strong>
  Click <strong>"Insert SQL into Editor"</strong> below to load this into the studio editor, then press <kbd>Ctrl+Enter</kbd> to execute.
  Ensure connector JARs are deployed to <code>/opt/flink/lib/</code> via the Systems Manager.
</div>`;

    setTimeout(() => { _femDrawCanvasSvg(); _femWireCanvasInteraction(); }, 80);
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline Canvas — SVG-based, fully zoomable / pannable
// ─────────────────────────────────────────────────────────────────────────────
function _femDrawCanvasSvg() {
    const svgEl = document.getElementById('fem-canvas-svg');
    const wrap  = document.getElementById('fem-canvas-wrap');
    if (!svgEl || !wrap) return;

    const src  = _FEM.sourceTable || 'source';
    const out  = _FEM.outputTable || 'output';
    const cols = (_FEM.selectedFeatures.length ? _FEM.selectedFeatures : _FEM.rawColumns);
    const txs  = _FEM.transformations.filter(t => t.colName !== '_expr_' && t.transformId !== 'passthrough');
    const wins = _FEM.windows;
    const custExprs = _FEM.transformations.filter(t => t.colName === '_expr_' && t.param);
    const hasNoWin  = (_FEM.noWinGroupBy?.trim() || _FEM.noWinAggs?.trim());

    // ── Layout constants (generous spacing — canvas is infinite, zoom handles it)
    const NODE_W    = 160;
    const SRC_H     = 42;
    const COL_H     = 24;
    const COL_GAP   = 5;
    const TX_H      = 34;
    const TX_GAP    = 10;
    const WIN_H     = 38;
    const WIN_GAP   = 12;
    const OUT_H     = 44;
    const COL_STEP  = COL_H + COL_GAP;
    const HGAP      = 80;   // horizontal gap between column groups
    const PAD       = 24;

    // Column X positions
    const X0 = PAD;                          // source + feature cols
    const X1 = X0 + NODE_W + HGAP;          // transforms
    const X2 = X1 + NODE_W + HGAP;          // windows / agg
    const X3 = X2 + NODE_W + HGAP;          // output

    // ── Compute Y centres for each column so columns are vertically centred
    const totalColH  = cols.length  * COL_STEP  - COL_GAP;
    const activeTx   = txs.length   > 0 ? txs   : [{ alias: 'all features', transformId: 'passthrough', colName: 'columns' }];
    const totalTxH   = activeTx.length * (TX_H + TX_GAP) - TX_GAP;
    const winItems   = [...wins, ...(hasNoWin ? [{ kind:'groupby', groupBy:_FEM.noWinGroupBy||'', aggs:_FEM.noWinAggs||'' }] : [])];
    if (!wins.length && !hasNoWin) winItems.push({ kind:'direct', aggs:'' });
    const totalWinH  = winItems.length * (WIN_H + WIN_GAP) - WIN_GAP;

    const contentH = Math.max(SRC_H + 16 + totalColH, totalTxH, totalWinH, OUT_H) + PAD * 2;
    const midY = contentH / 2;

    const srcY    = PAD;
    const colsY0  = srcY + SRC_H + 16;
    const txY0    = midY - totalTxH  / 2;
    const winY0   = midY - totalWinH / 2;
    const outY    = midY - OUT_H / 2;

    const totalW  = X3 + NODE_W + PAD;
    const totalH  = contentH;

    _FEMC.svgW = totalW;
    _FEMC.svgH = totalH;

    // ── Colour palette
    const C = {
        bg:       '#060a12',
        grid:     'rgba(0,212,170,0.04)',
        src:      { fill:'rgba(26,111,168,0.18)',  stroke:'#4e9de8', text:'#a8d4f5', sub:'#4a7a9a' },
        col:      { fill:'rgba(26,111,168,0.10)',  stroke:'rgba(78,157,232,0.45)', text:'#8ab8d8', type:'#3a6070' },
        tx:       { fill:'rgba(90,42,138,0.18)',   stroke:'#b080e0', text:'#d4b0ff', sub:'#8a60b0' },
        win:      { fill:'rgba(138,106,0,0.18)',   stroke:'#f5a623', text:'#f5c860', sub:'#a07820' },
        out:      { fill:'rgba(10,90,90,0.22)',    stroke:'#00d4aa', text:'#80f0d8', sub:'#30a090' },
        edge:     { src:'rgba(78,157,232,0.50)',   tx:'rgba(176,128,224,0.55)', win:'rgba(245,166,35,0.50)', out:'rgba(0,212,170,0.55)' },
    };

    // ── SVG helpers
    const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    const trunc = (s, n) => (s||'').length > n ? (s||'').slice(0, n) + '…' : (s||'');

    const rr = (x, y, w, h, r, fill, stroke, sw) =>
        `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" ry="${r}"
      fill="${fill}" stroke="${stroke}" stroke-width="${sw||1.5}"/>`;

    const txt = (s, x, y, fill, size, weight, anchor) =>
        `<text x="${x}" y="${y}" fill="${fill}" font-size="${size||10}"
      font-weight="${weight||'400'}" font-family="var(--mono,monospace)"
      text-anchor="${anchor||'start'}" dominant-baseline="middle">${esc(s)}</text>`;

    const bezier = (x1, y1, x2, y2, color, sw) => {
        const cp = Math.abs(x2 - x1) * 0.45;
        return `<path d="M${x1},${y1} C${x1+cp},${y1} ${x2-cp},${y2} ${x2},${y2}"
      stroke="${color}" stroke-width="${sw||1.4}" fill="none" opacity="0.85"
      marker-end="url(#fem-arr)"/>`;
    };

    // ── Icon helpers (inline SVG text glyphs for each type)
    const iconGlyph = { src:'◉', col:'·', tx:'⨍', win:'▦', out:'▣', nowin:'∑', direct:'→' };

    let s = `<defs>
    <marker id="fem-arr" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
      <path d="M0,0.5 L0,6.5 L7,3.5 z" fill="rgba(150,180,220,0.5)"/>
    </marker>
    <filter id="fem-glow"><feGaussianBlur stdDeviation="2" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
  </defs>`;

    // Grid
    s += `<rect width="${totalW}" height="${totalH}" fill="${C.bg}"/>`;
    for (let x = 0; x <= totalW; x += 22)
        s += `<line x1="${x}" y1="0" x2="${x}" y2="${totalH}" stroke="${C.grid}" stroke-width="0.5"/>`;
    for (let y = 0; y <= totalH; y += 22)
        s += `<line x1="0" y1="${y}" x2="${totalW}" y2="${y}" stroke="${C.grid}" stroke-width="0.5"/>`;

    // ── Column header labels
    const colHeaders = [
        { x: X0, label: 'SOURCE' },
        { x: X1, label: 'TRANSFORMS' },
        { x: X2, label: 'WINDOWS & AGG' },
        { x: X3, label: 'OUTPUT' },
    ];
    colHeaders.forEach(h => {
        s += txt(h.label, h.x, PAD - 8, 'rgba(120,160,200,0.45)', 8, '700', 'start');
    });

    // ── Source node
    const srcMidY = srcY + SRC_H / 2;
    s += rr(X0, srcY, NODE_W, SRC_H, 6, C.src.fill, C.src.stroke, 2);
    s += txt(iconGlyph.src, X0 + 8, srcMidY, C.src.stroke, 13, '400', 'start');
    s += txt(trunc(src, 16), X0 + 26, srcMidY - 6, C.src.text, 11, '700', 'start');
    s += txt(`${cols.length} cols · ${(_FEM.sourceFormat||'kafka').toUpperCase()}`, X0 + 26, srcMidY + 8, C.src.sub, 9, '400', 'start');

    // ── Feature column nodes + edges from source
    const colRxArr = [];
    cols.forEach((c, i) => {
        const y = colsY0 + i * COL_STEP;
        const mid = y + COL_H / 2;
        s += rr(X0, y, NODE_W, COL_H, 3, C.col.fill, C.col.stroke, 1);
        s += txt(trunc(c.name, 14), X0 + 8, mid, C.col.text, 10, '500', 'start');
        s += txt(c.type, X0 + NODE_W - 6, mid, C.col.type, 8, '400', 'end');
        // Edge: source right → col right-edge midpoint (vertical fan)
        s += `<line x1="${X0 + NODE_W}" y1="${srcY + SRC_H}" x2="${X0 + NODE_W}" y2="${mid}"
      stroke="${C.edge.src}" stroke-width="1" opacity="0.4"/>`;
        colRxArr.push({ x: X0 + NODE_W, y: mid });
    });
    // Vertical trunk from source to first/last col
    if (cols.length) {
        s += `<line x1="${X0+NODE_W}" y1="${srcY+SRC_H}" x2="${X0+NODE_W}" y2="${colsY0 + (cols.length-1)*COL_STEP + COL_H/2}"
      stroke="${C.edge.src}" stroke-width="1" opacity="0.25"/>`;
    }

    // ── Transform nodes + edges from feature cols
    const txRxArr = [];
    activeTx.forEach((t, i) => {
        const y = txY0 + i * (TX_H + TX_GAP);
        const mid = y + TX_H / 2;
        const alias = trunc(t.alias || t.colName || 'transform', 14);
        const subLbl = (t.transformId || 'pass').toUpperCase().slice(0, 12);
        s += rr(X1, y, NODE_W, TX_H, 5, C.tx.fill, C.tx.stroke, 1.8);
        s += txt(iconGlyph.tx, X1 + 7, mid, C.tx.stroke, 13, '400', 'start');
        s += txt(alias, X1 + 23, mid - 6, C.tx.text, 11, '600', 'start');
        s += txt(subLbl, X1 + 23, mid + 7, C.tx.sub, 8, '400', 'start');
        // Edges: connect a proportional subset of colRxArr → this tx node
        const step = Math.max(1, Math.floor(colRxArr.length / activeTx.length));
        const start = i * step;
        const end   = Math.min(colRxArr.length, start + step + (i === activeTx.length - 1 ? colRxArr.length : 0));
        for (let ci = start; ci < Math.min(colRxArr.length, end + 1); ci++) {
            s += bezier(colRxArr[ci].x, colRxArr[ci].y, X1, mid, C.edge.tx, 1.2);
        }
        txRxArr.push({ x: X1 + NODE_W, y: mid });
    });

    // ── Window / aggregation nodes + edges from transforms
    const winRxArr = [];
    winItems.forEach((w, i) => {
        const y = winY0 + i * (WIN_H + WIN_GAP);
        const mid = y + WIN_H / 2;
        let label, sub, icon, strokeColor, fillColor;
        if (w.kind === 'groupby') {
            label = 'GROUP BY';
            sub   = trunc(w.groupBy || 'keyless', 16);
            icon  = iconGlyph.nowin;
            strokeColor = C.win.stroke; fillColor = C.win.fill;
        } else if (w.kind === 'direct') {
            label = 'STREAM THRU';
            sub   = 'no aggregation';
            icon  = iconGlyph.direct;
            strokeColor = C.out.stroke; fillColor = 'rgba(0,212,170,0.08)';
        } else {
            label = (w.kind || 'TUMBLE').toUpperCase();
            sub   = trunc(w.size || w.gap || '', 14);
            icon  = iconGlyph.win;
            strokeColor = C.win.stroke; fillColor = C.win.fill;
        }
        s += rr(X2, y, NODE_W, WIN_H, 5, fillColor, strokeColor, 1.8);
        s += txt(icon, X2 + 7, mid, strokeColor, 13, '400', 'start');
        s += txt(label, X2 + 23, mid - 7, C.win.text, 11, '700', 'start');
        s += txt(sub, X2 + 23, mid + 7, C.win.sub, 9, '400', 'start');
        // Edges from all tx nodes
        txRxArr.forEach(tr => s += bezier(tr.x, tr.y, X2, mid, C.edge.win, 1.3));
        winRxArr.push({ x: X2 + NODE_W, y: mid });
    });

    // Custom expression nodes (shown below windows)
    if (custExprs.length) {
        const cy0 = winY0 + winItems.length * (WIN_H + WIN_GAP) + 8;
        custExprs.slice(0, 4).forEach((e, i) => {
            const y   = cy0 + i * (TX_H + TX_GAP);
            const mid = y + TX_H / 2;
            s += rr(X2, y, NODE_W, TX_H, 4, C.tx.fill, C.tx.stroke, 1.4);
            s += txt('⨍', X2 + 6, mid, C.tx.stroke, 12);
            s += txt(trunc(e.param || 'expr', 16), X2 + 20, mid - 5, C.tx.text, 10, '500');
            s += txt('CUSTOM EXPR', X2 + 20, mid + 6, C.tx.sub, 8);
            winRxArr.push({ x: X2 + NODE_W, y: mid });
        });
    }

    // ── Output node + edges from windows
    const outMidY = outY + OUT_H / 2;
    s += rr(X3, outY, NODE_W, OUT_H, 6, C.out.fill, C.out.stroke, 2.5);
    s += txt(iconGlyph.out, X3 + 7, outMidY, C.out.stroke, 14, '400', 'start');
    s += txt(trunc(out, 14), X3 + 25, outMidY - 7, C.out.text, 11, '700', 'start');
    s += txt((_FEM.sinkType || 'sink').toUpperCase(), X3 + 25, outMidY + 7, C.out.sub, 9, '400', 'start');
    // Glow on output
    s += `<rect x="${X3}" y="${outY}" width="${NODE_W}" height="${OUT_H}" rx="6"
    fill="none" stroke="${C.out.stroke}" stroke-width="1" opacity="0.2" filter="url(#fem-glow)"/>`;
    winRxArr.forEach(wr => s += bezier(wr.x, wr.y, X3, outMidY, C.edge.out, 1.5));

    svgEl.setAttribute('width',  totalW);
    svgEl.setAttribute('height', totalH);
    svgEl.setAttribute('viewBox', `0 0 ${totalW} ${totalH}`);
    svgEl.innerHTML = s;

    // Auto-fit after render
    setTimeout(_femCanvasFitToView, 30);
}

// LEGACY stub kept so existing setTimeout calls don't throw
function _femDrawCanvas() { _femDrawCanvasSvg(); }

// ── dummy for old code that called _femAddWindow inline ─────────────────────
function _femAddWindow() {
    _FEM.windows.push({
        kind:'tumble', timeCol:_FEM.timeCol||'', size:'5 MINUTE',
        slide:'1 MINUTE', gap:'30 SECOND', maxSize:'1 HOUR', groupBy:'', aggs:''
    });
    _femRenderWindowsList();
}


// ─────────────────────────────────────────────────────────────────────────────
// SQL Generation
// ─────────────────────────────────────────────────────────────────────────────
const _femBuildSelectExpr = (t) => {
    const c = t.colName, p = t.param || '', a = t.alias || c;
    switch (t.transformId) {
        case 'passthrough':   return c === a ? c : `${c} AS ${a}`;
        case 'cast':          return `CAST(${c} AS ${p || 'DOUBLE'}) AS ${a}`;
        case 'math':          return `${p || c} AS ${a}`;
        case 'coalesce':      return `COALESCE(${c}, ${p || 0}) AS ${a}`;
        case 'ifnull':        return `IFNULL(${c}, ${p || 0}) AS ${a}`;
        case 'upper':         return `UPPER(${c}) AS ${a}`;
        case 'lower':         return `LOWER(${c}) AS ${a}`;
        case 'trim':          return `TRIM(${c}) AS ${a}`;
        case 'substring':     return `SUBSTRING(${c} FROM ${p || '1 FOR 3'}) AS ${a}`;
        case 'round':         return `ROUND(${c}, ${p || 2}) AS ${a}`;
        case 'abs':           return `ABS(${c}) AS ${a}`;
        case 'log':           return `LN(${c}) AS ${a}`;
        case 'hash_encode':   return `MD5(CAST(${c} AS STRING)) AS ${a}`;
        case 'date_format':   return `DATE_FORMAT(${c}, '${p || 'yyyy-MM-dd'}') AS ${a}`;
        case 'ts_to_epoch':   return `UNIX_TIMESTAMP(CAST(${c} AS STRING)) AS ${a}`;
        case 'epoch_to_ts':   return `FROM_UNIXTIME(${c}) AS ${a}`;
        case 'lag':           return `LAG(${c}, 1) OVER (PARTITION BY ${p || 'id'} ORDER BY ${_FEM.timeCol || 'event_time'}) AS ${a}_lag`;
        case 'normalise': {
            const [mn, mx] = (p || '0,1').split(',').map(x => x.trim());
            return `(${c} - ${mn || 0}) / NULLIF((${mx || 1} - ${mn || 0}), 0) AS ${a}_norm`;
        }
        case 'bucket': {
            const thresholds = (p || '').split(',').map(x => x.trim()).filter(Boolean);
            if (!thresholds.length) return `${c} AS ${a}`;
            let cs = 'CASE';
            thresholds.forEach((t2, i) => { cs += `\n    WHEN ${c} < ${t2} THEN 'bucket_${i}'`; });
            cs += `\n    ELSE 'bucket_${thresholds.length}'\n  END AS ${a}`;
            return cs;
        }
        case 'custom': return p || `${c} AS ${a}`;
        default:       return c === a ? c : `${c} AS ${a}`;
    }
};

function _femGenerateSql() {
    const src  = _FEM.sourceTable || 'source_table';
    const out  = _FEM.outputTable || 'features_out';
    const cols = _FEM.selectedFeatures.length ? _FEM.selectedFeatures : _FEM.rawColumns;
    const wmd  = _FEM.watermarkDelay || '5 SECOND';
    const tc   = _FEM.timeCol || '';

    const lines = [];
    lines.push('-- ═══════════════════════════════════════════════════════════════════════');
    lines.push(`-- Feature Engineering Pipeline`);
    lines.push(`-- Source : ${src}   Sink: ${out}`);
    lines.push(`-- Generated: ${new Date().toISOString()}`);
    lines.push('-- Str:::lab Studio — Feature Engineering Manager');
    lines.push('-- ═══════════════════════════════════════════════════════════════════════\n');

    lines.push("SET 'execution.runtime-mode' = 'streaming';");
    lines.push("SET 'parallelism.default' = '4';");
    lines.push("SET 'execution.checkpointing.interval' = '10000';");
    lines.push("SET 'table.exec.state.ttl' = '3600000';\n");

    // Source DDL
    const schemaCols = cols.map(c => `  ${c.name.padEnd(30)} ${c.type}`);
    if (tc) {
        const tcol = cols.find(c => c.name === tc);
        const needsCast = tcol && /BIGINT|INT/i.test(tcol.type);
        if (needsCast) {
            schemaCols.push(`  ${(tc + '_ts').padEnd(30)} AS TO_TIMESTAMP_LTZ(${tc}, 3)`);
            schemaCols.push(`  WATERMARK FOR ${tc}_ts AS ${tc}_ts - INTERVAL '${wmd}'`);
        } else {
            schemaCols.push(`  WATERMARK FOR ${tc} AS ${tc} - INTERVAL '${wmd}'`);
        }
    }

    lines.push(`-- Source: ${src}`);
    lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${src} (`);
    lines.push(schemaCols.join(',\n'));
    lines.push(`) WITH (`);
    if (_FEM.sourceFormat === 'kafka') {
        lines.push(`  'connector'                    = 'kafka',`);
        lines.push(`  'topic'                        = '<REPLACE_ME:topic>',`);
        lines.push(`  'properties.bootstrap.servers' = '<REPLACE_ME:bootstrap.servers>',`);
        lines.push(`  'properties.group.id'          = 'feature-pipeline-group',`);
        lines.push(`  'scan.startup.mode'            = 'latest-offset',`);
        lines.push(`  'format'                       = 'json'`);
    } else if (_FEM.sourceFormat === 'datagen') {
        lines.push(`  'connector'       = 'datagen',`);
        lines.push(`  'rows-per-second' = '100'`);
    } else {
        lines.push(`  'connector' = '${_FEM.sourceFormat}'`);
    }
    lines.push(`);\n`);

    // Sink DDL
    lines.push(`-- Sink: ${out} (${_FEM.sinkType})`);
    if (_FEM.sinkType === 'kafka') {
        lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} WITH (`);
        lines.push(`  'connector'                    = 'kafka',`);
        lines.push(`  'topic'                        = '<REPLACE_ME:output_topic>',`);
        lines.push(`  'properties.bootstrap.servers' = '<REPLACE_ME:bootstrap.servers>',`);
        lines.push(`  'format'                       = 'json'`);
        lines.push(`) LIKE ${src} (EXCLUDING ALL);\n`);
    } else if (_FEM.sinkType === 'jdbc') {
        lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} (`);
        cols.slice(0, 4).forEach(c => lines.push(`  ${c.name.padEnd(30)} ${c.type},`));
        lines.push(`  PRIMARY KEY (${cols[0]?.name || 'id'}) NOT ENFORCED`);
        lines.push(`) WITH (`);
        lines.push(`  'connector'  = 'jdbc',`);
        lines.push(`  'url'        = '<REPLACE_ME:jdbc_url>',`);
        lines.push(`  'table-name' = '${out}',`);
        lines.push(`  'username'   = '<REPLACE_ME>',`);
        lines.push(`  'password'   = '<REPLACE_ME>'`);
        lines.push(`);\n`);
    } else if (_FEM.sinkType === 'iceberg') {
        lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} (`);
        cols.slice(0, 4).forEach(c => lines.push(`  ${c.name.padEnd(30)} ${c.type},`));
        lines.push(`  PRIMARY KEY (${cols[0]?.name || 'id'}) NOT ENFORCED`);
        lines.push(`) WITH (`);
        lines.push(`  'connector'        = 'iceberg',`);
        lines.push(`  'catalog-name'     = '<REPLACE_ME:catalog>',`);
        lines.push(`  'catalog-database' = '<REPLACE_ME:db>',`);
        lines.push(`  'catalog-table'    = '${out}',`);
        lines.push(`  'catalog-type'     = 'hive',`);
        lines.push(`  'warehouse'        = '<REPLACE_ME:warehouse_path>'`);
        lines.push(`);\n`);
    } else if (_FEM.sinkType === 'print') {
        lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} WITH (`);
        lines.push(`  'connector' = 'print'`);
        lines.push(`) LIKE ${src} (EXCLUDING ALL);\n`);
    } else if (_FEM.sinkType === 'blackhole') {
        lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} WITH (`);
        lines.push(`  'connector' = 'blackhole'`);
        lines.push(`) LIKE ${src} (EXCLUDING ALL);\n`);
    } else {
        lines.push(`-- TODO: Add ${_FEM.sinkType} connector DDL for table ${out}\n`);
    }

    // Build select exprs from transformations
    const colExprs   = _FEM.transformations.filter(t => t.colName !== '_expr_');
    const custExprs  = _FEM.transformations.filter(t => t.colName === '_expr_');

    // Window INSERT statements
    _FEM.windows.forEach((w, wi) => {
        lines.push(`-- Window ${wi + 1}: ${w.kind.toUpperCase()} — ${w.size || w.gap || ''}`);
        const selItems = [];
        if (w.groupBy?.trim()) w.groupBy.split(',').map(c => c.trim()).filter(Boolean).forEach(c => selItems.push(c));
        selItems.push('window_start', 'window_end');
        if (w.aggs) w.aggs.split('\n').map(l => l.trim()).filter(Boolean).forEach(a => selItems.push(a));
        lines.push(`INSERT INTO ${out}`);
        lines.push(`SELECT`);
        lines.push('  ' + selItems.join(',\n  '));
        const tcol = w.timeCol || tc || 'event_time';
        if (w.kind === 'tumble') {
            lines.push(`FROM TABLE(TUMBLE(TABLE ${src}, DESCRIPTOR(${tcol}), INTERVAL '${w.size || '5 MINUTE'}'))`);
        } else if (w.kind === 'hop') {
            lines.push(`FROM TABLE(HOP(TABLE ${src}, DESCRIPTOR(${tcol}), INTERVAL '${w.slide || '1 MINUTE'}', INTERVAL '${w.size || '5 MINUTE'}'))`);
        } else if (w.kind === 'session') {
            lines.push(`FROM TABLE(SESSION(TABLE ${src}, DESCRIPTOR(${tcol}), DESCRIPTOR(${w.groupBy?.split(',')[0]?.trim() || 'user_id'}), INTERVAL '${w.gap || '30 SECOND'}'))`);
        } else if (w.kind === 'cumulate') {
            lines.push(`FROM TABLE(CUMULATE(TABLE ${src}, DESCRIPTOR(${tcol}), INTERVAL '${w.size || '5 MINUTE'}', INTERVAL '${w.maxSize || '1 HOUR'}'))`);
        }
        const gb = [(w.groupBy || ''), 'window_start, window_end'].filter(Boolean).join(', ');
        lines.push(`GROUP BY ${gb};\n`);
    });

    // No-window aggregation
    if (_FEM.noWinGroupBy?.trim() || _FEM.noWinAggs?.trim()) {
        lines.push('-- Keyless group aggregation (no window)');
        const gbCols = (_FEM.noWinGroupBy || '').split(',').map(c => c.trim()).filter(Boolean);
        const aggLines = (_FEM.noWinAggs || '').split('\n').map(l => l.trim()).filter(Boolean);
        const selItems = [...gbCols, ...aggLines];
        lines.push(`INSERT INTO ${out}`);
        lines.push('SELECT');
        lines.push('  ' + selItems.join(',\n  '));
        lines.push(`FROM ${src}`);
        if (_FEM.noWinGroupBy?.trim()) lines.push(`GROUP BY ${_FEM.noWinGroupBy}`);
        if (_FEM.noWinHaving?.trim())  lines.push(`HAVING ${_FEM.noWinHaving}`);
        lines.push(';\n');
    }

    // Direct transformation (no windows and no group-by → emit enriched stream)
    if (!_FEM.windows.length && !_FEM.noWinGroupBy?.trim()) {
        lines.push('-- Direct feature transformation stream');
        const selItems = colExprs.map(t => _femBuildSelectExpr(t));
        custExprs.forEach(t => { if (t.param) selItems.push(t.param); });
        if (!selItems.length) selItems.push('*');
        lines.push(`INSERT INTO ${out}`);
        lines.push('SELECT');
        lines.push('  ' + selItems.join(',\n  '));
        lines.push(`FROM ${src};\n`);
    }

    return lines.join('\n');
}

function _femInsertSql() {
    const sql = _FEM.generatedSql || _femGenerateSql();
    if (!sql) { if (typeof toast === 'function') toast('Build pipeline first', 'warn'); return; }
    const ed = document.getElementById('sql-editor');
    if (!ed) return;
    const prefix = ed.value.trim() ? '\n\n' : '';
    ed.value += prefix + sql + '\n';
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('fem-modal');
    if (typeof toast === 'function') toast('Feature pipeline SQL inserted into editor', 'ok');
}

// ─────────────────────────────────────────────────────────────────────────────
// History Panel
// ─────────────────────────────────────────────────────────────────────────────
function _femShowHistory() {
    const old = document.getElementById('fem-hist-modal');
    if (old) { old.remove(); return; }
    if (!_FEM.history.length) { if (typeof toast === 'function') toast('No history yet', 'info'); return; }

    const m = document.createElement('div');
    m.id = 'fem-hist-modal';
    m.style.cssText = 'position:fixed;z-index:10003;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--bg2);border:1px solid var(--border2);border-radius:8px;box-shadow:0 12px 48px rgba(0,0,0,0.7);width:620px;max-height:80vh;display:flex;flex-direction:column;overflow:hidden;';
    m.innerHTML = `
<div style="padding:12px 16px;background:var(--bg1);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-shrink:0;">
  <span style="font-size:13px;font-weight:700;color:var(--text0);">🕓 Feature Engineering History</span>
  <span style="font-size:10px;color:var(--text3);font-family:var(--mono);">${_FEM.history.length} entries</span>
  <button onclick="document.getElementById('fem-hist-modal').remove()" style="margin-left:auto;background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;">×</button>
</div>
<div style="flex:1;overflow-y:auto;padding:12px;">
  ${_FEM.history.map((h, i) => `
  <div class="fem-hist-item" onclick="_femLoadFromHistory(${i})" title="Click to load SQL into editor">
    <div style="flex:1;min-width:0;">
      <div style="font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
        ${_escFem(h.sourceTable)} → ${_escFem(h.outputTable)}
      </div>
      <div style="font-size:10px;color:var(--text3);margin-top:2px;display:flex;gap:10px;flex-wrap:wrap;">
        <span>${h.columnCount} cols</span>
        <span>${h.transformCount} transforms</span>
        <span>${h.windowCount} window(s)</span>
        <span style="color:var(--blue);">${h.sinkType}</span>
        <span style="margin-left:auto;">${new Date(h.ts).toLocaleString()}</span>
      </div>
    </div>
    <div style="display:flex;gap:5px;flex-shrink:0;">
      <button onclick="event.stopPropagation();_femLoadFromHistory(${i})"
        style="font-size:10px;padding:3px 9px;border-radius:3px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;font-family:var(--mono);">Insert SQL</button>
      <button onclick="event.stopPropagation();_FEM.history.splice(${i},1);try{localStorage.setItem('strlabstudio_fem_history',JSON.stringify(_FEM.history));}catch(_){};_femUpdateHistCount();document.getElementById('fem-hist-modal').remove();if(typeof toast==='function')toast('Entry removed','ok');"
        style="font-size:10px;padding:3px 7px;border-radius:3px;background:rgba(255,77,109,0.07);border:1px solid rgba(255,77,109,0.3);color:var(--red);cursor:pointer;">×</button>
    </div>
  </div>`).join('')}
</div>
<div style="padding:10px 16px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end;background:var(--bg1);flex-shrink:0;">
  <button onclick="if(confirm('Clear all history?')){_FEM.history=[];try{localStorage.removeItem('strlabstudio_fem_history');}catch(_){}; _femUpdateHistCount();document.getElementById('fem-hist-modal').remove();if(typeof toast==='function')toast('History cleared','ok');}"
    style="font-size:11px;padding:5px 12px;border-radius:3px;background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.3);color:var(--red);cursor:pointer;font-family:var(--mono);">Clear All History</button>
  <button onclick="document.getElementById('fem-hist-modal').remove()"
    style="font-size:11px;padding:5px 12px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Close</button>
</div>`;
    document.body.appendChild(m);
}

function _femLoadFromHistory(i) {
    const h = _FEM.history[i];
    if (!h || !h.sql) { if (typeof toast === 'function') toast('No SQL in this entry', 'warn'); return; }
    const ed = document.getElementById('sql-editor');
    if (!ed) return;
    const prefix = ed.value.trim() ? '\n\n' : '';
    ed.value += prefix + h.sql + '\n';
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    const hm = document.getElementById('fem-hist-modal');
    if (hm) hm.remove();
    closeModal('fem-modal');
    if (typeof toast === 'function') toast(`Historical pipeline SQL loaded: ${h.sourceTable} → ${h.outputTable}`, 'ok');
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────
function _escFem(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function _escId(s) {
    return String(s || '').replace(/[^a-zA-Z0-9_]/g, '_');
}