/**
 * inference-manager.js  —  Str:::lab Studio
 * ─────────────────────────────────────────────────────────────────────────────
 * AI Model Inference Manager
 * Configure real-time ML model inference on Flink streaming pipelines.
 * Supports MLflow, MinIO, SageMaker, Azure ML, Vertex AI, BentoML,
 * Triton, TorchServe, TF Serving, HuggingFace, OpenAI-compatible,
 * Anthropic, Cohere, and fully bespoke/custom endpoints.
 * Generates production-ready Flink SQL with async UDF patterns.
 * ─────────────────────────────────────────────────────────────────────────────
 */

// ── State ─────────────────────────────────────────────────────────────────────
const _IFM = {
    step: 0,
    sourceTable: '',
    inputColumns: [],
    selectedInputCols: [],
    modelServer: 'mlflow',
    modelConfig: {},
    authType: 'bearer',
    authConfig: {},
    inferenceConfig: {
        inputCol: '',
        outputAlias: 'prediction',
        outputType: 'DOUBLE',
        batchSize: '1',
        timeoutMs: '5000',
        retries: '2',
        asyncParallelism: '4',
        passthroughCols: '',
        preProcessExpr: '',
        postProcessExpr: '',
    },
    outputTable: '',
    sinkType: 'kafka',
    generatedSql: '',
    history: [],
};

(function () {
    try {
        const raw = localStorage.getItem('strlabstudio_ifm_history');
        if (raw) _IFM.history = JSON.parse(raw);
    } catch (_) {}
})();

function _ifmSaveHistory() {
    try {
        const entry = {
            id: 'ifm_' + Date.now(),
            ts: new Date().toISOString(),
            sourceTable: _IFM.sourceTable,
            outputTable: _IFM.outputTable,
            modelServer: _IFM.modelServer,
            outputAlias: _IFM.inferenceConfig.outputAlias,
            sql: _IFM.generatedSql,
        };
        _IFM.history.unshift(entry);
        if (_IFM.history.length > 20) _IFM.history.pop();
        localStorage.setItem('strlabstudio_ifm_history', JSON.stringify(_IFM.history));
    } catch (_) {}
}

function openInferenceManager() {
    if (!document.getElementById('ifm-modal')) _ifmBuildModal();
    openModal('ifm-modal');
    _ifmGoStep(0);
}

// ── CSS ───────────────────────────────────────────────────────────────────────
function _ifmInjectCss() {
    if (document.getElementById('ifm-css')) return;
    const s = document.createElement('style');
    s.id = 'ifm-css';
    s.textContent = `
#ifm-modal .modal{width:min(1160px,97vw);height:90vh;max-height:90vh;display:flex;flex-direction:column;overflow:hidden;}
#ifm-wizard-wrap{flex:1;display:flex;flex-direction:column;overflow:hidden;}
#ifm-step-bar{display:flex;align-items:center;gap:0;background:var(--bg2);border-bottom:1px solid var(--border);padding:0 16px;flex-shrink:0;overflow-x:auto;}
.ifm-step-item{display:flex;align-items:center;gap:6px;padding:9px 12px;font-size:11px;font-family:var(--mono);color:var(--text3);white-space:nowrap;cursor:pointer;border-bottom:2px solid transparent;transition:all 0.15s;user-select:none;}
.ifm-step-item.active{color:var(--blue,#4fa3e0);border-bottom-color:var(--blue,#4fa3e0);font-weight:700;}
.ifm-step-item.done{color:var(--green);}
.ifm-step-item.done .ifm-step-num{background:var(--green);color:#000;}
.ifm-step-num{width:17px;height:17px;border-radius:50%;background:var(--bg3);color:var(--text3);font-size:9px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;}
.ifm-step-item.active .ifm-step-num{background:var(--blue,#4fa3e0);color:#000;}
.ifm-step-connector{width:20px;height:1px;background:var(--border);flex-shrink:0;}
#ifm-body{flex:1;overflow-y:auto;padding:20px 24px;}
#ifm-footer{flex-shrink:0;padding:11px 20px;border-top:1px solid var(--border);background:var(--bg2);display:flex;align-items:center;gap:8px;}
#ifm-validation-banner{display:none;background:rgba(255,77,109,0.08);border-bottom:1px solid rgba(255,77,109,0.3);padding:6px 18px;font-size:11px;color:var(--red);font-family:var(--mono);flex-shrink:0;}
.ifm-section-title{font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:9px;padding-bottom:4px;border-bottom:1px solid var(--border);}
.ifm-card{background:var(--bg2);border:1px solid var(--border);border-radius:5px;padding:13px 15px;margin-bottom:11px;}
.ifm-info-box{background:rgba(79,163,224,0.05);border:1px solid rgba(79,163,224,0.2);border-left:3px solid var(--blue,#4fa3e0);border-radius:3px;padding:8px 12px;font-size:11px;color:var(--text1);line-height:1.7;margin-bottom:13px;}
.ifm-warn-box{background:rgba(245,166,35,0.06);border:1px solid rgba(245,166,35,0.25);border-left:3px solid var(--yellow,#f5a623);border-radius:3px;padding:8px 12px;font-size:11px;color:var(--text1);line-height:1.7;margin-bottom:10px;}
.ifm-server-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:8px;margin-bottom:14px;}
.ifm-server-card{padding:10px 12px;border:1px solid var(--border2);border-radius:5px;cursor:pointer;background:var(--bg3);transition:all 0.12s;user-select:none;}
.ifm-server-card:hover{background:var(--bg1);border-color:rgba(79,163,224,0.4);}
.ifm-server-card.selected{background:rgba(79,163,224,0.1);border-color:var(--blue,#4fa3e0);box-shadow:0 0 0 1px rgba(79,163,224,0.3);}
.ifm-server-card .ifm-sc-icon{font-size:20px;margin-bottom:5px;display:block;}
.ifm-server-card .ifm-sc-name{font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);}
.ifm-server-card .ifm-sc-desc{font-size:9px;color:var(--text3);margin-top:2px;line-height:1.4;}
.ifm-col-tag{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:3px;font-size:10px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;transition:all 0.1s;user-select:none;margin:2px;}
.ifm-col-tag.selected{background:rgba(79,163,224,0.12);border-color:rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);}
.ifm-col-tag .ifm-tag-type{font-size:8px;opacity:0.55;margin-left:2px;}
.ifm-auth-btn{display:flex;align-items:center;gap:6px;padding:6px 12px;border:1px solid var(--border2);border-radius:3px;cursor:pointer;font-size:10px;font-family:var(--mono);color:var(--text1);background:var(--bg3);transition:all 0.12s;user-select:none;}
.ifm-auth-btn:hover{background:var(--bg1);}
.ifm-auth-btn.selected{background:rgba(79,163,224,0.1);border-color:rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);font-weight:700;}
.ifm-field-label{font-size:10px;color:var(--text2);margin-bottom:3px;display:block;font-family:var(--mono);}
.ifm-required{color:var(--red);}
.ifm-sql-preview{background:var(--bg0);border:1px solid var(--border);border-radius:5px;padding:12px;font-size:10px;font-family:var(--mono);color:var(--text1);line-height:1.75;white-space:pre;overflow:auto;height:100%;}
.ifm-hist-item{padding:9px 12px;border:1px solid var(--border);border-radius:4px;background:var(--bg2);margin-bottom:6px;display:flex;align-items:center;gap:10px;cursor:pointer;transition:background 0.1s;}
.ifm-hist-item:hover{background:var(--bg1);}
.ifm-badge{display:inline-block;font-size:9px;padding:2px 7px;border-radius:10px;font-weight:600;font-family:var(--mono);background:rgba(79,163,224,0.1);color:var(--blue,#4fa3e0);margin-left:5px;}
`;
    document.head.appendChild(s);
}

// ── Server catalogue ───────────────────────────────────────────────────────────
const IFM_SERVERS = [
    // Managed MLOps Platforms
    { id:'mlflow',       group:'MLOps Platforms',   icon:'🔬', name:'MLflow',            desc:'MLflow Model Registry & Serving',   authTypes:['bearer','basic','none'] },
    { id:'sagemaker',    group:'MLOps Platforms',   icon:'☁', name:'AWS SageMaker',      desc:'SageMaker Endpoints (IAM/AK)',       authTypes:['aws_sigv4','aws_keys'] },
    { id:'azureml',      group:'MLOps Platforms',   icon:'🔷', name:'Azure ML',           desc:'Azure ML Online Endpoints',          authTypes:['azure_ad','api_key'] },
    { id:'vertexai',     group:'MLOps Platforms',   icon:'⬡', name:'Google Vertex AI',   desc:'Vertex AI Endpoints',                authTypes:['google_sa','bearer'] },
    { id:'databricks',   group:'MLOps Platforms',   icon:'🧱', name:'Databricks',         desc:'Databricks Model Serving',           authTypes:['bearer'] },
    // Open-Source Serving
    { id:'mlflow_serve', group:'Open-Source Serving',icon:'🧪', name:'MLflow pyfunc',     desc:'mlflow models serve REST API',       authTypes:['none','bearer'] },
    { id:'bentoml',      group:'Open-Source Serving',icon:'🍱', name:'BentoML',           desc:'BentoML Serve REST / gRPC',          authTypes:['api_key','none'] },
    { id:'triton',       group:'Open-Source Serving',icon:'🔱', name:'Triton Inference',  desc:'NVIDIA Triton (HTTP/gRPC)',           authTypes:['none','bearer'] },
    { id:'torchserve',   group:'Open-Source Serving',icon:'🔥', name:'TorchServe',        desc:'PyTorch TorchServe Management API',  authTypes:['none','bearer'] },
    { id:'tfserving',    group:'Open-Source Serving',icon:'🧠', name:'TF Serving',        desc:'TensorFlow Serving REST API',        authTypes:['none','bearer'] },
    { id:'ray',          group:'Open-Source Serving',icon:'🌞', name:'Ray Serve',         desc:'Ray Serve HTTP endpoints',           authTypes:['none','bearer'] },
    { id:'seldon',       group:'Open-Source Serving',icon:'🚀', name:'Seldon Core',       desc:'Seldon V2 Protocol (Istio)',         authTypes:['bearer','none'] },
    { id:'kserve',       group:'Open-Source Serving',icon:'🎯', name:'KServe',            desc:'KServe InferenceService (K8s)',       authTypes:['bearer','none'] },
    // Model Registries / Artifact Stores
    { id:'minio',        group:'Artifact Stores',   icon:'🗄', name:'MinIO / S3',         desc:'Load model artifact from object store', authTypes:['minio_keys','aws_keys'] },
    { id:'huggingface',  group:'Hosted APIs',       icon:'🤗', name:'HuggingFace Hub',    desc:'Inference API / Endpoints',          authTypes:['bearer'] },
    // Hosted LLM APIs
    { id:'openai',       group:'Hosted APIs',       icon:'✦', name:'OpenAI',              desc:'GPT-4o, GPT-4, Embeddings',          authTypes:['api_key'] },
    { id:'anthropic',    group:'Hosted APIs',       icon:'✧', name:'Anthropic Claude',    desc:'Claude 3.5 / Claude 4 series',       authTypes:['api_key','x_api_key'] },
    { id:'cohere',       group:'Hosted APIs',       icon:'🔵', name:'Cohere',             desc:'Command R+, Embed, Classify',        authTypes:['api_key','bearer'] },
    { id:'mistral',      group:'Hosted APIs',       icon:'💨', name:'Mistral AI',         desc:'Mistral, Mixtral endpoints',         authTypes:['api_key'] },
    { id:'together',     group:'Hosted APIs',       icon:'🌐', name:'Together AI',        desc:'Open models via Together API',       authTypes:['api_key'] },
    { id:'bedrock',      group:'Hosted APIs',       icon:'🪨', name:'AWS Bedrock',        desc:'Claude, Titan, Llama via Bedrock',   authTypes:['aws_sigv4','aws_keys'] },
    // Custom / Bespoke
    { id:'openai_compat',group:'Custom / Bespoke',  icon:'🔌', name:'OpenAI-compatible',  desc:'vLLM, LocalAI, Ollama, etc.',        authTypes:['api_key','bearer','none'] },
    { id:'custom_http',  group:'Custom / Bespoke',  icon:'⚙', name:'Custom HTTP REST',   desc:'Any REST endpoint (fully configurable)', authTypes:['bearer','api_key','basic','header','none'] },
    { id:'custom_grpc',  group:'Custom / Bespoke',  icon:'⚡', name:'Custom gRPC',        desc:'gRPC streaming / unary inference',   authTypes:['bearer','tls_cert','none'] },
    { id:'custom_udf',   group:'Custom / Bespoke',  icon:'⨍', name:'Flink UDF / JAR',   desc:'Registered Flink scalar/async UDF',  authTypes:['none'] },
];

const IFM_STEPS = [
    { label:'Source Table',  icon:'⬡' },
    { label:'Model Server',  icon:'🔬' },
    { label:'Model Config',  icon:'⚙' },
    { label:'Auth & Creds',  icon:'🔑' },
    { label:'Inference I/O', icon:'◈' },
    { label:'Output Sink',   icon:'⤵' },
    { label:'Review & SQL',  icon:'⟨/⟩' },
];

// ── Modal skeleton ─────────────────────────────────────────────────────────────
function _ifmBuildModal() {
    _ifmInjectCss();
    const stepBar = IFM_STEPS.map((s, i) => `
    ${i > 0 ? '<div class="ifm-step-connector"></div>' : ''}
    <div class="ifm-step-item" id="ifm-step-item-${i}" onclick="_ifmTryGoStep(${i})">
      <div class="ifm-step-num" id="ifm-step-num-${i}">${i + 1}</div>
      <span>${s.icon} ${s.label}</span>
    </div>`).join('');

    const m = document.createElement('div');
    m.id = 'ifm-modal';
    m.className = 'modal-overlay';
    m.innerHTML = `
<div class="modal">
  <div class="modal-header" style="background:rgba(79,163,224,0.05);border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;">
    <span style="display:flex;align-items:center;gap:9px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--blue,#4fa3e0)" stroke-width="2">
        <path d="M12 2a4 4 0 0 1 4 4 4 4 0 0 1-4 4 4 4 0 0 1-4-4 4 4 0 0 1 4-4z"/>
        <path d="M4 20a8 8 0 0 1 16 0"/>
        <path d="M20 8l2-2M22 14h-2M14 22l-2-2"/>
      </svg>
      AI Model Inference Manager
      <span style="font-size:9px;font-weight:400;color:var(--text3);font-family:var(--mono);">Real-time ML Inference on Flink Streaming Pipelines</span>
    </span>
    <div style="display:flex;align-items:center;gap:8px;margin-left:auto;">
      <button onclick="_ifmShowHistory()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid var(--border2);background:var(--bg3);color:var(--text2);cursor:pointer;font-family:var(--mono);">
        🕓 History <span id="ifm-hist-count" style="color:var(--blue,#4fa3e0);"></span>
      </button>
      <button onclick="_ifmResetAll()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;font-family:var(--mono);">↺ Reset</button>
      <button class="modal-close" onclick="closeModal('ifm-modal')">×</button>
    </div>
  </div>
  <div id="ifm-wizard-wrap">
    <div id="ifm-step-bar">${stepBar}</div>
    <div id="ifm-validation-banner"><span id="ifm-validation-msg-text"></span></div>
    <div id="ifm-body"></div>
    <div id="ifm-footer">
      <button id="ifm-btn-back" class="btn btn-secondary" style="font-size:11px;display:none;" onclick="_ifmBack()">← Back</button>
      <span id="ifm-step-label" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
      <div style="flex:1;"></div>
      <button id="ifm-btn-next" class="btn btn-primary" style="font-size:11px;background:rgba(79,163,224,0.15);border:1px solid rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);" onclick="_ifmNext()">Next →</button>
      <button id="ifm-btn-generate" style="font-size:11px;display:none;padding:7px 14px;border-radius:var(--radius);background:rgba(79,163,224,0.12);border:1px solid rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);cursor:pointer;font-family:var(--mono);font-weight:700;" onclick="_ifmInsertSql()">
        ⤵ Insert SQL into Editor
      </button>
    </div>
  </div>
</div>`;
    document.body.appendChild(m);
    m.addEventListener('click', e => { if (e.target === m) closeModal('ifm-modal'); });
    _ifmUpdateHistCount();
}

function _ifmUpdateHistCount() {
    const el = document.getElementById('ifm-hist-count');
    if (el) el.textContent = _IFM.history.length ? `(${_IFM.history.length})` : '';
}

function _ifmResetAll() {
    if (!confirm('Reset all Inference Manager settings?')) return;
    Object.assign(_IFM, {
        step:0, sourceTable:'', inputColumns:[], selectedInputCols:[],
        modelServer:'mlflow', modelConfig:{}, authType:'bearer', authConfig:{},
        inferenceConfig:{ inputCol:'', outputAlias:'prediction', outputType:'DOUBLE',
            batchSize:'1', timeoutMs:'5000', retries:'2', asyncParallelism:'4',
            passthroughCols:'', preProcessExpr:'', postProcessExpr:'' },
        outputTable:'', sinkType:'kafka', generatedSql:'',
    });
    _ifmGoStep(0);
}

// ── Validation ────────────────────────────────────────────────────────────────
function _ifmValidateStep(step) {
    switch (step) {
        case 0: {
            const tbl = document.getElementById('ifm-src-table')?.value?.trim() || _IFM.sourceTable;
            if (!tbl) return 'Source table name is required.';
            if (!_IFM.inputColumns.length) return 'Define at least one input column.';
            return null;
        }
        case 1:
            if (!_IFM.modelServer) return 'Select a model server to continue.';
            return null;
        case 2: {
            const srv = IFM_SERVERS.find(s => s.id === _IFM.modelServer);
            if (_IFM.modelServer === 'custom_udf') {
                if (!document.getElementById('ifm-mc-udf-name')?.value?.trim()) return 'UDF function name is required.';
            } else if (_IFM.modelServer !== 'minio') {
                const ep = document.getElementById('ifm-mc-endpoint')?.value?.trim();
                if (!ep && _IFM.modelServer !== 'custom_udf') return 'Endpoint / URL is required.';
            }
            return null;
        }
        case 4: {
            const inp = document.getElementById('ifm-ic-input-col')?.value?.trim() || _IFM.inferenceConfig.inputCol;
            if (!inp) return 'Select or enter the input column for inference.';
            const alias = document.getElementById('ifm-ic-output-alias')?.value?.trim() || _IFM.inferenceConfig.outputAlias;
            if (!alias) return 'Output alias for the model result is required.';
            return null;
        }
        case 5: {
            const out = document.getElementById('ifm-out-table')?.value?.trim() || _IFM.outputTable;
            if (!out) return 'Output table name is required.';
            return null;
        }
        default: return null;
    }
}

function _ifmShowValidationError(msg) {
    const banner = document.getElementById('ifm-validation-banner');
    const txt = document.getElementById('ifm-validation-msg-text');
    if (banner && txt) {
        txt.textContent = '⚠ ' + msg;
        banner.style.display = '';
        setTimeout(() => { if (banner) banner.style.display = 'none'; }, 4500);
    }
}

function _ifmCollectCurrentStep() {
    switch (_IFM.step) {
        case 0:
            _IFM.sourceTable = document.getElementById('ifm-src-table')?.value?.trim() || _IFM.sourceTable;
            _ifmParseSourceCols(document.getElementById('ifm-src-cols')?.value || '');
            break;
        case 2: _ifmCollectModelConfig(); break;
        case 3: _ifmCollectAuthConfig(); break;
        case 4: _ifmCollectInferenceConfig(); break;
        case 5:
            _IFM.outputTable = document.getElementById('ifm-out-table')?.value?.trim() || _IFM.outputTable;
            _IFM.sinkType = document.getElementById('ifm-sink-type')?.value || _IFM.sinkType;
            _ifmCollectSinkConfig();
            break;
    }
}

function _ifmTryGoStep(n) {
    if (n > _IFM.step) {
        for (let s = _IFM.step; s < n; s++) {
            const err = _ifmValidateStep(s);
            if (err) { _ifmShowValidationError(err); return; }
        }
    }
    _ifmCollectCurrentStep();
    _ifmGoStep(n);
}

function _ifmGoStep(n) {
    _IFM.step = n;
    const banner = document.getElementById('ifm-validation-banner');
    if (banner) banner.style.display = 'none';
    document.querySelectorAll('.ifm-step-item').forEach((el, i) => {
        el.classList.toggle('active', i === n);
        el.classList.toggle('done', i < n);
    });
    document.querySelectorAll('.ifm-step-num').forEach((el, i) => {
        el.textContent = i < n ? '✓' : String(i + 1);
    });
    const back = document.getElementById('ifm-btn-back');
    const next = document.getElementById('ifm-btn-next');
    const gen  = document.getElementById('ifm-btn-generate');
    const lbl  = document.getElementById('ifm-step-label');
    if (back) back.style.display = n === 0 ? 'none' : '';
    if (next) next.style.display = n === 6 ? 'none' : '';
    if (gen)  gen.style.display  = n === 6 ? '' : 'none';
    if (lbl)  lbl.textContent    = `Step ${n + 1} of ${IFM_STEPS.length} — ${IFM_STEPS[n].label}`;

    const renderers = [
        _ifmRenderStep0, _ifmRenderStep1, _ifmRenderStep2,
        _ifmRenderStep3, _ifmRenderStep4, _ifmRenderStep5, _ifmRenderStep6,
    ];
    const body = document.getElementById('ifm-body');
    if (body) { body.innerHTML = ''; renderers[n]?.(); }
}

function _ifmNext() {
    const err = _ifmValidateStep(_IFM.step);
    if (err) { _ifmShowValidationError(err); return; }
    _ifmCollectCurrentStep();
    if (_IFM.step < 6) _ifmGoStep(_IFM.step + 1);
}

function _ifmBack() {
    _ifmCollectCurrentStep();
    if (_IFM.step > 0) _ifmGoStep(_IFM.step - 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 0 — Source Table
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep0() {
    const body = document.getElementById('ifm-body');

    // Try to pull existing tables from session state
    let sessionTables = [];
    try {
        const hist = JSON.parse(localStorage.getItem('strlabstudio_query_history') || '[]');
        const createMatches = hist.map(h => h.sql || '').join('\n')
            .match(/CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/gi) || [];
        sessionTables = [...new Set(createMatches.map(m => {
            const parts = m.trim().split(/\s+/);
            return parts[parts.length - 1];
        }))];
    } catch (_) {}

    // Also pull from FEM history
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        femH.forEach(h => { if (h.outputTable) sessionTables.push(h.outputTable); });
    } catch (_) {}

    sessionTables = [...new Set(sessionTables)].slice(0, 40);

    // Build combined table list: FEM outputs first, then session tables
    const femTables = [];
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        femH.forEach(h => { if (h.outputTable) femTables.push({ name: h.outputTable, src: 'FEM', ts: h.ts }); });
    } catch (_) {}

    // Merge, dedupe, FEM first
    const allKnownTables = [
        ...femTables,
        ...sessionTables.filter(t => !femTables.find(f => f.name === t)).map(t => ({ name: t, src: 'session' })),
    ];

    body.innerHTML = `
<div class="ifm-info-box">
  Select the <strong>source Flink table</strong> that carries the streaming data for ML inference.
  Use the dropdown to pick from known session/FEM tables, or type a custom name.
  Selecting a table auto-populates the schema from history where available.
</div>

<div class="ifm-card">
  <div class="ifm-section-title">Source Table</div>

  <!-- Table selector row: dropdown + text input + FEM button all aligned -->
  <div style="display:flex;gap:8px;align-items:stretch;margin-bottom:10px;">
    <!-- Known-tables dropdown -->
    <div style="flex-shrink:0;min-width:0;">
      <label class="ifm-field-label">Select Known Table</label>
      <select id="ifm-table-dropdown" class="field-input"
        style="font-size:11px;font-family:var(--mono);height:32px;cursor:pointer;"
        onchange="_ifmSelectKnownTable(this.value)">
        <option value="">— pick a table —</option>
        ${allKnownTables.length ? `<optgroup label="── Feature Engineering outputs">` : ''}
        ${femTables.map(t => `<option value="${_escIfm(t.name)}" data-src="fem">⬡ ${_escIfm(t.name)}</option>`).join('')}
        ${femTables.length && sessionTables.filter(t => !femTables.find(f=>f.name===t)).length ? `</optgroup><optgroup label="── Session tables">` : (femTables.length ? '</optgroup>' : '')}
        ${sessionTables.filter(t => !femTables.find(f => f.name === t)).map(t => `<option value="${_escIfm(t)}" data-src="session">◈ ${_escIfm(t)}</option>`).join('')}
        ${sessionTables.filter(t => !femTables.find(f => f.name === t)).length && femTables.length ? '</optgroup>' : ''}
      </select>
    </div>

    <!-- Manual text input -->
    <div style="flex:1;min-width:0;">
      <label class="ifm-field-label">Table Name <span class="ifm-required">*</span></label>
      <input id="ifm-src-table" class="field-input" type="text"
        style="font-size:11px;font-family:var(--mono);height:32px;box-sizing:border-box;"
        placeholder="user_transaction_features"
        value="${_escIfm(_IFM.sourceTable)}"
        oninput="_IFM.sourceTable=this.value" />
    </div>

    <!-- From FEM button — aligned to input height -->
    <div style="flex-shrink:0;display:flex;flex-direction:column;justify-content:flex-end;">
      <label class="ifm-field-label" style="visibility:hidden;">btn</label>
      <button onclick="_ifmAutoFillFromFem()"
        title="Load latest Feature Engineering output table"
        style="height:32px;padding:0 12px;font-size:11px;font-family:var(--mono);
          border-radius:var(--radius,3px);border:1px solid rgba(0,212,170,0.35);
          background:rgba(0,212,170,0.08);color:var(--accent,#00d4aa);
          cursor:pointer;white-space:nowrap;display:flex;align-items:center;gap:5px;">
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
        </svg>
        From FEM
      </button>
    </div>
  </div>

  <!-- Known tables quick-pills -->
  ${allKnownTables.length ? `
  <div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:2px;">
    ${allKnownTables.slice(0,12).map(t => `
      <div onclick="_ifmSelectKnownTable('${_escIfm(t.name)}')"
        title="${t.src === 'fem' ? 'Feature Engineering output' : 'Session table'}"
        style="padding:2px 9px;border-radius:3px;font-size:10px;font-family:var(--mono);cursor:pointer;
          background:${t.src==='fem'?'rgba(0,212,170,0.08)':'rgba(79,163,224,0.07)'};
          border:1px solid ${t.src==='fem'?'rgba(0,212,170,0.25)':'rgba(79,163,224,0.2)'};
          color:${t.src==='fem'?'var(--accent)':'var(--blue,#4fa3e0)'};
          transition:opacity 0.1s;" onmouseenter="this.style.opacity='0.75'" onmouseleave="this.style.opacity='1'">
        ${t.src==='fem'?'⬡':'◈'} ${_escIfm(t.name)}
      </div>`).join('')}
  </div>` : ''}
</div>

<div class="ifm-card">
  <div class="ifm-section-title">Input Schema <span class="ifm-required">*</span></div>
  <div style="font-size:11px;color:var(--text2);margin-bottom:8px;line-height:1.7;">
    Columns available in the source table. Selecting a table above auto-fills this from history.
    Format: <code style="background:var(--bg0);padding:1px 5px;border-radius:2px;color:var(--blue,#4fa3e0);">name TYPE</code> one per line.
  </div>

  <!-- Schema loading indicator -->
  <div id="ifm-schema-loading" style="display:none;font-size:11px;color:var(--accent);font-family:var(--mono);margin-bottom:6px;">
    ⟳ Loading schema…
  </div>

  <!-- Column tag display (shown when a table is selected from dropdown) -->
  <div id="ifm-col-tags-wrap" style="display:${_IFM.inputColumns.length?'flex':'none'};flex-wrap:wrap;gap:3px;margin-bottom:8px;padding:8px;background:var(--bg0);border:1px solid var(--border);border-radius:3px;min-height:32px;">
    ${_IFM.inputColumns.map(c => `
      <div class="ifm-col-tag selected" title="${c.type}">
        ${_escIfm(c.name)}<span class="ifm-tag-type">${c.type}</span>
      </div>`).join('')}
  </div>

  <textarea id="ifm-src-cols" class="field-input" rows="7"
    style="font-family:var(--mono);font-size:11px;resize:vertical;line-height:1.8;"
    placeholder="user_id BIGINT&#10;amount DOUBLE&#10;tx_count_5m BIGINT&#10;total_amt_5m DOUBLE&#10;avg_amt_5m DOUBLE&#10;risk_tier STRING&#10;is_international BOOLEAN&#10;event_time TIMESTAMP(3)"
    oninput="_ifmParseSourceCols(this.value)">${_IFM.inputColumns.map(c=>c.name+' '+c.type).join('\n')}</textarea>
  <div id="ifm-src-cols-feedback" style="font-size:10px;margin-top:4px;color:var(--text3);font-family:var(--mono);"></div>
</div>`;

    setTimeout(() => {
        const ta = document.getElementById('ifm-src-cols');
        if (ta && ta.value.trim()) _ifmParseSourceCols(ta.value);
        // Sync dropdown to current value
        const dd = document.getElementById('ifm-table-dropdown');
        if (dd && _IFM.sourceTable) dd.value = _IFM.sourceTable;
    }, 50);
}

function _ifmSelectKnownTable(tableName) {
    if (!tableName) return;
    _IFM.sourceTable = tableName;

    // Sync text input and dropdown
    const inp = document.getElementById('ifm-src-table');
    if (inp) inp.value = tableName;
    const dd = document.getElementById('ifm-table-dropdown');
    if (dd) dd.value = tableName;

    const loading = document.getElementById('ifm-schema-loading');
    if (loading) loading.style.display = '';

    let loaded = false;

    // ── Strategy 1: direct outputColumns from FEM history entry (richest — set by _femDeriveOutputColumns)
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        const match = femH.find(h => h.outputTable === tableName);
        if (match) {
            // Prefer outputColumns (schema of the output/scored table) — exactly what IFM needs
            const cols = match.outputColumns?.length ? match.outputColumns
                : match.sourceColumns?.length ? match.sourceColumns
                    : null;
            if (cols && cols.length) {
                _IFM.inputColumns = cols;
                loaded = true;
                _ifmRefreshSchemaDisplay(cols,
                    `✓ ${cols.length} columns loaded from Feature Engineering output "${tableName}"`);
            }
        }
    } catch (_) {}

    // ── Strategy 2: fem_state snapshot (quick-access, written by _femSaveHistory)
    if (!loaded) {
        try {
            const state = JSON.parse(localStorage.getItem('strlabstudio_fem_state') || 'null');
            if (state) {
                const cols = state.outputTable === tableName
                    ? (state.outputColumns?.length ? state.outputColumns : state.sourceColumns)
                    : state.sourceTable === tableName
                        ? state.sourceColumns
                        : null;
                if (cols && cols.length) {
                    _IFM.inputColumns = cols;
                    loaded = true;
                    _ifmRefreshSchemaDisplay(cols, `✓ ${cols.length} columns loaded from FEM state snapshot`);
                }
            }
        } catch (_) {}
    }

    // ── Strategy 3: parse CREATE TABLE DDL from the stored SQL
    if (!loaded) {
        try {
            const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
            const match = femH.find(h => h.outputTable === tableName || h.sourceTable === tableName);
            if (match && match.sql) {
                // Match the exact table name in CREATE TABLE
                const re = new RegExp(
                    `CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?${tableName}\\s*\\(([\\s\\S]+?)\\)\\s*WITH`,
                    'i'
                );
                const createMatch = match.sql.match(re);
                if (createMatch) {
                    const cols = createMatch[1]
                        .split('\n')
                        .map(l => l.trim().replace(/,\s*$/, ''))
                        .filter(l => l && !l.toUpperCase().startsWith('PRIMARY')
                            && !l.toUpperCase().startsWith('WATERMARK')
                            && !l.startsWith('--')
                            && !l.startsWith('/*'))
                        .map(l => {
                            const parts = l.trim().split(/\s+/);
                            return parts.length >= 2
                                ? { name: parts[0], type: parts.slice(1).join(' ').replace(/,$/, '') }
                                : null;
                        })
                        .filter(Boolean);
                    if (cols.length) {
                        _IFM.inputColumns = cols;
                        loaded = true;
                        _ifmRefreshSchemaDisplay(cols, `✓ ${cols.length} columns parsed from pipeline SQL`);
                    }
                }
            }
        } catch (_) {}
    }

    // ── Nothing found — clear indicator and let user type manually
    if (!loaded) {
        if (loading) loading.style.display = 'none';
        // Clear any stale columns if switching to an unknown table
        _IFM.inputColumns = [];
        const tagsWrap = document.getElementById('ifm-col-tags-wrap');
        if (tagsWrap) tagsWrap.style.display = 'none';
        const fb = document.getElementById('ifm-src-cols-feedback');
        if (fb) { fb.textContent = `Table "${tableName}" not in FEM history — enter schema manually below`; fb.style.color = 'var(--yellow,#f5a623)'; }
        if (typeof toast === 'function') toast(`"${tableName}" selected — enter schema columns manually`, 'info');
    }
}

function _ifmRefreshSchemaDisplay(cols, feedbackMsg) {
    const loading = document.getElementById('ifm-schema-loading');
    if (loading) loading.style.display = 'none';

    // Update textarea
    const ta = document.getElementById('ifm-src-cols');
    if (ta) ta.value = cols.map(c => c.name + ' ' + c.type).join('\n');

    // Update tag display
    const tagsWrap = document.getElementById('ifm-col-tags-wrap');
    if (tagsWrap) {
        tagsWrap.style.display = 'flex';
        tagsWrap.innerHTML = cols.map(c => `
      <div class="ifm-col-tag selected" title="${c.type}">
        ${_escIfm(c.name)}<span class="ifm-tag-type">${c.type}</span>
      </div>`).join('');
    }

    // Feedback
    const fb = document.getElementById('ifm-src-cols-feedback');
    if (fb) { fb.textContent = feedbackMsg; fb.style.color = 'var(--green)'; }

    if (typeof toast === 'function') toast(feedbackMsg, 'ok');
}

function _ifmParseSourceCols(raw) {
    const cols = raw.split('\n').map(l => l.trim()).filter(Boolean).map(line => {
        const parts = line.split(/\s+/);
        return parts.length >= 2 ? { name: parts[0], type: parts.slice(1).join(' ') } : null;
    }).filter(Boolean);
    _IFM.inputColumns = cols;

    // Update tag display live as user types
    const tagsWrap = document.getElementById('ifm-col-tags-wrap');
    if (tagsWrap && cols.length) {
        tagsWrap.style.display = 'flex';
        tagsWrap.innerHTML = cols.map(c => `
      <div class="ifm-col-tag selected" title="${c.type}">
        ${_escIfm(c.name)}<span class="ifm-tag-type">${c.type}</span>
      </div>`).join('');
    } else if (tagsWrap) {
        tagsWrap.style.display = 'none';
    }

    const fb = document.getElementById('ifm-src-cols-feedback');
    if (fb) { fb.textContent = cols.length ? `✓ ${cols.length} column${cols.length > 1 ? 's' : ''} parsed` : ''; fb.style.color = 'var(--text3)'; }
}

function _ifmAutoFillFromFem() {
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        if (!femH.length) { if (typeof toast === 'function') toast('No Feature Engineering history found — run the Feature Engineering Manager first', 'warn'); return; }
        const latest = femH[0];
        if (latest.outputTable) _ifmSelectKnownTable(latest.outputTable);
    } catch (_) {}
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1 — Model Server Selection
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep1() {
    const body = document.getElementById('ifm-body');
    const groups = [...new Set(IFM_SERVERS.map(s => s.group))];

    body.innerHTML = `
<div class="ifm-info-box">
  Select the <strong>model serving platform</strong> where your trained model is hosted. Each server type generates the appropriate connector pattern, authentication, and Flink async UDF template.
</div>
${groups.map(g => `
  <div style="margin-bottom:16px;">
    <div class="ifm-section-title">${g}</div>
    <div class="ifm-server-grid">
      ${IFM_SERVERS.filter(s => s.group === g).map(s => `
        <div class="ifm-server-card ${_IFM.modelServer === s.id ? 'selected' : ''}"
          id="ifm-sc-${s.id}" onclick="_ifmSelectServer('${s.id}')">
          <span class="ifm-sc-icon">${s.icon}</span>
          <div class="ifm-sc-name">${s.name}</div>
          <div class="ifm-sc-desc">${s.desc}</div>
        </div>`).join('')}
    </div>
  </div>`).join('')}`;
}

function _ifmSelectServer(id) {
    _IFM.modelServer = id;
    document.querySelectorAll('.ifm-server-card').forEach(el => el.classList.remove('selected'));
    const card = document.getElementById('ifm-sc-' + id);
    if (card) card.classList.add('selected');
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2 — Model Configuration
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep2() {
    const body = document.getElementById('ifm-body');
    const srv = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const mc = _IFM.modelConfig;

    const fi = (label, id, ph, req, type, val) =>
        `<div><label class="ifm-field-label">${label}${req?` <span class="ifm-required">*</span>`:''}</label>
    <input type="${type||'text'}" id="${id}" class="field-input"
      placeholder="${_escIfm(ph||'')}" value="${_escIfm(val||mc[id]||'')}"
      style="font-size:11px;font-family:var(--mono);" /></div>`;

    const sel = (label, id, opts, val) =>
        `<div><label class="ifm-field-label">${label}</label>
    <select id="${id}" class="field-input" style="font-size:11px;">
      ${opts.map(o => typeof o === 'string'
            ? `<option value="${o}" ${(val||mc[id]||opts[0])===o?'selected':''}>${o}</option>`
            : `<option value="${o.v}" ${(val||mc[id]||opts[0].v)===o.v?'selected':''}>${o.l}</option>`
        ).join('')}
    </select></div>`;

    const serverForms = {
        mlflow: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('MLflow Tracking URI','ifm-mc-endpoint','http://mlflow:5000',true)}
        ${fi('Model Name','ifm-mc-model-name','fraud-detection-model',true)}
        ${fi('Model Stage / Version','ifm-mc-model-version','Production')}
        ${sel('Serving Flavour','ifm-mc-mlflow-flavour',['REST (pyfunc serve)','Python Function','ONNX','Spark ML','H2O'])}
        ${fi('Registered Model URI','ifm-mc-model-uri','models:/fraud-detection-model/Production')}
        ${fi('Experiment ID (optional)','ifm-mc-exp-id','')}
      </div>`,

        mlflow_serve: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('mlflow serve Endpoint URL','ifm-mc-endpoint','http://model-server:5001/invocations',true)}
        ${fi('Model Name','ifm-mc-model-name','my-model',true)}
        ${sel('Input Format','ifm-mc-mlflow-fmt',['dataframe-split','dataframe-records','instances','inputs'])}
      </div>`,

        sagemaker: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Endpoint Name','ifm-mc-endpoint','fraud-detection-endpoint-prod',true)}
        ${fi('AWS Region','ifm-mc-aws-region','us-east-1',true)}
        ${sel('Content Type','ifm-mc-content-type',['application/json','text/csv','application/x-recordio-protobuf'])}
        ${sel('Accept Type','ifm-mc-accept-type',['application/json','text/csv'])}
        ${fi('Model Variant (optional)','ifm-mc-variant','')}
        ${fi('Target Container Hostname (optional)','ifm-mc-target-host','')}
      </div>`,

        azureml: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Endpoint URL','ifm-mc-endpoint','https://my-endpoint.eastus.inference.ml.azure.com/score',true)}
        ${fi('Deployment Name','ifm-mc-deployment','fraud-blue')}
        ${fi('Workspace Name (for logging)','ifm-mc-workspace','my-workspace')}
        ${fi('Resource Group','ifm-mc-rg','ml-resource-group')}
        ${fi('Subscription ID','ifm-mc-sub-id','')}
      </div>`,

        vertexai: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Project ID','ifm-mc-gcp-project','my-gcp-project',true)}
        ${fi('Location / Region','ifm-mc-gcp-region','us-central1',true)}
        ${fi('Endpoint ID','ifm-mc-endpoint','1234567890',true)}
        ${sel('API Version','ifm-mc-gcp-ver',['v1','v1beta1'])}
        ${fi('Deployed Model ID (optional)','ifm-mc-gcp-deployed-model','')}
      </div>`,

        databricks: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Databricks Workspace URL','ifm-mc-endpoint','https://my-workspace.azuredatabricks.net',true)}
        ${fi('Registered Model Name','ifm-mc-model-name','fraud-model',true)}
        ${fi('Model Version or Alias','ifm-mc-model-version','Champion')}
        ${fi('Serving Endpoint Name','ifm-mc-db-serving-ep','fraud-model-serving')}
      </div>`,

        bentoml: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('BentoML Server URL','ifm-mc-endpoint','http://bentoml:3000',true)}
        ${fi('Service / Runner Route','ifm-mc-route','/predict',true)}
        ${sel('Protocol','ifm-mc-protocol',['HTTP REST','gRPC'])}
        ${fi('Bento Tag','ifm-mc-bento-tag','fraud_classifier:latest')}
      </div>`,

        triton: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Triton HTTP URL','ifm-mc-endpoint','http://triton:8000',true)}
        ${fi('Model Name','ifm-mc-model-name','fraud_onnx',true)}
        ${fi('Model Version','ifm-mc-model-version','1')}
        ${sel('Protocol','ifm-mc-protocol',['HTTP','gRPC (port 8001)'])}
        ${sel('Input Data Type','ifm-mc-triton-dtype',['FP32','FP64','INT32','INT64','BYTES','BOOL'])}
        ${fi('Input Tensor Name','ifm-mc-triton-input','INPUT__0')}
      </div>`,

        torchserve: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('TorchServe URL','ifm-mc-endpoint','http://torchserve:8080',true)}
        ${fi('Model Name','ifm-mc-model-name','fraud_classifier',true)}
        ${fi('Model Version','ifm-mc-model-version','1.0')}
        ${sel('API Type','ifm-mc-ts-api',['Predictions API','Explanations API'])}
      </div>`,

        tfserving: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('TF Serving URL','ifm-mc-endpoint','http://tfserving:8501',true)}
        ${fi('Model Name','ifm-mc-model-name','fraud_v2',true)}
        ${fi('Signature Name','ifm-mc-tf-sig','serving_default')}
        ${sel('API Version','ifm-mc-tf-apiver',['v1 (REST)','v2 (REST)'])}
      </div>`,

        ray: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Ray Serve URL','ifm-mc-endpoint','http://ray-head:8000',true)}
        ${fi('Deployment Route','ifm-mc-route','/fraud-classifier',true)}
        ${fi('Ray Dashboard URL (opt)','ifm-mc-ray-dashboard','http://ray-head:8265')}
      </div>`,

        seldon: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Seldon URL','ifm-mc-endpoint','http://seldon-ambassador:80',true)}
        ${fi('Namespace','ifm-mc-seldon-ns','default',true)}
        ${fi('Deployment Name','ifm-mc-model-name','fraud-classifier',true)}
        ${sel('Predictor Protocol','ifm-mc-seldon-proto',['REST V2','REST V1','gRPC'])}
      </div>`,

        kserve: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('KServe Ingress URL','ifm-mc-endpoint','http://istio-ingressgateway',true)}
        ${fi('InferenceService Name','ifm-mc-model-name','fraud-classifier',true)}
        ${fi('Namespace','ifm-mc-kserve-ns','default')}
        ${sel('Protocol Version','ifm-mc-kserve-proto',['v2','v1'])}
      </div>`,

        minio: `
      <div class="ifm-warn-box">
        MinIO / S3 is an artifact store. Models are loaded by the Flink operator or UDF at startup. Configure the model artifact path and loading strategy below.
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('MinIO / S3 Endpoint','ifm-mc-endpoint','http://minio:9000',true)}
        ${fi('Bucket Name','ifm-mc-minio-bucket','ml-models',true)}
        ${fi('Model Object Path','ifm-mc-minio-path','fraud/v3/model.pkl',true)}
        ${sel('Model Format','ifm-mc-minio-fmt',['pickle (sklearn)','ONNX','TensorFlow SavedModel','PyTorch TorchScript','XGBoost JSON','LightGBM','PMML'])}
        ${fi('Local Load Path (Flink TaskManager)','ifm-mc-local-path','/tmp/models/fraud_v3')}
        ${fi('Cache TTL (seconds, 0=no refresh)','ifm-mc-cache-ttl','3600')}
      </div>`,

        huggingface: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Model ID / Endpoint URL','ifm-mc-endpoint','https://api-inference.huggingface.co/models/my-org/fraud-v1',true)}
        ${fi('Model ID (for Inference API)','ifm-mc-hf-model','my-org/fraud-classifier')}
        ${sel('Task','ifm-mc-hf-task',['text-classification','token-classification','feature-extraction','text-generation','fill-mask','question-answering','zero-shot-classification','tabular-classification','tabular-regression'])}
        ${sel('API Type','ifm-mc-hf-api',['Inference API (free)','Inference Endpoints (dedicated)'])}
      </div>`,

        openai: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Base URL (override, optional)','ifm-mc-endpoint','https://api.openai.com/v1','false')}
        ${fi('Model','ifm-mc-model-name','gpt-4o-mini',true)}
        ${fi('System Prompt','ifm-mc-system-prompt','You are a fraud detection assistant. Classify each transaction.')}
        ${sel('Response Format','ifm-mc-openai-fmt',['text','json_object'])}
        ${fi('Max Tokens','ifm-mc-max-tokens','64')}
        ${fi('Temperature','ifm-mc-temperature','0.0')}
      </div>`,

        anthropic: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('API Base URL','ifm-mc-endpoint','https://api.anthropic.com')}
        ${sel('Model','ifm-mc-model-name',[{v:'claude-sonnet-4-6',l:'Claude Sonnet 4.6'},{v:'claude-opus-4-6',l:'Claude Opus 4.6'},{v:'claude-haiku-4-5-20251001',l:'Claude Haiku 4.5'}])}
        ${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction as FRAUD or LEGITIMATE.')}
        ${fi('Max Tokens','ifm-mc-max-tokens','128')}
        ${fi('anthropic-version header','ifm-mc-anthropic-ver','2023-06-01')}
        ${sel('Response Parsing','ifm-mc-anthropic-parse',['First content text block','JSON from text block'])}
      </div>`,

        cohere: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('API Base URL','ifm-mc-endpoint','https://api.cohere.com')}
        ${sel('Endpoint','ifm-mc-cohere-ep',['/v2/classify','/v2/embed','/v2/chat','/v2/generate'])}
        ${fi('Model','ifm-mc-model-name','command-r-plus')}
        ${fi('Classification Labels (comma-sep for /classify)','ifm-mc-cohere-labels','FRAUD,LEGITIMATE')}
      </div>`,

        mistral: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('API Base URL','ifm-mc-endpoint','https://api.mistral.ai/v1')}
        ${fi('Model','ifm-mc-model-name','mistral-small-latest',true)}
        ${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction.')}
        ${fi('Max Tokens','ifm-mc-max-tokens','64')}
        ${fi('Temperature','ifm-mc-temperature','0.0')}
      </div>`,

        together: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('API Base URL','ifm-mc-endpoint','https://api.together.xyz/v1')}
        ${fi('Model','ifm-mc-model-name','meta-llama/Llama-3-8b-chat-hf',true)}
        ${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction as FRAUD or LEGITIMATE. Respond with one word.')}
        ${fi('Max Tokens','ifm-mc-max-tokens','16')}
      </div>`,

        bedrock: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('AWS Region','ifm-mc-aws-region','us-east-1',true)}
        ${sel('Model ID','ifm-mc-model-name',[
            {v:'anthropic.claude-sonnet-4-6',l:'Claude Sonnet 4.6'},
            {v:'amazon.titan-text-express-v1',l:'Amazon Titan Text Express'},
            {v:'meta.llama3-8b-instruct-v1:0',l:'Meta Llama 3 8B'},
            {v:'mistral.mistral-7b-instruct-v0:2',l:'Mistral 7B Instruct'},
            {v:'cohere.command-r-v1:0',l:'Cohere Command R'},
            {v:'ai21.j2-ultra-v1',l:'AI21 Jurassic-2 Ultra'},
        ])}
        ${fi('System Prompt','ifm-mc-system-prompt','Classify the following transaction.')}
        ${fi('Max Tokens','ifm-mc-max-tokens','128')}
      </div>`,

        openai_compat: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Base URL','ifm-mc-endpoint','http://localhost:11434/v1',true)}
        ${fi('Model','ifm-mc-model-name','llama3',true)}
        ${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction.')}
        ${fi('Max Tokens','ifm-mc-max-tokens','64')}
        ${fi('Temperature','ifm-mc-temperature','0.0')}
        ${sel('Compatible With','ifm-mc-compat-type',['vLLM','Ollama','LocalAI','LM Studio','Llamafile','Other'])}
      </div>`,

        custom_http: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('Endpoint URL','ifm-mc-endpoint','https://my-model-server.internal/v1/predict',true)}
        ${sel('HTTP Method','ifm-mc-http-method',['POST','GET','PUT'])}
        ${sel('Content-Type','ifm-mc-content-type',['application/json','text/plain','application/x-www-form-urlencoded','multipart/form-data'])}
        ${sel('Response Parsing','ifm-mc-http-parse',['JSON path extraction','Full JSON response as STRING','Raw text body'])}
        ${fi('JSON Response Path (e.g. $.result.score)','ifm-mc-json-path','$.prediction')}
        ${fi('Request Body Template','ifm-mc-req-template','{"inputs": [{{INPUT_COL}}]}')}
      </div>
      <div style="margin-top:8px;">
        <label class="ifm-field-label">Custom Request Headers (one per line: Header-Name: value)</label>
        <textarea id="ifm-mc-custom-headers" class="field-input" rows="3"
          style="font-size:11px;font-family:var(--mono);resize:vertical;"
          placeholder="X-Model-Version: v3&#10;X-Tenant-ID: fraud-team">${_escIfm(mc['ifm-mc-custom-headers']||'')}</textarea>
      </div>`,

        custom_grpc: `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('gRPC Target (host:port)','ifm-mc-endpoint','model-server:50051',true)}
        ${fi('Proto Package.Service.Method','ifm-mc-grpc-method','inference.PredictionService/Predict',true)}
        ${fi('Proto File Path or Descriptor','ifm-mc-grpc-proto','/opt/flink/protos/inference.proto')}
        ${sel('Channel Security','ifm-mc-grpc-tls',['Plaintext','TLS','mTLS'])}
        ${fi('Request Timeout (ms)','ifm-mc-grpc-timeout','3000')}
      </div>`,

        custom_udf: `
      <div class="ifm-info-box">
        Use a Flink scalar or async UDF that wraps your model inference logic (Java/Scala/Python). The UDF must be registered in the session via the UDF Manager or a prior CREATE FUNCTION statement.
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        ${fi('UDF Function Name','ifm-mc-udf-name','predict_fraud_score',true)}
        ${sel('UDF Kind','ifm-mc-udf-kind',['Scalar Function','Async Scalar Function','Table Function','Aggregate Function'])}
        ${fi('UDF Arguments (col names, comma-sep)','ifm-mc-udf-args','amount, tx_count_5m, avg_amt_5m')}
        ${fi('UDF Return Type','ifm-mc-udf-return','DOUBLE')}
        ${fi('CREATE FUNCTION Statement (if not yet registered)','ifm-mc-udf-create','')}
        ${fi('JAR Path / Class Name (for reference)','ifm-mc-udf-class','com.mycompany.flink.udf.FraudScoreUDF')}
      </div>`,
    };

    const form = serverForms[_IFM.modelServer] || `
    <div class="ifm-warn-box">No specific configuration form for "${srv.name}". Fill in the endpoint and proceed to Auth.</div>
    <div style="display:grid;grid-template-columns:1fr;gap:10px;">
      ${fi('Endpoint / Base URL','ifm-mc-endpoint','https://your-model-server.com/predict',true)}
    </div>`;

    body.innerHTML = `
<div class="ifm-info-box">
  Configure your <strong>${srv.icon} ${srv.name}</strong> model server connection. Fields marked <span class="ifm-required">*</span> are required.
</div>
<div class="ifm-card">
  <div class="ifm-section-title">${srv.name} — Connection Settings</div>
  ${form}
</div>`;
}

function _ifmCollectModelConfig() {
    const mc = {};
    document.querySelectorAll('[id^="ifm-mc-"]').forEach(el => {
        mc[el.id] = el.value;
    });
    _IFM.modelConfig = mc;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3 — Authentication & Credentials
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep3() {
    const body = document.getElementById('ifm-body');
    const srv = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const ac = _IFM.authConfig;

    const AUTH_TYPES = [
        { id:'none',       label:'No Auth',           icon:'🔓' },
        { id:'bearer',     label:'Bearer Token',       icon:'🔑' },
        { id:'api_key',    label:'API Key (header)',   icon:'🗝' },
        { id:'x_api_key',  label:'x-api-key header',  icon:'🗝' },
        { id:'basic',      label:'Basic Auth',         icon:'👤' },
        { id:'header',     label:'Custom Header',      icon:'📋' },
        { id:'aws_sigv4',  label:'AWS SigV4',          icon:'☁' },
        { id:'aws_keys',   label:'AWS Access Keys',    icon:'☁' },
        { id:'minio_keys', label:'MinIO Access Keys',  icon:'🗄' },
        { id:'azure_ad',   label:'Azure AD Token',     icon:'🔷' },
        { id:'google_sa',  label:'Google Service Acct',icon:'⬡' },
        { id:'tls_cert',   label:'mTLS Certificate',   icon:'🛡' },
    ];

    const availableAuth = srv.authTypes || ['bearer','api_key','none'];
    const current = _IFM.authType || availableAuth[0] || 'none';

    const authForms = {
        none:      `<div style="font-size:11px;color:var(--text3);padding:8px 0;">No authentication required for this endpoint.</div>`,
        bearer:    `<div><label class="ifm-field-label">Bearer Token <span class="ifm-required">*</span></label>
                <input id="ifm-ac-token" class="field-input" type="password" placeholder="eyJhbGci…" value="${_escIfm(ac['ifm-ac-token']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div style="margin-top:8px;font-size:10px;color:var(--text3);">Sent as: <code>Authorization: Bearer &lt;token&gt;</code></div>`,
        api_key:   `<div><label class="ifm-field-label">API Key <span class="ifm-required">*</span></label>
                <input id="ifm-ac-apikey" class="field-input" type="password" placeholder="sk-…" value="${_escIfm(ac['ifm-ac-apikey']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label" style="margin-top:8px;">Header Name</label>
                <input id="ifm-ac-header-name" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-header-name']||'Authorization')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div style="margin-top:6px;font-size:10px;color:var(--text3);">Sent as: <code>Authorization: Bearer &lt;key&gt;</code> or <code>&lt;Header&gt;: &lt;key&gt;</code></div>`,
        x_api_key: `<div><label class="ifm-field-label">Anthropic API Key <span class="ifm-required">*</span></label>
                <input id="ifm-ac-xapikey" class="field-input" type="password" placeholder="sk-ant-…" value="${_escIfm(ac['ifm-ac-xapikey']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div style="margin-top:6px;font-size:10px;color:var(--text3);">Sent as: <code>x-api-key: &lt;key&gt;</code></div>`,
        basic:     `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div><label class="ifm-field-label">Username <span class="ifm-required">*</span></label>
                <input id="ifm-ac-user" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-user']||'')}" placeholder="admin" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Password <span class="ifm-required">*</span></label>
                <input id="ifm-ac-pass" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-pass']||'')}" placeholder="●●●●●●" style="font-size:11px;font-family:var(--mono);" /></div>
                </div>`,
        header:    `<div><label class="ifm-field-label">Header Name <span class="ifm-required">*</span></label>
                <input id="ifm-ac-h-name" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-h-name']||'X-API-Token')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div style="margin-top:8px;"><label class="ifm-field-label">Header Value <span class="ifm-required">*</span></label>
                <input id="ifm-ac-h-val" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-h-val']||'')}" placeholder="secret-token" style="font-size:11px;font-family:var(--mono);" /></div>`,
        aws_sigv4: `<div class="ifm-info-box">AWS SigV4 signing is handled by the Flink async UDF. Credentials are read from environment variables or the instance role — no additional config needed if running on EC2/ECS/EKS.</div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div><label class="ifm-field-label">AWS Region</label>
                <input id="ifm-ac-aws-region" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-region']||_IFM.modelConfig['ifm-mc-aws-region']||'us-east-1')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Service Name</label>
                <input id="ifm-ac-aws-service" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-service']||'sagemaker')}" style="font-size:11px;font-family:var(--mono);" /></div>
                </div>`,
        aws_keys:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div><label class="ifm-field-label">Access Key ID <span class="ifm-required">*</span></label>
                <input id="ifm-ac-aws-kid" class="field-input" type="text" placeholder="AKIAIOSFODNN7EXAMPLE" value="${_escIfm(ac['ifm-ac-aws-kid']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Secret Access Key <span class="ifm-required">*</span></label>
                <input id="ifm-ac-aws-sak" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-aws-sak']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Session Token (optional)</label>
                <input id="ifm-ac-aws-token" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-aws-token']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">AWS Region</label>
                <input id="ifm-ac-aws-region" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-region']||'us-east-1')}" style="font-size:11px;font-family:var(--mono);" /></div>
                </div>`,
        minio_keys:`<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div><label class="ifm-field-label">Access Key <span class="ifm-required">*</span></label>
                <input id="ifm-ac-minio-key" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-minio-key']||'minioadmin')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Secret Key <span class="ifm-required">*</span></label>
                <input id="ifm-ac-minio-secret" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-minio-secret']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                </div>`,
        azure_ad:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div><label class="ifm-field-label">Tenant ID <span class="ifm-required">*</span></label>
                <input id="ifm-ac-az-tenant" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-az-tenant']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Client ID <span class="ifm-required">*</span></label>
                <input id="ifm-ac-az-client" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-az-client']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Client Secret <span class="ifm-required">*</span></label>
                <input id="ifm-ac-az-secret" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-az-secret']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Scope</label>
                <input id="ifm-ac-az-scope" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-az-scope']||'https://ml.azure.com/.default')}" style="font-size:11px;font-family:var(--mono);" /></div>
                </div>`,
        google_sa: `<div><label class="ifm-field-label">Service Account JSON Key Path <span class="ifm-required">*</span></label>
                <input id="ifm-ac-gsa-path" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-gsa-path']||'/etc/gcp/sa-key.json')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div style="margin-top:8px;"><label class="ifm-field-label">Scopes</label>
                <input id="ifm-ac-gsa-scope" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-gsa-scope']||'https://www.googleapis.com/auth/cloud-platform')}" style="font-size:11px;font-family:var(--mono);" /></div>`,
        tls_cert:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div><label class="ifm-field-label">Client Certificate Path</label>
                <input id="ifm-ac-tls-cert" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-tls-cert']||'/etc/certs/client.crt')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">Client Key Path</label>
                <input id="ifm-ac-tls-key" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-tls-key']||'/etc/certs/client.key')}" style="font-size:11px;font-family:var(--mono);" /></div>
                <div><label class="ifm-field-label">CA Certificate Path</label>
                <input id="ifm-ac-tls-ca" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-tls-ca']||'/etc/certs/ca.crt')}" style="font-size:11px;font-family:var(--mono);" /></div>
                </div>`,
    };

    body.innerHTML = `
<div class="ifm-info-box">
  Configure authentication for <strong>${srv.icon} ${srv.name}</strong>. Credentials stored here are embedded as comments in the generated SQL — use environment variables or Flink secrets in production.
</div>
<div class="ifm-card">
  <div class="ifm-section-title">Authentication Method</div>
  <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px;" id="ifm-auth-btns">
    ${AUTH_TYPES.filter(a => availableAuth.includes(a.id)).map(a => `
      <div class="ifm-auth-btn ${current === a.id ? 'selected' : ''}" id="ifm-ab-${a.id}" onclick="_ifmSelectAuth('${a.id}')">
        ${a.icon} ${a.label}
      </div>`).join('')}
  </div>
  <div id="ifm-auth-form"></div>
</div>
<div class="ifm-card">
  <div class="ifm-section-title">TLS / Network Settings</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
    <div><label class="ifm-field-label">Connection Timeout (ms)</label>
      <input id="ifm-ac-conn-timeout" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-conn-timeout']||'3000')}" style="font-size:11px;font-family:var(--mono);" /></div>
    <div><label class="ifm-field-label">Read Timeout (ms)</label>
      <input id="ifm-ac-read-timeout" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-read-timeout']||'5000')}" style="font-size:11px;font-family:var(--mono);" /></div>
    <div style="display:flex;align-items:center;gap:8px;">
      <input type="checkbox" id="ifm-ac-tls-verify" ${ac['ifm-ac-tls-verify']!=='false'?'checked':''} />
      <label for="ifm-ac-tls-verify" class="ifm-field-label" style="margin:0;cursor:pointer;">Verify TLS Certificate</label>
    </div>
    <div><label class="ifm-field-label">Proxy URL (optional)</label>
      <input id="ifm-ac-proxy" class="field-input" type="text" placeholder="http://proxy:3128" value="${_escIfm(ac['ifm-ac-proxy']||'')}" style="font-size:11px;font-family:var(--mono);" /></div>
  </div>
</div>`;

    _ifmRenderAuthForm(current);
}

function _ifmSelectAuth(id) {
    _IFM.authType = id;
    document.querySelectorAll('.ifm-auth-btn').forEach(el => el.classList.remove('selected'));
    const btn = document.getElementById('ifm-ab-' + id);
    if (btn) btn.classList.add('selected');
    _ifmRenderAuthForm(id);
}

function _ifmRenderAuthForm(id) {
    const wrap = document.getElementById('ifm-auth-form');
    if (!wrap) return;
    const authForms = {
        none:      `<div style="font-size:11px;color:var(--text3);">No authentication required.</div>`,
        bearer:    `<label class="ifm-field-label">Bearer Token</label><input id="ifm-ac-token" class="field-input" type="password" placeholder="eyJhbGci…" value="${_escIfm(_IFM.authConfig['ifm-ac-token']||'')}" style="font-size:11px;font-family:var(--mono);" />`,
        api_key:   `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">API Key</label><input id="ifm-ac-apikey" class="field-input" type="password" placeholder="sk-…" value="${_escIfm(_IFM.authConfig['ifm-ac-apikey']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Header Name</label><input id="ifm-ac-header-name" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-header-name']||'Authorization')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        x_api_key: `<label class="ifm-field-label">x-api-key</label><input id="ifm-ac-xapikey" class="field-input" type="password" placeholder="sk-ant-…" value="${_escIfm(_IFM.authConfig['ifm-ac-xapikey']||'')}" style="font-size:11px;font-family:var(--mono);" />`,
        basic:     `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Username</label><input id="ifm-ac-user" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-user']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Password</label><input id="ifm-ac-pass" class="field-input" type="password" value="${_escIfm(_IFM.authConfig['ifm-ac-pass']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        header:    `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Header Name</label><input id="ifm-ac-h-name" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-h-name']||'X-API-Token')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Header Value</label><input id="ifm-ac-h-val" class="field-input" type="password" value="${_escIfm(_IFM.authConfig['ifm-ac-h-val']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        aws_sigv4: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">AWS Region</label><input id="ifm-ac-aws-region" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-aws-region']||'us-east-1')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Service</label><input id="ifm-ac-aws-service" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-aws-service']||'sagemaker')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        aws_keys:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Access Key ID</label><input id="ifm-ac-aws-kid" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-aws-kid']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Secret Access Key</label><input id="ifm-ac-aws-sak" class="field-input" type="password" value="${_escIfm(_IFM.authConfig['ifm-ac-aws-sak']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">AWS Region</label><input id="ifm-ac-aws-region" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-aws-region']||'us-east-1')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        minio_keys:`<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Access Key</label><input id="ifm-ac-minio-key" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-minio-key']||'minioadmin')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Secret Key</label><input id="ifm-ac-minio-secret" class="field-input" type="password" value="${_escIfm(_IFM.authConfig['ifm-ac-minio-secret']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        azure_ad:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Tenant ID</label><input id="ifm-ac-az-tenant" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-az-tenant']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Client ID</label><input id="ifm-ac-az-client" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-az-client']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Client Secret</label><input id="ifm-ac-az-secret" class="field-input" type="password" value="${_escIfm(_IFM.authConfig['ifm-ac-az-secret']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        google_sa: `<label class="ifm-field-label">Service Account Key Path</label><input id="ifm-ac-gsa-path" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-gsa-path']||'/etc/gcp/sa-key.json')}" style="font-size:11px;font-family:var(--mono);" />`,
        tls_cert:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Cert Path</label><input id="ifm-ac-tls-cert" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-tls-cert']||'/etc/certs/client.crt')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Key Path</label><input id="ifm-ac-tls-key" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-tls-key']||'/etc/certs/client.key')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">CA Path</label><input id="ifm-ac-tls-ca" class="field-input" type="text" value="${_escIfm(_IFM.authConfig['ifm-ac-tls-ca']||'/etc/certs/ca.crt')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
    };
    wrap.innerHTML = authForms[id] || authForms['none'];
}

function _ifmCollectAuthConfig() {
    const ac = {};
    document.querySelectorAll('[id^="ifm-ac-"]').forEach(el => {
        ac[el.id] = el.type === 'checkbox' ? String(el.checked) : el.value;
    });
    _IFM.authConfig = ac;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 4 — Inference I/O
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep4() {
    const body = document.getElementById('ifm-body');
    const ic = _IFM.inferenceConfig;
    const cols = _IFM.inputColumns;
    const numericTypes = ['INT','BIGINT','DOUBLE','FLOAT','DECIMAL','SMALLINT'];
    const numCols = cols.filter(c => numericTypes.some(t => c.type.toUpperCase().startsWith(t)));

    body.innerHTML = `
<div class="ifm-info-box">
  Configure the <strong>input features</strong>, <strong>output</strong>, and <strong>pre/post-processing</strong> expressions.
  The model call is wrapped in a Flink <strong>async scalar UDF pattern</strong> for non-blocking parallel inference.
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
  <div>
    <div class="ifm-card">
      <div class="ifm-section-title">Input Features <span class="ifm-required">*</span></div>
      <div style="font-size:11px;color:var(--text2);margin-bottom:8px;">Select columns to pass to the model (or enter a custom expression).</div>
      <div style="display:flex;flex-wrap:wrap;margin-bottom:8px;" id="ifm-feat-tags">
        ${cols.map(c => {
        const isSel = _IFM.selectedInputCols.length === 0 || _IFM.selectedInputCols.find(f => f === c.name);
        return `<div class="ifm-col-tag ${isSel?'selected':''}" id="ifm-ftag-${c.name.replace(/\W/g,'_')}"
            onclick="_ifmToggleFeatCol('${c.name}')" title="${c.type}">
            ${c.name}<span class="ifm-tag-type">${c.type}</span>
          </div>`;
    }).join('')}
      </div>
      <div>
        <label class="ifm-field-label">Primary Input Column / Expression <span class="ifm-required">*</span></label>
        <input id="ifm-ic-input-col" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
          placeholder="amount" value="${_escIfm(ic.inputCol)}"
          list="ifm-ic-col-datalist" />
        <datalist id="ifm-ic-col-datalist">
          ${cols.map(c => `<option value="${_escIfm(c.name)}">`).join('')}
        </datalist>
        <div style="font-size:9px;color:var(--text3);margin-top:3px;">For multi-feature models: use ARRAY[col1, col2, col3] or a JSON expression.</div>
      </div>
    </div>

    <div class="ifm-card">
      <div class="ifm-section-title">Pre-processing (optional)</div>
      <label class="ifm-field-label">Transform before sending to model</label>
      <textarea id="ifm-ic-preprocess" class="field-input" rows="3"
        style="font-size:11px;font-family:var(--mono);resize:vertical;"
        placeholder="CAST(amount AS DOUBLE) / 1000.0&#10;-- or leave blank to pass raw column">${_escIfm(ic.preProcessExpr)}</textarea>
    </div>

    <div class="ifm-card">
      <div class="ifm-section-title">Passthrough Columns</div>
      <label class="ifm-field-label">Columns to carry forward alongside the prediction (comma-separated)</label>
      <input id="ifm-ic-passthrough" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
        placeholder="user_id, event_time, amount" value="${_escIfm(ic.passthroughCols)}" />
    </div>
  </div>

  <div>
    <div class="ifm-card">
      <div class="ifm-section-title">Model Output <span class="ifm-required">*</span></div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">
        <div><label class="ifm-field-label">Output Column Alias <span class="ifm-required">*</span></label>
          <input id="ifm-ic-output-alias" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            placeholder="fraud_score" value="${_escIfm(ic.outputAlias||'prediction')}" /></div>
        <div><label class="ifm-field-label">Output SQL Type</label>
          <select id="ifm-ic-output-type" class="field-input" style="font-size:11px;">
            ${['DOUBLE','FLOAT','BIGINT','INT','STRING','BOOLEAN','ARRAY<DOUBLE>','ROW<label STRING, score DOUBLE>','MAP<STRING,DOUBLE>'].map(t =>
        `<option value="${t}" ${ic.outputType===t?'selected':''}>${t}</option>`).join('')}
          </select></div>
      </div>
      <div><label class="ifm-field-label">Post-processing Expression (optional)</label>
        <textarea id="ifm-ic-postprocess" class="field-input" rows="3"
          style="font-size:11px;font-family:var(--mono);resize:vertical;"
          placeholder="-- e.g. CASE WHEN prediction > 0.85 THEN 'HIGH_RISK' ELSE 'LOW_RISK' END AS risk_tier">${_escIfm(ic.postProcessExpr)}</textarea>
      </div>
    </div>

    <div class="ifm-card">
      <div class="ifm-section-title">Async UDF Performance Settings</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
        <div><label class="ifm-field-label">Async Parallelism (concurrent requests)</label>
          <input id="ifm-ic-async-par" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            value="${_escIfm(ic.asyncParallelism||'4')}" placeholder="4" /></div>
        <div><label class="ifm-field-label">Request Timeout (ms)</label>
          <input id="ifm-ic-timeout" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            value="${_escIfm(ic.timeoutMs||'5000')}" placeholder="5000" /></div>
        <div><label class="ifm-field-label">Retry Count on Failure</label>
          <input id="ifm-ic-retries" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            value="${_escIfm(ic.retries||'2')}" placeholder="2" /></div>
        <div><label class="ifm-field-label">Default on Error (null = propagate)</label>
          <input id="ifm-ic-on-error" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            value="${_escIfm(ic.onError||'-1.0')}" placeholder="-1.0" /></div>
      </div>
    </div>

    <div class="ifm-card">
      <div class="ifm-section-title">Flink Job Settings</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
        <div><label class="ifm-field-label">Job Parallelism</label>
          <input id="ifm-ic-parallelism" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            value="${_escIfm(ic.parallelism||'4')}" placeholder="4" /></div>
        <div><label class="ifm-field-label">Checkpoint Interval (ms)</label>
          <input id="ifm-ic-checkpoint" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
            value="${_escIfm(ic.checkpointInterval||'10000')}" placeholder="10000" /></div>
      </div>
    </div>
  </div>
</div>`;

    if (_IFM.selectedInputCols.length === 0) {
        _IFM.selectedInputCols = cols.map(c => c.name);
    }
}

function _ifmToggleFeatCol(name) {
    const idx = _IFM.selectedInputCols.indexOf(name);
    if (idx >= 0) _IFM.selectedInputCols.splice(idx, 1);
    else _IFM.selectedInputCols.push(name);
    const tag = document.getElementById('ifm-ftag-' + name.replace(/\W/g,'_'));
    if (tag) tag.classList.toggle('selected', idx < 0);
}

function _ifmCollectInferenceConfig() {
    const g = id => document.getElementById(id)?.value?.trim() || '';
    _IFM.inferenceConfig = {
        inputCol:           g('ifm-ic-input-col'),
        outputAlias:        g('ifm-ic-output-alias') || 'prediction',
        outputType:         g('ifm-ic-output-type')  || 'DOUBLE',
        asyncParallelism:   g('ifm-ic-async-par')    || '4',
        timeoutMs:          g('ifm-ic-timeout')       || '5000',
        retries:            g('ifm-ic-retries')        || '2',
        onError:            g('ifm-ic-on-error')       || '-1.0',
        passthroughCols:    g('ifm-ic-passthrough'),
        preProcessExpr:     g('ifm-ic-preprocess'),
        postProcessExpr:    g('ifm-ic-postprocess'),
        parallelism:        g('ifm-ic-parallelism')    || '4',
        checkpointInterval: g('ifm-ic-checkpoint')     || '10000',
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 5 — Output Sink
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep5() {
    const body = document.getElementById('ifm-body');
    const src = _IFM.sourceTable || 'source_table';

    body.innerHTML = `
<div class="ifm-info-box">
  Configure the <strong>output table</strong> that receives the original stream columns plus the model inference result.
</div>
<div class="ifm-card">
  <div class="ifm-section-title">Output Table</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;">
    <div>
      <label class="ifm-field-label">Output Table Name <span class="ifm-required">*</span></label>
      <input id="ifm-out-table" class="field-input" type="text" style="font-size:11px;font-family:var(--mono);"
        placeholder="${src}_scored"
        value="${_escIfm(_IFM.outputTable || (src + '_scored'))}"
        oninput="_IFM.outputTable=this.value" />
    </div>
    <div>
      <label class="ifm-field-label">Sink Type</label>
      <select id="ifm-sink-type" class="field-input" style="font-size:11px;"
        onchange="_IFM.sinkType=this.value;_ifmRenderIfmSinkParams(this.value)">
        <option value="kafka">Apache Kafka</option>
        <option value="jdbc">JDBC (PostgreSQL / MySQL)</option>
        <option value="elasticsearch">Elasticsearch / OpenSearch</option>
        <option value="filesystem">File System / S3</option>
        <option value="iceberg">Apache Iceberg</option>
        <option value="print">Print (debug)</option>
        <option value="blackhole">Blackhole (throughput test)</option>
      </select>
    </div>
  </div>
  <div id="ifm-sink-params-wrap"></div>
</div>`;

    document.getElementById('ifm-sink-type').value = _IFM.sinkType || 'kafka';
    _ifmRenderIfmSinkParams(_IFM.sinkType || 'kafka');
}

function _ifmRenderIfmSinkParams(type) {
    const wrap = document.getElementById('ifm-sink-params-wrap');
    if (!wrap) return;
    _IFM.sinkType = type;
    // Collect existing sink config before re-rendering (preserves values on type change)
    _ifmCollectSinkConfig();
    const sc = _IFM.sinkConfig || {};
    const fi = `style="width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:3px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;"`;
    const inp = (label, id, ph, isPass, req) =>
        `<div><label class="ifm-field-label">${label}${req?' <span style="color:var(--red)">*</span>':''}</label>
     <input type="${isPass?'password':'text'}" id="${id}" ${fi} placeholder="${ph||''}" value="${_escIfm(sc[id]||'')}" /></div>`;
    const sel = (label, id, opts, val) =>
        `<div><label class="ifm-field-label">${label}</label>
     <select id="${id}" ${fi}>${opts.map(o=>`<option value="${o}" ${(sc[id]||val||opts[0])===o?'selected':''}>${o}</option>`).join('')}</select></div>`;

    const forms = {
        kafka: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Bootstrap Servers <span style="color:var(--red)">*</span>','ifm-sk-bootstrap','kafka:9092')}
      ${inp('Output Topic <span style="color:var(--red)">*</span>','ifm-sk-topic','scored-output')}
      ${sel('Format','ifm-sk-format',['json','avro','avro-confluent','csv'])}
      ${sel('Security Protocol','ifm-sk-security',['PLAINTEXT','SASL_SSL','SSL','SASL_PLAINTEXT'],'PLAINTEXT')}
      ${inp('SASL Username / API Key','ifm-sk-sasl-user','api-key')}
      ${inp('SASL Password / Secret','ifm-sk-sasl-pass','',true)}
      ${inp('Schema Registry URL (avro-confluent)','ifm-sk-sr-url','http://schema-registry:8081')}
    </div>`,
        jdbc: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('JDBC URL <span style="color:var(--red)">*</span>','ifm-sk-jdbc-url','jdbc:postgresql://localhost:5432/ml_results')}
      ${inp('Target Table <span style="color:var(--red)">*</span>','ifm-sk-jdbc-table','public.fraud_scores')}
      ${inp('Username','ifm-sk-jdbc-user','flink_writer')}
      ${inp('Password','ifm-sk-jdbc-pass','',true)}
      ${sel('Driver','ifm-sk-jdbc-driver',['org.postgresql.Driver','com.mysql.cj.jdbc.Driver','com.amazon.redshift.jdbc.Driver'])}
      ${inp('Sink Parallelism','ifm-sk-jdbc-par','4')}
    </div>`,
        elasticsearch: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('ES Hosts <span style="color:var(--red)">*</span>','ifm-sk-es-hosts','http://elasticsearch:9200')}
      ${inp('Index <span style="color:var(--red)">*</span>','ifm-sk-es-index','ml-scored-results')}
      ${sel('ES Version','ifm-sk-es-ver',['7','8'])}
      ${inp('Username','ifm-sk-es-user','elastic')}
      ${inp('Password','ifm-sk-es-pass','',true)}
    </div>`,
        filesystem: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Sink Path <span style="color:var(--red)">*</span>','ifm-sk-fs-path','s3://my-bucket/scored/')}
      ${sel('Format','ifm-sk-fs-format',['parquet','orc','json','csv'])}
      ${inp('Rolling Interval','ifm-sk-fs-roll','10 min')}
      ${inp('Partition Column','ifm-sk-fs-part','')}
    </div>`,
        iceberg: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      ${inp('Catalog Name','ifm-sk-ice-cat','iceberg_catalog')}
      ${inp('Database.Table <span style="color:var(--red)">*</span>','ifm-sk-ice-tbl','ml.fraud_scores')}
      ${sel('Catalog Type','ifm-sk-ice-cattype',['hive','hadoop','rest','glue'])}
      ${inp('Warehouse Path','ifm-sk-ice-wh','s3://warehouse/iceberg')}
    </div>`,
        print:     `<div style="font-size:11px;color:var(--text3);padding:6px 0;">Outputs to TaskManager stdout. Development only — no credentials needed.</div>`,
        blackhole: `<div style="font-size:11px;color:var(--text3);padding:6px 0;">Discards all output — no credentials needed. Use for throughput benchmarking.</div>`,
    };
    wrap.innerHTML = forms[type] || '';
}

function _ifmCollectSinkConfig() {
    const cfg = {};
    const ids = [
        'ifm-sk-bootstrap','ifm-sk-topic','ifm-sk-format','ifm-sk-security',
        'ifm-sk-sasl-user','ifm-sk-sasl-pass','ifm-sk-sr-url',
        'ifm-sk-jdbc-url','ifm-sk-jdbc-table','ifm-sk-jdbc-user','ifm-sk-jdbc-pass','ifm-sk-jdbc-driver','ifm-sk-jdbc-par',
        'ifm-sk-es-hosts','ifm-sk-es-index','ifm-sk-es-ver','ifm-sk-es-user','ifm-sk-es-pass',
        'ifm-sk-fs-path','ifm-sk-fs-format','ifm-sk-fs-roll','ifm-sk-fs-part',
        'ifm-sk-ice-cat','ifm-sk-ice-tbl','ifm-sk-ice-cattype','ifm-sk-ice-wh',
    ];
    ids.forEach(id => {
        const el = document.getElementById(id);
        if (el) cfg[id] = el.value;
    });
    _IFM.sinkConfig = { ...(_IFM.sinkConfig || {}), ...cfg };
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 6 — Review & SQL
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// IFM Canvas state (zoom / pan / maximise)
// ─────────────────────────────────────────────────────────────────────────────
const _IFMC = {
    zoom: 1, panX: 0, panY: 0,
    panning: false, panSX: 0, panSY: 0, panOX: 0, panOY: 0,
    maximised: false, origStyle: null,
    svgW: 0, svgH: 0,
};

function _ifmApplyCanvasTransform() {
    const svg = document.getElementById('ifm-canvas-svg');
    if (svg) svg.style.transform = `translate(${_IFMC.panX}px,${_IFMC.panY}px) scale(${_IFMC.zoom})`;
    const lbl = document.getElementById('ifm-canvas-zoom-lbl');
    if (lbl) lbl.textContent = Math.round(_IFMC.zoom * 100) + '%';
}

function _ifmCanvasZoom(delta, cx, cy) {
    const wrap = document.getElementById('ifm-canvas-wrap');
    if (!wrap) return;
    const r = wrap.getBoundingClientRect();
    const mx = cx !== undefined ? cx - r.left : r.width / 2;
    const my = cy !== undefined ? cy - r.top  : r.height / 2;
    const prev = _IFMC.zoom;
    _IFMC.zoom = Math.max(0.12, Math.min(4, _IFMC.zoom + delta));
    _IFMC.panX = mx - (mx - _IFMC.panX) * (_IFMC.zoom / prev);
    _IFMC.panY = my - (my - _IFMC.panY) * (_IFMC.zoom / prev);
    _ifmApplyCanvasTransform();
}

function _ifmCanvasFitToView() {
    const wrap = document.getElementById('ifm-canvas-wrap');
    if (!wrap || !_IFMC.svgW || !_IFMC.svgH) return;
    const PAD = 28;
    const wW = wrap.clientWidth  - PAD * 2;
    const wH = wrap.clientHeight - PAD * 2;
    _IFMC.zoom = Math.min(4, Math.max(0.12, Math.min(wW / _IFMC.svgW, wH / _IFMC.svgH)));
    _IFMC.panX = (wrap.clientWidth  - _IFMC.svgW * _IFMC.zoom) / 2;
    _IFMC.panY = (wrap.clientHeight - _IFMC.svgH * _IFMC.zoom) / 2;
    _ifmApplyCanvasTransform();
}

function _ifmToggleCanvasMaximise() {
    const container = document.getElementById('ifm-canvas-container');
    const btn = document.getElementById('ifm-canvas-max-btn');
    if (!container || !btn) return;
    if (!_IFMC.maximised) {
        _IFMC.origStyle = container.getAttribute('style') || '';
        _IFMC.maximised = true;
        container.style.cssText = `
      position:fixed;inset:0;z-index:9990;
      background:var(--bg0,#050810);
      display:flex;flex-direction:column;
      border:none;border-radius:0;
    `;
        btn.textContent = '⊟';
        btn.title = 'Restore canvas';
    } else {
        _IFMC.maximised = false;
        container.setAttribute('style', _IFMC.origStyle || '');
        btn.textContent = '⊞';
        btn.title = 'Maximise canvas';
    }
    setTimeout(_ifmCanvasFitToView, 60);
}

function _ifmWireCanvasInteraction() {
    const wrap = document.getElementById('ifm-canvas-wrap');
    if (!wrap || wrap._ifmWired) return;
    wrap._ifmWired = true;

    wrap.addEventListener('wheel', e => {
        e.preventDefault();
        _ifmCanvasZoom(e.deltaY < 0 ? 0.12 : -0.12, e.clientX, e.clientY);
    }, { passive: false });

    wrap.addEventListener('mousedown', e => {
        if (e.button !== 0) return;
        _IFMC.panning = true;
        _IFMC.panSX = e.clientX; _IFMC.panSY = e.clientY;
        _IFMC.panOX = _IFMC.panX; _IFMC.panOY = _IFMC.panY;
        wrap.style.cursor = 'grabbing';
        e.preventDefault();
    });
    const onMove = e => {
        if (!_IFMC.panning) return;
        _IFMC.panX = _IFMC.panOX + (e.clientX - _IFMC.panSX);
        _IFMC.panY = _IFMC.panOY + (e.clientY - _IFMC.panSY);
        _ifmApplyCanvasTransform();
    };
    const onUp = () => { if (!_IFMC.panning) return; _IFMC.panning = false; wrap.style.cursor = 'grab'; };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);

    wrap.addEventListener('dblclick', () => {
        _IFMC.zoom = 1; _IFMC.panX = 0; _IFMC.panY = 0;
        _ifmCanvasFitToView();
    });
}

function _ifmRenderStep6() {
    _IFM.outputTable = document.getElementById('ifm-out-table')?.value?.trim() || _IFM.outputTable || (_IFM.sourceTable + '_scored');
    const sql = _ifmGenerateSql();
    _IFM.generatedSql = sql;
    _ifmSaveHistory();
    _ifmUpdateHistCount();

    // Reset canvas state
    _IFMC.zoom = 1; _IFMC.panX = 0; _IFMC.panY = 0;
    _IFMC.maximised = false; _IFMC.origStyle = null;

    const srv = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const body = document.getElementById('ifm-body');
    body.innerHTML = `
<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
  <div>
    <div style="font-size:13px;font-weight:700;color:var(--text0);">Generated Inference Pipeline SQL</div>
    <div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:2px;">
      <span style="color:var(--blue);">${_escIfm(_IFM.sourceTable)}</span>
      → <span style="color:var(--yellow);">${srv.icon} ${srv.name}</span>
      → <span style="color:var(--blue);">${_escIfm(_IFM.inferenceConfig.outputAlias)}</span>
      → <span style="color:var(--accent);">${_escIfm(_IFM.outputTable)}</span>
      <span style="margin-left:6px;background:rgba(79,163,224,0.1);color:var(--blue);padding:1px 7px;border-radius:10px;font-size:9px;font-weight:700;">SAVED TO HISTORY</span>
    </div>
  </div>
  <div style="margin-left:auto;">
    <button onclick="navigator.clipboard.writeText(document.getElementById('ifm-sql-out').textContent).then(()=>{if(typeof toast==='function')toast('SQL copied','ok');})"
      style="font-size:10px;padding:4px 10px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;font-family:var(--mono);">Copy SQL</button>
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;height:calc(90vh - 255px);min-height:380px;">

  <!-- ── Canvas panel ── -->
  <div id="ifm-canvas-container" style="display:flex;flex-direction:column;overflow:hidden;background:var(--bg0);border:1px solid rgba(79,163,224,0.25);border-radius:5px;">
    <!-- Toolbar -->
    <div style="display:flex;align-items:center;gap:4px;padding:5px 8px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;">
      <span style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.2px;text-transform:uppercase;flex:1;">Inference Pipeline Canvas</span>
      <button onclick="_ifmCanvasZoom(-0.15)" title="Zoom out"
        style="font-size:13px;line-height:1;padding:1px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">−</button>
      <span id="ifm-canvas-zoom-lbl" style="font-size:10px;color:var(--text3);font-family:var(--mono);min-width:36px;text-align:center;">100%</span>
      <button onclick="_ifmCanvasZoom(0.15)" title="Zoom in"
        style="font-size:13px;line-height:1;padding:1px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">+</button>
      <button onclick="_ifmCanvasFitToView()" title="Fit to view"
        style="font-size:10px;padding:2px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;">⊙ Fit</button>
      <button id="ifm-canvas-max-btn" onclick="_ifmToggleCanvasMaximise()" title="Maximise canvas"
        style="font-size:12px;line-height:1;padding:2px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;">⊞</button>
    </div>
    <!-- Viewport -->
    <div id="ifm-canvas-wrap" style="flex:1;overflow:hidden;position:relative;cursor:grab;background:var(--bg0);">
      <svg id="ifm-canvas-svg" style="transform-origin:0 0;will-change:transform;display:block;overflow:visible;"></svg>
    </div>
    <!-- Legend -->
    <div style="display:flex;gap:10px;padding:4px 10px;border-top:1px solid var(--border);flex-shrink:0;background:var(--bg2);flex-wrap:wrap;">
      <span style="font-size:9px;color:#4e9de8;font-family:var(--mono);">◉ Source cols</span>
      <span style="font-size:9px;color:#b080e0;font-family:var(--mono);">⨍ Pre-process</span>
      <span style="font-size:9px;color:#4fa3e0;font-family:var(--mono);">${srv.icon} Model</span>
      <span style="font-size:9px;color:#00d4aa;font-family:var(--mono);">▣ Output</span>
      <span style="font-size:9px;color:var(--text3);font-family:var(--mono);margin-left:auto;">scroll=zoom · drag=pan · dblclick=fit</span>
    </div>
  </div>

  <!-- ── SQL panel ── -->
  <div style="display:flex;flex-direction:column;overflow:hidden;">
    <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:7px;">GENERATED SQL</div>
    <pre id="ifm-sql-out" class="ifm-sql-preview">${_escIfm(sql)}</pre>
  </div>
</div>

<div style="margin-top:10px;background:rgba(79,163,224,0.04);border:1px solid rgba(79,163,224,0.15);border-radius:4px;padding:9px 13px;font-size:11px;color:var(--text1);line-height:1.7;">
  <strong style="color:var(--blue,#4fa3e0);">Implementation note:</strong>
  The generated SQL uses a <strong>placeholder UDF</strong> pattern — <code style="color:var(--accent);">CALL_MODEL_UDF</code>.
  Replace this with your registered Flink async UDF name (via UDF Manager or Systems Manager).
  For HTTP-based servers, implement <code>RichAsyncFunction&lt;Row, Row&gt;</code> using Apache HttpAsyncClient or OkHttp.
</div>`;

    setTimeout(() => { _ifmDrawCanvasSvg(); _ifmWireCanvasInteraction(); }, 80);
}

// ─────────────────────────────────────────────────────────────────────────────
// Inference Canvas — SVG-based, fully zoomable / pannable
// ─────────────────────────────────────────────────────────────────────────────
function _ifmDrawCanvasSvg() {
    const svgEl = document.getElementById('ifm-canvas-svg');
    const wrap  = document.getElementById('ifm-canvas-wrap');
    if (!svgEl || !wrap) return;

    const srv   = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const cols  = (_IFM.selectedInputCols.length ? _IFM.selectedInputCols : _IFM.inputColumns.map(c=>c.name));
    const ic    = _IFM.inferenceConfig;
    const alias = ic.outputAlias || 'prediction';
    const outTable = _IFM.outputTable || 'output';
    const hasPre  = !!ic.preProcessExpr?.trim();
    const hasPost = !!ic.postProcessExpr?.trim();
    const ptCols  = (ic.passthroughCols || '').split(',').map(c=>c.trim()).filter(Boolean);

    // ── Layout
    const PAD    = 28;
    const NODE_W = 155;
    const COL_H  = 22;
    const COL_GAP = 4;
    const MODEL_H = 90;
    const HGAP   = 75;

    // Column count drives height
    const displayCols = cols.slice(0, 20);
    const colsH = displayCols.length * (COL_H + COL_GAP) - COL_GAP;
    const srcH  = 44;

    // X positions: source | [pre-process] | model | [post] | output
    const colCount = 2 + (hasPre ? 1 : 0) + (hasPost ? 1 : 0);
    const X0 = PAD;
    const X1 = X0 + NODE_W + HGAP;
    const X2 = X1 + (hasPre ? NODE_W + HGAP : 0);
    const X3 = X2 + NODE_W + HGAP;
    const X4 = X3 + (hasPost ? NODE_W + HGAP : 0);
    const X_OUT = hasPost ? X4 : X3 + NODE_W + HGAP;

    const totalW = X_OUT + NODE_W + PAD;
    const contentH = Math.max(srcH + 16 + colsH, MODEL_H + 60, 200) + PAD * 2;
    const midY = contentH / 2;
    const totalH = contentH;

    _IFMC.svgW = totalW;
    _IFMC.svgH = totalH;

    // ── Colours
    const C = {
        bg:    '#060a12',
        grid:  'rgba(79,163,224,0.04)',
        src:   { fill:'rgba(26,111,168,0.16)',  stroke:'#4e9de8', text:'#a8d4f5', sub:'#4a7a9a' },
        col:   { fill:'rgba(26,111,168,0.09)',  stroke:'rgba(78,157,232,0.4)', text:'#7aafd4', type:'#3a6080' },
        pre:   { fill:'rgba(90,42,138,0.16)',   stroke:'#b080e0', text:'#d4b0ff', sub:'#8060b0' },
        model: { fill:'rgba(79,163,224,0.09)',  stroke:'#4fa3e0', text:'#c0e0ff', sub:'#6090c0' },
        post:  { fill:'rgba(90,42,138,0.13)',   stroke:'#b080e088', text:'#c0a0f0', sub:'#7050a0' },
        out:   { fill:'rgba(0,212,170,0.13)',   stroke:'#00d4aa', text:'#70f0d8', sub:'#30a090' },
        pt:    { fill:'rgba(79,163,224,0.07)',  stroke:'rgba(79,163,224,0.3)', text:'#6090c0' },
        edge:  { src:'rgba(78,157,232,0.5)', pre:'rgba(176,128,224,0.6)', model:'rgba(79,163,224,0.7)', out:'rgba(0,212,170,0.6)' },
    };

    const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    const tr  = (s, n) => (s||'').length > n ? (s||'').slice(0,n)+'…' : (s||'');

    const rr = (x,y,w,h,r,fill,stroke,sw) =>
        `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${sw||1.5}"/>`;

    const txt = (s,x,y,fill,size,weight,anchor) =>
        `<text x="${x}" y="${y}" fill="${fill}" font-size="${size||10}" font-weight="${weight||'400'}"
      font-family="var(--mono,monospace)" text-anchor="${anchor||'start'}" dominant-baseline="middle">${esc(s)}</text>`;

    const bezier = (x1,y1,x2,y2,color,sw) => {
        const cp = Math.abs(x2-x1)*0.42;
        return `<path d="M${x1},${y1} C${x1+cp},${y1} ${x2-cp},${y2} ${x2},${y2}"
      stroke="${color}" stroke-width="${sw||1.4}" fill="none" opacity="0.9" marker-end="url(#ifm-arr)"/>`;
    };

    let s = `<defs>
    <marker id="ifm-arr" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
      <path d="M0,0.5 L0,6.5 L7,3.5 z" fill="rgba(130,170,220,0.55)"/>
    </marker>
    <filter id="ifm-glow"><feGaussianBlur stdDeviation="3" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
    <radialGradient id="ifm-model-grad" cx="50%" cy="50%" r="50%">
      <stop offset="0%" stop-color="${C.model.stroke}" stop-opacity="0.18"/>
      <stop offset="100%" stop-color="${C.model.stroke}" stop-opacity="0.04"/>
    </radialGradient>
  </defs>`;

    // Background + grid
    s += `<rect width="${totalW}" height="${totalH}" fill="${C.bg}"/>`;
    for (let x=0;x<=totalW;x+=22) s += `<line x1="${x}" y1="0" x2="${x}" y2="${totalH}" stroke="${C.grid}" stroke-width="0.5"/>`;
    for (let y=0;y<=totalH;y+=22) s += `<line x1="0" y1="${y}" x2="${totalW}" y2="${y}" stroke="${C.grid}" stroke-width="0.5"/>`;

    // Column headers
    const headers = [
        { x:X0,   l:'SOURCE FEATURES' },
        hasPre  ? { x:X1, l:'PRE-PROCESS' } : null,
        { x:hasPre?X2:X1, l:`MODEL · ${srv.name.toUpperCase()}` },
        hasPost ? { x:X3+NODE_W+HGAP, l:'POST-PROCESS' } : null,
        { x:X_OUT, l:'OUTPUT' },
    ].filter(Boolean);
    headers.forEach(h => { s += txt(h.l, h.x, PAD-10, 'rgba(100,150,200,0.4)', 8, '700'); });

    // ── Source node
    const srcY = PAD;
    const srcMid = srcY + srcH/2;
    s += rr(X0, srcY, NODE_W, srcH, 6, C.src.fill, C.src.stroke, 2);
    s += txt('◉', X0+8, srcMid, C.src.stroke, 14);
    s += txt(tr(_IFM.sourceTable||'source',14), X0+26, srcMid-7, C.src.text, 11, '700');
    s += txt(`${cols.length} input features`, X0+26, srcMid+7, C.src.sub, 9);

    // ── Feature column nodes
    const colY0 = srcY + srcH + 14;
    const colRx = X0 + NODE_W;
    const colMids = [];
    displayCols.forEach((c, i) => {
        const y = colY0 + i*(COL_H+COL_GAP);
        const mid = y + COL_H/2;
        const isInput = c === ic.inputCol;
        const isPT = ptCols.includes(c);
        const borderC = isInput ? '#00d4aa' : isPT ? C.pt.stroke : C.col.stroke;
        const fillC   = isInput ? 'rgba(0,212,170,0.12)' : isPT ? C.pt.fill : C.col.fill;
        s += rr(X0, y, NODE_W, COL_H, 3, fillC, borderC, isInput?2:1);
        const colObj = _IFM.inputColumns.find(col=>col.name===c);
        s += txt(tr(c,14), X0+7, mid, isInput?'#00d4aa':isPT?'#6090c0':C.col.text, 10, isInput?'700':'400');
        if (colObj) s += txt(colObj.type, X0+NODE_W-6, mid, C.col.type, 8, '400', 'end');
        if (isInput) s += txt('→ model input', X0+7, mid+COL_H/2+3, 'rgba(0,212,170,0.5)', 7, '400');
        // Fan from source
        s += `<line x1="${colRx}" y1="${srcY+srcH}" x2="${colRx}" y2="${mid}" stroke="${C.edge.src}" stroke-width="0.8" opacity="0.3"/>`;
        colMids.push(mid);
    });
    // Trunk
    if (colMids.length > 1) {
        s += `<line x1="${colRx}" y1="${srcY+srcH}" x2="${colRx}" y2="${colMids[colMids.length-1]}" stroke="${C.edge.src}" stroke-width="0.8" opacity="0.2"/>`;
    }

    // ── Pre-process node
    let preRightX = colRx, preRightY = midY;
    if (hasPre) {
        const ppH = 44, ppY = midY - ppH/2;
        const ppMid = midY;
        s += rr(X1, ppY, NODE_W, ppH, 6, C.pre.fill, C.pre.stroke, 1.8);
        s += txt('⨍', X1+8, ppMid, C.pre.stroke, 15);
        s += txt('Pre-process', X1+26, ppMid-8, C.pre.text, 11, '600');
        s += txt(tr(ic.preProcessExpr||'transform',16), X1+26, ppMid+6, C.pre.sub, 8);
        // Edges from all cols
        colMids.forEach(cy => s += bezier(colRx, cy, X1, ppMid, C.edge.pre, 1.1));
        preRightX = X1+NODE_W; preRightY = ppMid;
    } else {
        // Direct edges from input col to model
        const inputIdx = displayCols.indexOf(ic.inputCol);
        const targetMids = inputIdx >= 0 ? [colMids[inputIdx]] : colMids.slice(0,3);
        targetMids.forEach(cy => {
            const edgeMidY = (cy + midY)/2;
            s += bezier(colRx, cy, hasPre?X2:X1, midY, C.edge.pre, 1.2);
        });
        preRightX = colRx; preRightY = colMids[0] || midY;
    }

    // ── Model server node (hero node, larger)
    const modelX = hasPre ? X2 : X1;
    const modelY = midY - MODEL_H/2;
    const modelMidY = midY;
    // Outer glow ring
    s += `<circle cx="${modelX+NODE_W/2}" cy="${modelMidY}" r="${MODEL_H*0.62}" fill="url(#ifm-model-grad)" stroke="${C.model.stroke}" stroke-width="1" opacity="0.5"/>`;
    s += `<circle cx="${modelX+NODE_W/2}" cy="${modelMidY}" r="${MODEL_H*0.62}" fill="none" stroke="${C.model.stroke}" stroke-width="1.5" stroke-dasharray="5 5" opacity="0.2"/>`;
    // Main box
    s += rr(modelX, modelY, NODE_W, MODEL_H, 10, C.model.fill, C.model.stroke, 2.5);
    // Glow effect
    s += `<rect x="${modelX}" y="${modelY}" width="${NODE_W}" height="${MODEL_H}" rx="10" fill="none" stroke="${C.model.stroke}" stroke-width="1" opacity="0.15" filter="url(#ifm-glow)"/>`;
    // Icon
    s += `<text x="${modelX+NODE_W/2}" y="${modelMidY-16}" font-size="22" text-anchor="middle" dominant-baseline="middle" font-family="serif">${esc(srv.icon)}</text>`;
    s += txt(tr(srv.name,16), modelX+NODE_W/2, modelMidY+4, C.model.text, 11, '700', 'center');
    s += txt(_IFM.modelServer, modelX+NODE_W/2, modelMidY+17, C.model.sub, 8, '400', 'center');
    s += txt('async UDF', modelX+NODE_W/2, modelY-12, 'rgba(79,163,224,0.4)', 8, '400', 'center');
    // Edge from pre/cols
    if (hasPre) s += bezier(preRightX, preRightY, modelX, modelMidY, C.edge.model, 1.6);

    // ── Post-process node
    const modelRx = modelX + NODE_W;
    let outLeftX = modelRx, outLeftY = modelMidY;
    if (hasPost) {
        const ppH = 40, ppY = modelMidY - ppH/2;
        const ppMid = modelMidY;
        s += rr(X3+NODE_W+HGAP, ppY, NODE_W, ppH, 5, C.post.fill, C.post.stroke, 1.5);
        s += txt('⨍', X3+NODE_W+HGAP+8, ppMid, C.post.stroke, 13);
        s += txt('Post-process', X3+NODE_W+HGAP+24, ppMid-7, C.post.text, 10, '600');
        s += txt(tr(ic.postProcessExpr||'transform',14), X3+NODE_W+HGAP+24, ppMid+6, C.post.sub, 8);
        s += bezier(modelRx, modelMidY, X3+NODE_W+HGAP, ppMid, C.edge.pre, 1.4);
        outLeftX = X3+NODE_W+HGAP+NODE_W; outLeftY = ppMid;
    }

    // ── Output node
    const outH = 52, outY = midY - outH/2, outMid = midY;
    s += rr(X_OUT, outY, NODE_W, outH, 7, C.out.fill, C.out.stroke, 2.5);
    s += `<rect x="${X_OUT}" y="${outY}" width="${NODE_W}" height="${outH}" rx="7" fill="none" stroke="${C.out.stroke}" stroke-width="1" opacity="0.18" filter="url(#ifm-glow)"/>`;
    s += txt('▣', X_OUT+8, outMid, C.out.stroke, 14);
    s += txt(tr(alias,14), X_OUT+26, outMid-9, C.out.text, 11, '700');
    s += txt(`${ic.outputType||'DOUBLE'}`, X_OUT+26, outMid+3, 'rgba(0,212,170,0.6)', 9);
    s += txt(`→ ${tr(outTable,12)}`, X_OUT+26, outMid+15, C.out.sub, 8);
    // Edge from model or post
    if (!hasPost) s += bezier(modelRx, modelMidY, X_OUT, outMid, C.edge.out, 1.6);
    else s += bezier(outLeftX, outLeftY, X_OUT, outMid, C.edge.out, 1.6);

    // ── Passthrough annotation
    if (ptCols.length) {
        const ptY = outY + outH + 8;
        const ptH = 16 + ptCols.length * 14;
        s += rr(X_OUT, ptY, NODE_W, ptH, 4, C.pt.fill, C.pt.stroke, 1);
        s += txt('passthrough', X_OUT+8, ptY+10, C.pt.text, 8, '700');
        ptCols.slice(0,5).forEach((c,i) => {
            s += txt(`· ${c}`, X_OUT+10, ptY+22+i*13, '#6090c0', 9);
        });
    }

    // ── Async parallelism badge on model
    if (ic.asyncParallelism) {
        s += `<rect x="${modelX+NODE_W-28}" y="${modelY+2}" width="26" height="14" rx="3" fill="rgba(79,163,224,0.15)" stroke="${C.model.stroke}" stroke-width="0.8"/>`;
        s += txt(`p${ic.asyncParallelism}`, modelX+NODE_W-15, modelY+9, C.model.stroke, 8, '700', 'middle');
    }

    svgEl.setAttribute('width', totalW);
    svgEl.setAttribute('height', totalH);
    svgEl.setAttribute('viewBox', `0 0 ${totalW} ${totalH}`);
    svgEl.innerHTML = s;

    setTimeout(_ifmCanvasFitToView, 30);
}

// Legacy stub — keep for any existing setTimeout references
function _ifmDrawCanvas() { _ifmDrawCanvasSvg(); }

// ─────────────────────────────────────────────────────────────────────────────
// SQL Generation
// ─────────────────────────────────────────────────────────────────────────────
function _ifmGenerateSql() {
    // Ensure latest sink fields are collected before generating
    _ifmCollectSinkConfig();

    const src   = _IFM.sourceTable || 'source_table';
    const out   = _IFM.outputTable || (src + '_scored');
    const ic    = _IFM.inferenceConfig;
    const srv   = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const mc    = _IFM.modelConfig;
    const ac    = _IFM.authConfig;
    const sc    = _IFM.sinkConfig || {};
    const cols  = _IFM.inputColumns;
    const ptCols = (ic.passthroughCols || '').split(',').map(c=>c.trim()).filter(Boolean);
    const inputCol    = ic.inputCol    || (cols[0]?.name || 'feature_col');
    const outputAlias = ic.outputAlias || 'prediction';
    const outputType  = ic.outputType  || 'DOUBLE';

    // ── helpers
    const q   = v => v ? `'${v}'` : "'<REQUIRED>'";         // single-quote value
    const pad = (k, width) => k.padEnd(width || 36);         // key alignment

    const lines = [];
    lines.push('-- ═══════════════════════════════════════════════════════════════════════════');
    lines.push('-- Real-time ML Inference Pipeline');
    lines.push(`-- Model Server  : ${srv.icon} ${srv.name}`);
    lines.push(`-- Auth          : ${_IFM.authType}`);
    lines.push(`-- Source Table  : ${src}`);
    lines.push(`-- Output Sink   : ${_IFM.sinkType} → ${out}`);
    lines.push(`-- Prediction    : ${outputAlias} (${outputType})`);
    lines.push(`-- Generated     : ${new Date().toISOString()}`);
    lines.push('-- Str:::lab Studio — Inference Manager');
    lines.push('-- ═══════════════════════════════════════════════════════════════════════════\n');

    lines.push(`SET 'execution.runtime-mode' = 'streaming';`);
    lines.push(`SET 'parallelism.default' = '${ic.parallelism || 4}';`);
    lines.push(`SET 'execution.checkpointing.interval' = '${ic.checkpointInterval || 10000}';\n`);

    // ── Source table reference (commented — it already exists in the session)
    lines.push(`-- Source table schema reference (table already created in your session)`);
    lines.push(`-- CREATE TEMPORARY TABLE IF NOT EXISTS ${src} (`);
    cols.forEach(c => lines.push(`--   ${c.name.padEnd(30)} ${c.type},`));
    lines.push(`-- ) WITH ( ... );\n`);

    // ── Output / scored-results sink DDL — fully populated from user config
    lines.push(`-- ── Output table: ${out} (${_IFM.sinkType} sink) ──────────────────────────────`);
    lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} (`);

    // All passthrough cols
    ptCols.forEach(c => {
        const colDef = cols.find(col => col.name === c);
        lines.push(`  ${c.padEnd(30)} ${colDef?.type || 'STRING'},`);
    });
    // Primary input col (if not already a passthrough)
    if (!ptCols.includes(inputCol)) {
        const inputType = cols.find(c => c.name === inputCol)?.type || 'DOUBLE';
        lines.push(`  ${inputCol.padEnd(30)} ${inputType},`);
    }
    // Model output
    lines.push(`  ${outputAlias.padEnd(30)} ${outputType}`);
    lines.push(`) WITH (`);

    switch (_IFM.sinkType) {
        case 'kafka': {
            const bootstrap = sc['ifm-sk-bootstrap'] || '<bootstrap.servers>';
            const topic     = sc['ifm-sk-topic']     || '<output_topic>';
            const format    = sc['ifm-sk-format']    || 'json';
            const security  = sc['ifm-sk-security']  || 'PLAINTEXT';
            lines.push(`  ${pad("'connector'")}          = 'kafka',`);
            lines.push(`  ${pad("'topic'")}              = ${q(topic)},`);
            lines.push(`  ${pad("'properties.bootstrap.servers'")} = ${q(bootstrap)},`);
            lines.push(`  ${pad("'format'")}             = '${format}'`);
            if (security && security !== 'PLAINTEXT') {
                lines[lines.length - 1] += ',';
                lines.push(`  ${pad("'properties.security.protocol'")} = '${security}'`);
                const user = sc['ifm-sk-sasl-user'];
                const pass = sc['ifm-sk-sasl-pass'];
                if (user && pass) {
                    lines[lines.length - 1] += ',';
                    lines.push(`  ${pad("'properties.sasl.mechanism'")} = 'PLAIN',`);
                    lines.push(`  ${pad("'properties.sasl.jaas.config'")} = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${user}" password="${pass}";'`);
                }
            }
            const srUrl = sc['ifm-sk-sr-url'];
            if (format === 'avro-confluent' && srUrl) {
                lines[lines.length - 1] += ',';
                lines.push(`  ${pad("'avro-confluent.url'")} = ${q(srUrl)}`);
            }
            break;
        }
        case 'jdbc': {
            const jdbcUrl = sc['ifm-sk-jdbc-url']   || '<jdbc_url>';
            const tbl     = sc['ifm-sk-jdbc-table'] || out;
            const user    = sc['ifm-sk-jdbc-user']  || '';
            const pass    = sc['ifm-sk-jdbc-pass']  || '';
            const driver  = sc['ifm-sk-jdbc-driver']|| 'org.postgresql.Driver';
            lines.push(`  ${pad("'connector'")} = 'jdbc',`);
            lines.push(`  ${pad("'url'")}       = ${q(jdbcUrl)},`);
            lines.push(`  ${pad("'table-name'")}= ${q(tbl)},`);
            lines.push(`  ${pad("'driver'")}    = '${driver}'`);
            if (user) { lines[lines.length-1] += ','; lines.push(`  ${pad("'username'")} = ${q(user)}`); }
            if (pass) { lines[lines.length-1] += ','; lines.push(`  ${pad("'password'")} = ${q(pass)}`); }
            break;
        }
        case 'elasticsearch': {
            const hosts = sc['ifm-sk-es-hosts'] || '<es_hosts>';
            const index = sc['ifm-sk-es-index'] || out.toLowerCase().replace(/_/g,'-');
            const ver   = sc['ifm-sk-es-ver']   || '7';
            const user  = sc['ifm-sk-es-user']  || '';
            const pass  = sc['ifm-sk-es-pass']  || '';
            lines.push(`  ${pad("'connector'")} = 'elasticsearch-${ver}',`);
            lines.push(`  ${pad("'hosts'")}     = ${q(hosts)},`);
            lines.push(`  ${pad("'index'")}     = ${q(index)}`);
            if (user) { lines[lines.length-1] += ','; lines.push(`  ${pad("'username'")} = ${q(user)}`); }
            if (pass) { lines[lines.length-1] += ','; lines.push(`  ${pad("'password'")} = ${q(pass)}`); }
            break;
        }
        case 'filesystem': {
            const path   = sc['ifm-sk-fs-path']   || '<path>';
            const format = sc['ifm-sk-fs-format'] || 'parquet';
            const roll   = sc['ifm-sk-fs-roll']   || '';
            const part   = sc['ifm-sk-fs-part']   || '';
            lines.push(`  ${pad("'connector'")} = 'filesystem',`);
            lines.push(`  ${pad("'path'")}      = ${q(path)},`);
            lines.push(`  ${pad("'format'")}    = '${format}'`);
            if (roll) { lines[lines.length-1] += ','; lines.push(`  ${pad("'sink.rolling-policy.rollover-interval'")} = '${roll}'`); }
            if (part) { lines[lines.length-1] += ','; lines.push(`  ${pad("'partition.fields'")} = ${q(part)}`); }
            break;
        }
        case 'iceberg': {
            const cat  = sc['ifm-sk-ice-cat']     || 'iceberg_catalog';
            const tbl  = sc['ifm-sk-ice-tbl']     || out;
            const type = sc['ifm-sk-ice-cattype'] || 'hive';
            const wh   = sc['ifm-sk-ice-wh']      || '';
            const [db, tblName] = tbl.includes('.') ? tbl.split('.') : ['default', tbl];
            lines.push(`  ${pad("'connector'")}         = 'iceberg',`);
            lines.push(`  ${pad("'catalog-name'")}      = ${q(cat)},`);
            lines.push(`  ${pad("'catalog-database'")}  = ${q(db)},`);
            lines.push(`  ${pad("'catalog-table'")}     = ${q(tblName)},`);
            lines.push(`  ${pad("'catalog-type'")}      = '${type}'`);
            if (wh) { lines[lines.length-1] += ','; lines.push(`  ${pad("'warehouse'")} = ${q(wh)}`); }
            break;
        }
        case 'print':
            lines.push(`  'connector' = 'print'`);
            break;
        case 'blackhole':
            lines.push(`  'connector' = 'blackhole'`);
            break;
        default:
            lines.push(`  'connector' = '${_IFM.sinkType}'`);
    }
    lines.push(`);\n`);

    // ── Model server config block — UDF registration + endpoint/auth details
    if (_IFM.modelServer === 'custom_udf') {
        const udfName  = mc['ifm-mc-udf-name']   || 'predict_score';
        const udfClass = mc['ifm-mc-udf-class']  || 'com.example.flink.udf.ModelUDF';
        const udfKind  = mc['ifm-mc-udf-kind']   || 'Scalar Function';
        lines.push(`-- ── UDF Registration ────────────────────────────────────────────────────────`);
        lines.push(`-- Kind: ${udfKind}   Class: ${udfClass}`);
        lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS ${udfName}`);
        lines.push(`  AS '${udfClass}'`);
        lines.push(`  LANGUAGE JAVA;\n`);
    } else {
        const endpoint = mc['ifm-mc-endpoint'] || '';
        const modelName = mc['ifm-mc-model-name'] || '';
        const modelVer  = mc['ifm-mc-model-version'] || '';

        lines.push(`-- ── Model Server Configuration ─────────────────────────────────────────────`);
        lines.push(`-- Server   : ${srv.name} (${_IFM.modelServer})`);
        if (endpoint)  lines.push(`-- Endpoint : ${endpoint}`);
        if (modelName) lines.push(`-- Model    : ${modelName}${modelVer ? ' @ ' + modelVer : ''}`);

        // Auth details — actual values from user config
        lines.push(`-- Auth     : ${_IFM.authType}`);
        switch (_IFM.authType) {
            case 'bearer':
                lines.push(`--   Header : Authorization: Bearer ${ac['ifm-ac-token'] ? ac['ifm-ac-token'].slice(0,8) + '…[TOKEN]' : '<TOKEN>'}`);
                break;
            case 'api_key': {
                const hdr = ac['ifm-ac-header-name'] || 'Authorization';
                const key = ac['ifm-ac-apikey']      || '';
                lines.push(`--   Header : ${hdr}: ${key ? key.slice(0,8) + '…[KEY]' : '<API_KEY>'}`);
                break;
            }
            case 'x_api_key': {
                const key = ac['ifm-ac-xapikey'] || '';
                lines.push(`--   Header : x-api-key: ${key ? key.slice(0,8) + '…[KEY]' : '<KEY>'}`);
                break;
            }
            case 'basic': {
                const u = ac['ifm-ac-user'] || '<username>';
                lines.push(`--   Basic Auth user: ${u}`);
                break;
            }
            case 'aws_sigv4':
                lines.push(`--   AWS SigV4 region: ${ac['ifm-ac-aws-region'] || mc['ifm-mc-aws-region'] || 'us-east-1'}`);
                lines.push(`--   AWS service    : ${ac['ifm-ac-aws-service'] || 'sagemaker'}`);
                break;
            case 'aws_keys': {
                const kid = ac['ifm-ac-aws-kid'] || '';
                lines.push(`--   AWS Key ID: ${kid ? kid.slice(0,8) + '…' : '<ACCESS_KEY_ID>'}`);
                lines.push(`--   AWS Region: ${ac['ifm-ac-aws-region'] || 'us-east-1'}`);
                break;
            }
            case 'minio_keys': {
                const mk = ac['ifm-ac-minio-key'] || '';
                lines.push(`--   MinIO Access Key: ${mk || '<ACCESS_KEY>'}`);
                break;
            }
            case 'azure_ad':
                lines.push(`--   Azure Tenant  : ${ac['ifm-ac-az-tenant'] || '<tenant_id>'}`);
                lines.push(`--   Azure Client  : ${ac['ifm-ac-az-client'] || '<client_id>'}`);
                break;
            case 'google_sa':
                lines.push(`--   SA Key path   : ${ac['ifm-ac-gsa-path'] || '/etc/gcp/sa-key.json'}`);
                break;
        }

        // Server-specific extra settings as comments
        _ifmServerConfigComment(lines, mc);

        lines.push(`--`);
        lines.push(`-- Async UDF connection settings (embed in your UDF implementation):`);
        lines.push(`--   Timeout  : ${ic.timeoutMs || 5000}ms`);
        lines.push(`--   Retries  : ${ic.retries || 2}`);
        lines.push(`--   On error : ${ic.onError || '-1.0'}`);
        lines.push(`--   Async parallelism : ${ic.asyncParallelism || 4}`);
        lines.push(`-- ─────────────────────────────────────────────────────────────────────────\n`);

        lines.push(`-- Register the async inference UDF (adapt class path to your JAR):`);
        lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS CALL_MODEL_UDF`);
        lines.push(`  AS 'com.yourcompany.flink.udf.${_ifmUdfClassName(srv.name)}AsyncUDF'`);
        lines.push(`  LANGUAGE JAVA;\n`);
    }

    // ── INSERT INTO — the actual streaming inference query
    const udfFn = _IFM.modelServer === 'custom_udf'
        ? (mc['ifm-mc-udf-name'] || 'predict_score')
        : 'CALL_MODEL_UDF';

    const udfArgs = _IFM.modelServer === 'custom_udf'
        ? (mc['ifm-mc-udf-args'] || inputCol)
        : _ifmBuildUdfArgs(srv, mc, inputCol, ic);

    const selectItems = [];
    ptCols.forEach(c => selectItems.push(c));

    if (ic.preProcessExpr?.trim()) {
        selectItems.push(`${ic.preProcessExpr.trim()} AS ${inputCol}_processed`);
    } else if (!ptCols.includes(inputCol)) {
        selectItems.push(inputCol);
    }

    if (ic.postProcessExpr?.trim()) {
        selectItems.push(ic.postProcessExpr.trim());
    } else {
        selectItems.push(`${udfFn}(${udfArgs}) AS ${outputAlias}`);
    }

    lines.push(`-- ── Streaming inference pipeline ───────────────────────────────────────────`);
    lines.push(`-- Score each incoming record from ${src} using ${srv.name}`);
    lines.push(`INSERT INTO ${out}`);
    lines.push(`SELECT`);
    lines.push('  ' + selectItems.join(',\n  '));
    lines.push(`FROM ${src};\n`);

    return lines.join('\n');
}

// Server-specific model config emitted as comments for UDF implementation reference
function _ifmServerConfigComment(lines, mc) {
    const srv = _IFM.modelServer;
    const ep  = mc['ifm-mc-endpoint'] || '';
    switch (srv) {
        case 'mlflow':
        case 'mlflow_serve':
            if (ep) lines.push(`--   MLflow URI  : ${ep}`);
            if (mc['ifm-mc-model-uri']) lines.push(`--   Model URI   : ${mc['ifm-mc-model-uri']}`);
            if (mc['ifm-mc-mlflow-flavour']) lines.push(`--   Flavour     : ${mc['ifm-mc-mlflow-flavour']}`);
            break;
        case 'sagemaker':
            lines.push(`--   Endpoint    : ${mc['ifm-mc-endpoint'] || ''}`);
            lines.push(`--   Region      : ${mc['ifm-mc-aws-region'] || ''}`);
            if (mc['ifm-mc-content-type']) lines.push(`--   Content-Type: ${mc['ifm-mc-content-type']}`);
            break;
        case 'azureml':
            if (ep) lines.push(`--   AzureML URL : ${ep}`);
            if (mc['ifm-mc-deployment']) lines.push(`--   Deployment  : ${mc['ifm-mc-deployment']}`);
            break;
        case 'vertexai':
            lines.push(`--   GCP Project  : ${mc['ifm-mc-gcp-project'] || ''}`);
            lines.push(`--   Region       : ${mc['ifm-mc-gcp-region']  || ''}`);
            lines.push(`--   Endpoint ID  : ${mc['ifm-mc-endpoint']    || ''}`);
            break;
        case 'openai':
        case 'openai_compat':
        case 'mistral':
        case 'together':
            if (ep) lines.push(`--   Base URL    : ${ep}`);
            lines.push(`--   Model       : ${mc['ifm-mc-model-name'] || ''}`);
            if (mc['ifm-mc-max-tokens'])  lines.push(`--   Max tokens  : ${mc['ifm-mc-max-tokens']}`);
            if (mc['ifm-mc-temperature']) lines.push(`--   Temperature : ${mc['ifm-mc-temperature']}`);
            break;
        case 'anthropic':
            lines.push(`--   Model       : ${mc['ifm-mc-model-name'] || 'claude-sonnet-4-6'}`);
            if (mc['ifm-mc-max-tokens'])      lines.push(`--   Max tokens  : ${mc['ifm-mc-max-tokens']}`);
            if (mc['ifm-mc-anthropic-ver'])   lines.push(`--   API version : ${mc['ifm-mc-anthropic-ver']}`);
            break;
        case 'cohere':
            if (ep) lines.push(`--   API URL     : ${ep}`);
            lines.push(`--   Endpoint    : ${mc['ifm-mc-cohere-ep'] || '/v2/classify'}`);
            if (mc['ifm-mc-model-name']) lines.push(`--   Model       : ${mc['ifm-mc-model-name']}`);
            break;
        case 'bedrock':
            lines.push(`--   Region      : ${mc['ifm-mc-aws-region'] || ''}`);
            lines.push(`--   Model ID    : ${mc['ifm-mc-model-name'] || ''}`);
            break;
        case 'triton':
            if (ep) lines.push(`--   Triton URL  : ${ep}`);
            lines.push(`--   Model name  : ${mc['ifm-mc-model-name'] || ''}`);
            lines.push(`--   Version     : ${mc['ifm-mc-model-version'] || '1'}`);
            if (mc['ifm-mc-triton-dtype']) lines.push(`--   DType       : ${mc['ifm-mc-triton-dtype']}`);
            break;
        case 'torchserve':
            if (ep) lines.push(`--   TorchServe  : ${ep}`);
            lines.push(`--   Model name  : ${mc['ifm-mc-model-name'] || ''}`);
            break;
        case 'tfserving':
            if (ep) lines.push(`--   TF Serving  : ${ep}`);
            lines.push(`--   Model name  : ${mc['ifm-mc-model-name'] || ''}`);
            if (mc['ifm-mc-tf-sig']) lines.push(`--   Signature   : ${mc['ifm-mc-tf-sig']}`);
            break;
        case 'minio': {
            if (ep)  lines.push(`--   MinIO URL   : ${ep}`);
            const bkt  = mc['ifm-mc-minio-bucket'] || '';
            const path = mc['ifm-mc-minio-path']   || '';
            const fmt  = mc['ifm-mc-minio-fmt']    || '';
            if (bkt)  lines.push(`--   Bucket      : ${bkt}`);
            if (path) lines.push(`--   Object path : ${path}`);
            if (fmt)  lines.push(`--   Format      : ${fmt}`);
            if (mc['ifm-mc-cache-ttl']) lines.push(`--   Cache TTL   : ${mc['ifm-mc-cache-ttl']}s`);
            break;
        }
        case 'huggingface':
            if (ep) lines.push(`--   HF Endpoint : ${ep}`);
            if (mc['ifm-mc-hf-task']) lines.push(`--   Task        : ${mc['ifm-mc-hf-task']}`);
            break;
        case 'custom_http':
            if (ep) lines.push(`--   URL         : ${ep}`);
            if (mc['ifm-mc-http-method'])   lines.push(`--   Method      : ${mc['ifm-mc-http-method']}`);
            if (mc['ifm-mc-json-path'])     lines.push(`--   JSON path   : ${mc['ifm-mc-json-path']}`);
            if (mc['ifm-mc-req-template'])  lines.push(`--   Body tmpl   : ${mc['ifm-mc-req-template']}`);
            if (mc['ifm-mc-custom-headers']) {
                mc['ifm-mc-custom-headers'].split('\n').filter(Boolean).forEach(h => lines.push(`--   Header      : ${h}`));
            }
            break;
        case 'custom_grpc':
            if (ep) lines.push(`--   gRPC target : ${ep}`);
            if (mc['ifm-mc-grpc-method']) lines.push(`--   Method      : ${mc['ifm-mc-grpc-method']}`);
            if (mc['ifm-mc-grpc-tls'])    lines.push(`--   TLS mode    : ${mc['ifm-mc-grpc-tls']}`);
            break;
        default:
            if (ep) lines.push(`--   Endpoint    : ${ep}`);
    }
}

function _ifmAuthComment(lines, srv, ac, mc) {
    lines.push(`-- Auth: ${_IFM.authType}`);
    switch (_IFM.authType) {
        case 'bearer':    lines.push(`--   Authorization: Bearer <TOKEN>`); break;
        case 'api_key':   lines.push(`--   ${ac['ifm-ac-header-name']||'Authorization'}: Bearer <API_KEY>`); break;
        case 'x_api_key': lines.push(`--   x-api-key: <API_KEY>`); break;
        case 'aws_sigv4': lines.push(`--   AWS SigV4 (region: ${ac['ifm-ac-aws-region']||mc['ifm-mc-aws-region']||'us-east-1'})`); break;
        case 'aws_keys':  lines.push(`--   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars`); break;
        case 'minio_keys':lines.push(`--   MinIO AccessKey / SecretKey`); break;
        case 'azure_ad':  lines.push(`--   Azure AD OAuth2 (tenant: ${ac['ifm-ac-az-tenant']||'<tenant>'})`); break;
        case 'google_sa': lines.push(`--   Google SA JSON: ${ac['ifm-ac-gsa-path']||'/etc/gcp/sa-key.json'}`); break;
    }
}

function _ifmBuildUdfArgs(srv, mc, inputCol, ic) {
    const selCols = _IFM.selectedInputCols.length ? _IFM.selectedInputCols.join(', ') : inputCol;
    switch (srv.id) {
        case 'openai':
        case 'anthropic':
        case 'cohere':
        case 'mistral':
        case 'together':
        case 'openai_compat': {
            const model   = mc['ifm-mc-model-name'] || 'model';
            const prompt  = mc['ifm-mc-system-prompt'] || '';
            const tokens  = mc['ifm-mc-max-tokens'] || '64';
            return `CAST(${inputCol} AS STRING), '${model}', '${prompt.replace(/'/g,"\\'")}', ${tokens}`;
        }
        case 'bedrock': {
            const region = mc['ifm-mc-aws-region'] || 'us-east-1';
            const model  = mc['ifm-mc-model-name'] || 'anthropic.claude-sonnet-4-6';
            return `CAST(${inputCol} AS STRING), '${model}', '${region}'`;
        }
        case 'sagemaker': {
            const ep = mc['ifm-mc-endpoint'] || '<endpoint>';
            return `${selCols}, '${ep}'`;
        }
        case 'triton': {
            return `ARRAY[${selCols}], '${mc['ifm-mc-model-name']||'model'}', '${mc['ifm-mc-model-version']||'1'}'`;
        }
        default:
            return selCols;
    }
}

function _ifmUdfClassName(name) {
    return (name || 'Model').replace(/[^a-zA-Z0-9]/g, '');
}

function _ifmInsertSql() {
    const sql = _IFM.generatedSql || _ifmGenerateSql();
    if (!sql) { if (typeof toast === 'function') toast('Build pipeline first', 'warn'); return; }
    const ed = document.getElementById('sql-editor');
    if (!ed) return;
    const prefix = ed.value.trim() ? '\n\n' : '';
    ed.value += prefix + sql + '\n';
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('ifm-modal');
    if (typeof toast === 'function') toast('Inference pipeline SQL inserted into editor', 'ok');
}

// ─────────────────────────────────────────────────────────────────────────────
// History
// ─────────────────────────────────────────────────────────────────────────────
function _ifmShowHistory() {
    const old = document.getElementById('ifm-hist-modal');
    if (old) { old.remove(); return; }
    if (!_IFM.history.length) { if (typeof toast === 'function') toast('No history yet', 'info'); return; }

    const m = document.createElement('div');
    m.id = 'ifm-hist-modal';
    m.style.cssText = 'position:fixed;z-index:10003;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--bg2);border:1px solid var(--border2);border-radius:8px;box-shadow:0 12px 48px rgba(0,0,0,0.7);width:620px;max-height:80vh;display:flex;flex-direction:column;overflow:hidden;';
    m.innerHTML = `
<div style="padding:12px 16px;background:var(--bg1);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-shrink:0;">
  <span style="font-size:13px;font-weight:700;color:var(--text0);">🕓 Inference Manager History</span>
  <span style="font-size:10px;color:var(--text3);font-family:var(--mono);">${_IFM.history.length} entries</span>
  <button onclick="document.getElementById('ifm-hist-modal').remove()" style="margin-left:auto;background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;">×</button>
</div>
<div style="flex:1;overflow-y:auto;padding:12px;">
  ${_IFM.history.map((h, i) => {
        const srv = IFM_SERVERS.find(s => s.id === h.modelServer);
        return `
  <div class="ifm-hist-item" onclick="_ifmLoadFromHistory(${i})">
    <div style="flex:1;min-width:0;">
      <div style="font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);">
        ${srv?.icon||'🔬'} ${h.sourceTable} → ${h.outputTable}
      </div>
      <div style="font-size:10px;color:var(--text3);margin-top:2px;display:flex;gap:10px;flex-wrap:wrap;">
        <span>${srv?.name||h.modelServer}</span>
        <span style="color:var(--blue);">→ ${h.outputAlias}</span>
        <span style="margin-left:auto;">${new Date(h.ts).toLocaleString()}</span>
      </div>
    </div>
    <div style="display:flex;gap:5px;flex-shrink:0;">
      <button onclick="event.stopPropagation();_ifmLoadFromHistory(${i})"
        style="font-size:10px;padding:3px 9px;border-radius:3px;background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.3);color:var(--blue,#4fa3e0);cursor:pointer;font-family:var(--mono);">Insert SQL</button>
      <button onclick="event.stopPropagation();_IFM.history.splice(${i},1);try{localStorage.setItem('strlabstudio_ifm_history',JSON.stringify(_IFM.history));}catch(_){};_ifmUpdateHistCount();document.getElementById('ifm-hist-modal').remove();"
        style="font-size:10px;padding:3px 7px;border-radius:3px;background:rgba(255,77,109,0.07);border:1px solid rgba(255,77,109,0.3);color:var(--red);cursor:pointer;">×</button>
    </div>
  </div>`;}).join('')}
</div>
<div style="padding:10px 16px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end;background:var(--bg1);flex-shrink:0;">
  <button onclick="if(confirm('Clear all inference history?')){_IFM.history=[];try{localStorage.removeItem('strlabstudio_ifm_history');}catch(_){}; _ifmUpdateHistCount();document.getElementById('ifm-hist-modal').remove();}"
    style="font-size:11px;padding:5px 12px;border-radius:3px;background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.3);color:var(--red);cursor:pointer;font-family:var(--mono);">Clear All</button>
  <button onclick="document.getElementById('ifm-hist-modal').remove()"
    style="font-size:11px;padding:5px 12px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Close</button>
</div>`;
    document.body.appendChild(m);
}

function _ifmLoadFromHistory(i) {
    const h = _IFM.history[i];
    if (!h?.sql) { if (typeof toast === 'function') toast('No SQL in this entry', 'warn'); return; }
    const ed = document.getElementById('sql-editor');
    if (!ed) return;
    const prefix = ed.value.trim() ? '\n\n' : '';
    ed.value += prefix + h.sql + '\n';
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    const hm = document.getElementById('ifm-hist-modal');
    if (hm) hm.remove();
    closeModal('ifm-modal');
    if (typeof toast === 'function') toast(`Inference SQL loaded: ${h.sourceTable} → ${h.outputTable}`, 'ok');
}

function _escIfm(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}