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
 * ICON UPDATE:
 *  - Brand icons now served from https://cdn.simpleicons.org/<slug>/<hex>
 *  - Icons without a Simple Icons equivalent keep their original emoji
 *  - Helper _ifmSimpleIcon(slug, hex, size) builds a consistent <img> tag
 *  - Used in IFM_SERVERS catalogue (icon field) AND in the canvas SVG
 * ─────────────────────────────────────────────────────────────────────────────
 */

// ── Simple Icons helper ───────────────────────────────────────────────────────
// Returns an <img> tag from cdn.simpleicons.org with optional colour override.
// slug — the Simple Icons slug (e.g. 'mlflow')
// hex  — colour WITHOUT the # (e.g. 'ffffff')  — omit for brand default colour
// size — px (default 20)
function _ifmSimpleIcon(slug, hex, size) {
    size = size || 20;
    const src = hex
        ? `https://cdn.simpleicons.org/${slug}/${hex}`
        : `https://cdn.simpleicons.org/${slug}`;
    return `<img src="${src}" width="${size}" height="${size}"
    style="display:inline-block;vertical-align:middle;flex-shrink:0;" />`;
}

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
     inferenceMethod: 'async_udf',
     inferenceMethodConfig: {},
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
.ifm-server-card .ifm-sc-icon{font-size:20px;margin-bottom:5px;display:flex;align-items:center;min-height:24px;}
.ifm-server-card .ifm-sc-icon img{width:22px;height:22px;}
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

// ── Server catalogue — icons updated to Simple Icons CDN where available ───────
// Icon strategy:
//   ✅ Simple Icons CDN  — mlflow, sagemaker, azureml, vertexai, databricks,
//                          nvidia (triton), pytorch (torchserve), tensorflow,
//                          minio, huggingface, openai, anthropic, cohere,
//                          mistral, amazonaws (bedrock)
//   ✅ Kept as emoji     — bentoml, ray, seldon, kserve, together, openai_compat,
//                          custom_http, custom_grpc, custom_udf (no SI equivalent)
// ─────────────────────────────────────────────────────────────────────────────
const IFM_SERVERS = [
    // Managed MLOps Platforms
    {
        id:'mlflow', group:'MLOps Platforms',
        icon: _ifmSimpleIcon('mlflow', '0194E2', 22),
        name:'MLflow', desc:'MLflow Model Registry & Serving',
        authTypes:['bearer','basic','none']
    },
    {
        id:'sagemaker', group:'MLOps Platforms',
        icon: `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="3" fill="#FF9900"/><path d="M12 4L5 8v8l7 4 7-4V8z" fill="none" stroke="white" stroke-width="1.5"/><path d="M12 4v16M5 8l7 4 7-4" stroke="white" stroke-width="1.5"/></svg>`,
        name:'AWS SageMaker', desc:'SageMaker Endpoints (IAM/AK)',
        authTypes:['aws_sigv4','aws_keys']
    },
    {
        id:'azureml', group:'MLOps Platforms',
        icon: `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="3" fill="#0078D4"/><path d="M13.5 4H8L4 13h5l-2 7 11-9.5h-6z" fill="white"/></svg>`,
        name:'Azure ML', desc:'Azure ML Online Endpoints',
        authTypes:['azure_ad','api_key']
    },
    {
        id:'vertexai', group:'MLOps Platforms',
        icon: _ifmSimpleIcon('googlecloud', '4285F4', 22),
        name:'Google Vertex AI', desc:'Vertex AI Endpoints',
        authTypes:['google_sa','bearer']
    },
    {
        id:'databricks', group:'MLOps Platforms',
        icon: _ifmSimpleIcon('databricks', 'FF3621', 22),
        name:'Databricks', desc:'Databricks Model Serving',
        authTypes:['bearer']
    },
    // Open-Source Serving
    {
        id:'mlflow_serve', group:'Open-Source Serving',
        icon: _ifmSimpleIcon('mlflow', '0194E2', 22),
        name:'MLflow pyfunc', desc:'mlflow models serve REST API',
        authTypes:['none','bearer']
    },
    {
        id:'bentoml', group:'Open-Source Serving',
        // No Simple Icons equivalent — keep emoji
        icon:'<span style="font-size:20px;line-height:1;">🍱</span>',
        name:'BentoML', desc:'BentoML Serve REST / gRPC',
        authTypes:['api_key','none']
    },
    {
        id:'triton', group:'Open-Source Serving',
        icon: _ifmSimpleIcon('nvidia', '76B900', 22),
        name:'Triton Inference', desc:'NVIDIA Triton (HTTP/gRPC)',
        authTypes:['none','bearer']
    },
    {
        id:'torchserve', group:'Open-Source Serving',
        icon: _ifmSimpleIcon('pytorch', 'EE4C2C', 22),
        name:'TorchServe', desc:'PyTorch TorchServe Management API',
        authTypes:['none','bearer']
    },
    {
        id:'tfserving', group:'Open-Source Serving',
        icon: _ifmSimpleIcon('tensorflow', 'FF6F00', 22),
        name:'TF Serving', desc:'TensorFlow Serving REST API',
        authTypes:['none','bearer']
    },
    {
        id:'ray', group:'Open-Source Serving',
        icon:`<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#f5a623" stroke-width="1.8"><circle cx="12" cy="12" r="3"/><line x1="12" y1="2" x2="12" y2="6"/><line x1="12" y1="18" x2="12" y2="22"/><line x1="2" y1="12" x2="6" y2="12"/><line x1="18" y1="12" x2="22" y2="12"/><line x1="4.9" y1="4.9" x2="7.8" y2="7.8"/><line x1="16.2" y1="16.2" x2="19.1" y2="19.1"/><line x1="19.1" y1="4.9" x2="16.2" y2="7.8"/><line x1="7.8" y1="16.2" x2="4.9" y2="19.1"/></svg>`,
        name:'Ray Serve',
        authTypes:['none','bearer']
    },
    {
        id:'seldon', group:'Open-Source Serving',
        icon:`<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#4e9de8" stroke-width="1.8"><circle cx="12" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/><line x1="12" y1="7" x2="5" y2="17"/><line x1="12" y1="7" x2="19" y2="17"/><line x1="7" y1="19" x2="17" y2="19"/></svg>`,
        name:'Seldon Core',
        authTypes:['bearer','none']
    },
    {
        id:'kserve', group:'Open-Source Serving',
        icon:`<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#57c764" stroke-width="1.8"><polygon points="12 2 22 12 12 22 2 12"/><polygon points="12 7 17 12 12 17 7 12"/><circle cx="12" cy="12" r="2" fill="#57c764"/></svg>`,
        name:'KServe',
        authTypes:['bearer','none']
    },
    // Model Registries / Artifact Stores
    {
        id:'minio', group:'Artifact Stores',
        icon: _ifmSimpleIcon('minio', 'C72E49', 22),
        name:'MinIO / S3', desc:'Load model artifact from object store',
        authTypes:['minio_keys','aws_keys']
    },
    // Hosted APIs
    {
        id:'huggingface', group:'Hosted APIs',
        icon: _ifmSimpleIcon('huggingface', 'FF9D00', 22),
        name:'HuggingFace Hub', desc:'Inference API / Endpoints',
        authTypes:['bearer']
    },
    {
        id:'openai', group:'Hosted APIs',
        icon: `<svg width="22" height="22" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="3" fill="#412991"/><path d="M20.9 9.5a5.8 5.8 0 0 0-.4-4.7 6 6 0 0 0-6.4-2.8 5.8 5.8 0 0 0-4.4-2A6 6 0 0 0 4 3.8a5.8 5.8 0 0 0-3.9 2.8 6 6 0 0 0 .7 7 5.8 5.8 0 0 0 .4 4.8 6 6 0 0 0 6.4 2.8 5.8 5.8 0 0 0 4.4 2 6 6 0 0 0 5.7-4.1 5.8 5.8 0 0 0 3.9-2.8 6 6 0 0 0-.7-6.8zM14 20a4.5 4.5 0 0 1-2.8-1l.1-.1 4.6-2.7a.8.8 0 0 0 .4-.7v-6.5l2 1.1v5.3A4.5 4.5 0 0 1 14 20zM3.5 16.6a4.5 4.5 0 0 1-.5-3l.1.1 4.6 2.7a.8.8 0 0 0 .8 0l5.6-3.2v2.2l-4.7 2.7a4.5 4.5 0 0 1-5.9-1.5zM2.6 8a4.5 4.5 0 0 1 2.3-2L5 6v5.5a.8.8 0 0 0 .4.7l5.6 3.2-2 1.1-4.6-2.7A4.5 4.5 0 0 1 2.6 8zM17.6 12l-5.6-3.2L14 7.7l4.6 2.6a4.5 4.5 0 0 1-.7 8.1V13a.8.8 0 0 0-.3-.7v-.3zm2-3-.1-.1-4.6-2.7a.8.8 0 0 0-.8 0L8.5 9.4V7.2l4.7-2.7a4.5 4.5 0 0 1 6.7 4.6v.1zM7.4 13L5.5 12l.1-5.3a4.5 4.5 0 0 1 7.3-3.4l-.1.1-4.6 2.7a.8.8 0 0 0-.4.7L7.4 13zm1 -2.2 2.5-1.4 2.5 1.4v2.8l-2.5 1.5-2.5-1.5V10.8z" fill="white"/></svg>`,
        name:'OpenAI',
        authTypes:['api_key']
    },
    {
        id:'anthropic', group:'Hosted APIs',
        icon: _ifmSimpleIcon('anthropic', 'D4A27F', 22),
        name:'Anthropic Claude', desc:'Claude 3.5 / Claude 4 series',
        authTypes:['api_key','x_api_key']
    },
    {
        id:'cohere', group:'Hosted APIs',
        icon: `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="3" fill="#39594D"/><circle cx="12" cy="12" r="7" stroke="white" stroke-width="1.8" fill="none"/><circle cx="12" cy="12" r="3.5" fill="white"/><circle cx="17" cy="7" r="2" fill="#84C9A8"/></svg>`,
        name:'Cohere', desc:'Command R+, Embed, Classify',
        authTypes:['api_key','bearer']
    },
    {
        id:'mistral', group:'Hosted APIs',
        icon: `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="4" fill="#FF7000"/><path d="M7 17V7h3v4h4V7h3v10h-3v-4h-4v4z" fill="white"/></svg>`,
        name:'Mistral AI', desc:'Mistral, Mixtral endpoints',
        authTypes:['api_key']
    },
    {
        id:'together', group:'Hosted APIs',
        // No Simple Icons equivalent — keep emoji
        icon:'<span style="font-size:20px;line-height:1;">🌐</span>',
        name:'Together AI', desc:'Open models via Together API',
        authTypes:['api_key']
    },
    {
        id:'bedrock', group:'Hosted APIs',
        icon: `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="24" height="24" rx="3" fill="#FF9900"/><rect x="4" y="14" width="16" height="4" rx="1" fill="white"/><rect x="4" y="9" width="12" height="4" rx="1" fill="white" opacity="0.75"/><rect x="4" y="4" width="8" height="4" rx="1" fill="white" opacity="0.5"/></svg>`,
        name:'AWS Bedrock',
        authTypes:['aws_sigv4','aws_keys']
    },
    // Custom / Bespoke
    {
        id:'openai_compat', group:'Custom / Bespoke',
        // vLLM/Ollama/LocalAI — no single SI icon; keep emoji
        icon:'<span style="font-size:20px;line-height:1;">🔌</span>',
        name:'OpenAI-compatible', desc:'vLLM, LocalAI, Ollama, etc.',
        authTypes:['api_key','bearer','none']
    },
    {
        id:'custom_http', group:'Custom / Bespoke',
        icon:'<span style="font-size:20px;line-height:1;">⚙</span>',
        name:'Custom HTTP REST', desc:'Any REST endpoint (fully configurable)',
        authTypes:['bearer','api_key','basic','header','none']
    },
    {
        id:'custom_grpc', group:'Custom / Bespoke',
        icon:'<span style="font-size:20px;line-height:1;">⚡</span>',
        name:'Custom gRPC', desc:'gRPC streaming / unary inference',
        authTypes:['bearer','tls_cert','none']
    },
    {
        id:'custom_udf', group:'Custom / Bespoke',
        icon:'<span style="font-size:20px;line-height:1;">⨍</span>',
        name:'Flink UDF / JAR', desc:'Registered Flink scalar/async UDF',
        authTypes:['none']
    },
];

const IFM_STEPS = [
    { label:'Source Table',      icon:'⬡' },
    { label:'Model Server',      icon:'🔬' },
    { label:'Model Config',      icon:'⚙' },
    { label:'Inference Method',  icon:'◈' },
    { label:'Auth & Creds',      icon:'🔑' },
    { label:'Inference I/O',     icon:'▶' },
    { label:'Output Sink',       icon:'⤵' },
    { label:'Review & SQL',      icon:'⟨/⟩' },
];

const IFM_INFERENCE_METHODS = [
    {
        id: 'async_udf',
        label: 'Async Scalar UDF',
        icon: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#4fa3e0" stroke-width="1.8"><path d="M4 17l6-6-6-6"/><line x1="12" y1="19" x2="20" y2="19"/></svg>`,
        badge: 'RECOMMENDED',
        badgeColor: '#4fa3e0',
        desc: 'Wraps model inference in a Flink RichAsyncFunction JAR. Fully non-blocking, supports retries and timeouts. Most flexible option for any HTTP/gRPC endpoint.',
        sqlPattern: 'CALL_MODEL_UDF(features) AS prediction',
        pros: ['Non-blocking I/O', 'Configurable parallelism & timeout', 'Works with any endpoint', 'Retry logic built-in'],
        cons: ['Requires a JAR deployment', 'Custom Java/Scala/Python implementation needed'],
        generates: 'CREATE TEMPORARY FUNCTION + INSERT INTO ... SELECT udf(...)',
    },
    {
        id: 'flink_ml',
        label: 'Flink ML Predict',
        icon: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#57c764" stroke-width="1.8"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>`,
        badge: 'NATIVE',
        badgeColor: '#57c764',
        desc: 'Uses Flink ML\'s built-in ML_PREDICT table function (Flink 2.1). Loads ONNX, PMML or custom model files from a path and applies them inline — no external server needed.',
        sqlPattern: 'SELECT * FROM ML_PREDICT(TABLE source, MODEL model_name)',
        pros: ['Zero external dependencies', 'Model loaded once at startup', 'Lowest latency (in-process)', 'Official Flink API'],
        cons: ['Flink 2.1 required', 'Supports ONNX/PMML only (no LLM APIs)', 'Model must be accessible on all TaskManagers'],
        generates: 'CREATE MODEL + SELECT * FROM ML_PREDICT(...)',
    },
    {
        id: 'otter_streams',
        label: 'Otter-Streams Connector',
        icon: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#00d4aa" stroke-width="1.8"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v4c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M4 10v4c0 1.7 3.6 3 8 3s8-1.3 8-3v-4"/><path d="M4 14v4c0 1.7 3.6 3 8 3s8-1.3 8-3v-4"/></svg>`,
        badge: 'CUSTOM',
        badgeColor: '#00d4aa',
        desc: 'Uses a registered Otter-Streams ML inference function. Configure your custom connector endpoint and function name — generates a CALL statement using your registered connector function.',
        sqlPattern: 'CALL_OTTER_ML(features, endpoint, model) AS prediction',
        pros: ['Integrates with Otter-Streams platform', 'Managed credentials & retries', 'Works with your existing connectors', 'No JAR required if already registered'],
        cons: ['Requires Otter-Streams runtime', 'Function must be pre-registered'],
        generates: 'CREATE TEMPORARY FUNCTION (Otter connector) + INSERT INTO ...',
    },
    {
        id: 'udf_view',
        label: 'UDF View Pattern',
        icon: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#b080e0" stroke-width="1.8"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M9 21V9"/></svg>`,
        badge: 'ADVANCED',
        badgeColor: '#b080e0',
        desc: 'Creates a reusable Flink TEMPORARY VIEW that encapsulates the inference logic. Downstream queries just SELECT from the view — clean separation of inference from business logic.',
        sqlPattern: 'CREATE VIEW scored_view AS SELECT ..., udf(features) AS prediction FROM source',
        pros: ['Reusable across multiple queries', 'Clean SQL abstraction', 'Easy to test and iterate', 'Composable with other views'],
        cons: ['Still needs an underlying UDF', 'View must be recreated each session'],
        generates: 'CREATE TEMPORARY VIEW + underlying UDF + INSERT INTO ...',
    },
    {
        id: 'custom_function',
        label: 'Custom SQL Function',
        icon: `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#f5a623" stroke-width="1.8"><path d="M4 17l6-6-6-6"/><path d="M12 19h8"/><circle cx="18" cy="19" r="2"/></svg>`,
        badge: 'MANUAL',
        badgeColor: '#f5a623',
        desc: 'You write the full CREATE FUNCTION statement yourself. Use this when you have a pre-registered function, a Flink scalar function, or a platform-specific function not covered above.',
        sqlPattern: 'YOUR_FUNCTION(features) AS prediction',
        pros: ['Complete control', 'Works with any registered function', 'No assumptions about runtime'],
        cons: ['Requires manual SQL authoring', 'No validation of function signature'],
        generates: 'INSERT INTO ... SELECT your_function(...)',
    },
];

// ── Inference Method Step Renderer ────────────────────────────────────────────
function _ifmRenderStepMethod() {
    const body = document.getElementById('ifm-body');
    const current = _IFM.inferenceMethod || 'async_udf';
    const mc = _IFM.inferenceMethodConfig || {};

    body.innerHTML = `
<div class="ifm-info-box">
  Select how Flink will call your model at inference time. Each method generates different SQL patterns.
  <strong>Async Scalar UDF</strong> is the most flexible; <strong>Flink ML Predict</strong> is the most native for ONNX/PMML models.
</div>
 
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px;margin-bottom:18px;">
  ${IFM_INFERENCE_METHODS.map(m => `
    <div id="ifm-mth-card-${m.id}"
      onclick="_ifmSelectMethod('${m.id}')"
      style="padding:12px 14px;border:2px solid ${current===m.id?'var(--blue,#4fa3e0)':'var(--border2)'};
        border-radius:6px;cursor:pointer;background:${current===m.id?'rgba(79,163,224,0.1)':'var(--bg3)'};
        transition:all 0.12s;user-select:none;position:relative;">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        <span style="display:flex;align-items:center;flex-shrink:0;">${m.icon}</span>
        <span style="font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);">${m.label}</span>
      </div>
      <div style="position:absolute;top:8px;right:8px;font-size:8px;font-weight:700;padding:1px 6px;
        border-radius:8px;background:${m.badgeColor}22;color:${m.badgeColor};border:1px solid ${m.badgeColor}44;">
        ${m.badge}
      </div>
      <div style="font-size:10px;color:var(--text3);line-height:1.5;margin-top:2px;">${m.desc.slice(0,90)}…</div>
      <div style="margin-top:8px;font-size:9px;font-family:var(--mono);color:${m.badgeColor};
        background:${m.badgeColor}11;padding:3px 6px;border-radius:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
        ${m.sqlPattern}
      </div>
    </div>`).join('')}
</div>
 
<!-- Detail panel for selected method -->
<div id="ifm-mth-detail" style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;overflow:hidden;">
  <!-- Rendered by _ifmRenderMethodDetail() -->
</div>`;

    _ifmRenderMethodDetail(current);
}

function _ifmSelectMethod(id) {
    _IFM.inferenceMethod = id;
    document.querySelectorAll('[id^="ifm-mth-card-"]').forEach(el => {
        const isSelected = el.id === 'ifm-mth-card-' + id;
        const m = IFM_INFERENCE_METHODS.find(m => 'ifm-mth-card-' + m.id === el.id);
        if (!m) return;
        el.style.borderColor = isSelected ? 'var(--blue,#4fa3e0)' : 'var(--border2)';
        el.style.background  = isSelected ? 'rgba(79,163,224,0.1)' : 'var(--bg3)';
    });
    _ifmRenderMethodDetail(id);
}

function _ifmRenderMethodDetail(id) {
    const m = IFM_INFERENCE_METHODS.find(x => x.id === id);
    if (!m) return;
    const wrap = document.getElementById('ifm-mth-detail');
    if (!wrap) return;
    const mc = _IFM.inferenceMethodConfig || {};

    // Configuration fields vary per method
    const configForms = {
        async_udf: `
<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
  <div>
    <label class="ifm-field-label">UDF Class Name <span class="ifm-required">*</span></label>
    <input id="ifm-mth-udf-class" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="com.yourcompany.flink.udf.ModelAsyncUDF"
      value="${_escIfm(mc['ifm-mth-udf-class']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Registered Function Name</label>
    <input id="ifm-mth-udf-name" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="CALL_MODEL_UDF"
      value="${_escIfm(mc['ifm-mth-udf-name']||'CALL_MODEL_UDF')}" />
  </div>
  <div>
    <label class="ifm-field-label">JAR Path (for reference)</label>
    <input id="ifm-mth-udf-jar" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="/opt/flink/lib/model-udf.jar"
      value="${_escIfm(mc['ifm-mth-udf-jar']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Language</label>
    <select id="ifm-mth-udf-lang" class="field-input" style="font-size:11px;">
      ${['JAVA','SCALA','PYTHON'].map(l=>`<option value="${l}" ${(mc['ifm-mth-udf-lang']||'JAVA')===l?'selected':''}>${l}</option>`).join('')}
    </select>
  </div>
</div>`,

        flink_ml: `
<div class="ifm-info-box" style="margin-bottom:10px;">
  Flink ML <code>ML_PREDICT</code> is available from Flink 1.16+. The model file must be reachable
  from all TaskManagers (e.g. on a shared filesystem or mounted volume).
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
  <div>
    <label class="ifm-field-label">Model Name (CREATE MODEL name) <span class="ifm-required">*</span></label>
    <input id="ifm-mth-flinkml-name" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="fraud_model"
      value="${_escIfm(mc['ifm-mth-flinkml-name']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Model Format</label>
    <select id="ifm-mth-flinkml-fmt" class="field-input" style="font-size:11px;">
      ${['ONNX','PMML','TensorFlow SavedModel','XGBoost','LightGBM','Scikit-learn (pickle)'].map(f=>`<option value="${f}" ${(mc['ifm-mth-flinkml-fmt']||'ONNX')===f?'selected':''}>${f}</option>`).join('')}
    </select>
  </div>
  <div>
    <label class="ifm-field-label">Model File Path <span class="ifm-required">*</span></label>
    <input id="ifm-mth-flinkml-path" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="/opt/flink/models/fraud_model.onnx"
      value="${_escIfm(mc['ifm-mth-flinkml-path']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Input Tensor / Column Mapping</label>
    <input id="ifm-mth-flinkml-input" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="price, quantity, timestamp"
      value="${_escIfm(mc['ifm-mth-flinkml-input']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Output Column Name</label>
    <input id="ifm-mth-flinkml-output" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="prediction"
      value="${_escIfm(mc['ifm-mth-flinkml-output']||'prediction')}" />
  </div>
  <div>
    <label class="ifm-field-label">Flink Version</label>
    <select id="ifm-mth-flinkml-ver" class="field-input" style="font-size:11px;">
      ${['1.16','1.17','1.18','1.19','2.0'].map(v=>`<option value="${v}" ${(mc['ifm-mth-flinkml-ver']||'1.18')===v?'selected':''}>${v}</option>`).join('')}
    </select>
  </div>
</div>`,

        otter_streams: `
<div class="ifm-info-box" style="margin-bottom:10px;">
  Otter-Streams ML inference connector. Register your function once and call it by name.
  Credentials and connection settings are managed by the Otter-Streams runtime.
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
  <div>
    <label class="ifm-field-label">Otter Function Name <span class="ifm-required">*</span></label>
    <input id="ifm-mth-otter-fn" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="OTTER_ML_PREDICT"
      value="${_escIfm(mc['ifm-mth-otter-fn']||'OTTER_ML_PREDICT')}" />
  </div>
  <div>
    <label class="ifm-field-label">Connector Endpoint Override (optional)</label>
    <input id="ifm-mth-otter-ep" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="http://triton:8000 (leave blank to use model config)"
      value="${_escIfm(mc['ifm-mth-otter-ep']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Model Version Tag</label>
    <input id="ifm-mth-otter-ver" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="v1 / latest / champion"
      value="${_escIfm(mc['ifm-mth-otter-ver']||'latest')}" />
  </div>
  <div>
    <label class="ifm-field-label">Registered in Session?</label>
    <select id="ifm-mth-otter-reg" class="field-input" style="font-size:11px;">
      <option value="yes" ${(mc['ifm-mth-otter-reg']||'yes')==='yes'?'selected':''}>Yes — already registered</option>
      <option value="no"  ${mc['ifm-mth-otter-reg']==='no'?'selected':''}>No — include CREATE FUNCTION</option>
    </select>
  </div>
  <div>
    <label class="ifm-field-label">Otter Connector JAR (if registering)</label>
    <input id="ifm-mth-otter-jar" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="/opt/flink/lib/otter-streams-connector.jar"
      value="${_escIfm(mc['ifm-mth-otter-jar']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Otter Class Name (if registering)</label>
    <input id="ifm-mth-otter-class" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="com.otterstreams.flink.udf.OtterMLPredict"
      value="${_escIfm(mc['ifm-mth-otter-class']||'')}" />
  </div>
</div>`,

        udf_view: `
<div class="ifm-info-box" style="margin-bottom:10px;">
  Creates a <code>CREATE TEMPORARY VIEW</code> that wraps the inference logic.
  Downstream queries simply <code>SELECT * FROM scored_view</code>.
  The underlying UDF is still needed — this is a SQL organisation pattern.
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
  <div>
    <label class="ifm-field-label">View Name <span class="ifm-required">*</span></label>
    <input id="ifm-mth-view-name" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="scored_transactions_v"
      value="${_escIfm(mc['ifm-mth-view-name']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Underlying UDF Function Name</label>
    <input id="ifm-mth-view-udf" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="CALL_MODEL_UDF"
      value="${_escIfm(mc['ifm-mth-view-udf']||'CALL_MODEL_UDF')}" />
  </div>
  <div>
    <label class="ifm-field-label">UDF Class Name</label>
    <input id="ifm-mth-view-class" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="com.yourcompany.flink.udf.ModelAsyncUDF"
      value="${_escIfm(mc['ifm-mth-view-class']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Also generate INSERT INTO sink?</label>
    <select id="ifm-mth-view-insert" class="field-input" style="font-size:11px;">
      <option value="yes" ${(mc['ifm-mth-view-insert']||'yes')==='yes'?'selected':''}>Yes — INSERT INTO sink FROM view</option>
      <option value="no"  ${mc['ifm-mth-view-insert']==='no'?'selected':''}>No — view only</option>
    </select>
  </div>
</div>`,

        custom_function: `
<div class="ifm-info-box" style="margin-bottom:10px;">
  Specify the function name to call. Write any additional setup SQL in the custom preamble below.
  This gives you full control over the generated INSERT statement.
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
  <div>
    <label class="ifm-field-label">Function Name <span class="ifm-required">*</span></label>
    <input id="ifm-mth-custom-fn" class="field-input" type="text"
      style="font-size:11px;font-family:var(--mono);"
      placeholder="MY_PREDICT_FUNCTION"
      value="${_escIfm(mc['ifm-mth-custom-fn']||'')}" />
  </div>
  <div>
    <label class="ifm-field-label">Function already registered?</label>
    <select id="ifm-mth-custom-reg" class="field-input" style="font-size:11px;">
      <option value="yes" ${(mc['ifm-mth-custom-reg']||'yes')==='yes'?'selected':''}>Yes — skip CREATE FUNCTION</option>
      <option value="no"  ${mc['ifm-mth-custom-reg']==='no'?'selected':''}>No — include CREATE FUNCTION stub</option>
    </select>
  </div>
</div>
<div style="margin-top:10px;">
  <label class="ifm-field-label">Custom preamble SQL (executed before INSERT — optional)</label>
  <textarea id="ifm-mth-custom-preamble" class="field-input" rows="4"
    style="font-size:11px;font-family:var(--mono);resize:vertical;"
    placeholder="-- e.g. CREATE TEMPORARY FUNCTION IF NOT EXISTS MY_PREDICT_FUNCTION&#10;--   AS 'com.example.MyUDF' LANGUAGE JAVA;">${_escIfm(mc['ifm-mth-custom-preamble']||'')}</textarea>
</div>`,
    };

    wrap.innerHTML = `
<div style="padding:12px 14px;background:var(--bg1);border-bottom:1px solid var(--border);
  display:flex;align-items:center;gap:10px;">
  <span style="display:flex;align-items:center;">${m.icon}</span>
  <div>
    <div style="font-size:12px;font-weight:700;color:var(--text0);">${m.label}</div>
    <div style="font-size:10px;color:var(--text3);margin-top:1px;">${m.desc}</div>
  </div>
  <span style="margin-left:auto;font-size:9px;font-weight:700;padding:2px 8px;border-radius:10px;
    background:${m.badgeColor}22;color:${m.badgeColor};border:1px solid ${m.badgeColor}44;">${m.badge}</span>
</div>
 
<div style="display:grid;grid-template-columns:1fr 1fr;gap:0;border-bottom:1px solid var(--border);">
  <div style="padding:10px 14px;border-right:1px solid var(--border);">
    <div style="font-size:9px;font-weight:700;color:var(--green);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">✓ Pros</div>
    ${m.pros.map(p=>`<div style="font-size:10px;color:var(--text1);margin-bottom:3px;display:flex;gap:5px;"><span style="color:var(--green);flex-shrink:0;">+</span>${p}</div>`).join('')}
  </div>
  <div style="padding:10px 14px;">
    <div style="font-size:9px;font-weight:700;color:var(--yellow,#f5a623);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">⚠ Cons</div>
    ${m.cons.map(c=>`<div style="font-size:10px;color:var(--text1);margin-bottom:3px;display:flex;gap:5px;"><span style="color:var(--yellow,#f5a623);flex-shrink:0;">−</span>${c}</div>`).join('')}
  </div>
</div>
 
<div style="padding:10px 14px;border-bottom:1px solid var(--border);background:var(--bg0);">
  <span style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;">Generates: </span>
  <code style="font-size:10px;color:var(--accent);font-family:var(--mono);">${m.generates}</code>
</div>
 
<div style="padding:14px;">
  <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:10px;">Configuration</div>
  ${configForms[m.id] || '<div style="font-size:11px;color:var(--text3);">No additional configuration required.</div>'}
</div>`;
}

function _ifmCollectMethodConfig() {
    const cfg = {};
    document.querySelectorAll('[id^="ifm-mth-"]').forEach(el => {
        cfg[el.id] = el.value;
    });
    _IFM.inferenceMethodConfig = cfg;
    // Also store the currently selected method (in case user didn't click a card)
    const selected = document.querySelector('[id^="ifm-mth-card-"][style*="rgba(79,163,224,0.1)"]');
    // Method is already stored in _IFM.inferenceMethod via _ifmSelectMethod()
}

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
      <button onclick="_ifmExportPipeline()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(0,212,170,0.35);background:rgba(0,212,170,0.07);color:var(--accent,#00d4aa);cursor:pointer;font-family:var(--mono);">⬆ Export</button>
      <label style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(245,166,35,0.35);background:rgba(245,166,35,0.07);color:var(--yellow,#f5a623);cursor:pointer;font-family:var(--mono);" title="Import a previously exported pipeline JSON">⬇ Import<input type="file" accept=".json" style="display:none;" onchange="_ifmImportPipeline(this)"/></label>
      <button onclick="_ifmResetAll()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;font-family:var(--mono);">↺ Reset</button>
       <button id="ifm-modal-expand-btn" onclick="_ifmToggleModalExpand()" title="Expand to fit screen" style="background:none;border:1px solid var(--border);color:var(--text2);cursor:pointer;font-size:13px;padding:1px 8px;border-radius:3px;margin-right:2px;">⤢</button>
      <button onclick="modalMinimize('ifm-modal','Inference Manager')" style="background:none;border:1px solid var(--border);color:var(--text2);cursor:pointer;font-size:13px;padding:1px 8px;border-radius:3px;margin-right:4px;" title="Minimise to statusbar">⊟</button><button class="modal-close" onclick="closeModal('ifm-modal')">×</button>    </div>
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
            inferenceMethod:'async_udf', inferenceMethodConfig:{},
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
            if (_IFM.modelServer === 'custom_udf') {
                if (!document.getElementById('ifm-mc-udf-name')?.value?.trim()) return 'UDF function name is required.';
            } else if (_IFM.modelServer !== 'minio') {
                const ep = document.getElementById('ifm-mc-endpoint')?.value?.trim();
                if (!ep && _IFM.modelServer !== 'custom_udf') return 'Endpoint / URL is required.';
            }
            return null;
        }
        case 5: {
            const inp = document.getElementById('ifm-ic-input-col')?.value?.trim() || _IFM.inferenceConfig.inputCol;
            if (!inp) return 'Select or enter the input column for inference.';
            const alias = document.getElementById('ifm-ic-output-alias')?.value?.trim() || _IFM.inferenceConfig.outputAlias;
            if (!alias) return 'Output alias for the model result is required.';
            return null;
        }
        case 6: {
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
        case 3: _ifmCollectMethodConfig(); break;
        case 4: _ifmCollectAuthConfig(); break;
        case 5: _ifmCollectInferenceConfig(); break;
        case 6:
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
    if (next) next.style.display = n === 7 ? 'none' : '';
     if (gen)  gen.style.display  = n === 7 ? '' : 'none';
    if (lbl)  lbl.textContent    = `Step ${n + 1} of ${IFM_STEPS.length} — ${IFM_STEPS[n].label}`;

    const renderers = [
         _ifmRenderStep0, _ifmRenderStep1, _ifmRenderStep2,
         _ifmRenderStepMethod,
     _ifmRenderStep3, _ifmRenderStep4, _ifmRenderStep5, _ifmRenderStep6,
     ];
    const body = document.getElementById('ifm-body');
    if (body) { body.innerHTML = ''; renderers[n]?.(); }
}

function _ifmNext() {
    const err = _ifmValidateStep(_IFM.step);
    if (err) { _ifmShowValidationError(err); return; }
    _ifmCollectCurrentStep();
    if (_IFM.step < 7) _ifmGoStep(_IFM.step + 1);
}

function _ifmBack() {
    _ifmCollectCurrentStep();
    if (_IFM.step > 0) _ifmGoStep(_IFM.step - 1);
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 0 — Source Table (unchanged from original)
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep0() {
    const body = document.getElementById('ifm-body');
    let sessionTables = [];
    try {
        const hist = JSON.parse(localStorage.getItem('strlabstudio_query_history') || '[]');
        const createMatches = hist.map(h => h.sql || '').join('\n')
            .match(/CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/gi) || [];
        sessionTables = [...new Set(createMatches.map(m => { const parts = m.trim().split(/\s+/); return parts[parts.length - 1]; }))];
    } catch (_) {}
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        femH.forEach(h => { if (h.outputTable) sessionTables.push(h.outputTable); });
    } catch (_) {}
    sessionTables = [...new Set(sessionTables)].slice(0, 40);
    const femTables = [];
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        femH.forEach(h => { if (h.outputTable) femTables.push({ name: h.outputTable, src: 'fem', ts: h.ts }); });
    } catch (_) {}
    const allKnownTables = [
        ...femTables,
        ...sessionTables.filter(t => !femTables.find(f => f.name === t)).map(t => ({ name: t, src: 'session' })),
    ];

    body.innerHTML = `
<div class="ifm-info-box">
  Select the <strong>source Flink table</strong> that carries the streaming data for ML inference.
  Use the dropdown to pick from known session/FEM tables, or type a custom name.
</div>
<div class="ifm-card">
  <div class="ifm-section-title">Source Table</div>
  <div style="display:flex;gap:8px;align-items:stretch;margin-bottom:10px;">
    <div style="flex-shrink:0;min-width:0;">
      <label class="ifm-field-label">Select Known Table</label>
      <select id="ifm-table-dropdown" class="field-input"
        style="font-size:11px;font-family:var(--mono);height:32px;cursor:pointer;"
        onchange="_ifmSelectKnownTable(this.value)">
        <option value="">— pick a table —</option>
        ${femTables.length ? `<optgroup label="── Feature Engineering outputs">` : ''}
        ${femTables.map(t => `<option value="${_escIfm(t.name)}" data-src="fem">⬡ ${_escIfm(t.name)}</option>`).join('')}
        ${femTables.length ? `</optgroup>` : ''}
        ${sessionTables.filter(t => !femTables.find(f => f.name === t)).length ? `<optgroup label="── Session tables">` : ''}
        ${sessionTables.filter(t => !femTables.find(f => f.name === t)).map(t => `<option value="${_escIfm(t)}" data-src="session">◈ ${_escIfm(t)}</option>`).join('')}
        ${sessionTables.filter(t => !femTables.find(f => f.name === t)).length ? `</optgroup>` : ''}
      </select>
    </div>
    <div style="flex:1;min-width:0;">
      <label class="ifm-field-label">Table Name <span class="ifm-required">*</span></label>
      <input id="ifm-src-table" class="field-input" type="text"
        style="font-size:11px;font-family:var(--mono);height:32px;box-sizing:border-box;"
        placeholder="user_transaction_features"
        value="${_escIfm(_IFM.sourceTable)}"
        oninput="_IFM.sourceTable=this.value" />
    </div>
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
  ${allKnownTables.length ? `
  <div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:2px;">
    ${allKnownTables.slice(0,12).map(t => `
      <div onclick="_ifmSelectKnownTable('${_escIfm(t.name)}')"
        style="padding:2px 9px;border-radius:3px;font-size:10px;font-family:var(--mono);cursor:pointer;
          background:${t.src==='fem'?'rgba(0,212,170,0.08)':'rgba(79,163,224,0.07)'};
          border:1px solid ${t.src==='fem'?'rgba(0,212,170,0.25)':'rgba(79,163,224,0.2)'};
          color:${t.src==='fem'?'var(--accent)':'var(--blue,#4fa3e0)'};">
        ${t.src==='fem'?'⬡':'◈'} ${_escIfm(t.name)}
      </div>`).join('')}
  </div>` : ''}
</div>
<div class="ifm-card">
  <div class="ifm-section-title">Input Schema <span class="ifm-required">*</span></div>
  <div style="font-size:11px;color:var(--text2);margin-bottom:8px;line-height:1.7;">
    Format: <code style="background:var(--bg0);padding:1px 5px;border-radius:2px;color:var(--blue,#4fa3e0);">name TYPE</code> one per line.
  </div>
  <div id="ifm-schema-loading" style="display:none;font-size:11px;color:var(--accent);font-family:var(--mono);margin-bottom:6px;">⟳ Loading schema…</div>
  <div id="ifm-col-tags-wrap" style="display:${_IFM.inputColumns.length?'flex':'none'};flex-wrap:wrap;gap:3px;margin-bottom:8px;padding:8px;background:var(--bg0);border:1px solid var(--border);border-radius:3px;min-height:32px;">
    ${_IFM.inputColumns.map(c => `<div class="ifm-col-tag selected" title="${c.type}">${_escIfm(c.name)}<span class="ifm-tag-type">${c.type}</span></div>`).join('')}
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
        const dd = document.getElementById('ifm-table-dropdown');
        if (dd && _IFM.sourceTable) dd.value = _IFM.sourceTable;
    }, 50);
}

function _ifmSelectKnownTable(tableName) {
    if (!tableName) return;
    _IFM.sourceTable = tableName;
    const inp = document.getElementById('ifm-src-table');
    if (inp) inp.value = tableName;
    const dd = document.getElementById('ifm-table-dropdown');
    if (dd) dd.value = tableName;
    const loading = document.getElementById('ifm-schema-loading');
    if (loading) loading.style.display = '';
    let loaded = false;
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        const match = femH.find(h => h.outputTable === tableName);
        if (match) {
            const cols = match.outputColumns?.length ? match.outputColumns : match.sourceColumns?.length ? match.sourceColumns : null;
            if (cols && cols.length) { _IFM.inputColumns = cols; loaded = true; _ifmRefreshSchemaDisplay(cols, `✓ ${cols.length} columns loaded from Feature Engineering output "${tableName}"`); }
        }
    } catch (_) {}
    if (!loaded) {
        try {
            const state = JSON.parse(localStorage.getItem('strlabstudio_fem_state') || 'null');
            if (state) {
                const cols = state.outputTable === tableName ? (state.outputColumns?.length ? state.outputColumns : state.sourceColumns) : state.sourceTable === tableName ? state.sourceColumns : null;
                if (cols && cols.length) { _IFM.inputColumns = cols; loaded = true; _ifmRefreshSchemaDisplay(cols, `✓ ${cols.length} columns loaded from FEM state snapshot`); }
            }
        } catch (_) {}
    }
    if (!loaded) {
        if (loading) loading.style.display = 'none';
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
    const ta = document.getElementById('ifm-src-cols');
    if (ta) ta.value = cols.map(c => c.name + ' ' + c.type).join('\n');
    const tagsWrap = document.getElementById('ifm-col-tags-wrap');
    if (tagsWrap) {
        tagsWrap.style.display = 'flex';
        tagsWrap.innerHTML = cols.map(c => `<div class="ifm-col-tag selected" title="${c.type}">${_escIfm(c.name)}<span class="ifm-tag-type">${c.type}</span></div>`).join('');
    }
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
    const tagsWrap = document.getElementById('ifm-col-tags-wrap');
    if (tagsWrap && cols.length) {
        tagsWrap.style.display = 'flex';
        tagsWrap.innerHTML = cols.map(c => `<div class="ifm-col-tag selected" title="${c.type}">${_escIfm(c.name)}<span class="ifm-tag-type">${c.type}</span></div>`).join('');
    } else if (tagsWrap) { tagsWrap.style.display = 'none'; }
    const fb = document.getElementById('ifm-src-cols-feedback');
    if (fb) { fb.textContent = cols.length ? `✓ ${cols.length} column${cols.length > 1 ? 's' : ''} parsed` : ''; fb.style.color = 'var(--text3)'; }
}

function _ifmAutoFillFromFem() {
    try {
        const femH = JSON.parse(localStorage.getItem('strlabstudio_fem_history') || '[]');
        if (!femH.length) { if (typeof toast === 'function') toast('No Feature Engineering history found', 'warn'); return; }
        _ifmSelectKnownTable(femH[0].outputTable);
    } catch (_) {}
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1 — Model Server Selection
// NOTE: Server cards now render op.icon directly (img tag or emoji span).
//       Both are treated identically by innerHTML injection.
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
// STEP 2 — Model Configuration (unchanged logic, icon refs updated in srv object)
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
        mlflow: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('MLflow Tracking URI','ifm-mc-endpoint','http://mlflow:5000',true)}${fi('Model Name','ifm-mc-model-name','fraud-detection-model',true)}${fi('Model Stage / Version','ifm-mc-model-version','Production')}${sel('Serving Flavour','ifm-mc-mlflow-flavour',['REST (pyfunc serve)','Python Function','ONNX','Spark ML','H2O'])}${fi('Registered Model URI','ifm-mc-model-uri','models:/fraud-detection-model/Production')}${fi('Experiment ID (optional)','ifm-mc-exp-id','')}</div>`,
        mlflow_serve: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('mlflow serve Endpoint URL','ifm-mc-endpoint','http://model-server:5001/invocations',true)}${fi('Model Name','ifm-mc-model-name','my-model',true)}${sel('Input Format','ifm-mc-mlflow-fmt',['dataframe-split','dataframe-records','instances','inputs'])}</div>`,
        sagemaker: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Endpoint Name','ifm-mc-endpoint','fraud-detection-endpoint-prod',true)}${fi('AWS Region','ifm-mc-aws-region','us-east-1',true)}${sel('Content Type','ifm-mc-content-type',['application/json','text/csv','application/x-recordio-protobuf'])}${sel('Accept Type','ifm-mc-accept-type',['application/json','text/csv'])}${fi('Model Variant (optional)','ifm-mc-variant','')}${fi('Target Container Hostname (optional)','ifm-mc-target-host','')}</div>`,
        azureml: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Endpoint URL','ifm-mc-endpoint','https://my-endpoint.eastus.inference.ml.azure.com/score',true)}${fi('Deployment Name','ifm-mc-deployment','fraud-blue')}${fi('Workspace Name (for logging)','ifm-mc-workspace','my-workspace')}${fi('Resource Group','ifm-mc-rg','ml-resource-group')}${fi('Subscription ID','ifm-mc-sub-id','')}</div>`,
        vertexai: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Project ID','ifm-mc-gcp-project','my-gcp-project',true)}${fi('Location / Region','ifm-mc-gcp-region','us-central1',true)}${fi('Endpoint ID','ifm-mc-endpoint','1234567890',true)}${sel('API Version','ifm-mc-gcp-ver',['v1','v1beta1'])}${fi('Deployed Model ID (optional)','ifm-mc-gcp-deployed-model','')}</div>`,
        databricks: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Databricks Workspace URL','ifm-mc-endpoint','https://my-workspace.azuredatabricks.net',true)}${fi('Registered Model Name','ifm-mc-model-name','fraud-model',true)}${fi('Model Version or Alias','ifm-mc-model-version','Champion')}${fi('Serving Endpoint Name','ifm-mc-db-serving-ep','fraud-model-serving')}</div>`,
        bentoml: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('BentoML Server URL','ifm-mc-endpoint','http://bentoml:3000',true)}${fi('Service / Runner Route','ifm-mc-route','/predict',true)}${sel('Protocol','ifm-mc-protocol',['HTTP REST','gRPC'])}${fi('Bento Tag','ifm-mc-bento-tag','fraud_classifier:latest')}</div>`,
        triton: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Triton HTTP URL','ifm-mc-endpoint','http://triton:8000',true)}${fi('Model Name','ifm-mc-model-name','fraud_onnx',true)}${fi('Model Version','ifm-mc-model-version','1')}${sel('Protocol','ifm-mc-protocol',['HTTP','gRPC (port 8001)'])}${sel('Input Data Type','ifm-mc-triton-dtype',['FP32','FP64','INT32','INT64','BYTES','BOOL'])}${fi('Input Tensor Name','ifm-mc-triton-input','INPUT__0')}</div>`,
        torchserve: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('TorchServe URL','ifm-mc-endpoint','http://torchserve:8080',true)}${fi('Model Name','ifm-mc-model-name','fraud_classifier',true)}${fi('Model Version','ifm-mc-model-version','1.0')}${sel('API Type','ifm-mc-ts-api',['Predictions API','Explanations API'])}</div>`,
        tfserving: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('TF Serving URL','ifm-mc-endpoint','http://tfserving:8501',true)}${fi('Model Name','ifm-mc-model-name','fraud_v2',true)}${fi('Signature Name','ifm-mc-tf-sig','serving_default')}${sel('API Version','ifm-mc-tf-apiver',['v1 (REST)','v2 (REST)'])}</div>`,
        ray: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Ray Serve URL','ifm-mc-endpoint','http://ray-head:8000',true)}${fi('Deployment Route','ifm-mc-route','/fraud-classifier',true)}${fi('Ray Dashboard URL (opt)','ifm-mc-ray-dashboard','http://ray-head:8265')}</div>`,
        seldon: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Seldon URL','ifm-mc-endpoint','http://seldon-ambassador:80',true)}${fi('Namespace','ifm-mc-seldon-ns','default',true)}${fi('Deployment Name','ifm-mc-model-name','fraud-classifier',true)}${sel('Predictor Protocol','ifm-mc-seldon-proto',['REST V2','REST V1','gRPC'])}</div>`,
        kserve: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('KServe Ingress URL','ifm-mc-endpoint','http://istio-ingressgateway',true)}${fi('InferenceService Name','ifm-mc-model-name','fraud-classifier',true)}${fi('Namespace','ifm-mc-kserve-ns','default')}${sel('Protocol Version','ifm-mc-kserve-proto',['v2','v1'])}</div>`,
        minio: `<div class="ifm-warn-box">MinIO / S3 is an artifact store. Models are loaded by the Flink operator or UDF at startup.</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('MinIO / S3 Endpoint','ifm-mc-endpoint','http://minio:9000',true)}${fi('Bucket Name','ifm-mc-minio-bucket','ml-models',true)}${fi('Model Object Path','ifm-mc-minio-path','fraud/v3/model.pkl',true)}${sel('Model Format','ifm-mc-minio-fmt',['pickle (sklearn)','ONNX','TensorFlow SavedModel','PyTorch TorchScript','XGBoost JSON','LightGBM','PMML'])}${fi('Local Load Path (Flink TaskManager)','ifm-mc-local-path','/tmp/models/fraud_v3')}${fi('Cache TTL (seconds, 0=no refresh)','ifm-mc-cache-ttl','3600')}</div>`,
        huggingface: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Model ID / Endpoint URL','ifm-mc-endpoint','https://api-inference.huggingface.co/models/my-org/fraud-v1',true)}${fi('Model ID (for Inference API)','ifm-mc-hf-model','my-org/fraud-classifier')}${sel('Task','ifm-mc-hf-task',['text-classification','token-classification','feature-extraction','text-generation','fill-mask','question-answering','zero-shot-classification','tabular-classification','tabular-regression'])}${sel('API Type','ifm-mc-hf-api',['Inference API (free)','Inference Endpoints (dedicated)'])}</div>`,
        openai: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Base URL (override, optional)','ifm-mc-endpoint','https://api.openai.com/v1')}${fi('Model','ifm-mc-model-name','gpt-4o-mini',true)}${fi('System Prompt','ifm-mc-system-prompt','You are a fraud detection assistant. Classify each transaction.')}${sel('Response Format','ifm-mc-openai-fmt',['text','json_object'])}${fi('Max Tokens','ifm-mc-max-tokens','64')}${fi('Temperature','ifm-mc-temperature','0.0')}</div>`,
        anthropic: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('API Base URL','ifm-mc-endpoint','https://api.anthropic.com')}${sel('Model','ifm-mc-model-name',[{v:'claude-sonnet-4-6',l:'Claude Sonnet 4.6'},{v:'claude-opus-4-6',l:'Claude Opus 4.6'},{v:'claude-haiku-4-5-20251001',l:'Claude Haiku 4.5'}])}${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction as FRAUD or LEGITIMATE.')}${fi('Max Tokens','ifm-mc-max-tokens','128')}${fi('anthropic-version header','ifm-mc-anthropic-ver','2023-06-01')}${sel('Response Parsing','ifm-mc-anthropic-parse',['First content text block','JSON from text block'])}</div>`,
        cohere: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('API Base URL','ifm-mc-endpoint','https://api.cohere.com')}${sel('Endpoint','ifm-mc-cohere-ep',['/v2/classify','/v2/embed','/v2/chat','/v2/generate'])}${fi('Model','ifm-mc-model-name','command-r-plus')}${fi('Classification Labels (comma-sep for /classify)','ifm-mc-cohere-labels','FRAUD,LEGITIMATE')}</div>`,
        mistral: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('API Base URL','ifm-mc-endpoint','https://api.mistral.ai/v1')}${fi('Model','ifm-mc-model-name','mistral-small-latest',true)}${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction.')}${fi('Max Tokens','ifm-mc-max-tokens','64')}${fi('Temperature','ifm-mc-temperature','0.0')}</div>`,
        together: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('API Base URL','ifm-mc-endpoint','https://api.together.xyz/v1')}${fi('Model','ifm-mc-model-name','meta-llama/Llama-3-8b-chat-hf',true)}${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction as FRAUD or LEGITIMATE. Respond with one word.')}${fi('Max Tokens','ifm-mc-max-tokens','16')}</div>`,
        bedrock: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('AWS Region','ifm-mc-aws-region','us-east-1',true)}${sel('Model ID','ifm-mc-model-name',[{v:'anthropic.claude-sonnet-4-6',l:'Claude Sonnet 4.6'},{v:'amazon.titan-text-express-v1',l:'Amazon Titan Text Express'},{v:'meta.llama3-8b-instruct-v1:0',l:'Meta Llama 3 8B'},{v:'mistral.mistral-7b-instruct-v0:2',l:'Mistral 7B Instruct'},{v:'cohere.command-r-v1:0',l:'Cohere Command R'},{v:'ai21.j2-ultra-v1',l:'AI21 Jurassic-2 Ultra'}])}${fi('System Prompt','ifm-mc-system-prompt','Classify the following transaction.')}${fi('Max Tokens','ifm-mc-max-tokens','128')}</div>`,
        openai_compat: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Base URL','ifm-mc-endpoint','http://localhost:11434/v1',true)}${fi('Model','ifm-mc-model-name','llama3',true)}${fi('System Prompt','ifm-mc-system-prompt','Classify this transaction.')}${fi('Max Tokens','ifm-mc-max-tokens','64')}${fi('Temperature','ifm-mc-temperature','0.0')}${sel('Compatible With','ifm-mc-compat-type',['vLLM','Ollama','LocalAI','LM Studio','Llamafile','Other'])}</div>`,
        custom_http: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Endpoint URL','ifm-mc-endpoint','https://my-model-server.internal/v1/predict',true)}${sel('HTTP Method','ifm-mc-http-method',['POST','GET','PUT'])}${sel('Content-Type','ifm-mc-content-type',['application/json','text/plain','application/x-www-form-urlencoded','multipart/form-data'])}${sel('Response Parsing','ifm-mc-http-parse',['JSON path extraction','Full JSON response as STRING','Raw text body'])}${fi('JSON Response Path (e.g. $.result.score)','ifm-mc-json-path','$.prediction')}${fi('Request Body Template','ifm-mc-req-template','{"inputs": [{{INPUT_COL}}]}')}</div><div style="margin-top:8px;"><label class="ifm-field-label">Custom Request Headers (one per line: Header-Name: value)</label><textarea id="ifm-mc-custom-headers" class="field-input" rows="3" style="font-size:11px;font-family:var(--mono);resize:vertical;" placeholder="X-Model-Version: v3&#10;X-Tenant-ID: fraud-team">${_escIfm(mc['ifm-mc-custom-headers']||'')}</textarea></div>`,
        custom_grpc: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('gRPC Target (host:port)','ifm-mc-endpoint','model-server:50051',true)}${fi('Proto Package.Service.Method','ifm-mc-grpc-method','inference.PredictionService/Predict',true)}${fi('Proto File Path or Descriptor','ifm-mc-grpc-proto','/opt/flink/protos/inference.proto')}${sel('Channel Security','ifm-mc-grpc-tls',['Plaintext','TLS','mTLS'])}${fi('Request Timeout (ms)','ifm-mc-grpc-timeout','3000')}</div>`,
        custom_udf: `<div class="ifm-info-box">Use a Flink scalar or async UDF that wraps your model inference logic (Java/Scala/Python). The UDF must be registered in the session via the UDF Manager or a prior CREATE FUNCTION statement.</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('UDF Function Name','ifm-mc-udf-name','predict_fraud_score',true)}${sel('UDF Kind','ifm-mc-udf-kind',['Scalar Function','Async Scalar Function','Table Function','Aggregate Function'])}${fi('UDF Arguments (col names, comma-sep)','ifm-mc-udf-args','amount, tx_count_5m, avg_amt_5m')}${fi('UDF Return Type','ifm-mc-udf-return','DOUBLE')}${fi('CREATE FUNCTION Statement (if not yet registered)','ifm-mc-udf-create','')}${fi('JAR Path / Class Name (for reference)','ifm-mc-udf-class','com.mycompany.flink.udf.FraudScoreUDF')}</div>`,
    };

    const form = serverForms[_IFM.modelServer] || `<div class="ifm-warn-box">No specific configuration form for "${srv.name}". Fill in the endpoint and proceed to Auth.</div><div style="display:grid;grid-template-columns:1fr;gap:10px;">${fi('Endpoint / Base URL','ifm-mc-endpoint','https://your-model-server.com/predict',true)}</div>`;

    body.innerHTML = `
<div class="ifm-info-box">
  Configure your <strong>${srv.name}</strong> model server connection. Fields marked <span class="ifm-required">*</span> are required.
</div>
<div class="ifm-card">
  <div class="ifm-section-title">${srv.name} — Connection Settings</div>
  ${form}
</div>`;
}

function _ifmCollectModelConfig() {
    const mc = {};
    document.querySelectorAll('[id^="ifm-mc-"]').forEach(el => { mc[el.id] = el.value; });
    _IFM.modelConfig = mc;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3 — Authentication & Credentials (unchanged)
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep3() {
    const body = document.getElementById('ifm-body');
    const srv = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const ac = _IFM.authConfig;
    const AUTH_TYPES = [
        { id:'none',       label:'No Auth',            icon:'🔓' },
        { id:'bearer',     label:'Bearer Token',        icon:'🔑' },
        { id:'api_key',    label:'API Key (header)',    icon:'🗝' },
        { id:'x_api_key',  label:'x-api-key header',   icon:'🗝' },
        { id:'basic',      label:'Basic Auth',          icon:'👤' },
        { id:'header',     label:'Custom Header',       icon:'📋' },
        { id:'aws_sigv4',  label:'AWS SigV4',           icon:'☁' },
        { id:'aws_keys',   label:'AWS Access Keys',     icon:'☁' },
        { id:'minio_keys', label:'MinIO Access Keys',   icon:'🗄' },
        { id:'azure_ad',   label:'Azure AD Token',      icon:'🔷' },
        { id:'google_sa',  label:'Google Service Acct', icon:'⬡' },
        { id:'tls_cert',   label:'mTLS Certificate',    icon:'🛡' },
    ];
    const availableAuth = srv.authTypes || ['bearer','api_key','none'];
    const current = _IFM.authType || availableAuth[0] || 'none';

    body.innerHTML = `
<div class="ifm-info-box">
  Configure authentication for <strong>${srv.name}</strong>. Credentials stored here are embedded as comments in the generated SQL — use environment variables or Flink secrets in production.
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
    const ac = _IFM.authConfig;
    const authForms = {
        none:      `<div style="font-size:11px;color:var(--text3);">No authentication required.</div>`,
        bearer:    `<label class="ifm-field-label">Bearer Token</label><input id="ifm-ac-token" class="field-input" type="password" placeholder="eyJhbGci…" value="${_escIfm(ac['ifm-ac-token']||'')}" style="font-size:11px;font-family:var(--mono);" />`,
        api_key:   `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">API Key</label><input id="ifm-ac-apikey" class="field-input" type="password" placeholder="sk-…" value="${_escIfm(ac['ifm-ac-apikey']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Header Name</label><input id="ifm-ac-header-name" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-header-name']||'Authorization')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        x_api_key: `<label class="ifm-field-label">x-api-key</label><input id="ifm-ac-xapikey" class="field-input" type="password" placeholder="sk-ant-…" value="${_escIfm(ac['ifm-ac-xapikey']||'')}" style="font-size:11px;font-family:var(--mono);" />`,
        basic:     `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Username</label><input id="ifm-ac-user" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-user']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Password</label><input id="ifm-ac-pass" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-pass']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        header:    `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Header Name</label><input id="ifm-ac-h-name" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-h-name']||'X-API-Token')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Header Value</label><input id="ifm-ac-h-val" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-h-val']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        aws_sigv4: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">AWS Region</label><input id="ifm-ac-aws-region" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-region']||'us-east-1')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Service</label><input id="ifm-ac-aws-service" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-service']||'sagemaker')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        aws_keys:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Access Key ID</label><input id="ifm-ac-aws-kid" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-kid']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Secret Access Key</label><input id="ifm-ac-aws-sak" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-aws-sak']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">AWS Region</label><input id="ifm-ac-aws-region" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-aws-region']||'us-east-1')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        minio_keys:`<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Access Key</label><input id="ifm-ac-minio-key" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-minio-key']||'minioadmin')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Secret Key</label><input id="ifm-ac-minio-secret" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-minio-secret']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        azure_ad:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Tenant ID</label><input id="ifm-ac-az-tenant" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-az-tenant']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Client ID</label><input id="ifm-ac-az-client" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-az-client']||'')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Client Secret</label><input id="ifm-ac-az-secret" class="field-input" type="password" value="${_escIfm(ac['ifm-ac-az-secret']||'')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
        google_sa: `<label class="ifm-field-label">Service Account Key Path</label><input id="ifm-ac-gsa-path" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-gsa-path']||'/etc/gcp/sa-key.json')}" style="font-size:11px;font-family:var(--mono);" />`,
        tls_cert:  `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><div><label class="ifm-field-label">Cert Path</label><input id="ifm-ac-tls-cert" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-tls-cert']||'/etc/certs/client.crt')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">Key Path</label><input id="ifm-ac-tls-key" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-tls-key']||'/etc/certs/client.key')}" style="font-size:11px;font-family:var(--mono);" /></div><div><label class="ifm-field-label">CA Path</label><input id="ifm-ac-tls-ca" class="field-input" type="text" value="${_escIfm(ac['ifm-ac-tls-ca']||'/etc/certs/ca.crt')}" style="font-size:11px;font-family:var(--mono);" /></div></div>`,
    };
    wrap.innerHTML = authForms[id] || authForms['none'];
}

function _ifmCollectAuthConfig() {
    const ac = {};
    document.querySelectorAll('[id^="ifm-ac-"]').forEach(el => { ac[el.id] = el.type === 'checkbox' ? String(el.checked) : el.value; });
    _IFM.authConfig = ac;
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 4 — Inference I/O (unchanged)
// ─────────────────────────────────────────────────────────────────────────────
function _ifmRenderStep4() {
    const body = document.getElementById('ifm-body');
    const ic = _IFM.inferenceConfig;
    const cols = _IFM.inputColumns;

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
        placeholder="CAST(amount AS DOUBLE) / 1000.0">${_escIfm(ic.preProcessExpr)}</textarea>
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
// STEP 5 — Output Sink (unchanged)
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
        kafka: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${inp('Bootstrap Servers','ifm-sk-bootstrap','kafka:9092')}${inp('Output Topic','ifm-sk-topic','scored-output')}${sel('Format','ifm-sk-format',['json','avro','avro-confluent','csv'])}${sel('Security Protocol','ifm-sk-security',['PLAINTEXT','SASL_SSL','SSL','SASL_PLAINTEXT'],'PLAINTEXT')}${inp('SASL Username / API Key','ifm-sk-sasl-user','api-key')}${inp('SASL Password / Secret','ifm-sk-sasl-pass','',true)}${inp('Schema Registry URL (avro-confluent)','ifm-sk-sr-url','http://schema-registry:8081')}</div>`,
        jdbc: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${inp('JDBC URL','ifm-sk-jdbc-url','jdbc:postgresql://localhost:5432/ml_results')}${inp('Target Table','ifm-sk-jdbc-table','public.fraud_scores')}${inp('Username','ifm-sk-jdbc-user','flink_writer')}${inp('Password','ifm-sk-jdbc-pass','',true)}${sel('Driver','ifm-sk-jdbc-driver',['org.postgresql.Driver','com.mysql.cj.jdbc.Driver','com.amazon.redshift.jdbc.Driver'])}${inp('Sink Parallelism','ifm-sk-jdbc-par','4')}</div>`,
        elasticsearch: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${inp('ES Hosts','ifm-sk-es-hosts','http://elasticsearch:9200')}${inp('Index','ifm-sk-es-index','ml-scored-results')}${sel('ES Version','ifm-sk-es-ver',['7','8'])}${inp('Username','ifm-sk-es-user','elastic')}${inp('Password','ifm-sk-es-pass','',true)}</div>`,
        filesystem: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${inp('Sink Path','ifm-sk-fs-path','s3://my-bucket/scored/')}${sel('Format','ifm-sk-fs-format',['parquet','orc','json','csv'])}${inp('Rolling Interval','ifm-sk-fs-roll','10 min')}${inp('Partition Column','ifm-sk-fs-part','')}</div>`,
        iceberg: `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${inp('Catalog Name','ifm-sk-ice-cat','iceberg_catalog')}${inp('Database.Table','ifm-sk-ice-tbl','ml.fraud_scores')}${sel('Catalog Type','ifm-sk-ice-cattype',['hive','hadoop','rest','glue'])}${inp('Warehouse Path','ifm-sk-ice-wh','s3://warehouse/iceberg')}</div>`,
        print:     `<div style="font-size:11px;color:var(--text3);padding:6px 0;">Outputs to TaskManager stdout. Development only — no credentials needed.</div>`,
        blackhole: `<div style="font-size:11px;color:var(--text3);padding:6px 0;">Discards all output — no credentials needed. Use for throughput benchmarking.</div>`,
    };
    wrap.innerHTML = forms[type] || '';
}

function _ifmCollectSinkConfig() {
    const cfg = {};
    const ids = ['ifm-sk-bootstrap','ifm-sk-topic','ifm-sk-format','ifm-sk-security','ifm-sk-sasl-user','ifm-sk-sasl-pass','ifm-sk-sr-url','ifm-sk-jdbc-url','ifm-sk-jdbc-table','ifm-sk-jdbc-user','ifm-sk-jdbc-pass','ifm-sk-jdbc-driver','ifm-sk-jdbc-par','ifm-sk-es-hosts','ifm-sk-es-index','ifm-sk-es-ver','ifm-sk-es-user','ifm-sk-es-pass','ifm-sk-fs-path','ifm-sk-fs-format','ifm-sk-fs-roll','ifm-sk-fs-part','ifm-sk-ice-cat','ifm-sk-ice-tbl','ifm-sk-ice-cattype','ifm-sk-ice-wh'];
    ids.forEach(id => { const el = document.getElementById(id); if (el) cfg[id] = el.value; });
    _IFM.sinkConfig = { ...(_IFM.sinkConfig || {}), ...cfg };
}


// ─────────────────────────────────────────────────────────────────────────────
// STEP 6 — Review & SQL
// Canvas icon update: Simple Icons <img> tags are SVG-incompatible, so the
// canvas SVG still uses emoji/text glyphs for server icons inside the SVG.
// The srv.icon (img tag) is used in the HTML header above the canvas only.
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
        container.style.cssText = `position:fixed;inset:0;z-index:9990;background:var(--bg0,#050810);display:flex;flex-direction:column;border:none;border-radius:0;`;
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
    wrap.addEventListener('wheel', e => { e.preventDefault(); _ifmCanvasZoom(e.deltaY < 0 ? 0.12 : -0.12, e.clientX, e.clientY); }, { passive: false });
    wrap.addEventListener('mousedown', e => {
        if (e.button !== 0) return;
        _IFMC.panning = true;
        _IFMC.panSX = e.clientX; _IFMC.panSY = e.clientY;
        _IFMC.panOX = _IFMC.panX; _IFMC.panOY = _IFMC.panY;
        wrap.style.cursor = 'grabbing';
        e.preventDefault();
    });
    const onMove = e => { if (!_IFMC.panning) return; _IFMC.panX = _IFMC.panOX + (e.clientX - _IFMC.panSX); _IFMC.panY = _IFMC.panOY + (e.clientY - _IFMC.panSY); _ifmApplyCanvasTransform(); };
    const onUp = () => { if (!_IFMC.panning) return; _IFMC.panning = false; wrap.style.cursor = 'grab'; };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
    wrap.addEventListener('dblclick', () => { _IFMC.zoom = 1; _IFMC.panX = 0; _IFMC.panY = 0; _ifmCanvasFitToView(); });
}

function _ifmRenderStep6() {
    _IFM.outputTable = document.getElementById('ifm-out-table')?.value?.trim() || _IFM.outputTable || (_IFM.sourceTable + '_scored');
    const sql = _ifmGenerateSql();
    _IFM.generatedSql = sql;
    _ifmSaveHistory();
    _ifmUpdateHistCount();

    _IFMC.zoom = 1; _IFMC.panX = 0; _IFMC.panY = 0;
    _IFMC.maximised = false; _IFMC.origStyle = null;

    const srv = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const body = document.getElementById('ifm-body');

    // For the HTML header we use srv.icon (img tag or emoji span).
    // The SVG canvas uses a text glyph mapped from server id (SVG can't embed external images).
    const srvCanvasGlyph = _ifmServerCanvasGlyph(srv.id);

    body.innerHTML = `
<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
  <div>
    <div style="font-size:13px;font-weight:700;color:var(--text0);">Generated Inference Pipeline SQL</div>
    <div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:2px;display:flex;align-items:center;gap:6px;">
      <span style="color:var(--blue);">${_escIfm(_IFM.sourceTable)}</span>
      → <span style="display:inline-flex;align-items:center;gap:4px;color:var(--yellow);">${srv.icon} ${srv.name}</span>
      → <span style="color:var(--blue);">${_escIfm(_IFM.inferenceConfig.outputAlias)}</span>
      → <span style="color:var(--accent);">${_escIfm(_IFM.outputTable)}</span>
      <span style="background:rgba(79,163,224,0.1);color:var(--blue);padding:1px 7px;border-radius:10px;font-size:9px;font-weight:700;">SAVED TO HISTORY</span>
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
    <div id="ifm-canvas-wrap" style="flex:1;overflow:hidden;position:relative;cursor:grab;background:var(--bg0);">
      <svg id="ifm-canvas-svg" style="transform-origin:0 0;will-change:transform;display:block;overflow:visible;"></svg>
    </div>
    <div style="display:flex;gap:10px;padding:4px 10px;border-top:1px solid var(--border);flex-shrink:0;background:var(--bg2);flex-wrap:wrap;">
      <span style="font-size:9px;color:#4e9de8;font-family:var(--mono);">◉ Source cols</span>
      <span style="font-size:9px;color:#b080e0;font-family:var(--mono);">⨍ Pre-process</span>
      <span style="font-size:9px;color:#4fa3e0;font-family:var(--mono);display:inline-flex;align-items:center;gap:3px;"><span style="display:inline-flex;align-items:center;width:14px;height:14px;overflow:hidden;">${srv.icon.replace(/width="22"/g,'width="14"').replace(/height="22"/g,'height="14"').replace(/width="20"/g,'width="14"').replace(/height="20"/g,'height="14"')}</span> Model</span>
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

// ── Canvas glyph mapping: SVG can't embed external <img> tags, so we map
//    each server id to a short Unicode glyph used inside the SVG canvas only.
function _ifmServerCanvasGlyph(id) {
    const map = {
        mlflow:'🔬', mlflow_serve:'🔬',
        sagemaker:'☁', bedrock:'☁',
        azureml:'🔷', vertexai:'⬡',
        databricks:'🧱', bentoml:'🍱',
        triton:'⚡', torchserve:'🔥',
        tfserving:'🧠', ray:'🌞',
        seldon:'🚀', kserve:'🎯',
        minio:'🗄', huggingface:'🤗',
        openai:'✦', anthropic:'✧',
        cohere:'🔵', mistral:'💨',
        together:'🌐', openai_compat:'🔌',
        custom_http:'⚙', custom_grpc:'⚡',
        custom_udf:'⨍',
    };
    return map[id] || '🔬';
}

// Renders the actual srv.icon HTML into the SVG canvas via <foreignObject>.
// x, y = top-left corner of the icon box; size = width & height in px.
// Returns an SVG <foreignObject> string containing the real icon HTML.
function _ifmServerIconSvg(srv, cx, cy, size) {
    size = size || 40;
    const x = cx - size / 2;
    const y = cy - size / 2;
    // Wrap the icon in a centered flex div so both <img> and <svg> icons
    // render at the right size regardless of their natural dimensions.
    return `<foreignObject x="${x}" y="${y}" width="${size}" height="${size}">`
        + `<div xmlns="http://www.w3.org/1999/xhtml" style="width:${size}px;height:${size}px;`
        + `display:flex;align-items:center;justify-content:center;overflow:hidden;">`
        + `<div style="width:${size}px;height:${size}px;display:flex;align-items:center;`
        + `justify-content:center;filter:drop-shadow(0 0 4px rgba(79,163,224,0.5));">`
        + srv.icon
        + `</div></div></foreignObject>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Inference Canvas SVG (unchanged — uses text glyphs, not img tags)
// ─────────────────────────────────────────────────────────────────────────────
function _ifmDrawCanvasSvg() {
    const svgEl = document.getElementById('ifm-canvas-svg');
    const wrap  = document.getElementById('ifm-canvas-wrap');
    if (!svgEl || !wrap) return;

    const srv      = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const srvGlyph = _ifmServerCanvasGlyph(srv.id);
    const cols     = (_IFM.selectedInputCols.length ? _IFM.selectedInputCols : _IFM.inputColumns.map(c=>c.name));
    const ic       = _IFM.inferenceConfig;
    const alias    = ic.outputAlias || 'prediction';
    const outTable = _IFM.outputTable || 'output';
    const hasPre   = !!ic.preProcessExpr?.trim();
    const hasPost  = !!ic.postProcessExpr?.trim();
    const ptCols   = (ic.passthroughCols || '').split(',').map(c=>c.trim()).filter(Boolean);

    const PAD    = 28;
    const NODE_W = 155;
    const COL_H  = 22;
    const COL_GAP = 4;
    const MODEL_H = 90;
    const HGAP   = 75;

    const displayCols = cols.slice(0, 20);
    const colsH = displayCols.length * (COL_H + COL_GAP) - COL_GAP;
    const srcH  = 44;

    const X0    = PAD;
    const X1    = X0 + NODE_W + HGAP;
    const X2    = X1 + (hasPre ? NODE_W + HGAP : 0);
    const X3    = X2 + NODE_W + HGAP;
    const X4    = X3 + (hasPost ? NODE_W + HGAP : 0);
    const X_OUT = X4;

    const totalW  = X_OUT + NODE_W + PAD;
    const contentH = Math.max(srcH + 16 + colsH, MODEL_H + 60, 200) + PAD * 2;
    const midY    = contentH / 2;
    const totalH  = contentH;

    _IFMC.svgW = totalW;
    _IFMC.svgH = totalH;

    const C = {
        bg:'#060a12', grid:'rgba(79,163,224,0.04)',
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
    const rr  = (x,y,w,h,r,fill,stroke,sw) => `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${sw||1.5}"/>`;
    const txt = (s,x,y,fill,size,weight,anchor) => `<text x="${x}" y="${y}" fill="${fill}" font-size="${size||10}" font-weight="${weight||'400'}" font-family="var(--mono,monospace)" text-anchor="${anchor||'start'}" dominant-baseline="middle">${esc(s)}</text>`;
    const bezier = (x1,y1,x2,y2,color,sw) => { const cp=Math.abs(x2-x1)*0.42; return `<path d="M${x1},${y1} C${x1+cp},${y1} ${x2-cp},${y2} ${x2},${y2}" stroke="${color}" stroke-width="${sw||1.4}" fill="none" opacity="0.9" marker-end="url(#ifm-arr)"/>`; };

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

    s += `<rect width="${totalW}" height="${totalH}" fill="${C.bg}"/>`;
    for (let x=0;x<=totalW;x+=22) s += `<line x1="${x}" y1="0" x2="${x}" y2="${totalH}" stroke="${C.grid}" stroke-width="0.5"/>`;
    for (let y=0;y<=totalH;y+=22) s += `<line x1="0" y1="${y}" x2="${totalW}" y2="${y}" stroke="${C.grid}" stroke-width="0.5"/>`;

    // Headers
    const headers = [
        { x:X0, l:'SOURCE FEATURES' },
        hasPre ? { x:X1, l:'PRE-PROCESS' } : null,
        { x:hasPre?X2:X1, l:`MODEL · ${srv.name.toUpperCase()}` },
        hasPost ? { x:X3, l:'POST-PROCESS' } : null,
        { x:X_OUT, l:'OUTPUT' },
    ].filter(Boolean);
    headers.forEach(h => { s += txt(h.l, h.x, PAD-10, 'rgba(100,150,200,0.4)', 8, '700'); });

    // Source node
    const srcY = PAD;
    const srcMid = srcY + srcH/2;
    s += rr(X0, srcY, NODE_W, srcH, 6, C.src.fill, C.src.stroke, 2);
    s += txt('◉', X0+8, srcMid, C.src.stroke, 14);
    s += txt(tr(_IFM.sourceTable||'source',14), X0+26, srcMid-7, C.src.text, 11, '700');
    s += txt(`${cols.length} input features`, X0+26, srcMid+7, C.src.sub, 9);

    // Column nodes
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
        s += `<line x1="${colRx}" y1="${srcY+srcH}" x2="${colRx}" y2="${mid}" stroke="${C.edge.src}" stroke-width="0.8" opacity="0.3"/>`;
        colMids.push(mid);
    });
    if (colMids.length > 1) s += `<line x1="${colRx}" y1="${srcY+srcH}" x2="${colRx}" y2="${colMids[colMids.length-1]}" stroke="${C.edge.src}" stroke-width="0.8" opacity="0.2"/>`;

    // Pre-process node
    let preRightX = colRx, preRightY = midY;
    if (hasPre) {
        const ppH = 44, ppY = midY - ppH/2, ppMid = midY;
        s += rr(X1, ppY, NODE_W, ppH, 6, C.pre.fill, C.pre.stroke, 1.8);
        s += txt('⨍', X1+8, ppMid, C.pre.stroke, 15);
        s += txt('Pre-process', X1+26, ppMid-8, C.pre.text, 11, '600');
        s += txt(tr(ic.preProcessExpr||'transform',16), X1+26, ppMid+6, C.pre.sub, 8);
        colMids.forEach(cy => s += bezier(colRx, cy, X1, ppMid, C.edge.pre, 1.1));
        preRightX = X1+NODE_W; preRightY = ppMid;
    } else {
        // Draw lines from ALL columns to the model node.
        // To avoid visual clutter with many columns, bundle them into a
        // single convergence point at the mid-right edge of the column panel,
        // then draw one thicker line from there to the model.
        const modelTargetX = hasPre ? X2 : X1;
        if (colMids.length <= 6) {
            // Few columns — draw individual lines
            colMids.forEach(cy => s += bezier(colRx, cy, modelTargetX, midY, C.edge.pre, 1.2));
        } else {
            // Many columns — draw individual thin lines to a funnel point,
            // then one thicker line from funnel to model
            const funnelX = colRx + HGAP * 0.4;
            const funnelY = midY;
            colMids.forEach(cy => {
                s += `<line x1="${colRx}" y1="${cy}" x2="${funnelX}" y2="${funnelY}"
                    stroke="${C.edge.pre}" stroke-width="0.7" opacity="0.35"/>`;
            });
            // Funnel convergence dot
            s += `<circle cx="${funnelX}" cy="${funnelY}" r="4"
                fill="${C.edge.pre}" opacity="0.5"/>`;
            // One thick line from funnel to model
            s += bezier(funnelX, funnelY, modelTargetX, midY, C.edge.model, 2.2);
            // Label showing count
            s += txt(`${colMids.length} features`, funnelX + 6, funnelY - 10,
                C.edge.pre, 8, '600');
        }
    }

    // Model server node (hero)
    const modelX = hasPre ? X2 : X1;
    const modelY = midY - MODEL_H/2;
    const modelMidY = midY;
    s += `<circle cx="${modelX+NODE_W/2}" cy="${modelMidY}" r="${MODEL_H*0.62}" fill="url(#ifm-model-grad)" stroke="${C.model.stroke}" stroke-width="1" opacity="0.5"/>`;
    s += `<circle cx="${modelX+NODE_W/2}" cy="${modelMidY}" r="${MODEL_H*0.62}" fill="none" stroke="${C.model.stroke}" stroke-width="1.5" stroke-dasharray="5 5" opacity="0.2"/>`;
    s += rr(modelX, modelY, NODE_W, MODEL_H, 10, C.model.fill, C.model.stroke, 2.5);
    s += `<rect x="${modelX}" y="${modelY}" width="${NODE_W}" height="${MODEL_H}" rx="10" fill="none" stroke="${C.model.stroke}" stroke-width="1" opacity="0.15" filter="url(#ifm-glow)"/>`;
    // Use text glyph in SVG (img tags not supported inside SVG)
    s += _ifmServerIconSvg(srv, modelX + NODE_W / 2, modelMidY - 14, 36);
    s += txt(tr(srv.name,16), modelX+NODE_W/2, modelMidY+14, C.model.text, 11, '700', 'center');
    s += txt(_IFM.modelServer, modelX+NODE_W/2, modelMidY+26, C.model.sub, 8, '400', 'center');
    s += txt('async UDF', modelX+NODE_W/2, modelY-12, 'rgba(79,163,224,0.4)', 8, '400', 'center');
    if (hasPre) s += bezier(preRightX, preRightY, modelX, modelMidY, C.edge.model, 1.6);

    // Post-process node
    const modelRx = modelX + NODE_W;
    let outLeftX = modelRx, outLeftY = modelMidY;
    if (hasPost) {
        const ppX = X3, ppH = 40, ppY = modelMidY - ppH/2, ppMid = modelMidY;
        s += rr(ppX, ppY, NODE_W, ppH, 5, C.post.fill, C.post.stroke, 1.5);
        s += txt('⨍', ppX+8, ppMid, C.post.stroke, 13);
        s += txt('Post-process', ppX+24, ppMid-7, C.post.text, 10, '600');
        s += txt(tr(ic.postProcessExpr||'transform',14), ppX+24, ppMid+6, C.post.sub, 8);
        s += bezier(modelRx, modelMidY, ppX, ppMid, C.edge.pre, 1.4);
        outLeftX = ppX + NODE_W; outLeftY = ppMid;
    }

    // Output node
    const outH = 52, outY = midY - outH/2, outMid = midY;
    s += rr(X_OUT, outY, NODE_W, outH, 7, C.out.fill, C.out.stroke, 2.5);
    s += `<rect x="${X_OUT}" y="${outY}" width="${NODE_W}" height="${outH}" rx="7" fill="none" stroke="${C.out.stroke}" stroke-width="1" opacity="0.18" filter="url(#ifm-glow)"/>`;
    s += txt('▣', X_OUT+8, outMid, C.out.stroke, 14);
    s += txt(tr(alias,14), X_OUT+26, outMid-9, C.out.text, 11, '700');
    s += txt(`${ic.outputType||'DOUBLE'}`, X_OUT+26, outMid+3, 'rgba(0,212,170,0.6)', 9);
    s += txt(`→ ${tr(outTable,12)}`, X_OUT+26, outMid+15, C.out.sub, 8);
    if (!hasPost) s += bezier(modelRx, modelMidY, X_OUT, outMid, C.edge.out, 1.6);
    else s += bezier(outLeftX, outLeftY, X_OUT, outMid, C.edge.out, 1.6);

    // Passthrough annotation
    if (ptCols.length) {
        const ptY = outY + outH + 8;
        const ptH = 16 + ptCols.length * 14;
        s += rr(X_OUT, ptY, NODE_W, ptH, 4, C.pt.fill, C.pt.stroke, 1);
        s += txt('passthrough', X_OUT+8, ptY+10, C.pt.text, 8, '700');
        ptCols.slice(0,5).forEach((c,i) => { s += txt(`· ${c}`, X_OUT+10, ptY+22+i*13, '#6090c0', 9); });
    }

    // Async parallelism badge
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

function _ifmDrawCanvas() { _ifmDrawCanvasSvg(); }

// ─────────────────────────────────────────────────────────────────────────────
// SQL Generation (unchanged from original)
// ─────────────────────────────────────────────────────────────────────────────
function _ifmGenerateSql() {
    _ifmCollectSinkConfig();
    const src          = _IFM.sourceTable || 'source_table';
    const out          = _IFM.outputTable || (src + '_scored');
    const ic           = _IFM.inferenceConfig;
    const srv          = IFM_SERVERS.find(s => s.id === _IFM.modelServer) || IFM_SERVERS[0];
    const mc           = _IFM.modelConfig;
    const ac           = _IFM.authConfig;
    const sc           = _IFM.sinkConfig || {};
    const mthCfg       = _IFM.inferenceMethodConfig || {};
    const method       = _IFM.inferenceMethod || 'async_udf';
    const cols         = _IFM.inputColumns;
    const ptCols       = (ic.passthroughCols || '').split(',').map(c=>c.trim()).filter(Boolean);
    const inputCol     = ic.inputCol    || (cols[0]?.name || 'feature_col');
    const outputAlias  = ic.outputAlias || 'prediction';
    const outputType   = ic.outputType  || 'DOUBLE';
    const selCols      = _IFM.selectedInputCols.length ? _IFM.selectedInputCols : cols.map(c=>c.name);

    const q   = v => v ? `'${v}'` : "'<REQUIRED>'";
    const pad = (k, width) => k.padEnd(width || 36);

    const lines = [];
    lines.push('-- ═══════════════════════════════════════════════════════════════════════════');
    lines.push('-- Real-time ML Inference Pipeline');
    lines.push(`-- Model Server     : ${srv.name}`);
    lines.push(`-- Inference Method : ${IFM_INFERENCE_METHODS.find(m=>m.id===method)?.label || method}`);
    lines.push(`-- Auth             : ${_IFM.authType}`);
    lines.push(`-- Source Table     : ${src}`);
    lines.push(`-- Output Sink      : ${_IFM.sinkType} → ${out}`);
    lines.push(`-- Prediction       : ${outputAlias} (${outputType})`);
    lines.push(`-- Generated        : ${new Date().toISOString()}`);
    lines.push('-- Str:::lab Studio — Inference Manager');
    lines.push('-- ═══════════════════════════════════════════════════════════════════════════\n');

    lines.push(`SET 'execution.runtime-mode' = 'streaming';`);
    lines.push(`SET 'parallelism.default' = '${ic.parallelism || 4}';`);
    lines.push(`SET 'execution.checkpointing.interval' = '${ic.checkpointInterval || 10000}';\n`);

    // Source schema reference comment
    lines.push(`-- Source table schema reference (table already created in your session)`);
    lines.push(`-- CREATE TEMPORARY TABLE IF NOT EXISTS ${src} (`);
    cols.forEach(c => lines.push(`--   ${c.name.padEnd(30)} ${c.type},`));
    lines.push(`-- ) WITH ( ... );\n`);

    // ── Output sink DDL ──────────────────────────────────────────────────────
    // Skip sink DDL for Flink ML (uses ML_PREDICT which returns its own output)
    // and for udf_view when insert=no
    const skipSinkDDL = method === 'flink_ml'
        || (method === 'udf_view' && mthCfg['ifm-mth-view-insert'] === 'no');

    if (!skipSinkDDL) {
        lines.push(`-- ── Output table: ${out} (${_IFM.sinkType} sink) ──────────────────────────────`);
        lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} (`);
        ptCols.forEach(c => {
            const colDef = cols.find(col => col.name === c);
            lines.push(`  ${c.padEnd(30)} ${colDef?.type || 'STRING'},`);
        });
        if (!ptCols.includes(inputCol)) {
            const inputType = cols.find(c => c.name === inputCol)?.type || 'DOUBLE';
            lines.push(`  ${inputCol.padEnd(30)} ${inputType},`);
        }
        lines.push(`  ${outputAlias.padEnd(30)} ${outputType}`);
        lines.push(`) WITH (`);
        _ifmBuildSinkWith(lines, sc, out, _IFM.sinkType, pad, q);
        lines.push(`);\n`);
    }

    // ── Method-specific SQL ──────────────────────────────────────────────────
    lines.push(`-- ── Model Server Configuration ─────────────────────────────────────────────`);
    lines.push(`-- Server   : ${srv.name} (${_IFM.modelServer})`);
    const endpoint = mc['ifm-mc-endpoint'] || '';
    if (endpoint) lines.push(`-- Endpoint : ${endpoint}`);
    const modelName = mc['ifm-mc-model-name'] || '';
    if (modelName) lines.push(`-- Model    : ${modelName}`);
    _ifmServerConfigComment(lines, mc);
    lines.push('');

    switch (method) {

        // ── 1. Async Scalar UDF ───────────────────────────────────────────────
        case 'async_udf': {
            const udfName  = mthCfg['ifm-mth-udf-name']  || 'CALL_MODEL_UDF';
            const udfClass = mthCfg['ifm-mth-udf-class'] || `com.yourcompany.flink.udf.${_ifmUdfClassName(srv.name)}AsyncUDF`;
            const udfLang  = mthCfg['ifm-mth-udf-lang']  || 'JAVA';
            const udfJar   = mthCfg['ifm-mth-udf-jar']   || '';

            lines.push(`-- ── Async UDF settings ─────────────────────────────────────────────────────`);
            lines.push(`--   Timeout          : ${ic.timeoutMs || 5000}ms`);
            lines.push(`--   Retries          : ${ic.retries || 2}`);
            lines.push(`--   On error         : ${ic.onError || '-1.0'}`);
            lines.push(`--   Async parallelism: ${ic.asyncParallelism || 4}`);
            if (udfJar) lines.push(`--   JAR              : ${udfJar}`);
            lines.push(`-- ─────────────────────────────────────────────────────────────────────────\n`);

            lines.push(`-- Register the async inference UDF`);
            lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS ${udfName}`);
            lines.push(`  AS '${udfClass}'`);
            lines.push(`  LANGUAGE ${udfLang};\n`);

            const udfArgs = _ifmBuildUdfArgs(srv, mc, inputCol, ic);
            const selectItems = _ifmBuildSelectItems(ptCols, inputCol, ic, `${udfName}(${udfArgs}) AS ${outputAlias}`);

            lines.push(`-- ── Streaming inference pipeline ───────────────────────────────────────────`);
            lines.push(`-- Score each incoming record from ${src} using ${srv.name} (Async UDF)`);
            lines.push(`INSERT INTO ${out}`);
            lines.push(`SELECT`);
            lines.push(`  ` + selectItems.join(',\n  '));
            lines.push(`FROM ${src};\n`);
            break;
        }

        // ── 2. Flink ML Predict ───────────────────────────────────────────────
        case 'flink_ml': {
            const mlModelName = mthCfg['ifm-mth-flinkml-name'] || 'inference_model';
            const mlPath      = mthCfg['ifm-mth-flinkml-path'] || '/opt/flink/models/model.onnx';
            const mlFmt       = mthCfg['ifm-mth-flinkml-fmt']  || 'ONNX';
            const mlInputs    = mthCfg['ifm-mth-flinkml-input'] || selCols.join(', ');
            const mlOutput    = mthCfg['ifm-mth-flinkml-output']|| outputAlias;
            const flinkVer    = mthCfg['ifm-mth-flinkml-ver']  || '1.18';

            lines.push(`-- ── Flink ML Predict (native, Flink ${flinkVer}+) ─────────────────────────────`);
            lines.push(`-- Model format : ${mlFmt}`);
            lines.push(`-- Model path   : ${mlPath}`);
            lines.push(`-- Flink docs   : https://nightlies.apache.org/flink/flink-ml-docs-stable/\n`);

            lines.push(`-- Step 1: Register the model`);
            lines.push(`CREATE MODEL IF NOT EXISTS ${mlModelName}`);
            lines.push(`INPUT (${mlInputs})`);
            lines.push(`OUTPUT (${mlOutput} ${outputType})`);
            lines.push(`WITH (`);
            lines.push(`  'provider' = 'local',`);
            lines.push(`  'format'   = '${mlFmt}',`);
            lines.push(`  'path'     = '${mlPath}'`);
            lines.push(`);\n`);

            lines.push(`-- Step 2: Create output sink`);
            lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${out} (`);
            ptCols.forEach(c => {
                const colDef = cols.find(col => col.name === c);
                lines.push(`  ${c.padEnd(30)} ${colDef?.type || 'STRING'},`);
            });
            lines.push(`  ${mlOutput.padEnd(30)} ${outputType}`);
            lines.push(`) WITH (`);
            _ifmBuildSinkWith(lines, sc, out, _IFM.sinkType, pad, q);
            lines.push(`);\n`);

            lines.push(`-- Step 3: Run inference using ML_PREDICT`);
            lines.push(`-- Score each incoming record from ${src} using Flink ML (${mlFmt})`);
            lines.push(`INSERT INTO ${out}`);
            lines.push(`SELECT ${ptCols.length ? ptCols.join(', ') + ', ' : ''}${mlOutput}`);
            lines.push(`FROM ML_PREDICT(`);
            lines.push(`  TABLE ${src},`);
            lines.push(`  MODEL ${mlModelName}`);
            lines.push(`);\n`);
            break;
        }

        // ── 3. Otter-Streams Connector ─────────────────────────────────────────
        case 'otter_streams': {
            const otterFn    = mthCfg['ifm-mth-otter-fn']    || 'OTTER_ML_PREDICT';
            const otterEp    = mthCfg['ifm-mth-otter-ep']    || endpoint;
            const otterVer   = mthCfg['ifm-mth-otter-ver']   || 'latest';
            const otterReg   = mthCfg['ifm-mth-otter-reg']   || 'yes';
            const otterClass = mthCfg['ifm-mth-otter-class'] || 'com.otterstreams.flink.udf.OtterMLPredict';
            const otterJar   = mthCfg['ifm-mth-otter-jar']   || '/opt/flink/lib/otter-streams-connector.jar';

            lines.push(`-- ── Otter-Streams ML Inference Connector ───────────────────────────────────`);
            lines.push(`-- Function     : ${otterFn}`);
            lines.push(`-- Endpoint     : ${otterEp || '(from model config)'}`);
            lines.push(`-- Model version: ${otterVer}`);
            lines.push(`-- ─────────────────────────────────────────────────────────────────────────\n`);

            if (otterReg === 'no') {
                lines.push(`-- Register Otter-Streams ML function`);
                lines.push(`-- Add the connector JAR to your Flink classpath: ${otterJar}`);
                lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS ${otterFn}`);
                lines.push(`  AS '${otterClass}'`);
                lines.push(`  LANGUAGE JAVA;\n`);
            } else {
                lines.push(`-- ${otterFn} already registered in session — skipping CREATE FUNCTION\n`);
            }

            const endpointArg = otterEp ? `, '${otterEp}'` : ``;
            const selectItems = _ifmBuildSelectItems(
                ptCols, inputCol, ic,
                `${otterFn}(ARRAY[${selCols.join(', ')}], '${modelName}'${endpointArg}, '${otterVer}') AS ${outputAlias}`
            );

            lines.push(`-- ── Streaming inference pipeline (Otter-Streams) ────────────────────────────`);
            lines.push(`INSERT INTO ${out}`);
            lines.push(`SELECT`);
            lines.push(`  ` + selectItems.join(',\n  '));
            lines.push(`FROM ${src};\n`);
            break;
        }

        // ── 4. UDF View Pattern ────────────────────────────────────────────────
        case 'udf_view': {
            const viewName   = mthCfg['ifm-mth-view-name']   || (src + '_scored_v');
            const viewUdf    = mthCfg['ifm-mth-view-udf']    || 'CALL_MODEL_UDF';
            const viewClass  = mthCfg['ifm-mth-view-class']  || `com.yourcompany.flink.udf.${_ifmUdfClassName(srv.name)}AsyncUDF`;
            const withInsert = (mthCfg['ifm-mth-view-insert'] || 'yes') === 'yes';

            lines.push(`-- ── UDF View Pattern ────────────────────────────────────────────────────────`);
            lines.push(`-- View        : ${viewName}`);
            lines.push(`-- Underlying  : ${viewUdf}`);
            lines.push(`-- ─────────────────────────────────────────────────────────────────────────\n`);

            if (viewClass) {
                lines.push(`-- Register underlying UDF`);
                lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS ${viewUdf}`);
                lines.push(`  AS '${viewClass}'`);
                lines.push(`  LANGUAGE JAVA;\n`);
            }

            const udfArgs    = _ifmBuildUdfArgs(srv, mc, inputCol, ic);
            const viewCols   = [...(ptCols.length ? ptCols : cols.map(c=>c.name)),
                ...(ptCols.includes(inputCol)?[]:[inputCol])];
            const viewSelect = [...viewCols, `${viewUdf}(${udfArgs}) AS ${outputAlias}`];

            lines.push(`-- Create reusable scored view`);
            lines.push(`CREATE TEMPORARY VIEW IF NOT EXISTS ${viewName} AS`);
            lines.push(`SELECT`);
            lines.push(`  ` + viewSelect.join(',\n  '));
            lines.push(`FROM ${src};\n`);

            lines.push(`-- ── Query the view directly for ad-hoc analysis ────────────────────────────`);
            lines.push(`-- SELECT * FROM ${viewName} WHERE ${outputAlias} > 0.85;\n`);

            if (withInsert) {
                lines.push(`-- ── Stream view into sink ───────────────────────────────────────────────────`);
                lines.push(`INSERT INTO ${out}`);
                lines.push(`SELECT ${ptCols.length ? ptCols.join(', ') + ', ' : ''}${outputAlias}`);
                lines.push(`FROM ${viewName};\n`);
            }
            break;
        }

        // ── 5. Custom Function ─────────────────────────────────────────────────
        case 'custom_function': {
            const customFn      = mthCfg['ifm-mth-custom-fn']       || 'MY_PREDICT_FUNCTION';
            const customReg     = mthCfg['ifm-mth-custom-reg']      || 'yes';
            const customPre     = mthCfg['ifm-mth-custom-preamble'] || '';

            lines.push(`-- ── Custom Function ─────────────────────────────────────────────────────────\n`);

            if (customPre.trim()) {
                lines.push(`-- Custom preamble SQL`);
                lines.push(customPre.trim() + '\n');
            } else if (customReg === 'no') {
                lines.push(`-- Register custom function (fill in class name)`);
                lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS ${customFn}`);
                lines.push(`  AS 'com.yourcompany.YourUDFClass'`);
                lines.push(`  LANGUAGE JAVA;\n`);
            }

            const selectItems = _ifmBuildSelectItems(
                ptCols, inputCol, ic,
                `${customFn}(${selCols.join(', ')}) AS ${outputAlias}`
            );

            lines.push(`-- ── Streaming inference pipeline ───────────────────────────────────────────`);
            lines.push(`INSERT INTO ${out}`);
            lines.push(`SELECT`);
            lines.push(`  ` + selectItems.join(',\n  '));
            lines.push(`FROM ${src};\n`);
            break;
        }

        default:
            lines.push(`-- Unknown inference method: ${method}`);
    }

    return lines.join('\n');
}

// Helper: build sink WITH clause lines (extracted to avoid duplication)
function _ifmBuildSinkWith(lines, sc, out, sinkType, pad, q) {
    switch (sinkType) {
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
                lines[lines.length-1] += ',';
                lines.push(`  ${pad("'properties.security.protocol'")} = '${security}'`);
                const user = sc['ifm-sk-sasl-user'], pass = sc['ifm-sk-sasl-pass'];
                if (user && pass) {
                    lines[lines.length-1] += ',';
                    lines.push(`  ${pad("'properties.sasl.mechanism'")} = 'PLAIN',`);
                    lines.push(`  ${pad("'properties.sasl.jaas.config'")} = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${user}" password="${pass}";'`);
                }
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
            lines.push(`  ${pad("'connector'")} = 'filesystem',`);
            lines.push(`  ${pad("'path'")}      = ${q(path)},`);
            lines.push(`  ${pad("'format'")}    = '${format}'`);
            if (roll) { lines[lines.length-1] += ','; lines.push(`  ${pad("'sink.rolling-policy.rollover-interval'")} = '${roll}'`); }
            break;
        }
        case 'iceberg': {
            const cat  = sc['ifm-sk-ice-cat']    || 'iceberg_catalog';
            const tbl  = sc['ifm-sk-ice-tbl']    || out;
            const type = sc['ifm-sk-ice-cattype']|| 'hive';
            const wh   = sc['ifm-sk-ice-wh']     || '';
            const [db, tblName] = tbl.includes('.') ? tbl.split('.') : ['default', tbl];
            lines.push(`  ${pad("'connector'")}         = 'iceberg',`);
            lines.push(`  ${pad("'catalog-name'")}      = ${q(cat)},`);
            lines.push(`  ${pad("'catalog-database'")}  = ${q(db)},`);
            lines.push(`  ${pad("'catalog-table'")}     = ${q(tblName)},`);
            lines.push(`  ${pad("'catalog-type'")}      = '${type}'`);
            if (wh) { lines[lines.length-1] += ','; lines.push(`  ${pad("'warehouse'")} = ${q(wh)}`); }
            break;
        }
        case 'print':     lines.push(`  'connector' = 'print'`);     break;
        case 'blackhole': lines.push(`  'connector' = 'blackhole'`); break;
        default:          lines.push(`  'connector' = '${sinkType}'`);
    }
}

// Helper: build SELECT item list with passthrough + pre/post processing
function _ifmBuildSelectItems(ptCols, inputCol, ic, inferenceExpr) {
    const items = [];
    ptCols.forEach(c => items.push(c));
    if (ic.preProcessExpr?.trim()) {
        items.push(`${ic.preProcessExpr.trim()} AS ${inputCol}_processed`);
    } else if (!ptCols.includes(inputCol)) {
        items.push(inputCol);
    }
    if (ic.postProcessExpr?.trim()) {
        items.push(ic.postProcessExpr.trim());
    } else {
        items.push(inferenceExpr);
    }
    return items;
}

function _ifmServerConfigComment(lines, mc) {
    const ep = mc['ifm-mc-endpoint'] || '';
    switch (_IFM.modelServer) {
        case 'mlflow': case 'mlflow_serve': if(ep)lines.push(`--   MLflow URI  : ${ep}`); if(mc['ifm-mc-model-uri'])lines.push(`--   Model URI   : ${mc['ifm-mc-model-uri']}`); if(mc['ifm-mc-mlflow-flavour'])lines.push(`--   Flavour     : ${mc['ifm-mc-mlflow-flavour']}`); break;
        case 'sagemaker': lines.push(`--   Endpoint    : ${ep}`); lines.push(`--   Region      : ${mc['ifm-mc-aws-region']||''}`); if(mc['ifm-mc-content-type'])lines.push(`--   Content-Type: ${mc['ifm-mc-content-type']}`); break;
        case 'azureml': if(ep)lines.push(`--   AzureML URL : ${ep}`); if(mc['ifm-mc-deployment'])lines.push(`--   Deployment  : ${mc['ifm-mc-deployment']}`); break;
        case 'vertexai': lines.push(`--   GCP Project  : ${mc['ifm-mc-gcp-project']||''}`); lines.push(`--   Region       : ${mc['ifm-mc-gcp-region']||''}`); lines.push(`--   Endpoint ID  : ${ep}`); break;
        case 'openai': case 'openai_compat': case 'mistral': case 'together': if(ep)lines.push(`--   Base URL    : ${ep}`); lines.push(`--   Model       : ${mc['ifm-mc-model-name']||''}`); if(mc['ifm-mc-max-tokens'])lines.push(`--   Max tokens  : ${mc['ifm-mc-max-tokens']}`); if(mc['ifm-mc-temperature'])lines.push(`--   Temperature : ${mc['ifm-mc-temperature']}`); break;
        case 'anthropic': lines.push(`--   Model       : ${mc['ifm-mc-model-name']||'claude-sonnet-4-6'}`); if(mc['ifm-mc-max-tokens'])lines.push(`--   Max tokens  : ${mc['ifm-mc-max-tokens']}`); if(mc['ifm-mc-anthropic-ver'])lines.push(`--   API version : ${mc['ifm-mc-anthropic-ver']}`); break;
        case 'cohere': if(ep)lines.push(`--   API URL     : ${ep}`); lines.push(`--   Endpoint    : ${mc['ifm-mc-cohere-ep']||'/v2/classify'}`); if(mc['ifm-mc-model-name'])lines.push(`--   Model       : ${mc['ifm-mc-model-name']}`); break;
        case 'bedrock': lines.push(`--   Region      : ${mc['ifm-mc-aws-region']||''}`); lines.push(`--   Model ID    : ${mc['ifm-mc-model-name']||''}`); break;
        case 'triton': if(ep)lines.push(`--   Triton URL  : ${ep}`); lines.push(`--   Model name  : ${mc['ifm-mc-model-name']||''}`); lines.push(`--   Version     : ${mc['ifm-mc-model-version']||'1'}`); if(mc['ifm-mc-triton-dtype'])lines.push(`--   DType       : ${mc['ifm-mc-triton-dtype']}`); break;
        case 'torchserve': if(ep)lines.push(`--   TorchServe  : ${ep}`); lines.push(`--   Model name  : ${mc['ifm-mc-model-name']||''}`); break;
        case 'tfserving': if(ep)lines.push(`--   TF Serving  : ${ep}`); lines.push(`--   Model name  : ${mc['ifm-mc-model-name']||''}`); if(mc['ifm-mc-tf-sig'])lines.push(`--   Signature   : ${mc['ifm-mc-tf-sig']}`); break;
        case 'minio': if(ep)lines.push(`--   MinIO URL   : ${ep}`); if(mc['ifm-mc-minio-bucket'])lines.push(`--   Bucket      : ${mc['ifm-mc-minio-bucket']}`); if(mc['ifm-mc-minio-path'])lines.push(`--   Object path : ${mc['ifm-mc-minio-path']}`); if(mc['ifm-mc-minio-fmt'])lines.push(`--   Format      : ${mc['ifm-mc-minio-fmt']}`); if(mc['ifm-mc-cache-ttl'])lines.push(`--   Cache TTL   : ${mc['ifm-mc-cache-ttl']}s`); break;
        case 'huggingface': if(ep)lines.push(`--   HF Endpoint : ${ep}`); if(mc['ifm-mc-hf-task'])lines.push(`--   Task        : ${mc['ifm-mc-hf-task']}`); break;
        case 'custom_http': if(ep)lines.push(`--   URL         : ${ep}`); if(mc['ifm-mc-http-method'])lines.push(`--   Method      : ${mc['ifm-mc-http-method']}`); if(mc['ifm-mc-json-path'])lines.push(`--   JSON path   : ${mc['ifm-mc-json-path']}`); if(mc['ifm-mc-req-template'])lines.push(`--   Body tmpl   : ${mc['ifm-mc-req-template']}`); if(mc['ifm-mc-custom-headers'])mc['ifm-mc-custom-headers'].split('\n').filter(Boolean).forEach(h=>lines.push(`--   Header      : ${h}`)); break;
        case 'custom_grpc': if(ep)lines.push(`--   gRPC target : ${ep}`); if(mc['ifm-mc-grpc-method'])lines.push(`--   Method      : ${mc['ifm-mc-grpc-method']}`); if(mc['ifm-mc-grpc-tls'])lines.push(`--   TLS mode    : ${mc['ifm-mc-grpc-tls']}`); break;
        default: if(ep)lines.push(`--   Endpoint    : ${ep}`);
    }
}

function _ifmBuildUdfArgs(srv, mc, inputCol, ic) {
    const selCols = _IFM.selectedInputCols.length ? _IFM.selectedInputCols.join(', ') : inputCol;
    switch (srv.id) {
        case 'openai': case 'anthropic': case 'cohere': case 'mistral': case 'together': case 'openai_compat': { const model=mc['ifm-mc-model-name']||'model',prompt=mc['ifm-mc-system-prompt']||'',tokens=mc['ifm-mc-max-tokens']||'64'; return `CAST(${inputCol} AS STRING), '${model}', '${prompt.replace(/'/g,"\\'")}', ${tokens}`; }
        case 'bedrock': { const region=mc['ifm-mc-aws-region']||'us-east-1',model=mc['ifm-mc-model-name']||'anthropic.claude-sonnet-4-6'; return `CAST(${inputCol} AS STRING), '${model}', '${region}'`; }
        case 'sagemaker': { const ep=mc['ifm-mc-endpoint']||'<endpoint>'; return `${selCols}, '${ep}'`; }
        case 'triton': return `ARRAY[${selCols}], '${mc['ifm-mc-model-name']||'model'}', '${mc['ifm-mc-model-version']||'1'}'`;
        default: return selCols;
    }
}

function _ifmUdfClassName(name) { return (name || 'Model').replace(/[^a-zA-Z0-9]/g, ''); }

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
// History (unchanged)
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
        const glyph = _ifmServerCanvasGlyph(h.modelServer);
        return `
  <div class="ifm-hist-item" onclick="_ifmLoadFromHistory(${i})">
    <div style="flex:1;min-width:0;">
      <div style="font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);">${glyph} ${h.sourceTable} → ${h.outputTable}</div>
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
  </div>`; }).join('')}
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

// ── Modal Expand / Fit to Screen ─────────────────────────────────────────────
const _IFMX = { expanded: false, origModalCss: '', origOverlayCss: '' };

function _ifmToggleModalExpand() {
    const overlay = document.getElementById('ifm-modal');
    const modal   = overlay?.querySelector('.modal');
    const btn     = document.getElementById('ifm-modal-expand-btn');
    if (!overlay || !modal || !btn) return;

    if (!_IFMX.expanded) {
        // Save current inline styles
        _IFMX.origModalCss   = modal.getAttribute('style')   || '';
        _IFMX.origOverlayCss = overlay.getAttribute('style') || '';
        _IFMX.expanded = true;

        // Expand the overlay to fill the viewport
        overlay.style.cssText = (
            _IFMX.origOverlayCss +
            ';padding:0!important;align-items:stretch!important;justify-content:stretch!important;'
        );
        // Expand the modal inner box to fill all available space
        modal.style.cssText = (
            _IFMX.origModalCss +
            ';width:100vw!important;max-width:100vw!important;' +
            'height:100vh!important;max-height:100vh!important;' +
            'border-radius:0!important;margin:0!important;'
        );
        btn.textContent = '⤡';
        btn.title = 'Restore window size';

        // Re-fit canvas if on the review step
        setTimeout(() => { if (typeof _ifmCanvasFitToView === 'function') _ifmCanvasFitToView(); }, 80);
    } else {
        _IFMX.expanded = false;

        // Restore original styles
        if (_IFMX.origModalCss)   modal.setAttribute('style', _IFMX.origModalCss);
        else                       modal.removeAttribute('style');
        if (_IFMX.origOverlayCss) overlay.setAttribute('style', _IFMX.origOverlayCss);
        else                       overlay.removeAttribute('style');

        btn.textContent = '⤢';
        btn.title = 'Expand to fit screen';

        setTimeout(() => { if (typeof _ifmCanvasFitToView === 'function') _ifmCanvasFitToView(); }, 80);
    }
}

// ── Export / Import Pipeline ─────────────────────────────────────────────────
function _ifmExportPipeline() {
    _ifmCollectCurrentStep();
    const payload = {
        _version: 1,
        _exported: new Date().toISOString(),
        _tool: 'Str:::lab Studio — Inference Manager',
        sourceTable:          _IFM.sourceTable,
        inputColumns:         _IFM.inputColumns,
        selectedInputCols:    _IFM.selectedInputCols,
        modelServer:          _IFM.modelServer,
        modelConfig:          _IFM.modelConfig,
        authType:             _IFM.authType,
        authConfig:           _IFM.authConfig,
        inferenceConfig:      _IFM.inferenceConfig,
        inferenceMethod:      _IFM.inferenceMethod,
        inferenceMethodConfig:_IFM.inferenceMethodConfig,
        outputTable:          _IFM.outputTable,
        sinkType:             _IFM.sinkType,
        sinkConfig:           _IFM.sinkConfig || {},
        generatedSql:         _IFM.generatedSql,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    const safeName = (_IFM.sourceTable || 'pipeline').replace(/[^a-zA-Z0-9_-]/g, '_');
    a.href     = url;
    a.download = `ifm_${safeName}_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
    if (typeof toast === 'function') toast('Pipeline exported as JSON', 'ok');
}

function _ifmImportPipeline(input) {
    const file = input.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = e => {
        try {
            const data = JSON.parse(e.target.result);
            if (!data._version || !data.sourceTable) throw new Error('Invalid pipeline file');
            const FIELDS = [
                'sourceTable','inputColumns','selectedInputCols','modelServer',
                'modelConfig','authType','authConfig','inferenceConfig',
                'inferenceMethod','inferenceMethodConfig','outputTable',
                'sinkType','sinkConfig','generatedSql',
            ];
            FIELDS.forEach(k => { if (data[k] !== undefined) _IFM[k] = data[k]; });
            input.value = '';
            _ifmGoStep(0);
            if (typeof toast === 'function') toast(`Pipeline imported: ${data.sourceTable} → ${data.outputTable || '?'}`, 'ok');
        } catch (err) {
            if (typeof toast === 'function') toast('Import failed: ' + err.message, 'error');
            console.error('IFM import error:', err);
        }
    };
    reader.readAsText(file);
}

function _escIfm(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}