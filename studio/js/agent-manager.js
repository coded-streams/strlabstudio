/**
 * agent-manager.js  —  Str:::lab Studio
 * ─────────────────────────────────────────────────────────────────────────────
 * Flink Agent Manager  (Apache Flink Agents 0.2)
 *
 * Icons follow the SAME convention as pipeline-manager.js:
 *   - Brands with Simple Icons: _agSimpleIcon(slug, hex)
 *   - Everything else: clean monochrome inline SVG — NO colored emoji
 *
 * Fixes vs previous version:
 *   1. All operator icons: Simple Icons CDN or mono SVG — zero emoji
 *   2. Node cards have ✏ edit button (identical to PLM)
 *   3. Modal header has ⤢ expand-to-fullscreen / ⤡ restore button
 *   4. Observability tab: real Kafka via SQL Gateway ONLY — no simulation
 *   5. Palette search box (same as PLM)
 * ─────────────────────────────────────────────────────────────────────────────
 */

/* ── Simple Icons CDN helper (mirrors _plmSimpleIcon exactly) ─────────────── */
function _agSimpleIcon(slug, hex, size) {
    size = size || 16;
    return '<img src="https://cdn.simpleicons.org/' + slug + '/' + hex
        + '" width="' + size + '" height="' + size
        + '" style="display:inline-block;vertical-align:middle;flex-shrink:0;" />';
}

/* ── Monochrome SVG helper — stroke only, no fill, no colour ─────────────── */
function _agSvgIcon(paths, size) {
    size = size || 16;
    return '<svg width="' + size + '" height="' + size
        + '" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">'
        + paths + '</svg>';
}

/* ── Icon map ─────────────────────────────────────────────────────────────── */
// Simple Icons slugs: apachekafka, postgresql, amazons3, amazonwebservices,
// openai, anthropic, microsoftazure, amazonaws, ollama, mistralai,
// huggingface, elasticsearch, redis, apachepulsar
// Everything else → inline SVG (no emoji)
const _AG_ICONS = {
    kafka_event:        _agSimpleIcon('apachekafka','ffffff'),
    datagen_event:      _agSvgIcon('<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>'),
    jdbc_event:         _agSimpleIcon('postgresql','ffffff'),
    kinesis_event:      _agSimpleIcon('amazonwebservices','FF9900'),
    pulsar_event:       _agSimpleIcon('apachepulsar','ffffff'),
    filesystem_event:   _agSimpleIcon('amazons3','FF9900'),
    llm_openai:         _agSimpleIcon('openai','ffffff'),
    llm_anthropic:      _agSimpleIcon('anthropic','D4A27F'),
    llm_azureai:        _agSimpleIcon('microsoftazure','ffffff'),
    llm_bedrock:        _agSimpleIcon('amazonaws','FF9900'),
    llm_ollama:         _agSimpleIcon('ollama','ffffff'),
    llm_mistral:        _agSimpleIcon('mistralai','FF7000'),
    llm_vertexai:       _agSimpleIcon('googlecloud','4285F4'),
    llm_cohere:         _agSvgIcon('<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="4"/><circle cx="17" cy="7" r="2" fill="currentColor"/>'),
    embed_openai:       _agSimpleIcon('openai','ffffff'),
    embed_ollama:       _agSimpleIcon('ollama','ffffff'),
    embed_hf:           _agSimpleIcon('huggingface','FF9D00'),
    embed_azure:        _agSimpleIcon('microsoftazure','ffffff'),
    vs_elasticsearch:   _agSimpleIcon('elasticsearch','ffffff'),
    vs_redis:           _agSimpleIcon('redis','ffffff'),
    vs_milvus:          _agSvgIcon('<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v5c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 10v4c0 1.7 3.6 3 8 3s8-1.3 8-3v-4"/>'),
    vs_qdrant:          _agSvgIcon('<polygon points="12 2 20 7 20 17 12 22 4 17 4 7"/><polygon points="12 7 17 10 17 14 12 17 7 14 7 10"/>'),
    vs_opensearch:      _agSimpleIcon('elasticsearch','ffffff'),
    mem_sensory:        _agSvgIcon('<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M4.22 4.22l2.12 2.12M17.66 17.66l2.12 2.12M2 12h3M19 12h3M4.22 19.78l2.12-2.12M17.66 6.34l2.12-2.12"/>'),
    mem_short:          _agSvgIcon('<rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>'),
    mem_long:           _agSvgIcon('<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.7 4 3 9 3s9-1.3 9-3V5"/><path d="M3 12c0 1.7 4 3 9 3s9-1.3 9-3"/>'),
    prompt_template:    _agSvgIcon('<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="13" x2="15" y2="13"/><line x1="9" y1="17" x2="15" y2="17"/>'),
    few_shot:           _agSvgIcon('<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/>'),
    tool_http:          _agSvgIcon('<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>'),
    tool_kafka_produce: _agSimpleIcon('apachekafka','ffffff'),
    tool_sql:           _agSvgIcon('<polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>'),
    mcp_server:         _agSvgIcon('<circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/><line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/>'),
    agent_workflow:     _agSvgIcon('<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><line x1="10" y1="6.5" x2="14" y2="6.5"/><line x1="17.5" y1="10" x2="17.5" y2="14"/>'),
    agent_react:        _agSvgIcon('<circle cx="12" cy="12" r="3"/><circle cx="12" cy="12" r="8" stroke-dasharray="3 2"/><path d="M12 2v3M12 19v3M4.22 4.22l2.12 2.12M17.66 17.66l2.12 2.12M2 12h3M19 12h3"/>'),
    agent_multi:        _agSvgIcon('<circle cx="8" cy="8" r="4"/><circle cx="16" cy="16" r="4"/><path d="M12 12l-1.5-1.5M12 12l1.5 1.5"/><path d="M8 12v1a3 3 0 0 0 3 3h1"/>'),
    action_kafka:       _agSimpleIcon('apachekafka','ffffff'),
    action_jdbc:        _agSimpleIcon('postgresql','ffffff'),
    action_http:        _agSvgIcon('<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>'),
    action_elasticsearch: _agSimpleIcon('elasticsearch','ffffff'),
    event_log:          _agSvgIcon('<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><line x1="9" y1="13" x2="15" y2="13"/><line x1="9" y1="17" x2="15" y2="17"/><polyline points="14 2 14 8 20 8"/>'),
    vector_search:      _agSvgIcon('<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><circle cx="11" cy="11" r="3" stroke-dasharray="2 1"/>'),
    sink_print:         _agSvgIcon('<polyline points="6 9 6 2 18 2 18 9"/><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"/><rect x="6" y="14" width="12" height="8"/>'),
    sink_blackhole:     _agSvgIcon('<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="4"/><circle cx="12" cy="12" r="1" fill="currentColor"/>'),
};

function _agIcon(opId) {
    return _AG_ICONS[opId] || _agSvgIcon('<rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="21" x2="9" y2="9"/>');
}
/* ── Helpers ──────────────────────────────────────────────────────────────── */
function _agEsc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
function _agTr(s,n){return(s||'').length>n?String(s).slice(0,n)+'…':String(s||'');}
function _agUID(){return'ag'+(++window._agUIDCounter);}
window._agUIDCounter=window._agUIDCounter||0;

/* ── CSS injection ────────────────────────────────────────────────────────── */
function _agInjectCss(){
    if(document.getElementById('ag-css'))return;
    const s=document.createElement('style');s.id='ag-css';
    s.textContent=`
#ag-modal .modal{width:min(1300px,97vw);height:92vh;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;background:var(--bg1);}
#ag-modal .modal-header{background:rgba(0,176,143,0.07);border-bottom:1px solid rgba(0,176,143,0.25);}
#ag-tab-bar{display:flex;gap:0;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;}
.ag-tab{padding:8px 14px;font-size:11px;font-weight:500;background:transparent;border:none;border-bottom:2px solid transparent;color:var(--text2);cursor:pointer;font-family:var(--mono);white-space:nowrap;transition:all 0.14s;}
.ag-tab:hover{color:var(--text0);}
.ag-tab.active{color:#00c4a0;border-bottom-color:#00c4a0;font-weight:700;}
/* Welcome */
#ag-welcome{flex:1;overflow-y:auto;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px;}
.ag-welcome-hero{text-align:center;margin-bottom:28px;}
.ag-welcome-hero h1{font-size:26px;font-weight:700;color:var(--text0);font-family:var(--mono);margin:0 0 6px;}
.ag-welcome-hero h1 span{color:#00c4a0;}
.ag-welcome-hero p{font-size:13px;color:var(--text2);max-width:560px;line-height:1.75;margin:0 auto;}
.ag-mode-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;max-width:860px;width:100%;margin-bottom:28px;}
.ag-mode-card{padding:20px 18px;border:1.5px solid var(--border2);border-radius:8px;background:var(--bg2);cursor:pointer;transition:all 0.15s;text-align:left;}
.ag-mode-card:hover{border-color:#00c4a0;background:rgba(0,176,143,0.07);transform:translateY(-2px);}
.ag-mode-card.selected{border-color:#00c4a0;background:rgba(0,176,143,0.1);}
.ag-mode-card .ag-mc-icon{display:flex;align-items:center;height:28px;margin-bottom:10px;color:#00c4a0;}
.ag-mode-card .ag-mc-title{font-size:13px;font-weight:700;color:var(--text0);margin-bottom:5px;font-family:var(--mono);}
.ag-mode-card .ag-mc-desc{font-size:11px;color:var(--text3);line-height:1.6;}
.ag-mode-card .ag-mc-badge{display:inline-block;font-size:9px;font-weight:700;padding:1px 7px;border-radius:10px;margin-top:8px;background:rgba(0,176,143,0.12);color:#00c4a0;border:1px solid rgba(0,176,143,0.3);}
.ag-template-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;max-width:860px;width:100%;}
.ag-tmpl-card{padding:11px 13px;border:1px solid var(--border);border-radius:6px;cursor:pointer;background:var(--bg3);transition:all 0.12s;}
.ag-tmpl-card:hover{border-color:#00c4a0;background:rgba(0,176,143,0.07);}
.ag-tmpl-card .ag-tc-icon{display:flex;align-items:center;height:20px;margin-bottom:5px;color:#00c4a0;}
.ag-tmpl-card .ag-tc-name{font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);}
.ag-tmpl-card .ag-tc-desc{font-size:9px;color:var(--text3);margin-top:2px;line-height:1.4;}
/* Canvas */
#ag-canvas-pane{flex:1;display:flex;overflow:hidden;}
#ag-palette{width:185px;flex-shrink:0;background:var(--bg2);border-right:1px solid var(--border);overflow-y:auto;padding:4px 3px;}
.ag-pal-group-label{font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--text3);padding:5px 8px 2px;}
.ag-pal-item{display:flex;align-items:center;gap:6px;padding:5px 7px;border-radius:3px;cursor:grab;font-size:11px;color:var(--text1);user-select:none;transition:background 0.1s;}
.ag-pal-item:hover{background:rgba(0,176,143,0.08);}
.ag-pal-item .ag-pi-icon{flex-shrink:0;display:flex;align-items:center;width:20px;}
.ag-pal-item .ag-pi-label{flex:1;font-family:var(--mono);font-size:10px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
#ag-canvas-wrap{flex:1;position:relative;overflow:hidden;background:var(--bg0);}
/* Nodes — identical structure to .plm-node */
.ag-node{position:absolute;border-radius:6px;cursor:pointer;box-shadow:0 3px 12px rgba(0,0,0,0.45);transition:box-shadow 0.12s;user-select:none;font-family:var(--mono);font-size:11px;border:2px solid rgba(255,255,255,0.12);}
.ag-node:hover{box-shadow:0 5px 20px rgba(0,0,0,0.6);}
.ag-node.selected{border-color:rgba(255,255,255,0.65)!important;box-shadow:0 0 0 3px rgba(255,255,255,0.15),0 5px 20px rgba(0,0,0,0.6)!important;}
.ag-node svg,.ag-node span,.ag-node div,.ag-node img{pointer-events:none;}
.ag-node button.ag-node-del{pointer-events:auto!important;}
.ag-node .ag-port{pointer-events:auto!important;}
.ag-port{position:absolute;width:10px;height:10px;border-radius:50%;background:rgba(255,255,255,0.3);border:2px solid rgba(255,255,255,0.7);cursor:crosshair;transition:all 0.12s;z-index:5;}
.ag-port:hover{background:white;transform:scale(1.5);}
.ag-port.out{right:-6px;top:50%;transform:translateY(-50%);}
.ag-port.in{left:-6px;top:50%;transform:translateY(-50%);}
.ag-port.out:hover{transform:translateY(-50%) scale(1.5);}
.ag-port.in:hover{transform:translateY(-50%) scale(1.5);}
/* Toolbar */
#ag-canvas-toolbar{display:flex;align-items:center;gap:4px;padding:5px 10px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;}
.ag-tb-btn{padding:4px 9px;font-size:10px;background:var(--bg3);border:1px solid var(--border);color:var(--text2);border-radius:3px;cursor:pointer;font-family:var(--mono);display:flex;align-items:center;gap:3px;white-space:nowrap;font-weight:500;}
.ag-tb-btn:hover{background:var(--bg2);color:var(--text0);}
.ag-tb-btn.purple{background:rgba(0,176,143,0.12);border-color:rgba(0,176,143,0.4);color:#00c4a0;}
.ag-tb-btn.green{background:rgba(99,201,150,0.1);border-color:rgba(99,201,150,0.3);color:var(--green);}
.ag-tb-btn.red{background:rgba(255,77,109,0.08);border-color:rgba(255,77,109,0.3);color:var(--red);}
/* Code */
#ag-code-pane{flex:1;display:flex;overflow:hidden;}
.ag-code-tabs{display:flex;gap:0;border-right:1px solid var(--border);background:var(--bg2);flex-direction:column;width:160px;flex-shrink:0;padding:6px 4px;}
.ag-code-tab{padding:7px 10px;font-size:10px;font-family:var(--mono);border:none;border-radius:3px;background:transparent;color:var(--text2);cursor:pointer;text-align:left;margin-bottom:2px;}
.ag-code-tab:hover{background:rgba(0,176,143,0.08);color:var(--text0);}
.ag-code-tab.active{background:rgba(0,176,143,0.15);color:#00c4a0;font-weight:700;}
#ag-code-output{flex:1;overflow:auto;padding:14px 16px;font-family:var(--mono);font-size:11px;line-height:1.7;color:var(--text1);background:var(--bg0);white-space:pre;}
/* Config modal */
#ag-cfg-modal{position:fixed;z-index:10003;background:var(--bg2);border:1px solid rgba(0,176,143,0.3);border-radius:8px;box-shadow:0 12px 48px rgba(0,0,0,0.7);width:460px;max-height:88vh;display:flex;flex-direction:column;overflow:hidden;}
.ag-cfg-header{padding:11px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:9px;cursor:move;}
.ag-cfg-body{flex:1;overflow-y:auto;padding:14px;}
.ag-cfg-footer{padding:10px 14px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end;background:var(--bg1);}
/* Wizard */
#ag-wizard-pane{flex:1;display:flex;overflow:hidden;}
#ag-wiz-steps{width:200px;flex-shrink:0;border-right:1px solid var(--border);background:var(--bg2);padding:14px 8px;overflow-y:auto;}
.ag-wiz-step{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:4px;cursor:pointer;margin-bottom:3px;transition:all 0.12s;}
.ag-wiz-step:hover{background:rgba(0,176,143,0.08);}
.ag-wiz-step.active{background:rgba(0,176,143,0.15);}
.ag-wiz-step .ag-ws-num{width:20px;height:20px;border-radius:50%;font-size:9px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;background:var(--bg3);color:var(--text3);}
.ag-wiz-step.active .ag-ws-num{background:#00c4a0;color:#000;}
.ag-wiz-step.done .ag-ws-num{background:var(--green);color:#000;}
.ag-wiz-step .ag-ws-label{font-size:11px;font-family:var(--mono);color:var(--text1);}
.ag-wiz-step.active .ag-ws-label{color:#00c4a0;font-weight:700;}
#ag-wiz-body{flex:1;overflow-y:auto;padding:20px 24px;}
#ag-wiz-footer{flex-shrink:0;padding:11px 20px;border-top:1px solid var(--border);background:var(--bg2);display:flex;align-items:center;gap:8px;}
.ag-info{background:rgba(0,176,143,0.05);border:1px solid rgba(0,176,143,0.2);border-left:3px solid #00c4a0;border-radius:3px;padding:8px 12px;font-size:11px;color:var(--text1);line-height:1.7;margin-bottom:12px;}
.ag-warn{background:rgba(245,166,35,0.06);border:1px solid rgba(245,166,35,0.25);border-left:3px solid var(--yellow,#f5a623);border-radius:3px;padding:8px 12px;font-size:11px;color:var(--text1);line-height:1.7;margin-bottom:10px;}
.ag-section{font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border);}
.ag-card{background:var(--bg2);border:1px solid var(--border);border-radius:5px;padding:12px 14px;margin-bottom:10px;}
/* Observability */
.agx-event-row{display:flex;gap:8px;padding:3px 10px;border-bottom:1px solid rgba(0,176,143,0.06);font-family:var(--mono);font-size:10px;color:var(--text1);align-items:baseline;}
.agx-event-row:hover{background:rgba(0,176,143,0.04);}
.agx-ev-ts{color:var(--text3);flex-shrink:0;font-size:9px;}
.agx-ev-type{font-weight:700;flex-shrink:0;min-width:110px;}
.agx-ev-msg{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.agx-ev-lat{color:var(--text3);flex-shrink:0;font-size:9px;}
.agx-kafka-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;background:var(--text3);transition:background 0.3s;}
.agx-kafka-dot.connected{background:var(--green);box-shadow:0 0 6px var(--green);}
.agx-kafka-dot.error{background:var(--red);}
/* JAR */
#ag-jar-dropzone{border:2px dashed rgba(0,176,143,0.35);border-radius:6px;background:rgba(0,176,143,0.03);padding:18px;text-align:center;cursor:pointer;transition:all 0.15s;display:flex;flex-direction:column;align-items:center;gap:6px;}
#ag-jar-dropzone:hover,#ag-jar-dropzone.dz-active{border-color:#00c4a0;background:rgba(0,176,143,0.08);}
/* Status */
#ag-status-bar{display:flex;align-items:center;gap:10px;padding:5px 14px;background:var(--bg0);border-top:1px solid rgba(0,176,143,0.15);flex-shrink:0;font-size:10px;font-family:var(--mono);color:var(--text3);}
/* History */
.ag-hist-item{padding:8px 11px;border:1px solid var(--border);border-radius:4px;background:var(--bg2);margin-bottom:5px;cursor:pointer;display:flex;align-items:center;gap:8px;transition:background 0.1s;}
.ag-hist-item:hover{background:var(--bg1);}
/* Zoom label */
#ag-zoom-lbl{font-size:10px;color:var(--text3);font-family:var(--mono);min-width:40px;text-align:center;}
`;
    document.head.appendChild(s);
}
/* ══════════════════════════════════════════════════════════════════════════
   OPERATOR CATALOGUE
   ══════════════════════════════════════════════════════════════════════════ */
const AG_OPERATORS = [
    // ── EVENT SOURCES ──
    { id:'kafka_event', group:'Event Sources', label:'Kafka Event', color:'#1a6fa8', isSource:true,
        desc:'Kafka topic event source with watermark support', params:[
            {id:'table_name',label:'Table Name',type:'text',req:true,ph:'agent_events'},
            {id:'topic',label:'Topic',type:'text',req:true,ph:'agent-input-events'},
            {id:'bootstrap',label:'Bootstrap Servers',type:'text',req:true,ph:'kafka:9092'},
            {id:'format',label:'Format',type:'select',opts:['json','avro','avro-confluent','protobuf','raw'],val:'json'},
            {id:'group_id',label:'Consumer Group',type:'text',ph:'agent-consumer'},
            {id:'startup_mode',label:'Startup Mode',type:'select',opts:['latest-offset','earliest-offset','group-offsets'],val:'latest-offset'},
            {id:'watermark_col',label:'Watermark Column',type:'text',ph:'event_time'},
            {id:'watermark_delay',label:'Watermark Delay (s)',type:'text',ph:'5'},
            {id:'schema',label:'Schema (name TYPE per line)',type:'textarea',ph:'event_id BIGINT\npayload STRING\nevent_time TIMESTAMP(3)'},
            {id:'security_protocol',label:'Security Protocol',type:'select',opts:['','PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL'],val:''},
            {id:'sasl_username',label:'SASL Username',type:'text',ph:'api-key'},
            {id:'sasl_password',label:'SASL Password',type:'text',ph:'api-secret'},
        ]},
    { id:'datagen_event', group:'Event Sources', label:'Datagen Events', color:'#2d8a4e', isSource:true,
        desc:'Synthetic random event generator for development', params:[
            {id:'table_name',label:'Table Name',type:'text',req:true,ph:'mock_events'},
            {id:'rows_per_second',label:'Events / Second',type:'text',ph:'50'},
            {id:'schema',label:'Schema',type:'textarea',ph:'id BIGINT\npayload STRING\nts TIMESTAMP(3)'},
        ]},
    { id:'jdbc_event', group:'Event Sources', label:'JDBC Events', color:'#4a8fa8', isSource:true,
        desc:'JDBC database as streaming event source', params:[
            {id:'table_name',label:'Table Name',type:'text',req:true,ph:'pg_events'},
            {id:'jdbc_url',label:'JDBC URL',type:'text',req:true,ph:'jdbc:postgresql://localhost/db'},
            {id:'db_table',label:'DB Table',type:'text',req:true,ph:'public.events'},
            {id:'username',label:'Username',type:'text',ph:'flink_user'},
            {id:'password',label:'Password',type:'text',ph:'secret'},
            {id:'schema',label:'Schema',type:'textarea',ph:'id BIGINT\ndata STRING'},
        ]},
    { id:'kinesis_event', group:'Event Sources', label:'Kinesis Events', color:'#e8620a', isSource:true,
        desc:'AWS Kinesis Data Streams event source', params:[
            {id:'table_name',label:'Table Name',type:'text',req:true,ph:'kinesis_events'},
            {id:'stream',label:'Stream Name',type:'text',req:true,ph:'my-event-stream'},
            {id:'region',label:'AWS Region',type:'text',req:true,ph:'us-east-1'},
            {id:'format',label:'Format',type:'select',opts:['json','csv'],val:'json'},
            {id:'schema',label:'Schema',type:'textarea',ph:'id BIGINT\ndata STRING'},
        ]},
    // ── LLM MODELS ──
    { id:'llm_openai', group:'LLM Models', label:'OpenAI', color:'#412991', desc:'GPT-4o, GPT-4o-mini, GPT-4 Turbo', params:[
            {id:'model',label:'Model',type:'select',opts:['gpt-4o','gpt-4o-mini','gpt-4-turbo','gpt-3.5-turbo'],val:'gpt-4o-mini'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'OPENAI_API_KEY'},
            {id:'system_prompt',label:'System Prompt',type:'textarea',ph:'You are a fraud detection agent…'},
            {id:'temperature',label:'Temperature',type:'text',ph:'0.0'},
            {id:'max_tokens',label:'Max Tokens',type:'text',ph:'512'},
        ]},
    { id:'llm_anthropic', group:'LLM Models', label:'Anthropic Claude', color:'#d4602a', desc:'Claude Sonnet, Opus, Haiku', params:[
            {id:'model',label:'Model',type:'select',opts:['claude-sonnet-4-6','claude-opus-4-6','claude-haiku-4-5-20251001'],val:'claude-sonnet-4-6'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'ANTHROPIC_API_KEY'},
            {id:'system_prompt',label:'System Prompt',type:'textarea',ph:'You are an IoT anomaly detection agent…'},
            {id:'max_tokens',label:'Max Tokens',type:'text',ph:'512'},
        ]},
    { id:'llm_azureai', group:'LLM Models', label:'Azure AI', color:'#0078D4', desc:'Azure OpenAI and Azure AI Foundry', params:[
            {id:'endpoint',label:'Azure Endpoint',type:'text',req:true,ph:'https://myinst.openai.azure.com'},
            {id:'deployment',label:'Deployment Name',type:'text',req:true,ph:'gpt-4o'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'AZURE_OPENAI_KEY'},
            {id:'api_version',label:'API Version',type:'text',ph:'2024-02-01'},
            {id:'max_tokens',label:'Max Tokens',type:'text',ph:'512'},
        ]},
    { id:'llm_bedrock', group:'LLM Models', label:'AWS Bedrock', color:'#FF9900', desc:'Claude, Titan, Llama via Bedrock', params:[
            {id:'model_id',label:'Model ID',type:'select',opts:['anthropic.claude-sonnet-4-6','amazon.titan-text-express-v1','meta.llama3-8b-instruct-v1:0'],val:'anthropic.claude-sonnet-4-6'},
            {id:'region',label:'AWS Region',type:'text',ph:'us-east-1'},
            {id:'system_prompt',label:'System Prompt',type:'textarea',ph:''},
            {id:'max_tokens',label:'Max Tokens',type:'text',ph:'512'},
        ]},
    { id:'llm_ollama', group:'LLM Models', label:'Ollama (Local)', color:'#2a4a3a', desc:'Local LLM via Ollama — no API key', params:[
            {id:'model',label:'Model',type:'text',req:true,ph:'llama3'},
            {id:'base_url',label:'Ollama Base URL',type:'text',ph:'http://localhost:11434'},
            {id:'system_prompt',label:'System Prompt',type:'textarea',ph:''},
            {id:'temperature',label:'Temperature',type:'text',ph:'0.0'},
            {id:'max_tokens',label:'Max Tokens',type:'text',ph:'512'},
        ]},
    { id:'llm_mistral', group:'LLM Models', label:'Mistral AI', color:'#FF7000', desc:'Mistral, Mixtral hosted API', params:[
            {id:'model',label:'Model',type:'text',ph:'mistral-small-latest'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'MISTRAL_API_KEY'},
            {id:'system_prompt',label:'System Prompt',type:'textarea',ph:''},
            {id:'max_tokens',label:'Max Tokens',type:'text',ph:'512'},
        ]},
    // ── EMBEDDINGS ──
    { id:'embed_openai', group:'Embeddings', label:'OpenAI Embeddings', color:'#412991', desc:'text-embedding-3-small / large', params:[
            {id:'model',label:'Model',type:'select',opts:['text-embedding-3-small','text-embedding-3-large','text-embedding-ada-002'],val:'text-embedding-3-small'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'OPENAI_API_KEY'},
            {id:'dimensions',label:'Dimensions',type:'text',ph:'1536'},
        ]},
    { id:'embed_ollama', group:'Embeddings', label:'Ollama Embeddings', color:'#2a4a3a', desc:'Local embeddings via Ollama', params:[
            {id:'model',label:'Model',type:'text',req:true,ph:'nomic-embed-text'},
            {id:'base_url',label:'Ollama URL',type:'text',ph:'http://localhost:11434'},
            {id:'dimensions',label:'Dimensions',type:'text',ph:'768'},
        ]},
    { id:'embed_hf', group:'Embeddings', label:'HuggingFace Embed', color:'#FF9D00', desc:'Sentence Transformers via HuggingFace', params:[
            {id:'model_id',label:'Model ID',type:'text',req:true,ph:'sentence-transformers/all-MiniLM-L6-v2'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'HF_API_KEY'},
        ]},
    // ── VECTOR STORES ──
    { id:'vs_elasticsearch', group:'Vector Stores', label:'Elasticsearch VSS', color:'#5a0a00', desc:'Elasticsearch HNSW dense vector search', params:[
            {id:'hosts',label:'ES Hosts',type:'text',req:true,ph:'http://elasticsearch:9200'},
            {id:'index',label:'Index Name',type:'text',req:true,ph:'agent-context'},
            {id:'dims',label:'Vector Dimensions',type:'text',ph:'1536'},
            {id:'username',label:'Username',type:'text',ph:'elastic'},
            {id:'password',label:'Password',type:'text',ph:''},
            {id:'top_k',label:'Top-K Results',type:'text',ph:'5'},
        ]},
    { id:'vs_redis', group:'Vector Stores', label:'Redis VSS', color:'#8b1a1a', desc:'Redis Vector Similarity Search', params:[
            {id:'host',label:'Redis Host',type:'text',ph:'localhost'},
            {id:'port',label:'Port',type:'text',ph:'6379'},
            {id:'index_name',label:'Index Name',type:'text',ph:'agent-mem'},
            {id:'dims',label:'Dimensions',type:'text',ph:'1536'},
            {id:'top_k',label:'Top-K Results',type:'text',ph:'5'},
        ]},
    { id:'vs_milvus', group:'Vector Stores', label:'Milvus', color:'#1a5aa8', desc:'Milvus distributed vector database', params:[
            {id:'uri',label:'Milvus URI',type:'text',ph:'http://localhost:19530'},
            {id:'collection',label:'Collection',type:'text',ph:'agent_memory'},
            {id:'dims',label:'Dimensions',type:'text',ph:'1536'},
            {id:'top_k',label:'Top-K Results',type:'text',ph:'5'},
        ]},
    { id:'vs_qdrant', group:'Vector Stores', label:'Qdrant', color:'#4a2a8a', desc:'Qdrant high-performance vector database', params:[
            {id:'url',label:'Qdrant URL',type:'text',ph:'http://localhost:6333'},
            {id:'collection',label:'Collection Name',type:'text',ph:'agent-ctx'},
            {id:'api_key_env',label:'API Key (env var)',type:'text',ph:'QDRANT_API_KEY'},
            {id:'dims',label:'Dimensions',type:'text',ph:'1536'},
            {id:'top_k',label:'Top-K Results',type:'text',ph:'5'},
        ]},
    // ── MEMORY ──
    { id:'mem_sensory', group:'Memory', label:'Sensory Memory', color:'#7a3a9a', desc:'In-run context window for one agent execution', params:[
            {id:'max_tokens',label:'Max Context Tokens',type:'text',ph:'4096'},
            {id:'strategy',label:'Retention Strategy',type:'select',opts:['sliding_window','summarize','truncate'],val:'sliding_window'},
        ]},
    { id:'mem_short', group:'Memory', label:'Short-Term Memory', color:'#4a2a9a', desc:'Cross-run context in Flink RocksDB state', params:[
            {id:'ttl_hours',label:'TTL (hours)',type:'text',ph:'24'},
            {id:'max_entries',label:'Max Entries per Key',type:'text',ph:'100'},
            {id:'scope',label:'Partition Key (e.g. user_id)',type:'text',ph:'user_id'},
        ]},
    { id:'mem_long', group:'Memory', label:'Long-Term Memory', color:'#2a1a7a', desc:'Semantic vector retrieval from Vector Store', params:[
            {id:'compaction',label:'Compaction Strategy',type:'select',opts:['summary','deduplicate','none'],val:'summary'},
            {id:'compact_interval',label:'Compact Every N Runs',type:'text',ph:'100'},
            {id:'recall_top_k',label:'Recall Top-K',type:'text',ph:'5'},
        ]},
    // ── PROMPTS ──
    { id:'prompt_template', group:'Prompts', label:'Prompt Template', color:'#4a6a1a', desc:'Jinja2-style prompt template with variable injection', params:[
            {id:'template_name',label:'Template Name',type:'text',req:true,ph:'fraud_prompt'},
            {id:'template',label:'Prompt Template',type:'textarea',ph:'Analyze: {{event}}\nContext: {{memory}}\nDecide: FRAUD or LEGITIMATE'},
            {id:'variables',label:'Variables (comma-sep)',type:'text',ph:'event,memory'},
        ]},
    { id:'few_shot', group:'Prompts', label:'Few-Shot Examples', color:'#2a5a1a', desc:'Dynamic few-shot injection from vector store', params:[
            {id:'num_examples',label:'Number of Examples',type:'text',ph:'3'},
            {id:'selection',label:'Selection Strategy',type:'select',opts:['semantic_similarity','random','fixed'],val:'semantic_similarity'},
            {id:'examples',label:'Examples (JSON array)',type:'textarea',ph:'[{"input":"tx:$500","output":"LEGITIMATE"}]'},
        ]},
    // ── TOOLS / MCP ──
    { id:'tool_http', group:'Tools', label:'HTTP Tool', color:'#1a5a6a', desc:'HTTP REST endpoint callable by the agent', params:[
            {id:'tool_name',label:'Tool Name',type:'text',req:true,ph:'check_fraud_score'},
            {id:'url',label:'Endpoint URL',type:'text',req:true,ph:'https://api.example.com/score'},
            {id:'method',label:'HTTP Method',type:'select',opts:['POST','GET','PUT'],val:'POST'},
            {id:'description',label:'Tool Description (for LLM)',type:'textarea',ph:'Checks real-time fraud score for a transaction'},
            {id:'timeout_ms',label:'Timeout (ms)',type:'text',ph:'5000'},
        ]},
    { id:'tool_kafka_produce', group:'Tools', label:'Kafka Produce Tool', color:'#1a3a6a', desc:'Agent action: produce message to Kafka topic', params:[
            {id:'tool_name',label:'Tool Name',type:'text',req:true,ph:'publish_alert'},
            {id:'topic',label:'Target Topic',type:'text',req:true,ph:'agent-alerts'},
            {id:'bootstrap',label:'Bootstrap Servers',type:'text',ph:'kafka:9092'},
            {id:'description',label:'Tool Description',type:'textarea',ph:'Publishes an alert to the alerts topic'},
        ]},
    { id:'tool_sql', group:'Tools', label:'Flink SQL Tool', color:'#1a2a6a', desc:'Execute a Flink SQL query as an agent tool', params:[
            {id:'tool_name',label:'Tool Name',type:'text',req:true,ph:'query_user_history'},
            {id:'sql',label:'SQL Query Template',type:'textarea',ph:'SELECT * FROM user_tx WHERE user_id = {{user_id}} LIMIT 10'},
            {id:'description',label:'Tool Description',type:'textarea',ph:'Queries last 10 transactions for a user'},
        ]},
    { id:'mcp_server', group:'Tools', label:'MCP Server', color:'#5a1a8a', desc:'Model Context Protocol server — exposes tools', params:[
            {id:'server_name',label:'MCP Server Name',type:'text',req:true,ph:'my-tool-server'},
            {id:'url',label:'MCP Server URL',type:'text',req:true,ph:'http://mcp-server:3000/sse'},
            {id:'transport',label:'Transport',type:'select',opts:['SSE','stdio','WebSocket'],val:'SSE'},
            {id:'auth_env',label:'Auth Token (env var)',type:'text',ph:'MCP_AUTH_TOKEN'},
        ]},
    // ── ORCHESTRATION ──
    { id:'agent_workflow', group:'Orchestration', label:'Workflow Agent', color:'#6a0a9a', desc:'Deterministic multi-step workflow (WorkflowAgent API)', params:[
            {id:'agent_name',label:'Agent Name',type:'text',req:true,ph:'FraudDetectionAgent'},
            {id:'language',label:'Language',type:'select',opts:['Python','Java'],val:'Python'},
            {id:'checkpoint_interval',label:'Checkpoint Interval (ms)',type:'text',ph:'10000'},
            {id:'parallelism',label:'Parallelism',type:'text',ph:'4'},
            {id:'exactly_once',label:'Exactly-Once Actions',type:'select',opts:['enabled','disabled'],val:'enabled'},
            {id:'durable_execution',label:'Durable Execution',type:'select',opts:['enabled','disabled'],val:'enabled'},
        ]},
    { id:'agent_react', group:'Orchestration', label:'ReAct Agent', color:'#0a6a9a', desc:'Reasoning + Acting loop (ReActAgent API)', params:[
            {id:'agent_name',label:'Agent Name',type:'text',req:true,ph:'IoTAnalyticsAgent'},
            {id:'language',label:'Language',type:'select',opts:['Python','Java'],val:'Python'},
            {id:'max_iterations',label:'Max ReAct Iterations',type:'text',ph:'10'},
            {id:'checkpoint_interval',label:'Checkpoint Interval (ms)',type:'text',ph:'10000'},
            {id:'parallelism',label:'Parallelism',type:'text',ph:'4'},
            {id:'exactly_once',label:'Exactly-Once Actions',type:'select',opts:['enabled','disabled'],val:'enabled'},
        ]},
    { id:'agent_multi', group:'Orchestration', label:'Multi-Agent (A2A)', color:'#0a5a2a', desc:'Agent-to-Agent coordination via Google A2A protocol', params:[
            {id:'agent_name',label:'Coordinator Name',type:'text',req:true,ph:'OrchestratorAgent'},
            {id:'a2a_endpoint',label:'A2A Endpoint',type:'text',ph:'http://agent-router:8080/a2a'},
            {id:'child_agents',label:'Child Agent IDs (comma-sep)',type:'text',ph:'fraud_agent,compliance_agent'},
            {id:'delegation_strategy',label:'Delegation Strategy',type:'select',opts:['round_robin','capability_match','load_balance'],val:'capability_match'},
        ]},
    // ── ACTIONS ──
    { id:'action_kafka', group:'Actions', label:'Kafka Action', color:'#0a3a6a', isSink:true, desc:'Exactly-once agent results to Kafka', params:[
            {id:'table_name',label:'Output Table Name',type:'text',req:true,ph:'agent_output_events'},
            {id:'topic',label:'Topic',type:'text',req:true,ph:'agent-output'},
            {id:'bootstrap',label:'Bootstrap Servers',type:'text',ph:'kafka:9092'},
            {id:'format',label:'Format',type:'select',opts:['json','avro'],val:'json'},
        ]},
    { id:'action_jdbc', group:'Actions', label:'JDBC Action', color:'#0a5a5a', isSink:true, desc:'Write agent decisions to a relational database', params:[
            {id:'table_name',label:'Output Table Name',type:'text',req:true,ph:'agent_decisions'},
            {id:'jdbc_url',label:'JDBC URL',type:'text',req:true,ph:'jdbc:postgresql://localhost/db'},
            {id:'db_table',label:'DB Table',type:'text',ph:'public.agent_decisions'},
            {id:'username',label:'Username',type:'text',ph:'flink_user'},
            {id:'password',label:'Password',type:'text',ph:''},
        ]},
    { id:'action_http', group:'Actions', label:'HTTP Webhook Action', color:'#2a5a2a', isSink:true, desc:'POST agent output to an external webhook', params:[
            {id:'url',label:'Webhook URL',type:'text',req:true,ph:'https://hooks.slack.com/…'},
            {id:'method',label:'HTTP Method',type:'select',opts:['POST','PUT'],val:'POST'},
            {id:'auth_env',label:'Auth Header Env Var',type:'text',ph:'WEBHOOK_TOKEN'},
            {id:'body_template',label:'Body Template ({{field}} vars)',type:'textarea',ph:'{"text":"Agent decision: {{decision}}"}'},
        ]},
    // ── OBSERVABILITY ──
    { id:'event_log', group:'Observability', label:'Event Log', color:'#2a4a2a', desc:'Full audit trail: LLM calls, tool invocations, decisions', params:[
            {id:'sink_type',label:'Event Log Sink',type:'select',opts:['Kafka','Elasticsearch','Print (debug)'],val:'Kafka'},
            {id:'topic',label:'Log Topic / Index',type:'text',ph:'flink-agent-events'},
            {id:'include_llm',label:'Log LLM Calls',type:'select',opts:['yes','no'],val:'yes'},
            {id:'include_tools',label:'Log Tool Calls',type:'select',opts:['yes','no'],val:'yes'},
        ]},
    { id:'vector_search', group:'Observability', label:'VECTOR_SEARCH', color:'#2a1a6a', desc:'Flink 2.2 native streaming vector similarity search', params:[
            {id:'top_k',label:'Top-K',type:'text',ph:'5'},
            {id:'similarity_threshold',label:'Min Similarity Score',type:'text',ph:'0.8'},
        ]},
    // ── SINKS ──
    { id:'sink_print', group:'Sinks', label:'Print (Debug)', color:'#3a3a3a', isSink:true, desc:'Print outputs to TaskManager stdout', params:[
            {id:'table_name',label:'Sink Name',type:'text',ph:'agent_debug_sink'},
        ]},
    { id:'sink_blackhole', group:'Sinks', label:'Blackhole', color:'#1a1a2a', isSink:true, desc:'Discard all output — use for benchmarking', params:[
            {id:'table_name',label:'Sink Name',type:'text',ph:'blackhole_sink'},
        ]},
];

const AG_GROUPS = [...new Set(AG_OPERATORS.map(o => o.group))];
/* ══════════════════════════════════════════════════════════════════════════
   AGENT TEMPLATES — icons use _agSvgIcon only (no emoji)
   ══════════════════════════════════════════════════════════════════════════ */
const AG_TEMPLATES = [
    { id:'fraud_detection',
        icon: _agSvgIcon('<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',20),
        name:'Fraud Detection Agent',
        desc:'ReAct agent scores transactions using LLM + historical context',
        nodes:[
            {opId:'kafka_event',   x:40,  y:110, label:'Tx Events'},
            {opId:'mem_short',     x:200, y:40,  label:'User Context'},
            {opId:'llm_anthropic', x:200, y:160, label:'Claude Scorer'},
            {opId:'agent_react',   x:380, y:110, label:'FraudAgent'},
            {opId:'tool_http',     x:560, y:40,  label:'Score API'},
            {opId:'action_kafka',  x:560, y:140, label:'Fraud Alerts'},
            {opId:'event_log',     x:560, y:240, label:'Audit Log'},
        ],
        edges:[['n1','n4'],['n2','n4'],['n3','n4'],['n4','n5'],['n4','n6'],['n4','n7']],
    },
    { id:'iot_anomaly',
        icon: _agSvgIcon('<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',20),
        name:'IoT Anomaly Agent',
        desc:'Workflow agent monitoring sensor streams with vector memory',
        nodes:[
            {opId:'kafka_event',      x:40,  y:100, label:'Sensor Stream'},
            {opId:'embed_openai',     x:200, y:40,  label:'Embeddings'},
            {opId:'vs_elasticsearch', x:200, y:160, label:'Vector Memory'},
            {opId:'agent_workflow',   x:380, y:100, label:'IoTAgent'},
            {opId:'llm_openai',       x:200, y:280, label:'GPT-4o'},
            {opId:'action_http',      x:560, y:100, label:'Alert Webhook'},
        ],
        edges:[['n1','n4'],['n1','n2'],['n2','n3'],['n3','n4'],['n5','n4'],['n4','n6']],
    },
    { id:'customer_support',
        icon: _agSvgIcon('<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>',20),
        name:'Customer Support Agent',
        desc:'Multi-agent system with RAG, long-term memory and MCP CRM tools',
        nodes:[
            {opId:'kafka_event',   x:40,  y:130, label:'Support Events'},
            {opId:'mem_long',      x:200, y:40,  label:'Long-Term Mem'},
            {opId:'vs_qdrant',     x:200, y:160, label:'KB Vector Store'},
            {opId:'llm_openai',    x:380, y:80,  label:'GPT-4o Resolver'},
            {opId:'agent_react',   x:380, y:200, label:'SupportAgent'},
            {opId:'mcp_server',    x:560, y:80,  label:'CRM Tools (MCP)'},
            {opId:'action_kafka',  x:560, y:200, label:'Resolution Output'},
        ],
        edges:[['n1','n5'],['n2','n5'],['n3','n5'],['n4','n5'],['n5','n6'],['n5','n7']],
    },
    { id:'supply_chain',
        icon: _agSvgIcon('<rect x="1" y="3" width="15" height="13"/><path d="M16 8h4l3 3v5h-7V8z"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/>',20),
        name:'Supply Chain Optimizer',
        desc:'A2A multi-agent logistics optimization on AWS Bedrock',
        nodes:[
            {opId:'kafka_event',   x:40,  y:110, label:'Logistics Events'},
            {opId:'agent_workflow',x:220, y:60,  label:'RouteAgent'},
            {opId:'agent_multi',   x:220, y:200, label:'A2A Coordinator'},
            {opId:'llm_bedrock',   x:420, y:110, label:'Bedrock Optimizer'},
            {opId:'action_kafka',  x:600, y:110, label:'Dispatch Events'},
            {opId:'event_log',     x:600, y:220, label:'Decision Log'},
        ],
        edges:[['n1','n2'],['n1','n3'],['n2','n4'],['n3','n4'],['n4','n5'],['n4','n6']],
    },
    { id:'log_classifier',
        icon: _agSvgIcon('<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="13" x2="15" y2="13"/>',20),
        name:'Log Classification Agent',
        desc:'Event-driven log triage using LLM classification and routing',
        nodes:[
            {opId:'kafka_event',    x:40,  y:110, label:'App Logs'},
            {opId:'prompt_template',x:220, y:40,  label:'Triage Prompt'},
            {opId:'llm_openai',     x:220, y:160, label:'Classifier LLM'},
            {opId:'agent_workflow', x:400, y:110, label:'LogAgent'},
            {opId:'action_kafka',   x:580, y:60,  label:'Critical Logs'},
            {opId:'sink_print',     x:580, y:200, label:'Debug Output'},
        ],
        edges:[['n1','n4'],['n2','n4'],['n3','n4'],['n4','n5'],['n4','n6']],
    },
    { id:'blank',
        icon: _agSvgIcon('<rect x="3" y="3" width="18" height="18" rx="2"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/>',20),
        name:'Blank Agent',
        desc:'Start from an empty canvas and build from scratch',
        nodes:[], edges:[],
    },
];
/* ══════════════════════════════════════════════════════════════════════════
   STATE
   ══════════════════════════════════════════════════════════════════════════ */
const _AG = {
    mode:null, activeTab:'welcome',
    canvas:{nodes:[],edges:[],pan:{x:0,y:0},scale:1.0},
    connecting:null, selectedNode:null,
    panDrag:false, panSX:0, panSY:0, panOX:0, panOY:0,
    dragNode:null, dragOffX:0, dragOffY:0,
    animating:false, animTimer:null,
    wizStep:0, wizData:{}, codeTab:'sql',
    history:[], fullscreen:false,
    jarFile:null, uidCounter:1,
    obs:{running:false,pollInterval:null,buffer:[],total:0,errors:0},
};
(function(){try{const r=localStorage.getItem('strlabstudio_agent_history');if(r)_AG.history=JSON.parse(r);}catch(_){}})();
function _agSaveHistory(e){_AG.history.unshift({...e,ts:new Date().toISOString()});if(_AG.history.length>20)_AG.history.pop();try{localStorage.setItem('strlabstudio_agent_history',JSON.stringify(_AG.history));}catch(_){}}

/* ══════════════════════════════════════════════════════════════════════════
   ENTRY POINT
   ══════════════════════════════════════════════════════════════════════════ */
function openAgentManager(){
    _agInjectCss();
    if(!document.getElementById('ag-modal'))_agBuildModal();
    openModal('ag-modal');
    _agSwitchTab('welcome');
}

/* ══════════════════════════════════════════════════════════════════════════
   MODAL SHELL
   ══════════════════════════════════════════════════════════════════════════ */
function _agBuildModal(){
    const m=document.createElement('div');
    m.id='ag-modal'; m.className='modal-overlay';
    m.innerHTML=`
<div class="modal" id="ag-modal-inner">
  <div class="modal-header">
    <span style="display:flex;align-items:center;gap:9px;font-size:13px;font-weight:700;color:var(--text0);">
      ${_agSvgIcon('<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M4.22 4.22l2.12 2.12M17.66 17.66l2.12 2.12M2 12h3M19 12h3M4.22 19.78l2.12-2.12M17.66 6.34l2.12-2.12"/>',14).replace('stroke="currentColor"','stroke="#00c4a0"')}
      Flink Agent Manager
      <span style="font-size:9px;font-weight:400;color:var(--text3);font-family:var(--mono);">Apache Flink Agents 0.2</span>
    </span>
    <div style="display:flex;align-items:center;gap:5px;margin-left:auto;">
      <button onclick="_agShowHistory()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid var(--border2);background:var(--bg3);color:var(--text2);cursor:pointer;font-family:var(--mono);">History <span id="ag-hist-count"></span></button>
      <button onclick="_agExportAgent()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(0,176,143,0.3);background:rgba(0,176,143,0.08);color:#00c4a0;cursor:pointer;font-family:var(--mono);">⬆ Export</button>
      <label style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(245,166,35,0.3);background:rgba(245,166,35,0.07);color:var(--yellow,#f5a623);cursor:pointer;font-family:var(--mono);">⬇ Import<input type="file" accept=".json" style="display:none;" onchange="_agImportAgent(this)"/></label>
      <button onclick="_agResetAll()" style="font-size:10px;padding:3px 9px;border-radius:3px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;font-family:var(--mono);">↺ Reset</button>
      <button id="ag-expand-btn" onclick="_agToggleFullscreen()" title="Expand to full screen"
        style="background:none;border:1px solid var(--border);color:var(--text2);cursor:pointer;font-size:16px;padding:1px 8px;border-radius:3px;line-height:1;">⤢</button>
      <button onclick="modalMinimize('ag-modal','Agent Manager')" style="background:none;border:1px solid var(--border);color:var(--text2);cursor:pointer;font-size:13px;padding:1px 8px;border-radius:3px;" title="Minimise">⊟</button>
      <button class="modal-close" onclick="closeModal('ag-modal')">×</button>
    </div>
  </div>
  <div id="ag-tab-bar">
    <button class="ag-tab active" id="ag-t-welcome"       onclick="_agSwitchTab('welcome')">◈ Welcome</button>
    <button class="ag-tab"        id="ag-t-canvas"        onclick="_agSwitchTab('canvas')">◈ Visual Canvas</button>
    <button class="ag-tab"        id="ag-t-wizard"        onclick="_agSwitchTab('wizard')">◈ Guided Wizard</button>
    <button class="ag-tab"        id="ag-t-code"          onclick="_agSwitchTab('code')">⟨/⟩ Generated Code</button>
    <button class="ag-tab"        id="ag-t-jar"           onclick="_agSwitchTab('jar')">◈ JAR Upload</button>
    <button class="ag-tab"        id="ag-t-observability" onclick="_agSwitchTab('observability')">◈ Observability</button>
  </div>
  <div id="ag-content" style="flex:1;display:flex;flex-direction:column;overflow:hidden;"></div>
  <div id="ag-status-bar">
    <span id="ag-stat-nodes" style="color:var(--text2);">0 nodes</span>
    <span>·</span>
    <span id="ag-stat-edges" style="color:var(--text2);">0 edges</span>
    <span>·</span>
    <span id="ag-stat-mode"  style="color:#00c4a0;">No agent built yet</span>
    <span style="margin-left:auto;color:var(--accent);" id="ag-stat-msg"></span>
  </div>
</div>`;
    document.body.appendChild(m);
    m.addEventListener('click',e=>{if(e.target===m)closeModal('ag-modal');});
    _agUpdateHistCount();
}

/* ── Fullscreen toggle (mirrors _plmToggleFullscreen exactly) ──────────── */
function _agToggleFullscreen(){
    const inner=document.getElementById('ag-modal-inner');
    const btn=document.getElementById('ag-expand-btn');
    if(!inner)return;
    _AG.fullscreen=!_AG.fullscreen;
    if(_AG.fullscreen){
        inner.style.width='100vw'; inner.style.height='100vh';
        inner.style.maxHeight='100vh'; inner.style.borderRadius='0';
        if(btn){btn.textContent='⤡';btn.title='Restore window size';}
    } else {
        inner.style.width='min(1300px,97vw)'; inner.style.height='92vh';
        inner.style.maxHeight='92vh'; inner.style.borderRadius='';
        if(btn){btn.textContent='⤢';btn.title='Expand to full screen';}
    }
    setTimeout(()=>{_agDrawGrid();_agRenderAllEdges();},220);
}

/* ══════════════════════════════════════════════════════════════════════════
   TAB SWITCHING
   ══════════════════════════════════════════════════════════════════════════ */
function _agSwitchTab(tab){
    if(_AG.obs.running&&tab!=='observability')_agObsStop(true);
    _AG.activeTab=tab;
    document.querySelectorAll('.ag-tab').forEach(b=>b.classList.remove('active'));
    document.getElementById('ag-t-'+tab)?.classList.add('active');
    const content=document.getElementById('ag-content');
    if(!content)return;
    content.innerHTML='';
    ({welcome:_agRenderWelcome,canvas:_agRenderCanvas,wizard:_agRenderWizard,
        code:_agRenderCode,jar:_agRenderJar,observability:_agRenderObservability})[tab]?.();
}
/* ══════════════════════════════════════════════════════════════════════════
   WELCOME TAB
   ══════════════════════════════════════════════════════════════════════════ */
function _agRenderWelcome(){
    const content=document.getElementById('ag-content');
    content.innerHTML=`
<div id="ag-welcome">
  <div class="ag-welcome-hero">
    <div style="display:flex;justify-content:center;margin-bottom:14px;color:#00c4a0;">
      ${_agSvgIcon('<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M4.22 4.22l2.12 2.12M17.66 17.66l2.12 2.12M2 12h3M19 12h3M4.22 19.78l2.12-2.12M17.66 6.34l2.12-2.12"/><circle cx="12" cy="12" r="8" stroke-dasharray="3 2"/>',48)}
    </div>
    <h1>Flink <span>Agent</span> Manager</h1>
    <p>Build event-driven AI agents on Apache Flink's streaming runtime. Choose how to construct your agent — then generate production-ready Flink Agents 0.2 artefacts.</p>
  </div>
  <div class="ag-mode-cards">
    <div class="ag-mode-card" id="ag-mc-canvas" onclick="_agSelectMode('canvas')">
      <div class="ag-mc-icon">${_agSvgIcon('<circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/><line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/>',22)}</div>
      <div class="ag-mc-title">Visual Canvas</div>
      <div class="ag-mc-desc">Drag operator blocks onto the canvas and connect them. Instantly see the agent architecture and generate code.</div>
      <div class="ag-mc-badge">RECOMMENDED</div>
    </div>
    <div class="ag-mode-card" id="ag-mc-wizard" onclick="_agSelectMode('wizard')">
      <div class="ag-mc-icon">${_agSvgIcon('<path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/>',22)}</div>
      <div class="ag-mc-title">Guided Wizard</div>
      <div class="ag-mc-desc">Step-by-step dialog covering every Flink Agent component. Perfect for quickly scaffolding a production agent.</div>
      <div class="ag-mc-badge">BEGINNER FRIENDLY</div>
    </div>
    <div class="ag-mode-card" id="ag-mc-code" onclick="_agSelectMode('code')">
      <div class="ag-mc-icon">${_agSvgIcon('<polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>',22)}</div>
      <div class="ag-mc-title">Code First</div>
      <div class="ag-mc-desc">Jump directly to the generated code view. Edit parameters and see SQL, Java, and Python skeletons update in real time.</div>
      <div class="ag-mc-badge">ADVANCED</div>
    </div>
  </div>
  <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:12px;text-align:center;">Or start from a template</div>
  <div class="ag-template-grid">
    ${AG_TEMPLATES.map(t=>`
      <div class="ag-tmpl-card" onclick="_agLoadTemplate('${t.id}')">
        <div class="ag-tc-icon">${t.icon}</div>
        <div class="ag-tc-name">${t.name}</div>
        <div class="ag-tc-desc">${t.desc}</div>
      </div>`).join('')}
  </div>
  <div style="margin-top:24px;text-align:center;font-size:11px;color:var(--text3);line-height:1.8;">
    <strong style="color:var(--text2);">Flink Agents 0.2</strong> · WorkflowAgent + ReActAgent · Java &amp; Python APIs ·
    MCP + A2A protocols · Three-tier Memory · Exactly-Once Actions · VECTOR_SEARCH (Flink 2.2)
  </div>
</div>`;
}

function _agSelectMode(mode){
    document.querySelectorAll('.ag-mode-card').forEach(c=>c.classList.remove('selected'));
    document.getElementById('ag-mc-'+mode)?.classList.add('selected');
    _AG.mode=mode;
    setTimeout(()=>_agSwitchTab(mode),180);
}

function _agLoadTemplate(id){
    const tmpl=AG_TEMPLATES.find(t=>t.id===id); if(!tmpl)return;
    _AG.canvas.nodes=[]; _AG.canvas.edges=[];
    const nodeMap={};
    tmpl.nodes.forEach((n,i)=>{
        const uid='n'+(i+1);
        const opDef=AG_OPERATORS.find(o=>o.id===n.opId)||AG_OPERATORS[0];
        _AG.canvas.nodes.push({uid,opId:n.opId,x:n.x,y:n.y,label:n.label||opDef.label,params:{},configured:true,summary:''});
        nodeMap['n'+(i+1)]=uid;
    });
    (tmpl.edges||[]).forEach(([f,t])=>{if(nodeMap[f]&&nodeMap[t])_AG.canvas.edges.push({uid:_agUID(),fromUid:nodeMap[f],toUid:nodeMap[t]});});
    _AG.mode='canvas';
    _agSwitchTab('canvas');
    if(typeof toast==='function')toast('Template "'+tmpl.name+'" loaded','ok');
}

/* ══════════════════════════════════════════════════════════════════════════
   CANVAS TAB
   ══════════════════════════════════════════════════════════════════════════ */
function _agRenderCanvas(){
    const content=document.getElementById('ag-content');
    const paletteHtml=AG_GROUPS.map(g=>`
      <div class="ag-pal-group-label">${g}</div>
      ${AG_OPERATORS.filter(o=>o.group===g).map(op=>`
        <div class="ag-pal-item" draggable="true" data-opid="${op.id}"
          ondragstart="_agPalDragStart(event,'${op.id}')"
          title="${_agEsc(op.desc||op.label)}${op.isSource?' · SOURCE':''}${op.isSink?' · SINK':''}">
          <span class="ag-pi-icon" style="color:${op.color};">${_agIcon(op.id)}</span>
          <span class="ag-pi-label">${op.label}</span>
          ${op.isSource?'<span style="font-size:7px;font-weight:700;background:rgba(0,212,170,0.15);color:var(--accent);padding:0 3px;border-radius:2px;flex-shrink:0;">SRC</span>':''}
          ${op.isSink?'<span style="font-size:7px;font-weight:700;background:rgba(79,163,224,0.12);color:var(--blue,#4fa3e0);padding:0 3px;border-radius:2px;flex-shrink:0;">SINK</span>':''}
        </div>`).join('')}`).join('');

    content.innerHTML=`
<div id="ag-canvas-toolbar">
  <button class="ag-tb-btn" onclick="_agAutoLayout()">⊞ Auto Layout</button>
  <button class="ag-tb-btn" onclick="_agClearCanvas()">✕ Clear</button>
  <button class="ag-tb-btn green" id="ag-run-btn" onclick="_agToggleAnimation()">▶ Simulate Flow</button>
  <div style="flex:1;"></div>
  <button class="ag-tb-btn" onclick="_agCanvasZoom(-0.15)">−</button>
  <span id="ag-zoom-lbl">100%</span>
  <button class="ag-tb-btn" onclick="_agCanvasZoom(0.15)">+</button>
  <button class="ag-tb-btn" onclick="_agFitToView()">⊙ Fit</button>
  <div style="width:1px;height:16px;background:var(--border);margin:0 4px;"></div>
  <button class="ag-tb-btn purple" onclick="_agSwitchTab('code')">⟨/⟩ View Code</button>
  <button class="ag-tb-btn green"  onclick="_agInsertCodeToEditor()">⤵ Insert SQL</button>
</div>
<div id="ag-canvas-pane">
  <div id="ag-palette">
    <div style="padding:4px 7px 3px;font-size:9px;color:var(--text3);font-family:var(--mono);font-weight:700;letter-spacing:1.5px;text-transform:uppercase;">OPERATORS</div>
    <div style="padding:4px 6px;border-bottom:1px solid var(--border);">
      <div style="position:relative;">
        <svg style="position:absolute;left:7px;top:50%;transform:translateY(-50%);pointer-events:none;opacity:0.35;" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg>
        <input id="ag-pal-search" type="text" placeholder="Search operators…" autocomplete="off"
          style="width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;padding:4px 8px 4px 25px;font-size:11px;font-family:var(--mono);color:var(--text1);outline:none;"
          oninput="_agPalSearch(this.value)"/>
      </div>
    </div>
    ${paletteHtml}
    <div style="margin-top:8px;border-top:1px solid var(--border);padding:7px;">
      <div style="font-size:9px;color:var(--text3);line-height:1.8;">
        <div>🖱 Drag to canvas</div><div>✏ Click edit to configure</div>
        <div>○ Drag port → connect</div><div>Del to remove node</div>
      </div>
    </div>
  </div>
  <div id="ag-canvas-wrap"
    ondragover="event.preventDefault()" ondrop="_agCanvasDrop(event)"
    onmousedown="_agCanvasMouseDown(event)"
    onmousemove="_agCanvasMouseMove(event)"
    onmouseup="_agCanvasMouseUp(event)"
    onwheel="_agCanvasWheel(event)">
    <svg id="ag-canvas-grid" style="position:absolute;inset:0;width:100%;height:100%;pointer-events:none;"></svg>
    <svg id="ag-canvas-edges" style="position:absolute;inset:0;width:100%;height:100%;overflow:visible;pointer-events:none;">
      <defs><marker id="ag-arr" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
        <path d="M0,0 L0,6 L8,3 z" fill="rgba(0,196,160,0.6)"/>
      </marker></defs>
      <g id="ag-edges-g"></g><g id="ag-particles-g"></g><g id="ag-draw-g"></g>
    </svg>
    <div id="ag-canvas-nodes" style="position:absolute;top:0;left:0;transform-origin:0 0;"></div>
    <div id="ag-canvas-empty" style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;pointer-events:none;gap:10px;color:var(--text3);">
      <div style="opacity:0.15;">${_agSvgIcon('<circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/><line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/>',44)}</div>
      <div style="font-size:13px;">Drag operators from the palette to build your agent</div>
    </div>
  </div>
</div>`;
    setTimeout(()=>{_agDrawGrid();_agRenderAllNodes();_agRenderAllEdges();_agApplyCanvasTransform();},50);
}

/* ── Palette search ───────────────────────────────────────────────────────── */
function _agPalSearch(q){
    q=(q||'').toLowerCase().trim();
    document.querySelectorAll('.ag-pal-item[data-opid]').forEach(el=>{
        const label=el.querySelector('.ag-pi-label')?.textContent.toLowerCase()||'';
        const opId=(el.dataset.opid||'').toLowerCase();
        el.style.display=(!q||label.includes(q)||opId.includes(q))?'':'none';
    });
}

/* ── Grid ─────────────────────────────────────────────────────────────────── */
function _agDrawGrid(){
    const svg=document.getElementById('ag-canvas-grid'); if(!svg)return;
    const w=svg.clientWidth||1200,h=svg.clientHeight||800,sz=24;
    let d='';
    for(let x=0;x<=w;x+=sz)d+=`M${x},0 L${x},${h} `;
    for(let y=0;y<=h;y+=sz)d+=`M0,${y} L${w},${y} `;
    svg.innerHTML=`<path d="${d}" stroke="rgba(0,176,143,0.03)" stroke-width="1" fill="none"/>`;
}

/* ── Transform / zoom / fit ───────────────────────────────────────────────── */
function _agApplyCanvasTransform(){
    const c=document.getElementById('ag-canvas-nodes'); if(!c)return;
    const {pan,scale}=_AG.canvas;
    c.style.transform=`translate(${pan.x}px,${pan.y}px) scale(${scale})`;
    _agRenderAllEdges();
    const lbl=document.getElementById('ag-zoom-lbl'); if(lbl)lbl.textContent=Math.round(scale*100)+'%';
}
function _agCanvasZoom(delta,cx,cy){
    const wrap=document.getElementById('ag-canvas-wrap'); if(!wrap)return;
    const r=wrap.getBoundingClientRect();
    const mx=cx!==undefined?cx-r.left:r.width/2,my=cy!==undefined?cy-r.top:r.height/2;
    const prev=_AG.canvas.scale;
    _AG.canvas.scale=Math.max(0.12,Math.min(3,_AG.canvas.scale+delta));
    _AG.canvas.pan.x=mx-(mx-_AG.canvas.pan.x)*(_AG.canvas.scale/prev);
    _AG.canvas.pan.y=my-(my-_AG.canvas.pan.y)*(_AG.canvas.scale/prev);
    _agApplyCanvasTransform();
}
function _agFitToView(){
    const wrap=document.getElementById('ag-canvas-wrap'),nodes=_AG.canvas.nodes;
    if(!wrap||!nodes.length)return;
    let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
    nodes.forEach(n=>{minX=Math.min(minX,n.x);minY=Math.min(minY,n.y);maxX=Math.max(maxX,n.x+165);maxY=Math.max(maxY,n.y+70);});
    const pad=40,W=wrap.clientWidth-pad*2,H=wrap.clientHeight-pad*2;
    _AG.canvas.scale=Math.max(0.2,Math.min(2,Math.min(W/(maxX-minX),H/(maxY-minY))));
    _AG.canvas.pan.x=pad-minX*_AG.canvas.scale;
    _AG.canvas.pan.y=pad-minY*_AG.canvas.scale;
    _agApplyCanvasTransform();
}
function _agCanvasWheel(e){e.preventDefault();_agCanvasZoom(e.deltaY<0?0.1:-0.1,e.clientX,e.clientY);}

/* ── Auto layout ──────────────────────────────────────────────────────────── */
function _agAutoLayout(){
    const {nodes,edges}=_AG.canvas; if(!nodes.length)return;
    const inDeg={},children={};
    nodes.forEach(n=>{inDeg[n.uid]=0;children[n.uid]=[];});
    edges.forEach(e=>{if(inDeg[e.toUid]!==undefined)inDeg[e.toUid]++;if(children[e.fromUid])children[e.fromUid].push(e.toUid);});
    let queue=nodes.filter(n=>inDeg[n.uid]===0).map(n=>n.uid);
    const layers=[],visited=new Set();
    while(queue.length){layers.push([...queue]);const next=[];queue.forEach(id=>{visited.add(id);(children[id]||[]).forEach(cid=>{inDeg[cid]--;if(inDeg[cid]===0&&!visited.has(cid))next.push(cid);});});queue=next;}
    nodes.filter(n=>!visited.has(n.uid)).forEach(n=>layers.push([n.uid]));
    const COL_W=220,ROW_H=110,PAD_X=50,PAD_Y=50;
    layers.forEach((layer,li)=>{layer.forEach((uid,ri)=>{const node=nodes.find(n=>n.uid===uid);if(node){node.x=PAD_X+li*COL_W;node.y=PAD_Y+ri*ROW_H;}});});
    _agRenderAllNodes();_agRenderAllEdges();
    if(typeof toast==='function')toast('Auto-layout applied','ok');
}

/* ── Drag from palette ────────────────────────────────────────────────────── */
let _agDragOpId=null;
function _agPalDragStart(e,opId){_agDragOpId=opId;e.dataTransfer.effectAllowed='copy';}
function _agCanvasDrop(e){
    e.preventDefault();const opId=_agDragOpId;if(!opId)return;
    const wrap=document.getElementById('ag-canvas-wrap'),rect=wrap.getBoundingClientRect();
    const {pan,scale}=_AG.canvas;
    const x=(e.clientX-rect.left-pan.x)/scale-80,y=(e.clientY-rect.top-pan.y)/scale-35;
    _agAddNode(opId,Math.max(0,x),Math.max(0,y));_agDragOpId=null;
}
function _agAddNode(opId,x,y){
    const opDef=AG_OPERATORS.find(o=>o.id===opId);if(!opDef)return;
    const uid=_agUID();
    _AG.canvas.nodes.push({uid,opId,x,y,label:opDef.label,params:{},configured:false,summary:''});
    _agRenderAllNodes();_agRenderAllEdges();_agUpdateStatus();
    const empty=document.getElementById('ag-canvas-empty');if(empty)empty.style.display='none';
    setTimeout(()=>_agOpenNodeCfg(uid),80);
}

/* ── Render nodes — mirrors PLM node card exactly (with ✏ edit button) ───── */
function _agRenderAllNodes(){
    const container=document.getElementById('ag-canvas-nodes');if(!container)return;
    const {pan,scale}=_AG.canvas;
    container.style.transform=`translate(${pan.x}px,${pan.y}px) scale(${scale})`;
    container.innerHTML='';
    _AG.canvas.nodes.forEach(node=>{
        const opDef=AG_OPERATORS.find(o=>o.id===node.opId)||{label:node.opId,color:'#555',isSource:false,isSink:false};
        const nodeColor=node.customColor||opDef.color;
        const isRunning=_AG.animating;
        const borderColor=node.selected?'rgba(255,255,255,0.65)':isRunning?'rgba(87,198,100,0.75)':node.configured?'rgba(255,255,255,0.18)':'rgba(255,80,80,0.6)';
        const dotColor=isRunning?'#39d353':node.configured?'#55aa55':'#666';

        const div=document.createElement('div');
        div.className='ag-node'+(node.selected?' selected':'')+(isRunning?' running':'');
        div.dataset.uid=node.uid;
        div.style.cssText=`left:${node.x}px;top:${node.y}px;width:162px;background:${nodeColor};color:#fff;border:2px solid ${borderColor};position:absolute;`;

        // Header row — icon + label + badges + ✏ edit button
        const hdr=document.createElement('div');
        hdr.style.cssText='padding:6px 28px 5px 8px;display:flex;align-items:center;gap:6px;pointer-events:none;';

        const iconSpan=document.createElement('span');
        iconSpan.style.cssText='flex-shrink:0;display:flex;align-items:center;pointer-events:none;';
        iconSpan.innerHTML=_agIcon(node.opId);
        hdr.appendChild(iconSpan);

        const meta=document.createElement('div');
        meta.style.cssText='flex:1;min-width:0;pointer-events:none;';

        const labelDiv=document.createElement('div');
        labelDiv.style.cssText='font-size:11px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;pointer-events:none;';
        labelDiv.textContent=node.label||opDef.label;
        meta.appendChild(labelDiv);

        const badgeRow=document.createElement('div');
        badgeRow.style.cssText='font-size:9px;opacity:0.7;display:flex;align-items:center;gap:3px;margin-top:1px;pointer-events:none;';

        const dot=document.createElement('span');
        dot.style.cssText=`width:5px;height:5px;border-radius:50%;background:${dotColor};flex-shrink:0;display:inline-block;pointer-events:none;`;
        badgeRow.appendChild(dot);

        if(opDef.isSource){const b=document.createElement('span');b.style.cssText='background:rgba(0,0,0,0.2);padding:0 3px;border-radius:2px;font-size:8px;pointer-events:none;';b.textContent='SRC';badgeRow.appendChild(b);}
        if(opDef.isSink){const b=document.createElement('span');b.style.cssText='background:rgba(0,0,0,0.2);padding:0 3px;border-radius:2px;font-size:8px;pointer-events:none;';b.textContent='SINK';badgeRow.appendChild(b);}

        const stateSpan=document.createElement('span');
        stateSpan.style.cssText='pointer-events:none;'+(isRunning?'color:#39d353;font-weight:600;':'opacity:0.5;');
        stateSpan.textContent=isRunning?'● running':node.configured?'✓ ready':'⚠ config';
        badgeRow.appendChild(stateSpan);

        // ✏ edit button — identical to PLM
        const editBtn=document.createElement('button');
        editBtn.textContent='✏ edit';
        editBtn.style.cssText='pointer-events:auto;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);color:var(--accent,#00d4aa);font-size:8px;padding:1px 5px;border-radius:3px;cursor:pointer;font-family:var(--mono,monospace);margin-left:auto;flex-shrink:0;line-height:1.4;';
        editBtn.addEventListener('mousedown',e=>{e.stopPropagation();e.preventDefault();});
        editBtn.addEventListener('click',e=>{e.stopPropagation();e.preventDefault();_agOpenNodeCfg(node.uid);});
        badgeRow.appendChild(editBtn);

        meta.appendChild(badgeRow);
        hdr.appendChild(meta);
        div.appendChild(hdr);

        if(node.summary){
            const sum=document.createElement('div');
            sum.style.cssText='padding:0 8px 5px;font-size:9px;opacity:0.5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;pointer-events:none;';
            sum.textContent=node.summary.slice(0,42);
            div.appendChild(sum);
        }

        // × delete button
        const delBtn=document.createElement('button');
        delBtn.textContent='×';delBtn.className='ag-node-del';
        delBtn.style.cssText='position:absolute;top:3px;right:4px;background:none;border:none;color:#fff;opacity:0.5;cursor:pointer;font-size:15px;line-height:1;padding:1px 4px;z-index:10;pointer-events:auto;';
        delBtn.addEventListener('mousedown',e=>{e.stopPropagation();e.preventDefault();});
        delBtn.addEventListener('click',e=>{e.stopPropagation();e.preventDefault();_agDeleteNode(node.uid);});
        div.appendChild(delBtn);

        // Ports
        if(!opDef.isSource){
            const inPort=document.createElement('div');
            inPort.className='ag-port in';inPort.dataset.uid=node.uid;inPort.style.pointerEvents='auto';
            inPort.addEventListener('mousedown',e=>{e.stopPropagation();e.preventDefault();if(_AG.connecting)_agFinishConnect(node.uid);});
            div.appendChild(inPort);
        }
        if(!opDef.isSink){
            const outPort=document.createElement('div');
            outPort.className='ag-port out';outPort.dataset.uid=node.uid;outPort.style.pointerEvents='auto';
            outPort.addEventListener('mousedown',e=>{e.stopPropagation();e.preventDefault();_agStartConnect(e,node.uid);});
            div.appendChild(outPort);
        }

        div.addEventListener('mousedown',e=>{if(e.target.classList.contains('ag-node-del')||e.target.classList.contains('ag-port'))return;_agNodeMouseDown(e,node.uid);});
        div.addEventListener('dblclick',e=>{if(e.target.classList.contains('ag-port'))return;e.stopPropagation();_agOpenNodeCfg(node.uid);});
        container.appendChild(div);
    });
}

/* ── Edges ────────────────────────────────────────────────────────────────── */
function _agRenderAllEdges(){
    const g=document.getElementById('ag-edges-g');if(!g)return;
    const edgeSvg=document.getElementById('ag-canvas-edges');if(!edgeSvg)return;
    const container=document.getElementById('ag-canvas-nodes');if(!container)return;
    const {pan,scale}=_AG.canvas;
    const getPos=(uid,dir)=>{
        const el=container.querySelector(`.ag-node[data-uid="${uid}"]`);if(!el)return null;
        const nX=parseFloat(el.style.left)*scale+pan.x,nY=parseFloat(el.style.top)*scale+pan.y;
        const nW=el.offsetWidth*scale,nH=el.offsetHeight*scale;
        return dir==='out'?{x:nX+nW,y:nY+nH/2}:{x:nX,y:nY+nH/2};
    };
    // Helper: get a node's base colour (customColor or opDef.color)
    const nodeColor=(uid)=>{
        const node=_AG.canvas.nodes.find(n=>n.uid===uid);if(!node)return '#00c4a0';
        const opDef=AG_OPERATORS.find(o=>o.id===node.opId);
        return node.customColor||opDef?.color||'#00c4a0';
    };
    // Build per-edge gradient defs + path elements
    let defs='';
    let paths='';
    _AG.canvas.edges.forEach((edge,i)=>{
        const from=getPos(edge.fromUid,'out'),to=getPos(edge.toUid,'in');if(!from||!to)return;
        const cx1=from.x+(to.x-from.x)*0.45,cy1=from.y,cx2=from.x+(to.x-from.x)*0.55,cy2=to.y;
        const colFrom=nodeColor(edge.fromUid);
        const colTo  =nodeColor(edge.toUid);
        const gradId =`ag-eg-${i}`;
        // Linear gradient from fromNode colour → toNode colour
        defs+=`<linearGradient id="${gradId}" x1="${from.x}" y1="${from.y}" x2="${to.x}" y2="${to.y}" gradientUnits="userSpaceOnUse">
          <stop offset="0%"   stop-color="${colFrom}" stop-opacity="0.85"/>
          <stop offset="100%" stop-color="${colTo}"   stop-opacity="0.75"/>
        </linearGradient>`;
        // Matching arrowhead marker per edge
        const markId=`ag-arr-${i}`;
        defs+=`<marker id="${markId}" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill="${colTo}" opacity="0.8"/>
        </marker>`;
        paths+=`<path d="M${from.x},${from.y} C${cx1},${cy1} ${cx2},${cy2} ${to.x},${to.y}"
          stroke="url(#${gradId})" stroke-width="2" fill="none"
          marker-end="url(#${markId})" opacity="0.9"/>`;
    });
    // Replace defs block inside the edges SVG (keep existing static marker)
    let defsEl=edgeSvg.querySelector('defs');
    if(!defsEl){defsEl=document.createElementNS('http://www.w3.org/2000/svg','defs');edgeSvg.insertBefore(defsEl,edgeSvg.firstChild);}
    // Keep the static ag-arr marker, append gradient defs
    const staticMarker=defsEl.querySelector('#ag-arr');
    defsEl.innerHTML=(staticMarker?staticMarker.outerHTML:'')+defs;
    g.innerHTML=paths;
}

/* ── Mouse interactions ───────────────────────────────────────────────────── */
let _agCfgUid=null;
function _agNodeMouseDown(e,uid){
    e.stopPropagation();
    if(_AG.connecting){_agFinishConnect(uid);return;}
    _AG.canvas.nodes.forEach(n=>n.selected=n.uid===uid);
    _AG.selectedNode=uid;_agRenderAllNodes();
    const wrap=document.getElementById('ag-canvas-wrap'),wRect=wrap.getBoundingClientRect();
    const {pan,scale}=_AG.canvas;
    const node=_AG.canvas.nodes.find(n=>n.uid===uid);if(!node)return;
    _AG.dragNode=uid;
    _AG.dragOffX=(e.clientX-wRect.left-pan.x)/scale-node.x;
    _AG.dragOffY=(e.clientY-wRect.top-pan.y)/scale-node.y;
}
function _agCanvasMouseDown(e){
    const t=e.target,cw=document.getElementById('ag-canvas-wrap');
    if(t===cw||t===document.getElementById('ag-canvas-grid')||t.closest('#ag-canvas-edges')){
        if(_AG.connecting){_agCancelConnect();return;}
        _AG.panDrag=true;_AG.panSX=e.clientX;_AG.panSY=e.clientY;
        _AG.panOX=_AG.canvas.pan.x;_AG.panOY=_AG.canvas.pan.y;
        _AG.canvas.nodes.forEach(n=>n.selected=false);_AG.selectedNode=null;_agRenderAllNodes();
    }
}
function _agCanvasMouseMove(e){
    if(_AG.dragNode){
        const wrap=document.getElementById('ag-canvas-wrap');if(!wrap)return;
        const wRect=wrap.getBoundingClientRect(),{pan,scale}=_AG.canvas;
        const node=_AG.canvas.nodes.find(n=>n.uid===_AG.dragNode);if(!node)return;
        node.x=Math.max(0,(e.clientX-wRect.left-pan.x)/scale-_AG.dragOffX);
        node.y=Math.max(0,(e.clientY-wRect.top-pan.y)/scale-_AG.dragOffY);
        _agRenderAllNodes();_agRenderAllEdges();
    } else if(_AG.panDrag){
        _AG.canvas.pan.x=_AG.panOX+(e.clientX-_AG.panSX);
        _AG.canvas.pan.y=_AG.panOY+(e.clientY-_AG.panSY);
        _agApplyCanvasTransform();
    } else if(_AG.connecting){
        _agDrawConnectingLine(e);
    }
}
function _agCanvasMouseUp(){if(_AG.dragNode){_AG.dragNode=null;_agUpdateStatus();}  _AG.panDrag=false;}
function _agStartConnect(e,fromUid){e.stopPropagation();_AG.connecting={fromUid};const w=document.getElementById('ag-canvas-wrap');if(w)w.style.cursor='crosshair';}
function _agFinishConnect(toUid){
    const {fromUid}=_AG.connecting||{};
    if(!fromUid||fromUid===toUid){_agCancelConnect();return;}
    if(_AG.canvas.edges.find(e=>e.fromUid===fromUid&&e.toUid===toUid)){_agCancelConnect();return;}
    _AG.canvas.edges.push({uid:_agUID(),fromUid,toUid});
    _agCancelConnect();_agRenderAllEdges();_agUpdateStatus();
}
function _agCancelConnect(){
    _AG.connecting=null;
    const w=document.getElementById('ag-canvas-wrap');if(w)w.style.cursor='default';
    const g=document.getElementById('ag-draw-g');if(g)g.innerHTML='';
}
function _agDrawConnectingLine(e){
    const g=document.getElementById('ag-draw-g');if(!g)return;
    const wrap=document.getElementById('ag-canvas-wrap');if(!wrap)return;
    const {fromUid}=_AG.connecting||{};
    const container=document.getElementById('ag-canvas-nodes');
    const {pan,scale}=_AG.canvas;
    const fromEl=container?.querySelector(`.ag-node[data-uid="${fromUid}"]`);if(!fromEl)return;
    const nX=parseFloat(fromEl.style.left)*scale+pan.x,nY=parseFloat(fromEl.style.top)*scale+pan.y;
    const nW=fromEl.offsetWidth*scale,nH=fromEl.offsetHeight*scale;
    const wRect=wrap.getBoundingClientRect();
    const x1=nX+nW,y1=nY+nH/2,x2=e.clientX-wRect.left,y2=e.clientY-wRect.top;
    g.innerHTML=`<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="#00c4a0" stroke-width="2" stroke-dasharray="5 3" opacity="0.85"/><circle cx="${x1}" cy="${y1}" r="4" fill="#00c4a0"/>`;
}
function _agDeleteNode(uid){
    _AG.canvas.nodes=_AG.canvas.nodes.filter(n=>n.uid!==uid);
    _AG.canvas.edges=_AG.canvas.edges.filter(e=>e.fromUid!==uid&&e.toUid!==uid);
    if(_AG.selectedNode===uid)_AG.selectedNode=null;
    _agRenderAllNodes();_agRenderAllEdges();_agUpdateStatus();
    const empty=document.getElementById('ag-canvas-empty');if(empty)empty.style.display=_AG.canvas.nodes.length?'none':'flex';
}
function _agClearCanvas(){
    if(!confirm('Clear all nodes and edges?'))return;
    _AG.canvas.nodes=[];_AG.canvas.edges=[];
    _AG.animating=false;if(_AG.animTimer){cancelAnimationFrame(_AG.animTimer);_AG.animTimer=null;}
    _agRenderAllNodes();_agRenderAllEdges();_agUpdateStatus();
    const empty=document.getElementById('ag-canvas-empty');if(empty)empty.style.display='flex';
}
/* ── Animation / flow particles ───────────────────────────────────────────── */
function _agToggleAnimation(){
    const btn=document.getElementById('ag-run-btn');if(!btn)return;
    if(!_AG.animating){
        if(!_AG.canvas.nodes.length){if(typeof toast==='function')toast('Add nodes first','warn');return;}
        _AG.animating=true;btn.textContent='⏹ Stop';btn.style.color='var(--red)';
        _agStartParticles();_agRenderAllNodes();
    } else {
        _AG.animating=false;btn.textContent='▶ Simulate Flow';btn.style.color='';
        if(_AG.animTimer){cancelAnimationFrame(_AG.animTimer);_AG.animTimer=null;}
        const pg=document.getElementById('ag-particles-g');if(pg)pg.innerHTML='';
        _agRenderAllNodes();
    }
}
function _agStartParticles(){
    const particles=_AG.canvas.edges.flatMap(e=>[{edgeUid:e.uid,t:Math.random()},{edgeUid:e.uid,t:Math.random()*0.5}]);
    const container=document.getElementById('ag-canvas-nodes');
    const animate=()=>{
        if(!_AG.animating)return;
        const pg=document.getElementById('ag-particles-g');if(!pg)return;
        const {pan,scale}=_AG.canvas;
        const getPos=(uid,dir)=>{const el=container?.querySelector(`.ag-node[data-uid="${uid}"]`);if(!el)return null;const nX=parseFloat(el.style.left)*scale+pan.x,nY=parseFloat(el.style.top)*scale+pan.y,nW=el.offsetWidth*scale,nH=el.offsetHeight*scale;return dir==='out'?{x:nX+nW,y:nY+nH/2}:{x:nX,y:nY+nH/2};};
        let html='';
        particles.forEach(p=>{
            const edge=_AG.canvas.edges.find(e=>e.uid===p.edgeUid);if(!edge)return;
            const from=getPos(edge.fromUid,'out'),to=getPos(edge.toUid,'in');if(!from||!to)return;
            p.t+=0.016;if(p.t>=1)p.t=0;
            const t=p.t,mt=1-t,cx1=from.x+(to.x-from.x)*0.45,cy1=from.y,cx2=from.x+(to.x-from.x)*0.55,cy2=to.y;
            const px=mt*mt*mt*from.x+3*mt*mt*t*cx1+3*mt*t*t*cx2+t*t*t*to.x;
            const py=mt*mt*mt*from.y+3*mt*mt*t*cy1+3*mt*t*t*cy2+t*t*t*to.y;
            const alpha=Math.sin(t*Math.PI);
            const _fromNode=_AG.canvas.nodes.find(n=>n.uid===edge.fromUid);
            const _fromOp=AG_OPERATORS.find(o=>o.id===_fromNode?.opId);
            const _pColor=_fromNode?.customColor||_fromOp?.color||'#00c4a0';
            html+=`<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="3.5" fill="${_pColor}" opacity="${alpha.toFixed(2)}"/>`;
        });
        pg.innerHTML=html;
        _AG.animTimer=requestAnimationFrame(animate);
    };
    _AG.animTimer=requestAnimationFrame(animate);
}

/* ── Node config modal (mirrors _plmOpenCfgModal exactly) ─────────────────── */
function _agOpenNodeCfg(uid){
    const old=document.getElementById('ag-cfg-modal');
    if(old){old._agDragClean?.();old.remove();}
    if(_agCfgUid===uid){_agCfgUid=null;return;}
    _agCfgUid=uid;
    const node=_AG.canvas.nodes.find(n=>n.uid===uid);if(!node)return;
    const opDef=AG_OPERATORS.find(o=>o.id===node.opId)||{label:node.opId,color:'#555',params:[],desc:''};
    const nodeColor=node.customColor||opDef.color;
    const inputStyle='width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';

    const paramsHtml=(opDef.params||[]).map(p=>{
        const val=node.params[p.id]!==undefined?node.params[p.id]:(p.val||'');
        const lbl=`<label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">${_agEsc(p.label)}${p.req?'<span style="color:var(--red);"> *</span>':''}</label>`;
        if(p.type==='textarea')return`<div style="margin-bottom:9px;">${lbl}<textarea id="ag-f-${p.id}" style="${inputStyle}min-height:60px;resize:vertical;" placeholder="${_agEsc(p.ph||'')}">${_agEsc(val)}</textarea></div>`;
        if(p.type==='select')return`<div style="margin-bottom:9px;">${lbl}<select id="ag-f-${p.id}" style="${inputStyle}">${(p.opts||[]).map(o=>`<option value="${_agEsc(o)}" ${val===o?'selected':''}>${_agEsc(o)}</option>`).join('')}</select></div>`;
        return`<div style="margin-bottom:9px;">${lbl}<input id="ag-f-${p.id}" type="text" value="${_agEsc(val)}" placeholder="${_agEsc(p.ph||'')}" style="${inputStyle}"/></div>`;
    }).join('');

    const modal=document.createElement('div');
    modal.id='ag-cfg-modal';
    modal.innerHTML=`
<div class="ag-cfg-header" style="background:${nodeColor}18;">
  <span style="color:${nodeColor};display:flex;align-items:center;flex-shrink:0;">${_agIcon(node.opId)}</span>
  <div style="flex:1;min-width:0;">
    <div style="font-size:13px;font-weight:700;color:var(--text0);">${_agEsc(node.label||opDef.label)}</div>
    <div style="font-size:9px;color:var(--text3);">${_agEsc(opDef.desc||opDef.label||'')}</div>
  </div>
  <button id="ag-cfg-x" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:20px;padding:0 4px;flex-shrink:0;line-height:1;">×</button>
</div>
<div class="ag-cfg-body">
  <div style="margin-bottom:9px;">
    <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Node Label</label>
    <input id="ag-f-label" type="text" value="${_agEsc(node.label||opDef.label)}" style="${inputStyle}"/>
  </div>
  <div style="margin-bottom:12px;">
    <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Colour</label>
    <div style="display:flex;gap:6px;align-items:center;">
      <input id="ag-f-color" type="color" value="${nodeColor}" style="width:32px;height:28px;border:none;border-radius:4px;cursor:pointer;"/>
      <input id="ag-f-color-hex" type="text" value="${nodeColor}" style="${inputStyle}width:80px;" oninput="document.getElementById('ag-f-color').value=this.value"/>
      <button onclick="document.getElementById('ag-f-color').value='${_agEsc(opDef.color)}';document.getElementById('ag-f-color-hex').value='${_agEsc(opDef.color)}';" style="font-size:10px;padding:4px 8px;border-radius:4px;border:1px solid var(--border2);background:var(--bg3);color:var(--text2);cursor:pointer;">Reset</button>
    </div>
  </div>
  ${paramsHtml?`<div style="border-top:1px solid var(--border);padding-top:10px;"><div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:10px;">Parameters</div>${paramsHtml}</div>`:''}
</div>
<div class="ag-cfg-footer">
  <button id="ag-cfg-cancel" style="padding:6px 16px;font-size:12px;border-radius:4px;border:1px solid var(--border2);background:var(--bg3);color:var(--text1);cursor:pointer;">Cancel</button>
  <button onclick="_agCfgSave('${uid}')" style="padding:6px 16px;font-size:12px;font-weight:700;border-radius:4px;border:none;background:#00c4a0;color:#fff;cursor:pointer;">✓ Apply</button>
</div>`;

    modal.querySelector('#ag-f-color')?.addEventListener('input',function(){const h=modal.querySelector('#ag-f-color-hex');if(h)h.value=this.value;});
    const close=()=>{modal._agDragClean?.();modal.remove();_agCfgUid=null;};
    modal.querySelector('#ag-cfg-x').addEventListener('click',close);
    modal.querySelector('#ag-cfg-cancel').addEventListener('click',close);

    // Position near node (same logic as PLM)
    const {pan,scale}=_AG.canvas;
    const container=document.getElementById('ag-canvas-nodes');
    const nodeEl=container?.querySelector(`.ag-node[data-uid="${uid}"]`);
    const wrap=document.getElementById('ag-canvas-wrap'),wRect=wrap?.getBoundingClientRect()||{left:0,top:0};
    let mx=window.innerWidth/2-230,my=window.innerHeight/2-200;
    if(nodeEl){mx=parseFloat(nodeEl.style.left)*scale+pan.x+nodeEl.offsetWidth*scale+14+wRect.left;my=parseFloat(nodeEl.style.top)*scale+pan.y+wRect.top;}
    modal.style.left=Math.min(mx,window.innerWidth-480)+'px';
    modal.style.top=Math.max(8,Math.min(my,window.innerHeight-80))+'px';
    document.body.appendChild(modal);
    _agMakeDraggable(modal);
}

function _agCfgSave(uid){
    const node=_AG.canvas.nodes.find(n=>n.uid===uid);if(!node)return;
    const opDef=AG_OPERATORS.find(o=>o.id===node.opId)||{params:[]};
    node.label=document.getElementById('ag-f-label')?.value||node.label;
    const col=document.getElementById('ag-f-color')?.value;
    node.customColor=(col&&col!==opDef.color)?col:null;
    const params={};
    (opDef.params||[]).forEach(p=>{const el=document.getElementById('ag-f-'+p.id);if(el)params[p.id]=el.value;});
    node.params=params;
    node.configured=(opDef.params||[]).filter(p=>p.req&&!params[p.id]).length===0;
    node.summary=(opDef.params||[]).filter(p=>['table_name','topic','model','model_id','agent_name','tool_name','server_name'].includes(p.id)).map(f=>params[f.id]).filter(Boolean).slice(0,2).join(' · ');
    const modal=document.getElementById('ag-cfg-modal');modal?._agDragClean?.();modal?.remove();_agCfgUid=null;
    _agRenderAllNodes();_agUpdateStatus();
    if(typeof toast==='function')toast(`✓ ${node.label} configured`,'ok');
}

function _agMakeDraggable(modal){
    const hdr=modal.querySelector('.ag-cfg-header');if(!hdr)return;
    let active=false,sx=0,sy=0,sl=0,st=0;
    const onDown=e=>{if(e.target.closest('button,input,select,textarea'))return;active=true;sx=e.clientX;sy=e.clientY;sl=parseInt(modal.style.left,10)||0;st=parseInt(modal.style.top,10)||0;e.preventDefault();};
    const onMove=e=>{if(!active)return;modal.style.left=Math.max(0,sl+(e.clientX-sx))+'px';modal.style.top=Math.max(0,st+(e.clientY-sy))+'px';};
    const onUp=()=>{active=false;};
    hdr.addEventListener('mousedown',onDown);window.addEventListener('mousemove',onMove);window.addEventListener('mouseup',onUp);
    modal._agDragClean=()=>{hdr.removeEventListener('mousedown',onDown);window.removeEventListener('mousemove',onMove);window.removeEventListener('mouseup',onUp);};
}

/* ── Status bar ───────────────────────────────────────────────────────────── */
function _agUpdateStatus(){
    const nn=document.getElementById('ag-stat-nodes'),ne=document.getElementById('ag-stat-edges');
    if(nn)nn.textContent=_AG.canvas.nodes.length+' nodes';
    if(ne)ne.textContent=_AG.canvas.edges.length+' edges';
    const sm=document.getElementById('ag-stat-mode');
    if(sm){const agents=_AG.canvas.nodes.filter(n=>['agent_workflow','agent_react','agent_multi'].includes(n.opId));sm.textContent=agents.length?agents.map(a=>a.label).join(', '):'No agent orchestrator yet';}
}
/* ══════════════════════════════════════════════════════════════════════════
   GUIDED WIZARD
   ══════════════════════════════════════════════════════════════════════════ */
const AG_WIZ_STEPS=[
    {label:'Agent Profile'},{label:'Event Source'},{label:'LLM Model'},
    {label:'Memory Config'},{label:'Tools & MCP'},{label:'Orchestration'},
    {label:'Actions & Sinks'},{label:'Observability'},{label:'Review & Build'},
];

function _agRenderWizard(){
    const content=document.getElementById('ag-content');
    const stepsHtml=AG_WIZ_STEPS.map((s,i)=>`
      <div class="ag-wiz-step ${i===_AG.wizStep?'active':i<_AG.wizStep?'done':''}" id="ag-ws-${i}" onclick="_agWizGoStep(${i})">
        <div class="ag-ws-num">${i<_AG.wizStep?'✓':(i+1)}</div>
        <div class="ag-ws-label">${s.label}</div>
      </div>`).join('');
    content.innerHTML=`
<div id="ag-wizard-pane">
  <div id="ag-wiz-steps">
    <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;padding:4px 8px 10px;">Build Steps</div>
    ${stepsHtml}
  </div>
  <div style="flex:1;display:flex;flex-direction:column;overflow:hidden;">
    <div id="ag-wiz-body" style="flex:1;overflow-y:auto;padding:20px 24px;"></div>
    <div id="ag-wiz-footer">
      <button class="ag-tb-btn" id="ag-wiz-back" onclick="_agWizBack()">← Back</button>
      <span style="font-size:10px;color:var(--text3);font-family:var(--mono);" id="ag-wiz-step-lbl"></span>
      <div style="flex:1;"></div>
      <button class="ag-tb-btn purple" id="ag-wiz-next" onclick="_agWizNext()">Next →</button>
      <button class="ag-tb-btn green"  id="ag-wiz-build" style="display:none;" onclick="_agWizBuild()">⚛ Build Agent</button>
    </div>
  </div>
</div>`;
    _agWizGoStep(_AG.wizStep);
}

function _agWizGoStep(n){
    _agWizCollect();_AG.wizStep=n;
    document.querySelectorAll('.ag-wiz-step').forEach((el,i)=>{
        el.classList.toggle('active',i===n);el.classList.toggle('done',i<n);
        el.querySelector('.ag-ws-num').textContent=i<n?'✓':String(i+1);
    });
    const back=document.getElementById('ag-wiz-back'),next=document.getElementById('ag-wiz-next'),build=document.getElementById('ag-wiz-build'),lbl=document.getElementById('ag-wiz-step-lbl');
    if(back)back.style.display=n===0?'none':'';
    if(next)next.style.display=n===AG_WIZ_STEPS.length-1?'none':'';
    if(build)build.style.display=n===AG_WIZ_STEPS.length-1?'':'none';
    if(lbl)lbl.textContent=`Step ${n+1} of ${AG_WIZ_STEPS.length} — ${AG_WIZ_STEPS[n].label}`;
    _agWizRenderStep(n);
}
function _agWizNext(){_agWizGoStep(Math.min(_AG.wizStep+1,AG_WIZ_STEPS.length-1));}
function _agWizBack(){_agWizGoStep(Math.max(_AG.wizStep-1,0));}
function _agWizCollect(){document.querySelectorAll('[id^="ag-wf-"]').forEach(el=>{_AG.wizData[el.id.slice(6)]=el.value;});}

function _agWizRenderStep(n){
    const body=document.getElementById('ag-wiz-body');if(!body)return;
    const d=_AG.wizData;
    const fi=(label,id,ph,val,req)=>`<div style="margin-bottom:10px;"><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">${label}${req?'<span style="color:var(--red)"> *</span>':''}</label><input id="ag-wf-${id}" class="field-input" type="text" value="${_agEsc(val||d[id]||'')}" placeholder="${_agEsc(ph||'')}" style="font-size:11px;font-family:var(--mono);"/></div>`;
    const sel=(label,id,opts,val)=>`<div style="margin-bottom:10px;"><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">${label}</label><select id="ag-wf-${id}" class="field-input" style="font-size:11px;">${opts.map(o=>`<option value="${o}" ${(val||d[id]||opts[0])===o?'selected':''}>${o}</option>`).join('')}</select></div>`;
    const ta=(label,id,ph,val)=>`<div style="margin-bottom:10px;"><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">${label}</label><textarea id="ag-wf-${id}" class="field-input" rows="3" style="font-size:11px;font-family:var(--mono);resize:vertical;" placeholder="${_agEsc(ph||'')}">${_agEsc(val||d[id]||'')}</textarea></div>`;
    const steps={
        0:`<div class="ag-info">Define your agent's identity, orchestration pattern, and deployment configuration.</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">${fi('Agent Name','agent_name','FraudDetectionAgent','',true)}${fi('Description','agent_desc','Detects payment fraud in real-time','')}${sel('Orchestration Pattern','agent_pattern',['WorkflowAgent (Deterministic)','ReActAgent (Reasoning+Acting)','Multi-Agent A2A'],'')}${sel('Implementation Language','agent_lang',['Python','Java'],'')}${fi('Parallelism','agent_parallelism','4','')}${fi('Checkpoint Interval (ms)','agent_checkpoint','10000','')}${sel('Exactly-Once Actions','agent_exactly_once',['enabled','disabled'],'')}${sel('Durable Execution','agent_durable',['enabled','disabled'],'')}</div>`,
        1:_agWizRenderSourceStep(),
        2:`<div class="ag-info">Choose the LLM your agent will use for reasoning and decision-making.</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">${sel('LLM Provider','llm_provider',['OpenAI','Anthropic Claude','Azure AI','AWS Bedrock','Ollama (local)','Mistral AI'],'')}${fi('Model Name / Deployment','llm_model','gpt-4o-mini','',true)}${fi('API Key (env var)','llm_key_env','OPENAI_API_KEY','')}${fi('Base URL Override','llm_endpoint','','')}${fi('Temperature','llm_temperature','0.0','')}${fi('Max Output Tokens','llm_max_tokens','512','')}${fi('Timeout (ms)','llm_timeout','30000','')}${ta('System Prompt','llm_system_prompt','You are a real-time fraud detection agent.','')}</div>`,
        3:`<div class="ag-info">Configure the three-tier memory: Sensory → Short-Term (RocksDB) → Long-Term (Vector Store).</div><div class="ag-card"><div class="ag-section">Sensory Memory (within one agent run)</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('Max Context Tokens','mem_sensory_tokens','4096','')}${sel('Retention Strategy','mem_sensory_strategy',['sliding_window','summarize','truncate'],'')}</div></div><div class="ag-card"><div class="ag-section">Short-Term Memory (across runs — RocksDB state)</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${fi('TTL (hours)','mem_short_ttl','24','')}${fi('Max Entries per Key','mem_short_max','100','')}${fi('Partition Key','mem_short_key','user_id','')}</div></div><div class="ag-card"><div class="ag-section">Long-Term Memory (semantic vector retrieval)</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">${sel('Vector Store','mem_lt_store',['Elasticsearch','Redis VSS','Milvus','Qdrant','None'],'')}${fi('Vector Store URL','mem_lt_url','http://elasticsearch:9200','')}${fi('Embedding Model','mem_lt_embed','text-embedding-3-small','')}${fi('Recall Top-K','mem_lt_topk','5','')}</div></div>`,
        4:`<div class="ag-info">Register HTTP tools and MCP servers your agent can invoke during execution.</div><div class="ag-card"><div class="ag-section">HTTP Tool</div>${fi('Tool Name','tool1_name','check_fraud_score','')}${fi('Tool Endpoint URL','tool1_url','https://api.example.com/score','')}${ta('Tool Description (shown to LLM)','tool1_desc','Checks real-time fraud score. Input: transaction_id (string).','')} </div><div class="ag-card"><div class="ag-section">MCP Server</div>${fi('MCP Server Name','mcp_name','my-tool-server','')}${fi('MCP Server URL','mcp_url','http://mcp-server:3000/sse','')}${sel('MCP Transport','mcp_transport',['SSE','stdio','WebSocket'],'')}</div>`,
        5:`<div class="ag-info">Configure the orchestration strategy that drives your agent's reasoning loop.</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">${sel('Execution Pattern','orch_pattern',['WorkflowAgent — Sequential','WorkflowAgent — Parallel','ReActAgent — Think/Act/Observe','Custom Hybrid'],'')}${fi('Max ReAct Iterations','orch_max_iter','10','')}${sel('A2A Protocol','orch_a2a',['disabled','enabled — coordinator','enabled — sub-agent'],'')}${fi('A2A Endpoint URL','orch_a2a_url','http://agent-router:8080/a2a','')}${fi('Child Agent IDs (comma-sep)','orch_children','','')}</div>`,
        6:`<div class="ag-info">Define where your agent writes decisions, alerts, and results.</div><div class="ag-card"><div class="ag-section">Primary Output Sink</div>${sel('Sink Type','sink_type',['Kafka','JDBC (PostgreSQL)','HTTP Webhook','Print (debug)'],'')}${fi('Output Table Name','sink_table','agent_output','',true)}${fi('Endpoint / Topic / URL','sink_endpoint','agent-output-events','')}${sel('Output Format','sink_format',['json','avro','string'],'')}</div><div class="ag-card"><div class="ag-section">Alert Sink (secondary)</div>${sel('Alert Sink Type','alert_sink',['None','Kafka Topic','Slack Webhook'],'')}${fi('Alert Endpoint','alert_endpoint','','')}</div>`,
        7:`<div class="ag-info">Configure the agent event log topic for the Observability tab.</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">${sel('Event Log Sink','obs_sink',['Kafka','Elasticsearch','Print (debug)','Disable'],'')}${fi('Log Topic / Index','obs_topic','flink-agent-events','')}${sel('Log LLM Calls','obs_llm',['yes','no'],'')}${sel('Log Tool Invocations','obs_tools',['yes','no'],'')}${sel('Log Memory Ops','obs_memory',['no','yes'],'')}${fi('Retention (days)','obs_retention','30','')}</div>`,
        8:_agWizRenderReviewStep(),
    };
    body.innerHTML=`<div>${steps[n]||'<div>Step content coming soon.</div>'}</div>`;
}

function _agWizBuild(){
    _agWizCollect();
    const d=_AG.wizData;
    _AG.canvas.nodes=[];_AG.canvas.edges=[];
    let uid=0;
    const mk=(opId,label,x,y)=>{const u='wn'+(++uid);_AG.canvas.nodes.push({uid:u,opId,x,y,label,params:{},configured:true,summary:label});return u;};
    const srcOpId=d.source_type==='Datagen (testing)'?'datagen_event':d.source_type==='JDBC'?'jdbc_event':'kafka_event';
    const llmOpId={'OpenAI':'llm_openai','Anthropic Claude':'llm_anthropic','Azure AI':'llm_azureai','AWS Bedrock':'llm_bedrock','Ollama (local)':'llm_ollama','Mistral AI':'llm_mistral'}[d.llm_provider]||'llm_openai';
    const orchOpId=d.orch_pattern?.includes('ReAct')?'agent_react':'agent_workflow';
    const sinkOpId={'Kafka':'action_kafka','JDBC (PostgreSQL)':'action_jdbc','HTTP Webhook':'action_http','Print (debug)':'sink_print'}[d.sink_type]||'action_kafka';
    const srcUid=mk(srcOpId,d.source_table||'Events',40,120);
    const memUid=mk('mem_short','Short-Term Memory',220,40);
    const llmUid=mk(llmOpId,d.llm_model||'LLM',220,160);
    const orchUid=mk(orchOpId,d.agent_name||'Agent',420,110);
    const sinkUid=mk(sinkOpId,d.sink_table||'Output',600,110);
    const obsUid=mk('event_log','Event Log',600,220);
    if(d.mcp_url){const mId=mk('mcp_server',d.mcp_name||'MCP Server',220,280);_AG.canvas.edges.push({uid:_agUID(),fromUid:mId,toUid:orchUid});}
    if(d.tool1_name){const tId=mk('tool_http',d.tool1_name,420,260);_AG.canvas.edges.push({uid:_agUID(),fromUid:orchUid,toUid:tId});}
    _AG.canvas.edges.push({uid:_agUID(),fromUid:srcUid,toUid:orchUid},{uid:_agUID(),fromUid:memUid,toUid:orchUid},{uid:_agUID(),fromUid:llmUid,toUid:orchUid},{uid:_agUID(),fromUid:orchUid,toUid:sinkUid},{uid:_agUID(),fromUid:orchUid,toUid:obsUid});
    _agSaveHistory({type:'wizard',agent_name:d.agent_name,nodes:_AG.canvas.nodes.length});_agUpdateHistCount();
    if(typeof toast==='function')toast('Agent built — switching to Canvas','ok');
    _agSwitchTab('canvas');
    setTimeout(()=>{_agAutoLayout();_agFitToView();},200);
}

/* ══════════════════════════════════════════════════════════════════════════
   CODE GENERATION
   ══════════════════════════════════════════════════════════════════════════ */
function _agRenderCode(){
    const content=document.getElementById('ag-content');
    content.innerHTML=`
<div id="ag-code-pane">
  <div class="ag-code-tabs">
    <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;padding:4px 8px 10px;">Output</div>
    <button class="ag-code-tab active" id="ag-ct-sql"          onclick="_agCodeTab('sql')">Flink SQL</button>
    <button class="ag-code-tab"        id="ag-ct-java"         onclick="_agCodeTab('java')">Java (API)</button>
    <button class="ag-code-tab"        id="ag-ct-python"       onclick="_agCodeTab('python')">Python (PyFlink)</button>
    <button class="ag-code-tab"        id="ag-ct-flip531"      onclick="_agCodeTab('flip531')">FLIP-531 Native ✦</button>
    <button class="ag-code-tab"        id="ag-ct-pom"          onclick="_agCodeTab('pom')">pom.xml</button>
    <button class="ag-code-tab"        id="ag-ct-requirements" onclick="_agCodeTab('requirements')">requirements.txt</button>
    <button class="ag-code-tab"        id="ag-ct-docker"       onclick="_agCodeTab('docker')">docker-compose</button>
    <button class="ag-code-tab"        id="ag-ct-k8s"          onclick="_agCodeTab('k8s')">Kubernetes YAML</button>
    <div style="flex:1;"></div>
    <button onclick="_agCopyCode()" class="ag-tb-btn" style="margin:4px 6px;font-size:9px;padding:3px 8px;">Copy</button>
    <button onclick="_agInsertCodeToEditor()" class="ag-tb-btn purple" style="margin:0 6px 4px;font-size:9px;padding:3px 8px;">⤵ Insert SQL</button>
  </div>
  <pre id="ag-code-output">${_agEsc(_agGenerateCode('sql'))}</pre>
</div>`;
}

function _agCodeTab(tab){
    _AG.codeTab=tab;
    document.querySelectorAll('.ag-code-tab').forEach(b=>b.classList.remove('active'));
    document.getElementById('ag-ct-'+tab)?.classList.add('active');
    const out=document.getElementById('ag-code-output');if(out)out.textContent=_agGenerateCode(tab);
}
function _agCopyCode(){const out=document.getElementById('ag-code-output');if(!out)return;navigator.clipboard.writeText(out.textContent).then(()=>{if(typeof toast==='function')toast('Code copied','ok');});}
function _agInsertCodeToEditor(){const sql=_agGenerateCode('sql');const ed=document.getElementById('sql-editor');if(!ed)return;const prefix=ed.value.trim()?'\n\n':'';ed.value+=prefix+sql+'\n';if(typeof updateLineNumbers==='function')updateLineNumbers();closeModal('ag-modal');if(typeof toast==='function')toast('Agent SQL inserted','ok');}

function _agGenerateCode(type){
    const nodes=_AG.canvas.nodes,d=_AG.wizData;
    const agentName=d.agent_name||nodes.find(n=>['agent_workflow','agent_react','agent_multi'].includes(n.opId))?.label||'MyFlinkAgent';
    if(type==='sql')         return _agGenSQL(nodes,d,agentName);
    if(type==='java')        return _agGenJava(nodes,d,agentName);
    if(type==='python')      return _agGenPython(nodes,d,agentName);
    if(type==='flip531')     return _agGenFlip531(nodes,d,agentName);
    if(type==='pom')         return _agGenPom(agentName);
    if(type==='requirements')return _agGenRequirements();
    if(type==='docker')      return _agGenDocker(agentName);
    if(type==='k8s')         return _agGenK8s(agentName);
    return '-- Select a code type';
}
/* ══════════════════════════════════════════════════════════════════════════
   CODE GENERATORS
   ══════════════════════════════════════════════════════════════════════════ */
function _agGenSQL(nodes,d,agentName){
    const lines=[];
    const srcNodes=nodes.filter(n=>['kafka_event','datagen_event','jdbc_event','kinesis_event'].includes(n.opId));
    const llmNodes=nodes.filter(n=>n.opId.startsWith('llm_'));
    const sinkNodes=nodes.filter(n=>['action_kafka','action_jdbc','sink_print','sink_blackhole'].includes(n.opId));
    const srcTbl=srcNodes[0]?.params?.table_name||d.source_table||'agent_events';
    const sinkTbl=sinkNodes[0]?.params?.table_name||d.sink_table||'agent_output';
    lines.push(`-- ══════════════════════════════════════════════════════`);
    lines.push(`-- Flink Agent: ${agentName}`);
    lines.push(`-- Framework: Apache Flink Agents 0.2`);
    lines.push(`-- Generated: ${new Date().toISOString()}`);
    lines.push(`-- ══════════════════════════════════════════════════════\n`);
    lines.push(`SET 'execution.runtime-mode' = 'streaming';`);
    lines.push(`SET 'parallelism.default'    = '${d.agent_parallelism||4}';`);
    lines.push(`SET 'execution.checkpointing.interval' = '${d.agent_checkpoint||10000}';\n`);
    srcNodes.forEach(n=>{
        const p=n.params,tbl=p.table_name||d.source_table||'agent_events';
        const schema=(p.schema||d.source_schema||'event_id BIGINT\npayload STRING\nevent_time TIMESTAMP(3)').split('\n').map(l=>l.trim()).filter(Boolean);
        lines.push(`-- Source: ${tbl}`);
        if(n.opId==='kafka_event'){
            lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (`);
            schema.forEach(col=>lines.push(`  ${col},`));
            lines.push(`  WATERMARK FOR ${p.watermark_col||d.source_wm_col||'event_time'} AS ${p.watermark_col||d.source_wm_col||'event_time'} - INTERVAL '${p.watermark_delay||d.source_wm_delay||5}' SECOND`);
            lines.push(`) WITH (`);
            lines.push(`  'connector'                    = 'kafka',`);
            lines.push(`  'topic'                        = '${p.topic||d.source_topic||'agent-input'}',`);
            lines.push(`  'properties.bootstrap.servers' = '${p.bootstrap||d.source_endpoint||'kafka:9092'}',`);
            lines.push(`  'properties.group.id'          = '${agentName.toLowerCase()}-consumer',`);
            lines.push(`  'scan.startup.mode'            = '${p.startup_mode||'latest-offset'}',`);
            lines.push(`  'format'                       = '${p.format||d.source_format||'json'}'`);
            lines.push(`);\n`);
        } else if(n.opId==='datagen_event'){
            lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (`);
            schema.forEach((col,i)=>lines.push(`  ${col}${i<schema.length-1?',':''}`));
            lines.push(`) WITH (`);
            lines.push(`  'connector'       = 'datagen',`);
            lines.push(`  'rows-per-second' = '${p.rows_per_second||50}'`);
            lines.push(`);\n`);
        }
    });
    llmNodes.forEach(n=>{
        const p=n.params;
        lines.push(`-- LLM Model: ${n.label}`);
        lines.push(`CREATE MODEL IF NOT EXISTS ${agentName}_llm`);
        lines.push(`WITH (`);
        lines.push(`  'provider'    = '${n.opId.replace('llm_','').toUpperCase()}',`);
        lines.push(`  'model'       = '${p.model||p.model_id||d.llm_model||'gpt-4o-mini'}',`);
        lines.push(`  'api-key'     = '${p.api_key_env||d.llm_key_env||'OPENAI_API_KEY'}',`);
        lines.push(`  'temperature' = '${p.temperature||d.llm_temperature||'0.0'}',`);
        lines.push(`  'max-tokens'  = '${p.max_tokens||d.llm_max_tokens||'512'}'`);
        lines.push(`);\n`);
    });
    const orchNodes=nodes.filter(n=>['agent_workflow','agent_react','agent_multi'].includes(n.opId));
    if(orchNodes.length){
        lines.push(`-- Register agent UDF`);
        lines.push(`CREATE TEMPORARY FUNCTION IF NOT EXISTS ${agentName.toUpperCase()}_AGENT`);
        lines.push(`  AS 'com.yourcompany.agents.${agentName}'`);
        lines.push(`  LANGUAGE ${d.agent_lang==='Java'?'JAVA':'PYTHON'};\n`);
    }
    sinkNodes.forEach(n=>{
        const p=n.params,tbl=p.table_name||d.sink_table||'agent_output';
        if(n.opId==='action_kafka'){
            lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (`);
            lines.push(`  'connector'                    = 'kafka',`);
            lines.push(`  'topic'                        = '${p.topic||d.sink_endpoint||'agent-output'}',`);
            lines.push(`  'properties.bootstrap.servers' = '${p.bootstrap||d.source_endpoint||'kafka:9092'}',`);
            lines.push(`  'format'                       = '${p.format||d.sink_format||'json'}'`);
            lines.push(`) LIKE ${srcTbl} (EXCLUDING ALL);\n`);
        } else if(n.opId==='sink_print'){
            lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (`);
            lines.push(`  'connector' = 'print'`);
            lines.push(`) LIKE ${srcTbl} (EXCLUDING ALL);\n`);
        } else if(n.opId==='sink_blackhole'){
            lines.push(`CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (`);
            lines.push(`  'connector' = 'blackhole'`);
            lines.push(`) LIKE ${srcTbl} (EXCLUDING ALL);\n`);
        }
    });
    lines.push(`-- Pipeline execution`);
    lines.push(`INSERT INTO ${sinkTbl}`);
    lines.push(`SELECT`);
    lines.push(`  event_id,`);
    lines.push(`  '${agentName}' AS agent_name,`);
    lines.push(`  ${agentName.toUpperCase()}_AGENT(payload) AS decision,`);
    lines.push(`  CURRENT_TIMESTAMP AS processed_at`);
    lines.push(`FROM ${srcTbl};`);
    return lines.join('\n');
}

function _agGenFlip531(nodes,d,agentName){
    const isReact=nodes.some(n=>n.opId==='agent_react');
    const llmNode=nodes.find(n=>n.opId.startsWith('llm_'));
    const srcNode=nodes.find(n=>n.opId.endsWith('_event'));
    const sinkNode=nodes.find(n=>n.opId.startsWith('action_'));
    return `-- FLIP-531 Native SQL Agent Syntax (Experimental — Flink Agents 0.2+)
-- Agent: ${agentName}  |  Pattern: ${isReact?'ReActAgent':'WorkflowAgent'}
-- ⚠ Experimental — subject to change in future Flink Agents releases

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default'    = '${d.agent_parallelism||4}';

CREATE MODEL IF NOT EXISTS ${agentName}_llm
WITH (
  'provider'    = '${(llmNode?.opId||'llm_openai').replace('llm_','').toUpperCase()}',
  'model'       = '${llmNode?.params?.model||d.llm_model||'gpt-4o-mini'}',
  'api-key'     = '${llmNode?.params?.api_key_env||d.llm_key_env||'OPENAI_API_KEY'}',
  'temperature' = '${d.llm_temperature||'0.0'}',
  'max-tokens'  = '${d.llm_max_tokens||'512'}'
);

CREATE AGENT IF NOT EXISTS ${agentName} (
  TYPE              = '${isReact?'REACT':'WORKFLOW'}',
  MODEL             = ${agentName}_llm,
  SYSTEM_PROMPT     = '${_agEsc(d.llm_system_prompt||'You are an event-driven AI agent.')}',
  EXACTLY_ONCE      = ${d.agent_exactly_once!=='disabled'?'TRUE':'FALSE'},
  DURABLE_EXECUTION = ${d.agent_durable!=='disabled'?'TRUE':'FALSE'},
  PARALLELISM       = ${d.agent_parallelism||4},
  CHECKPOINT_INTERVAL = '${d.agent_checkpoint||10000} ms'
);

RUN AGENT ${agentName}
  INPUT         = TABLE ${srcNode?.params?.table_name||d.source_table||'agent_events'},
  OUTPUT        = TABLE ${sinkNode?.params?.table_name||d.sink_table||'agent_output'},
  INPUT_COLUMN  = 'payload',
  OUTPUT_COLUMN = 'agent_decision';

-- SHOW AGENTS;
-- DESCRIBE AGENT ${agentName};
-- DROP AGENT IF EXISTS ${agentName};
`;
}

function _agGenJava(nodes,d,agentName){
    const isReact=d.agent_pattern?.includes('ReAct')||nodes.some(n=>n.opId==='agent_react');
    return `// Flink Agent: ${agentName}
// Framework: Apache Flink Agents 0.2 — Java API
// Pattern: ${isReact?'ReActAgent':'WorkflowAgent'}

package com.yourcompany.agents;

import org.apache.flink.agents.api.java.*;
import org.apache.flink.agents.api.java.memory.*;
import org.apache.flink.agents.api.java.tools.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ${agentName} extends ${isReact?'ReActAgent':'WorkflowAgent'}<EventRow, DecisionRow> {

    private final ChatModel llm = ChatModel.builder()
        .provider("${(d.llm_provider||'OpenAI').replace(/ /g,'')}")
        .model("${d.llm_model||'gpt-4o-mini'}")
        .apiKey(System.getenv("${d.llm_key_env||'OPENAI_API_KEY'}"))
        .temperature(${d.llm_temperature||'0.0'})
        .maxTokens(${d.llm_max_tokens||512})
        .build();

    private final ShortTermMemory shortTermMemory = ShortTermMemory.builder()
        .partitionKey("${d.mem_short_key||'user_id'}")
        .ttl(java.time.Duration.ofHours(${d.mem_short_ttl||24}))
        .maxEntries(${d.mem_short_max||100})
        .build();

    private final Tool scoreTool = Tool.http()
        .name("${d.tool1_name||'check_score'}")
        .description("${d.tool1_desc||'Score a transaction'}")
        .endpoint("${d.tool1_url||'https://api.example.com/score'}")
        .method("POST")
        .timeout(${d.llm_timeout||5000})
        .build();

${isReact?`    @Override
    public DecisionRow reason(EventRow event, AgentContext ctx) {
        String memory = shortTermMemory.recall(event.getUserId(), ctx);
        LlmResponse response = llm.chat(
            Message.system("${d.llm_system_prompt||'You are a fraud detection agent.'}"),
            Message.user("Event: " + event.toJson() + "\\nContext: " + memory)
        );
        if (response.hasToolCalls()) {
            for (ToolCall call : response.getToolCalls()) {
                String result = invokeToolDurable(call);
                ctx.addObservation(call.getName(), result);
            }
            return reason(event, ctx);
        }
        shortTermMemory.store(event.getUserId(), response.getText(), ctx);
        return DecisionRow.fromResponse(event, response);
    }`:
        `    @Override
    public List<AgentStep> defineWorkflow() {
        return AgentSteps.of(
            step("retrieve-context").run(ctx -> {
                ctx.set("memory", shortTermMemory.recall(ctx.getInput("user_id"), ctx));
            }),
            step("llm-decision").durable().run(ctx -> {
                LlmResponse resp = llm.chat(
                    Message.system("${d.llm_system_prompt||'You are a fraud detection agent.'}"),
                    Message.user(ctx.buildPrompt())
                );
                ctx.set("decision", resp.getText());
            }),
            step("emit-result").run(ctx -> {
                shortTermMemory.store(ctx.getInput("user_id"), ctx.get("decision"), ctx);
                ctx.emit(DecisionRow.from(ctx));
            })
        );
    }`}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(${d.agent_checkpoint||10000});
        env.setParallelism(${d.agent_parallelism||4});
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryFunction("${agentName.toUpperCase()}_AGENT", ${agentName}.class);
        tEnv.executeSql("-- paste generated Flink SQL here");
    }
}`;
}

function _agGenPython(nodes,d,agentName){
    const isReact=d.agent_pattern?.includes('ReAct')||nodes.some(n=>n.opId==='agent_react');
    return `# Flink Agent: ${agentName}
# Framework: Apache Flink Agents 0.2 — Python API (PyFlink)
# Pattern: ${isReact?'ReActAgent':'WorkflowAgent'}

from flink_agents.api import (
    ${isReact?'ReActAgent':'WorkflowAgent'}, AgentConfig, AgentContext,
    ChatModel, Tool, ShortTermMemory,
)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
import os
from dataclasses import dataclass

@dataclass
class EventRow:
    event_id: int
    user_id: str
    payload: str

@dataclass
class DecisionRow:
    event_id: int
    agent_name: str
    decision: str
    confidence: float

class ${agentName}(${isReact?'ReActAgent':'WorkflowAgent'}[EventRow, DecisionRow]):
    config = AgentConfig(
        name="${agentName}",
        parallelism=${d.agent_parallelism||4},
        checkpoint_interval_ms=${d.agent_checkpoint||10000},
        exactly_once=${d.agent_exactly_once!=='disabled'?'True':'False'},
        durable_execution=${d.agent_durable!=='disabled'?'True':'False'},
    )
    llm = ChatModel(
        provider="${(d.llm_provider||'openai').toLowerCase().replace(/ /g,'_')}",
        model="${d.llm_model||'gpt-4o-mini'}",
        api_key=os.environ.get("${d.llm_key_env||'OPENAI_API_KEY'}"),
        temperature=${d.llm_temperature||0.0},
        max_tokens=${d.llm_max_tokens||512},
        system_prompt="""${d.llm_system_prompt||'You are a real-time event analysis agent.'}""",
    )
    short_term_memory = ShortTermMemory(
        partition_key="${d.mem_short_key||'user_id'}",
        ttl_hours=${d.mem_short_ttl||24},
        max_entries=${d.mem_short_max||100},
    )
    tools = [
        Tool.http(
            name="${d.tool1_name||'check_score'}",
            description="${d.tool1_desc||'Score an event'}",
            url="${d.tool1_url||'https://api.example.com/score'}",
            method="POST",
        ),
    ]

${isReact?`    async def reason(self, event: EventRow, ctx: AgentContext) -> DecisionRow:
        memory = await self.short_term_memory.recall(event.user_id, ctx)
        for _ in range(${d.orch_max_iter||10}):
            response = await self.llm.chat(
                f"Event: {event.payload}\\nContext: {memory}\\nDecision:",
                ctx=ctx,
            )
            if response.has_tool_calls:
                for call in response.tool_calls:
                    result = await ctx.invoke_tool_durable(call)
                    ctx.add_observation(call.name, result)
                continue
            await self.short_term_memory.store(event.user_id, response.text, ctx)
            return DecisionRow(
                event_id=event.event_id,
                agent_name="${agentName}",
                decision=response.text,
                confidence=0.95,
            )`:
        `    async def run(self, event: EventRow, ctx: AgentContext) -> DecisionRow:
        memory = await self.short_term_memory.recall(event.user_id, ctx)
        async with ctx.durable_block("llm-decision"):
            response = await self.llm.chat(
                f"Event: {event.payload}\\nContext: {memory}\\nDecide: FRAUD or LEGITIMATE",
                ctx=ctx,
            )
        await self.short_term_memory.store(event.user_id, response.text, ctx)
        return DecisionRow(
            event_id=event.event_id,
            agent_name="${agentName}",
            decision=response.text,
            confidence=0.95,
        )`}

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(${d.agent_checkpoint||10000})
    t_env = StreamTableEnvironment.create(env)
    t_env.register_function("${agentName.toUpperCase()}_AGENT", ${agentName}())
    t_env.execute_sql(open("agent_pipeline.sql").read())

if __name__ == "__main__":
    main()
`;
}

function _agGenPom(n){const l=n.toLowerCase().replace(/\s+/g,'-');return`<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.yourcompany.agents</groupId>
  <artifactId>${l}</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <properties>
    <flink.version>2.2.0</flink.version>
    <flink.agents.version>0.2.0</flink.agents.version>
    <java.version>11</java.version>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
  </properties>
  <dependencies>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-api-java-bridge</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-agents-java</artifactId><version>\${flink.agents.version}</version></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId><version>3.3.0-1.20</version></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-jdbc</artifactId><version>3.2.0-1.19</version></dependency>
  </dependencies>
  <build><plugins><plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.0</version>
    <executions><execution><phase>package</phase><goals><goal>shade</goal></goals>
    <configuration><transformers><transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
    <mainClass>com.yourcompany.agents.${n}</mainClass>
    </transformer></transformers></configuration></execution></executions>
  </plugin></plugins></build>
</project>`;}

function _agGenRequirements(){return`# Flink Agent Python Dependencies (Flink Agents 0.2)
apache-flink==2.2.0
flink-agents-python==0.2.0
openai>=1.40.0
anthropic>=0.30.0
mistralai>=1.0.0
boto3>=1.34.0
azure-ai-inference>=1.0.0
sentence-transformers>=2.7.0
elasticsearch>=8.13.0
redis>=5.0.0
pymilvus>=2.4.0
qdrant-client>=1.10.0
mcp-python>=0.1.0
pydantic>=2.7.0
aiohttp>=3.9.0
tenacity>=8.3.0
`;}

function _agGenDocker(n){const l=n.toLowerCase().replace(/\s+/g,'-');return`# docker-compose.yml — ${n} Agent Stack (Flink Agents 0.2)
version: '3.9'
services:
  jobmanager:
    image: apache/flink:2.2.0-scala_2.12-java11
    ports: ["8081:8081"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        state.backend: rocksdb
        execution.checkpointing.interval: 10000
        flink.agents.action-store.type: rocksdb
        flink.agents.durable-execution.enabled: true
    command: jobmanager
    volumes:
      - ./${l}.jar:/opt/flink/usrlib/agent.jar
  taskmanager:
    image: apache/flink:2.2.0-scala_2.12-java11
    depends_on: [jobmanager]
    scale: 2
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
    command: taskmanager
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    depends_on: [zookeeper]
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment: {ZOOKEEPER_CLIENT_PORT: 2181}
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.14.0
    environment: [discovery.type=single-node, xpack.security.enabled=false]
    ports: ["9200:9200"]
  redis:
    image: redis/redis-stack:7.2.0-v9
    ports: ["6379:6379"]
`;}

function _agGenK8s(n){const l=n.toLowerCase().replace(/\s+/g,'-');return`# kubernetes.yaml — ${n} (Flink Kubernetes Operator 1.9+)
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: ${l}
  namespace: flink-agents
spec:
  image: apache/flink:2.2.0-scala_2.12-java11
  flinkVersion: v2_2
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend: rocksdb
    state.checkpoints.dir: "s3://your-bucket/flink/checkpoints/${l}"
    execution.checkpointing.interval: "10000"
    flink.agents.action-store.type: rocksdb
    flink.agents.durable-execution.enabled: "true"
  serviceAccount: flink-service-account
  jobManager:
    resource: {memory: "2048m", cpu: 1}
  taskManager:
    resource: {memory: "4096m", cpu: 2}
    replicas: 2
  job:
    jarURI: "s3://your-bucket/jars/${l}-1.0.0.jar"
    entryClass: "com.yourcompany.agents.${n}"
    parallelism: 4
    upgradeMode: stateful
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - {name: OPENAI_API_KEY,    valueFrom: {secretKeyRef: {name: agent-secrets, key: openai-api-key}}}
            - {name: ANTHROPIC_API_KEY, valueFrom: {secretKeyRef: {name: agent-secrets, key: anthropic-api-key}}}
`;}
/* ══════════════════════════════════════════════════════════════════════════
   JAR UPLOAD TAB
   ══════════════════════════════════════════════════════════════════════════ */
function _agRenderJar(){
    const content=document.getElementById('ag-content');
    content.innerHTML=`
<div style="display:flex;flex:1;overflow:hidden;">
  <div style="flex:1;overflow-y:auto;padding:18px 20px;border-right:1px solid var(--border);">
    <div class="ag-info">Upload a compiled Flink Agent JAR implementing <code>WorkflowAgent</code> or <code>ReActAgent</code> from flink-agents-java 0.2.0. Build with: <code>mvn package -DskipTests</code></div>
    <div id="ag-jar-dropzone" onclick="document.getElementById('ag-jar-file-input').click()"
      ondragover="_agJarDragOver(event)" ondragleave="_agJarDragLeave(event)" ondrop="_agJarDrop(event)">
      <div style="color:var(--text3);">${_agSvgIcon('<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>',36)}</div>
      <div id="ag-jar-dz-title" style="font-size:12px;font-weight:600;color:var(--text0);">Drop Agent JAR here or click to browse</div>
      <div id="ag-jar-dz-sub"   style="font-size:10px;color:var(--text3);">Compiled Flink Agent fat JAR (.jar)</div>
      <input type="file" id="ag-jar-file-input" accept=".jar" style="display:none;" onchange="_agJarFileSelected(event)"/>
    </div>
    <div id="ag-jar-file-card" style="display:none;background:var(--bg2);border:1px solid rgba(0,176,143,0.2);border-radius:5px;padding:9px 11px;margin-top:8px;flex-direction:row;align-items:center;gap:8px;">
      <div style="flex-shrink:0;color:var(--text2);">${_agSvgIcon('<path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>',22)}</div>
      <div style="flex:1;"><div id="ag-jar-fname" style="font-size:11px;font-weight:600;color:var(--text0);"></div><div id="ag-jar-fmeta" style="font-size:10px;color:var(--text3);"></div></div>
      <button onclick="_agJarClearFile()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:15px;">×</button>
    </div>
    <div style="margin-top:14px;display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      <div><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Entry Class</label><input id="ag-jar-class" class="field-input" type="text" placeholder="com.yourcompany.agents.MyAgent" style="font-size:11px;font-family:var(--mono);"/></div>
      <div><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Parallelism</label><input id="ag-jar-par" class="field-input" type="number" placeholder="4" style="font-size:11px;"/></div>
      <div><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Program Arguments</label><input id="ag-jar-args" class="field-input" type="text" placeholder="--agent.name MyAgent" style="font-size:11px;font-family:var(--mono);"/></div>
      <div><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Savepoint Path (optional)</label><input id="ag-jar-sp" class="field-input" type="text" placeholder="s3://bucket/sp-..." style="font-size:11px;font-family:var(--mono);"/></div>
    </div>
    <label style="display:flex;align-items:center;gap:7px;font-size:11px;color:var(--text1);cursor:pointer;margin-top:8px;"><input type="checkbox" id="ag-jar-allow-nr"/> Allow non-restored state</label>
    <div id="ag-jar-progress-wrap" style="display:none;margin-top:10px;">
      <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text1);margin-bottom:4px;"><span id="ag-jar-prog-lbl">Uploading…</span><span id="ag-jar-prog-pct" style="font-family:var(--mono);color:#00c4a0;">0%</span></div>
      <div style="background:var(--bg3);border-radius:3px;height:5px;overflow:hidden;"><div id="ag-jar-prog-bar" style="height:100%;width:0%;border-radius:3px;background:linear-gradient(90deg,#00c4a0,#00d4aa);transition:width 0.2s;"></div></div>
    </div>
    <div style="display:flex;gap:8px;align-items:center;margin-top:14px;">
      <div id="ag-jar-status" style="flex:1;font-size:11px;font-family:var(--mono);color:var(--text2);"></div>
      <button class="ag-tb-btn" onclick="_agJarReset()">✕ Clear</button>
      <button class="ag-tb-btn purple" onclick="_agJarSubmit()" style="min-width:160px;justify-content:center;">Upload &amp; Submit Agent</button>
    </div>
  </div>
  <div style="width:280px;flex-shrink:0;overflow-y:auto;padding:12px 14px;">
    <div style="display:flex;align-items:center;margin-bottom:8px;">
      <span style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:0.8px;text-transform:uppercase;flex:1;">Cluster JARs</span>
      <button onclick="_agJarRefreshCluster()" style="font-size:10px;background:none;border:1px solid var(--border);border-radius:2px;color:var(--text2);cursor:pointer;padding:2px 7px;">⟳</button>
    </div>
    <div id="ag-jar-cluster-list"><span style="font-size:10px;color:var(--text3);">Connect then click ⟳</span></div>
    <div style="margin-top:14px;border-top:1px solid var(--border);padding-top:12px;">
      <div style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:8px;">Submit History</div>
      <div id="ag-jar-history"><span style="font-size:10px;color:var(--text3);">No submissions yet.</span></div>
    </div>
  </div>
</div>`;
    _agJarRefreshCluster();_agJarRenderHistory();
}

function _agJarDragOver(e){e.preventDefault();document.getElementById('ag-jar-dropzone')?.classList.add('dz-active');}
function _agJarDragLeave(){document.getElementById('ag-jar-dropzone')?.classList.remove('dz-active');}
function _agJarDrop(e){e.preventDefault();_agJarDragLeave();const f=e.dataTransfer?.files?.[0];if(f)_agJarSetFile(f);}
function _agJarFileSelected(e){const f=e.target?.files?.[0];if(f)_agJarSetFile(f);}
function _agJarSetFile(file){
    if(!file.name.endsWith('.jar')){if(typeof toast==='function')toast('Only .jar files accepted','err');return;}
    _AG.jarFile=file;
    document.getElementById('ag-jar-fname').textContent=file.name;
    document.getElementById('ag-jar-fmeta').textContent=_agFmtBytes(file.size)+'  ·  '+new Date(file.lastModified).toLocaleString();
    document.getElementById('ag-jar-file-card').style.display='flex';
    document.getElementById('ag-jar-dz-title').textContent='JAR selected — ready to submit';
    document.getElementById('ag-jar-dz-sub').textContent=file.name;
}
function _agJarClearFile(){_AG.jarFile=null;document.getElementById('ag-jar-file-card').style.display='none';document.getElementById('ag-jar-dz-title').textContent='Drop Agent JAR here or click to browse';document.getElementById('ag-jar-dz-sub').textContent='Compiled Flink Agent fat JAR (.jar)';const inp=document.getElementById('ag-jar-file-input');if(inp)inp.value='';}
function _agJarReset(){_agJarClearFile();['ag-jar-class','ag-jar-par','ag-jar-args','ag-jar-sp'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});document.getElementById('ag-jar-status').textContent='';}
function _agJarSetStatus(msg,type){const el=document.getElementById('ag-jar-status');if(!el)return;el.style.color={ok:'var(--green)',err:'var(--red)',info:'#00c4a0'}[type]||'var(--text2)';el.textContent=msg;}
function _agJarSetProgress(label,pct){const wrap=document.getElementById('ag-jar-progress-wrap');if(!wrap)return;if(pct<0){wrap.style.display='none';return;}wrap.style.display='block';const bar=document.getElementById('ag-jar-prog-bar');if(bar)bar.style.width=Math.min(100,pct)+'%';const lbl=document.getElementById('ag-jar-prog-lbl');if(lbl)lbl.textContent=label;const pe=document.getElementById('ag-jar-prog-pct');if(pe)pe.textContent=Math.round(pct)+'%';}
function _agFmtBytes(b){b=Number(b)||0;if(b>=1073741824)return(b/1073741824).toFixed(2)+' GB';if(b>=1048576)return(b/1048576).toFixed(1)+' MB';if(b>=1024)return(b/1024).toFixed(0)+' KB';return b+' B';}

async function _agJarSubmit(){
    if(!_AG.jarFile){_agJarSetStatus('Select a JAR file first.','err');return;}
    if(typeof _jarXhrBase!=='function'){_agJarSetStatus('Connect to a Flink cluster first.','err');return;}
    const ec=document.getElementById('ag-jar-class')?.value?.trim()||'';
    const ar=document.getElementById('ag-jar-args')?.value?.trim()||'';
    const pr=parseInt(document.getElementById('ag-jar-par')?.value||'0',10)||null;
    const sp=document.getElementById('ag-jar-sp')?.value?.trim()||'';
    const nr=document.getElementById('ag-jar-allow-nr')?.checked||false;
    _agJarSetStatus('Uploading agent JAR…','info');_agJarSetProgress('Uploading…',5);
    try{
        const jarId=await new Promise((resolve,reject)=>{
            const xhr=new XMLHttpRequest();xhr.open('POST',_jarXhrBase()+'/jars/upload');
            if(typeof state!=='undefined'&&state.gateway?.headers){Object.entries(state.gateway.headers).forEach(([k,v])=>{if(k.toLowerCase()!=='content-type')xhr.setRequestHeader(k,v);});}
            xhr.upload.onprogress=e=>{if(e.lengthComputable)_agJarSetProgress('Uploading…',(e.loaded/e.total)*70);};
            xhr.onload=()=>{if(xhr.status>=200&&xhr.status<300){try{const r=JSON.parse(xhr.responseText);resolve(r.filename||r.jarId||r.id||'');}catch(e){reject(new Error('Parse error'));}}else reject(new Error('HTTP '+xhr.status));};
            xhr.onerror=()=>reject(new Error('Network error'));
            const fd=new FormData();fd.append('jarfile',_AG.jarFile,_AG.jarFile.name);xhr.send(fd);
        });
        _agJarSetProgress('Submitting…',80);
        const basename=jarId.split('/').pop();
        const payload={};
        if(ec)payload.entryClass=ec;if(ar)payload.programArgsList=ar.split(/\s+/);if(pr)payload.parallelism=pr;if(sp)payload.savepointPath=sp;if(nr)payload.allowNonRestoredState=true;
        const resp=await fetch(_jarXhrBase()+'/jars/'+encodeURIComponent(basename)+'/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
        if(!resp.ok)throw new Error('HTTP '+resp.status);
        const data=await resp.json();
        const jobId=data.jobid||data.id||'';
        _agJarSetProgress('Done!',100);
        _agJarSetStatus('Agent submitted! Job ID: '+(jobId.slice(0,8)||'?')+'…','ok');
        if(typeof _jarAddHistory==='function')_jarAddHistory({file:_AG.jarFile.name,size:_AG.jarFile.size,jarId:basename,jobId,entryClass:ec,args:ar,parallelism:pr});
        _agJarRenderHistory();
        if(typeof toast==='function')toast('Agent job submitted','ok');
        setTimeout(()=>_agJarSetProgress('',-1),3000);
        _agJarRefreshCluster();
    }catch(err){_agJarSetProgress('',-1);_agJarSetStatus(''+err.message,'err');if(typeof toast==='function')toast('Submit failed: '+err.message,'err');}
}
async function _agJarRefreshCluster(){
    const el=document.getElementById('ag-jar-cluster-list');if(!el)return;
    if(typeof jmApi!=='function'){el.innerHTML='<span style="font-size:10px;color:var(--text3);">Not connected</span>';return;}
    try{
        const data=await jmApi('/jars');
        const jars=Array.isArray(data)?data:(data?.files||data?.jars||[]);
        if(!jars.length){el.innerHTML='<span style="font-size:10px;color:var(--text3);">No JARs on cluster.</span>';return;}
        el.innerHTML=jars.slice(0,10).map(j=>{
            const name=j.name||(j.filename||j.id||'').split('/').pop()||'';
            const sid=encodeURIComponent((j.filename||j.id||'').split('/').pop()||j.id||'');
            return`<div style="border:1px solid var(--border);border-radius:4px;padding:6px 8px;background:var(--bg1);display:flex;align-items:center;gap:5px;margin-bottom:3px;"><div style="flex:1;min-width:0;overflow:hidden;"><div style="font-size:10px;font-weight:600;color:var(--text0);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${_agEsc(name)}</div><div style="font-size:9px;color:var(--text3);">${_agFmtBytes(j.size||0)}</div></div><button onclick="_agJarRunExisting('${sid}')" style="font-size:9px;padding:2px 6px;border-radius:2px;cursor:pointer;background:rgba(0,176,143,0.12);border:1px solid rgba(0,176,143,0.3);color:#00c4a0;flex-shrink:0;">▶</button></div>`;
        }).join('');
    }catch(e){el.innerHTML=`<span style="font-size:10px;color:var(--red);">✗ ${_agEsc(e.message)}</span>`;}
}
async function _agJarRunExisting(encodedId){
    if(typeof _jarXhrBase!=='function'){if(typeof toast==='function')toast('Connect to cluster first','warn');return;}
    const ec=document.getElementById('ag-jar-class')?.value?.trim()||'';
    const pr=parseInt(document.getElementById('ag-jar-par')?.value||'0',10)||null;
    const payload={};if(ec)payload.entryClass=ec;if(pr)payload.parallelism=pr;
    try{
        const resp=await fetch(_jarXhrBase()+'/jars/'+encodedId+'/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
        if(!resp.ok)throw new Error('HTTP '+resp.status);
        const data=await resp.json();
        _agJarSetStatus('Submitted! ID: '+((data.jobid||data.id||'').slice(0,8)||'?')+'…','ok');
        if(typeof toast==='function')toast('Agent job submitted','ok');
    }catch(e){_agJarSetStatus(''+e.message,'err');}
}
function _agJarRenderHistory(){
    const el=document.getElementById('ag-jar-history');if(!el)return;
    let hist=[];
    if(typeof _jarLoadHistory==='function')hist=_jarLoadHistory().slice(0,5);
    if(!hist.length){el.innerHTML='<span style="font-size:10px;color:var(--text3);">No submissions yet.</span>';return;}
    el.innerHTML=hist.map(h=>`<div style="border:1px solid var(--border);border-radius:4px;padding:6px 8px;background:var(--bg1);margin-bottom:3px;"><div style="font-size:10px;font-weight:600;color:var(--text0);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${_agEsc(h.file||'')}</div><div style="font-size:9px;color:var(--text3);font-family:var(--mono);">job: <span style="color:#00c4a0;">${(h.jobId||'').slice(0,8)||'?'}…</span></div><div style="font-size:9px;color:var(--text3);">${new Date(h.ts||'').toLocaleString()}</div></div>`).join('');
}

/* ══════════════════════════════════════════════════════════════════════════
   OBSERVABILITY — Real Kafka only via SQL Gateway (no simulation)
   ══════════════════════════════════════════════════════════════════════════ */
const _AGX_EVENT_COLORS={LLM_CALL:'#00c4a0',TOOL_INVOKE:'#4fa3e0',MEMORY_READ:'#f5a623',MEMORY_WRITE:'#d4960a',DECISION:'#00d4aa',AGENT_START:'#63c996',AGENT_END:'#63c996',ERROR:'#ff4d6d',CHECKPOINT:'#7a9ab0'};

function _agRenderObservability(){
    const content=document.getElementById('ag-content');
    const logNode=(_AG.canvas.nodes||[]).find(n=>n.opId==='event_log');
    const defaultTopic=logNode?.params?.topic||_AG.wizData?.obs_topic||'flink-agent-events';
    const defaultBootstrap=_AG.wizData?.source_endpoint||'kafka:9092';
    content.innerHTML=`
<div style="display:flex;flex-direction:column;flex:1;overflow:hidden;">
  <div style="display:flex;align-items:center;gap:6px;padding:7px 12px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;flex-wrap:wrap;">
    <span class="agx-kafka-dot" id="agx-kafka-dot"></span>
    <span style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:0.8px;text-transform:uppercase;">Live Kafka Event Log</span>
    <input id="agx-kafka-bootstrap" class="field-input" type="text" value="${_agEsc(defaultBootstrap)}" placeholder="kafka:9092" style="font-size:10px;font-family:var(--mono);width:180px;"/>
    <input id="agx-kafka-topic" class="field-input" type="text" value="${_agEsc(defaultTopic)}" placeholder="flink-agent-events" style="font-size:10px;font-family:var(--mono);width:200px;"/>
    <select id="agx-kafka-offset" class="field-input" style="font-size:10px;width:140px;">
      <option value="latest-offset">Latest offset</option>
      <option value="earliest-offset">Earliest offset</option>
    </select>
    <button class="ag-tb-btn green" id="agx-kafka-start-btn" onclick="_agObsConnect()">▶ Connect</button>
    <button class="ag-tb-btn red"   id="agx-kafka-stop-btn"  onclick="_agObsStop()" style="display:none;">⏹ Disconnect</button>
    <button class="ag-tb-btn" onclick="_agObsClear()">✕ Clear</button>
    <select class="field-input" id="agx-obs-filter" style="font-size:10px;width:150px;" onchange="_agObsRender()">
      <option value="">All event types</option>
      ${Object.keys(_AGX_EVENT_COLORS).map(t=>`<option value="${t}">${t}</option>`).join('')}
    </select>
    <label style="display:flex;align-items:center;gap:4px;font-size:10px;color:var(--text2);cursor:pointer;margin-left:auto;">
      <input type="checkbox" id="agx-obs-autoscroll" checked/> Auto-scroll
    </label>
    <button onclick="_agObsExportCsv()" class="ag-tb-btn" style="font-size:9px;">⬇ Export CSV</button>
  </div>
  <div id="agx-kafka-status-bar" style="display:none;padding:4px 12px;background:rgba(0,176,143,0.05);border-bottom:1px solid rgba(0,176,143,0.15);font-size:10px;font-family:var(--mono);color:var(--text3);flex-shrink:0;flex-direction:row;align-items:center;gap:10px;">
    <span id="agx-kafka-status-txt"></span>
    <span id="agx-kafka-rate" style="margin-left:auto;color:var(--green);"></span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(6,1fr);border-bottom:1px solid var(--border);flex-shrink:0;background:var(--bg1);">
    ${['Total Events','LLM Calls','Tool Invokes','Decisions','Avg Latency ms','Errors'].map((l,i)=>`
      <div style="padding:8px 10px;border-right:1px solid var(--border);text-align:center;${i===5?'border-right:none':''}">
        <div style="font-size:16px;font-weight:700;color:#00c4a0;font-family:var(--mono);" id="agx-met-${i}">0</div>
        <div style="font-size:9px;color:var(--text3);margin-top:2px;">${l}</div>
      </div>`).join('')}
  </div>
  <canvas id="agx-obs-spark" height="36" style="width:100%;height:36px;display:block;background:var(--bg1);border-bottom:1px solid var(--border);flex-shrink:0;"></canvas>
  <div style="flex:1;overflow-y:auto;background:var(--bg0);" id="agx-obs-stream">
    <div style="padding:40px;text-align:center;color:var(--text3);font-size:12px;">
      <div style="opacity:0.12;display:flex;justify-content:center;margin-bottom:12px;">${_agSvgIcon('<path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.68 11.9a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.56 1h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L7.91 8.54a16 16 0 0 0 6.29 6.29l.91-.91a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/>',44)}</div>
      <div style="margin-bottom:8px;">Configure Kafka bootstrap and topic above, then click <strong style="color:#00c4a0;">▶ Connect</strong>.</div>
      <div style="font-size:10px;line-height:1.8;color:var(--text3);">
        The Studio creates a TEMPORARY TABLE over your agent event log topic<br>
        and streams events via the Flink SQL Gateway session.<br>
        <strong style="color:var(--text2);">No data is simulated</strong> — all events come from your real Kafka topic.
      </div>
    </div>
  </div>
  <div style="padding:6px 12px;background:var(--bg2);border-top:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-shrink:0;font-size:10px;font-family:var(--mono);color:var(--text3);">
    <span>Events: <strong id="agx-footer-count" style="color:#00c4a0;">0</strong></span>
    <span>·</span>
    <span>Session: <strong id="agx-footer-session" style="color:var(--accent);">${_agEsc(window.state?.activeSession||'—')}</strong></span>
    <span>·</span>
    <span id="agx-footer-mode">disconnected</span>
  </div>
</div>`;
}

async function _agObsConnect(){
    const bootstrap=document.getElementById('agx-kafka-bootstrap')?.value?.trim()||'kafka:9092';
    const topic    =document.getElementById('agx-kafka-topic')?.value?.trim()||'flink-agent-events';
    const offset   =document.getElementById('agx-kafka-offset')?.value||'latest-offset';
    const session  =window.state?.activeSession;
    if(!session){_agObsSetDot('error');_agObsSetStatus('No active Flink session — connect to a cluster first.');return;}
    if(typeof jmApi!=='function'){_agObsSetDot('error');_agObsSetStatus('jmApi not available — ensure connection.js is loaded.');return;}
    _agObsSetDot('connecting');_agObsSetStatus(`Creating event log table for topic: ${topic}…`);
    const tableName=`_agx_evlog_${Date.now()}`;
    const createDDL=`CREATE TEMPORARY TABLE \`${tableName}\` (log_id BIGINT,event_type STRING,agent_name STRING,event_payload STRING,latency_ms BIGINT,log_time TIMESTAMP(3),WATERMARK FOR log_time AS log_time - INTERVAL '5' SECOND) WITH ('connector'='kafka','topic'='${topic}','properties.bootstrap.servers'='${bootstrap}','properties.group.id'='strlab-agx-${Date.now()}','scan.startup.mode'='${offset}','format'='json','json.ignore-parse-errors'='true')`;
    try{await _agObsGwExec(session,createDDL);}catch(err){_agObsSetDot('error');_agObsSetStatus('Failed to create event log table: '+err.message);return;}
    const selectSql=`SELECT log_id,event_type,agent_name,event_payload,latency_ms,log_time FROM \`${tableName}\``;
    let stmtHandle;
    try{stmtHandle=await _agObsGwSubmit(session,selectSql);}catch(err){_agObsSetDot('error');_agObsSetStatus('SQL Gateway SELECT failed: '+err.message+'. Ensure the session is active and Kafka is reachable.');return;}
    _AG.obs.running=true;_AG.obs.total=0;_AG.obs.errors=0;
    document.getElementById('agx-kafka-start-btn').style.display='none';
    document.getElementById('agx-kafka-stop-btn').style.display='';
    _agObsSetDot('connected');_agObsSetStatus(`Connected · topic: ${topic} · stmt: ${stmtHandle?.slice(0,8)||'?'}…`);
    document.getElementById('agx-footer-mode').textContent='live kafka';
    let lastT=Date.now(),evN=0;
    _AG.obs.pollInterval=setInterval(async()=>{
        if(!_AG.obs.running)return;
        try{
            const rows=await _agObsFetchPage(session,stmtHandle);
            rows.forEach(r=>{_agObsIngest(r);evN++;});
            const elapsed=(Date.now()-lastT)/1000;
            if(elapsed>2){const rate=(evN/elapsed).toFixed(1);const re=document.getElementById('agx-kafka-rate');if(re)re.textContent=rate+' events/s';evN=0;lastT=Date.now();}
            _agObsRender();_agObsUpdateMetrics();_agObsDrawSparkline();
        }catch(_){}
    },1500);
}

function _agObsStop(silent){
    _AG.obs.running=false;
    if(_AG.obs.pollInterval){clearInterval(_AG.obs.pollInterval);_AG.obs.pollInterval=null;}
    _agObsSetDot('');
    if(!silent)_agObsSetStatus('Disconnected.');
    const sb=document.getElementById('agx-kafka-start-btn'),eb=document.getElementById('agx-kafka-stop-btn');
    if(sb)sb.style.display='';if(eb)eb.style.display='none';
    const m=document.getElementById('agx-footer-mode');if(m)m.textContent='disconnected';
}
async function _agObsGwExec(session,sql){if(typeof jmApi!=='function')return;await jmApi(`/sessions/${session}/statements`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({statement:sql})});}
async function _agObsGwSubmit(session,sql){if(typeof jmApi!=='function')throw new Error('jmApi not available');const resp=await jmApi(`/sessions/${session}/statements`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({statement:sql})});const handle=resp?.operationHandle?.identifier?.guid||resp?.handle||null;if(!handle)throw new Error('No statement handle returned');return handle;}
async function _agObsFetchPage(session,handle){if(typeof jmApi!=='function')return[];try{const resp=await jmApi(`/sessions/${session}/operations/${handle}/result/0`);const data=resp?.results?.data||[];return data.map(r=>{const f=r.fields||r;return{log_id:f[0]||0,event_type:f[1]||'UNKNOWN',agent_name:f[2]||'agent',event_payload:f[3]||'{}',latency_ms:f[4]||0,log_time:f[5]||new Date().toISOString()};});}catch(_){return[];}}

function _agObsIngest(row){
    _AG.obs.total++;
    if(row.event_type==='ERROR')_AG.obs.errors++;
    let payload={};try{payload=JSON.parse(row.event_payload||'{}');}catch(_){}
    _AG.obs.buffer.unshift({ts:new Date(row.log_time||Date.now()).toLocaleTimeString('en-GB',{hour12:false,fractionalSecondDigits:3}),type:row.event_type||'UNKNOWN',agent:row.agent_name||'agent',msg:payload.message||payload.decision||payload.tool||String(row.event_payload||'').slice(0,80)||'—',latency:row.latency_ms||0});
    if(_AG.obs.buffer.length>500)_AG.obs.buffer.pop();
    const fc=document.getElementById('agx-footer-count');if(fc)fc.textContent=_AG.obs.total;
}
function _agObsRender(){
    const stream=document.getElementById('agx-obs-stream');if(!stream)return;
    const filter=document.getElementById('agx-obs-filter')?.value||'';
    const events=filter?_AG.obs.buffer.filter(e=>e.type===filter):_AG.obs.buffer;
    if(!events.length)return;
    stream.innerHTML=events.slice(0,150).map(e=>`
      <div class="agx-event-row">
        <span class="agx-ev-ts">${e.ts}</span>
        <span class="agx-ev-type" style="color:${_AGX_EVENT_COLORS[e.type]||'#00c4a0'};">${_agEsc(e.type)}</span>
        <span style="color:var(--text3);font-size:9px;flex-shrink:0;">${_agEsc(e.agent)}</span>
        <span class="agx-ev-msg">${_agEsc(e.msg)}</span>
        <span class="agx-ev-lat">${e.latency}ms</span>
      </div>`).join('');
    if(document.getElementById('agx-obs-autoscroll')?.checked)stream.scrollTop=0;
}
function _agObsUpdateMetrics(){
    const buf=_AG.obs.buffer;
    const llm=buf.filter(e=>e.type==='LLM_CALL').length,tool=buf.filter(e=>e.type==='TOOL_INVOKE').length,dec=buf.filter(e=>e.type==='DECISION').length;
    const lats=buf.filter(e=>e.latency>0).map(e=>e.latency),avg=lats.length?Math.round(lats.reduce((a,b)=>a+b,0)/lats.length):0;
    [_AG.obs.total,llm,tool,dec,avg,_AG.obs.errors].forEach((v,i)=>{const el=document.getElementById(`agx-met-${i}`);if(el)el.textContent=v;});
}
function _agObsDrawSparkline(){
    const canvas=document.getElementById('agx-obs-spark');if(!canvas)return;
    const ctx=canvas.getContext('2d');if(!ctx)return;
    canvas.width=canvas.offsetWidth||800;ctx.clearRect(0,0,canvas.width,36);
    const buf=_AG.obs.buffer.slice(0,60).reverse(),vals=buf.map(e=>e.latency||0);
    if(vals.length<2)return;
    const maxV=Math.max(...vals,1),W=canvas.width,H=36,step=W/(vals.length-1);
    ctx.beginPath();ctx.strokeStyle='#00c4a0';ctx.lineWidth=1.5;
    vals.forEach((v,i)=>{const x=i*step,y=H-(v/maxV)*(H-4)-2;i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);});
    ctx.stroke();
    ctx.lineTo(W,H);ctx.lineTo(0,H);ctx.closePath();ctx.fillStyle='rgba(0,176,143,0.07)';ctx.fill();
}
function _agObsClear(){_AG.obs.buffer=[];_AG.obs.total=0;_AG.obs.errors=0;_agObsRender();_agObsUpdateMetrics();_agObsDrawSparkline();[0,1,2,3,4,5].forEach(i=>{const e=document.getElementById(`agx-met-${i}`);if(e)e.textContent='0';});const fc=document.getElementById('agx-footer-count');if(fc)fc.textContent='0';}
function _agObsExportCsv(){
    const rows=_AG.obs.buffer;if(!rows.length){if(typeof toast==='function')toast('No events to export','warn');return;}
    const header='timestamp,event_type,agent,message,latency_ms';
    const lines=rows.map(r=>`"${r.ts}","${r.type}","${r.agent}","${r.msg.replace(/"/g,'""')}","${r.latency}"`);
    const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([[header,...lines].join('\n')],{type:'text/csv'}));a.download=`agent-event-log-${Date.now()}.csv`;a.click();
    if(typeof toast==='function')toast('Event log exported','ok');
}
function _agObsSetDot(state){const dot=document.getElementById('agx-kafka-dot');if(!dot)return;dot.className='agx-kafka-dot'+(state==='connected'?' connected':state==='error'?' error':'');}
function _agObsSetStatus(msg){const el=document.getElementById('agx-kafka-status-txt');if(el)el.textContent=msg;const bar=document.getElementById('agx-kafka-status-bar');if(bar)bar.style.display='flex';}

/* ══════════════════════════════════════════════════════════════════════════
   EXPORT / IMPORT / RESET / HISTORY / KEYBOARD
   ══════════════════════════════════════════════════════════════════════════ */
function _agExportAgent(){
    const payload={_version:2,_tool:'Str:::lab Studio — Agent Manager',_exported:new Date().toISOString(),wizData:_AG.wizData,canvas:{nodes:_AG.canvas.nodes,edges:_AG.canvas.edges}};
    const name=(_AG.wizData.agent_name||'flink-agent').replace(/\s+/g,'_');
    const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}));a.download=`agent_${name}_${Date.now()}.json`;a.click();
    if(typeof toast==='function')toast('Agent exported as JSON','ok');
}
function _agImportAgent(input){
    const file=input?.files?.[0];if(!file)return;
    const r=new FileReader();
    r.onload=e=>{
        try{const data=JSON.parse(e.target.result);if(!data._version)throw new Error('Invalid agent file');if(data.wizData)_AG.wizData=data.wizData;if(data.canvas){_AG.canvas.nodes=data.canvas.nodes||[];_AG.canvas.edges=data.canvas.edges||[];}input.value='';_agSwitchTab('canvas');if(typeof toast==='function')toast('Agent imported','ok');}
        catch(err){if(typeof toast==='function')toast('Import failed: '+err.message,'err');}
    };
    r.readAsText(file);
}
function _agResetAll(){
    if(!confirm('Reset all Agent Manager settings?'))return;
    _AG.wizData={};_AG.wizStep=0;
    _AG.canvas={nodes:[],edges:[],pan:{x:0,y:0},scale:1.0};
    _AG.obs={running:false,pollInterval:null,buffer:[],total:0,errors:0};
    if(_AG.animTimer){cancelAnimationFrame(_AG.animTimer);_AG.animTimer=null;}
    _agSwitchTab('welcome');
    if(typeof toast==='function')toast('Agent Manager reset','ok');
}
function _agUpdateHistCount(){const el=document.getElementById('ag-hist-count');if(el)el.textContent=_AG.history.length?`(${_AG.history.length})`:''; }
function _agShowHistory(){
    if(!_AG.history.length){if(typeof toast==='function')toast('No agent history yet','info');return;}
    const old=document.getElementById('ag-hist-popup');if(old){old.remove();return;}
    const m=document.createElement('div');m.id='ag-hist-popup';
    m.style.cssText='position:fixed;z-index:10004;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--bg2);border:1px solid rgba(0,176,143,0.3);border-radius:8px;box-shadow:0 12px 48px rgba(0,0,0,0.7);width:520px;max-height:70vh;display:flex;flex-direction:column;overflow:hidden;';
    m.innerHTML=`<div style="padding:11px 14px;background:rgba(0,176,143,0.06);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;flex-shrink:0;"><span style="font-size:13px;font-weight:700;color:var(--text0);">Agent Build History</span><span style="font-size:10px;color:var(--text3);">${_AG.history.length} entries</span><button onclick="document.getElementById('ag-hist-popup').remove()" style="margin-left:auto;background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;">×</button></div><div style="flex:1;overflow-y:auto;padding:10px;">${_AG.history.map(h=>`<div class="ag-hist-item"><div style="flex:1;min-width:0;"><div style="font-size:11px;font-weight:700;color:var(--text0);font-family:var(--mono);">${_agEsc(h.agent_name||'(unnamed)')}</div><div style="font-size:10px;color:var(--text3);">${h.nodes||0} nodes · ${new Date(h.ts).toLocaleString()}</div></div></div>`).join('')}</div><div style="padding:10px 14px;border-top:1px solid var(--border);display:flex;justify-content:flex-end;"><button onclick="document.getElementById('ag-hist-popup').remove()" style="padding:5px 14px;font-size:11px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Close</button></div>`;
    document.body.appendChild(m);
}

/* ── Keyboard shortcuts ───────────────────────────────────────────────────── */
window.addEventListener('keydown',e=>{
    const modal=document.getElementById('ag-modal');
    if(!modal||!modal.classList.contains('open'))return;
    if((e.key==='Delete'||e.key==='Backspace')&&_AG.selectedNode&&document.activeElement?.tagName!=='INPUT'&&document.activeElement?.tagName!=='TEXTAREA'){_agDeleteNode(_AG.selectedNode);}
    if(e.key==='Escape'&&_AG.connecting)_agCancelConnect();
});

/* ══════════════════════════════════════════════════════════════════════════
   WIZARD STEP 1 — EVENT SOURCE (with Kafka Schema Registry schema loading)
   Mirrors IFM's table selector / schema auto-fill pattern exactly.
   ══════════════════════════════════════════════════════════════════════════ */
function _agWizRenderSourceStep() {
    const d = _AG.wizData;
    const fi = (label, id, ph, val, req) =>
        `<div style="margin-bottom:10px;"><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">${label}${req ? '<span style="color:var(--red)"> *</span>' : ''}</label>
        <input id="ag-wf-${id}" class="field-input" type="text" value="${_agEsc(val || d[id] || '')}" placeholder="${_agEsc(ph || '')}" style="font-size:11px;font-family:var(--mono);"/></div>`;
    const sel = (label, id, opts, val) =>
        `<div style="margin-bottom:10px;"><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">${label}</label>
        <select id="ag-wf-${id}" class="field-input" style="font-size:11px;">${opts.map(o => `<option value="${o}" ${(val || d[id] || opts[0]) === o ? 'selected' : ''}>${o}</option>`).join('')}</select></div>`;

    return `
<div class="ag-info">Configure the streaming Kafka event source. If using Avro/Confluent format, connect to Schema Registry to load the schema automatically.</div>

<!-- Source type selector -->
<div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;">
  ${['Kafka (JSON)','Kafka + Schema Registry','Datagen (testing)','JDBC','Kinesis'].map(t =>
        `<div onclick="_agWizSelectSourceType('${t}')"
      style="padding:6px 13px;border-radius:4px;cursor:pointer;font-size:11px;font-family:var(--mono);
        border:1.5px solid ${(d.source_type||'Kafka (JSON)')===t?'#00c4a0':'var(--border2)'};
        background:${(d.source_type||'Kafka (JSON)')===t?'rgba(0,176,143,0.1)':'var(--bg3)'};
        color:${(d.source_type||'Kafka (JSON)')===t?'#00c4a0':'var(--text2)'};
        font-weight:${(d.source_type||'Kafka (JSON)')===t?'700':'400'};
        transition:all 0.12s;user-select:none;"
      id="ag-src-type-${t.replace(/[^a-z0-9]/gi,'_')}">${t}</div>`).join('')}
</div>

<!-- Kafka connection -->
<div class="ag-card" id="ag-src-kafka-card" style="${(d.source_type||'Kafka (JSON)').includes('Datagen')||(d.source_type||'').includes('JDBC')||(d.source_type||'').includes('Kinesis')?'display:none;':''}">
  <div class="ag-section">Kafka Connection</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
    ${fi('Source Table Name','source_table','agent_events','',true)}
    ${fi('Bootstrap Servers','source_endpoint','kafka:9092','')}
    ${fi('Topic','source_topic','agent-input-events','')}
    ${sel('Event Format','source_format',['json','avro','avro-confluent','protobuf','raw'],d.source_format||'json')}
    ${fi('Watermark Column','source_wm_col','event_time','')}
    ${fi('Watermark Delay (s)','source_wm_delay','5','')}
  </div>
</div>

<!-- Schema Registry card (shown when Kafka + SR selected) -->
<div class="ag-card" id="ag-src-sr-card" style="${(d.source_type||'Kafka (JSON)')!=='Kafka + Schema Registry'?'display:none;':''}">
  <div class="ag-section">Schema Registry</div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px;">
    <div style="grid-column:1/3;">
      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Schema Registry URL <span style="color:var(--red)">*</span></label>
      <input id="ag-wf-sr_url" class="field-input" type="text"
        value="${_agEsc(d.sr_url||'')}" placeholder="http://schema-registry:8081"
        style="font-size:11px;font-family:var(--mono);"/>
    </div>
    <div style="display:flex;flex-direction:column;justify-content:flex-end;">
      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Subject Strategy</label>
      <select id="ag-wf-sr_strategy" class="field-input" style="font-size:11px;">
        <option value="TopicNameStrategy" ${(d.sr_strategy||'TopicNameStrategy')==='TopicNameStrategy'?'selected':''}>TopicNameStrategy</option>
        <option value="RecordNameStrategy" ${d.sr_strategy==='RecordNameStrategy'?'selected':''}>RecordNameStrategy</option>
        <option value="TopicRecordNameStrategy" ${d.sr_strategy==='TopicRecordNameStrategy'?'selected':''}>TopicRecordNameStrategy</option>
      </select>
    </div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;">
    <div>
      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">SR Username / API Key (optional)</label>
      <input id="ag-wf-sr_user" class="field-input" type="text" value="${_agEsc(d.sr_user||'')}" placeholder="api-key" style="font-size:11px;font-family:var(--mono);"/>
    </div>
    <div>
      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">SR Password / Secret (optional)</label>
      <input id="ag-wf-sr_pass" class="field-input" type="password" value="${_agEsc(d.sr_pass||'')}" placeholder="api-secret" style="font-size:11px;font-family:var(--mono);"/>
    </div>
  </div>
  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
    <div style="flex:1;min-width:160px;">
      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Schema Subject / Name</label>
      <input id="ag-wf-sr_subject" class="field-input" type="text" value="${_agEsc(d.sr_subject||d.source_topic||'')}"
        placeholder="agent-input-events-value" style="font-size:11px;font-family:var(--mono);"/>
    </div>
    <div style="display:flex;flex-direction:column;justify-content:flex-end;gap:4px;">
      <label style="font-size:10px;color:transparent;display:block;">btn</label>
      <div style="display:flex;gap:6px;">
        <button onclick="_agSrFetchSubjects()" class="ag-tb-btn" style="border-color:rgba(0,176,143,0.4);color:#00c4a0;">
          ⟳ Load Subjects
        </button>
        <button onclick="_agSrFetchSchema()" class="ag-tb-btn purple" style="background:rgba(0,176,143,0.12);border-color:rgba(0,176,143,0.4);color:#00c4a0;">
          ↓ Load Schema
        </button>
      </div>
    </div>
  </div>
  <!-- Subject dropdown (populated after fetch) -->
  <div id="ag-sr-subjects-wrap" style="display:none;margin-top:8px;">
    <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Available Subjects</label>
    <select id="ag-sr-subjects-select" class="field-input" style="font-size:11px;"
      onchange="document.getElementById('ag-wf-sr_subject').value=this.value">
    </select>
  </div>
  <!-- Schema load status -->
  <div id="ag-sr-status" style="margin-top:8px;font-size:10px;font-family:var(--mono);color:var(--text3);"></div>
</div>

<!-- Datagen card -->
<div class="ag-card" id="ag-src-datagen-card" style="${(d.source_type||'Kafka (JSON)')!=='Datagen (testing)'?'display:none;':''}">
  <div class="ag-section">Datagen Configuration</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
    ${fi('Source Table Name','source_table','mock_agent_events','',true)}
    ${fi('Events Per Second','datagen_eps','50','')}
  </div>
</div>

<!-- JDBC card -->
<div class="ag-card" id="ag-src-jdbc-card" style="${(d.source_type||'Kafka (JSON)')!=='JDBC'?'display:none;':''}">
  <div class="ag-section">JDBC Source</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
    ${fi('Source Table Name','source_table','pg_events','',true)}
    ${fi('JDBC URL','source_endpoint','jdbc:postgresql://localhost/db','',true)}
    ${fi('DB Table','source_topic','public.events','')}
    ${fi('Username','jdbc_user','flink_user','')}
  </div>
</div>

<!-- Kinesis card -->
<div class="ag-card" id="ag-src-kinesis-card" style="${(d.source_type||'Kafka (JSON)')!=='Kinesis'?'display:none;':''}">
  <div class="ag-section">Kinesis Source</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
    ${fi('Source Table Name','source_table','kinesis_events','',true)}
    ${fi('Stream Name','source_topic','my-event-stream','',true)}
    ${fi('AWS Region','source_endpoint','us-east-1','')}
  </div>
</div>

<!-- Schema preview (populated from SR or manual) -->
<div class="ag-card">
  <div class="ag-section" style="display:flex;align-items:center;justify-content:space-between;">
    <span>Event Schema</span>
    <span id="ag-src-schema-badge" style="font-size:9px;color:var(--text3);font-family:var(--mono);"></span>
  </div>
  <div id="ag-src-schema-tags" style="display:flex;flex-wrap:wrap;gap:3px;margin-bottom:8px;min-height:28px;padding:6px;background:var(--bg0);border:1px solid var(--border);border-radius:3px;${(d.source_schema||'').trim()?'':''}">
    ${(d.source_schema||'').split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{
        const parts=l.split(/\s+/),name=parts[0],type=parts.slice(1).join(' ');
        return `<span style="padding:2px 8px;border-radius:3px;font-size:10px;font-family:var(--mono);background:rgba(0,176,143,0.08);border:1px solid rgba(0,176,143,0.25);color:#00c4a0;">${_agEsc(name)}<span style="font-size:8px;opacity:0.6;margin-left:3px;">${_agEsc(type)}</span></span>`;
    }).join('')}
  </div>
  <textarea id="ag-wf-source_schema" class="field-input" rows="5"
    style="font-size:11px;font-family:var(--mono);resize:vertical;"
    placeholder="event_id BIGINT&#10;payload STRING&#10;event_time TIMESTAMP(3)"
    oninput="_agSrcSchemaChanged(this.value)">${_agEsc(d.source_schema||'event_id BIGINT\npayload STRING\nevent_time TIMESTAMP(3)')}</textarea>
</div>`;
}

/* Source type selection */
function _agWizSelectSourceType(type) {
    const d = _AG.wizData;
    d.source_type = type;
    // Update button styles
    ['Kafka (JSON)','Kafka + Schema Registry','Datagen (testing)','JDBC','Kinesis'].forEach(t => {
        const btn = document.getElementById('ag-src-type-' + t.replace(/[^a-z0-9]/gi,'_'));
        if (!btn) return;
        const active = t === type;
        btn.style.borderColor    = active ? '#00c4a0' : 'var(--border2)';
        btn.style.background     = active ? 'rgba(0,176,143,0.1)' : 'var(--bg3)';
        btn.style.color          = active ? '#00c4a0' : 'var(--text2)';
        btn.style.fontWeight     = active ? '700' : '400';
    });
    // Show/hide cards
    const show = id => { const el = document.getElementById(id); if (el) el.style.display = ''; };
    const hide = id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; };
    const isKafka = type.startsWith('Kafka');
    isKafka ? show('ag-src-kafka-card') : hide('ag-src-kafka-card');
    type === 'Kafka + Schema Registry' ? show('ag-src-sr-card') : hide('ag-src-sr-card');
    type === 'Datagen (testing)' ? show('ag-src-datagen-card') : hide('ag-src-datagen-card');
    type === 'JDBC'    ? show('ag-src-jdbc-card')    : hide('ag-src-jdbc-card');
    type === 'Kinesis' ? show('ag-src-kinesis-card') : hide('ag-src-kinesis-card');
    // Auto-set format for SR
    const fmtEl = document.getElementById('ag-wf-source_format');
    if (fmtEl && type === 'Kafka + Schema Registry') fmtEl.value = 'avro-confluent';
}

/* Schema tag refresh when user edits textarea */
function _agSrcSchemaChanged(raw) {
    const tags = document.getElementById('ag-src-schema-tags'); if (!tags) return;
    const cols = raw.split('\n').map(l => l.trim()).filter(Boolean).map(l => {
        const p = l.split(/\s+/); return { name: p[0], type: p.slice(1).join(' ') };
    }).filter(c => c.name && c.type);
    const badge = document.getElementById('ag-src-schema-badge');
    if (badge) badge.textContent = cols.length ? `${cols.length} fields` : '';
    tags.innerHTML = cols.map(c =>
        `<span style="padding:2px 8px;border-radius:3px;font-size:10px;font-family:var(--mono);background:rgba(0,176,143,0.08);border:1px solid rgba(0,176,143,0.25);color:#00c4a0;">${_agEsc(c.name)}<span style="font-size:8px;opacity:0.6;margin-left:3px;">${_agEsc(c.type)}</span></span>`
    ).join('');
}

/* Fetch subjects list from Schema Registry */
async function _agSrFetchSubjects() {
    const srUrl  = document.getElementById('ag-wf-sr_url')?.value?.trim();
    const srUser = document.getElementById('ag-wf-sr_user')?.value?.trim();
    const srPass = document.getElementById('ag-wf-sr_pass')?.value?.trim();
    if (!srUrl) { if (typeof toast === 'function') toast('Enter Schema Registry URL first', 'warn'); return; }
    const status = document.getElementById('ag-sr-status');
    if (status) { status.textContent = '⟳ Fetching subjects…'; status.style.color = '#00c4a0'; }
    try {
        const headers = {};
        if (srUser && srPass) headers['Authorization'] = 'Basic ' + btoa(srUser + ':' + srPass);
        // Use the Studio's existing gateway proxy if available, else direct fetch
        let subjects = [];
        if (typeof jmApi === 'function') {
            // Try via SQL Gateway REST proxy (avoids CORS)
            const data = await jmApi(`/schema-registry/subjects`, { headers });
            subjects = Array.isArray(data) ? data : data?.subjects || [];
        } else {
            const resp = await fetch(srUrl.replace(/\/$/, '') + '/subjects', { headers });
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            subjects = await resp.json();
        }
        const wrap = document.getElementById('ag-sr-subjects-wrap');
        const sel  = document.getElementById('ag-sr-subjects-select');
        if (sel) {
            sel.innerHTML = subjects.map(s => `<option value="${_agEsc(s)}">${_agEsc(s)}</option>`).join('');
        }
        if (wrap) wrap.style.display = '';
        if (status) { status.textContent = `✓ ${subjects.length} subjects loaded`; status.style.color = 'var(--green)'; }
        if (typeof toast === 'function') toast(`${subjects.length} SR subjects loaded`, 'ok');
    } catch (err) {
        if (status) { status.textContent = '✗ ' + err.message; status.style.color = 'var(--red)'; }
        if (typeof toast === 'function') toast('SR subjects failed: ' + err.message, 'err');
    }
}

/* Fetch schema from Schema Registry and convert to Flink DDL */
async function _agSrFetchSchema() {
    const srUrl     = document.getElementById('ag-wf-sr_url')?.value?.trim();
    const srUser    = document.getElementById('ag-wf-sr_user')?.value?.trim();
    const srPass    = document.getElementById('ag-wf-sr_pass')?.value?.trim();
    const subject   = document.getElementById('ag-wf-sr_subject')?.value?.trim()
        || document.getElementById('ag-sr-subjects-select')?.value?.trim();
    if (!srUrl)    { if (typeof toast === 'function') toast('Enter Schema Registry URL', 'warn'); return; }
    if (!subject)  { if (typeof toast === 'function') toast('Enter a subject name or load subjects first', 'warn'); return; }
    const status = document.getElementById('ag-sr-status');
    if (status) { status.textContent = `⟳ Fetching schema for ${subject}…`; status.style.color = '#00c4a0'; }
    try {
        const headers = {};
        if (srUser && srPass) headers['Authorization'] = 'Basic ' + btoa(srUser + ':' + srPass);
        let schema = null;
        if (typeof jmApi === 'function') {
            const data = await jmApi(`/schema-registry/subjects/${encodeURIComponent(subject)}/versions/latest`, { headers });
            schema = JSON.parse(data?.schema || 'null');
        } else {
            const resp = await fetch(srUrl.replace(/\/$/, '') + '/subjects/' + encodeURIComponent(subject) + '/versions/latest', { headers });
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const data = await resp.json();
            schema = JSON.parse(data.schema);
        }
        if (!schema) throw new Error('Empty schema response');
        const ddlLines = _agAvroToDDL(schema);
        const ta = document.getElementById('ag-wf-source_schema');
        if (ta) { ta.value = ddlLines; _agSrcSchemaChanged(ddlLines); }
        _AG.wizData.source_schema = ddlLines;
        const badge = document.getElementById('ag-src-schema-badge');
        if (badge) badge.textContent = `loaded from SR · ${subject}`;
        if (status) { status.textContent = `✓ Schema loaded from ${subject} (${ddlLines.split('\n').length} fields)`; status.style.color = 'var(--green)'; }
        if (typeof toast === 'function') toast('Schema loaded from Schema Registry', 'ok');
    } catch (err) {
        if (status) { status.textContent = '✗ ' + err.message; status.style.color = 'var(--red)'; }
        if (typeof toast === 'function') toast('SR schema fetch failed: ' + err.message, 'err');
    }
}

/* Convert Avro schema fields → Flink SQL DDL type strings */
function _agAvroToDDL(schema) {
    const AVRO_TO_FLINK = {
        'string':  'STRING',  'int':      'INT',    'long':    'BIGINT',
        'float':   'FLOAT',   'double':   'DOUBLE', 'boolean': 'BOOLEAN',
        'bytes':   'BYTES',   'null':     'NULL',
    };
    const avroTypeToFlink = (avroType) => {
        if (typeof avroType === 'string') return AVRO_TO_FLINK[avroType] || avroType.toUpperCase();
        if (Array.isArray(avroType)) {
            // union — pick non-null
            const nonNull = avroType.filter(t => t !== 'null');
            return avroTypeToFlink(nonNull[0] || 'string');
        }
        if (typeof avroType === 'object') {
            if (avroType.type === 'record')    return 'ROW<...>';
            if (avroType.type === 'array')     return 'ARRAY<' + avroTypeToFlink(avroType.items) + '>';
            if (avroType.type === 'map')       return 'MAP<STRING,' + avroTypeToFlink(avroType.values) + '>';
            if (avroType.type === 'enum')      return 'STRING';
            if (avroType.logicalType === 'timestamp-millis' || avroType.logicalType === 'timestamp-micros') return 'TIMESTAMP(3)';
            if (avroType.logicalType === 'date') return 'DATE';
            if (avroType.logicalType === 'time-millis') return 'TIME(0)';
            if (avroType.logicalType === 'decimal') return `DECIMAL(${avroType.precision||18},${avroType.scale||6})`;
            return AVRO_TO_FLINK[avroType.type] || avroType.type?.toUpperCase() || 'STRING';
        }
        return 'STRING';
    };
    const fields = schema.fields || [];
    const lines = fields.map(f => `${f.name} ${avroTypeToFlink(f.type)}`);
    // Always add event_time if not present
    if (!lines.some(l => l.toLowerCase().includes('event_time') || l.toLowerCase().includes('timestamp'))) {
        lines.push('event_time TIMESTAMP(3)');
    }
    return lines.join('\n');
}

/* ══════════════════════════════════════════════════════════════════════════
   WIZARD STEP 8 — REVIEW & PIPELINE GRAPH
   Renders a compact SVG pipeline graph (like IFM's canvas) alongside the
   config summary. The visual canvas is kept separately in the Canvas tab.
   ══════════════════════════════════════════════════════════════════════════ */
const _AGWIZ = { zoom:1, panX:0, panY:0, panning:false, panSX:0, panSY:0, panOX:0, panOY:0 };

function _agWizRenderReviewStep() {
    const d = _AG.wizData;
    return `
<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;height:calc(90vh - 250px);min-height:360px;">

  <!-- LEFT: Pipeline graph canvas -->
  <div style="display:flex;flex-direction:column;overflow:hidden;background:var(--bg0);border:1px solid rgba(0,176,143,0.25);border-radius:5px;">
    <div style="display:flex;align-items:center;gap:5px;padding:5px 8px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;">
      <span style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.2px;text-transform:uppercase;flex:1;">Agent Pipeline Preview</span>
      <button onclick="_agWizCanvasZoom(-0.15)" style="font-size:13px;padding:1px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">−</button>
      <span id="ag-wiz-zoom-lbl" style="font-size:10px;color:var(--text3);font-family:var(--mono);min-width:36px;text-align:center;">100%</span>
      <button onclick="_agWizCanvasZoom(0.15)" style="font-size:13px;padding:1px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">+</button>
      <button onclick="_agWizCanvasFit()" style="font-size:10px;padding:2px 7px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text3);cursor:pointer;">⊙ Fit</button>
    </div>
    <div id="ag-wiz-canvas-wrap" style="flex:1;overflow:hidden;position:relative;cursor:grab;"
      onwheel="event.preventDefault();_agWizCanvasZoom(event.deltaY<0?0.12:-0.12)"
      onmousedown="_agWizPanStart(event)" onmousemove="_agWizPanMove(event)" onmouseup="_agWizPanEnd()">
      <svg id="ag-wiz-canvas-svg" style="transform-origin:0 0;will-change:transform;display:block;overflow:visible;"></svg>
    </div>
    <div style="padding:4px 10px;border-top:1px solid var(--border);background:var(--bg2);font-size:9px;color:var(--text3);font-family:var(--mono);">
      scroll=zoom · drag=pan · dbl-click=fit to view
    </div>
  </div>

  <!-- RIGHT: Config summary -->
  <div style="display:flex;flex-direction:column;overflow:hidden;">
    <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px;">Configuration Summary</div>
    <div style="flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:8px;">
      ${[
        ['Agent',     d.agent_name||'—',     d.agent_pattern||'WorkflowAgent'],
        ['Language',  d.agent_lang||'Python', `p=${d.agent_parallelism||4} · ckpt=${d.agent_checkpoint||10000}ms`],
        ['Source',    d.source_table||'—',   (d.source_type||'Kafka')+(d.source_endpoint?' @ '+d.source_endpoint:'')],
        ['LLM',       d.llm_model||'—',      d.llm_provider||'OpenAI'],
        ['Memory',    d.mem_lt_store!=='None'?'Short+Long-Term':'Short-Term only', 'TTL '+( d.mem_short_ttl||24)+'h'],
        ['Tools',     d.tool1_name||'—',     d.mcp_url?'+ MCP: '+(d.mcp_name||'server'):'no MCP'],
        ['Output',    d.sink_table||'—',     d.sink_type||'Kafka'],
        ['Event Log', d.obs_topic||'flink-agent-events', d.obs_sink||'Kafka'],
    ].map(([label, primary, secondary]) => `
        <div class="ag-card" style="padding:9px 12px;">
          <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.2px;text-transform:uppercase;margin-bottom:4px;">${label}</div>
          <div style="font-size:12px;font-weight:700;color:var(--text0);font-family:var(--mono);">${_agEsc(primary)}</div>
          <div style="font-size:10px;color:var(--text3);margin-top:2px;">${_agEsc(secondary)}</div>
        </div>`).join('')}
    </div>
    <div class="ag-info" style="margin-top:8px;flex-shrink:0;">Click <strong>⚛ Build Agent</strong> to generate all code artefacts and populate the Visual Canvas.</div>
  </div>
</div>`;
}

/* Wire up canvas interactions after DOM renders */
function _agWizInitReviewCanvas() {
    const wrap = document.getElementById('ag-wiz-canvas-wrap');
    if (!wrap || wrap._wired) return;
    wrap._wired = true;
    wrap.addEventListener('dblclick', _agWizCanvasFit);
}

function _agWizPanStart(e) {
    if (e.button !== 0) return;
    _AGWIZ.panning = true;
    _AGWIZ.panSX = e.clientX; _AGWIZ.panSY = e.clientY;
    _AGWIZ.panOX = _AGWIZ.panX; _AGWIZ.panOY = _AGWIZ.panY;
    e.currentTarget.style.cursor = 'grabbing';
}
function _agWizPanMove(e) {
    if (!_AGWIZ.panning) return;
    _AGWIZ.panX = _AGWIZ.panOX + (e.clientX - _AGWIZ.panSX);
    _AGWIZ.panY = _AGWIZ.panOY + (e.clientY - _AGWIZ.panSY);
    _agWizApplyTransform();
}
function _agWizPanEnd() {
    _AGWIZ.panning = false;
    const wrap = document.getElementById('ag-wiz-canvas-wrap');
    if (wrap) wrap.style.cursor = 'grab';
}
function _agWizCanvasZoom(delta) {
    _AGWIZ.zoom = Math.max(0.15, Math.min(3, _AGWIZ.zoom + delta));
    _agWizApplyTransform();
}
function _agWizApplyTransform() {
    const svg = document.getElementById('ag-wiz-canvas-svg');
    if (svg) svg.style.transform = `translate(${_AGWIZ.panX}px,${_AGWIZ.panY}px) scale(${_AGWIZ.zoom})`;
    const lbl = document.getElementById('ag-wiz-zoom-lbl');
    if (lbl) lbl.textContent = Math.round(_AGWIZ.zoom * 100) + '%';
}
function _agWizCanvasFit() {
    const wrap = document.getElementById('ag-wiz-canvas-wrap');
    const svg  = document.getElementById('ag-wiz-canvas-svg');
    if (!wrap || !svg) return;
    const svgW = parseInt(svg.getAttribute('width') || '800');
    const svgH = parseInt(svg.getAttribute('height') || '300');
    const pad  = 20;
    _AGWIZ.zoom = Math.max(0.15, Math.min(2, Math.min((wrap.clientWidth - pad*2) / svgW, (wrap.clientHeight - pad*2) / svgH)));
    _AGWIZ.panX = (wrap.clientWidth  - svgW * _AGWIZ.zoom) / 2;
    _AGWIZ.panY = (wrap.clientHeight - svgH * _AGWIZ.zoom) / 2;
    _agWizApplyTransform();
}

function _agWizDrawPipelineGraph() {
    const svg = document.getElementById('ag-wiz-canvas-svg'); if (!svg) return;
    const d = _AG.wizData;
    const PAD = 24, NW = 140, NH = 52, GAP = 70;
    const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const tr  = (s,n) => (s||'').length > n ? String(s).slice(0,n)+'…' : String(s||'');

    // Node definitions (ordered left → right)
    const nodes = [
        { id:'src',  label:d.source_table||'Events',        sub:d.source_type||'Kafka',           color:'#1a6fa8', icon:'◉'  },
        { id:'mem',  label:'Memory',                         sub:(d.mem_lt_store&&d.mem_lt_store!=='None')?'Short+Long':'Short-Term', color:'#4a2a9a', icon:'🧠' },
        { id:'llm',  label:tr(d.llm_model||'LLM Model',16), sub:d.llm_provider||'OpenAI',         color:'#2d6a4e', icon:'✦'  },
        { id:'orch', label:tr(d.agent_name||'Agent',16),    sub:d.orch_pattern?.includes('ReAct')?'ReActAgent':'WorkflowAgent', color:'#6a0a9a', icon:'⚛' },
        { id:'sink', label:tr(d.sink_table||'Output',16),   sub:d.sink_type||'Kafka',             color:'#0a5a5a', icon:'▣'  },
    ];
    const tools = [];
    if (d.tool1_name) tools.push({ label: tr(d.tool1_name,14), sub: 'HTTP Tool' });
    if (d.mcp_url)    tools.push({ label: tr(d.mcp_name||'MCP',14), sub: 'MCP Server' });

    const totalW  = nodes.length * (NW + GAP) - GAP + PAD * 2;
    const totalH  = NH * 3 + PAD * 2 + (tools.length ? NH + 40 : 0);
    const midY    = PAD + NH + 30;  // row for main nodes
    const bg      = '#060a12';
    const grid    = 'rgba(0,176,143,0.04)';
    const edgeCol = 'rgba(0,196,160,0.6)';

    let s = `<defs>
      <marker id="agwiz-arr" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
        <path d="M0,0.5 L0,6.5 L7,3.5 z" fill="${edgeCol}"/>
      </marker>
    </defs>
    <rect width="${totalW}" height="${totalH}" fill="${bg}"/>`;

    // Grid
    for (let x=0;x<totalW;x+=22) s += `<line x1="${x}" y1="0" x2="${x}" y2="${totalH}" stroke="${grid}" stroke-width="0.5"/>`;
    for (let y=0;y<totalH;y+=22) s += `<line x1="0" y1="${y}" x2="${totalW}" y2="${y}" stroke="${grid}" stroke-width="0.5"/>`;

    const bezier = (x1,y1,x2,y2) => { const cp=Math.abs(x2-x1)*0.42; return `<path d="M${x1},${y1} C${x1+cp},${y1} ${x2-cp},${y2} ${x2},${y2}" stroke="${edgeCol}" stroke-width="1.4" fill="none" opacity="0.85" marker-end="url(#agwiz-arr)"/>`; };

    // Draw main nodes + edges
    nodes.forEach((n, i) => {
        const x = PAD + i * (NW + GAP);
        const y = midY;
        const mid = y + NH / 2;
        const isOrch = n.id === 'orch';
        s += `<rect x="${x}" y="${y}" width="${NW}" height="${NH}" rx="8" fill="${n.color}20" stroke="${n.color}" stroke-width="${isOrch?2.5:1.8}"/>`;
        if (isOrch) s += `<rect x="${x}" y="${y}" width="${NW}" height="${NH}" rx="8" fill="none" stroke="${n.color}" stroke-width="1" opacity="0.18" filter="url(#agwiz-glow)"/>`;
        s += `<text x="${x+10}" y="${mid-8}" fill="${n.color}" font-size="14" font-family="monospace" dominant-baseline="middle">${esc(n.icon)}</text>`;
        s += `<text x="${x+28}" y="${mid-8}" fill="${isOrch?'#c0ffe8':'#e0f0ff'}" font-size="11" font-weight="700" font-family="var(--mono,monospace)" dominant-baseline="middle">${esc(tr(n.label,14))}</text>`;
        s += `<text x="${x+28}" y="${mid+8}" fill="${n.color}99" font-size="9" font-family="var(--mono,monospace)" dominant-baseline="middle">${esc(tr(n.sub,18))}</text>`;
        // Edge to next node
        if (i < nodes.length - 1) {
            s += bezier(x + NW, mid, x + NW + GAP, midY + NH/2);
        }
    });

    // Memory feeds into orch from above
    const memX  = PAD + 1 * (NW + GAP);
    const orchX = PAD + 3 * (NW + GAP);
    const orchMidY = midY + NH / 2;
    s += bezier(memX + NW/2, midY, orchX + NW/2, midY);

    // Tool nodes below orch
    if (tools.length) {
        const toolY = midY + NH + 40;
        const orchMidX = orchX + NW / 2;
        tools.forEach((t, i) => {
            const tx = orchX - (tools.length - 1) * (NW/2 + 10) + i * (NW + 20);
            const ty = toolY;
            s += `<rect x="${tx}" y="${ty}" width="${NW}" height="${NH-10}" rx="5" fill="rgba(0,176,143,0.07)" stroke="rgba(0,196,160,0.4)" stroke-width="1.2"/>`;
            s += `<text x="${tx+10}" y="${ty+20}" fill="#00c4a0" font-size="10" font-weight="700" font-family="var(--mono,monospace)" dominant-baseline="middle">🔌 ${esc(t.label)}</text>`;
            s += `<text x="${tx+10}" y="${ty+34}" fill="rgba(0,196,160,0.5)" font-size="9" font-family="var(--mono,monospace)" dominant-baseline="middle">${esc(t.sub)}</text>`;
            // Arrow from orch down to tool
            s += `<path d="M${orchMidX},${midY+NH} L${tx+NW/2},${ty}" stroke="rgba(0,196,160,0.4)" stroke-width="1.2" stroke-dasharray="4 3" fill="none" marker-end="url(#agwiz-arr)"/>`;
        });
    }

    // Obs log node — below sink
    const sinkX    = PAD + 4 * (NW + GAP);
    const obsY     = midY + NH + 40;
    const sinkMidX = sinkX + NW / 2;
    s += `<rect x="${sinkX}" y="${obsY}" width="${NW}" height="${NH-10}" rx="5" fill="rgba(42,74,42,0.25)" stroke="rgba(0,196,160,0.3)" stroke-width="1.2"/>`;
    s += `<text x="${sinkX+10}" y="${obsY+20}" fill="rgba(0,196,160,0.8)" font-size="10" font-weight="700" font-family="var(--mono,monospace)" dominant-baseline="middle">◉ ${esc(tr(d.obs_topic||'Event Log',14))}</text>`;
    s += `<text x="${sinkX+10}" y="${obsY+34}" fill="rgba(0,196,160,0.45)" font-size="9" font-family="var(--mono,monospace)" dominant-baseline="middle">Event Log</text>`;
    s += `<path d="M${sinkMidX},${midY+NH} L${sinkMidX},${obsY}" stroke="rgba(0,196,160,0.35)" stroke-width="1.2" stroke-dasharray="4 3" fill="none" marker-end="url(#agwiz-arr)"/>`;

    // Column labels above nodes
    ['Source', 'Memory', 'LLM', 'Orchestrator', 'Sink'].forEach((label, i) => {
        const x = PAD + i * (NW + GAP) + NW / 2;
        s += `<text x="${x}" y="${midY - 10}" fill="rgba(0,196,160,0.35)" font-size="8" font-weight="700" font-family="var(--mono,monospace)" text-anchor="middle">${label.toUpperCase()}</text>`;
    });

    svg.setAttribute('width',  totalW);
    svg.setAttribute('height', totalH);
    svg.setAttribute('viewBox', `0 0 ${totalW} ${totalH}`);
    svg.innerHTML = s;
    setTimeout(_agWizCanvasFit, 30);
}

/* Hook into the existing _agWizGoStep to draw graph when step 8 loads */
(function _patchWizGoStepForGraph() {
    const _origGoStep = window._agWizGoStep;
    window._agWizGoStep = function(n) {
        _origGoStep(n);
        if (n === 8) {
            setTimeout(() => {
                _agWizInitReviewCanvas();
                _agWizDrawPipelineGraph();
            }, 80);
        }
    };
})();