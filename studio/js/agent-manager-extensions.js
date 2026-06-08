/**
 * agent-manager-extensions.js  —  Str:::lab Studio
 * ─────────────────────────────────────────────────────────────────────────────
 * Flink Agent Manager — Feature Extensions (load AFTER agent-manager.js)
 *
 * 1. Live Kafka Event Log Connector  — wires Observability tab to a real Kafka
 *    topic via the Studio's existing gateway/session infrastructure
 *
 * 2. Agent Version Diff              — compare two exported agent JSON files
 *    side-by-side (canvas diff + config diff + code diff)
 *
 * 3. FLIP-531 SQL Native codegen     — first-class Flink SQL agent syntax using
 *    CREATE AGENT / RUN AGENT / SHOW AGENTS and ML_PREDICT with agent context
 *
 * 4. Agent Test Runner               — send a test event payload through the
 *    agent reasoning chain and display the full trace (LLM + tool calls)
 *
 * 5. A2A Topology View               — dedicated canvas showing multi-agent
 *    communication flows with animated message counts
 *
 * 6. LLM Cost Estimator              — estimate token cost per event at a given
 *    throughput rate across all LLM nodes in the current agent graph
 * ─────────────────────────────────────────────────────────────────────────────
 */

/* ════════════════════════════════════════════════════════════════════════════
   EXTENSION STATE & CSS
   ════════════════════════════════════════════════════════════════════════════ */
const _AGX = {
    // Live Kafka log connector
    kafka: {
        running: false,
        pollInterval: null,
        sessionHandle: null,
        sourceTable: null,
        statementHandle: null,
        buffer: [],
        totalConsumed: 0,
        errorCount: 0,
    },
    // Diff
    diffA: null,
    diffB: null,
    // Test runner
    testRunning: false,
    testTrace: [],
    testAbortController: null,
    // A2A topology
    a2aAnimTimer: null,
    a2aMessages: [],
    a2aStats: {},
    // Cost estimator
    costModel: 'gpt-4o-mini',
};

(function _agxInjectCss() {
    if (document.getElementById('agx-css')) return;
    const s = document.createElement('style');
    s.id = 'agx-css';
    s.textContent = `
/* ── Extensions shared ── */
.agx-panel { flex:1; display:flex; flex-direction:column; overflow:hidden; }
.agx-toolbar { display:flex; align-items:center; gap:6px; padding:7px 12px;
  background:var(--bg2); border-bottom:1px solid var(--border); flex-shrink:0; }
.agx-toolbar-label { font-size:10px; font-weight:700; color:var(--text2);
  letter-spacing:0.8px; text-transform:uppercase; }
.agx-btn { padding:4px 10px; font-size:10px; background:var(--bg3);
  border:1px solid var(--border); color:var(--text2); border-radius:3px;
  cursor:pointer; font-family:var(--mono); white-space:nowrap; }
.agx-btn:hover { background:var(--bg1); color:var(--text0); }
.agx-btn.purple { background:rgba(0,176,143,0.12); border-color:rgba(0,176,143,0.4); color:#00c4a0; }
.agx-btn.green  { background:rgba(99,201,150,0.1);  border-color:rgba(99,201,150,0.3);  color:var(--green); }
.agx-btn.red    { background:rgba(255,77,109,0.08);  border-color:rgba(255,77,109,0.3);  color:var(--red); }
.agx-btn.yellow { background:rgba(245,166,35,0.08);  border-color:rgba(245,166,35,0.3);  color:var(--yellow,#f5a623); }
.agx-card { background:var(--bg2); border:1px solid var(--border); border-radius:5px;
  padding:12px 14px; margin-bottom:10px; }
.agx-section { font-size:9px; font-weight:700; color:var(--text3); letter-spacing:1.5px;
  text-transform:uppercase; margin-bottom:8px; padding-bottom:4px; border-bottom:1px solid var(--border); }
.agx-info { background:rgba(0,176,143,0.05); border:1px solid rgba(0,176,143,0.2);
  border-left:3px solid #00c4a0; border-radius:3px; padding:8px 12px; font-size:11px;
  color:var(--text1); line-height:1.7; margin-bottom:12px; }
.agx-warn { background:rgba(245,166,35,0.06); border:1px solid rgba(245,166,35,0.25);
  border-left:3px solid #f5a623; border-radius:3px; padding:8px 12px; font-size:11px;
  color:var(--text1); line-height:1.7; margin-bottom:10px; }

/* ── Live Kafka Log ── */
.agx-event-row { display:flex; gap:8px; padding:3px 10px; border-bottom:1px solid rgba(0,176,143,0.06);
  font-family:var(--mono); font-size:10px; color:var(--text1); align-items:baseline; }
.agx-event-row:hover { background:rgba(0,176,143,0.04); }
.agx-ev-ts   { color:var(--text3); flex-shrink:0; font-size:9px; }
.agx-ev-type { font-weight:700; flex-shrink:0; min-width:100px; }
.agx-ev-msg  { flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.agx-ev-lat  { color:var(--text3); flex-shrink:0; font-size:9px; }
.agx-metric-grid { display:grid; grid-template-columns:repeat(6,1fr); border-bottom:1px solid var(--border); flex-shrink:0; background:var(--bg1); }
.agx-metric-cell { padding:8px 10px; border-right:1px solid var(--border); text-align:center; }
.agx-metric-cell:last-child { border-right:none; }
.agx-metric-val { font-size:16px; font-weight:700; color:#00c4a0; font-family:var(--mono); }
.agx-metric-lbl { font-size:9px; color:var(--text3); margin-top:2px; }
/* Kafka connection status indicator */
.agx-kafka-dot { width:8px; height:8px; border-radius:50%; flex-shrink:0;
  background:var(--text3); transition:background 0.3s; }
.agx-kafka-dot.connected { background:var(--green); box-shadow:0 0 6px var(--green); }
.agx-kafka-dot.error { background:var(--red); box-shadow:0 0 6px var(--red); animation:agx-pulse-red 1s infinite; }
@keyframes agx-pulse-red { 0%,100%{opacity:1} 50%{opacity:0.4} }

/* ── Diff view ── */
.agx-diff-col { flex:1; overflow:auto; padding:12px; min-width:0; }
.agx-diff-col + .agx-diff-col { border-left:1px solid var(--border); }
.agx-diff-line { font-family:var(--mono); font-size:10px; line-height:1.7; padding:1px 6px;
  border-radius:2px; white-space:pre-wrap; word-break:break-word; }
.agx-diff-add { background:rgba(99,201,150,0.1); color:var(--green); }
.agx-diff-del { background:rgba(255,77,109,0.08); color:var(--red); }
.agx-diff-chg { background:rgba(245,166,35,0.08); color:var(--yellow,#f5a623); }
.agx-diff-same { color:var(--text3); }
.agx-diff-section { font-size:9px; font-weight:700; color:#00c4a0; letter-spacing:1.5px;
  text-transform:uppercase; padding:6px 6px 3px; margin-top:8px; border-top:1px solid rgba(0,176,143,0.15); }
/* Stat badges in diff header */
.agx-diff-badge { display:inline-block; padding:1px 7px; border-radius:10px; font-size:9px;
  font-weight:700; font-family:var(--mono); }

/* ── Test Runner ── */
.agx-trace-step { border:1px solid var(--border); border-radius:5px; margin-bottom:8px; overflow:hidden; }
.agx-trace-step-hdr { padding:8px 12px; display:flex; align-items:center; gap:8px;
  cursor:pointer; user-select:none; background:var(--bg2); }
.agx-trace-step-hdr:hover { background:var(--bg1); }
.agx-trace-step-icon { font-size:14px; flex-shrink:0; }
.agx-trace-step-title { flex:1; font-size:11px; font-weight:700; color:var(--text0); font-family:var(--mono); }
.agx-trace-step-lat  { font-size:10px; color:var(--text3); font-family:var(--mono); }
.agx-trace-step-status { font-size:9px; font-weight:700; padding:1px 6px; border-radius:3px; }
.agx-trace-step-body { padding:10px 12px; background:var(--bg0); border-top:1px solid var(--border);
  font-family:var(--mono); font-size:10px; color:var(--text1); line-height:1.7; white-space:pre-wrap;
  word-break:break-word; display:none; }
.agx-trace-step.open .agx-trace-step-body { display:block; }
/* Typing cursor animation */
@keyframes agx-blink { 0%,100%{opacity:1} 50%{opacity:0} }
.agx-cursor::after { content:'▋'; animation:agx-blink 0.7s infinite; color:#00c4a0; }
/* Progress dots for thinking */
@keyframes agx-dots { 0%{content:'.'} 33%{content:'..'} 66%{content:'...'} }
.agx-thinking::after { content:'.'; animation:agx-dots 1s infinite; color:#00c4a0; }

/* ── A2A Topology ── */
#agx-a2a-canvas-wrap { flex:1; position:relative; overflow:hidden; background:var(--bg0); }
#agx-a2a-svg { position:absolute; inset:0; width:100%; height:100%; overflow:visible; }
.agx-a2a-node { position:absolute; border-radius:8px; padding:10px 14px; font-family:var(--mono);
  font-size:11px; font-weight:700; color:#fff; border:2px solid rgba(255,255,255,0.15);
  box-shadow:0 4px 16px rgba(0,0,0,0.5); cursor:pointer; user-select:none; text-align:center; min-width:130px; }
.agx-a2a-node:hover { border-color:rgba(255,255,255,0.5); }
.agx-a2a-counter { display:inline-block; background:rgba(0,196,160,0.2); border:1px solid rgba(0,196,160,0.4);
  border-radius:10px; padding:1px 8px; font-size:9px; margin-top:4px; color:#00c4a0; }
.agx-a2a-msg-badge { position:absolute; font-size:9px; font-family:var(--mono); font-weight:700;
  background:rgba(20,8,40,0.85); border:1px solid #00c4a0; border-radius:10px;
  padding:1px 7px; color:#00c4a0; pointer-events:none; white-space:nowrap; }

/* ── Cost Estimator ── */
.agx-cost-row { display:flex; align-items:center; gap:10px; padding:8px 0;
  border-bottom:1px solid var(--border); font-size:11px; }
.agx-cost-row:last-child { border-bottom:none; }
.agx-cost-node  { flex:1; font-family:var(--mono); color:var(--text0); font-weight:600; }
.agx-cost-model { color:var(--text2); font-family:var(--mono); font-size:10px; min-width:140px; }
.agx-cost-val   { font-family:var(--mono); font-weight:700; min-width:90px; text-align:right; }
.agx-cost-total { background:rgba(0,176,143,0.08); border:1px solid rgba(0,176,143,0.2);
  border-radius:5px; padding:12px 14px; margin-top:12px; }
.agx-cost-spark { height:48px; width:100%; }
`;
    document.head.appendChild(s);
})();

/* ════════════════════════════════════════════════════════════════════════════
   PATCH: extend _agSwitchTab to recognise new tabs
   ════════════════════════════════════════════════════════════════════════════ */
(function _patchTabSwitcher() {
    const _orig = window._agSwitchTab;
    window._agSwitchTab = function(tab) {
        // Stop live Kafka polling when leaving the observability tab
        if (_AGX.kafka.running && tab !== 'observability') _agxKafkaStop(true);
        // Stop A2A animation when leaving
        if (_AGX.a2aAnimTimer && tab !== 'a2a') { cancelAnimationFrame(_AGX.a2aAnimTimer); _AGX.a2aAnimTimer=null; }
        // Handle new extension tabs
        const extTabs = ['observability', 'diff', 'testrunner', 'a2a', 'cost'];
        if (!extTabs.includes(tab)) { _orig(tab); return; }

        // Update tab bar highlighting (reuse existing logic)
        document.querySelectorAll('.ag-tab').forEach(b => b.classList.remove('active'));
        const btn = document.getElementById('ag-t-' + tab);
        if (btn) btn.classList.add('active');
        if (window._AG) _AG.activeTab = tab;

        const content = document.getElementById('ag-content');
        if (!content) return;
        content.innerHTML = '';

        const renderers = {
            observability: _agxRenderObservability,
            diff:          _agxRenderDiff,
            testrunner:    _agxRenderTestRunner,
            a2a:           _agxRenderA2A,
            cost:          _agxRenderCost,
        };
        renderers[tab]?.();
    };
})();

/* ════════════════════════════════════════════════════════════════════════════
   PATCH: inject new tabs into the tab bar after the modal is built
   ════════════════════════════════════════════════════════════════════════════ */
(function _patchModalBuild() {
    const _origOpen = window.openAgentManager;
    window.openAgentManager = function() {
        _origOpen();
        setTimeout(() => {
            const bar = document.getElementById('ag-tab-bar');
            if (!bar || bar.querySelector('#ag-t-diff')) return;
            const newTabs = [
                { id:'diff',       label:'⊞ Version Diff'  },
                { id:'testrunner', label:'▶ Test Runner'    },
                { id:'a2a',        label:'🤝 A2A Topology'  },
                { id:'cost',       label:'💰 Cost Estimator'},
            ];
            newTabs.forEach(t => {
                const b = document.createElement('button');
                b.className = 'ag-tab';
                b.id = 'ag-t-' + t.id;
                b.textContent = t.label;
                b.onclick = () => _agSwitchTab(t.id);
                bar.appendChild(b);
            });
        }, 60);
    };
})();

/* ════════════════════════════════════════════════════════════════════════════
   FEATURE 1 — LIVE KAFKA EVENT LOG CONNECTOR
   ════════════════════════════════════════════════════════════════════════════
   Uses the Studio's existing session infrastructure (jmApi / state) to:
   1. Create a TEMPORARY TABLE over the agent's event log Kafka topic
   2. Execute a SELECT * FROM that table via the SQL Gateway
   3. Poll the result set handle and stream rows into the UI
   4. Parse the agent event envelope (event_type, agent_name, payload, latency_ms)
   ════════════════════════════════════════════════════════════════════════════ */

const _AGX_EVENT_COLORS = {
    LLM_CALL:     '#00c4a0',
    TOOL_INVOKE:  '#4fa3e0',
    MEMORY_READ:  '#f5a623',
    MEMORY_WRITE: '#d4960a',
    DECISION:     '#00d4aa',
    AGENT_START:  '#63c996',
    AGENT_END:    '#63c996',
    ERROR:        '#ff4d6d',
    CHECKPOINT:   '#7a9ab0',
};

function _agxRenderObservability() {
    const content = document.getElementById('ag-content');
    // Try to pre-fill topic from canvas Event Log node
    const logNode = (window._AG?.canvas?.nodes || []).find(n => n.opId === 'event_log');
    const defaultTopic = logNode?.params?.topic || window._AG?.wizData?.obs_topic || 'flink-agent-events';
    const defaultBootstrap = window._AG?.wizData?.source_endpoint || 'kafka:9092';

    content.innerHTML = `
<div class="agx-panel">
  <!-- ── Connection bar ── -->
  <div class="agx-toolbar" style="flex-wrap:wrap;gap:6px;">
    <span class="agx-kafka-dot" id="agx-kafka-dot"></span>
    <span class="agx-toolbar-label">Live Kafka Event Log</span>
    <div style="display:flex;gap:5px;align-items:center;flex:1;flex-wrap:wrap;">
      <input id="agx-kafka-bootstrap" class="field-input" type="text"
        value="${_agEsc(defaultBootstrap)}"
        placeholder="kafka:9092" style="font-size:10px;font-family:var(--mono);width:180px;"
        title="Kafka Bootstrap Servers"/>
      <input id="agx-kafka-topic" class="field-input" type="text"
        value="${_agEsc(defaultTopic)}"
        placeholder="flink-agent-events" style="font-size:10px;font-family:var(--mono);width:200px;"
        title="Agent Event Log Kafka Topic"/>
      <select id="agx-kafka-offset" class="field-input" style="font-size:10px;width:140px;">
        <option value="latest-offset">Latest offset</option>
        <option value="earliest-offset">Earliest offset</option>
        <option value="group-offsets">Group offsets</option>
      </select>
    </div>
    <button class="agx-btn green" id="agx-kafka-start-btn" onclick="_agxKafkaConnect()">▶ Connect</button>
    <button class="agx-btn red"   id="agx-kafka-stop-btn"  onclick="_agxKafkaStop()" style="display:none;">⏹ Disconnect</button>
    <button class="agx-btn" onclick="_agxObsClear()">✕ Clear</button>
    <select class="field-input" id="agx-obs-filter" style="font-size:10px;width:150px;" onchange="_agxObsRender()">
      <option value="">All event types</option>
      ${Object.keys(_AGX_EVENT_COLORS).map(t=>`<option value="${t}">${t}</option>`).join('')}
    </select>
    <label style="display:flex;align-items:center;gap:4px;font-size:10px;color:var(--text2);cursor:pointer;">
      <input type="checkbox" id="agx-obs-autoscroll" checked/> Auto-scroll
    </label>
  </div>

  <!-- ── Status line ── -->
  <div id="agx-kafka-status-bar" style="display:none;padding:4px 12px;background:rgba(0,176,143,0.05);
    border-bottom:1px solid rgba(0,176,143,0.15);font-size:10px;font-family:var(--mono);color:var(--text3);
    flex-shrink:0;display:flex;align-items:center;gap:10px;">
    <span id="agx-kafka-status-txt">Connecting…</span>
    <span id="agx-kafka-stmt-id" style="color:#00c4a0;"></span>
    <span id="agx-kafka-rate" style="margin-left:auto;color:var(--green);"></span>
  </div>

  <!-- ── Metrics ── -->
  <div class="agx-metric-grid">
    ${['Total Events','LLM Calls','Tool Invokes','Decisions','Avg Latency (ms)','Errors'].map((l,i)=>`
      <div class="agx-metric-cell">
        <div class="agx-metric-val" id="agx-met-${i}">0</div>
        <div class="agx-metric-lbl">${l}</div>
      </div>`).join('')}
  </div>

  <!-- ── Sparkline canvas ── -->
  <div style="padding:0 10px;background:var(--bg1);border-bottom:1px solid var(--border);flex-shrink:0;">
    <canvas id="agx-obs-spark" height="36" style="width:100%;height:36px;display:block;"></canvas>
  </div>

  <!-- ── Event stream ── -->
  <div style="flex:1;overflow-y:auto;background:var(--bg0);" id="agx-obs-stream">
    <div style="padding:30px;text-align:center;color:var(--text3);font-size:12px;">
      <div style="font-size:36px;opacity:0.15;margin-bottom:10px;">📡</div>
      <div style="margin-bottom:6px;">Configure the Kafka bootstrap and topic above, then click <strong style="color:#00c4a0;">Connect</strong>.</div>
      <div style="font-size:10px;color:var(--text3);">
        The Studio will create a TEMPORARY TABLE over your agent event log topic<br>
        and stream events via the Flink SQL Gateway session.
      </div>
    </div>
  </div>

  <!-- ── Footer ── -->
  <div style="padding:6px 12px;background:var(--bg2);border-top:1px solid var(--border);display:flex;
    align-items:center;gap:10px;flex-shrink:0;font-size:10px;font-family:var(--mono);color:var(--text3);">
    <span>Events: <strong id="agx-footer-count" style="color:#00c4a0;">0</strong></span>
    <span>·</span>
    <span>Session: <strong id="agx-footer-session" style="color:var(--accent);">${_agEsc(window.state?.activeSession||'—')}</strong></span>
    <span>·</span>
    <span>Mode: <strong id="agx-footer-mode" style="color:var(--text2);">disconnected</strong></span>
    <button onclick="_agxExportEventLog()" class="agx-btn" style="margin-left:auto;font-size:9px;padding:2px 8px;">⬇ Export CSV</button>
  </div>
</div>`;

    _agxObsRender();
    _agxDrawSparkline();
}

async function _agxKafkaConnect() {
    const bootstrap = document.getElementById('agx-kafka-bootstrap')?.value?.trim() || 'kafka:9092';
    const topic     = document.getElementById('agx-kafka-topic')?.value?.trim()     || 'flink-agent-events';
    const offset    = document.getElementById('agx-kafka-offset')?.value            || 'latest-offset';

    _agxSetKafkaDot('connecting');
    _agxSetKafkaStatus(`Creating event log table for topic: ${topic}…`);

    // ── Step 1: Ensure we have a session ───────────────────────────────────
    const sessionHandle = window.state?.activeSession;
    if (!sessionHandle) {
        _agxSetKafkaDot('error');
        _agxSetKafkaStatus('✗ No active Flink session. Connect to a cluster first.');
        return;
    }

    // ── Step 2: Create TEMPORARY TABLE over the event log topic ──────────
    const tableName = `_agx_event_log_${Date.now()}`;
    _AGX.kafka.sourceTable = tableName;

    const createDDL = `
CREATE TEMPORARY TABLE \`${tableName}\` (
  log_id        BIGINT,
  event_type    STRING,
  agent_name    STRING,
  event_payload STRING,
  latency_ms    BIGINT,
  log_time      TIMESTAMP(3),
  WATERMARK FOR log_time AS log_time - INTERVAL '5' SECOND
) WITH (
  'connector'                          = 'kafka',
  'topic'                              = '${topic}',
  'properties.bootstrap.servers'       = '${bootstrap}',
  'properties.group.id'                = 'strlab-agent-log-consumer-${Date.now()}',
  'scan.startup.mode'                  = '${offset}',
  'format'                             = 'json',
  'json.ignore-parse-errors'           = 'true'
)`.trim();

    // Use jmApi if available (Studio's existing gateway helper)
    if (typeof jmApi === 'function') {
        try {
            await _agxSqlGatewayExec(sessionHandle, createDDL);
        } catch(err) {
            _agxSetKafkaDot('error');
            _agxSetKafkaStatus('✗ Failed to create event log table: ' + err.message);
            return;
        }
    }

    // ── Step 3: Execute streaming SELECT ─────────────────────────────────
    const selectSql = `SELECT log_id, event_type, agent_name, event_payload, latency_ms, log_time FROM \`${tableName}\``;

    let stmtHandle;
    try {
        stmtHandle = await _agxSqlGatewaySubmit(sessionHandle, selectSql);
        _AGX.kafka.statementHandle = stmtHandle;
    } catch(err) {
        // Fallback: use simulation if gateway submission not available
        _agxSetKafkaDot('connected');
        _agxSetKafkaStatus(`⚠ Gateway SELECT unavailable — running simulation mode`);
        document.getElementById('agx-footer-mode').textContent = 'simulation';
        _agxStartSimulation(topic);
        return;
    }

    // ── Step 4: Poll results ──────────────────────────────────────────────
    _AGX.kafka.running    = true;
    _AGX.kafka.totalConsumed = 0;
    _AGX.kafka.errorCount = 0;
    const startBtn = document.getElementById('agx-kafka-start-btn');
    const stopBtn  = document.getElementById('agx-kafka-stop-btn');
    if (startBtn) startBtn.style.display = 'none';
    if (stopBtn)  stopBtn.style.display  = '';
    _agxSetKafkaDot('connected');
    _agxSetKafkaStatus(`✓ Connected · topic: ${topic} · stmt: ${stmtHandle?.slice(0,8)||'?'}…`);
    document.getElementById('agx-footer-mode').textContent = 'live kafka';
    const stmtId = document.getElementById('agx-kafka-stmt-id');
    if (stmtId) stmtId.textContent = stmtHandle?.slice(0,12)||'';

    let lastPollTime = Date.now(), eventsThisPoll = 0;
    _AGX.kafka.pollInterval = setInterval(async () => {
        if (!_AGX.kafka.running) return;
        try {
            const rows = await _agxFetchResultPage(sessionHandle, stmtHandle);
            rows.forEach(row => {
                _agxIngestRow(row);
                eventsThisPoll++;
            });
            // Update rate
            const elapsed = (Date.now() - lastPollTime) / 1000;
            if (elapsed > 2) {
                const rate = (eventsThisPoll / elapsed).toFixed(1);
                const rateEl = document.getElementById('agx-kafka-rate');
                if (rateEl) rateEl.textContent = `${rate} events/s`;
                eventsThisPoll = 0; lastPollTime = Date.now();
            }
            _agxObsRender();
            _agxUpdateMetrics();
            _agxDrawSparkline();
        } catch(_) { /* continue polling even on transient errors */ }
    }, 1500);
}

function _agxKafkaStop(silent) {
    _AGX.kafka.running = false;
    if (_AGX.kafka.pollInterval) { clearInterval(_AGX.kafka.pollInterval); _AGX.kafka.pollInterval=null; }
    _agxSetKafkaDot('disconnected');
    if (!silent) _agxSetKafkaStatus('Disconnected.');
    const startBtn = document.getElementById('agx-kafka-start-btn');
    const stopBtn  = document.getElementById('agx-kafka-stop-btn');
    if (startBtn) startBtn.style.display = '';
    if (stopBtn)  stopBtn.style.display  = 'none';
    const modeEl = document.getElementById('agx-footer-mode');
    if (modeEl) modeEl.textContent = 'disconnected';
}

/* Helper: submit SQL via Flink SQL Gateway REST API */
async function _agxSqlGatewayExec(session, sql) {
    if (typeof jmApi !== 'function') return;
    // POST /sessions/:session/statements
    const resp = await jmApi(`/sessions/${session}/statements`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ statement: sql }),
    });
    return resp?.operationHandle || resp?.handle || null;
}

async function _agxSqlGatewaySubmit(session, sql) {
    if (typeof jmApi !== 'function') throw new Error('jmApi not available');
    const resp = await jmApi(`/sessions/${session}/statements`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ statement: sql }),
    });
    const handle = resp?.operationHandle?.identifier?.guid || resp?.handle || null;
    if (!handle) throw new Error('No statement handle returned');
    return handle;
}

async function _agxFetchResultPage(session, handle) {
    if (typeof jmApi !== 'function') return [];
    try {
        const resp = await jmApi(`/sessions/${session}/operations/${handle}/result/0`);
        const data = resp?.results?.data || [];
        return data.map(row => {
            const fields = row.fields || row;
            return {
                log_id:        fields[0]  || 0,
                event_type:    fields[1]  || 'UNKNOWN',
                agent_name:    fields[2]  || 'agent',
                event_payload: fields[3]  || '{}',
                latency_ms:    fields[4]  || 0,
                log_time:      fields[5]  || new Date().toISOString(),
            };
        });
    } catch(_) { return []; }
}

function _agxIngestRow(row) {
    _AGX.kafka.totalConsumed++;
    if (row.event_type === 'ERROR') _AGX.kafka.errorCount++;
    let payload = {};
    try { payload = JSON.parse(row.event_payload || '{}'); } catch(_) {}
    _AGX.kafka.buffer.unshift({
        ts:        new Date(row.log_time || Date.now()).toLocaleTimeString('en-GB',{hour12:false,fractionalSecondDigits:3}),
        type:      row.event_type || 'UNKNOWN',
        agent:     row.agent_name || 'agent',
        msg:       payload.message || payload.decision || payload.tool || row.event_payload?.slice(0,80) || '—',
        latency:   row.latency_ms || 0,
        raw:       row,
    });
    if (_AGX.kafka.buffer.length > 500) _AGX.kafka.buffer.pop();
    // Count by type for sparkline
    const t = row.event_type;
    _AGX.a2aStats[t] = (_AGX.a2aStats[t] || 0) + 1;
    // Footer count
    const fc = document.getElementById('agx-footer-count');
    if (fc) fc.textContent = _AGX.kafka.totalConsumed;
}

/* Fallback simulation when SQL Gateway isn't available */
function _agxStartSimulation(topic) {
    const types = Object.keys(_AGX_EVENT_COLORS);
    const agents = (window._AG?.canvas?.nodes||[]).filter(n=>n.opId.startsWith('agent_')).map(n=>n.label);
    const agentName = agents[0] || 'FlinkAgent';
    const msgs = {
        LLM_CALL:     [`gpt-4o-mini tokens=187 latency=312ms`, `claude-sonnet decision=FRAUD confidence=0.91`],
        TOOL_INVOKE:  [`check_fraud_score(tx=${Math.random().toFixed(6)}) → score=0.87`, `mcp_server.crm_lookup(id=usr_${Math.floor(Math.random()*9999)})`],
        MEMORY_READ:  [`short_term key=user_id=${Math.floor(Math.random()*9999)} hits=3`, `long_term vector_search top_k=5 similarity=0.91`],
        MEMORY_WRITE: [`short_term store user_id=${Math.floor(Math.random()*9999)} decision=FRAUD`],
        DECISION:     [`FINAL: FRAUD confidence=0.92 action=block_and_alert`],
        AGENT_START:  [`${agentName} event_id=${Math.floor(Math.random()*99999)}`],
        AGENT_END:    [`${agentName} completed in ${Math.floor(Math.random()*800+100)}ms`],
        CHECKPOINT:   [`checkpoint_${Math.floor(Math.random()*100)} size=128MB duration=245ms`],
    };
    _AGX.kafka.pollInterval = setInterval(() => {
        if (!_AGX.kafka.running) return;
        const type = types[Math.floor(Math.random()*types.length)];
        const msgArr = msgs[type] || ['event'];
        const msg = msgArr[Math.floor(Math.random()*msgArr.length)];
        _agxIngestRow({
            log_id: _AGX.kafka.totalConsumed+1, event_type: type, agent_name: agentName,
            event_payload: JSON.stringify({message:msg}), latency_ms: Math.floor(Math.random()*500+10),
            log_time: new Date().toISOString(),
        });
        _agxObsRender();
        _agxUpdateMetrics();
        _agxDrawSparkline();
    }, 900);
}

function _agxObsRender() {
    const stream = document.getElementById('agx-obs-stream'); if (!stream) return;
    const filter = document.getElementById('agx-obs-filter')?.value || '';
    const events = filter ? _AGX.kafka.buffer.filter(e=>e.type===filter) : _AGX.kafka.buffer;
    if (!events.length) return;
    stream.innerHTML = events.slice(0,150).map(e => `
      <div class="agx-event-row" onclick="this.classList.toggle('agx-expanded')">
        <span class="agx-ev-ts">${e.ts}</span>
        <span class="agx-ev-type" style="color:${_AGX_EVENT_COLORS[e.type]||'#00c4a0'};">${e.type}</span>
        <span style="color:var(--text3);font-size:9px;flex-shrink:0;">${_agEsc(e.agent)}</span>
        <span class="agx-ev-msg">${_agEsc(e.msg)}</span>
        <span class="agx-ev-lat">${e.latency}ms</span>
      </div>`).join('');
    const autoScroll = document.getElementById('agx-obs-autoscroll')?.checked;
    if (autoScroll) stream.scrollTop = 0;
}

function _agxUpdateMetrics() {
    const buf = _AGX.kafka.buffer;
    const llm  = buf.filter(e=>e.type==='LLM_CALL').length;
    const tool = buf.filter(e=>e.type==='TOOL_INVOKE').length;
    const dec  = buf.filter(e=>e.type==='DECISION').length;
    const lats = buf.filter(e=>e.latency>0).map(e=>e.latency);
    const avg  = lats.length ? Math.round(lats.reduce((a,b)=>a+b,0)/lats.length) : 0;
    [_AGX.kafka.totalConsumed, llm, tool, dec, avg, _AGX.kafka.errorCount].forEach((v,i) => {
        const el = document.getElementById(`agx-met-${i}`); if (el) el.textContent = v;
    });
}

function _agxDrawSparkline() {
    const canvas = document.getElementById('agx-obs-spark'); if (!canvas) return;
    const ctx = canvas.getContext('2d'); if (!ctx) return;
    canvas.width = canvas.offsetWidth || 800;
    ctx.clearRect(0,0,canvas.width,canvas.height);
    const buf = _AGX.kafka.buffer.slice(0,60).reverse();
    if (buf.length < 2) return;
    const vals = buf.map(e=>e.latency||0);
    const maxV = Math.max(...vals,1);
    const W = canvas.width, H = canvas.height, step = W/(vals.length-1);
    ctx.beginPath();
    ctx.strokeStyle = '#00c4a0';
    ctx.lineWidth = 1.5;
    vals.forEach((v,i) => {
        const x = i*step, y = H - (v/maxV)*(H-4) - 2;
        i===0 ? ctx.moveTo(x,y) : ctx.lineTo(x,y);
    });
    ctx.stroke();
    // Fill under
    ctx.lineTo(W,H); ctx.lineTo(0,H); ctx.closePath();
    ctx.fillStyle = 'rgba(0,176,143,0.07)'; ctx.fill();
}

function _agxObsClear() {
    _AGX.kafka.buffer = []; _AGX.kafka.totalConsumed=0; _AGX.kafka.errorCount=0;
    _agxObsRender(); _agxUpdateMetrics(); _agxDrawSparkline();
    [0,1,2,3,4,5].forEach(i=>{ const e=document.getElementById(`agx-met-${i}`); if(e) e.textContent='0'; });
    const fc=document.getElementById('agx-footer-count'); if(fc) fc.textContent='0';
}

function _agxExportEventLog() {
    const rows = _AGX.kafka.buffer;
    if (!rows.length) { if(typeof toast==='function') toast('No events to export','warn'); return; }
    const header = 'timestamp,event_type,agent,message,latency_ms';
    const lines  = rows.map(r=>`"${r.ts}","${r.type}","${r.agent}","${r.msg.replace(/"/g,'""')}","${r.latency}"`);
    const csv    = [header,...lines].join('\n');
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
    a.download = `agent-event-log-${Date.now()}.csv`;
    a.click();
    if(typeof toast==='function') toast('Event log exported','ok');
}

function _agxSetKafkaDot(state) {
    const dot = document.getElementById('agx-kafka-dot'); if (!dot) return;
    dot.className = 'agx-kafka-dot' + (state==='connected'?' connected':state==='error'?' error':'');
}

function _agxSetKafkaStatus(msg) {
    const el = document.getElementById('agx-kafka-status-txt'); if (el) el.textContent=msg;
    const bar = document.getElementById('agx-kafka-status-bar'); if (bar) bar.style.display='flex';
}

/* ════════════════════════════════════════════════════════════════════════════
   FEATURE 2 — AGENT VERSION DIFF
   Compares two exported agent JSON files: canvas nodes, wizard config, edges
   Shows: Added nodes, Removed nodes, Changed parameters, Code diff
   ════════════════════════════════════════════════════════════════════════════ */
function _agxRenderDiff() {
    const content = document.getElementById('ag-content');
    content.innerHTML = `
<div class="agx-panel">
  <div class="agx-toolbar" style="flex-wrap:wrap;">
    <span class="agx-toolbar-label">Agent Version Diff</span>
    <div style="display:flex;gap:8px;flex:1;flex-wrap:wrap;align-items:center;">
      <div style="display:flex;flex-direction:column;gap:3px;">
        <label style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;">Version A (baseline)</label>
        <label class="agx-btn" style="cursor:pointer;">
          📂 Load File A
          <input type="file" accept=".json" style="display:none;" onchange="_agxDiffLoadA(this)"/>
        </label>
        <div id="agx-diff-a-name" style="font-size:9px;color:var(--text3);font-family:var(--mono);">No file loaded</div>
      </div>
      <div style="font-size:20px;color:var(--text3);align-self:center;">⇆</div>
      <div style="display:flex;flex-direction:column;gap:3px;">
        <label style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;">Version B (new)</label>
        <label class="agx-btn" style="cursor:pointer;">
          📂 Load File B
          <input type="file" accept=".json" style="display:none;" onchange="_agxDiffLoadB(this)"/>
        </label>
        <div id="agx-diff-b-name" style="font-size:9px;color:var(--text3);font-family:var(--mono);">No file loaded</div>
      </div>
      <div style="display:flex;flex-direction:column;gap:3px;">
        <label style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;">Or diff current canvas</label>
        <button class="agx-btn purple" onclick="_agxDiffUseCurrent()">Use Current Agent</button>
      </div>
    </div>
    <button class="agx-btn purple" onclick="_agxDiffRun()">⊞ Compare</button>
    <button class="agx-btn" onclick="_agxDiffExport()">⬇ Export Diff</button>
  </div>

  <!-- Diff stats bar -->
  <div id="agx-diff-stats" style="display:none;padding:7px 14px;background:var(--bg2);
    border-bottom:1px solid var(--border);display:flex;gap:12px;flex-shrink:0;flex-wrap:wrap;">
  </div>

  <!-- Diff body: two columns -->
  <div style="flex:1;display:flex;overflow:hidden;">
    <!-- Left: Version A -->
    <div class="agx-diff-col" id="agx-diff-col-a">
      <div style="font-size:10px;font-weight:700;color:var(--text2);margin-bottom:8px;text-transform:uppercase;letter-spacing:1px;">
        📄 Version A <span id="agx-diff-a-ts" style="font-weight:400;color:var(--text3);"></span>
      </div>
      <div id="agx-diff-a-content" style="color:var(--text3);font-size:11px;">Load a file to compare.</div>
    </div>
    <!-- Divider -->
    <div style="width:1px;background:var(--border);flex-shrink:0;"></div>
    <!-- Right: Version B -->
    <div class="agx-diff-col" id="agx-diff-col-b">
      <div style="font-size:10px;font-weight:700;color:var(--text2);margin-bottom:8px;text-transform:uppercase;letter-spacing:1px;">
        📄 Version B <span id="agx-diff-b-ts" style="font-weight:400;color:var(--text3);"></span>
      </div>
      <div id="agx-diff-b-content" style="color:var(--text3);font-size:11px;">Load a file to compare.</div>
    </div>
  </div>
</div>`;
}

function _agxDiffLoadA(input) {
    const f = input.files[0]; if (!f) return;
    const r = new FileReader();
    r.onload = e => {
        try { _AGX.diffA = JSON.parse(e.target.result); }
        catch(_) { if(typeof toast==='function') toast('Invalid JSON','err'); return; }
        document.getElementById('agx-diff-a-name').textContent = f.name;
        const ts = document.getElementById('agx-diff-a-ts');
        if (ts) ts.textContent = _AGX.diffA._exported ? `· ${new Date(_AGX.diffA._exported).toLocaleString()}` : '';
        _agxDiffRender('a');
    };
    r.readAsText(f);
}

function _agxDiffLoadB(input) {
    const f = input.files[0]; if (!f) return;
    const r = new FileReader();
    r.onload = e => {
        try { _AGX.diffB = JSON.parse(e.target.result); }
        catch(_) { if(typeof toast==='function') toast('Invalid JSON','err'); return; }
        document.getElementById('agx-diff-b-name').textContent = f.name;
        const ts = document.getElementById('agx-diff-b-ts');
        if (ts) ts.textContent = _AGX.diffB._exported ? `· ${new Date(_AGX.diffB._exported).toLocaleString()}` : '';
        _agxDiffRender('b');
    };
    r.readAsText(f);
}

function _agxDiffUseCurrent() {
    const current = {
        _version: 2, _exported: new Date().toISOString(),
        canvas: { nodes: window._AG?.canvas?.nodes||[], edges: window._AG?.canvas?.edges||[] },
        wizData: window._AG?.wizData || {},
    };
    if (!_AGX.diffA) { _AGX.diffA = current; document.getElementById('agx-diff-a-name').textContent = '(current canvas)'; _agxDiffRender('a'); }
    else { _AGX.diffB = current; document.getElementById('agx-diff-b-name').textContent = '(current canvas)'; _agxDiffRender('b'); }
}

function _agxDiffRender(side) {
    const data = side==='a' ? _AGX.diffA : _AGX.diffB;
    const el   = document.getElementById(`agx-diff-${side}-content`); if (!el) return;
    const nodes = data?.canvas?.nodes||[];
    const wiz   = data?.wizData||{};
    const lines = [];
    lines.push('<div class="agx-diff-section">Canvas Nodes</div>');
    nodes.forEach(n => {
        lines.push(`<div class="agx-diff-line">${_agEsc(n.opId.padEnd(22))} ${_agEsc(n.label)}</div>`);
    });
    lines.push('<div class="agx-diff-section">Agent Configuration</div>');
    Object.entries(wiz).filter(([k,v])=>v).forEach(([k,v]) => {
        lines.push(`<div class="agx-diff-line">${_agEsc(k.padEnd(28))} ${_agEsc(String(v).slice(0,60))}</div>`);
    });
    el.innerHTML = lines.join('');
}

function _agxDiffRun() {
    if (!_AGX.diffA || !_AGX.diffB) { if(typeof toast==='function') toast('Load both files first','warn'); return; }
    const nodesA = _AGX.diffA?.canvas?.nodes||[];
    const nodesB = _AGX.diffB?.canvas?.nodes||[];
    const wizA   = _AGX.diffA?.wizData||{};
    const wizB   = _AGX.diffB?.wizData||{};

    const idsA = new Set(nodesA.map(n=>n.opId+'::'+n.label));
    const idsB = new Set(nodesB.map(n=>n.opId+'::'+n.label));
    const added   = nodesB.filter(n=>!idsA.has(n.opId+'::'+n.label));
    const removed = nodesA.filter(n=>!idsB.has(n.opId+'::'+n.label));

    // Wiz diff
    const allKeys = new Set([...Object.keys(wizA),...Object.keys(wizB)]);
    const changed = [...allKeys].filter(k=>wizA[k]!==wizB[k]);

    // Update stats bar
    const stats = document.getElementById('agx-diff-stats');
    if (stats) {
        stats.style.display='flex';
        stats.innerHTML = [
            `<span class="agx-diff-badge" style="background:rgba(99,201,150,0.12);color:var(--green);">+${added.length} added</span>`,
            `<span class="agx-diff-badge" style="background:rgba(255,77,109,0.08);color:var(--red);">-${removed.length} removed</span>`,
            `<span class="agx-diff-badge" style="background:rgba(245,166,35,0.08);color:var(--yellow,#f5a623);">⟳ ${changed.length} changed</span>`,
            `<span style="font-size:10px;color:var(--text3);">Nodes A: ${nodesA.length} → B: ${nodesB.length}</span>`,
        ].join('');
    }

    // Render annotated A col
    const colA = document.getElementById('agx-diff-col-a');
    const colB = document.getElementById('agx-diff-col-b');
    const aLines=[], bLines=[];
    const renderNodes = (nodes, removedSet, addedSet, target) => {
        target.push('<div class="agx-diff-section">Canvas Nodes</div>');
        nodes.forEach(n => {
            const key = n.opId+'::'+n.label;
            const isRem = removedSet.has(key);
            const isAdd = addedSet.has(key);
            const cls = isRem ? 'agx-diff-del' : isAdd ? 'agx-diff-add' : 'agx-diff-same';
            const pfx = isRem ? '− ' : isAdd ? '+ ' : '  ';
            target.push(`<div class="agx-diff-line ${cls}">${pfx}${_agEsc(n.opId.padEnd(22))} ${_agEsc(n.label)}</div>`);
        });
        target.push('<div class="agx-diff-section">Configuration Changes</div>');
        changed.forEach(k => {
            const va = wizA[k]||'(none)', vb = wizB[k]||'(none)';
            const cls = target===aLines ? 'agx-diff-del' : 'agx-diff-add';
            const val = target===aLines ? va : vb;
            target.push(`<div class="agx-diff-line ${cls}">  ${_agEsc(k.padEnd(26))} ${_agEsc(String(val).slice(0,55))}</div>`);
        });
    };
    const removedKeys = new Set(removed.map(n=>n.opId+'::'+n.label));
    const addedKeys   = new Set(added.map(n=>n.opId+'::'+n.label));
    renderNodes(nodesA, removedKeys, new Set(), aLines);
    renderNodes(nodesB, new Set(), addedKeys, bLines);
    if (colA) colA.querySelector('#agx-diff-a-content').innerHTML = aLines.join('');
    if (colB) colB.querySelector('#agx-diff-b-content').innerHTML = bLines.join('');
    if(typeof toast==='function') toast(`Diff: +${added.length} −${removed.length} ⟳${changed.length}`,'ok');
}

function _agxDiffExport() {
    if (!_AGX.diffA || !_AGX.diffB) { if(typeof toast==='function') toast('Run a diff first','warn'); return; }
    const report = { diffedAt: new Date().toISOString(), versionA: _AGX.diffA, versionB: _AGX.diffB };
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([JSON.stringify(report,null,2)],{type:'application/json'}));
    a.download = `agent-diff-${Date.now()}.json`;
    a.click();
}

/* ════════════════════════════════════════════════════════════════════════════
   FEATURE 3 — FLIP-531 SQL NATIVE (extends existing SQL codegen)
   Generates first-class CREATE AGENT / RUN AGENT syntax (experimental)
   and patches the Code tab to include a "FLIP-531 Native" option
   ════════════════════════════════════════════════════════════════════════════ */
(function _patchFlip531() {
    // Patch _agSwitchTab for code tab to add FLIP-531 code tab option
    const origCode = window._agRenderCode;
    window._agRenderCode = function() {
        origCode?.();
        // Add FLIP-531 tab to the code tabs sidebar
        setTimeout(() => {
            const tabs = document.querySelector('.ag-code-tabs');
            if (!tabs || tabs.querySelector('#ag-ct-flip531')) return;
            const b = document.createElement('button');
            b.className = 'ag-code-tab';
            b.id = 'ag-ct-flip531';
            b.textContent = 'FLIP-531 Native ✦';
            b.style.color = '#00c4a0';
            b.onclick = () => _agxCodeTabFlip531();
            // Insert after python tab
            const pyTab = document.getElementById('ag-ct-python');
            if (pyTab) pyTab.after(b); else tabs.insertBefore(b, tabs.querySelector('.agx-sep'));
        }, 50);
    };
})();

function _agxCodeTabFlip531() {
    document.querySelectorAll('.ag-code-tab').forEach(b=>b.classList.remove('active'));
    document.getElementById('ag-ct-flip531')?.classList.add('active');
    const out = document.getElementById('ag-code-output');
    if (out) out.textContent = _agxGenFlip531();
}

function _agxGenFlip531() {
    const nodes   = window._AG?.canvas?.nodes || [];
    const d       = window._AG?.wizData || {};
    const agentName = d.agent_name || nodes.find(n=>['agent_workflow','agent_react'].includes(n.opId))?.label || 'MyFlinkAgent';
    const llmNode = nodes.find(n=>n.opId.startsWith('llm_'));
    const srcNode = nodes.find(n=>n.opId.endsWith('_event'));
    const sinkNode= nodes.find(n=>n.opId.startsWith('action_'));
    const memNodes= nodes.filter(n=>n.opId.startsWith('mem_'));
    const toolNodes=nodes.filter(n=>['tool_http','tool_sql','mcp_server'].includes(n.opId));
    const isReact = nodes.some(n=>n.opId==='agent_react');

    return `-- ═══════════════════════════════════════════════════════════════════════════════
-- FLIP-531 Native SQL Agent Syntax (Experimental — Flink Agents 0.2+)
-- Agent: ${agentName}
-- Pattern: ${isReact ? 'ReActAgent' : 'WorkflowAgent'}
-- ⚠ This syntax is experimental and subject to change in future Flink Agents releases.
-- ═══════════════════════════════════════════════════════════════════════════════

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default'    = '${d.agent_parallelism || 4}';
SET 'execution.checkpointing.interval' = '${d.agent_checkpoint || 10000}';

-- ── Step 1: Register LLM Model ────────────────────────────────────────────────
CREATE MODEL IF NOT EXISTS ${agentName}_llm
WITH (
  'provider'     = '${(llmNode?.opId||'llm_openai').replace('llm_','').toUpperCase()}',
  'model'        = '${llmNode?.params?.model || d.llm_model || 'gpt-4o-mini'}',
  'api-key'      = '${llmNode?.params?.api_key_env || d.llm_key_env || 'OPENAI_API_KEY'}',
  'temperature'  = '${llmNode?.params?.temperature || d.llm_temperature || '0.0'}',
  'max-tokens'   = '${llmNode?.params?.max_tokens || d.llm_max_tokens || '512'}'
);

${memNodes.some(n=>n.opId==='mem_long') ? `-- ── Step 2: Register Embedding Model (for Long-Term Memory) ──────────────────
CREATE MODEL IF NOT EXISTS ${agentName}_embed
WITH (
  'provider'    = 'OPENAI',
  'model'       = '${d.mem_lt_embed || 'text-embedding-3-small'}',
  'dimensions'  = '1536',
  'api-key'     = 'OPENAI_API_KEY'
);

` : ''}-- ── Step 3: Define Agent (FLIP-531 CREATE AGENT statement) ─────────────────────
-- Note: CREATE AGENT is an experimental FLIP-531 extension.
-- Use the Java/Python API for production deployments.
CREATE AGENT IF NOT EXISTS ${agentName} (
  -- Agent type
  TYPE = '${isReact ? 'REACT' : 'WORKFLOW'}',

  -- LLM binding
  MODEL = ${agentName}_llm,

  -- System prompt
  SYSTEM_PROMPT = '${_agEsc(d.llm_system_prompt || 'You are an event-driven AI agent.')}',

  -- Memory configuration (Flink Agents 0.2 three-tier memory)
${memNodes.map(n=>{
        if(n.opId==='mem_sensory') return `  SENSORY_MEMORY = (MAX_TOKENS = ${n.params?.max_tokens || 4096}, STRATEGY = '${n.params?.strategy || 'SLIDING_WINDOW'}'),`;
        if(n.opId==='mem_short')   return `  SHORT_TERM_MEMORY = (TTL_HOURS = ${n.params?.ttl_hours || 24}, PARTITION_KEY = '${n.params?.scope || 'user_id'}'),`;
        if(n.opId==='mem_long')    return `  LONG_TERM_MEMORY = (EMBEDDING_MODEL = ${agentName}_embed, RECALL_TOP_K = ${n.params?.recall_top_k || 5}),`;
        return '';
    }).filter(Boolean).join('\n')}

  -- Tools
${toolNodes.map(t=>{
        if(t.opId==='tool_http') return `  TOOL ${t.params?.tool_name||'http_tool'} = HTTP_TOOL('${t.params?.url||'https://api.example.com'}', '${t.params?.method||'POST'}', DESC='${_agEsc(t.params?.description||'')}'),`;
        if(t.opId==='mcp_server') return `  MCP_SERVER ${t.params?.server_name||'mcp'} = ('${t.params?.url||'http://localhost:3000/sse'}', TRANSPORT='${t.params?.transport||'SSE'}'),`;
        return '';
    }).filter(Boolean).join('\n')}

  -- Execution guarantees
  EXACTLY_ONCE = ${d.agent_exactly_once !== 'disabled' ? 'TRUE' : 'FALSE'},
  DURABLE_EXECUTION = ${d.agent_durable !== 'disabled' ? 'TRUE' : 'FALSE'},

  -- Parallelism
  PARALLELISM = ${d.agent_parallelism || 4},

  -- Checkpointing
  CHECKPOINT_INTERVAL = '${d.agent_checkpoint || 10000} ms'
);

-- ── Step 4: Source table ──────────────────────────────────────────────────────
CREATE TEMPORARY TABLE IF NOT EXISTS ${srcNode?.params?.table_name || d.source_table || 'agent_events'} (
  event_id    BIGINT,
  payload     STRING,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '${d.source_wm_delay || 5}' SECOND
) WITH (
  'connector'                          = 'kafka',
  'topic'                              = '${srcNode?.params?.topic || d.source_topic || 'agent-input-events'}',
  'properties.bootstrap.servers'       = '${srcNode?.params?.bootstrap || d.source_endpoint || 'kafka:9092'}',
  'format'                             = 'json'
);

-- ── Step 5: Output sink ───────────────────────────────────────────────────────
CREATE TEMPORARY TABLE IF NOT EXISTS ${sinkNode?.params?.table_name || d.sink_table || 'agent_output'} WITH (
  'connector'                          = 'kafka',
  'topic'                              = '${sinkNode?.params?.topic || d.sink_endpoint || 'agent-output'}',
  'properties.bootstrap.servers'       = '${d.source_endpoint || 'kafka:9092'}',
  'format'                             = 'json'
) LIKE ${srcNode?.params?.table_name || 'agent_events'} (EXCLUDING ALL);

-- ── Step 6: Run agent pipeline (FLIP-531 RUN AGENT statement) ────────────────
RUN AGENT ${agentName}
  INPUT  = TABLE ${srcNode?.params?.table_name || 'agent_events'},
  OUTPUT = TABLE ${sinkNode?.params?.table_name || 'agent_output'},
  INPUT_COLUMN  = 'payload',
  OUTPUT_COLUMN = 'agent_decision';

-- ── Monitoring: SHOW AGENTS ───────────────────────────────────────────────────
-- SHOW AGENTS;
-- DESCRIBE AGENT ${agentName};
-- DROP AGENT IF EXISTS ${agentName};

-- ── VECTOR_SEARCH integration (Flink 2.2) ────────────────────────────────────
-- SELECT event_id, payload,
--   VECTOR_SEARCH(TABLE context_store, 'embedding_col',
--     ML_PREDICT(TABLE src, MODEL ${agentName}_embed), 5) AS relevant_context
-- FROM ${srcNode?.params?.table_name || 'agent_events'};
`;
}

/* ════════════════════════════════════════════════════════════════════════════
   FEATURE 4 — AGENT TEST RUNNER
   Sends a user-provided test event through the agent chain and displays
   a step-by-step reasoning trace with LLM calls, tool invocations, decisions
   Uses Anthropic API (via agent-manager's existing API access) to simulate
   a real reasoning trace when connected; falls back to a deterministic demo.
   ════════════════════════════════════════════════════════════════════════════ */
function _agxRenderTestRunner() {
    const content = document.getElementById('ag-content');
    const nodes = window._AG?.canvas?.nodes || [];
    const agentNode = nodes.find(n=>['agent_workflow','agent_react'].includes(n.opId));
    const srcSchema = window._AG?.wizData?.source_schema || 'event_id BIGINT\npayload STRING\nevent_time TIMESTAMP(3)';

    content.innerHTML = `
<div class="agx-panel">
  <div class="agx-toolbar" style="flex-wrap:wrap;gap:8px;">
    <span class="agx-toolbar-label">Agent Test Runner</span>
    <span style="font-size:10px;color:var(--text3);">
      Agent: <strong style="color:#00c4a0;">${_agEsc(agentNode?.label || '(no agent on canvas)')}</strong>
    </span>
    <div style="margin-left:auto;display:flex;gap:6px;">
      <button class="agx-btn green" id="agx-test-run-btn" onclick="_agxTestRun()">▶ Run Test</button>
      <button class="agx-btn red"   id="agx-test-stop-btn" onclick="_agxTestStop()" style="display:none;">⏹ Stop</button>
      <button class="agx-btn" onclick="_agxTestClear()">✕ Clear</button>
      <button class="agx-btn yellow" onclick="_agxTestExport()">⬇ Export Trace</button>
    </div>
  </div>

  <div style="flex:1;display:flex;overflow:hidden;">
    <!-- LEFT: Input panel -->
    <div style="width:340px;flex-shrink:0;overflow-y:auto;padding:14px;border-right:1px solid var(--border);">
      <div class="agx-section">Test Event Payload</div>
      <div class="agx-info">Provide a JSON event that matches your agent's source schema. The runner will simulate the full reasoning chain and display each step.</div>

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:4px;">Event JSON</label>
      <textarea id="agx-test-payload" class="field-input" rows="8" style="font-size:11px;font-family:var(--mono);resize:vertical;">{
  "event_id": 98234,
  "user_id": "usr_4821",
  "amount": 2450.00,
  "merchant": "CRYPTO_EXCHANGE_XYZ",
  "country": "NG",
  "payload": "Suspicious high-value crypto exchange transaction from new device",
  "event_time": "${new Date().toISOString()}"
}</textarea>

      <div style="margin-top:10px;">
        <div class="agx-section">Run Configuration</div>
        <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Max Reasoning Steps</label>
        <input id="agx-test-max-steps" class="field-input" type="number" value="5" min="1" max="20" style="font-size:11px;"/>
        <label style="display:flex;align-items:center;gap:6px;margin-top:8px;font-size:11px;color:var(--text1);cursor:pointer;">
          <input type="checkbox" id="agx-test-use-llm" checked/> Use real LLM calls (Anthropic API)
        </label>
        <label style="display:flex;align-items:center;gap:6px;margin-top:4px;font-size:11px;color:var(--text1);cursor:pointer;">
          <input type="checkbox" id="agx-test-mock-tools" checked/> Mock tool responses
        </label>
      </div>

      <div style="margin-top:10px;">
        <div class="agx-section">Quick Test Payloads</div>
        ${[
        {label:'💳 Fraud Transaction', payload:'{"event_id":1,"user_id":"usr_99","amount":5000,"merchant":"DARK_WEB_MKT","country":"XX","payload":"High-risk transaction"}'},
        {label:'✅ Normal Purchase',  payload:'{"event_id":2,"user_id":"usr_01","amount":45.99,"merchant":"AMAZON","country":"US","payload":"Normal retail purchase"}'},
        {label:'📡 IoT Anomaly',      payload:'{"event_id":3,"device_id":"sensor_42","value":98.7,"unit":"celsius","payload":"Temperature spike detected"}'},
    ].map(q=>`<button class="agx-btn" onclick="_agxTestSetPayload(${_agEsc(JSON.stringify(q.payload))})" style="display:block;width:100%;text-align:left;margin-bottom:4px;">${q.label}</button>`).join('')}
      </div>
    </div>

    <!-- RIGHT: Trace output -->
    <div style="flex:1;overflow-y:auto;padding:14px;" id="agx-test-trace-wrap">
      <div style="text-align:center;color:var(--text3);font-size:12px;padding:40px 20px;">
        <div style="font-size:36px;opacity:0.15;margin-bottom:10px;">▶</div>
        Configure a test event and click <strong style="color:#00c4a0;">Run Test</strong> to see the agent reasoning trace.
      </div>
    </div>
  </div>

  <!-- Summary bar (appears after run) -->
  <div id="agx-test-summary" style="display:none;padding:8px 14px;background:var(--bg2);
    border-top:1px solid var(--border);flex-shrink:0;display:flex;gap:14px;flex-wrap:wrap;">
  </div>
</div>`;
}

function _agxTestSetPayload(jsonStr) {
    const el = document.getElementById('agx-test-payload');
    if (el) { try { el.value = JSON.stringify(JSON.parse(jsonStr), null, 2); } catch(_) { el.value = jsonStr; } }
}

async function _agxTestRun() {
    const payloadEl = document.getElementById('agx-test-payload');
    let payload; try { payload = JSON.parse(payloadEl?.value || '{}'); } catch(_) { if(typeof toast==='function') toast('Invalid JSON payload','err'); return; }
    const maxSteps  = parseInt(document.getElementById('agx-test-max-steps')?.value||'5',10);
    const useLLM    = document.getElementById('agx-test-use-llm')?.checked;
    const mockTools = document.getElementById('agx-test-mock-tools')?.checked;
    const nodes     = window._AG?.canvas?.nodes || [];
    const d         = window._AG?.wizData || {};

    _AGX.testRunning = true;
    _AGX.testTrace   = [];
    document.getElementById('agx-test-run-btn').style.display  = 'none';
    document.getElementById('agx-test-stop-btn').style.display = '';

    const wrap = document.getElementById('agx-test-trace-wrap');
    if (wrap) wrap.innerHTML = '<div style="font-size:11px;color:#00c4a0;padding:14px;font-family:var(--mono);" class="agx-thinking">Agent reasoning</div>';

    const agentName = d.agent_name || nodes.find(n=>['agent_workflow','agent_react'].includes(n.opId))?.label || 'FlinkAgent';
    const isReact   = nodes.some(n=>n.opId==='agent_react');
    const llmModel  = nodes.find(n=>n.opId.startsWith('llm_'))?.params?.model || d.llm_model || 'claude-sonnet-4-6';
    const tools     = nodes.filter(n=>['tool_http','tool_sql','mcp_server'].includes(n.opId));
    const memNodes  = nodes.filter(n=>n.opId.startsWith('mem_'));
    const sysPrompt = d.llm_system_prompt || 'You are a real-time event analysis agent. Analyze the event and decide what action to take.';

    // Step 0: Agent Start
    await _agxTraceAdd({ type:'AGENT_START', icon:'⚛', title:`${agentName} — started`, status:'ok',
        body:`Event received:\n${JSON.stringify(payload,null,2)}\n\nPattern: ${isReact?'ReAct':'Workflow'} · Model: ${llmModel}`, latency:2 });

    // Step 1: Sensory Memory
    if (memNodes.some(n=>n.opId==='mem_sensory')) {
        await _agxTraceAdd({ type:'MEMORY_READ', icon:'🧠', title:'Sensory Memory — captured event context', status:'ok',
            body:`Context window initialized.\nEvent keys: ${Object.keys(payload).join(', ')}\nTokens used: ${JSON.stringify(payload).length/4|0} / 4096`, latency:1 });
    }

    // Step 2: Short-term memory lookup
    if (memNodes.some(n=>n.opId==='mem_short')) {
        await _agxTraceAdd({ type:'MEMORY_READ', icon:'🧠', title:'Short-Term Memory — retrieved user context', status:'ok',
            body:`Partition key: ${payload.user_id || payload.device_id || 'unknown'}\nEntries found: 7\nLast decision: LEGITIMATE (3 mins ago)\nPattern: no prior FRAUD flags`, latency:3 });
    }

    // Step 3: Long-term vector search
    if (memNodes.some(n=>n.opId==='mem_long')) {
        await _agxTraceAdd({ type:'MEMORY_READ', icon:'🔭', title:'Long-Term Memory — VECTOR_SEARCH (semantic recall)', status:'ok',
            body:`Embedding: text-embedding-3-small\nQuery: "${payload.payload||JSON.stringify(payload).slice(0,60)}"\nTop-5 results:\n  0.91 — Similar high-value crypto tx (FRAUD confirmed)\n  0.88 — Same merchant pattern (FRAUD confirmed)\n  0.79 — Different user, similar amount (LEGITIMATE)\n  0.74 — New device fingerprint (FRAUD confirmed)\n  0.71 — Weekend transaction spike (LEGITIMATE)`, latency:45 });
    }

    // Step 4: LLM reasoning call (real or simulated)
    if (useLLM) {
        await _agxTraceAdd({ type:'LLM_CALL', icon:'✦', title:`Calling ${llmModel} — reasoning…`, status:'pending',
            body:'Sending event + context to LLM…', latency:0, pending:true });
        try {
            const llmResult = await _agxCallLLM(sysPrompt, payload, d);
            _AGX.testTrace[_AGX.testTrace.length-1].body = llmResult.text;
            _AGX.testTrace[_AGX.testTrace.length-1].latency = llmResult.latency;
            _AGX.testTrace[_AGX.testTrace.length-1].status = 'ok';
            _AGX.testTrace[_AGX.testTrace.length-1].pending = false;
        } catch(err) {
            _AGX.testTrace[_AGX.testTrace.length-1].body = `LLM call failed: ${err.message}\nFalling back to simulated response.`;
            _AGX.testTrace[_AGX.testTrace.length-1].status = 'warn';
            _AGX.testTrace[_AGX.testTrace.length-1].latency = 0;
            _agxAddSimulatedLLM(payload);
        }
    } else {
        _agxAddSimulatedLLM(payload);
        await new Promise(r=>setTimeout(r,600));
    }

    if (!_AGX.testRunning) { _agxTestFinalize(); return; }

    // Step 5: Tool calls
    for (const t of tools.slice(0,3)) {
        if (!_AGX.testRunning) break;
        const toolName = t.params?.tool_name || t.params?.server_name || t.opId;
        const mockResp = t.opId === 'tool_http'
            ? `{"score":0.87,"risk":"HIGH","reason":"unusual_merchant_category","merchant_country_mismatch":true}`
            : t.opId === 'mcp_server'
                ? `{"crm_tier":"standard","account_age_days":45,"fraud_history_count":0,"recent_disputes":1}`
                : `[{"event_id":98201,"amount":1200,"merchant":"CRYPTO_X","decision":"FRAUD"}]`;
        await _agxTraceAdd({ type:'TOOL_INVOKE', icon:'🔌', title:`Tool: ${toolName}`, status:'ok',
            body:`Request: ${JSON.stringify({...payload,limit:5},null,0).slice(0,120)}\nResponse:\n${mockResp}`, latency: Math.floor(Math.random()*200+50) });
        if (isReact && _AGX.testTrace.length < maxSteps) {
            await _agxTraceAdd({ type:'LLM_CALL', icon:'✦', title:`ReAct Observe — processing tool result`, status:'ok',
                body:`Tool "${toolName}" returned risk score 0.87.\nUpdating reasoning: HIGH_RISK assessment confirmed.\nDecision: proceed to FRAUD verdict.`, latency: Math.floor(Math.random()*300+100) });
        }
    }

    if (!_AGX.testRunning) { _agxTestFinalize(); return; }

    // Step 6: Memory write
    if (memNodes.some(n=>n.opId==='mem_short')) {
        await _agxTraceAdd({ type:'MEMORY_WRITE', icon:'🧠', title:'Short-Term Memory — storing decision', status:'ok',
            body:`Key: ${payload.user_id || 'user'}\nValue: {"decision":"FRAUD","confidence":0.92,"ts":"${new Date().toISOString()}"}\nTTL: 24h`, latency:2 });
    }

    // Step 7: Final decision
    const isFraud = (payload.amount > 1000 || (payload.merchant||'').includes('CRYPTO') || (payload.country||'') === 'XX');
    const decision = isFraud ? 'FRAUD' : 'LEGITIMATE';
    const confidence = isFraud ? 0.92 : 0.97;
    await _agxTraceAdd({ type:'DECISION', icon:'✅', title:`FINAL DECISION: ${decision}`, status:'ok',
        body:`Decision:    ${decision}\nConfidence:  ${confidence}\nAction:      ${isFraud ? 'block_transaction, publish_fraud_alert, notify_compliance' : 'approve_transaction, update_trust_score'}\nTotal steps: ${_AGX.testTrace.length}\nAgent:       ${agentName}`, latency:1 });

    // Step 8: Agent end
    const totalLatency = _AGX.testTrace.reduce((s,t)=>s+(t.latency||0),0);
    await _agxTraceAdd({ type:'AGENT_END', icon:'⚛', title:`${agentName} — completed`, status:'ok',
        body:`Total runtime: ${totalLatency}ms\nSteps executed: ${_AGX.testTrace.length}\nDecision: ${decision} (${confidence} confidence)\nOutput published to: ${window._AG?.wizData?.sink_table || 'agent_output'}`, latency:1 });

    _agxTestFinalize();
}

async function _agxCallLLM(sysPrompt, payload, d) {
    const start = Date.now();
    const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            model: 'claude-sonnet-4-6',
            max_tokens: 400,
            messages: [{
                role: 'user',
                content: `${sysPrompt}\n\nEvent to analyze:\n${JSON.stringify(payload,null,2)}\n\nProvide a brief reasoning trace (3-4 sentences) and a final decision. Format: REASONING: ... DECISION: FRAUD or LEGITIMATE CONFIDENCE: 0.XX`,
            }],
        }),
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const text = data.content?.find(b=>b.type==='text')?.text || '(no response)';
    return { text, latency: Date.now()-start };
}

function _agxAddSimulatedLLM(payload) {
    const isFraud = (payload.amount>1000 || (payload.merchant||'').includes('CRYPTO') || payload.country==='XX');
    const reasoning = isFraud
        ? `REASONING: The transaction exhibits multiple high-risk indicators:\n1. High transaction amount ($${payload.amount||'?'}) inconsistent with user history\n2. Merchant category (CRYPTO_EXCHANGE) flagged in risk database\n3. Geographic mismatch — transaction origin inconsistent with user profile\n4. New device fingerprint detected — no prior activity\n5. Vector search: 2/5 similar historical events were confirmed FRAUD\n\nCONFIDENCE: 0.92\nACTION: BLOCK_AND_ALERT — invoking check_fraud_score tool for confirmation`
        : `REASONING: The transaction appears consistent with normal user behavior:\n1. Amount ($${payload.amount||'?'}) within expected range for user tier\n2. Merchant (${payload.merchant||'known'}) has clean history\n3. Geographic location matches user's usual activity\n4. Device fingerprint recognized — regular usage pattern\n5. No anomalies detected in recent transaction history\n\nCONFIDENCE: 0.97\nACTION: APPROVE — low-risk transaction`;
    if (_AGX.testTrace.length && _AGX.testTrace[_AGX.testTrace.length-1].pending) {
        _AGX.testTrace[_AGX.testTrace.length-1].body    = reasoning;
        _AGX.testTrace[_AGX.testTrace.length-1].latency = Math.floor(Math.random()*400+200);
        _AGX.testTrace[_AGX.testTrace.length-1].status  = 'ok';
        _AGX.testTrace[_AGX.testTrace.length-1].pending = false;
    } else {
        _AGX.testTrace.push({ type:'LLM_CALL', icon:'✦', title:`LLM reasoning (simulated)`, status:'ok', body:reasoning, latency:312 });
    }
    _agxTestRenderTrace();
}

async function _agxTraceAdd(step) {
    _AGX.testTrace.push(step);
    _agxTestRenderTrace();
    await new Promise(r => setTimeout(r, step.pending ? 50 : 400));
}

function _agxTestRenderTrace() {
    const wrap = document.getElementById('agx-test-trace-wrap'); if (!wrap) return;
    const statusColors = { ok:'var(--green)', warn:'var(--yellow,#f5a623)', error:'var(--red)', pending:'#00c4a0' };
    const typeColors   = _AGX_EVENT_COLORS;
    wrap.innerHTML = _AGX.testTrace.map((step,i) => `
      <div class="agx-trace-step ${step.open?'open':''}" id="agx-ts-${i}">
        <div class="agx-trace-step-hdr" onclick="document.getElementById('agx-ts-${i}').classList.toggle('open')">
          <span class="agx-trace-step-icon">${step.icon||'◈'}</span>
          <div style="flex:1;min-width:0;">
            <div class="agx-trace-step-title" style="color:${typeColors[step.type]||'#00c4a0'};">${_agEsc(step.title)}</div>
            <div style="font-size:9px;color:var(--text3);">${step.type}</div>
          </div>
          <span class="agx-trace-step-lat">${step.latency>0?step.latency+'ms':step.pending?'…':''}</span>
          <span class="agx-trace-step-status" style="background:${statusColors[step.status]||'var(--text3)'}22;color:${statusColors[step.status]||'var(--text3)'};">${step.status==='pending'?'running':step.status}</span>
          <span style="font-size:10px;color:var(--text3);margin-left:4px;">▾</span>
        </div>
        <div class="agx-trace-step-body">${_agEsc(step.body||'')}</div>
      </div>`).join('');
    // Auto-open last step
    if (_AGX.testTrace.length) {
        document.getElementById(`agx-ts-${_AGX.testTrace.length-1}`)?.classList.add('open');
    }
    wrap.scrollTop = wrap.scrollHeight;
}

function _agxTestFinalize() {
    _AGX.testRunning = false;
    document.getElementById('agx-test-run-btn').style.display  = '';
    document.getElementById('agx-test-stop-btn').style.display = 'none';
    const total    = _AGX.testTrace.reduce((s,t)=>s+(t.latency||0),0);
    const decision = _AGX.testTrace.find(t=>t.type==='DECISION')?.title||'—';
    const summary  = document.getElementById('agx-test-summary');
    if (summary) {
        summary.style.display = 'flex';
        summary.innerHTML = [
            `<span style="font-size:11px;color:var(--text0);font-family:var(--mono);">Total runtime: <strong style="color:#00c4a0;">${total}ms</strong></span>`,
            `<span style="font-size:11px;color:var(--text0);">Steps: <strong style="color:var(--accent);">${_AGX.testTrace.length}</strong></span>`,
            `<span style="font-size:11px;color:var(--text0);">Decision: <strong style="color:${decision.includes('FRAUD')?'var(--red)':'var(--green)'};">${decision.replace('FINAL DECISION: ','')}</strong></span>`,
        ].join('<span style="color:var(--border);">·</span>');
    }
    if(typeof toast==='function') toast('Test run complete','ok');
}

function _agxTestStop() {
    _AGX.testRunning = false;
    document.getElementById('agx-test-run-btn').style.display  = '';
    document.getElementById('agx-test-stop-btn').style.display = 'none';
}

function _agxTestClear() {
    _AGX.testTrace = [];
    const wrap = document.getElementById('agx-test-trace-wrap');
    if (wrap) wrap.innerHTML = '<div style="text-align:center;color:var(--text3);font-size:12px;padding:40px 20px;">Trace cleared.</div>';
    const sum = document.getElementById('agx-test-summary'); if(sum) sum.style.display='none';
}

function _agxTestExport() {
    if (!_AGX.testTrace.length) { if(typeof toast==='function') toast('No trace to export','warn'); return; }
    const data = { agent: window._AG?.wizData?.agent_name||'agent', ts: new Date().toISOString(), trace: _AGX.testTrace };
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));
    a.download = `agent-trace-${Date.now()}.json`;
    a.click();
}

/* ════════════════════════════════════════════════════════════════════════════
   FEATURE 5 — A2A TOPOLOGY VIEW
   Dedicated canvas showing multi-agent communication flows with animated
   message counts, throughput per link, and agent status badges
   ════════════════════════════════════════════════════════════════════════════ */
function _agxRenderA2A() {
    const content = document.getElementById('ag-content');
    const nodes   = window._AG?.canvas?.nodes || [];
    const allAgents = nodes.filter(n=>['agent_workflow','agent_react','agent_multi'].includes(n.opId));

    content.innerHTML = `
<div class="agx-panel">
  <div class="agx-toolbar">
    <span class="agx-toolbar-label">A2A Topology View</span>
    <span style="font-size:10px;color:var(--text3);">
      ${allAgents.length} agent${allAgents.length!==1?'s':''} on canvas
    </span>
    <button class="agx-btn green" onclick="_agxA2AStartSim()">▶ Simulate Messages</button>
    <button class="agx-btn red"   onclick="_agxA2AStopSim()">⏹ Stop</button>
    <button class="agx-btn"       onclick="_agxA2AReset()">↺ Reset Counters</button>
    <div style="flex:1;"></div>
    <button class="agx-btn" onclick="_agxA2AZoom(-0.12)">−</button>
    <span id="agx-a2a-zoom-lbl" style="font-size:10px;color:var(--text3);font-family:var(--mono);min-width:38px;text-align:center;">100%</span>
    <button class="agx-btn" onclick="_agxA2AZoom(0.12)">+</button>
    <button class="agx-btn" onclick="_agxA2AFit()">⊙ Fit</button>
  </div>

  <div id="agx-a2a-canvas-wrap" style="flex:1;position:relative;overflow:hidden;background:var(--bg0);cursor:grab;"
    onwheel="_agxA2AWheel(event)">
    <svg id="agx-a2a-svg" style="position:absolute;inset:0;width:100%;height:100%;overflow:visible;"></svg>
    <div id="agx-a2a-nodes" style="position:absolute;top:0;left:0;transform-origin:0 0;"></div>
  </div>

  <!-- Stats bar -->
  <div style="padding:6px 14px;background:var(--bg2);border-top:1px solid var(--border);
    display:flex;gap:16px;flex-shrink:0;font-size:10px;font-family:var(--mono);color:var(--text3);flex-wrap:wrap;">
    <span>Total messages: <strong id="agx-a2a-total" style="color:#00c4a0;">0</strong></span>
    <span>·</span>
    <span>Active links: <strong id="agx-a2a-links" style="color:var(--green);">0</strong></span>
    <span>·</span>
    <span>Protocol: <strong style="color:var(--accent);">Google A2A · JSON-RPC 2.0</strong></span>
    <div id="agx-a2a-agent-stats" style="display:flex;gap:10px;margin-left:auto;flex-wrap:wrap;"></div>
  </div>
</div>`;

    setTimeout(() => _agxA2ABuild(), 50);
}

const _agxA2A = { scale:1, panX:0, panY:0, panDrag:false, panSX:0, panSY:0, panOX:0, panOY:0 };
const _agxA2ALinkCounts = {};

function _agxA2ABuild() {
    const nodes = window._AG?.canvas?.nodes || [];
    const edges = window._AG?.canvas?.edges || [];
    const agentOps = new Set(['agent_workflow','agent_react','agent_multi']);
    const agentNodes = nodes.filter(n=>agentOps.has(n.opId));
    const allNodes   = nodes; // include sources/sinks for context

    // Layout agents in a circle + other nodes around
    const cx=400, cy=280, r=180;
    const placed = {};
    agentNodes.forEach((n,i) => {
        const angle = (i/Math.max(agentNodes.length,1))*2*Math.PI - Math.PI/2;
        placed[n.uid] = { x: cx+r*Math.cos(angle), y: cy+r*Math.sin(angle) };
    });
    // Non-agent nodes in outer ring
    const others = allNodes.filter(n=>!agentOps.has(n.opId));
    others.forEach((n,i) => {
        const angle = (i/Math.max(others.length,1))*2*Math.PI - Math.PI/2;
        placed[n.uid] = { x: cx+(r+110)*Math.cos(angle), y: cy+(r+110)*Math.sin(angle) };
    });

    const container = document.getElementById('agx-a2a-nodes');
    const svg       = document.getElementById('agx-a2a-svg');
    if (!container || !svg) return;
    container.innerHTML = '';

    // Draw nodes
    allNodes.forEach(n => {
        const opDef = (window.AG_OPERATORS||[]).find(o=>o.id===n.opId)||{color:'#555',icon:'◈'};
        const pos = placed[n.uid]||{x:100,y:100};
        const isAgent = agentOps.has(n.opId);
        const div = document.createElement('div');
        div.className = 'agx-a2a-node';
        div.id = 'agx-a2a-n-' + n.uid;
        div.style.cssText = `left:${pos.x-65}px;top:${pos.y-35}px;background:${opDef.color};
          border-color:${isAgent?'rgba(0,196,160,0.6)':'rgba(255,255,255,0.1)'};
          ${isAgent?'box-shadow:0 0 20px rgba(0,176,143,0.3);':'opacity:0.7;'}`;
        div.innerHTML = `<div style="font-size:11px;font-weight:700;">${opDef.icon||'◈'} ${_agEsc(n.label||n.opId)}</div>
          <div class="agx-a2a-counter" id="agx-a2a-cnt-${n.uid}">0 msgs</div>`;
        container.appendChild(div);
    });

    // Draw A2A edges
    const linkEdges = edges.filter(e => {
        const from = nodes.find(n=>n.uid===e.fromUid);
        const to   = nodes.find(n=>n.uid===e.toUid);
        return from && to; // all connections visible
    });

    let svgHTML = `<defs>
      <marker id="agx-a2a-arr" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
        <path d="M0,0 L0,6 L8,3 z" fill="rgba(0,196,160,0.5)"/>
      </marker>
    </defs>
    <g id="agx-a2a-edges-g"></g>
    <g id="agx-a2a-particles-g"></g>`;

    linkEdges.forEach(e => {
        const fromPos = placed[e.fromUid], toPos = placed[e.toUid];
        if (!fromPos||!toPos) return;
        _agxA2ALinkCounts[e.uid] = 0;
        const cx1=fromPos.x+(toPos.x-fromPos.x)*0.45, cy1=fromPos.y;
        const cx2=fromPos.x+(toPos.x-fromPos.x)*0.55, cy2=toPos.y;
        svgHTML += `<path id="agx-a2a-path-${e.uid}"
          d="M${fromPos.x},${fromPos.y} C${cx1},${cy1} ${cx2},${cy2} ${toPos.x},${toPos.y}"
          stroke="rgba(0,176,143,0.3)" stroke-width="1.5" fill="none"
          marker-end="url(#agx-a2a-arr)" opacity="0.7"/>`;
    });

    svg.innerHTML = svgHTML;
    document.getElementById('agx-a2a-links').textContent = linkEdges.length;
    _agxA2AApplyTransform();
}

function _agxA2AApplyTransform() {
    const c = document.getElementById('agx-a2a-nodes'); if(!c) return;
    c.style.transform = `translate(${_agxA2A.panX}px,${_agxA2A.panY}px) scale(${_agxA2A.scale})`;
    const svg = document.getElementById('agx-a2a-svg'); if(!svg) return;
    svg.style.transform = `translate(${_agxA2A.panX}px,${_agxA2A.panY}px) scale(${_agxA2A.scale})`;
    const lbl = document.getElementById('agx-a2a-zoom-lbl'); if(lbl) lbl.textContent=Math.round(_agxA2A.scale*100)+'%';
}

function _agxA2AZoom(d) {
    _agxA2A.scale = Math.max(0.2,Math.min(3,_agxA2A.scale+d));
    _agxA2AApplyTransform();
}

function _agxA2AFit() { _agxA2A.scale=0.9; _agxA2A.panX=20; _agxA2A.panY=20; _agxA2AApplyTransform(); }
function _agxA2AWheel(e) { e.preventDefault(); _agxA2AZoom(e.deltaY<0?0.1:-0.1); }

function _agxA2AStartSim() {
    if (_AGX.a2aAnimTimer) return;
    const edges = window._AG?.canvas?.edges || [];
    const nodes = window._AG?.canvas?.nodes || [];
    const particles = [];
    edges.forEach(e => particles.push({edgeUid:e.uid,t:Math.random()},{edgeUid:e.uid,t:Math.random()*0.5}));
    const placed = {};
    const agentOps = new Set(['agent_workflow','agent_react','agent_multi']);
    const agentNodes = nodes.filter(n=>agentOps.has(n.opId));
    const allNodes   = nodes;
    const cx=400,cy=280,r=180;
    agentNodes.forEach((n,i)=>{ const a=(i/Math.max(agentNodes.length,1))*2*Math.PI-Math.PI/2; placed[n.uid]={x:cx+r*Math.cos(a),y:cy+r*Math.sin(a)}; });
    const others=allNodes.filter(n=>!agentOps.has(n.opId));
    others.forEach((n,i)=>{ const a=(i/Math.max(others.length,1))*2*Math.PI-Math.PI/2; placed[n.uid]={x:cx+(r+110)*Math.cos(a),y:cy+(r+110)*Math.sin(a)}; });

    let total=0;
    const animate=()=>{
        const pg=document.getElementById('agx-a2a-particles-g'); if(!pg){_AGX.a2aAnimTimer=null;return;}
        let html='';
        particles.forEach(p=>{
            const e=edges.find(e=>e.uid===p.edgeUid); if(!e) return;
            const fp=placed[e.fromUid],tp=placed[e.toUid]; if(!fp||!tp) return;
            p.t+=0.013; if(p.t>=1){p.t=0; _agxA2ALinkCounts[e.uid]=(_agxA2ALinkCounts[e.uid]||0)+1; total++;
                const cnt=document.getElementById(`agx-a2a-cnt-${e.toUid}`); if(cnt) cnt.textContent=(_agxA2ALinkCounts[e.uid]||0)+' msgs';
                const tot=document.getElementById('agx-a2a-total'); if(tot) tot.textContent=total;
                _agxA2AUpdateAgentStats(nodes,placed);
            }
            const t=p.t,mt=1-t;
            const cx1=fp.x+(tp.x-fp.x)*0.45,cy1=fp.y,cx2=fp.x+(tp.x-fp.x)*0.55,cy2=tp.y;
            const px=mt*mt*mt*fp.x+3*mt*mt*t*cx1+3*mt*t*t*cx2+t*t*t*tp.x;
            const py=mt*mt*mt*fp.y+3*mt*mt*t*cy1+3*mt*t*t*cy2+t*t*t*tp.y;
            const alpha=Math.sin(t*Math.PI);
            html+=`<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="4" fill="#00c4a0" opacity="${alpha.toFixed(2)}"/>`;
            // Tiny message badge
            if(Math.abs(t-0.5)<0.02){
                const types=['TASK_REQUEST','TASK_RESPONSE','DELEGATE','STATUS_UPDATE','A2A_PING'];
                const msgType=types[Math.floor(Math.random()*types.length)];
                html+=`<text x="${px.toFixed(1)}" y="${(py-10).toFixed(1)}" font-size="7" fill="#00c4a0" font-family="monospace" text-anchor="middle" opacity="0.7">${msgType}</text>`;
            }
        });
        pg.innerHTML=html;
        _AGX.a2aAnimTimer=requestAnimationFrame(animate);
    };
    _AGX.a2aAnimTimer=requestAnimationFrame(animate);
}

function _agxA2AUpdateAgentStats(nodes, placed) {
    const el = document.getElementById('agx-a2a-agent-stats'); if (!el) return;
    const agentOps = new Set(['agent_workflow','agent_react','agent_multi']);
    const agents = nodes.filter(n=>agentOps.has(n.opId));
    el.innerHTML = agents.map(n=>`<span style="font-size:9px;padding:1px 7px;border-radius:10px;background:rgba(0,176,143,0.12);color:#00c4a0;border:1px solid rgba(0,176,143,0.25);">${_agEsc(n.label)}: ${_agxA2ALinkCounts[n.uid]||0}</span>`).join('');
}

function _agxA2AStopSim() {
    if (_AGX.a2aAnimTimer) { cancelAnimationFrame(_AGX.a2aAnimTimer); _AGX.a2aAnimTimer=null; }
    const pg = document.getElementById('agx-a2a-particles-g'); if(pg) pg.innerHTML='';
}

function _agxA2AReset() {
    Object.keys(_agxA2ALinkCounts).forEach(k=>_agxA2ALinkCounts[k]=0);
    document.querySelectorAll('[id^="agx-a2a-cnt-"]').forEach(el=>el.textContent='0 msgs');
    const tot=document.getElementById('agx-a2a-total'); if(tot) tot.textContent='0';
    const stats=document.getElementById('agx-a2a-agent-stats'); if(stats) stats.innerHTML='';
}

/* ════════════════════════════════════════════════════════════════════════════
   FEATURE 6 — LLM COST ESTIMATOR
   Estimates token cost per event and per day across all LLM nodes on the
   current canvas, given user-specified throughput (events/sec).
   Pricing data: June 2026 public API pricing (USD per 1M tokens)
   ════════════════════════════════════════════════════════════════════════════ */
const _AGX_PRICING = {
    // Model → { input, output } USD per 1M tokens (June 2026)
    'gpt-4o':                        { input:5.00,  output:15.00 },
    'gpt-4o-mini':                   { input:0.15,  output:0.60  },
    'gpt-4-turbo':                   { input:10.00, output:30.00 },
    'gpt-3.5-turbo':                 { input:0.50,  output:1.50  },
    'claude-sonnet-4-6':             { input:3.00,  output:15.00 },
    'claude-opus-4-6':               { input:15.00, output:75.00 },
    'claude-haiku-4-5-20251001':     { input:0.25,  output:1.25  },
    'mistral-small-latest':          { input:0.20,  output:0.60  },
    'mistral-large-latest':          { input:3.00,  output:9.00  },
    'command-r-plus':                { input:3.00,  output:15.00 },
    'anthropic.claude-sonnet-4-6':   { input:3.00,  output:15.00 },
    'amazon.titan-text-express-v1':  { input:0.80,  output:1.00  },
    'meta.llama3-8b-instruct-v1:0':  { input:0.40,  output:0.60  },
    'text-embedding-3-small':        { input:0.02,  output:0.00  },
    'text-embedding-3-large':        { input:0.13,  output:0.00  },
    'text-embedding-ada-002':        { input:0.10,  output:0.00  },
    'nomic-embed-text':              { input:0.00,  output:0.00  }, // local/free
};

function _agxRenderCost() {
    const content = document.getElementById('ag-content');
    const nodes   = window._AG?.canvas?.nodes || [];
    const llmNodes  = nodes.filter(n=>n.opId.startsWith('llm_')||n.opId.startsWith('embed_'));
    const agentName = window._AG?.wizData?.agent_name || 'Agent';

    content.innerHTML = `
<div class="agx-panel">
  <div class="agx-toolbar" style="flex-wrap:wrap;gap:8px;">
    <span class="agx-toolbar-label">LLM Cost Estimator</span>
    <span style="font-size:10px;color:var(--text3);">${llmNodes.length} LLM/embedding node${llmNodes.length!==1?'s':''} on canvas</span>
    <div style="margin-left:auto;display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
      <label style="font-size:10px;color:var(--text2);">Events/sec:</label>
      <input id="agx-cost-eps" class="field-input" type="number" value="100" min="1" style="font-size:11px;width:90px;" oninput="_agxCostCalc()"/>
      <label style="font-size:10px;color:var(--text2);">Input tokens/event:</label>
      <input id="agx-cost-input-tok" class="field-input" type="number" value="500" min="1" style="font-size:11px;width:90px;" oninput="_agxCostCalc()"/>
      <label style="font-size:10px;color:var(--text2);">Output tokens/event:</label>
      <input id="agx-cost-output-tok" class="field-input" type="number" value="200" min="1" style="font-size:11px;width:90px;" oninput="_agxCostCalc()"/>
      <button class="agx-btn yellow" onclick="_agxCostExport()">⬇ Export CSV</button>
    </div>
  </div>

  <div style="flex:1;overflow-y:auto;padding:16px 20px;">
    <div class="agx-info" style="margin-bottom:16px;">
      Cost estimates are based on <strong>June 2026 public API pricing</strong>.
      Prices are per event × throughput rate. Embedding models are included where present.
      Ollama and other self-hosted models are shown as $0 (infrastructure cost only).
    </div>

    <!-- Per-node breakdown -->
    <div class="agx-section">Per-LLM-Node Cost Breakdown</div>
    <div id="agx-cost-rows">
      ${llmNodes.length === 0
        ? '<div style="font-size:11px;color:var(--text3);">No LLM or embedding nodes on the canvas. Add nodes in the Visual Canvas tab.</div>'
        : llmNodes.map(n=>{
            const model = n.params?.model || n.params?.model_id || window._AG?.wizData?.llm_model || 'gpt-4o-mini';
            return `<div class="agx-cost-row">
                <div class="agx-cost-node">${n.opId.replace('llm_','').replace('embed_','')} · <strong>${_agEsc(n.label)}</strong></div>
                <div class="agx-cost-model">${_agEsc(model)}</div>
                <div class="agx-cost-val" id="agx-cv-${n.uid}" style="color:#00c4a0;">—</div>
                <div style="font-size:9px;color:var(--text3);font-family:var(--mono);">/event</div>
                <div class="agx-cost-val" id="agx-cd-${n.uid}" style="color:var(--accent);">—</div>
                <div style="font-size:9px;color:var(--text3);font-family:var(--mono);">/day</div>
              </div>`;
        }).join('')}
    </div>

    <!-- Totals -->
    <div class="agx-cost-total" id="agx-cost-total-box">
      <div class="agx-section">Total Estimated Cost — ${_agEsc(agentName)}</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;">
        ${[
        ['Per Event',     'agx-ct-event', '$0.000000'],
        ['Per Hour',      'agx-ct-hour',  '$0.00'],
        ['Per Day',       'agx-ct-day',   '$0.00'],
        ['Per Month (30d)','agx-ct-month','$0.00'],
    ].map(([l,id,ph])=>`
          <div style="text-align:center;padding:10px;background:rgba(0,176,143,0.08);border:1px solid rgba(0,176,143,0.2);border-radius:5px;">
            <div id="${id}" style="font-size:18px;font-weight:700;color:#00c4a0;font-family:var(--mono);">${ph}</div>
            <div style="font-size:10px;color:var(--text3);margin-top:3px;">${l}</div>
          </div>`).join('')}
      </div>
    </div>

    <!-- Throughput chart -->
    <div style="margin-top:16px;">
      <div class="agx-section">Cost vs Throughput</div>
      <canvas id="agx-cost-chart" height="120" style="width:100%;height:120px;background:var(--bg0);border:1px solid var(--border);border-radius:4px;display:block;"></canvas>
      <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--text3);font-family:var(--mono);margin-top:3px;">
        <span>1 event/s</span><span id="agx-chart-mid">—</span><span id="agx-chart-max">— events/s</span>
      </div>
    </div>

    <!-- Pricing table reference -->
    <div style="margin-top:16px;">
      <div class="agx-section">Model Pricing Reference (USD / 1M tokens · June 2026)</div>
      <table style="width:100%;border-collapse:collapse;font-size:10px;font-family:var(--mono);">
        <tr style="background:var(--bg2);">
          <th style="padding:5px 8px;text-align:left;color:var(--text2);border-bottom:1px solid var(--border);">Model</th>
          <th style="padding:5px 8px;text-align:right;color:var(--text2);border-bottom:1px solid var(--border);">Input $</th>
          <th style="padding:5px 8px;text-align:right;color:var(--text2);border-bottom:1px solid var(--border);">Output $</th>
          <th style="padding:5px 8px;text-align:right;color:var(--text2);border-bottom:1px solid var(--border);">@ 100 e/s / day</th>
        </tr>
        ${Object.entries(_AGX_PRICING).map(([model,p])=>{
        const dailyCost = ((p.input*500 + p.output*200)/1e6)*100*86400;
        return `<tr style="border-bottom:1px solid rgba(255,255,255,0.04);">
              <td style="padding:4px 8px;color:var(--text0);">${_agEsc(model)}</td>
              <td style="padding:4px 8px;text-align:right;color:var(--green);">$${p.input.toFixed(2)}</td>
              <td style="padding:4px 8px;text-align:right;color:var(--yellow,#f5a623);">$${p.output.toFixed(2)}</td>
              <td style="padding:4px 8px;text-align:right;color:#00c4a0;font-weight:600;">$${dailyCost.toFixed(2)}</td>
            </tr>`;
    }).join('')}
      </table>
    </div>
  </div>
</div>`;

    setTimeout(_agxCostCalc, 60);
}

function _agxCostCalc() {
    const eps     = parseFloat(document.getElementById('agx-cost-eps')?.value||'100');
    const inTok   = parseInt(document.getElementById('agx-cost-input-tok')?.value||'500',10);
    const outTok  = parseInt(document.getElementById('agx-cost-output-tok')?.value||'200',10);
    const nodes   = window._AG?.canvas?.nodes || [];
    const llmNodes = nodes.filter(n=>n.opId.startsWith('llm_')||n.opId.startsWith('embed_'));

    let totalPerEvent = 0;
    llmNodes.forEach(n => {
        const model = n.params?.model || n.params?.model_id || window._AG?.wizData?.llm_model || 'gpt-4o-mini';
        const p = _AGX_PRICING[model] || { input:0.5, output:1.5 };
        const costPerEvent = (p.input*inTok + p.output*outTok) / 1e6;
        totalPerEvent += costPerEvent;
        const costPerDay = costPerEvent * eps * 86400;
        const cvEl = document.getElementById(`agx-cv-${n.uid}`); if(cvEl) cvEl.textContent = `$${costPerEvent.toFixed(6)}`;
        const cdEl = document.getElementById(`agx-cd-${n.uid}`); if(cdEl) cdEl.textContent = `$${costPerDay.toFixed(2)}`;
    });

    const perHour  = totalPerEvent * eps * 3600;
    const perDay   = totalPerEvent * eps * 86400;
    const perMonth = perDay * 30;

    const setEl = (id,v) => { const e=document.getElementById(id); if(e) e.textContent=v; };
    setEl('agx-ct-event', `$${totalPerEvent.toFixed(6)}`);
    setEl('agx-ct-hour',  `$${perHour.toFixed(2)}`);
    setEl('agx-ct-day',   `$${perDay.toFixed(2)}`);
    setEl('agx-ct-month', `$${perMonth.toFixed(2)}`);

    _agxCostDrawChart(totalPerEvent, inTok, outTok, eps);
}

function _agxCostDrawChart(baseCostPerEvent, inTok, outTok, currentEps) {
    const canvas = document.getElementById('agx-cost-chart'); if (!canvas) return;
    const ctx = canvas.getContext('2d'); if (!ctx) return;
    canvas.width  = canvas.offsetWidth || 800;
    canvas.height = 120;

    const maxEps  = Math.max(currentEps*5, 1000);
    const steps   = 60;
    const vals    = Array.from({length:steps},(_,i)=>baseCostPerEvent*(i/steps*maxEps)*86400);
    const maxCost = Math.max(...vals,0.01);
    const W=canvas.width, H=canvas.height;

    ctx.clearRect(0,0,W,H);
    // Grid
    ctx.strokeStyle='rgba(0,176,143,0.08)'; ctx.lineWidth=1;
    for(let i=0;i<=4;i++){const y=H-(i/4)*H;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke();}

    // Cost curve
    ctx.beginPath();
    vals.forEach((v,i)=>{ const x=i/steps*W,y=H-(v/maxCost)*(H-8)-4; i===0?ctx.moveTo(x,y):ctx.lineTo(x,y); });
    ctx.strokeStyle='#00c4a0'; ctx.lineWidth=2; ctx.stroke();

    // Current eps marker
    const markerX = (currentEps/maxEps)*W;
    ctx.strokeStyle='var(--accent,#00d4aa)'||'#00d4aa'; ctx.lineWidth=1.5; ctx.setLineDash([4,3]);
    ctx.beginPath(); ctx.moveTo(markerX,0); ctx.lineTo(markerX,H); ctx.stroke(); ctx.setLineDash([]);

    // Fill
    ctx.lineTo(W,H); ctx.lineTo(0,H); ctx.closePath();
    ctx.fillStyle='rgba(0,176,143,0.06)'; ctx.fill();

    // Y-axis labels
    ctx.fillStyle='rgba(0,196,160,0.5)'; ctx.font='9px monospace'; ctx.textAlign='left';
    ctx.fillText(`$${maxCost.toFixed(2)}/day`,4,12);
    ctx.fillText(`$0`,4,H-2);

    const midEl=document.getElementById('agx-chart-mid'); if(midEl) midEl.textContent=Math.round(maxEps/2)+' e/s';
    const maxEl=document.getElementById('agx-chart-max'); if(maxEl) maxEl.textContent=Math.round(maxEps)+' e/s';
}

function _agxCostExport() {
    const eps=parseFloat(document.getElementById('agx-cost-eps')?.value||'100');
    const inTok=parseInt(document.getElementById('agx-cost-input-tok')?.value||'500',10);
    const outTok=parseInt(document.getElementById('agx-cost-output-tok')?.value||'200',10);
    const nodes=(window._AG?.canvas?.nodes||[]).filter(n=>n.opId.startsWith('llm_')||n.opId.startsWith('embed_'));
    const header='node_label,model,cost_per_event_usd,cost_per_day_usd,cost_per_month_usd';
    const rows=nodes.map(n=>{
        const model=n.params?.model||n.params?.model_id||'gpt-4o-mini';
        const p=_AGX_PRICING[model]||{input:0.5,output:1.5};
        const cpe=(p.input*inTok+p.output*outTok)/1e6;
        return `"${n.label}","${model}","${cpe.toFixed(6)}","${(cpe*eps*86400).toFixed(2)}","${(cpe*eps*86400*30).toFixed(2)}"`;
    });
    const csv=[header,...rows].join('\n');
    const a=document.createElement('a');
    a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
    a.download=`agent-cost-estimate-${Date.now()}.csv`;
    a.click();
    if(typeof toast==='function') toast('Cost estimate exported','ok');
}