/**
 * schema-registry  —  Str:::lab Studio v0.0.22
 * ─────────────────────────────────────────────────────────────────
 * Feature : Live Schema Registry Browser & Evolution Validator
 *
 * Connects to Confluent Schema Registry or Apicurio REST API.
 * Provides:
 *   • Browse all subjects and their versions
 *   • View schema JSON/Avro/Protobuf with syntax highlighting
 *   • Compare two schema versions — highlights breaking vs
 *     non-breaking field changes
 *   • Validate that a CREATE TABLE ... WITH avro-confluent
 *     definition is compatible with the registered schema
 *   • One-click "Generate CREATE TABLE" from a registered schema
 * ─────────────────────────────────────────────────────────────────
 */

// ── State ──────────────────────────────────────────────────────────
const _SR = {
    baseUrl     : '',
    authHeader  : '',
    subjects    : [],
    cache       : {},     // subject → [versions]
    schemaCache : {},     // subject:version → schema object
    selected    : null,   // { subject, version }
    compareWith : null,   // { subject, version }
    connected   : false,
};

// ── Entry point ────────────────────────────────────────────────────
function openSchemaRegistry() {
    if (!document.getElementById('sr-modal')) _srBuildModal();
    openModal('sr-modal');
    _srAutoDetect();
}

// ── Auto-detect from state
function _srAutoDetect() {
    // Try to pull Schema Registry URL from existing state/connection info
    if (typeof state !== 'undefined' && state.gateway) {
        const host = (state.gateway.host || 'localhost');
        const srUrl = `http://${host}:8081`;
        const inp = document.getElementById('sr-url-input');
        if (inp && !inp.value) inp.value = srUrl;
    }
}

// ── API calls
async function _srFetch(path) {
    const url = _SR.baseUrl.replace(/\/$/, '') + path;
    const headers = { 'Accept': 'application/vnd.schemaregistry.v1+json, application/json' };
    if (_SR.authHeader) headers['Authorization'] = _SR.authHeader;
    const res = await fetch(url, { headers });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text().catch(() => '')}`);
    return res.json();
}

// ── Connect
async function _srConnect() {
    const urlEl  = document.getElementById('sr-url-input');
    const userEl = document.getElementById('sr-user-input');
    const passEl = document.getElementById('sr-pass-input');
    if (!urlEl) return;

    _SR.baseUrl = (urlEl.value || '').trim().replace(/\/$/, '');
    if (!_SR.baseUrl) { _srStatus('Enter a Schema Registry URL', 'warn'); return; }

    if (userEl && userEl.value && passEl && passEl.value) {
        _SR.authHeader = 'Basic ' + btoa(userEl.value + ':' + passEl.value);
    } else {
        _SR.authHeader = '';
    }

    _srStatus('Connecting…', 'info');
    try {
        // Test connectivity
        const info = await _srFetch('/').catch(() => _srFetch('/v1'));
        _SR.connected = true;
        _srStatus('Connected', 'ok');

        // Show main panel
        _srShowPanel('browser');
        await _srLoadSubjects();
    } catch (e) {
        _srStatus('Connection failed: ' + e.message, 'err');
        _SR.connected = false;
    }
}

async function _srLoadSubjects() {
    _srSetSubjectList('<div style="padding:12px;font-size:11px;color:var(--text3);">Loading…</div>');
    try {
        _SR.subjects = await _srFetch('/subjects');
        _srRenderSubjectList(_SR.subjects);
    } catch (e) {
        _srSetSubjectList(`<div style="padding:12px;font-size:11px;color:var(--red);">Error: ${_escSr(e.message)}</div>`);
    }
}

async function _srSelectSubject(subject) {
    _SR.selected = { subject, version: null };
    document.querySelectorAll('.sr-subject-item').forEach(el =>
        el.classList.toggle('selected', el.dataset.subject === subject));

    _srShowDetail('Loading versions…');
    try {
        const versions = await _srFetch(`/subjects/${encodeURIComponent(subject)}/versions`);
        _SR.cache[subject] = versions;
        _srRenderVersionList(subject, versions);
    } catch (e) {
        _srShowDetail(`<div style="color:var(--red);font-size:11px;">Error: ${_escSr(e.message)}</div>`);
    }
}

async function _srSelectVersion(subject, version) {
    _SR.selected = { subject, version };
    document.querySelectorAll('.sr-version-btn').forEach(el =>
        el.classList.toggle('active', el.dataset.version == version));

    const cacheKey = subject + ':' + version;
    try {
        let schema = _SR.schemaCache[cacheKey];
        if (!schema) {
            schema = await _srFetch(`/subjects/${encodeURIComponent(subject)}/versions/${version}`);
            _SR.schemaCache[cacheKey] = schema;
        }
        _srRenderSchema(schema, subject, version);
    } catch (e) {
        _srShowDetail(`<div style="color:var(--red);font-size:11px;">Error: ${_escSr(e.message)}</div>`);
    }
}

// ── Schema diff
async function _srCompare(subject, v1, v2) {
    const k1 = subject + ':' + v1, k2 = subject + ':' + v2;
    const [s1, s2] = await Promise.all([
        _SR.schemaCache[k1] || _srFetch(`/subjects/${encodeURIComponent(subject)}/versions/${v1}`).then(s => { _SR.schemaCache[k1]=s; return s; }),
        _SR.schemaCache[k2] || _srFetch(`/subjects/${encodeURIComponent(subject)}/versions/${v2}`).then(s => { _SR.schemaCache[k2]=s; return s; }),
    ]);

    const diff = _srDiffSchemas(s1, s2);
    _srRenderDiff(subject, v1, v2, diff, s1, s2);
}

function _srDiffSchemas(s1, s2) {
    // Parse schema strings
    let a = {}, b = {};
    try { a = JSON.parse(s1.schema || '{}'); } catch (_) {}
    try { b = JSON.parse(s2.schema || '{}'); } catch (_) {}

    const fieldsA = _srExtractFields(a);
    const fieldsB = _srExtractFields(b);

    const added    = [];
    const removed  = [];
    const changed  = [];
    const breaking = [];

    // Removed fields — BREAKING if required (no default)
    fieldsA.forEach(fa => {
        const fb = fieldsB.find(f => f.name === fa.name);
        if (!fb) {
            removed.push(fa);
            if (!fa.hasDefault) breaking.push(`Removed required field "${fa.name}" (no default)`);
        } else if (fa.type !== fb.type) {
            changed.push({ name: fa.name, from: fa.type, to: fb.type });
            // Type changes that break consumers
            const safeWidening = [['int','long'],['float','double'],['null','string']];
            const isWidening = safeWidening.some(([f,t]) => fa.type===f && fb.type===t);
            if (!isWidening) breaking.push(`Type change "${fa.name}": ${fa.type} → ${fb.type}`);
        }
    });

    // Added fields — non-breaking if they have defaults
    fieldsB.forEach(fb => {
        if (!fieldsA.find(f => f.name === fb.name)) {
            added.push(fb);
            if (!fb.hasDefault) breaking.push(`Added required field "${fb.name}" without default — old consumers may fail`);
        }
    });

    // Schema type change
    if (a.type !== b.type && a.type && b.type) {
        breaking.push(`Schema type changed: "${a.type}" → "${b.type}"`);
    }

    return { added, removed, changed, breaking, compatible: breaking.length === 0 };
}

function _srExtractFields(schema) {
    const fields = [];
    const rawFields = schema.fields || (schema.type === 'record' ? [] : []);
    rawFields.forEach(f => {
        const type = Array.isArray(f.type) ? f.type.filter(t => t !== 'null').join('|') : String(f.type);
        const hasDefault = f.hasOwnProperty('default');
        fields.push({ name: f.name, type, hasDefault });
    });
    return fields;
}

// ── Generate CREATE TABLE from schema
function _srGenerateCreateTable(schemaObj, subject) {
    let parsed;
    try { parsed = JSON.parse(schemaObj.schema || '{}'); } catch (_) { parsed = {}; }

    const fields  = _srExtractFields(parsed);
    const tblName = (parsed.name || subject.replace(/-value|-key/, '')).replace(/[^a-zA-Z0-9_]/g, '_');
    const schemaId= schemaObj.id || '';

    const TYPE_MAP = {
        'string': 'STRING', 'int': 'INT', 'long': 'BIGINT',
        'float': 'FLOAT', 'double': 'DOUBLE', 'boolean': 'BOOLEAN',
        'bytes': 'BYTES', 'null': 'STRING',
    };

    const cols = fields.map(f => {
        const flinkType = TYPE_MAP[f.type.toLowerCase()] || 'STRING';
        return `    ${f.name.padEnd(24)} ${flinkType}`;
    });

    // Guess if there's a timestamp column for watermark
    const tsCol = fields.find(f => /time|ts|timestamp|created|updated/i.test(f.name) && /long|string/i.test(f.type));

    if (tsCol && tsCol.type === 'long') {
        cols.push(`    ${(tsCol.name + '_ts').padEnd(24)} AS TO_TIMESTAMP_LTZ(${tsCol.name}, 3)`);
        cols.push(`    WATERMARK FOR ${tsCol.name}_ts AS ${tsCol.name}_ts - INTERVAL '5' SECOND`);
    }

    const sql = [
        `-- Generated from Schema Registry`,
        `-- Subject: ${subject}  |  Schema ID: ${schemaId}`,
        `CREATE TABLE ${tblName} (`,
        cols.join(',\n'),
        `) WITH (`,
        `    'connector'                    = 'kafka',`,
        `    'topic'                        = '<REPLACE_ME:topic>',`,
        `    'properties.bootstrap.servers' = '<REPLACE_ME:bootstrap.servers>',`,
        `    'properties.group.id'          = '<REPLACE_ME:group_id>',`,
        `    'format'                       = 'avro-confluent',`,
        `    'avro-confluent.url'           = '${_SR.baseUrl}',`,
        `    'avro-confluent.schema-id'     = '${schemaId}'`,
        `);`,
    ].join('\n');

    return sql;
}

// ── Compatibility check against CREATE TABLE SQL
async function _srValidateCreateTable() {
    const ed = document.getElementById('sql-editor');
    if (!ed) return;
    const sql = ed.value || '';

    // Extract table schema columns from SQL
    const colRe = /CREATE\s+(?:TEMPORARY\s+)?TABLE\s+\S+\s*\(([\s\S]*?)\)\s*WITH/i;
    const m = colRe.exec(sql);
    if (!m) {
        _srShowValidation('No CREATE TABLE found in editor. Write your SQL first.', 'warn');
        return;
    }

    if (!_SR.selected || !_SR.selected.version) {
        _srShowValidation('Select a schema version first.', 'warn');
        return;
    }

    const { subject, version } = _SR.selected;
    const cacheKey = subject + ':' + version;
    let regSchema = _SR.schemaCache[cacheKey];
    if (!regSchema) {
        try {
            regSchema = await _srFetch(`/subjects/${encodeURIComponent(subject)}/versions/${version}`);
            _SR.schemaCache[cacheKey] = regSchema;
        } catch (e) {
            _srShowValidation('Could not load schema: ' + e.message, 'err');
            return;
        }
    }

    // Parse registered schema fields
    let parsed;
    try { parsed = JSON.parse(regSchema.schema || '{}'); } catch (_) { parsed = {}; }
    const regFields = _srExtractFields(parsed);

    // Parse SQL columns
    const sqlCols = m[1].split('\n')
        .map(l => l.trim().replace(/,$/, ''))
        .filter(l => l && !l.toUpperCase().startsWith('WATERMARK') && !l.toUpperCase().startsWith('PRIMARY'))
        .map(l => {
            const parts = l.split(/\s+/);
            return { name: parts[0], type: parts[1] || '' };
        })
        .filter(c => c.name && c.type && !c.name.startsWith('--'));

    const issues = [];
    const TYPE_MAP = { 'STRING':'string', 'INT':'int', 'BIGINT':'long',
        'FLOAT':'float', 'DOUBLE':'double', 'BOOLEAN':'boolean', 'BYTES':'bytes' };

    // Check each SQL column against registry
    sqlCols.forEach(col => {
        const reg = regFields.find(f => f.name.toLowerCase() === col.name.toLowerCase());
        if (!reg) {
            issues.push({ level: 'warn', msg: `Column "${col.name}" not in registry schema` });
        } else {
            const regFlinkType = (TYPE_MAP[col.type.toUpperCase()] || '').toLowerCase();
            if (reg.type.toLowerCase() !== regFlinkType && regFlinkType) {
                issues.push({ level: 'err', msg: `Type mismatch "${col.name}": SQL has ${col.type}, registry has ${reg.type}` });
            }
        }
    });

    // Check registry fields missing from SQL
    regFields.forEach(rf => {
        if (!sqlCols.find(c => c.name.toLowerCase() === rf.name.toLowerCase())) {
            issues.push({ level: rf.hasDefault ? 'info' : 'warn',
                msg: `Registry field "${rf.name}" (${rf.type}) not in SQL table${rf.hasDefault ? ' — has default, OK' : ''}` });
        }
    });

    if (!issues.length) {
        _srShowValidation('✓ SQL table is compatible with registry schema', 'ok');
    } else {
        const html = issues.map(i =>
            `<div class="sr-issue-${i.level}">${i.level === 'err' ? '✗' : i.level === 'warn' ? '⚠' : 'ℹ'} ${_escSr(i.msg)}</div>`
        ).join('');
        _srShowValidation(html, issues.some(i => i.level === 'err') ? 'err' : 'warn');
    }
}

// ── Modal builder
function _srBuildModal() {
    if (!document.getElementById('sr-css')) {
        const st = document.createElement('style');
        st.id = 'sr-css';
        st.textContent = `
      #sr-modal .modal {
        width: min(1080px, 97vw);
        height: 88vh;
        max-height: 88vh;
        display: flex;
        flex-direction: column;
      }
      #sr-body { display:flex; flex:1; overflow:hidden; gap:0; }
      #sr-sidebar {
        width: 240px; flex-shrink:0;
        background: var(--bg2,#0f1924);
        border-right: 1px solid var(--border,#1a2a3a);
        display: flex; flex-direction: column; overflow: hidden;
      }
      #sr-sidebar-list {
        flex:1; overflow-y:auto; padding:4px;
      }
      .sr-subject-item {
        display:flex; align-items:center; gap:6px;
        padding:5px 8px; border-radius:3px; cursor:pointer;
        font-size:11px; color:var(--text1,#a0c8e8);
        font-family:var(--mono,monospace);
        white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
        transition:background 0.1s;
      }
      .sr-subject-item:hover { background:rgba(255,255,255,0.05); }
      .sr-subject-item.selected { background:rgba(0,212,170,0.1); color:var(--accent,#00d4aa); }
      #sr-main {
        flex:1; display:flex; flex-direction:column; overflow:hidden;
      }
      #sr-toolbar {
        display:flex; align-items:center; gap:6px; padding:7px 12px;
        border-bottom:1px solid var(--border,#1a2a3a);
        background:var(--bg2,#0f1924); flex-shrink:0; flex-wrap:wrap;
      }
      #sr-detail {
        flex:1; overflow-y:auto; padding:14px 16px;
        font-family:var(--mono,monospace); font-size:11px;
      }
      .sr-version-btn {
        padding:3px 10px; border-radius:3px; font-size:10px;
        background:var(--bg3,#1a2a3a); border:1px solid var(--border,#1a2a3a);
        color:var(--text2,#7a9ab0); cursor:pointer; font-family:var(--mono,monospace);
      }
      .sr-version-btn.active {
        background:rgba(0,212,170,0.12); border-color:rgba(0,212,170,0.35);
        color:var(--accent,#00d4aa); font-weight:600;
      }
      .sr-schema-pre {
        background:var(--bg0,#080b0f); border:1px solid var(--border,#1a2a3a);
        border-radius:5px; padding:12px; font-size:10px; line-height:1.7;
        overflow:auto; max-height:280px; color:var(--text1,#a0c8e8);
        white-space:pre; tab-size:2;
      }
      .sr-diff-added   { color:var(--green,#39d353);  background:rgba(57,211,83,0.07);  padding:1px 4px; border-radius:2px; }
      .sr-diff-removed { color:var(--red,#ff4d6d);    background:rgba(255,77,109,0.07); padding:1px 4px; border-radius:2px; }
      .sr-diff-changed { color:var(--yellow,#f5a623); background:rgba(245,166,35,0.07); padding:1px 4px; border-radius:2px; }
      .sr-diff-breaking{ color:var(--red,#ff4d6d); font-weight:700; }
      .sr-status-ok   { color:var(--green,#39d353); }
      .sr-status-warn { color:var(--yellow,#f5a623); }
      .sr-status-err  { color:var(--red,#ff4d6d); }
      .sr-status-info { color:var(--accent,#00d4aa); }
      .sr-badge {
        font-size:9px; padding:2px 7px; border-radius:10px; font-weight:600;
        font-family:var(--mono,monospace);
      }
      .sr-badge-compat   { background:rgba(57,211,83,0.12);  color:var(--green,#39d353); }
      .sr-badge-breaking { background:rgba(255,77,109,0.12); color:var(--red,#ff4d6d); }
      .sr-issue-err  { color:var(--red,#ff4d6d);    padding:3px 0; }
      .sr-issue-warn { color:var(--yellow,#f5a623); padding:3px 0; }
      .sr-issue-info { color:var(--text2);           padding:3px 0; }
      #sr-connect-panel { padding:20px; display:flex; flex-direction:column; gap:12px; }
      #sr-browser-panel { display:none; flex:1; flex-direction:column; overflow:hidden; }
    `;
        document.head.appendChild(st);
    }

    const modal = document.createElement('div');
    modal.id        = 'sr-modal';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
<div class="modal">
  <div class="modal-header" style="background:rgba(79,163,224,0.06);border-bottom:1px solid rgba(79,163,224,0.2);">
    <span style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--text0);">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--blue,#4fa3e0)" stroke-width="2">
        <ellipse cx="12" cy="6" rx="8" ry="3"/>
        <path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/>
        <path d="M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/>
      </svg>
      Schema Registry
    </span>
    <div style="display:flex;align-items:center;gap:6px;margin-left:auto;">
      <span id="sr-status-text" style="font-size:10px;font-family:var(--mono,monospace);"></span>
      <button onclick="modalMinimize('sr-modal','Schema Registry')" style="background:none;border:1px solid var(--border);color:var(--text2);cursor:pointer;font-size:13px;padding:1px 8px;border-radius:3px;margin-right:4px;" title="Minimise to statusbar">⊟</button><button class="modal-close" onclick="closeModal('sr-modal')">×</button>
    </div>
  </div>

  <!-- Connect panel -->
  <div id="sr-connect-panel">
    <div style="font-size:12px;color:var(--text1);line-height:1.8;">
      Connect to a Confluent Schema Registry or Apicurio REST API endpoint.
    </div>
    <div style="display:grid;grid-template-columns:1fr auto auto;gap:8px;align-items:end;">
      <div>
        <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;
          text-transform:uppercase;letter-spacing:0.8px;">Registry URL</label>
        <input id="sr-url-input" class="field-input" type="text"
          placeholder="http://schema-registry:8081" style="font-size:11px;font-family:var(--mono);" />
      </div>
      <div>
        <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;
          text-transform:uppercase;letter-spacing:0.8px;">API Key (opt)</label>
        <input id="sr-user-input" class="field-input" type="text"
          placeholder="API key" style="font-size:11px;font-family:var(--mono);width:130px;" />
      </div>
      <div>
        <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;
          text-transform:uppercase;letter-spacing:0.8px;">Secret (opt)</label>
        <input id="sr-pass-input" class="field-input" type="password"
          placeholder="API secret" style="font-size:11px;font-family:var(--mono);width:130px;" />
      </div>
    </div>
    <div style="display:flex;gap:8px;">
      <button class="btn btn-primary" style="font-size:11px;" onclick="_srConnect()">
        Connect to Registry
      </button>
      <div style="font-size:11px;color:var(--text2);display:flex;align-items:center;gap:6px;">
        Compatible with: Confluent Schema Registry · Apicurio · Karapace · AWS Glue SR
      </div>
    </div>
  </div>

  <!-- Browser panel (shown after connect) -->
  <div id="sr-browser-panel">
    <div id="sr-body">
      <!-- Sidebar: subject list -->
      <div id="sr-sidebar">
        <div style="padding:8px;border-bottom:1px solid var(--border);flex-shrink:0;">
          <input id="sr-search" class="field-input" type="text" placeholder="Filter subjects…"
            style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;"
            oninput="_srFilterSubjects(this.value)" />
        </div>
        <div style="padding:5px 8px 3px;font-size:9px;color:var(--text3);
          font-family:var(--mono);letter-spacing:1px;text-transform:uppercase;flex-shrink:0;">
          SUBJECTS <span id="sr-subject-count" style="color:var(--accent)"></span>
        </div>
        <div id="sr-sidebar-list"></div>
        <div style="padding:6px 8px;border-top:1px solid var(--border);flex-shrink:0;">
          <button onclick="_srLoadSubjects()"
            style="font-size:10px;width:100%;padding:4px;border-radius:3px;
            border:1px solid var(--border2);background:var(--bg3);color:var(--text2);cursor:pointer;">
            ⟳ Refresh
          </button>
        </div>
      </div>

      <!-- Main detail area -->
      <div id="sr-main">
        <div id="sr-toolbar">
          <span style="font-size:10px;color:var(--text3);font-family:var(--mono);">
            Select a subject →
          </span>
        </div>
        <div id="sr-detail">
          <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
            height:200px;gap:10px;color:var(--text3);">
            <svg width="36" height="36" viewBox="0 0 24 24" fill="none"
              stroke="currentColor" stroke-width="1" opacity="0.3">
              <ellipse cx="12" cy="6" rx="8" ry="3"/>
              <path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/>
              <path d="M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/>
            </svg>
            <span style="font-size:12px;">Select a subject from the list</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('sr-modal'); });
}

// ── Renderers
function _srRenderSubjectList(subjects) {
    const el    = document.getElementById('sr-sidebar-list');
    const count = document.getElementById('sr-subject-count');
    if (count) count.textContent = subjects.length;
    if (!el) return;
    if (!subjects.length) {
        el.innerHTML = '<div style="padding:12px;font-size:11px;color:var(--text3);">No subjects found.</div>';
        return;
    }
    el.innerHTML = subjects.map(s => `
    <div class="sr-subject-item" data-subject="${_escSr(s)}" onclick="_srSelectSubject('${_escSr(s)}')">
      <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor"
        stroke-width="2" style="flex-shrink:0;opacity:0.5;">
        <ellipse cx="12" cy="6" rx="8" ry="3"/>
        <path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/>
      </svg>
      <span style="overflow:hidden;text-overflow:ellipsis;" title="${_escSr(s)}">${_escSr(s)}</span>
    </div>`).join('');
}

function _srFilterSubjects(q) {
    const all = _SR.subjects;
    const filtered = q ? all.filter(s => s.toLowerCase().includes(q.toLowerCase())) : all;
    _srRenderSubjectList(filtered);
}

function _srRenderVersionList(subject, versions) {
    const maxV  = Math.max(...versions);
    const toolbar = document.getElementById('sr-toolbar');
    if (toolbar) {
        toolbar.innerHTML = `
      <span style="font-size:11px;font-weight:700;color:var(--text0);flex:1;
        font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;">${_escSr(subject)}</span>
      <span style="font-size:10px;color:var(--text3);">Versions:</span>
      ${versions.map(v => `<button class="sr-version-btn ${v === maxV ? 'active' : ''}"
        data-version="${v}" onclick="_srSelectVersion('${_escSr(subject)}',${v})">${v}</button>`).join('')}
      <div style="margin-left:auto;display:flex;gap:5px;">
        ${versions.length >= 2 ? `
          <select id="sr-compare-v1" style="font-size:10px;background:var(--bg3);
            border:1px solid var(--border);color:var(--text1);padding:2px 5px;border-radius:2px;">
            ${versions.map(v => `<option value="${v}">${v}</option>`).join('')}
          </select>
          <span style="font-size:10px;color:var(--text3);">vs</span>
          <select id="sr-compare-v2" style="font-size:10px;background:var(--bg3);
            border:1px solid var(--border);color:var(--text1);padding:2px 5px;border-radius:2px;">
            ${versions.map((v,i) => `<option value="${v}" ${i===versions.length-1?'selected':''}>${v}</option>`).join('')}
          </select>
          <button onclick="_srCompare('${_escSr(subject)}',
            document.getElementById('sr-compare-v1').value,
            document.getElementById('sr-compare-v2').value)"
            style="font-size:10px;padding:2px 8px;border-radius:2px;
            background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.3);
            color:var(--blue,#4fa3e0);cursor:pointer;">Diff</button>
        ` : ''}
      </div>`;
    }
    // Auto-load latest version
    _srSelectVersion(subject, maxV);
}

function _srRenderSchema(schema, subject, version) {
    let parsed, pretty;
    try {
        parsed = JSON.parse(schema.schema || '{}');
        pretty = JSON.stringify(parsed, null, 2);
    } catch (_) {
        parsed = {};
        pretty = schema.schema || '';
    }

    const fields   = _srExtractFields(parsed);
    const schemaId = schema.id || '?';
    const schemaType = schema.schemaType || 'AVRO';

    _srShowDetail(`
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap;">
      <span style="font-size:12px;font-weight:700;color:var(--text0);">
        ${_escSr(subject)} — v${version}
      </span>
      <span style="font-size:9px;padding:2px 7px;border-radius:3px;
        background:rgba(79,163,224,0.1);color:var(--blue,#4fa3e0);
        font-family:var(--mono);font-weight:600;">${schemaType}</span>
      <span style="font-size:10px;color:var(--text3);font-family:var(--mono);">ID: ${schemaId}</span>
      <div style="margin-left:auto;display:flex;gap:6px;">
        <button onclick="_srInsertCreateTable(${JSON.stringify(schema).replace(/"/g,'&quot;')}, '${_escSr(subject)}')"
          style="font-size:10px;padding:3px 9px;border-radius:3px;font-weight:600;
          background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.35);
          color:var(--accent);cursor:pointer;">
          Generate CREATE TABLE
        </button>
        <button onclick="_srValidateCreateTable()"
          style="font-size:10px;padding:3px 9px;border-radius:3px;
          background:rgba(79,163,224,0.08);border:1px solid rgba(79,163,224,0.25);
          color:var(--blue,#4fa3e0);cursor:pointer;">
          Validate Editor SQL
        </button>
      </div>
    </div>

    ${fields.length ? `
    <div style="margin-bottom:12px;">
      <div style="font-size:9px;color:var(--text3);font-family:var(--mono);
        letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Fields (${fields.length})</div>
      <table style="width:100%;border-collapse:collapse;font-size:11px;">
        <thead>
          <tr style="border-bottom:1px solid var(--border);">
            <th style="text-align:left;padding:4px 8px;color:var(--text3);font-weight:500;font-size:9px;">NAME</th>
            <th style="text-align:left;padding:4px 8px;color:var(--text3);font-weight:500;font-size:9px;">TYPE</th>
            <th style="text-align:left;padding:4px 8px;color:var(--text3);font-weight:500;font-size:9px;">DEFAULT</th>
          </tr>
        </thead>
        <tbody>
          ${fields.map(f => `
            <tr style="border-bottom:1px solid var(--border);">
              <td style="padding:5px 8px;font-family:var(--mono);color:var(--text0);">${_escSr(f.name)}</td>
              <td style="padding:5px 8px;font-family:var(--mono);color:var(--blue,#4fa3e0);">${_escSr(f.type)}</td>
              <td style="padding:5px 8px;color:${f.hasDefault ? 'var(--green)' : 'var(--text3)'};">
                ${f.hasDefault ? '✓ has default' : '— required'}</td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>` : ''}

    <div>
      <div style="font-size:9px;color:var(--text3);font-family:var(--mono);
        letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Raw Schema</div>
      <pre class="sr-schema-pre">${_escSr(pretty)}</pre>
    </div>

    <div id="sr-validation-result" style="margin-top:10px;"></div>
  `);
}

function _srRenderDiff(subject, v1, v2, diff, s1, s2) {
    const breakingHtml = diff.breaking.length
        ? `<div style="margin-bottom:10px;background:rgba(255,77,109,0.07);border:1px solid rgba(255,77,109,0.2);
        border-radius:5px;padding:10px 12px;">
        <div style="font-size:10px;font-weight:700;color:var(--red);margin-bottom:6px;text-transform:uppercase;">
          ✗ ${diff.breaking.length} Breaking Change${diff.breaking.length > 1 ? 's' : ''}
        </div>
        ${diff.breaking.map(b => `<div class="sr-diff-breaking">• ${_escSr(b)}</div>`).join('')}
      </div>`
        : `<div style="margin-bottom:10px;background:rgba(57,211,83,0.07);border:1px solid rgba(57,211,83,0.2);
        border-radius:5px;padding:8px 12px;font-size:11px;color:var(--green);">
        ✓ No breaking changes detected — schemas are backward compatible
      </div>`;

    const changeRows = [
        ...diff.added.map(f =>
            `<tr><td style="padding:5px 8px;" class="sr-diff-added">+ ${_escSr(f.name)}</td>
       <td style="padding:5px 8px;" class="sr-diff-added">${_escSr(f.type)}</td>
       <td style="padding:5px 8px;font-size:10px;color:var(--green);">Added</td></tr>`),
        ...diff.removed.map(f =>
            `<tr><td style="padding:5px 8px;" class="sr-diff-removed">− ${_escSr(f.name)}</td>
       <td style="padding:5px 8px;" class="sr-diff-removed">${_escSr(f.type)}</td>
       <td style="padding:5px 8px;font-size:10px;color:var(--red);">Removed</td></tr>`),
        ...diff.changed.map(f =>
            `<tr><td style="padding:5px 8px;" class="sr-diff-changed">~ ${_escSr(f.name)}</td>
       <td style="padding:5px 8px;" class="sr-diff-changed">${_escSr(f.from)} → ${_escSr(f.to)}</td>
       <td style="padding:5px 8px;font-size:10px;color:var(--yellow);">Type changed</td></tr>`),
    ].join('');

    _srShowDetail(`
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap;">
      <span style="font-size:12px;font-weight:700;color:var(--text0);">
        Schema Diff — ${_escSr(subject)}
      </span>
      <span style="font-size:11px;color:var(--text3);">v${v1} → v${v2}</span>
      <span class="sr-badge ${diff.compatible ? 'sr-badge-compat' : 'sr-badge-breaking'}" style="margin-left:4px;">
        ${diff.compatible ? '✓ COMPATIBLE' : '✗ BREAKING'}
      </span>
    </div>
    ${breakingHtml}
    ${changeRows ? `
    <div style="font-size:9px;color:var(--text3);font-family:var(--mono);
      letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Field Changes</div>
    <table style="width:100%;border-collapse:collapse;font-size:11px;margin-bottom:14px;">
      <thead>
        <tr style="border-bottom:1px solid var(--border);">
          <th style="text-align:left;padding:4px 8px;color:var(--text3);font-weight:500;font-size:9px;">FIELD</th>
          <th style="text-align:left;padding:4px 8px;color:var(--text3);font-weight:500;font-size:9px;">TYPE / CHANGE</th>
          <th style="text-align:left;padding:4px 8px;color:var(--text3);font-weight:500;font-size:9px;">STATUS</th>
        </tr>
      </thead>
      <tbody>${changeRows}</tbody>
    </table>` : '<div style="font-size:11px;color:var(--text3);margin-bottom:14px;">No field changes.</div>'}
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
      <div>
        <div style="font-size:9px;color:var(--text3);font-family:var(--mono);
          letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">v${v1}</div>
        <pre class="sr-schema-pre" style="max-height:180px;">${_escSr(s1.schema||'')}</pre>
      </div>
      <div>
        <div style="font-size:9px;color:var(--text3);font-family:var(--mono);
          letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">v${v2}</div>
        <pre class="sr-schema-pre" style="max-height:180px;">${_escSr(s2.schema||'')}</pre>
      </div>
    </div>`);
}

function _srInsertCreateTable(schema, subject) {
    const sql = _srGenerateCreateTable(schema, subject);
    const ed  = document.getElementById('sql-editor');
    if (!ed) return;
    const pos    = ed.selectionStart || ed.value.length;
    const prefix = ed.value.trim().length ? '\n\n' : '';
    ed.value = ed.value.slice(0, pos) + prefix + sql + '\n' + ed.value.slice(pos);
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('sr-modal');
    if (typeof toast === 'function') toast('CREATE TABLE inserted from Schema Registry', 'ok');
}

// ── Helpers
function _srShowDetail(html) {
    const el = document.getElementById('sr-detail');
    if (el) el.innerHTML = html;
}

function _srSetSubjectList(html) {
    const el = document.getElementById('sr-sidebar-list');
    if (el) el.innerHTML = html;
}

function _srShowPanel(which) {
    const cp = document.getElementById('sr-connect-panel');
    const bp = document.getElementById('sr-browser-panel');
    if (cp) cp.style.display = which === 'connect' ? 'flex' : 'none';
    if (bp) bp.style.display = which === 'browser' ? 'flex' : 'none';
}

function _srShowValidation(html, level) {
    const el = document.getElementById('sr-validation-result');
    if (!el) return;
    const colors = { ok:'var(--green)', warn:'var(--yellow)', err:'var(--red)', info:'var(--accent)' };
    el.style.cssText = `padding:10px 12px;background:var(--bg2);border:1px solid var(--border);
    border-radius:5px;font-size:11px;color:${colors[level]||'var(--text1)'};line-height:1.8;`;
    el.innerHTML = html;
}

function _srStatus(msg, level) {
    const el = document.getElementById('sr-status-text');
    if (!el) return;
    el.className = 'sr-status-' + level;
    el.textContent = msg;
}

function _escSr(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}