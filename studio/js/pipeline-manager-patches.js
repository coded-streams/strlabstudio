/* Str:::lab Studio — pipeline-manager-patches.js v1.4.0
 * ══════════════════════════════════════════════════════════════════════
 * Add to index.html AFTER pipeline-manager.js:
 *   <script src="js/pipeline-manager-patches.js"></script>
 *
 * FIXES IN v1.4.0:
 *
 * 1. CATALOG SELECTOR ON OPERATORS (jdbc_source, jdbc_sink, and any
 *    connector that reads from a registered catalog):
 *    - Each operator config modal now shows a "Catalog" dropdown that
 *      lists all currently active Flink catalogs (via SHOW CATALOGS).
 *    - The selected catalog is stored in node.params.catalog_name.
 *    - SQL generation respects the chosen catalog: tables are qualified
 *      as `catalog.database.table_name` when a non-default catalog is
 *      selected, and CREATE TABLE DDL is forced into default_catalog
 *      (connector tables cannot be created inside JDBC/Hive catalogs).
 *    - The dropdown refreshes on every modal open.
 *
 * 2. MULTI-STATEMENT SQL PARSE FIX:
 *    The Flink SQL Gateway /v1/sessions/{id}/statements endpoint accepts
 *    exactly ONE statement per call. The pipeline submit path was sending
 *    the entire generated SQL block as one request, which the parser
 *    rejects with "Encountered 'use' at line N".
 *    Fix: _plmValidateAndSubmit now inserts SQL into the editor and lets
 *    the normal Studio executor handle statement splitting (same as
 *    manual execution). The SQL runner is NOT called directly anymore.
 *
 * 3. JDBC CATALOG DDL RESTRICTION:
 *    AbstractJdbcCatalog.createTable() throws UnsupportedOperationException
 *    because JDBC catalogs are read-only metadata views — they reflect
 *    existing relational DB tables, you cannot CREATE TABLE inside them.
 *    Fix: generated SQL now uses CREATE TEMPORARY TABLE (session-scoped,
 *    always lands in default_catalog regardless of active catalog) for
 *    all connector source/sink tables. The active catalog is only used
 *    for qualifying SELECT/INSERT target names when reading real DB tables
 *    via the jdbc_source operator.
 *
 * 4. SELECT * COMMA BUG:
 *    `INSERT INTO sink SELECT *, udf_fn(col) AS alias` is invalid Flink SQL.
 *    Fix: passthrough cols are enumerated from the source schema, not `*`.
 *
 * 5. USE DATABASE default BUG IN catalog-manager.js:
 *    After CREATE CATALOG + USE CATALOG, the code called
 *    updateCatalogStatus(name, 'default') which triggered
 *    USE DATABASE default — failing because JDBC catalogs don't have
 *    a 'default' database. Fix: after USE CATALOG succeeds, query
 *    SHOW DATABASES to find the actual first database and use that.
 *
 * 6. STOP BUTTON STYLE (retained from v1.1.0):
 *    Compact teal glass button, not the big red pill.
 *
 * 7. UDF DROPDOWN LIVE RELOAD (retained from v1.2.0).
 *
 * ══════════════════════════════════════════════════════════════════════
 */

(function _plmPatches() {

    // ─────────────────────────────────────────────────────────────────
    // HELPERS
    // ─────────────────────────────────────────────────────────────────

    /** Run a single Flink SQL statement and return rows. */
    async function _runSingleStmt(sql) {
        if (typeof state === 'undefined' || !state?.gateway || !state?.activeSession) return [];
        const sess    = state.activeSession;
        const trimmed = sql.trim().replace(/;+$/, '');
        const resp    = await api('POST', `/v1/sessions/${sess}/statements`, { statement: trimmed, executionTimeout: 0 });
        const op      = resp.operationHandle;
        for (let i = 0; i < 60; i++) {
            await new Promise(r => setTimeout(r, 300));
            const st = await api('GET', `/v1/sessions/${sess}/operations/${op}/status`);
            const s  = (st.operationStatus || st.status || '').toUpperCase();
            if (s === 'ERROR') return [];
            if (s === 'FINISHED') {
                try {
                    const r = await api('GET', `/v1/sessions/${sess}/operations/${op}/result/0?rowFormat=JSON&maxFetchSize=200`);
                    return (r.results?.data || []).map(row => {
                        const f = row?.fields ?? row;
                        return Array.isArray(f) ? f : Object.values(f);
                    });
                } catch(_) { return []; }
            }
        }
        return [];
    }

    /** Fetch the list of active catalogs from Flink. */
    async function _fetchCatalogs() {
        try {
            const rows = await _runSingleStmt('SHOW CATALOGS');
            return rows.map(r => String(r[0] || '')).filter(Boolean);
        } catch(_) { return ['default_catalog']; }
    }

    /** Fetch databases inside a catalog. */
    async function _fetchDatabases(catalogName) {
        try {
            const rows = await _runSingleStmt(`SHOW DATABASES IN \`${catalogName}\``);
            return rows.map(r => String(r[0] || '')).filter(Boolean);
        } catch(_) { return []; }
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 1 — CATALOG SELECTOR INJECTED INTO OPERATOR CONFIG MODALS
    // ─────────────────────────────────────────────────────────────────
    // Operators that should expose a catalog picker
    const CATALOG_AWARE_OPS = new Set([
        'jdbc_source', 'jdbc_sink',
        'filesystem_source', 'filesystem_sink',
        'hive_source', 'hive_sink',
        'iceberg_sink', 'result_output',
    ]);

    async function _injectCatalogSelector(uid) {
        const modal = document.getElementById('plm-cfg-modal');
        if (!modal) return;
        const node  = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
        if (!node) return;
        if (!CATALOG_AWARE_OPS.has(node.opId)) return;

        // Don't inject twice
        if (modal.querySelector('#plm-catalog-selector-wrap')) return;

        const catalogs = await _fetchCatalogs();
        const current  = node.params?.catalog_name || '';

        const inputStyle = 'width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';

        const wrap = document.createElement('div');
        wrap.id = 'plm-catalog-selector-wrap';
        wrap.style.cssText = 'margin-bottom:12px;border:1px solid rgba(0,212,170,0.2);border-radius:5px;padding:10px 12px;background:rgba(0,212,170,0.04);';
        wrap.innerHTML = `
            <div style="font-size:10px;font-weight:700;color:var(--accent,#00d4aa);letter-spacing:.5px;text-transform:uppercase;margin-bottom:8px;">
                ⊛ Catalog Context
            </div>
            <div style="margin-bottom:8px;">
                <label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">
                    Catalog
                    <span style="color:var(--text3);font-weight:400;"> — tables will be read from this catalog</span>
                </label>
                <select id="plm-catalog-picker" style="${inputStyle}" onchange="_plmCatalogPickerChanged('${uid}')">
                    <option value="">default_catalog (connector tables only)</option>
                    ${catalogs.filter(c => c !== 'default_catalog').map(c =>
            `<option value="${escHtml(c)}" ${current === c ? 'selected' : ''}>${escHtml(c)}</option>`
        ).join('')}
                </select>
                <div style="font-size:10px;color:var(--text3);margin-top:3px;">
                    ℹ JDBC/Hive catalogs are read-only for metadata — connector tables (Kafka, Datagen, Print, etc.)
                    are always created in <code>default_catalog</code> regardless of this setting.
                </div>
            </div>
            <div id="plm-catalog-db-wrap" style="${current ? '' : 'display:none;'}margin-bottom:0;">
                <label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">Database</label>
                <select id="plm-catalog-db-picker" style="${inputStyle}" onchange="_plmCatalogDbPickerChanged('${uid}')">
                    <option value="">— loading —</option>
                </select>
            </div>`;

        // Insert before the Parameters divider (after the color row)
        const paramsPane = modal.querySelector('#plm-cfg-pane-params');
        if (!paramsPane) return;
        const divider = paramsPane.querySelector('div[style*="border-top"]');
        if (divider) {
            paramsPane.insertBefore(wrap, divider);
        } else {
            paramsPane.appendChild(wrap);
        }

        // Populate databases if a catalog is already selected
        if (current) {
            _plmPopulateCatalogDbs(uid, current, node.params?.catalog_database || '');
        }
    }

    window._plmCatalogPickerChanged = async function(uid) {
        const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
        if (!node) return;
        const sel   = document.getElementById('plm-catalog-picker');
        const dbWrap = document.getElementById('plm-catalog-db-wrap');
        const catVal = sel?.value || '';
        node.params = node.params || {};
        node.params.catalog_name = catVal;
        node.params.catalog_database = '';
        if (catVal) {
            if (dbWrap) dbWrap.style.display = 'block';
            _plmPopulateCatalogDbs(uid, catVal, '');
        } else {
            if (dbWrap) dbWrap.style.display = 'none';
        }
        if (typeof _plmBuildPreview === 'function') _plmBuildPreview();
    };

    window._plmCatalogDbPickerChanged = function(uid) {
        const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
        if (!node) return;
        const sel = document.getElementById('plm-catalog-db-picker');
        node.params = node.params || {};
        node.params.catalog_database = sel?.value || '';
        if (typeof _plmBuildPreview === 'function') _plmBuildPreview();
    };

    async function _plmPopulateCatalogDbs(uid, catalogName, currentDb) {
        const dbSel = document.getElementById('plm-catalog-db-picker');
        if (!dbSel) return;
        dbSel.innerHTML = '<option value="">— loading —</option>';
        const dbs = await _fetchDatabases(catalogName);
        if (!dbs.length) {
            dbSel.innerHTML = '<option value="">No databases found</option>';
            return;
        }
        dbSel.innerHTML = dbs.map(db =>
            `<option value="${escHtml(db)}" ${currentDb === db ? 'selected' : ''}>${escHtml(db)}</option>`
        ).join('');
        // Auto-select first if nothing selected
        if (!currentDb && dbs.length) {
            const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
            if (node) { node.params = node.params || {}; node.params.catalog_database = dbs[0]; }
            dbSel.value = dbs[0];
        }
    }

    // Patch _plmOpenCfgModal to inject catalog selector after build
    const _origOpenCfg = window._plmOpenCfgModal;
    if (_origOpenCfg && !_origOpenCfg._catalogPatched) {
        window._plmOpenCfgModal = function(uid) {
            _origOpenCfg.apply(this, arguments);
            setTimeout(() => {
                // Inject catalog selector for relevant op types
                _injectCatalogSelector(uid);
                // Also refresh UDF dropdown (retained from v1.2.0)
                const modal = document.getElementById('plm-cfg-modal');
                if (!modal) return;
                const node  = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
                if (!node || node.opId !== 'udf_node') return;
                const sel = modal.querySelector('#plm-cfg-f-udf_name');
                if (!sel) return;
                const udfs = window._plmGetUdfs();
                const curVal = sel.value;
                sel.innerHTML = '<option value="">— select UDF —</option>'
                    + udfs.map(u => {
                        const name = u.name || u.functionName || '';
                        const lang = u.language || u.lang || '';
                        return `<option value="${escHtml(name)}" ${curVal === name ? 'selected' : ''}>${escHtml(name)}${lang ? ' [' + lang + ']' : ''}</option>`;
                    }).join('')
                    + (udfs.length === 0 ? '<option disabled>No UDFs registered yet</option>' : '');
            }, 80);
        };
        window._plmOpenCfgModal._catalogPatched = true;
    }

    // Patch _plmCfgSave to persist catalog fields
    const _origCfgSave = window._plmCfgSave;
    if (_origCfgSave && !_origCfgSave._catalogPatched) {
        window._plmCfgSave = function(uid) {
            // Capture catalog values before the modal is destroyed
            const catPicker  = document.getElementById('plm-catalog-picker');
            const dbPicker   = document.getElementById('plm-catalog-db-picker');
            const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
            if (node && catPicker) {
                node.params = node.params || {};
                node.params.catalog_name     = catPicker.value || '';
                node.params.catalog_database = dbPicker?.value || '';
            }
            _origCfgSave.apply(this, arguments);
        };
        window._plmCfgSave._catalogPatched = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 2 — MULTI-STATEMENT SUBMIT
    // The Flink SQL Gateway accepts ONE statement per /statements call.
    // Fix: insert SQL into editor and trigger normal execution path.
    // ─────────────────────────────────────────────────────────────────
    const _origSubmit = window._plmValidateAndSubmit;
    if (_origSubmit && !_origSubmit._multiStmtPatched) {
        window._plmValidateAndSubmit = async function() {
            if (!state?.gateway || !state?.activeSession) { toast('Not connected to a session', 'err'); return; }
            const errs = (typeof _plmValidatePipeline === 'function') ? _plmValidatePipeline() : [];
            if (errs.length > 0) {
                const errEl = document.getElementById('plm-status-errors');
                if (errEl) errEl.textContent = '⚠ ' + errs.length + ' error(s)';
                toast(errs[0].msg, 'err');
                if (typeof _plmRenderAll === 'function') _plmRenderAll();
                return;
            }
            const sql = (typeof _plmGenerateSqlWithCatalog === 'function')
                ? _plmGenerateSqlWithCatalog()
                : (typeof _plmGenerateSql === 'function' ? _plmGenerateSql() : '');
            if (!sql || sql.startsWith('-- Add operators')) { toast('Build a pipeline first', 'warn'); return; }

            // Insert into editor — the Studio executor handles statement-by-statement splitting
            const ed = document.getElementById('sql-editor');
            if (ed) {
                ed.value = sql;
                if (typeof updateLineNumbers === 'function') updateLineNumbers();
            }

            const pipelineName = window._plmState?.activePipeline?.name || 'Untitled';
            if (typeof closeModal === 'function') closeModal('modal-pipeline-manager');
            if (typeof addLog   === 'function') addLog('OK', 'Pipeline SQL inserted: ' + pipelineName);
            toast('Pipeline SQL inserted into editor — press ▶ Run or Ctrl+Enter to execute', 'ok');
        };
        window._plmValidateAndSubmit._multiStmtPatched = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 3 — SQL GENERATION WITH CATALOG AWARENESS + TEMP TABLE FIX
    // JDBC/Hive catalogs throw UnsupportedOperationException on CREATE TABLE.
    // All connector source/sink tables must use CREATE TEMPORARY TABLE
    // (session-scoped, always in default_catalog).
    // When a node has catalog_name set, we qualify SELECT/INSERT with it.
    // ─────────────────────────────────────────────────────────────────

    /** Returns fully-qualified table ref for a node, or bare name if default. */
    function _qualifyTableRef(node) {
        const tbl   = (node.params?.table_name || node.label || 'table').toLowerCase().replace(/\s+/g, '_');
        const cat   = node.params?.catalog_name || '';
        const db    = node.params?.catalog_database || '';
        if (!cat || cat === 'default_catalog') return tbl;
        if (db) return `\`${cat}\`.\`${db}\`.\`${tbl}\``;
        return `\`${cat}\`.\`${tbl}\``;
    }

    /** Build per-node SQL using TEMPORARY TABLE for connector nodes. */
    function _plmNodeToSqlPatched(node) {
        const PM_OPS  = window.PM_OPERATORS || [];
        const opDef   = PM_OPS.find(o => o.id === node.opId);
        if (!opDef) return '-- Node: ' + node.label;
        const p     = node.params || {};
        const tbl   = (p.table_name || node.label).toLowerCase().replace(/\s+/g, '_');
        const rawSchema = p.schema || 'id BIGINT\npayload STRING\nts TIMESTAMP(3)';
        const schemaCols = rawSchema.split('\n').map(l => l.trim()).filter(Boolean).map(l => '  ' + l);
        const schema = schemaCols.join(',\n');
        const wm = p.watermark;
        const wmClause = wm ? `,\n  WATERMARK FOR ${wm} AS ${wm} - INTERVAL '${p.watermark_delay || '5'}' SECOND` : '';

        // For JDBC source: use qualified ref if catalog selected
        const jdbcQualRef = _qualifyTableRef(node);

        switch (node.opId) {
            // ── SOURCES ───────────────────────────────────────────────
            case 'kafka_source': {
                const saslProps = _buildKafkaSaslProps(p);
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}${wmClause}\n) WITH (\n  'connector' = 'kafka',\n  'topic' = '${p.topic || 'my-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'properties.group.id' = '${p.group_id || 'flink-group'}',\n  'scan.startup.mode' = '${p.startup_mode || 'latest-offset'}',\n  'format' = '${p.format || 'json'}'${saslProps}\n);`;
            }
            case 'datagen_source':
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'datagen',\n  'rows-per-second' = '${p.rows_per_second || '10'}'\n);`;

            case 'jdbc_source': {
                // JDBC source: register a connector table pointing at the real DB table
                // The real table lives in the catalog, so we use the qualified ref as 'table-name'
                const dbTable = p.db_table || 'your_table';
                const userProp  = p.username ? `,\n  'username' = '${p.username}'` : '';
                const passProp  = p.password ? `,\n  'password' = '${p.password}'` : '';
                const driverProp = p.driver  ? `,\n  'driver'   = '${p.driver}'` : '';
                return `-- Source: ${tbl} (reads from relational DB via JDBC)\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'jdbc',\n  'url'        = '${p.jdbc_url || 'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${dbTable}'${userProp}${passProp}${driverProp}\n);`;
            }
            case 'filesystem_source':
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${p.path || 's3://bucket/data/'}',\n  'format' = '${p.format || 'parquet'}'\n);`;
            case 'pulsar_source':
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'pulsar',\n  'service-url' = '${p.service_url || 'pulsar://pulsar-broker:6650'}',\n  'topics' = '${p.topic || 'my-topic'}',\n  'format' = '${p.format || 'json'}'\n);`;
            case 'kinesis_source':
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'kinesis',\n  'stream' = '${p.stream || 'my-stream'}',\n  'aws.region' = '${p.region || 'us-east-1'}',\n  'format' = '${p.format || 'json'}'\n);`;

            // ── SINKS ─────────────────────────────────────────────────
            case 'kafka_sink': {
                const saslProps = _buildKafkaSaslProps(p);
                const canvas = window._plmState?.canvas;
                const srcNode = canvas?.nodes?.find(n => (window.PM_OPERATORS||[]).find(o => o.id === n.opId)?.isSource);
                const srcTbl  = srcNode?.params?.table_name || 'source_table';
                if (p.schema && p.schema.trim()) {
                    const sc = p.schema.split('\n').map(l => '  ' + l.trim()).filter(Boolean).join(',\n');
                    return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${sc}\n) WITH (\n  'connector' = 'kafka',\n  'topic' = '${p.topic || 'output-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'format' = '${p.format || 'json'}'${saslProps}\n);`;
                }
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'kafka',\n  'topic' = '${p.topic || 'output-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'format' = '${p.format || 'json'}'${saslProps}\n) LIKE ${srcTbl} (EXCLUDING ALL);`;
            }
            case 'jdbc_sink': {
                const userProp  = p.username ? `,\n  'username' = '${p.username}'` : '';
                const passProp  = p.password ? `,\n  'password' = '${p.password}'` : '';
                const driverProp = p.driver  ? `,\n  'driver'   = '${p.driver}'` : '';
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url'        = '${p.jdbc_url || 'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table || 'output_table'}'${userProp}${passProp}${driverProp}\n);`;
            }
            case 'filesystem_sink':
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${p.path || 's3://bucket/output/'}',\n  'format' = '${p.format || 'parquet'}'\n);`;
            case 'elasticsearch_sink':
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-${p.es_version || '7'}',\n  'hosts' = '${p.hosts || 'http://elasticsearch:9200'}',\n  'index' = '${p.index || 'my-index'}'\n);`;
            case 'print_sink': {
                const canvas = window._plmState?.canvas;
                const srcNode = canvas?.nodes?.find(n => (window.PM_OPERATORS||[]).find(o => o.id === n.opId)?.isSource);
                const srcTbl  = srcNode?.params?.table_name || 'source_table';
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'print'${p.print_identifier ? `,\n  'print-identifier' = '${p.print_identifier}'` : ''}\n) LIKE ${srcTbl} (EXCLUDING ALL);`;
            }
            case 'blackhole_sink': {
                const canvas = window._plmState?.canvas;
                const srcNode = canvas?.nodes?.find(n => (window.PM_OPERATORS||[]).find(o => o.id === n.opId)?.isSource);
                const srcTbl  = srcNode?.params?.table_name || 'source_table';
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'blackhole'\n) LIKE ${srcTbl} (EXCLUDING ALL);`;
            }
            case 'mongodb_sink':
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'mongodb',\n  'uri' = '${p.uri || 'mongodb://localhost:27017/mydb'}',\n  'collection' = '${p.collection || 'my-collection'}'\n);`;
            case 'result_output':
                return '';  // handled in INSERT section
            default:
                return `-- Operator: ${node.label} (${node.opId})`;
        }
    }

    function _buildKafkaSaslProps(p) {
        if (!p.security_protocol || p.security_protocol === '') return '';
        let out = `,\n  'properties.security.protocol' = '${p.security_protocol}'`;
        if (p.sasl_mechanism)  out += `,\n  'properties.sasl.mechanism' = '${p.sasl_mechanism}'`;
        if (p.sasl_username && p.sasl_password) {
            out += `,\n  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${p.sasl_username}" password="${p.sasl_password}";'`;
        }
        if (p.schema_registry_url) {
            out += `,\n  'schema-registry.url' = '${p.schema_registry_url}'`;
        }
        return out;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 4 — SELECT * COMMA BUG + catalog-aware INSERT SQL
    // ─────────────────────────────────────────────────────────────────
    function _buildInsertSqlPatched(sources, sinks, nodes, edges) {
        if (!sources.length || !sinks.length) return '';
        const src  = sources[0];
        const sink = sinks[0];
        const PM_OPS = window.PM_OPERATORS || [];

        const srcName  = src.params?.table_name  || 'source_table';
        const sinkName = sink.params?.table_name || 'sink_table';

        const transforms = nodes.filter(n => {
            const o = PM_OPS.find(op => op.id === n.opId);
            return o && !o.isSource && !o.isSink;
        });

        // Enumerate schema columns from source to avoid SELECT * issues
        const rawSchema = src.params?.schema || '';
        const schemaCols = rawSchema.split('\n')
            .map(l => l.trim().split(/\s+/)[0])
            .filter(Boolean);
        const allCols = schemaCols.length ? schemaCols.join(', ') : '*';

        let selectCols = allCols;
        let fromClause = srcName;
        let whereClauses = [];
        let groupBy = null;

        transforms.forEach(node => {
            const np = node.params || {};
            switch (node.opId) {
                case 'filter':
                    if (np.condition) whereClauses.push(np.condition);
                    break;
                case 'project':
                    if (np.columns) selectCols = np.columns.split('\n').map(l => l.trim()).filter(Boolean).join(',\n  ');
                    break;
                case 'map_udf':
                    if (np.function_name && np.input_col && np.output_alias) {
                        const passCols = np.extra_cols
                            ? np.extra_cols.split(',').map(c => c.trim()).filter(Boolean).join(', ')
                            : allCols;
                        selectCols = passCols + ',\n  ' + np.function_name + '(' + np.input_col + ') AS ' + np.output_alias;
                    }
                    break;
                case 'udf_node':
                    if (np.udf_name && np.input_cols && np.output_alias) {
                        const passCols = np.extra_cols
                            ? np.extra_cols.split(',').map(c => c.trim()).filter(Boolean).join(', ')
                            : allCols;
                        selectCols = passCols + ',\n  ' + np.udf_name + '(' + np.input_cols + ') AS ' + np.output_alias;
                    }
                    break;
                case 'tumble_window':
                    fromClause = `TABLE(TUMBLE(TABLE ${fromClause}, DESCRIPTOR(${np.time_col}), INTERVAL '${np.window_size || '1 MINUTE'}'))`;
                    if (np.aggregations) selectCols = 'window_start, window_end,\n  ' + np.aggregations.split('\n').map(l => l.trim()).filter(Boolean).join(',\n  ');
                    groupBy = 'window_start, window_end' + (np.group_by ? ', ' + np.group_by : '');
                    break;
                case 'aggregate':
                    if (np.aggregations) selectCols = (np.group_by ? np.group_by + ',\n  ' : '') + np.aggregations.split('\n').map(l => l.trim()).filter(Boolean).join(',\n  ');
                    groupBy = np.group_by || null;
                    break;
            }
        });

        let sql = `INSERT INTO ${sinkName}\nSELECT\n  ${selectCols}\nFROM ${fromClause}`;
        if (whereClauses.length) sql += '\nWHERE ' + whereClauses.join(' AND ');
        if (groupBy) sql += '\nGROUP BY ' + groupBy;
        sql += ';';
        return sql;
    }

    // Patch the main SQL generator to use fixed helpers
    function _plmGenerateSqlWithCatalog() {
        const { nodes, edges } = window._plmState?.canvas || { nodes: [], edges: [] };
        if (!nodes.length) return '-- Add operators to the canvas to generate SQL';

        const PM_OPS = window.PM_OPERATORS || [];
        const lines = [];
        lines.push('-- ══════════════════════════════════════════════════════');
        lines.push('-- Pipeline: ' + (window._plmState?.activePipeline?.name || 'Untitled'));
        lines.push('-- Generated by Str:::lab Studio Pipeline Manager v2.1');
        lines.push('-- ══════════════════════════════════════════════════════\n');

        const ps = window._plmState?.pipelineSettings;
        if (ps && typeof _plmBuildSettingsSql === 'function') {
            lines.push(_plmBuildSettingsSql(ps) + '\n');
        }

        // Topological sort
        const inDeg = {}, childMap = {};
        nodes.forEach(n => { inDeg[n.uid] = 0; childMap[n.uid] = []; });
        edges.forEach(e => { if (inDeg[e.toUid] !== undefined) inDeg[e.toUid]++; if (childMap[e.fromUid]) childMap[e.fromUid].push(e.toUid); });
        let queue = nodes.filter(n => inDeg[n.uid] === 0).map(n => n.uid);
        const order = [], tmpCount = { ...inDeg };
        while (queue.length) {
            const uid = queue.shift(); order.push(uid);
            (childMap[uid] || []).forEach(cid => { tmpCount[cid]--; if (tmpCount[cid] === 0) queue.push(cid); });
        }
        const orderedNodes = order.map(uid => nodes.find(n => n.uid === uid)).filter(Boolean);
        nodes.filter(n => !order.includes(n.uid)).forEach(n => orderedNodes.push(n));

        const sources = nodes.filter(n => PM_OPS.find(o => o.id === n.opId)?.isSource);
        const sinks   = nodes.filter(n => PM_OPS.find(o => o.id === n.opId)?.isSink);

        // Emit CREATE TEMPORARY TABLE for sources
        orderedNodes.filter(n => PM_OPS.find(o => o.id === n.opId)?.isSource).forEach(n => {
            const sql = _plmNodeToSqlPatched(n);
            if (sql) { lines.push(sql); lines.push(''); }
        });

        // Emit CREATE TEMPORARY TABLE for sinks
        orderedNodes.filter(n => PM_OPS.find(o => o.id === n.opId)?.isSink).forEach(n => {
            const sql = _plmNodeToSqlPatched(n);
            if (sql) { lines.push(sql); lines.push(''); }
        });

        if (sources.length && sinks.length) {
            lines.push('-- ──────────────────────────────────────────────────────');
            lines.push('-- Pipeline execution');
            lines.push('-- ──────────────────────────────────────────────────────');
            const insertSql = _buildInsertSqlPatched(sources, sinks, nodes, edges);
            if (insertSql) lines.push(insertSql);
        }

        return lines.join('\n');
    }

    // Expose so _plmValidateAndSubmit can call it
    window._plmGenerateSqlWithCatalog = _plmGenerateSqlWithCatalog;

    // Also patch the live SQL preview to use the catalog-aware generator
    const _origUpdateSqlPreview = window._plmUpdateSqlPreview;
    if (_origUpdateSqlPreview && !_origUpdateSqlPreview._catalogPatched) {
        window._plmUpdateSqlPreview = function() {
            const sql     = _plmGenerateSqlWithCatalog();
            const prevEl  = document.getElementById('plm-sql-preview');
            const fullEl  = document.getElementById('plm-sql-full');
            if (prevEl) prevEl.textContent = sql;
            if (fullEl) fullEl.textContent = sql;
        };
        window._plmUpdateSqlPreview._catalogPatched = true;
    }
    // Alias used elsewhere
    window._plmUpdateSqlView = window._plmUpdateSqlPreview;

    // ─────────────────────────────────────────────────────────────────
    // FIX 5 — USE DATABASE default BUG IN catalog-manager.js
    // After a successful USE CATALOG, query SHOW DATABASES to find the
    // actual default DB instead of hardcoding 'default'.
    // ─────────────────────────────────────────────────────────────────
    function _patchCatExecute() {
        if (typeof window._catExecute !== 'function') return false;
        if (window._catExecute._sidebarPatched) return true;
        const _origCatExec = window._catExecute;
        window._catExecute = async function() {
            await _origCatExec.apply(this, arguments);
            setTimeout(async () => {
                const name        = (document.getElementById('cat-name-input')?.value || '').trim();
                const switchAfter = document.getElementById('cat-switch-after')?.value || 'yes';
                if (typeof refreshCatalog === 'function') refreshCatalog();
                if (name && switchAfter === 'yes') {
                    // Find actual first database in the new catalog — don't assume 'default'
                    let actualDb = 'default';
                    try {
                        const rows = await _runSingleStmt(`SHOW DATABASES IN \`${name}\``);
                        if (rows.length) actualDb = String(rows[0][0] || 'default');
                    } catch(_) {}

                    if (typeof state !== 'undefined') {
                        state.activeCatalog  = name;
                        state.activeDatabase = actualDb;
                    }
                    if (typeof updateCatalogStatus === 'function') updateCatalogStatus(name, actualDb);
                    ['refreshCatalogBrowser', 'loadCatalogTree', 'buildCatalogTree'].forEach(fn => {
                        if (typeof window[fn] === 'function') try { window[fn](); } catch(_) {}
                    });
                }
            }, 900);
        };
        window._catExecute._sidebarPatched = true;
        return true;
    }
    if (!_patchCatExecute()) {
        const t = setInterval(() => { if (_patchCatExecute()) clearInterval(t); }, 400);
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 6 — STOP BUTTON STYLE (retained from v1.1.0)
    // ─────────────────────────────────────────────────────────────────
    if (!document.getElementById('plm-stop-btn-patch-css')) {
        const s = document.createElement('style');
        s.id = 'plm-stop-btn-patch-css';
        s.textContent = `
#plm-float-stop-btn {
  background: linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12)) !important;
  backdrop-filter: blur(8px) !important;
  -webkit-backdrop-filter: blur(8px) !important;
  border: 1px solid rgba(0,212,170,0.45) !important;
  color: #00d4aa !important;
  padding: 5px 14px !important;
  border-radius: 6px !important;
  font-size: 10px !important;
  font-weight: 700 !important;
  letter-spacing: 0.8px !important;
  box-shadow: 0 0 12px rgba(0,212,170,0.2), inset 0 1px 0 rgba(255,255,255,0.08) !important;
  text-transform: uppercase !important;
}
#plm-float-stop-btn:hover {
  background: linear-gradient(135deg,rgba(0,212,170,0.28),rgba(0,180,200,0.20)) !important;
  border-color: rgba(0,212,170,0.7) !important;
  box-shadow: 0 0 18px rgba(0,212,170,0.35), inset 0 1px 0 rgba(255,255,255,0.1) !important;
}`;
        document.head.appendChild(s);
    }

    function _patchStopBtn() {
        const floatBtn = document.getElementById('plm-float-stop-btn');
        if (!floatBtn || floatBtn._patched) return !!floatBtn;
        floatBtn._patched = true;
        const origSync = window._plmSyncRunBtn;
        if (origSync && !origSync._stopPatched) {
            window._plmSyncRunBtn = function() {
                origSync.apply(this, arguments);
                const fb = document.getElementById('plm-float-stop-btn');
                if (!fb) return;
                if (window._plmState?.animating) {
                    fb.style.cssText = [
                        'display:block', 'position:absolute', 'top:8px', 'left:50%',
                        'transform:translateX(-50%)', 'z-index:30',
                        'background:linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12))',
                        'backdrop-filter:blur(8px)', '-webkit-backdrop-filter:blur(8px)',
                        'border:1px solid rgba(0,212,170,0.45)', 'color:#00d4aa', 'cursor:pointer',
                        'padding:5px 14px', 'border-radius:6px', 'font-size:10px', 'font-weight:700',
                        'font-family:var(--mono)', 'letter-spacing:0.8px',
                        'box-shadow:0 0 12px rgba(0,212,170,0.2),inset 0 1px 0 rgba(255,255,255,0.08)',
                        'white-space:nowrap', 'text-transform:uppercase',
                    ].join(';');
                    fb.innerHTML = '<svg width="9" height="9" viewBox="0 0 12 12" fill="currentColor" style="margin-right:5px;vertical-align:middle;"><rect x="1" y="1" width="10" height="10" rx="1"/></svg>Stop';
                } else {
                    fb.style.display = 'none';
                }
            };
            window._plmSyncRunBtn._stopPatched = true;
        }
        return true;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 7 — UDF DROPDOWN LIVE RELOAD (retained from v1.2.0)
    // ─────────────────────────────────────────────────────────────────
    window._plmGetUdfs = function() {
        try {
            const a1 = JSON.parse(localStorage.getItem('strlabstudio_udfs') || '[]');
            const a2 = JSON.parse(localStorage.getItem('strlabstudio_udf_registry') || '[]');
            const seen = new Set(), merged = [];
            for (const u of [...a1, ...a2]) {
                const n = u.name || u.functionName || '';
                if (n && !seen.has(n.toLowerCase())) { seen.add(n.toLowerCase()); merged.push(u); }
            }
            return merged;
        } catch(_) { return []; }
    };

    // ─────────────────────────────────────────────────────────────────
    // INIT
    // ─────────────────────────────────────────────────────────────────
    let attempts = 0;
    const initTimer = setInterval(() => {
        attempts++;
        if (_patchStopBtn()) clearInterval(initTimer);
        if (attempts > 60) clearInterval(initTimer);
    }, 300);

    const _origOpenPLM = window.openPipelineManager;
    if (_origOpenPLM && !_origOpenPLM._patchesApplied) {
        window.openPipelineManager = function() {
            _origOpenPLM.apply(this, arguments);
            setTimeout(_patchStopBtn, 200);
        };
        window.openPipelineManager._patchesApplied = true;
    }

    console.log('[PLM Patches v1.4.0] loaded — catalog selector, multi-stmt fix, temp table fix, select* fix, activeDatabase fix');
})();