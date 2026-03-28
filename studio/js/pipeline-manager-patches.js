/* Str:::lab Studio — pipeline-manager-patches.js v1.6.0
 * ══════════════════════════════════════════════════════════════════════
 * v1.6.0 changes:
 *  - Catalog selector ONLY on jdbc_source and jdbc_sink — not on Kafka,
 *    Filesystem, Hive, Iceberg or any other operator. JDBC catalogs are
 *    the only ones where the catalog context changes which DB table is read.
 *  - Added cdc_source operator: MySQL CDC and PostgreSQL CDC sources with
 *    full params and correct SQL generation.
 *  - Added cdc_sink operator: writes CDC changelog to JDBC or upsert-kafka.
 *  - All prior fixes retained (multi-stmt submit, TEMP TABLE, select* fix,
 *    activeDatabase fix, stop button style, UDF dropdown live reload).
 * ══════════════════════════════════════════════════════════════════════
 */

(function _plmPatches() {

    // ─────────────────────────────────────────────────────────────────
    // HELPERS
    // ─────────────────────────────────────────────────────────────────
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
                    return (r.results?.data || []).map(row => { const f = row?.fields ?? row; return Array.isArray(f) ? f : Object.values(f); });
                } catch(_) { return []; }
            }
        }
        return [];
    }

    async function _fetchCatalogs() {
        try { const rows = await _runSingleStmt('SHOW CATALOGS'); return rows.map(r => String(r[0] || '')).filter(Boolean); }
        catch(_) { return ['default_catalog']; }
    }

    async function _fetchDatabases(catalogName) {
        try { const rows = await _runSingleStmt(`SHOW DATABASES IN \`${catalogName}\``); return rows.map(r => String(r[0] || '')).filter(Boolean); }
        catch(_) { return []; }
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 1 — CATALOG SELECTOR: ONLY jdbc_source AND jdbc_sink
    // These are the only operators where catalog context affects which
    // database table is connected to. All other connectors (Kafka, CDC,
    // Filesystem, Print, Blackhole, etc.) always use default_catalog.
    // ─────────────────────────────────────────────────────────────────
    const JDBC_CATALOG_OPS = new Set(['jdbc_source', 'jdbc_sink']);

    async function _injectCatalogSelector(uid) {
        const modal = document.getElementById('plm-cfg-modal');
        if (!modal) return;
        const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
        if (!node || !JDBC_CATALOG_OPS.has(node.opId)) return;
        if (modal.querySelector('#plm-catalog-row')) return;

        const catalogs = await _fetchCatalogs();
        const current  = node.params?.catalog_name || '';
        const paramsPane = modal.querySelector('#plm-cfg-pane-params');
        if (!paramsPane) return;

        const inputStyle = 'width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';

        const row = document.createElement('div');
        row.id = 'plm-catalog-row';
        row.style.cssText = 'margin-bottom:10px;';
        row.innerHTML = `
            <label style="display:block;font-size:10px;color:var(--text2);font-weight:600;margin-bottom:3px;text-transform:uppercase;letter-spacing:.4px;">Catalog</label>
            <select id="plm-catalog-picker" style="${inputStyle}" onchange="_plmCatalogPickerChanged('${uid}')">
                <option value="">default_catalog</option>
                ${catalogs.filter(c => c !== 'default_catalog').map(c =>
            `<option value="${escHtml(c)}" ${current === c ? 'selected' : ''}>${escHtml(c)}</option>`
        ).join('')}
            </select>`;

        const firstChild = paramsPane.firstElementChild;
        if (firstChild) paramsPane.insertBefore(row, firstChild);
        else paramsPane.appendChild(row);

        if (current && current !== 'default_catalog') {
            _plmInjectDbPicker(uid, current, node.params?.catalog_database || '');
        }
    }

    async function _plmInjectDbPicker(uid, catalogName, currentDb) {
        const modal = document.getElementById('plm-cfg-modal');
        if (!modal || modal.querySelector('#plm-catalog-db-row')) return;
        const dbs = await _fetchDatabases(catalogName);
        if (!dbs.length) return;
        const paramsPane = modal.querySelector('#plm-cfg-pane-params'); if (!paramsPane) return;
        const inputStyle = 'width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
        const dbRow = document.createElement('div');
        dbRow.id = 'plm-catalog-db-row';
        dbRow.style.cssText = 'margin-bottom:10px;';
        dbRow.innerHTML = `
            <label style="display:block;font-size:10px;color:var(--text2);font-weight:600;margin-bottom:3px;text-transform:uppercase;letter-spacing:.4px;">Database</label>
            <select id="plm-catalog-db-picker" style="${inputStyle}" onchange="_plmCatalogDbPickerChanged('${uid}')">
                ${dbs.map(db => `<option value="${escHtml(db)}" ${currentDb === db ? 'selected' : ''}>${escHtml(db)}</option>`).join('')}
            </select>`;
        const catalogRow = paramsPane.querySelector('#plm-catalog-row');
        if (catalogRow?.nextSibling) paramsPane.insertBefore(dbRow, catalogRow.nextSibling);
        else paramsPane.appendChild(dbRow);
        const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
        if (node && !currentDb && dbs.length) { node.params = node.params || {}; node.params.catalog_database = dbs[0]; }
    }

    window._plmCatalogPickerChanged = async function(uid) {
        const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid); if (!node) return;
        const catVal = document.getElementById('plm-catalog-picker')?.value || '';
        node.params = node.params || {};
        node.params.catalog_name     = catVal;
        node.params.catalog_database = '';
        const oldDb = document.getElementById('plm-catalog-db-row'); if (oldDb) oldDb.remove();
        if (catVal && catVal !== 'default_catalog') _plmInjectDbPicker(uid, catVal, '');
        if (typeof _plmBuildPreview === 'function') _plmBuildPreview();
    };

    window._plmCatalogDbPickerChanged = function(uid) {
        const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid); if (!node) return;
        node.params = node.params || {};
        node.params.catalog_database = document.getElementById('plm-catalog-db-picker')?.value || '';
        if (typeof _plmBuildPreview === 'function') _plmBuildPreview();
    };

    const _origOpenCfg = window._plmOpenCfgModal;
    if (_origOpenCfg && !_origOpenCfg._catalogPatched) {
        window._plmOpenCfgModal = function(uid) {
            _origOpenCfg.apply(this, arguments);
            setTimeout(() => {
                _injectCatalogSelector(uid);
                // UDF dropdown refresh
                const modal = document.getElementById('plm-cfg-modal'); if (!modal) return;
                const node  = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
                if (!node || node.opId !== 'udf_node') return;
                const sel = modal.querySelector('#plm-cfg-f-udf_name'); if (!sel) return;
                const udfs = window._plmGetUdfs(); const curVal = sel.value;
                sel.innerHTML = '<option value="">— select UDF —</option>' + udfs.map(u => { const n = u.name||u.functionName||''; const l = u.language||u.lang||''; return `<option value="${escHtml(n)}" ${curVal===n?'selected':''}>${escHtml(n)}${l?' ['+l+']':''}</option>`; }).join('') + (udfs.length===0?'<option disabled>No UDFs registered yet</option>':'');
            }, 80);
        };
        window._plmOpenCfgModal._catalogPatched = true;
    }

    const _origCfgSave = window._plmCfgSave;
    if (_origCfgSave && !_origCfgSave._catalogPatched) {
        window._plmCfgSave = function(uid) {
            const catPicker = document.getElementById('plm-catalog-picker');
            const dbPicker  = document.getElementById('plm-catalog-db-picker');
            const node = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
            if (node && catPicker) { node.params = node.params || {}; node.params.catalog_name = catPicker.value || ''; node.params.catalog_database = dbPicker?.value || ''; }
            _origCfgSave.apply(this, arguments);
        };
        window._plmCfgSave._catalogPatched = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 2 — MULTI-STATEMENT SUBMIT (insert into editor, not direct exec)
    // ─────────────────────────────────────────────────────────────────
    const _origSubmit = window._plmValidateAndSubmit;
    if (_origSubmit && !_origSubmit._multiStmtPatched) {
        window._plmValidateAndSubmit = async function() {
            if (!state?.gateway || !state?.activeSession) { toast('Not connected to a session', 'err'); return; }
            const errs = (typeof _plmValidatePipeline === 'function') ? _plmValidatePipeline() : [];
            if (errs.length > 0) { const errEl = document.getElementById('plm-status-errors'); if (errEl) errEl.textContent='⚠ '+errs.length+' error(s)'; toast(errs[0].msg,'err'); if (typeof _plmRenderAll==='function') _plmRenderAll(); return; }
            const sql = (typeof _plmGenerateSqlWithCatalog === 'function') ? _plmGenerateSqlWithCatalog() : (typeof _plmGenerateSql === 'function' ? _plmGenerateSql() : '');
            if (!sql || sql.startsWith('-- Add operators')) { toast('Build a pipeline first', 'warn'); return; }
            const ed = document.getElementById('sql-editor');
            if (ed) { ed.value = sql; if (typeof updateLineNumbers === 'function') updateLineNumbers(); }
            if (typeof closeModal === 'function') closeModal('modal-pipeline-manager');
            if (typeof addLog   === 'function') addLog('OK', 'Pipeline SQL inserted: ' + (window._plmState?.activePipeline?.name || 'Untitled'));
            toast('Pipeline SQL inserted into editor — press ▶ Run or Ctrl+Enter to execute', 'ok');
        };
        window._plmValidateAndSubmit._multiStmtPatched = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 3 — SQL GENERATION: TEMP TABLE + catalog-aware JDBC
    // ─────────────────────────────────────────────────────────────────
    function _buildKafkaSaslProps(p) {
        if (!p.security_protocol) return '';
        let out = `,\n  'properties.security.protocol' = '${p.security_protocol}'`;
        if (p.sasl_mechanism)  out += `,\n  'properties.sasl.mechanism' = '${p.sasl_mechanism}'`;
        if (p.sasl_username && p.sasl_password) out += `,\n  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${p.sasl_username}" password="${p.sasl_password}";'`;
        if (p.schema_registry_url) out += `,\n  'schema-registry.url' = '${p.schema_registry_url}'`;
        return out;
    }

    function _plmNodeToSqlPatched(node) {
        const PM_OPS = window.PM_OPERATORS || [];
        const opDef  = PM_OPS.find(o => o.id === node.opId); if (!opDef) return '-- Node: ' + node.label;
        const p   = node.params || {};
        const tbl = (p.table_name || node.label).toLowerCase().replace(/\s+/g, '_');
        const rawSchema  = p.schema || 'id BIGINT\npayload STRING\nts TIMESTAMP(3)';
        const schemaCols = rawSchema.split('\n').map(l => l.trim()).filter(Boolean).map(l => '  ' + l);
        const schema     = schemaCols.join(',\n');
        const wm         = p.watermark;
        const wmClause   = wm ? `,\n  WATERMARK FOR ${wm} AS ${wm} - INTERVAL '${p.watermark_delay || '5'}' SECOND` : '';

        switch (node.opId) {
            case 'kafka_source': {
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}${wmClause}\n) WITH (\n  'connector' = 'kafka',\n  'topic' = '${p.topic || 'my-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'properties.group.id' = '${p.group_id || 'flink-group'}',\n  'scan.startup.mode' = '${p.startup_mode || 'latest-offset'}',\n  'format' = '${p.format || 'json'}'${_buildKafkaSaslProps(p)}\n);`;
            }
            case 'datagen_source':
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'datagen',\n  'rows-per-second' = '${p.rows_per_second || '10'}'\n);`;

            // ── CDC SOURCE ─────────────────────────────────────────────
            case 'cdc_source': {
                const cdcType  = p.cdc_type || 'mysql-cdc';
                const isPg     = cdcType === 'postgres-cdc';
                const port     = p.port || (isPg ? '5432' : '3306');
                const serverIdProp = (!isPg && p.server_id)  ? `\n  'server-id'            = '${p.server_id}',` : (!isPg ? `\n  'server-id'            = '5401-5404',` : '');
                const slotProp     = (isPg)                  ? `\n  'slot.name'            = '${p.slot_name || 'flink_slot'}',` : '';
                const pluginProp   = (isPg)                  ? `\n  'decoding.plugin.name' = '${p.plugin_name || 'pgoutput'}',` : '';
                const schemaProp   = (isPg && p.schema_name) ? `\n  'schema-name'          = '${p.schema_name}',` : '';
                const prereq = isPg
                    ? `-- PG prerequisites: ALTER SYSTEM SET wal_level=logical; SELECT pg_create_logical_replication_slot('${p.slot_name||'flink_slot'}','pgoutput');`
                    : `-- MySQL prerequisites: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${p.username||'flink_user'}'@'%';`;
                return `${prereq}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}${wmClause}\n) WITH (\n  'connector'     = '${cdcType}',\n  'hostname'      = '${p.hostname || 'localhost'}',\n  'port'          = '${port}',\n  'username'      = '${p.username || 'flink_user'}',\n  'password'      = '${p.password || ''}',\n  'database-name' = '${p.database_name || 'mydb'}',${schemaProp}${serverIdProp}${slotProp}${pluginProp}\n  'table-name'    = '${p.db_table || 'orders'}'\n);`;
            }

            // ── CDC SINK ───────────────────────────────────────────────
            case 'cdc_sink': {
                const sinkType = p.sink_type || 'jdbc';
                if (sinkType === 'upsert-kafka') {
                    return `-- CDC Sink (upsert-kafka — preserves changelog semantics):\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'upsert-kafka',\n  'topic' = '${p.topic || 'cdc-output'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'key.format'   = 'json',\n  'value.format' = 'json'\n);`;
                }
                // Default: JDBC sink (materialise CDC into a relational table)
                const userProp   = p.username ? `,\n  'username' = '${p.username}'` : '';
                const passProp   = p.password ? `,\n  'password' = '${p.password}'` : '';
                return `-- CDC Sink (JDBC — materialises CDC changes into a DB table):\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector'  = 'jdbc',\n  'url'        = '${p.jdbc_url || 'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table || tbl}'${userProp}${passProp}\n);`;
            }

            case 'jdbc_source': {
                const dbTable   = p.db_table || 'your_table';
                const userProp  = p.username ? `,\n  'username' = '${p.username}'` : '';
                const passProp  = p.password ? `,\n  'password' = '${p.password}'` : '';
                return `-- Source: ${tbl} (JDBC — reads from relational DB)\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'jdbc',\n  'url'        = '${p.jdbc_url || 'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${dbTable}'${userProp}${passProp}\n);`;
            }
            case 'filesystem_source':
                return `-- Source: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${p.path || 's3://bucket/data/'}',\n  'format' = '${p.format || 'parquet'}'\n);`;

            case 'kafka_sink': {
                const saslProps = _buildKafkaSaslProps(p);
                const canvas    = window._plmState?.canvas;
                const srcNode   = canvas?.nodes?.find(n => (window.PM_OPERATORS||[]).find(o => o.id === n.opId)?.isSource);
                const srcTbl    = srcNode?.params?.table_name || 'source_table';
                if (p.schema && p.schema.trim()) {
                    const sc = p.schema.split('\n').map(l => '  ' + l.trim()).filter(Boolean).join(',\n');
                    return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${sc}\n) WITH (\n  'connector' = 'kafka',\n  'topic' = '${p.topic || 'output-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'format' = '${p.format || 'json'}'${saslProps}\n);`;
                }
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'kafka',\n  'topic' = '${p.topic || 'output-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers || 'kafka:9092'}',\n  'format' = '${p.format || 'json'}'${saslProps}\n) LIKE ${srcTbl} (EXCLUDING ALL);`;
            }
            case 'jdbc_sink': {
                const userProp   = p.username ? `,\n  'username' = '${p.username}'` : '';
                const passProp   = p.password ? `,\n  'password' = '${p.password}'` : '';
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url'        = '${p.jdbc_url || 'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table || 'output_table'}'${userProp}${passProp}\n);`;
            }
            case 'filesystem_sink':
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'filesystem',\n  'path' = '${p.path || 's3://bucket/output/'}',\n  'format' = '${p.format || 'parquet'}'\n);`;
            case 'elasticsearch_sink':
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-${p.es_version || '7'}',\n  'hosts' = '${p.hosts || 'http://elasticsearch:9200'}',\n  'index' = '${p.index || 'my-index'}'\n);`;
            case 'print_sink': {
                const srcNode = window._plmState?.canvas?.nodes?.find(n => (window.PM_OPERATORS||[]).find(o => o.id === n.opId)?.isSource);
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'print'${p.print_identifier?`,\n  'print-identifier' = '${p.print_identifier}'`:''}\n) LIKE ${srcNode?.params?.table_name || 'source_table'} (EXCLUDING ALL);`;
            }
            case 'blackhole_sink': {
                const srcNode = window._plmState?.canvas?.nodes?.find(n => (window.PM_OPERATORS||[]).find(o => o.id === n.opId)?.isSource);
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'blackhole'\n) LIKE ${srcNode?.params?.table_name || 'source_table'} (EXCLUDING ALL);`;
            }
            case 'mongodb_sink':
                return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'mongodb',\n  'uri' = '${p.uri || 'mongodb://localhost:27017/mydb'}',\n  'collection' = '${p.collection || 'my-collection'}'\n);`;
            case 'result_output': return '';
            default: return `-- Operator: ${node.label} (${node.opId})`;
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 4 — INSERT SQL: enumerate cols (no SELECT * bug)
    // ─────────────────────────────────────────────────────────────────
    function _buildInsertSqlPatched(sources, sinks, nodes, edges) {
        if (!sources.length || !sinks.length) return '';
        const src  = sources[0]; const sink = sinks[0];
        const PM_OPS = window.PM_OPERATORS || [];
        const srcName  = src.params?.table_name  || 'source_table';
        const sinkName = sink.params?.table_name || 'sink_table';
        const transforms = nodes.filter(n => { const o = PM_OPS.find(op => op.id === n.opId); return o && !o.isSource && !o.isSink; });
        const rawSchema  = src.params?.schema || '';
        const schemaCols = rawSchema.split('\n').map(l => l.trim().split(/\s+/)[0]).filter(Boolean);
        const allCols    = schemaCols.length ? schemaCols.join(', ') : '*';
        let selectCols   = allCols;
        let fromClause   = srcName;
        let whereClauses = [];
        let groupBy      = null;
        transforms.forEach(node => {
            const np = node.params || {};
            switch (node.opId) {
                case 'filter':     if (np.condition) whereClauses.push(np.condition); break;
                case 'project':    if (np.columns) selectCols = np.columns.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  '); break;
                case 'map_udf': case 'udf_node':
                    const fn = np.function_name || np.udf_name; const ic = np.input_col || np.input_cols; const oa = np.output_alias;
                    if (fn && ic && oa) { const pc = np.extra_cols ? np.extra_cols.split(',').map(c=>c.trim()).filter(Boolean).join(', ') : allCols; selectCols = pc + ',\n  ' + fn + '(' + ic + ') AS ' + oa; }
                    break;
                case 'tumble_window':
                    fromClause = `TABLE(TUMBLE(TABLE ${fromClause}, DESCRIPTOR(${np.time_col}), INTERVAL '${np.window_size||'1 MINUTE'}'))`;
                    if (np.aggregations) selectCols = 'window_start, window_end,\n  ' + np.aggregations.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  ');
                    groupBy = 'window_start, window_end' + (np.group_by ? ', ' + np.group_by : '');
                    break;
                case 'aggregate':
                    if (np.aggregations) selectCols = (np.group_by?np.group_by+',\n  ':'') + np.aggregations.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  ');
                    groupBy = np.group_by || null; break;
            }
        });
        let sql = `INSERT INTO ${sinkName}\nSELECT\n  ${selectCols}\nFROM ${fromClause}`;
        if (whereClauses.length) sql += '\nWHERE ' + whereClauses.join(' AND ');
        if (groupBy) sql += '\nGROUP BY ' + groupBy;
        sql += ';';
        return sql;
    }

    // Main SQL generator
    function _plmGenerateSqlWithCatalog() {
        const { nodes, edges } = window._plmState?.canvas || { nodes: [], edges: [] };
        if (!nodes.length) return '-- Add operators to the canvas to generate SQL';
        const PM_OPS = window.PM_OPERATORS || [];
        const lines  = [];
        lines.push('-- ══════════════════════════════════════════════════════');
        lines.push('-- Pipeline: ' + (window._plmState?.activePipeline?.name || 'Untitled'));
        lines.push('-- Generated by Str:::lab Studio Pipeline Manager v2.1');
        lines.push('-- ══════════════════════════════════════════════════════\n');
        const ps = window._plmState?.pipelineSettings;
        if (ps && typeof _plmBuildSettingsSql === 'function') lines.push(_plmBuildSettingsSql(ps) + '\n');
        // Topological sort
        const inDeg = {}, childMap = {};
        nodes.forEach(n => { inDeg[n.uid]=0; childMap[n.uid]=[]; });
        edges.forEach(e => { if (inDeg[e.toUid]!==undefined) inDeg[e.toUid]++; if (childMap[e.fromUid]) childMap[e.fromUid].push(e.toUid); });
        let queue = nodes.filter(n => inDeg[n.uid]===0).map(n => n.uid);
        const order = [], tmpCount = { ...inDeg };
        while (queue.length) { const uid = queue.shift(); order.push(uid); (childMap[uid]||[]).forEach(cid => { tmpCount[cid]--; if (tmpCount[cid]===0) queue.push(cid); }); }
        const orderedNodes = order.map(uid => nodes.find(n => n.uid===uid)).filter(Boolean);
        nodes.filter(n => !order.includes(n.uid)).forEach(n => orderedNodes.push(n));
        const sources = nodes.filter(n => PM_OPS.find(o => o.id===n.opId)?.isSource);
        const sinks   = nodes.filter(n => PM_OPS.find(o => o.id===n.opId)?.isSink);
        orderedNodes.filter(n => PM_OPS.find(o => o.id===n.opId)?.isSource).forEach(n => { const sql = _plmNodeToSqlPatched(n); if (sql) { lines.push(sql); lines.push(''); } });
        orderedNodes.filter(n => PM_OPS.find(o => o.id===n.opId)?.isSink).forEach(n => { const sql = _plmNodeToSqlPatched(n); if (sql) { lines.push(sql); lines.push(''); } });
        if (sources.length && sinks.length) {
            lines.push('-- ──────────────────────────────────────────────────────');
            lines.push('-- Pipeline execution');
            lines.push('-- ──────────────────────────────────────────────────────');
            const insertSql = _buildInsertSqlPatched(sources, sinks, nodes, edges);
            if (insertSql) lines.push(insertSql);
        }
        return lines.join('\n');
    }

    window._plmGenerateSqlWithCatalog = _plmGenerateSqlWithCatalog;

    const _origUpdateSqlPreview = window._plmUpdateSqlPreview;
    if (_origUpdateSqlPreview && !_origUpdateSqlPreview._catalogPatched) {
        window._plmUpdateSqlPreview = function() {
            const sql = _plmGenerateSqlWithCatalog();
            const prevEl = document.getElementById('plm-sql-preview');
            const fullEl = document.getElementById('plm-sql-full');
            if (prevEl) prevEl.textContent = sql;
            if (fullEl) fullEl.textContent = sql;
        };
        window._plmUpdateSqlPreview._catalogPatched = true;
    }
    window._plmUpdateSqlView = window._plmUpdateSqlPreview;

    // ─────────────────────────────────────────────────────────────────
    // FIX 5 — activeDatabase fix in catalog execute
    // ─────────────────────────────────────────────────────────────────
    function _patchCatExecute() {
        if (typeof window._catExecute !== 'function') return false;
        if (window._catExecute._sidebarPatched) return true;
        const _orig = window._catExecute;
        window._catExecute = async function() {
            await _orig.apply(this, arguments);
            setTimeout(async () => {
                const name        = (document.getElementById('cat-name-input')?.value || '').trim();
                const switchAfter = document.getElementById('cat-switch-after')?.value || 'yes';
                if (typeof refreshCatalog === 'function') refreshCatalog();
                if (name && switchAfter === 'yes') {
                    let actualDb = 'default';
                    try { const rows = await _runSingleStmt(`SHOW DATABASES IN \`${name}\``); if (rows.length) actualDb = String(rows[0][0]||'default'); } catch(_) {}
                    if (typeof state !== 'undefined') { state.activeCatalog=name; state.activeDatabase=actualDb; }
                    if (typeof updateCatalogStatus === 'function') updateCatalogStatus(name, actualDb);
                    ['refreshCatalogBrowser','loadCatalogTree','buildCatalogTree'].forEach(fn => { if (typeof window[fn]==='function') try { window[fn](); } catch(_) {} });
                }
            }, 900);
        };
        window._catExecute._sidebarPatched = true;
        return true;
    }
    if (!_patchCatExecute()) { const t = setInterval(() => { if (_patchCatExecute()) clearInterval(t); }, 400); }

    // ─────────────────────────────────────────────────────────────────
    // FIX 6 — STOP BUTTON STYLE
    // ─────────────────────────────────────────────────────────────────
    if (!document.getElementById('plm-stop-btn-patch-css')) {
        const s = document.createElement('style'); s.id='plm-stop-btn-patch-css';
        s.textContent = `#plm-float-stop-btn{background:linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12))!important;backdrop-filter:blur(8px)!important;border:1px solid rgba(0,212,170,0.45)!important;color:#00d4aa!important;padding:5px 14px!important;border-radius:6px!important;font-size:10px!important;font-weight:700!important;letter-spacing:0.8px!important;text-transform:uppercase!important;}#plm-float-stop-btn:hover{background:linear-gradient(135deg,rgba(0,212,170,0.28),rgba(0,180,200,0.20))!important;border-color:rgba(0,212,170,0.7)!important;}`;
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
                const fb = document.getElementById('plm-float-stop-btn'); if (!fb) return;
                if (window._plmState?.animating) { fb.style.cssText='display:block;position:absolute;top:8px;left:50%;transform:translateX(-50%);z-index:30;background:linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12));backdrop-filter:blur(8px);border:1px solid rgba(0,212,170,0.45);color:#00d4aa;cursor:pointer;padding:5px 14px;border-radius:6px;font-size:10px;font-weight:700;font-family:var(--mono);letter-spacing:0.8px;white-space:nowrap;text-transform:uppercase;'; fb.innerHTML='<svg width="9" height="9" viewBox="0 0 12 12" fill="currentColor" style="margin-right:5px;vertical-align:middle;"><rect x="1" y="1" width="10" height="10" rx="1"/></svg>Stop'; }
                else { fb.style.display='none'; }
            };
            window._plmSyncRunBtn._stopPatched = true;
        }
        return true;
    }

    // ─────────────────────────────────────────────────────────────────
    // FIX 7 — UDF DROPDOWN LIVE RELOAD
    // ─────────────────────────────────────────────────────────────────
    window._plmGetUdfs = function() {
        try {
            const a1 = JSON.parse(localStorage.getItem('strlabstudio_udfs') || '[]');
            const a2 = JSON.parse(localStorage.getItem('strlabstudio_udf_registry') || '[]');
            const seen = new Set(), merged = [];
            for (const u of [...a1, ...a2]) { const n = u.name||u.functionName||''; if (n && !seen.has(n.toLowerCase())) { seen.add(n.toLowerCase()); merged.push(u); } }
            return merged;
        } catch(_) { return []; }
    };

    // ─────────────────────────────────────────────────────────────────
    // CDC OPERATORS — inject into PM_OPERATORS
    // ─────────────────────────────────────────────────────────────────
    function _injectCdcOperators() {
        if (!window.PM_OPERATORS) return false;

        // CDC SOURCE
        if (!window.PM_OPERATORS.find(o => o.id === 'cdc_source')) {
            window.PM_OPERATORS.push({
                id: 'cdc_source', label: 'CDC Source', category: 'Sources',
                isSource: true, isSink: false, color: '#e84393', icon: '⟳',
                desc: 'Change Data Capture source. Streams INSERT/UPDATE/DELETE from MySQL or PostgreSQL as a Flink changelog stream.',
                aboutHtml: `<p><strong>CDC (Change Data Capture)</strong> streams every row-level change from your database as a Flink changelog event.</p>
<p><strong>Requires:</strong> mysql-cdc or postgres-cdc JAR in <code>/opt/flink/lib/</code>. Download from <a href="https://github.com/apache/flink-cdc/releases" target="_blank">Flink CDC Releases</a>.</p>
<p><strong>MySQL prerequisites:</strong><br><code>GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';</code><br>Also enable binlog: <code>binlog_format=ROW, binlog_row_image=FULL</code> in my.cnf.</p>
<p><strong>PostgreSQL prerequisites:</strong><br><code>ALTER SYSTEM SET wal_level = logical;</code><br><code>SELECT pg_create_logical_replication_slot('flink_slot','pgoutput');</code></p>
<p>CDC produces a <strong>changelog stream</strong> — the sink must support changelog (e.g. JDBC sink with primary key, upsert-kafka).</p>`,
                params: [
                    { id:'table_name',    label:'Table Name *',           type:'text',   placeholder:'cdc_orders',   required:true },
                    { id:'cdc_type',      label:'CDC Connector *',        type:'select',  options:['mysql-cdc','postgres-cdc','sqlserver-cdc','oracle-cdc'], required:true },
                    { id:'hostname',      label:'Hostname *',             type:'text',   placeholder:'mysql-host',   required:true },
                    { id:'port',          label:'Port',                   type:'text',   placeholder:'3306 or 5432' },
                    { id:'database_name', label:'Database Name *',        type:'text',   placeholder:'mydb',         required:true },
                    { id:'db_table',      label:'Table / Pattern *',      type:'text',   placeholder:'orders',       required:true, hint:'MySQL supports regex: mydb\\..*' },
                    { id:'username',      label:'Username *',             type:'text',   placeholder:'flink_user',   required:true },
                    { id:'password',      label:'Password',               type:'password', placeholder:'••••••••' },
                    { id:'server_id',     label:'Server ID (MySQL only)', type:'text',   placeholder:'5401-5404',    hint:'Range recommended for parallel reading.' },
                    { id:'schema_name',   label:'Schema (PG only)',       type:'text',   placeholder:'public',       hint:'PostgreSQL schema name.' },
                    { id:'slot_name',     label:'Slot Name (PG only)',    type:'text',   placeholder:'flink_slot',   hint:'Must match the replication slot you created.' },
                    { id:'plugin_name',   label:'Plugin (PG only)',       type:'select',  options:['pgoutput','decoderbufs','wal2json'], hint:'pgoutput is built-in (PG 10+).' },
                    { id:'watermark',     label:'Watermark Column',       type:'text',   placeholder:'updated_at' },
                    { id:'watermark_delay',label:'Watermark Delay (s)',   type:'text',   placeholder:'5' },
                    { id:'schema',        label:'Schema (columns)',       type:'textarea', placeholder:'id BIGINT\nname STRING\nupdated_at TIMESTAMP(3)', hint:'Define output columns matching your DB table.' },
                ],
            });
        }

        // CDC SINK
        if (!window.PM_OPERATORS.find(o => o.id === 'cdc_sink')) {
            window.PM_OPERATORS.push({
                id: 'cdc_sink', label: 'CDC Sink', category: 'Sinks',
                isSource: false, isSink: true, color: '#e84393', icon: '⟳→',
                desc: 'Materialises a CDC changelog stream into a JDBC table or an upsert-Kafka topic.',
                aboutHtml: `<p>The CDC Sink consumes a <strong>changelog stream</strong> (from a CDC Source) and writes it to a downstream system.</p>
<p><strong>JDBC Sink mode</strong>: Writes upserts into a relational table. The target table must have a primary key. Requires the JDBC connector JAR + DB driver in <code>/opt/flink/lib/</code>.</p>
<p><strong>Upsert-Kafka mode</strong>: Writes the changelog as upsert messages to a Kafka topic. The Kafka connector JAR is required. Downstream consumers can use the topic as a changelog source.</p>`,
                params: [
                    { id:'table_name',        label:'Table Name *',       type:'text',     placeholder:'cdc_sink',        required:true },
                    { id:'sink_type',          label:'Sink Type *',        type:'select',   options:['jdbc','upsert-kafka'], required:true },
                    { id:'jdbc_url',           label:'JDBC URL (jdbc mode)', type:'text',  placeholder:'jdbc:postgresql://localhost/mydb' },
                    { id:'db_table',           label:'DB Table (jdbc mode)', type:'text',  placeholder:'output_table' },
                    { id:'username',           label:'Username (jdbc)',    type:'text',     placeholder:'flink_user' },
                    { id:'password',           label:'Password (jdbc)',    type:'password', placeholder:'••••••••' },
                    { id:'bootstrap_servers',  label:'Brokers (upsert-kafka)', type:'text', placeholder:'kafka:9092' },
                    { id:'topic',              label:'Topic (upsert-kafka)', type:'text',  placeholder:'cdc-output' },
                    { id:'schema',             label:'Schema (columns)',   type:'textarea', placeholder:'id BIGINT\nname STRING\nupdated_at TIMESTAMP(3)', hint:'Must include primary key column.' },
                ],
            });
        }

        return true;
    }

    _injectCdcOperators();
    if (!_injectCdcOperators()) { const t = setInterval(() => { if (_injectCdcOperators()) clearInterval(t); }, 300); }

    // ─────────────────────────────────────────────────────────────────
    // INIT
    // ─────────────────────────────────────────────────────────────────
    let attempts = 0;
    const initTimer = setInterval(() => { attempts++; if (_patchStopBtn()) clearInterval(initTimer); if (attempts > 60) clearInterval(initTimer); }, 300);

    const _origOpenPLM = window.openPipelineManager;
    if (_origOpenPLM && !_origOpenPLM._patchesApplied) {
        window.openPipelineManager = function() {
            _origOpenPLM.apply(this, arguments);
            _injectCdcOperators();
            setTimeout(_patchStopBtn, 200);
        };
        window.openPipelineManager._patchesApplied = true;
    }

    console.log('[PLM Patches v1.6.0] loaded — JDBC-only catalog selector, CDC source + sink operators, all prior fixes retained');
})();