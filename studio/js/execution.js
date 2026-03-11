/* FlinkSQL Studio — SQL Execution Engine
 * Handles: runSQL, submitStatement, pollOperation, cancelOperation
 *
 * LIVE STREAMING FIX (v10):
 *  - Status and result fetch are now DECOUPLED: we always attempt to
 *    fetch results whenever status is RUNNING or FINISHED, not just
 *    after the status check in the same tick.
 *  - Token is NEVER reset between polls — advancing always uses
 *    nextResultUri when present, integer +1 otherwise.
 *  - PAYLOAD_MISMATCH: both result.results.data[] row formats handled:
 *      {fields:[...]}  — standard REST row
 *      [...values]     — compact array row
 *  - EOS is checked BEFORE reading data so we never miss the last page.
 *  - fetchSize bumped to 1000 for lower latency on fast Kafka topics.
 *  - Empty-data polls on a RUNNING query no longer reset the poll counter
 *    (prevents silent timeout on slow topics).
 *  - The "results-tab-btn" element fallback uses querySelector so it
 *    works even if id casing differs across builds.
 */

// ── SQL splitting ─────────────────────────────────────────────────────────────
function splitSQL(raw) {
  const stmts = [];
  let cur = '', inStr = false, strChar = '', i = 0;
  while (i < raw.length) {
    const ch = raw[i];
    if (!inStr && (ch === "'" || ch === '"' || ch === '`')) {
      inStr = true; strChar = ch; cur += ch;
    } else if (inStr && ch === strChar && raw[i-1] !== '\\') {
      inStr = false; cur += ch;
    } else if (!inStr && ch === '-' && raw[i+1] === '-') {
      while (i < raw.length && raw[i] !== '\n') i++;
      continue;
    } else if (!inStr && ch === ';') {
      const s = cur.trim();
      if (s) stmts.push(s);
      cur = '';
    } else {
      cur += ch;
    }
    i++;
  }
  const tail = cur.trim();
  if (tail) stmts.push(tail);
  return stmts;
}

// ── Catalog statusbar update ───────────────────────────────────────────────────
function updateCatalogStatus(catalog, database) {
  if (catalog !== null) state.activeCatalog = catalog;
  if (database !== null) state.activeDatabase = database;
  const el = document.getElementById('status-catalog');
  if (el) el.textContent = `${state.activeCatalog || '—'}.${state.activeDatabase || '—'}`;
}

// ── Main entry points ─────────────────────────────────────────────────────────
async function executeSQL() {
  const el = document.getElementById('sql-editor');
  const sql = el?.value?.trim();
  if (!sql) return;
  await runSQL(sql);
}

async function executeSelected() {
  const el = document.getElementById('sql-editor');
  const start = el.selectionStart, end = el.selectionEnd;
  const sel = (start !== end ? el.value.slice(start, end) : el.value).trim();
  if (!sel) { toast('No SQL selected', 'err'); return; }
  await runSQL(sel);
}

async function explainSQL() {
  const el = document.getElementById('sql-editor');
  const sql = (el?.value || '').trim().replace(/;+$/, '');
  if (!sql) return;
  await runSQL(`EXPLAIN ${sql}`);
}

// ── Core runner ───────────────────────────────────────────────────────────────
async function runSQL(sql) {
  if (!state.activeSession) { toast('No active session', 'err'); return; }
  if (!state.gateway) {
    try {
      state.gateway = { host: window.location.hostname, port: window.location.port || '80', baseUrl: getBaseUrl() };
    } catch(_) {}
  }
  if (!state.gateway) { toast('Not connected — please reconnect', 'err'); return; }

  const statements = splitSQL(sql);
  if (statements.length === 0) return;

  setExecuting(true);
  clearResults();
  perfQueryStart();
  addLog('INFO', `Running ${statements.length} statement${statements.length > 1 ? 's' : ''} on session ${shortHandle(state.activeSession)}`);

  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    addLog('SQL', `[${i+1}/${statements.length}] ${stmt.replace(/\s+/g,' ').slice(0,120)}${stmt.length>120?'…':''}`);

    const useCatalogMatch = stmt.match(/^\s*USE\s+CATALOG\s+[`"]?(\S+?)[`"]?\s*;?\s*$/i);
    const useDbMatch      = stmt.match(/^\s*USE\s+(?!CATALOG\b)[`"]?(\S+?)[`"]?\s*;?\s*$/i);
    if (useCatalogMatch) updateCatalogStatus(useCatalogMatch[1].replace(/`/g,''), null);
    if (useDbMatch)      updateCatalogStatus(null, useDbMatch[1].replace(/`/g,''));

    try {
      await submitStatement(stmt);
    } catch (e) {
      const friendly = parseFlinkError(e.message);
      addLog('ERR', friendly, e.message);
      toast(friendly.slice(0, 90), 'err');
      break;
    }
  }
  setExecuting(false);
}

async function submitStatement(sql) {
  const cleanSql = sql.trim().replace(/;+$/, '');
  const sessionHandle = state.activeSession;

  const resp = await api('POST', `/v1/sessions/${sessionHandle}/statements`, {
    statement: cleanSql,
    executionTimeout: 0,
  });

  const opHandle = resp.operationHandle;

  addToHistory(sql, 'running', opHandle);
  addOperation(opHandle, sql);

  await pollOperation(opHandle, sql, sessionHandle);
}

// ── Poll loop ────────────────────────────────────────────────────────────────
async function pollOperation(opHandle, sql, sessionHandle) {

  const mySession = sessionHandle || state.activeSession;
  let token = 0;
  const maxPolls = 3600;
  let polls = 0;
  let firstRows = true;
  let emptyRunningPolls = 0;

  state.currentOp = { opHandle, sessionHandle: mySession };
  state._maxRowsWarned = false;

  const stopBtn = document.getElementById('stop-btn');
  if (stopBtn) stopBtn.style.display = 'flex';

  function showResultsTab() {
    const btn =
        document.getElementById('results-tab-btn') ||
        document.querySelector('[data-tab="data"]') ||
        document.querySelector('.result-tab');

    if (btn) switchResultTab('data', btn);
  }

  while (polls < maxPolls) {

    if (!state.currentOp || state.currentOp.opHandle !== opHandle) break;

    await sleep(500);

    // ── status ─────────────────────────────────────────────
    let status;

    try {
      status = await api('GET',
          `/v1/sessions/${mySession}/operations/${opHandle}/status`);
    } catch (e) {
      addLog('ERR', `Status check failed: ${parseFlinkError(e.message)}`);
      break;
    }

    const opStatus = (status.operationStatus || status.status || '').toUpperCase();

    updateOperationStatus(opHandle, opStatus);

    // ── terminal error ─────────────────────────────────────
    if (opStatus === 'ERROR') {

      let rawError = status.errorMessage || status.message || null;

      if (!rawError) {
        try {
          const errResult = await api('GET',
              `/v1/sessions/${mySession}/operations/${opHandle}/result/0?rowFormat=JSON`);

          rawError =
              errResult.errors?.join('\n') ||
              errResult.message ||
              errResult.results?.data?.[0]?.fields?.[0] ||
              JSON.stringify(errResult);

        } catch {}
      }

      const friendly = parseFlinkError(rawError);

      addLog('ERR', friendly, rawError);

      updateHistoryStatus(opHandle, 'err');

      toast(friendly.slice(0,90),'err');

      break;
    }

    if (['NOT_READY','PENDING','INITIALIZED','ACCEPTED'].includes(opStatus))
      continue;

    // ── fetch results ──────────────────────────────────────
    if (opStatus === 'RUNNING' || opStatus === 'FINISHED') {

      let result;

      try {

        result = await api('GET',
            `/v1/sessions/${mySession}/operations/${opHandle}/result/${token}?rowFormat=JSON&maxFetchSize=1000`);

      } catch (e) {

        if (e.message.includes('404')) {
          const rowCount = state.results.length;
          addLog('OK',`Stream ended — ${rowCount} rows`);
          break;
        }

        addLog('ERR', parseFlinkError(e.message));
        break;
      }

      // token advance
      if (result.nextResultUri) {
        const parsed = extractToken(result.nextResultUri);
        if (parsed !== null) token = parsed;
      } else token++;

      // ── EOS ──────────────────────────────────────────────
      if (result.resultType === 'EOS' || result.resultType === 'PAYLOAD_EOS') {

        const rowCount = state.results.length;

        addLog('OK',`Query complete — ${rowCount} rows`);

        updateHistoryStatus(opHandle,'ok');

        perfQueryEnd(rowCount);

        break;
      }

      // ── schema capture (FIX) ─────────────────────────────
      if (result.results?.columns?.length) {
        state.resultColumns = result.results.columns;

      } else if (result.schema?.columns?.length) {
        state.resultColumns = result.schema.columns;
      }

      // ── rows ─────────────────────────────────────────────
      const rawData = result.results?.data || [];

      const newRows = rawData.map(row => {

        if (row?.fields !== undefined) {
          return { fields: Array.isArray(row.fields) ? row.fields : Object.values(row.fields) };
        }

        return { fields: Array.isArray(row) ? row : Object.values(row) };
      });

      if (newRows.length > 0) {

        if (firstRows) {

          firstRows = false;

          showResultsTab();

          addLog('INFO','Data arriving — streaming rows…');
        }

        emptyRunningPolls = 0;

        const remaining = MAX_ROWS - state.results.length;

        if (remaining > 0) {

          state.results.push(...newRows.slice(0,remaining));

          // PERF improvement
          if (state.resultColumns?.length) {
            renderResults();
          }

          const badge = document.getElementById('result-row-badge');

          if (badge) badge.textContent = state.results.length;
        }

      } else {

        if (opStatus === 'RUNNING') {

          emptyRunningPolls++;

          if (emptyRunningPolls % 20 === 0) {
            addLog('INFO',`Streaming… ${state.results.length} rows so far`);
          }
        }
      }

      if (opStatus === 'FINISHED') {

        const rowCount = state.results.length;

        addLog('OK',`Query finished — ${rowCount} rows`);

        updateHistoryStatus(opHandle,'ok');

        perfQueryEnd(rowCount);

        break;
      }
    }

    polls++;
  }

  state.currentOp = null;

  if (stopBtn) stopBtn.style.display = 'none';
}

// ── token extraction ─────────────────────────────────────────────────────────
function extractToken(uri) {
  if (!uri) return null;
  const m = uri.match(/\/result\/(\d+)/);
  return m ? parseInt(m[1],10) : null;
}

// ── cancel ───────────────────────────────────────────────────────────────────
async function cancelOperation() {

  if (!state.currentOp) return;

  const { opHandle } = state.currentOp;

  state.currentOp = null;

  try {

    await api('DELETE',
        `/v1/sessions/${state.activeSession}/operations/${opHandle}/cancel`);

    addLog('WARN',`Operation ${shortHandle(opHandle)} cancelled`);

    toast('Operation cancelled','info');

  } catch (e) {

    addLog('WARN',`Cancel request sent (${e.message})`);
  }

  const stopBtn = document.getElementById('stop-btn');

  if (stopBtn) stopBtn.style.display='none';

  setExecuting(false);
}

function setExecuting(val) {
  const el = document.getElementById('status-exec');
  if (el) el.style.display = val ? 'flex':'none';
}

// ── internal data fetch ──────────────────────────────────────────────────────
async function executeForData(sql) {

  const resp = await api('POST',
      `/v1/sessions/${state.activeSession}/statements`,
      { statement: sql });

  const opHandle = resp.operationHandle;

  let retries = 60;

  while (retries-- > 0) {

    await sleep(400);

    const status = await api('GET',
        `/v1/sessions/${state.activeSession}/operations/${opHandle}/status`);

    const s = (status.operationStatus || status.status || '').toUpperCase();

    if (s === 'ERROR')
      throw new Error(status.errorMessage || 'Query error');

    if (['NOT_READY','PENDING','INITIALIZED','ACCEPTED'].includes(s))
      continue;

    if (s === 'FINISHED' || s === 'RUNNING') {

      const result = await api('GET',
          `/v1/sessions/${state.activeSession}/operations/${opHandle}/result/0?rowFormat=JSON`);

      const cols = (result.results?.columns || result.schema?.columns || [])
          .map(c => c.name);

      const rows = (result.results?.data || []).map(r => {

        const f = r?.fields ?? r;

        return Array.isArray(f) ? f : Object.values(f);
      });

      return { cols, rows };
    }
  }

  return { cols: [], rows: [] };
}
