// RESULTS RENDERING
// ──────────────────────────────────────────────
function renderResults() {
  const wrap = document.getElementById('result-table-wrap');
  const cols = state.resultColumns;
  // Apply search filter
  const q = (state.resultSearch || '').toLowerCase().trim();
  let rows = state.results;
  if (q) {
    rows = rows.filter(row => {
      const fields = Array.isArray(row?.fields) ? row.fields : Object.values(row?.fields || row || {});
      return fields.some(v => String(v ?? '').toLowerCase().includes(q));
    });
  }
  // Apply sort direction — newest-first reverses the array display
  const displayRows = state.resultNewestFirst ? [...rows].reverse() : rows;
  const page = state.resultPage;
  const start = page * state.pageSize;
  const pageRows = displayRows.slice(start, start + state.pageSize);

  if (rows.length === 0) {
    wrap.innerHTML = '<div class="empty-state"><div class="icon">⚡</div><div class="msg">Query returned no rows.</div></div>';
    document.getElementById('result-pagination').style.display = 'none';
    document.getElementById('result-row-badge').textContent = '0';
    return;
  }

  document.getElementById('result-row-badge').textContent = rows.length > 999 ? '999+' : rows.length;

  let html = '<table class="result-table"><thead><tr><th><span class="row-index">#</span></th>';
  cols.forEach(c => {
    html += `<th>${c.name}<span class="col-type">${c.logicalType?.type || c.type || ''}</span></th>`;
  });
  html += '</tr></thead><tbody>';

  pageRows.forEach((row, ri) => {
    // Show original row number (account for reversed display)
    const origIdx = state.resultNewestFirst
      ? (rows.length - 1 - (start + ri))
      : (start + ri);
    html += `<tr><td class="row-index">${origIdx + 1}</td>`;
    const rawFields = (row && row.fields !== undefined) ? row.fields : row;
    const fieldArr = Array.isArray(rawFields) ? rawFields : Object.values(rawFields || {});
    fieldArr.forEach((val, ci) => {
      const colType = (cols[ci]?.logicalType?.type || cols[ci]?.type || '').toUpperCase();
      let cls = '';
      if (val === null || val === undefined) cls = 'null-val';
      else if (['INT','BIGINT','DOUBLE','FLOAT','DECIMAL','INTEGER','TINYINT','SMALLINT'].some(t => colType.includes(t))) cls = 'num-val';
      else if (colType.includes('BOOL')) cls = 'bool-val';
      else if (colType.includes('TIMESTAMP') || colType.includes('TIME') || colType.includes('DATE')) cls = 'ts-val';
      // Pretty-print JSON values (common from Kafka JSON format)
      let display = val === null ? 'NULL' : String(val);
      if (display.startsWith('{') || display.startsWith('[')) {
        try {
          const parsed = JSON.parse(display);
          // Flatten one level for readability in the cell
          if (typeof parsed === 'object' && !Array.isArray(parsed)) {
            display = Object.entries(parsed)
              .map(([k,v]) => `${k}: ${typeof v === 'object' ? JSON.stringify(v) : v}`)
              .join('  |  ');
            cls += ' json-val';
          }
        } catch(_) {}
      }
      html += `<td class="${cls}" title="${String(val===null?'NULL':val).replace(/"/g,'&quot;').replace(/</g,'&lt;')}">${display}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // Pagination
  const totalPages = Math.ceil(displayRows.length / state.pageSize);
  const pag = document.getElementById('result-pagination');
  if (totalPages > 0) {
    if (pag) pag.style.display = 'flex';
    const pi = document.getElementById('page-info');
    if (pi) pi.textContent = `Page ${page + 1} of ${Math.max(1,totalPages)}`;
    const pp = document.getElementById('page-prev');
    if (pp) pp.disabled = page === 0;
    const pn = document.getElementById('page-next');
    if (pn) pn.disabled = page >= totalPages - 1;
    const matchText = q ? ` · ${rows.length} match` : '';
    const rs = document.getElementById('result-stats');
    if (rs) rs.textContent = `${state.results.length} total rows · ${cols.length} cols${matchText}`;
  } else {
    if (pag) pag.style.display = 'none';
  }
  // Render search + sort toolbar
  _renderResultsToolbar(cols, rows);

  // NOTE: Do NOT call switchResultTab here — renderResults is called
  // on every streaming tick and forcing a tab switch would prevent the user
  // from navigating to Log, Job Graph, or Performance while data streams in.
  // Tab switching is handled by showResultsTab() in pollOperation (firstRows only).
}

function changePage(dir) {
  const totalPages = Math.ceil(state.results.length / state.pageSize);
  state.resultPage = Math.max(0, Math.min(state.resultPage + dir, totalPages - 1));
  renderResults();
}

// ── Results search + sort toolbar ────────────────────────────────────────────
function _renderResultsToolbar(cols, filteredRows) {
  let tb = document.getElementById('results-sort-toolbar');
  if (!tb) {
    const wrap = document.getElementById('result-data-tab');
    if (!wrap) return;
    tb = document.createElement('div');
    tb.id = 'results-sort-toolbar';
    tb.style.cssText = `
      display:flex; align-items:center; gap:6px; padding:4px 8px;
      background:var(--bg2); border-bottom:1px solid var(--border);
      flex-shrink:0; min-height:32px;
    `;
    // Insert after stream-selector-bar if it exists, else at top
    const streamBar = document.getElementById('stream-selector-bar');
    if (streamBar && streamBar.nextSibling) {
      wrap.insertBefore(tb, streamBar.nextSibling);
    } else {
      wrap.insertBefore(tb, wrap.firstChild);
    }
  }

  const q = state.resultSearch || '';
  const newest = state.resultNewestFirst;
  const totalRows = state.results.length;
  const filtered = filteredRows.length;

  tb.innerHTML = `
    <input id="result-search-input" type="text" placeholder="🔍 Search rows…" value="${escHtml(q)}"
      style="flex:1;min-width:0;max-width:220px;padding:2px 7px;font-size:11px;
             font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);
             border-radius:3px;color:var(--text0);outline:none;"
      oninput="state.resultSearch=this.value;state.resultPage=0;renderResults();"
      onkeydown="event.stopPropagation();">
    ${q ? `<span style="font-size:10px;color:var(--text2);">${filtered}/${totalRows}</span>
           <button onclick="state.resultSearch='';state.resultPage=0;renderResults();"
             style="padding:1px 5px;font-size:10px;background:none;border:1px solid var(--border);
                    color:var(--text2);border-radius:2px;cursor:pointer;">✕</button>` : ''}
    <button id="sort-dir-btn" title="${newest?'Newest first (click for oldest first)':'Oldest first (click for newest first)'}"
      onclick="state.resultNewestFirst=!state.resultNewestFirst;state.resultPage=0;renderResults();"
      style="padding:2px 8px;font-size:10px;font-family:var(--mono);cursor:pointer;
             background:${newest?'rgba(0,212,170,0.1)':'var(--bg3)'};
             border:1px solid ${newest?'rgba(0,212,170,0.35)':'var(--border)'};
             color:${newest?'var(--accent)':'var(--text2)'};border-radius:3px;white-space:nowrap;">
      ${newest?'↓ Newest first':'↑ Oldest first'}
    </button>
    <button onclick="clearCurrentSlotResults();"
      title="Clear results — start fresh"
      style="padding:2px 8px;font-size:10px;font-family:var(--mono);cursor:pointer;
             background:var(--bg3);border:1px solid var(--border);color:var(--text2);border-radius:3px;">
      ⌫ Clear
    </button>
    <button onclick="openResultsReportModal();"
      title="Generate PDF report from current results"
      style="padding:2px 8px;font-size:10px;font-family:var(--mono);cursor:pointer;
             background:rgba(0,212,170,0.08);border:1px solid rgba(0,212,170,0.3);
             color:var(--accent);border-radius:3px;margin-left:auto;">
      📊 Report
    </button>
  `;
}

function clearCurrentSlotResults() {
  // Clear only the active slot's rows (doesn't stop streaming — just empties display)
  const slot = (state.resultSlots || []).find(s => s.id === state.activeSlot);
  if (slot) { slot.rows = []; }
  state.results = [];
  state.resultPage = 0;
  state._maxRowsWarned = false;
  renderResults();
  const badge = document.getElementById('result-row-badge');
  if (badge) badge.textContent = '0';
  addLog('INFO', 'Results cleared — new rows will appear as they arrive.');
}


// ── Results table → PDF report modal ────────────────────────────────────────
function openResultsReportModal() {
  const existing = document.getElementById('results-report-modal');
  if (existing) existing.remove();

  // Get rows from the active slot
  const slot = (state.resultSlots || []).find(s => s.id === state.activeSlot);
  const allRows = slot ? slot.rows : (state.results || []);
  const rowCount = allRows.length;

  if (rowCount === 0) {
    toast('No result rows to export — run a query first', 'err');
    return;
  }

  const modal = document.createElement('div');
  modal.id = 'results-report-modal';
  modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;z-index:10000;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:var(--bg2);border:1px solid var(--border2);border-radius:6px;padding:24px;min-width:340px;max-width:480px;font-family:var(--mono);">
      <div style="font-size:14px;font-weight:700;color:var(--text0);margin-bottom:4px;">📊 Export Results as PDF</div>
      <div style="font-size:10px;color:var(--text2);margin-bottom:16px;">${rowCount.toLocaleString()} rows available in current stream</div>

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Row range (leave blank = all rows, max 5000 for PDF):</label>
      <div style="display:flex;gap:8px;margin-bottom:12px;">
        <input id="rr-from" type="number" placeholder="From row" min="1" value="1"
          style="flex:1;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);">
        <input id="rr-to" type="number" placeholder="To row" value="${Math.min(rowCount, 500)}"
          style="flex:1;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);">
      </div>

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Filter by value (searches all columns — e.g. CRITICAL, VISA, NORTH_EU):</label>
      <input id="rr-filter" type="text" placeholder="Leave blank to include all rows in range"
        style="width:100%;box-sizing:border-box;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);margin-bottom:12px;"
        onkeydown="event.stopPropagation();">

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Report title (optional):</label>
      <input id="rr-title" type="text" placeholder="e.g. Fraud Analysis — High Risk Transactions"
        style="width:100%;box-sizing:border-box;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);margin-bottom:20px;"
        onkeydown="event.stopPropagation();">

      <div style="display:flex;gap:8px;justify-content:flex-end;">
        <button onclick="document.getElementById('results-report-modal').remove();"
          style="padding:6px 16px;font-size:11px;font-family:var(--mono);cursor:pointer;background:var(--bg3);border:1px solid var(--border);color:var(--text2);border-radius:3px;">Cancel</button>
        <button id="rr-gen-btn"
          style="padding:6px 16px;font-size:11px;font-family:var(--mono);cursor:pointer;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.35);color:var(--accent);border-radius:3px;font-weight:700;">Generate PDF</button>
      </div>
    </div>`;

  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });

  document.getElementById('rr-gen-btn').onclick = () => {
    const rowFrom    = Math.max(1, parseInt(document.getElementById('rr-from').value) || 1);
    const rowToRaw   = parseInt(document.getElementById('rr-to').value);
    const rowTo      = isNaN(rowToRaw) ? rowCount : Math.min(rowToRaw, rowCount);
    const entityFilt = (document.getElementById('rr-filter').value || '').toLowerCase().trim();
    const rptTitle   = document.getElementById('rr-title').value.trim() ||
                       'FlinkSQL Studio — Results Export';
    modal.remove();

    // Gather parallelism + job info from the live perf snapshot
    const jobs        = (typeof perf !== 'undefined' && perf.lastJobs) ? perf.lastJobs : [];
    const runningJob  = jobs.find(j => j.state === 'RUNNING');
    const parallelism = runningJob?.parallelism ?? state?.parallelism ?? '—';
    const jobName     = runningJob?.name ?? '—';
    const sessionId   = state?.activeSession ? state.activeSession.slice(0,12)+'…' : '—';

    _buildResultsPDF(allRows, rowFrom, rowTo, entityFilt, rptTitle, {
      parallelism, jobName, sessionId,
      catalog:  state?.activeCatalog  || 'default_catalog',
      database: state?.activeDatabase || 'default',
    });
  };
}

// ── Column description lookup for PDF legend ─────────────────────────────
function _colDesc(name) {
  const n = (name || '').toLowerCase().replace(/[_\s\-]/g, '');
  const map = {
    'eventid':          'Unique identifier for this event',
    'txnid':            'Unique transaction identifier',
    'cardid':           'Anonymised card identifier',
    'towerid':          'Cell tower identifier (1-200)',
    'deviceid':         'IoT device identifier',
    'region':           'Geographic region of origin',
    'cardnetwork':      'Card network (VISA, MASTERCARD, AMEX, DISCOVER, UNIONPAY, JCB)',
    'merchantcategory': 'Merchant business category',
    'currency':         'Transaction currency (ISO 4217)',
    'country':          'Country code of the transaction',
    'channel':          'Transaction channel (ONLINE, POS, MOBILE, ATM)',
    'amountusd':        'Transaction amount in US dollars',
    'fraudscore':       'Composite fraud score 0-100 (higher = more suspicious)',
    'risklabel':        'Risk classification: LOW_RISK, MEDIUM_RISK, HIGH_RISK',
    'action':           'System action: APPROVE, FLAG, REVIEW, BLOCK',
    'scoreamount':      'Fraud signal: high transaction amount (0-20)',
    'scoregeo':         'Fraud signal: geographic anomaly (0-15)',
    'scoredevice':      'Fraud signal: device fingerprint mismatch (0-12)',
    'scoreborder':      'Fraud signal: cross-border flag (0-10)',
    'scoretime':        'Fraud signal: unusual transaction time (0-8)',
    'scorechannel':     'Fraud signal: card-not-present channel (0-5)',
    'scoremerchant':    'Fraud signal: high-risk merchant category (0-20)',
    'signalquality':    'Composite signal quality: EXCELLENT, GOOD, FAIR, POOR',
    'slaclass':         'SLA tier: GOLD, SILVER, BRONZE, BREACHED',
    'alertlevel':       'Alert severity: LOW, MEDIUM, HIGH, CRITICAL',
    'towerstatus':      'Tower status: ACTIVE, DEGRADED, MAINTENANCE, CONGESTED, OFFLINE',
    'towertype':        'Radio tech: 4G_LTE, 5G_NR, 3G_UMTS, 5G_MMWAVE',
    'rssidbm':          'Signal strength dBm (-120 weak to -40 strong)',
    'sinrdb':           'Signal quality dB (0 poor to 30 excellent)',
    'latencyms':        'Round-trip latency in milliseconds',
    'packetlosspct':    'Packet loss percentage (0-15%)',
    'activesubscribers':'Active subscribers on this tower',
    'handovercount':    'Handover events in this reporting period',
    'uptimepct':        'Tower uptime percentage over last hour',
    'cpuutilization':   'Tower CPU utilisation percentage',
    'devicetype':       'Sensor type: TEMPERATURE, HUMIDITY, PRESSURE, CO2, VIBRATION',
    'sensorval':        'Raw sensor reading in unit specified by UNIT column',
    'riskscore':        'Device anomaly risk score (0-100)',
    'batterypct':       'Device battery percentage',
    'batterystatus':    'Battery condition: OK, LOW, CRITICAL',
    'classification':   'Event class: NORMAL, DEGRADED, ANOMALY, HIGH-RISK, CRITICAL-ANOMALY',
    'eventts':          'Event timestamp (UTC)',
    'status':           'Operational status',
    'instrument':       'Financial instrument or trading pair',
    'tradeprice':       'Execution price of the trade',
    'quantity':         'Number of units traded',
    'exchange':         'Trading exchange identifier',
    'side':             'Trade side: BUY or SELL',
  };
  return map[n] || null;
}

function _buildResultsPDF(allRows, rowFrom, rowTo, entityFilter, title, meta = {}) {
  // Get column definitions
  const cols = state.resultColumns || [];

  // Slice to requested range (1-based)
  let rows = allRows.slice(rowFrom - 1, rowTo);

  // Apply entity filter across all fields
  if (entityFilter) {
    rows = rows.filter(row => {
      const fields = Array.isArray(row?.fields)
        ? row.fields
        : Object.values(row?.fields || row || {});
      return fields.some(v => String(v ?? '').toLowerCase().includes(entityFilter));
    });
  }

  if (rows.length === 0) {
    toast('No rows match your filter — try a different search term', 'err');
    return;
  }

  // Cap at 5000 rows for PDF rendering performance
  const capped = rows.length > 5000;
  if (capped) rows = rows.slice(0, 5000);

  const generatedAt = new Date().toLocaleString();
  const filterNote  = entityFilter ? ` · Filter: "${entityFilter}"` : '';
  const capNote     = capped ? ' (capped at 5,000)' : '';

  // Build column headers
  const colNames = cols.length > 0
    ? cols.map(c => c.name || c)
    : (rows[0] && Array.isArray(rows[0]?.fields)
        ? rows[0].fields.map((_, i) => 'col_' + i)
        : Object.keys(rows[0]?.fields || rows[0] || {}));

  // Build rows HTML
  const rowsHtml = rows.map((row, i) => {
    const fields = Array.isArray(row?.fields)
      ? row.fields
      : Object.values(row?.fields || row || {});
    const rowNum = rowFrom + i;
    const cells = fields.map(v =>
      `<td>${v === null || v === undefined ? '<em style="color:#888">NULL</em>' : String(v).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</td>`
    ).join('');
    return `<tr class="${i % 2 === 0 ? 'even' : 'odd'}"><td class="rn">${rowNum}</td>${cells}</tr>`;
  }).join('');

  const headerCells = colNames.map(n =>
    `<th>${String(n).replace(/</g,'&lt;')}</th>`
  ).join('');

  const html = `<!DOCTYPE html><html lang="en"><head>
  <meta charset="UTF-8">
  <title>${title.replace(/</g,'&lt;')}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 11px; color: #1a1a2e; background: #fff; }
    .header { background: #0c1018; color: #e8edf5; padding: 20px 32px; }
    .header h1 { font-size: 18px; font-weight: 700; margin-bottom: 4px; color: #00d4aa; }
    .header .meta { font-size: 10px; color: #6a7f96; font-family: monospace; }
    .header .badge { display:inline-block;background:rgba(0,212,170,0.15);color:#00d4aa;
      font-size:9px;padding:2px 8px;border-radius:2px;margin-right:6px;border:1px solid rgba(0,212,170,0.3); }
    .summary { padding: 12px 32px; background: #f4f6fb; border-bottom: 1px solid #dde2ed;
      font-size: 10px; color: #4a5568; font-family: monospace; display:flex; gap:24px; }
    .summary b { color: #1a2535; }
    .table-wrap { padding: 0 16px 24px; overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 10px; }
    thead th { background: #0c1018; color: #a8b8cc; font-weight: 600; padding: 6px 8px;
      text-align: left; border: 1px solid #1e2d42; font-family: monospace; white-space: nowrap; }
    tr.even td { background: #f9fafb; }
    tr.odd  td { background: #ffffff; }
    td { padding: 4px 8px; border: 1px solid #e2e8f0; font-family: monospace;
      max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    td.rn { color: #9090a0; text-align: right; width: 40px; background: #f4f6fb !important; }
    tr:hover td { background: #eef4ff !important; }
    .footer { margin-top: 24px; padding: 12px 32px; font-size: 9px; color: #9090a0;
      border-top: 1px solid #dde2ed; font-family: monospace; }
    .legend-section { padding: 16px 32px 8px; border-top: 2px solid #e2e8f0; margin-top: 16px; }
    .legend-title { font-size: 9px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase;
      color: #6a7f96; margin-bottom: 10px; font-family: monospace; }
    .legend-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 4px 24px; }
    .legend-item { display: flex; gap: 8px; padding: 3px 0; border-bottom: 1px solid #f0f4f8; font-size: 10px; }
    .legend-col  { font-family: monospace; color: #1a2535; font-weight: 600; white-space: nowrap;
      min-width: 140px; flex-shrink: 0; }
    .legend-desc { color: #4a5568; line-height: 1.5; }
    @media print {
      body { font-size: 9px; }
      .header { padding: 12px 16px; }
      .table-wrap { padding: 0 8px; }
      td { padding: 3px 5px; }
      .legend-section { padding: 10px 8px; }
    }
  </style>
</head><body>
  <div class="header">
    <h1>${title.replace(/</g,'&lt;')}</h1>
    <div class="meta">
      <span class="badge">FlinkSQL Studio</span>
      Generated: ${generatedAt}
    </div>
  </div>
  <div class="summary">
    <span><b>Rows exported:</b> ${rows.length.toLocaleString()}${capNote}</span>
    <span><b>Range:</b> ${rowFrom}–${rowFrom + rows.length - 1}</span>
    <span><b>Columns:</b> ${colNames.length}</span>
    <span><b>Parallelism:</b> ${meta.parallelism ?? '—'}</span>
    <span><b>Session:</b> ${meta.sessionId ?? '—'}</span>
    ${meta.jobName && meta.jobName !== '—' ? `<span><b>Job:</b> ${meta.jobName}</span>` : ''}
    ${meta.catalog  ? `<span><b>Catalog:</b> ${meta.catalog}</span>` : ''}
    ${meta.database ? `<span><b>Database:</b> ${meta.database}</span>` : ''}
    ${entityFilter ? `<span><b>Filter:</b> "${entityFilter}"</span>` : ''}
    ${entityFilter ? `<span><b>Matched:</b> ${rows.length} of ${allRows.length.toLocaleString()} total</span>` : ''}
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th class="rn">#</th>${headerCells}</tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table>
  </div>
  <div class="legend-section">
    <div class="legend-title">FIELD DESCRIPTIONS</div>
    <div class="legend-grid" id="legend-grid">
      ${colNames.map((n, i) => `
        <div class="legend-item">
          <span class="legend-col">${String(n).replace(/</g,'&lt;')}</span>
          <span class="legend-desc">${_getColDescription(String(n))}</span>
        </div>`).join('')}
    </div>
  </div>
  <div class="legend-section" style="margin:0 16px 24px;padding:14px 16px;
    background:#f4f6fb;border:1px solid #dde2ed;border-radius:4px;">
    <div style="font-size:10px;font-weight:700;color:#1a2535;letter-spacing:1px;
      text-transform:uppercase;margin-bottom:8px;">Field Reference</div>
    <div style="display:flex;flex-wrap:wrap;gap:6px 24px;">
      ${colNames.map(col => {
        const desc = _colDesc(col);
        return desc ? `<div style="font-size:9px;font-family:monospace;color:#4a5568;min-width:200px;">
          <span style="color:#1a2535;font-weight:600;">${col.replace(/</g,'&lt;')}</span>
          ${desc ? `<span style="color:#718096;"> — ${desc}</span>` : ''}
        </div>` : '';
      }).filter(Boolean).join('')}
    </div>
    ${colNames.every(c => !_colDesc(c)) ?
      '<div style="font-size:9px;color:#9090a0;font-style:italic;">Column definitions not available — field names are self-descriptive.</div>' : ''}
  </div>
  <div class="footer">
    FlinkSQL Studio · Results Export · ${generatedAt}${filterNote}${capNote}
  </div>
</body></html>`;

  const win = window.open('', '_blank', 'width=1200,height=900');
  if (!win) {
    toast('Pop-up blocked — please allow pop-ups and retry', 'err');
    return;
  }
  win.document.write(html);
  win.document.close();
  win.focus();
  setTimeout(() => win.print(), 600);
  addLog('INFO', `Results PDF: ${rows.length} rows exported (${rowFrom}–${rowFrom + rows.length - 1})${filterNote}`);
  toast(`PDF ready — ${rows.length} rows · save from print dialog`, 'ok');
}


// ── Column description lookup for PDF legend ─────────────────────────────
function _getColDescription(colName) {
  const lower = colName.toLowerCase();
  // Common streaming / Flink field patterns
  const knownFields = {
    // Identifiers
    'event_id'          : 'Unique identifier for this event record',
    'txn_id'            : 'Unique transaction identifier',
    'card_id'           : 'Card identifier (anonymised)',
    'tower_id'          : 'Cell tower identifier (1–200)',
    'device_id'         : 'Device identifier in the fleet',
    'merchant_id'       : 'Merchant identifier',
    'terminal_id'       : 'POS terminal identifier',
    'alert_id'          : 'Unique alert identifier',
    // Classifications / Labels
    'signal_quality'    : 'Composite signal quality: EXCELLENT / GOOD / FAIR / POOR',
    'risk_label'        : 'Fraud risk classification: LOW_RISK / MEDIUM_RISK / HIGH_RISK',
    'risk_score'        : 'Composite risk score (0–100, higher = more risk)',
    'fraud_score'       : 'Composite fraud score (0–100). ≥80 = BLOCK, ≥60 = REVIEW, ≥40 = FLAG',
    'alert_level'       : 'Alert severity: LOW / MEDIUM / HIGH / CRITICAL',
    'sla_class'         : 'SLA tier: GOLD / SILVER / BRONZE / BREACHED (based on latency + packet loss)',
    'tower_status'      : 'Operational status: ACTIVE / DEGRADED / MAINTENANCE / CONGESTED / OFFLINE',
    'tower_type'        : 'Radio technology: 4G_LTE / 5G_NR / 3G_UMTS / 5G_MMWAVE',
    'action'            : 'Recommended action: APPROVE / FLAG / REVIEW / BLOCK',
    'device_type'       : 'Sensor device type: TEMPERATURE / HUMIDITY / PRESSURE / CO2 / VIBRATION',
    'battery_status'    : 'Battery condition: OK / LOW / CRITICAL',
    'classification'    : 'Event classification: NORMAL / ANOMALY / DEGRADED / HIGH-RISK / CRITICAL-ANOMALY',
    'velocity_flag'     : 'Card velocity flag: NORMAL / ELEVATED / HIGH_VELOCITY',
    'rapid_flag'        : 'Rapid succession flag: NORMAL / RAPID_SUCCESSION / FRAUD_PATTERN',
    'outage_risk'       : 'Tower outage risk: NORMAL / MEDIUM_RISK / HIGH_RISK',
    'capacity_status'   : 'Capacity state: UNDERUTILIZED / NORMAL / HIGH / OVERLOADED',
    'anomaly_flag'      : 'Handover anomaly: NORMAL / ELEVATED / ANOMALY',
    'breach_type'       : 'Type of SLA breach: HIGH_LATENCY / HIGH_PACKET_LOSS / LATENCY_AND_LOSS / DEGRADED_SIGNAL',
    // Network / geography
    'region'            : 'Geographic region (e.g. NORTH_EU, SOUTH_US, ASIA_PAC)',
    'country'           : 'ISO country code of the transaction',
    'card_network'      : 'Payment network: VISA / MASTERCARD / AMEX / DISCOVER / UNIONPAY / JCB',
    'merchant_category' : 'Merchant category code: RETAIL / GAMBLING / CRYPTO / TRAVEL / FOOD / LUXURY…',
    'currency'          : 'Transaction currency (ISO 4217)',
    'channel'           : 'Transaction channel: ONLINE / POS / MOBILE / ATM',
    // Numeric metrics
    'rssi_dbm'          : 'Received Signal Strength Indicator in dBm (–120 = weakest, –40 = strongest)',
    'sinr_db'           : 'Signal-to-Interference-plus-Noise Ratio in dB (higher = better)',
    'latency_ms'        : 'Round-trip network latency in milliseconds',
    'packet_loss_pct'   : 'Packet loss percentage (0–15%). >3% = HIGH alert threshold',
    'active_subscribers': 'Number of concurrent active subscribers on this tower',
    'handover_count'    : 'Number of cell handovers recorded in this period',
    'uptime_pct'        : 'Tower uptime percentage in the last hour (100% = fully available)',
    'cpu_utilization'   : 'Tower processing CPU utilisation percentage',
    'sensor_val'        : 'Raw sensor reading value (units depend on device_type)',
    'battery_pct'       : 'Device battery level as a percentage (0–100)',
    'amount_usd'        : 'Transaction amount in US Dollars',
    'fraud_rate_pct'    : 'Percentage of transactions classified as HIGH_RISK in this window',
    'breach_score'      : 'Numeric SLA breach severity score (GOLD=100, SILVER=50, BRONZE=25)',
    // Fraud signal scores
    'score_amount'      : 'Fraud signal: high transaction amount (0–20)',
    'score_geo'         : 'Fraud signal: geographic location anomaly (0–15)',
    'score_device'      : 'Fraud signal: device fingerprint mismatch (0–12)',
    'score_border'      : 'Fraud signal: cross-border transaction flag (0–10)',
    'score_time'        : 'Fraud signal: unusual time-of-day pattern (0–8)',
    'score_channel'     : 'Fraud signal: card-not-present channel risk (0–5)',
    'score_merchant'    : 'Fraud signal: high-risk merchant category (0–20)',
    // Timestamps / windows
    'event_ts'          : 'Event timestamp (watermarked for stream processing)',
    'txn_ts'            : 'Transaction timestamp (watermarked for stream processing)',
    'window_start'      : 'Start of the aggregation window (TUMBLE / HOP)',
    'window_end'        : 'End of the aggregation window (TUMBLE / HOP)',
    'session_start'     : 'Start of the SESSION window (first event in idle gap)',
    'session_end'       : 'End of the SESSION window (last event before idle gap)',
    // Aggregation fields
    'avg_rssi'          : 'Average RSSI across all towers in this window and region',
    'avg_sinr'          : 'Average SINR across all towers in this window and region',
    'avg_latency_ms'    : 'Average round-trip latency across towers in this window',
    'avg_packet_loss'   : 'Average packet loss percentage across towers in this window',
    'total_subscribers' : 'Total active subscribers summed across all towers in the region',
    'total_handovers'   : 'Total handover events across towers in this window',
    'avg_uptime'        : 'Average tower uptime percentage across the region',
    'avg_cpu'           : 'Average CPU utilisation across towers in this window',
    'degraded_towers'   : 'Count of towers with POOR signal quality in this window',
    'critical_alerts'   : 'Count of towers with CRITICAL alert level in this window',
    'avg_sqi'           : 'Average Signal Quality Index (0–100, 100=EXCELLENT, 25=POOR)',
    'poor_quality_count': 'Number of towers rated POOR signal quality in this window',
    'avg_fraud_score'   : 'Average composite fraud score across all transactions in this window',
    'total_txns'        : 'Total transaction count in this aggregation window',
    'blocked_txns'      : 'Count of transactions with action=BLOCK in this window',
    'review_txns'       : 'Count of transactions with action=REVIEW in this window',
    'fraud_txns'        : 'Count of transactions classified as HIGH_RISK',
    'unique_merchants'  : 'Number of distinct merchants in this card session',
    'unique_countries'  : 'Number of distinct countries in this card velocity window',
    'max_score'         : 'Highest individual fraud score seen in this card session',
    'top_signal'        : 'Dominant fraud signal that most contributed to the fraud score',
    'block_reason'      : 'Combination of signals that triggered the BLOCK action',
    'txn_count'         : 'Number of transactions in this card velocity window',
    'total_amount'      : 'Total USD value transacted in this window',
    'max_amount'        : 'Highest single transaction amount in this window',
    'stddev_amount'     : 'Standard deviation of transaction amounts (higher = more volatile)',
    'high_amount_count' : 'Number of transactions above $2,000 threshold',
    'risk_tier'         : 'Merchant risk tier: LOW / MEDIUM / HIGH / CRITICAL (based on fraud rate)',
    'event_count'       : 'Number of events in this session window',
    'avg_sqi_val'       : 'Average Signal Quality Index value in this HOP window',
    'min_rssi'          : 'Minimum (weakest) RSSI value across towers in this window',
    'max_sinr'          : 'Maximum (best) SINR value across towers in this window',
    'peak_subscribers'  : 'Peak simultaneous subscriber count across the region',
    'is_recurring'      : 'Whether this is a recurring/subscription transaction (1=yes)',
    'unit'              : 'Unit of measurement for the sensor reading',
  };

  // Exact match first
  if (knownFields[lower]) return knownFields[lower];

  // Fuzzy pattern matches
  if (lower.includes('_id') || lower.endsWith('id'))  return 'Unique record identifier';
  if (lower.includes('ts') || lower.includes('time') || lower.includes('date'))
    return 'Timestamp field';
  if (lower.startsWith('avg_'))   return 'Average (mean) value computed over the aggregation window';
  if (lower.startsWith('max_'))   return 'Maximum value in the aggregation window';
  if (lower.startsWith('min_'))   return 'Minimum value in the aggregation window';
  if (lower.startsWith('sum_') || lower.startsWith('total_')) return 'Sum of values in the aggregation window';
  if (lower.startsWith('count_') || lower.endsWith('_count')) return 'Count of records matching the condition';
  if (lower.includes('score'))    return 'Numeric score (higher = higher risk/priority)';
  if (lower.includes('flag'))     return 'Boolean or categorical flag indicating a condition';
  if (lower.includes('pct') || lower.includes('rate') || lower.includes('ratio'))
    return 'Percentage or ratio value';
  if (lower.includes('status'))   return 'Categorical status indicator';
  if (lower.includes('type'))     return 'Category or type classification';
  if (lower.includes('region'))   return 'Geographic region identifier';
  if (lower.includes('window'))   return 'Aggregation window boundary timestamp';

  return 'Data field from the streaming pipeline';
}

// ── Stream Selector: lets user pick which result slot to view ────────────────
function renderStreamSelector() {
  const slots = state.resultSlots || [];
  let sel = document.getElementById('stream-selector-bar');

  if (slots.length === 0) {
    if (sel) sel.style.display = 'none';
    return;
  }

  // Create bar if it doesn't exist
  if (!sel) {
    const wrap = document.getElementById('result-data-tab');
    if (!wrap) return;
    sel = document.createElement('div');
    sel.id = 'stream-selector-bar';
    sel.style.cssText = `
      display:flex; align-items:center; gap:4px; padding:4px 8px;
      background:var(--bg2); border-bottom:1px solid var(--border);
      flex-shrink:0; overflow-x:auto; white-space:nowrap; min-height:32px;
    `;
    wrap.insertBefore(sel, wrap.firstChild);
  }
  sel.style.display = slots.length > 0 ? 'flex' : 'none';

  const activeId = state.activeSlot;
  sel.innerHTML = '<span style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-right:4px;flex-shrink:0;">Streams:</span>';

  slots.forEach(slot => {
    const isActive = slot.id === activeId;
    const isStreaming = slot.status === 'streaming';
    const badge = document.createElement('button');
    badge.style.cssText = `
      display:inline-flex; align-items:center; gap:5px; padding:2px 8px;
      border-radius:3px; font-size:10px; font-family:var(--mono); cursor:pointer;
      border: 1px solid ${isActive ? 'var(--accent)' : 'var(--border)'};
      background: ${isActive ? 'rgba(0,212,170,0.12)' : 'var(--bg3)'};
      color: ${isActive ? 'var(--accent)' : 'var(--text2)'};
      flex-shrink:0;
    `;
    if (isStreaming) {
      badge.innerHTML = `<span style="width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 1s infinite;display:inline-block;"></span>${escHtml(slot.label)} <span style="color:var(--text3)">${slot.rows.length}</span>`;
    } else {
      badge.innerHTML = `<span style="color:var(--text3);">✓</span>${escHtml(slot.label)} <span style="color:var(--text3)">${slot.rows.length}</span>`;
    }
    badge.title = slot.sql;
    badge.onclick = () => switchToSlot(slot.id);

    // Close button
    const close = document.createElement('span');
    close.textContent = '×';
    close.style.cssText = 'margin-left:2px;opacity:0.5;cursor:pointer;font-size:11px;';
    close.onclick = (e) => { e.stopPropagation(); removeSlot(slot.id); };
    badge.appendChild(close);
    sel.appendChild(badge);
  });

  // "Clear All" button
  if (slots.length > 1) {
    const clearBtn = document.createElement('button');
    clearBtn.textContent = 'Clear All';
    clearBtn.style.cssText = `
      margin-left:auto; padding:2px 8px; font-size:9px; font-family:var(--mono);
      background:none; border:1px solid var(--border); color:var(--text3); cursor:pointer; border-radius:3px; flex-shrink:0;
    `;
    clearBtn.onclick = clearAllSlots;
    sel.appendChild(clearBtn);
  }
}

function switchToSlot(slotId) {
  const slot = (state.resultSlots || []).find(s => s.id === slotId);
  if (!slot) return;
  state.activeSlot = slotId;
  state.results = slot.rows;
  state.resultColumns = slot.columns;
  state.resultPage = 0;
  renderStreamSelector();
  renderResults();
  const tab = document.getElementById('results-tab-btn');
  if (tab) switchResultTab('data', tab);
}

function removeSlot(slotId) {
  state.resultSlots = (state.resultSlots || []).filter(s => s.id !== slotId);
  if (state.activeSlot === slotId) {
    const remaining = state.resultSlots;
    if (remaining.length > 0) {
      switchToSlot(remaining[remaining.length - 1].id);
    } else {
      state.activeSlot = null;
      clearResults();
    }
  }
  renderStreamSelector();
}

function clearAllSlots() {
  state.resultSlots = [];
  state.activeSlot = null;
  clearResults();
  renderStreamSelector();
}

// ── DDL/SET status confirmation card ─────────────────────────────────────────
function showDDLStatus(verb, sql) {
  // Accumulate DDL status lines in a special status slot
  let statusSlot = (state.resultSlots || []).find(s => s.id === 'ddl-status');
  if (!statusSlot) {
    statusSlot = {
      id: 'ddl-status',
      label: 'Statements',
      sql: '',
      columns: [{ name: 'Status', type: 'VARCHAR' }, { name: 'Statement', type: 'VARCHAR' }, { name: 'Time', type: 'VARCHAR' }],
      rows: [],
      status: 'done',
      startedAt: new Date(),
    };
    if (!state.resultSlots) state.resultSlots = [];
    state.resultSlots.unshift(statusSlot); // put it first
  }
  const ts = new Date().toLocaleTimeString('en-US',{hour12:false});
  statusSlot.rows.push({ fields: ['OK', sql, ts] });
  statusSlot.columns = [{ name: 'Status', type: 'VARCHAR' }, { name: 'Statement', type: 'VARCHAR' }, { name: 'Time', type: 'VARCHAR' }];

  // Only switch to this slot if user is already on results tab or no active slot
  if (!state.activeSlot || state.activeSlot === 'ddl-status') {
    state.activeSlot = 'ddl-status';
    state.results = statusSlot.rows;
    state.resultColumns = statusSlot.columns;
    state.resultPage = 0;
    renderResults();
  }
  renderStreamSelector();
}

function clearResults() {
  state.results = [];
  state.resultColumns = [];
  state.resultPage = 0;
  const wrap = document.getElementById('result-table-wrap');
  if (wrap) wrap.innerHTML = `
    <div class="empty-state">
      <div class="icon">⚡</div>
      <div class="msg">Run a query to see results here.<br><kbd>Ctrl+Enter</kbd> to execute.</div>
    </div>`;
  const badge = document.getElementById('result-row-badge');
  if (badge) badge.textContent = '0';
  const pag = document.getElementById('result-pagination');
  if (pag) pag.style.display = 'none';
}

function exportCSV() {
  const hasResults = state.results.length > 0;
  const hasLogs    = state.logLines.length > 0;
  const hasPerf    = perf.timings.length > 0;

  if (!hasResults && !hasLogs && !hasPerf) {
    toast('Nothing to export yet — run a query first', 'err');
    return;
  }

  const ts = new Date().toISOString().slice(0,19).replace(/[T:]/g,'-');
  const parts = [];

  // Section 1: Query Results
  if (hasResults) {
    const cols = state.resultColumns.map(c => c.name);
    const rows = state.results.map(row => {
      const fields = row.fields || row;
      const arr = Array.isArray(fields) ? fields : Object.values(fields);
      return arr.map(v => v === null ? '' : '"' + String(v).replace(/"/g, '""') + '"').join(',');
    });
    parts.push('# SECTION: QUERY RESULTS');
    parts.push('# Exported: ' + new Date().toISOString());
    parts.push('# Rows: ' + state.results.length + '  Columns: ' + cols.length);
    parts.push(cols.join(','));
    rows.forEach(r => parts.push(r));
    parts.push('');
  }

  // Section 2: Session Log
  if (hasLogs) {
    parts.push('# SECTION: SESSION LOG');
    parts.push('# Lines: ' + state.logLines.length);
    parts.push('time,level,message');
    state.logLines.forEach(l => {
      const msg = String(l.msg || '').replace(/"/g, '""');
      parts.push('"' + l.time + '","' + l.level + '","' + msg + '"');
    });
    parts.push('');
  }

  // Section 3: Performance Timings
  if (hasPerf) {
    parts.push('# SECTION: PERFORMANCE TIMINGS');
    parts.push('# Queries: ' + perf.timings.length);
    parts.push('sql,duration_ms,rows');
    perf.timings.forEach(t => {
      const sql = String(t.sql || '').replace(/"/g, '""').replace(/\n/g, ' ');
      parts.push('"' + sql + '",' + t.ms + ',' + (t.rows || 0));
    });
    parts.push('');
  }

  const blob = new Blob([parts.join('\n')], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'flinksql-export-' + ts + '.csv';
  a.click();
  URL.revokeObjectURL(a.href);
  const sections = [hasResults && (state.results.length + ' result rows'), hasLogs && (state.logLines.length + ' log lines'), hasPerf && (perf.timings.length + ' perf timings')].filter(Boolean).join(', ');
  toast('Exported: ' + sections, 'ok');
}

// ──────────────────────────────────────────────
// LOG
// ──────────────────────────────────────────────
// ── Extract meaningful message from Java stack trace ─────────
function parseFlinkError(raw) {
  if (!raw) return 'Unknown error';
  var s = typeof raw === 'string' ? raw : JSON.stringify(raw, null, 2);

  // ── Strip JSON envelope — Flink wraps errors in {errors:[], message:""}
  try {
    var j = JSON.parse(s);
    // errors[] array — join all messages (usually one long Java stack trace)
    if (j.errors && j.errors.length) {
      s = Array.isArray(j.errors) ? j.errors.join('\n') : String(j.errors);
    } else if (j.message) {
      s = j.message;
    } else if (j.status === 'ERROR' && !j.errors && !j.message) {
      // Bare {"status":"ERROR"} with nothing useful — signal caller to fetch more
      return 'Query failed (no details — check the full error via Details button)';
    }
  } catch (_) {}

  // ── Detect information_schema usage (Flink does not support it)
  if (s.indexOf('information_schema') !== -1 || s.indexOf('. tables') !== -1 || s.indexOf('.tables') !== -1) {
    return 'Flink SQL does not support information_schema. Use: SHOW TABLES; / SHOW FULL TABLES; / DESCRIBE <table>; / SHOW CREATE TABLE <table>;';
  }

  // ── Detect USE CATALOG missing
  if (s.indexOf('No current catalog') !== -1 || s.indexOf('No default catalog') !== -1) {
    return 'No active catalog. Run: USE CATALOG default_catalog; (or create one first)';
  }

  // ── Detect SESSION expired / does not exist ─────────────────────────────
  if (s.indexOf('does not exist') !== -1 && (s.indexOf('Session') !== -1 || s.indexOf('session') !== -1)) {
    // Trigger auto-renewal in background — next retry will use the new session
    if (typeof showSessionExpiredBanner === 'function') showSessionExpiredBanner();
    return 'Session expired — your Flink session no longer exists. Click "Create New Session" in the banner above, then re-run your CREATE TABLE statements.';
  }

  // ── Detect table not found (TEMPORARY tables are session-scoped)
  var noObj = s.match(/Object\s+'([^']+)'\s+not found/) ||
              s.match(/Cannot find table '([^']+)'/) ||
              s.match(/Table '([^']+)' was not found/);
  if (noObj) {
    var tblName = noObj[1].split('.').pop().replace(/`/g, '');
    return "Table '" + tblName + "' not found.\n\n" +
      "TEMPORARY tables only live for the current session.\n" +
      "Your session may have expired or been restarted.\n\n" +
      "Fix: Re-run all CREATE TEMPORARY TABLE statements (Step 1) in this session, then retry.\n" +
      "Tip: Use the 'Create New Session' button if the session expired.";
  }

  // ── Detect catalog not found
  var noCat = s.match(/Catalog\s+'([^']+)'\s+does not exist/);
  if (noCat) {
    return "Catalog '" + noCat[1] + "' does not exist. Run: SHOW CATALOGS; then USE CATALOG <name>";
  }

  // ── Detect connector/JAR not found
  if (s.indexOf('Unable to create a source') !== -1 || s.indexOf('Cannot find connector') !== -1) {
    var connMatch = s.match(/'connector'\s*=\s*'([^']+)'/);
    return "Connector" + (connMatch ? " '" + connMatch[1] + "'" : "") + " not found — ensure the connector JAR is on the Flink classpath.";
  }

  // ── Parse failure: extract position from Calcite parse error
  var parseMatch = s.match(/Encountered "([^"]+)" at line (\d+), column (\d+)/);
  if (parseMatch) {
    return 'SQL parse error at line ' + parseMatch[2] + ' col ' + parseMatch[3] + ' — unexpected token: ' + parseMatch[1];
  }

  // ── Validation error
  var validMatch = s.match(/ValidationException:[^\r\n]*:[\s]*([^\r\n]{5,200})/);
  if (validMatch) return 'Validation error: ' + validMatch[1].trim();

  // ── General Caused by extraction — walk all lines
  var lines = s.split('\n');
  var causedLines = lines.filter(function(l){ return l.indexOf('Caused by:') !== -1; });
  if (causedLines.length) {
    var last = causedLines[causedLines.length - 1];
    // Extract message after the exception class name
    var colonIdx = last.lastIndexOf(':');
    if (colonIdx > -1) {
      var msg = last.slice(colonIdx + 1).trim().slice(0, 250);
      if (msg && msg.length > 5) return msg;
    }
  }

  // ── First Exception/Error line
  var exMatch = s.match(/(?:Exception|Error)[^:\r\n]*:[\s]*([^\r\n]{5,250})/);
  if (exMatch) return exMatch[1].trim();

  // ── HTTP error
  var httpMatch = s.match(/HTTP \d+: ([\s\S]{1,300})/);
  if (httpMatch) {
    try { var hj = JSON.parse(httpMatch[1]); if (hj.message) return hj.message; } catch(_){}
    return httpMatch[1].replace(/[\r\n]+/g,' ').slice(0,200);
  }

  return s.replace(/[\r\n]+/g,' ').slice(0, 250);
}

