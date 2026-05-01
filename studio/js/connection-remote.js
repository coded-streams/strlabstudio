// ═══════════════════════════════════════════════════════════════════════════════
// UI HELPERS — Ververica model switcher, AWS mode/auth switcher,
//              Confluent endpoint live preview.
// These are called by onclick handlers in index.html so MUST be global.
// ═══════════════════════════════════════════════════════════════════════════════

let _vvpModel    = 'selfmanaged';
let _awsMode     = 'studio';
let _awsAuthType = 'profile';

// ── Ververica: deployment model toggle
function setVvpModel(model) {
    _vvpModel = model;
    ['selfmanaged','cloud','byoc'].forEach(m => {
        const btn = document.getElementById('vvp-model-' + m);
        if (!btn) return;
        btn.style.background = (m === model) ? 'var(--accent)' : 'var(--bg3)';
        btn.style.color      = (m === model) ? '#000'          : 'var(--text2)';
        btn.style.fontWeight = (m === model) ? '700'           : '';
    });
    const hint     = document.getElementById('vvp-url-hint');
    const urlInput = document.getElementById('inp-vvp-url');
    if (model === 'selfmanaged') {
        if (hint) hint.textContent = 'Your VVP host — e.g. https://vvp.internal or http://localhost:8080';
        if (urlInput) urlInput.placeholder = 'https://vvp.your-company.com';
    } else if (model === 'cloud') {
        if (hint) hint.textContent = 'Ververica Cloud base URL — https://app.ververica.cloud';
        if (urlInput) urlInput.placeholder = 'https://app.ververica.cloud';
    } else {
        if (hint) hint.textContent = 'Your BYOC endpoint — same pattern as Self-Managed but cloud-hosted.';
        if (urlInput) urlInput.placeholder = 'https://vvp.your-byoc-host.com';
    }
    _updateVvpPreview();
}

function _updateVvpPreview() {
    const base = (document.getElementById('inp-vvp-url')?.value || '').trim().replace(/\/$/, '');
    const ns   = (document.getElementById('inp-vvp-namespace')?.value || '').trim() || 'default';
    const el   = document.getElementById('vvp-endpoint-text');
    if (!el) return;
    el.textContent = base ? `${base}/api/v1/namespaces/${ns}/deployments` : 'fill in the fields above';
}

// ── AWS: interaction mode toggle ───────────────────────────────────────────────
function setAwsMode(mode) {
    _awsMode = mode;
    ['studio','app'].forEach(m => {
        const btn = document.getElementById('aws-mode-' + m);
        if (!btn) return;
        btn.style.background = (m === mode) ? 'var(--accent)' : 'var(--bg3)';
        btn.style.color      = (m === mode) ? '#000'          : 'var(--text2)';
        btn.style.fontWeight = (m === mode) ? '700'           : '';
    });
    const studioNote = document.getElementById('aws-studio-note');
    const appNote    = document.getElementById('aws-app-note');
    const zepRow     = document.getElementById('aws-zeppelin-row');
    const restRow    = document.getElementById('aws-flink-rest-row');
    if (mode === 'studio') {
        if (studioNote) studioNote.style.display = '';
        if (appNote)    appNote.style.display    = 'none';
        if (zepRow)     zepRow.style.display     = '';
        if (restRow)    restRow.style.display    = 'none';
    } else {
        if (studioNote) studioNote.style.display = 'none';
        if (appNote)    appNote.style.display    = '';
        if (zepRow)     zepRow.style.display     = 'none';
        if (restRow)    restRow.style.display    = '';
    }
}

// ── AWS: IAM auth type toggle ──────────────────────────────────────────────────
function setAwsAuthType(type) {
    _awsAuthType = type;
    ['profile','keys','role'].forEach(t => {
        const btn = document.getElementById('aws-auth-' + t);
        if (!btn) return;
        btn.style.background = (t === type) ? 'var(--accent)' : 'var(--bg3)';
        btn.style.color      = (t === type) ? '#000'          : 'var(--text2)';
        btn.style.fontWeight = (t === type) ? '700'           : '';
        const fields = document.getElementById('aws-auth-' + t + '-fields');
        if (fields) fields.style.display = (t === type) ? '' : 'none';
    });
}

// ── Confluent: live endpoint preview ──────────────────────────────────────────
function _updateConfluentPreview() {
    const region   = (document.getElementById('inp-cf-region')?.value   || '').trim().toLowerCase();
    const provider = (document.getElementById('inp-cf-provider')?.value || '').trim().toLowerCase();
    const orgId    = (document.getElementById('inp-cf-org')?.value      || '').trim();
    const envId    = (document.getElementById('inp-cf-env')?.value      || '').trim();
    const priv     = document.getElementById('inp-cf-private')?.checked;
    const el       = document.getElementById('cf-endpoint-text');
    if (!el) return;
    if (!region || !provider || !orgId || !envId) {
        el.textContent = 'fill in the fields above';
        return;
    }
    const host = priv
        ? `flink.${region}.${provider}.private.confluent.cloud`
        : `flink.${region}.${provider}.confluent.cloud`;
    el.textContent = `https://${host}/sql/v1/organizations/${orgId}/environments/${envId}/statements`;
}

// Wire up live previews once DOM is ready
(function _wireupPreviews() {
    function wire() {
        ['inp-cf-region','inp-cf-provider','inp-cf-org','inp-cf-env'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', _updateConfluentPreview);
        });
        const cfPriv = document.getElementById('inp-cf-private');
        if (cfPriv) cfPriv.addEventListener('change', _updateConfluentPreview);

        ['inp-vvp-url','inp-vvp-namespace'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', _updateVvpPreview);
        });
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', wire);
    } else {
        wire();
    }
})();


// VERVERICA PLATFORM / CLOUD  — connection helpers

function _getVvpBaseUrl() {
    return (document.getElementById('inp-vvp-url')?.value || '').trim().replace(/\/$/, '');
}

function _getVvpAuthHeaders() {
    const token = (document.getElementById('inp-vvp-token')?.value || '').trim();
    return token ? { 'Authorization': 'Bearer ' + token } : {};
}

function _getVvpNamespace() {
    return (document.getElementById('inp-vvp-namespace')?.value || '').trim() || 'default';
}

/**
 * Test connection for Ververica:
 * GET /api/v1/namespaces/<ns>/deployments  — lists deployments, returns 200 if auth OK.
 * Falls back to listing namespaces if that endpoint returns 404.
 */
async function testVvpConnection() {
    const baseUrl = _getVvpBaseUrl();
    const ns      = _getVvpNamespace();
    if (!baseUrl) {
        setConnectStatus('err', 'Please enter the VVP Base URL.');
        return false;
    }
    const authHeaders = _getVvpAuthHeaders();
    const testUrl = `${baseUrl}/api/v1/namespaces/${ns}/deployments`;
    setConnectStatus('loading', `Testing ${testUrl} …`);
    try {
        const resp = await fetchWithTimeout(testUrl, {
            headers: { 'Accept': 'application/json', ...authHeaders }
        }, 10000);
        if (resp.status === 401) {
            setConnectStatus('err', '401 Unauthorized — check your API Token.');
            return false;
        }
        if (resp.status === 403) {
            setConnectStatus('err', '403 Forbidden — token lacks access to this namespace.');
            return false;
        }
        if (resp.status === 404) {
            // Namespace might not exist — try root health/info
            const infoResp = await fetchWithTimeout(`${baseUrl}/api/v1/namespaces`, {
                headers: { 'Accept': 'application/json', ...authHeaders }
            }, 6000);
            if (!infoResp.ok) {
                setConnectStatus('err', `HTTP ${infoResp.status} — check Base URL and token.`);
                return false;
            }
        } else if (!resp.ok) {
            setConnectStatus('err', `HTTP ${resp.status} — check Base URL, namespace, and token.`);
            return false;
        }
        setConnectStatus('ok', `✓ Ververica reachable — click Connect`);
        toast('Ververica connection test passed', 'ok');
        return true;
    } catch (e) {
        setConnectStatus('err', `${e.message}<br>Check Base URL and network connectivity.`);
        return false;
    }
}

/**
 * Connect to Ververica.
 * VVP does NOT use the Flink SQL Gateway session model.
 * We store all the VVP config in state.gateway and note that SQL submission
 * here means POSTing a SQLSCRIPT deployment to the deployments endpoint,
 * not the /v1/sessions flow.
 */
async function doVvpConnect() {
    const baseUrl        = _getVvpBaseUrl();
    const ns             = _getVvpNamespace();
    const deployTarget   = (document.getElementById('inp-vvp-target')?.value        || '').trim();
    const sessionCluster = (document.getElementById('inp-vvp-session-cluster')?.value || '').trim();
    const flinkVersion   = (document.getElementById('inp-vvp-flink-version')?.value  || '1.15').trim();
    const token          = (document.getElementById('inp-vvp-token')?.value           || '').trim();

    if (!baseUrl) {
        setConnectStatus('err', 'Please enter the VVP Base URL.');
        return;
    }

    setConnectStatus('loading', 'Connecting to Ververica…');
    document.getElementById('connect-btn').disabled = true;

    const ok = await testVvpConnection();
    if (!ok) {
        document.getElementById('connect-btn').disabled = false;
        return;
    }

    // Store everything in state.gateway
    state.gateway = {
        host:    new URL(baseUrl).hostname,
        port:    new URL(baseUrl).port || (baseUrl.startsWith('https') ? '443' : '80'),
        baseUrl: baseUrl,
        mode:    'ververica',
        vvpConfig: {
            namespace:     ns,
            deployTarget,
            sessionCluster,
            flinkVersion,
            model:         typeof _vvpModel !== 'undefined' ? _vvpModel : 'selfmanaged',
        },
    };

    // No real "session" concept — synthesise one for display
    state.activeSession  = `vvp:${ns}`;
    state.isAdminSession = false;
    state.sessions = [{
        handle:  `vvp:${ns}`,
        name:    `Ververica (${ns})`,
        created: new Date(),
    }];

    launchApp(state.gateway.host, state.gateway.port);
    toast(`Connected to Ververica — namespace: ${ns}`, 'ok');
    addLog('OK', [
        `Ververica connected.`,
        `Base URL: ${baseUrl}`,
        `Namespace: ${ns}`,
        deployTarget   ? `Deployment Target: ${deployTarget}`   : null,
        sessionCluster ? `Session Cluster:   ${sessionCluster}` : null,
        `Flink Version: ${flinkVersion}`,
        `Model: ${state.gateway.vvpConfig.model}`,
    ].filter(Boolean).join(' | '));

    addLog('INFO', [
        'Ververica uses deployment-based SQL execution — not SQL Gateway sessions.',
        'To submit SQL, go to ▶ Pipeline Manager or use INSERT INTO directly;',
        'the studio will POST a SQLSCRIPT deployment to:',
        `${baseUrl}/api/v1/namespaces/${ns}/deployments`,
    ].join(' '));
}


// AWS MANAGED FLINK — connection helpers

/**
 * AWS Managed Flink has NO Flink SQL Gateway REST API.
 * The "connect" flow here:
 *   1. Records the AWS config in state.gateway for reference.
 *   2. For Studio notebooks: opens the Zeppelin URL in a new tab (user works there).
 *   3. For Application mode: reminds user to use Direct Gateway mode after port-forwarding.
 *
 * Str:::lab Studio cannot make signed AWS API calls from the browser (no IAM SigV4 signing
 * and no CORS support from kinesisanalyticsv2.amazonaws.com). All management is via AWS CLI/SDK.
 */
async function doAwsConnect() {
    const region    = (document.getElementById('inp-aws-region')?.value    || '').trim();
    const appName   = (document.getElementById('inp-aws-app-name')?.value  || '').trim();
    const zepUrl    = (document.getElementById('inp-aws-zeppelin-url')?.value || '').trim();
    const flinkRest = (document.getElementById('inp-aws-flink-rest')?.value   || '').trim();
    const awsMode   = typeof _awsMode !== 'undefined' ? _awsMode : 'studio';

    if (!region || !appName) {
        setConnectStatus('err', 'Please enter AWS Region and Application Name.');
        return;
    }

    setConnectStatus('loading', 'Setting up AWS Managed Flink connection…');
    document.getElementById('connect-btn').disabled = true;

    if (awsMode === 'studio' && zepUrl) {
        // Studio notebook: if user has provided the Zeppelin URL, try to reach it
        setConnectStatus('loading', `Checking Zeppelin at ${zepUrl} …`);
        try {
            await fetchWithTimeout(zepUrl, { mode: 'no-cors' }, 8000);
            setConnectStatus('ok', `✓ Zeppelin URL accepted — opening notebook in new tab…`);
            toast('Opening Studio notebook in new tab', 'ok');
            window.open(zepUrl, '_blank');
        } catch (e) {
            // no-cors fetch always "fails" — but if the URL is reachable it just returns opaque
            toast('Zeppelin URL saved — open the notebook manually if needed', 'info');
            setConnectStatus('ok', `Zeppelin URL saved — click Open Notebook to launch`);
        }
    } else if (awsMode === 'app' && flinkRest) {
        // Application mode with a Flink REST UI URL — treat as a direct gateway connection
        setConnectStatus('loading', `Probing Flink REST at ${flinkRest}/v1/info …`);
        try {
            const resp = await fetchWithTimeout(`${flinkRest.replace(/\/$/, '')}/v1/info`, {
                headers: { Accept: 'application/json' }
            }, 8000);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const info = await resp.json();
            const ver  = info.flinkVersion || info.version || 'unknown';
            setConnectStatus('ok', `✓ Flink REST reachable — Flink ${ver} — click Connect`);
            toast('Flink REST UI reachable', 'ok');

            // Switch to direct mode and connect through it
            connMode = 'direct';
            const url = new URL(flinkRest.replace(/\/$/, ''));
            if (document.getElementById('inp-host'))
                document.getElementById('inp-host').value = url.hostname;
            if (document.getElementById('inp-port'))
                document.getElementById('inp-port').value = url.port || '8081';
            document.getElementById('connect-btn').disabled = false;
            return; // Let user click Connect again in direct mode
        } catch (e) {
            setConnectStatus('err', [
                `Could not reach Flink REST at ${flinkRest}: ${e.message}`,
                'Ensure you have a VPC tunnel or port-forward active.',
                'See the note in the AWS panel for options.',
            ].join('<br>'));
            document.getElementById('connect-btn').disabled = false;
            return;
        }
    }

    // Record AWS config in state for reference / CLI generation
    state.gateway = {
        host:    `kinesisanalyticsv2.${region}.amazonaws.com`,
        port:    '443',
        baseUrl: `https://kinesisanalyticsv2.${region}.amazonaws.com`,
        mode:    'aws',
        awsConfig: {
            region,
            appName,
            zepUrl,
            flinkRest,
            awsMode,
            authType: typeof _awsAuthType !== 'undefined' ? _awsAuthType : 'profile',
            accessKeyId:     (document.getElementById('inp-aws-key-id')?.value     || '').trim(),
            sessionToken:    (document.getElementById('inp-aws-session-token')?.value || '').trim(),
        },
    };

    state.activeSession  = `aws:${appName}`;
    state.isAdminSession = false;
    state.sessions = [{
        handle:  `aws:${appName}`,
        name:    `AWS Managed Flink (${appName})`,
        created: new Date(),
    }];

    launchApp(state.gateway.host, state.gateway.port);

    // Log actionable information
    addLog('WARN', [
        'AWS Managed Flink: No Flink SQL Gateway REST API is exposed.',
        'SQL execution in Str:::lab Studio is not available via the browser for this provider.',
    ].join(' '));

    addLog('INFO', [
        `Application: ${appName} | Region: ${region}`,
        zepUrl    ? `Zeppelin URL: ${zepUrl}` : null,
        flinkRest ? `Flink REST:   ${flinkRest}` : null,
    ].filter(Boolean).join(' | '));

    addLog('INFO', 'To run interactive Flink SQL: open the Studio notebook in the AWS Console → Apache Zeppelin.');
    addLog('INFO', [
        'AWS CLI quick reference:',
        `  aws kinesisanalyticsv2 start-application --application-name ${appName} --region ${region}`,
        `  aws kinesisanalyticsv2 describe-application --application-name ${appName} --region ${region}`,
    ].join('\n'));

    toast(`AWS Managed Flink config saved — ${appName} (${region})`, 'info');
    document.getElementById('connect-btn').disabled = false;
}


// ═══════════════════════════════════════════════════════════════════════════════
// UPDATED setMode() — add 'ververica' and 'aws' branches
// Replace the existing setMode() in connection.js with this version.
// ═══════════════════════════════════════════════════════════════════════════════

function setMode(mode) {
    connMode = mode;
    ['proxy','direct','confluent','ververica','aws'].forEach(m => {
        const btn  = document.getElementById('mode-' + m);
        const info = document.getElementById('mode-' + m + '-info');
        if (btn)  btn.classList.toggle('active', m === mode);
        if (info) info.style.display = (m === mode) ? 'block' : 'none';
    });

    // Hide session block for cloud providers that don't use SQL Gateway sessions
    const sessArea = document.getElementById('session-mode-row');
    const hideSession = ['confluent', 'ververica', 'aws'].includes(mode);
    if (sessArea) sessArea.style.display = hideSession ? 'none' : '';

    // Also hide Advanced Properties for cloud providers (not applicable)
    const propsArea = document.querySelector('.connect-props')?.closest('.field-group');
    if (propsArea) propsArea.style.display = hideSession ? 'none' : '';

    // Update connect button label
    const connectBtn = document.getElementById('connect-btn');
    if (connectBtn) {
        const labels = {
            confluent: 'Connect to Confluent Cloud',
            ververica: 'Connect to Ververica',
            aws:       'Save AWS Config & Open',
            proxy:     'Connect & Open Studio',
            direct:    'Connect & Open Studio',
        };
        connectBtn.textContent = labels[mode] || 'Connect & Open Studio';
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
// UPDATED doConnect() — routes all 6 modes
// Replace the existing doConnect() in connection.js with this version.
// ═══════════════════════════════════════════════════════════════════════════════

async function doConnect() {
    if (connMode === 'confluent')  { await doConfluentConnect(); return; }
    if (connMode === 'ververica')  { await doVvpConnect();       return; }
    if (connMode === 'aws')        { await doAwsConnect();        return; }

    // ── Original proxy / direct / remote flow (unchanged) ────────────────────
    const baseUrl     = getBaseUrl();
    const host        = connMode === 'proxy'
        ? window.location.hostname
        : (document.getElementById('inp-host')?.value.trim() || 'localhost');
    const port        = connMode === 'proxy'
        ? (window.location.port || '80')
        : (document.getElementById('inp-port')?.value.trim() || '8084');
    const sessionName = (document.getElementById('inp-session-name')?.value || '').trim();
    const propsRaw    = (document.getElementById('inp-props')?.value || '').trim();

    setConnectStatus('loading', `Connecting via ${baseUrl} …`);
    document.getElementById('connect-btn').disabled = true;

    state.gateway = { host, port, baseUrl };

    try {
        const authHeaders = getAuthHeaders();
        const verifyResp  = await fetchWithTimeout(`${baseUrl}/v1/info`, {
            headers: { Accept: 'application/json', ...authHeaders }
        }, 8000);
        if (!verifyResp.ok) throw new Error(`Gateway returned HTTP ${verifyResp.status} — is it fully started?`);

        if (_sessionMode === 'admin') {
            const passcode = (document.getElementById('inp-admin-pass')?.value || '').trim();
            if (!passcode) throw new Error('Admin passcode is required.');
            if (passcode !== _adminPasscode) throw new Error('Incorrect admin passcode.');
            const sessResp = await api('POST', '/v1/sessions', { sessionName: 'admin-session' });
            state.activeSession  = sessResp.sessionHandle;
            state.isAdminSession = true;
            state.adminName      = (document.getElementById('inp-admin-name')?.value || '').trim() || 'Admin';
            state.sessions = [{ handle: sessResp.sessionHandle, name: 'admin-session', created: new Date(), isAdmin: true }];
            launchApp(host, port);
            toast(`Admin session started — welcome, ${state.adminName}`, 'ok');
            addLog('OK', `Admin session created: ${shortHandle(sessResp.sessionHandle)}`);

        } else if (_sessionMode === 'existing') {
            const existingHandle = (document.getElementById('inp-session-handle')?.value || '').trim();
            if (!existingHandle || !existingHandle.includes('-')) {
                throw new Error('Please paste a valid session UUID handle.');
            }
            setConnectStatus('loading', 'Verifying existing session…');
            try { await api('POST', `/v1/sessions/${existingHandle}/heartbeat`); }
            catch(e) { throw new Error(`Session '${existingHandle.slice(0,8)}…' does not exist or has expired.`); }
            state.activeSession  = existingHandle;
            state.isAdminSession = false;
            state.sessions = [{ handle: existingHandle, name: 'reconnected', created: new Date() }];
            launchApp(host, port);
            toast('Reconnected to existing session', 'ok');
            addLog('OK', `Reconnected to session ${existingHandle.slice(0,8)}…`);

        } else {
            const props = parseProps(propsRaw);
            const sessionBody = {};
            if (sessionName) sessionBody.sessionName = sessionName;
            if (Object.keys(props).length) sessionBody.properties = props;
            const sessResp = await api('POST', '/v1/sessions', sessionBody);
            if (!sessResp || !sessResp.sessionHandle) throw new Error('Gateway did not return a session handle.');
            state.activeSession  = sessResp.sessionHandle;
            state.isAdminSession = false;
            state.sessions = [{ handle: sessResp.sessionHandle, name: sessionName || 'default', created: new Date() }];
            launchApp(host, port);
            toast(`Session '${sessionName || sessResp.sessionHandle.slice(0,8)}' created`, 'ok');
        }
    } catch (e) {
        setConnectStatus('err', `Failed: ${e.message}`);
        document.getElementById('connect-btn').disabled = false;
        state.gateway = null;
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
// UPDATED testConnection() — handles all 6 modes
// Replace the existing testConnection() in connection.js with this version.
// ═══════════════════════════════════════════════════════════════════════════════

async function testConnection() {
    if (connMode === 'ververica') { await testVvpConnection(); return; }
    if (connMode === 'aws') {
        const zepUrl = (document.getElementById('inp-aws-zeppelin-url')?.value || '').trim();
        if (zepUrl) {
            setConnectStatus('loading', `Pinging ${zepUrl} …`);
            try {
                await fetchWithTimeout(zepUrl, { mode: 'no-cors' }, 8000);
                setConnectStatus('ok', `✓ Zeppelin URL accepted (no-cors — browser cannot verify further)`);
            } catch(e) {
                setConnectStatus('err', `Could not reach Zeppelin URL: ${e.message}`);
            }
        } else {
            setConnectStatus('ok', 'AWS Managed Flink — enter a Zeppelin URL above to test, or use the AWS CLI to verify.');
        }
        return;
    }

    // Confluent Cloud test
    if (connMode === 'confluent') {
        const orgId = (document.getElementById('inp-cf-org')?.value || '').trim();
        const envId = (document.getElementById('inp-cf-env')?.value || '').trim();
        if (!orgId || !envId) {
            setConnectStatus('err', 'Please fill in Organization ID and Environment ID first.');
            return;
        }
        const testUrl = `${_getConfluentSqlBase()}/statements?page_size=1`;
        setConnectStatus('loading', `Testing Confluent Cloud…`);
        try {
            const resp = await fetchWithTimeout(testUrl, {
                headers: { 'Accept': 'application/json', ...getAuthHeaders() }
            }, 10000);
            if (resp.status === 401) { setConnectStatus('err', '401 Unauthorized — check Flink API Key and Secret.'); return; }
            if (resp.status === 403) { setConnectStatus('err', '403 Forbidden — key lacks access to this environment.'); return; }
            if (!resp.ok) { setConnectStatus('err', `HTTP ${resp.status} — check Org ID, Env ID, region, and provider.`); return; }
            setConnectStatus('ok', `✓ Confluent Cloud reachable — click Connect`);
            toast('Confluent Cloud connection test passed', 'ok');
        } catch (e) {
            setConnectStatus('err', `${e.message}<br>Check region, provider, and network.`);
        }
        return;
    }

    // Standard Flink SQL Gateway test (proxy / direct)
    const baseUrl = getBaseUrl();
    const authHeaders = getAuthHeaders();
    setConnectStatus('loading', `Testing ${baseUrl}/v1/info …`);
    try {
        const resp = await fetchWithTimeout(`${baseUrl}/v1/info`, {
            headers: { 'Accept': 'application/json', ...authHeaders }
        }, 8000);
        if (!resp.ok) {
            let msg = `HTTP ${resp.status}`;
            if (resp.status === 502) msg = `502 — Flink SQL Gateway still starting. Wait 30s and retry.`;
            if (resp.status === 503) msg = `503 — Flink SQL Gateway not ready yet.`;
            setConnectStatus('err', msg);
            return;
        }
        const info = await resp.json();
        const ver = info.flinkVersion || info.version || 'unknown';
        setConnectStatus('ok', `✓ Gateway reachable! Flink ${ver} — click Connect`);
        toast('Connection test passed', 'ok');
        loadExistingSessionsFromGateway(baseUrl);
    } catch (e) {
        let hint = '';
        if (connMode === 'proxy') {
            hint = `<br>Make sure you opened the IDE at <strong>http://localhost:3030</strong>.<br>Run: <strong>docker compose ps</strong> — both containers must be Up.`;
        } else {
            hint = `<br>Check host/port and that the CORS proxy is running.`;
        }
        setConnectStatus('err', `${e.message}${hint}`);
    }
}