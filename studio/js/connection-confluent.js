// ── CONNECTION MODE
// Modes: 'proxy' | 'direct' | 'confluent' | 'remote'
let connMode = 'proxy';
let authMode = 'bearer';

function setMode(mode) {
    connMode = mode;
    ['proxy','direct','confluent','remote'].forEach(m => {
        const btn  = document.getElementById('mode-' + m);
        const info = document.getElementById('mode-' + m + '-info');
        if (btn)  btn.classList.toggle('active', m === mode);
        if (info) info.style.display = (m === mode) ? 'block' : 'none';
    });

    // Hide session mode tabs that don't apply to Confluent Cloud
    const sessArea = document.getElementById('session-mode-row');
    if (sessArea) sessArea.style.display = (mode === 'confluent') ? 'none' : '';

    // Show/hide connect button label
    const connectBtn = document.getElementById('connect-btn');
    if (connectBtn) {
        connectBtn.textContent = (mode === 'confluent')
            ? 'Connect to Confluent Cloud'
            : 'Connect & Open Studio';
    }
}

function setAuthMode(mode) {
    authMode = mode;
    ['none','bearer','basic'].forEach(m => {
        const btn = document.getElementById('auth-' + m);
        if (btn) btn.classList.toggle('active', m === mode);
    });
    const bearerF = document.getElementById('auth-bearer-fields');
    const basicF  = document.getElementById('auth-basic-fields');
    if (bearerF) bearerF.style.display = mode === 'bearer' ? 'block' : 'none';
    if (basicF)  basicF.style.display  = mode === 'basic'  ? 'block' : 'none';
}

// ── URL + AUTH HELPERS

function _getConfluentBaseUrl() {
    const region   = (document.getElementById('inp-cf-region')?.value   || '').trim().toLowerCase();
    const provider = (document.getElementById('inp-cf-provider')?.value || '').trim().toLowerCase();
    const priv     = document.getElementById('inp-cf-private')?.checked;
    const host = priv
        ? `https://flink.${region}.${provider}.private.confluent.cloud`
        : `https://flink.${region}.${provider}.confluent.cloud`;
    return host;
}

function _getConfluentSqlBase() {
    const orgId = (document.getElementById('inp-cf-org')?.value || '').trim();
    const envId = (document.getElementById('inp-cf-env')?.value || '').trim();
    return `${_getConfluentBaseUrl()}/sql/v1/organizations/${orgId}/environments/${envId}`;
}

function getBaseUrl() {
    if (connMode === 'proxy')     return `${window.location.protocol}//${window.location.host}/flink-api`;
    if (connMode === 'confluent') return _getConfluentBaseUrl();
    if (connMode === 'remote') {
        const url = (document.getElementById('inp-remote-url')?.value || '').trim();
        return url.replace(/\/$/, '');
    }
    const host = document.getElementById('inp-host')?.value.trim() || 'localhost';
    const port = document.getElementById('inp-port')?.value.trim() || '8084';
    return `http://${host}:${port}`;
}

function getAuthHeaders() {
    if (connMode === 'confluent') {
        const key    = (document.getElementById('inp-cf-apikey')?.value    || '').trim();
        const secret = (document.getElementById('inp-cf-apisecret')?.value || '').trim();
        if (key && secret) {
            return { 'Authorization': 'Basic ' + btoa(key + ':' + secret) };
        }
        return {};
    }
    if (connMode !== 'remote') return {};
    if (authMode === 'bearer') {
        const token = (document.getElementById('inp-token')?.value || '').trim();
        return token ? { 'Authorization': 'Bearer ' + token } : {};
    }
    if (authMode === 'basic') {
        const user = (document.getElementById('inp-basic-user')?.value || '').trim();
        const pass = (document.getElementById('inp-basic-pass')?.value || '').trim();
        if (user) return { 'Authorization': 'Basic ' + btoa(user + ':' + pass) };
    }
    return {};
}

// ── TEST CONNECTION 

async function testConnection() {
    const baseUrl = getBaseUrl();
    const authHeaders = getAuthHeaders();

    if (connMode === 'confluent') {
        // For Confluent Cloud, test by GETting the statements list endpoint
        const orgId = (document.getElementById('inp-cf-org')?.value || '').trim();
        const envId = (document.getElementById('inp-cf-env')?.value || '').trim();
        if (!orgId || !envId) {
            setConnectStatus('err', 'Please fill in Organization ID and Environment ID first.');
            return;
        }
        const testUrl = `${_getConfluentSqlBase()}/statements?page_size=1`;
        setConnectStatus('loading', `Testing Confluent Cloud connection…`);
        try {
            const resp = await fetchWithTimeout(testUrl, {
                headers: { 'Accept': 'application/json', ...authHeaders }
            }, 10000);
            if (resp.status === 401) {
                setConnectStatus('err', '401 Unauthorized — check your Flink API Key and Secret.');
                return;
            }
            if (resp.status === 403) {
                setConnectStatus('err', '403 Forbidden — your API key lacks access to this environment.');
                return;
            }
            if (!resp.ok) {
                setConnectStatus('err', `HTTP ${resp.status} — check Org ID, Env ID, region, and provider.`);
                return;
            }
            setConnectStatus('ok', `✓ Confluent Cloud reachable — click Connect to Confluent Cloud`);
            toast('Confluent Cloud connection test passed', 'ok');
        } catch (e) {
            setConnectStatus('err', `${e.message}<br>Check your region, provider, and network connectivity.`);
        }
        return;
    }

    // Standard gateway test
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
        setConnectStatus('ok', `✓ Gateway reachable! &nbsp; Flink ${ver} &nbsp; — click Connect`);
        toast('Connection test passed', 'ok');
        loadExistingSessionsFromGateway(baseUrl);
    } catch (e) {
        let hint = '';
        if (connMode === 'proxy') {
            hint = `<br>Make sure you opened the IDE at <strong>http://localhost:3030</strong> (not as a local file).<br>Run: <strong>docker compose ps</strong> — flink-studio and flink-sql-gateway must be Up.`;
        } else {
            hint = `<br>Check host/port and that the CORS proxy is running.`;
        }
        setConnectStatus('err', `${e.message}${hint}`);
    }
}

// ── CONFLUENT CLOUD CONNECT 

async function doConfluentConnect() {
    const orgId      = (document.getElementById('inp-cf-org')?.value      || '').trim();
    const envId      = (document.getElementById('inp-cf-env')?.value      || '').trim();
    const region     = (document.getElementById('inp-cf-region')?.value   || '').trim();
    const provider   = (document.getElementById('inp-cf-provider')?.value || '').trim().toLowerCase();
    const poolId     = (document.getElementById('inp-cf-pool')?.value     || '').trim();
    const apiKey     = (document.getElementById('inp-cf-apikey')?.value   || '').trim();
    const apiSecret  = (document.getElementById('inp-cf-apisecret')?.value|| '').trim();

    if (!orgId || !envId || !region || !provider || !apiKey || !apiSecret) {
        setConnectStatus('err', 'Please fill in all required Confluent Cloud fields.');
        return;
    }

    setConnectStatus('loading', 'Connecting to Confluent Cloud…');
    document.getElementById('connect-btn').disabled = true;

    try {
        const sqlBase = _getConfluentSqlBase();
        const authHeaders = getAuthHeaders();

        // Verify connectivity by listing statements (page_size=1)
        const verifyResp = await fetchWithTimeout(`${sqlBase}/statements?page_size=1`, {
            headers: { 'Accept': 'application/json', ...authHeaders }
        }, 10000);

        if (verifyResp.status === 401) throw new Error('401 Unauthorized — check your Flink API Key and Secret.');
        if (verifyResp.status === 403) throw new Error('403 Forbidden — your key lacks access to this environment.');
        if (!verifyResp.ok) throw new Error(`HTTP ${verifyResp.status} — check your Org ID, Env ID, region, and provider.`);

        // Store Confluent-specific state
        state.gateway = {
            host: `flink.${region}.${provider}.confluent.cloud`,
            port: '443',
            baseUrl: _getConfluentBaseUrl(),
            mode: 'confluent',
            confluentConfig: { orgId, envId, region, provider, poolId, sqlBase },
        };
        state.activeSession  = `confluent:${envId}`;
        state.isAdminSession = false;
        state.sessions = [{
            handle: `confluent:${envId}`,
            name: `Confluent Cloud (${envId})`,
            created: new Date(),
        }];

        launchApp(`flink.${region}.${provider}.confluent.cloud`, '443');
        toast(`Connected to Confluent Cloud — env ${envId}`, 'ok');
        addLog('OK', `Confluent Cloud connected. Org: ${orgId} | Env: ${envId} | Region: ${region} | Provider: ${provider.toUpperCase()}${poolId ? ' | Pool: ' + poolId : ''}`);

    } catch (e) {
        setConnectStatus('err', `Failed: ${e.message}`);
        document.getElementById('connect-btn').disabled = false;
        state.gateway = null;
    }
}

// ── MAIN doConnect — routes to Confluent or standard

async function doConnect() {
    if (connMode === 'confluent') {
        await doConfluentConnect();
        return;
    }

    // ── Original doConnect logic below (unchanged)
    const baseUrl   = getBaseUrl();
    const host      = connMode === 'proxy' ? window.location.hostname : (document.getElementById('inp-host')?.value.trim() || 'localhost');
    const port      = connMode === 'proxy' ? (window.location.port || '80') : (document.getElementById('inp-port')?.value.trim() || '8084');
    const sessionName = (document.getElementById('inp-session-name')?.value || '').trim();
    const propsRaw  = (document.getElementById('inp-props')?.value || '').trim();

    setConnectStatus('loading', `Connecting via ${baseUrl} …`);
    document.getElementById('connect-btn').disabled = true;

    state.gateway = { host, port, baseUrl };

    try {
        const authHeaders = getAuthHeaders();
        const verifyResp = await fetchWithTimeout(`${baseUrl}/v1/info`, {
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
            try {
                await api('POST', `/v1/sessions/${existingHandle}/heartbeat`);
            } catch(e) {
                throw new Error(`Session '${existingHandle.slice(0,8)}…' does not exist or has expired.`);
            }
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