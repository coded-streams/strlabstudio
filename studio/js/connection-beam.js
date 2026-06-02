/* ============================================================
   js/connection-beam.js
   Str:::Beam Engine — auth, tenant welcome screen, session launch
   ============================================================ */

'use strict';

/* ── UI HELPERS ────────────────────────────────────────────── */

function beamUpdateEndpointPreview() {
    const url = (document.getElementById('inp-beam-url')?.value || '').trim().replace(/\/$/, '');
    const el  = document.getElementById('beam-endpoint-preview');
    if (el) el.textContent = (url || 'http://localhost:8090') + '/api/v1/auth/…';
}

function beamToggleApiKeyForm() {
    const form  = document.getElementById('beam-apikey-form');
    const arrow = document.getElementById('beam-apikey-arrow');
    if (!form) return;
    const isOpen = form.style.display === 'flex';
    form.style.display = isOpen ? 'none' : 'flex';
    if (arrow) arrow.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(180deg)';
}

function beamGetBaseUrl() {
    return (document.getElementById('inp-beam-url')?.value || '')
            .trim()
            .replace(/\/$/, '')
        || 'http://localhost:8090';
}

/* ── API: GET /api/v1/auth/providers ───────────────────────── */
async function beamLoadProviders() {
    const url = beamGetBaseUrl();
    try {
        const r = await fetch(`${url}/api/v1/auth/providers`, {
            headers: { 'Accept': 'application/json' }
        });
        if (!r.ok) return;
        const data = await r.json();
        if (typeof addLog === 'function') {
            addLog('INFO', `Str:::Beam: providers available — ${JSON.stringify(data)}`);
        }
        return data;
    } catch (_) {}
}

/* ── SSO LOGIN - Same tab redirect, NO POPUP ───────────────── */
function beamInitiateSSO(provider) {
    const url = beamGetBaseUrl();
    if (!url) {
        if (typeof toast === 'function') toast('Enter the Str:::Beam Engine URL first', 'err');
        return;
    }
    try {
        sessionStorage.setItem('strbeam_baseurl', url);
        sessionStorage.setItem('strbeam_oauth_return_url', window.location.href);
    } catch (_) {}

    const ssoEndpoints = { google: '/api/v1/auth/google', github: '/api/v1/auth/github' };
    const endpoint = ssoEndpoints[provider];
    if (!endpoint) {
        if (typeof toast === 'function') toast(`Unknown SSO provider: ${provider}`, 'err');
        return;
    }

    if (typeof setConnectStatus === 'function') {
        setConnectStatus('loading', `Redirecting to ${provider} sign-in…`);
    }

    // Same-tab redirect - NO POPUP
    window.location.href = `${url}${endpoint}`;
}

/* ── OAUTH CALLBACK HANDLER ───────────────────────────────── */
async function beamHandleOAuthCallback() {
    const hash = window.location.hash;
    if (!hash.includes('token=')) return false;

    const params = new URLSearchParams(hash.slice(1));
    const jwt    = params.get('token');
    if (!jwt) return false;

    history.replaceState(null, '', window.location.pathname + window.location.search);

    const baseUrl = (() => {
        try { return sessionStorage.getItem('strbeam_baseurl') || 'http://localhost:8090'; }
        catch (_) { return 'http://localhost:8090'; }
    })();

    await _beamProcessToken(jwt, baseUrl);
    return true;
}

async function _beamProcessToken(jwt, baseUrl) {
    if (typeof setConnectStatus === 'function') {
        setConnectStatus('loading', 'Loading your Str:::Beam workspace…');
    }

    const headers = {
        'Accept': 'application/json',
        'Authorization': `Bearer ${jwt}`,
    };

    try {
        let tenant = null;
        try {
            const rMe = await fetch(`${baseUrl}/api/v1/tenants/me`, { headers });
            if (rMe.ok) tenant = await rMe.json();
        } catch (_) {}

        if (!tenant) throw new Error('Could not load tenant information.');

        if (!window.state) window.state = {};
        window.state.beam = { baseUrl, jwt, tenant, authMethod: 'sso' };

        await beamShowTenantWelcome(tenant, jwt, baseUrl);

    } catch (e) {
        if (typeof setConnectStatus === 'function') {
            setConnectStatus('err', `Str:::Beam login failed: ${e.message}`);
        }
        if (typeof addLog === 'function') {
            addLog('ERR', `Beam SSO error: ${e.message}`);
        }
    }
}

/* ── API KEY LOGIN ─────────────────────────────────────────── */
async function beamConnectWithApiKey() {
    const url       = beamGetBaseUrl();
    const tenantKey = (document.getElementById('inp-beam-tenant-key')?.value || '').trim();
    const apiKey    = (document.getElementById('inp-beam-apikey')?.value     || '').trim();

    if (!url)       { if (typeof toast === 'function') toast('Enter the Str:::Beam Engine URL', 'err'); return; }
    if (!tenantKey) { if (typeof toast === 'function') toast('Enter your tenant key', 'err'); return; }
    if (!apiKey)    { if (typeof toast === 'function') toast('Enter your API key', 'err'); return; }

    if (typeof setConnectStatus === 'function') {
        setConnectStatus('loading', 'Authenticating with Str:::Beam…');
    }

    try {
        const r = await fetch(`${url}/api/v1/tenants/${tenantKey}`, {
            headers: {
                'Accept': 'application/json',
                'Authorization': `Bearer ${apiKey}`,
            }
        });

        if (r.status === 401 || r.status === 403) {
            throw new Error('Invalid API key or tenant key.');
        }
        if (r.status === 404) {
            throw new Error(`Tenant "${tenantKey}" not found.`);
        }
        if (!r.ok) {
            throw new Error(`Str:::Beam returned HTTP ${r.status}`);
        }

        const tenant = await r.json();

        if (tenant.status === 'SUSPENDED') {
            throw new Error(`Tenant "${tenantKey}" is suspended.`);
        }
        if (tenant.status === 'DELETED') {
            throw new Error(`Tenant "${tenantKey}" has been deleted.`);
        }

        if (!window.state) window.state = {};
        window.state.beam = { baseUrl: url, jwt: apiKey, tenant, authMethod: 'apikey' };

        await beamShowTenantWelcome(tenant, apiKey, url);

    } catch (e) {
        if (typeof setConnectStatus === 'function') {
            setConnectStatus('err', `Str:::Beam: ${e.message}`);
        }
        if (typeof toast === 'function') toast(e.message, 'err');
        if (typeof addLog === 'function') addLog('ERR', `Beam auth failed: ${e.message}`);
    }
}

/* ── TENANT WELCOME SCREEN ─────────────────────────────────── */
async function beamShowTenantWelcome(tenant, jwt, baseUrl) {
    const headers = {
        'Accept': 'application/json',
        'Authorization': `Bearer ${jwt}`,
    };

    const [usageRes, jobsRes, pricingRes] = await Promise.allSettled([
        fetch(`${baseUrl}/api/v1/metrics/tenants/${tenant.tenantKey}/usage`, { headers }),
        fetch(`${baseUrl}/api/v1/jobs`, { headers }),
        fetch(`${baseUrl}/api/v1/pricing/tiers`),
    ]);

    const usage   = usageRes.status   === 'fulfilled' && usageRes.value.ok   ? await usageRes.value.json()   : null;
    const jobs    = jobsRes.status    === 'fulfilled' && jobsRes.value.ok    ? await jobsRes.value.json()    : null;
    const pricing = pricingRes.status === 'fulfilled' && pricingRes.value.ok ? await pricingRes.value.json() : null;

    _beamInjectWelcomeScreen(tenant, jwt, baseUrl, usage, jobs, pricing);
}

function _beamInjectWelcomeScreen(tenant, jwt, baseUrl, usage, jobs, pricing) {
    // Remove any existing overlay
    const existing = document.getElementById('beam-welcome-overlay');
    if (existing) existing.remove();

    // Hide connect screen
    const cs = document.getElementById('connect-screen');
    if (cs) cs.style.display = 'none';

    // Show the main app (which contains the dashboard)
    const app = document.getElementById('app');
    if (app) app.classList.add('visible');

    // CRITICAL FIX: Don't automatically launch Studio
    // Only store the data and show the dashboard

    // Store data for the existing dashboard
    window.beamWelcomeData = { tenant, jwt, baseUrl, usage, jobs, pricing };

    // Manually populate the dashboard
    if (typeof populateAll === 'function') {
        populateAll();
    } else if (window.populateAll) {
        window.populateAll();
    } else if (window.populate) {
        window.populate();
    }

    // Update code snippets
    if (typeof updateCodeSnippets === 'function') {
        updateCodeSnippets();
    } else if (window.updateCodeSnippets) {
        window.updateCodeSnippets();
    }

    // Show overview section
    if (typeof showSection === 'function') {
        showSection('overview');
    } else if (window.showSection) {
        window.showSection('overview');
    }

    // Update topbar with tenant info
    const navTenant = document.getElementById('nav-tenant-key');
    if (navTenant && tenant.tenantKey) {
        navTenant.textContent = tenant.tenantKey;
        const envDiv = document.getElementById('topnav-env');
        if (envDiv) envDiv.style.display = 'flex';
    }

    // Store callback for Connect button - DON'T call it automatically
    window._beamWelcomeConnectCallback = async function({ tenant: t, jwt: j, baseUrl: u }) {
        if (!window.state) window.state = {};
        window.state.beam = { baseUrl: u, jwt: j, tenant: t, authMethod: 'sso' };

        const remoteInput = document.getElementById('inp-remote-url');
        if (remoteInput) remoteInput.value = u;
        const tokenInput = document.getElementById('inp-token');
        if (tokenInput) tokenInput.value = j;
        const sessNameInput = document.getElementById('inp-session-name');
        if (sessNameInput) sessNameInput.value = t.tenantKey || '';

        if (typeof setMode === 'function') setMode('remote');
        if (typeof setAuthMode === 'function') setAuthMode('bearer');

        if (typeof doConnect === 'function') {
            try {
                await doConnect();
            } catch (e) {
                if (typeof toast === 'function') toast('Could not open session: ' + e.message, 'err');
                if (cs) cs.style.display = 'flex';
                if (app) app.classList.remove('visible');
            }
        }
    };

    // REMOVED: The automatic call to launch Studio
    // The user must now explicitly click "Connect to Studio" button
}

/* ── LAUNCH STUDIO ─────────────────────────────────────────── */
async function beamLaunchStudio() {
    const beam = window.state?.beam;
    if (!beam) return;

    if (typeof window._beamWelcomeConnectCallback === 'function') {
        await window._beamWelcomeConnectCallback({
            tenant:  beam.tenant,
            jwt:     beam.jwt,
            baseUrl: beam.baseUrl,
        });
    }
}

/* ── LOGOUT ────────────────────────────────────────────────── */
async function beamLogout() {
    const beam = window.state?.beam;

    if (beam?.baseUrl && beam?.jwt) {
        try {
            await fetch(`${beam.baseUrl}/api/v1/auth/logout`, {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${beam.jwt}` }
            });
        } catch (_) {}
    }

    if (window.state) window.state.beam = null;
    try { sessionStorage.removeItem('strbeam_baseurl'); } catch (_) {}
    try { sessionStorage.removeItem('strbeam_pending_auth'); } catch (_) {}

    const overlay = document.getElementById('beam-welcome-overlay');
    if (overlay) overlay.remove();

    const cs = document.getElementById('connect-screen');
    if (cs) cs.style.display = 'flex';

    const app = document.getElementById('app');
    if (app) app.classList.remove('visible');

    if (typeof toast === 'function') toast('Signed out of Str:::Beam', 'info');
}

/* ── UPGRADE INFO ──────────────────────────────────────────── */
async function beamOpenUpgradeInfo(currentTier) {
    const baseUrl = window.state?.beam?.baseUrl || beamGetBaseUrl();
    try {
        const r = await fetch(`${baseUrl}/api/v1/pricing/tiers`);
        const tiers = r.ok ? await r.json() : null;
        const msg = tiers
            ? `Upgrade options:\n${JSON.stringify(tiers, null, 2)}`
            : `Contact sales to upgrade from ${currentTier}.`;
        alert(msg);
    } catch (_) {
        alert(`Contact sales to upgrade from ${currentTier} to PRO or ENTERPRISE.`);
    }
}

/* ── PAGE LOAD INIT ────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', function () {
    beamHandleOAuthCallback();
});