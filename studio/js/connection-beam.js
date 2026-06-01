/* ============================================================
   js/connection-beam.js
   Str:::Beam Engine — auth, tenant welcome screen, session launch
   All API calls use the Str:::Beam OpenAPI spec (default: localhost:8090)

   HOW THIS FILE IS USED
   ─────────────────────
   1. Drop this file into your js/ folder
   2. In index.html, find the commented-out line:
        <!--<script src="js/connection-beam.js"></script>-->
      Uncomment it (remove <!-- and -->)
   3. Remove the large inline <script> block at the bottom of
      index.html that contains all the beam* functions — this
      file replaces it entirely.
   4. Everything else (HTML panel, button, connection.js patches)
      stays exactly as you already have it.

   WHAT THIS FILE DOES
   ───────────────────
   • beamUpdateEndpointPreview()  — live URL preview as user types
   • beamToggleApiKeyForm()       — expand/collapse API key form
   • beamLoadProviders()          — GET /api/v1/auth/providers
   • beamInitiateSSO(provider)    — redirect to Google or GitHub OAuth2
   • beamHandleOAuthCallback()    — read #token=… after OAuth redirect
   • beamConnectWithApiKey()      — authenticate with tenant API key
   • beamGetBaseUrl()             — read engine URL from input field
   • beamShowTenantWelcome()      — fetch usage + jobs + pricing, show dashboard
   • _beamInjectWelcomeScreen()   — render the tenant welcome overlay
   • beamLaunchStudio()           — open SQL Gateway session → launch app
   • beamLogout()                 — POST /api/v1/auth/logout + clear state
   • beamOpenUpgradeInfo()        — GET /api/v1/pricing/tiers

   OAUTH CALLBACK EXPLAINED (plain english)
   ────────────────────────────────────────
   The flow when a user clicks "Continue with Google":

   1. User clicks the button → beamInitiateSSO('google') runs
   2. Browser is sent to:  GET {beamUrl}/api/v1/auth/google
   3. Beam engine redirects browser to Google's consent screen
   4. User approves → Google sends browser back to Beam engine at:
        GET {beamUrl}/api/v1/auth/google/callback?code=...&state=...
   5. Beam engine exchanges the code for a user profile,
      creates a JWT, then redirects the browser to Studio:
        index.html#token=eyJhbGciOiJIUzI1NiJ9...
   6. Studio's page loads (or reloads). beamHandleOAuthCallback()
      runs on DOMContentLoaded and checks:
        "Is there a #token=... in window.location.hash?"
   7. If yes → extract JWT, call GET /api/v1/tenants/{key},
      fetch usage/jobs/pricing, show the welcome screen.
   8. If no  → do nothing, show normal connect screen as usual.

   This is standard OAuth2 implicit/fragment flow. The token is
   in the hash (#) not the query string (?) so it never hits
   your server logs. It is cleaned from the URL immediately after
   being read (history.replaceState).
   ============================================================ */

'use strict';

/* ── state.beam shape ────────────────────────────────────────────
   window.state.beam = {
     baseUrl:    string,   // e.g. 'http://localhost:8090'
     jwt:        string,   // Bearer token (API key or OAuth JWT)
     tenant:     object,   // TenantResponse from /api/v1/tenants/{key}
     authMethod: string,   // 'sso' | 'apikey'
   }
   ──────────────────────────────────────────────────────────────── */


/* ──────────────────────────────────────────────────────────────────
   UI HELPERS
   ────────────────────────────────────────────────────────────────── */

/**
 * Called by oninput on #inp-beam-url.
 * Updates the small endpoint preview line under the auth panel.
 */
function beamUpdateEndpointPreview() {
    const url = (document.getElementById('inp-beam-url')?.value || '').trim().replace(/\/$/, '');
    const el  = document.getElementById('beam-endpoint-preview');
    if (el) el.textContent = (url || 'http://localhost:8090') + '/api/v1/auth/…';
}

/**
 * Toggle the inline API key form open/closed.
 * Called by onclick on #beam-apikey-toggle button.
 */
function beamToggleApiKeyForm() {
    const form  = document.getElementById('beam-apikey-form');
    const arrow = document.getElementById('beam-apikey-arrow');
    if (!form) return;
    const isOpen = form.style.display === 'flex';
    form.style.display = isOpen ? 'none' : 'flex';
    if (arrow) arrow.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(180deg)';
}

/**
 * Read the engine URL from the input field.
 * Falls back to localhost:8090 if empty.
 */
function beamGetBaseUrl() {
    return (document.getElementById('inp-beam-url')?.value || '')
            .trim()
            .replace(/\/$/, '')
        || 'http://localhost:8090';
}


/* ──────────────────────────────────────────────────────────────────
   API: GET /api/v1/auth/providers
   Called when the user selects Str:::Beam mode.
   Fetches which SSO providers are enabled on this Beam engine
   (e.g. google, github). Currently used for logging only —
   you can extend this to dynamically show/hide the SSO buttons.
   ────────────────────────────────────────────────────────────────── */
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
    } catch (_) {
        // Providers check is best-effort — don't break the UI if it fails
    }
}


/* ──────────────────────────────────────────────────────────────────
   SSO LOGIN
   GET /api/v1/auth/google  or  GET /api/v1/auth/github

   This does NOT make a fetch() call. It redirects the entire
   browser tab to the Beam engine's SSO endpoint. The engine
   handles the OAuth2 dance and eventually redirects back to
   index.html with #token=JWT in the URL fragment.

   See beamHandleOAuthCallback() below for what happens next.
   ────────────────────────────────────────────────────────────────── */
function beamInitiateSSO(provider) {
    const url = beamGetBaseUrl();

    if (!url) {
        if (typeof toast === 'function') toast('Enter the Str:::Beam Engine URL first', 'err');
        return;
    }

    // Save the base URL so beamHandleOAuthCallback() can use it after the redirect
    try { sessionStorage.setItem('strbeam_baseurl', url); } catch (_) {}

    // Save return info in case we need it
    try {
        sessionStorage.setItem('strbeam_pending_auth', JSON.stringify({
            provider,
            baseUrl: url,
            returnTo: window.location.href
        }));
    } catch (_) {}

    // Supported endpoints from the Beam OpenAPI spec:
    //   GET /api/v1/auth/google  → Beam engine redirects to Google consent
    //   GET /api/v1/auth/github  → Beam engine redirects to GitHub consent
    const ssoEndpoints = {
        google: '/api/v1/auth/google',
        github: '/api/v1/auth/github',
    };

    const endpoint = ssoEndpoints[provider];
    if (!endpoint) {
        if (typeof toast === 'function') toast(`Unknown SSO provider: ${provider}`, 'err');
        return;
    }

    // Leave Studio — browser goes to Beam engine, then Google/GitHub, then back
    window.location.href = `${url}${endpoint}`;
}


/* ──────────────────────────────────────────────────────────────────
   OAUTH CALLBACK HANDLER
   Runs automatically on page load (DOMContentLoaded, see bottom).

   After the SSO flow completes, the Beam engine redirects the
   browser back to Studio with the JWT in the URL hash:
     index.html#token=eyJhbGciOiJIUzI1NiJ9...

   This function:
     1. Checks if #token=... is present in the URL
     2. If yes, extracts the JWT
     3. Cleans the token out of the URL (so it doesn't sit in history)
     4. Calls GET /api/v1/tenants/{key} to load the tenant
     5. Shows the welcome screen
     6. Returns true if it handled a callback, false otherwise
   ────────────────────────────────────────────────────────────────── */
async function beamHandleOAuthCallback() {
    const hash = window.location.hash;

    // Quick exit — no token in URL, nothing to handle
    if (!hash.includes('token=')) return false;

    const params = new URLSearchParams(hash.slice(1)); // slice(1) removes the leading '#'
    const jwt    = params.get('token');
    if (!jwt) return false;

    // Remove token from URL immediately — don't leave JWTs in browser history
    history.replaceState(null, '', window.location.pathname + window.location.search);

    // Retrieve the Beam base URL that was saved before the redirect
    const baseUrl = (() => {
        try { return sessionStorage.getItem('strbeam_baseurl') || 'http://localhost:8090'; }
        catch (_) { return 'http://localhost:8090'; }
    })();

    if (typeof setConnectStatus === 'function') {
        setConnectStatus('loading', 'Loading your Str:::Beam workspace…');
    }

    try {
        // ── Step 1: Decode the tenant key from the JWT payload ──────────
        // JWT format: header.payload.signature  (all base64url encoded)
        // The payload contains claims like tenantKey, sub, etc.
        let tenantKey = '';
        try {
            const payloadBase64 = jwt.split('.')[1];
            const payload       = JSON.parse(atob(payloadBase64));
            tenantKey = payload.tenantKey || payload.sub || payload.tenant || '';
        } catch (_) {
            // If JWT decoding fails, we fall back to listing tenants below
        }

        // ── Step 2: Load tenant info ─────────────────────────────────────
        // GET /api/v1/tenants/{tenantKey} (requires Bearer auth)
        let tenant = null;

        if (tenantKey) {
            const r = await fetch(`${baseUrl}/api/v1/tenants/${tenantKey}`, {
                headers: {
                    'Accept':        'application/json',
                    'Authorization': `Bearer ${jwt}`,
                }
            });
            if (r.ok) tenant = await r.json();
        }

        // ── Step 3: Fallback — list all tenants if direct lookup failed ───
        // GET /api/v1/tenants  (admin-only in the spec, but also used for
        // listing the calling user's own tenants depending on implementation)
        if (!tenant) {
            const r2 = await fetch(`${baseUrl}/api/v1/tenants`, {
                headers: {
                    'Accept':        'application/json',
                    'Authorization': `Bearer ${jwt}`,
                }
            });
            if (r2.ok) {
                const list = await r2.json();
                if (Array.isArray(list) && list.length > 0) tenant = list[0];
            }
        }

        if (!tenant) throw new Error('Could not load tenant information after login.');

        // ── Step 4: Store auth state and show welcome screen ─────────────
        if (!window.state) window.state = {};
        window.state.beam = { baseUrl, jwt, tenant, authMethod: 'sso' };

        await beamShowTenantWelcome(tenant, jwt, baseUrl);
        return true;

    } catch (e) {
        if (typeof setConnectStatus === 'function') {
            setConnectStatus('err', `Str:::Beam login failed: ${e.message}`);
        }
        if (typeof addLog === 'function') {
            addLog('ERR', `Beam OAuth callback error: ${e.message}`);
        }
        return false;
    }
}


/* ──────────────────────────────────────────────────────────────────
   API KEY LOGIN
   GET /api/v1/tenants/{tenantKey}  with  Authorization: Bearer {apiKey}

   Used when the user fills in the "Tenant API Key" inline form
   instead of going through SSO.
   ────────────────────────────────────────────────────────────────── */
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
                'Accept':        'application/json',
                'Authorization': `Bearer ${apiKey}`,
            }
        });

        if (r.status === 401 || r.status === 403) {
            throw new Error('Invalid API key or tenant key. Check your credentials and try again.');
        }
        if (r.status === 404) {
            throw new Error(`Tenant "${tenantKey}" not found. Check the tenant key.`);
        }
        if (!r.ok) {
            throw new Error(`Str:::Beam returned HTTP ${r.status} — ${r.statusText}`);
        }

        const tenant = await r.json();

        if (tenant.status === 'SUSPENDED') {
            throw new Error(`Tenant "${tenantKey}" is suspended. Contact your administrator to reactivate.`);
        }
        if (tenant.status === 'DELETED') {
            throw new Error(`Tenant "${tenantKey}" has been deleted.`);
        }

        // Store auth state
        if (!window.state) window.state = {};
        window.state.beam = { baseUrl: url, jwt: apiKey, tenant, authMethod: 'apikey' };

        await beamShowTenantWelcome(tenant, apiKey, url);

    } catch (e) {
        if (typeof setConnectStatus === 'function') {
            setConnectStatus('err', `Str:::Beam: ${e.message}`);
        }
        if (typeof toast === 'function')  toast(e.message, 'err');
        if (typeof addLog === 'function') addLog('ERR', `Beam auth failed: ${e.message}`);
    }
}


/* ──────────────────────────────────────────────────────────────────
   TENANT WELCOME SCREEN
   Fetches three things in parallel, then renders the overlay.

   APIs called:
     GET /api/v1/metrics/tenants/{key}/usage  → 1h / 24h / 7d slot usage
     GET /api/v1/jobs                         → tenant-filtered job list
     GET /api/v1/pricing/tiers               → tier feature matrix (public)
   ────────────────────────────────────────────────────────────────── */
async function beamShowTenantWelcome(tenant, jwt, baseUrl) {
    const headers = {
        'Accept':        'application/json',
        'Authorization': `Bearer ${jwt}`,
    };

    // Fetch in parallel — if any fail, we still show the screen with partial data
    const [usageRes, jobsRes, pricingRes] = await Promise.allSettled([
        fetch(`${baseUrl}/api/v1/metrics/tenants/${tenant.tenantKey}/usage`, { headers }),
        fetch(`${baseUrl}/api/v1/jobs`,          { headers }),
        fetch(`${baseUrl}/api/v1/pricing/tiers`), // public — no auth needed
    ]);

    const usage   = usageRes.status   === 'fulfilled' && usageRes.value.ok   ? await usageRes.value.json()   : null;
    const jobs    = jobsRes.status    === 'fulfilled' && jobsRes.value.ok    ? await jobsRes.value.json()    : null;
    const pricing = pricingRes.status === 'fulfilled' && pricingRes.value.ok ? await pricingRes.value.json() : null;

    _beamInjectWelcomeScreen(tenant, jwt, baseUrl, usage, jobs, pricing);
}


/* ──────────────────────────────────────────────────────────────────
   RENDER WELCOME OVERLAY
   Builds and injects the full-screen tenant dashboard overlay.
   ────────────────────────────────────────────────────────────────── */
function _beamInjectWelcomeScreen(tenant, jwt, baseUrl, usage, jobs, pricing) {
    // Remove any existing welcome overlay (handles re-login)
    const existing = document.getElementById('beam-welcome-overlay');
    if (existing) existing.remove();

    // ── Derived data ─────────────────────────────────────────────────
    const tierColors = { FREE: '#f5a623', PRO: '#4fa3e0', ENTERPRISE: '#00d4aa' };
    const tierColor  = tierColors[tenant.tier] || '#f5a623';

    const jobArr      = Array.isArray(jobs?.jobs) ? jobs.jobs : Array.isArray(jobs) ? jobs : [];
    const runningJobs = jobArr.filter(j => j.status === 'RUNNING').length;
    const totalJobs   = jobArr.length;

    // Extract usage values — Beam API may use different key names
    const getUsage = (window) => {
        if (!usage) return '—';
        const keys = {
            '1h':  ['1h', 'lastHour',  'oneHour',  'hour'],
            '24h': ['24h', 'lastDay',  'oneDay',   'day'],
            '7d':  ['7d',  'lastWeek', 'oneWeek',  'week'],
        };
        for (const k of (keys[window] || [])) {
            if (usage[k] != null) return typeof usage[k] === 'number'
                ? usage[k].toLocaleString() + ' slot-s'
                : usage[k];
        }
        return '—';
    };

    const statusColor = tenant.status === 'ACTIVE' ? '#63c996' : '#f5a623';

    const createdAt = tenant.createdAt
        ? new Date(tenant.createdAt).toLocaleDateString('en-GB', {
            day: '2-digit', month: 'short', year: 'numeric'
        })
        : '—';

    // Tier feature list — uses pricing API response if available, fallback to defaults
    const defaultFeatures = {
        FREE:       ['1 concurrent session', '5 SQL statements / min', 'Community support', 'No custom JARs'],
        PRO:        ['5 concurrent sessions', 'Unlimited statements', 'Up to 5 custom JARs', 'Email support'],
        ENTERPRISE: ['Unlimited sessions', 'Unlimited statements', 'Unlimited custom JARs', 'Priority support + SLA'],
    };

    let features = defaultFeatures[tenant.tier] || defaultFeatures.FREE;
    if (pricing) {
        const tier = pricing[tenant.tier] || pricing[tenant.tier?.toLowerCase()];
        if (tier?.features && Array.isArray(tier.features)) {
            features = tier.features.map(f => typeof f === 'string' ? f : f.name || String(f));
        }
    }

    // Safe HTML escape (uses Studio's escHtml if available)
    const esc = typeof escHtml === 'function'
        ? escHtml
        : s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

    // ── Build overlay ─────────────────────────────────────────────────
    const overlay = document.createElement('div');
    overlay.id = 'beam-welcome-overlay';

    overlay.innerHTML = `
<style>
/* Scoped styles — all under #beam-welcome-overlay */
#beam-welcome-overlay {
  position: fixed; inset: 0; z-index: 2000;
  background: var(--bg0, #080b0f);
  display: flex; align-items: center; justify-content: center;
  padding: 24px; overflow-y: auto;
  font-family: 'IBM Plex Mono', monospace;
}
/* Hide the stream canvas behind the overlay */
#beam-welcome-overlay ~ #stream-bg-canvas { display: none !important; }

#bw-card {
  width: 100%; max-width: 860px;
  background: var(--bg1, #0c1219);
  border: 1px solid rgba(232,51,74,0.18);
  border-radius: 8px; overflow: hidden;
  box-shadow: 0 32px 80px rgba(0,0,0,0.7);
}
#bw-card .bw-banner {
  background: rgba(232,51,74,0.06);
  border-bottom: 1px solid rgba(232,51,74,0.14);
  padding: 24px 32px; display: flex; align-items: center; gap: 20px;
}
#bw-card .bw-logo {
  width: 52px; height: 52px; flex-shrink: 0;
  background: rgba(232,51,74,0.1); border: 2px solid rgba(232,51,74,0.5);
  clip-path: polygon(0 0,80% 0,100% 20%,100% 100%,20% 100%,0 80%);
  display: flex; align-items: center; justify-content: center;
}
#bw-card .bw-banner-title { font-size: 22px; font-weight: 700; color: var(--text0, #e8f0f8); }
#bw-card .bw-banner-title span { color: #e8334a; }
#bw-card .bw-banner-sub { font-size: 12px; color: var(--text2, #5a7a8a); margin-top: 4px; }
#bw-card .bw-status-chip {
  display: inline-flex; align-items: center; gap: 5px;
  font-size: 10px; padding: 3px 10px; border-radius: 20px;
  border: 1px solid; font-weight: 700; flex-shrink: 0;
}
#bw-card .bw-body { padding: 28px 32px; display: flex; flex-direction: column; gap: 24px; }
#bw-card .bw-section-label {
  font-size: 9px; letter-spacing: 2px; text-transform: uppercase;
  color: var(--text3, #2a4a5a);
  border-bottom: 1px solid rgba(255,255,255,0.04);
  padding-bottom: 6px; margin-bottom: 12px;
}
#bw-card .bw-info-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px;
}
#bw-card .bw-info-card {
  background: var(--bg2, #0f1924);
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 4px; padding: 14px 16px;
}
#bw-card .bw-info-label { font-size: 9px; letter-spacing: 1.5px; text-transform: uppercase; color: var(--text3, #2a4a5a); margin-bottom: 6px; }
#bw-card .bw-info-val   { font-size: 15px; font-weight: 600; color: var(--text0, #e8f0f8); }
#bw-card .bw-info-sub   { font-size: 10px; color: var(--text2, #5a7a8a); margin-top: 3px; }
#bw-card .bw-two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media(max-width:600px){ #bw-card .bw-two-col { grid-template-columns: 1fr; } }
#bw-card .bw-panel {
  background: var(--bg2, #0f1924); border-radius: 4px; overflow: hidden;
}
#bw-card .bw-panel-head {
  padding: 12px 16px; border-bottom: 1px solid rgba(255,255,255,0.06);
  display: flex; align-items: center; gap: 8px;
}
#bw-card .bw-panel-body { padding: 14px 16px; }
#bw-card .bw-tier-badge { font-size: 11px; font-weight: 700; padding: 3px 12px; border-radius: 20px; letter-spacing: 1px; }
#bw-card .bw-feature-list { list-style: none; display: flex; flex-direction: column; gap: 7px; }
#bw-card .bw-feature-list li { font-size: 11px; color: var(--text1, #a8b8cc); display: flex; align-items: center; gap: 8px; }
#bw-card .bw-feature-list li::before { content: '✓'; color: var(--green, #63c996); font-size: 10px; flex-shrink: 0; }
#bw-card .bw-usage-row { display: grid; grid-template-columns: repeat(3,1fr); gap: 10px; }
#bw-card .bw-usage-cell { background: var(--bg2, #0f1924); border-radius: 4px; padding: 12px 14px; text-align: center; }
#bw-card .bw-usage-val  { font-size: 15px; font-weight: 600; color: var(--text0, #e8f0f8); }
#bw-card .bw-usage-lbl  { font-size: 9px; color: var(--text3, #2a4a5a); margin-top: 4px; letter-spacing: 1px; text-transform: uppercase; }
#bw-card .bw-job-chip { font-size: 11px; padding: 4px 12px; border-radius: 3px; background: var(--bg3, #1a2535); border: 1px solid rgba(255,255,255,0.07); color: var(--text1, #a8b8cc); }
#bw-card .bw-job-chip.running { background: rgba(99,201,150,0.08); border-color: rgba(99,201,150,0.25); color: var(--green, #63c996); }
#bw-card .bw-quota-section { background: rgba(0,212,170,0.03); border: 1px solid rgba(0,212,170,0.1); border-radius: 4px; padding: 12px 16px; }
#bw-card .bw-quota-title { font-size: 9px; letter-spacing: 1.5px; text-transform: uppercase; color: var(--accent, #00d4aa); margin-bottom: 8px; }
#bw-card .bw-quota-pill { font-size: 10px; padding: 2px 9px; border-radius: 20px; background: rgba(0,212,170,0.07); border: 1px solid rgba(0,212,170,0.18); color: var(--accent, #00d4aa); margin: 2px; display: inline-block; }
#bw-card .bw-suspended-bar { background: rgba(245,166,35,0.1); border: 1px solid rgba(245,166,35,0.3); border-radius: 3px; padding: 10px 14px; font-size: 11px; color: #f5a623; line-height: 1.7; }
#bw-card .bw-footer {
  border-top: 1px solid rgba(255,255,255,0.06);
  padding: 20px 32px; background: var(--bg2, #0f1924);
  display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap;
}
#bw-card .bw-connect-btn {
  display: flex; align-items: center; gap: 8px;
  padding: 11px 28px; cursor: pointer;
  background: rgba(232,51,74,0.12); border: 1.5px solid rgba(232,51,74,0.5); border-radius: 4px;
  color: var(--text0, #e8f0f8); font-family: 'IBM Plex Mono', monospace;
  font-size: 13px; font-weight: 700; letter-spacing: 0.5px; transition: all 0.15s;
}
#bw-card .bw-connect-btn:hover { background: rgba(232,51,74,0.22); border-color: rgba(232,51,74,0.8); }
#bw-card .bw-logout-btn {
  padding: 9px 16px; cursor: pointer;
  background: transparent; border: 1px solid rgba(255,255,255,0.1); border-radius: 4px;
  color: var(--text2, #5a7a8a); font-family: 'IBM Plex Mono', monospace; font-size: 11px; transition: all 0.12s;
}
#bw-card .bw-logout-btn:hover { border-color: rgba(255,255,255,0.25); color: var(--text0, #e8f0f8); }
#bw-card .bw-footer-hint { font-size: 10px; color: var(--text3, #2a4a5a); line-height: 1.6; }
</style>

<div id="bw-card">

  <!-- ── Banner ─────────────────────────────────────────────────── -->
  <div class="bw-banner">
    <div class="bw-logo">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none"
           stroke="#e8334a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
      </svg>
    </div>
    <div style="flex:1;min-width:0;">
      <div class="bw-banner-title">Welcome to Str<span>:::</span>Beam</div>
      <div class="bw-banner-sub">
        Your managed Apache Flink® cluster is ready ·
        Tenant <strong style="color:var(--text1,#a8b8cc);">${esc(tenant.tenantKey)}</strong>
      </div>
    </div>
    <div class="bw-status-chip"
         style="color:${statusColor};border-color:${statusColor}33;background:${statusColor}11;">
      <span style="width:6px;height:6px;border-radius:50%;background:${statusColor};display:inline-block;"></span>
      ${tenant.status}
    </div>
  </div>

  <!-- ── Body ───────────────────────────────────────────────────── -->
  <div class="bw-body">

    ${tenant.status === 'SUSPENDED' ? `
    <div class="bw-suspended-bar">
      ⚠ <strong>Tenant Suspended.</strong> You cannot open new sessions or submit SQL.
      Running jobs continue until active sessions expire.
      Contact your administrator to reactivate.
    </div>` : ''}

    <!-- Tenant information -->
    <div>
      <div class="bw-section-label">Tenant Information</div>
      <div class="bw-info-grid">
        <div class="bw-info-card">
          <div class="bw-info-label">Display Name</div>
          <div class="bw-info-val" style="font-size:14px;">${esc(tenant.displayName)}</div>
        </div>
        <div class="bw-info-card">
          <div class="bw-info-label">Tenant Key</div>
          <div class="bw-info-val" style="font-size:13px;color:var(--accent,#00d4aa);">${esc(tenant.tenantKey)}</div>
          <div class="bw-info-sub">Used in all API calls</div>
        </div>
        <div class="bw-info-card">
          <div class="bw-info-label">Catalog Name</div>
          <div class="bw-info-val" style="font-size:13px;">${esc(tenant.catalogName || tenant.tenantKey + '_catalog')}</div>
          <div class="bw-info-sub">Default Flink catalog</div>
        </div>
        <div class="bw-info-card">
          <div class="bw-info-label">Member Since</div>
          <div class="bw-info-val" style="font-size:13px;">${createdAt}</div>
        </div>
      </div>
    </div>

    <!-- Plan & Cluster -->
    <div>
      <div class="bw-section-label">Plan &amp; Cluster</div>
      <div class="bw-two-col">

        <!-- Tier card -->
        <div class="bw-panel" style="border:1px solid ${tierColor}22;">
          <div class="bw-panel-head">
            <div class="bw-tier-badge"
                 style="background:${tierColor}18;border:1px solid ${tierColor}44;color:${tierColor};">
              ${tenant.tier}
            </div>
            <span style="font-size:11px;color:var(--text2,#5a7a8a);">Current plan</span>
            ${tenant.tier !== 'ENTERPRISE' ? `
            <button onclick="beamOpenUpgradeInfo('${tenant.tier}')"
                    style="margin-left:auto;background:transparent;border:1px solid rgba(232,51,74,0.3);
                           border-radius:3px;color:#e8334a;font-family:'IBM Plex Mono',monospace;
                           font-size:9px;padding:3px 9px;cursor:pointer;">↑ Upgrade</button>` : ''}
          </div>
          <div class="bw-panel-body">
            <ul class="bw-feature-list">
              ${features.map(f => `<li>${esc(f)}</li>`).join('')}
            </ul>
          </div>
        </div>

        <!-- Cluster status card -->
        <div class="bw-panel" style="border:1px solid rgba(255,255,255,0.06);">
          <div class="bw-panel-head">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--accent,#00d4aa)"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="3"/>
              <path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/>
            </svg>
            <span style="font-size:11px;color:var(--text2,#5a7a8a);">Cluster status</span>
            <span style="margin-left:auto;font-size:9px;padding:2px 8px;border-radius:20px;
                         background:rgba(99,201,150,0.1);border:1px solid rgba(99,201,150,0.3);
                         color:var(--green,#63c996);">● ONLINE</span>
          </div>
          <div class="bw-panel-body">
            <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;">
              <span class="bw-job-chip ${runningJobs > 0 ? 'running' : ''}">
                ${runningJobs} running job${runningJobs !== 1 ? 's' : ''}
              </span>
              <span class="bw-job-chip">${totalJobs} total jobs</span>
            </div>
            <div style="font-size:10px;color:var(--text3,#2a4a5a);line-height:1.7;">
              Engine: <span style="color:var(--text2,#5a7a8a);">${esc(baseUrl)}</span><br>
              Auth: <span style="color:var(--text2,#5a7a8a);">Bearer token</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Usage metrics -->
    <div>
      <div class="bw-section-label">Slot-Second Usage</div>
      <div class="bw-usage-row">
        <div class="bw-usage-cell">
          <div class="bw-usage-val">${getUsage('1h')}</div>
          <div class="bw-usage-lbl">Last 1 hour</div>
        </div>
        <div class="bw-usage-cell">
          <div class="bw-usage-val">${getUsage('24h')}</div>
          <div class="bw-usage-lbl">Last 24 hours</div>
        </div>
        <div class="bw-usage-cell">
          <div class="bw-usage-val">${getUsage('7d')}</div>
          <div class="bw-usage-lbl">Last 7 days</div>
        </div>
      </div>
    </div>

    <!-- Quota overrides (only shown if ENTERPRISE has overrides) -->
    ${tenant.quotaOverrides && Object.keys(tenant.quotaOverrides).length > 0 ? `
    <div>
      <div class="bw-quota-section">
        <div class="bw-quota-title">Quota Overrides</div>
        <div>
          ${Object.entries(tenant.quotaOverrides).map(([k,v]) =>
        `<span class="bw-quota-pill">${esc(k)}: ${esc(v)}</span>`
    ).join('')}
        </div>
      </div>
    </div>` : ''}

  </div><!-- /bw-body -->

  <!-- ── Footer CTA ─────────────────────────────────────────────── -->
  <div class="bw-footer">
    <button class="bw-connect-btn"
            onclick="beamLaunchStudio()"
            ${tenant.status === 'SUSPENDED' ? 'disabled style="opacity:0.4;cursor:not-allowed;"' : ''}>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
      </svg>
      Connect to Studio →
    </button>
    <div class="bw-footer-hint">
      Opens a Flink SQL Gateway session scoped to<br>
      <strong style="color:var(--text2,#5a7a8a);">${esc(tenant.displayName)}</strong>
      · ${tenant.tier} tier · catalog: ${esc(tenant.catalogName || tenant.tenantKey + '_catalog')}
    </div>
    <button class="bw-logout-btn" onclick="beamLogout()">⏻ Sign out</button>
  </div>

</div><!-- /bw-card -->
`;

    document.body.appendChild(overlay);

    // Hide the connect screen while the welcome overlay is visible
    const cs = document.getElementById('connect-screen');
    if (cs) cs.style.display = 'none';
}


/* ──────────────────────────────────────────────────────────────────
   LAUNCH STUDIO
   Called when the user clicks "Connect to Studio →" on the welcome overlay.

   Flow:
     1. Show a loading spinner over the welcome screen
     2. Patch Studio's internal state with the Beam JWT + base URL
     3. Remove the welcome overlay
     4. Call doConnect() which opens a Flink SQL Gateway session
        using POST /api/v1/sessions (already in js/connection.js)
   ────────────────────────────────────────────────────────────────── */
async function beamLaunchStudio() {
    const beam = window.state?.beam;
    if (!beam) return;

    // Show loading spinner over the card
    const overlay = document.getElementById('beam-welcome-overlay');
    if (overlay) {
        const spinner = document.createElement('div');
        spinner.style.cssText = `
      position:fixed;inset:0;background:rgba(8,11,15,0.92);
      display:flex;flex-direction:column;align-items:center;justify-content:center;
      gap:16px;z-index:10;
    `;
        spinner.innerHTML = `
      <div style="width:36px;height:36px;border:3px solid rgba(232,51,74,0.15);
                  border-top-color:#e8334a;border-radius:50%;
                  animation:_spin 0.8s linear infinite;"></div>
      <div style="font-size:11px;color:#e8334a;letter-spacing:1px;">
        Opening SQL Gateway session…
      </div>`;
        overlay.appendChild(spinner);
    }

    const { baseUrl, jwt, tenant } = beam;

    // ── Patch Studio's hidden remote-mode fields ──────────────────────
    // connection.js uses these to build the API base URL and auth header
    const remoteInput = document.getElementById('inp-remote-url');
    if (remoteInput) remoteInput.value = baseUrl;

    const tokenInput = document.getElementById('inp-token');
    if (tokenInput) tokenInput.value = jwt;

    // Switch to 'remote' mode so getBaseUrl() returns the Beam engine URL
    // and getAuthHeaders() sends the Bearer JWT
    if (typeof setMode     === 'function') setMode('remote');
    if (typeof setAuthMode === 'function') setAuthMode('bearer');

    // Pre-fill session name with tenant key
    const sessNameInput = document.getElementById('inp-session-name');
    if (sessNameInput) sessNameInput.value = tenant.tenantKey;

    // Remove overlay and hide connect screen
    if (overlay) overlay.remove();
    const cs = document.getElementById('connect-screen');
    if (cs) cs.style.display = 'none';

    // Fire doConnect() — this opens POST /api/v1/sessions and launches the app
    if (typeof doConnect === 'function') {
        try {
            await doConnect();
        } catch (e) {
            if (typeof toast === 'function') toast('Could not open session: ' + e.message, 'err');
            if (cs) cs.style.display = 'flex'; // show connect screen again on failure
        }
    }
}


/* ──────────────────────────────────────────────────────────────────
   LOGOUT
   POST /api/v1/auth/logout

   Note: the Beam spec says "JWTs are stateless — logout is
   client-side (discard the token). This endpoint exists for
   completeness and audit logging." So we always clear local
   state regardless of whether the API call succeeds.
   ────────────────────────────────────────────────────────────────── */
async function beamLogout() {
    const beam = window.state?.beam;

    if (beam?.baseUrl && beam?.jwt) {
        try {
            await fetch(`${beam.baseUrl}/api/v1/auth/logout`, {
                method:  'POST',
                headers: { 'Authorization': `Bearer ${beam.jwt}` }
            });
        } catch (_) {
            // Ignore — JWT is stateless, we clear it client-side regardless
        }
    }

    // Clear all Beam state
    if (window.state) window.state.beam = null;
    try { sessionStorage.removeItem('strbeam_baseurl');      } catch (_) {}
    try { sessionStorage.removeItem('strbeam_pending_auth'); } catch (_) {}

    // Remove welcome overlay and show connect screen
    const overlay = document.getElementById('beam-welcome-overlay');
    if (overlay) overlay.remove();

    const cs = document.getElementById('connect-screen');
    if (cs) cs.style.display = 'flex';

    if (typeof toast === 'function') toast('Signed out of Str:::Beam', 'info');
}


/* ──────────────────────────────────────────────────────────────────
   UPGRADE INFO
   GET /api/v1/pricing/tiers  (public — no auth needed)
   ────────────────────────────────────────────────────────────────── */
async function beamOpenUpgradeInfo(currentTier) {
    const baseUrl = window.state?.beam?.baseUrl || beamGetBaseUrl();
    try {
        const r     = await fetch(`${baseUrl}/api/v1/pricing/tiers`);
        const tiers = r.ok ? await r.json() : null;
        const msg   = tiers
            ? `Upgrade options:\n${JSON.stringify(tiers, null, 2)}`
            : `Contact sales to upgrade from ${currentTier}.`;
        alert(msg);
    } catch (_) {
        alert(`Contact sales to upgrade from ${currentTier} to PRO or ENTERPRISE.`);
    }
}


/* ──────────────────────────────────────────────────────────────────
   PAGE LOAD INIT
   Runs on DOMContentLoaded:
     1. Check for OAuth callback (#token=…) — handles SSO return
     2. Wire up the Beam mode button to load providers on click
   ────────────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', function () {
    beamHandleOAuthCallback();
});