/**
 * stream-bg.js  —  Str:::lab Studio
 * ─────────────────────────────────────────────────────────────────
 * Flowing mesh-wave background for the connection screen.
 * Industry labels + icons travel slowly along the wave strands.
 * Each industry has its own colour, font style, and icon.
 * Theme-aware: wave line colours follow CSS theme vars.
 * ─────────────────────────────────────────────────────────────────
 */
(function () {
    'use strict';

    // ── Industry definitions ─────────────────────────────────────────
    // Each entry: name shown on canvas, icon beside it, unique hex
    // colour, font style (weight + family), and font size.
    const INDUSTRIES = [
        {
            name : 'Finance',
            icon : '💹',
            color: '#00d4aa',
            font : '700 14px "IBM Plex Mono", monospace',
            size : 14,
        },
        {
            name : 'Fintech',
            icon : '💳',
            color: '#4fa3e0',
            font : '300 13px Georgia, "Times New Roman", serif',
            size : 13,
        },
        {
            name : 'TRADING',
            icon : '📈',
            color: '#f5c518',
            font : '800 15px "Arial Black", Arial, sans-serif',
            size : 15,
        },
        {
            name : 'Telecoms',
            icon : '📡',
            color: '#a78bfa',
            font : 'italic 500 12px "IBM Plex Sans", sans-serif',
            size : 12,
        },
        {
            name : 'IoT',
            icon : '⚙️',
            color: '#fb923c',
            font : '700 16px "Courier New", Courier, monospace',
            size : 16,
        },
        {
            name : 'Healthcare',
            icon : '🏥',
            color: '#34d399',
            font : '400 13px Verdana, Geneva, sans-serif',
            size : 13,
        },
        {
            name : 'AI / ML',
            icon : '🤖',
            color: '#818cf8',
            font : '600 14px "IBM Plex Mono", monospace',
            size : 14,
        },
        {
            name : 'eCommerce',
            icon : '🛒',
            color: '#f472b6',
            font : 'italic 600 13px Georgia, serif',
            size : 13,
        },
        {
            name : 'GovTech',
            icon : '🏛️',
            color: '#60a5fa',
            font : '500 11px Tahoma, Verdana, sans-serif',
            size : 11,
        },
        {
            name : 'Logistics',
            icon : '🏭',
            color: '#fbbf24',
            font : '900 13px Impact, "Arial Narrow", sans-serif',
            size : 13,
        },
        {
            name : 'Insurance',
            icon : '🛡️',
            color: '#2dd4bf',
            font : '300 12px "IBM Plex Sans", sans-serif',
            size : 12,
        },
        {
            name : 'Electronics',
            icon : '⚡',
            color: '#e879f9',
            font : 'italic 700 13px "Trebuchet MS", sans-serif',
            size : 13,
        },
    ];

    // ── Wave band definitions ────────────────────────────────────────
    const WAVE_DEFS = [
        { yFrac:0.25, amp:0.12, freq:1.2,  speed:0.000005, colorIdx:0, alpha:0.55, strands:9  },
        { yFrac:0.44, amp:0.16, freq:1.0,  speed:0.000004, colorIdx:1, alpha:0.65, strands:11 },
        { yFrac:0.60, amp:0.11, freq:1.5,  speed:0.000006, colorIdx:2, alpha:0.48, strands:8  },
        { yFrac:0.74, amp:0.09, freq:1.2,  speed:0.000004, colorIdx:3, alpha:0.36, strands:7  },
    ];

    // How many industry tokens travel per wave band
    const TOKENS_PER_WAVE = 3;

    const MESH_COLS  = 60;
    const VERT_EVERY = 5;

    // ── Runtime state ────────────────────────────────────────────────
    let canvas, ctx, W = 0, H = 0;
    let raf = null, lastTs = null, T = 0;
    let wavePalette = [];   // wave line colours from CSS theme
    let tokens = [];        // travelling industry label+icon objects

    // ── CSS theme helpers ────────────────────────────────────────────
    function _getWavePalette() {
        const s = getComputedStyle(document.documentElement);
        const g = (v, f) => (s.getPropertyValue(v) || '').trim() || f;
        return [
            g('--accent',  '#00d4aa'),
            g('--blue',    '#4fa3e0'),
            g('--accent2', '#00ccaa'),
            g('--yellow',  '#f5c518'),
        ];
    }

    function _getBg() {
        const s = getComputedStyle(document.documentElement);
        return (s.getPropertyValue('--bg0') || '').trim() || '#080b0f';
    }

    // ── Colour utilities ─────────────────────────────────────────────
    function _hexToRgb(hex) {
        hex = (hex || '#000').replace(/^#/, '');
        if (hex.length === 3) hex = hex.split('').map(c => c + c).join('');
        const n = parseInt(hex, 16) || 0;
        return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
    }

    function _rgba(hex, a) {
        try {
            const [r, g, b] = _hexToRgb(hex);
            return `rgba(${r},${g},${b},${Math.min(1, Math.max(0, a)).toFixed(3)})`;
        } catch (e) { return `rgba(0,212,170,${a})`; }
    }

    // ── Wave Y calculation ───────────────────────────────────────────
    function _waveY(def, xFrac, time) {
        const base = def.yFrac * H;
        const t1   = xFrac * Math.PI * 2 * def.freq;
        const s1   = Math.sin(t1        + time * def.speed * 200) * def.amp * H;
        const s2   = Math.sin(t1 * 0.6  + time * def.speed * 140 + 1.2) * def.amp * H * 0.42;
        const s3   = Math.sin(t1 * 1.45 + time * def.speed * 270 + 2.6) * def.amp * H * 0.18;
        return base + s1 + s2 + s3;
    }

    // ── Draw one wave band (mesh grid) ──────────────────────────────
    function _drawWave(def, time) {
        const color = wavePalette[def.colorIdx % wavePalette.length] || '#00d4aa';

        // Precompute grid points: [strand][col]
        const pts = [];
        for (let si = 0; si < def.strands; si++) {
            const row = [];
            const sf  = si / Math.max(def.strands - 1, 1);
            const vOff = (sf - 0.5) * def.amp * H * 0.70;
            for (let xi = 0; xi <= MESH_COLS; xi++) {
                const xf = xi / MESH_COLS;
                row.push({ x: xf * W, y: _waveY(def, xf, time) + vOff });
            }
            pts.push(row);
        }

        ctx.save();

        // Horizontal strands
        for (let si = 0; si < def.strands; si++) {
            const sf          = si / Math.max(def.strands - 1, 1);
            const distCentre  = Math.abs(sf - 0.5) * 2;
            const strandAlpha = def.alpha * (1 - distCentre * 0.60);
            const isCentre    = si === Math.floor(def.strands / 2);
            const row         = pts[si];

            ctx.beginPath();
            ctx.moveTo(row[0].x, row[0].y);
            for (let xi = 1; xi <= MESH_COLS; xi++) {
                const prev = row[xi - 1], curr = row[xi];
                ctx.quadraticCurveTo(prev.x, prev.y,
                    (prev.x + curr.x) / 2, (prev.y + curr.y) / 2);
            }
            ctx.strokeStyle = _rgba(color, strandAlpha);
            ctx.lineWidth   = isCentre ? 1.4 : 0.65;
            ctx.shadowBlur  = isCentre ? 6  : 2;
            ctx.shadowColor = _rgba(color, strandAlpha * 0.5);
            ctx.stroke();
        }

        // Vertical cross-hatch
        for (let xi = 0; xi <= MESH_COLS; xi += VERT_EVERY) {
            ctx.beginPath();
            for (let si = 0; si < def.strands; si++) {
                const p = pts[si][xi];
                si === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y);
            }
            ctx.strokeStyle = _rgba(color, def.alpha * 0.25);
            ctx.lineWidth   = 0.45;
            ctx.shadowBlur  = 2;
            ctx.shadowColor = _rgba(color, 0.12);
            ctx.stroke();
        }

        ctx.restore();
    }

    // ── Token (industry label + icon) system ─────────────────────────
    // Pick the next industry in round-robin order per wave so every
    // industry appears and repeats evenly across all bands.
    let _industryCounter = 0;

    function _nextIndustry() {
        const ind = INDUSTRIES[_industryCounter % INDUSTRIES.length];
        _industryCounter++;
        return ind;
    }

    function _makeToken(def, waveIdx, phaseOffset) {
        const ind = _nextIndustry();
        // Prefer the centre strands so labels ride the brightest line
        const centreBias = Math.floor(def.strands * 0.35 + Math.random() * def.strands * 0.30);
        return {
            waveIdx,
            def,
            ind,
            strandIdx : Math.max(0, Math.min(def.strands - 1, centreBias)),
            phase     : phaseOffset,
            // Very slow — takes 60–100 s to cross the full screen width
            speed     : 0.000003 + Math.random() * 0.000003,
        };
    }

    function _initTokens() {
        tokens = [];
        _industryCounter = 0;
        WAVE_DEFS.forEach((def, wi) => {
            for (let i = 0; i < TOKENS_PER_WAVE; i++) {
                tokens.push(_makeToken(def, wi, i / TOKENS_PER_WAVE));
            }
        });
    }

    function _tickTokens(dt) {
        tokens.forEach(tk => {
            tk.phase += tk.speed * dt;
            if (tk.phase >= 1) {
                tk.phase    -= 1;
                tk.ind       = _nextIndustry();
                // Pick a fresh strand when wrapping
                const def    = tk.def;
                const centre = Math.floor(def.strands * 0.35 + Math.random() * def.strands * 0.30);
                tk.strandIdx = Math.max(0, Math.min(def.strands - 1, centre));
            }
        });
    }

    function _drawTokens(time) {
        tokens.forEach(tk => {
            const def    = tk.def;
            const ind    = tk.ind;
            const xFrac  = tk.phase;
            const x      = xFrac * W;
            const sf     = tk.strandIdx / Math.max(def.strands - 1, 1);
            const vOff   = (sf - 0.5) * def.amp * H * 0.70;
            const y      = _waveY(def, xFrac, time) + vOff;

            // Fade in/out at left and right edges (8% of width each side)
            const edgeFade = Math.min(xFrac * 12, 1) * Math.min((1 - xFrac) * 12, 1);
            if (edgeFade < 0.02) return;

            ctx.save();

            // ── Soft glow pill behind the label ────────────────────────
            ctx.font         = ind.font;
            const labelText  = ind.name;
            const iconText   = ind.icon;
            const labelW     = ctx.measureText(labelText).width;
            const iconW      = ind.size + 14;        // wider gap between icon and text
            const totalW     = iconW + labelW + 10;
            const pillH      = ind.size + 10;
            const pillX      = x - totalW / 2;
            const pillY      = y - pillH / 2;
            const pillR      = 5;

            // Pill background
            ctx.globalAlpha = edgeFade * 0.82;
            ctx.shadowBlur  = 0;
            ctx.fillStyle   = 'rgba(6,10,18,0.72)';
            _roundRect(ctx, pillX - 4, pillY, totalW + 8, pillH, pillR);
            ctx.fill();

            // Pill border — reduced shadow
            ctx.globalAlpha  = edgeFade * 0.65;
            ctx.strokeStyle  = _rgba(ind.color, 0.70);
            ctx.lineWidth    = 0.8;
            ctx.shadowBlur   = 3;                    // was 8
            ctx.shadowColor  = _rgba(ind.color, 0.30); // was 0.55
            _roundRect(ctx, pillX - 4, pillY, totalW + 8, pillH, pillR);
            ctx.stroke();

            // ── Icon — zero blur for sharpness, with breathing room ────
            ctx.shadowBlur   = 0;
            ctx.globalAlpha  = edgeFade * 0.95;
            ctx.font         = `${ind.size + 2}px serif`;
            ctx.textAlign    = 'left';
            ctx.textBaseline = 'middle';
            ctx.fillText(iconText, pillX + 2, y);

            // ── Industry name — minimal shadow ──────────────────────────
            ctx.font         = ind.font;
            ctx.fillStyle    = ind.color;
            ctx.globalAlpha  = edgeFade * 0.95;
            ctx.shadowBlur   = 3;                    // was 6
            ctx.shadowColor  = _rgba(ind.color, 0.45); // was 0.80
            ctx.textAlign    = 'left';
            ctx.textBaseline = 'middle';
            ctx.fillText(labelText, pillX + iconW + 2, y);

            ctx.restore();
        });
    }

    // ── Rounded rect helper (no Path2D for compat) ───────────────────
    function _roundRect(ctx, x, y, w, h, r) {
        ctx.beginPath();
        ctx.moveTo(x + r, y);
        ctx.lineTo(x + w - r, y);
        ctx.quadraticCurveTo(x + w, y, x + w, y + r);
        ctx.lineTo(x + w, y + h - r);
        ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
        ctx.lineTo(x + r, y + h);
        ctx.quadraticCurveTo(x, y + h, x, y + h - r);
        ctx.lineTo(x, y + r);
        ctx.quadraticCurveTo(x, y, x + r, y);
        ctx.closePath();
    }

    // ── Post-processing overlays ─────────────────────────────────────
    function _drawVignette() {
        const grd = ctx.createRadialGradient(W * 0.5, H * 0.5, H * 0.10,
            W * 0.5, H * 0.5, H * 0.88);
        grd.addColorStop(0,    'rgba(0,0,0,0)');
        grd.addColorStop(0.50, 'rgba(0,0,0,0.10)');
        grd.addColorStop(1,    'rgba(0,0,0,0.68)');
        ctx.save();
        ctx.globalAlpha = 1;
        ctx.shadowBlur  = 0;
        ctx.fillStyle   = grd;
        ctx.fillRect(0, 0, W, H);
        ctx.restore();
    }

    function _drawFades(bg) {
        ctx.save();
        ctx.globalAlpha = 1;
        ctx.shadowBlur  = 0;

        // Bottom fade
        const bgrd = ctx.createLinearGradient(0, H * 0.60, 0, H);
        bgrd.addColorStop(0,    _rgba(bg, 0));
        bgrd.addColorStop(0.55, _rgba(bg, 0.70));
        bgrd.addColorStop(1,    _rgba(bg, 1));
        ctx.fillStyle = bgrd;
        ctx.fillRect(0, H * 0.60, W, H * 0.40);

        // Top fade
        const tgrd = ctx.createLinearGradient(0, 0, 0, H * 0.14);
        tgrd.addColorStop(0,   _rgba(bg, 0.55));
        tgrd.addColorStop(1,   _rgba(bg, 0));
        ctx.fillStyle = tgrd;
        ctx.fillRect(0, 0, W, H * 0.14);

        ctx.restore();
    }

    // ── Main animation loop ──────────────────────────────────────────
    function _frame(ts) {
        if (!lastTs) lastTs = ts;
        const dt = Math.min(ts - lastTs, 60);
        lastTs   = ts;
        T       += dt;

        // Pick up theme changes every ~5 s
        if (Math.floor(T / 5000) !== Math.floor((T - dt) / 5000)) {
            wavePalette = _getWavePalette();
        }

        const bg = _getBg();

        // Soft translucent wipe — low alpha preserves wave glow trails
        ctx.globalAlpha = 0.10;
        ctx.shadowBlur  = 0;
        ctx.fillStyle   = bg;
        ctx.fillRect(0, 0, W, H);
        ctx.globalAlpha = 1;

        // Draw mesh waves
        WAVE_DEFS.forEach(def => _drawWave(def, T));

        // Tick and draw industry tokens
        _tickTokens(dt);
        _drawTokens(T);

        // Post effects
        _drawVignette();
        _drawFades(bg);

        raf = requestAnimationFrame(_frame);
    }

    // ── Resize ───────────────────────────────────────────────────────
    function _resize() {
        const screen = document.getElementById('connect-screen');
        W = (screen ? screen.offsetWidth  : 0) || window.innerWidth;
        H = (screen ? screen.offsetHeight : 0) || window.innerHeight;
        canvas.width  = W;
        canvas.height = H;
        ctx.fillStyle = _getBg();
        ctx.fillRect(0, 0, W, H);
        wavePalette = _getWavePalette();
    }

    // ── Bootstrap ────────────────────────────────────────────────────
    function _init() {
        const screen = document.getElementById('connect-screen');
        if (!screen) return;

        canvas    = document.createElement('canvas');
        canvas.id = 'stream-bg-canvas';
        canvas.style.cssText =
            'position:absolute;inset:0;width:100%;height:100%;' +
            'z-index:0;pointer-events:none;display:block;';
        screen.insertBefore(canvas, screen.firstChild);

        ctx = canvas.getContext('2d');

        _resize();
        _initTokens();
        wavePalette = _getWavePalette();

        window.addEventListener('resize', () => { _resize(); _initTokens(); });

        // Pause when screen is hidden after login
        const obs = new MutationObserver(() => {
            const visible = screen.style.display !== 'none' && screen.isConnected;
            if (!visible && raf)       { cancelAnimationFrame(raf); raf = null; lastTs = null; }
            else if (visible && !raf)  { raf = requestAnimationFrame(_frame); }
        });
        obs.observe(screen, { attributes: true, attributeFilter: ['style'] });

        raf = requestAnimationFrame(_frame);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }

})();