/**
 * stream-bg.js
 * Full-screen animated data-stream background for the connection screen.
 * Renders on a <canvas> injected behind #connect-screen content.
 *
 * Visual concept:
 *   • Dozens of vertical "data streams" falling downward, like a
 *     stylised Matrix / Kafka event log — but minimal and on-brand.
 *   • Each column emits glowing characters from a set of SQL/data glyphs.
 *   • Colour palette follows the active CSS theme vars (accent, blue, etc.)
 *     with a deep translucent fade at the bottom so the panel reads cleanly.
 *   • A soft radial vignette darkens the edges to focus the eye on the panel.
 */
(function () {
    'use strict';

    // ── Glyph set ────────────────────────────────────────────────────
    // SQL keywords, numbers, symbols — gives a "streaming data" feel
    const GLYPHS =
        '01SELECT FROM WHERE GROUPBY TUMBLE HOP JOIN INSERT INTO KAFKA ' +
        'WINDOW EMIT WATERMARK TIMESTAMP FLINK ∑ ∂ → ⟶ ⊕ ◈ ⬡ ✦ ▶ ■ ░ █ ' +
        '0123456789ABCDEF{}[]<>|/*-+=_#@!?;:.,';

    const GLYPH_ARR = GLYPHS.split('').filter(Boolean);

    // ── Config ───────────────────────────────────────────────────────
    const CFG = {
        colW:       22,       // px between columns
        fontSize:   13,       // px
        speed:      0.6,      // base drop speed (cells/frame at 60fps)
        speedVar:   0.55,     // speed variance per column
        trailLen:   22,       // how many cells glow behind the head
        spawnRate:  0.018,    // probability per frame a dormant column activates
        fadeAlpha:  0.055,    // per-frame canvas fade (controls trail length)
        numColors:  4,        // colour slots (see _getColors)
    };

    // ── State ────────────────────────────────────────────────────────
    let canvas, ctx, cols = [], raf = null, W = 0, H = 0;

    // ── Colour helper ────────────────────────────────────────────────
    // Reads CSS vars so the animation matches whichever theme is active.
    function _getColors() {
        const s = getComputedStyle(document.documentElement);
        const accent  = s.getPropertyValue('--accent').trim()  || '#00d4aa';
        const accent2 = s.getPropertyValue('--accent2').trim() || '#00ffcc';
        const blue    = s.getPropertyValue('--blue').trim()    || '#4fa3e0';
        const text3   = s.getPropertyValue('--text3').trim()   || '#2a4a5a';
        return [accent, blue, accent2, text3];
    }

    // ── Column class ─────────────────────────────────────────────────
    function Column(x, colors) {
        this.x       = x;
        this.colors  = colors;
        this.reset();
    }
    Column.prototype.reset = function () {
        this.y       = -(Math.random() * 30 + 5);  // start above viewport
        this.speed   = CFG.speed + Math.random() * CFG.speedVar;
        this.active  = false;
        this.color   = this.colors[Math.floor(Math.random() * this.colors.length)];
        this.glyphs  = [];  // per-cell cached characters (refreshed occasionally)
        this.glyphTick = 0;
        this.alpha   = 0.7 + Math.random() * 0.3;
    };
    Column.prototype.activate = function () {
        this.active = true;
        this.y = -2;
        this.color = this.colors[Math.floor(Math.random() * this.colors.length)];
        this.alpha = 0.6 + Math.random() * 0.4;
        this.speed = CFG.speed + Math.random() * CFG.speedVar;
    };
    Column.prototype.tick = function (cellH) {
        if (!this.active) {
            if (Math.random() < CFG.spawnRate) this.activate();
            return;
        }
        this.y += this.speed;
        // Occasionally swap a glyph in the trail (flickering effect)
        this.glyphTick++;
        if (this.glyphTick % 6 === 0) {
            const idx = Math.floor(Math.random() * CFG.trailLen);
            this.glyphs[idx] = GLYPH_ARR[Math.floor(Math.random() * GLYPH_ARR.length)];
        }
        if (this.y - CFG.trailLen > H / cellH + 4) {
            this.reset();
        }
    };
    Column.prototype.draw = function (ctx, cellH) {
        if (!this.active) return;
        const headY = Math.floor(this.y);

        for (let i = 0; i < CFG.trailLen; i++) {
            const cy = headY - i;
            if (cy < 0 || cy * cellH > H) continue;

            const glyph = this.glyphs[i] ||
                (this.glyphs[i] = GLYPH_ARR[Math.floor(Math.random() * GLYPH_ARR.length)]);

            // Head cell: bright white glow
            if (i === 0) {
                ctx.shadowBlur   = 14;
                ctx.shadowColor  = this.color;
                ctx.fillStyle    = '#ffffff';
                ctx.globalAlpha  = this.alpha;
            } else {
                // Trail fades with distance from head
                const fade = 1 - i / CFG.trailLen;
                ctx.shadowBlur   = 6 * fade;
                ctx.shadowColor  = this.color;
                ctx.fillStyle    = this.color;
                ctx.globalAlpha  = this.alpha * fade * 0.85;
            }

            ctx.fillText(glyph, this.x, cy * cellH + cellH);
        }
    };

    // ── Radial vignette overlay ───────────────────────────────────────
    function _drawVignette() {
        const grd = ctx.createRadialGradient(W / 2, H / 2, H * 0.18, W / 2, H / 2, H * 0.82);
        grd.addColorStop(0,   'rgba(0,0,0,0)');
        grd.addColorStop(0.6, 'rgba(0,0,0,0.18)');
        grd.addColorStop(1,   'rgba(0,0,0,0.72)');
        ctx.save();
        ctx.globalAlpha = 1;
        ctx.shadowBlur  = 0;
        ctx.fillStyle   = grd;
        ctx.fillRect(0, 0, W, H);
        ctx.restore();
    }

    // ── Bottom fade: hides streams behind the panel area ─────────────
    function _drawBottomFade() {
        const bg = getComputedStyle(document.documentElement)
            .getPropertyValue('--bg0').trim() || '#080b0f';
        const grd = ctx.createLinearGradient(0, H * 0.72, 0, H);
        grd.addColorStop(0,   bg + '00');
        grd.addColorStop(0.5, bg + 'aa');
        grd.addColorStop(1,   bg + 'ff');
        ctx.save();
        ctx.globalAlpha = 1;
        ctx.shadowBlur  = 0;
        ctx.fillStyle   = grd;
        ctx.fillRect(0, H * 0.72, W, H * 0.28);
        ctx.restore();
    }

    // ── Resize ────────────────────────────────────────────────────────
    function _resize() {
        W = canvas.offsetWidth  || window.innerWidth;
        H = canvas.offsetHeight || window.innerHeight;
        canvas.width  = W;
        canvas.height = H;
        ctx.font = `${CFG.fontSize}px "IBM Plex Mono", "Courier New", monospace`;
        const colors  = _getColors();
        const numCols = Math.ceil(W / CFG.colW);
        // Grow or shrink columns array without losing existing active streams
        while (cols.length < numCols) {
            const x = cols.length * CFG.colW + Math.floor(CFG.colW / 2);
            const c = new Column(x, colors);
            // Stagger initial activation so they don't all start at once
            if (Math.random() < 0.25) { c.activate(); c.y = Math.random() * 20; }
            cols.push(c);
        }
        cols.length = numCols;
        cols.forEach((c, i) => { c.x = i * CFG.colW + Math.floor(CFG.colW / 2); });
    }

    // ── Main loop ─────────────────────────────────────────────────────
    function _frame() {
        const cellH = CFG.fontSize + 3;

        // Soft fade — creates the glowing trail effect
        ctx.globalAlpha = CFG.fadeAlpha;
        ctx.shadowBlur  = 0;
        const bg = getComputedStyle(document.documentElement)
            .getPropertyValue('--bg0').trim() || '#080b0f';
        ctx.fillStyle   = bg;
        ctx.fillRect(0, 0, W, H);

        // Draw columns
        ctx.font = `${CFG.fontSize}px "IBM Plex Mono", "Courier New", monospace`;
        cols.forEach(c => {
            c.tick(cellH);
            c.draw(ctx, cellH);
        });

        ctx.globalAlpha = 1;
        ctx.shadowBlur  = 0;

        _drawVignette();
        _drawBottomFade();

        raf = requestAnimationFrame(_frame);
    }

    // ── Init ──────────────────────────────────────────────────────────
    function _init() {
        const screen = document.getElementById('connect-screen');
        if (!screen) return;

        // Inject canvas as first child of connect-screen
        canvas = document.createElement('canvas');
        canvas.id = 'stream-bg-canvas';
        canvas.style.cssText = [
            'position:absolute', 'inset:0', 'width:100%', 'height:100%',
            'z-index:0', 'pointer-events:none', 'display:block'
        ].join(';');
        screen.insertBefore(canvas, screen.firstChild);

        ctx = canvas.getContext('2d');
        _resize();

        window.addEventListener('resize', _resize);

        // Pause when connect screen is hidden, resume when shown
        // (avoids burning CPU after user connects)
        const obs = new MutationObserver(() => {
            const hidden = screen.style.display === 'none' || !screen.isConnected;
            if (hidden && raf) { cancelAnimationFrame(raf); raf = null; }
            else if (!hidden && !raf) { raf = requestAnimationFrame(_frame); }
        });
        obs.observe(screen, { attributes: true, attributeFilter: ['style'] });
        obs.observe(document.body, { childList: true });

        raf = requestAnimationFrame(_frame);
    }

    // Kick off after DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }
})();