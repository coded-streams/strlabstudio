/* Str:::lab Studio
 * ═══════════════════════════════════════════════════════════════════
 * Mirror-div syntax highlighter for the Flink SQL editor.
 *
 * Technique: a <div id="sql-highlight-mirror"> sits BEHIND the textarea
 * (which is made background:transparent / color:transparent / caret-color:normal).
 * The mirror is styled identically to the textarea (same font, padding, size)
 * so highlighted spans line up pixel-perfectly with what the user types.
 *
 * Token colours (designed for both dark + light themes via CSS vars):
 *   --sh-keyword   DDL/DML keywords        teal / accent
 *   --sh-clause    clause starters         blue
 *   --sh-type      data types              yellow-green
 *   --sh-func      built-in functions      purple/orchid
 *   --sh-string    string literals         orange
 *   --sh-number    numeric literals        cyan
 *   --sh-comment   -- line comments        muted green
 *   --sh-operator  operators + punctuation muted silver
 *   --sh-table     table aliases / refs    inherit (default text color)
 * ═══════════════════════════════════════════════════════════════════
 */
(function () {
    'use strict';

    /* ── Token definitions (order matters — earlier = higher priority) ── */
    const TOKENS = [
        // 1. Line comments  -- …
        { type: 'comment', rx: /--[^\n]*/g },

        // 2. Single-quoted strings
        { type: 'string',  rx: /'(?:[^'\\]|\\.)*'/g },

        // 3. Back-tick quoted identifiers  `catalog`.`table`
        { type: 'ident',   rx: /`[^`]*`/g },

        // 4. DDL / DML keywords (statement-level)
        {
            type: 'keyword',
            rx: /\b(CREATE|ALTER|DROP|INSERT|UPSERT|UPDATE|DELETE|TRUNCATE|REPLACE|MERGE|LOAD|UNLOAD|EXPLAIN|DESCRIBE|DESC|SHOW|USE|SET|RESET|BEGIN|COMMIT|ROLLBACK|GRANT|REVOKE|EXECUTE|CALL|TEMPORARY|TEMP|EXTERNAL|IF\s+NOT\s+EXISTS|IF\s+EXISTS|NOT\s+ENFORCED|NOT\s+NULL|PRIMARY\s+KEY|UNIQUE\s+KEY|WATERMARK\s+FOR|LIKE|EXCLUDING\s+ALL|OVERWRITE|ANALYZE|REFRESH|COMPILE|OPTIMIZE)\b/gi,
        },

        // 5. Clause / structural keywords
        {
            type: 'clause',
            rx: /\b(SELECT|DISTINCT|ALL|FROM|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|FETCH\s+FIRST|ROWS?\s+ONLY|UNION\s+ALL|UNION|INTERSECT|EXCEPT|INTO|VALUES|RETURNING|WITH|AS|ON|USING|JOIN|INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|NATURAL\s+JOIN|LATERAL\s+TABLE|FOR\s+SYSTEM_TIME\s+AS\s+OF|PARTITION\s+BY|OVER|WINDOW|WHEN|THEN|ELSE|END|CASE|AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|ILIKE|IS\s+NULL|IS\s+NOT\s+NULL|NULLIF|COALESCE|CAST|TRY_CAST|TABLE|VIEW|DATABASE|CATALOG|FUNCTION|FUNCTIONS|JOBS|JOB|TABLES|COLUMNS|COLUMN|SCHEMA|SCHEMAS|NAMESPACE|NAMESPACES|SAVEPOINTS|MATERIALIZED)\b/gi,
        },

        // 6. Flink streaming / window / CDC keywords
        {
            type: 'stream',
            rx: /\b(TUMBLE|HOP|SESSION|CUMULATE|DESCRIPTOR|MATCH_RECOGNIZE|PATTERN|DEFINE|MEASURES|ONE\s+ROW\s+PER\s+MATCH|ALL\s+ROWS\s+PER\s+MATCH|AFTER\s+MATCH\s+SKIP|WITHIN|PERMUTE|CLASSIFIER|RUNNING|FINAL|PREV|NEXT|FIRST|LAST|CHANGE_LOG|CHANGELOG|AUDIT_LOG|RETRACT|UPSERT_KAFKA|PROCTIME|ROWTIME|SYSTEM_TIME|WATERMARK|BOUNDED|UNBOUNDED\s+PRECEDING|UNBOUNDED\s+FOLLOWING|CURRENT\s+ROW|ROWS\s+BETWEEN|RANGE\s+BETWEEN|PRECEDING|FOLLOWING|START\s+WITH|CONNECT\s+BY|SOURCE|SINK)\b/gi,
        },

        // 7. Data types
        {
            type: 'type',
            rx: /\b(BIGINT|INT|INTEGER|SMALLINT|TINYINT|FLOAT|DOUBLE|REAL|DECIMAL|NUMERIC|VARCHAR|CHAR|CHARACTER|STRING|BOOLEAN|BYTES|BINARY|VARBINARY|DATE|TIME|TIMESTAMP(?:\s+WITH\s+LOCAL\s+TIME\s+ZONE)?|TIMESTAMP_LTZ|INTERVAL|ARRAY|MAP|MULTISET|ROW|RAW|NULL)\b/gi,
        },

        // 8. Built-in functions
        {
            type: 'func',
            rx: /\b(COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|COLLECT|LISTAGG|ARRAY_AGG|FIRST_VALUE|LAST_VALUE|ROW_NUMBER|RANK|DENSE_RANK|NTILE|PERCENT_RANK|CUME_DIST|LAG|LEAD|ABS|CEIL|CEILING|FLOOR|ROUND|TRUNCATE|SIGN|POWER|SQRT|LN|LOG|LOG2|LOG10|EXP|MOD|RAND|RANDOM|PI|LOWER|UPPER|LENGTH|CHAR_LENGTH|OCTET_LENGTH|TRIM|LTRIM|RTRIM|LPAD|RPAD|SUBSTRING|SUBSTR|OVERLAY|POSITION|REPLACE|INITCAP|CONCAT|CONCAT_WS|SPLIT_INDEX|REGEXP_EXTRACT|REGEXP_REPLACE|REGEXP|TO_DATE|TO_TIMESTAMP|TO_TIMESTAMP_LTZ|DATE_FORMAT|DATE_ADD|DATE_SUB|DATEDIFF|TIMESTAMPADD|TIMESTAMPDIFF|EXTRACT|CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|NOW|LOCALTIME|LOCALTIMESTAMP|UNIX_TIMESTAMP|FROM_UNIXTIME|CAST|TRY_CAST|COALESCE|NULLIF|IF|IFF|IFNULL|NVL|NVL2|ISNULL|ISNOTNULL|DECODE|TYPEOF|PROCTIME|GREATEST|LEAST|TO_BASE64|FROM_BASE64|MD5|SHA1|SHA256|UUID|JSON_VALUE|JSON_QUERY|JSON_EXISTS|JSON_OBJECT|JSON_ARRAY|IS_JSON|CARDINALITY|ELEMENT|ARRAY_CONTAINS|ARRAY_REMOVE|ARRAY_REVERSE|ARRAY_DISTINCT|ARRAY_UNION|MAP_KEYS|MAP_VALUES|MAP_ENTRIES|MAP_FROM_ARRAYS|MAP_UNION|FLATTEN|ZIP_WITH|SPLIT|STRING_SPLIT|REGEXP_SPLIT|FORMAT|PRINTF|WINDOW_START|WINDOW_END|WINDOW_TIME|HOP_START|HOP_END|HOP_PROCTIME|HOP_ROWTIME|SESSION_START|SESSION_END|SESSION_ROWTIME|SESSION_PROCTIME|TUMBLE_START|TUMBLE_END|TUMBLE_ROWTIME|TUMBLE_PROCTIME|MATCH_ROWTIME|MATCH_PROCTIME|SOURCE_WATERMARK|ROW_NUMBER)\b(?=\s*\()/gi,
        },

        // 9. Numeric literals  (integer, float, hex)
        { type: 'number', rx: /\b(0x[\da-fA-F]+|\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b/g },

        // 10. Operators   = <> != <= >= < > + - * / || :: ;
        { type: 'operator', rx: /(<>|!=|<=|>=|=>|->|\|\||::|\*|\/|\+|-|=|<|>|;)/g },
    ];

    /* Map token type → CSS variable name (defined below) */
    const TYPE_CLASS = {
        keyword:  'sh-keyword',
        clause:   'sh-clause',
        stream:   'sh-stream',
        type:     'sh-type',
        func:     'sh-func',
        string:   'sh-string',
        ident:    'sh-ident',
        number:   'sh-number',
        comment:  'sh-comment',
        operator: 'sh-operator',
    };

    /* ── Inject CSS variables + mirror styles ─────────────────────── */
    function injectStyles() {
        if (document.getElementById('sql-hl-css')) return;
        const s = document.createElement('style');
        s.id = 'sql-hl-css';
        s.textContent = `
      /* ── Token colour palette (dark theme defaults) ─────────────── */
      :root {
        --sh-keyword:  #00d4aa;   /* teal — accent                     */
        --sh-clause:   #4fa3e0;   /* blue                               */
        --sh-stream:   #7ec8e3;   /* light cyan — Flink-specific        */
        --sh-type:     #c8d96a;   /* yellow-green                       */
        --sh-func:     #c792ea;   /* orchid / purple                    */
        --sh-string:   #f5a623;   /* amber / orange                     */
        --sh-ident:    #7ab8d8;   /* back-tick identifiers — steel blue */
        --sh-number:   #56d3d8;   /* cyan                               */
        --sh-comment:  #4d7a55;   /* muted green                        */
        --sh-operator: #7a8a9a;   /* muted silver                       */
        --sh-plain:    #c8d8e8;   /* plain unmatched text               */
      }

      /* ── Light theme ────────────────────────────────────────────── */
      body.theme-light {
        --sh-keyword:  #0a8a6a;
        --sh-clause:   #1560a0;
        --sh-stream:   #1578a8;
        --sh-type:     #6a7a00;
        --sh-func:     #7a00c0;
        --sh-string:   #c06000;
        --sh-ident:    #2060a0;
        --sh-number:   #007878;
        --sh-comment:  #3a7a3a;
        --sh-operator: #556677;
        --sh-plain:    #2a3a4a;
      }

      /* ── Monokai — authentic Sublime Text Monokai palette ───────── */
      body.theme-monokai {
        --sh-keyword:  #f92672;   /* pink-red  — keywords              */
        --sh-clause:   #66d9e8;   /* cyan      — clause starters       */
        --sh-stream:   #a6e22e;   /* lime      — Flink streaming        */
        --sh-type:     #66d9e8;   /* cyan      — types                  */
        --sh-func:     #a6e22e;   /* lime      — functions              */
        --sh-string:   #e6db74;   /* yellow    — strings               */
        --sh-ident:    #fd971f;   /* orange    — identifiers           */
        --sh-number:   #ae81ff;   /* purple    — numbers               */
        --sh-comment:  #75715e;   /* warm grey — comments              */
        --sh-operator: #f8f8f2;   /* near-white — operators            */
        --sh-plain:    #f8f8f2;   /* near-white — plain text           */
      }

      /* ── Contrast — high-contrast, pure black bg ────────────────── */
      body.theme-contrast {
        --sh-keyword:  #00ffcc;   /* bright teal                        */
        --sh-clause:   #66b3ff;   /* bright blue                        */
        --sh-stream:   #00e5ff;   /* electric cyan                      */
        --sh-type:     #ccff00;   /* electric yellow-green              */
        --sh-func:     #ee82ee;   /* violet                             */
        --sh-string:   #ffaa00;   /* bright amber                       */
        --sh-ident:    #80ccff;   /* light blue                         */
        --sh-number:   #00ffff;   /* pure cyan                          */
        --sh-comment:  #888888;   /* mid-grey                           */
        --sh-operator: #dddddd;   /* light grey                         */
        --sh-plain:    #ffffff;   /* pure white                         */
      }

      /* ── Dracula — purple-toned dark theme ──────────────────────── */
      body.theme-dracula {
        --sh-keyword:  #ff79c6;   /* pink                               */
        --sh-clause:   #8be9fd;   /* cyan                               */
        --sh-stream:   #50fa7b;   /* green                              */
        --sh-type:     #8be9fd;   /* cyan                               */
        --sh-func:     #50fa7b;   /* green                              */
        --sh-string:   #f1fa8c;   /* yellow                             */
        --sh-ident:    #ffb86c;   /* orange                             */
        --sh-number:   #bd93f9;   /* purple                             */
        --sh-comment:  #6272a4;   /* muted blue-grey                    */
        --sh-operator: #f8f8f2;   /* near-white                         */
        --sh-plain:    #f8f8f2;   /* near-white                         */
      }

      /* ── Nord — cool arctic blue-grey palette ───────────────────── */
      body.theme-nord {
        --sh-keyword:  #81a1c1;   /* steel blue                         */
        --sh-clause:   #88c0d0;   /* ice blue                           */
        --sh-stream:   #8fbcbb;   /* frost                              */
        --sh-type:     #b48ead;   /* purple-grey                        */
        --sh-func:     #88c0d0;   /* ice blue                           */
        --sh-string:   #a3be8c;   /* sage green                         */
        --sh-ident:    #d08770;   /* aurora orange                      */
        --sh-number:   #b48ead;   /* purple                             */
        --sh-comment:  #616e88;   /* muted blue                         */
        --sh-operator: #eceff4;   /* snow white                         */
        --sh-plain:    #d8dee9;   /* pale arctic                        */
      }

      /* ── Onyx — deep charcoal, professional gold/amber accents ─────── */
      body.theme-onyx {
        --sh-keyword:  #c8a84b;   /* rich gold                          */
        --sh-clause:   #7cbde8;   /* cornflower blue                    */
        --sh-stream:   #63c996;   /* seafoam green                      */
        --sh-type:     #e8b86d;   /* warm amber                         */
        --sh-func:     #d09de8;   /* soft violet                        */
        --sh-string:   #78c078;   /* medium green                       */
        --sh-ident:    #a8c8e8;   /* light steel blue                   */
        --sh-number:   #85c8e0;   /* cerulean                           */
        --sh-comment:  #5a6a5a;   /* dark sage                          */
        --sh-operator: #9090a0;   /* slate                              */
        --sh-plain:    #d0ccc0;   /* warm off-white                     */
      }

      /* ── ST Dark (Sublime Text inspired) ────────────────────────── */
      body.theme-stdark {
        --sh-keyword:  #4e9de8;   /* bright blue                        */
        --sh-clause:   #4e9de8;   /* bright blue                        */
        --sh-stream:   #57c764;   /* bright green                       */
        --sh-type:     #e2c26e;   /* gold                               */
        --sh-func:     #d4a2f7;   /* lavender                           */
        --sh-string:   #e07734;   /* rust orange                        */
        --sh-ident:    #9cdcfe;   /* light blue                         */
        --sh-number:   #b5cea8;   /* pale green                         */
        --sh-comment:  #5c7a5c;   /* muted green                        */
        --sh-operator: #888a9a;   /* muted                              */
        --sh-plain:    #cdd6e0;   /* light grey-blue                    */
      }

      /* ── Mirror div ─────────────────────────────────────────────── */
      #sql-highlight-mirror {
        position: absolute;
        top: 0; left: 0; right: 0; bottom: 0;
        pointer-events: none;
        overflow: hidden;
        white-space: pre-wrap;
        word-wrap: break-word;
        font-family: var(--mono, 'IBM Plex Mono', 'Fira Code', monospace);
        font-size: 13px;
        line-height: 1.6;
        padding: 10px 14px 10px 14px;
        box-sizing: border-box;
        color: transparent;
        background: transparent;
        z-index: 0;
        tab-size: 4;
        -moz-tab-size: 4;
      }

      /* ── Textarea sits above mirror, text must be transparent ────── */
      #sql-editor {
        position: relative !important;
        z-index: 1 !important;
        background: transparent !important;
        color: transparent !important;
        caret-color: var(--text0, #e8f0f8) !important;
        -webkit-text-fill-color: transparent !important;
      }

      /* Make wrapper position:relative so mirror can be absolute */
      #editor-wrapper { position: relative !important; }

      /* ── Per-theme: force correct editor background + caret ─────── */
      /* Monokai: #272822 bg, warm text */
      body.theme-monokai #editor-wrapper,
      body.theme-monokai .sql-editor { background: #272822 !important; }
      body.theme-monokai #sql-editor { caret-color: #f8f8f2 !important; }
      body.theme-monokai #sql-highlight-mirror { background: #272822 !important; }

      /* Onyx: deep charcoal bg */
      body.theme-onyx #editor-wrapper,
      body.theme-onyx .sql-editor { background: #1a1814 !important; }
      body.theme-onyx #sql-editor { caret-color: #e8e0d0 !important; }
      body.theme-onyx #sql-highlight-mirror { background: #1a1814 !important; }

      /* Contrast: pure black bg, white text */
      body.theme-contrast #editor-wrapper,
      body.theme-contrast .sql-editor { background: #000000 !important; }
      body.theme-contrast #sql-editor { caret-color: #ffffff !important; }
      body.theme-contrast #sql-highlight-mirror { background: #000000 !important; }

      /* ── Highlighted token spans ─────────────────────────────────── */
      #sql-highlight-mirror .sh-keyword  { color: var(--sh-keyword);  font-weight: 600; }
      #sql-highlight-mirror .sh-clause   { color: var(--sh-clause);   font-weight: 600; }
      #sql-highlight-mirror .sh-stream   { color: var(--sh-stream);   font-weight: 600; }
      #sql-highlight-mirror .sh-type     { color: var(--sh-type); }
      #sql-highlight-mirror .sh-func     { color: var(--sh-func); }
      #sql-highlight-mirror .sh-string   { color: var(--sh-string); }
      #sql-highlight-mirror .sh-ident    { color: var(--sh-ident); }
      #sql-highlight-mirror .sh-number   { color: var(--sh-number); }
      #sql-highlight-mirror .sh-comment  { color: var(--sh-comment); font-style: italic; }
      #sql-highlight-mirror .sh-operator { color: var(--sh-operator); }
      /* Plain text (not matched) — uses per-theme CSS var */
      #sql-highlight-mirror .sh-plain    { color: var(--sh-plain, #c8d8e8); }
    `;
        document.head.appendChild(s);
    }

    /* ── Core highlighter: text → HTML with span tokens ──────────── */
    function highlight(text) {
        if (!text) return '';

        // Build a flat array of {start, end, type} spans from all token regexes
        const spans = [];

        TOKENS.forEach(({ type, rx }) => {
            rx.lastIndex = 0;
            let m;
            while ((m = rx.exec(text)) !== null) {
                spans.push({ start: m.index, end: m.index + m[0].length, type });
                if (!rx.global) break;
            }
        });

        // Sort by start position; earlier wins (first-match-wins collision resolution)
        spans.sort((a, b) => a.start - b.start || b.end - a.end);

        // Walk through text, skipping overlapping spans
        const html = [];
        let pos = 0;

        for (const span of spans) {
            if (span.start < pos) continue;   // overlapped — skip

            // Plain text before this span
            if (span.start > pos) {
                html.push('<span class="sh-plain">' + esc(text.slice(pos, span.start)) + '</span>');
            }

            // Token span
            const cls = TYPE_CLASS[span.type] || 'sh-plain';
            html.push('<span class="' + cls + '">' + esc(text.slice(span.start, span.end)) + '</span>');
            pos = span.end;
        }

        // Trailing plain text
        if (pos < text.length) {
            html.push('<span class="sh-plain">' + esc(text.slice(pos)) + '</span>');
        }

        return html.join('');
    }

    /* Escape HTML special chars (must not escape \n so pre-wrap works) */
    function esc(s) {
        return s
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    /* ── Create the mirror div ─────────────────────────────────────── */
    function createMirror(wrapper, textarea) {
        const mirror = document.createElement('div');
        mirror.id = 'sql-highlight-mirror';

        // Copy computed styles that affect layout from textarea
        // (padding, font-size, line-height are set via CSS above — just ensure
        //  left offset matches the line-numbers panel width)
        const ln = document.getElementById('line-numbers');
        const lnW = ln ? ln.offsetWidth : 0;
        mirror.style.left = lnW + 'px';

        wrapper.insertBefore(mirror, textarea);
        return mirror;
    }

    /* ── Sync mirror scroll with textarea scroll ─────────────────── */
    function syncScroll(textarea, mirror) {
        mirror.scrollTop  = textarea.scrollTop;
        mirror.scrollLeft = textarea.scrollLeft;
    }

    /* ── Update mirror content ────────────────────────────────────── */
    function updateMirror(textarea, mirror) {
        // Add a trailing newline so last-line cursor has correct height
        mirror.innerHTML = highlight(textarea.value + '\n');
        syncScroll(textarea, mirror);
    }

    /* ── Adjust mirror left to match current line-numbers width ─────  */
    function adjustMirrorLeft(mirror) {
        const ln = document.getElementById('line-numbers');
        if (ln) mirror.style.left = ln.offsetWidth + 'px';
    }

    /* ── Bootstrap ─────────────────────────────────────────────────── */
    function init() {
        injectStyles();

        const textarea = document.getElementById('sql-editor');
        const wrapper  = document.getElementById('editor-wrapper');
        if (!textarea || !wrapper) return;

        // Mirror must exist before first paint
        const mirror = createMirror(wrapper, textarea);

        // Initial render
        adjustMirrorLeft(mirror);
        updateMirror(textarea, mirror);

        // Re-highlight on every input/change
        textarea.addEventListener('input',  () => updateMirror(textarea, mirror));
        textarea.addEventListener('change', () => updateMirror(textarea, mirror));
        textarea.addEventListener('scroll', () => syncScroll(textarea, mirror));

        // Keep left offset in sync if line-numbers width changes
        const resizeObs = new ResizeObserver(() => {
            adjustMirrorLeft(mirror);
            syncScroll(textarea, mirror);
        });
        resizeObs.observe(wrapper);

        // Re-render when external code sets textarea.value
        // (tabs switch, snippet insert, workspace load, etc.)
        const origDesc = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');
        if (origDesc && origDesc.set) {
            Object.defineProperty(textarea, 'value', {
                get() { return origDesc.get.call(this); },
                set(v) {
                    origDesc.set.call(this, v);
                    // Defer so line-number update runs first
                    requestAnimationFrame(() => updateMirror(textarea, mirror));
                },
                configurable: true,
            });
        }

        // Expose for manual trigger (e.g. after paste from other modules)
        window._sqlHighlightUpdate = () => updateMirror(textarea, mirror);

        // ── Hook into applyTheme so mirror re-renders on theme switch ──
        // applyTheme() is defined in theme.js and called by selectTheme().
        // We wrap it to re-render the mirror after the body class changes.
        const _origApplyTheme = window.applyTheme;
        if (typeof _origApplyTheme === 'function' && !_origApplyTheme._hlPatched) {
            window.applyTheme = function() {
                _origApplyTheme.apply(this, arguments);
                // Small defer so the new body class is applied before we re-render
                requestAnimationFrame(() => updateMirror(textarea, mirror));
            };
            window.applyTheme._hlPatched = true;
        } else {
            // applyTheme not yet defined — poll until it is then patch
            const _patchTimer = setInterval(() => {
                if (typeof window.applyTheme === 'function' && !window.applyTheme._hlPatched) {
                    const _orig = window.applyTheme;
                    window.applyTheme = function() {
                        _orig.apply(this, arguments);
                        requestAnimationFrame(() => updateMirror(textarea, mirror));
                    };
                    window.applyTheme._hlPatched = true;
                    clearInterval(_patchTimer);
                }
            }, 100);
        }
    }

    /* Run after DOM is ready */
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();