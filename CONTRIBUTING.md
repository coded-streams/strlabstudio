# Contributing to Str:::Lab Studio

Thank you for taking the time to contribute. This document covers everything you need to know to get your change merged cleanly.

---

## Before You Start

- Search [existing issues](https://github.com/coded-streams/strlabstudio/issues) before opening a new one — your question or bug may already be tracked.
- For significant changes (new panels, new connection modes, redesigns), open an issue first to discuss the approach before writing code.
- All contributions must be compatible with the **Apache 2.0 license**.

---

## Development Setup

Str:::Lab Studio is intentionally a single-file application (`studio/index.html`). This keeps the deployment surface minimal and makes it easy to run without a build step.

**Prerequisites:** Docker Desktop, any modern browser.

```bash
# 1. Clone the repository
git clone https://github.com/coded-streams/strlabstudio.git
cd strlabstudio

# 2. Start a local Flink cluster
docker compose --profile local-flink up -d

# 3. Open the IDE directly from disk (no build needed)
open studio/index.html
# — or —
# Start the full container (rebuilds on each run)
docker compose up --build strlabstudio
```

Changes to `studio/index.html` are live immediately when opened from disk. You do not need to restart Docker to iterate on the UI.

---

## Code Style

The project is intentionally low-dependency. There is no TypeScript, no bundler, no framework.

- **JavaScript:** plain ES2020+ (`async/await`, optional chaining, template literals). No external JS libraries.
- **CSS:** custom properties (`--var`) for theming. No preprocessor.
- **HTML:** a single `<script>` block. All logic lives in named functions — no inline scripts beyond `onclick` attributes.
- **Naming:** `camelCase` for functions and variables. `kebab-case` for CSS classes and HTML ids.
- **Comments:** use `//` comment blocks at the top of each logical section. The `──` separator style is used throughout — keep it consistent.

---

## Submitting a Pull Request

1. Fork the repository and create a branch from `main`:
   ```
   git checkout -b feat/my-feature
   ```

2. Make your changes in `studio/index.html` (and any supporting files).

3. Run the syntax check before committing:
   ```bash
   python3 -c "
   import re, subprocess, tempfile, os
   html = open('studio/index.html').read()
   js = re.findall(r'<script>([\s\S]*?)</script>', html)[-1]
   f = tempfile.NamedTemporaryFile(suffix='.js', delete=False, mode='w')
   f.write('(function(){\n' + js + '\n});'); f.close()
   r = subprocess.run(['node', '--check', f.name], capture_output=True, text=True)
   os.unlink(f.name)
   print('CLEAN' if r.returncode == 0 else r.stderr)
   "
   ```

4. Commit with a clear message:
   ```
   feat: add query history search
   fix: correct splitSQL handling of backtick strings
   docs: update connection guide for Confluent Cloud
   ```

5. Push your branch and open a pull request against `main`. Fill out the PR template — describe *what* changed and *why*.

---

## Reporting Bugs

When reporting a bug, please include:

- Str:::Lab Studio version (top bar or Docker image tag)
- Apache Flink version
- Browser and OS
- What you expected vs. what happened
- Any error messages from the browser console (`F12 → Console`)
- Relevant SQL statements (please strip any sensitive data)

---

## Feature Requests

Open an issue with the label `enhancement` and describe:

- The problem you are trying to solve
- Your proposed solution
- Any alternatives you considered

---

## Code of Conduct

This project follows a straightforward standard: be respectful, be constructive, and assume good faith. Harassment or personal attacks will not be tolerated.

---

## Contact

Maintainer: **Nestor A. A** · [nestorabiawuh@gmail.com](mailto:nestorabiawuh@gmail.com)  
Organization: [codedstreams](https://github.com/coded-streams)
