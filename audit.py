#!/usr/bin/env python3
"""
audit.py — STOCKROOM repo-drift detector.

Run from the repo root:
    python3 audit.py

Returns exit code 0 on clean (no drift), 1 on any finding.

Categories audited:
  1. Caddyfile routes vs main.ts URL handlers
  2. Dockerfile COPY list vs files referenced (and existence)
  3. deploy.yml REQUIRED list vs Dockerfile COPY list
  4. package.json build script vs Dockerfile build
  5. <script src="X.js"> in index.html / landing.html vs files in build
  6. DB_STORES vs actual dbGet/dbPut calls in app.js
  7. Auth dual-path pattern in main.ts authenticated endpoints
  8. Stale write_dockerfile.py vs committed Dockerfile/Caddyfile/start.sh

This is a static analysis — no network calls, no DB queries. It just reads
files and prints what's out of sync.
"""

import os
import re
import sys
import json
import subprocess
import tempfile
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────
# Output helpers
# ─────────────────────────────────────────────────────────────────────────

RED    = '\033[91m'
YELLOW = '\033[93m'
GREEN  = '\033[92m'
BLUE   = '\033[94m'
DIM    = '\033[2m'
BOLD   = '\033[1m'
RESET  = '\033[0m'

# Disable colour if stdout isn't a tty (e.g. piping to file)
if not sys.stdout.isatty():
    RED = YELLOW = GREEN = BLUE = DIM = BOLD = RESET = ''

findings = []

def header(title):
    print(f"\n{BOLD}{BLUE}━━━ {title} ━━━{RESET}")

def ok(msg):
    print(f"  {GREEN}✓{RESET} {msg}")

def warn(msg, severity='warn'):
    findings.append((severity, msg))
    colour = RED if severity == 'error' else YELLOW
    icon = '✗' if severity == 'error' else '⚠'
    print(f"  {colour}{icon}{RESET} {msg}")

def info(msg):
    print(f"  {DIM}·{RESET} {msg}")

# ─────────────────────────────────────────────────────────────────────────
# File helpers
# ─────────────────────────────────────────────────────────────────────────

def read(path):
    """Read a file, return its content or None if absent."""
    p = Path(path)
    if not p.exists():
        return None
    return p.read_text(encoding='utf-8', errors='replace')


# ─────────────────────────────────────────────────────────────────────────
# Audit 1 — Caddyfile routes vs main.ts URL handlers
# ─────────────────────────────────────────────────────────────────────────

def audit_caddyfile_routes():
    header("1. Caddyfile reverse-proxy routes vs main.ts handlers")

    main_ts = read('main.ts')
    caddyfile = read('Caddyfile')

    if main_ts is None:
        warn("main.ts not found — skipping", 'error')
        return
    if caddyfile is None:
        warn("Caddyfile not found — skipping", 'error')
        return

    # Extract all url.pathname === '/foo' patterns from main.ts
    # and url.pathname.startsWith('/foo') patterns
    paths_used = set()
    for match in re.finditer(r"url\.pathname\s*===\s*['\"]([^'\"]+)['\"]", main_ts):
        paths_used.add(match.group(1))
    for match in re.finditer(r"url\.pathname\.startsWith\(['\"]([^'\"]+)['\"]\)", main_ts):
        paths_used.add(match.group(1))

    # Get top-level prefixes (everything after the first / up to the second /)
    top_level_used = set()
    for path in paths_used:
        parts = path.split('/')
        if len(parts) >= 2 and parts[1]:
            top_level_used.add('/' + parts[1])

    # Extract handle blocks from Caddyfile: 'handle /xxx {' or 'handle /xxx/* {'
    handle_blocks = set()
    for match in re.finditer(r'^\s*handle\s+(/\S+)\s*\{', caddyfile, re.MULTILINE):
        block = match.group(1)
        # Strip trailing /* for comparison
        prefix = block.rstrip('*').rstrip('/')
        if prefix:
            handle_blocks.add(prefix)

    info(f"main.ts uses {len(top_level_used)} top-level URL prefixes")
    info(f"Caddyfile has {len(handle_blocks)} matching handle blocks (excluding catch-all and host-matched)")

    # Find prefixes in main.ts but NOT in Caddyfile (these will 404)
    missing_from_caddy = top_level_used - handle_blocks
    # Exclude prefixes that are sub-paths of an existing Caddyfile handle
    # (e.g. /admin/r2/restore is covered by /admin/*)
    truly_missing = set()
    for p in missing_from_caddy:
        if not any(p.startswith(h + '/') or p == h for h in handle_blocks):
            truly_missing.add(p)

    if truly_missing:
        for p in sorted(truly_missing):
            # Show example endpoints for context
            examples = [path for path in paths_used if path.startswith(p)][:3]
            warn(f"main.ts handles '{p}' but Caddyfile has no matching reverse_proxy block. "
                 f"Affected endpoints: {', '.join(examples)}{'…' if len(examples) >= 3 else ''}",
                 'error')
    else:
        ok("All main.ts URL prefixes have matching Caddyfile reverse_proxy blocks")

    # Find Caddyfile handles that don't match anything in main.ts (dead routes)
    dead_in_caddy = handle_blocks - top_level_used
    # Exclude things like /ping, /robots.txt, host-handlers — filter to /xyz handles with no usage
    # /ping is special (it's in main.ts as a health check)
    # /robots.txt is special (served by Caddy directly)
    really_dead = []
    for p in dead_in_caddy:
        if p in ('/ping', '/robots.txt'):
            continue
        # If the Caddyfile has the handle but main.ts has zero usage, flag as dead
        if not any(path.startswith(p) for path in paths_used):
            really_dead.append(p)

    if really_dead:
        for p in sorted(really_dead):
            warn(f"Caddyfile reverse-proxies '{p}' but main.ts has no handler for it. "
                 "Either remove the Caddyfile handle or check whether main.ts is missing a route.",
                 'warn')


# ─────────────────────────────────────────────────────────────────────────
# Audit 2 — Dockerfile COPY list vs files referenced
# ─────────────────────────────────────────────────────────────────────────

def audit_dockerfile_copy():
    header("2. Dockerfile COPY list vs files actually present")

    dockerfile = read('Dockerfile')
    if dockerfile is None:
        warn("Dockerfile not found — skipping", 'error')
        return

    # Extract all files copied from build context (COPY foo bar baz ./)
    # Skip COPY --from=... lines (those copy from previous stages)
    copied_files = set()
    for line in dockerfile.split('\n'):
        line = line.strip()
        if not line.startswith('COPY ') or '--from=' in line:
            continue
        # Strip 'COPY ' prefix, drop the destination (last token)
        parts = line.split()
        if len(parts) < 3:
            continue
        for token in parts[1:-1]:  # everything except 'COPY' and destination
            # Skip flags
            if token.startswith('-'):
                continue
            # Skip paths into build context
            if token.endswith('/') or '*' in token:
                continue
            copied_files.add(token)

    info(f"Dockerfile COPYs {len(copied_files)} named files into the build context")

    # Check each copied file exists
    missing = []
    for f in sorted(copied_files):
        if not Path(f).exists():
            missing.append(f)

    if missing:
        for f in missing:
            warn(f"Dockerfile COPYs '{f}' but the file is not in the repo. "
                 "Build will fail on this COPY step.", 'error')
    else:
        ok(f"All {len(copied_files)} Dockerfile-referenced files exist in the repo")

    # Also extract 'cp X public/X' patterns inside RUN blocks — these copy from
    # the build context into the public output directory. If a file is COPYed
    # but never cp'd into public/, it won't be served.
    cp_to_public = set()
    for match in re.finditer(r'cp\s+(\S+)\s+public/', dockerfile):
        cp_to_public.add(match.group(1))

    # Also pick up terser / cleancss / html-minifier outputs — those land in public/.
    # cleancss form: `cleancss -o public/styles.css styles.css` (input is LAST arg)
    # terser form: `terser app.js ... -o public/app.js` (input is FIRST positional)
    # html-min form: `html-minifier-terser index.html ... -o public/index.html`
    processed = set()
    for match in re.finditer(r'(?:terser|html-minifier-terser)\s+(\S+\.(?:js|html))', dockerfile):
        processed.add(match.group(1))
    # cleancss puts the input file at the end, after -o public/xxx
    for match in re.finditer(r'cleancss\s+-o\s+\S+\s+(\S+\.css)', dockerfile):
        processed.add(match.group(1))

    served_files = cp_to_public | processed

    # Anything COPYed into the build context but never written to public/
    # is a wasted COPY — unless it's a backend file (main.ts, deno.json) that
    # the second stage uses but never serves statically.
    backend_only = {'main.ts', 'deno.json', 'start.sh', 'Caddyfile', 'package.json'}
    untouched = copied_files - served_files - backend_only
    # Filter out non-content files
    content_extensions = {'.html', '.js', '.css', '.png', '.svg', '.json', '.ico', '.webp', '.woff', '.woff2'}
    untouched_content = {f for f in untouched if Path(f).suffix in content_extensions}

    if untouched_content:
        for f in sorted(untouched_content):
            warn(f"Dockerfile COPYs '{f}' into build context but never writes it to public/. "
                 "If this file is meant to be served, add a 'cp {f} public/{f}' step.".format(f=f),
                 'warn')


# ─────────────────────────────────────────────────────────────────────────
# Audit 3 — deploy.yml REQUIRED list vs Dockerfile COPY
# ─────────────────────────────────────────────────────────────────────────

def audit_deploy_yml():
    header("3. deploy.yml REQUIRED files vs Dockerfile COPY list")

    deploy = read('deploy.yml')
    dockerfile = read('Dockerfile')
    if deploy is None:
        warn("deploy.yml not found — skipping", 'warn')
        return
    if dockerfile is None:
        warn("Dockerfile not found — skipping", 'error')
        return

    # Extract REQUIRED=( ... ) block from deploy.yml
    m = re.search(r'REQUIRED=\(([^)]+)\)', deploy)
    if not m:
        warn("deploy.yml has no REQUIRED=( ... ) block — skipping", 'warn')
        return
    required = set(re.findall(r'[\w.-]+', m.group(1)))
    required.discard('REQUIRED')  # the variable name itself

    # Same extraction as audit 2
    copied_files = set()
    for line in dockerfile.split('\n'):
        line = line.strip()
        if not line.startswith('COPY ') or '--from=' in line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        for token in parts[1:-1]:
            if token.startswith('-') or token.endswith('/') or '*' in token:
                continue
            copied_files.add(token)

    info(f"deploy.yml REQUIRED list has {len(required)} files")
    info(f"Dockerfile COPYs {len(copied_files)} files from the build context")

    # main.ts, deno.json, start.sh, Caddyfile are required but not in COPY (they're handled separately by COPY main.ts ./main.ts etc.)
    # So check intersection more carefully
    in_required_not_copied = required - copied_files
    in_copied_not_required = copied_files - required

    if in_required_not_copied:
        for f in sorted(in_required_not_copied):
            # main.ts, deno.json, start.sh, Caddyfile are COPYed individually
            # in the second stage — they'll be in copied_files already
            warn(f"deploy.yml REQUIREs '{f}' but Dockerfile never COPYs it. "
                 "deploy.yml preflight will pass, but Docker build won't have the file.",
                 'warn')

    if in_copied_not_required:
        for f in sorted(in_copied_not_required):
            warn(f"Dockerfile COPYs '{f}' but deploy.yml does NOT require it. "
                 "Add to deploy.yml's REQUIRED=( ) list so the preflight catches a missing file before docker build runs.",
                 'warn')

    if not in_required_not_copied and not in_copied_not_required:
        ok("deploy.yml REQUIRED list and Dockerfile COPY list agree")


# ─────────────────────────────────────────────────────────────────────────
# Audit 4 — package.json build vs Dockerfile build
# ─────────────────────────────────────────────────────────────────────────

def audit_package_vs_dockerfile():
    header("4. package.json build script vs Dockerfile build")

    pkg = read('package.json')
    dockerfile = read('Dockerfile')
    if pkg is None:
        warn("package.json not found — skipping", 'warn')
        return
    if dockerfile is None:
        warn("Dockerfile not found — skipping", 'error')
        return

    try:
        pkg_data = json.loads(pkg)
    except json.JSONDecodeError as e:
        warn(f"package.json has JSON errors: {e}", 'error')
        return

    scripts = pkg_data.get('scripts', {})

    # Find all .js files terser'd by either side
    pkg_terser = set(re.findall(r'terser\s+(\S+\.js)', json.dumps(scripts)))
    dock_terser = set(re.findall(r'terser\s+(\S+\.js)', dockerfile))
    # Strip src/ prefix if present in either (Dockerfile may use src/foo.js or just foo.js)
    pkg_terser = {x.replace('src/', '') for x in pkg_terser}
    dock_terser = {x.replace('src/', '') for x in dock_terser}

    # html-minifier
    pkg_html = set(re.findall(r'html-minifier-terser\s+(\S+\.html)', json.dumps(scripts)))
    dock_html = set(re.findall(r'html-minifier-terser\s+(\S+\.html)', dockerfile))
    pkg_html = {x.replace('src/', '') for x in pkg_html}
    dock_html = {x.replace('src/', '') for x in dock_html}

    # cp foo public/
    pkg_cp = set(re.findall(r'cp\s+(\S+\.(?:html|js|json|png))\s+public/', json.dumps(scripts)))
    dock_cp = set(re.findall(r'cp\s+(\S+\.(?:html|js|json|png))\s+public/', dockerfile))
    pkg_cp = {x.replace('src/', '') for x in pkg_cp}
    dock_cp = {x.replace('src/', '') for x in dock_cp}

    diff = []

    for f in sorted(pkg_terser - dock_terser):
        diff.append(('terser', f, 'in package.json not Dockerfile'))
    for f in sorted(dock_terser - pkg_terser):
        diff.append(('terser', f, 'in Dockerfile not package.json'))
    for f in sorted(pkg_html - dock_html):
        diff.append(('html-min', f, 'in package.json not Dockerfile'))
    for f in sorted(dock_html - pkg_html):
        diff.append(('html-min', f, 'in Dockerfile not package.json'))
    for f in sorted(pkg_cp - dock_cp):
        diff.append(('cp', f, 'in package.json not Dockerfile'))
    for f in sorted(dock_cp - pkg_cp):
        diff.append(('cp', f, 'in Dockerfile not package.json'))

    if diff:
        for step, f, where in diff:
            warn(f"Build step mismatch: {step} {f} — {where}", 'warn')
    else:
        ok("package.json and Dockerfile build steps agree")


# ─────────────────────────────────────────────────────────────────────────
# Audit 5 — <script src> tags vs files that exist
# ─────────────────────────────────────────────────────────────────────────

def audit_script_tags():
    header("5. <script src=\"X.js\"> tags vs files actually present")

    for html_file in ('index.html', 'landing.html'):
        html = read(html_file)
        if html is None:
            info(f"{html_file} not found — skipping")
            continue

        # Find all <script src="X"> patterns — local refs only (no http://)
        srcs = re.findall(r'<script[^>]+\bsrc=["\']([^"\']+)["\']', html)
        local_srcs = [s for s in srcs if not s.startswith(('http://', 'https://', '//'))]

        for src in local_srcs:
            # Strip leading slash if present
            check = src.lstrip('/')
            if not Path(check).exists():
                warn(f"{html_file} references <script src=\"{src}\"> but '{check}' doesn't exist in the repo",
                     'error')
            else:
                info(f"{html_file} → {src} ✓")


# ─────────────────────────────────────────────────────────────────────────
# Audit 6 — DB_STORES vs dbGet/dbPut calls
# ─────────────────────────────────────────────────────────────────────────

def audit_db_stores():
    header("6. DB_STORES in app.js vs dbGet/dbPut store names")

    app = read('app.js')
    if app is None:
        warn("app.js not found — skipping", 'error')
        return

    # Extract DB_STORES = ['a','b',...]
    m = re.search(r"const\s+DB_STORES\s*=\s*\[([^\]]+)\]", app)
    if not m:
        warn("DB_STORES constant not found in app.js — skipping", 'warn')
        return

    stores = set(re.findall(r"['\"]([^'\"]+)['\"]", m.group(1)))
    info(f"DB_STORES declares {len(stores)} object stores")

    # Find every dbGet('xxx', ...) and dbPut('xxx', ...) call across ALL JS files
    # — budget.js, notes.js, demo.js, scanner.js can all use the same shared DB.
    used = set()
    for js_file in ('app.js', 'budget.js', 'notes.js', 'demo.js', 'scanner.js'):
        content = read(js_file)
        if content is None:
            continue
        # Strip single-line comments before scanning — we don't want documentation
        # mentions of dbPut('xxx', ...) to count as actual usage. Block comments
        # are less of a concern because they rarely contain runnable-looking
        # call syntax mid-prose.
        stripped = re.sub(r'//[^\n]*', '', content)
        # Also strip /* ... */ block comments (non-greedy across newlines)
        stripped = re.sub(r'/\*[\s\S]*?\*/', '', stripped)
        for match in re.finditer(r"db(?:Get|Put|Delete)\(['\"]([^'\"]+)['\"]", stripped):
            used.add(match.group(1))

    info(f"dbGet/dbPut/dbDelete (across all JS files) reference {len(used)} stores")

    missing = used - stores
    unused = stores - used

    if missing:
        for s in sorted(missing):
            warn(f"JS code uses store '{s}' (via dbGet/dbPut/dbDelete) but it's NOT in DB_STORES. "
                 f"IDB will throw NotFoundError on access. Add '{s}' to DB_STORES and bump DB_VERSION.",
                 'error')
    if unused:
        for s in sorted(unused):
            warn(f"DB_STORES declares '{s}' but no dbGet/dbPut/dbDelete uses it. "
                 "Dead store name — can be removed at the next DB_VERSION bump.",
                 'warn')

    if not missing and not unused:
        ok("DB_STORES matches actual usage")


# ─────────────────────────────────────────────────────────────────────────
# Audit 7 — Auth dual-path pattern in main.ts
# ─────────────────────────────────────────────────────────────────────────

def audit_auth_dual_path():
    header("7. Auth dual-path pattern (sessionToken || verifier)")

    main_ts = read('main.ts')
    if main_ts is None:
        warn("main.ts not found — skipping", 'error')
        return

    # The pattern from your standing invariant:
    #   ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }
    # On the SERVER side, the equivalent shape is reading both:
    #   const { emailHash, verifier, sessionToken } = await request.json()
    # then verifying via whichever was provided.

    # Find every route handler that reads BOTH verifier and sessionToken
    # vs handlers that read ONLY verifier (suspicious — would break passkey users)
    # vs handlers that read ONLY sessionToken (suspicious — would break passphrase users)

    # Coarse heuristic: scan each `if (url.pathname === '...' && request.method === 'POST')` block
    # for what destructuring patterns it uses.

    route_pattern = re.compile(
        r"if\s*\(\s*url\.pathname\s*===\s*['\"]([^'\"]+)['\"][\s\S]*?(?=\n\s*if\s*\(\s*url\.pathname|\n\s*\}\s*\}|\Z)",
        re.MULTILINE
    )

    # Routes that are deliberately session-less or use a different auth model
    # — exclude these from the dual-path check.
    excluded_prefixes = (
        '/ping', '/robots.txt',
        '/webhook',              # Stripe webhooks use signature verification, not user auth
        '/auth/register',        # Initial register has no session yet
        '/auth/login',           # Initial login has no session yet
        '/auth/otp',             # OTP flow uses email-based auth
        '/recovery',             # Recovery uses recovery code, not session/verifier
        '/admin',                # Admin routes have their own auth model
        '/unsubscribe',          # Public unsubscribe link, token-based
        '/user/verify',          # Login endpoint — verifier is the auth
        '/user/register',
        '/email/verify',         # Email verification uses OTP
        '/share/join',           # Share join uses join code
        '/key/store',            # Key storage during registration
        '/passkey/register',     # Passkey registration starts the session
        '/passkey/login',
        '/passkey/begin',
        '/passkey/finish',
        '/passkey/auth/begin',   # Passkey login: pre-session phase, no session token yet
        '/passkey/auth/finish',  # Passkey login: produces session token, doesn't use one
        '/passkey/verify-session', # Verifies a session token — by definition session-only
        '/key/passkey-prf-store', # Passkey-specific operation, requires active passkey session
        '/key/passkey-prf-get',   # Passkey-specific operation
        '/referral',             # Public referral lookup
        '/device/register',      # Device registration may not have session yet
        '/billing/checkout',     # Stripe checkout init
        '/presence-stream',      # SSE handshake
        '/presence-update',      # Presence beacon (uses sessionToken-only commonly)
        '/check-now',
        '/send-reminder',
        '/set-schedule',
        '/reset-schedule',
        '/debug-schedule',
        '/user/delete-confirm-send', # Intentionally verifier-only: account deletion explicitly
                                     # requires the passphrase even for passkey-enabled users.
                                     # Highest-stakes action; client gates passkey users out.
        '/data/force-remote/ack',
        '/data/modified',
    )

    findings_this = []
    routes_checked = 0
    for match in route_pattern.finditer(main_ts):
        path = match.group(1)
        body = match.group(0)
        if any(path.startswith(p) for p in excluded_prefixes):
            continue
        # Skip GET-like routes (route_pattern requires POST anyway)
        routes_checked += 1

        # Look for body destructuring of incoming request specifically:
        #   const { ..., verifier, ... } = await request.json()
        # NOT every appearance of the word 'verifier' in the function body —
        # kvGet(['user', emailHash, 'verifier']) is a key lookup, not auth.
        body_destructure = re.search(
            r"const\s*\{([^}]+)\}\s*=\s*await\s+request\.json\(\)",
            body
        )
        if not body_destructure:
            continue  # route doesn't destructure body — likely lookup-only
        destructured_fields = body_destructure.group(1)
        reads_verifier = bool(re.search(r'\b(?:guest)?[Vv]erifier\b', destructured_fields))
        reads_token    = bool(re.search(r'\b(?:guest)?[Ss]essionToken\b', destructured_fields))

        if reads_verifier and not reads_token:
            findings_this.append((path, 'reads verifier only — passkey users will fail'))
        elif reads_token and not reads_verifier:
            findings_this.append((path, 'reads sessionToken only — passphrase users will fail'))

    info(f"Checked {routes_checked} authenticated routes")

    if findings_this:
        for path, msg in findings_this:
            warn(f"{path}: {msg}", 'warn')
    else:
        ok("All authenticated routes accept both sessionToken and verifier")


# ─────────────────────────────────────────────────────────────────────────
# Audit 8 — write_dockerfile.py outputs vs committed files
# ─────────────────────────────────────────────────────────────────────────

def audit_writedockerfile_drift():
    header("8. write_dockerfile.py outputs vs committed Dockerfile/Caddyfile/start.sh")

    if not Path('write_dockerfile.py').exists():
        info("write_dockerfile.py not found — skipping")
        return

    # Regenerate into a temp directory and diff against committed files
    with tempfile.TemporaryDirectory() as td:
        # The script writes to the current directory, so we copy it into the temp dir and run it
        import shutil
        shutil.copy('write_dockerfile.py', td)
        try:
            result = subprocess.run(
                ['python3', 'write_dockerfile.py'],
                cwd=td,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                warn(f"write_dockerfile.py failed to run: {result.stderr}", 'error')
                return
        except Exception as e:
            warn(f"Could not run write_dockerfile.py: {e}", 'warn')
            return

        # Diff each output
        any_drift = False
        for f in ('Dockerfile', 'Caddyfile', 'start.sh'):
            if not Path(f).exists():
                warn(f"committed {f} is missing — write_dockerfile.py would create it", 'warn')
                any_drift = True
                continue
            generated = Path(td, f).read_text(encoding='utf-8')
            committed = Path(f).read_text(encoding='utf-8')
            # Normalise line endings before comparing
            if generated.replace('\r\n', '\n') != committed.replace('\r\n', '\n'):
                any_drift = True
                warn(f"{f} on disk differs from what write_dockerfile.py generates. "
                     f"Either commit the regenerated file (run `python3 write_dockerfile.py`) "
                     f"or update the template inside write_dockerfile.py to match the on-disk file.",
                     'error')

        if not any_drift:
            ok("write_dockerfile.py output matches committed Dockerfile/Caddyfile/start.sh")


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main():
    if not Path('main.ts').exists() and not Path('app.js').exists():
        print(f"{RED}This doesn't look like the STOCKROOM repo. "
              f"Run from the repo root.{RESET}")
        sys.exit(1)

    print(f"{BOLD}STOCKROOM repo drift audit{RESET}")
    print(f"{DIM}Static analysis only — no network calls, no DB queries.{RESET}")

    audit_caddyfile_routes()
    audit_dockerfile_copy()
    audit_deploy_yml()
    audit_package_vs_dockerfile()
    audit_script_tags()
    audit_db_stores()
    audit_auth_dual_path()
    audit_writedockerfile_drift()

    # Summary
    print()
    errors = [f for f in findings if f[0] == 'error']
    warns  = [f for f in findings if f[0] == 'warn']
    if not findings:
        print(f"{GREEN}{BOLD}━━━ Clean: no drift detected ━━━{RESET}")
        sys.exit(0)
    else:
        print(f"{BOLD}━━━ Summary: {len(errors)} errors, {len(warns)} warnings ━━━{RESET}")
        if errors:
            print(f"{RED}{BOLD}Errors block deploys — fix these first.{RESET}")
        if warns:
            print(f"{YELLOW}Warnings are non-blocking but worth addressing.{RESET}")
        sys.exit(1 if errors else 0)


if __name__ == '__main__':
    main()
