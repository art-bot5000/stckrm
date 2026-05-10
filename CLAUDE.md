# STOCKROOM — Cowork workspace brief

This is your context for working on STOCKROOM. Read this fully before
touching any file. Pete has tuned this brief deliberately — the rules
here have specific bug-and-pain-point origin stories, so don't second-
guess them.

## What this app is

STOCKROOM is a household consumables tracker + grocery + budget +
notes/reminders PWA. It is in **active iterative development**, used
in real life by Pete and a small number of testers. **Production
breakage is real-world breakage.** Treat every push as a live deploy
to a tool people use.

## Repository layout

```
app.js          ~13,500+ lines — primary frontend logic. THE file.
index.html      Thin shell. Markup, modals, SVG icon defs. NOT logic.
styles.css      All styling.
sw.js           Service worker. Cache key controls staleness.
scanner.js      Lazy-loaded barcode + Open Food Facts integration.
main.ts         Deno backend, ~1,650 lines. Single entry point.
deno.json       Deno config.
admin.html      Admin panel (separate, 2FA-gated).
landing.html    Marketing / signed-out page.
manifest.json   PWA manifest.
fly.toml        Production Fly app: stckrm
fly_staging.toml Staging Fly app: stckrm-staging
deploy.yml      .github/workflows/ — main → production
deploy-staging.yml .github/workflows/ — staging → staging app
Dockerfile      Multi-stage: Node minify (Terser/CleanCSS/html-min) → Deno+Caddy
Caddyfile       Caddy server config (Brotli enabled).
start.sh        Container entrypoint.
```

## Branch + deploy model

- `main` branch → production app `stckrm` (auto-deploy via deploy.yml)
- `staging` branch → staging app `stckrm-staging` (auto-deploy via deploy-staging.yml)
- **Default to working on `staging` for any non-trivial change.** Cut
  over to `main` only after Pete confirms staging looks good on his
  Android device.
- The CI is the deploy. Do not invoke `flyctl deploy` locally; commit
  and push, then watch the Actions run.

## Hard rules — non-negotiable

These are wired to specific past disasters. Do not relax them.

1. **NEVER bump the `sw.js` cache version.** That is Pete's job. He
   coordinates SW bumps with frontend cuts and does not want a Cowork
   session pre-empting that decision. If a change clearly requires a
   SW bump, **flag it in the commit message** and stop — do not do it.

2. **Auth duality is real and lethal.** Every endpoint in `main.ts`
   that requires a credential MUST accept BOTH `verifier` (password
   accounts) AND `sessionToken` / `guestSessionToken` (passkey
   accounts). Every client-side guard MUST check both:
   ```js
   if (!_kvVerifier && !_kvSessionToken) return;
   ```
   And outgoing requests use the canonical pattern:
   ```js
   _kvSessionToken ? { sessionToken } : { verifier }
   ```
   When fixing one auth bug, **sweep the file for sibling instances**
   — passkey-auth bugs always come in clusters because they were
   originally written before passkeys existed.

3. **Frontend logic lives in `app.js`, not `index.html`.** A past
   mistake involved applying fixes to `index.html` that were entirely
   ineffective because the corresponding logic lives in `app.js`. If
   you are editing JavaScript, you are editing `app.js`. If you find
   yourself opening `index.html` to change behaviour, stop and
   reconsider.

4. **Background syncs are silent.** Any sync triggered by queue,
   timer, focus, or visibility change passes `silent=true` to
   suppress the sync pill. The sync pill is reserved for
   user-initiated changes via `_syncQueue.enqueue()`. Do not surface
   UI feedback for background work.

5. **Deploy order matters: `main.ts` first, frontend second.**
   Because the CI deploys everything in one push, this means:
   schema/endpoint changes in `main.ts` MUST be backwards-compatible
   with the currently-deployed frontend. Frontend changes that
   require new backend endpoints MUST land in two separate pushes:
   backend first, wait for green CI + ~60s for Fly rollout, then
   frontend.

6. **All file writes use UTF-8 + Unix line endings, NO BOM.** Pete is
   on Windows; PowerShell will sometimes try to add a BOM to files.
   This caused a real production corruption during the Fly migration.
   When writing files, explicitly write UTF-8 and verify no `\ufeff`
   at the start.

7. **All user data is AES-GCM encrypted client-side before reaching
   the server.** Do not add server-side processing of user content
   (groceries, items, notes, reminders). The server sees ciphertext
   only. The only exceptions are: email addresses (hashed for
   indexing), KDF salts, share keys (encrypted with owner's data
   key), public ECDH keys, and admin metadata.

8. **`tombstones` exist for a reason — preserve them.** Both
   household deletion and grocery item deletion use localStorage
   tombstones to prevent sync from re-creating deleted records.
   Don't simplify deletion paths to "just remove from the array"
   without checking the tombstone pattern.

## Validation — required before any commit

This is the local gate. CI repeats it but local failure saves time.

```bash
# 1. JS syntax check (CI does this too — match it locally)
node --check app.js
node --check scanner.js

# 2. If main.ts changed: type-check via Deno
deno check main.ts

# 3. If you touched the Dockerfile or added new files, verify the
#    file list in deploy.yml's preflight step still matches reality.
```

If `node --check` fails, do NOT commit. Fix and re-check. The CI
preflight will reject the push otherwise and waste a deploy slot.

## Editing patterns specific to this codebase

### `app.js` is huge — line numbers shift constantly

Always re-view the section you intend to edit immediately before
editing it. Do not rely on line numbers from a previous view. Use
`grep -n` to find anchors:

```bash
grep -n "function kvSyncNow" app.js
grep -n "passphraseAuth" main.ts
```

Anchor to **distinctive identifiers**, not line numbers. When using
str_replace, the `old_str` should be the smallest unique snippet
that unambiguously identifies the edit location.

### `main.ts` endpoint pattern

Every endpoint in `main.ts` follows this shape:

```typescript
if (url.pathname === '/some/path' && request.method === 'POST') {
  try {
    const body = await request.json();
    const { sessionToken, verifier, ...payload } = body;

    // BOTH credential types accepted — this is the canonical pattern
    const auth = await authenticateRequest({ sessionToken, verifier, emailHash });
    if (!auth.ok) return json({ error: 'unauthorized' }, corsHeaders, 401);

    // ... do the work ...
    return json({ ok: true, ...result }, corsHeaders);
  } catch (err) {
    return json({ error: err.message }, corsHeaders, 500);
  }
}
```

When adding a new endpoint, copy this shape exactly. Especially the
auth-duality pattern.

### Service worker changes

If you touch `sw.js`, **stop and ask Pete** before committing. The
cache key controls staleness across all installed PWAs and bumping
it is his decision because it interacts with rollout strategy.

## What you (Cowork) should and shouldn't do

### Good Cowork tasks for this repo

- Implementing a clearly-scoped feature with a written spec.
- Sweeping `main.ts` for a known bug pattern across all endpoints.
- Adding a new modal/screen with HTML in `index.html` + logic in
  `app.js` + styling in `styles.css`.
- Renaming a function and updating all call sites.
- Writing a one-off migration script.
- Running `node --check` and `deno check` and reporting results.
- Drafting a commit message describing what changed.

### Tasks you should refuse / escalate

- Bumping `sw.js` cache version (rule 1).
- Pushing to `main` without explicit instruction (use `staging`).
- Anything that changes the crypto envelope, key derivation, or KDF
  parameters. Crypto changes need Pete's review.
- Changes to `Dockerfile`, `Caddyfile`, `fly.toml`, `start.sh`, or
  GitHub Actions workflows without explicit instruction.
- Force-push, history rewrite, branch deletion. Don't.
- Any change that would require a one-time data migration on
  existing users' encrypted blobs. Flag it and stop.

## Commit + push hygiene

- One logical change per commit. If you fixed three unrelated bugs,
  that's three commits.
- Commit message format:
  ```
  <area>: <imperative summary>

  - bullet of what changed
  - bullet of why
  - mention if SW bump needed (DO NOT bump it yourself)
  ```
  Areas: `groceries`, `stockroom`, `budget`, `share`, `auth`, `sync`,
  `ui`, `crypto`, `backend`, `build`.
- Push to `staging` by default. Confirm with Pete before pushing to
  `main`.
- After pushing, watch the GitHub Actions run. Report the URL of the
  run + the result. If the run fails, **do not retry** — show Pete
  the failure and let him decide.

## Testing reality

You cannot do meaningful end-to-end testing on this app from
desktop. The hard surfaces are:

- Mobile-data flakiness (only Pete's Android device exposes this)
- Multi-account sharing (requires `pete@artbot5000.com` AND
  `pasmith984@gmail.com` sessions on separate devices)
- PWA install + service worker upgrade behaviour
- Camera scanner integration (mobile-only)

So your role in testing is bounded:
- Static checks: `node --check`, `deno check`, search for obvious
  references to renamed identifiers.
- Browser smoke test on staging URL: load page, sign in, click
  through the changed surface, screenshot the result.
- Report what you saw. **Do not declare a feature "tested" based on
  desktop browser behaviour alone.** Real validation happens on
  Pete's phone.

## Communication style Pete prefers

- Direct. State what you did and what you didn't do.
- No prefacing every response with a summary of the request.
- Push back when you disagree — Pete prefers correction to
  agreement-by-default.
- Flag uncertainty explicitly. "I think this is right but I haven't
  verified X" is more useful than confident-but-wrong.
- Don't pad with "Let me know if you need anything else!" — finish
  the work, summarise the result, stop.

## Glossary of in-repo terms

- **Household** — a sharing unit. One owner + zero or more guests.
  Each user can be in multiple households.
- **Share target** — a household someone has been invited to, from
  the joining user's perspective.
- **DATA KEY** — the per-account AES-GCM key that encrypts all user
  data. Wrapped by the passphrase-derived wrap key, by each recovery
  code, and by a device-trust secret.
- **v1 / v2 crypto** — see `stockroom-context.md` for full detail.
  v2 = PBKDF2 600k + AES-KW + random KDF salt. v1 accounts auto-
  migrate after `CRYPTO_V2_SWITCHOVER`.
- **`_kvVerifier`** — password-account credential held in memory.
- **`_kvSessionToken`** — passkey-account credential held in memory.
- **`_shareState`** — set when the user is acting as a guest
  viewing a shared household; takes priority over own-account pull.
- **Tombstone** — a localStorage record of "I deliberately deleted
  this" used to prevent sync from re-creating it.
- **Sync pill** — the toast-style indicator that appears for
  user-initiated changes flowing through `_syncQueue`.

## When in doubt

Ask Pete before doing the thing. He prefers a clarifying question
to a wrong push.
