# STOCKROOM security audit harness

Three Node scripts that together answer the question: *can user data leave the
app without a valid login?*

Each phase is independently runnable and writes a Markdown report. The
top-level `audit.js` runs all three in sequence and exits with the worst
status code of the three.

```
audit.js           — runs all three phases
audit-inventory.js — static scan of main.ts: classify every endpoint's auth
audit-probe.js     — runtime: fire 6 bad-credential scenarios per endpoint
audit-crypto.js    — runtime: confirm responses contain only ciphertext
```

No source files were modified to build this harness. Drop the `audit/`
folder anywhere in the repo (suggest top level, alongside `main.ts`).

---

## Quick start

```sh
# Phase 1 only (static, no network):
node audit/audit.js

# All three phases against staging:
export ACCOUNT_A_EMAIL=audit-a@stockroom.test
export ACCOUNT_A_PASSPHRASE='pick-something-long-and-random'
export ACCOUNT_B_EMAIL=audit-b@stockroom.test
export ACCOUNT_B_PASSPHRASE='pick-something-else-equally-random'
node audit/audit.js https://stckrm-staging.fly.dev
```

The probe **refuses to run against `*.fly.dev` URLs that don't contain
`staging`** unless `ALLOW_PROD=1` is set. This is on purpose — Phase 2 fires
hundreds of malformed requests and creates two test accounts, neither of
which you want on prod.

---

## Phase 1: inventory (`audit-inventory.js`)

Parses `main.ts`, finds every `if (url.pathname === '...')` handler, walks the
handler body, and tags it with one of:

| Pattern | Meaning |
|---|---|
| 🔐 user | uses `requireUserAuth`/`verifyUserAuth` or the inline `verifier‖sessionToken` pair |
| 🛡️ admin | uses `verifyAdminRequest` / `ADMIN_SECRET` |
| ⚙️ dispatch | `PUSH_DISPATCH_SECRET` bearer (cron) |
| 🪝 webhook | Stripe signature verification |
| 🔑 recovery | short-lived recovery token |
| 🗑️ delete-token | email-delivered single-use deletion token |
| 📧 otp | OTP send/verify (auth is the OTP itself) |
| 🚪 auth-flow | pre-auth (registration, challenge, login) — returns no user data |
| 🌍 public | explicitly no-auth-by-design |
| ⚠️ FINDING | matched no known pattern — needs review or fix |

The classifier is the trustworthy bit. New endpoints that don't fit a known
pattern flip the exit code to 1. **Commit the inventory report and re-run on
every deploy** — any new line in the FINDING bucket is a security review
gate.

### What it caught on the current `main.ts`

18 unclassified endpoints, bucketed by what an attacker could actually do:

| Severity | Endpoints |
|---|---|
| 🔴 DATA-EXPOSURE (1) | `/share/key/get` |
| 🟠 DATA-MUTATION (7) | `/device/register`, `/device/seen`, `/share/key/store`, `/set-schedule`, `/reset-schedule`, `/unsubscribe`, `/presence-update` |
| 🟡 ACCOUNT-ENUM (2) | `/debug-user`, `/user/email-verified` |
| 🟡 EMAIL-ABUSE (1) | `/send-reminder` |
| 🟢 SERVICE-ABUSE (1) | `/check-now` |
| 🟢 METADATA-LEAK (6) | `/debug-kv`, `/debug-schedule`, `/data/modified`, `/share/data/modified`, `/presence-list`, `/presence-stream` |

The full report (`audit-inventory.md`) shows each handler's source snippet
inline so you can see exactly what the classifier was looking at.

---

## Phase 2: probe (`audit-probe.js`)

For every endpoint Phase 1 tagged `🔐 user`, fires six requests and asserts
the server returns non-200 to all of them:

1. No credentials at all
2. `emailHash` only, no verifier or session token
3. `emailHash` + random verifier
4. `emailHash` + random session token
5. Target's `emailHash` + attacker's real verifier
6. Target's `emailHash` + attacker's real session token

Both `verifier` and `sessionToken` are tested against every endpoint — this
is the explicit defence against the recurring auth-gap bug pattern
(share/sync/presence/MFA/key-management).

The probe also runs targeted checks for known finding categories:
account-enum (distinguish known vs unknown email), public-write
(`/presence-update`, `/set-schedule`), and email-abuse (`/send-reminder`).

Skipped endpoints (would create real test garbage on staging if probed
repeatedly): `/data/push`, `/data/pull`, share data push, deletes, key
updates, share creation. These are best covered by a separate
manual integration test pass once a release.

---

## Phase 3: crypto invariants (`audit-crypto.js`)

Two invariants, both runtime:

**A. Plaintext-free storage.** Pulls `/data/pull` for the test account and
greps the response for the test email address, recognisable data-field names
(`"items":[`, `"groceries":[`, etc.), and a fresh per-run canary string.
Zero matches expected. If anything plaintext shows up, the encryption
boundary has a hole — a server breach or stolen R2 snapshot would expose
user data.

**B. Wrong-key fails to decrypt.** Pulls the ciphertext, derives an AES-GCM
key from the *wrong* passphrase, attempts to decrypt. AES-GCM's auth tag
must fail. This confirms the server isn't quietly handing back a different
blob or plaintext fallback to a wrong-key request.

Invariant B needs the test account to have pushed at least one data blob.
If `/data/pull` comes back with no ciphertext, the script says so and skips
B rather than passing it falsely.

---

## What the harness does NOT cover

Worth stating plainly so you don't get false confidence:

- **Client-side leaks.** A guest's UI revealing notes they shouldn't see, or
  shared data merged into the wrong household — these are app.js bugs and
  need separate manual testing.
- **Supply chain.** Compromised Terser, npm dep, or build container.
- **Compromised Fly machine** or admin session that *legitimately* extracts
  data (your existing audit log and IP-bound admin sessions are the control
  here, not this harness).
- **Resend webhook impersonation** or other third-party-service spoofing.

This harness is specifically for: *can an HTTP caller without a valid login
get user data out of the Stockroom API?*

---

## Wiring into CI

Add to `.github/workflows/deploy.yml`, after the existing
`node --check app.js`:

```yaml
- name: Endpoint inventory audit
  run: node audit/audit.js
```

That runs Phase 1 only (no network). It fails the build on any new
unclassified endpoint, which is exactly what you want for catching new
auth-gap bugs at PR time.

For Phase 2/3 (runtime), suggest a separate workflow that runs on the
staging deploy completion, not the prod deploy — staging gives the probe a
safe target.

---

## Addressing the 18 findings

Here are concrete teaching points for the most impactful fixes, in the
format you prefer (file, unique search string, exact change). All of these
land in `main.ts` and warrant a coordinated deploy — fix several together,
then bump cache version manually as usual.

### 1. `/share/key/get` — partial-auth (🔴 data exposure)

**File:** `main.ts`
**Find with Ctrl+F:** `if (url.pathname === '/share/key/get'`
**Change:** the body destructures `{ ownerEmailHash, verifier, code }` —
missing `sessionToken`. Replace the destructure and the auth check with the
canonical dual-path pattern. After the change, the block should accept
both passphrase and passkey users.

Before:
```ts
const { ownerEmailHash, verifier, code } = await request.json();
if (!ownerEmailHash || !verifier || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
```

After:
```ts
const { ownerEmailHash, verifier, sessionToken, code } = await request.json();
if (!ownerEmailHash || !code || (!verifier && !sessionToken)) return json({ error: 'Missing fields' }, corsHeaders, 400);
const _authFail = await requireUserAuth(ownerEmailHash, verifier, sessionToken);
if (_authFail) return _authFail;
```

**Verify:** save, then `deno check main.ts` (or just attempt deploy — Deno
will catch the type error). Re-run `node audit/audit.js`; this endpoint
should drop out of the findings list.

### 2. Same fix for `/share/key/store`, `/device/register`, `/device/seen`

Same pattern, same fix. Each one currently has `verifier`-only inline
checks. Replace with `requireUserAuth(emailHash, verifier, sessionToken)`.

### 3. `/debug-user` — account enumeration (🟡)

**File:** `main.ts`
**Find with Ctrl+F:** `if (url.pathname === '/debug-user'`
**Decision:** is this endpoint actually used by anything in `app.js`? If no
(likely — it's named `debug-`), delete the whole handler. If yes, gate it
behind `verifyAdminRequest`.

Quick check from a terminal: `grep -n "debug-user" app.js` — if zero hits,
safe to remove.

### 4. `/user/email-verified` — account enumeration (🟡)

This one is harder because the login flow legitimately needs to know whether
to prompt the user to verify their email. Two options:

- **Constant-response:** always return `{ exists: true, verified: false }`
  and let the actual verify flow fail on bad input. Loses functionality.
- **Auth-gate:** require a verifier or sessionToken before returning the
  email, returning a generic `{ ok: true, verified: <bool> }` only. The
  client already has the email (they typed it), so don't return it again.

Option 2 is the right call. Find with `if (url.pathname === '/user/email-verified'`
and require auth before returning anything other than the bare `verified`
boolean. Don't return the `email` field at all — the client already has it.

### 5. `/send-reminder` — email abuse (🟡)

**File:** `main.ts`
**Find with Ctrl+F:** `if (url.pathname === '/send-reminder'`
**Decision:** this endpoint is part of the legacy single-user
Google-Drive-era email cron. Is it still called from `app.js`?

```sh
grep -n "send-reminder" app.js
```

If no hits, delete the handler entirely. If it's still in use, require
`emailHash + verifier|sessionToken` and verify the `email` field matches the
auth'd account's stored email. The current implementation is essentially an
open mail relay scoped to your Resend account.

### 6. Presence cluster — `/presence-update`, `/presence-list`, `/presence-stream`

These are the noisiest findings by volume. They predate the multi-user
model. Two paths:

- **Scope to a share code.** Require `emailHash + verifier|sessionToken +
  shareCode`, verify the user is a member of that share, and key presence
  entries by `[presence, shareCode, userId]` so the listing returns only
  members of the same share.
- **Remove entirely** if the feature isn't carrying its weight.

Option 1 is right if the feature is valuable — presence is most useful in a
shared household context anyway. Either way, today's "global anonymous
presence" is broken-by-design and worth addressing in one chunk.

### 7. Legacy schedule cluster — `/set-schedule`, `/reset-schedule`,
`/unsubscribe`, `/check-now`, `/debug-schedule`, `/debug-kv`

All vestiges of the single-tenant Drive era. Quickest path: search `app.js`
for each path, and if it's not called anywhere, delete the handler. The
ones that ARE still wired need auth gates added.

For each:
```sh
grep -n "set-schedule\|reset-schedule\|unsubscribe\|check-now\|debug-schedule\|debug-kv" app.js
```

### 8. `/data/modified` and `/share/data/modified` (🟢 metadata leak)

These are how clients cheaply poll for changes without pulling the full
blob. The leak is "anyone with a share code can poll its modified
timestamp." Lower severity than the others — fix by requiring user-auth on
both branches (the user branch needs `sessionToken` added; the share branch
needs auth at all).

---

## Session end checklist

Files created (new — none modified in `/mnt/project/`):

- `audit/audit.js` — 47 lines
- `audit/audit-inventory.js` — 209 lines
- `audit/audit-probe.js` — 270 lines
- `audit/audit-crypto.js` — 213 lines
- `audit/audit-inventory.md` — generated report (re-run to refresh)
- `audit/audit-inventory.json` — generated, consumed by probe

Nothing to commit yet for the actual fixes — those are decisions for you to
make from the findings list above. The harness itself is ready to drop in
under `audit/` in the repo root.
