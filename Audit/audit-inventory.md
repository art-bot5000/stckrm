# STOCKROOM endpoint inventory

Source: `./main.ts` (7550 lines)
Generated: 2026-05-23T22:39:58.502Z
Endpoints found: **124**

## Summary by auth pattern

| Pattern | Count |
|---|---:|
| 🔐 user | 61 |
| 🛡️  admin | 22 |
| ⚙️  dispatch | 1 |
| 🪝 webhook | 1 |
| 🔑 recovery | 3 |
| 🗑️  delete-token | 1 |
| 📧 otp | 8 |
| 🚪 auth-flow | 10 |
| 🌍 public | 3 |
| ⚠️  FINDING | 14 |

## ⚠️  Findings (14)

These endpoints did not match any known auth pattern. Each one must either be confirmed safe (and the classifier updated to recognise it), or fixed.

### Severity buckets

| Severity | Meaning |
|---|---|
| 🔴 DATA-EXPOSURE | Could return a user's data to an unauthenticated caller |
| 🟠 DATA-MUTATION | Could modify a user's data without their auth |
| 🟡 ACCOUNT-ENUM | Lets an attacker confirm whether an account exists for an email |
| 🟡 EMAIL-ABUSE | Lets an attacker cause emails to be sent (spam vector, reputation harm) |
| 🟢 SERVICE-ABUSE | Lets an attacker consume server resources without auth |
| 🟢 METADATA-LEAK | Reveals non-content info (online users, timestamps, counts) |
| ⚪ REVIEW | Needs manual review to determine impact |

### 🟠 DATA-MUTATION (4)

#### `POST /presence-update` (line 6446)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/presence-update' && request.method === 'POST') {
    try {
      const { userId, name, initials, colour, view } = await request.json();
      if (!userId) return json({ error: 'Missing userId' }, corsHeaders, 400);
      await kvSet(['presence', userId], JSON.stringify({ userId, name, initials, colour, view, ts: new Date().toISOString() }), { expireIn: 5 * 60 * 1000 });
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }
```

#### `POST /set-schedule` (line 6491)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/set-schedule' && request.method === 'POST') {
    try {
      const body = await request.json();
      const { email, emailHash, startDate, startTime, intervalDays, household, urgent = [], upcoming = [] } = body;
      if (!email || !startDate) return json({ error: 'Missing email or startDate' }, corsHeaders, 400);
      const sfx     = household && household !== 'default' ? `:${household}` : '';
      const ehash   = emailHash || await hashEmail(email);
      // ── Free-tier email gate ──
      // Per spec, emails for free-tier users are 'silently dropped'. We
      // return ok=true but skip persisting the schedule. The next time the
      // user upgrades and re-saves, the schedule will be re-created. We
      // also delete any pre-existing schedule so we don't keep emailing a
      // free-tier user from a stale schedule.
      const gate = await gateFeature(ehash, 'emails');
      if (!gate.ok) {
        try { await kvDel([`schedule${sfx}`]); } catch (_) {}
        try { await kvDel([`user_items${sfx}`]); } catch (_) {}
        return json({ ok: true, skipped: 'free_tier' }, corsHeaders);
      }
      await kvSet([`schedule${sfx}`], JSON.stringify({ startDate, startTime: startTime||'09:00', intervalDays: intervalDays??30, email, emailHash: ehash }));
      if (urgent.length || upcoming.length) await kvSet([`user_items${sfx}`], JSON.stringify({ urgent, upcoming }));
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }
```

#### `POST /reset-schedule` (line 6519)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/reset-schedule' && request.method === 'POST') {
    try {
      const body      = await request.json().catch(() => ({}));
      const household = body.household || null;
      const key       = household && household !== 'default' ? `last_sent:${household}` : 'last_sent';
      await kvDel([key]);
    } catch(e) { /* ok */ }
    return json({ ok: true }, corsHeaders);
  }
```

#### `POST /unsubscribe` (line 6530)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/unsubscribe' && request.method === 'POST') {
    try {
      const body      = await request.json().catch(() => ({}));
      const household = body.household || null;
      const sfx       = household && household !== 'default' ? `:${household}` : '';
      await kvDel([`schedule${sfx}`]);
      await kvDel([`last_sent${sfx}`]);
      await kvDel([`user_items${sfx}`]);
    } catch(e) { /* ok */ }
    return json({ ok: true }, corsHeaders);
  }
```

### 🟡 ACCOUNT-ENUM (2)

#### `POST /user/email-verified` (line 3899)

**Reason classifier flagged it:** partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN

```ts
  if (url.pathname === '/user/email-verified' && request.method === 'POST') {
    try {
      const { emailHash } = await request.json();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      const verifierRow = await kvGet(['user', emailHash, 'verifier']);
      if (!verifierRow.value) return json({ exists: false, verified: false }, corsHeaders);
      const verified = await kvGet(['user', emailHash, 'email_verified']);
      const emailRow = await kvGet(['user', emailHash, 'email']);
      return json({
        exists: true,
        verified: !!verified.value,
        email: emailRow.value || null,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }
```

#### `POST /debug-user` (line 5562)

**Reason classifier flagged it:** partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN

```ts
  if (url.pathname === '/debug-user' && request.method === 'POST') {
    try {
      const { emailHash } = await request.json();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      const hasVerifier = !!(await kvGet(['user', emailHash, 'verifier'])).value;
      const hasData     = !!(await kvGet(['user', emailHash, 'data', 'default'])).value;
      const created     = (await kvGet(['user', emailHash, 'created'])).value;
      const modified    = (await kvGet(['user', emailHash, 'modified', 'default'])).value;
      return json({ emailHash, hasVerifier, hasData, created, modified }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }
```

### 🟡 EMAIL-ABUSE (1)

#### `POST /send-reminder` (line 6543)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/send-reminder' && request.method === 'POST') {
    try {
      const body = await request.json();
      const { email, urgent = [], upcoming = [], manual = false } = body;
      if (!email) return json({ error: 'Missing email' }, corsHeaders, 400);
      const result = await sendEmail(email, urgent, upcoming);
      if (!result.ok) return json({ error: result.error }, corsHeaders, 500);
      if (!manual) await kvSet(['last_sent'], new Date().toISOString());
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }
```

### 🟢 SERVICE-ABUSE (1)

#### `POST /check-now` (line 6583)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/check-now' && request.method === 'POST') {
    try {
      await cronCheck();
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }
```

### 🟢 METADATA-LEAK (6)

#### `GET /debug-kv` (line 4334)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/debug-kv' && request.method === 'GET') {
    // Test KV read with explicit timeout
    const kvReadWithTimeout = async (key) => {
      const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('KV read timeout')), 5000));
      return Promise.race([kv.get(key), timeout]);
    };
    try {
      const health = await kvReadWithTimeout(['_health']);
      const shareCount = { n: 0 };
      try {
        const entries = kv.list({ prefix: ['share'] });
        for await (const _ of entries) shareCount.n++;
      } catch(e) { /* ok */ }
      return json({ ok: true, kvReads: 'working', health: health.value, shareTargets: shareCount.n, ts: new Date().toISOString() }, corsHeaders);
    } catch(e) {
      return json({ ok: false, error: e.message, ts: new Date().toISOString() }, corsHeaders, 500);
    }
  }
```

#### `POST /data/modified` (line 5676)

**Reason classifier flagged it:** partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN

```ts
  if (url.pathname === '/data/modified' && request.method === 'POST') {
    try {
      const { emailHash, verifier, household, shareCode } = await request.json();
      const hKey = household || 'default';
      let modifiedVal = null;
      if (emailHash && verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (stored.value && stored.value === verifier) {
          const m = await kvGet(['user', emailHash, 'modified', hKey]);
          modifiedVal = m.value;
        }
      } else if (shareCode) {
        const r = await kvGet(['share', shareCode.toUpperCase()]);
        if (r.value) {
          const target = JSON.parse(r.value);
          const m = await kvGet(['user', target.ownerEmailHash, 'modified', hKey]);
          modifiedVal = m.value;
        }
      }
      return json({ modifiedTime: modifiedVal }, corsHeaders);
    } catch(err) {
      return json({ modifiedTime }, corsHeaders);
    }
  }
```

#### `POST /share/data/modified` (line 6307)

**Reason classifier flagged it:** partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN

```ts
  if (url.pathname === '/share/data/modified' && request.method === 'POST') {
    try {
      const { guestEmailHash, guestVerifier, code, household } = await request.json();
      if (!code || !guestEmailHash || !guestVerifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const guestStored = await kvGet(['user', guestEmailHash, 'verifier']);
      if (!guestStored.value || guestStored.value !== guestVerifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const hKey    = household && household !== 'default' ? household : 'default';
      const modified = await kvGet(['share_data', code.toUpperCase(), `${hKey}_modified`]);
      return json({ modifiedTime: modified.value||null }, corsHeaders);
    } catch(err) { return json({ modifiedTime: null }, corsHeaders); }
  }
```

#### `GET /presence-list` (line 6456)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/presence-list' && request.method === 'GET') {
    try {
      const users = [];
      const entries = kv.list({ prefix: ['presence'] });
      for await (const entry of entries) {
        try { users.push(JSON.parse(entry.value)); } catch(e) {}
      }
      return json({ users }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }
```

#### `GET /presence-stream` (line 6468)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/presence-stream' && request.method === 'GET') {
    const stream = new ReadableStream({
      start(controller) {
        const send = (data) => controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`));
        const interval = setInterval(async () => {
          try {
            const users = [];
            const entries = kv.list({ prefix: ['presence'] });
            for await (const entry of entries) {
              try { users.push(JSON.parse(entry.value)); } catch(e) {}
            }
            send({ users });
          } catch(e) {}
        }, 5000);
        setTimeout(() => { clearInterval(interval); controller.close(); }, 5 * 60 * 1000);
      }
    });
    return new Response(stream, {
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' }
    });
  }
```

#### `GET /debug-schedule` (line 6558)

**Reason classifier flagged it:** no recognised auth pattern in handler body

```ts
  if (url.pathname === '/debug-schedule' && request.method === 'GET') {
    const schedRaw  = await kvGet(['schedule']);
    const lastSent  = await kvGet(['last_sent']);
    const hasItems  = !!(await kvGet(['user_items'])).value;
    const schedule  = schedRaw.value ? JSON.parse(schedRaw.value) : null;
    const now       = new Date();
    let nextSend    = null;
    if (schedule && !lastSent.value) {
      nextSend = `${schedule.startDate}T${schedule.startTime||'09:00'} UK time`;
    } else if (schedule && lastSent.value) {
      nextSend = new Date(new Date(lastSent.value).getTime() + schedule.intervalDays * 86400000).toISOString();
    }
    return json({
      now:      now.toISOString(),
      storage:  'Deno KV (no Drive)',
      schedule: schedule || '✗ missing',
      lastSent: lastSent.value || 'never',
      nextSend,
      kvSnapshot: hasItems ? '✓' : '✗',
    }, corsHeaders);
  }
```

## Full endpoint list

| Method | Path | Line | Auth pattern | Evidence |
|---|---|---:|---|---|
| POST | `/admin/audit-log` | 4918 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/billing/get` | 5237 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/billing/run-migration` | 5313 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/billing/set-grace` | 5283 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/billing/set-grandfather` | 5256 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/crypto-status` | 4946 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/delete-account` | 5014 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/list-accounts` | 4972 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/otp/send` | 4692 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/otp/verify` | 4732 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/r2/backup-now` | 5100 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/r2/list` | 5083 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/r2/prune` | 5115 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/r2/restore` | 5132 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/r2/send-heartbeat` | 5153 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/r2/status` | 5034 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/revoke-all-sessions` | 4822 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/revoke-session` | 4888 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/sessions` | 4858 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/user/backup` | 5171 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/user/list-backups` | 5191 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/admin/user/restore` | 5212 | 🛡️  admin | verifyAdminRequest / adminToken / ADMIN_SECRET |
| POST | `/billing/ack-notifications` | 3126 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/apply-promo` | 3421 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/cancel` | 3449 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/checkout` | 3313 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/pending-notifications` | 3112 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/portal` | 3396 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/resume` | 3471 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/billing/status` | 3097 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/check-now` | 6583 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/data/force-remote/ack` | 5707 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/data/modified` | 5676 | ⚠️  FINDING | partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN |
| POST | `/data/pull` | 5614 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/data/push` | 5577 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| GET | `/debug-kv` | 4334 | ⚠️  FINDING | no recognised auth pattern in handler body |
| GET | `/debug-schedule` | 6558 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/debug-user` | 5562 | ⚠️  FINDING | partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN |
| POST | `/device/clear-all` | 3789 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/device/list` | 3739 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/device/register` | 3721 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/device/remove` | 3775 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/device/seen` | 3757 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/email/verify/check` | 3866 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/email/verify/send` | 3814 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/key/get` | 4407 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/key/passkey-prf-get` | 4480 | 🔐 user | session-token only (passkey-specific feature, no verifier path needed) |
| POST | `/key/passkey-prf-store` | 4457 | 🔐 user | session-token only (passkey-specific feature, no verifier path needed) |
| POST | `/key/recover` | 5332 | 🔑 recovery | recovery token check |
| POST | `/key/store` | 4366 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/key/update-passphrase` | 4500 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/key/update-recovery` | 4528 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/mfa/otp/send` | 6791 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/mfa/otp/verify` | 6850 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/note/body/delete` | 6925 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/note/body/pull` | 6905 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/note/body/push` | 6872 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/note/otp/send` | 6945 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/note/otp/verify` | 6998 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/passkey/auth/begin` | 4122 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/passkey/auth/finish` | 4154 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/passkey/list` | 4290 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/passkey/register/begin` | 4021 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/passkey/register/finish` | 4057 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/passkey/remove` | 4318 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/passkey/verify-session` | 4277 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| ANY | `/ping` | 3090 | 🌍 public | health endpoint (no data returned) |
| GET | `/presence-list` | 6456 | ⚠️  FINDING | no recognised auth pattern in handler body |
| GET | `/presence-stream` | 6468 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/presence-update` | 6446 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/push/cancel` | 3244 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| GET | `/push/config` | 3143 | 🌍 public | explicit "no auth / public" comment near handler |
| POST | `/push/dispatch` | 3261 | ⚙️  dispatch | PUSH_DISPATCH_SECRET bearer |
| POST | `/push/schedule` | 3203 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/push/self-test` | 3279 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/push/subscribe` | 3154 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/push/unsubscribe` | 3184 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/recovery/request` | 3916 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/recovery/reset` | 5376 | 🔑 recovery | recovery token check |
| POST | `/recovery/verify` | 3976 | 🔑 recovery | recovery token check |
| POST | `/referral/code` | 3494 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/referral/list` | 3543 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/referral/validate` | 3580 | 🌍 public | public-by-design (whitelisted in classifier) |
| POST | `/reset-schedule` | 6519 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/send-reminder` | 6543 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/set-schedule` | 6491 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/share/create` | 5728 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/data/modified` | 6307 | ⚠️  FINDING | partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN |
| POST | `/share/data/pull` | 6137 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/data/pull-owner` | 6207 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/data/push` | 6052 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/data/push-guest` | 6090 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/delete` | 6352 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/ecdh-key/get` | 6770 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/ecdh-key/pending-rewraps` | 6741 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/ecdh-key/request-rewrap` | 6717 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/ecdh-key/store` | 6617 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/join` | 5965 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/share/key/get` | 5793 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/key/store` | 5774 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/list` | 5898 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/member/remove` | 6383 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/memberships` | 5934 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/refresh` | 6422 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/send-email` | 6649 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/tips` | 6249 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/share/update` | 6320 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/unsubscribe` | 6530 | ⚠️  FINDING | no recognised auth pattern in handler body |
| POST | `/user/deactivate` | 7023 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/user/delete` | 3648 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/user/delete-confirm-send` | 7085 | 📧 otp | OTP send/verify (auth is the OTP itself) |
| POST | `/user/delete-execute` | 7121 | 🗑️  delete-token | single-use email-delivered deletion token |
| POST | `/user/ecdh-pubkey/get` | 6605 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/user/ecdh-pubkey/store` | 6593 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/user/email-verified` | 3899 | ⚠️  FINDING | partial auth: verifier-only (no session path) — RECURRING AUTH-GAP PATTERN |
| POST | `/user/reactivate` | 7071 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/user/register` | 5409 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/user/share-envelope/delete` | 5876 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/user/share-envelope/get` | 5853 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/user/share-envelope/store` | 5819 | 🔐 user | inline emailHash+verifier|sessionToken KV check |
| POST | `/user/snapshots/extract` | 3700 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/user/snapshots/list` | 3672 | 🔐 user | requireUserAuth / verifyUserAuth helper |
| POST | `/user/verify` | 5493 | 🚪 auth-flow | pre-auth endpoint (auth flow itself); returns challenge/token, never user data |
| POST | `/webhook/stripe` | 3602 | 🪝 webhook | Stripe signature verification |
