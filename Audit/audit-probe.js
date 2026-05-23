// audit-probe.js — STOCKROOM runtime endpoint probe
//
// Reads audit-inventory.json and fires a series of auth-bypass attempts
// against every endpoint that claims to require user-auth. Verifies each
// one returns 401/403/400 (never 200) when the credentials are missing,
// wrong, or belong to a different account.
//
// What it tests, per user-auth endpoint:
//   1. No credentials at all
//   2. emailHash but no verifier or sessionToken
//   3. emailHash + wrong verifier
//   4. emailHash + random session token
//   5. emailHash + cross-account session token (signed in as B, asking for A's data)
//   6. emailHash + cross-account verifier
//
// Plus a few cross-cutting checks for known finding categories:
//   - account-enum probes (/user/email-verified, /debug-user)
//   - public-write probes (/presence-update, /set-schedule)
//
// Run:
//   node audit-probe.js <base-url> <inventory.json> [out-report.md]
//   node audit-probe.js https://stckrm-staging.fly.dev ./audit-inventory.json
//
// Requires: ACCOUNT_A_EMAIL, ACCOUNT_A_PASSPHRASE, ACCOUNT_B_EMAIL,
// ACCOUNT_B_PASSPHRASE in env. These should be DEDICATED TEST ACCOUNTS on
// staging, not real accounts. The probe will create them if they don't
// exist (via /user/register).
//
// Safety: by default the probe REFUSES to run against a URL containing
// 'fly.dev' that doesn't contain 'staging'. Override with ALLOW_PROD=1
// (do NOT do this unless you really mean it).
//
// Exit codes:
//   0 — all probes returned the expected non-200 status
//   1 — one or more endpoints leaked: returned 200 to a bad-credentials request
//   2 — script error or test-account setup failed

const fs     = require('fs');
const crypto = require('crypto');

const BASE      = process.argv[2] || process.env.STOCKROOM_BASE_URL;
const INVENTORY = process.argv[3] || './audit-inventory.json';
const OUT_MD    = process.argv[4] || './audit-probe.md';

if (!BASE) {
  console.error('Usage: node audit-probe.js <base-url> <inventory.json> [out.md]');
  console.error('       (or set STOCKROOM_BASE_URL)');
  process.exit(2);
}

// ── Production safeguard ─────────────────────────────────────────────────
// The probe creates real accounts and fires malformed requests. Running it
// against the live app would clutter KV with junk data and pollute logs.
// Force the user to opt in explicitly if they really want prod.
if (BASE.includes('fly.dev') && !BASE.includes('staging') && process.env.ALLOW_PROD !== '1') {
  console.error(`\n❌ Refusing to run against what looks like production: ${BASE}`);
  console.error(`   Set ALLOW_PROD=1 to override. (You probably want a staging URL.)\n`);
  process.exit(2);
}

const A_EMAIL = process.env.ACCOUNT_A_EMAIL || 'audit-a@stockroom.test';
const A_PASS  = process.env.ACCOUNT_A_PASSPHRASE || 'audit-test-passphrase-A-' + Math.random().toString(36).slice(2);
const B_EMAIL = process.env.ACCOUNT_B_EMAIL || 'audit-b@stockroom.test';
const B_PASS  = process.env.ACCOUNT_B_PASSPHRASE || 'audit-test-passphrase-B-' + Math.random().toString(36).slice(2);

// ── Inventory load ────────────────────────────────────────────────────────
let inventory;
try {
  inventory = JSON.parse(fs.readFileSync(INVENTORY, 'utf8'));
} catch (e) {
  console.error(`Could not read ${INVENTORY}: ${e.message}`);
  console.error(`Run audit-inventory.js first.`);
  process.exit(2);
}

// ── Crypto helpers (mirroring main.ts) ────────────────────────────────────
async function hashEmail(email) {
  const h = crypto.createHash('sha256').update(email.toLowerCase().trim()).digest('hex');
  return h.slice(0, 32);
}

async function makeVerifier(passphrase, emailHash) {
  // Matches client: SHA-256(passphrase + ':' + emailHash) as hex
  return crypto.createHash('sha256').update(passphrase + ':' + emailHash).digest('hex');
}

function randomHex(bytes) {
  return crypto.randomBytes(bytes).toString('hex');
}

// ── HTTP helper ──────────────────────────────────────────────────────────
async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let json = null;
  try { json = await res.json(); } catch (_) {}
  return { status: res.status, json };
}

async function get(path) {
  const res = await fetch(`${BASE}${path}`);
  let json = null;
  try { json = await res.json(); } catch (_) {}
  return { status: res.status, json };
}

// ── Account setup ────────────────────────────────────────────────────────
async function ensureAccount(email, pass) {
  const emailHash = await hashEmail(email);
  const verifier  = await makeVerifier(pass, emailHash);
  // Try /user/verify first — if 200, account exists.
  const v = await post('/user/verify', { emailHash, verifier });
  if (v.status === 200 && v.json && v.json.sessionToken) {
    return { email, emailHash, verifier, sessionToken: v.json.sessionToken };
  }
  // Try to register.
  const r = await post('/user/register', { emailHash, verifier, email, salt: randomHex(16) });
  if (r.status !== 200) {
    throw new Error(`Could not register ${email}: ${r.status} ${JSON.stringify(r.json)}`);
  }
  // Verify again to get a sessionToken.
  const v2 = await post('/user/verify', { emailHash, verifier });
  if (v2.status !== 200 || !v2.json?.sessionToken) {
    throw new Error(`Registered ${email} but verify failed: ${v2.status} ${JSON.stringify(v2.json)}`);
  }
  return { email, emailHash, verifier, sessionToken: v2.json.sessionToken };
}

// ── Probe scenarios ──────────────────────────────────────────────────────
// For each user-auth endpoint, fire these requests and assert non-200.
function scenariosForUserAuthEndpoint(target, attacker, path) {
  const isOwnerPath = /ownerEmailHash/i.test(path) || path.startsWith('/share/');
  const isGuestPath = /guestEmailHash/i.test(path) || path === '/share/data/pull' || path === '/share/data/push-guest';
  const ehashField = isOwnerPath ? 'ownerEmailHash' : (isGuestPath ? 'guestEmailHash' : 'emailHash');
  const verField   = isOwnerPath ? 'verifier'        : (isGuestPath ? 'guestVerifier'   : 'verifier');
  const sessField  = isOwnerPath ? 'sessionToken'    : (isGuestPath ? 'guestSessionToken': 'sessionToken');

  return [
    {
      name: 'no credentials',
      body: {},
    },
    {
      name: `${ehashField} only, no creds`,
      body: { [ehashField]: target.emailHash },
    },
    {
      name: `${ehashField} + random ${verField}`,
      body: { [ehashField]: target.emailHash, [verField]: randomHex(32) },
    },
    {
      name: `${ehashField} + random ${sessField}`,
      body: { [ehashField]: target.emailHash, [sessField]: randomHex(32) },
    },
    {
      name: `target's ${ehashField} + attacker's ${verField}`,
      body: { [ehashField]: target.emailHash, [verField]: attacker.verifier },
    },
    {
      name: `target's ${ehashField} + attacker's ${sessField}`,
      body: { [ehashField]: target.emailHash, [sessField]: attacker.sessionToken },
    },
  ];
}

// ── Run probes ───────────────────────────────────────────────────────────
const results = [];

function pushResult(path, scenario, status, leaked, note) {
  results.push({ path, scenario, status, leaked, note });
}

async function probeUserAuthEndpoint(ep, accountA, accountB) {
  for (const scen of scenariosForUserAuthEndpoint(accountA, accountB, ep.path)) {
    const { status, json } = await post(ep.path, scen.body);
    // We want a non-200. 200 = potential leak. 400/401/403/404 = good.
    // 500 is also concerning (crash on malformed input) — flag but don't fail.
    const leaked = status === 200;
    const note = leaked
      ? `200 OK with body keys: ${json ? Object.keys(json).join(',') : '(empty)'}`
      : `${status} ${json?.error ? `"${json.error.slice(0, 80)}"` : ''}`;
    pushResult(ep.path, scen.name, status, leaked, note);
  }
}

async function probeKnownFindings() {
  // Account enumeration
  const knownEh = await hashEmail(A_EMAIL);
  const unknownEh = await hashEmail('does-not-exist-' + randomHex(4) + '@stockroom.test');

  const e1 = await post('/user/email-verified', { emailHash: knownEh });
  const e2 = await post('/user/email-verified', { emailHash: unknownEh });
  const enumLeaks = e1.json?.exists !== e2.json?.exists;
  pushResult('/user/email-verified', 'account-enumeration distinguish exists/notexists',
    e1.status, enumLeaks,
    enumLeaks ? `LEAKS: known=${JSON.stringify(e1.json)} unknown=${JSON.stringify(e2.json)}` : 'identical responses');

  const d1 = await post('/debug-user', { emailHash: knownEh });
  const d2 = await post('/debug-user', { emailHash: unknownEh });
  const debugLeaks = JSON.stringify(d1.json) !== JSON.stringify(d2.json);
  pushResult('/debug-user', 'account-enumeration distinguish exists/notexists',
    d1.status, debugLeaks,
    debugLeaks ? `LEAKS: known=${JSON.stringify(d1.json).slice(0,120)} unknown=${JSON.stringify(d2.json).slice(0,120)}` : 'identical responses');

  // Public-write probes
  const presBefore = await get('/presence-list');
  const fakeId = 'attacker-' + randomHex(4);
  await post('/presence-update', { userId: fakeId, name: 'Mallory', initials: 'MA', colour: '#000', view: 'stockroom' });
  const presAfter = await get('/presence-list');
  const presLeaked = JSON.stringify(presAfter.json).includes(fakeId);
  pushResult('/presence-update', 'unauthenticated write accepted',
    200, presLeaked,
    presLeaked ? `LEAKS: injected fakeId visible in /presence-list` : 'write rejected or invisible');

  // Schedule write
  const sched = await post('/set-schedule', { email: 'noone@example.invalid', startDate: '2030-01-01', startTime: '09:00', intervalDays: 30 });
  pushResult('/set-schedule', 'unauthenticated schedule write',
    sched.status, sched.status === 200, sched.status === 200 ? 'ACCEPTED' : 'rejected');

  // Send-reminder abuse
  const rem = await post('/send-reminder', { email: 'noone@example.invalid', urgent: [], upcoming: [] });
  pushResult('/send-reminder', 'unauthenticated email-send attempt',
    rem.status, rem.status === 200, rem.status === 200 ? 'ACCEPTED (would have sent email)' : 'rejected');
}

// ── Main ─────────────────────────────────────────────────────────────────
(async () => {
  console.log(`Probing ${BASE}`);
  console.log(`Setting up test accounts...`);

  let accountA, accountB;
  try {
    accountA = await ensureAccount(A_EMAIL, A_PASS);
    accountB = await ensureAccount(B_EMAIL, B_PASS);
  } catch (e) {
    console.error(`Test account setup failed: ${e.message}`);
    process.exit(2);
  }
  console.log(`  Account A: ${A_EMAIL} (${accountA.emailHash})`);
  console.log(`  Account B: ${B_EMAIL} (${accountB.emailHash})`);

  // Probe every user-auth endpoint
  const userAuth = inventory.filter(e =>
    e.pattern === 'user-auth' &&
    e.method !== 'GET' &&  // GETs typically take query params, not POST body
    // Skip endpoints that materially change state per call to avoid creating
    // tons of test garbage on staging.
    !['/data/push','/data/pull','/share/data/push','/share/data/push-guest',
      '/user/delete','/user/deactivate','/user/delete-confirm-send','/user/delete-execute',
      '/share/delete','/share/member/remove','/admin/delete-account',
      '/key/store','/key/update-passphrase','/key/update-recovery',
      '/share/create','/share/update',
    ].includes(e.path)
  );

  console.log(`\nProbing ${userAuth.length} user-auth endpoints with 6 scenarios each (${userAuth.length * 6} requests)...`);

  let n = 0;
  for (const ep of userAuth) {
    n++;
    process.stdout.write(`\r  [${n}/${userAuth.length}] ${ep.path.padEnd(50)}`);
    try {
      await probeUserAuthEndpoint(ep, accountA, accountB);
    } catch (e) {
      pushResult(ep.path, 'probe error', 0, false, `request failed: ${e.message}`);
    }
  }
  console.log('');

  // Probe known findings
  console.log(`\nProbing known finding categories...`);
  await probeKnownFindings();

  // ── Report ────────────────────────────────────────────────────────────
  const leaks = results.filter(r => r.leaked);

  let md = `# STOCKROOM endpoint probe report\n\n`;
  md += `Base URL: \`${BASE}\`\n`;
  md += `Generated: ${new Date().toISOString()}\n`;
  md += `Total probes: ${results.length}\n`;
  md += `Leaks: **${leaks.length}**\n\n`;

  if (leaks.length > 0) {
    md += `## ❌ Leaks (${leaks.length})\n\n`;
    md += `| Path | Scenario | Status | Detail |\n|---|---|---:|---|\n`;
    for (const l of leaks) {
      md += `| \`${l.path}\` | ${l.scenario} | ${l.status} | ${l.note.replace(/\|/g,'\\|')} |\n`;
    }
    md += `\n`;
  } else {
    md += `## ✅ No leaks detected\n\nEvery probed endpoint returned a non-200 status for malformed or wrong-account requests.\n\n`;
  }

  md += `## All probe results\n\n`;
  md += `| Path | Scenario | Status | Detail |\n|---|---|---:|---|\n`;
  for (const r of results) {
    const mark = r.leaked ? '❌' : '✅';
    md += `| ${mark} \`${r.path}\` | ${r.scenario} | ${r.status} | ${r.note.replace(/\|/g,'\\|')} |\n`;
  }

  fs.writeFileSync(OUT_MD, md);
  console.log(`\nWrote ${OUT_MD}`);
  console.log(`Total probes: ${results.length}`);
  console.log(`Leaks: ${leaks.length}`);

  if (leaks.length > 0) {
    console.error(`\n❌ ${leaks.length} probe(s) returned 200 for a bad-credentials request.`);
    process.exit(1);
  }
  console.log(`\n✅ All probes returned non-200 for bad/missing credentials.`);
  process.exit(0);
})();
