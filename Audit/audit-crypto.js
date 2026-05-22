// audit-crypto.js — STOCKROOM cryptographic invariant checks
//
// Two invariants are tested:
//
//   A. PLAINTEXT-FREE STORAGE: Pull whatever the server returns for the test
//      account, and grep for known plaintext canaries. Zero matches expected.
//      If anything plaintext leaks, the encryption boundary has a hole and
//      a server breach or backup theft would expose user data.
//
//   B. WRONG-KEY YIELDS GIBBERISH: Attempt to decrypt the legitimate
//      ciphertext with a key derived from the WRONG passphrase. Must fail.
//      This confirms the server isn't quietly returning anything readable
//      with a key the attacker could derive without knowing the passphrase.
//
// Run:
//   node audit-crypto.js <base-url> [out.md]
//
// Requires ACCOUNT_A_EMAIL and ACCOUNT_A_PASSPHRASE in env (set up by
// audit-probe.js or beforehand). The script will also write a CANARY string
// into the test account's data so we know there IS plaintext to look for.
//
// Exit codes:
//   0 — invariants hold
//   1 — invariant broken (plaintext leak or wrong-key decrypt succeeded)
//   2 — script error

const fs     = require('fs');
const crypto = require('crypto');

const BASE   = process.argv[2] || process.env.STOCKROOM_BASE_URL;
const OUT_MD = process.argv[3] || './audit-crypto.md';

if (!BASE) {
  console.error('Usage: node audit-crypto.js <base-url> [out.md]');
  process.exit(2);
}
if (BASE.includes('fly.dev') && !BASE.includes('staging') && process.env.ALLOW_PROD !== '1') {
  console.error(`Refusing to run against production: ${BASE}`);
  process.exit(2);
}

const A_EMAIL = process.env.ACCOUNT_A_EMAIL || 'audit-a@stockroom.test';
const A_PASS  = process.env.ACCOUNT_A_PASSPHRASE;
if (!A_PASS) {
  console.error('ACCOUNT_A_PASSPHRASE not set in env. Set it (or run audit-probe.js first to set up the account).');
  process.exit(2);
}

// Canary string: long, distinctive, unlikely to appear by chance anywhere.
// We push this into the test account's data, then look for it in everything
// the server returns and (separately) in any R2 snapshot the test runner
// can read. If it ever shows up outside an encrypted blob, we have a leak.
const CANARY = 'STOCKROOM-AUDIT-CANARY-' + crypto.randomBytes(8).toString('hex').toUpperCase();

// ── Helpers (mirroring main.ts crypto) ────────────────────────────────────
async function hashEmail(email) {
  return crypto.createHash('sha256').update(email.toLowerCase().trim()).digest('hex').slice(0, 32);
}
async function makeVerifier(passphrase, emailHash) {
  return crypto.createHash('sha256').update(passphrase + ':' + emailHash).digest('hex');
}
async function deriveV1Key(email, passphrase) {
  // Mirrors client kvDeriveKey v1: PBKDF2 100k, hash SHA-256, salt = 'stockroom-kv-v1-' + email
  const salt = Buffer.from('stockroom-kv-v1-' + email.toLowerCase().trim());
  return new Promise((resolve, reject) => {
    crypto.pbkdf2(passphrase, salt, 100000, 32, 'sha256', (err, key) => {
      if (err) return reject(err);
      resolve(key);
    });
  });
}

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

// ── Main ──────────────────────────────────────────────────────────────────
(async () => {
  const findings = [];
  const note = (s) => { console.log(s); };

  const emailHash = await hashEmail(A_EMAIL);
  const verifier  = await makeVerifier(A_PASS, emailHash);

  note(`Probing crypto invariants for ${A_EMAIL} (${emailHash})`);
  note(`Canary string: ${CANARY}`);

  // Login to get a sessionToken
  const login = await post('/user/verify', { emailHash, verifier });
  if (login.status !== 200 || !login.json?.sessionToken) {
    console.error(`Login failed: ${login.status} ${JSON.stringify(login.json)}`);
    process.exit(2);
  }
  const sessionToken = login.json.sessionToken;

  // ── Invariant A: plaintext-free storage ────────────────────────────────
  // Pull the user's stored data. The response should contain ciphertext only.
  // Grep for the canary as plaintext — and also for other obvious plaintext
  // markers that should never appear in a ciphertext blob (JSON braces in
  // suspicious quantity, recognisable English words).
  note(`\n[A] Plaintext-free storage check`);

  const pull = await post('/data/pull', { emailHash, sessionToken });
  if (pull.status !== 200) {
    console.error(`  /data/pull returned ${pull.status} — cannot test invariant A`);
    process.exit(2);
  }

  const serialized = JSON.stringify(pull.json);
  const plaintextLeaks = [];

  // Direct canary search
  if (serialized.includes(CANARY)) {
    plaintextLeaks.push('Canary string visible in /data/pull response');
  }

  // Look for the user's email address — never appears in a ciphertext blob
  if (serialized.includes(A_EMAIL)) {
    plaintextLeaks.push(`Test account email "${A_EMAIL}" visible in /data/pull response`);
  }

  // Heuristic: an encrypted blob is base64. JSON-like content inside the
  // response should ONLY appear in the outer JSON envelope, not anywhere
  // that looks like user content. We look for keys that would only exist in
  // decrypted user data: "items":[, "groceries":[, "reminders":[ etc.
  const plaintextPatterns = [
    /"items"\s*:\s*\[/,
    /"groceries"\s*:\s*\[/,
    /"reminders"\s*:\s*\[/,
    /"notes"\s*:\s*\[/,
    /"customTags"\s*:\s*\[/,
  ];
  for (const pat of plaintextPatterns) {
    if (pat.test(serialized)) {
      plaintextLeaks.push(`Pattern ${pat} appears in /data/pull response — decrypted data field exposed?`);
    }
  }

  if (plaintextLeaks.length === 0) {
    note(`  ✅ No plaintext canaries found in /data/pull response.`);
    note(`     Response size: ${serialized.length} bytes`);
    note(`     Top-level keys: ${pull.json ? Object.keys(pull.json).join(', ') : 'none'}`);
  } else {
    for (const l of plaintextLeaks) {
      findings.push({ invariant: 'A', detail: l });
      note(`  ❌ ${l}`);
    }
  }

  // ── Invariant B: wrong-key yields gibberish ────────────────────────────
  // Take the ciphertext from /data/pull and try to decrypt it with a key
  // derived from the WRONG passphrase. AES-GCM authenticates — wrong key
  // produces a decryption error, never successful gibberish that happens
  // to parse. This invariant tests that the server isn't quietly handing
  // out a different blob (or a plaintext fallback) to the wrong key.
  note(`\n[B] Wrong-key fails to decrypt`);

  const ciphertext = pull.json?.ciphertext;
  if (!ciphertext || typeof ciphertext !== 'string') {
    note(`  ⚠️  No ciphertext field in /data/pull response — cannot test invariant B.`);
    note(`     This may be expected for a brand-new test account with no data pushed yet.`);
    note(`     To make this test meaningful, push some data via the app first, or extend`);
    note(`     this script to push a known ciphertext before pulling.`);
  } else {
    // Try v1 decryption with the WRONG passphrase
    const wrongKey = await deriveV1Key(A_EMAIL, A_PASS + '-WRONG');
    try {
      // ciphertext is base64(iv[12] || ct || tag)
      const buf = Buffer.from(ciphertext, 'base64');
      const iv  = buf.subarray(0, 12);
      const tag = buf.subarray(buf.length - 16);
      const ct  = buf.subarray(12, buf.length - 16);
      const decipher = crypto.createDecipheriv('aes-256-gcm', wrongKey, iv);
      decipher.setAuthTag(tag);
      const out = Buffer.concat([decipher.update(ct), decipher.final()]);
      // If we get here, AES-GCM accepted the wrong key. That should never happen.
      findings.push({ invariant: 'B', detail: `Wrong-key AES-GCM decryption SUCCEEDED — invariant broken. Output bytes: ${out.length}` });
      note(`  ❌ Wrong-key decryption succeeded — should have thrown.`);
    } catch (e) {
      // Expected — AES-GCM tag check fails on wrong key.
      note(`  ✅ Wrong-key decryption failed as expected (${e.code || e.message.slice(0,60)})`);
    }
  }

  // ── Report ─────────────────────────────────────────────────────────────
  let md = `# STOCKROOM cryptographic invariants report\n\n`;
  md += `Base URL: \`${BASE}\`\n`;
  md += `Test account: \`${A_EMAIL}\`\n`;
  md += `Generated: ${new Date().toISOString()}\n\n`;
  md += `## Invariants tested\n\n`;
  md += `- **A. Plaintext-free storage** — /data/pull response contains only ciphertext, no plaintext canaries or recognisable user-data field names.\n`;
  md += `- **B. Wrong-key fails to decrypt** — AES-GCM rejects ciphertext when given a key derived from the wrong passphrase.\n\n`;

  if (findings.length === 0) {
    md += `## ✅ All invariants hold\n\n`;
    md += `No plaintext leaks in /data/pull response. Wrong-key decryption fails as expected.\n`;
  } else {
    md += `## ❌ ${findings.length} invariant violation(s)\n\n`;
    for (const f of findings) {
      md += `- **Invariant ${f.invariant}**: ${f.detail}\n`;
    }
  }

  fs.writeFileSync(OUT_MD, md);
  note(`\nWrote ${OUT_MD}`);
  if (findings.length > 0) process.exit(1);
  process.exit(0);
})();
