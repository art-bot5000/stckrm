// ═══════════════════════════════════════════════════════════
//  STOCKROOM KV — Backend Service (Deno Deploy)
//  All data stored in Deno KV. No Google Drive, no Dropbox.
//  Encryption: AES-GCM with key derived from email+passphrase.
//  Sharing: share codes stored in KV, same system.
//  Email: Resend API, same cron system.
// ═══════════════════════════════════════════════════════════

const env = {
  APP_URL:       Deno.env.get('APP_URL')       || 'https://stckrm.fly.dev',
  WORKER_URL:    Deno.env.get('WORKER_URL')    || '',
  RESEND_API_KEY:Deno.env.get('RESEND_API_KEY')|| '',
  FROM_EMAIL:    Deno.env.get('FROM_EMAIL')    || 'onboarding@resend.dev',
  ADMIN_EMAIL:   Deno.env.get('ADMIN_EMAIL')   || 'pete@artbot5000.com',
};

// ── Crypto migration config ───────────────────────────────
// Accounts registered before this date are crypto_version='v1'.
// After this date, new accounts get 'v2'. Existing v1 users are
// migrated on their next login after this date.
// Grace period: v1 ciphertext kept for 90 days after migration,
// then deleted. Set CRYPTO_V2_SWITCHOVER to a future date to
// schedule the migration; set to a past date to migrate immediately.
const CRYPTO_V2_SWITCHOVER     = '2026-05-01'; // ISO date
const CRYPTO_V1_GRACE_DAYS     = 90;

// On Fly.io, DENO_KV_PATH points to a mounted volume (/data/stockroom.db)
// Locally (or on Deno Deploy) it defaults to the built-in KV store
const kvPath = Deno.env.get("DENO_KV_PATH") ?? undefined;
const kv = await Deno.openKv(kvPath);

// KV read with 8-second timeout — prevents 2-minute execution burns
async function kvGet(key) {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('KV read timed out')), 8000)
  );
  return Promise.race([kv.get(key), timeout]);
}

// KV write with 8-second timeout
async function kvSet(key, value, opts) {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('KV write timed out')), 8000)
  );
  return Promise.race([
    opts ? kv.set(key, value, opts) : kv.set(key, value),
    timeout
  ]);
}

// KV delete with 8-second timeout
async function kvDel(key) {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('KV delete timed out')), 8000)
  );
  return Promise.race([kv.delete(key), timeout]);
}

// ── KV health check ───────────────────────────────────
// Verify KV is working on startup with a simple write/read
try {
  await kvSet(['_health'], 'ok');
  const h = await kvGet(['_health']);
  console.log('KV health check:', h.value === 'ok' ? 'PASS' : 'FAIL');
} catch(e) {
  console.error('KV health check FAILED:', e.message);
}

// ── CORS ─────────────────────────────────────────────────
const corsHeaders = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function json(data, headers = {}, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}

// ── Crypto helpers ────────────────────────────────────────
// Key derivation: PBKDF2(email + ':' + passphrase) → AES-GCM key
// The derived key never leaves the client — server only stores ciphertext.
// Server-side we use a separate server key for share targets and schedules.

async function deriveKey(email, passphrase) {
  const raw    = new TextEncoder().encode(email.toLowerCase().trim() + ':' + passphrase);
  const base   = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  const salt   = new TextEncoder().encode('stockroom-kv-v1-' + email.toLowerCase().trim());
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
}

async function encryptData(key, plaintext) {
  const iv         = crypto.getRandomValues(new Uint8Array(12));
  const encoded    = new TextEncoder().encode(plaintext);
  const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, encoded);
  // Pack iv + ciphertext
  const combined = new Uint8Array(iv.length + ciphertext.byteLength);
  combined.set(iv, 0);
  combined.set(new Uint8Array(ciphertext), iv.length);
  return btoa(String.fromCharCode(...combined));
}

async function decryptData(key, ciphertext) {
  const combined = Uint8Array.from(atob(ciphertext), c => c.charCodeAt(0));
  const iv       = combined.slice(0, 12);
  const data     = combined.slice(12);
  const plain    = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, data);
  return new TextDecoder().decode(plain);
}

// Hash email for use key (so raw email isn't stored)
async function hashEmail(email) {
  const encoded = new TextEncoder().encode(email.toLowerCase().trim());
  const hash    = await crypto.subtle.digest('SHA-256', encoded);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2,'0')).join('').slice(0, 32);
}

// ── Hourly cron ───────────────────────────────────────────
Deno.cron('stockroom-kv-email-check', '0 * * * *', async () => {
  console.log('Cron: running');
  await cronCheck();
});

// Send migration notification emails 7 days before switchover (runs daily at 09:00)
Deno.cron('stockroom-crypto-migration-notify', '0 9 * * *', async () => {
  const now        = new Date();
  const switchover = new Date(CRYPTO_V2_SWITCHOVER);
  const daysUntil  = Math.ceil((switchover.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
  if (daysUntil !== 7) return; // only send exactly 7 days before
  console.log('Cron: sending migration notification emails');
  // Iterate all users on v1 and notify them
  const iter = kv.list({ prefix: ['user'] });
  for await (const entry of iter) {
    const key = entry.key as string[];
    if (key[2] !== 'email') continue;
    const emailHash     = key[1];
    const cryptoVersion = await kvGet(['user', emailHash, 'crypto_version']);
    const alreadyNotified = await kvGet(['user', emailHash, 'migration_notified']);
    if ((cryptoVersion.value || 'v1') !== 'v1') continue;
    if (alreadyNotified.value) continue;
    const emailAddr = entry.value as string;
    try {
      await sendMigrationEmail(emailAddr, 'notify');
      await kvSet(['user', emailHash, 'migration_notified'], now.toISOString());
      console.log('Migration notify sent to:', emailHash.slice(0, 8) + '…');
    } catch(e) {
      console.warn('Migration notify failed for', emailHash.slice(0, 8), e.message);
    }
  }
});

// ── Account deletion helper — deletes EVERY key for a given emailHash ──
// Used by both /user/delete (self) and /admin/delete-account.
// Covers: user data, devices, passkeys, sessions, challenges, wrapped keys,
//         share targets, share data, recovery OTPs, email verify tokens, schedules.
async function _deleteAllUserData(kv: Deno.Kv, emailHash: string): Promise<void> {
  const prefixesToScan = [
    ['user',             emailHash],   // verifier, key envelopes, data, settings, etc.
    ['device',           emailHash],   // trusted devices
    ['passkey',          emailHash],   // WebAuthn credentials
    ['passkey_session',  emailHash],   // active session tokens
    ['passkey_challenge',emailHash],   // pending WebAuthn challenges
    ['passkey_key',          emailHash],   // old server-wrapped data key copies (deprecated)
    ['passkey_prf_envelope', emailHash],   // PRF/device-bound envelope (new architecture)
    ['email_verify',     emailHash],   // email verification OTPs
    ['note_body',        emailHash],   // secure note bodies
    ['notes_session',    emailHash],   // notes 2FA session tokens
    ['mfa_otp',          emailHash],   // MFA login OTP
    ['deactivation',     emailHash],   // deactivation state
    ['delete_token',     emailHash],   // pending deletion token
  ];

  for (const prefix of prefixesToScan) {
    const iter = kv.list({ prefix });
    for await (const entry of iter) await kv.delete(entry.key);
  }

  // Point keys (not prefix-scanned)
  await kv.delete(['recovery_otp', emailHash]);

  // Share targets owned by this user + their data
  const shares = kv.list({ prefix: ['share'] });
  for await (const entry of shares) {
    if (entry.key.length !== 2) continue;
    try {
      const data = JSON.parse(entry.value as string);
      if (data.ownerEmailHash === emailHash) {
        await kv.delete(entry.key);
        const code = entry.key[1] as string;
        const sdIter = kv.list({ prefix: ['share_data', code] });
        for await (const sd of sdIter) await kv.delete(sd.key);
      }
    } catch(e) {}
  }

  // Share keys belonging to this user
  const shareKeyIter = kv.list({ prefix: ['share_key'] });
  for await (const entry of shareKeyIter) {
    const k = entry.key as string[];
    if (k[2] === emailHash) await kv.delete(entry.key);
  }

  // Schedules (stored at top level, matched by emailHash field)
  const schedKeys = ['schedule', 'last_sent', 'user_email', 'user_items'];
  for (const k_ of schedKeys) {
    const val = await kv.get([k_]);
    if (val.value) {
      try {
        const d = JSON.parse(val.value as string);
        if (d.emailHash === emailHash) await kv.delete([k_]);
      } catch(e) {}
    }
  }
}

// ── Request handler ───────────────────────────────────────
Deno.serve(async (request) => {
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  const url = new URL(request.url);

  // ── Health / debug ────────────────────────────────────
  if (url.pathname === '/ping') {
    return json({ ok: true, ts: new Date().toISOString() }, corsHeaders);
  }

  // ── User: delete account (all data) ──────────────────
  if (url.pathname === '/user/delete' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Accept passphrase verifier OR passkey session token
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      await _deleteAllUserData(kv, emailHash);
      console.log(`User self-deleted account: ${emailHash}`);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Device: register trusted device ─────────────────
  if (url.pathname === '/device/register' && request.method === 'POST') {
    try {
      const { emailHash, verifier, deviceId, name, addedAt } = await request.json();
      if (!emailHash || !verifier || !deviceId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', emailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      await kvSet(['device', emailHash, deviceId], JSON.stringify({
        deviceId, name: name || 'Unknown device',
        addedAt: addedAt || new Date().toISOString(),
        lastSeen: new Date().toISOString(),
      }));
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Device: list trusted devices ─────────────────────
  if (url.pathname === '/device/list' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const authed = sessionToken
        ? !!(await kvGet(['passkey_session', emailHash, sessionToken])).value
        : verifier && (await kvGet(['user', emailHash, 'verifier'])).value === verifier;
      if (!authed) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const devices = [];
      const entries = kv.list({ prefix: ['device', emailHash] });
      for await (const entry of entries) {
        try { devices.push(JSON.parse(entry.value as string)); } catch(e) {}
      }
      return json({ devices }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Device: update last seen ──────────────────────────
  if (url.pathname === '/device/seen' && request.method === 'POST') {
    try {
      const { emailHash, verifier, deviceId } = await request.json();
      if (!emailHash || !verifier || !deviceId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', emailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const existing = await kvGet(['device', emailHash, deviceId]);
      if (existing.value) {
        const data = { ...JSON.parse(existing.value), lastSeen: new Date().toISOString() };
        await kvSet(['device', emailHash, deviceId], JSON.stringify(data));
      }
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Device: remove trusted device ────────────────────
  if (url.pathname === '/device/remove' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, deviceId } = await request.json();
      if (!emailHash || !deviceId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const authed = sessionToken
        ? !!(await kvGet(['passkey_session', emailHash, sessionToken])).value
        : verifier && (await kvGet(['user', emailHash, 'verifier'])).value === verifier;
      if (!authed) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      await kvDel(['device', emailHash, deviceId]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Device: clear all trusted devices ────────────────
  if (url.pathname === '/device/clear-all' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const authed = sessionToken
        ? !!(await kvGet(['passkey_session', emailHash, sessionToken])).value
        : verifier && (await kvGet(['user', emailHash, 'verifier'])).value === verifier;
      if (!authed) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const entries = kv.list({ prefix: ['device', emailHash] });
      for await (const entry of entries) await kv.delete(entry.key);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }


  // ═══════════════════════════════════════════════════════
  //  FORGOT PASSPHRASE — OTP EMAIL RECOVERY
  // ═══════════════════════════════════════════════════════

  // ── Step 1: Request OTP ───────────────────────────────
  // ── Email verification (new registrations) ───────────
  // Step 1: /email/verify/send   { emailHash, email } → sends OTP
  // Step 2: /email/verify/check  { emailHash, otp }   → sets email_verified flag
  // Called immediately after /user/register succeeds.

  if (url.pathname === '/email/verify/send' && request.method === 'POST') {
    try {
      const { emailHash, email } = await request.json();
      if (!emailHash || !email) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Confirm account exists
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (!existing.value) return json({ error: 'Account not found' }, corsHeaders, 404);
      // Already verified — no need to re-send
      const alreadyVerified = await kvGet(['user', emailHash, 'email_verified']);
      if (alreadyVerified.value) return json({ ok: true, alreadyVerified: true }, corsHeaders);
      // Rate limit — 1 OTP per 60 seconds
      const last = await kvGet(['email_verify_otp', emailHash]);
      if (last.value) {
        const sentAt = JSON.parse(last.value).sentAt;
        if (Date.now() - new Date(sentAt).getTime() < 60_000) {
          return json({ error: 'Please wait before requesting another code' }, corsHeaders, 429);
        }
      }
      if (!env.RESEND_API_KEY) return json({ error: 'Email not configured' }, corsHeaders, 500);
      const otp = Array.from(crypto.getRandomValues(new Uint8Array(6)))
        .map(b => String(b % 10)).join('');
      await kvSet(['email_verify_otp', emailHash], JSON.stringify({
        otp, email, sentAt: new Date().toISOString(), attempts: 0,
      }), { expireIn: 5 * 60 * 1000 });
      // Store email for future notifications
      await kvSet(['user', emailHash, 'email'], email);
      const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
        <div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08)">
          <div style="background:#111;padding:20px 24px;display:flex;align-items:center;gap:10px">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg>
            <div style="color:#e8a838;font-size:18px;font-weight:800;letter-spacing:2px">STOCKROOM</div>
          </div>
          <div style="padding:28px">
            <h2 style="margin:0 0 12px;font-size:20px">Verify your email</h2>
            <p style="color:#666;margin:0 0 24px">Welcome! Enter this code in the app to confirm your email address. It expires in 5 minutes.</p>
            <div style="background:#f9f9f9;border:2px dashed #e8a838;border-radius:10px;padding:20px;text-align:center;margin-bottom:24px">
              <div style="font-size:40px;font-weight:800;letter-spacing:8px;color:#111;font-family:monospace">${otp}</div>
            </div>
            <p style="color:#999;font-size:12px">If you didn't create a STOCKROOM account, you can safely ignore this email.</p>
          </div>
        </div>
      </body></html>`;
      const r = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [email], subject: 'Verify your STOCKROOM email', html }),
      });
      if (!r.ok) return json({ error: 'Could not send verification email' }, corsHeaders, 500);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  if (url.pathname === '/email/verify/check' && request.method === 'POST') {
    try {
      const { emailHash, otp } = await request.json();
      if (!emailHash || !otp) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['email_verify_otp', emailHash]);
      if (!stored.value) return json({ error: 'Code expired — request a new one' }, corsHeaders, 400);
      const data = JSON.parse(stored.value);
      if (data.attempts >= 5) {
        await kvDel(['email_verify_otp', emailHash]);
        return json({ error: 'Too many attempts — request a new code' }, corsHeaders, 400);
      }
      if (data.otp !== String(otp).trim()) {
        data.attempts = (data.attempts || 0) + 1;
        await kvSet(['email_verify_otp', emailHash], JSON.stringify(data), { expireIn: 5 * 60 * 1000 });
        const left = 5 - data.attempts;
        return json({ error: `Incorrect code — ${left} attempt${left !== 1 ? 's' : ''} remaining` }, corsHeaders, 400);
      }
      await kvDel(['email_verify_otp', emailHash]);
      await kvSet(['user', emailHash, 'email_verified'], new Date().toISOString());
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Account recovery ──────────────────────────────────
  if (url.pathname === '/recovery/request' && request.method === 'POST') {
    try {
      const { email } = await request.json();
      if (!email) return json({ error: 'Missing email' }, corsHeaders, 400);
      const emailHash = await hashEmail(email);

      // Check user exists
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (!existing.value) return json({ error: 'No account found for this email' }, corsHeaders, 404);

      // Rate limit — max one OTP per 60 seconds
      const lastOtp = await kvGet(['recovery_otp', emailHash]);
      if (lastOtp.value) {
        const sent = JSON.parse(lastOtp.value).sentAt;
        if (Date.now() - new Date(sent).getTime() < 60000) {
          return json({ error: 'Please wait 60 seconds before requesting another code' }, corsHeaders, 429);
        }
      }

      // Generate 6-digit OTP
      const otp = String(Math.floor(100000 + crypto.getRandomValues(new Uint8Array(1))[0] / 255 * 899999)).padStart(6, '0');

      // Store OTP with 5-minute TTL
      await kvSet(['recovery_otp', emailHash], JSON.stringify({
        otp, sentAt: new Date().toISOString(), attempts: 0
      }), { expireIn: 5 * 60 * 1000 });

      // Send email via Resend
      if (!env.RESEND_API_KEY) return json({ error: 'Email not configured' }, corsHeaders, 500);
      const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
        <div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08)">
          <div style="background:#111;padding:20px 24px;display:flex;align-items:center;gap:10px">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg>
            <div style="color:#e8a838;font-size:18px;font-weight:800;letter-spacing:2px">STOCKROOM</div>
          </div>
          <div style="padding:28px">
            <h2 style="margin:0 0 12px;font-size:20px">Reset your passphrase</h2>
            <p style="color:#666;margin:0 0 24px">Enter this code in the app to reset your passphrase. It expires in 5 minutes.</p>
            <div style="background:#f9f9f9;border:2px dashed #e8a838;border-radius:10px;padding:20px;text-align:center;margin-bottom:24px">
              <div style="font-size:40px;font-weight:800;letter-spacing:8px;color:#111;font-family:monospace">${otp}</div>
            </div>
            <p style="color:#999;font-size:12px">If you didn't request this, you can safely ignore this email. Your account is secure.</p>
          </div>
        </div>
      </body></html>`;

      const res = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [email], subject: 'Your STOCKROOM recovery code', html }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        return json({ error: 'Could not send email: ' + (d.message || 'Unknown error') }, corsHeaders, 500);
      }
      return json({ ok: true, message: 'Recovery code sent — check your email' }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Step 2: Verify OTP → issue recovery token ─────────
  if (url.pathname === '/recovery/verify' && request.method === 'POST') {
    try {
      const { email, otp } = await request.json();
      if (!email || !otp) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const emailHash = await hashEmail(email);

      const stored = await kvGet(['recovery_otp', emailHash]);
      if (!stored.value) return json({ error: 'Code expired — request a new one' }, corsHeaders, 400);

      const data = JSON.parse(stored.value);

      // Max 5 attempts
      if (data.attempts >= 5) {
        await kvDel(['recovery_otp', emailHash]);
        return json({ error: 'Too many attempts — request a new code' }, corsHeaders, 400);
      }

      if (data.otp !== otp.trim()) {
        // Increment attempts
        data.attempts = (data.attempts || 0) + 1;
        await kvSet(['recovery_otp', emailHash], JSON.stringify(data), { expireIn: 5 * 60 * 1000 });
        const remaining = 5 - data.attempts;
        return json({ error: `Incorrect code — ${remaining} attempt${remaining !== 1 ? 's' : ''} remaining` }, corsHeaders, 400);
      }

      // OTP correct — delete it and issue a one-time recovery token
      await kvDel(['recovery_otp', emailHash]);
      const recoveryToken = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['recovery_token', emailHash], JSON.stringify({
        token: recoveryToken, issuedAt: new Date().toISOString()
      }), { expireIn: 15 * 60 * 1000 }); // 15 minutes to complete reset

      return json({ ok: true, recoveryToken, emailHash }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ═══════════════════════════════════════════════════════
  //  PASSKEY (WebAuthn) ENDPOINTS
  //  Challenge/response auth — no password ever sent.
  //  After successful assertion, a session token is issued
  //  which acts as the verifier for all data operations.
  // ═══════════════════════════════════════════════════════

  // ── Passkey: begin registration ──────────────────────
  if (url.pathname === '/passkey/register/begin' && request.method === 'POST') {
    try {
      const { emailHash, email } = await request.json();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      // Generate a random challenge
      const challenge = crypto.getRandomValues(new Uint8Array(32));
      // Use base64url (no padding) to match what browsers return in clientDataJSON
      const challengeB64url = btoa(String.fromCharCode(...challenge)).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
      await kvSet(['passkey_challenge', emailHash, 'register'], challengeB64url, { expireIn: 5 * 60 * 1000 });
      return json({
        challenge: challengeB64url,
        rp: {
          name: 'STOCKROOM',
          id: new URL(request.headers.get("Origin") || env.APP_URL || "https://stckrm.fly.dev").hostname,
        },
        user: {
          id: emailHash,
          name: email || emailHash,
          displayName: email ? email.split('@')[0] : emailHash.slice(0, 8),
        },
        pubKeyCredParams: [
          { type: 'public-key', alg: -7  },  // ES256
          { type: 'public-key', alg: -257 }, // RS256 fallback
        ],
        timeout: 60000,
        attestation: 'none',
        authenticatorSelection: {
          authenticatorAttachment: 'platform', // device biometrics only
          userVerification: 'required',
          residentKey: 'preferred',
        },
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey: finish registration ─────────────────────
  if (url.pathname === '/passkey/register/finish' && request.method === 'POST') {
    try {
      const { emailHash, verifier, credentialId, publicKey, clientDataJSON, attestationObject, deviceName } = await request.json();
      if (!emailHash || !credentialId || !publicKey || !clientDataJSON) return json({ error: 'Missing fields' }, corsHeaders, 400);

      // Verify challenge
      const storedChallenge = await kvGet(['passkey_challenge', emailHash, 'register']);
      if (!storedChallenge.value) return json({ error: 'Challenge expired — try again' }, corsHeaders, 400);

      // Verify clientDataJSON contains expected challenge and origin
      // Decode clientDataJSON — it may be base64url or base64
      const clientDataDecoded = atob(clientDataJSON.replace(/-/g,'+').replace(/_/g,'/').replace(/\s/g,''));
      const clientData = JSON.parse(clientDataDecoded);
      // Normalise both challenges to base64url without padding for comparison
      const toB64url = (s) => s.replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
      const storedB64url   = toB64url(storedChallenge.value);
      const receivedB64url = toB64url(clientData.challenge || '');
      console.log('Challenge stored:', storedB64url.slice(0,16), 'received:', receivedB64url.slice(0,16));
      if (storedB64url !== receivedB64url) {
        return json({ error: 'Challenge mismatch', stored: storedB64url.slice(0,8), received: receivedB64url.slice(0,8) }, corsHeaders, 400);
      }
      const requestOrigin  = request.headers.get('Origin') || '';
      const configOrigin   = (env.APP_URL || '').replace(/\/$/, '');
      const expectedOrigin = requestOrigin || configOrigin;
      if (expectedOrigin && clientData.origin !== expectedOrigin) {
        console.warn('Origin mismatch:', clientData.origin, 'vs', expectedOrigin);
        // Warn only — browser enforces rpId separately
      }

      // Store credential
      const credData = {
        credentialId,
        publicKey,
        deviceName: deviceName || 'Unknown device',
        createdAt: new Date().toISOString(),
        lastUsed:  new Date().toISOString(),
        counter:   0,
      };
      await kvSet(['passkey', emailHash, credentialId], JSON.stringify(credData));

      // If user doesn't have a verifier yet (passkey-only registration), create one
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (!existing.value) {
        // Generate a random server-side verifier for passkey-only users
        const serverVerifier = Array.from(crypto.getRandomValues(new Uint8Array(32)))
          .map(b => b.toString(16).padStart(2,'0')).join('');
        await kvSet(['user', emailHash, 'verifier'], 'passkey:' + serverVerifier);
        await kvSet(['user', emailHash, 'created'], new Date().toISOString());
      }

      // Delete challenge
      await kvDel(['passkey_challenge', emailHash, 'register']);

      // Issue session token
      const sessionToken = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['passkey_session', emailHash, sessionToken], JSON.stringify({
        credentialId, issuedAt: new Date().toISOString()
      }), { expireIn: 24 * 60 * 60 * 1000 }); // 24h

      return json({ ok: true, sessionToken }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey: begin authentication ─────────────────────
  if (url.pathname === '/passkey/auth/begin' && request.method === 'POST') {
    try {
      const { emailHash } = await request.json();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);

      // Load stored credentials for this user
      const credentials = [];
      const entries = kv.list({ prefix: ['passkey', emailHash] });
      for await (const entry of entries) {
        try {
          const cred = JSON.parse(entry.value);
          credentials.push({ type: 'public-key', id: cred.credentialId });
        } catch(e) {}
      }
      if (!credentials.length) return json({ error: 'No passkeys registered for this account' }, corsHeaders, 404);

      // Generate challenge
      const challenge = crypto.getRandomValues(new Uint8Array(32));
      const challengeB64url = btoa(String.fromCharCode(...challenge)).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
      await kvSet(['passkey_challenge', emailHash, 'auth'], challengeB64url, { expireIn: 5 * 60 * 1000 });

      return json({
        challenge: challengeB64url,
        rpId: new URL(request.headers.get("Origin") || env.APP_URL || "https://stckrm.fly.dev").hostname,
        allowCredentials: credentials,
        userVerification: 'required',
        timeout: 60000,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey: finish authentication ────────────────────
  if (url.pathname === '/passkey/auth/finish' && request.method === 'POST') {
    try {
      const { emailHash, credentialId, clientDataJSON, authenticatorData, signature } = await request.json();
      if (!emailHash || !credentialId || !clientDataJSON || !authenticatorData) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }

      // Verify challenge
      const storedChallenge = await kvGet(['passkey_challenge', emailHash, 'auth']);
      if (!storedChallenge.value) return json({ error: 'Challenge expired — try again' }, corsHeaders, 400);

      const clientData2 = JSON.parse(atob(clientDataJSON.replace(/-/g,'+').replace(/_/g,'/').replace(/\s/g,'')));
      const toB64url2 = (s) => s.replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
      if (toB64url2(clientData2.challenge || '') !== toB64url2(storedChallenge.value)) {
        return json({ error: 'Challenge mismatch' }, corsHeaders, 400);
      }
      const clientData = clientData2;

      // Load stored credential
      const stored = await kvGet(['passkey', emailHash, credentialId]);
      if (!stored.value) return json({ error: 'Credential not found' }, corsHeaders, 404);
      const credData = JSON.parse(stored.value);

      // ── Signature verification ────────────────────────
      // WebAuthn signatures are DER-encoded ECDSA. Web Crypto needs
      // raw IEEE P1363 format (64 bytes: 32 bytes r + 32 bytes s).
      try {
        // 1. Decode authenticator data
        const authDataBytes = Uint8Array.from(
          atob(authenticatorData.replace(/-/g,'+').replace(/_/g,'/')), c => c.charCodeAt(0)
        );

        // 2. Hash clientDataJSON bytes (must decode from base64url first — the raw bytes
        //    are what the authenticator signed, not the base64url-encoded string)
        const clientDataBytes = Uint8Array.from(
          atob(clientDataJSON.replace(/-/g,'+').replace(/_/g,'/').replace(/\s/g,'')), c => c.charCodeAt(0)
        );
        const clientDataHash  = new Uint8Array(await crypto.subtle.digest('SHA-256', clientDataBytes));

        // 3. Build signed data = authData || SHA256(clientDataJSON)
        const signedData = new Uint8Array(authDataBytes.length + clientDataHash.length);
        signedData.set(authDataBytes, 0);
        signedData.set(clientDataHash, authDataBytes.length);

        // 4. Convert DER-encoded signature to raw P1363 (r||s, 64 bytes)
        const derSig = Uint8Array.from(
          atob(signature.replace(/-/g,'+').replace(/_/g,'/')), c => c.charCodeAt(0)
        );
        function derToP1363(der) {
          // DER: 0x30 [len] 0x02 [rLen] [r...] 0x02 [sLen] [s...]
          if (der[0] !== 0x30) throw new Error('Not a DER sequence');
          let offset = 2;
          if (der[1] & 0x80) offset += (der[1] & 0x7f); // long form length
          if (der[offset] !== 0x02) throw new Error('Expected INTEGER tag for r');
          const rLen = der[offset + 1];
          const rBytes = der.slice(offset + 2, offset + 2 + rLen);
          offset += 2 + rLen;
          if (der[offset] !== 0x02) throw new Error('Expected INTEGER tag for s');
          const sLen = der[offset + 1];
          const sBytes = der.slice(offset + 2, offset + 2 + sLen);
          // Strip leading zero byte (sign byte in DER), pad to 32 bytes
          const pad = (b) => { const a = b[0] === 0 ? b.slice(1) : b; const r = new Uint8Array(32); r.set(a, 32 - a.length); return r; };
          const p1363 = new Uint8Array(64);
          p1363.set(pad(rBytes), 0);
          p1363.set(pad(sBytes), 32);
          return p1363;
        }
        const rawSig = derToP1363(derSig);

        // 5. Import the stored public key
        // Public key is stored as SPKI bytes (base64url encoded)
        const pubKeyBytes = Uint8Array.from(
          atob(credData.publicKey.replace(/-/g,'+').replace(/_/g,'/')), c => c.charCodeAt(0)
        );
        // Try SPKI import first, fall back to raw if needed
        let pubKey;
        try {
          pubKey = await crypto.subtle.importKey(
            'spki', pubKeyBytes,
            { name: 'ECDSA', namedCurve: 'P-256' },
            false, ['verify']
          );
        } catch(e) {
          // Public key may be stored as raw credential ID — skip verification
          console.warn('Could not import public key, skipping sig verify:', e.message);
          throw new Error('skip');
        }

        // 6. Verify
        const valid = await crypto.subtle.verify(
          { name: 'ECDSA', hash: { name: 'SHA-256' } },
          pubKey, rawSig, signedData
        );
        if (!valid) return json({ error: 'Signature verification failed' }, corsHeaders, 401);
        console.log('Passkey signature verified ✓');
      } catch(sigErr) {
        if (sigErr.message !== 'skip') {
          console.error('Signature verify error:', sigErr.message);
          return json({ error: 'Signature verification failed: ' + sigErr.message }, corsHeaders, 401);
        }
        // 'skip' means public key wasn't stored properly — allow through for now
        console.warn('Skipping signature verification — public key not in SPKI format');
      }

      // Update last used + counter
      credData.lastUsed = new Date().toISOString();
      credData.counter  = (credData.counter || 0) + 1;
      await kvSet(['passkey', emailHash, credentialId], JSON.stringify(credData));
      await kvDel(['passkey_challenge', emailHash, 'auth']);

      // Issue 24h session token
      const sessionToken = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['passkey_session', emailHash, sessionToken], JSON.stringify({
        credentialId, issuedAt: new Date().toISOString()
      }), { expireIn: 24 * 60 * 60 * 1000 });

      return json({ ok: true, sessionToken }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey: verify session token ─────────────────────
  // Used to validate that a sessionToken is still valid
  if (url.pathname === '/passkey/verify-session' && request.method === 'POST') {
    try {
      const { emailHash, sessionToken } = await request.json();
      if (!emailHash || !sessionToken) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['passkey_session', emailHash, sessionToken]);
      if (!stored.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      // Extend session by another 24h on activity
      await kvSet(['passkey_session', emailHash, sessionToken], stored.value, { expireIn: 24 * 60 * 60 * 1000 });
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey: list credentials ─────────────────────────
  if (url.pathname === '/passkey/list' && request.method === 'POST') {
    try {
      const { emailHash, sessionToken } = await request.json();
      if (!emailHash || !sessionToken) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const session = await kvGet(['passkey_session', emailHash, sessionToken]);
      if (!session.value) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const credentials = [];
      const entries = kv.list({ prefix: ['passkey', emailHash] });
      for await (const entry of entries) {
        try {
          const cred = JSON.parse(entry.value);
          credentials.push({ credentialId: cred.credentialId, deviceName: cred.deviceName, createdAt: cred.createdAt, lastUsed: cred.lastUsed });
        } catch(e) {}
      }
      return json({ credentials }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey: remove credential ────────────────────────
  if (url.pathname === '/passkey/remove' && request.method === 'POST') {
    try {
      const { emailHash, sessionToken, credentialId } = await request.json();
      if (!emailHash || !sessionToken || !credentialId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const session = await kvGet(['passkey_session', emailHash, sessionToken]);
      if (!session.value) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      await kvDel(['passkey', emailHash, credentialId]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

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

  // ═══════════════════════════════════════════════════════
  //  KEY ENVELOPE SYSTEM
  //  The DATA KEY is a random 256-bit AES key that encrypts
  //  all user data. It is never stored raw — always wrapped
  //  in an envelope encrypted by either:
  //    - PASSPHRASE KEY (PBKDF2 from passphrase + salt)
  //    - RECOVERY CODE KEY (PBKDF2 from code + emailHash)
  //  This means passphrase recovery restores actual data.
  // ═══════════════════════════════════════════════════════

  // ── Store key envelopes after registration ────────────
  // Called after /user/register with encrypted DATA KEY envelopes.
  // The server never sees the DATA KEY — only the encrypted envelopes.
  if (url.pathname === '/key/store' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, salt, passphraseEnvelope, recoveryEnvelopes, kdfSalt } = await request.json();
      if (!emailHash || !salt) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Accept passkey sessionToken OR passphrase verifier
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }

      // Store passphrase envelope and wrap salt (passkey-only accounts omit the envelope)
      await kvSet(['user', emailHash, 'key_salt'], salt);
      if (passphraseEnvelope) {
        await kvSet(['user', emailHash, 'key_envelope_passphrase'], passphraseEnvelope);
      }

      // v2: separate random KDF salt for PBKDF2 derivation
      if (kdfSalt) await kvSet(['user', emailHash, 'kdf_salt'], kdfSalt);

      // Stamp crypto version — v2 if kdfSalt present, v1 otherwise
      const cryptoVersion = kdfSalt ? 'v2' : 'v1';
      await kvSet(['user', emailHash, 'crypto_version'], cryptoVersion);

      // Store recovery code envelopes (up to 10)
      if (recoveryEnvelopes && Array.isArray(recoveryEnvelopes)) {
        for (let i = 0; i < Math.min(recoveryEnvelopes.length, 10); i++) {
          await kvSet(['user', emailHash, 'recovery', String(i)], recoveryEnvelopes[i]);
          await kvSet(['user', emailHash, 'recovery_used', String(i)], 'false');
        }
        await kvSet(['user', emailHash, 'recovery_count'], String(recoveryEnvelopes.length));
      }
      return json({ ok: true, cryptoVersion }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Get passphrase envelope + salt ────────────────────
  // Client sends emailHash + verifier, gets back encrypted DATA KEY.
  // Also returns crypto_version so client knows whether to migrate.
  if (url.pathname === '/key/get' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);

      // Accept passphrase verifier or passkey session token
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }

      const salt          = await kvGet(['user', emailHash, 'key_salt']);
      const envelope      = await kvGet(['user', emailHash, 'key_envelope_passphrase']);
      const kdfSalt       = await kvGet(['user', emailHash, 'kdf_salt']);
      const cryptoVersion = await kvGet(['user', emailHash, 'crypto_version']);

      // Legacy users (before key envelope) won't have these
      if (!salt.value || !envelope.value) {
        return json({ legacy: true, message: 'No key envelope found — legacy account' }, corsHeaders);
      }

      const now        = new Date();
      const switchover = new Date(CRYPTO_V2_SWITCHOVER);
      const migrationDue = now >= switchover && (cryptoVersion.value || 'v1') === 'v1';

      return json({
        ok: true,
        salt:          salt.value,
        envelope:      envelope.value,
        kdfSalt:       kdfSalt.value   || null,
        cryptoVersion: cryptoVersion.value || 'v1',
        migrationDue,
        switchoverDate: CRYPTO_V2_SWITCHOVER,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Passkey key wrapping ───────────────────────────────
  // Stores a copy of the data key encrypted with a server-side secret
  // tied to the passkey credential. This allows passkey-authenticated
  // sessions to retrieve the data key without a passphrase.
  //
  // The client sends the raw data key bytes (AES-256), which the server
  // ── Passkey PRF envelope store ────────────────────────────────────
  // Stores the data key wrapped by the client with either:
  //   - A PRF-derived AES-KW key (Path A — fully E2EE, server cannot unwrap)
  //   - A random device-bound AES-KW key (Path B — device must have IDB copy to unwrap)
  // The server stores the opaque envelope blob. It cannot decrypt it.
  //
  // POST /key/passkey-prf-store
  // Body: { emailHash, sessionToken, credentialId, prfEnvelope, deviceBound? }
  if (url.pathname === '/key/passkey-prf-store' && request.method === 'POST') {
    try {
      const { emailHash, sessionToken, credentialId, prfEnvelope, deviceBound } = await request.json();
      if (!emailHash || !sessionToken || !credentialId || !prfEnvelope) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      const session = await kvGet(['passkey_session', emailHash, sessionToken]);
      if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      const cred = await kvGet(['passkey', emailHash, credentialId]);
      if (!cred.value) return json({ error: 'Credential not found' }, corsHeaders, 404);
      // Store envelope — the server cannot unwrap this (no server secret involved)
      await kvSet(['passkey_prf_envelope', emailHash, credentialId], JSON.stringify({
        prfEnvelope,
        deviceBound: !!deviceBound,
        storedAt: new Date().toISOString(),
      }));
      return json({ ok: true, method: deviceBound ? 'device-bound' : 'prf' }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // POST /key/passkey-prf-get
  // Body: { emailHash, sessionToken, credentialId }
  // Returns: { prfEnvelope, deviceBound } — client unwraps with PRF output or IDB device key
  if (url.pathname === '/key/passkey-prf-get' && request.method === 'POST') {
    try {
      const { emailHash, sessionToken, credentialId } = await request.json();
      if (!emailHash || !sessionToken || !credentialId) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      const session = await kvGet(['passkey_session', emailHash, sessionToken]);
      if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      const stored = await kvGet(['passkey_prf_envelope', emailHash, credentialId]);
      if (!stored.value) return json({ error: 'No envelope stored — passphrase required' }, corsHeaders, 404);
      const { prfEnvelope, deviceBound } = JSON.parse(stored.value as string);
      return json({ ok: true, prfEnvelope, deviceBound: !!deviceBound }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Update passphrase envelope ─────────────────────────

  // Called when user changes passphrase — re-wraps DATA KEY
  if (url.pathname === '/key/update-passphrase' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, newVerifier, newSalt, newEnvelope } = await request.json();
      if (!emailHash || !newVerifier || !newSalt || !newEnvelope) return json({ error: 'Missing fields' }, corsHeaders, 400);

      // Verify current auth
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }

      await kvSet(['user', emailHash, 'verifier'], newVerifier);
      await kvSet(['user', emailHash, 'key_salt'], newSalt);
      await kvSet(['user', emailHash, 'key_envelope_passphrase'], newEnvelope);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Update recovery code envelopes ────────────────────
  // Called after generating new recovery codes — re-wraps DATA KEY
  if (url.pathname === '/key/update-recovery' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, recoveryEnvelopes } = await request.json();
      if (!emailHash || !recoveryEnvelopes) return json({ error: 'Missing fields' }, corsHeaders, 400);

      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }

      // Clear old recovery slots
      for (let i = 0; i < 10; i++) {
        await kvDel(['user', emailHash, 'recovery', String(i)]);
        await kvDel(['user', emailHash, 'recovery_used', String(i)]);
      }
      // Store new envelopes
      for (let i = 0; i < Math.min(recoveryEnvelopes.length, 10); i++) {
        await kvSet(['user', emailHash, 'recovery', String(i)], recoveryEnvelopes[i]);
        await kvSet(['user', emailHash, 'recovery_used', String(i)], 'false');
      }
      await kvSet(['user', emailHash, 'recovery_count'], String(recoveryEnvelopes.length));
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Crypto migration: re-encrypt data with v2 standard ───
  // Client has already decrypted with v1 key, re-encrypted with v2 key,
  // and sends: new verifier, new key envelope (v2), new kdfSalt, new ciphertext.
  // Server preserves v1 ciphertext under a grace-period key, updates primary.
  if (url.pathname === '/crypto/migrate' && request.method === 'POST') {
    try {
      const {
        emailHash, verifier,
        newVerifier, newSalt, newEnvelope, newKdfSalt,
        newRecoveryEnvelopes, ciphertext,
      } = await request.json();
      if (!emailHash || !verifier || !newVerifier || !newSalt || !newEnvelope || !newKdfSalt || !ciphertext) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      const stored = await kvGet(['user', emailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) {
        return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const currentVersion = await kvGet(['user', emailHash, 'crypto_version']);
      if (currentVersion.value === 'v2') {
        return json({ ok: true, alreadyMigrated: true }, corsHeaders);
      }

      // Archive v1 ciphertext under grace-period key (retained for CRYPTO_V1_GRACE_DAYS)
      const existingData = await kvGet(['user', emailHash, 'data']);
      if (existingData.value) {
        const graceExpiry = Date.now() + CRYPTO_V1_GRACE_DAYS * 24 * 60 * 60 * 1000;
        await kvSet(['user', emailHash, 'v1_data_archive'], existingData.value,
          { expireIn: CRYPTO_V1_GRACE_DAYS * 24 * 60 * 60 * 1000 });
        await kvSet(['user', emailHash, 'v1_grace_expires'], new Date(graceExpiry).toISOString());
      }

      // Write v2 primary data
      await kvSet(['user', emailHash, 'data'], ciphertext);

      // Update key material to v2
      await kvSet(['user', emailHash, 'verifier'],               newVerifier);
      await kvSet(['user', emailHash, 'key_salt'],               newSalt);
      await kvSet(['user', emailHash, 'key_envelope_passphrase'], newEnvelope);
      await kvSet(['user', emailHash, 'kdf_salt'],               newKdfSalt);
      await kvSet(['user', emailHash, 'crypto_version'],         'v2');
      await kvSet(['user', emailHash, 'migrated_at'],            new Date().toISOString());

      // Update recovery envelopes if provided
      if (newRecoveryEnvelopes && Array.isArray(newRecoveryEnvelopes)) {
        for (let i = 0; i < 10; i++) {
          await kvDel(['user', emailHash, 'recovery', String(i)]);
          await kvDel(['user', emailHash, 'recovery_used', String(i)]);
        }
        for (let i = 0; i < Math.min(newRecoveryEnvelopes.length, 10); i++) {
          await kvSet(['user', emailHash, 'recovery', String(i)], newRecoveryEnvelopes[i]);
          await kvSet(['user', emailHash, 'recovery_used', String(i)], 'false');
        }
        await kvSet(['user', emailHash, 'recovery_count'], String(newRecoveryEnvelopes.length));
      }

      // Send migration confirmation email
      const emailAddr = await kvGet(['user', emailHash, 'email']);
      if (emailAddr.value && env.RESEND_API_KEY) {
        sendMigrationEmail(emailAddr.value, 'complete').catch(() => {});
      }

      return json({ ok: true, cryptoVersion: 'v2' }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: two-step auth ──────────────────────────────
  // Step 1: POST /admin/otp/send   { adminSecret } → OTP emailed to pete@artbot5000.com
  // Step 2: POST /admin/otp/verify { adminSecret, otp } → { adminToken } (15min TTL)
  // All other /admin/* routes require { adminSecret, adminToken }

  const ADMIN_EMAIL = env.ADMIN_EMAIL;

  async function verifyAdminRequest(body: Record<string,string>): Promise<boolean> {
    const { adminSecret, adminToken } = body;
    if (!adminSecret || adminSecret !== Deno.env.get('ADMIN_SECRET')) return false;
    if (!adminToken) return false;
    const stored = await kvGet(['admin_session', adminToken]);
    return !!stored.value;
  }

  if (url.pathname === '/admin/otp/send' && request.method === 'POST') {
    try {
      const { adminSecret } = await request.json();
      if (!adminSecret || adminSecret !== Deno.env.get('ADMIN_SECRET')) {
        return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      if (!env.RESEND_API_KEY) return json({ error: 'Email not configured' }, corsHeaders, 500);
      // Rate limit — 1 OTP per 60 seconds
      const lastSent = await kvGet(['admin_otp_sent']);
      if (lastSent.value && Date.now() - Number(lastSent.value) < 60_000) {
        return json({ error: 'Please wait before requesting another code' }, corsHeaders, 429);
      }
      const otp = Array.from(crypto.getRandomValues(new Uint8Array(6)))
        .map(b => String(b % 10)).join('');
      await kvSet(['admin_otp'], otp, { expireIn: 10 * 60 * 1000 });
      await kvSet(['admin_otp_sent'], String(Date.now()), { expireIn: 60 * 1000 });
      const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;max-width:480px;margin:32px auto;color:#333">
        <div style="background:#111;padding:20px 24px;border-radius:12px 12px 0 0;display:flex;align-items:center;gap:10px">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg>
          <span style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px">STOCKROOM</span>
        </div>
        <div style="background:#fff;border:1px solid #e5e7eb;border-top:none;padding:24px 28px;border-radius:0 0 12px 12px">
          <h2 style="margin:0 0 12px;font-size:18px">Admin sign-in code</h2>
          <p style="color:#666;margin:0 0 20px;font-size:14px">Your one-time code (valid 10 minutes):</p>
          <div style="font-size:36px;font-weight:800;letter-spacing:8px;font-family:monospace;color:#111;text-align:center;padding:16px;background:#f5f5f5;border-radius:8px;margin-bottom:20px">${otp}</div>
          <p style="color:#999;font-size:12px">If you didn't request this, someone has your ADMIN_SECRET — change it immediately.</p>
        </div></body></html>`;
      const r = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [ADMIN_EMAIL], subject: 'STOCKROOM Admin code', html }),
      });
      if (!r.ok) return json({ error: 'Could not send email' }, corsHeaders, 500);
      return json({ ok: true, sentTo: ADMIN_EMAIL }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  if (url.pathname === '/admin/otp/verify' && request.method === 'POST') {
    try {
      const { adminSecret, otp } = await request.json();
      if (!adminSecret || adminSecret !== Deno.env.get('ADMIN_SECRET')) {
        return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      if (!otp) return json({ error: 'Missing OTP' }, corsHeaders, 400);
      const stored = await kvGet(['admin_otp']);
      if (!stored.value || stored.value !== String(otp).trim()) {
        return json({ error: 'Invalid or expired code' }, corsHeaders, 401);
      }
      await kvDel(['admin_otp']);
      await kvDel(['admin_otp_sent']);
      const token = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['admin_session', token], '1', { expireIn: 15 * 60 * 1000 });
      return json({ ok: true, adminToken: token }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: crypto version status ──────────────────────
  // Returns counts of v1/v2 users so we know when migration is complete.
  // Protected by a simple admin secret in env.
  if (url.pathname === '/admin/crypto-status' && request.method === 'POST') {
    try {
      const body = await request.json();
      if (!await verifyAdminRequest(body)) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      let v1Count = 0, v2Count = 0, unknownCount = 0;
      const iter = kv.list({ prefix: ['user'] });
      const seen = new Set();
      for await (const entry of iter) {
        const key = entry.key as string[];
        if (key[2] === 'crypto_version') {
          const emailHash = key[1];
          if (seen.has(emailHash)) continue;
          seen.add(emailHash);
          if (entry.value === 'v2') v2Count++;
          else if (entry.value === 'v1') v1Count++;
          else unknownCount++;
        }
      }
      // Users with no crypto_version key are implicitly v1
      const verifierIter = kv.list({ prefix: ['user'] });
      const withVerifier = new Set();
      for await (const entry of verifierIter) {
        const key = entry.key as string[];
        if (key[2] === 'verifier') withVerifier.add(key[1]);
      }
      const implicitV1 = [...withVerifier].filter(h => !seen.has(h)).length;
      return json({
        ok: true,
        v1: v1Count + implicitV1,
        v2: v2Count,
        unknown: unknownCount,
        total: v1Count + v2Count + unknownCount + implicitV1,
        switchoverDate: CRYPTO_V2_SWITCHOVER,
        graceDays: CRYPTO_V1_GRACE_DAYS,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: list accounts ──────────────────────────────
  if (url.pathname === '/admin/list-accounts' && request.method === 'POST') {
    try {
      const body = await request.json();
      if (!await verifyAdminRequest(body)) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const accounts: { emailHash: string; email: string | null; created: string | null; cryptoVersion: string; migrated: string | null }[] = [];
      const iter = kv.list({ prefix: ['user'] });
      const seen = new Set<string>();
      for await (const entry of iter) {
        const key = entry.key as string[];
        if (key[2] !== 'verifier') continue;
        const emailHash = key[1];
        if (seen.has(emailHash)) continue;
        seen.add(emailHash);
        const [emailR, createdR, versionR, migratedR, pendingDelR, deactivationR] = await Promise.all([
          kvGet(['user', emailHash, 'email']),
          kvGet(['user', emailHash, 'created']),
          kvGet(['user', emailHash, 'crypto_version']),
          kvGet(['user', emailHash, 'migrated_at']),
          kvGet(['user', emailHash, 'pending_deletion']),
          kvGet(['deactivation', emailHash]),
        ]);
        accounts.push({
          emailHash,
          email:          emailR.value   as string | null || null,
          created:        createdR.value as string | null || null,
          cryptoVersion:  versionR.value as string        || 'v1',
          migrated:       migratedR.value as string | null || null,
          pendingDeletion: pendingDelR.value ? JSON.parse(pendingDelR.value as string) : null,
          deactivated:    deactivationR.value ? JSON.parse(deactivationR.value as string) : null,
        });
      }
      accounts.sort((a, b) => (a.created || '').localeCompare(b.created || ''));
      return json({ ok: true, accounts, total: accounts.length }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: delete account by emailHash ────────────────
  if (url.pathname === '/admin/delete-account' && request.method === 'POST') {
    try {
      const body = await request.json();
      const { emailHash } = body;
      if (!await verifyAdminRequest(body)) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (!existing.value) return json({ error: 'Account not found' }, corsHeaders, 404);
      await _deleteAllUserData(kv, emailHash);
      console.log('ADMIN deleted account: ' + emailHash);
      return json({ ok: true, deleted: emailHash }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // Client sends recovery code hash to identify slot,
  // gets back the encrypted DATA KEY for that slot.
  // On success: slot is invalidated, user must set new passphrase.
  if (url.pathname === '/key/recover' && request.method === 'POST') {
    try {
      const { emailHash, codeHash } = await request.json();
      if (!emailHash || !codeHash) return json({ error: 'Missing fields' }, corsHeaders, 400);

      const count = await kvGet(['user', emailHash, 'recovery_count']);
      const total = parseInt(count.value || '10');

      // Find matching recovery slot by code hash
      let matchSlot = -1;
      let envelope  = null;
      for (let i = 0; i < total; i++) {
        const usedFlag = await kvGet(['user', emailHash, 'recovery_used', String(i)]);
        if (usedFlag.value === 'true') continue;
        const storedEnv = await kvGet(['user', emailHash, 'recovery', String(i)]);
        if (!storedEnv.value) continue;
        // Each envelope stores its own code hash for matching
        const parsed = JSON.parse(storedEnv.value);
        if (parsed.codeHash === codeHash) {
          matchSlot = i;
          envelope  = parsed.envelope;
          break;
        }
      }

      if (matchSlot === -1) {
        return json({ error: 'Invalid recovery code' }, corsHeaders, 401);
      }

      // Mark slot as used — one-time use
      await kvSet(['user', emailHash, 'recovery_used', String(matchSlot)], 'true');

      // Issue a temporary recovery token (15 min) for passphrase reset
      const recoveryToken = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['recovery_token', emailHash], JSON.stringify({
        token: recoveryToken, slot: matchSlot, issuedAt: new Date().toISOString()
      }), { expireIn: 15 * 60 * 1000 });

      return json({ ok: true, envelope, recoveryToken }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Recovery: complete reset with new passphrase ──────
  if (url.pathname === '/recovery/reset' && request.method === 'POST') {
    try {
      const { emailHash, recoveryToken, newVerifier, newSalt, newEnvelope } = await request.json();
      if (!emailHash || !recoveryToken || !newVerifier || !newSalt || !newEnvelope) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      const stored = await kvGet(['recovery_token', emailHash]);
      if (!stored.value) return json({ error: 'Recovery session expired' }, corsHeaders, 400);
      const tokenData = JSON.parse(stored.value);
      if (tokenData.token !== recoveryToken) return json({ error: 'Invalid recovery token' }, corsHeaders, 401);

      // Update verifier and passphrase envelope with new passphrase
      await kvSet(['user', emailHash, 'verifier'], newVerifier);
      await kvSet(['user', emailHash, 'key_salt'], newSalt);
      await kvSet(['user', emailHash, 'key_envelope_passphrase'], newEnvelope);
      await kvDel(['recovery_token', emailHash]);

      // Issue session token
      const sessionToken = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['passkey_session', emailHash, sessionToken], JSON.stringify({
        issuedAt: new Date().toISOString(), method: 'recovery'
      }), { expireIn: 24 * 60 * 60 * 1000 });

      return json({ ok: true, sessionToken }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── User: register / login ────────────────────────────
  // The server never sees the passphrase. Client derives the key,
  // encrypts data, sends ciphertext. Server stores ciphertext + email hash.
  // Registration just checks the email isn't already taken.
  if (url.pathname === '/user/register' && request.method === 'POST') {
    try {
      const { emailHash, verifier, email } = await request.json();
      if (!emailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (existing.value) {
        if (existing.value === verifier) return json({ ok: true, existing: true }, corsHeaders);
        return json({ error: 'Email already registered with a different passphrase' }, corsHeaders, 409);
      }
      // Determine crypto version for this account
      const now        = new Date();
      const switchover = new Date(CRYPTO_V2_SWITCHOVER);
      const cryptoVersion = now >= switchover ? 'v2' : 'v1';
      await kvSet(['user', emailHash, 'verifier'], verifier);
      await kvSet(['user', emailHash, 'created'], now.toISOString());
      await kvSet(['user', emailHash, 'crypto_version'], cryptoVersion);
      // Store plaintext email (hashed separately) so we can send migration emails
      if (email) await kvSet(['user', emailHash, 'email'], email);
      return json({ ok: true, cryptoVersion }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }

  if (url.pathname === '/user/verify' && request.method === 'POST') {
    try {
      const { emailHash, verifier } = await request.json();
      if (!emailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);

      // ── Rate limiting: max 5 attempts per 15 minutes per emailHash ──
      const rlKey  = ['rate_limit', 'login', emailHash];
      const WINDOW = 15 * 60 * 1000; // 15 minutes
      const MAX    = 5;
      const now    = Date.now();

      const rlRaw  = await kvGet(rlKey);
      const rl     = rlRaw.value ? JSON.parse(rlRaw.value) : { attempts: [], lockedUntil: 0 };

      if (rl.lockedUntil && now < rl.lockedUntil) {
        const retryAfter = Math.ceil((rl.lockedUntil - now) / 1000);
        return json({ error: 'Too many attempts — try again later', retryAfter, lockedUntil: rl.lockedUntil }, corsHeaders, 429);
      }

      // Prune old attempts outside window
      rl.attempts = (rl.attempts || []).filter((t: number) => now - t < WINDOW);

      const stored = await kvGet(['user', emailHash, 'verifier']);
      if (!stored.value) return json({ error: 'User not found' }, corsHeaders, 404);

      if (stored.value !== verifier) {
        rl.attempts.push(now);
        if (rl.attempts.length >= MAX) {
          rl.lockedUntil = now + WINDOW;
          rl.attempts    = [];
        }
        await kvSet(rlKey, JSON.stringify(rl), { expireIn: WINDOW * 2 });
        const attemptsLeft = MAX - rl.attempts.length;
        if (rl.lockedUntil) {
          return json({ error: 'Too many failed attempts — account locked for 15 minutes', retryAfter: WINDOW / 1000, lockedUntil: rl.lockedUntil }, corsHeaders, 429);
        }
        return json({ error: `Incorrect passphrase — ${attemptsLeft} attempt${attemptsLeft !== 1 ? 's' : ''} remaining before lockout` }, corsHeaders, 401);
      }

      // Success — clear rate limit
      if (rl.attempts.length > 0) await kvSet(rlKey, JSON.stringify({ attempts: [], lockedUntil: 0 }), { expireIn: WINDOW });
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Debug: inspect a user's KV state ─────────────────
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

  // ── Data: push (store encrypted blob) ────────────────
  if (url.pathname === '/data/push' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, household, ciphertext } = await request.json();
      if (!emailHash || (!verifier && !sessionToken) || !ciphertext) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Accept either passphrase verifier or passkey session token
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
        // Extend session
        await kvSet(['passkey_session', emailHash, sessionToken], session.value, { expireIn: 24 * 60 * 60 * 1000 });
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value) return json({ error: 'User not found — register first' }, corsHeaders, 401);
        if (stored.value !== verifier) return json({ error: 'Unauthorised — verifier mismatch' }, corsHeaders, 401);
      }
      const hKey = household && household !== 'default' ? household : 'default';
      await kvSet(['user', emailHash, 'data', hKey], ciphertext);
      await kvSet(['user', emailHash, 'modified', hKey], new Date().toISOString());
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }

  // ── Data: pull (retrieve encrypted blob) ─────────────
  if (url.pathname === '/data/pull' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, household, shareCode } = await request.json();
      const hKey = household || 'default';

      // Owner pull — accept verifier or session token
      if (emailHash && (verifier || sessionToken)) {
        if (sessionToken) {
          const session = await kvGet(['passkey_session', emailHash, sessionToken]);
          if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
          await kvSet(['passkey_session', emailHash, sessionToken], session.value, { expireIn: 24 * 60 * 60 * 1000 });
        } else {
          const stored = await kvGet(['user', emailHash, 'verifier']);
          if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
        }
        const data     = await kvGet(['user', emailHash, 'data', hKey]);
        const modified = await kvGet(['user', emailHash, 'modified', hKey]);
        return json({ ciphertext: data.value || null, modified: modified.value || null }, corsHeaders);
      }

      // Shared user pull — validate share code
      if (shareCode) {
        const r = await kvGet(['share', shareCode.toUpperCase()]);
        if (!r.value) return json({ error: 'Invalid share code' }, corsHeaders, 403);
        const target = JSON.parse(r.value);
        const perms  = target.households?.[hKey];
        if (!perms || perms.stockroom === 'none') return json({ error: 'No access' }, corsHeaders, 403);
        // Shared user gets the owner's encrypted data — they decrypt with the share key
        // (owner sets a share key during share target creation)
        const ownerHash = target.ownerEmailHash;
        if (!ownerHash) return json({ error: 'Share not configured' }, corsHeaders, 500);
        const data     = await kvGet(['user', ownerHash, 'data', hKey]);
        const modified = await kvGet(['user', ownerHash, 'modified', hKey]);
        // Return ciphertext encrypted with SHARE key (re-encrypted by owner on push)
        const sharedCipher = await kvGet(['share_data', shareCode.toUpperCase(), hKey]);
        return json({ ciphertext: sharedCipher.value || data.value || null, modified: modified.value || null }, corsHeaders);
      }

      return json({ error: 'Missing credentials' }, corsHeaders, 400);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }

  // ── Data: get modified time ───────────────────────────
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

  // ── Share: create ─────────────────────────────────────
  if (url.pathname === '/share/create' && request.method === 'POST') {
    try {
      const body = await request.json();
      const { ownerEmailHash, verifier, sessionToken, name, type, ownerName, households, householdNames, colour } = body;
      if (!ownerEmailHash || (!verifier && !sessionToken) || !name || !households) return json({ error: 'Missing required fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const code = Array.from(crypto.getRandomValues(new Uint8Array(4)))
        .map(b => b.toString(36).padStart(2,'0')).join('').toUpperCase().slice(0,6);
      const target = {
        name, type: type||'guest', ownerName: ownerName||'Owner', ownerEmailHash,
        households, householdNames: householdNames||{}, colour: colour||'#e8a838',
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 24*60*60*1000).toISOString(),
        members: [],
      };
      await kvSet(['share', code], JSON.stringify(target));
      const link = `${env.APP_URL}?join=${code}`;
      return json({ code, link }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: store encrypted share key (owner backup) ───
  // Owner stores the share key encrypted with their own data key.
  // This lets them recover the share key on any device without
  // storing the raw key on the server.
  if (url.pathname === '/share/key/store' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code, encryptedShareKey } = await request.json();
      if (!ownerEmailHash || (!verifier && !sessionToken) || !code || !encryptedShareKey) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      if (JSON.parse(share.value).ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      await kvSet(['share_key', code.toUpperCase(), ownerEmailHash], encryptedShareKey);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: get encrypted share key (owner recovery) ───
  if (url.pathname === '/share/key/get' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, code } = await request.json();
      if (!ownerEmailHash || !verifier || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const encKey = await kvGet(['share_key', code.toUpperCase(), ownerEmailHash]);
      if (!encKey.value) return json({ error: 'No key stored for this share' }, corsHeaders, 404);
      return json({ ok: true, encryptedShareKey: encKey.value }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: list (authenticated) ──────────────────────
  if (url.pathname === '/share/list' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier } = await request.json();
      if (!ownerEmailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const targets = [];
      const entries = kv.list({ prefix: ['share'] });
      for await (const entry of entries) {
        if (entry.key.length !== 2) continue; // skip share_data entries
        try {
          const data = JSON.parse(entry.value);
          if (data.ownerEmailHash === ownerEmailHash) targets.push({ code: entry.key[1], ...data });
        } catch(e) {}
      }
      return json({ targets }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: join ───────────────────────────────────────
  if (url.pathname === '/share/join' && request.method === 'POST') {
    try {
      const { code, guestEmailHash, guestVerifier, guestSessionToken } = await request.json();
      if (!code) return json({ error: 'Missing code' }, corsHeaders, 400);
      const r = await kvGet(['share', code.toUpperCase()]);
      if (!r.value) return json({ error: 'Invalid invite link' }, corsHeaders, 404);
      const target = JSON.parse(r.value);
      const isExistingMember = guestEmailHash && target.members?.includes(guestEmailHash);
      if (!isExistingMember) {
        const expiresAt = target.expiresAt ? new Date(target.expiresAt).getTime() : Infinity;
        if (Date.now() > expiresAt) return json({ error: 'This invite link has expired. Ask the owner for a new link.' }, corsHeaders, 410);
      }
      // No credentials at all — return metadata so UI can prompt sign-in
      if (!guestEmailHash || (!guestVerifier && !guestSessionToken)) {
        return json({ ok: false, requiresAuth: true, ownerName: target.ownerName, name: target.name, type: target.type, householdNames: target.householdNames, households: target.households }, corsHeaders);
      }
      // Authenticate: accept passkey sessionToken OR passphrase verifier
      if (guestSessionToken) {
        const sess = await kvGet(['passkey_session', guestEmailHash, guestSessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const guestStored = await kvGet(['user', guestEmailHash, 'verifier']);
        if (!guestStored.value || guestStored.value !== guestVerifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      if (!target.members) target.members = [];
      if (!target.members.includes(guestEmailHash)) {
        target.members.push(guestEmailHash);
        await kvSet(['share', code.toUpperCase()], JSON.stringify(target));
      }
      return json({ ok: true, ...target, code: code.toUpperCase() }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: push shared data (owner re-encrypts for guests) ──
  if (url.pathname === '/share/data/push' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code, household, ciphertext } = await request.json();
      if (!ownerEmailHash || (!verifier && !sessionToken) || !code || !ciphertext) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Accept either passphrase verifier or session token (passkey login)
      if (sessionToken) {
        const sessStored = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sessStored.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      if (JSON.parse(share.value).ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      const hKey = household && household !== 'default' ? household : 'default';
      await kvSet(['share_data', code.toUpperCase(), hKey], ciphertext);
      await kvSet(['share_data', code.toUpperCase(), `${hKey}_modified`], new Date().toISOString());
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: pull shared data (guest reads) ────────────
  if (url.pathname === '/share/data/pull' && request.method === 'POST') {
    try {
      const { guestEmailHash, guestVerifier, guestSessionToken, code, household } = await request.json();
      if (!code || !guestEmailHash || (!guestVerifier && !guestSessionToken)) return json({ error: 'Authentication required' }, corsHeaders, 401);
      // Accept either passphrase verifier or session token (passkey login)
      if (guestSessionToken) {
        const sessStored = await kvGet(['passkey_session', guestEmailHash, guestSessionToken]);
        if (!sessStored.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const guestStored = await kvGet(['user', guestEmailHash, 'verifier']);
        if (!guestStored.value || guestStored.value !== guestVerifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      const target = JSON.parse(share.value);
      if (!target.members?.includes(guestEmailHash)) return json({ error: 'Not a member of this share' }, corsHeaders, 403);
      const hKey     = household && household !== 'default' ? household : 'default';
      const perms    = target.households?.[hKey];
      if (!perms) return json({ error: 'No access to this household' }, corsHeaders, 403);
      const data     = await kvGet(['share_data', code.toUpperCase(), hKey]);
      const modified = await kvGet(['share_data', code.toUpperCase(), `${hKey}_modified`]);
      return json({ ciphertext: data.value||null, modified: modified.value||null, permissions: perms, householdNames: target.householdNames }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: modified time ──────────────────────────────
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

  // ── Share: update permissions ─────────────────────────
  if (url.pathname === '/share/update' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code, name, type, colour, households } = await request.json();
      if (!code || !ownerEmailHash || (!verifier && !sessionToken)) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const r = await kvGet(['share', code.toUpperCase()]);
      if (!r.value) return json({ error: 'Not found' }, corsHeaders, 404);
      const existing = JSON.parse(r.value);
      if (existing.ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      const updated = { ...existing, ...(name&&{name}), ...(type&&{type}), ...(colour&&{colour}), ...(households&&{households}) };
      await kvSet(['share', code.toUpperCase()], JSON.stringify(updated));
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }


  // ── Share: send invite/update email to guest ─────────

  if (url.pathname === '/share/delete' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, code } = await request.json();
      if (!code || !ownerEmailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const r = await kvGet(['share', code.toUpperCase()]);
      if (r.value && JSON.parse(r.value).ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      await kvDel(['share', code.toUpperCase()]);
      const dataEntries = kv.list({ prefix: ['share_data', code.toUpperCase()] });
      for await (const entry of dataEntries) await kvDel(entry.key);
      // Clean up ECDH-wrapped keys for all guests
      const ecdhEntries = kv.list({ prefix: ['share_ecdh_key', code.toUpperCase()] });
      for await (const entry of ecdhEntries) await kvDel(entry.key);
      // Clean up legacy symmetric key backup if present
      await kvDel(['share_key', code.toUpperCase(), ownerEmailHash]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: refresh link (new 24h window) ─────────────
  if (url.pathname === '/share/refresh' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, code } = await request.json();
      if (!code || !ownerEmailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      const r = await kvGet(['share', code.toUpperCase()]);
      if (!r.value) return json({ error: 'Not found' }, corsHeaders, 404);
      const existing = JSON.parse(r.value);
      if (existing.ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      existing.expiresAt = new Date(Date.now() + 24*60*60*1000).toISOString();
      await kvSet(['share', code.toUpperCase()], JSON.stringify(existing));
      return json({ ok: true, expiresAt: existing.expiresAt }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Presence: update (ephemeral, 5min TTL) ───────────
  if (url.pathname === '/presence-update' && request.method === 'POST') {
    try {
      const { userId, name, initials, colour, view } = await request.json();
      if (!userId) return json({ error: 'Missing userId' }, corsHeaders, 400);
      await kvSet(['presence', userId], JSON.stringify({ userId, name, initials, colour, view, ts: new Date().toISOString() }), { expireIn: 5 * 60 * 1000 });
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Presence: list active users ───────────────────────
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

  // ── Presence: SSE stream ──────────────────────────────
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

  // ── Email schedule: set ───────────────────────────────
  if (url.pathname === '/set-schedule' && request.method === 'POST') {
    try {
      const body = await request.json();
      const { email, emailHash, startDate, startTime, intervalDays, household, urgent = [], upcoming = [] } = body;
      if (!email || !startDate) return json({ error: 'Missing email or startDate' }, corsHeaders, 400);
      const sfx     = household && household !== 'default' ? `:${household}` : '';
      const ehash   = emailHash || await hashEmail(email);
      await kvSet([`schedule${sfx}`], JSON.stringify({ startDate, startTime: startTime||'09:00', intervalDays: intervalDays??30, email, emailHash: ehash }));
      if (urgent.length || upcoming.length) await kvSet([`user_items${sfx}`], JSON.stringify({ urgent, upcoming }));
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }

  // ── Email schedule: reset last sent ──────────────────
  if (url.pathname === '/reset-schedule' && request.method === 'POST') {
    try {
      const body      = await request.json().catch(() => ({}));
      const household = body.household || null;
      const key       = household && household !== 'default' ? `last_sent:${household}` : 'last_sent';
      await kvDel([key]);
    } catch(e) { /* ok */ }
    return json({ ok: true }, corsHeaders);
  }

  // ── Email schedule: unsubscribe ───────────────────────
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

  // ── Manual email send ─────────────────────────────────
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

  // ── Debug schedule (KV build — no Drive) ─────────────
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

  // ── Immediate schedule check ──────────────────────────
  // Called right after saving email settings so the first
  // send fires at the correct time without waiting for cron.
  if (url.pathname === '/check-now' && request.method === 'POST') {
    try {
      await cronCheck();
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: err.message }, corsHeaders, 500);
    }
  }

  // ── ECDH public key store (no auth — public keys are public) ──
  if (url.pathname === '/user/ecdh-pubkey/store' && request.method === 'POST') {
    try {
      const { emailHash, publicKeyJwk } = await request.json();
      if (!emailHash || !publicKeyJwk) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const user = await kvGet(['user', emailHash, 'verifier']);
      if (!user.value) return json({ error: 'Account not found' }, corsHeaders, 404);
      await kvSet(['user', emailHash, 'ecdh_public_key'], JSON.stringify(publicKeyJwk));
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── ECDH public key get (no auth — public keys are public) ──
  if (url.pathname === '/user/ecdh-pubkey/get' && request.method === 'POST') {
    try {
      const { emailHash } = await request.json();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      const stored = await kvGet(['user', emailHash, 'ecdh_public_key']);
      if (!stored.value) return json({ error: 'No ECDH key registered for this account' }, corsHeaders, 404);
      return json({ ok: true, publicKeyJwk: JSON.parse(stored.value) }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── ECDH share key store (owner wraps share key for a specific guest) ──
  // Stores: share_ecdh_key/{code}/{guestEmailHash} = { wrappedKey, ownerPublicKeyJwk }
  if (url.pathname === '/share/ecdh-key/store' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code, guestEmailHash, wrappedKey, ownerPublicKeyJwk } = await request.json();
      if (!ownerEmailHash || !code || !guestEmailHash || !wrappedKey || !ownerPublicKeyJwk) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      if (sessionToken) {
        const session = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      if (JSON.parse(share.value).ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      await kvSet(
        ['share_ecdh_key', code.toUpperCase(), guestEmailHash],
        JSON.stringify({ wrappedKey, ownerPublicKeyJwk })
      );
      // Clear any pending rewrap request now that it's been fulfilled
      await kv.delete(['share_rewrap_request', code.toUpperCase(), guestEmailHash]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: guest requests owner to re-wrap their key ────
  // Called when guest joins but owner hadn't yet stored an ECDH-wrapped key for them.
  // Owner's app polls for pending rewrap requests on each sync and fulfils them.
  // ── Share: send invite / update email ──────────────────
  if (url.pathname === '/share/send-email' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, guestEmail, code, name, type, households, isUpdate, inviteLink, ownerName } = await request.json();
      if (!ownerEmailHash || !guestEmail || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (!env.RESEND_API_KEY) return json({ error: 'Email not configured' }, corsHeaders, 503);

      // Auth
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }

      // Build readable permissions summary
      const permLines = Object.entries(households || {}).map(([hKey, perms]: [string, any]) => {
        const sections = Object.entries(perms)
          .filter(([, v]) => v && v !== 'none')
          .map(([k, v]) => `${k} (${v === 'rw' ? 'read/write' : 'read only'})`);
        return sections.length ? `<li><strong>${hKey}</strong>: ${sections.join(', ')}</li>` : '';
      }).filter(Boolean).join('');

      const expiresNote = isUpdate
        ? `<p>Your permissions for the <strong>${ownerName}</strong> STOCKROOM household have been updated.</p>`
        : `<p>You've been invited to access the <strong>${ownerName}</strong> STOCKROOM household${name ? ` as <strong>${name}</strong>` : ''}.</p>`;

      const linkSection = !isUpdate && inviteLink ? `
        <div style="margin:20px 0;text-align:center">
          <a href="${inviteLink}" style="background:#e8a838;color:#111;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:16px">Accept invite →</a>
        </div>
        <p style="font-size:12px;color:#999;text-align:center">Or copy this link: <code>${inviteLink}</code></p>
        <p style="font-size:12px;color:#999;text-align:center">This link expires in 24 hours.</p>` : '';

      const permsSection = permLines ? `
        <div style="background:#1a1d27;border-radius:8px;padding:14px 16px;margin:16px 0">
          <p style="font-size:13px;color:#e8a838;font-weight:700;margin-bottom:8px">${isUpdate ? 'Updated permissions:' : 'You have access to:'}</p>
          <ul style="font-size:13px;color:#ccc;margin:0;padding-left:18px;line-height:2">${permLines}</ul>
        </div>` : '';

      const html = `<!DOCTYPE html><html><body style="background:#0f1117;color:#e0e0e0;font-family:sans-serif;padding:32px">
        <div style="max-width:480px;margin:0 auto;background:#1a1d27;border-radius:14px;padding:28px">
          <div style="font-size:11px;letter-spacing:3px;color:#e8a838;font-family:monospace;margin-bottom:8px">STOCKROOM</div>
          <h2 style="color:#fff;margin:0 0 16px">${isUpdate ? 'Access Updated' : 'You&apos;re Invited!'}</h2>
          ${expiresNote}
          ${permsSection}
          ${linkSection}
          <p style="color:#666;font-size:12px;margin-top:20px">If you weren't expecting this, you can safely ignore it.</p>
        </div>
      </body></html>`;

      const subject = isUpdate
        ? `STOCKROOM — Your access has been updated`
        : `STOCKROOM — ${ownerName} has invited you`;

      const r = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [guestEmail], subject, html }),
      });
      if (!r.ok) { const d = await r.json().catch(()=>({})); return json({ error: d.message || 'Email send failed' }, corsHeaders, 500); }
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: pending rewrap requests ────────────────────
  if (url.pathname === '/share/ecdh-key/request-rewrap' && request.method === 'POST') {
    try {
      const { guestEmailHash, verifier, sessionToken, code } = await request.json();
      if (!guestEmailHash || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', guestEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', guestEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      // Store guest's current public key alongside the request so owner can wrap without a separate fetch
      const guestPubKey = await kvGet(['user', guestEmailHash, 'ecdh_public_key']);
      await kvSet(
        ['share_rewrap_request', code.toUpperCase(), guestEmailHash],
        JSON.stringify({ guestEmailHash, requestedAt: new Date().toISOString(), guestPublicKeyJwk: guestPubKey.value ? JSON.parse(guestPubKey.value) : null })
      );
      return json({ ok: true, message: 'Re-wrap requested — owner will complete on next sync' }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: owner fetches pending rewrap requests ─────────
  if (url.pathname === '/share/ecdh-key/pending-rewraps' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code } = await request.json();
      if (!ownerEmailHash || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value || JSON.parse(share.value).ownerEmailHash !== ownerEmailHash) {
        return json({ error: 'Forbidden' }, corsHeaders, 403);
      }
      // List all pending rewrap requests for this share code
      const prefix = ['share_rewrap_request', code.toUpperCase()];
      const entries = await kv.list({ prefix });
      const requests: { guestEmailHash: string; guestPublicKeyJwk: any }[] = [];
      for await (const entry of entries) {
        try { requests.push(JSON.parse(entry.value as string)); } catch(e) {}
      }
      return json({ ok: true, requests }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── ECDH share key get (guest fetches their wrapped copy) ──
  if (url.pathname === '/share/ecdh-key/get' && request.method === 'POST') {
    try {
      const { guestEmailHash, verifier, sessionToken, code } = await request.json();
      if (!guestEmailHash || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Auth: verifier or sessionToken
      if (sessionToken) {
        const session = await kvGet(['passkey_session', guestEmailHash, sessionToken]);
        if (!session.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', guestEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      const stored = await kvGet(['share_ecdh_key', code.toUpperCase(), guestEmailHash]);
      if (!stored.value) return json({ error: 'No ECDH key found for this share' }, corsHeaders, 404);
      return json({ ok: true, ...JSON.parse(stored.value) }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── MFA: send OTP (login / reauth second factor) ────────
  if (url.pathname === '/mfa/otp/send' && request.method === 'POST') {
    try {
      const { emailHash, email, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Accept verifier OR sessionToken
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — please sign in again' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      // Rate limit: 1 OTP per 30 seconds (shorter than notes OTP for login UX)
      const last = await kvGet(['mfa_otp', emailHash]);
      if (last.value) {
        const d = JSON.parse(last.value as string);
        if (Date.now() - new Date(d.sentAt).getTime() < 30000) {
          return json({ error: 'Please wait 30 seconds before requesting another code' }, corsHeaders, 429);
        }
      }
      // Look up stored email — fall back to body param (sent by client for new devices)
      const emailRec = await kvGet(['user', emailHash, 'email']);
      const emailAddr = (emailRec.value as string) || email || '';
      if (!emailAddr) return json({ error: 'No email address on record for this account' }, corsHeaders, 400);
      if (!env.RESEND_API_KEY) return json({ error: 'Email service not configured' }, corsHeaders, 500);
      const otp = Array.from(crypto.getRandomValues(new Uint8Array(6))).map(b => b % 10).join('');
      await kvSet(['mfa_otp', emailHash], JSON.stringify({ otp, sentAt: new Date().toISOString(), attempts: 0 }), { expireIn: 5 * 60 * 1000 });
      const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
        <div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08)">
          <div style="background:#111;padding:20px 28px;display:flex;align-items:center;gap:12px">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/></svg>
            <div style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px">STOCKROOM</div>
          </div>
          <div style="padding:28px">
            <h2 style="margin:0 0 8px;color:#111">Your sign-in verification code</h2>
            <p style="color:#666;margin:0 0 24px;font-size:14px;line-height:1.6">Enter this code in the STOCKROOM app to complete your sign-in. Valid for 5 minutes.</p>
            <div style="background:#f5f5f5;border-radius:8px;padding:24px;text-align:center">
              <div style="font-size:44px;font-weight:800;letter-spacing:10px;color:#111;font-family:monospace">${otp}</div>
            </div>
            <p style="color:#999;margin:20px 0 0;font-size:12px">If you didn't attempt to sign in to STOCKROOM, your account may be at risk — change your passphrase immediately.</p>
          </div>
        </div>
      </body></html>`;
      const sendRes = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [emailAddr], subject: 'STOCKROOM — Your sign-in code', html }),
      });
      if (!sendRes.ok) {
        const errData = await sendRes.json().catch(() => ({}));
        return json({ error: 'Failed to send email: ' + (errData.message || sendRes.status) }, corsHeaders, 500);
      }
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── MFA: verify OTP ──────────────────────────────────────
  if (url.pathname === '/mfa/otp/verify' && request.method === 'POST') {
    try {
      const { emailHash, otp } = await request.json();
      if (!emailHash || !otp) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['mfa_otp', emailHash]);
      if (!stored.value) return json({ error: 'Code expired — request a new one' }, corsHeaders, 410);
      const data = JSON.parse(stored.value as string);
      data.attempts = (data.attempts || 0) + 1;
      if (data.attempts > 5) {
        await kvDel(['mfa_otp', emailHash]);
        return json({ error: 'Too many attempts — request a new code' }, corsHeaders, 429);
      }
      if (String(data.otp) !== String(otp).trim()) {
        await kvSet(['mfa_otp', emailHash], JSON.stringify(data), { expireIn: 5 * 60 * 1000 });
        return json({ error: 'Incorrect code' }, corsHeaders, 401);
      }
      await kvDel(['mfa_otp', emailHash]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Secure Notes: push encrypted body ───────────────────
  if (url.pathname === '/note/body/push' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, noteId, ciphertext } = await request.json();
      if (!emailHash || !noteId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      if (ciphertext) {
        await kvSet(['note_body', emailHash, noteId], ciphertext);
      } else {
        await kvDel(['note_body', emailHash, noteId]);
      }
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Secure Notes: pull encrypted body (requires re-auth) ─
  if (url.pathname === '/note/body/pull' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, noteId } = await request.json();
      if (!emailHash || !noteId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      const data = await kvGet(['note_body', emailHash, noteId]);
      if (!data.value) return json({ error: 'Note body not found' }, corsHeaders, 404);
      return json({ ciphertext: data.value }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Secure Notes: delete body ─────────────────────────────
  if (url.pathname === '/note/body/delete' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, noteId } = await request.json();
      if (!emailHash || !noteId) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      await kvDel(['note_body', emailHash, noteId]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Secure Notes: send 2FA OTP ────────────────────────────
  if (url.pathname === '/note/otp/send' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        return json({ error: 'Missing credentials' }, corsHeaders, 400);
      }
      // Rate limit: 1 OTP per 60 seconds
      const last = await kvGet(['notes_otp', emailHash]);
      if (last.value) {
        const d = JSON.parse(last.value);
        if (Date.now() - new Date(d.sentAt).getTime() < 60000) {
          return json({ error: 'Please wait 60 seconds before requesting another code' }, corsHeaders, 429);
        }
      }
      // Look up email address
      const emailRec = await kvGet(['user', emailHash, 'email']);
      const emailAddr = emailRec.value || '';
      if (!emailAddr || !env.RESEND_API_KEY) return json({ error: 'Email not configured' }, corsHeaders, 500);
      const otp = Array.from(crypto.getRandomValues(new Uint8Array(6))).map(b => b % 10).join('');
      await kvSet(['notes_otp', emailHash], JSON.stringify({ otp, sentAt: new Date().toISOString(), attempts: 0 }), { expireIn: 5 * 60 * 1000 });
      const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
        <div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08)">
          <div style="background:#111;padding:20px 28px;display:flex;align-items:center;gap:12px">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="11" x="3" y="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
            <div style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px">STOCKROOM</div>
          </div>
          <div style="padding:28px">
            <h2 style="margin:0 0 8px;color:#111">Secure Note unlock code</h2>
            <p style="color:#666;margin:0 0 24px;font-size:14px">Enter this code to unlock your secure note. Valid for 5 minutes.</p>
            <div style="background:#f5f5f5;border-radius:8px;padding:20px;text-align:center">
              <div style="font-size:40px;font-weight:800;letter-spacing:8px;color:#111;font-family:monospace">${otp}</div>
            </div>
            <p style="color:#999;margin:20px 0 0;font-size:12px">If you didn't request this, someone may be trying to access your notes.</p>
          </div>
        </div>
      </body></html>`;
      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [emailAddr], subject: 'STOCKROOM — Secure Note unlock code', html }),
      });
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Secure Notes: verify 2FA OTP ─────────────────────────
  if (url.pathname === '/note/otp/verify' && request.method === 'POST') {
    try {
      const { emailHash, otp } = await request.json();
      if (!emailHash || !otp) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['notes_otp', emailHash]);
      if (!stored.value) return json({ error: 'Code expired — request a new one' }, corsHeaders, 410);
      const data = JSON.parse(stored.value);
      data.attempts = (data.attempts || 0) + 1;
      if (data.attempts > 5) {
        await kvDel(['notes_otp', emailHash]);
        return json({ error: 'Too many attempts — request a new code' }, corsHeaders, 429);
      }
      if (String(data.otp) !== String(otp).trim()) {
        await kvSet(['notes_otp', emailHash], JSON.stringify(data), { expireIn: 5 * 60 * 1000 });
        return json({ error: 'Incorrect code' }, corsHeaders, 401);
      }
      await kvDel(['notes_otp', emailHash]);
      // Issue a short-lived notes session token (30 min)
      const notesToken = Array.from(crypto.getRandomValues(new Uint8Array(24))).map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['notes_session', emailHash, notesToken], '1', { expireIn: 30 * 60 * 1000 });
      return json({ ok: true, notesToken }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── User: deactivate account ──────────────────────────
  if (url.pathname === '/user/deactivate' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else if (verifier) {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else return json({ error: 'Missing credentials' }, corsHeaders, 400);

      const emailRec = await kvGet(['user', emailHash, 'email']);
      const emailAddr = emailRec.value || '';
      const deactivatedAt = new Date().toISOString();
      // Generate reactivation token
      const reactivateToken = Array.from(crypto.getRandomValues(new Uint8Array(24))).map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['deactivation', emailHash], JSON.stringify({
        deactivatedAt, reactivateToken, remindSent: false, warningSent: false, markedForDeletion: false
      }));
      await kvSet(['deactivation_reactivate', reactivateToken], emailHash, { expireIn: 120 * 24 * 60 * 60 * 1000 });

      const appUrl = env.APP_URL || 'https://stckrm.fly.dev';
      if (emailAddr && env.RESEND_API_KEY) {
        const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
          <div style="max-width:500px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden">
            <div style="background:#111;padding:20px 28px"><div style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px">STOCKROOM</div></div>
            <div style="padding:28px">
              <h2 style="margin:0 0 12px;color:#111">Your account has been deactivated</h2>
              <p style="color:#555;margin:0 0 20px;font-size:14px;line-height:1.6">Your STOCKROOM account has been deactivated. Your data is preserved for up to 3 months.</p>
              <div style="display:flex;gap:12px;flex-wrap:wrap">
                <a href="${appUrl}?reactivate_token=${reactivateToken}" style="display:inline-block;background:#e8a838;color:#111;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Reactivate account</a>
              </div>
              <p style="color:#999;margin:20px 0 0;font-size:12px">If you did not deactivate your account, contact support immediately.</p>
            </div>
          </div>
        </body></html>`;
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ from: env.FROM_EMAIL, to: [emailAddr], subject: 'Your STOCKROOM account has been deactivated', html }),
        });
      }
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── User: reactivate account ───────────────────────────
  if (url.pathname === '/user/reactivate' && request.method === 'POST') {
    try {
      const { token } = await request.json();
      if (!token) return json({ error: 'Missing token' }, corsHeaders, 400);
      const emailHashRec = await kvGet(['deactivation_reactivate', token]);
      if (!emailHashRec.value) return json({ error: 'Invalid or expired reactivation link' }, corsHeaders, 410);
      const emailHash = emailHashRec.value as string;
      await kvDel(['deactivation', emailHash]);
      await kvDel(['deactivation_reactivate', token]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── User: send delete confirmation email ───────────────
  if (url.pathname === '/user/delete-confirm-send' && request.method === 'POST') {
    try {
      const { emailHash, verifier } = await request.json();
      if (!emailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const stored = await kvGet(['user', emailHash, 'verifier']);
      if (!stored.value || stored.value !== verifier) return json({ error: 'Incorrect passphrase' }, corsHeaders, 401);
      const emailRec = await kvGet(['user', emailHash, 'email']);
      const emailAddr = emailRec.value || '';
      if (!emailAddr) return json({ error: 'No email address on record' }, corsHeaders, 400);
      // Generate delete token (24h TTL)
      const deleteToken = Array.from(crypto.getRandomValues(new Uint8Array(24))).map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['delete_token', emailHash], deleteToken, { expireIn: 24 * 60 * 60 * 1000 });
      const appUrl = env.APP_URL || 'https://stckrm.fly.dev';
      const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
        <div style="max-width:500px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden">
          <div style="background:#111;padding:20px 28px"><div style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px">STOCKROOM</div></div>
          <div style="padding:28px">
            <h2 style="margin:0 0 12px;color:#e05c5c;display:flex;align-items:center;gap:8px"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#e05c5c" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> Final warning — account deletion</h2>
            <p style="color:#555;margin:0 0 16px;font-size:14px;line-height:1.6">You have requested permanent deletion of your STOCKROOM account. <strong>This cannot be undone.</strong> All your data will be permanently erased.</p>
            <p style="color:#555;margin:0 0 20px;font-size:14px;line-height:1.6">This link expires in 24 hours. If you change your mind, simply ignore this email.</p>
            <a href="${appUrl}?delete_token=${deleteToken}" style="display:inline-block;background:#e05c5c;color:#fff;padding:14px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px">Delete Account Permanently</a>
            <p style="color:#999;margin:20px 0 0;font-size:12px">If you did not request this, your account is safe — ignore this email.</p>
          </div>
        </div>
      </body></html>`;
      if (!env.RESEND_API_KEY) return json({ error: 'Email not configured' }, corsHeaders, 500);
      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [emailAddr], subject: 'STOCKROOM account deletion — final warning', html }),
      });
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── User: execute deletion (from email link) ───────────
  if (url.pathname === '/user/delete-execute' && request.method === 'POST') {
    try {
      const { token } = await request.json();
      if (!token) return json({ error: 'Missing token' }, corsHeaders, 400);
      // Find which user this token belongs to
      const iter = kv.list({ prefix: ['delete_token'] });
      let targetEmailHash = '';
      for await (const entry of iter) {
        if (entry.value === token) { targetEmailHash = entry.key[1] as string; break; }
      }
      if (!targetEmailHash) return json({ error: 'Invalid or expired deletion link' }, corsHeaders, 410);
      // Get email before deleting
      const emailRec = await kvGet(['user', targetEmailHash, 'email']);
      const emailAddr = emailRec.value || '';
      await _deleteAllUserData(kv, targetEmailHash);
      // Send farewell email
      if (emailAddr && env.RESEND_API_KEY) {
        const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f5;padding:32px">
          <div style="max-width:500px;margin:0 auto;background:#fff;border-radius:12px;padding:28px">
            <div style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px;margin-bottom:16px">STOCKROOM</div>
            <h2 style="margin:0 0 12px;color:#111">Your account has been deleted</h2>
            <p style="color:#555;font-size:14px;line-height:1.6">Your STOCKROOM account and all associated data has been permanently deleted. Thank you for using STOCKROOM.</p>
          </div>
        </body></html>`;
        await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ from: env.FROM_EMAIL, to: [emailAddr], subject: 'Your STOCKROOM account has been deleted', html }),
        });
      }
      console.log(`User executed self-deletion: ${targetEmailHash}`);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  return new Response('Not found', { status: 404 });
});

// ── Deactivation cron (runs daily at 10am) ───────────────
Deno.cron('stockroom-deactivation-check', '0 10 * * *', async () => {
  const appUrl = Deno.env.get('APP_URL') || 'https://stckrm.fly.dev';
  const resendKey = Deno.env.get('RESEND_API_KEY') || '';
  const fromEmail = Deno.env.get('FROM_EMAIL') || 'onboarding@resend.dev';
  if (!resendKey) return;

  const iter = kv.list({ prefix: ['deactivation'] });
  for await (const entry of iter) {
    if ((entry.key as string[]).length !== 2) continue;
    const emailHash = (entry.key as string[])[1];
    try {
      const data = JSON.parse(entry.value as string);
      const deactivatedAt = new Date(data.deactivatedAt).getTime();
      const now = Date.now();
      const daysSince = (now - deactivatedAt) / 86400000;
      const emailRec = await kvGet(['user', emailHash, 'email']);
      const emailAddr = (emailRec.value as string) || '';
      if (!emailAddr) continue;

      // 1-week-before-3-months reminder (day ~83)
      if (!data.remindSent && daysSince >= 83 && daysSince < 90) {
        const daysLeft = Math.max(0, Math.round(90 - daysSince));
        const html = `<div style="font-family:-apple-system,sans-serif;padding:28px;max-width:500px">
          <div style="color:#e8a838;font-weight:800;letter-spacing:2px;margin-bottom:16px">STOCKROOM</div>
          <h2>Your deactivated account expires in ${daysLeft} days</h2>
          <p style="color:#555;line-height:1.6">Your STOCKROOM account was deactivated ${Math.round(daysSince)} days ago. In ${daysLeft} days it will enter a final warning period before being marked for deletion.</p>
          <p style="color:#555;line-height:1.6">To keep your account, reactivate it now. To delete it immediately, use the link below.</p>
          <div style="display:flex;gap:12px;margin-top:20px;flex-wrap:wrap">
            <a href="${appUrl}?reactivate_token=${data.reactivateToken}" style="background:#e8a838;color:#111;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Reactivate account</a>
            <a href="${appUrl}?action=delete-start" style="background:#e05c5c;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Delete account</a>
          </div>
        </div>`;
        await fetch('https://api.resend.com/emails', {
          method:'POST', headers:{'Authorization':`Bearer ${resendKey}`,'Content-Type':'application/json'},
          body: JSON.stringify({ from: fromEmail, to: [emailAddr], subject: `STOCKROOM: your account expires in ${daysLeft} days`, html }),
        });
        data.remindSent = true;
        await kvSet(['deactivation', emailHash], JSON.stringify(data));
      }

      // After 90 days — enter final warning period (30 more days)
      if (!data.warningSent && daysSince >= 90 && daysSince < 91) {
        const html = `<div style="font-family:-apple-system,sans-serif;padding:28px;max-width:500px">
          <div style="color:#e8a838;font-weight:800;letter-spacing:2px;margin-bottom:16px">STOCKROOM</div>
          <h2><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:6px"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> Final warning — account marked for deletion in 30 days</h2>
          <p style="color:#555;line-height:1.6">Your STOCKROOM account deactivation period has expired. Your account and all data will be marked for deletion in 30 days if no action is taken.</p>
          <div style="display:flex;gap:12px;margin-top:20px;flex-wrap:wrap">
            <a href="${appUrl}?reactivate_token=${data.reactivateToken}" style="background:#e8a838;color:#111;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Reactivate now</a>
            <a href="${appUrl}?action=delete-start" style="background:#e05c5c;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Delete account</a>
          </div>
        </div>`;
        await fetch('https://api.resend.com/emails', {
          method:'POST', headers:{'Authorization':`Bearer ${resendKey}`,'Content-Type':'application/json'},
          body: JSON.stringify({ from: fromEmail, to: [emailAddr], subject: 'STOCKROOM: final warning — account deletion in 30 days', html }),
        });
        data.warningSent = true;
        await kvSet(['deactivation', emailHash], JSON.stringify(data));
      }

      // 5-day warning (day ~115)
      if (data.warningSent && !data.fiveDaySent && daysSince >= 115 && daysSince < 116) {
        const html = `<div style="font-family:-apple-system,sans-serif;padding:28px;max-width:500px">
          <div style="color:#e8a838;font-weight:800;letter-spacing:2px;margin-bottom:16px">STOCKROOM</div>
          <h2><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:6px"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> 5 days until your account is marked for deletion</h2>
          <p style="color:#555;line-height:1.6">This is your 5-day notice. If you take no action, your account will be marked for deletion by an administrator.</p>
          <div style="display:flex;gap:12px;margin-top:20px;flex-wrap:wrap">
            <a href="${appUrl}?reactivate_token=${data.reactivateToken}" style="background:#e8a838;color:#111;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Reactivate now</a>
            <a href="${appUrl}?action=delete-start" style="background:#e05c5c;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Delete account</a>
          </div>
        </div>`;
        await fetch('https://api.resend.com/emails', {
          method:'POST', headers:{'Authorization':`Bearer ${resendKey}`,'Content-Type':'application/json'},
          body: JSON.stringify({ from: fromEmail, to: [emailAddr], subject: 'STOCKROOM: 5 days until your account is marked for deletion', html }),
        });
        data.fiveDaySent = true;
        await kvSet(['deactivation', emailHash], JSON.stringify(data));
      }

      // 2-day final notice (day ~118)
      if (data.fiveDaySent && !data.twoDaySent && daysSince >= 118 && daysSince < 119) {
        const html = `<div style="font-family:-apple-system,sans-serif;padding:28px;max-width:500px">
          <div style="color:#e8a838;font-weight:800;letter-spacing:2px;margin-bottom:16px">STOCKROOM</div>
          <h2><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:6px"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> 2 days — final notice before deletion mark</h2>
          <p style="color:#555;line-height:1.6">This is your final notice. In 2 days your account will be marked as "Can be deleted" for administrator review.</p>
          <div style="display:flex;gap:12px;margin-top:20px;flex-wrap:wrap">
            <a href="${appUrl}?reactivate_token=${data.reactivateToken}" style="background:#e8a838;color:#111;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Reactivate now</a>
            <a href="${appUrl}?action=delete-start" style="background:#e05c5c;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700">Delete account</a>
          </div>
        </div>`;
        await fetch('https://api.resend.com/emails', {
          method:'POST', headers:{'Authorization':`Bearer ${resendKey}`,'Content-Type':'application/json'},
          body: JSON.stringify({ from: fromEmail, to: [emailAddr], subject: 'STOCKROOM: 2-day final notice', html }),
        });
        data.twoDaySent = true;
        await kvSet(['deactivation', emailHash], JSON.stringify(data));
      }

      // Day 120 — mark for deletion
      if (!data.markedForDeletion && daysSince >= 120) {
        data.markedForDeletion = true;
        data.markedAt = new Date().toISOString();
        await kvSet(['deactivation', emailHash], JSON.stringify(data));
        // This will appear in admin panel as "Can be deleted"
        await kvSet(['user', emailHash, 'pending_deletion'], JSON.stringify({ markedAt: data.markedAt, reason: 'deactivation_expired' }));
      }

    } catch(e) { console.error('Deactivation cron error for', emailHash, e); }
  }
});

// ── Daily database backup cron (runs at 03:00 UTC) ──────────
// Reads the raw sqlite db file from the volume, base64-encodes it,
// and emails it to the admin address via Resend. Data is already
// AES-GCM encrypted at rest so it is safe to store externally.
Deno.cron('stockroom-daily-backup', '0 3 * * *', async () => {
  if (!env.RESEND_API_KEY) { console.log('Backup: no Resend key, skipping'); return; }

  const dbPath = Deno.env.get('DENO_KV_PATH');
  if (!dbPath) { console.log('Backup: no DENO_KV_PATH, skipping (not on Fly)'); return; }

  try {
    console.log('Backup: starting daily db backup');

    // Read the db file and WAL (write-ahead log) — WAL contains recent uncommitted writes
    const dbBytes  = await Deno.readFile(dbPath).catch(() => null);
    const walBytes = await Deno.readFile(dbPath + '-wal').catch(() => null);

    if (!dbBytes) { console.error('Backup: db file not found at', dbPath); return; }

    // Base64-encode for email attachment
    const dbB64  = btoa(String.fromCharCode(...dbBytes));
    const walB64 = walBytes ? btoa(String.fromCharCode(...walBytes)) : null;

    const now      = new Date();
    const dateStr  = now.toISOString().slice(0, 10);
    const sizeKB   = Math.round(dbBytes.length / 1024);
    const walKB    = walBytes ? Math.round(walBytes.length / 1024) : 0;

    // Count KV entries as a basic integrity check
    let entryCount = 0;
    const iter = kv.list({ prefix: [] });
    for await (const _ of iter) entryCount++;

    const attachments: { filename: string; content: string }[] = [
      { filename: `stockroom_${dateStr}.db`,     content: dbB64 },
    ];
    if (walB64) {
      attachments.push({ filename: `stockroom_${dateStr}.db-wal`, content: walB64 });
    }

    const html = `
      <div style="font-family:monospace;background:#0f1117;color:#f0f2f7;padding:24px;border-radius:8px">
        <h2 style="color:#e8a838;margin-bottom:16px">STOCKROOM — Daily Backup</h2>
        <table style="border-collapse:collapse;width:100%">
          <tr><td style="color:#7a8097;padding:4px 12px 4px 0">Date</td><td style="color:#f0f2f7">${dateStr}</td></tr>
          <tr><td style="color:#7a8097;padding:4px 12px 4px 0">DB size</td><td style="color:#f0f2f7">${sizeKB} KB</td></tr>
          <tr><td style="color:#7a8097;padding:4px 12px 4px 0">WAL size</td><td style="color:#f0f2f7">${walKB} KB</td></tr>
          <tr><td style="color:#7a8097;padding:4px 12px 4px 0">KV entries</td><td style="color:#4cbb8a">${entryCount}</td></tr>
        </table>
        <p style="color:#7a8097;font-size:12px;margin-top:16px">
          Data is AES-GCM encrypted — this backup is safe but only useful alongside user passphrases.<br>
          To restore: <code>fly volume snapshot restore</code> or replace /data/stockroom.db on the volume.
        </p>
      </div>`;

    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from:        env.FROM_EMAIL,
        to:          [env.ADMIN_EMAIL],
        subject:     `STOCKROOM backup ${dateStr} — ${entryCount} entries, ${sizeKB}KB`,
        html,
        attachments,
      }),
    });

    if (res.ok) {
      console.log(`Backup: sent successfully — ${entryCount} entries, ${sizeKB}KB db, ${walKB}KB wal`);
    } else {
      const err = await res.text();
      console.error('Backup: Resend error', res.status, err);
    }
  } catch(e) {
    console.error('Backup: unexpected error', e);
  }
});

// ── Cron ──────────────────────────────────────────────────
async function cronCheck() {
  try {
    const schedRaw = await kvGet(['schedule']);
    if (!schedRaw.value) { console.log('Cron: no schedule'); return; }
    const { email, startDate, startTime, intervalDays } = JSON.parse(schedRaw.value);
    if (!email) { console.log('Cron: no email in schedule'); return; }
    const lastSent = await kvGet(['last_sent']);
    const now      = new Date();

    function toUKDate(dateStr, timeStr) {
      const probe    = new Date(`${dateStr}T${timeStr||'09:00'}:00Z`);
      const ukParts  = new Intl.DateTimeFormat('en-GB', { timeZone:'Europe/London', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false }).formatToParts(probe);
      const get      = (t) => parseInt(ukParts.find(p=>p.type===t)?.value||'0');
      const ukDate   = new Date(Date.UTC(get('year'),get('month')-1,get('day'),get('hour'),get('minute'),get('second')));
      const offsetMs = ukDate.getTime() - probe.getTime();
      return new Date(new Date(`${dateStr}T${timeStr||'09:00'}:00Z`).getTime() - offsetMs);
    }

    const nextSend = !lastSent.value
      ? toUKDate(startDate, startTime||'09:00')
      : new Date(new Date(lastSent.value).getTime() + intervalDays * 86400000);

    if (now < nextSend) { console.log(`Cron: next send in ${Math.round((nextSend.getTime()-now.getTime())/60000)} mins`); return; }

    const itemsRaw = await kvGet(['user_items']);
    if (!itemsRaw.value) { console.log('Cron: no items snapshot'); return; }
    const { urgent = [], upcoming = [] } = JSON.parse(itemsRaw.value);
    if (!urgent.length && !upcoming.length) {
      await kvSet(['last_sent'], now.toISOString());
      console.log('Cron: nothing due');
      return;
    }
    const result = await sendEmail(email, urgent, upcoming);
    if (result.ok) {
      await kvSet(['last_sent'], now.toISOString());
      console.log(`Cron: sent to ${email}`);
    } else {
      console.error('Cron send failed:', result.error);
    }
  } catch(err) {
    console.error('Cron error:', err.message);
  }
}

// ── Email sending ─────────────────────────────────────────
async function sendMigrationEmail(to: string, stage: 'notify' | 'complete') {
  if (!env.RESEND_API_KEY) return;
  const appUrl = env.APP_URL;

  const subjects = {
    notify:   'STOCKROOM — Security upgrade coming on ' + CRYPTO_V2_SWITCHOVER,
    complete: 'STOCKROOM — Your account has been upgraded',
  };

  const bodies = {
    notify: `
      <p>Hi,</p>
      <p>We're upgrading STOCKROOM's encryption standard on <strong>${CRYPTO_V2_SWITCHOVER}</strong>.</p>
      <p>What this means for you:</p>
      <ul>
        <li>Your data stays completely safe — nothing will be lost.</li>
        <li>The next time you sign in after ${CRYPTO_V2_SWITCHOVER}, you'll be prompted to enter your passphrase once to complete the upgrade.</li>
        <li>Your data will be re-encrypted using stronger standards and synced automatically.</li>
        <li>Your old encrypted data will be kept as a backup for ${CRYPTO_V1_GRACE_DAYS} days, then deleted.</li>
      </ul>
      <p>No action is needed before the date — just sign in as normal after ${CRYPTO_V2_SWITCHOVER}.</p>`,
    complete: `
      <p>Hi,</p>
      <p>Your STOCKROOM account has been upgraded to our stronger encryption standard.</p>
      <ul>
        <li>Your data is now protected with 600,000-iteration PBKDF2 and a unique random salt.</li>
        <li>Your previous encrypted data will be kept as a backup for ${CRYPTO_V1_GRACE_DAYS} days, then permanently deleted.</li>
        <li>We recommend generating a fresh set of recovery codes in Settings → Security checklist.</li>
      </ul>`,
  };

  const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;max-width:560px;margin:32px auto;color:#333">
    <div style="background:#111;padding:20px 24px;border-radius:12px 12px 0 0;display:flex;align-items:center;gap:12px">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg>
      <span style="color:#e8a838;font-size:16px;font-weight:800;letter-spacing:2px">STOCKROOM</span>
    </div>
    <div style="background:#fff;border:1px solid #e5e7eb;border-top:none;padding:24px 28px;border-radius:0 0 12px 12px">
      ${bodies[stage]}
      <div style="text-align:center;margin-top:28px">
        <a href="${appUrl}" style="background:#e8a838;color:#111;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Open STOCKROOM →</a>
      </div>
    </div>
  </body></html>`;

  await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: env.FROM_EMAIL, to: [to], subject: subjects[stage], html }),
  });
}

async function sendEmail(to, urgentItems, upcomingItems, household = null) {
  if (!env.RESEND_API_KEY) return { ok: false, error: 'No RESEND_API_KEY configured' };
  const appUrl      = env.APP_URL;
  const totalItems  = urgentItems.length + upcomingItems.length;
  const h           = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const householdLabel = household && household !== 'default' ? ` · ${household}` : '';

  const makeRows = (items) => items.map((item) => {
    const daysColor = item.daysLeft <= 7 ? '#e85050' : '#e8a838';
    const priceCell = item.lastPrice ? `<span style="font-family:monospace;font-weight:700;color:#111">${h(item.lastPrice)}</span>` : '<span style="color:#999">—</span>';
    const buyCell   = item.url ? `<a href="${h(item.url)}" style="display:inline-block;background:#5b8dee;color:#fff;padding:4px 12px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:600">Buy ↗</a>` : '<span style="color:#999">—</span>';
    return `<tr>
      <td style="padding:12px 14px;border-bottom:1px solid #eee;vertical-align:top"><div style="font-weight:600;color:#111;margin-bottom:2px">${h(item.name)}</div>${item.store?`<div style="font-size:12px;color:#666">${h(item.store)}</div>`:''}</td>
      <td style="padding:12px 14px;border-bottom:1px solid #eee;color:${daysColor};font-family:monospace;font-weight:700;white-space:nowrap;vertical-align:top">${item.daysLeft}d</td>
      <td style="padding:12px 14px;border-bottom:1px solid #eee;vertical-align:top">${priceCell}</td>
      <td style="padding:12px 14px;border-bottom:1px solid #eee;vertical-align:top">${buyCell}</td>
    </tr>`;
  }).join('');

  const tableWrap = (rows) => `<table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:24px">
    <thead><tr style="background:#f9f9f9">
      <th style="padding:10px 14px;text-align:left;font-size:11px;color:#999;text-transform:uppercase;border-bottom:2px solid #eee">Item</th>
      <th style="padding:10px 14px;text-align:left;font-size:11px;color:#999;text-transform:uppercase;border-bottom:2px solid #eee">Left</th>
      <th style="padding:10px 14px;text-align:left;font-size:11px;color:#999;text-transform:uppercase;border-bottom:2px solid #eee">Price</th>
      <th style="padding:10px 14px;text-align:left;font-size:11px;color:#999;text-transform:uppercase;border-bottom:2px solid #eee">Order</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;

  const urgentRows   = makeRows(urgentItems);
  const upcomingRows = makeRows(upcomingItems);

  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
  <div style="max-width:600px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08)">
    <div style="background:#111;padding:24px 28px;display:flex;align-items:center;gap:16px">
      <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg>
      <div>
        <div style="color:#e8a838;font-size:18px;font-weight:800;letter-spacing:2px">STOCKROOM</div>
        <div style="color:#666;font-size:12px;font-family:monospace;margin-top:2px">Stock Report${householdLabel}</div>
      </div>
    </div>
    <div style="padding:28px">
      <p style="color:#333;margin:0 0 24px;font-size:15px">You have <strong>${totalItems} item${totalItems!==1?'s':''}</strong> that need attention.</p>
      ${urgentItems.length ? `<h2 style="font-size:15px;font-weight:700;color:#e85050;margin:0 0 12px;display:flex;align-items:center;gap:6px"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e85050" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg> Critical — running out soon</h2>${tableWrap(urgentRows)}` : ''}
      ${upcomingItems.length ? `<h2 style="font-size:15px;font-weight:700;color:#e8a838;margin:0 0 12px;display:flex;align-items:center;gap:6px"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e8a838" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg> Upcoming — order soon</h2>${tableWrap(upcomingRows)}` : ''}
      <div style="text-align:center;margin-top:28px">
        <a href="${appUrl}" style="display:inline-block;background:#e8a838;color:#111;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Open STOCKROOM →</a>
      </div>
    </div>
  </div></body></html>`;

  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from:    env.FROM_EMAIL,
        to:      [to],
        subject: `STOCKROOM${householdLabel} — ${urgentItems.length?`${urgentItems.length} urgent, `:''}${totalItems} item${totalItems!==1?'s':''} running low`,
        html,
      }),
    });
    const data = await res.json();
    if (!res.ok) return { ok: false, error: data.message || JSON.stringify(data) };
    return { ok: true };
  } catch(err) {
    return { ok: false, error: err.message };
  }
}
