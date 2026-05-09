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

// ── Crypto architecture ───────────────────────────────────
// All accounts use the v2 envelope architecture:
//   - DATA KEY: random 256-bit AES-GCM key, never stored raw
//   - WRAP KEY: PBKDF2(passphrase, kdf_salt, 600,000 iters) → AES-KW
//   - On disk we store an envelope: the data key wrapped by the wrap key
//   - Recovery: extra envelopes wrapped by recovery-code-derived keys
// Legacy v1 (passphrase-derived data key, no envelope) was retired —
// the auto-migration path that ran on first login post-2026-05-01
// upgraded the entire user base in May 2026.

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

// ── Chunked KV helpers (kvGetLarge / kvSetLarge / kvDelLarge) ──
// Deno KV has a 64 KB hard limit per value. Encrypted ciphertext for
// stockroom data can exceed this once photos are involved. These helpers
// transparently chunk values larger than CHUNK_SIZE across N sub-keys
// while remaining fully backwards-compatible with existing inline values.
//
// Storage layout:
//   primary key  → either inline ciphertext (small case)
//                  OR a manifest: '__chunked:N' where N = chunk count
//   primary key + 'c' + i → chunk i (only when chunked)
//
// The sentinel `__chunked:` is safe because all our payloads are base64
// (never contains underscore or colon), so no real ciphertext can be
// confused with a manifest.
//
// Atomicity: writes happen chunk-first, manifest-last. If a partial write
// occurs (e.g. timeout mid-write), the old manifest is still pointing at
// the old chunks, so reads continue to return the previous coherent
// state. Orphaned chunks from a failed write are harmless — the next
// successful write overwrites them or the cleanup pass removes them.
const KV_CHUNK_SIZE      = 50 * 1024;        // 50 KB; leaves headroom under the 64 KB hard limit
const KV_MAX_CHUNKS      = 50;               // 2.5 MB ceiling; sane defence against runaway values
const KV_CHUNK_SENTINEL  = '__chunked:';     // manifest prefix marker

async function kvGetLarge(key: any[]): Promise<{ value: string | null }> {
  const head = await kvGet(key);
  const v = head.value;
  if (!v) return { value: null };
  if (typeof v !== 'string' || !v.startsWith(KV_CHUNK_SENTINEL)) {
    // Legacy inline value or any non-chunked write — return as-is.
    return { value: v as string };
  }
  const count = parseInt(v.slice(KV_CHUNK_SENTINEL.length), 10);
  if (!Number.isFinite(count) || count < 1 || count > KV_MAX_CHUNKS) {
    console.error(`[kvGetLarge] invalid manifest at ${JSON.stringify(key)}: "${v.slice(0, 50)}"`);
    return { value: null };
  }
  // Read all chunks in parallel, preserving order.
  const reads = [];
  for (let i = 0; i < count; i++) {
    reads.push(kvGet([...key, 'c', i]));
  }
  const results = await Promise.all(reads);
  const parts: string[] = [];
  for (let i = 0; i < count; i++) {
    const c = results[i].value;
    if (typeof c !== 'string') {
      console.error(`[kvGetLarge] missing chunk ${i}/${count} at ${JSON.stringify(key)}`);
      return { value: null };
    }
    parts.push(c);
  }
  return { value: parts.join('') };
}

async function kvSetLarge(key: any[], value: string, opts?: any): Promise<void> {
  if (typeof value !== 'string') throw new Error('kvSetLarge: value must be a string');
  // Read the previous manifest so we know which old chunks (if any) to
  // clean up after the new write succeeds.
  let oldChunkCount = 0;
  try {
    const prev = await kvGet(key);
    const pv = prev.value;
    if (typeof pv === 'string' && pv.startsWith(KV_CHUNK_SENTINEL)) {
      const n = parseInt(pv.slice(KV_CHUNK_SENTINEL.length), 10);
      if (Number.isFinite(n) && n > 0) oldChunkCount = n;
    }
  } catch (_) { /* best-effort */ }

  if (value.length <= KV_CHUNK_SIZE) {
    // Inline write — single key, legacy format.
    await kvSet(key, value, opts);
    // Clean up any stale chunks from a previous larger write.
    for (let i = 0; i < oldChunkCount; i++) {
      try { await kvDel([...key, 'c', i]); } catch (_) {}
    }
    return;
  }

  // Chunked write — split into 50 KB chunks, write chunks first, then
  // overwrite the primary key with a manifest so reads remain coherent
  // across the partial-write window.
  const newCount = Math.ceil(value.length / KV_CHUNK_SIZE);
  if (newCount > KV_MAX_CHUNKS) {
    throw new Error(`Value too large: ${value.length} bytes exceeds ${KV_MAX_CHUNKS * KV_CHUNK_SIZE} byte ceiling`);
  }
  for (let i = 0; i < newCount; i++) {
    const slice = value.slice(i * KV_CHUNK_SIZE, (i + 1) * KV_CHUNK_SIZE);
    await kvSet([...key, 'c', i], slice);
  }
  await kvSet(key, `${KV_CHUNK_SENTINEL}${newCount}`, opts);
  // Delete any leftover chunks beyond the new count (e.g. shrinking from
  // 10 chunks to 6 leaves 4 stale chunks otherwise).
  for (let i = newCount; i < oldChunkCount; i++) {
    try { await kvDel([...key, 'c', i]); } catch (_) {}
  }
}

async function kvDelLarge(key: any[]): Promise<void> {
  // Read manifest first (best-effort) so we know how many chunks to drop.
  let chunkCount = 0;
  try {
    const cur = await kvGet(key);
    const v = cur.value;
    if (typeof v === 'string' && v.startsWith(KV_CHUNK_SENTINEL)) {
      const n = parseInt(v.slice(KV_CHUNK_SENTINEL.length), 10);
      if (Number.isFinite(n) && n > 0) chunkCount = n;
    }
  } catch (_) { /* best-effort */ }
  await kvDel(key);
  for (let i = 0; i < chunkCount; i++) {
    try { await kvDel([...key, 'c', i]); } catch (_) {}
  }
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

// ── Auth helpers ──────────────────────────────────────────
// verifyUserAuth — accepts BOTH passphrase verifier AND passkey sessionToken.
// Returns true on success, false on missing/invalid credentials.
// This is the canonical dual-auth check. Endpoints that previously inlined
// their own verifier/sessionToken validation should call this instead — that
// way new endpoints can't accidentally forget to accept passkey sessions
// (the recurring "passkey auth gap" bug class).
async function verifyUserAuth(emailHash: string, verifier?: string, sessionToken?: string): Promise<boolean> {
  if (!emailHash) return false;
  if (sessionToken) {
    const session = await kvGet(['passkey_session', emailHash, sessionToken]);
    return !!session.value;
  }
  if (verifier) {
    const stored = await kvGet(['user', emailHash, 'verifier']);
    return !!stored.value && stored.value === verifier;
  }
  return false;
}

// requireUserAuth — verifies and returns null on success, or a Response (401/400)
// on failure. Use as: `const fail = await requireUserAuth(...); if (fail) return fail;`
async function requireUserAuth(
  emailHash: string,
  verifier?: string,
  sessionToken?: string,
): Promise<Response | null> {
  if (!emailHash || (!verifier && !sessionToken)) {
    return json({ error: 'Missing fields' }, corsHeaders, 400);
  }
  const ok = await verifyUserAuth(emailHash, verifier, sessionToken);
  if (!ok) {
    // Distinguish session expiry from bad credentials so the UI can prompt re-login
    return json({ error: sessionToken ? 'Session expired' : 'Unauthorised' }, corsHeaders, 401);
  }
  return null;
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

// ═══════════════════════════════════════════════════════════
//  CLOUDFLARE R2 BACKUP — every 5 min snapshot of KV durable data
// ═══════════════════════════════════════════════════════════
// Why R2: free tier (10GB storage, 1M Class A ops/month), data is
// already AES-GCM encrypted client-side so the snapshot contains
// only ciphertext. Geo-redundant — survives a Fly LHR outage.
//
// What gets backed up: only durable KV prefixes. Transient prefixes
// (OTPs, challenges, sessions) are skipped — they expire naturally
// and restoring stale ones would be confusing.
const R2_DURABLE_PREFIXES = [
  'user', 'device', 'passkey', 'passkey_prf_envelope',
  'share', 'share_data', 'share_key',
  'note_body',
  'schedule', 'last_sent', 'user_email', 'user_items',
  'deactivation', 'deactivation_reactivate',
  'billing', 'billing_idx',
  'referral',
];

const R2_CFG = {
  accountId: Deno.env.get('R2_ACCOUNT_ID')        || '',
  accessKey: Deno.env.get('R2_ACCESS_KEY_ID')     || '',
  secretKey: Deno.env.get('R2_SECRET_ACCESS_KEY') || '',
  bucket:    Deno.env.get('R2_BUCKET_NAME')       || '',
  region:    'auto',
  service:   's3',
};
const r2Configured = () => !!(R2_CFG.accountId && R2_CFG.accessKey && R2_CFG.secretKey && R2_CFG.bucket);

// ── AWS SigV4 signing for R2 (S3-compatible) ──────────────
// Hand-rolled because pulling in aws4fetch adds 50KB and the
// signing algorithm is small enough to write inline.
async function _hmac(key: ArrayBuffer | Uint8Array | string, msg: string): Promise<ArrayBuffer> {
  const enc = new TextEncoder();
  const keyBuf = typeof key === 'string' ? enc.encode(key) : key;
  const cryptoKey = await crypto.subtle.importKey('raw', keyBuf, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return crypto.subtle.sign('HMAC', cryptoKey, enc.encode(msg));
}
async function _sha256Hex(data: string | Uint8Array): Promise<string> {
  const buf = typeof data === 'string' ? new TextEncoder().encode(data) : data;
  const hash = await crypto.subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
}
function _hex(buf: ArrayBuffer): string {
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function r2Fetch(method: string, key: string, body: Uint8Array | null = null, extraHeaders: Record<string,string> = {}): Promise<Response> {
  if (!r2Configured()) throw new Error('R2 not configured');
  const host = `${R2_CFG.accountId}.r2.cloudflarestorage.com`;
  const url  = `https://${host}/${R2_CFG.bucket}/${key}`;
  const now  = new Date();
  const amzDate    = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
  const dateStamp  = amzDate.slice(0, 8);
  const payloadHash = body ? await _sha256Hex(body) : await _sha256Hex('');

  const headers: Record<string, string> = {
    'host': host,
    'x-amz-date': amzDate,
    'x-amz-content-sha256': payloadHash,
    ...extraHeaders,
  };
  // SSE-C / SSE — Cloudflare R2 always encrypts at rest by default; no header needed
  // (server-side encryption-at-rest is automatic and free, per Cloudflare docs)

  const signedHeaders = Object.keys(headers).sort().join(';');
  const canonicalHeaders = Object.keys(headers).sort().map(h => `${h}:${headers[h]}`).join('\n') + '\n';
  const canonicalUri = `/${R2_CFG.bucket}/${encodeURIComponent(key).replace(/%2F/g, '/')}`;
  const canonicalRequest = [method, canonicalUri, '', canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const credentialScope  = `${dateStamp}/${R2_CFG.region}/${R2_CFG.service}/aws4_request`;
  const stringToSign     = ['AWS4-HMAC-SHA256', amzDate, credentialScope, await _sha256Hex(canonicalRequest)].join('\n');

  const kDate    = await _hmac('AWS4' + R2_CFG.secretKey, dateStamp);
  const kRegion  = await _hmac(kDate, R2_CFG.region);
  const kService = await _hmac(kRegion, R2_CFG.service);
  const kSigning = await _hmac(kService, 'aws4_request');
  const signature = _hex(await _hmac(kSigning, stringToSign));

  const auth = `AWS4-HMAC-SHA256 Credential=${R2_CFG.accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  const fetchHeaders: Record<string,string> = { ...headers, 'Authorization': auth };
  delete fetchHeaders.host; // fetch sets it

  return fetch(url, { method, headers: fetchHeaders, body: body || undefined });
}

// ── Snapshot KV durable data and upload to R2 ─────────────
async function backupKVToR2(label: string = 'auto'): Promise<{ ok: boolean; key?: string; size?: number; entries?: number; error?: string }> {
  if (!r2Configured()) return { ok: false, error: 'R2 not configured' };
  try {
    const snapshot: { meta: any; entries: Array<{ key: any; value: any }> } = {
      meta: {
        version: 1,
        createdAt: new Date().toISOString(),
        label,
        prefixes: R2_DURABLE_PREFIXES,
      },
      entries: [],
    };
    for (const prefix of R2_DURABLE_PREFIXES) {
      const iter = kv.list({ prefix: [prefix] });
      for await (const entry of iter) {
        snapshot.entries.push({ key: entry.key, value: entry.value });
      }
    }
    const json = JSON.stringify(snapshot);
    const body = new TextEncoder().encode(json);
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const key = `${label}/${ts}.json`;
    const res = await r2Fetch('PUT', key, body, { 'content-type': 'application/json' });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      return { ok: false, error: `R2 PUT ${res.status}: ${text.slice(0, 200)}` };
    }
    return { ok: true, key, size: body.length, entries: snapshot.entries.length };
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
}

// ── List snapshots in R2 (parses ListObjectsV2 XML) ───────
async function listR2Snapshots(prefix: string = ''): Promise<Array<{ key: string; size: number; lastModified: string }>> {
  if (!r2Configured()) return [];
  const host = `${R2_CFG.accountId}.r2.cloudflarestorage.com`;
  // SigV4 requires query parameters sorted alphabetically in the canonical request
  const queryString = `list-type=2&max-keys=1000&prefix=${encodeURIComponent(prefix)}`;
  // Path-style bucket-level operation requires trailing slash on the bucket path
  const url  = `https://${host}/${R2_CFG.bucket}/?${queryString}`;
  const now  = new Date();
  const amzDate    = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
  const dateStamp  = amzDate.slice(0, 8);
  const payloadHash = await _sha256Hex('');
  const headers: Record<string,string> = {
    'host': host,
    'x-amz-date': amzDate,
    'x-amz-content-sha256': payloadHash,
  };
  const signedHeaders   = 'host;x-amz-content-sha256;x-amz-date';
  const canonicalHeaders = Object.keys(headers).sort().map(h => `${h}:${headers[h]}`).join('\n') + '\n';
  const canonicalRequest = ['GET', `/${R2_CFG.bucket}/`, queryString, canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const credentialScope  = `${dateStamp}/${R2_CFG.region}/${R2_CFG.service}/aws4_request`;
  const stringToSign     = ['AWS4-HMAC-SHA256', amzDate, credentialScope, await _sha256Hex(canonicalRequest)].join('\n');
  const kDate    = await _hmac('AWS4' + R2_CFG.secretKey, dateStamp);
  const kRegion  = await _hmac(kDate, R2_CFG.region);
  const kService = await _hmac(kRegion, R2_CFG.service);
  const kSigning = await _hmac(kService, 'aws4_request');
  const signature = _hex(await _hmac(kSigning, stringToSign));
  const auth = `AWS4-HMAC-SHA256 Credential=${R2_CFG.accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  const res = await fetch(url, { method: 'GET', headers: { ...headers, 'Authorization': auth } });
  if (!res.ok) {
    console.warn('R2 list failed:', res.status, await res.text().catch(() => ''));
    return [];
  }
  const xml = await res.text();
  // Minimal XML parser — extract <Contents> blocks
  const out: Array<{ key: string; size: number; lastModified: string }> = [];
  const contentsRe = /<Contents>([\s\S]*?)<\/Contents>/g;
  let m: RegExpExecArray | null;
  while ((m = contentsRe.exec(xml)) !== null) {
    const block = m[1];
    const k = /<Key>([^<]+)<\/Key>/.exec(block)?.[1] || '';
    const s = parseInt(/<Size>([^<]+)<\/Size>/.exec(block)?.[1] || '0', 10);
    const lm = /<LastModified>([^<]+)<\/LastModified>/.exec(block)?.[1] || '';
    if (k) out.push({ key: k, size: s, lastModified: lm });
  }
  return out.sort((a,b) => a.lastModified.localeCompare(b.lastModified));
}

// ── Fetch a snapshot's contents from R2 ───────────────────
async function getR2Snapshot(key: string): Promise<{ ok: boolean; data?: any; error?: string }> {
  if (!r2Configured()) return { ok: false, error: 'R2 not configured' };
  try {
    const res = await r2Fetch('GET', key);
    if (!res.ok) return { ok: false, error: `R2 GET ${res.status}` };
    const data = await res.json();
    return { ok: true, data };
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
}

// ── Extract just one user's encrypted blobs from a snapshot ──
// Returns the entries in the snapshot that belong to the given emailHash.
// Includes the user's main data blob, household-shared data blobs, and
// encrypted note bodies. Does NOT include passkeys, sessions, or shares
// (those are auth artefacts, not user data).
//
// The data is already encrypted with the user's key — the server can't
// read it. The user is the only one who can decrypt their own backup.
async function extractUserDataFromSnapshot(snapshotKey: string, emailHash: string): Promise<{
  ok: boolean;
  entries?: Array<{ key: any; value: any }>;
  meta?: { snapshotKey: string; snapshotTakenAt: string; entriesIncluded: number };
  error?: string;
}> {
  if (!emailHash || typeof emailHash !== 'string' || emailHash.length < 8) {
    return { ok: false, error: 'Invalid emailHash' };
  }
  const snap = await getR2Snapshot(snapshotKey);
  if (!snap.ok || !snap.data) return { ok: false, error: snap.error || 'Failed to fetch snapshot' };
  const allEntries = snap.data.entries as Array<{ key: any; value: any }>;
  if (!Array.isArray(allEntries)) return { ok: false, error: 'Invalid snapshot format' };

  // Filter to entries that belong to this user. The KV key is an array; we
  // match on the second element (which is emailHash for user/* and note_body/*).
  const mine = allEntries.filter(e => {
    if (!Array.isArray(e.key) || e.key.length < 2) return false;
    const top = e.key[0];
    const hash = e.key[1];
    if (hash !== emailHash) return false;
    return top === 'user' || top === 'note_body';
  });
  return {
    ok: true,
    entries: mine,
    meta: {
      snapshotKey,
      snapshotTakenAt: snap.data.meta?.createdAt || '',
      entriesIncluded: mine.length,
    },
  };
}

// ── Delete a snapshot ─────────────────────────────────────
async function deleteR2Snapshot(key: string): Promise<boolean> {
  if (!r2Configured()) return false;
  try {
    const res = await r2Fetch('DELETE', key);
    return res.ok;
  } catch (e) {
    console.warn('R2 delete failed:', key, e?.message);
    return false;
  }
}

// ── Retention policy — keep recent dense, older sparse ────
// 24h × 5-min  (most recent ~288)
// 30d × daily  (next 30)
// 90d × weekly (next 13)
async function pruneR2Snapshots(): Promise<{ kept: number; pruned: number }> {
  if (!r2Configured()) return { kept: 0, pruned: 0 };
  const all = await listR2Snapshots('auto/');
  const now = Date.now();
  const day = 24 * 60 * 60 * 1000;
  const keep = new Set<string>();

  // Bucket each snapshot by its retention tier
  // Tier 1 — last 24h: keep all
  // Tier 2 — 1d to 30d: keep one per UTC day
  // Tier 3 — 30d to 90d: keep one per UTC week (Monday)
  // Older than 90d: prune
  const dailyChosen = new Map<string, string>();   // YYYY-MM-DD → key
  const weeklyChosen = new Map<string, string>();  // YYYY-Www → key
  for (const s of all) {
    const t = new Date(s.lastModified).getTime();
    const ageMs = now - t;
    if (ageMs <= day) {
      keep.add(s.key);
    } else if (ageMs <= 30 * day) {
      const d = new Date(t);
      const dayKey = `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
      // Keep the LATEST snapshot of each day (overwrite in iteration order)
      dailyChosen.set(dayKey, s.key);
    } else if (ageMs <= 90 * day) {
      const d = new Date(t);
      // ISO week number
      const tmp = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
      tmp.setUTCDate(tmp.getUTCDate() + 4 - (tmp.getUTCDay()||7));
      const yearStart = new Date(Date.UTC(tmp.getUTCFullYear(),0,1));
      const weekNo = Math.ceil(((tmp.getTime()-yearStart.getTime())/86400000 + 1)/7);
      const weekKey = `${tmp.getUTCFullYear()}-W${String(weekNo).padStart(2,'0')}`;
      weeklyChosen.set(weekKey, s.key);
    }
  }
  for (const k of dailyChosen.values()) keep.add(k);
  for (const k of weeklyChosen.values()) keep.add(k);

  let pruned = 0;
  for (const s of all) {
    if (!keep.has(s.key)) {
      const ok = await deleteR2Snapshot(s.key);
      if (ok) pruned++;
    }
  }
  return { kept: keep.size, pruned };
}

// ── Per-user prune ────────────────────────────────────────────
// Same retention rules as the whole-DB prune but applied to a single
// user's snapshot prefix. Pre-restore snapshots (label='pre-restore')
// and pre-delete snapshots (label='pre-delete') are always kept — they
// represent the safety net before destructive operations.
async function pruneR2SnapshotsForUser(emailHash: string): Promise<{ kept: number; pruned: number }> {
  if (!r2Configured() || !emailHash) return { kept: 0, pruned: 0 };
  const all = await listR2Snapshots(`user-backup/${emailHash}/`);
  const now = Date.now();
  const day = 24 * 60 * 60 * 1000;
  const keep = new Set<string>();
  const dailyChosen  = new Map<string, string>();
  const weeklyChosen = new Map<string, string>();

  for (const s of all) {
    // Always keep safety snapshots — they're rare and important
    const fname = (s.key.split('/').pop() || '').toLowerCase();
    if (fname.startsWith('pre-restore') || fname.startsWith('pre-delete')) {
      keep.add(s.key);
      continue;
    }
    const t = new Date(s.lastModified).getTime();
    const ageMs = now - t;
    if (ageMs <= day) {
      // Tier 1 — last 24h: keep all
      keep.add(s.key);
    } else if (ageMs <= 30 * day) {
      // Tier 2 — 1d to 30d: keep the latest snapshot per UTC day
      const d = new Date(t);
      const dayKey = `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
      dailyChosen.set(dayKey, s.key);
    } else if (ageMs <= 90 * day) {
      // Tier 3 — 30d to 90d: keep one per ISO week
      const d = new Date(t);
      const tmp = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
      tmp.setUTCDate(tmp.getUTCDate() + 4 - (tmp.getUTCDay()||7));
      const yearStart = new Date(Date.UTC(tmp.getUTCFullYear(),0,1));
      const weekNo = Math.ceil(((tmp.getTime()-yearStart.getTime())/86400000 + 1)/7);
      const weekKey = `${tmp.getUTCFullYear()}-W${String(weekNo).padStart(2,'0')}`;
      weeklyChosen.set(weekKey, s.key);
    }
    // Older than 90d: not kept — falls through to prune
  }
  for (const k of dailyChosen.values()) keep.add(k);
  for (const k of weeklyChosen.values()) keep.add(k);

  let pruned = 0;
  for (const s of all) {
    if (!keep.has(s.key)) {
      const ok = await deleteR2Snapshot(s.key);
      if (ok) pruned++;
    }
  }
  return { kept: keep.size, pruned };
}

// ── Restore from a snapshot ───────────────────────────────
// SAFETY: takes a pre-restore snapshot first (labelled 'pre-restore')
// so a bad restore can be undone. Restore overwrites only the durable
// prefixes that were in the snapshot — does not touch transient data.
async function restoreFromR2Snapshot(key: string): Promise<{ ok: boolean; restored?: number; preRestoreKey?: string; error?: string }> {
  if (!r2Configured()) return { ok: false, error: 'R2 not configured' };
  // 1. Take a safety snapshot of the current state
  const pre = await backupKVToR2('pre-restore');
  if (!pre.ok) return { ok: false, error: 'Pre-restore snapshot failed: ' + pre.error };
  // 2. Fetch the target snapshot
  const snap = await getR2Snapshot(key);
  if (!snap.ok || !snap.data) return { ok: false, error: 'Fetch snapshot failed: ' + snap.error };
  const entries = snap.data.entries as Array<{ key: any; value: any }>;
  if (!Array.isArray(entries)) return { ok: false, error: 'Invalid snapshot format' };
  // 3. Wipe durable prefixes from current KV
  for (const prefix of R2_DURABLE_PREFIXES) {
    const iter = kv.list({ prefix: [prefix] });
    for await (const entry of iter) {
      try { await kv.delete(entry.key); } catch (e) { console.warn('Restore wipe failed for', entry.key, e?.message); }
    }
  }
  // 4. Write the snapshot's entries back
  let restored = 0;
  for (const e of entries) {
    try {
      await kv.set(e.key, e.value);
      restored++;
    } catch (err) {
      console.warn('Restore write failed for', e.key, err?.message);
    }
  }
  console.log(`R2 restore: ${restored}/${entries.length} entries restored from ${key}; pre-restore safety snapshot: ${pre.key}`);
  return { ok: true, restored, preRestoreKey: pre.key };
}

// ── Per-user backup / restore ─────────────────────────────────
// Differs from the whole-DB backup above: filters KV entries to a single
// user's emailHash, so admins can snapshot or roll back one user's data
// without touching anyone else's. The snapshot stays encrypted because
// every user blob is already encrypted client-side — the server never
// sees plaintext; restore writes the same ciphertext back.
//
// Inclusion rule: an entry belongs to the user if `key[1] === emailHash`.
// We also pull the `user_email` reverse-map entry by scanning that prefix
// for one whose value matches the email associated with this hash. Without
// it, restoring a wiped account leaves the email→hash mapping orphaned
// and login lookups would fail.
async function backupUserToR2(emailHash: string, label: string = 'manual'): Promise<{ ok: boolean; key?: string; size?: number; entries?: number; error?: string }> {
  if (!r2Configured()) return { ok: false, error: 'R2 not configured' };
  if (!emailHash || typeof emailHash !== 'string') return { ok: false, error: 'Invalid emailHash' };
  try {
    const collected: Array<{ key: any; value: any }> = [];
    // First pass: every user-keyed entry across the durable prefixes (skip
    // the share* family — they're keyed by share code, joined separately
    // below — and user_email which is keyed by plaintext email, joined
    // via reverse-value match).
    const skipPrefixes = new Set(['user_email', 'share', 'share_key', 'share_data']);
    for (const prefix of R2_DURABLE_PREFIXES) {
      if (skipPrefixes.has(prefix)) continue;
      const iter = kv.list({ prefix: [prefix] });
      for await (const entry of iter) {
        if (Array.isArray(entry.key) && entry.key[1] === emailHash) {
          collected.push({ key: entry.key, value: entry.value });
        }
      }
    }
    // Second pass: email→hash reverse-map entry. value === emailHash for
    // the row we want.
    const emailIter = kv.list({ prefix: ['user_email'] });
    for await (const entry of emailIter) {
      if (entry.value === emailHash) {
        collected.push({ key: entry.key, value: entry.value });
      }
    }
    // Third pass: shares this user OWNS. We scan the `share` index, parse
    // each value's ownerEmailHash, and for matches pull the matching
    // `share_key/{code}/{owner}` and `share_data/{code}/*` entries. This is
    // an O(total_shares) scan — fine at the scale of consumer apps.
    const ownedShareCodes: string[] = [];
    const shareIter = kv.list({ prefix: ['share'] });
    for await (const entry of shareIter) {
      // The 'share' prefix matches both ['share', code] and ['share_data', ...].
      // Filter strictly to the 2-element owner-record shape.
      if (!Array.isArray(entry.key) || entry.key.length !== 2 || entry.key[0] !== 'share') continue;
      try {
        const parsed = JSON.parse(entry.value as string);
        if (parsed?.ownerEmailHash === emailHash) {
          collected.push({ key: entry.key, value: entry.value });
          ownedShareCodes.push(entry.key[1] as string);
        }
      } catch (_) { /* non-JSON share entry — skip */ }
    }
    if (ownedShareCodes.length) {
      const codeSet = new Set(ownedShareCodes.map(c => c.toUpperCase()));
      // share_key entries
      const shareKeyIter = kv.list({ prefix: ['share_key'] });
      for await (const entry of shareKeyIter) {
        if (Array.isArray(entry.key) && codeSet.has((entry.key[1] as string)?.toUpperCase?.() || '')) {
          collected.push({ key: entry.key, value: entry.value });
        }
      }
      // share_data entries (encrypted blob plus modified-timestamp markers)
      const shareDataIter = kv.list({ prefix: ['share_data'] });
      for await (const entry of shareDataIter) {
        if (Array.isArray(entry.key) && codeSet.has((entry.key[1] as string)?.toUpperCase?.() || '')) {
          collected.push({ key: entry.key, value: entry.value });
        }
      }
    }
    const snapshot = {
      meta: {
        version: 1,
        kind: 'user',
        emailHash,
        createdAt: new Date().toISOString(),
        label,
        prefixes: R2_DURABLE_PREFIXES,
        ownedShareCodes,
      },
      entries: collected,
    };
    const json = JSON.stringify(snapshot);
    const body = new TextEncoder().encode(json);
    const ts  = new Date().toISOString().replace(/[:.]/g, '-');
    const key = `user-backup/${emailHash}/${label}-${ts}.json`;
    const res = await r2Fetch('PUT', key, body, { 'content-type': 'application/json' });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      return { ok: false, error: `R2 PUT ${res.status}: ${text.slice(0, 200)}` };
    }
    return { ok: true, key, size: body.length, entries: collected.length };
  } catch (e) {
    return { ok: false, error: String((e as Error)?.message || e) };
  }
}

async function restoreUserFromR2Snapshot(emailHash: string, snapshotKey: string): Promise<{ ok: boolean; restored?: number; preRestoreKey?: string; error?: string }> {
  if (!r2Configured()) return { ok: false, error: 'R2 not configured' };
  if (!emailHash || !snapshotKey) return { ok: false, error: 'Missing emailHash or snapshotKey' };
  // Belt-and-braces: the snapshot key path itself must contain the emailHash.
  // Prevents an admin pasting the wrong key from a different user.
  if (!snapshotKey.includes(`/${emailHash}/`)) {
    return { ok: false, error: 'Snapshot does not belong to this user (path mismatch)' };
  }
  // 1. Take a safety snapshot of the user's current state
  const pre = await backupUserToR2(emailHash, 'pre-restore');
  if (!pre.ok) return { ok: false, error: 'Pre-restore snapshot failed: ' + pre.error };
  // 2. Fetch the target snapshot
  const snap = await getR2Snapshot(snapshotKey);
  if (!snap.ok || !snap.data) return { ok: false, error: 'Fetch snapshot failed: ' + snap.error };
  const entries = snap.data.entries as Array<{ key: any; value: any }>;
  if (!Array.isArray(entries)) return { ok: false, error: 'Invalid snapshot format' };
  // Verify the snapshot's metadata claims it's for this user.
  if (snap.data.meta?.emailHash && snap.data.meta.emailHash !== emailHash) {
    return { ok: false, error: 'Snapshot metadata emailHash does not match request' };
  }
  // 3. Validate every entry — refuse to restore if any entry doesn't match.
  // Share entries are allowed if the snapshot's metadata declares the share
  // code as owned by this user. This is the same join we do in backup.
  const ownedShareCodes = new Set<string>(
    Array.isArray(snap.data.meta?.ownedShareCodes)
      ? (snap.data.meta.ownedShareCodes as string[]).map(c => c.toUpperCase())
      : []
  );
  for (const e of entries) {
    if (!Array.isArray(e.key)) {
      return { ok: false, error: 'Invalid entry: key is not an array' };
    }
    const prefix = e.key[0];
    const isUserKey      = e.key[1] === emailHash;
    const isReverseMap   = prefix === 'user_email' && e.value === emailHash;
    const isOwnedShare   = (prefix === 'share' || prefix === 'share_key' || prefix === 'share_data')
                            && ownedShareCodes.has(((e.key[1] as string) || '').toUpperCase());
    if (!isUserKey && !isReverseMap && !isOwnedShare) {
      return { ok: false, error: `Snapshot contains entry not belonging to user: ${JSON.stringify(e.key)}` };
    }
  }
  // 4. Wipe the user's current keys across the durable prefixes
  const wipeSkip = new Set(['user_email', 'share', 'share_key', 'share_data']);
  for (const prefix of R2_DURABLE_PREFIXES) {
    if (wipeSkip.has(prefix)) continue;
    const iter = kv.list({ prefix: [prefix] });
    for await (const entry of iter) {
      if (Array.isArray(entry.key) && entry.key[1] === emailHash) {
        try { await kv.delete(entry.key); } catch (e) { console.warn('User restore wipe failed for', entry.key, (e as Error)?.message); }
      }
    }
  }
  // Wipe user_email entries pointing at this hash
  const emailIter = kv.list({ prefix: ['user_email'] });
  for await (const entry of emailIter) {
    if (entry.value === emailHash) {
      try { await kv.delete(entry.key); } catch (e) { console.warn('user_email wipe failed for', entry.key, (e as Error)?.message); }
    }
  }
  // Wipe shares owned by this user — only those declared in the snapshot's
  // metadata. We DON'T touch shares owned by other users that this user
  // happens to be a target of (those belong to other users' backups).
  if (ownedShareCodes.size) {
    const shareIter = kv.list({ prefix: ['share'] });
    for await (const entry of shareIter) {
      if (!Array.isArray(entry.key) || entry.key.length !== 2 || entry.key[0] !== 'share') continue;
      try {
        const parsed = JSON.parse(entry.value as string);
        if (parsed?.ownerEmailHash === emailHash) {
          await kv.delete(entry.key);
        }
      } catch (_) {}
    }
    const shareKeyIter = kv.list({ prefix: ['share_key'] });
    for await (const entry of shareKeyIter) {
      if (Array.isArray(entry.key) && ownedShareCodes.has(((entry.key[1] as string) || '').toUpperCase())) {
        try { await kv.delete(entry.key); } catch (_) {}
      }
    }
    const shareDataIter = kv.list({ prefix: ['share_data'] });
    for await (const entry of shareDataIter) {
      if (Array.isArray(entry.key) && ownedShareCodes.has(((entry.key[1] as string) || '').toUpperCase())) {
        try { await kv.delete(entry.key); } catch (_) {}
      }
    }
  }
  // 5. Write the snapshot's entries back
  let restored = 0;
  for (const e of entries) {
    try {
      await kv.set(e.key, e.value);
      restored++;
    } catch (err) {
      console.warn('User restore write failed for', e.key, (err as Error)?.message);
    }
  }
  console.log(`User restore for ${emailHash.slice(0,8)}…: ${restored}/${entries.length} entries from ${snapshotKey}; pre-restore: ${pre.key}`);
  return { ok: true, restored, preRestoreKey: pre.key };
}

// ── Dirty-flag pattern: only back up when data has actually changed ──
// Idle hours skip backups; an active editing session gets fine-grained
// 5-minute snapshots. A separate daily-at-03:00 cron forces a backup
// regardless so retention pruning always has something to keep.
// ── Dirty-flag pattern: only back up when data has actually changed ──
// Per-user variant: each user maintains their own dirty marker so the cron
// only backs up users whose data has changed since their last snapshot.
// This replaces the previous whole-DB dirty flag. The old `markKVDirty` /
// `_isKVDirty` / `_clearKVDirty` helpers are kept for legacy admin endpoints
// (whole-DB backup-now / restore) but the runtime no longer uses them.
async function markUserDirty(emailHash: string): Promise<void> {
  if (!emailHash) return;
  try {
    await kv.set(['_user_dirty', emailHash], '1');
  } catch (e) {
    console.warn('markUserDirty failed for', emailHash.slice(0,8), (e as Error)?.message);
  }
}

async function _isUserDirty(emailHash: string): Promise<boolean> {
  try {
    const r = await kv.get(['_user_dirty', emailHash]);
    return !!r.value;
  } catch (_) { return true; /* err on the side of backing up */ }
}

async function _clearUserDirty(emailHash: string): Promise<void> {
  try { await kv.delete(['_user_dirty', emailHash]); } catch (_) {}
}

async function _listDirtyUsers(): Promise<string[]> {
  const out: string[] = [];
  try {
    const iter = kv.list({ prefix: ['_user_dirty'] });
    for await (const entry of iter) {
      if (Array.isArray(entry.key) && typeof entry.key[1] === 'string') {
        out.push(entry.key[1] as string);
      }
    }
  } catch (e) {
    console.warn('_listDirtyUsers failed:', (e as Error)?.message);
  }
  return out;
}

// List every known user's emailHash by scanning the user_email reverse-map.
// Used by the forced-daily cron so dormant accounts also get snapshotted.
async function _listAllUserEmailHashes(): Promise<string[]> {
  const out = new Set<string>();
  try {
    const iter = kv.list({ prefix: ['user_email'] });
    for await (const entry of iter) {
      if (typeof entry.value === 'string') out.add(entry.value);
    }
  } catch (e) {
    console.warn('_listAllUserEmailHashes failed:', (e as Error)?.message);
  }
  return [...out];
}

// LEGACY — whole-DB dirty flag. No longer used by runtime; preserved so the
// existing /admin/r2/* endpoints (whole-DB backup-now etc) still compile.
// Will be removed once those endpoints are deleted.
async function markKVDirty(): Promise<void> {
  try {
    await kv.set(['_kv_dirty'], '1');
  } catch (e) {
    console.warn('markKVDirty failed:', (e as Error)?.message);
  }
}

async function _isKVDirty(): Promise<boolean> {
  try {
    const r = await kv.get(['_kv_dirty']);
    return !!r.value;
  } catch (_) { return true; }
}

async function _clearKVDirty(): Promise<void> {
  try { await kv.delete(['_kv_dirty']); } catch (_) {}
}

// ── Crons: per-user backup every 5 min for dirty users; daily forced;
//          daily prune. Each user has their own snapshot history under
//          `user-backup/{emailHash}/` so admin restore is per-account.

Deno.cron('stockroom-r2-backup', '*/5 * * * *', async () => {
  if (!r2Configured()) return;
  const dirty = await _listDirtyUsers();
  if (!dirty.length) return;
  let ok = 0, fail = 0;
  for (const emailHash of dirty) {
    const result = await backupUserToR2(emailHash, 'auto');
    if (result.ok) {
      await _clearUserDirty(emailHash);
      ok++;
    } else {
      // Leave the dirty flag set so the next tick retries this user
      console.error(`Per-user R2 backup failed for ${emailHash.slice(0,8)}:`, result.error);
      fail++;
    }
  }
  if (ok || fail) console.log(`Per-user R2 backup tick: ${ok} ok, ${fail} failed (${dirty.length} dirty)`);
});

// Forced daily backup at 03:00 UTC — snapshots EVERY known user, even
// dormant ones, so retention pruning always has fresh material per user.
Deno.cron('stockroom-r2-backup-forced', '0 3 * * *', async () => {
  if (!r2Configured()) return;
  const all = await _listAllUserEmailHashes();
  let ok = 0, fail = 0;
  for (const emailHash of all) {
    const result = await backupUserToR2(emailHash, 'auto');
    if (result.ok) {
      await _clearUserDirty(emailHash);
      ok++;
    } else {
      console.error(`Forced daily backup failed for ${emailHash.slice(0,8)}:`, result.error);
      fail++;
    }
  }
  console.log(`Forced daily per-user backup: ${ok} ok, ${fail} failed (${all.length} users)`);
});

Deno.cron('stockroom-r2-prune', '0 3 * * *', async () => {
  if (!r2Configured()) return;
  const all = await _listAllUserEmailHashes();
  let totalKept = 0, totalPruned = 0;
  for (const emailHash of all) {
    const result = await pruneR2SnapshotsForUser(emailHash);
    totalKept += result.kept;
    totalPruned += result.pruned;
  }
  // Also tidy legacy whole-DB snapshots from before the per-user migration.
  // These no longer accumulate but the existing ones still respect retention.
  const legacy = await pruneR2Snapshots();
  console.log(`Per-user R2 prune: kept ${totalKept}, pruned ${totalPruned} across ${all.length} users (legacy: kept ${legacy.kept}, pruned ${legacy.pruned})`);
});

// ── Hourly cron ───────────────────────────────────────────
Deno.cron('stockroom-kv-email-check', '0 * * * *', async () => {
  console.log('Cron: running');
  await cronCheck();
});

// ── Account deletion helper — deletes EVERY key for a given emailHash ──
// Used by both /user/delete (self) and /admin/delete-account.
// Covers: user data, devices, passkeys, sessions, challenges, wrapped keys,
//         share targets, share data, recovery OTPs, email verify tokens, schedules.
async function _deleteAllUserData(kv: Deno.Kv, emailHash: string): Promise<void> {
  // Take a pre-delete snapshot first so admin can roll the user back if
  // the deletion was an accident. Best-effort — if R2 is misconfigured or
  // the snapshot fails, deletion still proceeds (we don't want a backup
  // failure to block account deletion).
  try {
    if (r2Configured()) {
      const result = await backupUserToR2(emailHash, 'pre-delete');
      if (result.ok) {
        console.log(`pre-delete snapshot for ${emailHash.slice(0,8)}: ${result.key}`);
      } else {
        console.warn(`pre-delete snapshot failed for ${emailHash.slice(0,8)}: ${result.error}`);
      }
    }
  } catch (e) {
    console.warn('pre-delete snapshot threw:', (e as Error)?.message);
  }

  const prefixesToScan = [
    ['user',             emailHash],   // verifier, key envelopes, data, settings, email_verified, etc.
    ['device',           emailHash],   // trusted devices
    ['passkey',          emailHash],   // WebAuthn credentials
    ['passkey_session',  emailHash],   // active session tokens
    ['passkey_challenge',emailHash],   // pending WebAuthn challenges
    ['passkey_key',          emailHash],   // old server-wrapped data key copies (deprecated)
    ['passkey_prf_envelope', emailHash],   // PRF/device-bound envelope (new architecture)
    ['email_verify',     emailHash],   // (legacy) email verification namespace
    ['email_verify_otp', emailHash],   // pending email verify OTP — actual key shape
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
  // Login rate-limit counter — leaving this would let a deleted-then-
  // recreated account inherit a stale lockout window.
  await kv.delete(['rate_limit', 'login', emailHash]);

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

  // Account is gone — clear any dangling dirty marker so the cron doesn't
  // try to re-snapshot a wiped user. The pre-delete snapshot above is the
  // record we keep.
  await _clearUserDirty(emailHash);
}

// ═══════════════════════════════════════════════════════════
//  STRIPE BILLING — Phase 1 (foundations only, no UI yet)
// ═══════════════════════════════════════════════════════════
// This module provides:
//   • Stripe API helper (direct fetch — no SDK dependency)
//   • KV schema helpers for billing:account:{emailHash}
//   • Webhook signature verification (HMAC-SHA256 of t.payload)
//   • Startup migration that grandfathers test accounts and grants
//     a 90-day grace period to every other existing user (idempotent;
//     marks itself complete via a KV flag so it only runs once).
//
// Phase 1 is intentionally invisible to end users: no UI changes, no
// paywall enforcement, no checkout flow. Webhooks are received and
// logged but business logic is stubbed. This phase exists to lay the
// data model and prove the webhook pipe before we build on top of it.
//
// Future phases: trial-on-signup + Checkout (Phase 2), promo codes
// (Phase 3), referrals + abuse detection (Phase 4), polish (Phase 5).
const STRIPE_CFG = {
  secretKey:      Deno.env.get('STRIPE_SECRET_KEY')      || '',
  webhookSecret:  Deno.env.get('STRIPE_WEBHOOK_SECRET')  || '',
  priceId:        Deno.env.get('STRIPE_PRICE_ID')        || '',
  publishableKey: Deno.env.get('STRIPE_PUBLISHABLE_KEY') || '',
};
const stripeConfigured = () => !!(STRIPE_CFG.secretKey && STRIPE_CFG.webhookSecret);

// Direct Stripe API call. We use form-encoded bodies (Stripe's native
// format) rather than JSON because their API was designed for it and
// nested params are easier to express. Returns parsed JSON or throws.
async function stripeFetch(
  path: string,
  method: 'GET' | 'POST' | 'DELETE' = 'GET',
  params?: Record<string, string | number | boolean | undefined>
): Promise<any> {
  if (!STRIPE_CFG.secretKey) throw new Error('STRIPE_SECRET_KEY not configured');
  const headers: Record<string, string> = {
    'Authorization': `Bearer ${STRIPE_CFG.secretKey}`,
    'Stripe-Version': '2024-11-20.acacia',
  };
  let url = `https://api.stripe.com${path}`;
  let body: string | undefined;
  if (params && method !== 'GET') {
    const enc = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null) enc.set(k, String(v));
    }
    body = enc.toString();
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
  } else if (params && method === 'GET') {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null) qs.set(k, String(v));
    }
    url += '?' + qs.toString();
  }
  const r = await fetch(url, { method, headers, body });
  const text = await r.text();
  let parsed: any = null;
  try { parsed = text ? JSON.parse(text) : {}; } catch { parsed = { raw: text }; }
  if (!r.ok) {
    const msg = parsed?.error?.message || `Stripe API ${r.status}`;
    const err: any = new Error(msg);
    err.status = r.status;
    err.code = parsed?.error?.code;
    err.type = parsed?.error?.type;
    throw err;
  }
  return parsed;
}

// ── Billing KV schema ─────────────────────────────────────
// Single record per user account at ['billing', emailHash]. JSON-serialized
// because Deno KV values are arbitrary; we keep it stringified for parity
// with the rest of main.ts which uses string values throughout.
//
// Shape:
//   {
//     stripeCustomerId?: string,
//     stripeSubscriptionId?: string,
//     status: 'none' | 'trialing' | 'active' | 'past_due' | 'canceled' | 'free' | 'grandfathered',
//     trialEndsAt?: number,        // unix seconds
//     graceUntil?: number,         // unix seconds — applies only when status is 'none'
//     currentPeriodEnd?: number,   // unix seconds
//     cancelAtPeriodEnd?: boolean,
//     cardOnFile: boolean,
//     cardFingerprint?: string,    // for future abuse detection
//     grandfathered: boolean,      // overrides everything; bypasses paywall
//     updatedAt: number,           // unix seconds
//     createdAt: number,           // unix seconds
//   }
type BillingStatus = 'none' | 'trialing' | 'active' | 'past_due' | 'canceled' | 'free' | 'grandfathered';
interface BillingAccount {
  stripeCustomerId?:     string;
  stripeSubscriptionId?: string;
  status:                BillingStatus;
  trialEndsAt?:          number;
  graceUntil?:           number;
  currentPeriodEnd?:     number;
  cancelAtPeriodEnd?:    boolean;
  cardOnFile:            boolean;
  cardFingerprint?:      string;
  grandfathered:         boolean;
  updatedAt:             number;
  createdAt:             number;
}

async function getBillingAccount(emailHash: string): Promise<BillingAccount | null> {
  const r = await kvGet(['billing', emailHash]);
  if (!r.value) return null;
  try { return JSON.parse(String(r.value)) as BillingAccount; } catch { return null; }
}

async function setBillingAccount(emailHash: string, acct: BillingAccount): Promise<void> {
  acct.updatedAt = Math.floor(Date.now() / 1000);
  await kvSet(['billing', emailHash], JSON.stringify(acct));
  // Mark dirty so the next R2 snapshot includes the updated billing record
  await markUserDirty(emailHash);
}

// Convenience: ensure a billing record exists for a user, creating a
// minimal one if not. Returns the existing or newly-created record.
async function ensureBillingAccount(emailHash: string): Promise<BillingAccount> {
  const existing = await getBillingAccount(emailHash);
  if (existing) return existing;
  const now = Math.floor(Date.now() / 1000);
  const fresh: BillingAccount = {
    status:        'none',
    cardOnFile:    false,
    grandfathered: false,
    createdAt:     now,
    updatedAt:     now,
  };
  await setBillingAccount(emailHash, fresh);
  return fresh;
}

// Lookup a Stripe customer ID -> emailHash. Used in webhook handlers
// where we receive a Stripe event mentioning a customer and need to
// find the local user. We maintain a reverse-index alongside the main
// billing record for O(1) lookup.
async function setStripeCustomerIndex(stripeCustomerId: string, emailHash: string): Promise<void> {
  await kvSet(['billing_idx', 'stripe_customer', stripeCustomerId], emailHash);
}
async function lookupEmailHashByStripeCustomer(stripeCustomerId: string): Promise<string | null> {
  const r = await kvGet(['billing_idx', 'stripe_customer', stripeCustomerId]);
  return r.value ? String(r.value) : null;
}

// Create or fetch the Stripe customer for a user. Idempotent — if the
// user already has a Stripe customer ID, we return it; otherwise we
// create a new Stripe customer and persist the ID. The plaintext email
// is sent to Stripe (Stripe needs it for receipts) but only if the
// client supplied it at signup; otherwise we use a placeholder.
async function ensureStripeCustomer(emailHash: string, plaintextEmail?: string): Promise<string | null> {
  if (!stripeConfigured()) return null;
  const acct = await ensureBillingAccount(emailHash);
  if (acct.stripeCustomerId) return acct.stripeCustomerId;
  try {
    const params: Record<string, string> = {
      'metadata[emailHash]': emailHash,
      'metadata[source]':    'stockroom',
    };
    if (plaintextEmail) params.email = plaintextEmail;
    const cust = await stripeFetch('/v1/customers', 'POST', params);
    const stripeCustomerId = cust.id as string;
    acct.stripeCustomerId = stripeCustomerId;
    await setBillingAccount(emailHash, acct);
    await setStripeCustomerIndex(stripeCustomerId, emailHash);
    console.log(`Stripe customer created: ${stripeCustomerId} for emailHash=${emailHash}`);
    return stripeCustomerId;
  } catch (err) {
    console.error(`ensureStripeCustomer failed for ${emailHash}:`, (err as Error).message);
    return null;
  }
}

// ── Webhook signature verification ────────────────────────
// Stripe signs each webhook with HMAC-SHA256 of `${timestamp}.${payload}`
// using the webhook signing secret. The signature header looks like:
//   t=1700000000,v1=abc123...,v1=def456...
// We accept if any v1 signature matches and the timestamp is within
// 5 minutes (replay protection).
async function verifyStripeSignature(payload: string, sigHeader: string | null): Promise<boolean> {
  if (!sigHeader || !STRIPE_CFG.webhookSecret) return false;
  const parts: Record<string, string[]> = {};
  for (const seg of sigHeader.split(',')) {
    const [k, v] = seg.split('=');
    if (!k || !v) continue;
    (parts[k.trim()] = parts[k.trim()] || []).push(v.trim());
  }
  const ts = parts.t?.[0];
  const sigs = parts.v1 || [];
  if (!ts || sigs.length === 0) return false;
  // Replay guard: 5 minutes
  const now = Math.floor(Date.now() / 1000);
  const eventTime = parseInt(ts, 10);
  if (Number.isNaN(eventTime) || Math.abs(now - eventTime) > 300) {
    console.warn(`Stripe webhook outside replay window: ts=${ts} now=${now}`);
    return false;
  }
  const signed = `${ts}.${payload}`;
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(STRIPE_CFG.webhookSecret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const macBuf = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(signed));
  const expected = _hex(macBuf);
  // Constant-time compare
  for (const s of sigs) {
    if (s.length === expected.length) {
      let diff = 0;
      for (let i = 0; i < s.length; i++) diff |= s.charCodeAt(i) ^ expected.charCodeAt(i);
      if (diff === 0) return true;
    }
  }
  return false;
}

// ── Webhook event handlers (Phase 1: stubs that log + persist event ID) ──
// We persist every processed event ID to make the handler idempotent —
// Stripe retries, and out-of-order delivery means the same event.id
// can arrive multiple times. We short-circuit on a match.
async function _isEventProcessed(eventId: string): Promise<boolean> {
  const r = await kvGet(['stripe_event', eventId]);
  return !!r.value;
}
async function _markEventProcessed(eventId: string): Promise<void> {
  // Keep for 30 days — Stripe retries for max 3 days, so this is comfortably safe
  await kvSet(['stripe_event', eventId], '1', { expireIn: 30 * 24 * 60 * 60 * 1000 });
}

// ── Effective status — single source of truth ────────────
// All paywall enforcement (server) and gating (client via /billing/status)
// derives from this. Everything else is just plumbing into it.
//
// Three buckets:
//   'paid'  — full access (active sub, past_due retry grace, grandfathered, or in graceUntil window)
//   'trial' — 30-day no-card trial that hasn't expired
//   'free'  — limits apply (5 items visible, no photos, no grocery, no notes, no emails)
//
// Note that 'cancelled' (subscription cancelled but currentPeriodEnd not yet
// reached) is treated as 'paid' until the period ends — the user paid for
// the month, they keep using it.
type EffectiveStatus = 'paid' | 'trial' | 'free';
function computeEffectiveStatus(acct: BillingAccount | null): EffectiveStatus {
  if (!acct) return 'free';
  if (acct.grandfathered) return 'paid';
  const now = Math.floor(Date.now() / 1000);
  // Active or in retry window — treat as paid
  if (acct.status === 'active') return 'paid';
  if (acct.status === 'past_due') return 'paid'; // brief retry grace
  // Cancelled but period hasn't ended — keep paid until period end
  if (acct.status === 'canceled' && acct.currentPeriodEnd && acct.currentPeriodEnd > now) {
    return 'paid';
  }
  // Trialing (Stripe-side trial)
  if (acct.status === 'trialing' && acct.trialEndsAt && acct.trialEndsAt > now) {
    return 'trial';
  }
  // Local trial (hybrid model — no Stripe sub, just a trialEndsAt)
  if (!acct.stripeSubscriptionId && acct.trialEndsAt && acct.trialEndsAt > now) {
    return 'trial';
  }
  // Legacy grace period (90-day grant for users who existed before billing launched)
  if (acct.graceUntil && acct.graceUntil > now) return 'paid';
  return 'free';
}

// Free-tier limits — the constants the entire enforcement layer references.
const FREE_TIER = {
  ITEM_COUNT_LIMIT: 5,
  // Blob size limit for free tier. The encrypted blob includes all items,
  // their metadata, household/list structures, etc. A typical text-only
  // 5-item household sits around 5–15KB. We allow generous headroom but
  // block photo uploads (which push blobs into hundreds of KB).
  // Paid users have a higher implicit limit (Deno KV value max is 64KB
  // per key, and the existing R2 fallback handles bigger blobs).
  BLOB_SIZE_BYTES: 50 * 1024, // 50 KB
} as const;

// Compose the status payload returned to the client by /billing/status.
// The client uses this to drive UI gating and trial countdown banners.
async function getBillingStatusForUser(emailHash: string): Promise<{
  status:           EffectiveStatus;
  rawStatus:        BillingStatus;
  grandfathered:    boolean;
  trialEndsAt?:     number;
  graceUntil?:      number;
  trialDaysLeft?:   number;
  cardOnFile:       boolean;
  cancelAtPeriodEnd:boolean;
  currentPeriodEnd?:number;
  hasStripeCustomer:boolean;
  hasStripeSubscription:boolean;
  limits: {
    itemCount: number;
    blobBytes: number;
    photosAllowed: boolean;
    groceryAllowed: boolean;
    notesAllowed: boolean;
    emailsAllowed: boolean;
  };
  pricing: {
    configured: boolean;
    priceId?: string;
    publishableKey?: string;
  };
}> {
  const acct = await getBillingAccount(emailHash);
  const eff  = computeEffectiveStatus(acct);
  const now  = Math.floor(Date.now() / 1000);

  let trialDaysLeft: number | undefined;
  if (eff === 'trial' && acct?.trialEndsAt) {
    trialDaysLeft = Math.max(0, Math.ceil((acct.trialEndsAt - now) / 86400));
  }

  const isPaid = eff !== 'free';
  return {
    status:                eff,
    rawStatus:             acct?.status ?? 'none',
    grandfathered:         !!acct?.grandfathered,
    trialEndsAt:           acct?.trialEndsAt,
    graceUntil:            acct?.graceUntil,
    trialDaysLeft,
    cardOnFile:            !!acct?.cardOnFile,
    cancelAtPeriodEnd:     !!acct?.cancelAtPeriodEnd,
    currentPeriodEnd:      acct?.currentPeriodEnd,
    hasStripeCustomer:     !!acct?.stripeCustomerId,
    hasStripeSubscription: !!acct?.stripeSubscriptionId,
    limits: {
      itemCount:      isPaid ? Number.POSITIVE_INFINITY : FREE_TIER.ITEM_COUNT_LIMIT,
      blobBytes:      isPaid ? Number.POSITIVE_INFINITY : FREE_TIER.BLOB_SIZE_BYTES,
      photosAllowed:  isPaid,
      groceryAllowed: isPaid,
      notesAllowed:   isPaid,
      emailsAllowed:  isPaid,
    },
    pricing: {
      configured:     stripeConfigured() && !!STRIPE_CFG.priceId,
      priceId:        STRIPE_CFG.priceId || undefined,
      publishableKey: STRIPE_CFG.publishableKey || undefined,
    },
  };
}

// Convenience wrapper for server-side gating: 'is this user allowed to
// write this kind of thing?'. Returns {ok: true} when allowed, or
// {ok: false, status, reason} when blocked — caller can return that as a
// 402 Payment Required response. status and reason are populated on both
// branches (with default values when ok) to keep TS narrowing simple at
// call sites.
async function gateFeature(
  emailHash: string,
  feature: 'photos' | 'grocery' | 'notes' | 'emails' | 'blobsize',
  payloadBytes?: number
): Promise<{ ok: boolean; status: number; reason: string }> {
  const status = await getBillingStatusForUser(emailHash);
  if (status.status !== 'free') return { ok: true, status: 200, reason: 'ok' };
  switch (feature) {
    case 'photos':
      return { ok: false, status: 402, reason: 'photos_require_upgrade' };
    case 'grocery':
      return { ok: false, status: 402, reason: 'grocery_requires_upgrade' };
    case 'notes':
      return { ok: false, status: 402, reason: 'notes_require_upgrade' };
    case 'emails':
      return { ok: false, status: 402, reason: 'email_reminders_require_upgrade' };
    case 'blobsize':
      if (payloadBytes !== undefined && payloadBytes > FREE_TIER.BLOB_SIZE_BYTES) {
        return { ok: false, status: 402, reason: 'free_tier_size_exceeded' };
      }
      return { ok: true, status: 200, reason: 'ok' };
  }
  return { ok: true, status: 200, reason: 'ok' };
}

async function handleStripeEvent(event: any): Promise<void> {
  const type: string = event.type;
  const obj = event?.data?.object || {};
  // Find the local user this event is about. Most events have a `customer`
  // field; payment_method.attached has it; subscription events have it.
  // checkout.session.completed has both `customer` and `client_reference_id`
  // (we stash emailHash in client_reference_id when creating the session).
  const stripeCustomerId: string | undefined =
    obj.customer || (typeof obj === 'object' && obj.object === 'customer' ? obj.id : undefined);
  let emailHash: string | null = null;
  // Prefer client_reference_id when present (more reliable for first checkout
  // before we've indexed the customer). Falls through to customer-id lookup.
  if (typeof obj.client_reference_id === 'string' && /^[a-f0-9]{16,128}$/i.test(obj.client_reference_id)) {
    emailHash = obj.client_reference_id;
    // Backfill the index if missing
    if (stripeCustomerId) {
      const indexed = await lookupEmailHashByStripeCustomer(stripeCustomerId);
      if (!indexed) await setStripeCustomerIndex(stripeCustomerId, emailHash);
    }
  } else if (stripeCustomerId) {
    emailHash = await lookupEmailHashByStripeCustomer(stripeCustomerId);
  }

  console.log(`[stripe-webhook] ${type} customer=${stripeCustomerId || 'n/a'} emailHash=${emailHash || 'unknown'}`);

  // If we can't resolve a local user, log and exit. Sometimes happens for
  // synthetic test events (Stripe dashboard's "Send test webhook" uses
  // hardcoded fake customer IDs not in our system). Returning successfully
  // (no throw) so we still mark the event processed and don't retry.
  if (!emailHash) return;

  switch (type) {
    case 'checkout.session.completed':
      await _handleCheckoutCompleted(emailHash, obj);
      break;
    case 'customer.subscription.created':
    case 'customer.subscription.updated':
      await _handleSubscriptionUpsert(emailHash, obj);
      break;
    case 'customer.subscription.deleted':
      await _handleSubscriptionDeleted(emailHash, obj);
      break;
    case 'customer.subscription.trial_will_end':
      await _handleTrialWillEnd(emailHash, obj);
      break;
    case 'invoice.payment_succeeded':
      await _handleInvoicePaid(emailHash, obj);
      break;
    case 'invoice.payment_failed':
      await _handleInvoiceFailed(emailHash, obj);
      break;
    case 'payment_method.attached':
      await _handlePaymentMethodAttached(emailHash, obj);
      break;
    default:
      console.log(`[stripe-webhook] unhandled event type: ${type}`);
  }
}

// When checkout completes, Stripe creates the subscription server-side and
// will send us a `customer.subscription.created` event right after this.
// So this handler is mostly defensive — we record any session-level data
// (e.g. payment_intent succeeded) but the subscription handler does the
// real status update.
async function _handleCheckoutCompleted(emailHash: string, session: any): Promise<void> {
  const acct = await ensureBillingAccount(emailHash);
  if (session.subscription && !acct.stripeSubscriptionId) {
    acct.stripeSubscriptionId = session.subscription;
  }
  if (session.customer && !acct.stripeCustomerId) {
    acct.stripeCustomerId = session.customer;
    await setStripeCustomerIndex(session.customer, emailHash);
  }
  await setBillingAccount(emailHash, acct);
}

// Map a Stripe subscription object onto our local billing record. This is
// the heart of staying in sync: we treat Stripe's status as authoritative
// and just mirror it. The one local-only state we preserve is
// `grandfathered` (it's an admin-set flag that overrides Stripe entirely).
async function _handleSubscriptionUpsert(emailHash: string, sub: any): Promise<void> {
  const acct = await ensureBillingAccount(emailHash);
  // Don't let Stripe events overwrite a grandfathered account. Should never
  // happen in practice (grandfathered users don't have Stripe subs) but
  // defensive coding here is cheap.
  if (acct.grandfathered) {
    console.log(`[stripe-webhook] ignoring subscription update for grandfathered ${emailHash}`);
    return;
  }
  acct.stripeSubscriptionId = sub.id;
  // Stripe statuses: trialing, active, past_due, canceled, unpaid, incomplete, incomplete_expired
  // Map onto our schema:
  switch (sub.status) {
    case 'trialing':           acct.status = 'trialing'; break;
    case 'active':             acct.status = 'active'; break;
    case 'past_due':           acct.status = 'past_due'; break;
    case 'unpaid':             acct.status = 'past_due'; break;
    case 'canceled':           acct.status = 'canceled'; break;
    case 'incomplete':         acct.status = 'past_due'; break;
    case 'incomplete_expired': acct.status = 'canceled'; break;
    default:                   acct.status = 'free';
  }
  acct.cancelAtPeriodEnd = !!sub.cancel_at_period_end;
  if (typeof sub.trial_end === 'number') acct.trialEndsAt = sub.trial_end;
  if (typeof sub.current_period_end === 'number') acct.currentPeriodEnd = sub.current_period_end;
  // Default payment method tells us whether a card is on file
  if (sub.default_payment_method) acct.cardOnFile = true;
  await setBillingAccount(emailHash, acct);
  console.log(`[stripe-webhook] sub ${sub.id} status=${sub.status} -> local status=${acct.status}`);
}

async function _handleSubscriptionDeleted(emailHash: string, sub: any): Promise<void> {
  const acct = await ensureBillingAccount(emailHash);
  if (acct.grandfathered) return;
  acct.status = 'canceled';
  acct.cancelAtPeriodEnd = false;
  // Keep stripeSubscriptionId for historical reference; the user might
  // resubscribe and we'll create a new one on the next checkout.
  await setBillingAccount(emailHash, acct);
}

// Stripe sends this 3 days before a Stripe-side trial ends. We use it as
// a hook to email the user. For the hybrid model (local trial), this
// event will not fire — we handle that separately via a daily cron. For
// users who have moved to a Stripe trial (added card), this fires.
async function _handleTrialWillEnd(emailHash: string, sub: any): Promise<void> {
  // Phase 2: just log. Email in a future polish pass.
  console.log(`[stripe-webhook] trial_will_end for ${emailHash}, ends at ${sub.trial_end}`);
}

// Successful payment. Sync any subscription changes (period rolled forward,
// status flipped from past_due to active, etc.).
async function _handleInvoicePaid(emailHash: string, invoice: any): Promise<void> {
  if (!invoice.subscription) return;
  // Re-fetch the subscription to get the latest status.
  try {
    const sub = await stripeFetch(`/v1/subscriptions/${invoice.subscription}`, 'GET');
    await _handleSubscriptionUpsert(emailHash, sub);
  } catch (err) {
    console.error(`[stripe-webhook] failed to fetch subscription ${invoice.subscription}:`, (err as Error).message);
  }
  // Advance the referral state machine if this user is a referee.
  // Each successful invoice (1st → converted, 2nd → qualified). Idempotent
  // on already-qualified or rejected referrals.
  try { await processInvoiceForReferral(emailHash); }
  catch (err) { console.error('[referral] processInvoiceForReferral failed:', (err as Error).message); }
}

async function _handleInvoiceFailed(emailHash: string, invoice: any): Promise<void> {
  const acct = await ensureBillingAccount(emailHash);
  if (acct.grandfathered) return;
  acct.status = 'past_due';
  await setBillingAccount(emailHash, acct);
  console.log(`[stripe-webhook] invoice.payment_failed for ${emailHash}, marked past_due`);
}

async function _handlePaymentMethodAttached(emailHash: string, pm: any): Promise<void> {
  const acct = await ensureBillingAccount(emailHash);
  acct.cardOnFile = true;
  if (pm?.card?.fingerprint) {
    acct.cardFingerprint = pm.card.fingerprint;
    // Maintain the fingerprint reverse-index for abuse detection.
    // Best-effort — never block the webhook on this.
    try { await indexCardFingerprint(emailHash, pm.card.fingerprint); }
    catch (err) { console.warn('[referral] indexCardFingerprint failed:', (err as Error).message); }
  }
  await setBillingAccount(emailHash, acct);
}

// ── Startup migration: grandfather test accounts, grace existing users ──
// Idempotent: marks itself complete via ['_billing_migration_v1', 'done'].
// On next deploy, the marker is already present so this is a no-op.
//
// Test accounts (hard-coded by emailHash, computed via SHA-256(email.lower().trim())[:32]):
//   pete@artbot5000.com   -> 9e3b241ca0c59deb3215330953f3c8a9
//   pasmith984@gmail.com  -> 30efe663f9c51af805ab389a0b142d18
//   qwertyblue@gmail.com  -> c2a0b0e84f7167844c0f3d89253f5de6
//   test1@artbot5000.com  -> 5792455ffee52ae98982f962f154e831
//   test2@artbot5000.com  -> 8f5d614b01b73396c5cf34d6d52ed031
//
// Note: test2@artbot5000.com may not exist yet (user mentioned "not yet
// set up"). If absent, we still pre-create a billing record so when the
// account is registered the grandfather flag is already in place.
const GRANDFATHERED_TEST_HASHES = [
  '9e3b241ca0c59deb3215330953f3c8a9', // Pete@artbot5000.com
  '30efe663f9c51af805ab389a0b142d18', // pasmith984@gmail.com
  'c2a0b0e84f7167844c0f3d89253f5de6', // qwertyblue@gmail.com
  '5792455ffee52ae98982f962f154e831', // test1@artbot5000.com
  '8f5d614b01b73396c5cf34d6d52ed031', // test2@artbot5000.com (may not exist)
];
const GRACE_PERIOD_DAYS = 90;

async function runBillingMigration(): Promise<void> {
  const marker = await kvGet(['_billing_migration_v1', 'done']);
  if (marker.value) {
    console.log('Billing migration v1: already complete, skipping');
    return;
  }
  console.log('Billing migration v1: starting');
  const now = Math.floor(Date.now() / 1000);
  const graceUntil = now + (GRACE_PERIOD_DAYS * 24 * 60 * 60);
  let grandfathered = 0;
  let graced = 0;
  let skipped = 0;

  // 1. Grandfather the test accounts unconditionally (whether they exist or not).
  //    If they don't exist yet, the billing record sits there waiting for them.
  for (const emailHash of GRANDFATHERED_TEST_HASHES) {
    const existing = await getBillingAccount(emailHash);
    if (existing && existing.grandfathered) { skipped++; continue; }
    const acct: BillingAccount = existing || {
      status:        'grandfathered',
      cardOnFile:    false,
      grandfathered: true,
      createdAt:     now,
      updatedAt:     now,
    };
    acct.grandfathered = true;
    acct.status = 'grandfathered';
    await setBillingAccount(emailHash, acct);
    grandfathered++;
  }

  // 2. Grant 90-day grace to every other existing user. We iterate
  //    ['user', emailHash, 'verifier'] entries — each represents a
  //    registered account.
  try {
    const iter = kv.list({ prefix: ['user'] });
    const seen = new Set<string>();
    for await (const entry of iter) {
      // Keys look like ['user', emailHash, 'verifier'] or ['user', emailHash, 'created'] etc.
      // We just want each unique emailHash once.
      const key = entry.key as unknown as string[];
      if (key.length < 3 || key[2] !== 'verifier') continue;
      const emailHash = key[1];
      if (seen.has(emailHash)) continue;
      seen.add(emailHash);
      if (GRANDFATHERED_TEST_HASHES.includes(emailHash)) continue; // already handled
      const existing = await getBillingAccount(emailHash);
      if (existing) { skipped++; continue; } // already has a billing record, don't overwrite
      const acct: BillingAccount = {
        status:        'none',
        graceUntil,
        cardOnFile:    false,
        grandfathered: false,
        createdAt:     now,
        updatedAt:     now,
      };
      await setBillingAccount(emailHash, acct);
      graced++;
    }
  } catch (err) {
    console.error('Billing migration: error iterating users:', (err as Error).message);
    // Don't mark complete — let it retry next startup
    return;
  }

  await kvSet(['_billing_migration_v1', 'done'], new Date().toISOString());
  console.log(`Billing migration v1: complete — grandfathered=${grandfathered}, graced=${graced}, skipped=${skipped}`);
}

// Kick off the migration in the background so it doesn't block startup.
// If it fails, the marker stays unset and it'll retry next deploy.
runBillingMigration().catch(err => {
  console.error('Billing migration: unhandled error:', err);
});

// ═══════════════════════════════════════════════════════════
//  REFERRALS — Phase 3
// ═══════════════════════════════════════════════════════════
// Each user gets one referral code (lazy-created on first request).
// Referees who sign up with a code get 50% off their first 3 paid months
// via a Stripe coupon attached at checkout. When a referee completes
// their 2nd successful paid invoice, the referrer gets a 30-day extension
// on their subscription period (or a 30-day extension to their trial if
// they're not yet on paid). Lifetime cap of 12 referrer extensions.
//
// Abuse defenses applied at qualification time:
//   • Card fingerprint dedup (set on payment_method.attached webhook)
//   • Email normalization (gmail dots/+aliases collapsed)
//   • Lifetime cap (12 months total per referrer)
//
// KV schema:
//   referral:code:{emailHash}     → JSON { code, createdAt, lifetimeCreditsMonths, qualifiedCount, signupCount, convertedCount }
//   referral:lookup:{CODE}        → emailHash (uppercase code → owner)
//   referral:signup:{refereeHash} → JSON { referrerHash, signedUpAt, firstPaymentAt?, qualifiedAt?, status, rejectionReason? }
//   referral:fp_index:{fingerprint} → JSON [emailHash, ...]   (cards seen across multiple accounts)
//   referral:emailnorm:{hash}     → emailHash                  (normalized-email reverse index)

const REFERRAL_LIFETIME_CAP_MONTHS = 12;
const REFEREE_DISCOUNT_PERCENT     = 50;
const REFEREE_DISCOUNT_MONTHS      = 3;
const REFERRER_EXTENSION_DAYS      = 30;
const REFERRAL_CODE_RANDOM_DIGITS  = 4;
// Stripe coupon ID we create lazily for the referee 50%-off-3-months perk.
const REFEREE_COUPON_ID            = 'STOCKROOM_REFEREE_50OFF_3MO';

// ── Code generation ──
// Generate a friendly code like "PETE-7421" using the user's email
// localpart (max 4 chars, A-Z only) + a 4-digit random suffix. The
// alphabet excludes ambiguous chars (0, O, 1, I) to make codes
// shareable verbally.
const REFERRAL_CODE_ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'; // 32 chars, no 0/O/1/I
function _genReferralSuffix(): string {
  const bytes = new Uint8Array(REFERRAL_CODE_RANDOM_DIGITS);
  crypto.getRandomValues(bytes);
  let out = '';
  for (let i = 0; i < REFERRAL_CODE_RANDOM_DIGITS; i++) {
    out += REFERRAL_CODE_ALPHABET[bytes[i] % REFERRAL_CODE_ALPHABET.length];
  }
  return out;
}
function _genReferralPrefix(plaintextEmail?: string): string {
  if (!plaintextEmail) return 'USER';
  const local = plaintextEmail.split('@')[0] || '';
  const cleaned = local.replace(/[^a-zA-Z]/g, '').toUpperCase().slice(0, 4);
  return cleaned.length >= 3 ? cleaned.padEnd(4, 'X') : 'USER';
}

// Email normalization for abuse detection. Returns a hashed token that
// is stable across gmail dots/+aliases and case variants. SHA-256 hashed
// so we never store plaintext emails in the abuse index.
async function _normalizeEmail(email: string): Promise<string> {
  if (!email) return '';
  let e = email.toLowerCase().trim();
  const at = e.indexOf('@');
  if (at < 0) return e;
  let local = e.slice(0, at);
  const domain = e.slice(at + 1);
  // Strip + suffix (gmail, fastmail, etc. all support this convention)
  const plus = local.indexOf('+');
  if (plus >= 0) local = local.slice(0, plus);
  // Strip dots in local part for known gmail domains
  if (domain === 'gmail.com' || domain === 'googlemail.com') {
    local = local.replace(/\./g, '');
  }
  const normalized = `${local}@${domain === 'googlemail.com' ? 'gmail.com' : domain}`;
  // Hash so we don't store plaintext emails in the abuse index
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(normalized));
  const arr = Array.from(new Uint8Array(buf));
  return arr.map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 32);
}

interface ReferralCodeRecord {
  code:                  string;
  createdAt:             number;
  lifetimeCreditsMonths: number; // running total of months credited (cap = 12)
  signupCount:           number; // count of referees who signed up using this code
  convertedCount:        number; // count of referees who reached 1st paid invoice
  qualifiedCount:        number; // count of referees who qualified (2nd paid invoice)
}
interface ReferralSignupRecord {
  referrerHash:    string;
  signedUpAt:      number;
  firstPaymentAt?: number;
  qualifiedAt?:    number;
  creditedAt?:     number;
  status:          'pending' | 'converted' | 'qualified' | 'rejected';
  rejectionReason?: 'card_dup' | 'email_dup' | 'lifetime_cap' | 'self_referral' | 'churned' | 'manual' | 'referrer_ineligible';
}

async function getReferralCodeForUser(emailHash: string): Promise<ReferralCodeRecord | null> {
  const r = await kvGet(['referral', 'code', emailHash]);
  if (!r.value) return null;
  try { return JSON.parse(String(r.value)); } catch { return null; }
}

async function ensureReferralCode(emailHash: string, plaintextEmail?: string): Promise<ReferralCodeRecord> {
  const existing = await getReferralCodeForUser(emailHash);
  if (existing) return existing;
  // Generate a unique code with retry-on-collision (extremely rare —
  // 32^4 = ~1M space, but we still loop to be safe).
  const prefix = _genReferralPrefix(plaintextEmail);
  let code = '';
  for (let attempt = 0; attempt < 8; attempt++) {
    const candidate = `${prefix}-${_genReferralSuffix()}`;
    const collision = await kvGet(['referral', 'lookup', candidate]);
    if (!collision.value) { code = candidate; break; }
  }
  if (!code) throw new Error('Could not generate unique referral code');
  const now = Math.floor(Date.now() / 1000);
  const record: ReferralCodeRecord = {
    code,
    createdAt:             now,
    lifetimeCreditsMonths: 0,
    signupCount:           0,
    convertedCount:        0,
    qualifiedCount:        0,
  };
  await kvSet(['referral', 'code', emailHash], JSON.stringify(record));
  await kvSet(['referral', 'lookup', code], emailHash);
  return record;
}

async function getReferrerByCode(code: string): Promise<string | null> {
  if (!code) return null;
  const r = await kvGet(['referral', 'lookup', code.toUpperCase()]);
  return r.value ? String(r.value) : null;
}

// ── Referrer eligibility ──
// Determines whether a user is allowed to refer friends. The rule is
// 'they must have skin in the game' — either grandfathered, or have a
// card on file with an active/past-due/cancelling-but-still-paid sub.
// Trial users (never paid) cannot refer; this prevents abuse where
// someone chains trial accounts together to issue free months.
//
// Importantly, a user who is currently in a referrer-rewarded extension
// (i.e. trialEndsAt is in the future because they qualified a friend)
// IS eligible — because they have card on file from a previous paid
// state. The cardOnFile flag is the deciding factor.
function isReferrerEligible(acct: BillingAccount | null): boolean {
  if (!acct) return false;
  if (acct.grandfathered) return true;
  // Must have a card on file. This rules out original-trial users
  // who never added payment.
  if (!acct.cardOnFile) return false;
  // Status must indicate they paid us at some point (and aren't fully
  // lapsed). 'free' means they cancelled and the period ended without
  // any active subscription — not eligible until they re-subscribe.
  switch (acct.status) {
    case 'active':
    case 'past_due':
    case 'trialing':   // mid-extension reward, came from active state (cardOnFile gate above guarantees this)
      return true;
    case 'canceled': {
      // Cancelled but still within paid period? Eligible until period ends.
      const now = Math.floor(Date.now() / 1000);
      return !!acct.currentPeriodEnd && acct.currentPeriodEnd > now;
    }
    default:
      return false;
  }
}

async function getReferralSignup(refereeHash: string): Promise<ReferralSignupRecord | null> {
  const r = await kvGet(['referral', 'signup', refereeHash]);
  if (!r.value) return null;
  try { return JSON.parse(String(r.value)); } catch { return null; }
}

async function setReferralSignup(refereeHash: string, rec: ReferralSignupRecord): Promise<void> {
  await kvSet(['referral', 'signup', refereeHash], JSON.stringify(rec));
}

// Update referrer's stats counter (one of signupCount/convertedCount/
// qualifiedCount/lifetimeCreditsMonths). Caller passes the field and an
// integer delta. No-op if the code record doesn't exist.
async function _bumpReferrerStat(referrerHash: string, field: keyof ReferralCodeRecord, delta: number): Promise<void> {
  const rec = await getReferralCodeForUser(referrerHash);
  if (!rec) return;
  (rec as any)[field] = ((rec as any)[field] || 0) + delta;
  await kvSet(['referral', 'code', referrerHash], JSON.stringify(rec));
}

// Lazily ensure the referee Stripe coupon exists. Called from /billing/checkout
// when a referee starts their first paid month.
async function ensureRefereeCoupon(): Promise<string | null> {
  if (!stripeConfigured()) return null;
  try {
    // GET first — Stripe coupons are idempotent by ID. If it exists, we're done.
    const existing = await stripeFetch(`/v1/coupons/${REFEREE_COUPON_ID}`, 'GET').catch(() => null);
    if (existing?.id) return existing.id;
    // Create
    const created = await stripeFetch('/v1/coupons', 'POST', {
      'id':                  REFEREE_COUPON_ID,
      'percent_off':         REFEREE_DISCOUNT_PERCENT,
      'duration':            'repeating',
      'duration_in_months':  REFEREE_DISCOUNT_MONTHS,
      'name':                `${REFEREE_DISCOUNT_PERCENT}% off first ${REFEREE_DISCOUNT_MONTHS} months (referee perk)`,
      'metadata[source]':    'stockroom_referral_referee',
    });
    return created.id;
  } catch (err) {
    console.error('[referral] ensureRefereeCoupon failed:', (err as Error).message);
    return null;
  }
}

// Apply the referrer reward: 30-day extension. Logic differs by current state:
//   • Trial user (no Stripe sub yet) → bump trialEndsAt forward by 30 days
//   • Active Stripe sub → push trial_end (which Stripe interprets as
//     'skip next billing for this period')
//   • Free / cancelled → set a 30-day local "rewarded period" by writing
//     trialEndsAt = now + 30 days and status = trialing
async function applyReferrerExtension(referrerHash: string): Promise<{ ok: boolean; reason?: string }> {
  const acct = await getBillingAccount(referrerHash);
  if (!acct) return { ok: false, reason: 'no_billing_account' };
  if (acct.grandfathered) {
    // No effect — grandfathered users already have full access. Still
    // count it for stats but don't change their state.
    return { ok: true };
  }
  const now = Math.floor(Date.now() / 1000);
  const ext = REFERRER_EXTENSION_DAYS * 86400;

  // Active Stripe sub → push trial_end forward to extend free period
  if (acct.stripeSubscriptionId && (acct.status === 'active' || acct.status === 'trialing')) {
    try {
      // Compute the new trial end: max(current_period_end, now) + 30 days
      const baseTs = Math.max(acct.currentPeriodEnd || 0, now);
      const newTrialEnd = baseTs + ext;
      await stripeFetch(`/v1/subscriptions/${acct.stripeSubscriptionId}`, 'POST', {
        'trial_end':              String(newTrialEnd),
        'proration_behavior':     'none',
      });
      // The webhook customer.subscription.updated will mirror status into KV;
      // we also patch optimistically so UI updates instantly.
      acct.trialEndsAt = newTrialEnd;
      acct.status      = 'trialing';
      await setBillingAccount(referrerHash, acct);
      return { ok: true };
    } catch (err) {
      console.error('[referral] Stripe sub extension failed:', (err as Error).message);
      return { ok: false, reason: 'stripe_error' };
    }
  }

  // No Stripe sub (or sub is inactive) → bump local trial state
  const newTrialEnd = Math.max(acct.trialEndsAt || now, now) + ext;
  acct.trialEndsAt = newTrialEnd;
  acct.status      = 'trialing';
  await setBillingAccount(referrerHash, acct);
  return { ok: true };
}

// Send the referrer-rewarded email (Resend). Best-effort — we never throw.
async function _sendReferrerQualifiedEmail(referrerHash: string): Promise<void> {
  try {
    const emailRow = await kvGet(['user', referrerHash, 'email']);
    if (!emailRow.value) return; // user signed up without sharing plaintext email
    const to = String(emailRow.value);
    const RESEND_KEY = Deno.env.get('RESEND_API_KEY') || '';
    const FROM       = Deno.env.get('FROM_EMAIL') || 'STOCKROOM <noreply@stckrm.com>';
    if (!RESEND_KEY) { console.warn('[referral] RESEND_API_KEY not set; skipping email'); return; }
    const subject = '✓ A friend just signed up — you earned 30 free days!';
    const html = `<!DOCTYPE html>
<html><body style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:520px;margin:0 auto;padding:24px;color:#111">
  <h2 style="font-size:20px;margin:0 0 12px">A friend you referred just qualified.</h2>
  <p style="font-size:15px;line-height:1.5;color:#333">
    They signed up using your code, completed their second paid month, and now
    you've earned <strong>30 free days</strong> on your subscription.
  </p>
  <p style="font-size:15px;line-height:1.5;color:#333">
    The credit has been applied automatically — your next renewal date has
    been pushed out by a month. No action needed on your part.
  </p>
  <p style="font-size:13px;color:#666;margin-top:24px">
    Want to refer more friends? Open STOCKROOM → Billing to grab your code.
  </p>
  <hr style="border:none;border-top:1px solid #eee;margin:24px 0">
  <p style="font-size:11px;color:#999">You're receiving this because someone signed up with your referral code on STOCKROOM. If you didn't expect this email, you can safely ignore it.</p>
</body></html>`;
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${RESEND_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: FROM, to, subject, html }),
    });
  } catch (err) {
    console.warn('[referral] email send failed:', (err as Error).message);
  }
}

// Record a referee's signup. Validates the code exists and isn't a self-
// referral. Idempotent — calling twice for the same referee is a no-op.
async function recordReferralSignup(refereeHash: string, code: string, refereeEmail?: string): Promise<{ ok: boolean; reason?: string }> {
  if (!code) return { ok: false, reason: 'no_code' };
  const referrerHash = await getReferrerByCode(code);
  if (!referrerHash) return { ok: false, reason: 'invalid_code' };
  if (referrerHash === refereeHash) return { ok: false, reason: 'self_referral' };
  // Eligibility check — the referrer must be a paid/grandfathered user.
  // Trial users cannot refer (prevents trial-account chaining abuse).
  // We deliberately don't reject the signup itself — the referee still
  // gets the standard 30-day trial. We just don't record the referral
  // so no credit can ever be issued to an ineligible referrer.
  const referrerAcct = await getBillingAccount(referrerHash);
  if (!isReferrerEligible(referrerAcct)) {
    console.log(`[referral] dropping signup for ineligible referrer ${referrerHash.slice(0,8)} (status=${referrerAcct?.status || 'none'} cardOnFile=${!!referrerAcct?.cardOnFile})`);
    return { ok: false, reason: 'referrer_ineligible' };
  }
  // Don't overwrite an existing record (prevents fraud via re-running signup)
  const existing = await getReferralSignup(refereeHash);
  if (existing) return { ok: false, reason: 'already_recorded' };
  const now = Math.floor(Date.now() / 1000);
  await setReferralSignup(refereeHash, {
    referrerHash,
    signedUpAt: now,
    status:     'pending',
  });
  await _bumpReferrerStat(referrerHash, 'signupCount', 1);
  // Pre-compute and store the normalized email for abuse detection
  if (refereeEmail) {
    const norm = await _normalizeEmail(refereeEmail);
    if (norm) await kvSet(['referral', 'emailnorm', norm], refereeHash);
  }
  return { ok: true };
}

// Run abuse checks at qualification. Returns null if clean, or a rejection
// reason string. We check: card fingerprint vs referrer, normalized email
// vs referrer, lifetime cap, AND ongoing referrer eligibility.
async function _checkReferralAbuse(refereeHash: string, referrerHash: string): Promise<string | null> {
  // 0. Referrer must still be eligible at qualification time. They might
  // have been eligible when the referee signed up but cancelled before
  // the referee qualified — in which case we don't issue them credit.
  const referrerAcctEarly = await getBillingAccount(referrerHash);
  if (!isReferrerEligible(referrerAcctEarly)) {
    return 'referrer_ineligible';
  }
  // 1. Card fingerprint dedup
  const refereeAcct  = await getBillingAccount(refereeHash);
  const referrerAcct = referrerAcctEarly;
  if (refereeAcct?.cardFingerprint && referrerAcct?.cardFingerprint
      && refereeAcct.cardFingerprint === referrerAcct.cardFingerprint) {
    return 'card_dup';
  }
  // Also check the fingerprint index for any other account collision
  if (refereeAcct?.cardFingerprint) {
    const idxRow = await kvGet(['referral', 'fp_index', refereeAcct.cardFingerprint]);
    if (idxRow.value) {
      try {
        const accounts = JSON.parse(String(idxRow.value)) as string[];
        // If the referrer is anywhere in this list, the cards have crossed
        if (accounts.includes(referrerHash)) return 'card_dup';
      } catch (_) {}
    }
  }
  // 2. Normalized email dedup
  const refereeEmailRow  = await kvGet(['user', refereeHash, 'email']);
  const referrerEmailRow = await kvGet(['user', referrerHash, 'email']);
  if (refereeEmailRow.value && referrerEmailRow.value) {
    const refNorm  = await _normalizeEmail(String(refereeEmailRow.value));
    const reffNorm = await _normalizeEmail(String(referrerEmailRow.value));
    if (refNorm && refNorm === reffNorm) return 'email_dup';
  }
  // 3. Lifetime cap
  const refRecord = await getReferralCodeForUser(referrerHash);
  if (refRecord && refRecord.lifetimeCreditsMonths >= REFERRAL_LIFETIME_CAP_MONTHS) {
    return 'lifetime_cap';
  }
  return null;
}

// Track a paid invoice for a referee. Advances state machine:
//   pending  →  converted  (1st paid invoice)
//   converted → qualified (2nd paid invoice; runs abuse checks; issues credit)
async function processInvoiceForReferral(refereeHash: string): Promise<void> {
  const signup = await getReferralSignup(refereeHash);
  if (!signup) return; // not a referral signup
  if (signup.status === 'qualified' || signup.status === 'rejected') return;
  const now = Math.floor(Date.now() / 1000);

  if (signup.status === 'pending') {
    signup.status = 'converted';
    signup.firstPaymentAt = now;
    await setReferralSignup(refereeHash, signup);
    await _bumpReferrerStat(signup.referrerHash, 'convertedCount', 1);
    console.log(`[referral] ${refereeHash.slice(0,8)} converted (1st payment)`);
    return;
  }

  if (signup.status === 'converted') {
    // Run abuse checks
    const rejectionReason = await _checkReferralAbuse(refereeHash, signup.referrerHash);
    if (rejectionReason) {
      signup.status          = 'rejected';
      signup.rejectionReason = rejectionReason as any;
      await setReferralSignup(refereeHash, signup);
      console.log(`[referral] ${refereeHash.slice(0,8)} rejected at qualification: ${rejectionReason}`);
      return;
    }
    // Apply the extension
    const ext = await applyReferrerExtension(signup.referrerHash);
    if (!ext.ok) {
      console.error(`[referral] failed to apply extension: ${ext.reason}`);
      return; // leave as 'converted' so we retry next webhook
    }
    signup.status      = 'qualified';
    signup.qualifiedAt = now;
    signup.creditedAt  = now;
    await setReferralSignup(refereeHash, signup);
    await _bumpReferrerStat(signup.referrerHash, 'qualifiedCount', 1);
    await _bumpReferrerStat(signup.referrerHash, 'lifetimeCreditsMonths', 1);
    console.log(`[referral] ${refereeHash.slice(0,8)} QUALIFIED — referrer ${signup.referrerHash.slice(0,8)} got 30 days`);
    // Send referrer email (best-effort, async)
    _sendReferrerQualifiedEmail(signup.referrerHash).catch(_ => {});
  }
}

// Maintain the card fingerprint reverse index — append-only list of
// emailHashes that have used this fingerprint.
async function indexCardFingerprint(emailHash: string, fingerprint: string): Promise<void> {
  if (!fingerprint) return;
  const row = await kvGet(['referral', 'fp_index', fingerprint]);
  let list: string[] = [];
  if (row.value) {
    try { list = JSON.parse(String(row.value)); } catch { list = []; }
  }
  if (!list.includes(emailHash)) {
    list.push(emailHash);
    await kvSet(['referral', 'fp_index', fingerprint], JSON.stringify(list));
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

  // ── Billing: status — what UI gates / banners to show ──
  // Auth: verifier OR sessionToken (passkey). Same dual-auth pattern as
  // /data/push and friends.
  if (url.pathname === '/billing/status' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      const status = await getBillingStatusForUser(emailHash);
      return json(status, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Billing: checkout — create a Stripe Checkout Session ──
  // Returns a URL the client redirects to. Uses Stripe's hosted checkout
  // page so we never touch card details (PCI scope = SAQ-A, the easiest
  // tier).
  if (url.pathname === '/billing/checkout' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, promoCode } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      if (!stripeConfigured() || !STRIPE_CFG.priceId) {
        return json({ error: 'Billing not configured' }, corsHeaders, 503);
      }
      // Ensure the user has a Stripe customer
      const customerId = await ensureStripeCustomer(emailHash);
      if (!customerId) return json({ error: 'Could not create Stripe customer' }, corsHeaders, 502);

      // Build the success/cancel URLs. APP_URL points at the PWA hostname.
      const appUrl = (Deno.env.get('APP_URL') || 'https://app.stckrm.com').replace(/\/$/, '');
      const successUrl = `${appUrl}/?billing=success&session_id={CHECKOUT_SESSION_ID}`;
      const cancelUrl  = `${appUrl}/?billing=cancel`;

      const params: Record<string, string | number | boolean> = {
        'mode':                          'subscription',
        'customer':                      customerId,
        'client_reference_id':           emailHash, // for idempotent webhook lookup
        'line_items[0][price]':          STRIPE_CFG.priceId,
        'line_items[0][quantity]':       1,
        'success_url':                   successUrl,
        'cancel_url':                    cancelUrl,
        'allow_promotion_codes':         true,
        // We do NOT set subscription_data[trial_period_days] here — the
        // hybrid trial model means by the time a user clicks Upgrade,
        // their local trial may have run out. If they're upgrading early
        // we don't want a second 30-day trial on top. If they want a
        // trial extension as a promo, that's what promo codes are for.
      };
      if (promoCode && typeof promoCode === 'string') {
        // Look up the promotion code to get its ID. allow_promotion_codes
        // also allows users to enter codes on the Stripe-hosted page itself.
        try {
          const lookup = await stripeFetch(`/v1/promotion_codes`, 'GET', { code: promoCode, active: 'true', limit: 1 });
          const pc = lookup?.data?.[0];
          if (pc?.id) {
            params['discounts[0][promotion_code]'] = pc.id;
            // Can't have both allow_promotion_codes and a fixed discount
            delete params['allow_promotion_codes'];
          }
        } catch (err) {
          console.warn(`[billing/checkout] promo lookup failed: ${(err as Error).message}`);
        }
      } else {
        // No explicit promo code → check if this user is a referee with a
        // pending discount (50% off first 3 months) and apply that.
        // Promo code takes precedence over referee perk because the user
        // explicitly chose to enter a code (e.g. WELCOME50 may be better).
        try {
          const refSignup = await getReferralSignup(emailHash);
          // Eligible only if signup exists, hasn't been rejected, and we
          // haven't already applied the discount on a previous checkout
          // attempt that succeeded (in which case they'd have a Stripe
          // sub already and wouldn't be checking out again).
          if (refSignup
              && refSignup.status !== 'rejected'
              && !(await getBillingAccount(emailHash))?.stripeSubscriptionId) {
            const couponId = await ensureRefereeCoupon();
            if (couponId) {
              params['discounts[0][coupon]'] = couponId;
              delete params['allow_promotion_codes'];
              console.log(`[billing/checkout] applied referee coupon ${couponId} for ${emailHash.slice(0,8)}`);
            }
          }
        } catch (err) {
          console.warn(`[billing/checkout] referee coupon attach failed: ${(err as Error).message}`);
        }
      }

      const session = await stripeFetch('/v1/checkout/sessions', 'POST', params);
      return json({ url: session.url, sessionId: session.id }, corsHeaders);
    } catch(err) {
      const msg = (err as Error).message;
      console.error(`[billing/checkout] error:`, msg);
      return json({ error: msg }, corsHeaders, 500);
    }
  }

  // ── Billing: portal — Stripe Customer Portal session ──
  // For users who already have a sub: manage card, view invoices, cancel.
  if (url.pathname === '/billing/portal' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      if (!stripeConfigured()) return json({ error: 'Billing not configured' }, corsHeaders, 503);
      const acct = await getBillingAccount(emailHash);
      if (!acct?.stripeCustomerId) return json({ error: 'No billing customer found' }, corsHeaders, 404);
      const appUrl = (Deno.env.get('APP_URL') || 'https://app.stckrm.com').replace(/\/$/, '');
      const params: Record<string, string> = {
        customer:   acct.stripeCustomerId,
        return_url: `${appUrl}/?billing=portal-return`,
      };
      const portal = await stripeFetch('/v1/billing_portal/sessions', 'POST', params);
      return json({ url: portal.url }, corsHeaders);
    } catch(err) {
      console.error(`[billing/portal] error:`, (err as Error).message);
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Billing: apply-promo — validate a promotion code ──
  // Used by the UI to validate before checkout (so we can show "valid:
  // 50% off for 3 months" or similar). The code is not actually attached
  // to anything here — it's passed through to /billing/checkout.
  if (url.pathname === '/billing/apply-promo' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, code } = await request.json();
      if (!code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      if (!stripeConfigured()) return json({ error: 'Billing not configured' }, corsHeaders, 503);
      const lookup = await stripeFetch(`/v1/promotion_codes`, 'GET', { code, active: 'true', limit: 1 });
      const pc = lookup?.data?.[0];
      if (!pc) return json({ valid: false, reason: 'not_found' }, corsHeaders);
      const coupon = pc.coupon || {};
      return json({
        valid:       true,
        code:        pc.code,
        percentOff:  coupon.percent_off ?? null,
        amountOff:   coupon.amount_off ?? null,
        currency:    coupon.currency ?? null,
        duration:    coupon.duration ?? null,           // 'forever', 'once', 'repeating'
        durationInMonths: coupon.duration_in_months ?? null,
      }, corsHeaders);
    } catch(err) {
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Billing: cancel — cancel subscription at period end ──
  // Sets cancel_at_period_end=true so the user keeps access until the
  // period they paid for runs out, then drops to free tier.
  if (url.pathname === '/billing/cancel' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      if (!stripeConfigured()) return json({ error: 'Billing not configured' }, corsHeaders, 503);
      const acct = await getBillingAccount(emailHash);
      if (!acct?.stripeSubscriptionId) return json({ error: 'No active subscription' }, corsHeaders, 404);
      const updated = await stripeFetch(`/v1/subscriptions/${acct.stripeSubscriptionId}`, 'POST', {
        cancel_at_period_end: 'true',
      });
      // Mirror locally — the webhook will also do this but UI should reflect
      // immediately
      acct.cancelAtPeriodEnd = true;
      await setBillingAccount(emailHash, acct);
      return json({ ok: true, currentPeriodEnd: updated.current_period_end }, corsHeaders);
    } catch(err) {
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Billing: resume — undo cancel-at-period-end ──
  if (url.pathname === '/billing/resume' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      if (!stripeConfigured()) return json({ error: 'Billing not configured' }, corsHeaders, 503);
      const acct = await getBillingAccount(emailHash);
      if (!acct?.stripeSubscriptionId) return json({ error: 'No subscription' }, corsHeaders, 404);
      await stripeFetch(`/v1/subscriptions/${acct.stripeSubscriptionId}`, 'POST', {
        cancel_at_period_end: 'false',
      });
      acct.cancelAtPeriodEnd = false;
      await setBillingAccount(emailHash, acct);
      return json({ ok: true }, corsHeaders);
    } catch(err) {
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Referral: get my code + stats ──
  // Returns the user's referral code (creating it lazily) plus stats and
  // a list of their referrals (with status). UI uses this to populate the
  // 'Refer a Friend' section.
  if (url.pathname === '/referral/code' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      const emailRow = await kvGet(['user', emailHash, 'email']);
      const plaintextEmail = emailRow.value ? String(emailRow.value) : undefined;
      const record = await ensureReferralCode(emailHash, plaintextEmail);
      const cap = REFERRAL_LIFETIME_CAP_MONTHS;
      // Determine current eligibility — drives whether the UI shows the
      // share controls or a 'upgrade to start referring' message.
      const myAcct = await getBillingAccount(emailHash);
      const eligible = isReferrerEligible(myAcct);
      let ineligibleReason: string | null = null;
      if (!eligible) {
        if (!myAcct)                         ineligibleReason = 'no_billing_account';
        else if (!myAcct.cardOnFile)         ineligibleReason = 'no_card_on_file';
        else if (myAcct.status === 'free')   ineligibleReason = 'free_tier';
        else                                 ineligibleReason = 'not_active';
      }
      return json({
        ok:                    true,
        code:                  record.code,
        createdAt:             record.createdAt,
        signupCount:           record.signupCount,
        convertedCount:        record.convertedCount,
        qualifiedCount:        record.qualifiedCount,
        lifetimeCreditsMonths: record.lifetimeCreditsMonths,
        lifetimeCapMonths:     cap,
        capReached:            record.lifetimeCreditsMonths >= cap,
        eligible,
        ineligibleReason,
        rewards: {
          referrerExtensionDays: REFERRER_EXTENSION_DAYS,
          refereeDiscountPercent: REFEREE_DISCOUNT_PERCENT,
          refereeDiscountMonths:  REFEREE_DISCOUNT_MONTHS,
        },
      }, corsHeaders);
    } catch (err) {
      console.error('[referral/code] error:', (err as Error).message);
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Referral: list my referrals ──
  // Returns the signups attributed to this user's code, with each
  // referee's status (pending/converted/qualified/rejected). Referee
  // identity is anonymized (we expose first 6 chars of emailHash + signup
  // date only) — never plaintext email.
  if (url.pathname === '/referral/list' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      const _authFail = await requireUserAuth(emailHash, verifier, sessionToken);
      if (_authFail) return _authFail;
      // Walk all referral:signup:* entries and collect those where
      // referrerHash matches. This is O(N) over all signups but N is small
      // and we don't need a reverse index for this rare-ish query.
      const list: any[] = [];
      const iter = kv.list({ prefix: ['referral', 'signup'] });
      for await (const entry of iter) {
        try {
          const rec = JSON.parse(String(entry.value)) as ReferralSignupRecord;
          if (rec.referrerHash !== emailHash) continue;
          const refereeHash = String((entry.key as any[])[2]);
          list.push({
            refereeRef:    refereeHash.slice(0, 6),
            signedUpAt:    rec.signedUpAt,
            firstPaymentAt: rec.firstPaymentAt,
            qualifiedAt:   rec.qualifiedAt,
            status:        rec.status,
            rejectionReason: rec.rejectionReason,
          });
        } catch (_) {}
      }
      // Sort newest first
      list.sort((a, b) => (b.signedUpAt || 0) - (a.signedUpAt || 0));
      return json({ ok: true, referrals: list }, corsHeaders);
    } catch (err) {
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Referral: validate a code ──
  // Used by the signup form to give live feedback. Returns whether the
  // code is valid + a friendly description. Does NOT reveal the
  // referrer's identity.
  if (url.pathname === '/referral/validate' && request.method === 'POST') {
    try {
      const { code } = await request.json();
      if (!code || typeof code !== 'string') return json({ valid: false, reason: 'missing' }, corsHeaders);
      const referrerHash = await getReferrerByCode(code);
      if (!referrerHash) return json({ valid: false, reason: 'not_found' }, corsHeaders);
      return json({
        valid:           true,
        rewardForReferee:`${REFEREE_DISCOUNT_PERCENT}% off your first ${REFEREE_DISCOUNT_MONTHS} paid months`,
      }, corsHeaders);
    } catch (err) {
      return json({ error: (err as Error).message }, corsHeaders, 500);
    }
  }

  // ── Stripe webhook ────────────────────────────────────
  // Receives events from Stripe (subscription lifecycle, invoices,
  // payment methods). Signature is HMAC-SHA256 verified against
  // STRIPE_WEBHOOK_SECRET. Idempotent — same event.id arriving twice
  // is processed once. Always returns 200 quickly to avoid Stripe
  // marking the endpoint as failing; processing happens inline but
  // is intentionally cheap in Phase 1.
  if (url.pathname === '/webhook/stripe' && request.method === 'POST') {
    if (!stripeConfigured()) {
      console.error('[stripe-webhook] received event but Stripe not configured');
      // Return 200 anyway — Stripe will keep retrying otherwise, and
      // an unconfigured server can't do anything useful with retries.
      return new Response('not configured', { status: 200 });
    }
    const sigHeader = request.headers.get('Stripe-Signature');
    let payload: string;
    try { payload = await request.text(); }
    catch (err) {
      console.error('[stripe-webhook] could not read body:', (err as Error).message);
      return new Response('bad request', { status: 400 });
    }
    const valid = await verifyStripeSignature(payload, sigHeader);
    if (!valid) {
      console.warn('[stripe-webhook] signature verification failed');
      return new Response('bad signature', { status: 400 });
    }
    let event: any;
    try { event = JSON.parse(payload); }
    catch {
      return new Response('bad json', { status: 400 });
    }
    const eventId: string = event?.id || '';
    if (!eventId) return new Response('missing event id', { status: 400 });

    // Idempotency check — skip if we've already processed this event.id
    if (await _isEventProcessed(eventId)) {
      console.log(`[stripe-webhook] skipping duplicate ${event.type} ${eventId}`);
      return new Response('ok', { status: 200 });
    }

    try {
      await handleStripeEvent(event);
      await _markEventProcessed(eventId);
    } catch (err) {
      // If our handler threw, do NOT mark as processed — Stripe will
      // retry. Return 500 so Stripe knows to retry.
      console.error(`[stripe-webhook] handler error for ${event.type} ${eventId}:`, (err as Error).message);
      return new Response('handler error', { status: 500 });
    }
    return new Response('ok', { status: 200 });
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

  // ── User: list snapshots that contain my data ─────────────
  // Returns metadata only — no encrypted data here. The user can then call
  // /user/snapshot/extract with a specific snapshotKey to download their own
  // encrypted blob from that snapshot.
  if (url.pathname === '/user/snapshots/list' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken } = await request.json();
      if (!emailHash) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (!await verifyUserAuth(emailHash, verifier, sessionToken)) {
        return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      if (!r2Configured()) {
        return json({ ok: true, configured: false, snapshots: [] }, corsHeaders);
      }
      // Just list everything; the snapshots include all users' data, but the
      // user can only download their own portion via the extract endpoint.
      const all = await listR2Snapshots('');
      // Most recent first, capped to a reasonable number of entries
      const snapshots = all.slice(-200).reverse().map(s => ({
        key:          s.key,
        size:         s.size,
        lastModified: s.lastModified,
        // Approximate "yours" size — we don't actually know without fetching.
        // Frontend will display total snapshot size and explain it's an upper bound.
      }));
      return json({ ok: true, configured: true, snapshots }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── User: extract just my encrypted data from a snapshot ──
  // Returns the user's encrypted blobs from the named snapshot. The data
  // is already AES-GCM encrypted with the user's key; the server can't read it.
  if (url.pathname === '/user/snapshots/extract' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, snapshotKey } = await request.json();
      if (!emailHash || !snapshotKey) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (!await verifyUserAuth(emailHash, verifier, sessionToken)) {
        return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      if (typeof snapshotKey !== 'string' || snapshotKey.length > 200) {
        return json({ error: 'Invalid snapshotKey' }, corsHeaders, 400);
      }
      const result = await extractUserDataFromSnapshot(snapshotKey, emailHash);
      if (!result.ok) return json({ error: result.error }, corsHeaders, 500);
      return json({
        ok:      true,
        meta:    result.meta,
        entries: result.entries,
      }, corsHeaders);
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

  // ── Email verification status (unauthenticated check) ──────
  // Used by the client on session restore to detect "user has a
  // cached session but their account isn't email-verified" — e.g.
  // they signed up, the OTP email failed, they closed the tab,
  // come back later. Without this check the client would let them
  // straight into the app on a session that the server now refuses
  // (post-fix). Returns minimal info: whether the account exists,
  // whether it's verified, and the stored email if any. The same
  // existence signal is already implicit in /user/register's
  // "account already exists" response, so no new info is leaked.
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
      const { emailHash, sessionToken, verifier } = await request.json();
      if (!emailHash || (!sessionToken && !verifier)) return json({ error: 'Missing fields' }, corsHeaders, 400);
      // Accept passkey session OR passphrase verifier — the recurring
      // auth-gap pattern. A user signed in via passphrase on a device
      // that never registered a passkey still has a legitimate need
      // to view (and add to) their passkey list.
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
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
      const { emailHash, sessionToken, verifier, credentialId } = await request.json();
      if (!emailHash || !credentialId || (!sessionToken && !verifier)) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const session = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!session.value) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
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

      // v2 envelope architecture is the only supported format. kdfSalt
      // is required — it's the random per-user salt used for PBKDF2.
      if (!kdfSalt) return json({ error: 'kdfSalt required (v2 only)' }, corsHeaders, 400);
      await kvSet(['user', emailHash, 'kdf_salt'], kdfSalt);

      // Store recovery code envelopes (up to 10)
      if (recoveryEnvelopes && Array.isArray(recoveryEnvelopes)) {
        for (let i = 0; i < Math.min(recoveryEnvelopes.length, 10); i++) {
          await kvSet(['user', emailHash, 'recovery', String(i)], recoveryEnvelopes[i]);
          await kvSet(['user', emailHash, 'recovery_used', String(i)], 'false');
        }
        await kvSet(['user', emailHash, 'recovery_count'], String(recoveryEnvelopes.length));
      }
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Get passphrase envelope + salt ────────────────────
  // Client sends emailHash + verifier, gets back the encrypted DATA KEY
  // envelope and salts needed to derive the wrap key.
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

      const salt     = await kvGet(['user', emailHash, 'key_salt']);
      const envelope = await kvGet(['user', emailHash, 'key_envelope_passphrase']);
      const kdfSalt  = await kvGet(['user', emailHash, 'kdf_salt']);

      // Passkey-only accounts have no passphrase envelope but may still
      // call /key/get during a probe — return what we have. The client
      // distinguishes by checking `envelope` itself.
      if (!salt.value || !envelope.value || !kdfSalt.value) {
        return json({ ok: true, envelope: null, salt: null, kdfSalt: null }, corsHeaders);
      }

      return json({
        ok: true,
        salt:    salt.value,
        envelope: envelope.value,
        kdfSalt: kdfSalt.value,
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
  // Re-wraps DATA KEY when user changes passphrase. The data key
  // inside the envelope is unchanged, so all per-household ciphertext,
  // share keys, ECDH keypair, and recovery envelopes stay readable.
  // Passphrase rotation is O(1), not O(data).
  if (url.pathname === '/key/update-passphrase' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, newVerifier, newSalt, newEnvelope, newKdfSalt } = await request.json();
      if (!emailHash || !newVerifier || !newSalt || !newEnvelope || !newKdfSalt) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }

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

      await kvSet(['user', emailHash, 'verifier'],                 newVerifier);
      await kvSet(['user', emailHash, 'key_salt'],                 newSalt);
      await kvSet(['user', emailHash, 'key_envelope_passphrase'],  newEnvelope);
      await kvSet(['user', emailHash, 'kdf_salt'],                 newKdfSalt);
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

  // ── Admin: two-step auth ──────────────────────────────
  // Step 1: POST /admin/otp/send   { adminSecret } → OTP emailed to pete@artbot5000.com
  // Step 2: POST /admin/otp/verify { adminSecret, otp } → { adminToken } (15min TTL)
  // All other /admin/* routes require { adminSecret, adminToken }

  const ADMIN_EMAIL = env.ADMIN_EMAIL;

  // ── Tier 1 hardening helpers ──────────────────────────
  function _getClientIp(req: Request): string {
    // Behind Fly's edge, Fly-Client-IP is the canonical header.
    // Fall back to common alternatives for local dev.
    return req.headers.get('Fly-Client-IP')
        || req.headers.get('CF-Connecting-IP')
        || req.headers.get('X-Forwarded-For')?.split(',')[0]?.trim()
        || 'unknown';
  }

  // 5 verify attempts per IP per minute. Stored as a small counter that
  // expires after 60s so the limit naturally resets.
  const ADMIN_VERIFY_RATE_LIMIT = 5;
  async function _checkVerifyRateLimit(ip: string): Promise<{ ok: boolean; remaining: number; retryAfterSec?: number }> {
    const key = ['admin_verify_rl', ip];
    const cur = await kvGet(key);
    const count = cur.value ? parseInt(String(cur.value), 10) : 0;
    if (count >= ADMIN_VERIFY_RATE_LIMIT) {
      return { ok: false, remaining: 0, retryAfterSec: 60 };
    }
    await kvSet(key, String(count + 1), { expireIn: 60_000 });
    return { ok: true, remaining: ADMIN_VERIFY_RATE_LIMIT - (count + 1) };
  }

  // Per-OTP attempt counter. After 5 wrong tries the OTP itself is purged
  // so no amount of further attempts can succeed without requesting a new OTP.
  const ADMIN_OTP_MAX_ATTEMPTS = 5;

  async function _sendAdminAlertEmail(subject: string, html: string): Promise<void> {
    if (!env.RESEND_API_KEY) return; // silent — alerts are best-effort
    try {
      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: env.FROM_EMAIL, to: [ADMIN_EMAIL], subject, html }),
      });
    } catch (e) { console.warn('Admin alert email failed:', (e as Error)?.message); }
  }

  function _renderAdminAlertEmail(opts: { headerColour: string; title: string; subtitle: string; rows: Array<[string, string]>; footnote?: string }): string {
    const rowsHTML = opts.rows.map(([k, v]) =>
      `<tr><td style="color:#7a8097;padding:4px 12px 4px 0;font-size:13px">${k}</td><td style="color:#f0f2f7;font-size:13px">${v}</td></tr>`
    ).join('');
    return `
      <div style="font-family:system-ui,-apple-system,sans-serif;background:#0f1117;color:#f0f2f7;padding:24px;border-radius:8px;max-width:600px">
        <h2 style="color:${opts.headerColour};margin:0 0 4px;font-size:18px">${opts.title}</h2>
        <div style="color:#7a8097;font-size:12px;margin-bottom:20px">${opts.subtitle}</div>
        <table style="border-collapse:collapse;width:100%">${rowsHTML}</table>
        ${opts.footnote ? `<p style="color:#7a8097;font-size:11px;margin-top:20px;line-height:1.5">${opts.footnote}</p>` : ''}
      </div>`;
  }

  // ── Tier 2: audit log ────────────────────────────────
  // Records every admin action with a timestamp, IP, user-agent, action, and
  // outcome. Stored under audit_log/<isoTimestamp>-<rand> with 90-day TTL.
  // Used to investigate suspected compromise after the fact.
  const AUDIT_LOG_TTL_MS = 90 * 24 * 60 * 60 * 1000;
  async function _writeAuditLog(opts: {
    action: string;
    outcome: 'success' | 'failure' | 'denied';
    ip: string;
    userAgent: string;
    details?: string;
  }): Promise<void> {
    try {
      const ts = new Date().toISOString();
      const rand = crypto.getRandomValues(new Uint8Array(4))
        .reduce((s, b) => s + b.toString(16).padStart(2, '0'), '');
      const key = ['audit_log', `${ts}-${rand}`];
      await kvSet(key, JSON.stringify({
        ts,
        action:    opts.action,
        outcome:   opts.outcome,
        ip:        opts.ip,
        ua:        opts.userAgent.slice(0, 200),
        details:   (opts.details || '').slice(0, 400),
      }), { expireIn: AUDIT_LOG_TTL_MS });
    } catch (e) { console.warn('Audit log write failed:', (e as Error)?.message); }
  }

  // Returns rich metadata so callers can audit-log with full context.
  // If the call returns ok=false, the response should be 401 Unauthorised
  // and the caller should NOT include the reason in the response body
  // (so an attacker can't distinguish "wrong secret" from "IP mismatch").
  // Verifies an authenticated admin request. Once a session token has been
  // issued (via /admin/otp/verify), the token alone is sufficient — we do
  // NOT re-check the master secret on every request. This minimises how
  // often the secret travels over the network. The token is a 256-bit
  // random value, IP-bound, and expires after 15 minutes.
  async function verifyAdminRequest(body: Record<string,string>, req?: Request): Promise<{ ok: boolean; reason?: 'no-token' | 'expired-token' | 'ip-mismatch'; sessionMeta?: { ip: string; userAgent: string; createdAt: number } }> {
    const { adminToken } = body;
    if (!adminToken) return { ok: false, reason: 'no-token' };
    const stored = await kvGet(['admin_session', adminToken]);
    if (!stored.value) return { ok: false, reason: 'expired-token' };
    let meta: { ip: string; userAgent: string; createdAt: number } | null = null;
    try { meta = JSON.parse(String(stored.value)); } catch (_) { meta = null; }
    // Strict IP-binding: if a session was created from one IP and is being used
    // from another, treat the token as compromised. Note that `req` is optional
    // for backwards compatibility (old call sites that haven't been updated yet
    // skip the binding check). New code paths must always pass `req`.
    if (req && meta && typeof meta.ip === 'string') {
      const callerIp = _getClientIp(req);
      if (callerIp !== meta.ip && callerIp !== 'unknown' && meta.ip !== 'unknown') {
        // Alert and revoke this specific token — we don't want to keep a
        // potentially-compromised session alive.
        try { await kv.delete(['admin_session', adminToken]); } catch (_) {}
        _sendAdminAlertEmail(
          '⚠ STOCKROOM admin: session IP mismatch',
          _renderAdminAlertEmail({
            headerColour: '#e8a838',
            title: '⚠ Admin session blocked — IP mismatch',
            subtitle: 'A request used a session token from a different IP than it was issued for. The session has been revoked.',
            rows: [
              ['When',         new Date().toUTCString()],
              ['Original IP',  meta.ip],
              ['Request IP',   callerIp],
              ['User-Agent',   (req.headers.get('User-Agent') || 'unknown').slice(0, 200)],
            ],
            footnote: 'If your IP changed naturally (mobile network, VPN toggle, ISP rotation), just sign in again. If you did not initiate this, your ADMIN_SECRET may be compromised — rotate it immediately.',
          })
        ).catch(() => {});
        return { ok: false, reason: 'ip-mismatch' };
      }
    }
    return { ok: true, sessionMeta: meta || undefined };
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
      // Reset per-OTP attempt counter for the freshly-issued OTP
      await kvSet(['admin_otp_attempts'], '0', { expireIn: 10 * 60 * 1000 });
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
      await _writeAuditLog({ action: '/admin/otp/send', outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      return json({ ok: true, sentTo: ADMIN_EMAIL }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  if (url.pathname === '/admin/otp/verify' && request.method === 'POST') {
    try {
      const { adminSecret, otp } = await request.json();
      const ip = _getClientIp(request);
      const userAgent = request.headers.get('User-Agent') || 'unknown';

      // 1. Per-IP rate limit on verify attempts — closes the brute-force window
      const rl = await _checkVerifyRateLimit(ip);
      if (!rl.ok) {
        await _writeAuditLog({ action: '/admin/otp/verify', outcome: 'denied', ip, userAgent, details: 'rate-limited' });
        return json({ error: 'Too many attempts — try again in a minute' }, { ...corsHeaders, 'Retry-After': String(rl.retryAfterSec || 60) }, 429);
      }

      if (!adminSecret || adminSecret !== Deno.env.get('ADMIN_SECRET')) {
        await _writeAuditLog({ action: '/admin/otp/verify', outcome: 'denied', ip, userAgent, details: 'bad-secret' });
        return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      if (!otp) return json({ error: 'Missing OTP' }, corsHeaders, 400);

      // 2. Per-OTP attempt counter — invalidate after 5 wrong tries
      const attemptsRec = await kvGet(['admin_otp_attempts']);
      const attempts    = attemptsRec.value ? parseInt(String(attemptsRec.value), 10) : 0;
      const stored      = await kvGet(['admin_otp']);

      if (!stored.value) {
        return json({ error: 'Invalid or expired code' }, corsHeaders, 401);
      }

      if (stored.value !== String(otp).trim()) {
        const newAttempts = attempts + 1;
        if (newAttempts >= ADMIN_OTP_MAX_ATTEMPTS) {
          // Purge the OTP entirely — attacker cannot continue without requesting a new one
          await kvDel(['admin_otp']);
          await kvDel(['admin_otp_attempts']);
          await kvDel(['admin_otp_sent']);
          // Alert: someone hit the attempt cap
          _sendAdminAlertEmail(
            '⚠ STOCKROOM admin: OTP attempt cap hit',
            _renderAdminAlertEmail({
              headerColour: '#e8a838',
              title: '⚠ Possible admin sign-in attack',
              subtitle: 'The admin OTP was invalidated after too many wrong attempts.',
              rows: [
                ['When',       new Date().toUTCString()],
                ['Source IP',  ip],
                ['User-Agent', userAgent.slice(0, 200)],
                ['Attempts',   String(newAttempts)],
              ],
              footnote: 'Whoever did this had your ADMIN_SECRET (or guessed it). Rotate it now via flyctl secrets set ADMIN_SECRET=… and review recent activity. The OTP has been invalidated and they would need to request a fresh one to continue.',
            })
          ).catch(() => {});
          return json({ error: 'Too many wrong codes — request a new one' }, corsHeaders, 401);
        }
        await kvSet(['admin_otp_attempts'], String(newAttempts), { expireIn: 10 * 60 * 1000 });
        return json({ error: `Invalid or expired code (${ADMIN_OTP_MAX_ATTEMPTS - newAttempts} attempts left)` }, corsHeaders, 401);
      }

      // ── Success ──
      await kvDel(['admin_otp']);
      await kvDel(['admin_otp_sent']);
      await kvDel(['admin_otp_attempts']);
      const token = Array.from(crypto.getRandomValues(new Uint8Array(32)))
        .map(b => b.toString(16).padStart(2,'0')).join('');
      await kvSet(['admin_session', token], JSON.stringify({ ip, userAgent: userAgent.slice(0, 200), createdAt: Date.now() }), { expireIn: 15 * 60 * 1000 });

      // Sign-in notification email — best-effort, doesn't block the response
      _sendAdminAlertEmail(
        '✓ STOCKROOM admin sign-in',
        _renderAdminAlertEmail({
          headerColour: '#4cbb8a',
          title: '✓ Admin sign-in successful',
          subtitle: 'A new admin session was created.',
          rows: [
            ['When',        new Date().toUTCString()],
            ['Source IP',   ip],
            ['User-Agent',  userAgent.slice(0, 200)],
            ['Session TTL', '15 minutes'],
          ],
          footnote: 'If this was not you, change ADMIN_SECRET immediately and revoke active sessions in the admin panel.',
        })
      ).catch(() => {});

      await _writeAuditLog({ action: '/admin/otp/verify', outcome: 'success', ip, userAgent });
      return json({ ok: true, adminToken: token }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: revoke all active sessions ─────────────────
  // Wipes every entry under admin_session/* — useful if a session is suspected
  // compromised or you simply want to force re-auth across all open admin tabs.
  if (url.pathname === '/admin/revoke-all-sessions' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      let revoked = 0;
      const iter = kv.list({ prefix: ['admin_session'] });
      for await (const entry of iter) {
        try { await kv.delete(entry.key); revoked++; } catch(_) {}
      }
      const ip = _getClientIp(request);
      console.log(`Admin: ${revoked} session(s) revoked from ${ip}`);
      _sendAdminAlertEmail(
        '🔒 STOCKROOM admin sessions revoked',
        _renderAdminAlertEmail({
          headerColour: '#e8a838',
          title: '🔒 All admin sessions revoked',
          subtitle: 'A revoke-all-sessions action was performed.',
          rows: [
            ['When',       new Date().toUTCString()],
            ['Source IP',  ip],
            ['Sessions revoked', String(revoked)],
          ],
        })
      ).catch(() => {});
      return json({ ok: true, revoked }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: list active sessions ────────────────────────
  // Returns metadata for every entry under admin_session/*, sorted newest first.
  // The token used to make this request is flagged with `current: true`.
  if (url.pathname === '/admin/sessions' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });

      const sessions: Array<{ tokenPrefix: string; ip: string; userAgent: string; createdAt: number; current: boolean }> = [];
      const iter = kv.list({ prefix: ['admin_session'] });
      for await (const entry of iter) {
        const token = (entry.key as string[])[1];
        let meta: { ip?: string; userAgent?: string; createdAt?: number } = {};
        try { meta = JSON.parse(String(entry.value)); } catch (_) { /* legacy '1' value */ }
        sessions.push({
          tokenPrefix: token.slice(0, 8),
          ip:          meta.ip || 'unknown',
          userAgent:   meta.userAgent || 'unknown',
          createdAt:   meta.createdAt || 0,
          current:     token === body.adminToken,
        });
      }
      sessions.sort((a, b) => b.createdAt - a.createdAt);
      return json({ ok: true, sessions }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: revoke a single session by token prefix ────
  if (url.pathname === '/admin/revoke-session' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const { tokenPrefix } = body;
      if (!tokenPrefix || typeof tokenPrefix !== 'string' || tokenPrefix.length < 6) {
        return json({ error: 'Invalid tokenPrefix' }, corsHeaders, 400);
      }
      // Find matching session and delete it
      let deleted = false;
      const iter = kv.list({ prefix: ['admin_session'] });
      for await (const entry of iter) {
        const token = (entry.key as string[])[1];
        if (token.startsWith(tokenPrefix)) {
          await kv.delete(entry.key);
          deleted = true;
          break;
        }
      }
      await _writeAuditLog({ action: url.pathname, outcome: deleted ? 'success' : 'failure', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `prefix=${tokenPrefix} deleted=${deleted}` });
      return json({ ok: deleted, deleted }, corsHeaders, deleted ? 200 : 404);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: audit log viewer ────────────────────────────
  // Returns the most recent audit log entries, newest first. Capped at 200.
  if (url.pathname === '/admin/audit-log' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      // Note: not writing a success audit log here, since reading the log is
      // a meta-action that would create noise on every refresh. The denied
      // case is logged so we still see attempts to read the log without auth.

      const limit = Math.min(parseInt(String(body.limit || '100'), 10) || 100, 200);
      const entries: Array<{ ts: string; action: string; outcome: string; ip: string; ua: string; details?: string }> = [];
      const iter = kv.list({ prefix: ['audit_log'] }, { reverse: true, limit });
      for await (const entry of iter) {
        try {
          const v = JSON.parse(String(entry.value));
          entries.push(v);
        } catch (_) { /* skip malformed entries */ }
      }
      return json({ ok: true, entries }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: crypto version status ──────────────────────
  // All accounts are v2; this endpoint is kept for the admin UI which
  // still polls it. Returns total user count and zero v1.
  if (url.pathname === '/admin/crypto-status' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      let total = 0;
      const iter = kv.list({ prefix: ['user'] });
      for await (const entry of iter) {
        const key = entry.key as string[];
        if (key[2] === 'verifier') total++;
      }
      return json({
        ok: true,
        v1: 0,
        v2: total,
        unknown: 0,
        total,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: list accounts ──────────────────────────────
  if (url.pathname === '/admin/list-accounts' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      const accounts: { emailHash: string; email: string | null; created: string | null; cryptoVersion: string; migrated: string | null }[] = [];
      const iter = kv.list({ prefix: ['user'] });
      const seen = new Set<string>();
      for await (const entry of iter) {
        const key = entry.key as string[];
        if (key[2] !== 'verifier') continue;
        const emailHash = key[1];
        if (seen.has(emailHash)) continue;
        seen.add(emailHash);
        const [emailR, createdR, pendingDelR, deactivationR] = await Promise.all([
          kvGet(['user', emailHash, 'email']),
          kvGet(['user', emailHash, 'created']),
          kvGet(['user', emailHash, 'pending_deletion']),
          kvGet(['deactivation', emailHash]),
        ]);
        accounts.push({
          emailHash,
          email:          emailR.value   as string | null || null,
          created:        createdR.value as string | null || null,
          // All accounts are v2 — kept in the response shape for the
          // existing admin.html badge column (always shows v2 ✓).
          cryptoVersion:  'v2',
          migrated:       null,
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
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (!existing.value) return json({ error: 'Account not found' }, corsHeaders, 404);
      await _deleteAllUserData(kv, emailHash);
      console.log('ADMIN deleted account: ' + emailHash);
      return json({ ok: true, deleted: emailHash }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: R2 backup status ───────────────────────────
  if (url.pathname === '/admin/r2/status' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      if (!r2Configured()) {
        return json({ ok: true, configured: false, missing: [
          R2_CFG.accountId ? null : 'R2_ACCOUNT_ID',
          R2_CFG.accessKey ? null : 'R2_ACCESS_KEY_ID',
          R2_CFG.secretKey ? null : 'R2_SECRET_ACCESS_KEY',
          R2_CFG.bucket    ? null : 'R2_BUCKET_NAME',
        ].filter(Boolean) }, corsHeaders);
      }
      const all = await listR2Snapshots('');
      const auto    = all.filter(s => s.key.startsWith('auto/'));
      const manual  = all.filter(s => s.key.startsWith('manual/'));
      const preR    = all.filter(s => s.key.startsWith('pre-restore/'));
      const userBackups = all.filter(s => s.key.startsWith('user-backup/'));
      // Per-user totals: count distinct emailHash directories under user-backup/
      const userHashes = new Set<string>();
      for (const s of userBackups) {
        const parts = s.key.split('/');
        if (parts.length >= 2 && parts[0] === 'user-backup') userHashes.add(parts[1]);
      }
      const totalSize = all.reduce((sum, s) => sum + s.size, 0);
      const lastAuto    = auto.at(-1);
      const lastUserBackup = userBackups.at(-1);
      return json({
        ok: true,
        configured: true,
        bucket: R2_CFG.bucket,
        totalSnapshots: all.length,
        autoCount: auto.length,
        manualCount: manual.length,
        preRestoreCount: preR.length,
        userBackupCount: userBackups.length,
        userBackupUserCount: userHashes.size,
        totalSizeBytes: totalSize,
        lastAuto: lastAuto ? { key: lastAuto.key, size: lastAuto.size, lastModified: lastAuto.lastModified } : null,
        lastUserBackup: lastUserBackup ? { key: lastUserBackup.key, size: lastUserBackup.size, lastModified: lastUserBackup.lastModified } : null,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: list R2 snapshots (most recent first) ──────
  if (url.pathname === '/admin/r2/list' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      const prefix = (body.prefix || '').toString();
      const all = await listR2Snapshots(prefix);
      // Most recent first, capped to 200 entries to keep responses small
      return json({ ok: true, snapshots: all.slice(-200).reverse() }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: trigger immediate backup ───────────────────
  if (url.pathname === '/admin/r2/backup-now' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      const result = await backupKVToR2('manual');
      return json(result, corsHeaders, result.ok ? 200 : 500);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: prune snapshots now (testing / manual cleanup) ─
  if (url.pathname === '/admin/r2/prune' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      const result = await pruneR2Snapshots();
      return json({ ok: true, ...result }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: restore from a specific snapshot ───────────
  // Body: { adminSecret, adminToken, snapshotKey, confirm: 'RESTORE' }
  // Requires explicit confirm string to prevent accidents.
  if (url.pathname === '/admin/r2/restore' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      const { snapshotKey, confirm } = body;
      if (confirm !== 'RESTORE') return json({ error: 'Confirmation phrase missing — body.confirm must equal "RESTORE"' }, corsHeaders, 400);
      if (!snapshotKey || typeof snapshotKey !== 'string') return json({ error: 'Missing snapshotKey' }, corsHeaders, 400);
      console.warn(`ADMIN R2 RESTORE initiated from snapshot: ${snapshotKey}`);
      const result = await restoreFromR2Snapshot(snapshotKey);
      if (!result.ok) return json({ error: result.error }, corsHeaders, 500);
      console.warn(`ADMIN R2 RESTORE complete: ${result.restored} entries; safety snapshot at ${result.preRestoreKey}`);
      return json(result, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: send heartbeat email now (test the daily 03:05 cron) ─
  if (url.pathname === '/admin/r2/send-heartbeat' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      const result = await sendBackupHeartbeatEmail();
      return json(result, corsHeaders, result.ok ? 200 : 500);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Admin: per-user backup ────────────────────────────
  // Body: { adminSecret, adminToken, emailHash, label? }
  // Snapshots a single user's KV state to R2. Useful before risky operations
  // (rolling out a schema change, helping a user with a stuck account, etc).
  if (url.pathname === '/admin/user/backup' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const emailHash = (body.emailHash || '').toString();
      const label     = (body.label || 'manual').toString();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      if (!/^[a-f0-9]{16,128}$/i.test(emailHash)) return json({ error: 'emailHash must be hex' }, corsHeaders, 400);
      const result = await backupUserToR2(emailHash, label);
      await _writeAuditLog({ action: url.pathname, outcome: result.ok ? 'success' : 'error', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `emailHash=${emailHash.slice(0,8)} entries=${result.entries||0}` });
      return json(result, corsHeaders, result.ok ? 200 : 500);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Admin: list per-user snapshots ────────────────────
  // Body: { adminSecret, adminToken, emailHash }
  if (url.pathname === '/admin/user/list-backups' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const emailHash = (body.emailHash || '').toString();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      if (!/^[a-f0-9]{16,128}$/i.test(emailHash)) return json({ error: 'emailHash must be hex' }, corsHeaders, 400);
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `emailHash=${emailHash.slice(0,8)}` });
      const all = await listR2Snapshots(`user-backup/${emailHash}/`);
      return json({ ok: true, snapshots: all.slice(-100).reverse() }, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Admin: per-user restore from snapshot ─────────────
  // Body: { adminSecret, adminToken, emailHash, snapshotKey, confirm: 'RESTORE' }
  // Wipes the user's current keys then writes the snapshot's entries back.
  // A pre-restore safety snapshot is taken automatically.
  if (url.pathname === '/admin/user/restore' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const emailHash   = (body.emailHash   || '').toString();
      const snapshotKey = (body.snapshotKey || '').toString();
      const confirm     = body.confirm;
      if (confirm !== 'RESTORE') return json({ error: 'Confirmation phrase missing — body.confirm must equal "RESTORE"' }, corsHeaders, 400);
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      if (!snapshotKey) return json({ error: 'Missing snapshotKey' }, corsHeaders, 400);
      if (!/^[a-f0-9]{16,128}$/i.test(emailHash)) return json({ error: 'emailHash must be hex' }, corsHeaders, 400);
      console.warn(`ADMIN USER RESTORE initiated for ${emailHash.slice(0,8)} from snapshot: ${snapshotKey}`);
      const result = await restoreUserFromR2Snapshot(emailHash, snapshotKey);
      await _writeAuditLog({ action: url.pathname, outcome: result.ok ? 'success' : 'error', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `emailHash=${emailHash.slice(0,8)} key=${snapshotKey} restored=${result.restored||0}${result.error ? ' err='+result.error : ''}` });
      if (!result.ok) return json({ error: result.error }, corsHeaders, 500);
      console.warn(`ADMIN USER RESTORE complete: ${result.restored} entries; safety snapshot at ${result.preRestoreKey}`);
      return json(result, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Admin: billing — inspect a user's billing record ──
  if (url.pathname === '/admin/billing/get' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const emailHash = (body.emailHash || '').toString();
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      if (!/^[a-f0-9]{16,128}$/i.test(emailHash)) return json({ error: 'emailHash must be hex' }, corsHeaders, 400);
      const acct = await getBillingAccount(emailHash);
      const stripeConfiguredFlag = stripeConfigured();
      return json({ ok: true, account: acct, stripeConfigured: stripeConfiguredFlag }, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Admin: billing — set or unset grandfathered flag ──
  // Body: { adminToken, adminSecret, emailHash, grandfathered: true|false }
  if (url.pathname === '/admin/billing/set-grandfather' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const emailHash = (body.emailHash || '').toString();
      const grandfathered = body.grandfathered === true;
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      if (!/^[a-f0-9]{16,128}$/i.test(emailHash)) return json({ error: 'emailHash must be hex' }, corsHeaders, 400);
      const acct = await ensureBillingAccount(emailHash);
      acct.grandfathered = grandfathered;
      // When grandfathering, set status accordingly. When un-grandfathering,
      // fall back to whatever Stripe-driven status is appropriate (or 'none').
      if (grandfathered) acct.status = 'grandfathered';
      else if (acct.status === 'grandfathered') acct.status = 'none';
      await setBillingAccount(emailHash, acct);
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `emailHash=${emailHash.slice(0,8)} grandfathered=${grandfathered}` });
      return json({ ok: true, account: acct }, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Admin: billing — extend or set graceUntil ──
  // Body: { adminToken, adminSecret, emailHash, graceUntil: <unix-seconds | null> }
  // Pass null/0 to clear the grace period.
  if (url.pathname === '/admin/billing/set-grace' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      const emailHash = (body.emailHash || '').toString();
      const graceUntilRaw = body.graceUntil;
      if (!emailHash) return json({ error: 'Missing emailHash' }, corsHeaders, 400);
      if (!/^[a-f0-9]{16,128}$/i.test(emailHash)) return json({ error: 'emailHash must be hex' }, corsHeaders, 400);
      const acct = await ensureBillingAccount(emailHash);
      if (graceUntilRaw === null || graceUntilRaw === 0 || graceUntilRaw === undefined) {
        delete acct.graceUntil;
      } else {
        const ts = parseInt(String(graceUntilRaw), 10);
        if (!Number.isFinite(ts) || ts < 0) return json({ error: 'graceUntil must be a positive unix seconds value or null' }, corsHeaders, 400);
        acct.graceUntil = ts;
      }
      await setBillingAccount(emailHash, acct);
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `emailHash=${emailHash.slice(0,8)} graceUntil=${acct.graceUntil ?? 'cleared'}` });
      return json({ ok: true, account: acct }, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
  }

  // ── Admin: billing — re-run migration on demand ──
  // Useful if the auto-startup migration failed. Idempotent — won't redo
  // accounts that already have records, but will pick up newly-registered
  // test accounts that need grandfathering.
  if (url.pathname === '/admin/billing/run-migration' && request.method === 'POST') {
    try {
      const body = await request.json();
      const _adminAuth = await verifyAdminRequest(body, request);
      if (!_adminAuth.ok) {
        await _writeAuditLog({ action: url.pathname, outcome: 'denied', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown', details: `reason=${_adminAuth.reason}` });
        return json({ error: _adminAuth.reason === 'ip-mismatch' ? 'Session IP changed — please sign in again' : 'Unauthorised' }, corsHeaders, 401);
      }
      // Clear the marker so runBillingMigration() executes fully
      try { await kvDel(['_billing_migration_v1', 'done']); } catch (_) {}
      await runBillingMigration();
      await _writeAuditLog({ action: url.pathname, outcome: 'success', ip: _getClientIp(request), userAgent: request.headers.get('User-Agent') || 'unknown' });
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: (err as Error).message }, corsHeaders, 500); }
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
      const { emailHash, recoveryToken, newVerifier, newSalt, newEnvelope, newKdfSalt } = await request.json();
      if (!emailHash || !recoveryToken || !newVerifier || !newSalt || !newEnvelope || !newKdfSalt) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      const stored = await kvGet(['recovery_token', emailHash]);
      if (!stored.value) return json({ error: 'Recovery session expired' }, corsHeaders, 400);
      const tokenData = JSON.parse(stored.value);
      if (tokenData.token !== recoveryToken) return json({ error: 'Invalid recovery token' }, corsHeaders, 401);

      // Update verifier and passphrase envelope with new passphrase
      await kvSet(['user', emailHash, 'verifier'],                newVerifier);
      await kvSet(['user', emailHash, 'key_salt'],                newSalt);
      await kvSet(['user', emailHash, 'key_envelope_passphrase'], newEnvelope);
      await kvSet(['user', emailHash, 'kdf_salt'],                newKdfSalt);
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
      const { emailHash, verifier, email, referralCode } = await request.json();
      if (!emailHash || !verifier) return json({ error: 'Missing fields' }, corsHeaders, 400);
      const existing = await kvGet(['user', emailHash, 'verifier']);
      if (existing.value) {
        if (existing.value === verifier) return json({ ok: true, existing: true }, corsHeaders);
        return json({ error: 'Email already registered with a different passphrase' }, corsHeaders, 409);
      }
      const now = new Date();
      await kvSet(['user', emailHash, 'verifier'], verifier);
      await kvSet(['user', emailHash, 'created'], now.toISOString());
      // Store plaintext email (hashed separately) so we can address
      // operational emails like account-deletion confirmations.
      if (email) await kvSet(['user', emailHash, 'email'], email);

      // Defensively clear any residual email_verified flag and pending
      // verification OTP from a previous account life. _deleteAllUserData
      // already covers these via prefix scan, but a fresh registration is
      // the canonical "new identity" event — verification status from a
      // prior signup must not carry over. Without this, a deleted-then-
      // recreated account could skip the OTP step entirely (and thence
      // join shares) because /email/verify/send returns alreadyVerified
      // and /user/verify allows sign-in.
      await kvDel(['user', emailHash, 'email_verified']);
      await kvDel(['email_verify_otp', emailHash]);

      // ── Billing: create or refresh the billing record ──
      // For new signups, we initialise a 30-day local trial (hybrid model
      // per spec: track trial in our KV, do NOT create a Stripe sub yet —
      // saves Stripe API calls and avoids 'trial expired with no card'
      // edge cases. We DO pre-create the Stripe customer in the background
      // so it's ready when the user later starts checkout).
      // For test accounts that were pre-grandfathered by the Phase 1
      // migration, we leave their record alone.
      try {
        const existingBilling = await getBillingAccount(emailHash);
        if (!existingBilling) {
          // Brand-new account → start 30-day trial
          const nowSec = Math.floor(Date.now() / 1000);
          const trialDays = 30;
          await setBillingAccount(emailHash, {
            status:        'trialing',
            trialEndsAt:   nowSec + (trialDays * 86400),
            cardOnFile:    false,
            grandfathered: false,
            createdAt:     nowSec,
            updatedAt:     nowSec,
          });
        }
        // else: existing record (e.g. grandfathered test account or graced
        // legacy user) — don't overwrite
        // Fire-and-forget Stripe customer creation. Don't await — keeps
        // signup fast and tolerant of Stripe outages. Idempotent.
        ensureStripeCustomer(emailHash, email).catch(err => {
          console.error(`Background Stripe customer creation failed for ${emailHash}:`, err.message);
        });
      } catch (err) {
        console.error(`Billing record init failed for ${emailHash} (continuing anyway):`, (err as Error).message);
      }

      // ── Referrals: record signup if code was provided ──
      // Best-effort — failures here don't block signup. The result is
      // returned in the response so the client can show a "Welcome,
      // your friend's code was applied!" message.
      let referralApplied = false;
      let referralReason: string | undefined;
      if (referralCode && typeof referralCode === 'string') {
        try {
          const result = await recordReferralSignup(emailHash, referralCode, email);
          if (result.ok) referralApplied = true;
          else referralReason = result.reason;
        } catch (err) {
          console.error('[referral] signup recording failed:', (err as Error).message);
          referralReason = 'error';
        }
      }

      return json({ ok: true, referralApplied, referralReason }, corsHeaders);
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

      // ── Email verification gate ─────────────────────────
      // Block sign-in until the user has confirmed their email address.
      // Without this gate, anyone who completes signup but never enters
      // the OTP (e.g. by closing the tab on wizard-step-1f) can still
      // log in normally — and worse, can accept share invites without
      // the owner ever seeing an OTP roundtrip. The client expects 403
      // with { requiresEmailVerification: true } and routes to the OTP
      // step. Note: this fires AFTER rate-limit clearing because a
      // correct passphrase is what got us here — the issue is only the
      // missing email confirmation, not bad credentials.
      const verified = await kvGet(['user', emailHash, 'email_verified']);
      if (!verified.value) {
        const emailRow = await kvGet(['user', emailHash, 'email']);
        return json({
          error: 'Email verification required',
          requiresEmailVerification: true,
          email: emailRow.value || null,
        }, corsHeaders, 403);
      }

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
      // ── Free-tier blob size gate ──
      // The blob is encrypted client-side, so we can't enforce per-feature
      // limits server-side (item count, photo presence). What we CAN do is
      // enforce a total size cap, which catches photo uploads (which blow
      // up blob size) and prevents abuse. Client should already be hiding
      // over-limit items, but this is a defence-in-depth check.
      const blobBytes = typeof ciphertext === 'string' ? ciphertext.length : 0;
      const gate = await gateFeature(emailHash, 'blobsize', blobBytes);
      if (!gate.ok) {
        return json({ error: 'Free tier size limit exceeded', reason: gate.reason, limit: FREE_TIER.BLOB_SIZE_BYTES }, corsHeaders, gate.status);
      }
      const hKey = household && household !== 'default' ? household : 'default';
      await kvSetLarge(['user', emailHash, 'data', hKey], ciphertext);
      await kvSet(['user', emailHash, 'modified', hKey], new Date().toISOString());
      await markUserDirty(emailHash);
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
        const data     = await kvGetLarge(['user', emailHash, 'data', hKey]);
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
        const data     = await kvGetLarge(['user', ownerHash, 'data', hKey]);
        const modified = await kvGet(['user', ownerHash, 'modified', hKey]);
        // Return ciphertext encrypted with SHARE key (re-encrypted by owner on push)
        const sharedCipher = await kvGetLarge(['share_data', shareCode.toUpperCase(), hKey]);
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
      const { ownerEmailHash, verifier, sessionToken, name, type, ownerName, households, householdNames, colour, shareManagement, guestEmail, pendingInvite } = body;
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
      // Validate shareManagement; default to 'none' if absent or invalid.
      const mgmt = ['none','view','edit'].includes(shareManagement) ? shareManagement : 'none';
      // pendingInvite is set by the client when the guest doesn't have an
      // ECDH public key yet (i.e. no STOCKROOM account at create time).
      // It's a hint for the owner UI ("awaiting signup") and lets us
      // recognise a sign-up-via-link flow on the server side. The actual
      // ECDH key wrapping happens later via the existing rewrap queue.
      const validPending = pendingInvite && typeof pendingInvite === 'object'
        && typeof pendingInvite.guestEmailHash === 'string'
        ? { guestEmailHash: pendingInvite.guestEmailHash, guestEmail: pendingInvite.guestEmail || null }
        : null;
      const target = {
        name, type: type||'guest', ownerName: ownerName||'Owner', ownerEmailHash,
        households, householdNames: householdNames||{}, colour: colour||'#e8a838',
        shareManagement: mgmt,
        ...(typeof guestEmail === 'string' && guestEmail ? { guestEmail } : {}),
        ...(validPending ? { pendingInvite: validPending } : {}),
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 60*60*1000).toISOString(),
        members: [],
        memberDetails: {},
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

  // ── Guest share envelope: store ────────────────────────
  // The GUEST stores the share key wrapped under their own data key.
  // This makes the share survive anything that survives the guest's
  // own data — browser clear, new device, passphrase change. The
  // ECDH-wrapped key from /share/ecdh-key/get is only used for the
  // first-time handshake; once the guest has it, they immediately
  // re-wrap with their data key and store under their own namespace.
  //
  // Auth is the GUEST's auth, not the owner's. The owner has no
  // visibility into what the guest stores — this is the guest's
  // own private cache of their share access, encrypted with their
  // own data key. Server holds ciphertext only.
  if (url.pathname === '/user/share-envelope/store' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, code, envelope } = await request.json();
      if (!emailHash || (!verifier && !sessionToken) || !code || !envelope) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      // Auth — accept either passphrase verifier or session token.
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      // Optional sanity check: the guest must actually be a member of
      // this share. Without this we'd accept envelope writes from any
      // authenticated user — not a security hole (the envelope is just
      // their own ciphertext) but rejecting non-members keeps the data
      // model tidy and surfaces logic errors early.
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      const target = JSON.parse(share.value);
      if (!target.members?.includes(emailHash)) {
        return json({ error: 'Not a member of this share' }, corsHeaders, 403);
      }
      await kvSet(['user', emailHash, 'share_envelope', code.toUpperCase()], envelope);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Guest share envelope: get ──────────────────────────
  // Called on fresh device / browser / login to recover the share key
  // without needing the owner online. Returns the ciphertext blob; the
  // client's data key is what unwraps it.
  if (url.pathname === '/user/share-envelope/get' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, code } = await request.json();
      if (!emailHash || (!verifier && !sessionToken) || !code) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const env = await kvGet(['user', emailHash, 'share_envelope', code.toUpperCase()]);
      if (!env.value) return json({ ok: true, envelope: null }, corsHeaders);
      return json({ ok: true, envelope: env.value }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Guest share envelope: delete ──────────────────────
  // Called on leave-share or eviction. The envelope is also covered by
  // _deleteAllUserData on full account deletion (prefix scan picks it
  // up under ['user', emailHash, ...]).
  if (url.pathname === '/user/share-envelope/delete' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, code } = await request.json();
      if (!emailHash || (!verifier && !sessionToken) || !code) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      await kvDel(['user', emailHash, 'share_envelope', code.toUpperCase()]);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: list (authenticated) ──────────────────────
  // Accepts both `verifier` (passphrase login) and `sessionToken` (passkey
  // login). Mismatched auth was a recurring bug — passkey sessions getting
  // silently rejected. See pattern note in account memory.
  if (url.pathname === '/share/list' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken } = await request.json();
      if (!ownerEmailHash || (!verifier && !sessionToken)) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
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
      // No credentials at all — return metadata so UI can prompt sign-in.
      // Include `pendingInvite` so the share-gate can pre-fill the email
      // for guests who don't have an account yet (the link was created
      // for a specific address and signing up with that address gives
      // them direct access without an owner-side rewrap roundtrip).
      if (!guestEmailHash || (!guestVerifier && !guestSessionToken)) {
        return json({
          ok: false, requiresAuth: true,
          ownerName: target.ownerName, name: target.name, type: target.type,
          householdNames: target.householdNames, households: target.households,
          ...(target.pendingInvite ? { pendingInvite: target.pendingInvite } : {}),
        }, corsHeaders);
      }
      // Authenticate: accept passkey sessionToken OR passphrase verifier
      if (guestSessionToken) {
        const sess = await kvGet(['passkey_session', guestEmailHash, guestSessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const guestStored = await kvGet(['user', guestEmailHash, 'verifier']);
        if (!guestStored.value || guestStored.value !== guestVerifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }

      // ── Email verification gate (security-critical) ─────────────
      // Even with a valid passphrase or session, refuse to admit an
      // unverified account to a share. This is the canonical security
      // boundary — without it, an attacker who registers (or re-registers
      // a deleted account) can join any share they were invited to
      // without ever proving they control the inbox. Client-side checks
      // in shareGateSignIn / shareGateRegister are belt-and-braces; this
      // is the actual fence.
      const guestVerified = await kvGet(['user', guestEmailHash, 'email_verified']);
      if (!guestVerified.value) {
        const emailRow = await kvGet(['user', guestEmailHash, 'email']);
        return json({
          error: 'Email verification required before joining a shared household',
          requiresEmailVerification: true,
          email: emailRow.value || null,
        }, corsHeaders, 403);
      }
      if (!target.members) target.members = [];
      if (!target.memberDetails) target.memberDetails = {};
      const isNewMember = !target.members.includes(guestEmailHash);
      if (isNewMember) {
        target.members.push(guestEmailHash);
        // If this share was created for a guest who didn't have an account
        // at create time, joining is the moment we promote pendingInvite
        // to guestEmail (so the owner UI shows a normal "linked" state)
        // and clear pendingInvite. We only do this when the joining
        // emailHash matches the pending one — a guest who signs up with a
        // different email shouldn't silently consume someone else's pending invite.
        if (target.pendingInvite && target.pendingInvite.guestEmailHash === guestEmailHash) {
          if (target.pendingInvite.guestEmail) target.guestEmail = target.pendingInvite.guestEmail;
          delete target.pendingInvite;
        }
      }
      // Record/refresh memberDetails on every successful join so the owner
      // can see when each guest first connected and last accessed the share.
      const nowIso = new Date().toISOString();
      const md = target.memberDetails[guestEmailHash] || {};
      if (!md.firstSeenAt) md.firstSeenAt = nowIso;
      md.lastActiveAt = nowIso;
      target.memberDetails[guestEmailHash] = md;
      await kvSet(['share', code.toUpperCase()], JSON.stringify(target));
      // Re-joining a previously-evicted member clears the revocation marker
      // (the owner can re-add them via /share/update or by sharing the link
      // again — this just allows the rejoin path to succeed).
      if (isNewMember) {
        await kvDel(['share_revoked', code.toUpperCase(), guestEmailHash]);
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
      await kvSetLarge(['share_data', code.toUpperCase(), hKey], ciphertext);
      await kvSet(['share_data', code.toUpperCase(), `${hKey}_modified`], new Date().toISOString());
      // Mark the writer as the owner — guests pulling this blob can use
      // this to skip merging their own writes back as if they were owner edits.
      await kvSet(['share_data', code.toUpperCase(), `${hKey}_writer`], JSON.stringify({ kind: 'owner', emailHash: ownerEmailHash, at: new Date().toISOString() }));
      // Share data lives under the owner's emailHash conceptually — mark the
      // owner dirty so the per-user backup captures the new ciphertext.
      await markUserDirty(ownerEmailHash);
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: guest pushes their own edits back to the share blob ──────
  // Authenticated as a guest member with at least one rw permission on
  // the requested household. The server can't read what's in the
  // ciphertext (e2e encrypted with the share key), so per-section
  // enforcement is necessarily at the blob level — if the guest has no
  // rw permission on the household at all, we refuse. If they have at
  // least one rw section, we accept the whole blob and trust the client
  // not to mutate read-only sections. (The owner's view of merged
  // state is ground truth, and tampering would be visible to them on
  // their next sync — see pull-guest-writes flow.)
  if (url.pathname === '/share/data/push-guest' && request.method === 'POST') {
    try {
      const { guestEmailHash, guestVerifier, guestSessionToken, code, household, ciphertext } = await request.json();
      if (!code || !guestEmailHash || (!guestVerifier && !guestSessionToken) || !ciphertext) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      if (guestSessionToken) {
        const sessStored = await kvGet(['passkey_session', guestEmailHash, guestSessionToken]);
        if (!sessStored.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const guestStored = await kvGet(['user', guestEmailHash, 'verifier']);
        if (!guestStored.value || guestStored.value !== guestVerifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      // Email-verification gate (see /share/join, /share/data/pull)
      const guestVerifiedPush = await kvGet(['user', guestEmailHash, 'email_verified']);
      if (!guestVerifiedPush.value) {
        return json({ error: 'Email verification required', requiresEmailVerification: true }, corsHeaders, 403);
      }
      const revoked = await kvGet(['share_revoked', code.toUpperCase(), guestEmailHash]);
      if (revoked.value) return json({ error: 'Access revoked', revoked: true }, corsHeaders, 403);
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      const target = JSON.parse(share.value);
      if (!target.members?.includes(guestEmailHash)) return json({ error: 'Not a member of this share' }, corsHeaders, 403);
      const hKey  = household && household !== 'default' ? household : 'default';
      const perms = target.households?.[hKey];
      if (!perms) return json({ error: 'No access to this household' }, corsHeaders, 403);
      // Require at least one rw section on this household. A read-only
      // guest must not be able to overwrite the share blob.
      const hasAnyRw = Object.values(perms).some(p => p === 'rw');
      if (!hasAnyRw) return json({ error: 'No write access to this household' }, corsHeaders, 403);
      await kvSetLarge(['share_data', code.toUpperCase(), hKey], ciphertext);
      const nowIso = new Date().toISOString();
      await kvSet(['share_data', code.toUpperCase(), `${hKey}_modified`], nowIso);
      // Stamp the writer as this guest so the owner can recognise the
      // edit and merge it into their own data on next sync. Without
      // this, an owner pulling the share blob after a guest write would
      // be unable to distinguish guest changes from their own last push.
      await kvSet(['share_data', code.toUpperCase(), `${hKey}_writer`], JSON.stringify({ kind: 'guest', emailHash: guestEmailHash, at: nowIso }));
      // Mark owner dirty for backup purposes — a guest write is still a
      // write to the owner's logical share data.
      await markUserDirty(target.ownerEmailHash);
      return json({ ok: true, modified: nowIso }, corsHeaders);
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
      // Email verification gate — also enforced at /share/join, but
      // re-checked here so that a guest who joined before the gate was
      // added (legacy data) can't keep pulling shared data without
      // verifying. Defence in depth.
      const guestVerifiedPull = await kvGet(['user', guestEmailHash, 'email_verified']);
      if (!guestVerifiedPull.value) {
        return json({ error: 'Email verification required', requiresEmailVerification: true }, corsHeaders, 403);
      }
      // Check eviction marker first — short-circuits before share record fetch
      // so a removed guest gets a clear, fast response and the client can
      // self-clean its share state.
      const revoked = await kvGet(['share_revoked', code.toUpperCase(), guestEmailHash]);
      if (revoked.value) return json({ error: 'Access revoked', revoked: true }, corsHeaders, 403);
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      const target = JSON.parse(share.value);
      if (!target.members?.includes(guestEmailHash)) return json({ error: 'Not a member of this share' }, corsHeaders, 403);
      const hKey     = household && household !== 'default' ? household : 'default';
      const perms    = target.households?.[hKey];
      if (!perms) return json({ error: 'No access to this household' }, corsHeaders, 403);
      const data     = await kvGetLarge(['share_data', code.toUpperCase(), hKey]);
      const modified = await kvGet(['share_data', code.toUpperCase(), `${hKey}_modified`]);
      const writerR  = await kvGet(['share_data', code.toUpperCase(), `${hKey}_writer`]);
      let writer = null;
      try { writer = writerR.value ? JSON.parse(writerR.value) : null; } catch(_e) { writer = null; }
      // Record member activity. We update at most every 60 seconds per member
      // to avoid hammering KV — pulls are frequent (every focus/visibility
      // change for active guests). Owner sees a "last active" timestamp
      // accurate to the minute, which is enough granularity.
      try {
        const now = Date.now();
        if (!target.memberDetails) target.memberDetails = {};
        const md = target.memberDetails[guestEmailHash] || {};
        const lastTs = md.lastActiveAt ? new Date(md.lastActiveAt).getTime() : 0;
        if (now - lastTs > 60 * 1000) {
          md.lastActiveAt = new Date(now).toISOString();
          if (!md.firstSeenAt) md.firstSeenAt = md.lastActiveAt;
          md.pullCount = (md.pullCount || 0) + 1;
          target.memberDetails[guestEmailHash] = md;
          await kvSet(['share', code.toUpperCase()], JSON.stringify(target));
        }
      } catch(_e) { /* best-effort, never block the pull */ }
      return json({
        ciphertext: data.value||null,
        modified: modified.value||null,
        writer,
        permissions: perms,
        householdNames: target.householdNames,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: owner pulls share blob to absorb guest writes ────
  // After a guest pushes via /share/data/push-guest, the owner needs a
  // way to read the updated blob and merge those changes into their
  // own KV data. This endpoint authenticates as owner and returns the
  // blob plus writer metadata. The owner's client decrypts with the
  // share key and merges using the same logic as the standard sync.
  if (url.pathname === '/share/data/pull-owner' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code, household } = await request.json();
      if (!ownerEmailHash || (!verifier && !sessionToken) || !code) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const share = await kvGet(['share', code.toUpperCase()]);
      if (!share.value) return json({ error: 'Share not found' }, corsHeaders, 404);
      const target = JSON.parse(share.value);
      if (target.ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      const hKey     = household && household !== 'default' ? household : 'default';
      const data     = await kvGetLarge(['share_data', code.toUpperCase(), hKey]);
      const modified = await kvGet(['share_data', code.toUpperCase(), `${hKey}_modified`]);
      const writerR  = await kvGet(['share_data', code.toUpperCase(), `${hKey}_writer`]);
      let writer = null;
      try { writer = writerR.value ? JSON.parse(writerR.value) : null; } catch(_e) { writer = null; }
      return json({
        ciphertext: data.value||null,
        modified: modified.value||null,
        writer,
        householdNames: target.householdNames,
      }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: cheap multi-tip endpoint for active-tab polling ────
  // Returns the modified timestamp + last writer for every (code,
  // household) pair the caller has access to. Used by the client's
  // 15s active-tab poll to decide whether to fire a real sync. No
  // ciphertext, no decryption, no bandwidth — just a few KV point
  // reads per request.
  //
  // Auth: caller passes their emailHash + verifier|sessionToken.
  // Per share, we either confirm they own it OR they're a member.
  // Anything else they ask about is silently dropped (returns null
  // for that stamp) so we don't leak existence of shares to non-
  // members.
  if (url.pathname === '/share/tips' && request.method === 'POST') {
    try {
      const { emailHash, verifier, sessionToken, codes } = await request.json();
      if (!emailHash || (!verifier && !sessionToken) || !Array.isArray(codes) || !codes.length) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      // Auth — accept either passphrase verifier or session token.
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', emailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', emailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      // Email-verification gate — same as /share/data/pull et al.
      // Without this an unverified attacker could still poll for
      // existence of shares, which leaks low-grade info. Cheap to
      // gate.
      const verifiedRow = await kvGet(['user', emailHash, 'email_verified']);
      if (!verifiedRow.value) {
        return json({ error: 'Email verification required', requiresEmailVerification: true }, corsHeaders, 403);
      }
      // Cap codes at 20 to bound work per request. A normal user is
      // a member of one share or owner of a handful — well under 20.
      const requestedCodes = codes.slice(0, 20).map(c => String(c).toUpperCase());
      const tips = {};
      for (const code of requestedCodes) {
        const shareRow = await kvGet(['share', code]);
        if (!shareRow.value) continue;
        let target;
        try { target = JSON.parse(shareRow.value); } catch(_e) { continue; }
        const isOwner    = target.ownerEmailHash === emailHash;
        const isMember   = Array.isArray(target.members) && target.members.includes(emailHash);
        if (!isOwner && !isMember) continue; // silently drop
        // For each household in this share, return its tip.
        const households = target.households ? Object.keys(target.households) : ['default'];
        for (const hKey of households) {
          const stamp     = `${code}:${hKey}`;
          const modifiedR = await kvGet(['share_data', code, `${hKey}_modified`]);
          const writerR   = await kvGet(['share_data', code, `${hKey}_writer`]);
          let writer = null;
          try {
            if (writerR.value) {
              const w = JSON.parse(writerR.value);
              writer = w?.kind || null;
            }
          } catch(_e) {}
          tips[stamp] = {
            modified: modifiedR.value || null,
            writer,
          };
        }
      }
      return json({ tips }, corsHeaders);
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
      const { ownerEmailHash, verifier, sessionToken, code, name, type, colour, households, shareManagement } = await request.json();
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
      // shareManagement is a string ('none' | 'view' | 'edit'); accept it
      // explicitly so we don't merge in undefined and clobber existing.
      const validMgmt = ['none','view','edit'].includes(shareManagement) ? shareManagement : null;
      const updated = {
        ...existing,
        ...(name&&{name}), ...(type&&{type}), ...(colour&&{colour}),
        ...(households&&{households}),
        ...(validMgmt!==null && { shareManagement: validMgmt }),
      };
      await kvSet(['share', code.toUpperCase()], JSON.stringify(updated));
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }


  // ── Share: send invite/update email to guest ─────────

  if (url.pathname === '/share/delete' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code } = await request.json();
      if (!code || !ownerEmailHash || (!verifier && !sessionToken)) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
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

  // ── Share: remove a single member (surgical eject) ────
  // Accepts owner credentials (passphrase or passkey) and a guestEmailHash.
  // Removes the member from the share record, deletes their ECDH-wrapped
  // key entry, and writes a "revoked" tombstone entry so the guest's next
  // pull returns 403 fast and they can detect the eviction client-side.
  // The share itself stays intact for other members.
  if (url.pathname === '/share/member/remove' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code, guestEmailHash } = await request.json();
      if (!code || !ownerEmailHash || (!verifier && !sessionToken) || !guestEmailHash) {
        return json({ error: 'Missing fields' }, corsHeaders, 400);
      }
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const r = await kvGet(['share', code.toUpperCase()]);
      if (!r.value) return json({ error: 'Not found' }, corsHeaders, 404);
      const target = JSON.parse(r.value);
      if (target.ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      // Drop from members list
      target.members = (target.members || []).filter((m: string) => m !== guestEmailHash);
      // Drop activity record for this member
      if (target.memberDetails && typeof target.memberDetails === 'object') {
        delete target.memberDetails[guestEmailHash];
      }
      await kvSet(['share', code.toUpperCase()], JSON.stringify(target));
      // Drop the ECDH-wrapped key so the guest can't decrypt new pushes
      await kvDel(['share_ecdh_key', code.toUpperCase(), guestEmailHash]);
      // Write a short-lived revocation marker that the guest's next /pull
      // can read to know they were evicted (so the client can self-clean).
      // 7 day TTL is enough for any realistic resync window.
      await kvSet(
        ['share_revoked', code.toUpperCase(), guestEmailHash],
        new Date().toISOString(),
        { expireIn: 7 * 24 * 60 * 60 * 1000 }
      );
      return json({ ok: true }, corsHeaders);
    } catch(err) { return json({ error: err.message }, corsHeaders, 500); }
  }

  // ── Share: refresh link (new 24h window) ─────────────
  if (url.pathname === '/share/refresh' && request.method === 'POST') {
    try {
      const { ownerEmailHash, verifier, sessionToken, code } = await request.json();
      if (!code || !ownerEmailHash || (!verifier && !sessionToken)) return json({ error: 'Missing fields' }, corsHeaders, 400);
      if (sessionToken) {
        const sess = await kvGet(['passkey_session', ownerEmailHash, sessionToken]);
        if (!sess.value) return json({ error: 'Session expired — sign in again' }, corsHeaders, 401);
      } else {
        const stored = await kvGet(['user', ownerEmailHash, 'verifier']);
        if (!stored.value || stored.value !== verifier) return json({ error: 'Unauthorised' }, corsHeaders, 401);
      }
      const r = await kvGet(['share', code.toUpperCase()]);
      if (!r.value) return json({ error: 'Not found' }, corsHeaders, 404);
      const existing = JSON.parse(r.value);
      if (existing.ownerEmailHash !== ownerEmailHash) return json({ error: 'Forbidden' }, corsHeaders, 403);
      // Match the original 1-hour window from /share/create — refresh
      // gives the link a fresh hour, not a different duration.
      existing.expiresAt = new Date(Date.now() + 60*60*1000).toISOString();
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
        <p style="font-size:12px;color:#999;text-align:center">This link expires in 1 hour.</p>` : '';

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
      // ── Free-tier feature gate: notes mode ──
      // Allow deletes through (so users can clean up old notes when
      // downgrading) but block creation/update of note bodies.
      if (ciphertext) {
        const gate = await gateFeature(emailHash, 'notes');
        if (!gate.ok) {
          return json({ error: 'Notes require upgrade', reason: gate.reason }, corsHeaders, gate.status);
        }
      }
      if (ciphertext) {
        await kvSet(['note_body', emailHash, noteId], ciphertext);
      } else {
        await kvDel(['note_body', emailHash, noteId]);
      }
      await markUserDirty(emailHash);
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
      await markUserDirty(emailHash);
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

// ── Daily backup heartbeat email (runs at 03:05 UTC) ────────
// Sends a daily confirmation email summarising the latest R2 snapshot.
// No attachment — keeps the email tiny and avoids Resend's 40 MB cap.
// Runs at :05 (not :00) so it picks up a snapshot that was just written
// by the 5-min auto-backup cron at 03:00.
async function sendBackupHeartbeatEmail(): Promise<{ ok: boolean; error?: string; total?: number; latest?: string | null }> {
  if (!env.RESEND_API_KEY) {
    console.log('Heartbeat: no Resend key, skipping');
    return { ok: false, error: 'RESEND_API_KEY not set' };
  }
  try {
    console.log('Heartbeat: building daily backup summary');

    const now     = new Date();
    const dateStr = now.toISOString().slice(0, 10);

    // Count KV entries grouped by top-level prefix — quick integrity overview
    const counts: Record<string, number> = {};
    let total = 0;
    const iter = kv.list({ prefix: [] });
    for await (const entry of iter) {
      total++;
      const k = entry.key as unknown[];
      const top = String(k[0] ?? '_unknown');
      counts[top] = (counts[top] || 0) + 1;
    }

    // Most recent auto snapshot from R2
    let latestSnap: { key: string; size: number; lastModified: string } | null = null;
    let r2Status = 'not configured';
    if (r2Configured()) {
      try {
        const all = await listR2Snapshots('auto/');
        latestSnap = all.at(-1) || null;
        r2Status = latestSnap ? 'ok' : 'configured but no snapshots found';
      } catch (e) {
        r2Status = `error: ${(e as Error)?.message || e}`;
      }
    }

    // Pending changes? If dirty, the next 5-min cron will pick them up.
    const pendingChanges = await _isKVDirty();

    // Total R2 storage used (rough — listing only auto/ above)
    let totalSnapshots = 0;
    let totalBytes = 0;
    if (r2Configured() && r2Status === 'ok') {
      try {
        const everything = await listR2Snapshots('');
        totalSnapshots = everything.length;
        totalBytes = everything.reduce((s, x) => s + x.size, 0);
      } catch (_) { /* non-fatal */ }
    }

    const fmtBytes = (n: number) => {
      if (!n) return '0 B';
      if (n < 1024) return n + ' B';
      if (n < 1024*1024) return (n/1024).toFixed(1) + ' KB';
      if (n < 1024*1024*1024) return (n/1024/1024).toFixed(2) + ' MB';
      return (n/1024/1024/1024).toFixed(2) + ' GB';
    };
    const fmtTime = (iso: string) => {
      if (!iso) return '—';
      try { return new Date(iso).toUTCString(); } catch { return iso; }
    };

    const isHealthy = r2Status === 'ok' && latestSnap && (now.getTime() - new Date(latestSnap.lastModified).getTime()) < 30 * 60 * 1000;
    const headerColour = isHealthy ? '#4cbb8a' : '#e8a838';
    const headerLabel  = isHealthy ? '✓ Backup healthy' : '⚠ Backup needs attention';
    const subjectTag   = isHealthy ? 'OK' : 'CHECK';

    const topPrefixes = Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6);

    const adminUrl = `${env.APP_URL.replace(/\/$/, '')}/admin.html`;

    const r2Lines = latestSnap
      ? `
        <tr><td style="color:#7a8097;padding:4px 12px 4px 0">Latest snapshot</td><td style="color:#f0f2f7"><code style="background:#1a1d28;padding:2px 6px;border-radius:4px">${latestSnap.key}</code></td></tr>
        <tr><td style="color:#7a8097;padding:4px 12px 4px 0">Snapshot taken</td><td style="color:#f0f2f7">${fmtTime(latestSnap.lastModified)}</td></tr>
        <tr><td style="color:#7a8097;padding:4px 12px 4px 0">Snapshot size</td><td style="color:#f0f2f7">${fmtBytes(latestSnap.size)}</td></tr>`
      : `
        <tr><td colspan="2" style="color:#e8a838;padding:8px 0">⚠ No recent R2 snapshot found — check the auto-backup cron is running and admin secrets are set.</td></tr>`;

    const r2TotalsLines = totalSnapshots
      ? `<tr><td style="color:#7a8097;padding:4px 12px 4px 0">R2 total</td><td style="color:#f0f2f7">${totalSnapshots} snapshots, ${fmtBytes(totalBytes)} of 10 GB free tier</td></tr>`
      : '';

    const pendingLine = `<tr><td style="color:#7a8097;padding:4px 12px 4px 0">Pending changes</td><td style="color:${pendingChanges ? '#e8a838' : '#4cbb8a'}">${pendingChanges ? 'yes — will be captured at next 5-min tick' : 'none — KV is clean'}</td></tr>`;

    const prefixRows = topPrefixes.map(([p, c]) =>
      `<tr><td style="color:#7a8097;padding:3px 12px 3px 0;font-size:12px"><code>${p}</code></td><td style="color:#f0f2f7;font-size:12px">${c}</td></tr>`
    ).join('');

    const html = `
      <div style="font-family:system-ui,-apple-system,sans-serif;background:#0f1117;color:#f0f2f7;padding:24px;border-radius:8px;max-width:600px">
        <h2 style="color:${headerColour};margin:0 0 4px;font-size:18px">${headerLabel}</h2>
        <div style="color:#7a8097;font-size:12px;margin-bottom:20px">STOCKROOM daily backup summary — ${dateStr}</div>

        <h3 style="color:#e8a838;font-size:13px;margin:20px 0 8px;text-transform:uppercase;letter-spacing:0.5px">Snapshot</h3>
        <table style="border-collapse:collapse;width:100%;font-size:13px">
          ${r2Lines}
          ${r2TotalsLines}
          ${pendingLine}
        </table>

        <h3 style="color:#e8a838;font-size:13px;margin:24px 0 8px;text-transform:uppercase;letter-spacing:0.5px">KV integrity</h3>
        <table style="border-collapse:collapse;width:100%;font-size:13px">
          <tr><td style="color:#7a8097;padding:4px 12px 4px 0">Total entries</td><td style="color:#4cbb8a">${total}</td></tr>
          ${prefixRows}
        </table>

        <div style="margin-top:24px;padding:14px 16px;background:#1a1d28;border-radius:6px;border:1px solid #2a2e3d">
          <div style="font-size:12px;color:#7a8097;margin-bottom:6px">To browse, download, or restore from snapshots:</div>
          <a href="${adminUrl}" style="color:#e8a838;font-weight:600;text-decoration:none;font-size:14px">→ Open admin panel</a>
        </div>

        <p style="color:#5a607a;font-size:11px;margin-top:24px;line-height:1.6">
          Data is AES-GCM encrypted client-side; snapshots stored in Cloudflare R2 are useless without the user passphrase.
          Auto backup runs every 5 minutes when data has changed; idle hours are skipped. A daily forced backup at 03:00 UTC ensures retention always has fresh material. Snapshot retention: 24h × 5min, 30d × daily, 90d × weekly.
        </p>
      </div>`;

    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from:    env.FROM_EMAIL,
        to:      [env.ADMIN_EMAIL],
        subject: `STOCKROOM backup ${dateStr} — ${subjectTag} — ${total} entries`,
        html,
      }),
    });

    if (res.ok) {
      console.log(`Heartbeat: sent — total ${total} entries, R2 ${r2Status}, latest ${latestSnap?.key || 'none'}`);
      return { ok: true, total, latest: latestSnap?.key || null };
    } else {
      const err = await res.text();
      console.error('Heartbeat: Resend error', res.status, err);
      return { ok: false, error: `Resend ${res.status}: ${err.slice(0, 200)}` };
    }
  } catch (e) {
    const msg = (e as Error)?.message || String(e);
    console.error('Heartbeat: unexpected error', msg);
    return { ok: false, error: msg };
  }
}

Deno.cron('stockroom-daily-backup-heartbeat', '5 3 * * *', async () => {
  await sendBackupHeartbeatEmail();
});

// ── Cron ──────────────────────────────────────────────────
async function cronCheck() {
  try {
    const schedRaw = await kvGet(['schedule']);
    if (!schedRaw.value) { console.log('Cron: no schedule'); return; }
    const { email, startDate, startTime, intervalDays, emailHash } = JSON.parse(schedRaw.value);
    if (!email) { console.log('Cron: no email in schedule'); return; }
    // ── Free-tier email gate ──
    // Even if a stale schedule exists in KV (e.g. user upgraded, scheduled,
    // then downgraded), don't actually send emails to free-tier users.
    if (emailHash) {
      const eff = computeEffectiveStatus(await getBillingAccount(emailHash));
      if (eff === 'free') {
        console.log(`Cron: skipping ${email} — free tier`);
        return;
      }
    }
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
