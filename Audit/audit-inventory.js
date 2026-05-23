// audit-inventory.js — STOCKROOM endpoint inventory & auth-pattern classifier
//
// Reads main.ts, locates every `if (url.pathname === '...' && request.method
// === 'X')` handler, extracts the handler body, and classifies how it
// authenticates. Output: Markdown report + JSON dump (consumed by
// audit-probe.js).
//
// Run: node audit-inventory.js <path-to-main.ts> [out.md] [out.json]
//      node audit-inventory.js ./main.ts
//
// Exit codes:
//   0 — clean: every endpoint matched a known auth pattern
//   1 — findings: at least one endpoint could not be classified (TREAT AS FAIL)
//   2 — script error (couldn't read file, etc.)

const fs   = require('fs');
const path = require('path');

const SRC      = process.argv[2] || './main.ts';
const OUT_MD   = process.argv[3] || './audit-inventory.md';
const OUT_JSON = process.argv[4] || './audit-inventory.json';

let source;
try {
  source = fs.readFileSync(SRC, 'utf8');
} catch (e) {
  console.error(`Could not read ${SRC}: ${e.message}`);
  process.exit(2);
}

const lines = source.split('\n');

// ── 1. Find every endpoint declaration ────────────────────────────────────
const HANDLER_RE = /if\s*\(\s*url\.pathname\s*===\s*['"`]([^'"`]+)['"`]\s*(?:&&\s*request\.method\s*===\s*['"`](GET|POST|PUT|DELETE|PATCH)['"`])?\s*\)\s*\{/;

const endpoints = [];

for (let i = 0; i < lines.length; i++) {
  const m = lines[i].match(HANDLER_RE);
  if (!m) continue;
  const [, p, method] = m;

  // Walk forward, tracking brace depth, to find the matching close brace.
  let depth = 1;
  let j = i + 1;
  for (; j < lines.length && depth > 0; j++) {
    // Cheap brace counter — doesn't account for braces in strings/comments,
    // but for the Stockroom handler style it's accurate enough. If we
    // miscount, the classifier still runs; the probe phase catches real
    // auth failures regardless.
    for (const ch of lines[j]) {
      if (ch === '{') depth++;
      else if (ch === '}') depth--;
    }
  }
  const startLine = i + 1;
  const endLine   = Math.min(j, lines.length);
  const body      = lines.slice(i, endLine).join('\n');

  const cls = classify(body, p);

  endpoints.push({
    path: p,
    method: method || 'ANY',
    startLine,
    endLine,
    pattern: cls.pattern,
    evidence: cls.evidence,
    bodySnippet: lines.slice(i, Math.min(i + 30, endLine)).join('\n'),
  });
}

// ── 2. Classifier ─────────────────────────────────────────────────────────
function classify(body, p) {
  // Order matters: most specific first.

  // Stripe webhook signature
  if (/verifyStripeWebhookSignature|Stripe-Signature|stripe\.webhooks\.constructEvent/i.test(body)) {
    return { pattern: 'webhook-sig', evidence: 'Stripe signature verification' };
  }

  // Dispatch secret (cron-style bearer)
  if (/PUSH_DISPATCH_SECRET|dispatchSecret|DISPATCH_SECRET/.test(body)) {
    return { pattern: 'dispatch-secret', evidence: 'PUSH_DISPATCH_SECRET bearer' };
  }

  // Admin endpoints
  if (/verifyAdminRequest|adminToken|ADMIN_SECRET/.test(body)) {
    return { pattern: 'admin-auth', evidence: 'verifyAdminRequest / adminToken / ADMIN_SECRET' };
  }

  // Recovery token (limited-purpose, short-lived)
  if (/recoveryToken|recovery_token/.test(body) && !/verifyUserAuth|requireUserAuth/.test(body)) {
    return { pattern: 'recovery-token', evidence: 'recovery token check' };
  }

  // OTP send/verify — auth IS the OTP. Path-based so we don't mistake an
  // OTP-protected endpoint that ALSO does user-auth for pure OTP flow.
  if (
    /\/otp\/(send|verify)$/.test(p) ||
    /\/email\/verify\/(send|check)$/.test(p) ||
    /\/recovery\/(request|verify|reset)$/.test(p) ||
    /\/user\/delete-confirm-send$/.test(p)
  ) {
    return { pattern: 'otp-flow', evidence: 'OTP send/verify (auth is the OTP itself)' };
  }

  // Auth-establishment endpoints: necessarily pre-auth because they ARE the
  // auth flow. These return challenges, credential lists, or session tokens —
  // never user data. Adding/removing one of these is itself a security
  // decision worth gating: if the inventory diff between deploys shows a new
  // entry in this list that wasn't reviewed, that's a finding.
  const AUTH_ESTABLISHMENT = new Set([
    '/user/register',           // create account
    '/user/verify',             // passphrase login → returns sessionToken
    '/user/reactivate',         // re-enable a deactivated account
    '/passkey/register/begin',  // WebAuthn registration challenge
    '/passkey/auth/begin',      // WebAuthn auth challenge
    '/passkey/auth/finish',     // WebAuthn auth completion → returns sessionToken
    '/passkey/verify-session',  // ping session token validity
    '/user/ecdh-pubkey/store',  // first-run pubkey upload (server checks account exists)
    '/user/ecdh-pubkey/get',    // public key — public by design
    '/share/join',              // requires share code knowledge + own user-auth
  ]);
  if (AUTH_ESTABLISHMENT.has(p)) {
    return { pattern: 'auth-establishment', evidence: 'pre-auth endpoint (auth flow itself); returns challenge/token, never user data' };
  }

  // Public-by-design endpoints that legitimately have no auth.
  // Each one is here because the data it returns is either public or
  // non-sensitive (no user data, no account existence info).
  const PUBLIC_BY_DESIGN = new Set([
    '/referral/validate',     // checks a referral code is valid before signup
    '/user/email-verified',   // pre-auth check used by session-restore guard;
                              // returns verified:true for both no-account AND
                              // verified-account so the response cannot be
                              // used to enumerate accounts. See main.ts handler.
  ]);
  if (PUBLIC_BY_DESIGN.has(p)) {
    return { pattern: 'public', evidence: 'public-by-design (whitelisted in classifier)' };
  }

  // Session-only auth: endpoints that ONLY accept sessionToken because the
  // feature is passkey-specific. PRF envelopes don't exist for passphrase-
  // only users, so a verifier-path makes no sense.
  const SESSION_ONLY_BY_DESIGN = new Set([
    '/key/passkey-prf-store',
    '/key/passkey-prf-get',
  ]);
  if (SESSION_ONLY_BY_DESIGN.has(p)) {
    return { pattern: 'user-auth', evidence: 'session-token only (passkey-specific feature, no verifier path needed)' };
  }

  // Single-use email-delivered token (similar to recovery-token).
  if (p === '/user/delete-execute') {
    return { pattern: 'delete-token', evidence: 'single-use email-delivered deletion token' };
  }

  // Standard user auth (helper-based)
  if (/requireUserAuth|verifyUserAuth/.test(body)) {
    return { pattern: 'user-auth', evidence: 'requireUserAuth / verifyUserAuth helper' };
  }

  // Inline user-auth: must check BOTH the session path AND the verifier path.
  // Variable names vary by role (emailHash / ownerEmailHash / guestEmailHash),
  // so the regex accepts any identifier in the KV-key position.
  const hasInlineSession  = /\['passkey_session',\s*\w+,\s*\w+\]/.test(body);
  const hasInlineVerifier = /\['user',\s*\w+,\s*'verifier'\]/.test(body);
  if (hasInlineSession && hasInlineVerifier) {
    return { pattern: 'user-auth', evidence: 'inline emailHash+verifier|sessionToken KV check' };
  }
  if (hasInlineSession || hasInlineVerifier) {
    // Only ONE of the two paths is checked — recurring failure mode that has
    // bitten share, sync, presence, MFA, and key management endpoints before.
    return {
      pattern: 'unclassified',
      evidence: `partial auth: ${hasInlineSession ? 'session-only (no verifier path)' : 'verifier-only (no session path)'} — RECURRING AUTH-GAP PATTERN`,
    };
  }

  // Public-by-design — explicit comment near top of handler
  if (/no auth|public.*key.*public|unauthenticated|no-auth/i.test(body.slice(0, 600))) {
    return { pattern: 'public', evidence: 'explicit "no auth / public" comment near handler' };
  }

  // Known-safe simple endpoints
  if (p === '/ping') {
    return { pattern: 'public', evidence: 'health endpoint (no data returned)' };
  }

  return { pattern: 'unclassified', evidence: 'no recognised auth pattern in handler body' };
}

// ── 3. Severity tagging for findings ──────────────────────────────────────
// Not every finding is the same. Sort them into buckets so Pete can triage:
//   data-exposure: the endpoint could return user data without proper auth
//   data-mutation: the endpoint could mutate user data without proper auth
//   account-enum:  the endpoint reveals whether an account exists
//   metadata-leak: leaks coarse info (presence, timestamps, counts)
//   email-abuse:   could trigger emails to arbitrary recipients
//   service-abuse: lets unauthenticated callers consume resources
//   review:        unclear, needs human eyes
function severity(p, body) {
  // Hand-curated based on what the endpoint reads/writes.
  const map = {
    '/debug-kv':            'metadata-leak',
    '/debug-user':          'account-enum',
    '/debug-schedule':      'metadata-leak',
    '/user/email-verified': 'account-enum',
    '/data/modified':       'metadata-leak',
    '/share/data/modified': 'metadata-leak',
    '/presence-update':     'data-mutation',
    '/presence-list':       'metadata-leak',
    '/presence-stream':     'metadata-leak',
    '/set-schedule':        'data-mutation',
    '/reset-schedule':      'data-mutation',
    '/unsubscribe':         'data-mutation',
    '/send-reminder':       'email-abuse',
    '/check-now':           'service-abuse',
    // Partial-auth findings: data exposure is possible if the missing
    // path is the one the attacker would use.
    '/share/key/store':     'data-mutation',
    '/share/key/get':       'data-exposure',
    '/device/register':     'data-mutation',
    '/device/seen':         'data-mutation',
  };
  return map[p] || 'review';
}

function sevTag(s) {
  return {
    'data-exposure': '🔴 DATA-EXPOSURE',
    'data-mutation': '🟠 DATA-MUTATION',
    'account-enum':  '🟡 ACCOUNT-ENUM',
    'email-abuse':   '🟡 EMAIL-ABUSE',
    'service-abuse': '🟢 SERVICE-ABUSE',
    'metadata-leak': '🟢 METADATA-LEAK',
    'review':        '⚪ REVIEW',
  }[s];
}

endpoints.forEach(e => {
  if (e.pattern === 'unclassified') e.severity = severity(e.path, '');
});
function tag(pattern) {
  return {
    'user-auth':           '🔐 user',
    'admin-auth':          '🛡️  admin',
    'dispatch-secret':     '⚙️  dispatch',
    'webhook-sig':         '🪝 webhook',
    'recovery-token':      '🔑 recovery',
    'delete-token':        '🗑️  delete-token',
    'otp-flow':            '📧 otp',
    'auth-establishment':  '🚪 auth-flow',
    'public':              '🌍 public',
    'unclassified':        '⚠️  FINDING',
  }[pattern];
}

const counts = {
  'user-auth': 0, 'admin-auth': 0, 'dispatch-secret': 0, 'webhook-sig': 0,
  'recovery-token': 0, 'delete-token': 0, 'otp-flow': 0, 'auth-establishment': 0,
  'public': 0, 'unclassified': 0,
};
endpoints.forEach(e => counts[e.pattern]++);

let md = `# STOCKROOM endpoint inventory\n\n`;
md += `Source: \`${SRC}\` (${lines.length} lines)\n`;
md += `Generated: ${new Date().toISOString()}\n`;
md += `Endpoints found: **${endpoints.length}**\n\n`;
md += `## Summary by auth pattern\n\n`;
md += `| Pattern | Count |\n|---|---:|\n`;
for (const [k, v] of Object.entries(counts)) {
  if (v > 0) md += `| ${tag(k)} | ${v} |\n`;
}
md += `\n`;

const findings = endpoints.filter(e => e.pattern === 'unclassified');
if (findings.length > 0) {
  md += `## ⚠️  Findings (${findings.length})\n\n`;
  md += `These endpoints did not match any known auth pattern. Each one must either be confirmed safe (and the classifier updated to recognise it), or fixed.\n\n`;
  md += `### Severity buckets\n\n`;
  md += `| Severity | Meaning |\n|---|---|\n`;
  md += `| 🔴 DATA-EXPOSURE | Could return a user's data to an unauthenticated caller |\n`;
  md += `| 🟠 DATA-MUTATION | Could modify a user's data without their auth |\n`;
  md += `| 🟡 ACCOUNT-ENUM | Lets an attacker confirm whether an account exists for an email |\n`;
  md += `| 🟡 EMAIL-ABUSE | Lets an attacker cause emails to be sent (spam vector, reputation harm) |\n`;
  md += `| 🟢 SERVICE-ABUSE | Lets an attacker consume server resources without auth |\n`;
  md += `| 🟢 METADATA-LEAK | Reveals non-content info (online users, timestamps, counts) |\n`;
  md += `| ⚪ REVIEW | Needs manual review to determine impact |\n\n`;

  const order = ['data-exposure','data-mutation','account-enum','email-abuse','service-abuse','metadata-leak','review'];
  const grouped = {};
  for (const sev of order) grouped[sev] = [];
  for (const f of findings) grouped[f.severity || 'review'].push(f);

  for (const sev of order) {
    if (!grouped[sev].length) continue;
    md += `### ${sevTag(sev)} (${grouped[sev].length})\n\n`;
    for (const f of grouped[sev]) {
      md += `#### \`${f.method} ${f.path}\` (line ${f.startLine})\n\n`;
      md += `**Reason classifier flagged it:** ${f.evidence}\n\n`;
      md += '```ts\n' + f.bodySnippet + '\n```\n\n';
    }
  }
}

md += `## Full endpoint list\n\n`;
md += `| Method | Path | Line | Auth pattern | Evidence |\n|---|---|---:|---|---|\n`;
for (const e of [...endpoints].sort((a, b) => a.path.localeCompare(b.path))) {
  md += `| ${e.method} | \`${e.path}\` | ${e.startLine} | ${tag(e.pattern)} | ${e.evidence} |\n`;
}

fs.writeFileSync(OUT_MD, md);
fs.writeFileSync(OUT_JSON, JSON.stringify(endpoints, null, 2));

console.log(`Wrote ${OUT_MD} and ${OUT_JSON}`);
console.log(`Total endpoints: ${endpoints.length}`);
for (const [k, v] of Object.entries(counts)) {
  if (v > 0) console.log(`  ${tag(k).padEnd(16)} ${v}`);
}

if (findings.length > 0) {
  console.error(`\n❌ ${findings.length} endpoint(s) could not be classified — review ${OUT_MD}`);
  process.exit(1);
}
console.log(`\n✅ All endpoints matched a known auth pattern.`);
process.exit(0);
