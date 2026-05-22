// audit.js — STOCKROOM security audit runner
//
// Runs all three phases in sequence:
//   1. Static endpoint inventory & classification (against ./main.ts)
//   2. Runtime probe (against staging URL)
//   3. Crypto invariant check (against staging URL)
//
// Exit code is the worst of the three.
//
// Usage:
//   node audit.js                           # inventory only (no network)
//   node audit.js https://stckrm-staging.fly.dev   # all three phases

const { spawnSync } = require('child_process');
const path          = require('path');

const HERE = __dirname;
const BASE = process.argv[2];

function run(script, args) {
  console.log(`\n${'='.repeat(72)}\n  ${script}\n${'='.repeat(72)}`);
  const r = spawnSync('node', [path.join(HERE, script), ...args], {
    stdio: 'inherit',
    env: process.env,
  });
  return r.status ?? 2;
}

const codes = [];

// Phase 1: inventory
codes.push(run('audit-inventory.js', [
  process.env.STOCKROOM_MAIN_TS || './main.ts',
  path.join(HERE, 'audit-inventory.md'),
  path.join(HERE, 'audit-inventory.json'),
]));

if (BASE) {
  // Phase 2: probe
  codes.push(run('audit-probe.js', [BASE, path.join(HERE, 'audit-inventory.json'), path.join(HERE, 'audit-probe.md')]));

  // Phase 3: crypto
  codes.push(run('audit-crypto.js', [BASE, path.join(HERE, 'audit-crypto.md')]));
} else {
  console.log(`\n(No base URL given — skipping runtime probe and crypto invariants.)`);
  console.log(`To run all three phases: node audit.js https://stckrm-staging.fly.dev`);
}

const worst = Math.max(...codes);
console.log(`\n${'='.repeat(72)}`);
console.log(`  Summary: phase exit codes = [${codes.join(', ')}], worst = ${worst}`);
console.log(`${'='.repeat(72)}\n`);
process.exit(worst);
