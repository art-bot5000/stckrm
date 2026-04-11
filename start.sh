#!/bin/sh
# ═══════════════════════════════════════════════════════════
#  STOCKROOM — Container start script
#  Launches Caddy and Deno in parallel.
#  If either process exits, the container exits (Fly.io
#  will restart it automatically).
# ═══════════════════════════════════════════════════════════

set -e

# ── Start Deno API backend on port 8000 ───────────────────
deno run \
  --unstable-kv \
  --unstable-cron \
  --allow-net \
  --allow-env \
  --allow-read=/app \
  /app/main.ts &

DENO_PID=$!

# ── Wait for Deno to be ready ─────────────────────────────
echo "Waiting for Deno backend to start..."
for i in $(seq 1 20); do
  if curl -sf http://localhost:8000/ping > /dev/null 2>&1; then
    echo "Deno backend ready."
    break
  fi
  sleep 0.5
done

# ── Start Caddy (foreground) ──────────────────────────────
caddy run --config /app/Caddyfile --adapter caddyfile &

CADDY_PID=$!

# ── Exit if either process dies ───────────────────────────
wait -n $DENO_PID $CADDY_PID
EXIT_CODE=$?
echo "A process exited with code $EXIT_CODE — shutting down container"
kill $DENO_PID $CADDY_PID 2>/dev/null || true
exit $EXIT_CODE
