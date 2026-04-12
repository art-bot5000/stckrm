#!/bin/sh
set -e

deno run \
  --unstable-kv \
  --unstable-cron \
  --allow-net \
  --allow-env \
  --allow-read=/app,/data \
  --allow-write=/data \
  /app/main.ts &

DENO_PID=$!

i=0
while [ $i -lt 20 ]; do
  if curl -sf http://localhost:8000/ping > /dev/null 2>&1; then
    echo "Deno ready."
    break
  fi
  sleep 0.5
  i=$((i+1))
done

caddy run --config /app/Caddyfile --adapter caddyfile &
CADDY_PID=$!

wait $DENO_PID
