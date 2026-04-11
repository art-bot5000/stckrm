# ═══════════════════════════════════════════════════════════
#  STOCKROOM — Fly.io Dockerfile
#  Two-process container:
#    - Caddy  (port 80/443): serves static files with Brotli + caching
#    - Deno   (port 8000):   API backend using Deno KV
#  Supervisor (s6-overlay) keeps both processes running.
# ═══════════════════════════════════════════════════════════

FROM denoland/deno:2.3.1 AS base

# ── Install Caddy + s6-overlay (process supervisor) ──────
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      debian-keyring \
      debian-archive-keyring \
      apt-transport-https \
      xz-utils \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
       | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
       | tee /etc/apt/sources.list.d/caddy-stable.list \
    && apt-get update && apt-get install -y --no-install-recommends caddy \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── App directory ─────────────────────────────────────────
WORKDIR /app

# ── Copy static front-end files ───────────────────────────
COPY index.html     ./public/index.html
COPY app.js         ./public/app.js
COPY scanner.js     ./public/scanner.js
COPY styles.css     ./public/styles.css
COPY manifest.json  ./public/manifest.json
COPY sw.js          ./public/sw.js
COPY admin.html     ./public/admin.html

# ── Copy Deno backend ─────────────────────────────────────
COPY main.ts        ./main.ts
COPY deno.json      ./deno.json

# ── Pre-cache Deno dependencies ───────────────────────────
RUN deno cache --unstable-kv --unstable-cron main.ts

# ── Copy Caddy config ─────────────────────────────────────
COPY Caddyfile ./Caddyfile

# ── Copy start script ─────────────────────────────────────
COPY start.sh ./start.sh
RUN chmod +x ./start.sh

# ── Fly.io exposes port 8080 by default ───────────────────
# Caddy listens on 8080 (HTTP only — Fly handles TLS termination)
EXPOSE 8080

CMD ["./start.sh"]
