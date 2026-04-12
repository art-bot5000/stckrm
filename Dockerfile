# ═══════════════════════════════════════════════════════════
#  STOCKROOM — Fly.io Dockerfile
#  Multi-stage build:
#    Stage 1 (builder): Node.js minifies src/ → public/
#    Stage 2 (runner):  Deno + Caddy serve the app1
# ═══════════════════════════════════════════════════════════

# ── Stage 1: Build (minify CSS, JS, HTML) ────────────────
FROM node:22-slim AS builder

WORKDIR /build

# Copy package.json and install build tools
COPY package.json ./
RUN npm install

# Copy source files
COPY src/ ./src/

# Run the build (minifies everything into public/)
RUN mkdir -p public && \
    npx terser src/app.js --compress --mangle --comments false -o public/app.js && \
    npx terser src/scanner.js --compress --mangle --comments false -o public/scanner.js && \
    npx cleancss -o public/styles.css src/styles.css && \
    npx html-minifier-terser src/index.html \
      --collapse-whitespace \
      --remove-comments \
      --remove-optional-tags \
      --remove-redundant-attributes \
      --remove-script-type-attributes \
      --remove-tag-whitespace \
      --minify-css true \
      --minify-js true \
      -o public/index.html && \
    cp src/sw.js public/sw.js && \
    cp src/manifest.json public/manifest.json && \
    cp src/admin.html public/admin.html

# ── Stage 2: Runtime (Deno + Caddy) ──────────────────────
FROM denoland/deno:2.3.1

# Install Caddy
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      debian-keyring \
      debian-archive-keyring \
      apt-transport-https \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
       | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
       | tee /etc/apt/sources.list.d/caddy-stable.list \
    && apt-get update && apt-get install -y --no-install-recommends caddy \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy minified static files from builder stage
COPY --from=builder /build/public/ ./public/

# Copy Deno backend
COPY main.ts   ./main.ts
COPY deno.json ./deno.json

# Pre-cache Deno dependencies
RUN deno cache --unstable-kv --unstable-cron main.ts

# Copy Caddy config and start script
COPY Caddyfile ./Caddyfile
COPY start.sh  ./start.sh
RUN chmod +x ./start.sh

EXPOSE 8080

CMD ["./start.sh"]
