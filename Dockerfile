FROM node:22-slim AS builder
WORKDIR /build
COPY package.json ./
RUN npm install
# Frontend source files live at the repo root, not in src/. The previous
# Dockerfile copied from src/ which silently became stale, so root-level
# edits never reached production.
COPY app.js scanner.js styles.css index.html landing.html sw.js manifest.json admin.html ./
RUN mkdir -p public && \
    npx terser app.js --compress --mangle --comments false -o public/app.js && \
    npx terser scanner.js --compress --mangle --comments false -o public/scanner.js && \
    npx cleancss -o public/styles.css styles.css && \
    npx html-minifier-terser index.html \
      --collapse-whitespace --remove-comments \
      --remove-redundant-attributes --remove-script-type-attributes \
      --minify-css true \
      -o public/index.html && \
    npx html-minifier-terser landing.html \
      --collapse-whitespace --remove-comments \
      --remove-redundant-attributes --remove-script-type-attributes \
      --minify-css true --minify-js true \
      -o public/landing.html && \
    cp sw.js public/sw.js && \
    cp manifest.json public/manifest.json && \
    cp admin.html public/admin.html

FROM denoland/deno:2.3.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "https://caddyserver.com/api/download?os=linux&arch=amd64&p=github.com/ueffel/caddy-brotli" \
      -o /usr/local/bin/caddy && \
    chmod +x /usr/local/bin/caddy
WORKDIR /app
COPY --from=builder /build/public/ ./public/
COPY main.ts ./main.ts
COPY deno.json ./deno.json
RUN deno cache --unstable-kv --unstable-cron main.ts
COPY start.sh /app/start.sh
COPY Caddyfile /app/Caddyfile
RUN chmod +x /app/start.sh
EXPOSE 8080
CMD ["/app/start.sh"]
