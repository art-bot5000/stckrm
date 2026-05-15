# STOCKROOM_ENV is passed in from fly.toml ([env] block). It's 'production'
# by default, 'staging' for the staging app. Used below to inject noindex
# meta tags into the staging build so search engines don't crawl it.
ARG STOCKROOM_ENV=production

FROM node:22-slim AS builder
ARG STOCKROOM_ENV=production
WORKDIR /build
COPY package.json ./
RUN npm install
# Frontend source files live at the repo root, not in src/. The previous
# Dockerfile copied from src/ which silently became stale, so root-level
# edits never reached production.
COPY app.js scanner.js styles.css index.html landing.html sw.js manifest.json admin.html favicon.ico icon-16.png icon-32.png icon-48.png icon-96.png icon-144.png icon-180.png icon-192.png icon-512.png icon-512-maskable.png icon-1024.png ./
RUN mkdir -p public && \
    npx terser app.js --compress --mangle --comments false -o public/app.js && \
    npx terser scanner.js --compress --mangle --comments false -o public/scanner.js && \
    npx cleancss -o public/styles.css styles.css && \
    if [ "$STOCKROOM_ENV" = "staging" ]; then \
      echo "Staging build — injecting noindex meta tags into index.html and landing.html"; \
      sed -i 's|<head>|<head><meta name="robots" content="noindex,nofollow"><meta name="x-stockroom-env" content="staging">|' index.html landing.html ; \
    fi && \
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
    cp favicon.ico public/favicon.ico && \
    cp icon-16.png icon-32.png icon-48.png icon-96.png icon-144.png icon-180.png icon-192.png icon-512.png icon-512-maskable.png icon-1024.png public/ && \
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
