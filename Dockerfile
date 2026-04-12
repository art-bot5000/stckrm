FROM node:22-slim AS builder
WORKDIR /build
COPY package.json ./
RUN npm install
COPY src/ ./src/
RUN mkdir -p public && \
    npx terser src/app.js --compress --mangle --comments false -o public/app.js && \
    npx terser src/scanner.js --compress --mangle --comments false -o public/scanner.js && \
    npx cleancss -o public/styles.css src/styles.css && \
    npx html-minifier-terser src/index.html \
      --collapse-whitespace --remove-comments --remove-optional-tags \
      --remove-redundant-attributes --remove-script-type-attributes \
      --remove-tag-whitespace --minify-css true --minify-js true \
      -o public/index.html && \
    cp src/sw.js public/sw.js && \
    cp src/manifest.json public/manifest.json && \
    cp src/admin.html public/admin.html

FROM denoland/deno:2.3.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "https://caddyserver.com/api/download?os=linux&arch=amd64&p=github.com/ueffel/caddy-brotli&p=github.com/dunglas/caddy-cbrotli" \
      -o /usr/local/bin/caddy && \
    chmod +x /usr/local/bin/caddy

WORKDIR /app
COPY --from=builder /build/public/ ./public/
COPY main.ts ./main.ts
COPY deno.json ./deno.json
RUN deno cache --unstable-kv --unstable-cron main.ts

# Write Caddyfile inline to avoid Windows encoding issues
RUN printf ':8080 {\n\
    encode {\n\
        zstd\n\
        br\n\
        gzip\n\
        minimum_length 256\n\
    }\n\
\n\
    handle /ping { reverse_proxy localhost:8000 }\n\
    handle /auth/* { reverse_proxy localhost:8000 }\n\
    handle /user/* { reverse_proxy localhost:8000 }\n\
    handle /device/* { reverse_proxy localhost:8000 }\n\
    handle /share/* { reverse_proxy localhost:8000 }\n\
    handle /schedule/* { reverse_proxy localhost:8000 }\n\
    handle /passkey/* { reverse_proxy localhost:8000 }\n\
    handle /admin/* { reverse_proxy localhost:8000 }\n\
    handle /household/* { reverse_proxy localhost:8000 }\n\
    handle /items/* { reverse_proxy localhost:8000 }\n\
\n\
    handle {\n\
        root * /app/public\n\
        file_server\n\
\n\
        @html path *.html\n\
        header @html Cache-Control "no-cache, no-store, must-revalidate"\n\
\n\
        @sw path /sw.js\n\
        header @sw Cache-Control "no-cache, no-store, must-revalidate"\n\
\n\
        @manifest path /manifest.json\n\
        header @manifest Cache-Control "public, max-age=3600"\n\
\n\
        @assets path *.js *.css\n\
        header @assets Cache-Control "public, max-age=31536000, immutable"\n\
\n\
        header {\n\
            X-Content-Type-Options "nosniff"\n\
            X-Frame-Options "DENY"\n\
            Referrer-Policy "strict-origin-when-cross-origin"\n\
            -Server\n\
        }\n\
    }\n\
}\n' > /app/Caddyfile

# Write start.sh inline to avoid Windows encoding issues
RUN printf '#!/bin/sh\n\
set -e\n\
\n\
deno run \\\n\
  --unstable-kv \\\n\
  --unstable-cron \\\n\
  --allow-net \\\n\
  --allow-env \\\n\
  --allow-read=/app,/data \\\n\
  --allow-write=/data \\\n\
  /app/main.ts &\n\
\n\
DENO_PID=$!\n\
\n\
echo "Waiting for Deno backend to start..."\n\
i=0\n\
while [ $i -lt 20 ]; do\n\
  if curl -sf http://localhost:8000/ping > /dev/null 2>&1; then\n\
    echo "Deno backend ready."\n\
    break\n\
  fi\n\
  sleep 0.5\n\
  i=$((i+1))\n\
done\n\
\n\
caddy run --config /app/Caddyfile --adapter caddyfile &\n\
CADDY_PID=$!\n\
\n\
wait $DENO_PID\n' > /app/start.sh && chmod +x /app/start.sh

EXPOSE 8080
CMD ["/app/start.sh"]
