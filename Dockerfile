FROM node:22-slim AS builder
WORKDIR /build
COPY package.json ./
RUN npm install
COPY app.js budget.js budget-ui.js notes.js notes-ui.js share-ui.js demo.js scanner.js ./
COPY styles.css index.html landing.html ./
COPY sw.js manifest.json admin.html diag-trusted.html ./
COPY logo.png logo.webp favicon.ico ./
COPY zxing.min.js ./
RUN mkdir -p public && \
    npx terser app.js     --compress passes=3 --mangle --comments false -o public/app.js && \
    npx terser budget.js  --compress passes=3 --mangle --comments false -o public/budget.js && \
    npx terser budget-ui.js --compress passes=3 --mangle --comments false -o public/budget-ui.js && \
    npx terser notes.js   --compress passes=3 --mangle --comments false -o public/notes.js && \
    npx terser notes-ui.js --compress passes=3 --mangle --comments false -o public/notes-ui.js && \
    npx terser share-ui.js --compress passes=3 --mangle --comments false -o public/share-ui.js && \
    npx terser demo.js    --compress passes=3 --mangle --comments false -o public/demo.js && \
    npx terser scanner.js --compress passes=3 --mangle --comments false -o public/scanner.js && \
    npx cleancss -o public/styles.css styles.css && \
    npx html-minifier-terser index.html \
      --collapse-whitespace --remove-comments --remove-optional-tags \
      --remove-redundant-attributes --remove-script-type-attributes \
      --remove-tag-whitespace --minify-css true --minify-js true \
      -o public/index.html && \
    npx html-minifier-terser landing.html \
      --collapse-whitespace --remove-comments --remove-optional-tags \
      --remove-redundant-attributes --remove-script-type-attributes \
      --remove-tag-whitespace --minify-css true --minify-js true \
      -o public/landing.html && \
    cp sw.js public/sw.js && \
    cp manifest.json public/manifest.json && \
    cp admin.html public/admin.html && \
    cp diag-trusted.html public/diag-trusted.html && \
    cp logo.png public/logo.png && \
    cp logo.webp public/logo.webp && \
    cp favicon.ico public/favicon.ico && \
    cp zxing.min.js public/zxing.min.js

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
