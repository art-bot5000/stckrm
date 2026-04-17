FROM denoland/deno:2.3.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "https://caddyserver.com/api/download?os=linux&arch=amd64&p=github.com/ueffel/caddy-brotli" \
      -o /usr/local/bin/caddy && \
    chmod +x /usr/local/bin/caddy
WORKDIR /app
# Copy frontend files directly from repo root — no build step, no src/ directory
RUN mkdir -p /app/public
COPY app.js      /app/public/app.js
COPY index.html  /app/public/index.html
COPY styles.css  /app/public/styles.css
COPY sw.js       /app/public/sw.js
COPY manifest.json /app/public/manifest.json
COPY admin.html  /app/public/admin.html
COPY diag.html   /app/public/diag.html
COPY scanner.js  /app/public/scanner.js
# Copy backend
COPY main.ts     ./main.ts
COPY deno.json   ./deno.json
RUN deno cache --unstable-kv --unstable-cron main.ts
COPY start.sh    /app/start.sh
COPY Caddyfile   /app/Caddyfile
RUN chmod +x /app/start.sh
EXPOSE 8080
CMD ["/app/start.sh"]
