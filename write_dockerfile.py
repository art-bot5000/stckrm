import pathlib

# The correct Caddy Brotli module is:
# github.com/ueffel/caddy-brotli
# This is the well-maintained, widely-used Brotli encoder for Caddy.
# The download API uses the ?p= query param for plugins.
# URL format: https://caddyserver.com/api/download?os=linux&arch=amd64&p=github.com/ueffel/caddy-brotli

startsh = """\
#!/bin/sh
set -e

deno run \\
  --unstable-kv \\
  --unstable-cron \\
  --allow-net \\
  --allow-env \\
  --allow-read=/app,/data \\
  --allow-write=/data \\
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
"""

caddyfile = """\
:8080 {
    encode {
        zstd
        br
        gzip
        minimum_length 256
    }

    handle /ping {
        reverse_proxy localhost:8000
    }
    handle /user/* {
        reverse_proxy localhost:8000
    }
    handle /device/* {
        reverse_proxy localhost:8000
    }
    handle /share/* {
        reverse_proxy localhost:8000
    }
    handle /passkey/* {
        reverse_proxy localhost:8000
    }
    handle /admin/* {
        reverse_proxy localhost:8000
    }
    handle /key/* {
        reverse_proxy localhost:8000
    }
    # ────────────────────────────────────────────────────────
    # All other backend route prefixes used by main.ts. Without these,
    # the catch-all `handle { file_server }` below returns 404 for every
    # one of them — breaking data sync, billing, MFA, notes, email
    # verification, recovery, presence, push notifications, webhooks,
    # cron triggers, and the unsubscribe flow.
    #
    # IMPORTANT: any new top-level route prefix added to main.ts must
    # ALSO be added here. The deployed Caddyfile must list every prefix
    # main.ts handles, or those calls 404 silently.
    # ────────────────────────────────────────────────────────
    handle /data/* {
        reverse_proxy localhost:8000
    }
    handle /billing/* {
        reverse_proxy localhost:8000
    }
    handle /mfa/* {
        reverse_proxy localhost:8000
    }
    handle /note/* {
        reverse_proxy localhost:8000
    }
    handle /email/* {
        reverse_proxy localhost:8000
    }
    handle /recovery/* {
        reverse_proxy localhost:8000
    }
    handle /referral/* {
        reverse_proxy localhost:8000
    }
    handle /webhook/* {
        reverse_proxy localhost:8000
    }
    handle /presence-stream {
        reverse_proxy localhost:8000
    }
    handle /presence-update {
        reverse_proxy localhost:8000
    }
    handle /check-now {
        reverse_proxy localhost:8000
    }
    handle /send-reminder {
        reverse_proxy localhost:8000
    }
    handle /set-schedule {
        reverse_proxy localhost:8000
    }
    handle /reset-schedule {
        reverse_proxy localhost:8000
    }
    handle /debug-schedule {
        reverse_proxy localhost:8000
    }
    handle /unsubscribe {
        reverse_proxy localhost:8000
    }
    handle /push/* {
        reverse_proxy localhost:8000
    }

    # robots.txt — Lighthouse SEO fix. Block the authenticated app shell
    # from crawlers (no value to index) and the admin panel. Let the marketing
    # landing remain crawlable.
    handle /robots.txt {
        header Content-Type "text/plain; charset=utf-8"
        header Cache-Control "public, max-age=86400"
        respond "User-agent: *
Allow: /
Disallow: /app
Disallow: /admin
"
    }

    # ─────────────────────────────────────────────────────────
    # Host-based root routing
    #
    # stckrm.com / www.stckrm.com  →  serve landing.html at /
    # app.stckrm.com (and any other Host)  →  serve index.html at / (PWA)
    #
    # Both hosts share the same file pool under /app/public so resources
    # (CSS, JS, fonts, images) resolve from either origin. The only thing
    # that differs is which HTML file is served when the user hits /.
    #
    # Logged-in users hitting stckrm.com should be redirected to
    # app.stckrm.com by landing.html's own pre-paint JS — Caddy can't
    # see auth state (it's in IndexedDB) so the redirect is client-side.
    # ─────────────────────────────────────────────────────────
    @landingHost host stckrm.com www.stckrm.com
    handle @landingHost {
        @rootPath path / /index.html
        rewrite @rootPath /landing.html
        root * /app/public
        file_server

        @html path *.html
        header @html Cache-Control "no-cache, no-store, must-revalidate"

        @assets path *.js *.css
        header @assets Cache-Control "public, max-age=31536000, immutable"

        header {
            X-Content-Type-Options "nosniff"
            X-Frame-Options "DENY"
            Referrer-Policy "strict-origin-when-cross-origin"
            Permissions-Policy "camera=*, microphone=()"
            -Server
        }
    }

    handle {
        root * /app/public
        file_server

        @html path *.html
        header @html Cache-Control "no-cache, no-store, must-revalidate"

        @sw path /sw.js
        header @sw Cache-Control "no-cache, no-store, must-revalidate"

        @manifest path /manifest.json
        header @manifest Cache-Control "public, max-age=3600"

        @assets path *.js *.css
        header @assets Cache-Control "public, max-age=31536000, immutable"

        header {
            X-Content-Type-Options "nosniff"
            X-Frame-Options "DENY"
            Referrer-Policy "strict-origin-when-cross-origin"
            Permissions-Policy "camera=*, microphone=()"
            -Server
        }
    }
}
"""

dockerfile = """\
FROM node:22-slim AS builder
WORKDIR /build
COPY package.json ./
RUN npm install
COPY app.js budget.js notes.js demo.js scanner.js ./
COPY styles.css index.html landing.html ./
COPY sw.js manifest.json admin.html diag-trusted.html ./
COPY logo.png ./
RUN mkdir -p public && \\
    npx terser app.js     --compress --mangle --comments false -o public/app.js && \\
    npx terser budget.js  --compress --mangle --comments false -o public/budget.js && \\
    npx terser notes.js   --compress --mangle --comments false -o public/notes.js && \\
    npx terser demo.js    --compress --mangle --comments false -o public/demo.js && \\
    npx terser scanner.js --compress --mangle --comments false -o public/scanner.js && \\
    npx cleancss -o public/styles.css styles.css && \\
    npx html-minifier-terser index.html \\
      --collapse-whitespace --remove-comments --remove-optional-tags \\
      --remove-redundant-attributes --remove-script-type-attributes \\
      --remove-tag-whitespace --minify-css true --minify-js true \\
      -o public/index.html && \\
    npx html-minifier-terser landing.html \\
      --collapse-whitespace --remove-comments --remove-optional-tags \\
      --remove-redundant-attributes --remove-script-type-attributes \\
      --remove-tag-whitespace --minify-css true --minify-js true \\
      -o public/landing.html && \\
    cp sw.js public/sw.js && \\
    cp manifest.json public/manifest.json && \\
    cp admin.html public/admin.html && \\
    cp diag-trusted.html public/diag-trusted.html && \\
    cp logo.png public/logo.png

FROM denoland/deno:2.3.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \\
    apt-get clean && rm -rf /var/lib/apt/lists/* && \\
    curl -fsSL "https://caddyserver.com/api/download?os=linux&arch=amd64&p=github.com/ueffel/caddy-brotli" \\
      -o /usr/local/bin/caddy && \\
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
"""

pathlib.Path('Dockerfile').write_text(dockerfile, encoding='utf-8', newline='\n')
pathlib.Path('start.sh').write_text(startsh, encoding='utf-8', newline='\n')
pathlib.Path('Caddyfile').write_text(caddyfile, encoding='utf-8', newline='\n')

print('All files written successfully:')
print(f'  Dockerfile: {len(dockerfile.splitlines())} lines')
print(f'  start.sh:   {len(startsh.splitlines())} lines')
print(f'  Caddyfile:  {len(caddyfile.splitlines())} lines')
print()
print('Note: also added /key/* route to Caddyfile for passkey key/store endpoint')