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
    handle /auth/* {
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
    handle /schedule/* {
        reverse_proxy localhost:8000
    }
    handle /passkey/* {
        reverse_proxy localhost:8000
    }
    handle /admin/* {
        reverse_proxy localhost:8000
    }
    handle /household/* {
        reverse_proxy localhost:8000
    }
    handle /items/* {
        reverse_proxy localhost:8000
    }
    handle /key/* {
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
COPY src/ ./src/
RUN mkdir -p public && \\
    npx terser src/app.js --compress --mangle --comments false -o public/app.js && \\
    npx terser src/budget.js --compress --mangle --comments false -o public/budget.js && \\
    npx terser src/scanner.js --compress --mangle --comments false -o public/scanner.js && \\
    npx cleancss -o public/styles.css src/styles.css && \\
    npx html-minifier-terser src/index.html \\
      --collapse-whitespace --remove-comments --remove-optional-tags \\
      --remove-redundant-attributes --remove-script-type-attributes \\
      --remove-tag-whitespace --minify-css true --minify-js true \\
      -o public/index.html && \\
    cp src/sw.js public/sw.js && \\
    cp src/manifest.json public/manifest.json && \\
    cp src/admin.html public/admin.html

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