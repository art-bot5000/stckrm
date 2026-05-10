# Caddyfile fix — add missing reverse_proxy handlers for /mfa/* and /note/*

## Bug

Sign-in MFA was broken: `POST /mfa/otp/send` returned **405 Method Not Allowed** with `Allow: GET, HEAD`. Server logs would show nothing, because the request never reached the Deno backend — Caddy was falling through to the `file_server` SPA fallback, which only accepts GET/HEAD.

The client surfaced this as: `Could not send code: Failed to execute 'json' on 'Response': Unexpected end of JSON input` (the client called `.json()` on an empty 405 body).

## Root cause

The Caddyfile has explicit `handle /<prefix>/*` blocks that reverse_proxy to the Deno backend on localhost:8000. Two backend route prefixes that exist in `main.ts` were never added to Caddy:

- `/mfa/*` — used by `/mfa/otp/send` and `/mfa/otp/verify`
- `/note/*` — used by `/note/otp/send`, `/note/body/push`, `/note/body/pull`, `/note/body/delete`

Without those handlers, requests fall through to `handle @app_host { ... file_server }`, which 405s on POST (Allow: GET, HEAD) and 200s on GET (serving index.html via SPA fallback).

## Fix

In `Caddyfile`, in the "API routes (apply to ANY hostname)" section, add these two `handle` blocks alongside the existing ones (anywhere in that group is fine; alphabetical placement shown):

```caddy
    handle /mfa/* {
        reverse_proxy localhost:8000
    }
    handle /note/* {
        reverse_proxy localhost:8000
    }
```

## Verify after deploy

From the browser console on app.stckrm.com:

```js
(async () => {
  const r = await fetch('/mfa/otp/send', { method: 'POST', headers: {'Content-Type':'application/json'}, body: '{}' });
  console.log('mfa status:', r.status); // expect 400 (Missing fields) — proves it hit the worker
})();
```

Status 400 (Missing fields) = fixed; 405 = still broken.

## Bonus — client-side hardening (optional)

`app.js` line ~29219 calls `.json()` on the response without checking `response.ok` first. When the body is empty (any 4xx/5xx without a JSON body) it crashes with "Unexpected end of JSON input" instead of surfacing the real status. Worth adding:

```js
const res = await postKV(`${WORKER_URL}/mfa/otp/send`, {...});
if (!res.ok) {
  const txt = await res.text();
  throw new Error(`MFA send failed: ${res.status} ${txt || res.statusText}`);
}
const data = await res.json();
```

This way future routing/deploy regressions surface as "MFA send failed: 405" instead of a misleading JSON parse error.
