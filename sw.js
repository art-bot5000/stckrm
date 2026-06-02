const CACHE_VERSION = 'stockroom-kv-v492';
// Namespace the cache by hostname so staging and production PWAs don't
// fight over the same cache when both are installed on the same device.
// Production hostnames (stckrm.com, app.stckrm.com, stckrm.fly.dev) all
// resolve to 'prod'; anything else (stckrm-staging.fly.dev, localhost)
// gets its own namespace based on the hostname.
const SW_HOSTNAME   = (self.location && self.location.hostname) || 'prod';
const SW_NAMESPACE  = /^(stckrm\.com|app\.stckrm\.com|stckrm\.fly\.dev)$/.test(SW_HOSTNAME)
  ? 'prod'
  : SW_HOSTNAME.replace(/[^a-z0-9]/gi, '-');
const CACHE_NAME    = `${SW_NAMESPACE}-${CACHE_VERSION}`;

const CACHE_URLS = [
  '/',                  // landing page
  '/landing.html',      // direct hit on landing
  '/app',               // app shell
  '/index.html',        // app shell direct
  '/app.js',
  '/budget.js',
  '/styles.css',
  '/manifest.json',
  '/logo.png',          // brand mark — referenced in landing + app header + wizard bars
];

const SYNC_TAG = 'stockroom-sync';

// ── Install ───────────────────────────────────────────────────
self.addEventListener('install', event => {
  // Skip waiting immediately — take over from any old SW right away
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return Promise.all(CACHE_URLS.map(url => {
        return fetch(url + '?v=' + CACHE_VERSION, { cache: 'no-store' })
          .then(res => { if (res.ok) return cache.put(url, res); })
          .catch(() => {});
      }));
    })
  );
});

// ── Activate: delete EVERY cache except current ───────────────
self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    console.log('[SW] activate — found caches:', keys);
    await Promise.all(keys.map(k => {
      if (k !== CACHE_NAME) {
        console.log('[SW] deleting cache:', k);
        return caches.delete(k);
      }
    }));
    // Take control of all open pages immediately
    await self.clients.claim();
    // Tell all clients to reload so they get the new app.js
    const clientList = await self.clients.matchAll({ type: 'window' });
    clientList.forEach(client => {
      client.postMessage({ type: 'SW_UPDATED', version: CACHE_VERSION });
    });
    console.log('[SW] activated v' + CACHE_VERSION + ', told', clientList.length, 'clients to reload');
  })());
});

// ── Message ───────────────────────────────────────────────────
self.addEventListener('message', event => {
  if (event.data?.type === 'SKIP_WAITING') self.skipWaiting();
});

// ── Sync ──────────────────────────────────────────────────────
self.addEventListener('sync', event => {
  if (event.tag === SYNC_TAG) event.waitUntil(triggerAppSync());
});

async function triggerAppSync() {
  const clientList = await clients.matchAll({ type: 'window', includeUncontrolled: true });
  const appClient  = clientList.find(c => c.url.includes('stockroom'));
  if (appClient) {
    appClient.postMessage({ type: 'BG_SYNC' });
  } else {
    const cache = await caches.open('stockroom-flags');
    await cache.put('pending-sync', new Response('1'));
  }
}

// ── Fetch: network-first for app shell, passthrough for API ──
self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;

  const url = new URL(event.request.url);

  // Only handle same-origin requests
  if (url.hostname !== self.location.hostname) return;

  // API routes — passthrough, never cache
  const apiPaths = [
    '/ping','/auth/','/user/','/device/','/share/','/schedule/',
    '/passkey/','/admin/','/household/','/items/','/key/','/data/',
    '/recovery/','/email/','/note/','/invite/','/crypto/','/sync/','/presence',
    '/reminder','/status','/register','/unregister','/unsubscribe',
    '/check-now','/send-now','/debug-schedule','/reset-schedule',
    '/set-schedule','/send-reminder',
  ];
  if (apiPaths.some(p => url.pathname === p || url.pathname.startsWith(p + '/'))) return;

  // Diagnostic and admin pages — always network
  if (['/diag.html', '/admin.html'].includes(url.pathname)) return;

  // App shell — network first, cache fallback for offline
  event.respondWith(
    fetch(event.request, { cache: 'no-store' })
      .then(response => {
        if (response.ok) {
          caches.open(CACHE_NAME).then(c => c.put(event.request, response.clone()));
        }
        return response;
      })
      .catch(() =>
        caches.match(event.request).then(cached => {
          if (cached) return cached;
          if (event.request.mode === 'navigate') {
            // Navigation fallback: app paths get the app shell, others get landing
            const path = new URL(event.request.url).pathname;
            if (path === '/app' || path.startsWith('/app/')) {
              return caches.match('/index.html') || caches.match('/app');
            }
            return caches.match('/landing.html') || caches.match('/');
          }
        })
      )
  );
});

// ── Notification click ────────────────────────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const data = event.notification.data || {};
  const appUrl = data.url || './';

  if (event.action === 'replaced' && data.reminderId && data.token && data.workerUrl) {
    event.waitUntil(
      fetch(`${data.workerUrl}/reminder-done?id=${encodeURIComponent(data.reminderId)}&token=${encodeURIComponent(data.token)}&name=${encodeURIComponent(data.reminderName||'')}&source=push`)
        .then(r => r.json())
        .then(result => {
          const date = result.date || new Date().toISOString().slice(0,10);
          return clients.matchAll({ type:'window', includeUncontrolled:true })
            .then(list => list.forEach(c => c.postMessage({ type:'REMINDER_REPLACED', reminderId:data.reminderId, date, token:data.token })));
        })
        .catch(() => clients.matchAll({ type:'window', includeUncontrolled:true })
          .then(list => { for (const c of list) if (c.url.includes('stockroom') && 'focus' in c) return c.focus(); if (clients.openWindow) return clients.openWindow(appUrl); })
        )
    );
    return;
  }

  event.waitUntil(
    clients.matchAll({ type:'window', includeUncontrolled:true }).then(list => {
      for (const c of list) if (c.url.includes('stockroom') && 'focus' in c) return c.focus();
      if (clients.openWindow) return clients.openWindow(appUrl);
    })
  );
});

// ══════════════════════════════════════════════════════════════════════
// PUSH NOTIFICATIONS (Web Push, Option A)
// ══════════════════════════════════════════════════════════════════════
// The body of each push is a JSON object with v, sourceRef, fallbackTitle,
// and ciphertextB64 (AES-GCM encrypted notification body, with iv prepended).
// The SW decrypts ciphertextB64 using the user's data key, which it reads
// from IndexedDB on each push. If decryption fails (key not available on
// this device, e.g. user hasn't trusted it), we show the fallbackTitle
// with no body — generic enough that we never leak content.

// IDB names — must match constants in app.js (DEVICE_DB_NAME, _DEVICE_INFO_DB, etc.)
// Per-user IDB rewrite: the push device id/secret used to live in the shared
// `stockroom` IDB's `settings` store. They now live in a small device-scoped
// DB (`stockroom-device-info`) that's shared across all users on this
// browser. The SW reads from there so it doesn't need to know — and can't
// guess — which per-user DB to open at push time.
const PUSH_DEVICE_DB_NAME    = 'stockroom-kv-device';
const PUSH_DEVICE_STORE_NAME = 'keys';
const PUSH_MAIN_DB_NAME      = 'stockroom-device-info';
const PUSH_SETTINGS_STORE    = 'kv';

function _pushOpenDb(name) {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(name);
    req.onsuccess = e => resolve(e.target.result);
    req.onerror   = e => reject(e.target.error);
  });
}

function _pushIdbGet(dbName, storeName, key) {
  return _pushOpenDb(dbName).then(db => new Promise((resolve, reject) => {
    try {
      const tx  = db.transaction(storeName, 'readonly');
      const req = tx.objectStore(storeName).get(key);
      req.onsuccess = e => resolve(e.target.result ?? null);
      req.onerror   = e => reject(e.target.error);
    } catch (err) { reject(err); }
  }));
}

// Derive the device-wrap key from a device secret. Mirrors the
// deriveDeviceWrapKey helper in app.js — must stay in sync.
async function _pushDeriveWrapKey(deviceSecret) {
  const base = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(deviceSecret),
    { name: 'PBKDF2' }, false, ['deriveKey'],
  );
  const salt = new TextEncoder().encode('stockroom-device-wrap-v1');
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 50000, hash: 'SHA-256' },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt'],
  );
}

// Load the user's AES-GCM data key from IDB. Returns null if not available
// (e.g. SW has no access to localStorage, but the page mirrors the device
// secret into IDB on sign-in — see _saveDeviceSecretToIdb in app.js).
async function _pushLoadDataKey() {
  try {
    const deviceId = await _pushIdbGet(PUSH_MAIN_DB_NAME, PUSH_SETTINGS_STORE, '_pushDeviceId');
    if (!deviceId) return null;
    const deviceSecret = await _pushIdbGet(PUSH_MAIN_DB_NAME, PUSH_SETTINGS_STORE, '_pushDeviceSecret');
    if (!deviceSecret) return null;
    const stored = await _pushIdbGet(PUSH_DEVICE_DB_NAME, PUSH_DEVICE_STORE_NAME, deviceId);
    if (!stored) return null;
    const combined = Uint8Array.from(atob(stored), c => c.charCodeAt(0));
    const iv        = combined.slice(0, 12);
    const encrypted = combined.slice(12);
    const wrapKey   = await _pushDeriveWrapKey(deviceSecret);
    const keyData   = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, wrapKey, encrypted);
    return crypto.subtle.importKey('raw', keyData, { name: 'AES-GCM', length: 256 }, false, ['decrypt']);
  } catch (err) {
    console.warn('[SW push] _pushLoadDataKey failed:', err && err.message);
    return null;
  }
}

// Decrypt the body ciphertext (base64 of iv(12) || aes-gcm-ciphertext).
async function _pushDecryptBody(dataKey, ciphertextB64) {
  if (!dataKey || !ciphertextB64) return null;
  try {
    const combined = Uint8Array.from(atob(ciphertextB64), c => c.charCodeAt(0));
    if (combined.length < 13) return null;
    const iv         = combined.slice(0, 12);
    const ciphertext = combined.slice(12);
    const plain = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, dataKey, ciphertext);
    const text  = new TextDecoder().decode(plain);
    return JSON.parse(text); // { title, body, tag?, url? }
  } catch (err) {
    console.warn('[SW push] _pushDecryptBody failed:', err && err.message);
    return null;
  }
}

self.addEventListener('push', event => {
  event.waitUntil((async () => {
    let serverBody = null;
    try {
      // event.data is the AES128GCM-decrypted body the push service already
      // unwrapped for us. Format: JSON { v, sourceRef, fallbackTitle, ciphertextB64 }
      serverBody = event.data ? event.data.json() : null;
    } catch (err) {
      console.warn('[SW push] event.data parse failed:', err && err.message);
    }
    const fallbackTitle = (serverBody && serverBody.fallbackTitle) || 'New Stockroom notification';
    const sourceRef     = (serverBody && serverBody.sourceRef) || '';
    // Per-type tag so OS coalesces. Tag is derived from the sourceRef
    // prefix (everything before the first colon): 'reminder', 'lowstock',
    // 'billing', 'account', 'share'. Multiple of the same type collapse
    // into one OS notification.
    const tagPrefix = sourceRef.includes(':') ? sourceRef.split(':')[0] : 'stockroom';
    const tag = `stockroom-${tagPrefix}`;

    // Try to decrypt the body
    let decrypted = null;
    if (serverBody && serverBody.ciphertextB64) {
      const dataKey = await _pushLoadDataKey();
      decrypted = await _pushDecryptBody(dataKey, serverBody.ciphertextB64);
    }

    const title = (decrypted && decrypted.title) || fallbackTitle;
    const opts  = {
      body: (decrypted && decrypted.body) || '',
      tag,
      // Re-notify so the OS lights up the device even when the previous
      // notification with the same tag is still on screen — without this,
      // a tag collision silently replaces with no chime.
      renotify: true,
      icon:  '/icons/icon-192.png',
      badge: '/icons/icon-72.png',
      data: {
        sourceRef,
        url: (decrypted && decrypted.url) || './',
      },
    };
    return self.registration.showNotification(title, opts);
  })());
});

// ── Push subscription change ─────────────────────────────────────────
// Fires when the push service rotates the endpoint (rare but real). We
// can't re-subscribe automatically because we don't have the VAPID
// public key in the SW; instead we postMessage to any open client
// asking it to re-subscribe on next page load.
self.addEventListener('pushsubscriptionchange', event => {
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
      for (const c of list) c.postMessage({ type: 'PUSH_SUBSCRIPTION_CHANGE' });
    })
  );
});
