const CACHE_VERSION = 'stockroom-kv-v190';
const CACHE_NAME    = CACHE_VERSION;

const CACHE_URLS = [
  './index.html',
  './app.js',
  './styles.css',
  './manifest.json',
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
    await Promise.all(keys.map(k => {
      if (k !== CACHE_NAME) return caches.delete(k);
    }));
    // Take control of all open pages immediately
    await self.clients.claim();
    // Tell all clients to reload so they get the new app.js
    const clientList = await self.clients.matchAll({ type: 'window' });
    clientList.forEach(client => {
      client.postMessage({ type: 'SW_UPDATED', version: CACHE_VERSION });
    });
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
    '/recovery/','/email/','/invite/','/crypto/','/sync/','/presence',
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
          if (event.request.mode === 'navigate') return caches.match('./index.html');
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
