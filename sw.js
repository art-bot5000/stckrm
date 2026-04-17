// ── INCREMENT THIS VERSION NUMBER EVERY TIME YOU DEPLOY ──────
const CACHE_VERSION = 'stockroom-kv-v96';
const CACHE_NAME    = CACHE_VERSION;

const CACHE_URLS = [
  './',
  './index.html',
  './app.js',
  './styles.css',
  './manifest.json',
];

const SYNC_TAG = 'stockroom-sync';

// ── Install: cache core files then activate immediately ───────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      // Fetch each file with cache-busting so we bypass any HTTP cache
      return Promise.all(CACHE_URLS.map(url => {
        const bustUrl = url + (url.includes('?') ? '&' : '?') + '_sw=' + CACHE_VERSION;
        return fetch(bustUrl, { cache: 'no-store' })
          .then(res => {
            if (res.ok) return cache.put(url, res);
          })
          .catch(e => console.warn('SW install: could not cache', url, e.message));
      }));
    })
  );
  // Activate immediately — don't wait for SKIP_WAITING message
  self.skipWaiting();
});

// ── Activate: clean up ALL old caches ────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => {
      console.log('SW activate: found caches:', keys);
      return Promise.all(
        keys
          .filter(k => k !== CACHE_NAME && k !== 'stockroom-flags')
          .map(k => {
            console.log('SW: deleting old cache', k);
            return caches.delete(k);
          })
      );
    }).then(() => self.clients.claim())
  );
});

// ── Message: handle SKIP_WAITING (legacy support) ─────────────
self.addEventListener('message', event => {
  if (event.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

// ── Background Sync ───────────────────────────────────────────
self.addEventListener('sync', event => {
  if (event.tag === SYNC_TAG) {
    event.waitUntil(triggerAppSync());
  }
});

async function triggerAppSync() {
  const clientList = await clients.matchAll({ type: 'window', includeUncontrolled: true });
  const appClient  = clientList.find(c => c.url.includes('stockroom'));
  if (appClient) {
    appClient.postMessage({ type: 'BG_SYNC' });
    console.log('SW: background sync — notified open app client');
  } else {
    console.log('SW: background sync — app closed, flagging for sync on next open');
    const cache = await caches.open('stockroom-flags');
    await cache.put('pending-sync', new Response('1'));
  }
}

// ── Fetch: network-first for same-origin, passthrough for API ─
self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;

  const url = new URL(event.request.url);

  // Passthrough for all third-party APIs
  if (url.hostname !== self.location.hostname) return;

  // Passthrough for API routes on same origin — only cache app shell files
  const apiPrefixes = [
    '/ping', '/auth/', '/user/', '/device/', '/share/', '/schedule/',
    '/passkey/', '/admin/', '/household/', '/items/', '/key/', '/data/',
    '/recovery/', '/email/', '/invite/', '/crypto/', '/sync/', '/presence',
    '/reminder', '/status', '/register', '/unregister', '/unsubscribe',
    '/check-now', '/send-now', '/debug-schedule', '/reset-schedule',
    '/set-schedule', '/send-reminder',
  ];
  if (apiPrefixes.some(p => url.pathname.startsWith(p))) return;

  // Network-first for app shell — ensures fresh files are always used
  event.respondWith(
    fetch(event.request, { cache: 'no-store' })
      .then(response => {
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => {
        // Offline fallback — serve from cache
        return caches.match(event.request).then(cached => {
          if (cached) return cached;
          if (event.request.mode === 'navigate') {
            return caches.match('./index.html');
          }
        });
      })
  );
});

// ── Notification click ────────────────────────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();

  const data       = event.notification.data || {};
  const action     = event.action;
  const reminderId = data.reminderId;
  const token      = data.token;
  const workerUrl  = data.workerUrl;
  const appUrl     = data.url || './';

  if (action === 'replaced' && reminderId && token && workerUrl) {
    event.waitUntil(
      fetch(`${workerUrl}/reminder-done?id=${encodeURIComponent(reminderId)}&token=${encodeURIComponent(token)}&name=${encodeURIComponent(data.reminderName || '')}&source=push`)
        .then(res => res.json())
        .then(result => {
          const date = result.date || new Date().toISOString().slice(0, 10);
          return clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clientList => {
            clientList.forEach(client => client.postMessage({ type: 'REMINDER_REPLACED', reminderId, date, token }));
          });
        })
        .catch(() => {
          return clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clientList => {
            for (const client of clientList) {
              if (client.url.includes('stockroom') && 'focus' in client) return client.focus();
            }
            if (clients.openWindow) return clients.openWindow(appUrl);
          });
        })
    );
    return;
  }

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clientList => {
      for (const client of clientList) {
        if (client.url.includes('stockroom') && 'focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow(appUrl);
    })
  );
});
