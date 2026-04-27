// ═══════════════════════════════════════════
//  DIAGNOSTIC — surface silent errors visibly AND log to backend
//  Two layers:
//    1. On-screen red banner so the active user knows something broke
//    2. Best-effort POST to /error/log so the developer has a record
//       across all users (TTL 7 days, dedup'd by message+location)
//  Both layers are wrapped in try/catch and never throw — error reporting
//  must never make things worse.
// ═══════════════════════════════════════════
(function installErrorReporter() {
  if (window._stockroomClickDiagInstalled) return;
  window._stockroomClickDiagInstalled = true;

  // Throttle reports to the backend: dedup identical errors within 60s
  // so a tight loop of failures doesn't hammer the API.
  const _reportSeen = new Map();
  function reportToBackend(payload) {
    try {
      // Need a backend URL to be configured
      if (typeof WORKER_URL === 'undefined' || !WORKER_URL) return;
      const key = `${payload.message}|${payload.where || ''}`;
      const now = Date.now();
      const last = _reportSeen.get(key);
      if (last && now - last < 60_000) return;
      _reportSeen.set(key, now);
      // Trim the map if it grows too large
      if (_reportSeen.size > 50) {
        const cutoff = now - 60_000;
        for (const [k, t] of _reportSeen) if (t < cutoff) _reportSeen.delete(k);
      }
      // Add identifying context where available — these globals may not be defined yet
      const ctx = {
        emailHash: typeof _kvEmailHash !== 'undefined' ? (_kvEmailHash || null) : null,
        ua: navigator.userAgent.slice(0, 200),
        ts: new Date().toISOString(),
        url: location.pathname + location.search,
      };
      // Fire-and-forget — never block on the report
      fetch(`${WORKER_URL}/error/log`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...payload, ...ctx }),
        keepalive: true, // survive page unload
      }).catch(() => {});
    } catch (_) {}
  }

  function showBanner(msg) {
    try {
      let bar = document.getElementById('_diag-bar');
      if (!bar) {
        bar = document.createElement('div');
        bar.id = '_diag-bar';
        bar.style.cssText = 'position:fixed;bottom:0;left:0;right:0;background:#7a1d1d;color:#fff;padding:10px 14px;font:600 12px/1.4 monospace;z-index:99999;cursor:pointer';
        bar.onclick = () => bar.remove();
        document.body && document.body.appendChild(bar);
      }
      bar.textContent = `⚠ ${msg} — tap to dismiss`;
    } catch (_) {}
  }

  window.addEventListener('error', e => {
    try {
      const msg = e.message || (e.error && e.error.message) || 'Unknown error';
      const where = e.filename ? `@${(e.filename + '').split('/').pop()}:${e.lineno}` : '';
      const stack = (e.error && e.error.stack) ? String(e.error.stack).slice(0, 1500) : '';
      showBanner(`${msg}${where ? ' ' + where : ''}`);
      reportToBackend({ kind: 'error', message: msg, where, stack });
      console.error('[stockroom diag]', e);
    } catch (_) {}
  });

  window.addEventListener('unhandledrejection', e => {
    try {
      const reason = e.reason;
      const msg = (reason && (reason.message || reason)) || 'Unknown promise rejection';
      const stack = (reason && reason.stack) ? String(reason.stack).slice(0, 1500) : '';
      showBanner(String(msg));
      reportToBackend({ kind: 'rejection', message: String(msg), stack });
      console.error('[stockroom diag rejection]', e);
    } catch (_) {}
  });
})();

// ═══════════════════════════════════════════
//  DATA
// ═══════════════════════════════════════════
const CATEGORIES = ['Kitchen','Bathroom','Cleaning','Food & Drink','Health','Garden','Office','Other'];

const COUNTRIES = [
  { code:'GB', flag:'🇬🇧', name:'United Kingdom' },
  { code:'US', flag:'🇺🇸', name:'United States' },
  { code:'CA', flag:'🇨🇦', name:'Canada' },
  { code:'AU', flag:'🇦🇺', name:'Australia' },
  { code:'DE', flag:'🇩🇪', name:'Germany' },
  { code:'FR', flag:'🇫🇷', name:'France' },
  { code:'NL', flag:'🇳🇱', name:'Netherlands' },
  { code:'IE', flag:'🇮🇪', name:'Ireland' },
  { code:'ES', flag:'🇪🇸', name:'Spain' },
  { code:'IT', flag:'🇮🇹', name:'Italy' },
  { code:'SE', flag:'🇸🇪', name:'Sweden' },
  { code:'JP', flag:'🇯🇵', name:'Japan' },
  { code:'OTHER', flag:'🌍', name:'Other' },
];

const STORES_BY_COUNTRY = {
  GB: [
    { name:'🛒 Amazon UK',      url: q => `https://www.amazon.co.uk/s?k=${q}` },
    { name:'🏪 Costco UK',      url: q => `https://www.costco.co.uk/search?q=${q}` },
    { name:"🛍️ Ocado",          url: q => `https://www.ocado.com/search?entry=${q}` },
    { name:'🛍️ Tesco',          url: q => `https://www.tesco.com/groceries/en-GB/search?query=${q}` },
    { name:"🛍️ Sainsbury's",    url: q => `https://www.sainsburys.co.uk/gol-ui/SearchDisplayView?searchTerm=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.co.uk/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://uk.camelcamelcamel.com/search?sq=${q}` },
  ],
  US: [
    { name:'🛒 Amazon US',      url: q => `https://www.amazon.com/s?k=${q}` },
    { name:'🏪 Costco US',      url: q => `https://www.costco.com/catalogsearch/results?q=${q}` },
    { name:'🛍️ Walmart',        url: q => `https://www.walmart.com/search?q=${q}` },
    { name:'🛍️ Target',         url: q => `https://www.target.com/s?searchTerm=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.com/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://camelcamelcamel.com/search?sq=${q}` },
  ],
  CA: [
    { name:'🛒 Amazon CA',      url: q => `https://www.amazon.ca/s?k=${q}` },
    { name:'🏪 Costco CA',      url: q => `https://www.costco.ca/catalogsearch/results?q=${q}` },
    { name:'🛍️ Walmart CA',     url: q => `https://www.walmart.ca/search?q=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.ca/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://camelcamelcamel.com/search?sq=${q}` },
  ],
  AU: [
    { name:'🛒 Amazon AU',      url: q => `https://www.amazon.com.au/s?k=${q}` },
    { name:'🏪 Costco AU',      url: q => `https://www.costco.com.au/search?q=${q}` },
    { name:'🛍️ Woolworths',     url: q => `https://www.woolworths.com.au/shop/search/products?searchTerm=${q}` },
    { name:'🛍️ Coles',          url: q => `https://www.coles.com.au/search?q=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.com.au/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://camelcamelcamel.com/search?sq=${q}` },
  ],
  DE: [
    { name:'🛒 Amazon DE',      url: q => `https://www.amazon.de/s?k=${q}` },
    { name:'🛍️ REWE',           url: q => `https://shop.rewe.de/productList?search=${q}` },
    { name:'🛍️ dm',             url: q => `https://www.dm.de/search?query=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.de/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://camelcamelcamel.com/search?sq=${q}` },
  ],
  FR: [
    { name:'🛒 Amazon FR',      url: q => `https://www.amazon.fr/s?k=${q}` },
    { name:'🛍️ Carrefour',      url: q => `https://www.carrefour.fr/recherche?query=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.fr/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://camelcamelcamel.com/search?sq=${q}` },
  ],
  NL: [
    { name:'🛒 Amazon NL',      url: q => `https://www.amazon.nl/s?k=${q}` },
    { name:'🛍️ Bol.com',        url: q => `https://www.bol.com/nl/nl/s/?searchtext=${q}` },
    { name:'🛍️ Albert Heijn',   url: q => `https://www.ah.nl/zoeken?query=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.nl/search?q=${q}&tbm=shop` },
  ],
  IE: [
    { name:'🛒 Amazon UK',      url: q => `https://www.amazon.co.uk/s?k=${q}` },
    { name:'🛍️ Tesco IE',       url: q => `https://www.tesco.ie/groceries/en-IE/search?query=${q}` },
    { name:'🛍️ Dunnes',         url: q => `https://www.dunnesstoresgrocery.com/sm/delivery/rsid/258/results?q=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.ie/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://uk.camelcamelcamel.com/search?sq=${q}` },
  ],
  ES: [
    { name:'🛒 Amazon ES',      url: q => `https://www.amazon.es/s?k=${q}` },
    { name:'🛍️ El Corte Inglés',url: q => `https://www.elcorteingles.es/buscar/?s=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.es/search?q=${q}&tbm=shop` },
  ],
  IT: [
    { name:'🛒 Amazon IT',      url: q => `https://www.amazon.it/s?k=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.it/search?q=${q}&tbm=shop` },
  ],
  SE: [
    { name:'🛒 Amazon DE',      url: q => `https://www.amazon.de/s?k=${q}` },
    { name:'🛍️ ICA',            url: q => `https://www.ica.se/sok/?query=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.se/search?q=${q}&tbm=shop` },
  ],
  JP: [
    { name:'🛒 Amazon JP',      url: q => `https://www.amazon.co.jp/s?k=${q}` },
    { name:'🛍️ Rakuten',        url: q => `https://search.rakuten.co.jp/search/mall/${q}/` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.co.jp/search?q=${q}&tbm=shop` },
  ],
  OTHER: [
    { name:'🛒 Amazon',         url: q => `https://www.amazon.com/s?k=${q}` },
    { name:'🔍 Google Shop',    url: q => `https://www.google.com/search?q=${q}&tbm=shop` },
    { name:'💰 CamelCamelCamel',url: q => `https://camelcamelcamel.com/search?sq=${q}` },
  ],
};

function getStores(code) { return STORES_BY_COUNTRY[code] || STORES_BY_COUNTRY.OTHER; }

// ═══════════════════════════════════════════
//  INDEXEDDB — async storage layer
//  Replaces localStorage for all data stores.
//  UI state (wizard, compact, notif flags) stays in localStorage.
// ═══════════════════════════════════════════

const DB_NAME    = 'stockroom';
const DB_VERSION = 2;
const DB_STORES  = ['items','settings','reminders','groceries','departments','deletedIds','profiles','groceryDeletedIds','groceryLists'];

let _db = null;

function openDB() {
  if (_db) return Promise.resolve(_db);
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = e => {
      const db = e.target.result;
      DB_STORES.forEach(s => { if (!db.objectStoreNames.contains(s)) db.createObjectStore(s); });
    };
    req.onsuccess = e => {
      _db = e.target.result;
      // If another tab upgrades the DB, reset our handle so we re-open with new schema
      _db.onversionchange = () => { _db.close(); _db = null; };
      resolve(_db);
    };
    req.onerror   = e => reject(e.target.error);
  });
}

async function dbGet(store, key) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(store, 'readonly');
    const req = tx.objectStore(store).get(key);
    req.onsuccess = e => resolve(e.target.result ?? null);
    req.onerror   = e => reject(e.target.error);
  });
}

async function dbPut(store, key, value) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(store, 'readwrite');
    const req = tx.objectStore(store).put(value, key);
    req.onsuccess = () => resolve();
    req.onerror   = e => reject(e.target.error);
  });
}

async function dbDelete(store, key) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(store, 'readwrite');
    const req = tx.objectStore(store).delete(key);
    req.onsuccess = () => resolve();
    req.onerror   = e => reject(e.target.error);
  });
}

// Migrate a single key from localStorage into IndexedDB then remove it
async function migrateFromLocalStorage(lsKey, dbStore, dbKey, transform) {
  const raw = localStorage.getItem(lsKey);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    const value  = transform ? transform(parsed) : parsed;
    await dbPut(dbStore, dbKey, value);
    localStorage.removeItem(lsKey);
    return value;
  } catch(e) {
    console.warn('Migration failed for', lsKey, e);
    return null;
  }
}

// ═══════════════════════════════════════════
//  CONFIG
// ═══════════════════════════════════════════
const CLIENT_ID       = '589308993147-rfj3kbaave6uhf3k1ojes3ph2l1pkd1m.apps.googleusercontent.com';
const SCOPES          = 'https://www.googleapis.com/auth/drive.file';
// KV-native: no Drive file
const WORKER_URL      = 'https://stckrm.fly.dev';

// ═══════════════════════════════════════════
//  STATE
// ═══════════════════════════════════════════
let items = [];
let settings = { threshold: 20, country: 'GB' };
let editingId = null;
let loggingId = null;
let tempStorePrices = []; // also declared in scanner.js; declared here so openAddModal works before scanner.js loads
let activeFilter = 'all';
let activeCadence = 'all';
let activeStore = 'all';
let activeRating = 0;
let currentRating = 0;
let wizardCountry = 'GB';
let compactView = false;
let activeProfile = 'default'; // household profile key

// ── Notes state ───────────────────────────────────────────
let notes = [];                    // array of note metadata + body (unlocked) or no body (locked)
let _notesFilter = 'all';          // 'all'|'pinned'|'archived'|'trash'
let _notesSearch = '';
let _editingNoteId = null;         // currently open note id
let _noteUnlocked = new Map();     // noteId → { body, lastActivity, inactivityTimer }
let _noteColourPickerOpen = false;
let _noteUndoStack = new Map();    // noteId → string[]
let _noteRedoStack = new Map();    // noteId → string[]
let _noteBodyDirty = false;        // unsaved changes flag
let _noteAutoSaveTimer = null;
let _noteOtpPending = false;       // waiting for 2FA OTP input

// ═══════════════════════════════════════════
//  MODAL HELPERS (defined early — used everywhere)
// ═══════════════════════════════════════════
function openModal(id) {
  const el = document.getElementById(id);
  if (!el) { console.error('openModal: element not found:', id); return; }
  // Hide FAB and done-slide while any modal is open
  const fabBtn = document.getElementById('fab-btn');
  if (fabBtn) fabBtn.style.opacity = '0';
  document.getElementById('grocery-done-slide')?.remove();
  closeFab(true);
  try {
    if (_vtSupported() && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      el.style.viewTransitionName = 'modal-layer';
      document.startViewTransition(() => { el.classList.add('open'); });
    } else {
      el.classList.add('open');
    }
  } catch(e) {
    console.warn('openModal: view transition failed, opening directly', e.message);
    el.classList.add('open');
  }
}
function closeModal(id) {
  if (_vtSupported() && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    const el = document.getElementById(id);
    el.style.viewTransitionName = 'modal-layer';
    document.startViewTransition(() => { el.classList.remove('open'); });
  } else {
    document.getElementById(id).classList.remove('open');
  }
  // Restore FAB visibility if no other modals remain open
  setTimeout(() => {
    const anyOpen = document.querySelector('.modal-backdrop.open, .modal.open');
    if (!anyOpen) {
      const fabBtn = document.getElementById('fab-btn');
      if (fabBtn && fabBtn.style.display !== 'none') fabBtn.style.opacity = '1';
      // Restore done-editing slide if still in grocery edit mode
      if (_currentView === 'grocery' && groceryEditMode) _showGroceryDoneSlide();
    }
  }, 50);
}

// ═══════════════════════════════════════════
//  UTILS
// ═══════════════════════════════════════════
const uid = () => 'id_' + Date.now() + '_' + Math.random().toString(36).slice(2,7);

// ── Lucide icon helper ────────────────────────────────────────────────────
// ic('package')           → small inline icon (16px)
// ic('package','md')      → 18px
// ic('package','lg')      → 20px
// ic('package','xl')      → 24px
// ic('package','tab')     → 22px for nav tabs
function ic(name, size='') {
  const cls = size ? `icon icon-${size}` : 'icon';
  return `<svg class="${cls}" aria-hidden="true"><use href="#i-${name}"></use></svg>`;
}
const today = () => new Date().toISOString().split('T')[0];
const fmtDate = d => d ? new Date(d+'T12:00:00').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}) : '—';
// Generic date formatter — accepts both 'YYYY-MM-DD' and full ISO timestamps.
// Replaces a previously-local helper that several call sites still reference.
const fmt = d => {
  if (!d) return '—';
  const s = String(d);
  const date = s.length <= 10 ? new Date(s + 'T12:00:00') : new Date(s);
  if (isNaN(date)) return '—';
  return date.toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' });
};

function timeAgo(dateStr) {
  if (!dateStr) return '—';
  const days = Math.floor((Date.now() - new Date(dateStr+'T12:00:00')) / 86400000);
  if (days === 0) return 'today';
  if (days === 1) return 'yesterday';
  if (days < 7)  return `${days}d ago`;
  if (days < 30) return `${Math.floor(days/7)}w ago`;
  if (days < 365) return `${Math.floor(days/30)}mo ago`;
  return `${Math.floor(days/365)}y ago`;
}

function calcStock(item) {
  if (!item.logs || !item.logs.length) return null;
  // Only count delivered (non-pending) logs for stock calculation
  const deliveredLogs = item.logs.filter(l => !l.pendingDelivery);
  if (!deliveredLogs.length) return null;
  const last = deliveredLogs[deliveredLogs.length - 1];

  // If a manual stock count has been recorded, project from that count
  if (item.stockCount != null && item.stockCountDate) {
    const daysSinceCount  = (Date.now() - new Date(item.stockCountDate+'T12:00:00')) / 86400000;
    const unitsRemaining  = item.stockCount;
    const daysPerUnit     = (item.months || 1) * 30.5;
    const totalDays       = daysPerUnit * Math.max(1, unitsRemaining);
    let daysLeft;
    const daysSincePurchase = (Date.now() - new Date(last.date+'T12:00:00')) / 86400000;
    const used = (last.qty || 1) - unitsRemaining;
    if (used > 0 && daysSincePurchase > 1) {
      const ratePerDay = used / daysSincePurchase;
      daysLeft = Math.round(Math.max(0, unitsRemaining / ratePerDay));
    } else {
      daysLeft = Math.round(Math.max(0, daysPerUnit * unitsRemaining - daysSinceCount));
    }
    const pct = Math.round(Math.max(0, Math.min(100, (daysLeft / Math.max(1, totalDays)) * 100)));
    return { pct, daysLeft, referenceDate: item.stockCountDate, fromStockCount: true };
  }

  // Default: time-based from purchase / startedUsing date
  const referenceDate = item.startedUsing || last.date;
  const daysSince  = (Date.now() - new Date(referenceDate+'T12:00:00')) / 86400000;
  const totalDays  = (item.months||1) * 30.5 * (last.qty||1);
  const daysLeft   = Math.round(Math.max(0, totalDays - daysSince));
  const pct        = Math.round(Math.max(0, Math.min(100, (daysLeft / totalDays) * 100)));
  return { pct, daysLeft, referenceDate };
}

const STATUS_COLOR = { critical:'#e85050', warn:'#e8a838', ok:'#4cbb8a', nodata:'#7880a0' };
const STATUS_LABEL = { critical:`<span class='status-dot status-critical'></span> Critical`, warn:`<span class='status-dot status-low'></span> Low`, ok:`<span class='status-dot status-ok'></span> Good`, nodata:`<span class='status-dot status-none'></span> No data` };

// ═══════════════════════════════════════════
//  PERSISTENCE — IndexedDB backed
// ═══════════════════════════════════════════
async function loadData() {
  // Detect user switch — if emailHash changed, wipe stale local data before sync
  const storedHash = await dbGet('settings', '_activeUserHash');
  const currentHash = _kvEmailHash || null;
  if (currentHash && storedHash && storedHash !== currentHash) {
    await dbPut('items',    'items',    []);
    await dbPut('settings', 'settings', {});
    await dbPut('profiles', 'profiles', {});
    await dbPut('settings', '_activeUserHash', currentHash);
    items    = [];
    settings = { threshold:20, country:'GB', email:'', emailInterval:7, emailStartDate:null, emailStartTime:'09:00', displayName:'', mfa:{ enabled:false, method:'email', totpSecret:null }, customTags:[], lastSynced:'' };
    return;
  }
  if (currentHash) await dbPut('settings', '_activeUserHash', currentHash);

  // Try IndexedDB first
  let loadedItems    = await dbGet('items',    'items');
  let loadedSettings = await dbGet('settings', 'settings');

  // First-run migration from localStorage
  if (!loadedItems) {
    loadedItems = await migrateFromLocalStorage('stockroom_items', 'items', 'items', v => v);
  }
  if (!loadedSettings) {
    loadedSettings = await migrateFromLocalStorage('stockroom_settings', 'settings', 'settings', v => v);
  }

  if (Array.isArray(loadedItems)) items = loadedItems;
  if (loadedSettings && typeof loadedSettings === 'object') settings = { ...settings, ...loadedSettings };

  // Backfill localStorage from user settings so early-firing browser events (beforeinstallprompt)
  // also see the dismissed flag without waiting for a network sync
  if (settings._installDismissed) {
    try { localStorage.setItem('stockroom_install_dismissed', '1'); } catch(e) {}
    try { localStorage.setItem('stockroom_ios_banner_dismissed', '1'); } catch(e) {}
  }
  // Hide Amazon banner immediately if previously dismissed (avoid flash)
  if (settings._amazonBannerDismissed) {
    const desktop = document.getElementById('amazon-banner-desktop');
    const mobile  = document.getElementById('amazon-banner-mobile');
    if (desktop) desktop.style.display = 'none';
    if (mobile)  mobile.style.display  = 'none';
  }
}

async function saveData() {
  if (!Array.isArray(items)) { console.error('stockroom: items is not an array, aborting save'); return; }
  await dbPut('items', 'items', items);
  if (activeProfile) await saveCurrentProfile();
  registerBackgroundSync();
  bcPost({ type: 'DATA_CHANGED' });
}

async function saveSettings() {
  settings.threshold      = parseInt(document.getElementById('setting-threshold').value);
  // Country: read from Account & Security select (the canonical visible one); hidden input is a fallback mirror
  settings.country        = (document.getElementById('setting-country-sec') || document.getElementById('setting-country'))?.value || settings.country || 'GB';
  settings.email          = document.getElementById('setting-email').value.trim();
  settings.emailInterval  = parseInt(document.getElementById('setting-email-interval').value);
  settings.emailStartDate = document.getElementById('setting-email-start').value || null;
  settings.emailStartTime = document.getElementById('setting-email-start-time').value || '09:00';
  await dbPut('settings', 'settings', settings);
  updateLastSentUI();
  pushScheduleToWorker();
  scheduleRender('settings-ui', 'sns');
  // Keep Account & Security view in sync if it's visible
  renderAccountSecurity();
}

function _capitaliseFirst(str) {
  if (!str) return '';
  return str.charAt(0).toUpperCase() + str.slice(1);
}

function saveDisplayName(raw) {
  const name = _capitaliseFirst((raw || '').trim());
  settings.displayName = name;
  _saveSettings();
  updateHeaderGreeting();
}

function wizardNameInput(raw) {
  const name = _capitaliseFirst((raw || '').trim());
  settings.displayName = name || undefined;
}

function updateHeaderGreeting() {
  const el = document.getElementById('header-greeting');
  if (!el) return;
  const name = settings.displayName ? _capitaliseFirst(settings.displayName) : '';
  if (name) {
    el.style.display = 'flex';
    el.innerHTML = `Hi, <strong style="color:var(--text);margin-left:3px">${esc(name)}</strong>`;
  } else {
    el.style.display = 'flex';
    el.innerHTML = `Hi, <strong style="color:var(--text);margin-left:3px">there</strong> — <a href="#" onclick="event.preventDefault();openSettingsSection('settings-prefs-body')" style="color:var(--accent);margin-left:4px;font-size:11px">add your name</a>`;
  }
}

// renderAccountSecurity moved to Account & Security section below

function _updateSidebarProfile() {
  const el = document.getElementById('app-nav-profile');
  if (!el) return;
  const name = settings.displayName ? _capitaliseFirst(settings.displayName) : '';
  if (name) {
    el.textContent = 'Hi, ' + name;
    el.style.color = 'var(--muted)';
    el.style.cursor = 'default';
    el.onclick = null;
  } else {
    el.textContent = '';
  }
}

function saveSettingsCountry(val) {
  settings.country = val;
  // Keep both selects in sync
  const main = document.getElementById('setting-country');
  const sec  = document.getElementById('setting-country-sec');
  if (main) main.value = val;
  if (sec)  sec.value  = val;
  _saveSettings();
}

function openSettingsSection(sectionId) {
  const tab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Settings'));
  if (tab) showView('settings', tab);
  setTimeout(() => {
    const el = document.getElementById(sectionId);
    if (el) {
      el.classList.add('open');
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, 150);
}

async function _saveSettings() {
  // Internal: save settings without triggering UI side effects
  await dbPut('settings', 'settings', settings);
  bcPost({ type: 'SETTINGS_CHANGED' });
}

// Stamp updatedAt on an item whenever it's modified
// ── Field-level timestamps (lightweight CRDT) ────────────
// touchField stamps individual fields in item._fieldTs so mergeItems
// can resolve conflicts at field granularity rather than whole-item LWW.
// Fields with no _fieldTs entry fall back to item-level updatedAt.
function touchField(item, ...fields) {
  const now = new Date().toISOString();
  item.updatedAt = now;
  if (!item._fieldTs) item._fieldTs = {};
  if (fields.length === 0) {
    // No specific fields — stamp the whole item (backwards compat for touchItem callers)
    return;
  }
  fields.forEach(f => { item._fieldTs[f] = now; });
}

// Keep touchItem as an alias for callers that set multiple fields at once
function touchItem(item) { touchField(item); }

// ── Logs union merge ─────────────────────────────────────
// Logs are append-only — merge by ID union, never discard an entry
function mergeLogs(localLogs = [], remoteLogs = []) {
  const byId = new Map();
  // Remote first so local can overwrite (local edits to existing logs win)
  for (const l of remoteLogs) byId.set(l.id, l);
  for (const l of localLogs)  byId.set(l.id, l);
  return Array.from(byId.values()).sort((a, b) => new Date(a.date) - new Date(b.date));
}

// ── Field-level item merge ───────────────────────────────
function mergeItemFields(local, remote) {
  // Start with a shallow copy of local
  const result = { ...local };

  // Fields we track individually — everything except id, updatedAt, _fieldTs, logs
  const SCALAR_FIELDS = [
    'name','category','cadence','qty','months','url','store','notes',
    'startedUsing','rating','imageUrl','storePrices','expiry',
    'thresholdOverride','replacementInterval','replacementUnit',
    'lastReplaced','ordered','orderedAt','quickAdded','tags',
    'subscribeAndSave','stockCount','stockCountDate',
  ];

  const localTs  = local._fieldTs  || {};
  const remoteTs = remote._fieldTs || {};
  const itemLocalTime  = local.updatedAt  ? new Date(local.updatedAt).getTime()  : 0;
  const itemRemoteTime = remote.updatedAt ? new Date(remote.updatedAt).getTime() : 0;

  for (const field of SCALAR_FIELDS) {
    if (!(field in remote)) continue; // remote doesn't have this field — keep local
    const lt = localTs[field]  ? new Date(localTs[field]).getTime()  : itemLocalTime;
    const rt = remoteTs[field] ? new Date(remoteTs[field]).getTime() : itemRemoteTime;
    if (rt > lt) result[field] = remote[field];
  }

  // Logs: union merge by ID
  result.logs = mergeLogs(local.logs, remote.logs);

  // Merge _fieldTs — keep the newest timestamp per field
  result._fieldTs = { ...remoteTs, ...localTs };
  for (const f of Object.keys(remoteTs)) {
    if (!localTs[f] || new Date(remoteTs[f]) > new Date(localTs[f])) {
      result._fieldTs[f] = remoteTs[f];
    }
  }

  // updatedAt = newest of the two
  result.updatedAt = itemLocalTime >= itemRemoteTime ? local.updatedAt : remote.updatedAt;

  return result;
}

async function mergeItems(local, remote, remoteWins = false) {
  const deletedIds = await loadDeletedIds();
  const merged = new Map();

  for (const item of local) {
    if (!deletedIds.has(item.id)) merged.set(item.id, item);
  }

  for (const remoteItem of remote) {
    if (deletedIds.has(remoteItem.id)) continue;
    const localItem = merged.get(remoteItem.id);
    if (!localItem) {
      // Item only in remote — add it
      merged.set(remoteItem.id, remoteItem);
    } else {
      // Item in both — field-level merge
      merged.set(remoteItem.id, mergeItemFields(localItem, remoteItem));
    }
  }

  return Array.from(merged.values());
}
// ── Grocery tombstones — tracks deleted grocery item IDs so they aren't re-added on sync
async function loadGroceryDeletedIds() {
  try {
    const stored = await dbGet('groceryDeletedIds', 'groceryDeletedIds');
    return new Set(Array.isArray(stored) ? stored : []);
  } catch(e) { return new Set(); }
}
async function addGroceryTombstone(id) {
  try {
    const set = await loadGroceryDeletedIds();
    set.add(id);
    // Keep max 500 tombstones; prune oldest if over limit
    const arr = [...set];
    const trimmed = arr.slice(-500);
    await dbPut('groceryDeletedIds', 'groceryDeletedIds', trimmed);
  } catch(e) {}
}

async function loadDeletedIds() {
  const stored = await dbGet('deletedIds', 'deletedIds');
  if (stored) return new Set(stored);
  // Migration from localStorage
  try {
    const raw = localStorage.getItem('stockroom_deleted_ids');
    if (raw) {
      const set = new Set(JSON.parse(raw));
      await saveDeletedIds(set);
      localStorage.removeItem('stockroom_deleted_ids');
      return set;
    }
  } catch(e) {}
  return new Set();
}
async function saveDeletedIds(set) {
  await dbPut('deletedIds', 'deletedIds', [...set]);
}
async function addTombstone(id) {
  const set = await loadDeletedIds();
  set.add(id);
  await saveDeletedIds(set);
}
async function removeTombstone(id) {
  const set = await loadDeletedIds();
  set.delete(id);
  await saveDeletedIds(set);
}

// ═══════════════════════════════════════════
//  WIZARD
// ═══════════════════════════════════════════
function buildCountryGrid() {
  const grid = document.getElementById('country-grid');
  if (!grid) return;
  // Pick up any selection made before app.js loaded (via the inline stub)
  if (window._pendingCountry) wizardCountry = window._pendingCountry;
  // If buttons already exist (pre-rendered in HTML), just update selected state
  if (grid.children.length > 0) {
    selectCountry(wizardCountry);
    return;
  }
  // Fallback: build from COUNTRIES array if somehow empty
  grid.innerHTML = COUNTRIES.map(c => `
    <button class="country-btn${c.code===wizardCountry?' selected':''}" id="cbtn-${c.code}" onclick="selectCountry('${c.code}')">
      <span style="font-size:22px">${c.flag}</span>
      <span>${c.name}</span>
      <span class="check">✓</span>
    </button>`).join('');
  updateCountryConfirm();
}

function selectCountry(code) {
  wizardCountry = code;
  window._pendingCountry = null; // clear any pending stub selection
  document.querySelectorAll('.country-btn').forEach(b => b.classList.remove('selected'));
  document.getElementById('cbtn-'+code)?.classList.add('selected');
  updateCountryConfirm();
}

function updateCountryConfirm() {
  const c = COUNTRIES.find(x => x.code === wizardCountry);
  const el = document.getElementById('country-confirm');
  if (el && c) el.textContent = `✓ Using ${c.flag} ${c.name} stores`;
}

function enableItemEdit() {
  document.getElementById('item-modal-title').textContent = 'Edit Item';
  document.getElementById('item-modal-subtitle').textContent = 'Editing: ' + (items.find(i => i.id === editingId)?.name || '');
  document.getElementById('item-readonly-view').style.display = 'none';
  document.getElementById('item-edit-view').style.display = 'block';
}

function wizardNext() {
  localStorage.setItem('stockroom_country_set', '1');
  document.getElementById('wizard-step-1').classList.remove('active');
  document.getElementById('wizard-step-2').classList.add('active');
  document.getElementById('dot-1').classList.remove('active');
  document.getElementById('dot-1').classList.add('done');
  document.getElementById('dot-2').classList.add('active');
  document.getElementById('dot-2').style.width = '24px';
}

function wizardBack() {
  document.getElementById('wizard-step-2').classList.remove('active');
  document.getElementById('wizard-step-1').classList.add('active');
  document.getElementById('dot-2').classList.remove('active');
  document.getElementById('dot-2').style.width = '8px';
  document.getElementById('dot-1').classList.remove('done');
  document.getElementById('dot-1').classList.add('active');
}

async function wizardFinish() {
  try {
    // Pick up country selected before app.js loaded (via inline stub)
    if (window._pendingCountry) wizardCountry = window._pendingCountry;
    settings.country = wizardCountry;
    // Capture name from wizard if entered
    const wizardName = document.getElementById('wizard-display-name')?.value?.trim();
    if (wizardName) settings.displayName = _capitaliseFirst(wizardName);
    await _saveSettings();
    await setCountrySetForDevice();
    const countrySel = document.getElementById('setting-country');
    if (countrySel) countrySel.value = settings.country;
    updateHeaderGreeting();
    // MFA already verified before we got here — go straight to Stockroom
    await _enterStockroom();
  } catch(e) {
    console.error('wizardFinish error:', e);
    // Still try to dismiss wizard even if something failed
    localStorage.setItem('stockroom_seen', '1');
    localStorage.setItem('stockroom_country_set', '1');
    document.body.classList.remove('wizard-active');
    document.getElementById('wizard').style.display = 'none';
  }
}

// ═══════════════════════════════════════════
//  VIEWS & TABS
// ═══════════════════════════════════════════

function setFilter(type, value, btn) {
  if (type === 'status') {
    activeFilter = value;
    // deactivate all status + cadence chips, activate clicked
    document.querySelectorAll('#filter-bar .filter-chip').forEach(c => c.classList.remove('active'));
  } else {
    activeCadence = activeCadence === value ? 'all' : value;
    // toggle cadence chips
    document.querySelectorAll('#filter-bar .filter-chip').forEach(c => {
      if (c.textContent.includes('Monthly') || c.textContent.includes('Bulk')) c.classList.remove('active');
    });
  }
  if (btn) btn.classList.add('active');
  if (activeCadence === 'all' && type === 'cadence') btn.classList.remove('active');
  updateFilterBadge();
  renderGrid();
}

function setStoreFilter(store, btn) {
  activeStore = store;
  document.querySelectorAll('#store-filter-bar .filter-chip').forEach(c => c.classList.remove('active'));
  btn.classList.add('active');
  updateFilterBadge();
  renderGrid();
}

function buildStoreFilterBar() {
  const bar = document.getElementById('store-filter-bar');
  const storeSet = new Set();
  items.forEach(item => {
    if (item.store && item.store.trim()) storeSet.add(item.store.trim());
    (item.logs||[]).forEach(l => { if (l.store && l.store.trim()) storeSet.add(l.store.trim()); });
  });
  const stores = [...storeSet].sort();
  // Reset stale filter if selected store no longer exists
  if (activeStore !== 'all' && !storeSet.has(activeStore)) activeStore = 'all';
  bar.innerHTML = `<span style="font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase">Store:</span>
    <button class="filter-chip${activeStore==='all'?' active':''}" onclick="setStoreFilter('all',this)">All Stores</button>
    ${stores.map(s => `<button class="filter-chip${activeStore===s?' active':''}" onclick="setStoreFilter('${s.replace(/'/g,"\\'")}',this)">${esc(s)}</button>`).join('')}`;
}



function setRatingFilter(rating, btn) {
  activeRating = rating;
  document.querySelectorAll('#rating-filter-bar .filter-chip').forEach(c => c.classList.remove('active'));
  btn.classList.add('active');
  updateFilterBadge();
  renderGrid();
}

// ── Modal star rating ──
const RATING_LABELS = { 0:'Not rated', 1:'Poor — look for alternatives', 2:'Below average', 3:'Acceptable', 4:'Good', 5:'Excellent — keep buying this' };

function previewModalStars(val) {
  document.querySelectorAll('#f-star-rating .star').forEach(s => {
    const n = parseInt(s.dataset.val);
    s.classList.toggle('preview', n <= val);
    s.classList.remove('on');
  });
  const label = document.getElementById('f-rating-label');
  if (label) {
    label.textContent = RATING_LABELS[val] || '';
    label.style.color = val <= 2 ? 'var(--danger)' : val >= 4 ? 'var(--ok)' : 'var(--muted)';
  }
}

function resetModalStars() {
  document.querySelectorAll('#f-star-rating .star').forEach(s => {
    s.classList.remove('preview');
    s.classList.toggle('on', parseInt(s.dataset.val) <= currentRating);
  });
  const label = document.getElementById('f-rating-label');
  if (label) {
    label.textContent = RATING_LABELS[currentRating] || 'Not rated';
    label.style.color = !currentRating ? 'var(--muted)' : currentRating <= 2 ? 'var(--danger)' : currentRating >= 4 ? 'var(--ok)' : 'var(--muted)';
  }
}

function setRating(val) {
  currentRating = val;
  renderStars();
}

function renderStars() {
  document.querySelectorAll('#f-star-rating .star').forEach(s => {
    s.classList.remove('preview');
    s.classList.toggle('on', parseInt(s.dataset.val) <= currentRating);
  });
  const label = document.getElementById('f-rating-label');
  label.textContent = RATING_LABELS[currentRating] || 'Not rated';
  label.style.color = !currentRating ? 'var(--muted)' : currentRating <= 2 ? 'var(--danger)' : currentRating >= 4 ? 'var(--ok)' : 'var(--muted)';
}

function starsHTML(rating) {
  if (!rating) return '<span style="color:var(--muted);font-size:11px;font-family:var(--mono)">unrated</span>';
  const color = rating <= 1 ? '#e85050' : rating === 2 ? '#e8a838' : rating >= 4 ? '#4cbb8a' : '#7880a0';
  return `<span class="card-stars" style="color:${color}" title="${RATING_LABELS[rating]||''}">${'★'.repeat(rating)}<span style="color:var(--border)">${'★'.repeat(5-rating)}</span></span>`;
}

// ═══════════════════════════════════════════
//  EMAIL REMINDERS
// ═══════════════════════════════════════════
function getItemsDueWithin(days) {
  return items
    .map(item => { const s = calcStock(item); return s ? { item, daysLeft: s.daysLeft } : null; })
    .filter(x => x && x.daysLeft <= days)
    .sort((a, b) => a.daysLeft - b.daysLeft)
    .map(({ item, daysLeft }) => {
      const history  = getPriceHistory(item);
      const lastPrice = history.length ? history[history.length - 1].raw || null : null;
      return {
        name:        item.name,
        daysLeft,
        store:       item.store || '',
        url:         item.url   || '',
        rating:      item.rating || null,
        lastPrice,
        storePrices: (item.storePrices || []).filter(sp => sp.store && sp.price),
      };
    });
}

// Push the user's schedule + current items to the Worker KV
// so the hourly cron can act on it without the app being open
async function checkKVStatus() {
  const btn    = document.getElementById('kv-check-btn');
  const status = document.getElementById('kv-status-text');
  if (!WORKER_URL) { if (status) status.textContent = '✗ No backend URL configured'; return; }
  if (btn) { btn.textContent = '⏳'; btn.disabled = true; }
  try {
    const res  = await fetch(`${WORKER_URL}/debug-schedule`);
    const data = await res.json();
    // Deno KV backend: no Drive token needed — check schedule + kvSnapshot
    const hasSchedule = data.schedule && data.schedule !== '✗ missing';
    const hasItems    = data.kvSnapshot === '<svg class="icon" aria-hidden="true"><use href="#i-check"></use></svg>';
    const allOk       = hasSchedule && hasItems && !!settings.email;
    const scheduleLabel = typeof data.schedule === 'object' && data.schedule ? '✓ set' : (data.schedule || '✗ missing');
    const lines = [
      settings.email ? `email: ✓ ${settings.email}` : `email: ✗ not set`,
      `schedule: ${scheduleLabel}`,
      `storage: ${data.storage || 'Deno KV'}`,
      `items snapshot: ${data.kvSnapshot || '✗ none'}`,
      `last sent: ${data.lastSent || 'never'}`,
      data.nextSend ? `next send: ${data.nextSend}` : '',
    ].filter(Boolean);
    if (status) {
      status.style.color = allOk ? 'var(--ok)' : 'var(--warn)';
      status.innerHTML = lines.map(l => {
        const isOk  = l.includes('<svg class="icon" aria-hidden="true"><use href="#i-check"></use></svg>');
        const isBad = l.includes('✗');
        const color = isOk ? 'var(--ok)' : isBad ? 'var(--danger)' : 'var(--muted)';
        return `<span style="color:${color}">${l}</span>`;
      }).join(' · ');
    }
    if (!allOk) {
      toast('<svg class="icon" aria-hidden="true"><use href="#i-alert-triangle"></use></svg> Some server settings missing — tap Re-push');
    }
  } catch(err) {
    if (status) { status.style.color = 'var(--danger)'; status.textContent = '✗ Could not reach server'; }
  } finally {
    if (btn) { btn.textContent = 'Check'; btn.disabled = false; }
  }
}

async function repushToServer() {
  const btn    = document.getElementById('kv-repush-btn');
  const status = document.getElementById('kv-status-text');
  if (!WORKER_URL) { toast('No backend URL configured'); return; }
  if (!settings.email) { toast('Set your email address first'); return; }

  if (btn) { btn.textContent = '⏳'; btn.disabled = true; }
  try {
    await pushScheduleToWorker();
    await pushItemsToWorker();
    if (status) { status.style.color = 'var(--ok)'; status.textContent = '✓ Re-pushed — tap Check to verify'; }
    toast('Settings re-pushed to server ✓');
    setTimeout(checkKVStatus, 1000);
  } catch(err) {
    toast('Re-push failed: ' + err.message);
  } finally {
    if (btn) { btn.textContent = 'Re-push'; btn.disabled = false; }
  }
}

async function pushScheduleToWorker() {
  if (!WORKER_URL || !settings.email || !settings.emailStartDate) return;
  try {
    const urgent   = getItemsDueWithin(7);
    const upcoming = getItemsDueWithin(30).filter(i => i.daysLeft > 7);
    await fetch(`${WORKER_URL}/set-schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email:        settings.email,
        emailHash:    _kvEmailHash || null,
        verifier:     _kvVerifier,
        sessionToken: _kvSessionToken,
        startDate:    settings.emailStartDate,
        startTime:    settings.emailStartTime || '09:00',
        intervalDays: settings.emailInterval ?? 30,
        household:    activeProfile === 'default' ? null : activeProfile,
        urgent,
        upcoming,
      }),
    });
    // Trigger immediate check so schedule is active right away
    // without waiting up to an hour for the cron
    fetch(`${WORKER_URL}/check-now`, { method: 'POST' }).catch(() => {});
  } catch(e) {
    console.warn('Could not push schedule to Worker:', e.message);
  }
}

async function pushItemsToWorker() {
  if (!WORKER_URL || !settings.email) return;
  try {
    const urgent   = getItemsDueWithin(7);
    const upcoming = getItemsDueWithin(30).filter(i => i.daysLeft > 7);
    if (!urgent.length && !upcoming.length) return;
    await fetch(`${WORKER_URL}/set-schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email:        settings.email,
        emailHash:    _kvEmailHash || null,
        verifier:     _kvVerifier,
        sessionToken: _kvSessionToken,
        startDate:    settings.emailStartDate,
        startTime:    settings.emailStartTime || '09:00',
        intervalDays: settings.emailInterval ?? 30,
        household:    activeProfile === 'default' ? null : activeProfile,
        urgent,
        upcoming,
      }),
    });
  } catch(e) {}
}

function updateLastSentUI() {
  const row      = document.getElementById('last-sent-row');
  const textEl   = document.getElementById('last-sent-text');
  const interval = settings.emailInterval ?? 30;
  if (!row || !textEl) return;

  if (!settings.email || interval === 0) { row.style.display = 'none'; return; }
  row.style.display = 'flex';

  const lastSent  = settings.emailLastSent;
  const startDate = settings.emailStartDate;
  const startTime = settings.emailStartTime || '09:00';
  const now       = new Date();

  if (!lastSent) {
    if (startDate) {
      const start     = new Date(`${startDate}T${startTime}:00`);
      const minsUntil = Math.ceil((start - now) / 60000);
      if (start > now) {
        const timeStr = minsUntil < 120
          ? `in ${minsUntil} minute${minsUntil !== 1 ? 's' : ''}`
          : minsUntil < 1440
            ? `in ${Math.ceil(minsUntil/60)} hour${Math.ceil(minsUntil/60) !== 1 ? 's' : ''}`
            : `on ${fmtDate(startDate)} at ${startTime}`;
        textEl.textContent = `☁️ First reminder scheduled ${timeStr} — Deno will send automatically`;
      } else {
        textEl.textContent = `⏳ First reminder is due — Deno cron will send on next hourly check`;
      }
    } else {
      textEl.textContent = 'Set a start date above to schedule automatic reminders';
    }
    return;
  }

  const next      = new Date(new Date(lastSent).getTime() + interval * 86400000);
  const daysUntil = Math.ceil((next - now) / 86400000);
  textEl.textContent = `Last sent ${fmtDate(lastSent)} · Next ${daysUntil > 0
    ? `in ${daysUntil} day${daysUntil !== 1 ? 's' : ''}`
    : 'is overdue — Deno will send on next hourly check'}`;
}

async function resetLastSent() {
  settings.emailLastSent = null;
  await _saveSettings();
  if (WORKER_URL && settings.email) {
    fetch(`${WORKER_URL}/reset-schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: settings.email, emailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken }),
    }).catch(() => {});
  }
  updateLastSentUI();
  toast('Schedule reset — next send will be at your start date/time');
}

async function handleUnsubscribe() {
  // Switch to settings tab so the user can see the email card
  const settingsTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Settings'));
  if (settingsTab) showView('settings', settingsTab);

  // Show a confirmation prompt
  setTimeout(async () => {
    if (!confirm('Stop scheduled email reminders from STOCKROOM?\n\nYou can re-enable them any time in Settings → Email Reminders.')) return;

    settings.emailStartDate = null;
    settings.emailStartTime = null;
    settings.emailLastSent  = null;
    settings.emailInterval  = 0;
    await _saveSettings();

    // Clear schedule on Worker too
    if (WORKER_URL) {
      fetch(`${WORKER_URL}/unsubscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: settings.email, emailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken }),
      }).catch(() => {});
    }

    scheduleRender('grid', 'dashboard', 'settings-ui');
    toast('Email reminders disabled ✓');

    // Scroll to email settings card so they can see it
    setTimeout(() => {
      const emailCard = document.querySelector('#view-settings .settings-card h3');
      let card = null;
      document.querySelectorAll('#view-settings .settings-card h3').forEach(h => {
        if (h.textContent.includes('Email')) card = h.closest('.settings-card');
      });
      if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 300);
  }, 400);
}

async function checkScheduledEmail() {
  // Client-side: just update the UI — the Deno cron is the single source of truth for sending.
  // Removed client-side send trigger to prevent double-sends if app opens after scheduled time.
  updateLastSentUI();
}

async function sendReminderEmail(manual = true) {
  const email = settings.email || document.getElementById('setting-email')?.value.trim();
  if (!email) { if (manual) toast('Enter your email address in Settings first'); return; }

  const urgent   = getItemsDueWithin(7);
  const upcoming = getItemsDueWithin(30).filter(i => i.daysLeft > 7);

  // Build S&S savings summary for email
  const snsAll      = analyseSnS();
  const snsEligible = snsAll.filter(r => r.status === 'eligible');
  const snsActive   = snsAll.filter(r => r.status === 'active');
  const snsSaving   = [...snsEligible, ...snsActive].reduce((s,r) => s + (r.annualSaving||0), 0);
  const snsPayload  = (snsEligible.length + snsActive.length) >= SNS_MIN_ITEMS
    ? { eligible: snsEligible.map(r => ({ name: r.item.name, annualSaving: r.annualSaving, snsLink: getSnSLink(r.item) })), totalSaving: snsSaving }
    : null;

  if (!urgent.length && !upcoming.length) {
    if (manual) toast('Nothing running out in the next 30 days — nothing to send!');
    return;
  }

  if (manual) {
    const btn = document.getElementById('send-reminder-btn');
    const statusEl = document.getElementById('email-status');
    if (btn) { btn.textContent = '⏳ Sending…'; btn.disabled = true; }
    if (statusEl) statusEl.style.display = 'none';
  }

  try {
    const res = await fetch(`${WORKER_URL}/send-reminder`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, emailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, urgent, upcoming, sns: snsPayload, manual }),
    });
    const data = await res.json();
    if (res.ok) {
      // Only update last-sent timestamp for scheduled sends, not manual ones
      if (!manual) {
        settings.emailLastSent = new Date().toISOString();
        await _saveSettings();
        updateLastSentUI();
      }
      if (manual) {
        const total = urgent.length + upcoming.length;
        const statusEl = document.getElementById('email-status');
        if (statusEl) {
          statusEl.textContent   = `✓ Reminder sent to ${email} (${total} item${total !== 1 ? 's' : ''})`;
          statusEl.style.color   = 'var(--ok)';
          statusEl.style.display = 'block';
        }
        toast('Reminder email sent ✓');
      }
    } else { throw new Error(data.error || 'Unknown error'); }
  } catch(err) {
    if (manual) {
      const statusEl = document.getElementById('email-status');
      if (statusEl) {
        statusEl.textContent   = `✗ Failed: ${err.message}`;
        statusEl.style.color   = 'var(--danger)';
        statusEl.style.display = 'block';
      }
    }
    console.error('Email reminder error:', err);
  } finally {
    if (manual) {
      const btn = document.getElementById('send-reminder-btn');
      if (btn) { btn.innerHTML = '<svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-mail"></use></svg> Send Now'; btn.disabled = false; }
    }
  }
}

// ═══════════════════════════════════════════
//  ITEM TEMPLATES
// ═══════════════════════════════════════════
const ITEM_TEMPLATES = [
  // Kitchen
  { name:'Coffee Beans',            category:'Kitchen',      cadence:'monthly', qty:1,  months:1,   emoji:'☕' },
  { name:'Ground Coffee',           category:'Kitchen',      cadence:'monthly', qty:1,  months:0.5, emoji:'☕' },
  { name:'Coffee Pods (48-pack)',   category:'Kitchen',      cadence:'monthly', qty:48, months:1,   emoji:'☕' },
  { name:'Tea Bags (240-pack)',     category:'Kitchen',      cadence:'bulk',    qty:1,  months:3,   emoji:'🍵' },
  { name:'Kitchen Roll',            category:'Kitchen',      cadence:'monthly', qty:1,  months:1,   emoji:'🧻' },
  { name:'Olive Oil',               category:'Kitchen',      cadence:'monthly', qty:1,  months:2,   emoji:'🫒' },
  { name:'Washing Up Liquid',       category:'Kitchen',      cadence:'monthly', qty:1,  months:1,   emoji:'🍽️' },
  { name:'Foil / Cling Film',       category:'Kitchen',      cadence:'bulk',    qty:1,  months:3,   emoji:'📦' },
  { name:'Dishwasher Salt',         category:'Kitchen',      cadence:'bulk',    qty:1,  months:2,   emoji:'🧂' },
  { name:'Dishwasher Rinse Aid',    category:'Kitchen',      cadence:'bulk',    qty:1,  months:3,   emoji:'✨' },
  { name:'Dishwasher Tablets',      category:'Kitchen',      cadence:'bulk',    qty:60, months:2,   emoji:'✨' },
  { name:'Bin Bags',                category:'Kitchen',      cadence:'bulk',    qty:1,  months:2,   emoji:'🗑️' },
  { name:'Zip Lock Bags',           category:'Kitchen',      cadence:'bulk',    qty:1,  months:4,   emoji:'📦' },
  // Bathroom
  { name:'Shampoo',                 category:'Bathroom',     cadence:'monthly', qty:1,  months:2,   emoji:'🧴' },
  { name:'Conditioner',             category:'Bathroom',     cadence:'monthly', qty:1,  months:2,   emoji:'🧴' },
  { name:'Shower Gel',              category:'Bathroom',     cadence:'monthly', qty:1,  months:1,   emoji:'🚿' },
  { name:'Toothpaste',              category:'Bathroom',     cadence:'monthly', qty:1,  months:2,   emoji:'🦷' },
  { name:'Toothbrush',              category:'Bathroom',     cadence:'bulk',    qty:1,  months:3,   emoji:'🪥', replacementInterval:3, replacementUnit:'months' },
  { name:'Toothbrush Heads',        category:'Bathroom',     cadence:'bulk',    qty:4,  months:3,   emoji:'🪥', replacementInterval:3, replacementUnit:'months' },
  { name:'Floss',                   category:'Bathroom',     cadence:'monthly', qty:1,  months:2,   emoji:'🦷' },
  { name:'Mouthwash',               category:'Bathroom',     cadence:'monthly', qty:1,  months:2,   emoji:'🫧' },
  { name:'Hand Soap',               category:'Bathroom',     cadence:'monthly', qty:1,  months:1,   emoji:'🧼' },
  { name:'Deodorant',               category:'Bathroom',     cadence:'monthly', qty:1,  months:1,   emoji:'💨' },
  { name:'Razor Blades (8-pack)',   category:'Bathroom',     cadence:'bulk',    qty:8,  months:2,   emoji:'🪒' },
  { name:'Toilet Roll (9-pack)',    category:'Bathroom',     cadence:'bulk',    qty:9,  months:1,   emoji:'🧻' },
  { name:'Cotton Buds',             category:'Bathroom',     cadence:'bulk',    qty:1,  months:3,   emoji:'🌿' },
  { name:'Face Moisturiser',        category:'Bathroom',     cadence:'monthly', qty:1,  months:2,   emoji:'🧴' },
  { name:'Sun Cream SPF50',         category:'Bathroom',     cadence:'bulk',    qty:1,  months:3,   emoji:'☀️' },
  // Cleaning
  { name:'Washing Powder',          category:'Cleaning',     cadence:'monthly', qty:1,  months:2,   emoji:'🧺' },
  { name:'Washing Liquid',          category:'Cleaning',     cadence:'monthly', qty:1,  months:2,   emoji:'🧺' },
  { name:'Fabric Softener',         category:'Cleaning',     cadence:'monthly', qty:1,  months:2,   emoji:'🌸' },
  { name:'Laundry Capsules (40pk)', category:'Cleaning',     cadence:'bulk',    qty:40, months:2,   emoji:'🧺' },
  { name:'Washing Machine Cleaner', category:'Cleaning',     cadence:'bulk',    qty:1,  months:3,   emoji:'🌀', replacementInterval:3, replacementUnit:'months' },
  { name:'Surface Spray',           category:'Cleaning',     cadence:'monthly', qty:1,  months:1,   emoji:'🧹' },
  { name:'Bleach',                  category:'Cleaning',     cadence:'monthly', qty:1,  months:2,   emoji:'🫧' },
  { name:'Toilet Cleaner',          category:'Cleaning',     cadence:'monthly', qty:1,  months:2,   emoji:'🚽' },
  { name:'Sponges (multi-pack)',    category:'Cleaning',     cadence:'monthly', qty:1,  months:1,   emoji:'🧽' },
  { name:'Microfibre Cloths',       category:'Cleaning',     cadence:'bulk',    qty:1,  months:3,   emoji:'🧽' },
  { name:'Mop Heads',               category:'Cleaning',     cadence:'bulk',    qty:1,  months:6,   emoji:'🧹', replacementInterval:6, replacementUnit:'months' },
  { name:'Vacuum Bags',             category:'Cleaning',     cadence:'bulk',    qty:1,  months:3,   emoji:'🌀' },
  { name:'Air Freshener',           category:'Cleaning',     cadence:'monthly', qty:1,  months:1,   emoji:'🌸' },
  // Food & Drink
  { name:'Milk (4-pint)',           category:'Food & Drink', cadence:'monthly', qty:4,  months:0.5, emoji:'🥛' },
  { name:'Pasta (500g)',            category:'Food & Drink', cadence:'bulk',    qty:4,  months:1,   emoji:'🍝' },
  { name:'Rice (1kg)',              category:'Food & Drink', cadence:'bulk',    qty:2,  months:2,   emoji:'🍚' },
  { name:'Tinned Tomatoes',         category:'Food & Drink', cadence:'bulk',    qty:6,  months:2,   emoji:'🍅' },
  { name:'Tinned Tuna',             category:'Food & Drink', cadence:'bulk',    qty:6,  months:2,   emoji:'🐟' },
  { name:'Tinned Beans',            category:'Food & Drink', cadence:'bulk',    qty:6,  months:2,   emoji:'🫘' },
  { name:'Cereal',                  category:'Food & Drink', cadence:'monthly', qty:1,  months:1,   emoji:'🥣' },
  { name:'Bread',                   category:'Food & Drink', cadence:'monthly', qty:2,  months:0.5, emoji:'🍞' },
  { name:'Cooking Oil',             category:'Food & Drink', cadence:'bulk',    qty:1,  months:2,   emoji:'🫙' },
  { name:'Salt',                    category:'Food & Drink', cadence:'bulk',    qty:1,  months:6,   emoji:'🧂' },
  { name:'Sugar',                   category:'Food & Drink', cadence:'bulk',    qty:1,  months:3,   emoji:'🍬' },
  { name:'Dog Food',                category:'Food & Drink', cadence:'bulk',    qty:1,  months:1,   emoji:'🐕' },
  { name:'Cat Food',                category:'Food & Drink', cadence:'bulk',    qty:1,  months:1,   emoji:'🐈' },
  // Health
  { name:'Paracetamol (32-pack)',   category:'Health',       cadence:'bulk',    qty:1,  months:6,   emoji:'💊' },
  { name:'Ibuprofen (32-pack)',     category:'Health',       cadence:'bulk',    qty:1,  months:6,   emoji:'💊' },
  { name:'Multivitamins',           category:'Health',       cadence:'monthly', qty:1,  months:1,   emoji:'💊' },
  { name:'Vitamin D (90 caps)',     category:'Health',       cadence:'bulk',    qty:1,  months:3,   emoji:'☀️' },
  { name:'Vitamin C',               category:'Health',       cadence:'monthly', qty:1,  months:1,   emoji:'🍊' },
  { name:'Omega 3 Fish Oil',        category:'Health',       cadence:'monthly', qty:1,  months:1,   emoji:'🐟' },
  { name:'Plasters (assorted)',     category:'Health',       cadence:'bulk',    qty:1,  months:6,   emoji:'🩹' },
  { name:'Antiseptic Cream',        category:'Health',       cadence:'bulk',    qty:1,  months:12,  emoji:'🩺' },
  { name:'Indigestion Tablets',     category:'Health',       cadence:'bulk',    qty:1,  months:6,   emoji:'💊' },
  { name:'Antihistamine',           category:'Health',       cadence:'bulk',    qty:1,  months:6,   emoji:'💊' },
  // Garden
  { name:'Lawn Feed',               category:'Garden',       cadence:'bulk',    qty:1,  months:3,   emoji:'🌱' },
  { name:'Compost (60L)',           category:'Garden',       cadence:'bulk',    qty:1,  months:3,   emoji:'🌿' },
  { name:'Slug Pellets',            category:'Garden',       cadence:'bulk',    qty:1,  months:3,   emoji:'🐌' },
  { name:'Bird Seed',               category:'Garden',       cadence:'monthly', qty:1,  months:1,   emoji:'🐦' },
  { name:'Plant Feed',              category:'Garden',       cadence:'monthly', qty:1,  months:2,   emoji:'🌻' },
  { name:'BBQ Charcoal',            category:'Garden',       cadence:'bulk',    qty:1,  months:2,   emoji:'🔥' },
  { name:'BBQ Gas Cylinder',        category:'Garden',       cadence:'bulk',    qty:1,  months:3,   emoji:'🔥' },
  { name:'Log Burner Fuel',         category:'Garden',       cadence:'bulk',    qty:1,  months:1,   emoji:'🪵' },
  // Other
  { name:'AA Batteries (16pk)',     category:'Other',        cadence:'bulk',    qty:16, months:6,   emoji:'🔋' },
  { name:'AAA Batteries (16pk)',    category:'Other',        cadence:'bulk',    qty:16, months:6,   emoji:'🔋' },
  { name:'Printer Ink',             category:'Other',        cadence:'bulk',    qty:1,  months:3,   emoji:'🖨️' },
  { name:'Printer Paper (500pk)',   category:'Other',        cadence:'bulk',    qty:1,  months:3,   emoji:'📄' },
  { name:'Postage Stamps (12pk)',   category:'Other',        cadence:'bulk',    qty:12, months:6,   emoji:'✉️' },
  { name:'Light Bulbs (4pk)',       category:'Other',        cadence:'bulk',    qty:4,  months:12,  emoji:'💡' },
  { name:'Smoke Alarm Battery',     category:'Other',        cadence:'bulk',    qty:1,  months:12,  emoji:'🔋', replacementInterval:12, replacementUnit:'months' },
  { name:'Water Filter Cartridge',  category:'Other',        cadence:'bulk',    qty:1,  months:2,   emoji:'💧', replacementInterval:2, replacementUnit:'months' },
  { name:'Candles',                 category:'Other',        cadence:'bulk',    qty:1,  months:3,   emoji:'🕯️' },
  { name:'Tin Foil (30m)',          category:'Other',        cadence:'bulk',    qty:1,  months:3,   emoji:'🫙' },
];

let activeTemplateCat = 'all';

function filterTemplates(cat, btn) {
  activeTemplateCat = cat;
  document.querySelectorAll('#template-cat-filter button').forEach(b => b.style.background = '');
  if (btn) btn.style.background = 'var(--surface2)';
  renderTemplateGrid();
}

function renderTemplateGrid() {
  const grid = document.getElementById('templates-grid');
  if (!grid) return;
  const filtered = activeTemplateCat === 'all'
    ? ITEM_TEMPLATES
    : ITEM_TEMPLATES.filter(t => t.category === activeTemplateCat);
  grid.innerHTML = filtered.map((t, i) => {
    const realIdx = ITEM_TEMPLATES.indexOf(t);
    return `<button onclick="applyTemplate(${realIdx})"
      style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;cursor:pointer;text-align:left;transition:border-color 0.15s;width:100%"
      onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
      <span style="font-size:22px;flex-shrink:0">${t.emoji}</span>
      <div style="min-width:0">
        <div style="font-size:12px;font-weight:600;color:var(--text);line-height:1.3">${esc(t.name)}</div>
        <div style="font-size:10px;color:var(--muted);margin-top:2px">${t.months}mo supply${t.replacementInterval ? ' · <svg class="icon" aria-hidden="true"><use href="#i-bell"></use></svg> reminder' : ''}</div>
      </div>
    </button>`;
  }).join('');
}

function openTemplatesModal() {
  activeTemplateCat = 'all';
  // Reset category filter highlight
  document.querySelectorAll('#template-cat-filter button').forEach((b, i) => {
    b.style.background = i === 0 ? 'var(--surface2)' : '';
  });
  renderTemplateGrid();
  openModal('templates-modal');
}

function applyTemplate(idx) {
  const t = ITEM_TEMPLATES[idx];
  closeModal('templates-modal');
  document.getElementById('f-name').value     = t.name;
  document.getElementById('f-category').value = t.category;
  document.getElementById('f-cadence').value  = t.cadence;
  document.getElementById('f-qty').value      = t.qty;
  document.getElementById('f-months').value   = t.months;
  // Pre-fill replacement reminder if template has one
  const replIntervalEl = document.getElementById('f-replace-interval');
  const replUnitEl     = document.getElementById('f-replace-unit');
  if (replIntervalEl) replIntervalEl.value = t.replacementInterval || '';
  if (replUnitEl && t.replacementUnit) replUnitEl.value = t.replacementUnit;
  document.getElementById('f-name').focus();
  toast(`Template: ${t.name}`);
}

// ═══════════════════════════════════════════
//  BARCODE SCANNER — loaded lazily in scanner.js
// ═══════════════════════════════════════════


function getQuickAddItems() {
  return items.filter(i => i.quickAdded);
}

function renderPendingDeliveries() {
  const pending = items.filter(i => i.logs?.some(l => l.pendingDelivery));

  // ── Stock view section ──────────────────────────────────
  const section = document.getElementById('pending-deliveries-section');
  const grid    = document.getElementById('pending-deliveries-grid');
  const badge   = document.getElementById('pending-deliveries-badge');
  if (section && grid) {
    if (!pending.length) {
      section.style.display = 'none';
    } else {
      section.style.display = 'block';
      if (badge) badge.textContent = pending.length;
      grid.innerHTML = pending.map(item => {
        const pendingLog = [...item.logs].reverse().find(l => l.pendingDelivery);
        const orderedDate = pendingLog?.date ? fmtDate(pendingLog.date) : '—';
        const qty = pendingLog?.qty || item.qty || 1;
        const price = pendingLog?.price || '';
        return `<div class="incomplete-card" style="border-color:rgba(91,141,238,0.4)">
          <div style="flex:1;min-width:0">
            <div class="inc-name">${esc(item.name)}</div>
            <div class="inc-meta">Ordered ${orderedDate} · qty ${qty}${price ? ' · ' + esc(price) : ''}</div>
          </div>
          <button class="btn btn-sm" style="background:rgba(76,187,138,0.15);color:var(--ok);border:1px solid rgba(76,187,138,0.3);white-space:nowrap"
            onclick="openDeliveredModal('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-package-check"></use></svg> Delivered</button>
        </div>`;
      }).join('');
    }
  }

  // ── Report view section ─────────────────────────────────
  const reportEl = document.getElementById('report-pending-deliveries');
  if (reportEl) {
    if (!pending.length) {
      reportEl.style.display = 'none';
    } else {
      reportEl.style.display = 'block';
      reportEl.innerHTML = `
        <div style="background:rgba(91,141,238,0.08);border:1px solid rgba(91,141,238,0.25);border-radius:12px;padding:18px 20px">
          <div style="font-size:13px;font-weight:700;color:#5b8dee;margin-bottom:12px"><svg class="icon" aria-hidden="true"><use href="#i-truck"></use></svg> Pending Deliveries — ${pending.length} item${pending.length!==1?'s':''}</div>
          ${pending.map(item => {
            const pendingLog = [...item.logs].reverse().find(l => l.pendingDelivery);
            const orderedDate = pendingLog?.date ? fmtDate(pendingLog.date) : '—';
            const qty   = pendingLog?.qty   || item.qty || 1;
            const price = pendingLog?.price || '';
            return `<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid rgba(91,141,238,0.15)">
              <div>
                <div style="font-size:14px;font-weight:600">${esc(item.name)}</div>
                <div style="font-size:12px;color:var(--muted);font-family:var(--mono);margin-top:2px">Ordered ${orderedDate} · qty ${qty}${price ? ' · ' + esc(price) : ''}</div>
              </div>
              <button class="btn btn-sm" style="background:rgba(76,187,138,0.15);color:var(--ok);border:1px solid rgba(76,187,138,0.3);white-space:nowrap"
                onclick="openDeliveredModal('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-package-check"></use></svg> Delivered</button>
            </div>`;
          }).join('')}
        </div>`;
    }
  }
}

function renderIncompleteSection() {
  const incomplete = getQuickAddItems();
  const section    = document.getElementById('incomplete-section');
  const grid       = document.getElementById('incomplete-grid');
  const badge      = document.getElementById('incomplete-count-badge');
  const banner     = document.getElementById('incomplete-banner');
  const bannerText = document.getElementById('incomplete-banner-text');

  if (!incomplete.length) {
    if (section) section.style.display = 'none';
    if (banner)  banner.style.display  = 'none';
    return;
  }

  // Show banner
  if (banner) {
    banner.style.display = 'flex';
    if (bannerText) bannerText.textContent = `${incomplete.length} item${incomplete.length !== 1 ? 's' : ''} need setting up`;
  }

  // Show section on stock view
  if (section) {
    section.style.display = 'block';
    if (badge) badge.textContent = incomplete.length;
  }

  if (grid) {
    grid.innerHTML = incomplete.map(item => `
      <div class="incomplete-card">
        <div>
          <div class="inc-name">${esc(item.name)}</div>
          <div class="inc-meta"><svg class="icon" aria-hidden="true"><use href="#i-zap"></use></svg> Quick added · no details yet</div>
        </div>
        <div style="display:flex;gap:6px;flex-shrink:0">
          <button class="btn-icon" title="Complete setup" onclick="openEditModal('${item.id}');enableItemEdit()"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
          <button class="btn-icon" title="Remove" onclick="deleteItem('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg></button>
        </div>
      </div>`).join('');
  }
}

function scrollToIncomplete() {
  // Switch to stock tab if not there
  const stockTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Stock'));
  if (stockTab && !stockTab.classList.contains('active')) showView('stock', stockTab);
  // Scroll to section
  setTimeout(() => {
    const section = document.getElementById('incomplete-section');
    if (section) section.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, 100);
}

// When an item is saved via the full edit form, clear quickAdded flag
async function clearQuickAddedFlag(id) {
  const item = items.find(i => i.id === id);
  if (item) item.quickAdded = false;
}

// ═══════════════════════════════════════════
//  REMINDERS SYSTEM
// ═══════════════════════════════════════════

const REMINDER_SUGGESTIONS = [
  { name: 'Toothbrush Head',       interval: 3,  unit: 'months' },
  { name: 'Water Filter Cartridge',interval: 2,  unit: 'months' },
  { name: 'Shower Head',           interval: 6,  unit: 'months' },
  { name: 'Air Filter',            interval: 3,  unit: 'months' },
  { name: 'Smoke Alarm Battery',   interval: 12, unit: 'months' },
  { name: 'Fridge Filter',         interval: 6,  unit: 'months' },
  { name: 'Razor Blade',           interval: 2,  unit: 'weeks'  },
  { name: 'Dishwasher Filter',     interval: 1,  unit: 'months' },
  { name: 'Car Air Filter',        interval: 12, unit: 'months' },
  { name: 'Boiler Service',        interval: 12, unit: 'months' },
  { name: 'Vacuum Filter',         interval: 3,  unit: 'months' },
  { name: 'Contact Lenses',        interval: 30, unit: 'days'   },
];

let reminders = []; // separate from items[]
let editingReminderId = null;
let loggingReminderId = null;

// ── Persistence ───────────────────────────
async function loadReminders() {
  const stored = await dbGet('reminders', 'reminders');
  if (stored) { reminders = stored; return; }
  // Migration
  const raw = localStorage.getItem('stockroom_reminders');
  if (raw) {
    try {
      reminders = JSON.parse(raw) || [];
      await dbPut('reminders', 'reminders', reminders);
      localStorage.removeItem('stockroom_reminders');
    } catch(e) { reminders = []; }
  }
}

async function saveReminders() {
  await dbPut('reminders', 'reminders', reminders);
  if (activeProfile) await saveCurrentProfile();
}

async function loadNotes() {
  const stored = await dbGet('items', 'notes');
  if (stored && Array.isArray(stored)) notes = stored;
}

async function saveNotes() {
  await dbPut('items', 'notes', notes);
}

// ── Calculations ──────────────────────────
function getReminderIntervalDays(reminder) {
  const n = reminder.interval || 1;
  if (reminder.unit === 'days')   return n;
  if (reminder.unit === 'weeks')  return n * 7;
  if (reminder.unit === 'months') return n * 30.5;
  return n * 30.5;
}

function getReminderDueDate(reminder) {
  if (!reminder.lastReplaced) return null;
  const lastMs   = new Date(reminder.lastReplaced + 'T12:00:00').getTime();
  const intervalMs = getReminderIntervalDays(reminder) * 86400000;
  return new Date(lastMs + intervalMs);
}

function getReminderDaysUntil(reminder) {
  const due = getReminderDueDate(reminder);
  if (!due) return null;
  return Math.round((due.getTime() - Date.now()) / 86400000);
}

function getReminderStatus(reminder) {
  const days = getReminderDaysUntil(reminder);
  if (days === null)  return 'unknown';
  if (days < 0)       return 'overdue';
  if (days <= 30)     return 'soon';
  return 'upcoming';
}

// ── Render ────────────────────────────────
async function renderReminders() {
  await loadReminders();

  // Also collect reminders embedded in items — supports both old single and new array format
  const allReminders = [
    ...reminders,
    ...items.flatMap(i => {
      const fallbackDate = i.startedUsing || i.logs?.filter(l => !l.pendingDelivery)[0]?.date || null;
      if (i.replacementReminders?.length) {
        // New array format — one entry per named reminder
        return i.replacementReminders.map(r => ({
          id:           `item_${i.id}_${r.id}`,
          name:         r.name ? `${i.name} — ${r.name}` : i.name,
          itemName:     i.name,
          reminderName: r.name || '',
          interval:     r.interval,
          unit:         r.unit,
          lastReplaced: r.lastReplaced || fallbackDate || null,
          lastReplacedIsFallback: !r.lastReplaced && !!fallbackDate,
          notes:        i.notes || '',
          fromItem:     i.id,
          fromReminder: r.id,
        }));
      } else if (i.replacementInterval && i.replacementUnit) {
        // Legacy single-reminder format
        return [{
          id:           `item_${i.id}`,
          name:         i.name,
          itemName:     i.name,
          reminderName: '',
          interval:     i.replacementInterval,
          unit:         i.replacementUnit,
          lastReplaced: i.lastReplaced || fallbackDate || null,
          lastReplacedIsFallback: !i.lastReplaced && !!fallbackDate,
          notes:        i.notes || '',
          fromItem:     i.id,
          fromReminder: null,
        }];
      }
      return [];
    }),
  ];

  const overdue  = allReminders.filter(r => getReminderStatus(r) === 'overdue');
  const soon     = allReminders.filter(r => getReminderStatus(r) === 'soon');
  const upcoming = allReminders.filter(r => getReminderStatus(r) === 'upcoming');
  const unknown  = allReminders.filter(r => getReminderStatus(r) === 'unknown');

  const empty = document.getElementById('reminders-empty');
  if (allReminders.length === 0) {
    if (empty) empty.style.display = 'block';
    ['overdue-section','soon-section','upcoming-section'].forEach(s => {
      const el = document.getElementById('reminders-' + s);
      if (el) el.style.display = 'none';
    });
    const nudge = document.getElementById('reminders-nudge');
    if (nudge) nudge.style.display = 'none';
    return;
  }
  if (empty) empty.style.display = 'none';

  // Nudge bar
  const nudge     = document.getElementById('reminders-nudge');
  const nudgeText = document.getElementById('reminders-nudge-text');
  if (nudge && overdue.length > 0) {
    nudge.style.display = 'flex';
    nudgeText.innerHTML = `<svg class="icon icon-sm" aria-hidden="true"><use href="#i-alert-triangle"></use></svg> ${overdue.length} replacement${overdue.length !== 1 ? 's' : ''} overdue`;
  } else if (nudge) {
    nudge.style.display = 'none';
  }

  // Render each section
  const renderSection = (list, containerId, sectionId, countId) => {
    const section   = document.getElementById(sectionId);
    const container = document.getElementById(containerId);
    const countEl   = document.getElementById(countId);
    if (!section || !container) return;
    if (!list.length) { section.style.display = 'none'; return; }
    section.style.display = 'block';
    if (countEl) countEl.textContent = list.length;
    container.innerHTML = list.map(r => reminderCardHTML(r)).join('');
  };

  // Sort overdue by most overdue first
  overdue.sort((a,b)  => (getReminderDaysUntil(a)||0) - (getReminderDaysUntil(b)||0));
  soon.sort((a,b)     => (getReminderDaysUntil(a)||0) - (getReminderDaysUntil(b)||0));
  upcoming.sort((a,b) => (getReminderDaysUntil(a)||0) - (getReminderDaysUntil(b)||0));

  renderSection([...overdue], 'reminders-overdue-list',    'reminders-overdue-section', 'overdue-count');
  renderSection([...soon],    'reminders-soon-list',       'reminders-soon-section',    'soon-count');
  renderSection([...upcoming, ...unknown], 'reminders-upcoming-list', 'reminders-upcoming-section', null);

  // Also update stock view badge if reminders are overdue
  updateRemindersBadge(overdue.length + soon.length);
}

function reminderCardHTML(r) {
  const days     = getReminderDaysUntil(r);
  const status   = getReminderStatus(r);
  const dueDate  = getReminderDueDate(r);
  const isFromItem = r.fromItem;

  const statusColor = status === 'overdue' ? 'var(--danger)' : status === 'soon' ? 'var(--warn)' : 'var(--muted)';
  const borderColor = status === 'overdue' ? 'rgba(232,80,80,0.3)' : status === 'soon' ? 'rgba(232,168,56,0.3)' : 'var(--border)';

  let timeLabel = '';
  if (days === null) {
    timeLabel = 'No replacement date recorded';
  } else if (days < 0) {
    timeLabel = `Overdue by ${Math.abs(days)} day${Math.abs(days) !== 1 ? 's' : ''}`;
  } else if (days === 0) {
    timeLabel = 'Due today';
  } else {
    timeLabel = `Due in ${days} day${days !== 1 ? 's' : ''}`;
  }

  const intervalLabel = `Every ${r.interval} ${r.unit}`;
  const lastLabel = r.lastReplaced
    ? (r.lastReplacedIsFallback ? `First used ${timeAgo(r.lastReplaced)}` : `Last replaced ${timeAgo(r.lastReplaced)}`)
    : 'Never replaced';
  const nextLabel = dueDate ? `Next: ${fmtDate(dueDate.toISOString().slice(0,10))}` : 'Set a date to track';

  return `<div style="background:var(--surface);border:1px solid ${borderColor};border-radius:12px;padding:14px 16px;margin-bottom:10px;display:flex;gap:12px;align-items:flex-start;cursor:pointer;transition:border-color 0.15s,box-shadow 0.15s" onclick="openReminderTimeline('${r.id}')" title="Tap to view timeline">
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px">
        <span style="font-size:15px;font-weight:700;color:var(--text)">${esc(r.itemName || r.name)}</span>
        ${r.reminderName ? `<span style="font-size:12px;color:var(--muted)">${esc(r.reminderName)}</span>` : ''}
        ${isFromItem ? `<span style="font-size:10px;color:var(--muted);font-family:var(--mono);padding:1px 6px;border:1px solid var(--border);border-radius:99px">linked</span>` : ''}
        <span style="font-size:10px;color:var(--muted);margin-left:auto;opacity:0.5"><svg class="icon" aria-hidden="true"><use href="#i-bar-chart-2"></use></svg> Timeline</span>
      </div>
      <div style="font-size:12px;font-weight:700;color:${statusColor};margin-bottom:4px">${timeLabel}</div>
      <div style="font-size:11px;color:var(--muted);font-family:var(--mono);line-height:1.8">
        ${intervalLabel} · ${lastLabel}<br>${nextLabel}
      </div>
      ${r.notes ? `<div style="font-size:12px;color:var(--muted);font-style:italic;margin-top:6px"><svg class="icon" aria-hidden="true"><use href="#i-message-square"></use></svg> ${esc(r.notes)}</div>` : ''}
      <div style="display:flex;gap:6px;margin-top:10px;flex-wrap:wrap" onclick="event.stopPropagation()">
        <button class="btn btn-primary btn-sm" onclick="openLogReplacementModal('${r.id}')"><svg class="icon" aria-hidden="true"><use href="#i-check-circle-2"></use></svg> Mark replaced</button>
        ${!isFromItem
          ? `<button class="btn btn-ghost btn-sm" onclick="openEditReminderModal('${r.id}')"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg> Edit</button>`
          : `<button class="btn btn-ghost btn-sm" onclick="openEditModal('${r.fromItem}')"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg> Edit item</button>`
        }
        <button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="deleteReminder('${r.id}')"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg> Delete</button>
      </div>
    </div>
  </div>`;
}

// ── Resolve a reminder ID to its full data object ──────────
// Handles: standalone reminder IDs, item_${itemId} (legacy), item_${itemId}_${remId} (new)
function _resolveReminderId(id) {
  // Standalone reminder
  const standalone = reminders.find(r => r.id === id);
  if (standalone) return { r: standalone, item: null, remEntry: null };

  if (!id.startsWith('item_')) return null;

  const rest = id.slice('item_'.length); // either "itemId" or "itemId_remId"
  const underIdx = rest.indexOf('_');

  if (underIdx === -1) {
    // Legacy: item_${itemId}
    const item = items.find(i => i.id === rest);
    if (!item) return null;
    const fallbackDate = item.startedUsing || item.logs?.filter(l => !l.pendingDelivery)[0]?.date || null;
    return {
      r: {
        id, name: item.name, itemName: item.name, reminderName: '',
        interval: item.replacementInterval, unit: item.replacementUnit,
        lastReplaced: item.lastReplaced || fallbackDate || null,
        lastReplacedIsFallback: !item.lastReplaced && !!fallbackDate,
        notes: item.notes || '', fromItem: item.id, fromReminder: null,
      },
      item, remEntry: null,
    };
  } else {
    // New: item_${itemId}_${remId}
    const itemId = rest.slice(0, underIdx);
    const remId  = rest.slice(underIdx + 1);
    const item   = items.find(i => i.id === itemId);
    if (!item) return null;
    const remEntry = item.replacementReminders?.find(r => r.id === remId);
    if (!remEntry) return null;
    const fallbackDate = item.startedUsing || item.logs?.filter(l => !l.pendingDelivery)[0]?.date || null;
    return {
      r: {
        id, name: remEntry.name ? `${item.name} — ${remEntry.name}` : item.name,
        itemName: item.name, reminderName: remEntry.name || '',
        interval: remEntry.interval, unit: remEntry.unit,
        lastReplaced: remEntry.lastReplaced || fallbackDate || null,
        lastReplacedIsFallback: !remEntry.lastReplaced && !!fallbackDate,
        notes: item.notes || '', fromItem: item.id, fromReminder: remId,
      },
      item, remEntry,
    };
  }
}

// ── Reminder Timeline ──────────────────────
function openReminderTimeline(reminderId) {
  const resolved = _resolveReminderId(reminderId);
  if (!resolved) return;
  const r = resolved.r;

  const intervalDays = getReminderIntervalDays(r);
  const now          = new Date();
  const today        = now.toISOString().slice(0, 10);
  const status       = getReminderStatus(r);
  const dueDate      = getReminderDueDate(r);

  document.getElementById('rtl-title').textContent = r.name;
  document.getElementById('rtl-subtitle').textContent =
    `Every ${r.interval} ${r.unit} · ${r.lastReplaced
      ? (r.lastReplacedIsFallback ? 'Started ' : 'Last replaced ') + fmtDate(r.lastReplaced)
      : 'No start date recorded'}`;

  // Wire up the Mark replaced button
  const markBtn = document.getElementById('rtl-mark-btn');
  markBtn.onclick = () => { closeModal('reminder-timeline-modal'); openLogReplacementModal(reminderId); };

  // ── Build timeline nodes ─────────────────
  // Show 2 past cycles + current + 3 future cycles + reorder point
  const nodes = [];

  if (r.lastReplaced) {
    const startMs    = new Date(r.lastReplaced + 'T12:00:00').getTime();
    const intervalMs = intervalDays * 86400000;

    // Past replacement events (up to 2 before the most recent)
    for (let i = 2; i >= 1; i--) {
      const d = new Date(startMs - i * intervalMs);
      if (d < new Date(startMs - 365 * 86400000 * 2)) continue; // don't go more than 2 years back
      nodes.push({ type: 'past', date: d, label: 'Replaced' });
    }
    // Most recent replacement (anchor)
    nodes.push({ type: 'anchor', date: new Date(startMs), label: r.lastReplacedIsFallback ? 'First used' : 'Last replaced' });
    // Future replacement events
    for (let i = 1; i <= 3; i++) {
      const d = new Date(startMs + i * intervalMs);
      const isPast = d < now;
      nodes.push({
        type:  isPast ? 'overdue' : (i === 1 ? 'next' : 'future'),
        date:  d,
        label: i === 1 ? 'Replace' : 'Replace',
        cycle: i,
      });
    }
    // Reorder point — 80% into the NEXT cycle (buy before you run out)
    const nextDue   = new Date(startMs + intervalMs);
    const cycleAfter = new Date(startMs + 2 * intervalMs);
    const reorderDate = new Date(nextDue.getTime() + (cycleAfter.getTime() - nextDue.getTime()) * 0.2);
    // Only show reorder if it's in the future
    if (reorderDate > now) {
      nodes.push({ type: 'reorder', date: reorderDate, label: 'Reorder by' });
    }
    // Sort all by date
    nodes.sort((a, b) => a.date - b.date);
  }

  // ── Current period progress ──────────────
  let progressPct = null;
  let progressLabel = '';
  if (dueDate && r.lastReplaced) {
    const startMs  = new Date(r.lastReplaced + 'T12:00:00').getTime();
    const endMs    = dueDate.getTime();
    const nowMs    = now.getTime();
    progressPct    = Math.min(100, Math.max(0, Math.round(((nowMs - startMs) / (endMs - startMs)) * 100)));
    const daysLeft = getReminderDaysUntil(r);
    progressLabel  = daysLeft === null ? '' : daysLeft < 0 ? `${Math.abs(daysLeft)}d overdue` : daysLeft === 0 ? 'Due today' : `${daysLeft}d remaining`;
  }

  // ── Render ───────────────────────────────
  const statusColor = status === 'overdue' ? '#e85050' : status === 'soon' ? '#e8a838' : '#4cbb8a';

  let html = '';

  if (!r.lastReplaced) {
    html = `<div style="text-align:center;padding:32px 16px;color:var(--muted)">
      <div style="margin-bottom:12px;color:var(--accent)"><svg aria-hidden="true" style="width:36px;height:36px"><use href="#i-calendar"></use></svg></div>
      <div style="font-size:14px;font-weight:600;color:var(--text);margin-bottom:6px">No start date recorded</div>
      <div style="font-size:13px;line-height:1.6">Mark this as replaced to start tracking the timeline.</div>
    </div>`;
  } else {
    // Progress bar for current period
    if (progressPct !== null) {
      const barColor = status === 'overdue' ? '#e85050' : status === 'soon' ? '#e8a838' : '#4cbb8a';
      html += `<div style="margin-bottom:20px">
        <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--muted);font-family:var(--mono);margin-bottom:6px">
          <span>${r.lastReplacedIsFallback ? 'First used' : 'Last replaced'}</span>
          <span style="color:${barColor};font-weight:700">${progressLabel}</span>
          <span>Due ${dueDate ? fmtDate(dueDate.toISOString().slice(0,10)) : '—'}</span>
        </div>
        <div style="height:10px;background:var(--surface2);border-radius:99px;overflow:hidden;position:relative">
          <div style="height:100%;width:${progressPct}%;background:${barColor};border-radius:99px;transition:width 0.6s"></div>
          ${progressPct >= 100 ? '' : `<div style="position:absolute;top:0;left:${progressPct}%;transform:translateX(-50%);width:2px;height:100%;background:${barColor};opacity:0.6"></div>`}
        </div>
        <div style="font-size:11px;color:var(--muted);text-align:center;margin-top:4px;font-family:var(--mono)">${progressPct}% through current cycle</div>
      </div>`;
    }

    // Timeline nodes
    if (nodes.length) {
      html += `<div style="position:relative;padding:8px 0 8px 20px">`;

      // Vertical line
      html += `<div style="position:absolute;left:20px;top:20px;bottom:20px;width:2px;background:linear-gradient(to bottom,var(--surface2),var(--border),var(--surface2));border-radius:2px"></div>`;

      nodes.forEach((node, idx) => {
        const isPast    = node.type === 'past';
        const isAnchor  = node.type === 'anchor';
        const isOverdue = node.type === 'overdue';
        const isNext    = node.type === 'next';
        const isReorder = node.type === 'reorder';
        const isFuture  = node.type === 'future';

        const dotColor  = isOverdue ? '#e85050'
          : isNext    ? statusColor
          : isReorder ? '#e8a838'
          : isAnchor  ? '#5b8dee'
          : isPast    ? 'var(--border)'
          : 'var(--surface2)';

        const dotBorder = isNext || isReorder || isOverdue
          ? `3px solid ${dotColor}`
          : `2px solid ${dotColor}`;

        const dotSize   = isNext || isReorder ? '16px' : isAnchor ? '14px' : '10px';
        const dotBg     = isPast || isFuture ? 'var(--bg)' : isAnchor ? '#5b8dee' : isOverdue ? '#e85050' : isNext ? statusColor : '#e8a838';

        const nameColor = isOverdue ? '#e85050' : isNext ? statusColor : isReorder ? '#e8a838' : isAnchor ? '#5b8dee' : isPast ? 'var(--muted)' : 'var(--muted)';
        const fontWeight = isNext || isReorder || isAnchor ? '700' : '400';

        const isToday   = node.date.toISOString().slice(0,10) === today;
        const dateStr   = isToday ? 'Today' : fmtDate(node.date.toISOString().slice(0,10));
        const relStr    = isToday ? '' : (() => {
          const diff = Math.round((node.date - now) / 86400000);
          if (diff < 0) return `${Math.abs(diff)}d ago`;
          if (diff === 0) return 'today';
          return `in ${diff}d`;
        })();

        const icon = isReorder ? '<svg class="icon" aria-hidden="true"><use href="#i-shopping-cart"></use></svg>' : isOverdue ? '<svg class="icon" aria-hidden="true"><use href="#i-alert-triangle"></use></svg>' : isNext ? '<svg class="icon" aria-hidden="true"><use href="#i-bell"></use></svg>' : isAnchor ? '<svg class="icon" aria-hidden="true"><use href="#i-pin"></use></svg>' : isPast ? '<svg class="icon" aria-hidden="true"><use href="#i-check"></use></svg>' : '○';
        const chip = isReorder
          ? `<span style="font-size:10px;font-weight:700;background:rgba(232,168,56,0.15);color:#e8a838;border:1px solid rgba(232,168,56,0.3);border-radius:99px;padding:1px 7px;font-family:var(--mono)">REORDER</span>`
          : isNext
          ? `<span style="font-size:10px;font-weight:700;background:rgba(76,187,138,0.15);color:${statusColor};border:1px solid rgba(76,187,138,0.3);border-radius:99px;padding:1px 7px;font-family:var(--mono)">NEXT</span>`
          : isOverdue
          ? `<span style="font-size:10px;font-weight:700;background:rgba(232,80,80,0.15);color:#e85050;border:1px solid rgba(232,80,80,0.3);border-radius:99px;padding:1px 7px;font-family:var(--mono)">OVERDUE</span>`
          : '';

        html += `<div style="display:flex;align-items:flex-start;gap:14px;margin-bottom:${idx < nodes.length-1 ? '18' : '4'}px;position:relative">
          <div style="flex-shrink:0;width:${dotSize};height:${dotSize};border-radius:50%;background:${dotBg};border:${dotBorder};margin-top:3px;position:relative;z-index:1;margin-left:calc(-${dotSize}/2 + 1px)"></div>
          <div style="flex:1;min-width:0;padding-bottom:4px">
            <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
              <span style="font-size:14px;font-weight:${fontWeight};color:${nameColor}">${icon} ${esc(node.label)}</span>
              ${chip}
            </div>
            <div style="font-size:12px;color:var(--muted);font-family:var(--mono);margin-top:2px">
              ${dateStr}${relStr ? ` <span style="opacity:0.6">· ${relStr}</span>` : ''}
            </div>
          </div>
        </div>`;
      });

      html += `</div>`;
    }

    // Summary row
    const nextReplaceDate = dueDate ? fmtDate(dueDate.toISOString().slice(0,10)) : '—';
    const reorderNode = nodes.find(n => n.type === 'reorder');
    const reorderDateStr = reorderNode ? fmtDate(reorderNode.date.toISOString().slice(0,10)) : null;
    html += `<div style="margin-top:16px;padding:12px 14px;background:var(--surface2);border-radius:10px;border:1px solid var(--border);font-size:12px;font-family:var(--mono);line-height:2;color:var(--muted)">
      <div><svg class="icon" aria-hidden="true"><use href="#i-bell"></use></svg> <span style="color:var(--text)">Next replacement:</span> ${nextReplaceDate}</div>
      ${reorderDateStr ? `<div><svg class="icon" aria-hidden="true"><use href="#i-shopping-cart"></use></svg> <span style="color:var(--text)">Reorder by:</span> <span style="color:#e8a838">${reorderDateStr}</span></div>` : ''}
      <div><svg class="icon" aria-hidden="true"><use href="#i-repeat"></use></svg> <span style="color:var(--text)">Cycle:</span> every ${r.interval} ${r.unit} (${Math.round(intervalDays)} days)</div>
    </div>`;
  }

  document.getElementById('rtl-body').innerHTML = html;
  openModal('reminder-timeline-modal');
}

function updateRemindersBadge(count) {
  // Add a badge to the Reminders tab if there are overdue/soon items
  const tab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Reminders'));
  if (!tab) return;
  const existing = tab.querySelector('.reminder-badge');
  if (existing) existing.remove();
  if (count > 0) {
    const badge = document.createElement('span');
    badge.className = 'reminder-badge';
    badge.textContent = count;
    badge.style.cssText = 'background:var(--danger);color:#fff;font-size:9px;font-weight:700;padding:1px 5px;border-radius:99px;margin-left:4px;font-family:var(--mono)';
    tab.appendChild(badge);
  }
}

// ── Add / Edit ─────────────────────────────
function openAddReminderModal(prefill) {
  editingReminderId = null;
  document.getElementById('reminder-modal-title').innerHTML = '<svg class="icon icon-md" aria-hidden="true"><use href="#i-bell"></use></svg> Add Reminder';
  document.getElementById('r-name').value          = prefill?.name     || '';
  document.getElementById('r-interval').value      = prefill?.interval || 3;
  document.getElementById('r-unit').value          = prefill?.unit     || 'months';
  document.getElementById('r-last-replaced').value = '';
  document.getElementById('r-notes').value         = '';
  document.getElementById('r-item-link').value     = '';

  // Populate item link dropdown
  const sel = document.getElementById('r-item-link');
  sel.innerHTML = '<option value="">— standalone reminder —</option>'
    + items.filter(i => !i.quickAdded).map(i => `<option value="${i.id}">${esc(i.name)}</option>`).join('');

  // Render suggestion chips
  const sugg = document.getElementById('reminder-suggestions');
  sugg.innerHTML = REMINDER_SUGGESTIONS.map(s =>
    `<button onclick="applySuggestion(${JSON.stringify(s).replace(/"/g,'&quot;')})"
      style="font-size:11px;padding:4px 10px;border-radius:99px;background:var(--surface2);border:1px solid var(--border);color:var(--text);cursor:pointer;white-space:nowrap"
      onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">${esc(s.name)}</button>`
  ).join('');

  openModal('reminder-modal');
  setTimeout(() => document.getElementById('r-name').focus(), 100);
}

function applySuggestion(s) {
  document.getElementById('r-name').value     = s.name;
  document.getElementById('r-interval').value = s.interval;
  document.getElementById('r-unit').value     = s.unit;
}

function openEditReminderModal(id) {
  const r = reminders.find(r => r.id === id);
  if (!r) return;
  editingReminderId = id;
  document.getElementById('reminder-modal-title').innerHTML = '<svg class="icon icon-md" aria-hidden="true"><use href="#i-bell"></use></svg> Edit Reminder';
  document.getElementById('r-name').value          = r.name     || '';
  document.getElementById('r-interval').value      = r.interval || 3;
  document.getElementById('r-unit').value          = r.unit     || 'months';
  document.getElementById('r-last-replaced').value = r.lastReplaced || '';
  document.getElementById('r-notes').value         = r.notes    || '';
  const sel = document.getElementById('r-item-link');
  sel.innerHTML = '<option value="">— standalone reminder —</option>'
    + items.filter(i => !i.quickAdded).map(i => `<option value="${i.id}">${esc(i.name)}</option>`).join('');
  sel.value = r.linkedItemId || '';
  openModal('reminder-modal');
}

async function saveReminder() {
  if (!canWrite("reminders")) { showLockBanner("reminders"); return; }
  const name = document.getElementById('r-name').value.trim();
  if (!name) { toast('Enter a name for this reminder'); return; }

  const intervalVal = parseInt(document.getElementById('r-interval').value) || 3;
  const unit        = document.getElementById('r-unit').value;
  const lastDate    = document.getElementById('r-last-replaced').value;
  const notes       = document.getElementById('r-notes').value.trim();
  const linkedItemId = document.getElementById('r-item-link').value || null;

  if (editingReminderId) {
    const r = reminders.find(r => r.id === editingReminderId);
    if (r) {
      r.name = name; r.interval = intervalVal; r.unit = unit;
      r.lastReplaced = lastDate || r.lastReplaced; r.notes = notes;
      r.linkedItemId = linkedItemId;
    }
  } else {
    reminders.push({
      id:           uid(),
      name,
      interval:     intervalVal,
      unit,
      lastReplaced: lastDate || null,
      notes,
      linkedItemId,
      createdAt:    new Date().toISOString(),
    });
  }

  // Also update linked item's replacement fields
  if (linkedItemId) {
    const item = items.find(i => i.id === linkedItemId);
    if (item) {
      item.replacementInterval = intervalVal;
      item.replacementUnit     = unit;
      item.lastReplaced        = lastDate || item.lastReplaced || null;
      touchItem(item);
      await saveData();
    }
  }

  await saveReminders();
  closeModal('reminder-modal');
  renderReminders();
  setTimeout(syncAll, 400);
  toast('Reminder saved ✓');
}

async function deleteReminder(id) {
  if (!canWrite('reminders')) { showLockBanner('reminders'); return; }

  const isItemReminder = id.startsWith('item_');

  if (isItemReminder) {
    const resolved = _resolveReminderId(id);
    if (!resolved) return;
    const { item, remEntry } = resolved;
    const name = resolved.r.reminderName ? `${item.name} — ${resolved.r.reminderName}` : item.name;
    const confirmed = confirm(
      `Delete the replacement reminder "${name}"?\n\nThe item itself will remain in your Stockroom.`
    );
    if (!confirmed) return;

    if (remEntry && item.replacementReminders) {
      // New array format — remove specific reminder
      item.replacementReminders = item.replacementReminders.filter(r => r.id !== remEntry.id);
      // Sync legacy fields to first remaining reminder
      if (item.replacementReminders.length) {
        item.replacementInterval = item.replacementReminders[0].interval;
        item.replacementUnit     = item.replacementReminders[0].unit;
      } else {
        item.replacementInterval = null;
        item.replacementUnit     = null;
      }
    } else {
      // Legacy single reminder
      item.replacementInterval = null;
      item.replacementUnit     = null;
    }
    touchField(item, 'replacementInterval', 'replacementReminders');
    await saveData();
    await kvSyncNow(true);
    renderReminders();
    scheduleRender('grid', 'dashboard');
    toast('Reminder deleted');
  } else {
    const reminder = reminders.find(r => r.id === id);
    const name = reminder?.name || 'this reminder';
    const confirmed = confirm(`Delete "${name}"?\n\nThis cannot be undone.`);
    if (!confirmed) return;
    reminders = reminders.filter(r => r.id !== id);
    await saveReminders();
    _syncQueue.enqueue();
    renderReminders();
    toast('Reminder deleted');
  }
}

// ── Log replacement ───────────────────────
function openLogReplacementModal(id) {
  loggingReminderId = id;
  const resolved = _resolveReminderId(id);
  const name     = resolved?.r.name || '';
  const interval = resolved?.r ? `${resolved.r.interval} ${resolved.r.unit}` : '';

  document.getElementById('log-replacement-title').textContent    = `Replaced: ${name}`;
  document.getElementById('log-replacement-subtitle').textContent = interval ? `Next replacement in ${interval}` : 'When did you replace it?';
  document.getElementById('log-replacement-date').value           = today();
  openModal('log-replacement-modal');
}

async function confirmLogReplacement() {
  if (!canWrite("reminders")) { showLockBanner("reminders"); return; }
  const date = document.getElementById('log-replacement-date').value || today();
  const id   = loggingReminderId;

  if (id.startsWith('item_')) {
    const resolved = _resolveReminderId(id);
    if (resolved?.remEntry) {
      // New array format — update specific reminder's lastReplaced
      resolved.remEntry.lastReplaced = date;
      touchField(resolved.item, 'replacementReminders');
      await saveData();
    } else if (resolved?.item) {
      // Legacy single-reminder format
      resolved.item.lastReplaced = date;
      touchField(resolved.item, 'lastReplaced');
      await saveData();
    }
  } else {
    const r = reminders.find(r => r.id === id);
    if (r) { r.lastReplaced = date; await saveReminders(); }
    if (r?.linkedItemId) {
      const item = items.find(i => i.id === r.linkedItemId);
      if (item) { item.lastReplaced = date; touchField(item, 'lastReplaced'); await saveData(); }
    }
  }

  closeModal('log-replacement-modal');
  renderReminders();
  setTimeout(syncAll, 400);
  toast('Replacement logged ✓');
}

// ── Smart reminder actions ──────────────────
async function applyReminderReplaced(reminderId, date, token) {
  // Verify via backend then apply locally
  try {
    const res  = await fetch(`${WORKER_URL}/reminder-pending?id=${encodeURIComponent(reminderId)}&token=${encodeURIComponent(token)}`);
    const data = await res.json();
    // Whether KV has it or not, apply locally with the provided date
    const useDate = data.date || date || today();
    _applyReplacedLocally(reminderId, useDate);
  } catch(e) {
    // Offline or error — still apply locally
    _applyReplacedLocally(reminderId, date || today());
  }
}

async function _applyReplacedLocally(reminderId, date) {
  let changed = false;
  if (reminderId.startsWith('item_')) {
    const resolved = _resolveReminderId(reminderId);
    if (resolved?.remEntry) {
      resolved.remEntry.lastReplaced = date;
      touchField(resolved.item, 'replacementReminders');
      await saveData();
      changed = true;
    } else if (resolved?.item) {
      resolved.item.lastReplaced = date;
      touchField(resolved.item, 'lastReplaced');
      await saveData();
      changed = true;
    }
  } else {
    const r = reminders.find(r => r.id === reminderId);
    if (r) {
      r.lastReplaced = date;
      await saveReminders();
      changed = true;
      if (r.linkedItemId) {
        const item = items.find(i => i.id === r.linkedItemId);
        if (item) { item.lastReplaced = date; touchField(item, 'lastReplaced'); await saveData(); }
      }
    }
  }
  if (changed) {
    renderReminders();
    setTimeout(syncAll, 400);
    showToast('✅ Marked as replaced');
    bcPost({ type: 'REMINDER_REPLACED', reminderId, date });
  }
}

// Poll backend for any reminder replacements triggered via email (runs on load + visibility change)
async function pollReminderReplacements() {
  if (!WORKER_URL) return;
  const allR = [
    ...reminders,
    ...items.flatMap(i => {
      const base = { lastReplaced: i.lastReplaced || i.startedUsing || i.logs?.filter(l=>!l.pendingDelivery)[0]?.date || null };
      if (i.replacementReminders?.length) {
        return i.replacementReminders.map(r => ({
          id: `item_${i.id}_${r.id}`, name: r.name ? `${i.name} — ${r.name}` : i.name,
          interval: r.interval, unit: r.unit,
          lastReplaced: r.lastReplaced || base.lastReplaced,
        }));
      } else if (i.replacementInterval) {
        return [{ id: 'item_' + i.id, name: i.name, interval: i.replacementInterval, unit: i.replacementUnit, lastReplaced: base.lastReplaced }];
      }
      return [];
    }),
  ].filter(r => ['overdue','soon','today'].includes(getReminderStatus(r)));

  for (const r of allR) {
    try {
      const tokenRes  = await fetch(`${WORKER_URL}/reminder-token`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ reminderId: r.id }) });
      const { token } = await tokenRes.json();
      if (!token) continue;
      const checkRes  = await fetch(`${WORKER_URL}/reminder-pending?id=${encodeURIComponent(r.id)}&token=${encodeURIComponent(token)}`);
      const check     = await checkRes.json();
      if (check.replaced) _applyReplacedLocally(r.id, check.date || today());
    } catch(e) { /* ignore network errors */ }
  }
}

// ── In-app notification check ─────────────
async function checkReminderNotifications() {
  if (!notifEnabled || Notification.permission !== 'granted') return;

  const allReminders = [
    ...reminders,
    ...items.flatMap(i => {
      const base = { lastReplaced: i.lastReplaced || i.startedUsing || i.logs?.filter(l=>!l.pendingDelivery)[0]?.date || null };
      if (i.replacementReminders?.length) {
        return i.replacementReminders.map(r => ({
          id: `item_${i.id}_${r.id}`, name: r.name ? `${i.name} — ${r.name}` : i.name,
          interval: r.interval, unit: r.unit,
          lastReplaced: r.lastReplaced || base.lastReplaced,
        }));
      } else if (i.replacementInterval) {
        return [{ id: 'item_' + i.id, name: i.name, interval: i.replacementInterval, unit: i.replacementUnit, lastReplaced: base.lastReplaced }];
      }
      return [];
    }),
  ];

  const overdue  = allReminders.filter(r => getReminderStatus(r) === 'overdue');
  const dueToday = allReminders.filter(r => getReminderDaysUntil(r) === 0);

  if (!overdue.length && !dueToday.length) return;

  const today2 = new Date().toISOString().slice(0,10);
  if (localStorage.getItem('stockroom_last_reminder_notif') === today2) return;

  // Send one notification per due/overdue reminder (up to 3) with a Replaced action button
  const toNotify = [...overdue, ...dueToday].slice(0, 3);
  toNotify.forEach(r => sendReminderActionNotification(r));
  localStorage.setItem('stockroom_last_reminder_notif', today2);
}

async function sendReminderActionNotification(r) {
  if (!('serviceWorker' in navigator) || !navigator.serviceWorker.controller) {
    // Fallback: plain notification
    sendLocalNotification(`🔔 ${r.name}`, 'Tap to open STOCKROOM', 'stockroom-reminder-' + r.id);
    return;
  }
  // Fetch a token for this reminder so the SW can call /reminder-done
  let token = '';
  try {
    const res = await fetch(`${WORKER_URL}/reminder-token`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ reminderId: r.id }),
    });
    const data = await res.json();
    token = data.token || '';
  } catch(e) { /* no token — will open app instead */ }

  const status = getReminderStatus(r);
  const body   = status === 'overdue'
    ? `Overdue by ${Math.abs(getReminderDaysUntil(r))} day${Math.abs(getReminderDaysUntil(r)) !== 1 ? 's' : ''}`
    : 'Due today';

  const reg = await navigator.serviceWorker.ready;
  reg.showNotification(`🔔 ${r.name}`, {
    body,
    tag:       'stockroom-reminder-' + r.id,
    renotify:  true,
    icon:      './icon-192.png',
    badge:     './icon-192.png',
    actions: [
      { action: 'replaced', title: '✅ Replaced' },
      { action: 'open',     title: '📦 Open app' },
    ],
    data: {
      url:        window.location.href,
      reminderId: r.id,
      reminderName: r.name,
      token,
      workerUrl:  WORKER_URL,
    },
  });
}


async function markOrdered(id) {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  item.ordered   = true;
  item.orderedAt = new Date().toISOString();
  touchField(item, 'ordered','orderedAt');
  await saveData();
  scheduleRender('grid', 'dashboard', 'shopping');
  setTimeout(syncAll, 400);
  toast(`📦 ${item.name} marked as ordered`);
}

async function unmarkOrdered(id) {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  item.ordered   = false;
  item.orderedAt = null;
  touchField(item, 'ordered','orderedAt');
  await saveData();
  scheduleRender('grid', 'dashboard', 'shopping');
  setTimeout(syncAll, 400);
  toast('Removed ordered status');
}

// ═══════════════════════════════════════════
//  30 — COMPACT VIEW
// ═══════════════════════════════════════════
function toggleCompactView() {
  compactView = !compactView;
  const grid = document.getElementById('items-grid');
  const btn  = document.getElementById('compact-toggle-btn');
  if (grid) grid.classList.toggle('compact-view', compactView);
  if (btn)  btn.textContent = compactView ? '⊞' : '⊟';
  try { localStorage.setItem('stockroom_compact', compactView ? '1' : ''); } catch(e){}
}

function loadCompactView() {
  compactView = !!localStorage.getItem('stockroom_compact');
  const grid = document.getElementById('items-grid');
  const btn  = document.getElementById('compact-toggle-btn');
  if (grid && compactView) grid.classList.add('compact-view');
  if (btn) btn.textContent = compactView ? '⊞' : '⊟';
}

// ═══════════════════════════════════════════
//  19 — HOUSEHOLD PROFILES
// ═══════════════════════════════════════════
async function getProfiles() {
  const stored = await dbGet('profiles', 'profiles');
  if (stored) return stored;
  // Migration from localStorage
  try {
    const raw = localStorage.getItem('stockroom_profiles');
    if (raw) {
      const profiles = JSON.parse(raw) || {};
      await dbPut('profiles', 'profiles', profiles);
      localStorage.removeItem('stockroom_profiles');
      return profiles;
    }
  } catch(e) {}
  return {};
}

async function saveProfiles(profiles) {
  await dbPut('profiles', 'profiles', profiles);
}

async function loadProfile(key) {
  activeProfile = key || 'default';
  try { localStorage.setItem('stockroom_active_profile', activeProfile); } catch(e){}
  const profiles  = await getProfiles();
  const profile   = profiles[activeProfile];
  if (profile) {
    const deletedIds = await loadDeletedIds();
    items        = (profile.items || []).filter(i => !deletedIds.has(i.id));
    settings     = { threshold: 20, country: 'GB', ...profile.settings };
    reminders    = profile.reminders   || [];
    groceryItems = profile.groceries   || [];
    groceryDepts = profile.departments?.length ? profile.departments : DEFAULT_DEPTS.map(d => ({...d}));
  } else {
    await loadData();
    await loadReminders();
    await loadGrocery();
    await saveCurrentProfile();
  }
  updateProfileLabel();
  scheduleRender(...RENDER_REGIONS);
  updateSyncUI();
  applyTabPermissions();
}

async function saveCurrentProfile() {
  const deletedIds = await loadDeletedIds();
  const profiles = await getProfiles();
  const existing = profiles[activeProfile] || {};
  profiles[activeProfile] = {
    ...existing,
    items:       JSON.parse(JSON.stringify(items.filter(i => !deletedIds.has(i.id)))),
    settings,
    reminders:   JSON.parse(JSON.stringify(reminders)),
    groceries:   JSON.parse(JSON.stringify(groceryItems)),
    departments: JSON.parse(JSON.stringify(groceryDepts)),
  };
  await saveProfiles(profiles);
}

async function updateProfileLabel() {
  const profiles = await getProfiles();
  const profile  = profiles[activeProfile];
  const name     = profile?.name || (activeProfile === 'default' ? 'Home' : activeProfile);
  const colour   = profile?.colour || '#e8a838';
  const label    = document.getElementById('profile-label');
  if (label) {
    label.className = 'profile-label-subtitle';
    label.innerHTML = `/ <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${colour};margin-right:4px;vertical-align:middle"></span>${name.toUpperCase()}`;
  }
}

function openProfilePicker() {
  renderProfileList();
  openModal('profile-modal');
}

const HOUSEHOLD_COLOURS = [
  '#e8a838','#5b8dee','#4cbb8a','#e85050','#b45dee',
  '#ee8c5b','#5bdee8','#dee85b','#ee5bab','#7880a0',
];
const MAX_HOUSEHOLDS = 5;

async function renderProfileList() {
  const profiles = await getProfiles();
  const list     = document.getElementById('profile-list');
  const addSec   = document.getElementById('profile-add-section');
  if (!list) return;

  if (!profiles['default']) profiles['default'] = { name: 'Home', colour: '#e8a838', items: [], settings: {} };

  const entries = Object.entries(profiles);
  if (addSec) addSec.style.display = entries.length >= MAX_HOUSEHOLDS ? 'none' : 'block';

  list.innerHTML = entries.map(([key, p]) => {
    const isActive = key === activeProfile;
    const colour   = p.colour || '#e8a838';
    const count    = (p.items || []).length;
    const reminderCount = (p.reminders || []).length;
    const syncFile = p.driveFileName ? `<span style="font-size:10px;color:var(--muted);font-family:var(--mono)">${esc(p.driveFileName)}</span>` : '';
    return `<div style="display:flex;align-items:center;gap:10px;padding:12px 14px;background:${isActive ? 'rgba(232,168,56,0.08)' : 'var(--surface2)'};border:2px solid ${isActive ? colour : 'var(--border)'};border-radius:12px;transition:border-color 0.2s">
      <div style="width:14px;height:14px;border-radius:50%;background:${colour};flex-shrink:0;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>
      <div style="flex:1;min-width:0">
        <div style="font-size:14px;font-weight:700;color:${isActive ? colour : 'var(--text)'}">${esc(p.name || key)}${isActive ? ' ✓' : ''}</div>
        <div style="font-size:11px;color:var(--muted);font-family:var(--mono)">${count} item${count!==1?'s':''} · ${reminderCount} reminder${reminderCount!==1?'s':''}</div>
        ${syncFile}
      </div>
      <div style="display:flex;gap:6px;flex-shrink:0">
        ${!isActive ? `<button class="btn btn-primary btn-sm" onclick="switchProfile('${key}')">Switch</button>` : '<span style="font-size:11px;color:var(--accent);font-weight:700">Active</span>'}
        <button class="btn btn-ghost btn-sm" onclick="openHouseholdEdit('${key}')" title="Edit"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
        ${key !== 'default' ? `<button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="deleteProfile('${key}')" title="Delete"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>` : ''}
      </div>
    </div>`;
  }).join('');
}

function openHouseholdEdit(key) {
  getProfiles().then(profiles => {
    const p = profiles[key] || {};
    document.getElementById('household-edit-key').value  = key;
    document.getElementById('household-edit-name').value = p.name || '';
    const currentColour = p.colour || '#e8a838';
    const colourPicker  = document.getElementById('household-edit-colours');
    if (colourPicker) {
      colourPicker.innerHTML = HOUSEHOLD_COLOURS.map(c => `
        <div onclick="selectHouseholdColour('${c}')"
          data-colour="${c}"
          style="width:28px;height:28px;border-radius:50%;background:${c};cursor:pointer;
                 border:3px solid ${c === currentColour ? 'var(--text)' : 'transparent'};
                 transition:border-color 0.15s;box-shadow:0 2px 6px rgba(0,0,0,0.3)"></div>
      `).join('');
    }
    openModal('household-edit-modal');
  });
}

function selectHouseholdColour(colour) {
  document.querySelectorAll('#household-edit-colours [data-colour]').forEach(el => {
    el.style.borderColor = el.dataset.colour === colour ? 'var(--text)' : 'transparent';
  });
}

async function saveHouseholdEdit() {
  const key    = document.getElementById('household-edit-key').value;
  const name   = document.getElementById('household-edit-name').value.trim();
  if (!name) { toast('Enter a name'); return; }
  const selected = document.querySelector('#household-edit-colours [style*="var(--text)"]');
  const colour   = selected?.dataset.colour || '#e8a838';
  const profiles = await getProfiles();
  if (profiles[key]) {
    profiles[key].name   = name;
    profiles[key].colour = colour;
    await saveProfiles(profiles);
  }
  closeModal('household-edit-modal');
  renderProfileList();
  renderSettingsHouseholdList();
  if (key === activeProfile) updateProfileLabel();
  toast('Household updated ✓');
  _syncQueue.enqueue();
}

async function switchProfile(key) {
  await saveCurrentProfile();
  closeModal('profile-modal');
  await loadProfile(key);
  const profiles = await getProfiles();
  toast(`Switched to ${profiles[key]?.name || key}`);
}

async function addProfile() {
  const profiles = await getProfiles();
  if (Object.keys(profiles).length >= MAX_HOUSEHOLDS) {
    toast(`Maximum ${MAX_HOUSEHOLDS} households reached`);
    return;
  }
  const nameEl  = document.getElementById('new-profile-name');
  const count   = Object.keys(profiles).length + 1;
  const name    = nameEl?.value.trim() || `Home ${count}`;
  const colour  = HOUSEHOLD_COLOURS[count % HOUSEHOLD_COLOURS.length];
  const key     = 'profile_' + Date.now();
  profiles[key] = {
    name,
    colour,
    items:       [],
    settings:    { threshold: 20, country: settings.country || 'GB' },
    reminders:   [],
    groceries:   [],
    departments: DEFAULT_DEPTS.map(d => ({...d})),
  };
  await saveProfiles(profiles);
  if (nameEl) nameEl.value = '';
  renderProfileList();
  renderSettingsHouseholdList();
  toast(`"${name}" created — switch to it to set it up`);
  // Sync so new household persists across devices and reloads
  _syncQueue.enqueue();
}

async function deleteProfile(key) {
  const profiles = await getProfiles();
  const name = profiles[key]?.name || key;
  if (!confirm(`Delete "${name}" and all its items, groceries and reminders?\n\nThis cannot be undone.`)) return;
  delete profiles[key];
  await saveProfiles(profiles);
  // Record this key as deleted so the sync merge doesn't re-create it
  _addDeletedHousehold(key);
  if (activeProfile === key) await loadProfile('default');
  renderProfileList();
  renderSettingsHouseholdList();
  toast(`"${name}" deleted`);
  _syncQueue.enqueue();
}

function _getDeletedHouseholds() {
  try { return new Set(JSON.parse(localStorage.getItem('stockroom_deleted_households') || '[]')); }
  catch(e) { return new Set(); }
}
function _addDeletedHousehold(key) {
  const set = _getDeletedHouseholds();
  set.add(key);
  try { localStorage.setItem('stockroom_deleted_households', JSON.stringify([...set])); } catch(e) {}
}

// ═══════════════════════════════════════════
//  20 — EXPIRY DATES
// ═══════════════════════════════════════════
function getExpiryStatus(item) {
  if (!item.expiry) return null;
  const daysUntil = Math.floor((new Date(item.expiry + 'T12:00:00') - Date.now()) / 86400000);
  if (daysUntil < 0)   return { label: 'Expired', color: 'var(--danger)', days: daysUntil };
  if (daysUntil <= 30) return { label: `Expires in ${daysUntil}d`, color: 'var(--warn)', days: daysUntil };
  return { label: `Expires ${fmtDate(item.expiry)}`, color: 'var(--muted)', days: daysUntil };
}

// ═══════════════════════════════════════════
//  21 — REORDER POINT OVERRIDE
// ═══════════════════════════════════════════
function getItemThreshold(item) {
  return item.thresholdOverride ?? settings.threshold ?? 20;
}

// Patch getStatus to use per-item threshold
const _origGetStatus = getStatus;
// eslint-disable-next-line no-global-assign
function getStatus(pct, threshold, item) {
  const t = item ? getItemThreshold(item) : (threshold ?? 20);
  if (pct === null || pct === undefined) return 'nodata';
  if (pct <= t / 2) return 'critical';
  if (pct <= t) return 'warn';
  return 'ok';
}

// ═══════════════════════════════════════════
//  23 — SMART REORDER SUGGESTION
// ═══════════════════════════════════════════
function getReorderSuggestion(item) {
  if (!item.logs || item.logs.length < 2) return null;

  // Average days between purchases
  const sorted = [...item.logs].sort((a, b) => new Date(a.date) - new Date(b.date));
  const gaps   = [];
  for (let i = 1; i < sorted.length; i++) {
    const gap = (new Date(sorted[i].date) - new Date(sorted[i-1].date)) / 86400000;
    if (gap > 0) gaps.push(gap);
  }
  if (!gaps.length) return null;

  const avgGapDays = gaps.reduce((a,b) => a+b, 0) / gaps.length;
  const avgQty     = sorted.reduce((a,b) => a + (b.qty || 1), 0) / sorted.length;
  const totalDays  = (item.months || 1) * 30.5 * (item.qty || 1);

  // How many units to cover until next shop window?
  const shopWindowDays = Math.min(avgGapDays, 30);
  const suggestedQty   = Math.ceil((shopWindowDays / totalDays) * (item.qty || 1));

  return {
    qty: Math.max(1, suggestedQty),
    shopWindowDays: Math.round(shopWindowDays),
    avgGapDays: Math.round(avgGapDays),
  };
}

// ═══════════════════════════════════════════
//  35 — USAGE ANALYTICS
// ═══════════════════════════════════════════
function openAnalyticsModal(id) {
  const item = items.find(i => i.id === id);
  if (!item) return;

  const titleEl   = document.getElementById('analytics-title');
  const contentEl = document.getElementById('analytics-content');
  if (!titleEl || !contentEl) return;

  titleEl.innerHTML = `<svg class="icon" aria-hidden="true"><use href="#i-bar-chart-2"></use></svg> ${esc(item.name)}`;

  if (!item.logs || item.logs.length < 2) {
    contentEl.innerHTML = `<p style="color:var(--muted);font-size:13px">Not enough purchase history yet. Log at least 2 purchases to see analytics.</p>`;
    openModal('analytics-modal');
    return;
  }

  const sorted = [...item.logs].sort((a,b) => new Date(a.date) - new Date(b.date));
  const gaps   = [];
  for (let i = 1; i < sorted.length; i++) {
    const gap = (new Date(sorted[i].date) - new Date(sorted[i-1].date)) / 86400000;
    if (gap > 0) gaps.push({ gap: Math.round(gap), date: sorted[i].date });
  }

  const avgGap  = Math.round(gaps.reduce((a,b) => a+b.gap, 0) / gaps.length);
  const minGap  = Math.min(...gaps.map(g => g.gap));
  const maxGap  = Math.max(...gaps.map(g => g.gap));
  const prices  = sorted.filter(l => l.price).map(l => ({ val: parsePriceValue(l.price), raw: l.price, date: l.date })).filter(p => p.val);
  const avgPrice = prices.length ? prices.reduce((a,b) => a + b.val, 0) / prices.length : null;
  const totalSpend = prices.reduce((a,b) => a + b.val, 0);

  const suggestion = getReorderSuggestion(item);

  // Mini gap chart
  const barW = 100 / gaps.length;
  const maxG  = Math.max(...gaps.map(g => g.gap), 1);
  const gapBars = gaps.map(g =>
    `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px" title="${g.date}: ${g.gap} days">
      <div style="width:100%;max-width:32px;height:${Math.round((g.gap/maxG)*60)+4}px;background:var(--accent2);border-radius:3px 3px 0 0;opacity:0.8"></div>
      <div style="font-size:9px;color:var(--muted);font-family:var(--mono)">${g.gap}d</div>
    </div>`
  ).join('');

  contentEl.innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px">
      <div style="background:var(--surface2);border-radius:10px;padding:12px">
        <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-bottom:4px">AVG RESTOCK</div>
        <div style="font-size:22px;font-weight:700;color:var(--text)">${avgGap}<span style="font-size:13px;color:var(--muted)">d</span></div>
        <div style="font-size:11px;color:var(--muted)">range: ${minGap}–${maxGap} days</div>
      </div>
      <div style="background:var(--surface2);border-radius:10px;padding:12px">
        <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-bottom:4px">TOTAL SPEND</div>
        <div style="font-size:22px;font-weight:700;color:var(--ok)">£${totalSpend.toFixed(2)}</div>
        <div style="font-size:11px;color:var(--muted)">${prices.length} purchase${prices.length !== 1 ? 's' : ''}${avgPrice ? ` · avg £${avgPrice.toFixed(2)}` : ''}</div>
      </div>
    </div>
    ${gaps.length > 1 ? `
    <div style="margin-bottom:16px">
      <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px">Days between restocks</div>
      <div style="display:flex;align-items:flex-end;gap:3px;height:72px">${gapBars}</div>
    </div>` : ''}
    ${suggestion ? `
    <div style="background:rgba(232,168,56,0.08);border:1px solid rgba(232,168,56,0.2);border-radius:10px;padding:12px;margin-bottom:12px">
      <div style="font-size:12px;font-weight:700;color:var(--accent);margin-bottom:4px"><svg class="icon" aria-hidden="true"><use href="#i-lightbulb"></use></svg> Smart Reorder Suggestion</div>
      <div style="font-size:13px;color:var(--text)">Buy <strong>×${suggestion.qty}</strong> — covers your typical ${suggestion.shopWindowDays}-day shop window</div>
      <div style="font-size:11px;color:var(--muted);margin-top:3px">Based on your avg restock interval of ${suggestion.avgGapDays} days</div>
    </div>` : ''}
    <div style="font-size:11px;color:var(--muted);font-family:var(--mono)">
      First purchased: ${fmtDate(sorted[0].date)} · Total purchases: ${sorted.length}
    </div>`;

  openModal('analytics-modal');
}

// ═══════════════════════════════════════════
//  28 — CAMERA PHOTO
// ═══════════════════════════════════════════
let cameraStream = null;

function openCameraModal() {
  openModal('camera-modal');
  const video = document.getElementById('camera-video');
  navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } })
    .then(stream => { cameraStream = stream; video.srcObject = stream; })
    .catch(() => { toast('Could not access camera — check permissions'); closeModal('camera-modal'); });
}

function closeCameraModal() {
  if (cameraStream) { cameraStream.getTracks().forEach(t => t.stop()); cameraStream = null; }
  closeModal('camera-modal');
}

function capturePhoto() {
  const video  = document.getElementById('camera-video');
  const canvas = document.createElement('canvas');
  canvas.width  = video.videoWidth  || 640;
  canvas.height = video.videoHeight || 480;
  canvas.getContext('2d').drawImage(video, 0, 0);
  const dataUrl = canvas.toDataURL('image/jpeg', 0.85);
  pendingImageUrl = dataUrl;
  showImagePreview(dataUrl, 'Photo captured');
  closeCameraModal();
}

// ═══════════════════════════════════════════
//  36 — SHARED HOUSEHOLD / JOIN CODE
// ═══════════════════════════════════════════
function generateJoinCode() {
  // 6-character alphanumeric code stored in localStorage
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

function getOrCreateJoinCode() {
  let code = localStorage.getItem('stockroom_join_code');
  if (!code) { code = generateJoinCode(); localStorage.setItem('stockroom_join_code', code); }
  return code;
}

function renderHealthDashboard() {
  const el = document.getElementById('health-dashboard');
  if (!el || !items.length) { if (el) el.innerHTML = ''; return; }
  const threshold = settings.threshold;
  let critical = 0, warn = 0, ok = 0, nodata = 0;
  items.forEach(item => {
    const s = calcStock(item);
    const status = getStatus(s?.pct ?? null, threshold);
    if (status === 'critical') critical++;
    else if (status === 'warn') warn++;
    else if (status === 'ok') ok++;
    else nodata++;
  });
  const pill = (count, label, color, filterVal) => count === 0 ? '' :
    `<button onclick="setFilter('status','${filterVal}',this)" style="display:inline-flex;align-items:center;gap:5px;padding:5px 12px;border-radius:99px;border:1px solid ${color}33;background:${color}15;color:${color};font-size:12px;font-weight:700;cursor:pointer;font-family:var(--sans)">
      ${label} <span style="font-size:14px;font-weight:800">${count}</span>
    </button>`;
  el.innerHTML =
    pill(critical, `<span class='status-dot status-critical'></span> Critical`, '#e85050', 'critical') +
    pill(warn,     `<span class='status-dot status-low'></span> Low`,      '#e8a838', 'warn') +
    pill(ok,       `<span class='status-dot status-ok'></span> Good`,     '#4cbb8a', 'ok') +
    (nodata ? `<span style="font-size:12px;color:var(--muted);padding:5px 4px">${nodata} no data</span>` : '');
}

// ═══════════════════════════════════════════
//  DELETE LOG ENTRIES
// ═══════════════════════════════════════════
async function deleteLogEntry(itemId, logId) {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === itemId);
  if (!item) return;
  item.logs = (item.logs||[]).filter(l => l.id !== logId);
  touchItem(item);
  await saveData();
  // Re-render the log history in place
  renderLogHistory(item);
  scheduleRender('grid', 'dashboard');
  setTimeout(syncAll, 400);
}

function renderLogHistory(item) {
  const histWrap    = document.getElementById('log-history');
  const histEntries = document.getElementById('log-history-entries');
  if (!histWrap || !histEntries) return;
  if (!item.logs || !item.logs.length) { histWrap.style.display = 'none'; return; }

  histWrap.style.display = 'block';
  const pricedLogs = item.logs.map(l => parsePriceValue(l.price)).filter(p => p !== null);
  const minPrice   = pricedLogs.length ? Math.min(...pricedLogs) : null;
  const maxPrice   = pricedLogs.length ? Math.max(...pricedLogs) : null;

  histEntries.innerHTML = [...item.logs].reverse().slice(0,8).map((l, i, arr) => {
    const thisPrice = parsePriceValue(l.price);
    const prevLog   = arr[i + 1];
    const prevPrice = prevLog ? parsePriceValue(prevLog.price) : null;
    let priceTrendEl = '';
    if (thisPrice !== null && prevPrice !== null) {
      const diff = thisPrice - prevPrice;
      if (Math.abs(diff) >= 0.01)
        priceTrendEl = diff > 0
          ? `<span style="color:var(--danger);font-size:10px">↑</span>`
          : `<span style="color:var(--ok);font-size:10px">↓</span>`;
    }
    const isCheapest = thisPrice !== null && thisPrice === minPrice && pricedLogs.length > 1;
    const isMostExp  = thisPrice !== null && thisPrice === maxPrice && pricedLogs.length > 1 && minPrice !== maxPrice;
    return `<div class="log-entry" style="display:flex;align-items:center;gap:8px">
      <div style="flex:1;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <span style="color:var(--muted);font-size:12px">${esc(l.store)||'—'}</span>
        <span style="color:var(--muted);font-family:var(--mono);font-size:11px">${fmtDate(l.date)}</span>
        <span style="font-family:var(--mono);font-size:12px;display:flex;align-items:center;gap:4px">
          ×${l.qty}
          ${l.price ? `<span style="color:${isCheapest?'var(--ok)':isMostExp?'var(--danger)':'var(--text)'};font-weight:700">${esc(l.price)}</span>${priceTrendEl}${isCheapest?'<svg class="icon" aria-hidden="true"><use href="#i-tag"></use></svg>':''}` : ''}
        </span>
      </div>
      <button onclick="deleteLogEntry('${item.id}','${l.id}')" title="Delete this entry" style="background:none;border:none;cursor:pointer;font-size:14px;color:var(--muted);padding:2px 4px;border-radius:4px;flex-shrink:0" onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--muted)'"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
    </div>`;
  }).join('');

  if (pricedLogs.length >= 2) {
    const avg = (pricedLogs.reduce((a,b)=>a+b,0) / pricedLogs.length).toFixed(2);
    histEntries.innerHTML += `<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);font-size:11px;color:var(--muted);font-family:var(--mono);display:flex;gap:16px">
      <span>avg £${avg}</span>
      <span style="color:var(--ok)">low £${minPrice.toFixed(2)}</span>
      <span style="color:var(--danger)">high £${maxPrice.toFixed(2)}</span>
    </div>`;
  }
}

// ═══════════════════════════════════════════
//  QUICK LOG FROM SHOPPING LIST
// ═══════════════════════════════════════════
function quickLogFromShopping(itemId) {
  // Open the log modal pre-filled, then switch back to shopping after save
  openLogModal(itemId);
  // Mark that we came from shopping so we can return after save
  sessionStorage.setItem('log_return_view', 'shopping');
}


// ═══════════════════════════════════════════
//  PWA — SERVICE WORKER + INSTALL PROMPT
// ═══════════════════════════════════════════
let deferredInstallPrompt = null;

// ── iOS detection ─────────────────────────
const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent) ||
  (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
const isInStandaloneMode = window.matchMedia('(display-mode: standalone)').matches
  || window.navigator.standalone === true;
const isAndroid = /android/i.test(navigator.userAgent);

if ('serviceWorker' in navigator) {
  const isDeployed = location.hostname.includes('github.io') || location.hostname.includes('artbot5000');
  if (isDeployed) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('./sw.js').then(reg => {
        reg.update();
        if (reg.waiting) showUpdateBanner(reg.waiting);
        reg.addEventListener('updatefound', () => {
          const newWorker = reg.installing;
          newWorker.addEventListener('statechange', () => {
            if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
              showUpdateBanner(newWorker);
            }
          });
        });
      }).catch(e => console.warn('SW failed:', e));

      // Only reload automatically if the user explicitly triggered the update
      let _userTriggeredUpdate = false;
      window._markUserTriggeredUpdate = () => { _userTriggeredUpdate = true; };
      navigator.serviceWorker.addEventListener('controllerchange', () => {
        if (_userTriggeredUpdate) { location.reload(); }
        // If not user-triggered (e.g. SW auto-activating), just show the banner
        // so the user can choose when to refresh — don't force-reload
      });
    });

    // iOS install banner is shown via maybeShowInstallBanner() called from
    // _enterStockroom(), after server settings have been pulled and verified.
    // This ensures settings._installDismissed is authoritative (not stale from
    // IDB which may have been cleared along with cookies).
  }
}

function showUpdateBanner(worker) {
  const banner = document.getElementById('update-banner');
  if (banner) { banner.style.display = 'flex'; banner._worker = worker; }
}

function applyUpdate() {
  const banner = document.getElementById('update-banner');
  if (window._markUserTriggeredUpdate) window._markUserTriggeredUpdate();
  if (banner?._worker) banner._worker.postMessage({ type: 'SKIP_WAITING' });
  else location.reload(true);
}

// Android install prompt — just capture the event; banner is shown via
// maybeShowInstallBanner() called from _enterStockroom() after server settings
// are confirmed, so settings._installDismissed is always authoritative.
window.addEventListener('beforeinstallprompt', e => {
  e.preventDefault();
  deferredInstallPrompt = e;
  const row = document.getElementById('install-prompt-row');
  if (row) row.style.display = 'flex';
});

window.addEventListener('appinstalled', () => {
  deferredInstallPrompt = null;
  const banner = document.getElementById('install-banner');
  if (banner) banner.classList.remove('show');
  const iosBanner = document.getElementById('ios-install-banner');
  if (iosBanner) iosBanner.style.display = 'none';
  toast('STOCKROOM installed ✓');
});

async function installPWA() {
  if (isIOS) {
    // Can't prompt on iOS — show the iOS banner instead
    const banner = document.getElementById('ios-install-banner');
    if (banner) { banner.style.display = 'block'; banner.scrollIntoView({ behavior: 'smooth' }); }
    return;
  }
  if (!deferredInstallPrompt) {
    toast('Use "Add to Home Screen" from your browser menu');
    return;
  }
  deferredInstallPrompt.prompt();
  const { outcome } = await deferredInstallPrompt.userChoice;
  deferredInstallPrompt = null;
  const banner = document.getElementById('install-banner');
  if (banner) banner.classList.remove('show');
}

// ── Amazon Order History Import banner ───────────────────────────────────────
function _showAmazonBanners() {
  if (settings._amazonBannerDismissed) return;
  const desktop = document.getElementById('amazon-banner-desktop');
  const mobile  = document.getElementById('amazon-banner-mobile');
  if (desktop) desktop.style.display = 'block';
  if (mobile)  mobile.style.display  = 'block';
}

function dismissAmazonBanner() {
  const desktop = document.getElementById('amazon-banner-desktop');
  const mobile  = document.getElementById('amazon-banner-mobile');
  if (desktop) desktop.style.display = 'none';
  if (mobile)  mobile.style.display  = 'none';
  settings._amazonBannerDismissed = true;
  _saveSettings().then(() => kvPush().catch(() => {}));
  setTimeout(() => toast('Amazon order import is available anytime in Account & Security → Data'), 400);
}

function dismissInstallBanner() {
  const banner = document.getElementById('install-banner');
  if (banner) banner.classList.remove('show');
  // Save to localStorage immediately so it survives for the rest of this session
  try { localStorage.setItem('stockroom_install_dismissed', '1'); } catch(e){}
  // Save to settings object + IDB so it persists across page loads and devices
  settings._installDismissed = true;
  _saveSettings().then(() => {
    // Push to server so the flag is in the encrypted user blob on all devices
    if (kvConnected) kvSyncNow(true).catch(() => {});
  });
  setTimeout(() => toast('You can install STOCKROOM anytime from Settings'), 400);
}

function dismissIOSBanner() {
  const banner = document.getElementById('ios-install-banner');
  if (banner) banner.style.display = 'none';
  try { localStorage.setItem('stockroom_ios_banner_dismissed', '1'); } catch(e){}
  settings._installDismissed = true;
  _saveSettings().then(() => {
    if (kvConnected) kvSyncNow(true).catch(() => {});
  });
}

// ═══════════════════════════════════════════
//  PUSH NOTIFICATIONS
// ═══════════════════════════════════════════
let notifEnabled = false;

function loadNotifSettings() {
  try {
    const saved = JSON.parse(localStorage.getItem('stockroom_notif') || '{}');
    notifEnabled = saved.enabled || false;
    const daysEl = document.getElementById('notif-days');
    if (daysEl && saved.days) daysEl.value = saved.days;
  } catch(e){}
  // Set platform-specific description
  const note = document.getElementById('notif-platform-note');
  if (note) {
    if (isIOS && !isInStandaloneMode) {
      note.innerHTML = 'Push notifications on iPhone require the app to be <strong style="color:var(--text)">installed to your home screen</strong> via Safari. Once installed, notifications work like a native app.';
      note.style.color = 'var(--warn)';
    } else if (isIOS && isInStandaloneMode) {
      note.textContent = 'Running as installed app — notifications are fully supported on your iPhone.';
      note.style.color = 'var(--ok)';
    } else {
      note.textContent = 'Get notified when items are running low — no email needed. Works best when installed to your home screen.';
    }
  }
  updateNotifUI();
}

function saveNotifSettings() {
  try {
    const days = document.getElementById('notif-days')?.value || '14';
    localStorage.setItem('stockroom_notif', JSON.stringify({ enabled: notifEnabled, days }));
  } catch(e){}
}

function updateNotifUI() {
  const btn        = document.getElementById('notif-btn');
  const statusText = document.getElementById('notif-status-text');
  const threshRow  = document.getElementById('notif-threshold-row');
  const installRow = document.getElementById('install-prompt-row');
  const installInfo = document.getElementById('install-prompt-info');
  const installBtn  = document.getElementById('install-prompt-btn');
  if (!btn || !statusText) return;

  const permission = 'Notification' in window ? Notification.permission : 'unsupported';

  // iOS not installed — notifications won't work, show install prompt instead
  if (isIOS && !isInStandaloneMode) {
    statusText.textContent = 'Requires home screen install on iPhone';
    statusText.style.color = 'var(--warn)';
    btn.style.display = 'none';
    if (installRow) installRow.style.display = 'flex';
    if (installInfo) installInfo.innerHTML = '<h4>Add to Home Screen first</h4><p>iOS only supports notifications when installed via Safari → Share → Add to Home Screen</p>';
    if (installBtn) { installBtn.textContent = 'How to install'; installBtn.onclick = () => {
      const banner = document.getElementById('ios-install-banner');
      if (banner) { banner.style.display = 'block'; banner.scrollIntoView({ behavior: 'smooth' }); }
    }; }
    if (threshRow) threshRow.style.display = 'none';
    return;
  }

  if (permission === 'unsupported') {
    statusText.textContent = 'Not supported on this browser';
    btn.style.display = 'none';
    return;
  }
  if (permission === 'denied') {
    statusText.textContent = 'Blocked — enable notifications in browser/phone settings';
    statusText.style.color = 'var(--danger)';
    btn.style.display = 'none';
    return;
  }
  if (notifEnabled && permission === 'granted') {
    statusText.textContent = isIOS
      ? 'Active — you\'ll be alerted when items run low'
      : 'Active — you\'ll be alerted when items run low';
    statusText.style.color = 'var(--ok)';
    btn.textContent = 'Disable';
    btn.className   = 'btn btn-danger btn-sm';
    btn.style.display = 'inline-flex';
    if (threshRow)  threshRow.style.display  = 'flex';
    if (installRow) installRow.style.display = (!isIOS && deferredInstallPrompt) ? 'flex' : 'none';
  } else {
    statusText.textContent = 'Not enabled';
    statusText.style.color = 'var(--muted)';
    btn.textContent   = 'Enable';
    btn.className     = 'btn btn-ghost btn-sm';
    btn.style.display = 'inline-flex';
    if (threshRow)  threshRow.style.display  = 'none';
    if (installRow) installRow.style.display = (!isIOS && deferredInstallPrompt) ? 'flex' : 'none';
  }
}

async function toggleNotifications() {
  if (notifEnabled) {
    notifEnabled = false;
    saveNotifSettings();
    updateNotifUI();
    toast('Notifications disabled');
    return;
  }
  if (!('Notification' in window)) { toast('Not supported on this browser'); return; }
  const permission = await Notification.requestPermission();
  if (permission === 'granted') {
    notifEnabled = true;
    saveNotifSettings();
    updateNotifUI();
    toast('Notifications enabled ✓');
    setTimeout(() => sendLocalNotification(
      '📦 STOCKROOM notifications active',
      'You\'ll be notified when items are running low.',
      'stockroom-test'
    ), 600);
  } else {
    toast('Notifications blocked — enable in browser settings');
    updateNotifUI();
  }
}

function sendLocalNotification(title, body, tag) {
  if (!notifEnabled || Notification.permission !== 'granted') return;
  if ('serviceWorker' in navigator && navigator.serviceWorker.controller) {
    navigator.serviceWorker.ready.then(reg => {
      reg.showNotification(title, {
        body, tag: tag || 'stockroom', renotify: true,
        icon: './icon-192.png', badge: './icon-192.png',
        data: { url: window.location.href },
      });
    });
  } else {
    new Notification(title, { body, icon: './icon-192.png', tag: tag || 'stockroom' });
  }
}

async function checkLowStockNotifications() {
  if (!notifEnabled || Notification.permission !== 'granted') return;
  const days = parseInt(document.getElementById('notif-days')?.value || '14');
  const due  = items
    .map(item => { const s = calcStock(item); return s && s.daysLeft <= days ? { item, daysLeft: s.daysLeft } : null; })
    .filter(Boolean)
    .sort((a, b) => a.daysLeft - b.daysLeft);
  if (!due.length) return;

  const today = new Date().toISOString().slice(0, 10);
  if (localStorage.getItem('stockroom_last_notif') === today) return;

  // Include household name so user knows which household triggered the alert
  const profiles = await getProfiles();
  const householdName = profiles[activeProfile]?.name || 'Home';
  const hLabel = Object.keys(profiles).length > 1 ? ` · ${householdName}` : '';

  const critical = due.filter(d => d.daysLeft <= 7);
  const title = (critical.length
    ? `🔴 ${critical.length} item${critical.length !== 1 ? 's' : ''} critically low!`
    : `📦 ${due.length} item${due.length !== 1 ? 's' : ''} running low`) + hLabel;
  const body = due.slice(0, 3).map(d => `• ${d.item.name} (${d.daysLeft}d left)`).join('\n')
    + (due.length > 3 ? `\n+ ${due.length - 3} more` : '');

  sendLocalNotification(title, body, 'stockroom-lowstock');
  try { localStorage.setItem('stockroom_last_notif', today); } catch(e){}
}

// ═══════════════════════════════════════════
//  SORT
// ═══════════════════════════════════════════
let activeSort = 'status';

function setSort(val) {
  activeSort = val;
  renderGrid();
}

// ═══════════════════════════════════════════
//  SWIPE TO LOG
// ═══════════════════════════════════════════
const swipeState = {};
const SWIPE_THRESHOLD  = 110; // px to trigger (was 72)
const SWIPE_LOCK_ANGLE = 30;  // degrees — swipe must be within this of horizontal

function swipeStart(e, id) {
  const t = e.touches[0];
  swipeState[id] = { startX: t.clientX, startY: t.clientY, triggered: false, locked: null };
}

function swipeMove(e, id) {
  const s = swipeState[id];
  if (!s) return;
  const dx = e.touches[0].clientX - s.startX;
  const dy = e.touches[0].clientY - s.startY;
  if (dx < 0) return; // only right swipe

  // Direction lock: decide horizontal vs vertical on first meaningful movement
  if (s.locked === null && (Math.abs(dx) > 6 || Math.abs(dy) > 6)) {
    const angle = Math.abs(Math.atan2(Math.abs(dy), dx) * 180 / Math.PI);
    s.locked = angle < SWIPE_LOCK_ANGLE ? 'h' : 'v';
  }
  if (s.locked !== 'h') return; // vertical scroll — ignore

  const hint = document.getElementById('swipe-hint-' + id);
  if (hint) hint.style.opacity = Math.min(1, dx / SWIPE_THRESHOLD);
  if (dx > SWIPE_THRESHOLD && !s.triggered) {
    s.triggered = true;
    if (hint) hint.style.opacity = 1;
    navigator.vibrate && navigator.vibrate(30);
  }
}

async function swipeEnd(e, id) {
  const s = swipeState[id];
  if (!s) return;
  const hint = document.getElementById('swipe-hint-' + id);
  if (hint) hint.style.opacity = 0;
  // Use changedTouches for final position — only open if user ended the swipe far enough right
  const endX = e.changedTouches[0].clientX;
  const finalDx = endX - s.startX;
  if (s.triggered && finalDx >= SWIPE_THRESHOLD) openLogModal(id);
  delete swipeState[id];
}

// ═══════════════════════════════════════════
//  UNDO DELETE
// ═══════════════════════════════════════════
let deletedItem   = null;
let deletedIndex  = null;
let undoTimer     = null;

async function archiveItem(id) {
  if (!canWrite('stockroom')) { showLockBanner('stockroom'); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  item._archived  = true;
  item.updatedAt  = new Date().toISOString();
  await saveData();
  scheduleRender('grid', 'dashboard');
  _syncQueue.enqueue();
  toast(`"${item.name}" archived`);
}

async function restoreItem(id) {
  if (!canWrite('stockroom')) { showLockBanner('stockroom'); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  delete item._archived;
  item.updatedAt = new Date().toISOString();
  await saveData();
  scheduleRender('grid', 'dashboard');
  _syncQueue.enqueue();
  toast(`"${item.name}" restored`);
}

async function deleteItem(id) {
  const idx  = items.findIndex(i => i.id === id);
  const item = items[idx];
  if (!item) return;
  if (!confirm('Remove "' + item.name + '" from your stockroom?')) return;

  // Stash for undo
  deletedItem  = item;
  deletedIndex = idx;
  items.splice(idx, 1);
  await addTombstone(id);
  await saveData();
  // Close edit modal if it was open (deleteItem can be invoked from there)
  closeModal('item-modal');
  scheduleRender('grid', 'dashboard', 'shopping');
  _syncQueue.enqueue('Deleting item…');

  // Show undo toast
  clearTimeout(undoTimer);
  const t = document.getElementById('undo-toast');
  const m = document.getElementById('undo-msg');
  if (m) m.textContent = `"${item.name}" removed`;
  if (t) t.classList.add('show');
  undoTimer = setTimeout(() => {
    if (t) t.classList.remove('show');
    deletedItem  = null;
    deletedIndex = null;
  }, 5000);
}

async function undoDelete() {
  clearTimeout(undoTimer);
  const t = document.getElementById('undo-toast');
  if (t) t.classList.remove('show');
  if (!deletedItem) return;
  // Remove tombstone so the item can come back
  await removeTombstone(deletedItem.id);
  items.splice(deletedIndex, 0, deletedItem);
  deletedItem  = null;
  deletedIndex = null;
  await saveData();
  scheduleRender('grid', 'dashboard', 'shopping');
  setTimeout(syncAll, 400);
  toast('Restored ✓');
}


let lastAutoSync = 0;
const AUTO_SYNC_COOLDOWN = 30000; // min 30s between auto-syncs

// Sync when user switches back to the tab/app
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') {
    // Re-lock all unlocked secure notes immediately
    _relockAllNotes();
  }
  if (document.visibilityState === 'visible') {
    const now = Date.now();
    if (now - lastAutoSync > AUTO_SYNC_COOLDOWN) {
      lastAutoSync = now;
      checkCloudAhead();
    }
    // Check if any reminder was marked replaced via email while app was in background
    setTimeout(pollReminderReplacements, 800);
  }
});

// ── Service Worker message handler ───────────────────────
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.addEventListener('message', event => {
    if (event.data?.type === 'REMINDER_REPLACED') {
      const { reminderId, date } = event.data;
      _applyReplacedLocally(reminderId, date);
    }
    if (event.data?.type === 'BG_SYNC') {
      syncAll().catch(e => console.warn('BG_SYNC syncAll failed:', e));
    }
    if (event.data?.type === 'SW_UPDATED') {
      // New service worker activated — reload to get fresh app.js
      window.location.reload();
    }
  });
}

// ── BroadcastChannel — cross-tab sync ────────────────────
// When any tab saves data, all other open tabs update instantly.
// Zero server cost, works natively in Chrome/Firefox/Safari.
const _bc = ('BroadcastChannel' in window) ? new BroadcastChannel('stockroom') : null;

// Message types:
//   { type: 'DATA_CHANGED' }         — items changed, reload from IDB and re-render
//   { type: 'REMINDER_REPLACED', reminderId, date } — reminder marked done in another tab
//   { type: 'GROCERY_CHANGED' }      — grocery list changed
//   { type: 'SETTINGS_CHANGED' }     — settings updated

if (_bc) {
  _bc.onmessage = async event => {
    const { type } = event.data;

    if (type === 'DATA_CHANGED') {
      // Another tab saved items — reload from IndexedDB and re-render
      const fresh = await dbGet('items', 'items');
      if (fresh && Array.isArray(fresh)) {
        items = fresh;
        scheduleRender('grid', 'dashboard', 'shopping', 'sns', 'filters');
      }
    }

    if (type === 'REMINDER_REPLACED') {
      const { reminderId, date } = event.data;
      await _applyReplacedLocally(reminderId, date);
    }

    if (type === 'GROCERY_CHANGED') {
      const fresh = await dbGet('groceries', 'items');
      if (fresh) {
        groceryItems = fresh;
        renderGrocery();
      }
    }

    if (type === 'SETTINGS_CHANGED') {
      const fresh = await dbGet('settings', 'settings');
      if (fresh) {
        settings = { ...settings, ...fresh };
        scheduleRender('settings-ui');
      }
    }
  };
}

function bcPost(message) {
  try { _bc?.postMessage(message); } catch(e) {}
}

// ── Register background sync after every save ─────────────
// The browser will fire the 'sync' event in the SW when connectivity
// returns, even if this tab is closed.
async function registerBackgroundSync() {
  if (!('serviceWorker' in navigator) || !('SyncManager' in window)) return;
  try {
    const reg = await navigator.serviceWorker.ready;
    await reg.sync.register('stockroom-sync');
  } catch(e) {
    // Background Sync not supported or permission denied — silent fallback
  }
}

// ── Check for pending sync flag set by SW while app was closed ─
async function checkPendingSWSync() {
  if (!('caches' in window)) return;
  try {
    const cache   = await caches.open('stockroom-flags');
    const pending = await cache.match('pending-sync');
    if (pending) {
      await cache.delete('pending-sync');
      setTimeout(syncAll, 800);
    }
  } catch(e) {}
}

// Poll every 5 minutes as a backstop
setInterval(() => {
  if (document.visibilityState === 'visible') {
    lastAutoSync = Date.now();
    checkCloudAhead();
  }
}, 5 * 60 * 1000);

// Check if cloud is ahead of local — if so, sync silently
async function checkCloudAhead() {
  if (!kvConnected && !_shareState) return;
  try {
    let remoteModified = null;

    if (_shareState) {
      remoteModified = await proxyGetModifiedTime();
    } else if (kvConnected) {
      const res = await fetchKV(`${WORKER_URL}/data/modified`, {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify({emailHash: _kvEmailHash, verifier: _kvVerifier, household: activeProfile})
      });
      if (res.ok) remoteModified = (await res.json()).modifiedTime;
    }

    if (!remoteModified) return;

    const remoteTime = new Date(remoteModified).getTime();
    const localTime  = settings.lastSynced ? new Date(settings.lastSynced).getTime() : 0;

    if (remoteTime > localTime + 5000) {
      // Cloud is ahead — sync silently
      await syncAll();
      hideSyncBanner();
    }
  } catch(e) {
    console.warn('Auto-sync check failed:', e.message);
  }
}

function showSyncBanner() {
  const b = document.getElementById('sync-banner');
  if (b) b.style.display = 'flex';
}

function hideSyncBanner() {
  const b = document.getElementById('sync-banner');
  if (b) b.style.display = 'none';
}

async function syncNowAndDismissBanner() {
  hideSyncBanner();
  await syncAll();
}

// ═══════════════════════════════════════════
//  FILTER PANEL TOGGLE
// ═══════════════════════════════════════════
let filtersOpen = false;

function toggleFilters() {
  filtersOpen = !filtersOpen;
  const panel = document.getElementById('filter-panel');
  const icon  = document.getElementById('filter-toggle-icon');
  if (panel) panel.style.display = filtersOpen ? 'flex' : 'none';
  if (icon)  icon.textContent    = filtersOpen ? '▾' : '▸';
  // State is in-memory only — always resets to closed on page load
}

function loadFilterPanelState() {
  // Always default to closed — never restore across page loads
  filtersOpen = false;
  try { sessionStorage.removeItem('stockroom_filters_open'); } catch(e) {}
  const panel = document.getElementById('filter-panel');
  const icon  = document.getElementById('filter-toggle-icon');
  if (panel) panel.style.display = 'none';
  if (icon)  icon.textContent    = '▸';
}

function updateFilterBadge() {
  const badge = document.getElementById('filter-active-badge');
  if (!badge) return;
  const active = [
    activeFilter !== 'all',
    activeCadence !== 'all',
    activeStore !== 'all',
    activeRating !== 0,
  ].filter(Boolean).length;
  if (active > 0) {
    badge.textContent = active;
    badge.style.display = 'inline';
  } else {
    badge.style.display = 'none';
  }
}

function resetAllFilters() {
  activeFilter  = 'all';
  activeCadence = 'all';
  activeStore   = 'all';
  activeRating  = 0;
  // Reset chip visual states
  document.querySelectorAll('#filter-bar .filter-chip').forEach((c,i) => c.classList.toggle('active', i === 0));
  document.querySelectorAll('#store-filter-bar .filter-chip').forEach((c,i) => c.classList.toggle('active', i === 0));
  document.querySelectorAll('#rating-filter-bar .filter-chip').forEach((c,i) => c.classList.toggle('active', i === 0));
  updateFilterBadge();
  renderGrid();
}


function reconcileFilters() {
  const threshold = settings.threshold;
  const nonQuick  = items.filter(i => !i.quickAdded);

  // Helper: count items passing all current filters except the one being tested
  const anyMatch = (testFn) => nonQuick.some(item => {
    const s      = calcStock(item);
    const status = getStatus(s?.pct ?? null, threshold);
    return testFn(item, status);
  });

  let changed = false;

  // Status filter stale?
  if (activeFilter !== 'all') {
    const stillHas = anyMatch((item, status) => status === activeFilter);
    if (!stillHas) {
      activeFilter = 'all';
      // Reset status chip UI
      document.querySelectorAll('#filter-bar .filter-chip').forEach(c => {
        const txt = c.textContent.trim();
        if (txt === 'All' || txt.includes('All')) c.classList.add('active');
        else c.classList.remove('active');
      });
      changed = true;
    }
  }

  // Cadence filter stale?
  if (activeCadence !== 'all') {
    const stillHas = anyMatch((item) => item.cadence === activeCadence);
    if (!stillHas) {
      activeCadence = 'all';
      document.querySelectorAll('#filter-bar .filter-chip').forEach(c => {
        if (c.textContent.includes('Monthly') || c.textContent.includes('Bulk')) c.classList.remove('active');
      });
      changed = true;
    }
  }

  // Store filter stale?
  if (activeStore !== 'all') {
    const allStores = new Set();
    nonQuick.forEach(item => {
      if (item.store?.trim()) allStores.add(item.store.trim());
      (item.logs||[]).forEach(l => { if (l.store?.trim()) allStores.add(l.store.trim()); });
    });
    if (!allStores.has(activeStore)) {
      activeStore = 'all';
      changed = true;
    }
  }

  // Rating filter stale?
  if (activeRating !== 0) {
    const stillHas = activeRating === -1
      ? anyMatch(item => !item.rating)
      : anyMatch(item => item.rating === activeRating);
    if (!stillHas) {
      activeRating = 0;
      document.querySelectorAll('#rating-filter-bar .filter-chip').forEach(c => c.classList.remove('active'));
      const allRatingBtn = document.querySelector('#rating-filter-bar .filter-chip');
      if (allRatingBtn) allRatingBtn.classList.add('active');
      changed = true;
    }
  }

  // Tag filter stale?
  if (activeTagFilter !== null) {
    const stillHas = anyMatch(item => (item.tags||[]).includes(activeTagFilter));
    if (!stillHas) {
      activeTagFilter = null;
      changed = true;
    }
  }

  if (changed) updateFilterBadge();
}

// ═══════════════════════════════════════════════════════════
//  REACTIVE RENDER SCHEDULER
//  Replaces renderAll() with targeted dirty-flag rendering.
//  Multiple saves in the same frame coalesce into one render pass.
// ═══════════════════════════════════════════════════════════

const RENDER_REGIONS = ['grid','dashboard','filters','shopping','sns','settings-ui'];
const _dirty  = new Set();
let   _raf    = null;
let   _rendering = false;

function scheduleRender(...regions) {
  if (regions.length === 0) regions = RENDER_REGIONS; // full render if no args
  regions.forEach(r => _dirty.add(r));
  if (_raf) return; // already scheduled — extra dirty flags will be picked up
  _raf = requestAnimationFrame(_flushRender);
}

function _flushRender() {
  _raf = null;
  if (_rendering) {
    // Re-schedule — a previous render is still running (async)
    _raf = requestAnimationFrame(_flushRender);
    return;
  }
  _rendering = true;
  const regions = new Set(_dirty);
  _dirty.clear();

  try {
    // Always update item count
    const countEl = document.getElementById('item-count');
    if (countEl) countEl.textContent = items.length + ' item' + (items.length!==1?'s':'');

    if (regions.has('filters')) {
      buildStoreFilterBar();
      buildTagFilterBar();
      buildShoppingTagFilterBarInline();
      reconcileFilters();
    }
    if (regions.has('dashboard')) {
      renderHealthDashboard();
      renderPendingDeliveries();
      renderIncompleteSection();
    }
    if (regions.has('sns')) {
      updateSnSBanner();
    }
    if (regions.has('grid')) {
      renderGrid();
    }
    if (regions.has('shopping') && document.getElementById('shopping-panel')?.style.display !== 'none') {
      renderShoppingList();
    }
    if (regions.has('settings-ui')) {
      const t  = document.getElementById('setting-threshold');
      const c  = document.getElementById('setting-country');
      const e  = document.getElementById('setting-email');
      const iv = document.getElementById('setting-email-interval');
      const sd = document.getElementById('setting-email-start');
      const st = document.getElementById('setting-email-start-time');
      if (t)  t.value  = settings.threshold;
      // Re-populate country options if empty (e.g. first render after sign-in)
      if (c && c.options.length === 0) buildSettingsCountrySelect();
      if (c)  c.value  = settings.country || 'GB';
      if (e)  e.value  = settings.email || '';
      if (iv) iv.value = settings.emailInterval ?? 30;
      if (sd) sd.value = settings.emailStartDate || '';
      if (st) st.value = settings.emailStartTime || '09:00';
      updateLastSentUI();
    }
  } finally {
    _rendering = false;
    // If new dirty flags arrived during render, flush again
    if (_dirty.size > 0) _raf = requestAnimationFrame(_flushRender);
  }
}

// Legacy alias — keeps existing callers working, schedules a full render
function renderAll() { scheduleRender(...RENDER_REGIONS); }

function renderGrid() {
  const threshold   = settings.threshold;
  const grid        = document.getElementById('items-grid');
  const stockSearch = (document.getElementById('stock-search')?.value || '').toLowerCase().trim();

  // Defensive guard — should never happen but prevents blank screens
  if (!Array.isArray(items)) {
    console.error('stockroom: items is not an array in renderGrid', items);
    items = [];
  }

  let filtered = items.filter(item => {
    // Quick-added items live in their own section
    if (item.quickAdded) return false;
    // Archived items only shown when archive filter is active
    if (item._archived && activeFilter !== 'archived') return false;
    if (!item._archived && activeFilter === 'archived') return false;
    if (activeFilter === 'archived') {
      if (stockSearch && !item.name.toLowerCase().includes(stockSearch) &&
          !(item.notes||'').toLowerCase().includes(stockSearch) &&
          !(item.category||'').toLowerCase().includes(stockSearch)) return false;
      return true;
    }

    // Text search filter
    if (stockSearch) {
      const hay = [item.name, item.category, item.notes, item.store, ...(item.tags||[])].join(' ').toLowerCase();
      if (!hay.includes(stockSearch)) return false;
    }

    const s = calcStock(item);
    const status = getStatus(s?.pct ?? null, threshold);

    // Status filter
    if (activeFilter !== 'all' && status !== activeFilter) return false;

    // Cadence filter
    if (activeCadence !== 'all' && item.cadence !== activeCadence) return false;

    // Store filter — match item-level store, or any log entry
    if (activeStore !== 'all') {
      const itemStore = item.store && item.store.trim() === activeStore;
      const logStore = (item.logs||[]).some(l => l.store && l.store.trim() === activeStore);
      if (!itemStore && !logStore) return false;
    }

    // Rating filter
    if (activeRating === -1 && item.rating) return false;        // unrated only
    if (activeRating === -1 && !item.rating) return true;
    if (activeRating > 0 && item.rating !== activeRating) return false;

    // Tag filter
    if (activeTagFilter !== null) {
      if (!(item.tags || []).includes(activeTagFilter)) return false;
    }

    return true;
  });

  if (items.length === 0) {
    grid.innerHTML = `<div class="empty-state">
      <div class="empty-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"/><path d="M12 22V12"/><polyline points="3.29 7 12 12 20.71 7"/><path d="m7.5 4.27 9 5.15"/></svg></div>
      <h3>Your stockroom is empty</h3>
      <p>Add the household consumables you buy regularly — coffee, toilet paper, cleaning supplies, anything you don't want to run out of.</p>
      <button class="btn btn-primary" onclick="openAddModal()">+ Add Your First Item</button>
    </div>`;
    return;
  }

  if (filtered.length === 0) {
    grid.innerHTML = `<div class="empty-state">
      <div class="empty-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="m21 21-4.34-4.34"/><circle cx="11" cy="11" r="8"/></svg></div>
      <h3>No items match this filter</h3>
      <p>Try a different filter above.</p>
    </div>`;
    return;
  }

  // Sort
  filtered.sort((a, b) => {
    switch (activeSort) {
      case 'name':
        return (a.name||'').localeCompare(b.name||'');
      case 'days': {
        const da = calcStock(a)?.daysLeft ?? 9999;
        const db = calcStock(b)?.daysLeft ?? 9999;
        return da - db;
      }
      case 'lastbought': {
        const la = a.logs?.at(-1)?.date || '0000';
        const lb = b.logs?.at(-1)?.date || '0000';
        return lb.localeCompare(la); // most recent first
      }
      case 'rating':
        return (b.rating||0) - (a.rating||0);
      case 'added':
        return 0; // preserve insertion order
      case 'status':
      default: {
        const pa = calcStock(a)?.pct ?? 101;
        const pb = calcStock(b)?.pct ?? 101;
        return pa - pb;
      }
    }
  });

  grid.innerHTML = filtered.map(item => cardHTML(item, threshold)).join('');
}

function cardHTML(item, threshold) {
  const s = calcStock(item);
  const pct = s?.pct ?? null;
  const daysLeft = s?.daysLeft ?? null;
  const status = getStatus(pct, threshold);
  const color = STATUS_COLOR[status];
  const lastLog = item.logs?.at(-1);

  const fillColor = status === 'critical' ? '#e85050' : status === 'warn' ? '#e8a838' : '#4cbb8a';
  const cadenceBadge = item.cadence === 'bulk'
    ? `<span class="cadence-badge badge-bulk"><svg class="icon" aria-hidden="true"><use href="#i-package"></use></svg> Bulk</span>`
    : `<span class="cadence-badge badge-monthly"><svg class="icon" aria-hidden="true"><use href="#i-calendar-days"></use></svg> Monthly</span>`;
  const statusBadge = `<span class="status-badge" style="background:${color}22;color:${color}">${STATUS_LABEL[status]}</span>`;

  return `
  <div class="item-card" style="border-left:3px solid ${color}" data-id="${item.id}"
    ontouchstart="swipeStart(event,'${item.id}')" ontouchmove="swipeMove(event,'${item.id}')" ontouchend="swipeEnd(event,'${item.id}')">
    <div class="swipe-hint" id="swipe-hint-${item.id}"><svg class="icon" aria-hidden="true"><use href="#i-clipboard-list"></use></svg></div>
    ${item.imageUrl ? `<img class="card-image" src="${esc(item.imageUrl)}" alt="${esc(item.name)}" onerror="this.style.display='none'">` : ''}
    <div class="card-top">
      <div class="card-category">${item.category||'Other'}</div>
      <div class="card-btns">
        <button class="btn-icon" title="Update stock count" onclick="openStockCountModal('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-hash"></use></svg></button>
        <button class="btn-icon" title="Usage analytics" onclick="openAnalyticsModal('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-bar-chart-2"></use></svg></button>
        <button class="btn-icon" title="Price history" onclick="openPriceHistoryModal('${item.id}')" ${getPriceHistory(item).length < 2 ? 'style="opacity:0.35;cursor:default"' : ''}><svg class="icon" aria-hidden="true"><use href="#i-banknote"></use></svg></button>
        <button class="btn-icon" title="Share item" onclick="shareItem('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-share-2"></use></svg></button>
        <button class="btn-icon" title="Edit" onclick="openEditModal('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
        ${item._archived
          ? `<button class="btn-icon" title="Restore from archive" onclick="restoreItem('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-refresh-ccw"></use></svg></button>
             <button class="btn-icon" title="Delete permanently" onclick="deleteItem('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg></button>`
          : `<button class="btn-icon" title="Archive item" onclick="archiveItem('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-archive"></use></svg></button>`}
      </div>
    </div>
    <div class="card-name" style="margin-bottom:12px">${esc(item.name)}</div>
    <div class="stock-bar-wrap">
      <div class="stock-bar-label">
        <span>STOCK</span>
        <span style="color:${color}">${pct !== null ? pct+'%' : '?'}</span>
      </div>
      <div class="stock-bar">
        <div class="stock-bar-fill" style="width:${pct??0}%;background:${fillColor}"></div>
      </div>
    </div>
    <div class="card-meta">
      <div class="meta-item"><strong>${daysLeft !== null ? daysLeft+'d left' : 'No data'}</strong>Est. remaining</div>
      <div class="meta-item" title="${fmtDate(lastLog?.date)}"><strong>${timeAgo(lastLog?.date)}</strong>Last bought</div>
      <div class="meta-item"><strong>${item.startedUsing ? fmtDate(item.startedUsing) : '—'}</strong>Started using</div>
      <div class="meta-item"><strong>${item.months||1}mo</strong>Per purchase</div>
    </div>
    ${item.stockCount != null ? `<div style="font-size:11px;color:var(--accent2);font-family:var(--mono);margin-bottom:8p"><svg class="icon" aria-hidden="true"><use href="#i-hash"></use></svg> Stock count: ${item.stockCount} units remaining · counted ${fmtDate(item.stockCountDate)}</div>` : ''}
    ${priceTrendHTML(item)}
    ${frequencyInsightHTML(item)}
    <div style="margin-bottom:8px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      ${cadenceBadge}${statusBadge}
      ${item.ordered ? `<span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:99px;background:rgba(91,141,238,0.15);border:1px solid rgba(91,141,238,0.3);color:#5b8dee;font-family:var(--mono"><svg class="icon" aria-hidden="true"><use href="#i-truck"></use></svg> Ordered</span>` : ''}
      ${(() => { const ex = getExpiryStatus(item); return ex ? `<span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:99px;background:rgba(232,168,56,0.1);border:1px solid rgba(232,168,56,0.3);color:${ex.color};font-family:var(--mono)" title="${fmtDate(item.expiry)}">⏰ ${ex.label}</span>` : ''; })()}
    </div>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
      <div class="card-star-rating" title="Click to rate">
        ${[1,2,3,4,5].map(n => `<span class="card-star${(item.rating||0)>=n?' on':''}" onclick="rateItem('${item.id}',${n})" data-id="${item.id}" data-val="${n}"
          onmouseover="previewCardStars('${item.id}',${n})" onmouseout="resetCardStars('${item.id}')">★</span>`).join('')}
      </div>
      <span class="card-rating-label" id="rl-${item.id}" style="font-size:11px;color:${!item.rating?'var(--muted)':item.rating<=2?'var(--danger)':item.rating>=4?'var(--ok)':'var(--muted)'}">
        ${item.rating ? RATING_LABELS[item.rating] : 'Not rated'}
      </span>
    </div>
    ${cardTagsHTML(item)}
    ${item.notes ? `<div class="card-notes"><svg class="icon" aria-hidden="true"><use href="#i-message-square"></use></svg> ${esc(item.notes)}</div>` : ''}
    ${storePricesCardHTML(item)}
    ${item.url ? `<a class="card-link" href="${esc(item.url)}" target="_blank" rel="noopener"><svg class="icon" aria-hidden="true"><use href="#i-shopping-cart"></use></svg> Buy now ↗</a>` : ''}
    <div style="display:flex;gap:6px;margin-top:10px">
      ${_cardOrderButton(item)}
      <button class="btn btn-ghost btn-sm log-btn" style="flex:1" onclick="openReplRemindersModal('${item.id}')"><svg class="icon" aria-hidden="true"><use href="#i-bell"></use></svg> Replacement Reminders</button>
    </div>
  </div>`;
}

// Smart order button for item cards — shows correct stage based on order progress
function _cardOrderButton(item) {
  const hasPending         = (item.logs || []).some(l => l.pendingDelivery);
  const hasDeliveredNoStart= (item.logs || []).some(l => !l.pendingDelivery && l.deliveredDate) && !item.startedUsing;
  if (hasPending) {
    return `<button class="btn btn-sm log-btn" style="flex:1;background:rgba(76,187,138,0.15);color:var(--ok);border:1px solid rgba(76,187,138,0.3)" onclick="openOrderFlow('${item.id}','delivered')"><svg class="icon" aria-hidden="true"><use href="#i-package-check"></use></svg> Mark Delivered</button>`;
  } else if (hasDeliveredNoStart) {
    return `<button class="btn btn-sm log-btn" style="flex:1;background:rgba(91,141,238,0.15);color:#5b8dee;border:1px solid rgba(91,141,238,0.3)" onclick="openOrderFlow('${item.id}','startusing')"><svg class="icon" aria-hidden="true"><use href="#i-play"></use></svg> Start Using</button>`;
  } else {
    return `<button class="btn btn-ghost btn-sm log-btn" style="flex:1" onclick="openOrderFlow('${item.id}','purchase')"><svg class="icon" aria-hidden="true"><use href="#i-clipboard-list"></use></svg> Log Purchase</button>`;
  }
}

function storePricesCardHTML(item) {
  const prices = (item.storePrices || []).filter(sp => sp.store && sp.price);
  if (!prices.length) return '';
  const parsed = prices.map(sp => ({ ...sp, val: parsePriceValue(sp.price) }));
  const validPrices = parsed.filter(p => p.val !== null);
  const minVal = validPrices.length ? Math.min(...validPrices.map(p => p.val)) : null;
  const chips = parsed.map(sp => {
    const isBest = sp.val !== null && sp.val === minVal && validPrices.length > 1;
    return `<span style="font-size:11px;font-family:var(--mono);padding:2px 8px;border-radius:99px;background:${isBest?'rgba(76,187,138,0.15)':'var(--surface2)'};border:1px solid ${isBest?'rgba(76,187,138,0.4)':'var(--border)'};color:${isBest?'var(--ok)':'var(--muted)'};white-space:nowrap">
      ${esc(sp.store)}: ${esc(sp.price)}${isBest?' <svg class="icon" aria-hidden="true"><use href="#i-tag"></use></svg>':''}
    </span>`;
  }).join('');
  return `<div style="display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px">${chips}</div>`;
}

function esc(str) {
  return String(str||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ═══════════════════════════════════════════
//  PRODUCT IMAGE FETCHING
// ═══════════════════════════════════════════
let pendingImageUrl = null; // holds fetched image URL until item is saved

const CATEGORY_EMOJI = {
  'Kitchen':'🍳', 'Bathroom':'🛁', 'Cleaning':'🧹',
  'Food & Drink':'☕', 'Health':'💊', 'Garden':'🌱',
  'Office':'📎', 'Other':'📦'
};

// ═══════════════════════════════════════════
//  CUSTOM TAGS
// ═══════════════════════════════════════════
const TAG_COLORS = [
  { bg:'rgba(91,141,238,0.15)',  border:'rgba(91,141,238,0.5)',  text:'#5b8dee' },  // blue
  { bg:'rgba(76,187,138,0.15)', border:'rgba(76,187,138,0.5)',  text:'#4cbb8a' },  // green
  { bg:'rgba(232,168,56,0.15)', border:'rgba(232,168,56,0.5)',  text:'#e8a838' },  // amber
  { bg:'rgba(193,100,232,0.15)',border:'rgba(193,100,232,0.5)', text:'#c164e8' },  // purple
  { bg:'rgba(232,80,80,0.15)',  border:'rgba(232,80,80,0.5)',   text:'#e85050' },  // red
];

let activeTagFilter = null; // null = all, 0-4 = tag index

function getCustomTags() {
  return settings.customTags || ['','','','',''];
}

function cardTagsHTML(item) {
  const tags = getCustomTags();
  const itemTags = item.tags || [];
  const defined = tags.filter(t => t && t.trim());
  if (!defined.length) return '';

  const chips = tags.map((tag, i) => {
    if (!tag || !tag.trim()) return '';
    const c = TAG_COLORS[i];
    const active = itemTags.includes(i);
    return `<span class="item-tag ${active ? 'active' : 'inactive'}"
      style="background:${active ? c.bg : 'transparent'};border-color:${c.border};color:${c.text}"
      onclick="toggleItemTag('${item.id}',${i})"
      title="${active ? 'Remove tag' : 'Add tag'}">${esc(tag)}</span>`;
  }).join('');

  return `<div style="display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px">${chips}</div>`;
}

async function toggleItemTag(itemId, tagIndex) {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === itemId);
  if (!item) return;
  if (!item.tags) item.tags = [];
  const idx = item.tags.indexOf(tagIndex);
  if (idx === -1) item.tags.push(tagIndex);
  else item.tags.splice(idx, 1);
  touchItem(item);
  await saveData();
  // Re-render just this card's tags without full grid re-render
  scheduleRender('grid');
  _syncQueue.enqueue();
}

function buildTagFilterBar() {
  const bar = document.getElementById('tag-filter-bar');
  if (!bar) return;
  const tags = getCustomTags();
  const defined = tags.map((t,i) => ({t,i})).filter(({t}) => t && t.trim());
  const hasRoom = defined.length < 5;

  const label = `<span style="font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;flex-shrink:0">Tags:</span>`;
  const allChip = defined.length
    ? `<button class="tag-filter-chip${activeTagFilter===null?' active':''}" onclick="setTagFilter(null,this)">All</button>`
    : '';
  const chips = defined.map(({t,i}) => {
    const c = TAG_COLORS[i];
    const isActive = activeTagFilter === i;
    return `<span class="tag-filter-chip${isActive?' active':''}"
      style="${isActive?`background:${c.bg};border-color:${c.border};color:${c.text}`:''}"
      onclick="setTagFilter(${i},this)">
      ${esc(t)}
      <span class="tag-x" onclick="event.stopPropagation();deleteTag(${i})" title="Remove tag">×</span>
    </span>`;
  }).join('');
  const addBtn = hasRoom
    ? `<button class="btn-add-tag" onclick="showAddTagInput('tag-filter-bar')">+ Tag</button>`
    : '';
  bar.innerHTML = label + allChip + chips + addBtn;
}

function buildShoppingTagFilterBarInline() {
  const bar = document.getElementById('shopping-tag-filter-bar');
  if (!bar) return;
  const tags = getCustomTags();
  const defined = tags.map((t,i) => ({t,i})).filter(({t}) => t && t.trim());
  const hasRoom = defined.length < 5;

  const label = `<span style="font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;flex-shrink:0">Tags:</span>`;
  const allChip = defined.length
    ? `<button class="tag-filter-chip${shoppingTagFilter===null?' active':''}" onclick="setShoppingTagFilter(null,this)">All</button>`
    : '';
  const chips = defined.map(({t,i}) => {
    const c = TAG_COLORS[i];
    const isActive = shoppingTagFilter === i;
    return `<span class="tag-filter-chip${isActive?' active':''}"
      style="${isActive?`background:${c.bg};border-color:${c.border};color:${c.text}`:''}"
      onclick="setShoppingTagFilter(${i},this)">
      ${esc(t)}
    </span>`;
  }).join('');
  const addBtn = hasRoom
    ? `<button class="btn-add-tag" onclick="showAddTagInput('shopping-tag-filter-bar')">+ Tag</button>`
    : '';
  bar.innerHTML = label + allChip + chips + addBtn;
}

function showAddTagInput(barId) {
  const bar = document.getElementById(barId);
  if (!bar) return;
  const addBtn = bar.querySelector('.btn-add-tag');
  if (!addBtn) return;
  const inp = document.createElement('input');
  inp.className = 'tag-inline-input';
  inp.placeholder = 'Tag name…';
  inp.maxLength = 20;
  addBtn.replaceWith(inp);
  inp.focus();

  let committed = false;
  const commit = () => {
    if (committed) return;
    committed = true;
    const val = inp.value.trim();
    if (val) addTag(val);
    else { buildTagFilterBar(); buildShoppingTagFilterBarInline(); }
  };
  inp.addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); commit(); }
    if (e.key === 'Escape') { committed = true; buildTagFilterBar(); buildShoppingTagFilterBarInline(); }
  });
  inp.addEventListener('blur', commit);
}

async function addTag(name) {
  if (!isOwner()) { toast("Settings are read-only"); return; }
  const tags = getCustomTags();
  const firstEmpty = tags.findIndex(t => !t || !t.trim());
  if (firstEmpty === -1) return;
  tags[firstEmpty] = name;
  settings.customTags = tags;
  await _saveSettings();
  buildTagFilterBar();
  buildShoppingTagFilterBarInline();
  renderGrid();
  _syncQueue.enqueue();
}

async function deleteTag(index) {
  if (!isOwner()) { toast("Settings are read-only"); return; }
  if (!confirm(`Remove tag "${getCustomTags()[index]}"? Items will lose this tag.`)) return;
  const tags = getCustomTags();
  tags[index] = '';
  settings.customTags = tags;
  items.forEach(item => {
    if (item.tags) item.tags = item.tags.filter(t => t !== index);
  });
  if (activeTagFilter === index) activeTagFilter = null;
  if (shoppingTagFilter === index) shoppingTagFilter = null;
  await _saveSettings();
  await saveData();
  buildTagFilterBar();
  buildShoppingTagFilterBarInline();
  renderGrid();
  _syncQueue.enqueue();
}

function setTagFilter(index, btn) {
  activeTagFilter = index;
  buildTagFilterBar();
  renderGrid();
}

function buildTagSettingsRows() {} // no-op — tags now managed inline



async function fetchProductImage() {
  const name = document.getElementById('f-name').value.trim();
  const url  = document.getElementById('f-url').value.trim();
  if (!name && !url) { alert('Enter a product name first.'); return; }

  const btn    = document.getElementById('fetch-img-btn');
  const wrap   = document.getElementById('img-preview-wrap');
  const status = document.getElementById('img-preview-status');
  const preview = document.getElementById('img-preview');

  btn.textContent = '⏳ Searching…';
  btn.disabled = true;
  status.textContent = '';
  wrap.style.display = 'none';

  const imageUrl = await findProductImage(name || url);

  btn.textContent = 'Find Image';
  btn.disabled = false;

  if (imageUrl) {
    pendingImageUrl = imageUrl;
    preview.src = imageUrl;
    preview.onerror = () => {
      status.textContent = '<svg class="icon" aria-hidden="true"><use href="#i-alert-triangle"></use></svg> Image may not display cross-origin';
      status.style.color = 'var(--warn)';
    };
    status.textContent = '✓ Image found';
    status.style.color = 'var(--ok)';
    wrap.style.display = 'flex';
  } else {
    pendingImageUrl = null;
    wrap.style.display = 'none';
    toast('No image found — try a more specific product name');
  }
}

async function findProductImage(query) {
  // 1. Open Food Facts — best for food, drink, household consumables
  try {
    const offUrl = `https://world.openfoodfacts.org/cgi/search.pl?search_terms=${encodeURIComponent(query)}&search_simple=1&action=process&json=1&page_size=5&fields=image_url,image_front_url,product_name`;
    const res = await fetch(offUrl, { signal: AbortSignal.timeout(6000) });
    if (res.ok) {
      const data = await res.json();
      const products = data.products || [];
      for (const p of products) {
        const img = p.image_front_url || p.image_url;
        if (img && img.startsWith('http')) return img;
      }
    }
  } catch(e) {}

  // 2. Open Beauty Facts — covers toiletries, cleaning products, cosmetics
  try {
    const obfUrl = `https://world.openbeautyfacts.org/cgi/search.pl?search_terms=${encodeURIComponent(query)}&search_simple=1&action=process&json=1&page_size=5&fields=image_url,image_front_url`;
    const res = await fetch(obfUrl, { signal: AbortSignal.timeout(6000) });
    if (res.ok) {
      const data = await res.json();
      const products = data.products || [];
      for (const p of products) {
        const img = p.image_front_url || p.image_url;
        if (img && img.startsWith('http')) return img;
      }
    }
  } catch(e) {}

  // 3. DuckDuckGo image search as fallback — works for anything not in the above databases
  try {
    const ddgUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(`https://duckduckgo.com/?q=${encodeURIComponent(query + ' product')}&iax=images&ia=images&format=json`)}`;
    const res = await fetch(ddgUrl, { signal: AbortSignal.timeout(8000) });
    if (res.ok) {
      const wrapper = await res.json();
      const html = wrapper.contents || '';
      // DDG embeds image data in a JSON block in the page
      const match = html.match(/"height":\d+,"image":"(https?:[^"]+)","source"/);
      if (match && match[1]) return match[1].replace(/\\\//g, '/');
    }
  } catch(e) {}

  return null;
}

function clearProductImage() {
  pendingImageUrl = null;
  document.getElementById('img-preview-wrap').style.display = 'none';
  document.getElementById('img-preview').src = '';
}

function showImagePreview(imageUrl, statusText) {
  showModalImagePreview(imageUrl);
  if (statusText) {
    const status = document.getElementById('img-preview-status');
    if (status) { status.textContent = '<svg class="icon" aria-hidden="true"><use href="#i-check"></use></svg> ' + statusText; status.style.color = 'var(--ok)'; }
  }
}

function showModalImagePreview(imageUrl) {
  if (!imageUrl) { clearProductImage(); return; }
  pendingImageUrl = imageUrl;
  const preview = document.getElementById('img-preview');
  const wrap = document.getElementById('img-preview-wrap');
  const status = document.getElementById('img-preview-status');
  preview.src = imageUrl;
  status.textContent = '✓ Saved image';
  status.style.color = 'var(--ok)';
  wrap.style.display = 'flex';
}



function parsePriceValue(priceStr) {
  if (!priceStr) return null;
  const num = parseFloat(String(priceStr).replace(/[^0-9.]/g, ''));
  return isNaN(num) ? null : num;
}

function getPriceHistory(item) {
  if (!item.logs || !item.logs.length) return [];
  return item.logs
    .map(l => ({ date: l.date, price: parsePriceValue(l.price), raw: l.price, store: l.store }))
    .filter(l => l.price !== null)
    .slice(-6); // last 6 priced purchases
}

function getPriceTrend(item) {
  const history = getPriceHistory(item);
  if (history.length < 2) return null;
  const last = history[history.length - 1].price;
  const prev = history[history.length - 2].price;
  const diff = last - prev;
  const pct  = Math.round(Math.abs(diff) / prev * 100);
  if (Math.abs(diff) < 0.01) return { dir: 'flat', diff: 0, pct: 0, last, prev };
  return { dir: diff > 0 ? 'up' : 'down', diff, pct, last, prev };
}

function priceTrendHTML(item) {
  const history = getPriceHistory(item);
  if (!history.length) return '';
  const last = history[history.length - 1];
  const trend = getPriceTrend(item);
  const lastFormatted = last.raw || `£${last.price.toFixed(2)}`;

  let trendBadge = '';
  if (trend) {
    if (trend.dir === 'up')   trendBadge = `<span style="color:var(--danger);font-size:10px;font-family:var(--mono)">↑${trend.pct}%</span>`;
    if (trend.dir === 'down') trendBadge = `<span style="color:var(--ok);font-size:10px;font-family:var(--mono)">↓${trend.pct}%</span>`;
    if (trend.dir === 'flat') trendBadge = `<span style="color:var(--muted);font-size:10px;font-family:var(--mono)">→</span>`;
  }

  // Mini sparkline dots for last 4 prices
  const sparkline = history.slice(-4).length > 1 ? (() => {
    const prices = history.slice(-4).map(h => h.price);
    const min = Math.min(...prices), max = Math.max(...prices);
    const range = max - min || 1;
    return `<span style="display:inline-flex;align-items:flex-end;gap:2px;height:14px;margin-left:4px">
      ${prices.map(p => {
        const h = Math.round(4 + ((p - min) / range) * 9);
        const c = p === prices[prices.length-1] ? 'var(--accent)' : 'var(--border)';
        return `<span style="width:3px;height:${h}px;background:${c};border-radius:1px;display:inline-block"></span>`;
      }).join('')}
    </span>`;
  })() : '';

  return `<div style="display:flex;align-items:center;gap:5px;font-size:11px;color:var(--muted);margin-bottom:6px">
    <span style="font-family:var(--mono)">💰 ${esc(lastFormatted)}</span>
    ${trendBadge}${sparkline}
    ${history.length > 1 ? `<span style="opacity:0.5">(${history.length} prices)</span>` : ''}
  </div>`;
}

function getFrequencyAnalysis(item) {
  if (!item.logs || item.logs.length < 2) return null;
  const sorted = [...item.logs].sort((a,b) => new Date(a.date) - new Date(b.date));
  const gaps = [];
  for (let i = 1; i < sorted.length; i++) {
    const days = (new Date(sorted[i].date) - new Date(sorted[i-1].date)) / 86400000;
    if (days > 0) gaps.push(days);
  }
  if (!gaps.length) return null;
  const avgDays = gaps.reduce((a,b) => a+b, 0) / gaps.length;
  const avgMonths = avgDays / 30.5;
  const configuredMonths = item.months || 1;
  const ratio = avgMonths / configuredMonths;
  // Only suggest if meaningfully different (>25% off)
  const significant = ratio < 0.75 || ratio > 1.25;
  return { avgDays: Math.round(avgDays), avgMonths: parseFloat(avgMonths.toFixed(1)), configuredMonths, significant, purchases: sorted.length };
}

function frequencyInsightHTML(item) {
  const f = getFrequencyAnalysis(item);
  if (!f || !f.significant) return '';
  const actual = f.avgMonths < 1
    ? `~${Math.round(f.avgDays)} days`
    : `~${f.avgMonths} months`;
  const configured = f.configuredMonths === 1 ? '1 month' : `${f.configuredMonths} months`;
  const faster = f.avgMonths < f.configuredMonths;
  return `<div style="background:rgba(91,141,238,0.08);border:1px solid rgba(91,141,238,0.25);border-radius:8px;padding:8px 10px;margin-bottom:8px;font-size:11px;line-height:1.5;display:flex;align-items:flex-start;gap:8px">
    <span><svg class="icon" aria-hidden="true"><use href="#i-lightbulb"></use></svg></span>
    <span style="color:var(--muted)">
      Based on ${f.purchases} purchases, you actually restock every ${actual}
      — ${faster ? 'faster' : 'slower'} than your configured ${configured}.
      <button onclick="applyFrequencySuggestion('${item.id}',${f.avgMonths})" style="background:none;border:none;color:var(--accent2);cursor:pointer;font-size:11px;font-family:var(--sans);text-decoration:underline;padding:0;margin-left:4px">Update setting</button>
    </span>
  </div>`;
}

async function applyFrequencySuggestion(id, avgMonths) {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  const rounded = Math.max(0.5, Math.round(avgMonths * 2) / 2); // round to nearest 0.5
  item.months = rounded;
  await saveData();
  scheduleRender('grid');
  toast(`Updated to ${rounded} month${rounded !== 1 ? 's' : ''} per purchase ✓`);
  _syncQueue.enqueue();
}


const DOMAIN_STORE_MAP = {
  'amazon.co.uk': 'Amazon UK', 'amazon.com': 'Amazon US', 'amazon.ca': 'Amazon CA',
  'amazon.com.au': 'Amazon AU', 'amazon.de': 'Amazon DE', 'amazon.fr': 'Amazon FR',
  'amazon.es': 'Amazon ES', 'amazon.it': 'Amazon IT', 'amazon.nl': 'Amazon NL',
  'amazon.co.jp': 'Amazon JP',
  'costco.co.uk': 'Costco UK', 'costco.com': 'Costco US', 'costco.ca': 'Costco CA',
  'costco.com.au': 'Costco AU',
  'tesco.com': 'Tesco', 'tesco.ie': 'Tesco IE',
  'sainsburys.co.uk': 'Sainsbury\'s',
  'ocado.com': 'Ocado',
  'waitrose.com': 'Waitrose',
  'asda.com': 'ASDA',
  'morrisons.com': 'Morrisons',
  'walmart.com': 'Walmart US', 'walmart.ca': 'Walmart CA',
  'target.com': 'Target',
  'woolworths.com.au': 'Woolworths',
  'coles.com.au': 'Coles',
  'rewe.de': 'REWE', 'dm.de': 'dm',
  'carrefour.fr': 'Carrefour',
  'bol.com': 'Bol.com',
  'ah.nl': 'Albert Heijn',
  'dunnesstoresgrocery.com': 'Dunnes',
  'elcorteingles.es': 'El Corte Inglés',
  'ica.se': 'ICA',
  'rakuten.co.jp': 'Rakuten',
  'iherb.com': 'iHerb',
  'hollandandbarrett.com': 'Holland & Barrett',
  'boots.com': 'Boots',
  'superdrug.com': 'Superdrug',
  'whsmith.co.uk': 'WHSmith',
  'johnlewis.com': 'John Lewis',
  'marks-and-spencer.com': 'M&S', 'marksandspencer.com': 'M&S',
};

function urlToStoreName(url) {
  if (!url || !url.trim()) return '';
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, '');
    // Exact match first
    if (DOMAIN_STORE_MAP[hostname]) return DOMAIN_STORE_MAP[hostname];
    // Partial match — find any key that the hostname ends with
    const match = Object.keys(DOMAIN_STORE_MAP).find(k => hostname.endsWith(k));
    if (match) return DOMAIN_STORE_MAP[match];
    // Fallback: capitalise the second-level domain
    const parts = hostname.split('.');
    const sld = parts.length >= 2 ? parts[parts.length - 2] : parts[0];
    return sld.charAt(0).toUpperCase() + sld.slice(1);
  } catch(e) { return ''; }
}

function autoFillStore() {
  const url = document.getElementById('f-url').value.trim();
  const storeField = document.getElementById('f-store');
  // Only auto-fill if the field is empty or was previously auto-filled
  if (!storeField.dataset.manual) {
    const detected = urlToStoreName(url);
    storeField.value = detected;
    storeField.dataset.autoFilled = detected ? '1' : '';
  }
}


window._reportHousehold = '__all__';

async function setReportHousehold(key) {
  window._reportHousehold = key;
  if (key !== '__all__' && key !== activeProfile) {
    // Load that household's items temporarily for the report view
    const profiles = await getProfiles();
    const profile  = profiles[key];
    if (profile) {
      // Temporarily override items for report rendering
      const savedItems = items;
      items = profile.items || [];
      renderReport();
      items = savedItems;
      return;
    }
  }
  renderReport();
}

function renderReport() {
  const threshold = settings.threshold;
  document.getElementById('report-date').textContent = 'Generated: ' + new Date().toLocaleDateString('en-GB',{weekday:'long',year:'numeric',month:'long',day:'numeric'});
  renderPendingDeliveries();

  // Render household tabs if multiple households exist
  getProfiles().then(profiles => {
    const keys = Object.keys(profiles);
    const tabsEl = document.getElementById('report-household-tabs');
    if (!tabsEl) return;
    if (keys.length <= 1) { tabsEl.style.display = 'none'; return; }
    tabsEl.style.display = 'flex';
    tabsEl.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px';
    tabsEl.innerHTML = [{ key: '__all__', name: 'All Households', colour: '#7880a0' },
      ...keys.map(k => ({ key: k, name: profiles[k]?.name || k, colour: profiles[k]?.colour || '#e8a838' }))
    ].map(({ key, name, colour }) => {
      const active = (window._reportHousehold || '__all__') === key;
      return `<button onclick="setReportHousehold('${key}')"
        style="padding:6px 14px;border-radius:99px;border:2px solid ${active ? colour : 'var(--border)'};
               background:${active ? colour + '22' : 'transparent'};color:${active ? colour : 'var(--muted)'};
               font-size:12px;font-weight:${active ? '700' : '400'};cursor:pointer;transition:all 0.2s">
        ${esc(name)}
      </button>`;
    }).join('');
  });

  // Determine which items to report on
  const reportItems = items; // current household items (or will be combined below)

  const groups = { critical:[], warn:[], ok:[], nodata:[] };
  reportItems.forEach(item => {
    const s = calcStock(item);
    const status = getStatus(s?.pct??null, threshold);
    groups[status].push({item, pct:s?.pct??null, daysLeft:s?.daysLeft??null});
  });

  const needBuy = [...groups.critical, ...groups.warn];
  let html = '';

  if (needBuy.length) {
    html += `<div id="need-to-buy"><h3><svg class="icon" aria-hidden="true"><use href="#i-alert-triangle"></use></svg> BUY BEFORE YOU RUN OUT</h3><div class="buy-chips">
      ${needBuy.map(({item,daysLeft}) => `<span class="buy-chip">${esc(item.name)} <span style="color:${daysLeft<7?'#e85050':'#e8a838'};font-family:var(--mono)">(${daysLeft??'?'}d)</span></span>`).join('')}
    </div></div>`;
  }

  const makeTable = entries => {
    if (!entries.length) return '<p style="color:var(--muted);font-size:13px;padding:10px 0">None</p>';
    return `<table class="report-table">
      <thead><tr><th>Item</th><th>Category</th><th>Rating</th><th>Last Price</th><th>Est. %</th><th>Days Left</th><th>Buy</th></tr></thead>
      <tbody>${entries.map(({item,pct,daysLeft}) => {
        const history = getPriceHistory(item);
        const trend = getPriceTrend(item);
        const lastPrice = history.length ? history[history.length-1].raw || `£${history[history.length-1].price.toFixed(2)}` : '—';
        const trendStr = trend ? (trend.dir==='up'?` <span style="color:#e85050">↑${trend.pct}%</span>`:trend.dir==='down'?` <span style="color:#4cbb8a">↓${trend.pct}%</span>`:'') : '';
        return `
        <tr>
          <td><strong>${esc(item.name)}</strong></td>
          <td style="color:var(--muted)">${esc(item.category||'Other')}</td>
          <td>${starsHTML(item.rating)}</td>
          <td style="font-family:var(--mono);font-size:12px">${esc(lastPrice)}${trendStr}</td>
          <td style="font-family:var(--mono);font-weight:700;color:${STATUS_COLOR[getStatus(pct,threshold)]}">${pct!==null?pct+'%':'—'}</td>
          <td style="font-family:var(--mono)">${daysLeft!==null?daysLeft+'d':'—'}</td>
          <td>${item.url?`<a href="${esc(item.url)}" target="_blank" rel="noopener" style="color:var(--accent2);font-size:12px;text-decoration:none">Buy ↗</a>`:'—'}</td>
        </tr>`;}).join('')}
      </tbody></table>`;
  };

  const section = (title, color, entries) => `
    <div class="report-section">
      <h3 style="color:${color};border-color:${color}44">${title} (${entries.length})</h3>
      ${makeTable(entries)}
    </div>`;

  html += section('<svg class="icon" aria-hidden="true"><use href="#i-circle"></use></svg> Critical — Order Now', '#e85050', groups.critical);
  html += section('<svg class="icon" aria-hidden="true"><use href="#i-circle"></use></svg> Getting Low — Order Soon', '#e8a838', groups.warn);
  html += section('<svg class="icon" aria-hidden="true"><use href="#i-circle"></use></svg> Well Stocked', '#4cbb8a', groups.ok);
  if (groups.nodata.length) html += section('<svg class="icon" aria-hidden="true"><use href="#i-circle"></use></svg> No Purchase Data', '#7880a0', groups.nodata);

  document.getElementById('report-content').innerHTML = html || '<p style="color:var(--muted);text-align:center;padding:60px">No items yet.</p>';
}

// ═══════════════════════════════════════════
//  MONTHLY SPEND TRACKER
// ═══════════════════════════════════════════
let spendVisible = false;

function toggleSpendView() {
  spendVisible = !spendVisible;
  const section = document.getElementById('spend-section');
  const btn     = document.getElementById('spend-toggle-btn');
  if (section) section.style.display = spendVisible ? 'block' : 'none';
  if (btn)     btn.style.background  = spendVisible ? 'var(--surface2)' : '';
  if (spendVisible) renderSpendChart();
}

function getMonthlySpend() {
  // Aggregate all log entries by month
  const byMonth = {};
  items.forEach(item => {
    (item.logs || []).forEach(log => {
      if (!log.date || !log.price) return;
      const val = parsePriceValue(log.price);
      if (val === null) return;
      const key = log.date.slice(0, 7); // YYYY-MM
      if (!byMonth[key]) byMonth[key] = { total: 0, entries: [] };
      byMonth[key].total += val;
      byMonth[key].entries.push({ item: item.name, price: val, raw: log.price, store: log.store });
    });
  });
  // Sort by month
  return Object.entries(byMonth)
    .sort(([a], [b]) => a.localeCompare(b))
    .slice(-12); // last 12 months
}

function calcPeriodSpend(days) {
  // Sum all logged purchases in the last N days that have prices
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
  let total = 0, priced = 0, unpriced = 0;
  const missingItems = [];
  items.forEach(item => {
    if (item._archived) return;
    let itemHadLog = false;
    (item.logs || []).forEach(log => {
      if (!log.date) return;
      const logMs = new Date(log.date).getTime();
      if (logMs < cutoff) return;
      itemHadLog = true;
      const val = parsePriceValue(log.price);
      if (val !== null) { total += val; priced++; }
      else unpriced++;
    });
    if (itemHadLog) {
      const latestLog = (item.logs || []).filter(l => {
        const ms = new Date(l.date).getTime();
        return ms >= cutoff;
      });
      const anyUnpriced = latestLog.some(l => parsePriceValue(l.price) === null);
      if (anyUnpriced && !missingItems.includes(item.name)) {
        missingItems.push(item.name);
      }
    }
  });
  return { total, priced, unpriced, missingItems };
}

function renderSpendChart() {
  const chartEl     = document.getElementById('spend-chart');
  const breakdownEl = document.getElementById('spend-breakdown');
  if (!chartEl || !breakdownEl) return;

  // ── 7 / 30 day spend summary ──
  const s7  = calcPeriodSpend(7);
  const s30 = calcPeriodSpend(30);
  const currency = '£';
  const hasAnySpend = s7.total > 0 || s30.total > 0;
  const allMissing  = [...new Set([...s7.missingItems, ...s30.missingItems])];

  let summaryHtml = '';
  if (hasAnySpend) {
    const missingNote = allMissing.length
      ? `<div style="margin-top:8px;font-size:11px;color:var(--muted);line-height:1.5">
          ⚠️ <strong>Estimate only</strong> — some purchases have no price logged:
          ${allMissing.slice(0,5).map(n=>`<em>${n}</em>`).join(', ')}${allMissing.length>5?' and more':''}.<br>
          Figures may not reflect actual spend due to missing prices and usage changes.
        </div>`
      : `<div style="margin-top:6px;font-size:11px;color:var(--muted)">Based on logged purchase prices. Actual spend may vary.</div>`;

    summaryHtml = `
      <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
        <div style="flex:1;min-width:120px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px 16px">
          <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Last 7 days</div>
          <div style="font-size:24px;font-weight:700;font-family:var(--mono);color:var(--text)">${currency}${s7.total.toFixed(2)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${s7.priced} purchase${s7.priced!==1?'s':''} logged</div>
        </div>
        <div style="flex:1;min-width:120px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px 16px">
          <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Last 30 days</div>
          <div style="font-size:24px;font-weight:700;font-family:var(--mono);color:var(--accent)">${currency}${s30.total.toFixed(2)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${s30.priced} purchase${s30.priced!==1?'s':''} logged</div>
        </div>
      </div>
      ${missingNote}
      ${allMissing.length ? '' : ''}`;
  }

  const data = getMonthlySpend();
  if (!data.length) {
    chartEl.innerHTML     = summaryHtml + `<p style="color:var(--muted);font-size:13px;margin-top:12px">No price data logged yet. Add prices when logging purchases to track spend over time.</p>`;
    breakdownEl.innerHTML = '';
    return;
  }
  chartEl.innerHTML = summaryHtml;

  const totals  = data.map(([, d]) => d.total);
  const maxVal  = Math.max(...totals);
  const W = 440, H = 180, PAD = { t: 16, r: 16, b: 40, l: 52 };
  const chartW  = W - PAD.l - PAD.r;
  const chartH  = H - PAD.t - PAD.b;
  const barW    = Math.min(36, (chartW / data.length) - 4);

  const bars = data.map(([month, d], i) => {
    const barH = maxVal > 0 ? (d.total / maxVal) * chartH : 0;
    const x    = PAD.l + (i / data.length) * chartW + (chartW / data.length - barW) / 2;
    const y    = PAD.t + chartH - barH;
    const isLatest = i === data.length - 1;
    const label = new Date(month + '-01').toLocaleDateString('en-GB', { month: 'short' });
    return `
      <rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="3" fill="${isLatest ? '#e8a838' : '#2e3350'}"/>
      <text x="${x + barW/2}" y="${y - 5}" text-anchor="middle" font-size="9" fill="${isLatest ? '#e8a838' : '#7880a0'}" font-family="monospace">${currency}${d.total.toFixed(0)}</text>
      <text x="${x + barW/2}" y="${H - PAD.b + 14}" text-anchor="middle" font-size="9" fill="#7880a0" font-family="monospace">${label}</text>`;
  }).join('');

  // avg line
  const avg  = totals.reduce((a, b) => a + b, 0) / totals.length;
  const avgY = PAD.t + chartH - (avg / maxVal) * chartH;
  const avgLine = `
    <line x1="${PAD.l}" y1="${avgY}" x2="${W - PAD.r}" y2="${avgY}" stroke="#7880a0" stroke-dasharray="4,3" stroke-width="1"/>
    <text x="${PAD.l - 4}" y="${avgY + 4}" text-anchor="end" font-size="9" fill="#7880a0" font-family="monospace">avg</text>`;

  // y-axis
  const yLabel = v => {
    const yy = PAD.t + chartH - (v / maxVal) * chartH;
    return `<text x="${PAD.l - 6}" y="${yy + 4}" text-anchor="end" font-size="9" fill="#7880a0" font-family="monospace">${currency}${v.toFixed(0)}</text>`;
  };
  const yLabels = [0, maxVal / 2, maxVal].map(yLabel).join('');

  chartEl.innerHTML += `
    <div style="font-size:13px;font-weight:700;color:var(--muted);margin-bottom:8px;margin-top:12px;font-family:var(--mono);text-transform:uppercase;letter-spacing:1px">Monthly trend</div>
    <svg viewBox="0 0 ${W} ${H}" style="width:100%;border-radius:8px;background:#1a1d27">
      ${yLabels}${avgLine}${bars}
    </svg>
    <div style="display:flex;gap:16px;margin-top:8px;font-size:11px;font-family:monospace;color:var(--muted)">
      <span>Total logged: ${currency}${totals.reduce((a,b)=>a+b,0).toFixed(2)}</span>
      <span>Monthly avg: ${currency}${avg.toFixed(2)}</span>
      <span style="color:#e8a838">This month: ${currency}${totals[totals.length-1].toFixed(2)}</span>
    </div>`;

  // Breakdown by category for latest month
  const [latestMonth, latestData] = data[data.length - 1];
  const byCat = {};
  latestData.entries.forEach(e => {
    const item = items.find(i => i.name === e.item);
    const cat  = item?.category || 'Other';
    if (!byCat[cat]) byCat[cat] = 0;
    byCat[cat] += e.price;
  });

  const catRows = Object.entries(byCat)
    .sort(([, a], [, b]) => b - a)
    .map(([cat, total]) => `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);font-size:13px">
        <span style="color:var(--muted)">${CATEGORY_EMOJI[cat]||'📦'} ${cat}</span>
        <span style="font-family:monospace;font-weight:700">${currency}${total.toFixed(2)}</span>
      </div>`).join('');

  const mLabel = new Date(latestMonth + '-01').toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });
  breakdownEl.innerHTML = `
    <div style="margin-top:16px;padding-top:14px;border-top:1px solid var(--border)">
      <div style="font-size:13px;font-weight:700;margin-bottom:10px">Breakdown — ${mLabel}</div>
      ${catRows || '<p style="font-size:13px;color:var(--muted)">No data for this month.</p>'}
    </div>`;
}


function exportReport() {
  renderReport();
  const win = window.open('','_blank');
  win.document.write(`<html><head><title>Stockroom Report</title><style>body{font-family:sans-serif;padding:20px;color:#111}table{width:100%;border-collapse:collapse}td,th{padding:10px;border-bottom:1px solid #eee;text-align:left}h3{margin:24px 0 10px}</style></head><body><h1>Stockroom Report</h1><p>${new Date().toLocaleDateString()}</p>${document.getElementById('report-content').innerHTML}</body></html>`);
  win.document.close(); win.print();
}

// ═══════════════════════════════════════════
//  EMAIL REPORT (mailto: — Part A)
// ═══════════════════════════════════════════
function buildEmailBody() {
  const threshold = settings.threshold;
  const now = Date.now();
  const dateStr = new Date().toLocaleDateString('en-GB', { weekday:'long', year:'numeric', month:'long', day:'numeric' });

  // Classify items
  const within7  = [];
  const within30 = [];
  const ok       = [];

  items.forEach(item => {
    const s = calcStock(item);
    if (!s) return;
    if (s.daysLeft <= 7)  within7.push({ item, ...s });
    else if (s.daysLeft <= 30) within30.push({ item, ...s });
    else ok.push({ item, ...s });
  });

  within7.sort((a,b)  => a.daysLeft - b.daysLeft);
  within30.sort((a,b) => a.daysLeft - b.daysLeft);

  const runOutDate = daysLeft => new Date(now + daysLeft * 86400000)
    .toLocaleDateString('en-GB', { day:'numeric', month:'short' });

  const stars = r => r ? '★'.repeat(r) + '☆'.repeat(5-r) : 'unrated';

  const formatSection = (entries, label) => {
    if (!entries.length) return `${label}\n(none)\n`;
    return `${label}\n${'─'.repeat(40)}\n` +
      entries.map(({item, daysLeft, pct}) => {
        const lines = [
          `• ${item.name}`,
          `  Runs out: ${runOutDate(daysLeft)} (${daysLeft} days, ${pct}% remaining)`,
        ];
        if (item.store) lines.push(`  Store: ${item.store}`);
        if (item.rating) lines.push(`  Rating: ${stars(item.rating)}`);
        if (item.url)   lines.push(`  Buy: ${item.url}`);
        return lines.join('\n');
      }).join('\n\n') + '\n';
  };

  const body = [
    `STOCKROOM REPORT — ${dateStr}`,
    `${'═'.repeat(50)}`,
    ``,
    `🔴 RUNNING OUT THIS WEEK (within 7 days) — ${within7.length} item${within7.length!==1?'s':''}`,
    formatSection(within7, ''),
    `🟡 RUNNING OUT THIS MONTH (8–30 days) — ${within30.length} item${within30.length!==1?'s':''}`,
    formatSection(within30, ''),
    `🟢 WELL STOCKED — ${ok.length} item${ok.length!==1?'s':''}`,
    ok.length ? ok.map(({item}) => `• ${item.name}`).join('\n') : '(none)',
    ``,
    `─────────────────────────────────────────`,
    `Total items tracked: ${items.length}`,
    `Generated by STOCKROOM`,
  ].join('\n');

  return body;
}

function emailReport() {
  const body   = buildEmailBody();
  const within7 = items.filter(i => { const s = calcStock(i); return s && s.daysLeft <= 7; }).length;
  const within30 = items.filter(i => { const s = calcStock(i); return s && s.daysLeft > 7 && s.daysLeft <= 30; }).length;
  const subject = `Stockroom Report — ${within7} urgent, ${within30} due soon`;
  const mailto  = `mailto:?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
  window.location.href = mailto;
}



async function rateItem(id, val) {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  item.rating = item.rating === val ? null : val;
  touchItem(item);
  await saveData();
  _syncQueue.enqueue();
  // Update stars in place — clear preview, set on
  const rating = item.rating || 0;
  document.querySelectorAll(`.card-star[data-id="${id}"]`).forEach(s => {
    s.classList.remove('preview');
    s.classList.toggle('on', parseInt(s.dataset.val) <= rating);
  });
  const label = document.getElementById('rl-' + id);
  if (label) {
    label.textContent = item.rating ? RATING_LABELS[item.rating] : 'Not rated';
    label.style.color = !item.rating ? 'var(--muted)' : item.rating <= 2 ? 'var(--danger)' : item.rating >= 4 ? 'var(--ok)' : 'var(--muted)';
  }
  currentRating = item.rating || 0;
}

function previewCardStars(id, val) {
  document.querySelectorAll(`.card-star[data-id="${id}"]`).forEach(s => {
    const n = parseInt(s.dataset.val);
    s.classList.toggle('preview', n <= val);
    s.classList.remove('on');
  });
  const item = items.find(i => i.id === id);
  const label = document.getElementById('rl-' + id);
  if (label) {
    label.textContent = RATING_LABELS[val] || '';
    label.style.color = val <= 2 ? 'var(--danger)' : val >= 4 ? 'var(--ok)' : 'var(--muted)';
  }
}

function resetCardStars(id) {
  const item = items.find(i => i.id === id);
  const rating = item?.rating || 0;
  document.querySelectorAll(`.card-star[data-id="${id}"]`).forEach(s => {
    const n = parseInt(s.dataset.val);
    s.classList.remove('preview');
    s.classList.toggle('on', n <= rating);
  });
  const label = document.getElementById('rl-' + id);
  if (label) {
    label.textContent = rating ? RATING_LABELS[rating] : 'Not rated';
    label.style.color = !rating ? 'var(--muted)' : rating <= 2 ? 'var(--danger)' : rating >= 4 ? 'var(--ok)' : 'var(--muted)';
  }
}


// ═══════════════════════════════════════════
//  PRICE HISTORY MODAL
// ═══════════════════════════════════════════
function openPriceHistoryModal(id) {
  const item = items.find(i => i.id === id);
  if (!item) return;
  const history = getPriceHistory(item);
  if (!history.length) return;

  document.getElementById('ph-title').innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-banknote"></use></svg> ' + esc(item.name);

  const prices  = history.map(h => h.price);
  const minP    = Math.min(...prices);
  const maxP    = Math.max(...prices);
  const avgP    = prices.reduce((a,b)=>a+b,0) / prices.length;
  const trend   = getPriceTrend(item);
  const lastP   = prices[prices.length-1];
  const currency = (history.at(-1)?.raw||'').replace(/[\d.,\s]/g,'').trim() || '£';

  // Subtitle: trend summary
  let trendDesc = 'Stable price';
  if (trend) {
    if (trend.dir === 'up')   trendDesc = `↑ Up ${trend.pct}% vs last purchase`;
    if (trend.dir === 'down') trendDesc = `↓ Down ${trend.pct}% vs last purchase`;
  }
  document.getElementById('ph-subtitle').textContent = `${history.length} purchase${history.length!==1?'s':''} · ${trendDesc}`;

  // ── SVG bar chart ──
  const W = 440, H = 160, PAD = { t:16, r:16, b:36, l:48 };
  const chartW = W - PAD.l - PAD.r;
  const chartH = H - PAD.t - PAD.b;
  const range  = maxP - minP || 1;
  const barW   = Math.min(40, (chartW / history.length) - 6);

  const bars = history.map((h, i) => {
    const x    = PAD.l + (i / history.length) * chartW + (chartW / history.length - barW) / 2;
    const barH = ((h.price - minP) / range) * (chartH * 0.75) + chartH * 0.15;
    const y    = PAD.t + chartH - barH;
    const isLow  = h.price === minP && prices.length > 1;
    const isHigh = h.price === maxP && prices.length > 1 && minP !== maxP;
    const isLast = i === history.length - 1;
    const fill   = isLow ? '#4cbb8a' : isHigh ? '#e85050' : isLast ? '#e8a838' : '#2e3350';
    const label  = `${currency}${h.price.toFixed(2)}`;
    const dateLabel = new Date(h.date+'T12:00:00').toLocaleDateString('en-GB',{day:'numeric',month:'short'});
    return `
      <rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="3" fill="${fill}"/>
      <text x="${x+barW/2}" y="${y-5}" text-anchor="middle" font-size="10" fill="${fill}" font-family="monospace">${label}</text>
      <text x="${x+barW/2}" y="${H-PAD.b+14}" text-anchor="middle" font-size="9" fill="#7880a0" font-family="monospace">${dateLabel}</text>`;
  }).join('');

  // avg line
  const avgY = PAD.t + chartH - (((avgP - minP) / range) * (chartH * 0.75) + chartH * 0.15);
  const avgLine = `
    <line x1="${PAD.l}" y1="${avgY}" x2="${W-PAD.r}" y2="${avgY}" stroke="#7880a0" stroke-dasharray="4,3" stroke-width="1"/>
    <text x="${PAD.l-4}" y="${avgY+4}" text-anchor="end" font-size="9" fill="#7880a0" font-family="monospace">avg</text>`;

  // y-axis labels
  const yLabels = [minP, avgP, maxP].map(p => {
    const y = PAD.t + chartH - (((p - minP) / range) * (chartH * 0.75) + chartH * 0.15);
    return `<text x="${PAD.l-6}" y="${y+4}" text-anchor="end" font-size="9" fill="#7880a0" font-family="monospace">${currency}${p.toFixed(2)}</text>`;
  }).join('');

  document.getElementById('ph-chart').innerHTML = `
    <svg viewBox="0 0 ${W} ${H}" style="width:100%;border-radius:8px;background:#1a1d27">
      ${yLabels}${avgLine}${bars}
    </svg>
    <div style="display:flex;gap:14px;margin-top:8px;font-size:11px;font-family:monospace">
      <span style="color:#4cbb8a">● Lowest: ${currency}${minP.toFixed(2)}</span>
      <span style="color:#7880a0">● Avg: ${currency}${avgP.toFixed(2)}</span>
      <span style="color:#e85050">● Highest: ${currency}${maxP.toFixed(2)}</span>
      <span style="color:#e8a838">● Latest: ${currency}${lastP.toFixed(2)}</span>
    </div>`;

  // ── Table ──
  document.getElementById('ph-table').innerHTML = `
    <div style="margin-top:16px;padding-top:14px;border-top:1px solid var(--border)">
      <table class="report-table">
        <thead><tr><th>Date</th><th>Store</th><th>Qty</th><th>Price</th><th>vs Prev</th></tr></thead>
        <tbody>${[...history].reverse().map((h,i,arr) => {
          const prev = arr[i+1];
          const prevP = prev ? prev.price : null;
          let vsEl = '—';
          if (prevP !== null) {
            const diff = h.price - prevP;
            const pct  = Math.round(Math.abs(diff)/prevP*100);
            vsEl = Math.abs(diff) < 0.01 ? '→' :
              diff > 0 ? `<span style="color:var(--danger)">↑${pct}%</span>` :
                         `<span style="color:var(--ok)">↓${pct}%</span>`;
          }
          const isLow  = h.price === minP && prices.length > 1;
          const isHigh = h.price === maxP && prices.length > 1 && minP !== maxP;
          return `<tr>
            <td style="font-family:monospace;font-size:12px">${fmtDate(h.date)}</td>
            <td style="color:var(--muted);font-size:12px">${esc(h.store||'—')}</td>
            <td style="font-family:monospace;font-size:12px">${history[history.length-1-i]?.qty||'—'}</td>
            <td style="font-family:monospace;font-weight:700;color:${isLow?'var(--ok)':isHigh?'var(--danger)':'var(--text)'}">${h.raw||currency+h.price.toFixed(2)}${isLow?' <svg class="icon" aria-hidden="true"><use href="#i-tag"></use></svg>':''}</td>
            <td style="font-size:12px">${vsEl}</td>
          </tr>`;
        }).join('')}</tbody>
      </table>
    </div>`;

  openModal('price-history-modal');
}


let stockCountId = null;

function openStockCountModal(id) {
  stockCountId = id;
  const item = items.find(i => i.id === id);
  if (!item) return;
  const last = item.logs?.at(-1);
  const totalPurchased = last?.qty || 1;

  document.getElementById('sc-subtitle').textContent = `How many units of "${item.name}" do you have left?`;
  document.getElementById('sc-explanation').textContent =
    `You last bought ${totalPurchased} unit${totalPurchased!==1?'s':''} on ${fmtDate(last?.date)}. ` +
    `Enter how many you have now — the app will calculate your actual consumption rate and project when you'll run out.`;
  document.getElementById('sc-remaining').value = item.stockCount != null ? item.stockCount : '';
  document.getElementById('sc-date').value = today();
  document.getElementById('sc-months').value = item.months || 1;
  document.getElementById('sc-preview').style.display = 'none';

  const remaining = document.getElementById('sc-remaining');
  const dateEl = document.getElementById('sc-date');
  const monthsEl = document.getElementById('sc-months');
  const preview = () => updateStockCountPreview(item);
  remaining.oninput = preview;
  dateEl.onchange = preview;
  monthsEl.oninput = preview;

  if (item.stockCount != null) updateStockCountPreview(item);
  openModal('stock-count-modal');
}

function updateStockCountPreview(item) {
  const remaining = parseFloat(document.getElementById('sc-remaining').value);
  const countDate = document.getElementById('sc-date').value;
  const monthsVal = parseFloat(document.getElementById('sc-months').value) || item.months || 1;
  const preview = document.getElementById('sc-preview');
  const previewText = document.getElementById('sc-preview-text');

  if (isNaN(remaining) || !countDate) { preview.style.display = 'none'; return; }

  const last = item.logs?.at(-1);
  const totalPurchased = last?.qty || 1;
  const daysSincePurchase = (Date.now() - new Date((last?.date||countDate)+'T12:00:00')) / 86400000;
  const used = totalPurchased - remaining;

  let daysLeft;
  if (used <= 0 || daysSincePurchase <= 0) {
    const totalDays = monthsVal * 30.5 * totalPurchased;
    daysLeft = Math.round(Math.max(0, totalDays - daysSincePurchase));
  } else {
    const ratePerDay = used / daysSincePurchase;
    daysLeft = ratePerDay > 0 ? Math.round(remaining / ratePerDay) : null;
  }

  const pct = Math.round(Math.max(0, Math.min(100, (remaining / totalPurchased) * 100)));
  const runOutDate = daysLeft != null
    ? new Date(Date.now() + daysLeft * 86400000).toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'})
    : 'unknown';

  let msg;
  if (used > 0 && daysSincePurchase > 0) {
    const ratePerDay = used / daysSincePurchase;
    msg = `Used ${used.toFixed(1)} of ${totalPurchased} in ${Math.round(daysSincePurchase)} days ` +
          `(${ratePerDay.toFixed(2)}/day). ` +
          `${remaining} left → runs out ~${runOutDate} (${daysLeft ?? '?'} days, ${pct}% remaining).`;
  } else {
    msg = `${remaining} of ${totalPurchased} units remaining (${pct}%) → estimated ${daysLeft ?? '?'} days left (${monthsVal} month${monthsVal!==1?'s':''}/unit).`;
  }

  previewText.textContent = msg;
  previewText.style.color = pct <= 15 ? 'var(--danger)' : pct <= 30 ? 'var(--warn)' : 'var(--ok)';
  preview.style.display = 'block';
}

async function saveStockCount() {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === stockCountId);
  if (!item) return;
  const val = document.getElementById('sc-remaining').value.trim();
  const countDate = document.getElementById('sc-date').value;
  const monthsVal = parseFloat(document.getElementById('sc-months').value);

  // Save months-per-unit if changed
  if (!isNaN(monthsVal) && monthsVal > 0 && monthsVal !== item.months) {
    item.months = monthsVal;
  }

  if (val === '') {
    item.stockCount = null;
    item.stockCountDate = null;
  } else {
    item.stockCount = parseFloat(val);
    item.stockCountDate = countDate || today();
  }

  touchItem(item);
  await saveData();
  closeModal('stock-count-modal');
  scheduleRender('grid', 'dashboard');
  toast(val === '' ? 'Stock count cleared' : 'Stock count updated ✓');
  _syncQueue.enqueue();
}

// ═══════════════════════════════════════════
//  SUBSCRIBE & SAVE
// ═══════════════════════════════════════════

const SNS_DISCOUNT      = 0.05;  // 5% when 4+ items
const SNS_MIN_PURCHASES = 3;     // need at least 3 purchases
const SNS_MIN_SPAN_DAYS = 60;    // spanning at least 60 days
const SNS_MIN_ITEMS     = 4;     // need 4+ to hit 5% tier

function isAmazonStore(store) {
  return store && /amazon/i.test(store);
}

function isAmazonUrl(url) {
  return url && /amazon\./i.test(url);
}

function getSnSLink(item) {
  // Deep link to Subscribe & Save on Amazon using the product URL
  if (!item.url) return null;
  try {
    const u = new URL(item.url);
    // Extract ASIN from Amazon URL
    const asinMatch = item.url.match(/\/([A-Z0-9]{10})(?:[/?]|$)/);
    if (!asinMatch) return item.url; // fallback to product page
    const asin   = asinMatch[1];
    const domain = u.hostname; // e.g. www.amazon.co.uk
    return `https://${domain}/dp/${asin}?th=1&psc=1&subscribeSave=1`;
  } catch(e) { return item.url; }
}

function analyseSnS() {
  // Returns array of qualifying items with savings data
  const results = [];

  items.forEach(item => {
    const isAmazon = isAmazonStore(item.store) || isAmazonUrl(item.url);
    if (!isAmazon) return;

    const logs = (item.logs || []).filter(l => l.date).sort((a,b) => new Date(a.date) - new Date(b.date));
    if (logs.length < SNS_MIN_PURCHASES) {
      // Tracking — not enough purchases yet
      if (logs.length >= 1) {
        results.push({ item, status: 'tracking', logs, purchaseCount: logs.length, spanDays: 0, avgMonthlySpend: null, annualSaving: null });
      }
      return;
    }

    const firstDate = new Date(logs[0].date + 'T12:00:00');
    const lastDate  = new Date(logs[logs.length-1].date + 'T12:00:00');
    const spanDays  = (lastDate - firstDate) / 86400000;

    if (spanDays < SNS_MIN_SPAN_DAYS) {
      results.push({ item, status: 'tracking', logs, purchaseCount: logs.length, spanDays, avgMonthlySpend: null, annualSaving: null });
      return;
    }

    // Calculate avg monthly spend from price logs
    const pricedLogs = logs.filter(l => parsePriceValue(l.price) !== null);
    let avgMonthlySpend = null;
    let annualSaving    = null;

    if (pricedLogs.length >= 1) {
      const avgPrice    = pricedLogs.reduce((s,l) => s + parsePriceValue(l.price), 0) / pricedLogs.length;
      const monthsSpan  = spanDays / 30.5;
      const buysPerMonth = logs.length / monthsSpan;
      avgMonthlySpend   = avgPrice * buysPerMonth;
      annualSaving      = avgMonthlySpend * 12 * SNS_DISCOUNT;
    }

    const status = item.subscribeAndSave ? 'active' : 'eligible';
    results.push({ item, status, logs, purchaseCount: logs.length, spanDays: Math.round(spanDays), avgMonthlySpend, annualSaving });
  });

  return results;
}

function renderSavingsView() {
  const all      = analyseSnS();
  const active   = all.filter(r => r.status === 'active');
  const eligible = all.filter(r => r.status === 'eligible');
  const tracking = all.filter(r => r.status === 'tracking');
  const qualifies = (active.length + eligible.length) >= SNS_MIN_ITEMS;

  const totalAnnualSaving = [...active, ...eligible]
    .reduce((s, r) => s + (r.annualSaving || 0), 0);
  const activeAnnualSaving = active
    .reduce((s, r) => s + (r.annualSaving || 0), 0);

  let html = '';

  if (!all.length) {
    html = `<div class="sns-empty">
      <div style="margin-bottom:16px;color:var(--accent)"><svg aria-hidden="true" style="width:48px;height:48px"><use href="#i-shopping-cart"></use></svg></div>
      <h3 style="font-size:18px;color:var(--text);margin-bottom:8px">No Amazon purchases found</h3>
      <p style="font-size:14px;line-height:1.7">Add items purchased from Amazon and log at least 3 purchases to see Subscribe &amp; Save opportunities.</p>
    </div>`;
    document.getElementById('savings-content').innerHTML = html;
    return;
  }

  // Summary card
  const qualifiesMsg = qualifies
    ? `<span style="color:var(--ok);font-weight:700">✓ You qualify for 5% off</span> — you have ${active.length + eligible.length} eligible Amazon items.`
    : `You need <strong>${SNS_MIN_ITEMS - (active.length + eligible.length)}</strong> more eligible item${(SNS_MIN_ITEMS - active.length - eligible.length) !== 1 ? 's' : ''} to reach the 5% discount tier.`;

  html += `<div class="sns-summary-card">
    ${totalAnnualSaving > 0 ? `<div class="sns-summary-stat"><div class="val">£${totalAnnualSaving.toFixed(2)}</div><div class="lbl">Est. annual saving</div></div>` : ''}
    ${activeAnnualSaving > 0 ? `<div class="sns-summary-stat"><div class="val" style="color:var(--accent2)">£${activeAnnualSaving.toFixed(2)}</div><div class="lbl">Already saving/yr</div></div>` : ''}
    <div class="sns-summary-stat"><div class="val" style="color:var(--warn)">${active.length + eligible.length}</div><div class="lbl">Eligible items</div></div>
    <div class="sns-summary-stat"><div class="val" style="color:var(--muted)">${tracking.length}</div><div class="lbl">Being tracked</div></div>
    <div style="flex:1;min-width:200px">
      <p style="font-size:13px;line-height:1.6;color:var(--muted)">${qualifiesMsg}</p>
      <p style="font-size:12px;color:var(--muted);margin-top:6px">Amazon Subscribe &amp; Save gives 5% off when you subscribe to 4+ products in a single delivery.</p>
    </div>
  </div>`;

  const makeItem = (r) => {
    const { item, status, purchaseCount, spanDays, avgMonthlySpend, annualSaving } = r;
    const snsLink  = getSnSLink(item);
    const lastPrice = (() => { const pl = (item.logs||[]).filter(l=>parsePriceValue(l.price)!==null); return pl.length ? pl[pl.length-1].price : null; })();

    const badge = status === 'active'
      ? `<span class="sns-badge sns-badge-active">✓ S&amp;S Active</span>`
      : status === 'eligible'
        ? `<span class="sns-badge sns-badge-eligible">💡 Eligible</span>`
        : `<span class="sns-badge sns-badge-tracking"><svg class="icon" aria-hidden="true"><use href="#i-timer"></use></svg> Tracking (${purchaseCount}/${SNS_MIN_PURCHASES} purchases)</span>`;

    const savingEl = annualSaving
      ? `<div class="sns-saving">£${annualSaving.toFixed(2)}/yr saving</div>`
      : lastPrice
        ? `<div style="font-size:12px;color:var(--muted)">Add prices to see £ saving</div>`
        : `<div style="font-size:12px;color:var(--muted)">Log prices to calculate saving</div>`;

    const toggleBtn = status !== 'tracking'
      ? `<button class="btn btn-sm ${status === 'active' ? 'btn-ghost' : 'btn-primary'}" onclick="toggleSnS('${item.id}')">${status === 'active' ? 'Mark as not subscribed' : '✓ Mark as subscribed'}</button>`
      : '';

    const snsBuyBtn = snsLink && status !== 'active'
      ? `<a href="${esc(snsLink)}" target="_blank" rel="noopener" class="btn btn-ghost btn-sm"><svg class="icon" aria-hidden="true"><use href="#i-link"></use></svg> Subscribe on Amazon</a>`
      : '';

    return `<div class="sns-item">
      <div class="sns-item-header">
        <div class="sns-item-name">${esc(item.name)}</div>
        ${badge}
      </div>
      <div class="sns-stats">
        <div class="sns-stat"><strong>${purchaseCount}</strong><span>purchases</span></div>
        ${spanDays ? `<div class="sns-stat"><strong>${Math.round(spanDays/30.5 * 10)/10}mo</strong><span>tracked</span></div>` : ''}
        ${lastPrice ? `<div class="sns-stat"><strong>${esc(lastPrice)}</strong><span>last price</span></div>` : ''}
        ${avgMonthlySpend ? `<div class="sns-stat"><strong>£${avgMonthlySpend.toFixed(2)}</strong><span>/month avg</span></div>` : ''}
      </div>
      ${savingEl}
      <div class="sns-actions">${toggleBtn}${snsBuyBtn}</div>
    </div>`;
  };

  if (eligible.length) {
    html += `<div class="sns-section-title">💡 Eligible — not yet subscribed (${eligible.length})</div>`;
    html += eligible.map(makeItem).join('');
  }
  if (active.length) {
    html += `<div class="sns-section-title">✓ Already subscribed (${active.length})</div>`;
    html += active.map(makeItem).join('');
  }
  if (tracking.length) {
    html += `<div class="sns-section-title"><svg class="icon" aria-hidden="true"><use href="#i-timer"></use></svg> Building history (${tracking.length})</div>`;
    html += tracking.map(makeItem).join('');
  }

  document.getElementById('savings-content').innerHTML = html;
}

async function toggleSnS(id) {
  if (!canWrite("savings")) { showLockBanner("savings"); return; }
  const item = items.find(i => i.id === id);
  if (!item) return;
  item.subscribeAndSave = !item.subscribeAndSave;
  await saveData();
  renderSavingsView();
  scheduleRender('sns');
  toast(item.subscribeAndSave ? 'Marked as subscribed ✓' : 'Marked as not subscribed');
  _syncQueue.enqueue();
}

function updateSnSBanner() {
  const banner = document.getElementById('sns-banner');
  if (!banner) return;
  const all      = analyseSnS();
  const eligible = all.filter(r => r.status === 'eligible');
  const active   = all.filter(r => r.status === 'active');
  const total    = eligible.length + active.length;

  if (total < 1) { banner.style.display = 'none'; return; }

  const qualifies = total >= SNS_MIN_ITEMS;
  const totalSaving = [...eligible, ...active].reduce((s,r) => s + (r.annualSaving||0), 0);
  const savingStr   = totalSaving > 0 ? ` — save <strong>£${totalSaving.toFixed(2)}/yr</strong>` : '';

  if (qualifies) {
    banner.innerHTML = `<div class="sns-banner" onclick="switchToSavings()">
      <div class="sns-banner-text">💰 You qualify for <strong>Amazon Subscribe &amp; Save 5%</strong> on ${total} items${savingStr}</div>
      <button class="btn btn-ghost btn-sm" style="flex-shrink:0">View savings →</button>
    </div>`;
  } else if (eligible.length > 0) {
    const needed = SNS_MIN_ITEMS - total;
    banner.innerHTML = `<div class="sns-banner" onclick="switchToSavings()">
      <div class="sns-banner-text">💡 <strong>${total} Amazon item${total!==1?'s':''}</strong> could be on Subscribe &amp; Save — ${needed} more needed for 5% discount${savingStr}</div>
      <button class="btn btn-ghost btn-sm" style="flex-shrink:0">View savings →</button>
    </div>`;
  } else {
    banner.style.display = 'none';
    return;
  }
  banner.style.display = 'block';
}

function switchToSavings() {
  const tab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Savings'));
  if (tab) showView('savings', tab);
}

// ═══════════════════════════════════════════
//  SHOPPING LIST
// ═══════════════════════════════════════════
let shoppingStores = new Set(['__all__']);
let shoppingDays = 30;
let shoppingTagFilter = null; // null = all tags

function updateStockShoppingHeader(mode) {
  const stockBtn    = document.getElementById('hdr-stock-btn');
  const shoppingBtn = document.getElementById('hdr-shopping-btn');
  const toggle      = document.getElementById('days-toggle');
  const countEl     = document.getElementById('item-count');
  if (mode === 'stock') {
    if (stockBtn)    { stockBtn.style.fontWeight = '700'; stockBtn.style.color = 'var(--text)'; }
    if (shoppingBtn) { shoppingBtn.style.fontWeight = '400'; shoppingBtn.style.color = 'var(--muted)'; }
    if (toggle)      toggle.style.display = 'none';
    if (countEl)     countEl.style.display = '';
  } else {
    if (stockBtn)    { stockBtn.style.fontWeight = '400'; stockBtn.style.color = 'var(--muted)'; }
    if (shoppingBtn) { shoppingBtn.style.fontWeight = '700'; shoppingBtn.style.color = 'var(--text)'; }
    if (toggle)      toggle.style.display = '';
    if (countEl)     countEl.style.display = 'none';
  }
}

// ── View Transitions helpers ──────────────────────────────
const TAB_ORDER = ['stock', 'grocery', 'notes', 'reminders', 'savings', 'report', 'account-security', 'settings'];
let _currentView = 'stock';

function _vtSupported() { return !!document.startViewTransition; }

function _doTransition(direction, fn) {
  if (!_vtSupported() || window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    fn(); return;
  }
  const content = document.getElementById('view-content');
  if (content) content.style.viewTransitionName = direction === 'back' ? 'main-view-back' : 'main-view';
  document.startViewTransition(() => { fn(); });
}

function setStockOnlyUI(visible) {
  const ids = ['health-dashboard', 'filter-toggle-btn', 'sort-select', 'tag-filter-bar', 'sns-banner', 'pending-deliveries-section', 'incomplete-section'];
  ids.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = visible ? '' : 'none';
  });
  // filter-panel visibility must respect filtersOpen state, not just show/hide blindly
  const filterPanel = document.getElementById('filter-panel');
  if (filterPanel) filterPanel.style.display = visible && filtersOpen ? 'flex' : 'none';
  const filterWrap = document.querySelector('#view-stock > div:nth-child(5)');
  if (filterWrap) filterWrap.style.display = visible ? '' : 'none';
}

function switchToStock() {
  _currentViewName = 'stock';
  _doTransition('back', () => {
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.getElementById('view-stock').classList.add('active');
    const stockTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Stockroom'));
    if (stockTab) stockTab.classList.add('active');
    document.getElementById('items-grid').style.display = '';
    document.getElementById('shopping-panel').style.display = 'none';
    setStockOnlyUI(true);
    updateStockShoppingHeader('stock');
    _currentView = 'stock';
  });
  if (_householdEnabled) pushPresence();
}

function switchToShopping() {
  _currentViewName = 'shopping';
  _doTransition('forward', () => {
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.getElementById('view-stock').classList.add('active');
    const stockTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Stockroom'));
    if (stockTab) stockTab.classList.add('active');
    document.getElementById('items-grid').style.display = 'none';
    document.getElementById('shopping-panel').style.display = '';
    setStockOnlyUI(false);
    renderShoppingList();
    updateStockShoppingHeader('shopping');
  });
  if (_householdEnabled) pushPresence();
}

function showShoppingListInline() { switchToShopping(); }

// Session-only tick-off for shopping items (doesn't affect stock data)
const _shoppingTicked = new Set();
function toggleShoppingTick(id, labelEl) {
  const ticked = _shoppingTicked.has(id);
  if (ticked) {
    _shoppingTicked.delete(id);
  } else {
    _shoppingTicked.add(id);
  }
  const box     = document.getElementById(`sl-tick-${id}`);
  const name    = document.getElementById(`sl-name-${id}`);
  const bar     = document.getElementById(`sl-bar-${id}`);
  const actions = document.getElementById(`sl-actions-${id}`);
  const card    = document.getElementById(`si-${id}`);
  if (!ticked) {
    // Now ticked
    if (box)     { box.style.background = 'var(--ok)'; box.style.borderColor = 'var(--ok)'; box.innerHTML = '<svg width="12" height="12" viewBox="0 0 12 12"><polyline points="2,6 5,9 10,3" stroke="#fff" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>'; }
    if (name)    { name.style.textDecoration = 'line-through'; name.style.color = 'var(--muted)'; }
    if (bar)     { bar.style.opacity = '0.3'; }
    if (actions) { actions.style.display = 'none'; }
    if (card)    { card.style.opacity = '0.55'; }
  } else {
    // Unticked
    if (box)     { box.style.background = ''; box.style.borderColor = 'var(--border)'; box.innerHTML = ''; }
    if (name)    { name.style.textDecoration = ''; name.style.color = ''; }
    if (bar)     { bar.style.opacity = ''; }
    if (actions) { actions.style.display = ''; }
    if (card)    { card.style.opacity = ''; }
  }
}

function showView(name, btn) {
  _currentViewName = name;
  const sectionMap = { grocery:'groceries', reminders:'reminders', savings:'savings', report:'report', stock:'stockroom', shopping:'stockroom' };
  const section    = sectionMap[name];
  if (section && !canView(section)) { showLockBanner(section); return; }
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  const targetView = document.getElementById('view-'+name);
  if (targetView) targetView.classList.add('active');
  if (btn) btn.classList.add('active');
  // Sync sidebar active state
  document.querySelectorAll('.app-nav-link').forEach(a => {
    a.classList.toggle('active', a.dataset.view === name);
  });
  if (name === 'report')    { renderReport(); if (spendVisible) renderSpendChart(); }
  if (name === 'reminders') renderReminders();
  if (name === 'shopping')  renderShoppingList();
  if (name === 'savings')   renderSavingsView();
  if (name === 'grocery')   renderGrocery();
  if (name === 'notes')     { renderNotes(); setTimeout(_maybeShowMfaPrompt, 600); }
  if (name === 'account-security') renderAccountSecurity();
  if (name === 'stock') {
    updateStockShoppingHeader('stock');
    setStockOnlyUI(true);
    document.getElementById('items-grid').style.display = '';
    document.getElementById('shopping-panel').style.display = 'none';
  }
  if (name === 'settings') {
    renderSettingsForUser();
    buildSettingsCountrySelect();
    if (kvConnected) { loadTrustedDevices(); loadPasskeys(); }
    initSettingsCollapsibles();
  }
  _currentView = name;
  if (_householdEnabled) pushPresence();
  updateFab(name);
  // Clear grocery done-slide when leaving grocery view
  if (name !== 'grocery') _hideGroceryDoneSlide?.();
}

// navTo — called by sidebar links (no btn element needed)
function navTo(name) {
  // Find the matching tab button if it exists (for mobile tab state)
  const tabBtn = [...document.querySelectorAll('.tab')].find(t => {
    const oc = t.getAttribute('onclick') || '';
    return oc.includes(`'${name}'`);
  });
  showView(name, tabBtn || { classList: { add: () => {}, remove: () => {} } });
}

// Update sidebar profile label and sync state


function getShoppingItems() {
  return items.filter(item => {
    const s = calcStock(item);
    if (!s) return false;
    if (s.daysLeft > shoppingDays) return false;
    // Store filter
    if (!shoppingStores.has('__all__')) {
      const itemStore = item.store && item.store.trim();
      if (!itemStore || !shoppingStores.has(itemStore)) return false;
    }
    // Tag filter
    if (shoppingTagFilter !== null) {
      if (!(item.tags || []).includes(shoppingTagFilter)) return false;
    }
    return true;
  }).map(item => {
    const s = calcStock(item);
    return { item, daysLeft: s.daysLeft, pct: s.pct };
  }).sort((a, b) => a.daysLeft - b.daysLeft);
}

function getAllStoresFromItems() {
  const storeSet = new Set();
  items.forEach(item => {
    if (item.store && item.store.trim()) storeSet.add(item.store.trim());
    (item.logs||[]).forEach(l => { if (l.store && l.store.trim()) storeSet.add(l.store.trim()); });
  });
  return [...storeSet].sort();
}

function openStorePickerModal() {
  const stores = getAllStoresFromItems();
  const container = document.getElementById('store-picker-options');

  // Build item counts per store
  const countPerStore = {};
  stores.forEach(s => {
    countPerStore[s] = items.filter(item => {
      const st = calcStock(item);
      return st && st.daysLeft <= shoppingDays && (
        (item.store && item.store.trim() === s) ||
        (item.logs||[]).some(l => l.store && l.store.trim() === s)
      );
    }).length;
  });

  // "All stores" row
  const allChecked = shoppingStores.has('__all__');
  let html = `<div class="store-picker-row" onclick="toggleStorePick('__all__',this)">
    <input type="checkbox" id="sp-all" ${allChecked?'checked':''} onclick="event.stopPropagation();toggleStorePick('__all__',this.closest('.store-picker-row'))">
    <label for="sp-all" onclick="event.preventDefault()">🌐 All Stores</label>
  </div>`;

  if (stores.length === 0) {
    html += `<p style="font-size:13px;color:var(--muted);padding:10px 0">No stores found. Add stores when logging purchases.</p>`;
  } else {
    html += stores.map(s => {
      const checked = shoppingStores.has('__all__') || shoppingStores.has(s);
      const id = 'sp-' + s.replace(/\s+/g,'_');
      const cnt = countPerStore[s] || 0;
      return `<div class="store-picker-row" onclick="toggleStorePick('${esc(s)}',this)">
        <input type="checkbox" id="${id}" ${checked?'checked':''} onclick="event.stopPropagation();toggleStorePick('${esc(s)}',this.closest('.store-picker-row'))">
        <label for="${id}" onclick="event.preventDefault()">${esc(s)}</label>
        <span class="store-item-count">${cnt} item${cnt!==1?'s':''} due</span>
      </div>`;
    }).join('');
  }

  container.innerHTML = html;

  openModal('store-picker-modal');
}

function toggleStorePick(store, row) {
  const cb = row.querySelector('input[type=checkbox]');
  if (store === '__all__') {
    shoppingStores = new Set(['__all__']);
    // uncheck all individual store boxes
    document.querySelectorAll('#store-picker-options input[type=checkbox]').forEach(c => {
      c.checked = c.id === 'sp-all';
    });
    if (cb) cb.checked = true;
  } else {
    // Deselect "all"
    shoppingStores.delete('__all__');
    const allCb = document.getElementById('sp-all');
    if (allCb) allCb.checked = false;

    if (cb) cb.checked = !cb.checked;
    if (cb && cb.checked) shoppingStores.add(store);
    else shoppingStores.delete(store);

    // If nothing selected, revert to all
    if (shoppingStores.size === 0) {
      shoppingStores.add('__all__');
      if (allCb) allCb.checked = true;
    }
  }
}


function setShoppingTagFilter(index, btn) {
  shoppingTagFilter = index;
  buildShoppingTagFilterBarInline();
  renderShoppingList();
}


function toggleShoppingDays() {
  shoppingDays = shoppingDays === 30 ? 7 : 30;
  document.getElementById('days-opt-30').classList.toggle('active', shoppingDays === 30);
  document.getElementById('days-opt-30').classList.toggle('faded', shoppingDays !== 30);
  document.getElementById('days-opt-7').classList.toggle('active', shoppingDays === 7);
  document.getElementById('days-opt-7').classList.toggle('faded', shoppingDays !== 7);
  renderShoppingList();
}

function applyShoppingList() {
  closeModal('store-picker-modal');
  showShoppingListInline();
}

function renderShoppingList() {
  buildShoppingTagFilterBarInline();
  const shoppingItems = getShoppingItems();
  const storeLabel = shoppingStores.has('__all__') ? 'All Stores' : [...shoppingStores].join(', ');
  const threshold = settings.threshold;

  document.getElementById('shopping-subtitle').textContent =
    `Items running out within ${shoppingDays} days · ${storeLabel}`;

  const pillsEl = document.getElementById('shopping-store-pills');
  const pillStores = shoppingStores.has('__all__') ? ['All Stores'] : [...shoppingStores];
  pillsEl.innerHTML = pillStores.map(s =>
    `<span style="font-size:12px;font-weight:600;padding:4px 12px;border-radius:99px;background:var(--surface2);border:1px solid var(--border);color:var(--text)">${esc(s)}</span>`
  ).join('');

  const container = document.getElementById('shopping-content');

  if (shoppingItems.length === 0) {
    container.innerHTML = getShoppingPresenceBar() + `<div class="shopping-empty">
      <div style="font-size:48px;margin-bottom:16px"><svg class="icon icon-xl" aria-hidden="true"><use href="#i-party-popper"></use></svg></div>
      <h3 style="font-size:18px;color:var(--text);margin-bottom:8px">You're all stocked up!</h3>
      <p style="font-size:14px;line-height:1.7">No items are running out within ${shoppingDays} days for the selected stores.</p>
      <button class="btn btn-ghost" style="margin-top:16px" onclick="openStorePickerModal()">Change filters</button>
    </div>`;
    return;
  }

  // Group by store
  const byStore = {};
  shoppingItems.forEach(({ item, daysLeft, pct }) => {
    const store = item.store || 'No store set';
    if (!byStore[store]) byStore[store] = [];
    byStore[store].push({ item, daysLeft, pct });
  });

  const q = encodeURIComponent;

  container.innerHTML = getShoppingPresenceBar() + Object.entries(byStore).map(([store, entries]) => {
    const itemsHTML = entries.map(({ item, daysLeft, pct }) => {
      const s = calcStock(item);
      const status = getStatus(s?.pct ?? null, threshold);
      const color = STATUS_COLOR[status];
      const fillColor = status === 'critical' ? '#e85050' : status === 'warn' ? '#e8a838' : '#4cbb8a';
      const urgencyColor = daysLeft <= 7 ? 'var(--danger)' : daysLeft <= 14 ? 'var(--warn)' : 'var(--muted)';
      const runOutDate = new Date(Date.now() + daysLeft * 86400000)
        .toLocaleDateString('en-GB', { day:'numeric', month:'short' });

      // Buy now link
      const buyLink = item.url
        ? `<a class="sl-btn sl-btn-buy" href="${esc(item.url)}" target="_blank" rel="noopener">🛒 Buy now</a>`
        : '';

      // Store search links (first 2 only to keep compact)
      const altLinks = getStores(settings.country).slice(0,2).map(st =>
        `<a class="sl-btn sl-btn-search" href="${st.url(q(item.name))}" target="_blank" rel="noopener">🔍 ${st.name.replace(/^[^\s]+\s/,'')}</a>`
      ).join('');

      // Ordered / Delivered buttons
      const orderedBtn = !item.ordered
        ? `<button class="sl-btn sl-btn-order" onclick="markOrdered('${item.id}')">📦 Ordered?</button>`
        : `<button class="sl-btn sl-btn-unorder" onclick="unmarkOrdered('${item.id}')">↩ Unmark</button>`;
      const deliveredBtn = `<button class="sl-btn sl-btn-delivered" onclick="openDeliveredModal('${item.id}')">✓ Delivered</button>`;

      const orderedBadge = item.ordered
        ? `<span style="font-size:10px;font-weight:700;padding:1px 6px;border-radius:99px;background:rgba(91,141,238,0.15);border:1px solid rgba(91,141,238,0.3);color:#5b8dee;font-family:var(--mono);flex-shrink:0">📦 Ordered</span>`
        : '';

      return `<div class="sl-item${item.ordered?' sl-ordered':''}" id="si-${item.id}" style="border-left:3px solid ${color}">
        <div class="sl-item-top">
          <label style="display:flex;align-items:center;gap:8px;cursor:pointer;flex:1;min-width:0" onclick="toggleShoppingTick('${item.id}',this)">
            <span class="sl-tick-box" id="sl-tick-${item.id}" style="flex-shrink:0;width:20px;height:20px;border-radius:5px;border:2px solid var(--border);display:inline-flex;align-items:center;justify-content:center;transition:all .15s"></span>
            <span class="sl-item-name" id="sl-name-${item.id}">${esc(item.name)}</span>
          </label>
          ${orderedBadge}
          <span class="sl-days" style="color:${urgencyColor}">${daysLeft}d · ${runOutDate}</span>
        </div>
        <div class="sl-bar-wrap" id="sl-bar-${item.id}">
          <div class="sl-bar"><div class="sl-bar-fill" style="width:${pct??0}%;background:${fillColor}"></div></div>
          <span class="sl-pct" style="color:${color}">${pct ?? '?'}%</span>
        </div>
        <div class="sl-actions" id="sl-actions-${item.id}">
          ${buyLink}${altLinks}${orderedBtn}${deliveredBtn}
        </div>
      </div>`;
    }).join('');

    return `<div class="shopping-store-section">
      <div class="shopping-store-header">
        <h3><svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg> ${esc(store)}</h3>
        <span class="shopping-store-count">${entries.length} item${entries.length!==1?'s':''}</span>
      </div>
      ${itemsHTML}
    </div>`;
  }).join('');
}

function toggleShoppingCheck(itemId, cb) {
  const el = document.getElementById('si-' + itemId);
  if (el) el.classList.toggle('checked', cb.checked);
}

function exportShoppingList() {
  const shoppingItems = getShoppingItems();
  if (!shoppingItems.length) { toast('Nothing to export'); return; }

  const lines = ['STOCKROOM — Shopping List', `Generated: ${new Date().toLocaleDateString('en-GB')}`, `Stores: ${shoppingStores.has('__all__')?'All':[...shoppingStores].join(', ')}`, `Window: Next ${shoppingDays} days`, '', '---', ''];

  const byStore = {};
  shoppingItems.forEach(({item, daysLeft}) => {
    const store = item.store || 'No store set';
    if (!byStore[store]) byStore[store] = [];
    byStore[store].push({item, daysLeft});
  });

  Object.entries(byStore).forEach(([store, entries]) => {
    lines.push(`## ${store}`);
    entries.forEach(({item, daysLeft}) => {
      const runOut = new Date(Date.now() + daysLeft * 86400000).toLocaleDateString('en-GB');
      lines.push(`[ ] ${item.name}`);
      lines.push(`    Runs out: ${runOut} (${daysLeft}d)`);
      if (item.url) lines.push(`    Buy: ${item.url}`);
      if (item.rating) lines.push(`    Rating: ${'★'.repeat(item.rating)}${'☆'.repeat(5-item.rating)}`);
      lines.push('');
    });
    lines.push('');
  });

  const blob = new Blob([lines.join('\n')], {type:'text/plain'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `shopping_list_${new Date().toISOString().split('T')[0]}.txt`;
  a.click();
}



// ═══════════════════════════════════════════════════════════════
//  ADD ITEM WIZARD
// ═══════════════════════════════════════════════════════════════
let _wizStep = 1;
let _wizRating = 0;
let _wizImageUrl = null;
let _wizReminders = []; // [{id, name, interval, unit}]

function openAddModal() {
  _wizStep = 1;
  _wizRating = 0;
  _wizImageUrl = null;
  _wizReminders = [];
  // Reset all fields
  const reset = { 'wiz-name':'', 'wiz-category':'Kitchen', 'wiz-cadence':'monthly',
    'wiz-qty':1, 'wiz-months':1, 'wiz-url':'', 'wiz-store':'', 'wiz-notes':'',
    'wiz-last-date':today(), 'wiz-last-qty':1, 'wiz-last-price':'', 'wiz-started-using':'',
    'wiz-price-search':'' };
  Object.entries(reset).forEach(([id, v]) => { const el = document.getElementById(id); if (el) el.value = v; });
  const storeEl = document.getElementById('wiz-store');
  if (storeEl) { storeEl.dataset.manual = ''; storeEl.dataset.autoFilled = ''; }
  wizSetRating(0);
  wizClearImage();
  document.getElementById('wiz-price-links').innerHTML = '';
  document.getElementById('wiz-reminders-list').innerHTML = '';
  _wizGotoStep(1);
  openModal('add-item-wizard-modal');
}

function _wizGotoStep(n) {
  _wizStep = n;
  [1,2,3,4].forEach(i => {
    const s = document.getElementById('wiz-step-' + i);
    if (s) s.style.display = i === n ? 'block' : 'none';
    const p = document.getElementById('wprog-' + i);
    if (p) { p.classList.toggle('active', i === n); p.classList.toggle('done', i < n); }
  });
  document.getElementById('wizard-step-label').textContent = `Step ${n} of 4`;
  document.getElementById('wiz-back-btn').style.display = n > 1 ? 'inline-flex' : 'none';
  const nextBtn = document.getElementById('wiz-next-btn');
  const skipBtn = document.getElementById('wiz-skip-btn');
  if (n === 4) {
    nextBtn.textContent = 'Add Item ✓';
    nextBtn.onclick = wizSave;
    skipBtn.style.display = 'none';
    _wizRenderTimeline();
  } else {
    nextBtn.textContent = 'Next →';
    nextBtn.onclick = wizNext;
    skipBtn.style.display = n === 2 || n === 3 ? 'inline-flex' : 'none';
  }
}

function wizNext() {
  if (_wizStep === 1) {
    const name = document.getElementById('wiz-name').value.trim();
    if (!name) { document.getElementById('wiz-name').focus(); toast('Please enter a name for this item'); return; }
  }
  if (_wizStep < 4) _wizGotoStep(_wizStep + 1);
}
function wizBack() { if (_wizStep > 1) _wizGotoStep(_wizStep - 1); }
function wizSkip() { if (_wizStep < 4) _wizGotoStep(_wizStep + 1); }

function wizAutoFillStore() {
  const urlEl = document.getElementById('wiz-url');
  const storeEl = document.getElementById('wiz-store');
  if (!urlEl || !storeEl) return;
  if (storeEl.dataset.manual) return;
  const s = urlToStoreName(urlEl.value);
  if (s) { storeEl.value = s; storeEl.dataset.autoFilled = '1'; }
}

function wizSetRating(n) {
  _wizRating = n;
  const stars = document.querySelectorAll('#wiz-star-rating .star');
  const label = document.getElementById('wiz-rating-label');
  stars.forEach((s, i) => s.classList.toggle('active', i < n));
  if (label) label.textContent = n ? (RATING_LABELS?.[n] || `${n} star${n>1?'s':''}`) : 'Not rated';
}
function wizPreviewStars(n) { document.querySelectorAll('#wiz-star-rating .star').forEach((s,i) => s.classList.toggle('active', i < n)); }
function wizResetStars() { wizSetRating(_wizRating); }
function wizUpdatePriceLinks() {
  const q = document.getElementById('wiz-price-search').value.trim();
  const container = document.getElementById('wiz-price-links');
  if (!container) return;
  if (typeof buildPriceLinks === 'function') container.innerHTML = buildPriceLinks(q);
  else if (typeof updatePriceLinks === 'function') updatePriceLinks('wiz-price-search', 'wiz-price-links');
}
async function wizFetchProductImage() {
  const name = document.getElementById('wiz-name').value.trim() || document.getElementById('wiz-price-search').value.trim();
  if (!name) { toast('Enter an item name first'); return; }
  if (typeof fetchProductImage !== 'function') return;
  // Delegate to existing function but redirect to wiz preview
  const btn = document.querySelector('#add-item-wizard-modal button[onclick="wizFetchProductImage()"]');
  if (btn) btn.disabled = true;
  try {
    const url = await _fetchProductImageUrl(name);
    if (url) { _wizImageUrl = url; wizShowImage(url); }
    else toast('No image found');
  } catch(e) { toast('Image search failed'); }
  if (btn) btn.disabled = false;
}
function wizShowImage(url) {
  const wrap = document.getElementById('wiz-img-preview-wrap');
  const img = document.getElementById('wiz-img-preview');
  if (wrap && img) { img.src = url; wrap.style.display = 'flex'; }
}
function wizClearImage() {
  _wizImageUrl = null;
  const wrap = document.getElementById('wiz-img-preview-wrap');
  if (wrap) wrap.style.display = 'none';
}

// ── Wizard step 3: reminders ──────────────────────────────
function wizAddReminder() {
  _wizReminders.push({ id: uid(), name: '', interval: 3, unit: 'months' });
  _wizRenderReminders();
}
function _wizRenderReminders() {
  const list = document.getElementById('wiz-reminders-list');
  if (!list) return;
  list.innerHTML = _wizReminders.map((r, i) => `
    <div class="repl-reminder-row" id="wizrem-${r.id}">
      <div style="flex:1;display:flex;flex-direction:column;gap:6px">
        <input type="text" placeholder="Name (e.g. Pete, Main filter)" value="${esc(r.name)}"
          style="width:100%" oninput="_wizReminders[${i}].name=this.value">
        <div style="display:flex;gap:6px;align-items:center">
          <span style="font-size:12px;color:var(--muted);white-space:nowrap">Replace every</span>
          <input type="number" min="1" max="365" value="${r.interval}" style="width:60px"
            oninput="_wizReminders[${i}].interval=parseInt(this.value)||1">
          <select style="flex:1" onchange="_wizReminders[${i}].unit=this.value">
            <option value="days" ${r.unit==='days'?'selected':''}>days</option>
            <option value="weeks" ${r.unit==='weeks'?'selected':''}>weeks</option>
            <option value="months" ${r.unit==='months'?'selected':''}>months</option>
          </select>
        </div>
      </div>
      <button onclick="_wizReminders.splice(${i},1);_wizRenderReminders()" style="background:none;border:none;cursor:pointer;color:var(--danger);padding:4px" title="Remove">
        <svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg>
      </button>
    </div>`).join('');
}

// ── Wizard step 4: timeline ────────────────────────────────
function _wizRenderTimeline() {
  const content = document.getElementById('wiz-timeline-content');
  if (!content) return;
  const name    = document.getElementById('wiz-name').value.trim() || 'This item';
  const months  = parseFloat(document.getElementById('wiz-months').value) || 1;
  const qty     = parseFloat(document.getElementById('wiz-last-qty').value) || 1;
  const refDate = document.getElementById('wiz-started-using').value
               || document.getElementById('wiz-last-date').value
               || today();
  const totalDays = Math.round(months * 30.5 * qty);
  const ref     = new Date(refDate + 'T12:00:00');
  const runOut  = new Date(ref.getTime() + totalDays * 86400000);
  const threshold = settings.threshold || 20;
  const warnDays  = Math.round(totalDays * threshold / 100);
  const warnDate  = new Date(runOut.getTime() - warnDays * 86400000);
  const fmtD = d => d.toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' });
  const daysDiff = d => Math.round((d - Date.now()) / 86400000);
  const relLabel = d => { const diff = daysDiff(d); return diff < 0 ? `${Math.abs(diff)}d ago` : diff === 0 ? 'today' : `in ${diff}d`; };

  const events = [
    { label: 'Started using', date: ref, color: '#5b8dee', icon: 'i-play' },
    { label: `${threshold}% warning threshold (order reminder)`, date: warnDate, color: 'var(--warn)', icon: 'i-bell' },
    { label: 'Estimated run-out', date: runOut, color: 'var(--danger)', icon: 'i-alert-triangle' },
  ];

  // Add replacement reminders to timeline
  _wizReminders.forEach(r => {
    const days = r.unit==='days' ? r.interval : r.unit==='weeks' ? r.interval*7 : r.interval*30.5;
    const replDate = new Date(ref.getTime() + Math.round(days)*86400000);
    events.push({ label: `Replace${r.name ? ` (${r.name})` : ''}`, date: replDate, color:'var(--ok)', icon:'i-repeat' });
  });

  events.sort((a,b) => a.date - b.date);

  content.innerHTML = `
    <div style="background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:16px">
      <div style="font-size:13px;font-weight:700;color:var(--text);margin-bottom:12px">${esc(name)}</div>
      <div style="display:flex;flex-direction:column;gap:10px">
        ${events.map(ev => `
          <div style="display:flex;align-items:center;gap:10px">
            <div style="width:32px;height:32px;border-radius:50%;background:${ev.color}22;border:1px solid ${ev.color}55;display:flex;align-items:center;justify-content:center;flex-shrink:0">
              <svg class="icon" aria-hidden="true" style="color:${ev.color};width:14px;height:14px"><use href="#${ev.icon}"></use></svg>
            </div>
            <div style="flex:1;min-width:0">
              <div style="font-size:12px;color:var(--muted)">${ev.label}</div>
              <div style="font-size:13px;font-weight:700;color:var(--text)">${fmtD(ev.date)} <span style="font-weight:400;color:var(--muted);font-size:12px">${relLabel(ev.date)}</span></div>
            </div>
          </div>`).join('')}
      </div>
    </div>
    <div style="display:flex;gap:16px;flex-wrap:wrap">
      <div style="flex:1;min-width:100px;background:var(--surface2);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:24px;font-weight:800;color:var(--text)">${totalDays}d</div>
        <div style="font-size:11px;color:var(--muted)">Total supply</div>
      </div>
      <div style="flex:1;min-width:100px;background:var(--surface2);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:24px;font-weight:800;color:var(--warn)">${warnDays}d</div>
        <div style="font-size:11px;color:var(--muted)">Warning at</div>
      </div>
      <div style="flex:1;min-width:100px;background:var(--surface2);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:24px;font-weight:800;color:var(--accent)">${Math.max(0,daysDiff(runOut))}d</div>
        <div style="font-size:11px;color:var(--muted)">Days left</div>
      </div>
    </div>`;
}

async function wizSave() {
  if (!canWrite('stockroom')) { showLockBanner('stockroom'); return; }
  const name = document.getElementById('wiz-name').value.trim();
  if (!name) { _wizGotoStep(1); document.getElementById('wiz-name').focus(); toast('Please enter an item name'); return; }
  const storeVal = document.getElementById('wiz-store').value.trim()
    || urlToStoreName(document.getElementById('wiz-url').value.trim());
  const lastDate  = document.getElementById('wiz-last-date').value;
  const lastQty   = parseFloat(document.getElementById('wiz-last-qty').value) || 1;
  const lastPrice = document.getElementById('wiz-last-price').value.trim();
  const startedUsing = document.getElementById('wiz-started-using').value || null;

  const newItem = {
    id:                   uid(),
    name,
    category:             document.getElementById('wiz-category').value,
    cadence:              document.getElementById('wiz-cadence').value,
    qty:                  parseFloat(document.getElementById('wiz-qty').value) || 1,
    months:               parseFloat(document.getElementById('wiz-months').value) || 1,
    url:                  document.getElementById('wiz-url').value.trim(),
    store:                storeVal,
    notes:                document.getElementById('wiz-notes').value.trim(),
    startedUsing,
    rating:               _wizRating || null,
    imageUrl:             _wizImageUrl || null,
    storePrices:          [],
    replacementReminders: _wizReminders.filter(r => r.interval > 0).map(r => ({ ...r, lastReplaced: startedUsing || lastDate || null })),
    logs:                 lastDate ? [{ id: uid(), date: lastDate, qty: lastQty, price: lastPrice, store: storeVal, pendingDelivery: false, deliveredDate: lastDate }] : [],
    updatedAt:            new Date().toISOString(),
  };
  // Backwards compat: copy first reminder to replacementInterval/Unit
  if (newItem.replacementReminders.length) {
    newItem.replacementInterval = newItem.replacementReminders[0].interval;
    newItem.replacementUnit     = newItem.replacementReminders[0].unit;
  }
  items.push(newItem);
  await saveData();
  closeModal('add-item-wizard-modal');
  scheduleRender('grid', 'dashboard', 'filters', 'shopping', 'sns');
  toast(`"${name}" added ✓`);
  _syncQueue.enqueue('Saving item…');
}



function openEditModal(id) {
  const item = items.find(i => i.id === id);
  if (!item) return;
  editingId = id;
  document.getElementById('item-modal-title').textContent = 'Item Details';
  document.getElementById('item-modal-subtitle').textContent = item.name;

  // Populate readonly summary
  document.getElementById('ro-category').textContent = item.category || 'Other';
  document.getElementById('ro-name').textContent = item.name;
  const metaParts = [];
  if (item.cadence === 'bulk') metaParts.push('Bulk');
  else metaParts.push('Monthly');
  if (item.qty) metaParts.push(`${item.qty} unit${item.qty !== 1 ? 's' : ''} per purchase`);
  if (item.months) metaParts.push(`${item.months} month${item.months !== 1 ? 's' : ''} supply`);
  const lastLog = (item.logs || []).filter(l => !l.pendingDelivery).at(-1);
  if (lastLog?.price) metaParts.push(`Last price: ${lastLog.price}`);
  document.getElementById('ro-meta').textContent = metaParts.join(' · ');
  document.getElementById('ro-store').innerHTML = item.store
    ? `<svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg> ${esc(item.store)}` : '';

  // ── Order history ──
  _renderEditOrderHistory(item);

  // ── Replacement reminders ──
  _renderEditReminders(item);

  // Show readonly, hide edit form
  document.getElementById('item-readonly-view').style.display = 'block';
  document.getElementById('item-edit-view').style.display = 'none';

  // Pre-populate edit form fields
  tempStorePrices = JSON.parse(JSON.stringify(item.storePrices || []));
  if (typeof renderTempStorePrices === 'function') renderTempStorePrices();
  if (tempStorePrices.length) document.getElementById('store-prices-section').style.display = 'block';
  document.getElementById('f-name').value = item.name;
  document.getElementById('f-category').value = item.category || 'Kitchen';
  document.getElementById('f-cadence').value = item.cadence || 'monthly';
  document.getElementById('f-qty').value = item.qty || 1;
  document.getElementById('f-months').value = item.months || 1;
  document.getElementById('f-url').value = item.url || '';
  pendingImageUrl = item.imageUrl || null;
  showModalImagePreview(item.imageUrl || null);
  const storeField = document.getElementById('f-store');
  storeField.value = item.store || urlToStoreName(item.url || '');
  storeField.dataset.manual = item.store ? '1' : '';
  storeField.dataset.autoFilled = '';
  document.getElementById('f-notes').value = item.notes || '';
  const expiryEl    = document.getElementById('f-expiry');
  const thresholdEl = document.getElementById('f-threshold');
  if (expiryEl)    expiryEl.value    = item.expiry || '';
  if (thresholdEl) thresholdEl.value = item.thresholdOverride != null ? item.thresholdOverride : '';
  // Legacy compat fields (hidden inputs)
  const last = (item.logs || []).filter(l => !l.pendingDelivery).at(-1);
  const flDate = document.getElementById('f-last-date'); if (flDate) flDate.value = last?.date || today();
  const flQty  = document.getElementById('f-last-qty');  if (flQty)  flQty.value  = last?.qty  || 1;
  const flPrice= document.getElementById('f-last-price');if (flPrice) flPrice.value= last?.price|| '';
  const flSU   = document.getElementById('f-started-using'); if (flSU) flSU.value = item.startedUsing || '';
  const flRI   = document.getElementById('f-replace-interval'); if (flRI) flRI.value = item.replacementInterval || '';
  const flRU   = document.getElementById('f-replace-unit'); if (flRU) flRU.value = item.replacementUnit || 'months';
  currentRating = item.rating || 0;
  renderStars();
  document.getElementById('price-search-input').value = item.name;
  updatePriceLinks();
  openModal('item-modal');
}

function _renderEditOrderHistory(item) {
  const logs = (item.logs || []).slice().reverse().slice(0, 5); // latest 5
  const container = document.getElementById('ro-order-history');
  if (!container) return;

  if (!logs.length) {
    container.innerHTML = `<p style="font-size:13px;color:var(--muted)">No orders yet.</p>`;
  } else {
    container.innerHTML = logs.map(l => {
      const stage = !l.pendingDelivery && l.deliveredDate && item.startedUsing ? 'using'
                  : !l.pendingDelivery && l.deliveredDate ? 'delivered'
                  : 'pending';
      const stageLabel = stage === 'using' ? 'In use' : stage === 'delivered' ? 'Delivered' : 'Ordered';
      const dateStr = l.deliveredDate || l.date;
      return `<div class="order-history-row">
        <span class="order-stage-badge ${stage}">${stageLabel}</span>
        <span style="flex:1;color:var(--muted);font-size:12px">${l.date ? fmt(l.date) : ''}${l.price ? ` · ${esc(l.price)}` : ''}${l.qty && l.qty !== 1 ? ` · ×${l.qty}` : ''}</span>
      </div>`;
    }).join('');
  }

  // Smart order button
  const btn = document.getElementById('ro-order-btn');
  if (!btn) return;
  const hasPending = (item.logs || []).some(l => l.pendingDelivery);
  const hasDelivered = (item.logs || []).some(l => !l.pendingDelivery && l.deliveredDate && !item.startedUsing);
  if (hasPending) {
    btn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-package-check"></use></svg> Mark as Delivered';
    btn.onclick = () => { closeModal('item-modal'); openOrderFlow(item.id, 'delivered'); };
    btn.style.background = 'rgba(76,187,138,0.12)'; btn.style.color = 'var(--ok)'; btn.style.borderColor = 'rgba(76,187,138,0.3)';
  } else if (hasDelivered) {
    btn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-play"></use></svg> Set Start Using Date';
    btn.onclick = () => { closeModal('item-modal'); openOrderFlow(item.id, 'startusing'); };
    btn.style.background = 'rgba(91,141,238,0.12)'; btn.style.color = '#5b8dee'; btn.style.borderColor = 'rgba(91,141,238,0.3)';
  } else {
    btn.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-clipboard-list"></use></svg> Log Purchase';
    btn.onclick = () => { closeModal('item-modal'); openOrderFlow(item.id, 'purchase'); };
    btn.style.background = ''; btn.style.color = ''; btn.style.borderColor = '';
  }
}

function _renderEditReminders(item) {
  const container = document.getElementById('ro-reminders-list');
  if (!container) return;
  const reminders = _getItemReminders(item);
  if (!reminders.length) {
    container.innerHTML = `<p style="font-size:13px;color:var(--muted)">No replacement reminders set.</p>`;
  } else {
    container.innerHTML = reminders.map(r => `
      <div class="order-history-row">
        <svg class="icon" aria-hidden="true" style="color:var(--ok)"><use href="#i-bell"></use></svg>
        <span style="flex:1;font-size:12px;color:var(--text)">${r.name ? esc(r.name) + ' — ' : ''}Every ${r.interval} ${r.unit}</span>
        ${r.lastReplaced ? `<span style="font-size:11px;color:var(--muted)">Last: ${fmt(r.lastReplaced)}</span>` : ''}
      </div>`).join('');
  }
}

// Get replacement reminders from item — always returns from the array (migrating legacy first)
function _getItemReminders(item) {
  const hadLegacy = !item.replacementReminders?.length && !!item.replacementInterval;
  _ensureReplRemindersArray(item);
  if (hadLegacy && item.replacementReminders?.length) {
    // Persist the migration so IDs are stable across page loads
    saveData().catch(() => {});
  }
  return item.replacementReminders || [];
}



function openLogModal(id) { openOrderFlow(id, 'purchase'); } // backward compat
function openLogPurchaseModal(id) { openOrderFlow(id, 'purchase'); } // backward compat
function openLogPurchaseModalLegacy(id) {
  // Legacy fallback for when new index.html hasn't been deployed
  loggingId = id;
  const item = items.find(i => i.id === id);
  if (!item || !document.getElementById('log-modal')) return;
  document.getElementById('log-modal-title').textContent = 'Log Purchase — ' + item.name;
  document.getElementById('log-date').value  = today();
  document.getElementById('log-qty').value   = item.qty || 1;
  document.getElementById('log-price').value = '';
  document.getElementById('log-store').value = item.store || urlToStoreName(item.url||'') || '';
  renderLogHistory(item);
  openModal('log-modal');
}

// ═══════════════════════════════════════════════════════════════
//  ORDER FLOW — unified Log Purchase / Delivered / Start Using
// ═══════════════════════════════════════════════════════════════
let _ofItemId   = null;
let _ofStage    = 'purchase'; // 'purchase' | 'delivered' | 'startusing'

function openOrderFlow(id, stage) {
  const item = items.find(i => i.id === id);
  if (!item) return;

  // Create order-flow-modal dynamically if index.html doesn't have it yet
  if (!document.getElementById('order-flow-modal')) {
    const backdrop = document.createElement('div');
    backdrop.className = 'modal-backdrop';
    backdrop.id = 'order-flow-modal';
    backdrop.innerHTML = `
      <div class="modal" style="max-width:460px">
        <h2 id="order-flow-title"><svg class="icon icon-md" aria-hidden="true"><use href="#i-shopping-cart"></use></svg> Log Purchase</h2>
        <p class="subtitle" id="order-flow-subtitle"></p>
        <div id="order-stage-purchase">
          <div class="form-grid">
            <div class="field"><label>Date purchased</label><input type="date" id="of-date"></div>
            <div class="field"><label>Quantity</label><input type="number" id="of-qty" min="0.1" step="0.1" value="1"></div>
            <div class="field"><label>Price paid <span style="font-weight:400;color:var(--muted)">(optional)</span></label><input type="text" id="of-price" placeholder="e.g. £12.99"></div>
            <div class="field"><label>Store <span style="font-weight:400;color:var(--muted)">(optional)</span></label><input type="text" id="of-store" placeholder="Amazon, Tesco…"></div>
          </div>
          <div id="of-purchase-history" style="margin-top:12px"></div>
        </div>
        <div id="order-stage-delivered" style="display:none">
          <div class="form-grid">
            <div class="field"><label>Delivery date</label><input type="date" id="of-delivered-date"></div>
            <div class="field"><label>Quantity delivered</label><input type="number" id="of-delivered-qty" min="0.1" step="0.1" value="1"></div>
          </div>
          <div style="margin-top:14px;padding:14px;background:var(--surface2);border-radius:10px;border:1px solid var(--border)">
            <label style="font-size:13px;font-weight:700;display:flex;align-items:center;gap:8px;cursor:pointer">
              <input type="checkbox" id="of-started-now" onchange="ofToggleStarted()" style="width:16px;height:16px">
              I'm starting to use this straight away
            </label>
            <div id="of-started-row" style="display:none;margin-top:10px">
              <div class="field"><label>Started using date</label><input type="date" id="of-started-date"></div>
            </div>
            <p id="of-started-hint" style="font-size:12px;color:var(--muted);margin-top:8px;line-height:1.5">If not using it immediately, leave this unchecked — you'll be prompted to set the date when you open it.</p>
          </div>
        </div>
        <div id="order-stage-startusing" style="display:none">
          <p style="font-size:14px;color:var(--text);margin-bottom:16px;line-height:1.6">When did you start using this item? This anchors the stock clock so your estimates stay accurate.</p>
          <div class="field"><label>Started using date</label><input type="date" id="of-startusing-date"></div>
        </div>
        <div class="modal-footer" style="margin-top:16px">
          <button class="btn btn-ghost" onclick="closeModal('order-flow-modal')">Cancel</button>
          <button class="btn btn-primary" id="of-save-btn" onclick="ofSave()">Save</button>
        </div>
      </div>`;
    document.body.appendChild(backdrop);
  }

  _ofItemId = id;

  // Auto-detect stage if not specified
  if (!stage) {
    const hasPending = (item.logs || []).some(l => l.pendingDelivery);
    const hasDeliveredNoStart = (item.logs || []).some(l => !l.pendingDelivery && l.deliveredDate) && !item.startedUsing;
    stage = hasPending ? 'delivered' : hasDeliveredNoStart ? 'startusing' : 'purchase';
  }
  _ofStage = stage;

  const titles = { purchase:'Log Purchase', delivered:'Mark as Delivered', startusing:'Set Start Using Date' };
  const icons  = { purchase:'i-shopping-cart', delivered:'i-package-check', startusing:'i-play' };
  document.getElementById('order-flow-title').innerHTML =
    `<svg class="icon icon-md" aria-hidden="true"><use href="#${icons[stage]}"></use></svg> ${titles[stage]}`;
  document.getElementById('order-flow-subtitle').textContent = item.name;

  // Show/hide stages
  document.getElementById('order-stage-purchase').style.display   = stage === 'purchase'   ? 'block' : 'none';
  document.getElementById('order-stage-delivered').style.display  = stage === 'delivered'  ? 'block' : 'none';
  document.getElementById('order-stage-startusing').style.display = stage === 'startusing' ? 'block' : 'none';

  const saveBtn = document.getElementById('of-save-btn');
  if (stage === 'purchase')   { saveBtn.textContent = 'Save Purchase'; }
  if (stage === 'delivered')  { saveBtn.textContent = 'Confirm Delivery'; }
  if (stage === 'startusing') { saveBtn.textContent = 'Save Date'; }

  if (stage === 'purchase') {
    document.getElementById('of-date').value  = today();
    document.getElementById('of-qty').value   = item.qty || 1;
    document.getElementById('of-price').value = '';
    document.getElementById('of-store').value = item.store || urlToStoreName(item.url || '') || '';
    // Show recent purchase history
    _renderOfHistory(item);
  }
  if (stage === 'delivered') {
    document.getElementById('of-delivered-date').value = today();
    const pendingLog = [...(item.logs || [])].reverse().find(l => l.pendingDelivery);
    document.getElementById('of-delivered-qty').value = pendingLog?.qty || item.qty || 1;
    document.getElementById('of-started-now').checked = false;
    document.getElementById('of-started-row').style.display = 'none';
    document.getElementById('of-started-hint').style.display = 'block';
    document.getElementById('of-started-date').value = today();
  }
  if (stage === 'startusing') {
    document.getElementById('of-startusing-date').value = item.startedUsing || today();
  }

  openModal('order-flow-modal');
}

function _renderOfHistory(item) {
  const hist = document.getElementById('of-purchase-history');
  if (!hist) return;
  const logs = (item.logs || []).filter(l => !l.pendingDelivery).slice(-3).reverse();
  if (!logs.length) { hist.innerHTML = ''; return; }
  hist.innerHTML = `<p style="font-size:11px;font-weight:700;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">Recent purchases</p>` +
    logs.map(l => `<div style="font-size:12px;color:var(--muted);padding:4px 0">${fmt(l.date)}${l.price ? ' · ' + esc(l.price) : ''}${l.qty && l.qty !== 1 ? ' · ×' + l.qty : ''}</div>`).join('');
}

function ofToggleStarted() {
  const checked = document.getElementById('of-started-now').checked;
  document.getElementById('of-started-row').style.display  = checked ? 'block' : 'none';
  document.getElementById('of-started-hint').style.display = checked ? 'none' : 'block';
}

async function ofSave() {
  if (!canWrite('stockroom')) { showLockBanner('stockroom'); return; }
  const item = items.find(i => i.id === _ofItemId);
  if (!item) return;

  if (_ofStage === 'purchase') {
    if (!item.logs) item.logs = [];
    item.logs.push({
      id:              uid(),
      date:            document.getElementById('of-date').value || today(),
      qty:             parseFloat(document.getElementById('of-qty').value) || 1,
      price:           document.getElementById('of-price').value.trim(),
      store:           document.getElementById('of-store').value.trim(),
      pendingDelivery: true,
    });
    item.logs.sort((a,b) => new Date(a.date) - new Date(b.date));
    item.ordered   = true;
    item.orderedAt = new Date().toISOString();
    touchField(item, 'logs', 'ordered', 'orderedAt');
    await saveData();
    closeModal('order-flow-modal');
    scheduleRender('grid', 'dashboard', 'shopping');
    setTimeout(syncAll, 400);
    toast('Purchase logged — tap the order button when it arrives ✓');

  } else if (_ofStage === 'delivered') {
    if (!item.logs) item.logs = [];
    const deliveryDate = document.getElementById('of-delivered-date').value || today();
    const qty          = parseFloat(document.getElementById('of-delivered-qty').value) || 1;
    const startedNow   = document.getElementById('of-started-now').checked;
    const startedDate  = startedNow ? (document.getElementById('of-started-date').value || deliveryDate) : null;
    const pendingIdx   = [...item.logs].map((l,i) => ({l,i})).reverse().find(({l}) => l.pendingDelivery)?.i;
    if (pendingIdx !== undefined) {
      item.logs[pendingIdx].pendingDelivery = false;
      item.logs[pendingIdx].deliveredDate   = deliveryDate;
      item.logs[pendingIdx].qty             = qty;
    } else {
      item.logs.push({ id: uid(), date: deliveryDate, qty, pendingDelivery: false, deliveredDate });
      item.logs.sort((a,b) => new Date(a.date) - new Date(b.date));
    }
    item.ordered = false; item.orderedAt = null;
    if (startedDate) item.startedUsing = startedDate;
    touchItem(item);
    await saveData();
    closeModal('order-flow-modal');
    scheduleRender('grid', 'dashboard', 'shopping');
    setTimeout(syncAll, 400);
    toast(startedDate ? 'Delivered and marked as in use ✓' : 'Delivery confirmed ✓');

  } else if (_ofStage === 'startusing') {
    const date = document.getElementById('of-startusing-date').value || today();
    item.startedUsing = date;
    touchField(item, 'startedUsing');
    await saveData();
    closeModal('order-flow-modal');
    scheduleRender('grid', 'dashboard');
    setTimeout(syncAll, 400);
    toast('Start date saved — stock clock updated ✓');
  }
}

// Unified card button — determines correct stage automatically
function openOrderFlowFromCard(id) { openOrderFlow(id, null); }

// ═══════════════════════════════════════════════════════════════
//  REPLACEMENT REMINDERS MODAL
// ═══════════════════════════════════════════════════════════════
let _replItemId = null;

function openReplRemindersModal(id) {
  const item = items.find(i => i.id === id);
  if (!item) return;

  // Migrate legacy single-reminder into array format before opening, and persist it
  const hadLegacy = !item.replacementReminders?.length && !!item.replacementInterval;
  _ensureReplRemindersArray(item);
  if (hadLegacy && item.replacementReminders?.length) {
    // Silently persist the migrated format so future opens use stable IDs
    saveData().then(() => _syncQueue.enqueue()).catch(() => {});
  }

  _replItemId = id;

  // Create modal dynamically if index.html doesn't have it yet
  if (!document.getElementById('repl-reminders-modal')) {
    const backdrop = document.createElement('div');
    backdrop.className = 'modal-backdrop';
    backdrop.id = 'repl-reminders-modal';
    backdrop.innerHTML = `
      <div class="modal" style="max-width:480px">
        <h2><svg class="icon icon-md" aria-hidden="true"><use href="#i-bell"></use></svg> Replacement Reminders</h2>
        <p class="subtitle" id="repl-reminders-subtitle"></p>
        <div id="repl-reminders-list" style="display:flex;flex-direction:column;gap:10px;margin-bottom:16px"></div>
        <button class="btn btn-ghost btn-sm" onclick="replAddReminder()" style="width:100%">
          <svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg> Add a reminder
        </button>
        <div class="modal-footer" style="margin-top:16px">
          <button class="btn btn-ghost" onclick="closeModal('repl-reminders-modal')">Close</button>
        </div>
      </div>`;
    document.body.appendChild(backdrop);
  }

  document.getElementById('repl-reminders-subtitle').textContent = item.name;
  _renderReplRemindersList(item);
  openModal('repl-reminders-modal');
}

function _renderReplRemindersList(item) {
  const list = document.getElementById('repl-reminders-list');
  if (!list) return;
  const reminders = _getItemReminders(item);
  if (!reminders.length) {
    list.innerHTML = `<p style="font-size:13px;color:var(--muted)">No reminders yet. Add one below.</p>`;
    return;
  }
  list.innerHTML = reminders.map((r, i) => `
    <div class="repl-reminder-row" id="replrem-${r.id}">
      <div style="flex:1;display:flex;flex-direction:column;gap:6px">
        <div style="display:flex;align-items:center;gap:8px">
          <svg class="icon" aria-hidden="true" style="color:var(--ok);flex-shrink:0"><use href="#i-bell"></use></svg>
          <input type="text" placeholder="Name (e.g. Pete, Main filter — optional)" value="${esc(r.name || '')}"
            style="flex:1" oninput="_replUpdateName('${r.id}',this.value)">
        </div>
        <div style="display:flex;gap:6px;align-items:center;padding-left:24px">
          <span style="font-size:12px;color:var(--muted);white-space:nowrap">Every</span>
          <input type="number" min="1" max="365" value="${r.interval}" style="width:60px"
            oninput="_replUpdateInterval('${r.id}',parseInt(this.value)||1)">
          <select style="flex:1" onchange="_replUpdateUnit('${r.id}',this.value)">
            <option value="days" ${r.unit==='days'?'selected':''}>days</option>
            <option value="weeks" ${r.unit==='weeks'?'selected':''}>weeks</option>
            <option value="months" ${r.unit==='months'?'selected':''}>months</option>
          </select>
        </div>
        ${r.lastReplaced ? `<div style="font-size:11px;color:var(--muted);padding-left:24px">Last replaced: ${fmt(r.lastReplaced)}</div>` : ''}
      </div>
      <button onclick="_replDeleteReminder('${r.id}')" style="background:none;border:none;cursor:pointer;color:var(--danger);padding:4px" title="Delete">
        <svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg>
      </button>
    </div>`).join('');
}

function _ensureReplRemindersArray(item) {
  if (!item.replacementReminders?.length) {
    if (item.replacementInterval) {
      item.replacementReminders = [{ id: uid(), name: '', interval: item.replacementInterval, unit: item.replacementUnit || 'months', lastReplaced: item.startedUsing || null }];
    } else {
      item.replacementReminders = [];
    }
  }
}

function replAddReminder() {
  const item = items.find(i => i.id === _replItemId);
  if (!item) return;
  _ensureReplRemindersArray(item);
  item.replacementReminders.push({ id: uid(), name: '', interval: 3, unit: 'months', lastReplaced: null });
  _saveReplReminders(item);
}

function _replUpdateName(rid, val) {
  const item = items.find(i => i.id === _replItemId);
  if (!item?.replacementReminders) return;
  const r = item.replacementReminders.find(r => r.id === rid);
  if (r) { r.name = val; _saveReplRemindersDebounced(item); }
}
function _replUpdateInterval(rid, val) {
  const item = items.find(i => i.id === _replItemId);
  if (!item?.replacementReminders) return;
  const r = item.replacementReminders.find(r => r.id === rid);
  if (r) { r.interval = val; _saveReplRemindersDebounced(item); }
}
function _replUpdateUnit(rid, val) {
  const item = items.find(i => i.id === _replItemId);
  if (!item?.replacementReminders) return;
  const r = item.replacementReminders.find(r => r.id === rid);
  if (r) { r.unit = val; _saveReplRemindersDebounced(item); }
}
function _replDeleteReminder(rid) {
  const item = items.find(i => i.id === _replItemId);
  if (!item?.replacementReminders) return;
  item.replacementReminders = item.replacementReminders.filter(r => r.id !== rid);
  _saveReplReminders(item);
}

let _replSaveTimer = null;
function _saveReplRemindersDebounced(item) {
  clearTimeout(_replSaveTimer);
  _replSaveTimer = setTimeout(() => _saveReplReminders(item), 600);
}
async function _saveReplReminders(item) {
  // Keep legacy fields in sync with first reminder
  if (item.replacementReminders?.length) {
    item.replacementInterval = item.replacementReminders[0].interval;
    item.replacementUnit     = item.replacementReminders[0].unit;
  } else {
    item.replacementInterval = null;
    item.replacementUnit     = null;
  }
  touchItem(item);
  await saveData();
  _renderReplRemindersList(item);
  scheduleRender('grid');
  setTimeout(syncAll, 400);
}





// ═══════════════════════════════════════════
//  SAVE / DELETE
// ═══════════════════════════════════════════
async function saveItem() {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const name = document.getElementById('f-name').value.trim();
  if (!name) { alert('Please enter a name for this item.'); return; }
  const startedUsing = document.getElementById('f-started-using').value || null;
  const storeVal = document.getElementById('f-store').value.trim()
    || urlToStoreName(document.getElementById('f-url').value.trim());

  if (editingId) {
    const item = items.find(i => i.id === editingId);
    if (item) {
      item.name         = name;
      item.category     = document.getElementById('f-category').value;
      item.cadence      = document.getElementById('f-cadence').value;
      item.qty          = parseFloat(document.getElementById('f-qty').value)||1;
      item.months       = parseFloat(document.getElementById('f-months').value)||1;
      item.url          = document.getElementById('f-url').value.trim();
      item.store        = storeVal;
      // Propagate new store name to all log entries so filter bar stays clean
      if (storeVal && item.logs) {
        item.logs.forEach(l => { if (l.store && l.store !== storeVal) l.store = storeVal; });
      }
      item.notes        = document.getElementById('f-notes').value.trim();
      item.startedUsing = startedUsing;
      item.rating       = currentRating || null;
      item.imageUrl     = pendingImageUrl || item.imageUrl || null;
      item.storePrices  = tempStorePrices.filter(sp => sp.store || sp.price);
      item.expiry       = document.getElementById('f-expiry')?.value || null;
      const tOverride   = parseInt(document.getElementById('f-threshold')?.value);
      item.thresholdOverride = isNaN(tOverride) ? null : tOverride;
      // Legacy single-reminder fields are now mirrors of replacementReminders[0]
      // (kept in sync by _saveReplReminders). Only write them from the hidden
      // form fields if the new array doesn't exist — otherwise we'd overwrite
      // changes the user just made via the Replacement Reminders modal.
      if (!item.replacementReminders?.length) {
        const replInterval = parseInt(document.getElementById('f-replace-interval')?.value);
        item.replacementInterval = isNaN(replInterval) ? null : replInterval;
        item.replacementUnit     = document.getElementById('f-replace-unit')?.value || 'months';
      }

      // Update the most recent log entry's date and qty
      const lastDate  = document.getElementById('f-last-date').value;
      const lastQty   = parseFloat(document.getElementById('f-last-qty').value)||1;
      const lastPrice = document.getElementById('f-last-price').value.trim();
      if (lastDate) {
        if (item.logs && item.logs.length) {
          item.logs[item.logs.length - 1].date  = lastDate;
          item.logs[item.logs.length - 1].qty   = lastQty;
          item.logs[item.logs.length - 1].price = lastPrice;
          item.logs.sort((a,b) => new Date(a.date) - new Date(b.date));
        } else {
          item.logs = [{ id: uid(), date: lastDate, qty: lastQty, price: lastPrice, store: storeVal }];
        }
      }
      touchField(item,
        'name','category','cadence','qty','months','url','store',
        'notes','startedUsing','rating','imageUrl','storePrices',
        'expiry','thresholdOverride','replacementInterval','replacementUnit'
      );
    }
  } else {
    const lastDate  = document.getElementById('f-last-date').value;
    const lastQty   = parseFloat(document.getElementById('f-last-qty').value)||1;
    const lastPrice = document.getElementById('f-last-price').value.trim();
    items.push({
      id:                uid(),
      name,
      category:          document.getElementById('f-category').value,
      cadence:           document.getElementById('f-cadence').value,
      qty:               parseFloat(document.getElementById('f-qty').value)||1,
      months:            parseFloat(document.getElementById('f-months').value)||1,
      url:               document.getElementById('f-url').value.trim(),
      store:             storeVal,
      notes:             document.getElementById('f-notes').value.trim(),
      startedUsing,
      rating:            currentRating || null,
      imageUrl:          pendingImageUrl || null,
      storePrices:       tempStorePrices.filter(sp => sp.store || sp.price),
      expiry:            document.getElementById('f-expiry')?.value || null,
      thresholdOverride: (() => { const v = parseInt(document.getElementById('f-threshold')?.value); return isNaN(v) ? null : v; })(),
      logs:              lastDate ? [{ id:uid(), date:lastDate, qty:lastQty, price:lastPrice, store:storeVal }] : [],
      updatedAt:         new Date().toISOString(),
    });
  }
  await saveData();
  closeModal('item-modal');
  if (editingId) clearQuickAddedFlag(editingId);
  scheduleRender('grid', 'dashboard', 'filters', 'shopping', 'sns');
  toast('Item saved ✓');
  _syncQueue.enqueue('Saving item…');
}

async function saveLog() {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === loggingId);
  if (!item) return;
  if (!item.logs) item.logs = [];
  item.logs.push({
    id:             uid(),
    date:           document.getElementById('log-date').value,
    qty:            parseFloat(document.getElementById('log-qty').value)||1,
    price:          document.getElementById('log-price').value.trim(),
    store:          document.getElementById('log-store').value.trim(),
    pendingDelivery: true,
  });
  item.logs.sort((a,b) => new Date(a.date) - new Date(b.date));
  // Mark as ordered
  item.ordered   = true;
  item.orderedAt = new Date().toISOString();
  touchField(item, 'logs', 'ordered', 'orderedAt');
  await saveData();
  closeModal('log-modal');
  scheduleRender('grid', 'dashboard', 'shopping');
  setTimeout(syncAll, 400);

  // If triggered from "Delivered" button with no prior log, go straight to delivered modal
  const logThenDeliver = sessionStorage.getItem('log_then_deliver');
  if (logThenDeliver && logThenDeliver === loggingId) {
    sessionStorage.removeItem('log_then_deliver');
    setTimeout(() => openDeliveredModal(loggingId), 200);
  } else {
    toast('Purchase logged — tap Delivered when it arrives ✓');
  }
}

// ── Delivered flow ─────────────────────────────────────────
let deliveringId = null;

function openDeliveredModal(id) {
  const item = items.find(i => i.id === id);
  if (!item) return;

  const hasPendingLog = item.logs?.some(l => l.pendingDelivery);

  // If not ordered at all and no pending log, route through log purchase first
  if (!hasPendingLog && !item.ordered) {
    sessionStorage.setItem('log_then_deliver', id);
    openLogPurchaseModal(id);
    setTimeout(() => {
      const title = document.getElementById('log-modal-title');
      if (title) title.innerHTML = '<svg class="icon icon-md" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-package"></use></svg> Purchase Details — ' + esc(item.name);
      const sub = document.querySelector('#log-modal .subtitle');
      if (sub) sub.textContent = 'Enter the purchase details then we\'ll confirm delivery.';
      const saveBtn = document.querySelector('#log-modal .btn-primary');
      if (saveBtn) { saveBtn.textContent = 'Next — Confirm Delivery →'; saveBtn.style.background = 'var(--ok)'; }
    }, 30);
    return;
  }

  deliveringId = id;
  document.getElementById('delivered-modal-title').innerHTML = `<svg class="icon icon-md" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-package-check"></use></svg> Delivered — ${esc(item.name)}`;
  document.getElementById('delivered-date').value = today();
  const pendingLog = [...(item.logs||[])].reverse().find(l => l.pendingDelivery);
  document.getElementById('delivered-qty').value = pendingLog?.qty || item.qty || 1;
  document.getElementById('delivered-started-now').checked = false;
  document.getElementById('delivered-started-row').style.display = 'none';
  document.getElementById('delivered-started-hint').style.display = 'block';
  document.getElementById('delivered-started-date').value = today();
  openModal('delivered-modal');
}

function toggleDeliveredStarted() {
  const checked = document.getElementById('delivered-started-now').checked;
  document.getElementById('delivered-started-row').style.display = checked ? 'block' : 'none';
  document.getElementById('delivered-started-hint').style.display = checked ? 'none' : 'block';
}

async function saveDelivered() {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === deliveringId);
  if (!item) return;

  const deliveryDate  = document.getElementById('delivered-date').value || today();
  const qty           = parseFloat(document.getElementById('delivered-qty').value) || 1;
  const startedNow    = document.getElementById('delivered-started-now').checked;
  const startedDate   = startedNow ? (document.getElementById('delivered-started-date').value || deliveryDate) : null;

  if (!item.logs) item.logs = [];

  // Find and update the most recent pending log, or create a new delivered log
  const pendingIdx = [...item.logs].map((l,i)=>({l,i})).reverse().find(({l})=>l.pendingDelivery)?.i;
  if (pendingIdx !== undefined) {
    item.logs[pendingIdx].pendingDelivery = false;
    item.logs[pendingIdx].deliveredDate   = deliveryDate;
    item.logs[pendingIdx].qty             = qty;
  } else {
    item.logs.push({ id: uid(), date: deliveryDate, qty, pendingDelivery: false, deliveredDate });
    item.logs.sort((a,b) => new Date(a.date) - new Date(b.date));
  }

  // Clear ordered flag
  item.ordered   = false;
  item.orderedAt = null;

  // Set startedUsing if user said they're starting now
  if (startedDate) item.startedUsing = startedDate;

  touchItem(item);
  await saveData();
  closeModal('delivered-modal');
  scheduleRender('grid', 'dashboard', 'shopping');
  setTimeout(syncAll, 400);

  // Offer reminder setup
  openDeliveryReminderModal(deliveringId, !startedDate);
}

// ── Post-delivery reminder modal ───────────────────────────
let deliveryReminderItemId = null;
let deliveryNeedsStartDate = false;

function openDeliveryReminderModal(id, needsStartDate) {
  const item = items.find(i => i.id === id);
  if (!item) return;
  deliveryReminderItemId = id;
  deliveryNeedsStartDate = needsStartDate;

  document.getElementById('delivery-reminder-subtitle').textContent =
    needsStartDate
      ? `Come back and set a "started using" date to keep calculations accurate.`
      : `Do you want a reminder when it's time to replace this?`;

  const hasReminder = item.replacementInterval;
  const body = document.getElementById('delivery-reminder-body');
  body.innerHTML = `
    ${needsStartDate ? `
      <div style="background:rgba(232,168,56,0.1);border:1px solid rgba(232,168,56,0.25);border-radius:10px;padding:12px 14px;margin-bottom:16px;font-size:13px;color:var(--warn);line-height:1.5">
        ⏳ When you open the first pack, tap <strong>"Set date"</strong> on the card — it anchors the stock clock and any reminders to the right start point.
      </div>` : ''}
    <div style="margin-bottom:14px">
      <label class="form-label">Replace every</label>
      <div style="display:flex;gap:8px;align-items:center">
        <input class="form-input" id="dr-interval" type="number" min="1" max="365" value="${item.replacementInterval||3}" style="width:80px">
        <select class="form-input" id="dr-unit" style="flex:1">
          <option value="days"   ${(item.replacementUnit||'months')==='days'   ?'selected':''}>days</option>
          <option value="weeks"  ${(item.replacementUnit||'months')==='weeks'  ?'selected':''}>weeks</option>
          <option value="months" ${(item.replacementUnit||'months')==='months' ?'selected':''}>months</option>
        </select>
      </div>
    </div>
    ${!hasReminder ? `<p style="font-size:12px;color:var(--muted)">You can also set this later via ✏️ Edit on the item.</p>` : ''}
  `;
  openModal('delivery-reminder-modal');
}

async function saveDeliveryReminder() {
  const item = items.find(i => i.id === deliveryReminderItemId);
  if (!item) return;
  const interval = parseInt(document.getElementById('dr-interval').value) || 3;
  const unit     = document.getElementById('dr-unit').value || 'months';
  item.replacementInterval = interval;
  item.replacementUnit     = unit;
  touchItem(item);
  await saveData();
  closeModal('delivery-reminder-modal');
  scheduleRender('grid');
  setTimeout(syncAll, 400);
  toast(`🔔 Reminder set: every ${interval} ${unit}`);
}

// ── Started using modal ────────────────────────────────────
let startedUsingItemId = null;

function openStartedUsingModal(id) {
  startedUsingItemId = id;
  const item = items.find(i => i.id === id);
  if (!item) return;
  document.getElementById('started-using-title').innerHTML = `<svg class="icon icon-md" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-calendar"></use></svg> ${esc(item.name)} — when did you start using it?`;
  document.getElementById('started-using-date').value = today();
  openModal('started-using-modal');
}

async function saveStartedUsing() {
  if (!canWrite("stockroom")) { showLockBanner("stockroom"); return; }
  const item = items.find(i => i.id === startedUsingItemId);
  if (!item) return;
  const date = document.getElementById('started-using-date').value || today();
  item.startedUsing = date;
  touchField(item, 'startedUsing');
  await saveData();
  closeModal('started-using-modal');
  scheduleRender('grid', 'dashboard');
  setTimeout(syncAll, 400);
  toast('Start date saved — stock clock updated ✓');
}

// ═══════════════════════════════════════════
//  PRICE LINKS
// ═══════════════════════════════════════════
function updatePriceLinks() {
  const raw = document.getElementById('price-search-input').value.trim()
            || document.getElementById('f-name').value.trim();
  const container = document.getElementById('price-links');
  if (!raw) { container.innerHTML = ''; return; }
  const q = encodeURIComponent(raw);
  container.innerHTML = getStores(settings.country).map(s =>
    `<a class="price-link" href="${s.url(q)}" target="_blank" rel="noopener">${s.name}</a>`
  ).join('');
}

// ═══════════════════════════════════════════
//  DATA MANAGEMENT
// ═══════════════════════════════════════════
function exportData() {
  const hasSecure = notes.some(n => n.locked);
  if (hasSecure) {
    openModal('export-secure-notes-modal');
  } else {
    requireReauth('Re-enter your passphrase to export your data.', _doExportData, { passkeyAllowed: true });
  }
}

function exportDataWithSecureChoice(includeUnlocked) {
  closeModal('export-secure-notes-modal');
  requireReauth('Re-enter your passphrase to export your data.', () => _doExportData(includeUnlocked), { passkeyAllowed: true });
}

function _doExportData(includeSecureNotes = false) {
  // Build notes export - exclude body of locked notes unless includeSecureNotes
  const exportNotes = notes.map(n => {
    if (n.locked && !includeSecureNotes) return { ...n, body: undefined };
    if (n.locked && includeSecureNotes) {
      const unlocked = _noteUnlocked.get(n.id);
      return { ...n, body: unlocked?.body || '[locked — could not export]', locked: false };
    }
    return n;
  });
  const exportPayload = {
    items,
    settings,
    groceries:   groceryItems,
    reminders,
    departments: groceryDepts,
    notes:       exportNotes,
    exportedAt:  new Date().toISOString(),
    version:     2,
  };
  const blob = new Blob([JSON.stringify(exportPayload, null, 2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'stockroom_backup_' + today() + '.json';
  a.click();
  try { localStorage.setItem('stockroom_last_export', String(Date.now())); } catch(e) {}
  document.getElementById('export-reminder-banner')?.remove();
}

async function importData(e) {
  if (!isOwner()) { toast('Settings are read-only'); return; }
  const file = e.target.files[0];
  if (!file) return;
  // Reset the input so the same file can be re-imported if needed
  e.target.value = '';
  const reader = new FileReader();
  reader.onload = async ev => {
    try {
      const raw = ev.target.result;
      let d;
      try {
        d = JSON.parse(raw);
      } catch(parseErr) {
        alert('Could not read file — it does not appear to be valid JSON.\n\nError: ' + parseErr.message);
        return;
      }
      if (!d || typeof d !== 'object') {
        alert('Invalid backup file — unexpected format.');
        return;
      }
      // Restore all data types from the backup
      if (Array.isArray(d.items))       items        = d.items;
      if (d.settings && typeof d.settings === 'object')
                                        settings     = { ...settings, ...d.settings };
      if (Array.isArray(d.groceries))   groceryItems = d.groceries;
      if (Array.isArray(d.reminders))   reminders    = d.reminders;
      if (Array.isArray(d.departments)) groceryDepts = d.departments;
      await saveData();
      await _saveSettings();
      if (Array.isArray(d.groceries))   await saveGrocery();
      if (Array.isArray(d.reminders))   await saveReminders();
      if (Array.isArray(d.departments)) await saveGroceryDepts();
      scheduleRender(...RENDER_REGIONS);
      // Push to server so data is encrypted and saved
      if (kvConnected) {
        await kvSyncNow(true).catch(err => console.warn('Import sync failed:', err.message));
      }
      const counts = [
        d.items?.length ? `${d.items.length} items` : '',
        d.groceries?.length ? `${d.groceries.length} groceries` : '',
        d.reminders?.length ? `${d.reminders.length} reminders` : '',
      ].filter(Boolean).join(', ');
      toast('Imported ✓' + (counts ? ` — ${counts}` : ''));
    } catch(err) {
      alert('Import failed: ' + err.message + '\n\nPlease try again or check the browser console for details.');
      console.error('importData error:', err);
    }
  };
  reader.onerror = () => alert('Could not read the file. Please try again.');
  reader.readAsText(file);
}

async function clearAll() {
  if (!isOwner()) {
    // Guest clearing orphaned share data — no reauth needed, just local wipe
    if (!confirm('Clear all data from the shared household? This removes the local copy only — it does not affect the owner\'s data.')) return;
    items = [];
    _shareState   = null;
    _sharedFileId = null;
    _shareKey     = null;
    saveShareState();
    try { localStorage.removeItem('stockroom_share_keys'); } catch(e) {}
    applyTabPermissions();
    scheduleRender(...RENDER_REGIONS);
    toast('Shared data cleared');
    if (kvConnected) kvSyncNow().catch(() => {});
    return;
  }
  if (!confirm('This will permanently delete ALL your items, purchase history, and shared data. Are you sure?')) return;
  requireReauth('Confirm your identity to clear all data.', _doClearAll, { passkeyAllowed: true });
}

async function _doClearAll() {
  // Delete all share targets from backend first
  for (const target of (_shareTargets || [])) {
    try {
      await fetchKV(`${WORKER_URL}/share/delete`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code: target.code }),
      });
    } catch(e) { console.warn('clearAll: could not delete share', target.code, e.message); }
  }
  _shareTargets = [];
  try { localStorage.removeItem('stockroom_share_keys'); } catch(e) {}
  // Clear own data locally and push empty state to server
  items = [];
  await saveData();
  await kvPush().catch(e => console.warn('clearAll: push failed', e.message));
  renderShareTargetsList();
  scheduleRender(...RENDER_REGIONS);
  toast('All data cleared');
}

// ═══════════════════════════════════════════
//  TOAST
// ═══════════════════════════════════════════
function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}

// ═══════════════════════════════════════════
//  CLOSE MODAL ON BACKDROP CLICK
// ═══════════════════════════════════════════
document.querySelectorAll('.modal-backdrop').forEach(b => {
  b.addEventListener('click', e => { if (e.target === b) b.classList.remove('open'); });
});

// ═══════════════════════════════════════════
//  BUILD SETTINGS COUNTRY DROPDOWN
// ═══════════════════════════════════════════
function buildSettingsCountrySelect() {
  const options = COUNTRIES.map(c => `<option value="${c.code}">${c.flag} ${c.name}</option>`).join('');
  const val = settings.country || 'GB';
  ['setting-country', 'setting-country-sec'].forEach(id => {
    const sel = document.getElementById(id);
    if (!sel) return;
    if (!sel.options.length) sel.innerHTML = options;
    sel.value = val;
  });
}

// ═══════════════════════════════════════════
//  EMAIL REPORTS (Part C)
// ═══════════════════════════════════════════

function updateEmailDayVisibility() {
  const freq = document.getElementById('email-frequency')?.value;
  const dayField = document.getElementById('email-day-field');
  if (dayField) dayField.style.display = freq === 'weekly' ? 'block' : 'none';
}

function renderEmailSettingsUI() {
  const needsSetup  = document.getElementById('email-setup-needed');
  const notReg      = document.getElementById('email-not-registered');
  const regEl       = document.getElementById('email-registered');

  if (!WORKER_URL) {
    if (needsSetup) needsSetup.style.display = 'block';
    if (notReg)     notReg.style.display     = 'none';
    if (regEl)      regEl.style.display      = 'none';
    return;
  }
  if (needsSetup) needsSetup.style.display = 'none';

  if (emailRegistered) {
    if (notReg) notReg.style.display = 'none';
    if (regEl)  regEl.style.display  = 'block';
    const label  = document.getElementById('email-registered-label');
    const detail = document.getElementById('email-registered-detail');
    if (label)  label.textContent  = `Sending to ${emailAddress}`;
    if (detail) detail.textContent = `${capitalize(emailFrequency)}${emailFrequency==='weekly'?' on '+capitalize(emailDayOfWeek):''}`;
    const freqSel = document.getElementById('email-frequency-update');
    const daySel  = document.getElementById('email-day-update');
    if (freqSel) freqSel.value = emailFrequency;
    if (daySel)  daySel.value  = emailDayOfWeek;
  } else {
    if (notReg) notReg.style.display = 'block';
    if (regEl)  regEl.style.display  = 'none';
  }
}

function capitalize(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : ''; }

async function connectEmailReports() {
  if (!WORKER_URL) { toast('Worker URL not configured'); return; }
  const email = document.getElementById('email-input')?.value.trim();
  if (!email || !email.includes('@')) { alert('Please enter a valid email address.'); return; }

  const frequency  = document.getElementById('email-frequency')?.value || 'weekly';
  const dayOfWeek  = document.getElementById('email-day')?.value       || 'monday';

  // Build state param containing registration details
  const state = btoa(JSON.stringify({ email, frequency, dayOfWeek }));

  // Build OAuth URL pointing to Worker's /auth endpoint as redirect
  const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth'
    + `?client_id=${CLIENT_ID}`
    + `&redirect_uri=${encodeURIComponent(WORKER_URL + '/auth')}`
    + `&response_type=code`
    + `&scope=${encodeURIComponent('https://www.googleapis.com/auth/drive.file')}`
    + `&access_type=offline`
    + `&prompt=consent`
    + `&state=${encodeURIComponent(state)}`;

  // Store email locally so we can check status on return
  try { localStorage.setItem('stockroom_pending_email', email); } catch(e){}
  window.location.href = authUrl;
}

async function checkEmailRegistrationOnLoad() {
  if (!WORKER_URL) return;

  // Check if returning from OAuth
  const params = new URLSearchParams(window.location.search);
  const authResult = params.get('email_auth');
  const returnEmail = params.get('email');
  if (authResult) {
    // Clean URL
    history.replaceState(null, '', location.pathname);
    if (authResult === 'success' && returnEmail) {
      emailRegistered = true;
      emailAddress    = returnEmail;
      try { localStorage.setItem('stockroom_email', JSON.stringify({ emailRegistered, emailAddress, emailFrequency, emailDayOfWeek })); } catch(e){}
      toast('Email reports enabled ✓');
    } else {
      const reason = params.get('reason') || 'unknown';
      alert(`Email setup failed: ${reason}. Please try again.`);
    }
    renderEmailSettingsUI();
    return;
  }

  // Restore from localStorage
  try {
    const saved = JSON.parse(localStorage.getItem('stockroom_email') || '{}');
    if (saved.emailRegistered && saved.emailAddress) {
      emailRegistered = saved.emailRegistered;
      emailAddress    = saved.emailAddress;
      emailFrequency  = saved.emailFrequency || 'weekly';
      emailDayOfWeek  = saved.emailDayOfWeek || 'monday';
      // Verify still registered with worker
      const res = await fetch(`${WORKER_URL}/status?email=${encodeURIComponent(emailAddress)}`);
      if (res.ok) {
        const data = await res.json();
        if (!data.registered) {
          emailRegistered = false;
          localStorage.removeItem('stockroom_email');
        } else {
          emailFrequency = data.frequency || emailFrequency;
          emailDayOfWeek = data.dayOfWeek || emailDayOfWeek;
        }
      }
    }
  } catch(e) {}

  renderEmailSettingsUI();
}

async function updateEmailSettings() {
  if (!WORKER_URL || !emailRegistered) return;
  const frequency = document.getElementById('email-frequency-update')?.value || emailFrequency;
  const dayOfWeek = document.getElementById('email-day-update')?.value       || emailDayOfWeek;
  emailFrequency  = frequency;
  emailDayOfWeek  = dayOfWeek;
  try { localStorage.setItem('stockroom_email', JSON.stringify({ emailRegistered, emailAddress, emailFrequency, emailDayOfWeek })); } catch(e){}
  try {
    await fetch(`${WORKER_URL}/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: emailAddress, frequency, dayOfWeek }),
    });
    renderEmailSettingsUI();
    toast('Email schedule updated ✓');
  } catch(e) { toast('Could not update — check your connection'); }
}

async function disableEmailReports() {
  if (!confirm(`Stop email reports to ${emailAddress}?`)) return;
  try {
    if (WORKER_URL) {
      await fetch(`${WORKER_URL}/unregister`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: emailAddress }),
      });
    }
  } catch(e) {}
  emailRegistered = false;
  emailAddress    = '';
  try { localStorage.removeItem('stockroom_email'); } catch(e){}
  renderEmailSettingsUI();
  toast('Email reports disabled');
}

async function sendEmailNow() {
  if (!WORKER_URL || !emailRegistered) return;
  toast('Sending…');
  try {
    const res = await fetch(`${WORKER_URL}/send-now`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: emailAddress }),
    });
    const data = await res.json();
    if (data.ok) toast('Report sent ✓ — check your inbox');
    else toast('Send failed: ' + (data.error || 'unknown error'));
  } catch(e) { toast('Could not reach email service'); }
}


// ── Drive sync via backend proxy ─────────────────────────
// The frontend never talks to Drive directly. All reads/writes
// go through WORKER_URL which uses the stored refresh token.
// No access token, no sessionStorage token, no re-auth needed.

async function drivePull() {
  const hParam = activeProfile && activeProfile !== 'default' ? `&household=${encodeURIComponent(activeProfile)}` : '';
  const sParam = _shareState?.code ? `&share=${encodeURIComponent(_shareState.code)}` : '';
  const res = await fetch(`${WORKER_URL}/sync/pull?1=1${hParam}${sParam}`);
  if (res.status === 503) throw new Error('NOT_CONNECTED');
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Pull failed: ${res.status}`);
  }
  return res.json();
}

async function drivePush(payload) {
  const hParam = activeProfile && activeProfile !== 'default' ? `&household=${encodeURIComponent(activeProfile)}` : '';
  const sParam = _shareState?.code ? `&share=${encodeURIComponent(_shareState.code)}` : '';
  const res = await fetch(`${WORKER_URL}/sync/push?1=1${hParam}${sParam}`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    payload,
  });
  if (res.status === 503) throw new Error('NOT_CONNECTED');
  if (res.status === 403) throw new Error('READ_ONLY');
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Push failed: ${res.status}`);
  }
}

async function syncNow() {
  if (!kvConnected && !_shareState) return;
  updateSyncPill('syncing');
  try {
    // Pull from backend (owner uses /sync/pull, joined members use proxy)
    // Guests use kvPull() which correctly hits /share/data/pull with credentials
    const remote = await (_shareState ? kvPull() : drivePull());

    if (remote && Array.isArray(remote.items)) {
      const localLastSynced  = settings.lastSynced ? new Date(settings.lastSynced).getTime() : 0;
      const remoteLastSynced = remote.lastSynced   ? new Date(remote.lastSynced).getTime()   : 0;
      const remoteWins       = remoteLastSynced > localLastSynced;

      items = await mergeItems(items, remote.items, remoteWins);
      await saveData();

      if (remote.settings) {
        const localTags     = settings.customTags;
        settings            = { ...remote.settings, ...settings };
        const remoteTags    = remote.settings.customTags || [];
        const localDefined  = (localTags||[]).filter(t=>t&&t.trim()).length;
        const remoteDefined = remoteTags.filter(t=>t&&t.trim()).length;
        settings.customTags = localDefined >= remoteDefined ? (localTags||[]) : remoteTags;
        await _saveSettings();
      }
      if (remote.groceries) {
        const remoteG = remote.groceries;
        const localGEmpty = groceryItems.length === 0;
        const gTombstones = await loadGroceryDeletedIds();
        if (remoteWins || localGEmpty) {
          groceryItems = remoteG.filter(i => !gTombstones.has(i.id)); await _saveGroceryLocal();
        } else {
          const localGIds = new Set(groceryItems.map(i => i.id));
          const newG = remoteG.filter(i => !localGIds.has(i.id) && !gTombstones.has(i.id));
          if (newG.length) { groceryItems = [...groceryItems, ...newG]; await _saveGroceryLocal(); }
        }
      }
      if (remote.departments?.length) {
        const localDEmpty = groceryDepts.length === 0;
        if (remoteWins || localDEmpty) {
          groceryDepts = remote.departments; await saveGroceryDepts();
        } else {
          const localDIds = new Set(groceryDepts.map(d => d.id || d.name));
          const newD = remote.departments.filter(d => !localDIds.has(d.id || d.name));
          if (newD.length) { groceryDepts = [...groceryDepts, ...newD]; await saveGroceryDepts(); }
        }
      }
      if (remote.reminders && Array.isArray(remote.reminders)) {
        const localREmpty = reminders.length === 0;
        if (remoteWins || localREmpty) {
          reminders = remote.reminders; await saveReminders();
        } else {
          const localRIds = new Set(reminders.map(r => r.id));
          const newR = remote.reminders.filter(r => !localRIds.has(r.id));
          if (newR.length) { reminders = [...reminders, ...newR]; await saveReminders(); }
        }
      }
      if (remote.deletedIds && Array.isArray(remote.deletedIds)) {
        const merged = await loadDeletedIds();
        remote.deletedIds.forEach(id => merged.add(id));
        await saveDeletedIds(merged);
      }
      // Merge household directory
      if (remote.householdDir && typeof remote.householdDir === 'object') {
        const localProfiles = await getProfiles();
        let changed = false;
        const deletedHouseholds = _getDeletedHouseholds();
        Object.entries(remote.householdDir).forEach(([key, meta]) => {
          // Skip re-creating households the user has explicitly deleted
          if (deletedHouseholds.has(key)) return;
          if (!localProfiles[key]) {
            localProfiles[key] = { name: meta.name, colour: meta.colour, items: [], settings: {}, reminders: [], groceries: [], departments: [] };
            changed = true;
          } else {
            if (meta.name && localProfiles[key].name !== meta.name) { localProfiles[key].name = meta.name; changed = true; }
            if (meta.colour && localProfiles[key].colour !== meta.colour) { localProfiles[key].colour = meta.colour; changed = true; }
          }
        });
        if (changed) {
          await saveProfiles(localProfiles);
          renderSettingsHouseholdList();
        }
      }
      // Restore share targets from Drive if KV was wiped
      if (remote.shareTargets && Array.isArray(remote.shareTargets) && remote.shareTargets.length) {
        _shareTargets = remote.shareTargets;
        renderShareTargetsList();
        // Re-seed KV so the backend can validate codes again
        if (WORKER_URL && isOwner()) {
          remote.shareTargets.forEach(t => {
            if (t.code) {
              fetch(`${WORKER_URL}/share/restore`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(t),
              }).catch(() => {});
            }
          });
        }
      }
      scheduleRender(...RENDER_REGIONS);
    }

    // Push merged data back
    settings.lastSynced = new Date().toISOString();
    await _saveSettings();
    const uploadedIds  = new Set(items.map(i => i.id));
    const tombstones   = await loadDeletedIds();
    tombstones.forEach(id => { if (uploadedIds.has(id)) tombstones.delete(id); });
    await saveDeletedIds(tombstones);
    // Include household directory and share targets in Drive payload
    // so both survive KV wipes and are restored on reconnect
    const allProfiles  = await getProfiles();
    const householdDir = Object.fromEntries(
      Object.entries(allProfiles).map(([k, p]) => [k, { name: p.name, colour: p.colour }])
    );
    const payload = JSON.stringify({
      items, settings, lastSynced: settings.lastSynced,
      groceries: groceryItems, departments: groceryDepts,
      reminders, deletedIds: [...tombstones],
      householdDir, activeProfile,
      shareTargets: _shareTargets, // persists share targets to Drive
    });
    // Guests are read-only — never push back to the owner's store
    if (!_shareState) await drivePush(payload);

    updateSyncPill('synced');
  } catch(err) {
    console.error('Drive sync error:', err);
    if (err.message === 'NOT_CONNECTED') {
      updateSyncPill('error');
      const label = document.getElementById('sync-label');
      if (label) label.textContent = 'Not connected';
      if (!sessionStorage.getItem('connect_drive_prompted')) {
        sessionStorage.setItem('connect_drive_prompted', '1');
        toast('Sign in to enable sync');
      }
    } else if (err.message === 'OWNER_NOT_CONNECTED') {
      updateSyncPill('error');
      toast('The household owner needs to reconnect their account');
    } else if (err.message?.startsWith('ACCESS_DENIED')) {
      updateSyncPill('error');
      toast('Access denied — your invite link may have been revoked');
    } else {
      updateSyncPill('error');
      if (!err.message.includes('fetch') && !err.message.includes('NetworkError')) {
        toast('Sync failed — ' + err.message);
      }
    }
  }
}

// ═══════════════════════════════════════════
//  KV AUTH & SYNC (replaces Drive/Dropbox)
// ═══════════════════════════════════════════

// Fetch with 10s timeout — prevents UI hanging on network issues
async function fetchKV(url, opts = {}) {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 10000);
  try {
    return await fetch(url, { ...opts, signal: controller.signal });
  } catch(e) {
    if (e.name === 'AbortError') throw new Error('Request timed out — check your connection');
    throw e;
  } finally {
    clearTimeout(tid);
  }
}


// ── WebAuthn PRF constants ─────────────────────────────────────────────────
// Fixed salt evaluated by the secure enclave to derive a deterministic key.
// Must be identical at registration and every subsequent login.
const PASSKEY_PRF_SALT = new TextEncoder().encode('stockroom-e2ee-passkey-prf-v1!!').buffer;

// Test whether the current browser supports the PRF extension.
// Returns true only if navigator.credentials exists AND the extension is usable.
async function passkeyPrfSupported() {
  try {
    if (!window.PublicKeyCredential) return false;
    // Chrome 116+/Safari 17+ ship PRF; Firefox does not yet
    return true; // we attempt it and fall back gracefully on failure
  } catch(e) { return false; }
}

// Given PRF output bytes (ArrayBuffer), import as AES-KW key for wrapping/unwrapping
async function prfBytesToWrapKey(prfBytes) {
  return crypto.subtle.importKey('raw', prfBytes, 'AES-KW', false, ['wrapKey', 'unwrapKey']);
}

// Wrap the data key with a PRF-derived AES-KW key → base64 envelope
async function wrapKeyWithPrf(dataKey, prfWrapKey) {
  const wrapped = await crypto.subtle.wrapKey('raw', dataKey, prfWrapKey, 'AES-KW');
  return btoa(String.fromCharCode(...new Uint8Array(wrapped)));
}

// Unwrap the data key using a PRF-derived AES-KW key
async function unwrapKeyWithPrf(envelopeB64, prfWrapKey) {
  const wrapped = Uint8Array.from(atob(envelopeB64), c => c.charCodeAt(0));
  return crypto.subtle.unwrapKey(
    'raw', wrapped, prfWrapKey, 'AES-KW',
    { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']
  );
}


// ── State ──────────────────────────────────
let kvConnected      = false;
let _kvEmail         = '';
let _kvEmailHash     = '';
let _kvVerifier      = '';
let _kvKey           = null;
// Captured when the "Remember me" checkbox changes — survives wizard DOM teardown
let _rememberMeChecked = false;
let _kvSessionToken  = null;

// Sentinel — thrown by kvPull when ciphertext exists but decryption fails.
// Distinct from null (no data) so kvSyncNow can surface a recovery prompt
// rather than silently leaving the user with an empty stockroom.
class KvDecryptError extends Error {
  constructor(msg) { super(msg); this.name = 'KvDecryptError'; }
}
let _kvAuthMethod    = '';
// Recovery flow state
let _recoveryEmail     = '';
let _recoveryEmailHash = '';
let _recoveryToken     = '';
let _recoveryDataKey   = null;
let _recoverySessionToken = ''; // set when passkey is used as first factor

// Send OTP email for recovery — shared by code-path and passkey-path
async function _recoverySendOtp(emailHash) {
  try {
    await fetchKV(`${WORKER_URL}/recovery/request`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: _recoveryEmail }),
    });
  } catch(e) { console.warn('_recoverySendOtp:', e.message); }
  document.getElementById('recovery-step-otp').style.display = '';
  setTimeout(() => document.getElementById('recovery-otp-input')?.focus(), 100);
}

// ── Crypto helpers (client-side) ───────────
// ── Crypto version config ─────────────────────────────────
// Must match CRYPTO_V2_SWITCHOVER in main.ts
const CRYPTO_V2_SWITCHOVER = '2026-05-01';

// v1 key derivation — kept for legacy login and migration decryption only
async function kvDeriveKey(email, passphrase) {
  const raw  = new TextEncoder().encode(email.toLowerCase().trim() + ':' + passphrase);
  const base = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  const salt = new TextEncoder().encode('stockroom-kv-v1-' + email.toLowerCase().trim());
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' },
    base, { name: 'AES-GCM', length: 256 },
    true, ['encrypt', 'decrypt']
  );
}

// ── v2 crypto primitives ───────────────────────────────────
// 600k PBKDF2 iterations, server-stored random KDF salt, AES-KW wrapping.

function generateKdfSalt() {
  return btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16))));
}

async function derivePassphraseWrapKeyV2(passphrase, emailHash, kdfSaltB64) {
  if (!kdfSaltB64) throw new Error('v2 KDF salt missing');
  const salt = Uint8Array.from(atob(kdfSaltB64), c => c.charCodeAt(0));
  const raw  = new TextEncoder().encode(passphrase + ':' + emailHash);
  const base = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 600000, hash: 'SHA-256' },
    base, { name: 'AES-KW', length: 256 }, false, ['wrapKey', 'unwrapKey']
  );
}

async function generateDataKeyV2Extractable() {
  return crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
}

async function wrapDataKeyV2(dataKey, wrapKey) {
  const wrapped = await crypto.subtle.wrapKey('raw', dataKey, wrapKey, 'AES-KW');
  return btoa(String.fromCharCode(...new Uint8Array(wrapped)));
}

async function unwrapDataKeyV2(wrappedB64, wrapKey, extractable = false) {
  const wrapped = Uint8Array.from(atob(wrappedB64), c => c.charCodeAt(0));
  return crypto.subtle.unwrapKey(
    'raw', wrapped, wrapKey, 'AES-KW',
    { name: 'AES-GCM', length: 256 }, extractable, ['encrypt', 'decrypt']
  );
}

async function deriveRecoveryWrapKeyV2(code, emailHash) {
  const raw  = new TextEncoder().encode(code.replace(/-/g,'').toUpperCase() + ':' + emailHash);
  const base = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  const salt = new TextEncoder().encode('stockroom-recovery-v2-' + emailHash);
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 600000, hash: 'SHA-256' },
    base, { name: 'AES-KW', length: 256 }, false, ['wrapKey', 'unwrapKey']
  );
}

async function buildRecoveryEnvelopesV2(codes, dataKey, emailHash) {
  const envelopes = [];
  for (const code of codes) {
    const wrapKey  = await deriveRecoveryWrapKeyV2(code, emailHash);
    const envelope = await wrapDataKeyV2(dataKey, wrapKey);
    const codeHash = await hashRecoveryCode(code, emailHash);
    envelopes.push(JSON.stringify({ envelope, codeHash, version: 'v2' }));
  }
  return envelopes;
}

async function kvEncrypt(key, plaintext) {
  const iv         = crypto.getRandomValues(new Uint8Array(12));
  const encoded    = new TextEncoder().encode(plaintext);
  const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, encoded);
  const combined   = new Uint8Array(iv.length + ciphertext.byteLength);
  combined.set(iv, 0);
  combined.set(new Uint8Array(ciphertext), iv.length);
  return btoa(String.fromCharCode(...combined));
}

async function kvDecrypt(key, ciphertext) {
  const combined = Uint8Array.from(atob(ciphertext), c => c.charCodeAt(0));
  const iv       = combined.slice(0, 12);
  const data     = combined.slice(12);
  const plain    = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, data);
  return new TextDecoder().decode(plain);
}

async function kvHashEmail(email) {
  const encoded = new TextEncoder().encode(email.toLowerCase().trim());
  const hash    = await crypto.subtle.digest('SHA-256', encoded);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2,'0')).join('').slice(0, 32);
}

async function kvMakeVerifier(passphrase, emailHash) {
  // SHA-256(passphrase + ':' + emailHash) — proves passphrase without revealing it
  const encoded = new TextEncoder().encode(passphrase + ':' + emailHash);
  const hash    = await crypto.subtle.digest('SHA-256', encoded);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2,'0')).join('');
}

// ══════════════════════════════════════════
//  ECDH ASYMMETRIC SHARE KEY SYSTEM
// ══════════════════════════════════════════
// Each account has a P-256 ECDH keypair.
//   Public key  → stored on server (unauthenticated read), used by others to wrap keys for us.
//   Private key → stored in IDB locally, never leaves the device unencrypted.
//
// Share flow:
//   Owner creates share → fetches guest's public key → ECDH-derives shared secret →
//   HKDF → AES-KW wrap key → wraps the AES-GCM share key → stores on server.
//   Guest joins → fetches wrapped key + owner public key → ECDH-derives same shared secret →
//   unwraps share key → decrypts shared data. No secret ever in a URL.

const ECDH_DB_NAME    = 'stockroom-kv-ecdh';
const ECDH_STORE_NAME = 'keys';

async function openEcdhDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(ECDH_DB_NAME, 1);
    req.onupgradeneeded = e => e.target.result.createObjectStore(ECDH_STORE_NAME);
    req.onsuccess = e => resolve(e.target.result);
    req.onerror   = e => reject(e.target.error);
  });
}

async function generateEcdhKeypair() {
  return crypto.subtle.generateKey(
    { name: 'ECDH', namedCurve: 'P-256' },
    true, // extractable so we can export for storage
    ['deriveKey', 'deriveBits']
  );
}

// Store private key as JWK in IDB, keyed by emailHash
async function storeEcdhPrivateKey(emailHash, privateKey) {
  const jwk = await crypto.subtle.exportKey('jwk', privateKey);
  const db  = await openEcdhDb();
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(ECDH_STORE_NAME, 'readwrite');
    tx.objectStore(ECDH_STORE_NAME).put(JSON.stringify(jwk), emailHash);
    tx.oncomplete = () => resolve();
    tx.onerror    = () => reject(tx.error);
  });
}

// Load private key from IDB
async function loadEcdhPrivateKey(emailHash) {
  try {
    const db = await openEcdhDb();
    const jwkStr = await new Promise((resolve, reject) => {
      const tx  = db.transaction(ECDH_STORE_NAME, 'readonly');
      const req = tx.objectStore(ECDH_STORE_NAME).get(emailHash);
      req.onsuccess = () => resolve(req.result ?? null);
      req.onerror   = () => reject(req.error);
    });
    if (!jwkStr) return null;
    return crypto.subtle.importKey(
      'jwk', JSON.parse(jwkStr),
      { name: 'ECDH', namedCurve: 'P-256' },
      false, ['deriveKey', 'deriveBits']
    );
  } catch(e) { console.warn('loadEcdhPrivateKey failed:', e.message); return null; }
}

// Derive an AES-KW wrapping key from two ECDH keys via HKDF
async function ecdhDeriveWrapKey(myPrivateKey, theirPublicKeyJwk) {
  const theirPub = await crypto.subtle.importKey(
    'jwk', theirPublicKeyJwk,
    { name: 'ECDH', namedCurve: 'P-256' },
    false, []
  );
  const bits = await crypto.subtle.deriveBits(
    { name: 'ECDH', public: theirPub }, myPrivateKey, 256
  );
  // HKDF over the raw ECDH bits → deterministic AES-KW key
  const hkdfKey = await crypto.subtle.importKey('raw', bits, 'HKDF', false, ['deriveKey']);
  return crypto.subtle.deriveKey(
    { name: 'HKDF', hash: 'SHA-256',
      salt:  new TextEncoder().encode('stockroom-ecdh-share-v1'),
      info:  new Uint8Array(0) },
    hkdfKey,
    { name: 'AES-KW', length: 256 },
    false, ['wrapKey', 'unwrapKey']
  );
}

// Wrap a share key (AES-GCM CryptoKey) using ECDH-derived AES-KW key
// Returns base64 string
async function ecdhWrapShareKey(myPrivateKey, theirPublicKeyJwk, shareKey) {
  const wrapKey = await ecdhDeriveWrapKey(myPrivateKey, theirPublicKeyJwk);
  const wrapped = await crypto.subtle.wrapKey('raw', shareKey, wrapKey, 'AES-KW');
  return btoa(String.fromCharCode(...new Uint8Array(wrapped)));
}

// Unwrap a share key using ECDH-derived AES-KW key
// wrappedB64: base64 string returned by ecdhWrapShareKey
async function ecdhUnwrapShareKey(myPrivateKey, theirPublicKeyJwk, wrappedB64) {
  const wrapKey = await ecdhDeriveWrapKey(myPrivateKey, theirPublicKeyJwk);
  const wrapped = Uint8Array.from(atob(wrappedB64), c => c.charCodeAt(0));
  return crypto.subtle.unwrapKey(
    'raw', wrapped, wrapKey, 'AES-KW',
    { name: 'AES-GCM', length: 256 },
    true, ['encrypt', 'decrypt']
  );
}

// Idempotent: generate keypair if not in IDB, then upload public key if not on server.
// Safe to call on every login — exits early if already done.
async function ensureEcdhKeypair(emailHash) {
  if (!emailHash) return;
  try {
    let privateKey = await loadEcdhPrivateKey(emailHash);
    let needsUpload = false;

    if (!privateKey) {
      // First time on this device — generate
      const kp = await generateEcdhKeypair();
      await storeEcdhPrivateKey(emailHash, kp.privateKey);
      privateKey = kp.privateKey;
      // Upload public key
      const pubJwk = await crypto.subtle.exportKey('jwk', kp.publicKey);
      await fetchKV(`${WORKER_URL}/user/ecdh-pubkey/store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash, publicKeyJwk: pubJwk }),
      });
      return;
    }

    // Private key exists locally — check server has our public key
    const check = await fetchKV(`${WORKER_URL}/user/ecdh-pubkey/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash }),
    });
    if (!check.ok) needsUpload = true;

    if (needsUpload) {
      // Re-derive public key from stored private key isn't possible with Web Crypto
      // (private key is non-extractable after import). Regenerate the pair.
      const kp = await generateEcdhKeypair();
      await storeEcdhPrivateKey(emailHash, kp.privateKey);
      const pubJwk = await crypto.subtle.exportKey('jwk', kp.publicKey);
      await fetchKV(`${WORKER_URL}/user/ecdh-pubkey/store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash, publicKeyJwk: pubJwk }),
      });
    }
  } catch(e) {
    console.warn('ensureEcdhKeypair failed (non-fatal):', e.message);
  }
}


// ── Dev diagnostic — run window.stockroomDiag() in browser console ──
window.stockroomDiag = async function() {
  const out = [];
  out.push('=== STOCKROOM DIAGNOSTIC ===');
  out.push(`kvConnected:  ${kvConnected}`);
  out.push(`_kvEmail:     ${_kvEmail || '(empty)'}`);
  out.push(`_kvEmailHash: ${_kvEmailHash || '(empty)'}`);
  out.push(`_kvVerifier:  ${_kvVerifier ? _kvVerifier.slice(0,8)+'…' : '(empty)'}`);
  out.push(`_kvKey:       ${_kvKey ? 'SET' : 'NULL'}`);
  out.push(`_kvSessionToken: ${_kvSessionToken ? 'SET' : 'null'}`);
  out.push(`device_secret:   ${localStorage.getItem('stockroom_device_secret') ? 'SET' : 'absent'}`);
  try {
    const sk = await lsGetEncrypted('stockroom_kv_session_key');
    out.push(`session_key cache: ${sk ? `SET (expires ${new Date(sk.expiry).toISOString()}, emailHash match: ${sk.emailHash === _kvEmailHash})` : 'absent'}`);
  } catch(e) { out.push('session_key cache: parse error'); }
  out.push(`local items: ${items?.length ?? 'undefined'}`);
  out.push(`_shareState: ${_shareState ? JSON.stringify({code:_shareState.code,type:_shareState.type,ownerName:_shareState.ownerName}) : 'null'}`);
  out.push(`_shareKey:   ${_shareKey ? 'SET' : 'null'}`);
  try {
    const sk = await _getShareKeys();
    out.push(`share_keys local: ${Object.keys(sk).length ? Object.keys(sk).join(', ') : 'none'}`);
  } catch(e) { out.push('share_keys local: parse error'); }
  // Check what the server has for this account
  if (_kvEmailHash && _kvVerifier) {
    try {
      const r = await fetch(`${WORKER_URL}/key/get`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, verifier: _kvVerifier }),
      });
      const d = await r.json();
      out.push(`server key/get status: ${r.status}`);
      out.push(`server key/get body:   ${JSON.stringify(d)}`);
    } catch(e) { out.push(`server key/get error: ${e.message}`); }
    // Try a raw pull to see if ciphertext exists
    try {
      const r = await fetch(`${WORKER_URL}/data/pull`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, verifier: _kvVerifier }),
      });
      const d = await r.json();
      out.push(`server data/pull status: ${r.status}`);
      out.push(`ciphertext present: ${!!d.ciphertext} (${d.ciphertext ? d.ciphertext.length + ' chars' : 'none'})`);
    } catch(e) { out.push(`server data/pull error: ${e.message}`); }
  } else {
    out.push('Cannot check server — no emailHash/verifier in session');
  }
  out.push('=== END ===');
  return out.join('\n');
};

// ── Email verification flow ────────────────────────────────
// _emailVerifyCallback is set before showing step-1f.
// On successful verification it is called to continue the normal flow.
let _emailVerifyEmail     = '';
let _emailVerifyEmailHash = '';
let _emailVerifyCallback  = null; // fn to call after verification succeeds

async function showEmailVerification(email, emailHash, onSuccess) {
  _emailVerifyEmail     = email;
  _emailVerifyEmailHash = emailHash;
  _emailVerifyCallback  = onSuccess;

  // Display email address in the step
  const display = document.getElementById('verify-email-display');
  if (display) display.textContent = email;

  // Clear previous state
  const otpInput = document.getElementById('email-verify-otp');
  const errEl    = document.getElementById('email-verify-error');
  const okEl     = document.getElementById('email-verify-ok');
  if (otpInput) otpInput.value = '';
  if (errEl)    errEl.style.display = 'none';
  if (okEl)     okEl.style.display  = 'none';

  // Show the step
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1f')?.classList.add('active');
  setTimeout(() => otpInput?.focus(), 100);

  // Send OTP
  await sendEmailVerificationOtp(email, emailHash);
}

async function sendEmailVerificationOtp(email, emailHash) {
  const okEl  = document.getElementById('email-verify-ok');
  const errEl = document.getElementById('email-verify-error');
  const btn   = document.getElementById('email-verify-resend-btn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Sending…'; }
  try {
    const res = await fetchKV(`${WORKER_URL}/email/verify/send`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, email }),
    });
    const data = await res.json();
    if (res.status === 429) {
      if (okEl) { okEl.textContent = 'Code already sent — check your email.'; okEl.style.display = 'block'; }
      return;
    }
    if (!res.ok) throw new Error(data.error || 'Could not send code');
    if (data.alreadyVerified) {
      // Email already verified — skip straight to callback
      if (_emailVerifyCallback) await _emailVerifyCallback();
      return;
    }
    if (okEl) { okEl.textContent = 'Code sent — check your inbox.'; okEl.style.display = 'block'; }
  } catch(e) {
    if (errEl) { errEl.textContent = e.message; errEl.style.display = 'block'; }
  } finally {
    // Resend cooldown
    if (btn) {
      let secs = 60;
      const tick = setInterval(() => {
        secs--;
        btn.textContent = secs > 0 ? `Resend (${secs}s)` : 'Resend code';
        if (secs <= 0) { clearInterval(tick); btn.disabled = false; }
      }, 1000);
    }
  }
}

async function submitEmailVerification() {
  const otp   = document.getElementById('email-verify-otp')?.value.trim();
  const errEl = document.getElementById('email-verify-error');
  const okEl  = document.getElementById('email-verify-ok');
  if (!otp || otp.length !== 6) {
    if (errEl) { errEl.textContent = 'Enter the 6-digit code from your email'; errEl.style.display = 'block'; }
    return;
  }
  if (errEl) errEl.style.display = 'none';
  if (okEl)  okEl.style.display  = 'none';
  const btn = document.querySelector('#wizard-step-1f .btn-primary');
  if (btn) { btn.textContent = '⏳ Verifying…'; btn.disabled = true; }
  try {
    const res = await fetchKV(`${WORKER_URL}/email/verify/check`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _emailVerifyEmailHash, otp }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Verification failed');
    // Verified — continue to next step
    if (_emailVerifyCallback) await _emailVerifyCallback();
  } catch(e) {
    if (errEl) { errEl.textContent = e.message; errEl.style.display = 'block'; }
  } finally {
    if (btn) { btn.textContent = 'Verify email →'; btn.disabled = false; }
  }
}

async function resendEmailVerification() {
  const errEl = document.getElementById('email-verify-error');
  const okEl  = document.getElementById('email-verify-ok');
  if (errEl) errEl.style.display = 'none';
  if (okEl)  okEl.style.display  = 'none';
  await sendEmailVerificationOtp(_emailVerifyEmail, _emailVerifyEmailHash);
}

// ═══════════════════════════════════════════════════════════
//  DATA LOADING OVERLAY
// ═══════════════════════════════════════════════════════════

function showDataLoadingOverlay(statusText) {
  const overlay = document.getElementById('data-loading-overlay');
  const status  = document.getElementById('data-loading-status');
  if (!overlay) return;
  if (status && statusText) status.textContent = statusText;
  overlay.style.display = 'flex';
  // Safety net — always dismiss after 8s even if sync hangs
  clearTimeout(window._loadingOverlayTimeout);
  window._loadingOverlayTimeout = setTimeout(hideDataLoadingOverlay, 8000);
}

function hideDataLoadingOverlay() {
  clearTimeout(window._loadingOverlayTimeout);
  const overlay = document.getElementById('data-loading-overlay');
  if (!overlay || overlay.style.display === 'none') return;
  // Fade out smoothly
  overlay.style.transition = 'opacity 0.4s ease';
  overlay.style.opacity = '0';
  setTimeout(() => {
    overlay.style.display = 'none';
    overlay.style.opacity = '1';
    overlay.style.transition = '';
  }, 400);
}

// ═══════════════════════════════════════════════════════════

const COOKIE_CONSENT_KEY  = 'stockroom_cookie_consent';   // 'granted' | 'declined'
const COOKIE_EMAIL_KEY    = 'stockroom_remembered_email';
const COOKIE_PASSKEY_KEY  = 'stockroom_remembered_passkey'; // 'true' | 'false'

function getCookieConsent() {
  try { return localStorage.getItem(COOKIE_CONSENT_KEY); } catch(e) { return null; }
}
// Passkey registration is device-specific and stored without requiring cookie consent
// (it's functional data, not tracking data — tells us this device has a passkey registered)
const DEVICE_PASSKEY_KEY = 'stockroom_device_has_passkey';

function getDeviceHasPasskey() {
  try { return localStorage.getItem(DEVICE_PASSKEY_KEY) === 'true'; } catch(e) { return false; }
}
function setDeviceHasPasskey(val) {
  try { localStorage.setItem(DEVICE_PASSKEY_KEY, val ? 'true' : 'false'); } catch(e) {}
}

function getRememberedEmail() {
  // Consent is implicit — the email is only ever saved when Remember me is ticked,
  // so if it's present, reading it back is always appropriate.
  try { return localStorage.getItem(COOKIE_EMAIL_KEY) || null; } catch(e) { return null; }
}
function getRememberedPasskey() {
  // First check the device-level flag (no cookie consent required)
  if (getDeviceHasPasskey()) return 'true';
  // Fall back to cookie-consent-gated preference
  if (getCookieConsent() !== 'granted') return null;
  try { return localStorage.getItem(COOKIE_PASSKEY_KEY); } catch(e) { return null; }
}
function setRememberedEmail(email) {
  try { localStorage.setItem(COOKIE_EMAIL_KEY, email); } catch(e) {}
}
function setRememberedPasskey(hasPasskey) {
  // Store in both places so it works with or without cookie consent
  setDeviceHasPasskey(hasPasskey);
  try { localStorage.setItem(COOKIE_PASSKEY_KEY, hasPasskey ? 'true' : 'false'); } catch(e) {}
}
function clearRememberedCookieData() {
  try {
    localStorage.removeItem(COOKIE_EMAIL_KEY);
    localStorage.removeItem(COOKIE_PASSKEY_KEY);
    localStorage.removeItem(COOKIE_CONSENT_KEY);
    // Note: deliberately keep DEVICE_PASSKEY_KEY — it's device state, not a tracking cookie
  } catch(e) {}
}

// Called after successful login — persists email + method if consent granted
function persistLoginCookies(email, hasPasskey) {
  // Always store passkey registration (functional, not tracking)
  if (hasPasskey) setDeviceHasPasskey(true);
  // Save email if Remember me was ticked (consent already granted by checkbox)
  if (_rememberMeChecked || getCookieConsent() === 'granted') {
    setRememberedEmail(email);
    setRememberedPasskey(hasPasskey);
  }
}

// ── Navigation ───────────────────────────────────────────────

function showKvRegister() {
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1')?.classList.add('active');
}

function showKvLogin() {
  // Always clear all wizard steps first — prevents country/protect screens bleeding through
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  const rememberedEmail   = getRememberedEmail();
  const rememberedPasskey = getRememberedPasskey();
  if (rememberedEmail) {
    // Email remembered — skip email entry screen, go straight to auth
    _showAuthStep(rememberedEmail, rememberedPasskey === 'true');
  } else {
    document.getElementById('wizard-step-1b')?.classList.add('active');
    // Reset remember-me checkbox to unchecked for a fresh login
    const cb = document.getElementById('remember-me-checkbox');
    if (cb) cb.checked = false;
    // If this device has a passkey, show hint so users know they can use it
    const deviceHasPK = getDeviceHasPasskey();
    const pkHint = document.getElementById('login-passkey-hint');
    if (pkHint) pkHint.style.display = deviceHasPK ? 'block' : 'none';
  }
}

// The main Continue action — called from inline onclick and Enter key
function doLoginContinue() {
  const emailEl = document.getElementById('kv-login-email');
  const errEl   = document.getElementById('kv-login-email-error');
  if (!emailEl) { if(errEl){errEl.textContent='Field missing';errEl.style.display='block';} return; }
  const email = emailEl.value.trim();
  if (!email || !email.includes('@')) {
    if(errEl){errEl.textContent='Enter a valid email address';errEl.style.display='block';}
    emailEl.focus(); return;
  }
  if(errEl) errEl.style.display='none';

  // If "Remember me" is ticked and consent was just granted inline, save now
  const cb = document.getElementById('remember-me-checkbox');
  if (cb && cb.checked && getCookieConsent() === 'granted') {
    setRememberedEmail(email);
    // passkey preference saved after successful login
  }

  // Navigate to auth step
  const usePasskey = getRememberedPasskey() === 'true';
  _showAuthStep(email, usePasskey);
}

// Keep loginContinue as alias so any cached references still work
function loginContinue() { doLoginContinue(); }

// Internal: switch to the auth step and configure it
function _showAuthStep(email, usePasskey) {
  const wizard = document.getElementById('wizard');
  if (wizard) { wizard.style.display = 'flex'; document.body.classList.add('wizard-active'); }
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  const authStep = document.getElementById('wizard-step-1b-auth');
  if (authStep) authStep.classList.add('active');

  // Populate email display
  const display = document.getElementById('kv-login-email-display');
  if (display) display.value = email;
  const real = document.getElementById('kv-login-email');
  if (real) real.value = email;

  const pk        = document.getElementById('auth-passkey-section');
  const pp        = document.getElementById('auth-passphrase-section');
  const pkOrDiv   = document.getElementById('auth-passkey-or-divider');
  const pkLink    = document.getElementById('auth-use-passkey-link');
  const cookieConsent  = getCookieConsent();
  const pkSupported    = passkeySupported();
  const devicePasskey  = getDeviceHasPasskey();

  if ((usePasskey || devicePasskey) && pkSupported) {
    // Device has passkey registered — show passkey first, passphrase toggle below
    if (pk) pk.style.display = 'block';
    if (pp) pp.style.display = 'none';
    if (pkOrDiv) pkOrDiv.style.display = 'none';
    if (pkLink) pkLink.style.display = 'none';
  } else if ((cookieConsent === 'declined' || cookieConsent === null) && pkSupported) {
    // No cookie consent — show both options stacked
    if (pk) pk.style.display = 'block';
    if (pp) pp.style.display = 'block';
    if (pkOrDiv) pkOrDiv.style.display = 'flex';
    if (pkLink) pkLink.style.display = 'none';
  } else {
    // Consent granted, passkey not remembered — show passphrase with passkey link below
    if (pk) pk.style.display = 'none';
    if (pp) pp.style.display = 'block';
    if (pkOrDiv) pkOrDiv.style.display = 'none';
    // Show "Use passkey instead" link if passkeys are supported on this device
    if (pkLink) pkLink.style.display = pkSupported ? 'inline-flex' : 'none';
    setTimeout(() => { document.getElementById('kv-login-pass')?.focus(); }, 100);
  }
  const errEl = document.getElementById('kv-login-error');
  if (errEl) errEl.style.display = 'none';

  // Pre-check "Remember me" when a remembered email is present or consent was granted
  const authCb = document.getElementById('remember-me-checkbox-auth');
  if (authCb) {
    const shouldCheck = !!getRememberedEmail() || getCookieConsent() === 'granted';
    authCb.checked = shouldCheck;
    _rememberMeChecked = shouldCheck;
  }
}

// Show passphrase section within auth step
function showAuthPassphrase() {
  const pk = document.getElementById('auth-passkey-section');
  const pp = document.getElementById('auth-passphrase-section');
  if (pk) pk.style.display = 'none';
  if (pp) pp.style.display = 'block';
  const errEl = document.getElementById('kv-login-error');
  if (errEl) errEl.style.display = 'none';
  setTimeout(() => { document.getElementById('kv-login-pass')?.focus(); }, 100);
}

// Show passkey section within auth step
function showAuthPasskey() {
  const pk = document.getElementById('auth-passkey-section');
  const pp = document.getElementById('auth-passphrase-section');
  if (pk) pk.style.display = 'block';
  if (pp) pp.style.display = 'none';
}

// "Not you?" — back to email screen, clears remembered data
function loginBackToEmail() {
  clearRememberedCookieData();
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1b')?.classList.add('active');
  const emailEl = document.getElementById('kv-login-email');
  if (emailEl) { emailEl.value = ''; }
  const cb = document.getElementById('remember-me-checkbox');
  if (cb) cb.checked = false;
  const panel = document.getElementById('cookie-inline-panel');
  if (panel) panel.style.display = 'none';
}

// Legacy aliases
function loginChangeUser() { loginBackToEmail(); }
function showAuthScreen(email, usePasskey) { _showAuthStep(email, usePasskey); }
function cookieConsentAccept() { try{localStorage.setItem(COOKIE_CONSENT_KEY,'granted');}catch(e){} }
function cookieConsentDecline() { try{localStorage.setItem(COOKIE_CONSENT_KEY,'declined');}catch(e){} }
function maybeShowCookieConsentBanner() { /* now handled inline on login screen */ }
function applyLoginScreenState() { showKvLogin(); }

// ═══════════════════════════════════════════════════════════

async function kvRegister() {
  const email      = document.getElementById('kv-email')?.value.trim();
  const passphrase = document.getElementById('kv-pass')?.value;
  const errEl      = document.getElementById('kv-wizard-error');
  const btn        = document.querySelector('[onclick="kvRegister()"]');
  if (!email || !passphrase) { if(errEl){errEl.textContent='Enter email and passphrase';errEl.style.display='block';} return; }
  if (passphrase.length < 8) { if(errEl){errEl.textContent='Passphrase must be at least 8 characters';errEl.style.display='block';} return; }
  if (btn) { btn.textContent = '⏳ Creating…'; btn.disabled = true; }
  try {
    const emailHash = await kvHashEmail(email);
    const verifier  = await kvMakeVerifier(passphrase, emailHash);
    // Always use v2 for new registrations — the switchover date only controls migration of
    // existing v1 accounts, not the crypto version chosen for brand-new ones.
    const useV2     = true;

    // Register — send plaintext email so server can send migration notifications
    const res = await fetchKV(`${WORKER_URL}/user/register`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier, email }),
    });
    const data = await res.json();
    if (res.status === 409) { showDuplicateAccountScreen(email); return; }
    if (!res.ok) throw new Error(data.error || 'Registration failed');

    let dataKey, passphraseEnvelope, saltB64, kdfSalt, recoveryCodes, recoveryEnvelopes;

    if (useV2) {
      kdfSalt            = generateKdfSalt();
      const wrapKey      = await derivePassphraseWrapKeyV2(passphrase, emailHash, kdfSalt);
      dataKey            = await generateDataKeyV2Extractable();
      passphraseEnvelope = await wrapDataKeyV2(dataKey, wrapKey);
      saltB64            = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))));
      recoveryCodes      = generateRecoveryCodes(10);
      recoveryEnvelopes  = await buildRecoveryEnvelopesV2(recoveryCodes, dataKey, emailHash);
    } else {
      dataKey            = await generateDataKey();
      const wrapped      = await derivePassphraseWrapKey(passphrase, emailHash, null);
      passphraseEnvelope = await wrapDataKey(dataKey, wrapped.wrapKey);
      saltB64            = wrapped.saltB64;
      recoveryCodes      = generateRecoveryCodes(10);
      recoveryEnvelopes  = await buildRecoveryEnvelopes(recoveryCodes, dataKey, emailHash);
    }

    const storeRes = await fetchKV(`${WORKER_URL}/key/store`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash, verifier, salt: saltB64, passphraseEnvelope, recoveryEnvelopes,
        ...(useV2 ? { kdfSalt } : {}),
      }),
    });
    if (!storeRes.ok) throw new Error('Could not store key envelopes — try again');

    _kvKey = dataKey;
    await kvStoreSession(email, emailHash, verifier, dataKey);
    if(errEl) errEl.style.display = 'none';
    // Clear any stale device setup flags from a previous account on this device
    // so the protect screen shows correctly with the new recovery codes
    localStorage.removeItem('stockroom_protect_seen');
    localStorage.removeItem('stockroom_country_set');
    try {
      const devId = getOrCreateDeviceId();
      await dbPut('settings', `device_setup_${devId}_protect_seen`, null);
      await dbPut('settings', `device_setup_${devId}_country_set`, null);
    } catch(e) {}
    // Verify email ownership before continuing to protect screen
    await showEmailVerification(email, emailHash, () => showProtectDataScreen(recoveryCodes));
  } catch(err) {
    if(errEl){errEl.textContent = err.message; errEl.style.display='block';}
  } finally {
    if (btn) { btn.textContent = 'Create account with passphrase →'; btn.disabled = false; }
  }
}

async function kvLogin() {
  const email      = document.getElementById('kv-login-email')?.value.trim();
  const passphrase = document.getElementById('kv-login-pass')?.value;
  const errEl      = document.getElementById('kv-login-error');
  const btn        = document.querySelector('[onclick="kvLogin()"]');
  if (!email || !passphrase) { if(errEl){errEl.textContent='Enter email and passphrase';errEl.style.display='block';} return; }
  if (btn) { btn.textContent = '⏳ Signing in…'; btn.disabled = true; }
  try {
    const emailHash = await kvHashEmail(email);
    const verifier  = await kvMakeVerifier(passphrase, emailHash);
    const res = await fetchKV(`${WORKER_URL}/user/verify`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier }),
    });
    const data = await res.json();
    if (res.status === 404) throw new Error('Account not found for ' + email + ' — check your email address, or create a new account');
    if (res.status === 429) throw new Error(_handleLoginRateLimit(res, data) || data.error);
    if (res.status === 401) throw new Error(data.error || 'Incorrect passphrase — try again');
    if (!res.ok) throw new Error(data.error || 'Sign-in failed');

    // Fetch key envelope — response carries cryptoVersion, kdfSalt, migrationDue
    let dataKey;
    const keyRes  = await fetchKV(`${WORKER_URL}/key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier }),
    });
    const keyData = await keyRes.json();

    if (keyRes.ok && !keyData.legacy && keyData.envelope) {
      if (keyData.cryptoVersion === 'v2' && keyData.kdfSalt) {
        const wrapKey = await derivePassphraseWrapKeyV2(passphrase, emailHash, keyData.kdfSalt);
        dataKey = await unwrapDataKeyV2(keyData.envelope, wrapKey, true);
      } else {
        const { wrapKey } = await derivePassphraseWrapKey(passphrase, emailHash, keyData.salt);
        dataKey = await unwrapDataKey(keyData.envelope, wrapKey);
      }
    } else {
      dataKey = await kvDeriveKey(email, passphrase);
    }

    await kvStoreSession(email, emailHash, verifier, dataKey);
    if(errEl) errEl.style.display = 'none';
    // Persist email cookie after successful login (no passkey)
    persistLoginCookies(email, false);
    await _trustIfRemembered(email, emailHash, verifier, dataKey);

    // Trigger v1→v2 migration if server says it's due
    if (keyData.migrationDue) {
      await runCryptoMigration(email, emailHash, verifier, passphrase, dataKey);
      return;
    }
    await postLoginWizardRoute();
  } catch(err) {
    if(errEl){errEl.textContent = err.message; errEl.style.display='block';}
  } finally {
    if (btn) { btn.textContent = 'Sign in →'; btn.disabled = false; }
  }
}

// ── Passkey (WebAuthn) helpers ────────────────────────────

function passkeySupported() {
  // Basic check — show buttons if the API exists at all
  return !!(window.PublicKeyCredential && navigator.credentials);
}

async function passkeyPlatformSupported() {
  // More thorough async check
  if (!window.PublicKeyCredential) return false;
  try {
    return await PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable();
  } catch(e) {
    return !!navigator.credentials?.create;
  }
}

function b64urlToUint8(b64) {
  const b64std = b64.replace(/-/g,'+').replace(/_/g,'/');
  const pad    = b64std.length % 4 ? '='.repeat(4 - b64std.length % 4) : '';
  return Uint8Array.from(atob(b64std + pad), c => c.charCodeAt(0));
}

function uint8ToB64url(bytes) {
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}

async function exportPublicKey(credentialResponse) {
  // Extract the SPKI public key from attestation object for storage
  // For simplicity we store the rawId and use it for identification
  return uint8ToB64url(new Uint8Array(credentialResponse.rawId));
}

// ── Passkey Registration ──────────────────────────────────
// DEPRECATED — not called from UI. Passkey is always added AFTER passphrase registration
// via _doAddPasskeyToAccount() from the Protect screen.
async function kvRegisterWithPasskey() {
  console.warn('kvRegisterWithPasskey: deprecated');
}


async function kvLoginWithPasskey() {
  // Support both the normal input and the remembered-email display input
  const email  = (document.getElementById('kv-login-email')?.value.trim()) ||
                 (document.getElementById('kv-login-email-display')?.value.trim());
  const errEl  = document.getElementById('kv-login-error');
  const btn    = document.querySelector('[onclick="kvLoginWithPasskey()"]');
  if (!email) { if(errEl){errEl.textContent='Enter your email address first';errEl.style.display='block';} return; }
  if (!passkeySupported()) { if(errEl){errEl.textContent='Passkeys not supported — use passphrase below';errEl.style.display='block';} return; }
  if (btn) { btn.textContent = '⏳ Checking…'; btn.disabled = true; }
  try {
    const emailHash = await kvHashEmail(email);

    // Get challenge
    const beginRes = await fetchKV(`${WORKER_URL}/passkey/auth/begin`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash }),
    });
    const beginData = await beginRes.json();
    if (beginRes.status === 404) throw new Error('No passkeys found for this account — use passphrase below or register a passkey first');
    if (!beginRes.ok) throw new Error(beginData.error || 'Could not start sign-in');

    // Get assertion from device (triggers Face ID / fingerprint)
    const assertion = await navigator.credentials.get({
      publicKey: {
        challenge:        b64urlToUint8(beginData.challenge),
        rpId:             beginData.rpId,
        allowCredentials: beginData.allowCredentials.map(c => ({
          type: 'public-key',
          id:   b64urlToUint8(c.id),
        })),
        userVerification: beginData.userVerification,
        timeout:          beginData.timeout,
        extensions: {
          prf: { eval: { first: PASSKEY_PRF_SALT } },
        },
      },
    });
    if (!assertion) throw new Error('Sign-in cancelled');

    const credId          = uint8ToB64url(new Uint8Array(assertion.rawId));
    const clientDataJSON  = uint8ToB64url(new Uint8Array(assertion.response.clientDataJSON));
    const authenticatorData = uint8ToB64url(new Uint8Array(assertion.response.authenticatorData));
    const signature       = uint8ToB64url(new Uint8Array(assertion.response.signature));

    // Extract PRF output if available (same salt → same deterministic output)
    const loginExt   = assertion.getClientExtensionResults?.() || {};
    const loginPrf   = loginExt?.prf?.results?.first || null;

    // Finish auth on server
    const finishRes = await fetchKV(`${WORKER_URL}/passkey/auth/finish`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, credentialId: credId, clientDataJSON, authenticatorData, signature }),
    });
    const finishData = await finishRes.json();
    if (!finishRes.ok) throw new Error(finishData.error || 'Sign-in failed');

    const sessionToken = finishData.sessionToken;
    let dataKey = null;

    // ── Fetch the PRF envelope from server then attempt to unwrap ───────
    const envelopeRes = await fetchKV(`${WORKER_URL}/key/passkey-prf-get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, sessionToken, credentialId: credId }),
    }).catch(() => null);

    if (envelopeRes && envelopeRes.ok) {
      const envelopeData = await envelopeRes.json();
      const { prfEnvelope, deviceBound } = envelopeData;

      if (!deviceBound && loginPrf) {
        // ── Path A: PRF — fully E2EE ──────────────────────────────────
        try {
          const prfWrapKey = await prfBytesToWrapKey(loginPrf);
          dataKey = await unwrapKeyWithPrf(prfEnvelope, prfWrapKey);
        } catch(e) { console.warn('[passkey] PRF unwrap failed:', e.message); }
      } else if (deviceBound) {
        // ── Path B: Device-bound IDB key ──────────────────────────────
        try {
          // Check IDB first, fall back to localStorage backup
          let deviceKeyB64 = await dbGet('settings', `passkey_device_key_${credId}`);
          if (!deviceKeyB64) {
            deviceKeyB64 = localStorage.getItem(`stockroom_passkey_dk_${credId}`);
            if (deviceKeyB64) {
              // Restore to IDB for future use
              await dbPut('settings', `passkey_device_key_${credId}`, deviceKeyB64);
            }
          }
          if (deviceKeyB64) {
            const deviceKeyRaw  = Uint8Array.from(atob(deviceKeyB64), c => c.charCodeAt(0));
            const deviceWrapKey = await crypto.subtle.importKey('raw', deviceKeyRaw, 'AES-KW', false, ['unwrapKey']);
            dataKey = await unwrapKeyWithPrf(prfEnvelope, deviceWrapKey);
          }
        } catch(e) { console.warn('[passkey] Device-bound unwrap failed:', e.message); }
      }
    }

    if (!dataKey) {
      // No envelope or unwrap failed — prompt passphrase once to bootstrap this device
      if (btn) { btn.innerHTML = '<svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-key-round"></use></svg> One moment…'; }
      dataKey = await _getKeyViaPassphrase(emailHash, sessionToken, credId, errEl);
      if (!dataKey) {
        if (btn) { btn.innerHTML = '<svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-key-round"></use></svg> Sign in with Face ID / Fingerprint'; btn.disabled = false; }
        return;
      }
    }

    await kvStorePasskeySession(email, emailHash, sessionToken, dataKey);
    if(errEl) errEl.style.display = 'none';
    persistLoginCookies(email, true);
    // Stamp permanent setup flags — these survive sign-out
    await setProtectSeenForDevice();
    await setCountrySetForDevice();
    localStorage.setItem('stockroom_seen', '1');
    await postLoginWizardRoute();
  } catch(err) {
    if (err.name === 'NotAllowedError') {
      if(errEl){errEl.textContent='Sign-in was cancelled — try again';errEl.style.display='block';}
    } else {
      if(errEl){errEl.textContent = err.message; errEl.style.display='block';}
    }
  } finally {
    if (btn) { btn.innerHTML = '<svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-key-round"></use></svg> Sign in with Face ID / Fingerprint'; btn.disabled = false; }
  }
}

// ══════════════════════════════════════════
//  RE-AUTHENTICATION GATE
// ══════════════════════════════════════════
// Prompts for passphrase (or passkey) before sensitive actions.
// Usage: requireReauth('reason text', callback, { passkeyAllowed: true/false })

let _reauthCallback = null;
let _reauthPasskeyAllowed = true;

function requireReauth(reason, callback, opts = {}) {
  if (!kvConnected) { toast('Sign in first'); return; }
  _reauthCallback = callback;
  _reauthPasskeyAllowed = opts.passkeyAllowed !== false;
  document.getElementById('reauth-reason').textContent = reason;
  document.getElementById('reauth-pass').value = '';
  document.getElementById('reauth-error').style.display = 'none';
  // Show/hide passkey option
  const pkOpt = document.getElementById('reauth-passkey-option');
  if (pkOpt) pkOpt.style.display = (_reauthPasskeyAllowed && passkeySupported()) ? 'block' : 'none';
  document.getElementById('reauth-modal').style.display = 'flex';
  setTimeout(() => document.getElementById('reauth-pass')?.focus(), 100);
}

function closeReauth() {
  document.getElementById('reauth-modal').style.display = 'none';
  _reauthCallback = null;
}

async function reauthWithPassphrase() {
  const pass  = document.getElementById('reauth-pass')?.value;
  const errEl = document.getElementById('reauth-error');
  if (!pass) { errEl.textContent = 'Enter your passphrase'; errEl.style.display = 'block'; return; }
  try {
    const verifier = await kvMakeVerifier(pass, _kvEmailHash);
    const res = await fetchKV(`${WORKER_URL}/user/verify`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, verifier }),
    });
    if (res.status === 429) {
      const d = await res.json().catch(() => ({}));
      errEl.textContent = _handleLoginRateLimit(res, d) || 'Too many attempts — please wait';
      errEl.style.display = 'block'; return;
    }
    if (res.status === 401) { errEl.textContent = 'Incorrect passphrase'; errEl.style.display = 'block'; return; }
    if (!res.ok) throw new Error('Verification failed');

    // Also unwrap the data key so _kvKey is available for any callback that needs it
    // (e.g. _doAddPasskeyToAccount needs _kvKey to store a passkey-wrapped copy)
    if (!_kvKey) {
      try {
        const keyRes  = await fetchKV(`${WORKER_URL}/key/get`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ emailHash: _kvEmailHash, verifier }),
        });
        const keyData = await keyRes.json();
        if (keyRes.ok && keyData.envelope) {
          if (keyData.cryptoVersion === 'v2' && keyData.kdfSalt) {
            const wrapKey = await derivePassphraseWrapKeyV2(pass, _kvEmailHash, keyData.kdfSalt);
            _kvKey = await unwrapDataKeyV2(keyData.envelope, wrapKey, true);
          } else if (keyData.salt) {
            const { wrapKey } = await derivePassphraseWrapKey(pass, _kvEmailHash, keyData.salt);
            _kvKey = await unwrapDataKey(keyData.envelope, wrapKey);
          }
          if (_kvKey) {
            // Cache it
            const exported = await crypto.subtle.exportKey('raw', _kvKey);
            const keyB64   = btoa(String.fromCharCode(...new Uint8Array(exported)));
            await lsSetEncrypted('stockroom_kv_session_key', {
              keyData: keyB64, emailHash: _kvEmailHash,
              expiry: Date.now() + 4 * 60 * 60 * 1000,
            });
          }
        }
      } catch(keyErr) {
        console.warn('reauthWithPassphrase: could not fetch key envelope:', keyErr.message);
        // Non-fatal — callback still runs
      }
    }

    errEl.style.display = 'none';
    document.getElementById('reauth-modal').style.display = 'none';
    // MFA intercept — if enabled, show MFA modal before running callback
    if (_mfaEnabled() && _reauthCallback) {
      await _mfaIntercept(_reauthCallback);
      _reauthCallback = null;
    } else {
      if (_reauthCallback) { _reauthCallback(); _reauthCallback = null; }
    }
  } catch(e) {
    errEl.textContent = e.message; errEl.style.display = 'block';
  }
}

async function reauthWithPasskey() {
  const errEl = document.getElementById('reauth-error');
  if (!passkeySupported()) { errEl.textContent = 'Passkeys not supported'; errEl.style.display = 'block'; return; }
  try {
    const beginRes = await fetchKV(`${WORKER_URL}/passkey/auth/begin`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash }),
    });
    const beginData = await beginRes.json();
    if (!beginRes.ok) throw new Error(beginData.error || 'Could not start verification');
    const assertion = await navigator.credentials.get({
      publicKey: {
        challenge:        b64urlToUint8(beginData.challenge),
        rpId:             beginData.rpId,
        allowCredentials: beginData.allowCredentials.map(c => ({
          type: 'public-key', id: b64urlToUint8(c.id),
        })),
        userVerification: beginData.userVerification,
        timeout:          beginData.timeout,
        extensions: { prf: { eval: { first: PASSKEY_PRF_SALT } } },
      },
    });
    if (!assertion) throw new Error('Verification cancelled');
    const credId          = uint8ToB64url(new Uint8Array(assertion.rawId));
    const clientDataJSON  = uint8ToB64url(new Uint8Array(assertion.response.clientDataJSON));
    const authenticatorData = uint8ToB64url(new Uint8Array(assertion.response.authenticatorData));
    const signature       = uint8ToB64url(new Uint8Array(assertion.response.signature));
    const finishRes = await fetchKV(`${WORKER_URL}/passkey/auth/finish`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, credentialId: credId, clientDataJSON, authenticatorData, signature }),
    });
    if (!finishRes.ok) { const d = await finishRes.json().catch(()=>({})); throw new Error(d.error || 'Verification failed'); }
    errEl.style.display = 'none';
    document.getElementById('reauth-modal').style.display = 'none';
    if (_mfaEnabled() && _reauthCallback) {
      await _mfaIntercept(_reauthCallback);
      _reauthCallback = null;
    } else {
      if (_reauthCallback) { _reauthCallback(); _reauthCallback = null; }
    }
  } catch(e) {
    if (e.name === 'NotAllowedError') { errEl.textContent = 'Verification cancelled'; }
    else { errEl.textContent = e.message; }
    errEl.style.display = 'block';
  }
}

// ══════════════════════════════════════════
//  PROTECTING YOUR DATA SCREEN
// ══════════════════════════════════════════

// Shared post-login wizard routing — called by all sign-in paths
async function postLoginWizardRoute(recoveryCodes = []) {
  // ── Step 1: MFA first — authenticate fully before showing any onboarding ──
  // _mfaGate also pulls fresh settings from the server, so _setupProtectSeen /
  // _setupCountrySet / _installDismissed are authoritative when we route below.
  await _mfaGate(async () => {
    // ── Step 2: Now fully authenticated — route to the right onboarding screen ──

    // Re-read flags after the server pull that _mfaGate just did
    const protectSeen = await getProtectSeenForDevice();
    const countrySet  = await getCountrySetForDevice();

    // New accounts always see protect screen (recoveryCodes present).
    // Returning users who have already completed it skip straight to country/stockroom.
    const isReturningUser = protectSeen || (recoveryCodes.length === 0 && _kvAuthMethod === 'passkey');

    if (!isReturningUser) {
      // Show Protecting Your Data screen — MFA already done, so protectContinue()
      // goes directly to country/stockroom without another MFA prompt.
      showProtectDataScreen(recoveryCodes);
    } else if (!countrySet) {
      // Protect already seen, but country/name not set yet
      document.body.classList.add('wizard-active');
      document.getElementById('wizard').style.display = 'flex';
      document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
      document.getElementById('wizard-step-2').classList.add('active');
      wizardCountry = settings.country || 'GB';
      requestAnimationFrame(() => {
        buildCountryGrid();
        selectCountry(wizardCountry);
      });
    } else {
      // Everything done — go straight to Stockroom
      await _enterStockroom();
    }
  });
}

// Shared helper: transition into the Stockroom after all onboarding is complete.
// Called from protectContinue(), wizardFinish(), and postLoginWizardRoute().
// MFA must already be verified before calling this.
async function _enterStockroom() {
  showDataLoadingOverlay('Syncing your data…');
  document.body.classList.remove('wizard-active');
  document.getElementById('wizard').style.display = 'none';
  window.scrollTo(0, 0);
  localStorage.setItem('stockroom_seen', '1');
  const stockTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Stockroom'));
  if (stockTab) showView('stock', stockTab);
  scheduleRender(...RENDER_REGIONS);
  try { await kvSyncNow(true); } finally { hideDataLoadingOverlay(); }
  // Show install banner only after server sync — settings._installDismissed is
  // now authoritative regardless of whether IDB/cookies were cleared.
  setTimeout(maybeShowInstallBanner, 2000);
}

// Show the install banner (Android or iOS) only if not already dismissed.
// Must be called after _mfaGate / kvSyncNow so settings._installDismissed
// reflects the server value, not just whatever survived a cookie clear.
function maybeShowInstallBanner() {
  if (settings._installDismissed) return;
  if (isInStandaloneMode) return; // already installed

  if (isIOS && !isInStandaloneMode) {
    const banner = document.getElementById('ios-install-banner');
    if (banner) banner.style.display = 'block';
  } else if (deferredInstallPrompt) {
    const banner = document.getElementById('install-banner');
    if (banner) banner.classList.add('show');
  }
}

let _protectRecoveryCodes = []; // held in memory during setup only

function showProtectDataScreen(recoveryCodes, isMigration = false) {
  _protectRecoveryCodes = recoveryCodes || [];
  const hasCodes = _protectRecoveryCodes.length > 0;
  const step1d = document.getElementById('wizard-step-1d');
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  if (step1d) {
    step1d.classList.add('active');
    // Update heading and subtext for migration context
    const heading = step1d.querySelector('h1');
    const subtext = step1d.querySelector('p');
    if (isMigration) {
      if (heading) heading.innerHTML = '<svg class="icon icon-md" aria-hidden="true" style="color:var(--ok);vertical-align:-3px"><use href="#i-lock"></use></svg> Encryption upgraded';
      if (subtext)  subtext.textContent = 'Your account now uses stronger encryption. Save your new recovery codes — your old ones no longer work.';
    } else {
      if (heading) heading.textContent = 'Protecting your data';
      if (subtext)  subtext.textContent = 'Complete these steps to keep your account safe.';
    }
  }
  document.body.classList.add('wizard-active'); document.getElementById('wizard').style.display = 'flex';
  // Reset state — including any greying from previous visits
  const passkeySection = document.getElementById('protect-passkey-section');
  if (passkeySection) {
    passkeySection.style.opacity       = '';
    passkeySection.style.pointerEvents = '';
    passkeySection.style.borderColor   = '';
    passkeySection.style.background    = '';
    passkeySection.style.display       = '';
    passkeySection.querySelectorAll('.protect-skip-msg').forEach(el => el.remove());
  }
  document.getElementById('protect-passkey-done').style.display    = 'none';
  document.getElementById('protect-passkey-buttons').style.display  = 'flex';
  document.getElementById('protect-codes-grid').style.display       = 'none';
  document.getElementById('protect-codes-confirm').style.display    = 'none';
  document.getElementById('protect-codes-checkbox').checked         = false;
  if (hasCodes) {
    document.getElementById('protect-codes-hidden').style.display     = '';
    document.getElementById('protect-continue-btn').disabled          = true;
    document.getElementById('protect-continue-btn').style.opacity     = '0.5';
  } else {
    document.getElementById('protect-codes-hidden').innerHTML = '<p style="font-size:12px;color:var(--ok);line-height:1.5">✓ Recovery codes already set up. Generate new ones in Settings → Account if needed.</p>';
    // Tick the checkbox so updateProtectContinueBtn() doesn't re-disable the button
    document.getElementById('protect-codes-checkbox').checked         = true;
    document.getElementById('protect-continue-btn').disabled  = false;
    document.getElementById('protect-continue-btn').style.opacity = '1';
  }
  // Hide passkey option if not supported; show as done if already registered on this device
  passkeyPlatformSupported().then(supported => {
    const section = document.getElementById('protect-passkey-section');
    if (!supported) {
      if (section) section.style.display = 'none';
      return;
    }
    // If this device already has a passkey registered, show it as already done
    if (getDeviceHasPasskey()) {
      _protectPasskeyDone();
      const doneEl = document.getElementById('protect-passkey-done');
      if (doneEl) doneEl.textContent = '✓ Passkey already registered on this device';
    }
  });
}

function revealRecoveryCodes() {
  const grid = document.getElementById('protect-codes-list');
  if (!grid || !_protectRecoveryCodes.length) return;
  grid.innerHTML = _protectRecoveryCodes.map((c, i) =>
    `<div style="padding:3px 0"><span style="color:var(--muted)">${String(i+1).padStart(2,'0')}.</span> <strong>${c}</strong></div>`
  ).join('');
  document.getElementById('protect-codes-grid').style.display   = '';
  document.getElementById('protect-codes-hidden').style.display = 'none';
  document.getElementById('protect-codes-confirm').style.display = 'flex';
}

function copyRecoveryCodes() {
  const text = _protectRecoveryCodes.map((c, i) => `${i+1}. ${c}`).join('\n');
  navigator.clipboard?.writeText(text).then(() => toast('Recovery codes copied ✓')).catch(() => {});
}

async function protectAddPasskey() {
  // On initial passkey signup there is no passphrase, so call _doAddPasskeyToAccount
  // directly (already authenticated) rather than routing through requireReauth.
  try {
    await _doAddPasskeyToAccount();
    _protectPasskeyDone();
  } catch(err) {
    // Show error persistently in the protect screen rather than just a toast
    const btn = document.querySelector('#protect-passkey-buttons .btn-primary');
    if (btn) btn.textContent = 'Add passkey';
    // Find or create an error element in the passkey section
    let errEl = document.getElementById('protect-passkey-error');
    if (!errEl) {
      errEl = document.createElement('p');
      errEl.id = 'protect-passkey-error';
      errEl.style.cssText = 'font-size:12px;color:var(--danger);margin-top:8px;line-height:1.5';
      document.getElementById('protect-passkey-section')?.appendChild(errEl);
    }
    if (err.name === 'NotAllowedError') {
      errEl.textContent = 'Passkey setup was cancelled — try again';
    } else {
      errEl.textContent = 'Could not add passkey: ' + err.message;
    }
  }
}

function _protectPasskeyDone() {
  const doneEl  = document.getElementById('protect-passkey-done');
  const buttons = document.getElementById('protect-passkey-buttons');
  const section = document.getElementById('protect-passkey-section');
  if (doneEl)  { doneEl.style.display  = ''; doneEl.textContent = '✓ Passkey added — you can sign in with Face ID / Fingerprint'; }
  if (buttons) buttons.style.display = 'none';
  // Grey the section like skip does, so it's visually "done and locked"
  if (section) {
    section.style.opacity      = '0.6';
    section.style.pointerEvents = 'none';
    section.style.borderColor  = 'rgba(76,187,138,0.4)';
    section.style.background   = 'rgba(76,187,138,0.04)';
  }
}

function protectSkipPasskey() {
  const section = document.getElementById('protect-passkey-section');
  if (section) {
    section.style.opacity      = '0.4';
    section.style.pointerEvents = 'none';
    document.getElementById('protect-passkey-buttons').style.display = 'none';
    // Remove any previous skip message to avoid duplicates
    section.querySelectorAll('.protect-skip-msg').forEach(el => el.remove());
    const skip = document.createElement('div');
    skip.className    = 'protect-skip-msg';
    skip.style.cssText = 'font-size:12px;color:var(--muted);margin-top:4px';
    skip.textContent   = 'Skipped — you can add a passkey later in Settings';
    section.appendChild(skip);
  }
}

function updateProtectContinueBtn() {
  const checked = document.getElementById('protect-codes-checkbox')?.checked;
  const btn     = document.getElementById('protect-continue-btn');
  if (btn) { btn.disabled = !checked; btn.style.opacity = checked ? '1' : '0.5'; }
}

async function protectContinue() {
  _protectRecoveryCodes = [];
  await setProtectSeenForDevice();
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  const countrySet = await getCountrySetForDevice();
  if (countrySet) {
    // MFA already verified before we got here — go straight to Stockroom
    await _enterStockroom();
  } else {
    document.getElementById('wizard-step-2')?.classList.add('active');
  }
}

// Open Security Checklist from Settings
async function openSecurityChecklist() {
  if (!kvConnected) { toast('Sign in first'); return; }
  showProtectDataScreen([]);
}

// ══════════════════════════════════════════
//  RECOVERY CODE SIGN-IN
// ══════════════════════════════════════════

// ── Recovery Step A: Enter email ─────────────────────────
async function recoveryStepEmail() {
  const email = document.getElementById('recovery-email')?.value.trim();
  const errEl = document.getElementById('recovery-email-error');
  if (!email) { if(errEl){errEl.textContent='Enter your email address';errEl.style.display='block';} return; }
  _recoveryEmail = email;
  _recoveryEmailHash = await kvHashEmail(email);
  if(errEl) errEl.style.display='none';
  document.getElementById('recovery-step-email').style.display = 'none';
  // Show method choice screen — offer passkey if this device has one
  const methodStep = document.getElementById('recovery-step-method');
  const pkOption   = document.getElementById('recovery-passkey-option');
  if (methodStep) {
    if (pkOption) pkOption.style.display = (getDeviceHasPasskey() && passkeySupported()) ? 'block' : 'none';
    methodStep.style.display = '';
  } else {
    // Fallback: skip straight to code
    document.getElementById('recovery-step-code').style.display = '';
    setTimeout(() => document.getElementById('recovery-code-input')?.focus(), 100);
  }
}

function recoveryChooseCode() {
  document.getElementById('recovery-step-method').style.display = 'none';
  document.getElementById('recovery-step-code').style.display = '';
  setTimeout(() => document.getElementById('recovery-code-input')?.focus(), 100);
}

async function recoveryWithPasskey() {
  const errEl = document.getElementById('recovery-method-error');
  if(errEl) errEl.style.display = 'none';
  try {
    const emailHash = _recoveryEmailHash || await kvHashEmail(_recoveryEmail);
    const beginRes = await fetchKV(`${WORKER_URL}/passkey/auth/begin`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash }),
    });
    const beginData = await beginRes.json();
    if (!beginRes.ok) throw new Error(beginData.error || 'Could not start passkey verification');
    const assertion = await navigator.credentials.get({
      publicKey: {
        challenge:        b64urlToUint8(beginData.challenge),
        rpId:             beginData.rpId,
        allowCredentials: beginData.allowCredentials.map(c => ({ type: 'public-key', id: b64urlToUint8(c.id) })),
        userVerification: beginData.userVerification,
        timeout:          beginData.timeout,
        extensions: { prf: { eval: { first: PASSKEY_PRF_SALT } } },
      },
    });
    if (!assertion) throw new Error('Cancelled');
    const credId           = uint8ToB64url(new Uint8Array(assertion.rawId));
    const clientDataJSON   = uint8ToB64url(new Uint8Array(assertion.response.clientDataJSON));
    const authenticatorData = uint8ToB64url(new Uint8Array(assertion.response.authenticatorData));
    const signature        = uint8ToB64url(new Uint8Array(assertion.response.signature));
    const finishRes = await fetchKV(`${WORKER_URL}/passkey/auth/finish`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, credentialId: credId, clientDataJSON, authenticatorData, signature }),
    });
    const finishData = await finishRes.json();
    if (!finishRes.ok) throw new Error(finishData.error || 'Passkey verification failed');
    // Passkey verified — store session token for OTP step, then send email code
    _recoverySessionToken = finishData.sessionToken;
    document.getElementById('recovery-step-method').style.display = 'none';
    await _recoverySendOtp(emailHash);
  } catch(e) {
    if (errEl) { errEl.textContent = e.name === 'NotAllowedError' ? 'Cancelled — try again' : e.message; errEl.style.display = 'block'; }
  }
}

// ── Recovery Step B: Enter recovery code → validate → send email OTP ──
async function recoveryStepCode() {
  const code  = document.getElementById('recovery-code-input')?.value.trim();
  const errEl = document.getElementById('recovery-code-error');
  const btn   = document.querySelector('[onclick="recoveryStepCode()"]');
  if (!code || code.replace(/-/g,'').length < 16) {
    if(errEl){errEl.textContent='Enter a valid recovery code (XXXX-XXXX-XXXX-XXXX)';errEl.style.display='block';} return;
  }
  if (btn) { btn.textContent = '⏳ Verifying…'; btn.disabled = true; }
  try {
    const emailHash = await kvHashEmail(_recoveryEmail);
    const codeHash  = await hashRecoveryCode(code, emailHash);
    const res = await fetchKV(`${WORKER_URL}/key/recover`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, codeHash }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Invalid recovery code');
    const wrapKey = await deriveRecoveryWrapKey(code, emailHash);
    const dataKey = await unwrapDataKey(data.envelope, wrapKey);
    _recoveryEmailHash = emailHash;
    _recoveryToken     = data.recoveryToken;
    _recoveryDataKey   = dataKey;
    // Send email OTP as second factor
    const otpRes = await fetchKV(`${WORKER_URL}/recovery/request`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: _recoveryEmail }),
    });
    if (!otpRes.ok) {
      const otpData = await otpRes.json().catch(()=>({}));
      throw new Error(otpData.error || 'Could not send email verification');
    }
    if(errEl) errEl.style.display = 'none';
    document.getElementById('recovery-step-code').style.display = 'none';
    document.getElementById('recovery-step-otp').style.display  = '';
    setTimeout(() => document.getElementById('recovery-otp-input')?.focus(), 100);
  } catch(err) {
    if(errEl){errEl.textContent = err.message; errEl.style.display='block';}
  } finally {
    if (btn) { btn.textContent = 'Verify code →'; btn.disabled = false; }
  }
}

// ── Recovery Step C: Verify email OTP ───────────────────
async function recoveryStepOtp() {
  const otp   = document.getElementById('recovery-otp-input')?.value.trim();
  const errEl = document.getElementById('recovery-otp-error');
  const btn   = document.querySelector('[onclick="recoveryStepOtp()"]');
  if (!otp || otp.length !== 6) { if(errEl){errEl.textContent='Enter the 6-digit code from your email';errEl.style.display='block';} return; }
  if (btn) { btn.textContent = '⏳ Verifying…'; btn.disabled = true; }
  try {
    const res = await fetchKV(`${WORKER_URL}/recovery/verify`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: _recoveryEmail, otp }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Invalid code');
    if(errEl) errEl.style.display = 'none';
    document.getElementById('recovery-step-otp').style.display   = 'none';
    document.getElementById('recovery-step-reset').style.display = '';
    setTimeout(() => document.getElementById('recovery-new-pass')?.focus(), 100);
  } catch(err) {
    if(errEl){errEl.textContent = err.message; errEl.style.display='block';}
  } finally {
    if (btn) { btn.textContent = 'Verify email code →'; btn.disabled = false; }
  }
}

async function recoveryResendOtp() {
  try {
    await fetchKV(`${WORKER_URL}/recovery/request`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: _recoveryEmail }),
    });
    toast('Verification code resent');
  } catch(e) { toast('Could not resend — try again'); }
}

function recoveryBack(step) {
  document.getElementById('recovery-step-method').style.display = 'none';
  document.getElementById('recovery-step-code').style.display   = 'none';
  document.getElementById('recovery-step-otp').style.display    = 'none';
  document.getElementById('recovery-step-reset').style.display  = 'none';
  if (step === 'email')  document.getElementById('recovery-step-email').style.display = '';
  else if (step === 'method') document.getElementById('recovery-step-method').style.display = '';
  else if (step === 'code')   document.getElementById('recovery-step-code').style.display = '';
}

// ── Recovery Step D: Set new passphrase ─────────────────
async function completeRecovery() {
  const newPass     = document.getElementById('recovery-new-pass')?.value;
  const confirmPass = document.getElementById('recovery-confirm-pass')?.value;
  const errEl       = document.getElementById('recovery-reset-error');
  const btn         = document.querySelector('[onclick="completeRecovery()"]');
  if (!newPass || newPass.length < 8) { if(errEl){errEl.textContent='Passphrase must be at least 8 characters';errEl.style.display='block';} return; }
  if (newPass !== confirmPass) { if(errEl){errEl.textContent='Passphrases do not match';errEl.style.display='block';} return; }
  if (!_recoveryToken || !_recoveryEmailHash || !_recoveryDataKey) {
    if(errEl){errEl.textContent='Recovery session expired — start again';errEl.style.display='block';} return;
  }
  if (btn) { btn.textContent = '⏳ Resetting…'; btn.disabled = true; }
  try {
    const { wrapKey, saltB64 } = await derivePassphraseWrapKey(newPass, _recoveryEmailHash, null);
    const newVerifier          = await kvMakeVerifier(newPass, _recoveryEmailHash);
    const newEnvelope          = await wrapDataKey(_recoveryDataKey, wrapKey);
    const res = await fetchKV(`${WORKER_URL}/recovery/reset`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _recoveryEmailHash, recoveryToken: _recoveryToken, newVerifier, newSalt: saltB64, newEnvelope }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Reset failed');
    await kvStoreSession(_recoveryEmail, _recoveryEmailHash, newVerifier, _recoveryDataKey);
    const newCodes     = generateRecoveryCodes(10);
    const newEnvelopes = await buildRecoveryEnvelopes(newCodes, _recoveryDataKey, _recoveryEmailHash);
    await fetchKV(`${WORKER_URL}/key/update-recovery`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _recoveryEmailHash, verifier: newVerifier, recoveryEnvelopes: newEnvelopes }),
    });
    _recoveryEmail = _recoveryEmailHash = _recoveryToken = '';
    _recoveryDataKey = null;
    if(errEl) errEl.style.display = 'none';
    localStorage.setItem('stockroom_seen', '1');
    toast('Access restored ✓ — please save your new recovery codes');
    showProtectDataScreen(newCodes);
  } catch(err) {
    if(errEl){errEl.textContent = err.message; errEl.style.display='block';}
  } finally {
    if (btn) { btn.textContent = 'Reset passphrase & Sign in →'; btn.disabled = false; }
  }
}

function showDuplicateAccountScreen(email) {
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1e')?.classList.add('active');
  const display = document.getElementById('duplicate-email-display');
  if (display) display.textContent = email || '';
  // Pre-fill login and recovery email fields so all routes start ready
  const loginEl  = document.getElementById('kv-login-email');
  const recovEl  = document.getElementById('recovery-email');
  if (loginEl)  loginEl.value  = email || '';
  if (recovEl)  recovEl.value  = email || '';
}

function duplicateGoToLogin() {
  // Email already pre-filled by showDuplicateAccountScreen
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1b')?.classList.add('active');
  // Clear any stale login error
  const errEl = document.getElementById('kv-login-error');
  if (errEl) errEl.style.display = 'none';
}

function duplicateGoToRecovery() {
  // Email already pre-filled; show step-1c at the email sub-step
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1c')?.classList.add('active');
  document.getElementById('recovery-step-email').style.display = '';
  document.getElementById('recovery-step-code').style.display  = 'none';
  document.getElementById('recovery-step-otp').style.display   = 'none';
  document.getElementById('recovery-step-reset').style.display = 'none';
  const errEl = document.getElementById('recovery-email-error');
  if (errEl) errEl.style.display = 'none';
  setTimeout(() => document.getElementById('recovery-code-input')?.focus(), 100);
}

function duplicateGoToImport() {
  // Dismiss wizard, land on settings, trigger file picker
  localStorage.setItem('stockroom_seen', '1');
  document.body.classList.remove('wizard-active');
  document.getElementById('wizard').style.display = 'none';
  const settingsTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Settings'));
  if (settingsTab) showView('settings', settingsTab);
  setTimeout(() => document.getElementById('import-file')?.click(), 300);
}

function duplicateGoToNewAccount() {
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1')?.classList.add('active');
  // Clear the email that caused the conflict and focus it with a note
  const emailEl = document.getElementById('kv-email');
  if (emailEl) { emailEl.value = ''; emailEl.focus(); }
  const errEl = document.getElementById('kv-wizard-error');
  if (errEl) {
    errEl.textContent = 'That email is already registered — enter a different one to create a new account.';
    errEl.style.display = 'block';
  }
}

function showDecryptErrorBanner() {
  // Remove any existing banner first
  document.getElementById('kv-decrypt-error-banner')?.remove();

  const banner = document.createElement('div');
  banner.id = 'kv-decrypt-error-banner';
  banner.style.cssText = [
    'position:fixed;top:0;left:0;right:0;z-index:9000',
    'background:var(--danger,#e05c5c);color:#fff',
    'padding:14px 16px;font-size:13px;line-height:1.5',
    'display:flex;align-items:center;justify-content:space-between;gap:12px',
    'box-shadow:0 2px 12px rgba(0,0,0,0.4)',
  ].join(';');

  banner.innerHTML = `
    <div>
      <strong>⚠️ Your data could not be decrypted.</strong><br>
      <span style="opacity:.9;font-size:12px">Your items are safe on the server — re-entering your passphrase will restore them.</span>
    </div>
    <button onclick="dismissDecryptErrorAndReauth()" style="
      background:rgba(255,255,255,0.2);border:1px solid rgba(255,255,255,0.4);
      color:#fff;border-radius:8px;padding:7px 14px;font-size:13px;
      cursor:pointer;white-space:nowrap;flex-shrink:0
    ">Re-enter passphrase</button>
  `;
  document.body.prepend(banner);
}

async function dismissDecryptErrorAndReauth() {
  document.getElementById('kv-decrypt-error-banner')?.remove();
  // Wipe ALL cached key material — including device trust — so kvEnsureKey
  // cannot restore a bad key and is forced to show the passphrase prompt.
  _kvKey = null;
  try { localStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
  try { localStorage.removeItem('stockroom_kv_key_fallback'); } catch(e) {}
  try { localStorage.removeItem('stockroom_device_secret'); } catch(e) {}
  try { sessionStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
  try { await removeWrappedKey(getOrCreateDeviceId()); } catch(e) {}
  // Show passphrase prompt directly — don't go through kvEnsureKey's
  // device-trust branches which might restore another cached bad key.
  const result = await showPassphrasePrompt();
  if (!result) return;
  const { passphrase, trust } = result;
  try {
    // Build auth credentials — passkey sessions have no verifier, use sessionToken instead
    const verifier = _kvVerifier || (await kvMakeVerifier(passphrase, _kvEmailHash));
    let dataKey;
    try {
      const authBody = _kvSessionToken && !_kvVerifier
        ? { emailHash: _kvEmailHash, sessionToken: _kvSessionToken }
        : { emailHash: _kvEmailHash, verifier };
      const keyRes  = await fetchKV(`${WORKER_URL}/key/get`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(authBody),
      });
      const keyData = await keyRes.json();
      if (keyRes.status === 401) { toast('Incorrect passphrase — try again'); showDecryptErrorBanner(); return; }
      if (keyRes.ok && !keyData.legacy && keyData.envelope) {
        // Try v2 unwrap first, fall back to v1
        try {
          if (keyData.cryptoVersion === 'v2' && keyData.kdfSalt) {
            const wrapKey = await derivePassphraseWrapKeyV2(passphrase, _kvEmailHash, keyData.kdfSalt);
            dataKey = await unwrapDataKeyV2(keyData.envelope, wrapKey, true);
          } else {
            const { wrapKey } = await derivePassphraseWrapKey(passphrase, _kvEmailHash, keyData.salt);
            dataKey = await unwrapDataKey(keyData.envelope, wrapKey);
          }
        } catch(unwrapErr) {
          console.warn('Key unwrap failed:', unwrapErr.message);
        }
      }
    } catch(e) { console.warn('Key fetch failed:', e.message); }
    if (!dataKey) {
      // Last resort: v1 derive (legacy accounts only)
      dataKey = await kvDeriveKey(_kvEmail, passphrase);
    }
    _kvKey = dataKey;
    // Re-cache as fresh 4-hour session
    try {
      const exported = await crypto.subtle.exportKey('raw', _kvKey);
      const keyB64   = btoa(String.fromCharCode(...new Uint8Array(exported)));
      await lsSetEncrypted('stockroom_kv_session_key', {
        keyData: keyB64, emailHash: _kvEmailHash,
        expiry: Date.now() + 4 * 60 * 60 * 1000,
      });
    } catch(e) {}
    if (trust) await trustThisDeviceWith(_kvEmail, _kvEmailHash, _kvVerifier, _kvKey);
    updateSyncPill('syncing');
    await kvSyncNow();
  } catch(e) {
    toast('Could not unlock — ' + e.message);
    showDecryptErrorBanner();
  }
}

// ── Crypto v1 → v2 migration ──────────────────────────────
// Triggered on login when server reports migrationDue = true.
// The user is already signed in with their v1 key in memory.
// We re-encrypt the server ciphertext with a fresh v2 key and push.
async function runCryptoMigration(email, emailHash, verifier, passphrase, v1DataKey) {
  try {
    // Show a non-dismissible progress overlay
    const overlay = document.createElement('div');
    overlay.id = 'crypto-migration-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.85);z-index:10000;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;color:#fff;font-family:var(--sans)';
    overlay.innerHTML = `
      <div style="color:var(--accent)"><svg aria-hidden="true" style="width:40px;height:40px"><use href="#i-lock"></use></svg></div>
      <div style="font-size:18px;font-weight:700">Upgrading your encryption</div>
      <div id="crypto-migration-status" style="font-size:13px;color:rgba(255,255,255,0.7);text-align:center;max-width:300px;line-height:1.6">
        Fetching your data…
      </div>
      <div style="width:200px;height:4px;background:rgba(255,255,255,0.15);border-radius:2px;overflow:hidden">
        <div id="crypto-migration-bar" style="height:100%;width:0%;background:var(--accent,#e8a838);border-radius:2px;transition:width 0.4s"></div>
      </div>`;
    document.body.appendChild(overlay);

    const setStatus = (msg, pct) => {
      const s = document.getElementById('crypto-migration-status');
      const b = document.getElementById('crypto-migration-bar');
      if (s) s.textContent = msg;
      if (b) b.style.width = pct + '%';
    };

    // 1. Pull current (v1) ciphertext from server
    setStatus('Fetching your data…', 10);
    const pullRes = await fetchKV(`${WORKER_URL}/data/pull`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier }),
    });
    if (!pullRes.ok) throw new Error('Could not fetch data for migration');
    const { ciphertext: v1Ciphertext } = await pullRes.json();

    // 2. Decrypt with v1 key
    setStatus('Decrypting with current key…', 25);
    let plaintext;
    if (v1Ciphertext) {
      plaintext = await kvDecrypt(v1DataKey, v1Ciphertext);
    }

    // 3. Generate fresh v2 key material
    setStatus('Generating new encryption key…', 40);
    const kdfSalt         = generateKdfSalt();
    const newVerifier     = verifier; // passphrase unchanged during migration
    const wrapKey         = await derivePassphraseWrapKeyV2(passphrase, emailHash, kdfSalt);
    const v2DataKey       = await generateDataKeyV2Extractable();
    const passphraseEnv   = await wrapDataKeyV2(v2DataKey, wrapKey);
    const saltB64         = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))));

    // 4. Re-encrypt data with v2 key
    setStatus('Re-encrypting your data…', 55);
    const v2Ciphertext = plaintext ? await kvEncrypt(v2DataKey, plaintext) : null;

    // 5. Generate fresh recovery envelopes with v2
    setStatus('Updating recovery codes…', 70);
    const recoveryCodes     = generateRecoveryCodes(10);
    const recoveryEnvelopes = await buildRecoveryEnvelopesV2(recoveryCodes, v2DataKey, emailHash);

    // 6. Push to server — atomically archives v1 and writes v2
    setStatus('Saving to server…', 85);
    const migrateRes = await fetchKV(`${WORKER_URL}/crypto/migrate`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash,
        verifier,
        newVerifier,
        newSalt:              saltB64,
        newEnvelope:          passphraseEnv,
        newKdfSalt:           kdfSalt,
        newRecoveryEnvelopes: recoveryEnvelopes,
        ciphertext:           v2Ciphertext,
      }),
    });
    if (!migrateRes.ok) {
      const d = await migrateRes.json().catch(() => ({}));
      throw new Error(d.error || 'Migration failed — your data is unchanged');
    }

    // 7. Update local session with v2 key
    setStatus('Done! Finishing up…', 95);
    _kvKey = v2DataKey;
    await kvStoreSession(email, emailHash, verifier, v2DataKey);

    // 8. Re-backup all share keys encrypted with the new v2 data key.
    // The old backups were encrypted with the v1 key and are now unreadable.
    // We also re-push shared data so guests get a fresh copy under the new owner key.
    if (_shareTargets?.length) {
      setStatus('Re-encrypting share keys…', 97);
      for (const target of _shareTargets) {
        try {
          // Recover the share key — it may still be in localStorage cache from this session
          const sk = await recoverShareKeyWithOldKey(target.code, v1DataKey);
          if (sk) {
            // Back up with new v2 key
            await backupShareKey(target.code, sk);
            // Re-push shared data (owner now has v2 key in _kvKey)
            await pushSharedData(target.code, sk);
          } else {
            console.warn('Migration: could not recover share key for', target.code, '— share backup skipped');
          }
        } catch(e) {
          console.warn('Migration: share key re-backup failed for', target.code, e.message);
        }
      }
    }

    overlay.remove();

    // Show recovery codes — user must save new v2 codes
    showProtectDataScreen(recoveryCodes, true /* isMigration */);

  } catch(err) {
    document.getElementById('crypto-migration-overlay')?.remove();
    console.error('Migration failed:', err);
    // Non-fatal — user can still use the app on v1; migration will retry next login
    toast('Encryption upgrade failed — ' + err.message + '. Will retry next sign-in.');
    await postLoginWizardRoute();
  }
}

// ── Sync pill 5-tap debug trigger ─────────────────────────
let _syncPillTaps = 0;
let _syncPillTimer = null;

function handleSyncPillTap() {
  _syncPillTaps++;
  if (_syncPillTimer) clearTimeout(_syncPillTimer);
  _syncPillTimer = setTimeout(() => { _syncPillTaps = 0; }, 1500);
  if (_syncPillTaps >= 5) {
    _syncPillTaps = 0;
    showMobileDiag();
  }
}

async function showMobileDiag() {
  // Collect diagnostic info
  const lines = [];
  lines.push(`kvConnected: ${kvConnected}`);
  lines.push(`_kvEmail: ${_kvEmail || '(empty)'}`);
  lines.push(`_kvEmailHash: ${_kvEmailHash || '(empty)'}`);
  lines.push(`_kvVerifier: ${_kvVerifier ? _kvVerifier.slice(0,8)+'…' : '(empty)'}`);
  lines.push(`_kvKey: ${_kvKey ? 'SET' : 'NULL'}`);
  lines.push(`_shareState: ${_shareState ? _shareState.code : 'null'}`);
  lines.push(`_shareKey: ${_shareKey ? 'SET' : 'null'}`);
  lines.push(`local items: ${items?.length ?? '?'}`);

  try {
    const sk = await lsGetEncrypted('stockroom_kv_session_key');
    lines.push(`session_key: ${sk ? `SET (exp ${new Date(sk.expiry).toLocaleTimeString()}, match: ${sk.emailHash === _kvEmailHash})` : 'absent'}`);
  } catch(e) { lines.push('session_key: parse error'); }

  lines.push(`device_secret: ${localStorage.getItem('stockroom_device_secret') ? 'SET' : 'absent'}`);

  const localKeys = await _getShareKeys();
  lines.push(`share_keys: ${Object.keys(localKeys).join(', ') || 'none'}`);

  // Server checks if we have credentials
  if (_kvEmailHash && _kvVerifier) {
    try {
      const r = await fetchKV(`${WORKER_URL}/key/get`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, verifier: _kvVerifier }),
      });
      const d = await r.json();
      lines.push(`key/get: ${r.status} cv=${d.cryptoVersion||'?'} env=${!!d.envelope} kdf=${!!d.kdfSalt} mig=${d.migrationDue}`);
    } catch(e) { lines.push(`key/get error: ${e.message}`); }

    try {
      const r = await fetchKV(`${WORKER_URL}/data/pull`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, verifier: _kvVerifier }),
      });
      const d = await r.json();
      lines.push(`data/pull: ${r.status} ct=${d.ciphertext ? d.ciphertext.length+'ch' : 'none'}`);
    } catch(e) { lines.push(`data/pull error: ${e.message}`); }
  } else {
    lines.push('(no credentials — server checks skipped)');
  }

  // Show modal
  const modal = document.createElement('div');
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.85);z-index:99999;display:flex;flex-direction:column;padding:20px;overflow-y:auto';
  modal.innerHTML = `
    <div style="background:#1a1d27;border:1px solid #2a2d3a;border-radius:12px;padding:16px;max-width:500px;width:100%;margin:auto">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <span style="font-size:13px;font-weight:700;color:#e8a838"><svg class="icon" aria-hidden="true"><use href="#i-wrench"></use></svg> Diagnostic</span>
        <button onclick="this.closest('[style*=fixed]').remove()" style="background:transparent;border:none;color:#6b7280;font-size:18px;cursor:pointer;padding:0 4px"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
      </div>
      <pre style="font-family:monospace;font-size:11px;color:#e8e8f0;line-height:1.8;white-space:pre-wrap;word-break:break-all;margin:0 0 12px">${lines.join('\n')}</pre>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button onclick="navigator.clipboard?.writeText(${JSON.stringify(lines.join('\n'))}).then(()=>toast('Copied ✓'))" style="background:#2a2d3a;border:none;color:#e8e8f0;border-radius:8px;padding:8px 14px;font-size:12px;cursor:pointer">Copy</button>
        <button onclick="clearLocalKeyMaterial();this.closest('[style*=fixed]').remove()" style="background:#e05c5c;border:none;color:#fff;border-radius:8px;padding:8px 14px;font-size:12px;cursor:pointer">Clear cached keys & retry</button>
        <button onclick="this.closest('[style*=fixed]').remove()" style="background:transparent;border:1px solid #2a2d3a;color:#6b7280;border-radius:8px;padding:8px 14px;font-size:12px;cursor:pointer">Close</button>
      </div>
    </div>`;
  document.body.appendChild(modal);
}

async function clearLocalKeyMaterial() {
  _kvKey = null;
  try { localStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
  try { localStorage.removeItem('stockroom_kv_key_fallback'); } catch(e) {}
  try { localStorage.removeItem('stockroom_device_secret'); } catch(e) {}
  try { sessionStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
  try { await removeWrappedKey(getOrCreateDeviceId()); } catch(e) {}
  document.getElementById('kv-decrypt-error-banner')?.remove();
  toast('Cached keys cleared — signing in again…');
  // Re-attempt sync which will prompt for passphrase cleanly
  await kvSyncNow();
}

function showForgotPassphrase() {
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  document.getElementById('wizard-step-1c')?.classList.add('active');
  const loginEmail = document.getElementById('kv-login-email')?.value.trim();
  if (loginEmail) {
    const recovEl = document.getElementById('recovery-email');
    if (recovEl) recovEl.value = loginEmail;
  }
  document.getElementById('recovery-step-email').style.display = '';
  document.getElementById('recovery-step-code').style.display  = 'none';
  document.getElementById('recovery-step-otp').style.display   = 'none';
  document.getElementById('recovery-step-reset').style.display = 'none';
}

// ── Store passkey session (no encryption key — different model) ──
// ── Passkey key helpers ────────────────────────────────────

// Try to fetch the data key from the server passkey-wrap store.
// Returns the CryptoKey if found, null if not yet stored.
async function _fetchPasskeyWrappedKey(emailHash, sessionToken, credentialId) {
  try {
    const res  = await fetchKV(`${WORKER_URL}/key/passkey-unwrap`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, sessionToken, credentialId }),
    });
    if (!res.ok) return null; // 404 = not stored yet, other errors = skip
    const { rawKeyB64 } = await res.json();
    if (!rawKeyB64) return null;
    const rawBytes = Uint8Array.from(atob(rawKeyB64), c => c.charCodeAt(0));
    const key = await crypto.subtle.importKey('raw', rawBytes, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
    // Cache locally for 24h so page refreshes don't hit the server
    const keyB64 = btoa(String.fromCharCode(...rawBytes));
    await lsSetEncrypted('stockroom_kv_session_key', {
      keyData: keyB64, emailHash, expiry: Date.now() + 24 * 60 * 60 * 1000,
    });
    return key;
  } catch(e) {
    console.warn('_fetchPasskeyWrappedKey failed:', e.message);
    return null;
  }
}

// Prompt the user for their passphrase once, unwrap the data key,
// then store a passkey-wrapped copy on the server so future logins
// don't need the passphrase.
async function _getKeyViaPassphrase(emailHash, sessionToken, credentialId, errEl) {
  // Show a clear one-time explanation
  const result = await showPassphrasePrompt(
    'One-time setup: enter your passphrase to unlock your data. ' +
    'After this, Face ID / Fingerprint alone will be enough on this device.'
  );
  if (!result) return null;
  const { passphrase } = result;

  try {
    // Fetch the passphrase-wrapped envelope
    const keyRes  = await fetchKV(`${WORKER_URL}/key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, sessionToken }),
    });
    const keyData = await keyRes.json();
    if (keyRes.status === 401) {
      if (errEl) { errEl.textContent = 'Incorrect passphrase — try again'; errEl.style.display = 'block'; }
      return null;
    }

    let dataKey = null;
    if (keyRes.ok && !keyData.legacy && keyData.envelope) {
      try {
        if (keyData.cryptoVersion === 'v2' && keyData.kdfSalt) {
          const wrapKey = await derivePassphraseWrapKeyV2(passphrase, emailHash, keyData.kdfSalt);
          dataKey = await unwrapDataKeyV2(keyData.envelope, wrapKey, true);
        } else {
          const { wrapKey } = await derivePassphraseWrapKey(passphrase, emailHash, keyData.salt);
          dataKey = await unwrapDataKey(keyData.envelope, wrapKey);
        }
      } catch(e) {
        if (errEl) { errEl.textContent = 'Incorrect passphrase — try again'; errEl.style.display = 'block'; }
        return null;
      }
    } else {
      // Legacy: derive from passphrase directly
      dataKey = await kvDeriveKey(_kvEmail || '', passphrase);
    }

    if (!dataKey) return null;

    // Store a passkey-wrapped copy so future logins don't need passphrase
    try {
      const exported  = await crypto.subtle.exportKey('raw', dataKey);
      const rawKeyB64 = btoa(String.fromCharCode(...new Uint8Array(exported)));
      // Store via new PRF endpoint (device-bound since we don't have PRF at this point)
      const deviceKeyRaw  = crypto.getRandomValues(new Uint8Array(32));
      const deviceWrapKey = await crypto.subtle.importKey('raw', deviceKeyRaw, 'AES-KW', false, ['wrapKey']);
      const deviceKeyB64  = btoa(String.fromCharCode(...deviceKeyRaw));
      const deviceEnv     = await wrapKeyWithPrf(dataKey, deviceWrapKey);
      // Store device key in BOTH IDB and localStorage so clearing one doesn't break passkey login
      await dbPut('settings', `passkey_device_key_${credentialId}`, deviceKeyB64);
      try { localStorage.setItem(`stockroom_passkey_dk_${credentialId}`, deviceKeyB64); } catch(e) {}
      await fetchKV(`${WORKER_URL}/key/passkey-prf-store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash, sessionToken, credentialId, prfEnvelope: deviceEnv, deviceBound: true }),
      });
      // Cache session key
      await lsSetEncrypted('stockroom_kv_session_key', {
        keyData: rawKeyB64, emailHash, expiry: Date.now() + 24 * 60 * 60 * 1000,
      });
    } catch(e) {
      console.warn('Could not store passkey-wrapped key:', e.message);
      // Non-fatal — key still works, just needs passphrase next time
    }

    return dataKey;
  } catch(e) {
    if (errEl) { errEl.textContent = e.message; errEl.style.display = 'block'; }
    return null;
  }
}

async function kvStorePasskeySession(email, emailHash, sessionToken, dataKey) {
  _kvEmail        = email;
  _kvEmailHash    = emailHash;
  _kvSessionToken = sessionToken;
  _kvVerifier     = ''; // passkey users authenticate via sessionToken
  _kvAuthMethod   = 'passkey';
  kvConnected     = true;
  _kvKey = dataKey || null;

  // Store session
  try {
    const sessionJson = JSON.stringify({ email, emailHash, sessionToken, authMethod: 'passkey' });
    localStorage.setItem('stockroom_kv_session', sessionJson);
    // Also persist to IDB so session survives localStorage/cookie clears
    dbPut('settings', 'stockroom_kv_session', sessionJson).catch(() => {});
    // Cache key for 24h
    const exported = await crypto.subtle.exportKey('raw', _kvKey);
    const keyData  = btoa(String.fromCharCode(...new Uint8Array(exported)));
    await lsSetEncrypted('stockroom_kv_session_key', {
      keyData, emailHash,
      expiry: Date.now() + 24 * 60 * 60 * 1000,
    });
  } catch(e) {}
  const el = document.getElementById('kv-account-email');
  if (el) el.textContent = email;
  updateSyncUI();
  // Ensure ECDH keypair exists for secure sharing (non-blocking)
  ensureEcdhKeypair(emailHash).catch(e => console.warn('ensureEcdhKeypair:', e.message));
}

// ── Restore passkey session on page load ──────────────────
async function kvRestorePasskeySession(session) {
  const { email, emailHash, sessionToken } = session;
  // Verify session is still valid on backend
  try {
    const res = await fetchKV(`${WORKER_URL}/passkey/verify-session`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, sessionToken }),
    });
    if (!res.ok) return false;
  } catch(e) {
    // Network error — try to restore from cached key
  }
  _kvEmail        = email;
  _kvEmailHash    = emailHash;
  _kvSessionToken = sessionToken;
  _kvVerifier     = '';
  _kvAuthMethod   = 'passkey';
  kvConnected     = true;
  // Ensure device flag is set so login screen shows passkey option next time
  setDeviceHasPasskey(true);
  // Stamp permanent setup flags — these survive sign-out and should always be set for returning users
  await setProtectSeenForDevice();
  await setCountrySetForDevice();
  localStorage.setItem('stockroom_seen', '1');
  const el = document.getElementById('kv-account-email');
  if (el) el.textContent = email;
  updateSyncUI();
  return true;
}

// ── Add passkey to existing account (from Settings) ───────
async function addPasskeyToAccount() {
  if (!passkeySupported()) { toast('Passkeys not supported on this device'); return; }
  if (!kvConnected) { toast('Sign in first'); return; }
  requireReauth('Re-enter your passphrase to add a passkey.', async () => {
    try {
      await _doAddPasskeyToAccount();
    } catch(err) {
      if (err.name === 'NotAllowedError') { toast('Setup cancelled'); }
      else { toast('Could not add passkey: ' + err.message); }
    }
  }, { passkeyAllowed: false });
}

async function _doAddPasskeyToAccount() {

  // Prompt for a friendly device name so users can identify passkeys in settings
  const suggestedName = getDeviceName();
  const deviceName = window.prompt(
    'Give this passkey a name so you can identify it later.\n(e.g. "Pete\'s iPhone", "Work laptop")',
    suggestedName
  );
  if (deviceName === null) throw new Error('Setup cancelled'); // user pressed Cancel
  const finalDeviceName = (deviceName || suggestedName).trim().slice(0, 50) || suggestedName;

  try {
    const beginRes = await fetchKV(`${WORKER_URL}/passkey/register/begin`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, email: _kvEmail }),
    });
    const beginData = await beginRes.json();
    if (!beginRes.ok) throw new Error(beginData.error || 'Could not start setup');

    const credential = await navigator.credentials.create({
      publicKey: {
        challenge:              b64urlToUint8(beginData.challenge),
        rp:                     beginData.rp,
        user: {
          id:          new TextEncoder().encode(beginData.user.id),
          name:        beginData.user.name,
          displayName: beginData.user.displayName,
        },
        pubKeyCredParams:       beginData.pubKeyCredParams,
        timeout:                beginData.timeout,
        attestation:            beginData.attestation,
        authenticatorSelection: beginData.authenticatorSelection,
        extensions: {
          prf: { eval: { first: PASSKEY_PRF_SALT } },
        },
      },
    });
    if (!credential) throw new Error('Setup cancelled');

    const credId           = uint8ToB64url(new Uint8Array(credential.rawId));
    const clientDataJSON   = uint8ToB64url(new Uint8Array(credential.response.clientDataJSON));
    const attestationObject = uint8ToB64url(new Uint8Array(credential.response.attestationObject));
    let publicKey = credId;
    try {
      const pkResult = credential.response.getPublicKey?.();
      const pk = pkResult instanceof Promise ? await pkResult : pkResult;
      if (pk) publicKey = uint8ToB64url(new Uint8Array(pk));
    } catch(e) { console.warn('getPublicKey failed:', e.message); }

    // Check if PRF extension gave us output at registration time
    const extResults = credential.getClientExtensionResults?.() || {};
    const prfOutput  = extResults?.prf?.results?.first || null;

    const verifierToSend = _kvSessionToken ? undefined : _kvVerifier;
    const finishRes = await fetchKV(`${WORKER_URL}/passkey/register/finish`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash: _kvEmailHash, email: _kvEmail,
        credentialId: credId, publicKey, clientDataJSON, attestationObject,
        deviceName: finalDeviceName,
        ...(verifierToSend ? { verifier: verifierToSend } : {}),
      }),
    });
    const finishData = await finishRes.json();
    if (!finishRes.ok) throw new Error(finishData.error || 'Setup failed');

    // Update session token
    _kvSessionToken = finishData.sessionToken;
    _kvAuthMethod   = 'passkey';
    // Mark this device as having a passkey (used on protect screen and login screen)
    setDeviceHasPasskey(true);
    try {
      const s = JSON.parse(localStorage.getItem('stockroom_kv_session') || '{}');
      s.sessionToken = finishData.sessionToken;
      s.authMethod   = 'passkey';
      localStorage.setItem('stockroom_kv_session', JSON.stringify(s));
      const storedCreds = JSON.parse(localStorage.getItem('stockroom_passkey_creds') || '{}');
      storedCreds[_kvEmailHash] = credId;
      localStorage.setItem('stockroom_passkey_creds', JSON.stringify(storedCreds));
    } catch(e) {}

    // ── KEY ARCHITECTURE: PRF-first, device-bound fallback ────────────
    // Path A: PRF output from the secure enclave → AES-KW wrap key → wraps data key.
    //         This is fully E2EE: the server never sees the raw data key.
    // Path B: No PRF support → generate random device key → store in IDB →
    //         wrap data key with it → send ONLY the wrapped copy to server.
    if (!_kvKey) throw new Error('Encryption key not in memory. Sign out and back in, then try again.');

    if (prfOutput) {
      // ── Path A: PRF ──────────────────────────────────────────────────
      const prfWrapKey = await prfBytesToWrapKey(prfOutput);
      const prfEnvelope = await wrapKeyWithPrf(_kvKey, prfWrapKey);
      // Store PRF envelope on server (server stores ciphertext only — never the raw key)
      const storeRes = await fetchKV(`${WORKER_URL}/key/passkey-prf-store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          emailHash:    _kvEmailHash,
          sessionToken: finishData.sessionToken,
          credentialId: credId,
          prfEnvelope,  // data key wrapped with PRF-derived AES-KW — server cannot unwrap
        }),
      });
      if (!storeRes.ok) {
        const e = await storeRes.json().catch(() => ({}));
        throw new Error('Could not store PRF envelope: ' + (e.error || storeRes.status));
      }
    } else {
      // ── Path B: Device-bound IDB key ─────────────────────────────────
      const deviceKeyRaw  = crypto.getRandomValues(new Uint8Array(32));
      const deviceWrapKey = await crypto.subtle.importKey('raw', deviceKeyRaw, 'AES-KW', false, ['wrapKey', 'unwrapKey']);
      const deviceEnvelope = await wrapKeyWithPrf(_kvKey, deviceWrapKey); // same wrap fn, different key
      // Store device key in IDB (never leaves device)
      const deviceKeyB64 = btoa(String.fromCharCode(...deviceKeyRaw));
      await dbPut('settings', `passkey_device_key_${credId}`, deviceKeyB64);
      // Store envelope on server (useless without the device key — fallback is device-bound)
      const storeRes = await fetchKV(`${WORKER_URL}/key/passkey-prf-store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          emailHash:    _kvEmailHash,
          sessionToken: finishData.sessionToken,
          credentialId: credId,
          prfEnvelope:  deviceEnvelope,
          deviceBound:  true, // flag: envelope is device-bound, not PRF-based
        }),
      });
      if (!storeRes.ok) {
        const e = await storeRes.json().catch(() => ({}));
        throw new Error('Could not store device envelope: ' + (e.error || storeRes.status));
      }
    }


    toast('Passkey added ✓ — you can now sign in with Face ID / Fingerprint');
    loadPasskeys();
  } catch(err) {
    // Toast for settings context, but also rethrow so protect screen can show persistent error
    if (err.name !== 'NotAllowedError') {
      toast('Could not add passkey: ' + err.message);
    }
    throw err; // rethrow so protectAddPasskey can catch and display persistently
  }
}

// ── Load and display passkeys in Settings ─────────────────
async function generateNewRecoveryCodes() {
  if (!kvConnected || !_kvKey) { toast('Sign in first'); return; }
  requireReauth('Re-enter your passphrase to generate new recovery codes.', _doGenerateNewRecoveryCodes, { passkeyAllowed: true });
}

async function _doGenerateNewRecoveryCodes() {
  if (!confirm('Generate 10 new recovery codes?\n\nThis will invalidate all your existing recovery codes.')) return;
  try {
    const newCodes     = generateRecoveryCodes(10);
    const newEnvelopes = await buildRecoveryEnvelopes(newCodes, _kvKey, _kvEmailHash);
    const body = _kvSessionToken
      ? { emailHash: _kvEmailHash, sessionToken: _kvSessionToken, recoveryEnvelopes: newEnvelopes }
      : { emailHash: _kvEmailHash, verifier: _kvVerifier, recoveryEnvelopes: newEnvelopes };
    const res = await fetchKV(`${WORKER_URL}/key/update-recovery`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error('Could not update recovery codes');
    // Show new codes
    showProtectDataScreen(newCodes);
    toast('New recovery codes generated — save them now');
  } catch(err) { toast('Could not generate codes: ' + err.message); }
}

// ══════════════════════════════════════════
//  EXPORT REMINDER SYSTEM
// ══════════════════════════════════════════

function checkExportReminder() {
  if (!kvConnected) return;
  try {
    const lastExport = localStorage.getItem('stockroom_last_export');
    const now        = Date.now();
    const thirtyDays = 30 * 24 * 60 * 60 * 1000;
    const sixtyDays  = 60 * 24 * 60 * 60 * 1000;
    if (!lastExport) {
      // Never exported — remind after 30 days of account age
      const session = JSON.parse(localStorage.getItem('stockroom_kv_session') || '{}');
      return; // Don't nag on first load
    }
    const daysSince = now - parseInt(lastExport);
    if (daysSince > sixtyDays) {
      showExportReminder('urgent');
    } else if (daysSince > thirtyDays) {
      showExportReminder('normal');
    }
  } catch(e) {}
}

function showExportReminder(level) {
  const existing = document.getElementById('export-reminder-banner');
  if (existing) return; // already showing
  const banner = document.createElement('div');
  banner.id    = 'export-reminder-banner';
  const isUrgent = level === 'urgent';
  banner.style.cssText = `background:${isUrgent ? 'rgba(232,80,80,0.1)' : 'rgba(232,168,56,0.1)'};border-bottom:1px solid ${isUrgent ? 'rgba(232,80,80,0.3)' : 'rgba(232,168,56,0.3)'};padding:10px 16px;display:flex;align-items:center;gap:10px;font-size:13px`;
  banner.innerHTML = `
    <span style="flex:1">${isUrgent ? '⚠️' : '📦'} <strong>${isUrgent ? 'Over 60 days' : 'Over 30 days'} since your last data export.</strong> Export your data regularly as a backup.</span>
    <button class="btn btn-sm" style="background:${isUrgent?'rgba(232,80,80,0.2)':'rgba(232,168,56,0.2)'};border:1px solid ${isUrgent?'rgba(232,80,80,0.4)':'rgba(232,168,56,0.4)'};color:${isUrgent?'var(--danger)':'var(--warn)'};white-space:nowrap" onclick="exportDataAndDismiss()">Export now</button>
    <button class="btn btn-ghost btn-sm" onclick="this.closest('#export-reminder-banner').remove()"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
  `;
  // Insert after update banner
  const updateBanner = document.getElementById('update-banner');
  updateBanner?.parentNode?.insertBefore(banner, updateBanner.nextSibling) || document.querySelector('.app-header')?.after(banner);
}

async function exportDataAndDismiss() {
  document.getElementById('export-reminder-banner')?.remove();
  localStorage.setItem('stockroom_last_export', String(Date.now()));
  // Trigger the existing export function
  if (typeof exportData === 'function') exportData();
}

// Hook into existing export to track last export time
const _origExportData = typeof exportData !== 'undefined' ? exportData : null;

async function loadPasskeys() {
  // Primary target is Account & Security; passkey-list (Settings) was removed
  const container = document.getElementById('passkey-list-sec') || document.getElementById('passkey-list');
  if (!container) return;
  if (!kvConnected) {
    container.innerHTML = '<p style="font-size:12px;color:var(--muted)">Sign in to view and manage passkeys.</p>';
    return;
  }
  container.innerHTML = '<p style="font-size:12px;color:var(--muted)">Loading…</p>';
  try {
    const body = _kvSessionToken
      ? { emailHash: _kvEmailHash, sessionToken: _kvSessionToken }
      : { emailHash: _kvEmailHash, verifier: _kvVerifier };
    const res  = await fetchKV(`${WORKER_URL}/passkey/list`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      if (res.status === 400 || res.status === 404) {
        container.innerHTML = '<p style="font-size:12px;color:var(--muted)">No passkeys registered on this account yet.</p>';
        return;
      }
      throw new Error('Server returned ' + res.status);
    }
    const { credentials } = await res.json();
    if (!credentials || !credentials.length) {
      container.innerHTML = '<p style="font-size:12px;color:var(--muted)">No passkeys registered yet. Add one below.</p>';
    } else {
      container.innerHTML = credentials.map(c => `
        <div style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;margin-bottom:6px">
          <div><svg class="icon icon-lg" aria-hidden="true"><use href="#i-key-round"></use></svg></div>
          <div style="flex:1;min-width:0">
            <div style="font-size:13px;font-weight:700">${esc(c.deviceName)}</div>
            <div style="font-size:11px;color:var(--muted)">Added ${new Date(c.createdAt).toLocaleDateString('en-GB')} · Last used ${new Date(c.lastUsed).toLocaleDateString('en-GB')}</div>
          </div>
          <button class="btn btn-ghost btn-sm" style="color:var(--danger);flex-shrink:0" onclick="removePasskey('${esc(c.credentialId)}')">Remove</button>
        </div>`).join('');
    }
  } catch(e) {
    container.innerHTML = `<p style="font-size:12px;color:var(--muted)">Could not load passkeys — ${e.message}. <button onclick="loadPasskeys()" style="background:none;border:none;color:var(--accent);font-size:12px;cursor:pointer;padding:0;text-decoration:underline">Try again</button></p>`;
  }
}

async function removePasskey(credentialId) {
  if (!confirm('Remove this passkey? You\'ll need to use your passphrase or another passkey to sign in.')) return;
  try {
    const body = _kvSessionToken
      ? { emailHash: _kvEmailHash, sessionToken: _kvSessionToken, credentialId }
      : { emailHash: _kvEmailHash, verifier: _kvVerifier, credentialId };
    const res = await fetch(`${WORKER_URL}/passkey/remove`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error('Could not remove');
    toast('Passkey removed ✓');
    loadPasskeys();
  } catch(e) { toast('Could not remove passkey: ' + e.message); }
}

async function initPasskeyUI() {
  // Show passkey buttons by default, hide only if definitely not supported
  const supported = await passkeyPlatformSupported().catch(() => false);
  const regOption   = document.getElementById('passkey-register-option');
  const loginOption = document.getElementById('passkey-login-option');
  const addBtn = document.getElementById('add-passkey-btn') || document.getElementById('add-passkey-btn-sec');
  if (!supported) {
    if (regOption)   regOption.style.display   = 'none';
    if (loginOption) loginOption.style.display  = 'none';
    if (addBtn)      addBtn.style.display       = 'none';
  } else {
    if (regOption)   regOption.style.display   = '';
    if (loginOption) loginOption.style.display  = '';
    if (addBtn)      addBtn.style.display       = '';
  }
}
// ── Key Envelope System ───────────────────────────────────
// DATA KEY = random 256-bit AES key (encrypts all user data)
// Wrapped by passphrase key, recovery code keys, passkey sessions.

async function generateDataKey() {
  return crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
}

async function derivePassphraseWrapKey(passphrase, emailHash, saltB64) {
  const salt    = saltB64
    ? Uint8Array.from(atob(saltB64), c => c.charCodeAt(0))
    : crypto.getRandomValues(new Uint8Array(32));
  const raw     = new TextEncoder().encode(passphrase + ':' + emailHash);
  const base    = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  const wrapKey = await crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 200000, hash: 'SHA-256' },
    base, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']
  );
  return { wrapKey, saltB64: saltB64 || btoa(String.fromCharCode(...salt)) };
}

async function deriveRecoveryWrapKey(code, emailHash) {
  const raw  = new TextEncoder().encode(code.replace(/-/g,'').toUpperCase() + ':' + emailHash);
  const base = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  const salt = new TextEncoder().encode('stockroom-recovery-v1-' + emailHash);
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' },
    base, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']
  );
}

async function wrapDataKey(dataKey, wrapKey) {
  const raw       = await crypto.subtle.exportKey('raw', dataKey);
  const iv        = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, wrapKey, raw);
  const combined  = new Uint8Array(iv.length + encrypted.byteLength);
  combined.set(iv, 0); combined.set(new Uint8Array(encrypted), iv.length);
  return btoa(String.fromCharCode(...combined));
}

async function unwrapDataKey(envelopeB64, wrapKey) {
  const combined  = Uint8Array.from(atob(envelopeB64), c => c.charCodeAt(0));
  const iv        = combined.slice(0, 12);
  const encrypted = combined.slice(12);
  const raw       = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, wrapKey, encrypted);
  return crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
}

function generateRecoveryCodes(count = 10) {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({ length: count }, () => {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    return [0,4,8,12].map(s => Array.from(bytes.slice(s,s+4)).map(b => chars[b % chars.length]).join('')).join('-');
  });
}

async function hashRecoveryCode(code, emailHash) {
  const encoded = new TextEncoder().encode(code.replace(/-/g,'').toUpperCase() + ':' + emailHash);
  const hash    = await crypto.subtle.digest('SHA-256', encoded);
  return btoa(String.fromCharCode(...new Uint8Array(hash)));
}

async function buildRecoveryEnvelopes(codes, dataKey, emailHash) {
  const envelopes = [];
  for (const code of codes) {
    const wrapKey  = await deriveRecoveryWrapKey(code, emailHash);
    const envelope = await wrapDataKey(dataKey, wrapKey);
    const codeHash = await hashRecoveryCode(code, emailHash);
    envelopes.push(JSON.stringify({ envelope, codeHash }));
  }
  return envelopes;
}

// Uses IndexedDB to store a wrapped copy of the encryption key.
// The wrapping key is device-specific (derived from a random device secret
// stored in localStorage). The server stores device metadata (name, last seen)
// keyed by deviceId so the owner can manage/revoke trusted devices.

const DEVICE_DB_NAME    = 'stockroom-kv-device';
const DEVICE_STORE_NAME = 'keys';

// ── localStorage encryption helpers ──────────────────────────────────────────
// Sensitive blobs (session key, share keys) are encrypted with a device-bound
// secret before being written to localStorage. Falls back to a session secret
// stored in sessionStorage if no device secret exists yet (passkey-only users
// who never ticked "Stay signed in").
//
// Format stored: base64( iv[12] || AES-GCM-ciphertext )
// The wrap key is derived via PBKDF2 from the secret so it's 256-bit AES-GCM.

async function _lsWrapKey(secret) {
  const enc  = new TextEncoder();
  const base = await crypto.subtle.importKey('raw', enc.encode(secret), 'PBKDF2', false, ['deriveKey']);
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt: enc.encode('stockroom-ls-v1'), iterations: 100000, hash: 'SHA-256' },
    base, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']
  );
}

function _getLsSecret() {
  // Prefer the long-lived device secret; fall back to a session-scoped secret.
  let s = localStorage.getItem('stockroom_device_secret');
  if (s) return s;
  let ss = sessionStorage.getItem('stockroom_ls_session_secret');
  if (!ss) {
    ss = Array.from(crypto.getRandomValues(new Uint8Array(32))).map(b => b.toString(16).padStart(2,'0')).join('');
    try { sessionStorage.setItem('stockroom_ls_session_secret', ss); } catch(e) {}
  }
  return ss;
}

async function lsEncrypt(value) {
  try {
    const secret  = _getLsSecret();
    const wk      = await _lsWrapKey(secret);
    const iv      = crypto.getRandomValues(new Uint8Array(12));
    const enc     = new TextEncoder();
    const ct      = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, wk, enc.encode(JSON.stringify(value)));
    const combined = new Uint8Array(12 + ct.byteLength);
    combined.set(iv);
    combined.set(new Uint8Array(ct), 12);
    return btoa(String.fromCharCode(...combined));
  } catch(e) {
    // Fallback: store plaintext if crypto unavailable (should never happen)
    return JSON.stringify(value);
  }
}

async function lsDecrypt(blob) {
  try {
    const secret  = _getLsSecret();
    const wk      = await _lsWrapKey(secret);
    const combined = Uint8Array.from(atob(blob), c => c.charCodeAt(0));
    const iv      = combined.slice(0, 12);
    const ct      = combined.slice(12);
    const plain   = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, wk, ct);
    return JSON.parse(new TextDecoder().decode(plain));
  } catch(e) {
    // Attempt legacy plaintext parse (for upgrading existing stored values)
    try { return JSON.parse(blob); } catch(e2) { return null; }
  }
}

// Encrypted get/set wrappers for the two sensitive keys
async function lsSetEncrypted(key, value) {
  try {
    const blob = await lsEncrypt(value);
    localStorage.setItem(key, blob);
  } catch(e) {}
}

async function lsGetEncrypted(key) {
  try {
    const blob = localStorage.getItem(key);
    if (!blob) return null;
    return await lsDecrypt(blob);
  } catch(e) { return null; }
}

// Share key helpers — always encrypted at rest
async function _getShareKeys() {
  return (await lsGetEncrypted('stockroom_share_keys')) || {};
}
async function _setShareKeys(obj) {
  await lsSetEncrypted('stockroom_share_keys', obj);
}

async function openDeviceDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DEVICE_DB_NAME, 1);
    req.onupgradeneeded = e => e.target.result.createObjectStore(DEVICE_STORE_NAME);
    req.onsuccess = e => resolve(e.target.result);
    req.onerror   = e => reject(e.target.error);
  });
}

function getOrCreateDeviceId() {
  let id = localStorage.getItem('stockroom_device_id');
  if (!id) {
    // Try to recover from IDB (handles localStorage clear)
    id = sessionStorage.getItem('stockroom_device_id_session');
  }
  if (!id) {
    id = Array.from(crypto.getRandomValues(new Uint8Array(16)))
      .map(b => b.toString(16).padStart(2,'0')).join('');
  }
  try { localStorage.setItem('stockroom_device_id', id); } catch(e) {}
  try { sessionStorage.setItem('stockroom_device_id_session', id); } catch(e) {}
  return id;
}

// On startup, also persist device ID into IDB (fire-and-forget)
// so it can be recovered if localStorage is cleared
;(function _persistDeviceId() {
  try {
    const id = getOrCreateDeviceId();
    dbPut('settings', 'stockroom_device_id', id).catch(() => {});
    // Also try to restore from IDB if LS is empty
    const lsId = localStorage.getItem('stockroom_device_id');
    if (!lsId) {
      dbGet('settings', 'stockroom_device_id').then(val => {
        if (val) {
          localStorage.setItem('stockroom_device_id', val);
          sessionStorage.setItem('stockroom_device_id_session', val);
        }
      }).catch(() => {});
    }
  } catch(e) {}
})();

// ── Per-user, per-device setup flags ────────────────────────────────────────
// Keyed by emailHash so multiple users share a device without interfering.
// Flags also stored in settings blob so they follow the user to new devices.
// ── One-time setup flags ──────────────────────────────────────────────────────
// These flags are stored in the encrypted settings blob on the server, so they
// follow the user to every device. Once set, the screens never show again —
// regardless of which device, browser, or whether localStorage/cookies are cleared.
// The "ForDevice" suffix is kept on the function names for backwards compatibility
// but the flags are truly account-wide (not device-scoped).

function _setupFlagKey(flagName) {
  // Kept for any legacy IDB reads, but new code only uses settings._setup*
  const hash = _kvEmailHash || '';
  return hash ? `device_setup_${getOrCreateDeviceId()}_${hash}_${flagName}`
              : `device_setup_${getOrCreateDeviceId()}_${flagName}`;
}

async function getProtectSeenForDevice() {
  // Primary: check server-synced settings blob (follows user to any device)
  return !!settings._setupProtectSeen;
}
async function setProtectSeenForDevice() {
  settings._setupProtectSeen = true;
  await dbPut('settings', 'settings', settings);
  // Push to server immediately — flag must be durable before user proceeds
  if (kvConnected) kvSyncNow(true).catch(() => {});
}
async function getCountrySetForDevice() {
  return !!settings._setupCountrySet;
}
async function setCountrySetForDevice() {
  settings._setupCountrySet = true;
  await dbPut('settings', 'settings', settings);
  if (kvConnected) kvSyncNow(true).catch(() => {});
}

function getDeviceName() {
  const ua = navigator.userAgent;
  if (/iPhone/i.test(ua))  return 'iPhone';
  if (/iPad/i.test(ua))    return 'iPad';
  if (/Android/i.test(ua)) return /Mobile/i.test(ua) ? 'Android Phone' : 'Android Tablet';
  if (/Mac/i.test(ua))     return 'Mac';
  if (/Windows/i.test(ua)) return 'Windows PC';
  return 'Browser';
}

async function deriveDeviceWrapKey(deviceSecret) {
  const raw  = new TextEncoder().encode(deviceSecret);
  const base = await crypto.subtle.importKey('raw', raw, 'PBKDF2', false, ['deriveKey']);
  const salt = new TextEncoder().encode('stockroom-device-wrap-v1');
  // Use encrypt/decrypt (not wrapKey/unwrapKey) — simpler and consistent
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt, iterations: 50000, hash: 'SHA-256' },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
}

async function saveWrappedKey(deviceId, encryptionKey, deviceSecret) {
  // Export raw key bytes, encrypt them with the device wrap key, store in IDB
  const exported  = await crypto.subtle.exportKey('raw', encryptionKey); // needs extractable:true
  const wrapKey   = await deriveDeviceWrapKey(deviceSecret);
  const iv        = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, wrapKey, exported);
  const combined  = new Uint8Array(iv.length + encrypted.byteLength);
  combined.set(iv, 0);
  combined.set(new Uint8Array(encrypted), iv.length);
  const db = await openDeviceDb();
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(DEVICE_STORE_NAME, 'readwrite');
    const req = tx.objectStore(DEVICE_STORE_NAME).put(btoa(String.fromCharCode(...combined)), deviceId);
    req.onsuccess = () => resolve(true);
    req.onerror   = () => reject(req.error);
  });
}

async function loadWrappedKey(deviceId, deviceSecret) {
  try {
    const db     = await openDeviceDb();
    const stored = await new Promise((resolve, reject) => {
      const tx  = db.transaction(DEVICE_STORE_NAME, 'readonly');
      const req = tx.objectStore(DEVICE_STORE_NAME).get(deviceId);
      req.onsuccess = () => resolve(req.result);
      req.onerror   = () => reject(req.error);
    });
    if (!stored) return null;
    const combined  = Uint8Array.from(atob(stored), c => c.charCodeAt(0));
    const iv        = combined.slice(0, 12);
    const encrypted = combined.slice(12);
    const wrapKey   = await deriveDeviceWrapKey(deviceSecret);
    const keyData   = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, wrapKey, encrypted);
    // Restore as extractable:true so it can be re-wrapped if needed
    return crypto.subtle.importKey('raw', keyData, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
  } catch(e) {
    console.warn('loadWrappedKey failed:', e.message);
    return null;
  }
}

async function removeWrappedKey(deviceId) {
  try {
    const db = await openDeviceDb();
    await new Promise((resolve, reject) => {
      const tx  = db.transaction(DEVICE_STORE_NAME, 'readwrite');
      const req = tx.objectStore(DEVICE_STORE_NAME).delete(deviceId);
      req.onsuccess = () => resolve();
      req.onerror   = () => reject(req.error);
    });
  } catch(e) {}
}

// Silent trust — called after successful passphrase login.
// If the user ticked "Remember me", trust the device without any popup.
// This replaces the old offerTrustDevice() confirm dialog entirely.
async function _trustIfRemembered(email, emailHash, verifier, key) {
  const cb1 = document.getElementById('remember-me-checkbox');
  const cb2 = document.getElementById('remember-me-checkbox-auth');
  const remembered = _rememberMeChecked
    || cb1?.checked
    || cb2?.checked
    || getCookieConsent() === 'granted';
  if (!remembered) return;
  const secret = localStorage.getItem('stockroom_device_secret');
  if (secret) {
    const existing = await loadWrappedKey(getOrCreateDeviceId(), secret);
    if (existing) return;
  }
  await trustThisDeviceWith(email, emailHash, verifier, key);
}

async function offerTrustDevice(email, emailHash, verifier, key) {
  // Popup removed — now handled silently via "Remember me" checkbox.
  // This stub is kept so any call sites that haven't been updated still work.
  await _trustIfRemembered(email, emailHash, verifier, key);
}

// Handles 429 rate-limit responses from /user/verify
// Returns an error message string, or null if not a rate limit
function _handleLoginRateLimit(res, data) {
  if (res.status !== 429) return null;
  const until = data?.lockedUntil;
  if (until) {
    const remaining = Math.ceil((until - Date.now()) / 60000);
    return `Too many failed attempts — please wait ${remaining} minute${remaining !== 1 ? 's' : ''} before trying again.`;
  }
  const retryAfter = data?.retryAfter;
  if (retryAfter) return `Too many failed attempts — please wait ${Math.ceil(retryAfter / 60)} minutes before trying again.`;
  return 'Too many attempts — please wait before trying again.';
}

async function trustThisDeviceWith(email, emailHash, verifier, key) {
  try {
    const deviceId = getOrCreateDeviceId();
    let   secret   = localStorage.getItem('stockroom_device_secret');
    if (!secret) {
      secret = Array.from(crypto.getRandomValues(new Uint8Array(32))).map(b => b.toString(16).padStart(2,'0')).join('');
      localStorage.setItem('stockroom_device_secret', secret);
    }
    await saveWrappedKey(deviceId, key, secret);
    // Record when trust was established — used for 30-day expiry
    localStorage.setItem('stockroom_trust_ts', String(Date.now()));
    // Also store raw key bytes in localStorage as fallback for IDB failures
    try {
      const exported = await crypto.subtle.exportKey('raw', key);
      const keyB64   = btoa(String.fromCharCode(...new Uint8Array(exported)));
      localStorage.setItem('stockroom_kv_key_fallback', JSON.stringify({
        keyB64, emailHash,
        expiresAt: Date.now() + 30 * 24 * 60 * 60 * 1000,
      }));
    } catch(e) {}
    // Register on backend
    const name = getDeviceName();
    fetchKV(`${WORKER_URL}/device/register`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash, verifier, deviceId, name, addedAt: new Date().toISOString() }),
    }).catch(() => {});
    toast('This device is now trusted ✓');
    loadTrustedDevices();
  } catch(e) {
    toast('Could not trust device: ' + e.message);
  }
}

async function trustThisDevice() {
  if (!_kvKey) { toast('Sign in first'); return; }
  await trustThisDeviceWith(_kvEmail, _kvEmailHash, _kvVerifier, _kvKey);
}

async function loadTrustedDevices() {
  if (!kvConnected || !_kvEmailHash || (!_kvVerifier && !_kvSessionToken)) return;
  const list = document.getElementById('trusted-devices-list');
  const trustRow = document.getElementById('trust-this-device-row');
  if (!list) return;

  try {
    const res  = await fetchKV(`${WORKER_URL}/device/list`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier } }),
    });
    const data = await res.json();
    const devices = data.devices || [];
    const myDeviceId = getOrCreateDeviceId();

    if (!devices.length) {
      list.innerHTML = '<p style="font-size:12px;color:var(--muted)">No trusted devices yet.</p>';
    } else {
      list.innerHTML = devices.map(d => {
        const isThis = d.deviceId === myDeviceId;
        const added  = d.addedAt ? new Date(d.addedAt).toLocaleDateString('en-GB') : '—';
        const last   = d.lastSeen ? new Date(d.lastSeen).toLocaleDateString('en-GB') : '—';
        return `<div style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--surface2);border:1px solid ${isThis?'var(--accent)':'var(--border)'};border-radius:10px">
          <div style="flex:1;min-width:0">
            <div style="font-size:13px;font-weight:700">${esc(d.name)}${isThis?' <span style="font-size:11px;color:var(--accent);font-weight:400">(this device)</span>':''}</div>
            <div style="font-size:11px;color:var(--muted);font-family:var(--mono)">Added ${added} · Last seen ${last}</div>
          </div>
          <button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="removeTrustedDevice('${d.deviceId}')">Remove</button>
        </div>`;
      }).join('');
    }

    // Show trust button only if this device isn't trusted
    const isTrusted = devices.some(d => d.deviceId === myDeviceId);
    if (trustRow) trustRow.style.display = isTrusted ? 'none' : 'block';
  } catch(e) {
    list.innerHTML = '<p style="font-size:12px;color:var(--muted)">Could not load devices.</p>';
  }
}

async function removeTrustedDevice(deviceId) {
  if (!confirm('Remove this device? It will need to sign in with a passphrase or passkey next time.')) return;
  try {
    await fetchKV(`${WORKER_URL}/device/remove`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }, deviceId }),
    });
    // If removing this device, clear local trust data too
    if (deviceId === getOrCreateDeviceId()) {
      await removeWrappedKey(deviceId);
      localStorage.removeItem('stockroom_device_secret');
    }
    toast('Device removed ✓');
    loadTrustedDevices();
  } catch(e) { toast('Could not remove device'); }
}

async function clearAllTrustedDevices() {
  if (!confirm('Remove all trusted devices? All devices will need to sign in again.')) return;
  const list = document.getElementById('trusted-devices-list');
  if (list) list.querySelectorAll('[data-device-id]').forEach(el => {});
  try {
    const res = await fetchKV(`${WORKER_URL}/device/clear-all`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier } }),
    });
    if (!res.ok) throw new Error('Could not clear devices');
    // Clear local trust data for this device too
    const myDeviceId = getOrCreateDeviceId();
    await removeWrappedKey(myDeviceId).catch(() => {});
    localStorage.removeItem('stockroom_device_secret');
    toast('All trusted devices removed ✓');
    loadTrustedDevices();
    // Sync to acc-sec panel
    setTimeout(() => {
      const tdListSec = document.getElementById('trusted-devices-list-sec');
      const tdListMain = document.getElementById('trusted-devices-list');
      if (tdListSec && tdListMain) tdListSec.innerHTML = tdListMain.innerHTML;
    }, 600);
  } catch(e) { toast('Could not clear devices: ' + e.message); }
}

async function kvStoreSession(email, emailHash, verifier, key) {
  _kvEmail     = email;
  _kvEmailHash = emailHash;
  _kvVerifier  = verifier;
  _kvKey       = key;
  kvConnected  = true;

  try {
    const sessionJson = JSON.stringify({ email, emailHash, verifier });
    localStorage.setItem('stockroom_kv_session', sessionJson);
    // Also persist to IDB so session survives localStorage/cookie clears
    dbPut('settings', 'stockroom_kv_session', sessionJson).catch(() => {});
  } catch(e) {}
  // Cache key — 4 hours normally, 30 days if user asked to stay signed in
  if (key) {
    try {
      const exported = await crypto.subtle.exportKey('raw', key);
      const keyData  = btoa(String.fromCharCode(...new Uint8Array(exported)));
      const cb1 = document.getElementById('remember-me-checkbox');
      const cb2 = document.getElementById('remember-me-checkbox-auth');
      const staySignedIn = _rememberMeChecked || cb1?.checked || cb2?.checked || getCookieConsent() === 'granted';
      const expiry = Date.now() + (staySignedIn ? 30 : 4) * 24 * 60 * 60 * 1000;
      await lsSetEncrypted('stockroom_kv_session_key', { keyData, emailHash, expiry });
    } catch(e) {}
  }
  const el = document.getElementById('kv-account-email');
  if (el) el.textContent = email;
  updateSyncUI();
  // Ensure ECDH keypair exists for secure sharing (non-blocking)
  ensureEcdhKeypair(emailHash).catch(e => console.warn('ensureEcdhKeypair:', e.message));
}

async function kvRestoreSession() {
  try {
    const raw = localStorage.getItem('stockroom_kv_session');
    if (!raw) return false;
    const { email, emailHash, verifier, sessionToken, authMethod } = JSON.parse(raw);
    if (!email || !emailHash) return false;

    // Back-fill device passkey flag from stored credential map (for users pre-dating this flag)
    try {
      const storedCreds = JSON.parse(localStorage.getItem('stockroom_passkey_creds') || '{}');
      if (storedCreds[emailHash]) setDeviceHasPasskey(true);
    } catch(e) {}

    // Passkey session restore
    if (authMethod === 'passkey' && sessionToken) {
      // Ensure device flag is set — handles users who had passkeys before this flag existed
      setDeviceHasPasskey(true);
      return await kvRestorePasskeySession({ email, emailHash, sessionToken });
    }

    if (!verifier) return false;

    // Helper to fully restore session once key is found
    const restoreWith = async (key, source) => {
      _kvEmail     = email;
      _kvEmailHash = emailHash;
      _kvVerifier  = verifier;
      _kvKey       = key;
      kvConnected  = true;
      const el = document.getElementById('kv-account-email');
      if (el) el.textContent = email;
      updateSyncUI();
      return true;
    };

    // 1. Trusted device — try IndexedDB first
    const deviceId = getOrCreateDeviceId();
    const secret   = localStorage.getItem('stockroom_device_secret');
    if (secret) {
      try {
        const wrappedKey = await loadWrappedKey(deviceId, secret);
        if (wrappedKey) {
          // Also cache raw key bytes in localStorage as a resilient fallback
          try {
            const exported = await crypto.subtle.exportKey('raw', wrappedKey);
            const keyB64   = btoa(String.fromCharCode(...new Uint8Array(exported)));
            localStorage.setItem('stockroom_kv_key_fallback', JSON.stringify({
              keyB64,
              emailHash,
              expiresAt: Date.now() + 30 * 24 * 60 * 60 * 1000, // 30 days
            }));
          } catch(e) {}
          fetch(`${WORKER_URL}/device/seen`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ emailHash, verifier, deviceId }),
          }).catch(() => {});
          return await restoreWith(wrappedKey, 'trusted-device-idb');
        }
      } catch(e) { console.warn('IDB restore failed:', e.message); }

      // IDB failed — try localStorage key fallback
      try {
        const fb = JSON.parse(localStorage.getItem('stockroom_kv_key_fallback') || 'null');
        if (fb && fb.emailHash === emailHash && Date.now() < fb.expiresAt) {
          const raw2 = Uint8Array.from(atob(fb.keyB64), c => c.charCodeAt(0));
          const key  = await crypto.subtle.importKey('raw', raw2, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
          return await restoreWith(key, 'trusted-device-localstorage-fallback');
        }
      } catch(e) { console.warn('LS key fallback failed:', e.message); }
    }

    // 2. 4-hour session key — stored encrypted in localStorage with expiry
    try {
      const cached = await lsGetEncrypted('stockroom_kv_session_key');
      if (cached && cached.emailHash === emailHash && Date.now() < cached.expiry) {
        const raw2 = Uint8Array.from(atob(cached.keyData), c => c.charCodeAt(0));
        const key  = await crypto.subtle.importKey('raw', raw2, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
        return await restoreWith(key, '4hr-session');
      } else if (cached) {
        localStorage.removeItem('stockroom_kv_session_key');
      }
    } catch(e) {}

    // 3. Credentials only — key will be prompted on first sync
    // Don't make a network call here — just restore what we have from localStorage
    _kvEmail     = email;
    _kvEmailHash = emailHash;
    _kvVerifier  = verifier;
    kvConnected  = true;
    const el2 = document.getElementById('kv-account-email');
    if (el2) el2.textContent = email;
    updateSyncUI();
    return true;
  } catch(e) {
    console.warn('kvRestoreSession error:', e);
    return false;
  }
}

// Check if the trusted-device session has expired (30 days).
// Returns true if expired and reauth was shown; the caller should return false.
async function _checkTrustExpiry() {
  const ts = localStorage.getItem('stockroom_trust_ts');
  if (!ts) return false; // no trust established
  const THIRTY_DAYS = 30 * 24 * 60 * 60 * 1000;
  if (Date.now() - parseInt(ts) < THIRTY_DAYS) return false; // still valid

  // Expired — clear cached key material but keep session credentials
  localStorage.removeItem('stockroom_device_secret');
  localStorage.removeItem('stockroom_kv_key_fallback');
  localStorage.removeItem('stockroom_trust_ts');
  localStorage.removeItem('stockroom_kv_session_key');
  _kvKey = null;

  // Show the "Protecting your data" reauth modal
  return new Promise(resolve => {
    const modal = document.getElementById('reauth-modal');
    const reasonEl = document.getElementById('reauth-reason');
    if (reasonEl) reasonEl.innerHTML =
      `Your session has expired. For your security, STOCKROOM requires you to re-enter your passphrase every 30 days.<br>
       <span style="font-size:11px;opacity:0.7;display:block;margin-top:6px">Your data remains safely encrypted on the server.</span>`;
    // Update heading to "Protecting your data"
    const h2 = modal?.querySelector('h2');
    if (h2) h2.textContent = 'Protecting your data';
    // Hide passkey option for this re-authentication
    const pkOpt = document.getElementById('reauth-passkey-option');
    if (pkOpt) pkOpt.style.display = 'none';
    requireReauth('', async () => {
      // After successful reauth, re-establish trust so the 30-day clock resets
      if (_kvKey && _kvEmail && _kvEmailHash && _kvVerifier) {
        await trustThisDeviceWith(_kvEmail, _kvEmailHash, _kvVerifier, _kvKey);
      }
      resolve(false); // not expired (now re-authenticated)
    }, { passkeyAllowed: false });
    // If modal was not shown (requireReauth bailed), resolve
    if (!document.getElementById('reauth-modal').style.display || document.getElementById('reauth-modal').style.display === 'none') {
      resolve(true);
    }
  });
}

async function kvEnsureKey() {
  if (_kvKey) return true;

  // 30-day trusted-device session expiry check
  const expired = await _checkTrustExpiry();
  if (expired) return false;
  if (_kvKey) return true; // reauth restored the key

  // 1. Trusted device — IndexedDB (with localStorage fallback for desktop/mobile switches)
  const secret = localStorage.getItem('stockroom_device_secret');
  if (secret) {
    const deviceId = getOrCreateDeviceId();
    try {
      const wrappedKey = await loadWrappedKey(deviceId, secret);
      if (wrappedKey) { _kvKey = wrappedKey; return true; }
    } catch(e) {}
    // IDB failed — try localStorage raw key fallback
    try {
      const fb = JSON.parse(localStorage.getItem('stockroom_kv_key_fallback') || 'null');
      if (fb && fb.emailHash === _kvEmailHash && Date.now() < fb.expiresAt) {
        const raw = Uint8Array.from(atob(fb.keyB64), c => c.charCodeAt(0));
        _kvKey = await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
        if (_kvKey) return true;
      }
    } catch(e) {}
    localStorage.removeItem('stockroom_device_secret');
  }

  // 2. 4-hour session — localStorage encrypted (survives page refresh and tab close)
  try {
    const cached = await lsGetEncrypted('stockroom_kv_session_key');
    if (cached && cached.emailHash === _kvEmailHash && Date.now() < cached.expiry) {
      const raw = Uint8Array.from(atob(cached.keyData), c => c.charCodeAt(0));
      _kvKey = await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
      if (_kvKey) { return true; }
    } else if (cached) {
      localStorage.removeItem('stockroom_kv_session_key');
    }
  } catch(e) {}

  // 2b. Passkey session — fetch the server-wrapped data key (no passphrase needed after first setup)
  if (_kvSessionToken && _kvEmailHash) {
    try {
      // Get stored credential ID for this email hash
      const storedCreds = JSON.parse(localStorage.getItem('stockroom_passkey_creds') || '{}');
      const credentialId = storedCreds[_kvEmailHash];
      if (credentialId) {
        // Try PRF envelope from server
        const envRes = await fetchKV(`${WORKER_URL}/key/passkey-prf-get`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ emailHash: _kvEmailHash, sessionToken: _kvSessionToken, credentialId }),
        }).catch(() => null);
        if (envRes && envRes.ok) {
          const { prfEnvelope, deviceBound } = await envRes.json();
          if (deviceBound) {
            let dkb64 = await dbGet('settings', `passkey_device_key_${credentialId}`);
            if (!dkb64) dkb64 = localStorage.getItem(`stockroom_passkey_dk_${credentialId}`);
            if (dkb64) {
              if (!await dbGet('settings', `passkey_device_key_${credentialId}`))
                await dbPut('settings', `passkey_device_key_${credentialId}`, dkb64);
              const dkr = Uint8Array.from(atob(dkb64), c => c.charCodeAt(0));
              const dwk = await crypto.subtle.importKey('raw', dkr, 'AES-KW', false, ['unwrapKey']);
              _kvKey = await unwrapKeyWithPrf(prfEnvelope, dwk);
              if (_kvKey) return true;
            }
          }
          // PRF path requires a fresh credential.get() — can't do here without user gesture
          // Fall through to passphrase prompt
        }
      }
    } catch(e) { console.warn('kvEnsureKey: passkey-unwrap failed:', e.message); }
  }

  // 3. Passphrase prompt with trust option
  const result = await showPassphrasePrompt();
  if (!result) return false;
  const { passphrase, trust } = result;

  try {
    const verifier = await kvMakeVerifier(passphrase, _kvEmailHash);

    let dataKey;
    const keyRes  = await fetchKV(`${WORKER_URL}/key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, verifier }),
    });
    const keyData = await keyRes.json();

    if (keyRes.status === 401) { toast('Incorrect passphrase'); return false; }

    if (keyRes.ok && !keyData.legacy && keyData.envelope) {
      // Envelope exists — must unwrap it. Do NOT fall through to legacy derive
      // if this fails, as that would set a wrong key and cause KvDecryptError.
      try {
        if (keyData.cryptoVersion === 'v2' && keyData.kdfSalt) {
          const wrapKey = await derivePassphraseWrapKeyV2(passphrase, _kvEmailHash, keyData.kdfSalt);
          dataKey = await unwrapDataKeyV2(keyData.envelope, wrapKey, true);
        } else {
          const { wrapKey } = await derivePassphraseWrapKey(passphrase, _kvEmailHash, keyData.salt);
          dataKey = await unwrapDataKey(keyData.envelope, wrapKey);
        }
      } catch(e) {
        toast('Incorrect passphrase');
        console.warn('kvEnsureKey: envelope unwrap failed —', e.message);
        return false;
      }
    } else {
      // No envelope (pre-envelope legacy account) — derive key directly
      dataKey = await kvDeriveKey(_kvEmail, passphrase);
    }

    _kvKey = dataKey;

    // Cache as 4-hour session in localStorage
    try {
      const exported = await crypto.subtle.exportKey('raw', _kvKey);
      const keyData2 = btoa(String.fromCharCode(...new Uint8Array(exported)));
      await lsSetEncrypted('stockroom_kv_session_key', {
        keyData: keyData2, emailHash: _kvEmailHash,
        expiry: Date.now() + 4 * 60 * 60 * 1000,
      });
    } catch(e) {}

    if (trust) await trustThisDeviceWith(_kvEmail, _kvEmailHash, _kvVerifier, _kvKey);
    return true;
  } catch(e) {
    console.error('kvEnsureKey unexpected error:', e.message);
    toast('Could not unlock — ' + e.message);
    return false;
  }
}

// Show a passphrase prompt modal (replaces browser prompt())
function showPassphrasePrompt(subtitleOverride = null) {
  return new Promise(resolve => {
    // Remove any existing prompt
    document.getElementById('kv-passphrase-modal')?.remove();

    // If we're in a passkey session, explain why passphrase is needed
    const isPasskeySession = _kvAuthMethod === 'passkey';
    const subtitle = subtitleOverride
      ? subtitleOverride
      : isPasskeySession
        ? 'Your passkey proved your identity — enter your passphrase once to unlock your encrypted data. It will be remembered for 24 hours.'
        : `Decrypt your data for <strong style="color:var(--text)">${esc(_kvEmail)}</strong>`;

    const modal = document.createElement('div');
    modal.id    = 'kv-passphrase-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.7);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px';
    modal.innerHTML = `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:28px 24px;width:100%;max-width:380px;box-shadow:0 8px 32px rgba(0,0,0,0.4)">
        <div style="margin-bottom:12px;text-align:center;color:var(--accent)"><svg aria-hidden="true" style="width:32px;height:32px"><use href="#i-${isPasskeySession ? 'unlock' : 'key-round'}"></use></svg></div>
        <h3 style="font-size:17px;font-weight:700;margin-bottom:6px;text-align:center">${isPasskeySession ? 'One-time unlock needed' : 'Enter passphrase'}</h3>
        <p style="font-size:13px;color:var(--muted);margin-bottom:18px;text-align:center;line-height:1.5">${subtitle}</p>
        <input type="password" id="kv-pp-input" placeholder="Your passphrase"
          style="width:100%;box-sizing:border-box;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:10px 12px;color:var(--text);font-family:var(--sans);font-size:15px;outline:none;margin-bottom:8px">
        <p id="kv-pp-error" style="font-size:12px;color:var(--danger);margin-bottom:12px;display:none">Incorrect passphrase</p>
        <label style="display:flex;align-items:center;gap:8px;font-size:13px;color:var(--muted);margin-bottom:18px;cursor:pointer">
          <input type="checkbox" id="kv-pp-trust" style="width:16px;height:16px;accent-color:var(--accent)" ${isPasskeySession ? 'checked' : ''}>
          Trust this device (stay signed in permanently)
        </label>
        <div style="display:flex;gap:10px">
          <button id="kv-pp-cancel" style="flex:1;padding:10px;border-radius:8px;border:1px solid var(--border);background:transparent;color:var(--muted);font-size:14px;cursor:pointer">Cancel</button>
          <button id="kv-pp-ok" style="flex:2;padding:10px;border-radius:8px;border:none;background:var(--accent);color:#111;font-size:14px;font-weight:700;cursor:pointer">Unlock →</button>
        </div>
      </div>`;
    document.body.appendChild(modal);

    const input    = modal.querySelector('#kv-pp-input');
    const okBtn    = modal.querySelector('#kv-pp-ok');
    const cancelBtn= modal.querySelector('#kv-pp-cancel');
    const trustChk = modal.querySelector('#kv-pp-trust');

    setTimeout(() => input.focus(), 100);

    const submit = () => {
      const passphrase = input.value;
      const trust      = trustChk.checked;
      if (!passphrase) return;
      modal.remove();
      resolve({ passphrase, trust });
    };

    okBtn.onclick     = submit;
    cancelBtn.onclick = () => { modal.remove(); resolve(null); };
    input.onkeydown   = e => { if (e.key === 'Enter') submit(); };
  });
}

// ── Sync: push encrypted data to KV ────────
async function kvPush() {
  if (!kvConnected || !_kvEmailHash) {
    console.warn('kvPush: not connected, skipping');
    return;
  }
  // Require either passphrase verifier OR passkey session token
  if (!_kvVerifier && !_kvSessionToken) {
    console.warn('kvPush: missing credentials (no verifier or sessionToken), skipping');
    return;
  }
  if (!await kvEnsureKey()) return;
  const allProfiles  = await getProfiles();
  const householdDir = Object.fromEntries(
    Object.entries(allProfiles).map(([k, p]) => [k, { name: p.name, colour: p.colour }])
  );
  const tombstones = await loadDeletedIds();
  const payload = JSON.stringify({
    items, settings, lastSynced: settings.lastSynced || new Date().toISOString(),
    groceries: groceryItems, departments: groceryDepts,
    groceryLists,
    reminders, deletedIds: [...tombstones],
    householdDir, activeProfile,
    shareTargets: _shareTargets,
    notes: notes.map(n => ({ ...n, body: n.locked ? undefined : n.body })),
  });

  const ciphertext = await kvEncrypt(_kvKey, payload);
  const res = await fetchKV(`${WORKER_URL}/data/push`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      emailHash:    _kvEmailHash,
      ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
      household:    activeProfile === 'default' ? null : activeProfile,
      ciphertext,
    }),
  });
  if (!res.ok) {
    const d = await res.json().catch(() => ({}));
    const msg = d.error || 'Push failed';
    if (msg.includes('not found') || res.status === 401) {
      console.warn('kvPush: auth failed —', msg);
    }
    throw new Error(msg);
  }
  // After successful push, re-encrypt for all active share targets
  await pushAllSharedData().catch(e => console.warn('pushAllSharedData failed:', e.message));
}

// ── Sync: pull and decrypt data from KV ────
async function kvPull() {
  if (!kvConnected && !_shareState) return null;
  if (kvConnected && (!_kvEmailHash || (!_kvVerifier && !_kvSessionToken))) {
    console.warn('kvPull: missing credentials, skipping');
    return null;
  }
  // Only require encryption key for own-account pulls, not share pulls
  if (kvConnected && !_shareState && !await kvEnsureKey()) return null;

  // Guest pull — _shareState takes priority even if the guest also has their own account.
  // A user with both a personal account AND a share must pull from the share, not their own data.
  if (_shareState) {
    if (!_shareKey) { console.warn('kvPull (share): no share key in memory'); return null; }
    const res = await fetchKV(`${WORKER_URL}/share/data/pull`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ guestEmailHash: _kvEmailHash, guestVerifier: _kvVerifier, guestSessionToken: _kvSessionToken, code: _shareState.code, household: activeProfile }),
    });
    if (!res.ok) {
      // Share has been deleted or guest removed — auto-clear local share state
      if (res.status === 404 || res.status === 403) {
        console.warn('kvPull (share): share gone from server, clearing local state');
        const code = _shareState.code;
        _shareState   = null;
        _sharedFileId = null;
        _shareKey     = null;
        saveShareState();
        try {
          const stored = await _getShareKeys();
          delete stored[code];
          await _setShareKeys(stored);
        } catch(e) {}
        applyTabPermissions();
        items = [];
        await saveData();
        scheduleRender(...RENDER_REGIONS);
        toast('Shared household access was removed — switching to your own account');
        if (kvConnected) setTimeout(() => kvSyncNow(true), 500);
      }
      return null;
    }
    const { ciphertext } = await res.json();
    if (!ciphertext) return null; // owner hasn't pushed yet — not an error
    try {
      const plain = await decryptWithShareKey(_shareKey, ciphertext);
      return JSON.parse(plain);
    } catch(e) { console.warn('kvPull (share): decrypt failed', e.message); return null; }
  }

  // Owner pull — from data/pull using own key
  const res = await fetchKV(`${WORKER_URL}/data/pull`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      emailHash: _kvEmailHash,
      ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
      household: activeProfile,
    }),
  });
  if (!res.ok) return null;
  const { ciphertext } = await res.json();
  if (!ciphertext) return null;
  try {

    const plain = await kvDecrypt(_kvKey, ciphertext);
    return JSON.parse(plain);
  } catch(e) {
    console.error('kvPull: decrypt failed — key mismatch or corrupted data.', e.message);
    throw new KvDecryptError('Could not decrypt your data — the encryption key may be wrong.');
  }
}

// ── syncNow for KV mode ────────────────────
async function kvSyncNow(silent = false) {
  if (!kvConnected && !_shareState) return;
  if (kvConnected && (!_kvEmailHash || (!_kvVerifier && !_kvSessionToken))) {
    console.warn('kvSyncNow: missing credentials, skipping');
    return;
  }
  if (!silent) updateSyncPill('syncing');
  const _wasSilent = silent;
  try {
    const remote = await kvPull();
    if (remote && Array.isArray(remote.items)) {
      const localLastSynced  = settings.lastSynced ? new Date(settings.lastSynced).getTime() : 0;
      const remoteLastSynced = remote.lastSynced   ? new Date(remote.lastSynced).getTime()   : 0;
      const remoteWins       = remoteLastSynced > localLastSynced;
      items = await mergeItems(items, remote.items, remoteWins);
      await saveData();
      if (remote.settings) {
        const localTags = settings.customTags;
        // Merge: remote wins for most settings, but local wins for user preferences
        // EXCEPTION: MFA config always takes from remote — it's a security setting
        // that must never be downgraded by stale local state.
        const remoteMfa = remote.settings.mfa;
        settings = { ...remote.settings, ...settings };
        if (remoteMfa !== undefined) settings.mfa = remoteMfa; // remote MFA always wins
        const remoteTags = remote.settings.customTags || [];
        settings.customTags = (localTags||[]).filter(t=>t&&t.trim()).length >= (remoteTags).filter(t=>t&&t.trim()).length ? (localTags||[]) : remoteTags;
        await _saveSettings();
      }
      if (remote.groceries) {
        const localEmpty = groceryItems.length === 0;
        const groceryTombstones = await loadGroceryDeletedIds();
        if (remoteWins || localEmpty) {
          // Filter out any items we have locally tombstoned (user deleted them)
          groceryItems = remote.groceries.filter(i => !groceryTombstones.has(i.id));
          await _saveGroceryLocal();
        } else {
          // Merge: add remote items not in local AND not tombstoned (i.e. not deleted locally)
          const localIds = new Set(groceryItems.map(i => i.id));
          const newFromRemote = remote.groceries.filter(i => !localIds.has(i.id) && !groceryTombstones.has(i.id));
          if (newFromRemote.length) {
            groceryItems = [...groceryItems, ...newFromRemote];
            await _saveGroceryLocal();
          }
        }
      }
      if (remote.departments?.length) {
        const localDeptsEmpty = groceryDepts.length === 0;
        if (remoteWins || localDeptsEmpty || remote.departments.length > groceryDepts.length) {
          groceryDepts = remote.departments;
          await saveGroceryDepts();
        }
      }
      // Restore grocery lists (named lists per store)
      if (remote.groceryLists && Array.isArray(remote.groceryLists)) {
        const localListsEmpty = groceryLists.length <= 1 && groceryLists[0]?.id === 'default';
        if (remoteWins || localListsEmpty || remote.groceryLists.length > groceryLists.length) {
          groceryLists = remote.groceryLists;
          await _saveGroceryLists();
        } else {
          // Merge: add any lists not present locally
          const localListIds = new Set(groceryLists.map(l => l.id));
          const newLists = remote.groceryLists.filter(l => !localListIds.has(l.id));
          if (newLists.length) {
            groceryLists = [...groceryLists, ...newLists];
            await _saveGroceryLists();
          }
        }
      }
      if (remote.reminders && Array.isArray(remote.reminders)) {
        const localREmpty = reminders.length === 0;
        if (remoteWins || localREmpty) {
          reminders = remote.reminders; await saveReminders();
        } else {
          const localRIds = new Set(reminders.map(r => r.id));
          const newR = remote.reminders.filter(r => !localRIds.has(r.id));
          if (newR.length) { reminders = [...reminders, ...newR]; await saveReminders(); }
        }
      }
      if (remote.deletedIds && Array.isArray(remote.deletedIds)) {
        const merged = await loadDeletedIds();
        remote.deletedIds.forEach(id => merged.add(id));
        await saveDeletedIds(merged);
      }
      // Merge notes
      if (remote.notes && Array.isArray(remote.notes)) {
        if (remoteWins || notes.length === 0) {
          notes = remote.notes;
        } else {
          const localNMap = new Map(notes.map(n => [n.id, n]));
          remote.notes.forEach(rn => {
            const ln = localNMap.get(rn.id);
            if (!ln || new Date(rn.updatedAt) > new Date(ln.updatedAt || 0)) {
              localNMap.set(rn.id, { ...rn, body: rn.locked ? (ln?.body) : rn.body });
            }
          });
          notes = [...localNMap.values()];
        }
        await saveNotes();
      }
      if (remote.householdDir) {
        const localProfiles = await getProfiles();
        let changed = false;
        const deletedHouseholds = _getDeletedHouseholds();
        Object.entries(remote.householdDir).forEach(([key, meta]) => {
          // Skip re-creating households the user has explicitly deleted
          if (deletedHouseholds.has(key)) return;
          if (!localProfiles[key]) {
            localProfiles[key] = { name: meta.name, colour: meta.colour, items: [], settings: {}, reminders: [], groceries: [], departments: [] };
            changed = true;
          } else {
            if (meta.name && localProfiles[key].name !== meta.name) { localProfiles[key].name = meta.name; changed = true; }
            if (meta.colour && localProfiles[key].colour !== meta.colour) { localProfiles[key].colour = meta.colour; changed = true; }
          }
        });
        if (changed) { await saveProfiles(localProfiles); renderSettingsHouseholdList(); }
      }
      // shareTargets are authoritative from /share/list, not the data blob — skip blob restore
      // loadShareTargets() is called separately on login and after mutations
      scheduleRender(...RENDER_REGIONS);
    }
    settings.lastSynced = new Date().toISOString();
    await _saveSettings();
    const tombstones = await loadDeletedIds();
    await saveDeletedIds(tombstones);
    // Push if we have any local data — items OR groceries OR reminders
    const hasLocalData = items.length > 0 || groceryItems.length > 0 || reminders.length > 0;
    if (kvConnected && !_shareState && hasLocalData) {
      await kvPush();
    } else if (kvConnected && !_shareState) {
      // Nothing local to push — pull was enough
    }
    if (!_wasSilent) updateSyncPill('synced'); else updateSyncPill('connected');
    hideDataLoadingOverlay();
  } catch(err) {
    hideDataLoadingOverlay();
    console.error('KV sync error:', err);
    if (err instanceof KvDecryptError) {
      // Key mismatch — wipe ALL cached key material including IDB trusted device,
      // so the bad key cannot be reloaded on next page refresh.
      _kvKey = null;
      try { localStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
      try { localStorage.removeItem('stockroom_kv_key_fallback'); } catch(e) {}
      try { localStorage.removeItem('stockroom_device_secret'); } catch(e) {}
      try { sessionStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
      try { await removeWrappedKey(getOrCreateDeviceId()); } catch(e) {}
      updateSyncPill('error');
      // Don't show the decrypt error banner if the wizard is active (user is in login flow)
      // or if this was a silent background sync (would be confusing/unexpected)
      const wizardActive = document.body.classList.contains('wizard-active');
      if (!wizardActive && !_wasSilent) {
        showDecryptErrorBanner();
      }
      return;
    }
    if (!_wasSilent) updateSyncPill('error');
    if (!_wasSilent) toast('Sync failed — ' + err.message);
  }
}

async function kvDeleteAccount() {
  requireReauth('Re-enter your passphrase to delete your account.', _doDeleteAccount, { passkeyAllowed: true });
}

async function _doDeleteAccount() {
  if (!confirm(`Delete your account?\n\nThis permanently removes all your data, households, reminders and share targets from the server. This cannot be undone.\n\nYour local data on this device will also be cleared.`)) return;
  if (!confirm('Are you absolutely sure? This cannot be undone.')) return;
  try {
    const res = await fetchKV(`${WORKER_URL}/user/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, verifier: _kvVerifier }),
    });
    if (!res.ok) { const d = await res.json().catch(()=>({})); throw new Error(d.error || 'Delete failed'); }
    // Clear all local state
    const deviceId = getOrCreateDeviceId();
    await removeWrappedKey(deviceId);
    localStorage.removeItem('stockroom_device_secret');
    localStorage.removeItem('stockroom_kv_session');
    localStorage.removeItem('stockroom_seen');
    localStorage.removeItem('stockroom_country_set');
    localStorage.removeItem('stockroom_protect_seen');
    try { sessionStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
    // Also clear IDB session copy
    dbPut('settings', 'stockroom_kv_session', null).catch(() => {});
    kvConnected  = false;
    _kvEmail     = '';
    _kvEmailHash = '';
    _kvVerifier  = '';
    _kvKey       = null;
    items        = [];
    await saveData();
    toast('Account deleted');
    // Reload to show registration screen
    setTimeout(() => location.reload(), 1500);
  } catch(err) {
    toast('Could not delete account: ' + err.message);
  }
}

async function kvSignOut() {
  if (!confirm('Sign out?\n\nYour encrypted data stays safely on the server. Sign back in with your email and passphrase to access it.')) return;
  // If MFA was active, record it so _mfaGate enforces it even offline on next login
  if (_mfaEnabled()) {
    localStorage.setItem('stockroom_mfa_was_active', _kvEmailHash || '1');
  } else {
    localStorage.removeItem('stockroom_mfa_was_active');
  }
  // Clear the per-session verified flag — next login must verify MFA again
  try { sessionStorage.removeItem(_MFA_SESSION_KEY); } catch(e) {}
  // Dismiss any decrypt error banner — no need to show it after an intentional sign-out
  document.getElementById('kv-decrypt-error-banner')?.remove();
  // Clean up any grocery drag UI
  document.getElementById('grocery-change-dept-zone')?.remove();
  document.getElementById('grocery-dept-picker-overlay')?.remove();
  groceryEditMode = false;
  // Clear device trust
  const deviceId = getOrCreateDeviceId();
  await removeWrappedKey(deviceId);
  localStorage.removeItem('stockroom_device_secret');
  localStorage.removeItem('stockroom_kv_key_fallback');
  localStorage.removeItem('stockroom_kv_session_key');
  try { sessionStorage.removeItem('stockroom_kv_session_key'); } catch(e) {}
  // Clear session credentials (but keep permanent setup flags)
  localStorage.removeItem('stockroom_kv_session');
  // Also clear IDB session copy so the user is fully signed out
  dbPut('settings', 'stockroom_kv_session', null).catch(() => {});
  localStorage.removeItem('stockroom_seen');
  // Note: keep stockroom_protect_seen and stockroom_country_set — they are permanent
  // one-time setup flags, not session data. Removing them causes the protect/country
  // screens to re-appear on every login, which is wrong.
  // Clear local app data (encrypted copy stays on server)
  items        = [];
  settings     = {};
  groceryItems = [];
  groceryDepts = [];
  reminders    = [];
  await saveData();
  await _saveSettings();
  // Explicitly clear groceries and reminders from their own IDB stores
  // (saveData only updates items/settings; grocery and reminder stores are separate)
  await dbPut('groceries',  'items',     []);
  await dbPut('reminders',  'reminders', []);
  await dbPut('departments','departments', []);
  // Reset in-memory state
  kvConnected  = false;
  _kvEmail     = '';
  _kvEmailHash = '';
  _kvVerifier  = '';
  _kvKey       = null;
  _shareState  = null;
  // Show login screen
  document.body.classList.add('wizard-active'); document.getElementById('wizard').style.display = 'flex';
  document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
  showKvLogin();
  updateSyncUI();
  toast('Signed out');
}

function openChangePassphrase() {
  const oldPass = prompt('Enter your current passphrase:');
  if (!oldPass) return;
  const newPass = prompt('Enter your new passphrase (min 8 characters):');
  if (!newPass || newPass.length < 8) { toast('New passphrase too short'); return; }
  const confirm_ = prompt('Confirm new passphrase:');
  if (newPass !== confirm_) { toast('Passphrases do not match'); return; }
  kvChangePassphrase(oldPass, newPass);
}

async function kvChangePassphrase(oldPass, newPass) {
  try {
    // Verify old passphrase
    const oldVerifier = await kvMakeVerifier(oldPass, _kvEmailHash);
    if (oldVerifier !== _kvVerifier) { toast('Current passphrase incorrect'); return; }
    // Decrypt with old key, re-encrypt with new key
    const oldKey    = await kvDeriveKey(_kvEmail, oldPass);
    const newKey    = await kvDeriveKey(_kvEmail, newPass);
    const newVerifier = await kvMakeVerifier(newPass, _kvEmailHash);
    // Pull current ciphertext
    const res = await fetchKV(`${WORKER_URL}/data/pull`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, verifier: _kvVerifier, household: activeProfile }),
    });
    if (!res.ok) throw new Error('Could not fetch current data');
    const { ciphertext } = await res.json();
    if (ciphertext) {
      const plain      = await kvDecrypt(oldKey, ciphertext);
      const newCipher  = await kvEncrypt(newKey, plain);
      // Push re-encrypted data with new verifier
      await fetchKV(`${WORKER_URL}/data/push`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, verifier: newVerifier, household: activeProfile, ciphertext: newCipher }),
      });
    }
    // Update session
    _kvKey      = newKey;
    _kvVerifier = newVerifier;
    try { localStorage.setItem('stockroom_kv_session', JSON.stringify({ email: _kvEmail, emailHash: _kvEmailHash, verifier: newVerifier })); } catch(e) {}
    toast('Passphrase changed ✓');
  } catch(err) { toast('Could not change passphrase: ' + err.message); }
}

// ═══════════════════════════════════════════
//  DROPBOX SYNC (kept for reference, disabled in KV build)
// ── Sync Queue — visual feedback for pending changes ──────
// Shows a bottom bar while changes are queued/syncing.
// Debounces rapid saves so we don't hammer the backend.
const _syncQueue = {
  pending:  0,
  syncing:  false,
  timer:    null,
  errorMsg: null,

  // Call this whenever a change is made (save item, delete, etc.)
  enqueue(label = 'Saving changes…') {
    this.pending++;
    this.errorMsg = null;
    this._show('syncing', label);
    // Debounce — wait 1.5s after last change before actually syncing
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this._flush(), 1500);
  },

  async _flush() {
    if (this.syncing) return;
    // If no user-initiated changes are pending, don't show the bar at all
    if (this.pending === 0) return;
    this.syncing = true;
    const count  = this.pending;
    this.pending = 0;
    this._show('syncing', `Syncing ${count} change${count!==1?'s':''}…`);
    try {
      // silent=true: bottom bar provides feedback; pill stays Connected
      if (kvConnected || _shareState) await kvSyncNow(true);
      this._show('done', 'All changes saved');
      setTimeout(() => this._hide(), 2500);
    } catch(e) {
      this._show('error', 'Sync failed — will retry');
      setTimeout(() => this._hide(), 4000);
    }
    this.syncing = false;
  },

  _show(state, label) {
    const bar      = document.getElementById('sync-queue-bar');
    const spinner  = document.getElementById('sqb-spinner');
    const done     = document.getElementById('sqb-done');
    const err      = document.getElementById('sqb-error');
    const lbl      = document.getElementById('sqb-label');
    const cnt      = document.getElementById('sqb-count');
    if (!bar) return;
    bar.classList.add('visible');
    spinner.style.display = state === 'syncing' ? 'block' : 'none';
    done.style.display    = state === 'done'    ? 'block' : 'none';
    err.style.display     = state === 'error'   ? 'block' : 'none';
    if (lbl) lbl.textContent = label;
    if (cnt) cnt.textContent = this.pending > 0 ? `${this.pending} pending` : '';
  },

  _hide() {
    document.getElementById('sync-queue-bar')?.classList.remove('visible');
  },
};

// ── syncAll with debounce to prevent rapid-fire calls ─────
let _syncDebounceTimer = null;
async function syncAll() {
  if (_syncDebounceTimer) clearTimeout(_syncDebounceTimer);
  return new Promise(resolve => {
    _syncDebounceTimer = setTimeout(async () => {
      _syncDebounceTimer = null;
      if (kvConnected || _shareState) await kvSyncNow(true); // always silent — background only
      resolve();
    }, 1200);
  });
}

// ── Unified UI update for both providers ────────────────
function updateSyncUI() {
  const pill  = document.getElementById('sync-pill');
  const label = document.getElementById('sync-label');
  if (!pill || !label) return;
  if (kvConnected && settings.lastSynced) {
    pill.className    = 'sync-pill synced';
    label.textContent = 'Connected';
  } else if (kvConnected) {
    pill.className    = 'sync-pill synced';
    label.textContent = 'Connected';
  } else if (_shareState) {
    pill.className    = 'sync-pill synced';
    label.textContent = 'Connected';
  } else {
    pill.className    = 'sync-pill pending';
    label.textContent = 'Not signed in';
  }
  // Update account email in settings
  const el = document.getElementById('kv-account-email');
  if (el && _kvEmail) el.textContent = _kvEmail;
}

function renderSettingsForUser() {
  const settingsView = document.getElementById('view-settings');
  if (!settingsView) return;
  _updateSidebarProfile();

  if (!isOwner()) {
    // Shared user — show banner and hide owner-only cards
    if (!document.getElementById('shared-user-settings-banner')) {
      const banner = document.createElement('div');
      banner.id    = 'shared-user-settings-banner';
      banner.style.cssText = 'background:rgba(91,141,238,0.08);border:1px solid rgba(91,141,238,0.2);border-radius:12px;padding:14px 16px;margin-bottom:16px;font-size:13px;line-height:1.6';
      const userName  = esc(_shareState?.name || '');
      const ownerName = esc(_shareState?.ownerName || 'the owner');
      const type      = _shareState?.type || 'guest';
      const roleLabel = type === 'family' ? ' as a <strong>family member</strong>' : type === 'cleaner' ? ' as a <strong>cleaner</strong>' : ' as a <strong>guest</strong>';
      const greeting  = userName ? `Hi <strong>${userName}</strong>, you` : 'You';
      banner.innerHTML = `
        <div style="font-size:14px;margin-bottom:6px">${greeting} have joined <strong>${ownerName}</strong>'s household${roleLabel}.</div>
        <span style="color:var(--muted);font-size:12px">Some settings are managed by the owner.</span>
        ${!kvConnected ? `<br><button class="btn btn-ghost btn-sm" style="margin-top:8px" onclick="showKvRegister();document.getElementById('wizard').style.display='flex'">☁️ Create your own account</button>` : ''}
        <button class="btn btn-ghost btn-sm" style="margin-top:8px;color:var(--danger);border-color:var(--danger)" onclick="leaveShare()">Leave shared household</button>
      `;
      settingsView.insertBefore(banner, settingsView.firstChild);
    }
    const ownerOnlyHeadings = ['Alerts', 'Reminder Schedules', 'Preferences', 'Data'];
    document.querySelectorAll('#view-settings .settings-card h3').forEach(h3 => {
      if (ownerOnlyHeadings.some(s => h3.textContent.includes(s)))
        h3.closest('.settings-card').style.display = 'none';
    });
    return;
  }

  // Owner — show/hide cards based on sign-in state
  const signedIn = kvConnected;
  document.querySelectorAll('#view-settings .settings-card h3').forEach(h3 => {
    const text = h3.textContent;
    const card = h3.closest('.settings-card');
    // These cards require being signed in
    const needsAuth = ['Alerts', 'Reminder Schedules', 'Preferences', 'Data', 'Households'];
    if (needsAuth.some(s => text.includes(s))) {
      card.style.display = signedIn ? '' : 'none';
    }
  });
  // Populate display name — both hidden (Settings) and visible (Account & Security)
  ['setting-display-name', 'setting-display-name-sec'].forEach(id => {
    const el = document.getElementById(id);
    if (el && settings.displayName) el.value = settings.displayName;
  });
  // Notes 2FA button state (compat — kept for old settings UI buttons if present)
  const n2faBtn = document.getElementById('notes-2fa-settings-btn');
  if (n2faBtn) n2faBtn.textContent = _mfaEnabled() ? 'Disable' : 'Enable';
  updateHeaderGreeting();
  _updateSidebarProfile();
  renderAccountSecurity();
}

function updateSyncPill(state, provider) {
  const pill  = document.getElementById('sync-pill');
  const label = document.getElementById('sync-label');
  const navPill  = document.getElementById('app-nav-sync');
  const navLabel = navPill?.querySelector('.sync-label-nav');

  function _applyState(p, l) {
    if (!p || !l) return;
    if (state === 'syncing')   { p.className = p.className.replace(/synced|error/g,'').trim() + ' pending'; l.textContent = 'Syncing…'; }
    if (state === 'synced')    { p.className = p.className.replace(/pending|error/g,'').trim() + ' synced';  l.textContent = 'Synced'; }
    if (state === 'connected') { p.className = p.className.replace(/pending|error/g,'').trim() + ' synced';  l.textContent = 'Connected'; }
    if (state === 'error')     { p.className = p.className.replace(/synced|pending/g,'').trim() + ' error';  l.textContent = 'Sync error'; }
  }

  if (pill && label) {
    if (state === 'syncing') { pill.className = 'sync-pill pending'; label.textContent = 'Syncing…'; }
    if (state === 'synced')  { pill.className = 'sync-pill synced';  label.textContent = 'Synced';
      clearTimeout(pill._connectedTimer);
      pill._connectedTimer = setTimeout(() => {
        if (pill.className === 'sync-pill synced' && label.textContent === 'Synced') label.textContent = 'Connected';
        if (navLabel && navLabel.textContent === 'Synced') navLabel.textContent = 'Connected';
      }, 3000);
    }
    if (state === 'connected') { pill.className = 'sync-pill synced'; label.textContent = 'Connected'; }
    if (state === 'error')     { pill.className = 'sync-pill error';  label.textContent = 'Sync error'; }
  }
  _applyState(navPill, navLabel);
}


// ═══════════════════════════════════════════════════════════
//  ADAPTIVE DARK MODE 2.0 — Time-of-day colour temperature
// ═══════════════════════════════════════════════════════════

function applyAdaptiveColourTemp() {
  const h = new Date().getHours();
  let tint = 'rgba(0,0,0,0)';
  let surfaceOpacity = '0.82';

  if (h >= 22 || h < 6) {
    // Late night / early morning: warm amber tint, more opaque surfaces
    tint = 'rgba(60,25,0,0.08)';
    surfaceOpacity = '0.92';
    document.documentElement.style.setProperty('--ok',    '#3db87a');
    document.documentElement.style.setProperty('--accent2','#4f82e0');
  } else if (h >= 6 && h < 10) {
    // Morning: cool blue-white, crisp
    tint = 'rgba(10,20,60,0.04)';
    surfaceOpacity = '0.78';
    document.documentElement.style.setProperty('--ok',    '#4cbb8a');
    document.documentElement.style.setProperty('--accent2','#5b8dee');
  } else if (h >= 18 && h < 22) {
    // Evening: subtle warm shift
    tint = 'rgba(40,15,0,0.05)';
    surfaceOpacity = '0.86';
    document.documentElement.style.setProperty('--ok',    '#45b882');
    document.documentElement.style.setProperty('--accent2','#547ee8');
  } else {
    // Daytime: neutral
    tint = 'rgba(0,0,0,0)';
    surfaceOpacity = '0.82';
    document.documentElement.style.setProperty('--ok',    '#4cbb8a');
    document.documentElement.style.setProperty('--accent2','#5b8dee');
  }

  document.documentElement.style.setProperty('--temp-tint', tint);
  document.documentElement.style.setProperty('--surface-opacity', surfaceOpacity);
}

applyAdaptiveColourTemp();
// Re-check every 10 minutes in case the app stays open across hour boundaries
setInterval(applyAdaptiveColourTemp, 600000);


// ═══════════════════════════════════════════════════════════
//  PREDICTIVE MICRO-INTERACTIONS
// ═══════════════════════════════════════════════════════════

// Pre-warm renders on pointerdown (fires ~80ms before click on mobile)
// and on hover (desktop). Gives the UI a "telepathic" feel.

let _groceryPrewarmed = false;
let _reportPrewarmed  = false;

async function prewarmView(name) {
  if (name === 'grocery' && !_groceryPrewarmed) {
    _groceryPrewarmed = true;
    // Pre-load grocery data into memory so render is instant on click
    await loadGrocery();
  }
  if (name === 'report' && !_reportPrewarmed) {
    _reportPrewarmed = true;
    // Pre-calculate report data off the critical path
    requestIdleCallback ? requestIdleCallback(() => renderReport()) : setTimeout(() => renderReport(), 0);
  }
}

// Patch tab buttons with pointerdown + pointerenter pre-warming
// ═══════════════════════════════════════════════════════════
//  FLOATING ACTION BUTTON (mobile only, context-aware)
// ═══════════════════════════════════════════════════════════

let _fabOpen = false;

const FAB_ACTIONS = {
  stock: [
    { icon: '<svg class="icon" aria-hidden="true"><use href="#i-zap"></use></svg>',    label: 'Quick Add',    action: () => { closeFab(); openQuickAdd(); } },
    { icon: '<svg class="icon" aria-hidden="true"><use href="#i-camera"></use></svg>', label: 'Scan Barcode', action: () => { closeFab(); sessionStorage.setItem('barcode_target','scan-chooser'); openBarcodeScanner(); } },
    { icon: '<svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg>',   label: 'Add Item',     action: () => { closeFab(); openAddModal(); } },
  ],
  reminders: [
    { icon: '<svg class="icon" aria-hidden="true"><use href="#i-plus"></use></svg>', label: 'Add Reminder', action: () => { closeFab(); openAddReminderModal(); } },
  ],
  notes: [
    { icon: '<svg class="icon" aria-hidden="true"><use href="#i-notebook-pen"></use></svg>', label: 'New Note', action: () => { closeFab(); openNoteEditor(null); } },
  ],
};

// Grocery FAB: primary action slides left, secondaries stack above
function getGroceryFabActions() {
  if (groceryEditMode) {
    return {
      primary:     { icon: '<svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg>',          label: 'Done editing',   action: () => { closeFab(); toggleGroceryEditMode(); } },
      secondaries: [
        { icon: '<svg class="icon" aria-hidden="true"><use href="#i-clipboard-list"></use></svg>', label: 'Import List', action: () => { closeFab(); openGroceryImport(); } },
        { icon: '<svg class="icon" aria-hidden="true"><use href="#i-tag"></use></svg>',             label: 'Edit Depts',  action: () => { closeFab(); openGroceryDepts(); } },
      ],
    };
  }
  return {
    primary:     { icon: '<svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg>',         label: 'Add / Edit List', action: () => { closeFab(); toggleGroceryEditMode(); } },
    secondaries: [
      { icon: '<svg class="icon" aria-hidden="true"><use href="#i-zap"></use></svg>',            label: 'Quick List',  action: () => { closeFab(); openQuickList(); } },
      { icon: '<svg class="icon" aria-hidden="true"><use href="#i-clipboard-list"></use></svg>', label: 'Import List', action: () => { closeFab(); openGroceryImport(); } },
      { icon: '<svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg>',           label: 'Add Store',   action: () => { closeFab(); openAddGroceryList(); } },
      { icon: '<svg class="icon" aria-hidden="true"><use href="#i-tag"></use></svg>',             label: 'Edit Depts',  action: () => { closeFab(); openGroceryDepts(); } },
    ],
  };
}

function updateFab(viewName) {
  const btn       = document.getElementById('fab-btn');
  const container = document.getElementById('fab-container');
  if (!btn || !container) return;
  const isMobile  = window.innerWidth < 700;
  // FAB only on stock, grocery, reminders, notes — never on settings/savings/report
  const hasActions = (viewName === 'grocery' || !!FAB_ACTIONS[viewName])
                  && viewName !== 'settings' && viewName !== 'savings' && viewName !== 'report';
  if (!isMobile || !hasActions) {
    btn.style.display = 'none'; container.style.display = 'none';
    closeFab(true); return;
  }
  btn.style.display = 'flex';
  btn.style.opacity = '1';
  closeFab(true);
}

function toggleFab() { _fabOpen ? closeFab() : openFab(); }

function openFab() {
  _fabOpen = true;
  const btn       = document.getElementById('fab-btn');
  const menu      = document.getElementById('fab-menu');
  const container = document.getElementById('fab-container');
  if (!btn || !menu || !container) return;

  btn.textContent      = '×';
  btn.style.transform  = 'rotate(45deg)';
  btn.style.background = 'var(--surface)';
  btn.style.color      = 'var(--text)';
  btn.style.boxShadow  = '0 4px 20px rgba(0,0,0,0.4)';
  container.style.display = 'block';

  if (_currentView === 'grocery') {
    const { primary, secondaries } = getGroceryFabActions();

    // Secondaries stack above FAB (normal vertical menu)
    menu.innerHTML = secondaries.map((a, i) => `
      <div style="display:flex;align-items:center;justify-content:flex-end;gap:10px;animation:fabItemIn 0.18s ease ${(i+1)*0.06}s both">
        <span onclick="(${a.action.toString()})()" style="font-size:17px;font-weight:600;color:var(--text);background:var(--surface);padding:7px 14px;border-radius:8px;white-space:nowrap;box-shadow:0 2px 8px rgba(0,0,0,0.3);border:1px solid var(--border);cursor:pointer">${a.label}</span>
        <button onclick="(${a.action.toString()})()" style="width:52px;height:52px;border-radius:50%;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:22px;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 2px 12px rgba(0,0,0,0.3);flex-shrink:0">${a.icon}</button>
      </div>`).join('');

    // Primary slides out to the LEFT of the FAB button
    document.getElementById('fab-primary-slide')?.remove();
    const slide = document.createElement('div');
    slide.id = 'fab-primary-slide';
    slide.style.cssText = 'position:fixed;bottom:88px;right:112px;z-index:1100;display:flex;align-items:center;gap:10px;animation:fabSlideLeft 0.22s cubic-bezier(0.34,1.56,0.64,1) both';
    slide.innerHTML = `
      <span onclick="(${primary.action.toString()})()" style="font-size:17px;font-weight:700;color:#111;background:var(--accent);padding:10px 18px;border-radius:12px;white-space:nowrap;box-shadow:0 4px 16px rgba(232,168,56,0.45);cursor:pointer;border:none">${primary.label}</span>
      <span style="font-size:24px">${primary.icon}</span>`;
    container.appendChild(slide);

  } else {
    const actions = FAB_ACTIONS[_currentView] || [];
    menu.innerHTML = actions.map((a, i) => `
      <div style="display:flex;align-items:center;gap:10px;animation:fabItemIn 0.18s ease ${i*0.05}s both">
        <span onclick="(${a.action.toString()})()" style="font-size:17px;font-weight:600;color:var(--text);background:var(--surface);padding:7px 14px;border-radius:8px;white-space:nowrap;box-shadow:0 2px 8px rgba(0,0,0,0.3);border:1px solid var(--border);cursor:pointer">${a.label}</span>
        <button onclick="(${a.action.toString()})()" style="width:52px;height:52px;border-radius:50%;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:22px;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 2px 12px rgba(0,0,0,0.3);flex-shrink:0">${a.icon}</button>
      </div>`).join('');
  }
}

function closeFab(silent = false) {
  _fabOpen = false;
  const btn       = document.getElementById('fab-btn');
  const menu      = document.getElementById('fab-menu');
  const container = document.getElementById('fab-container');
  document.getElementById('fab-primary-slide')?.remove();
  if (!btn) return;
  btn.textContent      = '+';
  btn.style.transform  = '';
  btn.style.background = 'var(--accent)';
  btn.style.color      = '#111';
  btn.style.boxShadow  = '0 4px 20px rgba(232,168,56,0.5)';
  if (menu) menu.innerHTML = '';
  if (container) container.style.display = 'none';
}

// Resize handler — show/hide FAB based on viewport width
// ── Sticky tab bar scroll behaviour ─────────────────────────────
// On desktop: add/remove .tabs-scrolled class based on scroll position
// This drives the border and padding transition in CSS
(function initTabsScroll() {
  let _lastScrollY = 0;
  const SCROLL_THRESHOLD = 40; // px from top before "scrolled" state kicks in

  function _updateTabsScroll() {
    const wrap = document.getElementById('tabs-sticky-wrap');
    if (!wrap) return;
    const scrolled = (window.scrollY || document.documentElement.scrollTop) > SCROLL_THRESHOLD;
    wrap.classList.toggle('tabs-scrolled', scrolled);
  }

  // Only wire up on desktop — on mobile the tabs wrap isn't sticky
  if (window.innerWidth >= 900) {
    window.addEventListener('scroll', _updateTabsScroll, { passive: true });
  }
  window.addEventListener('resize', () => {
    if (window.innerWidth >= 900) {
      window.addEventListener('scroll', _updateTabsScroll, { passive: true });
    }
  }, { passive: true });
})();

// ── Smart sync — latency-aware debounce for grocery checks ──────
// Detects high-latency / cellular connections and batches grocery
// UI updates optimistically (instant local render) then syncs lazily.
const _smartSync = {
  _isHighLatency: false,
  _lastProbe:     0,
  _probeInterval: 30_000, // probe every 30s max

  // Quick heuristic: NetworkInformation API if available
  _checkNetwork() {
    try {
      const conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
      if (conn) {
        // effectiveType: 'slow-2g'|'2g'|'3g'|'4g'
        if (['slow-2g','2g','3g'].includes(conn.effectiveType)) return true;
        if (conn.saveData) return true;
        if (conn.rtt && conn.rtt > 200) return true;  // >200ms RTT = high latency
      }
    } catch(e) {}
    return false;
  },

  // Probe actual latency with a tiny HEAD request to our own origin
  async _probeLatency() {
    const now = Date.now();
    if (now - this._lastProbe < this._probeInterval) return;
    this._lastProbe = now;
    try {
      const t0  = performance.now();
      await fetch(window.location.origin + '/favicon.ico?_=' + now, {
        method: 'HEAD', cache: 'no-store', signal: AbortSignal.timeout(3000),
      });
      const rtt = performance.now() - t0;
      this._isHighLatency = rtt > 300; // >300ms round trip = treat as high latency
    } catch(e) {
      // If probe fails, assume high latency / offline
      this._isHighLatency = true;
    }
  },

  // Returns appropriate debounce delay in ms
  debounceMs() {
    if (this._checkNetwork() || this._isHighLatency) return 5000; // batch over 5s on slow connections
    return 1500; // default
  },

  // Queue-aware enqueue that respects detected latency
  enqueueGrocery() {
    // Run probe in background (non-blocking)
    this._probeLatency().catch(() => {});
    const delay = this.debounceMs();
    _syncQueue.pending++;
    _syncQueue.errorMsg = null;
    clearTimeout(_syncQueue.timer);
    _syncQueue.timer = setTimeout(() => _syncQueue._flush(), delay);
    // Show a subtle indicator only if delay is noticeable
    if (delay >= 5000) {
      _syncQueue._show('syncing', 'Changes queued — syncing when ready…');
    } else {
      _syncQueue._show('syncing', 'Saving…');
    }
  },
};

window.addEventListener('resize', () => {
  if (_currentView) updateFab(_currentView);
}, { passive: true });

document.addEventListener('DOMContentLoaded', () => {
  // Capture "Remember me" checkbox state into a module variable immediately.
  // There are two checkboxes: one on the email step (new users) and one on the
  // passphrase step (returning users). Both set _rememberMeChecked.
  const rememberCb = document.getElementById('remember-me-checkbox');
  const rememberCbAuth = document.getElementById('remember-me-checkbox-auth');
  // Pre-check if email already remembered
  const emailAlreadyRemembered = !!getRememberedEmail() || getCookieConsent() === 'granted';
  if (emailAlreadyRemembered) {
    _rememberMeChecked = true;
    if (rememberCb) rememberCb.checked = true;
    if (rememberCbAuth) rememberCbAuth.checked = true;
  }
  if (rememberCb) {
    rememberCb.addEventListener('change', () => {
      _rememberMeChecked = rememberCb.checked;
      if (rememberCbAuth) rememberCbAuth.checked = rememberCb.checked;
    });
  }
  if (rememberCbAuth) {
    rememberCbAuth.addEventListener('change', () => {
      _rememberMeChecked = rememberCbAuth.checked;
      if (rememberCb) rememberCb.checked = rememberCbAuth.checked;
    });
  }

  document.querySelectorAll('.tab').forEach(btn => {
    const onclick = btn.getAttribute('onclick') || '';
    const match   = onclick.match(/showView\('(\w+)'/);
    if (!match) return;
    const viewName = match[1];
    const prewarm  = () => prewarmView(viewName);
    btn.addEventListener('pointerenter', prewarm, {passive:true});
    btn.addEventListener('pointerdown',  prewarm, {passive:true});
  });
});

// Predictive button ripple — gives physical depth feedback on tap
document.addEventListener('pointerdown', (e) => {
  const btn = e.target.closest('.btn, .tab, .filter-chip, .grocery-cb');
  if (!btn || btn.classList.contains('no-ripple')) return;

  const ripple = document.createElement('span');
  const rect   = btn.getBoundingClientRect();
  const size   = Math.max(rect.width, rect.height) * 2;
  const x      = e.clientX - rect.left - size / 2;
  const y      = e.clientY - rect.top  - size / 2;

  ripple.style.cssText = `
    position:absolute;width:${size}px;height:${size}px;
    left:${x}px;top:${y}px;border-radius:50%;
    background:rgba(255,255,255,0.12);pointer-events:none;
    transform:scale(0);animation:ripple-expand 0.45s ease-out forwards;
    z-index:0;
  `;

  // Ensure the button has relative positioning for ripple containment
  const pos = getComputedStyle(btn).position;
  if (pos === 'static') btn.style.position = 'relative';
  btn.style.overflow = 'hidden';
  btn.appendChild(ripple);
  setTimeout(() => ripple.remove(), 500);
}, {passive:true});


// ═══════════════════════════════════════════════════════════
//  GROCERY LIST
// ═══════════════════════════════════════════════════════════

const DEFAULT_DEPTS = [
  { id:'bakery',     name:'Bakery',       emoji:'🍞' },
  { id:'fruit-veg',  name:'Fruit & Veg',  emoji:'🥦' },
  { id:'meat-fish',  name:'Meat & Fish',  emoji:'🥩' },
  { id:'dairy',      name:'Dairy',        emoji:'🧀' },
  { id:'frozen',     name:'Frozen',       emoji:'🧊' },
  { id:'drinks',     name:'Drinks',       emoji:'🥤' },
  { id:'snacks',     name:'Snacks',       emoji:'🍿' },
  { id:'cupboard',   name:'Cupboard',     emoji:'🥫' },
  { id:'household',  name:'Household',    emoji:'🧹' },
  { id:'toiletries', name:'Toiletries',   emoji:'🧴' },
  { id:'baby-care',  name:'Baby Care',    emoji:'🍼' },
  { id:'other',      name:'Other',        emoji:'📦' },
];

let groceryItems    = [];
let groceryDepts    = [];
let grocerySort     = 'dept'; // 'dept' | 'alpha'
let groceryEditMode    = false;  // unlock/lock toggle
let groceryHideChecked = false;  // hide ticked items toggle
let groceryLists    = [];    // [{ id, name, store, createdAt, updatedAt }]
try { groceryHideChecked = localStorage.getItem('stockroom_hide_checked') === '1'; } catch(e) {}
let activeGroceryListId = 'default'; // currently viewed list
let grocerySelected   = new Set(); // IDs selected in edit mode multi-select

// Manual sort order — array of item IDs in user-defined order
// Persisted to localStorage so it survives page reloads and view switches
function getGroceryManualOrder() {
  try { return JSON.parse(localStorage.getItem('stockroom_grocery_order') || '[]'); } catch(e) { return []; }
}
function saveGroceryManualOrder(order) {
  try { localStorage.setItem('stockroom_grocery_order', JSON.stringify(order)); } catch(e) {}
}
// Returns groceryItems sorted by manual order (items not in order go to end)
function getGroceryItemsInOrder() {
  const order = getGroceryManualOrder();
  const listFiltered = groceryItems.filter(i => (i.listId || 'default') === activeGroceryListId);
  if (!order.length) return [...listFiltered];
  const orderMap = new Map(order.map((id, i) => [id, i]));
  return [...listFiltered].sort((a, b) => {
    const ai = orderMap.has(a.id) ? orderMap.get(a.id) : Infinity;
    const bi = orderMap.has(b.id) ? orderMap.get(b.id) : Infinity;
    return ai - bi;
  });
}
// When a new item is added, append its id to the end of manual order
function appendToGroceryOrder(id) {
  const order = getGroceryManualOrder();
  if (!order.includes(id)) { order.push(id); saveGroceryManualOrder(order); }
}
// Clean stale IDs from the order array (items that were deleted)
function cleanGroceryOrder() {
  const ids = new Set(groceryItems.map(i => i.id));
  const clean = getGroceryManualOrder().filter(id => ids.has(id));
  saveGroceryManualOrder(clean);
}
let groceryContextTarget = null;
let groceryConvertItem   = null;

async function loadGrocery() {
  const storedItems = await dbGet('groceries', 'items');
  const storedDepts = await dbGet('departments', 'departments');
  const storedLists = await dbGet('groceryLists', 'groceryLists');
  if (storedItems) {
    groceryItems = storedItems;
  } else {
    try {
      const raw = localStorage.getItem('stockroom_groceries');
      if (raw) { groceryItems = JSON.parse(raw) || []; await dbPut('groceries', 'items', groceryItems); localStorage.removeItem('stockroom_groceries'); }
    } catch(e) { groceryItems = []; }
  }
  if (storedDepts) {
    groceryDepts = storedDepts;
  } else {
    try {
      const raw = localStorage.getItem('stockroom_departments');
      if (raw) { groceryDepts = JSON.parse(raw); await dbPut('departments', 'departments', groceryDepts); localStorage.removeItem('stockroom_departments'); }
    } catch(e) { groceryDepts = null; }
  }
  if (!groceryDepts || groceryDepts.length === 0) groceryDepts = DEFAULT_DEPTS.map(d => ({...d}));

  // Load grocery lists — migrate existing items to 'default' list if needed
  if (storedLists && storedLists.length) {
    groceryLists = storedLists;
  } else {
    groceryLists = [{ id: 'default', name: 'Main List', store: '', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }];
    await _saveGroceryLists();
  }
  // Ensure all items have a listId
  let needsSave = false;
  groceryItems.forEach(i => { if (!i.listId) { i.listId = 'default'; needsSave = true; } });
  if (needsSave) await _saveGroceryLocal();
  activeGroceryListId = localStorage.getItem('stockroom_active_grocery_list') || 'default';
  if (!groceryLists.find(l => l.id === activeGroceryListId)) activeGroceryListId = 'default';
}

async function _saveGroceryLists() {
  await dbPut('groceryLists', 'groceryLists', groceryLists);
}

function _activeGroceryListItems() {
  return groceryItems.filter(i => (i.listId || 'default') === activeGroceryListId);
}

function _activeGroceryList() {
  return groceryLists.find(l => l.id === activeGroceryListId) || groceryLists[0];
}

// ── Grocery List Picker (shown when 2+ lists exist) ──────────────────────
function renderGroceryListPicker() {
  const body = document.getElementById('grocery-list-body');
  const query = (document.getElementById('grocery-search')?.value || '').toLowerCase().trim();
  if (!body) return;

  let lists = [...groceryLists].sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));

  // When searching, include lists that match by name/store OR contain matching items
  if (query) {
    lists = lists.filter(l => {
      const nameMatch  = l.name.toLowerCase().includes(query) || (l.store||'').toLowerCase().includes(query);
      const itemMatch  = groceryItems.some(i =>
        (i.listId||'default') === l.id &&
        (i.name.toLowerCase().includes(query) || (i.notes||'').toLowerCase().includes(query))
      );
      return nameMatch || itemMatch;
    });
  }

  const fmt = d => new Date(d).toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' });

  body.innerHTML = `
    <div style="margin-bottom:16px">
      ${lists.map(l => {
        const allListItems = groceryItems.filter(i => (i.listId||'default') === l.id);
        const itemCount = allListItems.filter(i => !i.checked).length;
        const checked   = allListItems.filter(i =>  i.checked).length;

        // When searching, show matching items inline under the list card
        let matchingItemsHTML = '';
        if (query) {
          const matchingItems = allListItems.filter(i =>
            i.name.toLowerCase().includes(query) || (i.notes||'').toLowerCase().includes(query)
          );
          if (matchingItems.length) {
            matchingItemsHTML = `<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border)">
              ${matchingItems.slice(0,5).map(i =>
                `<div style="font-size:12px;color:var(--muted);padding:2px 0;display:flex;align-items:center;gap:6px">
                  <span style="color:${i.checked?'var(--ok)':'var(--text)'}">${i.checked?'<svg class="icon" aria-hidden="true"><use href="#i-square-check"></use></svg>':'<svg class="icon" aria-hidden="true"><use href="#i-square"></use></svg>'}</span>
                  <span style="${i.checked?'text-decoration:line-through':''}">${esc(i.name)}</span>
                  ${i.notes?`<span style="color:var(--muted);font-style:italic">— ${esc(i.notes)}</span>`:''}
                </div>`
              ).join('')}
              ${matchingItems.length > 5 ? `<div style="font-size:11px;color:var(--muted);margin-top:4px">+${matchingItems.length - 5} more</div>` : ''}
            </div>`;
          }
        }

        return `
        <div onclick="switchGroceryList('${l.id}')" style="display:flex;flex-direction:column;padding:14px 16px;background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:10px;cursor:pointer;transition:border-color 0.15s" onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
          <div style="display:flex;align-items:center;gap:14px">
            <div style="flex:1;min-width:0">
              <div style="font-size:16px;font-weight:700;margin-bottom:3px">${esc(l.name)}</div>
              <div style="font-size:12px;color:var(--muted);font-family:var(--mono)">
                ${l.store ? `<svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg> ${esc(l.store)} · ` : ''}${itemCount} item${itemCount!==1?'s':''} remaining${checked ? ` · ${checked} done` : ''} · ${fmt(l.updatedAt)}
              </div>
            </div>
            <div style="display:flex;gap:6px;flex-shrink:0">
              <button onclick="event.stopPropagation();editGroceryList('${l.id}')" style="padding:6px 10px;border-radius:7px;border:1px solid var(--border);background:var(--surface2);color:var(--muted);font-size:13px;cursor:pointer"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
              ${groceryLists.length > 1 ? `<button onclick="event.stopPropagation();deleteGroceryList('${l.id}')" style="padding:6px 10px;border-radius:7px;border:1px solid var(--border);background:var(--surface2);color:var(--danger);font-size:13px;cursor:pointer"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg></button>` : ''}
            </div>
          </div>
          ${matchingItemsHTML}
        </div>`; }).join('')}
    </div>`;

  // Switch sort button labels for multi-list view
  const _deptBtn = document.getElementById('grocery-sort-dept');
  const _alphaBtn = document.getElementById('grocery-sort-alpha');
  if (_deptBtn) _deptBtn.textContent = 'By Store';
  if (_alphaBtn) _alphaBtn.textContent = 'A–Z';
  const sub = document.getElementById('grocery-subtitle');
  if (sub) sub.textContent = query
    ? `${lists.length} list${lists.length!==1?'s':''} match`
    : `${groceryLists.length} list${groceryLists.length!==1?'s':''} · tap to open`;
}

function switchGroceryList(id) {
  groceryEditMode = false;
  activeGroceryListId = id;
  try { localStorage.setItem('stockroom_active_grocery_list', id); } catch(e) {}
  // Restore per-list sort labels
  const _db = document.getElementById('grocery-sort-dept');
  const _ab = document.getElementById('grocery-sort-alpha');
  if (_db) _db.textContent = 'By Dept';
  if (_ab) _ab.textContent = 'A–Z';
  const list = groceryLists.find(l => l.id === id);
  if (list) { list.updatedAt = new Date().toISOString(); _saveGroceryLists(); }
  renderGrocery();
}

// ── Add / Edit Store list ─────────────────────────────────────────────────
function openAddGroceryList() {
  _openGroceryListModal(null);
}

function editGroceryList(id) {
  _openGroceryListModal(id);
}

function _openGroceryListModal(id) {
  document.getElementById('grocery-list-picker-overlay')?.remove();
  const list = id ? groceryLists.find(l => l.id === id) : null;
  const overlay = document.createElement('div');
  overlay.id = 'grocery-list-picker-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:600;background:rgba(0,0,0,0.7);display:flex;align-items:flex-end;justify-content:center;backdrop-filter:blur(4px)';
  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:20px 20px 0 0;width:100%;max-width:560px;padding:24px 20px 36px;box-shadow:0 -8px 32px rgba(0,0,0,0.5)">
      <div style="width:40px;height:4px;background:var(--border);border-radius:2px;margin:0 auto 18px"></div>
      <h3 style="font-size:18px;font-weight:700;margin-bottom:16px;text-align:center">${list ? 'Edit List' : '➕ New Shopping List'}</h3>
      <div class="field" style="margin-bottom:12px">
        <label style="font-size:12px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px">List name</label>
        <input id="gl-name" class="form-input" type="text" value="${esc(list?.name||'')}" placeholder="e.g. Tesco run, Weekend shop…" autocomplete="off">
      </div>
      <div class="field" style="margin-bottom:20px">
        <label style="font-size:12px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px">Store (optional)</label>
        <input id="gl-store" class="form-input" type="text" value="${esc(list?.store||'')}" placeholder="e.g. Tesco, Lidl, Amazon…" autocomplete="off">
      </div>
      <div style="display:flex;gap:10px">
        <button onclick="document.getElementById('grocery-list-picker-overlay').remove()" style="flex:1;padding:13px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:16px;font-weight:600;cursor:pointer">Cancel</button>
        <button onclick="_saveGroceryListModal('${id||''}')" style="flex:2;padding:13px;border-radius:10px;border:none;background:var(--accent);color:#111;font-size:16px;font-weight:700;cursor:pointer">${list ? 'Save' : 'Create list'}</button>
      </div>
    </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
  setTimeout(() => document.getElementById('gl-name').focus(), 100);
}

async function _saveGroceryListModal(id) {
  const name  = document.getElementById('gl-name')?.value.trim();
  const store = document.getElementById('gl-store')?.value.trim();
  if (!name) { toast('Enter a list name'); return; }
  document.getElementById('grocery-list-picker-overlay')?.remove();

  if (id) {
    const list = groceryLists.find(l => l.id === id);
    if (list) { list.name = name; list.store = store; list.updatedAt = new Date().toISOString(); }
  } else {
    const newId = 'gl_' + Date.now() + '_' + Math.random().toString(36).slice(2,5);
    groceryLists.push({ id: newId, name, store, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
    activeGroceryListId = newId;
    try { localStorage.setItem('stockroom_active_grocery_list', newId); } catch(e) {}
  }
  await _saveGroceryLists();
  renderGrocery();
}

async function deleteGroceryList(id) {
  if (groceryLists.length <= 1) { toast("Can't delete the last list"); return; }
  if (!confirm('Delete this list? Items in it will also be deleted.')) return;
  await Promise.all(groceryItems.filter(i => (i.listId||'default') === id).map(i => addGroceryTombstone(i.id)));
  groceryItems = groceryItems.filter(i => (i.listId||'default') !== id);
  groceryLists = groceryLists.filter(l => l.id !== id);
  if (activeGroceryListId === id) {
    activeGroceryListId = groceryLists[0]?.id || 'default';
    try { localStorage.setItem('stockroom_active_grocery_list', activeGroceryListId); } catch(e) {}
  }
  await _saveGroceryLists();
  await saveGrocery();
  renderGrocery();
}

// ── Quick List ────────────────────────────────────────────────────────────
function openQuickList() {
  document.getElementById('quick-list-overlay')?.remove();
  const overlay = document.createElement('div');
  overlay.id = 'quick-list-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:600;background:rgba(0,0,0,0.75);display:flex;align-items:flex-end;justify-content:center;backdrop-filter:blur(4px)';
  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:20px 20px 0 0;width:100%;max-width:600px;padding:24px 20px 36px;box-shadow:0 -8px 32px rgba(0,0,0,0.5);max-height:90vh;overflow-y:auto">
      <div style="width:40px;height:4px;background:var(--border);border-radius:2px;margin:0 auto 14px"></div>
      <h3 style="font-size:18px;font-weight:700;margin-bottom:4px;text-align:center"><svg class="icon icon-md" aria-hidden="true" style="vertical-align:-3px;color:var(--accent)"><use href="#i-zap"></use></svg> Quick List</h3>
      <p style="font-size:13px;color:var(--muted);text-align:center;margin-bottom:16px">Type items separated by commas. Tap a suggestion to add it.</p>
      <div style="display:flex;gap:10px;margin-bottom:12px;flex-wrap:wrap">
        <div style="flex:1;min-width:140px">
          <label style="font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px">List name (optional)</label>
          <input id="ql-list-name" class="form-input" type="text" placeholder="e.g. Mid-week top-up" autocomplete="off" style="width:100%;box-sizing:border-box">
        </div>
        <div style="flex:1;min-width:140px">
          <label style="font-size:11px;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px">Store (optional)</label>
          <input id="ql-store-name" class="form-input" type="text" placeholder="e.g. Tesco, Lidl" autocomplete="off" style="width:100%;box-sizing:border-box">
        </div>
      </div>
      <div style="position:relative;margin-bottom:10px">
        <textarea id="quick-list-input" rows="3" placeholder="milk, eggs, bread…"
          style="width:100%;box-sizing:border-box;background:var(--surface2);border:1.5px solid var(--border);border-radius:10px;padding:12px;color:var(--text);font-size:16px;font-family:var(--sans);resize:none;line-height:1.5"
          oninput="_quickListAutocomplete(this.value)" onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();_saveQuickList()}"></textarea>
        <div id="quick-list-suggestions" style="display:none;position:absolute;bottom:100%;left:0;right:0;background:var(--surface);border:1px solid var(--border);border-radius:8px;max-height:180px;overflow-y:auto;margin-bottom:4px;box-shadow:0 -4px 16px rgba(0,0,0,0.3);z-index:10"></div>
      </div>
      <div id="quick-list-preview" style="display:flex;flex-wrap:wrap;gap:6px;min-height:28px;margin-bottom:14px"></div>
      <div style="display:flex;gap:10px">
        <button onclick="document.getElementById('quick-list-overlay').remove()" style="flex:1;padding:13px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:16px;font-weight:600;cursor:pointer">Cancel</button>
        <button onclick="_saveQuickList()" style="flex:2;padding:13px;border-radius:10px;border:none;background:var(--accent);color:#111;font-size:16px;font-weight:700;cursor:pointer">Save list →</button>
      </div>
    </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
  setTimeout(() => document.getElementById('ql-list-name')?.focus(), 100);
}

function _quickListAutocomplete(val) {
  const sugg    = document.getElementById('quick-list-suggestions');
  const preview = document.getElementById('quick-list-preview');
  if (!sugg || !preview) return;

  // Preview pills for all typed entries
  const parts = val.split(',').map(s => s.trim()).filter(Boolean);
  preview.innerHTML = parts.map(p => `<span style="padding:4px 12px;border-radius:99px;background:rgba(232,168,56,0.15);border:1px solid rgba(232,168,56,0.3);font-size:13px;color:var(--accent)">${esc(p)}</span>`).join('');

  // Autocomplete on the last partial token — match ANYWHERE in name
  const lastPart = val.split(',').pop()?.trim() || '';
  if (lastPart.length < 1) { sugg.style.display = 'none'; return; }
  const already = new Set(parts.slice(0, -1).map(p => p.toLowerCase()));
  const matches = groceryItems
    .filter(i => i.name.toLowerCase().includes(lastPart.toLowerCase()) && !already.has(i.name.toLowerCase()))
    .slice(0, 8);
  if (!matches.length) { sugg.style.display = 'none'; return; }
  sugg.style.display = 'block';
  sugg.innerHTML = matches.map(i => {
    // Highlight the matching part
    const lo = i.name.toLowerCase();
    const lp = lastPart.toLowerCase();
    const idx = lo.indexOf(lp);
    const before = esc(i.name.slice(0, idx));
    const match  = `<strong style="color:var(--accent)">${esc(i.name.slice(idx, idx + lastPart.length))}</strong>`;
    const after  = esc(i.name.slice(idx + lastPart.length));
    return `<div onclick="_quickListPickSuggestion('${esc(i.name).replace(/'/g,"\\'")}','${i.department||'other'}')"
      style="padding:10px 14px;cursor:pointer;font-size:14px;border-bottom:1px solid var(--border)"
      onmouseover="this.style.background='var(--surface2)'" onmouseout="this.style.background=''"
      >${before}${match}${after}</div>`;
  }).join('');
}

function _quickListPickSuggestion(name, dept) {
  const inp = document.getElementById('quick-list-input');
  if (!inp) return;
  const parts = inp.value.split(',');
  parts[parts.length - 1] = ' ' + name;
  inp.value = parts.join(',') + ', ';
  document.getElementById('quick-list-suggestions').style.display = 'none';
  _quickListAutocomplete(inp.value);
  inp.focus();
}

async function _saveQuickList() {
  const val = document.getElementById('quick-list-input')?.value || '';
  const names = val.split(',').map(s => s.trim()).filter(Boolean);
  if (!names.length) { toast('Type at least one item'); return; }

  const listNameVal  = document.getElementById('ql-list-name')?.value.trim();
  const storeNameVal = document.getElementById('ql-store-name')?.value.trim();
  const depts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;

  // Create a new list for this quick session
  const newListId  = 'gl_' + Date.now() + '_' + Math.random().toString(36).slice(2,5);
  const newListName = listNameVal || `Quick list ${new Date().toLocaleDateString('en-GB', {day:'numeric',month:'short'})}`;
  groceryLists.push({ id: newListId, name: newListName, store: storeNameVal || '', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });

  // Add items with best-effort department detection
  for (const name of names) {
    const existing = groceryItems.find(i => i.name.toLowerCase() === name.toLowerCase());
    const dept = existing?.department || detectDepartment(name) || depts[0]?.id || 'other';
    const newId = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
    groceryItems.push({ id: newId, name, department: dept, listId: newListId, notes: '', recurring: false, intervalDays: 7, checked: false, addedAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
    appendToGroceryOrder(newId);
  }

  // Switch to new list
  activeGroceryListId = newListId;
  try { localStorage.setItem('stockroom_active_grocery_list', newListId); } catch(e) {}

  document.getElementById('quick-list-overlay')?.remove();
  await _saveGroceryLists();
  await saveGrocery();
  renderGrocery();
  toast(`✓ Created "${newListName}" with ${names.length} item${names.length!==1?'s':''}`);
}

// ── Change store for grocery item ─────────────────────────────────────────
function openChangeGroceryItemStore(itemId) {
  document.getElementById('grocery-store-picker-overlay')?.remove();
  const item = groceryItems.find(i => i.id === itemId);
  const stores = [...new Set(groceryLists.filter(l => l.store).map(l => l.store))];
  const overlay = document.createElement('div');
  overlay.id = 'grocery-store-picker-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:600;background:rgba(0,0,0,0.7);display:flex;align-items:flex-end;justify-content:center;backdrop-filter:blur(4px)';
  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:20px 20px 0 0;width:100%;max-width:560px;padding:24px 20px 36px;box-shadow:0 -8px 32px rgba(0,0,0,0.5)">
      <div style="width:40px;height:4px;background:var(--border);border-radius:2px;margin:0 auto 18px"></div>
      <h3 style="font-size:17px;font-weight:700;margin-bottom:4px;text-align:center">Move to list</h3>
      <p style="font-size:13px;color:var(--muted);text-align:center;margin-bottom:14px">Choose which list to move <strong>${esc(item?.name||'this item')}</strong> to</p>
      <div style="display:flex;flex-direction:column;gap:8px;max-height:50vh;overflow-y:auto;margin-bottom:16px">
        ${groceryLists.map(l => `
          <button onclick="_moveItemToList('${itemId}','${l.id}')"
            style="display:flex;align-items:center;gap:12px;padding:12px 14px;background:${(item?.listId||'default')===l.id?'rgba(232,168,56,0.12)':'var(--surface2)'};border:2px solid ${(item?.listId||'default')===l.id?'rgba(232,168,56,0.5)':'var(--border)'};border-radius:10px;cursor:pointer;text-align:left;width:100%">
            <span style="font-size:16px;font-weight:600;color:var(--text);flex:1">${esc(l.name)}</span>
            ${l.store ? `<span style="font-size:12px;color:var(--muted);display:inline-flex;align-items:center;gap:4px"><svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg> ${esc(l.store)}</span>` : ''}
            ${(item?.listId||'default')===l.id ? '<span style="color:var(--accent);font-size:18px">✓</span>' : ''}
          </button>`).join('')}
      </div>
      <button onclick="document.getElementById('grocery-store-picker-overlay').remove()" style="width:100%;padding:13px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:16px;font-weight:600;cursor:pointer">Cancel</button>
    </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

async function _moveItemToList(itemId, listId) {
  document.getElementById('grocery-store-picker-overlay')?.remove();
  const item = groceryItems.find(i => i.id === itemId);
  if (item) { item.listId = listId; item.updatedAt = new Date().toISOString(); }
  const list = groceryLists.find(l => l.id === listId);
  if (list) { list.updatedAt = new Date().toISOString(); await _saveGroceryLists(); }
  await saveGrocery();
  renderGrocery();
}

async function _moveSelectedToList(listId) {
  document.getElementById('grocery-store-picker-overlay')?.remove();
  const ids = [...grocerySelected];
  ids.forEach(id => {
    const item = groceryItems.find(i => i.id === id);
    if (item) { item.listId = listId; item.updatedAt = new Date().toISOString(); }
  });
  const list = groceryLists.find(l => l.id === listId);
  if (list) { list.updatedAt = new Date().toISOString(); await _saveGroceryLists(); }
  grocerySelected.clear();
  await saveGrocery();
  renderGrocery();
}

function openChangeSelectedStore() {
  if (grocerySelected.size === 0) return;
  document.getElementById('grocery-store-picker-overlay')?.remove();
  const overlay = document.createElement('div');
  overlay.id = 'grocery-store-picker-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:600;background:rgba(0,0,0,0.7);display:flex;align-items:flex-end;justify-content:center;backdrop-filter:blur(4px)';
  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:20px 20px 0 0;width:100%;max-width:560px;padding:24px 20px 36px;box-shadow:0 -8px 32px rgba(0,0,0,0.5)">
      <div style="width:40px;height:4px;background:var(--border);border-radius:2px;margin:0 auto 18px"></div>
      <h3 style="font-size:17px;font-weight:700;margin-bottom:14px;text-align:center">Move ${grocerySelected.size} item${grocerySelected.size!==1?'s':''} to list</h3>
      <div style="display:flex;flex-direction:column;gap:8px;max-height:50vh;overflow-y:auto;margin-bottom:16px">
        ${groceryLists.map(l => `
          <button onclick="_moveSelectedToList('${l.id}')"
            style="display:flex;align-items:center;gap:12px;padding:12px 14px;background:var(--surface2);border:2px solid var(--border);border-radius:10px;cursor:pointer;text-align:left;width:100%">
            <span style="font-size:16px;font-weight:600;color:var(--text);flex:1">${esc(l.name)}</span>
            ${l.store ? `<span style="font-size:12px;color:var(--muted);display:inline-flex;align-items:center;gap:4px"><svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg> ${esc(l.store)}</span>` : ''}
          </button>`).join('')}
      </div>
      <button onclick="document.getElementById('grocery-store-picker-overlay').remove()" style="width:100%;padding:13px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:16px;font-weight:600;cursor:pointer">Cancel</button>
    </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

// Save groceries to IDB and profile only — no sync trigger.
// Used internally (e.g. from within a sync) to avoid re-entrant sync loops.
async function _saveGroceryLocal() {
  // Strip blank entries that are NOT currently being edited inline
  // Items with _isNew flag are mid-creation and must not be stripped
  groceryItems = groceryItems.filter(i => i._isNew || (i.name && i.name.trim().length > 0));
  await dbPut('groceries', 'items', groceryItems);
  await _saveGroceryLists();
  if (activeProfile) await saveCurrentProfile();
}

async function saveGrocery() {
  await _saveGroceryLocal();
  _syncQueue.enqueue();
  bcPost({ type: 'GROCERY_CHANGED' });
}

async function saveGroceryDepts() {
  await dbPut('departments', 'departments', groceryDepts);
  if (activeProfile) await saveCurrentProfile();
}

function getGroceryShopInterval() {
  try { return JSON.parse(localStorage.getItem('stockroom_grocery_interval') || '{"value":7,"unit":1}'); } catch(e) { return {value:7,unit:1}; }
}

function saveGroceryShopInterval() {
  const v = parseInt(document.getElementById('grocery-shop-interval').value) || 7;
  const u = parseInt(document.getElementById('grocery-shop-interval-unit').value) || 1;
  try { localStorage.setItem('stockroom_grocery_interval', JSON.stringify({value:v,unit:u})); } catch(e) {}
  showToast('Shop interval saved');
}

function setGrocerySort(mode, btn) {
  grocerySort = mode;
  document.querySelectorAll('.grocery-sort-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderGrocery();
}

// ═══════════════════════════════════════════════════════════
//  GROCERY IMPORT — from .txt / .csv
// ═══════════════════════════════════════════════════════════

// Keyword map for auto-department detection
const DEPT_KEYWORDS = {
  'bakery':     ['bread','loaf','roll','baguette','bagel','pitta','crumpet','muffin','croissant','brioche','sourdough','toast','wrap','tortilla'],
  'fruit-veg':  ['apple','banana','orange','grape','strawberry','berry','lemon','lime','mango','pear','peach','plum','melon','pineapple','kiwi','avocado','tomato','potato','carrot','onion','garlic','spinach','lettuce','cucumber','pepper','courgette','broccoli','cauliflower','celery','mushroom','leek','asparagus','kale','cabbage','salad','herb','coriander','parsley','basil','mint','ginger','sweetcorn','corn','pea','bean','lentil'],
  'meat-fish':  ['chicken','beef','pork','lamb','turkey','salmon','tuna','cod','haddock','prawn','shrimp','sausage','bacon','mince','steak','fillet','ham','salami','pepperoni','chorizo','duck','venison','mackerel','sea bass','crab','lobster','fish'],
  'dairy':      ['milk','cheese','butter','yogurt','yoghurt','cream','cheddar','mozzarella','brie','camembert','feta','eggs','egg','margarine','flora','crème fraîche','soured cream','quark','ricotta'],
  'frozen':     ['frozen','ice cream','ice lolly','pizza','chips','waffles','nuggets','burger','oven ready'],
  'drinks':     ['juice','water','coffee','tea','wine','beer','lager','cider','spirit','gin','vodka','rum','whisky','cola','pepsi','coke','fanta','sprite','squash','cordial','smoothie','energy drink','oat milk','almond milk','soy milk'],
  'snacks':     ['crisp','chip','nuts','biscuit','cookie','chocolate','sweet','candy','popcorn','pretzel','cracker','rice cake','granola bar','cereal bar','flapjack'],
  'cupboard':   ['pasta','rice','flour','sugar','oil','vinegar','sauce','ketchup','mustard','mayo','mayonnaise','salt','pepper','spice','herb','stock','gravy','tin','can','soup','beans','chickpea','lentils','noodle','cereal','oat','porridge','jam','honey','syrup','spread','peanut butter','coffee','tea','cocoa','breadcrumb','stuffing'],
  'household':  ['washing','detergent','cleaner','bleach','sponge','cloth','toilet','kitchen roll','tissue','bin bag','clingfilm','foil','parchment','zip bag','mop','brush','nappy','baby wipe'],
  'toiletries': ['soap','shampoo','conditioner','deodorant','toothbrush','toothpaste','razor','shower gel','body wash','moisturiser','lotion','sunscreen','face wash','cotton','floss','mouthwash','lip balm','perfume','aftershave','nail','pad','tampon','sanitary'],
  'baby-care':  ['nappy','baby formula','baby food','dummy','baby bottle','baby wipe','baby lotion'],
};

function detectDepartment(name) {
  const lower = name.toLowerCase();
  for (const [deptId, keywords] of Object.entries(DEPT_KEYWORDS)) {
    if (keywords.some(kw => lower.includes(kw))) return deptId;
  }
  return 'other';
}

function openGroceryImport() {
  openModal('grocery-import-info-modal');
}

async function pasteGroceryImport() {
  closeModal('grocery-import-info-modal');
  try {
    const text = await navigator.clipboard.readText();
    if (!text || !text.trim()) { toast('Clipboard is empty'); return; }
    const raw = text.split(/[\n\r,;]+/)
      .map(s => s.replace(/["']/g, '').trim())
      .filter(s => s.length > 1 && s.length < 200);
    const unique = [...new Set(raw)];
    if (!unique.length) { toast('No items found in clipboard text'); return; }
    buildGroceryImportReview(unique);
  } catch(e) {
    toast('Cannot read clipboard — try choosing a file instead');
  }
}

function handleGroceryImportFile(e) {
  const file = e.target.files[0];
  if (!file) return;
  e.target.value = '';
  const ext = file.name.split('.').pop().toLowerCase();
  const reader = new FileReader();

  reader.onerror = () => toast('Could not read file');

  reader.onload = ev => {
    let text = '';
    const result = ev.target.result;

    if (ext === 'xml') {
      // Parse XML and extract all text nodes
      try {
        const parser = new DOMParser();
        const doc    = parser.parseFromString(result, 'text/xml');
        const walker = document.createTreeWalker(doc, NodeFilter.SHOW_TEXT);
        const parts  = [];
        let node;
        while ((node = walker.nextNode())) {
          const t = node.textContent.trim();
          if (t) parts.push(t);
        }
        text = parts.join('\n');
      } catch(e) { text = result; }

    } else if (ext === 'rtf') {
      // Strip RTF control words, keep readable text
      text = result
        .replace(/\\[a-z]+\d*\s?/g, ' ')
        .replace(/[{}\\]/g, ' ')
        .replace(/\s+/g, '\n');

    } else if (ext === 'md') {
      // Extract bullet list items and headings stripped
      text = result
        .split('\n')
        .map(l => l.replace(/^[\s]*[-*+]\s+/, '').replace(/^#{1,6}\s+/, '').trim())
        .filter(Boolean)
        .join('\n');

    } else if (ext === 'docx' || ext === 'doc') {
      // docx is a zip — extract readable text via raw read
      // For binary formats, convert result bytes to string and pull any readable sequences
      const bytes = new Uint8Array(result);
      const chunks = [];
      let current = '';
      for (let i = 0; i < bytes.length; i++) {
        const b = bytes[i];
        if (b >= 32 && b < 127) {
          current += String.fromCharCode(b);
        } else if (current.length >= 3) {
          chunks.push(current.trim());
          current = '';
        } else {
          current = '';
        }
      }
      if (current.length >= 3) chunks.push(current.trim());
      // Filter out XML/binary noise, keep human-readable word-like chunks
      text = chunks
        .filter(s => /^[A-Za-z]/.test(s) && !/^(xml|http|rId|docx|rels|word|type|xmlns|Content|Relation|Target|schema|xmlns)/i.test(s))
        .join('\n');

    } else {
      // Plain text: txt, csv, tsv, text, fallback
      text = typeof result === 'string' ? result : new TextDecoder().decode(result);
    }

    // Split on newlines, tabs, or commas
    const sep = ext === 'tsv' ? /[\t\n\r]+/ : /[\n\r,;]+/;
    const raw = text.split(sep)
      .map(s => s.replace(/["']/g, '').trim())   // strip quotes
      .filter(s => s.length > 1 && s.length < 200 && !/^[<>\[\]{}&|\\\/]/.test(s));

    const unique = [...new Set(raw)]; // deduplicate
    if (!unique.length) { toast('No items found in file — try a plain text file with one item per line'); return; }
    buildGroceryImportReview(unique);
  };

  // docx/doc need binary read
  if (ext === 'docx' || ext === 'doc') {
    reader.readAsArrayBuffer(file);
  } else {
    reader.readAsText(file);
  }
}

// Holds pending import items during review
let _groceryImportDraft = [];

function buildGroceryImportReview(names) {
  _groceryImportDraft = names.map(name => ({
    name,
    department: detectDepartment(name),
    notes: '',
  }));

  renderGroceryImportList();
  const btn = document.getElementById('grocery-import-confirm-btn');
  if (btn) btn.textContent = `✓ Add ${_groceryImportDraft.length} item${_groceryImportDraft.length !== 1 ? 's' : ''}`;
  openModal('grocery-import-modal');
}

function renderGroceryImportList() {
  const container = document.getElementById('grocery-import-list');
  if (!container) return;

  const depts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;

  container.innerHTML = _groceryImportDraft.map((item, i) => {
    const deptInfo = depts.find(d => d.id === item.department) || { emoji: '📦', name: 'Other' };
    const deptOptions = depts.map(d =>
      `<option value="${esc(d.id)}" ${d.id === item.department ? 'selected' : ''}>${d.emoji} ${esc(d.name)}</option>`
    ).join('');

    return `<div style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:10px 12px;display:flex;flex-direction:column;gap:6px" id="gi-row-${i}">
      <div style="display:flex;gap:8px;align-items:center">
        <input type="text" value="${esc(item.name)}"
          style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:6px 8px;color:var(--text);font-size:13px;font-weight:600"
          oninput="_groceryImportDraft[${i}].name=this.value"
          placeholder="Item name">
        <button onclick="removeGroceryImportRow(${i})"
          style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:18px;padding:2px 4px;flex-shrink:0"
          title="Remove this item"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
      </div>
      <div style="display:flex;gap:8px;align-items:center">
        <select style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px"
          onchange="_groceryImportDraft[${i}].department=this.value">
          ${deptOptions}
        </select>
        <input type="text" value="${esc(item.notes)}"
          style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--muted);font-size:12px"
          placeholder="Note (optional)"
          oninput="_groceryImportDraft[${i}].notes=this.value">
      </div>
    </div>`;
  }).join('');
}

function removeGroceryImportRow(i) {
  _groceryImportDraft.splice(i, 1);
  renderGroceryImportList();
  const btn = document.getElementById('grocery-import-confirm-btn');
  if (btn) btn.textContent = `✓ Add ${_groceryImportDraft.length} item${_groceryImportDraft.length !== 1 ? 's' : ''}`;
  if (_groceryImportDraft.length === 0) closeModal('grocery-import-modal');
}

async function confirmGroceryImport() {
  const valid = _groceryImportDraft.filter(item => item.name.trim().length > 0);
  if (!valid.length) { toast('No items to add'); return; }

  const now = new Date().toISOString();
  valid.forEach(item => {
    // Skip duplicates (same name, case-insensitive)
    const exists = groceryItems.some(g => g.name.toLowerCase() === item.name.trim().toLowerCase());
    if (!exists) {
      const newId = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
      groceryItems.push({
        id:         newId,
        name:       item.name.trim(),
        department: item.department || 'other',
        notes:      item.notes.trim(),
        recurring:  false,
        intervalDays: 0,
        checked:    false,
        addedAt:    now,
        updatedAt:  now,
      });
      appendToGroceryOrder(newId);
    }
  });

  closeModal('grocery-import-modal');
  _groceryImportDraft = [];
  await saveGrocery();
  renderGrocery();
  toast(`${valid.length} item${valid.length !== 1 ? 's' : ''} added to grocery list ✓`);
}

function renderGrocery() {
  const query = (document.getElementById('grocery-search')?.value || '').toLowerCase().trim();
  const body  = document.getElementById('grocery-list-body');
  const infoEl = document.getElementById('grocery-interval-info');
  const checkedBar = document.getElementById('grocery-checked-bar');
  if (!body) return;

  // ── Multi-list picker: show list selector when 2+ lists and no active list chosen ──
  const multiList = groceryLists.length > 1;
  const listBrowsing = multiList && !activeGroceryListId;

  // Update back-to-lists button visibility
  let backBtn = document.getElementById('grocery-back-to-lists');
  if (multiList) {
    if (!backBtn) {
      backBtn = document.createElement('button');
      backBtn.id = 'grocery-back-to-lists';
      backBtn.className = 'btn btn-ghost btn-sm';
      backBtn.textContent = '← All lists';
      backBtn.onclick = () => { groceryEditMode = false; activeGroceryListId = ''; renderGrocery(); };
      backBtn.style.cssText = 'margin-bottom:12px;display:block';
      body.parentNode.insertBefore(backBtn, body);
    }
    backBtn.style.display = activeGroceryListId ? 'inline-flex' : 'none';
  } else if (backBtn) {
    backBtn.remove();
  }

  // Show list picker if no active list or browsing mode
  if (multiList && !activeGroceryListId) {
    document.body.classList.add('grocery-multilist');
    // Hide single-list-only controls
    const editToggle = document.getElementById('grocery-edit-toggle');
    const addItem    = document.querySelector('#view-grocery .btn-primary[onclick*="openAddGroceryItem"]');
    if (editToggle) editToggle.style.display = 'none';
    if (addItem)    addItem.style.display    = 'none';
    renderGroceryListPicker();
    const sub = document.getElementById('grocery-subtitle');
    if (sub) sub.textContent = `${groceryLists.length} lists · tap to open`;
    if (infoEl) infoEl.textContent = '';
    return;
  }
  // Single list mode — restore controls
  document.body.classList.remove('grocery-multilist');
  const _editToggle = document.getElementById('grocery-edit-toggle');
  const _addItem    = document.querySelector('#view-grocery .btn-primary[onclick*="openAddGroceryItem"]');
  if (_editToggle) _editToggle.style.display = '';
  if (_addItem)    _addItem.style.display    = '';

  cleanGroceryOrder();
  // Hide multi-select bar whenever we re-render
  if (!groceryEditMode) {
    const sb = document.getElementById('grocery-selection-bar');
    if (sb) sb.style.display = 'none';
  }

  // Filter to active list only
  const listItems = groceryItems.filter(i => (i.listId || 'default') === activeGroceryListId);

  // Interval info — scoped to active list
  const si = getGroceryShopInterval();
  const unitLabel = si.unit === 7 ? (si.value === 1 ? 'week' : 'weeks') : si.unit === 30 ? (si.value === 1 ? 'month' : 'months') : (si.value === 1 ? 'day' : 'days');
  const activeListName = _activeGroceryList()?.name || '';
  if (infoEl) infoEl.textContent = `Shopping every ${si.value} ${unitLabel} · ${listItems.filter(i=>!i.checked).length} item${listItems.filter(i=>!i.checked).length===1?'':'s'} remaining`;

  const sub = document.getElementById('grocery-subtitle');
  if (sub) sub.innerHTML = `${listItems.length} item${listItems.length===1?'':'s'} · ${groceryEditMode ? '<svg class="icon icon-sm" aria-hidden="true"><use href="#i-pencil"></use></svg> editing' : 'tap to check off'}`;

  // Edit mode lock button
  const editBtn = document.getElementById('grocery-edit-toggle');
  if (editBtn) {
    editBtn.innerHTML = groceryEditMode
      ? '<svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg> Done'
      : '<svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg> Edit';
    editBtn.style.background = groceryEditMode ? 'rgba(232,168,56,0.15)' : '';
    editBtn.style.color = groceryEditMode ? 'var(--accent)' : '';
    editBtn.style.borderColor = groceryEditMode ? 'rgba(232,168,56,0.4)' : '';
  }

  let filtered = listItems.filter(i => !query || i.name.toLowerCase().includes(query) || (i.notes||'').toLowerCase().includes(query));
  const unchecked = filtered.filter(i => !i.checked);
  // If hideChecked: hide checked items from main render (they can still be cleared)
  const checked   = groceryHideChecked ? [] : filtered.filter(i => i.checked);

  const checkedCount = groceryItems.filter(i => i.checked).length;
  if (checkedBar) {
    const actualCheckedCount = listItems.filter(i => i.checked).length;
    checkedBar.style.display = !groceryEditMode && actualCheckedCount > 0 && activeGroceryListId ? 'flex' : 'none';
    const cc = document.getElementById('grocery-checked-count');
    if (cc) cc.textContent = `${actualCheckedCount} item${actualCheckedCount===1?'':'s'} checked`;
  }
  // Update hide-checked button state
  const hcBtn = document.getElementById('grocery-hide-checked-btn');
  if (hcBtn) {
    const hasChecked = listItems.some(i => i.checked);
    hcBtn.style.display = hasChecked && !groceryEditMode && activeGroceryListId ? 'inline-flex' : 'none';
    hcBtn.innerHTML = groceryHideChecked ? `<svg class="icon" aria-hidden="true"><use href="#i-eye"></use></svg> Show ticked` : `<svg class="icon" aria-hidden="true"><use href="#i-eye-off"></use></svg> Hide ticked`;
    hcBtn.style.color = groceryHideChecked ? 'var(--accent)' : '';
    hcBtn.style.borderColor = groceryHideChecked ? 'rgba(232,168,56,0.4)' : '';
    hcBtn.style.background = groceryHideChecked ? 'rgba(232,168,56,0.08)' : '';
  }

  if (listItems.length === 0) {
    body.innerHTML = `<div class="grocery-empty"><div class="grocery-empty-icon" style="color:var(--accent)"><svg aria-hidden="true" style="width:48px;height:48px"><use href="#i-shopping-cart"></use></svg></div><div style="font-size:16px;font-weight:700;margin-bottom:8px">No items in this list yet</div><div style="font-size:13px;color:var(--muted)">Tap <svg class="icon icon-sm" aria-hidden="true" style="vertical-align:middle"><use href="#i-zap"></use></svg> Quick List or <svg class="icon icon-sm" aria-hidden="true" style="vertical-align:middle"><use href="#i-pencil"></use></svg> Edit to add items.</div></div>`;
    return;
  }

  if (filtered.length === 0 && !groceryEditMode) {
    body.innerHTML = `<div class="grocery-empty"><div class="grocery-empty-icon" style="color:var(--muted)"><svg aria-hidden="true" style="width:48px;height:48px"><use href="#i-search"></use></svg></div><div style="font-size:15px;font-weight:600">No matches</div></div>`;
    return;
  }

  // In edit mode: show items in same structure as locked, just with drag handle + delete + inline note
  if (groceryEditMode) {
    const depts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;
    const isDeptView = grocerySort === 'dept';
    const canDrag    = isDeptView; // drag only in dept view

    let editHtml = `<div id="grocery-drag-hint" style="font-size:12px;color:var(--muted);padding:6px 0 10px;text-align:center;position:sticky;top:0;z-index:10;background:var(--bg);margin:0 -2px;border-bottom:1px solid rgba(46,51,80,0.3);margin-bottom:8px">
      ${canDrag ? '<svg class="icon" aria-hidden="true"><use href="#i-grip-vertical"></use></svg> Hold and drag to reorder within departments' : 'A–Z view — switch to By Dept to reorder'}
    </div>`;

    const ordered = getGroceryItemsInOrder().filter(i => !i.checked && (!query || i.name.toLowerCase().includes(query) || (i.notes||'').toLowerCase().includes(query)));

    if (isDeptView) {
      // Group by department, same structure as locked view
      const deptMap = {};
      ordered.forEach(item => {
        const d = item.department || 'other';
        if (!deptMap[d]) deptMap[d] = [];
        deptMap[d].push(item);
      });
      const deptOrder = depts.map(d => d.id);
      const extraDepts = [...new Set(ordered.map(i => i.department || 'other'))].filter(d => !deptOrder.includes(d));
      [...deptOrder, ...extraDepts].forEach(deptId => {
        const deptItems = deptMap[deptId];
        if (!deptItems || !deptItems.length) return;
        const deptDef = depts.find(d => d.id === deptId) || {name: deptId, emoji:'📦'};
        editHtml += `<div class="grocery-dept-group">
          <div class="grocery-dept-header">
            <span class="grocery-dept-label">${deptDef.emoji} ${esc(deptDef.name)}</span>
            <span style="display:flex;align-items:center;gap:8px">
              <span class="grocery-dept-count">${deptItems.length}</span>
              <button onclick="event.stopPropagation();addGroceryItemToDept('${deptId}')"
                style="background:none;border:none;cursor:pointer;color:var(--accent);font-size:22px;line-height:1;padding:0 2px;font-weight:300;transition:opacity 0.15s"
                title="Add item to ${deptDef.name}">＋</button>
            </span>
          </div>
          <div class="grocery-edit-dept-group" data-dept="${deptId}">
            ${deptItems.map(item => groceryItemEditHTML(item, depts, canDrag)).join('')}
          </div>
        </div>`;
      });
    } else {
      // A-Z: sorted, no drag
      const sorted = [...ordered].sort((a,b) => a.name.localeCompare(b.name));
      editHtml += `<div class="grocery-edit-dept-group" data-dept="all">
        ${sorted.map(item => groceryItemEditHTML(item, depts, false)).join('')}
      </div>`;
    }

    body.innerHTML = editHtml;
    if (canDrag) initGroceryDragSort();
    return;
  }

  let html = '';

  if (grocerySort === 'dept') {
    // Use manual order within each department group
    const ordered = getGroceryItemsInOrder();
    const filteredOrdered = ordered.filter(i => !query || i.name.toLowerCase().includes(query) || (i.notes||'').toLowerCase().includes(query));

    // Build dept map including both checked and unchecked
    const deptMap = {};
    filteredOrdered.forEach(item => {
      const dept = item.department || 'other';
      if (!deptMap[dept]) deptMap[dept] = { unchecked: [], checked: [] };
      if (item.checked) deptMap[dept].checked.push(item);
      else deptMap[dept].unchecked.push(item);
    });

    const deptOrder = (groceryDepts.length ? groceryDepts : DEFAULT_DEPTS).map(d => d.id);
    const extraDepts = [...new Set(filteredOrdered.map(i => i.department || 'other'))].filter(d => !deptOrder.includes(d));
    const activeDepts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;

    [...deptOrder, ...extraDepts].forEach(deptId => {
      const bucket = deptMap[deptId];
      if (!bucket || (bucket.unchecked.length === 0 && bucket.checked.length === 0)) return;
      const deptDef = activeDepts.find(d => d.id === deptId) || {name: deptId, emoji:'📦'};
      const allChecked = bucket.unchecked.length === 0 && bucket.checked.length > 0;
      const totalCount = bucket.unchecked.length + bucket.checked.length;

      // Fully-checked dept: show collapsed with toggle
      if (allChecked) {
        html += `<div class="grocery-dept-group">
          <div class="grocery-dept-header" onclick="_toggleDeptCollapse('${deptId}')" style="cursor:pointer">
            <span class="grocery-dept-label" style="color:var(--muted);text-decoration:line-through">${deptDef.emoji} ${esc(deptDef.name)}</span>
            <span style="display:flex;align-items:center;gap:6px">
              <span class="grocery-dept-count">✓ all ${totalCount}</span>
              <span id="dept-chevron-${deptId}" style="color:var(--muted);font-size:11px;transition:transform 0.2s">▼</span>
            </span>
          </div>
          <div id="dept-body-${deptId}" style="display:none">
            ${bucket.checked.map(item => groceryItemHTML(item)).join('')}
          </div>
        </div>`;
        return;
      }

      // Mixed dept: unchecked first, then checked at bottom of this dept
      html += `<div class="grocery-dept-group">
        <div class="grocery-dept-header">
          <span class="grocery-dept-label">${deptDef.emoji} ${esc(deptDef.name)}</span>
          <span style="display:flex;align-items:center;gap:8px">
            <span class="grocery-dept-count">${bucket.unchecked.length}${bucket.checked.length ? ` · ${bucket.checked.length} done` : ''}</span>
            <button onclick="event.stopPropagation();addGroceryItemToDept('${deptId}')"
              style="background:none;border:none;cursor:pointer;color:var(--accent);font-size:22px;line-height:1;padding:0 2px;font-weight:300;transition:opacity 0.15s"
              title="Add item to ${deptDef.name}">＋</button>
          </span>
        </div>
        ${bucket.unchecked.map(item => groceryItemHTML(item)).join('')}
        ${bucket.checked.length ? `<div style="border-top:1px dashed rgba(46,51,80,0.4);margin-top:2px">${bucket.checked.map(item => groceryItemHTML(item)).join('')}</div>` : ''}
      </div>`;
    });
  } else {
    // Alpha view — sort alphabetically but preserve manual sub-order within same letter
    const ordered = getGroceryItemsInOrder().filter(i => !i.checked && (!query || i.name.toLowerCase().includes(query) || (i.notes||'').toLowerCase().includes(query)));
    const sorted  = [...ordered].sort((a,b) => a.name.localeCompare(b.name));
    html += sorted.map(item => groceryItemHTML(item)).join('');
  }

  // In alpha view: checked items at bottom (in dept view they're per-dept above)
  if (grocerySort === 'alpha' && checked.length > 0) {
    html += `<div style="margin-top:16px">
      <div class="grocery-dept-header">
        <span class="grocery-dept-label" style="color:var(--muted)">✓ Checked</span>
        <span class="grocery-dept-count">${checked.length}</span>
      </div>
      ${checked.map(item => groceryItemHTML(item)).join('')}
    </div>`;
  }

  body.innerHTML = html;
}

function _toggleDeptCollapse(deptId) {
  const body    = document.getElementById(`dept-body-${deptId}`);
  const chevron = document.getElementById(`dept-chevron-${deptId}`);
  if (!body) return;
  const isOpen = body.style.display !== 'none';
  body.style.display = isOpen ? 'none' : '';
  if (chevron) chevron.style.transform = isOpen ? '' : 'rotate(180deg)';
}

// Edit mode item — same visual design as locked view, just with drag handle + delete + inline edit
function groceryItemEditHTML(item, depts, canDrag) {
  const deptDef  = depts.find(d => d.id === (item.department||'other')) || {name:'Other', emoji:'📦'};
  const metaLine = grocerySort === 'alpha' ? `${deptDef.emoji} ${deptDef.name}` : '';
  const isSelected = grocerySelected.has(item.id);
  const isNew = item._isNew; // blank row being named inline
  const dragHandle = canDrag
    ? `<span class="grocery-drag-handle" title="Hold to drag and reorder" style="color:var(--muted);font-size:16px;cursor:grab;flex-shrink:0;touch-action:none;user-select:none;padding:0 4px">☰</span>`
    : `<span style="width:24px;flex-shrink:0"></span>`;
  return `<div class="grocery-item grocery-edit-row${isSelected?' edit-selected':''}" data-id="${item.id}" data-dept="${item.department||'other'}" ${canDrag ? 'draggable="true"' : ''}>
    ${dragHandle}
    <input type="checkbox" class="grocery-cb grocery-select-cb" ${isSelected?'checked':''}
      onchange="toggleGrocerySelect('${item.id}',this)"
      onclick="event.stopPropagation()"
      title="Select item">
    <div class="grocery-item-info">
      <input type="text" value="${esc(item.name)}"
        class="grocery-item-name"
        id="gi-name-${item.id}"
        style="background:transparent;border:none;border-bottom:1px solid rgba(46,51,80,0.5);width:100%;color:var(--text);padding:0 0 2px;outline:none;font-weight:600;font-size:inherit"
        placeholder="${isNew ? 'Item name…' : ''}"
        onchange="updateGroceryItemInline('${item.id}','name',this.value)"
        ${isNew ? `onblur="_groceryNewItemBlur('${item.id}')"` : ''}>
      <div style="display:flex;gap:6px;margin-top:3px;align-items:center">
        ${metaLine ? `<span class="grocery-item-meta" style="flex-shrink:0;font-size:inherit">${esc(metaLine)}</span>` : ''}
        <input type="text" value="${esc(item.notes||'')}" placeholder="Add note…"
          class="grocery-item-meta"
          style="background:transparent;border:none;border-bottom:1px dashed rgba(46,51,80,0.35);color:var(--muted);padding:0;flex:1;min-width:40px;outline:none;font-size:inherit"
          onchange="updateGroceryItemInline('${item.id}','notes',this.value)">
      </div>
    </div>
  </div>`;
}

async function updateGroceryItemInline(id, field, value) {
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;
  item[field] = value;
  item.updatedAt = new Date().toISOString();
  // Once the user has set a name, clear the _isNew flag
  if (field === 'name') delete item._isNew;
  await saveGrocery();
}

// Called when a new blank item's name field loses focus — remove if still empty
async function _groceryNewItemBlur(id) {
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;
  if (item._isNew && !item.name.trim()) {
    // Never named — remove it silently, no sync needed
    groceryItems = groceryItems.filter(i => i.id !== id);
    const order = getGroceryManualOrder().filter(oid => oid !== id);
    saveGroceryManualOrder(order);
    await _saveGroceryLocal(); // local only — don't push a blank item delete
    renderGrocery();
  } else if (item._isNew && item.name.trim()) {
    // Named successfully — clear _isNew flag and do a real save+sync
    delete item._isNew;
    item.updatedAt = new Date().toISOString();
    await saveGrocery(); // now safe to sync — item has a name
    renderGrocery();
  }
}

async function deleteGroceryItem(id) {
  if (!confirm('Remove this item from your grocery list?')) return;
  groceryItems = groceryItems.filter(i => i.id !== id);
  grocerySelected.delete(id);
  await addGroceryTombstone(id);
  await saveGrocery();
  renderGrocery();
  _updateGrocerySelectionBar();
}

// ── Multi-select (edit mode) ─────────────────────────────────────────

function toggleGrocerySelect(id, cb) {
  if (cb.checked) grocerySelected.add(id);
  else            grocerySelected.delete(id);
  // Highlight the row
  const row = document.querySelector(`.grocery-edit-row[data-id="${id}"]`);
  if (row) row.classList.toggle('edit-selected', cb.checked);
  _updateGrocerySelectionBar();
}

function _updateGrocerySelectionBar() {
  let bar = document.getElementById('grocery-selection-bar');
  const count = grocerySelected.size;

  if (!groceryEditMode || count === 0) {
    if (bar) bar.style.display = 'none';
    return;
  }

  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'grocery-selection-bar';
    bar.style.cssText = [
      'position:fixed;bottom:0;left:0;right:0;z-index:400',
      'background:var(--surface)',
      'border-top:2px solid var(--border)',
      'padding:12px 16px 28px',
      'display:flex;gap:10px;align-items:center',
      'backdrop-filter:blur(8px)',
      'box-shadow:0 -4px 20px rgba(0,0,0,0.3)',
    ].join(';');
    document.body.appendChild(bar);
  }

  bar.style.display = 'flex';
  bar.innerHTML = `
    <span style="font-size:13px;color:var(--muted);flex-shrink:0">${count} selected</span>
    <div style="flex:1"></div>
    ${groceryLists.length > 1 ? `<button onclick="openChangeSelectedStore()"
      style="padding:10px 16px;border-radius:8px;border:1px solid rgba(91,141,238,0.4);background:rgba(91,141,238,0.07);color:#5b8dee;font-size:14px;font-weight:600;cursor:pointer">
      <svg class="icon" aria-hidden="true"><use href="#i-store"></use></svg> Move list
    </button>` : ''}
    <button onclick="_changeSelectedDept()"
      style="padding:10px 16px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:14px;font-weight:600;cursor:pointer">
      📂 Change Dept
    </button>
    <button onclick="_deleteSelected()"
      style="padding:10px 16px;border-radius:8px;border:none;background:rgba(232,80,80,0.15);color:var(--danger);font-size:14px;font-weight:600;cursor:pointer">
      🗑️ Delete
    </button>`;
}

function _changeSelectedDept() {
  if (grocerySelected.size === 0) return;
  // Show dept picker — we use a special sentinel item id 'multi' to signal multi-mode
  _showDeptPickerForSelection();
}

function _showDeptPickerForSelection() {
  document.getElementById('grocery-dept-picker-overlay')?.remove();
  const depts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;
  const count = grocerySelected.size;

  const overlay = document.createElement('div');
  overlay.id = 'grocery-dept-picker-overlay';
  overlay.style.cssText = [
    'position:fixed;inset:0;z-index:600',
    'background:rgba(0,0,0,0.7)',
    'display:flex;align-items:flex-end;justify-content:center',
    'backdrop-filter:blur(4px)',
  ].join(';');
  overlay.dataset.mode     = 'multi';
  overlay.dataset.selected = '';

  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:20px 20px 0 0;width:100%;max-height:75vh;display:flex;flex-direction:column;padding:20px 16px 36px;box-shadow:0 -8px 32px rgba(0,0,0,0.5)">
      <div style="width:40px;height:4px;background:var(--border);border-radius:2px;margin:0 auto 18px"></div>
      <h3 style="font-size:17px;font-weight:700;margin-bottom:4px;text-align:center">Change Department</h3>
      <p style="font-size:13px;color:var(--muted);text-align:center;margin-bottom:4px">Move ${count} item${count!==1?'s':''} to…</p>
      <div style="text-align:center;margin-bottom:14px">
        <button onclick="_cancelDeptPicker();openGroceryDepts()" style="background:none;border:none;color:var(--accent);font-size:13px;cursor:pointer;text-decoration:underline;padding:0">✏️ Edit Departments</button>
      </div>
      <div style="overflow-y:auto;flex:1;display:flex;flex-direction:column;gap:6px;margin-bottom:16px">
        ${depts.map(d => `
          <button id="dept-pick-${d.id}" onclick="_selectPickerDept('${d.id}')"
            style="display:flex;align-items:center;gap:12px;padding:12px 14px;background:var(--surface2);border:2px solid var(--border);border-radius:10px;cursor:pointer;text-align:left;width:100%;transition:all 0.15s">
            <span style="font-size:22px">${d.emoji}</span>
            <span style="font-size:16px;font-weight:600;color:var(--text)">${esc(d.name)}</span>
            <span id="dept-pick-check-${d.id}" style="margin-left:auto;display:none;color:var(--accent);font-size:18px">✓</span>
          </button>`).join('')}
      </div>
      <div style="display:flex;gap:10px">
        <button onclick="_cancelDeptPicker()" style="flex:1;padding:13px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:16px;font-weight:600;cursor:pointer">Cancel</button>
        <button onclick="_confirmDeptPicker()" style="flex:2;padding:13px;border-radius:10px;border:none;background:var(--accent);color:#111;font-size:16px;font-weight:700;cursor:pointer">Move here →</button>
      </div>
    </div>`;

  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) _cancelDeptPicker(); });
}

async function _deleteSelected() {
  const ids = [...grocerySelected];
  if (!ids.length) return;
  if (!confirm(`Delete ${ids.length} item${ids.length!==1?'s':''}?`)) return;
  groceryItems = groceryItems.filter(i => !grocerySelected.has(i.id));
  // Add all deleted IDs to tombstones so they don't return from sync
  await Promise.all(ids.map(id => addGroceryTombstone(id)));
  grocerySelected.clear();
  await saveGrocery();
  renderGrocery();
  _updateGrocerySelectionBar();
}

// ── Inline add (+ button) ────────────────────────────────────────────

function toggleGroceryHideChecked() {
  groceryHideChecked = !groceryHideChecked;
  try { localStorage.setItem('stockroom_hide_checked', groceryHideChecked ? '1' : '0'); } catch(e) {}
  const btn = document.getElementById('grocery-hide-checked-btn');
  if (btn) {
    btn.innerHTML = groceryHideChecked ? `<svg class="icon" aria-hidden="true"><use href="#i-eye"></use></svg> Show ticked` : `<svg class="icon" aria-hidden="true"><use href="#i-eye-off"></use></svg> Hide ticked`;
    btn.style.color = groceryHideChecked ? 'var(--accent)' : '';
    btn.style.borderColor = groceryHideChecked ? 'rgba(232,168,56,0.4)' : '';
    btn.style.background = groceryHideChecked ? 'rgba(232,168,56,0.08)' : '';
  }
  renderGrocery();
}

async function addGroceryItemToDept(deptId) {
  // Enable edit mode if not already — don't re-render yet, do it once after item is created
  groceryEditMode = true;
  // Create a blank item and insert at the top of that dept
  const newId = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
  const newItem = {
    id: newId, name: '', department: deptId || 'other',
    listId: activeGroceryListId || 'default',
    notes: '', recurring: false, intervalDays: 0,
    checked: false, addedAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    _isNew: true,
  };
  groceryItems.unshift(newItem);
  const order = getGroceryManualOrder();
  saveGroceryManualOrder([newId, ...order]);
  // Save without triggering sync yet — item is still blank
  await _saveGroceryLocal();
  // Single render with edit mode already on
  renderGrocery();
  // Focus the new item's name input
  setTimeout(() => {
    const input = document.getElementById(`gi-name-${newId}`);
    if (input) { input.focus(); input.select(); }
  }, 80);
}

// ── Settings collapsible sections ────────────────────────────────
// All sections start open by default. State persists to localStorage.

const SETTINGS_COLLAPSE_KEY = 'stockroom_settings_collapsed';

function _getSettingsCollapsed() {
  try { return JSON.parse(localStorage.getItem(SETTINGS_COLLAPSE_KEY) || '{}'); }
  catch(e) { return {}; }
}

function _saveSettingsCollapsed(state) {
  try { localStorage.setItem(SETTINGS_COLLAPSE_KEY, JSON.stringify(state)); }
  catch(e) {}
}

function toggleSettings(bodyId, headerEl) {
  // On desktop the sections are always expanded — clicking the header scrolls to it instead
  if (window.innerWidth >= 900) {
    scrollToSection(bodyId);
    return;
  }
  const body = document.getElementById(bodyId);
  if (!body) return;
  const isOpen = body.style.display !== 'none';
  const nowOpen = !isOpen;
  body.style.display = nowOpen ? '' : 'none';
  const chevron = headerEl?.querySelector('.settings-chevron');
  if (chevron) chevron.style.transform = nowOpen ? '' : 'rotate(-90deg)';
  const state = _getSettingsCollapsed();
  if (nowOpen) delete state[bodyId];
  else state[bodyId] = true;
  _saveSettingsCollapsed(state);
}

function initSettingsCollapsibles() {
  const collapsed = _getSettingsCollapsed();
  const allIds = [
    'settings-households-body', 'settings-account-body', 'settings-alerts-body',
    'settings-reminders-body',  'settings-prefs-body',   'settings-about-body',
  ];

  // On desktop: always show all sections, sidebar handles navigation
  const isDesktop = window.innerWidth >= 900;

  allIds.forEach(bodyId => {
    const body = document.getElementById(bodyId);
    if (!body) return;
    const header = body.previousElementSibling;
    const chevron = header?.querySelector?.('.settings-chevron');
    if (isDesktop) {
      // Always expanded on desktop
      body.style.display = '';
      if (chevron) chevron.style.transform = '';
    } else if (collapsed[bodyId]) {
      body.style.display = 'none';
      if (chevron) chevron.style.transform = 'rotate(-90deg)';
    } else {
      body.style.display = '';
      if (chevron) chevron.style.transform = '';
    }
  });

  if (isDesktop) _initSettingsSidebarScroll();
}

function scrollToSection(bodyId) {
  const el = document.getElementById(bodyId);
  if (!el) return;
  const offset = 56 + 61 + 20; // app-header + tabs-wrap + spacing
  const top = el.getBoundingClientRect().top + window.scrollY - offset;
  window.scrollTo({ top, behavior: 'smooth' });
  document.querySelectorAll('.settings-nav-link').forEach(a => {
    a.classList.toggle('active', (a.getAttribute('onclick') || '').includes(bodyId));
  });
}

function _initSettingsSidebarScroll() {
  // Highlight sidebar link as each section scrolls into view
  const sections = [
    'settings-households-body', 'settings-account-body', 'settings-alerts-body',
    'settings-reminders-body',  'settings-prefs-body',   'settings-about-body',
  ];
  // Remove any old observer
  if (window._settingsObserver) window._settingsObserver.disconnect();
  window._settingsObserver = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const id = entry.target.id;
        document.querySelectorAll('.settings-nav-link').forEach(a => {
          a.classList.toggle('active', (a.getAttribute('onclick') || '').includes(id));
        });
      }
    });
  }, { rootMargin: '-10% 0px -70% 0px', threshold: 0 });
  sections.forEach(id => {
    const el = document.getElementById(id);
    if (el) window._settingsObserver.observe(el);
  });
  // Highlight first link by default
  const first = document.querySelector('.settings-nav-link');
  if (first) first.classList.add('active');
}

// Keep old name as alias for any stale references
function toggleSettingsSection(bodyId, headerEl) { toggleSettings(bodyId, headerEl); }

function toggleGroceryEditMode() {
  groceryEditMode = !groceryEditMode;
  if (!groceryEditMode) grocerySelected.clear();
  // When entering edit mode, auto-show the Done editing pill beside the FAB
  if (groceryEditMode) {
    _showGroceryDoneSlide();
  } else {
    _hideGroceryDoneSlide();
  }
  renderGrocery();
  _updateGrocerySelectionBar();
}

// Show the "Done editing" amber pill sliding left from FAB — always visible in edit mode
function _showGroceryDoneSlide() {
  const btn = document.getElementById('fab-btn');
  if (!btn || btn.style.display === 'none') return;
  _hideGroceryDoneSlide();
  const slide = document.createElement('div');
  slide.id = 'grocery-done-slide';
  slide.onclick = () => { toggleGroceryEditMode(); };
  slide.style.cssText = 'position:fixed;bottom:88px;right:112px;z-index:1100;display:flex;align-items:center;gap:10px;cursor:pointer;animation:fabSlideLeft 0.22s cubic-bezier(0.34,1.56,0.64,1) both';
  slide.innerHTML = `<span style="font-size:17px;font-weight:700;color:#111;background:var(--accent);padding:10px 18px;border-radius:12px;white-space:nowrap;box-shadow:0 4px 16px rgba(232,168,56,0.45);display:flex;align-items:center;gap:8px"><svg class="icon" aria-hidden="true" style="color:#111"><use href="#i-lock"></use></svg> Done editing</span>`;
  document.body.appendChild(slide);
}

function _hideGroceryDoneSlide() {
  document.getElementById('grocery-done-slide')?.remove();
}

// ── Drag-to-reorder ─────────────────────────────────────────
let _dragSrcEl   = null;
let _dragSrcDept = null;

function _showChangeDeptZone() {
  // Fixed bar pinned to bottom of screen — appears when drag starts
  if (document.getElementById('grocery-change-dept-zone')) {
    document.getElementById('grocery-change-dept-zone').style.display = 'flex';
    return;
  }
  const zone = document.createElement('div');
  zone.id = 'grocery-change-dept-zone';
  zone.style.cssText = [
    'position:fixed;bottom:0;left:0;right:0;z-index:500',
    'background:rgba(15,17,23,0.96)',
    'border-top:2px solid rgba(232,168,56,0.5)',
    'padding:14px 16px 28px',
    'display:flex;align-items:center;justify-content:center;gap:12px',
    'backdrop-filter:blur(8px)',
    'transition:transform 0.2s ease',
  ].join(';');

  // Change Dept drop target
  const deptBtn = document.createElement('div');
  deptBtn.id = 'grocery-zone-dept';
  deptBtn.style.cssText = 'flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px;border:2px dashed rgba(232,168,56,0.4);border-radius:12px;cursor:pointer';
  deptBtn.innerHTML = `<svg aria-hidden="true" style="width:22px;height:22px;color:var(--accent)"><use href="#i-folder-open"></use></svg><div style="font-size:12px;font-weight:700;color:var(--accent);letter-spacing:1px">CHANGE DEPT</div>`;
  deptBtn.addEventListener('dragover', e => { e.preventDefault(); deptBtn.style.background='rgba(232,168,56,0.12)'; });
  deptBtn.addEventListener('dragleave', () => { deptBtn.style.background=''; });
  deptBtn.addEventListener('drop', e => { e.preventDefault(); deptBtn.style.background=''; _showDeptPickerOverlay(); });
  deptBtn.addEventListener('touchend', e => { if (_dragSrcEl) { e.preventDefault(); _showDeptPickerOverlay(); } });

  // Change List button (only if 2+ lists)
  const listBtn = document.createElement('div');
  listBtn.id = 'grocery-zone-list';
  listBtn.style.cssText = 'flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px;border:2px dashed rgba(91,141,238,0.4);border-radius:12px;cursor:pointer';
  listBtn.innerHTML = `<svg class="icon" style="width:22px;height:22px" aria-hidden="true"><use href="#i-store"></use></svg><div style="font-size:12px;font-weight:700;color:#5b8dee;letter-spacing:1px">CHANGE LIST</div>`;
  listBtn.addEventListener('dragover', e => { e.preventDefault(); listBtn.style.background='rgba(91,141,238,0.12)'; });
  listBtn.addEventListener('dragleave', () => { listBtn.style.background=''; });
  listBtn.addEventListener('drop', e => { e.preventDefault(); listBtn.style.background=''; const id = _dragSrcEl?.dataset?.id; if (id) openChangeGroceryItemStore(id); _hideChangeDeptZone(); });
  listBtn.addEventListener('touchend', e => { if (_dragSrcEl) { e.preventDefault(); const id = _dragSrcEl.dataset.id; if (id) openChangeGroceryItemStore(id); _hideChangeDeptZone(); } });

  // Delete button
  const delBtn = document.createElement('div');
  delBtn.id = 'grocery-zone-delete';
  delBtn.style.cssText = 'flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px;border:2px dashed rgba(232,80,80,0.4);border-radius:12px;cursor:pointer';
  delBtn.innerHTML = `<svg aria-hidden="true" style="width:22px;height:22px;color:var(--danger)"><use href="#i-trash-2"></use></svg><div style="font-size:12px;font-weight:700;color:var(--danger);letter-spacing:1px">DELETE</div>`;
  delBtn.addEventListener('dragover', e => { e.preventDefault(); delBtn.style.background='rgba(232,80,80,0.12)'; });
  delBtn.addEventListener('dragleave', () => { delBtn.style.background=''; });
  delBtn.addEventListener('drop', async e => {
    e.preventDefault(); delBtn.style.background='';
    const id = _dragSrcEl?.dataset?.id;
    if (id) { groceryItems = groceryItems.filter(i => i.id !== id); await addGroceryTombstone(id); await saveGrocery(); renderGrocery(); }
    _hideChangeDeptZone(); _dragSrcEl = null;
  });
  delBtn.addEventListener('touchend', async e => {
    if (_dragSrcEl) {
      e.preventDefault();
      const id = _dragSrcEl.dataset.id;
      if (id) { groceryItems = groceryItems.filter(i => i.id !== id); await addGroceryTombstone(id); await saveGrocery(); renderGrocery(); }
      _hideChangeDeptZone(); _dragSrcEl = null;
    }
  });

  zone.appendChild(deptBtn);
  if (groceryLists.length > 1) zone.appendChild(listBtn);
  zone.appendChild(delBtn);
  document.body.appendChild(zone);
}

function _hideChangeDeptZone() {
  const zone = document.getElementById('grocery-change-dept-zone');
  if (zone) { zone.style.display = 'none'; }
}

function _showDeptPickerOverlay() {
  // Remove any existing
  document.getElementById('grocery-dept-picker-overlay')?.remove();

  const depts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;
  // Capture item ID NOW while _dragSrcEl is still live
  const itemId      = _dragSrcEl?.dataset?.id || '';
  const currentDept = _dragSrcEl?.dataset?.dept || groceryItems.find(i => i.id === itemId)?.department || 'other';

  const overlay = document.createElement('div');
  overlay.id = 'grocery-dept-picker-overlay';
  overlay.style.cssText = [
    'position:fixed;inset:0;z-index:600',
    'background:rgba(0,0,0,0.7)',
    'display:flex;align-items:flex-end;justify-content:center',
    'padding:0',
    'backdrop-filter:blur(4px)',
  ].join(';');

  // Store item ID and selection on overlay so _confirmDeptPicker doesn't need _dragSrcEl
  overlay.dataset.itemId   = itemId;
  overlay.dataset.selected = currentDept;

  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:20px 20px 0 0;width:100%;max-height:75vh;display:flex;flex-direction:column;padding:20px 16px 36px;box-shadow:0 -8px 32px rgba(0,0,0,0.5)">
      <div style="width:40px;height:4px;background:var(--border);border-radius:2px;margin:0 auto 18px"></div>
      <h3 style="font-size:17px;font-weight:700;margin-bottom:4px;text-align:center">Change Department</h3>
      <p style="font-size:13px;color:var(--muted);text-align:center;margin-bottom:4px">Choose a department for this item</p>
      <div style="text-align:center;margin-bottom:14px">
        <button onclick="_cancelDeptPicker();openGroceryDepts()" style="background:none;border:none;color:var(--accent);font-size:13px;cursor:pointer;text-decoration:underline;padding:0">✏️ Edit Departments</button>
      </div>
      <div id="dept-picker-list" style="overflow-y:auto;flex:1;display:flex;flex-direction:column;gap:6px;margin-bottom:16px">
        ${depts.map(d => `
          <button id="dept-pick-${d.id}" onclick="_selectPickerDept('${d.id}')"
            style="display:flex;align-items:center;gap:12px;padding:12px 14px;background:${d.id === currentDept ? 'rgba(232,168,56,0.12)' : 'var(--surface2)'};border:2px solid ${d.id === currentDept ? 'rgba(232,168,56,0.5)' : 'var(--border)'};border-radius:10px;cursor:pointer;text-align:left;transition:all 0.15s;width:100%">
            <span style="font-size:22px">${d.emoji}</span>
            <span style="font-size:16px;font-weight:600;color:var(--text)">${esc(d.name)}</span>
            <span id="dept-pick-check-${d.id}" style="margin-left:auto;display:${d.id === currentDept ? 'block' : 'none'};color:var(--accent);font-size:18px">✓</span>
          </button>`).join('')}
      </div>
      <div style="display:flex;gap:10px">
        <button onclick="_cancelDeptPicker()" style="flex:1;padding:13px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:16px;font-weight:600;cursor:pointer">Cancel</button>
        <button id="dept-picker-ok" onclick="_confirmDeptPicker()" style="flex:2;padding:13px;border-radius:10px;border:none;background:var(--accent);color:#111;font-size:16px;font-weight:700;cursor:pointer">Move here →</button>
      </div>
    </div>`;

  document.body.appendChild(overlay);

  // Tap backdrop to cancel
  overlay.addEventListener('click', e => { if (e.target === overlay) _cancelDeptPicker(); });
}

function _selectPickerDept(deptId) {
  const overlay = document.getElementById('grocery-dept-picker-overlay');
  if (!overlay) return;
  // Update highlight
  const depts = groceryDepts.length ? groceryDepts : DEFAULT_DEPTS;
  depts.forEach(d => {
    const btn   = document.getElementById(`dept-pick-${d.id}`);
    const check = document.getElementById(`dept-pick-check-${d.id}`);
    if (btn) {
      btn.style.background    = d.id === deptId ? 'rgba(232,168,56,0.12)' : 'var(--surface2)';
      btn.style.borderColor   = d.id === deptId ? 'rgba(232,168,56,0.5)' : 'var(--border)';
    }
    if (check) check.style.display = d.id === deptId ? 'block' : 'none';
    // Remove "current" label from all, it only showed pre-selection
  });
  overlay.dataset.selected = deptId;
}

async function _confirmDeptPicker() {
  const overlay   = document.getElementById('grocery-dept-picker-overlay');
  const newDeptId = overlay?.dataset?.selected;
  if (!newDeptId) { overlay?.remove(); return; }

  if (overlay?.dataset?.mode === 'multi') {
    // Multi-select mode — move all selected items
    const ids = [...grocerySelected];
    ids.forEach(id => {
      const item = groceryItems.find(i => i.id === id);
      if (item) { item.department = newDeptId; item.updatedAt = new Date().toISOString(); }
    });
    grocerySelected.clear();
    await saveGrocery();
    overlay?.remove();
    renderGrocery();
    _updateGrocerySelectionBar();
  } else {
    // Single drag-to-change-dept mode
    const itemId = overlay?.dataset?.itemId;
    if (itemId) {
      const item = groceryItems.find(i => i.id === itemId);
      if (item && item.department !== newDeptId) {
        item.department = newDeptId;
        item.updatedAt  = new Date().toISOString();
        await saveGrocery();
      }
    }
    if (_dragSrcEl) { _dragSrcEl.classList.remove('dragging'); _dragSrcEl = null; _dragSrcDept = null; }
    overlay?.remove();
    _hideChangeDeptZone();
    renderGrocery();
  }
}

function _cancelDeptPicker() {
  document.getElementById('grocery-dept-picker-overlay')?.remove();
  // Item stays in original dept — just drop drag state
  _dragSrcEl?.classList.remove('dragging');
  _dragSrcEl = null; _dragSrcDept = null;
  _hideChangeDeptZone();
  _persistDragOrder();
}

async function _changeDragItemDept(newDeptId) {
  // Kept for backwards compat — now delegates to confirm flow
  const overlay = document.getElementById('grocery-dept-picker-overlay');
  if (overlay) { overlay.dataset.selected = newDeptId; await _confirmDeptPicker(); return; }
  if (!_dragSrcEl) return;
  const id   = _dragSrcEl.dataset.id;
  const item = groceryItems.find(i => i.id === id);
  if (item && item.department !== newDeptId) {
    item.department = newDeptId;
    item.updatedAt  = new Date().toISOString();
    await saveGrocery();
  }
  _dragSrcEl.classList.remove('dragging');
  _dragSrcEl = null; _dragSrcDept = null;
  _hideChangeDeptZone();
  renderGrocery();
}

function initGroceryDragSort() {
  document.querySelectorAll('.grocery-edit-dept-group').forEach(group => {
    group.querySelectorAll('.grocery-edit-row[draggable="true"]').forEach(row => {
      row.addEventListener('dragstart', e => {
        _dragSrcEl   = row;
        _dragSrcDept = group;
        e.dataTransfer.effectAllowed = 'move';
        setTimeout(() => { row.classList.add('dragging'); _showChangeDeptZone(); }, 0);
      });
      row.addEventListener('dragend', () => {
        row.classList.remove('dragging');
        _hideChangeDeptZone();
        _dragSrcEl = null; _dragSrcDept = null;
        _persistDragOrder();
      });
      row.addEventListener('dragover', e => {
        e.preventDefault();
        if (!_dragSrcEl || _dragSrcEl === row || _dragSrcDept !== group) return;
        const rect = row.getBoundingClientRect();
        if (e.clientY < rect.top + rect.height / 2) group.insertBefore(_dragSrcEl, row);
        else group.insertBefore(_dragSrcEl, row.nextSibling);
      });
      // Touch drag — requires 400ms hold before drag activates
      let _touchHoldTimer = null;
      let _touchMoved     = false;

      row.addEventListener('touchstart', e => {
        _touchMoved = false;
        _touchHoldTimer = setTimeout(() => {
          // Hold confirmed — start drag
          _dragSrcEl   = row;
          _dragSrcDept = group;
          row.classList.add('dragging');
          _showChangeDeptZone();
          // Light haptic if supported
          if (navigator.vibrate) navigator.vibrate(30);
        }, 400);
      }, { passive: true });

      row.addEventListener('touchmove', e => {
        // If finger moves more than 8px before hold, cancel the hold timer (it's a scroll)
        if (_touchHoldTimer && !_dragSrcEl) {
          clearTimeout(_touchHoldTimer);
          _touchHoldTimer = null;
          return;
        }
        if (!_dragSrcEl) return;
        e.preventDefault(); // prevent scroll while dragging
        const t = e.touches[0];
        const target = document.elementFromPoint(t.clientX, t.clientY)?.closest('.grocery-edit-row');
        if (target && target !== _dragSrcEl && target.closest('.grocery-edit-dept-group') === group) {
          const rect = target.getBoundingClientRect();
          if (t.clientY < rect.top + rect.height / 2) group.insertBefore(_dragSrcEl, target);
          else group.insertBefore(_dragSrcEl, target.nextSibling);
        }
        // If finger lands on change-dept zone, highlight it
        const zone = document.getElementById('grocery-change-dept-zone');
        if (zone) {
          const zr = zone.getBoundingClientRect();
          const onZone = t.clientY >= zr.top;
          zone.style.borderTopColor = onZone ? 'var(--accent)' : 'rgba(232,168,56,0.5)';
          zone.style.background = onZone ? 'rgba(232,168,56,0.18)' : 'rgba(15,17,23,0.96)';
        }
      }, { passive: false });

      row.addEventListener('touchend', e => {
        clearTimeout(_touchHoldTimer);
        _touchHoldTimer = null;
        if (!_dragSrcEl) return; // was a tap, not a drag — do nothing

        // Check if finger ended on change-dept zone
        const t = e.changedTouches[0];
        const zone = document.getElementById('grocery-change-dept-zone');
        if (zone) {
          const zr = zone.getBoundingClientRect();
          if (t.clientY >= zr.top) {
            // Dropped on zone — show dept picker without clearing _dragSrcEl
            zone.style.borderTopColor = 'rgba(232,168,56,0.5)';
            zone.style.background     = 'rgba(15,17,23,0.96)';
            _showDeptPickerOverlay();
            return; // _dragSrcEl stays alive for _confirmDeptPicker
          }
        }

        _dragSrcEl.classList.remove('dragging');
        _hideChangeDeptZone();
        _dragSrcEl = null; _dragSrcDept = null;
        _persistDragOrder();
      });
    });
  });
}

function _persistDragOrder() {
  // Collect order from all dept groups in DOM order
  const newOrder = [...document.querySelectorAll('.grocery-edit-dept-group .grocery-edit-row')]
    .map(el => el.dataset.id).filter(Boolean);
  const checkedIds = groceryItems.filter(i => i.checked).map(i => i.id);
  // Append any items not currently visible (checked, filtered out) to end
  const visible = new Set(newOrder);
  const rest = groceryItems.filter(i => !visible.has(i.id)).map(i => i.id);
  saveGroceryManualOrder([...newOrder, ...rest]);
}

function groceryItemHTML(item) {
  const deptDef = groceryDepts.find(d => d.id === (item.department || 'other')) || {name:'Other', emoji:'📦'};
  const meta = [
    grocerySort === 'alpha' ? `${deptDef.emoji} ${deptDef.name}` : null,
    item.notes || null,
    item.recurring ? `↻ every ${item.intervalDays||7}d` : null,
  ].filter(Boolean).join(' · ');

  // Full-row tap target: no checkbox, no menu button
  return `<div class="grocery-item${item.checked?' checked':''}" id="gitem-${item.id}"
    role="button" tabindex="0"
    onclick="tapGroceryItem('${item.id}')"
    onkeydown="if(event.key==='Enter'||event.key===' ')tapGroceryItem('${item.id}')"
    style="cursor:pointer;user-select:none">
    <div class="grocery-item-info">
      <div class="grocery-item-name">${esc(item.name)}</div>
      ${meta ? `<div class="grocery-item-meta">${esc(meta)}</div>` : ''}
    </div>
  </div>`;
}

async function tapGroceryItem(id) {
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;

  // Optimistic update — flip state immediately in memory
  item.checked   = !item.checked;
  item.checkedAt = item.checked ? new Date().toISOString() : null;

  // Update the DOM instantly without waiting for save/re-render
  _updateGroceryItemDOM(id, item.checked);

  // Save locally (IDB, fast), then smart-queue server sync
  await _saveGroceryLocal();
  _smartSync.enqueueGrocery();
  bcPost({ type: 'GROCERY_CHANGED' });

  // Full re-render deferred — lets item animate to bottom of dept smoothly
  setTimeout(() => renderGrocery(), 350);
}

async function toggleGroceryCheck(id, cb) {
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;
  item.checked   = cb.checked;
  item.checkedAt = cb.checked ? new Date().toISOString() : null;

  // Save and smart-queue
  await _saveGroceryLocal();
  _smartSync.enqueueGrocery();
  bcPost({ type: 'GROCERY_CHANGED' });

  setTimeout(() => renderGrocery(), 350);
}

// Instantly update a single grocery item's visual state without full re-render
function _updateGroceryItemDOM(id, checked) {
  const el = document.getElementById(`gitem-${id}`);
  if (!el) return;
  el.classList.toggle('checked', checked);
  // Strike through name
  const nameEl = el.querySelector('.grocery-item-name');
  if (nameEl) nameEl.style.textDecoration = checked ? 'line-through' : '';
  // Dim the row
  el.style.opacity = checked ? '0.45' : '';
}

async function clearCheckedGrocery() {
  if (!canWrite('groceries')) { showLockBanner('groceries'); return; }
  groceryItems = groceryItems.map(item => {
    if ((item.listId || 'default') !== activeGroceryListId) return item;
    if (!item.checked) return item;
    // Uncheck all checked items (recurring and non-recurring alike)
    return { ...item, checked: false, checkedAt: null };
  });
  await _saveGroceryLocal();
  _smartSync.enqueueGrocery();
  renderGrocery();
}

// ── Add / Edit ────────────────────────────────────────────

function openAddGroceryItem(prefillName) {
  document.getElementById('grocery-modal-title').textContent = 'Add Grocery Item';
  document.getElementById('grocery-edit-id').value = '';
  document.getElementById('grocery-f-name').value  = prefillName || '';
  document.getElementById('grocery-f-notes').value = '';
  document.getElementById('grocery-f-recurring').checked = false;
  document.getElementById('grocery-f-interval').value = '7';
  document.getElementById('grocery-f-interval-unit').value = '1';
  document.getElementById('grocery-recurring-opts').style.display = 'none';
  populateGroceryDeptSelect('');
  openModal('grocery-item-modal');
  setTimeout(() => document.getElementById('grocery-f-name').focus(), 100);
}

// Add item directly to a specific department — enables edit mode first
function openEditGroceryItem(id) {
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;
  document.getElementById('grocery-modal-title').textContent = 'Edit Item';
  document.getElementById('grocery-edit-id').value = id;
  document.getElementById('grocery-f-name').value  = item.name;
  document.getElementById('grocery-f-notes').value = item.notes || '';
  document.getElementById('grocery-f-recurring').checked = !!item.recurring;
  const days = item.intervalDays || 7;
  // pick best unit
  if (days % 30 === 0 && days >= 30) {
    document.getElementById('grocery-f-interval').value = days / 30;
    document.getElementById('grocery-f-interval-unit').value = '30';
  } else if (days % 7 === 0) {
    document.getElementById('grocery-f-interval').value = days / 7;
    document.getElementById('grocery-f-interval-unit').value = '7';
  } else {
    document.getElementById('grocery-f-interval').value = days;
    document.getElementById('grocery-f-interval-unit').value = '1';
  }
  document.getElementById('grocery-recurring-opts').style.display = item.recurring ? 'block' : 'none';
  populateGroceryDeptSelect(item.department || 'other');
  openModal('grocery-item-modal');
}

function populateGroceryDeptSelect(selectedId) {
  const sel = document.getElementById('grocery-f-dept');
  sel.innerHTML = groceryDepts.map(d =>
    `<option value="${d.id}" ${d.id === selectedId ? 'selected' : ''}>${d.emoji} ${d.name}</option>`
  ).join('');
}

function toggleGroceryRecurring() {
  const checked = document.getElementById('grocery-f-recurring').checked;
  document.getElementById('grocery-recurring-opts').style.display = checked ? 'block' : 'none';
}

async function saveGroceryItem() {
  if (!canWrite("groceries")) { showLockBanner("groceries"); return; }
  const name = document.getElementById('grocery-f-name').value.trim();
  if (!name) { showToast('Please enter an item name'); return; }
  const id        = document.getElementById('grocery-edit-id').value;
  const dept      = document.getElementById('grocery-f-dept').value;
  const notes     = document.getElementById('grocery-f-notes').value.trim();
  const recurring = document.getElementById('grocery-f-recurring').checked;
  const intervalVal  = parseInt(document.getElementById('grocery-f-interval').value) || 7;
  const intervalUnit = parseInt(document.getElementById('grocery-f-interval-unit').value) || 1;
  const intervalDays = intervalVal * intervalUnit;

  if (id) {
    const item = groceryItems.find(i => i.id === id);
    if (item) { Object.assign(item, {name, department:dept, notes, recurring, intervalDays, updatedAt:new Date().toISOString()}); }
  } else {
    const newId = 'g_'+Date.now()+'_'+Math.random().toString(36).slice(2,6);
    groceryItems.push({ id: newId, name, department:dept, notes, recurring, intervalDays, checked:false, listId: activeGroceryListId, addedAt:new Date().toISOString(), updatedAt:new Date().toISOString() });
    appendToGroceryOrder(newId);
  }
  await saveGrocery();
  closeModal('grocery-item-modal');
  renderGrocery();
}

// ── Context menu ─────────────────────────────────────────

function openGroceryContext(e, id) {
  e.stopPropagation();
  groceryContextTarget = id;
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;
  const menu = document.getElementById('grocery-context-menu');
  menu.innerHTML = `
    <button class="grocery-context-item" onclick="editFromContext()"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg> Edit item</button>
    <button class="grocery-context-item" onclick="convertFromContext()"><svg class="icon" aria-hidden="true"><use href="#i-package"></use></svg> Convert to stock item</button>
    <button class="grocery-context-item danger" onclick="deleteFromContext()"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg> Delete</button>
  `;
  // Position near tap
  const rect = e.currentTarget.getBoundingClientRect();
  menu.style.display = 'block';
  const menuW = 190, menuH = 140;
  let left = rect.left - menuW + rect.width;
  let top  = rect.bottom + 6;
  if (left < 8) left = 8;
  if (top + menuH > window.innerHeight) top = rect.top - menuH - 6;
  menu.style.left = left + 'px';
  menu.style.top  = top  + 'px';

  setTimeout(() => document.addEventListener('click', dismissGroceryContext, {once:true}), 10);
}

function dismissGroceryContext() {
  document.getElementById('grocery-context-menu').style.display = 'none';
  groceryContextTarget = null;
}

function editFromContext() {
  const id = groceryContextTarget;
  dismissGroceryContext();
  if (id) openEditGroceryItem(id);
}

async function deleteFromContext() {
  const id = groceryContextTarget;
  dismissGroceryContext();
  if (!id) return;
  groceryItems = groceryItems.filter(i => i.id !== id);
  await saveGrocery();
  renderGrocery();
}

function convertFromContext() {
  const id = groceryContextTarget;
  dismissGroceryContext();
  if (!id) return;
  const item = groceryItems.find(i => i.id === id);
  if (!item) return;
  groceryConvertItem = item;
  const body = document.getElementById('grocery-convert-body');
  const deptDef = groceryDepts.find(d => d.id === (item.department||'other')) || {name:'Other',emoji:'📦'};
  body.innerHTML = `
    <div style="background:var(--surface2);border-radius:8px;padding:14px;margin-bottom:16px;border:1px solid var(--border)">
      <div style="font-size:15px;font-weight:700;margin-bottom:4px">${esc(item.name)}</div>
      <div style="font-size:12px;color:var(--muted)">${deptDef.emoji} ${esc(deptDef.name)}${item.notes?' · '+esc(item.notes):''}</div>
    </div>
    <div class="form-group">
      <label class="form-label">Category in Stockroom</label>
      <select class="form-input" id="convert-category">
        <option>Food &amp; Drink</option><option>Kitchen</option><option>Bathroom</option>
        <option>Cleaning</option><option>Health</option><option>Garden</option><option>Other</option>
      </select>
    </div>
    <div class="form-group">
      <label class="form-label">Months of supply per purchase</label>
      <input class="form-input" id="convert-months" type="number" min="0.25" max="24" step="0.25" value="1">
    </div>
    <p style="font-size:12px;color:var(--muted);margin-top:4px">You can finish setting it up in the stock item form.</p>
  `;
  openModal('grocery-convert-modal');
}

function doConvertToStock() {
  if (!groceryConvertItem) return;
  const category = document.getElementById('convert-category').value;
  const months   = parseFloat(document.getElementById('convert-months').value) || 1;
  closeModal('grocery-convert-modal');
  // Pre-fill the main item modal
  openItemModal();
  setTimeout(() => {
    const nameEl = document.getElementById('f-name');
    const catEl  = document.getElementById('f-category');
    const monEl  = document.getElementById('f-months');
    if (nameEl) nameEl.value = groceryConvertItem.name;
    if (catEl)  catEl.value  = category;
    if (monEl)  monEl.value  = months;
    // Switch to the stock tab
    const stockTab = [...document.querySelectorAll('.tab')].find(t => t.textContent.includes('Stock'));
    if (stockTab) showView('stock', stockTab);
  }, 150);
  groceryConvertItem = null;
}

// ── Departments modal ──────────────────────────────────────

function openGroceryDepts() {
  renderGroceryDeptsModal();
  // populate shop interval
  const si = getGroceryShopInterval();
  document.getElementById('grocery-shop-interval').value = si.value;
  document.getElementById('grocery-shop-interval-unit').value = si.unit;
  openModal('grocery-depts-modal');
}

function renderGroceryDeptsModal() {
  const list = document.getElementById('grocery-depts-list');
  list.innerHTML = groceryDepts.map((d, idx) => `
    <div class="dept-manage-row">
      <span class="dept-manage-emoji">${d.emoji}</span>
      <span class="dept-manage-name">${esc(d.name)}</span>
      <div class="dept-manage-actions">
        ${idx > 0 ? `<button class="grocery-icon-btn" onclick="moveGroceryDept(${idx},-1)" title="Move up">↑</button>` : ''}
        ${idx < groceryDepts.length-1 ? `<button class="grocery-icon-btn" onclick="moveGroceryDept(${idx},1)" title="Move down">↓</button>` : ''}
        <button class="grocery-icon-btn" style="color:var(--danger)" onclick="deleteGroceryDept('${d.id}')" title="Delete"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
      </div>
    </div>
  `).join('');
}

async function moveGroceryDept(idx, dir) {
  const newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= groceryDepts.length) return;
  [groceryDepts[idx], groceryDepts[newIdx]] = [groceryDepts[newIdx], groceryDepts[idx]];
  await saveGroceryDepts();
  renderGroceryDeptsModal();
}

async function deleteGroceryDept(id) {
  const inUse = groceryItems.some(i => i.department === id);
  if (inUse && !confirm('Items use this department. They\'ll move to Other. Continue?')) return;
  if (inUse) groceryItems.forEach(i => { if (i.department === id) i.department = 'other'; });
  groceryDepts = groceryDepts.filter(d => d.id !== id);
  await saveGroceryDepts();
  await saveGrocery();
  renderGroceryDeptsModal();
}

async function addGroceryDept() {
  const nameInput  = document.getElementById('grocery-new-dept-name');
  const emojiInput = document.getElementById('grocery-new-dept-emoji');
  const name  = nameInput.value.trim();
  const emoji = emojiInput.value.trim() || '📦';
  if (!name) { showToast('Please enter a department name'); return; }
  const id = 'dept_' + Date.now();
  groceryDepts.push({id, name, emoji});
  await saveGroceryDepts();
  nameInput.value  = '';
  emojiInput.value = '';
  renderGroceryDeptsModal();
  showToast('Department added');
}

// ── Auto-restore recurring items ───────────────────────────
async function checkGroceryRecurring() {
  const now = Date.now();
  let changed = false;
  groceryItems.forEach(item => {
    if (!item.recurring || !item.checked || !item.checkedAt) return;
    const intervalMs = (item.intervalDays || 7) * 86400000;
    if (now - new Date(item.checkedAt).getTime() >= intervalMs) {
      item.checked   = false;
      item.checkedAt = null;
      changed = true;
    }
  });
  if (changed) { saveGrocery(); renderGrocery(); }
}

// ═══════════════════════════════════════════════════════════
//  HOUSEHOLD PRESENCE
// ═══════════════════════════════════════════════════════════

const PRESENCE_COLOURS = [
  '#e8a838','#5b8dee','#4cbb8a','#e85050','#b45dee',
  '#ee8c5b','#5bdee8','#dee85b','#ee5bab','#5beeaa',
];

let _householdEnabled  = false;
let _householdName     = '';
let _householdColour   = PRESENCE_COLOURS[0];
let _presenceSSE       = null;
let _presencePingTimer = null;
let _currentViewName   = 'stock';
let _otherPresence     = {}; // { userId: { name, colour, view, ts } }

// ── Persistence ───────────────────────────────────────────
function loadHouseholdSettings() {
  try {
    const raw = localStorage.getItem('stockroom_household');
    if (!raw) return;
    const d = JSON.parse(raw);
    _householdEnabled = !!d.enabled;
    _householdColour  = d.colour || PRESENCE_COLOURS[0];
  } catch(e) {}
}

function saveHouseholdSettings() {
  // Household mode auto-enables when a share target is added
  if (!_householdEnabled) return;
  try {
    localStorage.setItem('stockroom_household', JSON.stringify({
      enabled: _householdEnabled,
      colour:  _householdColour,
    }));
  } catch(e) {}
  if (_householdEnabled) connectPresence();
}

function pickHouseholdColour(colour) {
  _householdColour = colour;
  renderHouseholdColourPicker();
  saveHouseholdSettings();
  pushPresence();
}

function renderHouseholdColourPicker() {
  const el = document.getElementById('household-colour-picker');
  if (!el) return;
  el.innerHTML = PRESENCE_COLOURS.map(c => `
    <div onclick="pickHouseholdColour('${c}')"
      style="width:28px;height:28px;border-radius:50%;background:${c};cursor:pointer;
             border:3px solid ${c === _householdColour ? 'var(--text)' : 'transparent'};
             transition:border-color 0.15s;box-shadow:0 2px 6px rgba(0,0,0,0.3)"></div>
  `).join('');
}

// ── Household sharing — file proxy ───────────────────────
// ═══════════════════════════════════════════════════════════
//  SHARE PERMISSION SYSTEM
//  When a user joins via a share link, _shareState holds their
//  permission set. This drives tab locking and edit enforcement.
// ═══════════════════════════════════════════════════════════

// _shareState: null = owner (full access), or { code, name, type, ownerName, households }
let _shareState    = null;
let _sharedFileId  = null; // kept for legacy compat
let _shareKey      = null; // CryptoKey — the AES-GCM share key (travels in URL fragment)
let _pendingJoinCode  = null; // join code awaiting auth
let _pendingShareMeta = null; // share metadata awaiting auth
let _inviteCode    = null;

// Default permission sets per user type
const SHARE_TYPE_DEFAULTS = {
  family: {
    stockroom: 'rw', groceries: 'rw', reminders: 'rw', savings: 'rw', report: 'r'
  },
  cleaner: {
    stockroom: 'r', groceries: 'rw', reminders: 'none', savings: 'none', report: 'none'
  },
  guest: {
    stockroom: 'r', groceries: 'r', reminders: 'none', savings: 'none', report: 'r'
  },
};

const SECTION_LABELS = {
  stockroom: '📦 Stockroom', groceries: '🛒 Groceries',
  reminders: '🔔 Reminders', savings: '💰 Savings', report: '📋 Report',
};

// Get permission for a section in the current household
// Returns 'rw', 'r', or 'none'. Returns 'rw' for owners.
function getSectionPerm(section) {
  if (!_shareState) return 'rw'; // owner
  const hKey  = activeProfile || 'default';
  const hPerms = _shareState.households?.[hKey];
  if (!hPerms) return 'none'; // no access to this household
  return hPerms[section] || 'none';
}

function canView(section)  { const p = getSectionPerm(section); return p === 'rw' || p === 'r'; }
function canWrite(section) { return getSectionPerm(section) === 'rw'; }
function isOwner()         { return !_shareState; }

// Apply permission state to the tab bar — lock inaccessible sections
function applyTabPermissions() {
  if (!_shareState) return; // owner sees everything
  const sectionToTab = {
    stockroom: 'stock', groceries: 'grocery', reminders: 'reminders',
    savings: 'savings', report: 'report',
  };
  document.querySelectorAll('.tab').forEach(tab => {
    const text = tab.textContent.trim().toLowerCase();
    let section = null;
    if (text.includes('stockroom')) section = 'stockroom';
    else if (text.includes('groceries') || text.includes('grocery')) section = 'groceries';
    else if (text.includes('reminders')) section = 'reminders';
    else if (text.includes('savings')) section = 'savings';
    else if (text.includes('report')) section = 'report';
    if (!section) return;
    if (canView(section)) {
      tab.disabled = false;
      tab.style.opacity = '';
      tab.title = '';
    } else {
      tab.disabled = true;
      tab.style.opacity = '0.4';
      tab.title = `🔒 Ask ${_shareState.ownerName || 'the owner'} for access`;
    }
  });
}

// Show lock banner when a user tries to access a restricted section
function showLockBanner(section) {
  const label = SECTION_LABELS[section] || section;
  toast(`🔒 ${label} — ask ${_shareState?.ownerName || 'the owner'} for access`);
}

// Save/restore share state from localStorage
async function loadShareState() {
  try {
    const raw = localStorage.getItem('stockroom_share_state');
    if (!raw) return;
    const stored = JSON.parse(raw);
    // Don't load share state if this user is the owner of this share
    if (stored.ownerEmailHash && _kvEmailHash && stored.ownerEmailHash === _kvEmailHash) {
      localStorage.removeItem('stockroom_share_state');
      return;
    }
    _shareState   = stored;
    _sharedFileId = stored._sharedFileId || null;
    // Restore share key from local cache (ECDH system — key is never stored in state)
    try {
      const localKeys = await _getShareKeys();
      const keyB64    = localKeys[stored.code];
      if (keyB64) {
        const keyBytes = Uint8Array.from(atob(keyB64), c => c.charCodeAt(0));
        _shareKey = await crypto.subtle.importKey('raw', keyBytes, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
      }
    } catch(e) { console.warn('Could not restore share key from cache:', e.message); }
  } catch(e) {}
}

function saveShareState() {
  try {
    if (_shareState) {
      localStorage.setItem('stockroom_share_state', JSON.stringify({ ..._shareState, _sharedFileId }));
    } else {
      localStorage.removeItem('stockroom_share_state');
    }
  } catch(e) {}
}

// Generate a new AES-GCM share key — used when creating a share link
async function generateShareKey() {
  return crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
}

// Export a share key to base64 for embedding in URL fragment
async function exportShareKey(key) {
  const raw = await crypto.subtle.exportKey('raw', key);
  return btoa(String.fromCharCode(...new Uint8Array(raw)));
}

// Encrypt data with a share key (for /share/data/push)
async function encryptWithShareKey(shareKey, plaintext) {
  const iv         = crypto.getRandomValues(new Uint8Array(12));
  const encoded    = new TextEncoder().encode(plaintext);
  const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, shareKey, encoded);
  const combined   = new Uint8Array(iv.length + ciphertext.byteLength);
  combined.set(iv, 0);
  combined.set(new Uint8Array(ciphertext), iv.length);
  return btoa(String.fromCharCode(...combined));
}

// Decrypt data received from /share/data/pull
async function decryptWithShareKey(shareKey, ciphertext) {
  const combined = Uint8Array.from(atob(ciphertext), c => c.charCodeAt(0));
  const iv       = combined.slice(0, 12);
  const data     = combined.slice(12);
  const plain    = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, shareKey, data);
  return new TextDecoder().decode(plain);
}

// joinViaShareCode — replaced by handleShareJoinLink + completePendingJoin
async function joinViaShareCode(code) {
  // Legacy shim — load scanner.js which defines handleShareJoinLink
  await window._loadScanner().catch(() => {});
  if (typeof handleShareJoinLink === 'function') await handleShareJoinLink(code);
}

async function leaveShare() {
  if (!confirm('Leave this shared household?\n\nYou can rejoin with the same link. Your own households are unaffected.')) return;
  const code = _shareState?.code;
  _shareState   = null;
  _sharedFileId = null;
  _shareKey     = null;
  saveShareState();
  // Clear cached share key
  if (code) {
    try {
      const stored = await _getShareKeys();
      delete stored[code];
      await _setShareKeys(stored);
    } catch(e) {}
  }
  applyTabPermissions();
  toast('Left shared household');
  // Switch to own default profile and reload own data
  loadProfile('default');
  kvSyncNow().catch(() => {});
}

// ── Share state: stored share (old key, migration) ────────
function loadHouseholdShareState() {
  // Migrated to loadShareState() — kept for backwards compat
  loadShareState();
  if (!_shareState) {
    // Check old format
    try {
      const raw = localStorage.getItem('stockroom_household_share');
      if (raw) {
        const d = JSON.parse(raw);
        if (d.fileId) { _sharedFileId = d.fileId; }
      }
    } catch(e) {}
  }
  updateHouseholdShareUI();
}

function saveHouseholdShareState() { saveShareState(); }

async function createInviteCode() {
  if (!kvConnected) {
    toast('Sign in first');
    return;
  }
  const btn = document.querySelector('[onclick="createInviteCode()"]');
  if (btn) { btn.textContent = '⏳ Generating…'; btn.disabled = true; }
  try {
    // Backend uses its stored drive_file_id — no need to send it from frontend
    const res  = await fetch(`${WORKER_URL}/invite/create`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Failed');
    _inviteCode = data.code;
    document.getElementById('invite-code-value').textContent = data.code;
    document.getElementById('invite-code-display').style.display = 'block';
    toast('Invite code created — valid for 24 hours');
  } catch(err) {
    toast('Could not create invite: ' + err.message);
  } finally {
    if (btn) { btn.innerHTML = '<svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-link"></use></svg> Generate invite code'; btn.disabled = false; }
  }
}

function copyInviteCode() {
  const code = document.getElementById('invite-code-value')?.textContent?.trim();
  if (!code) return;
  navigator.clipboard?.writeText(code).then(() => toast('Code copied ✓')).catch(() => {
    prompt('Copy this invite code:', code);
  });
}

async function joinHousehold() {
  const input = document.getElementById('invite-code-input');
  const code  = (input?.value || '').trim().toUpperCase();
  if (code.length < 4) { toast('Enter the full invite code'); return; }

  const btn = document.querySelector('[onclick="joinHousehold()"]');
  if (btn) { btn.textContent = '⏳'; btn.disabled = true; }

  try {
    const res  = await fetch(`${WORKER_URL}/invite/join`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ code }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Invalid code');

    _sharedFileId = data.fileId;
    saveHouseholdShareState();
    updateHouseholdShareUI();
    toast('Joined household ✓ — syncing now');
    // Immediately sync to pull the shared file
    await syncAll();
  } catch(err) {
    toast('Could not join: ' + err.message);
  } finally {
    if (btn) { btn.textContent = 'Join'; btn.disabled = false; }
  }
}

function leaveHousehold() { leaveShare(); }

// ── Proxy read/write used when _sharedFileId is set ──────
async function proxyReadDrive() {
  const code   = _shareState?.code || '';
  const hParam = activeProfile && activeProfile !== 'default' ? `&household=${encodeURIComponent(activeProfile)}` : '';
  const res    = await fetch(`${WORKER_URL}/sync/pull?share=${encodeURIComponent(code)}${hParam}`);
  if (res.status === 404) return null;
  if (res.status === 403) {
    const d = await res.json().catch(() => ({}));
    throw new Error('ACCESS_DENIED: ' + (d.error || 'No access'));
  }
  if (res.status === 503) throw new Error('OWNER_NOT_CONNECTED');
  if (!res.ok) throw new Error('Proxy read failed: ' + res.status);
  return res.json();
}

async function proxyWriteDrive(payload) {
  // Shared user — push via backend, permission validated server-side with share code
  const code   = _shareState?.code || '';
  const hParam = activeProfile && activeProfile !== 'default' ? `&household=${encodeURIComponent(activeProfile)}` : '';
  const res    = await fetch(`${WORKER_URL}/sync/push?share=${encodeURIComponent(code)}${hParam}`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    payload,
  });
  if (res.status === 403) throw new Error('READ_ONLY');
  if (!res.ok) throw new Error('Proxy write failed: ' + res.status);
}

async function proxyGetModifiedTime() {
  if (!_shareState?.code || !_kvEmailHash || (!_kvVerifier && !_kvSessionToken)) return null;
  try {
    const res = await fetchKV(`${WORKER_URL}/share/data/modified`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ guestEmailHash: _kvEmailHash, guestVerifier: _kvVerifier, guestSessionToken: _kvSessionToken, code: _shareState.code, household: activeProfile }),
    });
    if (!res.ok) return null;
    return (await res.json()).modifiedTime || null;
  } catch(e) { return null; }
}

// ── Share Target Management ───────────────────────────────
let _shareTargets     = []; // cached from backend
let _shareTargetType  = 'family';
let _shareTargetPerms = {}; // { householdKey: { stockroom, groceries, reminders, savings, report } }
let _shareTargetColour = HOUSEHOLD_COLOURS[0];
let _shareTargetDone   = false; // true after link is generated — btn becomes Done

function handleShareTargetBtn() {
  if (_shareTargetDone) {
    closeModal('share-target-modal');
  } else {
    saveShareTarget();
  }
}

async function loadShareTargets() {
  if (!WORKER_URL || !isOwner() || !_kvEmailHash || (!_kvVerifier && !_kvSessionToken)) return;
  try {
    const res  = await fetchKV(`${WORKER_URL}/share/list`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken }),
    });
    const data = await res.json();
    _shareTargets = data.targets || [];
    renderShareTargetsList();
  } catch(e) { console.warn('Could not load share targets:', e); }
}

function renderShareTargetsList() {
  const list = document.getElementById('share-targets-list');
  const btn  = document.getElementById('add-share-target-btn');
  if (!list) return;
  if (!isOwner()) { list.closest('#share-targets-section')?.style && (list.closest('#share-targets-section').style.display = 'none'); return; }

  const typeEmoji = { family: '👨‍👩‍👧', cleaner: '🧹', guest: '👤' };
  if (!_shareTargets.length) {
    list.innerHTML = `<p style="font-size:12px;color:var(--muted)">No one has access yet.</p>`;
  } else {
    list.innerHTML = _shareTargets.map(t => {
      const colour    = t.colour || '#e8a838';
      const members   = t.members?.length || 0;
      const expired   = t.expiresAt && Date.now() > new Date(t.expiresAt).getTime();
      const expiryStr = t.expiresAt ? (expired ? '<svg class="icon" aria-hidden="true"><use href="#i-alert-triangle"></use></svg> Link expired' : `Link valid until ${new Date(t.expiresAt).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'})}`) : '';
      return `
      <div style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--surface2);border:1px solid ${expired?'var(--danger)':'var(--border)'};border-radius:10px">
        <div style="width:12px;height:12px;border-radius:50%;background:${colour};flex-shrink:0;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>
        <div style="flex:1;min-width:0">
          <div style="font-size:13px;font-weight:700">${typeEmoji[t.type]||'👤'} ${esc(t.name)}</div>
          <div style="font-size:11px;color:var(--muted);font-family:var(--mono)">${t.type}${members?' · '+members+' member'+(members!==1?'s':''):''}</div>
          ${expiryStr?`<div style="font-size:10px;color:${expired?'var(--danger)':'var(--muted)'};margin-top:2px">${expiryStr}</div>`:''}
        </div>
        <button class="btn btn-ghost btn-sm" onclick="openEditShareTarget('${t.code}')" title="Edit"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
        ${expired
          ? `<button class="btn btn-ghost btn-sm" onclick="refreshShareLink('${t.code}')" title="Refresh link (new 24h window)"><svg class="icon" aria-hidden="true"><use href="#i-refresh-cw"></use></svg></button>`
          : `<button class="btn btn-ghost btn-sm" onclick="copyShareTargetLink('${t.code}')" title="Copy invite link">🔗</button>`
        }
        <button class="btn btn-ghost btn-sm" onclick="resyncSharedData('${t.code}')" title="Re-sync data to guest"><svg class="icon" aria-hidden="true"><use href="#i-share-2"></use></svg></button>
        <button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="deleteShareTarget('${t.code}')"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
      </div>`;
    }).join('');
  }
  if (btn) btn.style.display = _shareTargets.length >= 5 ? 'none' : 'inline-flex';
  const clearBtn = document.getElementById('clear-all-shares-btn');
  if (clearBtn) clearBtn.style.display = _shareTargets.length > 0 ? 'inline-flex' : 'none';
}

function renderShareTargetColourPicker(selectedColour) {
  const el = document.getElementById('share-target-colours');
  if (!el) return;
  el.innerHTML = HOUSEHOLD_COLOURS.map(c => `
    <div onclick="selectShareTargetColour('${c}')"
      data-colour="${c}"
      style="width:26px;height:26px;border-radius:50%;background:${c};cursor:pointer;
             border:3px solid ${c === selectedColour ? 'var(--text)' : 'transparent'};
             transition:border-color 0.15s;box-shadow:0 2px 5px rgba(0,0,0,0.3)"></div>
  `).join('');
}

function selectShareTargetColour(colour) {
  _shareTargetColour = colour;
  document.querySelectorAll('#share-target-colours [data-colour]').forEach(el => {
    el.style.borderColor = el.dataset.colour === colour ? 'var(--text)' : 'transparent';
  });
}

function selectShareType(type, btn) {
  _shareTargetType = type;
  document.querySelectorAll('.share-type-btn').forEach(b => {
    const isActive = b.dataset.type === type;
    b.style.borderColor = isActive ? 'var(--accent)' : 'var(--border)';
    b.style.background  = isActive ? 'rgba(232,168,56,0.1)' : 'transparent';
    b.style.color       = isActive ? 'var(--accent)' : 'var(--muted)';
  });
  // Apply defaults for this type to all households
  Object.keys(_shareTargetPerms).forEach(hKey => {
    const defaults = SHARE_TYPE_DEFAULTS[type] || SHARE_TYPE_DEFAULTS.guest;
    _shareTargetPerms[hKey] = { ...defaults };
  });
  renderShareHouseholdPerms();
}

async function renderShareHouseholdPerms() {
  const container = document.getElementById('share-household-perms');
  if (!container) return;
  const profiles  = await getProfiles();
  const sections  = ['stockroom','groceries','reminders','savings','report'];
  const defaults  = SHARE_TYPE_DEFAULTS[_shareTargetType] || SHARE_TYPE_DEFAULTS.guest;

  container.innerHTML = Object.entries(profiles).map(([key, p]) => {
    const hName  = p.name || (key === 'default' ? 'Home' : key);
    const colour = p.colour || '#e8a838';
    const perms  = _shareTargetPerms[key] || { ...defaults };
    _shareTargetPerms[key] = perms;

    return `<div style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:10px">
        <div style="width:10px;height:10px;border-radius:50%;background:${colour}"></div>
        <strong style="font-size:13px">${esc(hName)}</strong>
      </div>
      <div style="display:flex;flex-direction:column;gap:6px">
        ${sections.map(s => {
          const val = perms[s] || 'none';
          return `<div style="display:flex;align-items:center;justify-content:space-between;gap:8px">
            <span style="font-size:12px;color:var(--muted)">${SECTION_LABELS[s]||s}</span>
            <div style="display:flex;gap:4px">
              ${['none','r','rw'].map(opt => `
                <button onclick="setSharePerm('${key}','${s}','${opt}')"
                  id="spm-${key}-${s}-${opt}"
                  style="padding:3px 8px;border-radius:6px;font-size:11px;cursor:pointer;
                         border:1px solid ${val===opt?'var(--accent)':'var(--border)'};
                         background:${val===opt?'rgba(232,168,56,0.15)':'transparent'};
                         color:${val===opt?'var(--accent)':'var(--muted)'};transition:all 0.15s">
                  ${opt==='none'?'🔒 None':opt==='r'?'👁 View':'✏️ Edit'}
                </button>`).join('')}
            </div>
          </div>`;
        }).join('')}
      </div>
    </div>`;
  }).join('');
}

function setSharePerm(hKey, section, value) {
  if (!_shareTargetPerms[hKey]) _shareTargetPerms[hKey] = {};
  _shareTargetPerms[hKey][section] = value;
  // Update button states
  ['none','r','rw'].forEach(opt => {
    const btn = document.getElementById(`spm-${hKey}-${section}-${opt}`);
    if (!btn) return;
    const active = opt === value;
    btn.style.borderColor = active ? 'var(--accent)' : 'var(--border)';
    btn.style.background  = active ? 'rgba(232,168,56,0.15)' : 'transparent';
    btn.style.color       = active ? 'var(--accent)' : 'var(--muted)';
  });
}

async function openAddShareTarget() {
  if (!isOwner()) { toast('Only the household owner can manage share access'); return; }
  if (_shareTargets.length >= 5) { toast('Maximum 5 share targets reached'); return; }
  _shareTargetType   = 'family';
  _shareTargetPerms  = {};
  _shareTargetColour = HOUSEHOLD_COLOURS[_shareTargets.length % HOUSEHOLD_COLOURS.length];
  const profiles     = await getProfiles();
  const defaults     = SHARE_TYPE_DEFAULTS.family;
  // Always include at least the default household
  const profileKeys  = Object.keys(profiles).length ? Object.keys(profiles) : ['default'];
  profileKeys.forEach(k => { _shareTargetPerms[k] = { ...defaults }; });

  _shareTargetDone   = false;
  document.getElementById('share-target-modal-title').innerHTML = '<svg class="icon icon-md" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-user"></use></svg> Add Person';
  document.getElementById('share-target-code').value = '';
  document.getElementById('share-target-name').value = '';
  document.getElementById('share-target-email').value = '';
  // Hide the "send notification" checkbox — only relevant on edit
  const sendEmailRow = document.getElementById('share-send-email-row');
  if (sendEmailRow) sendEmailRow.style.display = 'none';
  document.getElementById('share-link-section').style.display = 'none';
  document.getElementById('share-target-save-btn').textContent = 'Create & get link';
  selectShareType('family', document.querySelector('.share-type-btn[data-type="family"]'));
  renderShareTargetColourPicker(_shareTargetColour);
  await renderShareHouseholdPerms();
  openModal('share-target-modal');
}

async function openEditShareTarget(code) {
  const target = _shareTargets.find(t => t.code === code);
  if (!target) return;
  _shareTargetType   = target.type || 'family';
  _shareTargetPerms  = JSON.parse(JSON.stringify(target.households || {}));
  _shareTargetColour = target.colour || HOUSEHOLD_COLOURS[0];
  _shareTargetDone   = false;

  document.getElementById('share-target-modal-title').innerHTML = '<svg class="icon icon-md" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-pencil"></use></svg> Edit Access';
  document.getElementById('share-target-code').value = code;
  document.getElementById('share-target-name').value = target.name || '';

  // Restore plain label text (same as create)
  const emailGroup = document.getElementById('share-target-email-group');
  if (emailGroup) emailGroup.style.display = 'block';
  const emailLabel = document.querySelector('#share-target-email-group label');
  if (emailLabel) emailLabel.textContent = 'Their email address';
  const emailHint = document.querySelector('#share-target-email-group p');
  if (emailHint) emailHint.textContent = 'Their email is used to encrypt the share key — it never leaves your device.';
  // Pre-fill saved email
  document.getElementById('share-target-email').value = target.guestEmail || '';

  // Show "send email notification" checkbox (hidden on create screen)
  const sendEmailRow = document.getElementById('share-send-email-row');
  if (sendEmailRow) sendEmailRow.style.display = 'block';
  const sendEmailCb = document.getElementById('share-send-email-cb');
  if (sendEmailCb) sendEmailCb.checked = false;

  document.getElementById('share-link-section').style.display = 'none';
  document.getElementById('share-target-save-btn').textContent = 'Save changes';
  selectShareType(_shareTargetType, document.querySelector(`.share-type-btn[data-type="${_shareTargetType}"]`));
  renderShareTargetColourPicker(_shareTargetColour);
  await renderShareHouseholdPerms();
  openModal('share-target-modal');
}

async function saveShareTarget() {
  const name = document.getElementById('share-target-name').value.trim();
  if (!name) { toast('Enter a name for this person'); return; }
  const code      = document.getElementById('share-target-code').value;
  const colour    = _shareTargetColour;
  const profiles  = await getProfiles();
  const ownerName = settings.email?.split('@')[0] || profiles['default']?.name || 'Home';
  const btn = document.getElementById('share-target-save-btn');
  if (btn) { btn.textContent = '⏳'; btn.disabled = true; }

  try {
    if (code) {
      // Update existing — re-use existing share key
      const res = await fetchKV(`${WORKER_URL}/share/update`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code, name, type: _shareTargetType, colour, households: _shareTargetPerms }),
      });
      if (!res.ok) { const d = await res.json().catch(()=>({})); throw new Error(d.error || 'Update failed'); }
      await pushSharedData(code);

      // Always persist the email address; only send notification if checkbox ticked
      const guestEmailEl  = document.getElementById('share-target-email');
      const guestEmailVal = guestEmailEl?.value.trim();
      const tgt = _shareTargets.find(t => t.code === code);
      if (tgt && guestEmailVal) tgt.guestEmail = guestEmailVal;
      const sendEmailCb = document.getElementById('share-send-email-cb');
      if (guestEmailVal && sendEmailCb?.checked && WORKER_URL) {
        await _sendShareEmail(guestEmailVal, { code, name, type: _shareTargetType, households: _shareTargetPerms, isUpdate: true }).catch(() => {});
      }

      await loadShareTargets();
      closeModal('share-target-modal');
      toast(`✓ ${name}'s access updated`);
    } else {
      // Create new — ECDH key wrapping flow
      const guestEmail = document.getElementById('share-target-email')?.value.trim();
      if (!guestEmail) throw new Error('Enter their email address so their share key can be encrypted for them');

      // 1. Hash guest email → fetch their ECDH public key
      const guestEmailHash = await kvHashEmail(guestEmail);
      const pubRes = await fetchKV(`${WORKER_URL}/user/ecdh-pubkey/get`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: guestEmailHash }),
      });
      if (pubRes.status === 404) throw new Error(`${guestEmail} doesn't have a STOCKROOM account yet — they need to sign up first`);
      if (!pubRes.ok) throw new Error('Could not fetch their encryption key — try again');
      const { publicKeyJwk: guestPubKeyJwk } = await pubRes.json();

      // 2. Load our own ECDH private key
      const ownerPrivKey = await loadEcdhPrivateKey(_kvEmailHash);
      if (!ownerPrivKey) throw new Error('Your encryption key is missing — try signing out and back in');

      // 3. Generate the AES-GCM share key
      const shareKey    = await generateShareKey();
      const shareKeyB64 = await exportShareKey(shareKey);

      // 4. ECDH-wrap the share key for the guest
      const wrappedKey = await ecdhWrapShareKey(ownerPrivKey, guestPubKeyJwk, shareKey);

      // 5. Export our own public key JWK to send alongside (guest needs it to unwrap)
      const ownerPubRes = await fetchKV(`${WORKER_URL}/user/ecdh-pubkey/get`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash }),
      });
      if (!ownerPubRes.ok) throw new Error('Could not fetch your encryption key — try again');
      const { publicKeyJwk: ownerPubKeyJwk } = await ownerPubRes.json();

      // 6. Create share on server
      const res = await fetchKV(`${WORKER_URL}/share/create`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ownerEmailHash: _kvEmailHash,
          ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
          name, type: _shareTargetType, colour, ownerName,
          households: _shareTargetPerms,
          householdNames: Object.fromEntries(
            Object.entries(profiles).map(([k,p]) => [k, p.name||(k==='default'?'Home':k)])
          ),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed');

      // 7. Store ECDH-wrapped key on server for the guest
      const ecdhStoreRes = await fetchKV(`${WORKER_URL}/share/ecdh-key/store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ownerEmailHash: _kvEmailHash,
          ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
          code: data.code,
          guestEmailHash,
          wrappedKey,
          ownerPublicKeyJwk: ownerPubKeyJwk,
        }),
      });
      if (!ecdhStoreRes.ok) throw new Error('Could not store encrypted share key — try again');

      // 8. Cache share key locally and back it up (for owner cross-device recovery)
      try {
        const stored = await _getShareKeys();
        stored[data.code] = shareKeyB64;
        await _setShareKeys(stored);
      } catch(e) {}
      await backupShareKey(data.code, shareKey).catch(e => console.warn('Share key backup failed:', e.message));

      // 9. Push initial shared data
      await pushSharedData(data.code, shareKey);

      // 10. Share created — enable household, close modal, show link in toast
      if (!_householdEnabled) {
        _householdEnabled = true;
        try { localStorage.setItem('stockroom_household', JSON.stringify({ enabled: true, colour: _householdColour })); } catch(e) {}
        connectPresence();
      }

      // Copy link to clipboard and close modal — no "Done" step needed
      const inviteLink = data.link || `${location.origin}${location.pathname}?join=${data.code}`;
      try { await navigator.clipboard.writeText(inviteLink); } catch(e) {}

      // Send invite email if address provided
      const createEmailEl = document.getElementById('share-target-email');
      const createEmailVal = createEmailEl?.value.trim();
      if (createEmailVal && WORKER_URL) {
        await _sendShareEmail(createEmailVal, {
          code: data.code, name, type: _shareTargetType,
          households: _shareTargetPerms, isUpdate: false, inviteLink,
        }).catch(() => {});
      }

      await loadShareTargets();
      closeModal('share-target-modal');
      _shareTargetDone = false; // reset for next use

      toast(`✓ Share created — link copied! Send it to ${name}`);
      if (kvConnected) setTimeout(syncAll, 600);
    }
  } catch(err) {
    console.error('saveShareTarget:', err);
    toast('Could not save: ' + err.message);
    if (btn) { btn.textContent = code ? 'Save changes' : 'Create & get link'; btn.disabled = false; }
  }
}

// Encrypt the raw share key with the owner's data key and store on server.
// This lets the owner recover the share key on any device.
// Recover a share key using a specific data key (used during migration
// when _kvKey has already been updated to v2 but server backup is v1-encrypted).
async function recoverShareKeyWithOldKey(code, dataKey) {
  // 1. Try local cache first — no decryption needed
  try {
    const stored = await _getShareKeys();
    const keyB64 = stored[code];
    if (keyB64) {
      const raw = Uint8Array.from(atob(keyB64), c => c.charCodeAt(0));
      return await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
    }
  } catch(e) {}

  // 2. Fetch server backup and decrypt with the provided (old) key
  try {
    const res = await fetchKV(`${WORKER_URL}/share/key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, code }),
    });
    if (!res.ok) return null;
    const { encryptedShareKey } = await res.json();
    if (!encryptedShareKey) return null;
    const keyB64 = await kvDecrypt(dataKey, encryptedShareKey);
    const raw    = Uint8Array.from(atob(keyB64), c => c.charCodeAt(0));
    return await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
  } catch(e) {
    console.warn('recoverShareKeyWithOldKey failed:', e.message);
    return null;
  }
}

async function backupShareKey(code, shareKey) {
  if (!_kvKey || !_kvEmailHash || !_kvVerifier) return;
  const raw        = await crypto.subtle.exportKey('raw', shareKey);
  const ciphertext = await kvEncrypt(_kvKey, btoa(String.fromCharCode(...new Uint8Array(raw))));
  await fetchKV(`${WORKER_URL}/share/key/store`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, code, encryptedShareKey: ciphertext }),
  });
}

// Recover share key from server — decrypts with owner's data key.
// Returns a CryptoKey or null.
async function recoverShareKey(code) {
  if (!_kvKey || !_kvEmailHash || !_kvVerifier) return null;
  try {
    const res = await fetchKV(`${WORKER_URL}/share/key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, code }),
    });
    if (!res.ok) return null;
    const { encryptedShareKey } = await res.json();
    if (!encryptedShareKey) return null;
    const keyB64 = await kvDecrypt(_kvKey, encryptedShareKey);
    const raw    = Uint8Array.from(atob(keyB64), c => c.charCodeAt(0));
    const sk     = await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
    // Cache locally so subsequent pushes don't need a round-trip
    try {
      const stored = await _getShareKeys();
      stored[code] = keyB64;
      await _setShareKeys(stored);
    } catch(e) {}
    return sk;
  } catch(e) {
    console.warn('recoverShareKey failed:', e.message);
    return null;
  }
}

// Push owner's data re-encrypted with the share key for a specific share code
async function pushSharedData(code, shareKey) {
  if (!_kvKey) return;
  // Get or restore share key — local cache first, then server backup
  let sk = shareKey;
  if (!sk) {
    try {
      const stored = await _getShareKeys();
      const keyB64 = stored[code];
      if (keyB64) {
        const raw = Uint8Array.from(atob(keyB64), c => c.charCodeAt(0));
        sk = await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
      }
    } catch(e) {}
  }
  // If still missing, try recovering from server
  if (!sk) {
    sk = await recoverShareKey(code);
  }
  if (!sk) {
    console.warn('pushSharedData: no share key for', code, '— skipping');
    return;
  }

  // Find which households this share code covers
  const target = _shareTargets.find(t => t.code === code);
  const households = target?.households ? Object.keys(target.households) : ['default'];

  for (const hKey of households) {
    try {
      // Build full payload for this household (same shape as kvPush so guest merge works)
      const allProfiles = await getProfiles();
      const hProfile    = allProfiles[hKey] || allProfiles['default'];
      const hItems      = hKey === activeProfile ? items       : (hProfile?.items      || []);
      const hSettings   = hKey === activeProfile ? settings    : (hProfile?.settings   || {});
      const hGroceries  = hKey === activeProfile ? groceryItems: (hProfile?.groceries  || []);
      const hReminders  = hKey === activeProfile ? reminders   : (hProfile?.reminders  || []);
      const hDepts      = hKey === activeProfile ? groceryDepts: (hProfile?.departments|| []);

      // Respect per-section permissions — only push what the guest can view
      const perms = target?.households?.[hKey] || {};
      const canSeeStockroom  = perms.stockroom  && perms.stockroom  !== 'none';
      const canSeeGroceries  = perms.groceries  && perms.groceries  !== 'none';
      const canSeeReminders  = perms.reminders  && perms.reminders  !== 'none';

      const payload = JSON.stringify({
        items:       canSeeStockroom ? hItems     : [],
        settings:    hSettings,
        groceries:   canSeeGroceries ? hGroceries : [],
        reminders:   canSeeReminders ? hReminders : [],
        departments: canSeeGroceries ? hDepts     : [],
        lastSynced:  new Date().toISOString(),
      });
      const ciphertext  = await encryptWithShareKey(sk, payload);
      const authFields  = _kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier };
      await fetchKV(`${WORKER_URL}/share/data/push`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ownerEmailHash: _kvEmailHash, ...authFields, code, household: hKey, ciphertext }),
      });
    } catch(e) { console.warn('pushSharedData failed for', hKey, e.message); }
  }
}

// Push shared data for ALL active share targets (called from kvPush)
async function pushAllSharedData() {
  if (!_shareTargets?.length) return;
  for (const target of _shareTargets) {
    await pushSharedData(target.code).catch(e => console.warn('pushAllSharedData failed for', target.code, e.message));
    // Also fulfil any pending rewrap requests from new guests
    await _fulfilPendingRewraps(target.code).catch(e => console.warn('rewrap failed for', target.code, e.message));
  }
}

// When a new guest accepts an invite before the owner had their ECDH pubkey,
// they store a rewrap request. Owner's app picks this up on next sync and wraps for them.
async function _fulfilPendingRewraps(code) {
  if (!_kvEmailHash || (!_kvVerifier && !_kvSessionToken)) return;
  const authFields = _kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier };
  try {
    const res = await fetchKV(`${WORKER_URL}/share/ecdh-key/pending-rewraps`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, ...authFields, code }),
    });
    if (!res.ok) return;
    const { requests } = await res.json();
    if (!requests?.length) return;

    const ownerPrivKey = await loadEcdhPrivateKey(_kvEmailHash);
    if (!ownerPrivKey) return;
    const ownerPubRes = await fetchKV(`${WORKER_URL}/user/ecdh-pubkey/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash }),
    });
    if (!ownerPubRes.ok) return;
    const { publicKeyJwk: ownerPubKeyJwk } = await ownerPubRes.json();

    // Recover the share key for this code
    const sk = await (async () => {
      try {
        const stored = await _getShareKeys();
        const b64 = stored[code];
        if (b64) {
          const raw = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
          return crypto.subtle.importKey('raw', raw, { name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
        }
      } catch(e) {}
      return recoverShareKey(code);
    })();
    if (!sk) return;

    for (const req of requests) {
      if (!req.guestEmailHash || !req.guestPublicKeyJwk) continue;
      try {
        const wrappedKey = await ecdhWrapShareKey(ownerPrivKey, req.guestPublicKeyJwk, sk);
        await fetchKV(`${WORKER_URL}/share/ecdh-key/store`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ownerEmailHash: _kvEmailHash, ...authFields, code,
            guestEmailHash: req.guestEmailHash, wrappedKey, ownerPublicKeyJwk: ownerPubKeyJwk,
          }),
        });
      } catch(e) { console.warn('[share] rewrap failed for', req.guestEmailHash, e.message); }
    }
  } catch(e) { /* non-critical */ }
}

async function resyncSharedData(code) {
  toast('Syncing to guest…');
  try {
    await pushSharedData(code);
    toast('Synced ✓');
  } catch(e) {
    toast('Sync failed: ' + e.message);
  }
}

// ── Share diagnostic — run window.shareDiag() on owner device ──
window.shareDiag = async function(code) {
  const out = [];
  out.push('=== SHARE DIAGNOSTIC ===');
  out.push(`_kvKey:       ${_kvKey ? 'SET' : 'NULL — owner not signed in with key'}`);
  out.push(`_kvEmailHash: ${_kvEmailHash || '(empty)'}`);
  out.push(`_kvVerifier:  ${_kvVerifier ? _kvVerifier.slice(0,8)+'…' : '(empty)'}`);
  out.push(`_shareTargets: ${JSON.stringify((_shareTargets||[]).map(t=>({code:t.code,households:Object.keys(t.households||{})})))}`);

  const localKeys = await _getShareKeys();
  out.push(`local share keys: ${Object.keys(localKeys).join(', ') || 'none'}`);

  const codes = code ? [code] : (_shareTargets||[]).map(t=>t.code);
  if (!codes.length) { out.push('No share targets found'); return out.join('\n'); }

  for (const c of codes) {
    out.push(`\n--- Code: ${c} ---`);
    out.push(`local key present: ${!!localKeys[c]}`);

    // Try server key backup
    try {
      const r = await fetch(`${WORKER_URL}/share/key/get`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ownerEmailHash:_kvEmailHash, verifier:_kvVerifier, code:c}),
      });
      out.push(`share/key/get status: ${r.status}`);
      const d = await r.json();
      out.push(`share/key/get body: ${JSON.stringify(d)}`);
    } catch(e) { out.push(`share/key/get error: ${e.message}`); }

    // Try share/data/push with a test payload to see if auth works
    // (just check whether we can get a share key at all)
    let sk = null;
    if (localKeys[c]) {
      try {
        const raw = Uint8Array.from(atob(localKeys[c]), c2=>c2.charCodeAt(0));
        sk = await crypto.subtle.importKey('raw', raw, {name:'AES-GCM',length:256}, true, ['encrypt','decrypt']);
        out.push(`local key import: OK`);
      } catch(e) { out.push(`local key import FAILED: ${e.message}`); }
    }
    if (!sk) {
      out.push(`attempting server key recovery…`);
      sk = await recoverShareKey(c).catch(e => { out.push(`recoverShareKey error: ${e.message}`); return null; });
      out.push(`server key recovery: ${sk ? 'OK' : 'FAILED — no backup stored'}`);
    }

    // Check if share_data exists on server using owner credentials
    if (_kvEmailHash && _kvVerifier) {
      try {
        // share/data/pull requires guest credentials — use owner as a proxy check via share/list
        const shareRecord = _shareTargets.find(t => t.code === c);
        out.push(`share record in _shareTargets: ${shareRecord ? 'YES' : 'NO'}`);
        out.push(`share members: ${JSON.stringify(shareRecord?.members || [])}`);
        out.push(`share households: ${JSON.stringify(Object.keys(shareRecord?.households || {}))}`);
      } catch(e) { out.push(`share record check error: ${e.message}`); }
    }
  }
  out.push('\nTo recover a share with no key: run window.rebuildShare("CODE") to delete and recreate it.');
  out.push('=== END ===');
  return out.join('\n');
};

// Delete a broken share target (no key) and toast instructions to recreate
window.clearBrokenShare = async function(code) {
  if (!code) { console.error('Usage: clearBrokenShare("CODE")'); return; }
  if (!confirm(`Delete share target ${code}? The guest will lose access. You'll need to send them a new link.`)) return;
  try {
    const res = await fetchKV(`${WORKER_URL}/share/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code }),
    });
    if (!res.ok) { const d = await res.json(); throw new Error(d.error || 'Delete failed'); }
    // Remove from local share keys
    try {
      const stored = await _getShareKeys();
      delete stored[code];
      await _setShareKeys(stored);
    } catch(e) {}
    await loadShareTargets();
    toast('Share removed — create a new one from Settings → Households');
  } catch(e) {
    console.error('clearBrokenShare failed:', e.message);
  }
};

async function refreshShareLink(code) {
  const target = _shareTargets.find(t => t.code === code);
  if (!target) return;
  try {
    const res = await fetchKV(`${WORKER_URL}/share/refresh`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code }),
    });
    if (!res.ok) throw new Error('Could not refresh');
    const baseLink = `${location.origin}${location.pathname}?join=${code}`;
    await navigator.clipboard.writeText(baseLink).catch(()=>{});
    toast('New link copied ✓ (valid 24h)');
    await loadShareTargets();
  } catch(err) { toast('Could not refresh link: ' + err.message); }
}

async function deleteShareTarget(code) {
  if (!confirm('Remove this person\'s access? They will no longer be able to sync your data.')) return;
  try {
    const res = await fetchKV(`${WORKER_URL}/share/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code }),
    });
    if (!res.ok) { const d = await res.json().catch(()=>({})); throw new Error(d.error || 'Could not delete'); }
    // Remove local share key
    try {
      const stored = await _getShareKeys();
      delete stored[code];
      await _setShareKeys(stored);
    } catch(e) {}
    toast('Access removed ✓');
    await loadShareTargets();
  } catch(err) { toast('Could not remove: ' + err.message); }
}

function clearAllShares() {
  if (!_shareTargets?.length) { toast('No shares to clear'); return; }
  if (!confirm(`Remove all ${_shareTargets.length} share link${_shareTargets.length !== 1 ? 's' : ''}? ` +
    'Guests will immediately lose access and their local share data will be cleared on next sync.')) return;
  requireReauth('Confirm your identity to remove all shares.', _doClearAllShares, { passkeyAllowed: true });
}

async function _doClearAllShares() {
  let failed = 0;
  for (const target of (_shareTargets || [])) {
    try {
      const res = await fetchKV(`${WORKER_URL}/share/delete`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ownerEmailHash: _kvEmailHash, verifier: _kvVerifier, sessionToken: _kvSessionToken, code: target.code }),
      });
      if (!res.ok) failed++;
    } catch(e) { failed++; }
  }
  // Clear local share key cache
  try { localStorage.removeItem('stockroom_share_keys'); } catch(e) {}
  await loadShareTargets();
  toast(failed ? `Cleared with ${failed} error(s) — check console` : 'All shares removed ✓');
}

// Send share invite or update email via server
async function _sendShareEmail(guestEmail, { code, name, type, households, isUpdate = false, inviteLink = '' }) {
  if (!guestEmail || !WORKER_URL) return;
  try {
    const authFields = _kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier };
    await fetchKV(`${WORKER_URL}/share/send-email`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ownerEmailHash: _kvEmailHash, ...authFields,
        guestEmail, code, name, type, households, isUpdate, inviteLink,
        ownerName: settings.email?.split('@')[0] || 'Your household',
      }),
    });
  } catch(e) { console.warn('share email failed:', e.message); }
}

function copyShareLink() {
  const link = document.getElementById('share-link-value')?.textContent?.trim();
  if (!link) return;
  navigator.clipboard?.writeText(link).then(() => toast('Link copied ✓')).catch(() => prompt('Copy this link:', link));
}

function copyShareTargetLink(code) {
  const link = `${location.origin}${location.pathname}?join=${code}`;
  navigator.clipboard.writeText(link).then(() => toast('Invite link copied ✓')).catch(() => prompt('Copy this link:', link));
}

function updateHouseholdShareUI() {
  const joinedSection = document.getElementById('household-joined-section');
  if (!joinedSection) return;
  if (_shareState) {
    joinedSection.style.display = 'block';
    const statusEl = document.getElementById('joined-status-text');
    if (statusEl) statusEl.textContent = `✓ Joined ${_shareState.ownerName || 'a household'}'s STOCKROOM as ${_shareState.type || 'guest'}`;
  } else {
    joinedSection.style.display = 'none';
  }
}

async function renderSettingsHouseholdList() {
  const list = document.getElementById('settings-household-list');
  if (!list) return;
  const profiles = await getProfiles();
  list.innerHTML = Object.entries(profiles).map(([key, p]) => {
    const isActive = key === activeProfile;
    const colour   = p.colour || '#e8a838';
    const count    = (p.items || []).length;
    return `<div style="display:flex;align-items:center;gap:8px;padding:10px 12px;background:${isActive?'rgba(232,168,56,0.08)':'var(--surface2)'};border:2px solid ${isActive?colour:'var(--border)'};border-radius:10px">
      <div style="width:10px;height:10px;border-radius:50%;background:${colour};flex-shrink:0"></div>
      <div style="flex:1;font-size:13px;font-weight:${isActive?'700':'400'};color:${isActive?colour:'var(--text)'}">
        ${esc(p.name || key)}${isActive?' ✓':''}
        <span style="font-size:11px;font-weight:400;color:var(--muted);margin-left:6px">${count} item${count!==1?'s':''}</span>
      </div>
      ${!isActive ? `<button class="btn btn-ghost btn-sm" onclick="switchProfileFromSettings('${key}')">Switch</button>` : ''}
      <button class="btn btn-ghost btn-sm" onclick="openHouseholdEdit('${key}')" title="Edit"><svg class="icon" aria-hidden="true"><use href="#i-pencil"></use></svg></button>
      ${key !== 'default' ? `<button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="deleteProfile('${key}');renderSettingsHouseholdList()"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>` : ''}
    </div>`;
  }).join('');
}

async function switchProfileFromSettings(key) {
  await switchProfile(key);
  renderSettingsHouseholdList();
}

async function addProfileFromSettings() {
  const input = document.getElementById('settings-new-household-name');
  const name  = input?.value.trim() || '';
  // Temporarily set the new-profile-name so addProfile can read it
  const nameEl = document.getElementById('new-profile-name');
  if (nameEl) nameEl.value = name;
  await addProfile();
  if (input) input.value = '';
  renderSettingsHouseholdList();
}

function initHouseholdSettingsUI() {
  loadHouseholdSettings();
  loadShareState();
  updateHouseholdShareUI();
  renderHouseholdColourPicker(); // still used by presence system internally
  if (_householdEnabled) connectPresence();
  applyTabPermissions();
  renderSettingsHouseholdList();
  if (isOwner() && WORKER_URL) loadShareTargets();
}

// ── User ID ───────────────────────────────────────────────
async function getHouseholdUserId() {
  // Use the KV email hash if signed in — stable across devices, no raw email sent
  if (_kvEmailHash) return 'u_' + _kvEmailHash.slice(0, 12);
  // Fall back to settings email hash
  const email = settings.email || '';
  if (email) {
    const msgBuffer  = new TextEncoder().encode(email.toLowerCase().trim());
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    return 'u_' + Array.from(new Uint8Array(hashBuffer)).slice(0, 6).map(b => b.toString(16).padStart(2,'0')).join('');
  }
  // No email — per-device ID
  let id = localStorage.getItem('stockroom_user_id');
  if (!id) {
    id = 'u_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    localStorage.setItem('stockroom_user_id', id);
  }
  return id;
}

// ── SSE connection ────────────────────────────────────────
async function connectPresence() {
  if (!WORKER_URL || !_householdEnabled) return;
  disconnectPresence(); // close any existing

  const userId = await getHouseholdUserId();
  const url    = `${WORKER_URL}/presence-stream?userId=${encodeURIComponent(userId)}`;

  try {
    _presenceSSE = new EventSource(url);

    _presenceSSE.onmessage = async e => {
      try {
        const data = JSON.parse(e.data);
        if (data.type === 'presence') {
          _otherPresence = {};
          const myId = await getHouseholdUserId();
          (data.users || []).forEach(u => {
            if (u.userId !== myId) _otherPresence[u.userId] = u;
          });
          renderPresenceAvatars();
          // Update shopping list if we're on it — items may have changed
          if (_currentViewName === 'shopping' || document.getElementById('shopping-panel')?.style.display !== 'none') {
            scheduleRender('shopping');
          }
        }
      } catch(err) { console.warn('Presence parse error', err); }
    };

    _presenceSSE.onerror = () => {
      // SSE dropped — try to reconnect after 10s
      disconnectPresence();
      setTimeout(() => { if (_householdEnabled) connectPresence(); }, 10000);
    };

    // Push our own presence immediately and then every 25s
    pushPresence();
    _presencePingTimer = setInterval(pushPresence, 25000);

    const statusEl = document.getElementById('household-status');
    if (statusEl) statusEl.innerHTML = '<span style="color:var(--ok)">●</span> Connected — presence active';
  } catch(e) {
    console.warn('Presence SSE failed:', e);
  }
}

function disconnectPresence() {
  if (_presenceSSE) { _presenceSSE.close(); _presenceSSE = null; }
  clearInterval(_presencePingTimer);
  _otherPresence = {};
  renderPresenceAvatars();
  const statusEl = document.getElementById('household-status');
  if (statusEl) statusEl.textContent = '';
}

async function pushPresence() {
  if (!WORKER_URL || !_householdEnabled) return;
  if (!_kvEmailHash || (!_kvVerifier && !_kvSessionToken)) return; // wait for auth
  const name   = _householdName || settings.email?.split('@')[0] || 'You';
  const initials = name.slice(0,2).toUpperCase();
  try {
    await fetch(`${WORKER_URL}/presence-update`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        userId:       await getHouseholdUserId(),
        emailHash:    _kvEmailHash,
        verifier:     _kvVerifier,
        sessionToken: _kvSessionToken,
        name,
        initials,
        colour:   _householdColour,
        view:     _currentViewName,
        ts:       new Date().toISOString(),
      }),
    });
  } catch(e) { /* offline — ignore */ }
}

// ── Presence avatars ──────────────────────────────────────
function renderPresenceAvatars() {
  const bar = document.getElementById('presence-bar');
  if (!bar) return;
  const users = Object.values(_otherPresence);
  if (!users.length) { bar.style.display = 'none'; return; }
  bar.style.display = 'flex';
  bar.innerHTML = users.map(u => {
    const viewLabel = { stock:'Stockroom', grocery:'Groceries', shopping:'Shopping', reminders:'Reminders', savings:'Savings', report:'Report', settings:'Settings' }[u.view] || u.view;
    const isShopping = u.view === 'shopping';
    return `<div class="presence-avatar ${isShopping ? 'shopping' : ''}"
      style="background:${u.colour};color:#111"
      title="${u.name} — viewing ${viewLabel}">
      ${u.initials}
      <div class="presence-dot"></div>
      <div class="presence-tooltip">${esc(u.name)} · ${viewLabel}</div>
    </div>`;
  }).join('');
}

// Hook into showView and switchTo* to push presence on navigation
// Note: we patch _currentViewName directly rather than wrapping functions,
// since function re-declaration with hoisting causes infinite recursion.

// ── Shopping list: show who else is viewing ───────────────
function getShoppingPresenceBar() {
  const shoppers = Object.values(_otherPresence).filter(u => u.view === 'shopping');
  if (!shoppers.length) return '';
  return `<div style="display:flex;align-items:center;gap:8px;padding:10px 14px;background:rgba(232,168,56,0.08);border:1px solid rgba(232,168,56,0.2);border-radius:10px;margin-bottom:14px;font-size:13px">
    <div style="display:flex;gap:4px">${shoppers.map(u =>
      `<div class="presence-avatar shopping" style="background:${u.colour};color:#111;width:24px;height:24px;font-size:10px">${u.initials}<div class="presence-dot"></div></div>`
    ).join('')}</div>
    <span style="color:var(--warn)">${shoppers.map(u=>esc(u.name)).join(', ')} ${shoppers.length===1?'is':'are'} also on the shopping list</span>
  </div>`;
}

async function init() {
  // ── Restore device ID from IDB before anything else ──────────────────────
  // getOrCreateDeviceId() is synchronous, so the async IDB restore in the IIFE
  // below may not have completed yet. If localStorage was cleared, a new random
  // device ID would be generated, making all IDB setup-flag lookups miss.
  // We await the IDB restore here so the correct device ID is in localStorage
  // before any setup-flag checks run.
  if (!localStorage.getItem('stockroom_device_id')) {
    try {
      const savedId = await dbGet('settings', 'stockroom_device_id');
      if (savedId) {
        localStorage.setItem('stockroom_device_id', savedId);
        sessionStorage.setItem('stockroom_device_id_session', savedId);
      }
    } catch(e) {}
  }

  // ── Restore kv_session from IDB if localStorage was cleared ──────────────
  // stockroom_kv_session is the gating key for kvRestoreSession(). We also
  // persist it to IDB so a cookie/localStorage clear doesn't force a re-login.
  if (!localStorage.getItem('stockroom_kv_session')) {
    try {
      const savedSession = await dbGet('settings', 'stockroom_kv_session');
      if (savedSession) {
        localStorage.setItem('stockroom_kv_session', savedSession);
      }
    } catch(e) {}
  }

  // Load all data from IndexedDB (migrates from localStorage on first run)
  await loadData();

  // ── Migration: stamp updatedAt on any item that doesn't have one ──
  const now = new Date().toISOString();
  let migrated = false;
  items.forEach(item => {
    if (!item.updatedAt) { item.updatedAt = now; migrated = true; }
  });
  if (migrated) {
    await saveData();
  }

  // Restore KV session
  const _diagSession = localStorage.getItem('stockroom_kv_session');
  const _diagSecret  = localStorage.getItem('stockroom_device_secret');
  const _diagEmail   = localStorage.getItem('stockroom_remembered_email');
  const _diagConsent = localStorage.getItem('stockroom_cookie_consent');
  const _diagFallback= localStorage.getItem('stockroom_kv_key_fallback');
  const _diagLsKey   = localStorage.getItem('stockroom_kv_session_key');
  const kvRestored = await kvRestoreSession();
  if (kvRestored) {
    // Always ensure the data key is available before syncing.
    // If _kvKey is already set (trusted device / 4h cache), this is instant.
    // If not, kvEnsureKey will prompt cleanly BEFORE the stockroom shows.
    if (!_kvKey) {
      // Show overlay so user knows something is happening
      showDataLoadingOverlay('Unlocking your data…');
      const keyOk = await kvEnsureKey().catch(() => false);
      hideDataLoadingOverlay();
      if (keyOk) {
        setTimeout(() => kvSyncNow(true), 400);
      }
      // If keyOk is false user cancelled — leave them on current screen with sign-in option
    } else {
      setTimeout(() => kvSyncNow(true), 800);
    }
  }

  // Restore Dropbox state (disabled in KV build)

  await loadShareState();

  // Check for ?join=CODE — show loading state immediately so user doesn't see normal wizard
  const _joinCode = new URLSearchParams(location.search).get('join');
  if (_joinCode) {
    const _step1b = document.getElementById('wizard-step-1b');
    if (_step1b) {
      document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
      _step1b.innerHTML = '<div style="font-size:52px;margin-bottom:16px">\uD83C\uDFE0</div><h1 style="font-size:24px;font-weight:700;margin-bottom:8px">Joining household…</h1><p style="color:var(--muted);font-size:14px">Checking your invite link</p>';
      _step1b.classList.add('active');
    }
  }

  handleURLAction(); // processes ?join= and replaces loading state with join screen

  buildCountryGrid();
  buildSettingsCountrySelect();
  buildTagSettingsRows();
  buildTagFilterBar();
  wizardCountry = settings.country || 'GB';
  selectCountry(wizardCountry);

  const seen        = kvConnected; // signed-in users skip the wizard
  const wizardStep  = localStorage.getItem('stockroom_wizard_step');

  const countrySet  = await getCountrySetForDevice();
  const protectSeen = await getProtectSeenForDevice();

  if (_joinCode) {
    // join flow handled by handleURLAction above
  } else if (kvConnected) {
    if (!_kvKey) {
      showDataLoadingOverlay('Unlocking your data…');
      const keyOk = await kvEnsureKey().catch(() => false);
      hideDataLoadingOverlay();
      if (!keyOk) { showKvLogin(); return; }
    }
    // Always go through MFA first — protect/country/stockroom routing happens inside
    document.body.classList.remove('wizard-active');
    document.getElementById('wizard').style.display = 'none';
    window.scrollTo(0, 0);
    await _mfaGate(async () => {
      // Re-read flags after _mfaGate's server pull
      const ps = await getProtectSeenForDevice();
      const cs = await getCountrySetForDevice();
      if (!ps) {
        showProtectDataScreen([]);
      } else if (!cs) {
        document.body.classList.add('wizard-active');
        document.getElementById('wizard').style.display = 'flex';
        document.querySelectorAll('.wizard-step').forEach(s => s.classList.remove('active'));
        document.getElementById('wizard-step-2').classList.add('active');
        wizardCountry = settings.country || 'GB';
        requestAnimationFrame(() => { buildCountryGrid(); selectCountry(wizardCountry); });
      } else {
        showDataLoadingOverlay('Loading your Stockroom…');
        scheduleRender(...RENDER_REGIONS);
        try { await kvSyncNow(true); } catch(e) {}
        hideDataLoadingOverlay();
        // Show install banner after sync — settings._installDismissed now authoritative
        setTimeout(maybeShowInstallBanner, 2000);
      }
    });
  } else if (wizardStep === '2') {
    localStorage.removeItem('stockroom_wizard_step');
    wizardNext();
  } else if (countrySet) {
    wizardNext();
  } else {
    // No session, no seen flag — show login (will auto-skip to auth if remembered)
    showKvLogin();
  }

  updateSyncUI();
  renderSettingsForUser();
  _showAmazonBanners();
  initPasskeyUI();
  loadCompactView();
  loadFilterPanelState();
  updateFab(_currentView || 'stock'); // init FAB for current view
  _updateSidebarProfile();            // populate sidebar household name

  // Init profiles — migrate if needed
  const existingProfiles = await getProfiles();
  const profileKeys = Object.keys(existingProfiles);
  if (!profileKeys.length) {
    // First run — wrap current items/settings into default profile
    const profiles = { default: { name: 'Home', items: JSON.parse(JSON.stringify(items)), settings: JSON.parse(JSON.stringify(settings)) } };
    await saveProfiles(profiles);
  }
  activeProfile = localStorage.getItem('stockroom_active_profile') || 'default';
  await loadProfile(activeProfile);

  scheduleRender(...RENDER_REGIONS);
  loadNotifSettings();
  initHouseholdSettingsUI();
  updateHeaderGreeting();
  await loadReminders();
  await loadNotes();
  await loadGrocery();
  await checkGroceryRecurring();
  renderReminders(); // pre-render for badge count

  setTimeout(() => { lastAutoSync = Date.now(); checkCloudAhead(); }, 1500);
  checkPendingSWSync();
  if (migrated) setTimeout(syncAll, 1500);
  // Shared users need an explicit sync on load
  if (_shareState && !migrated) setTimeout(() => kvSyncNow(true), 800);
  setTimeout(checkExportReminder, 5000);
  setTimeout(checkScheduledEmail, 2000);
  setTimeout(checkLowStockNotifications, 3000);
  setTimeout(checkReminderNotifications, 3500);
}

// ═══════════════════════════════════════════════════════════
//  STORE PRICES (multiple stores per item)
//  Moved from scanner.js — needed as soon as the item modal opens
// ═══════════════════════════════════════════════════════════

function renderStorePricesSection(item) {
  const section = document.getElementById('store-prices-section');
  const list    = document.getElementById('store-prices-list');
  if (!section || !list) return;
  const prices = item?.storePrices || [];
  section.style.display = 'block';
  if (!prices.length) {
    list.innerHTML = `<p style="font-size:12px;color:var(--muted);padding:4px 0">No store prices added yet. Click + Add Store to compare prices across different shops.</p>`;
    return;
  }
  list.innerHTML = prices.map((sp, i) => `
    <div style="display:flex;gap:8px;align-items:center;padding:6px 0;border-bottom:1px solid var(--border)">
      <input type="text" value="${esc(sp.store)}" placeholder="Store name"
        style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px"
        onchange="updateStorePrice(${i},'store',this.value)">
      <input type="text" value="${esc(sp.price)}" placeholder="Price"
        style="width:80px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px;font-family:var(--mono)"
        onchange="updateStorePrice(${i},'price',this.value)">
      <button onclick="removeStorePrice(${i})" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:16px;padding:2px 4px"
        onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--muted)'"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
    </div>`).join('');
  if (prices.length > 1) {
    const parsed = prices.map((sp,i) => ({ i, val: parsePriceValue(sp.price) })).filter(x => x.val !== null);
    if (parsed.length > 1) {
      const min = Math.min(...parsed.map(x => x.val));
      const cheapestIdx = parsed.find(x => x.val === min)?.i;
      if (cheapestIdx !== undefined) {
        const rows = list.querySelectorAll('div');
        if (rows[cheapestIdx]) rows[cheapestIdx].style.background = 'rgba(76,187,138,0.08)';
      }
    }
  }
}

function addStorePriceRow() {
  tempStorePrices.push({ store: '', price: '' });
  renderTempStorePrices();
}

function updateStorePrice(idx, field, val) {
  if (tempStorePrices[idx]) tempStorePrices[idx][field] = val;
}

function removeStorePrice(idx) {
  tempStorePrices.splice(idx, 1);
  renderTempStorePrices();
}

function renderTempStorePrices() {
  const section = document.getElementById('store-prices-section');
  const list    = document.getElementById('store-prices-list');
  if (!section || !list) return;
  section.style.display = 'block';
  if (!tempStorePrices.length) {
    list.innerHTML = `<p style="font-size:12px;color:var(--muted);padding:4px 0">No store prices added yet.</p>`;
    return;
  }
  list.innerHTML = tempStorePrices.map((sp, i) => `
    <div style="display:flex;gap:8px;align-items:center;padding:6px 0;border-bottom:1px solid var(--border)">
      <input type="text" value="${esc(sp.store)}" placeholder="Store name"
        style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px"
        oninput="updateStorePrice(${i},'store',this.value)">
      <input type="text" value="${esc(sp.price)}" placeholder="e.g. £12.99"
        style="width:90px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:5px 8px;color:var(--text);font-size:12px;font-family:var(--mono)"
        oninput="updateStorePrice(${i},'price',this.value)">
      <button onclick="removeStorePrice(${i})" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:16px;padding:2px 4px"
        onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--muted)'"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
    </div>`).join('');
}

// ═══════════════════════════════════════════════════════════
//  QUICK ADD — moved from scanner.js
// ═══════════════════════════════════════════════════════════

function openQuickAdd() {
  document.getElementById('quick-add-input').value = '';
  document.getElementById('quick-add-preview').style.display = 'none';
  document.getElementById('quick-add-chips').innerHTML = '';
  openModal('quick-add-modal');
  setTimeout(() => document.getElementById('quick-add-input')?.focus(), 100);
}

function parseQuickAddNames() {
  return document.getElementById('quick-add-input').value
    .split(',').map(s => s.trim()).filter(s => s.length > 0);
}

function updateQuickAddPreview() {
  const names   = parseQuickAddNames();
  const preview = document.getElementById('quick-add-preview');
  const chips   = document.getElementById('quick-add-chips');
  const btn     = document.getElementById('quick-add-save-btn');
  if (!names.length) { preview.style.display = 'none'; return; }
  preview.style.display = 'block';
  chips.innerHTML = names.map(n =>
    `<span style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;background:var(--surface2);border:1px solid var(--border);border-radius:99px;font-size:12px;color:var(--text)">
      📦 ${esc(n)}
    </span>`
  ).join('');
  if (btn) btn.innerHTML = `<svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-zap"></use></svg> Add ${names.length} Item${names.length !== 1 ? 's' : ''}`;
}

async function saveQuickAdd() {
  const names = parseQuickAddNames();
  if (!names.length) { toast('Enter at least one item name'); return; }
  const now = new Date().toISOString();
  names.forEach(name => {
    items.push({
      id: uid(), name, category: 'Other', cadence: 'monthly',
      qty: 1, months: 1, url: '', store: '', notes: '',
      rating: null, imageUrl: null, logs: [], storePrices: [],
      quickAdded: true, updatedAt: now,
    });
  });
  await saveData();
  closeModal('quick-add-modal');
  scheduleRender('grid', 'dashboard', 'filters', 'shopping');
  _syncQueue.enqueue();
  toast(`${names.length} item${names.length !== 1 ? 's' : ''} added — complete their details when ready`);
}

// ═══════════════════════════════════════════════════════════
//  SCAN CHOOSER — shown after a barcode is scanned
// ═══════════════════════════════════════════════════════════

let scannedProductName  = '';
let scannedProductImage = null;

function openScanChooser(name, imageUrl) {
  scannedProductName  = name;
  scannedProductImage = imageUrl;
  const nameEl = document.getElementById('scan-chooser-name');
  if (nameEl) nameEl.textContent = name;
  openModal('scan-chooser-modal');
}

function scanChooserQuickAdd() {
  closeModal('scan-chooser-modal');
  openQuickAdd();
  const ta = document.getElementById('quick-add-input');
  if (ta) { ta.value = scannedProductName; updateQuickAddPreview(); }
}

function scanChooserLogPurchase() {
  closeModal('scan-chooser-modal');
  openLogPicker();
  const search = document.getElementById('log-picker-search');
  if (search) { search.value = scannedProductName; filterLogPicker(scannedProductName); }
}

function scanChooserFullAdd() {
  closeModal('scan-chooser-modal');
  openAddModal();
  document.getElementById('f-name').value = scannedProductName;
  if (scannedProductImage) {
    pendingImageUrl = scannedProductImage;
    showImagePreview(scannedProductImage, 'Image found via barcode');
  }
}

// ═══════════════════════════════════════════════════════════
//  LOG PURCHASE PICKER — moved from scanner.js
// ═══════════════════════════════════════════════════════════

function openLogPicker() {
  renderLogPickerList('');
  document.getElementById('log-picker-search').value = '';
  openModal('log-picker-modal');
  setTimeout(() => document.getElementById('log-picker-search').focus(), 100);
}

function renderLogPickerList(filter) {
  const list = document.getElementById('log-picker-list');
  if (!list) return;
  const q      = filter.toLowerCase().trim();
  const sorted = [...items]
    .filter(i => !i.quickAdded)
    .filter(i => !q || i.name.toLowerCase().includes(q))
    .sort((a, b) => {
      const la = a.logs?.at(-1)?.date || '0000';
      const lb = b.logs?.at(-1)?.date || '0000';
      return lb.localeCompare(la);
    });
  if (!sorted.length) {
    list.innerHTML = `<p style="font-size:13px;color:var(--muted);text-align:center;padding:20px">No items found</p>`;
    return;
  }
  list.innerHTML = sorted.map(item => {
    const s        = calcStock(item);
    const daysLeft = s?.daysLeft ?? null;
    const color    = STATUS_COLOR[getStatus(s?.pct ?? null, settings.threshold)];
    const lastLog  = item.logs?.at(-1);
    return `<button onclick="pickItemForLog('${item.id}')"
      style="display:flex;align-items:center;gap:12px;padding:10px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;cursor:pointer;text-align:left;width:100%;transition:border-color 0.15s"
      onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
      <div style="width:4px;height:36px;border-radius:2px;background:${color};flex-shrink:0"></div>
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(item.name)}</div>
        <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-top:2px">
          ${daysLeft !== null ? `${daysLeft}d left` : 'no data'}
          ${lastLog ? ` · last bought ${fmtDate(lastLog.date)}` : ''}
          ${item.store ? ` · ${esc(item.store)}` : ''}
        </div>
      </div>
      <span style="font-size:18px;flex-shrink:0">+</span>
    </button>`;
  }).join('');
}

function filterLogPicker(val) { renderLogPickerList(val); }

function pickItemForLog(id) {
  closeModal('log-picker-modal');
  openLogModal(id);
}

// ═══════════════════════════════════════════════════════════
//  SHARE ITEM — moved from scanner.js
// ═══════════════════════════════════════════════════════════

let sharingItem = null;

function shareItem(id) {
  const item = items.find(i => i.id === id);
  if (!item) return;
  sharingItem = item;
  const subtitle = document.getElementById('share-modal-subtitle');
  if (subtitle) subtitle.textContent = item.name;
  drawShareCard(item);
  openModal('share-modal');
}

function drawShareCard(item) {
  const canvas = document.getElementById('share-canvas');
  if (!canvas) return;
  const W = 600, H = 340;
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext('2d');
  const grad = ctx.createLinearGradient(0, 0, W, H);
  grad.addColorStop(0, '#0f1117'); grad.addColorStop(1, '#1a1d27');
  ctx.fillStyle = grad; ctx.fillRect(0, 0, W, H);
  const glow = ctx.createRadialGradient(0, 0, 0, 0, 0, 260);
  glow.addColorStop(0, 'rgba(232,168,56,0.07)'); glow.addColorStop(1, 'rgba(232,168,56,0)');
  ctx.fillStyle = glow; ctx.fillRect(0, 0, W, H);
  ctx.strokeStyle = '#2e3350'; ctx.lineWidth = 1; ctx.strokeRect(0.5, 0.5, W-1, H-1);
  const headerH = 44;
  ctx.fillStyle = '#1a1d27'; ctx.fillRect(0, 0, W, headerH);
  ctx.strokeStyle = '#2e3350'; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(0, headerH); ctx.lineTo(W, headerH); ctx.stroke();
  ctx.fillStyle = '#e8a838'; ctx.font = 'bold 13px monospace'; ctx.fillText('📦 STOCKROOM', 20, 28);
  ctx.fillStyle = '#7880a0'; ctx.font = '11px monospace'; ctx.textAlign = 'right';
  ctx.fillText('Household Consumables Tracker', W - 20, 28); ctx.textAlign = 'left';
  const contentY = headerH + 24;
  ctx.fillStyle = 'rgba(120,128,160,0.2)';
  const catText = (item.category || 'Other').toUpperCase();
  ctx.font = '10px monospace';
  const catW = ctx.measureText(catText).width + 16;
  _roundRect(ctx, 20, contentY, catW, 20, 4); ctx.fill();
  ctx.fillStyle = '#7880a0'; ctx.fillText(catText, 28, contentY + 14);
  ctx.fillStyle = '#e8eaf2'; ctx.font = 'bold 28px system-ui, sans-serif';
  const nameLines = _wrapText(ctx, item.name || '', W - 48);
  nameLines.slice(0, 2).forEach((line, i) => ctx.fillText(line, 20, contentY + 46 + i * 36));
  const afterName = contentY + 46 + Math.min(nameLines.length, 2) * 36;
  const pillY = afterName + 16; let pillX = 20;
  const drawPill = (label, value, bg, textCol) => {
    if (!value) return;
    ctx.font = '11px system-ui, sans-serif';
    const fullText = label ? `${label}: ${value}` : value;
    const pw = ctx.measureText(fullText).width + 20;
    ctx.fillStyle = bg; ctx.beginPath();
    ctx.roundRect ? ctx.roundRect(pillX, pillY, pw, 24, 12) : _roundRect(ctx, pillX, pillY, pw, 24, 12);
    ctx.fill(); ctx.fillStyle = textCol; ctx.fillText(fullText, pillX + 10, pillY + 16);
    pillX += pw + 8;
  };
  const history = getPriceHistory(item);
  const lastPrice = history.length ? (history[history.length-1].raw || `£${history[history.length-1].price.toFixed(2)}`) : null;
  if (lastPrice) drawPill('Price', lastPrice, 'rgba(76,187,138,0.15)', '#4cbb8a');
  if (item.store) drawPill('From', item.store, 'rgba(91,141,238,0.15)', '#5b8dee');
  if (item.qty && item.qty !== 1) drawPill('Qty', `×${item.qty}`, 'rgba(120,128,160,0.15)', '#7880a0');
  if (item.rating) drawPill('', '★'.repeat(item.rating) + '☆'.repeat(5 - item.rating), 'rgba(232,168,56,0.15)', '#e8a838');
  const barY = H - 30;
  ctx.fillStyle = '#0f1117'; ctx.fillRect(0, barY, W, 30);
  ctx.strokeStyle = '#2e3350'; ctx.beginPath(); ctx.moveTo(0, barY); ctx.lineTo(W, barY); ctx.stroke();
  if (item.url) {
    try {
      const domain = new URL(item.url).hostname.replace('www.', '');
      ctx.fillStyle = '#5b8dee'; ctx.font = '11px monospace'; ctx.fillText(`🛒 ${domain}`, 20, barY + 20);
    } catch(e) {}
  }
  ctx.fillStyle = '#4a5070'; ctx.font = '10px monospace'; ctx.textAlign = 'right';
  ctx.fillText('stckrm.fly.dev', W - 20, barY + 20); ctx.textAlign = 'left';
}

function _wrapText(ctx, text, maxWidth) {
  const words = text.split(' '), lines = [];
  let current = '';
  for (const word of words) {
    const test = current ? current + ' ' + word : word;
    if (ctx.measureText(test).width > maxWidth && current) { lines.push(current); current = word; }
    else { current = test; }
  }
  if (current) lines.push(current);
  return lines;
}

function _roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y); ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r); ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h); ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r); ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

async function doShareImage() {
  const canvas = document.getElementById('share-canvas');
  if (!canvas || !sharingItem) return;
  canvas.toBlob(async blob => {
    const file = new File([blob], `${sharingItem.name.replace(/\s+/g, '-')}.png`, { type: 'image/png' });
    if (navigator.canShare && navigator.canShare({ files: [file] })) {
      try { await navigator.share({ title: sharingItem.name, text: buildShareText(sharingItem), files: [file] }); closeModal('share-modal'); }
      catch(e) { if (e.name !== 'AbortError') _downloadShareImage(canvas, sharingItem); }
    } else { _downloadShareImage(canvas, sharingItem); }
  }, 'image/png');
}

function _downloadShareImage(canvas, item) {
  const a = document.createElement('a');
  a.download = `${item.name.replace(/\s+/g, '-')}.png`;
  a.href = canvas.toDataURL('image/png'); a.click();
  toast('Image saved — share from your downloads');
}

async function doShareItemLink() {
  if (!sharingItem) return;
  const link = generateItemShareLink(sharingItem);
  if (navigator.share) {
    try { await navigator.share({ title: `${sharingItem.name} — via STOCKROOM`, text: `I'm using ${sharingItem.name} in STOCKROOM:`, url: link }); closeModal('share-modal'); return; }
    catch(e) { if (e.name === 'AbortError') return; }
  }
  fallbackCopy(link);
}

async function doShareLink() {
  const url = sharingItem?.url || window.location.href;
  if (navigator.share) {
    try { await navigator.share({ title: sharingItem?.name, text: buildShareText(sharingItem), url }); closeModal('share-modal'); }
    catch(e) { if (e.name !== 'AbortError') fallbackCopy(url); }
  } else { fallbackCopy(url); }
}

async function doShareText() {
  if (!sharingItem) return;
  fallbackCopy(buildShareText(sharingItem));
}

function buildShareText(item) {
  if (!item) return '';
  const history = getPriceHistory(item);
  const lastPrice = history.length ? (history[history.length-1].raw || `£${history[history.length-1].price.toFixed(2)}`) : null;
  const storePrices = (item.storePrices || []).filter(sp => sp.store && sp.price);
  const lines = [`📦 ${item.name}`];
  if (item.category) lines.push(`Category: ${item.category}`);
  if (lastPrice)     lines.push(`Price: ${lastPrice}`);
  if (item.store)    lines.push(`Available at: ${item.store}`);
  if (storePrices.length > 1) { lines.push('Price comparison:'); storePrices.forEach(sp => lines.push(`  ${sp.store}: ${sp.price}`)); }
  if (item.qty && item.qty !== 1) lines.push(`Pack size: ×${item.qty}`);
  if (item.rating)   lines.push(`Rated: ${'★'.repeat(item.rating)}${'☆'.repeat(5 - item.rating)}`);
  if (item.notes)    lines.push(`Note: ${item.notes}`);
  if (item.url)      lines.push(`Buy here: ${item.url}`);
  lines.push(''); lines.push('Shared via STOCKROOM — stckrm.fly.dev');
  return lines.join('\n');
}

function fallbackCopy(text) {
  navigator.clipboard?.writeText(text)
    .then(() => { toast('Copied to clipboard ✓'); closeModal('share-modal'); })
    .catch(() => {
      const ta = document.createElement('textarea');
      ta.value = text; ta.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(ta); ta.select(); document.execCommand('copy');
      document.body.removeChild(ta); toast('Copied to clipboard ✓'); closeModal('share-modal');
    });
}

// ═══════════════════════════════════════════════════════════
//  ITEM SHARE LINK — receive items shared via URL
// ═══════════════════════════════════════════════════════════

function generateItemShareLink(item) {
  const payload = {
    v: 1, name: item.name, category: item.category, cadence: item.cadence,
    qty: item.qty, months: item.months, url: item.url || '', store: item.store || '',
    notes: item.notes || '', rating: item.rating || null,
    storePrices: (item.storePrices || []).filter(sp => sp.store && sp.price),
  };
  const encoded = btoa(unescape(encodeURIComponent(JSON.stringify(payload))));
  return `${window.location.origin}/?item=${encoded}`;
}

function checkIncomingItem() {
  const params  = new URLSearchParams(location.search);
  const encoded = params.get('item');
  if (!encoded) return false;
  history.replaceState(null, '', location.pathname);
  try {
    const payload = JSON.parse(decodeURIComponent(escape(atob(encoded))));
    if (!payload?.name) return false;
    setTimeout(() => showIncomingItemPrompt(payload), 700);
    return true;
  } catch(e) { return false; }
}

function showIncomingItemPrompt(payload) {
  const priceStr = (payload.storePrices || []).map(sp => `${sp.store}: ${sp.price}`).join(' · ');
  let domainStr = '';
  try { if (payload.url) domainStr = new URL(payload.url).hostname.replace('www.', ''); } catch(e) {}
  const el = document.createElement('div');
  el.id = 'incoming-item-overlay';
  el.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:9999;display:flex;align-items:flex-end;justify-content:center;padding:20px';
  el.onclick = e => { if (e.target === el) closeIncomingItem(); };
  el.innerHTML = `
    <div style="background:var(--surface);border-radius:16px 16px 12px 12px;padding:24px;max-width:440px;width:100%;border:1px solid var(--border);box-shadow:0 -4px 32px rgba(0,0,0,0.4)">
      <div style="font-size:11px;font-weight:700;color:var(--accent);font-family:var(--mono);letter-spacing:1px;margin-bottom:8px">📦 ITEM SHARED WITH YOU</div>
      <div style="font-size:20px;font-weight:700;color:var(--text);margin-bottom:4px">${esc(payload.name)}</div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:12px;line-height:1.8">
        ${payload.category ? `<span style="font-family:var(--mono)">${esc(payload.category)}</span>` : ''}
        ${payload.store    ? ` · From <strong style="color:var(--text)">${esc(payload.store)}</strong>` : ''}
        ${priceStr         ? ` · ${esc(priceStr)}` : ''}
        ${payload.months   ? ` · ${payload.months}mo supply` : ''}
      </div>
      ${payload.notes ? `<div style="font-size:12px;color:var(--muted);font-style:italic;margin-bottom:14px;padding:8px;background:var(--surface2);border-radius:6px"><svg class="icon" aria-hidden="true"><use href="#i-message-square"></use></svg> ${esc(payload.notes)}</div>` : ''}
      ${domainStr ? `<div style="margin-bottom:14px"><a href="${esc(payload.url)}" target="_blank" rel="noopener" style="font-size:12px;color:var(--accent2)">🛒 ${esc(domainStr)} ↗</a></div>` : ''}
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">
        <button class="btn btn-primary" style="flex:1" id="incoming-add-btn">+ Add to my stockroom</button>
        <button class="btn btn-ghost" id="incoming-edit-btn">✏️ Add &amp; set up</button>
        <button class="btn btn-ghost btn-sm" onclick="closeIncomingItem()">Dismiss</button>
      </div>
    </div>`;
  document.body.appendChild(el);
  document.getElementById('incoming-add-btn').onclick  = () => addIncomingItem(payload, false);
  document.getElementById('incoming-edit-btn').onclick = () => addIncomingItem(payload, true);
}

function closeIncomingItem() {
  document.getElementById('incoming-item-overlay')?.remove();
}

async function addIncomingItem(payload, openEdit) {
  closeIncomingItem();
  const newItem = {
    id: uid(), name: payload.name, category: payload.category || 'Other',
    cadence: payload.cadence || 'monthly', qty: payload.qty || 1, months: payload.months || 1,
    url: payload.url || '', store: payload.store || '', notes: payload.notes || '',
    rating: payload.rating || null, storePrices: payload.storePrices || [],
    imageUrl: null, logs: [], quickAdded: false, updatedAt: new Date().toISOString(),
  };
  items.push(newItem);
  await saveData();
  scheduleRender('grid', 'dashboard', 'shopping');
  _syncQueue.enqueue();
  if (openEdit) { setTimeout(() => { openEditModal(newItem.id); enableItemEdit(); }, 300); }
  else { toast(`"${payload.name}" added ✓`); }
}

// ═══════════════════════════════════════════════════════════
//  SHARE JOIN FLOW — moved from scanner.js
// ═══════════════════════════════════════════════════════════

async function handleShareJoinLink(code) {
  updateSyncPill('syncing');
  try {
    const probe = await fetchKV(`${WORKER_URL}/share/join`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code }),
    });
    const probeData = await probe.json();
    if (probe.status === 410) { toast('This invite link has expired — ask the owner for a new one'); updateSyncPill('error'); return; }
    if (!probe.ok && !probeData.requiresAuth) throw new Error(probeData.error || 'Invalid link');
    _pendingJoinCode  = code.toUpperCase();
    _pendingShareMeta = probeData;
    if (kvConnected && _kvEmailHash && (_kvVerifier || _kvSessionToken)) { await completePendingJoin(); return; }
    showShareAuthGate(probeData);
  } catch(err) { updateSyncPill('error'); toast('Invalid invite link — ' + err.message); }
}

function showShareAuthGate(meta) {
  const wizard = document.getElementById('wizard');
  if (!wizard) return;
  wizard.style.display = 'flex';
  const step1 = document.getElementById('wizard-step-1');
  if (!step1) return;
  const hCount = Object.keys(meta.households || {}).length;
  const groupName = meta.name ? `the <strong>${esc(meta.name)}</strong> group` : 'this household';
  step1.innerHTML = `
    <div style="margin-bottom:12px;color:var(--accent)"><svg aria-hidden="true" style="width:44px;height:44px"><use href="#i-home"></use></svg></div>
    <h1 style="font-size:22px;font-weight:700;margin-bottom:6px">You're invited!</h1>
    <p style="color:var(--muted);font-size:13px;line-height:1.6;margin-bottom:16px">
      <strong style="color:var(--text)">${esc(meta.ownerName||'Someone')}</strong> has invited you
      to access ${hCount} household${hCount!==1?'s':''} as a member of ${groupName}.
    </p>
    <div style="text-align:left;margin-bottom:12px">
      <div class="form-group" style="margin-bottom:10px">
        <label class="form-label">Email address</label>
        <input class="form-input" id="share-gate-email" type="email" placeholder="you@example.com" autocomplete="email">
      </div>
      <div class="form-group" style="margin-bottom:8px">
        <label class="form-label">Passphrase</label>
        <input class="form-input" id="share-gate-pass" type="password" placeholder="Your passphrase" autocomplete="current-password">
      </div>
    </div>
    <button class="btn btn-primary btn-xl full" style="margin-bottom:8px" onclick="shareGateSignIn()">Sign in &amp; Accept →</button>
    <button class="btn btn-ghost btn-xl full" style="font-size:13px;margin-bottom:8px" onclick="shareGateRegister()">Create new account &amp; Accept →</button>
    <p id="share-gate-error" style="font-size:12px;color:var(--danger);margin-top:6px;display:none"></p>
  `;
  step1.classList.add('active');
}

async function shareGateSignIn() {
  const email = document.getElementById('share-gate-email')?.value.trim();
  const pass  = document.getElementById('share-gate-pass')?.value;
  const errEl = document.getElementById('share-gate-error');
  if (!email || !pass) { if(errEl){errEl.textContent='Enter email and passphrase';errEl.style.display='block';} return; }
  try {
    const emailHash = await kvHashEmail(email);
    const verifier  = await kvMakeVerifier(pass, emailHash);
    const res = await fetchKV(`${WORKER_URL}/user/verify`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ emailHash, verifier }) });
    const d = await res.json();
    if (res.status === 404) throw new Error('Account not found — use Create new account');
    if (!res.ok) throw new Error(d.error || 'Sign-in failed');
    const key = await kvDeriveKey(email, pass);
    await kvStoreSession(email, emailHash, verifier, key);
    // Ensure ECDH keypair is ready BEFORE attempting join (required for key unwrapping)
    await ensureEcdhKeypair(emailHash).catch(e => console.warn('ensureEcdhKeypair:', e.message));
    await _trustIfRemembered(email, emailHash, verifier, key);
    await completePendingJoin();
  } catch(err) { if(errEl){errEl.textContent=err.message;errEl.style.display='block';} }
}

async function shareGateRegister() {
  const email = document.getElementById('share-gate-email')?.value.trim();
  const pass  = document.getElementById('share-gate-pass')?.value;
  const errEl = document.getElementById('share-gate-error');
  if (!email || !pass) { if(errEl){errEl.textContent='Enter email and passphrase';errEl.style.display='block';} return; }
  if (pass.length < 8) { if(errEl){errEl.textContent='Passphrase must be at least 8 characters';errEl.style.display='block';} return; }
  try {
    const emailHash = await kvHashEmail(email);
    const verifier  = await kvMakeVerifier(pass, emailHash);
    const res = await fetchKV(`${WORKER_URL}/user/register`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ emailHash, verifier, email }) });
    const d = await res.json();
    if (!res.ok) throw new Error(d.error || 'Registration failed');
    const kdfSalt = generateKdfSalt();
    const wrapKey = await derivePassphraseWrapKeyV2(pass, emailHash, kdfSalt);
    const dataKey = await generateDataKeyV2Extractable();
    const passphraseEnvelope = await wrapDataKeyV2(dataKey, wrapKey);
    const saltB64 = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))));
    const recoveryCodes = generateRecoveryCodes(10);
    const recoveryEnvelopes = await buildRecoveryEnvelopesV2(recoveryCodes, dataKey, emailHash);
    const storeRes = await fetchKV(`${WORKER_URL}/key/store`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ emailHash, verifier, salt: saltB64, passphraseEnvelope, recoveryEnvelopes, kdfSalt }) });
    if (!storeRes.ok) throw new Error('Could not store key envelopes — try again');
    _kvKey = dataKey;
    await kvStoreSession(email, emailHash, verifier, dataKey);
    await showEmailVerification(email, emailHash, async () => {
      await _trustIfRemembered(email, emailHash, verifier, dataKey);
      await completePendingJoin();
    });
  } catch(err) { if(errEl){errEl.textContent=err.message;errEl.style.display='block';} }
}

async function completePendingJoin() {
  if (!_pendingJoinCode) return;
  const code = _pendingJoinCode;
  try {
    const res  = await fetchKV(`${WORKER_URL}/share/join`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        code,
        guestEmailHash:    _kvEmailHash,
        ...(_kvSessionToken ? { guestSessionToken: _kvSessionToken } : { guestVerifier: _kvVerifier }),
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Invalid invite link — it may have expired');
    // Server returned 200 but with ok:false — means it needs auth (shouldn't happen here but guard it)
    if (data.requiresAuth) throw new Error('Authentication required — please sign in first');

    const ecdhRes = await fetchKV(`${WORKER_URL}/share/ecdh-key/get`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        guestEmailHash: _kvEmailHash,
        ...(_kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }),
        code,
      }),
    });

    // If no wrapped key exists yet, request the owner to re-wrap on their next sync
    if (ecdhRes.status === 404) {
      // Store a pending-rewrap request on the server so owner's app picks it up
      await fetchKV(`${WORKER_URL}/share/ecdh-key/request-rewrap`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          guestEmailHash: _kvEmailHash,
          ...(_kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }),
          code,
        }),
      }).catch(() => {}); // non-blocking
      throw new Error('Your invite is being set up — ask the owner to open STOCKROOM, then tap this link again');
    }

    if (!ecdhRes.ok) {
      const ed = await ecdhRes.json().catch(() => ({}));
      throw new Error(ed.error || 'Could not retrieve your share key — ask the owner to re-send the invite');
    }
    const { wrappedKey, ownerPublicKeyJwk } = await ecdhRes.json();
    const guestPrivKey = await loadEcdhPrivateKey(_kvEmailHash);
    if (!guestPrivKey) throw new Error('Your encryption key is missing — sign out and back in to regenerate it');
    const shareKey    = await ecdhUnwrapShareKey(guestPrivKey, ownerPublicKeyJwk, wrappedKey);
    const shareKeyB64 = await exportShareKey(shareKey);
    try {
      const stored = await _getShareKeys();
      stored[code] = shareKeyB64;
      await _setShareKeys(stored);
    } catch(e) {}
    _shareState = { ...data, code };
    _shareKey   = shareKey;
    saveShareState();
    _pendingJoinCode  = null;
    _pendingShareMeta = null;
    localStorage.setItem('stockroom_seen', '1');
    localStorage.setItem('stockroom_country_set', '1');
    document.body.classList.remove('wizard-active');
    document.getElementById('wizard').style.display = 'none';
    applyTabPermissions();
    updateSyncPill('syncing');
    await kvSyncNow();
    scheduleRender(...RENDER_REGIONS);
    toast(`✓ Joined ${data.ownerName || 'household'}'s STOCKROOM`);
  } catch(err) {
    const msg = err.message || 'Unknown error — please try again';
    toast('Could not join: ' + msg);
    updateSyncPill('error');
  }
}

function showShareWizard(shareData) { showShareAuthGate(shareData); }
async function acceptShareAndContinue() { await completePendingJoin(); }
function showShareJoinConfirm(shareData) { toast(`✓ Joined ${shareData.ownerName || 'household'}'s STOCKROOM as ${shareData.type || 'guest'}`); }

// ═══════════════════════════════════════════════════════════
//  URL ACTION HANDLER — moved from scanner.js
// ═══════════════════════════════════════════════════════════
function handleURLAction() {
  // ── Share join link: ?join=CODE ──────────────────────────
  const joinParams = new URLSearchParams(location.search);
  const joinCode   = joinParams.get('join');
  if (joinCode) {
    history.replaceState(null, '', location.pathname);
    if (typeof handleShareJoinLink === 'function') handleShareJoinLink(joinCode.toUpperCase());
    return;
  }

  // Check for incoming shared item (defined in scanner.js)
  if (typeof checkIncomingItem === 'function' && checkIncomingItem()) return;

  const params = new URLSearchParams(location.search);
  const action = params.get('action');
  if (!action) return;

  history.replaceState(null, '', location.pathname);

  setTimeout(() => {
    if (action === 'quick-add') {
      if (typeof openQuickAdd === 'function') openQuickAdd();
    } else if (action === 'log-purchase') {
      if (typeof openLogPicker === 'function') openLogPicker();
    } else if (action === 'shopping') {
      showShoppingListInline();
    } else if (action === 'scan') {
      sessionStorage.setItem('barcode_target', 'scan-chooser');
      openBarcodeScanner();
    } else if (action === 'reminder-sync') {
      const id    = params.get('id')    || '';
      const date  = params.get('date')  || today();
      const token = params.get('token') || '';
      if (id && token && typeof applyReminderReplaced === 'function') applyReminderReplaced(id, date, token);
    } else if (action === 'unsubscribe') {
      if (typeof handleUnsubscribe === 'function') handleUnsubscribe();
    } else if (action === 'share') {
      // Share target — lazy-load scanner.js which has full handling
      window._loadScanner().then(() => handleURLAction()).catch(() => {});
    }
  }, 600);
}

init();

// ═══════════════════════════════════════════
//  SECURE NOTES
// ═══════════════════════════════════════════

function _noteUid() {
  return 'n' + Date.now().toString(36) + Math.random().toString(36).slice(2,7);
}

// ── Re-lock helpers ───────────────────────
function _relockAllNotes() {
  _noteUnlocked.forEach((state, noteId) => {
    clearTimeout(state.inactivityTimer);
  });
  _noteUnlocked.clear();
  // If editor is open on a locked note, close it back to grid
  if (_editingNoteId) {
    const n = notes.find(x => x.id === _editingNoteId);
    if (n && n.locked) {
      _closeNoteEditorImmediate();
      renderNotes();
    }
  }
}

function _startNoteInactivityTimer(noteId) {
  const state = _noteUnlocked.get(noteId);
  if (!state) return;
  clearTimeout(state.inactivityTimer);
  state.inactivityTimer = setTimeout(() => {
    _noteUnlocked.delete(noteId);
    // If this note is currently open, close editor to grid
    if (_editingNoteId === noteId) {
      _closeNoteEditorImmediate();
      renderNotes();
      toast('Note locked after inactivity');
    }
  }, 30 * 60 * 1000); // 30 minutes
}

function _resetNoteActivity(noteId) {
  const state = _noteUnlocked.get(noteId);
  if (!state) return;
  state.lastActivity = Date.now();
  _startNoteInactivityTimer(noteId);
}

// ── IDB persistence ───────────────────────
// (loadNotes / saveNotes defined earlier near saveReminders)

// ── Render notes grid ─────────────────────
async function renderNotes() {
  await loadNotes();
  const grid  = document.getElementById('notes-grid');
  const empty = document.getElementById('notes-empty');
  if (!grid) return;

  const q = (_notesSearch || '').toLowerCase().trim();
  const now = Date.now();
  const thirtyDaysMs = 30 * 24 * 60 * 60 * 1000;

  // Filter
  let visible = notes.filter(n => {
    if (_notesFilter === 'trash')    return !!n.deletedAt;
    if (_notesFilter === 'archived') return !n.deletedAt && !!n.archived;
    if (_notesFilter === 'pinned')   return !n.deletedAt && !n.archived && !!n.pinned;
    return !n.deletedAt && !n.archived; // 'all'
  });

  // Purge notes deleted >30 days ago
  const before = notes.length;
  notes = notes.filter(n => !n.deletedAt || (now - new Date(n.deletedAt).getTime()) < thirtyDaysMs);
  if (notes.length !== before) await saveNotes();

  // Search
  if (q) {
    visible = visible.filter(n =>
      n.title.toLowerCase().includes(q) ||
      (!n.locked && (n.body || '').toLowerCase().includes(q))
    );
  }

  if (!visible.length) {
    grid.innerHTML = '';
    if (empty) empty.style.display = 'block';
    return;
  }
  if (empty) empty.style.display = 'none';

  // Sort: pinned first, then by updatedAt desc
  visible.sort((a, b) => {
    if (a.pinned && !b.pinned) return -1;
    if (!a.pinned && b.pinned) return 1;
    return new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0);
  });

  // Trash filter: show empty state instead of add-note prompt
  if (_notesFilter === 'trash' && !visible.length) {
    grid.innerHTML = '';
    if (empty) { empty.style.display = 'block'; empty.innerHTML = `<div style="margin-bottom:12px;opacity:0.4"><svg style="width:48px;height:48px" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M10 11v6"/><path d="M14 11v6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M3 6h18"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg></div><div style="font-size:16px;font-weight:700;margin-bottom:8px;color:var(--text);font-family:var(--sans)">Trash is empty</div><p style="font-size:13px;line-height:1.6">Deleted notes appear here for 30 days.</p>`; }
    return;
  }
  if (_notesFilter === 'archived' && !visible.length) {
    grid.innerHTML = '';
    if (empty) { empty.style.display = 'block'; empty.innerHTML = `<div style="margin-bottom:12px;opacity:0.4"><svg style="width:48px;height:48px" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect width="20" height="5" x="2" y="3" rx="1"/><path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8"/><path d="M10 12h4"/></svg></div><div style="font-size:16px;font-weight:700;margin-bottom:8px;color:var(--text);font-family:var(--sans)">No archived notes</div><p style="font-size:13px;line-height:1.6">Archived notes appear here.</p>`; }
    return;
  }

  // Trash: show "Empty trash" button
  const trashBar = _notesFilter === 'trash' && visible.length
    ? `<div style="display:flex;justify-content:flex-end;margin-bottom:10px"><button class="btn btn-ghost btn-sm" style="color:var(--danger)" onclick="emptyNotesTrash()"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg> Empty trash</button></div>`
    : '';

  // Render pinned section header if mixed
  const hasPinned   = visible.some(n => n.pinned);
  const hasUnpinned = visible.some(n => !n.pinned);
  const showHeaders = hasPinned && hasUnpinned && _notesFilter === 'all';

  let html = trashBar;
  let inPinned = false;

  visible.forEach(n => {
    if (showHeaders && n.pinned && !inPinned) {
      html += `<div class="notes-section-label" style="padding:8px 0 4px"><svg class="icon" aria-hidden="true"><use href="#i-pin"></use></svg> Pinned</div>`;
      inPinned = true;
    }
    if (showHeaders && !n.pinned && inPinned) {
      html += `<div class="notes-section-label" style="padding:8px 0 4px"><svg class="icon" aria-hidden="true" style="vertical-align:-3px"><use href="#i-notebook-pen"></use></svg> Notes</div>`;
      inPinned = false;
    }
    html += _noteCardHTML(n);
  });

  grid.innerHTML = html;
}

function _noteCardHTML(n) {
  const isUnlocked = _noteUnlocked.has(n.id);
  const bgStyle    = n.colour ? `background:${n.colour};` : '';
  const unlocked   = _noteUnlocked.get(n.id);
  const rawPreview = n.locked && !isUnlocked ? '' : (unlocked?.body || n.body || '');
  const _tmpDiv = document.createElement('div'); _tmpDiv.innerHTML = rawPreview;
  const previewText = (_tmpDiv.innerText || _tmpDiv.textContent || '').trim();
  // Show up to 2 lines worth (~120 chars)
  const preview = previewText.slice(0, 120) + (previewText.length > 120 ? '…' : '');

  const isSelected = _noteSelected.has(n.id);

  // Status icons
  const icons = [];
  if (n.pinned)  icons.push('📌');
  if (n.locked)  icons.push(isUnlocked ? '<svg class="icon" aria-hidden="true"><use href="#i-unlock"></use></svg>' : '<svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg>');
  if (n.archived) icons.push('📦');
  if (n.deletedAt) {
    const daysLeft = Math.max(0, 30 - Math.round((Date.now()-new Date(n.deletedAt).getTime())/86400000));
    icons.push(`<span style="font-size:10px;color:var(--danger);font-family:var(--mono)">🗑 ${daysLeft}d</span>`);
  }
  const linkedReminder = reminders.find(r => r.linkedNoteId === n.id);
  if (linkedReminder) {
    const days = getReminderDaysUntil(linkedReminder);
    const col  = days !== null && days < 0 ? 'var(--danger)' : days !== null && days <= 7 ? 'var(--warn)' : 'var(--muted)';
    icons.push(`<span style="color:${col};font-size:12px">🔔</span>`);
  }
  if (n.tickBoxesVisible && n.tickBoxes) {
    const total = Object.keys(n.tickBoxes).length;
    const checked = Object.values(n.tickBoxes).filter(Boolean).length;
    if (total > 0) icons.push(`<span style="font-size:10px;color:var(--ok);font-family:var(--mono)">☑${checked}/${total}</span>`);
  }
  const iconHtml = icons.length ? `<span style="display:flex;align-items:center;gap:4px;flex-shrink:0">${icons.join('')}</span>` : '';

  // Secure-now button for unlocked secure notes
  const secureNowBtn = (n.locked && isUnlocked)
    ? `<button onclick="event.stopPropagation();secureLockNote('${n.id}')" class="note-secure-now-btn" title="Lock again">🔒 Secure now</button>`
    : '';

  const selectedStyle = isSelected ? 'border-color:var(--accent);background:rgba(232,168,56,0.08);' : '';

  return `<div class="note-row${isSelected?' note-selected':''}" style="${bgStyle}${selectedStyle}"
    data-note-id="${n.id}"
    onclick="_noteRowClick('${n.id}', event)"
    ontouchstart="_noteRowTouchStart('${n.id}', event)"
    ontouchend="_noteRowTouchEnd('${n.id}', event)"
    role="button" tabindex="0">
    <div style="display:flex;align-items:flex-start;gap:10px">
      ${_noteSelected.size > 0 ? `<div class="note-select-indicator${isSelected?' checked':''}" onclick="event.stopPropagation();_toggleNoteSelect('${n.id}')"></div>` : ''}
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px">
          <div class="note-card-title" style="flex:1">${esc(n.title) || '<span style="color:var(--muted);font-style:italic">Untitled</span>'}</div>
          ${iconHtml}
        </div>
        ${n.locked && !isUnlocked
          ? `<div style="font-size:12px;color:var(--muted);margin-top:3px">🔒 Tap to unlock</div>`
          : preview ? `<div class="note-row-preview">${esc(preview)}</div>` : ''}
      </div>
    </div>
    ${secureNowBtn}
  </div>`;
}

// ── Note row interaction ──────────────────
let _noteLongPressTimer = null;
let _noteTouchMoved = false;
let _noteSelected = new Set();

function _noteRowTouchStart(id, e) {
  _noteTouchMoved = false;
  _noteLongPressTimer = setTimeout(() => {
    if (!_noteTouchMoved) {
      navigator.vibrate && navigator.vibrate(40);
      _startNoteMultiSelect(id);
    }
  }, 500);
}

function _noteRowTouchEnd(id, e) {
  clearTimeout(_noteLongPressTimer);
}

document.addEventListener('touchmove', () => { _noteTouchMoved = true; clearTimeout(_noteLongPressTimer); }, { passive: true });

function _noteRowClick(id, e) {
  if (_noteSelected.size > 0) {
    _toggleNoteSelect(id);
    return;
  }
  openNoteEditor(id);
}

function _startNoteMultiSelect(id) {
  _noteSelected.add(id);
  renderNotes();
  _showNoteActionBar();
}

function _toggleNoteSelect(id) {
  if (_noteSelected.has(id)) _noteSelected.delete(id);
  else _noteSelected.add(id);
  if (_noteSelected.size === 0) { _hideNoteActionBar(); }
  else { _showNoteActionBar(); }
  renderNotes();
}

function _showNoteActionBar() {
  let bar = document.getElementById('note-action-bar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'note-action-bar';
    bar.style.cssText = 'position:fixed;bottom:0;left:0;right:0;background:var(--surface);border-top:1px solid var(--border);padding:16px 20px;display:flex;gap:10px;justify-content:space-around;z-index:350;box-shadow:0 -4px 20px rgba(0,0,0,0.3)';
    bar.innerHTML = `
      <button class="btn btn-ghost" onclick="_bulkNoteAction('pin')" style="flex:1;flex-direction:column;gap:4px;height:56px;font-size:12px"><svg class="icon" aria-hidden="true"><use href="#i-pin"></use></svg><br>Pin</button>
      <button class="btn btn-ghost" onclick="_bulkNoteAction('archive')" style="flex:1;flex-direction:column;gap:4px;height:56px;font-size:12px"><svg class="icon" aria-hidden="true"><use href="#i-archive"></use></svg><br>Archive</button>
      <button class="btn btn-danger" onclick="_bulkNoteAction('delete')" style="flex:1;flex-direction:column;gap:4px;height:56px;font-size:12px"><svg class="icon" aria-hidden="true"><use href="#i-trash-2"></use></svg><br>Delete</button>
      <button class="btn btn-ghost" onclick="_cancelNoteSelect()" style="flex:1;flex-direction:column;gap:4px;height:56px;font-size:12px"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg><br>Cancel</button>
    `;
    document.body.appendChild(bar);
  }
  // Update count
  bar.querySelector('button:last-child').parentElement;
  const countEl = document.getElementById('note-action-count');
  if (!countEl) {
    const label = document.createElement('div');
    label.id = 'note-action-count';
    label.style.cssText = 'position:fixed;bottom:76px;left:50%;transform:translateX(-50%);background:var(--accent);color:#111;font-size:12px;font-weight:700;padding:3px 12px;border-radius:99px;z-index:351';
    document.body.appendChild(label);
  }
  document.getElementById('note-action-count').textContent = `${_noteSelected.size} selected`;
}

function _hideNoteActionBar() {
  document.getElementById('note-action-bar')?.remove();
  document.getElementById('note-action-count')?.remove();
}

function _cancelNoteSelect() {
  _noteSelected.clear();
  _hideNoteActionBar();
  renderNotes();
}

async function _bulkNoteAction(action) {
  const ids = [..._noteSelected];
  for (const id of ids) {
    const n = notes.find(x => x.id === id);
    if (!n) continue;
    if (action === 'pin')     { n.pinned = !n.pinned; n.updatedAt = new Date().toISOString(); }
    if (action === 'archive') { n.archived = true; n.updatedAt = new Date().toISOString(); }
    if (action === 'delete')  { n.deletedAt = new Date().toISOString(); n.updatedAt = new Date().toISOString(); }
  }
  _noteSelected.clear();
  _hideNoteActionBar();
  await saveNotes();
  await _syncNoteIfConnected();
  renderNotes();
  toast(action === 'pin' ? 'Updated ✓' : action === 'archive' ? 'Archived ✓' : 'Moved to trash');
}

async function emptyNotesTrash() {
  if (!confirm('Permanently delete all notes in trash? This cannot be undone.')) return;
  const trashIds = notes.filter(n => !!n.deletedAt).map(n => n.id);
  for (const id of trashIds) {
    const n = notes.find(x => x.id === id);
    if (n?.locked) {
      await fetchKV(`${WORKER_URL}/note/body/delete`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ emailHash:_kvEmailHash, ..._kvSessionToken?{sessionToken:_kvSessionToken}:{verifier:_kvVerifier}, noteId:id }),
      }).catch(()=>{});
    }
  }
  notes = notes.filter(n => !n.deletedAt);
  await saveNotes();
  await _syncNoteIfConnected();
  renderNotes();
  toast('Trash emptied');
}

async function secureLockNote(noteId) {
  const n = notes.find(x => x.id === noteId);
  if (!n || !n.locked) return;
  const state = _noteUnlocked.get(noteId);
  if (!state) return;
  if (!await kvEnsureKey()) return;
  const ciphertext = await kvEncrypt(_kvKey, state.body);
  const res = await fetchKV(`${WORKER_URL}/note/body/push`, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ emailHash:_kvEmailHash, ..._kvSessionToken?{sessionToken:_kvSessionToken}:{verifier:_kvVerifier}, noteId, ciphertext }),
  });
  if (res.ok) {
    _noteUnlocked.delete(noteId);
    clearTimeout(state.inactivityTimer);
    // Close editor if open
    if (_editingNoteId === noteId) { _closeNoteEditorImmediate(); }
    renderNotes();
    toast('Note locked 🔒');
  }
}

function setNotesFilter(f, btn) {
  _notesFilter = f;
  document.querySelectorAll('.note-chip').forEach(c => c.classList.remove('active'));
  if (btn) btn.classList.add('active');
  renderNotes();
}

function filterNotes(q) {
  _notesSearch = q;
  renderNotes();
}

// ── Editor open/close ─────────────────────
async function openNoteEditor(noteId) {
  try {
  const overlay = document.getElementById('note-editor-overlay');
  if (!overlay) { console.error('note-editor-overlay not found'); return; }

  if (!noteId) {
    // New note
    const n = {
      id: _noteUid(), title: '', body: '', locked: false,
      pinned: false, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      deletedAt: null,
    };
    notes.push(n);
    _editingNoteId = n.id;
    _noteUndoStack.set(n.id, []);
    _noteRedoStack.set(n.id, []);
    // Show overlay first so elements are visible, then render into them
    overlay.style.display = 'flex';
    document.getElementById('note-editor-body').style.display = 'flex';
    document.getElementById('note-lock-screen').style.display = 'none';
    _renderNoteEditor(n, false);
    _showNoteBody(n); // always set body (clears previous note's content from contenteditable)
    document.getElementById('note-title-input')?.focus();
    saveNotes().catch(e => console.warn('saveNotes:', e));
    return;
  }

  const n = notes.find(x => x.id === noteId);
  if (!n) return;
  _editingNoteId = noteId;
  if (!_noteUndoStack.has(noteId)) { _noteUndoStack.set(noteId, []); _noteRedoStack.set(noteId, []); }

  // Trash guard
  if (n.deletedAt) {
    if (confirm(`Restore "${n.title}" from trash?`)) {
      n.deletedAt = null; n.updatedAt = new Date().toISOString();
      await saveNotes(); renderNotes();
    }
    return;
  }

  const isUnlocked = _noteUnlocked.has(noteId);
  // Show overlay before rendering so all child elements are accessible
  overlay.style.display = 'flex';
  _renderNoteEditor(n, n.locked && !isUnlocked);

  if (n.locked && !isUnlocked) {
    _showNoteLockScreen(n);
  } else {
    _showNoteBody(n);
    if (isUnlocked) _resetNoteActivity(noteId);
  }
  } catch(err) { console.error('openNoteEditor failed:', err); }
}

function _renderNoteEditor(n, showLock) {
  // Toolbar state
  document.getElementById('note-btn-pin')?.classList.toggle('active', !!n.pinned);
  document.getElementById('note-btn-archive')?.classList.toggle('active', !!n.archived);
  document.getElementById('note-btn-lock')?.classList.toggle('active', !!n.locked);
  const lockBtn = document.getElementById('note-btn-lock');
  if (lockBtn) lockBtn.innerHTML = n.locked
    ? '<svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg>'
    : '<svg class="icon" aria-hidden="true"><use href="#i-unlock"></use></svg>';
  document.getElementById('note-btn-tick')?.classList.toggle('active', !!n.tickBoxesVisible);
  const secureBadge = document.getElementById('note-secure-badge');
  if (secureBadge) secureBadge.style.display = n.locked ? 'block' : 'none';

  // Colour swatches
  document.querySelectorAll('.note-swatch').forEach(s => {
    s.classList.toggle('active', (s.dataset.colour || '') === (n.colour || ''));
  });

  // Editor background
  const overlay = document.getElementById('note-editor-overlay');
  if (overlay) overlay.style.background = n.colour || 'var(--bg)';

  // Undo/redo buttons
  _updateNoteUndoRedoBtns(n.id);

  // Title
  const titleEl = document.getElementById('note-title-input');
  if (titleEl) { titleEl.value = n.title; titleEl.style.display = ''; }

  // Reminder button badge
  const hasReminder = reminders.some(r => r.linkedNoteId === n.id);
  const rBtn = document.getElementById('note-btn-reminder');
  if (rBtn) rBtn.classList.toggle('active', hasReminder);
}

function _showNoteLockScreen(n) {
  document.getElementById('note-editor-body').style.display  = 'none';
  document.getElementById('note-lock-screen').style.display = 'flex';
  document.getElementById('note-lock-title').textContent = n.title;
  document.getElementById('note-lock-error').textContent = '';
  document.getElementById('note-otp-section').style.display = 'none';
  document.getElementById('note-unlock-btn').style.display  = 'block';
  _noteOtpPending = false;
}

function _showNoteBody(n) {
  document.getElementById('note-lock-screen').style.display = 'none';
  const editorBody = document.getElementById('note-editor-body');
  if (editorBody) editorBody.style.display = 'flex';

  const unlocked = _noteUnlocked.get(n.id);
  const body = unlocked ? unlocked.body : (n.body || '');

  if (n.tickBoxesVisible) {
    _renderTickBody(n, body);
  } else {
    const ticksBody = document.getElementById('note-ticks-body');
    if (ticksBody) ticksBody.style.display = 'none';
    const ta = document.getElementById('note-body-input');
    if (ta) {
      ta.style.display = '';
      // contenteditable div — always set innerHTML to prevent leak between notes
      ta.innerHTML = body ? body.replace(/\n/g, '<br>') : '';
    }
  }
}

function _closeNoteEditorImmediate() {
  const overlay = document.getElementById('note-editor-overlay');
  if (overlay) overlay.style.display = 'none';
  _editingNoteId = null;
  _noteBodyDirty = false;
  clearTimeout(_noteAutoSaveTimer);
  _noteColourPickerOpen = false;
  const picker = document.getElementById('note-colour-picker');
  if (picker) picker.style.display = 'none';
}

async function closeNoteEditor() {
  const id = _editingNoteId;
  if (_noteBodyDirty) await _autoSaveNote();
  _closeNoteEditorImmediate();
  // Discard untitled empty notes — don't litter the grid with blank cards
  if (id) {
    const n = notes.find(x => x.id === id);
    if (n && !n.title?.trim() && !n.body?.trim() && !(document.getElementById('note-body-input')?.innerHTML?.trim())) {
      notes = notes.filter(x => x.id !== id);
      await saveNotes();
    }
  }
  renderNotes();
}

// ── Unlock flow ───────────────────────────
async function unlockCurrentNote() {
  const n = notes.find(x => x.id === _editingNoteId);
  if (!n) return;
  const errEl = document.getElementById('note-lock-error');
  errEl.textContent = '';

  // Use existing requireReauth mechanism
  requireReauth(
    `Unlock "${n.title}"`,
    async () => {
      // First factor passed — check if MFA needed
      if (_mfaEnabled()) {
        await _sendNoteOtp();
      } else {
        await _fetchAndUnlockNote(n);
      }
    },
    { passkeyAllowed: true }
  );
}

async function _sendNoteOtp() {
  const errEl = document.getElementById('note-lock-error');
  try {
    const res = await fetchKV(`${WORKER_URL}/note/otp/send`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
      }),
    });
    if (!res.ok) { const d = await res.json(); errEl.textContent = d.error || 'Could not send code'; return; }
    // Show OTP input
    document.getElementById('note-unlock-btn').style.display = 'none';
    document.getElementById('note-otp-section').style.display = 'flex';
    document.getElementById('note-otp-input').value = '';
    document.getElementById('note-otp-error').textContent = '';
    setTimeout(() => document.getElementById('note-otp-input')?.focus(), 100);
    _noteOtpPending = true;
  } catch(e) {
    errEl.textContent = 'Error: ' + e.message;
  }
}

async function verifyNoteOtp() {
  const otp   = document.getElementById('note-otp-input')?.value.trim();
  const errEl = document.getElementById('note-otp-error');
  if (!otp || otp.length !== 6) { errEl.textContent = 'Enter the 6-digit code'; return; }
  try {
    const res = await fetchKV(`${WORKER_URL}/note/otp/verify`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, otp }),
    });
    const d = await res.json();
    if (!res.ok) { errEl.textContent = d.error || 'Incorrect code'; return; }
    // OTP verified — fetch the body
    const n = notes.find(x => x.id === _editingNoteId);
    if (n) await _fetchAndUnlockNote(n);
  } catch(e) {
    errEl.textContent = 'Error: ' + e.message;
  }
}

async function resendNoteOtp() {
  document.getElementById('note-otp-error').textContent = '';
  await _sendNoteOtp();
}

async function _fetchAndUnlockNote(n) {
  const errEl = document.getElementById('note-lock-error');
  try {
    const res = await fetchKV(`${WORKER_URL}/note/body/pull`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
        noteId: n.id,
      }),
    });
    if (!res.ok) {
      const d = await res.json();
      errEl.textContent = d.error || 'Could not fetch note';
      return;
    }
    const { ciphertext } = await res.json();
    if (!await kvEnsureKey()) { errEl.textContent = 'Encryption key unavailable'; return; }
    const body = await kvDecrypt(_kvKey, ciphertext);
    // Cache in memory (body stored as innerHTML)
    _noteUnlocked.set(n.id, { body, lastActivity: Date.now(), inactivityTimer: null });
    _startNoteInactivityTimer(n.id);
    _showNoteBody(n);
    _renderNoteEditor(n, false);
  } catch(e) {
    errEl.textContent = 'Could not unlock: ' + e.message;
  }
}

// ── Toolbar actions ───────────────────────
async function toggleNotePin() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  n.pinned = !n.pinned; n.updatedAt = new Date().toISOString();
  document.getElementById('note-btn-pin')?.classList.toggle('active', n.pinned);
  await saveNotes(); await _syncNoteIfConnected();
}

async function toggleNoteArchive() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  n.archived = !n.archived; n.updatedAt = new Date().toISOString();
  document.getElementById('note-btn-archive')?.classList.toggle('active', n.archived);
  await saveNotes(); await _syncNoteIfConnected();
  if (n.archived) { toast('Note archived'); closeNoteEditor(); }
}

async function toggleNoteLock() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  if (!n.locked) {
    // Suggest MFA when first securing a note
    setTimeout(_maybeShowMfaPrompt, 800);
  }
  if (n.locked) {
    // Unlocking — pull body from server and embed locally
    if (!confirm('Remove security from this note? The body will be stored with your other data.')) return;
    const unlocked = _noteUnlocked.get(n.id);
    if (!unlocked) { toast('Unlock the note first before removing security'); return; }
    n.body   = unlocked.body; // stored as innerHTML
    n.locked = false;
    _noteUnlocked.delete(n.id);
    // Delete the server-side body
    await fetchKV(`${WORKER_URL}/note/body/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
        noteId: n.id,
      }),
    }).catch(() => {});
    toast('Note is no longer secured');
  } else {
    // Locking — push body to server and strip from local
    if (!n.body && !_noteUnlocked.get(n.id)?.body) { toast('Add some content first'); return; }
    const body = _noteUnlocked.get(n.id)?.body || n.body || '';
    if (!await kvEnsureKey()) return;
    const ciphertext = await kvEncrypt(_kvKey, body);
    const res = await fetchKV(`${WORKER_URL}/note/body/push`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
        noteId: n.id, ciphertext,
      }),
    });
    if (!res.ok) { toast('Could not secure note — check connection'); return; }
    n.body   = undefined;
    n.locked = true;
    _noteUnlocked.set(n.id, { body, lastActivity: Date.now(), inactivityTimer: null });
    _startNoteInactivityTimer(n.id);
    toast('Note is now secured 🔒');
  }
  n.updatedAt = new Date().toISOString();
  _renderNoteEditor(n, false);
  await saveNotes(); await _syncNoteIfConnected();
}

async function toggleNoteTicks() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  n.tickBoxesVisible = !n.tickBoxesVisible;
  if (!n.tickBoxes) n.tickBoxes = {};
  document.getElementById('note-btn-tick')?.classList.toggle('active', n.tickBoxesVisible);
  const bodyEl  = document.getElementById('note-body-input');
  const ticksEl = document.getElementById('note-ticks-body');

  if (n.tickBoxesVisible) {
    // Switching to tick view — extract plain-text lines from current HTML body
    const rawHTML = bodyEl ? bodyEl.innerHTML : (n.body || '');
    // Convert <br> / block tags to newlines then strip remaining tags
    const tmp = document.createElement('div'); tmp.innerHTML = rawHTML;
    tmp.querySelectorAll('br').forEach(br => br.replaceWith('\n'));
    tmp.querySelectorAll('p,div,li').forEach(el => { el.insertAdjacentText('afterend', '\n'); });
    const plainText = (tmp.textContent || '').trim();
    if (bodyEl) bodyEl.style.display = 'none';
    _renderTickBody(n, plainText);
  } else {
    // Switching back to rich text — collect tick labels back to text
    if (ticksEl) {
      const labels = ticksEl.querySelectorAll('label span');
      const lines  = [...labels].map(s => s.textContent).join('\n');
      if (bodyEl) { bodyEl.style.display = ''; bodyEl.innerHTML = lines.replace(/\n/g, '<br>'); }
      ticksEl.style.display = 'none';
    } else {
      if (bodyEl) bodyEl.style.display = '';
    }
  }
  n.updatedAt = new Date().toISOString();
  await saveNotes();
}

function _renderTickBody(n, body) {
  const container = document.getElementById('note-ticks-body');
  if (!container) return;
  container.style.display = 'block';
  const paragraphs = (body || '').split('\n').filter(p => p.trim());
  if (!paragraphs.length) {
    container.innerHTML = `<p style="color:var(--muted);font-size:13px">Add some lines — each line becomes a tick box.</p>`;
    return;
  }
  container.innerHTML = paragraphs.map((p, i) => {
    const checked = !!(n.tickBoxes || {})[i];
    return `<label style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);cursor:pointer">
      <input type="checkbox" ${checked ? 'checked' : ''} onchange="onNoteTick(${i},this.checked)"
        style="margin-top:3px;width:18px;height:18px;min-width:18px;accent-color:var(--accent)">
      <span style="${checked ? 'text-decoration:line-through;color:var(--muted)' : 'color:var(--text)'};font-size:14px;line-height:1.5">${esc(p)}</span>
    </label>`;
  }).join('');
}

async function onNoteTick(idx, checked) {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  if (!n.tickBoxes) n.tickBoxes = {};
  n.tickBoxes[idx] = checked;
  n.updatedAt = new Date().toISOString();
  await saveNotes();
  if (n.locked) _resetNoteActivity(n.id);
}

function toggleNoteColourPicker() {
  const picker = document.getElementById('note-colour-picker');
  _noteColourPickerOpen = !_noteColourPickerOpen;
  picker.style.display = _noteColourPickerOpen ? 'flex' : 'none';
}

async function setNoteColour(colour) {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  n.colour = colour || null; n.updatedAt = new Date().toISOString();
  document.getElementById('note-editor-overlay').style.background = colour || 'var(--bg)';
  document.querySelectorAll('.note-swatch').forEach(s =>
    s.classList.toggle('active', (s.dataset.colour || '') === (colour || ''))
  );
  _noteColourPickerOpen = false;
  document.getElementById('note-colour-picker').style.display = 'none';
  await saveNotes(); await _syncNoteIfConnected();
}

function copyNoteBody() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const rawBody = _getCurrentEditorBody(n);
  // Convert HTML to plain text preserving newlines
  const tmp = document.createElement('div'); tmp.innerHTML = rawBody;
  // Replace <br> and block elements with newlines before extracting text
  tmp.querySelectorAll('br').forEach(br => br.replaceWith('\n'));
  tmp.querySelectorAll('p, div, li, h1, h2, h3, h4').forEach(el => {
    el.insertAdjacentText('afterend', '\n');
  });
  const plainBody = (tmp.textContent || tmp.innerText || '').replace(/\n{3,}/g, '\n\n').trim();
  const plainText = n.title ? `${n.title}\n\n${plainBody}` : plainBody;

  if (navigator.clipboard?.write) {
    // Write both HTML and plain text so paste destination can choose
    const htmlBlob  = new Blob([`<b>${n.title}</b><br><br>${rawBody}`], { type: 'text/html' });
    const textBlob  = new Blob([plainText], { type: 'text/plain' });
    navigator.clipboard.write([new ClipboardItem({ 'text/html': htmlBlob, 'text/plain': textBlob })])
      .then(() => toast('Copied ✓'))
      .catch(() => navigator.clipboard.writeText(plainText).then(() => toast('Copied ✓')).catch(() => {}));
  } else {
    navigator.clipboard?.writeText(plainText).then(() => toast('Copied ✓')).catch(() => {
      const ta = document.createElement('textarea');
      ta.value = plainText; ta.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(ta); ta.select(); document.execCommand('copy');
      document.body.removeChild(ta); toast('Copied ✓');
    });
  }
}

async function deleteCurrentNote() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  if (n.deletedAt) {
    if (!confirm(`Permanently delete "${n.title}"? This cannot be undone.`)) return;
    notes = notes.filter(x => x.id !== n.id);
    if (n.locked) {
      await fetchKV(`${WORKER_URL}/note/body/delete`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier }, noteId: n.id }),
      }).catch(() => {});
    }
  } else {
    if (!confirm(`Move "${n.title}" to trash?`)) return;
    n.deletedAt  = new Date().toISOString();
    n.updatedAt  = new Date().toISOString();
    // Also remove from reminders
    reminders = reminders.filter(r => r.linkedNoteId !== n.id);
    await saveReminders();
  }
  _noteUnlocked.delete(n.id);
  await saveNotes();
  await _syncNoteIfConnected();
  _closeNoteEditorImmediate();
  renderNotes();
}

// ── Body editing ──────────────────────────
function _getCurrentEditorBody(n) {
  if (n.tickBoxesVisible) {
    const labels = document.querySelectorAll('#note-ticks-body label span');
    return [...labels].map(s => s.textContent).join('\n');
  }
  return document.getElementById('note-body-input')?.innerHTML || '';
}

function onNoteTitleInput() {
  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 1200);
  const n = notes.find(x => x.id === _editingNoteId);
  if (n && n.locked) _resetNoteActivity(n.id);
}

function onNoteBodyInput() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const el   = document.getElementById('note-body-input');
  const body = el?.innerHTML || '';

  // Push undo snapshot
  const stack = _noteUndoStack.get(n.id) || [];
  const last  = stack[stack.length - 1];
  if (last !== body) {
    stack.push(body);
    if (stack.length > 50) stack.shift();
    _noteUndoStack.set(n.id, stack);
    _noteRedoStack.set(n.id, []);
    _updateNoteUndoRedoBtns(n.id);
  }

  _noteBodyDirty = true;
  clearTimeout(_noteAutoSaveTimer);
  _noteAutoSaveTimer = setTimeout(_autoSaveNote, 1200);
  if (n.locked) _resetNoteActivity(n.id);
}

async function _autoSaveNote() {
  const n = notes.find(x => x.id === _editingNoteId);
  if (!n) return;
  const titleEl = document.getElementById('note-title-input');
  const title   = (titleEl?.value || '').trim();
  if (!title) return; // require title

  const body = _getCurrentEditorBody(n);
  n.title     = title;
  n.updatedAt = new Date().toISOString();

  if (n.locked) {
    // Update in-memory cache only; push to server
    const state = _noteUnlocked.get(n.id);
    if (state) {
      state.body = body; // body is already innerHTML
      if (!await kvEnsureKey()) return;
      const ciphertext = await kvEncrypt(_kvKey, body);
      await fetchKV(`${WORKER_URL}/note/body/push`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          emailHash: _kvEmailHash,
          ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
          noteId: n.id, ciphertext,
        }),
      }).catch(() => {});
    }
  } else {
    n.body = body;
  }

  _noteBodyDirty = false;
  await saveNotes();
  await _syncNoteIfConnected();
}

// ── Undo / Redo ───────────────────────────
// Undo/redo delegated to browser execCommand — no manual stacks needed
function _updateNoteUndoRedoBtns(noteId) { /* browser handles undo/redo */ }

// ── Reminder ──────────────────────────────
function openNoteReminder() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const existing = reminders.find(r => r.linkedNoteId === n.id);
  const dtInput  = document.getElementById('note-reminder-datetime');
  const notesInp = document.getElementById('note-reminder-notes');
  const existDiv = document.getElementById('note-reminder-existing');
  const delBtn   = document.getElementById('note-reminder-delete-btn');

  if (existing) {
    existDiv.style.display = 'block';
    existDiv.textContent   = `Current: ${existing.name} — ${fmtDate(existing.lastReplaced || '')}`;
    delBtn.style.display   = 'inline-block';
  } else {
    existDiv.style.display = 'none';
    delBtn.style.display   = 'none';
  }

  // Default datetime: tomorrow at 9am
  const tomorrow = new Date(); tomorrow.setDate(tomorrow.getDate() + 1); tomorrow.setHours(9, 0, 0, 0);
  dtInput.value  = existing?.reminderDate ? existing.reminderDate.slice(0, 16) : tomorrow.toISOString().slice(0, 16);
  notesInp.value = existing?.notes || '';
  openModal('note-reminder-modal');
}

async function saveNoteReminder() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  const dt    = document.getElementById('note-reminder-datetime')?.value;
  const notes_ = document.getElementById('note-reminder-notes')?.value.trim();
  if (!dt) { toast('Pick a date and time'); return; }

  // Remove existing note reminder
  reminders = reminders.filter(r => r.linkedNoteId !== n.id);

  reminders.push({
    id:           uid(),
    name:         n.title || 'Note reminder',
    interval:     1, unit: 'months',
    lastReplaced: null,
    notes:        notes_,
    linkedNoteId: n.id,
    reminderDate: new Date(dt).toISOString(),
    createdAt:    new Date().toISOString(),
  });
  await saveReminders();
  closeModal('note-reminder-modal');
  _renderNoteEditor(n, false);
  renderNotes();
  await _syncNoteIfConnected();
  toast('Reminder set ✓');
}

async function deleteNoteReminder() {
  const n = notes.find(x => x.id === _editingNoteId); if (!n) return;
  reminders = reminders.filter(r => r.linkedNoteId !== n.id);
  await saveReminders();
  closeModal('note-reminder-modal');
  _renderNoteEditor(n, false);
  renderNotes();
  toast('Reminder removed');
}

// ── Keyboard shortcuts ────────────────────
document.addEventListener('keydown', e => {
  if (!_editingNoteId) return;
  if (e.key === 'Escape') {
    if (_noteColourPickerOpen) {
      _noteColourPickerOpen = false;
      const cp = document.getElementById('note-colour-picker');
      if (cp) cp.style.display = 'none';
    } else {
      closeNoteEditor();
    }
  }
});

// Update format button active states when selection changes
document.addEventListener('selectionchange', () => {
  if (!_editingNoteId) return;
  const active = document.activeElement;
  const bodyEl = document.getElementById('note-body-input');
  if (active === bodyEl || bodyEl?.contains(active)) _updateFmtBtnStates();
});

// ── Sync helper ───────────────────────────
async function _syncNoteIfConnected() {
  if (kvConnected && !_shareState) {
    kvPush().catch(e => console.warn('notes kvPush:', e.message));
  }
}

// ── Rich text formatting ─────────────────
function noteFmt(cmd) {
  const el = document.getElementById('note-body-input');
  if (!el) return;
  el.focus();
  document.execCommand(cmd, false, null);
  // Update active state on format buttons
  _updateFmtBtnStates();
  onNoteBodyInput();
}

function _updateFmtBtnStates() {
  const cmds = { bold:'bold', italic:'italic', underline:'underline', strikeThrough:'strikeThrough' };
  Object.entries(cmds).forEach(([cmd, title]) => {
    document.querySelectorAll('.note-fmt-btn').forEach(btn => {
      if (btn.title === title.charAt(0).toUpperCase() + title.slice(1)) {
        btn.classList.toggle('active', document.queryCommandState(cmd));
      }
    });
  });
}

// ── 2FA prompt (MFA system) ───────────────

// ═══════════════════════════════════════════
//  BACK BUTTON / SWIPE INTERSTITIAL
// ═══════════════════════════════════════════

(function _initBackInterstitial() {
  // Push a history entry so the first back press/swipe hits us, not the previous page
  history.pushState({ stockroom: true }, '');

  let _touchStartX = 0;
  let _touchStartY = 0;
  const SWIPE_THRESHOLD = 40; // px from left edge to count as back swipe

  document.addEventListener('touchstart', e => {
    _touchStartX = e.touches[0].clientX;
    _touchStartY = e.touches[0].clientY;
  }, { passive: true });

  document.addEventListener('touchend', e => {
    const dx = e.changedTouches[0].clientX - _touchStartX;
    const dy = Math.abs(e.changedTouches[0].clientY - _touchStartY);
    // Swipe right from left edge (back gesture on iOS/Android)
    if (_touchStartX < 30 && dx > 60 && dy < 80) {
      _showBackInterstitial();
    }
  }, { passive: true });

  window.addEventListener('popstate', e => {
    if (window._allowExit) { window._allowExit = false; return; }
    if (e.state?.stockroom) return; // our own push — ignore
    history.pushState({ stockroom: true }, '');
    _showBackInterstitial();
  });
})();

function _showBackInterstitial() {
  // Don't show if wizard is active or a modal is already open
  if (document.body.classList.contains('wizard-active')) return;
  if (document.querySelector('.modal-backdrop.open')) return;
  openModal('back-interstitial-modal');
}

function backInterstitialGoTo(view) {
  closeModal('back-interstitial-modal');
  const tab = [...document.querySelectorAll('.tab, .app-nav-link')].find(t =>
    t.dataset?.view === view || t.textContent?.toLowerCase().includes(view)
  );
  if (tab) { tab.click(); } else { navTo(view); }
}

function backInterstitialExit() {
  closeModal('back-interstitial-modal');
  window._allowExit = true;
  history.back();
}

// ═══════════════════════════════════════════
//  MULTIFACTOR AUTHENTICATION (MFA)
// ═══════════════════════════════════════════
// Replaces notes-only 2FA. Works across the whole app via requireReauth hook.
// Methods: Email OTP | TOTP (authenticator app)
// ── MFA — multi-method system ─────────────────────────────────────────────
// settings.mfa shape (v2, backwards-compat with v1 single-method):
// {
//   enabled: bool,
//   methods: [
//     { type:'email', primary:bool },
//     { type:'totp',  secret:string, primary:bool }
//   ]
// }
// v1 compat: if methods absent, derive from legacy { method, totpSecret }

const _MFA_DISMISSED_KEY = 'stockroom_mfa_prompt_dismissed';
let _pendingMfaCallback = null;
let _mfaOtpSent = false;

function _mfaEnabled() {
  return !!(settings.mfa?.enabled);
}

function _mfaMethods() {
  // v2 shape: methods array is the source of truth
  if (Array.isArray(settings.mfa?.methods)) return settings.mfa.methods;
  // Disabled or not configured
  if (!settings.mfa?.enabled) return [];
  // v1 single-method compat (legacy accounts that haven't been migrated yet)
  const m = settings.mfa?.method || 'email';
  return [{ type: m, primary: true, ...(m === 'totp' ? { secret: settings.mfa.totpSecret } : {}) }];
}

function _mfaPrimaryMethod() {
  const methods = _mfaMethods();
  return methods.find(m => m.primary) || methods[0] || null;
}

// Legacy compat — old code calls _mfaMethod()
function _mfaMethod() {
  return _mfaPrimaryMethod()?.type || 'email';
}

// ── Login intercept ─────────────────────────────────────────────────────────
async function _mfaLoginIntercept(callback) {
  _pendingMfaCallback = callback;
  _mfaVerifyAltMode   = false;

  const primary     = _mfaPrimaryMethod();
  const isTotp      = primary?.type === 'totp';
  const hasBoth     = _mfaMethods().length > 1;

  // Show the correct hint panel and hide the other
  const emailHint = document.getElementById('mfa-email-hint');
  const totpHint  = document.getElementById('mfa-totp-hint');
  const altBtn    = document.getElementById('mfa-alt-method');
  const resendBtn = document.getElementById('mfa-resend-btn');
  const codeEl    = document.getElementById('mfa-verify-code');

  if (emailHint) emailHint.style.display = isTotp ? 'none' : 'block';
  if (totpHint)  totpHint.style.display  = isTotp ? 'block' : 'none';
  // Alt button only shown when user has both methods
  if (altBtn) {
    altBtn.style.display = hasBoth ? '' : 'none';
    altBtn.textContent   = isTotp ? 'Use email code instead' : 'Use authenticator app instead';
  }
  // Resend only relevant for email
  if (resendBtn) resendBtn.style.display = isTotp ? 'none' : '';
  if (codeEl) codeEl.value = '';

  openModal('mfa-verify-modal');

  if (!isTotp) {
    // Email primary — send OTP immediately
    if (emailHint) emailHint.innerHTML = '<p style="font-size:13px;color:var(--muted);margin-bottom:8px">Sending a code to your email…</p>';
    await _mfaSendEmailOtp();
    if (emailHint) emailHint.innerHTML = '<p style="font-size:13px;color:var(--muted);margin-bottom:8px">Enter the code sent to your email.</p>';
  }
}

async function _mfaIntercept(callback) {
  if (!_mfaEnabled()) { callback(); return; }
  _pendingMfaCallback = callback;
  _mfaVerifyAltMode   = false;

  const primary  = _mfaPrimaryMethod();
  const isTotp   = primary?.type === 'totp';
  const hasBoth  = _mfaMethods().length > 1;
  const emailHint = document.getElementById('mfa-email-hint');
  const totpHint  = document.getElementById('mfa-totp-hint');
  const altBtn    = document.getElementById('mfa-alt-method');
  const resendBtn = document.getElementById('mfa-resend-btn');
  const codeEl    = document.getElementById('mfa-verify-code');

  if (emailHint) emailHint.style.display = isTotp ? 'none' : 'block';
  if (totpHint)  totpHint.style.display  = isTotp ? 'block' : 'none';
  if (altBtn)    { altBtn.style.display  = hasBoth ? '' : 'none'; altBtn.textContent = isTotp ? 'Use email code instead' : 'Use authenticator app instead'; }
  if (resendBtn) resendBtn.style.display = isTotp ? 'none' : '';
  if (codeEl)    codeEl.value = '';

  openModal('mfa-verify-modal');
  if (!isTotp) await _mfaSendEmailOtp();
}

async function _mfaSendEmailOtp() {
  const errEl     = document.getElementById('mfa-otp-error');
  const sendingEl = document.getElementById('mfa-sending-status');
  if (errEl)     errEl.textContent = '';
  if (sendingEl) { sendingEl.textContent = 'Sending code…'; sendingEl.style.display = 'block'; }
  try {
    const res = await fetchKV(`${WORKER_URL}/mfa/otp/send`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ emailHash:_kvEmailHash, email: _kvEmail || settings.email || '', ..._kvSessionToken?{sessionToken:_kvSessionToken}:{verifier:_kvVerifier} }),
    });
    const d = await res.json();
    if (!res.ok) {
      if (errEl) errEl.textContent = d.error || 'Could not send verification code';
      if (sendingEl) sendingEl.style.display = 'none';
      return;
    }
    if (sendingEl) { sendingEl.textContent = 'Code sent ✓ — check your email'; }
    setTimeout(() => { if (sendingEl) sendingEl.style.display = 'none'; }, 3000);
    _mfaOtpSent = true;
  } catch(e) {
    if (errEl) errEl.textContent = 'Could not send code: ' + e.message;
    if (sendingEl) sendingEl.style.display = 'none';
  }
}

let _mfaVerifyAltMode = false;

async function mfaVerifySubmit() {
  const primaryMethod = _mfaMethod();
  const method = _mfaVerifyAltMode
    ? (primaryMethod === 'totp' ? 'email' : 'totp')
    : primaryMethod;
  const codeEl = document.getElementById('mfa-verify-code');
  const errEl  = document.getElementById('mfa-otp-error');
  const code   = codeEl?.value.trim().replace(/\s/g,'');
  if (!code || code.length < 6) { if(errEl) errEl.textContent='Enter your 6-digit code'; return; }

  if (method === 'email') {
    const res = await fetchKV(`${WORKER_URL}/mfa/otp/verify`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ emailHash:_kvEmailHash, otp:code }),
    });
    const d = await res.json();
    if (!res.ok) { if(errEl) errEl.textContent = d.error || 'Incorrect code'; return; }
  } else {
    // TOTP — find secret from any stored location
    const methods    = _mfaMethods();
    const totpMethod = methods.find(m => m.type === 'totp');
    const secret     = totpMethod?.secret || settings.mfa?.totpSecret || null;
    if (!secret) {
      if(errEl) errEl.textContent = 'Authenticator app not configured on this account';
      console.error('TOTP verify: no secret found. settings.mfa =', JSON.stringify(settings.mfa));
      return;
    }
    const valid = await _totpVerify(secret, code);
    if (!valid) { if(errEl) errEl.textContent = 'Incorrect code — check your authenticator app and device clock'; return; }
  }

  _mfaVerifyAltMode = false;
  localStorage.removeItem('stockroom_mfa_was_active');
  // Mark MFA as verified for this browser session (allows trusted devices to skip on reload)
  try { sessionStorage.setItem(_MFA_SESSION_KEY, _kvEmailHash || '1'); } catch(e) {}
  closeModal('mfa-verify-modal');
  if (codeEl) codeEl.value = '';
  if (_pendingMfaCallback) { const cb = _pendingMfaCallback; _pendingMfaCallback = null; cb(); }
}

async function mfaResendCode() {
  await _mfaSendEmailOtp();
  toast('Code sent ✓');
}

async function mfaSwitchMethod() {
  _mfaVerifyAltMode = !_mfaVerifyAltMode;
  const primaryMethod   = _mfaMethod();
  const effectiveMethod = _mfaVerifyAltMode ? (primaryMethod === 'totp' ? 'email' : 'totp') : primaryMethod;
  const altBtn    = document.getElementById('mfa-alt-method');
  const totpHint  = document.getElementById('mfa-totp-hint');
  const emailHint = document.getElementById('mfa-email-hint');
  const resendBtn = document.getElementById('mfa-resend-btn');
  const codeEl    = document.getElementById('mfa-verify-code');
  if (altBtn)    altBtn.textContent        = effectiveMethod === 'totp' ? 'Use email code instead' : 'Use authenticator app instead';
  if (totpHint)  totpHint.style.display    = effectiveMethod === 'totp'  ? 'block' : 'none';
  if (emailHint) emailHint.style.display   = effectiveMethod === 'email' ? 'block' : 'none';
  if (resendBtn) resendBtn.style.display   = effectiveMethod === 'email' ? '' : 'none';
  if (codeEl)    codeEl.value = '';
  if (_mfaVerifyAltMode && effectiveMethod === 'email') await _mfaSendEmailOtp();
}

// ── Setup / Add method ──────────────────────────────────────────────────────
// _pendingMfaSetupMode: 'enable' (first time) | 'add' (adding second method)
let _pendingMfaSetupMode = 'enable';
let _mfaEmailTestSent    = false;

// ── MFA gate — single hard checkpoint for all login paths ────────────────────
// Every path into the app calls _mfaGate(callback). It:
//   1. Pulls the latest settings from the server (so MFA state is always fresh)
//   2. If MFA is enabled AND not already verified this session, shows verify modal
//   3. If disabled OR already verified this session, runs callback immediately
//
// "Stay signed in" behaviour with MFA:
//   - First load after sign-in: MFA fires, user verifies → flag set in sessionStorage
//   - Subsequent page loads in same browser session: flag present → MFA skipped
//   - Sign out or cookie/storage clear: sessionStorage wiped → MFA fires again next load
//   - Explicit sign-out always clears the flag
const _MFA_SESSION_KEY = 'stockroom_mfa_verified';

async function _mfaGate(callback) {
  // Pull fresh settings from server — MFA state must be authoritative from server.
  // Also merge setup flags so protectSeen/countrySet/installDismissed are fresh
  // when routing code runs inside the callback.
  if (kvConnected && _kvKey) {
    try {
      const remote = await kvPull();
      if (remote?.settings) {
        const remoteMfa = remote.settings.mfa;
        if (remoteMfa !== undefined) {
          settings.mfa = remoteMfa;
          localStorage.removeItem('stockroom_mfa_was_active');
        }
        // Merge one-time setup flags
        if (remote.settings._setupProtectSeen) settings._setupProtectSeen = true;
        if (remote.settings._setupCountrySet)  settings._setupCountrySet  = true;
        if (remote.settings._installDismissed) {
          settings._installDismissed = true;
          try { localStorage.setItem('stockroom_install_dismissed', '1'); } catch(e) {}
          try { localStorage.setItem('stockroom_ios_banner_dismissed', '1'); } catch(e) {}
        }
        await _saveSettings();
      }
    } catch(e) {
      console.warn('_mfaGate: pull failed, enforcing local/was-active MFA state:', e.message);
    }
  }

  const mfaActive = (kvConnected && _mfaEnabled()) ||
    (kvConnected && !!localStorage.getItem('stockroom_mfa_was_active'));

  if (!mfaActive) {
    await callback();
    return;
  }

  // MFA is enabled — check if already verified this browser session
  // sessionStorage is wiped on tab/browser close AND on cookie clear,
  // so "stay signed in" users only verify once per session.
  const alreadyVerified = sessionStorage.getItem(_MFA_SESSION_KEY) === _kvEmailHash;
  if (alreadyVerified) {
    await callback();
    return;
  }

  await _mfaLoginIntercept(callback);
}
function _totpGenerate(secret, time) {
  const b32   = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  const clean = secret.toUpperCase().replace(/[^A-Z2-7]/g,'');
  let bits = '';
  for (const c of clean) bits += b32.indexOf(c).toString(2).padStart(5,'0');
  const bytes = new Uint8Array(bits.length >> 3);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(bits.slice(i*8,(i+1)*8),2);
  const counter = Math.floor((time || Date.now()/1000) / 30);
  const msg = new Uint8Array(8);
  let c = counter;
  for (let i = 7; i >= 0; i--) { msg[i] = c & 0xff; c >>>= 8; }
  async function hmacSha1(key, data) {
    const k = await crypto.subtle.importKey('raw', key, {name:'HMAC',hash:'SHA-1'}, false, ['sign']);
    return new Uint8Array(await crypto.subtle.sign('HMAC', k, data));
  }
  return hmacSha1(bytes, msg).then(hash => {
    const offset = hash[19] & 0xf;
    const code = ((hash[offset]&0x7f)<<24|(hash[offset+1]&0xff)<<16|(hash[offset+2]&0xff)<<8|(hash[offset+3]&0xff)) % 1000000;
    return code.toString().padStart(6,'0');
  });
}

async function _totpVerify(secret, code) {
  const t = Date.now()/1000;
  for (const offset of [-30, 0, 30]) {
    if (await _totpGenerate(secret, t + offset) === code) return true;
  }
  return false;
}

function _totpNewSecret() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  return Array.from(crypto.getRandomValues(new Uint8Array(20))).map(b => chars[b % 32]).join('');
}

function _totpOtpauthUrl(secret) {
  const label  = encodeURIComponent(`STOCKROOM:${settings.email || _kvEmail || 'account'}`);
  const issuer = encodeURIComponent('STOCKROOM');
  return `otpauth://totp/${label}?secret=${secret}&issuer=${issuer}&digits=6&period=30`;
}

async function _renderTotpQr(secret) {
  const canvas = document.getElementById('mfa-totp-qr-canvas');
  if (!canvas) return;
  const uri = _totpOtpauthUrl(secret);
  try {
    if (!window.QRious) {
      await new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = 'https://cdnjs.cloudflare.com/ajax/libs/qrious/4.0.2/qrious.min.js';
        s.onload = resolve; s.onerror = reject;
        document.head.appendChild(s);
      });
    }
    canvas.style.display = 'block';
    new window.QRious({ element: canvas, value: uri, size: 200, background: '#ffffff', foreground: '#000000' });
  } catch(e) {
    canvas.style.display = 'none';
    const fallback = document.getElementById('mfa-totp-qr-fallback');
    if (fallback) { fallback.style.display = 'block'; fallback.textContent = 'Could not load QR code — use the manual code below.'; }
  }
}

async function openMfaSetup() {
  _pendingMfaSetupMode = 'enable';
  _mfaEmailTestSent    = false;
  _mfaSetupReset();
  document.getElementById('mfa-setup-title').innerHTML    = '<svg class="icon icon-md" aria-hidden="true" style="color:var(--accent);vertical-align:-3px"><use href="#i-shield"></use></svg> Set Up Multifactor Authentication';
  document.getElementById('mfa-setup-subtitle').textContent = 'Choose your second factor and verify it works before enabling.';
  document.getElementById('mfa-setup-confirm-btn').textContent = 'Send code to my email →';
  // Always show both tabs — fresh enable, no existing methods
  const step = document.getElementById('mfa-setup-step-method');
  if (step) {
    step.style.display = '';
    step.querySelectorAll('.mfa-method-btn').forEach(b => b.style.display = '');
  }
  _mfaSetupSelectMethod('email');
  openModal('mfa-setup-modal');
}

function openMfaAddMethod() {
  const existing    = _mfaMethods().map(m => m.type);
  const canAddEmail = !existing.includes('email');
  const canAddTotp  = !existing.includes('totp');
  if (!canAddEmail && !canAddTotp) { toast('All available methods are already added'); return; }
  _pendingMfaSetupMode = 'add';
  _mfaEmailTestSent    = false;
  _mfaSetupReset();
  document.getElementById('mfa-setup-title').innerHTML = '<svg class="icon icon-md" aria-hidden="true"><use href="#i-plus"></use></svg> Add Authentication Method';
  document.getElementById('mfa-setup-subtitle').textContent = 'Verify the new method works before adding it.';
  document.getElementById('mfa-setup-confirm-btn').textContent = canAddEmail ? 'Send code to my email →' : 'Enable authenticator app ✓';
  const step = document.getElementById('mfa-setup-step-method');
  // Reset all tabs visible first, then hide the one that already exists
  step.querySelectorAll('.mfa-method-btn').forEach(b => b.style.display = '');
  const emailBtn = step.querySelector('[data-method="email"]');
  const totpBtn  = step.querySelector('[data-method="totp"]');
  if (emailBtn) emailBtn.style.display = canAddEmail ? '' : 'none';
  if (totpBtn)  totpBtn.style.display  = canAddTotp  ? '' : 'none';
  step.style.display = '';
  _mfaSetupSelectMethod(canAddEmail ? 'email' : 'totp');
  openModal('mfa-setup-modal');
}

function _mfaSetupReset() {
  const errEl = document.getElementById('mfa-setup-error');
  if (errEl) errEl.textContent = '';
  const codeEl = document.getElementById('mfa-setup-totp-code');
  if (codeEl) codeEl.value = '';
  const emailCodeEl = document.getElementById('mfa-setup-email-code');
  if (emailCodeEl) emailCodeEl.value = '';
  const testDiv = document.getElementById('mfa-setup-email-test');
  if (testDiv) testDiv.style.display = 'none';
  const emailDisp = document.getElementById('mfa-setup-email-display');
  if (emailDisp) emailDisp.textContent = settings.email || _kvEmail || '(your email)';
  // Pre-generate TOTP secret
  const secret = _totpNewSecret();
  window._pendingTotpSecret = secret;
  _renderTotpQr(secret).catch(() => {});
  const secretEl = document.getElementById('mfa-totp-secret-display');
  if (secretEl) secretEl.textContent = secret.match(/.{1,4}/g).join(' ');
}

function _mfaSetupSelectMethod(method) {
  document.getElementById('mfa-setup-email-section').style.display = method === 'email' ? 'block' : 'none';
  document.getElementById('mfa-setup-totp-section').style.display  = method === 'totp'  ? 'block' : 'none';
  // Tab styling
  document.querySelectorAll('.mfa-method-btn').forEach(b => {
    const active = b.dataset.method === method;
    b.style.background = active ? 'var(--accent)' : 'var(--surface2)';
    b.style.color      = active ? '#111' : 'var(--muted)';
  });
  window._pendingMfaSetupMethod = method;
  // Button label depends on method — email needs send step, TOTP doesn't
  const btn = document.getElementById('mfa-setup-confirm-btn');
  if (btn) btn.textContent = method === 'email' ? 'Send code to my email →' : 'Enable authenticator app ✓';
  // Reset email test state when switching tabs
  if (method === 'email') {
    _mfaEmailTestSent = false;
    const testDiv = document.getElementById('mfa-setup-email-test');
    if (testDiv) testDiv.style.display = 'none';
    const codeEl = document.getElementById('mfa-setup-email-code');
    if (codeEl) codeEl.value = '';
  }
}

async function mfaSetupConfirm() {
  const method  = window._pendingMfaSetupMethod || 'email';
  const errEl   = document.getElementById('mfa-setup-error');
  const btn     = document.getElementById('mfa-setup-confirm-btn');
  if (errEl) errEl.textContent = '';

  if (method === 'email') {
    if (!_mfaEmailTestSent) {
      // Stage 1: send the test code
      if (btn) { btn.disabled = true; btn.textContent = '⏳ Sending…'; }
      try {
        const res = await fetchKV(`${WORKER_URL}/mfa/otp/send`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ emailHash: _kvEmailHash, email: _kvEmail || settings.email || '',
            ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier } }),
        });
        const d = await res.json();
        if (!res.ok) {
          if (errEl) errEl.textContent = d.error || 'Could not send code — check your email address';
          if (btn) { btn.disabled = false; btn.textContent = 'Send code to my email →'; }
          return;
        }
      } catch(e) {
        if (errEl) errEl.textContent = 'Error: ' + e.message;
        if (btn) { btn.disabled = false; btn.textContent = 'Send code to my email →'; }
        return;
      }
      _mfaEmailTestSent = true;
      // Reveal code input
      const testDiv = document.getElementById('mfa-setup-email-test');
      if (testDiv) testDiv.style.display = 'block';
      setTimeout(() => document.getElementById('mfa-setup-email-code')?.focus(), 100);
      if (btn) { btn.disabled = false; btn.textContent = 'Enable email MFA ✓'; }
      return; // wait for user to enter code and press again
    }

    // Stage 2: verify the code and enable
    const codeEl = document.getElementById('mfa-setup-email-code');
    const code   = codeEl?.value.trim();
    if (!code || code.length < 6) { if (errEl) errEl.textContent = 'Enter the 6-digit code from your email'; return; }
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Verifying…'; }
    try {
      const res = await fetchKV(`${WORKER_URL}/mfa/otp/verify`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ emailHash: _kvEmailHash, otp: code }),
      });
      const d = await res.json();
      if (!res.ok) {
        if (errEl) errEl.textContent = d.error || 'Incorrect code — request a new one';
        if (btn) { btn.disabled = false; btn.textContent = 'Enable email MFA ✓'; }
        return;
      }
    } catch(e) {
      if (errEl) errEl.textContent = 'Error: ' + e.message;
      if (btn) { btn.disabled = false; btn.textContent = 'Enable email MFA ✓'; }
      return;
    }
    const newMethod = { type: 'email', primary: true };
    _mfaFinishSetup(newMethod);

  } else {
    // TOTP — verify the code from the app
    const codeEl = document.getElementById('mfa-setup-totp-code');
    const code   = codeEl?.value.trim();
    if (!code || code.length < 6) { if (errEl) errEl.textContent = 'Enter the 6-digit code from your authenticator app'; return; }
    const valid = await _totpVerify(window._pendingTotpSecret, code);
    if (!valid) { if (errEl) errEl.textContent = 'Code incorrect — make sure your device clock is accurate'; return; }
    const newMethod = { type: 'totp', secret: window._pendingTotpSecret, primary: true };
    _mfaFinishSetup(newMethod);
  }
}

async function _mfaFinishSetup(newMethod) {
  if (_pendingMfaSetupMode === 'add') {
    const existing = _mfaMethods();
    newMethod.primary = false;
    existing.push(newMethod);
    settings.mfa = { enabled: true, methods: existing, method: null, totpSecret: null };
  } else {
    settings.mfa = { enabled: true, methods: [newMethod], method: null, totpSecret: null };
  }
  await _saveSettings();
  // Push first so the server has the new MFA state — then pull to confirm
  try {
    await kvPush();
  } catch(e) { console.warn('_mfaFinishSetup push failed:', e.message); }
  closeModal('mfa-setup-modal');
  closeModal('mfa-prompt-modal');
  localStorage.setItem(_MFA_DISMISSED_KEY, 'enabled');
  _updateMfaSettingsUI();
  toast(_pendingMfaSetupMode === 'add' ? 'Method added ✓' : 'Multifactor authentication enabled ✓');
}

// ── Manage methods ──────────────────────────────────────────────────────────
function openMfaManage() {
  _renderMfaManageList();
  _renderMfaManageFooter();
  openModal('mfa-manage-modal');
}

function _renderMfaManageFooter() {
  const footer  = document.getElementById('mfa-manage-footer');
  if (!footer) return;
  const methods  = _mfaMethods();
  const canAdd   = methods.length < 2;
  footer.innerHTML = `
    ${canAdd ? '<button class="btn btn-ghost" style="flex:1" onclick="closeModal(\'mfa-manage-modal\');openMfaAddMethod()">+ Add method</button>' : ''}
    <button class="btn btn-danger acc-sec-btn" onclick="closeModal(\'mfa-manage-modal\');mfaDisable()">Disable all MFA</button>
    <button class="btn btn-ghost" onclick="closeModal(\'mfa-manage-modal\')">Done</button>
  `;
}

function _renderMfaManageList() {
  const container = document.getElementById('mfa-manage-list');
  if (!container) return;
  const methods = _mfaMethods();
  if (!methods.length) { container.innerHTML = '<p style="font-size:13px;color:var(--muted)">No methods configured.</p>'; return; }
  container.innerHTML = methods.map((m, i) => {
    const label = m.type === 'totp' ? '<svg class="icon" aria-hidden="true"><use href="#i-smartphone"></use></svg> Authenticator app' : '<svg class="icon" aria-hidden="true"><use href="#i-mail"></use></svg> Email code';
    const primaryBadge = m.primary
      ? '<span style="font-size:10px;font-weight:700;color:var(--ok);background:rgba(76,187,138,0.15);padding:2px 8px;border-radius:99px;margin-left:8px">PRIMARY</span>'
      : `<button onclick="mfaSetPrimary(${i})" style="font-size:11px;background:none;border:none;color:var(--accent);cursor:pointer;margin-left:8px;text-decoration:underline">Set primary</button>`;
    const removeLabel = methods.length === 1 ? 'Remove &amp; disable MFA' : 'Remove';
    return `<div style="display:flex;align-items:center;justify-content:space-between;padding:12px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;margin-bottom:8px">
      <div style="font-size:13px;font-weight:600">${label}${primaryBadge}</div>
      <button class="btn btn-ghost btn-sm" style="color:var(--danger);flex-shrink:0;white-space:nowrap" onclick="mfaRemoveMethod(${i})">${removeLabel}</button>
    </div>`;
  }).join('');
}

async function mfaSetPrimary(idx) {
  const methods = _mfaMethods();
  methods.forEach((m, i) => { m.primary = (i === idx); });
  settings.mfa = { ...settings.mfa, methods, method: null, totpSecret: null };
  await _saveSettings();
  try { await kvPush(); } catch(e) { console.warn('mfaSetPrimary push failed:', e.message); }
  _renderMfaManageList();
  _updateMfaSettingsUI();
}

async function mfaRemoveMethod(idx) {
  const methods  = _mfaMethods();
  const removing = methods[idx];
  if (!removing) return;
  const label = removing.type === 'totp' ? 'authenticator app' : 'email code';
  if (methods.length === 1) { mfaDisable(); return; }
  // Snapshot updated list now before the reauth delay changes anything
  const updatedMethods = methods.filter((_, i) => i !== idx);
  if (!updatedMethods.some(m => m.primary)) updatedMethods[0].primary = true;
  requireReauth(
    `Confirm your identity to remove the ${label} from MFA.`,
    async () => {
      settings.mfa = { enabled: true, methods: updatedMethods, method: null, totpSecret: null };
      await _saveSettings();
      try { await kvPush(); } catch(e) { console.warn('mfaRemoveMethod push failed:', e.message); }
      _renderMfaManageList();
      _updateMfaSettingsUI();
      toast('Method removed');
    },
    { passkeyAllowed: true }
  );
}

function mfaDisable() {
  requireReauth(
    'Confirm your identity to disable multifactor authentication.',
    async () => {
      settings.mfa = { enabled: false, methods: [], method: null, totpSecret: null };
      await _saveSettings();
      try { await kvPush(); } catch(e) { console.warn('mfaDisable push failed:', e.message); }
      closeModal('mfa-manage-modal');
      _updateMfaSettingsUI();
      toast('MFA disabled');
    },
    { passkeyAllowed: true }
  );
}

function _updateMfaSettingsUI() {
  const enabledEl   = document.getElementById('mfa-settings-status');
  const toggleBtn   = document.getElementById('mfa-settings-toggle');
  const actionBtns  = document.getElementById('mfa-action-btns');
  const methodsList = document.getElementById('mfa-methods-list');
  const enabled  = _mfaEnabled() && _mfaMethods().length > 0;
  const methods  = _mfaMethods();

  if (enabledEl) {
    if (!enabled) {
      enabledEl.textContent = 'Disabled';
      enabledEl.style.color = '';
    } else {
      const primary = _mfaPrimaryMethod();
      const label   = primary?.type === 'totp' ? 'Authenticator app' : 'Email code';
      enabledEl.textContent = `Enabled — primary: ${label}${methods.length > 1 ? ` + ${methods.length - 1} backup` : ''}`;
      enabledEl.style.color = 'var(--ok)';
    }
  }

  // Swap action buttons based on state
  if (actionBtns) {
    if (!enabled) {
      actionBtns.innerHTML = '<button id="mfa-settings-toggle" class="btn btn-ghost acc-sec-btn" onclick="openMfaSetup()">Enable</button>';
    } else {
      actionBtns.innerHTML = `
        <button class="btn btn-ghost acc-sec-btn" onclick="openMfaManage()">Manage</button>
        ${methods.length < 2 ? '<button class="btn btn-ghost acc-sec-btn" onclick="openMfaAddMethod()">Add method</button>' : ''}`;
    }
  }

  // Show method list inline
  if (methodsList) {
    if (enabled && methods.length) {
      methodsList.style.display = '';
      methodsList.innerHTML = methods.map(m => {
        const icon  = m.type === 'totp'
          ? '<svg class="icon" aria-hidden="true" style="width:12px;height:12px"><use href="#i-smartphone"></use></svg>'
          : '<svg class="icon" aria-hidden="true" style="width:12px;height:12px"><use href="#i-mail"></use></svg>';
        const label = m.type === 'totp' ? 'Authenticator app' : 'Email code';
        return `<span style="display:inline-flex;align-items:center;gap:4px;font-size:12px;padding:3px 10px;border-radius:99px;background:${m.primary?'rgba(76,187,138,0.15)':'rgba(120,128,160,0.15)'};color:${m.primary?'var(--ok)':'var(--muted)'};margin-right:6px;margin-bottom:4px">${icon} ${label}${m.primary?' ★':''}</span>`;
      }).join('');
    } else {
      methodsList.style.display = 'none';
    }
  }

  // Compat — old UI button refs
  const n2faBtn = document.getElementById('notes-2fa-settings-btn');
  if (n2faBtn) n2faBtn.textContent = enabled ? 'Disable' : 'Enable';
  const n2faAcc = document.getElementById('notes-2fa-acc-btn');
  if (n2faAcc) n2faAcc.textContent = enabled ? 'Disable' : 'Enable';
}

function _maybeShowMfaPrompt() {
  if (_mfaEnabled()) return;
  if (localStorage.getItem(_MFA_DISMISSED_KEY)) return;
  if (!kvConnected) return;
  openModal('mfa-prompt-modal');
}

function dismissMfaPrompt() {
  localStorage.setItem(_MFA_DISMISSED_KEY, 'dismissed');
  closeModal('mfa-prompt-modal');
}
function renderAccountSecurity() {
  // Your Details
  const nameEl = document.getElementById('setting-display-name-sec');
  if (nameEl && settings.displayName) nameEl.value = settings.displayName;
  const emailEl = document.getElementById('acc-sec-email');
  if (emailEl) emailEl.textContent = settings.email || _kvEmail || '—';

  // Country
  const countryEl = document.getElementById('setting-country-sec');
  if (countryEl && COUNTRIES) {
    if (!countryEl.options.length) {
      COUNTRIES.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.code; opt.textContent = `${c.flag} ${c.name}`;
        countryEl.appendChild(opt);
      });
    }
    countryEl.value = settings.country || 'GB';
  }

  // MFA status
  _updateMfaSettingsUI();

  // Load passkeys into account security list
  if (kvConnected) {
    loadPasskeys().catch(() => {});

    loadTrustedDevices().then(() => {
      const tdListSec = document.getElementById('trusted-devices-list-sec');
      const tdListMain = document.getElementById('trusted-devices-list');
      if (tdListSec && tdListMain) tdListSec.innerHTML = tdListMain.innerHTML;
    }).catch(() => {});
  }
}

function toggleTrustedDevicesPanel() {
  const panel = document.getElementById('trusted-devices-panel-sec') || document.getElementById('trusted-devices-panel');
  if (!panel) return;
  const hidden = panel.style.display === 'none';
  panel.style.display = hidden ? 'block' : 'none';
  document.querySelectorAll('[onclick="toggleTrustedDevicesPanel()"]').forEach(btn => {
    btn.textContent = hidden ? 'Hide devices' : 'Show devices';
  });
  if (hidden) loadTrustedDevices();
}

// ── Deactivate Account ────────────────────
function openDeactivateAccount() {
  openModal('deactivate-account-modal');
}

async function confirmDeactivateAccount() {
  closeModal('deactivate-account-modal');
  try {
    const res = await fetchKV(`${WORKER_URL}/user/deactivate`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        emailHash: _kvEmailHash,
        ..._kvSessionToken ? { sessionToken: _kvSessionToken } : { verifier: _kvVerifier },
      }),
    });
    if (!res.ok) {
      const d = await res.json();
      toast('Could not deactivate: ' + (d.error || 'Unknown error'));
      return;
    }
    toast('Account deactivated. You will be signed out.');
    setTimeout(() => kvSignOut(), 2000);
  } catch(e) {
    toast('Error: ' + e.message);
  }
}

// ── Delete Account Flow ───────────────────
function openDeleteAccountFlow() {
  const inp = document.getElementById('delete-confirm-input');
  const err = document.getElementById('delete-confirm-error');
  const btn = document.getElementById('delete-confirm-btn');
  if (inp) inp.value = '';
  if (err) err.textContent = '';
  if (btn) btn.disabled = true;
  openModal('delete-account-step1-modal');
}

function deleteAccountStep2() {
  closeModal('delete-account-step1-modal');
  openModal('delete-account-step2-modal');
  setTimeout(() => document.getElementById('delete-confirm-input')?.focus(), 200);
}

function checkDeleteConfirmInput() {
  const val = document.getElementById('delete-confirm-input')?.value || '';
  const btn = document.getElementById('delete-confirm-btn');
  if (btn) btn.disabled = val !== 'Delete';
}

function deleteAccountStep3() {
  const val = document.getElementById('delete-confirm-input')?.value;
  if (val !== 'Delete') {
    const err = document.getElementById('delete-confirm-error');
    if (err) err.textContent = 'Type exactly: Delete';
    return;
  }
  closeModal('delete-account-step2-modal');
  const ppErr = document.getElementById('delete-passphrase-error');
  const ppInp = document.getElementById('delete-passphrase-input');
  if (ppErr) ppErr.textContent = '';
  if (ppInp) ppInp.value = '';
  openModal('delete-account-step3-modal');
  setTimeout(() => ppInp?.focus(), 200);
}

async function deleteAccountFinal() {
  const pass = document.getElementById('delete-passphrase-input')?.value;
  const errEl = document.getElementById('delete-passphrase-error');
  if (!pass) { if (errEl) errEl.textContent = 'Enter your passphrase'; return; }
  try {
    const verifier = await kvMakeVerifier(pass, _kvEmailHash);
    const res = await fetchKV(`${WORKER_URL}/user/delete-confirm-send`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emailHash: _kvEmailHash, verifier }),
    });
    if (!res.ok) {
      const d = await res.json();
      if (errEl) errEl.textContent = d.error || 'Incorrect passphrase';
      return;
    }
    closeModal('delete-account-step3-modal');
    openModal('delete-email-sent-modal');
  } catch(e) {
    if (errEl) errEl.textContent = 'Error: ' + e.message;
  }
}

// Called when user lands on app after clicking "Delete Account" in email
async function handleDeleteAccountConfirmation(token) {
  try {
    const res = await fetchKV(`${WORKER_URL}/user/delete-execute`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    });
    if (res.ok) {
      // Clear all local data
      try { localStorage.clear(); } catch(e) {}
      openModal('account-deleted-modal');
      // Hide the rest of the app
      document.getElementById('main')?.style && (document.getElementById('main').style.display = 'none');
    } else {
      const d = await res.json();
      toast('Could not complete deletion: ' + (d.error || 'Link may have expired'));
    }
  } catch(e) {
    toast('Error: ' + e.message);
  }
}

// Check URL for delete confirmation token on load
(function _checkDeleteToken() {
  const params = new URLSearchParams(location.search);
  const deleteToken = params.get('delete_token');
  if (deleteToken) {
    history.replaceState(null, '', location.pathname);
    // Wait for app to init before showing modal
    setTimeout(() => handleDeleteAccountConfirmation(deleteToken), 1000);
  }
  const reactivateToken = params.get('reactivate_token');
  if (reactivateToken) {
    history.replaceState(null, '', location.pathname);
    setTimeout(() => handleReactivation(reactivateToken), 1000);
  }
})();

async function handleReactivation(token) {
  try {
    const res = await fetchKV(`${WORKER_URL}/user/reactivate`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    });
    if (res.ok) {
      toast('Your account has been reactivated! Please sign in.');
    } else {
      const d = await res.json();
      toast('Reactivation failed: ' + (d.error || 'Link may have expired'));
    }
  } catch(e) {
    toast('Error: ' + e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
//  AMAZON ORDER HISTORY IMPORTER
// ═══════════════════════════════════════════════════════════════════════════

// ── Semantic anchor phrases for product clustering ─────────────────────────
const _AZ_ANCHORS = [
  'coffee beans','coffee bean','whole bean','ground coffee',
  'nespresso compatible','nespresso capsule','nespresso pod',
  'coffee pod','coffee capsule','tassimo','dolce gusto',
  'espresso capsule','espresso pod','espresso beans',
  'replacement blade','razor blade','shaving blade',
  'shampoo','conditioner','shower gel','body wash',
  'toothpaste','toothbrush head','electric toothbrush head',
  'deodorant','moisturiser','moisturizer','face wash','hand wash',
  'toilet roll','toilet paper','toilet tissue','bathroom tissue',
  'kitchen roll','kitchen towel','paper towel',
  'bin bag','bin liner','refuse sack',
  'dishwasher tablet','dishwasher pod','dishwasher capsule',
  'laundry capsule','laundry pod','washing capsule','washing pod',
  'washing powder','washing liquid','fabric conditioner','fabric softener',
  'surface spray','cleaning spray',
  'cat food','dog food','cat litter','cat treat','dog treat',
  'flea treatment','flea tablet',
  'protein powder','protein shake','whey protein',
  'vitamin d','vitamin c','omega 3','fish oil','cod liver oil',
  'olive oil','coconut oil',
  'printer ink','printer cartridge','toner cartridge',
  'aa battery','aaa battery','9v battery','cr2032',
  'water filter','filter cartridge','brita filter',
];

const _AZ_STOP = new Set([
  'the','and','for','with','pack','packs','set','box','case','bundle',
  'piece','pieces','count','units','unit','ml','litre','liter','liters',
  'litres','gram','grams','100','200','250','500','1000','large',
  'medium','small','extra','ultra','original','classic','new','pro',
  'max','plus','mini','super','premium','value','natural','organic',
  'free','made','each','per','size','type',
]);

function _azTokenise(str) {
  return (str||'').toLowerCase().replace(/[^a-z0-9\s]/g,' ').split(/\s+/)
    .filter(t => t.length > 2 && !/^\d+$/.test(t) && !_AZ_STOP.has(t));
}

function _azAnchor(name) {
  const n = (name||'').toLowerCase();
  const sorted = [..._AZ_ANCHORS].sort((a,b) => b.length - a.length);
  for (const a of sorted) if (n.includes(a)) return a;
  return null;
}

function _azJaccard(a, b) {
  const sa = new Set(a), sb = new Set(b);
  const inter = [...sa].filter(t => sb.has(t)).length;
  const union = new Set([...sa,...sb]).size;
  return union ? inter/union : 0;
}

function _azPriceBand(pa, pb, pct=0.30) {
  if (!pa || !pb) return true;
  const mid = (pa+pb)/2;
  return Math.abs(pa-pb)/mid <= pct;
}

function _azSharedKw(a, b, min=2) {
  const ta = new Set(_azTokenise(a)), tb = new Set(_azTokenise(b));
  return [...ta].filter(t => tb.has(t)).length >= min;
}

function _azShouldCluster(ga, gb) {
  const pa = ga.avgPrice||0, pb = gb.avgPrice||0;
  if (ga.anchor && ga.anchor === gb.anchor) return _azPriceBand(pa,pb,0.30);
  if (_azJaccard(ga.tokens, gb.tokens) >= 0.35) return _azPriceBand(pa,pb,0.40);
  if (_azSharedKw(ga.name, gb.name, 2)) return _azPriceBand(pa,pb,0.25);
  return false;
}

function _azAvgInterval(dates) {
  if (dates.length < 2) return null;
  const sorted = [...dates].sort();
  const dts = sorted.map(d => new Date(d).getTime());
  const gaps = dts.slice(1).map((t,i) => (t-dts[i])/86400000);
  return Math.round(gaps.reduce((a,b)=>a+b,0)/gaps.length);
}

function _azIntervalLabel(days) {
  if (!days) return 'occasional';
  if (days <= 21) return `every ~${days}d`;
  if (days <= 45) return '~monthly';
  if (days <= 75) return '~6 weekly';
  if (days <= 100) return '~2 monthly';
  if (days <= 130) return '~quarterly';
  if (days <= 200) return '~4 monthly';
  return `every ~${Math.round(days/30)}mo`;
}

function _azCategory(name) {
  const n = (name||'').toLowerCase();
  if (/coffee|tea|beans|espresso|capsule|pod|nescaf|latte|cappuccino|tassimo|dolce/i.test(n)) return '☕ Coffee & Tea';
  if (/cat|dog|pet|kitten|puppy|paw|flea|collar|litter/i.test(n)) return '🐾 Pet Supplies';
  if (/toilet|tissue|kitchen roll|paper towel|bathroom|hygiene|bin bag|bin liner/i.test(n)) return '🧻 Paper & Hygiene';
  if (/shampoo|conditioner|soap|shower|gel|moistur|lotion|cream|deodor|razors?|blade|shav/i.test(n)) return '🛁 Personal Care';
  if (/vitamin|supplement|protein|omega|tablet|capsule|health|cod liver/i.test(n)) return '💊 Health';
  if (/clean|detergent|bleach|dishwash|laundry|fabric|mop|sponge|wipe/i.test(n)) return '🧹 Cleaning';
  if (/battery|cable|charger|usb|bulb|light|led|smart|plug|adapter|filter|cartridge|ink|toner/i.test(n)) return '🔌 Electronics';
  if (/food|snack|crisp|biscuit|sauce|seasoning|oil|pasta|rice|grain|cereal|curry/i.test(n)) return '🥫 Food & Drink';
  return '📦 Other';
}

// ── State ─────────────────────────────────────────────────────────────────
let _azStage       = 'upload';   // upload|privacy|preview|analyse|results|merge|done
let _azAllRows     = [];         // all parsed CSV rows
let _azRows        = [];         // last-year filtered rows
let _azDeletedIds  = new Set();  // ids excluded from preview
let _azGroups      = [];         // analysis result groups
let _azDeletedGrps = new Set();  // group ids excluded from results
let _azSplitAsins  = {};         // { groupId: Set<asin> } — ASINs to split out
let _azExpandedSplit = new Set();// group ids with split panel expanded
let _azMatches     = [];         // merge stage match objects
let _azPickerOpen  = false;
let _azPickerIdx   = null;
let _azPickerSearch = '';

const _AZ_ONE_YEAR_AGO = new Date(Date.now()-365*24*60*60*1000).toISOString().slice(0,10);
const _AZ_STAGES = ['upload','privacy','preview','analyse','results','merge','done'];
const _AZ_STAGE_LABELS = ['Upload','Privacy','Review','Analyse','Results','Merge','Done'];

// ── Open / Close ──────────────────────────────────────────────────────────
function openAmazonImporter() {
  _azStage = 'upload'; _azAllRows=[]; _azRows=[]; _azDeletedIds=new Set();
  _azGroups=[]; _azDeletedGrps=new Set(); _azSplitAsins={}; _azMatches=[];
  const overlay = document.getElementById('amazon-import-overlay');
  if (overlay) { overlay.style.display='flex'; _azRender(); }
}

function closeAmazonImporter() {
  const overlay = document.getElementById('amazon-import-overlay');
  if (overlay) overlay.style.display = 'none';
}

// ── CSV parse ─────────────────────────────────────────────────────────────
function _azParseCSV(text) {
  const lines = text.split('\n').filter(l=>l.trim());
  const parseRow = line => {
    const cols=[]; let cur='', inQ=false;
    for (let i=0;i<line.length;i++) {
      const c=line[i];
      if (c==='"'&&!inQ){inQ=true;continue;}
      if (c==='"'&&inQ&&line[i+1]==='"'){cur+='"';i++;continue;}
      if (c==='"'&&inQ){inQ=false;continue;}
      if (c===','&&!inQ){cols.push(cur.trim());cur='';continue;}
      cur+=c;
    }
    cols.push(cur.trim()); return cols;
  };
  const headers = parseRow(lines[0]);
  const required = ['ASIN','Product Name','Order Date','Total Amount'];
  const missing = required.filter(r=>!headers.includes(r));
  if (missing.length) throw new Error(`Missing columns: ${missing.join(', ')} — are you sure this is an Order_History.csv file?`);
  return lines.slice(1).filter(l=>l.trim()).map((line,i)=>{
    const vals=parseRow(line), obj={_id:i};
    headers.forEach((h,j)=>{ obj[h]=(vals[j]||'').trim(); });
    return obj;
  });
}

function _azHandleFile(file) {
  if (!file) return;
  if (!file.name.endsWith('.csv')) { _azSetError('Please select a .csv file'); return; }
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const parsed = _azParseCSV(e.target.result);
      _azAllRows = parsed;
      _azRows = parsed
        .filter(r => (r['Order Date']||'').slice(0,10) >= _AZ_ONE_YEAR_AGO)
        .map(r => ({
          _id: r._id,
          ASIN: r['ASIN'],
          name: r['Product Name'],
          date: (r['Order Date']||'').slice(0,10),
          price: parseFloat(r['Total Amount'])||0,
        }));
      _azDeletedIds = new Set();
      _azStage = 'privacy';
      _azRender();
    } catch(err) { _azSetError(err.message); }
  };
  reader.readAsText(file);
}

function _azSetError(msg) {
  const el = document.getElementById('az-upload-error');
  if (el) el.textContent = msg;
}

// ── Analysis ─────────────────────────────────────────────────────────────
function _azRunAnalysis() {
  _azStage = 'analyse'; _azRender();
  setTimeout(() => {
    const active = _azRows.filter(r=>!_azDeletedIds.has(r._id));
    const byAsin = {};
    active.forEach(r=>{ if(!byAsin[r.ASIN]) byAsin[r.ASIN]=[]; byAsin[r.ASIN].push(r); });
    const initial = Object.entries(byAsin).map(([asin,items])=>{
      const name=items[0].name, anchor=_azAnchor(name);
      const totalSpend=items.reduce((s,r)=>s+r.price,0);
      return { id:asin, asins:[asin], name, anchor, items:items.sort((a,b)=>a.date.localeCompare(b.date)),
               category:_azCategory(name), tokens:_azTokenise(name), avgPrice:totalSpend/items.length, clusterReasons:[] };
    });
    const merged=[], used=new Set();
    for (let i=0;i<initial.length;i++) {
      if (used.has(i)) continue;
      const g={...initial[i],asins:[...initial[i].asins],items:[...initial[i].items],clusterReasons:[]};
      for (let j=i+1;j<initial.length;j++) {
        if (used.has(j)) continue;
        if (_azShouldCluster(g,initial[j])) {
          const ra=g.anchor, rb=initial[j].anchor;
          if (ra&&ra===rb) g.clusterReasons.push(`"${ra}"`);
          else if (_azJaccard(g.tokens,initial[j].tokens)>=0.35) g.clusterReasons.push('similar name');
          else g.clusterReasons.push('shared keywords+price');
          if (initial[j].items.length>g.items.length) g.name=initial[j].name;
          g.asins.push(...initial[j].asins);
          g.items.push(...initial[j].items);
          if (g.anchor!==initial[j].anchor) g.anchor=g.anchor||initial[j].anchor;
          used.add(j);
        }
      }
      g.items.sort((a,b)=>a.date.localeCompare(b.date));
      if (g.items.length>=2) {
        g.avgInterval=_azAvgInterval(g.items.map(r=>r.date));
        g.totalSpend=g.items.reduce((s,r)=>s+r.price,0);
        g.avgPrice=g.totalSpend/g.items.length;
        g.hasMerge=g.asins.length>1;
        g.clusterLabel=g.anchor?`Grouped by "${g.anchor}"`:(g.hasMerge?'Similar name & price':'Repeat purchase');
        merged.push(g);
      }
      used.add(i);
    }
    merged.sort((a,b)=>b.items.length-a.items.length);
    _azGroups=merged; _azDeletedGrps=new Set(); _azSplitAsins={}; _azExpandedSplit=new Set();
    _azStage='results'; _azRender();
  }, 1200);
}

// ── Match against existing items ──────────────────────────────────────────
function _azFindMatch(group) {
  const gAnchor=group.anchor||_azAnchor(group.name);
  const gTokens=group.tokens||_azTokenise(group.name);
  const gAvg=group.avgPrice||0;
  let best=null, bestScore=0, bestReason='', bestConf='';
  for (const item of items) {
    const iTokens=_azTokenise(item.name);
    const iAnchor=_azAnchor(item.name);
    const iAvg=item.logs?.length?item.logs.reduce((s,l)=>s+(l.price||0),0)/item.logs.length:0;
    // A: ASIN exact
    if (group.asins.includes(item.ASIN||'') && item.ASIN) return {item,confidence:'high',reason:'ASIN match'};
    // B: anchor
    if (gAnchor&&iAnchor&&gAnchor===iAnchor) {
      const score=_azPriceBand(gAvg,iAvg,0.35)?0.9:0.55;
      if (score>bestScore) { best=item; bestScore=score; bestConf=score>=0.9?'high':'low';
        bestReason=score>=0.9?`same product type ("${gAnchor}")`:`same category, different price`; }
      continue;
    }
    // C: Jaccard
    const j=_azJaccard(gTokens,iTokens);
    if (j>=0.35) {
      const ok=_azPriceBand(gAvg,iAvg,0.40), score=j*(ok?1:0.6);
      if (score>bestScore) { best=item; bestScore=score;
        bestReason=ok?'similar product name':'similar name, different price';
        bestConf=j>=0.55&&ok?'high':'medium'; }
      continue;
    }
    // D: shared keywords
    if (_azSharedKw(group.name,item.name,2)&&_azPriceBand(gAvg,iAvg,0.25)&&0.5>bestScore) {
      best=item; bestScore=0.5; bestReason='shared keywords + similar price'; bestConf='medium';
    }
  }
  return best?{item:best,confidence:bestConf,reason:bestReason}:null;
}

function _azGoToMerge() {
  const active=_azGroups.filter(g=>!_azDeletedGrps.has(g.id));
  const expanded=[];
  for (const g of active) {
    const toSplit=_azSplitAsins[g.id]||new Set();
    if (!toSplit.size) { expanded.push(g); continue; }
    for (const asin of toSplit) {
      const sub=g.items.filter(r=>r.ASIN===asin);
      if (!sub.length) continue;
      const subName=sub[0].name;
      expanded.push({...g,id:`${g.id}__${asin}`,asins:[asin],name:subName,items:sub,
        anchor:_azAnchor(subName),tokens:_azTokenise(subName),
        avgInterval:_azAvgInterval(sub.map(r=>r.date)),
        totalSpend:sub.reduce((s,r)=>s+r.price,0),avgPrice:sub.reduce((s,r)=>s+r.price,0)/sub.length,
        hasMerge:false,clusterLabel:'Split from group'});
    }
    const rem=g.asins.filter(a=>!toSplit.has(a));
    if (rem.length) {
      const remItems=g.items.filter(r=>rem.includes(r.ASIN));
      expanded.push({...g,asins:rem,items:remItems,
        avgInterval:_azAvgInterval(remItems.map(r=>r.date)),
        totalSpend:remItems.reduce((s,r)=>s+r.price,0),avgPrice:remItems.reduce((s,r)=>s+r.price,0)/remItems.length,
        hasMerge:rem.length>1});
    }
  }
  _azMatches=expanded.map(g=>{
    const f=_azFindMatch(g);
    return {group:g,existingItem:f?.item||null,matchReason:f?.reason||null,confidence:f?.confidence||null,decision:f?.item?'merge':'add'};
  });
  _azStage='merge'; _azPickerOpen=false; _azRender();
}

// ── Commit import ─────────────────────────────────────────────────────────
async function _azCommit() {
  const now = new Date().toISOString();
  for (const m of _azMatches) {
    const amazonLogs = m.group.items.map(r=>({date:r.date,qty:1,price:r.price,store:'Amazon',_fromAmazon:true}));
    if (m.decision==='merge' && m.existingItem) {
      // Merge: append amazon logs, dedup by date
      const existing = m.existingItem;
      const existingDates = new Set((existing.logs||[]).map(l=>l.date));
      const newLogs = amazonLogs.filter(l=>!existingDates.has(l.date));
      existing.logs = [...(existing.logs||[]),...newLogs].sort((a,b)=>a.date.localeCompare(b.date));
      existing.updatedAt = now;
      if (!existing.store) existing.store = 'Amazon';
      // Set ASIN if not already set
      if (!existing.ASIN && m.group.asins.length===1) existing.ASIN = m.group.asins[0];
    } else {
      // Add new item
      const newItem = {
        id: uid(), name: m.group.name, category: m.group.category.replace(/^.*? /,'') || 'Other',
        cadence: m.group.avgInterval && m.group.avgInterval<=45?'monthly':'monthly',
        qty:1, months:1, url:'', store:'Amazon', notes:'', rating:null, imageUrl:null,
        logs: amazonLogs, storePrices:[], quickAdded:false, updatedAt:now,
        ASIN: m.group.asins.length===1?m.group.asins[0]:undefined,
      };
      items.push(newItem);
    }
  }
  await saveData();
  scheduleRender('grid','dashboard','filters','shopping');
  setTimeout(syncAll, 400);
  _azStage='done'; _azRender();
}

// ── CSV download ──────────────────────────────────────────────────────────
function _azDownloadCSV() {
  const active=_azRows.filter(r=>!_azDeletedIds.has(r._id));
  const lines=['ASIN,Product Name,Order Date,Total Amount',
    ...active.map(r=>`${r.ASIN},"${(r.name||'').replace(/"/g,'""')}",${r.date},${r.price}`)];
  const blob=new Blob([lines.join('\n')],{type:'text/csv'});
  const a=document.createElement('a'); a.href=URL.createObjectURL(blob);
  a.download='stockroom_amazon_import.csv'; a.click();
}

// ── Render ────────────────────────────────────────────────────────────────
function _azRender() {
  const body=document.getElementById('amazon-import-body');
  const sub=document.getElementById('amazon-import-subtitle');
  const prog=document.getElementById('amazon-import-progress');
  if (!body) return;

  // Progress bar
  const si=_AZ_STAGES.indexOf(_azStage);
  if (prog) prog.innerHTML=_AZ_STAGE_LABELS.map((l,i)=>`
    <div style="display:flex;align-items:center;gap:4px">
      ${i>0?`<div style="width:14px;height:2px;background:${i<=si?'var(--ok)':'var(--border)'}"></div>`:''}
      <div style="width:20px;height:20px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;font-family:var(--mono);
        background:${i<si?'var(--ok)':i===si?'var(--accent)':'var(--border)'};color:${i<=si?'#111':'var(--muted)'}">
        ${i<si?'<svg class="icon" aria-hidden="true"><use href="#i-check"></use></svg>':i+1}</div>
      <span style="font-size:10px;color:${i===si?'var(--text)':'var(--muted)'}">${l}</span>
    </div>`).join('');

  if (_azStage==='upload') { if(sub) sub.textContent='Step 1 of 7 — Upload your file'; body.innerHTML=_azHtmlUpload(); _azBindDropzone(); }
  else if (_azStage==='privacy') { if(sub) sub.textContent='Step 2 — Privacy notice'; body.innerHTML=_azHtmlPrivacy(); }
  else if (_azStage==='preview') { if(sub) sub.textContent='Step 3 — Review data'; body.innerHTML=_azHtmlPreview(); }
  else if (_azStage==='analyse') { if(sub) sub.textContent='Analysing…'; body.innerHTML=_azHtmlAnalyse(); }
  else if (_azStage==='results') { if(sub) sub.textContent='Step 5 — Review patterns found'; body.innerHTML=_azHtmlResults(); _azBindResults(); }
  else if (_azStage==='merge')   { if(sub) sub.textContent='Step 6 — Match with your Stockroom'; body.innerHTML=_azHtmlMerge(); _azBindMerge(); }
  else if (_azStage==='done')    { if(sub) sub.textContent='Import complete'; body.innerHTML=_azHtmlDone(); }
}

// ── Stage HTML builders ───────────────────────────────────────────────────
function _azHtmlUpload() {
  return `
  <h2 style="font-size:22px;font-weight:700;margin-bottom:8px">Import your Amazon order history</h2>
  <p style="color:var(--muted);font-size:14px;line-height:1.7;margin-bottom:24px">
    Amazon lets you export your full order history. We'll use it to spot items you buy repeatedly so you can track them automatically — saving time on reorders and keeping you stocked up.
  </p>
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:20px">
    <div style="font-size:12px;font-weight:700;color:var(--accent);margin-bottom:12px;font-family:var(--mono);letter-spacing:1px">HOW TO GET YOUR FILE</div>
    <div style="display:flex;gap:12px;margin-bottom:10px;align-items:flex-start">
      <div style="width:22px;height:22px;border-radius:50%;background:var(--accent);color:#111;font-weight:700;font-size:11px;display:flex;align-items:center;justify-content:center;flex-shrink:0">1</div>
      <div><strong>Go to Amazon's privacy page: </strong><a href="https://www.amazon.co.uk/hz/privacy-central/data-requests/preview.html" target="_blank" rel="noopener" style="color:var(--blue,#5b8dee);font-family:var(--mono);font-size:11px;word-break:break-all">amazon.co.uk/hz/privacy-central/data-requests/preview.html</a></div>
    </div>
    <div style="display:flex;gap:12px;margin-bottom:10px;align-items:flex-start">
      <div style="width:22px;height:22px;border-radius:50%;background:var(--accent);color:#111;font-weight:700;font-size:11px;display:flex;align-items:center;justify-content:center;flex-shrink:0">2</div>
      <div><strong>Request your data: </strong><span style="color:var(--muted);font-size:13px">Select "Order History" and submit — Amazon will email when it's ready (usually minutes)</span></div>
    </div>
    <div style="display:flex;gap:12px;margin-bottom:10px;align-items:flex-start">
      <div style="width:22px;height:22px;border-radius:50%;background:var(--accent);color:#111;font-weight:700;font-size:11px;display:flex;align-items:center;justify-content:center;flex-shrink:0">3</div>
      <div><strong>Download: </strong><span style="color:var(--muted);font-size:13px">Save as Order_History.csv from the same page</span></div>
    </div>
    <div style="display:flex;gap:12px;align-items:flex-start">
      <div style="width:22px;height:22px;border-radius:50%;background:var(--accent);color:#111;font-weight:700;font-size:11px;display:flex;align-items:center;justify-content:center;flex-shrink:0">4</div>
      <div><strong>Optional but recommended: </strong><span style="color:var(--muted);font-size:13px">Remove private columns before uploading — explained on the next screen</span></div>
    </div>
  </div>
  <label id="az-dropzone" style="display:block;border:2px dashed var(--border);border-radius:16px;padding:48px 24px;text-align:center;cursor:pointer">
    <div style="margin-bottom:10px;color:var(--accent)"><svg aria-hidden="true" style="width:36px;height:36px"><use href="#i-folder-open"></use></svg></div>
    <div style="font-weight:700;margin-bottom:6px">Drop Order_History.csv here</div>
    <div style="color:var(--muted);font-size:13px">or click to browse</div>
    <input type="file" accept=".csv" id="az-file-input" style="display:none" onchange="(e=>_azHandleFile(e.target.files[0]))(event)">
  </label>
  <div id="az-upload-error" style="color:var(--danger);font-size:13px;margin-top:10px;min-height:18px"></div>
  <div style="margin-top:16px;padding:12px 16px;background:rgba(91,141,238,0.08);border:1px solid rgba(91,141,238,0.2);border-radius:10px;font-size:12px;color:var(--muted);line-height:1.7">
    🔒 <strong style="color:var(--text)">Your data stays private.</strong> All processing happens locally in your browser. Data is encrypted on your device before any upload to STOCKROOM.
  </div>`;
}

function _azHtmlPrivacy() {
  const private_cols=['Billing Address','Shipping Address','Payment Method Type'];
  const keep_cols=['ASIN','Product Name','Order Date','Total Amount'];
  const totalOrders=_azAllRows.length, recentOrders=_azRows.length, uniqueAsins=new Set(_azRows.map(r=>r.ASIN)).size;
  return `
  <h2 style="font-size:22px;font-weight:700;margin-bottom:8px"><svg class="icon icon-md" aria-hidden="true" style="vertical-align:-3px;color:var(--accent)"><use href="#i-alert-triangle"></use></svg> Before we continue</h2>
  <p style="color:var(--muted);font-size:14px;line-height:1.7;margin-bottom:20px">Your file contains sensitive personal information. We only need four columns — everything else is discarded locally. We recommend removing private columns first.</p>
  <div style="background:rgba(224,92,92,0.08);border:1px solid rgba(224,92,92,0.25);border-radius:12px;padding:16px;margin-bottom:14px">
    <div style="font-size:11px;font-weight:700;color:var(--danger);margin-bottom:10px;font-family:var(--mono);letter-spacing:1px">RECOMMENDED: REMOVE THESE COLUMNS FIRST</div>
    ${private_cols.map(c=>`<div style="display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid rgba(224,92,92,0.12)">
      <span style="color:var(--danger)"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></span><span style="font-family:var(--mono);font-size:12px">${c}</span><span style="color:var(--muted);font-size:11px;margin-left:auto">personal data</span>
    </div>`).join('')}
    <p style="font-size:12px;color:var(--muted);margin-top:10px;line-height:1.6">Open <strong>Order_History.csv</strong> in Microsoft Excel, Google Sheets, or LibreOffice Calc. Select and delete these three columns, save, then re-upload.</p>
  </div>
  <div style="background:rgba(76,187,138,0.06);border:1px solid rgba(76,187,138,0.2);border-radius:12px;padding:16px;margin-bottom:20px">
    <div style="font-size:11px;font-weight:700;color:var(--ok);margin-bottom:10px;font-family:var(--mono);letter-spacing:1px">ONLY THESE COLUMNS ARE USED</div>
    ${keep_cols.map(c=>`<div style="display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid rgba(76,187,138,0.1)">
      <span style="color:var(--ok)">✓</span><span style="font-family:var(--mono);font-size:12px">${c}</span>
    </div>`).join('')}
  </div>
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px 16px;margin-bottom:20px;font-size:12px;color:var(--muted);line-height:1.8">
    📊 <strong style="color:var(--text)">Found in your file:</strong>
    ${totalOrders} total orders &nbsp;·&nbsp; <strong style="color:var(--text)">${recentOrders}</strong> in the last 12 months &nbsp;·&nbsp; ${uniqueAsins} unique products
  </div>
  <div style="display:flex;gap:10px;flex-wrap:wrap">
    <button onclick="_azStage='preview';_azRender()" style="flex:1;padding:12px 20px;background:var(--ok);color:#111;border:none;border-radius:10px;font-weight:700;font-size:14px;cursor:pointer">Continue with this file →</button>
    <button onclick="_azStage='upload';_azRender()" style="padding:12px 20px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:10px;font-weight:600;font-size:13px;cursor:pointer">Re-upload cleaned file</button>
  </div>`;
}

function _azHtmlPreview() {
  const active=_azRows.filter(r=>!_azDeletedIds.has(r._id));
  return `
  <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px">
    <div>
      <h2 style="font-size:22px;font-weight:700;margin-bottom:4px">Review your order data</h2>
      <p style="color:var(--muted);font-size:13px">${active.length} orders · last 12 months only · tap ✕ to exclude any</p>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button onclick="_azDownloadCSV()" style="padding:8px 14px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:8px;font-size:12px;cursor:pointer;font-weight:600">⬇ Download CSV</button>
      <button onclick="_azRunAnalysis()" style="padding:8px 18px;background:var(--accent);color:#111;border:none;border-radius:8px;font-size:13px;cursor:pointer;font-weight:700">Analyse →</button>
    </div>
  </div>
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden">
    <div style="display:grid;grid-template-columns:110px 1fr 90px 72px 30px;padding:8px 14px;border-bottom:1px solid var(--border);font-size:10px;font-weight:700;color:var(--muted);font-family:var(--mono);letter-spacing:0.5px;gap:8px">
      <span>ASIN</span><span>Product</span><span>Date</span><span style="text-align:right">Price</span><span></span>
    </div>
    <div style="max-height:400px;overflow-y:auto" id="az-preview-list">
      ${_azRows.filter(r=>!_azDeletedIds.has(r._id)).map(r=>`
        <div class="az-preview-row" data-id="${r._id}" style="display:grid;grid-template-columns:110px 1fr 90px 72px 30px;padding:8px 14px;border-bottom:1px solid var(--border);font-size:12px;align-items:center;gap:8px">
          <span style="font-family:var(--mono);font-size:10px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.ASIN)}</span>
          <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.name)}</span>
          <span style="color:var(--muted);font-size:11px">${r.date}</span>
          <span style="text-align:right;font-family:var(--mono);font-size:11px;color:var(--ok)">£${r.price.toFixed(2)}</span>
          <button onclick="_azDeleteRow(${r._id},this)" style="background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:2px"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
        </div>`).join('')}
    </div>
  </div>
  ${_azDeletedIds.size>0?`<p style="margin-top:8px;font-size:12px;color:var(--muted)">${_azDeletedIds.size} item${_azDeletedIds.size!==1?'s':''} excluded · <button onclick="_azDeletedIds=new Set();_azRender()" style="background:none;border:none;color:var(--accent);cursor:pointer;font-size:12px;text-decoration:underline;padding:0">Restore all</button></p>`:''}`;
}

function _azHtmlAnalyse() {
  return `<div style="text-align:center;padding:60px 20px">
    <div style="margin-bottom:16px;display:inline-block;animation:az-spin 2s linear infinite;color:var(--accent)"><svg aria-hidden="true" style="width:48px;height:48px"><use href="#i-search"></use></svg></div>
    <style>@keyframes az-spin{from{transform:rotate(0)}to{transform:rotate(360deg)}}@keyframes az-pulse{0%,100%{opacity:.4}50%{opacity:1}}</style>
    <h2 style="font-size:20px;font-weight:700;margin-bottom:8px">Analysing your orders…</h2>
    <p style="color:var(--muted);font-size:13px;line-height:1.7;max-width:360px;margin:0 auto 20px">Finding repeat purchases, matching ASINs, and detecting similar products — all locally on your device.</p>
    ${['Filtering last 12 months','Grouping by ASIN','Detecting similar products','Calculating purchase intervals','Building recommendations'].map((s,i)=>`
      <div style="display:flex;align-items:center;gap:8px;max-width:280px;margin:6px auto;animation:az-pulse 1.5s ease ${i*0.3}s infinite">
        <div style="width:6px;height:6px;border-radius:50%;background:var(--accent);flex-shrink:0"></div>
        <span style="font-size:12px;color:var(--muted)">${s}</span>
      </div>`).join('')}
  </div>`;
}

function _azHtmlResults() {
  const active=_azGroups.filter(g=>!_azDeletedGrps.has(g.id));
  const cats=[...new Set(active.map(g=>g.category))];
  return `
  <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px">
    <div>
      <h2 style="font-size:22px;font-weight:700;margin-bottom:4px">${_azGroups.length} repeat purchase pattern${_azGroups.length!==1?'s':''} found</h2>
      <p style="color:var(--muted);font-size:13px;line-height:1.6">Review items STOCKROOM could track. Expand multi-ASIN groups to split distinct products.</p>
    </div>
    <button onclick="_azGoToMerge()" style="padding:10px 20px;background:var(--accent);color:#111;border:none;border-radius:10px;font-weight:700;font-size:14px;cursor:pointer">+ Add ${active.length} item${active.length!==1?'s':''} →</button>
  </div>
  <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px">
    ${cats.map(cat=>`<div style="padding:3px 12px;background:var(--surface);border:1px solid var(--border);border-radius:99px;font-size:12px;color:var(--muted)">${cat} <strong style="color:var(--text)">${active.filter(g=>g.category===cat).length}</strong></div>`).join('')}
  </div>
  <div id="az-results-list">
    ${active.map(g=>_azGroupCard(g)).join('')}
  </div>
  ${_azDeletedGrps.size>0?`<p style="font-size:12px;color:var(--muted);margin-top:8px">${_azDeletedGrps.size} removed · <button onclick="_azDeletedGrps=new Set();_azRender()" style="background:none;border:none;color:var(--accent);cursor:pointer;font-size:12px;text-decoration:underline;padding:0">Restore all</button></p>`:''}`;
}

function _azGroupCard(g) {
  const isExpanded=_azExpandedSplit.has(g.id);
  const splits=_azSplitAsins[g.id]||new Set();
  const recent=g.items.slice(-6);
  const projDate=(()=>{
    if (!g.avgInterval) return null;
    const last=new Date(g.items[g.items.length-1].date+'T12:00:00');
    const next=new Date(last.getTime()+g.avgInterval*86400000);
    return next>new Date()?next.toISOString().slice(0,10):null;
  })();
  return `
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:14px;margin-bottom:12px;overflow:hidden">
    <div style="padding:12px 16px;display:flex;gap:10px;align-items:flex-start;border-bottom:1px solid var(--border)">
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;flex-wrap:wrap">
          <span style="font-size:10px;background:rgba(232,168,56,0.15);color:var(--accent);padding:2px 8px;border-radius:99px;font-family:var(--mono);font-weight:700">${esc(g.category)}</span>
          <span style="font-size:10px;color:var(--muted);font-family:var(--mono)">${g.items.length} orders</span>
          ${g.avgInterval?`<span style="font-size:10px;color:#5b8dee;font-family:var(--mono)">⟳ ${_azIntervalLabel(g.avgInterval)}</span>`:''}
          ${g.hasMerge&&g.clusterLabel?`<span style="font-size:10px;color:var(--muted);background:var(--bg);padding:1px 7px;border-radius:99px;border:1px solid var(--border)" title="${esc(g.clusterReasons.join(', '))}">🔗 ${esc(g.clusterLabel)}</span>`:''}
        </div>
        <div style="font-weight:700;font-size:14px;line-height:1.3;margin-bottom:4px">${esc(g.name)}</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
          <span style="font-size:11px;color:var(--muted)">ASINs: ${g.asins.slice(0,3).map(a=>`<span style="font-family:var(--mono);background:var(--bg);padding:1px 5px;border-radius:4px;font-size:10px">${esc(a)}</span>`).join('')}${g.asins.length>3?`<span style="font-size:10px;color:var(--muted)"> +${g.asins.length-3}</span>`:''}</span>
          <span style="font-size:11px;color:var(--ok);font-weight:600">£${g.totalSpend.toFixed(2)} spent</span>
          ${g.avgPrice?`<span style="font-size:11px;color:var(--muted)">avg £${g.avgPrice.toFixed(2)}</span>`:''}
        </div>
      </div>
      <button onclick="_azDeleteGroup('${g.id}',this)" style="background:none;border:1px solid var(--border);color:var(--muted);cursor:pointer;font-size:12px;padding:4px 10px;border-radius:6px;font-weight:600;flex-shrink:0">Remove</button>
    </div>
    <!-- Timeline -->
    <div style="padding:10px 16px;display:flex;align-items:center;gap:5px;overflow-x:auto;padding-bottom:12px">
      ${recent.map((item,i)=>`
        ${i>0?'<div style="width:20px;height:1px;background:var(--border);flex-shrink:0"></div>':''}
        <div style="text-align:center;flex-shrink:0">
          <div style="width:8px;height:8px;border-radius:50%;background:var(--accent);margin:0 auto 3px"></div>
          <div style="font-size:9px;color:var(--muted);font-family:var(--mono);white-space:nowrap">${item.date.slice(5)}</div>
          <div style="font-size:9px;color:var(--ok);font-family:var(--mono)">£${item.price.toFixed(0)}</div>
        </div>`).join('')}
      ${projDate?`
        <div style="width:20px;height:1px;border-top:1px dashed var(--border);flex-shrink:0"></div>
        <div style="text-align:center;flex-shrink:0;opacity:.6">
          <div style="width:8px;height:8px;border-radius:50%;border:2px dashed var(--accent);margin:0 auto 3px"></div>
          <div style="font-size:9px;color:var(--accent);font-family:var(--mono);white-space:nowrap">${projDate.slice(5)}</div>
          <div style="font-size:9px;color:var(--muted);font-family:var(--mono)">due</div>
        </div>`:''}
      ${g.items.length>6?`<div style="font-size:10px;color:var(--muted);font-family:var(--mono);flex-shrink:0">+${g.items.length-6} more</div>`:''}
    </div>
    <!-- ASIN split panel -->
    ${g.hasMerge?`
    <div style="border-top:1px solid var(--border);background:rgba(91,141,238,0.04)">
      <div style="padding:9px 16px;display:flex;align-items:center;gap:10px">
        <span style="font-size:12px;color:#5b8dee;flex:1">🔗 ${g.asins.length} ASINs grouped${splits.size>0?` · <strong style="color:var(--accent)">${splits.size} marked to split</strong>`:''}</span>
        <button onclick="_azToggleSplitPanel('${g.id}')" style="padding:4px 12px;background:transparent;border:1px solid var(--border);color:var(--muted);border-radius:6px;font-size:11px;cursor:pointer;font-weight:600">${isExpanded?'▲ Hide':'▼ Review ASINs'}</button>
      </div>
      ${isExpanded?`
      <div style="padding:4px 16px 12px">
        <p style="font-size:11px;color:var(--muted);margin-bottom:8px;line-height:1.5">Tick any ASINs that are <em>distinct products</em> and should be tracked separately.</p>
        ${g.asins.map(asin=>{
          const asinItems=g.items.filter(r=>r.ASIN===asin);
          const asinName=asinItems[0]?.name||asin;
          const asinAvg=asinItems.reduce((s,r)=>s+r.price,0)/asinItems.length;
          const isSplit=splits.has(asin);
          return `<label style="display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:8px;margin-bottom:6px;background:${isSplit?'rgba(232,168,56,0.08)':'rgba(255,255,255,0.02)'};border:1px solid ${isSplit?'var(--accent)':'var(--border)'};cursor:pointer">
            <input type="checkbox" ${isSplit?'checked':''} onchange="_azToggleSplit('${g.id}','${asin}',this.checked)" style="accent-color:var(--accent);width:15px;height:15px;flex-shrink:0">
            <div style="flex:1;min-width:0">
              <div style="font-size:12px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:${isSplit?'var(--accent)':'var(--text)'}">${esc(asinName)}</div>
              <div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:1px">${esc(asin)} · ${asinItems.length} orders · avg £${asinAvg.toFixed(2)}</div>
            </div>
            ${isSplit?'<span style="font-size:10px;color:var(--accent);font-family:var(--mono);font-weight:700;flex-shrink:0">SPLIT</span>':''}
          </label>`;
        }).join('')}
        ${splits.size>0?`<div style="font-size:11px;color:var(--muted);margin-top:4px;padding:6px 8px;background:rgba(232,168,56,0.06);border-radius:6px">✓ ${splits.size} ASIN${splits.size!==1?'s':''} will become separate items.</div>`:''}
      </div>`:''}
    </div>`:''}
  </div>`;
}

function _azHtmlMerge() {
  const withMatch=_azMatches.filter(m=>m.existingItem);
  const withoutMatch=_azMatches.filter(m=>!m.existingItem);
  const merging=_azMatches.filter(m=>m.decision==='merge').length;
  const adding=_azMatches.filter(m=>m.decision==='add').length;
  const confColour=c=>c==='high'?'var(--ok)':c==='medium'?'var(--accent)':c==='manual'?'#5b8dee':'var(--muted)';
  const confLabel=c=>c==='high'?'✓ High confidence':c==='medium'?'~ Medium confidence':c==='manual'?'✎ Manually matched':c==='low'?'? Low confidence':'';
  return `
  <!-- Manual picker (inline overlay) -->
  ${_azPickerOpen?`
  <div style="position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:600;display:flex;align-items:center;justify-content:center;padding:20px" onclick="if(event.target===this){_azPickerOpen=false;_azRender()}">
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;width:100%;max-width:480px;max-height:80vh;display:flex;flex-direction:column;overflow:hidden" onclick="event.stopPropagation()">
      <div style="padding:14px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px">
        <div style="flex:1;min-width:0">
          <div style="font-weight:700;font-size:14px;margin-bottom:2px">Match to existing item</div>
          <div style="font-size:12px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">For: ${esc(_azPickerIdx!==null?(_azMatches[_azPickerIdx]?.group.name||'').slice(0,50):'')}</div>
        </div>
        <button onclick="_azPickerOpen=false;_azRender()" style="background:none;border:1px solid var(--border);color:var(--muted);border-radius:8px;padding:5px 12px;cursor:pointer;font-size:13px">Cancel</button>
      </div>
      <div style="padding:10px 20px;border-bottom:1px solid var(--border)">
        <input type="text" placeholder="Search your STOCKROOM items…" value="${esc(_azPickerSearch)}" oninput="_azPickerSearch=this.value;_azRender()" autofocus
          style="width:100%;box-sizing:border-box;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:9px 12px;color:var(--text);font-size:13px;outline:none">
      </div>
      <div style="overflow-y:auto;flex:1">
        ${items.filter(it=>!_azPickerSearch||it.name.toLowerCase().includes(_azPickerSearch.toLowerCase())).map(it=>{
          const avgP=it.logs?.length?it.logs.reduce((s,l)=>s+(l.price||0),0)/it.logs.length:null;
          return `<button onclick="_azApplyMatch(${items.indexOf(it)})" style="display:flex;align-items:center;gap:12px;width:100%;padding:12px 20px;background:transparent;border:none;border-bottom:1px solid var(--border);cursor:pointer;text-align:left"
            onmouseover="this.style.background='rgba(255,255,255,0.03)'" onmouseout="this.style.background='transparent'">
            <div style="width:32px;height:32px;border-radius:8px;background:rgba(232,168,56,0.12);display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0">📦</div>
            <div style="flex:1;min-width:0">
              <div style="font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(it.name)}</div>
              <div style="font-size:11px;color:var(--muted);margin-top:1px">${it.logs?.length||0} log entries${it.store?` · ${esc(it.store)}`:''}${avgP?` · avg £${avgP.toFixed(2)}`:''}</div>
            </div>
            <span style="font-size:12px;color:var(--accent);font-weight:600;flex-shrink:0">Match →</span>
          </button>`;
        }).join('')}
        ${items.filter(it=>!_azPickerSearch||it.name.toLowerCase().includes(_azPickerSearch.toLowerCase())).length===0?`<div style="padding:32px;text-align:center;color:var(--muted);font-size:13px">No items match "${esc(_azPickerSearch)}"</div>`:''}
      </div>
    </div>
  </div>`:''}

  <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:12px">
    <div>
      <h2 style="font-size:22px;font-weight:700;margin-bottom:4px">Match with existing items</h2>
      <p style="color:var(--muted);font-size:13px;line-height:1.6">${withMatch.length} matched automatically · ${withoutMatch.length} added as new · use "Match manually" for anything missed.</p>
    </div>
    <button onclick="_azCommit()" style="padding:10px 22px;background:var(--ok);color:#111;border:none;border-radius:10px;font-weight:700;font-size:14px;cursor:pointer;white-space:nowrap">✓ Import (${merging} merge, ${adding} add)</button>
  </div>

  ${withMatch.length?`
  <div style="font-size:10px;font-weight:700;color:var(--accent);font-family:var(--mono);letter-spacing:1px;margin:14px 0 8px">AUTO-MATCHED — ${withMatch.length}</div>
  ${withMatch.map(m=>{
    const idx=_azMatches.indexOf(m);
    return `<div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:10px;overflow:hidden">
      <div style="padding:10px 16px;border-bottom:1px solid var(--border);display:flex;gap:10px;align-items:center">
        <div style="width:26px;height:26px;border-radius:7px;background:rgba(232,168,56,0.12);display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0">📦</div>
        <div style="flex:1;min-width:0">
          <div style="font-size:10px;color:var(--accent);font-family:var(--mono);font-weight:700">FROM AMAZON</div>
          <div style="font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(m.group.name)}</div>
        </div>
        <div style="font-size:11px;color:var(--muted);text-align:right;flex-shrink:0"><div>${m.group.items.length} orders</div><div style="color:var(--ok)">£${m.group.totalSpend.toFixed(2)}</div></div>
      </div>
      <div style="padding:10px 16px;border-bottom:1px solid var(--border);display:flex;gap:10px;align-items:center;background:rgba(76,187,138,0.03)">
        <div style="width:26px;height:26px;border-radius:7px;background:rgba(76,187,138,0.12);display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0">🏠</div>
        <div style="flex:1;min-width:0">
          <div style="font-size:10px;color:var(--ok);font-family:var(--mono);font-weight:700">IN STOCKROOM</div>
          <div style="font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(m.existingItem.name)}</div>
        </div>
        <div style="flex-shrink:0;text-align:right">
          <div style="font-size:10px;color:${confColour(m.confidence)};font-family:var(--mono);font-weight:700">${confLabel(m.confidence)}</div>
          ${m.matchReason?`<div style="font-size:10px;color:var(--muted);margin-top:1px">${esc(m.matchReason)}</div>`:''}
        </div>
      </div>
      <div style="padding:9px 16px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <button onclick="_azSetDecision(${idx},'merge')" style="padding:6px 14px;border:1px solid ${m.decision==='merge'?'var(--ok)':'var(--border)'};background:${m.decision==='merge'?'rgba(76,187,138,0.12)':'transparent'};color:${m.decision==='merge'?'var(--ok)':'var(--muted)'};border-radius:7px;font-size:12px;cursor:pointer;font-weight:600">↩ Merge history in</button>
        <button onclick="_azSetDecision(${idx},'add')" style="padding:6px 14px;border:1px solid ${m.decision==='add'?'#5b8dee':'var(--border)'};background:${m.decision==='add'?'rgba(91,141,238,0.12)':'transparent'};color:${m.decision==='add'?'#5b8dee':'var(--muted)'};border-radius:7px;font-size:12px;cursor:pointer;font-weight:600">+ Add as new</button>
        <div style="margin-left:auto;display:flex;gap:6px">
          <button onclick="_azOpenPicker(${idx})" style="padding:5px 12px;background:transparent;border:1px solid var(--border);color:var(--muted);border-radius:7px;font-size:11px;cursor:pointer">✎ Change match</button>
          <button onclick="_azClearMatch(${idx})" title="Remove match — will add as new" style="padding:5px 10px;background:transparent;border:1px solid var(--border);color:var(--muted);border-radius:7px;font-size:11px;cursor:pointer"><svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg></button>
        </div>
      </div>
      ${m.decision==='merge'?`<div style="padding:6px 16px 10px;background:rgba(76,187,138,0.04);font-size:11px;color:var(--muted)">Will add ${m.group.items.length} Amazon purchase entries to <strong style="color:var(--text)">${esc(m.existingItem.name)}</strong>.</div>`:''}
    </div>`;
  }).join('')}`:''}

  ${withoutMatch.length?`
  <div style="font-size:10px;font-weight:700;color:var(--muted);font-family:var(--mono);letter-spacing:1px;margin:18px 0 8px">NO MATCH FOUND — ADDING AS NEW · ${withoutMatch.length}</div>
  ${withoutMatch.map(m=>{
    const idx=_azMatches.indexOf(m);
    return `<div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;margin-bottom:8px;padding:11px 16px;display:flex;gap:10px;align-items:center">
      <div style="width:26px;height:26px;border-radius:7px;background:rgba(91,141,238,0.12);display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0">➕</div>
      <div style="flex:1;min-width:0">
        <div style="font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(m.group.name)}</div>
        <div style="font-size:11px;color:var(--muted);margin-top:2px">${esc(m.group.category)} · ${m.group.items.length} orders · ${_azIntervalLabel(m.group.avgInterval)} · <span style="color:var(--ok)">£${m.group.totalSpend.toFixed(2)}</span></div>
      </div>
      <button onclick="_azOpenPicker(${idx})" style="padding:6px 14px;background:rgba(91,141,238,0.1);border:1px solid #5b8dee;color:#5b8dee;border-radius:7px;font-size:12px;cursor:pointer;font-weight:600;flex-shrink:0">✎ Match manually</button>
    </div>`;
  }).join('')}`:''}`;
}

function _azHtmlDone() {
  const merged=_azMatches.filter(m=>m.decision==='merge');
  const added=_azMatches.filter(m=>m.decision==='add');
  return `<div style="text-align:center;padding:60px 20px">
    <div style="font-size:52px;margin-bottom:16px"><svg class="icon icon-xl" aria-hidden="true"><use href="#i-party-popper"></use></svg></div>
    <h2 style="font-size:24px;font-weight:700;margin-bottom:8px">Import complete</h2>
    <p style="color:var(--muted);font-size:14px;line-height:1.7;max-width:400px;margin:0 auto 24px">Your Amazon order history has been imported. STOCKROOM will now track these items and alert you when stock is running low.</p>
    <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-bottom:28px">
      ${merged.length?`<div style="background:var(--surface);border:1px solid var(--ok);border-radius:12px;padding:16px 24px;min-width:140px"><div style="font-size:32px;font-weight:800;color:var(--ok)">${merged.length}</div><div style="font-size:12px;color:var(--muted);margin-top:4px">merged into existing items</div></div>`:''}
      ${added.length?`<div style="background:var(--surface);border:1px solid #5b8dee;border-radius:12px;padding:16px 24px;min-width:140px"><div style="font-size:32px;font-weight:800;color:#5b8dee">${added.length}</div><div style="font-size:12px;color:var(--muted);margin-top:4px">added as new items</div></div>`:''}
    </div>
    <button onclick="closeAmazonImporter()" style="padding:10px 24px;background:var(--accent);color:#111;border:none;border-radius:10px;font-weight:700;font-size:14px;cursor:pointer;margin-right:10px">Done ✓</button>
    <button onclick="openAmazonImporter()" style="padding:10px 24px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:10px;font-size:13px;cursor:pointer">Import another file</button>
  </div>`;
}

// ── Event handlers ────────────────────────────────────────────────────────
function _azDeleteRow(id, btn) {
  _azDeletedIds.add(id);
  const row = btn?.closest('.az-preview-row');
  if (row) row.remove();
  // Update header count
  const active = _azRows.filter(r=>!_azDeletedIds.has(r._id));
  const hdr = document.querySelector('#amazon-import-body p[style*="color:var(--muted)"]');
  if (hdr) hdr.textContent = active.length + ' orders · last 12 months only · tap ✕ to exclude any';
}

function _azDeleteGroup(id, btn) {
  _azDeletedGrps.add(id);
  // Remove the card from the DOM immediately without full re-render
  const card = btn?.closest('#az-results-list > div');
  if (card) card.remove();
  // Update the Add button count
  const active = _azGroups.filter(g=>!_azDeletedGrps.has(g.id));
  const addBtn = document.querySelector('#amazon-import-body button[onclick="_azGoToMerge()"]');
  if (addBtn) addBtn.textContent = `+ Add ${active.length} item${active.length!==1?'s':''} →`;
}

function _azBindDropzone() {
  const dz=document.getElementById('az-dropzone');
  const fi=document.getElementById('az-file-input');
  if (dz) {
    dz.addEventListener('dragover',e=>{e.preventDefault();dz.style.borderColor='var(--accent)';dz.style.background='rgba(232,168,56,0.05)'});
    dz.addEventListener('dragleave',()=>{dz.style.borderColor='var(--border)';dz.style.background='transparent'});
    dz.addEventListener('drop',e=>{e.preventDefault();dz.style.borderColor='var(--border)';dz.style.background='transparent';_azHandleFile(e.dataTransfer.files[0])});
    dz.addEventListener('click',()=>fi?.click());
  }
}

function _azBindResults() {
  // Results stage doesn't need extra binding — all in inline handlers
}

function _azBindMerge() {
  // Merge stage rendered with inline handlers — autofocus picker search if open
  if (_azPickerOpen) {
    setTimeout(()=>{
      const inp=document.querySelector('#amazon-import-body input[type="text"]');
      if (inp) inp.focus();
    },50);
  }
}

function _azToggleSplitPanel(groupId) {
  if (_azExpandedSplit.has(groupId)) _azExpandedSplit.delete(groupId);
  else _azExpandedSplit.add(groupId);
  _azRender();
}

function _azToggleSplit(groupId, asin, checked) {
  if (!_azSplitAsins[groupId]) _azSplitAsins[groupId]=new Set();
  const g=_azGroups.find(g=>g.id===groupId);
  if (!g) return;
  if (checked) {
    // Don't allow splitting all ASINs
    if (_azSplitAsins[groupId].size>=g.asins.length-1) return;
    _azSplitAsins[groupId].add(asin);
  } else {
    _azSplitAsins[groupId].delete(asin);
  }
  _azRender();
}

function _azSetDecision(idx, decision) {
  if (_azMatches[idx]) { _azMatches[idx].decision=decision; _azRender(); }
}

function _azOpenPicker(idx) {
  _azPickerOpen=true; _azPickerIdx=idx; _azPickerSearch=''; _azRender();
}

function _azApplyMatch(itemIdx) {
  const item=items[itemIdx];
  if (!item||_azPickerIdx===null) return;
  _azMatches[_azPickerIdx]={..._azMatches[_azPickerIdx],existingItem:item,matchReason:'manually matched',confidence:'manual',decision:'merge'};
  _azPickerOpen=false; _azPickerIdx=null; _azRender();
}

function _azClearMatch(idx) {
  if (_azMatches[idx]) { _azMatches[idx]={..._azMatches[idx],existingItem:null,matchReason:null,confidence:null,decision:'add'}; _azRender(); }
}
