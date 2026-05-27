// ═══════════════════════════════════════════════════════════════════
//  DEMO MODE — extracted from app.js (Option C plain-script split)
// ═══════════════════════════════════════════════════════════════════
//
// This file contains the entire Demo Mode feature: the two persona
// seeders (Couple and Family), the persistent banner, the contextual
// nudges, and the demo→real account conversion flow.
//
// Loading model: this is a PLAIN <script>, not type="module". It is
// loaded in index.html AFTER app.js, budget.js, and notes.js (all
// with `defer`). By the time these top-level declarations execute,
// every global this file depends on has been defined.
//
// Outbound dependencies on app.js (all accessed as globals):
//   State:     items, settings, reminders, notes, groceryItems,
//              groceryDepts, groceryLists, activeGroceryListId, bills,
//              billInstances, budgetSettings, budgetCategories,
//              transactions, budgetAccounts, incomeTemplates,
//              incomeEntries, _currentView
//   Persist:   dbPut, saveCurrentProfile
//   Setup:     _setActiveDbForDemo, _setActiveDbForSignedOut
//   Load:      loadData, loadReminders, loadNotifications, loadGrocery
//   UI:        toast, navTo, showView, showKvRegister, DEFAULT_DEPTS,
//              today
//
// Inbound API exposed to app.js (all globals):
//   Seed:      _seedDemoData
//   Banner:    _showDemoBanner
//   Nudges:    _showDemoNudge, _hideAllDemoNudges
//   Convert:   _demoConvertSeed (state), _demoCompleteConversion
//
// window-namespaced globals (set/read externally, NOT moved):
//   window._demoMode    — boolean flag, set by inline script in
//                         index.html and by demo entry/exit paths.
//                         Reads happen in app.js sync paths, init(),
//                         and in this file. Stays in window namespace
//                         because the inline detection script in HTML
//                         sets it before app.js even parses.
//   window._demoPersona — 'couple' or 'family'.
//
// History: extracted on 2026-05-21, after notes.js. Prior to this,
// these 1,432 lines lived at the top of app.js (lines 257–1688)
// across four sections: seed dispatcher, Couple persona, Family
// persona, banner, nudges, and conversion. The split mirrors the
// budget.js (2026-05-20) and notes.js (2026-05-21) extraction
// pattern.

// One-time load-order sanity check.
// Uses lexical `typeof` (not `typeof window.X`) because `items` and
// `settings` are declared as `let` in app.js — they live in module scope,
// not on the global object. The previous `window[name]` lookup was
// always falsy for those two, so this check fired the error on every
// cold page load regardless of whether the load order was actually
// correct. Now the check correctly verifies that the symbols are
// accessible from demo.js's lexical scope, which is what the rest of
// demo.js actually needs (it calls toast(), dbPut(), items.push(), etc.
// directly, not via window).
(function _demoLoadOrderCheck() {
  const checks = {
    toast:    typeof toast,
    dbPut:    typeof dbPut,
    items:    typeof items,
    settings: typeof settings,
  };
  const missing = Object.keys(checks).filter(k => checks[k] === 'undefined');
  if (missing.length > 0) {
    console.error('[demo.js] Missing globals at load time:', missing,
      '— check script order in index.html');
  }
})();

// ── DEMO MODE seed ──────────────────────────────────────────────────────
// Populates every visible tab with realistic example data so the demo lands
// on something useful. Data is written via dbPut into the `stockroom_demo`
// IDB (set up by _setActiveDbForDemo), which is wiped on every demo entry
// so a fresh tab always starts clean.
//
// Dispatcher: takes a persona ('couple' | 'family') and calls the matching
// builder, then runs persistence + materialisation + profile setup once.
// The builder functions populate the global state directly. Persona
// selection lives in window._demoPersona; default is 'couple'.
async function _seedDemoData(personaArg) {
  if (!window._demoMode) return;
  const persona = personaArg || window._demoPersona || 'couple';
  window._demoPersona = persona;

  const today    = new Date();
  const iso      = d => d.toISOString();
  const ymd      = d => d.toISOString().slice(0, 10);
  const daysAgo  = n => { const d = new Date(today); d.setDate(d.getDate() - n); return d; };
  const now      = iso(today);
  const ym       = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}`;
  const prevYm   = (() => {
    const d = new Date(today.getFullYear(), today.getMonth() - 1, 1);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
  })();
  const ctx = { today, iso, ymd, daysAgo, now, ym, prevYm };

  // Dispatch — each builder populates the globals directly. Falls back to
  // couple if an unknown persona slips through.
  if (persona === 'family')      _buildDemoFamily(ctx);
  else                           _buildDemoCouple(ctx);

  // ── Persist everything to the in-memory shim ──────────────────────────
  // Goes through the same dbPut / save functions a real account would use,
  // so the rest of the app sees identical data shapes.
  await dbPut('items',        'items',        items);
  await dbPut('settings',     'settings',     settings);
  await dbPut('reminders',    'reminders',    reminders);
  await dbPut('groceries',    'items',        groceryItems);
  await dbPut('departments',  'departments',  groceryDepts);
  await dbPut('groceryLists', 'groceryLists', groceryLists);
  await dbPut('items',        'notes',        notes);
  await dbPut('bills',            'bills',            bills);
  await dbPut('billInstances',    'billInstances',    billInstances);
  await dbPut('budgetSettings',   'budgetSettings',   budgetSettings);
  await dbPut('budgetCategories', 'budgetCategories', budgetCategories);
  await dbPut('transactions',     'transactions',     transactions);
  await dbPut('budgetAccounts',   'budgetAccounts',   budgetAccounts);
  await dbPut('incomeTemplates',  'incomeTemplates',  incomeTemplates);
  await dbPut('incomeEntries',    'incomeEntries',    incomeEntries);

  // Backfill saving instances for any split-payment bills so the demo
  // lands with visible carry-over progress in the Multi-month bills
  // section and the dashboard's Carry-over tile.
  if (typeof _backfillSavingInstancesForBill === 'function') {
    for (const b of bills) {
      if (b.paymentStrategy === 'split') {
        await _backfillSavingInstancesForBill(b);
      }
    }
  }
  // Materialise the current month so this month's bills show up
  // immediately when the user opens Budget.
  if (typeof materialiseMonth === 'function') {
    await materialiseMonth(ym, { persist: true });
  }

  // Pre-populate the default profile with the seeded arrays so a profile
  // switch doesn't wipe them.
  await dbPut('profiles', 'profiles', {
    default: {
      name: 'Home',
      colour: '#e8a838',
      items:       JSON.parse(JSON.stringify(items)),
      settings:    JSON.parse(JSON.stringify(settings)),
      reminders:   JSON.parse(JSON.stringify(reminders)),
      groceries:   JSON.parse(JSON.stringify(groceryItems)),
      departments: JSON.parse(JSON.stringify(groceryDepts)),
    },
  });
}

// Re-seed the demo with a different persona. Clears all in-memory storage,
// builds the new persona, and re-renders the current view in place. Called
// from the demo banner's persona toggle button.
async function _switchDemoPersona(persona) {
  if (!window._demoMode) return;
  if (persona === window._demoPersona) return;
  // Wipe all demo IDB state so leftovers don't bleed through. The demo
  // DB is deleted and re-opened so the new persona seeds into a fresh
  // store. Local-storage flags written by some tabs (like grocery active
  // list) are also reset to defaults.
  await _setActiveDbForDemo();
  try { localStorage.removeItem('stockroom_active_grocery_list'); } catch(e) {}
  // Re-seed with the new persona
  await _seedDemoData(persona);
  // Re-load all in-memory state from the freshly-seeded storage so the
  // app picks it up. The standard load functions read from dbGet which
  // routes to the demo shim.
  if (typeof loadData       === 'function') await loadData();        // items + settings
  if (typeof loadGrocery    === 'function') await loadGrocery();
  if (typeof loadReminders  === 'function') await loadReminders();
  if (typeof loadNotes      === 'function') await loadNotes();
  if (typeof loadNotifications === 'function') await loadNotifications();
  if (typeof loadBudget     === 'function') await loadBudget();
  if (typeof loadBudgetSpend === 'function') await loadBudgetSpend();
  if (typeof loadBudgetAccountsAndIncome === 'function') await loadBudgetAccountsAndIncome();
  // Re-render the current view in place. _currentView is the active tab;
  // navTo handles the rendering for whichever tab the user is on.
  if (typeof navTo === 'function' && typeof _currentView === 'string') {
    navTo(_currentView);
  } else if (typeof showView === 'function') {
    showView('stock');
  }
  // Reset nudge dismissed state so the user can see them again with
  // updated content for the new persona.
  if (typeof _demoNudgeReset === 'function') _demoNudgeReset();
  // Update the banner's persona-toggle label to reflect the new persona.
  if (typeof _updateDemoPersonaButton === 'function') _updateDemoPersonaButton();
  if (typeof toast === 'function') {
    toast(`Switched to ${persona === 'family' ? 'Family of 5' : 'Couple'} demo`);
  }
}


// ── DEMO MODE: Couple persona ───────────────────────────────────────────
// Sam & Alex — late twenties, busy professionals sharing a London flat.
// Convenience-skewed lifestyle: coffee pods, oat milk, takeaway nights,
// streaming subs, gym memberships. Weekend cooking only, eating out 2-3
// times a week. Two salaries, no kids, splitting most bills.
function _buildDemoCouple(ctx) {
  const { today, iso, ymd, daysAgo, now, ym, prevYm } = ctx;

  // ── Stockroom items ───────────────────────────────────────────────────
  // Tuned so the status colours come out as a healthy mix — a couple of
  // criticals to nudge the user, a handful of low items, plenty of good.
  items = [
    // CRITICAL — out of stock
    {
      id: 'demo_i_oatmilk',
      name: 'Oat milk 1L (Oatly Barista)',
      category: 'Drinks', emoji: '🥛',
      qty: 1, months: 0.13, daysPerUnit: 4,
      logs: [
        { id: 'demo_l_oat_1', date: ymd(daysAgo(4)), qty: 1, price: '2.00', store: 'Tesco', usingFromDate: ymd(daysAgo(4)) },
      ],
      tags: ['Coffee'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_lenses',
      name: 'Daily contact lenses (30-pack)',
      category: 'Personal', emoji: '👁️',
      qty: 1, months: 1, daysPerUnit: 1,
      logs: [
        { id: 'demo_l_len_1', date: ymd(daysAgo(28)), qty: 1, price: '24.00', store: 'Specsavers', usingFromDate: ymd(daysAgo(28)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(90)), updatedAt: now,
    },
    // LOW — running out within ~7-14 days
    {
      id: 'demo_i_coffee_pods',
      name: 'Nespresso pods (50-pack)',
      category: 'Drinks', emoji: '☕',
      qty: 1, months: 0.83, daysPerUnit: 0.5,
      logs: [
        { id: 'demo_l_pod_1', date: ymd(daysAgo(20)), qty: 1, price: '17.50', store: 'Amazon', usingFromDate: ymd(daysAgo(20)) },
        { id: 'demo_l_pod_2', date: ymd(today), qty: 1, price: '17.99', store: 'Amazon', pendingDelivery: true },
      ],
      tags: ['Coffee'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_dishwasher',
      name: 'Dishwasher tablets (40-pack)',
      category: 'Household', emoji: '🍽️',
      qty: 1, months: 1.3, daysPerUnit: 1,
      logs: [
        { id: 'demo_l_dish_1', date: ymd(daysAgo(28)), qty: 1, price: '8.50', store: 'Tesco', usingFromDate: ymd(daysAgo(28)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_vitamin',
      name: 'Vitamin D3 (90-pack)',
      category: 'Personal', emoji: '💊',
      qty: 1, months: 3, daysPerUnit: 1,
      logs: [
        { id: 'demo_l_vit_1', date: ymd(daysAgo(78)), qty: 1, price: '6.99', store: 'Boots', usingFromDate: ymd(daysAgo(78)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    // GOOD — well-stocked
    {
      id: 'demo_i_pasta_d',
      name: 'Penne pasta 500g',
      category: 'Cupboard', emoji: '🍝',
      qty: 4, months: 8, daysPerUnit: 60,
      logs: [
        { id: 'demo_l_pen_1', date: ymd(daysAgo(15)), qty: 4, price: '4.50', store: 'Tesco', usingFromDate: ymd(daysAgo(15)) },
      ],
      tags: ['Cupboard'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_olive',
      name: 'Olive oil 750ml',
      category: 'Cupboard', emoji: '🫒',
      qty: 1, months: 4, daysPerUnit: 120,
      logs: [
        { id: 'demo_l_oil_1', date: ymd(daysAgo(40)), qty: 1, price: '7.50', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(40)) },
      ],
      tags: ['Cupboard'],
      addedAt: iso(daysAgo(220)), updatedAt: now,
    },
    {
      id: 'demo_i_cereal_d',
      name: 'Granola (Jordans Country Crisp)',
      category: 'Cupboard', emoji: '🥣',
      qty: 2, months: 1.2, daysPerUnit: 18,
      logs: [
        { id: 'demo_l_cer_1', date: ymd(daysAgo(8)), qty: 2, price: '6.50', store: 'Tesco', usingFromDate: ymd(daysAgo(8)) },
      ],
      tags: ['Breakfast'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_paper',
      name: 'Toilet paper 9-pack',
      category: 'Household', emoji: '🧻',
      qty: 6, months: 2, daysPerUnit: 10,
      logs: [
        { id: 'demo_l_tp_1', date: ymd(daysAgo(15)), qty: 9, price: '6.50', store: 'Tesco', usingFromDate: ymd(daysAgo(15)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_handwash',
      name: 'Hand wash refill 1L',
      category: 'Household', emoji: '🧼',
      qty: 1, months: 4, daysPerUnit: 120,
      logs: [
        { id: 'demo_l_hw_1', date: ymd(daysAgo(35)), qty: 1, price: '5.00', store: 'Boots', usingFromDate: ymd(daysAgo(35)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_freezer',
      name: 'M&S frozen curry meals',
      category: 'Frozen', emoji: '🍛',
      qty: 4, months: 1, daysPerUnit: 7,
      logs: [
        { id: 'demo_l_fr_1', date: ymd(daysAgo(7)), qty: 6, price: '24.00', store: 'M&S', usingFromDate: ymd(daysAgo(7)) },
      ],
      tags: ['Frozen', 'Meals'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_snack',
      name: 'Granola bars (12-pack)',
      category: 'Cupboard', emoji: '🥜',
      qty: 8, months: 0.8, daysPerUnit: 3,
      logs: [
        { id: 'demo_l_sn_1', date: ymd(daysAgo(12)), qty: 12, price: '4.50', store: 'Tesco', usingFromDate: ymd(daysAgo(12)) },
      ],
      tags: ['Snacks'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_blade',
      name: 'Razor blades (8-pack)',
      category: 'Personal', emoji: '🪒',
      qty: 5, months: 5, daysPerUnit: 18,
      logs: [
        { id: 'demo_l_bl_1', date: ymd(daysAgo(60)), qty: 8, price: '18.00', store: 'Boots', usingFromDate: ymd(daysAgo(60)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_bodywash',
      name: 'Body wash 500ml',
      category: 'Personal', emoji: '🧴',
      qty: 2, months: 3, daysPerUnit: 45,
      logs: [
        { id: 'demo_l_bw_1', date: ymd(daysAgo(20)), qty: 2, price: '8.00', store: 'Boots', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
  ];

  // ── Settings ───────────────────────────────────────────────────────────
  settings = {
    ...settings,
    threshold: 20,
    country: 'GB',
    customTags: ['Personal', 'Household', 'Coffee', 'Snacks', 'Breakfast', 'Frozen', 'Meals'],
    _setupProtectSeen: true,
    _setupCountrySet: true,
    lastSynced: iso(daysAgo(1)),
  };

  // ── Grocery setup ─────────────────────────────────────────────────────
  groceryDepts = DEFAULT_DEPTS.map(d => ({...d}));
  groceryLists = [
    { id: 'demo_gl_tesco', name: 'Tesco run', store: 'Tesco', mode: 'stockcheck', shoppingPhase: 'shop', createdAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gl_topup', name: 'Quick top-up', store: 'Co-op', mode: 'simple', shoppingPhase: 'shop', createdAt: iso(daysAgo(1)), updatedAt: now },
  ];
  activeGroceryListId = 'demo_gl_tesco';
  try { localStorage.setItem('stockroom_active_grocery_list', activeGroceryListId); } catch(e) {}

  groceryItems = [
    // Tesco run — mid-shop, mix of ticked and remaining
    { id: 'demo_g_oatmilk', name: 'Oat milk Oatly Barista', department: 'dairy', listId: 'demo_gl_tesco', notes: '1L', recurring: true, intervalDays: 4, qty: 2, needed: true, checked: true, checkedAt: iso(daysAgo(0)), addedAt: iso(daysAgo(2)), updatedAt: now, lastBoughtAt: iso(daysAgo(4)) },
    { id: 'demo_g_eggs',    name: 'Free range eggs',       department: 'dairy', listId: 'demo_gl_tesco', notes: '6-pack', recurring: false, intervalDays: 7, needed: true, checked: true, checkedAt: iso(daysAgo(0)), addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_g_avocado', name: 'Avocados',              department: 'fruit-veg', listId: 'demo_gl_tesco', notes: 'Ripe', recurring: false, intervalDays: 7, qty: 2, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_g_chicken', name: 'Chicken breasts',       department: 'meat-fish', listId: 'demo_gl_tesco', notes: 'For Sun roast', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_g_yogurt',  name: 'Greek yoghurt 500g',    department: 'dairy', listId: 'demo_gl_tesco', notes: '', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_g_lentils', name: 'Pre-cooked lentils',    department: 'cupboard', listId: 'demo_gl_tesco', notes: '', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(1)), updatedAt: now },
    { id: 'demo_g_beer',    name: 'Beavertown Neck Oil',   department: 'drinks', listId: 'demo_gl_tesco', notes: '4-pack', recurring: false, intervalDays: 7, qty: 1, needed: true, checked: false, addedAt: iso(daysAgo(1)), updatedAt: now },

    // Quick top-up at Co-op
    { id: 'demo_g_milk_t',  name: 'Semi-skimmed milk',     department: 'dairy', listId: 'demo_gl_topup', notes: '2 pints', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(0)), updatedAt: now },
    { id: 'demo_g_bread_t', name: 'Sourdough loaf',        department: 'bakery', listId: 'demo_gl_topup', notes: '', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(0)), updatedAt: now },
    { id: 'demo_g_cookies', name: 'Dark chocolate digestives', department: 'cupboard', listId: 'demo_gl_topup', notes: '', recurring: false, intervalDays: 14, needed: true, checked: false, addedAt: iso(daysAgo(0)), updatedAt: now },
  ];

  // ── Reminders ─────────────────────────────────────────────────────────
  reminders = [
    {
      id: 'demo_r_lenses', name: 'Order more contact lenses',
      interval: 1, unit: 'months', lastReplaced: ymd(daysAgo(28)),
      notes: 'Specsavers — 30-pack auto-renew', linkedItemId: 'demo_i_lenses',
      createdAt: iso(daysAgo(180)),
    },
    {
      id: 'demo_r_smoke', name: 'Smoke detector batteries',
      interval: 6, unit: 'months', lastReplaced: ymd(daysAgo(160)),
      notes: '9V — by the front door and bedroom', linkedItemId: null,
      createdAt: iso(daysAgo(500)),
    },
    {
      id: 'demo_r_dentist', name: 'Dentist check-up',
      interval: 6, unit: 'months', lastReplaced: ymd(daysAgo(190)), // overdue
      notes: 'Both of us — book together', linkedItemId: null,
      createdAt: iso(daysAgo(500)),
    },
  ];

  // ── Notes ─────────────────────────────────────────────────────────────
  notes = [
    {
      id: 'demo_n_weekend', title: 'This weekend',
      body: '• Sat: brunch with Jess @ 11\n• Sat eve: try that new Vietnamese place\n• Sun: cinema (book Tues)\n• Sun roast — get chicken in the Tesco run\n• Laundry day — bring back to flat',
      locked: false, pinned: true, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(1)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_n_holiday', title: 'Lisbon trip — September',
      body: 'Booking flights this month. Aim for £200pp return.\n\n• AirBnB — Alfama district\n• Day trip to Sintra\n• Tasca Zé dos Cornos for dinner\n• Need: travel insurance, EHIC swap to GHIC, phone roaming',
      locked: false, pinned: true, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(8)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_n_birthday', title: 'Sam\'s birthday gift ideas',
      body: '— That denim jacket she pinned on Pinterest\n— Voucher for the pottery class\n— New AirPods (probably overkill?)\n— Weekend trip to the coast (joint with Mum)',
      locked: false, pinned: false, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(10)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_n_flat', title: 'Flat to-do',
      body: 'Bin store key — landlord copy\nCarbon monoxide alarm — replace by July\nOven needs deep clean before next inspection\nKitchen tap drips — book plumber',
      locked: false, pinned: false, archived: false, colour: null,
      tickBoxesVisible: true,
      tickBoxes: {},
      createdAt: iso(daysAgo(15)), updatedAt: now, deletedAt: null,
    },
  ];

  // ── Budget — accounts, categories, bills, transactions, income ────────
  budgetAccounts = [
    {
      id: 'demo_acc_main', name: 'Joint current', type: 'current',
      isPrimary: true, balance: 2150.40, balanceAsOf: ymd(daysAgo(1)),
      color: '#5b8dee', notes: '', archived: false,
      createdAt: iso(daysAgo(365)), updatedAt: now,
    },
    {
      id: 'demo_acc_savings', name: 'Joint savings', type: 'savings',
      isPrimary: false, balance: 4250.00, balanceAsOf: ymd(daysAgo(1)),
      color: '#4cbb8a', notes: 'Holiday + emergency fund', archived: false,
      createdAt: iso(daysAgo(365)), updatedAt: now,
    },
  ];

  budgetCategories = [
    { id: 'demo_cat_grocery', name: 'Groceries',    monthlyBudget: 280, weeklyBudget: null, budgetCycle: 'monthly', color: '#e8a838', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_eatout',  name: 'Eating out',   monthlyBudget: 250, weeklyBudget: null, budgetCycle: 'monthly', color: '#e85d8e', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_petrol',  name: 'Transport',    monthlyBudget: 180, weeklyBudget: null, budgetCycle: 'monthly', color: '#5b8dee', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_coffee',  name: 'Coffee & treats', monthlyBudget: null, weeklyBudget: 35, budgetCycle: 'weekly', color: '#a280e8', archived: false, createdAt: iso(daysAgo(120)), updatedAt: now },
    { id: 'demo_cat_fun',     name: 'Going out',    monthlyBudget: 200, weeklyBudget: null, budgetCycle: 'monthly', color: '#e8585d', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
  ];

  // Bill anchors so the demo lands on a useful state regardless of when
  // it's loaded.
  const currentMonthIdx = today.getMonth();
  const tvAnchor    = (currentMonthIdx + 1) % 12;       // pays next month
  const primeAnchor = (currentMonthIdx + 5) % 12;       // 6 months saved
  const insuranceAnchor = (currentMonthIdx + 8) % 12;   // 3 months saved

  bills = [
    { id: 'demo_bill_rent',      name: 'Rent',           amount: 1400.00, variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1,  categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(365)), updatedAt: now },
    { id: 'demo_bill_council',   name: 'Council tax',    amount: 138.00,  variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 5,  categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_broadband', name: 'Broadband',      amount: 28.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 15, categoryId: null, notes: 'BT Fibre', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_gas',       name: 'Gas & electric', amount: 85.00,   variableAmount: true,  frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 22, categoryId: null, notes: 'Octopus — varies', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_mobile_s',  name: 'Mobile (Sam)',   amount: 18.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 8,  categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_mobile_a',  name: 'Mobile (Alex)',  amount: 22.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 8,  categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_gym',       name: 'Gym (joint)',    amount: 49.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 12, categoryId: null, notes: 'PureGym — couple plan', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_netflix',   name: 'Netflix',        amount: 17.99,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 4,  categoryId: null, notes: 'Standard 1080p', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_spotify',   name: 'Spotify Duo',    amount: 16.99,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 19, categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    // Split bills — quarterly water + annual contents insurance + annual prime.
    { id: 'demo_bill_tv',        name: 'TV License',     amount: 44.88,   variableAmount: false, frequency: { unit: 'month', interval: 3, anchorMonth: tvAnchor }, dayOfMonth: 9, categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 3 }, createdAt: iso(daysAgo(120)), updatedAt: now },
    { id: 'demo_bill_prime',     name: 'Amazon Prime',   amount: 95.04,   variableAmount: false, frequency: { unit: 'year',  interval: 1, anchorMonth: primeAnchor }, dayOfMonth: 1, categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 12 }, createdAt: iso(daysAgo(200)), updatedAt: now },
    { id: 'demo_bill_contents',  name: 'Contents insurance', amount: 168.00, variableAmount: false, frequency: { unit: 'year', interval: 1, anchorMonth: insuranceAnchor }, dayOfMonth: 1, categoryId: null, notes: 'Direct Line', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 12 }, createdAt: iso(daysAgo(180)), updatedAt: now },
    // Archived
    { id: 'demo_bill_old', name: 'ClassPass (cancelled)', amount: 89.00, variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 5, categoryId: null, notes: 'Cancelled — too expensive', accountId: 'demo_acc_main', archived: true, paymentStrategy: 'lump', splitInto: null, createdAt: iso(daysAgo(450)), updatedAt: iso(daysAgo(60)) },
  ];
  billInstances = {};

  // Realistic transactions across the last ~30 days
  const txList = [
    { id: 'demo_tx_1',  date: ymd(daysAgo(2)),  amount: 38.40, categoryId: 'demo_cat_grocery', description: 'Tesco', accountId: 'demo_acc_main' },
    { id: 'demo_tx_2',  date: ymd(daysAgo(3)),  amount: 4.20,  categoryId: 'demo_cat_coffee',  description: 'Pret latte + croissant', accountId: 'demo_acc_main' },
    { id: 'demo_tx_3',  date: ymd(daysAgo(4)),  amount: 28.50, categoryId: 'demo_cat_eatout',  description: 'Honest Burgers', accountId: 'demo_acc_main' },
    { id: 'demo_tx_4',  date: ymd(daysAgo(5)),  amount: 12.80, categoryId: 'demo_cat_petrol',  description: 'TfL — Oyster top-up', accountId: 'demo_acc_main' },
    { id: 'demo_tx_5',  date: ymd(daysAgo(6)),  amount: 3.85,  categoryId: 'demo_cat_coffee',  description: 'Costa flat white', accountId: 'demo_acc_main' },
    { id: 'demo_tx_6',  date: ymd(daysAgo(8)),  amount: 47.30, categoryId: 'demo_cat_eatout',  description: 'Wagamama (Friday)', accountId: 'demo_acc_main' },
    { id: 'demo_tx_7',  date: ymd(daysAgo(9)),  amount: 32.00, categoryId: 'demo_cat_fun',     description: 'Cinema + drinks', accountId: 'demo_acc_main' },
    { id: 'demo_tx_8',  date: ymd(daysAgo(11)), amount: 52.10, categoryId: 'demo_cat_grocery', description: 'Sainsbury\'s', accountId: 'demo_acc_main' },
    { id: 'demo_tx_9',  date: ymd(daysAgo(12)), amount: 18.50, categoryId: 'demo_cat_petrol',  description: 'Uber home', accountId: 'demo_acc_main' },
    { id: 'demo_tx_10', date: ymd(daysAgo(14)), amount: 67.20, categoryId: 'demo_cat_eatout',  description: 'Date night — Padella', accountId: 'demo_acc_main' },
    { id: 'demo_tx_11', date: ymd(daysAgo(16)), amount: 24.50, categoryId: 'demo_cat_grocery', description: 'M&S frozen meals', accountId: 'demo_acc_main' },
    { id: 'demo_tx_12', date: ymd(daysAgo(18)), amount: 22.00, categoryId: 'demo_cat_fun',     description: 'Pub quiz round', accountId: 'demo_acc_main' },
    { id: 'demo_tx_13', date: ymd(daysAgo(22)), amount: 41.85, categoryId: 'demo_cat_grocery', description: 'Tesco', accountId: 'demo_acc_main' },
    { id: 'demo_tx_14', date: ymd(daysAgo(25)), amount: 15.20, categoryId: 'demo_cat_petrol',  description: 'TfL', accountId: 'demo_acc_main' },
  ];
  transactions = {};
  for (const tx of txList) {
    const txYm = tx.date.slice(0, 7);
    if (!transactions[txYm]) transactions[txYm] = {};
    transactions[txYm][tx.id] = { ...tx, createdAt: iso(daysAgo(0)), updatedAt: now };
  }

  incomeTemplates = [
    {
      id: 'demo_inc_sam', name: 'Salary (Sam)', amount: 2350, variableAmount: false,
      frequency: { unit: 'month', interval: 1, anchorMonth: null },
      dayOfMonth: 28, accountId: 'demo_acc_main', notes: 'Marketing exec',
      archived: false, createdAt: iso(daysAgo(365)), updatedAt: now,
    },
    {
      id: 'demo_inc_alex', name: 'Salary (Alex)', amount: 2180, variableAmount: false,
      frequency: { unit: 'month', interval: 1, anchorMonth: null },
      dayOfMonth: 25, accountId: 'demo_acc_main', notes: 'UX designer',
      archived: false, createdAt: iso(daysAgo(365)), updatedAt: now,
    },
  ];
  // Pre-seed paid actuals in the previous month
  const _isoFromYmd = (s) => new Date(s + 'T09:00:00Z').toISOString();
  const samDate  = `${prevYm}-28`;
  const alexDate = `${prevYm}-25`;
  incomeEntries = {};
  incomeEntries[prevYm] = {
    [`incTpl_demo_inc_sam__${samDate}`]: {
      id: `incTpl_demo_inc_sam__${samDate}`, date: samDate,
      amount: 2350, actualAmount: 2350, source: 'template_instance',
      templateId: 'demo_inc_sam', accountId: 'demo_acc_main', notes: '',
      paidAt: _isoFromYmd(samDate), createdAt: _isoFromYmd(samDate),
      createdBy: null, updatedAt: _isoFromYmd(samDate),
    },
    [`incTpl_demo_inc_alex__${alexDate}`]: {
      id: `incTpl_demo_inc_alex__${alexDate}`, date: alexDate,
      amount: 2180, actualAmount: 2180, source: 'template_instance',
      templateId: 'demo_inc_alex', accountId: 'demo_acc_main', notes: '',
      paidAt: _isoFromYmd(alexDate), createdAt: _isoFromYmd(alexDate),
      createdBy: null, updatedAt: _isoFromYmd(alexDate),
    },
  };

  budgetSettings = {
    ...((typeof budgetSettings === 'object' && budgetSettings) || {}),
    payDayOfMonth: 25,
    materialisedMonths: [],
  };
}


// ── DEMO MODE: Family persona ───────────────────────────────────────────
// The Hendersons — two parents (Liam & Beth) plus three kids (Tom 14,
// Mia 11, Noah 8). Suburban semi, batch-cooking, school clubs, big shops.
// Mortgage, larger pack sizes, school-trip savings, family insurance.
// One full salary, one part-time, child benefit.
function _buildDemoFamily(ctx) {
  const { today, iso, ymd, daysAgo, now, ym, prevYm } = ctx;

  // ── Stockroom items — bigger household, more variety ──────────────────
  items = [
    // CRITICAL — out
    {
      id: 'demo_i_milk_f',
      name: 'Semi-skimmed milk 6-pint',
      category: 'Dairy', emoji: '🥛',
      qty: 1, months: 0.18, daysPerUnit: 5.5,
      logs: [
        { id: 'demo_l_milk_f1', date: ymd(daysAgo(6)), qty: 2, price: '4.40', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(6)) },
      ],
      tags: ['Dairy', 'Daily'],
      addedAt: iso(daysAgo(300)), updatedAt: now,
    },
    {
      id: 'demo_i_kidsnacks',
      name: 'Lunchbox snack bars (24-pack)',
      category: 'Cupboard', emoji: '🥨',
      qty: 1, months: 0.5, daysPerUnit: 0.6,
      logs: [
        { id: 'demo_l_ks_1', date: ymd(daysAgo(15)), qty: 2, price: '7.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(15)) },
      ],
      tags: ['Kids', 'School'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_school_pe',
      name: 'White PE socks',
      category: 'Personal', emoji: '🧦',
      qty: 1, months: 0.2, daysPerUnit: 6,
      logs: [
        { id: 'demo_l_pe_1', date: ymd(daysAgo(50)), qty: 5, price: '8.00', store: 'M&S', usingFromDate: ymd(daysAgo(50)) },
      ],
      tags: ['Kids', 'School'],
      addedAt: iso(daysAgo(220)), updatedAt: now,
    },
    // LOW
    {
      id: 'demo_i_bread_f',
      name: 'Wholemeal sliced bread',
      category: 'Bakery', emoji: '🍞',
      qty: 1, months: 0.1, daysPerUnit: 3,
      logs: [
        { id: 'demo_l_br_1', date: ymd(daysAgo(2)), qty: 2, price: '2.40', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(2)) },
      ],
      tags: ['Bakery', 'Daily'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_kitchen_roll',
      name: 'Kitchen roll (6-pack)',
      category: 'Household', emoji: '🧻',
      qty: 1, months: 0.7, daysPerUnit: 4,
      logs: [
        { id: 'demo_l_kr_1', date: ymd(daysAgo(20)), qty: 1, price: '5.50', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_dishwasher_f',
      name: 'Dishwasher tablets (60-pack)',
      category: 'Household', emoji: '🍽️',
      qty: 1, months: 1, daysPerUnit: 0.5,
      logs: [
        { id: 'demo_l_dish_f1', date: ymd(daysAgo(28)), qty: 1, price: '12.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(28)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_bin_f',
      name: 'Bin bags (50-pack)',
      category: 'Household', emoji: '🗑️',
      qty: 1, months: 1.2, daysPerUnit: 0.7,
      logs: [
        { id: 'demo_l_bin_1', date: ymd(daysAgo(28)), qty: 1, price: '4.50', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(28)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    // GOOD
    {
      id: 'demo_i_pasta_f',
      name: 'Pasta variety (5-pack)',
      category: 'Cupboard', emoji: '🍝',
      qty: 4, months: 4, daysPerUnit: 30,
      logs: [
        { id: 'demo_l_pa_f1', date: ymd(daysAgo(20)), qty: 5, price: '6.50', store: 'Tesco', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Cupboard'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_rice_f',
      name: 'Basmati rice 5kg',
      category: 'Cupboard', emoji: '🍚',
      qty: 1, months: 6, daysPerUnit: 180,
      logs: [
        { id: 'demo_l_ri_f1', date: ymd(daysAgo(40)), qty: 1, price: '14.00', store: 'Costco', usingFromDate: ymd(daysAgo(40)) },
      ],
      tags: ['Cupboard', 'Bulk'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_cereal_f',
      name: 'Weetabix (72-pack)',
      category: 'Cupboard', emoji: '🥣',
      qty: 1, months: 1.5, daysPerUnit: 1,
      logs: [
        { id: 'demo_l_we_1', date: ymd(daysAgo(20)), qty: 1, price: '8.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Breakfast', 'Kids'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_juice_f',
      name: 'Apple juice 1L (multi)',
      category: 'Drinks', emoji: '🧃',
      qty: 4, months: 2, daysPerUnit: 7,
      logs: [
        { id: 'demo_l_aj_1', date: ymd(daysAgo(10)), qty: 6, price: '10.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(10)) },
      ],
      tags: ['Drinks', 'Kids'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_paper_f',
      name: 'Toilet paper 16-pack',
      category: 'Household', emoji: '🧻',
      qty: 12, months: 2, daysPerUnit: 4,
      logs: [
        { id: 'demo_l_tp_f1', date: ymd(daysAgo(15)), qty: 16, price: '12.00', store: 'Costco', usingFromDate: ymd(daysAgo(15)) },
      ],
      tags: ['Household', 'Bulk'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_eggs_f',
      name: 'Free range eggs (15-pack)',
      category: 'Dairy', emoji: '🥚',
      qty: 10, months: 0.5, daysPerUnit: 1.5,
      logs: [
        { id: 'demo_l_eg_f1', date: ymd(daysAgo(5)), qty: 15, price: '4.20', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(5)) },
      ],
      tags: ['Dairy'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_butter_f',
      name: 'Butter 250g',
      category: 'Dairy', emoji: '🧈',
      qty: 3, months: 1.5, daysPerUnit: 14,
      logs: [
        { id: 'demo_l_bu_f1', date: ymd(daysAgo(15)), qty: 4, price: '8.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(15)) },
      ],
      tags: ['Dairy'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_cheese_f',
      name: 'Mature cheddar 500g',
      category: 'Dairy', emoji: '🧀',
      qty: 2, months: 1, daysPerUnit: 14,
      logs: [
        { id: 'demo_l_ch_f1', date: ymd(daysAgo(15)), qty: 2, price: '7.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(15)) },
      ],
      tags: ['Dairy'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_chicken_f',
      name: 'Frozen chicken breasts (1kg)',
      category: 'Frozen', emoji: '🍗',
      qty: 3, months: 2, daysPerUnit: 14,
      logs: [
        { id: 'demo_l_chf_1', date: ymd(daysAgo(20)), qty: 3, price: '18.00', store: 'Costco', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Frozen'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_baked_beans',
      name: 'Baked beans (12-pack)',
      category: 'Cupboard', emoji: '🥫',
      qty: 8, months: 2, daysPerUnit: 5,
      logs: [
        { id: 'demo_l_bb_1', date: ymd(daysAgo(20)), qty: 12, price: '8.00', store: 'Costco', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Cupboard', 'Bulk'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_choc',
      name: 'Mini chocolate bars (multi-pack)',
      category: 'Cupboard', emoji: '🍫',
      qty: 1, months: 0.7, daysPerUnit: 2,
      logs: [
        { id: 'demo_l_cb_1', date: ymd(daysAgo(8)), qty: 1, price: '5.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(8)) },
      ],
      tags: ['Snacks', 'Kids'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_yog_f',
      name: 'Frube yoghurts (24-pack)',
      category: 'Dairy', emoji: '🍶',
      qty: 14, months: 0.7, daysPerUnit: 2,
      logs: [
        { id: 'demo_l_yo_f1', date: ymd(daysAgo(7)), qty: 24, price: '6.50', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(7)) },
      ],
      tags: ['Dairy', 'Kids'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_laundry_f',
      name: 'Laundry detergent 80 wash',
      category: 'Household', emoji: '🧺',
      qty: 1, months: 2, daysPerUnit: 0.75,
      logs: [
        { id: 'demo_l_la_f1', date: ymd(daysAgo(20)), qty: 1, price: '14.00', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_softener',
      name: 'Fabric softener 2L',
      category: 'Household', emoji: '🧴',
      qty: 1, months: 3, daysPerUnit: 90,
      logs: [
        { id: 'demo_l_so_1', date: ymd(daysAgo(30)), qty: 1, price: '6.50', store: 'Sainsbury\'s', usingFromDate: ymd(daysAgo(30)) },
      ],
      tags: ['Household'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_shampoo_f',
      name: 'Shampoo (family value)',
      category: 'Personal', emoji: '🧴',
      qty: 2, months: 2, daysPerUnit: 30,
      logs: [
        { id: 'demo_l_sh_f1', date: ymd(daysAgo(20)), qty: 3, price: '15.00', store: 'Boots', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_toothpaste_f',
      name: 'Toothpaste (family pack of 4)',
      category: 'Personal', emoji: '🦷',
      qty: 3, months: 4, daysPerUnit: 40,
      logs: [
        { id: 'demo_l_tp_f1', date: ymd(daysAgo(40)), qty: 4, price: '12.00', store: 'Boots', usingFromDate: ymd(daysAgo(40)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_brushheads_f',
      name: 'Toothbrush heads (8-pack)',
      category: 'Personal', emoji: '🪥',
      qty: 5, months: 8, daysPerUnit: 30,
      logs: [
        { id: 'demo_l_bh_f1', date: ymd(daysAgo(50)), qty: 8, price: '18.00', store: 'Amazon', usingFromDate: ymd(daysAgo(50)) },
      ],
      tags: ['Personal'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
    {
      id: 'demo_i_petfood',
      name: 'Dog food 12kg',
      category: 'Pets', emoji: '🐕',
      qty: 1, months: 2, daysPerUnit: 60,
      logs: [
        { id: 'demo_l_pf_1', date: ymd(daysAgo(30)), qty: 1, price: '32.00', store: 'Pets at Home', usingFromDate: ymd(daysAgo(30)) },
      ],
      tags: ['Pets'],
      addedAt: iso(daysAgo(220)), updatedAt: now,
    },
    {
      id: 'demo_i_petlitter',
      name: 'Cat litter 10L',
      category: 'Pets', emoji: '🐈',
      qty: 1, months: 1.5, daysPerUnit: 45,
      logs: [
        { id: 'demo_l_pl_1', date: ymd(daysAgo(20)), qty: 1, price: '8.00', store: 'Pets at Home', usingFromDate: ymd(daysAgo(20)) },
      ],
      tags: ['Pets'],
      addedAt: iso(daysAgo(220)), updatedAt: now,
    },
    {
      id: 'demo_i_painkill',
      name: 'Calpol (kids paracetamol)',
      category: 'Personal', emoji: '💊',
      qty: 2, months: 6, daysPerUnit: 60,
      logs: [
        { id: 'demo_l_cp_1', date: ymd(daysAgo(30)), qty: 2, price: '8.00', store: 'Boots', usingFromDate: ymd(daysAgo(30)) },
      ],
      tags: ['Personal', 'Kids'],
      addedAt: iso(daysAgo(220)), updatedAt: now,
    },
    {
      id: 'demo_i_pe_kit',
      name: 'PE shorts (boy & girl mix)',
      category: 'Personal', emoji: '🩳',
      qty: 6, months: 24, daysPerUnit: 120,
      logs: [
        { id: 'demo_l_pek_1', date: ymd(daysAgo(120)), qty: 6, price: '18.00', store: 'M&S', usingFromDate: ymd(daysAgo(120)) },
      ],
      tags: ['Kids', 'School'],
      addedAt: iso(daysAgo(180)), updatedAt: now,
    },
  ];

  // ── Settings ───────────────────────────────────────────────────────────
  settings = {
    ...settings,
    threshold: 25,
    country: 'GB',
    customTags: ['Personal', 'Household', 'Kids', 'School', 'Pets', 'Snacks', 'Breakfast', 'Frozen', 'Daily', 'Bulk', 'Drinks', 'Dairy', 'Bakery'],
    _setupProtectSeen: true,
    _setupCountrySet: true,
    lastSynced: iso(daysAgo(1)),
  };

  // ── Grocery setup ─────────────────────────────────────────────────────
  groceryDepts = DEFAULT_DEPTS.map(d => ({...d}));
  groceryLists = [
    { id: 'demo_gl_bigshop',  name: 'Big shop',     store: 'Sainsbury\'s', mode: 'stockcheck', shoppingPhase: 'check', createdAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gl_topup_f',  name: 'Tesco top-up', store: 'Tesco',         mode: 'simple',     shoppingPhase: 'shop',  createdAt: iso(daysAgo(1)), updatedAt: now },
    { id: 'demo_gl_costco',   name: 'Costco run',   store: 'Costco',        mode: 'simple',     shoppingPhase: 'check', createdAt: iso(daysAgo(4)), updatedAt: now },
  ];
  activeGroceryListId = 'demo_gl_bigshop';
  try { localStorage.setItem('stockroom_active_grocery_list', activeGroceryListId); } catch(e) {}

  groceryItems = [
    // Big shop — stockcheck mode, lots of items, mix of needed/not
    { id: 'demo_gf_milk',     name: 'Semi-skimmed milk 6-pint',      department: 'dairy',     listId: 'demo_gl_bigshop', notes: '2', recurring: true, intervalDays: 5, qty: 2, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_bread',    name: 'Wholemeal bread',               department: 'bakery',    listId: 'demo_gl_bigshop', notes: 'Hovis Best of Both', recurring: true, intervalDays: 3, qty: 2, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_yog',      name: 'Frube yoghurts',                department: 'dairy',     listId: 'demo_gl_bigshop', notes: '24-pack', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_chicken',  name: 'Chicken thighs',                department: 'meat-fish', listId: 'demo_gl_bigshop', notes: 'Sat dinner', recurring: false, intervalDays: 7, qty: 2, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_apples',   name: 'Pink Lady apples',              department: 'fruit-veg', listId: 'demo_gl_bigshop', notes: '8-pack', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_bananas',  name: 'Bananas',                       department: 'fruit-veg', listId: 'demo_gl_bigshop', notes: 'Lunchboxes', recurring: true, intervalDays: 3, qty: 8, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_lunchbox', name: 'Snack bars (lunchbox)',         department: 'cupboard',  listId: 'demo_gl_bigshop', notes: '24-pack', recurring: false, intervalDays: 14, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_pasta_g',  name: 'Penne 1kg',                     department: 'cupboard',  listId: 'demo_gl_bigshop', notes: '', recurring: false, intervalDays: 14, qty: 2, needed: false, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_sauce',    name: 'Pasta sauce',                   department: 'cupboard',  listId: 'demo_gl_bigshop', notes: 'Lloyd Grossman x2', recurring: false, intervalDays: 14, needed: false, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_juice',    name: 'Apple juice cartons',           department: 'drinks',    listId: 'demo_gl_bigshop', notes: '6-pack lunchbox', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_kidsweets',name: 'Mini choc bars',                department: 'cupboard',  listId: 'demo_gl_bigshop', notes: '20-pack', recurring: false, intervalDays: 14, needed: false, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_cereal',   name: 'Weetabix 72',                   department: 'cupboard',  listId: 'demo_gl_bigshop', notes: '', recurring: false, intervalDays: 21, needed: false, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_butter',   name: 'Butter 4-pack',                 department: 'dairy',     listId: 'demo_gl_bigshop', notes: '', recurring: false, intervalDays: 14, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },
    { id: 'demo_gf_eggs',     name: 'Eggs (15-box)',                 department: 'dairy',     listId: 'demo_gl_bigshop', notes: '', recurring: false, intervalDays: 7, needed: true, checked: false, addedAt: iso(daysAgo(2)), updatedAt: now },

    // Tesco top-up — quick run, simple mode
    { id: 'demo_gf_pin_milk', name: 'Milk 2 pints',                  department: 'dairy',     listId: 'demo_gl_topup_f', notes: '', recurring: false, intervalDays: 3, needed: true, checked: false, addedAt: iso(daysAgo(0)), updatedAt: now },
    { id: 'demo_gf_pin_bread',name: 'Sliced bread',                  department: 'bakery',    listId: 'demo_gl_topup_f', notes: '', recurring: false, intervalDays: 3, needed: true, checked: false, addedAt: iso(daysAgo(0)), updatedAt: now },
    { id: 'demo_gf_pin_baby', name: 'Babywipes',                     department: 'household', listId: 'demo_gl_topup_f', notes: 'For Noah school bag', recurring: false, intervalDays: 14, needed: true, checked: false, addedAt: iso(daysAgo(0)), updatedAt: now },

    // Costco — bulk run
    { id: 'demo_gf_co_paper', name: 'Toilet paper 16-pack',          department: 'household', listId: 'demo_gl_costco', notes: '', recurring: false, intervalDays: 60, needed: true, checked: false, addedAt: iso(daysAgo(4)), updatedAt: now },
    { id: 'demo_gf_co_dish',  name: 'Dishwasher tabs (60)',          department: 'household', listId: 'demo_gl_costco', notes: '', recurring: false, intervalDays: 30, needed: true, checked: false, addedAt: iso(daysAgo(4)), updatedAt: now },
    { id: 'demo_gf_co_chick', name: 'Frozen chicken breasts',        department: 'meat-fish', listId: 'demo_gl_costco', notes: '1kg', recurring: false, intervalDays: 30, qty: 2, needed: true, checked: false, addedAt: iso(daysAgo(4)), updatedAt: now },
    { id: 'demo_gf_co_rice',  name: 'Basmati rice 5kg',              department: 'cupboard',  listId: 'demo_gl_costco', notes: '', recurring: false, intervalDays: 90, needed: false, checked: false, addedAt: iso(daysAgo(4)), updatedAt: now },
  ];

  // ── Reminders — busy household has lots of moving parts ───────────────
  reminders = [
    { id: 'demo_rf_brushheads', name: 'Replace toothbrush heads', interval: 3, unit: 'months', lastReplaced: ymd(daysAgo(85)), notes: '5 in the family — buy in bulk', linkedItemId: 'demo_i_brushheads_f', createdAt: iso(daysAgo(300)) },
    { id: 'demo_rf_smoke',      name: 'Smoke alarm batteries',    interval: 6, unit: 'months', lastReplaced: ymd(daysAgo(140)), notes: 'Hallway, both landings, kids\' rooms', linkedItemId: null, createdAt: iso(daysAgo(500)) },
    { id: 'demo_rf_boiler',     name: 'Annual boiler service',    interval: 12, unit: 'months', lastReplaced: ymd(daysAgo(380)), notes: 'British Gas — book online (overdue)', linkedItemId: null, createdAt: iso(daysAgo(800)) },
    { id: 'demo_rf_mot',        name: 'Car MOT',                  interval: 12, unit: 'months', lastReplaced: ymd(daysAgo(330)), notes: 'KwikFit — book early', linkedItemId: null, createdAt: iso(daysAgo(800)) },
    { id: 'demo_rf_dental',     name: 'Family dental check',      interval: 6, unit: 'months', lastReplaced: ymd(daysAgo(200)), notes: 'All five — book together', linkedItemId: null, createdAt: iso(daysAgo(400)) },
    { id: 'demo_rf_eyes',       name: 'Kids eye tests',           interval: 12, unit: 'months', lastReplaced: ymd(daysAgo(370)), notes: 'Specsavers — back-to-school', linkedItemId: null, createdAt: iso(daysAgo(500)) },
    { id: 'demo_rf_petjab',     name: 'Dog booster jabs',         interval: 12, unit: 'months', lastReplaced: ymd(daysAgo(340)), notes: 'Vets4Pets', linkedItemId: null, createdAt: iso(daysAgo(500)) },
  ];

  // ── Notes — family logistics ──────────────────────────────────────────
  notes = [
    {
      id: 'demo_nf_school',
      title: 'School week schedule',
      body: 'TOM (14) — Yr 9\n• Mon: rugby practice 4pm\n• Wed: Maths club lunch\n• Fri: PE kit home for wash\n\nMIA (11) — Yr 7\n• Tues: ballet 5pm\n• Thurs: art club until 4:30\n• Fri: swimming galas (until end of term)\n\nNOAH (8) — Yr 4\n• Mon: spellings test\n• Wed: forest school (welly day)\n• Thurs: violin lesson 4pm\n• Fri: golden time treats',
      locked: false, pinned: true, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(40)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_nf_clubs',
      title: 'Clubs & fees this term',
      body: '£ Fees due:\n• Tom rugby — paid Sept (£60/term)\n• Mia ballet — DD running (£35/mth)\n• Mia swimming — paid Sept (£90/term)\n• Noah violin — DD running (£18/lesson)\n\nTransport rota:\n• Mon: Beth covers rugby\n• Tues: Liam ballet pickup\n• Wed: Liam forest school dropoff\n• Thurs: Beth violin run\n• Fri: shared',
      locked: false, pinned: true, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(35)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_nf_birthdays',
      title: 'Family birthdays this year',
      body: '— Mia\'s 12th: 23 June (sleepover, 6 friends — the planning starts now)\n— Tom\'s 15th: 14 August (laser tag with mates? cinema?)\n— Noah\'s 9th: 2 November (party at the trampoline place)\n— Beth\'s b\'day: 19 May — Liam to organise dinner\n— Liam\'s b\'day: 8 October',
      locked: false, pinned: false, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(20)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_nf_holiday',
      title: 'Summer holiday — Cornwall',
      body: 'Booked! Padstow cottage, 3-10 August.\n\nTo do before we go:\n• Kennel for dog (book by end May)\n• Pause milk delivery\n• Travel insurance (family policy)\n• Pack the cool box\n• Beach kit — boogie boards still in attic?\n\nMoney target: £1,200 for week + petrol + eating out — saving £150/mth into holiday pot.',
      locked: false, pinned: false, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(15)), updatedAt: now, deletedAt: null,
    },
    {
      id: 'demo_nf_meals',
      title: 'Batch cook plan',
      body: 'Sunday cook-day:\n• Bolognese (2 large) — freeze 4 portions\n• Chilli (1 large) — freeze 4\n• Chicken curry — fresh + 2 freezer portions\n• Mac & cheese — 2 portions for Noah\n\nFreezer running low on emergency dinners — top up this weekend.',
      locked: false, pinned: false, archived: false, colour: null,
      tickBoxesVisible: false, tickBoxes: {},
      createdAt: iso(daysAgo(7)), updatedAt: now, deletedAt: null,
    },
  ];

  // ── Budget — family finances ──────────────────────────────────────────
  budgetAccounts = [
    {
      id: 'demo_acc_main', name: 'Joint current', type: 'current',
      isPrimary: true, balance: 4280.65, balanceAsOf: ymd(daysAgo(1)),
      color: '#5b8dee', notes: '', archived: false,
      createdAt: iso(daysAgo(1500)), updatedAt: now,
    },
    {
      id: 'demo_acc_savings', name: 'Family savings', type: 'savings',
      isPrimary: false, balance: 12400.00, balanceAsOf: ymd(daysAgo(1)),
      color: '#4cbb8a', notes: 'Emergency fund + holidays', archived: false,
      createdAt: iso(daysAgo(1500)), updatedAt: now,
    },
    {
      id: 'demo_acc_kids', name: 'Kids ISAs (combined)', type: 'savings',
      isPrimary: false, balance: 8600.00, balanceAsOf: ymd(daysAgo(1)),
      color: '#a280e8', notes: 'Junior ISAs for the three kids', archived: false,
      createdAt: iso(daysAgo(1500)), updatedAt: now,
    },
  ];

  budgetCategories = [
    { id: 'demo_cat_grocery', name: 'Groceries',       monthlyBudget: 750, weeklyBudget: null, budgetCycle: 'monthly', color: '#e8a838', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_eatout',  name: 'Eating out',      monthlyBudget: 220, weeklyBudget: null, budgetCycle: 'monthly', color: '#e85d8e', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_petrol',  name: 'Fuel & transport',monthlyBudget: 320, weeklyBudget: null, budgetCycle: 'monthly', color: '#5b8dee', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_kids',    name: 'Kids clubs/extras',monthlyBudget: 280, weeklyBudget: null, budgetCycle: 'monthly', color: '#a280e8', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_pet',     name: 'Pets',            monthlyBudget: 80,  weeklyBudget: null, budgetCycle: 'monthly', color: '#5dbb88', archived: false, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_cat_lunch',   name: 'School lunches',  monthlyBudget: null,weeklyBudget: 65, budgetCycle: 'weekly', color: '#e8b85d', archived: false, createdAt: iso(daysAgo(120)), updatedAt: now },
  ];

  // Anchors so the demo lands on a useful state
  const currentMonthIdx = today.getMonth();
  const tvAnchor       = (currentMonthIdx + 1) % 12;
  const primeAnchor    = (currentMonthIdx + 5) % 12;
  const insuranceAnchor= (currentMonthIdx + 7) % 12;
  const holidayAnchor  = (currentMonthIdx + 3) % 12;
  const carAnchor      = (currentMonthIdx + 4) % 12;

  bills = [
    { id: 'demo_bill_mortgage', name: 'Mortgage',       amount: 1185.00, variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1,  categoryId: null, notes: 'Halifax fixed', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(1500)), updatedAt: now },
    { id: 'demo_bill_council',  name: 'Council tax',    amount: 215.00,  variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 5,  categoryId: null, notes: 'Band D', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_gas',      name: 'Gas & electric', amount: 175.00,  variableAmount: true,  frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 15, categoryId: null, notes: 'British Gas — varies', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_water',    name: 'Water',          amount: 38.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 18, categoryId: null, notes: 'Thames Water', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_broadband',name: 'Broadband',      amount: 32.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 12, categoryId: null, notes: 'BT Fibre', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_mobiles',  name: 'Mobiles (family)',amount: 75.00,  variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 8,  categoryId: null, notes: 'Liam + Beth + Tom', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_clubs',    name: 'Mia ballet',     amount: 35.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1,  categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_violin',   name: 'Noah violin',    amount: 72.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1,  categoryId: null, notes: '4 lessons × £18', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_dinners',  name: 'School dinners', amount: 95.00,   variableAmount: true,  frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1,  categoryId: null, notes: 'Three kids — varies', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_carfin',   name: 'Car finance',    amount: 295.00,  variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 28, categoryId: null, notes: 'Skoda Kodiaq', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(720)), updatedAt: now },
    { id: 'demo_bill_carins',   name: 'Car insurance',  amount: 68.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 12, categoryId: null, notes: 'DD — Direct Line', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_homeins',  name: 'Home insurance', amount: 38.00,   variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1,  categoryId: null, notes: 'Buildings + contents', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_streaming',name: 'Netflix + Disney+', amount: 28.00, variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 5,  categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'lump',  splitInto: null, createdAt: iso(daysAgo(180)), updatedAt: now },

    // Split bills — the ones that benefit most from the multi-month flow
    { id: 'demo_bill_tv',       name: 'TV License',     amount: 169.50,  variableAmount: false, frequency: { unit: 'year',  interval: 1, anchorMonth: tvAnchor }, dayOfMonth: 9, categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 12 }, createdAt: iso(daysAgo(120)), updatedAt: now },
    { id: 'demo_bill_prime',    name: 'Amazon Prime',   amount: 95.04,   variableAmount: false, frequency: { unit: 'year',  interval: 1, anchorMonth: primeAnchor }, dayOfMonth: 1, categoryId: null, notes: '', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 12 }, createdAt: iso(daysAgo(200)), updatedAt: now },
    { id: 'demo_bill_holiday',  name: 'Summer holiday pot', amount: 1200.00, variableAmount: false, frequency: { unit: 'year', interval: 1, anchorMonth: holidayAnchor }, dayOfMonth: 1, categoryId: null, notes: 'Cornwall — saving £100/mth', accountId: 'demo_acc_savings', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 12 }, createdAt: iso(daysAgo(180)), updatedAt: now },
    { id: 'demo_bill_school',   name: 'School trip pot', amount: 360.00, variableAmount: false, frequency: { unit: 'month', interval: 6, anchorMonth: insuranceAnchor }, dayOfMonth: 10, categoryId: null, notes: 'Yr 7 residential + Yr 4 farm', accountId: 'demo_acc_savings', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 6 }, createdAt: iso(daysAgo(120)), updatedAt: now },
    { id: 'demo_bill_carmot',   name: 'MOT + service',  amount: 280.00,  variableAmount: false, frequency: { unit: 'year',  interval: 1, anchorMonth: carAnchor }, dayOfMonth: 15, categoryId: null, notes: 'KwikFit annual', accountId: 'demo_acc_main', archived: false, paymentStrategy: 'split', splitInto: { unit: 'month', count: 12 }, createdAt: iso(daysAgo(180)), updatedAt: now },

    // Archived
    { id: 'demo_bill_old_nursery', name: 'Nursery (Noah, finished)', amount: 720.00, variableAmount: false, frequency: { unit: 'month', interval: 1, anchorMonth: null }, dayOfMonth: 1, categoryId: null, notes: 'Cancelled when Noah started school', accountId: 'demo_acc_main', archived: true, paymentStrategy: 'lump', splitInto: null, createdAt: iso(daysAgo(1300)), updatedAt: iso(daysAgo(900)) },
  ];
  billInstances = {};

  // Family-sized transactions — much more activity
  const txList = [
    { id: 'demo_txf_1',  date: ymd(daysAgo(1)),  amount: 142.30, categoryId: 'demo_cat_grocery', description: 'Sainsbury\'s big shop',  accountId: 'demo_acc_main' },
    { id: 'demo_txf_2',  date: ymd(daysAgo(1)),  amount: 8.50,   categoryId: 'demo_cat_lunch',   description: 'School lunch top-up',     accountId: 'demo_acc_main' },
    { id: 'demo_txf_3',  date: ymd(daysAgo(2)),  amount: 32.00,  categoryId: 'demo_cat_pet',     description: 'Dog food + treats',       accountId: 'demo_acc_main' },
    { id: 'demo_txf_4',  date: ymd(daysAgo(3)),  amount: 18.50,  categoryId: 'demo_cat_grocery', description: 'Tesco top-up',            accountId: 'demo_acc_main' },
    { id: 'demo_txf_5',  date: ymd(daysAgo(3)),  amount: 12.00,  categoryId: 'demo_cat_lunch',   description: 'School lunch top-up',     accountId: 'demo_acc_main' },
    { id: 'demo_txf_6',  date: ymd(daysAgo(4)),  amount: 65.50,  categoryId: 'demo_cat_petrol',  description: 'Esso fill-up',            accountId: 'demo_acc_main' },
    { id: 'demo_txf_7',  date: ymd(daysAgo(5)),  amount: 22.00,  categoryId: 'demo_cat_kids',    description: 'Tom rugby kit',           accountId: 'demo_acc_main' },
    { id: 'demo_txf_8',  date: ymd(daysAgo(6)),  amount: 48.50,  categoryId: 'demo_cat_eatout',  description: 'Pizza Express (Sat)',     accountId: 'demo_acc_main' },
    { id: 'demo_txf_9',  date: ymd(daysAgo(7)),  amount: 96.40,  categoryId: 'demo_cat_grocery', description: 'Sainsbury\'s mid-week',   accountId: 'demo_acc_main' },
    { id: 'demo_txf_10', date: ymd(daysAgo(8)),  amount: 15.00,  categoryId: 'demo_cat_kids',    description: 'Mia ballet shoes',        accountId: 'demo_acc_main' },
    { id: 'demo_txf_11', date: ymd(daysAgo(9)),  amount: 72.30,  categoryId: 'demo_cat_petrol',  description: 'Tesco fuel',              accountId: 'demo_acc_main' },
    { id: 'demo_txf_12', date: ymd(daysAgo(10)), amount: 9.50,   categoryId: 'demo_cat_lunch',   description: 'School lunch top-up',     accountId: 'demo_acc_main' },
    { id: 'demo_txf_13', date: ymd(daysAgo(11)), amount: 24.00,  categoryId: 'demo_cat_kids',    description: 'Noah birthday gift',      accountId: 'demo_acc_main' },
    { id: 'demo_txf_14', date: ymd(daysAgo(13)), amount: 165.20, categoryId: 'demo_cat_grocery', description: 'Costco bulk run',         accountId: 'demo_acc_main' },
    { id: 'demo_txf_15', date: ymd(daysAgo(14)), amount: 35.00,  categoryId: 'demo_cat_eatout',  description: 'Sunday lunch (Wagamama)', accountId: 'demo_acc_main' },
    { id: 'demo_txf_16', date: ymd(daysAgo(16)), amount: 11.00,  categoryId: 'demo_cat_lunch',   description: 'School lunch top-up',     accountId: 'demo_acc_main' },
    { id: 'demo_txf_17', date: ymd(daysAgo(18)), amount: 54.10,  categoryId: 'demo_cat_petrol',  description: 'Esso',                    accountId: 'demo_acc_main' },
    { id: 'demo_txf_18', date: ymd(daysAgo(20)), amount: 88.50,  categoryId: 'demo_cat_grocery', description: 'Sainsbury\'s',            accountId: 'demo_acc_main' },
    { id: 'demo_txf_19', date: ymd(daysAgo(22)), amount: 55.00,  categoryId: 'demo_cat_kids',    description: 'Tom rugby club fees',     accountId: 'demo_acc_main' },
    { id: 'demo_txf_20', date: ymd(daysAgo(24)), amount: 18.00,  categoryId: 'demo_cat_pet',     description: 'Cat litter',              accountId: 'demo_acc_main' },
    { id: 'demo_txf_21', date: ymd(daysAgo(27)), amount: 132.40, categoryId: 'demo_cat_grocery', description: 'Sainsbury\'s big shop',   accountId: 'demo_acc_main' },
  ];
  transactions = {};
  for (const tx of txList) {
    const txYm = tx.date.slice(0, 7);
    if (!transactions[txYm]) transactions[txYm] = {};
    transactions[txYm][tx.id] = { ...tx, createdAt: iso(daysAgo(0)), updatedAt: now };
  }

  incomeTemplates = [
    {
      id: 'demo_inc_liam', name: 'Salary (Liam)', amount: 4150, variableAmount: false,
      frequency: { unit: 'month', interval: 1, anchorMonth: null },
      dayOfMonth: 27, accountId: 'demo_acc_main', notes: 'Senior project manager',
      archived: false, createdAt: iso(daysAgo(1500)), updatedAt: now,
    },
    {
      id: 'demo_inc_beth', name: 'Salary (Beth, p/t)', amount: 1620, variableAmount: false,
      frequency: { unit: 'month', interval: 1, anchorMonth: null },
      dayOfMonth: 25, accountId: 'demo_acc_main', notes: '3 days/week — paediatric nurse',
      archived: false, createdAt: iso(daysAgo(1200)), updatedAt: now,
    },
    {
      id: 'demo_inc_cb', name: 'Child Benefit', amount: 254.40, variableAmount: false,
      frequency: { unit: 'month', interval: 1, anchorMonth: null },
      dayOfMonth: 5, accountId: 'demo_acc_main', notes: '3 children',
      archived: false, createdAt: iso(daysAgo(1500)), updatedAt: now,
    },
  ];

  const _isoFromYmd = (s) => new Date(s + 'T09:00:00Z').toISOString();
  const liamDate = `${prevYm}-27`;
  const bethDate = `${prevYm}-25`;
  const cbDate   = `${prevYm}-05`;
  incomeEntries = {};
  incomeEntries[prevYm] = {
    [`incTpl_demo_inc_liam__${liamDate}`]: {
      id: `incTpl_demo_inc_liam__${liamDate}`, date: liamDate,
      amount: 4150, actualAmount: 4150, source: 'template_instance',
      templateId: 'demo_inc_liam', accountId: 'demo_acc_main', notes: '',
      paidAt: _isoFromYmd(liamDate), createdAt: _isoFromYmd(liamDate),
      createdBy: null, updatedAt: _isoFromYmd(liamDate),
    },
    [`incTpl_demo_inc_beth__${bethDate}`]: {
      id: `incTpl_demo_inc_beth__${bethDate}`, date: bethDate,
      amount: 1620, actualAmount: 1620, source: 'template_instance',
      templateId: 'demo_inc_beth', accountId: 'demo_acc_main', notes: '',
      paidAt: _isoFromYmd(bethDate), createdAt: _isoFromYmd(bethDate),
      createdBy: null, updatedAt: _isoFromYmd(bethDate),
    },
    [`incTpl_demo_inc_cb__${cbDate}`]: {
      id: `incTpl_demo_inc_cb__${cbDate}`, date: cbDate,
      amount: 254.40, actualAmount: 254.40, source: 'template_instance',
      templateId: 'demo_inc_cb', accountId: 'demo_acc_main', notes: '',
      paidAt: _isoFromYmd(cbDate), createdAt: _isoFromYmd(cbDate),
      createdBy: null, updatedAt: _isoFromYmd(cbDate),
    },
  };

  budgetSettings = {
    ...((typeof budgetSettings === 'object' && budgetSettings) || {}),
    payDayOfMonth: 27,
    materialisedMonths: [],
  };
}


// ── DEMO MODE banner ────────────────────────────────────────────────────
// Persistent banner pinned at the top of the app while in demo mode.
// Three controls: a label, "Save my work" (opens conversion path), and
// "Exit demo".
function _showDemoBanner() {
  if (!window._demoMode) return;
  if (document.getElementById('demo-banner')) return;
  const banner = document.createElement('div');
  banner.id = 'demo-banner';
  banner.className = 'demo-banner';
  // Persona starts at 'couple' unless something else has set it. The toggle
  // button switches between 'couple' and 'family' demo data.
  const persona = window._demoPersona || 'couple';
  const personaLabel = persona === 'family' ? 'Family of 5' : 'Couple';
  const switchLabel  = persona === 'family' ? 'Switch to Couple' : 'Switch to Family';
  banner.innerHTML = `
    <span class="demo-banner-label">
      <span class="demo-banner-icon" aria-hidden="true">🎮</span>
      <strong>Demo mode</strong>
      <span class="demo-banner-sub">— <span class="demo-banner-persona">${personaLabel}</span>, nothing saved</span>
    </span>
    <span class="demo-banner-actions">
      <button class="demo-banner-persona-btn" onclick="_toggleDemoPersona()" title="${switchLabel}">${switchLabel}</button>
      <button class="demo-banner-tour" onclick="_demoReplayTour()" title="Replay the guided tour">Tour</button>
      <button class="demo-banner-save" onclick="_demoStartConversion()">
        <svg class="icon" aria-hidden="true"><use href="#i-lock"></use></svg>
        Save my work
      </button>
      <button class="demo-banner-exit" onclick="_exitDemo()" title="Exit demo">
        <svg class="icon" aria-hidden="true"><use href="#i-x"></use></svg>
      </button>
    </span>`;
  document.body.insertBefore(banner, document.body.firstChild);
  // Push the rest of the page down so the banner doesn't overlap content.
  // We use a CSS variable so other layout (header, FAB) can read the offset.
  document.documentElement.style.setProperty('--demo-banner-offset', '38px');
  document.body.classList.add('has-demo-banner');
}

// Update the persona indicator + button label after a switch. Lighter than
// re-rendering the whole banner.
function _updateDemoPersonaButton() {
  const banner = document.getElementById('demo-banner');
  if (!banner) return;
  const persona = window._demoPersona || 'couple';
  const personaLabel = persona === 'family' ? 'Family of 5' : 'Couple';
  const switchLabel  = persona === 'family' ? 'Switch to Couple' : 'Switch to Family';
  const personaSpan = banner.querySelector('.demo-banner-persona');
  if (personaSpan) personaSpan.textContent = personaLabel;
  const btn = banner.querySelector('.demo-banner-persona-btn');
  if (btn) {
    btn.textContent = switchLabel;
    btn.title = switchLabel;
  }
}

// Banner button click — flips between couple and family. Wired to
// _switchDemoPersona which clears the seed and re-renders in place.
function _toggleDemoPersona() {
  const next = window._demoPersona === 'family' ? 'couple' : 'family';
  _switchDemoPersona(next);
}

function _hideDemoBanner() {
  const banner = document.getElementById('demo-banner');
  if (banner) banner.remove();
  document.documentElement.style.removeProperty('--demo-banner-offset');
  document.body.classList.remove('has-demo-banner');
}

function _exitDemo() {
  if (!confirm('Exit demo? Any changes you made here will be lost.')) return;
  // Wipe demo IDB and flip back to unauth so the next dbGet doesn't open
  // stockroom_demo
  try { indexedDB.deleteDatabase('stockroom_demo'); } catch(e) {}
  _setActiveDbForSignedOut();
  window._demoMode = false;
  _hideDemoBanner();
  location.href = '/';
}

// ── DEMO MODE contextual nudges ─────────────────────────────────────────
// Small dismissible callouts that appear on each tab the first time it's
// visited during a demo session. Not modal — they don't block interaction,
// just point at something interesting. Dismissed state lives in
// localStorage so a refresh during the same session doesn't replay them.
const _DEMO_NUDGE_CONTENT = {
  stockroom: {
    text: "These items show every status — Critical, Low, Good, even a pending delivery. Tap any card for quick actions, or long-press to edit details.",
    anchor: '#items-grid',
    placement: 'top',
  },
  groceries: {
    text: "Multiple lists per store, drag-zone bulk actions, and a Quick List for typing items in fast — try the FAB. This list is in Stock Check mode; hit Start Shopping to filter to what's needed.",
    anchor: '#grocery-list-body',
    placement: 'top',
  },
  savings: {
    text: "Stockroom finds money for you — it spots Subscribe & Save eligibility from your real prices. Two items here qualify.",
    anchor: '#view-savings',
    placement: 'top',
  },
  budget: {
    text: "Track recurring bills, see what's safe to spend, and split big bills (annual subscriptions, quarterly TV License) across months — see Multi-month bills at the bottom of Bills. Try Basic Mode in the subnav for a read-only timeline.",
    anchor: '#view-budget',
    placement: 'top',
  },
};

function _demoNudgeKey(which) { return `stockroom_demo_nudge_${which}`; }
function _demoNudgeDismissed(which) {
  try { return localStorage.getItem(_demoNudgeKey(which)) === '1'; }
  catch (e) { return false; }
}
function _demoNudgeMarkSeen(which) {
  try { localStorage.setItem(_demoNudgeKey(which), '1'); } catch (e) {}
}
function _demoNudgeReset() {
  for (const key of Object.keys(_DEMO_NUDGE_CONTENT)) {
    try { localStorage.removeItem(_demoNudgeKey(key)); } catch (e) {}
  }
}

function _showDemoNudge(which) {
  if (!window._demoMode) return;
  if (_demoNudgeDismissed(which)) return;
  const cfg = _DEMO_NUDGE_CONTENT[which];
  if (!cfg) return;
  // Drop any existing nudge before showing the new one — only one at a time
  _hideAllDemoNudges();

  const anchor = document.querySelector(cfg.anchor);
  if (!anchor) return; // can't render without a target

  const nudge = document.createElement('div');
  nudge.className = 'demo-nudge';
  nudge.dataset.nudge = which;
  nudge.innerHTML = `
    <div class="demo-nudge-arrow"></div>
    <div class="demo-nudge-body">
      <span class="demo-nudge-icon" aria-hidden="true">💡</span>
      <span class="demo-nudge-text">${cfg.text}</span>
    </div>
    <button class="demo-nudge-ok" onclick="_dismissDemoNudge('${which}')">Got it</button>`;
  document.body.appendChild(nudge);

  _positionDemoNudge(nudge, anchor, cfg.placement);

  // Reposition on resize / scroll so the nudge keeps tracking the anchor
  const reposition = () => _positionDemoNudge(nudge, anchor, cfg.placement);
  nudge._reposition = reposition;
  window.addEventListener('resize', reposition);
  window.addEventListener('scroll', reposition, { passive: true });

  // Slide-in animation kicks off after the element is in the DOM
  requestAnimationFrame(() => nudge.classList.add('visible'));
}

function _positionDemoNudge(nudge, anchor, placement) {
  const rect = anchor.getBoundingClientRect();
  const nudgeRect = nudge.getBoundingClientRect();
  const margin = 8;
  // Always horizontally centred on the anchor, clamped to viewport
  let left = rect.left + (rect.width / 2) - (nudgeRect.width / 2);
  left = Math.max(12, Math.min(left, window.innerWidth - nudgeRect.width - 12));
  let top;
  let actualPlacement = placement;
  if (placement === 'bottom') {
    top = rect.bottom + margin;
  } else {
    // 'top' — sit above the anchor, but if there's not enough room, flip to below
    top = rect.top - nudgeRect.height - margin;
    if (top < 50) { top = rect.bottom + margin; actualPlacement = 'bottom'; }
  }
  nudge.style.left = `${Math.round(left)}px`;
  nudge.style.top  = `${Math.round(top)}px`;
  nudge.dataset.placement = actualPlacement;
}

function _dismissDemoNudge(which) {
  _demoNudgeMarkSeen(which);
  const nudge = document.querySelector(`.demo-nudge[data-nudge="${which}"]`);
  if (!nudge) return;
  nudge.classList.remove('visible');
  if (nudge._reposition) {
    window.removeEventListener('resize', nudge._reposition);
    window.removeEventListener('scroll', nudge._reposition);
  }
  setTimeout(() => nudge.remove(), 250);
}

function _hideAllDemoNudges() {
  document.querySelectorAll('.demo-nudge').forEach(el => {
    if (el._reposition) {
      window.removeEventListener('resize', el._reposition);
      window.removeEventListener('scroll', el._reposition);
    }
    el.remove();
  });
}

// Public: replay all nudges (called from the banner's "Replay tour" link)
function _demoReplayTour() {
  if (!window._demoMode) return;
  _demoNudgeReset();
  _hideAllDemoNudges();
  // Show whichever tab the user is on
  const view = _currentView || 'stock';
  const which = ({ stock: 'stockroom', grocery: 'groceries', savings: 'savings', budget: 'budget' })[view];
  if (which) _showDemoNudge(which);
  toast('Tour reset');
}

// ── DEMO → real account conversion ──────────────────────────────────────
// "Save my work" captures the in-memory state right now, opens registration,
// and after the user successfully registers we restore that state and write
// it to real IDB (and to the server via the first kvSyncNow). Net effect:
// the user spent 5 minutes playing with demo data, decided to keep it, and
// their first sign-in lands on exactly the state they were just looking at.
let _demoConvertSeed = null;

function _demoStartConversion() {
  // Snapshot every in-memory data variable so registration can restore it
  // afterwards. We deep-clone so any subsequent UI interactions during the
  // signup wizard (closing the banner, etc.) can't mutate the snapshot.
  try {
    _demoConvertSeed = {
      items:           JSON.parse(JSON.stringify(items || [])),
      settings:        JSON.parse(JSON.stringify(settings || {})),
      reminders:       JSON.parse(JSON.stringify(reminders || [])),
      notes:           JSON.parse(JSON.stringify(notes || [])),
      groceryItems:    JSON.parse(JSON.stringify(groceryItems || [])),
      groceryDepts:    JSON.parse(JSON.stringify(groceryDepts || [])),
      groceryLists:    JSON.parse(JSON.stringify(groceryLists || [])),
      bills:           JSON.parse(JSON.stringify(bills || [])),
      billInstances:   JSON.parse(JSON.stringify(billInstances || {})),
      budgetSettings:  JSON.parse(JSON.stringify(budgetSettings || {})),
      budgetCategories:JSON.parse(JSON.stringify(budgetCategories || [])),
      transactions:    JSON.parse(JSON.stringify(transactions || {})),
      budgetAccounts:  JSON.parse(JSON.stringify(budgetAccounts || [])),
      incomeTemplates: JSON.parse(JSON.stringify(incomeTemplates || [])),
      incomeEntries:   JSON.parse(JSON.stringify(incomeEntries || {})),
    };
  } catch (e) {
    console.error('Demo conversion snapshot failed:', e);
    _demoConvertSeed = null;
    toast('Could not save your work — please try again');
    return;
  }
  // The banner stays — it changes message until conversion completes,
  // so the user has a visual cue they're still in demo flow.
  const banner = document.getElementById('demo-banner');
  if (banner) {
    const sub = banner.querySelector('.demo-banner-sub');
    if (sub) sub.textContent = '— sign up to keep your data';
    const saveBtn = banner.querySelector('.demo-banner-save');
    if (saveBtn) saveBtn.style.display = 'none';
  }
  // Make sure the registration wizard is visible — demo mode had it hidden.
  const wiz = document.getElementById('wizard');
  if (wiz) wiz.style.display = 'flex';
  document.body.classList.add('wizard-active');
  // Send the user into the registration wizard. showKvRegister() opens the
  // create-account screen; after kvRegister() finishes we'll catch the
  // _demoConvertSeed flag and restore.
  if (typeof showKvRegister === 'function') {
    showKvRegister();
  } else {
    toast('Sign-up unavailable — try refreshing');
  }
}

// Called from kvRegister immediately after kvStoreSession succeeds. Switches
// the storage backend from in-memory to real IDB, restores the captured
// demo state to the in-memory variables, and persists it. The next
// kvSyncNow (which fires from _enterStockroom) will push everything up.
async function _demoCompleteConversion() {
  if (!_demoConvertSeed) return;
  const seed = _demoConvertSeed;
  _demoConvertSeed = null;
  // Flip the demo flag BEFORE we persist — sync paths, banner logic, and
  // other _demoMode gates immediately stop. The active DB was already
  // switched to the per-user DB by kvStoreSession; the demo DB is now
  // orphaned, so delete it to reclaim the space.
  window._demoMode = false;
  try { indexedDB.deleteDatabase('stockroom_demo'); } catch(e) {}

  // Restore the in-memory state from the snapshot. We merge user settings
  // (email, MFA config) on top of the demo settings — registration just
  // populated those, and we don't want to clobber them.
  items           = seed.items;
  reminders       = seed.reminders;
  notes           = seed.notes;
  groceryItems    = seed.groceryItems;
  groceryDepts    = seed.groceryDepts;
  groceryLists    = seed.groceryLists;
  bills           = seed.bills;
  billInstances   = seed.billInstances;
  budgetSettings  = seed.budgetSettings;
  budgetCategories= seed.budgetCategories;
  transactions    = seed.transactions;
  budgetAccounts  = seed.budgetAccounts;
  incomeTemplates = seed.incomeTemplates;
  incomeEntries   = seed.incomeEntries;
  // Carry over demo settings but keep the new account's auth-related fields
  settings = {
    ...seed.settings,
    ...settings,
    // Setup flags reset for the real account so the protect screen flow
    // works correctly with the new recovery codes
    _setupProtectSeen: false,
    _setupCountrySet:  true, // GB was set during demo
  };

  // Persist all in-memory state to real IDB. Each dbPut bypasses the shim
  // now that _demoMode is false. Mirror the keys used by the various
  // load/save helpers so subsequent reads find everything.
  try {
    await dbPut('items',            'items',            items);
    await dbPut('settings',         'settings',         settings);
    await dbPut('reminders',        'reminders',        reminders);
    await dbPut('groceries',        'items',            groceryItems);
    await dbPut('departments',      'departments',      groceryDepts);
    await dbPut('groceryLists',     'groceryLists',     groceryLists);
    await dbPut('items',            'notes',            notes);
    await dbPut('bills',            'bills',            bills);
    await dbPut('billInstances',    'billInstances',    billInstances);
    await dbPut('budgetSettings',   'budgetSettings',   budgetSettings);
    await dbPut('budgetCategories', 'budgetCategories', budgetCategories);
    await dbPut('transactions',     'transactions',     transactions);
    await dbPut('budgetAccounts',   'budgetAccounts',   budgetAccounts);
    await dbPut('incomeTemplates',  'incomeTemplates',  incomeTemplates);
    await dbPut('incomeEntries',    'incomeEntries',    incomeEntries);
    // Stash a profile so loadProfile finds something
    if (typeof saveCurrentProfile === 'function') {
      try { await saveCurrentProfile(); } catch(e) {}
    }
  } catch (e) {
    console.error('Demo conversion persist failed:', e);
  }
  _hideDemoBanner();
  toast('Your demo data is now your account');
}
