// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET CORE (formerly the head of budget.js)
//
//  This file is the ALWAYS-LOADED half of the Budget feature. It contains
//  only what runs on the boot / sync / omnibox-quick-add / settings hot
//  paths — load + save, all CRDT merge functions (called from kvSyncNow and
//  the share-import path in app.js), the quick-add parser + commit, and the
//  shared data getters those depend on. It is loaded eagerly via
//    <script src="budget.js" defer>
//  in index.html, exactly as the old single-file budget.js was.
//
//  The Budget VIEW (all render*, editors, modals, cash-flow calendar, the
//  Amazon importer, basic mode, and the phase 3/4/5 decorator wrappers) lives
//  in budget-ui.js, which is LAZY-LOADED on first open of the Budget view via
//  window._loadBudgetUI() (see index.html, same pattern as _loadScanner /
//  _loadDemo). app.js awaits _loadBudgetUI() inside showView('budget') before
//  calling renderBudget(), and the FAB add-actions / search-chip actions that
//  open budget editors await it too.
//
//  Three call sites here reach into budget-ui.js and are guarded with
//  `typeof X === 'function'` (confirmQuickAdd's post-add re-render, and
//  openQuickAddSpend's first-run setup modal). All Budget STATE that app.js
//  reads directly (bills, billInstances, budgetSettings, budgetCategories,
//  transactions, budgetAccounts, incomeTemplates, incomeEntries, all the
//  *DeletedIds tombstone sets) lives HERE so the sync path never depends on
//  the lazy bundle.
//
//  Load order in index.html MUST be: app.js FIRST, then budget.js.
//  budget.js and budget-ui.js (and app.js, index.html) must all land in
//  GitHub together for this split to work.
// ═══════════════════════════════════════════════════════════════════════════

// ── State ──────────────────────────────────────────────────────────────────
let bills          = [];          // bill templates (recurring)
let billInstances  = {};          // { 'YYYY-MM': { [billId]: instance } }
let billsDeletedIds = new Set();  // tombstones for hard-deleted bill templates
let budgetSettings = {
  weekStart: 'mon',                // 'mon' | 'sun' — Phase 2+ consumer
  materialisedMonths: [],          // months we've generated instances for (union-merged)
};

// ── Persistence ────────────────────────────────────────────────────────────
async function loadBudget() {
  const storedBills    = await dbGet('bills',          'bills');
  const storedInstances = await dbGet('billInstances', 'billInstances');
  const storedSettings = await dbGet('budgetSettings', 'budgetSettings');
  const storedTomb     = await dbGet('billsDeletedIds', 'billsDeletedIds');

  if (Array.isArray(storedBills)) bills = storedBills;
  if (storedInstances && typeof storedInstances === 'object') billInstances = storedInstances;
  if (storedSettings && typeof storedSettings === 'object') {
    budgetSettings = { ...budgetSettings, ...storedSettings };
    if (!Array.isArray(budgetSettings.materialisedMonths)) budgetSettings.materialisedMonths = [];
  }
  if (Array.isArray(storedTomb)) billsDeletedIds = new Set(storedTomb);
  // Apply tombstones to in-memory state defensively in case they arrived
  // before the bills array was loaded (e.g. from a remote sync that landed
  // partially). Filters bills and strips matching instances.
  if (billsDeletedIds.size > 0) {
    bills = bills.filter(b => !billsDeletedIds.has(b.id));
    for (const yyyymm of Object.keys(billInstances)) {
      for (const key of Object.keys(billInstances[yyyymm])) {
        const inst = billInstances[yyyymm][key];
        if (inst && billsDeletedIds.has(inst.billId)) {
          delete billInstances[yyyymm][key];
        }
      }
      if (Object.keys(billInstances[yyyymm]).length === 0) delete billInstances[yyyymm];
    }
  }
  // Run the per-month migration over every month we have on hand. This
  // collapses legacy `__SAV__` keys to the uniform format and purges
  // phantom instances from the pre-fix _setInstance bug. Cheap when nothing
  // needs migrating.
  if (typeof _migrateBillInstancesIfNeeded === 'function') {
    let touched = false;
    for (const yyyymm of Object.keys(billInstances)) {
      if (_migrateBillInstancesIfNeeded(yyyymm)) touched = true;
    }
    if (touched) await saveBudgetLocal();
  }
}

async function saveBudgetLocal() {
  await dbPut('bills',           'bills',           bills);
  await dbPut('billInstances',   'billInstances',   billInstances);
  await dbPut('budgetSettings',  'budgetSettings',  budgetSettings);
  await dbPut('billsDeletedIds', 'billsDeletedIds', [...billsDeletedIds]);
}

// ── View-state persistence (device-local, NOT synced) ───────────────────────
// The early-rollover view override and the "Start new month" button-dismissal
// are per-device UI preferences, not budget data. They must NOT live in
// budgetSettings — that blob is encrypted, synced, and shared with household
// guests, and a sync-down was overwriting these fields (the view kept snapping
// back to the real calendar month after a refresh). localStorage is the right
// home: it survives refresh, stays on this device, and never reaches the
// share/sync layer. Mirrors the app's convention for wizard/compact/notif flags.
const _LS_ROLLED_MONTH = 'stockroom_budget_rolled_month';   // 'YYYY-MM' or absent
const _LS_NEWMONTH_DISMISS = 'stockroom_budget_newmonth_dismissed'; // 'YYYY-MM' or absent

function _yyyymm(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function _yyyymmFromString(s) {
  // Accept '2026-04' or '2026-04-15' etc.
  return s.slice(0, 7);
}

function _parseYyyymm(yyyymm) {
  const [y, m] = yyyymm.split('-').map(Number);
  return { year: y, month: m - 1 }; // month 0-indexed, JS-style
}

function _daysInMonth(year, monthZeroIdx) {
  return new Date(year, monthZeroIdx + 1, 0).getDate();
}

function _clampDayOfMonth(day, year, monthZeroIdx) {
  return Math.min(day, _daysInMonth(year, monthZeroIdx));
}

function _isoDate(year, monthZeroIdx, day) {
  const m = String(monthZeroIdx + 1).padStart(2, '0');
  const d = String(day).padStart(2, '0');
  return `${year}-${m}-${d}`;
}

function _nowIso() { return new Date().toISOString(); }

// ── Frequency engine ───────────────────────────────────────────────────────
// Returns true if a bill template should produce an instance in the given month.
function shouldBeDueInMonth(template, year, monthZeroIdx) {
  const f = template.frequency || { unit: 'month', interval: 1 };
  if (f.unit === 'year') {
    const anchor = f.anchorMonth ?? 0;
    return monthZeroIdx === anchor;
  }
  // 'month' unit
  const interval = Math.max(1, f.interval || 1);
  const anchor   = f.anchorMonth ?? 0;
  // months since epoch (Jan 2000 = 0)
  const monthsSinceEpoch = (year - 2000) * 12 + monthZeroIdx;
  const offsetFromAnchor = monthsSinceEpoch - anchor;
  return offsetFromAnchor >= 0 && offsetFromAnchor % interval === 0;
}

// ── Materialisation ────────────────────────────────────────────────────────
// Generate instances for a month from current active templates.
// Idempotent: per-template loop skips templates whose instance already exists,
// so repeated calls only add NEW templates' instances. `force` rebuilds from
// scratch (used by "Regenerate this month" in the bills list).
function _isSplitBillSavingMonth(template, year, monthZeroIdx) {
  if (!template || template.archived) return false;
  if (template.paymentStrategy !== 'split') return false;
  if (!template.splitInto || !template.splitInto.count) return false;
  // Payment months don't get saving instances
  if (shouldBeDueInMonth(template, year, monthZeroIdx)) return false;
  // Must be within an active cycle: there's a previous payment date AND
  // a next payment date bracketing this month. For a split bill anchored
  // to its next payment with no prior cycle, the previous theoretical
  // payment may predate the template's createdAt — that's fine for our
  // purposes, the user is asserting they save on the bill's calendar.
  const yyyymm = `${year}-${String(monthZeroIdx + 1).padStart(2, '0')}`;
  const prev = _prevDueDateForTemplate(template, yyyymm, /* respectCreatedAt */ false);
  const next = _nextDueDateForTemplate(template, yyyymm);
  if (!next) return false;  // bill has no future — don't generate
  // We need either a prev-due OR fallback to template creation as cycle start.
  // Either way, the month must fall between cycle-start and next-due.
  return true;
}

// For a split bill, given a *viewed* month, returns the cycle progress as
// it stands at the END of that month (i.e. how much would be saved by the
// end of the viewed month, assuming the user pays in the per-period amount
// each period). Different from getBillCarryOver which is "now"-relative.
//
// Anchor is the previous theoretical due date in the bill's calendar,
// regardless of when the template was created — picking split mode is the
// user asserting they're saving on the bill's natural cycle.
//
//   slot       = whole periods elapsed from cycle start to month end
//   totalSlots = template.splitInto.count
//   accrued    = slot × perPeriod, capped at target
//   nextDueIso = the upcoming due date relative to this month
function _nextDueMonthForTemplate(template, fromYyyymm) {
  const { year, month } = _parseYyyymm(fromYyyymm);
  // Search up to 24 months ahead — covers annual + a year of buffer.
  for (let i = 0; i < 24; i++) {
    const cursorY = year + Math.floor((month + i) / 12);
    const cursorM = (month + i) % 12;
    if (shouldBeDueInMonth(template, cursorY, cursorM)) {
      return `${cursorY}-${String(cursorM + 1).padStart(2, '0')}`;
    }
  }
  return null;
}

// Returns the ISO date of the template's next instance on or after the given
// reference yyyymm. Combines _nextDueMonthForTemplate with the template's
// dayOfMonth (clamped to month length).
function _nextDueDateForTemplate(template, fromYyyymm) {
  const dueMonth = _nextDueMonthForTemplate(template, fromYyyymm);
  if (!dueMonth) return null;
  const { year, month } = _parseYyyymm(dueMonth);
  const dom = _clampDayOfMonth(template.dayOfMonth || 1, year, month);
  return _isoDate(year, month, dom);
}

// Returns the previous due date for a template strictly BEFORE the given
// reference month (yyyymm). Walks backward up to 24 months. Returns null if
// no prior instance exists in that window.
//
// `respectCreatedAt` (default true): when true, candidates predating the
// template's createdAt are rejected — useful for the "Saving up" section
// which shouldn't credit the user with savings for months before the bill
// was added. When false, returns the theoretical previous due regardless,
// which is what the carry-over math needs: a user adding a long-cycle bill
// is asserting "I've been saving for this on the bill's natural calendar",
// even if the bill didn't yet exist in the app.
function _prevDueDateForTemplate(template, fromYyyymm, respectCreatedAt = true) {
  const { year, month } = _parseYyyymm(fromYyyymm);
  const createdIso = (template.createdAt || _nowIso()).slice(0, 10);
  for (let i = 1; i <= 24; i++) {
    // Compute (year, month - i) handling negative wraparound
    const rawM = month - i;
    const realY = year + Math.floor(rawM / 12);
    const realM = ((rawM % 12) + 12) % 12;
    if (shouldBeDueInMonth(template, realY, realM)) {
      const dom = _clampDayOfMonth(template.dayOfMonth || 1, realY, realM);
      const candidate = _isoDate(realY, realM, dom);
      if (respectCreatedAt && candidate < createdIso) return null;
      return candidate;
    }
  }
  return null;
}

// Returns { accrued, target, slot, totalSlots, perPeriod, cycleAnchorIso,
// nextDueIso, currentMonthIsPayment } for a split-strategy bill, or null
// if the bill isn't split.
//
// New (Phase 5b) model: carry-over is derived from ACTUAL paid saving
// instances within the current cycle. Each saving month creates a saving
// instance the user marks paid; doing so adds its per-period amount to
// carry-over. The payment-month instance is the existing full-amount bill.
//
// `currentMonthIsPayment`: true when the bill's payment-month instance
// exists in the current calendar month and is unpaid. In that case the UI
// should NOT include this bill in the carry-over total because the bill
// itself appears in the regular bills list with the full amount, and the
// previously-saved portion will be released by paying it.
const _BILLS_TEMPLATE_CSV = `# STOCKROOM bills template — fill in the rows below this header and import.
# Lines starting with # are comments and ignored on import.
#
# Columns:
#   name           — the bill's display name (required)
#   amount         — number, e.g. 95.50 (required, use 0 for unknown variable)
#   variableAmount — yes/no  (optional; default no. "yes" = the amount changes month to month)
#   frequency      — one of: monthly, weekly, yearly, daily  (required)
#   dayOfMonth     — 1–31, the day the bill is due  (required)
#   category       — name of an existing budget category, or blank  (optional)
#   notes          — anything else, free text  (optional; wrap in quotes if it contains commas)
#
# The example rows below show the format. Replace them with your own.
name,amount,variableAmount,frequency,dayOfMonth,category,notes
Council tax,165.00,no,monthly,1,,
Broadband,32.00,no,monthly,15,,Provider XYZ
Gas & electric,95.00,yes,monthly,22,,"Variable, averages around £95"
Car insurance,420.00,no,yearly,1,,Renewal in March
TV licence,13.50,no,monthly,5,,
`;

function mergeBills(local, remote) {
  const map = new Map();
  for (const tpl of (local || []))  map.set(tpl.id, tpl);
  for (const tpl of (remote || [])) {
    const existing = map.get(tpl.id);
    if (!existing) {
      map.set(tpl.id, tpl);
    } else {
      const lt = existing.updatedAt ? new Date(existing.updatedAt).getTime() : 0;
      const rt = tpl.updatedAt      ? new Date(tpl.updatedAt).getTime()      : 0;
      if (rt > lt) map.set(tpl.id, tpl);
    }
  }
  // Drop anything tombstoned. This is what stops a hard-deleted bill from
  // popping back in from a peer device that hadn't yet seen the delete.
  if (typeof billsDeletedIds !== 'undefined' && billsDeletedIds && billsDeletedIds.size > 0) {
    for (const id of billsDeletedIds) map.delete(id);
  }
  return Array.from(map.values());
}

// Whole-instance LWW. Instances are nested: {yyyymm: {billId: instance}}
function mergeBillInstances(local, remote) {
  const out = { ...(local || {}) };
  if (!remote) return out;
  const tomb = (typeof billsDeletedIds !== 'undefined' && billsDeletedIds) ? billsDeletedIds : new Set();
  for (const yyyymm of Object.keys(remote)) {
    if (!out[yyyymm]) out[yyyymm] = {};
    for (const billId of Object.keys(remote[yyyymm])) {
      const ri = remote[yyyymm][billId];
      // Skip instances belonging to a tombstoned template
      if (ri && tomb.has(ri.billId)) continue;
      const li = out[yyyymm][billId];
      if (!li) {
        out[yyyymm][billId] = ri;
      } else {
        const lt = li.updatedAt ? new Date(li.updatedAt).getTime() : 0;
        const rt = ri.updatedAt ? new Date(ri.updatedAt).getTime() : 0;
        if (rt > lt) out[yyyymm][billId] = ri;
      }
    }
    // Also strip any local instances tied to a tombstoned bill (might have
    // been added by an earlier sync before the tombstone arrived).
    for (const key of Object.keys(out[yyyymm])) {
      const li = out[yyyymm][key];
      if (li && tomb.has(li.billId)) delete out[yyyymm][key];
    }
    if (Object.keys(out[yyyymm]).length === 0) delete out[yyyymm];
  }
  return out;
}

function mergeBudgetSettings(local, remote) {
  // weekStart: local wins (it's a per-device preference)
  // materialisedMonths: union
  const merged = { ...(local || {}) };
  if (remote) {
    const localMonths  = new Set(local?.materialisedMonths || []);
    const remoteMonths = new Set(remote.materialisedMonths || []);
    merged.materialisedMonths = Array.from(new Set([...localMonths, ...remoteMonths]));
  }
  return merged;
}

// ── Share permission backfill (one-shot) ───────────────────────────────────
// Owner-side helper: existing share targets may not have a `budget` perm because
// they were created before this feature existed. Detects and offers to backfill.
let _budgetActivePanel = 'dashboard'; // 'dashboard' | 'bills'
let _spendCategoryFilter = null;      // categoryId filter, null = show all. Promoted to core so search-chip 'View all' (in app.js) can set it before the lazy Budget UI loads.
function _money(n) {
  if (n == null || isNaN(n)) return '£0.00';
  const sign = n < 0 ? '-' : '';
  const abs = Math.abs(n);
  return sign + '£' + abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

// _shortDate — alias for fmt(d, { short: true }). Kept for back-compat.
const _shortDate = (iso) => fmt(iso, { short: true });

function _escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ── Mark paid / unpaid / skip / unskip handlers ────────────────────────────
let budgetCategories         = [];          // [{id, name, monthlyBudget, weeklyBudget, budgetCycle, color, archived, createdAt, updatedAt}]
let transactions             = {};          // {'YYYY-MM': {[txId]: tx}}
let budgetCategoryDeletedIds = new Set();   // tombstones for category deletions
let budgetTransactionDeletedIds = new Set(); // tombstones for tx deletions
let _budgetSetupOffered      = false;       // first-run seed modal — set true after first dismiss/accept

// ── Persistence ────────────────────────────────────────────────────────────
async function loadBudgetSpend() {
  const cats     = await dbGet('budgetCategories',         'budgetCategories');
  const txs      = await dbGet('transactions',             'transactions');
  const catTomb  = await dbGet('budgetCategoryDeletedIds', 'budgetCategoryDeletedIds');
  const txTomb   = await dbGet('budgetTransactionDeletedIds', 'budgetTransactionDeletedIds');
  const setupFlag= await dbGet('budgetSettings',           '_setupOffered');

  if (Array.isArray(cats))        budgetCategories = cats;
  if (txs && typeof txs === 'object') transactions = txs;
  if (Array.isArray(catTomb))     budgetCategoryDeletedIds    = new Set(catTomb);
  if (Array.isArray(txTomb))      budgetTransactionDeletedIds = new Set(txTomb);
  if (setupFlag === true)         _budgetSetupOffered = true;
}

async function saveBudgetSpendLocal() {
  await dbPut('budgetCategories', 'budgetCategories', budgetCategories);
  await dbPut('transactions',     'transactions',     transactions);
  await dbPut('budgetCategoryDeletedIds',    'budgetCategoryDeletedIds',    [...budgetCategoryDeletedIds]);
  await dbPut('budgetTransactionDeletedIds', 'budgetTransactionDeletedIds', [...budgetTransactionDeletedIds]);
  await dbPut('budgetSettings',   '_setupOffered',    _budgetSetupOffered);
}

// ── Default seed for the first-run setup modal (data only — UI in turn 2) ──
const _BUDGET_CATEGORY_SEED = [
  { name: 'Pete spending',  monthlyBudget: 400, weeklyBudget: null, budgetCycle: 'monthly', color: '#5b8dee' },
  { name: 'Carla spending', monthlyBudget: 400, weeklyBudget: null, budgetCycle: 'monthly', color: '#e85d8e' },
  { name: 'Monday nights',  monthlyBudget: 150, weeklyBudget: null, budgetCycle: 'monthly', color: '#e8a838' },
  { name: 'Shopping',       monthlyBudget: 475, weeklyBudget: 100,  budgetCycle: 'monthly', color: '#4cbb8a' },
  { name: 'Kids stuff',     monthlyBudget: 100, weeklyBudget: null, budgetCycle: 'monthly', color: '#b35bee' },
  { name: 'Activities',     monthlyBudget: 250, weeklyBudget: null, budgetCycle: 'monthly', color: '#5bd4e8' },
];

function getActiveBudgetCategories() {
  return budgetCategories.filter(c => !c.archived);
}

function getBudgetCategoryById(id) {
  return budgetCategories.find(c => c.id === id) || null;
}

// Cycle through a small palette when colour isn't specified
const _BUDGET_PALETTE = ['#5b8dee', '#e85d8e', '#e8a838', '#4cbb8a', '#b35bee', '#5bd4e8', '#e85050', '#7880a0'];
async function createTransaction(input) {
  const date = input.date || (new Date().toISOString().slice(0, 10));
  const yyyymm = _yyyymmFromString(date);
  const tx = {
    id:         'tx_' + (typeof uid === 'function' ? uid() : Date.now() + '_' + Math.random().toString(36).slice(2, 7)),
    date,
    amount:     Number(input.amount) || 0,
    where:      (input.where || '').trim(),
    notes:      (input.notes || '').trim(),
    categoryId: input.categoryId || null,
    source:     input.source || 'manual',
    createdAt:  _nowIso(),
    createdBy:  _kvEmailHash || null,
    updatedAt:  _nowIso(),
  };
  if (!transactions[yyyymm]) transactions[yyyymm] = {};
  transactions[yyyymm][tx.id] = tx;
  await saveBudgetSpendLocal();
  _syncQueue?.enqueue();
  return tx;
}

function getTotalSpendForMonth(yyyymm) {
  let total = 0;
  for (const tx of Object.values(transactions[yyyymm] || {})) {
    total += (tx.amount || 0);
  }
  return Math.round(total * 100) / 100;
}

// Returns { spent, budget, pct, status: 'ok'|'warn'|'over' } for a category in a given period.
// `period` is 'month' or 'week'. For 'week', `referenceDate` and `weekStart` determine the range.
function parseQuickAddInput(raw, { defaultCategoryId = null } = {}) {
  if (!raw || typeof raw !== 'string') return [];
  const aliasMap = buildBudgetCategoryAliasMap();
  const merchantMap = buildBudgetMerchantMemory();

  const entries = raw.split(',').map(s => s.trim()).filter(Boolean);
  const out = [];

  for (const entry of entries) {
    const parsed = _parseQuickAddOne(entry, aliasMap, merchantMap, defaultCategoryId);
    out.push(parsed);
  }
  return out;
}

function _parseQuickAddOne(entry, aliasMap, merchantMap, defaultCategoryId) {
  const tokens = entry.split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    return { ok: false, raw: entry, error: 'empty' };
  }

  // Find the amount: first token that looks numeric (with optional £ prefix)
  let amount = null;
  let amountIdx = -1;
  for (let i = 0; i < tokens.length; i++) {
    const cleaned = tokens[i].replace(/^£/, '');
    if (/^-?\d+(\.\d+)?$/.test(cleaned)) {
      amount = parseFloat(cleaned);
      amountIdx = i;
      break;
    }
  }
  if (amount == null) {
    return { ok: false, raw: entry, error: 'no_amount' };
  }

  // Remaining tokens (excluding amount) — split into "before" and "after"
  const before = tokens.slice(0, amountIdx);
  const after  = tokens.slice(amountIdx + 1);

  // Try to resolve category from EITHER the last word of `after`,
  // OR (if `after` is empty) the last word of `before`.
  let categoryHint = null;
  let categoryId   = null;
  let where        = '';

  if (after.length > 0) {
    // "merchant 47.50 hint" — last word of after is the hint candidate
    const candidate = after.join(' ').toLowerCase();
    if (aliasMap[candidate]) {
      categoryHint = candidate;
      categoryId   = aliasMap[candidate];
      where = before.join(' ');
    } else {
      // No category match — treat all words as merchant
      where = (before.join(' ') + ' ' + after.join(' ')).trim();
    }
  } else if (before.length > 1) {
    // "merchant words 47.50" — try the last word as hint
    const lastWord = before[before.length - 1].toLowerCase();
    if (aliasMap[lastWord]) {
      categoryHint = lastWord;
      categoryId   = aliasMap[lastWord];
      where = before.slice(0, -1).join(' ');
    } else {
      where = before.join(' ');
    }
  } else {
    where = before.join(' ');
  }

  // Merchant memory fallback if no category was hinted
  if (!categoryId && where) {
    const memoryHit = merchantMap[where.toLowerCase()];
    if (memoryHit) categoryId = memoryHit;
  }
  if (!categoryId) categoryId = defaultCategoryId;

  return {
    ok:           true,
    raw:          entry,
    where:        where.trim(),
    amount,
    categoryHint,
    categoryId,
    fromMemory:   !categoryHint && !!merchantMap[where.toLowerCase()],
    fromDefault:  !categoryHint && !merchantMap[where.toLowerCase()] && categoryId === defaultCategoryId,
  };
}

// Build a map of `alias → categoryId` from active categories.
// Aliases include the full lowercase name, hyphen-replaced name, first word,
// and a 3-4 char prefix.
function buildBudgetCategoryAliasMap() {
  const map = {};
  for (const cat of getActiveBudgetCategories()) {
    const aliases = generateAliasesForCategory(cat.name);
    for (const alias of aliases) {
      // First wins — earlier categories take precedence on collision.
      if (!(alias in map)) map[alias] = cat.id;
    }
  }
  return map;
}

function generateAliasesForCategory(name) {
  const lower    = name.toLowerCase().trim();
  const hyphen   = lower.replace(/\s+/g, '-');
  const stripped = lower.replace(/\s+/g, '');
  const first    = lower.split(/\s+/)[0];
  // We DELIBERATELY do not generate 3- or 4-char prefixes — they collide too
  // often with words that appear in merchant names ("shop" matches "Shopping"
  // but also appears in "mystery shop"). Users wanting a short alias should
  // pick a category name whose first word is short.
  return [...new Set([lower, hyphen, stripped, first].filter(s => s.length >= 2))];
}

// Build a map of `merchant (lowercase) → most-recent categoryId` from history.
// Walks all transactions, taking the newest for each unique `where`.
function buildBudgetMerchantMemory() {
  const map = {};
  const ts  = {}; // tracks timestamp for "most recent"
  for (const yyyymm of Object.keys(transactions)) {
    for (const tx of Object.values(transactions[yyyymm])) {
      if (!tx.where || !tx.categoryId) continue;
      const key = tx.where.toLowerCase().trim();
      const t   = new Date(tx.createdAt || tx.updatedAt || 0).getTime();
      if (!(key in ts) || t > ts[key]) {
        map[key] = tx.categoryId;
        ts[key]  = t;
      }
    }
  }
  return map;
}

// Returns merchants alphabetically — used for autocomplete.
function getMerchantSuggestions() {
  const set = new Set();
  for (const yyyymm of Object.keys(transactions)) {
    for (const tx of Object.values(transactions[yyyymm])) {
      if (tx.where) set.add(tx.where.trim());
    }
  }
  return [...set].sort((a, b) => a.localeCompare(b));
}

// Apply a parsed quick-add result to the database. Returns array of created tx.
async function commitQuickAdd(parsed, { date = null } = {}) {
  const created = [];
  const useDate = date || (new Date().toISOString().slice(0, 10));
  for (const p of parsed) {
    if (!p.ok) continue;
    const tx = await createTransaction({
      date:       useDate,
      where:      p.where,
      amount:     p.amount,
      categoryId: p.categoryId,
      source:     'manual',
    });
    created.push(tx);
  }
  return created;
}

// ── Sync merge ─────────────────────────────────────────────────────────────
// Categories: per-object LWW with tombstone respect
function mergeBudgetCategories(local, remote) {
  const map = new Map();
  for (const c of (local || []))  if (!budgetCategoryDeletedIds.has(c.id)) map.set(c.id, c);
  for (const c of (remote || [])) {
    if (budgetCategoryDeletedIds.has(c.id)) continue;
    const existing = map.get(c.id);
    if (!existing) {
      map.set(c.id, c);
    } else {
      const lt = existing.updatedAt ? new Date(existing.updatedAt).getTime() : 0;
      const rt = c.updatedAt        ? new Date(c.updatedAt).getTime()        : 0;
      if (rt > lt) map.set(c.id, c);
    }
  }
  return Array.from(map.values());
}

// Transactions: nested {yyyymm: {txId: tx}} — per-tx LWW with tombstone respect
function mergeTransactions(local, remote) {
  const out = JSON.parse(JSON.stringify(local || {})); // deep copy
  // Strip any local entries that match a remote tombstone we just learned about
  // (handled by caller updating the tombstone set first)
  // Apply remote entries
  if (!remote) return out;
  for (const yyyymm of Object.keys(remote)) {
    if (!out[yyyymm]) out[yyyymm] = {};
    for (const txId of Object.keys(remote[yyyymm])) {
      if (budgetTransactionDeletedIds.has(txId)) continue;
      const ri = remote[yyyymm][txId];
      const li = out[yyyymm][txId];
      if (!li) {
        out[yyyymm][txId] = ri;
      } else {
        const lt = li.updatedAt ? new Date(li.updatedAt).getTime() : 0;
        const rt = ri.updatedAt ? new Date(ri.updatedAt).getTime() : 0;
        if (rt > lt) out[yyyymm][txId] = ri;
      }
    }
  }
  // Strip locally-tombstoned entries from output (in case they were re-added by remote)
  for (const yyyymm of Object.keys(out)) {
    for (const txId of Object.keys(out[yyyymm])) {
      if (budgetTransactionDeletedIds.has(txId)) delete out[yyyymm][txId];
    }
    if (Object.keys(out[yyyymm]).length === 0) delete out[yyyymm];
  }
  return out;
}

// Tombstone sets: union merge
function mergeBudgetTombstoneSet(local, remote) {
  const out = new Set(local instanceof Set ? local : (Array.isArray(local) ? local : []));
  for (const id of (Array.isArray(remote) ? remote : [])) out.add(id);
  return out;
}

// ── Diagnostics ────────────────────────────────────────────────────────────
if (typeof window !== 'undefined') {
  window.budgetSpendDiag = function () {
    const yyyymm = _yyyymm(new Date());
    return {
      categories:        budgetCategories.length,
      activeCategories:  getActiveBudgetCategories().length,
      currentMonth:      yyyymm,
      currentMonthTxs:   Object.keys(transactions[yyyymm] || {}).length,
      totalMonths:       Object.keys(transactions).length,
      totalSpendThisMo:  getTotalSpendForMonth(yyyymm),
      categoryTombstones:    budgetCategoryDeletedIds.size,
      transactionTombstones: budgetTransactionDeletedIds.size,
      setupOffered:      _budgetSetupOffered,
    };
  };
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET SPEND UI — Phase 2b
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 2a foundations
//  block (just before the GROCERY LIST section).
// ═══════════════════════════════════════════════════════════════════════════

// ── Spend panel state ──────────────────────────────────────────────────────
let _quickAddDateOverride = null;       // ISO date if user picks a non-today date
let _quickAddDebounceTimer= null;
let _quickAddPrefillCatId = null;       // set when opened from a search chip's
                                        // "Add new" — biases the parser so
                                        // untagged entries land in this cat.
                                        // Cleared on modal close.

// ── Render entry — called by renderBudget when panel === 'spend' ───────────
function openQuickAddSpend() {
  if (getActiveBudgetCategories().length === 0) {
    // First-run setup modal lives in budget-ui.js. openQuickAddSpend can be
    // invoked from the omnibox before the Budget view (and thus the UI bundle)
    // has loaded, so ensure it's present before calling.
    if (typeof openBudgetSetupModal === 'function') {
      openBudgetSetupModal();
    } else if (typeof window._loadBudgetUI === 'function') {
      window._loadBudgetUI().then(() => {
        if (typeof openBudgetSetupModal === 'function') openBudgetSetupModal();
      });
    }
    return;
  }
  _quickAddDateOverride = null;
  // Pick up the search-chip prefill (if any). Pull off window, clear it
  // immediately so a second invocation without a chip doesn't reuse it.
  try {
    if (typeof window !== 'undefined' && window._searchChipPrefillCategoryId) {
      _quickAddPrefillCatId = window._searchChipPrefillCategoryId;
      window._searchChipPrefillCategoryId = null;
    } else {
      _quickAddPrefillCatId = null;
    }
  } catch (_) { _quickAddPrefillCatId = null; }

  document.getElementById('spend-quick-add-input').value = '';
  document.getElementById('spend-quick-add-date').value = new Date().toISOString().slice(0, 10);
  document.getElementById('spend-quick-add-preview').innerHTML = '';
  show('spend-quick-add-preview-empty', 'block');
  _refreshQuickAddCount(0);

  // Banner: show which category will be the default if any
  _refreshQuickAddPrefillBanner();

  openModal('spend-quick-add-modal');
  setTimeout(() => document.getElementById('spend-quick-add-input').focus(), 50);
}

// Render or hide the "Defaults to <category>" banner inside the quick-add
// modal. Inserts a small hint div above the input on first call; updates
// or hides it on subsequent calls. Idempotent.
function _refreshQuickAddPrefillBanner() {
  const input = document.getElementById('spend-quick-add-input');
  if (!input) return;
  let banner = document.getElementById('spend-quick-add-prefill-banner');
  if (!_quickAddPrefillCatId) {
    if (banner) banner.style.display = 'none';
    return;
  }
  const cat = getBudgetCategoryById(_quickAddPrefillCatId);
  if (!cat) {
    if (banner) banner.style.display = 'none';
    _quickAddPrefillCatId = null;
    return;
  }
  if (!banner) {
    banner = document.createElement('div');
    banner.id = 'spend-quick-add-prefill-banner';
    banner.className = 'quick-add-prefill-banner';
    // Insert just above the input
    input.parentNode.insertBefore(banner, input);
  }
  banner.style.display = '';
  banner.innerHTML =
    `<svg aria-hidden="true" style="width:12px;height:12px;flex-shrink:0"><use href="#i-tag"></use></svg>` +
    `<span>Defaults to <strong>${_escapeHtml(cat.name)}</strong> — type "&lt;category&gt; &lt;amount&gt;" to override</span>` +
    `<button class="quick-add-prefill-clear" onclick="_clearQuickAddPrefill()" title="Clear default" aria-label="Clear default category">×</button>`;
}

// Clear the prefill default while the modal is open. The user might want
// to add a mix of categorised entries and not want the chip's category
// applied as a fallback.
function _clearQuickAddPrefill() {
  _quickAddPrefillCatId = null;
  _refreshQuickAddPrefillBanner();
  // Re-render the preview so the change is reflected in any chips already
  // shown with the previous default.
  _renderQuickAddPreview();
}

function _refreshQuickAddCount(n) {
  const btn = document.getElementById('spend-quick-add-confirm');
  if (!btn) return;
  if (n === 0) {
    btn.disabled = true;
    btn.style.opacity = '0.5';
    btn.style.cursor = 'not-allowed';
    btn.querySelector('span').textContent = 'Add';
  } else {
    btn.disabled = false;
    btn.style.opacity = '1';
    btn.style.cursor = 'pointer';
    btn.querySelector('span').textContent = `Add ${n} ${n === 1 ? 'transaction' : 'transactions'}`;
  }
}

function quickAddInputChanged() {
  if (_quickAddDebounceTimer) clearTimeout(_quickAddDebounceTimer);
  _quickAddDebounceTimer = setTimeout(_renderQuickAddPreview, 80);
}

function _renderQuickAddPreview() {
  const raw = document.getElementById('spend-quick-add-input').value;
  const previewHost = document.getElementById('spend-quick-add-preview');
  const emptyHint   = document.getElementById('spend-quick-add-preview-empty');

  if (!raw.trim()) {
    previewHost.innerHTML = '';
    emptyHint.style.display = 'block';
    _refreshQuickAddCount(0);
    return;
  }
  emptyHint.style.display = 'none';

  // Newlines also separate entries — convert to commas for the parser
  const normalised = raw.replace(/\n/g, ',');
  const parsed = parseQuickAddInput(normalised, { defaultCategoryId: _quickAddPrefillCatId });

  const validCount = parsed.filter(p => p.ok && p.amount > 0).length;
  _refreshQuickAddCount(validCount);

  previewHost.innerHTML = parsed.map(_renderQuickAddChip).join('');
}

function _renderQuickAddChip(p) {
  if (!p.ok) {
    const reason = p.error === 'no_amount' ? 'no amount' : 'unparseable';
    return `<div class="quick-add-chip is-error" title="${_escapeHtml(p.raw)}">
      <span class="quick-add-chip-icon"><svg aria-hidden="true" style="width:12px;height:12px"><use href="#i-x"></use></svg></span>
      <span class="quick-add-chip-text">${_escapeHtml(p.raw)} <em style="color:var(--danger);font-style:normal">(${reason})</em></span>
    </div>`;
  }
  const cat = p.categoryId ? getBudgetCategoryById(p.categoryId) : null;
  const catName  = cat ? cat.name : 'No category';
  const catColor = cat ? cat.color : 'var(--muted)';
  const where = p.where || '(no merchant)';
  let sourceTag = '';
  if (p.categoryHint)        sourceTag = '<em style="color:var(--accent2);font-style:normal;margin-left:6px;font-size:10px">explicit</em>';
  else if (p.fromMemory)     sourceTag = '<em style="color:var(--ok);font-style:normal;margin-left:6px;font-size:10px">remembered</em>';
  else if (p.fromDefault)    sourceTag = '<em style="color:var(--accent);font-style:normal;margin-left:6px;font-size:10px">default</em>';
  else if (cat)              sourceTag = '';
  else                       sourceTag = '<em style="color:var(--warn);font-style:normal;margin-left:6px;font-size:10px">no category</em>';

  return `<div class="quick-add-chip ${cat ? '' : 'is-warn'}">
    <span class="quick-add-chip-amt">${_money(p.amount)}</span>
    <span class="quick-add-chip-where">${_escapeHtml(where)}</span>
    <span class="quick-add-chip-cat" style="background:${catColor}"></span>
    <span class="quick-add-chip-cat-name">${_escapeHtml(catName)}</span>
    ${sourceTag}
  </div>`;
}

async function confirmQuickAdd() {
  const raw = document.getElementById('spend-quick-add-input').value;
  const date = document.getElementById('spend-quick-add-date').value || (new Date().toISOString().slice(0, 10));
  const normalised = raw.replace(/\n/g, ',');
  // Use the same defaultCategoryId the preview used, so the persisted
  // transactions match the chips the user saw.
  const parsed = parseQuickAddInput(normalised, { defaultCategoryId: _quickAddPrefillCatId }).filter(p => p.ok && p.amount > 0);
  if (parsed.length === 0) { toast('Nothing to add'); return; }

  const created = await commitQuickAdd(parsed, { date });
  closeModal('spend-quick-add-modal');
  // Clear the prefill default now the modal is gone — next opening will
  // be a fresh state unless another search-chip "Add new" tap sets it.
  _quickAddPrefillCatId = null;
  toast(`Added ${created.length} transaction${created.length === 1 ? '' : 's'}`);

  // Re-render whatever's visible
  if (_currentView === 'budget') {
    // Budget UI is guaranteed loaded if we're on the budget view, but guard
    // defensively in case of an unexpected call path.
    if (_budgetActivePanel === 'spend' && typeof renderBudgetSpend === 'function') renderBudgetSpend();
    else if (_budgetActivePanel === 'dashboard' && typeof renderBudgetDashboard === 'function') renderBudgetDashboard();
  }
}

// ── Transaction edit modal ─────────────────────────────────────────────────
// Per-transaction sharing was removed in Pass 2d-rollback. The unit of
// share is now the budget CATEGORY (Pass 2e), and individual
// transactions inherit visibility from their category's share state.
// The registrations that used to live here (registerSharingSection
// 'transaction', registerBulkSelectSection 'transaction') are gone;
// 'category' replaces them in Pass 2e.

function _refreshBudgetWeekStartRadio() {
  const wkStart = budgetSettings.weekStart || 'mon';
  const monRadio = document.getElementById('setting-budget-weekstart-mon');
  const sunRadio = document.getElementById('setting-budget-weekstart-sun');
  if (monRadio) monRadio.checked = (wkStart === 'mon');
  if (sunRadio) sunRadio.checked = (wkStart === 'sun');
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET — Phase 3a Foundations (accounts, income, cash flow projection)
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 2b BUDGET SPEND UI
//  block (just before the GROCERY LIST section).
// ═══════════════════════════════════════════════════════════════════════════

// ── State ──────────────────────────────────────────────────────────────────
let budgetAccounts                    = [];        // [{id, name, type, isPrimary, balance, balanceAsOf, ...}]
let incomeTemplates                   = [];        // [{id, name, amount, frequency, dayOfMonth, accountId, ...}]
let incomeEntries                     = {};        // {'YYYY-MM': {[id]: entry}}
let budgetAccountDeletedIds           = new Set(); // tombstones
let incomeTemplateDeletedIds          = new Set();
let incomeEntryDeletedIds             = new Set();

// ── Persistence ────────────────────────────────────────────────────────────
async function loadBudgetAccountsAndIncome() {
  const accs       = await dbGet('budgetAccounts',          'budgetAccounts');
  const incTpl     = await dbGet('incomeTemplates',         'incomeTemplates');
  const incEnt     = await dbGet('incomeEntries',           'incomeEntries');
  const accTomb    = await dbGet('budgetAccountDeletedIds', 'budgetAccountDeletedIds');
  const incTplTomb = await dbGet('incomeTemplateDeletedIds','incomeTemplateDeletedIds');
  const incEntTomb = await dbGet('incomeEntryDeletedIds',   'incomeEntryDeletedIds');

  if (Array.isArray(accs))    budgetAccounts   = accs;
  if (Array.isArray(incTpl))  incomeTemplates  = incTpl;
  if (incEnt && typeof incEnt === 'object') incomeEntries = incEnt;
  if (Array.isArray(accTomb))    budgetAccountDeletedIds   = new Set(accTomb);
  if (Array.isArray(incTplTomb)) incomeTemplateDeletedIds  = new Set(incTplTomb);
  if (Array.isArray(incEntTomb)) incomeEntryDeletedIds     = new Set(incEntTomb);

  // One-time cleanup: drop unpaid template-instance entries whose template
  // was deleted or archived. These are stale ghosts that otherwise keep
  // producing phantom income in the projection. Paid-out entries are kept.
  await _purgePhantomUnpaidIncomeEntries();
}

// Sweeps incomeEntries and removes any unpaid template-instance entries
// that reference a missing or archived template. Persists if any changes
// were made. Cheap to run on every load.
async function _purgePhantomUnpaidIncomeEntries() {
  const validIds = new Set();
  for (const t of (incomeTemplates || [])) {
    if (!t.archived) validIds.add(t.id);
  }
  let touched = false;
  const monthsToRefresh = new Set();
  for (const yyyymm of Object.keys(incomeEntries)) {
    const month = incomeEntries[yyyymm];
    for (const entryId of Object.keys(month)) {
      const e = month[entryId];
      if (!e.templateId) continue;
      if (e.paidAt) continue;
      if (validIds.has(e.templateId)) continue;
      delete month[entryId];
      monthsToRefresh.add(yyyymm);
      touched = true;
    }
    if (Object.keys(month).length === 0) delete incomeEntries[yyyymm];
  }
  if (touched) {
    // Drop these months from materialisedMonths so a future re-open will
    // rebuild them from current templates rather than skipping.
    if (budgetSettings && Array.isArray(budgetSettings.materialisedMonths)) {
      budgetSettings.materialisedMonths = budgetSettings.materialisedMonths
        .filter(m => !monthsToRefresh.has(m));
    }
    await saveBudgetAccountsAndIncomeLocal();
    if (typeof saveBudgetLocal === 'function') await saveBudgetLocal();
  }
}

async function saveBudgetAccountsAndIncomeLocal() {
  await dbPut('budgetAccounts',           'budgetAccounts',           budgetAccounts);
  await dbPut('incomeTemplates',          'incomeTemplates',          incomeTemplates);
  await dbPut('incomeEntries',            'incomeEntries',            incomeEntries);
  await dbPut('budgetAccountDeletedIds',  'budgetAccountDeletedIds',  [...budgetAccountDeletedIds]);
  await dbPut('incomeTemplateDeletedIds', 'incomeTemplateDeletedIds', [...incomeTemplateDeletedIds]);
  await dbPut('incomeEntryDeletedIds',    'incomeEntryDeletedIds',    [...incomeEntryDeletedIds]);
}

// ── Account CRUD ───────────────────────────────────────────────────────────
function getActiveAccounts() {
  return budgetAccounts.filter(a => !a.archived);
}

function getPrimaryAccount() {
  return budgetAccounts.find(a => !a.archived && a.isPrimary) || null;
}

function getAccountById(id) {
  return budgetAccounts.find(a => a.id === id) || null;
}

const _ACCOUNT_PALETTE = ['#5b8dee', '#4cbb8a', '#e8a838', '#b35bee', '#5bd4e8', '#e85d8e'];
function getActiveIncomeTemplates() {
  return incomeTemplates.filter(t => !t.archived);
}

function getIncomeTemplateById(id) {
  return incomeTemplates.find(t => t.id === id) || null;
}

// ── Income entry CRUD (mirrors transaction structure) ──────────────────────
function projectCashFlow(accountId = null, daysAhead = 30, fromIso = null) {
  const account = accountId ? getAccountById(accountId) : getPrimaryAccount();

  // Setup-not-complete states
  if (!account) {
    return {
      account: null, startDate: null, startBalance: null, points: [],
      low: null, hasGaps: false, setupComplete: false,
      reason: 'no_primary_account',
    };
  }

  const todayIso = (new Date()).toISOString().slice(0, 10);
  const startIso = fromIso || todayIso;

  // Catch-up from balanceAsOf to startIso, applying historical events
  let runningBalance = account.balance;
  const balanceAsOf  = account.balanceAsOf || todayIso;
  const hasGaps      = balanceAsOf < startIso;
  if (hasGaps) {
    runningBalance = _applyHistoricalEvents(runningBalance, account, balanceAsOf, startIso);
  }
  const startBalance = runningBalance;

  // Walk forward day-by-day
  const points = [];
  const cursor = new Date(startIso + 'T12:00:00');
  for (let i = 0; i < daysAhead; i++) {
    const dayIso = cursor.toISOString().slice(0, 10);
    const events = _eventsOnDay(account, dayIso);
    const dayDelta = events.reduce((s, e) => s + e.amount, 0);
    runningBalance += dayDelta;
    points.push({
      date:    dayIso,
      balance: Math.round(runningBalance * 100) / 100,
      events,
    });
    cursor.setDate(cursor.getDate() + 1);
  }

  // Find low point
  let low = null;
  for (let i = 0; i < points.length; i++) {
    if (!low || points[i].balance < low.balance) {
      low = { date: points[i].date, balance: points[i].balance, daysFromStart: i };
    }
  }

  return {
    account,
    startDate:    startIso,
    startBalance: Math.round(startBalance * 100) / 100,
    points,
    low,
    hasGaps,
    setupComplete: true,
  };
}

// Apply all historical bill instances, income entries, and discretionary
// transactions between fromIso and toIso to a starting balance.
function _applyHistoricalEvents(startBalance, account, fromIso, toIso) {
  let bal = startBalance;
  const months = _enumerateMonths(fromIso, toIso);

  for (const yyyymm of months) {
    // Bill instances paid in this period (subtract)
    const bi = billInstances?.[yyyymm] || {};
    for (const inst of Object.values(bi)) {
      if (inst.skipped) continue;
      const tplAccountId = bills.find(b => b.id === inst.billId)?.accountId || getPrimaryAccount()?.id;
      if (tplAccountId !== account.id) continue;
      // Use the date the user marked it paid if present, else dueDate
      const eventDate = (inst.paidAt ? inst.paidAt.slice(0, 10) : inst.dueDate);
      if (eventDate <= fromIso || eventDate > toIso) continue;
      bal -= (inst.actualAmount ?? inst.expectedAmount) || 0;
    }

    // Income entries received in this period (add)
    const ie = incomeEntries?.[yyyymm] || {};
    for (const entry of Object.values(ie)) {
      if (entry.accountId !== account.id) continue;
      // If paidAt is set, use that; otherwise the entry is pending — only count if entry.date <= toIso
      const eventDate = entry.paidAt ? entry.paidAt.slice(0, 10) : entry.date;
      if (eventDate <= fromIso || eventDate > toIso) continue;
      bal += (entry.amount || 0);
    }

    // Discretionary transactions (Phase 2) — subtract, regardless of categoryId,
    // but only if they affect this account. Currently transactions don't have
    // accountId; assume they all hit the primary account.
    if (account.isPrimary) {
      const txs = transactions?.[yyyymm] || {};
      for (const tx of Object.values(txs)) {
        if (tx.date <= fromIso || tx.date > toIso) continue;
        bal -= (tx.amount || 0);
      }
    }
  }

  return bal;
}

// Returns events landing on a single date for the given account.
// Combines materialised bill instances, materialised income entries,
// and template projections for templates that haven't been materialised yet.
function _eventsOnDay(account, dayIso) {
  const events = [];
  const yyyymm = _yyyymmFromString(dayIso);
  const { year, month } = _parseYyyymm(yyyymm);

  // 1. Bill instances already materialised for this month
  const bi = billInstances?.[yyyymm] || {};
  const materialisedBillIds = new Set();
  for (const inst of Object.values(bi)) {
    if (inst.dueDate !== dayIso) continue;
    // Phase 5b: saving instances are paper-only — they don't move money
    // out of the account, so they should never be projection events.
    if (inst.kind === 'saving') continue;
    materialisedBillIds.add(inst.billId);
    if (inst.skipped || inst.paidAt) continue; // already paid/skipped — not a future event
    const tpl = bills.find(b => b.id === inst.billId);
    if (!tpl) continue;
    const effectiveAccountId = tpl.accountId || getPrimaryAccount()?.id;
    if (effectiveAccountId !== account.id) continue;
    events.push({
      type:     'bill',
      amount:   -((inst.actualAmount ?? inst.expectedAmount) || 0),
      label:    tpl.name,
      sourceId: inst.billId,
    });
  }

  // 2. Bill templates due on this day but NOT yet materialised
  for (const tpl of (bills || [])) {
    if (tpl.archived) continue;
    if (materialisedBillIds.has(tpl.id)) continue;
    if (!shouldBeDueInMonth(tpl, year, month)) continue;
    const dom = Math.min(tpl.dayOfMonth || 1, _daysInMonth(year, month));
    if (dom !== Number(dayIso.slice(8, 10))) continue;
    const effectiveAccountId = tpl.accountId || getPrimaryAccount()?.id;
    if (effectiveAccountId !== account.id) continue;
    events.push({
      type:     'bill',
      amount:   -(tpl.amount || 0),
      label:    tpl.name,
      sourceId: tpl.id,
    });
  }

  // 3. Income entries for this day
  const ie = incomeEntries?.[yyyymm] || {};
  const materialisedIncomeFromTplIds = new Set();
  for (const entry of Object.values(ie)) {
    if (entry.date !== dayIso) continue;
    if (entry.templateId) materialisedIncomeFromTplIds.add(entry.templateId);
    if (entry.accountId !== account.id) continue;
    // Already received → not a future event. The receipt is already
    // incorporated in the user's balance (either directly via a balance
    // update, or via _applyHistoricalEvents during catch-up). Mirrors
    // the bill instance skip at section 1 above — without this, confirmed
    // income gets double-counted in the cash flow projection.
    if (entry.paidAt) continue;
    // Defensive: skip phantom unpaid entries whose template was deleted
    // or archived (they're stale ghosts in the materialised store).
    if (entry.templateId) {
      const tpl = getIncomeTemplateById(entry.templateId);
      if (!tpl || tpl.archived) continue;
    }
    // Future expected income → counts; past unreceived → still counts but flagged
    events.push({
      type:     'income',
      amount:   (entry.amount || 0),
      label:    entry.notes || (entry.templateId ? getIncomeTemplateById(entry.templateId)?.name : null) || 'Income',
      sourceId: entry.id,
    });
  }

  // 4. Income templates that would land on this day but haven't been materialised
  for (const tpl of (incomeTemplates || [])) {
    if (tpl.archived) continue;
    if (materialisedIncomeFromTplIds.has(tpl.id)) continue;
    if (!shouldBeDueInMonth(tpl, year, month)) continue;
    const dom = Math.min(tpl.dayOfMonth || 25, _daysInMonth(year, month));
    if (dom !== Number(dayIso.slice(8, 10))) continue;
    if (tpl.accountId && tpl.accountId !== account.id) continue;
    if (!tpl.accountId && !account.isPrimary) continue;
    events.push({
      type:     'income',
      amount:   (tpl.amount || 0),
      label:    tpl.name,
      sourceId: tpl.id,
    });
  }

  return events;
}

function _enumerateMonths(fromIso, toIso) {
  const out = [];
  const start = new Date(fromIso + 'T12:00:00');
  const end   = new Date(toIso   + 'T12:00:00');
  const cursor = new Date(start.getFullYear(), start.getMonth(), 1);
  const final  = new Date(end.getFullYear(),   end.getMonth(),   1);
  while (cursor <= final) {
    out.push(`${cursor.getFullYear()}-${String(cursor.getMonth()+1).padStart(2,'0')}`);
    cursor.setMonth(cursor.getMonth() + 1);
  }
  return out;
}

// Convenience: just the low-point summary for the dashboard hero card
function mergeBudgetAccounts(local, remote) {
  const map = new Map();
  for (const a of (local || []))  if (!budgetAccountDeletedIds.has(a.id)) map.set(a.id, a);
  for (const a of (remote || [])) {
    if (budgetAccountDeletedIds.has(a.id)) continue;
    const existing = map.get(a.id);
    if (!existing) {
      map.set(a.id, a);
    } else {
      const lt = existing.updatedAt ? new Date(existing.updatedAt).getTime() : 0;
      const rt = a.updatedAt        ? new Date(a.updatedAt).getTime()        : 0;
      if (rt > lt) map.set(a.id, a);
    }
  }
  // Enforce primary-account invariant: at most one primary among non-archived
  const merged = Array.from(map.values());
  const activePrimaries = merged.filter(a => !a.archived && a.isPrimary);
  if (activePrimaries.length > 1) {
    // Keep the one with the latest updatedAt as primary; demote others
    activePrimaries.sort((a, b) => (new Date(b.updatedAt).getTime()) - (new Date(a.updatedAt).getTime()));
    for (let i = 1; i < activePrimaries.length; i++) {
      activePrimaries[i].isPrimary = false;
    }
  }
  return merged;
}

function mergeIncomeTemplates(local, remote) {
  const map = new Map();
  for (const t of (local || []))  if (!incomeTemplateDeletedIds.has(t.id)) map.set(t.id, t);
  for (const t of (remote || [])) {
    if (incomeTemplateDeletedIds.has(t.id)) continue;
    const existing = map.get(t.id);
    if (!existing) {
      map.set(t.id, t);
    } else {
      const lt = existing.updatedAt ? new Date(existing.updatedAt).getTime() : 0;
      const rt = t.updatedAt        ? new Date(t.updatedAt).getTime()        : 0;
      if (rt > lt) map.set(t.id, t);
    }
  }
  return Array.from(map.values());
}

function mergeIncomeEntries(local, remote) {
  const out = JSON.parse(JSON.stringify(local || {}));
  if (!remote) return out;
  for (const yyyymm of Object.keys(remote)) {
    if (!out[yyyymm]) out[yyyymm] = {};
    for (const id of Object.keys(remote[yyyymm])) {
      if (incomeEntryDeletedIds.has(id)) continue;
      const ri = remote[yyyymm][id];
      const li = out[yyyymm][id];
      if (!li) {
        out[yyyymm][id] = ri;
      } else {
        const lt = li.updatedAt ? new Date(li.updatedAt).getTime() : 0;
        const rt = ri.updatedAt ? new Date(ri.updatedAt).getTime() : 0;
        if (rt > lt) out[yyyymm][id] = ri;
      }
    }
  }
  // Strip locally-tombstoned entries
  for (const yyyymm of Object.keys(out)) {
    for (const id of Object.keys(out[yyyymm])) {
      if (incomeEntryDeletedIds.has(id)) delete out[yyyymm][id];
    }
    if (Object.keys(out[yyyymm]).length === 0) delete out[yyyymm];
  }
  return out;
}

// ── Diagnostics ────────────────────────────────────────────────────────────
if (typeof window !== 'undefined') {
  window.budgetAccountsDiag = function () {
    const primary = getPrimaryAccount();
    const today   = new Date().toISOString().slice(0, 10);
    const proj    = primary ? projectCashFlow(primary.id, 30) : null;
    return {
      accounts:       budgetAccounts.length,
      activeAccounts: getActiveAccounts().length,
      primary:        primary ? { id: primary.id, name: primary.name, balance: primary.balance, balanceAsOf: primary.balanceAsOf } : null,
      incomeTemplates: incomeTemplates.length,
      activeIncomeTemplates: getActiveIncomeTemplates().length,
      incomeEntriesMonths: Object.keys(incomeEntries).length,
      projection: proj && proj.setupComplete ? {
        startBalance: proj.startBalance,
        endBalance:   proj.points[proj.points.length - 1]?.balance,
        low:          proj.low,
        hasGaps:      proj.hasGaps,
      } : { setupComplete: false, reason: proj?.reason },
    };
  };
}


// ═══════════════════════════════════════════════════════════════════════════
//  BUDGET ACCOUNTS UI — Phase 3b
//  Insertion point: in app.js, IMMEDIATELY AFTER the Phase 3a foundations
//  block (just before the GROCERY LIST section).
// ═══════════════════════════════════════════════════════════════════════════

// ── State (UI-only, not persisted) ─────────────────────────────────────────
function getInstanceDatesInMonth(template, year, monthZeroIdx) {
  const f = template.frequency || { unit: 'month', interval: 1 };
  const monthStart = new Date(year, monthZeroIdx, 1, 12, 0, 0); // noon to match cursor
  const monthEnd   = new Date(year, monthZeroIdx + 1, 0, 23, 59, 59); // end of last day
  const monthEndDate = monthEnd.getDate();

  if (f.unit === 'day' || f.unit === 'week') {
    // Walk forward from the anchor date in interval steps (in days)
    if (!f.anchorDate) return [];
    const stepDays = (f.unit === 'week' ? 7 : 1) * Math.max(1, f.interval || 1);
    const anchor = new Date(f.anchorDate + 'T12:00:00');
    if (isNaN(anchor.getTime())) return [];
    // If anchor is later than the month-end, no instances yet
    if (anchor > monthEnd) return [];
    // Find the first occurrence on or after monthStart by jumping in stepDays
    const daysFromAnchorToMonthStart = Math.floor((monthStart.getTime() - anchor.getTime()) / MS_PER_DAY);
    let stepsToMonthStart = Math.max(0, Math.ceil(daysFromAnchorToMonthStart / stepDays));
    let cursor = new Date(anchor.getTime() + stepsToMonthStart * stepDays * MS_PER_DAY);
    // Edge: if cursor is before monthStart (anchor is itself within or before the month),
    // bump forward until we're inside or past
    while (cursor < monthStart) {
      cursor = new Date(cursor.getTime() + stepDays * MS_PER_DAY);
    }
    const out = [];
    while (cursor <= monthEnd) {
      out.push(_dateToIso(cursor));
      cursor = new Date(cursor.getTime() + stepDays * MS_PER_DAY);
    }
    return out;
  }

  // 'year' unit
  if (f.unit === 'year') {
    const anchorMonth = f.anchorMonth ?? 0;
    if (monthZeroIdx !== anchorMonth) return [];
    const dom = _clampDayOfMonth(template.dayOfMonth || 1, year, monthZeroIdx);
    return [_isoDate(year, monthZeroIdx, dom)];
  }

  // 'month' unit (and default)
  const interval = Math.max(1, f.interval || 1);
  const anchor   = f.anchorMonth ?? 0;
  const monthsSinceEpoch = (year - 2000) * 12 + monthZeroIdx;
  const offsetFromAnchor = monthsSinceEpoch - anchor;
  if (offsetFromAnchor < 0 || offsetFromAnchor % interval !== 0) return [];
  const dom = _clampDayOfMonth(template.dayOfMonth || 1, year, monthZeroIdx);
  return [_isoDate(year, monthZeroIdx, dom)];
}

// Backwards-compat shim — Phase 1/2/3 callers ask "is this template due in this
// month at all?" The generalised engine answers via getInstanceDatesInMonth.
// We override the existing function.
function _dateToIso(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

// ── Frequency label (extended for week/day units) ──────────────────────────
// Override the Phase 1 label function to handle the new units.
function _migrateBillInstancesIfNeeded(yyyymm) {
  const month = billInstances[yyyymm];
  if (!month) return false;
  let migrated = false;
  for (const key of Object.keys(month)) {
    const inst = month[key];

    // Purge phantom instances missing dueDate. These came from the pre-fix
    // _setInstance bug that wrote partial records when the key lookup
    // failed (the patch had paidAt etc. but no dueDate/expectedAmount).
    // Every legitimate instance has a dueDate.
    if (!inst || !inst.dueDate) {
      if (inst && !inst.dueDate) {
        delete month[key];
        migrated = true;
      }
      continue;
    }
    if (!inst.billId) continue;

    let newKey = null;
    if (!key.includes('__')) {
      // Legacy single-key format
      newKey = `${inst.billId}__${inst.dueDate}`;
    } else if (key.includes('__SAV__')) {
      // Legacy saving key — collapse to uniform format. Make sure the
      // value has the `kind: 'saving'` discriminator (older split-saving
      // backfills already set this).
      newKey = `${inst.billId}__${inst.dueDate}`;
      if (inst.kind !== 'saving') inst.kind = 'saving';
    }
    if (!newKey || newKey === key) continue;

    if (month[newKey]) {
      // collision — keep the more-recently-updated one
      const existing = month[newKey];
      const eUpd = existing.updatedAt ? new Date(existing.updatedAt).getTime() : 0;
      const iUpd = inst.updatedAt     ? new Date(inst.updatedAt).getTime()     : 0;
      if (iUpd > eUpd) month[newKey] = inst;
    } else {
      month[newKey] = inst;
    }
    delete month[key];
    migrated = true;
  }
  return migrated;
}

// ── Updated materialiseMonth for Phase 4 frequency model ───────────────────
// Override Phase 1's materialiseMonth to handle multiple instances per template
// per month (weekly/4-weekly bills).